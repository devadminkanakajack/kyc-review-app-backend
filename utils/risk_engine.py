# utils/risk_engine.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from utils.trigger_library import TRIGGER_LIBRARY
from utils.ipa_status import ipa_risk_impact

# NEW: profile wrapper (kept, even if not used yet)
from utils.kyc_profile import get_profile  # noqa: F401


RATING_BANDS = [
    ("LOW", 0, 24),
    ("MEDIUM", 25, 49),
    ("HIGH", 50, 74),
    ("CRITICAL", 75, 100),
]


def clamp(n: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, n))


def band_label(score: float) -> str:
    s = clamp(score)
    for label, lo, hi in RATING_BANDS:
        if lo <= s <= hi:
            return label
    return "MEDIUM"


def safe_boolish(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in ("true", "yes", "y", "1")
    if isinstance(v, dict):
        if "flagged" in v:
            return bool(v.get("flagged"))
        if "count" in v:
            try:
                return float(v.get("count") or 0) > 0
            except Exception:
                return False
        if "matches" in v and isinstance(v.get("matches"), list):
            return len(v["matches"]) > 0
    if isinstance(v, list):
        return len(v) > 0
    return False


def detector_strength(v: Any) -> int:
    """
    0 = none, 1 = low, 2 = medium, 3 = high
    Conservative conversion across different detector return shapes.
    """
    if not safe_boolish(v):
        return 0

    if isinstance(v, dict):
        count = v.get("count")
        total = v.get("total") or v.get("amount") or v.get("sum")
        matches = v.get("matches")

        try:
            if isinstance(count, (int, float)):
                if count >= 10:
                    return 3
                if count >= 4:
                    return 2
                return 1
        except Exception:
            pass

        try:
            if isinstance(total, (int, float)):
                if total >= 50000:
                    return 3
                if total >= 10000:
                    return 2
                return 1
        except Exception:
            pass

        if isinstance(matches, list):
            if len(matches) >= 10:
                return 3
            if len(matches) >= 4:
                return 2
            return 1

        return 1

    if isinstance(v, list):
        if len(v) >= 10:
            return 3
        if len(v) >= 4:
            return 2
        return 1

    return 1


def add_driver(drivers: List[Dict[str, Any]], title: str, detail: str, points: float) -> None:
    drivers.append({"title": title, "detail": detail, "points": round(float(points), 2)})


def merge_actions(base: List[str], extra: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in (base or []) + (extra or []):
        if not x:
            continue
        s = str(x).strip()
        if not s:
            continue
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    return out


def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None


def _collect_detector_row_ids(detectors: Dict[str, Any]) -> Dict[str, List[int]]:
    """Extract flagged row ids per detector (best-effort, fail-soft)."""
    out: Dict[str, List[int]] = {}
    if not isinstance(detectors, dict):
        return out

    for k, v in detectors.items():
        row_ids: List[int] = []

        # Common normalized shape
        if isinstance(v, dict):
            if isinstance(v.get("flagged_row_ids"), list):
                row_ids = [i for i in (_safe_int(x) for x in (v.get("flagged_row_ids") or [])) if i is not None]

            # Structured clusters may also carry per-cluster row ids
            if not row_ids and isinstance(v.get("clusters"), list):
                for c in v.get("clusters") or []:
                    if isinstance(c, dict) and isinstance(c.get("flagged_row_ids"), list):
                        for x in c.get("flagged_row_ids") or []:
                            ii = _safe_int(x)
                            if ii is not None:
                                row_ids.append(ii)

            # Recurrence detector returns multiple cluster lists
            if not row_ids and k == "recurrence":
                for list_key in ("identity_clusters", "same_day_identity_clusters", "narrative_clusters"):
                    for c in (v.get(list_key) or []):
                        if isinstance(c, dict) and isinstance(c.get("flagged_row_ids"), list):
                            for x in c.get("flagged_row_ids") or []:
                                ii = _safe_int(x)
                                if ii is not None:
                                    row_ids.append(ii)

        if row_ids:
            out[k] = sorted(set(row_ids))

    return out


def _map_rows_to_channels(
    raw_df: Any,
    detector_row_ids: Dict[str, List[int]],
    cap_per_channel: int = 50,
) -> Dict[str, Any]:
    """Build a drill-down map: TRANSCODE -> suspicious tx rows with detector tags."""
    if raw_df is None:
        return {"by_channel": {}, "total_flagged": 0}

    try:
        import pandas as pd  # local import
    except Exception:
        return {"by_channel": {}, "total_flagged": 0}

    if not hasattr(raw_df, "columns"):
        return {"by_channel": {}, "total_flagged": 0}

    df = raw_df
    if "ROW_ID" not in df.columns:
        try:
            df = df.copy()
            df["ROW_ID"] = df.index
        except Exception:
            return {"by_channel": {}, "total_flagged": 0}

    # Invert: row_id -> detectors
    row_to_detectors: Dict[int, List[str]] = {}
    for det_key, ids in (detector_row_ids or {}).items():
        for rid in ids or []:
            if rid is None:
                continue
            row_to_detectors.setdefault(int(rid), []).append(str(det_key))

    all_row_ids = sorted(row_to_detectors.keys())
    if not all_row_ids:
        return {"by_channel": {}, "total_flagged": 0}

    try:
        sub = df[df["ROW_ID"].isin(all_row_ids)].copy()
    except Exception:
        return {"by_channel": {}, "total_flagged": 0}


    # Apply materiality/fee suppression for listing
    try:
        from utils.materiality import should_include_row, MATERIALITY_MIN_AMOUNT
        sub = sub[sub.apply(lambda r: should_include_row(r, min_amount=MATERIALITY_MIN_AMOUNT), axis=1)]
    except Exception:
        pass

    try:
        all_row_ids = sorted(sub["ROW_ID"].dropna().astype(int).unique().tolist())
    except Exception:
        all_row_ids = sorted(row_to_detectors.keys())

    if not all_row_ids:
        return {"by_channel": {}, "total_flagged": 0}

    # Best-effort date
    date_col = None
    for c in ("DATE", "TXN_DATE", "VALUE_DATE", "POST_DATE", "TRAN_DATE", "DATE_POSTED", "DATE_STR"):
        if c in sub.columns:
            date_col = c
            break

    if date_col:
        try:
            sub["__DATE"] = pd.to_datetime(sub[date_col], errors="coerce")
        except Exception:
            sub["__DATE"] = None
    else:
        sub["__DATE"] = None

    # Ensure columns exist
    if "TRANSCODE" not in sub.columns:
        sub["TRANSCODE"] = "UNKNOWN"
    if "DESCRIPTION_RAW" not in sub.columns:
        sub["DESCRIPTION_RAW"] = ""
    if "CREDIT" not in sub.columns:
        sub["CREDIT"] = 0.0
    if "DEBIT" not in sub.columns:
        sub["DEBIT"] = 0.0

    by_channel: Dict[str, Any] = {}
    for code, grp in sub.groupby("TRANSCODE", dropna=False):
        code_s = str(code)
        try:
            grp2 = grp.sort_values("__DATE", ascending=False, na_position="last")
        except Exception:
            grp2 = grp

        rows: List[Dict[str, Any]] = []
        for _, r in grp2.head(max(1, int(cap_per_channel))).iterrows():
            rid = _safe_int(r.get("ROW_ID"))
            dets = row_to_detectors.get(int(rid)) if rid is not None else []
            dt = r.get("__DATE")
            date_s = None
            try:
                if dt is not None and str(dt) != "NaT":
                    date_s = dt.strftime("%Y-%m-%d")
            except Exception:
                date_s = None

            rows.append(
                {
                    "row_id": rid,
                    "date": date_s,
                    "description_raw": str(r.get("DESCRIPTION_RAW") or ""),
                    "credit": float(r.get("CREDIT") or 0.0),
                    "debit": float(r.get("DEBIT") or 0.0),
                    "detectors": sorted(set([str(x) for x in (dets or []) if x])),
                }
            )

        by_channel[code_s] = {"flagged_count": int(len(grp)), "rows": rows}

    return {"by_channel": by_channel, "total_flagged": int(len(all_row_ids))}


TRIGGER_UI_MAP = {
    "S81 Notices": "SECTION_81_NOTICE",
    "SMRs": "SMR_INTERNAL_FIU_TM",
    "AML (FIU) Triggers": "AML_FIU_INTERNAL_INVESTIGATION_TRIGGER",
    "Fraud Case High Priority": "FRAUD_CASE_HIGH_PRIORITY",
    "Fraud Case Low Priority": "FRAUD_CASE_LOW_PRIORITY",
    "Adverse Media": "ADVERSE_MEDIA",
    "Staff PEP Reviews": "STAFF_PEP_REVIEW",
}


def resolve_trigger_key(trigger: Dict[str, Any]) -> Optional[str]:
    src = (trigger or {}).get("source") or ""
    src = str(src).strip()
    if not src:
        return None
    return TRIGGER_UI_MAP.get(src)


def lending_impact(lending_facilities: Any) -> Tuple[float, List[Dict[str, Any]], List[str]]:
    if not lending_facilities:
        return 0.0, [], []

    drivers: List[Dict[str, Any]] = []
    actions: List[str] = []
    total_points = 0.0

    for idx, fac in enumerate(lending_facilities if isinstance(lending_facilities, list) else []):
        if not isinstance(fac, dict):
            continue

        arrears = fac.get("arrears")
        outstanding = fac.get("outstanding")

        try:
            arrears_val = float(arrears) if arrears is not None else 0.0
        except Exception:
            arrears_val = 0.0

        try:
            out_val = float(outstanding) if outstanding is not None else 0.0
        except Exception:
            out_val = 0.0

        if arrears_val <= 0:
            continue

        pts = 6.0
        if out_val > 0:
            ratio = arrears_val / out_val
            if ratio >= 0.25:
                pts += 10.0
            elif ratio >= 0.10:
                pts += 6.0
            elif ratio >= 0.05:
                pts += 3.0

        total_points += pts
        add_driver(
            drivers,
            "Lending arrears indicator",
            f"Facility #{idx+1} has arrears > 0 (arrears={arrears_val:,.2f}, outstanding={out_val:,.2f}).",
            pts,
        )

    if total_points > 0:
        actions += [
            "Obtain loan statements and confirm arrears status, repayment plan, and affordability assessment.",
            "Assess relationship continuation risk and escalate to senior management if arrears are material.",
        ]

    return total_points, drivers, actions


def compute_risk_metrics(
    analysis: Dict[str, Any],
    client_type: str,
    client_profile: Dict[str, Any],
    trigger: Dict[str, Any],
) -> Dict[str, Any]:
    drivers: List[Dict[str, Any]] = []
    actions: List[str] = []
    missing: List[str] = []

    # -----------------------------
    # 1) Trigger baseline
    # -----------------------------
    trigger_key = resolve_trigger_key(trigger)
    base_ml = 10.0
    base_tf = 5.0
    base_actions: List[str] = []

    if trigger_key and trigger_key in TRIGGER_LIBRARY:
        t = TRIGGER_LIBRARY[trigger_key]
        rr = t.get("risk_rating") or {}
        base_ml = float(rr.get("ml", 2)) * 20.0
        base_tf = float(rr.get("tf", 1)) * 20.0
        base_actions = t.get("recommended_actions_min") or []
        add_driver(drivers, "Trigger baseline applied", f"Trigger='{t.get('label') or trigger_key}' sets initial ML/TF posture.", 0.0)
    else:
        add_driver(drivers, "Trigger baseline defaulted", "Trigger source not mapped to library; using conservative baseline.", 0.0)

    actions = merge_actions(actions, base_actions)

    # -----------------------------
    # 2) KYC profile scoring (computed in analyze_statement)
    #     NOTE: In the UPDATED workflow, KYC scoring happens AFTER detectors,
    #     so KYC points may already incorporate detector-derived behavioural features.
    # -----------------------------
    kyc_block = (analysis or {}).get("kyc_profile") or {}
    kyc_points = 0.0
    profile_id_used = None

    # Which detector keys were already represented inside KYC scoring
    scored_detector_keys: List[str] = []
    if isinstance(kyc_block, dict) and kyc_block:
        try:
            kyc_points = float(kyc_block.get("points") or 0.0)
        except Exception:
            kyc_points = 0.0

        profile_id_used = kyc_block.get("profile_id")

        sdk = kyc_block.get("scored_detector_keys")
        if isinstance(sdk, list):
            scored_detector_keys = [str(x) for x in sdk if x]

        for d in (kyc_block.get("drivers") or []):
            if isinstance(d, dict) and d.get("title") and d.get("detail"):
                drivers.append(d)

        actions = merge_actions(actions, kyc_block.get("actions") or [])
    else:
        missing.append("KYC profile analysis block missing from statement analysis (profile scoring skipped).")

    # -----------------------------
    # 3) ML/TF detector scoring
    #     IMPORTANT: De-dupe detector scoring if those detectors were already
    #     used inside KYC behaviour scoring (to avoid double counting).
    # -----------------------------
    detectors = (analysis or {}).get("detectors")
    if not isinstance(detectors, dict) or not detectors:
        detectors = {
            "structured_deposits": (analysis or {}).get("structured_deposits"),
            "structured_payments": (analysis or {}).get("structured_payments"),
            "pass_through": (analysis or {}).get("pass_through"),
            "layering": (analysis or {}).get("layering"),
            "round_figures": (analysis or {}).get("round_figures"),
            "salary_pattern": (analysis or {}).get("salary_pattern"),
            "cash_intensive": (analysis or {}).get("cash_intensive"),
            "third_party": (analysis or {}).get("third_party"),
            "recurrence": (analysis or {}).get("recurrence"),
        }

    # --------------------------------------------------
    # 3A) Drill-down: map detector-flagged row ids -> channels
    #     (NO impact on scoring; supports UI listing per TRANSCODE)
    # --------------------------------------------------
    detector_row_ids = _collect_detector_row_ids(detectors)
    suspicious_map = _map_rows_to_channels(
        raw_df=(analysis or {}).get("raw_df"),
        detector_row_ids=detector_row_ids,
        cap_per_channel=50,
    )

    total_flagged_txns = int(suspicious_map.get("total_flagged") or 0)
    if total_flagged_txns > 0:
        add_driver(
            drivers,
            "Detector drill-down available",
            f"Detectors flagged {total_flagged_txns} transaction rows for review; see suspicious_by_channel for per-channel listing.",
            0.0,
        )

    WEIGHTS = {
        "structured_deposits": 10.0,
        "structured_payments": 10.0,
        "pass_through": 12.0,
        "layering": 12.0,
        "third_party": 12.0,
        "round_figures": 6.0,
        "cash_intensive": 8.0,
        "salary_pattern": 6.0,
        # recurrence is informational; score lightly if you want, or leave unscored
        # "recurrence": 4.0,
    }

    detector_points = 0.0
    deduped: List[str] = []

    for key, weight in WEIGHTS.items():
        if key in scored_detector_keys:
            deduped.append(key)
            continue

        strength = detector_strength(detectors.get(key))
        if strength <= 0:
            continue

        pts = weight * strength / 3.0
        detector_points += pts
        add_driver(drivers, "Detector signal", f"{key.replace('_', ' ').title()} flagged (strength={strength}/3).", pts)

    if detector_points > 0:
        actions = merge_actions(
            actions,
            [
                "Perform enhanced review of flagged ML/TF patterns and document rationale with supporting evidence.",
                "Where warranted, request customer clarification and supporting documents for key flows.",
                "Apply enhanced monitoring rules aligned to detected typologies.",
            ],
        )

    if deduped:
        add_driver(
            drivers,
            "Detector scoring deduped",
            f"Skipped detector scoring for keys already represented in KYC behaviour scoring: {', '.join(deduped)}.",
            0.0,
        )

    # -----------------------------
    # 4) IPA status impact (non-individual only)
    # -----------------------------
    ipa_points = 0.0
    ipa_status = None
    if str(client_type).strip().lower() != "individual":
        ipa_status = (
            client_profile.get("ipa_status")
            or client_profile.get("IPA Status")
            or client_profile.get("ipaCertificateStatus")
        )
        if ipa_status:
            ipa_points, ipa_actions = ipa_risk_impact(str(ipa_status))
            if ipa_points > 0:
                add_driver(drivers, "IPA/ROC compliance status impact", f"IPA status='{ipa_status}' increases compliance risk posture.", ipa_points)
                actions = merge_actions(actions, ipa_actions)
        else:
            missing.append("Non-individual: IPA/ROC status not provided (compliance posture may be incomplete).")

    # -----------------------------
    # 5) Lending impact
    # -----------------------------
    lending_facilities = client_profile.get("lending_facilities") or client_profile.get("Lending Facilities")
    lend_points, lend_drivers, lend_actions = lending_impact(lending_facilities)
    if lend_points > 0:
        drivers.extend(lend_drivers)
        actions = merge_actions(actions, lend_actions)

    # -----------------------------
    # 6) Missing data flags
    # -----------------------------
    if str(client_type).strip().lower() == "individual":
        sof = (client_profile.get("source_of_funds") or "").lower()
        salary = client_profile.get("salary")
        if "salary" in sof and (salary is None or str(salary).strip() == ""):
            missing.append("Individual: Source of funds is Salary but salary amount is missing.")

    # -----------------------------
    # 7) Aggregate scoring (Trigger + KYC + ML/TF + IPA + Lending)
    # -----------------------------
    ml_score = clamp(base_ml + kyc_points + detector_points + ipa_points + lend_points)
    tf_score = clamp(base_tf + (detector_points * 0.25))
    overall = clamp((ml_score * 0.75) + (tf_score * 0.25))

    # -----------------------------
    # 8) Confidence
    # -----------------------------
    conf = 0.80
    conf -= min(0.30, 0.05 * len(missing))
    conf = clamp(conf * 100, 0, 100)

    rating = band_label(overall)

    return {
        "version": "risk_engine_v3_detectors_then_kyc_deduped",
        "rating": rating,
        "scores": {
            "overall": round(overall, 2),
            "ml": round(ml_score, 2),
            "tf": round(tf_score, 2),
            "confidence": round(conf, 2),
            "kyc_profile_points": round(kyc_points, 2),
            "detector_points": round(detector_points, 2),
        },
        "drivers": drivers,
        "required_actions": actions,
        "missing_data_flags": missing,
        # ✅ Drill-down support for UI: suspicious tx rows grouped by TRANSCODE
        # Shape:
        #   {"total_flagged": int, "by_channel": {"101": {"flagged_count": n, "rows": [...]}, ...}}
        "suspicious_by_channel": suspicious_map,
        "suspicious_by_detector": {
            "total_flagged": int(sum(len(v or []) for v in detector_row_ids.values())),
            "row_ids": detector_row_ids,
        },
        "inputs_used": {
            "trigger_key": trigger_key,
            "kyc_profile_id": profile_id_used,
            "ipa_status": ipa_status,
            "detectors_present": list(detectors.keys()) if isinstance(detectors, dict) else [],
            "detector_keys_deduped": deduped,
            "kyc_scored_detector_keys": scored_detector_keys,
        },
    }
