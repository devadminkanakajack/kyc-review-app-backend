"""
layering.py
===========

Detects LAYERING typologies inside a single bank account statement.

Layering signals include:
    • Multi-stage movement (rapid CREDIT → DEBIT sequences)
    • Fragmentation of credits (many small → one or two large outward payments)
    • Reconsolidation (many small debits → single large debit)
    • Identity hopping (funds move through multiple counterparties within days)
    • Channel hopping (credits via A, payments via B/C/D within tight windows)
    • Circular indicators (same counterparty appears in both credit & debit)

Returns:
{
    "triggered": bool,
    "strength": 0–1,
    "indicators": [...],
    "flagged_row_ids": [...],   # ✅ row-level listing support
    "raw": { detailed metrics },
    "pattern": "layering" | None
}
"""

from typing import Dict, Any, List, Set, Optional, Tuple
import pandas as pd
import numpy as np
import re
from collections import Counter, defaultdict


# -------------------------------------------------------------------
# Helper: Extract identities used in layering analysis
# -------------------------------------------------------------------
# NOTE: keep your existing regex, but make extraction "prefer masked acct"
MASKED_ACCT_RE = re.compile(r"\*{4}\d{4}")
BO_RE = re.compile(r"\bB/O[:\s]+([A-Z][A-Z\s\.\-']{2,})")
FROM_TO_RE = re.compile(r"\bFROM\s+([A-Z][A-Z\s\.\-']{2,})|\bTO\s+([A-Z][A-Z\s\.\-']{2,})")

GENERIC_EXCLUDE = {
    "PAYMENT", "TRANSFER", "FUND", "ACCOUNT", "ACC", "KINA", "OTHER", "IB", "ECHANNEL",
    "SERVICE", "FEE", "CHARGE", "REVERSAL", "CASH", "DEP", "DEPOSIT", "WITHDRAWAL",
    "POS", "ATM", "ONLINE", "BANK", "BRANCH", "MOBILE"
}


def _clean_name(s: str) -> str:
    s = " ".join(str(s).strip().split())
    s = re.sub(r"[^\w\s\.\-']+$", "", s)
    return s


def _looks_generic(name: str) -> bool:
    if not name:
        return True
    toks = [t for t in re.split(r"\s+", name.upper()) if t]
    if not toks:
        return True
    hits = sum(1 for t in toks if t in GENERIC_EXCLUDE)
    return hits >= max(1, int(0.6 * len(toks)))


def _extract_identity(text: Any) -> Optional[str]:
    """
    Party extraction rules:
    1) Prefer masked acct ****dddd anywhere (covers: '... Julie TO ****6211' -> '****6211')
    2) If B/O: NAME exists -> NAME
    3) Else FROM / TO name
    """
    if not isinstance(text, str):
        return None

    t = text.upper()

    m = MASKED_ACCT_RE.search(t)
    if m:
        return m.group(0)

    m = BO_RE.search(t)
    if m:
        nm = _clean_name(m.group(1))
        return None if _looks_generic(nm) else nm

    m = FROM_TO_RE.search(t)
    if m:
        for i in (1, 2):
            grp = m.group(i)
            if grp:
                nm = _clean_name(grp)
                return None if _looks_generic(nm) else nm

    return None


# -------------------------------------------------------------------
# Party-aware chain clustering (SAFE ADDITION)
# -------------------------------------------------------------------
CHAIN_WINDOW_DAYS = 7            # clustering window (calendar days) for party chains
CHAIN_MIN_CREDIT = 5_000.0       # ignore tiny anchors
CHAIN_MIN_LINK_RATIO = 0.20      # require at least 20% of anchor credit to be paid out in window to be considered a chain
CHAIN_MAX_CHAINS = 200           # cap to prevent bloat in raw output

# Tightening guardrails
MIN_MEANINGFUL_CREDIT = 500.0
MIN_MEANINGFUL_DEBIT = 500.0
LAYERING_TRIGGER_THRESHOLD = 0.50
LAYERING_USAGE_RATIO_THRESHOLD = 0.70
LAYERING_USAGE_SCORE_FLOOR = 0.55

BENIGN_INFLOW_PATTERNS = (
    "SALARY", "WAGES", "PAYROLL", "ALLOWANCE", "STIPEND",
    "REIMBURSEMENT", "REFUND", "PAY REF", "PAYREF",
    "FAMILY USE", "FAMILY SUPPORT", "REMITTANCE",
)

ALLOWED_LAYERING_OUTFLOW_TRANSCODES = {"709", "708", "203", "201", "207", "729"}

HOUSEHOLD_SPEND_PATTERNS = (
    "UTILITY", "UTILITIES", "PNG POWER", "POWER", "WATER", "EASIPAY", "EASY PAY",
    "TELIKOM", "DIGICEL", "BEMOBILE", "VODAFONE", "TOPUP", "TOP UP", "AIRTIME",
    "PHONE CREDIT", "RENT", "BOARD", "BOARDING", "LANDLORD", "LEASE", "TENANCY",
    "SCHOOL", "TUITION", "FEE ASSIST", "FEES ASSIST", "SCHOOL FEE", "HOSPITAL",
    "MEDICAL", "CLINIC", "PHARMACY", "GROCERY", "SUPERMARKET", "SHOP", "STORE",
    "FUEL", "PMV", "BUS FARE", "TAXI", "TRANSPORT", "MARKET", "FOOD", "POS PURCHASE",
)

HOUSEHOLD_MERCHANT_PATTERNS = (
    "BSP", "DATEC", "AIR NIUGINI",
)


def _norm_text(v: Any) -> str:
    return " ".join(str(v or "").upper().split())


def _is_benign_inflow_row(row: pd.Series) -> bool:
    text = _norm_text(f"{row.get('DESCRIPTION_RAW', '')} {row.get('DESCRIPTION', '')} {row.get('IDENTITY', '')}")
    if not text:
        return False
    return any(pat in text for pat in BENIGN_INFLOW_PATTERNS)


def _is_allowed_layering_outflow_row(row: pd.Series) -> bool:
    transcode = str(row.get("TRANSCODE") or "").strip()
    return transcode in ALLOWED_LAYERING_OUTFLOW_TRANSCODES


def _is_household_spend_row(row: pd.Series) -> bool:
    text = _norm_text(f"{row.get('DESCRIPTION_RAW', '')} {row.get('DESCRIPTION', '')} {row.get('IDENTITY', '')}")
    if not text:
        return False

    # Strong utility / household keywords always exclude.
    if any(pat in text for pat in HOUSEHOLD_SPEND_PATTERNS):
        return True

    # POS/ATM-like household consumption guardrail.
    transcode = str(row.get("TRANSCODE") or "").strip()
    if transcode in {"729", "708"}:
        if any(pat in text for pat in HOUSEHOLD_MERCHANT_PATTERNS):
            return True
        generic_household = (
            "POS" in text or "PURCHASE" in text or "ATM" in text or "WITHDRAWAL" in text
        )
        known_counterparty = _norm_text(row.get("IDENTITY", ""))
        if generic_household and (not known_counterparty or known_counterparty in {"UNKNOWN", "NONE", "NAN"}):
            return True

    return False


def _known_non_generic_identity_set(series: pd.Series) -> Set[str]:
    out: Set[str] = set()
    if series is None:
        return out
    for v in series.dropna().astype(str).tolist():
        s = str(v).strip()
        if not s or s.upper() in {"UNKNOWN", "NONE", "NAN"}:
            continue
        if _looks_generic(s):
            continue
        out.add(s)
    return out


def _layering_like_debit_mask(links: pd.DataFrame) -> pd.Series:
    """Return True only for debits contributing to layering-style complexity."""
    if links is None or links.empty:
        return pd.Series(dtype=bool)

    work = links.copy()
    work["DEBIT"] = pd.to_numeric(work.get("DEBIT", 0.0), errors="coerce").fillna(0.0)
    work["DATE"] = pd.to_datetime(work.get("DATE"), errors="coerce").dt.normalize()
    work["IDENTITY"] = work.get("IDENTITY", "").fillna("").astype(str).str.strip()
    work["TRANSCODE"] = work.get("TRANSCODE", "UNKNOWN").fillna("UNKNOWN").astype(str).str.strip()

    allowed_mask = work.apply(_is_allowed_layering_outflow_row, axis=1)
    household_mask = work.apply(_is_household_spend_row, axis=1)
    work = work.loc[allowed_mask & (~household_mask)].copy()
    if work.empty:
        return pd.Series([False] * len(links), index=links.index)

    known_parties = _known_non_generic_identity_set(work["IDENTITY"])
    unique_channels = set(x for x in work["TRANSCODE"].tolist() if x and x.upper() != "UNKNOWN")
    amount_vc = work.loc[work["DEBIT"] > 0, "DEBIT"].round(2).value_counts()
    repeated_amounts = set(amount_vc[amount_vc >= 2].index.tolist()) if not amount_vc.empty else set()
    same_day_dates = set()
    if not work["DATE"].dropna().empty:
        day_counts = work.groupby("DATE").size()
        same_day_dates = set(day_counts[day_counts >= 2].index.tolist())

    complexity_is_present = bool(
        len(work) >= 3
        or len(known_parties) >= 2
        or len(unique_channels) >= 2
        or bool(repeated_amounts)
        or bool(same_day_dates)
    )
    if not complexity_is_present:
        return pd.Series([False] * len(work), index=work.index)

    mask = (work["DEBIT"] >= MIN_MEANINGFUL_DEBIT) & (
        work["IDENTITY"].isin(known_parties)
        | work["TRANSCODE"].isin(unique_channels)
        | work["DEBIT"].round(2).isin(repeated_amounts)
        | work["DATE"].isin(same_day_dates)
    )
    return mask.fillna(False)



def _party_chain_clusters(df: pd.DataFrame) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Build party-aware chains anchored on each credit:
      Anchor = a CREDIT row (>= CHAIN_MIN_CREDIT)
      Links  = all DEBIT rows within [anchor_date, anchor_date + CHAIN_WINDOW_DAYS]
    Compute:
      - parties involved (IDENTITY)
      - channels involved (TRANSCODE if available)
      - lag days distribution
      - fragmentation metrics (count debits, unique parties)
      - structured amount dominance (top debit amount frequency ratio)
      - recurrence fingerprints (party-set + channel-set)
    This is display/analysis only: does NOT alter scoring/trigger/flagged_row_ids.
    """
    if df.empty:
        return [], {
            "chains_built": 0,
            "chains_kept": 0,
            "reason": "empty_df"
        }

    # Expect DATE normalized already; keep safe:
    work = df.copy()
    work["DATE"] = pd.to_datetime(work["DATE"], errors="coerce").dt.normalize()

    # Pre-slice for speed
    credits = work[work["CREDIT"] >= CHAIN_MIN_CREDIT].sort_values("DATE")
    debits = work[work["DEBIT"] > 0].sort_values("DATE")

    if credits.empty or debits.empty:
        return [], {
            "chains_built": int(len(credits)),
            "chains_kept": 0,
            "reason": "no_credits_or_debits"
        }

    chains: List[Dict[str, Any]] = []
    built = 0
    kept = 0

    # For recurrence summary
    fp_counter = Counter()
    party_counter = Counter()
    route_counter = Counter()  # (in_channel -> out_channel) counts

    # Helper: get channel code safely
    def _ch(v: Any) -> str:
        if v is None:
            return "UNKNOWN"
        s = str(v).strip()
        return s if s else "UNKNOWN"

    for _, cr in credits.iterrows():
        built += 1
        if kept >= CHAIN_MAX_CHAINS:
            break

        a_date = cr["DATE"]
        a_amt = float(cr["CREDIT"] or 0.0)
        if a_amt <= 0:
            continue

        a_row_id = int(cr["ROW_ID"]) if pd.notna(cr.get("ROW_ID")) else None
        a_id = cr.get("IDENTITY")
        a_chan = _ch(cr.get("TRANSCODE"))

        end = a_date + pd.Timedelta(days=CHAIN_WINDOW_DAYS)

        # Linked debits within window
        links = debits[(debits["DATE"] >= a_date) & (debits["DATE"] <= end)].copy()
        if links.empty:
            continue

        out_sum = float(links["DEBIT"].sum())
        link_ratio = out_sum / a_amt if a_amt > 0 else 0.0
        if link_ratio < CHAIN_MIN_LINK_RATIO:
            # Not meaningful enough to be considered a chain
            continue

        layering_like_mask = _layering_like_debit_mask(links)
        layering_links = links.loc[layering_like_mask].copy() if len(layering_like_mask) == len(links) else links.iloc[0:0].copy()
        layering_outflow_sum = float(layering_links["DEBIT"].sum()) if not layering_links.empty else 0.0
        layering_usage_ratio = (layering_outflow_sum / a_amt) if a_amt > 0 else 0.0

        # Parties & channels on outflow
        out_parties = [x for x in links["IDENTITY"].dropna().astype(str).tolist() if x.strip()]
        out_channels = [_ch(x) for x in links.get("TRANSCODE", pd.Series(["UNKNOWN"] * len(links))).tolist()]

        uniq_parties = sorted(set(out_parties))
        uniq_out_channels = sorted(set(out_channels))

        # Lag days stats
        lags = (links["DATE"] - a_date).dt.days.astype(int).tolist()
        avg_lag = float(np.mean(lags)) if lags else None
        min_lag = int(min(lags)) if lags else None
        max_lag = int(max(lags)) if lags else None

        # Fragmentation: how split is the outflow?
        debit_count = int((links["DEBIT"] > 0).sum())
        unique_party_count = int(len(uniq_parties))
        fragmentation_index = float(min(1.0, debit_count / 12.0))  # 12+ debits ≈ highly fragmented (display only)

        # Structured amount dominance (repetition)
        dv = links["DEBIT"].round(2)
        if not dv.empty:
            vc = dv.value_counts()
            top_amt = float(vc.index[0])
            top_amt_count = int(vc.iloc[0])
            top_amt_ratio = float(top_amt_count / max(1, len(dv)))
        else:
            top_amt = None
            top_amt_count = 0
            top_amt_ratio = 0.0

        # Fingerprint for recurrence: party-set + out-channel-set (orderless)
        fp = (
            "|".join(uniq_parties[:10]),  # cap to keep fingerprints stable
            "|".join(uniq_out_channels),
        )
        fp_counter[fp] += 1

        # Party recurrence
        for p in uniq_parties:
            party_counter[p] += 1

        # Channel route recurrence (in_channel -> each out_channel)
        for oc in uniq_out_channels:
            route_counter[(a_chan, oc)] += 1

        # Keep link rows compact but useful
        link_rows = []
        for _, r in links.iterrows():
            link_rows.append({
                "row_id": int(r["ROW_ID"]) if pd.notna(r.get("ROW_ID")) else None,
                "date": pd.Timestamp(r["DATE"]).strftime("%Y-%m-%d") if pd.notna(r.get("DATE")) else None,
                "debit": float(r["DEBIT"] or 0.0),
                "identity": r.get("IDENTITY"),
                "transcode": _ch(r.get("TRANSCODE")),
                "lag_days": int((pd.Timestamp(r["DATE"]) - a_date).days) if pd.notna(r.get("DATE")) else None,
            })

        chains.append({
            "anchor": {
                "row_id": a_row_id,
                "date": pd.Timestamp(a_date).strftime("%Y-%m-%d"),
                "credit": a_amt,
                "identity": a_id,
                "transcode": a_chan,
            },
            "window_days": CHAIN_WINDOW_DAYS,
            "outflow_sum": round(out_sum, 2),
            "link_ratio": round(link_ratio, 3),
            "layering_outflow_sum": round(layering_outflow_sum, 2),
            "layering_usage_ratio": round(layering_usage_ratio, 3),
            "debit_count": debit_count,
            "unique_party_count": unique_party_count,
            "unique_parties": uniq_parties,
            "unique_out_channels": uniq_out_channels,
            "lag_days": {
                "avg": round(avg_lag, 2) if avg_lag is not None else None,
                "min": min_lag,
                "max": max_lag,
            },
            "structured_outflow": {
                "top_amount": top_amt,
                "top_amount_count": top_amt_count,
                "top_amount_ratio": round(top_amt_ratio, 3),
            },
            "fragmentation_index": round(fragmentation_index, 3),
            "linked_rows": link_rows,
        })
        kept += 1

    # Recurrence summaries (top N)
    top_fps = fp_counter.most_common(10)
    top_parties = party_counter.most_common(10)
    top_routes = route_counter.most_common(10)

    summary = {
        "chains_built": built,
        "chains_kept": kept,
        "recurrence_top_fingerprints": [
            {
                "party_set": k[0],
                "out_channels": k[1],
                "count": int(v),
            }
            for k, v in top_fps
        ],
        "top_parties": [{"identity": k, "count": int(v)} for k, v in top_parties],
        "top_channel_routes": [{"in_channel": k[0], "out_channel": k[1], "count": int(v)} for k, v in top_routes],
    }

    return chains, summary


# -------------------------------------------------------------------
# CORE LAYERING DETECTOR
# -------------------------------------------------------------------
def _sorted_window_slice(sorted_df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    """
    Fast inclusive date slice on a dataframe already sorted by DATE.
    Avoids repeated boolean mask construction on very large statements.
    """
    if sorted_df.empty:
        return sorted_df.iloc[0:0].copy()

    dates = sorted_df["DATE"]
    left = dates.searchsorted(start_date, side="left")
    right = dates.searchsorted(end_date, side="right")
    return sorted_df.iloc[left:right].copy()


def detect_layering(
    df: pd.DataFrame,
    analysis_cache: Optional[Dict[str, Any]] = None,
    prior_results: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    analysis_cache = analysis_cache or {}
    prior_results = prior_results or {}

    df = df.copy()

    # Preserve original row ids for downstream listing
    if "ROW_ID" not in df.columns:
        df["ROW_ID"] = df.index

    # Clean numeric once
    df["CREDIT"] = pd.to_numeric(df.get("CREDIT", 0), errors="coerce").fillna(0.0)
    df["DEBIT"] = pd.to_numeric(df.get("DEBIT", 0), errors="coerce").fillna(0.0)

    # Ensure DATE (normalize calendar day to keep daily metrics consistent)
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce", dayfirst=True).dt.normalize()
    elif "DATE_STR" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE_STR"], errors="coerce", dayfirst=True).dt.normalize()
    else:
        df["DATE"] = pd.date_range(start="2000-01-01", periods=len(df), freq="D")

    df["DESCRIPTION_RAW"] = df.get("DESCRIPTION_RAW", "").astype(str)
    if "IDENTITY" not in df.columns:
        df["IDENTITY"] = df["DESCRIPTION_RAW"].apply(_extract_identity)
    else:
        ident = df["IDENTITY"].fillna("").astype(str).str.strip()
        fallback_mask = ident.eq("") | ident.str.upper().isin(["UNKNOWN", "NONE", "NAN"])
        if bool(fallback_mask.any()):
            df.loc[fallback_mask, "IDENTITY"] = df.loc[fallback_mask, "DESCRIPTION_RAW"].apply(_extract_identity)

    df = df[df["DATE"].notna()].sort_values(["DATE", "ROW_ID"], kind="mergesort").reset_index(drop=True)

    total_credits = float(df["CREDIT"].sum())
    total_debits = float(df["DEBIT"].sum())

    if total_credits < 5000 or total_debits < 5000:
        return {
            "triggered": False,
            "strength": 0.0,
            "indicators": ["Low activity; layering patterns cannot be confidently assessed."],
            "flagged_row_ids": [],
            "raw": {},
            "pattern": None,
        }

    seeded_row_ids: Set[int] = set()
    for det_name in ("structured_deposits", "structured_payments", "pass_through"):
        det_out = prior_results.get(det_name) if isinstance(prior_results, dict) else None
        if isinstance(det_out, dict):
            for rid in det_out.get("flagged_row_ids", []) or []:
                try:
                    seeded_row_ids.add(int(rid))
                except Exception:
                    pass

    focus_df = df
    focus_mode = False
    if seeded_row_ids and len(df) > 15000 and "ROW_ID" in df.columns:
        hit = df[df["ROW_ID"].isin(sorted(seeded_row_ids))].copy()
        if not hit.empty:
            focus_dates: Set[pd.Timestamp] = set()
            for d in pd.to_datetime(hit["DATE"], errors="coerce").dropna().dt.normalize().tolist():
                for x in pd.date_range(d - pd.Timedelta(days=7), d + pd.Timedelta(days=7), freq="D"):
                    focus_dates.add(pd.Timestamp(x).normalize())
            if focus_dates:
                narrowed = df[df["DATE"].isin(sorted(focus_dates))].copy()
                if not narrowed.empty:
                    focus_df = narrowed
                    focus_mode = True

    analysis_df = focus_df.sort_values(["DATE", "ROW_ID"], kind="mergesort").reset_index(drop=True)
    credits_df = analysis_df[analysis_df["CREDIT"] > 0].copy()
    debits_df = analysis_df[analysis_df["DEBIT"] > 0].copy()

    # Tighten: exclude routine small / benign inflow anchors from layering windows.
    credits_df["__BENIGN_INFLOW__"] = credits_df.apply(_is_benign_inflow_row, axis=1)
    anchor_credits = credits_df[(credits_df["CREDIT"] >= MIN_MEANINGFUL_CREDIT) & (~credits_df["__BENIGN_INFLOW__"])].copy()
    debits_df["__ALLOWED_LAYERING_OUTFLOW__"] = debits_df.apply(_is_allowed_layering_outflow_row, axis=1)
    debits_df["__HOUSEHOLD_SPEND__"] = debits_df.apply(_is_household_spend_row, axis=1)
    meaningful_debits = debits_df[
        (debits_df["DEBIT"] >= MIN_MEANINGFUL_DEBIT)
        & (debits_df["__ALLOWED_LAYERING_OUTFLOW__"])
        & (~debits_df["__HOUSEHOLD_SPEND__"])
    ].copy()
    large_debits = meaningful_debits[meaningful_debits["DEBIT"] >= 10000].copy()

    # --------------------------------------------------
    # Tightened scoring: only meaningful, non-benign anchors contribute.
    # --------------------------------------------------
    windows = [1, 2, 3]
    multi_stage_scores: List[float] = []
    layering_usage_scores: List[float] = []
    credit_to_debit_days: List[int] = []
    qualifying_credit_row_ids: Set[int] = set()
    qualifying_debit_row_ids: Set[int] = set()

    for w in windows:
        for _, credit_row in anchor_credits.iterrows():
            close_debits = _sorted_window_slice(
                meaningful_debits,
                pd.Timestamp(credit_row["DATE"]),
                pd.Timestamp(credit_row["DATE"]) + pd.Timedelta(days=w),
            )
            if close_debits.empty:
                continue

            denom = float(credit_row["CREDIT"]) if float(credit_row["CREDIT"]) > 0 else 1.0
            debit_total = float(close_debits["DEBIT"].sum())
            amt_ratio = min(debit_total, float(credit_row["CREDIT"])) / denom

            unique_out_parties = _known_non_generic_identity_set(close_debits.get("IDENTITY", pd.Series(dtype=object)))
            unique_out_channels = set(close_debits.get("TRANSCODE", pd.Series(dtype=object)).dropna().astype(str).str.strip().tolist())
            same_day = bool((close_debits["DATE"] == pd.Timestamp(credit_row["DATE"])).any())

            layering_like_mask = _layering_like_debit_mask(close_debits)
            layering_like_debits = close_debits.loc[layering_like_mask].copy() if len(layering_like_mask) == len(close_debits) else close_debits.iloc[0:0].copy()
            layering_usage_ratio = min(float(layering_like_debits["DEBIT"].sum()), float(credit_row["CREDIT"])) / denom if not layering_like_debits.empty else 0.0

            qualifies = bool(
                amt_ratio >= 0.65
                and layering_usage_ratio >= LAYERING_USAGE_SCORE_FLOOR
                and (
                    len(close_debits) >= 2
                    or len(unique_out_parties) >= 2
                    or len(unique_out_channels) >= 2
                    or same_day
                )
            )
            if not qualifies:
                continue

            multi_stage_scores.append(float(amt_ratio))
            layering_usage_scores.append(float(layering_usage_ratio))
            first_debit_day = close_debits["DATE"].min()
            credit_to_debit_days.append(int((first_debit_day - credit_row["DATE"]).days))
            try:
                qualifying_credit_row_ids.add(int(credit_row["ROW_ID"]))
            except Exception:
                pass
            for rid in layering_like_debits["ROW_ID"].dropna().astype(int).tolist():
                qualifying_debit_row_ids.add(int(rid))

    multi_stage_score = float(np.mean(multi_stage_scores)) if multi_stage_scores else 0.0
    layering_usage_score = float(np.mean(layering_usage_scores)) if layering_usage_scores else 0.0
    avg_days_credit_to_debit = float(np.mean(credit_to_debit_days)) if credit_to_debit_days else None

    fr_scores: List[float] = []
    for _, debit_row in large_debits.iterrows():
        prior_credits = _sorted_window_slice(
            credits_df,
            pd.Timestamp(debit_row["DATE"]) - pd.Timedelta(days=7),
            pd.Timestamp(debit_row["DATE"]),
        )
        if len(prior_credits) < 3:
            continue

        denom = float(debit_row["DEBIT"]) if float(debit_row["DEBIT"]) > 0 else 1.0
        fr_scores.append(min(float(prior_credits["CREDIT"].sum()), float(debit_row["DEBIT"])) / denom)

    fragmentation_score = float(np.mean(fr_scores)) if fr_scores else 0.0

    # Identity hopping / channel hopping / circularity only across layering-eligible outflows.
    layering_eval_df = pd.concat([anchor_credits, meaningful_debits], axis=0).sort_values(["DATE", "ROW_ID"], kind="mergesort")
    identities = layering_eval_df["IDENTITY"].fillna("").astype(str).tolist()
    identity_switches = sum(1 for i in range(1, len(identities)) if identities[i] != identities[i - 1])
    identity_hop_ratio = identity_switches / max(1, len(identities)) if identities else 0.0

    if "TRANSCODE" in layering_eval_df.columns:
        transcodes = layering_eval_df["TRANSCODE"].astype(str).tolist()
        tc_switches = sum(1 for i in range(1, len(transcodes)) if transcodes[i] != transcodes[i - 1])
        tc_hop_ratio = tc_switches / max(1, len(transcodes))
    else:
        tc_hop_ratio = 0.0

    credit_ids = set(anchor_credits[anchor_credits["CREDIT"] > 0]["IDENTITY"].dropna())
    debit_ids = set(meaningful_debits[meaningful_debits["DEBIT"] > 0]["IDENTITY"].dropna())
    circular_overlap = len(credit_ids.intersection(debit_ids))
    circular_score = min(1.0, circular_overlap / 3)

    score = (
        0.30 * multi_stage_score
        + 0.30 * fragmentation_score
        + 0.20 * identity_hop_ratio
        + 0.10 * tc_hop_ratio
        + 0.10 * circular_score
    )
    score = float(max(0.0, min(score, 1.0)))

    # --------------------------------------------------
    # Row-level listing support (conservative, focus-aware)
    # --------------------------------------------------
    flagged: Set[int] = set(seeded_row_ids if focus_mode else set())

    for _, cr in anchor_credits.iterrows():
        close_debits_3d = _sorted_window_slice(
            meaningful_debits,
            pd.Timestamp(cr["DATE"]),
            pd.Timestamp(cr["DATE"]) + pd.Timedelta(days=3),
        )
        if close_debits_3d.empty:
            continue

        layering_like_mask = _layering_like_debit_mask(close_debits_3d)
        layering_like_debits = close_debits_3d.loc[layering_like_mask].copy() if len(layering_like_mask) == len(close_debits_3d) else close_debits_3d.iloc[0:0].copy()
        if layering_like_debits.empty:
            continue

        denom = float(cr["CREDIT"]) if float(cr["CREDIT"]) > 0 else 1.0
        amt_ratio = min(float(layering_like_debits["DEBIT"].sum()), float(cr["CREDIT"])) / denom
        if amt_ratio >= 0.40:
            try:
                flagged.add(int(cr["ROW_ID"]))
            except Exception:
                pass
            for rid in layering_like_debits["ROW_ID"].dropna().astype(int).tolist():
                flagged.add(int(rid))

    for _, dr in large_debits.iterrows():
        window_all = _sorted_window_slice(
            analysis_df,
            pd.Timestamp(dr["DATE"]) - pd.Timedelta(days=7),
            pd.Timestamp(dr["DATE"]),
        )
        if len(window_all[window_all["CREDIT"] > 0]) < 3:
            continue
        try:
            flagged.add(int(dr["ROW_ID"]))
        except Exception:
            pass
        for rid in window_all["ROW_ID"].dropna().astype(int).tolist():
            flagged.add(int(rid))

    overlap = credit_ids.intersection(debit_ids)
    if overlap:
        circ_rows = df[df["IDENTITY"].isin(overlap)]
        for rid in circ_rows["ROW_ID"].dropna().astype(int).tolist():
            flagged.add(int(rid))

    flagged_row_ids = sorted(flagged)

    party_chain_input = pd.concat([anchor_credits, meaningful_debits], axis=0).sort_values(["DATE", "ROW_ID"], kind="mergesort")
    party_chains, party_chain_summary = _party_chain_clusters(party_chain_input)

    largest_credit_amount = float(df["CREDIT"].max()) if total_credits > 0 else None
    largest_debit_amount = float(df["DEBIT"].max()) if total_debits > 0 else None

    daily_debits = df.groupby("DATE")["DEBIT"].sum()
    dominant_layering_date = daily_debits.idxmax().strftime("%Y-%m-%d") if not daily_debits.empty else None
    same_day_layering_ratio = float(daily_debits.max()) / total_debits if total_debits > 0 and not daily_debits.empty else None

    same_day_multi_debit_count = int(df[df["DEBIT"] > 0].groupby("DATE").size().max() if not df[df["DEBIT"] > 0].empty else 0)
    debit_values = df[df["DEBIT"] > 0]["DEBIT"]
    mirror_amount_count = int(debit_values.value_counts().iloc[0]) if not debit_values.empty else 0

    indicators: List[str] = []
    if multi_stage_score >= 0.4:
        indicators.append("Rapid credit-to-debit movement across short windows (layering behaviour).")
    if fragmentation_score >= 0.4:
        indicators.append("Fragmentation and reconsolidation detected.")
    if layering_usage_score >= LAYERING_USAGE_RATIO_THRESHOLD:
        indicators.append("High percentage of anchor funds was redistributed through layering-like outflows.")
    if identity_hop_ratio >= 0.3:
        indicators.append("Frequent counterparty changes observed.")
    if tc_hop_ratio >= 0.3:
        indicators.append("Multiple payment channels used in quick succession.")
    if circular_score >= 0.3:
        indicators.append("Circular flow detected between counterparties.")
    if focus_mode:
        indicators.append("Layering review used prior detector flags to narrow analysis windows on a high-volume statement.")
    if not indicators:
        indicators.append("No strong layering indicators detected.")

    return {
        "triggered": score >= LAYERING_TRIGGER_THRESHOLD,
        "strength": round(score, 3),
        "indicators": indicators,
        "flagged_row_ids": flagged_row_ids,
        "raw": {
            "multi_stage_score": multi_stage_score,
            "fragmentation_score": fragmentation_score,
            "layering_usage_score": layering_usage_score,
            "layering_usage_ratio_threshold": LAYERING_USAGE_RATIO_THRESHOLD,
            "allowed_outflow_transcodes": sorted(ALLOWED_LAYERING_OUTFLOW_TRANSCODES),
            "household_spend_exclusion_patterns": list(HOUSEHOLD_SPEND_PATTERNS),
            "identity_hop_ratio": identity_hop_ratio,
            "channel_hop_ratio": tc_hop_ratio,
            "circular_score": circular_score,
            "avg_days_credit_to_debit": avg_days_credit_to_debit,
            "same_day_layering_ratio": same_day_layering_ratio,
            "same_day_multi_debit_count": same_day_multi_debit_count,
            "mirror_amount_count": mirror_amount_count,
            "dominant_layering_date": dominant_layering_date,
            "largest_credit_amount": largest_credit_amount,
            "largest_debit_amount": largest_debit_amount,
            "focus_mode": focus_mode,
            "focus_row_count": int(len(analysis_df)),
            "full_row_count": int(len(df)),
            "seeded_row_id_count": int(len(seeded_row_ids)),
            "party_chain_config": {
                "chain_window_days": CHAIN_WINDOW_DAYS,
                "chain_min_credit": CHAIN_MIN_CREDIT,
                "chain_min_link_ratio": CHAIN_MIN_LINK_RATIO,
                "chain_max_chains": CHAIN_MAX_CHAINS,
            },
            "party_chains": party_chains,
            "party_chain_summary": party_chain_summary,
        },
        "pattern": "layering",
    }
