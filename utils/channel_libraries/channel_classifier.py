# utils/channel_libraries/channel_classifier.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .registry import get_channel_library


def _safe_float(v: Any) -> float:
    try:
        return float(v or 0.0)
    except Exception:
        return 0.0


def _safe_int(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _safe_str(v: Any) -> str:
    return str(v or "").strip()


def _norm(s: Any) -> str:
    return " ".join(_safe_str(s).upper().split())


def _direction(deposit: Any, withdrawal: Any) -> str:
    d = _safe_float(deposit)
    w = _safe_float(withdrawal)

    if d <= 0 and w <= 0:
        return "none"
    if d > 0 and w <= 0:
        return "credit"
    if w > 0 and d <= 0:
        return "debit"
    return "mixed"


def _token_hit(text_norm: str, keyword_norm: str) -> bool:
    if not keyword_norm:
        return False
    return keyword_norm in text_norm


def _normalize_sof_category(value: Any) -> str:
    s = _norm(value)
    if not s:
        return ""
    if any(x in s for x in ("SALARY", "WAGE", "PAYROLL", "ALLOWANCE", "EMPLOY")):
        return "employment_income"
    if any(x in s for x in ("BUSINESS", "SALES", "REVENUE", "PROCEEDS", "CUSTOMER", "INVOICE", "TAKINGS")):
        return "business_income"
    if any(x in s for x in ("REMIT", "FAMILY", "SUPPORT", "ASSISTANCE")):
        return "family_support"
    if any(x in s for x in ("PENSION", "BENEFIT", "SUPER", "GRANT")):
        return "benefits"
    if any(x in s for x in ("REIMBURSE", "REFUND")):
        return "reimbursement"
    if any(x in s for x in ("LOAN", "CAPITAL", "INJECTION", "SHAREHOLDER")):
        return "capital_or_loan"
    if any(x in s for x in ("CONTRACT", "COMMISSION", "DIVIDEND", "BONUS")):
        return "other_income"
    return "other"


def _rule_match_score(
    channel_code: Optional[int],
    channel_text_norm: str,
    rule: Dict[str, Any],
) -> Tuple[int, List[str]]:
    score = 0
    hits: List[str] = []

    codes = rule.get("codes") or []
    if channel_code is not None and codes:
        try:
            if int(channel_code) in [int(c) for c in codes]:
                score += 3
                hits.append(f"code:{channel_code}")
        except Exception:
            pass

    keywords = rule.get("keywords") or []
    for kw in keywords:
        kw_n = _norm(kw)
        if kw_n and _token_hit(channel_text_norm, kw_n):
            score += 1
            hits.append(f"kw:{kw}")

    return score, hits


def _best_rule(
    channel: Dict[str, Any],
    rules: Dict[str, Dict[str, Any]],
) -> Tuple[Optional[str], Optional[Dict[str, Any]], int, List[str]]:
    code_raw = channel.get("TRANSCODE")
    code: Optional[int] = None
    try:
        code = int(str(code_raw))
    except Exception:
        code = None

    desc = _safe_str(channel.get("DESCRIPTION"))
    raw = _safe_str(channel.get("DESCRIPTION_RAW"))

    phrases = channel.get("top_description_phrases")
    phrase_blob = ""
    if isinstance(phrases, list):
        parts: List[str] = []
        for p in phrases:
            if isinstance(p, dict):
                parts.append(_safe_str(p.get("phrase")))
            else:
                parts.append(_safe_str(p))
        phrase_blob = " ".join([x for x in parts if x])

    text_norm = _norm(f"{desc} {raw} {phrase_blob}".strip())

    best_key: Optional[str] = None
    best_rule_obj: Optional[Dict[str, Any]] = None
    best_score = 0
    best_hits: List[str] = []

    for rule_key, rule_obj in (rules or {}).items():
        score, hits = _rule_match_score(code, text_norm, rule_obj or {})
        if score > best_score:
            best_score = score
            best_key = rule_key
            best_rule_obj = rule_obj
            best_hits = hits

    if best_score <= 0:
        return None, None, 0, []

    return best_key, best_rule_obj, best_score, best_hits


def _compatibility_with_declared_sof(declared_sof: Any, classified_label: Any) -> Tuple[bool, str]:
    decl = _normalize_sof_category(declared_sof)
    clas = _normalize_sof_category(classified_label)
    if not decl or not clas:
        return False, ""
    compatible = decl == clas or (decl, clas) in {
        ("employment_income", "reimbursement"),
        ("other_income", "employment_income"),
        ("other_income", "reimbursement"),
        ("business_income", "other_income"),
    }
    return compatible, f"declared_sof:{decl}" if compatible else f"declared_sof_mismatch:{decl}->{clas}"


def classify_material_channels(
    material_channels: Optional[List[Dict[str, Any]]],
    client_type: str,
    profile: Optional[str] = None,
    declared_sof: Optional[str] = None,
) -> Dict[str, Any]:
    lib = get_channel_library(client_type, profile) or {}
    lib_profile = lib.get("profile") or f"{client_type}{' - ' + profile if profile else ''}"

    credit_rules = (lib.get("credit_rules") or {}).copy()
    debit_rules = (lib.get("debit_rules") or {}).copy()

    channels = material_channels or []
    out_credit: List[Dict[str, Any]] = []
    out_debit: List[Dict[str, Any]] = []
    unmatched: List[Dict[str, Any]] = []

    for ch in channels:
        dep = _safe_float(ch.get("deposit"))
        wdr = _safe_float(ch.get("withdrawal"))
        cnt = _safe_int(ch.get("count"))
        desc = _safe_str(ch.get("DESCRIPTION"))
        code = ch.get("TRANSCODE")

        dirn = _direction(dep, wdr)

        base = {
            "TRANSCODE": code,
            "DESCRIPTION": desc,
            "deposit": dep,
            "withdrawal": wdr,
            "count": cnt,
            "CR%": _safe_float(ch.get("CR%")),
            "DR%": _safe_float(ch.get("DR%")),
            "direction": dirn,
            "declared_sof": _safe_str(declared_sof),
        }

        if dirn in ("credit", "mixed"):
            rule_key, rule_obj, score, hits = _best_rule(ch, credit_rules)
            label = _safe_str(rule_key)
            sof = _safe_str((rule_obj or {}).get("sof")) or "Unverified"
            declared_match, declared_hint = _compatibility_with_declared_sof(declared_sof, sof)
            if declared_match:
                score += 2
                hits = hits + [declared_hint]

            labeled = {
                **base,
                "label": label,
                "match_score": score,
                "match_hits": hits,
                "sof": sof,
                "pof": None,
                "risk": _safe_str((rule_obj or {}).get("risk")) or ("low" if declared_match and label else ""),
                "notes": _safe_str((rule_obj or {}).get("notes")),
                "declared_sof_match": bool(declared_match),
                "classification_status": (
                    "Profile-consistent" if declared_match else ("Matched" if label else "Unverified")
                ),
            }
            out_credit.append(labeled)
            if not label:
                unmatched.append(labeled)

        if dirn in ("debit", "mixed"):
            rule_key, rule_obj, score, hits = _best_rule(ch, debit_rules)
            label = _safe_str(rule_key)
            pof = _safe_str((rule_obj or {}).get("pof")) or "Unverified"

            labeled = {
                **base,
                "label": label,
                "match_score": score,
                "match_hits": hits,
                "sof": None,
                "pof": pof,
                "risk": _safe_str((rule_obj or {}).get("risk")),
                "notes": _safe_str((rule_obj or {}).get("notes")),
                "declared_sof_match": False,
                "classification_status": "Matched" if label else "Unverified",
            }
            out_debit.append(labeled)
            if not label:
                unmatched.append(labeled)

        if dirn == "none":
            unmatched.append({**base, "label": ""})

    def _sum_amount(items: List[Dict[str, Any]], field: str) -> float:
        return round(sum(_safe_float(x.get(field)) for x in items), 2)

    def _sum_count(items: List[Dict[str, Any]]) -> int:
        return sum(_safe_int(x.get("count")) for x in items)

    credit_summary = {
        "total_credits": _sum_amount(out_credit, "deposit"),
        "channel_count": len(out_credit),
        "txn_count": _sum_count(out_credit),
        "unclassified_count": sum(1 for x in out_credit if not _safe_str(x.get("label"))),
        "profile_consistent_count": sum(1 for x in out_credit if bool(x.get("declared_sof_match"))),
    }

    debit_summary = {
        "total_debits": _sum_amount(out_debit, "withdrawal"),
        "channel_count": len(out_debit),
        "txn_count": _sum_count(out_debit),
        "unclassified_count": sum(1 for x in out_debit if not _safe_str(x.get("label"))),
    }

    return {
        "profile": lib_profile,
        "declared_sof": _safe_str(declared_sof),
        "credit": {"channels": out_credit, "summary": credit_summary},
        "debit": {"channels": out_debit, "summary": debit_summary},
        "unmatched": unmatched,
    }


def build_channel_profile(context: Dict[str, Any]) -> Dict[str, Any]:
    client = context.get("client") or {}
    client_type = _safe_str(client.get("client_type") or client.get("type")) or "Individual"
    profile = client.get("profile") or client.get("individualProfile")
    cd = context.get("client_data") or {}
    profile = profile or cd.get("individualProfile") or cd.get("profile")
    declared_sof = (
        client.get("source_of_funds")
        or client.get("sourceOfFunds")
        or cd.get("source_of_funds")
        or cd.get("sourceOfFunds")
    )
    material_channels = context.get("material_channels") or context.get("channels") or []
    return classify_material_channels(material_channels, client_type, profile, declared_sof=declared_sof)
