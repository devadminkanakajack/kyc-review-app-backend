# utils/analyze_statement.py

from __future__ import annotations

import inspect
import re
from collections import Counter
from difflib import SequenceMatcher
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd

from utils.channel_libraries import classify_material_channels
from utils.file_parser import (
    _norm_for_alias as _parser_norm_for_alias,
    _tokenize_for_alias as _parser_tokenize_for_alias,
    _is_partial_name_match as _parser_is_partial_name_match,
    _is_same_party_dynamic as _parser_is_same_party_dynamic,
)

from utils.detectors import (
    detect_all_recurrence,
    detect_structured_deposits,
    detect_structured_payments,
    detect_pass_through,
    detect_layering,
    detect_round_figures,
    detect_salary_pattern,
    detect_cash_intensive,
    detect_third_party,
)

from utils.kyc_profile import get_profile
from utils.materiality import should_include_row, MATERIALITY_MIN_AMOUNT


# ----------------------------
# Helpers
# ----------------------------

def _pick_date_col(df: pd.DataFrame) -> Optional[str]:
    candidates = ["DATE", "TXN_DATE", "VALUE_DATE", "POST_DATE", "TRAN_DATE", "DATE_POSTED"]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _op_eval(lhs: Any, op: str, rhs: Any) -> bool:
    try:
        if op == "==":
            return lhs == rhs
        if op == "!=":
            return lhs != rhs
        if op == ">":
            return float(lhs) > float(rhs)
        if op == ">=":
            return float(lhs) >= float(rhs)
        if op == "<":
            return float(lhs) < float(rhs)
        if op == "<=":
            return float(lhs) <= float(rhs)
    except Exception:
        return False
    return False


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
        return any(bool(v.get(k)) for k in ("events", "items", "alerts"))
    if isinstance(v, list):
        return len(v) > 0
    return False


def _extract_flagged_row_ids(detector_out: Any) -> List[int]:
    ids: List[int] = []

    if isinstance(detector_out, dict):
        fr = detector_out.get("flagged_row_ids")
        if isinstance(fr, list):
            for x in fr:
                try:
                    ids.append(int(x))
                except Exception:
                    continue

        clusters = detector_out.get("clusters")
        if isinstance(clusters, list):
            for c in clusters:
                if isinstance(c, dict) and isinstance(c.get("flagged_row_ids"), list):
                    for x in c.get("flagged_row_ids") or []:
                        try:
                            ids.append(int(x))
                        except Exception:
                            continue

    return sorted(set(ids))


def _safe_dt_str(v: Any) -> Optional[str]:
    try:
        ts = pd.to_datetime(v, errors="coerce", dayfirst=True)
        if pd.isna(ts):
            return None
        return ts.strftime("%Y-%m-%d")
    except Exception:
        return None


def _normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _normalize_declared_sof_category(value: Any) -> str:
    s = _normalize_text(str(value or ""))
    if not s:
        return ""
    if any(x in s for x in ("salary", "wage", "payroll", "allowance", "employment")):
        return "employment_income"
    if any(x in s for x in ("business", "revenue", "sales", "proceeds", "takings", "invoice", "customer")):
        return "business_income"
    if any(x in s for x in ("family", "support", "remittance", "assistance")):
        return "family_support"
    if any(x in s for x in ("benefit", "pension", "super", "grant")):
        return "benefits"
    if any(x in s for x in ("reimbursement", "refund")):
        return "reimbursement"
    if any(x in s for x in ("loan", "capital", "injection", "shareholder")):
        return "capital_or_loan"
    if any(x in s for x in ("contract", "commission", "dividend", "bonus")):
        return "other_income"
    return "other"


def _channel_map_from_profile(channel_profile: Dict[str, Any], side: str) -> Dict[str, Dict[str, Any]]:
    block = (channel_profile or {}).get(side) or {}
    out: Dict[str, Dict[str, Any]] = {}
    for ch in block.get("channels") or []:
        code = str(ch.get("TRANSCODE") or "").strip()
        if code:
            out[code] = ch
    return out


def _channel_label_category(ch: Dict[str, Any], direction: str) -> str:
    if not isinstance(ch, dict):
        return ""
    value = ch.get("sof") if direction == "credit" else ch.get("pof")
    return _normalize_declared_sof_category(value)


def _is_low_contextual_risk(ch: Dict[str, Any]) -> bool:
    risk = _normalize_text(str((ch or {}).get("risk") or ""))
    return risk in ("", "low")


def _clean_owner_name_token(value: Any) -> str:
    s = _norm_ident(value)
    if not s:
        return ""
    if re.fullmatch(r"\*{2,}\d{3,}", s):
        return s
    if re.fullmatch(r"\[\d+(?:\.\d+)?\]", s):
        return s
    return s


def _parse_owner_identifier_blob(value: Any) -> List[str]:
    if value is None:
        return []

    if isinstance(value, list):
        raw_parts = value
    else:
        raw = str(value or "")
        raw_parts = re.split(r"[\n,;|]+", raw)
    out: List[str] = []
    seen = set()
    for part in raw_parts:
        token = _clean_owner_name_token(part)
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out

def _extract_last4_identifier(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    masked = re.search(r"\*{2,}(\d{4})", raw)
    if masked:
        return masked.group(1)

    digits = re.sub(r"\D+", "", raw)
    if len(digits) >= 4:
        return digits[-4:]
    return ""


def _normalize_owned_identifier_variants(value: Any) -> List[str]:
    raw = str(value or "").strip()
    if not raw:
        return []

    variants: List[str] = []
    seen = set()

    def _add(v: Any) -> None:
        s = str(v or "").strip()
        if not s:
            return
        s_up = s.upper()
        if s_up not in seen:
            seen.add(s_up)
            variants.append(s_up)

    cleaned = _clean_owner_name_token(raw)
    if cleaned:
        _add(cleaned)

    digits_only = re.sub(r"\D+", "", raw)
    if digits_only:
        _add(digits_only)

    last4 = _extract_last4_identifier(raw)
    if last4:
        _add(last4)
        _add(f"****{last4}")

    return variants


def _expand_owner_identifier_tokens(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        for token in _normalize_owned_identifier_variants(value):
            if token and token not in seen:
                seen.add(token)
                out.append(token)
    return out



def _build_owner_context(client_data: Dict[str, Any]) -> Dict[str, Any]:
    client_data = client_data or {}

    primary_name_candidates = [
        client_data.get("client_name"),
        client_data.get("clientName"),
        client_data.get("account_name"),
        client_data.get("accountName"),
        client_data.get("customer_name"),
        client_data.get("customerName"),
        client_data.get("full_name"),
        client_data.get("fullName"),
        client_data.get("company_name"),
        client_data.get("companyName"),
        client_data.get("business_name"),
        client_data.get("businessName"),
    ]
    related_name_candidates = [
        client_data.get("ubo_name"),
        client_data.get("uboName"),
        client_data.get("beneficial_owner_name"),
        client_data.get("beneficialOwnerName"),
        client_data.get("director_name"),
        client_data.get("directorName"),
    ]

    def _collect_names(values: List[Any]) -> List[str]:
        out: List[str] = []
        seen = set()
        for val in values:
            for token in _parse_owner_identifier_blob(val):
                nm = _clean_owner_name_token(token)
                if nm and nm not in seen:
                    seen.add(nm)
                    out.append(nm)
        return out

    owner_names = _collect_names(primary_name_candidates)
    related_owner_names = _collect_names(related_name_candidates)

    owned_identifiers_raw = _parse_owner_identifier_blob(
        client_data.get("owned_account_identifiers_normalized")
        or client_data.get("ownedAccountIdentifiersNormalized")
        or client_data.get("owned_account_identifiers")
        or client_data.get("ownedAccountIdentifiers")
        or client_data.get("other_owned_accounts")
        or client_data.get("otherOwnedAccounts")
    )
    related_owned_identifiers_raw = _parse_owner_identifier_blob(
        client_data.get("ubo_account_identifiers_normalized")
        or client_data.get("uboAccountIdentifiersNormalized")
        or client_data.get("ubo_account_identifiers")
        or client_data.get("uboAccountIdentifiers")
        or client_data.get("beneficial_owner_account_identifiers")
        or client_data.get("beneficialOwnerAccountIdentifiers")
    )

    owned_identifiers = _expand_owner_identifier_tokens(owned_identifiers_raw)
    related_owned_identifiers = _expand_owner_identifier_tokens(related_owned_identifiers_raw)

    own_notes = " ".join(

        [
            str(client_data.get("own_account_context_notes") or ""),
            str(client_data.get("ownAccountContextNotes") or ""),
            str(client_data.get("ubo_account_notes") or ""),
            str(client_data.get("uboAccountNotes") or ""),
        ]
    ).strip()

    owner_last4 = {x for x in (_extract_last4_identifier(v) for v in owned_identifiers_raw) if x}
    related_owner_last4 = {x for x in (_extract_last4_identifier(v) for v in related_owned_identifiers_raw) if x}

    return {
        "owner_names": owner_names,
        "owner_name_set": set(owner_names),
        "related_owner_names": related_owner_names,
        "related_owner_name_set": set(related_owner_names),
        "owned_identifiers": owned_identifiers,
        "owned_identifiers_raw": owned_identifiers_raw,
        "owned_identifier_set": set(owned_identifiers),
        "owned_last4_set": owner_last4,
        "related_owned_identifiers": related_owned_identifiers,
        "related_owned_identifiers_raw": related_owned_identifiers_raw,
        "related_owned_identifier_set": set(related_owned_identifiers),
        "related_owned_last4_set": related_owner_last4,
        "all_linked_name_set": set(owner_names) | set(related_owner_names),
        "all_linked_identifier_set": set(owned_identifiers) | set(related_owned_identifiers),
        "all_linked_last4_set": owner_last4 | related_owner_last4,
        "own_notes": own_notes,
    }


_UNVERIFIED_OWNED_TRANSFER_PATTERNS = [
    r"\bTT\b",
    r"\bTELEGRAPHIC\b",
    r"\bSWIFT\b",
    r"\bINTERNATIONAL\b",
]


def _has_token_words(text: str, token: str) -> bool:
    parts = [t for t in _tok(token) if len(t) >= 3]
    if not parts:
        return False
    return all(re.search(rf"\b{re.escape(t)}\b", text) for t in parts[:3])


def _row_owner_linkage(row: pd.Series, owner_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    owner_context = owner_context or {}

    owner_names = set(owner_context.get("owner_name_set") or set())
    related_owner_names = set(owner_context.get("related_owner_name_set") or set())
    all_names = set(owner_context.get("all_linked_name_set") or set())
    owned_identifiers = set(owner_context.get("owned_identifier_set") or set())
    related_identifiers = set(owner_context.get("related_owned_identifier_set") or set())
    all_identifiers = set(owner_context.get("all_linked_identifier_set") or set())
    owned_last4 = set(owner_context.get("owned_last4_set") or set())
    related_last4 = set(owner_context.get("related_owned_last4_set") or set())
    all_last4 = set(owner_context.get("all_linked_last4_set") or set())

    raw_desc = str(row.get("DESCRIPTION_RAW") or "")
    identity = _clean_owner_name_token(row.get("IDENTITY"))
    transcode = str(row.get("TRANSCODE") or "").strip()
    text = " ".join(str(x or "") for x in [row.get("DESCRIPTION"), raw_desc, identity, transcode]).upper()
    identity_last4 = _extract_last4_identifier(identity)
    text_last4_hits = set(re.findall(r"\*{2,}(\d{4})", text))

    explicit_self_patterns = [
        r"OWN ACCOUNT",
        r"OWN ACC",
        r"SELF TRANSFER",
        r"TRANSFER BETWEEN OWN ACCOUNTS",
        r"MY ACCOUNT",
        r"SAME CUSTOMER",
        r"INTERNAL TRANSFER",
    ]
    has_explicit_self = any(re.search(p, text, flags=re.IGNORECASE) for p in explicit_self_patterns)

    transfer_markers = [
        r"TRANSFER",
        r"TRF",
        r"IB",
        r"MB",
        r"ECHANNEL",
        r"DIRECT CREDIT",
        r"TO",
        r"FROM",
    ]
    has_transfer_context = any(re.search(p, text, flags=re.IGNORECASE) for p in transfer_markers) or transcode in {"189", "703", "709"}
    unverified_owned_transfer = any(re.search(p, text, flags=re.IGNORECASE) for p in _UNVERIFIED_OWNED_TRANSFER_PATTERNS)

    matched = False
    link_type = ""

    if identity and identity in owned_identifiers:
        matched = True
        link_type = "declared_owned_account"
    elif identity and identity in related_identifiers:
        matched = True
        link_type = "related_owner_account"
    elif identity_last4 and identity_last4 in owned_last4:
        matched = True
        link_type = "declared_owned_account_last4"
    elif identity_last4 and identity_last4 in related_last4:
        matched = True
        link_type = "related_owner_account_last4"
    elif text_last4_hits and (text_last4_hits & owned_last4):
        matched = True
        link_type = "declared_owned_account_in_text"
    elif text_last4_hits and (text_last4_hits & related_last4):
        matched = True
        link_type = "related_owner_account_in_text"
    elif has_transfer_context and identity and identity in owner_names:
        matched = True
        link_type = "declared_owner_name"
    elif has_transfer_context and identity and identity in related_owner_names:
        matched = True
        link_type = "related_owner_name"
    elif all_identifiers and any(tok and tok in text for tok in all_identifiers):
        matched = True
        link_type = "identifier_in_text"
    elif all_last4 and text_last4_hits and (text_last4_hits & all_last4):
        matched = True
        link_type = "last4_identifier_in_text"
    elif has_transfer_context and all_names:
        for nm in all_names:
            if _has_token_words(text, nm):
                matched = True
                link_type = "name_in_transfer_text"
                break
    elif has_explicit_self:
        matched = True
        link_type = "explicit_self_transfer"

    return {
        "matched": matched,
        "unverified_owned_transfer": unverified_owned_transfer,
        "has_transfer_context": has_transfer_context,
        "link_type": link_type,
        "identity": identity,
        "identity_last4": identity_last4,
        "matched_last4": sorted(text_last4_hits & all_last4) if all_last4 else [],
    }


def _row_looks_like_self_transfer(row: pd.Series, owner_context: Optional[Dict[str, Any]]) -> bool:
    linkage = _row_owner_linkage(row, owner_context)
    return bool(linkage.get("matched")) and not bool(linkage.get("unverified_owned_transfer"))


def _should_suppress_detector_reason(

    det_key: str,
    row: pd.Series,
    client_type: str,
    individual_profile: Optional[str],
    declared_sof_category: str,
    credit_channel_map: Dict[str, Dict[str, Any]],
    debit_channel_map: Dict[str, Dict[str, Any]],
    detectors: Dict[str, Any],
    owner_context: Optional[Dict[str, Any]] = None,
) -> bool:
    transcode = str(row.get("TRANSCODE") or "").strip()
    credit = float(row.get("CREDIT") or 0.0) if row is not None else 0.0
    debit = float(row.get("DEBIT") or 0.0) if row is not None else 0.0
    direction = "credit" if credit > 0 else ("debit" if debit > 0 else "")
    ch = credit_channel_map.get(transcode, {}) if direction == "credit" else debit_channel_map.get(transcode, {})
    channel_cat = _channel_label_category(ch, direction)
    profile_txt = _normalize_text(individual_profile or "")
    ctype_txt = _normalize_text(client_type or "")

    salary_det = detectors.get("salary_pattern") or {}
    salary_wash = bool((salary_det or {}).get("salary_wash_flag"))

    if _row_looks_like_self_transfer(row, owner_context):
        return True

    if det_key == "salary_pattern":
        return not salary_wash

    if det_key == "recurrence" and channel_cat in {"employment_income", "family_support", "benefits", "business_income", "reimbursement"}:
        return True

    if det_key == "structured_deposits" and direction == "credit":
        if credit < 4950 and channel_cat in {"employment_income", "family_support", "benefits", "reimbursement", "business_income", "other_income"}:
            return True
        if credit < 4950 and declared_sof_category and declared_sof_category == channel_cat:
            return True

    if det_key == "third_party" and direction == "credit":
        strength = float((detectors.get("third_party") or {}).get("strength") or 0.0)
        if channel_cat in {"employment_income", "family_support", "benefits", "reimbursement"} and strength < 0.60:
            return True

    if det_key == "layering" and direction == "debit":
        strength = float((detectors.get("layering") or {}).get("strength") or 0.0)
        if ctype_txt == "individual" and transcode in {"708", "709", "719", "729"} and _is_low_contextual_risk(ch) and strength < 0.65:
            return True
        if "employed" in profile_txt and transcode in {"708", "709", "719", "729"} and strength < 0.65:
            return True

    return False


def _top_groups(
    df: pd.DataFrame,
    key_series: pd.Series,
    max_items: int = 3,
    min_count: int = 2,
) -> List[Dict[str, Any]]:
    if df is None or df.empty or key_series is None or key_series.empty:
        return []

    tmp = df.copy()
    tmp["__KEY"] = key_series
    tmp = tmp[tmp["__KEY"].notna() & (tmp["__KEY"].astype(str).str.strip() != "")]
    if tmp.empty:
        return []

    g = (
        tmp.groupby("__KEY")["__AMT"]
        .agg(["count", "sum"])
        .reset_index()
        .sort_values(by=["count", "sum"], ascending=[False, False])
    )
    g = g[g["count"] >= int(min_count)]
    if g.empty:
        return []

    g = g.head(max(1, int(max_items)))
    out: List[Dict[str, Any]] = []
    for _, row in g.iterrows():
        label = str(row["__KEY"])
        out.append(
            {
                "label": label,
                "count": int(row["count"]),
                "total_amount": round(float(row["sum"] or 0.0), 2),
            }
        )
    return out


def _top_phrases(series: pd.Series, top_n: int = 10) -> List[Dict[str, Any]]:
    vals = series.dropna().astype(str).map(_normalize_text)
    vals = vals[vals != ""]
    counts = Counter(vals.tolist())
    return [{"phrase": k, "count": int(v)} for k, v in counts.most_common(top_n)]


def _is_generic_or_unknown(desc: str) -> bool:
    d = _normalize_text(desc)
    if not d:
        return True

    if len(d) < 6:
        return True

    generic = {
        "transfer", "trf", "tfr", "payment", "pay", "deposit", "cash", "withdrawal",
        "teller", "pos", "atm", "mb", "mobile", "online", "internet", "funds",
        "misc", "other", "unknown", "n/a"
    }

    toks = [t for t in re.split(r"[^a-z0-9]+", d) if t]
    if not toks:
        return True

    generic_hits = sum(1 for t in toks if t in generic)
    return (generic_hits / max(1, len(toks))) >= 0.6


# ----------------------------
# Presentation helpers
# ----------------------------

def _blank_zero_money(v: Any) -> str:
    try:
        x = float(v or 0.0)
    except Exception:
        return ""
    return "" if abs(x) < 1e-9 else f"{x:.2f}"


def _blank_zero_pct(v: Any) -> str:
    try:
        x = float(v or 0.0)
    except Exception:
        return ""
    return "" if abs(x) < 1e-9 else f"{x:.2f}%"


# ============================================================
# IDENTIFIER NORMALISATION + MERGING
# ============================================================

_ALIAS_STOPWORDS = {
    "SERVICE", "SERVICES", "FEE", "FEES",
    "PAYMENT", "PAYMENTS", "PAY", "PMNT", "PMT",
    "OF", "FOR", "KINA", "BA",
    "FAMILY", "ASSISTANCE", "SUPPORT",
}

_SUFFIX_NOISE_PATTERNS = [
    r"\bPAYMENTS?\b.*$",
    r"\bPAY\b.*$",
    r"\bSERVICE\b.*$",
    r"\bSERVICES\b.*$",
    r"\bSERVICE\s+FEE\b.*$",
    r"\bKINA\s+BA\b.*$",
    r"\bPAYMEN\b.*$",
    r"\bPAYM\b.*$",
]


def _norm_ident(s: Any) -> str:
    return _parser_norm_for_alias(s) or ""



def _extract_bracket_ident(desc: Any) -> str:
    text = str(desc or "")
    m = re.search(r"\[([^\]]+)\]", text)
    if not m:
        return ""
    raw_inner = str(m.group(1) or "").strip()
    if re.fullmatch(r"\d+(?:\.\d+)?", raw_inner):
        return f"[{raw_inner}]"
    return _norm_ident(raw_inner)


def _tok(s: Any) -> List[str]:
    return list(_parser_tokenize_for_alias(s))



def _display_ident(tokens: List[str]) -> str:
    if not tokens:
        return "UNKNOWN"
    if len(tokens) == 1:
        return tokens[0]
    return " ".join(tokens[:3])


def _compact(s: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(s or "").upper())


def _seq_ratio(a: str, b: str) -> float:
    a2 = _compact(a)
    b2 = _compact(b)
    if not a2 or not b2:
        return 0.0
    return SequenceMatcher(None, a2, b2).ratio()


def _is_partial_match(short_tokens: List[str], long_tokens: List[str]) -> bool:
    return bool(_parser_is_partial_name_match(short_tokens, long_tokens))



def _is_fuzzy_same_party(a: str, b: str) -> bool:
    return bool(_parser_is_same_party_dynamic(a, b))



def _choose_better_anchor(a: str, b: str) -> str:
    na = _norm_ident(a)
    nb = _norm_ident(b)
    if (
        na.startswith("ID:")
        or nb.startswith("ID:")
        or re.fullmatch(r"\[\d+(?:\.\d+)?\]", na)
        or re.fullmatch(r"\[\d+(?:\.\d+)?\]", nb)
    ):
        return na if len(na) >= len(nb) else nb
    score_a = (len(_tok(na)), len(re.sub(r"[^A-Z]", "", na)), len(na))
    score_b = (len(_tok(nb)), len(re.sub(r"[^A-Z]", "", nb)), len(nb))
    return na if score_a >= score_b else nb



def _merge_identifier_labels(labels: List[str]) -> Dict[str, str]:
    uniq = []
    seen = set()
    for x in labels:
        nx = _norm_ident(x)
        if not nx:
            continue
        if nx not in seen:
            seen.add(nx)
            uniq.append(nx)

    if not uniq:
        return {}

    uniq_sorted = sorted(
        uniq,
        key=lambda s: (len(_tok(s)), len(s)),
        reverse=True,
    )

    groups: List[Dict[str, Any]] = []
    mapping: Dict[str, str] = {}

    for lab in uniq_sorted:
        toks = _tok(lab)
        if not toks:
            mapping[lab] = "UNKNOWN"
            continue

        merged_into = None
        for g in groups:
            if _is_fuzzy_same_party(lab, g["anchor"]):
                merged_into = g
                break

        if merged_into is None:
            anchor = lab
            groups.append({
                "anchor": anchor,
                "tokens": _tok(anchor),
                "display": _display_ident(_tok(anchor)),
                "members": [lab],
            })
            mapping[lab] = anchor
        else:
            merged_into["members"].append(lab)
            merged_into["anchor"] = _choose_better_anchor(merged_into["anchor"], lab)
            merged_into["tokens"] = _tok(merged_into["anchor"])
            merged_into["display"] = _display_ident(merged_into["tokens"])
            mapping[lab] = merged_into["anchor"]

    # remap all members to final anchors
    final_map: Dict[str, str] = {}
    for g in groups:
        final_anchor = g["anchor"]
        for m in g["members"]:
            final_map[m] = final_anchor

    return final_map


def _group_suspicious_by_identifier(
    sub: pd.DataFrame,
    date_col: str,
    direction: str,
    row_reasons: Dict[int, List[str]],
    top_n: int = 5,
) -> Dict[str, Any]:
    if sub is None or sub.empty:
        return {"count": 0, "total": 0.0, "date_min": None, "date_max": None, "identifiers": [], "reason_counts": {}}

    amt_col = "CREDIT" if direction == "credit" else "DEBIT"
    if amt_col not in sub.columns:
        return {"count": 0, "total": 0.0, "date_min": None, "date_max": None, "identifiers": [], "reason_counts": {}}

    sdir = sub[sub[amt_col] > 0].copy()
    if sdir.empty:
        return {"count": 0, "total": 0.0, "date_min": None, "date_max": None, "identifiers": [], "reason_counts": {}}

    if "IDENTITY" in sdir.columns:
        ident = sdir["IDENTITY"].map(_norm_ident)
    else:
        ident = pd.Series([""] * len(sdir), index=sdir.index)

    if "DESCRIPTION_RAW" in sdir.columns:
        bracket = sdir["DESCRIPTION_RAW"].map(_extract_bracket_ident)
        ident = ident.where(ident.astype(str).str.strip() != "", bracket)

    ident = ident.fillna("").astype(str)
    ident = ident.where(ident.str.strip() != "", "UNKNOWN")
    sdir["__IDENT"] = ident

    map_to_group = _merge_identifier_labels(sdir["__IDENT"].astype(str).tolist())
    if map_to_group:
        sdir["__IDENT_GROUP"] = sdir["__IDENT"].map(lambda x: map_to_group.get(_norm_ident(x), _norm_ident(x) or "UNKNOWN"))
    else:
        sdir["__IDENT_GROUP"] = sdir["__IDENT"].map(lambda x: _norm_ident(x) or "UNKNOWN")

    sdir["__DATE"] = pd.to_datetime(sdir[date_col], errors="coerce", dayfirst=True)
    sdir["__AMT"] = pd.to_numeric(sdir[amt_col], errors="coerce").fillna(0.0)

    dseries = sdir["__DATE"].dropna()
    date_min = dseries.min().strftime("%Y-%m-%d") if not dseries.empty else None
    date_max = dseries.max().strftime("%Y-%m-%d") if not dseries.empty else None

    reason_counts: Dict[str, int] = {}
    if "ROW_ID" in sdir.columns:
        for rid in sdir["ROW_ID"].dropna().astype(int).tolist():
            for det in row_reasons.get(int(rid), []):
                reason_counts[str(det)] = int(reason_counts.get(str(det), 0) + 1)

    g = (
        sdir.groupby("__IDENT_GROUP")
        .agg(
            count=("__AMT", "size"),
            total=("__AMT", "sum"),
            date_min=("__DATE", "min"),
            date_max=("__DATE", "max"),
        )
        .reset_index()
        .sort_values(by=["count", "total"], ascending=[False, False])
    )

    id_items: List[Dict[str, Any]] = []
    for _, r in g.head(max(1, int(top_n))).iterrows():
        dm = r["date_min"]
        dx = r["date_max"]
        id_items.append(
            {
                "identifier": str(r["__IDENT_GROUP"]),
                "count": int(r["count"] or 0),
                "total": round(float(r["total"] or 0.0), 2),
                "date_min": dm.strftime("%Y-%m-%d") if pd.notna(dm) else None,
                "date_max": dx.strftime("%Y-%m-%d") if pd.notna(dx) else None,
            }
        )

    return {
        "count": int(len(sdir)),
        "total": round(float(sdir["__AMT"].sum()), 2),
        "date_min": date_min,
        "date_max": date_max,
        "identifiers": id_items,
        "reason_counts": reason_counts,
    }


def _prepare_analysis_df(df: pd.DataFrame, code_lookup, client_type: str) -> Tuple[pd.DataFrame, str]:
    work = df.copy()

    if "TRANSCODE" not in work.columns:
        work["TRANSCODE"] = "UNKNOWN"
    if "DESCRIPTION_RAW" not in work.columns:
        work["DESCRIPTION_RAW"] = ""
    if "DEBIT" not in work.columns:
        work["DEBIT"] = 0.0
    if "CREDIT" not in work.columns:
        work["CREDIT"] = 0.0
    if "ROW_ID" not in work.columns:
        work["ROW_ID"] = work.index

    bf_mask = (
        work["DESCRIPTION_RAW"].astype(str).str.contains("B/F", case=False, na=False)
        | work["TRANSCODE"].astype(str).str.contains("B/F", case=False, na=False)
    )
    if bf_mask.any():
        work = work.loc[~bf_mask].copy()

    work["TRANSCODE"] = work["TRANSCODE"].astype(str).replace(["nan", "NaN", "None", ""], "UNKNOWN")
    work["DEBIT"] = pd.to_numeric(work["DEBIT"], errors="coerce").fillna(0.0)
    work["CREDIT"] = pd.to_numeric(work["CREDIT"], errors="coerce").fillna(0.0)

    if "DESCRIPTION" not in work.columns:
        work["DESCRIPTION"] = work["TRANSCODE"].map(lambda x: code_lookup.get_description(x, client_type))
    else:
        missing_desc = work["DESCRIPTION"].astype(str).str.strip().isin(["", "nan", "NaN", "None"])
        if bool(missing_desc.any()):
            work.loc[missing_desc, "DESCRIPTION"] = work.loc[missing_desc, "TRANSCODE"].map(
                lambda x: code_lookup.get_description(x, client_type)
            )
    work["DESCRIPTION"] = work["DESCRIPTION"].astype(str).replace(["nan", "NaN", "None", ""], "UNKNOWN")

    date_col = _pick_date_col(work) or "DATE"
    if date_col not in work.columns:
        work[date_col] = pd.NaT

    work[date_col] = pd.to_datetime(work[date_col], errors="coerce", dayfirst=True)
    if "DATE_STR" not in work.columns:
        work["DATE_STR"] = work[date_col].dt.strftime("%Y-%m-%d")

    # normalize identity field if it already exists from parser
    if "IDENTITY" in work.columns:
        work["IDENTITY"] = work["IDENTITY"].map(_norm_ident).replace("", None)

    work = work.sort_values(by=[date_col, "ROW_ID"], kind="mergesort").reset_index(drop=True)
    return work, date_col


def _build_analysis_cache(df: pd.DataFrame, date_col: str) -> Dict[str, Any]:
    credit_mask = pd.to_numeric(df.get("CREDIT", 0.0), errors="coerce").fillna(0.0) > 0
    debit_mask = pd.to_numeric(df.get("DEBIT", 0.0), errors="coerce").fillna(0.0) > 0

    ident_series = df.get("IDENTITY", pd.Series(["" for _ in range(len(df))], index=df.index)).fillna("").astype(str).str.strip()
    known_identity_mask = (~ident_series.isin(["", "UNKNOWN", "None", "nan", "NaN", "NAN"]))

    desc_series = df.get("DESCRIPTION", pd.Series(["" for _ in range(len(df))], index=df.index)).fillna("").astype(str)
    raw_series = df.get("DESCRIPTION_RAW", pd.Series(["" for _ in range(len(df))], index=df.index)).fillna("").astype(str)
    cash_like_mask = (
        desc_series.str.contains(r"cash|atm|teller", case=False, na=False)
        | raw_series.str.contains(r"cash|atm|teller", case=False, na=False)
    )

    amt_series = df[[c for c in ["DEBIT", "CREDIT"] if c in df.columns]].abs().max(axis=1) if not df.empty else pd.Series(dtype=float)
    round_amount_mask = amt_series.fillna(0.0).round(0).eq(amt_series.fillna(0.0))

    return {
        "date_col": date_col,
        "row_count": int(len(df)),
        "credit_mask": credit_mask,
        "debit_mask": debit_mask,
        "known_identity_mask": known_identity_mask,
        "unknown_identity_mask": ~known_identity_mask,
        "cash_like_mask": cash_like_mask,
        "round_amount_mask": round_amount_mask,
        "credits_df": df.loc[credit_mask].copy(),
        "debits_df": df.loc[debit_mask].copy(),
    }


def _call_detector(fn, df: pd.DataFrame, analysis_cache: Dict[str, Any], prior_results: Dict[str, Any], date_col: str):
    try:
        sig = inspect.signature(fn)
        params = sig.parameters
    except Exception:
        params = {}

    kwargs: Dict[str, Any] = {}
    if "analysis_cache" in params:
        kwargs["analysis_cache"] = analysis_cache
    if "prior_results" in params:
        kwargs["prior_results"] = prior_results
    if "date_col" in params:
        kwargs["date_col"] = date_col

    return fn(df, **kwargs)


def _run_detector_pipeline(
    df: pd.DataFrame,
    date_col: str,
    analysis_cache: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    detectors: Dict[str, Any] = {}
    analysis_cache = analysis_cache or {}

    pipeline = [
        ("round_figures", detect_round_figures),
        ("cash_intensive", detect_cash_intensive),
        ("third_party", detect_third_party),
        ("salary_pattern", detect_salary_pattern),
        ("recurrence", detect_all_recurrence),
        ("structured_deposits", detect_structured_deposits),
        ("structured_payments", detect_structured_payments),
        ("pass_through", detect_pass_through),
        ("layering", detect_layering),
    ]

    for name, fn in pipeline:
        try:
            print(f"   • running detector: {name}")
            detectors[name] = _call_detector(
                fn=fn,
                df=df,
                analysis_cache=analysis_cache,
                prior_results=detectors,
                date_col=date_col,
            )
            print(f"   ✓ detector completed: {name}")
        except Exception as e:
            print(f"   ✗ detector failed: {name} -> {e}")
            raise RuntimeError(f"Detector '{name}' failed: {e}") from e

    return detectors


# ============================================================
# CHANNEL / DESCRIPTION ANALYSIS (NO AI)
# ============================================================

def build_channel_analysis(
    df: pd.DataFrame,
    pivot_summary: List[Dict[str, Any]],
    narrative_library: Optional[Dict[str, Any]] = None,
    suspicious_cap: int = 200,
) -> Dict[str, Any]:
    narrative_library = narrative_library or {}

    totals_by_code: Dict[str, Dict[str, Any]] = {}
    for r in pivot_summary:
        code = str(r.get("TRANSCODE") or "")
        if code and code.upper() != "TOTAL":
            totals_by_code[code] = r

    channels: List[Dict[str, Any]] = []

    if "TRANSCODE" not in df.columns:
        return {"channels": channels}

    date_col = _pick_date_col(df)

    for code, g in df.groupby("TRANSCODE", dropna=False):
        code_str = str(code)

        top_ph = _top_phrases(g.get("DESCRIPTION_RAW", pd.Series([], dtype=str)), top_n=10)

        susp_rows: List[Dict[str, Any]] = []
        if "DESCRIPTION_RAW" in g.columns:
            for _, row in g.iterrows():
                raw_desc = str(row.get("DESCRIPTION_RAW") or "")
                if _is_generic_or_unknown(raw_desc):
                    susp_rows.append({
                        "date": row.get(date_col) if (date_col and date_col in row) else (row.get("DATE") if "DATE" in row else None),
                        "description_raw": raw_desc,
                        "debit": float(row.get("DEBIT") or 0.0) if "DEBIT" in row else 0.0,
                        "credit": float(row.get("CREDIT") or 0.0) if "CREDIT" in row else 0.0,
                    })

        narrative = ""
        sof = ""
        pof = ""
        relationship = ""
        confidence = 0.30

        raw_text_blob = " | ".join([p["phrase"] for p in top_ph])

        matched_rule = None
        for _, rule in narrative_library.items():
            phrases = [str(x).lower() for x in (rule.get("match_phrases") or [])]
            if any(ph in raw_text_blob for ph in phrases):
                matched_rule = rule
                break

        if matched_rule:
            sof = matched_rule.get("sof", "") or ""
            pof = matched_rule.get("pof", "") or ""
            relationship = matched_rule.get("relationship", "") or ""
            narrative = matched_rule.get("narrative", "") or ""
            try:
                confidence = float(matched_rule.get("confidence", 0.70) or 0.70)
            except Exception:
                confidence = 0.70
        else:
            if top_ph:
                narrative = (
                    f"Channel activity shows frequent references to '{top_ph[0]['phrase']}'. "
                    f"Transaction descriptions are used as the primary basis for SoF/PoF inference. "
                    f"Entries with weak or generic descriptions require clarification and supporting evidence."
                )
            else:
                narrative = (
                    "Channel activity contains insufficient description detail to form a clear SoF/PoF narrative. "
                    "Treat as unknown until supporting evidence is obtained."
                )

        totals = totals_by_code.get(code_str, {})
        channels.append({
            "TRANSCODE": code_str,
            "DESCRIPTION": totals.get("DESCRIPTION", ""),
            "deposit": totals.get("deposit", 0.0),
            "withdrawal": totals.get("withdrawal", 0.0),
            "count": totals.get("count", int(len(g))),
            "CR%": totals.get("CR%", 0.0),
            "DR%": totals.get("DR%", 0.0),
            "top_description_phrases": top_ph,
            "sof_inference": sof,
            "pof_inference": pof,
            "relationship_inference": relationship,
            "narrative": narrative,
            "confidence": round(float(confidence), 2),
            "suspicious_transactions": susp_rows[: max(0, int(suspicious_cap))],
            "suspicious_transactions_count": len(susp_rows),
        })

    return {"channels": channels}


# ============================================================
# KYC FEATURE EXTRACTION
# ============================================================

def _statement_span_months(raw_df: pd.DataFrame) -> float:
    date_col = _pick_date_col(raw_df)
    if not date_col or date_col not in raw_df.columns:
        return 1.0

    try:
        s = pd.to_datetime(raw_df[date_col], errors="coerce", dayfirst=True)
        s = s.dropna()
        if s.empty:
            return 1.0
        days = max(1, int((s.max() - s.min()).days) + 1)
        return max(1.0, days / 30.0)
    except Exception:
        return 1.0


def _channel_sums_from_pivot(
    pivot_summary: List[Dict[str, Any]]
) -> Tuple[float, float, Dict[str, float], Dict[str, float]]:
    total_credit = 0.0
    total_debit = 0.0
    credit_b: Dict[str, float] = {"cash": 0.0, "direct_credit": 0.0, "cheque": 0.0, "transfer_in": 0.0}
    debit_b: Dict[str, float] = {"atm": 0.0, "pos": 0.0, "billpay": 0.0, "ech_out": 0.0, "direct_debit": 0.0}

    for r in pivot_summary:
        if str(r.get("TRANSCODE")).upper() == "TOTAL":
            total_credit = float(r.get("deposit") or 0.0)
            total_debit = float(r.get("withdrawal") or 0.0)
            continue

        desc = str(r.get("DESCRIPTION") or "").lower()
        cr = float(r.get("deposit") or 0.0)
        dr = float(r.get("withdrawal") or 0.0)

        if "cash deposit" in desc or "cash dep" in desc:
            credit_b["cash"] += cr
        if "salary" in desc or "payroll" in desc or "wages" in desc or "direct credit" in desc:
            credit_b["direct_credit"] += cr
        if "cheque" in desc:
            credit_b["cheque"] += cr
        if "transfer" in desc or "trf" in desc:
            credit_b["transfer_in"] += cr

        if "atm" in desc or "cash withdrawal" in desc:
            debit_b["atm"] += dr
        if "pos" in desc or "eftpos" in desc:
            debit_b["pos"] += dr
        if "bill" in desc or "utility" in desc:
            debit_b["billpay"] += dr
        if any(k in desc for k in ("internet", "online", "mobile", "mb", "ib", "telebank", "transfer")):
            debit_b["ech_out"] += dr
        if "direct debit" in desc:
            debit_b["direct_debit"] += dr

    return total_credit, total_debit, credit_b, debit_b


def _extract_kyc_features(
    pivot_summary: List[Dict[str, Any]],
    raw_df: pd.DataFrame,
    detectors: Dict[str, Any],
    client_data: Dict[str, Any],
) -> Dict[str, Any]:
    features: Dict[str, Any] = {}

    months_equiv = _statement_span_months(raw_df)
    total_credit, total_debit, credit_b, debit_b = _channel_sums_from_pivot(pivot_summary)

    features["cash_deposit_share_pct"] = round((credit_b["cash"] / total_credit) * 100.0, 2) if total_credit else 0.0

    total_count = 0
    for r in pivot_summary:
        if str(r.get("TRANSCODE")).upper() == "TOTAL":
            total_count = int(r.get("count") or 0)
            break
    features["tx_count_monthly"] = round(total_count / months_equiv, 2) if months_equiv else float(total_count)

    intl_count = 0
    for r in pivot_summary:
        if str(r.get("TRANSCODE")).upper() == "TOTAL":
            continue
        desc = str(r.get("DESCRIPTION") or "").lower()
        if "swift" in desc or "international" in desc or "telegraphic" in desc or "tt " in desc:
            intl_count += int(r.get("count") or 0)
    features["intl_transfer_count_monthly"] = round(intl_count / months_equiv, 2) if months_equiv else float(intl_count)

    features["single_credit_max_multiple_of_median"] = 0.0
    try:
        if "CREDIT" in raw_df.columns:
            credits = raw_df.loc[raw_df["CREDIT"] > 0, "CREDIT"]
            if len(credits) > 0:
                mx = float(credits.max())
                med_val = float(credits.median())
                med = med_val if med_val > 0 else 0.0
                features["single_credit_max_multiple_of_median"] = round((mx / med), 2) if med else 0.0
    except Exception:
        pass

    features["pos_share_pct"] = round((debit_b["pos"] / total_debit) * 100.0, 2) if total_debit else 0.0
    features["atm_withdrawal_share_pct"] = round((debit_b["atm"] / total_debit) * 100.0, 2) if total_debit else 0.0
    features["echannel_transfer_out_share_pct"] = round((debit_b["ech_out"] / total_debit) * 100.0, 2) if total_debit else 0.0
    features["bill_payment_share_pct"] = round((debit_b["billpay"] / total_debit) * 100.0, 2) if total_debit else 0.0

    features["salary_like_credit_flag"] = False
    try:
        if "DESCRIPTION_RAW" in raw_df.columns and "CREDIT" in raw_df.columns:
            s = raw_df.loc[raw_df["CREDIT"] > 0, "DESCRIPTION_RAW"].astype(str).str.lower()
            if (s.str.contains("salary|payroll|wages", regex=True, na=False)).any():
                features["salary_like_credit_flag"] = True
    except Exception:
        pass

    declared_income = client_data.get("salary") or client_data.get("declared_monthly_income")
    try:
        declared_income_val = float(declared_income) if declared_income not in (None, "", "N/A") else 0.0
    except Exception:
        declared_income_val = 0.0

    monthly_turnover = ((total_credit + total_debit) / months_equiv) if months_equiv else (total_credit + total_debit)
    features["turnover_multiple_of_declared_income"] = (
        round((monthly_turnover / declared_income_val), 2) if declared_income_val > 0 else 0.0
    )

    features["cash_structuring_flag"] = safe_boolish(detectors.get("structured_deposits"))

    pt = detectors.get("pass_through")
    rapid_ratio = 0.0
    if isinstance(pt, dict):
        for k in ("rapid_in_out_ratio", "ratio", "pass_through_ratio"):
            if k in pt:
                try:
                    rapid_ratio = float(pt.get(k) or 0.0)
                except Exception:
                    rapid_ratio = 0.0
                break
        else:
            rapid_ratio = 1.0 if safe_boolish(pt) else 0.0
    else:
        rapid_ratio = 1.0 if safe_boolish(pt) else 0.0
    features["rapid_in_out_ratio"] = round(float(rapid_ratio), 4)

    features["merchant_supplier_pattern_flag"] = safe_boolish(detectors.get("layering"))
    features["payroll_distribution_pattern_flag"] = safe_boolish(detectors.get("structured_payments"))

    rec = detectors.get("recurrence") or {}
    features["recurring_expenses_detected_flag"] = safe_boolish(rec.get("identity_clusters")) or safe_boolish(rec.get("narrative_clusters"))

    features["high_risk_counterparty_flag"] = safe_boolish(detectors.get("third_party"))

    if "salary_pattern" in detectors:
        features["salary_like_credit_flag"] = safe_boolish(detectors.get("salary_pattern")) or features["salary_like_credit_flag"]

    return features


def _apply_profile_rules(profile: Dict[str, Any], features: Dict[str, Any]) -> Dict[str, Any]:
    out = {"points": 0.0, "drivers": [], "actions": [], "features": features}
    if not profile:
        return out

    points = 0.0
    drivers: List[Dict[str, Any]] = []
    for rule in profile.get("mismatch_rules", []) or []:
        cond = (rule or {}).get("when") or {}
        feat = cond.get("feature")
        op = cond.get("op")
        val = cond.get("value")
        if not feat or not op:
            continue

        lhs = features.get(feat)
        if lhs is None:
            continue

        if _op_eval(lhs, str(op), val):
            pts = float(rule.get("risk_points") or 0.0)
            points += pts
            drivers.append({
                "title": "KYC profile mismatch",
                "detail": f"{profile.get('label', profile.get('profile_id'))}: {rule.get('rationale', rule.get('id'))}",
                "points": round(pts, 2),
            })

    actions: List[str] = []
    if points > 0:
        actions = [
            "Compare observed inflow/outflow channels and regularity against declared source of funds and expected customer behaviour.",
            "Request/verify supporting documents for material inflows/outflows that fall outside expected profile behaviour.",
            "Document rationale for deviations and apply enhanced monitoring where warranted.",
        ]

    out["points"] = round(points, 2)
    out["drivers"] = drivers
    out["actions"] = actions
    return out



def _risk_label_for_reason(reason: Any) -> str:
    r = _normalize_text(str(reason or ""))
    if r in ("structured_deposits", "structured_payments"):
        return "Structuring"
    if r == "pass_through":
        return "Pass-through"
    if r == "layering":
        return "Layering"
    if r == "round_figures":
        return "Round-figure amounts"
    if r == "salary_pattern":
        return "Salary-like pattern"
    if r == "cash_intensive":
        return "Cash-intensive activity"
    if r == "third_party":
        return "Third-party activity"
    if r == "recurrence":
        return "Recurrence"
    return str(reason or "").replace("_", " ").title()


def _top_parties_from_stats(stats: Dict[str, Any], limit: int = 5) -> List[str]:
    out: List[str] = []
    if not isinstance(stats, dict):
        return out

    for item in (stats.get("top_parties") or []):
        s = str(item or "").strip()
        if s and s not in out:
            out.append(s)
        if len(out) >= max(1, int(limit)):
            return out

    for block_key in ("identifiers", "top_identifiers", "parties", "top_parties_detailed"):
        rows = stats.get(block_key) or []
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            s = str(
                row.get("identifier")
                or row.get("label")
                or row.get("party")
                or row.get("name")
                or ""
            ).strip()
            if s and s not in out:
                out.append(s)
            if len(out) >= max(1, int(limit)):
                return out
    return out


def _build_summary_patterns(material_channels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for ch in material_channels or []:
        channel_desc = str(ch.get("DESCRIPTION") or "").strip() or str(ch.get("TRANSCODE") or "").strip() or "UNKNOWN"
        by_reason = ch.get("detector_suspicious_by_reason") or {}
        if not isinstance(by_reason, dict):
            continue

        for reason, stats_block in by_reason.items():
            if not isinstance(stats_block, dict):
                continue

            risk_label = _risk_label_for_reason(reason)

            for direction in ("credit", "debit"):
                stats = stats_block.get(direction) or {}
                if not isinstance(stats, dict):
                    continue

                count = int(stats.get("count") or 0)
                total = float(stats.get("total") or 0.0)
                if count <= 0 and total <= 0:
                    continue

                key = (risk_label, direction)
                bucket = buckets.setdefault(key, {
                    "type": risk_label,
                    "direction": direction,
                    "count": 0,
                    "total": 0.0,
                    "date_min": None,
                    "date_max": None,
                    "channels": [],
                    "parties": [],
                })

                bucket["count"] += count
                bucket["total"] += total

                dmin = _safe_dt_str(stats.get("date_min"))
                dmax = _safe_dt_str(stats.get("date_max"))

                if dmin:
                    if not bucket["date_min"] or dmin < bucket["date_min"]:
                        bucket["date_min"] = dmin
                if dmax:
                    if not bucket["date_max"] or dmax > bucket["date_max"]:
                        bucket["date_max"] = dmax

                if channel_desc and channel_desc not in bucket["channels"]:
                    bucket["channels"].append(channel_desc)

                for p in _top_parties_from_stats(stats, limit=10):
                    if p and p not in bucket["parties"]:
                        bucket["parties"].append(p)

    ranked = sorted(
        buckets.values(),
        key=lambda x: (-float(x.get("total") or 0.0), -int(x.get("count") or 0), str(x.get("type") or "")),
    )

    out: List[Dict[str, Any]] = []
    for item in ranked:
        out.append({
            "type": item["type"],
            "direction": item["direction"],
            "count": int(item["count"]),
            "total": round(float(item["total"]), 2),
            "date_min": item["date_min"],
            "date_max": item["date_max"],
            "channels": item["channels"][:5],
            "channel": ", ".join(item["channels"][:3]),
            "parties": item["parties"][:5],
        })
    return out


def _build_report_summary(
    material_channels: List[Dict[str, Any]],
    channel_profile: Dict[str, Any],
    summary_patterns: List[Dict[str, Any]],
    total_credit: float,
    total_debit: float,
) -> Dict[str, Any]:
    def _rank_credit_rows() -> List[Dict[str, Any]]:
        rows = [dict(x) for x in (material_channels or []) if float(x.get("deposit") or 0.0) > 0]
        return sorted(rows, key=lambda r: -float(r.get("deposit") or 0.0))

    def _rank_debit_rows() -> List[Dict[str, Any]]:
        rows = [dict(x) for x in (material_channels or []) if float(x.get("withdrawal") or 0.0) > 0]
        return sorted(rows, key=lambda r: -float(r.get("withdrawal") or 0.0))

    top_sources = []
    for row in _rank_credit_rows()[:5]:
        top_sources.append({
            "TRANSCODE": str(row.get("TRANSCODE") or ""),
            "DESCRIPTION": str(row.get("DESCRIPTION") or ""),
            "amount": round(float(row.get("deposit") or 0.0), 2),
            "pct": round(float(row.get("CR%") or row.get("credit_pct") or 0.0), 2),
        })

    top_uses = []
    for row in _rank_debit_rows()[:5]:
        top_uses.append({
            "TRANSCODE": str(row.get("TRANSCODE") or ""),
            "DESCRIPTION": str(row.get("DESCRIPTION") or ""),
            "amount": round(float(row.get("withdrawal") or 0.0), 2),
            "pct": round(float(row.get("DR%") or row.get("debit_pct") or 0.0), 2),
        })

    credit_profile = ((channel_profile or {}).get("credit") or {}).get("channels") or []
    debit_profile = ((channel_profile or {}).get("debit") or {}).get("channels") or []

    recognized_sof = []
    for ch in credit_profile:
        if ch.get("declared_sof_match"):
            recognized_sof.append({
                "TRANSCODE": str(ch.get("TRANSCODE") or ""),
                "DESCRIPTION": str(ch.get("DESCRIPTION") or ""),
                "sof": str(ch.get("sof") or ""),
                "pct": round(float(ch.get("CR%") or ch.get("credit_pct") or 0.0), 2),
                "risk": str(ch.get("risk") or ""),
            })

    recognized_uof = []
    for ch in debit_profile[:5]:
        recognized_uof.append({
            "TRANSCODE": str(ch.get("TRANSCODE") or ""),
            "DESCRIPTION": str(ch.get("DESCRIPTION") or ""),
            "pof": str(ch.get("pof") or ""),
            "pct": round(float(ch.get("DR%") or ch.get("debit_pct") or 0.0), 2),
            "risk": str(ch.get("risk") or ""),
        })

    return {
        "account_overview": {
            "total_credits": round(float(total_credit or 0.0), 2),
            "total_debits": round(float(total_debit or 0.0), 2),
            "top_credit_channels": top_sources,
            "top_debit_channels": top_uses,
        },
        "source_of_funds": {
            "recognized": recognized_sof,
            "top_channels": top_sources,
        },
        "use_of_funds": {
            "recognized": recognized_uof,
            "top_channels": top_uses,
        },
        "suspicious_activity": summary_patterns[:10],
    }

def analyze_statement(
    df: pd.DataFrame,
    code_lookup,
    client_type: str,
    kyc_profile_id: Optional[str] = None,
    client_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    client_data = client_data or {}

    df, date_col = _prepare_analysis_df(df, code_lookup, client_type)
    analysis_cache = _build_analysis_cache(df, date_col)

    grouped = (
        df.groupby("TRANSCODE", dropna=False)
          .agg(
              deposit=("CREDIT", "sum"),
              withdrawal=("DEBIT", "sum"),
              count=("TRANSCODE", "size"),
              DESCRIPTION=("DESCRIPTION", "first"),
          )
          .reset_index()
    )

    total_deposit = float(grouped["deposit"].sum()) if len(grouped) else 0.0
    total_withdrawal = float(grouped["withdrawal"].sum()) if len(grouped) else 0.0
    total_count = int(grouped["count"].sum()) if len(grouped) else 0

    grouped["DESCRIPTION"] = grouped["DESCRIPTION"].astype(str).replace(["nan", "NaN", "None", ""], "UNKNOWN")

    grouped["CR%"] = (grouped["deposit"] / total_deposit * 100.0).round(2) if total_deposit else 0.0
    grouped["DR%"] = (grouped["withdrawal"] / total_withdrawal * 100.0).round(2) if total_withdrawal else 0.0

    grouped["deposit"] = grouped["deposit"].round(2)
    grouped["withdrawal"] = grouped["withdrawal"].round(2)
    grouped["count"] = grouped["count"].astype(int)

    grouped["TRANSCODE_NUM"] = pd.to_numeric(grouped["TRANSCODE"], errors="coerce")
    grouped = (
        grouped
        .sort_values(by=["TRANSCODE_NUM", "TRANSCODE"], ascending=[True, True])
        .drop(columns=["TRANSCODE_NUM"])
    )

    total_row = {
        "TRANSCODE": "TOTAL",
        "DESCRIPTION": "TOTAL",
        "deposit": round(total_deposit, 2),
        "withdrawal": round(total_withdrawal, 2),
        "count": total_count,
        "CR%": 100.0 if total_deposit else 0.0,
        "DR%": 100.0 if total_withdrawal else 0.0,
    }

    grouped = pd.concat([grouped, pd.DataFrame([total_row])], ignore_index=True)

    pivot_summary = grouped.to_dict(orient="records")

    pivot_summary_display: List[Dict[str, Any]] = []
    for r in pivot_summary:
        rr = dict(r)
        if str(rr.get("TRANSCODE", "")).upper() != "TOTAL":
            rr["deposit"] = _blank_zero_money(rr.get("deposit"))
            rr["withdrawal"] = _blank_zero_money(rr.get("withdrawal"))
            rr["CR%"] = _blank_zero_pct(rr.get("CR%"))
            rr["DR%"] = _blank_zero_pct(rr.get("DR%"))
        else:
            rr["deposit"] = f"{float(rr.get('deposit') or 0.0):.2f}"
            rr["withdrawal"] = f"{float(rr.get('withdrawal') or 0.0):.2f}"
            rr["CR%"] = f"{float(rr.get('CR%') or 0.0):.2f}%"
            rr["DR%"] = f"{float(rr.get('DR%') or 0.0):.2f}%"
        pivot_summary_display.append(rr)

    channel_analysis = build_channel_analysis(
        df=df,
        pivot_summary=pivot_summary,
        narrative_library={},
        suspicious_cap=200,
    )
    material_channels = channel_analysis.get("channels", [])

    individual_profile = (
        client_data.get("individualProfile")
        or client_data.get("profile")
        or client_data.get("individual_profile_type")
        or None
    )
    declared_sof = (
        client_data.get("source_of_funds")
        or client_data.get("sourceOfFunds")
        or client_data.get("declared_source_of_funds")
        or None
    )
    owner_context = _build_owner_context(client_data)
    declared_sof_category = _normalize_declared_sof_category(declared_sof)
    channel_profile = classify_material_channels(
        material_channels=material_channels,
        client_type=client_type,
        profile=individual_profile,
        declared_sof=declared_sof,
    )
    credit_channel_map = _channel_map_from_profile(channel_profile, "credit")
    debit_channel_map = _channel_map_from_profile(channel_profile, "debit")

    detectors = _run_detector_pipeline(
        df,
        date_col=date_col,
        analysis_cache=analysis_cache,
    )

    structured_dep = detectors.get("structured_deposits")
    structured_pay = detectors.get("structured_payments")
    pass_through = detectors.get("pass_through")
    layering = detectors.get("layering")
    round_figures = detectors.get("round_figures")
    salary_pattern = detectors.get("salary_pattern")
    cash_intensive = detectors.get("cash_intensive")
    third_party = detectors.get("third_party")
    recurrence = detectors.get("recurrence")

    if "ROW_ID" not in df.columns:
        df["ROW_ID"] = df.index

    row_reasons: Dict[int, List[str]] = {}

    try:
        row_lookup = df.set_index("ROW_ID", drop=False)
    except Exception:
        row_lookup = None

    def _get_row_by_id(rid: int):
        if row_lookup is None:
            return None
        try:
            row = row_lookup.loc[int(rid)]
            if hasattr(row, "columns"):
                row = row.iloc[0]
            return row
        except Exception:
            return None

    for det_key, det_out in detectors.items():
        flagged_ids: List[int] = []

        if det_key == "recurrence":
            if isinstance(det_out, dict):
                if isinstance(det_out.get("flagged_row_ids"), list):
                    for x in det_out.get("flagged_row_ids") or []:
                        try:
                            flagged_ids.append(int(x))
                        except Exception:
                            continue
                else:
                    for block_key in ("identity_clusters", "same_day_identity_clusters", "narrative_clusters"):
                        for item in (det_out.get(block_key) or []):
                            if not isinstance(item, dict):
                                continue
                            for x in item.get("flagged_row_ids") or []:
                                try:
                                    flagged_ids.append(int(x))
                                except Exception:
                                    continue
            flagged_ids = sorted(set(flagged_ids))
        else:
            flagged_ids = _extract_flagged_row_ids(det_out)

        for rid in flagged_ids:
            row = _get_row_by_id(rid)
            if row is None:
                continue

            if not should_include_row(row, min_amount=MATERIALITY_MIN_AMOUNT):
                continue

            if det_key == "structured_deposits":
                try:
                    if float(row.get("CREDIT") or 0.0) <= 0:
                        continue
                except Exception:
                    continue

            if det_key == "structured_payments":
                try:
                    if float(row.get("DEBIT") or 0.0) <= 0:
                        continue
                except Exception:
                    continue

            if _should_suppress_detector_reason(
                det_key=det_key,
                row=row,
                client_type=client_type,
                individual_profile=individual_profile,
                declared_sof_category=declared_sof_category,
                credit_channel_map=credit_channel_map,
                debit_channel_map=debit_channel_map,
                detectors=detectors,
                owner_context=owner_context,
            ):
                continue

            row_reasons.setdefault(int(rid), [])
            if det_key not in row_reasons[int(rid)]:
                row_reasons[int(rid)].append(det_key)

    suspicious_cap = 200
    _date_col_for_list = _pick_date_col(df) or "DATE"

    for ch in material_channels:
        code = str(ch.get("TRANSCODE") or "")
        if not code:
            continue

        try:
            code_mask = df["TRANSCODE"].astype(str) == code
            code_row_ids = set(df.loc[code_mask, "ROW_ID"].dropna().astype(int).tolist())
        except Exception:
            code_row_ids = set()

        hits = sorted(code_row_ids.intersection(row_reasons.keys()))
        ch["detector_suspicious_transactions_count"] = int(len(hits))

        try:
            sub = df[df["ROW_ID"].isin(hits)].copy() if hits else pd.DataFrame()
        except Exception:
            sub = pd.DataFrame()

        if not sub.empty:
            try:
                sub["__AMT"] = sub[["DEBIT", "CREDIT"]].abs().max(axis=1)
            except Exception:
                sub["__AMT"] = 0.0
            total_amt = float(sub["__AMT"].sum())
        else:
            total_amt = 0.0

        ch["detector_suspicious_total_amount"] = round(total_amt, 2)

        reason_counts: Dict[str, int] = {}
        for rid in hits:
            for det in row_reasons.get(int(rid), []):
                reason_counts[str(det)] = int(reason_counts.get(str(det), 0) + 1)
        ch["detector_suspicious_reasons_count"] = reason_counts

        _date_col_for_group = _pick_date_col(df) or "DATE"
        if _date_col_for_group not in df.columns:
            _date_col_for_group = "DATE"

        credit_summary = _group_suspicious_by_identifier(
            sub=sub,
            date_col=_date_col_for_group,
            direction="credit",
            row_reasons=row_reasons,
            top_n=100000,
        )
        debit_summary = _group_suspicious_by_identifier(
            sub=sub,
            date_col=_date_col_for_group,
            direction="debit",
            row_reasons=row_reasons,
            top_n=100000,
        )

        ch["detector_suspicious_directional_summary"] = {
            "credit": credit_summary,
            "debit": debit_summary,
        }

        txs: List[Dict[str, Any]] = []
        for rid in hits[: max(0, int(suspicious_cap))]:
            try:
                row = df.loc[df["ROW_ID"].astype(int) == int(rid)].iloc[0]
            except Exception:
                continue

            txs.append({
                "row_id": int(rid),
                "date": _safe_dt_str(row.get(_date_col_for_list)) or _safe_dt_str(row.get("DATE")),
                "description_raw": str(row.get("DESCRIPTION_RAW") or ""),
                "identity": str(row.get("IDENTITY") or ""),
                "debit": float(row.get("DEBIT") or 0.0) if "DEBIT" in df.columns else 0.0,
                "credit": float(row.get("CREDIT") or 0.0) if "CREDIT" in df.columns else 0.0,
                "reasons": row_reasons.get(int(rid), []),
            })

        ch["detector_suspicious_transactions"] = txs

        by_reason: Dict[str, Any] = {}
        if not sub.empty and hits:
            def _party_key(r: pd.Series) -> str:
                ident = _norm_ident(r.get("IDENTITY"))
                if ident:
                    return ident
                desc_ident = _extract_bracket_ident(r.get("DESCRIPTION_RAW"))
                if desc_ident:
                    return desc_ident
                desc = str(r.get("DESCRIPTION_RAW") or "").strip()
                return desc if desc else "UNKNOWN"

            _dc = _pick_date_col(sub) or "DATE"
            if _dc not in sub.columns:
                _dc = "DATE"

            reason_to_rowids: Dict[str, List[int]] = {}
            for rid in hits:
                for det in row_reasons.get(int(rid), []):
                    reason_to_rowids.setdefault(str(det), []).append(int(rid))

            def _agg(rsx: pd.DataFrame, direction: str, detector_key: str) -> Dict[str, Any]:
                if rsx is None or rsx.empty:
                    return {
                        "count": 0,
                        "total": 0.0,
                        "date_min": None,
                        "date_max": None,
                        "unique_parties": 0,
                        "top_parties": [],
                        "identifiers": [],
                        "reason_counts": {},
                    }

                amt_col = "CREDIT" if direction == "credit" else "DEBIT"

                try:
                    dates = pd.to_datetime(rsx[_dc], errors="coerce")
                    dmin = dates.min()
                    dmax = dates.max()
                    date_min = dmin.strftime("%Y-%m-%d") if pd.notna(dmin) else None
                    date_max = dmax.strftime("%Y-%m-%d") if pd.notna(dmax) else None
                except Exception:
                    date_min, date_max = None, None

                try:
                    amt = float(pd.to_numeric(rsx[amt_col], errors="coerce").fillna(0.0).sum())
                except Exception:
                    amt = 0.0

                try:
                    parties = rsx.apply(_party_key, axis=1).map(_norm_ident)
                    parties = parties[parties.astype(str).str.strip() != ""]
                    vc = parties.value_counts()
                    top_parties = [str(x) for x in vc.index.tolist()]
                    unique_parties = int(parties.nunique())
                except Exception:
                    top_parties = []
                    unique_parties = 0

                ident_summary = _group_suspicious_by_identifier(
                    sub=rsx,
                    date_col=_dc,
                    direction=direction,
                    row_reasons=row_reasons,
                    top_n=100000,
                )

                out = {
                    "count": int(len(rsx)),
                    "total": round(amt, 2),
                    "date_min": date_min,
                    "date_max": date_max,
                    "unique_parties": unique_parties,
                    "top_parties": top_parties,
                    "identifiers": ident_summary.get("identifiers", []),
                    "reason_counts": ident_summary.get("reason_counts", {}),
                }

                try:
                    if detector_key in ("structured_deposits", "structured_payments"):
                        out["sub_threshold_count"] = int(len(rsx))
                        out["distinct_amounts"] = int(
                            pd.to_numeric(rsx[amt_col], errors="coerce").fillna(0.0).round(2).nunique()
                        )

                    if detector_key == "pass_through":
                        out["window_days"] = 5

                    if detector_key == "layering":
                        out["channel_count"] = int(rsx["TRANSCODE"].astype(str).nunique()) if "TRANSCODE" in rsx.columns else 0

                    if detector_key == "recurrence":
                        out["distinct_amounts"] = int(
                            pd.to_numeric(rsx[amt_col], errors="coerce").fillna(0.0).round(2).nunique()
                        )
                except Exception:
                    pass

                return out

            for det, rids in reason_to_rowids.items():
                try:
                    rs = sub[sub["ROW_ID"].isin(rids)].copy()
                except Exception:
                    rs = pd.DataFrame()
                if rs.empty:
                    continue

                try:
                    rs_credit = rs[pd.to_numeric(rs.get("CREDIT", 0), errors="coerce").fillna(0.0) > 0].copy()
                except Exception:
                    rs_credit = pd.DataFrame()

                try:
                    rs_debit = rs[pd.to_numeric(rs.get("DEBIT", 0), errors="coerce").fillna(0.0) > 0].copy()
                except Exception:
                    rs_debit = pd.DataFrame()

                if det == "structured_deposits":
                    by_reason[det] = {
                        "credit": _agg(rs_credit, "credit", det),
                        "debit": _agg(pd.DataFrame(), "debit", det),
                    }
                elif det == "structured_payments":
                    by_reason[det] = {
                        "credit": _agg(pd.DataFrame(), "credit", det),
                        "debit": _agg(rs_debit, "debit", det),
                    }
                else:
                    by_reason[det] = {
                        "credit": _agg(rs_credit, "credit", det),
                        "debit": _agg(rs_debit, "debit", det),
                    }

        ch["detector_suspicious_by_reason"] = by_reason

    if not kyc_profile_id:
        if str(client_type).strip().lower() == "individual":
            kyc_profile_id = "INDIVIDUAL_EMPLOYED_GENERIC"
        else:
            kyc_profile_id = "NONIND_GENERIC_COMPANY"

    profile = get_profile(kyc_profile_id)

    kyc_features = _extract_kyc_features(
        pivot_summary=pivot_summary,
        raw_df=df,
        detectors=detectors,
        client_data=client_data,
    )

    kyc_result: Dict[str, Any] = {
        "profile_id": kyc_profile_id,
        "profile_loaded": bool(profile),
        "points": 0.0,
        "drivers": [],
        "actions": [],
        "features": kyc_features,
        "scored_detector_keys": ["salary_pattern", "structured_deposits", "pass_through", "third_party", "recurrence"],
    }

    if profile:
        kyc_scored = _apply_profile_rules(profile, kyc_features)
        kyc_result["points"] = kyc_scored["points"]
        kyc_result["drivers"] = kyc_scored["drivers"]
        kyc_result["actions"] = kyc_scored["actions"]
    else:
        kyc_result["drivers"] = [{
            "title": "KYC profile missing",
            "detail": f"KYC profile id '{kyc_profile_id}' not found in library.",
            "points": 0.0,
        }]

    suspicious_total_rows = 0
    try:
        suspicious_total_rows = int(sum(int(ch.get("detector_suspicious_transactions_count") or 0) for ch in material_channels))
    except Exception:
        suspicious_total_rows = 0

    summary_patterns = _build_summary_patterns(material_channels)
    report_summary = _build_report_summary(
        material_channels=material_channels,
        channel_profile=channel_profile,
        summary_patterns=summary_patterns,
        total_credit=total_deposit,
        total_debit=total_withdrawal,
    )

    result = {
        "pivot_summary": pivot_summary,
        "pivot_summary_display": pivot_summary_display,

        "material_channels": material_channels,
        "channel_analysis": channel_analysis,

        "channel_profile": channel_profile,
        "summary_patterns": summary_patterns,
        "summary": report_summary,
        "totals": {
            "credits": round(total_deposit, 2),
            "debits": round(total_withdrawal, 2),
        },
        "client": client_data,

        "raw_df": df,

        "identity_clusters": (recurrence or {}).get("identity_clusters", []),
        "narrative_clusters": (recurrence or {}).get("narrative_clusters", []),
        "same_day_identity_clusters": (recurrence or {}).get("same_day_identity_clusters", []),
        "identity_summary": (recurrence or {}).get("identity_summary", []),
        "recurrence_error": (recurrence or {}).get("error"),

        "kyc_profile": kyc_result,

        "structured_deposits": structured_dep,
        "structured_payments": structured_pay,
        "pass_through": pass_through,
        "layering": layering,
        "round_figures": round_figures,
        "salary_pattern": salary_pattern,
        "cash_intensive": cash_intensive,
        "third_party": third_party,

        "detectors": detectors,

        "suspicious_total_rows": suspicious_total_rows,
    }

    try:
        del analysis_cache
    except Exception:
        pass

    return result
