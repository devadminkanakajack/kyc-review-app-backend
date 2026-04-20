import re
import pandas as pd
from difflib import SequenceMatcher
from typing import List, Dict, Any

from utils.file_parser import build_identity_alias_map as _parser_build_identity_alias_map


# ============================================================
# 1. SHARED IDENTITY CLEANING / NORMALISATION
# ============================================================

_STOPWORDS = {
    "SGS", "LOAN", "EXTENSION", "SALARY", "PAYMENT", "PAYMENTS", "PAY",
    "ACC", "ACCOUNT", "LOANS", "DEPOSIT", "TRANSFER", "TT",
    "OUTWARD", "INWARD", "CASH", "ATM", "POS", "TOTAL",
    "VISION", "CITY", "BRANCH", "BANK", "SERVICE", "SERVICES",
    "FEE", "FEES", "KINA", "BA",
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


def _clean_identity_value(v: Any) -> str | None:
    if v is None:
        return None

    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    s = str(v).strip().upper()
    s = s.replace(".", " ")
    s = s.replace("_", " ")
    s = re.sub(r"[\[\]\(\)\{\}]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    if s in {"", "NAN", "NONE", "NULL", "UNKNOWN"}:
        return None

    # preserve masked account
    if re.fullmatch(r"\*{2,}\d{3,}", s):
        return s

    # preserve pure numeric identifiers in their original bracket-friendly form
    if re.fullmatch(r"\d+(?:\.\d+)?", s):
        return f"[{s}]"

    # preserve already-normalized stable ids
    if re.fullmatch(r"ID:\d+(?:\.\d+)?", s):
        return s
    if re.fullmatch(r"\[\d+(?:\.\d+)?\]", s):
        return s

    # remove long numeric refs appended to names
    s = re.sub(r"\b\d{4,}\b", " ", s)

    # common OCR / variation cleanup
    s = s.replace("KESHIA", "KESIA")
    s = s.replace("KESVA", "KESIA")
    s = s.replace("NANKING", "NANKI")
    s = s.replace("NENKI", "NANKI")
    s = s.replace("NENKIA", "NANKI")
    s = s.replace("NAKI", "NANKI")
    s = s.replace("MANKI", "NANKI")
    s = s.replace("NANKL", "NANKI")
    s = s.replace("WAIPAK", "WALPAK")
    s = s.replace("WAL PAK", "WALPAK")
    s = s.replace("KESINANKIWALPAK", "KESIA NANKI WALPAK")
    s = s.replace("KESIANANKIWALPAK", "KESIA NANKI WALPAK")

    for pat in _SUFFIX_NOISE_PATTERNS:
        s2 = re.sub(pat, "", s).strip()
        if s2:
            s = s2

    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    if s in {"", "NAN", "NONE", "NULL", "UNKNOWN"}:
        return None

    return s or None


def _compact(s: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(s or "").upper())


def _seq_ratio(a: str, b: str) -> float:
    a2 = _compact(a)
    b2 = _compact(b)
    if not a2 or not b2:
        return 0.0
    return SequenceMatcher(None, a2, b2).ratio()


def _tokenize_identity(s: Any) -> List[str]:
    cleaned = _clean_identity_value(s)
    if not cleaned:
        return []
    if cleaned.startswith("ID:") or re.fullmatch(r"\[\d+(?:\.\d+)?\]", cleaned):
        return [cleaned]
    return [t for t in cleaned.split() if t and t not in _STOPWORDS]


def _canonical_identity(s: Any) -> str | None:
    cleaned = _clean_identity_value(s)
    if not cleaned:
        return None

    if cleaned.startswith("ID:") or re.fullmatch(r"\[\d+(?:\.\d+)?\]", cleaned):
        return cleaned
    if re.fullmatch(r"\*{2,}\d{3,}", cleaned):
        return cleaned

    toks = _tokenize_identity(cleaned)

    # strong canonical collapse for known Kesia-family variants
    if "KESIA" in toks and ("NANKI" in toks or "WALPAK" in toks):
        if "NANKI" in toks and "WALPAK" in toks:
            return "KESIA NANKI WALPAK"
        if "NANKI" in toks:
            return "KESIA NANKI"
        if "WALPAK" in toks:
            return "KESIA WALPAK"

    return cleaned


# ============================================================
# 2. IDENTITY EXTRACTION (IMPROVED & HARDENED)
# ============================================================

def extract_identity(description_raw: str) -> str | None:
    """
    Extract a stable counterparty / beneficiary identity.

    Priority:
      1. Masked accounts ****#### (preferred stable id)
      2. Bracket identifiers [NAME] or [35602811]
      3. FROM <NAME>
      4. FOR <NAME>
      5. TO <NAME>
      6. Tail name tokens (dynamic)
      7. Fallback: cleaned description (if meaningful)
    """
    if not isinstance(description_raw, str):
        return None

    text = description_raw.strip()
    if not text:
        return None

    # 1) Masked account
    masked = re.search(r"\*{4}\d{4}", text)
    if masked:
        return _clean_identity_value(masked.group(0))

    upper = text.upper()

    def clean_block(s: str):
        s = " ".join(s.split())
        s = re.sub(r"[^A-Z0-9\s]", " ", s)
        s = " ".join(s.split())
        return _canonical_identity(s)

    # 2) bracket token
    m = re.search(r"\[([^\]]+)\]", text)
    if m:
        cleaned = _canonical_identity(m.group(1))
        if cleaned:
            return cleaned

    # 3) FROM <NAME>
    m = re.search(r"\bFROM\s+([A-Z][A-Z0-9\s\.]+)", upper)
    if m:
        cleaned = clean_block(m.group(1))
        if cleaned:
            return cleaned

    # 4) FOR <NAME>
    m = re.search(r"\bFOR\s+([A-Z][A-Z0-9\s\.]+)", upper)
    if m:
        cleaned = clean_block(m.group(1))
        if cleaned:
            return cleaned

    # 5) TO <NAME>
    m = re.search(r"\bTO\s+([A-Z][A-Z0-9\s\.]+)", upper)
    if m:
        cleaned = clean_block(m.group(1))
        if cleaned:
            return cleaned

    # 6) Dynamic tail names
    tokens = re.findall(r"[A-Z0-9]{3,}", upper)
    name_like = [t for t in tokens if t not in _STOPWORDS]

    if name_like:
        selected = name_like[-3:] if len(name_like) >= 3 else name_like
        cleaned = clean_block(" ".join(selected))
        if cleaned:
            return cleaned

    # 7) Fallback
    cleaned = re.sub(r"[\[\]()]", " ", upper)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return _canonical_identity(cleaned)


# ============================================================
# 3. GENERIC NARRATIVE-PATTERN KEY
# ============================================================

def build_narrative_key(description_raw: str, identity: str | None) -> str | None:
    """
    Build a generic pattern key for repeated behaviour in narrative text.
    Removes identity tokens, numbers, masked accounts, stopwords.
    Output is a compact signature used for grouping.
    """
    if not isinstance(description_raw, str) or not description_raw.strip():
        return None

    text = str(description_raw).upper()

    identity_text = _clean_identity_value(identity) if identity is not None else None
    if identity_text:
        # do not inject numeric ids into narrative key stripping
        if not (
            identity_text.startswith("ID:")
            or re.fullmatch(r"\[\d+(?:\.\d+)?\]", identity_text)
        ):
            for t in identity_text.upper().split():
                text = re.sub(rf"\b{re.escape(t)}\b", " ", text)

    # Preserve stable identifiers for recurrence grouping instead of stripping them.
    text = re.sub(r"(\*{2,}\d{3,})", lambda m: f" MASKED_{m.group(1).replace('*', 'X')} ", text)
    text = re.sub(r"\[(\d{2,})\]", lambda m: f" BRACKET_ID_{m.group(1)} ", text)
    text = re.sub(r"\b(\d{2,})\b", lambda m: f" NUM_{m.group(1)} ", text)
    text = re.sub(r"[\[\]()]", " ", text)
    text = re.sub(r"[^A-Z0-9_ ]+", " ", text)

    tokens = text.split()

    stop = {
        "ACC", "ACCT", "ACCOUNT", "A/C", "NO", "REF", "REFERENCE",
        "LOAN", "SALARY", "PAYMENT", "PAYMENTS", "PAY", "DEPOSIT", "TRANSFER",
        "INWARD", "OUTWARD", "BANK", "BRANCH", "CASH", "POS",
        "ATM", "IB", "OWN", "OTHER", "TOTAL", "CITY", "VISION",
        "SERVICE", "SERVICES", "FEE", "FEES", "KINA", "BA",
    }

    keep = [t for t in tokens if t not in stop]
    if not keep:
        keep = tokens

    key = " ".join(keep[:4]).strip()
    return key or None


# ============================================================
# 4. BUILD IDENTITY-BASED RECURRENCE
# ============================================================

def build_identity_clusters(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Detect repeated credits/debits to the SAME identity (across all dates).
    Grouping:
      - (TRANSCODE, IDENTITY) separately for credit and debit.
    Keeps groups with count >= 2 and returns totals per cluster.
    """
    clusters: List[Dict[str, Any]] = []

    if "IDENTITY" not in df.columns:
        return clusters

    subset = df.copy()
    subset["IDENTITY"] = subset["IDENTITY"].apply(_canonical_identity)
    subset = subset[subset["IDENTITY"].notna()].copy()
    if subset.empty:
        return clusters

    df_credit = subset[subset["CREDIT"] > 0]
    for (code, identity), grp in df_credit.groupby(["TRANSCODE", "IDENTITY"]):
        if len(grp) >= 2:
            clusters.append({
                "type": "identity",
                "direction": "credit",
                "TRANSCODE": code,
                "label": identity,
                "count": int(len(grp)),
                "total_amount": float(grp["CREDIT"].sum()),
                "flagged_row_ids": [int(x) for x in grp.get("ROW_ID", grp.index).tolist()],
            })

    df_debit = subset[subset["DEBIT"] > 0]
    for (code, identity), grp in df_debit.groupby(["TRANSCODE", "IDENTITY"]):
        if len(grp) >= 2:
            clusters.append({
                "type": "identity",
                "direction": "debit",
                "TRANSCODE": code,
                "label": identity,
                "count": int(len(grp)),
                "total_amount": float(grp["DEBIT"].sum()),
                "flagged_row_ids": [int(x) for x in grp.get("ROW_ID", grp.index).tolist()],
            })

    return clusters


# ============================================================
# 5. BUILD SAME-DAY IDENTITY RECURRENCE
# ============================================================

def _normalize_date_only(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", dayfirst=True)
    return dt.dt.date


def build_same_day_identity_clusters(
    df: pd.DataFrame,
    date_col: str = "DATE",
) -> List[Dict[str, Any]]:
    """
    Detect recurrence where the same identity appears multiple times on the SAME DATE,
    per TRANSCODE, separately for credit and debit.
    """
    clusters: List[Dict[str, Any]] = []

    required = {"IDENTITY", "TRANSCODE", "CREDIT", "DEBIT", date_col}
    missing = required - set(df.columns)
    if missing:
        return clusters

    tmp = df.copy()
    tmp["IDENTITY"] = tmp["IDENTITY"].apply(_canonical_identity)
    tmp["DATE_ONLY"] = _normalize_date_only(tmp[date_col])

    tmp = tmp[tmp["IDENTITY"].notna() & tmp["DATE_ONLY"].notna()].copy()
    if tmp.empty:
        return clusters

    credit = tmp[tmp["CREDIT"] > 0]
    for (d, code, identity), grp in credit.groupby(["DATE_ONLY", "TRANSCODE", "IDENTITY"]):
        if len(grp) >= 2:
            clusters.append({
                "type": "identity_same_day",
                "direction": "credit",
                "date": str(d),
                "TRANSCODE": code,
                "label": identity,
                "count": int(len(grp)),
                "total_amount": float(grp["CREDIT"].sum()),
                "flagged_row_ids": [int(x) for x in grp.get("ROW_ID", grp.index).tolist()],
            })

    debit = tmp[tmp["DEBIT"] > 0]
    for (d, code, identity), grp in debit.groupby(["DATE_ONLY", "TRANSCODE", "IDENTITY"]):
        if len(grp) >= 2:
            clusters.append({
                "type": "identity_same_day",
                "direction": "debit",
                "date": str(d),
                "TRANSCODE": code,
                "label": identity,
                "count": int(len(grp)),
                "total_amount": float(grp["DEBIT"].sum()),
                "flagged_row_ids": [int(x) for x in grp.get("ROW_ID", grp.index).tolist()],
            })

    return clusters


# ============================================================
# 6. BUILD NARRATIVE-PATTERN RECURRENCE
# ============================================================

def build_narrative_clusters(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Detect repeated credits/debits sharing the SAME narrative key (across all dates).
    Grouping:
      - (TRANSCODE, NARRATIVE_KEY) separately for credit and debit.
    Keeps groups with count >= 2 and returns totals per cluster.
    """
    clusters: List[Dict[str, Any]] = []

    subset = df[df["NARRATIVE_KEY"].notna()].copy()
    if subset.empty:
        return clusters

    df_credit = subset[subset["CREDIT"] > 0]
    for (code, key), grp in df_credit.groupby(["TRANSCODE", "NARRATIVE_KEY"]):
        if len(grp) >= 2:
            clusters.append({
                "type": "pattern",
                "direction": "credit",
                "TRANSCODE": code,
                "pattern": key,
                "count": int(len(grp)),
                "total_amount": float(grp["CREDIT"].sum()),
                "flagged_row_ids": [int(x) for x in grp.get("ROW_ID", grp.index).tolist()],
            })

    df_debit = subset[subset["DEBIT"] > 0]
    for (code, key), grp in df_debit.groupby(["TRANSCODE", "NARRATIVE_KEY"]):
        if len(grp) >= 2:
            clusters.append({
                "type": "pattern",
                "direction": "debit",
                "TRANSCODE": code,
                "pattern": key,
                "count": int(len(grp)),
                "total_amount": float(grp["DEBIT"].sum()),
                "flagged_row_ids": [int(x) for x in grp.get("ROW_ID", grp.index).tolist()],
            })

    return clusters


# ============================================================
# 7. OVERALL IDENTITY SUMMARY
# ============================================================

def build_identity_summary(
    df: pd.DataFrame,
    date_col: str = "DATE",
) -> List[Dict[str, Any]]:
    """
    Provide an overall summary per identity across the entire dataset.
    """
    summary: List[Dict[str, Any]] = []

    if "IDENTITY" not in df.columns:
        return summary

    subset = df.copy()
    subset["IDENTITY"] = subset["IDENTITY"].apply(_canonical_identity)
    subset = subset[subset["IDENTITY"].notna()].copy()
    if subset.empty:
        return summary

    has_date = date_col in subset.columns
    if has_date:
        subset["DATE_ONLY"] = _normalize_date_only(subset[date_col])

    for identity, grp in subset.groupby("IDENTITY"):
        total_credit = float(grp["CREDIT"].sum()) if "CREDIT" in grp.columns else 0.0
        total_debit = float(grp["DEBIT"].sum()) if "DEBIT" in grp.columns else 0.0

        active_days = None
        if has_date and "DATE_ONLY" in grp.columns:
            active_days = int(pd.Series(grp["DATE_ONLY"]).dropna().nunique())

        summary.append({
            "identity": identity,
            "txn_count": int(len(grp)),
            "total_credit": total_credit,
            "total_debit": total_debit,
            "net_flow": float(total_credit - total_debit),
            "active_days": active_days,
            "row_ids_sample": [int(x) for x in grp.get("ROW_ID", grp.index).head(50).tolist()],
        })

    return summary


# ============================================================
# 8. MASTER ANALYSIS FUNCTION
# ============================================================

def analyze_recurrence(
    df: pd.DataFrame,
    date_col: str = "DATE",
) -> Dict[str, Any]:
    """
    Main analysis entrypoint (recurrence + identity summaries).

    Requires:
      - DESCRIPTION_RAW
      - TRANSCODE
      - CREDIT
      - DEBIT
    """
    df = df.copy()

    if "ROW_ID" not in df.columns:
        df["ROW_ID"] = df.index

    if "DESCRIPTION_RAW" not in df.columns:
        return {"identity_clusters": [], "same_day_identity_clusters": [], "narrative_clusters": [], "identity_summary": [], "error": "Missing required column: DESCRIPTION_RAW"}
    if "TRANSCODE" not in df.columns:
        return {"identity_clusters": [], "same_day_identity_clusters": [], "narrative_clusters": [], "identity_summary": [], "error": "Missing required column: TRANSCODE"}
    if "CREDIT" not in df.columns or "DEBIT" not in df.columns:
        return {"identity_clusters": [], "same_day_identity_clusters": [], "narrative_clusters": [], "identity_summary": [], "error": "Missing required columns: CREDIT and/or DEBIT"}

    # Prefer existing parser-provided IDENTITY when present; only backfill missing / dirty values.
    if "IDENTITY" not in df.columns:
        df["IDENTITY"] = df["DESCRIPTION_RAW"].apply(extract_identity)
    else:
        df["IDENTITY"] = df["IDENTITY"].apply(_canonical_identity)
        parsed_identity = df["DESCRIPTION_RAW"].apply(extract_identity)
        df["IDENTITY"] = df["IDENTITY"].where(df["IDENTITY"].notna(), parsed_identity)

    df["IDENTITY"] = df["IDENTITY"].apply(_canonical_identity)
    alias_map = _parser_build_identity_alias_map(df["IDENTITY"].dropna())
    if alias_map:
        df["IDENTITY"] = df["IDENTITY"].map(lambda x: alias_map.get(x, x) if x is not None else None)

    df["NARRATIVE_KEY"] = [
        build_narrative_key(desc, ident)
        for desc, ident in zip(df["DESCRIPTION_RAW"], df["IDENTITY"])
    ]

    identity_clusters = build_identity_clusters(df)
    same_day_identity_clusters = build_same_day_identity_clusters(df, date_col=date_col)
    narrative_clusters = build_narrative_clusters(df)
    identity_summary = build_identity_summary(df, date_col=date_col)

    all_clusters = []
    for block in (identity_clusters, same_day_identity_clusters, narrative_clusters):
        if isinstance(block, list):
            all_clusters.extend([x for x in block if isinstance(x, dict)])

    flagged_row_ids = sorted({
        int(rid)
        for item in all_clusters
        for rid in (item.get("flagged_row_ids") or [])
        if str(rid).strip()
    })

    total_recurrence_hits = int(sum(int(item.get("count") or 0) for item in all_clusters))
    total_recurrence_amount = float(sum(float(item.get("total_amount") or 0.0) for item in all_clusters))
    cluster_count = int(len(all_clusters))
    strength = float(min(1.0, (cluster_count / 12.0) + (min(total_recurrence_hits, 100) / 200.0)))

    indicators = []
    if cluster_count > 0:
        indicators.append(
            f"Recurring transaction behaviour detected across {cluster_count} cluster(s) "
            f"covering {total_recurrence_hits} repeated transaction hits."
        )

    return {
        "triggered": bool(cluster_count > 0),
        "strength": round(strength, 3),
        "indicators": indicators,
        "flagged_row_ids": flagged_row_ids,
        "pattern": "recurrence",
        "identity_clusters": identity_clusters,
        "same_day_identity_clusters": same_day_identity_clusters,
        "narrative_clusters": narrative_clusters,
        "identity_summary": identity_summary,
        "cluster_count": cluster_count,
        "total_recurrence_hits": total_recurrence_hits,
        "total_recurrence_amount": round(total_recurrence_amount, 2),
    }


# ============================================================
# 9. BACKWARDS COMPATIBILITY
# ============================================================

def detect_all_recurrence(df: pd.DataFrame, date_col: str = "DATE") -> Dict[str, Any]:
    return analyze_recurrence(df, date_col=date_col)
