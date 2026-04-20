from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional
import os
import re
import pandas as pd
import numpy as np


# ------------------------------------------------------------
# REPORTING THRESHOLD STRUCTURING BANDS (PNG)
# (Matches structured_deposits.py logic, applied to DEBITS)
# ------------------------------------------------------------

REPORTING_THRESHOLDS = [5000, 20000, 50000, 500000, 1000000]

AVOIDANCE_BANDS = [
    ("<5k", 4950.00, 4999.99, 5000),
    ("<20k", 19950.00, 19999.99, 20000),
    ("<50k", 49950.00, 49999.99, 50000),
    ("<500k", 499950.00, 499999.99, 500000),
    ("<1m", 999950.00, 999999.99, 1000000),
]

CONFIRMED_BANDS = [
    ("<5k_confirmed", 4999.00, 4999.99, 5000),
    ("<20k_confirmed", 19999.00, 19999.99, 20000),
    ("<50k_confirmed", 49999.00, 49999.99, 50000),
    ("<500k_confirmed", 499999.00, 499999.99, 500000),
    ("<1m_confirmed", 999999.00, 999999.99, 1000000),
]

MIN_REPEATS_FOR_THRESHOLD_STRUCTURING = 2


# ------------------------------------------------------------
# AMOUNT BUCKET DEFINITIONS
# ------------------------------------------------------------

AMOUNT_BUCKETS: List[Tuple[float, float]] = [
    (0.0, 500.0),
    (500.0, 1000.0),
    (1000.0, 5000.0),
    (5000.0, 10000.0),
    (10000.0, 20000.0),
    (20000.0, 50000.0),
    (50000.0, 100000.0),
    (100000.0, 500000.0),
    (500000.0, 1000000.0),
]


# -----------------------------------------------------------
# Savings + Current restriction via Number Codes.csv
# -----------------------------------------------------------

ICBA_NUMBER_CODES_CSV = os.getenv("ICBA_NUMBER_CODES_CSV", "Number Codes.csv")
ICBA_ALLOWED_ACCOUNTS = {"CURRENT ACCOUNT", "SAVINGS ACCOUNT"}


# -----------------------------------------------------------
# Heuristic keyword families
# -----------------------------------------------------------

FEE_LIKE_PATTERNS = (
    "FEE", "CHARGE", "CHARGES", "LEVY", "DUTY", "TAX", "GST", "WITHHOLDING",
    "AVERAGE BALANCE", "ACCOUNT SERVICE", "SERVICE FEE", "BANK FEE",
)

ATM_CASH_PATTERNS = (
    "ATM", "CASH WITHDRAWAL", "WITHDRAWAL", "FASTCASH", "CASH WD", "CASH OUT",
)

INTERNAL_TRANSFER_PATTERNS = (
    "OWN ACCOUNT", "OWN ACC", "TRANSFER BETWEEN OWN ACCOUNTS", "SELF TRANSFER",
)

MERCHANT_SERVICE_PATTERNS = (
    "IPA", "BSP", "DIGICEL", "PNG POWER", "AIR NIUGINI", "DATEC",
    "HOTEL", "SUPERMARKET", "POS PURCHASE", "VODAFONE", "TELIKOM",
)

THIRD_PARTY_TRANSFER_PATTERNS = (
    "IB OTHER ACC", "IB OTHER BANK", "MB TRF", "TRANSFER OUT", "ECHANNEL TRANSFER OUT",
    "TT", "TELEGRAPHIC", "SWIFT", "TO ",
)

MASKED_ACCOUNT_RE = re.compile(r"\*{2,}\d{2,}")

FEE_REGEX = re.compile("|".join(re.escape(x) for x in FEE_LIKE_PATTERNS), flags=re.IGNORECASE)
ATM_REGEX = re.compile("|".join(re.escape(x) for x in ATM_CASH_PATTERNS), flags=re.IGNORECASE)
INTERNAL_TRANSFER_REGEX = re.compile("|".join(re.escape(x) for x in INTERNAL_TRANSFER_PATTERNS), flags=re.IGNORECASE)
MERCHANT_SERVICE_REGEX = re.compile("|".join(re.escape(x) for x in MERCHANT_SERVICE_PATTERNS), flags=re.IGNORECASE)
THIRD_PARTY_REGEX = re.compile("|".join(re.escape(x) for x in THIRD_PARTY_TRANSFER_PATTERNS), flags=re.IGNORECASE)


def _load_allowed_transcodes_from_number_codes() -> set:
    """
    Reads Number Codes.csv and returns allowed TRANSCODEs for CURRENT + SAVINGS only.
    """
    path = ICBA_NUMBER_CODES_CSV
    if not path or not os.path.exists(path):
        return set()

    try:
        m = pd.read_csv(path)
    except Exception:
        return set()

    m.columns = [str(c).strip() for c in m.columns]
    if "Line Description" not in m.columns or "Tran. Code" not in m.columns:
        return set()

    m["Line Description"] = m["Line Description"].astype(str).str.strip().str.upper()
    m["Tran. Code"] = m["Tran. Code"].astype(str).str.strip()

    m = m[m["Line Description"].isin(ICBA_ALLOWED_ACCOUNTS)].copy()
    if m.empty:
        return set()

    return set(m["Tran. Code"].dropna().astype(str).str.strip().tolist())


def _safe_channel_label(df: pd.DataFrame) -> pd.Series:
    """
    Channel label used for 'single-channel only' enforcement.
    Prefer DESCRIPTION (from code_lookup in analyze_statement), fallback to TRANSCODE.
    """
    if "DESCRIPTION" in df.columns:
        s = df["DESCRIPTION"].astype(str).replace({"None": "", "nan": "", "NaN": ""})
        s = s.str.strip()
        return s.replace("", "UNKNOWN")
    s = df["TRANSCODE"].astype(str).replace({"None": "", "nan": "", "NaN": ""})
    s = s.str.strip()
    return s.replace("", "UNKNOWN")


def _find_bucket(amount: float) -> Tuple[float, float] | None:
    """Return (lo, hi) bucket that 'amount' falls into, or None."""
    for lo, hi in AMOUNT_BUCKETS:
        if lo <= amount < hi:
            return lo, hi
    return None


def _build_beneficiary_key(row) -> str:
    """
    Beneficiary clustering key:
      - Prefer IDENTITY (masked account or extracted name) from load_transactions()
      - Fallback: DESCRIPTION_RAW (uppercased, stripped)
    """
    identity = str(row.get("IDENTITY") or "").strip()
    if identity:
        return identity.upper()

    desc = str(row.get("DESCRIPTION_RAW") or "").strip()
    return desc.upper() if desc else "UNKNOWN"


def _is_round_amount(x: float, round_to: float = 100.0) -> bool:
    """
    Smurfed round-figure test:
      - multiples of `round_to` (default: 100)
    """
    try:
        v = float(x or 0.0)
    except Exception:
        return False
    if v <= 0:
        return False
    return abs(v - (round(v / round_to) * round_to)) < 0.01


def _roundish_flags(amount: float) -> Dict[str, bool]:
    """
    Deposits-style round-figure support:
    - cents end in .00 / .50 / .99
    - OR kina multiple of 10/50/100/500/1000
    """
    if amount is None or not np.isfinite(amount) or amount <= 0:
        return {"is_roundish": False, "round_cents": False, "kina_multiple": False}

    cents = int(round((amount - int(amount)) * 100)) % 100
    round_cents = cents in (0, 50, 99)

    kina = int(amount)
    kina_multiple = False
    for m in (10, 50, 100, 500, 1000):
        if kina >= m and (kina % m) == 0:
            kina_multiple = True
            break

    return {
        "is_roundish": bool(round_cents or kina_multiple),
        "round_cents": bool(round_cents),
        "kina_multiple": bool(kina_multiple),
    }


def _assign_threshold_band(amount: float) -> Optional[Tuple[str, float, float, int, str]]:
    """
    Confirmed band wins if overlap.
    Returns: (band_name, low, high, threshold, band_type) where band_type in {'confirmed','avoidance'}.
    """
    if amount is None or not np.isfinite(amount) or amount <= 0:
        return None

    for name, lo, hi, thr in CONFIRMED_BANDS:
        if lo <= amount <= hi:
            return (name, lo, hi, thr, "confirmed")

    for name, lo, hi, thr in AVOIDANCE_BANDS:
        if lo <= amount <= hi:
            return (name, lo, hi, thr, "avoidance")

    return None


def _safe_dt_str(dt) -> Optional[str]:
    try:
        if pd.isna(dt):
            return None
        return pd.to_datetime(dt).strftime("%Y-%m-%d")
    except Exception:
        return None


def _collect_flagged_row_ids(window: pd.DataFrame) -> List[int]:
    out: List[int] = []
    if "ROW_ID" in window.columns:
        try:
            out = [int(x) for x in window["ROW_ID"].dropna().astype(int).tolist()]
        except Exception:
            out = []
    return sorted(set(out))


def _bucket_meta_for_window(window: pd.DataFrame) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Returns (bucket_lo, bucket_hi, ceiling_threshold) based on dominant bucket by count.
    Used for near-ceiling ratio.
    """
    if window.empty:
        return None, None, None

    amounts = window["DEBIT"].astype(float).to_numpy()
    buckets = [_find_bucket(float(v or 0.0)) for v in amounts]

    s = pd.Series(buckets)
    if s.dropna().empty:
        return None, None, None

    dominant = s.dropna().value_counts().index[0]
    bucket_lo, bucket_hi = float(dominant[0]), float(dominant[1])
    hi = float(bucket_hi)
    margin = min(hi * 0.1, 500.0)
    ceiling_threshold = hi - margin
    return bucket_lo, bucket_hi, ceiling_threshold


def _monthly_pattern_summary_unknown(df_hits: pd.DataFrame, min_repeats: int) -> Dict[str, Any]:
    """
    UNKNOWN key monthly confirmation:
      - repeated band within same month OR
      - same band across >=2 months
    """
    if df_hits.empty:
        return {"has_monthly_pattern": False, "reason": None, "support": {}}

    g = df_hits.copy()
    g["YM"] = g["DATE"].dt.to_period("M").astype(str)

    within = (
        g.groupby(["YM", "BAND_NAME"]).size().reset_index(name="count")
         .sort_values("count", ascending=False)
    )
    hit_within = within[within["count"] >= min_repeats]
    if not hit_within.empty:
        top = hit_within.iloc[0].to_dict()
        return {
            "has_monthly_pattern": True,
            "reason": "Repeated near-threshold payments within the same month for UNKNOWN beneficiary.",
            "support": {"top_month_band": top, "within_month": within.to_dict(orient="records")[:30]},
        }

    months_per_band = (
        g.groupby("BAND_NAME")["YM"].nunique().reset_index(name="month_count")
         .sort_values("month_count", ascending=False)
    )
    hit_across = months_per_band[months_per_band["month_count"] >= 2]
    if not hit_across.empty:
        top = hit_across.iloc[0].to_dict()
        return {
            "has_monthly_pattern": True,
            "reason": "Near-threshold payment band recurs across multiple months for UNKNOWN beneficiary.",
            "support": {"top_band": top, "months_per_band": months_per_band.to_dict(orient="records")},
        }

    return {
        "has_monthly_pattern": False,
        "reason": None,
        "support": {"within_month": within.to_dict(orient="records")[:30]},
    }


def _norm_text(x: Any) -> str:
    return str(x or "").strip().upper()


def _description_text(row: pd.Series) -> str:
    return " | ".join(
        [
            _norm_text(row.get("DESCRIPTION")),
            _norm_text(row.get("DESCRIPTION_RAW")),
            _norm_text(row.get("TRANSCODE")),
        ]
    )


def _contains_any(text: str, patterns: Tuple[str, ...]) -> bool:
    t = _norm_text(text)
    return any(p in t for p in patterns)


def _is_fee_like_text(text: str) -> bool:
    return _contains_any(text, FEE_LIKE_PATTERNS)


def _is_atm_or_cash_text(text: str) -> bool:
    return _contains_any(text, ATM_CASH_PATTERNS)


def _looks_masked_account(text: str) -> bool:
    return bool(MASKED_ACCOUNT_RE.search(_norm_text(text)))


def _counterparty_type(row: pd.Series) -> str:
    """
    Heuristic classification:
      - own_account
      - merchant_or_service_like
      - third_party
      - unknown
    """
    ben = _norm_text(row.get("BENEFICIARY_KEY"))
    text = _description_text(row)

    if ben == "UNKNOWN":
        if _is_atm_or_cash_text(text):
            return "unknown"
        return "unknown"

    if _contains_any(text, INTERNAL_TRANSFER_PATTERNS):
        return "own_account"

    if _contains_any(text, MERCHANT_SERVICE_PATTERNS):
        return "merchant_or_service_like"

    if _looks_masked_account(ben) or _contains_any(text, THIRD_PARTY_TRANSFER_PATTERNS):
        return "third_party"

    return "third_party"


def _window_counterparty_mix(window: pd.DataFrame) -> Dict[str, int]:
    if window.empty:
        return {}
    s = window["COUNTERPARTY_TYPE"].astype(str).value_counts()
    return {str(k): int(v) for k, v in s.to_dict().items()}


def _dominant_counterparty_type(window: pd.DataFrame) -> str:
    mix = _window_counterparty_mix(window)
    if not mix:
        return "unknown"
    return max(mix.items(), key=lambda kv: kv[1])[0]


def _fee_like_ratio(window: pd.DataFrame) -> float:
    if window.empty:
        return 0.0
    if "IS_FEE_LIKE" in window.columns:
        vals = window["IS_FEE_LIKE"]
        return float(vals.mean()) if len(vals) else 0.0
    vals = window["TEXT_FULL"].astype(str).map(_is_fee_like_text)
    return float(vals.mean()) if len(vals) else 0.0


def _atm_ratio(window: pd.DataFrame) -> float:
    if window.empty:
        return 0.0
    if "IS_ATM_LIKE" in window.columns:
        vals = window["IS_ATM_LIKE"]
        return float(vals.mean()) if len(vals) else 0.0
    vals = window["TEXT_FULL"].astype(str).map(_is_atm_or_cash_text)
    return float(vals.mean()) if len(vals) else 0.0


def _largest_exact_amount_repeat(amounts: pd.Series) -> int:
    if amounts is None or len(amounts) == 0:
        return 0
    return int(amounts.round(2).value_counts().iloc[0])


def _similar_amount_metrics(
    amounts: pd.Series,
    pct_tol: float = 0.02,
    min_abs_tol: float = 50.0,
    max_abs_tol: float = 250.0,
) -> Dict[str, Any]:
    """
    Finds dominant near-equal amount cluster.
    Amounts are grouped around a representative amount using adaptive tolerance:
      tolerance = max(min_abs_tol, rep * pct_tol), capped at max_abs_tol
    """
    if amounts is None or len(amounts) == 0:
        return {
            "largest_cluster_size": 0,
            "similar_amount_ratio": 0.0,
            "cluster_min": None,
            "cluster_max": None,
            "cluster_center": None,
        }

    vals = sorted([float(x) for x in amounts.dropna().astype(float).tolist() if float(x) > 0])
    if not vals:
        return {
            "largest_cluster_size": 0,
            "similar_amount_ratio": 0.0,
            "cluster_min": None,
            "cluster_max": None,
            "cluster_center": None,
        }

    best_cluster: List[float] = []
    n = len(vals)

    for i in range(n):
        rep = vals[i]
        tol = min(max(min_abs_tol, rep * pct_tol), max_abs_tol)
        cluster = [x for x in vals if abs(x - rep) <= tol]
        if len(cluster) > len(best_cluster):
            best_cluster = cluster

    if not best_cluster:
        return {
            "largest_cluster_size": 0,
            "similar_amount_ratio": 0.0,
            "cluster_min": None,
            "cluster_max": None,
            "cluster_center": None,
        }

    return {
        "largest_cluster_size": int(len(best_cluster)),
        "similar_amount_ratio": float(len(best_cluster) / len(vals)),
        "cluster_min": round(float(min(best_cluster)), 2),
        "cluster_max": round(float(max(best_cluster)), 2),
        "cluster_center": round(float(np.median(best_cluster)), 2),
    }


def _monthly_amount_concentration(mdf: pd.DataFrame, round_to: float = 100.0) -> Dict[str, Any]:
    amounts = mdf["DEBIT"].astype(float)
    if "IS_ROUND_AMOUNT" in mdf.columns and len(amounts):
        round_ratio = float(mdf["IS_ROUND_AMOUNT"].mean())
    else:
        round_ratio = float(amounts.map(lambda x: _is_round_amount(x, round_to)).mean()) if len(amounts) else 0.0
    sim = _similar_amount_metrics(amounts)
    return {
        "round_ratio": round_ratio,
        "largest_cluster_size": int(sim["largest_cluster_size"]),
        "similar_amount_ratio": float(sim["similar_amount_ratio"]),
        "cluster_min": sim["cluster_min"],
        "cluster_max": sim["cluster_max"],
        "cluster_center": sim["cluster_center"],
    }


def _window_support_metrics(
    window: pd.DataFrame,
    round_to: float = 100.0,
) -> Dict[str, Any]:
    amounts = window["DEBIT"].astype(float)
    if "IS_ROUND_AMOUNT" in window.columns and len(amounts):
        round_ratio = float(window["IS_ROUND_AMOUNT"].mean())
    else:
        round_ratio = float(amounts.map(lambda x: _is_round_amount(x, round_to)).mean()) if len(amounts) else 0.0

    mirror_amount_count = _largest_exact_amount_repeat(amounts)
    sim = _similar_amount_metrics(amounts)

    bucket_lo, bucket_hi, ceiling_threshold = _bucket_meta_for_window(window)
    near_ceiling_ratio = float((amounts >= float(ceiling_threshold)).mean()) if ceiling_threshold is not None else 0.0

    return {
        "round_ratio": round_ratio,
        "mirror_amount_count": mirror_amount_count,
        "largest_similar_cluster_size": int(sim["largest_cluster_size"]),
        "similar_amount_ratio": float(sim["similar_amount_ratio"]),
        "similar_cluster_min": sim["cluster_min"],
        "similar_cluster_max": sim["cluster_max"],
        "similar_cluster_center": sim["cluster_center"],
        "bucket_lo": bucket_lo,
        "bucket_hi": bucket_hi,
        "ceiling_threshold": ceiling_threshold,
        "near_ceiling_ratio": near_ceiling_ratio,
    }


def _qualifies_by_support(
    round_ratio: float,
    near_ceiling_ratio: float,
    similar_amount_ratio: float,
    min_round_ratio: float,
    near_ceiling_ratio_threshold: float,
    min_similar_amount_ratio: float,
) -> bool:
    return bool(
        round_ratio >= min_round_ratio
        or near_ceiling_ratio >= near_ceiling_ratio_threshold
        or similar_amount_ratio >= min_similar_amount_ratio
    )


def _severity_from_score(score: float) -> str:
    if score >= 0.8:
        return "high"
    if score >= 0.5:
        return "medium"
    return "low"


def _score_common_window(
    num_txns: int,
    span_days: int,
    round_ratio: float,
    near_ceiling_ratio: float,
    mirror_amount_count: int,
    similar_amount_ratio: float,
    unique_beneficiaries: int = 1,
    counterparty_type: str = "unknown",
    fee_like_ratio: float = 0.0,
    atm_ratio: float = 0.0,
    min_round_ratio: float = 0.6,
    near_ceiling_ratio_threshold: float = 0.6,
    min_similar_amount_ratio: float = 0.6,
) -> float:
    score = 0.0

    if num_txns >= 10:
        score += 0.30
    elif num_txns >= 6:
        score += 0.24
    elif num_txns >= 3:
        score += 0.16
    else:
        score += 0.10

    if span_days <= 0:
        score += 0.18
    elif span_days <= 1:
        score += 0.14
    elif span_days <= 3:
        score += 0.10

    if round_ratio >= 0.8:
        score += 0.18
    elif round_ratio >= min_round_ratio:
        score += 0.12

    if similar_amount_ratio >= 0.85:
        score += 0.22
    elif similar_amount_ratio >= min_similar_amount_ratio:
        score += 0.15

    if near_ceiling_ratio >= 0.8:
        score += 0.16
    elif near_ceiling_ratio >= near_ceiling_ratio_threshold:
        score += 0.10

    if mirror_amount_count >= 5:
        score += 0.12
    elif mirror_amount_count >= 3:
        score += 0.08

    if unique_beneficiaries >= 5:
        score += 0.16
    elif unique_beneficiaries >= 3:
        score += 0.10

    if counterparty_type == "third_party":
        score += 0.10
    elif counterparty_type == "own_account":
        score += 0.04
    elif counterparty_type == "merchant_or_service_like":
        score -= 0.08

    if atm_ratio >= 0.8:
        score += 0.14

    if fee_like_ratio >= 0.5:
        score -= 0.20

    return float(min(1.0, max(0.0, score)))


def _transcode_counts(window: pd.DataFrame) -> Dict[str, int]:
    s = window["TRANSCODE"].astype(str).value_counts()
    return {str(k): int(v) for k, v in s.to_dict().items()}


def _beneficiary_counts(window: pd.DataFrame) -> Dict[str, int]:
    if "BENEFICIARY_KEY" not in window.columns or window.empty:
        return {}
    s = window["BENEFICIARY_KEY"].astype(str).value_counts().head(20)
    return {str(k): int(v) for k, v in s.to_dict().items()}


def _base_cluster_payload(
    window: pd.DataFrame,
    pattern_subtype: str,
    channel: str,
    beneficiary_key: Optional[str] = None,
    months: Optional[List[str]] = None,
    date_start=None,
    date_end=None,
    extra: Optional[Dict[str, Any]] = None,
    round_to: float = 100.0,
    min_round_ratio: float = 0.6,
    near_ceiling_ratio_threshold: float = 0.6,
    min_similar_amount_ratio: float = 0.6,
) -> Dict[str, Any]:
    total_amount = float(window["DEBIT"].sum())
    num_txns = int(len(window))
    avg_amount = round(total_amount / float(num_txns), 2) if num_txns else 0.0

    if date_start is None:
        date_start = window["DATE"].min()
    if date_end is None:
        date_end = window["DATE"].max()

    span_days = int((date_end - date_start).days) if pd.notna(date_end) and pd.notna(date_start) else 0

    support = _window_support_metrics(window, round_to=round_to)
    fee_ratio = _fee_like_ratio(window)
    atm_ratio = _atm_ratio(window)
    unique_beneficiaries = int(window["BENEFICIARY_KEY"].nunique()) if "BENEFICIARY_KEY" in window.columns else 1
    cp_type = _dominant_counterparty_type(window)

    score = _score_common_window(
        num_txns=num_txns,
        span_days=span_days,
        round_ratio=float(support["round_ratio"]),
        near_ceiling_ratio=float(support["near_ceiling_ratio"]),
        mirror_amount_count=int(support["mirror_amount_count"]),
        similar_amount_ratio=float(support["similar_amount_ratio"]),
        unique_beneficiaries=unique_beneficiaries,
        counterparty_type=cp_type,
        fee_like_ratio=fee_ratio,
        atm_ratio=atm_ratio,
        min_round_ratio=min_round_ratio,
        near_ceiling_ratio_threshold=near_ceiling_ratio_threshold,
        min_similar_amount_ratio=min_similar_amount_ratio,
    )

    payload = {
        "pattern_subtype": str(pattern_subtype),
        "beneficiary_key": str(beneficiary_key) if beneficiary_key is not None else None,
        "channel": str(channel),
        "months": months,
        "bucket_lo": support["bucket_lo"],
        "bucket_hi": support["bucket_hi"],
        "date_start": _safe_dt_str(date_start),
        "date_end": _safe_dt_str(date_end),
        "span_days": span_days,
        "num_txns": num_txns,
        "unique_beneficiaries": unique_beneficiaries,
        "total_amount": round(total_amount, 2),
        "avg_amount": avg_amount,
        "round_ratio": round(float(support["round_ratio"]), 2),
        "near_ceiling_ratio": round(float(support["near_ceiling_ratio"]), 2),
        "mirror_amount_count": int(support["mirror_amount_count"]),
        "similar_amount_ratio": round(float(support["similar_amount_ratio"]), 2),
        "largest_similar_cluster_size": int(support["largest_similar_cluster_size"]),
        "similar_cluster_min": support["similar_cluster_min"],
        "similar_cluster_max": support["similar_cluster_max"],
        "similar_cluster_center": support["similar_cluster_center"],
        "counterparty_type": cp_type,
        "counterparty_mix": _window_counterparty_mix(window),
        "fee_like_ratio": round(fee_ratio, 2),
        "atm_ratio": round(atm_ratio, 2),
        "severity": _severity_from_score(score),
        "score": float(score),
        "flagged_row_ids": _collect_flagged_row_ids(window),
        "transcodes": _transcode_counts(window),
        "beneficiary_counts": _beneficiary_counts(window),
    }

    if extra:
        payload.update(extra)

    return payload


def _prepare_debits(df: pd.DataFrame) -> pd.DataFrame:
    debits = df.copy()
    debits["DATE_ONLY"] = debits["DATE"].dt.floor("D")
    debits["YEAR_MONTH"] = debits["DATE"].dt.to_period("M").astype(str)

    desc = debits.get("DESCRIPTION", pd.Series([""] * len(debits), index=debits.index)).fillna("").astype(str).str.strip()
    raw_desc = debits.get("DESCRIPTION_RAW", pd.Series([""] * len(debits), index=debits.index)).fillna("").astype(str).str.strip()
    debits["TEXT_FULL"] = (desc + " " + raw_desc).str.strip().str.upper()

    debits["CHANNEL_LABEL"] = _safe_channel_label(debits)

    if "BENEFICIARY_KEY" not in debits.columns:
        ident = debits.get("IDENTITY", pd.Series([""] * len(debits), index=debits.index)).fillna("").astype(str).str.strip()
        raw = debits.get("DESCRIPTION_RAW", pd.Series([""] * len(debits), index=debits.index)).fillna("").astype(str).str.strip()
        debits["BENEFICIARY_KEY"] = np.where(
            ident != "",
            ident.str.upper(),
            np.where(raw != "", raw.str.upper(), "UNKNOWN"),
        )

    debits["BENEFICIARY_KEY"] = debits["BENEFICIARY_KEY"].astype(str).str.strip().replace("", "UNKNOWN")
    debits["DEBIT_R2"] = debits["DEBIT"].round(2)
    debits["IS_ROUND_AMOUNT"] = ((debits["DEBIT"] / 100.0).round(2) - (debits["DEBIT"] / 100.0).round(0)).abs() < 0.0001

    debits["IS_FEE_LIKE"] = debits["TEXT_FULL"].str.contains(FEE_REGEX, na=False)
    debits["IS_ATM_LIKE"] = debits["TEXT_FULL"].str.contains(ATM_REGEX, na=False)

    ben = debits["BENEFICIARY_KEY"].astype(str).str.upper()
    txt = debits["TEXT_FULL"].astype(str)

    debits["COUNTERPARTY_TYPE"] = np.select(
        [
            ben.eq("UNKNOWN"),
            txt.str.contains(INTERNAL_TRANSFER_REGEX, na=False),
            txt.str.contains(MERCHANT_SERVICE_REGEX, na=False),
            ben.str.contains(MASKED_ACCOUNT_RE) | txt.str.contains(THIRD_PARTY_REGEX, na=False),
        ],
        ["unknown", "own_account", "merchant_or_service_like", "third_party"],
        default="third_party",
    )

    binfo = debits["DEBIT"].astype(float).map(_assign_threshold_band)
    debits["BAND_NAME"] = binfo.map(lambda x: x[0] if x else None)
    debits["BAND_TYPE"] = binfo.map(lambda x: x[4] if x else None)
    debits["BAND_THRESHOLD"] = binfo.map(lambda x: x[3] if x else None)

    rf = debits["DEBIT"].astype(float).map(_roundish_flags)
    debits["IS_ROUNDISH"] = rf.map(lambda d: d["is_roundish"])
    debits["ROUND_CENTS"] = rf.map(lambda d: d["round_cents"])
    debits["KINA_MULTIPLE"] = rf.map(lambda d: d["kina_multiple"])

    return debits.sort_values("DATE").reset_index(drop=True)


def _iter_gap_windows(dates: List[pd.Timestamp], max_gap_days: int):
    """
    Yield contiguous [start, end) windows where the gap between consecutive rows
    stays within max_gap_days.
    """
    n = len(dates)
    if n == 0:
        return

    start = 0
    last_date = dates[0]

    for i in range(1, n):
        d = dates[i]
        gap = int((d - last_date).days)
        if gap > max_gap_days:
            yield start, i
            start = i
        last_date = d

    yield start, n


# ============================================================
# MAIN DETECTOR
# ============================================================

def detect_structured_payments(
    df: pd.DataFrame,
    # core thresholds
    min_txns_per_cluster: int = 3,
    min_total_amount: float = 10000.0,
    max_gap_days: int = 7,
    # same-day split detection
    min_same_day_txns: int = 2,
    # recurring monthly detection
    min_months: int = 2,
    min_txns_per_month: int = 2,
    # smurfing / round figure emphasis
    round_to: float = 100.0,
    min_round_ratio: float = 0.6,
    # near ceiling emphasis
    near_ceiling_ratio_threshold: float = 0.6,
    # new support: near-equal amount clustering
    min_similar_amount_ratio: float = 0.6,
    # new support: multi-party fragmentation
    min_unique_beneficiaries_multi_party: int = 3,
    # new support: ATM same-day bursts
    min_same_day_atm_txns: int = 3,
    analysis_cache: Optional[Dict[str, Any]] = None,
    prior_results: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Detects payment structuring (OUTFLOWS).

    Enforcements:
      - Savings + Current only (TRANSCODE filtered using Number Codes.csv)
      - No channel hopping: patterns must occur within one channel label
      - Single-party patterns remain grouped by (beneficiary, channel)
      - Adds multi-party fragmentation detection per channel/day and per rolling window
      - Adds ATM/cash-out burst detection
      - Adds near-equal amount clustering support
      - Adds same-day multi-party threshold-band aggregation
    """

    empty = {"triggered": False, "strength": 0.0, "clusters": [], "flagged_row_ids": [], "pattern": None}

    if df is None or df.empty:
        return empty

    analysis_cache = analysis_cache or {}
    prior_results = prior_results or {}

    base = analysis_cache.get("debits_df") if isinstance(analysis_cache.get("debits_df"), pd.DataFrame) else df
    df = base.copy()

    if "ROW_ID" not in df.columns:
        df["ROW_ID"] = df.index

    if "DATE" not in df.columns:
        raise ValueError("detect_structured_payments: df must have a DATE column")

    if not pd.api.types.is_datetime64_any_dtype(df["DATE"]):
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

    df = df.dropna(subset=["DATE"]).copy()

    if "TRANSCODE" not in df.columns:
        return empty
    df["TRANSCODE"] = df["TRANSCODE"].astype(str).str.strip().replace({"": "UNKNOWN"})

    allowed_codes = _load_allowed_transcodes_from_number_codes()
    if allowed_codes:
        df = df[df["TRANSCODE"].isin(allowed_codes)].copy()
        if df.empty:
            return empty

    if "DESCRIPTION_RAW" not in df.columns:
        df["DESCRIPTION_RAW"] = ""

    if "DEBIT" not in df.columns:
        return empty

    df["DEBIT"] = pd.to_numeric(df["DEBIT"], errors="coerce").fillna(0.0)
    debits = df[df["DEBIT"] > 0].copy()
    if debits.empty:
        return empty

    debits = _prepare_debits(debits)

    clusters: List[Dict[str, Any]] = []

    # ========================================================
    # 1) SINGLE-PARTY SMURFING PATTERNS (PER BENEFICIARY + CHANNEL)
    # ========================================================
    for (ben, channel), df_bc in debits.groupby(["BENEFICIARY_KEY", "CHANNEL_LABEL"], sort=False):
        if df_bc.empty:
            continue

        # A) SAME-DAY SPLITS (ben+channel)
        for date_only, day_rows in df_bc.groupby("DATE_ONLY", sort=False):
            if len(day_rows) < min_same_day_txns:
                continue

            total_amount = float(day_rows["DEBIT"].sum())
            if total_amount < min_total_amount:
                continue

            support = _window_support_metrics(day_rows, round_to=round_to)
            if not _qualifies_by_support(
                round_ratio=float(support["round_ratio"]),
                near_ceiling_ratio=float(support["near_ceiling_ratio"]),
                similar_amount_ratio=float(support["similar_amount_ratio"]),
                min_round_ratio=min_round_ratio,
                near_ceiling_ratio_threshold=near_ceiling_ratio_threshold,
                min_similar_amount_ratio=min_similar_amount_ratio,
            ):
                continue

            clusters.append(
                _base_cluster_payload(
                    day_rows,
                    pattern_subtype="same_day_split_single_party",
                    beneficiary_key=str(ben),
                    channel=str(channel),
                    date_start=pd.to_datetime(date_only),
                    date_end=pd.to_datetime(date_only),
                    round_to=round_to,
                    min_round_ratio=min_round_ratio,
                    near_ceiling_ratio_threshold=near_ceiling_ratio_threshold,
                    min_similar_amount_ratio=min_similar_amount_ratio,
                )
            )

        # B) CLOSE-DATE ROLLING WINDOWS (ben+channel)
        rows = df_bc.reset_index(drop=True)
        dates = rows["DATE"].tolist()

        for start, end in _iter_gap_windows(dates, max_gap_days):
            window = rows.iloc[start:end]
            if len(window) < min_txns_per_cluster:
                continue

            total_amount = float(window["DEBIT"].sum())
            if total_amount < min_total_amount:
                continue

            support = _window_support_metrics(window, round_to=round_to)
            if not _qualifies_by_support(
                round_ratio=float(support["round_ratio"]),
                near_ceiling_ratio=float(support["near_ceiling_ratio"]),
                similar_amount_ratio=float(support["similar_amount_ratio"]),
                min_round_ratio=min_round_ratio,
                near_ceiling_ratio_threshold=near_ceiling_ratio_threshold,
                min_similar_amount_ratio=min_similar_amount_ratio,
            ):
                continue

            clusters.append(
                _base_cluster_payload(
                    window,
                    pattern_subtype="close_date_split_single_party",
                    beneficiary_key=str(ben),
                    channel=str(channel),
                    round_to=round_to,
                    min_round_ratio=min_round_ratio,
                    near_ceiling_ratio_threshold=near_ceiling_ratio_threshold,
                    min_similar_amount_ratio=min_similar_amount_ratio,
                )
            )

        # C) MONTHLY RECURRING (ben+channel)
        month_groups = []
        for ym, mdf in df_bc.groupby("YEAR_MONTH", sort=False):
            if len(mdf) < min_txns_per_month:
                continue

            total_amount = float(mdf["DEBIT"].sum())
            if total_amount < min_total_amount:
                continue

            m_support = _monthly_amount_concentration(mdf, round_to=round_to)
            qualifies = (
                m_support["round_ratio"] >= min_round_ratio
                or m_support["similar_amount_ratio"] >= min_similar_amount_ratio
                or m_support["largest_cluster_size"] >= min_txns_per_month
            )
            if not qualifies:
                continue

            month_groups.append((ym, mdf))

        if len(month_groups) >= min_months:
            combined = pd.concat([g[1] for g in month_groups], ignore_index=True)
            clusters.append(
                _base_cluster_payload(
                    combined,
                    pattern_subtype="monthly_recurring_single_party",
                    beneficiary_key=str(ben),
                    channel=str(channel),
                    months=[x[0] for x in month_groups],
                    round_to=round_to,
                    min_round_ratio=min_round_ratio,
                    near_ceiling_ratio_threshold=near_ceiling_ratio_threshold,
                    min_similar_amount_ratio=min_similar_amount_ratio,
                )
            )

    # ========================================================
    # 2) SAME-DAY MULTI-PARTY FRAGMENTATION (PER CHANNEL)
    # ========================================================
    for (channel, date_only), day_rows in debits.groupby(["CHANNEL_LABEL", "DATE_ONLY"], sort=False):
        if len(day_rows) < min_same_day_txns:
            continue

        total_amount = float(day_rows["DEBIT"].sum())
        if total_amount < min_total_amount:
            continue

        unique_bens = int(day_rows["BENEFICIARY_KEY"].nunique())
        if unique_bens < min_unique_beneficiaries_multi_party:
            continue

        fee_ratio = _fee_like_ratio(day_rows)
        if fee_ratio >= 0.8:
            continue

        support = _window_support_metrics(day_rows, round_to=round_to)
        if not _qualifies_by_support(
            round_ratio=float(support["round_ratio"]),
            near_ceiling_ratio=float(support["near_ceiling_ratio"]),
            similar_amount_ratio=float(support["similar_amount_ratio"]),
            min_round_ratio=min_round_ratio,
            near_ceiling_ratio_threshold=near_ceiling_ratio_threshold,
            min_similar_amount_ratio=min_similar_amount_ratio,
        ):
            continue

        clusters.append(
            _base_cluster_payload(
                day_rows,
                pattern_subtype="same_day_multi_party_fragmentation",
                channel=str(channel),
                date_start=pd.to_datetime(date_only),
                date_end=pd.to_datetime(date_only),
                extra={
                    "multi_party": True,
                    "fragmentation_type": "same_day",
                },
                round_to=round_to,
                min_round_ratio=min_round_ratio,
                near_ceiling_ratio_threshold=near_ceiling_ratio_threshold,
                min_similar_amount_ratio=min_similar_amount_ratio,
            )
        )

    # ========================================================
    # 3) CLOSE-DATE MULTI-PARTY FRAGMENTATION (PER CHANNEL)
    # ========================================================
    for channel, df_c in debits.groupby("CHANNEL_LABEL", sort=False):
        if df_c.empty:
            continue

        rows = df_c.sort_values("DATE").reset_index(drop=True)
        dates = rows["DATE"].tolist()

        for start, end in _iter_gap_windows(dates, max_gap_days):
            window = rows.iloc[start:end]
            if len(window) < min_txns_per_cluster:
                continue

            total_amount = float(window["DEBIT"].sum())
            if total_amount < min_total_amount:
                continue

            unique_bens = int(window["BENEFICIARY_KEY"].nunique())
            if unique_bens < min_unique_beneficiaries_multi_party:
                continue

            fee_ratio = _fee_like_ratio(window)
            if fee_ratio >= 0.8:
                continue

            support = _window_support_metrics(window, round_to=round_to)
            if not _qualifies_by_support(
                round_ratio=float(support["round_ratio"]),
                near_ceiling_ratio=float(support["near_ceiling_ratio"]),
                similar_amount_ratio=float(support["similar_amount_ratio"]),
                min_round_ratio=min_round_ratio,
                near_ceiling_ratio_threshold=near_ceiling_ratio_threshold,
                min_similar_amount_ratio=min_similar_amount_ratio,
            ):
                continue

            clusters.append(
                _base_cluster_payload(
                    window,
                    pattern_subtype="close_date_multi_party_fragmentation",
                    channel=str(channel),
                    extra={
                        "multi_party": True,
                        "fragmentation_type": "rolling_window",
                    },
                    round_to=round_to,
                    min_round_ratio=min_round_ratio,
                    near_ceiling_ratio_threshold=near_ceiling_ratio_threshold,
                    min_similar_amount_ratio=min_similar_amount_ratio,
                )
            )

    # ========================================================
    # 4) ATM / CASH-OUT BURSTS
    # ========================================================
    atm_like = debits[debits["IS_ATM_LIKE"]].copy()
    if not atm_like.empty:
        for (channel, date_only), day_rows in atm_like.groupby(["CHANNEL_LABEL", "DATE_ONLY"], sort=False):
            if len(day_rows) < min_same_day_atm_txns:
                continue

            total_amount = float(day_rows["DEBIT"].sum())
            if total_amount < min_total_amount and len(day_rows) < max(4, min_same_day_atm_txns):
                continue

            support = _window_support_metrics(day_rows, round_to=round_to)
            if not (
                float(support["similar_amount_ratio"]) >= min_similar_amount_ratio
                or float(support["round_ratio"]) >= min_round_ratio
                or int(support["mirror_amount_count"]) >= 2
            ):
                continue

            clusters.append(
                _base_cluster_payload(
                    day_rows,
                    pattern_subtype="atm_cashout_burst",
                    channel=str(channel),
                    date_start=pd.to_datetime(date_only),
                    date_end=pd.to_datetime(date_only),
                    extra={
                        "multi_party": False,
                        "fragmentation_type": "cash_extraction",
                    },
                    round_to=round_to,
                    min_round_ratio=min_round_ratio,
                    near_ceiling_ratio_threshold=near_ceiling_ratio_threshold,
                    min_similar_amount_ratio=min_similar_amount_ratio,
                )
            )

    # ========================================================
    # 5) De-duplicate clusters by subtype + identical row-id sets
    # ========================================================
    dedup: List[Dict[str, Any]] = []
    seen: set[Tuple[str, Tuple[int, ...]]] = set()

    for c in clusters:
        row_key = tuple(sorted([int(x) for x in (c.get("flagged_row_ids") or [])]))
        subtype = str(c.get("pattern_subtype") or "")
        if not row_key:
            continue
        key = (subtype, row_key)
        if key in seen:
            continue
        seen.add(key)
        dedup.append(c)

    clusters = dedup

    # ========================================================
    # 6) THRESHOLD BAND STRUCTURING (avoidance/confirmed) — PER CHANNEL
    #    Includes:
    #      A) known beneficiary aggregation
    #      B) UNKNOWN monthly confirmation
    #      C) same-day multi-party threshold fragmentation
    # ========================================================
    threshold_structuring = {
        "aggregation": [],
        "unknown_monthly_pattern": None,
        "same_day_multi_party": [],
        "hits": [],
        "flags": {},
    }

    hits = debits[debits["BAND_NAME"].notna()].copy()

    if not hits.empty:
        threshold_structuring["hits"] = hits[
            [
                "ROW_ID", "DATE", "BENEFICIARY_KEY", "CHANNEL_LABEL", "TRANSCODE", "DEBIT",
                "BAND_NAME", "BAND_TYPE", "BAND_THRESHOLD",
                "IS_ROUNDISH", "ROUND_CENTS", "KINA_MULTIPLE"
            ]
        ].to_dict(orient="records")

        aggregation: List[Dict[str, Any]] = []
        same_day_multi_party_aggs: List[Dict[str, Any]] = []
        confirmed_repeat = False
        avoidance_repeat = False
        roundish_support = False

        # A) known beneficiary aggregation (beneficiary + channel)
        known_hits = hits[hits["BENEFICIARY_KEY"] != "UNKNOWN"].copy()
        if not known_hits.empty:
            grp = (
                known_hits.groupby(
                    ["BENEFICIARY_KEY", "CHANNEL_LABEL", "BAND_NAME", "BAND_TYPE", "BAND_THRESHOLD"],
                    sort=False
                )
                .agg(
                    count=("BAND_NAME", "size"),
                    total_amount=("DEBIT", "sum"),
                    roundish_count=("IS_ROUNDISH", "sum"),
                    first_date=("DATE", "min"),
                    last_date=("DATE", "max"),
                )
                .reset_index()
            )

            qualified = grp[grp["count"] >= MIN_REPEATS_FOR_THRESHOLD_STRUCTURING].copy()

            for _, row in qualified.iterrows():
                subset = known_hits[
                    (known_hits["BENEFICIARY_KEY"] == row["BENEFICIARY_KEY"]) &
                    (known_hits["CHANNEL_LABEL"] == row["CHANNEL_LABEL"]) &
                    (known_hits["BAND_NAME"] == row["BAND_NAME"])
                ]

                ch_counts = _transcode_counts(subset)

                if str(row["BAND_TYPE"]) == "confirmed":
                    confirmed_repeat = True
                if str(row["BAND_TYPE"]) == "avoidance":
                    avoidance_repeat = True
                if int(row["roundish_count"]) >= MIN_REPEATS_FOR_THRESHOLD_STRUCTURING:
                    roundish_support = True

                aggregation.append({
                    "aggregation_key": str(row["BENEFICIARY_KEY"]),
                    "aggregation_type": "beneficiary",
                    "channel": str(row["CHANNEL_LABEL"]),
                    "band_name": str(row["BAND_NAME"]),
                    "band_type": str(row["BAND_TYPE"]),
                    "threshold": int(row["BAND_THRESHOLD"]) if pd.notna(row["BAND_THRESHOLD"]) else None,
                    "count": int(row["count"]),
                    "total_amount": float(row["total_amount"]),
                    "roundish_count": int(row["roundish_count"]),
                    "first_date": _safe_dt_str(row["first_date"]),
                    "last_date": _safe_dt_str(row["last_date"]),
                    "channels": ch_counts,
                    "flagged_row_ids": _collect_flagged_row_ids(subset),
                })

        # B) UNKNOWN beneficiary monthly confirmation (per channel)
        unknown_hits = hits[hits["BENEFICIARY_KEY"] == "UNKNOWN"].copy()
        unknown_patterns = []

        if not unknown_hits.empty:
            unknown_hits["YM"] = unknown_hits["DATE"].dt.to_period("M").astype(str)

            for channel, uh in unknown_hits.groupby("CHANNEL_LABEL", sort=False):
                monthly_info = _monthly_pattern_summary_unknown(uh, MIN_REPEATS_FOR_THRESHOLD_STRUCTURING)
                unknown_patterns.append({"channel": str(channel), "monthly_pattern": monthly_info})

                if not monthly_info.get("has_monthly_pattern"):
                    continue

                within = uh.groupby(["YM", "BAND_NAME"], sort=False).size().reset_index(name="count")
                qualifying_pairs_df = within[within["count"] >= MIN_REPEATS_FOR_THRESHOLD_STRUCTURING][["YM", "BAND_NAME"]].drop_duplicates()

                months_per_band = uh.groupby("BAND_NAME", sort=False)["YM"].nunique().reset_index(name="month_count")
                recurring_bands = set(months_per_band[months_per_band["month_count"] >= 2]["BAND_NAME"].tolist())

                q_unknown_parts = []

                if not qualifying_pairs_df.empty:
                    q_unknown_parts.append(
                        uh.merge(qualifying_pairs_df, on=["YM", "BAND_NAME"], how="inner")
                    )

                if recurring_bands:
                    q_unknown_parts.append(
                        uh[uh["BAND_NAME"].isin(recurring_bands)]
                    )

                if q_unknown_parts:
                    q_unknown = pd.concat(q_unknown_parts, ignore_index=False).drop_duplicates(subset=["ROW_ID"])
                else:
                    q_unknown = uh.iloc[0:0].copy()

                if q_unknown.empty:
                    continue

                gb = (
                    q_unknown.groupby(["YM", "BAND_NAME", "BAND_TYPE", "BAND_THRESHOLD"], sort=False)
                    .agg(
                        count=("BAND_NAME", "size"),
                        total_amount=("DEBIT", "sum"),
                        roundish_count=("IS_ROUNDISH", "sum"),
                        first_date=("DATE", "min"),
                        last_date=("DATE", "max"),
                    )
                    .reset_index()
                )

                for _, row in gb.iterrows():
                    subset = q_unknown[
                        (q_unknown["YM"] == row["YM"]) &
                        (q_unknown["BAND_NAME"] == row["BAND_NAME"])
                    ]
                    ch_counts = _transcode_counts(subset)

                    if str(row["BAND_TYPE"]) == "confirmed":
                        confirmed_repeat = True
                    if str(row["BAND_TYPE"]) == "avoidance":
                        avoidance_repeat = True
                    if int(row["roundish_count"]) >= MIN_REPEATS_FOR_THRESHOLD_STRUCTURING:
                        roundish_support = True

                    aggregation.append({
                        "aggregation_key": f"UNKNOWN_MONTH_{row['YM']}",
                        "aggregation_type": "unknown_monthly_pattern",
                        "channel": str(channel),
                        "band_name": str(row["BAND_NAME"]),
                        "band_type": str(row["BAND_TYPE"]),
                        "threshold": int(row["BAND_THRESHOLD"]) if pd.notna(row["BAND_THRESHOLD"]) else None,
                        "count": int(row["count"]),
                        "total_amount": float(row["total_amount"]),
                        "roundish_count": int(row["roundish_count"]),
                        "first_date": _safe_dt_str(row["first_date"]),
                        "last_date": _safe_dt_str(row["last_date"]),
                        "channels": ch_counts,
                        "flagged_row_ids": _collect_flagged_row_ids(subset),
                    })

        # C) same-day multi-party threshold fragmentation
        for (channel, date_only, band_name, band_type, band_thr), g in hits.groupby(
            ["CHANNEL_LABEL", "DATE_ONLY", "BAND_NAME", "BAND_TYPE", "BAND_THRESHOLD"],
            sort=False
        ):
            if len(g) < MIN_REPEATS_FOR_THRESHOLD_STRUCTURING:
                continue

            unique_bens = int(g["BENEFICIARY_KEY"].nunique())
            if unique_bens < min_unique_beneficiaries_multi_party:
                continue

            same_day_multi_party_aggs.append({
                "aggregation_key": f"{channel}_{date_only}_{band_name}",
                "aggregation_type": "same_day_multi_party_threshold_fragmentation",
                "channel": str(channel),
                "date": _safe_dt_str(pd.to_datetime(date_only)),
                "band_name": str(band_name),
                "band_type": str(band_type),
                "threshold": int(band_thr) if pd.notna(band_thr) else None,
                "count": int(len(g)),
                "unique_beneficiaries": unique_bens,
                "total_amount": float(g["DEBIT"].sum()),
                "roundish_count": int(g["IS_ROUNDISH"].sum()),
                "first_date": _safe_dt_str(g["DATE"].min()),
                "last_date": _safe_dt_str(g["DATE"].max()),
                "channels": _transcode_counts(g),
                "beneficiary_counts": _beneficiary_counts(g),
                "flagged_row_ids": _collect_flagged_row_ids(g),
            })

            if str(band_type) == "confirmed":
                confirmed_repeat = True
            if str(band_type) == "avoidance":
                avoidance_repeat = True
            if int(g["IS_ROUNDISH"].sum()) >= MIN_REPEATS_FOR_THRESHOLD_STRUCTURING:
                roundish_support = True

        threshold_structuring["unknown_monthly_pattern"] = unknown_patterns
        threshold_structuring["aggregation"] = aggregation
        threshold_structuring["same_day_multi_party"] = same_day_multi_party_aggs
        threshold_structuring["flags"] = {
            "confirmed_repeat": confirmed_repeat,
            "avoidance_repeat": avoidance_repeat,
            "roundish_support": roundish_support,
        }

    # ========================================================
    # MERGE OUTPUTS
    # ========================================================
    flagged_row_ids: List[int] = []

    for c in clusters:
        for rid in (c.get("flagged_row_ids") or []):
            try:
                flagged_row_ids.append(int(rid))
            except Exception:
                continue

    for a in (threshold_structuring.get("aggregation") or []):
        for rid in (a.get("flagged_row_ids") or []):
            try:
                flagged_row_ids.append(int(rid))
            except Exception:
                continue

    for a in (threshold_structuring.get("same_day_multi_party") or []):
        for rid in (a.get("flagged_row_ids") or []):
            try:
                flagged_row_ids.append(int(rid))
            except Exception:
                continue

    flagged_row_ids = sorted(set(flagged_row_ids))

    has_threshold_aggs = bool(
        (threshold_structuring.get("aggregation") or [])
        or (threshold_structuring.get("same_day_multi_party") or [])
    )

    if not clusters and not has_threshold_aggs:
        return empty

    max_cluster_score = max([float(c.get("score") or 0.0) for c in clusters], default=0.0)
    strength = max_cluster_score

    if has_threshold_aggs:
        flags = threshold_structuring.get("flags") or {}
        boost = 0.20
        if flags.get("avoidance_repeat"):
            boost += 0.05
        if flags.get("confirmed_repeat"):
            boost += 0.10
        if flags.get("roundish_support"):
            boost += 0.05
        strength = min(1.0, max(strength, 0.55) + boost)

    total_debits = float(debits["DEBIT"].sum())
    structured_total = float(sum(float(c.get("total_amount") or 0.0) for c in clusters))

    threshold_total = 0.0
    for a in (threshold_structuring.get("aggregation") or []):
        threshold_total += float(a.get("total_amount") or 0.0)
    for a in (threshold_structuring.get("same_day_multi_party") or []):
        threshold_total += float(a.get("total_amount") or 0.0)

    structured_ratio = structured_total / total_debits if total_debits > 0 else None

    return {
        "triggered": True,
        "strength": float(min(1.0, strength)),
        "clusters": clusters,
        "flagged_row_ids": flagged_row_ids,
        "raw": {
            "total_debits": total_debits,
            "structured_total_amount": structured_total,
            "threshold_structured_total_amount": threshold_total,
            "structured_ratio": structured_ratio,
            "cluster_count": int(len(clusters)),
            "cluster_type_counts": pd.Series([c.get("pattern_subtype") for c in clusters]).value_counts().to_dict() if clusters else {},
            "threshold_structuring": threshold_structuring,
        },
        "pattern": "structured_payments",
    }