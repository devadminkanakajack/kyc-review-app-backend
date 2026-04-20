from typing import Dict, Any, List, Optional, Tuple
import os
from datetime import timedelta

import numpy as np
import pandas as pd


"""
structured_deposits.py
=======================

Optimized structured deposit detector.

Goals of this rewrite:
- Keep the original detector logic and output shape
- Keep helper functions in the file
- Reduce freezing on large CSVs
- Avoid repeated full-data rescans inside nested loops
- Precompute expensive flags once
- Replace row-wise date filtering with index-based sliding windows

Returns:
{
    "triggered": bool,
    "strength": 0–1,
    "indicators": [...],
    "flagged_row_ids": [...],
    "raw": {...},
    "pattern": "structured_deposits" | None
}
"""


# -----------------------------------------------------------
# CONFIG
# -----------------------------------------------------------

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

BOUNDARY_BANDS = [
    ("<=5k_boundary", 4950.00, 5000.00, 5000),
    ("<=20k_boundary", 19950.00, 20000.00, 20000),
    ("<=50k_boundary", 49950.00, 50000.00, 50000),
    ("<=500k_boundary", 499950.00, 500000.00, 500000),
    ("<=1m_boundary", 999950.00, 1000000.00, 1000000),
]

MIN_REPEATS_FOR_STRUCTURING = 2

CLASSIC_WINDOW_DAYS = 3
CLASSIC_MIN_SUB_THRESHOLD_COUNT = 2
CLASSIC_MIN_NEAR_BAND_COUNT = 2
CLASSIC_MIN_SUB_THRESHOLD_SHARE = 0.80

SAME_DAY_MIN_TXNS = 3

ROLLING_WINDOW_DAYS = 7
ROLLING_MIN_TXNS = 2

DEPOSIT_REPEAT_BIN_SIZE = 0.0
MIN_REPEATS_SAME_AMOUNT_SUPPORT = 3

ICBA_NUMBER_CODES_CSV = os.getenv("ICBA_NUMBER_CODES_CSV", "Number Codes.csv")
ICBA_ALLOWED_ACCOUNTS = {"CURRENT ACCOUNT", "SAVINGS ACCOUNT"}

CHANNEL_STRUCT_THRESHOLD = 5000.0
CHANNEL_STRUCT_MIN_AMOUNT = 3000.0
CHANNEL_STRUCT_MIN_TXNS_PER_MONTH = 3
CHANNEL_STRUCT_MIN_QUALIFYING_MONTHS = 3
CHANNEL_STRUCT_MIN_GAP2_TRANSITIONS = 2
MONTHLY_MAX_GAP_DAYS = 7
MONTHLY_MAX_DISTINCT_DAYS = 3
MONTHLY_MIN_TXNS_WITHIN_7D = 3

LADDER_WINDOW_DAYS = 7
LADDER_MIN_DISTINCT_AMOUNTS = 3
LADDER_MIN_TOTAL_TO_THRESHOLD = True
LADDER_MIN_RECURRING_MONTHS = 2

IDENTITY_CONCENTRATION_WINDOW_DAYS = 30
IDENTITY_CONCENTRATION_MIN_TXNS = 3
IDENTITY_CONCENTRATION_MIN_MONTHS = 2

# Guardrail to prevent pathological deep scans per group.
# Keeps app responsive on very large statements.
MAX_GROUP_ROWS_FOR_DEEP_SCAN = 12000

LADDER_THRESHOLDS = [
    (5000, 4950.00, 5000.00),
    (20000, 19950.00, 20000.00),
    (50000, 49950.00, 50000.00),
    (500000, 499950.00, 500000.00),
    (1000000, 999950.00, 1000000.00),
]


# -----------------------------------------------------------
# Helpers
# -----------------------------------------------------------

def _empty() -> Dict[str, Any]:
    return {
        "triggered": False,
        "strength": 0.0,
        "indicators": [],
        "flagged_row_ids": [],
        "raw": {},
        "pattern": None,
    }


def _load_allowed_transcodes_from_number_codes() -> set:
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


def _safe_transcode(df: pd.DataFrame) -> pd.Series:
    if "TRANSCODE" in df.columns:
        s = df["TRANSCODE"].astype(str).replace({"None": "", "nan": "", "NaN": ""})
        s = s.map(lambda x: x.strip() if isinstance(x, str) else "")
        return s.replace("", "UNKNOWN")
    return pd.Series(["UNKNOWN"] * len(df), index=df.index)


def _safe_channel(df: pd.DataFrame) -> pd.Series:
    if "DESCRIPTION" in df.columns:
        s = df["DESCRIPTION"].astype(str).replace({"None": "", "nan": "", "NaN": ""})
        s = s.map(lambda x: x.strip() if isinstance(x, str) else "")
        return s.replace("", "UNKNOWN")
    return _safe_transcode(df)


def _is_roundish(amount: float) -> Dict[str, bool]:
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


def _assign_band(amount: float) -> Optional[Tuple[str, float, float, int, str]]:
    if amount is None or not np.isfinite(amount) or amount <= 0:
        return None

    for name, lo, hi, thr in CONFIRMED_BANDS:
        if lo <= amount <= hi:
            return (name, lo, hi, thr, "confirmed")

    for name, lo, hi, thr in AVOIDANCE_BANDS:
        if lo <= amount <= hi:
            return (name, lo, hi, thr, "avoidance")

    return None


def _assign_boundary_band(amount: float) -> Optional[Tuple[str, float, float, int]]:
    if amount is None or not np.isfinite(amount) or amount <= 0:
        return None

    for name, lo, hi, thr in BOUNDARY_BANDS:
        if lo <= amount <= hi:
            return (name, lo, hi, thr)

    return None


def _bucket_amount(x: float, bin_size: float) -> float:
    if bin_size is None or bin_size <= 0:
        return float(round(x, 2))
    return float(round((x / bin_size)) * bin_size)


def _period_to_int(p: str) -> int:
    y, m = p.split("-")
    return int(y) * 12 + int(m)


def _gap2_transitions(months: List[str]) -> int:
    if not months:
        return 0
    ints = sorted(_period_to_int(x) for x in months)
    gaps = [ints[i] - ints[i - 1] for i in range(1, len(ints))]
    return sum(1 for g in gaps if g == 2)


def _max_gap_days(dates: pd.Series) -> Optional[int]:
    vals = sorted(pd.to_datetime(dates, errors="coerce").dropna().tolist())
    if len(vals) < 2:
        return None
    gaps = [(vals[i] - vals[i - 1]).days for i in range(1, len(vals))]
    return max(gaps) if gaps else None


def _txns_within_days(dates: pd.Series, window_days: int = 7) -> int:
    vals = sorted(pd.to_datetime(dates, errors="coerce").dropna().tolist())
    if not vals:
        return 0

    best = 1
    left = 0
    for right in range(len(vals)):
        while left <= right and (vals[right] - vals[left]).days > window_days:
            left += 1
        best = max(best, right - left + 1)
    return int(best)


def _merge_overlapping_hits(
    hits: List[Dict[str, Any]],
    start_key: str,
    end_key: str,
    group_keys: List[str],
) -> List[Dict[str, Any]]:
    if not hits:
        return []

    def _to_ts(v):
        return pd.to_datetime(v, errors="coerce")

    ordered = sorted(
        [h.copy() for h in hits],
        key=lambda h: tuple(h.get(k) for k in group_keys) + (
            _to_ts(h.get(start_key)),
            _to_ts(h.get(end_key)),
        ),
    )

    merged: List[Dict[str, Any]] = []
    for hit in ordered:
        s = _to_ts(hit.get(start_key))
        e = _to_ts(hit.get(end_key))
        if pd.isna(s) or pd.isna(e):
            merged.append(hit)
            continue

        if not merged:
            merged.append(hit)
            continue

        prev = merged[-1]
        same_group = all(prev.get(k) == hit.get(k) for k in group_keys)
        prev_s = _to_ts(prev.get(start_key))
        prev_e = _to_ts(prev.get(end_key))

        if same_group and pd.notna(prev_s) and pd.notna(prev_e) and s <= prev_e:
            prev[start_key] = str(min(prev_s, s).date())
            prev[end_key] = str(max(prev_e, e).date())

            for k in [
                "count",
                "total",
                "total_amount",
                "near_count",
                "sub_threshold_count",
                "near_band_count",
            ]:
                if k in prev or k in hit:
                    if k.endswith("count"):
                        prev[k] = max(int(prev.get(k, 0) or 0), int(hit.get(k, 0) or 0))
                    else:
                        prev[k] = max(float(prev.get(k, 0) or 0), float(hit.get(k, 0) or 0))

            if "distinct_amounts" in prev or "distinct_amounts" in hit:
                prev["distinct_amounts"] = max(
                    int(prev.get("distinct_amounts", 0) or 0),
                    int(hit.get("distinct_amounts", 0) or 0),
                )

            if "transcodes" in prev or "transcodes" in hit:
                tc = dict(prev.get("transcodes", {}) or {})
                for kk, vv in (hit.get("transcodes", {}) or {}).items():
                    tc[kk] = max(tc.get(kk, 0), vv)
                prev["transcodes"] = tc

            if "identities" in prev or "identities" in hit:
                ids = set(prev.get("identities", []) or [])
                ids.update(hit.get("identities", []) or [])
                prev["identities"] = sorted(ids)[:20]
        else:
            merged.append(hit)

    return merged


def _monthly_pattern_summary(unknown_hits: pd.DataFrame) -> Dict[str, Any]:
    if unknown_hits.empty:
        return {"has_monthly_pattern": False, "reason": None, "support": {}}

    g = unknown_hits.copy()
    if "YM" not in g.columns:
        g["YM"] = g["DATE"].dt.to_period("M").astype(str)

    monthly_density = (
        g.groupby(["YM", "BAND_NAME"], dropna=False)
        .agg(
            count=("BAND_NAME", "size"),
            first_date=("DATE", "min"),
            last_date=("DATE", "max"),
            distinct_days=("DATE", lambda s: s.dt.date.nunique()),
            max_gap_days=("DATE", _max_gap_days),
            max_txns_within_7d=("DATE", lambda s: _txns_within_days(s, 7)),
        )
        .reset_index()
        .sort_values(["count", "YM"], ascending=[False, True])
    )

    hit_within = monthly_density[
        (monthly_density["count"] >= MIN_REPEATS_FOR_STRUCTURING)
        & (monthly_density["distinct_days"].fillna(0) <= MONTHLY_MAX_DISTINCT_DAYS)
        & (monthly_density["max_gap_days"].fillna(0) <= MONTHLY_MAX_GAP_DAYS)
        & (monthly_density["max_txns_within_7d"].fillna(0) >= MIN_REPEATS_FOR_STRUCTURING)
    ]

    if not hit_within.empty:
        top = hit_within.iloc[0].to_dict()
        return {
            "has_monthly_pattern": True,
            "reason": "Repeated near-threshold deposits within the same month for UNKNOWN identity, with sufficient density.",
            "support": {
                "top_month_band": top,
                "within_month": monthly_density.to_dict(orient="records")[:30],
            },
        }

    months_per_band = (
        g.groupby("BAND_NAME", dropna=False)["YM"]
        .nunique()
        .reset_index(name="month_count")
        .sort_values("month_count", ascending=False)
    )
    hit_across = months_per_band[months_per_band["month_count"] >= 2]
    if not hit_across.empty:
        top = hit_across.iloc[0].to_dict()
        return {
            "has_monthly_pattern": True,
            "reason": "Near-threshold deposit band recurs across multiple months for UNKNOWN identity.",
            "support": {
                "top_band": top,
                "months_per_band": months_per_band.to_dict(orient="records"),
                "within_month": monthly_density.to_dict(orient="records")[:30],
            },
        }

    return {
        "has_monthly_pattern": False,
        "reason": None,
        "support": {"within_month": monthly_density.to_dict(orient="records")[:30]},
    }


def _iter_date_windows(chdf: pd.DataFrame, window_days: int):
    """
    Yield (left, right) window boundaries for chdf already sorted by DATE.
    right is exclusive.
    """
    dates = pd.to_datetime(chdf["DATE"], errors="coerce").tolist()
    n = len(dates)
    right = 0

    for left in range(n):
        start_date = dates[left]
        if pd.isna(start_date):
            continue

        if right < left:
            right = left

        while right < n and pd.notna(dates[right]) and (dates[right] - start_date).days <= window_days:
            right += 1

        yield left, right


def _window_summary(win: pd.DataFrame, threshold: float) -> Dict[str, Any]:
    credits = win["CREDIT"]
    total = float(credits.sum())
    sub_mask = credits < threshold

    sub_threshold_count = int(sub_mask.sum())
    near_band_count = int(win["_IS_NEAR_OR_BOUNDARY_"].sum())
    sub_threshold_total = float(credits[sub_mask].sum())
    sub_threshold_share = (sub_threshold_total / total) if total > 0 else 0.0

    return {
        "total": total,
        "count": int(len(win)),
        "sub_threshold_count": sub_threshold_count,
        "near_band_count": near_band_count,
        "sub_threshold_share": round(sub_threshold_share, 4),
        "distinct_amounts": int(win["_CREDIT_R2_"].nunique()),
    }


def _should_deep_scan(group_df: pd.DataFrame) -> bool:
    return len(group_df) <= MAX_GROUP_ROWS_FOR_DEEP_SCAN


def _precompute_flags(work: pd.DataFrame) -> pd.DataFrame:
    work = work.copy()
    work = work.sort_values(["_CHANNEL_", "DATE", "_ROW_ID_"]).reset_index(drop=True)

    work["_DATE_DAY_"] = work["DATE"].dt.floor("D")
    work["YM"] = work["DATE"].dt.to_period("M").astype(str)
    work["_CREDIT_R2_"] = work["CREDIT"].round(2)

    band_info = work["CREDIT"].map(_assign_band)
    work["BAND_NAME"] = band_info.map(lambda x: x[0] if x else None)
    work["BAND_TYPE"] = band_info.map(lambda x: x[4] if x else None)
    work["BAND_THRESHOLD"] = band_info.map(lambda x: x[3] if x else None)

    boundary_info = work["CREDIT"].map(_assign_boundary_band)
    work["BOUNDARY_NAME"] = boundary_info.map(lambda x: x[0] if x else None)
    work["BOUNDARY_THRESHOLD"] = boundary_info.map(lambda x: x[3] if x else None)

    work["_IS_NEAR_OR_BOUNDARY_"] = work["BAND_NAME"].notna() | work["BOUNDARY_NAME"].notna()

    rf = work["CREDIT"].map(_is_roundish)
    work["IS_ROUNDISH"] = rf.map(lambda d: d["is_roundish"])
    work["ROUND_CENTS"] = rf.map(lambda d: d["round_cents"])
    work["KINA_MULTIPLE"] = rf.map(lambda d: d["kina_multiple"])

    return work


# -----------------------------------------------------------
# MAIN DETECTOR
# -----------------------------------------------------------

def detect_structured_deposits(
    df: pd.DataFrame,
    analysis_cache: Optional[Dict[str, Any]] = None,
    prior_results: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if df is None or len(df) == 0:
        return _empty()

    analysis_cache = analysis_cache or {}
    prior_results = prior_results or {}

    base = analysis_cache.get("credits_df") if isinstance(analysis_cache.get("credits_df"), pd.DataFrame) else df
    work = base.copy()

    if "CREDIT" not in work.columns or "DATE" not in work.columns:
        return _empty()

    work["DATE"] = pd.to_datetime(work["DATE"], errors="coerce")
    work["CREDIT"] = pd.to_numeric(work["CREDIT"], errors="coerce").fillna(0.0)

    work = work[(work["CREDIT"] > 0) & (work["DATE"].notna())].copy()
    if work.empty:
        return _empty()

    if "TRANSCODE" not in work.columns:
        return _empty()

    allowed_codes = _load_allowed_transcodes_from_number_codes()
    if allowed_codes:
        work["TRANSCODE"] = (
            work["TRANSCODE"]
            .astype(str)
            .replace({"None": "", "nan": "", "NaN": ""})
            .map(lambda x: x.strip() if isinstance(x, str) else "")
            .replace("", "UNKNOWN")
        )
        work = work[work["TRANSCODE"].isin(allowed_codes)].copy()
        if work.empty:
            return _empty()

    work["_ROW_ID_"] = work.index.astype(str)

    if "IDENTITY" in work.columns:
        ident = (
            work["IDENTITY"]
            .astype(str)
            .replace({"None": "", "nan": "", "NaN": ""})
            .map(lambda x: x.strip() if isinstance(x, str) else "")
        )
        work["_IDENTITY_"] = ident.replace("", "UNKNOWN")
    else:
        work["_IDENTITY_"] = "UNKNOWN"

    work["_TRANSCODE_"] = _safe_transcode(work)
    work["_CHANNEL_"] = _safe_channel(work)
    work = _precompute_flags(work)

    flagged_rows = set()
    indicators: List[str] = []
    raw: Dict[str, Any] = {}

    recurring_identities: set[str] = set()
    recurrence_out = prior_results.get("recurrence") if isinstance(prior_results, dict) else None
    if isinstance(recurrence_out, dict):
        for key in ("identity_clusters", "clusters", "identity_summary"):
            val = recurrence_out.get(key) or (
                (recurrence_out.get("raw", {}) if isinstance(recurrence_out.get("raw"), dict) else {}).get(key)
            )
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        ident = item.get("identity") or item.get("identifier") or item.get("party")
                        if ident:
                            recurring_identities.add(str(ident).strip().upper())
    work["__RECURRING_ID__"] = work["_IDENTITY_"].astype(str).str.upper().isin(recurring_identities)

    # =========================================================
    # 1) CLASSIC STRUCTURING
    # =========================================================
    classic_hits: List[Dict[str, Any]] = []

    for channel, chdf in work.groupby("_CHANNEL_", sort=False):
        chdf = chdf.sort_values("DATE")
        if chdf.empty or not _should_deep_scan(chdf):
            continue

        for left, right in _iter_date_windows(chdf, CLASSIC_WINDOW_DAYS):
            win = chdf.iloc[left:right]
            if len(win) < 2:
                continue

            total_all = float(win["CREDIT"].sum())
            if total_all < min(REPORTING_THRESHOLDS):
                continue

            start_date = win.iloc[0]["DATE"]
            window_end = start_date + timedelta(days=CLASSIC_WINDOW_DAYS)

            for threshold in REPORTING_THRESHOLDS:
                if total_all < threshold:
                    continue

                s = _window_summary(win, threshold)
                qualifies = (
                    s["count"] >= 3
                    or s["sub_threshold_count"] >= CLASSIC_MIN_SUB_THRESHOLD_COUNT
                    or s["near_band_count"] >= CLASSIC_MIN_NEAR_BAND_COUNT
                    or s["sub_threshold_share"] >= CLASSIC_MIN_SUB_THRESHOLD_SHARE
                )
                if not qualifies:
                    continue

                classic_hits.append({
                    "channel": channel,
                    "start_date": str(start_date.date()),
                    "window_end": str(window_end.date()),
                    "window_days": CLASSIC_WINDOW_DAYS,
                    "threshold": threshold,
                    "total": s["total"],
                    "count": s["count"],
                    "sub_threshold_count": s["sub_threshold_count"],
                    "near_band_count": s["near_band_count"],
                    "sub_threshold_share": s["sub_threshold_share"],
                    "distinct_amounts": s["distinct_amounts"],
                    "transcodes": win["_TRANSCODE_"].value_counts().to_dict(),
                })
                flagged_rows.update(win["_ROW_ID_"].tolist())

    classic_hits = _merge_overlapping_hits(classic_hits, "start_date", "window_end", ["channel", "threshold"])
    raw["classic_structuring"] = classic_hits
    if classic_hits:
        indicators.append(
            "Channel-specific deposit build-up detected: multiple inflows aggregated within a short time window reaching reporting thresholds, with structuring-like sub-threshold composition."
        )

    # =========================================================
    # 1A) SAME-DAY BURST STRUCTURING
    # =========================================================
    same_day_hits: List[Dict[str, Any]] = []

    known_same_day = work[work["_IDENTITY_"] != "UNKNOWN"]
    if not known_same_day.empty:
        for (identity, channel, day), g in known_same_day.groupby(
            ["_IDENTITY_", "_CHANNEL_", "_DATE_DAY_"], sort=False
        ):
            g = g.sort_values("DATE")
            if len(g) < SAME_DAY_MIN_TXNS:
                continue

            for threshold in REPORTING_THRESHOLDS:
                sub = g[g["CREDIT"] < threshold]
                if len(sub) < SAME_DAY_MIN_TXNS:
                    continue

                total = float(sub["CREDIT"].sum())
                if total < threshold:
                    continue

                same_day_hits.append({
                    "identity": identity,
                    "channel": channel,
                    "date": str(pd.to_datetime(day).date()),
                    "threshold": threshold,
                    "count": int(len(sub)),
                    "total": total,
                    "distinct_amounts": int(sub["_CREDIT_R2_"].nunique()),
                    "transcodes": sub["_TRANSCODE_"].value_counts().to_dict(),
                    "aggregation_type": "identity_channel",
                })
                flagged_rows.update(sub["_ROW_ID_"].tolist())

    unknown_same_day = work[work["_IDENTITY_"] == "UNKNOWN"]
    if not unknown_same_day.empty:
        for (channel, day), g in unknown_same_day.groupby(["_CHANNEL_", "_DATE_DAY_"], sort=False):
            g = g.sort_values("DATE")
            if len(g) < SAME_DAY_MIN_TXNS:
                continue

            for threshold in REPORTING_THRESHOLDS:
                sub = g[g["CREDIT"] < threshold]
                if len(sub) < SAME_DAY_MIN_TXNS:
                    continue

                total = float(sub["CREDIT"].sum())
                if total < threshold:
                    continue

                same_day_hits.append({
                    "channel": channel,
                    "date": str(pd.to_datetime(day).date()),
                    "threshold": threshold,
                    "count": int(len(sub)),
                    "total": total,
                    "distinct_amounts": int(sub["_CREDIT_R2_"].nunique()),
                    "transcodes": sub["_TRANSCODE_"].value_counts().to_dict(),
                    "aggregation_type": "unknown_channel_fallback",
                })
                flagged_rows.update(sub["_ROW_ID_"].tolist())

    raw["same_day_burst_structuring"] = same_day_hits
    if same_day_hits:
        indicators.append(
            "Same-day burst structuring detected: multiple sub-threshold inflows in a single day within one channel aggregated to reporting thresholds."
        )

    # =========================================================
    # 1B) CHANNEL-SPECIFIC MONTHLY THRESHOLD AVOIDANCE (<K5,000)
    # =========================================================
    channel_monthly_hits: List[Dict[str, Any]] = []

    for channel, chdf in work.groupby("_CHANNEL_", sort=False):
        sub = chdf[(chdf["CREDIT"] >= CHANNEL_STRUCT_MIN_AMOUNT) & (chdf["CREDIT"] < CHANNEL_STRUCT_THRESHOLD)]
        if sub.empty:
            continue

        monthly = (
            sub.groupby("YM", sort=True)
            .agg(
                count=("CREDIT", "size"),
                total=("CREDIT", "sum"),
                first_date=("DATE", "min"),
                last_date=("DATE", "max"),
                distinct_days=("DATE", lambda s: s.dt.date.nunique()),
                max_gap_days=("DATE", _max_gap_days),
                max_txns_within_7d=("DATE", lambda s: _txns_within_days(s, 7)),
            )
            .reset_index()
            .sort_values("YM")
        )

        qualifying = monthly[
            (monthly["count"] >= CHANNEL_STRUCT_MIN_TXNS_PER_MONTH)
            & (monthly["total"] >= CHANNEL_STRUCT_THRESHOLD)
            & (monthly["max_gap_days"].fillna(0) <= MONTHLY_MAX_GAP_DAYS)
            & (monthly["distinct_days"].fillna(0) <= MONTHLY_MAX_DISTINCT_DAYS)
            & (monthly["max_txns_within_7d"].fillna(0) >= MONTHLY_MIN_TXNS_WITHIN_7D)
        ].copy()

        q_months = qualifying["YM"].tolist()
        qualified = len(q_months) >= CHANNEL_STRUCT_MIN_QUALIFYING_MONTHS
        gap2 = _gap2_transitions(q_months)
        every_other = gap2 >= CHANNEL_STRUCT_MIN_GAP2_TRANSITIONS

        channel_monthly_hits.append({
            "channel": channel,
            "qualified": bool(qualified),
            "recurs_every_other_month": bool(every_other),
            "gap2_transitions": int(gap2),
            "qualifying_months": q_months,
            "monthly_summary": monthly.to_dict(orient="records"),
            "transcodes": sub["_TRANSCODE_"].value_counts().to_dict(),
        })

        if qualified and q_months:
            subq = sub[sub["YM"].isin(q_months)]
            flagged_rows.update(subq["_ROW_ID_"].tolist())

    raw["channel_monthly_threshold_avoidance"] = channel_monthly_hits
    if any(x.get("qualified") for x in channel_monthly_hits):
        indicators.append(
            "Threshold-avoidance structuring detected within single inflow channels: repeated sub-K5,000 inflows clustered by month with monthly totals reaching K5,000 and sufficient transaction density."
        )

    # =========================================================
    # 1C) ROLLING WINDOW SUB-THRESHOLD CLUSTERING
    # =========================================================
    rolling_hits: List[Dict[str, Any]] = []

    def _scan_rolling(group_df: pd.DataFrame, aggregation_type: str, identity: Optional[str] = None):
        out: List[Dict[str, Any]] = []
        if group_df.empty or not _should_deep_scan(group_df):
            return out

        group_df = group_df.sort_values("DATE")
        for left, right in _iter_date_windows(group_df, ROLLING_WINDOW_DAYS):
            win = group_df.iloc[left:right]
            if len(win) < ROLLING_MIN_TXNS:
                continue

            start_date = win.iloc[0]["DATE"]
            end_date = start_date + timedelta(days=ROLLING_WINDOW_DAYS)

            for threshold in REPORTING_THRESHOLDS:
                sub = win[win["CREDIT"] < threshold]
                if len(sub) < ROLLING_MIN_TXNS:
                    continue

                near_count = int(sub["_IS_NEAR_OR_BOUNDARY_"].sum())
                total = float(sub["CREDIT"].sum())

                if total >= threshold and (near_count >= 2 or len(sub) >= 3):
                    hit = {
                        "channel": win.iloc[0]["_CHANNEL_"],
                        "start_date": str(start_date.date()),
                        "end_date": str(end_date.date()),
                        "window_days": ROLLING_WINDOW_DAYS,
                        "threshold": threshold,
                        "count": int(len(sub)),
                        "near_count": near_count,
                        "total": total,
                        "distinct_amounts": int(sub["_CREDIT_R2_"].nunique()),
                        "transcodes": sub["_TRANSCODE_"].value_counts().to_dict(),
                        "aggregation_type": aggregation_type,
                    }
                    if identity is not None:
                        hit["identity"] = identity
                    out.append(hit)
                    flagged_rows.update(sub["_ROW_ID_"].tolist())
        return out

    known_rolling = work[work["_IDENTITY_"] != "UNKNOWN"]
    for (identity, channel), chdf in known_rolling.groupby(["_IDENTITY_", "_CHANNEL_"], sort=False):
        rolling_hits.extend(_scan_rolling(chdf, "identity_channel", identity))

    unknown_rolling = work[work["_IDENTITY_"] == "UNKNOWN"]
    for channel, chdf in unknown_rolling.groupby("_CHANNEL_", sort=False):
        rolling_hits.extend(_scan_rolling(chdf, "unknown_channel_fallback"))

    rolling_hits = _merge_overlapping_hits(
        rolling_hits,
        "start_date",
        "end_date",
        ["aggregation_type", "channel", "identity", "threshold"],
    )
    raw["rolling_window_subthreshold_clusters"] = rolling_hits
    if rolling_hits:
        indicators.append(
            "Rolling-window sub-threshold clustering detected: repeated sub-threshold deposits within 7 days aggregated to reporting thresholds in a single channel."
        )

    # =========================================================
    # 1D) EXPLICIT STAIRCASE / LADDERED THRESHOLD AVOIDANCE
    # =========================================================
    ladder_hits: List[Dict[str, Any]] = []

    def _scan_ladder(group_df: pd.DataFrame, aggregation_type: str, identity: Optional[str] = None):
        out: List[Dict[str, Any]] = []
        if group_df.empty or not _should_deep_scan(group_df):
            return out

        group_df = group_df.sort_values("DATE")
        for left, right in _iter_date_windows(group_df, LADDER_WINDOW_DAYS):
            win = group_df.iloc[left:right]
            if len(win) < MIN_REPEATS_FOR_STRUCTURING:
                continue

            start_date = win.iloc[0]["DATE"]
            end_date = start_date + timedelta(days=LADDER_WINDOW_DAYS)

            for threshold, lo, hi in LADDER_THRESHOLDS:
                band_subset = win[(win["CREDIT"] >= lo) & (win["CREDIT"] <= hi)]
                if band_subset.empty:
                    continue

                distinct_amounts = int(band_subset["_CREDIT_R2_"].nunique())
                total_amount = float(band_subset["CREDIT"].sum())

                if distinct_amounts < LADDER_MIN_DISTINCT_AMOUNTS:
                    continue
                if LADDER_MIN_TOTAL_TO_THRESHOLD and total_amount < threshold:
                    continue

                hit = {
                    "channel": win.iloc[0]["_CHANNEL_"],
                    "start_date": str(start_date.date()),
                    "end_date": str(end_date.date()),
                    "window_days": LADDER_WINDOW_DAYS,
                    "threshold": threshold,
                    "count": int(len(band_subset)),
                    "distinct_amounts": distinct_amounts,
                    "total_amount": total_amount,
                    "transcodes": band_subset["_TRANSCODE_"].value_counts().to_dict(),
                    "aggregation_type": aggregation_type,
                }
                if identity is not None:
                    hit["identity"] = identity

                out.append(hit)
                flagged_rows.update(band_subset["_ROW_ID_"].tolist())
        return out

    known_ladder_short = work[work["_IDENTITY_"] != "UNKNOWN"]
    for (identity, channel), chdf in known_ladder_short.groupby(["_IDENTITY_", "_CHANNEL_"], sort=False):
        ladder_hits.extend(_scan_ladder(chdf, "identity_channel", identity))

    unknown_ladder_short = work[work["_IDENTITY_"] == "UNKNOWN"]
    for channel, chdf in unknown_ladder_short.groupby("_CHANNEL_", sort=False):
        ladder_hits.extend(_scan_ladder(chdf, "unknown_channel_fallback"))

    ladder_hits = _merge_overlapping_hits(
        ladder_hits,
        "start_date",
        "end_date",
        ["aggregation_type", "channel", "identity", "threshold"],
    )

    ladder_monthly_recurrence: List[Dict[str, Any]] = []

    known_ladder = work[work["_IDENTITY_"] != "UNKNOWN"]
    for (identity, channel), tmp in known_ladder.groupby(["_IDENTITY_", "_CHANNEL_"], sort=False):
        for threshold, lo, hi in LADDER_THRESHOLDS:
            near = tmp[(tmp["CREDIT"] >= lo) & (tmp["CREDIT"] <= hi)]
            if near.empty:
                continue

            g = (
                near.groupby("YM", sort=True)
                .agg(
                    count=("CREDIT", "size"),
                    distinct_amounts=("CREDIT", lambda s: s.round(2).nunique()),
                    total_amount=("CREDIT", "sum"),
                    distinct_days=("DATE", lambda s: s.dt.date.nunique()),
                    max_gap_days=("DATE", _max_gap_days),
                )
                .reset_index()
            )

            q = g[
                (g["distinct_amounts"] >= LADDER_MIN_DISTINCT_AMOUNTS)
                & (g["distinct_days"] <= MONTHLY_MAX_DISTINCT_DAYS)
                & (g["max_gap_days"].fillna(0) <= MONTHLY_MAX_GAP_DAYS)
            ].copy()

            if q["YM"].nunique() >= LADDER_MIN_RECURRING_MONTHS:
                ladder_monthly_recurrence.append({
                    "identity": identity,
                    "channel": channel,
                    "threshold": threshold,
                    "months": q["YM"].tolist(),
                    "monthly_summary": q.to_dict(orient="records"),
                    "aggregation_type": "identity_channel",
                })
                flagged_rows.update(near[near["YM"].isin(q["YM"].tolist())]["_ROW_ID_"].tolist())

    unknown_ladder = work[work["_IDENTITY_"] == "UNKNOWN"]
    for channel, tmp in unknown_ladder.groupby("_CHANNEL_", sort=False):
        for threshold, lo, hi in LADDER_THRESHOLDS:
            near = tmp[(tmp["CREDIT"] >= lo) & (tmp["CREDIT"] <= hi)]
            if near.empty:
                continue

            g = (
                near.groupby("YM", sort=True)
                .agg(
                    count=("CREDIT", "size"),
                    distinct_amounts=("CREDIT", lambda s: s.round(2).nunique()),
                    total_amount=("CREDIT", "sum"),
                    distinct_days=("DATE", lambda s: s.dt.date.nunique()),
                    max_gap_days=("DATE", _max_gap_days),
                )
                .reset_index()
            )

            q = g[
                (g["distinct_amounts"] >= LADDER_MIN_DISTINCT_AMOUNTS)
                & (g["distinct_days"] <= MONTHLY_MAX_DISTINCT_DAYS)
                & (g["max_gap_days"].fillna(0) <= MONTHLY_MAX_GAP_DAYS)
            ].copy()

            if q["YM"].nunique() >= LADDER_MIN_RECURRING_MONTHS:
                ladder_monthly_recurrence.append({
                    "channel": channel,
                    "threshold": threshold,
                    "months": q["YM"].tolist(),
                    "monthly_summary": q.to_dict(orient="records"),
                    "aggregation_type": "unknown_channel_fallback",
                })
                flagged_rows.update(near[near["YM"].isin(q["YM"].tolist())]["_ROW_ID_"].tolist())

    raw["laddered_threshold_avoidance"] = ladder_hits
    raw["laddered_threshold_monthly_recurrence"] = ladder_monthly_recurrence
    if ladder_hits or ladder_monthly_recurrence:
        indicators.append(
            "Explicit staircase or laddered threshold-avoidance behaviour detected: multiple distinct near-threshold amounts within the same channel clustered in short windows or recurring across months."
        )

    # =========================================================
    # 1E) EXPLICIT IDENTITY CONCENTRATION WITHIN CHANNEL
    # =========================================================
    identity_concentration_hits: List[Dict[str, Any]] = []
    known_work = work[work["_IDENTITY_"] != "UNKNOWN"]
    if not known_work.empty:
        for (identity, channel), g in known_work.groupby(["_IDENTITY_", "_CHANNEL_"], sort=False):
            g = g.sort_values("DATE")
            if len(g) < IDENTITY_CONCENTRATION_MIN_TXNS:
                continue

            near_or_sub = g[
                (g["CREDIT"] < CHANNEL_STRUCT_THRESHOLD) | (g["_IS_NEAR_OR_BOUNDARY_"])
            ]
            if len(near_or_sub) < IDENTITY_CONCENTRATION_MIN_TXNS:
                continue

            recurring_months = int(near_or_sub["YM"].nunique())
            if recurring_months < IDENTITY_CONCENTRATION_MIN_MONTHS:
                continue

            max_gap = _max_gap_days(near_or_sub["DATE"])
            max_txns_7d = _txns_within_days(near_or_sub["DATE"], 7)
            max_txns_30d = _txns_within_days(near_or_sub["DATE"], IDENTITY_CONCENTRATION_WINDOW_DAYS)
            distinct_days = int(near_or_sub["DATE"].dt.date.nunique())

            if max_txns_30d < IDENTITY_CONCENTRATION_MIN_TXNS:
                continue

            identity_concentration_hits.append({
                "identity": identity,
                "channel": channel,
                "count": int(len(near_or_sub)),
                "total_amount": float(near_or_sub["CREDIT"].sum()),
                "recurring_months": recurring_months,
                "first_date": str(near_or_sub["DATE"].min().date()),
                "last_date": str(near_or_sub["DATE"].max().date()),
                "max_gap_days": max_gap,
                "max_txns_within_7d": max_txns_7d,
                "max_txns_within_30d": max_txns_30d,
                "distinct_days": distinct_days,
                "distinct_amounts": int(near_or_sub["_CREDIT_R2_"].nunique()),
                "transcodes": near_or_sub["_TRANSCODE_"].value_counts().to_dict(),
            })
            flagged_rows.update(near_or_sub["_ROW_ID_"].tolist())

    raw["identity_concentration_within_channel"] = identity_concentration_hits
    if identity_concentration_hits:
        indicators.append(
            "Identity concentration within channel detected: the same depositor repeatedly made near-threshold or sub-threshold deposits in the same inflow channel across recurring periods, including concentrated activity within 30-day windows."
        )

    # =========================================================
    # 2) SINGLE THRESHOLD STRUCTURING
    # =========================================================
    band_hits = work[work["BAND_NAME"].notna()].copy()

    if not band_hits.empty:
        raw["single_threshold_structuring_hits"] = band_hits[
            [
                "_ROW_ID_",
                "DATE",
                "_IDENTITY_",
                "_CHANNEL_",
                "_TRANSCODE_",
                "CREDIT",
                "BAND_NAME",
                "BAND_TYPE",
                "BAND_THRESHOLD",
                "IS_ROUNDISH",
                "ROUND_CENTS",
                "KINA_MULTIPLE",
            ]
        ].to_dict(orient="records")
    else:
        raw["single_threshold_structuring_hits"] = []

    aggregation: List[Dict[str, Any]] = []
    confirmed_repeat = False
    avoidance_repeat = False
    roundish_support = False
    channel_support_present = False

    if not band_hits.empty:
        known = band_hits[band_hits["_IDENTITY_"] != "UNKNOWN"]
        if not known.empty:
            grp = (
                known.groupby(["_IDENTITY_", "_CHANNEL_", "BAND_NAME", "BAND_TYPE", "BAND_THRESHOLD"], sort=False)
                .agg(
                    count=("BAND_NAME", "size"),
                    total_amount=("CREDIT", "sum"),
                    roundish_count=("IS_ROUNDISH", "sum"),
                    first_date=("DATE", "min"),
                    last_date=("DATE", "max"),
                )
                .reset_index()
            )
            raw["single_threshold_by_identity_summary"] = grp.to_dict(orient="records")

            qualified = grp[grp["count"] >= MIN_REPEATS_FOR_STRUCTURING]
            for _, row in qualified.iterrows():
                subset = known[
                    (known["_IDENTITY_"] == row["_IDENTITY_"])
                    & (known["_CHANNEL_"] == row["_CHANNEL_"])
                    & (known["BAND_NAME"] == row["BAND_NAME"])
                ]

                flagged_rows.update(subset["_ROW_ID_"].tolist())

                ch_counts = subset["_TRANSCODE_"].value_counts().to_dict()
                if ch_counts:
                    channel_support_present = True

                if str(row["BAND_TYPE"]) == "confirmed":
                    confirmed_repeat = True
                if str(row["BAND_TYPE"]) == "avoidance":
                    avoidance_repeat = True
                if int(row["roundish_count"]) >= MIN_REPEATS_FOR_STRUCTURING:
                    roundish_support = True

                aggregation.append({
                    "aggregation_key": row["_IDENTITY_"],
                    "aggregation_type": "identity",
                    "channel": row["_CHANNEL_"],
                    "band_name": row["BAND_NAME"],
                    "band_type": row["BAND_TYPE"],
                    "threshold": int(row["BAND_THRESHOLD"]) if pd.notna(row["BAND_THRESHOLD"]) else None,
                    "count": int(row["count"]),
                    "total_amount": float(row["total_amount"]),
                    "roundish_count": int(row["roundish_count"]),
                    "first_date": str(row["first_date"].date()),
                    "last_date": str(row["last_date"].date()),
                    "received_transcodes": ch_counts,
                })

        unknown = band_hits[band_hits["_IDENTITY_"] == "UNKNOWN"]
        unknown_pat_by_channel: List[Dict[str, Any]] = []

        for channel, uch in unknown.groupby("_CHANNEL_", sort=False):
            monthly_info = _monthly_pattern_summary(uch)
            unknown_pat_by_channel.append({"channel": channel, "monthly_pattern": monthly_info})

            if uch.empty or not monthly_info.get("has_monthly_pattern"):
                continue

            monthly_band_density = (
                uch.groupby(["YM", "BAND_NAME"], sort=False)
                .agg(
                    count=("BAND_NAME", "size"),
                    distinct_days=("DATE", lambda s: s.dt.date.nunique()),
                    max_gap_days=("DATE", _max_gap_days),
                    max_txns_within_7d=("DATE", lambda s: _txns_within_days(s, 7)),
                )
                .reset_index()
            )

            qualifying_pairs = set(
                tuple(x)
                for x in monthly_band_density[
                    (monthly_band_density["count"] >= MIN_REPEATS_FOR_STRUCTURING)
                    & (monthly_band_density["distinct_days"].fillna(0) <= MONTHLY_MAX_DISTINCT_DAYS)
                    & (monthly_band_density["max_gap_days"].fillna(0) <= MONTHLY_MAX_GAP_DAYS)
                    & (monthly_band_density["max_txns_within_7d"].fillna(0) >= MIN_REPEATS_FOR_STRUCTURING)
                ][["YM", "BAND_NAME"]].values.tolist()
            )

            band_month_counts = (
                pd.DataFrame(list(qualifying_pairs), columns=["YM", "BAND_NAME"])
                .groupby("BAND_NAME", sort=False)["YM"]
                .nunique()
                .reset_index(name="month_count")
                if qualifying_pairs
                else pd.DataFrame(columns=["BAND_NAME", "month_count"])
            )

            recurring_bands = set(
                band_month_counts[band_month_counts["month_count"] >= 2]["BAND_NAME"].tolist()
            )

            if qualifying_pairs:
                q_pairs_df = pd.DataFrame(list(qualifying_pairs), columns=["YM", "BAND_NAME"])
                q_unknown = uch.merge(q_pairs_df, on=["YM", "BAND_NAME"], how="inner")
            else:
                q_unknown = uch.iloc[0:0].copy()

            if recurring_bands:
                q_unknown = q_unknown[q_unknown["BAND_NAME"].isin(recurring_bands) | q_unknown["BAND_NAME"].isin(
                    q_pairs_df["BAND_NAME"].tolist() if not q_unknown.empty else []
                )]

            flagged_rows.update(q_unknown["_ROW_ID_"].tolist())

            if not q_unknown.empty:
                gb = (
                    q_unknown.groupby(["YM", "BAND_NAME", "BAND_TYPE", "BAND_THRESHOLD"], sort=False)
                    .agg(
                        count=("BAND_NAME", "size"),
                        total_amount=("CREDIT", "sum"),
                        roundish_count=("IS_ROUNDISH", "sum"),
                        first_date=("DATE", "min"),
                        last_date=("DATE", "max"),
                    )
                    .reset_index()
                )

                raw_key = f"single_threshold_unknown_monthly_summary__{str(channel)[:40]}"
                raw[raw_key] = gb.to_dict(orient="records")

                for _, row in gb.iterrows():
                    subset = q_unknown[
                        (q_unknown["YM"] == row["YM"]) & (q_unknown["BAND_NAME"] == row["BAND_NAME"])
                    ]

                    ch_counts = subset["_TRANSCODE_"].value_counts().to_dict()
                    if ch_counts:
                        channel_support_present = True

                    if str(row["BAND_TYPE"]) == "confirmed" and int(row["count"]) >= MIN_REPEATS_FOR_STRUCTURING:
                        confirmed_repeat = True
                    if str(row["BAND_TYPE"]) == "avoidance" and int(row["count"]) >= MIN_REPEATS_FOR_STRUCTURING:
                        avoidance_repeat = True
                    if int(row["roundish_count"]) >= MIN_REPEATS_FOR_STRUCTURING:
                        roundish_support = True

                    aggregation.append({
                        "aggregation_key": f"UNKNOWN_MONTH_{row['YM']}",
                        "aggregation_type": "unknown_monthly_pattern",
                        "channel": channel,
                        "band_name": row["BAND_NAME"],
                        "band_type": row["BAND_TYPE"],
                        "threshold": int(row["BAND_THRESHOLD"]) if pd.notna(row["BAND_THRESHOLD"]) else None,
                        "count": int(row["count"]),
                        "total_amount": float(row["total_amount"]),
                        "roundish_count": int(row["roundish_count"]),
                        "first_date": str(row["first_date"].date()),
                        "last_date": str(row["last_date"].date()),
                        "received_transcodes": ch_counts,
                    })

        raw["unknown_monthly_pattern_by_channel"] = unknown_pat_by_channel

        if not aggregation:
            gb = (
                band_hits.groupby(["_CHANNEL_", "BAND_NAME", "BAND_TYPE", "BAND_THRESHOLD"], sort=False)
                .agg(
                    count=("BAND_NAME", "size"),
                    total_amount=("CREDIT", "sum"),
                    roundish_count=("IS_ROUNDISH", "sum"),
                    first_date=("DATE", "min"),
                    last_date=("DATE", "max"),
                )
                .reset_index()
            )
            raw["single_threshold_band_fallback_summary"] = gb.to_dict(orient="records")

            qualified = gb[gb["count"] >= MIN_REPEATS_FOR_STRUCTURING]
            if not qualified.empty:
                for _, row in qualified.iterrows():
                    subset = band_hits[
                        (band_hits["_CHANNEL_"] == row["_CHANNEL_"])
                        & (band_hits["BAND_NAME"] == row["BAND_NAME"])
                    ]
                    flagged_rows.update(subset["_ROW_ID_"].tolist())

                    ch_counts = subset["_TRANSCODE_"].value_counts().to_dict()
                    if ch_counts:
                        channel_support_present = True

                    if str(row["BAND_TYPE"]) == "confirmed":
                        confirmed_repeat = True
                    if str(row["BAND_TYPE"]) == "avoidance":
                        avoidance_repeat = True
                    if int(row["roundish_count"]) >= MIN_REPEATS_FOR_STRUCTURING:
                        roundish_support = True

                    aggregation.append({
                        "aggregation_key": "CHANNEL_BAND",
                        "aggregation_type": "band_fallback",
                        "channel": row["_CHANNEL_"],
                        "band_name": row["BAND_NAME"],
                        "band_type": row["BAND_TYPE"],
                        "threshold": int(row["BAND_THRESHOLD"]) if pd.notna(row["BAND_THRESHOLD"]) else None,
                        "count": int(row["count"]),
                        "total_amount": float(row["total_amount"]),
                        "roundish_count": int(row["roundish_count"]),
                        "first_date": str(row["first_date"].date()),
                        "last_date": str(row["last_date"].date()),
                        "received_transcodes": ch_counts,
                    })

    raw["aggregation"] = aggregation

    # =========================================================
    # 2B) BOUNDARY-THRESHOLD SUPPORT
    # =========================================================
    boundary_hits = work[work["BOUNDARY_NAME"].notna()].copy()

    boundary_support: List[Dict[str, Any]] = []
    if not boundary_hits.empty:
        gb = (
            boundary_hits.groupby(["_CHANNEL_", "BOUNDARY_NAME", "BOUNDARY_THRESHOLD"], sort=False)
            .agg(
                count=("BOUNDARY_NAME", "size"),
                total_amount=("CREDIT", "sum"),
                first_date=("DATE", "min"),
                last_date=("DATE", "max"),
            )
            .reset_index()
        )
        boundary_support = gb.to_dict(orient="records")

    raw["boundary_threshold_support"] = boundary_support
    if any(x.get("count", 0) >= 2 for x in boundary_support):
        indicators.append(
            "Boundary-threshold deposit behaviour observed: repeated amounts at or immediately below reporting thresholds within single channels."
        )

    # =========================================================
    # 3) OPTIONAL SUPPORT: repeated same deposit amounts per transcode
    # =========================================================
    same_amount_support: List[Dict[str, Any]] = []
    if "TRANSCODE" in work.columns:
        tmp = work[["_TRANSCODE_", "CREDIT"]].copy()
        tmp["_AMT_BUCKET_"] = tmp["CREDIT"].map(lambda x: _bucket_amount(float(x), DEPOSIT_REPEAT_BIN_SIZE))

        gb = (
            tmp.groupby(["_TRANSCODE_", "_AMT_BUCKET_"], sort=False)
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        gb = gb[gb["count"] >= MIN_REPEATS_SAME_AMOUNT_SUPPORT]
        if not gb.empty:
            same_amount_support = gb.head(25).to_dict(orient="records")

    raw["same_amount_deposit_support_by_transcode"] = same_amount_support

    # ---------------------------------------------------------
    # Indicators for threshold structuring
    # ---------------------------------------------------------
    if aggregation:
        indicators.append(
            "Repeated near-threshold deposits detected within single inflow channels; aggregated by identity, unknown monthly pattern, or channel-band fallback where needed."
        )
    if avoidance_repeat:
        indicators.append(
            "Repeated deposits within near-threshold avoidance bands detected, supporting threshold structuring intent."
        )
    if confirmed_repeat:
        indicators.append(
            "Repeated deposits within confirmed near-threshold bands detected, indicating a strong threshold structuring signal."
        )
    if roundish_support:
        indicators.append(
            "Round-figure behaviour present in repeated near-threshold deposits as supporting evidence."
        )
    if channel_support_present:
        indicators.append(
            "Transcode breakdown attached for qualifying groups to show where structured deposits were received."
        )

    # =========================================================
    # SCORING
    # =========================================================
    strength = 0.0

    if classic_hits:
        strength += 0.25
    if same_day_hits:
        strength += 0.20
    if rolling_hits:
        strength += 0.15
    if raw.get("laddered_threshold_avoidance") or raw.get("laddered_threshold_monthly_recurrence"):
        strength += 0.18
    if raw.get("identity_concentration_within_channel"):
        strength += 0.15
    if any(x.get("qualified") for x in raw.get("channel_monthly_threshold_avoidance", [])):
        strength += 0.20
    if any(x.get("recurs_every_other_month") for x in raw.get("channel_monthly_threshold_avoidance", [])):
        strength += 0.05
    if aggregation:
        strength += 0.25
    if avoidance_repeat:
        strength += 0.08
    if confirmed_repeat:
        strength += 0.12
    if roundish_support:
        strength += 0.03

    strength = min(strength, 1.0)
    triggered = len(flagged_rows) > 0

    raw["upstream_support"] = {
        "round_figures_triggered": bool((prior_results.get("round_figures") or {}).get("triggered"))
        if isinstance(prior_results, dict)
        else False,
        "recurrence_available": bool(recurring_identities),
        "recurring_identity_hit_count": int(work["__RECURRING_ID__"].sum()) if "__RECURRING_ID__" in work.columns else 0,
        "analysis_cache_used": bool(analysis_cache),
        "input_row_count": int(len(df) if df is not None else 0),
        "credit_row_count": int(len(work)),
        "deep_scan_group_cap": MAX_GROUP_ROWS_FOR_DEEP_SCAN,
    }

    return {
        "triggered": triggered,
        "strength": strength if triggered else 0.0,
        "indicators": indicators,
        "flagged_row_ids": sorted(flagged_rows),
        "raw": raw,
        "pattern": "structured_deposits" if triggered else None,
    }