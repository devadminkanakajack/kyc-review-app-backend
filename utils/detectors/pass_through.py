"""
pass_through.py
===============

Detects pass-through / mule-style behaviour where funds enter the
account and are quickly paid out again with minimal retention.

Returns:
{
    "triggered": bool,
    "strength": float (0–1),
    "indicators": List[str],
    "flagged_row_ids": [...],          # row-level listing support
    "raw": { ... },
    "pattern": "pass_through" | None
}
"""

from typing import Dict, Any, List, Set, Tuple
import pandas as pd
import numpy as np


MIN_TOTAL_CREDITS = 10_000.0
MIN_WINDOW_CREDITS = 5_000.0

# calendar window
WINDOW_DAYS = 5

# Tightening guardrails
PASS_THROUGH_TRIGGER_THRESHOLD = 0.55
MIN_MEANINGFUL_CREDIT = 500.0
MIN_MEANINGFUL_DEBIT = 100.0
BENIGN_INFLOW_PATTERNS = (
    "SALARY", "WAGES", "PAYROLL", "ALLOWANCE", "STIPEND",
    "REIMBURSEMENT", "REFUND", "PAY REF", "PAYREF",
    "FAMILY USE", "FAMILY SUPPORT", "REMITTANCE",
)

# Display-only filter for flagged rows
MIN_FLAG_AMOUNT = 50.0


def _norm_text(v: Any) -> str:
    return " ".join(str(v or "").upper().split())


def _is_benign_inflow_row(row: pd.Series) -> bool:
    text = _norm_text(f"{row.get('DESCRIPTION_RAW', '')} {row.get('DESCRIPTION', '')} {row.get('IDENTITY', '')}")
    if not text:
        return False
    return any(pat in text for pat in BENIGN_INFLOW_PATTERNS)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = pd.to_numeric(v, errors="coerce")
        if pd.isna(x):
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def _ensure_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure DATE exists and is parsed, then normalized to day-level (00:00:00).
    This prevents same-day timestamps splitting into multiple groups.
    """
    df = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df).copy()

    if "DATE" in df.columns:
        out = pd.to_datetime(df["DATE"], errors="coerce", dayfirst=True)
        if out.notna().any():
            df["DATE"] = out.dt.normalize()
            return df

    if "DATE_STR" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE_STR"], errors="coerce", dayfirst=True).dt.normalize()
    else:
        # fallback: synthetic dates
        df["DATE"] = pd.to_datetime(range(len(df)), unit="D", origin="unix").normalize()

    return df


def _daily_series(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a true calendar-daily time series from min..max date,
    filling missing dates with 0 credits/debits.
    """
    daily = (
        df.groupby("DATE", sort=True)[["CREDIT", "DEBIT"]]
        .sum()
        .sort_index()
        .copy()
    )

    if daily.empty:
        return daily

    idx = pd.date_range(daily.index.min(), daily.index.max(), freq="D")
    daily = daily.reindex(idx, fill_value=0.0)
    daily.index.name = "DATE"
    return daily


def _compute_fast_outflow_ratio(df: pd.DataFrame) -> float:
    """
    Weighted average of N-day (calendar) windows:
      window_ratio = min(credits_Nd, debits_Nd) / credits_Nd
    Only consider windows where credits_Nd >= MIN_WINDOW_CREDITS.
    """
    daily = _daily_series(df)
    if daily.empty:
        return 0.0

    total_credits = float(daily["CREDIT"].sum())
    if total_credits <= 0:
        return 0.0

    daily["credits_w"] = daily["CREDIT"].rolling(window=WINDOW_DAYS, min_periods=1).sum()
    daily["debits_w"] = daily["DEBIT"].rolling(window=WINDOW_DAYS, min_periods=1).sum()

    mask = daily["credits_w"] >= MIN_WINDOW_CREDITS
    if not bool(mask.any()):
        return 0.0

    sub = daily.loc[mask, ["credits_w", "debits_w"]].copy()
    credits = sub["credits_w"].to_numpy(dtype=float)
    debits = sub["debits_w"].to_numpy(dtype=float)

    with np.errstate(divide="ignore", invalid="ignore"):
        ratios = np.where(credits > 0, np.minimum(credits, debits) / credits, 0.0)

    weights = credits
    if weights.sum() <= 0:
        return 0.0

    fast_ratio = float(np.average(ratios, weights=weights))
    return max(0.0, min(fast_ratio, 1.0))


def _flag_pass_through_rows(df: pd.DataFrame) -> Tuple[List[int], List[str]]:
    """
    Return row-level ids for the most pass-through-like N-day windows.

    Display-only:
      - Picks strong windows (>=0.7), else best single window.
      - Returns ROW_IDs for rows that fall within the flagged days,
        filtered by MIN_FLAG_AMOUNT to avoid fee spam.

    Returns:
      flagged_row_ids: list[int] of original dataframe indices
      flagged_window_dates: list[str] of YYYY-MM-DD dates covered by the flagged windows
    """
    if df.empty or "DATE" not in df.columns or "ROW_ID" not in df.columns:
        return [], []

    work = df[["DATE", "CREDIT", "DEBIT", "ROW_ID"]].copy()
    work["DATE"] = pd.to_datetime(work["DATE"], errors="coerce").dt.normalize()

    daily = _daily_series(work)
    if daily.empty or float(daily["CREDIT"].sum()) <= 0:
        return [], []

    daily["credits_w"] = daily["CREDIT"].rolling(window=WINDOW_DAYS, min_periods=1).sum()
    daily["debits_w"] = daily["DEBIT"].rolling(window=WINDOW_DAYS, min_periods=1).sum()

    mask = daily["credits_w"] >= MIN_WINDOW_CREDITS
    if not bool(mask.any()):
        return [], []

    sub = daily.loc[mask, ["credits_w", "debits_w"]].copy()
    credits = sub["credits_w"].to_numpy(dtype=float)
    debits = sub["debits_w"].to_numpy(dtype=float)

    with np.errstate(divide="ignore", invalid="ignore"):
        sub["window_ratio"] = np.where(credits > 0, np.minimum(credits, debits) / credits, 0.0)

    strong = sub[sub["window_ratio"] >= 0.7]
    pick = strong if not strong.empty else sub.nlargest(1, "window_ratio")

    flagged_dates: Set[pd.Timestamp] = set()
    for anchor_date in pick.index:
        for d in pd.date_range(end=anchor_date, periods=WINDOW_DAYS, freq="D"):
            flagged_dates.add(pd.Timestamp(d).normalize())

    if not flagged_dates:
        return [], []

    work["DATE_N"] = work["DATE"].dt.normalize()
    amt = work[["CREDIT", "DEBIT"]].max(axis=1)

    hit = work[
        (work["DATE_N"].isin(flagged_dates))
        & (amt >= MIN_FLAG_AMOUNT)
        & ((work["CREDIT"] > 0) | (work["DEBIT"] > 0))
    ]

    row_ids = [int(x) for x in hit["ROW_ID"].dropna().astype(int).tolist()]
    win_dates = sorted({d.strftime("%Y-%m-%d") for d in flagged_dates})

    return sorted(set(row_ids)), win_dates


def _expand_focus_dates_from_row_ids(
    base_df: pd.DataFrame,
    row_ids: List[int],
    pad_days: int = WINDOW_DAYS,
) -> List[pd.Timestamp]:
    if base_df.empty or not row_ids or "ROW_ID" not in base_df.columns or "DATE" not in base_df.columns:
        return []

    hit = base_df[base_df["ROW_ID"].isin(row_ids)].copy()
    if hit.empty:
        return []

    dates = pd.to_datetime(hit["DATE"], errors="coerce").dropna().dt.normalize().unique().tolist()
    out = set()

    for d in dates:
        for x in pd.date_range(
            pd.Timestamp(d) - pd.Timedelta(days=pad_days),
            pd.Timestamp(d) + pd.Timedelta(days=pad_days),
            freq="D",
        ):
            out.add(pd.Timestamp(x).normalize())

    return sorted(out)


def detect_pass_through(
    df: pd.DataFrame,
    analysis_cache: Dict[str, Any] | None = None,
    prior_results: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    analysis_cache = analysis_cache or {}
    prior_results = prior_results or {}

    # Use the full statement for pass-through. Using credits-only removes the outflow side
    # and can either under-detect or distort the signal.
    df = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df).copy()

    if "ROW_ID" not in df.columns:
        df["ROW_ID"] = df.index

    df["CREDIT"] = pd.to_numeric(df.get("CREDIT", 0), errors="coerce").fillna(0.0)
    df["DEBIT"] = pd.to_numeric(df.get("DEBIT", 0), errors="coerce").fillna(0.0)

    df = _ensure_date(df)
    df["DESCRIPTION_RAW"] = df.get("DESCRIPTION_RAW", "").astype(str)
    if "DESCRIPTION" in df.columns:
        df["DESCRIPTION"] = df.get("DESCRIPTION", "").astype(str)
    else:
        df["DESCRIPTION"] = ""
    if "IDENTITY" in df.columns:
        df["IDENTITY"] = df.get("IDENTITY", "").astype(str)
    else:
        df["IDENTITY"] = ""
    df = df.sort_values("DATE").reset_index(drop=True)

    total_credits = float(df["CREDIT"].sum())
    total_debits = float(df["DEBIT"].sum())

    if total_credits <= 0:
        return {
            "triggered": False,
            "strength": 0.0,
            "indicators": ["No credit activity; pass-through cannot be assessed."],
            "flagged_row_ids": [],
            "raw": {
                "total_credits": total_credits,
                "total_debits": total_debits,
                "fast_outflow_ratio": 0.0,
                "net_retention_ratio": 0.0,
                "credit_debit_gap_ratio": 0.0,
                "turnover_to_balance_ratio": None,
                "same_day_debit_ratio": None,
                "dominant_outflow_date": None,
                "largest_credit_amount": None,
                "largest_debit_amount": None,
                "post_outflow_balance_ratio": None,
                "same_day_multi_debit_count": 0,
                "flagged_window_dates": [],
            },
            "pattern": None,
        }

    start_balance = None
    end_balance = None

    if "BALANCE" in df.columns:
        start_balance = _safe_float(df["BALANCE"].iloc[0], 0.0)
        end_balance = _safe_float(df["BALANCE"].iloc[-1], 0.0)
        net_retained = abs(end_balance - start_balance)
    else:
        net_retained = abs(total_credits - total_debits)

    net_retention_ratio = min(max(net_retained / total_credits, 0.0), 1.0)
    credit_debit_gap_ratio = min(
        max(abs(total_credits - total_debits) / total_credits, 0.0), 1.0
    )

    seed_row_ids: List[int] = []
    for det_name in ("structured_deposits", "structured_payments"):
        det_out = prior_results.get(det_name) if isinstance(prior_results, dict) else None
        if isinstance(det_out, dict):
            seed_row_ids.extend([int(x) for x in det_out.get("flagged_row_ids", []) if str(x).isdigit()])

    focus_dates = _expand_focus_dates_from_row_ids(df, sorted(set(seed_row_ids)))
    focus_df = df[df["DATE"].dt.normalize().isin(focus_dates)].copy() if focus_dates and len(df) > 15000 else df

    focus_df = focus_df.copy()
    focus_df["__BENIGN_INFLOW__"] = focus_df.apply(_is_benign_inflow_row, axis=1)
    assessment_df = focus_df[
        ((focus_df["CREDIT"] >= MIN_MEANINGFUL_CREDIT) & (~focus_df["__BENIGN_INFLOW__"]))
        | (focus_df["DEBIT"] >= MIN_MEANINGFUL_DEBIT)
    ].copy()

    fast_outflow_ratio = _compute_fast_outflow_ratio(assessment_df)
    flagged_row_ids, flagged_window_dates = _flag_pass_through_rows(assessment_df)

    turnover_to_balance_ratio = None
    if "BALANCE" in df.columns:
        bal = (
            df.assign(DATE_N=df["DATE"].dt.normalize())
            .groupby("DATE_N")["BALANCE"]
            .last()
        )
        bal = pd.to_numeric(bal, errors="coerce").fillna(0.0).astype(float)
        avg_bal = float(bal.abs().mean()) if len(bal) > 0 else 0.0
        if avg_bal > 0:
            turnover_to_balance_ratio = float(total_credits / avg_bal)

    daily_debits = df.groupby(df["DATE"].dt.normalize())["DEBIT"].sum()
    dominant_outflow_date = None
    same_day_debit_ratio = None

    if total_debits > 0 and not daily_debits.empty:
        max_day_debit = float(daily_debits.max())
        same_day_debit_ratio = max_day_debit / total_debits
        dominant_outflow_date = pd.Timestamp(daily_debits.idxmax()).strftime("%Y-%m-%d")

    largest_credit_amount = float(df["CREDIT"].max()) if total_credits > 0 else None
    largest_debit_amount = float(df["DEBIT"].max()) if total_debits > 0 else None

    deb = df[df["DEBIT"] > 0].copy()
    if not deb.empty:
        same_day_multi_debit_count = int(deb.groupby(deb["DATE"].dt.normalize()).size().max())
    else:
        same_day_multi_debit_count = 0

    post_outflow_balance_ratio = None
    if "BALANCE" in df.columns and largest_credit_amount:
        post_outflow_balance_ratio = abs(float(end_balance)) / largest_credit_amount

    score = 0.0
    score += 0.45 * fast_outflow_ratio
    score += 0.25 * (1.0 - net_retention_ratio)
    score += 0.15 * (1.0 - credit_debit_gap_ratio)
    score += 0.15 * min((same_day_debit_ratio or 0.0) / 0.60, 1.0)

    if total_credits < MIN_TOTAL_CREDITS:
        score *= 0.7

    # Tighten: do not trigger on ordinary spend-down unless there is both strong fast
    # outflow and one more pass-through style support factor.
    support_count = 0
    if fast_outflow_ratio >= 0.65:
        support_count += 1
    if net_retention_ratio <= 0.35:
        support_count += 1
    if credit_debit_gap_ratio <= 0.20:
        support_count += 1
    if (same_day_debit_ratio or 0.0) >= 0.35:
        support_count += 1
    if (turnover_to_balance_ratio or 0.0) >= 8.0:
        support_count += 1

    if support_count < 3:
        score *= 0.6

    if fast_outflow_ratio < 0.50:
        score *= 0.5

    score = float(max(0.0, min(score, 1.0)))

    indicators: List[str] = []

    if fast_outflow_ratio >= 0.7:
        indicators.append(
            f"High fast-outflow behaviour – around {fast_outflow_ratio:.0%} of credits are offset by debits within tight short windows."
        )
    elif fast_outflow_ratio >= 0.4:
        indicators.append(
            f"Moderate fast-outflow behaviour – about {fast_outflow_ratio:.0%} of credits are offset by nearby debits."
        )

    if net_retention_ratio <= 0.2 and total_credits >= MIN_TOTAL_CREDITS:
        indicators.append(
            f"Low net retention of funds – only about {net_retention_ratio:.0%} of total credits remain."
        )

    if credit_debit_gap_ratio <= 0.2:
        indicators.append(
            f"Credits and debits are closely matched (gap ≈ {credit_debit_gap_ratio:.0%})."
        )

    if turnover_to_balance_ratio is not None and turnover_to_balance_ratio >= 10:
        indicators.append(
            f"High turnover relative to average balance (ratio ≈ {turnover_to_balance_ratio:.1f})."
        )

    if not indicators:
        indicators.append("No strong evidence of pass-through behaviour.")

    return {
        "triggered": score >= PASS_THROUGH_TRIGGER_THRESHOLD,
        "strength": round(score, 3),
        "indicators": indicators,
        "flagged_row_ids": flagged_row_ids,
        "raw": {
            "total_credits": total_credits,
            "total_debits": total_debits,
            "fast_outflow_ratio": fast_outflow_ratio,
            "net_retention_ratio": net_retention_ratio,
            "credit_debit_gap_ratio": credit_debit_gap_ratio,
            "turnover_to_balance_ratio": turnover_to_balance_ratio,
            "same_day_debit_ratio": same_day_debit_ratio,
            "dominant_outflow_date": dominant_outflow_date,
            "largest_credit_amount": largest_credit_amount,
            "largest_debit_amount": largest_debit_amount,
            "post_outflow_balance_ratio": post_outflow_balance_ratio,
            "same_day_multi_debit_count": same_day_multi_debit_count,
            "flagged_window_dates": flagged_window_dates,
            "assessment_row_count": int(len(assessment_df)),
            "benign_inflow_suppressed_count": int(focus_df["__BENIGN_INFLOW__"].sum()) if "__BENIGN_INFLOW__" in focus_df.columns else 0,
            "support_count": int(support_count),
        },
        "pattern": "pass_through",
    }