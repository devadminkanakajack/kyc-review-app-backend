"""
round_figures.py
================

Detects suspicious round-figure credit & debit patterns.
"""

from typing import Dict, Any, List
import pandas as pd
import numpy as np


def _is_round(value: float) -> bool:
    """Identify values like 100, 200, 500, 1000, 5000, 10000…"""
    if value <= 0:
        return False
    return value % 100 == 0  # simple but effective AML signal


def detect_round_figures(df: pd.DataFrame) -> Dict[str, Any]:
    df = df.copy()

    # Preserve original row ids for downstream listing
    if "ROW_ID" not in df.columns:
        df["ROW_ID"] = df.index

    df["CREDIT"] = pd.to_numeric(df.get("CREDIT", 0), errors="coerce").fillna(0.0)
    df["DEBIT"] = pd.to_numeric(df.get("DEBIT", 0), errors="coerce").fillna(0.0)

    # ------------------------------------------------------------
    # Ensure DATE exists
    # ------------------------------------------------------------
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    elif "DATE_STR" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE_STR"], errors="coerce")
    else:
        df["DATE"] = pd.date_range(start="2000-01-01", periods=len(df))

    # ------------------------------------------------------------
    # Flag round-figure credits & debits
    # ------------------------------------------------------------
    df["is_round_credit"] = df["CREDIT"].apply(_is_round)
    df["is_round_debit"] = df["DEBIT"].apply(_is_round)

    round_df = df[(df["is_round_credit"]) | (df["is_round_debit"])].copy()

    # ------------------------------------------------------------
    # NO ROUND FIGURES FOUND → SAFE RETURN
    # ------------------------------------------------------------
    if round_df.empty:
        return {
            "triggered": False,
            "strength": 0.0,
            "indicators": ["No round-figure patterns detected."],
            "flagged_row_ids": [],
            "raw": {},
            "pattern": None,
        }

    # ------------------------------------------------------------
    # ORIGINAL METRICS (UNCHANGED)
    # ------------------------------------------------------------
    round_count = len(round_df)
    total_txn = len(df)
    ratio = round_count / max(1, total_txn)

    # Clustering within short windows (≤ 3 days)
    round_df = round_df.sort_values("DATE")
    round_df["next_date"] = round_df["DATE"].shift(-1)

    cluster_count = 0
    for _, row in round_df.iterrows():
        if pd.isna(row["next_date"]):
            continue
        if (row["next_date"] - row["DATE"]).days <= 3:
            cluster_count += 1

    # ------------------------------------------------------------
    # SCORING (UNCHANGED)
    # ------------------------------------------------------------
    score = (0.6 * ratio) + (0.4 * min(cluster_count / 5, 1.0))
    strength = float(max(0, min(score, 1)))

    # ------------------------------------------------------------
    # Row-level listing support
    # ------------------------------------------------------------
    flagged_row_ids: List[int] = []
    try:
        flagged_row_ids = [int(x) for x in round_df["ROW_ID"].dropna().astype(int).tolist()]
        flagged_row_ids = sorted(set(flagged_row_ids))
    except Exception:
        flagged_row_ids = []

    # ------------------------------------------------------------
    # 🔧 NEW OBSERVABILITY METRICS (NO LOGIC CHANGE)
    # ------------------------------------------------------------
    round_credit_df = round_df[round_df["is_round_credit"]]
    round_debit_df = round_df[round_df["is_round_debit"]]

    round_credit_count = int(len(round_credit_df))
    round_debit_count = int(len(round_debit_df))

    round_credit_amount = float(round_credit_df["CREDIT"].sum())
    round_debit_amount = float(round_debit_df["DEBIT"].sum())

    total_value = float(df["CREDIT"].sum() + df["DEBIT"].sum())
    round_value_total = round_credit_amount + round_debit_amount
    round_value_ratio = round_value_total / total_value if total_value > 0 else None

    # Dominant round amount (e.g. repeated K50,000)
    amounts = (
        pd.concat([
            round_credit_df["CREDIT"],
            round_debit_df["DEBIT"]
        ])
        .round(2)
    )

    if not amounts.empty:
        dominant_round_amount = float(amounts.value_counts().idxmax())
        dominant_round_frequency = int(amounts.value_counts().max())
    else:
        dominant_round_amount = None
        dominant_round_frequency = 0

    # Same-day clustering
    same_day_round_clusters = (
        round_df.groupby(round_df["DATE"].dt.date)
        .size()
        .loc[lambda x: x >= 2]
        .count()
    )

    # ------------------------------------------------------------
    # INDICATORS (UNCHANGED, WITH BETTER CONTEXT)
    # ------------------------------------------------------------
    indicators: List[str] = []

    if ratio >= 0.2:
        indicators.append(
            f"High proportion of round-figure transactions ({ratio:.0%} of all activity)."
        )

    if cluster_count >= 3:
        indicators.append(
            f"Round-figure clustering detected ({cluster_count} events occurring within tight 3-day windows)."
        )

    if dominant_round_frequency >= 3:
        indicators.append(
            f"Repeated identical round amounts detected (e.g. {dominant_round_frequency} occurrences of {dominant_round_amount:.0f})."
        )

    if not indicators:
        indicators.append("Round-figure activity detected but not materially risky.")

    # ------------------------------------------------------------
    # FINAL RETURN
    # ------------------------------------------------------------
    return {
        "triggered": strength >= 0.35,
        "strength": round(strength, 3),
        "indicators": indicators,
        "flagged_row_ids": flagged_row_ids,
        "raw": {
            "round_txn_count": round_count,
            "round_ratio": ratio,
            "cluster_count": cluster_count,

            # 🔧 New metrics
            "round_credit_count": round_credit_count,
            "round_debit_count": round_debit_count,
            "round_credit_amount": round_credit_amount,
            "round_debit_amount": round_debit_amount,
            "round_value_ratio": round_value_ratio,
            "dominant_round_amount": dominant_round_amount,
            "dominant_round_frequency": dominant_round_frequency,
            "same_day_round_clusters": same_day_round_clusters,
        },
        "pattern": "round_figures",
    }
