"""
cash_intensive.py
=================

Detects cash-intensive behaviour:
    • Frequent ATM withdrawals
    • Large cash deposits
    • High ratio of cash-channel transactions
    • Recurrent small-value cash withdrawals (K200–K1500)
    • Cash turnover disproportionately high vs total turnover
    • Cash-in / cash-out same-day behaviour (mule patterns)
"""

from typing import Dict, Any, List
import pandas as pd
import re


# ============================================================
# CASH KEYWORDS — expanded & normalized
# ============================================================

CASH_KEYWORDS = [
    r"ATM",
    r"\bCASH\b",
    r"CASH DEP",
    r"CASH DEPOSIT",
    r"ATM WITHDRAWAL",
    r"WITHDRAWAL ATM",
    r"POS CASHOUT",
    r"CASH OUT",
    r"CASHOUT",
]


def _is_cash_related(text: str) -> bool:
    """Safe regex-based detection of cash keywords."""
    if not isinstance(text, str):
        return False
    text = text.upper()
    return any(re.search(pattern, text) for pattern in CASH_KEYWORDS)


# ============================================================
# MAIN DETECTOR
# ============================================================

def detect_cash_intensive(df: pd.DataFrame) -> Dict[str, Any]:
    df = df.copy()

    # Preserve original row ids for downstream listing
    if "ROW_ID" not in df.columns:
        df["ROW_ID"] = df.index

    df["DESCRIPTION_RAW"] = df.get("DESCRIPTION_RAW", "").astype(str)
    df["CREDIT"] = pd.to_numeric(df.get("CREDIT", 0), errors="coerce").fillna(0.0)
    df["DEBIT"] = pd.to_numeric(df.get("DEBIT", 0), errors="coerce").fillna(0.0)

    # Ensure DATE for same-day analysis
    if "DATE" not in df.columns:
        df["DATE"] = pd.to_datetime(df.get("DATE_STR"), errors="coerce")
    else:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

    total_debits = float(df["DEBIT"].sum())
    total_credits = float(df["CREDIT"].sum())
    total_turnover = max(1, total_debits + total_credits)

    # --------------------------------------------------------
    # Identify cash-related rows
    # --------------------------------------------------------
    df["is_cash"] = df["DESCRIPTION_RAW"].apply(_is_cash_related)
    cash_df = df[df["is_cash"]].copy()

    # Row-level listing support: all cash-related transactions
    flagged_row_ids: List[int] = []
    try:
        flagged_row_ids = [int(x) for x in cash_df["ROW_ID"].dropna().astype(int).tolist()]
        flagged_row_ids = sorted(set(flagged_row_ids))
    except Exception:
        flagged_row_ids = []

    # --------------------------------------------------------
    # SAFE FALSE RETURN
    # --------------------------------------------------------
    if cash_df.empty:
        return {
            "triggered": False,
            "strength": 0.0,
            "indicators": ["No cash-intensive indicators detected."],
            "flagged_row_ids": [],
            "raw": {
                # 🔧 new observability metrics (safe defaults)
                "cash_txn_count": 0,
                "cash_ratio": 0.0,
                "cash_turnover_ratio": 0.0,
                "cash_withdrawals": 0.0,
                "cash_deposits": 0.0,
                "small_atm_count": 0,
                "large_cash_deposits": 0,
                "same_day_cash_cycles": 0,
                "cash_credit_count": 0,
                "cash_debit_count": 0,
                "cash_credit_ratio": 0.0,
                "cash_debit_ratio": 0.0,
                "cash_withdrawal_ratio_of_debits": 0.0,
                "cash_deposit_ratio_of_credits": 0.0,
                "max_cash_withdrawal": 0.0,
                "max_cash_deposit": 0.0,
                "cash_days_active": 0,
            },
            "pattern": None,
        }

    cash_withdrawals = float(cash_df["DEBIT"].sum())
    cash_deposits = float(cash_df["CREDIT"].sum())
    cash_txn_count = len(cash_df)
    total_txn = len(df)

    cash_ratio = cash_txn_count / max(1, total_txn)
    cash_turnover_ratio = (cash_withdrawals + cash_deposits) / total_turnover

    # ============================================================
    # SPECIAL AML CASH PATTERNS
    # ============================================================

    # 1. Recurrent small ATM withdrawals (structuring-like)
    small_atm = cash_df[
        (cash_df["DEBIT"].between(200, 1500)) &
        cash_df["DESCRIPTION_RAW"].str.contains("ATM", case=False, na=False)
    ]
    small_atm_count = len(small_atm)

    # 2. Large cash deposits (red flag above K5–10k)
    large_cash_deposits = cash_df[cash_df["CREDIT"] >= 5000]

    # 3. Same-day cash-in → cash-out (mule pattern)
    same_day_cycles = 0
    for date, grp in cash_df.groupby(cash_df["DATE"].dt.date):
        if grp["CREDIT"].sum() > 0 and grp["DEBIT"].sum() > 0:
            same_day_cycles += 1

    # ============================================================
    # SCORING (0–1) — UNCHANGED
    # ============================================================

    score = 0.0

    # Core cash behaviour (50%)
    score += 0.25 * cash_ratio
    score += 0.25 * cash_turnover_ratio

    # Structuring indicator
    if small_atm_count >= 10:
        score += 0.2

    # Large cash deposits
    if len(large_cash_deposits) >= 3:
        score += 0.15

    # Same-day cycles
    if same_day_cycles >= 2:
        score += 0.15

    strength = float(max(0, min(score, 1)))

    # ============================================================
    # 🔧 NEW OBSERVABILITY METRICS (NO LOGIC CHANGE)
    # ============================================================

    cash_credit_df = cash_df[cash_df["CREDIT"] > 0]
    cash_debit_df = cash_df[cash_df["DEBIT"] > 0]

    cash_credit_count = int(len(cash_credit_df))
    cash_debit_count = int(len(cash_debit_df))

    cash_credit_ratio = cash_credit_count / max(1, cash_txn_count)
    cash_debit_ratio = cash_debit_count / max(1, cash_txn_count)

    cash_withdrawal_ratio_of_debits = cash_withdrawals / max(1.0, total_debits) if total_debits > 0 else 0.0
    cash_deposit_ratio_of_credits = cash_deposits / max(1.0, total_credits) if total_credits > 0 else 0.0

    max_cash_withdrawal = float(cash_debit_df["DEBIT"].max()) if not cash_debit_df.empty else 0.0
    max_cash_deposit = float(cash_credit_df["CREDIT"].max()) if not cash_credit_df.empty else 0.0

    cash_days_active = int(
        cash_df["DATE"].dt.date.nunique()
    ) if cash_df["DATE"].notna().any() else 0

    # ============================================================
    # INDICATORS (HUMAN READABLE) — UNCHANGED
    # ============================================================

    indicators: List[str] = []

    if cash_ratio >= 0.35:
        indicators.append(
            f"High proportion of cash-based activities ({cash_ratio:.0%} of all transactions)."
        )

    if cash_turnover_ratio >= 0.40:
        indicators.append(
            f"Cash turnover is high relative to total activity ({cash_turnover_ratio:.0%})."
        )

    if small_atm_count >= 10:
        indicators.append(
            f"Frequent small ATM withdrawals (≥ {small_atm_count} events between K200–K1500)."
        )

    if len(large_cash_deposits) >= 3:
        indicators.append(
            f"Multiple large cash deposits detected (≥ {len(large_cash_deposits)} deposits ≥K5,000)."
        )

    if same_day_cycles >= 2:
        indicators.append(
            f"Same-day cash-in and cash-out detected across {same_day_cycles} days (possible mule activity)."
        )

    if not indicators:
        indicators.append("Cash activity present but not materially risky.")

    return {
        "triggered": strength >= 0.35,
        "strength": round(strength, 3),
        "indicators": indicators,
        "flagged_row_ids": flagged_row_ids,
        "raw": {
            # Existing metrics (preserved)
            "cash_txn_count": cash_txn_count,
            "cash_ratio": cash_ratio,
            "cash_turnover_ratio": cash_turnover_ratio,
            "cash_withdrawals": cash_withdrawals,
            "cash_deposits": cash_deposits,
            "small_atm_count": small_atm_count,
            "large_cash_deposits": len(large_cash_deposits),
            "same_day_cash_cycles": same_day_cycles,

            # 🔧 New metrics
            "cash_credit_count": cash_credit_count,
            "cash_debit_count": cash_debit_count,
            "cash_credit_ratio": cash_credit_ratio,
            "cash_debit_ratio": cash_debit_ratio,
            "cash_withdrawal_ratio_of_debits": cash_withdrawal_ratio_of_debits,
            "cash_deposit_ratio_of_credits": cash_deposit_ratio_of_credits,
            "max_cash_withdrawal": max_cash_withdrawal,
            "max_cash_deposit": max_cash_deposit,
            "cash_days_active": cash_days_active,
        },
        "pattern": "cash_intensive",
    }
