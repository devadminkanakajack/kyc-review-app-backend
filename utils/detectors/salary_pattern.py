"""
salary_pattern.py
=================

Detects salary / payroll inflows (SoF) AND checks employed-style outflow composition (UoF)
using PNG transaction-code priors + recurrence cadence + amount stability.

Adds:
- flagged_row_ids (row-level listing support for per-channel suspicious listing)
  * salary-related credits (always)
  * plus salary-wash outflows when wash is detected
"""

from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple, Set
import re

import numpy as np
import pandas as pd


# -------------------------------------------------------------------
# PNG Transaction-code priors (your requested mapping)
# -------------------------------------------------------------------
# Inflow salary priority (highest → lowest)
SALARY_INFLOW_PRIORITY: List[str] = [
    "198",  # SAVINGS: Salary Cheque (Employee)
    "189",  # SAVINGS: Direct Credit
    "723",  # CURRENT: ATM POS Payment In (used as credit inbound channel)
    "101",  # SAVINGS: Cash Deposit With Book
    "102",  # SAVINGS: Cheque Deposit With Book
    "703",  # SAVINGS: ATM/TELEBANKING Transfer In
]

SALARY_INFLOW_SET = set(SALARY_INFLOW_PRIORITY)

# Outflow focus channels (employed UoF expectations)
OUTFLOW_CODES: Dict[str, List[str]] = {
    "ech_transfer_out": ["709"],   # eChannel Transfer Out
    "atm_withdrawal": ["708"],     # ATM Withdrawal
    "billpay_utilities": ["719"],  # eChannel Payment out (utilities: easipay/phone credits/etc)
    "pos_spend": ["729"],          # ATM POS Payment Out
}
OUTFLOW_CODE_SET = set(sum(OUTFLOW_CODES.values(), []))


# -------------------------------------------------------------------
# Text helpers
# -------------------------------------------------------------------
def _upper(s: Any) -> str:
    return str(s or "").upper().strip()


def _salary_keyword_match(text: str) -> bool:
    if not isinstance(text, str):
        return False
    t = text.upper()
    keywords = [
        "SALARY",
        "WAGES",
        "PAYROLL",
        "WAGE",
        "EARNINGS",
        "ALLOWANCE",
        "FORTNIGHT",
        "FN PAY",
        "PAY",
    ]
    return any(k in t for k in keywords)


def _utility_or_rent_keyword_match(text: str) -> bool:
    t = _upper(text)
    keywords = [
        "EASIPAY",
        "EASY PAY",
        "BILL",
        "UTILITY",
        "ELECTRIC",
        "PNG POWER",
        "WATER",
        "TELIKOM",
        "DIGICEL",
        "BSP",
        "TOPUP",
        "TOP UP",
        "PHONE",
        "CREDIT",
        "AIRTIME",
        "RENT",
        "BOARD",
        "BOARDING",
        "LANDLORD",
        "LEASE",
        "TENANCY",
    ]
    return any(k in t for k in keywords)


def _coerce_date(df: pd.DataFrame) -> pd.Series:
    if "DATE" in df.columns:
        s = pd.to_datetime(df["DATE"], errors="coerce")
        if s.notna().any():
            return s
    if "DATE_STR" in df.columns:
        s = pd.to_datetime(df["DATE_STR"], errors="coerce")
        return s
    return pd.Series([pd.NaT] * len(df), index=df.index)


def _compute_intervals(dates: List[pd.Timestamp]) -> List[int]:
    dates = sorted([d for d in dates if pd.notna(d)])
    if len(dates) < 2:
        return []
    return [(dates[i] - dates[i - 1]).days for i in range(1, len(dates))]


def _amount_stability_score(amounts: pd.Series) -> float:
    a = pd.to_numeric(amounts, errors="coerce").dropna()
    a = a[a > 0]
    if len(a) < 2:
        return 0.0

    med = float(np.median(a))
    if med <= 0:
        return 0.0

    mad = float(np.median(np.abs(a - med)))
    ratio = mad / med
    if ratio <= 0.05:
        return 1.0
    if ratio <= 0.20:
        return float(max(0.0, 1.0 - (ratio - 0.05) / 0.15 * 0.5))
    if ratio <= 0.50:
        return float(max(0.0, 0.5 - (ratio - 0.20) / 0.30 * 0.5))
    return 0.0


def _cadence_score(intervals: List[int]) -> Tuple[str, float, Dict[str, Any]]:
    if not intervals:
        return "none", 0.0, {"intervals": intervals, "fortnight_hits": 0, "monthly_hits": 0}

    fortnight_hits = sum(13 <= d <= 16 for d in intervals)
    monthly_hits = sum(26 <= d <= 33 for d in intervals)

    n = max(1, len(intervals))
    fortnight_ratio = fortnight_hits / n
    monthly_ratio = monthly_hits / n

    cycle = "none"
    score = 0.0

    if fortnight_hits >= 2 and fortnight_ratio >= monthly_ratio:
        cycle = "fortnightly"
        score = min(1.0, 0.35 + 0.65 * fortnight_ratio)
    elif monthly_hits >= 2:
        cycle = "monthly"
        score = min(1.0, 0.30 + 0.70 * monthly_ratio)

    return cycle, float(score), {
        "intervals": intervals,
        "fortnight_hits": int(fortnight_hits),
        "monthly_hits": int(monthly_hits),
        "fortnight_ratio": round(float(fortnight_ratio), 3),
        "monthly_ratio": round(float(monthly_ratio), 3),
    }


def _priority_score_from_codes(codes: pd.Series) -> float:
    seen = set(str(x).strip() for x in codes.dropna().astype(str).tolist())
    hits = [c for c in SALARY_INFLOW_PRIORITY if c in seen]
    if not hits:
        return 0.0

    weights = []
    for c in hits:
        idx = SALARY_INFLOW_PRIORITY.index(c)
        w = 1.0 - (idx / max(1, len(SALARY_INFLOW_PRIORITY) - 1)) * 0.6
        weights.append(w)

    return float(min(1.0, max(weights)))


def _extract_sources(df: pd.DataFrame) -> List[str]:
    sources: List[str] = []
    if "IDENTITY" in df.columns:
        vals = df["IDENTITY"].dropna().astype(str).map(lambda x: x.strip()).tolist()
        vals = [v for v in vals if v and v.upper() != "UNKNOWN"]
        if vals:
            vc = pd.Series(vals).value_counts()
            sources = vc.index[:5].tolist()
            return sources

    def extract_from(desc: Any) -> Optional[str]:
        t = _upper(desc)
        m = re.search(r"\bFROM\s+([A-Z0-9 .&\-]+?)(\bTO\b|$)", t)
        if m:
            s = " ".join(m.group(1).split())
            return s[:60] if s else None
        return None

    vals2 = df.get("DESCRIPTION_RAW", pd.Series([], dtype=str)).apply(extract_from).dropna().tolist()
    vals2 = [v for v in vals2 if v]
    if vals2:
        vc = pd.Series(vals2).value_counts()
        sources = vc.index[:5].tolist()
    return sources


def _outflow_profile(df: pd.DataFrame) -> Dict[str, Any]:
    if "DEBIT" not in df.columns:
        return {"total_debit": 0.0, "shares_pct": {}, "utilities_or_rent_recurrence_hint": False}

    deb = df.copy()
    deb["DEBIT"] = pd.to_numeric(deb["DEBIT"], errors="coerce").fillna(0.0)
    deb = deb[deb["DEBIT"] > 0].copy()
    if deb.empty:
        return {"total_debit": 0.0, "shares_pct": {}, "utilities_or_rent_recurrence_hint": False}

    deb["CODE"] = deb.get("TRANSCODE", "").astype(str).str.strip()
    total_debit = float(deb["DEBIT"].sum())

    bucket_sums: Dict[str, float] = {}
    for bucket, codes in OUTFLOW_CODES.items():
        s = float(deb.loc[deb["CODE"].isin(codes), "DEBIT"].sum())
        bucket_sums[bucket] = s

    kw_hits = 0
    if "DESCRIPTION_RAW" in deb.columns:
        kw_hits = int(deb["DESCRIPTION_RAW"].astype(str).apply(_utility_or_rent_keyword_match).sum())

    shares_pct = {
        k: round((v / total_debit) * 100.0, 2) if total_debit else 0.0
        for k, v in bucket_sums.items()
    }

    return {
        "total_debit": round(total_debit, 2),
        "bucket_sums": {k: round(v, 2) for k, v in bucket_sums.items()},
        "shares_pct": shares_pct,
        "utilities_or_rent_recurrence_hint": bool(kw_hits >= 2),
        "utilities_or_rent_keyword_hits": kw_hits,
    }


def _salary_wash_flag(df: pd.DataFrame, salary_tx: pd.DataFrame) -> bool:
    if salary_tx.empty:
        return False

    dts = _coerce_date(df)
    if dts.isna().all():
        return False

    work = df.copy()
    work["DATE__"] = dts
    work["CODE"] = work.get("TRANSCODE", "").astype(str).str.strip()
    work["CREDIT"] = pd.to_numeric(work.get("CREDIT", 0.0), errors="coerce").fillna(0.0)
    work["DEBIT"] = pd.to_numeric(work.get("DEBIT", 0.0), errors="coerce").fillna(0.0)

    out_mask = (work["DEBIT"] > 0) & (work["CODE"].isin(OUTFLOW_CODE_SET))
    out = work[out_mask].copy()
    if out.empty:
        return False

    salary_tx = salary_tx.copy()
    salary_tx["DATE__"] = _coerce_date(salary_tx)
    salary_tx["CREDIT"] = pd.to_numeric(salary_tx.get("CREDIT", 0.0), errors="coerce").fillna(0.0)

    for _, srow in salary_tx.iterrows():
        sdate = srow.get("DATE__")
        samt = float(srow.get("CREDIT") or 0.0)
        if pd.isna(sdate) or samt <= 0:
            continue

        window = out[(out["DATE__"] >= sdate) & (out["DATE__"] <= (sdate + pd.Timedelta(days=2)))]
        if window.empty:
            continue

        out_sum = float(window["DEBIT"].sum())
        if out_sum >= 0.70 * samt:
            return True

    return False


def _salary_wash_row_ids(df: pd.DataFrame, salary_tx: pd.DataFrame) -> List[int]:
    """
    Row-level support for listing:
    Collect outflow rows (709/708/719/729) within 0–2 days of salary credits where
    outflows consume >=70% of salary.

    Does NOT affect scoring/triggering.
    """
    if salary_tx.empty or "ROW_ID" not in df.columns:
        return []

    dts = _coerce_date(df)
    if dts.isna().all():
        return []

    work = df.copy()
    work["DATE__"] = dts
    work["CODE"] = work.get("TRANSCODE", "").astype(str).str.strip()
    work["CREDIT"] = pd.to_numeric(work.get("CREDIT", 0.0), errors="coerce").fillna(0.0)
    work["DEBIT"] = pd.to_numeric(work.get("DEBIT", 0.0), errors="coerce").fillna(0.0)

    out_mask = (work["DEBIT"] > 0) & (work["CODE"].isin(OUTFLOW_CODE_SET))
    out = work[out_mask].copy()
    if out.empty:
        return []

    salary_tx = salary_tx.copy()
    salary_tx["DATE__"] = _coerce_date(salary_tx)
    salary_tx["CREDIT"] = pd.to_numeric(salary_tx.get("CREDIT", 0.0), errors="coerce").fillna(0.0)

    flagged: Set[int] = set()

    for _, srow in salary_tx.iterrows():
        sdate = srow.get("DATE__")
        samt = float(srow.get("CREDIT") or 0.0)
        if pd.isna(sdate) or samt <= 0:
            continue

        window = out[(out["DATE__"] >= sdate) & (out["DATE__"] <= (sdate + pd.Timedelta(days=2)))]
        if window.empty:
            continue

        out_sum = float(window["DEBIT"].sum())
        if out_sum >= 0.70 * samt:
            for rid in window["ROW_ID"].dropna().astype(int).tolist():
                flagged.add(int(rid))

    return sorted(flagged)


# -------------------------------------------------------------------
# MAIN DETECTOR
# -------------------------------------------------------------------
def detect_salary_pattern(df: pd.DataFrame) -> Dict[str, Any]:
    df = df.copy()

    # Preserve original row ids for downstream listing
    if "ROW_ID" not in df.columns:
        df["ROW_ID"] = df.index

    # Guard rails
    if "CREDIT" not in df.columns:
        df["CREDIT"] = 0.0
    if "DEBIT" not in df.columns:
        df["DEBIT"] = 0.0
    if "TRANSCODE" not in df.columns:
        df["TRANSCODE"] = "UNKNOWN"
    if "DESCRIPTION_RAW" not in df.columns:
        df["DESCRIPTION_RAW"] = ""

    df["CREDIT"] = pd.to_numeric(df["CREDIT"], errors="coerce").fillna(0.0)
    df["DEBIT"] = pd.to_numeric(df["DEBIT"], errors="coerce").fillna(0.0)

    df["DATE__"] = _coerce_date(df)

    credits = df[df["CREDIT"] > 0].copy()
    if credits.empty:
        return {
            "triggered": False,
            "strength": 0.0,
            "cycle": "none",
            "salary_sources": [],
            "indicators": ["No credits found in statement period."],
            "outflow_profile": _outflow_profile(df),
            "salary_wash_flag": False,
            "flagged_row_ids": [],
            "raw": {"salary_transactions_count": 0},
        }

    credits["CODE"] = credits["TRANSCODE"].astype(str).str.strip()

    # STEP 1 — Salary candidates
    credits["IS_SALARY_CODE"] = credits["CODE"].isin(SALARY_INFLOW_SET)
    credits["IS_SALARY_KEYWORD"] = credits["DESCRIPTION_RAW"].astype(str).apply(_salary_keyword_match)

    credits["AMT_ROUND"] = credits["CREDIT"].round(-2)
    amt_counts = credits["AMT_ROUND"].value_counts()
    recurring_amounts = amt_counts[amt_counts >= 2].index.tolist()
    credits["IS_STABLE_AMOUNT"] = credits["AMT_ROUND"].isin(recurring_amounts)

    credits["SALARY_CANDIDATE"] = (
        credits["IS_SALARY_CODE"]
        | (credits["IS_SALARY_KEYWORD"] & credits["IS_STABLE_AMOUNT"])
        | (credits["IS_STABLE_AMOUNT"])
    )

    salary_df = credits[credits["SALARY_CANDIDATE"]].copy()
    if salary_df.empty:
        return {
            "triggered": False,
            "strength": 0.0,
            "cycle": "none",
            "salary_sources": [],
            "indicators": ["No salary-like credit candidates found (codes/keywords/recurrence)."],
            "outflow_profile": _outflow_profile(df),
            "salary_wash_flag": False,
            "flagged_row_ids": [],
            "raw": {
                "salary_transactions_count": 0,
                "recurring_amounts": recurring_amounts,
            },
        }

    # STEP 2 — Cadence
    salary_dates = list(salary_df["DATE__"].dropna())
    intervals = _compute_intervals(salary_dates)
    cycle, cadence_score, cadence_raw = _cadence_score(intervals)

    # STEP 3 — Sources
    sources = _extract_sources(salary_df)

    # STEP 4 — Amount stability
    stability = _amount_stability_score(salary_df["CREDIT"])

    # STEP 5 — Code priority score
    code_prior = _priority_score_from_codes(salary_df["CODE"])

    # STEP 6 — Final score
    score = 0.0
    score += 0.45 * code_prior
    score += 0.30 * cadence_score
    score += 0.20 * stability

    kw_hits = int(salary_df["IS_SALARY_KEYWORD"].sum())
    if kw_hits >= 2:
        score += 0.10
    elif kw_hits == 1:
        score += 0.05

    score = float(min(1.0, max(0.0, score)))

    # STEP 7 — Outflow profile + wash flag
    out_profile = _outflow_profile(df)
    wash = _salary_wash_flag(df, salary_df[["DATE__", "CREDIT", "TRANSCODE", "DESCRIPTION_RAW"]])

    # Recognised salary rows are kept separately from suspicious rows.
    recognized_row_ids: List[int] = []
    try:
        recognized_row_ids = sorted(set(int(rid) for rid in salary_df["ROW_ID"].dropna().astype(int).tolist()))
    except Exception:
        recognized_row_ids = []

    # Suspicious rows are only produced when salary-wash behaviour is present.
    flagged: Set[int] = set()
    if wash:
        for rid in recognized_row_ids:
            flagged.add(int(rid))
        for rid in _salary_wash_row_ids(df, salary_df[["DATE__", "CREDIT", "TRANSCODE", "DESCRIPTION_RAW", "ROW_ID"]]):
            flagged.add(int(rid))

    flagged_row_ids = sorted(flagged)

    # Indicators
    indicators: List[str] = []

    seen_codes = sorted(set(salary_df["CODE"].astype(str).tolist()))
    inflow_hits = [c for c in SALARY_INFLOW_PRIORITY if c in seen_codes]
    if inflow_hits:
        indicators.append(
            "Salary-like inflow channels detected via transaction codes (priority order): "
            + ", ".join(inflow_hits)
            + "."
        )

    if kw_hits > 0:
        indicators.append("Salary-related keywords detected in credit narratives (salary/wages/payroll/allowance).")

    if recurring_amounts:
        indicators.append("Stable recurring credit amounts detected (rounded grouping), consistent with payroll behaviour.")

    if cycle == "fortnightly":
        indicators.append("Fortnightly payroll cadence detected (13–16 day intervals between recurring credits).")
    elif cycle == "monthly":
        indicators.append("Monthly payroll cadence detected (26–33 day intervals between recurring credits).")
    else:
        indicators.append("No strong fortnightly/monthly cadence detected among salary-like credits (may be irregular or short period).")

    if sources:
        indicators.append("Likely salary sources/counterparties (IDENTITY/description): " + ", ".join(sources) + ".")

    shares = (out_profile or {}).get("shares_pct") or {}
    if shares:
        indicators.append(
            "Outflow composition (% of total debits): "
            + ", ".join([f"{k}={v}%" for k, v in shares.items()])
            + "."
        )

    if out_profile.get("utilities_or_rent_recurrence_hint"):
        indicators.append("Recurring utilities/rent-like payments hinted by narrative keywords (utilities/rent/boarding).")

    if wash:
        indicators.append("Potential salary-wash behaviour: large outflows within 0–2 days after salary-like credits.")

    triggered = bool(score >= 0.35)

    return {
        "triggered": triggered,
        "strength": round(score, 3),
        "cycle": cycle,
        "salary_sources": sources,
        "indicators": indicators,
        "outflow_profile": out_profile,
        "salary_wash_flag": bool(wash),
        "recognition_type": "legitimate_sof_candidate",
        "recognized_row_ids": recognized_row_ids,
        "flagged_row_ids": flagged_row_ids,
        "raw": {
            "salary_transactions_count": int(len(salary_df)),
            "salary_candidate_codes_seen": seen_codes,
            "intervals": intervals,
            "cadence_breakdown": cadence_raw,
            "recurring_amounts_rounded": recurring_amounts,
            "keyword_hits": int(kw_hits),
            "code_prior_score": round(float(code_prior), 3),
            "amount_stability_score": round(float(stability), 3),
        },
    }
