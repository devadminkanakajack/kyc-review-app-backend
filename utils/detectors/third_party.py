"""
third_party.py
==============

Detects large presence of unrelated third parties depositing funds.
"""

from typing import Dict, Any, List
import pandas as pd
from collections import Counter


def detect_third_party(df: pd.DataFrame) -> Dict[str, Any]:
    df = df.copy()

    # Preserve original row ids for downstream listing
    if "ROW_ID" not in df.columns:
        df["ROW_ID"] = df.index

    df["CREDIT"] = pd.to_numeric(df.get("CREDIT", 0), errors="coerce").fillna(0.0)
    df["IDENTITY"] = df.get("IDENTITY", "").astype(str)

    total_credits = float(df["CREDIT"].sum())

    credit_df = df[df["CREDIT"] > 0].copy()

    # Treat blanks / "None" as missing identities
    credit_df["IDENTITY_CLEAN"] = credit_df["IDENTITY"].replace(
        {"None": None, "nan": None, "NaN": None, "": None}
    )
    credit_identities = credit_df["IDENTITY_CLEAN"].dropna().astype(str).tolist()

    # ------------------------------------------------------------
    # NO IDENTIFIED IDENTITIES → SAFE FALSE RETURN
    # ------------------------------------------------------------
    if not credit_identities:
        return {
            "triggered": False,
            "strength": 0.0,
            "indicators": ["No identified third-party credit behaviour present."],
            "flagged_row_ids": [],
            "raw": {},
            "pattern": None,
        }

    counter = Counter(credit_identities)
    unique_sources = len(counter)
    total_credit_txns = len(credit_identities)

    # Dominant source stats (original logic preserved)
    most_common_identity, most_common_count = counter.most_common(1)[0]

    # Third-party credit ratio = deposits NOT from dominant counterparty
    third_party_ratio = 1 - (most_common_count / max(1, total_credit_txns))

    # Weighted by diversity of identities
    identity_density = unique_sources / max(1, total_credit_txns)

    # ------------------------------------------------------------
    # ORIGINAL SCORING (UNCHANGED)
    # ------------------------------------------------------------
    score = (0.7 * third_party_ratio) + (0.3 * identity_density)
    strength = max(0.0, min(float(score), 1.0))

    # ------------------------------------------------------------
    # Row-level listing support
    # ------------------------------------------------------------
    # Flag credits where identity is present AND not the dominant identity.
    # (This keeps listing focused on true "third-party" contributors.)
    flagged_row_ids: List[int] = []
    try:
        non_dominant = credit_df[
            credit_df["IDENTITY_CLEAN"].notna()
            & (credit_df["IDENTITY_CLEAN"].astype(str) != str(most_common_identity))
        ]
        flagged_row_ids = [int(x) for x in non_dominant["ROW_ID"].dropna().astype(int).tolist()]
        flagged_row_ids = sorted(set(flagged_row_ids))
    except Exception:
        flagged_row_ids = []

    # ------------------------------------------------------------
    # 🔧 NEW OBSERVABILITY METRICS (NO LOGIC CHANGE)
    # ------------------------------------------------------------
    top_5 = counter.most_common(5)
    top_10 = counter.most_common(10)

    top_5_share = sum(c for _, c in top_5) / total_credit_txns if total_credit_txns > 0 else None
    top_10_share = sum(c for _, c in top_10) / total_credit_txns if total_credit_txns > 0 else None

    single_use_sources = sum(1 for c in counter.values() if c == 1)
    single_use_ratio = single_use_sources / unique_sources if unique_sources > 0 else None

    hhi = sum((count / total_credit_txns) ** 2 for count in counter.values())

    value_by_identity = credit_df.groupby("IDENTITY_CLEAN")["CREDIT"].sum()
    dominant_value = float(value_by_identity.max())
    dominant_value_share = dominant_value / total_credits if total_credits > 0 else None

    indicators: List[str] = []

    if third_party_ratio >= 0.5:
        indicators.append(f"High third-party deposit ratio ({third_party_ratio:.0%}).")

    if identity_density >= 0.3:
        indicators.append(f"Many unique depositors ({unique_sources} different identities).")

    if single_use_sources >= 5 and single_use_ratio is not None and single_use_ratio >= 0.3:
        indicators.append(
            f"High proportion of single-time depositors ({single_use_sources} one-off contributors)."
        )

    if dominant_value_share is not None and dominant_value_share <= 0.1:
        indicators.append(
            "No dominant funding source by value; inflows are widely dispersed across many parties."
        )

    if not indicators:
        indicators.append("No strong third-party anomalies detected.")

    return {
        "triggered": strength >= 0.35,
        "strength": round(strength, 3),
        "indicators": indicators,
        "flagged_row_ids": flagged_row_ids,
        "raw": {
            # Original metrics (preserved)
            "unique_sources": unique_sources,
            "identity_density": identity_density,
            "third_party_ratio": third_party_ratio,
            "counter": counter,

            # 🔧 New metrics
            "total_credit_txns": total_credit_txns,
            "most_common_identity": most_common_identity,
            "most_common_count": most_common_count,
            "top_5_share": top_5_share,
            "top_10_share": top_10_share,
            "single_use_sources": single_use_sources,
            "single_use_ratio": single_use_ratio,
            "hhi_concentration": hhi,
            "dominant_value": dominant_value,
            "dominant_value_share": dominant_value_share,
        },
        "pattern": "third_party",
    }
