from __future__ import annotations

from utils.kyc_profile.schema import KYCProfile
from utils.kyc_rules import KYC_RULEBOOK, BEHAVIOUR_MATRIX


PROFILE = KYCProfile(
    profile_id="INDIVIDUAL_NON_EMPLOYED_GENERIC",
    label="Individual - Non-Employed (Generic)",
    customer_type="INDIVIDUAL",
    segment="NON_EMPLOYED",
    description=(
        "Non-employed individual funded by cash deposits (informal market takings/savings), "
        "pension/superannuation (regular or lump sum), and occasional one-off SME payments. "
        "Spending is personal/household focused."
    ),
    baseline_risk=30,

    source_of_funds=[
        "Cash deposits (informal market takings / informal savings)",
        "Retirement pension funds (monthly/fortnightly)",
        "Superannuation payments (fortnightly/monthly OR one-off lump sum)",
        "SME payments (one-off credits)",
        "Occasional domestic transfers (family support)",
    ],

    usage_of_funds=[
        "POS merchant spending (household, groceries, fuel, retail)",
        "Utilities and rent payments",
        "E-channel transfers for family/friends assistance",
        "ATM withdrawals (cash needs)",
        "Lifestyle spending",
        "One-off investments or asset purchases (significant amounts)",
    ],

    expected_patterns={
        "channels_alias": {
            "cash_deposits_high": ["Cash Deposit"],
            "pension_or_super_regular": ["Pension", "Superannuation"],
            "pension_or_super_lump_sum": ["Superannuation Lump Sum"],
            "direct_credits_low_to_medium": ["Direct Credit", "Transfers In"],
            "pos_purchases": ["POS", "EFTPOS"],
            "atm_withdrawals": ["ATM", "Cash Withdrawal"],
            "e_channel_transfers_out": ["Direct Credit", "Transfers", "Telebanking"],
            "bill_payments": ["Bill Payment"],
        },
        "inflows": {
            "expected_channels": [
                "cash_deposits_high",
                "pension_or_super_regular",
                "pension_or_super_lump_sum",
                "direct_credits_low_to_medium",
                "sme_payment_oneoff",
            ],
            "regularity": {
                "expected_monthly_income_presence": "medium",
                "expected_income_variance_pct_max": 250,
                "notes": "Non-employed can be irregular; focus on structuring, outliers, and unexplained large credits."
            },
            "cash_intensity": {
                "cash_deposit_share_pct_max": 95,
                "notes": "Cash-heavy can be normal here, but structuring and unexplained spikes are key risks."
            },
        },
        "outflows": {
            "expected_channels": [
                "pos_purchases",
                "atm_withdrawals",
                "bill_payments",
                "e_channel_transfers_out",
                "direct_debits_low",
            ],
        },
        "behaviour_matrix_reference": BEHAVIOUR_MATRIX.get("Individual", {}),
    },

    mismatch_rules=[
        {
            "id": "international_activity_unusual",
            "when": {"feature": "intl_transfer_count_monthly", "op": ">", "value": 1},
            "risk_points": 18,
            "rationale": "International transfer activity is unusual for non-employed profile without clear supporting explanation."
        },
        {
            "id": "large_outlier_credit",
            "when": {"feature": "single_credit_max_multiple_of_median", "op": ">", "value": 25},
            "risk_points": 20,
            "rationale": "Unusually large one-off credit relative to typical pattern; confirm source-of-funds (e.g., super lump sum, asset sale)."
        },
        {
            "id": "cash_structuring_detected",
            "when": {"feature": "cash_structuring_flag", "op": "==", "value": True},
            "risk_points": 25,
            "rationale": "Structured cash deposit behavior detected; requires enhanced scrutiny and evidence of legitimate cash income."
        },
        {
            "id": "pass_through_behavior",
            "when": {"feature": "rapid_in_out_ratio", "op": ">", "value": 0.9},
            "risk_points": 20,
            "rationale": "Very high pass-through pattern is inconsistent with household-spend profile; may indicate facilitation for third parties."
        },
    ],

    tags=["individual", "non_employed", "cash_heavy", "pension", "superannuation"],

    expected_behaviour=KYC_RULEBOOK["individual"]["expected_behaviour"],
    high_risk_behaviour=KYC_RULEBOOK["individual"]["prohibited_or_high_risk_behaviour"],
)
