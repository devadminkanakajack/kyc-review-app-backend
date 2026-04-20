from __future__ import annotations

from utils.kyc_profile.schema import KYCProfile
from utils.kyc_rules import KYC_RULEBOOK, BEHAVIOUR_MATRIX


PROFILE = KYCProfile(
    profile_id="INDIVIDUAL_SELF_EMPLOYED_GENERIC",
    label="Individual - Self-Employed (Generic)",
    customer_type="INDIVIDUAL",
    segment="SELF_EMPLOYED",
    description=(
        "Self-employed individual with mixed income sources (salary from own company, director fees, "
        "allowances, dividends, owner's drawings, and one-off proceeds/investment returns). "
        "Spending is expected to remain predominantly personal/non-business when using personal accounts."
    ),
    baseline_risk=35,

    source_of_funds=[
        "Salary (from own company, if paid as payroll)",
        "Director fees / sitting allowances (where applicable)",
        "Allowances (housing/rent, fuel, vehicle, communications)",
        "Owner’s withdrawals/drawings (transfers from business accounts)",
        "Proceeds from sales (one-off)",
        "Dividends (where applicable)",
        "Interest / withholding tax related credits (where applicable)",
        "Return on investments (one-off)",
        "Occasional domestic transfers (family/support)",
    ],

    usage_of_funds=[
        "POS merchant spending (household, fuel, retail, lifestyle)",
        "Utilities and rent payments",
        "ATM withdrawals",
        "E-channel transfers to family/friends (assistance/support)",
        "One-off significant investments (personal)",
        "One-off asset purchases (personal vehicle/household assets)",
    ],

    expected_patterns={
        "channels_alias": {
            "salary_like_credits_optional": ["Salary", "Wages", "Payroll"],
            "director_fee_like_credits_optional": ["Director Fees", "Sitting Allowance"],
            "dividend_like_credits_optional": ["Dividends", "Investment Income"],
            "direct_credits": ["Direct Credit", "Transfers In"],
            "cash_deposits_medium": ["Cash Deposit"],
            "pos_purchases": ["POS", "EFTPOS"],
            "atm_withdrawals": ["ATM", "Cash Withdrawal"],
            "e_channel_transfers_out": ["Direct Credit", "Transfers", "Telebanking"],
            "bill_payments": ["Bill Payment"],
        },
        "inflows": {
            "expected_channels": [
                "direct_credits",
                "cash_deposits_medium",
                "salary_like_credits_optional",
                "director_fee_like_credits_optional",
                "dividend_like_credits_optional",
                "asset_sale_proceeds_oneoff",
                "investment_return_oneoff",
            ],
            "regularity": {
                "expected_monthly_income_presence": "medium",
                "expected_income_variance_pct_max": 200,
                "notes": "Self-employed income can be irregular; focus on concentration, cash intensity, and structuring."
            },
            "cash_intensity": {
                "cash_deposit_share_pct_max": 70,
            },
        },
        "outflows": {
            "expected_channels": [
                "pos_purchases",
                "atm_withdrawals",
                "bill_payments",
                "e_channel_transfers_out",
                "direct_debits",
            ],
            "notes": "Using personal accounts: outflows should still look personal rather than supplier/payroll heavy.",
        },
        "behaviour_matrix_reference": BEHAVIOUR_MATRIX.get("Individual", {}),
    },

    mismatch_rules=[
        {
            "id": "supplier_or_payroll_pattern_in_personal_account",
            "when": {"feature": "merchant_supplier_pattern_flag", "op": "==", "value": True},
            "risk_points": 15,
            "rationale": "Spending resembles supplier/invoice settlement patterns; may indicate business activity running through personal account."
        },
        {
            "id": "payroll_distribution_pattern_detected",
            "when": {"feature": "payroll_distribution_pattern_flag", "op": "==", "value": True},
            "risk_points": 12,
            "rationale": "Outbound payments resemble payroll/allowance distributions; confirm whether personal account is used for business payroll."
        },
        {
            "id": "high_cash_structuring",
            "when": {"feature": "cash_structuring_flag", "op": "==", "value": True},
            "risk_points": 25,
            "rationale": "Structured cash deposit behavior detected."
        },
        {
            "id": "excess_pass_through",
            "when": {"feature": "rapid_in_out_ratio", "op": ">", "value": 0.9},
            "risk_points": 22,
            "rationale": "Very high pass-through activity may indicate layering or conduit behavior."
        },
        {
            "id": "high_risk_counterparties",
            "when": {"feature": "high_risk_counterparty_flag", "op": "==", "value": True},
            "risk_points": 20,
            "rationale": "High-risk counterparty indicators observed; enhanced due diligence required."
        },
    ],

    tags=["individual", "self_employed", "director_fees", "dividends", "mixed_income"],

    expected_behaviour=KYC_RULEBOOK["individual"]["expected_behaviour"],
    high_risk_behaviour=KYC_RULEBOOK["individual"]["prohibited_or_high_risk_behaviour"],
)
