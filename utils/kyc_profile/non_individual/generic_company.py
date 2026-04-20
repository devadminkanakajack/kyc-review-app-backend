from __future__ import annotations

from utils.kyc_profile.schema import KYCProfile
from utils.kyc_rules import KYC_RULEBOOK, BEHAVIOUR_MATRIX


PROFILE = KYCProfile(
    profile_id="NONIND_GENERIC_COMPANY",
    label="Non-Individual - Generic Company Profiling",
    customer_type="NON_INDIVIDUAL",
    segment="GENERIC_COMPANY",
    description=(
        "Generic company profiling focused on inflow channel detection and monthly regularity, "
        "and standard corporate outflows (rent, utilities, payroll, suppliers, capex, director fees/dividends)."
    ),
    baseline_risk=50,

    source_of_funds=[
        "Customer receipts (cash deposits, direct credits, transfers in, POS settlement, cheque deposits)",
        "Telebanking/e-channel transfers in",
        "Loan/overdraft disbursements (financing inflows)",
        "Asset sales proceeds (one-off credits)",
        "Refunds/adjustments (one-off credits)",
    ],

    usage_of_funds=[
        "Rent/lease payments",
        "Utilities (power, water, telecoms/internet)",
        "Payroll: salaries, wages, allowances, leave payments",
        "Statutory contributions/payments (e.g., superannuation, payroll-related statutory items where applicable)",
        "Supplier/invoice payments",
        "Fuel/transport/logistics",
        "Operating expenses (subscriptions, insurance, office supplies)",
        "Maintenance/repairs",
        "Capital expenditure (asset acquisition)",
        "Director fees / sitting allowances",
        "Dividends / shareholder distributions",
        "Loan repayments / interest / bank fees",
    ],

    expected_patterns={
        "channels_alias": {
            "cash_deposits": ["Cash Deposit"],
            "direct_credits": ["Direct Credit", "Transfers In"],
            "pos_settlements": ["POS Settlement", "EFTPOS Settlement"],
            "cheque_deposits": ["Cheque Deposit"],
            "supplier_invoice_payments": ["Transfers Out", "EFT", "Supplier Payment"],
            "payroll_and_allowances": ["Payroll", "Salary", "Wages"],
            "rent_utilities": ["Rent", "Utilities", "Bill Payment"],
            "atm_withdrawals_business": ["ATM", "Cash Withdrawal"],
        },
        "inflows": {
            "expected_channels": [
                "cash_deposits",
                "cheque_deposits",
                "direct_credits",
                "e_channel_transfers_in",
                "pos_settlements",
                "loan_disbursements",
                "asset_sale_proceeds_oneoff",
            ],
            "regularity": {
                "assess_monthly_presence": True,
                "assess_monthly_variance": True,
                "notes": "Compute monthly totals and measure stability/seasonality. Flag concentration and outliers."
            },
            "cash_intensity": {
                "cash_deposit_share_pct_expected": "varies",
                "risk_focus": "Flag structuring and unexplained cash dominance."
            },
        },
        "outflows": {
            "expected_channels": [
                "supplier_invoice_payments",
                "rent_utilities",
                "payroll_and_allowances",
                "e_channel_transfers_out",
                "direct_debits",
                "bill_payments",
                "capex_asset_purchases",
                "director_fees_dividends",
            ],
            "notes": "Look for recurring operating expense rhythm + periodic capex/distributions."
        },
        "behaviour_matrix_reference": BEHAVIOUR_MATRIX.get("Non-Individual", {}),
    },

    mismatch_rules=[
        {
            "id": "cash_structuring_detected",
            "when": {"feature": "cash_structuring_flag", "op": "==", "value": True},
            "risk_points": 25,
            "rationale": "Structured cash deposits detected; requires enhanced source-of-funds verification and reconciliation to sales records."
        },
        {
            "id": "excess_pass_through_layering",
            "when": {"feature": "rapid_in_out_ratio", "op": ">", "value": 0.9},
            "risk_points": 22,
            "rationale": "Very high pass-through activity may indicate layering or conduit behavior rather than genuine operating flows."
        },
        {
            "id": "no_recurring_operating_expenses_detected",
            "when": {"feature": "recurring_expenses_detected_flag", "op": "==", "value": False},
            "risk_points": 12,
            "rationale": "Recurring operating expenses (rent/utilities/payroll/suppliers) not clearly detected; verify whether this is the main operating account."
        },
        {
            "id": "high_risk_counterparty_flag",
            "when": {"feature": "high_risk_counterparty_flag", "op": "==", "value": True},
            "risk_points": 20,
            "rationale": "High-risk counterparty indicators observed; enhanced due diligence required."
        },
    ],

    tags=["non_individual", "generic_company", "channel_analysis", "regularity_stats"],

    expected_behaviour=KYC_RULEBOOK["non_individual"]["expected_behaviour"],
    high_risk_behaviour=KYC_RULEBOOK["non_individual"]["prohibited_or_high_risk_behaviour"],
)
