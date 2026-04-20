# utils/kyc_rules.py

from typing import Dict, Any, List

# ============================================================
# REQUIRED AI NARRATIVE SECTIONS (ONE SOURCE OF TRUTH)
# ============================================================

REQUIRED_HEADINGS: List[str] = [
    "Credit Rationale",
    "Debit Rationale",
    "Summary of Both Rationales",
    "Overview and Background of Review",
]

# ============================================================
# KYC COMPLIANCE RULEBOOK (LOGIC LAYER)
# ============================================================

KYC_RULEBOOK: Dict[str, Any] = {
    "individual": {
        "label": "Individual",
        "account_purpose": [
            "Receive salary and employment income",
            "Receive informal sector income (market sales, side jobs)",
            "Personal savings and household expenses",
        ],
        "expected_behaviour": [
            "Salary or regular employment credits as primary SOF",
            "Reasonable number of ATM / POS withdrawals for living expenses",
            "Retail, utilities, fuel and normal cost-of-living transactions",
            "Limited and stable set of counterparties over time",
        ],
        "prohibited_or_high_risk_behaviour": [
            "Multiple large cash or third-party credits inconsistent with salary level",
            "Frequent round-figure deposits and withdrawals (structuring indicators)",
            "Acting as a pass-through or funnel for multiple unrelated parties",
            "Significant business-like patterns (supplier payments, bulk payments)",
            "Rapid in–out flows where funds leave shortly after arrival",
        ],
    },
    "non_individual": {
        "label": "Non-Individual",
        "account_purpose": [
            "Receive business revenue",
            "Pay suppliers, salaries, rent and other operating expenses",
            "Hold working capital and reserves",
        ],
        "expected_behaviour": [
            "Credits consistent with declared business activities and industry",
            "Payments to suppliers, rent, utilities, staff, statutory obligations",
            "Limited cash withdrawals if business is not cash-intensive",
            "Clear segregation between business funds and personal expenses",
        ],
        "prohibited_or_high_risk_behaviour": [
            "Personal spending patterns (retail POS, entertainment, gambling)",
            "Regular ATM withdrawals with no legitimate business need",
            "Incoming funds from many unrelated personal accounts",
            "Circular transfers through related parties without economic purpose",
            "Use as a pass-through account for third-party transactions",
        ],
    },
}

# ============================================================
# BEHAVIOUR MATRIX (USED BY AI + RISK ENGINE)
# ============================================================

BEHAVIOUR_MATRIX: Dict[str, Any] = {
    "Individual": {
        "expected_channels": ["Salary", "Direct Credit", "ATM", "POS", "EFTPOS"],
        "red_flag_patterns": [
            "High volume of third-party credits",
            "Large or frequent cash deposits inconsistent with profile",
            "Rapid credit → debit movements (same day / next day)",
            "Fixed-value repeating withdrawals (e.g. K 5,020 x many days)",
            "Multiple outward transfers to unrelated accounts",
        ],
    },
    "Non-Individual": {
        "expected_channels": ["Direct Credit", "Business Transfers", "Payroll", "EFT"],
        "red_flag_patterns": [
            "Personal ATM / POS usage from business account",
            "Credits from employees’ or random personal accounts",
            "Unusual number of cash withdrawals",
            "Circular transfers to and from related entities with no invoices",
            "Use of business account as a conduit for third-party funds",
        ],
    },
}

# ============================================================
# HELPER: DETERMINE PROFILE KEY
# ============================================================

def normalize_client_type(client_type: str) -> str:
    """
    Map client_type string to keys used in KYC_RULEBOOK.
    Expected client_type values: "Individual", "Non-Individual", etc.
    """
    if not client_type:
        return "individual"

    ct = client_type.strip().lower()
    if "non" in ct or "company" in ct or "business" in ct:
        return "non_individual"
    return "individual"
