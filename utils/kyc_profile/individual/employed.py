# utils/kyc_profile/individual/employed.py

from __future__ import annotations

from utils.kyc_profile.schema import KYCProfile
from utils.kyc_rules import KYC_RULEBOOK, BEHAVIOUR_MATRIX


PROFILE = KYCProfile(
    profile_id="INDIVIDUAL_EMPLOYED_GENERIC",
    label="Individual - Employed (Generic)",
    customer_type="INDIVIDUAL",
    segment="EMPLOYED",
    description=(
        "Employed individual funded primarily by salary and allowances, with occasional one-off credits "
        "(investment returns, proceeds from asset sales). Spending is personal/household focused."
    ),
    baseline_risk=22,

    # -------------------------
    # Source of Funds (SoF)
    # -------------------------
    source_of_funds=[
        "Salary/Wages (regular payroll credits; monthly or fortnightly)",
        "Allowances (housing/rent, fuel, vehicle, communications, responsibility allowances)",
        "Return on investments (one-off credits)",
        "Proceeds from sales (one-off credits, e.g., asset sale)",
        "Occasional domestic support transfers (family)",
    ],

    # -------------------------
    # Usage of Funds (UoF) - personal/household
    # -------------------------
    usage_of_funds=[
        "POS merchant spending (groceries, fuel, retail, restaurants)",
        "Utilities and bill payments (eChannel payments: Easipay/eTopUp/Phone credits, etc.)",
        "Rent/boarding payments (usually recurring transfers)",
        "E-channel transfers (family/friends assistance, personal obligations)",
        "Lifestyle/discretionary spending",
        "ATM withdrawals (personal cash needs)",
        "One-off significant investments (personal)",
        "One-off asset purchases (vehicle, household assets)",
    ],

    # -------------------------
    # Expected channel patterns
    # -------------------------
    expected_patterns={
        # ------------------------------------------------------------------
        # IMPORTANT:
        # - code_lookup.get_description() prefers SAVINGS for Individuals but can fallback to CURRENT
        #   (because some “individual-like” POS channels exist under CURRENT account code families).
        # - These codes are your *priority* indicators for salary inflow and household outflow.
        # ------------------------------------------------------------------
        "transaction_code_priorities": {
            "salary_inflow_priority_codes": [
                # Highest priority salary signals (Savings)
                {"account_hint": "SAVINGS ACCOUNT", "code": "198", "label": "Salary Cheque (Employee)"},
                {"account_hint": "SAVINGS ACCOUNT", "code": "189", "label": "Direct Credit"},
                # Some banks/exports may classify certain inflow POS/ATM items under Current
                {"account_hint": "CURRENT ACCOUNT", "code": "723", "label": "ATM POS Payment In"},
                # Secondary inflow channels
                {"account_hint": "SAVINGS ACCOUNT", "code": "101", "label": "Cash Deposit With Book"},
                {"account_hint": "SAVINGS ACCOUNT", "code": "102", "label": "Cheque Deposit With Book"},
                # Least priority salary-like fallback
                {"account_hint": "SAVINGS ACCOUNT", "code": "703", "label": "ATM/TELEBANKING Transfer In"},
            ],
            "household_outflow_priority_codes": [
                # Utilities / bills (eChannel payment out)
                {"account_hint": "SAVINGS ACCOUNT", "code": "719", "label": "eChannel Payment Out (Easipay/Phone Credits/Utilities)"},
                # Transfers out (rent, boarding, obligations)
                {"account_hint": "SAVINGS ACCOUNT", "code": "709", "label": "eChannel Transfer Out (IB/MB/Telebanking)"},
                # Cash withdrawals
                {"account_hint": "SAVINGS ACCOUNT", "code": "708", "label": "ATM Withdrawal"},
                # POS merchant spending often appears in CURRENT code families
                {"account_hint": "CURRENT ACCOUNT", "code": "729", "label": "ATM POS Payment Out (Household/merchant spend)"},
            ],
        },

        # Alias layer for narrative grouping (used by reporting/doc-gen; not strict scoring)
        "channels_alias": {
            "salary_crediting": ["Salary", "Wages", "Payroll", "Direct Credit", "Salary Cheque"],
            "cash_deposits": ["Cash Deposit"],
            "cheque_deposits": ["Cheque Deposit", "Cheque"],
            "transfers_in": ["Transfer In", "Transfers In", "ATM/TELEBANKING Transfer In"],
            "atm_withdrawals": ["ATM", "Cash Withdrawal", "ATM Withdrawal"],
            "pos_purchases": ["POS", "EFTPOS", "ATM POS Payment Out"],
            "e_channel_transfers_out": ["eChannel Transfer Out", "IB", "MB", "Telebanking", "Online Transfer"],
            "bill_payments": ["eChannel Payment Out", "Easipay", "Utilities", "Phone Credits", "Bill Payment"],
        },

        # What we expect to see, at a high level (used to guide narrative)
        "inflows": {
            "expected_channels": [
                "salary_crediting",
                "cheque_deposits",
                "transfers_in",
                "cash_deposits",
            ],
            # For employed: recurring salary presence is important (monthly/fortnightly)
            "regularity": {
                "expected_monthly_income_presence": "high",
                "expected_income_variance_pct_max": 60,
                "notes": "Salary typically repeats on a cycle (monthly or fortnightly).",
            },
            "cash_intensity": {
                "cash_deposit_share_pct_max": 45,
                "notes": "Cash deposits can occur but should not dominate for an employed profile.",
            },
        },

        "outflows": {
            "expected_channels": [
                "pos_purchases",
                "atm_withdrawals",
                "bill_payments",
                "e_channel_transfers_out",
            ],
            "recurrence": [
                "utilities_monthly",
                "rent_monthly",
                "pos_spend_frequent",
            ],
        },

        "behaviour_matrix_reference": BEHAVIOUR_MATRIX.get("Individual", {}),
    },

    # -------------------------
    # Mismatch rules (pattern-to-declaration alignment)
    # These are NOT ML typologies; they score *inconsistency with declared profile*.
    # -------------------------
    mismatch_rules=[
        {
            "id": "no_salary_like_credits",
            "when": {"feature": "salary_like_credit_flag", "op": "==", "value": False},
            "risk_points": 18,
            "rationale": (
                "Expected salary/payroll-like credits are not observed for an employed profile; "
                "verify stated employment and how income is received."
            ),
        },
        {
            "id": "high_cash_deposit_share_for_employed",
            "when": {"feature": "cash_deposit_share_pct", "op": ">", "value": 60},
            "risk_points": 18,
            "rationale": (
                "High cash deposit share is inconsistent with typical salaried income; "
                "confirm source-of-funds and any side-business."
            ),
        },
        {
            "id": "turnover_excessive_for_declared_income",
            "when": {"feature": "turnover_multiple_of_declared_income", "op": ">", "value": 7.0},
            "risk_points": 20,
            "rationale": (
                "Account turnover appears unusually high relative to declared income; "
                "verify whether additional income sources or other legitimate drivers exist."
            ),
        },

        # UoF expectation: personal/household should show meaningful POS/ATM/bills presence.
        # If eChannel transfers dominate, the account may not match “personal spend” expectations.
        {
            "id": "echannel_outflow_dominates_household_profile",
            "when": {"feature": "echannel_transfer_out_share_pct", "op": ">", "value": 70},
            "risk_points": 14,
            "rationale": (
                "A very high share of outflows via eChannel transfers is not typical for a household-spend profile; "
                "confirm purpose of transfers (rent/boarding vs other obligations) and whether the account is used primarily to move funds."
            ),
        },
        {
            "id": "low_pos_and_atm_presence_for_household_profile",
            # “POS + ATM” combined should usually be meaningful for an employed personal account.
            # We implement as two checks (because the simple rule engine only evaluates one feature at a time).
            "when": {"feature": "pos_share_pct", "op": "<", "value": 10},
            "risk_points": 10,
            "rationale": (
                "POS spending share is unusually low for an employed household profile; "
                "verify whether spending occurs through other accounts or cash-heavy behaviour."
            ),
        },
        {
            "id": "low_atm_presence_for_household_profile",
            "when": {"feature": "atm_withdrawal_share_pct", "op": "<", "value": 5},
            "risk_points": 8,
            "rationale": (
                "ATM withdrawal share is unusually low for a household profile; "
                "confirm whether the customer is primarily cashless or uses other accounts."
            ),
        },
        {
            "id": "no_bill_payment_signature",
            "when": {"feature": "bill_payment_share_pct", "op": "<", "value": 1},
            "risk_points": 6,
            "rationale": (
                "Utilities/bill-payment signature is not observed; "
                "confirm whether bills are paid via another account or by cash/third parties."
            ),
        },
    ],

    tags=["individual", "employed", "salary", "allowances", "personal_spend"],

    # ✅ Policy hooks from kyc_rules.py (generic compliance framing)
    expected_behaviour=KYC_RULEBOOK["individual"]["expected_behaviour"],
    high_risk_behaviour=KYC_RULEBOOK["individual"]["prohibited_or_high_risk_behaviour"],
)
