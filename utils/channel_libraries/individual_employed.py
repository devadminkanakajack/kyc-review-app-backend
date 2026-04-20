CHANNEL_LIBRARY = {
    "profile": "Individual - Employed",

    "credit_rules": {
        "SALARY": {
            "keywords": [
                "salary", "payroll", "wages", "allowance", "stipend",
                "pay day", "fortnight", "fn pay", "earnings"
            ],
            "codes": [189, 198, 703],
            "sof": "Employment income",
            "risk": "low",
            "notes": "Expected for employed individuals where cadence and source align with declared salary or allowances.",
        },
        "EMPLOYER_REIMBURSEMENT": {
            "keywords": ["reimbursement", "refund", "medical refund", "travel refund", "expense claim"],
            "codes": [189, 703],
            "sof": "Employer reimbursements",
            "risk": "low",
            "notes": "Can be legitimate where linked to employer support or approved reimbursement.",
        },
        "FAMILY_SUPPORT": {
            "keywords": ["family support", "family use", "support", "assistance", "transfer from", "from"],
            "codes": [703, 189],
            "sof": "Family support / remittance",
            "risk": "medium",
            "notes": "Usually contextual rather than suspicious unless many unrelated senders or fast onward movement exist.",
        },
        "ONE_OFF_OTHER_INCOME": {
            "keywords": ["bonus", "advance", "contract", "refund", "proceeds"],
            "codes": [189, 703],
            "sof": "Other legitimate income",
            "risk": "medium",
            "notes": "Requires explanation where material or inconsistent with salary profile.",
        },
    },

    "debit_rules": {
        "ATM_CASH": {
            "keywords": ["atm withdrawal", "cash withdrawal", "atm", "westpac", "branch atm", "shpg center atm"],
            "codes": [708],
            "pof": "Cash usage / living expenses",
            "risk": "low",
            "notes": "Ordinary household cash usage for employed individuals unless unusually intense or immediately follows unexplained inflows.",
        },
        "FAMILY_TRANSFERS": {
            "keywords": ["family", "support", "transfer out", "ib other acc", "mb trf", "echannel transfer out"],
            "codes": [709],
            "pof": "Family support / personal transfers",
            "risk": "low",
            "notes": "Often profile-consistent for personal accounts unless part of pass-through behaviour.",
        },
        "RENT_UTILITIES": {
            "keywords": ["rent", "water", "power", "utility", "easipay", "digicel", "telikom", "phone", "airtime", "top up", "topup"],
            "codes": [719],
            "pof": "Household expenses",
            "risk": "low",
            "notes": "Expected bill-pay and household spending channel.",
        },
        "POS_SPEND": {
            "keywords": ["pos", "purchase", "store", "shop", "merchant", "payment out"],
            "codes": [729],
            "pof": "Living expenses",
            "risk": "low",
            "notes": "Expected retail spending channel for employed individuals.",
        },
    },
}
