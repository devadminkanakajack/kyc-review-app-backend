CHANNEL_LIBRARY = {
    "profile": "Individual - Non Employed",

    "credit_rules": {
        "FAMILY_SUPPORT": {
            "keywords": ["transfer from", "family support", "family use", "assistance", "support", "remittance"],
            "codes": [703, 189],
            "sof": "Family support / remittance",
            "risk": "low",
        },
        "BENEFITS_PENSION": {
            "keywords": ["pension", "benefit", "super", "allowance", "grant"],
            "codes": [189, 703],
            "sof": "Benefits / pension income",
            "risk": "low",
        },
        "ONE_OFF_OTHER_INCOME": {
            "keywords": ["contract payment", "proceeds", "refund", "reimbursement"],
            "codes": [189, 703],
            "sof": "One-off income",
            "risk": "medium",
        },
    },

    "debit_rules": {
        "GENERAL_EXPENSES": {
            "keywords": ["pos", "purchase", "withdrawal", "atm", "easipay", "digicel", "power", "water"],
            "codes": [708, 719, 729],
            "pof": "Personal expenses",
            "risk": "low",
        },
        "PERSONAL_TRANSFERS": {
            "keywords": ["family", "support", "transfer out", "ib other acc", "mb trf", "echannel transfer out"],
            "codes": [709],
            "pof": "Personal transfers / family support",
            "risk": "low",
        },
    },
}
