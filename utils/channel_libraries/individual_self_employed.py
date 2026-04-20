CHANNEL_LIBRARY = {
    "profile": "Individual - Self Employed",

    "credit_rules": {
        "BUSINESS_PROCEEDS": {
            "keywords": ["cash deposit", "takings", "sales", "proceeds", "customer payment", "invoice", "receipt"],
            "codes": [101, 102, 189, 703],
            "sof": "Business income",
            "risk": "medium",
            "notes": "Expected where activity aligns with declared self-employed profile and counterparties are commercially explainable.",
        },
        "COMMISSION_DIVIDEND": {
            "keywords": ["dividend", "commission", "incentive", "rebate"],
            "codes": [189, 703],
            "sof": "Business returns",
            "risk": "low",
        },
        "FAMILY_SUPPORT": {
            "keywords": ["family support", "assistance", "support", "transfer from"],
            "codes": [703],
            "sof": "Family support / remittance",
            "risk": "medium",
        },
    },

    "debit_rules": {
        "SUPPLIER_PAYMENTS": {
            "keywords": ["supplier", "invoice", "stock", "materials", "payable"],
            "codes": [709],
            "pof": "Business expenses",
            "risk": "low",
        },
        "BUSINESS_CASH_USAGE": {
            "keywords": ["cash withdrawal", "atm", "cash out"],
            "codes": [708],
            "pof": "Business cash usage",
            "risk": "medium",
        },
        "HOUSEHOLD_AND_BILLS": {
            "keywords": ["rent", "power", "water", "utility", "easipay", "digicel", "telikom", "pos"],
            "codes": [719, 729],
            "pof": "Mixed business / personal expenses",
            "risk": "medium",
        },
    },
}
