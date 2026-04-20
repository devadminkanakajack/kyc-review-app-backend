CHANNEL_LIBRARY = {
    "profile": "Non-Individual - Generic Company",

    "credit_rules": {
        "BUSINESS_REVENUE": {
            "keywords": ["invoice payment", "customer payment", "sales receipt", "sales", "receipt", "contract payment", "proceeds", "service payment"],
            "codes": [189, 703, 101, 102],
            "sof": "Business revenue",
            "risk": "low",
            "notes": "Expected where inflows align with the entity's declared operations.",
        },
        "CAPITAL_OR_LOAN": {
            "keywords": ["loan", "finance", "capital", "director advance", "shareholder advance", "injection"],
            "codes": [189, 703],
            "sof": "Capital / loan proceeds",
            "risk": "medium",
        },
    },

    "debit_rules": {
        "SUPPLIER_INVOICES": {
            "keywords": ["supplier", "invoice", "payable", "materials", "stock", "contractor"],
            "codes": [709],
            "pof": "Operating expenses",
            "risk": "low",
        },
        "PAYROLL": {
            "keywords": ["salary", "payroll", "wages", "allowance"],
            "codes": [189, 709],
            "pof": "Staff costs",
            "risk": "low",
        },
        "STATUTORY_AND_BILLS": {
            "keywords": ["tax", "gst", "ipa", "rent", "utility", "power", "water", "digicel", "telikom"],
            "codes": [719],
            "pof": "Statutory / operating costs",
            "risk": "low",
        },
        "PETTY_CASH_OR_ATM": {
            "keywords": ["atm", "cash withdrawal", "cash out"],
            "codes": [708],
            "pof": "Cash usage",
            "risk": "medium",
            "notes": "Needs context for non-cash-intensive businesses.",
        },
    },
}
