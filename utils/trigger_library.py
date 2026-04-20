# utils/trigger_library.py

TRIGGER_LIBRARY = {
    "SECTION_81_NOTICE": {
        "label": "Section 81 Notice (Regulatory / Law Enforcement Request)",
        "definition": (
            "A formal notice/request for information issued under applicable law by a competent authority "
            "(e.g., central bank/regulator, anti-corruption body, law enforcement) requesting customer, "
            "account, transaction, beneficial ownership, or related records, typically linked to an inquiry "
            "or investigation into suspected financial crime, corruption, or serious misconduct."
        ),
        "typology_signals": [
            "proceeds of corruption",
            "misappropriation of public funds",
            "laundering through accounts/entities",
            "concealment of beneficial ownership",
            "possible asset tracing / restraint action"
        ],
        "risk_rating": {"ml": 4, "tf": 3},
        "rating_rationale": (
            "Credible, external authority escalation indicates elevated suspicion or investigation. "
            "Even without proof of ML/TF, regulatory/law enforcement involvement significantly increases "
            "integrity, legal, and predicate-crime laundering risk."
        ),
        "recommended_actions_min": [
            "Immediate ECDD refresh (CDD, BO/UBO, control structure)",
            "Source of Funds (SoF) + Source of Wealth (SoW) verification",
            "12-month transaction review + linked-party mapping",
            "Senior management sign-off",
            "Enhanced monitoring rules applied"
        ],
        # Optional UI helpers (safe additions)
        "focus_detectors": [
            "structured_deposits",
            "structured_payments",
            "pass_through",
            "layering",
            "third_party",
            "cash_intensive",
        ],
        "evidence_requests": [
            "Provide authority request scope confirmation and required disclosure timeline.",
            "Provide SoF/SoW documentation supporting key inflows/outflows (contracts, payslips, invoices, tax filings).",
            "Provide explanation for any detector-flagged transaction rows (per-channel listing).",
        ],
    },

    "SMR_INTERNAL_FIU_TM": {
        "label": "Suspicious Matter Report (Internal FIU / Transaction Monitoring Trigger)",
        "definition": (
            "An internal suspicious matter report or alert raised by transaction monitoring, analytics, "
            "or investigator review due to activity inconsistent with the customer's profile, expected behavior, "
            "or known economic purpose, suggesting possible money laundering, fraud, or terrorist financing indicators."
        ),
        "typology_signals": [
            "structuring/smurfing",
            "rapid movement / velocity (layering)",
            "unusual cash intensity",
            "third-party funneling/mules",
            "unexplained international transfers"
        ],
        "risk_rating": {"ml": 3, "tf": 3},
        "rating_rationale": (
            "A validated internal suspicion signal indicates behavior-based risk. "
            "Without resolution/supporting evidence, the prudent posture is High pending outcome."
        ),
        "recommended_actions_min": [
            "Enhanced transaction review (6–12 months depending on policy)",
            "Customer clarification + supporting documents request",
            "SoF refresh for relevant flows",
            "Case outcome documented (substantiated/unsubstantiated)",
            "Tune monitoring thresholds for customer risk"
        ],
        "focus_detectors": [
            "structured_deposits",
            "structured_payments",
            "pass_through",
            "layering",
            "round_figures",
            "cash_intensive",
            "third_party",
            "recurrence",
        ],
        "evidence_requests": [
            "Provide transaction-level explanations for flagged rows and supporting proof (invoices/receipts/contracts).",
            "Provide counterparties list and relationship purpose for repeated/recurring identities.",
        ],
    },

    "AML_FIU_INTERNAL_INVESTIGATION_TRIGGER": {
        "label": "AML/FIU Trigger From Internal Investigation",
        "definition": (
            "A trigger generated through internal investigations identifying potential AML/CTF control circumvention, "
            "undisclosed relationships, hidden beneficial ownership, or suspicious customer behavior not captured solely by "
            "standard transaction monitoring."
        ),
        "typology_signals": [
            "control circumvention",
            "hidden related parties",
            "false/insufficient KYC information",
            "use of nominees/fronts",
            "account purpose misrepresentation"
        ],
        "risk_rating": {"ml": 3, "tf": 2},
        "rating_rationale": (
            "Internal investigative findings typically carry higher reliability than raw alerts. "
            "ML risk is elevated; TF is medium unless TF typologies are explicitly present."
        ),
        "recommended_actions_min": [
            "ECDD refresh focused on identified gaps (UBO, controls, purpose)",
            "Relationship mapping and counterparty analysis",
            "Apply enhanced monitoring and periodic review cadence",
            "Document investigation basis and outcomes"
        ],
        "focus_detectors": [
            "layering",
            "pass_through",
            "third_party",
            "recurrence",
        ],
    },

    "FRAUD_CASE_HIGH_PRIORITY": {
        "label": "Fraud Case (High Priority)",
        "definition": (
            "A high-priority fraud referral involving confirmed or strongly suspected fraud where the customer "
            "account appears directly involved in receiving, moving, or benefiting from illicit proceeds."
        ),
        "typology_signals": [
            "proceeds of fraud as predicate offense",
            "money mule networks",
            "rapid withdrawals / cash-outs",
            "account takeover / identity abuse",
            "cross-border dispersal"
        ],
        "risk_rating": {"ml": 4, "tf": 2},
        "rating_rationale": (
            "Confirmed/strongly suspected fraud produces illicit proceeds (predicate crime) and often involves "
            "layering and mule behavior. Critical ML risk until cleared/mitigated."
        ),
        "recommended_actions_min": [
            "Immediate ECDD and enhanced transaction investigation",
            "Apply restrictions per policy/legal framework",
            "Senior management sign-off for continuation",
            "External reporting per jurisdictional requirements where applicable",
            "Exit assessment where risk is unmanageable"
        ],
        "focus_detectors": [
            "pass_through",
            "layering",
            "cash_intensive",
            "third_party",
        ],
    },

    "FRAUD_CASE_LOW_PRIORITY": {
        "label": "Fraud Case (Low Priority)",
        "definition": (
            "A low-priority fraud referral where indicators suggest the customer may be an unwitting victim or "
            "where impact is limited, isolated, and customer cooperation is prompt and consistent."
        ),
        "typology_signals": [
            "isolated suspicious receipt",
            "victim behavior patterns",
            "attempted scam with no meaningful laundering"
        ],
        "risk_rating": {"ml": 2, "tf": 1},
        "rating_rationale": (
            "Risk exists due to potential misuse of account, but credible victim/cooperation factors reduce likelihood "
            "of intentional laundering. Maintain medium ML until resolved."
        ),
        "recommended_actions_min": [
            "Targeted review of relevant period/transactions",
            "Customer engagement + education",
            "Short-term enhanced monitoring",
            "Downgrade if cleared with evidence"
        ]
    },

    "ADVERSE_MEDIA": {
        "label": "Adverse Media (Negative News)",
        "definition": (
            "Credible, relevant negative public information linking the customer or related parties to allegations "
            "or findings of serious misconduct that may indicate elevated ML/TF risk."
        ),
        "typology_signals": [
            "corruption / bribery",
            "organized crime links",
            "fraud / embezzlement allegations",
            "sanctions evasion indicators",
            "terrorism/extremism associations (if present)"
        ],
        "risk_rating": {"ml": 3, "tf": 3},
        "rating_rationale": (
            "Adverse media increases reputational and predicate-crime risk. Credibility/severity/recency determine "
            "final rating; default high pending verification."
        ),
        "recommended_actions_min": [
            "Source credibility scoring + corroboration check",
            "ECDD refresh (UBO, SoW/SoF where relevant)",
            "Enhanced monitoring and senior review for continuation",
            "Document decision and evidence trail"
        ]
    },

    "STAFF_PEP_REVIEW": {
        "label": "Staff PEP Review (PEP / RCA Identification or Re-Classification)",
        "definition": (
            "A trigger raised when staff identify or confirm that a customer or related parties is a PEP/RCA."
        ),
        "typology_signals": [
            "bribery and corruption exposure",
            "abuse of office",
            "influence peddling",
            "misappropriation of public funds"
        ],
        "risk_rating": {"ml": 3, "tf": 1},
        "rating_rationale": (
            "PEPs carry elevated corruption predicate-offense risk; regulators expect ECDD and senior management approval."
        ),
        "recommended_actions_min": [
            "Full ECDD refresh (CDD, UBO, control structure)",
            "SoW verification and plausibility assessment",
            "Senior management approval to onboard/continue",
            "Enhanced ongoing monitoring + periodic review"
        ]
    },
}
