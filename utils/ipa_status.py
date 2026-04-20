# utils/ipa_status.py
from typing import Dict, Any, List, Tuple

IPA_STATUS_LIBRARY: Dict[str, Dict[str, Any]] = {
    "Registered": {
        "definition": "The entity is active and in good standing with the Registrar of Companies.",
        "risk_points": 0,
        "actions": [],
    },
    "Cancelled": {
        "definition": "Used for Business Names that have expired or where the Registrar is satisfied the business is no longer operating.",
        "risk_points": 15,
        "actions": [
            "Obtain updated registration evidence or confirm cessation of operations.",
            "Assess account purpose and consider restrictions if entity is no longer active.",
        ],
    },
    "Struck-Off / Removed": {
        "definition": "Taken off the active register (often non-compliance / failure to re-register / failure to file Annual Returns).",
        "risk_points": 35,
        "actions": [
            "Obtain formal registry evidence and legal status confirmation.",
            "Escalate to senior management; assess relationship continuation and possible exit per policy.",
        ],
    },
    "Lapsed": {
        "definition": "Business Name failed to file renewals; penalties apply to revoke.",
        "risk_points": 20,
        "actions": [
            "Request renewal evidence and timeline for reinstatement.",
            "Apply enhanced monitoring until status is normalized.",
        ],
    },
    "Suspended": {
        "definition": "Temporary hold, often due to failure to file Annual Status Report for foreign investments.",
        "risk_points": 25,
        "actions": [
            "Obtain explanation and supporting compliance documents; confirm reinstatement timeline.",
            "Consider restrictions if suspension persists.",
        ],
    },
    "In Liquidation / Liquidated": {
        "definition": "Company is winding up affairs or has ceased to exist after liquidation.",
        "risk_points": 30,
        "actions": [
            "Obtain liquidation documentation and authorized signatory confirmation.",
            "Restrict unusual activity and escalate continuation decision.",
        ],
    },
    "Not Yet Re-Registered / Not Yet Updated": {
        "definition": "Entity has not transitioned data to the new registry system; may face removal.",
        "risk_points": 22,
        "actions": [
            "Request proof of re-registration/update submission.",
            "Apply enhanced monitoring until confirmed active.",
        ],
    },
    "Intent / Registered - In Removal": {
        "definition": "Registrar started removal process, usually for non-compliance.",
        "risk_points": 32,
        "actions": [
            "Urgently obtain compliance remediation evidence.",
            "Escalate to senior management; consider restrictions.",
        ],
    },
    "Void": {
        "definition": "Registration declared legally invalid from the beginning.",
        "risk_points": 40,
        "actions": [
            "Treat as critical legal/compliance risk; escalate immediately.",
            "Assess immediate exit/restriction per policy and legal guidance.",
        ],
    },
    # Optional: you mentioned these too
    "Expired": {
        "definition": "Foreign certifications or specific registrations reached legal term end without renewal.",
        "risk_points": 18,
        "actions": [
            "Request renewal evidence and confirm continued authority to operate.",
        ],
    },
    "Certified / Exempted": {
        "definition": "Foreign enterprises granted permission to operate under Investment Promotion Act 1992.",
        "risk_points": 10,
        "actions": [
            "Verify certification scope and validity; confirm ongoing compliance filings.",
        ],
    },
    "Amalgamated": {
        "definition": "Two or more companies merged; previous registrations closed.",
        "risk_points": 12,
        "actions": [
            "Obtain amalgamation documentation and confirm correct operating entity details/UBO.",
        ],
    },
}


def ipa_risk_impact(status: str) -> Tuple[float, List[str]]:
    """
    Returns (risk_points, recommended_actions)
    """
    s = (status or "").strip()
    if not s:
        return 0.0, []
    info = IPA_STATUS_LIBRARY.get(s)
    if not info:
        # Unknown status: treat as medium concern
        return 18.0, [
            "IPA/ROC status provided is not recognized; obtain official registry evidence.",
            "Apply enhanced monitoring pending verification.",
        ]
    return float(info.get("risk_points", 0.0)), list(info.get("actions") or [])
