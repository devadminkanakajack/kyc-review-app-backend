from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class KYCProfile:
    profile_id: str
    label: str
    customer_type: str  # "INDIVIDUAL" or "NON_INDIVIDUAL"
    segment: str
    description: str
    baseline_risk: int  # 0-100

    # Narrative behavior library
    source_of_funds: List[str]
    usage_of_funds: List[str]

    # Expected patterns (feature-driven)
    expected_patterns: Dict[str, Any]

    # Rules evaluated by risk engine / profile scoring
    mismatch_rules: List[Dict[str, Any]]

    tags: List[str]

    # ✅ NEW: Policy/narrative hooks from utils/kyc_rules.py
    # These are generic and help the doc generator.
    expected_behaviour: Optional[List[str]] = None
    high_risk_behaviour: Optional[List[str]] = None


def as_dict(p: KYCProfile) -> Dict[str, Any]:
    return {
        "profile_id": p.profile_id,
        "label": p.label,
        "customer_type": p.customer_type,
        "segment": p.segment,
        "description": p.description,
        "baseline_risk": p.baseline_risk,
        "source_of_funds": p.source_of_funds,
        "usage_of_funds": p.usage_of_funds,
        "expected_patterns": p.expected_patterns,
        "mismatch_rules": p.mismatch_rules,
        "tags": p.tags,
        "expected_behaviour": p.expected_behaviour or [],
        "high_risk_behaviour": p.high_risk_behaviour or [],
    }
