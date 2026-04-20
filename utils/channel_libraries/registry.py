"""utils.channel_libraries.registry

Registry for channel classification libraries.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .individual_employed import CHANNEL_LIBRARY as INDIVIDUAL_EMPLOYED
from .individual_self_employed import CHANNEL_LIBRARY as INDIVIDUAL_SELF_EMPLOYED
from .individual_non_employed import CHANNEL_LIBRARY as INDIVIDUAL_NON_EMPLOYED
from .non_individual_generic_company import CHANNEL_LIBRARY as NON_INDIVIDUAL_GENERIC_COMPANY

REGISTRY: Dict[str, Dict[str, Any]] = {
    "individual:employed": INDIVIDUAL_EMPLOYED,
    "individual:self employed": INDIVIDUAL_SELF_EMPLOYED,
    "individual:non employed": INDIVIDUAL_NON_EMPLOYED,
    "non individual:generic company": NON_INDIVIDUAL_GENERIC_COMPANY,
}


def _norm(s: Any) -> str:
    return " ".join(str(s or "").replace("_", " ").replace("-", " ").strip().lower().split())


def _key(client_type: Any, profile: Any) -> str:
    ct = _norm(client_type)
    pr = _norm(profile)
    if pr:
        return f"{ct}:{pr}"
    return ct


def get_channel_library(client_type: str, profile: Optional[str] = None) -> Dict[str, Any]:
    """
    Return the appropriate CHANNEL_LIBRARY dict for the given client_type/profile.

    This function is used by utils/channel_libraries/channel_classifier.py.
    """
    ct = _norm(client_type)
    pr = _norm(profile)

    if ct.startswith("indiv"):
        if "self" in pr and "employ" in pr:
            return dict(INDIVIDUAL_SELF_EMPLOYED or {})
        if "non" in pr and "employ" in pr:
            return dict(INDIVIDUAL_NON_EMPLOYED or {})
        if "employ" in pr:
            return dict(INDIVIDUAL_EMPLOYED or {})

        reg = REGISTRY.get(_key("individual", pr))
        if reg:
            return dict(reg or {})

        return {
            "profile": "Individual - (Unknown)",
            "credit_rules": {},
            "debit_rules": {},
        }

    if ct.startswith("non") or "company" in ct or "corporate" in ct or "business" in ct:
        return dict(NON_INDIVIDUAL_GENERIC_COMPANY or {})

    reg = REGISTRY.get(_key(ct, pr)) or REGISTRY.get(ct)
    if reg:
        return dict(reg or {})

    return {
        "profile": f"{client_type or 'Unknown'} - (No Library)",
        "credit_rules": {},
        "debit_rules": {},
    }
