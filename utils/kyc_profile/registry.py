from __future__ import annotations

from typing import Dict, Any, List, Optional

from utils.kyc_profile.schema import KYCProfile, as_dict

# Profiles
from utils.kyc_profile.individual.employed import PROFILE as IND_EMPLOYED
from utils.kyc_profile.individual.self_employed import PROFILE as IND_SELF_EMPLOYED
from utils.kyc_profile.individual.non_employed import PROFILE as IND_NON_EMPLOYED
from utils.kyc_profile.non_individual.generic_company import PROFILE as NI_GENERIC

# ✅ Pull policy/narrative rulebook into dump
from utils.kyc_rules import REQUIRED_HEADINGS, KYC_RULEBOOK, BEHAVIOUR_MATRIX


FEATURES_CONTRACT: Dict[str, str] = {
    "cash_deposit_share_pct": "Percent of credits via cash deposits (0-100).",
    "turnover_multiple_of_declared_income": "Monthly turnover / declared monthly income (if available).",
    "tx_count_monthly": "Total transactions per month (or across statement period if multi-month).",
    "intl_transfer_count_monthly": "International transfers per month (or across statement period if multi-month).",
    "single_credit_max_multiple_of_median": "Largest credit divided by median credit size.",
    "rapid_in_out_ratio": "Fraction of credits quickly followed by debits (pass-through indicator).",

    # Pattern flags (from narration / detectors)
    "salary_like_credit_flag": "True if recurring payroll/salary-like credits detected.",
    "merchant_supplier_pattern_flag": "True if spending resembles supplier/invoice settlement patterns.",
    "payroll_distribution_pattern_flag": "True if outbound payments resemble payroll/allowance distributions.",
    "recurring_expenses_detected_flag": "True if recurring operating expenses (rent/utilities/payroll/suppliers) are detected.",
    "cash_structuring_flag": "True if cash deposits suggest structuring (multiple similar deposits below thresholds).",

    # Risk flags
    "high_risk_counterparty_flag": "True if counterparty matches high-risk typology/list.",
}

PROFILE_REGISTRY: Dict[str, KYCProfile] = {
    IND_EMPLOYED.profile_id: IND_EMPLOYED,
    IND_SELF_EMPLOYED.profile_id: IND_SELF_EMPLOYED,
    IND_NON_EMPLOYED.profile_id: IND_NON_EMPLOYED,
    NI_GENERIC.profile_id: NI_GENERIC,
}


def get_profile(profile_id: str) -> Optional[Dict[str, Any]]:
    p = PROFILE_REGISTRY.get(profile_id)
    return as_dict(p) if p else None


def list_profiles(customer_type: Optional[str] = None) -> List[Dict[str, Any]]:
    profiles = [as_dict(p) for p in PROFILE_REGISTRY.values()]
    if customer_type:
        profiles = [p for p in profiles if p["customer_type"] == customer_type]
    return sorted(profiles, key=lambda x: (x["customer_type"], x["label"]))


def library_dump() -> Dict[str, Any]:
    return {
        "meta": {"version": "1.1.0", "country": "PNG", "currency": "PGK"},
        # ✅ One source of truth for report sections + generic compliance expectations
        "required_headings": REQUIRED_HEADINGS,
        "kyc_rulebook": KYC_RULEBOOK,
        "behaviour_matrix": BEHAVIOUR_MATRIX,
        "features_contract": FEATURES_CONTRACT,
        "profiles": list_profiles(),
    }
