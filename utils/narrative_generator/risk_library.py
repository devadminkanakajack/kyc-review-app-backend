"""utils.narrative_generator.risk_library

Cards for "Risks Identified" keyed off risk_engine drivers.
"""

from __future__ import annotations

from typing import Dict, List

from .registry import NarrativeCard, risk_rating


def get_risk_cards() -> List[NarrativeCard]:
    cards: List[NarrativeCard] = []

    # Trigger risks (based on trigger drivers in risk_engine)
    cards.append(
        NarrativeCard(
            id="risk_trigger_smr",
            category="Risks Identified",
            priority=96,
            driver_terms=["SMR", "SUSPICIOUS", "FIU", "TRANSACTION MONITORING"],
            render=lambda ctx: (
                "Trigger indicates a Suspicious Matter Report / FIU transaction monitoring referral; this elevates ML/TF risk and requires strong documentation and escalation controls."
            ),
            tags=["trigger"],
        )
    )

    cards.append(
        NarrativeCard(
            id="risk_trigger_reg_notice",
            category="Risks Identified",
            priority=95,
            driver_terms=["SECTION_81", "SECTION 81", "S81", "NOTICE", "BPNG", "ICAC", "OMBUDSMAN"],
            render=lambda ctx: (
                "Trigger indicates a regulator/authority notice (e.g., Section 81 or similar), heightening compliance and reputational risk and requiring strict documentation of outcomes."
            ),
            tags=["trigger"],
        )
    )

    # Detector-driven risks (risk_engine uses "Detector signal" + the detector name in detail)
    cards.append(
        NarrativeCard(
            id="risk_pass_through",
            category="Risks Identified",
            priority=92,
            driver_terms=["PASS THROUGH", "PASS_THROUGH", "PASS-THROUGH"],
            render=lambda ctx: (
                "Pass-through behaviour detected (rapid funds in → rapid funds out), consistent with conduit/mule activity or layering."
            ),
            tags=["detector"],
        )
    )

    cards.append(
        NarrativeCard(
            id="risk_layering",
            category="Risks Identified",
            priority=90,
            driver_terms=["LAYERING"],
            render=lambda ctx: (
                "Layering indicators detected (movement of funds across multiple transfers/beneficiaries), potentially obscuring source and ownership."
            ),
            tags=["detector"],
        )
    )

    cards.append(
        NarrativeCard(
            id="risk_structured_deposits",
            category="Risks Identified",
            priority=89,
            driver_terms=["STRUCTURED DEPOSITS", "STRUCTURED_DEPOSITS"],
            render=lambda ctx: (
                "Structured credit/cash deposit behaviour detected (repeated deposits within thresholds/rounding patterns), consistent with placement structuring."
            ),
            tags=["detector"],
        )
    )

    cards.append(
        NarrativeCard(
            id="risk_structured_payments",
            category="Risks Identified",
            priority=88,
            driver_terms=["STRUCTURED PAYMENTS", "STRUCTURED_PAYMENTS"],
            render=lambda ctx: (
                "Structured debit payment behaviour detected (multiple similar-sized payments over short windows), which can indicate structuring to avoid detection/thresholds."
            ),
            tags=["detector"],
        )
    )

    cards.append(
        NarrativeCard(
            id="risk_third_party",
            category="Risks Identified",
            priority=87,
            driver_terms=["THIRD PARTY", "THIRD_PARTY"],
            render=lambda ctx: (
                "Third-party activity indicators detected (payments/credits involving parties not aligned to stated profile), increasing risk of nominee usage and unclear source of funds."
            ),
            tags=["detector"],
        )
    )

    cards.append(
        NarrativeCard(
            id="risk_cash_intensive",
            category="Risks Identified",
            priority=85,
            driver_terms=["CASH INTENSIVE", "CASH_INTENSIVE"],
            render=lambda ctx: (
                "Cash-intensive behaviour detected (cash deposits/withdrawals and cash-out concentration), elevating placement risk and reducing auditability of funds."
            ),
            tags=["detector"],
        )
    )

    cards.append(
        NarrativeCard(
            id="risk_round_figures",
            category="Risks Identified",
            priority=75,
            driver_terms=["ROUND FIGURES", "ROUND_FIGURES"],
            render=lambda ctx: (
                "Round-figure transaction patterns detected (repeated rounded amounts), which can be indicative of manual structuring or non-genuine commercial activity."
            ),
            tags=["detector"],
        )
    )

    # KYC mismatch-driven risks (generic matching terms — these come from profile mismatch rationales)
    cards.append(
        NarrativeCard(
            id="risk_kyc_structuring_mismatch",
            category="Risks Identified",
            priority=93,
            driver_terms=["STRUCTURED", "STRUCTURING", "CASH STRUCTURING"],
            render=lambda ctx: (
                "KYC behavioural mismatch indicates potential structuring activity requiring enhanced source-of-funds verification."
            ),
            tags=["kyc_mismatch"],
        )
    )

    cards.append(
        NarrativeCard(
            id="risk_kyc_no_operating_expenses",
            category="Risks Identified",
            priority=70,
            driver_terms=["RECURRING OPERATING EXPENSES", "MAIN OPERATING ACCOUNT", "OPERATING EXPENSES NOT"],
            render=lambda ctx: (
                "Recurring operating expenses were not clearly detected; verify whether this is the main operating account and reconcile expected vs observed activity."
            ),
            tags=["kyc_mismatch"],
        )
    )

    # Rating-driven wrap-up
    cards.append(
        NarrativeCard(
            id="risk_rating_high",
            category="Risks Identified",
            priority=60,
            applies=lambda ctx: risk_rating(ctx) in {"HIGH", "CRITICAL"},
            render=lambda ctx: (
                f"Aggregated assessment indicates {risk_rating(ctx)} risk posture based on trigger severity, behavioural indicators and KYC profile alignment."
            ),
            tags=["rating"],
        )
    )

    return cards
