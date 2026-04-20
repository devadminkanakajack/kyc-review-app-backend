"""utils.narrative_generator.mitigation_library

Cards for "Risk Mitigation" keyed off risk_engine drivers.
"""

from __future__ import annotations

from typing import List

from .registry import NarrativeCard, risk_rating


def get_mitigation_cards() -> List[NarrativeCard]:
    cards: List[NarrativeCard] = []

    # Baseline mitigation always present
    cards.append(
        NarrativeCard(
            id="mitigation_general_kyc_refresh",
            category="Risk Mitigation",
            priority=60,
            render=lambda ctx: (
                "Refresh KYC where required (occupation/business, income/turnover, address, beneficial ownership) and confirm expected account activity versus observed behaviour."
            ),
            tags=["baseline"],
        )
    )

    cards.append(
        NarrativeCard(
            id="mitigation_pass_through_controls",
            category="Risk Mitigation",
            priority=92,
            driver_terms=["PASS THROUGH", "PASS_THROUGH", "PASS-THROUGH"],
            render=lambda ctx: (
                "Apply enhanced monitoring for rapid in→out activity: review transfer beneficiaries, purposes/narratives, and verify the economic rationale for movement of funds."
            ),
        )
    )

    cards.append(
        NarrativeCard(
            id="mitigation_structuring_controls",
            category="Risk Mitigation",
            priority=90,
            driver_terms=["STRUCTURED DEPOSITS", "STRUCTURED PAYMENTS", "STRUCTURING"],
            render=lambda ctx: (
                "Implement structuring controls: review aggregation across days, confirm source documents for deposits/payments, and monitor for repeated sub-threshold or near-ceiling patterns."
            ),
        )
    )

    cards.append(
        NarrativeCard(
            id="mitigation_third_party_controls",
            category="Risk Mitigation",
            priority=88,
            driver_terms=["THIRD PARTY", "THIRD_PARTY", "NOMINEE"],
            render=lambda ctx: (
                "For third-party activity, validate relationship and rationale (e.g., customer receipts, remittance, supplier/customer payments) and obtain supporting documentation to evidence legitimacy."
            ),
        )
    )

    cards.append(
        NarrativeCard(
            id="mitigation_cash_controls",
            category="Risk Mitigation",
            priority=86,
            driver_terms=["CASH INTENSIVE", "CASH_INTENSIVE", "CASH DEPOSIT"],
            render=lambda ctx: (
                "For cash-intensive behaviour, verify cash-generating activity, consider cash controls/limits, and increase sampling of cash deposit narratives against supporting receipts/invoices where available."
            ),
        )
    )

    cards.append(
        NarrativeCard(
            id="mitigation_high_rating_escalation",
            category="Risk Mitigation",
            priority=96,
            applies=lambda ctx: risk_rating(ctx) in {"HIGH", "CRITICAL"},
            render=lambda ctx: (
                "Escalate to enhanced due diligence controls: senior/MLRO sign-off, closer transaction monitoring, and a documented decision rationale aligned to bank policy and regulatory expectations."
            ),
        )
    )

    return cards
