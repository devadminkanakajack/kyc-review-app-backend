"""utils.narrative_generator.action_item_library

Cards for "Action Items" keyed off risk_engine drivers.
"""

from __future__ import annotations

from typing import List

from .registry import NarrativeCard, risk_rating


def get_action_item_cards() -> List[NarrativeCard]:
    cards: List[NarrativeCard] = []

    cards.append(
        NarrativeCard(
            id="action_request_supporting_docs",
            category="Action Items",
            priority=60,
            render=lambda ctx: (
                "Request/verify supporting documentation for material credit sources and major payment purposes (contracts, invoices, remittance evidence, payslips), as applicable."
            ),
        )
    )

    cards.append(
        NarrativeCard(
            id="action_verify_salary_employer",
            category="Action Items",
            priority=80,
            driver_terms=["SALARY PATTERN", "SALARY_PATTERN", "PAYROLL"],
            render=lambda ctx: (
                "Where payroll-like credits are observed, verify employer/payroll source and confirm consistency with declared business/occupation/income; update KYC income/turnover where necessary."
            ),
        )
    )

    cards.append(
        NarrativeCard(
            id="action_review_beneficiaries",
            category="Action Items",
            priority=92,
            driver_terms=["PASS THROUGH", "PASS_THROUGH", "LAYERING"],
            render=lambda ctx: (
                "Review top transfer beneficiaries/counterparties for relationship and purpose. Apply alerts for repeat rapid in→out cycles and unexplained onward transfers."
            ),
        )
    )

    cards.append(
        NarrativeCard(
            id="action_structuring_review",
            category="Action Items",
            priority=90,
            driver_terms=["STRUCTURED DEPOSITS", "STRUCTURED PAYMENTS", "STRUCTURING"],
            render=lambda ctx: (
                "Conduct a structuring review across the statement period (aggregate same-day/near-day deposits/payments) and document whether activity appears designed to avoid thresholds or monitoring."
            ),
        )
    )

    cards.append(
        NarrativeCard(
            id="action_third_party_validation",
            category="Action Items",
            priority=88,
            driver_terms=["THIRD PARTY", "THIRD_PARTY", "NOMINEE"],
            render=lambda ctx: (
                "Validate third-party credits/debits (source, relationship, economic rationale). Obtain documentation and update counterparty notes where relevant."
            ),
        )
    )

    cards.append(
        NarrativeCard(
            id="action_high_risk_escalate",
            category="Action Items",
            priority=96,
            applies=lambda ctx: risk_rating(ctx) in {"HIGH", "CRITICAL"},
            render=lambda ctx: (
                "Escalate to MLRO/Compliance Manager for decision and record management sign-off. Ensure outcomes (restrict/close/retain) align to policy and regulatory expectations."
            ),
        )
    )

    cards.append(
        NarrativeCard(
            id="action_consider_smr",
            category="Action Items",
            priority=99,
            applies=lambda ctx: risk_rating(ctx) == "CRITICAL",
            render=lambda ctx: (
                "Consider filing/escalating an internal SMR where grounds are met; ensure evidence and reasoning are recorded, and apply interim account controls if required."
            ),
        )
    )

    return cards
