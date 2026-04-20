"""utils.narrative_generator.recommendation_library

Cards for "Recommendation" keyed off risk_engine drivers + rating.
"""

from __future__ import annotations

from typing import List

from .registry import NarrativeCard, risk_rating


def _top_flagged_channels(ctx, n: int = 3):
    """Best-effort helper to show where detector-flagged rows concentrate."""
    rm = (ctx or {}).get("risk_metrics") or {}
    sm = rm.get("suspicious_by_channel") or {}
    by_ch = sm.get("by_channel") if isinstance(sm, dict) else None
    if not isinstance(by_ch, dict) or not by_ch:
        return []

    items = []
    for code, block in by_ch.items():
        if not isinstance(block, dict):
            continue
        try:
            c = int(block.get("flagged_count") or 0)
        except Exception:
            c = 0
        items.append((str(code), c))

    items = sorted(items, key=lambda x: x[1], reverse=True)
    return [x for x in items if x[1] > 0][: max(0, int(n))]


def get_recommendation_cards() -> List[NarrativeCard]:
    cards: List[NarrativeCard] = []

    # Trigger override escalation
    cards.append(
        NarrativeCard(
            id="rec_trigger_override",
            category="Recommendation",
            priority=98,
            driver_terms=["SMR", "SUSPICIOUS", "SECTION 81", "S81", "NOTICE", "REGULATOR"],
            render=lambda ctx: (
                "Due to the trigger source (SMR / regulator notice), recommend enhanced escalation and documented senior approval regardless of transaction pattern findings."
            ),
        )
    )

    # Rating-based default recommendations
    cards.append(
        NarrativeCard(
            id="rec_critical",
            category="Recommendation",
            priority=100,
            applies=lambda ctx: risk_rating(ctx) == "CRITICAL",
            render=lambda ctx: (
                "Recommend senior escalation and interim account controls. If adverse grounds remain unresolved after EDD and evidence review, recommend restriction and exit of relationship as per policy."
            ),
        )
    )

    cards.append(
        NarrativeCard(
            id="rec_high",
            category="Recommendation",
            priority=90,
            applies=lambda ctx: risk_rating(ctx) == "HIGH",
            render=lambda ctx: (
                "Recommend retaining the relationship only subject to Enhanced Due Diligence completion, senior approval, and strengthened ongoing monitoring aligned to identified risks."
            ),
        )
    )

    cards.append(
        NarrativeCard(
            id="rec_medium",
            category="Recommendation",
            priority=80,
            applies=lambda ctx: risk_rating(ctx) == "MEDIUM",
            render=lambda ctx: (
                "Recommend retaining the relationship subject to completion of documented action items and periodic monitoring to confirm behaviour remains consistent with the stated profile."
            ),
        )
    )

    cards.append(
        NarrativeCard(
            id="rec_low",
            category="Recommendation",
            priority=70,
            applies=lambda ctx: risk_rating(ctx) in {"LOW", "UNKNOWN"},
            render=lambda ctx: (
                "Recommend retaining the relationship under standard monitoring, with KYC information kept current and reviewed at the normal cycle."
            ),
        )
    )

    # Special case: pass-through + layering at high/critical
    cards.append(
        NarrativeCard(
            id="rec_pass_through_layering_high",
            category="Recommendation",
            priority=99,
            driver_terms=["PASS THROUGH", "LAYERING"],
            applies=lambda ctx: risk_rating(ctx) in {"HIGH", "CRITICAL"},
            render=lambda ctx: (
                "Combined pass-through and layering signals elevate ML risk; if satisfactory explanations and evidence are not obtained, recommend restriction and potential exit in line with policy."
            ),
        )
    )

    # Drill-down aware recommendation: highlight where flagged txns cluster
    cards.append(
        NarrativeCard(
            id="rec_flagged_channels_summary",
            category="Recommendation",
            priority=85,
            applies=lambda ctx: bool(((ctx or {}).get("risk_metrics") or {}).get("suspicious_by_channel", {}).get("total_flagged")),
            render=lambda ctx: (
                "Detector-flagged transactions are available for per-channel review. "
                + (
                    "Top impacted channels (by flagged row count): "
                    + ", ".join([f"{code} ({cnt})" for code, cnt in _top_flagged_channels(ctx, n=3)])
                    + ". "
                    if _top_flagged_channels(ctx, n=3)
                    else ""
                )
                + "Recommend focusing evidence requests and rationale on these channels and documenting outcomes at transaction-row level."
            ),
        )
    )

    return cards
