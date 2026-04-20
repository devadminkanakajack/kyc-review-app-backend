"""utils.narrative_generator.registry

Deterministic narrative "card" registry + driver matching helpers.

We key content selection off `risk_metrics["drivers"]`.
Each driver is expected to be a dict like:
  {"title": "...", "detail": "...", "points": 12.0}
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

RuleFn = Callable[[Dict[str, Any]], bool]
RenderFn = Callable[[Dict[str, Any]], str]


@dataclass(frozen=True)
class NarrativeCard:
    """
    A deterministic narrative block.

    - category MUST match one of utils.kyc_rules.REQUIRED_HEADINGS.
    - driver_terms are keywords/phrases used to match risk_engine drivers.
    - applies can add extra boolean gating (optional).
    """
    id: str
    category: str
    priority: int = 50
    driver_terms: Optional[List[str]] = None
    applies: RuleFn = lambda _ctx: True
    render: RenderFn = lambda _ctx: ""
    tags: Optional[List[str]] = None


def safe_get(ctx: Dict[str, Any], path: List[str], default: Any = None) -> Any:
    cur: Any = ctx
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def risk_rating(ctx: Dict[str, Any]) -> str:
    risk = ctx.get("risk_metrics") or {}
    return str(risk.get("rating") or "UNKNOWN").upper()


def get_drivers(ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    risk = ctx.get("risk_metrics") or {}
    drivers = risk.get("drivers") or []
    return drivers if isinstance(drivers, list) else []


def _norm(s: Any) -> str:
    return str(s or "").strip().upper()


def match_driver_terms(ctx: Dict[str, Any], terms: List[str]) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Returns (matched, matched_drivers).
    A match occurs if ANY term appears in (driver.title + driver.detail).
    """
    if not terms:
        return False, []

    terms_u = [_norm(t) for t in terms if str(t).strip()]
    if not terms_u:
        return False, []

    matched: List[Dict[str, Any]] = []
    for d in get_drivers(ctx):
        blob = f"{d.get('title','')} {d.get('detail','')}"
        blob_u = _norm(blob)
        if any(t in blob_u for t in terms_u):
            matched.append(d)

    return (len(matched) > 0), matched


def driver_points(ctx: Dict[str, Any], terms: List[str]) -> float:
    """
    Sum points of drivers that match terms (best-effort).
    """
    ok, ds = match_driver_terms(ctx, terms)
    if not ok:
        return 0.0
    total = 0.0
    for d in ds:
        try:
            total += float(d.get("points") or 0.0)
        except Exception:
            pass
    return float(total)


def get_all_cards() -> List[NarrativeCard]:
    """Collect all cards from the sub-libraries."""
    from .risk_library import get_risk_cards
    from .mitigation_library import get_mitigation_cards
    from .action_item_library import get_action_item_cards
    from .recommendation_library import get_recommendation_cards

    cards: List[NarrativeCard] = []
    cards.extend(get_risk_cards())
    cards.extend(get_mitigation_cards())
    cards.extend(get_action_item_cards())
    cards.extend(get_recommendation_cards())
    return cards
