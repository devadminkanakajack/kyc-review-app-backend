"""Deterministic narrative generator package."""

from .builder import build_narrative_v0, build_narrative_v1

build_narrative = build_narrative_v1

__all__ = ["build_narrative", "build_narrative_v0", "build_narrative_v1"]
