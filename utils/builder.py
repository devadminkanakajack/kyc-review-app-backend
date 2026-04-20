"""Compatibility shim for legacy imports.

Main imports `build_narrative_v1` from utils.builder.
This module re-exports from utils.narrative_generator.builder.
"""
from utils.narrative_generator.builder import build_narrative_v0, build_narrative_v1

build_narrative = build_narrative_v1

__all__ = ["build_narrative", "build_narrative_v0", "build_narrative_v1"]
