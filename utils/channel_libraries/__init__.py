# utils/channel_libraries/__init__.py
"""
Channel Libraries Package
=========================

Public API for channel intelligence.

Used by:
  - analyze_statement.py
  - narrative_builder.py

Exports:
  - get_channel_library, REGISTRY
  - classify_material_channels, build_channel_profile
"""

from .registry import REGISTRY, get_channel_library
from .channel_classifier import classify_material_channels, build_channel_profile

# Optional direct access (debug/testing)
from .individual_employed import CHANNEL_LIBRARY as INDIVIDUAL_EMPLOYED
from .individual_self_employed import CHANNEL_LIBRARY as INDIVIDUAL_SELF_EMPLOYED
from .individual_non_employed import CHANNEL_LIBRARY as INDIVIDUAL_NON_EMPLOYED
from .non_individual_generic_company import CHANNEL_LIBRARY as NON_INDIVIDUAL_GENERIC_COMPANY

__all__ = [
    "REGISTRY",
    "get_channel_library",
    "classify_material_channels",
    "build_channel_profile",
    "INDIVIDUAL_EMPLOYED",
    "INDIVIDUAL_SELF_EMPLOYED",
    "INDIVIDUAL_NON_EMPLOYED",
    "NON_INDIVIDUAL_GENERIC_COMPANY",
]
