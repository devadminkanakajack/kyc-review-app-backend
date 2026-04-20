"""
AML Pattern Detectors Package
=============================

This directory contains modular behavioural detection components
used by the AML Review backend. Each detector evaluates a specific
money-laundering or financial-crime pattern in a transaction dataset.

All detector modules MUST implement a public function:
    detect(df: pandas.DataFrame) -> Dict[str, Any]

Or, for multi-output detectors:
    detect_structured_deposits(df)
    detect_structured_payments(df)
    detect_all_recurrence(df)

A detector must return a dictionary with structure:
{
    "triggered": bool,
    "strength": float (0–1),
    "indicators": List[str],
    "raw": Any (optional detailed values)
}

Detectors included:
-------------------
recurrence_detector.py
    • Identity recurrence clusters
    • Narrative/description recurrence clusters
    • Multi-counterparty flows

structured_deposits.py
    • Multi-third-party deposits
    • Structured build-up (1–7 day windows)
    • Threshold-based smurfing (0–500, 500–1k, 1k–5k, etc.)
    • Value structuring & timing structuring

structured_payments.py
    • Multi-beneficiary structuring (1–7 day windows)
    • Breakout flows following deposit build-ups

pass_through.py
    • Rapid in-out behaviour (same day, 1–3 days)

layering.py
    • Multi-hop, circular or multi-corridor movement

round_figures.py
    • Round-figure pattern detection (e.g., 500, 1000, 2000)

salary_pattern.py
    • Salary/regular income cycle detection

cash_intensive.py
    • Cash-heavy behaviour or inconsistencies

third_party.py
    • Third-party dependency & external counterparty saturation

These detectors are imported by analyze_statement.py and the risk_engine.

"""

# Explicit imports so callers can use clean namespace:
from .recurrence_detector import detect_all_recurrence
from .structured_deposits import detect_structured_deposits
from .structured_payments import detect_structured_payments
from .pass_through import detect_pass_through
from .layering import detect_layering
from .round_figures import detect_round_figures
from .salary_pattern import detect_salary_pattern
from .cash_intensive import detect_cash_intensive
from .third_party import detect_third_party

__all__ = [
    "detect_all_recurrence",
    "detect_structured_deposits",
    "detect_structured_payments",
    "detect_pass_through",
    "detect_layering",
    "detect_round_figures",
    "detect_salary_pattern",
    "detect_cash_intensive",
    "detect_third_party",
]
