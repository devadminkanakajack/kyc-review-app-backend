from __future__ import annotations

import re
from typing import Any

# Materiality threshold: include only rows strictly above this amount
MATERIALITY_MIN_AMOUNT = 10.0

# Known fee-focused transcodes (PNG: 299 = Miscellaneous Charges)
BANK_FEE_TRANSCODES = {"299"}

# Allowlist: legitimate fee types that should NOT be suppressed
LEGIT_FEE_ALLOWLIST_PATTERNS = [
    r"\bSCHOOL\b",
    r"\bSCHOOL FEES?\b",
    r"\bHOSPITAL\b",
    r"\bMEDICAL\b",
    r"\bLEGAL\b",
    r"\bDIRECTOR'?S?\b",
    r"\bREG(ISTRATION)?\b",
    r"\bUPNG\b",
    r"\bUNIVERSITY\b",
    r"\bCOLLEGE\b",
    r"\bTUITION\b",
    r"\bBOARDING\b",
    r"\bCOUNCIL\b",
    r"\bINSURANCE\b",
    r"\bLICENSE\b",
    r"\bLICENCE\b",
    r"\bRENT(AL)?\b",
    r"\bADMISSION\b",
    r"\bEXAM\b",
    r"\bFEES?\s+ASSIST",
]

# Bank/system fee patterns (suppress)
BANK_FEE_PATTERNS = [
    r"\bATM\b.*\bFEE\b",
    r"\bPOS\b.*\bFEE\b",
    r"\bVISA\b.*\bFEE\b",
    r"\bEFTPOS\b.*\bFEE\b",
    r"\bBAL(?:ANCE)?\s+INQUIRY\b.*\bFEE\b",
    r"\bBAL\s+INQUIRY\b.*\bFEE\b",
    r"\bTOP\s*UP\b.*\bFEE\b",
    r"\bMOBILE\s+TOPUP\b.*\bFEE\b",
    r"\bTRANSFER\b.*\bFEE\b",
    r"\bTELEGRAPHIC\s+TRANSFER\b.*\bFEE\b",
    r"\bOTHER\s+BANK\b.*\bFEE\b",
    r"\bKINA\s+ACC\b.*\bFEE\b",
    r"\bACCOUNT\b.*\bFEE\b",
    r"\bACC\b.*\bFEE\b",
    r"\bCASH\s+HANDLING\b.*\bFEE\b",
    r"\bCONVERSION\b.*\bFEE\b",
    r"\bWITHDRAWAL\b.*\bFEE\b",
    r"\bINQUIRY\b.*\bFEE\b",
]

BANK_FEE_TOKENS = [
    "ATM",
    "POS",
    "VISA",
    "EFTPOS",
    "BAL INQUIRY",
    "BALANCE INQUIRY",
    "TOPUP",
    "TOP UP",
    "MOBILE TOPUP",
    "TRANSFER",
    "TELEGRAPHIC",
    "OTHER BANK",
    "KINA ACC",
    "ACCOUNT",
    "ACC",
    "CASH HANDLING",
    "CONVERSION",
    "WITHDRAWAL",
    "INQUIRY",
]

FEE_WORDS = ["FEE", "FEES", "CHARGE", "CHARGES", "LEVY", "SURCHARGE"]

_ALLOWLIST_RX = [re.compile(p) for p in LEGIT_FEE_ALLOWLIST_PATTERNS]
_BANK_FEE_RX = [re.compile(p) for p in BANK_FEE_PATTERNS]


def _norm_text(text: Any) -> str:
    s = str(text or "")
    s = re.sub(r"\s+", " ", s.upper()).strip()
    return s


def _matches_any(text: str, patterns: list[re.Pattern]) -> bool:
    return any(p.search(text) for p in patterns)


def is_bank_fee(description_raw: Any, description: Any = None, transcode: Any = None) -> bool:
    text = _norm_text(f"{description_raw or ''} {description or ''}")
    if not text:
        return False

    if _matches_any(text, _ALLOWLIST_RX):
        return False

    if transcode is not None and str(transcode).strip() in BANK_FEE_TRANSCODES:
        return True

    if not any(w in text for w in FEE_WORDS):
        return False

    if _matches_any(text, _BANK_FEE_RX):
        return True

    if any(tok in text for tok in BANK_FEE_TOKENS):
        return True

    return False


def is_material_amount(debit: Any, credit: Any, min_amount: float = MATERIALITY_MIN_AMOUNT) -> bool:
    try:
        d = float(debit or 0.0)
    except Exception:
        d = 0.0
    try:
        c = float(credit or 0.0)
    except Exception:
        c = 0.0

    amt = max(abs(d), abs(c))
    return amt > float(min_amount)


def _row_get(row: Any, key: str) -> Any:
    if row is None:
        return None
    if hasattr(row, "get"):
        try:
            return row.get(key)
        except Exception:
            return None
    return getattr(row, key, None)


def should_include_row(row: Any, min_amount: float = MATERIALITY_MIN_AMOUNT) -> bool:
    if row is None:
        return False

    if not is_material_amount(_row_get(row, "DEBIT"), _row_get(row, "CREDIT"), min_amount=min_amount):
        return False

    if is_bank_fee(
        _row_get(row, "DESCRIPTION_RAW"),
        _row_get(row, "DESCRIPTION"),
        _row_get(row, "TRANSCODE"),
    ):
        return False

    return True
