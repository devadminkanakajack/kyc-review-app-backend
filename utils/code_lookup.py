from __future__ import annotations

import os
from collections import defaultdict

import pandas as pd


class TransactionCodeLookup:
    """
    Loads PNG ICBA Number Codes (CSV or XLSX).

    Column Mapping Rules:
        • LINE DESCRIPTION → ACCOUNT_TYPE
        • TRAN. CODE → CODE
        • TRAN. DESCRIPTION → DESCRIPTION

    PNG Banking Logic:
        • "Cheque Account" = "Current Account" (merge both)
        • Individual → prefer SAVINGS, then CURRENT, then ordered fallbacks, then global fallback
        • Non-Individual → prefer CURRENT/CORPORATE/SME first, then ordered fallbacks, then global fallback
    """

    _BAD_TOKENS = {"", "NAN", "NONE", "NULL", "<NA>"}

    def __init__(self, path: str):
        if not os.path.exists(path):
            raise FileNotFoundError(f"Transaction code file not found: {path}")

        # Load CSV or Excel
        ext = os.path.splitext(path)[1].lower()
        if ext == ".csv":
            df = pd.read_csv(path)
        elif ext in [".xlsx", ".xls"]:
            df = pd.read_excel(path)
        else:
            raise ValueError(f"Unsupported file type: {ext}")

        # Normalize column names to uppercase
        df.columns = [str(c).strip().upper() for c in df.columns]

        # Auto-map actual file structure
        rename_map = {}
        for col in df.columns:
            # Account Type = Line Description
            if col in ["LINE DESCRIPTION", "DESCRIPTION", "ACCOUNT TYPE", "ACCOUNT_TYPE"]:
                rename_map[col] = "ACCOUNT_TYPE"

            # Transaction Code column
            elif col in ["TRAN. CODE", "TRAN CODE", "CODE", "TRAN CODE.", "TRAN. CODE."]:
                rename_map[col] = "CODE"

            # Transaction Description column
            elif col in ["TRAN. DESCRIPTION", "TRAN DESCRIPTION", "TRANSACTION DESCRIPTION"]:
                rename_map[col] = "DESCRIPTION"

        df = df.rename(columns=rename_map)

        # Ensure required fields exist
        required = ["ACCOUNT_TYPE", "CODE", "DESCRIPTION"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(
                f"❌ Number Codes file missing required columns: {missing}\n"
                f"Detected columns: {list(df.columns)}"
            )

        # Keep only required columns to avoid noise
        df = df[required].copy()

        # Drop fully empty rows first
        df = df.dropna(how="all")

        # Normalize all required columns safely to strings
        for col in required:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
            )

        # Remove empty / nan-like garbage rows
        df = df[
            ~df["ACCOUNT_TYPE"].str.upper().isin(self._BAD_TOKENS)
            & ~df["CODE"].str.upper().isin(self._BAD_TOKENS)
            & ~df["DESCRIPTION"].str.upper().isin(self._BAD_TOKENS)
        ].copy()

        # Final normalization
        df["ACCOUNT_TYPE"] = df["ACCOUNT_TYPE"].str.upper().str.strip()
        df["CODE"] = df["CODE"].str.upper().str.strip()
        df["DESCRIPTION"] = df["DESCRIPTION"].str.strip()

        # Merge CHEQUE → CURRENT ACCOUNT (PNG standard)
        df.loc[df["ACCOUNT_TYPE"].str.contains("CHEQUE", na=False), "ACCOUNT_TYPE"] = "CURRENT ACCOUNT"

        # Build mapping dictionary
        self.library: dict[str, dict[str, str]] = defaultdict(dict)
        for _, row in df.iterrows():
            acc = str(row["ACCOUNT_TYPE"]).strip()
            code = str(row["CODE"]).strip()
            desc = str(row["DESCRIPTION"]).strip()

            if not acc or not code or not desc:
                continue

            self.library[acc][code] = desc

        # Deterministic ordering of account types
        self.account_types = sorted(
            str(k).strip() for k in self.library.keys() if str(k).strip()
        )

    # ----------------------------------------------------------
    # Normalization helpers
    # ----------------------------------------------------------
    def _normalize_client_type(self, client_type: str) -> str:
        """
        Normalize UI/backend client type labels into stable keys:
          - "individual"
          - "non_individual"
        """
        s = str(client_type or "").strip().lower()
        if not s:
            return "individual"
        if "non" in s or "company" in s or "business" in s or "corporate" in s:
            return "non_individual"
        return "individual"

    # ----------------------------------------------------------
    # Lookup Logic (PNG Banking)
    # ----------------------------------------------------------
    def get_description(self, code: int | str, client_type: str) -> str:
        """
        Resolve a transaction code description using a deterministic preference order.

        Individual preference order:
            SAVINGS ACCOUNT → CURRENT ACCOUNT → REMITTANCE SYSTEM(S) → FIXED DEPOSIT(S) → LOANS →
            TRADE FINANCE → MONEY MARKET → CUSTOMER INFORMATION FILE → ANYTHING (global fallback)

        Non-individual preference order:
            CURRENT / CORPORATE / SME → (ordered fallbacks) → SAVINGS ACCOUNT → ANYTHING (global fallback)
        """
        if code is None:
            return "UNKNOWN"

        code = str(code).strip().upper()
        if code in self._BAD_TOKENS:
            return "UNKNOWN"

        ct = self._normalize_client_type(client_type)

        ordered_after_current = [
            "REMITTANCE SYSTEM",
            "REMITTANCE SYSTEMS",
            "FIXED DEPOSIT",
            "FIXED DEPOSITS",
            "LOANS",
            "TRADE FINANCE",
            "MONEY MARKET",
            "CUSTOMER INFORMATION FILE",
        ]

        if ct == "individual":
            preferred_exact = ["SAVINGS ACCOUNT", "CURRENT ACCOUNT"] + ordered_after_current
            preferred_keywords = ["SAVING", "CURRENT", "REMITT", "FIXED", "LOAN", "TRADE", "MONEY", "CUSTOMER"]
        else:
            preferred_exact = ["CURRENT ACCOUNT", "CORPORATE ACCOUNT", "SME ACCOUNT"] + ordered_after_current + ["SAVINGS ACCOUNT"]
            preferred_keywords = ["CURRENT", "CORPORATE", "SME", "REMITT", "FIXED", "LOAN", "TRADE", "MONEY", "CUSTOMER", "SAVING"]

        # 1) Exact match lookup in preferred order
        for acc in preferred_exact:
            if acc in self.library and code in self.library[acc]:
                return self.library[acc][code]

        # 2) Fuzzy keyword lookup (covers slight naming differences)
        for kw in preferred_keywords:
            for acc in self.account_types:
                if kw in acc and code in self.library.get(acc, {}):
                    return self.library[acc][code]

        # 3) Final global fallback (search ANY account type)
        for acc in self.account_types:
            if code in self.library.get(acc, {}):
                return self.library[acc][code]

        return "UNKNOWN"

    # ----------------------------------------------------------
    def summary(self, limit: int = 10) -> str:
        parts = [f"Account Types Loaded: {len(self.account_types)}"]
        total = sum(len(v) for v in self.library.values())
        parts.append(f"Total Transaction Codes: {total}")
        parts.append("Sample:")

        shown = 0
        for acc in self.account_types:
            mapping = self.library.get(acc, {})
            for code, desc in mapping.items():
                parts.append(f"[{acc}] {code}: {desc}")
                shown += 1
                if shown >= limit:
                    return "\n".join(parts)

        return "\n".join(parts)