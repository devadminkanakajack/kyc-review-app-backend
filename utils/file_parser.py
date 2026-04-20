import os
import re
import pandas as pd
from difflib import SequenceMatcher
from typing import Any


# ---------------------------------------------------------------
# Performance controls
# ---------------------------------------------------------------

# Full pairwise alias clustering is expensive on large statements.
# Keep the same functionality for small/normal statements, then
# progressively reduce comparison breadth as the identity universe grows.
ALIAS_FULL_CLUSTER_MAX = 300
ALIAS_BUCKET_CLUSTER_MAX = 1200


def clean_money(value):
    # Treat NaN as 0.0 (very common in statement CSVs)
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0.0
    s = str(value).replace(",", "").replace("OD", "").strip()
    try:
        return float(s)
    except Exception:
        return 0.0


# ---------------------------------------------------------------
# IDENTITY EXTRACTION LOGIC (PNG BANKING FORMATS)
# ---------------------------------------------------------------

# Generic “counterparty-like” labels we should NOT keep as identities
_GENERIC_IDENTITY_EXCLUDE = {
    "SERVICE FEE",
    "SERVICE FEES",
    "PAYMENT OF SERVICE",
    "PAYMENT OF SERVICES",
    "PAYMENT FOR SERVICE",
    "PAYMENT FOR SERVICES",
    "PAYMENT OF",
    "PAYMENT",
    "SERVICE",
    "SERVICES",
    "FAMILY ASSISTANCE",
    "FAMILY SUPPORT",
    "SUPPORT",
    "CASH DEP",
    "CSH DEP",
    "CASH DEPOSIT",
    "CASH DEPOSIT WITHOUT BOOK",
    "CASH DEPOSIT WITH BOOK",
    "WITHOUT BOOK",
    "WITH BOOK",
    "DEPOSIT",
    "WITHDRAWAL",
    "TRANSFER",
    "SAVINGS",
    "SELF DEPOSIT",
    "GIFT",
    "SCHOOL FEE",
    "ONE HUNDRED KINA",
    "DIRECT CREDIT",
    "IB OTHER ACC",
    "OTHER ACC",
    "UNKNOWN",
}

# Common noise tokens we should drop when comparing identities (aliasing)
_ALIAS_STOPWORDS = {
    "MR", "MRS", "MS", "MISS",
    "LTD", "LIMITED", "INC", "CO", "COMPANY",
    "PTY", "PTYLTD", "ENTERPRISE", "ENTERPRISES",
    "TRADING", "TRADERS",
    "THE", "AND", "&",
    "PNG",
    "BANK", "BANKING",
    "SERVICE", "SERVICES", "FEE", "FEES",
    "PAYMENT", "PAYMENTS", "PAY", "PMNT", "PMT", "OF", "FOR",
    "FAMILY", "ASSISTANCE", "SUPPORT",
    "CASH", "DEP", "DEPOSIT",
    "TRANSFER", "TRF",
    "KINA", "BA",
}

# Noise suffixes / fragments commonly attached to real names
_SUFFIX_NOISE_PATTERNS = [
    r"\bPAYMENTS?\b.*$",
    r"\bPAY\b.*$",
    r"\bSERVICE\b.*$",
    r"\bSERVICES\b.*$",
    r"\bSERVICE\s+FEE\b.*$",
    r"\bKINA\s+BA\b.*$",
    r"\bWAL_PAKPAYMENT\b.*$",
    r"\bPAYMEN\b.*$",
    r"\bPAYM\b.*$",
]


def _clean_candidate(s: Any) -> str:
    """
    Safely normalize any candidate text into uppercase cleaned text.
    Prevents crashes where pandas/numpy NaN floats reach .upper().
    """
    if s is None:
        return ""

    try:
        if pd.isna(s):
            return ""
    except Exception:
        pass

    s = str(s).upper()

    # remove wrapping brackets/quotes and normalize whitespace
    s = re.sub(r"[\[\]\(\)\{\}]", " ", s)
    s = s.replace(".", " ")
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()

    # strip leading/trailing separators/labels
    s = re.sub(r"^[\-\:\.;,/ ]+", "", s).strip()
    s = re.sub(r"[\-\:\.;,/ ]+$", "", s).strip()

    return s


def _preserve_bracket_identifier(text_raw: str) -> str:
    """
    Preserve bracketed numeric/account-like identifiers even when the description
    would otherwise be considered generic.
    """
    if not isinstance(text_raw, str):
        return ""

    m = re.search(r"\[([^\]]+)\]", text_raw)
    if not m:
        return ""

    inner = _clean_candidate(m.group(1))
    if not inner:
        return ""

    if re.fullmatch(r"\d+(?:\.\d+)?", inner):
        return f"[{inner}]"
    if re.fullmatch(r"\*{2,}\d{3,}", inner):
        return inner

    return ""


def _clean_identity_text(s: Any) -> str:
    """
    Higher-level cleaning for party names / identifier tokens.
    Keeps masked accounts and numeric identifiers intact, but
    strips common noise from name-like descriptions.
    """
    x = _clean_candidate(s)
    if not x:
        return ""

    # Preserve masked accounts exactly
    if re.fullmatch(r"\*{2,}\d{3,}", x):
        return x

    # Preserve pure numeric identifiers in their original bracket-friendly form
    if re.fullmatch(r"\d+(?:\.\d+)?", x):
        return f"[{x}]"

    # Preserve already-normalized stable identifiers
    if re.fullmatch(r"ID:\d+(?:\.\d+)?", x):
        return x
    if re.fullmatch(r"\[\d+(?:\.\d+)?\]", x):
        return x

    # Remove embedded long numbers that are usually refs appended to names,
    # but only for name-like strings (not pure identifiers).
    x = re.sub(r"\b\d{4,}\b", " ", x)

    # Normalize common OCR / punctuation issues
    x = re.sub(r"[^A-Z0-9 ]+", " ", x)
    x = re.sub(r"\s+", " ", x).strip()

    # Drop repeated full-tail duplicates: "GABRIEL SAULEP GABRIEL SAULEP"
    toks = x.split()
    if len(toks) >= 4:
        half = len(toks) // 2
        if toks[:half] == toks[half:]:
            x = " ".join(toks[:half])

    # Strip common suffix noise from real names
    for pat in _SUFFIX_NOISE_PATTERNS:
        x2 = re.sub(pat, "", x).strip()
        if x2:
            x = x2

    # Remove extra spaces again
    x = re.sub(r"\s+", " ", x).strip()

    return x


def _is_generic_identity(identity: str) -> bool:
    x = _clean_identity_text(identity)
    if not x:
        return True

    # Stable identifiers are valid counterparties and should never be collapsed.
    if re.fullmatch(r"ID:\d+(?:\.\d+)?", x):
        return False
    if re.fullmatch(r"\[\d+(?:\.\d+)?\]", x):
        return False
    if re.fullmatch(r"\*{2,}\d{3,}", x):
        return False

    if x in _GENERIC_IDENTITY_EXCLUDE:
        return True

    toks = [t for t in re.split(r"[^A-Z0-9:]+", x) if t]
    if not toks or len(x) <= 2 or x in {"NAN", "NONE", "NULL", "UNKNOWN"}:
        return True

    generic_token_hits = sum(
        1
        for t in toks
        if t in _ALIAS_STOPWORDS
        or t in {"BOOK", "WITHOUT", "WITH", "DIRECT", "CREDIT", "SAVINGS", "GIFT", "SCHOOL", "FEE"}
    )
    if generic_token_hits / max(1, len(toks)) >= 0.8:
        return True

    return False


def extract_identity(desc: str) -> str:
    """
    Party/identifier extraction rules (priority):
      1) Masked account identifiers (e.g. ****0119) when present in IB OTHER ACC / TO / FROM context
      2) Explicit party markers (B/O:, FROM, BY, WAGES FOR, REFUNDS FROM, etc.)
      3) Known mixed generic + party formats (SERVICE FEE : NAME, FAMILY ASSISTANCE NAME, CASH DEP NAME)
      4) Numeric ref + name + repeated name (e.g. 250260... GABRIEL SAULEP GABRIEL SAULEP)
      5) Bracket-only tokens like [KESIA] or [35602811]
      6) Fallback heuristics
    """
    if not isinstance(desc, str):
        return "UNKNOWN"

    text_raw = desc.strip()
    text = text_raw.upper().strip()
    if not text:
        return "UNKNOWN"

    # ----------------------------
    # 1) Prefer masked account identifiers when present (KEEP as "****0119")
    # ----------------------------
    masked = re.search(r"\*{2,}\d{3,}", text)
    if masked:
        if (
            "IB OTHER ACC" in text
            or "OTHER ACC" in text
            or re.search(r"\bTO\s+\*{2,}\d{3,}\b", text)
            or re.search(r"\bFROM\s+\*{2,}\d{3,}\b", text)
        ):
            return masked.group(0)

    # ----------------------------
    # 2) Payment on behalf of / beneficiary patterns
    # ----------------------------
    m = re.search(r"\bB/O\s*:\s*([A-Z][A-Z ]+)$", text)
    if m:
        cand = _clean_identity_text(m.group(1))
        return cand if not _is_generic_identity(cand) else "UNKNOWN"

    m = re.search(r"\bBO\s*:\s*([A-Z][A-Z ]+)$", text)
    if m:
        cand = _clean_identity_text(m.group(1))
        return cand if not _is_generic_identity(cand) else "UNKNOWN"

    # ----------------------------
    # 3) Common “generic + name” formats
    # ----------------------------
    m = re.search(r"\bSERVICE\s+FEE\S*\s*:\s*([A-Z][A-Z ]+)$", text)
    if m:
        cand = _clean_identity_text(m.group(1))
        return cand if not _is_generic_identity(cand) else "UNKNOWN"

    m = re.search(r"\bFAMILY\s+ASSISTANCE\s+([A-Z][A-Z ]+)$", text)
    if m:
        cand = _clean_identity_text(m.group(1))
        return cand if not _is_generic_identity(cand) else "UNKNOWN"

    m = re.search(r"\b(?:CASH|CSH)\s+DEP(?:OSIT)?\s*[-:]?\s+([A-Z][A-Z ]+)$", text)
    if m:
        cand = _clean_identity_text(m.group(1))
        return cand if not _is_generic_identity(cand) else "UNKNOWN"

    m = re.search(r"\bPAYMENT\s+OF\s+SERVICES?\b.*?\bB/O\s*:\s*([A-Z][A-Z ]+)", text)
    if m:
        cand = _clean_identity_text(m.group(1))
        return cand if not _is_generic_identity(cand) else "UNKNOWN"

    # ----------------------------
    # 4) Explicit directional markers
    # ----------------------------
    m = re.search(r"\bFROM\s+([A-Z0-9 ]+?)(?:\bTO\b|$)", text)
    if m:
        cand = _clean_identity_text(m.group(1))
        return cand if not _is_generic_identity(cand) else "UNKNOWN"

    m = re.search(r"\bBY\s+([A-Z0-9 ]+)$", text)
    if m:
        cand = _clean_identity_text(m.group(1))
        return cand if not _is_generic_identity(cand) else "UNKNOWN"

    m = re.search(r"\bWAGES\s+FOR\s+([A-Z0-9 ]+)$", text)
    if m:
        cand = _clean_identity_text(m.group(1))
        return cand if not _is_generic_identity(cand) else "UNKNOWN"

    m = re.search(r"\bREFUNDS\s+FROM\s+([A-Z0-9 ]+)$", text)
    if m:
        cand = _clean_identity_text(m.group(1))
        return cand if not _is_generic_identity(cand) else "UNKNOWN"

    # ----------------------------
    # 5) IB OTHER ACC patterns (if no masked found earlier)
    # ----------------------------
    m = re.search(r"\bIB\s+OTHER\s+ACC\s+([A-Z][A-Z0-9 ]+?)(?:\bTO\b|$)", text)
    if m:
        cand = _clean_identity_text(m.group(1))
        return cand if not _is_generic_identity(cand) else "UNKNOWN"

    # ----------------------------
    # 6) Numeric ref + repeated name pattern
    # ----------------------------
    m = re.search(r"^\d{10,}\s+([A-Z][A-Z ]+?)\s+\1\s*$", text)
    if m:
        cand = _clean_identity_text(m.group(1))
        return cand if not _is_generic_identity(cand) else "UNKNOWN"

    m = re.search(r"^\d{10,}\s+([A-Z][A-Z ]+)\s*$", text)
    if m:
        cand = _clean_identity_text(m.group(1))
        return cand if not _is_generic_identity(cand) else "UNKNOWN"

    # ----------------------------
    # 7) Bracket-only tokens like [KESIA] or [35602811]
    # Keep numeric identifiers too.
    # ----------------------------
    if "[" in text_raw or "]" in text_raw:
        preserved = _preserve_bracket_identifier(text_raw)
        if preserved:
            return preserved

        m = re.search(r"\[([^\]]+)\]", text_raw)
        if m:
            bracket = _clean_identity_text(m.group(1))
            if bracket and not _is_generic_identity(bracket):
                return bracket

        # fallback if bracket is malformed / missing close bracket
        bracket = _clean_identity_text(text_raw)
        if bracket and not _is_generic_identity(bracket):
            return bracket

    # ----------------------------
    # 8) Final fallback: try split on " TO " but avoid returning generic labels
    # ----------------------------
    if " TO " in text:
        left = _clean_identity_text(text.split(" TO ")[0])
        return left if not _is_generic_identity(left) else "UNKNOWN"

    cand = _clean_identity_text(text[:80])
    return cand if not _is_generic_identity(cand) else "UNKNOWN"


# ---------------------------------------------------------------
# DYNAMIC ALIASING / PARTY RECOGNITION (per statement)
# ---------------------------------------------------------------

def _norm_for_alias(name: str) -> str:
    """
    Normalize identity strings for alias comparison.
    Keeps letters/digits but removes punctuation and collapses whitespace.
    Drops stopwords and single-letter noise tokens where possible.
    """
    x = _clean_identity_text(name)
    if not x:
        return ""

    # Keep stable identifiers and masked accounts exact
    if x.startswith("ID:"):
        return x
    if re.fullmatch(r"\[\d+(?:\.\d+)?\]", x):
        return x
    if re.fullmatch(r"\*{2,}\d{3,}", x):
        return x

    x = re.sub(r"[^A-Z0-9 ]+", " ", x)
    x = re.sub(r"\s+", " ", x).strip()

    # common typo normalizations / compaction help
    x = x.replace("KESHIA", "KESIA")
    x = x.replace("KESHIA", "KESIA")
    x = x.replace("KESVA", "KESIA")
    x = x.replace("KESINANKIWALPAK", "KESIA NANKI WALPAK")
    x = x.replace("KESIANANKIWALPAK", "KESIA NANKI WALPAK")
    x = x.replace("NANKING", "NANKI")
    x = x.replace("NENKI", "NANKI")
    x = x.replace("NENKIA", "NANKI")
    x = x.replace("NAKI", "NANKI")
    x = x.replace("MANKI", "NANKI")
    x = x.replace("NANKL", "NANKI")
    x = x.replace("WAIPAK", "WALPAK")
    x = x.replace("WAL PAK", "WALPAK")

    # remove embedded long numeric refs from name-like strings
    x = re.sub(r"\b\d{4,}\b", " ", x)

    # strip common suffix noise again at alias stage
    for pat in _SUFFIX_NOISE_PATTERNS:
        x2 = re.sub(pat, "", x).strip()
        if x2:
            x = x2

    # split compact token like KESINANKIWALPAK if still present
    compact = x.replace(" ", "")
    if compact == "KESINANKIWALPAK":
        x = "KESIA NANKI WALPAK"

    tokens = [t for t in x.split() if t]

    # Drop stopwords + obvious noise tokens
    tokens2 = []
    for t in tokens:
        if t in _ALIAS_STOPWORDS:
            continue
        if len(t) == 1:
            continue
        tokens2.append(t)

    # If everything got stripped, fall back to original cleaned tokens
    if not tokens2:
        tokens2 = [t for t in tokens if t not in _ALIAS_STOPWORDS]

    return " ".join(tokens2).strip()


def _tokenize_for_alias(s: Any) -> list[str]:
    cleaned = _norm_for_alias(s)
    if not cleaned:
        return []
    if (
        cleaned.startswith("ID:")
        or re.fullmatch(r"\[\d+(?:\.\d+)?\]", cleaned)
        or re.fullmatch(r"\*{2,}\d{3,}", cleaned)
    ):
        return [cleaned]
    return [t for t in cleaned.split() if t]


def _initials(tokens: list[str]) -> str:
    return "".join(t[0] for t in tokens if t and t[0].isalpha())


def _is_partial_name_match(short_tokens: list[str], long_tokens: list[str]) -> bool:
    if not short_tokens or not long_tokens:
        return False
    if len(short_tokens) == 1 and (
        short_tokens[0].startswith("ID:")
        or re.fullmatch(r"\[\d+(?:\.\d+)?\]", short_tokens[0])
    ):
        return short_tokens == long_tokens
    sset = set(short_tokens)
    lset = set(long_tokens)
    if sset and sset.issubset(lset):
        return True
    if len(short_tokens) >= 2 and len(long_tokens) >= 2 and short_tokens[0] == long_tokens[0] and short_tokens[-1] == long_tokens[-1]:
        return True
    s_init = _initials(short_tokens)
    l_init = _initials(long_tokens)
    if s_init and l_init and s_init == l_init and short_tokens[0] == long_tokens[0]:
        return True
    return False


def _tokenwise_edit_ratio(a_tokens: list[str], b_tokens: list[str]) -> float:
    if not a_tokens or not b_tokens:
        return 0.0
    scores = []
    for at in a_tokens:
        best = 0.0
        for bt in b_tokens:
            best = max(best, SequenceMatcher(None, at, bt).ratio())
        scores.append(best)
    return sum(scores) / len(scores) if scores else 0.0


def _is_same_party_dynamic(a: Any, b: Any) -> bool:
    na = _norm_for_alias(a)
    nb = _norm_for_alias(b)
    if not na or not nb:
        return False
    if (
        na.startswith("ID:")
        or nb.startswith("ID:")
        or re.fullmatch(r"\[\d+(?:\.\d+)?\]", na)
        or re.fullmatch(r"\[\d+(?:\.\d+)?\]", nb)
    ):
        return na == nb
    if re.fullmatch(r"\*{2,}\d{3,}", na) or re.fullmatch(r"\*{2,}\d{3,}", nb):
        return na == nb
    ta = _tokenize_for_alias(na)
    tb = _tokenize_for_alias(nb)
    if not ta or not tb:
        return False
    sa, sb = set(ta), set(tb)
    jac = _jaccard(sa, sb)
    seq = SequenceMatcher(None, na, nb).ratio()
    token_edit = max(_tokenwise_edit_ratio(ta, tb), _tokenwise_edit_ratio(tb, ta))
    if _is_partial_name_match(ta, tb) or _is_partial_name_match(tb, ta):
        return True
    if jac >= 0.75 or seq >= 0.90:
        return True
    if token_edit >= 0.88 and len(sa & sb) >= 1:
        return True
    if ta[0] == tb[0] and token_edit >= 0.82:
        return True
    if len(ta) >= 2 and len(tb) >= 2:
        first_sim = SequenceMatcher(None, ta[0], tb[0]).ratio()
        last_sim = SequenceMatcher(None, ta[-1], tb[-1]).ratio()
        if first_sim >= 0.85 and last_sim >= 0.80:
            return True
    return False


def _token_set(s: str) -> set:
    s2 = _norm_for_alias(s)
    return set([t for t in s2.split() if t])


def _jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _seq_ratio(a: str, b: str) -> float:
    a2 = _norm_for_alias(a)
    b2 = _norm_for_alias(b)
    if not a2 or not b2:
        return 0.0
    return SequenceMatcher(None, a2, b2).ratio()


def _looks_like_masked_account(x: Any) -> bool:
    if x is None:
        return False
    try:
        if pd.isna(x):
            return False
    except Exception:
        pass
    return bool(re.fullmatch(r"\*{2,}\d{3,}", str(x).strip()))


def _looks_like_numeric_identifier(x: Any) -> bool:
    if x is None:
        return False
    try:
        if pd.isna(x):
            return False
    except Exception:
        pass
    s = str(x).strip()
    return bool(
        re.fullmatch(r"ID:\d+(?:\.\d+)?", s)
        or re.fullmatch(r"\[\d+(?:\.\d+)?\]", s)
    )


def _masked_key(s: str) -> str:
    return str(s or "").strip()


def _prefix_key_for_bucket(norm_s: str) -> str:
    toks = [t for t in str(norm_s or "").split() if t]
    if not toks:
        return ""
    first = toks[0][:4]
    second = toks[1][:4] if len(toks) > 1 else ""
    return f"{first}|{second}"


def _choose_canonical(members, freq_map):
    members_sorted = sorted(
        members,
        key=lambda x: (freq_map.get(x, 0), len(_tokenize_for_alias(x)), len(_clean_identity_text(x))),
        reverse=True,
    )

    best = members_sorted[0] if members_sorted else ""
    best_clean = _clean_identity_text(best)
    best_norm = _norm_for_alias(best)

    if (
        best_norm
        and not best_norm.startswith("ID:")
        and not re.fullmatch(r"\[\d+(?:\.\d+)?\]", best_norm)
        and not re.fullmatch(r"\*{2,}\d{3,}", best_norm)
    ):
        return best_norm

    return best_clean if best_clean else ""



class _DSU:
    def __init__(self, n: int):
        self.p = list(range(n))
        self.r = [0] * n

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.r[ra] < self.r[rb]:
            self.p[ra] = rb
        elif self.r[ra] > self.r[rb]:
            self.p[rb] = ra
        else:
            self.p[rb] = ra
            self.r[ra] += 1


def build_identity_alias_map(identities: pd.Series) -> dict:
    """
    Build a dynamic alias→canonical mapping from identities observed in THIS statement.
    - Clusters near-duplicates (MONICA KILA ~ MONICA K.)
    - Keeps masked accounts separate
    - Keeps numeric identifiers separate (ID:35602811)

    Performance path:
    - small unique identity sets: full pairwise clustering
    - medium sets: compare within similarity buckets only
    - large sets: normalization-first canonicalization without pairwise linkage
    """
    vals = [x for x in identities.dropna().astype(str).tolist() if str(x).strip()]
    seen = set()
    uniq = []
    for v in vals:
        if v not in seen:
            seen.add(v)
            uniq.append(v)

    if not uniq:
        return {}

    masked = [u for u in uniq if _looks_like_masked_account(u)]
    numeric_ids = [u for u in uniq if _looks_like_numeric_identifier(u)]
    names = [u for u in uniq if not _looks_like_masked_account(u) and not _looks_like_numeric_identifier(u)]

    alias_map = {}

    # masked accounts: canonical is itself
    for m in masked:
        alias_map[m] = _masked_key(m)

    # numeric identifiers: canonical is itself
    for n in numeric_ids:
        alias_map[n] = str(n).strip()

    if not names:
        return alias_map

    freq = pd.Series(vals).value_counts().to_dict()
    norm_cache = {nm: _norm_for_alias(nm) for nm in names}
    token_cache = {nm: set([t for t in norm_cache[nm].split() if t]) for nm in names}

    # ------------------------------------------------------------
    # Fast-path for very large identity universes:
    # collapse only exact normalized matches.
    # ------------------------------------------------------------
    if len(names) > ALIAS_BUCKET_CLUSTER_MAX:
        by_norm = {}
        for nm in names:
            nk = norm_cache.get(nm, "")
            if not nk:
                nk = _clean_identity_text(nm)
            by_norm.setdefault(nk, []).append(nm)

        for _, members in by_norm.items():
            canonical = _choose_canonical(members, freq)
            for m in members:
                alias_map[m] = canonical
        return alias_map

    # ------------------------------------------------------------
    # Medium path: compare only inside coarse similarity buckets.
    # ------------------------------------------------------------
    if len(names) > ALIAS_FULL_CLUSTER_MAX:
        buckets = {}
        for nm in names:
            nk = norm_cache.get(nm, "")
            bucket_key = _prefix_key_for_bucket(nk)
            if not bucket_key:
                bucket_key = _clean_identity_text(nm)[:8]
            buckets.setdefault(bucket_key, []).append(nm)

        for _, members in buckets.items():
            if len(members) == 1:
                alias_map[members[0]] = _choose_canonical(members, freq)
                continue

            n = len(members)
            dsu = _DSU(n)
            local_norm = [norm_cache[m] for m in members]
            local_tokens = [token_cache[m] for m in members]

            for i in range(n):
                for j in range(i + 1, n):
                    if not local_norm[i] or not local_norm[j]:
                        continue

                    jac = _jaccard(local_tokens[i], local_tokens[j])
                    seq = SequenceMatcher(None, local_norm[i], local_norm[j]).ratio()
                    contains = (local_norm[i] in local_norm[j]) or (local_norm[j] in local_norm[i])

                    if _is_same_party_dynamic(members[i], members[j]):
                        dsu.union(i, j)

            groups = {}
            for idx, nm in enumerate(members):
                root = dsu.find(idx)
                groups.setdefault(root, []).append(nm)

            for _, grp_members in groups.items():
                canonical = _choose_canonical(grp_members, freq)
                for m in grp_members:
                    alias_map[m] = canonical

        return alias_map

    # ------------------------------------------------------------
    # Full pairwise path for manageable identity universes.
    # ------------------------------------------------------------
    n = len(names)
    dsu = _DSU(n)
    norm_strs = [norm_cache[x] for x in names]
    token_sets = [token_cache[x] for x in names]

    for i in range(n):
        for j in range(i + 1, n):
            if not norm_strs[i] or not norm_strs[j]:
                continue

            jac = _jaccard(token_sets[i], token_sets[j])
            seq = SequenceMatcher(None, norm_strs[i], norm_strs[j]).ratio()
            contains = (norm_strs[i] in norm_strs[j]) or (norm_strs[j] in norm_strs[i])

            if _is_same_party_dynamic(names[i], names[j]):
                dsu.union(i, j)

    groups = {}
    for idx, nm in enumerate(names):
        root = dsu.find(idx)
        groups.setdefault(root, []).append(nm)

    for _, members in groups.items():
        canonical = _choose_canonical(members, freq)
        for m in members:
            alias_map[m] = canonical

    return alias_map


# ---------------------------------------------------------------
# Loader utilities
# ---------------------------------------------------------------

def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [re.sub(r"\s+", " ", str(col).strip().upper()) for col in df.columns]
    return df


def _apply_rename_map(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        # Dates
        "DATE": "DATE",
        "TXN_DATE": "DATE",
        "VALUE_DATE": "DATE",
        "POST_DATE": "DATE",
        "TRAN_DATE": "DATE",
        "DATE_POSTED": "DATE",

        # Codes
        "TRANSCODE/REF NO.": "TRANSCODE",
        "TRANSCODE/REF": "TRANSCODE",
        "TRANSCODE": "TRANSCODE",

        # Amounts
        "WITHDRAWAL": "DEBIT",
        "DEBIT": "DEBIT",
        "DEPOSIT": "CREDIT",
        "CREDIT": "CREDIT",
        "BALANCE": "BALANCE",

        # Narrative
        "TRANS DESCRIPTION": "DESCRIPTION_RAW",
        "TRANSACTION DESCRIPTION": "DESCRIPTION_RAW",
        "DESCRIPTION": "DESCRIPTION_RAW",
        "NARRATIVE": "DESCRIPTION_RAW",
    }

    cols = set(df.columns)
    use_map = {k: v for k, v in rename_map.items() if k in cols}
    return df.rename(columns=use_map)


def _clean_date_strings(series: pd.Series) -> pd.Series:
    s = series

    if pd.api.types.is_datetime64_any_dtype(s):
        return s.dt.strftime("%d/%m/%Y").fillna("")

    s = s.astype(str)

    s = (
        s.str.replace("\ufeff", "", regex=False)
         .str.replace("\u00A0", " ", regex=False)
         .str.replace("\t", " ", regex=False)
         .str.replace("\r", " ", regex=False)
         .str.replace("\n", " ", regex=False)
         .str.strip()
    )

    s = s.str.replace(r"^(\d{4}[-/]\d{1,2}[-/]\d{1,2}).*$", r"\1", regex=True)

    s = (
        s.str.replace(".", "/", regex=False)
         .str.replace("-", "/", regex=False)
    )

    s = s.str.replace(r"\s+", " ", regex=True).str.strip()
    s = s.replace({"nan": "", "NaN": "", "None": "", "NULL": "", "": ""})

    return s


def _parse_dates_best_effort(series: pd.Series) -> pd.Series:
    s_str = _clean_date_strings(series)

    numeric = pd.to_numeric(s_str, errors="coerce")
    numeric_ratio = float(numeric.notna().mean()) if len(s_str) else 0.0
    if numeric_ratio >= 0.60:
        try:
            return pd.to_datetime(numeric, unit="D", origin="1899-12-30", errors="coerce")
        except Exception:
            pass

    dt_explicit = pd.to_datetime(s_str, format="%d/%m/%Y", errors="coerce")
    ok_explicit = float(dt_explicit.notna().mean()) if len(dt_explicit) else 0.0
    if ok_explicit >= 0.70:
        return dt_explicit

    dt_explicit2 = pd.to_datetime(s_str, format="%d/%m/%y", errors="coerce")
    ok_explicit2 = float(dt_explicit2.notna().mean()) if len(dt_explicit2) else 0.0
    if ok_explicit2 > ok_explicit and ok_explicit2 >= 0.70:
        return dt_explicit2

    dt_iso = pd.to_datetime(s_str, format="%Y/%m/%d", errors="coerce")
    ok_iso = float(dt_iso.notna().mean()) if len(dt_iso) else 0.0
    if ok_iso >= 0.70 and ok_iso > max(ok_explicit, ok_explicit2):
        return dt_iso

    dt1 = pd.to_datetime(s_str, dayfirst=True, errors="coerce")
    ok1 = float(dt1.notna().mean()) if len(dt1) else 0.0
    if ok1 >= 0.70:
        return dt1

    dt2 = pd.to_datetime(s_str, dayfirst=False, errors="coerce")
    ok2 = float(dt2.notna().mean()) if len(dt2) else 0.0
    return dt2 if ok2 > ok1 else dt1


def load_transactions(path: str, code_lookup, client_type: str) -> pd.DataFrame:
    """
    Loads CSV/XLSX, standardizes columns, parses DATE safely for recurrence,
    cleans amounts, extracts IDENTITY, dynamically clusters aliases, and attaches DESCRIPTION.

    Performance improvements:
    - assigns ROW_ID once at parser stage
    - reduces repeated dataframe copies
    - uses map/list-based assignments where practical
    - applies staged alias clustering for large identity universes
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(path, encoding_errors="replace", dtype=str)
    else:
        df = pd.read_excel(path, dtype=str)

    df = df.dropna(how="all")
    df = _normalize_headers(df)
    df = _apply_rename_map(df)

    required = ["DATE", "TRANSCODE", "DEBIT", "CREDIT", "BALANCE", "DESCRIPTION_RAW"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns: {missing}")

    # TRANSCODE always string
    df["TRANSCODE"] = (
        df["TRANSCODE"]
        .astype(str)
        .replace(["nan", "NaN", "None", ""], "UNKNOWN")
    )

    # Remove first row if it looks like B/F
    if len(df) > 0:
        first_desc = str(df.iloc[0]["DESCRIPTION_RAW"]).upper()
        first_code = str(df.iloc[0]["TRANSCODE"]).upper()
        if ("B/F" in first_desc) or ("BALANCE" in first_desc and "FORWARD" in first_desc) or ("B/F" in first_code):
            df = df.iloc[1:]

    # Remove any B/F rows inside statements
    bf_mask = (
        df["DESCRIPTION_RAW"].astype(str).str.contains("B/F", case=False, na=False)
        | df["TRANSCODE"].astype(str).str.contains("B/F", case=False, na=False)
    )
    df = df.loc[~bf_mask].reset_index(drop=True)

    # Single stable row id for downstream detectors
    df["ROW_ID"] = df.index.astype(int)

    # DATE
    df["DATE_RAW"] = df["DATE"]
    df["DATE"] = _parse_dates_best_effort(df["DATE"])

    # Clean numeric columns
    df["DEBIT"] = df["DEBIT"].map(clean_money)
    df["CREDIT"] = df["CREDIT"].map(clean_money)
    df["BALANCE"] = df["BALANCE"].map(clean_money)

    # Attach channel description early
    try:
        transcodes = df["TRANSCODE"].tolist()
        descs = [code_lookup.get_description(x, client_type) for x in transcodes]
        df["DESCRIPTION"] = pd.Series(descs, index=df.index).astype(str).replace(["nan", "NaN", "None", ""], "UNKNOWN")
    except Exception:
        df["DESCRIPTION"] = "UNKNOWN"

    # Identity extraction (raw)
    desc_raw = df["DESCRIPTION_RAW"].astype(str)
    identities_raw = [extract_identity(x) for x in desc_raw.tolist()]
    df["IDENTITY_RAW"] = pd.Series(identities_raw, index=df.index).replace("UNKNOWN", None)

    # Dynamic alias recognition: cluster identities observed in THIS statement
    alias_map = build_identity_alias_map(df["IDENTITY_RAW"])
    if alias_map:
        df["IDENTITY"] = df["IDENTITY_RAW"].map(alias_map).where(df["IDENTITY_RAW"].notna(), None)
    else:
        df["IDENTITY"] = df["IDENTITY_RAW"]

    # Optional: stable cluster id (useful for reporting/aggregation)
    canon_vals = [x for x in df["IDENTITY"].dropna().astype(str).unique().tolist() if x.strip()]
    canon_to_id = {c: i + 1 for i, c in enumerate(sorted(canon_vals))}
    if canon_to_id:
        df["IDENTITY_CLUSTER_ID"] = df["IDENTITY"].map(lambda x: canon_to_id.get(str(x), None) if x else None)
    else:
        df["IDENTITY_CLUSTER_ID"] = None

    # Date string for reporting
    df["DATE_STR"] = df["DATE"].dt.strftime("%Y-%m-%d").fillna("")

    return df