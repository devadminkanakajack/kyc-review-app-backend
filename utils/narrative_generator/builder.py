from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import re

from utils.kyc_rules import REQUIRED_HEADINGS


def _sf(v: Any) -> float:
    try:
        return float(v or 0.0)
    except Exception:
        return 0.0


def _si(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _ss(v: Any) -> str:
    return str(v or "").strip()


def _fmt_hits(hits: Any) -> str:
    if isinstance(hits, list):
        hits = [str(x) for x in hits if str(x).strip()]
        return ", ".join(hits)
    return _ss(hits)


def _top(items: List[Dict[str, Any]], key: str, n: int = 8) -> List[Dict[str, Any]]:
    return sorted(items or [], key=lambda x: _sf(x.get(key)), reverse=True)[: max(0, int(n))]


def _channel_pct(
    ch: Dict[str, Any],
    direction: str,
    material_by_code: Optional[Dict[str, Dict[str, Any]]] = None,
) -> float:
    if direction == "credit":
        for k in ("CR%", "credit_pct", "cr_pct", "cr"):
            if k in ch:
                return _sf(ch.get(k))

        code = _ss(ch.get("TRANSCODE"))
        src = (material_by_code or {}).get(code) or {}
        for k in ("CR%", "credit_pct", "cr_pct", "cr"):
            if k in src:
                return _sf(src.get(k))
        return 0.0

    for k in ("DR%", "debit_pct", "dr_pct", "dr"):
        if k in ch:
            return _sf(ch.get(k))

    code = _ss(ch.get("TRANSCODE"))
    src = (material_by_code or {}).get(code) or {}
    for k in ("DR%", "debit_pct", "dr_pct", "dr"):
        if k in src:
            return _sf(src.get(k))
    return 0.0


def _ymd_to_dmy(s: Any) -> str:
    t = _ss(s)
    if not t:
        return ""
    if re.match(r"^\d{2}/\d{2}/\d{4}$", t):
        return t
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", t)
    if not m:
        return t
    yyyy, mm, dd = m.group(1), m.group(2), m.group(3)
    return f"{dd}/{mm}/{yyyy}"


def _fmt_range(dmin: Any, dmax: Any) -> str:
    a = _ymd_to_dmy(dmin)
    b = _ymd_to_dmy(dmax)
    if not a and not b:
        return "N/A"
    if a and (a == b or not b):
        return a
    if b and not a:
        return b
    return f"{a} - {b}"


def _risk_label_for_reason(reason: str) -> str:
    r = _ss(reason).lower()
    if r in ("structured_deposits", "structured_payments"):
        return "Structuring"
    if r == "pass_through":
        return "Pass-through"
    if r == "layering":
        return "Layering"
    if r == "round_figures":
        return "Round-figure amounts"
    if r == "salary_pattern":
        return "Salary-like pattern"
    if r == "cash_intensive":
        return "Cash-intensive activity"
    if r == "third_party":
        return "Third-party activity"
    if r == "recurrence":
        return "Recurrence"
    return reason


def _risk_priority(risk_label: str) -> int:
    if risk_label == "Structuring":
        return 0
    if risk_label == "Layering":
        return 1
    if risk_label == "Pass-through":
        return 2
    if risk_label == "Third-party activity":
        return 3
    if risk_label == "Cash-intensive activity":
        return 4
    if risk_label == "Round-figure amounts":
        return 5
    if risk_label == "Salary-like pattern":
        return 6
    if risk_label == "Recurrence":
        return 7
    return 10


def _extract_identifier_groups(stats: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fail-soft support for different analyze_statement payload shapes.
    Accepts any of:
      - identifiers
      - top_identifiers
      - parties
      - top_parties_detailed
    """
    if not isinstance(stats, dict):
        return []

    candidates = (
        stats.get("identifiers"),
        stats.get("top_identifiers"),
        stats.get("parties"),
        stats.get("top_parties_detailed"),
    )

    picked: List[Dict[str, Any]] = []
    for cand in candidates:
        if isinstance(cand, list):
            for item in cand:
                if not isinstance(item, dict):
                    continue

                label = (
                    _ss(item.get("identifier"))
                    or _ss(item.get("label"))
                    or _ss(item.get("party"))
                    or _ss(item.get("name"))
                )
                if not label:
                    continue

                picked.append(
                    {
                        "identifier": label,
                        "count": _si(item.get("count")),
                        "total": _sf(item.get("total") or item.get("total_amount")),
                        "date_min": item.get("date_min"),
                        "date_max": item.get("date_max"),
                    }
                )
            if picked:
                break

    picked = [
        x for x in picked
        if x.get("identifier") and (_si(x.get("count")) > 0 or _sf(x.get("total")) > 0)
    ]

    return sorted(
        picked,
        key=lambda x: (-_si(x.get("count")), -_sf(x.get("total")), _ss(x.get("identifier"))),
    )


def _render_identifier_groups(stats: Dict[str, Any], indent: str = "      ", top_n: Optional[int] = None) -> List[str]:
    groups = _extract_identifier_groups(stats)
    if not groups:
        return []

    lines: List[str] = [f"{indent}Recurring Parties:"]
    display_groups = groups if top_n is None else groups[: max(1, int(top_n))]
    for item in display_groups:
        ident = _ss(item.get("identifier"))
        count = _si(item.get("count"))
        total = _sf(item.get("total"))
        rng = _fmt_range(item.get("date_min"), item.get("date_max"))
        lines.append(
            f"{indent}  • {ident}: {count}x | K{total:,.2f} | {rng}"
        )
    return lines


def _structuring_evidence_hint(stats: Dict[str, Any]) -> str:
    hints: List[str] = []

    band = _ss(stats.get("band_name") or stats.get("threshold_band") or stats.get("band"))
    threshold = _sf(stats.get("threshold"))
    near_count = _si(stats.get("near_band_count") or stats.get("near_count"))
    sub_count = _si(stats.get("sub_threshold_count"))
    distinct_amounts = _si(stats.get("distinct_amounts"))

    if band:
        hints.append(f"Band: {band}")
    elif threshold > 0:
        hints.append(f"Threshold focus: K{threshold:,.2f}")

    if near_count > 0:
        hints.append(f"Near-threshold entries: {near_count}")
    if sub_count > 0:
        hints.append(f"Sub-threshold entries: {sub_count}")
    if distinct_amounts > 0:
        hints.append(f"Distinct amounts: {distinct_amounts}")

    if not hints:
        return ""

    return " | ".join(hints[:4])


def _layering_evidence_hint(stats: Dict[str, Any]) -> str:
    hints: List[str] = []

    chains = _si(stats.get("chain_count") or stats.get("chains") or stats.get("chains_kept"))
    parties = _si(stats.get("unique_parties") or stats.get("party_count"))
    channels = _si(stats.get("unique_channels") or stats.get("channel_count"))
    link_ratio = _sf(stats.get("link_ratio") or stats.get("outflow_ratio"))
    lag_avg = stats.get("avg_lag_days") or stats.get("lag_avg_days")
    frag = stats.get("fragmentation_index")

    if chains > 0:
        hints.append(f"Chains: {chains}")
    if parties > 0:
        hints.append(f"Parties: {parties}")
    if channels > 0:
        hints.append(f"Channels: {channels}")
    if link_ratio > 0:
        hints.append(f"Outflow linkage: {link_ratio:.2f}")
    if lag_avg not in (None, "", "None"):
        try:
            hints.append(f"Avg lag: {float(lag_avg):.1f} days")
        except Exception:
            pass
    if frag not in (None, "", "None"):
        try:
            hints.append(f"Fragmentation: {float(frag):.2f}")
        except Exception:
            pass

    if not hints:
        return ""

    return " | ".join(hints[:4])


def _pass_through_evidence_hint(stats: Dict[str, Any]) -> str:
    hints: List[str] = []

    fast_ratio = _sf(stats.get("fast_outflow_ratio") or stats.get("window_ratio"))
    win_days = _si(stats.get("window_days"))
    window_count = _si(stats.get("window_count"))
    retained = _sf(stats.get("retained_ratio"))

    if win_days > 0:
        hints.append(f"Window: {win_days} days")
    if window_count > 0:
        hints.append(f"Flagged windows: {window_count}")
    if fast_ratio > 0:
        hints.append(f"Fast outflow ratio: {fast_ratio:.2f}")
    if retained > 0:
        hints.append(f"Retained ratio: {retained:.2f}")

    if not hints:
        return ""

    return " | ".join(hints[:4])


def _generic_evidence_hint(stats: Dict[str, Any], risk_label: str) -> str:
    if not isinstance(stats, dict):
        return ""

    if risk_label == "Structuring":
        return _structuring_evidence_hint(stats)
    if risk_label == "Layering":
        return _layering_evidence_hint(stats)
    if risk_label == "Pass-through":
        return _pass_through_evidence_hint(stats)

    hints: List[str] = []

    uniq = _si(stats.get("unique_parties"))
    if uniq > 0:
        hints.append(f"Parties: {uniq}")

    top_amount = _sf(stats.get("top_amount") or stats.get("dominant_amount"))
    if top_amount > 0:
        hints.append(f"Dominant amount: K{top_amount:,.2f}")

    top_freq = _si(stats.get("top_amount_count") or stats.get("dominant_frequency"))
    if top_freq > 0:
        hints.append(f"Frequency: {top_freq}")

    if not hints:
        return ""

    return " | ".join(hints[:3])


def _render_channel_suspicious_summaries(
    material_channel: Optional[Dict[str, Any]],
    direction: str,
) -> List[str]:
    ch = material_channel or {}
    by_reason = ch.get("detector_suspicious_by_reason") or {}
    if not isinstance(by_reason, dict) or not by_reason:
        return []

    rows: List[Tuple[str, Dict[str, Any]]] = []
    for reason, payload in by_reason.items():
        if not isinstance(payload, dict):
            continue
        stats = payload.get(direction) or {}
        if not isinstance(stats, dict):
            continue

        cnt = _si(stats.get("count"))
        tot = _sf(stats.get("total"))
        if cnt <= 0 and tot <= 0:
            continue

        rows.append((_risk_label_for_reason(str(reason)), stats))

    if not rows:
        return []

    rows = sorted(
        rows,
        key=lambda item: (
            _risk_priority(item[0]),
            -_si(item[1].get("count")),
            -_sf(item[1].get("total")),
            item[0],
        ),
    )

    lines: List[str] = ["  - Suspicious Transactions"]

    for risk_label, stats in rows:
        cnt = _si(stats.get("count"))
        tot = _sf(stats.get("total"))
        rng = _fmt_range(stats.get("date_min"), stats.get("date_max"))
        parties = _si(stats.get("unique_parties"))
        top_parties = [_ss(x) for x in (stats.get("top_parties") or []) if _ss(x)][:3]
        top_txt = f" | Top parties: {', '.join(top_parties)}" if top_parties else ""

        lines.append(f"    - {risk_label}")
        lines.append(
            f"      ✓ Date range: {rng} | Transactions: {cnt}x | Total: K{tot:,.2f} | Parties: {parties}{top_txt}"
        )

        evidence_hint = _generic_evidence_hint(stats, risk_label)
        if evidence_hint:
            lines.append(f"      ✓ Evidence: {evidence_hint}")

        ident_lines = _render_identifier_groups(stats, indent='      ', top_n=None)
        if ident_lines:
            lines.extend(ident_lines)

    return lines


def _render_credit_channels(
    profile_credit_channels: List[Dict[str, Any]],
    top_n: int = 8,
    material_by_code: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[str]:
    lines: List[str] = []
    ranked = sorted(
        profile_credit_channels or [],
        key=lambda ch: _channel_pct(ch, "credit", material_by_code=material_by_code),
        reverse=True,
    )[: max(0, int(top_n))]

    for ch in ranked:
        code = _ss(ch.get("TRANSCODE"))
        desc = _ss(ch.get("DESCRIPTION"))
        pct = _channel_pct(ch, "credit", material_by_code=material_by_code)
        material_src = (material_by_code or {}).get(code) or {}

        sof = _ss(ch.get("sof")) or "Unverified"
        risk = _ss(ch.get("risk"))
        hits = _fmt_hits(ch.get("match_hits"))

        tail = []
        if risk:
            tail.append(f"Risk: {risk}")
        if hits:
            tail.append(f"Match: {hits}")
        tail_txt = (" | " + " | ".join(tail)) if tail else ""

        lines.append(f"- {code} - {desc}: {pct:.2f}% of total credits. SoF: {sof}{tail_txt}")
        lines.extend(_render_channel_suspicious_summaries(material_src, "credit"))
    return lines


def _render_debit_channels(
    profile_debit_channels: List[Dict[str, Any]],
    top_n: int = 8,
    material_by_code: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[str]:
    lines: List[str] = []
    ranked = sorted(
        profile_debit_channels or [],
        key=lambda ch: _channel_pct(ch, "debit", material_by_code=material_by_code),
        reverse=True,
    )[: max(0, int(top_n))]

    for ch in ranked:
        code = _ss(ch.get("TRANSCODE"))
        desc = _ss(ch.get("DESCRIPTION"))
        pct = _channel_pct(ch, "debit", material_by_code=material_by_code)
        material_src = (material_by_code or {}).get(code) or {}

        pof = _ss(ch.get("pof")) or "Unverified"
        risk = _ss(ch.get("risk"))
        hits = _fmt_hits(ch.get("match_hits"))

        tail = []
        if risk:
            tail.append(f"Risk: {risk}")
        if hits:
            tail.append(f"Match: {hits}")
        tail_txt = (" | " + " | ".join(tail)) if tail else ""

        lines.append(f"- {code} - {desc}: {pct:.2f}% of total debits. UoF/PoF: {pof}{tail_txt}")
        lines.extend(_render_channel_suspicious_summaries(material_src, "debit"))
    return lines


def _fallback_ranked_channels(
    material_channels: List[Dict[str, Any]],
    direction: str,
    top_n: int = 5,
    material_by_code: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    amt_key = "deposit" if direction == "credit" else "withdrawal"
    subset = [c for c in (material_channels or []) if _sf(c.get(amt_key)) > 0]
    return sorted(
        subset,
        key=lambda x: _channel_pct(x, direction, material_by_code=material_by_code),
        reverse=True,
    )[: max(0, int(top_n))]


def _summary_channels(
    channels: List[Dict[str, Any]],
    direction: str,
    material_by_code: Optional[Dict[str, Dict[str, Any]]] = None,
    top_n: int = 3,
) -> List[str]:
    out: List[str] = []
    for ch in (channels or [])[: max(0, int(top_n))]:
        code = _ss(ch.get("TRANSCODE"))
        desc = _ss(ch.get("DESCRIPTION")) or "Unknown channel"
        pct = _channel_pct(ch, direction, material_by_code=material_by_code)
        label = _ss(ch.get("sof" if direction == "credit" else "pof")) or "Unverified"
        out.append(f"{code} - {desc} ({pct:.2f}% | {label})")
    return out


def _recognized_sof_lines(
    channel_profile: Dict[str, Any],
    detectors: Dict[str, Any],
    material_by_code: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[str]:
    lines: List[str] = []
    for ch in (channel_profile.get("credit") or {}).get("channels") or []:
        if not bool(ch.get("declared_sof_match")):
            continue
        code = _ss(ch.get("TRANSCODE"))
        desc = _ss(ch.get("DESCRIPTION"))
        sof = _ss(ch.get("sof")) or "Recognized SoF"
        pct = _channel_pct(ch, "credit", material_by_code=material_by_code)
        lines.append(f"    ✓ {code} - {desc}: appears consistent with declared SoF ({sof}) at {pct:.2f}% of total credits.")

    salary = detectors.get("salary_pattern") or {}
    if salary.get("triggered") and not salary.get("salary_wash_flag"):
        cycle = _ss(salary.get("cycle")) or "irregular"
        sources = salary.get("salary_sources") or []
        src_txt = f" | Sources: {', '.join(map(str, sources[:3]))}" if sources else ""
        lines.append(f"    ✓ Salary-like inflow pattern recognised as legitimate SoF support ({cycle} cadence){src_txt}.")
    elif salary.get("salary_wash_flag"):
        lines.append("    ✓ Salary-like inflows were detected, but associated salary-wash behaviour was also observed and retained under suspicious indicators.")

    # de-duplicate while preserving order
    out: List[str] = []
    seen = set()
    for line in lines:
        if line not in seen:
            seen.add(line)
            out.append(line)
    return out


def _collect_summary_patterns(
    detectors: Dict[str, Any],
    material_channels: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    pattern_map: Dict[str, Dict[str, Any]] = {}

    for name, payload in (detectors or {}).items():
        if not isinstance(payload, dict):
            continue
        if not payload.get("triggered"):
            continue
        if name == "salary_pattern" and not payload.get("salary_wash_flag"):
            continue

        label = _risk_label_for_reason(str(name))
        entry = pattern_map.setdefault(label, {
            "label": label,
            "strength": 0.0,
            "count": 0,
            "credit_total": 0.0,
            "debit_total": 0.0,
            "channels": set(),
        })
        entry["strength"] = max(entry["strength"], _sf(payload.get("strength")))

    for ch in material_channels or []:
        code = _ss(ch.get("TRANSCODE"))
        desc = _ss(ch.get("DESCRIPTION"))
        channel_label = f"{code} - {desc}" if code or desc else "Unknown channel"
        by_reason = ch.get("detector_suspicious_by_reason") or {}
        if not isinstance(by_reason, dict):
            continue

        for reason, stats_block in by_reason.items():
            label = _risk_label_for_reason(str(reason))
            entry = pattern_map.setdefault(label, {
                "label": label,
                "strength": 0.0,
                "count": 0,
                "credit_total": 0.0,
                "debit_total": 0.0,
                "channels": set(),
            })

            if channel_label:
                entry["channels"].add(channel_label)

            if isinstance(stats_block, dict):
                credit = stats_block.get("credit") or {}
                debit = stats_block.get("debit") or {}
                entry["count"] += _si(credit.get("count")) + _si(debit.get("count"))
                entry["credit_total"] += _sf(credit.get("total"))
                entry["debit_total"] += _sf(debit.get("total"))

    ranked = sorted(
        pattern_map.values(),
        key=lambda x: (-x["strength"], -x["count"], -(x["credit_total"] + x["debit_total"]), x["label"]),
    )

    out: List[Dict[str, Any]] = []
    for item in ranked:
        out.append({
            "label": item["label"],
            "strength": round(float(item["strength"]), 3),
            "count": int(item["count"]),
            "credit_total": round(float(item["credit_total"]), 2),
            "debit_total": round(float(item["debit_total"]), 2),
            "channels": sorted(item["channels"])[:3],
        })
    return out


def _recurrence_hint(recurrence_clusters: Dict[str, Any], direction: str) -> str:
    if not isinstance(recurrence_clusters, dict):
        return ""

    items = []
    for key in ("identity_clusters", "same_day_identity_clusters", "narrative_clusters"):
        val = recurrence_clusters.get(key)
        if isinstance(val, list):
            items.extend([x for x in val if isinstance(x, dict) and _ss(x.get("direction")).lower() == direction])

    if not items:
        return ""

    ranked = sorted(
        items,
        key=lambda x: (-_si(x.get("count")), -_sf(x.get("total_amount"))),
    )
    top = ranked[0]
    label = _ss(top.get("label") or top.get("pattern") or top.get("TRANSCODE") or "Recurring pattern")
    count = _si(top.get("count"))
    total = _sf(top.get("total_amount"))
    return f"Most evident recurring {direction} pattern: {label} ({count}x | K{total:,.2f})."


def build_narrative_v1(context: Dict[str, Any]) -> str:
    risk = context.get("risk_metrics") or {}
    trigger = context.get("trigger") or {}
    client = context.get("client") or {}
    totals = context.get("totals") or {}

    total_credits = _sf(totals.get("credits"))
    total_debits = _sf(totals.get("debits"))

    channel_profile = context.get("channel_profile") or {}
    cp_credit = (channel_profile.get("credit") or {})
    cp_debit = (channel_profile.get("debit") or {})
    cp_credit_channels = cp_credit.get("channels") or []
    cp_debit_channels = cp_debit.get("channels") or []
    using_profile = bool(cp_credit_channels or cp_debit_channels)

    material_channels = context.get("material_channels") or []
    material_by_code: Dict[str, Dict[str, Any]] = {
        _ss(ch.get("TRANSCODE")): ch
        for ch in material_channels
        if _ss(ch.get("TRANSCODE"))
    }

    detectors = context.get("detectors") or {}
    recurrence_clusters = context.get("recurrence_clusters") or {}

    rating = risk.get("rating", "UNKNOWN")
    scores = risk.get("scores", {}) or {}
    overall = scores.get("overall", "N/A")
    ml = scores.get("ml", "N/A")
    tf = scores.get("tf", "N/A")
    confidence = scores.get("confidence", "N/A")

    try:
        suspicious_total_rows = int(context.get("suspicious_total_rows") or 0)
    except Exception:
        suspicious_total_rows = 0

    lines: List[str] = []

    # 1) Credit Rationale
    lines.append(REQUIRED_HEADINGS[0])
    lines.append(f"- Total credits observed in the review period: K{total_credits:,.2f}.")

    recognized_sof = _recognized_sof_lines(channel_profile, detectors, material_by_code=material_by_code)
    if recognized_sof:
        lines.append("  - Recognized Legitimate SoF Patterns")
        lines.extend(recognized_sof)

    if using_profile:
        lines.extend(_render_credit_channels(cp_credit_channels, top_n=10, material_by_code=material_by_code))
    else:
        credit_only = _fallback_ranked_channels(material_channels, "credit", top_n=8, material_by_code=material_by_code)
        for ch in credit_only:
            code = _ss(ch.get("TRANSCODE"))
            desc = _ss(ch.get("DESCRIPTION"))
            pct = _channel_pct(ch, "credit", material_by_code=material_by_code)
            lines.append(f"- {code} - {desc}: {pct:.2f}% of total credits. SoF: Unverified")
            lines.extend(_render_channel_suspicious_summaries(ch, "credit"))

    lines.append(f"- Risk engine outcome (if enabled): {rating} (ML={ml}, TF={tf}, Overall={overall}, Confidence={confidence}).")

    # 2) Debit Rationale
    lines.append(REQUIRED_HEADINGS[1])
    lines.append(f"- Total debits observed in the review period: K{total_debits:,.2f}.")

    if using_profile:
        lines.extend(_render_debit_channels(cp_debit_channels, top_n=10, material_by_code=material_by_code))
    else:
        debit_only = _fallback_ranked_channels(material_channels, "debit", top_n=8, material_by_code=material_by_code)
        for ch in debit_only:
            code = _ss(ch.get("TRANSCODE"))
            desc = _ss(ch.get("DESCRIPTION"))
            pct = _channel_pct(ch, "debit", material_by_code=material_by_code)
            lines.append(f"- {code} - {desc}: {pct:.2f}% of total debits. UoF/PoF: Unverified")
            lines.extend(_render_channel_suspicious_summaries(ch, "debit"))

    # 3) Summary of Both Rationales
    lines.append(REQUIRED_HEADINGS[2])
    lines.append("- Summary derived deterministically from analyzed statement behaviour.")

    credit_summary_channels = cp_credit_channels if using_profile else _fallback_ranked_channels(
        material_channels,
        "credit",
        top_n=5,
        material_by_code=material_by_code,
    )
    debit_summary_channels = cp_debit_channels if using_profile else _fallback_ranked_channels(
        material_channels,
        "debit",
        top_n=5,
        material_by_code=material_by_code,
    )

    lines.append("  - Source of Funds (SoF)")
    top_credit_lines = _summary_channels(credit_summary_channels, "credit", material_by_code=material_by_code, top_n=3)
    if top_credit_lines:
        for item in top_credit_lines:
            lines.append(f"    ✓ {item}")
    else:
        lines.append("    ✓ No dominant inbound channel was identified from the analyzed statement.")

    credit_recur = _recurrence_hint(recurrence_clusters, "credit")
    if credit_recur:
        lines.append(f"    ✓ {credit_recur}")
    if suspicious_total_rows > 0:
        lines.append("    ✓ Inbound risk indicators, where present, are reflected under the relevant credit channels above.")

    lines.append("  - Use of Funds (UoF)")
    top_debit_lines = _summary_channels(debit_summary_channels, "debit", material_by_code=material_by_code, top_n=3)
    if top_debit_lines:
        for item in top_debit_lines:
            lines.append(f"    ✓ {item}")
    else:
        lines.append("    ✓ No dominant outbound channel was identified from the analyzed statement.")

    debit_recur = _recurrence_hint(recurrence_clusters, "debit")
    if debit_recur:
        lines.append(f"    ✓ {debit_recur}")
    if suspicious_total_rows > 0:
        lines.append("    ✓ Outbound risk indicators, where present, are reflected under the relevant debit channels above.")

    lines.append("  - Detected Patterns")
    patterns = _collect_summary_patterns(detectors, material_channels)
    if patterns:
        for item in patterns[:5]:
            channels_txt = f" | Channels: {', '.join(item['channels'])}" if item.get("channels") else ""
            totals_txt = []
            if _sf(item.get("credit_total")) > 0:
                totals_txt.append(f"Credit K{_sf(item.get('credit_total')):,.2f}")
            if _sf(item.get("debit_total")) > 0:
                totals_txt.append(f"Debit K{_sf(item.get('debit_total')):,.2f}")
            totals_tail = f" | {' | '.join(totals_txt)}" if totals_txt else ""
            count_tail = f" | Rows/instances: {_si(item.get('count'))}" if _si(item.get("count")) > 0 else ""
            lines.append(
                f"    ✓ {item['label']} | Strength: {float(item['strength']):.2f}{count_tail}{totals_tail}{channels_txt}"
            )
    else:
        lines.append("    ✓ No triggered detector pattern was identified from the analyzed statement.")

    # 4) Overview and Background of Review
    lines.append(REQUIRED_HEADINGS[3])
    lines.append(f"- Client type: {client.get('client_type', 'UNKNOWN')}.")
    declared_sof = client.get("source_of_funds") or client.get("sourceOfFunds") or client.get("declared_source_of_funds")
    if declared_sof:
        lines.append(f"- Declared source of funds: {declared_sof}.")
    if client.get("individualProfile"):
        lines.append(f"- Individual profile: {client.get('individualProfile')}.")
    elif client.get("profile"):
        lines.append(f"- Client profile: {client.get('profile')}.")
    lines.append(f"- Trigger type: {trigger.get('type', 'UNKNOWN')}.")
    lines.append(f"- Trigger source: {trigger.get('source', 'UNKNOWN')}.")
    if trigger.get("description"):
        lines.append(f"- Trigger background: {trigger.get('description')}")
    if suspicious_total_rows > 0:
        lines.append(f"- Detector-flagged suspicious transaction rows identified across analyzed material channels after contextual suppression: {suspicious_total_rows}.")

    return "\n".join(lines)


build_narrative_v0 = build_narrative_v1