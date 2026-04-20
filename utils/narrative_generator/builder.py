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
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", t)
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
        return "Round Figure Transactions"
    if r == "salary_pattern":
        return "Salary-like Pattern"
    if r == "cash_intensive":
        return "Cash-intensive Activity"
    if r == "third_party":
        return "Third-party Activity"
    if r == "recurrence":
        return "Recurrence Parties"
    return reason


def _risk_sort_key(label: str) -> int:
    order = {
        "Recurrence Parties": 0,
        "Round Figure Transactions": 1,
        "Structuring": 2,
        "Layering": 3,
        "Pass-through": 4,
        "Third-party Activity": 5,
        "Cash-intensive Activity": 6,
        "Salary-like Pattern": 7,
    }
    return order.get(label, 99)


def _extract_identifier_groups(stats: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(stats, dict):
        return []

    candidates = (
        stats.get("identifiers"),
        stats.get("top_identifiers"),
        stats.get("parties"),
        stats.get("top_parties_detailed"),
    )

    out: List[Dict[str, Any]] = []
    for cand in candidates:
        if not isinstance(cand, list):
            continue
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
            out.append(
                {
                    "identifier": label,
                    "count": _si(item.get("count")),
                    "total": _sf(item.get("total") or item.get("total_amount")),
                    "date_min": item.get("date_min"),
                    "date_max": item.get("date_max"),
                }
            )
        if out:
            break

    out = [
        x for x in out
        if x.get("identifier") and (_si(x.get("count")) > 0 or _sf(x.get("total")) > 0)
    ]
    return sorted(out, key=lambda x: (-_si(x.get("count")), -_sf(x.get("total")), _ss(x.get("identifier"))))


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


def _summary_channel_lines(
    channels: List[Dict[str, Any]],
    direction: str,
    material_by_code: Optional[Dict[str, Dict[str, Any]]] = None,
    top_n: int = 3,
) -> List[str]:
    lines: List[str] = []
    for ch in (channels or [])[: max(0, int(top_n))]:
        code = _ss(ch.get("TRANSCODE"))
        desc = _ss(ch.get("DESCRIPTION")) or "Unknown channel"
        pct = _channel_pct(ch, direction, material_by_code=material_by_code)
        label = _ss(ch.get("sof" if direction == "credit" else "pof")) or "Unverified"
        lines.append(f"    ✓ {code} - {desc} ({pct:.2f}% | {label})")
    return lines


def _build_reason_direction_groups(material_channels: List[Dict[str, Any]]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    out: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for ch in material_channels or []:
        code = _ss(ch.get("TRANSCODE"))
        desc = _ss(ch.get("DESCRIPTION"))
        by_reason = ch.get("detector_suspicious_by_reason") or {}
        if not isinstance(by_reason, dict):
            continue

        for reason, payload in by_reason.items():
            if not isinstance(payload, dict):
                continue
            risk = _risk_label_for_reason(str(reason))
            for direction in ("credit", "debit"):
                stats = payload.get(direction) or {}
                if not isinstance(stats, dict):
                    continue
                count = _si(stats.get("count"))
                total = _sf(stats.get("total"))
                if count <= 0 and total <= 0:
                    continue
                out.setdefault((risk, direction), []).append(
                    {
                        "channel_code": code,
                        "channel_desc": desc,
                        "stats": stats,
                    }
                )
    for key, items in list(out.items()):
        out[key] = sorted(
            items,
            key=lambda x: (-_si((x.get("stats") or {}).get("count")), -_sf((x.get("stats") or {}).get("total")), _ss(x.get("channel_code"))),
        )
    return out


def _render_recurrence_block(groups: Dict[Tuple[str, str], List[Dict[str, Any]]]) -> List[str]:
    lines: List[str] = []
    for direction, title in (("credit", "Recurrence Parties – Credits"), ("debit", "Recurrence Parties – Debits")):
        entries = groups.get(("Recurrence Parties", direction)) or []
        if not entries:
            continue
        lines.append(f"    ✓ {title}")
        for entry in entries:
            code = _ss(entry.get("channel_code"))
            desc = _ss(entry.get("channel_desc"))
            stats = entry.get("stats") or {}
            lines.append(f"      {code} - {desc}")
            ident_groups = _extract_identifier_groups(stats)
            if ident_groups:
                for item in ident_groups[:10]:
                    lines.append(
                        f"      • {item['identifier']}: {_si(item['count'])}x | K{_sf(item['total']):,.2f} | {_fmt_range(item.get('date_min'), item.get('date_max'))}"
                    )
            else:
                top_parties = [_ss(x) for x in (stats.get("top_parties") or []) if _ss(x)]
                summary = f"{_si(stats.get('count'))}x | K{_sf(stats.get('total')):,.2f} | {_fmt_range(stats.get('date_min'), stats.get('date_max'))}"
                if top_parties:
                    summary += f" | {', '.join(top_parties[:5])}"
                lines.append(f"      • {summary}")
    return lines


def _render_round_figure_block(groups: Dict[Tuple[str, str], List[Dict[str, Any]]]) -> List[str]:
    lines: List[str] = []
    credit_entries = groups.get(("Round Figure Transactions", "credit")) or []
    debit_entries = groups.get(("Round Figure Transactions", "debit")) or []
    if not credit_entries and not debit_entries:
        return lines

    lines.append("    ✓ Round Figure Transactions")
    for direction, title, entries in (("credit", "Credits", credit_entries), ("debit", "Debits", debit_entries)):
        if not entries:
            continue
        total_count = sum(_si((x.get("stats") or {}).get("count")) for x in entries)
        lines.append(f"      {title}")
        for entry in entries[:5]:
            stats = entry.get("stats") or {}
            count = _si(stats.get("count"))
            share = (count / total_count * 100.0) if total_count > 0 else 0.0
            code = _ss(entry.get("channel_code"))
            desc = _ss(entry.get("channel_desc"))
            lines.append(f"      • {code} - {desc}: {share:.0f}% of flows")
    return lines


def _render_structuring_block(groups: Dict[Tuple[str, str], List[Dict[str, Any]]]) -> List[str]:
    lines: List[str] = []
    credit_entries = groups.get(("Structuring", "credit")) or []
    debit_entries = groups.get(("Structuring", "debit")) or []
    if not credit_entries and not debit_entries:
        return lines

    lines.append("    ✓ Structuring")
    for title, entries in (("Credits", credit_entries), ("Debits", debit_entries)):
        if not entries:
            continue
        lines.append(f"      {title}")
        for entry in entries:
            code = _ss(entry.get("channel_code"))
            desc = _ss(entry.get("channel_desc"))
            stats = entry.get("stats") or {}
            ident_groups = _extract_identifier_groups(stats)
            if ident_groups:
                for item in ident_groups[:10]:
                    lines.append(
                        f"      • {code} - {desc} [{item['identifier']}]: {_si(item['count'])}x | K{_sf(item['total']):,.2f} | {_fmt_range(item.get('date_min'), item.get('date_max'))}"
                    )
            else:
                lines.append(
                    f"      • {code} - {desc}: {_si(stats.get('count'))}x | K{_sf(stats.get('total')):,.2f} | {_fmt_range(stats.get('date_min'), stats.get('date_max'))}"
                )
    return lines


def _render_layering_block(groups: Dict[Tuple[str, str], List[Dict[str, Any]]]) -> List[str]:
    lines: List[str] = []
    entries = (groups.get(("Layering", "credit")) or []) + (groups.get(("Layering", "debit")) or [])
    if not entries:
        return lines

    lines.append("    ✓ Layering")
    for entry in entries[:8]:
        code = _ss(entry.get("channel_code"))
        desc = _ss(entry.get("channel_desc"))
        stats = entry.get("stats") or {}
        lines.append(
            f"      • {code} - {desc}: {_si(stats.get('count'))}x | K{_sf(stats.get('total')):,.2f} | {_fmt_range(stats.get('date_min'), stats.get('date_max'))}"
        )
    return lines


def _render_other_risk_blocks(groups: Dict[Tuple[str, str], List[Dict[str, Any]]]) -> List[str]:
    lines: List[str] = []
    handled = {"Recurrence Parties", "Round Figure Transactions", "Structuring", "Layering"}
    labels = sorted({k[0] for k in groups.keys() if k[0] not in handled}, key=_risk_sort_key)
    for label in labels:
        lines.append(f"    ✓ {label}")
        for direction in ("credit", "debit"):
            entries = groups.get((label, direction)) or []
            if not entries:
                continue
            lines.append(f"      {direction.title()}s")
            for entry in entries[:5]:
                code = _ss(entry.get("channel_code"))
                desc = _ss(entry.get("channel_desc"))
                stats = entry.get("stats") or {}
                top_parties = [_ss(x) for x in (stats.get("top_parties") or []) if _ss(x)]
                tail = f" | {', '.join(top_parties[:3])}" if top_parties else ""
                lines.append(
                    f"      • {code} - {desc}: {_si(stats.get('count'))}x | K{_sf(stats.get('total')):,.2f} | {_fmt_range(stats.get('date_min'), stats.get('date_max'))}{tail}"
                )
    return lines


def _collect_summary_patterns(detectors: Dict[str, Any], material_channels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pattern_map: Dict[str, Dict[str, Any]] = {}

    for name, payload in (detectors or {}).items():
        if not isinstance(payload, dict):
            continue
        if not payload.get("triggered"):
            continue
        label = _risk_label_for_reason(str(name))
        entry = pattern_map.setdefault(label, {
            "label": label,
            "strength": 0.0,
            "count": 0,
            "credit_total": 0.0,
            "debit_total": 0.0,
        })
        entry["strength"] = max(entry["strength"], _sf(payload.get("strength")))

    for ch in material_channels or []:
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
            })
            if isinstance(stats_block, dict):
                credit = stats_block.get("credit") or {}
                debit = stats_block.get("debit") or {}
                entry["count"] += _si(credit.get("count")) + _si(debit.get("count"))
                entry["credit_total"] += _sf(credit.get("total"))
                entry["debit_total"] += _sf(debit.get("total"))

    return sorted(
        [
            {
                "label": v["label"],
                "strength": round(float(v["strength"]), 3),
                "count": int(v["count"]),
                "credit_total": round(float(v["credit_total"]), 2),
                "debit_total": round(float(v["debit_total"]), 2),
            }
            for v in pattern_map.values()
        ],
        key=lambda x: (_risk_sort_key(_ss(x.get("label"))), -_si(x.get("count")), -(_sf(x.get("credit_total")) + _sf(x.get("debit_total")))),
    )


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
    reason_groups = _build_reason_direction_groups(material_channels)
    summary_patterns = context.get("summary_patterns") or _collect_summary_patterns(detectors, material_channels)

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

    credit_channels = cp_credit_channels if using_profile else _fallback_ranked_channels(
        material_channels,
        "credit",
        top_n=5,
        material_by_code=material_by_code,
    )
    debit_channels = cp_debit_channels if using_profile else _fallback_ranked_channels(
        material_channels,
        "debit",
        top_n=5,
        material_by_code=material_by_code,
    )

    lines: List[str] = []

    # Credit Rationale
    lines.append(REQUIRED_HEADINGS[0])
    lines.append(f"- Total credits observed in the review period: K{total_credits:,.2f}.")
    if credit_channels:
        for ch in credit_channels[:3]:
            code = _ss(ch.get("TRANSCODE"))
            desc = _ss(ch.get("DESCRIPTION")) or "Unknown channel"
            pct = _channel_pct(ch, "credit", material_by_code=material_by_code)
            lines.append(f"- {code} - {desc}: {pct:.2f}%")
    else:
        lines.append("- No dominant inbound channels were identified.")

    # Debit Rationale
    lines.append(REQUIRED_HEADINGS[1])
    lines.append(f"- Total debits observed in the review period: K{total_debits:,.2f}.")
    if debit_channels:
        for ch in debit_channels[:3]:
            code = _ss(ch.get("TRANSCODE"))
            desc = _ss(ch.get("DESCRIPTION")) or "Unknown channel"
            pct = _channel_pct(ch, "debit", material_by_code=material_by_code)
            lines.append(f"- {code} - {desc}: {pct:.2f}%")
    else:
        lines.append("- No dominant outbound channels were identified.")

    # Summary of Both Rationales
    lines.append(REQUIRED_HEADINGS[2])
    lines.append("  - Source of Funds (SoF)")
    sof_lines = _summary_channel_lines(credit_channels, "credit", material_by_code=material_by_code, top_n=3)
    lines.extend(sof_lines or ["    ✓ No dominant source of funds was identified."])

    lines.append("  - Use of Funds (UoF)")
    uof_lines = _summary_channel_lines(debit_channels, "debit", material_by_code=material_by_code, top_n=3)
    lines.extend(uof_lines or ["    ✓ No dominant use of funds was identified."])

    lines.append("  - Suspicious Transactions")
    suspicious_lines: List[str] = []
    suspicious_lines.extend(_render_recurrence_block(reason_groups))
    suspicious_lines.extend(_render_round_figure_block(reason_groups))
    suspicious_lines.extend(_render_structuring_block(reason_groups))
    suspicious_lines.extend(_render_layering_block(reason_groups))
    suspicious_lines.extend(_render_other_risk_blocks(reason_groups))
    if suspicious_lines:
        lines.extend(suspicious_lines)
    else:
        lines.append("    ✓ No suspicious transaction pattern was identified after contextual suppression.")

    # Overview and Background of Review
    lines.append(REQUIRED_HEADINGS[3])
    lines.append(f"- Client type: {client.get('client_type', 'UNKNOWN')}.")
    declared_sof = client.get("declared_source_of_funds") or client.get("source_of_funds") or client.get("sourceOfFunds")
    if declared_sof:
        lines.append(f"- Declared source of funds: {declared_sof}.")
    profile = client.get("individualProfile") or client.get("profile") or client.get("individual_profile_type")
    if profile:
        lines.append(f"- Individual profile: {profile}.")
    lines.append(f"- Trigger type: {trigger.get('type', 'UNKNOWN')}.")
    lines.append(f"- Trigger source: {trigger.get('source', 'UNKNOWN')}.")
    if trigger.get("description"):
        lines.append(f"- Trigger background: {trigger.get('description')}")
    if suspicious_total_rows > 0:
        lines.append(
            f"- Detector-flagged suspicious transaction rows identified across analyzed material channels after contextual suppression: {suspicious_total_rows}."
        )
    if summary_patterns:
        top_labels = ", ".join([_ss(x.get("label")) for x in summary_patterns[:5] if _ss(x.get("label"))])
        if top_labels:
            lines.append(f"- Key detected patterns: {top_labels}.")
    lines.append(f"- Risk engine outcome (if enabled): {rating} (ML={ml}, TF={tf}, Overall={overall}, Confidence={confidence}).")

    return "\n".join(lines)


build_narrative_v0 = build_narrative_v1
