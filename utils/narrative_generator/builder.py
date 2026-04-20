from __future__ import annotations

from typing import Any, Dict, List, Optional
import re

from utils.kyc_rules import REQUIRED_HEADINGS


# ------------------------------------------------------------
# basic helpers
# ------------------------------------------------------------
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
    return reason.replace("_", " ").title()


# ------------------------------------------------------------
# fallbacks for older payloads
# ------------------------------------------------------------
def _channel_pct(
    ch: Dict[str, Any],
    direction: str,
    material_by_code: Optional[Dict[str, Dict[str, Any]]] = None,
) -> float:
    if direction == "credit":
        for k in ("CR%", "credit_pct", "cr_pct", "cr", "pct"):
            if k in ch:
                return _sf(ch.get(k))
        code = _ss(ch.get("TRANSCODE"))
        src = (material_by_code or {}).get(code) or {}
        for k in ("CR%", "credit_pct", "cr_pct", "cr", "pct"):
            if k in src:
                return _sf(src.get(k))
        return 0.0

    for k in ("DR%", "debit_pct", "dr_pct", "dr", "pct"):
        if k in ch:
            return _sf(ch.get(k))
    code = _ss(ch.get("TRANSCODE"))
    src = (material_by_code or {}).get(code) or {}
    for k in ("DR%", "debit_pct", "dr_pct", "dr", "pct"):
        if k in src:
            return _sf(src.get(k))
    return 0.0


def _fallback_top_channels(
    material_channels: List[Dict[str, Any]],
    direction: str,
    material_by_code: Optional[Dict[str, Dict[str, Any]]] = None,
    top_n: int = 5,
) -> List[Dict[str, Any]]:
    amt_key = "deposit" if direction == "credit" else "withdrawal"
    subset = [c for c in (material_channels or []) if _sf(c.get(amt_key)) > 0]
    subset = sorted(
        subset,
        key=lambda x: (_sf(x.get(amt_key)), _channel_pct(x, direction, material_by_code=material_by_code)),
        reverse=True,
    )
    rows: List[Dict[str, Any]] = []
    for row in subset[: max(0, int(top_n))]:
        rows.append({
            "TRANSCODE": _ss(row.get("TRANSCODE")),
            "DESCRIPTION": _ss(row.get("DESCRIPTION")),
            "amount": round(_sf(row.get(amt_key)), 2),
            "pct": round(_channel_pct(row, direction, material_by_code=material_by_code), 2),
        })
    return rows


def _fallback_summary_patterns(material_channels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for ch in material_channels or []:
        channel_desc = _ss(ch.get("DESCRIPTION")) or _ss(ch.get("TRANSCODE")) or "UNKNOWN"
        by_reason = ch.get("detector_suspicious_by_reason") or {}
        if not isinstance(by_reason, dict):
            continue
        for reason, stats_block in by_reason.items():
            if not isinstance(stats_block, dict):
                continue
            risk_label = _risk_label_for_reason(str(reason))
            for direction in ("credit", "debit"):
                stats = stats_block.get(direction) or {}
                if not isinstance(stats, dict):
                    continue
                count = _si(stats.get("count"))
                total = _sf(stats.get("total"))
                if count <= 0 and total <= 0:
                    continue
                bucket_key = f"{risk_label}|{direction}"
                bucket = buckets.setdefault(bucket_key, {
                    "type": risk_label,
                    "direction": direction,
                    "count": 0,
                    "total": 0.0,
                    "date_min": None,
                    "date_max": None,
                    "channels": [],
                    "parties": [],
                })
                bucket["count"] += count
                bucket["total"] += total
                dmin = _ss(stats.get("date_min")) or None
                dmax = _ss(stats.get("date_max")) or None
                if dmin and (bucket["date_min"] is None or dmin < bucket["date_min"]):
                    bucket["date_min"] = dmin
                if dmax and (bucket["date_max"] is None or dmax > bucket["date_max"]):
                    bucket["date_max"] = dmax
                if channel_desc and channel_desc not in bucket["channels"]:
                    bucket["channels"].append(channel_desc)
                for p in (stats.get("top_parties") or []):
                    s = _ss(p)
                    if s and s not in bucket["parties"]:
                        bucket["parties"].append(s)
    out = []
    for item in sorted(buckets.values(), key=lambda x: (-_sf(x.get("total")), -_si(x.get("count")), _ss(x.get("type")))):
        out.append({
            "type": item["type"],
            "direction": item["direction"],
            "count": _si(item["count"]),
            "total": round(_sf(item["total"]), 2),
            "date_min": item["date_min"],
            "date_max": item["date_max"],
            "channels": item["channels"][:5],
            "channel": ", ".join(item["channels"][:3]),
            "parties": item["parties"][:5],
        })
    return out


# ------------------------------------------------------------
# render helpers for new report summary shape
# ------------------------------------------------------------
def _render_top_channel_lines(rows: List[Dict[str, Any]], label_key: str) -> List[str]:
    out: List[str] = []
    for row in rows or []:
        code = _ss(row.get("TRANSCODE"))
        desc = _ss(row.get("DESCRIPTION")) or "Unknown channel"
        pct = _sf(row.get("pct"))
        label = _ss(row.get(label_key))
        amount = _sf(row.get("amount"))
        tail = f" | K{amount:,.2f}" if amount > 0 else ""
        if label:
            out.append(f"    ✓ {code} - {desc} ({pct:.2f}% | {label}){tail}")
        else:
            out.append(f"    ✓ {code} - {desc} ({pct:.2f}%){tail}")
    return out


def _render_behavior_observations(detectors: Dict[str, Any], patterns: List[Dict[str, Any]]) -> List[str]:
    lines: List[str] = []
    salary = detectors.get("salary_pattern") or {}
    if salary.get("triggered") and not salary.get("salary_wash_flag"):
        cycle = _ss(salary.get("cycle")) or "recurring"
        lines.append(f"    ✓ Salary-like inflow behaviour was detected with {cycle} cadence.")
    elif salary.get("salary_wash_flag"):
        lines.append("    ✓ Salary-like inflows were detected, however onward movement indicators were also observed.")

    if (detectors.get("cash_intensive") or {}).get("triggered"):
        lines.append("    ✓ Elevated cash-based activity was observed during the review period.")
    if (detectors.get("third_party") or {}).get("triggered"):
        lines.append("    ✓ Third-party transaction activity was identified in the account behaviour.")
    if (detectors.get("pass_through") or {}).get("triggered"):
        lines.append("    ✓ Rapid in-and-out movement of funds was identified in portions of the account activity.")
    if (detectors.get("layering") or {}).get("triggered"):
        lines.append("    ✓ Layering-style movement patterns were identified across flagged transactions.")
    if not lines and not patterns:
        lines.append("    ✓ Transaction activity appeared broadly consistent with expected account behaviour based on the analyzed statement.")
    return lines


def _render_suspicious_activity(patterns: List[Dict[str, Any]], top_n: int = 10) -> List[str]:
    lines: List[str] = []
    if not patterns:
        lines.append("    ✓ No suspicious transaction patterns were identified from the analyzed material channels.")
        return lines

    for item in patterns[: max(0, int(top_n))]:
        ptype = _ss(item.get("type")) or "Suspicious activity"
        direction = _ss(item.get("direction")).lower()
        direction_txt = f" ({direction})" if direction else ""
        rng = _fmt_range(item.get("date_min"), item.get("date_max"))
        count = _si(item.get("count"))
        total = _sf(item.get("total"))
        channel = _ss(item.get("channel"))
        if not channel:
            chans = item.get("channels") or []
            if isinstance(chans, list) and chans:
                channel = ", ".join([_ss(x) for x in chans if _ss(x)][:3])
        parties = item.get("parties") or []
        parties_txt = ", ".join([_ss(x) for x in parties if _ss(x)][:3])

        sentence = (
            f"    ✓ {ptype}{direction_txt} observed between {rng}: "
            f"{count} transaction(s) totalling K{total:,.2f}"
        )
        if channel:
            sentence += f" through {channel}"
        sentence += "."
        lines.append(sentence)
        if parties_txt:
            lines.append(f"      • Main parties involved: {parties_txt}.")
    return lines


# ------------------------------------------------------------
# main builder
# ------------------------------------------------------------
def build_narrative_v1(context: Dict[str, Any]) -> str:
    summary = context.get("summary") or {}
    totals = context.get("totals") or {}
    client = context.get("client") or {}
    trigger = context.get("trigger") or {}
    detectors = context.get("detectors") or {}
    material_channels = context.get("material_channels") or []
    channel_profile = context.get("channel_profile") or {}
    risk = context.get("risk_metrics") or {}

    total_credits = _sf((summary.get("account_overview") or {}).get("total_credits") or totals.get("credits"))
    total_debits = _sf((summary.get("account_overview") or {}).get("total_debits") or totals.get("debits"))

    material_by_code: Dict[str, Dict[str, Any]] = {
        _ss(ch.get("TRANSCODE")): ch
        for ch in material_channels
        if _ss(ch.get("TRANSCODE"))
    }

    top_credit_channels = (summary.get("account_overview") or {}).get("top_credit_channels") or []
    top_debit_channels = (summary.get("account_overview") or {}).get("top_debit_channels") or []
    if not top_credit_channels:
        top_credit_channels = _fallback_top_channels(material_channels, "credit", material_by_code=material_by_code, top_n=5)
    if not top_debit_channels:
        top_debit_channels = _fallback_top_channels(material_channels, "debit", material_by_code=material_by_code, top_n=5)

    recognized_sof = (summary.get("source_of_funds") or {}).get("recognized") or []
    recognized_uof = (summary.get("use_of_funds") or {}).get("recognized") or []

    summary_patterns = context.get("summary_patterns") or (summary.get("suspicious_activity") or [])
    if not summary_patterns:
        summary_patterns = _fallback_summary_patterns(material_channels)

    suspicious_total_rows = _si(context.get("suspicious_total_rows"))

    rating = _ss(risk.get("rating") or "")
    scores = risk.get("scores") or {}
    overall = risk.get("overall") if "overall" in risk else scores.get("overall")
    ml = scores.get("ml")
    tf = scores.get("tf")
    confidence = scores.get("confidence")

    lines: List[str] = []

    # --------------------------------------------------------
    # 1) Credit Rationale
    # --------------------------------------------------------
    lines.append(REQUIRED_HEADINGS[0])
    lines.append(f"- Total credits observed in the review period: K{total_credits:,.2f}.")

    lines.append("  - Account Overview")
    if top_credit_channels:
        lines.append("    ✓ The main inbound channels observed during the review period were:")
        lines.extend(_render_top_channel_lines(top_credit_channels[:5], label_key="sof"))
    else:
        lines.append("    ✓ No dominant inbound channel was identified from the analyzed statement.")

    lines.append("  - Source of Funds")
    if recognized_sof:
        lines.append("    ✓ Recognized legitimate source of funds indicators were identified as follows:")
        lines.extend(_render_top_channel_lines(recognized_sof[:5], label_key="sof"))
    elif top_credit_channels:
        lines.append("    ✓ The source of funds presentation is based on the top inbound channels observed in the statement:")
        for row in top_credit_channels[:5]:
            code = _ss(row.get("TRANSCODE"))
            desc = _ss(row.get("DESCRIPTION")) or "Unknown channel"
            pct = _sf(row.get("pct"))
            amount = _sf(row.get("amount"))
            lines.append(f"    ✓ {code} - {desc}: {pct:.2f}% of total credits | K{amount:,.2f}.")
    else:
        lines.append("    ✓ No clear dominant source of funds was identified from the analyzed material channels.")

    # --------------------------------------------------------
    # 2) Debit Rationale
    # --------------------------------------------------------
    lines.append(REQUIRED_HEADINGS[1])
    lines.append(f"- Total debits observed in the review period: K{total_debits:,.2f}.")

    lines.append("  - Use of Funds")
    if recognized_uof:
        lines.append("    ✓ The primary uses of funds identified from channel classification were:")
        lines.extend(_render_top_channel_lines(recognized_uof[:5], label_key="pof"))
    elif top_debit_channels:
        lines.append("    ✓ The use of funds presentation is based on the top outbound channels observed in the statement:")
        for row in top_debit_channels[:5]:
            code = _ss(row.get("TRANSCODE"))
            desc = _ss(row.get("DESCRIPTION")) or "Unknown channel"
            pct = _sf(row.get("pct"))
            amount = _sf(row.get("amount"))
            lines.append(f"    ✓ {code} - {desc}: {pct:.2f}% of total debits | K{amount:,.2f}.")
    else:
        lines.append("    ✓ No clear dominant use of funds was identified from the analyzed material channels.")

    lines.append("  - Transaction Behaviour and Observations")
    lines.extend(_render_behavior_observations(detectors, summary_patterns))

    # --------------------------------------------------------
    # 3) Summary of Both Rationales
    # --------------------------------------------------------
    lines.append(REQUIRED_HEADINGS[2])
    lines.append("- Summary derived deterministically from analyzed statement behaviour and detector-supported transaction patterns.")

    lines.append("  - Source of Funds (SoF)")
    if recognized_sof:
        for line in _render_top_channel_lines(recognized_sof[:3], label_key="sof"):
            lines.append(line)
    elif top_credit_channels:
        for row in top_credit_channels[:3]:
            lines.append(
                f"    ✓ {_ss(row.get('TRANSCODE'))} - {_ss(row.get('DESCRIPTION')) or 'Unknown channel'} "
                f"({_sf(row.get('pct')):.2f}% | K{_sf(row.get('amount')):,.2f})"
            )
    else:
        lines.append("    ✓ No dominant inbound channel was identified.")

    lines.append("  - Use of Funds (UoF)")
    if recognized_uof:
        for line in _render_top_channel_lines(recognized_uof[:3], label_key="pof"):
            lines.append(line)
    elif top_debit_channels:
        for row in top_debit_channels[:3]:
            lines.append(
                f"    ✓ {_ss(row.get('TRANSCODE'))} - {_ss(row.get('DESCRIPTION')) or 'Unknown channel'} "
                f"({_sf(row.get('pct')):.2f}% | K{_sf(row.get('amount')):,.2f})"
            )
    else:
        lines.append("    ✓ No dominant outbound channel was identified.")

    lines.append("  - Suspicious Activity")
    lines.extend(_render_suspicious_activity(summary_patterns, top_n=10))

    # --------------------------------------------------------
    # 4) Overview and Background of Review
    # --------------------------------------------------------
    lines.append(REQUIRED_HEADINGS[3])
    client_type = _ss(client.get("client_type") or client.get("type"))
    if client_type:
        lines.append(f"- Client type: {client_type}.")

    declared_sof = _ss(
        client.get("source_of_funds")
        or client.get("sourceOfFunds")
        or client.get("declared_source_of_funds")
    )
    if declared_sof:
        lines.append(f"- Declared source of funds: {declared_sof}.")

    client_profile = _ss(client.get("individualProfile") or client.get("profile") or client.get("individual_profile_type"))
    if client_profile:
        lines.append(f"- Client profile: {client_profile}.")

    trig_type = _ss(trigger.get("type"))
    trig_source = _ss(trigger.get("source"))
    trig_desc = _ss(trigger.get("description"))
    if trig_type:
        lines.append(f"- Trigger type: {trig_type}.")
    if trig_source:
        lines.append(f"- Trigger source: {trig_source}.")
    if trig_desc:
        lines.append(f"- Trigger background: {trig_desc}.")

    if suspicious_total_rows > 0:
        lines.append(
            f"- Detector-flagged suspicious transaction rows identified across analyzed material channels after contextual suppression: {suspicious_total_rows}."
        )

    if rating or overall or ml is not None or tf is not None or confidence is not None:
        tail = []
        if ml is not None:
            tail.append(f"ML={ml}")
        if tf is not None:
            tail.append(f"TF={tf}")
        if overall is not None:
            tail.append(f"Overall={overall}")
        if confidence is not None:
            tail.append(f"Confidence={confidence}")
        joined = ", ".join(tail)
        if rating and joined:
            lines.append(f"- Risk engine outcome (if enabled): {rating} ({joined}).")
        elif rating:
            lines.append(f"- Risk engine outcome (if enabled): {rating}.")
        elif joined:
            lines.append(f"- Risk engine metrics (if enabled): {joined}.")

    return "\n".join(lines)


build_narrative_v0 = build_narrative_v1
