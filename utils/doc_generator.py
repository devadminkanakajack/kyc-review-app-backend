from typing import Dict, Any, List, Optional

from docx import Document
from docx.shared import Inches, Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_CELL_VERTICAL_ALIGNMENT

from utils.kyc_rules import REQUIRED_HEADINGS


# ------------------------------------------------------------
# Basic helpers
# ------------------------------------------------------------
def set_paragraph_style(p, size: int = 11, after: int = 3, line_spacing: float = 1.15):
    for run in p.runs:
        run.font.size = Pt(size)
    p.paragraph_format.space_after = Pt(after)
    p.paragraph_format.line_spacing = line_spacing


def _safe_float(v: Any) -> float:
    try:
        return float(v or 0.0)
    except Exception:
        return 0.0


def _safe_int(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _safe_str(v: Any) -> str:
    return str(v or "").strip()


def validate_aml_narrative(aml_narrative: str) -> None:
    raw_lines = aml_narrative.split("\n")
    lines = [ln.rstrip() for ln in raw_lines if ln.strip()]
    if not lines:
        raise ValueError("Narrative is empty.")

    first = lines[0].strip()
    if first != REQUIRED_HEADINGS[0]:
        raise ValueError(
            f"Narrative must start with '{REQUIRED_HEADINGS[0]}', but got '{first}'."
        )

    heading_positions = {}
    for idx, line in enumerate(lines):
        s = line.strip()
        if s in REQUIRED_HEADINGS:
            if s in heading_positions:
                raise ValueError(f"Duplicate section heading detected: {s}")
            heading_positions[s] = idx

    missing = [h for h in REQUIRED_HEADINGS if h not in heading_positions]
    if missing:
        raise ValueError(f"Missing required section(s): {', '.join(missing)}")

    positions = [heading_positions[h] for h in REQUIRED_HEADINGS]
    if positions != sorted(positions):
        raise ValueError("Section headings are not in the required order.")


# ------------------------------------------------------------
# Formatting helpers
# ------------------------------------------------------------
def _set_cell_text(cell, text: str, bold: bool = False, align=WD_ALIGN_PARAGRAPH.LEFT, size: int = 10):
    cell.text = ""
    p = cell.paragraphs[0]
    p.alignment = align
    run = p.add_run(_safe_str(text))
    run.bold = bold
    run.font.size = Pt(size)
    p.paragraph_format.space_after = Pt(0)
    cell.vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER


def _add_key_value_table(doc: Document, heading: str, data: Dict[str, Any], preferred_order: Optional[List[str]] = None) -> None:
    doc.add_heading(heading, level=1)

    items: List[tuple[str, Any]] = []
    seen = set()

    for key in preferred_order or []:
        if key in (data or {}):
            items.append((key, data.get(key)))
            seen.add(key)

    for k, v in (data or {}).items():
        if k not in seen:
            items.append((k, v))

    filtered = [(k, v) for k, v in items if _safe_str(v)]
    if not filtered:
        p = doc.add_paragraph("No information provided.")
        set_paragraph_style(p)
        return

    table = doc.add_table(rows=0, cols=2)
    table.style = "Table Grid"
    table.alignment = WD_TABLE_ALIGNMENT.CENTER

    for key, value in filtered:
        row = table.add_row().cells
        _set_cell_text(row[0], key.replace("_", " ").title(), bold=True, size=10)
        _set_cell_text(row[1], value, size=10)

    doc.add_paragraph()


def _add_transaction_summary_table(doc: Document, pivot_summary: List[Dict[str, Any]]) -> None:
    doc.add_heading("Transaction Summary", level=1)

    headers = [
        "Transaction Description",
        "Transaction Code",
        "Deposit",
        "Withdrawal",
        "Transaction Count",
        "CR%",
        "DR%",
    ]

    table = doc.add_table(rows=1, cols=len(headers))
    table.style = "Table Grid"
    table.alignment = WD_TABLE_ALIGNMENT.CENTER

    for i, h in enumerate(headers):
        _set_cell_text(table.rows[0].cells[i], h, bold=True, align=WD_ALIGN_PARAGRAPH.CENTER, size=10)

    total_dep = 0.0
    total_wd = 0.0
    total_count = 0

    for row in pivot_summary or []:
        desc = _safe_str(row.get("DESCRIPTION"))
        code = _safe_str(row.get("TRANSCODE"))
        dep = _safe_float(row.get("deposit", 0.0))
        wd = _safe_float(row.get("withdrawal", 0.0))
        cnt = _safe_int(row.get("count", 0))
        cr = _safe_float(row.get("CR%", 0.0))
        dr = _safe_float(row.get("DR%", 0.0))

        total_dep += dep
        total_wd += wd
        total_count += cnt

        cells = table.add_row().cells
        _set_cell_text(cells[0], desc, size=10)
        _set_cell_text(cells[1], code, align=WD_ALIGN_PARAGRAPH.CENTER, size=10)
        _set_cell_text(cells[2], f"K{dep:,.2f}" if abs(dep) > 1e-9 else "", align=WD_ALIGN_PARAGRAPH.RIGHT, size=10)
        _set_cell_text(cells[3], f"K{wd:,.2f}" if abs(wd) > 1e-9 else "", align=WD_ALIGN_PARAGRAPH.RIGHT, size=10)
        _set_cell_text(cells[4], str(cnt) if cnt else "", align=WD_ALIGN_PARAGRAPH.CENTER, size=10)
        _set_cell_text(cells[5], f"{cr:.2f}%" if abs(cr) > 1e-9 else "", align=WD_ALIGN_PARAGRAPH.RIGHT, size=10)
        _set_cell_text(cells[6], f"{dr:.2f}%" if abs(dr) > 1e-9 else "", align=WD_ALIGN_PARAGRAPH.RIGHT, size=10)

    doc.add_paragraph()


_SECTION_TITLE_MAP = {
    REQUIRED_HEADINGS[0]: "1. Account Overview",
    REQUIRED_HEADINGS[1]: "2. Source of Funds and Use of Funds",
    REQUIRED_HEADINGS[2]: "3. Key Observations and Suspicious Activity",
    REQUIRED_HEADINGS[3]: "4. Conclusion and Review Context",
}


def _render_narrative(doc: Document, aml_narrative: str) -> None:
    raw_lines = aml_narrative.split("\n")
    lines = [ln.rstrip() for ln in raw_lines if ln.strip()]
    required_set = set(REQUIRED_HEADINGS)

    for line in lines:
        s = line.strip()

        if s in required_set:
            hd = doc.add_heading(_SECTION_TITLE_MAP.get(s, s), level=1)
            hd.paragraph_format.space_before = Pt(10)
            hd.paragraph_format.space_after = Pt(4)
            continue

        if line.startswith("- "):
            p = doc.add_paragraph(style="List Bullet")
            p.add_run(line[2:].strip())
            set_paragraph_style(p)
            continue

        if line.startswith("  - "):
            p = doc.add_paragraph(style="List Bullet 2")
            p.add_run(line[4:].strip())
            set_paragraph_style(p)
            continue

        if line.startswith("    - "):
            p = doc.add_paragraph(style="List Bullet 3")
            p.add_run(line[6:].strip())
            set_paragraph_style(p)
            continue

        if line.startswith("    ✓ ") or line.startswith("      ✓ "):
            text = s.replace("✓", "").strip()
            p = doc.add_paragraph(style="List Bullet 2")
            p.add_run(text)
            set_paragraph_style(p)
            continue

        if line.startswith("      • "):
            p = doc.add_paragraph(style="List Bullet 3")
            p.add_run(line[8:].strip())
            set_paragraph_style(p)
            continue

        p = doc.add_paragraph(s)
        set_paragraph_style(p)


# ------------------------------------------------------------
# Main generator
# ------------------------------------------------------------
def generate_review_doc(
    client: Dict[str, Any],
    trigger: Dict[str, Any],
    aml_narrative: str,
    pivot_summary: List[Dict[str, Any]],
    material_channels: Optional[List[Dict[str, Any]]] = None,
    suspicious_by_channel: Optional[Dict[str, Any]] = None,
    raw_transactions: Any = None,
) -> str:
    validate_aml_narrative(aml_narrative)

    doc = Document()

    section = doc.sections[0]
    section.page_width = Inches(8.27)
    section.page_height = Inches(11.69)
    section.top_margin = Inches(0.7)
    section.bottom_margin = Inches(0.7)
    section.left_margin = Inches(0.7)
    section.right_margin = Inches(0.7)

    title = doc.add_heading("", level=0)
    run = title.add_run("AML / CTF Statement Review Report")
    run.font.size = Pt(16)
    run.bold = True
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    title.paragraph_format.space_after = Pt(6)

    subtitle = doc.add_paragraph()
    subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
    subtitle_run = subtitle.add_run("Deterministic statement review output")
    subtitle_run.font.size = Pt(10)
    subtitle_run.italic = True
    subtitle.paragraph_format.space_after = Pt(10)

    _add_key_value_table(
        doc,
        "Client Information",
        client or {},
        preferred_order=[
            "company_name", "client_name", "ubo_name", "risk_classification",
            "source_of_funds", "declared_source_of_funds", "place_of_incorp",
            "nature_of_business", "review_period", "review_date", "client_type",
            "profile", "individualProfile",
        ],
    )

    _add_key_value_table(
        doc,
        "Trigger Information",
        trigger or {},
        preferred_order=["type", "source", "description", "pep_tier"],
    )

    _add_transaction_summary_table(doc, pivot_summary or [])

    doc.add_heading("Review Assessment", level=1)
    _render_narrative(doc, aml_narrative)

    output = "AML_Review_Report.docx"
    doc.save(output)
    return output
