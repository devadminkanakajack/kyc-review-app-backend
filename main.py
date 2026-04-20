# main.py  (AML Review Backend v7 — Detector-Enabled Architecture)
# UPDATED: uses analyze_statement material_channels detector_suspicious_transactions
# and prints them into the DOCX grouped by TRANSCODE.

import os
import json
import tempfile
import asyncio
import time
from typing import Any, Dict

import pandas as pd
from fastapi import FastAPI, Form, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from utils.file_parser import load_transactions
from utils.analyze_statement import analyze_statement
from utils.doc_generator import generate_review_doc
from utils.code_lookup import TransactionCodeLookup

# NOTE: your repo uses utils/builder.py (not narrative_builder.py)
from utils.builder import build_narrative_v1

# Optional risk engine
try:
    from utils import risk_engine
except ImportError:
    risk_engine = None


app = FastAPI(title="AML Review Backend (Detector-Enabled v7)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CODE_FILE = os.path.join("utils", "Number Codes.csv")


@app.on_event("startup")
async def startup_event():
    global code_lookup
    code_lookup = TransactionCodeLookup(CODE_FILE)

    print("======================================")
    print("🔥 AML Backend Started")
    print(" Transaction Code Library Loaded")
    print(code_lookup.summary())
    print("======================================")


async def delete_later(path: str, delay: int = 300):
    await asyncio.sleep(delay)
    if os.path.exists(path):
        os.remove(path)
        print(f"🧹 Deleted temp file: {path}")


def _safe_json_loads(s: str, fallback):
    try:
        return json.loads(s)
    except Exception:
        return fallback


def _safe_int(v, default=0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _timed_seconds(start: float) -> float:
    return round(time.perf_counter() - start, 4)


def _normalize_parser_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep fail-soft behavior, but avoid reworking parser-normalized columns more than needed.
    """
    if "ROW_ID" not in df.columns:
        df["ROW_ID"] = range(len(df))

    if "TRANSCODE" not in df.columns:
        df["TRANSCODE"] = "UNKNOWN"
    else:
        df["TRANSCODE"] = (
            df["TRANSCODE"]
            .astype(str)
            .replace(["nan", "NaN", "None", ""], "UNKNOWN")
        )

    if "DESCRIPTION" not in df.columns:
        df["DESCRIPTION"] = "UNKNOWN"
    else:
        df["DESCRIPTION"] = (
            df["DESCRIPTION"]
            .astype(str)
            .replace(["nan", "NaN", "None", ""], "UNKNOWN")
        )

    if "IDENTITY" not in df.columns:
        df["IDENTITY"] = "UNKNOWN"
    else:
        df["IDENTITY"] = (
            df["IDENTITY"]
            .astype(str)
            .replace(["nan", "NaN", "None", ""], "UNKNOWN")
        )

    if "DEBIT" not in df.columns:
        df["DEBIT"] = 0.0
    if "CREDIT" not in df.columns:
        df["CREDIT"] = 0.0
    if "BALANCE" not in df.columns:
        df["BALANCE"] = 0.0

    for col in ("DEBIT", "CREDIT", "BALANCE"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


def _log_dataframe_profile(df: pd.DataFrame) -> Dict[str, Any]:
    row_count = int(len(df))
    unique_transcodes = int(df["TRANSCODE"].nunique(dropna=True)) if "TRANSCODE" in df.columns else 0
    unique_identities = int(df["IDENTITY"].replace("", pd.NA).dropna().nunique()) if "IDENTITY" in df.columns else 0

    date_min = None
    date_max = None
    if "DATE" in df.columns:
        date_series = pd.to_datetime(df["DATE"], errors="coerce")
        if date_series.notna().any():
            date_min = str(date_series.min().date())
            date_max = str(date_series.max().date())

    total_credits = float(df["CREDIT"].sum()) if "CREDIT" in df.columns else 0.0
    total_debits = float(df["DEBIT"].sum()) if "DEBIT" in df.columns else 0.0

    if row_count >= 50000:
        size_band = "VERY_HEAVY"
    elif row_count >= 25000:
        size_band = "HEAVY"
    elif row_count >= 10000:
        size_band = "MEDIUM_HEAVY"
    else:
        size_band = "NORMAL"

    print("======================================")
    print("PARSED STATEMENT PROFILE")
    print(f" rows: {row_count}")
    print(f" size_band: {size_band}")
    print(f" unique_transcodes: {unique_transcodes}")
    print(f" unique_identities: {unique_identities}")
    print(f" date_range: {date_min or 'N/A'} -> {date_max or 'N/A'}")
    print(f" total_credits: {total_credits:,.2f}")
    print(f" total_debits: {total_debits:,.2f}")
    print("======================================")

    return {
        "row_count": row_count,
        "size_band": size_band,
        "unique_transcodes": unique_transcodes,
        "unique_identities": unique_identities,
        "date_min": date_min,
        "date_max": date_max,
        "total_credits": total_credits,
        "total_debits": total_debits,
    }


def _log_detector_summary(detectors: Dict[str, Any]) -> None:
    if not isinstance(detectors, dict):
        return

    print("======================================")
    print("DETECTOR SUMMARY")
    for name, payload in detectors.items():
        if not isinstance(payload, dict):
            print(f" {name}: unavailable")
            continue

        triggered = payload.get("triggered")
        strength = payload.get("strength")
        flagged_count = len(payload.get("flagged_row_ids") or [])
        print(
            f" {name}: triggered={triggered} | strength={strength} | flagged_rows={flagged_count}"
        )
    print("======================================")


@app.post("/api/review")
async def aml_review(
    client_type: str = Form(...),
    trigger_info: str = Form(...),
    client_data: str = Form(...),
    file: UploadFile = None,
):
    request_started = time.perf_counter()

    print("\n======================================")
    print(" AML REVIEW REQUEST RECEIVED")
    print("======================================")

    try:
        trigger = _safe_json_loads(trigger_info, fallback={}) or {}
        client = _safe_json_loads(client_data, fallback={}) or {}

        # Ensure canonical keys exist for downstream modules
        client["client_type"] = client_type
        client.setdefault("type", client_type)

        # Canonicalise profile + declared SoF fields so downstream modules use one view.
        profile_value = (
            client.get("individualProfile")
            or client.get("profile")
            or client.get("individual_profile_type")
            or client.get("individual_profile")
            or ("Generic Company" if str(client_type).strip().lower() != "individual" else None)
        )
        if profile_value:
            client["individualProfile"] = profile_value
            client["profile"] = profile_value

        declared_sof = (
            client.get("source_of_funds")
            or client.get("sourceOfFunds")
            or client.get("declared_source_of_funds")
            or client.get("sof")
        )
        if declared_sof:
            client["source_of_funds"] = declared_sof
            client["sourceOfFunds"] = declared_sof
            client["declared_source_of_funds"] = declared_sof

        print(f"➡️ Client Type: {client_type}")
        print(f"➡️ Trigger: {trigger.get('type')} | Source: {trigger.get('source')}")
        print(f"➡️ Description: {trigger.get('description')}")
        print(f"➡️ Individual profile: {client.get('individualProfile') or client.get('profile')}")
        print(f"➡️ Declared SoF: {client.get('source_of_funds') or client.get('sourceOfFunds') or client.get('declared_source_of_funds')}")
        print(f"➡️ File Received: {file.filename if file else 'None'}")

        if not file:
            return JSONResponse({"error": "Missing file"}, status_code=400)

        suffix = os.path.splitext(file.filename)[1] or ".dat"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        print(f"Temp file saved: {tmp_path}")

        parse_started = time.perf_counter()
        try:
            df = load_transactions(tmp_path, code_lookup, client_type)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        df = _normalize_parser_output(df)
        parse_seconds = _timed_seconds(parse_started)
        print(f"✅ load_transactions() completed in {parse_seconds}s")

        required_cols = ["ROW_ID", "DATE", "TRANSCODE", "DESCRIPTION", "DEBIT", "CREDIT", "IDENTITY"]
        missing_required = [c for c in required_cols if c not in df.columns]
        if missing_required:
            raise ValueError(f"Parser output missing required columns: {missing_required}")

        profile_stats = _log_dataframe_profile(df)

        # ----------------------------------------------------------
        # Analysis
        # IMPORTANT: pass client into analyze_statement so it can use individualProfile/profile
        # ----------------------------------------------------------
        analysis_started = time.perf_counter()
        print("🔍 Running analyze_statement() ...")
        analysis = analyze_statement(
            df=df,
            code_lookup=code_lookup,
            client_type=client_type,
            client_data=client,
        )
        analysis_seconds = _timed_seconds(analysis_started)
        print(f"✅ analyze_statement() completed in {analysis_seconds}s")

        pivot_summary = analysis.get("pivot_summary", [])
        raw_df = analysis.get("raw_df", df)

        recurrence_clusters = {
            "identity_clusters": analysis.get("identity_clusters", []),
            "narrative_clusters": analysis.get("narrative_clusters", []),
            "same_day_identity_clusters": analysis.get("same_day_identity_clusters", []),
            "identity_summary": analysis.get("identity_summary", []),
            "recurrence_error": analysis.get("recurrence_error"),
        }

        detectors = analysis.get("detectors") or {
            "structured_deposits": analysis.get("structured_deposits"),
            "structured_payments": analysis.get("structured_payments"),
            "pass_through": analysis.get("pass_through"),
            "layering": analysis.get("layering"),
            "round_figures": analysis.get("round_figures"),
            "salary_pattern": analysis.get("salary_pattern"),
            "cash_intensive": analysis.get("cash_intensive"),
            "third_party": analysis.get("third_party"),
            "recurrence": analysis.get("recurrence"),
        }

        material_channels = analysis.get("material_channels", [])
        channel_profile = analysis.get("channel_profile") or {}

        # ----------------------------------------------------------
        # Debug proof that the new channel profile pipeline is active
        # ----------------------------------------------------------
        try:
            cp_credit_n = len(((channel_profile.get("credit") or {}).get("channels") or []))
            cp_debit_n = len(((channel_profile.get("debit") or {}).get("channels") or []))
        except Exception:
            cp_credit_n, cp_debit_n = 0, 0

        print("======================================")
        print("DEBUG PIPELINE")
        print(" client.individualProfile:", client.get("individualProfile"))
        print(" client.profile:", client.get("profile"))
        print(" channel_profile.profile:", channel_profile.get("profile"))
        print(" channel_profile credit channels:", cp_credit_n)
        print(" channel_profile debit channels:", cp_debit_n)
        print("======================================")

        analysis_meta = analysis.get("performance") or analysis.get("meta") or {}
        if analysis_meta:
            print("======================================")
            print("ANALYSIS META")
            for k, v in analysis_meta.items():
                print(f" {k}: {v}")
            print("======================================")

        _log_detector_summary(detectors)

        # ----------------------------------------------------------
        # Optional risk engine (kept)
        # ----------------------------------------------------------
        risk_metrics = None
        risk_started = time.perf_counter()
        if risk_engine and hasattr(risk_engine, "compute_risk_metrics"):
            try:
                risk_metrics = risk_engine.compute_risk_metrics(
                    analysis=analysis,
                    client_type=client_type,
                    client_profile=client,
                    trigger=trigger,
                )
            except Exception as re_err:
                print(f"⚠️ Risk engine error ignored: {re_err}")
        risk_seconds = _timed_seconds(risk_started)
        print(f"✅ risk_engine stage completed in {risk_seconds}s")

        total_credits = (
            float(raw_df["CREDIT"].sum())
            if "CREDIT" in raw_df.columns
            else float(df["CREDIT"].sum())
        )
        total_debits = (
            float(raw_df["DEBIT"].sum())
            if "DEBIT" in raw_df.columns
            else float(df["DEBIT"].sum())
        )

        # ----------------------------------------------------------
        # NEW: count suspicious txns (from analyze_statement per-channel listings)
        # ----------------------------------------------------------
        suspicious_channels = []
        suspicious_total_rows = 0

        for ch in material_channels or []:
            c = _safe_int(ch.get("detector_suspicious_transactions_count"), 0)
            if c > 0:
                suspicious_channels.append(ch)
                suspicious_total_rows += c

        print(f"🧾 Per-channel detector suspicious rows (material_channels): {suspicious_total_rows}")

        # ----------------------------------------------------------
        # Context for deterministic narrative
        # ----------------------------------------------------------
        context = {
            "client": client,
            "trigger": trigger,
            "pivot_summary": pivot_summary,
            "material_channels": material_channels,
            "channel_profile": channel_profile,
            "recurrence_clusters": recurrence_clusters,
            "detectors": detectors,
            "totals": {"credits": total_credits, "debits": total_debits},
            "risk_metrics": risk_metrics,
            "statement_profile": profile_stats,
            "performance": {
                "parse_seconds": parse_seconds,
                "analysis_seconds": analysis_seconds,
                "risk_seconds": risk_seconds,
            },
            # narrative can reference this count if wanted
            "suspicious_total_rows": suspicious_total_rows,
        }

        narrative_started = time.perf_counter()
        print("Building deterministic narrative (no AI)...")
        aml_narrative = build_narrative_v1(context)
        narrative_seconds = _timed_seconds(narrative_started)
        print(f"✅ Narrative build completed in {narrative_seconds}s")

        doc_started = time.perf_counter()
        print("Generating DOCX AML Review Report...")
        doc_path = generate_review_doc(
            client=client,
            trigger=trigger,
            aml_narrative=aml_narrative,
            pivot_summary=pivot_summary,
            # primary source of row-level suspicious listings (already grouped by TRANSCODE)
            material_channels=material_channels,
            # Optional: keep risk drill-down if you still want it later
            suspicious_by_channel=(risk_metrics or {}).get("suspicious_by_channel") if isinstance(risk_metrics, dict) else None,
            raw_transactions=raw_df,
        )
        doc_seconds = _timed_seconds(doc_started)
        print(f"✅ DOCX generation completed in {doc_seconds}s")

        total_seconds = _timed_seconds(request_started)
        print("======================================")
        print("REQUEST TIMING SUMMARY")
        print(f" parse_seconds: {parse_seconds}")
        print(f" analysis_seconds: {analysis_seconds}")
        print(f" risk_seconds: {risk_seconds}")
        print(f" narrative_seconds: {narrative_seconds}")
        print(f" doc_seconds: {doc_seconds}")
        print(f" total_seconds: {total_seconds}")
        print("======================================")

        asyncio.create_task(delete_later(doc_path))

        return FileResponse(
            doc_path,
            filename="AML_Review_Report.docx",
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )

    except Exception as e:
        print("\n❌ ERROR OCCURRED DURING AML REVIEW")
        print(f" {e}")
        print("======================================\n")
        return JSONResponse({"error": str(e)}, status_code=500)
