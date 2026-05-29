"""
model_builder/api.py — FastAPI routes for Model Builder.

Routes orchestrate. They do not contain architectural logic.
Business logic lives in the modules they call:

    sessions.py     — session store
    inference.py    — column suggestions and type inference
    review.py       — pre-ingest flag generation
    resolver.py     — variant detection (D12)
    compiler/       — artifact emission (DuckDB, Postgres, SRF)
    parsers/        — format-specific source parsers (OSI, dbt)

Rule: MB declares and compiles. It does not silently reconcile,
cleanse, or assert identity. Crosswalk owns identity assertion.
"""

from __future__ import annotations

import io
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from pydantic import BaseModel

from .config   import OUTPUT_DIR, SAMPLE_ROWS
from .models   import (
    BuildSpec, IntrospectRequest, MappingRow,
    ReviewRequest,
)
from .sessions import (
    SessionData, get_session, new_token, put_session, purge_expired,
)
from .inference  import build_columns
from .review     import run_review
from .compiler   import compile_artifact

try:
    from .parsers.osi import parse_osi_upload, export_osi_model
    OSI_AVAILABLE = True
except ImportError:
    OSI_AVAILABLE = False

try:
    from .parsers.dbt import parse_dbt_upload
    DBT_AVAILABLE = True
except ImportError:
    DBT_AVAILABLE = False

try:
    from osi_parser import parse_json_array
    JSON_ARRAY_AVAILABLE = True
except ImportError:
    JSON_ARRAY_AVAILABLE = False

try:
    from sqlalchemy import create_engine, text, inspect as sa_inspect
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

try:
    from mb8_data_connect import connect_router
    MB8_AVAILABLE = True
except ImportError:
    MB8_AVAILABLE = False


# ── Router ────────────────────────────────────────────────────────────────────

router = APIRouter(prefix="/api/mb", tags=["model_builder"])

if MB8_AVAILABLE:
    router.include_router(connect_router)


# ── POST /api/mb/upload ───────────────────────────────────────────────────────

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload a CSV or Excel file.
    Returns columns with sample values and suggested dimension mappings.
    File is held in session store for subsequent /review and /compile calls.
    """
    purge_expired()

    ext = Path(file.filename).suffix.lower()
    if ext not in {".csv", ".xlsx", ".xls", ".json"}:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}. Use CSV, Excel, or JSON.")

    content = await file.read()

    try:
        if ext == ".csv":
            try:
                df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
            except UnicodeDecodeError:
                df = pd.read_csv(io.BytesIO(content), dtype=str, encoding="latin-1", keep_default_na=False)
        elif ext == ".json":
            if not JSON_ARRAY_AVAILABLE:
                raise HTTPException(
                    status_code=501,
                    detail="osi_parser.py required for JSON upload."
                )
            parsed = parse_json_array(content, file.filename)
            df = pd.DataFrame(parsed["rows"])
        else:
            df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=False)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not parse file: {e}")

    if df.empty:
        raise HTTPException(status_code=422, detail="File appears to be empty.")

    columns = build_columns(df)
    token   = new_token()
    put_session(token, SessionData(
        df          = df,
        source_info = {"type": "file", "filename": file.filename, "format": ext.lstrip(".")},
        columns     = columns,
    ))

    return {"upload_token": token, "columns": columns, "row_count": len(df)}


# ── POST /api/mb/upload_path ──────────────────────────────────────────────────

class UploadPathRequest(BaseModel):
    file_path: str


@router.post("/upload_path")
async def upload_path(req: UploadPathRequest):
    """
    Load a file by disk path (Tauri drag-drop).
    Returns the same shape as /upload.
    """
    purge_expired()

    path = Path(req.file_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {req.file_path}")

    ext = path.suffix.lower()
    if ext not in {".csv", ".xlsx", ".xls", ".json"}:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}.")

    try:
        content = path.read_bytes()
        if ext == ".csv":
            try:
                df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
            except UnicodeDecodeError:
                df = pd.read_csv(io.BytesIO(content), dtype=str, encoding="latin-1", keep_default_na=False)
        elif ext == ".json":
            if not JSON_ARRAY_AVAILABLE:
                raise HTTPException(status_code=501, detail="osi_parser.py required for JSON upload.")
            parsed = parse_json_array(content, path.name)
            df = pd.DataFrame(parsed["rows"])
        else:
            df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=False)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not parse file: {e}")

    if df.empty:
        raise HTTPException(status_code=422, detail="File appears to be empty.")

    columns = build_columns(df)
    token   = new_token()
    put_session(token, SessionData(
        df          = df,
        source_info = {"type": "file", "filename": path.name, "format": ext.lstrip(".")},
        columns     = columns,
    ))

    return {"upload_token": token, "columns": columns, "row_count": len(df)}


# ── POST /api/mb/introspect ───────────────────────────────────────────────────

@router.post("/introspect")
async def introspect_sql(req: IntrospectRequest):
    """
    Introspect a live Postgres table via SQLAlchemy. Read-only.
    Connection string is used once and never stored.
    """
    purge_expired()

    if not SQLALCHEMY_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="SQLAlchemy required. pip install sqlalchemy psycopg2-binary"
        )

    schema = req.schema_name or "public"
    table  = req.table_name

    try:
        engine = create_engine(req.connection_string, connect_args={"connect_timeout": 10})
        with engine.connect() as conn:
            col_rows = conn.execute(text(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = :schema AND table_name = :table "
                "ORDER BY ordinal_position"
            ), {"schema": schema, "table": table}).fetchall()

            if not col_rows:
                raise HTTPException(
                    status_code=404,
                    detail=f"Table '{schema}.{table}' not found or no columns accessible."
                )

            try:
                count_row = conn.execute(text(
                    "SELECT reltuples::BIGINT FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = :schema AND c.relname = :table"
                ), {"schema": schema, "table": table}).fetchone()
                row_count = int(count_row[0]) if count_row else 0
            except Exception:
                row_count = 0

            col_names    = [r[0] for r in col_rows]
            sample_rows  = conn.execute(
                text(f'SELECT * FROM "{schema}"."{table}" LIMIT :n'),
                {"n": SAMPLE_ROWS}
            ).fetchall()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Connection failed: {e}")
    finally:
        try:
            engine.dispose()
        except Exception:
            pass

    df      = pd.DataFrame([dict(zip(col_names, r)) for r in sample_rows], columns=col_names).astype(str)
    columns = build_columns(df)
    token   = new_token()
    put_session(token, SessionData(
        df          = df,
        source_info = {"type": "sql", "table_name": table, "schema_name": schema},
        columns     = columns,
    ))

    return {"introspect_token": token, "columns": columns, "row_count": row_count}


# ── POST /api/mb/review ───────────────────────────────────────────────────────

@router.post("/review")
async def review(req: ReviewRequest):
    """
    Run pre-ingest review on mapped columns.
    Flags are informational — not blocking. The human decides what matters.
    """
    purge_expired()
    session         = get_session(req.source_token)
    mapping         = [m.dict() for m in req.columns_mapped]
    session.mapping = mapping
    flags           = run_review(session.df, mapping)
    return {"flags": flags}


# ── POST /api/mb/compile ──────────────────────────────────────────────────────

@router.post("/compile")
async def compile_job(spec: BuildSpec):
    """
    Compile a BuildSpec → artifact + download URL.

    Routes call compile_artifact(). They do not know which emitter runs.
    INVARIANT: This endpoint never writes to any database directly.
    It produces an artifact file. The human loads it.
    """
    purge_expired()

    token = (
        spec.source.upload_token
        if spec.source.type in ("file", "osi", "json")
        else spec.source.introspect_token
    )
    if not token:
        raise HTTPException(status_code=400, detail="Missing source token in BuildSpec.")

    session      = get_session(token)
    mapping_cols = {m.column for m in spec.mapping}
    for col in spec.nucleus.columns:
        if col not in mapping_cols and col not in session.df.columns:
            raise HTTPException(
                status_code=422,
                detail=f"Nucleus column '{col}' not found in mapped columns or source data."
            )

    try:
        result = compile_artifact(session.df, spec.dict())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Compilation failed: {e}")

    return {
        "success":      True,
        "output_path":  result["output_path"],
        "download_url": result["download_url"],
        "entity_count": result["entity_count"],
        "fact_count":   result["fact_count"],
        "facts_by_dim": result["facts_by_dim"],
        "errors":       [],
        "warnings":     result["warnings"],
        "verification_report": {
            "facts_by_dim": result["facts_by_dim"],
            "entity_count": result["entity_count"],
            "lens_id":      spec.lens.lens_id,
            "backend":      spec.target.backend,
            "generated_at": datetime.utcnow().isoformat() + "Z",
        },
    }


# ── POST /api/mb/osi/parse ────────────────────────────────────────────────────

class OSIExportRequest(BaseModel):
    lens_id:     str
    description: Optional[str] = ""
    spoke_rows:  List[Dict[str, Any]]


@router.post("/osi/parse")
async def parse_osi(file: UploadFile = File(...)):
    """MB-6 — Parse an OSI semantic model definition (YAML or JSON)."""
    purge_expired()

    if not OSI_AVAILABLE:
        raise HTTPException(status_code=501, detail="osi_parser.py not found.")

    try:
        content = await file.read()
        result  = parse_osi_upload(content, file.filename)
    except RuntimeError as e:
        raise HTTPException(status_code=501, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OSI parse failed: {e}")

    token = new_token()
    put_session(token, SessionData(
        df          = result["df"],
        source_info = result["source_info"],
        columns     = result["columns"],
    ))

    return {
        "upload_token":  token,
        "columns":       result["columns"],
        "row_count":     result["field_count"],
        "nucleus_hints": result["nucleus_hints"],
        "relationships": result["relationships"],
        "osi_meta":      result["osi_meta"],
        "dataset_count": result["dataset_count"],
        "field_count":   result["field_count"],
        "source_type":   "osi",
    }


# ── POST /api/mb/osi/export ───────────────────────────────────────────────────

@router.post("/osi/export")
async def export_osi(req: OSIExportRequest):
    """MB-6 export — project a compiled SNF substrate as an OSI model."""
    if not OSI_AVAILABLE:
        raise HTTPException(status_code=501, detail="osi_parser.py not found.")
    if not req.spoke_rows:
        raise HTTPException(status_code=400, detail="spoke_rows must not be empty.")

    try:
        return export_osi_model(req.spoke_rows, req.lens_id, req.description)
    except RuntimeError as e:
        raise HTTPException(status_code=501, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OSI export failed: {e}")


# ── POST /api/mb/dbt/parse ────────────────────────────────────────────────────

@router.post("/dbt/parse")
async def parse_dbt(file: UploadFile = File(...)):
    """MB-7 — Parse a dbt schema.yml file."""
    purge_expired()

    if not DBT_AVAILABLE:
        raise HTTPException(status_code=501, detail="dbt_parser.py not found.")

    try:
        content = await file.read()
        result  = parse_dbt_upload(content, file.filename)
    except RuntimeError as e:
        raise HTTPException(status_code=501, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"dbt parse failed: {e}")

    token = new_token()
    put_session(token, SessionData(
        df          = result["df"],
        source_info = result["source_info"],
        columns     = result["columns"],
    ))

    return {
        "upload_token":    token,
        "columns":         result["columns"],
        "row_count":       result["field_count"],
        "nucleus_hints":   result["nucleus_hints"],
        "lens_candidates": result["lens_candidates"],
        "dbt_meta":        result["dbt_meta"],
        "model_count":     result["model_count"],
        "field_count":     result["field_count"],
        "source_type":     "dbt",
        "parse_warnings":  result["parse_warnings"],
    }


# ── GET /api/mb/download/{filename} ──────────────────────────────────────────

@router.get("/download/{filename}")
async def download_artifact(filename: str):
    """Serve a compiled artifact for download. No path traversal."""
    safe_name  = Path(filename).name
    path       = OUTPUT_DIR / safe_name
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Artifact '{safe_name}' not found.")
    if not path.is_file():
        raise HTTPException(status_code=400, detail="Not a file.")
    media_type = "text/plain" if safe_name.endswith(".sql") else "application/octet-stream"
    return FileResponse(path=str(path), filename=safe_name, media_type=media_type)


# ── POST /api/mb/save-to-substrates/{filename} ───────────────────────────────

@router.post("/save-to-substrates/{filename}")
async def save_to_substrates(filename: str):
    """
    Copy a compiled artifact into SNF_SUBSTRATES_DIR.
    Used by Tauri where browser downloads are not available.
    """
    safe_name = Path(filename).name
    src       = OUTPUT_DIR / safe_name
    if not src.exists():
        raise HTTPException(status_code=404, detail=f"Artifact '{safe_name}' not found.")
    if not src.is_file():
        raise HTTPException(status_code=400, detail="Not a file.")

    substrates_dir = Path(os.environ.get("SNF_SUBSTRATES_DIR", "./substrates"))
    substrates_dir.mkdir(parents=True, exist_ok=True)
    dest = substrates_dir / safe_name

    # On Windows, DuckDB holds a file lock while the substrate is loaded in Reckoner.
    # Delete the destination before copying to avoid WinError 32.
    if dest.exists():
        try:
            dest.unlink()
        except PermissionError:
            raise HTTPException(
                status_code=409,
                detail=f"Cannot overwrite '{safe_name}' — it is currently loaded in Reckoner. "
                       f"Remove it from Reckoner first, or use a different output_name."
            )

    shutil.copy2(src, dest)

    return {"saved": True, "filename": safe_name, "destination": str(dest)}
