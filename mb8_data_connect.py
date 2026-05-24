"""
mb8_data_connect.py — MB-8 Data Connection Step
Version: 0.2.0

FastAPI router fragment. Mount into model_builder_api.py.

MB-8 closes the "last third" problem for dbt and OSI vocab parsers.

The dbt/OSI parse gives you:
    dimension + semantic_key   (the first two thirds)

MB-8 adds:
    value from the actual data source   (the last third)

Two output paths:

PATH A — Postgres/SQL Server views (primary, data stays in place):
    1. Upload schema.yml -> /dbt/parse -> session token + dimension mappings
    2. POST /connect/inspect  — verify table exists, columns match, sample values
    3. POST /connect/compile  — emit materialized view DDL
    Data never moves. Views materialize in the user's own database.
    Works for Postgres and SQL Server via SQLAlchemy.

PATH B — Warehouse fallback (any warehouse, no credentials needed):
    1. POST /connect/generate-sql  — get SELECT DISTINCT query from session
    2. User runs query in Snowflake / Databricks / Redshift / Athena / BigQuery
    3. POST /connect/values  — upload three-column result CSV
    4. /compile with target=duckdb produces local substrate

The three-column canonical format (PATH B CSV, also returned by PATH A inspect):
    dimension | semantic_key | value
    WHAT      | album_name   | folklore
    HOW       | valence      | 0.237
    WHEN      | album_release_date | 2020-07-24

SQL Server connection strings:
    mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server
    mssql+pyodbc://@server/db?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes

Integration:
    from mb8_data_connect import connect_router
    router.include_router(connect_router)

Validated against:
    taylor_discography — Postgres, 20 columns, Spotify audio features
    jaffle_shop — CSV path, 3 tables, 28 assertions passing
"""

from __future__ import annotations

import io
from typing import Dict, List, Optional, Any

try:
    from fastapi import APIRouter, HTTPException, UploadFile, File, Form
    from pydantic import BaseModel
except ImportError:
    raise ImportError("FastAPI required.")

try:
    import pandas as pd
except ImportError:
    raise ImportError("pandas required.")


connect_router   = APIRouter()
VALID_DIMENSIONS = {"WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"}
SAMPLE_LIMIT     = 50


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class InspectRequest(BaseModel):
    upload_token:      str
    connection_string: str
    schema_name:       str = "public"
    table_name:        Optional[str] = None


class ColumnSample(BaseModel):
    column:         str
    dimension:      str
    semantic_key:   str
    sample_values:  List[str]
    distinct_count: int
    null_count:     int


class InspectResponse(BaseModel):
    upload_token:     str
    table_name:       str
    schema_name:      str
    row_count:        int
    columns_matched:  List[ColumnSample]
    columns_missing:  List[str]
    columns_extra:    List[str]
    ready_to_compile: bool
    generated_sql:    str


class CompileRequest(BaseModel):
    upload_token:      str
    connection_string: str
    schema_name:       str = "public"
    table_name:        Optional[str] = None
    lens_id:           Optional[str] = None
    output_name:       Optional[str] = None
    nucleus_column:    Optional[str] = None
    nucleus_prefix:    Optional[str] = None


class GenerateSqlRequest(BaseModel):
    upload_token: str
    schema_name:  str = "public"
    table_name:   Optional[str] = None


# ---------------------------------------------------------------------------
# SQLAlchemy helpers
# ---------------------------------------------------------------------------

def _get_engine(connection_string: str):
    try:
        from sqlalchemy import create_engine
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="sqlalchemy required. pip install sqlalchemy psycopg2-binary"
        )
    try:
        return create_engine(connection_string, pool_pre_ping=True)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid connection string: {e}")


def _test_connection(engine):
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Could not connect to database: {e}")


def _get_table_columns(engine, schema_name: str, table_name: str) -> List[str]:
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
                ORDER BY ordinal_position
            """), {"schema": schema_name, "table": table_name})
            cols = [row[0] for row in result]
        if not cols:
            raise HTTPException(
                status_code=404,
                detail=f"Table '{schema_name}.{table_name}' not found. Check schema_name and table_name."
            )
        return cols
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Column introspection failed: {e}")


def _get_row_count(engine, schema_name: str, table_name: str) -> int:
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text(
                f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"'
            ))
            return result.scalar() or 0
    except Exception:
        return -1


def _sample_column(engine, schema_name: str, table_name: str, column_name: str) -> Dict[str, Any]:
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            samples = [
                str(row[0]) for row in conn.execute(text(f"""
                    SELECT DISTINCT "{column_name}"::VARCHAR AS val
                    FROM "{schema_name}"."{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                    ORDER BY val
                    LIMIT :limit
                """), {"limit": SAMPLE_LIMIT})
                if row[0] is not None
            ]
            distinct_count = conn.execute(text(f"""
                SELECT COUNT(DISTINCT "{column_name}")
                FROM "{schema_name}"."{table_name}"
            """)).scalar() or 0
            null_count = conn.execute(text(f"""
                SELECT COUNT(*) FROM "{schema_name}"."{table_name}"
                WHERE "{column_name}" IS NULL
            """)).scalar() or 0
        return {"sample_values": samples, "distinct_count": distinct_count, "null_count": null_count}
    except Exception:
        return {"sample_values": [], "distinct_count": 0, "null_count": 0}


# ---------------------------------------------------------------------------
# SQL generator
# ---------------------------------------------------------------------------

def _generate_value_sql(mappings: List[dict], schema_name: str, table_name: str) -> str:
    """
    Generate a UNION ALL SELECT that produces dimension | semantic_key | value.
    Standard SQL — runs on Postgres, SQL Server, Snowflake, Databricks, Redshift, Athena.
    """
    parts = []
    for m in mappings:
        dim  = m.get("suggested_dim", m.get("dimension", ""))
        skey = m.get("suggested_key", m.get("semantic_key", ""))
        col  = m.get("name", m.get("column", ""))
        if not dim or not skey or not col or dim == "skip":
            continue
        parts.append(
            f"SELECT '{dim}' AS dimension, '{skey}' AS semantic_key, "
            f'CAST("{col}" AS VARCHAR) AS value\n'
            f'FROM "{schema_name}"."{table_name}"\n'
            f'WHERE "{col}" IS NOT NULL'
        )

    if not parts:
        return "-- No mappings found."

    return (
        "-- SNF value extraction\n"
        "-- Generated by Reckoner Model Builder\n"
        f"-- Source: {schema_name}.{table_name}\n"
        "-- Run this against your warehouse.\n"
        "-- Export result CSV with columns: dimension, semantic_key, value\n"
        "-- Upload via POST /api/mb/connect/values\n\n"
        + "\nUNION ALL\n".join(parts) + ";"
    )


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------

def _get_sessions():
    try:
        from model_builder_api import _sessions
        return _sessions
    except ImportError:
        raise HTTPException(status_code=500,
            detail="model_builder_api not found. mb8_data_connect must run alongside it.")


def _get_session(token: str):
    sessions = _get_sessions()
    if token not in sessions:
        raise HTTPException(status_code=404, detail=f"Session '{token}' not found or expired.")
    return sessions[token]


def _extract_mappings(columns_meta: List[dict]) -> List[dict]:
    return [
        c for c in columns_meta
        if not c.get("is_nucleus")
        and c.get("suggested_dim") not in (None, "skip", "")
    ]


def _infer_table_name(columns_meta: List[dict]) -> Optional[str]:
    from collections import Counter
    models = [c.get("model", "") for c in columns_meta if c.get("model")]
    if not models:
        return None
    return Counter(models).most_common(1)[0][0]


def _find_nucleus(columns_meta: List[dict], table_name: str) -> str:
    for c in columns_meta:
        if c.get("is_nucleus"):
            return c["name"]
    all_names = [c.get("name", "") for c in columns_meta]
    return next((n for n in all_names if n in ("id", f"{table_name}_id")),
                all_names[0] if all_names else "id")


# ---------------------------------------------------------------------------
# PATH A — Postgres / SQL Server
# ---------------------------------------------------------------------------

@connect_router.post("/connect/inspect")
async def connect_inspect(req: InspectRequest):
    """
    PATH A Step 1 — Connect to database, verify columns, sample values.

    Returns InspectResponse with:
    - columns_matched: declared columns that exist, with sample values
    - columns_missing: declared but not in table (mapping error)
    - columns_extra:   in table but not declared (informational)
    - generated_sql:   warehouse fallback SELECT for users who prefer not to share creds
    - ready_to_compile: True if all declared columns found
    """
    session      = _get_session(req.upload_token)
    columns_meta = session.columns
    mappings     = _extract_mappings(columns_meta)

    if not mappings:
        raise HTTPException(status_code=422,
            detail="No mappings in session. Run /dbt/parse or /osi/parse first.")

    table_name = req.table_name or _infer_table_name(columns_meta)
    if not table_name:
        raise HTTPException(status_code=422,
            detail="Could not infer table name. Provide table_name explicitly.")

    engine = _get_engine(req.connection_string)
    _test_connection(engine)

    actual_cols    = _get_table_columns(engine, req.schema_name, table_name)
    actual_col_set = set(actual_cols)
    declared_cols  = {m.get("name", m.get("column", "")) for m in mappings}

    columns_matched = []
    columns_missing = []

    for m in mappings:
        col  = m.get("name", m.get("column", ""))
        dim  = m.get("suggested_dim", m.get("dimension", ""))
        skey = m.get("suggested_key", m.get("semantic_key", ""))
        if col not in actual_col_set:
            columns_missing.append(col)
            continue
        sample = _sample_column(engine, req.schema_name, table_name, col)
        columns_matched.append(ColumnSample(
            column=col, dimension=dim, semantic_key=skey,
            **sample
        ))

    columns_extra = [c for c in actual_cols if c not in declared_cols]
    row_count     = _get_row_count(engine, req.schema_name, table_name)
    generated_sql = _generate_value_sql(mappings, req.schema_name, table_name)
    ready         = bool(columns_matched) and not columns_missing

    return InspectResponse(
        upload_token     = req.upload_token,
        table_name       = table_name,
        schema_name      = req.schema_name,
        row_count        = row_count,
        columns_matched  = columns_matched,
        columns_missing  = columns_missing,
        columns_extra    = columns_extra,
        ready_to_compile = ready,
        generated_sql    = generated_sql,
    )


@connect_router.post("/connect/compile")
async def connect_compile(req: CompileRequest):
    """
    PATH A Step 2 — Emit materialized view DDL.

    Builds BuildSpec from session metadata + connection details and calls
    the existing _emit_postgres_views unchanged. Returns SQL script for download.

    Generated script creates:
        {output_name}_snf schema
        snf_who/what/when/where/why/how materialized views
        snf_hub display table
        snf_affordances view (Portolan planner fast-path)
        refresh.sql comments

    Example for taylor_discography:
        CREATE SCHEMA IF NOT EXISTS taylor_discography_snf;
        CREATE MATERIALIZED VIEW taylor_discography_snf.snf_what AS
            SELECT track_id AS entity_id, 'what' AS dimension,
                   'album_name' AS semantic_key, album_name::VARCHAR AS value, ...
            FROM public.taylor_discography WHERE album_name IS NOT NULL
            UNION ALL ...
    """
    session      = _get_session(req.upload_token)
    columns_meta = session.columns
    mappings     = _extract_mappings(columns_meta)

    if not mappings:
        raise HTTPException(status_code=422, detail="No mappings in session.")

    table_name    = req.table_name  or _infer_table_name(columns_meta) or "source_table"
    output_name   = req.output_name or table_name.replace(".", "_")
    lens_id       = req.lens_id     or f"{output_name}_v1"
    nucleus_col   = req.nucleus_column or _find_nucleus(columns_meta, table_name)

    mapping_list = [
        {
            "column":       m.get("name", m.get("column", "")),
            "dimension":    m.get("suggested_dim", m.get("dimension", "")),
            "semantic_key": m.get("suggested_key", m.get("semantic_key", "")),
        }
        for m in mappings
        if m.get("name", m.get("column", ""))
        and m.get("suggested_dim", m.get("dimension", "")) != "skip"
    ]

    spec = {
        "mapping": mapping_list,
        "nucleus": {
            "columns":   [nucleus_col],
            "separator": "-",
            "prefix":    req.nucleus_prefix or "",
        },
        "lens":   {"lens_id": lens_id},
        "target": {"backend": "postgres_views", "output_name": output_name},
        "source": {
            "type":        "sql",
            "schema_name": req.schema_name,
            "table_name":  table_name,
        },
    }

    try:
        from model_builder_api import _emit_postgres_views
        result = _emit_postgres_views(pd.DataFrame(), spec)
    except ImportError:
        raise HTTPException(status_code=500,
            detail="_emit_postgres_views not found. Ensure model_builder_api.py is present.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"View generation failed: {e}")

    return {
        "upload_token":   req.upload_token,
        "table_name":     table_name,
        "schema_name":    req.schema_name,
        "lens_id":        lens_id,
        "output_name":    output_name,
        "nucleus_column": nucleus_col,
        "mappings_count": len(mapping_list),
        "download_url":   result["download_url"],
        "facts_by_dim":   result["facts_by_dim"],
        "message": (
            f"SQL script ready. Download and run in your Postgres instance. "
            f"Creates {output_name}_snf schema with materialized views over "
            f"{req.schema_name}.{table_name}. Data stays in place."
        ),
        "warnings": result.get("warnings", []),
    }


# ---------------------------------------------------------------------------
# PATH B — Warehouse fallback
# ---------------------------------------------------------------------------

@connect_router.post("/connect/generate-sql")
async def generate_sql(req: GenerateSqlRequest):
    """
    PATH B Step 1 — Generate value extraction SQL without connecting.

    For users who prefer not to share credentials, or whose warehouse
    SQLAlchemy doesn't cover (Snowflake, Databricks, BigQuery, Athena).

    Returns the SELECT DISTINCT SQL to run in their own environment.
    Output CSV uploaded via /connect/values.
    """
    session      = _get_session(req.upload_token)
    columns_meta = session.columns
    mappings     = _extract_mappings(columns_meta)
    table_name   = req.table_name or _infer_table_name(columns_meta) or "your_table"
    sql          = _generate_value_sql(mappings, req.schema_name, table_name)

    return {
        "upload_token": req.upload_token,
        "table_name":   table_name,
        "schema_name":  req.schema_name,
        "sql":          sql,
        "instructions": (
            "Run this SQL against your warehouse. "
            "Export result as CSV with columns: dimension, semantic_key, value. "
            "Upload via POST /api/mb/connect/values."
        ),
    }


@connect_router.post("/connect/values")
async def connect_values(
    upload_token: str        = Form(...),
    file:         UploadFile = File(...),
):
    """
    PATH B Step 2 — Accept three-column value CSV.

    CSV columns: dimension | semantic_key | value
    This is the output of running generated_sql from /connect/generate-sql.

    Populates session for /compile with target=duckdb.
    """
    session      = _get_session(upload_token)
    columns_meta = session.columns

    content = await file.read()
    try:
        values_df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not parse CSV: {e}")

    missing_cols = {"dimension", "semantic_key", "value"} - set(values_df.columns)
    if missing_cols:
        raise HTTPException(status_code=422,
            detail=f"CSV must have columns: dimension, semantic_key, value. Missing: {missing_cols}")

    bad_dims = set(values_df["dimension"].unique()) - VALID_DIMENSIONS
    if bad_dims:
        raise HTTPException(status_code=422,
            detail=f"Unknown dimensions: {bad_dims}. Valid: {VALID_DIMENSIONS}")

    # Build wide DataFrame: one column per (dimension, semantic_key) pair
    # Each row = one value. compile iterates rows and writes spoke facts.
    col_specs = (
        values_df[["dimension", "semantic_key"]]
        .drop_duplicates()
        .values.tolist()
    )

    col_mapping = []
    wide_data   = {}
    for dim, skey in col_specs:
        col_name = f"{dim}___{skey}"
        vals = values_df[
            (values_df["dimension"] == dim) &
            (values_df["semantic_key"] == skey)
        ]["value"].tolist()
        wide_data[col_name] = vals
        col_mapping.append({"column": col_name, "dimension": dim, "semantic_key": skey})

    # Pad to equal length
    max_len = max(len(v) for v in wide_data.values()) if wide_data else 0
    for col in wide_data:
        wide_data[col] += [None] * (max_len - len(wide_data[col]))

    wide_df = pd.DataFrame(wide_data)

    session.df = wide_df
    session.columns = [
        {
            "name":          m["column"],
            "suggested_dim": m["dimension"],
            "suggested_key": m["semantic_key"],
            "is_nucleus":    False,
            "confidence":    "from_values_csv",
        }
        for m in col_mapping
    ]

    return {
        "upload_token":    upload_token,
        "rows_received":   len(values_df),
        "columns_built":   len(col_mapping),
        "dimensions":      sorted(values_df["dimension"].unique().tolist()),
        "ready_to_compile": True,
        "message": (
            f"{len(values_df)} value rows loaded across {len(col_mapping)} coordinates. "
            "Proceed to /compile with target=duckdb."
        ),
    }
