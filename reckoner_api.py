"""
reckoner_api.py — Reckoner Python API Server

FastAPI backend for Reckoner. Replaces the JS backend.
Powered by snf-peirce. Substrate-neutral by construction.

The server knows nothing about domains. It routes coordinates
and returns them. The frontend decides what to display.

Usage:
    pip install fastapi uvicorn snf-peirce
    python reckoner_api.py

    Or with auto-reload for development:
    uvicorn reckoner_api:app --reload --port 8000

Endpoints:
    GET  /api/health
    GET  /api/schemas                    — list loaded substrates
    GET  /api/affordances?schema=        — fields and values per dimension
    GET  /api/values/{dim}/{field}       — values for a specific field
    POST /api/query                      — execute Peirce query
    POST /api/discover                   — execute discovery expression

Frontend compatibility:
    Matches the existing Reckoner frontend API contract exactly.
    Accepts both Peirce strings and legacy constraint arrays.
    Returns coordinates instead of schema-specific display objects —
    the frontend builds display from coordinates, not from the server.

Dependencies:
    pip install fastapi uvicorn snf-peirce pandas
"""

from __future__ import annotations

import os
import time
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# Load .env file if present — must happen before any os.environ.get() calls
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ─────────────────────────────────────────────────────────────────────────────
# FastAPI
# ─────────────────────────────────────────────────────────────────────────────

try:
    from fastapi import FastAPI, HTTPException, Query
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel
    import uvicorn
except ImportError:
    raise ImportError(
        "FastAPI and uvicorn required. Install with:\n"
        "  pip install fastapi uvicorn"
    )

# ─────────────────────────────────────────────────────────────────────────────
# snf-peirce
# ─────────────────────────────────────────────────────────────────────────────

from snf_peirce import compile_data, query as peirce_query, discover, load
from snf_peirce.compile import Substrate
from snf_peirce.parser import parse_to_constraints
from snf_peirce.peirce import PeirceParseError, PeirceDiscoveryError
from snf_peirce.srf import SRFRecord, SRFValidationError
import duckdb

# Force reload to ensure latest peirce.py is used
import importlib
import snf_peirce.peirce
importlib.reload(snf_peirce.peirce)
from snf_peirce.peirce import PeirceParseError, PeirceDiscoveryError

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

PORT            = int(os.environ.get("PORT", 8000))
SUBSTRATES_DIR  = os.environ.get("SNF_SUBSTRATES_DIR", "./substrates")
SRF_IMPORTS_DIR = os.environ.get("SNF_SRF_IMPORTS_DIR", os.path.join(SUBSTRATES_DIR, "srf_imports"))
DEBUG           = os.environ.get("DEBUG", "false").lower() == "true"

# ─────────────────────────────────────────────────────────────────────────────
# Query hash
# ─────────────────────────────────────────────────────────────────────────────

import hashlib

def compute_query_hash(
    substrate_id:       str,
    lens_id:            str,
    translator_version: str,
    constraints:        list,
) -> str:
    """
    Compute the canonical query hash per Result Set Identity Model v1.2.

    Canonical ordering rule (sort key, ascending, lexicographic):
        1. dimension  (WHO, WHAT, WHEN, WHERE, WHY, HOW)
        2. field      (semantic_key)
        3. operator   (eq, not_eq, gt, lt, between, only)
        4. value      (string representation, UTF-8, lowercased)

    Serialized as a JSON array with keys in alphabetical order.
    Hashed: SHA-256(substrate_id + lens_id + translator_version + canonical_json)

    Returns hex digest string.
    """
    def sort_key(c):
        return (
            str(c.get("dimension", "") or c.get("category", "")).upper(),
            str(c.get("field", "")).lower(),
            str(c.get("op", "eq")).lower(),
            str(c.get("value", "")).lower(),
        )

    sorted_constraints = sorted(constraints, key=sort_key)

    # Serialize each constraint with keys in alphabetical order
    canonical = json.dumps(
        [
            {
                "dimension": str(c.get("dimension", "") or c.get("category", "")).upper(),
                "field":     str(c.get("field", "")).lower(),
                "op":        str(c.get("op", "eq")).lower(),
                "value":     str(c.get("value", "")).lower(),
            }
            for c in sorted_constraints
        ],
        separators=(",", ":"),
        sort_keys=True,
    )

    payload = "|".join([
        str(substrate_id or ""),
        str(lens_id or ""),
        str(translator_version or ""),
        canonical,
    ])

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# Substrate registry
#
# Substrates are loaded from SUBSTRATES_DIR at startup.
# Each subdirectory that looks like a spoke directory is registered.
# Format: substrates/my_collection/snf_who.csv etc.
#
# You can also register substrates programmatically via the registry dict.
# ─────────────────────────────────────────────────────────────────────────────

_registry: Dict[str, Substrate] = {}
_registry_meta: Dict[str, dict] = {}

# ── Adapter registry (SubstrateAdapter-backed substrates) ────────────────────
# Holds PostgresAdapter and other non-snf-peirce adapters.
# Keyed by substrate_id, same namespace as _registry.
# The two registries are mutually exclusive — a name is in one or the other.
_adapter_registry: Dict[str, Any] = {}

# ── Affordances cache ─────────────────────────────────────────────────────────
# Affordances are static per substrate — computed once on first request,
# served from memory thereafter. Avoids repeated expensive aggregation
# queries on large substrates (especially Shape B coordinate-only schemas).
_affordances_cache: Dict[str, Any] = {}


def register_substrate(name: str, substrate: Substrate, meta: dict = None) -> None:
    """Register a compiled substrate under a name."""
    _registry[name] = substrate
    _registry_meta[name] = meta or {}
    print(f"[registry] Registered substrate: {name}")


def substrate_from_spoke_dir(subdir: Path) -> Substrate:
    """
    Load a substrate from a directory of spoke CSVs + lens.json.

    Expected layout:
        subdir/
            lens.json       — lens metadata (must contain lens_id)
            snf_who.csv     — spoke rows for WHO dimension
            snf_what.csv    — spoke rows for WHAT dimension
            ...

    Spoke CSV columns: entity_id, dimension, semantic_key, value, coordinate, lens_id

    Constructs a Substrate by loading all spoke CSVs into an in-memory DuckDB
    connection and creating the snf_spoke table that snf-peirce expects.
    """
    import pandas as pd

    # Read lens_id from lens.json
    lens_path = subdir / "lens.json"
    if lens_path.exists():
        with open(lens_path) as f:
            lens_data = json.load(f)
        lens_id = lens_data.get("lens_id", subdir.name)
    else:
        lens_id = subdir.name

    # Concatenate all spoke CSVs
    spoke_files = sorted(subdir.glob("snf_*.csv"))
    if not spoke_files:
        raise ValueError(f"No snf_*.csv files found in {subdir}")

    frames = []
    for sf in spoke_files:
        try:
            df = pd.read_csv(sf)
            frames.append(df)
        except Exception as e:
            print(f"[registry] Warning: could not read {sf.name}: {e}")

    if not frames:
        raise ValueError(f"No readable spoke CSVs in {subdir}")

    spokes = pd.concat(frames, ignore_index=True)

    # Ensure required columns exist
    required = {"entity_id", "dimension", "semantic_key", "value"}
    missing  = required - set(spokes.columns)
    if missing:
        raise ValueError(f"Spoke CSVs missing columns: {missing}")

    # Add coordinate and lens_id columns if absent
    if "coordinate" not in spokes.columns:
        spokes["coordinate"] = (
            spokes["dimension"].str.upper() + "|" +
            spokes["semantic_key"] + "|" +
            spokes["value"].astype(str)
        )
    if "lens_id" not in spokes.columns:
        spokes["lens_id"] = lens_id

    # Build in-memory DuckDB and create snf_spoke table
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE snf_spoke (
            entity_id    VARCHAR,
            dimension    VARCHAR,
            semantic_key VARCHAR,
            value        VARCHAR,
            coordinate   VARCHAR,
            lens_id      VARCHAR
        )
    """)
    conn.register("_spokes_df", spokes)
    conn.execute("""
        INSERT INTO snf_spoke
        SELECT
            CAST(entity_id    AS VARCHAR),
            CAST(dimension    AS VARCHAR),
            CAST(semantic_key AS VARCHAR),
            CAST(value        AS VARCHAR),
            CAST(coordinate   AS VARCHAR),
            CAST(lens_id      AS VARCHAR)
        FROM _spokes_df
    """)
    conn.unregister("_spokes_df")

    return Substrate(conn, lens_id, source_path=str(subdir))


def register_adapter(name: str, adapter, meta: dict = None) -> None:
    """Register a SubstrateAdapter under a name."""
    _adapter_registry[name] = adapter
    _registry_meta[name]    = meta or {}
    print(f"[registry] Registered adapter: {name}")


def load_postgres_adapters() -> None:
    """
    Load Postgres substrates from individual env vars.

    Simpler than JSON — one set of vars per substrate.
    Add to .env for each Postgres substrate you want to register:

        DATABASE_URL=postgresql://user:pass@localhost:5432/snf_bench

        # First substrate
        PG_1_NAME=legal
        PG_1_SCHEMA=legal
        PG_1_SUBSTRATE_ID=legal-prod
        PG_1_LENS_ID=legal-v1
        PG_1_TRANSLATOR_VERSION=1.0.0

        # Second substrate (optional)
        PG_2_NAME=dms
        PG_2_SCHEMA=dms
        PG_2_SUBSTRATE_ID=dms-prod
        PG_2_LENS_ID=dms-v1
        PG_2_TRANSLATOR_VERSION=1.0.0
    """
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return

    # Collect all PG_N_* configs
    configs = []
    for i in range(1, 10):
        prefix = f"PG_{i}_"
        name = os.environ.get(f"{prefix}NAME")
        if not name:
            break
        configs.append({
            "name":               name,
            "schema":             os.environ.get(f"{prefix}SCHEMA",             name),
            "substrate_id":       os.environ.get(f"{prefix}SUBSTRATE_ID",       f"{name}-prod"),
            "lens_id":            os.environ.get(f"{prefix}LENS_ID",            f"{name}-v1"),
            "translator_version": os.environ.get(f"{prefix}TRANSLATOR_VERSION", "1.0.0"),
        })

    if not configs:
        return

    try:
        import psycopg2
        from postgres_adapter import PostgresAdapter

        conn = psycopg2.connect(database_url)

        for cfg in configs:
            name   = cfg["name"]
            schema = cfg["schema"]
            try:
                adapter = PostgresAdapter.from_binding(
                    conn     = conn,
                    schema   = schema,
                    manifest = {
                        "substrate_id":       cfg["substrate_id"],
                        "lens_id":            cfg["lens_id"],
                        "translator_version": cfg["translator_version"],
                    }
                )
                meta = {
                    "label":        name,
                    "entity_count":  adapter.entity_count(),
                    "dimensions":    adapter.dimensions(),
                    "lens_id":       adapter.lens_id,
                    "backend":       "postgres",
                    "schema":        schema,
                }
                register_adapter(name, adapter, meta)
                print(f"[registry] Loaded postgres adapter: {name} "
                      f"({meta['entity_count']:,} entities, "
                      f"dims: {meta['dimensions']})")
            except Exception as e:
                print(f"[registry] Failed to load postgres adapter {name}: {e}")

    except ImportError:
        print("[registry] psycopg2 not installed — Postgres adapters not loaded")
    except Exception as e:
        print(f"[registry] Error loading Postgres adapters: {e}")


def load_substrates_from_disk() -> None:
    """
    Scan SUBSTRATES_DIR for spoke directories and .duckdb files and load them.

    Expected layouts:
        substrates/
            discogs/            ← spoke CSV directory (legacy)
                lens.json
                snf_who.csv
                snf_what.csv
                ...
            discogs.duckdb      ← model_builder.py output (preferred)
            disney.duckdb
    """
    base = Path(SUBSTRATES_DIR)
    if not base.exists():
        print(f"[registry] Substrates directory not found: {SUBSTRATES_DIR}")
        print(f"[registry] Create it and add spoke directories or .duckdb files to load substrates automatically.")
        return

    for entry in sorted(base.iterdir()):

        # ── .duckdb file — model_builder.py output ───────────────────────────
        if entry.is_file() and entry.suffix == ".duckdb":
            name = entry.stem
            try:
                conn      = duckdb.connect(str(entry), read_only=True)
                # Read lens_id from first row — stamped by model_builder
                row       = conn.execute("SELECT lens_id FROM snf_spoke LIMIT 1").fetchone()
                lens_id   = row[0] if row else name
                substrate = Substrate(conn, lens_id)
                meta = {
                    "path":         str(entry),
                    "entity_count": substrate.entity_count(),
                    "dimensions":   substrate.dimensions(),
                    "lens_id":      lens_id,
                    "label":        name,
                    "backend":      "duckdb_file",
                }
                register_substrate(name, substrate, meta)
                print(f"[registry] Loaded {name}: {meta['entity_count']:,} entities, "
                      f"dimensions: {meta['dimensions']} (duckdb)")
            except Exception as e:
                print(f"[registry] Failed to load {name}.duckdb: {e}")
            continue

        # ── spoke CSV directory — legacy format ───────────────────────────────
        if not entry.is_dir():
            continue

        spoke_files = list(entry.glob("snf_*.csv"))
        if not spoke_files:
            continue

        name = entry.name
        try:
            substrate = substrate_from_spoke_dir(entry)
            meta = {
                "path":         str(entry),
                "entity_count": substrate.entity_count(),
                "dimensions":   substrate.dimensions(),
                "lens_id":      substrate.lens_id,
                "label":        name,
            }
            register_substrate(name, substrate, meta)
            print(f"[registry] Loaded {name}: {meta['entity_count']:,} entities, "
                  f"dimensions: {meta['dimensions']}")
        except Exception as e:
            print(f"[registry] Failed to load {name}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Constraint → Peirce conversion
# (mirrors the toPeirce() function in the frontend)
# ─────────────────────────────────────────────────────────────────────────────

OP_TO_PEIRCE = {
    "eq": "=", "not_eq": "!=", "gt": ">", "lt": "<",
    "gte": ">=", "lte": "<=", "contains": "CONTAINS", "prefix": "PREFIX",
    "only": "ONLY",
}

def constraints_to_peirce(constraints: List[dict]) -> str:
    """Convert legacy constraint array to Peirce string."""
    parts = []
    for c in constraints:
        dim   = (c.get("category") or c.get("dimension") or "").upper()
        field = (c.get("field") or "").lower()
        op    = c.get("op", "eq")
        value = c.get("value", "")
        value2 = c.get("value2")

        if not dim or not field:
            continue

        def serialize(v):
            if isinstance(v, bool):
                return "true" if v else "false"
            if isinstance(v, (int, float)):
                return str(v)
            return f'"{str(v)}"'

        if op == "between" and value2 is not None:
            expr = f'{dim}.{field} BETWEEN {serialize(value)} AND {serialize(value2)}'
        else:
            peirce_op = OP_TO_PEIRCE.get(op, "=")
            expr = f'{dim}.{field} {peirce_op} {serialize(value)}'

        if c.get("negated"):
            expr = f"NOT {expr}"

        parts.append(expr)

    return " AND ".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Result hydration
#
# Takes entity IDs and returns coordinate objects.
# The server returns coordinates — the frontend decides what to display.
# No schema-specific logic here. Ever.
# ─────────────────────────────────────────────────────────────────────────────

def hydrate_results(
    entity_ids: List[str],
    substrate: Substrate,
    matched_coordinates: Dict[str, List[str]],
    fields: Optional[List[str]] = None,
) -> List[dict]:
    """
    For each entity ID, return its coordinates grouped by dimension.

    Args:
        entity_ids:           Matched entity IDs from routing
        substrate:            The substrate that was queried
        matched_coordinates:  Which coordinates triggered the match
                              {entity_id: [coordinate_string, ...]}
        fields:               Optional list of semantic_keys to include
                              e.g. ["artist", "released"]
                              None = include all fields

    Returns:
        List of result objects:
        {
            "id": "music:album:001",
            "coordinates": {
                "WHO":   [{"field": "artist", "value": "Miles Davis"}],
                "WHEN":  [{"field": "released", "value": "1959"}],
                ...
            },
            "matched_because": [
                {"dimension": "WHO", "field": "artist", "value": "Miles Davis", "coordinate": "WHO|artist|Miles Davis"}
            ]
        }
    """
    if not entity_ids:
        return []

    try:
        # Pull spoke rows for matched entities
        import pandas as pd
        placeholders = ", ".join("?" * len(entity_ids))
        rows = substrate._conn.execute(
            f"SELECT entity_id, dimension, semantic_key, value, coordinate "
            f"FROM snf_spoke "
            f"WHERE entity_id IN ({placeholders}) "
            f"AND lens_id = ? "
            f"ORDER BY entity_id, dimension, semantic_key",
            entity_ids + [substrate.lens_id]
        ).fetchall()

        # Group by entity_id
        by_entity: Dict[str, Dict[str, List[dict]]] = {}
        for entity_id in entity_ids:
            by_entity[entity_id] = {}

        for entity_id, dimension, semantic_key, value, coordinate in rows:
            # Apply field filter if specified
            if fields:
                # semantic_key is like "artist" — field name without dimension
                key_part = semantic_key.split(".")[-1] if "." in semantic_key else semantic_key
                if key_part not in fields and semantic_key not in fields:
                    continue

            dim_upper = dimension.upper()
            if dim_upper not in by_entity[entity_id]:
                by_entity[entity_id][dim_upper] = []

            # Extract field name from semantic_key
            field_name = semantic_key.split(".")[-1] if "." in semantic_key else semantic_key

            by_entity[entity_id][dim_upper].append({
                "field":      field_name,
                "value":      value,
                "coordinate": coordinate,
            })

        # Build result objects
        results = []
        for entity_id in entity_ids:
            # Build matched_because from coordinates that triggered the match
            matched = []
            for coord in matched_coordinates.get(entity_id, []):
                # Parse coordinate string: "WHO|artist|Miles Davis"
                parts = coord.split("|")
                if len(parts) >= 3:
                    matched.append({
                        "dimension":  parts[0],
                        "field":      parts[1],
                        "value":      "|".join(parts[2:]),
                        "coordinate": coord,
                        "matched":    True,
                    })
                else:
                    matched.append({
                        "coordinate": coord,
                        "matched":    True,
                    })

            results.append({
                "id":             entity_id,
                "coordinates":    by_entity.get(entity_id, {}),
                "matched_because": matched,
            })

        return results

    except Exception as e:
        if DEBUG:
            print(f"[hydrate] Error: {e}")
        # Fallback — return entity IDs with empty coordinates
        return [{"id": eid, "coordinates": {}, "matched_because": []} for eid in entity_ids]


def _duckdb_query_with_trace(
    substrate,
    peirce_string: str,
    limit: int = 100000,
):
    """
    Execute a Peirce query against a DuckDB substrate and return a real
    Portolan stepdown trace.

    Mirrors _route_coordinate_only in PostgresAdapter but operates against
    the single snf_spoke table that all DuckDB substrates use.

    Returns:
        (entity_ids: list[str], count: int, trace: list[dict])

    Trace shape matches PostgresAdapter — one entry per dimension in
    selectivity order:
        [
            {"dimension": "WHEN", "cardinality": 193,  "fields": [{"field": "year", "values": ["2015"]}]},
            {"dimension": "HOW",  "cardinality": 47,   "fields": [{"field": "citation_band", "values": ["high_20plus"]}]},
            {"dimension": "WHAT", "cardinality": 12,   "fields": [{"field": "status", "values": ["Published"]}]},
        ]

    Cardinality is the real running intersection count at each step — not the
    raw posting list size. This is the same value the stepdown trace panel
    displays as the "after this step" count.

    Falls back to peirce_query() (no trace) on any parse or execution error
    so existing error handling is preserved.
    """
    from snf_peirce.parser import parse_to_constraints

    conn     = substrate._conn
    lens_id  = substrate.lens_id

    # ── Parse ─────────────────────────────────────────────────────────────────
    parsed = parse_to_constraints(peirce_string)
    if not parsed.get("success"):
        raise PeirceParseError(
            error    = parsed.get("error", "Parse failed"),
            position = parsed.get("position", 0),
            token    = parsed.get("token"),
        )
    if parsed.get("type") == "discovery":
        raise PeirceDiscoveryError(
            scope     = parsed["scope"],
            dimension = parsed.get("dimension"),
            field     = parsed.get("field"),
        )

    # DNF: take first conjunct only for trace (OR across conjuncts falls back gracefully)
    conjuncts = parsed.get("conjuncts", [])
    if not conjuncts:
        return [], 0, []

    # For multi-conjunct (OR) queries we still want correct entity_ids — use
    # peirce_query for the actual result, then build a best-effort trace from
    # the first conjunct.  Single-conjunct (AND-only) queries get a full trace.
    is_dnf = len(conjuncts) > 1

    # ── Group constraints: { dim_upper → { field → [values] } } ──────────────
    def group_conjunct(conjunct):
        by_dim = {}
        for c in conjunct:
            if c.get("op") in ("contains", "prefix", "only", "between", "gt", "gte", "lt", "lte"):
                continue  # not routable as coordinate eq — range ops need special handling
            dim   = (c.get("category") or c.get("dimension") or "").upper()
            field = (c.get("field") or "").lower()
            value = str(c.get("value", ""))
            if not dim or not field:
                continue
            by_dim.setdefault(dim, {}).setdefault(field, []).append(value)
        return by_dim

    first_conjunct = conjuncts[0]
    by_dim = group_conjunct(first_conjunct)

    if not by_dim:
        # No routable constraints — fall back to peirce_query, no trace
        result = peirce_query(substrate, peirce_string, limit=limit)
        return result.entity_ids, result.count, []

    # ── Cardinality probe — one COUNT per dimension ───────────────────────────
    dim_counts = {}
    for dim, fields in by_dim.items():
        where_parts = []
        probe_params = [lens_id]
        for field, values in fields.items():
            phs = ", ".join(["?" for _ in values])
            where_parts.append(
                f"(dimension = ? AND semantic_key = ? AND value IN ({phs}))"
            )
            probe_params += [dim.lower(), field] + values
        where_sql = " OR ".join(where_parts)
        row = conn.execute(
            f"SELECT COUNT(DISTINCT entity_id) FROM snf_spoke "
            f"WHERE lens_id = ? AND ({where_sql})",
            probe_params
        ).fetchone()
        dim_counts[dim] = row[0] if row else 0

    # ── I1 ordering — ascending cardinality ───────────────────────────────────
    ordered_dims = sorted(by_dim.keys(), key=lambda d: dim_counts[d])

    # ── Stepdown probe — running intersection count at each step ─────────────
    # Step 1: anchor cardinality (already in dim_counts)
    # Step N: COUNT of entities matching first N dimensions intersected
    stepdown_counts = []

    if len(ordered_dims) == 1:
        stepdown_counts.append(dim_counts[ordered_dims[0]])
    else:
        for step_n in range(1, len(ordered_dims) + 1):
            dims_so_far = ordered_dims[:step_n]

            # Build an EXISTS-style intersection:
            # SELECT COUNT(DISTINCT entity_id) FROM snf_spoke
            # WHERE lens_id=? AND dim=? AND field IN (...)   -- anchor
            # AND entity_id IN (
            #   SELECT entity_id FROM snf_spoke WHERE lens_id=? AND dim=? ...
            # ) ...

            # Anchor: first dim
            anchor_dim    = dims_so_far[0]
            anchor_fields = by_dim[anchor_dim]
            anchor_parts  = []
            anchor_params = [lens_id, anchor_dim.lower()]
            for field, values in anchor_fields.items():
                phs = ", ".join(["?" for _ in values])
                anchor_parts.append(f"(semantic_key = ? AND value IN ({phs}))")
                anchor_params += [field] + values
            anchor_where = " OR ".join(anchor_parts)

            # Build nested IN subqueries for each additional dim
            nested_sql    = ""
            nested_params = []
            for dim in dims_so_far[1:]:
                dim_fields  = by_dim[dim]
                dim_parts   = []
                dim_params  = [lens_id, dim.lower()]
                for field, values in dim_fields.items():
                    phs = ", ".join(["?" for _ in values])
                    dim_parts.append(f"(semantic_key = ? AND value IN ({phs}))")
                    dim_params += [field] + values
                dim_where     = " OR ".join(dim_parts)
                nested_sql   += (
                    f" AND entity_id IN ("
                    f"SELECT entity_id FROM snf_spoke "
                    f"WHERE lens_id = ? AND dimension = ? AND ({dim_where}))"
                )
                nested_params += dim_params

            sql = (
                f"SELECT COUNT(DISTINCT entity_id) FROM snf_spoke "
                f"WHERE lens_id = ? AND dimension = ? AND ({anchor_where})"
                f"{nested_sql}"
            )
            all_params = anchor_params + nested_params
            row = conn.execute(sql, all_params).fetchone()
            stepdown_counts.append(row[0] if row else 0)

    # ── Execute query — use peirce_query for correct DNF / ONLY handling ──────
    result = peirce_query(substrate, peirce_string, limit=limit)

    # ── Build trace ───────────────────────────────────────────────────────────
    trace = [
        {
            "dimension":   dim,
            "cardinality": stepdown_counts[i],
            "fields": [
                {"field": f, "values": list(v)}
                for f, v in by_dim[dim].items()
            ],
        }
        for i, dim in enumerate(ordered_dims)
    ]

    return result.entity_ids, result.count, trace


def _resolve_substrate(substrate_id: str):
    """
    Resolve a substrate_id to either a Substrate or a SubstrateAdapter.
    Returns (substrate_or_adapter, is_adapter).
    Raises HTTPException if not found.
    """
    if substrate_id:
        if substrate_id in _registry:
            return _registry[substrate_id], False
        if substrate_id in _adapter_registry:
            return _adapter_registry[substrate_id], True

    # Fall back to first available
    if _registry:
        name = list(_registry.keys())[0]
        return _registry[name], False
    if _adapter_registry:
        name = list(_adapter_registry.keys())[0]
        return _adapter_registry[name], True

    raise HTTPException(status_code=404, detail="No substrates loaded")


def extract_matched_coordinates(
    peirce_string: str,
    entity_ids: List[str],
) -> Dict[str, List[str]]:
    """
    Build matched_because from the Peirce query string.
    All matched entities matched on the same coordinates.
    """
    try:
        parsed = parse_to_constraints(peirce_string)
        if not parsed.get("success") or parsed.get("type") == "discovery":
            return {}

        coords = []
        for conjunct in parsed.get("conjuncts", []):
            for c in conjunct:
                dim   = (c.get("category") or c.get("dimension") or "").upper()
                field = (c.get("field") or "").lower()
                value = c.get("value", "")
                if dim and field:
                    coords.append(f"{dim}|{field}|{value}")

        return {eid: coords for eid in entity_ids}

    except Exception:
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI app
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Reckoner API",
    description="SNF semantic query API. Powered by snf-peirce.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # Tighten for production
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Model Builder router ──────────────────────────────────────────────────────
# Mounts at /api/mb — upload, introspect, review, compile, download.
# model_builder_api.py must be in the same directory as this file.
try:
    from model_builder_api import router as mb_router
    app.include_router(mb_router)
    print("[api] Model Builder endpoints loaded at /api/mb")
except ImportError:
    print("[api] WARNING: model_builder_api.py not found — /api/mb endpoints unavailable.")
    print("[api]          Place model_builder_api.py alongside reckoner_api.py to enable.")

# Load substrates at startup
@app.on_event("startup")
async def startup():
    load_substrates_from_disk()
    load_postgres_adapters()
    load_srf_imports()
    if not _registry and not _adapter_registry:
        print("[api] No substrates loaded. Add spoke directories to:", SUBSTRATES_DIR)
        print("[api] Or set PG_SUBSTRATES to load Postgres adapters.")


# ─────────────────────────────────────────────────────────────────────────────
# Request / Response models
# ─────────────────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    peirce:      Optional[str]       = None   # Preferred
    constraints: Optional[List[dict]] = None  # Legacy fallback
    schema:      Optional[str]       = None   # substrate_id
    substrate_id: Optional[str]      = None   # preferred name
    limit:       Optional[int]       = None
    offset:      Optional[int]       = None   # for pagination
    fields:      Optional[List[str]] = None   # field projection

class DiscoverRequest(BaseModel):
    expression:  str
    schema:      Optional[str] = None
    substrate_id: Optional[str] = None
    limit:       Optional[int] = None

class ConditionalDiscoverRequest(BaseModel):
    expression:   str
    entity_ids:   Optional[List[str]] = None   # post-query path: explicit entity IDs
    constraints:  Optional[List[dict]] = None  # pre-query path: resolve constraints first (I18)
    schema:       Optional[str] = None
    substrate_id: Optional[str] = None
    limit:        Optional[int] = None


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health(schema: str = None):
    """Health check. Returns API status and substrate stats."""
    substrate_id = schema
    meta         = _registry_meta.get(substrate_id, {}) if substrate_id else {}

    # Resolve substrate or adapter — adapters don't have a .count() method
    # but their entity_count is stored in _registry_meta at registration time
    substrate    = _registry.get(substrate_id) if substrate_id else None
    adapter      = _adapter_registry.get(substrate_id) if substrate_id else None
    found        = substrate or adapter

    # total_facts: use substrate.count() for DuckDB, entity_count from meta for adapters
    if substrate:
        total_facts = substrate.count()
    elif adapter and hasattr(adapter, 'count'):
        total_facts = adapter.count()
    else:
        total_facts = meta.get("entity_count", 0)

    return {
        "status":     "ok",
        "version":    "1.0.0",
        "substrate":  substrate_id,
        "statistics": {
            "total_entities": meta.get("entity_count", 0),
            "total_facts":    total_facts,
            "dimensions":     meta.get("dimensions", []),
        } if found else None,
        "substrates_loaded": list({**_registry, **_adapter_registry}.keys()),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


@app.get("/api/schemas")
async def schemas():
    """List all loaded substrates."""
    return {
        "schemas": [
            {
                "schema":       name,
                "label":        meta.get("label", name),
                "entity_count": meta.get("entity_count", 0),
                "dimensions":   meta.get("dimensions", []),
                "lens_id":      meta.get("lens_id", ""),
            }
            for name, meta in _registry_meta.items()
        ]
    }


@app.get("/api/affordances")
async def affordances(schema: str = None):
    """
    Return field metadata per dimension for the chip-building UI.

    Response shape matches existing Reckoner frontend expectation:
    {
        "WHO": {
            "artist": { "fact_count": 833, "distinct_values": 312, "value_type": "text" }
        },
        ...
    }
    """
    substrate_or_adapter, is_adapter = _resolve_substrate(schema)
    substrate_id = schema or (list(_registry.keys())[0] if _registry else list(_adapter_registry.keys())[0])

    # Serve from cache if available — affordances are static per substrate
    if substrate_id in _affordances_cache:
        return _affordances_cache[substrate_id]

    try:
        if is_adapter:
            result = substrate_or_adapter.affordances()
            _affordances_cache[substrate_id] = result
            return result
        else:
            substrate = substrate_or_adapter
            result = {}
            dims   = substrate.dimensions()

            for dim in dims:
                dim_upper = dim.upper()
                result[dim_upper] = {}

                rows = substrate._conn.execute(
                    "SELECT semantic_key, "
                    "COUNT(DISTINCT entity_id) as distinct_entities, "
                    "COUNT(*) as fact_count "
                    "FROM snf_spoke "
                    "WHERE dimension = ? AND lens_id = ? "
                    "GROUP BY semantic_key "
                    "ORDER BY fact_count DESC",
                    [dim, substrate.lens_id]
                ).fetchall()

                for semantic_key, distinct_entities, fact_count in rows:
                    field_name = semantic_key.split(".")[-1] if "." in semantic_key else semantic_key
                    if any(kw in field_name.lower() for kw in ["year", "date", "month", "day", "release", "activity"]):
                        value_type = "date"
                    elif any(kw in field_name.lower() for kw in ["count", "amount", "price", "cmc", "size"]):
                        value_type = "number"
                    elif distinct_entities <= 25:
                        value_type = "enum"
                    else:
                        value_type = "text"

                    result[dim_upper][field_name] = {
                        "fact_count":      fact_count,
                        "distinct_values": distinct_entities,
                        "value_type":      value_type,
                    }

            _affordances_cache[substrate_id] = result
            return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/values/{dimension}/{field}")
async def values(dimension: str, field: str, schema: str = None):
    """
    Return distinct values for a specific field.
    Matches existing frontend API contract.
    """
    substrate_or_adapter, is_adapter = _resolve_substrate(schema)
    substrate_id = schema or (list(_registry.keys())[0] if _registry else list(_adapter_registry.keys())[0])

    try:
        if is_adapter:
            rows_raw      = substrate_or_adapter.values(dimension, field)
            values_list   = [r["value"] for r in rows_raw]
            values_detail = rows_raw
        else:
            substrate = substrate_or_adapter
            rows = substrate._conn.execute(
                "SELECT value, COUNT(DISTINCT entity_id) as cnt "
                "FROM snf_spoke "
                "WHERE dimension = ? AND semantic_key = ? AND lens_id = ? "
                "GROUP BY value "
                "ORDER BY cnt DESC "
                "LIMIT 200",
                [dimension.lower(), field, substrate.lens_id]
            ).fetchall()
            values_list   = [row[0] for row in rows]
            values_detail = [{"value": row[0], "count": row[1]} for row in rows]

        return {
            "dimension": dimension.upper(),
            "field":     field,
            "schema":    substrate_id,
            "values":    values_list,
            "detail":    values_detail,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/query")
async def query(req: QueryRequest):
    """
    Execute a Peirce query against a substrate.

    Accepts:
        { peirce: "WHO.artist = \"Miles Davis\"", schema: "discogs" }
        { constraints: [...], schema: "discogs" }   ← legacy fallback

    Returns:
        {
            results: [{ id, coordinates, matched_because }],
            row_count, probe_ms, execution_ms, total_ms, trace, peirce
        }

    coordinates shape (Option B — substrate-neutral):
        {
            "WHO":  [{ "field": "artist", "value": "Miles Davis", "coordinate": "WHO|artist|Miles Davis" }],
            "WHEN": [{ "field": "released", "value": "1959", "coordinate": "WHO|released|1959" }]
        }
    """
    start = time.perf_counter()

    # Resolve substrate
    substrate_id         = req.substrate_id or req.schema
    substrate_or_adapter, is_adapter = _resolve_substrate(substrate_id)
    substrate_id = substrate_id or (
        list(_registry.keys())[0] if _registry else list(_adapter_registry.keys())[0]
    )

    # Resolve Peirce string
    peirce_string = req.peirce

    if not peirce_string and req.constraints:
        peirce_string = constraints_to_peirce(req.constraints)

    if not peirce_string:
        raise HTTPException(status_code=400, detail="peirce or constraints required")

    if DEBUG:
        print(f"[query] substrate={substrate_id} adapter={is_adapter} peirce={peirce_string!r}")

    # ── Adaptive limit ────────────────────────────────────────────────────────
    # If the caller sends an explicit limit, honour it.
    # If not, use a large number to get the full routing result first,
    # then apply a display cap based on the actual result count.
    # This ensures row_count always reflects the true total while hydration
    # is capped to a sensible page size.
    DISPLAY_CAP      = 200   # max entities to hydrate and send per request
    SMALL_RESULT_CAP = 500   # results under this are shown in full

    probe_start = time.perf_counter()

    if is_adapter:
        # ── Adapter path (PostgresAdapter etc.) ───────────────────────────────
        try:
            adapter = substrate_or_adapter
            # Route without limit to get true count, then cap hydration
            result  = adapter.query(peirce_string, limit=req.limit or 100000)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        probe_ms   = (time.perf_counter() - probe_start) * 1000
        exec_start = time.perf_counter()

        # Apply adaptive display cap and offset for pagination
        offset       = req.offset or 0
        display_cap  = req.limit if req.limit is not None else (
            result.count if result.count <= SMALL_RESULT_CAP else DISPLAY_CAP
        )
        page_ids     = result.entity_ids[offset : offset + display_cap]

        matched_coords = extract_matched_coordinates(peirce_string, page_ids)
        hydrated       = adapter.hydrate(
            entity_ids          = page_ids,
            matched_coordinates = matched_coords,
            semantic_keys       = req.fields,
        )

    else:
        # ── snf-peirce path (DuckDB Substrate) ────────────────────────────────
        substrate = substrate_or_adapter
        try:
            entity_ids, total_count, duckdb_trace = _duckdb_query_with_trace(
                substrate     = substrate,
                peirce_string = peirce_string,
                limit         = req.limit or 100000,
            )
        except PeirceParseError as e:
            raise HTTPException(status_code=400, detail={
                "error":    str(e),
                "position": e.position,
                "token":    e.token,
            })
        except PeirceDiscoveryError:
            raise HTTPException(status_code=400, detail={
                "error": "Discovery expression given to /query — use /discover instead",
            })
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        probe_ms   = (time.perf_counter() - probe_start) * 1000
        exec_start = time.perf_counter()

        # Wrap into a lightweight result-like object so the rest of the handler
        # can stay the same (result.count, result.entity_ids)
        class _DuckDBResult:
            def __init__(self, ids, count, trace):
                self.entity_ids = ids
                self.count      = count
                self.trace      = trace

        result = _DuckDBResult(entity_ids, total_count, duckdb_trace)

        # Apply adaptive display cap and offset for pagination
        offset      = req.offset or 0
        display_cap = req.limit if req.limit is not None else (
            result.count if result.count <= SMALL_RESULT_CAP else DISPLAY_CAP
        )
        page_ids    = result.entity_ids[offset : offset + display_cap]

        matched_coords = extract_matched_coordinates(peirce_string, page_ids)
        hydrated       = hydrate_results(
            entity_ids          = page_ids,
            substrate           = substrate,
            matched_coordinates = matched_coords,
            fields              = req.fields,
        )

    execution_ms = (time.perf_counter() - exec_start) * 1000
    total_ms     = (time.perf_counter() - start) * 1000

    # Build QueryIdentity fields from adapter/substrate provenance
    if is_adapter:
        prov               = substrate_or_adapter.provenance()
        qi_lens_id         = prov.lens_id
        qi_translator_ver  = prov.extra.get("translator_version", "")
        qi_substrate_id    = prov.extra.get("substrate_id", substrate_id)
    else:
        substrate = substrate_or_adapter
        qi_lens_id         = getattr(substrate, "lens_id", "")
        qi_translator_ver  = ""
        qi_substrate_id    = substrate_id

    # Canonical constraint list for hashing — use parsed constraints from Peirce
    constraint_list = req.constraints or []
    if not constraint_list and peirce_string:
        # Re-parse the Peirce string into constraint dicts for hashing
        try:
            parsed = parse_to_constraints(peirce_string)
            if parsed.get("success"):
                constraint_list = [
                    c for conjunct in parsed.get("conjuncts", []) for c in conjunct
                ]
        except Exception:
            constraint_list = []

    qi_hash = compute_query_hash(
        substrate_id       = qi_substrate_id,
        lens_id            = qi_lens_id,
        translator_version = qi_translator_ver,
        constraints        = constraint_list,
    )

    executed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    return {
        "success":            True,
        "results":            hydrated,
        "row_count":          result.count,
        "page_offset":        req.offset or 0,
        "page_size":          len(hydrated),
        "peirce":             peirce_string,
        "substrate":          substrate_id,
        "probe_ms":           round(probe_ms, 2),
        "execution_ms":       round(execution_ms, 2),
        "total_ms":           round(total_ms, 2),
        "trace":              getattr(result, 'trace', []),
        "portolan_order":     [t["dimension"] for t in getattr(result, 'trace', [])],
        "query_identity": {
            "substrate_id":       qi_substrate_id,
            "lens_id":            qi_lens_id,
            "translator_version": qi_translator_ver,
            "query_hash":         qi_hash,
            "executed_at":        executed_at,
        },
    }


@app.post("/api/discover")
async def discover_endpoint(req: DiscoverRequest):
    """
    Execute a Peirce discovery expression.

    Expressions:
        *              — all dimensions with fact counts
        WHO|*          — all fields in WHO
        WHO|artist|*   — all values for WHO.artist
    """
    substrate_id         = req.substrate_id or req.schema
    substrate_or_adapter, is_adapter = _resolve_substrate(substrate_id)
    substrate_id = substrate_id or (
        list(_registry.keys())[0] if _registry else list(_adapter_registry.keys())[0]
    )

    try:
        if is_adapter:
            result = substrate_or_adapter.discover(req.expression, limit=req.limit)
        else:
            result = discover(substrate_or_adapter, req.expression, limit=req.limit)

        return {
            "scope":     result.scope,
            "dimension": result.dimension,
            "field":     result.field,
            "rows":      result.rows,
            "substrate": substrate_id,
        }
    except PeirceParseError as e:
        raise HTTPException(status_code=400, detail={"error": str(e)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Hydrate — turn entity IDs into display objects, no routing
# ─────────────────────────────────────────────────────────────────────────────

class HydrateRequest(BaseModel):
    entity_ids:  List[str]
    schema:      Optional[str]       = None
    substrate_id: Optional[str]      = None
    fields:      Optional[List[str]] = None  # field projection

@app.post("/api/hydrate")
async def hydrate(req: HydrateRequest):
    """
    Hydrate a list of entity IDs into display objects.

    Takes entity IDs directly — no routing, no Peirce parsing.
    Used by diff panel to inspect a derived group of entities
    without pretending they came from a single query.

    Returns the same result shape as /api/query so the frontend
    can render them with the same ResultCard components.
    """
    if not req.entity_ids:
        raise HTTPException(status_code=400, detail="entity_ids must not be empty")

    substrate_id = req.substrate_id or req.schema
    substrate_or_adapter, is_adapter = _resolve_substrate(substrate_id)
    substrate_id = substrate_id or (
        list(_registry.keys())[0] if _registry else list(_adapter_registry.keys())[0]
    )

    try:
        # No matched_coordinates — these entities weren't matched by a query
        empty_matched: Dict[str, List[str]] = {eid: [] for eid in req.entity_ids}

        if is_adapter:
            hydrated = substrate_or_adapter.hydrate(
                entity_ids           = req.entity_ids,
                matched_coordinates  = empty_matched,
                semantic_keys        = req.fields,
            )
        else:
            hydrated = hydrate_results(
                entity_ids           = req.entity_ids,
                substrate            = substrate_or_adapter,
                matched_coordinates  = empty_matched,
                fields               = req.fields,
            )

        return {
            "results":    hydrated,
            "row_count":  len(hydrated),
            "substrate":  substrate_id,
            "hydrated_from": "entity_ids",  # signals to frontend this came from diff
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Aggregate by count — find entities where a field has N occurrences
# ─────────────────────────────────────────────────────────────────────────────

class AggregateRequest(BaseModel):
    dimension:   str
    field:       str
    schema:      Optional[str]  = None
    substrate_id: Optional[str] = None
    count_min:   Optional[int]  = None   # inclusive lower bound on value count
    count_max:   Optional[int]  = None   # inclusive upper bound on value count
    search_term: Optional[str]  = None   # CONTAINS filter on value — bypasses count filtering
    limit:       Optional[int]  = None   # max entity_ids to return

@app.post("/api/aggregate")
async def aggregate_by_count(req: AggregateRequest):
    """
    Return entity_ids for entities where a field's value count falls
    within [count_min, count_max].

    Example: dimension=WHO, field=artist, count_min=1, count_max=1
    Returns all entities where the artist field has exactly 1 record
    in the substrate — i.e. artists you only have one record for.

    This is a server-side GROUP BY HAVING operation. It avoids the
    need to add hundreds of individual OR constraints for count-based
    selection.
    """
    substrate_id = req.substrate_id or req.schema
    substrate_or_adapter, is_adapter = _resolve_substrate(substrate_id)
    substrate_id = substrate_id or (
        list(_registry.keys())[0] if _registry else list(_adapter_registry.keys())[0]
    )

    try:
        limit = req.limit or 10000

        if is_adapter:
            # CONTAINS path — search_term bypasses count filtering
            if req.search_term:
                peirce = f'{req.dimension}.{req.field} CONTAINS "{req.search_term}"'
                try:
                    result = substrate_or_adapter.query(peirce, limit=req.limit or 10000)
                    return {
                        "entity_ids":      result.entity_ids,
                        "count":           result.count,
                        "matching_values": None,
                        "substrate":       substrate_id,
                        "dimension":       req.dimension,
                        "field":           req.field,
                        "search_term":     req.search_term,
                    }
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))

            # Count filter path
            rows = substrate_or_adapter.values(
                dimension = req.dimension,
                field     = req.field,
                limit     = 100000,  # get all values
            )
            # Filter to values whose count is in range
            matching_values = []
            for row in rows:
                count = row.get("count", 0)
                if req.count_min is not None and count < req.count_min:
                    continue
                if req.count_max is not None and count > req.count_max:
                    continue
                matching_values.append(row["value"])

            if not matching_values:
                return {
                    "entity_ids": [],
                    "count": 0,
                    "matching_values": 0,
                    "substrate": substrate_id,
                }

            # Build a Peirce OR query for matching values and route
            # Batch into chunks to avoid massive queries
            BATCH = 50
            all_entity_ids = []
            seen = set()
            for i in range(0, len(matching_values), BATCH):
                batch = matching_values[i:i+BATCH]
                or_clauses = " OR ".join(
                    f'{req.dimension}.{req.field} = "{v}"' for v in batch
                )
                try:
                    result = substrate_or_adapter.query(or_clauses, limit=limit)
                    for eid in result.entity_ids:
                        if eid not in seen:
                            seen.add(eid)
                            all_entity_ids.append(eid)
                except Exception:
                    continue

            return {
                "entity_ids":      all_entity_ids[:limit],
                "count":           len(all_entity_ids),
                "matching_values": len(matching_values),
                "substrate":       substrate_id,
                "dimension":       req.dimension,
                "field":           req.field,
                "count_min":       req.count_min,
                "count_max":       req.count_max,
            }

        else:
            # DuckDB path
            substrate = substrate_or_adapter

            # CONTAINS path — use DuckDB ILIKE directly for case-insensitive search
            if req.search_term:
                try:
                    pattern = f"%{req.search_term}%"
                    rows = substrate._conn.execute(
                        "SELECT DISTINCT entity_id FROM snf_spoke "
                        "WHERE dimension = ? AND semantic_key = ? AND lens_id = ? "
                        "AND value ILIKE ? LIMIT ?",
                        [req.dimension.lower(), req.field, substrate.lens_id,
                         pattern, req.limit or 10000]
                    ).fetchall()
                    entity_ids = [row[0] for row in rows]
                    return {
                        "entity_ids":      entity_ids,
                        "count":           len(entity_ids),
                        "matching_values": None,
                        "substrate":       substrate_id,
                        "dimension":       req.dimension,
                        "field":           req.field,
                        "search_term":     req.search_term,
                    }
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))

            # Get value counts from snf_spoke
            rows = substrate._conn.execute(
                "SELECT value, COUNT(DISTINCT entity_id) as cnt "
                "FROM snf_spoke "
                "WHERE dimension = ? AND semantic_key = ? AND lens_id = ? "
                "GROUP BY value ORDER BY cnt DESC",
                [req.dimension.lower(), req.field, substrate.lens_id]
            ).fetchall()

            matching_values = []
            for value, cnt in rows:
                if req.count_min is not None and cnt < req.count_min:
                    continue
                if req.count_max is not None and cnt > req.count_max:
                    continue
                matching_values.append(value)

            if not matching_values:
                return {
                    "entity_ids": [],
                    "count": 0,
                    "matching_values": 0,
                    "substrate": substrate_id,
                }

            # Get entity_ids for matching values
            placeholders = ", ".join(["?" for _ in matching_values])
            eid_rows = substrate._conn.execute(
                f"SELECT DISTINCT entity_id FROM snf_spoke "
                f"WHERE dimension = ? AND semantic_key = ? AND lens_id = ? "
                f"AND value IN ({placeholders}) "
                f"LIMIT ?",
                [req.dimension.lower(), req.field, substrate.lens_id] + matching_values + [limit]
            ).fetchall()

            entity_ids = [row[0] for row in eid_rows]

            return {
                "entity_ids":      entity_ids,
                "count":           len(entity_ids),
                "matching_values": len(matching_values),
                "substrate":       substrate_id,
                "dimension":       req.dimension,
                "field":           req.field,
                "count_min":       req.count_min,
                "count_max":       req.count_max,
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Conditional discover — values filtered to a known result set
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/api/discover/conditional")
async def discover_conditional(req: ConditionalDiscoverRequest):
    """
    Like /api/discover but counts are filtered to a supplied set of entities.

    Two input paths — same response shape from both:

    Post-query path (existing): supply entity_ids directly.
        Used by the trie when a query has already run and results are in hand.

    Pre-query path (I18 narrow mode): supply constraints instead.
        Constraints are resolved to entity_ids first via the normal query path,
        then the conditional discover runs against that resolved set.
        Used by the trie when constraints are active but no query has run yet.

    Expression format: DIM|field|*  (only value-level discovery is supported)
    """
    if not req.entity_ids and not req.constraints:
        raise HTTPException(
            status_code=400,
            detail="Either entity_ids or constraints must be provided"
        )

    # Parse expression — only DIM|field|* is supported here
    parts = req.expression.strip().split("|")
    if len(parts) != 3 or parts[2] != "*":
        raise HTTPException(
            status_code=400,
            detail="Conditional discover only supports DIM|field|* expressions"
        )
    dimension = parts[0].upper()
    field     = parts[1]

    substrate_id = req.substrate_id or req.schema
    substrate_or_adapter, is_adapter = _resolve_substrate(substrate_id)
    substrate_id = substrate_id or (
        list(_registry.keys())[0] if _registry else list(_adapter_registry.keys())[0]
    )

    try:
        # ── Resolve entity_ids from constraints if not supplied directly ──────
        # Pre-query narrow path: run constraints through normal query execution,
        # extract entity_ids, then proceed identically to the post-query path.
        if req.entity_ids:
            entity_ids = req.entity_ids
        else:
            peirce_string = constraints_to_peirce(req.constraints)
            if not peirce_string:
                raise HTTPException(status_code=400, detail="Could not build Peirce string from constraints")

            if is_adapter:
                result = substrate_or_adapter.query(peirce_string, limit=None)
                entity_ids = result.entity_ids if result else []
            else:
                result = peirce_query(substrate_or_adapter, peirce_string, limit=None)
                entity_ids = result.entity_ids if result else []

            if not entity_ids:
                # No entities match current constraints — return empty rows
                return {
                    "scope":        "values",
                    "dimension":    dimension,
                    "field":        field,
                    "rows":         [],
                    "substrate":    substrate_id,
                    "conditional":  True,
                    "entity_count": 0,
                    "narrow_mode":  True,
                }

        # ── Conditional discover against resolved entity_ids ─────────────────
        limit = req.limit or 200

        if is_adapter:
            rows = substrate_or_adapter.values_conditional(
                dimension=dimension,
                field=field,
                entity_ids=entity_ids,
                limit=limit,
            )
        else:
            substrate    = substrate_or_adapter
            placeholders = ", ".join(["?" for _ in entity_ids])
            db_rows = substrate._conn.execute(
                f"SELECT value, COUNT(DISTINCT entity_id) AS cnt "
                f"FROM snf_spoke "
                f"WHERE dimension = ? AND semantic_key = ? AND lens_id = ? "
                f"AND entity_id IN ({placeholders}) "
                f"GROUP BY value ORDER BY cnt DESC LIMIT ?",
                [dimension.lower(), field, substrate.lens_id] + entity_ids + [limit]
            ).fetchall()
            rows = [{"value": row[0], "count": row[1]} for row in db_rows]

        return {
            "scope":        "values",
            "dimension":    dimension,
            "field":        field,
            "rows":         rows,
            "substrate":    substrate_id,
            "conditional":  True,
            "entity_count": len(entity_ids),
            "narrow_mode":  req.constraints is not None,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))






# ─────────────────────────────────────────────────────────────────────────────
# Parquet export
# ─────────────────────────────────────────────────────────────────────────────

class ParquetExportRequest(BaseModel):
    peirce:       Optional[str]        = None
    constraints:  Optional[List[dict]] = None
    entity_ids:   Optional[List[str]]  = None   # selection — bypasses query when provided
    schema:       Optional[str]        = None
    substrate_id: Optional[str]        = None
    fields:       Optional[List[str]]  = None   # projection
    sort_field:   Optional[str]        = None
    sort_dir:     Optional[str]        = "asc"


@app.post("/api/export/parquet")
async def export_parquet(req: ParquetExportRequest):
    """
    Execute a query and return results as a Parquet file.

    DuckDB writes directly to Parquet — no intermediate conversion.
    Projection and sort are applied before writing.
    The file is streamed back as application/octet-stream.

    If entity_ids is provided, skips query execution and exports those
    entities directly — used by the row-level selection feature (27b).
    """
    from fastapi.responses import Response
    import tempfile, os

    substrate_id = req.substrate_id or req.schema
    substrate    = _registry.get(substrate_id)

    if not substrate:
        if _registry:
            substrate_id = list(_registry.keys())[0]
            substrate    = _registry[substrate_id]
        else:
            raise HTTPException(status_code=404, detail="No substrates loaded")

    # Resolve entity IDs — either from explicit selection or by running the query
    if req.entity_ids:
        entity_ids = req.entity_ids
    else:
        # Build Peirce string
        if req.peirce:
            peirce_string = req.peirce
        elif req.constraints:
            peirce_string = constraints_to_peirce(req.constraints)
        else:
            raise HTTPException(status_code=400, detail="peirce, constraints, or entity_ids required")

        try:
            result = peirce_query(substrate, peirce_string, limit=None)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

        entity_ids = result.entity_ids

    if not entity_ids:
        raise HTTPException(status_code=204, detail="No results to export")

    # Build flat table from coordinates
    # One row per entity, one column per dimension_field.
    rows = []
    placeholders = ", ".join("?" * len(entity_ids))
    spoke_rows = substrate._conn.execute(
        f"SELECT entity_id, dimension, semantic_key, value "
        f"FROM snf_spoke "
        f"WHERE entity_id IN ({placeholders}) AND lens_id = ? "
        f"ORDER BY entity_id, dimension, semantic_key",
        entity_ids + [substrate.lens_id]
    ).fetchall()

    # Group by entity
    from collections import defaultdict
    by_entity = defaultdict(dict)
    for entity_id, dimension, semantic_key, value in spoke_rows:
        field = semantic_key.split(".")[-1] if "." in semantic_key else semantic_key
        if req.fields and field not in req.fields:
            continue
        col = f"{dimension.lower()}_{field}"
        # Multi-value: join with '; '
        if col in by_entity[entity_id]:
            by_entity[entity_id][col] = f"{by_entity[entity_id][col]}; {value}"
        else:
            by_entity[entity_id][col] = value

    for eid in entity_ids:
        row = {"entity_id": eid}
        row.update(by_entity.get(eid, {}))
        rows.append(row)

    if not rows:
        raise HTTPException(status_code=204, detail="No rows after projection")

    # Sort if requested
    if req.sort_field:
        reverse = (req.sort_dir or "asc") == "desc"
        def sort_key(r):
            v = r.get(req.sort_field) or \
                next((r[k] for k in r if k.endswith(f"_{req.sort_field}")), "")
            try: return (0, float(v))
            except (ValueError, TypeError): return (1, str(v).lower())
        rows.sort(key=sort_key, reverse=reverse)

    # Write to Parquet via DuckDB
    import pandas as pd
    df = pd.DataFrame(rows)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        substrate._conn.execute(
            "COPY (SELECT * FROM df) TO ? (FORMAT PARQUET)",
            [tmp_path]
        )
        with open(tmp_path, "rb") as f:
            parquet_bytes = f.read()
    finally:
        os.unlink(tmp_path)

    return Response(
        content     = parquet_bytes,
        media_type  = "application/octet-stream",
        headers     = {"Content-Disposition": f"attachment; filename=reckoner_{substrate_id}.parquet"},
    )


# ─────────────────────────────────────────────────────────────────────────────
# SRF Import
#
# SRF records are never written into existing disk-backed substrates.
# Each lens_id gets its own in-memory substrate, created on first import
# and reused for subsequent imports of the same lens.
# These substrates appear in the registry alongside disk-backed ones and
# are queryable immediately after import.
# ─────────────────────────────────────────────────────────────────────────────

def _srf_import_path(entity_id: str, lens_id: str) -> Path:
    """
    Return the path where an SRF record should be persisted.
    e.g. substrates/srf_imports/fieldguild_v1/tmdb_film_550.srf
    """
    safe_entity_id = entity_id.replace(":", "_").replace("/", "_")
    lens_dir = Path(SRF_IMPORTS_DIR) / lens_id
    lens_dir.mkdir(parents=True, exist_ok=True)
    return lens_dir / f"{safe_entity_id}.srf"


def _persist_srf_record(record: SRFRecord) -> None:
    """Write an SRF record to disk for persistence across restarts."""
    import json
    path = _srf_import_path(record.entity_id, record.lens_id)
    path.write_text(json.dumps(record.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    if DEBUG:
        print(f"[import/srf] Persisted {record.entity_id} → {path}")


def load_srf_imports() -> None:
    """
    Scan SRF_IMPORTS_DIR for .srf files and replay them into in-memory substrates.
    Called at startup after disk substrates are loaded.

    Directory structure:
        substrates/srf_imports/
            fieldguild_v1/
                tmdb_film_550.srf
                tmdb_film_807.srf
            musicbrainz_v1/
                mb_recording_xxx.srf
    """
    import json
    base = Path(SRF_IMPORTS_DIR)
    if not base.exists():
        if DEBUG:
            print(f"[import/srf] No SRF imports directory found at {SRF_IMPORTS_DIR}")
        return

    total = 0
    errors = 0
    for srf_file in sorted(base.rglob("*.srf")):
        try:
            d = json.loads(srf_file.read_text(encoding="utf-8"))
            record = SRFRecord.from_dict(d)
            substrate = _get_or_create_srf_substrate(record.lens_id)

            # Skip if already loaded (shouldn't happen at startup but be safe)
            existing = substrate._conn.execute(
                "SELECT COUNT(*) FROM snf_spoke WHERE entity_id = ?",
                [record.entity_id]
            ).fetchone()[0]
            if existing > 0:
                continue

            rows = record.to_snf_rows()
            substrate._conn.executemany(
                "INSERT INTO snf_spoke "
                "(entity_id, dimension, semantic_key, value, coordinate, lens_id, translator_version) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (r["entity_id"], r["dimension"], r["semantic_key"],
                     r["value"], r["coordinate"], r["lens_id"], record.translator_version)
                    for r in rows["spoke_rows"]
                ]
            )
            _registry_meta[record.lens_id]["entity_count"]       = substrate.entity_count()
            _registry_meta[record.lens_id]["translator_version"] = record.translator_version
            total += 1
        except Exception as e:
            print(f"[import/srf] Failed to reload {srf_file}: {e}")
            errors += 1

    if total > 0 or errors > 0:
        print(f"[import/srf] Reloaded {total} SRF records ({errors} errors) from {SRF_IMPORTS_DIR}")


def _get_or_create_srf_substrate(lens_id: str) -> Substrate:
    """
    Return the in-memory SRF substrate for this lens_id, creating it if needed.
    SRF substrates are separate from disk-backed substrates and are always writable.
    """
    if lens_id in _registry:
        return _registry[lens_id]

    # Create a fresh in-memory DuckDB for this lens
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE snf_spoke (
            entity_id         VARCHAR,
            dimension         VARCHAR,
            semantic_key      VARCHAR,
            value             VARCHAR,
            coordinate        VARCHAR,
            lens_id           VARCHAR,
            translator_version VARCHAR
        )
    """)
    conn.execute("CREATE INDEX idx_spoke_coord ON snf_spoke(coordinate)")
    conn.execute("CREATE INDEX idx_spoke_eid   ON snf_spoke(entity_id)")
    conn.execute("CREATE INDEX idx_spoke_dim   ON snf_spoke(dimension, semantic_key)")

    substrate = Substrate(conn, lens_id)
    register_substrate(lens_id, substrate, meta={
        "label":              lens_id,
        "entity_count":       0,
        "dimensions":         [],
        "lens_id":            lens_id,
        "translator_version": "",
        "source":             "srf_import",
    })

    if DEBUG:
        print(f"[import/srf] Created new in-memory substrate for lens '{lens_id}'")

    return substrate


class SRFImportRequest(BaseModel):
    record: dict


@app.post("/api/import/srf")
async def import_srf(req: SRFImportRequest):
    """
    Accept an SRF record and write it into a per-lens in-memory substrate.

    The substrate is keyed by the record's lens_id and created automatically
    on first import. Existing disk-backed substrates are never touched.

    Request body:
        {
            "record": { ...srf record... }
        }

    Response:
        {
            "entity_id": "mb:recording:...",
            "spoke_rows_written": 5,
            "lens_id": "musicbrainz_v1",
            "translator_version": "1.0.0",
            "substrate": "musicbrainz_v1"
        }

    Errors:
        400 — SRF validation failed (field + reason in detail)
        409 — entity already exists in this substrate
    """
    # --- Validate SRF record ------------------------------------------------
    try:
        record = SRFRecord.from_dict(req.record)
    except SRFValidationError as e:
        raise HTTPException(
            status_code=400,
            detail={"error": "SRF validation failed", "field": e.field, "reason": e.reason}
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid SRF record: {e}")

    # --- Get or create the substrate for this lens --------------------------
    substrate = _get_or_create_srf_substrate(record.lens_id)

    # --- Check for duplicate ------------------------------------------------
    existing = substrate._conn.execute(
        "SELECT COUNT(*) FROM snf_spoke WHERE entity_id = ?",
        [record.entity_id]
    ).fetchone()[0]

    if existing > 0:
        raise HTTPException(
            status_code=409,
            detail=f"Entity '{record.entity_id}' already exists. "
                   f"Restart Reckoner to clear in-memory SRF substrates."
        )

    # --- Write spoke rows ---------------------------------------------------
    rows = record.to_snf_rows()
    spoke_rows = rows["spoke_rows"]

    if not spoke_rows:
        raise HTTPException(
            status_code=400,
            detail="SRF record produced no routable spoke rows (all facts are UNKNOWN dimension)"
        )

    substrate._conn.executemany(
        "INSERT INTO snf_spoke "
        "(entity_id, dimension, semantic_key, value, coordinate, lens_id, translator_version) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (
                r["entity_id"],
                r["dimension"],
                r["semantic_key"],
                r["value"],
                r["coordinate"],
                r["lens_id"],
                record.translator_version,
            )
            for r in spoke_rows
        ]
    )

    # --- Update registry meta -----------------------------------------------
    _registry_meta[record.lens_id]["entity_count"]       = substrate.entity_count()
    _registry_meta[record.lens_id]["dimensions"]         = substrate.dimensions()
    _registry_meta[record.lens_id]["translator_version"] = record.translator_version

    # --- Persist to disk ----------------------------------------------------
    _persist_srf_record(record)

    if DEBUG:
        print(
            f"[import/srf] {record.entity_id} → {record.lens_id} "
            f"({len(spoke_rows)} spoke rows)"
        )

    return {
        "entity_id":          record.entity_id,
        "spoke_rows_written": len(spoke_rows),
        "lens_id":            record.lens_id,
        "translator_version": record.translator_version,
        "substrate":          record.lens_id,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SRF Bulk Import
# ─────────────────────────────────────────────────────────────────────────────

class SRFBulkImportRequest(BaseModel):
    records: List[dict]


@app.post("/api/import/srf/bulk")
async def import_srf_bulk(req: SRFBulkImportRequest):
    """
    Accept a list of SRF records and write them into per-lens in-memory substrates.

    Each record is validated and imported independently. Failures are reported
    per-record and do not stop the rest of the import.

    Request body:
        {
            "records": [ {...srf record...}, {...srf record...}, ... ]
        }

    Response:
        {
            "imported":  5,
            "skipped":   1,
            "duplicate": 1,
            "total":     7,
            "results": [
                {"entity_id": "tmdb:film:550", "status": "ok", "spoke_rows_written": 17},
                {"entity_id": "tmdb:film:807", "status": "duplicate"},
                {"entity_id": "...",           "status": "error", "reason": "..."},
                ...
            ]
        }
    """
    if not req.records:
        raise HTTPException(status_code=400, detail="records array must not be empty")

    results = []
    imported  = 0
    skipped   = 0
    duplicate = 0

    for raw in req.records:
        # Validate
        try:
            record = SRFRecord.from_dict(raw)
        except SRFValidationError as e:
            results.append({
                "entity_id": raw.get("entity_id", "(unknown)"),
                "status":    "error",
                "reason":    f"{e.field}: {e.reason}",
            })
            skipped += 1
            continue
        except Exception as e:
            results.append({
                "entity_id": raw.get("entity_id", "(unknown)"),
                "status":    "error",
                "reason":    str(e),
            })
            skipped += 1
            continue

        # Get or create substrate
        substrate = _get_or_create_srf_substrate(record.lens_id)

        # Check duplicate
        existing = substrate._conn.execute(
            "SELECT COUNT(*) FROM snf_spoke WHERE entity_id = ?",
            [record.entity_id]
        ).fetchone()[0]

        if existing > 0:
            results.append({
                "entity_id": record.entity_id,
                "status":    "duplicate",
            })
            duplicate += 1
            continue

        # Write spoke rows
        rows       = record.to_snf_rows()
        spoke_rows = rows["spoke_rows"]

        if not spoke_rows:
            results.append({
                "entity_id": record.entity_id,
                "status":    "error",
                "reason":    "no routable spoke rows (all facts are UNKNOWN dimension)",
            })
            skipped += 1
            continue

        substrate._conn.executemany(
            "INSERT INTO snf_spoke "
            "(entity_id, dimension, semantic_key, value, coordinate, lens_id, translator_version) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    r["entity_id"],
                    r["dimension"],
                    r["semantic_key"],
                    r["value"],
                    r["coordinate"],
                    r["lens_id"],
                    record.translator_version,
                )
                for r in spoke_rows
            ]
        )

        # Update registry meta
        _registry_meta[record.lens_id]["entity_count"]       = substrate.entity_count()
        _registry_meta[record.lens_id]["dimensions"]         = substrate.dimensions()
        _registry_meta[record.lens_id]["translator_version"] = record.translator_version

        # Persist to disk
        _persist_srf_record(record)

        results.append({
            "entity_id":          record.entity_id,
            "status":             "ok",
            "spoke_rows_written": len(spoke_rows),
        })
        imported += 1

        if DEBUG:
            print(f"[import/srf/bulk] {record.entity_id} → {record.lens_id} ({len(spoke_rows)} rows)")

    return {
        "imported":  imported,
        "skipped":   skipped,
        "duplicate": duplicate,
        "total":     len(req.records),
        "results":   results,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Standalone entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Reckoner API — Python backend")
    print("  Model Builder endpoints: /api/mb/*")
    print("=" * 60)
    print(f"  Port:       {PORT}")
    print(f"  Substrates: {SUBSTRATES_DIR}")
    print(f"  Debug:      {DEBUG}")
    print(f"  Docs:       http://localhost:{PORT}/docs")
    print("=" * 60)

    uvicorn.run(
        "reckoner_api:app",
        host="0.0.0.0",
        port=PORT,
        reload=DEBUG,
        log_level="info" if DEBUG else "warning",
    )
