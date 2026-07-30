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
import zipfile
import io
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
    from fastapi import FastAPI, HTTPException, Query, UploadFile, File
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

# C-1 — the importlib.reload() of snf_peirce.peirce that used to sit here has
# been removed. It was a development convenience with a real cost: reloading a
# module creates a SECOND set of exception classes, so a PeirceParseError raised
# by the pre-reload module object is not caught by a handler that closed over
# the post-reload class. The parse error then escapes as a 500 instead of a
# useful message, intermittently, depending on import order.
#
# It also interacts badly with PyInstaller, which resolves modules at build
# time. If a dev-time reload is ever wanted again, put it behind an explicit
# flag rather than running it on every import.

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

# Shared with Model Builder — see model_builder/settings.py.
#
# SUBSTRATES_DIR now defaults to the directory Model Builder writes to, so a
# freshly built substrate is queryable with no copy step. It was previously
# "./substrates", resolved against whatever the working directory happened to
# be, which for a packaged application is unpredictable.
try:
    from model_builder.settings import (
        CORS_ORIGINS,
        DEBUG,
        EXPORT_DIR,
        HOST,
        PORT,
        REGISTRY_CACHE,
        SRF_IMPORTS_DIR,
        SUBSTRATES_DIR,
        describe as describe_settings,
    )
except ImportError as exc:
    raise SystemExit(
        "Could not import model_builder.settings.\n"
        f"  {exc}\n\n"
        "The model_builder package must be importable from the directory\n"
        "containing reckoner_api.py, and must contain settings.py and paths.py.\n"
        "Refusing to start rather than falling back to old defaults, which\n"
        "would silently write substrates where the API does not look."
    )

# Downstream code wraps these in Path(...) and os.path.join(...); both accept
# strings, so converting here means nothing else has to change.
SUBSTRATES_DIR  = str(SUBSTRATES_DIR)
SRF_IMPORTS_DIR = str(SRF_IMPORTS_DIR)
REGISTRY_CACHE  = str(REGISTRY_CACHE)

# ─────────────────────────────────────────────────────────────────────────────
# Query hash
# ─────────────────────────────────────────────────────────────────────────────

import hashlib

def _is_numeric(value: str) -> bool:
    """
    Return True if value parses as a finite float.
    Used for empirical value-type inference in affordances.
    Rejects 'nan', 'inf', '-inf' which float() accepts but are not useful data values.
    """
    try:
        f = float(value)
        import math
        return math.isfinite(f)
    except (ValueError, TypeError):
        return False

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

# ── Entity meta sidecar registry ─────────────────────────────────────────────
# Keyed by substrate_id. Loaded from snf_entity_meta.csv + display.json when
# present in a substrate directory. Powers Plover result card display —
# url, description, thumbnail, source_domain, provider, date.
# Absent for non-Plover substrates — that is normal and expected.
_entity_meta_store: Dict[str, Dict[str, dict]] = {}   # substrate_id → {entity_id → meta_row}
_display_contract: Dict[str, dict] = {}                # substrate_id → display.json contents

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

# ── Manifest registry (WS-3A) ─────────────────────────────────────────────────
# Keyed by lens_id. Loaded from <name>.manifest.json sidecar alongside each
# .duckdb at startup. Carries semantic metadata (mappings, structural_groups,
# stem_projections) for Portolan to access at query time.
# Absent for pre-WS-3A substrates — that is normal and expected.
_manifest_registry: Dict[str, dict] = {}



# ─────────────────────────────────────────────────────────────────────────────
# WS-3B — Stem projection / facet alias expansion
# ─────────────────────────────────────────────────────────────────────────────





def _resolve_alias_fields(field: str, lens_id: str) -> list | None:
    """If field is a facet alias, return the concrete fields it expands to. Returns None if not an alias."""
    try:
        manifest = _manifest_registry.get(lens_id)
        if not manifest:
            return None
        facet_aliases    = manifest.get("facet_aliases")    or []
        stem_projections = manifest.get("stem_projections") or []
        if not facet_aliases:
            return None
        stem_lookup = {sp["stem"]: sp["expands_to"] for sp in stem_projections}
        for fa in facet_aliases:
            if fa.get("name", "").lower() == field.lower():
                source_stem = fa.get("source_stem")
                if source_stem and source_stem in stem_lookup:
                    return stem_lookup[source_stem]
        return None
    except Exception:
        return None


def _execute_alias_clause(clause: dict, concrete_fields: list, substrate) -> set:
    """
    Execute one alias clause as posting-list set operations.
    Returns set of entity IDs.

    mode=any:  union all (value × concrete_field) posting lists
    mode=all:  for each value, union its slots → intersect across values
    """
    def serialize(v):
        if isinstance(v, bool): return "true" if v else "false"
        if isinstance(v, (int, float)): return str(v)
        return f'"{str(v)}"'

    dim          = (clause.get("dimension") or clause.get("category") or "").upper()
    mode         = clause.get("mode", "any")
    include_vals = clause.get("include") or []

    if not include_vals:
        return set()

    if mode == "any":
        exprs  = [f'{dim}.{cf} = {serialize(v)}' for v in include_vals for cf in concrete_fields]
        result = peirce_query(substrate, " OR ".join(exprs), limit=None)
        return set(result.entity_ids)
    else:
        # mode=all: intersect per-value OR sets
        result_set = None
        for v in include_vals:
            exprs     = [f'{dim}.{cf} = {serialize(v)}' for cf in concrete_fields]
            qr        = peirce_query(substrate, " OR ".join(exprs), limit=None)
            value_set = set(qr.entity_ids)
            result_set = value_set if result_set is None else result_set & value_set
        return result_set or set()


def plan_alias_constraints(
    constraints: list,
    substrate,
    lens_id: str,
) -> dict:
    """
    WS-3B alias-aware set planner.

    Separates alias clauses from concrete clauses.
    Alias clauses are executed as direct posting-list set operations.
    Concrete clauses are returned for normal Peirce serialization.

    Returns:
    {
        "entity_set":            set | None,   # intersection of all alias clause sets
        "remaining_constraints": list,         # concrete clauses for Peirce
        "used_alias_plan":       bool,
    }

    Alias exclude-without-include is deferred — treated as concrete for now.
    Non-fatal: exceptions fall back to all constraints as concrete.
    """
    try:
        alias_sets  = []
        remaining   = []

        for clause in constraints:
            field           = (clause.get("field") or "").lower()
            concrete_fields = _resolve_alias_fields(field, lens_id)
            include_vals    = clause.get("include") or []
            exclude_vals    = clause.get("exclude") or []
            is_clause       = "include" in clause or "exclude" in clause

            if not is_clause or not concrete_fields or (not include_vals and exclude_vals):
                # Not a clause, not an alias, or exclude-only (deferred) → concrete
                remaining.append(clause)
                continue

            alias_set = _execute_alias_clause(clause, concrete_fields, substrate)
            alias_sets.append(alias_set)

            # Exclude handling: subtract entities matching excluded values
            if exclude_vals:
                dim = (clause.get("dimension") or clause.get("category") or "").upper()
                def serialize(v):
                    if isinstance(v, bool): return "true" if v else "false"
                    if isinstance(v, (int, float)): return str(v)
                    return f'"{str(v)}"'
                for v in exclude_vals:
                    exprs   = [f'{dim}.{cf} = {serialize(v)}' for cf in concrete_fields]
                    ex_qr   = peirce_query(substrate, " OR ".join(exprs), limit=None)
                    alias_set -= set(ex_qr.entity_ids)

        if not alias_sets:
            return {"entity_set": None, "remaining_constraints": constraints, "used_alias_plan": False}

        # Intersect all alias sets
        entity_set = alias_sets[0]
        for s in alias_sets[1:]:
            entity_set = entity_set & s

        if DEBUG:
            print(f"[alias_plan] {len(alias_sets)} alias clause(s) → {len(entity_set)} entities")

        return {"entity_set": entity_set, "remaining_constraints": remaining, "used_alias_plan": True}

    except Exception as e:
        if DEBUG:
            print(f"[alias_plan] Warning: plan failed, falling back to concrete: {e}")
        return {"entity_set": None, "remaining_constraints": constraints, "used_alias_plan": False}


def register_substrate(name: str, substrate: Substrate, meta: dict = None) -> None:
    """Register a compiled substrate under a name."""
    _registry[name] = substrate
    _registry_meta[name] = meta or {}
    # Invalidate affordances cache on every registration — same-name re-ingests
    # must not serve stale type metadata from the previous substrate version.
    _affordances_cache.pop(name, None)
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

    # Concatenate dimension spoke CSVs only.
    # Explicitly exclude snf_meta.csv and snf_entity_meta.csv — these are
    # display/sidecar files with different schemas. Including them corrupts
    # the spoke table with NaN dimension/semantic_key rows and breaks the
    # affordances trie. They are loaded separately below.
    SPOKE_DIMENSIONS = {"who", "what", "when", "where", "why", "how"}
    spoke_files = sorted(
        sf for sf in subdir.glob("snf_*.csv")
        if sf.stem.replace("snf_", "") in SPOKE_DIMENSIONS
    )
    if not spoke_files:
        raise ValueError(f"No dimension snf_*.csv files found in {subdir}")

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

    # Add nucleus columns if absent — CSVs produced by snf-peirce >= 0.2.7
    # include them; older CSVs do not. NULL for old CSVs is correct —
    # queryable but not SRF-exportable without recompile.
    for col in ("nucleus_field", "nucleus_value", "nucleus_prefix"):
        if col not in spokes.columns:
            spokes[col] = None

    # Build in-memory DuckDB and create snf_spoke table
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE snf_spoke (
            entity_id      VARCHAR,
            dimension      VARCHAR,
            semantic_key   VARCHAR,
            value          VARCHAR,
            coordinate     VARCHAR,
            lens_id        VARCHAR,
            correlation_id VARCHAR,
            group_type     VARCHAR,
            nucleus_field  VARCHAR,
            nucleus_value  VARCHAR,
            nucleus_prefix VARCHAR
        )
    """)
    conn.register("_spokes_df", spokes)
    conn.execute("""
        INSERT INTO snf_spoke
        SELECT
            CAST(entity_id     AS VARCHAR),
            CAST(dimension     AS VARCHAR),
            CAST(semantic_key  AS VARCHAR),
            CAST(value         AS VARCHAR),
            CAST(coordinate    AS VARCHAR),
            CAST(lens_id       AS VARCHAR),
            NULL AS correlation_id,
            NULL AS group_type,
            CAST(nucleus_field  AS VARCHAR),
            CAST(nucleus_value  AS VARCHAR),
            CAST(nucleus_prefix AS VARCHAR)
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


# ── Substrate metadata cache ──────────────────────────────────────────────────
# Caches entity_count, dimensions, and lens_id keyed by filename + mtime.
# Avoids full DuckDB scans on startup for already-seen substrates.
# Cache file: <SUBSTRATES_DIR>/.registry_cache.json  (hidden, auto-managed)

def _load_meta_cache() -> dict:
    """Load the metadata cache from disk. Returns {} on any failure."""
    try:
        p = Path(REGISTRY_CACHE)
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _save_meta_cache(cache: dict) -> None:
    """Persist the metadata cache to disk. Non-fatal on failure."""
    try:
        Path(REGISTRY_CACHE).write_text(
            json.dumps(cache, indent=2), encoding="utf-8"
        )
    except Exception as e:
        print(f"[registry] Warning: could not write metadata cache: {e}")

def _cache_key(entry: Path) -> str:
    """Cache key = filename stem + mtime. Changes when file is replaced."""
    try:
        mtime = int(entry.stat().st_mtime)
    except OSError:
        mtime = 0
    return f"{entry.stem}:{mtime}"


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

    meta_cache = _load_meta_cache()
    cache_dirty = False

    for entry in sorted(base.iterdir()):

        # ── .duckdb file — model_builder.py output ───────────────────────────
        if entry.is_file() and entry.suffix == ".duckdb":
            name = entry.stem
            if name in _registry or name in _adapter_registry:
                continue  # already loaded — skip on refresh
            try:
                conn = duckdb.connect(str(entry), read_only=True)

                # Check metadata cache before running full scans.
                # Cache hit: skip entity_count() and dimensions() scans.
                # Cache miss or stale (mtime changed): run scans and update cache.
                ckey = _cache_key(entry)
                if ckey in meta_cache:
                    cached = meta_cache[ckey]
                    lens_id = cached["lens_id"]
                    substrate = Substrate(conn, lens_id)
                    meta = {
                        "path":         str(entry),
                        "entity_count": cached["entity_count"],
                        "dimensions":   cached["dimensions"],
                        "lens_id":      lens_id,
                        "label":        name,
                        "backend":      "duckdb_file",
                    }
                    print(f"[registry] Loaded {name}: {meta['entity_count']:,} entities "
                          f"(cached, duckdb)")
                else:
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
                    meta_cache[ckey] = {
                        "lens_id":      lens_id,
                        "entity_count": meta["entity_count"],
                        "dimensions":   meta["dimensions"],
                    }
                    cache_dirty = True
                    print(f"[registry] Loaded {name}: {meta['entity_count']:,} entities, "
                          f"dimensions: {meta['dimensions']} (duckdb)")

                register_substrate(name, substrate, meta)

                # WS-3A — probe for manifest sidecar alongside .duckdb.
                # <name>.manifest.json is written by compile_job() at ingest time.
                # Absent for pre-WS-3A substrates — skip silently.
                manifest_sidecar = entry.with_suffix(".manifest.json")
                if manifest_sidecar.exists():
                    try:
                        manifest_data = json.loads(
                            manifest_sidecar.read_text(encoding="utf-8")
                        )
                        _manifest_registry[lens_id] = manifest_data
                        print(f"[registry] Loaded manifest for {name} "
                              f"(lens_id: {lens_id})")
                    except Exception as manifest_err:
                        print(f"[registry] Warning: could not load manifest "
                              f"for {name}: {manifest_err}")

            except Exception as e:
                print(f"[registry] Failed to load {name}.duckdb: {e}")
            continue

        # ── spoke CSV directory — legacy format ───────────────────────────────
        if not entry.is_dir():
            continue

        spoke_files = [
            sf for sf in entry.glob("snf_*.csv")
            if sf.stem.replace("snf_", "") in {"who", "what", "when", "where", "why", "how"}
        ]
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

            # ── Load entity_meta sidecar if present (Plover substrates) ──────
            entity_meta_path = entry / "snf_entity_meta.csv"
            display_json_path = entry / "display.json"
            if entity_meta_path.exists():
                import csv as _csv
                entity_meta_index: Dict[str, dict] = {}
                with entity_meta_path.open(encoding="utf-8") as _f:
                    for _row in _csv.DictReader(_f):
                        entity_meta_index[_row["entity_id"]] = dict(_row)
                _entity_meta_store[name] = entity_meta_index
                print(f"[registry] Loaded entity_meta for {name}: "
                      f"{len(entity_meta_index):,} entries")
            if display_json_path.exists():
                with display_json_path.open(encoding="utf-8") as _f:
                    _display_contract[name] = json.load(_f)
                print(f"[registry] Loaded display contract for {name}")

        except Exception as e:
            print(f"[registry] Failed to load {name}: {e}")

    # Persist any new cache entries accumulated during this scan
    if cache_dirty:
        _save_meta_cache(meta_cache)
        print(f"[registry] Metadata cache updated ({len(meta_cache)} entries)")


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
    """Convert constraint array to Peirce string.

    Accepts two input shapes — both are handled transparently:

    1. Legacy flat constraint (existing chip format):
       {"category": "WHAT", "field": "ingredient", "op": "eq", "value": "gin"}

       Same-field flat constraints are grouped and OR'd — preserving the
       pre-clause behavior that multiple chips on the same field mean ANY.

    2. Clause object (new format):
       {
         "dimension": "WHAT",
         "field":     "ingredient",
         "mode":      "any",          # "any" → OR, "all" → AND
         "include":   ["gin", "Aperol"],
         "exclude":   ["egg white"]   # → AND NOT each value
       }

    Clauses are AND'd together across fields/dimensions.

    Serialization contract:
      include + mode:any  →  WHAT.field = "v1" OR WHAT.field = "v2"
      include + mode:all  →  WHAT.field = "v1" AND WHAT.field = "v2"
      exclude             →  AND NOT WHAT.field = "v"  (always, independent of mode)
      between             →  WHAT.field BETWEEN v1 AND v2
      single include      →  WHAT.field = "v"  (no parens, no join keyword)
    """
    from collections import OrderedDict

    def serialize(v):
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        return f'"{str(v)}"'

    and_parts     = []
    legacy_groups = OrderedDict()

    for c in constraints:
        if "include" in c or "exclude" in c:
            # Clause object — serialize directly
            dim   = (c.get("dimension") or c.get("category") or "").upper()
            field = (c.get("field") or "").lower()
            mode  = c.get("mode", "any")

            if not dim or not field:
                continue

            include_vals = c.get("include") or []
            exclude_vals = c.get("exclude") or []

            if include_vals:
                join_kw = " OR " if mode == "any" else " AND "
                exprs = [f'{dim}.{field} = {serialize(v)}' for v in include_vals]
                if len(exprs) == 1:
                    and_parts.append(exprs[0])
                else:
                    # No parens — Peirce parser rejects OR inside parentheses.
                    and_parts.append(join_kw.join(exprs))

            for v in exclude_vals:
                and_parts.append(f'NOT {dim}.{field} = {serialize(v)}')

        else:
            # Legacy flat constraint — collect into groups for OR-within-field
            dim    = (c.get("category") or c.get("dimension") or "").upper()
            field  = (c.get("field") or "").lower()
            op     = c.get("op", "eq")
            value  = c.get("value", "")
            value2 = c.get("value2")

            if not dim or not field:
                continue

            if op == "between" and value2 is not None:
                expr = f'{dim}.{field} BETWEEN {serialize(value)} AND {serialize(value2)}'
            else:
                peirce_op = OP_TO_PEIRCE.get(op, "=")
                expr = f'{dim}.{field} {peirce_op} {serialize(value)}'

            if c.get("negated"):
                expr = f"NOT {expr}"

            key = (dim, field)
            if key not in legacy_groups:
                legacy_groups[key] = []
            legacy_groups[key].append(expr)

    # Flush legacy groups — OR within field, AND across fields.
    # No parens — Peirce parser rejects OR inside parentheses.
    for exprs in legacy_groups.values():
        if len(exprs) == 1:
            and_parts.append(exprs[0])
        else:
            and_parts.append(" OR ".join(exprs))

    return " AND ".join(and_parts)


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
        placeholders = ", ".join("?" * len(entity_ids))

        # Probe for correlation_id column — old substrates predate WS-1 and don't have it.
        # Without this check the entire hydration falls through to the empty-coordinates
        # fallback, producing result cards with entity IDs but no dimension data.
        try:
            substrate._conn.execute("SELECT correlation_id FROM snf_spoke LIMIT 0")
            has_correlation_id = True
        except Exception:
            has_correlation_id = False

        select_cols = (
            "entity_id, dimension, semantic_key, value, coordinate, correlation_id"
            if has_correlation_id else
            "entity_id, dimension, semantic_key, value, coordinate"
        )

        rows = substrate._conn.execute(
            f"SELECT {select_cols} "
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

        for row in rows:
            entity_id, dimension, semantic_key, value, coordinate = row[:5]
            correlation_id = row[5] if has_correlation_id else None
            # Apply field filter if specified
            if fields:
                key_part = semantic_key.split(".")[-1] if "." in semantic_key else semantic_key
                if key_part not in fields and semantic_key not in fields:
                    continue

            dim_upper = dimension.upper()
            if dim_upper not in by_entity[entity_id]:
                by_entity[entity_id][dim_upper] = []

            field_name = semantic_key.split(".")[-1] if "." in semantic_key else semantic_key

            fact = {
                "field":      field_name,
                "value":      value,
                "coordinate": coordinate,
            }
            if correlation_id is not None:
                fact["correlation_id"] = correlation_id

            by_entity[entity_id][dim_upper].append(fact)

        # Build result objects
        results = []
        for entity_id in entity_ids:
            entity_coords_present = set()
            for dim_facts in by_entity[entity_id].values():
                for fact in dim_facts:
                    if fact.get("coordinate"):
                        entity_coords_present.add(fact["coordinate"].lower())

            matched = []
            for coord in matched_coordinates.get(entity_id, []):
                if coord.lower() not in entity_coords_present:
                    continue
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
    # Same-field AND semantics: multiple eq constraints on the same dim.field
    # are each given a unique occurrence key so they appear as separate groups
    # in the stepdown probe — intersected, not unioned.
    # e.g. WHAT.ingredient = "gin" AND WHAT.ingredient = "Aperol" produces
    # two groups: {WHAT: {ingredient_0: ["gin"]}} and {WHAT: {ingredient_1: ["Aperol"]}}
    # These are intersected in the stepdown probe, giving correct cardinality.
    def group_conjunct(conjunct):
        from collections import defaultdict
        by_dim = {}
        occurrence = defaultdict(int)
        for c in conjunct:
            if c.get("op") in ("contains", "prefix", "only", "between", "gt", "gte", "lt", "lte"):
                continue  # not routable as coordinate eq — range ops need special handling
            dim   = (c.get("category") or c.get("dimension") or "").upper()
            field = (c.get("field") or "").lower()
            value = str(c.get("value", ""))
            if not dim or not field:
                continue
            # Use occurrence index to distinguish same-field AND constraints
            occ       = occurrence[(dim, field)]
            occurrence[(dim, field)] += 1
            field_key = f"{field}_{occ}" if occ > 0 else field
            by_dim.setdefault(dim, {}).setdefault(field_key, []).append(value)
        return by_dim

    first_conjunct = conjuncts[0]
    by_dim = group_conjunct(first_conjunct)

    if not by_dim:
        # No routable constraints — fall back to peirce_query, no trace
        result = peirce_query(substrate, peirce_string, limit=limit)
        return result.entity_ids, result.count, []

    # ── Cardinality probe — one COUNT per dimension ───────────────────────────
    # Strip occurrence suffix from field keys (ingredient_1 → ingredient)
    # before using them as semantic_key values in SQL.
    import re as _re
    def _strip_occ(field_key):
        return _re.sub(r"_\d+$", "", field_key)

    dim_counts = {}
    for dim, fields in by_dim.items():
        # Build one subquery per field group and intersect them.
        # Multiple groups on the same field (ingredient_0, ingredient_1) must be
        # intersected — not unioned — to reflect same-field AND semantics.
        field_groups = list(fields.items())
        if len(field_groups) == 1:
            # Single group — simple COUNT
            field_key, values = field_groups[0]
            field = _strip_occ(field_key)
            phs   = ", ".join(["?" for _ in values])
            row = conn.execute(
                f"SELECT COUNT(DISTINCT entity_id) FROM snf_spoke "
                f"WHERE lens_id = ? AND dimension = ? AND semantic_key = ? AND value IN ({phs})",
                [lens_id, dim.lower(), field] + values
            ).fetchone()
            dim_counts[dim] = row[0] if row else 0
        else:
            # Multiple groups — intersect with nested IN subqueries
            first_key, first_values = field_groups[0]
            first_field = _strip_occ(first_key)
            first_phs   = ", ".join(["?" for _ in first_values])
            nested_sql    = ""
            nested_params = []
            for field_key, values in field_groups[1:]:
                field = _strip_occ(field_key)
                phs   = ", ".join(["?" for _ in values])
                nested_sql   += (
                    f" AND entity_id IN ("
                    f"SELECT entity_id FROM snf_spoke "
                    f"WHERE lens_id = ? AND dimension = ? AND semantic_key = ? AND value IN ({phs}))"
                )
                nested_params += [lens_id, dim.lower(), field] + values
            row = conn.execute(
                f"SELECT COUNT(DISTINCT entity_id) FROM snf_spoke "
                f"WHERE lens_id = ? AND dimension = ? AND semantic_key = ? AND value IN ({first_phs})"
                f"{nested_sql}",
                [lens_id, dim.lower(), first_field] + first_values + nested_params
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
            for field_key, values in anchor_fields.items():
                field = _strip_occ(field_key)
                phs = ", ".join(["?" for _ in values])
                anchor_parts.append(f"(semantic_key = ? AND value IN ({phs}))")
                anchor_params += [field] + values
            anchor_where = " OR ".join(anchor_parts)

            # Build nested IN subqueries for each additional dim
            # Each dim may have multiple field groups (e.g. ingredient_0, ingredient_1)
            # that must each be intersected separately.
            nested_sql    = ""
            nested_params = []
            for dim in dims_so_far[1:]:
                dim_fields  = by_dim[dim]
                for field_key, values in dim_fields.items():
                    field      = _strip_occ(field_key)
                    dim_params = [lens_id, dim.lower()]
                    phs        = ", ".join(["?" for _ in values])
                    dim_where  = f"(semantic_key = ? AND value IN ({phs}))"
                    dim_params += [field] + values
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
    # Strip occurrence suffixes from field keys for display (ingredient_0 → ingredient)
    # Merge same-field groups back together for the trace display — the trace
    # shows what was queried, not the internal grouping structure.
    def _merge_fields_for_display(fields_dict):
        merged = {}
        for field_key, values in fields_dict.items():
            field = _strip_occ(field_key)
            merged.setdefault(field, []).extend(values)
        return merged

    trace = [
        {
            "dimension":   dim,
            "cardinality": stepdown_counts[i],
            "fields": [
                {"field": f, "values": list(v)}
                for f, v in _merge_fields_for_display(by_dim[dim]).items()
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
    allow_origins=CORS_ORIGINS,   # Set CORS_ORIGINS in .env to tighten
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


@app.get("/api/refresh-substrates")
async def refresh_substrates():
    """Re-scan SUBSTRATES_DIR and load any new substrates without restarting."""
    before = set(_registry_meta.keys())
    load_substrates_from_disk()
    after  = set(_registry_meta.keys())
    new    = list(after - before)
    return {
        "added": new,
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
                    "COUNT(DISTINCT value) as distinct_value_count, "
                    "COUNT(*) as fact_count "
                    "FROM snf_spoke "
                    "WHERE dimension = ? AND lens_id = ? "
                    "GROUP BY semantic_key "
                    "ORDER BY fact_count DESC",
                    [dim, substrate.lens_id]
                ).fetchall()

                # Load compiled type table if present (written by Model Builder).
                # Eliminates keyword guessing for substrates built with current Model Builder.
                # Falls back to keyword heuristic for older substrates — no breakage.
                compiled_types: Dict[str, str] = {}
                try:
                    type_rows = substrate._conn.execute(
                        "SELECT dimension, semantic_key, value_type FROM snf_field_types"
                    ).fetchall()
                    for td, tsk, tvt in type_rows:
                        compiled_types[f"{td}|{tsk}"] = tvt
                except Exception:
                    pass  # table absent — older substrate, heuristic handles it

                for semantic_key, distinct_value_count, fact_count in rows:
                    field_name = semantic_key.split(".")[-1] if "." in semantic_key else semantic_key

                    compiled_key = f"{dim}|{semantic_key}"
                    if compiled_key in compiled_types:
                        # Compiled type table — authoritative, no guessing needed
                        value_type = compiled_types[compiled_key]
                    elif any(kw in field_name.lower() for kw in ["year", "date", "month", "day", "release", "activity"]):
                        # Known date fields — keyword fast path
                        value_type = "date"
                    elif any(kw in field_name.lower() for kw in ["count", "amount", "price", "cmc", "size"]):
                        # Known numeric fields — keyword fast path
                        value_type = "number"
                    elif any(kw in field_name.lower() for kw in ["_id", "code", "number", "ref", "key", "system", "status"]):
                        # Identifier/code fields — always TEXT even if values look numeric.
                        # e.g. matter_number="2024-0042", condition_code="44054006",
                        # postal_code="98119". A human knows these aren't measurements.
                        value_type = "text"
                    else:
                        # Value sampling — empirically determine type from actual values.
                        # Handles any domain's numeric fields without keyword maintenance.
                        # Falls back to enum/text if sampling fails or is inconclusive.
                        try:
                            sample = substrate._conn.execute(
                                "SELECT value FROM snf_spoke "
                                "WHERE dimension = ? AND semantic_key = ? AND lens_id = ? "
                                "LIMIT 20",
                                [dim, semantic_key, substrate.lens_id]
                            ).fetchall()
                            if sample:
                                numeric_hits = sum(
                                    1 for (v,) in sample
                                    if v is not None and _is_numeric(v)
                                )
                                if numeric_hits == len(sample):
                                    value_type = "number"
                                elif distinct_value_count <= 25:
                                    value_type = "enum"
                                else:
                                    value_type = "text"
                            else:
                                value_type = "text"
                        except Exception:
                            value_type = "enum" if distinct_value_count <= 25 else "text"

                    result[dim_upper][field_name] = {
                        "fact_count":      fact_count,
                        "distinct_values": distinct_value_count,
                        "value_type":      value_type,
                    }

            manifest = _manifest_registry.get(substrate.lens_id)
            if manifest:
                facet_aliases    = manifest.get("facet_aliases")    or []
                stem_projections = manifest.get("stem_projections") or []
                if facet_aliases or stem_projections:
                    stem_lookup = {sp["stem"]: set(sp["expands_to"]) for sp in stem_projections}
                    promoted_stems = {fa.get("source_stem") for fa in facet_aliases if fa.get("source_stem")}
                    for fa in facet_aliases:
                        alias_name  = fa.get("name")
                        source_stem = fa.get("source_stem")
                        if not alias_name or not source_stem:
                            continue
                        concrete_cols = stem_lookup.get(source_stem, set())
                        if not concrete_cols:
                            continue
                        for dim_upper, fields in result.items():
                            matching = {k: v for k, v in fields.items() if k in concrete_cols}
                            if not matching:
                                continue
                            agg_fact_count      = sum(m["fact_count"]      for m in matching.values())
                            agg_distinct_values = max(m["distinct_values"] for m in matching.values())
                            agg_value_type      = next(iter(matching.values()))["value_type"]
                            for k in matching:
                                del result[dim_upper][k]
                            result[dim_upper][alias_name] = {
                                "fact_count":      agg_fact_count,
                                "distinct_values": agg_distinct_values,
                                "value_type":      agg_value_type,
                                "is_alias":        True,
                            }
                    unpromoted_stems = {stem for stem in stem_lookup if stem not in promoted_stems}
                    for stem in unpromoted_stems:
                        concrete_cols = stem_lookup.get(stem, set())
                        for dim_upper, fields in result.items():
                            for k in list(fields.keys()):
                                if k in concrete_cols:
                                    del result[dim_upper][k]

            _affordances_cache[substrate_id] = result
            return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/entity-meta/{substrate_id}/{entity_id:path}")
async def entity_meta(substrate_id: str, entity_id: str):
    """
    Return entity_meta sidecar row for a specific entity in a Plover substrate.

    Powers Plover result card display: url, description, thumbnail,
    source_domain, provider, date, harvest_path.

    Returns 404 for substrates without entity_meta (non-Plover substrates).
    Returns 404 for unknown entity_id within a Plover substrate.

    The frontend calls this after a query returns entity_ids to hydrate
    the web-specific display layer without a second query to the spoke table.
    """
    store = _entity_meta_store.get(substrate_id)
    if store is None:
        raise HTTPException(
            status_code=404,
            detail=f"No entity_meta sidecar for substrate '{substrate_id}'. "
                   f"This is a non-Plover substrate — use /api/hydrate for coordinate display."
        )
    row = store.get(entity_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"entity_id '{entity_id}' not found in entity_meta for '{substrate_id}'"
        )
    return row


@app.get("/api/display-contract/{substrate_id}")
async def display_contract(substrate_id: str):
    """
    Return the display.json contract for a substrate if present.

    Tells the frontend which fields to use for H1, H2, description,
    link, and image in result cards. Absent for non-Plover substrates.

    Example response:
    {
        "primary_label":   "entity_meta.label",
        "secondary_label": "entity_meta.provider_date",
        "description":     "entity_meta.description",
        "link":            "entity_meta.url",
        "image":           "entity_meta.thumbnail_url"
    }
    """
    contract = _display_contract.get(substrate_id)
    if contract is None:
        raise HTTPException(
            status_code=404,
            detail=f"No display contract for substrate '{substrate_id}'."
        )
    return contract


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

    # ── Query resolution ──────────────────────────────────────────────────────
    #
    # Two paths, cleanly separated:
    #
    # Constraint path (frontend sends constraints array — normal UI path):
    #   1. Run alias clauses through set algebra planner (DuckDB only)
    #   2. Serialize remaining concrete clauses to Peirce
    #   3. Intersect alias set with concrete routing result
    #
    # Raw Peirce path (peirce only, no constraints):
    #   No alias expansion — pass through as-is.
    #   Alias fields in raw Peirce are the caller's responsibility.
    #
    # No alias expansion happens at the Peirce string level.

    lens_id       = getattr(substrate_or_adapter, "lens_id", None) or substrate_id
    alias_plan    = {"entity_set": None, "remaining_constraints": [], "used_alias_plan": False}
    peirce_string = None

    if req.constraints and not is_adapter:
        # DuckDB + constraints: run alias set planner first
        alias_plan    = plan_alias_constraints(req.constraints, substrate_or_adapter, lens_id)
        remaining     = alias_plan["remaining_constraints"]
        peirce_string = constraints_to_peirce(remaining) if remaining else None
    elif req.constraints and is_adapter:
        peirce_string = constraints_to_peirce(req.constraints)
    elif req.peirce:
        peirce_string = req.peirce

    alias_set = alias_plan["entity_set"]

    # Need at least one of: a Peirce string or an alias set result
    if not peirce_string and alias_set is None:
        raise HTTPException(status_code=400, detail="peirce or constraints required")

    if DEBUG and peirce_string:
        print(f"[query] substrate={substrate_id} adapter={is_adapter} peirce={peirce_string!r}")
    if DEBUG and alias_plan["used_alias_plan"]:
        print(f"[alias_plan] entity_set size={len(alias_set) if alias_set is not None else 0}")

    # ── Adaptive limit ────────────────────────────────────────────────────────
    DISPLAY_CAP      = 200
    SMALL_RESULT_CAP = 500

    probe_start = time.perf_counter()

    if is_adapter:
        # ── Adapter path (PostgresAdapter etc.) ───────────────────────────────
        try:
            adapter = substrate_or_adapter
            result  = adapter.query(peirce_string, limit=req.limit or 100000)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        probe_ms   = (time.perf_counter() - probe_start) * 1000
        exec_start = time.perf_counter()

        offset      = req.offset or 0
        display_cap = req.limit if req.limit is not None else (
            result.count if result.count <= SMALL_RESULT_CAP else DISPLAY_CAP
        )
        page_ids = result.entity_ids[offset : offset + display_cap]

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
            if peirce_string:
                entity_ids, total_count, duckdb_trace = _duckdb_query_with_trace(
                    substrate     = substrate,
                    peirce_string = peirce_string,
                    limit         = req.limit or 100000,
                )
                # Intersect with alias set plan if present
                if alias_set is not None:
                    entity_ids  = [e for e in entity_ids if e in alias_set]
                    total_count = len(entity_ids)
            else:
                # Alias set plan only — no concrete Peirce constraints
                entity_ids   = list(alias_set)
                total_count  = len(entity_ids)
                duckdb_trace = []

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

        class _DuckDBResult:
            def __init__(self, ids, count, trace):
                self.entity_ids = ids
                self.count      = count
                self.trace      = trace

        result = _DuckDBResult(entity_ids, total_count, duckdb_trace)

        offset      = req.offset or 0
        display_cap = req.limit if req.limit is not None else (
            result.count if result.count <= SMALL_RESULT_CAP else DISPLAY_CAP
        )
        page_ids = result.entity_ids[offset : offset + display_cap]

        matched_coords = extract_matched_coordinates(peirce_string or "", page_ids)
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
            return {"scope": result.scope, "dimension": result.dimension,
                    "field": result.field, "rows": result.rows, "substrate": substrate_id}
        else:
            substrate = substrate_or_adapter
            parts = req.expression.strip().split("|")
            if len(parts) == 3 and parts[2] == "*":
                dim   = parts[0].upper()
                field = parts[1]
                concrete_fields = _resolve_alias_fields(field, substrate.lens_id)
                if concrete_fields:
                    limit = req.limit or 500
                    phs   = ", ".join(["?" for _ in concrete_fields])
                    db_rows = substrate._conn.execute(
                        f"SELECT value, COUNT(DISTINCT entity_id) AS cnt FROM snf_spoke "
                        f"WHERE dimension = ? AND lens_id = ? AND semantic_key IN ({phs}) "
                        f"GROUP BY value ORDER BY cnt DESC LIMIT ?",
                        [dim.lower(), substrate.lens_id] + concrete_fields + [limit]
                    ).fetchall()
                    return {"scope": "values", "dimension": dim, "field": field,
                            "rows": [{"value": r[0], "count": r[1]} for r in db_rows],
                            "substrate": substrate_id}
            result = discover(substrate, req.expression, limit=req.limit)
            return {"scope": result.scope, "dimension": result.dimension,
                    "field": result.field, "rows": result.rows, "substrate": substrate_id}
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
                dimension=dimension, field=field, entity_ids=entity_ids, limit=limit)
        else:
            substrate = substrate_or_adapter
            concrete_fields = _resolve_alias_fields(field, substrate.lens_id)

            # Use a temp table for the entity ID set rather than a large IN clause.
            # DuckDB joins against a temp table significantly faster than filtering
            # against a long IN list of strings, especially for large result sets.
            substrate._conn.execute(
                "CREATE OR REPLACE TEMP TABLE _trie_ids (entity_id VARCHAR)"
            )
            if entity_ids:
                substrate._conn.executemany(
                    "INSERT INTO _trie_ids VALUES (?)",
                    [[eid] for eid in entity_ids]
                )

            if concrete_fields:
                field_phs = ", ".join(["?" for _ in concrete_fields])
                db_rows = substrate._conn.execute(
                    f"SELECT s.value, COUNT(DISTINCT s.entity_id) AS cnt "
                    f"FROM snf_spoke s JOIN _trie_ids t ON s.entity_id = t.entity_id "
                    f"WHERE s.dimension = ? AND s.lens_id = ? AND s.semantic_key IN ({field_phs}) "
                    f"GROUP BY s.value ORDER BY cnt DESC LIMIT ?",
                    [dimension.lower(), substrate.lens_id] + concrete_fields + [limit]
                ).fetchall()
            else:
                db_rows = substrate._conn.execute(
                    "SELECT s.value, COUNT(DISTINCT s.entity_id) AS cnt "
                    "FROM snf_spoke s JOIN _trie_ids t ON s.entity_id = t.entity_id "
                    "WHERE s.dimension = ? AND s.semantic_key = ? AND s.lens_id = ? "
                    "GROUP BY s.value ORDER BY cnt DESC LIMIT ?",
                    [dimension.lower(), field, substrate.lens_id, limit]
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
# Full result set export — bypasses display cap, returns all matching entities
# ─────────────────────────────────────────────────────────────────────────────

class FullExportRequest(BaseModel):
    peirce:       Optional[str]        = None
    constraints:  Optional[List[dict]] = None
    entity_ids:   Optional[List[str]]  = None   # explicit selection — bypasses query
    schema:       Optional[str]        = None
    substrate_id: Optional[str]        = None
    fields:       Optional[List[str]]  = None   # projection


@app.post("/api/export/full")
async def export_full(req: FullExportRequest):
    """
    Run a query without any display cap and return the full hydrated result set.

    Used by CSV, XLSX, and JSON export when the result set exceeds the display
    cap (200 entities per page). The client formats the response into the
    desired file format.

    Returns the same shape as /api/query but with all matching entities
    hydrated — no pagination, no offset.
    """
    substrate_id = req.substrate_id or req.schema
    substrate    = _registry.get(substrate_id)

    if not substrate:
        if _registry:
            substrate_id = list(_registry.keys())[0]
            substrate    = _registry[substrate_id]
        else:
            raise HTTPException(status_code=404, detail="No substrates loaded")

    # Resolve entity IDs
    if req.entity_ids:
        entity_ids = req.entity_ids
    else:
        if req.peirce:
            peirce_string = req.peirce
        elif req.constraints:
            peirce_string = constraints_to_peirce(req.constraints)
        else:
            raise HTTPException(status_code=400, detail="peirce, constraints, or entity_ids required")

        try:
            entity_ids, total_count, _ = _duckdb_query_with_trace(
                substrate     = substrate,
                peirce_string = peirce_string,
                limit         = 100000,  # hard cap — no reasonable cohort exceeds this
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    if not entity_ids:
        return {"results": [], "total": 0}

    # Hydrate all entities — no display cap
    hydrated = hydrate_results(
        entity_ids          = entity_ids,
        substrate           = substrate,
        matched_coordinates = {},
        fields              = req.fields,
    )

    return {
        "total":   len(entity_ids),
        "results": hydrated,
    }


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
# Save-to-disk exports  (v0.1 desktop contract)
#
# All formats write to ~/Documents/Reckoner Exports/ and return the path.
# The frontend shows the path — no blob, no hidden <a>, no browser download.
# ─────────────────────────────────────────────────────────────────────────────

from datetime import datetime

# EXPORT_DIR now comes from model_builder.settings (imported above), which
# creates it. Default is unchanged — Documents is right for files the user
# asked for — but it is overridable via RECKONER_EXPORT_DIR without a rebuild.


class SaveExportRequest(BaseModel):
    peirce:       Optional[str]        = None
    constraints:  Optional[List[dict]] = None
    entity_ids:   Optional[List[str]]  = None
    schema:       Optional[str]        = None
    substrate_id: Optional[str]        = None
    fields:       Optional[List[str]]  = None
    sort_field:   Optional[str]        = None
    sort_dir:     Optional[str]        = "asc"


def _resolve_rows(req: SaveExportRequest) -> tuple[list[dict], str]:
    """Shared logic: resolve entity IDs → flat row list. Returns (rows, substrate_id)."""
    import pandas as pd
    from collections import defaultdict

    substrate_id = req.substrate_id or req.schema
    substrate    = _registry.get(substrate_id)

    if not substrate:
        if _registry:
            substrate_id = list(_registry.keys())[0]
            substrate    = _registry[substrate_id]
        else:
            raise HTTPException(status_code=404, detail="No substrates loaded")

    # Resolve entity IDs
    if req.entity_ids:
        entity_ids = req.entity_ids
    else:
        if req.peirce:
            peirce_string = req.peirce
        elif req.constraints:
            peirce_string = constraints_to_peirce(req.constraints)
        else:
            raise HTTPException(status_code=400, detail="peirce, constraints, or entity_ids required")

        try:
            entity_ids, _, _ = _duckdb_query_with_trace(
                substrate     = substrate,
                peirce_string = peirce_string,
                limit         = 100000,
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    if not entity_ids:
        raise HTTPException(status_code=204, detail="No results to export")

    # Pull flat spoke rows
    placeholders = ", ".join("?" * len(entity_ids))
    spoke_rows = substrate._conn.execute(
        f"SELECT entity_id, dimension, semantic_key, value "
        f"FROM snf_spoke "
        f"WHERE entity_id IN ({placeholders}) AND lens_id = ? "
        f"ORDER BY entity_id, dimension, semantic_key",
        entity_ids + [substrate.lens_id]
    ).fetchall()

    by_entity = defaultdict(dict)
    for eid, dimension, semantic_key, value in spoke_rows:
        field = semantic_key.split(".")[-1] if "." in semantic_key else semantic_key
        if req.fields and field not in req.fields:
            continue
        col = f"{dimension.lower()}_{field}"
        if col in by_entity[eid]:
            by_entity[eid][col] = f"{by_entity[eid][col]}; {value}"
        else:
            by_entity[eid][col] = value

    rows = []
    for eid in entity_ids:
        row = {"entity_id": eid}
        row.update(by_entity.get(eid, {}))
        rows.append(row)

    # Sort if requested
    if req.sort_field and rows:
        reverse = (req.sort_dir or "asc") == "desc"
        def sort_key(r):
            v = r.get(req.sort_field) or \
                next((r[k] for k in r if k.endswith(f"_{req.sort_field}")), "")
            try:
                return (0, float(v))
            except (ValueError, TypeError):
                return (1, str(v).lower())
        rows.sort(key=sort_key, reverse=reverse)

    return rows, substrate_id


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


@app.post("/api/export/save-csv")
async def export_save_csv(req: SaveExportRequest):
    """Write a CSV to ~/Documents/Reckoner Exports/ and return the path."""
    import pandas as pd

    rows, substrate_id = _resolve_rows(req)
    filename = f"reckoner_{substrate_id}_{_ts()}.csv"
    path     = EXPORT_DIR / filename

    pd.DataFrame(rows).to_csv(path, index=False)

    return {"saved": True, "path": str(path), "filename": filename, "row_count": len(rows)}


@app.post("/api/export/save-json")
async def export_save_json(req: SaveExportRequest):
    """Write a JSON file to ~/Documents/Reckoner Exports/ and return the path."""
    rows, substrate_id = _resolve_rows(req)
    filename = f"reckoner_{substrate_id}_{_ts()}.json"
    path     = EXPORT_DIR / filename

    path.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")

    return {"saved": True, "path": str(path), "filename": filename, "row_count": len(rows)}


@app.post("/api/export/save-xlsx")
async def export_save_xlsx(req: SaveExportRequest):
    """Write an XLSX file to ~/Documents/Reckoner Exports/ and return the path."""
    import pandas as pd

    rows, substrate_id = _resolve_rows(req)
    filename = f"reckoner_{substrate_id}_{_ts()}.xlsx"
    path     = EXPORT_DIR / filename

    df = pd.DataFrame(rows)
    df.to_excel(str(path), index=False, engine="openpyxl")

    return {"saved": True, "path": str(path), "filename": filename, "row_count": len(rows)}


@app.post("/api/export/save-parquet")
async def export_save_parquet(req: SaveExportRequest):
    """Write a Parquet file to ~/Documents/Reckoner Exports/ and return the path."""
    import pandas as pd

    rows, substrate_id = _resolve_rows(req)
    filename = f"reckoner_{substrate_id}_{_ts()}.parquet"
    path     = EXPORT_DIR / filename

    substrate_id_key = req.substrate_id or req.schema
    substrate        = _registry.get(substrate_id_key) or _registry.get(substrate_id)

    if substrate:
        import tempfile, os as _os
        df = pd.DataFrame(rows)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            substrate._conn.execute(
                "COPY (SELECT * FROM df) TO ? (FORMAT PARQUET)",
                [str(path)]
            )
        finally:
            if _os.path.exists(tmp_path):
                _os.unlink(tmp_path)
    else:
        pd.DataFrame(rows).to_parquet(str(path), index=False)

    return {"saved": True, "path": str(path), "filename": filename, "row_count": len(rows)}


@app.post("/api/export/save-srf")
async def export_save_srf(req: SaveExportRequest):
    """Write an SRF JSON bundle to ~/Documents/Reckoner Exports/ and return the path."""
    rows, substrate_id = _resolve_rows(req)

    # Build SRF-style bundle: array of {entity_id, coordinates}
    # Reuse the hydrated shape the frontend already understands
    substrate_key = req.substrate_id or req.schema or substrate_id
    substrate     = _registry.get(substrate_key)

    if substrate:
        hydrated = hydrate_results(
            entity_ids          = [r["entity_id"] for r in rows],
            substrate           = substrate,
            matched_coordinates = {},
            fields              = req.fields,
        )
        bundle = {
            "schema":      substrate_id,
            "lens_id":     substrate.lens_id,
            "exported_at": datetime.now().isoformat(),
            "row_count":   len(hydrated),
            "records":     hydrated,
        }
    else:
        bundle = {
            "schema":      substrate_id,
            "exported_at": datetime.now().isoformat(),
            "row_count":   len(rows),
            "records":     rows,
        }

    filename = f"reckoner_{substrate_id}_{_ts()}.srf.json"
    path     = EXPORT_DIR / filename
    path.write_text(json.dumps(bundle, indent=2, default=str), encoding="utf-8")

    return {"saved": True, "path": str(path), "filename": filename, "row_count": bundle["row_count"]}


@app.post("/api/export/save-peirce")
async def export_save_peirce(req: SaveExportRequest):
    """Write a .peirce set bundle to ~/Documents/Reckoner Exports/ and return the path."""
    rows, substrate_id = _resolve_rows(req)

    peirce_str = req.peirce or (constraints_to_peirce(req.constraints) if req.constraints else "")

    bundle = {
        "set_id":      f"{substrate_id}_{_ts()}",
        "query": {
            "substrate_id":       substrate_id,
            "peirce":             peirce_str,
            "constraints":        req.constraints or [],
            "exported_at":        datetime.now().isoformat(),
        },
        "results": {
            "entity_ids":  [r["entity_id"] for r in rows],
            "count":       len(rows),
            "captured_at": datetime.now().isoformat(),
        },
    }

    filename = f"reckoner_{substrate_id}_{_ts()}.peirce"
    path     = EXPORT_DIR / filename
    path.write_text(json.dumps(bundle, indent=2, default=str), encoding="utf-8")

    return {"saved": True, "path": str(path), "filename": filename, "row_count": len(rows)}



# ─────────────────────────────────────────────────────────────────────────────
# SRF Import
#
# SRF records are never written into existing disk-backed substrates.
# Each lens_id gets its own in-memory substrate, created on first import
# and reused for subsequent imports of the same lens.
# These substrates appear in the registry alongside disk-backed ones and
# are queryable immediately after import.
# ─────────────────────────────────────────────────────────────────────────────

def _srf_import_path(entity_id: str, lens_id: str, substrate_key: str = None) -> Path:
    """
    Return the path where an SRF record should be persisted.
    Uses substrate_key as folder name if provided (for named collections),
    otherwise falls back to lens_id.
    e.g. substrates/srf_imports/novels_of_joseph_heller/isbn_9780774032551.srf
    """
    safe_entity_id = entity_id.replace(":", "_").replace("/", "_")
    folder = substrate_key if substrate_key else lens_id
    lens_dir = Path(SRF_IMPORTS_DIR) / folder
    lens_dir.mkdir(parents=True, exist_ok=True)
    return lens_dir / f"{safe_entity_id}.srf"


def _persist_srf_record(record: SRFRecord, substrate_key: str = None) -> None:
    """Write an SRF record to disk for persistence across restarts."""
    import json
    path = _srf_import_path(record.entity_id, record.lens_id, substrate_key)
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
            # Use the parent folder name as substrate key — this is the
            # collection name set at import time (e.g. novels_of_joseph_heller)
            substrate_key = srf_file.parent.name
            substrate = _get_or_create_srf_substrate(record.lens_id, substrate_key)

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
                "(entity_id, dimension, semantic_key, value, coordinate, lens_id, "
                "nucleus_field, nucleus_value, nucleus_prefix) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (r["entity_id"], r["dimension"].lower(), r["semantic_key"],
                     r["value"], r["coordinate"], record.lens_id,
                     r.get("nucleus_field"), r.get("nucleus_value"), r.get("nucleus_prefix"))
                    for r in rows["spoke_rows"]
                ]
            )
            _registry_meta[substrate_key]["entity_count"]       = substrate.entity_count()
            _registry_meta[substrate_key]["translator_version"] = record.translator_version
            total += 1
        except Exception as e:
            print(f"[import/srf] Failed to reload {srf_file}: {e}")
            errors += 1

    if total > 0 or errors > 0:
        print(f"[import/srf] Reloaded {total} SRF records ({errors} errors) from {SRF_IMPORTS_DIR}")


def _get_or_create_srf_substrate(lens_id: str, substrate_key: str = None) -> Substrate:
    """
    Return the in-memory SRF substrate for this lens_id, creating it if needed.
    substrate_key overrides lens_id as the registry key (for named collections).
    SRF substrates are separate from disk-backed substrates and are always writable.
    """
    key = substrate_key if substrate_key else lens_id

    if key in _registry:
        return _registry[key]

    # Create a fresh in-memory DuckDB for this substrate
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE snf_spoke (
            entity_id         VARCHAR,
            dimension         VARCHAR,
            semantic_key      VARCHAR,
            value             VARCHAR,
            coordinate        VARCHAR,
            lens_id           VARCHAR,
            correlation_id    VARCHAR,
            group_type        VARCHAR,
            nucleus_field     VARCHAR,
            nucleus_value     VARCHAR,
            nucleus_prefix    VARCHAR
        )
    """)
    conn.execute("CREATE INDEX idx_spoke_coord ON snf_spoke(coordinate)")
    conn.execute("CREATE INDEX idx_spoke_eid   ON snf_spoke(entity_id)")
    conn.execute("CREATE INDEX idx_spoke_dim   ON snf_spoke(dimension, semantic_key)")

    substrate = Substrate(conn, lens_id)  # lens_id is the semantic lens — not the collection key
    register_substrate(key, substrate, meta={
        "label":              key,
        "entity_count":       0,
        "dimensions":         [],
        "lens_id":            lens_id,
        "translator_version": "",
        "source":             "srf_import",
    })

    if DEBUG:
        print(f"[import/srf] Created new in-memory substrate '{key}' (lens: {lens_id})")

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
        "(entity_id, dimension, semantic_key, value, coordinate, lens_id, "
        "nucleus_field, nucleus_value, nucleus_prefix) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                r["entity_id"],
                r["dimension"].lower(),
                r["semantic_key"],
                r["value"],
                r["coordinate"],
                r["lens_id"],
                r.get("nucleus_field"),
                r.get("nucleus_value"),
                r.get("nucleus_prefix"),
            )
            for r in spoke_rows
        ]
    )
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
            "(entity_id, dimension, semantic_key, value, coordinate, lens_id, "
            "nucleus_field, nucleus_value, nucleus_prefix) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    r["entity_id"],
                    r["dimension"].lower(),
                    r["semantic_key"],
                    r["value"],
                    r["coordinate"],
                    r["lens_id"],
                    r.get("nucleus_field"),
                    r.get("nucleus_value"),
                    r.get("nucleus_prefix"),
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


@app.post("/api/load/srf")
async def load_srf_bundle(file: UploadFile = File(...)):
    """
    Accept an SRF bundle file exported from Reckoner and load it as a substrate.

    Accepts the bundle format produced by Reckoner's SRF export:
        {
            "srf_bundle_version": "1.0",
            "lens_id": "...",
            "record_count": N,
            "exported_at": "...",
            "peirce_query": "...",
            "records": [ {...srf record...}, ... ]
        }

    Also accepts a plain JSON array of SRF records for compatibility.

    Response:
        {
            "schema":       "lens_id",
            "entity_count": N,
            "imported":     N,
            "skipped":      N,
            "duplicate":    N,
        }
    """
    content = await file.read()

    # ── Zip file — unpack and process each .srf file as one substrate ────────
    if file.filename.endswith('.zip') or content[:2] == b'PK':
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                names = zf.namelist()

                # Try to read expedition metadata from .peirce file
                expedition_name = None
                expedition_id   = None
                peirce_files = [n for n in names if n.endswith('.peirce')]
                if peirce_files:
                    try:
                        peirce_data = json.loads(zf.read(peirce_files[0]).decode('utf-8'))
                        title = peirce_data.get('title', '').strip()
                        if title:
                            # Slugify: lowercase, spaces to underscores, strip non-alphanum
                            import re as _re
                            expedition_name = _re.sub(r'[^a-z0-9_]', '', title.lower().replace(' ', '_'))
                        expedition_id = peirce_data.get('expedition_id')
                    except Exception:
                        pass

                srf_files = [n for n in names if n.endswith('.srf') or n.endswith('.srf.json')]
                if not srf_files:
                    raise HTTPException(status_code=422, detail="Zip contains no .srf files.")

                records_raw = []
                for name in srf_files:
                    try:
                        record_text = zf.read(name).decode('utf-8')
                        records_raw.append(json.loads(record_text))
                    except Exception:
                        continue
        except zipfile.BadZipFile:
            raise HTTPException(status_code=400, detail="Could not read zip file.")
    else:
        # ── JSON — single record, bundle, or array ────────────────────────────
        expedition_name = None
        expedition_id   = None
        try:
            payload = json.loads(content.decode("utf-8"))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Could not parse SRF bundle: {e}")

        if isinstance(payload, list):
            records_raw = payload
        elif isinstance(payload, dict) and "records" in payload:
            # Reckoner bundle format — use collection_name if present
            records_raw = payload["records"]
            if not expedition_name and payload.get("collection_name"):
                expedition_name = payload["collection_name"]
        elif isinstance(payload, dict) and "srf_version" in payload:
            records_raw = [payload]
        else:
            raise HTTPException(
                status_code=422,
                detail="Expected an SRF bundle with a 'records' array, a plain JSON array of SRF records, or a single SRF record."
            )

    if not records_raw:
        raise HTTPException(status_code=400, detail="Bundle contains no records.")

    imported  = 0
    skipped   = 0
    duplicate = 0
    schema    = None

    for raw in records_raw:
        try:
            record = SRFRecord.from_dict(raw)
        except Exception as e:
            skipped += 1
            continue

        # Use expedition name as substrate key if available, otherwise lens_id
        substrate_key = expedition_name if expedition_name else record.lens_id

        if schema is None:
            schema = substrate_key

        substrate = _get_or_create_srf_substrate(record.lens_id, substrate_key)

        # Check for duplicate
        existing = substrate._conn.execute(
            "SELECT COUNT(*) FROM snf_spoke WHERE entity_id = ?",
            [record.entity_id]
        ).fetchone()[0]

        if existing > 0:
            duplicate += 1
            continue

        rows       = record.to_snf_rows()
        spoke_rows = rows.get("spoke_rows", [])

        if not spoke_rows:
            skipped += 1
            continue

        substrate._conn.executemany(
            "INSERT INTO snf_spoke "
            "(entity_id, dimension, semantic_key, value, coordinate, lens_id, "
            "nucleus_field, nucleus_value, nucleus_prefix) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    r["entity_id"],
                    r["dimension"].lower(),
                    r["semantic_key"],
                    r["value"],
                    r["coordinate"],
                    record.lens_id,  # always the semantic lens — substrate_key is registry-only
                    r.get("nucleus_field"),
                    r.get("nucleus_value"),
                    r.get("nucleus_prefix"),
                )
                for r in spoke_rows
            ]
        )

        _registry_meta[substrate_key]["entity_count"]       = substrate.entity_count()
        _registry_meta[substrate_key]["dimensions"]         = substrate.dimensions()
        _registry_meta[substrate_key]["translator_version"] = record.translator_version

        _persist_srf_record(record, substrate_key)
        imported += 1

    return {
        "schema":       schema or "unknown",
        "entity_count": imported,
        "imported":     imported,
        "skipped":      skipped,
        "duplicate":    duplicate,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Set-operation and .peirce-bundle save endpoints
# ─────────────────────────────────────────────────────────────────────────────

class SaveSetOpRequest(BaseModel):
    format:  str        # 'json' (xlsx not yet supported server-side)
    payload: dict


@app.post("/api/export/save-setop")
async def export_save_setop(req: SaveSetOpRequest):
    """Save a set-operation result (diff / union / intersect) to disk."""
    filename = f"reckoner_setop_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    path     = EXPORT_DIR / filename
    path.write_text(json.dumps(req.payload, indent=2, default=str), encoding="utf-8")
    return {"saved": True, "path": str(path), "filename": filename}


class SavePeirceBundleRequest(BaseModel):
    bundle:   dict
    filename: Optional[str] = None


@app.post("/api/export/save-peirce-bundle")
async def export_save_peirce_bundle(req: SavePeirceBundleRequest):
    """Save an arbitrary .peirce bundle dict to disk."""
    filename = req.filename or f"reckoner_{datetime.now().strftime('%Y%m%d_%H%M%S')}.peirce"
    # Sanitise filename
    safe = "".join(c for c in filename if c.isalnum() or c in "._- ").strip()
    path = EXPORT_DIR / (safe or filename)
    path.write_text(json.dumps(req.bundle, indent=2, default=str), encoding="utf-8")
    return {"saved": True, "path": str(path), "filename": path.name}


# ─────────────────────────────────────────────────────────────────────────────
# Crosswalk
#
# The continuity layer. Two modes:
#
#   pre-ingest  — raw CSV/TSV/XLSX files not yet in Model Builder.
#                 Output: merged file ready for MB ingest as one source.
#
#   post-ingest — substrates already in Reckoner/SNF.
#                 Output: SRF assertion record; set ops work immediately.
#
# Machine suggests. Human authorizes. Always.
#
# Endpoints:
#   POST /api/crosswalk/upload              — upload a raw file, get a source_id
#   POST /api/crosswalk/session             — start session (files or substrates)
#   GET  /api/crosswalk/session/{id}        — get session state
#   GET  /api/crosswalk/sessions            — list active sessions
#   POST /api/crosswalk/assign-ids          — declare match fields, load entities
#   POST /api/crosswalk/candidates          — run tantivy-py matching
#   POST /api/crosswalk/assert              — record accept / reject / provisional
#   GET  /api/crosswalk/unmatched/{id}      — entities with no candidate
#   POST /api/crosswalk/export              — emit merged file (pre) or SRF (post)
# ─────────────────────────────────────────────────────────────────────────────

import uuid
import sqlite3
import csv as _csv_module
import tempfile
from datetime import timezone
from typing import Literal

# ── tantivy-py ────────────────────────────────────────────────────────────────
try:
    import tantivy as _tantivy
    _TANTIVY_AVAILABLE = True
except ImportError:
    _TANTIVY_AVAILABLE = False
    print("[crosswalk] tantivy-py not available — manual matching only")

# ── Assertion store ───────────────────────────────────────────────────────────
CROSSWALK_DB_PATH = os.path.join(SUBSTRATES_DIR, ".crosswalk_assertions.db")
CROSSWALK_UPLOAD_DIR = os.path.join(SUBSTRATES_DIR, ".crosswalk_uploads")
os.makedirs(CROSSWALK_UPLOAD_DIR, exist_ok=True)


def _cw_db() -> sqlite3.Connection:
    conn = sqlite3.connect(CROSSWALK_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cw_sessions (
            session_id    TEXT PRIMARY KEY,
            created_at    TEXT NOT NULL,
            source_a_id   TEXT NOT NULL,
            source_b_id   TEXT NOT NULL,
            source_a_mode TEXT NOT NULL DEFAULT 'substrate',
            source_b_mode TEXT NOT NULL DEFAULT 'substrate',
            prefix        TEXT NOT NULL DEFAULT 'entity',
            match_fields  TEXT,
            status        TEXT NOT NULL DEFAULT 'active'
        );
        CREATE TABLE IF NOT EXISTS cw_assertions (
            assertion_id TEXT PRIMARY KEY,
            session_id   TEXT NOT NULL,
            a_id         TEXT NOT NULL,
            b_id         TEXT NOT NULL,
            composite_id TEXT NOT NULL,
            status       TEXT NOT NULL,
            asserted_at  TEXT NOT NULL,
            confidence   REAL,
            manual       INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS cw_raw_sources (
            source_id   TEXT PRIMARY KEY,
            filename    TEXT NOT NULL,
            filepath    TEXT NOT NULL,
            uploaded_at TEXT NOT NULL,
            row_count   INTEGER
        );
    """)
    conn.commit()
    return conn


# ── In-memory session store ───────────────────────────────────────────────────
_cw_sessions: Dict[str, dict] = {}
_cw_raw_sources: Dict[str, dict] = {}


# ── Pydantic models ───────────────────────────────────────────────────────────

class CWMatchField(BaseModel):
    field_a: str          # column name in source A
    field_b: str          # column name in source B (may differ)
    weight:  float = 1.0


class CWSessionRequest(BaseModel):
    source_a:      str
    source_b:      str
    source_a_mode: Literal["substrate", "file"] = "substrate"
    source_b_mode: Literal["substrate", "file"] = "substrate"
    prefix:        Optional[str] = None


class CWAssignIdsRequest(BaseModel):
    session_id:   str
    match_fields: List[CWMatchField]


class CWCandidatesRequest(BaseModel):
    session_id:       str
    confidence_floor: float = 0.40
    confidence_auto:  float = 0.90


class CWAssertRequest(BaseModel):
    session_id: str
    a_id:       str
    b_id:       str
    status:     Literal["accepted", "rejected", "provisional"]
    manual:     bool = False


class CWExportRequest(BaseModel):
    session_id: str
    format:     Literal["srf", "csv", "merged_csv", "merged_xlsx"] = "srf"


# ── Raw file loader ───────────────────────────────────────────────────────────

def _cw_read_raw_file(filepath: str):
    import pandas as pd
    ext = filepath.rsplit(".", 1)[-1].lower()
    if ext in ("xlsx", "xls"):
        return pd.read_excel(filepath, engine="openpyxl")
    return pd.read_csv(filepath, sep=None, engine="python", dtype=str)


def _cw_load_raw_entities(source_id: str, field_names: List[str]) -> List[dict]:
    raw = _cw_raw_sources.get(source_id)
    if not raw:
        raise HTTPException(status_code=404, detail=f"Raw source not found: {source_id}")
    import pandas as pd
    df = raw["df_cache"] if raw.get("df_cache") is not None else _cw_read_raw_file(raw["filepath"])
    raw["df_cache"] = df
    entities = []
    for idx, row in df.iterrows():
        # Load ALL columns — match fields are used for matching,
        # but the full row carries through to the export
        facts = {}
        for col in df.columns:
            if pd.notna(row[col]) and str(row[col]).strip():
                facts[col] = str(row[col])
        display_label = next(
            (facts[f] for f in field_names if f in facts and facts[f]),
            f"row_{idx}"
        ) if field_names else f"row_{idx}"
        entities.append({
            "local_id":      f"row_{idx}",
            "display_label": display_label,
            "facts":         facts,
            "row_index":     idx,
        })
    return entities


def _cw_load_substrate_entities(substrate_id: str, field_names: List[str]) -> List[dict]:
    substrate = _registry.get(substrate_id)
    if not substrate:
        raise HTTPException(status_code=404, detail=f"Substrate not found: {substrate_id}")
    conn    = substrate._conn
    lens_id = substrate.lens_id
    id_rows = conn.execute(
        "SELECT DISTINCT entity_id FROM snf_spoke WHERE lens_id = ?", [lens_id]
    ).fetchall()
    all_ids = [r[0] for r in id_rows]
    if not all_ids:
        return []
    facts_by_entity: Dict[str, Dict[str, str]] = {eid: {} for eid in all_ids}
    if field_names:
        placeholders = ", ".join("?" * len(field_names))
        fact_rows = conn.execute(
            f"SELECT entity_id, semantic_key, value FROM snf_spoke "
            f"WHERE lens_id = ? AND semantic_key IN ({placeholders}) "
            f"ORDER BY entity_id, semantic_key",
            [lens_id] + field_names
        ).fetchall()
        for row in fact_rows:
            eid, key, val = row[0], row[1], row[2]
            if eid in facts_by_entity and key not in facts_by_entity[eid]:
                facts_by_entity[eid][key] = str(val)
    entities = []
    for eid in all_ids:
        facts = facts_by_entity[eid]
        display_label = next(
            (facts[f] for f in field_names if f in facts and facts[f]), eid
        ) if field_names else eid
        entities.append({"local_id": eid, "display_label": display_label, "facts": facts})
    return entities


def _cw_load_entities(source_id: str, mode: str, field_names: List[str]) -> List[dict]:
    if mode == "file":
        return _cw_load_raw_entities(source_id, field_names)
    return _cw_load_substrate_entities(source_id, field_names)


def _cw_assign_synthetic_ids(entities: List[dict], prefix: str, source: str) -> List[dict]:
    for i, e in enumerate(entities, start=1):
        e["synthetic_id"] = f"{prefix}_{source}_{i:03d}"
    return entities


# ── Tantivy matching ──────────────────────────────────────────────────────────

def _cw_run_tantivy(entities_a, entities_b, match_fields, confidence_floor, confidence_auto):
    if not _TANTIVY_AVAILABLE or not match_fields:
        return []
    try:
        # Build schema — one search field per match pair using field_b names for the index
        sb = _tantivy.SchemaBuilder()
        sb.add_text_field("entity_key", stored=True)
        seen_fields = set()
        for mf in match_fields:
            if mf.field_b not in seen_fields:
                sb.add_text_field(mf.field_b, stored=True)
                seen_fields.add(mf.field_b)
        schema = sb.build()
        _cw_idx_dir = tempfile.mkdtemp(prefix="cw_tantivy_")
        index  = _tantivy.Index(schema, path=_cw_idx_dir)
        writer = index.writer(heap_size=50_000_000)
        # Index source B using field_b column names
        for e in entities_b:
            bid = e.get("synthetic_id") or e["local_id"]
            doc = _tantivy.Document()
            doc.add_text("entity_key", bid)
            for mf in match_fields:
                val = e["facts"].get(mf.field_b, "")
                if val:
                    doc.add_text(mf.field_b, str(val))
            writer.add_document(doc)
        writer.commit()
        index.reload()
        searcher = index.searcher()
        total_w  = sum(mf.weight for mf in match_fields) or 1.0
        norm     = {mf.field_b: mf.weight / total_w for mf in match_fields}
        best_per_b: Dict[str, dict] = {}
        for e_a in entities_a:
            aid          = e_a.get("synthetic_id") or e_a["local_id"]
            field_scores: Dict[str, float] = {}
            best_bid     = None
            best_score   = 0.0
            # Query using field_a value from source A against field_b index
            for mf in match_fields:
                val = e_a["facts"].get(mf.field_a, "")
                if not val:
                    continue
                try:
                    q    = index.parse_query(str(val), [mf.field_b])
                    hits = searcher.search(q, limit=5).hits
                except Exception as qex:
                    print(f"[crosswalk] query error for field {mf.field_b}: {qex}")
                    continue
                if hits:
                    top_score, top_addr = hits[0]
                    doc      = searcher.doc(top_addr)
                    bid      = doc.get_first("entity_key")
                    weighted = top_score * norm[mf.field_b]
                    field_scores[mf.field_b] = round(top_score, 3)
                    if weighted > best_score:
                        best_score = weighted
                        best_bid   = bid
            if best_bid and best_score >= confidence_floor:
                candidate = {
                    "a_id":         aid,
                    "b_id":         best_bid,
                    "confidence":   round(best_score, 3),
                    "auto_suggest": best_score >= confidence_auto,
                    "field_scores": field_scores,
                }
                existing = best_per_b.get(best_bid)
                if not existing or candidate["confidence"] > existing["confidence"]:
                    best_per_b[best_bid] = candidate
        results = sorted(best_per_b.values(), key=lambda c: c["confidence"], reverse=True)
        print(f"[crosswalk] tantivy: {len(entities_a)} A / {len(entities_b)} B -> {len(results)} candidates")
        if entities_a:
            print(f"[crosswalk] sample A entity facts: {entities_a[0].get('facts')}")
        if not results:
            print("[crosswalk] zero candidates — check field names exactly match column headers in both files")
        return results
    except Exception as ex:
        print(f"[crosswalk] Tantivy error: {ex}")
        return []


# ── Done signal ───────────────────────────────────────────────────────────────

def _cw_done_signal(s: dict, accepted: list, provisional: list) -> dict:
    ua, ub = len(s.get("unmatched_a", [])), len(s.get("unmatched_b", []))
    na, nb = s["source_a_id"], s["source_b_id"]
    is_pre = s["source_a_mode"] == "file" or s["source_b_mode"] == "file"
    next_step = ("Download the merged file and ingest it in Model Builder."
                 if is_pre else
                 "These substrates can now be compared in Reckoner.")
    msg = (
        f"{len(accepted)} assertion{'s' if len(accepted) != 1 else ''} confirmed."
        + (f" {len(provisional)} marked provisional." if provisional else "")
        + f" {ua} unmatched in {na}. {ub} unmatched in {nb}. {next_step}"
    )
    return {
        "accepted_count":     len(accepted),
        "provisional_count":  len(provisional),
        "unmatched_a_count":  ua,
        "unmatched_b_count":  ub,
        "source_a_id":        na,
        "source_b_id":        nb,
        "mode":               "pre_ingest" if is_pre else "post_ingest",
        "ready_for_reckoner": bool(accepted) and not is_pre,
        "ready_for_mb":       bool(accepted) and is_pre,
        "message":            msg,
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@app.post("/api/crosswalk/upload")
async def cw_upload_file(file: UploadFile = File(...)):
    """Upload a raw CSV, TSV, or XLSX file as a Crosswalk source."""
    filename = file.filename or "upload"
    ext      = filename.rsplit(".", 1)[-1].lower() if "." in filename else "csv"
    if ext not in ("csv", "tsv", "xlsx", "xls"):
        raise HTTPException(status_code=400, detail=f"Unsupported file type: .{ext}. Use CSV, TSV, or XLSX.")
    source_id = str(uuid.uuid4())
    filepath  = os.path.join(CROSSWALK_UPLOAD_DIR, f"{source_id}.{ext}")
    content   = await file.read()
    with open(filepath, "wb") as f:
        f.write(content)
    try:
        import pandas as pd
        df        = _cw_read_raw_file(filepath)
        columns   = list(df.columns)
        row_count = len(df)
    except Exception as e:
        os.unlink(filepath)
        raise HTTPException(status_code=400, detail=f"Could not read file: {e}")
    now = datetime.now(timezone.utc).isoformat()
    _cw_raw_sources[source_id] = {
        "source_id":   source_id,
        "filename":    filename,
        "filepath":    filepath,
        "uploaded_at": now,
        "row_count":   row_count,
        "columns":     columns,
        "df_cache":    df,
    }
    db = _cw_db()
    db.execute(
        "INSERT INTO cw_raw_sources (source_id, filename, filepath, uploaded_at, row_count) VALUES (?,?,?,?,?)",
        (source_id, filename, filepath, now, row_count)
    )
    db.commit()
    db.close()
    return {"source_id": source_id, "filename": filename,
            "row_count": row_count, "columns": columns, "mode": "file"}


@app.post("/api/crosswalk/session")
async def cw_start_session(req: CWSessionRequest):
    """Start a Crosswalk session. Sources can be substrates or uploaded files, mixed freely."""
    if req.source_a_mode == "substrate" and req.source_a not in _registry:
        raise HTTPException(status_code=404,
            detail=f"Substrate not found: {req.source_a}. Available: {list(_registry.keys())}")
    if req.source_a_mode == "file" and req.source_a not in _cw_raw_sources:
        raise HTTPException(status_code=404,
            detail=f"Raw source not found: {req.source_a}. Upload via /api/crosswalk/upload first.")
    if req.source_b_mode == "substrate" and req.source_b not in _registry:
        raise HTTPException(status_code=404,
            detail=f"Substrate not found: {req.source_b}. Available: {list(_registry.keys())}")
    if req.source_b_mode == "file" and req.source_b not in _cw_raw_sources:
        raise HTTPException(status_code=404,
            detail=f"Raw source not found: {req.source_b}. Upload via /api/crosswalk/upload first.")
    session_id = str(uuid.uuid4())
    now        = datetime.now(timezone.utc).isoformat()
    prefix     = req.prefix or "entity"
    s = {
        "session_id":       session_id,
        "created_at":       now,
        "source_a_id":      req.source_a,
        "source_b_id":      req.source_b,
        "source_a_mode":    req.source_a_mode,
        "source_b_mode":    req.source_b_mode,
        "prefix":           prefix,
        "status":           "pending_id_assignment",
        "match_fields":     [],
        "entities_a":       [],
        "entities_b":       [],
        "candidates":       [],
        "unmatched_a":      [],
        "unmatched_b":      [],
        "assertion_counts": {"accepted": 0, "rejected": 0, "provisional": 0},
    }
    _cw_sessions[session_id] = s
    db = _cw_db()
    db.execute(
        "INSERT INTO cw_sessions (session_id, created_at, source_a_id, source_b_id, source_a_mode, source_b_mode, prefix) VALUES (?,?,?,?,?,?,?)",
        (session_id, now, req.source_a, req.source_b, req.source_a_mode, req.source_b_mode, prefix)
    )
    db.commit()
    db.close()
    def _display(sid, mode):
        if mode == "file":
            return _cw_raw_sources.get(sid, {}).get("filename", sid)
        return sid
    return {
        "session_id":       session_id,
        "status":           "pending_id_assignment",
        "source_a_id":      req.source_a,
        "source_a_mode":    req.source_a_mode,
        "source_a_display": _display(req.source_a, req.source_a_mode),
        "source_b_id":      req.source_b,
        "source_b_mode":    req.source_b_mode,
        "source_b_display": _display(req.source_b, req.source_b_mode),
        "prefix":           prefix,
        "next_step":        "POST /api/crosswalk/assign-ids",
    }


@app.get("/api/crosswalk/sessions")
async def cw_list_sessions():
    return {"sessions": [
        {"session_id": s["session_id"], "source_a_id": s["source_a_id"],
         "source_b_id": s["source_b_id"], "status": s["status"],
         "created_at": s["created_at"], "assertion_counts": s["assertion_counts"],
         "source_a_mode": s["source_a_mode"], "source_b_mode": s["source_b_mode"]}
        for s in _cw_sessions.values()
    ]}


@app.get("/api/crosswalk/session/{session_id}")
async def cw_get_session(session_id: str):
    s = _cw_sessions.get(session_id)
    if not s:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")
    return {
        "session_id": s["session_id"], "status": s["status"],
        "source_a_id": s["source_a_id"], "source_a_mode": s["source_a_mode"],
        "source_b_id": s["source_b_id"], "source_b_mode": s["source_b_mode"],
        "prefix": s["prefix"], "match_fields": s["match_fields"],
        "entity_count_a": len(s["entities_a"]), "entity_count_b": len(s["entities_b"]),
        "candidate_count": len(s["candidates"]), "assertion_counts": s["assertion_counts"],
    }


@app.post("/api/crosswalk/assign-ids")
async def cw_assign_ids(req: CWAssignIdsRequest):
    s = _cw_sessions.get(req.session_id)
    if not s:
        raise HTTPException(status_code=404, detail=f"Session not found: {req.session_id}")
    fields_a = list({mf.field_a for mf in req.match_fields})
    fields_b = list({mf.field_b for mf in req.match_fields})
    entities_a    = _cw_load_entities(s["source_a_id"], s["source_a_mode"], fields_a)
    entities_b    = _cw_load_entities(s["source_b_id"], s["source_b_mode"], fields_b)
    entities_a    = _cw_assign_synthetic_ids(entities_a, s["prefix"], "a")
    entities_b    = _cw_assign_synthetic_ids(entities_b, s["prefix"], "b")
    s["entities_a"]   = entities_a
    s["entities_b"]   = entities_b
    s["match_fields"] = [mf.model_dump() for mf in req.match_fields]
    s["status"]       = "ready_for_candidates"
    db = _cw_db()
    db.execute(
        "UPDATE cw_sessions SET match_fields=?, status='ready_for_candidates' WHERE session_id=?",
        (json.dumps(s["match_fields"]), req.session_id)
    )
    db.commit()
    db.close()
    return {
        "session_id": req.session_id, "status": "ready_for_candidates",
        "entity_count_a": len(entities_a), "entity_count_b": len(entities_b),
        "match_fields": s["match_fields"],
        "sample_a": [{"synthetic_id": e["synthetic_id"], "display_label": e["display_label"], "facts": e["facts"]} for e in entities_a[:3]],
        "sample_b": [{"synthetic_id": e["synthetic_id"], "display_label": e["display_label"], "facts": e["facts"]} for e in entities_b[:3]],
        "next_step": "POST /api/crosswalk/candidates",
    }


@app.post("/api/crosswalk/candidates")
async def cw_candidates(req: CWCandidatesRequest):
    s = _cw_sessions.get(req.session_id)
    if not s:
        raise HTTPException(status_code=404, detail=f"Session not found: {req.session_id}")
    if s["status"] not in ("ready_for_candidates", "review_in_progress"):
        raise HTTPException(status_code=400, detail=f"Session not ready. Status: {s['status']}")
    mfs        = [CWMatchField(**mf) for mf in s["match_fields"]]
    candidates = _cw_run_tantivy(s["entities_a"], s["entities_b"], mfs,
                                  req.confidence_floor, req.confidence_auto)
    matched_a  = {c["a_id"] for c in candidates}
    matched_b  = {c["b_id"] for c in candidates}
    unmatched_a = [
        {"id": e.get("synthetic_id") or e["local_id"], "display_label": e["display_label"], "facts": e["facts"]}
        for e in s["entities_a"] if (e.get("synthetic_id") or e["local_id"]) not in matched_a
    ]
    unmatched_b = [
        {"id": e.get("synthetic_id") or e["local_id"], "display_label": e["display_label"], "facts": e["facts"]}
        for e in s["entities_b"] if (e.get("synthetic_id") or e["local_id"]) not in matched_b
    ]
    s["candidates"]  = candidates
    s["unmatched_a"] = unmatched_a
    s["unmatched_b"] = unmatched_b
    s["status"]      = "review_in_progress"
    a_map = {(e.get("synthetic_id") or e["local_id"]): e for e in s["entities_a"]}
    b_map = {(e.get("synthetic_id") or e["local_id"]): e for e in s["entities_b"]}
    enriched = [{
        **c,
        "a_label": a_map.get(c["a_id"], {}).get("display_label", c["a_id"]),
        "a_facts": a_map.get(c["a_id"], {}).get("facts", {}),
        "b_label": b_map.get(c["b_id"], {}).get("display_label", c["b_id"]),
        "b_facts": b_map.get(c["b_id"], {}).get("facts", {}),
    } for c in candidates]
    return {
        "session_id": req.session_id, "status": "review_in_progress",
        "tantivy_available": _TANTIVY_AVAILABLE,
        "candidate_count": len(candidates),
        "auto_suggest_count": sum(1 for c in candidates if c["auto_suggest"]),
        "unmatched_a_count": len(unmatched_a), "unmatched_b_count": len(unmatched_b),
        "candidates": enriched,
    }


@app.get("/api/crosswalk/unmatched/{session_id}")
async def cw_unmatched(session_id: str):
    s = _cw_sessions.get(session_id)
    if not s:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")
    return {"session_id": session_id,
            "unmatched_a": s.get("unmatched_a", []),
            "unmatched_b": s.get("unmatched_b", [])}


@app.post("/api/crosswalk/assert")
async def cw_assert(req: CWAssertRequest):
    s = _cw_sessions.get(req.session_id)
    if not s:
        raise HTTPException(status_code=404, detail=f"Session not found: {req.session_id}")
    confidence = next(
        (c["confidence"] for c in s.get("candidates", [])
         if c["a_id"] == req.a_id and c["b_id"] == req.b_id), None
    )
    s["assertion_counts"][req.status] += 1
    if req.status == "rejected":
        return {"session_id": req.session_id, "status": "rejected",
                "a_id": req.a_id, "b_id": req.b_id,
                "message": "Rejected. No assertion record created."}
    a_entity     = next((e for e in s["entities_a"] if e.get("synthetic_id") == req.a_id or e["local_id"] == req.a_id), None)
    b_entity     = next((e for e in s["entities_b"] if e.get("synthetic_id") == req.b_id or e["local_id"] == req.b_id), None)
    a_nucleus    = a_entity["local_id"] if a_entity else req.a_id
    b_nucleus    = b_entity["local_id"] if b_entity else req.b_id
    composite_id = f"{a_nucleus}:{b_nucleus}"
    assertion_id = str(uuid.uuid4())
    asserted_at  = datetime.now(timezone.utc).isoformat()
    db = _cw_db()
    db.execute(
        "INSERT INTO cw_assertions (assertion_id,session_id,a_id,b_id,composite_id,status,asserted_at,confidence,manual) VALUES (?,?,?,?,?,?,?,?,?)",
        (assertion_id, req.session_id, a_nucleus, b_nucleus, composite_id,
         req.status, asserted_at, confidence, 1 if req.manual else 0)
    )
    db.commit()
    db.close()
    return {"assertion_id": assertion_id, "session_id": req.session_id,
            "a_id": a_nucleus, "b_id": b_nucleus, "composite_id": composite_id,
            "status": req.status, "asserted_at": asserted_at,
            "confidence": confidence, "manual": req.manual}


@app.post("/api/crosswalk/export")
async def cw_export(req: CWExportRequest):
    """
    Export session results.

    Post-ingest (substrates): srf or csv
    Pre-ingest (files):       merged_csv, merged_xlsx, or srf for audit trail

    merged_csv / merged_xlsx: all rows from both sources combined,
    _composite_id column added. Ready for Model Builder as a single source.
    """
    s = _cw_sessions.get(req.session_id)
    if not s:
        raise HTTPException(status_code=404, detail=f"Session not found: {req.session_id}")
    db = _cw_db()
    rows = db.execute(
        "SELECT * FROM cw_assertions WHERE session_id=? AND status IN ('accepted','provisional') ORDER BY asserted_at",
        (req.session_id,)
    ).fetchall()
    db.close()
    assertions  = [dict(r) for r in rows]
    accepted    = [a for a in assertions if a["status"] == "accepted"]
    provisional = [a for a in assertions if a["status"] == "provisional"]
    done        = _cw_done_signal(s, accepted, provisional)

    if req.format in ("merged_csv", "merged_xlsx"):
        import pandas as pd

        # Build lookup: local_id -> entity for fast access
        a_by_id = {e["local_id"]: e for e in s["entities_a"]}
        b_by_id = {e["local_id"]: e for e in s["entities_b"]}

        # Prefix columns so same-named columns from A and B don't collide
        # Use filename (without extension) for file sources, substrate name for substrate sources
        def _src_label(source_id, mode):
            if mode == "file":
                raw = _cw_raw_sources.get(source_id, {})
                name = raw.get("filename", source_id)
                name = name.rsplit(".", 1)[0] if "." in name else name
            else:
                name = source_id
            return name.replace(" ", "_").replace("-", "_")[:24]

        src_a = _src_label(s["source_a_id"], s["source_a_mode"])
        src_b = _src_label(s["source_b_id"], s["source_b_mode"])

        records = []
        counter = 1
        prefix  = s["prefix"] or "entity"

        # Matched pairs — one row per assertion, columns from both sources
        matched_a_ids = set()
        matched_b_ids = set()
        for a in accepted + provisional:
            e_a = a_by_id.get(a["a_id"], {})
            e_b = b_by_id.get(a["b_id"], {})
            matched_a_ids.add(a["a_id"])
            matched_b_ids.add(a["b_id"])
            composite_id = f"{prefix}_{counter:03d}"
            counter += 1
            row = {"_composite_id": composite_id, "_match_status": a["status"]}
            for k, v in e_a.get("facts", {}).items():
                row[f"{src_a}__{k}"] = v
            for k, v in e_b.get("facts", {}).items():
                row[f"{src_b}__{k}"] = v
            records.append(row)

        # Unmatched from A — their own columns only
        for e in s["entities_a"]:
            if e["local_id"] not in matched_a_ids:
                composite_id = f"{prefix}_{counter:03d}"
                counter += 1
                row = {"_composite_id": composite_id, "_match_status": "unmatched"}
                for k, v in e.get("facts", {}).items():
                    row[f"{src_a}__{k}"] = v
                records.append(row)

        # Unmatched from B — their own columns only
        for e in s["entities_b"]:
            if e["local_id"] not in matched_b_ids:
                composite_id = f"{prefix}_{counter:03d}"
                counter += 1
                row = {"_composite_id": composite_id, "_match_status": "unmatched"}
                for k, v in e.get("facts", {}).items():
                    row[f"{src_b}__{k}"] = v
                records.append(row)

        merged = pd.DataFrame(records)

        if req.format == "merged_csv":
            return {"format": "merged_csv", "session_id": req.session_id,
                    "csv": merged.to_csv(index=False), "row_count": len(merged), "done_signal": done}
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            merged.to_excel(tmp.name, index=False, engine="openpyxl")
            import base64
            xlsx_b64 = base64.b64encode(open(tmp.name, "rb").read()).decode()
        return {"format": "merged_xlsx", "session_id": req.session_id,
                "xlsx_b64": xlsx_b64, "row_count": len(merged), "done_signal": done}

    if req.format == "srf":
        srf = {
            "srf_version": "1.0", "record_type": "crosswalk_assertion_set",
            "session_id": req.session_id,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "source_a": s["source_a_id"], "source_a_mode": s["source_a_mode"],
            "source_b": s["source_b_id"], "source_b_mode": s["source_b_mode"],
            "prefix": s["prefix"], "match_fields": s["match_fields"],
            "assertions": assertions, "summary": done,
        }
        return {"format": "srf", "session_id": req.session_id, "srf": srf, "done_signal": done}

    out = io.StringIO()
    w   = _csv_module.writer(out)
    w.writerow(["composite_id", "a_id", "b_id", "status", "confidence", "manual", "asserted_at"])
    for a in assertions:
        w.writerow([a["composite_id"], a["a_id"], a["b_id"],
                    a["status"], a["confidence"], a["manual"], a["asserted_at"]])
    return {"format": "csv", "session_id": req.session_id,
            "csv": out.getvalue(), "done_signal": done}


# ─────────────────────────────────────────────────────────────────────────────
# Static frontend — MUST be registered after all API routes
# so the catch-all doesn't intercept /api/* requests.
# ─────────────────────────────────────────────────────────────────────────────

from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse as _FileResponse

_DIST_DIR = Path(__file__).parent / "dist"

if _DIST_DIR.exists():
    app.mount(
        "/assets",
        StaticFiles(directory=str(_DIST_DIR / "assets")),
        name="assets",
    )

    @app.get("/")
    async def serve_frontend_root():
        return _FileResponse(str(_DIST_DIR / "index.html"))

    @app.get("/{full_path:path}")
    async def serve_frontend_catchall(full_path: str):
        return _FileResponse(str(_DIST_DIR / "index.html"))

    print("[api] Serving React frontend from dist/")
else:
    print("[api] No dist/ folder — frontend not served. Run npm run build.")


# ─────────────────────────────────────────────────────────────────────────────
# Standalone entry point — MUST be last. uvicorn.run() blocks; any route
# definitions below it will never register when run as python reckoner_api.py.
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Reckoner API — Python backend")
    print("  Model Builder endpoints: /api/mb/*")
    print("=" * 60)
    print(describe_settings())
    print(f"  Docs:         http://localhost:{PORT}/docs")
    print("=" * 60)

    # HOST defaults to 127.0.0.1 — loopback only. It was "0.0.0.0", which
    # binds every network interface and makes the substrate readable by
    # anything on the same network. A desktop application does not need that.
    # Set HOST=0.0.0.0 in .env if you deliberately want remote access.
    uvicorn.run(
        app,
        host=HOST,
        port=PORT,
        reload=False,
        log_level="info" if DEBUG else "warning",
    )
