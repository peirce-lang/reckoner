"""
model_builder_api.py — Model Builder API Router

FastAPI router mounted into reckoner_api.py at /api/mb.

Endpoints:
    POST /api/mb/upload        Upload a CSV or Excel file. Returns columns + samples.
    POST /api/mb/introspect    Introspect a live Postgres table via SQLAlchemy (read-only).
    POST /api/mb/review        Run pre-ingest review on mapped columns. Returns flags.
    POST /api/mb/compile       Compile a BuildSpec → BuildResult + download artifact.

Session store:
    In-memory dict { token → SessionData } keyed by UUID4.
    Tokens expire after SESSION_TTL_SECONDS (default 2 hours).
    Holds the parsed DataFrame and mapping between wizard steps so the
    file/connection is not re-sent on every request.

Tantivy (variant candidate detection):
    Used in /review to find near-duplicate coordinate values within the same
    semantic_key. Builds an in-memory index per semantic_key, queries each
    value against it, surfaces candidates with token overlap above threshold.

    INVARIANT (D12): This is the same resolver that /resolve will eventually use.
    When /resolve is built, it must use this same implementation — not a separate one.

    Tantivy fallback: if tantivy is not installed, falls back to a pure-Python
    implementation (token overlap + Levenshtein distance). The fallback is
    clearly marked in flag details so the caller knows which engine ran.

Integration:
    In reckoner_api.py, add near the bottom:

        from model_builder_api import router as mb_router
        app.include_router(mb_router)

Dependencies:
    pip install fastapi uvicorn pandas openpyxl sqlalchemy psycopg2-binary
    pip install tantivy          # recommended — variant detection
    pip install snf-peirce       # compile_data() for artifact emission
"""

from __future__ import annotations

import io
import os
import re
import time
import uuid
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

try:
    from fastapi import APIRouter, HTTPException, UploadFile, File
    from fastapi.responses import FileResponse
    from pydantic import BaseModel
except ImportError:
    raise ImportError("FastAPI required. pip install fastapi")

try:
    import pandas as pd
except ImportError:
    raise ImportError("pandas required. pip install pandas openpyxl")

# ─────────────────────────────────────────────────────────────────────────────
# Optional dependencies — fail gracefully with clear messages
# ─────────────────────────────────────────────────────────────────────────────

try:
    import tantivy
    TANTIVY_AVAILABLE = True
except ImportError:
    TANTIVY_AVAILABLE = False

try:
    from sqlalchemy import create_engine, text, inspect as sa_inspect
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

try:
    from snf_peirce import compile_data
    SNF_PEIRCE_AVAILABLE = True
except ImportError:
    SNF_PEIRCE_AVAILABLE = False

try:
    from osi_parser import parse_osi_file, parse_json_array, export_snf_as_osi
    OSI_PARSER_AVAILABLE = True
except ImportError:
    OSI_PARSER_AVAILABLE = False

try:
    from dbt_parser import parse_dbt_schema
    DBT_PARSER_AVAILABLE = True
except ImportError:
    DBT_PARSER_AVAILABLE = False

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

SESSION_TTL_SECONDS   = 60 * 60 * 2   # 2 hours
SAMPLE_ROWS           = 10            # rows shown as samples in column mapping
VARIANT_SCORE_THRESH  = 0.5           # Tantivy score threshold for variant candidates
SINGLETON_MAX_COUNT   = 1             # values appearing <= this are "singletons"
NULL_PCT_WARN_THRESH  = 0.20          # warn if > 20% of values are null
OUTPUT_DIR            = Path(tempfile.gettempdir()) / "model_builder_artifacts"
OUTPUT_DIR.mkdir(exist_ok=True)
SRF_IMPORTS_DIR       = Path(os.environ.get("SNF_SRF_IMPORTS_DIR", "./substrates/srf_imports"))

# ─────────────────────────────────────────────────────────────────────────────
# Session store
# ─────────────────────────────────────────────────────────────────────────────

class SessionData:
    def __init__(
        self,
        df:          pd.DataFrame,
        source_info: Dict[str, Any],
        columns:     List[Dict],
    ):
        self.df          = df
        self.source_info = source_info   # { type, filename/table_name, format, ... }
        self.columns     = columns       # [{ name, samples, suggested_dim, suggested_key }]
        self.created_at  = time.time()
        self.mapping:    Optional[List[Dict]] = None   # set by /review

_sessions: Dict[str, SessionData] = {}

def _new_token() -> str:
    return str(uuid.uuid4())

def _get_session(token: str) -> SessionData:
    session = _sessions.get(token)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session '{token}' not found or expired.")
    if time.time() - session.created_at > SESSION_TTL_SECONDS:
        del _sessions[token]
        raise HTTPException(status_code=410, detail="Session expired. Please re-upload your file.")
    return session

def _purge_expired():
    """Remove expired sessions. Called on each request — cheap enough at this scale."""
    now = time.time()
    expired = [t for t, s in _sessions.items() if now - s.created_at > SESSION_TTL_SECONDS]
    for t in expired:
        del _sessions[t]

# ─────────────────────────────────────────────────────────────────────────────
# Column inference — suggest dimension + semantic_key from column name
# ─────────────────────────────────────────────────────────────────────────────

# Keyword → (dimension, semantic_key) mappings.
# Ordered from most specific to least — first match wins.
_INFER_RULES: List[Tuple[List[str], str, Optional[str]]] = [
    # WHO
    (["artist", "author", "creator", "composer", "writer", "performer"],     "WHO", None),
    (["attorney", "lawyer", "counsel", "timekeeper"],                          "WHO", None),
    (["client", "customer", "patron", "borrower"],                             "WHO", None),
    (["publisher", "label", "imprint", "distributor"],                         "WHO", None),
    (["person", "name", "individual", "contact"],                              "WHO", None),
    (["organization", "org", "company", "firm", "institution"],                "WHO", None),
    (["assigned_to", "owner", "responsible"],                                  "WHO", None),
    # WHAT
    (["title", "name", "subject", "heading"],                                  "WHAT", None),
    (["matter", "case", "docket", "proceeding"],                               "WHAT", None),
    (["type", "format", "genre", "category", "kind", "class"],                 "WHAT", None),
    (["description", "summary", "abstract", "note"],                           "WHAT", None),
    (["isbn", "issn", "identifier", "id", "number", "code"],                   "WHAT", None),
    # WHEN
    (["date", "year", "month", "day", "time", "period", "released",
      "published", "created", "modified", "filed", "closed", "opened"],        "WHEN", None),
    # WHERE
    (["place", "location", "city", "state", "country", "region",
      "jurisdiction", "venue", "address", "office"],                           "WHERE", None),
    (["label", "where"],                                                        "WHERE", None),
    # WHY
    (["reason", "purpose", "cause", "status", "disposition",
      "privilege", "classification", "tag"],                                    "WHY",  None),
    # HOW
    (["condition", "method", "format", "medium", "rating",
      "quality", "version", "edition"],                                         "HOW",  None),
]

def _suggest_dim_key(col_name: str) -> Tuple[str, str]:
    """
    Infer (dimension, semantic_key) from a column name.
    Returns ('skip', col_name_normalized) if no rule matches.
    """
    normalized = col_name.lower().strip()
    # Remove common table prefixes like tbl_, t_, f_
    normalized = re.sub(r'^(tbl_|t_|f_|col_)', '', normalized)
    tokens = set(re.split(r'[_\s\-]+', normalized))

    for keywords, dim, key_override in _INFER_RULES:
        if any(kw in normalized or kw in tokens for kw in keywords):
            key = key_override or normalized
            return dim, key

    return 'skip', normalized.replace(" ", "_")


def _build_columns(df: pd.DataFrame) -> List[Dict]:
    """
    Build the columns payload from a DataFrame:
    [{ name, samples, suggested_dim, suggested_key }]
    """
    cols = []
    for col in df.columns:
        samples = (
            df[col]
            .dropna()
            .astype(str)
            .loc[lambda s: s.str.strip() != '']
            .head(SAMPLE_ROWS)
            .tolist()
        )
        suggested_dim, suggested_key = _suggest_dim_key(str(col))
        cols.append({
            "name":          str(col),
            "samples":       samples,
            "suggested_dim": suggested_dim,
            "suggested_key": suggested_key,
        })
    return cols

# ─────────────────────────────────────────────────────────────────────────────
# Variant detection — Tantivy path (D12 normative implementation)
# ─────────────────────────────────────────────────────────────────────────────

def _find_variants_tantivy(values: List[str]) -> List[Tuple[str, str, float]]:
    """
    Find near-duplicate string pairs using Tantivy in-memory index.

    Builds a temporary index, indexes all values, then queries each value
    as a tokenized phrase and collects hits above VARIANT_SCORE_THRESH.

    Returns list of (value_a, value_b, score) for candidate pairs.
    Never modifies values. Never writes to disk. Read-only operation.

    INVARIANT (D12): This is the normative implementation shared with /resolve.
    """
    if len(values) < 2:
        return []

    schema_builder = tantivy.SchemaBuilder()
    schema_builder.add_text_field("value", stored=True)
    schema_builder.add_unsigned_field("idx", stored=True)
    schema = schema_builder.build()

    index = tantivy.Index(schema, path=None)  # in-memory
    writer = index.writer(heap_size=15_000_000)

    for i, v in enumerate(values):
        doc = tantivy.Document()
        doc.add_text("value", str(v))
        doc.add_unsigned("idx", i)
        writer.add_document(doc)
    writer.commit()

    searcher = index.searcher()
    candidates = []
    seen = set()

    for i, query_val in enumerate(values):
        # Tokenize the query value and build a fuzzy term query
        tokens = re.sub(r'[^\w\s]', ' ', str(query_val).lower()).split()
        if not tokens:
            continue

        query = index.parse_query(" ".join(tokens), ["value"])
        hits = searcher.search(query, limit=10).hits

        for score, doc_address in hits:
            doc = searcher.doc(doc_address)
            j = doc.get_first("idx")
            if j == i or score < VARIANT_SCORE_THRESH:
                continue
            pair_key = (min(i, j), max(i, j))
            if pair_key in seen:
                continue
            seen.add(pair_key)
            candidates.append((values[i], values[j], round(score, 3)))

    return candidates


def _find_variants_fallback(values: List[str]) -> List[Tuple[str, str, float]]:
    """
    Fallback variant detection without Tantivy.
    Uses token-overlap (Jaccard similarity) on normalized tokens.
    Less sophisticated than Tantivy but catches obvious cases like
    '20,000 Leagues' vs '20000 Leagues'.

    Clearly marked in flag details so the caller knows this ran, not Tantivy.
    """
    def normalize(s: str) -> set:
        return set(re.sub(r'[^\w\s]', ' ', s.lower()).split())

    candidates = []
    seen = set()
    for i, a in enumerate(values):
        ta = normalize(a)
        if not ta:
            continue
        for j, b in enumerate(values):
            if j <= i:
                continue
            pair_key = (i, j)
            if pair_key in seen:
                continue
            tb = normalize(b)
            if not tb:
                continue
            intersection = len(ta & tb)
            union = len(ta | tb)
            if union == 0:
                continue
            jaccard = intersection / union
            if jaccard >= VARIANT_SCORE_THRESH:
                seen.add(pair_key)
                candidates.append((a, b, round(jaccard, 3)))

    return candidates


def _find_variants(values: List[str]) -> Tuple[List[Tuple[str, str, float]], str]:
    """
    Entry point for variant detection. Uses Tantivy if available, falls back otherwise.
    Returns (candidates, engine_name) so the caller can note which engine ran.
    """
    if TANTIVY_AVAILABLE:
        try:
            return _find_variants_tantivy(values), "tantivy"
        except Exception:
            pass  # fall through to fallback
    return _find_variants_fallback(values), "fallback"

# ─────────────────────────────────────────────────────────────────────────────
# Pre-ingest review — flag generation
# ─────────────────────────────────────────────────────────────────────────────

def _run_review(df: pd.DataFrame, mapping: List[Dict]) -> List[Dict]:
    """
    Run pre-ingest review against the mapped DataFrame.

    Steps:
      1. Translate rows to coordinate triples per the declared mapping.
      2. For each semantic_key, collect all distinct values.
      3. Run variant detection on values within the same semantic_key.
         (Values from different semantic_keys are never compared — coordinate
          namespace separation prevents WHAT.title tokens from matching WHO.artist.)
      4. Check null coverage per mapped field.
      5. Check for singleton values.
      6. Check nucleus stability (no stable shared ID heuristic).

    Returns list of flag dicts matching the flag taxonomy in the spec.
    """
    flags = []
    flag_id = 0

    def next_id():
        nonlocal flag_id
        flag_id += 1
        return f"flag_{flag_id}"

    # Build { semantic_key → [distinct values] } per the mapping
    key_values: Dict[str, List[str]] = {}
    null_stats: Dict[str, Tuple[int, int]] = {}  # { semantic_key → (null_count, total_count) }

    for row in mapping:
        col       = row["column"]
        dim       = row["dimension"]
        skey      = row["semantic_key"]
        coord_key = f"{dim}|{skey}"

        if col not in df.columns:
            continue

        series       = df[col].astype(str).str.strip()
        null_mask    = df[col].isna() | (series == '') | (series.str.lower() == 'nan')
        non_null     = series[~null_mask]
        null_count   = int(null_mask.sum())
        total_count  = len(df)

        null_stats[coord_key] = (null_count, total_count)

        distinct = non_null.unique().tolist()
        key_values.setdefault(coord_key, []).extend(
            v for v in distinct if v not in key_values.get(coord_key, [])
        )

    # ── Variant candidates (per semantic_key, Tantivy-powered) ──────────────
    for coord_key, values in key_values.items():
        if len(values) < 2:
            continue
        candidates, engine = _find_variants(values)
        for val_a, val_b, score in candidates:
            flags.append({
                "id":       next_id(),
                "type":     "variant_candidates",
                "severity": "warning",
                "message":  f"Possible duplicates in {coord_key}",
                "details":  (
                    f'"{val_a}" and "{val_b}" share significant tokens '
                    f'(similarity {score:.0%}) and may refer to the same entity. '
                    f'Confirm whether these should map to one entity ID. '
                    f'(Detection engine: {engine})'
                ),
            })

    # ── Null coordinate coverage ─────────────────────────────────────────────
    for coord_key, (null_count, total) in null_stats.items():
        if total == 0:
            continue
        null_pct = null_count / total
        if null_pct > NULL_PCT_WARN_THRESH:
            flags.append({
                "id":       next_id(),
                "type":     "null_coordinates",
                "severity": "info",
                "message":  f"{null_pct:.0%} of rows have no value for {coord_key}",
                "details":  (
                    f"{null_count} of {total} rows are null for this field. "
                    "Those entities will have incomplete dimension coverage in the substrate. "
                    "This may be expected — review before committing."
                ),
            })

    # ── Singleton values ─────────────────────────────────────────────────────
    for coord_key, values in key_values.items():
        singletons = []
        col_name = coord_key.split("|", 1)[-1]
        # Find which mapping row this coord_key belongs to
        for row in mapping:
            if f"{row['dimension']}|{row['semantic_key']}" == coord_key and row["column"] in df.columns:
                counts = df[row["column"]].astype(str).str.strip().value_counts()
                singletons = [v for v, c in counts.items() if c <= SINGLETON_MAX_COUNT and v.strip()]
                break

        if len(singletons) > 5:
            flags.append({
                "id":       next_id(),
                "type":     "singleton_values",
                "severity": "info",
                "message":  f"{len(singletons)} values appear only once in {coord_key}",
                "details":  (
                    f"Examples: {', '.join(repr(s) for s in singletons[:5])}{'…' if len(singletons) > 5 else ''}. "
                    "Singletons may be data entry errors or genuinely unique values. "
                    "Review if this field is expected to have repeating values."
                ),
            })

    # ── No stable shared ID heuristic ────────────────────────────────────────
    # If every mapped field has high cardinality (distinct ≈ row count),
    # there may not be a reliable stable ID. Surface as a warning.
    high_card_fields = []
    for row in mapping:
        col = row["column"]
        if col not in df.columns:
            continue
        n_distinct = df[col].nunique()
        n_rows     = len(df)
        if n_rows > 0 and n_distinct / n_rows > 0.95:
            high_card_fields.append(col)

    if len(high_card_fields) == len(mapping) and len(mapping) > 0:
        flags.append({
            "id":       next_id(),
            "type":     "no_stable_id",
            "severity": "warning",
            "message":  "No obvious stable unique identifier found",
            "details":  (
                "All mapped columns have high cardinality — no single column is clearly "
                "a stable ID. You may need a compound nucleus (combining two columns) "
                "to produce reliable entity IDs. Review in the next step."
            ),
        })

    return flags

# ─────────────────────────────────────────────────────────────────────────────
# Artifact emission — DuckDB path (v1.0)
# ─────────────────────────────────────────────────────────────────────────────

def _emit_duckdb(df: pd.DataFrame, spec: Dict) -> Dict:
    """
    Emit a DuckDB substrate artifact.

    Delegates to snf-peirce compile_data() if available.
    Falls back to a direct DuckDB write if snf-peirce is not installed
    (produces a valid Shape C substrate that Reckoner can read).

    Returns { output_path, entity_count, fact_count, facts_by_dim, warnings }
    """
    mapping      = spec["mapping"]
    nucleus_spec = spec["nucleus"]
    lens_id      = spec["lens"]["lens_id"]
    output_name  = spec["target"]["output_name"]
    output_path  = OUTPUT_DIR / f"{output_name}.duckdb"

    # Build entity_id per row from nucleus spec
    def make_entity_id(row) -> str:
        cols = nucleus_spec["columns"]
        sep  = nucleus_spec.get("separator", "-")
        pfx  = nucleus_spec.get("prefix", "")
        parts = [str(row.get(c, "")).strip() for c in cols]
        base  = sep.join(p for p in parts if p)
        return f"{pfx}:{base}" if pfx else base

    # Build spoke rows
    dim_rows: Dict[str, List[Dict]] = {d: [] for d in ["WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"]}
    meta_rows: List[Dict] = []
    warnings: List[str] = []
    translator_version = spec.get("provenance", {}).get("translator_version", "1.0.0")

    entity_ids_seen = set()

    for _, row in df.iterrows():
        eid = make_entity_id(row)
        if not eid:
            warnings.append(f"Skipped row with empty entity ID: {dict(row)}")
            continue

        if eid not in entity_ids_seen:
            entity_ids_seen.add(eid)
            # Nucleus value for display
            nucleus_cols = nucleus_spec["columns"]
            nucleus_val  = nucleus_spec.get("separator", "-").join(
                str(row.get(c, "")).strip() for c in nucleus_cols
            )
            # Best label: first WHAT mapping, fallback to first WHO
            label = ""
            sublabel = ""
            for m in mapping:
                if m["dimension"] == "WHAT" and not label:
                    label = str(row.get(m["column"], "")).strip()
                elif m["dimension"] == "WHO" and not sublabel:
                    sublabel = str(row.get(m["column"], "")).strip()
            meta_rows.append({
                "entity_id":          eid,
                "nucleus":            nucleus_val,
                "label":              label or eid,
                "sublabel":           sublabel,
                "lens_id":            lens_id,
                "translator_version": translator_version,
            })

        for m in mapping:
            col  = m["column"]
            dim  = m["dimension"]
            skey = m["semantic_key"]
            if col not in df.columns:
                continue
            raw = row.get(col)
            if pd.isna(raw) or str(raw).strip() == "":
                continue
            # ONE FACT PER ROW invariant — split multi-value fields on comma
            raw_str = str(raw).strip()
            values  = [v.strip() for v in raw_str.split(",") if v.strip()] if "," in raw_str else [raw_str]
            for val in values:
                skey_clean = skey.replace(" ", "_")

                # ── WHEN dimension: normalize dates and fan out granularities ──
                if dim == "WHEN":
                    # Strip trailing timestamp (2025-03-22 00:00:00 → 2025-03-22)
                    import re as _re
                    date_match = _re.match(r'^(\d{4}-\d{2}-\d{2})', val)
                    if date_match:
                        date_str = date_match.group(1)
                        try:
                            from datetime import datetime as _datetime
                            dt = _datetime.strptime(date_str, "%Y-%m-%d")
                            # Fan out into granularity facts
                            gran_facts = [
                                ("full_date",   date_str),
                                ("year",        str(dt.year)),
                                ("month",       date_str[:7]),          # 2025-03
                                ("month_name",  dt.strftime("%B")),     # March
                                ("day_of_week", dt.strftime("%A")),     # Saturday
                            ]
                            for gran_key, gran_val in gran_facts:
                                coordinate = f"{dim.lower()}|{gran_key}|{gran_val}"
                                dim_rows[dim].append({
                                    "entity_id":          eid,
                                    "dimension":          dim,
                                    "semantic_key":       gran_key,
                                    "value":              gran_val,
                                    "coordinate":         coordinate,
                                    "lens_id":            lens_id,
                                    "translator_version": translator_version,
                                })
                            continue  # skip the default single-fact write below
                        except ValueError:
                            pass  # not a parseable date — fall through to default

                # Default: write single fact
                coordinate = f"{dim.lower()}|{skey_clean}|{val}"
                dim_rows[dim].append({
                    "entity_id":          eid,
                    "dimension":          dim,
                    "semantic_key":       skey,
                    "value":              val,
                    "coordinate":         coordinate,
                    "lens_id":            lens_id,
                    "translator_version": translator_version,
                })

    # Write to DuckDB
    try:
        import duckdb
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="duckdb required for DuckDB output. pip install duckdb"
        )

    con = duckdb.connect(str(output_path))

    # Single snf_spoke table — matches Reckoner registry schema exactly.
    # All dimensions go into one table with a dimension column.
    con.execute("""
        CREATE OR REPLACE TABLE snf_spoke (
            entity_id    VARCHAR,
            dimension    VARCHAR,
            semantic_key VARCHAR,
            value        VARCHAR,
            coordinate   VARCHAR,
            lens_id      VARCHAR
        )
    """)

    all_spoke_rows = []
    for dim in ["WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"]:
        for r in dim_rows[dim]:
            all_spoke_rows.append((
                r["entity_id"],
                r["dimension"].lower(),
                r["semantic_key"].replace(" ", "_"),
                r["value"],
                r["coordinate"],
                r["lens_id"],
            ))

    if all_spoke_rows:
        con.executemany(
            "INSERT INTO snf_spoke VALUES (?, ?, ?, ?, ?, ?)",
            all_spoke_rows
        )

    con.execute("CREATE INDEX IF NOT EXISTS idx_spoke_coord ON snf_spoke(coordinate)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_spoke_eid   ON snf_spoke(entity_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_spoke_dim   ON snf_spoke(dimension, semantic_key)")

    # snf_meta display table
    con.execute("""
        CREATE OR REPLACE TABLE snf_meta (
            entity_id          VARCHAR NOT NULL,
            nucleus            VARCHAR,
            label              VARCHAR,
            sublabel           VARCHAR,
            lens_id            VARCHAR,
            translator_version VARCHAR
        )
    """)
    if meta_rows:
        con.executemany(
            "INSERT INTO snf_meta VALUES (?, ?, ?, ?, ?, ?)",
            [(r["entity_id"], r["nucleus"], r["label"],
              r["sublabel"], r["lens_id"], r["translator_version"])
             for r in meta_rows]
        )

    con.close()

    # ── Emit SRF records ────────────────────────────────────────────────────
    # Write one .srf file per entity into substrates/srf_imports/<lens_id>/
    # Makes every Model Builder dataset portable and federable.
    srf_count  = 0
    srf_errors = 0
    try:
        from snf_peirce.srf import SRFRecord, SRFValidationError
        import json as _json
        import datetime as _dt

        srf_imports_dir = SRF_IMPORTS_DIR / lens_id
        srf_imports_dir.mkdir(parents=True, exist_ok=True)

        now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        source_name    = spec.get("source", {}).get("filename") or output_name
        translator_ver = spec.get("provenance", {}).get("translator_version", "1.0.0")

        # Build facts per entity_id
        facts_by_entity: Dict[str, list] = {}
        for dim in ["WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"]:
            for r in dim_rows[dim]:
                eid = r["entity_id"]
                if eid not in facts_by_entity:
                    facts_by_entity[eid] = []
                facts_by_entity[eid].append({
                    "dimension":    dim,
                    "semantic_key": r["semantic_key"],
                    "value":        r["value"],
                })

        for meta in meta_rows:
            eid   = meta["entity_id"]
            facts = facts_by_entity.get(eid, [])
            if not facts:
                continue

            record_dict = {
                "srf_version": "1.0",
                "srf_uri":     f"srf://{lens_id}/mb/{eid}",
                "entity_id":   eid,
                "nucleus": {
                    "type":  nucleus_spec.get("authority") or lens_id,
                    "value": meta["nucleus"],
                },
                "facts": facts,
                "provenance": {
                    "source":             source_name,
                    "translated_by":      "ModelBuilder",
                    "translator_version": translator_ver,
                    "lens":               lens_id,
                    "translated_at":      now,
                },
            }

            try:
                record = SRFRecord.from_dict(record_dict)
                safe_eid = eid.replace(":", "_").replace("/", "_")
                out_file = srf_imports_dir / f"{safe_eid}.srf"
                out_file.write_text(
                    _json.dumps(record.to_dict(), indent=2, ensure_ascii=False),
                    encoding="utf-8"
                )
                srf_count += 1
            except Exception as e:
                srf_errors += 1
                warnings.append(f"SRF emit failed for {eid}: {e}")

        if srf_count > 0:
            print(f"[model_builder] Emitted {srf_count} SRF records to {srf_imports_dir}")
        if srf_errors > 0:
            print(f"[model_builder] {srf_errors} SRF emit errors (see warnings)")

    except ImportError:
        warnings.append(
            "SRF export skipped: snf-peirce >= 0.1.10 required. "
            "pip install snf-peirce>=0.1.10"
        )
    # ── End SRF emit ────────────────────────────────────────────────────────

    facts_by_dim = {dim: len(dim_rows[dim]) for dim in dim_rows}
    total_facts  = sum(facts_by_dim.values())

    return {
        "output_path":  str(output_path),
        "download_url": f"/api/mb/download/{output_path.name}",
        "entity_count": len(entity_ids_seen),
        "fact_count":   total_facts,
        "facts_by_dim": facts_by_dim,
        "warnings":     warnings,
        "srf_exported": srf_count,
    }


def _emit_postgres_views(df: pd.DataFrame, spec: Dict) -> Dict:
    """
    Emit a Postgres materialized views SQL script.

    For SQL sources: the script creates SNF views over the existing table.
    For file sources: the script creates a new schema with the data as static views.

    Returns { output_path, download_url, entity_count, fact_count, facts_by_dim, warnings }
    """
    mapping       = spec["mapping"]
    lens_id       = spec["lens"]["lens_id"]
    output_name   = spec["target"]["output_name"]
    source        = spec["source"]
    schema_name   = source.get("schema_name", "public")
    table_name    = source.get("table_name", output_name)
    snf_schema    = f"{output_name}_snf"

    lines = [
        f"-- SNF Materialized Views",
        f"-- Generated by Reckoner Model Builder",
        f"-- Dataset: {lens_id}",
        f"-- Source: {schema_name}.{table_name}",
        f"-- Generated: {datetime.utcnow().isoformat()}Z",
        f"-- Data stays in place. Run this script. Nothing moves.",
        f"",
        f"CREATE SCHEMA IF NOT EXISTS {snf_schema};",
        f"",
    ]

    dim_cols: Dict[str, List[Dict]] = {d: [] for d in ["WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"]}
    for m in mapping:
        dim_cols[m["dimension"]].append(m)

    # Build nucleus expression early — needed for entity_id in every spoke view
    nucleus_spec_early = spec["nucleus"]
    nucleus_cols_early = nucleus_spec_early["columns"]
    sep_early          = nucleus_spec_early.get("separator", "-")
    pfx_early          = nucleus_spec_early.get("prefix", "")
    nucleus_expr       = f" || '{sep_early}' || ".join(f"{c}::VARCHAR" for c in nucleus_cols_early)
    if pfx_early:
        nucleus_expr   = f"'{pfx_early}:' || {nucleus_expr}"

    facts_by_dim = {}
    entity_count = 0

    for dim, cols in dim_cols.items():
        if not cols:
            facts_by_dim[dim] = 0
            continue

        table = f"{snf_schema}.snf_{dim.lower()}"
        union_parts = []
        for m in cols:
            col  = m["column"]
            skey = m["semantic_key"].replace(" ", "_")
            union_parts.append(
                f"    SELECT\n"
                f"        {nucleus_expr} AS entity_id,\n"
                f"        '{dim.lower()}' AS dimension,\n"
                f"        '{skey}' AS semantic_key,\n"
                f"        {col}::VARCHAR AS value,\n"
                f"        '{dim.lower()}' || '|' || '{skey}' || '|' || {col}::VARCHAR AS coordinate,\n"
                f"        '{lens_id}' AS lens_id,\n"
                f"        '1.0.0' AS translator_version\n"
                f"    FROM {schema_name}.{table_name}\n"
                f"    WHERE {col} IS NOT NULL"
            )

        union_sql = "\n    UNION ALL\n".join(union_parts)
        lines += [
            f"CREATE MATERIALIZED VIEW {table} AS",
            union_sql + ";",
            f"",
            f"CREATE INDEX ON {table}(coordinate);",
            f"CREATE INDEX ON {table}(entity_id);",
            f"",
        ]
        facts_by_dim[dim] = len(cols)  # placeholder — real count at runtime

    # snf_hub — display table
    nucleus_spec = spec["nucleus"]
    nucleus_cols = nucleus_spec["columns"]
    sep          = nucleus_spec.get("separator", "-")
    pfx          = nucleus_spec.get("prefix", "")
    nucleus_expr = f" || '{sep}' || ".join(f"{c}::VARCHAR" for c in nucleus_cols)
    if pfx:
        nucleus_expr = f"'{pfx}:' || {nucleus_expr}"

    # Label: first WHAT col, sublabel: first WHO col
    label_col   = next((m["column"] for m in mapping if m["dimension"] == "WHAT"), nucleus_cols[0])
    sublabel_col = next((m["column"] for m in mapping if m["dimension"] == "WHO"), "NULL")

    lines += [
        f"CREATE MATERIALIZED VIEW {snf_schema}.snf_hub AS",
        f"    SELECT",
        f"        {nucleus_expr} AS entity_id,",
        f"        {nucleus_expr} AS nucleus,",
        f"        {label_col}::VARCHAR AS label,",
        f"        {sublabel_col}::VARCHAR AS sublabel,",
        f"        '{lens_id}' AS lens_id,",
        f"        '1.0.0' AS translator_version",
        f"    FROM {schema_name}.{table_name}",
        f"    WHERE {nucleus_cols[0]} IS NOT NULL;",
        f"",
        f"CREATE INDEX ON {snf_schema}.snf_hub(entity_id);",
        f"",
    ]

    # snf_affordances — fast-path for Reckoner's Portolan planner
    aff_parts = []
    for dim, cols in dim_cols.items():
        for m in cols:
            col  = m["column"]
            skey = m["semantic_key"]
            aff_parts.append(
                f"    SELECT '{dim.lower()}' AS dimension, '{skey}' AS field,\n"
                f"           COUNT(DISTINCT {nucleus_expr}) AS distinct_entities,\n"
                f"           COUNT(*) AS fact_count\n"
                f"    FROM {schema_name}.{table_name} WHERE {col} IS NOT NULL"
            )

    if aff_parts:
        lines += [
            f"CREATE MATERIALIZED VIEW {snf_schema}.snf_affordances AS",
            "\n    UNION ALL\n".join(aff_parts) + ";",
            f"",
        ]

    # Refresh script
    lines += [
        f"-- refresh.sql — run when source data changes",
        f"-- REFRESH MATERIALIZED VIEW {snf_schema}.snf_hub;",
    ] + [
        f"-- REFRESH MATERIALIZED VIEW {snf_schema}.snf_{dim.lower()};"
        for dim in ["WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"]
        if dim_cols.get(dim)
    ] + [
        f"-- REFRESH MATERIALIZED VIEW {snf_schema}.snf_affordances;",
    ]

    sql_content = "\n".join(lines)
    output_path = OUTPUT_DIR / f"{output_name}_snf_views.sql"
    output_path.write_text(sql_content, encoding="utf-8")

    return {
        "output_path":  str(output_path),
        "download_url": f"/api/mb/download/{output_path.name}",
        "entity_count": entity_count,
        "fact_count":   sum(facts_by_dim.values()),
        "facts_by_dim": facts_by_dim,
        "warnings":     [
            "Row and entity counts are not available for the views path — "
            "counts will reflect live data at query time."
        ],
    }

# ─────────────────────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────────────────────

class NucleusAuthority(str, Enum):
    """
    Portable authority namespace for nucleus values.

    Rule: if two different sources could ever hold a record for the same entity
    (same artwork at Met and Tate, same film on TMDB and Letterboxd), they must
    share a NucleusAuthority value so the enricher can match them.

    For firm-internal IDs that will never cross-enrich with anything, use 'local'.

    Adding a new translator: add its authority value here FIRST, then reference
    it in the translator's BuildSpec. Never add values retroactively — the enum
    is the contract.
    """
    # ── Cross-source portable (preferred) ─────────────────────────────────────
    wikidata_qid     = "wikidata_qid"      # Wikidata Q-number — universal fallback
    isbn             = "isbn"              # Books — Open Library, WorldCat, LOC
    musicbrainz_id   = "musicbrainz_id"    # Recordings, releases, artists
    tmdb_id          = "tmdb_id"           # Films, TV — TMDB
    discogs_id       = "discogs_id"        # Vinyl, releases, labels — Discogs
    # ── Museum / art ──────────────────────────────────────────────────────────
    met_object_id    = "met_object_id"     # Metropolitan Museum of Art
    artsy_id         = "artsy_id"          # Artsy — artworks, artists, movements
    getty_ulan_id    = "getty_ulan_id"     # Getty Union List of Artist Names
    # ── Legal / library ───────────────────────────────────────────────────────
    courtlistener_id = "courtlistener_id"  # CourtListener — opinions, dockets
    gutenberg_id     = "gutenberg_id"      # Project Gutenberg — public domain books
    # ── Weak / local ──────────────────────────────────────────────────────────
    letterboxd_uri   = "letterboxd_uri"    # Letterboxd — enrichable to tmdb_id
    row_number       = "row_number"        # No stable ID — weakest, not portable
    local            = "local"             # Firm-internal ID, never cross-enriches


class IntrospectRequest(BaseModel):
    connection_string: str
    table_name:        str
    schema_name:       Optional[str] = "public"

class MappingRow(BaseModel):
    column:       str
    dimension:    str
    semantic_key: str

class ReviewRequest(BaseModel):
    source_token:   str
    columns_mapped: List[MappingRow]

class NucleusSpec(BaseModel):
    type:      str                            # 'single' | 'compound'
    columns:   List[str]
    separator: Optional[str]               = "-"
    prefix:    Optional[str]               = ""
    authority: Optional[NucleusAuthority]  = None
    # authority declares the portable identity namespace for this nucleus value.
    # Required for cross-source enrichment. If None, falls back to lens_id in
    # SRF emission (not portable). Use NucleusAuthority.local for firm-internal IDs.

class LensSpec(BaseModel):
    lens_id: str
    version: Optional[str] = "1.0.0"

class TargetSpec(BaseModel):
    backend:     str        # 'duckdb' | 'postgres-views' | 'postgres-import'
    output_name: str

class SourceSpec(BaseModel):
    type:             str   # 'file' | 'sql'
    # file path
    upload_token:     Optional[str] = None
    filename:         Optional[str] = None
    format:           Optional[str] = None
    # sql path
    introspect_token: Optional[str] = None
    table_name:       Optional[str] = None
    schema_name:      Optional[str] = "public"

class ProvenanceSpec(BaseModel):
    created_at:          Optional[str] = None
    translator_version:  Optional[str] = "1.0.0"

class BuildOptions(BaseModel):
    overwrite: Optional[bool] = True

class BuildSpec(BaseModel):
    source:     SourceSpec
    mapping:    List[MappingRow]
    nucleus:    NucleusSpec
    lens:       LensSpec
    target:     TargetSpec
    provenance: Optional[ProvenanceSpec] = None
    options:    Optional[BuildOptions]   = None

# ─────────────────────────────────────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────────────────────────────────────

router = APIRouter(prefix="/api/mb", tags=["model_builder"])

# ── POST /api/mb/upload ──────────────────────────────────────────────────────

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload a CSV or Excel file.
    Returns columns with sample values and suggested dimension mappings.
    File is held in session store for subsequent /review and /compile calls.
    """
    _purge_expired()

    ext = Path(file.filename).suffix.lower()
    if ext not in {".csv", ".xlsx", ".xls", ".json"}:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}. Use CSV, Excel, or JSON.")

    content = await file.read()

    try:
        if ext == ".csv":
            # Try UTF-8 first, fall back to latin-1
            try:
                df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
            except UnicodeDecodeError:
                df = pd.read_csv(io.BytesIO(content), dtype=str, encoding="latin-1", keep_default_na=False)
        elif ext == ".json":
            if not OSI_PARSER_AVAILABLE:
                raise HTTPException(
                    status_code=501,
                    detail="osi_parser.py required for JSON upload. Ensure osi_parser.py is in the same directory."
                )
            try:
                parsed = parse_json_array(content, file.filename)
            except ValueError as e:
                raise HTTPException(status_code=422, detail=str(e))
            df = pd.DataFrame(parsed["rows"])
        else:
            df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=False)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not parse file: {e}")

    if df.empty:
        raise HTTPException(status_code=422, detail="File appears to be empty.")

    columns = _build_columns(df)
    token   = _new_token()

    _sessions[token] = SessionData(
        df          = df,
        source_info = {"type": "file", "filename": file.filename, "format": ext.lstrip(".")},
        columns     = columns,
    )

    return {
        "upload_token": token,
        "columns":      columns,
        "row_count":    len(df),
    }


# ── POST /api/mb/introspect ──────────────────────────────────────────────────

@router.post("/introspect")
async def introspect_sql(req: IntrospectRequest):
    """
    Introspect a live Postgres table via SQLAlchemy.

    Read-only. Never writes. Never exports to CSV.
    Reads column names + types from information_schema.
    Reads LIMIT 10 sample rows for mapping hints.

    The connection string is used once here and is NOT stored in the session.
    The session holds only the sampled DataFrame and column metadata.
    """
    _purge_expired()

    if not SQLALCHEMY_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="SQLAlchemy required for SQL introspection. pip install sqlalchemy psycopg2-binary"
        )

    schema = req.schema_name or "public"
    table  = req.table_name

    try:
        engine = create_engine(req.connection_string, connect_args={"connect_timeout": 10})
        with engine.connect() as conn:
            # Column introspection via information_schema — read-only
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

            # Row count (approximate via pg_class for speed)
            try:
                count_row = conn.execute(text(
                    "SELECT reltuples::BIGINT FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = :schema AND c.relname = :table"
                ), {"schema": schema, "table": table}).fetchone()
                row_count = int(count_row[0]) if count_row else 0
            except Exception:
                row_count = 0

            # Sample rows — LIMIT 10, read-only
            sample_rows = conn.execute(
                text(f'SELECT * FROM "{schema}"."{table}" LIMIT :n'),
                {"n": SAMPLE_ROWS}
            ).fetchall()
            col_names = [r[0] for r in col_rows]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Connection failed: {e}")
    finally:
        try:
            engine.dispose()
        except Exception:
            pass

    # Build DataFrame from samples (no CSV, no file on disk)
    sample_dicts = [dict(zip(col_names, r)) for r in sample_rows]
    df = pd.DataFrame(sample_dicts, columns=col_names).astype(str)

    columns = _build_columns(df)
    token   = _new_token()

    _sessions[token] = SessionData(
        df          = df,
        source_info = {
            "type":       "sql",
            "table_name": table,
            "schema_name": schema,
        },
        columns = columns,
    )

    return {
        "introspect_token": token,
        "columns":          columns,
        "row_count":        row_count,
    }


# ── POST /api/mb/review ──────────────────────────────────────────────────────

@router.post("/review")
async def review(req: ReviewRequest):
    """
    Run pre-ingest review on the mapped columns.

    Translates source columns to coordinate triples, runs variant detection
    (Tantivy if available, fallback otherwise), checks null coverage, singletons.

    Flags are informational — not blocking. The human decides what matters.
    """
    _purge_expired()

    session = _get_session(req.source_token)
    mapping = [m.dict() for m in req.columns_mapped]

    # Store mapping in session for use in /compile
    session.mapping = mapping

    flags = _run_review(session.df, mapping)

    return {"flags": flags}


# ── POST /api/mb/compile ─────────────────────────────────────────────────────

@router.post("/compile")
async def compile_job(spec: BuildSpec):
    """
    Compile a BuildSpec → BuildResult + download artifact.

    Retrieves the DataFrame from the session (file or SQL sample),
    applies mapping and nucleus, emits the target artifact.

    For DuckDB: produces a .duckdb file.
    For postgres-views: produces a .sql script.

    INVARIANT: This endpoint never writes to any database.
    It produces an artifact file. The human loads it.
    """
    _purge_expired()

    # Resolve session token — source type determines which token field to use
    token = (
        spec.source.upload_token
        if spec.source.type in ("file", "osi", "json")
        else spec.source.introspect_token
    )
    if not token:
        raise HTTPException(status_code=400, detail="Missing source token in BuildSpec.")

    session = _get_session(token)

    # Validate nucleus columns exist in mapping
    mapping_cols = {m.column for m in spec.mapping}
    for col in spec.nucleus.columns:
        if col not in mapping_cols and col not in session.df.columns:
            raise HTTPException(
                status_code=422,
                detail=f"Nucleus column '{col}' not found in mapped columns or source data."
            )

    spec_dict = spec.dict()
    df        = session.df
    backend   = spec.target.backend

    try:
        if backend == "duckdb":
            result = _emit_duckdb(df, spec_dict)
        elif backend in ("postgres-views", "postgres-import"):
            result = _emit_postgres_views(df, spec_dict)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported backend: '{backend}'")
    except HTTPException:
        raise
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
            "backend":      backend,
            "generated_at": datetime.utcnow().isoformat() + "Z",
        },
    }


# ── POST /api/mb/osi/parse ───────────────────────────────────────────────────

class OSIExportRequest(BaseModel):
    """Request shape for /api/mb/osi/export."""
    lens_id:     str
    description: Optional[str] = ""
    spoke_rows:  List[Dict[str, Any]]


@router.post("/osi/parse")
async def parse_osi(file: UploadFile = File(...)):
    """
    MB-6 — Parse an OSI semantic model definition (YAML or JSON).

    Accepts an OSI .yaml, .yml, or .json file.
    Returns the same column response shape as /upload so the existing
    six-step wizard handles steps 2–6 without modification.

    Primary key fields are marked suggested_dim='skip' and is_nucleus=True.
    The wizard should pre-populate step 4 (Nucleus) from nucleus_hints.

    Step 3 (review) produces no variant flags for OSI sources — there is
    no row data to scan. The frontend should skip or auto-acknowledge step 3
    when source_type='osi' is present in the response.

    Extra fields alongside the standard columns response:
        nucleus_hints  — primary key declarations per dataset (for step 4)
        relationships  — Relationship entries (for cross-entity linking hints)
        osi_meta       — model name, description, version (pre-fills step 5)
        dataset_count  — number of OSI Datasets parsed
        field_count    — total fields mapped
        source_type    — "osi" (frontend uses this to adapt wizard behaviour)
    """
    _purge_expired()

    if not OSI_PARSER_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="osi_parser.py required. Ensure osi_parser.py is in the same directory as model_builder_api.py."
        )

    ext = Path(file.filename).suffix.lower()
    if ext not in {".yaml", ".yml", ".json"}:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {ext}. OSI parser accepts .yaml, .yml, or .json"
        )

    content = await file.read()

    try:
        result = parse_osi_file(content, file.filename)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OSI parse failed: {e}")

    token = _new_token()

    # OSI has no row data — create empty DataFrame with field names as columns.
    # Session is still needed so /compile can resolve the token.
    col_names = [c["name"] for c in result["columns"]]
    df_osi    = pd.DataFrame(columns=col_names)

    _sessions[token] = SessionData(
        df          = df_osi,
        source_info = {
            "type":     "osi",
            "filename": file.filename,
            "format":   ext.lstrip("."),
        },
        columns = result["columns"],
    )

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


# ── POST /api/mb/osi/export ──────────────────────────────────────────────────

@router.post("/osi/export")
async def export_osi(req: OSIExportRequest):
    """
    MB-6 export direction — project a compiled SNF substrate as an OSI model.

    Accepts spoke_rows (snf_spoke table contents from a compiled substrate)
    and emits an OSI SemanticModel structure.

    Enables warehouse tools that speak OSI to consume a Reckoner substrate
    without knowing SNF internals — Peirce queries against warehouse data
    via the OSI bridge.

    spoke_rows shape (matches snf_spoke table):
        [ { entity_id, dimension, semantic_key, value, coordinate, lens_id }, ... ]

    Note: Relationship structure is not recoverable from spoke_rows alone.
    The returned relationships array will be empty. Merge relationship hints
    from the original BuildSpec on the client side if needed.
    """
    if not OSI_PARSER_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="osi_parser.py required. Ensure osi_parser.py is in the same directory as model_builder_api.py."
        )

    if not req.spoke_rows:
        raise HTTPException(status_code=400, detail="spoke_rows must not be empty.")

    try:
        osi_model = export_snf_as_osi(
            spoke_rows  = req.spoke_rows,
            lens_id     = req.lens_id,
            description = req.description or "",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OSI export failed: {e}")

    field_count = sum(
        len(ds.get("fields", []))
        for sm in osi_model.get("semantic_model", [])
        for ds in sm.get("datasets", [])
    )
    metric_count = sum(
        len(sm.get("metrics", []))
        for sm in osi_model.get("semantic_model", [])
    )

    return {
        "lens_id":      req.lens_id,
        "osi_model":    osi_model,
        "field_count":  field_count,
        "metric_count": metric_count,
    }


# ── POST /api/mb/dbt/parse ───────────────────────────────────────────────────

@router.post("/dbt/parse")
async def parse_dbt(file: UploadFile = File(...)):
    """
    MB-7 — Parse a dbt schema.yml file.

    Accepts a dbt schema.yml or schema.yaml file (version 2, semantic_model
    block optional).

    Returns the same column response shape as /upload so the existing
    six-step wizard handles steps 2–6 without modification.

    Dimension mappings are pre-filled at three confidence levels:
        deterministic — entity:primary/foreign, dimension:time, metrics
        strong_hint   — is_/has_* booleans, *_date/*_id/*_region suffixes
        needs_review  — categorical dimensions with no stronger signal

    Step 3 (review) produces no variant flags for dbt sources — there is
    no row data to scan. The frontend should skip or auto-acknowledge step 3
    when source_type='dbt' is present in the response.

    Extra fields alongside the standard columns response:
        nucleus_hints   — primary entity column per model (for step 4)
        lens_candidates — metrics block entries as lens candidates (step 5)
        dbt_meta        — model name, group, agg_time_dimension
        model_count     — number of dbt models parsed
        field_count     — total columns mapped
        source_type     — "dbt" (frontend uses this to adapt wizard behaviour)
    """
    _purge_expired()

    if not DBT_PARSER_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="dbt_parser.py required. Ensure dbt_parser.py is in the same directory as model_builder_api.py."
        )

    ext = Path(file.filename).suffix.lower()
    if ext not in {".yaml", ".yml"}:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {ext}. dbt parser accepts .yaml or .yml files."
        )

    content = await file.read()

    try:
        result = parse_dbt_schema(content.decode("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"dbt parse failed: {e}")

    if result.get("errors"):
        # Surface parse errors that aren't fatal but warrant a warning
        non_fatal = [e for e in result["errors"] if e.startswith("Warning")]
        fatal     = [e for e in result["errors"] if not e.startswith("Warning")]
        if fatal:
            raise HTTPException(status_code=422, detail="; ".join(fatal))

    if not result["models"]:
        raise HTTPException(
            status_code=422,
            detail="No models found in schema.yml. Check that the file is a valid dbt version 2 schema."
        )

    # Build the columns list in the same shape as /upload and /osi/parse.
    # Each mapped column becomes one entry. Nucleus columns are flagged.
    columns = []
    nucleus_hints = {}
    lens_candidates = {}
    dbt_meta = {}

    for model in result["models"]:
        model_name = model["model_name"]

        # Nucleus hint for this model
        if model["nucleus"]:
            nucleus_hints[model_name] = model["nucleus"]

        # Lens candidates from metrics
        if model["lens_candidates"]:
            lens_candidates[model_name] = model["lens_candidates"]

        # Meta for step 5 pre-fill
        dbt_meta[model_name] = {
            "description":      model["description"],
            "agg_time_dimension": model["agg_time_dimension"],
            "group":            model["meta"].get("group"),
        }

        # Nucleus column — flagged but not mapped to a dimension
        if model["nucleus"]:
            columns.append({
                "name":          model["nucleus"],
                "suggested_dim": "skip",
                "suggested_key": model["nucleus"],
                "is_nucleus":    True,
                "confidence":    "deterministic",
                "mapping_source": model["nucleus_source"],
                "sample_values": [],
                "model":         model_name,
            })

        # Mapped columns
        for m in model["mappings"]:
            columns.append({
                "name":          m["column_name"],
                "suggested_dim": m["dimension"] or "skip",
                "suggested_key": m["semantic_key"],
                "is_nucleus":    False,
                "confidence":    m["confidence"],
                "mapping_source": m["mapping_source"],
                "description":   m["description"],
                "notes":         m["notes"],
                "sample_values": [],
                "model":         model_name,
            })

    token = _new_token()

    # dbt has no row data — create empty DataFrame with column names.
    col_names = [c["name"] for c in columns]
    df_dbt    = pd.DataFrame(columns=col_names)

    _sessions[token] = SessionData(
        df          = df_dbt,
        source_info = {
            "type":     "dbt",
            "filename": file.filename,
            "format":   "yaml",
        },
        columns = columns,
    )

    field_count = len([c for c in columns if not c.get("is_nucleus")])

    return {
        "upload_token":   token,
        "columns":        columns,
        "row_count":      field_count,
        "nucleus_hints":  nucleus_hints,
        "lens_candidates": lens_candidates,
        "dbt_meta":       dbt_meta,
        "model_count":    result["summary"]["model_count"],
        "field_count":    field_count,
        "source_type":    "dbt",
        "parse_warnings": result.get("errors", []),
    }


# ── GET /api/mb/download/{filename} ─────────────────────────────────────────

@router.get("/download/{filename}")
async def download_artifact(filename: str):
    """
    Serve a compiled artifact for download.

    Artifacts are written to OUTPUT_DIR (a temp directory).
    Filename is returned in the BuildResult download_url.
    Only files in OUTPUT_DIR can be served — no path traversal.
    """
    # Security: no path traversal — only serve files from OUTPUT_DIR
    safe_name = Path(filename).name
    path      = OUTPUT_DIR / safe_name

    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Artifact '{safe_name}' not found.")
    if not path.is_file():
        raise HTTPException(status_code=400, detail="Not a file.")

    media_type = "application/octet-stream"
    if safe_name.endswith(".sql"):
        media_type = "text/plain"

    return FileResponse(
        path        = str(path),
        filename    = safe_name,
        media_type  = media_type,
    )
