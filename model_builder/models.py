"""
model_builder/models.py — Pydantic models and authority enum for Model Builder.

These are contracts. They govern the MB API surface and the BuildSpec
that flows from wizard → compiler. They must not contain implementation logic.

Rule: adding a new translator requires adding its NucleusAuthority value here
FIRST, before writing any translator code. The enum is the contract.
"""

from __future__ import annotations

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel


# ── Authority enum ────────────────────────────────────────────────────────────

class NucleusAuthority(str, Enum):
    """
    Portable authority namespace for nucleus values.

    If two different sources could ever hold a record for the same entity
    (same artwork at Met and Tate, same film on TMDB and Letterboxd), they must
    share a NucleusAuthority value so the enricher can match them.

    For firm-internal IDs that will never cross-enrich with anything, use 'local'.
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


# ── Request / spec models ─────────────────────────────────────────────────────

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
    type:      str                           # 'single' | 'compound'
    columns:   List[str]
    separator: Optional[str]              = "-"
    prefix:    Optional[str]              = ""
    authority: Optional[NucleusAuthority] = None
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
    upload_token:     Optional[str] = None
    filename:         Optional[str] = None
    format:           Optional[str] = None
    introspect_token: Optional[str] = None
    table_name:       Optional[str] = None
    schema_name:      Optional[str] = "public"


class ProvenanceSpec(BaseModel):
    created_at:         Optional[str] = None
    translator_version: Optional[str] = "1.0.0"


class BuildOptions(BaseModel):
    overwrite: Optional[bool] = True


class CorrelationMember(BaseModel):
    column: str   # source column name — must appear in mapping


class CorrelationGroup(BaseModel):
    group:   str                    # human-readable name e.g. "ingredient_measure"
    members: List[CorrelationMember]


class BuildSpec(BaseModel):
    source:       SourceSpec
    mapping:      List[MappingRow]
    nucleus:      NucleusSpec
    lens:         LensSpec
    target:       TargetSpec
    provenance:   Optional[ProvenanceSpec]         = None
    options:      Optional[BuildOptions]           = None
    correlations: Optional[List[CorrelationGroup]] = None
    # correlations — optional. When present, facts from member columns on the
    # same source row are stamped with a shared correlation_id: group_name_NNN.
    # correlation_id is a passenger column — not indexed, not routed, display-only.


class BuildResult(BaseModel):
    """Returned by compile_artifact() and the /compile route."""
    output_path:  str
    download_url: str
    entity_count: int
    fact_count:   int
    facts_by_dim: dict
    warnings:     List[str]
    lens_id:      str
