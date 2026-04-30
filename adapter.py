"""
adapter.py — SubstrateAdapter base class

The abstract interface that all SNF execution backends must implement.
Part of snf-adapters. Sits between snf-peirce (routing algebra) and
any application layer (e.g. Reckoner).

Dependency rule:
    snf-peirce          ← routing algebra, no DB dependencies
        ↑
    snf-adapters        ← execution backends (this package)
        ↑
    reckoner / app      ← HTTP skin, assembles responses

The application talks only to SubstrateAdapter. The backend is invisible to it.

Responsibility boundaries:
    snf-peirce      — parsing, routing algebra, what a query means
    snf-adapters    — fetching, hydrating, what the data is
    application     — wiring them together, matched_because, HTTP shapes

The matched_because seam:
    matched_because is computed by the application layer by re-parsing
    the Peirce string. It comes from the query, not the data. It is
    passed into hydrate() as a parameter. The adapter staples it onto
    result objects but does not compute it. This keeps snf-adapters
    free of any dependency on snf-peirce's parser.

Provenance source rule:
    Required identity fields (lens_id, translator_version, substrate_id)
    must come from substrate-bound ingest metadata (Source 1) or an
    adapter binding manifest (Source 2). They may not be derived,
    defaulted, or supplied as caller-controlled ad hoc assertions.

    provenance_source declares which source was used. It is audit
    metadata only — not part of query equivalence, query hashing, or
    result set compatibility checks. It must always be set explicitly:
        "substrate"  — identity read from ingest-time artifacts
        "binding"    — identity read from adapter binding manifest

    See Result Set Identity Model — Addendum: Allowed Provenance Sources.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


# ─────────────────────────────────────────────────────────────────────────────
# Result types
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class QueryResult:
    """
    Returned by query(). Entity IDs that satisfied the Peirce expression.

    entity_ids  — ordered list of matched entity IDs
    count       — total matches (may exceed len(entity_ids) if limit applied)
    trace       — Portolan execution trace: list of steps in execution order.
                  Each step: { dimension, cardinality, fields }
                  dimension   — e.g. "WHO"
                  cardinality — posting list size for this dimension
                  fields      — list of { field, values } constraints used
                  Empty list if trace not available (e.g. single-dimension query).
    """
    entity_ids: List[str]
    count:      int
    trace:      List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class DiscoverResult:
    """
    Returned by discover(). Rows from a discovery expression.

    scope       — "dimensions" | "fields" | "values"
    dimension   — populated if scope is "fields" or "values"
    field       — populated if scope is "values"
    rows        — result rows; shape depends on scope
    """
    scope:     str
    dimension: Optional[str]
    field:     Optional[str]
    rows:      List[Dict[str, Any]]


@dataclass
class ProvenanceRecord:
    """
    Provenance metadata for the substrate.
    Surfaced by provenance() so the SRF builder has what it needs
    without going back to the source.

    lens_id            — which lens this substrate was compiled under
    provenance_source  — how required identity fields were obtained:
                         "substrate" = read from ingest-time artifacts (normative)
                         "binding"   = read from adapter binding manifest (transitional)
                         Always set explicitly. Absence is not permitted.
                         AUDIT METADATA ONLY — not part of query equivalence,
                         query hashing, or result set compatibility checks.
    source             — origin system or collection (e.g. "Library of Congress")
    translated_by      — translator that produced the coordinates
                         (e.g. "MARCTranslator v3.0")
    lens_label         — human-readable lens name if available
    extra              — any additional provenance the adapter can surface
    """
    lens_id:           str
    provenance_source: str                  # "substrate" | "binding" — always explicit
    source:            Optional[str]        = None
    translated_by:     Optional[str]        = None
    lens_label:        Optional[str]        = None
    extra:             Dict[str, Any]       = field(default_factory=dict)

    def __post_init__(self):
        if self.provenance_source not in ("substrate", "binding"):
            raise ValueError(
                f"provenance_source must be 'substrate' or 'binding', "
                f"got '{self.provenance_source}'. "
                "See Result Set Identity Model — Addendum: Allowed Provenance Sources."
            )


# ─────────────────────────────────────────────────────────────────────────────
# SubstrateAdapter
# ─────────────────────────────────────────────────────────────────────────────

class SubstrateAdapter(ABC):
    """
    Abstract base class for all SNF execution backends.

    The core design principle: meet data where it lives.
    Organizations should not need to migrate their infrastructure to get
    semantic routing. The adapter pattern means SNF can layer over whatever
    store is already running — relational, columnar, document, or inverted
    index — without requiring the data to move.

    Concrete implementations:
        DuckDBAdapter         — wraps snf-peirce Substrate (CSV / in-process DuckDB)
        PostgresAdapter       — psycopg2
        PinotAdapter          — wraps pinot_substrate.py
        SQLServerAdapter      — pyodbc / pymssql
        ElasticsearchAdapter  — elasticsearch-py
                                Natural fit for large document stores: Lucene's
                                core operation is posting-list intersection, which
                                is exactly what SNF routing is. Use filter context
                                (not query context) to get exact boolean behavior
                                without relevance scoring. field/value pairs in the
                                index map directly to SNF coordinates.

    The application layer (e.g. Reckoner) holds a SubstrateAdapter and
    calls only the methods declared here. It has no knowledge of what
    backend is underneath.

    Provenance source contract:
        All concrete adapters must emit a ProvenanceRecord with
        provenance_source set explicitly. Source 1 (substrate-bound
        ingest metadata) emits "substrate". Source 2 (binding manifest)
        emits "binding". See Result Set Identity Model addendum.

    Future pressure points (do not act on yet):
        1. query() is text-level (Peirce string in). If planning becomes more
           formal, the adapter may eventually consume a normalized execution
           plan rather than raw query text. That would move the parse boundary
           above the adapter entirely.
        2. affordances() and values() are UI-support methods sitting alongside
           pure execution methods. If a thinner non-UI client is ever needed,
           these may split into a separate BrowseAdapter interface.
    """

    # ── Identity ──────────────────────────────────────────────────────────────

    @property
    @abstractmethod
    def lens_id(self) -> str:
        """
        The lens this substrate was compiled under.
        Read from the data on load — never declared in config.
        A substrate with missing or inconsistent lens_id is invalid
        and must raise at load time, not silently at query time.
        """
        ...

    # ── Metadata ──────────────────────────────────────────────────────────────

    @abstractmethod
    def entity_count(self) -> int:
        """Number of distinct entities in the substrate."""
        ...

    @abstractmethod
    def dimensions(self) -> List[str]:
        """
        Populated dimensions, lowercase.
        e.g. ["who", "what", "when", "where"]
        Only dimensions that have at least one fact are returned.
        Missing dimensions are absent — not present with zero count.
        """
        ...

    @abstractmethod
    def provenance(self) -> ProvenanceRecord:
        """
        Everything known about how this substrate was built.
        The adapter reads from the data, config, or both.
        The caller never touches config directly.

        Required by the SRF builder to construct a valid packet.
        If a field is unknown, return None for that field —
        do not fabricate values.

        provenance_source must always be set explicitly:
            "substrate" if identity came from ingest-time artifacts
            "binding"   if identity came from an adapter binding manifest
        """
        ...

    # ── Routing ───────────────────────────────────────────────────────────────

    @abstractmethod
    def query(self, peirce_string: str, limit: int = 100) -> QueryResult:
        """
        Execute a Peirce query. Returns entity_ids + total count.

        peirce_string — a valid Peirce expression
                        e.g. 'WHO.artist = "Miles Davis" AND WHEN.year = "1959"'
        limit         — max entity_ids to return; count reflects true total

        Raises:
            PeirceParseError    — malformed expression (from snf-peirce)
            PeirceDiscoveryError — discovery expression passed to query()
        """
        ...

    @abstractmethod
    def discover(self, expression: str, limit: Optional[int] = None) -> DiscoverResult:
        """
        Execute a Peirce discovery expression.

        Expressions:
            *               — all dimensions with fact counts
            WHO|*           — all fields in WHO
            WHO|artist|*    — all values for WHO.artist
        """
        ...

    # ── UI support ────────────────────────────────────────────────────────────

    @abstractmethod
    def affordances(self) -> Dict[str, Dict]:
        """
        Field metadata per dimension. Used by the Reckoner chip UI
        to know what constraints are expressible.

        Return shape:
        {
            "WHO": {
                "artist": {
                    "fact_count":      833,
                    "distinct_values": 312,
                    "value_type":      "text"   # text | enum | number | date
                }
            },
            "WHEN": { ... },
            ...
        }

        value_type is inferred by the adapter from field names and
        cardinality — the caller does not need to know how.
        """
        ...

    @abstractmethod
    def values(
        self,
        dimension: str,
        field:     str,
        limit:     int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Distinct values for a specific field, with entity counts.

        Return shape:
        [
            { "value": "Miles Davis", "count": 14 },
            { "value": "John Coltrane", "count": 9 },
            ...
        ]

        Ordered by count descending.
        """
        ...

    @abstractmethod
    def hydrate(
        self,
        entity_ids:          List[str],
        matched_coordinates: Dict[str, List[str]],
        semantic_keys:       Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch full coordinates for matched entity IDs.

        entity_ids
            Matched entity IDs from query().

        matched_coordinates
            Computed by the application layer from the Peirce string.
            Shape: { entity_id: ["WHO|artist|Miles Davis", ...] }
            The adapter staples this onto result objects.
            The adapter does NOT compute it.

        semantic_keys
            Optional list of semantic keys to include in returned coordinates,
            e.g. ["artist", "released"]. These are the key portion of a
            coordinate — dimension-agnostic, matched against semantic_key
            in the spoke table. None = include all.

        Return shape:
        [
            {
                "id": "music:album:001",
                "coordinates": {
                    "WHO":  [{ "field": "artist",   "value": "Miles Davis",
                               "coordinate": "WHO|artist|Miles Davis" }],
                    "WHEN": [{ "field": "released",  "value": "1959",
                               "coordinate": "WHEN|released|1959" }]
                },
                "matched_because": [
                    { "dimension": "WHO", "field": "artist",
                      "value": "Miles Davis",
                      "coordinate": "WHO|artist|Miles Davis",
                      "matched": True }
                ]
            },
            ...
        ]
        """
        ...
