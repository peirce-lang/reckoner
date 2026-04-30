"""
duckdb_adapter.py — DuckDBAdapter

Concrete SubstrateAdapter backed by snf-peirce's in-process DuckDB Substrate.
Covers CSV directories and any substrate already compiled by snf-peirce.

This adapter owns the three raw SQL operations that previously lived inline
in reckoner_api.py: affordances(), values(), and hydrate(). Everything else
delegates directly to snf-peirce.

Dependencies:
    pip install snf-peirce
    (DuckDB is bundled with snf-peirce — no separate install needed)

Usage:
    adapter = DuckDBAdapter.from_csv("substrates/discogs/")
    adapter = DuckDBAdapter.from_substrate(already_compiled_substrate)

Provenance source:
    This adapter is Source 1 (substrate-bound ingest metadata). Identity
    fields are read from lens.json (normative sidecar) and substrate.lens_id.
    provenance() always emits provenance_source = "substrate".
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from adapter import (
    SubstrateAdapter,
    QueryResult,
    DiscoverResult,
    ProvenanceRecord,
)

try:
    from snf_peirce import query as peirce_query, discover as peirce_discover
    from snf_peirce.compile import Substrate
except ImportError:
    raise ImportError(
        "snf-peirce required. Install with:\n"
        "  pip install snf-peirce"
    )


class DuckDBAdapter(SubstrateAdapter):
    """
    SubstrateAdapter backed by snf-peirce's in-process DuckDB Substrate.

    The Substrate object is held internally. Callers never touch it.
    All raw SQL (affordances, values, hydrate) is encapsulated here —
    it does not exist in any application layer.

    This is a Source 1 adapter. Identity comes from substrate-bound
    ingest metadata (lens.json sidecar + substrate.lens_id).
    provenance_source is always "substrate".
    """

    def __init__(self, substrate: Substrate, source_path: Optional[str] = None):
        """
        Prefer the factory methods from_csv() and from_substrate()
        over calling __init__ directly.

        substrate    — a compiled snf-peirce Substrate
        source_path  — path the substrate was loaded from, if known
                       used to populate provenance
        """
        self._substrate  = substrate
        self._source_path = source_path
        self._lens_id    = self._read_lens_id()

    # ── Factory methods ───────────────────────────────────────────────────────

    @classmethod
    def from_csv(cls, path: str) -> "DuckDBAdapter":
        """
        Load a substrate from a CSV spoke directory.

        Expected layout:
            path/
                snf_who.csv
                snf_what.csv
                ...
                lens.json     <- required, supplies lens_id and provenance
        """
        import duckdb
        import pandas as pd
        from pathlib import Path as _Path

        base = _Path(path)

        # Read lens_id from lens.json
        lens_file = base / "lens.json"
        if not lens_file.exists():
            raise ValueError(f"lens.json not found in {path}")
        lens_data = json.loads(lens_file.read_text())
        lens_id = lens_data.get("lens_id")
        if not lens_id:
            raise ValueError(f"lens.json in {path} has no lens_id field")

        # Load all spoke CSVs into a single in-memory DuckDB table
        spoke_files = sorted(base.glob("snf_*.csv"))
        if not spoke_files:
            raise ValueError(f"No snf_*.csv files found in {path}")

        frames = [pd.read_csv(f) for f in spoke_files]
        df = pd.concat(frames, ignore_index=True)

        conn = duckdb.connect()
        conn.execute("CREATE TABLE snf_spoke AS SELECT * FROM df")

        substrate = Substrate(conn, lens_id, source_path=path)
        return cls(substrate, source_path=path)

    @classmethod
    def from_substrate(cls, substrate: Substrate) -> "DuckDBAdapter":
        """
        Wrap an already-compiled snf-peirce Substrate.
        Use when the substrate was compiled in-process rather than loaded from disk.
        """
        return cls(substrate, source_path=None)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _read_lens_id(self) -> str:
        """
        Read lens_id from the substrate data.
        Raises ValueError if missing or inconsistent — fail at load time,
        never silently at query time.
        """
        lens_id = getattr(self._substrate, "lens_id", None)
        if not lens_id:
            raise ValueError(
                "Substrate has no lens_id. "
                "Substrate was not compiled correctly or is missing lens metadata."
            )
        return lens_id

    def _read_lens_json(self) -> dict:
        """
        Attempt to read lens.json from the source path.
        Returns empty dict if not found or not parseable.
        Lens.json is optional — its absence is not an error.
        """
        if not self._source_path:
            return {}
        lens_file = Path(self._source_path) / "lens.json"
        if not lens_file.exists():
            return {}
        try:
            return json.loads(lens_file.read_text())
        except Exception:
            return {}

    @staticmethod
    def _infer_value_type(field_name: str, distinct_values: int) -> str:
        """
        Infer value_type from field name heuristics and cardinality.
        Matches the logic previously inline in reckoner_api.py affordances().

        Returns one of: text | enum | number | date
        """
        name = field_name.lower()
        if any(kw in name for kw in ["year", "date", "month", "day", "release", "activity"]):
            return "date"
        if any(kw in name for kw in ["count", "amount", "price", "cmc", "size"]):
            return "number"
        if distinct_values <= 25:
            return "enum"
        return "text"

    # ── Identity ──────────────────────────────────────────────────────────────

    @property
    def lens_id(self) -> str:
        return self._lens_id

    # ── Metadata ──────────────────────────────────────────────────────────────

    def entity_count(self) -> int:
        return self._substrate.count()

    def dimensions(self) -> List[str]:
        """
        Returns populated dimensions as lowercase strings.
        Delegates to snf-peirce Substrate which already omits empty dimensions.
        """
        return [d.lower() for d in self._substrate.dimensions()]

    def provenance(self) -> ProvenanceRecord:
        """
        Assembles provenance from two sources:
            1. lens.json on disk (if present) — source, translated_by, lens_label
            2. substrate data — lens_id (authoritative)

        Both are Source 1 (substrate-bound ingest metadata).
        provenance_source is always "substrate" for this adapter.

        lens.json is optional. Missing fields return None — never fabricated.
        """
        lens_data = self._read_lens_json()

        return ProvenanceRecord(
            lens_id            = self._lens_id,
            provenance_source  = "substrate",
            source             = lens_data.get("source") or lens_data.get("authority") or None,
            translated_by      = lens_data.get("translated_by") or None,
            lens_label         = lens_data.get("label") or lens_data.get("lens_label") or None,
            extra              = {
                "source_path": self._source_path,
            } if self._source_path else {},
        )

    # ── Routing ───────────────────────────────────────────────────────────────

    def query(self, peirce_string: str, limit: int = 100) -> QueryResult:
        """
        Execute a Peirce query via snf-peirce.
        Exceptions from snf-peirce (PeirceParseError, PeirceDiscoveryError)
        are not caught here — they propagate to the application layer.
        """
        result = peirce_query(self._substrate, peirce_string, limit=limit)
        return QueryResult(
            entity_ids = result.entity_ids,
            count      = result.count,
        )

    def discover(self, expression: str, limit: Optional[int] = None) -> DiscoverResult:
        """
        Execute a Peirce discovery expression via snf-peirce.
        """
        result = peirce_discover(self._substrate, expression, limit=limit)
        return DiscoverResult(
            scope     = result.scope,
            dimension = result.dimension,
            field     = result.field,
            rows      = result.rows,
        )

    # ── UI support ────────────────────────────────────────────────────────────

    def affordances(self) -> Dict[str, Dict]:
        """
        Field metadata per dimension for the Reckoner chip UI.
        Queries snf_spoke directly via the substrate's DuckDB connection.
        """
        result = {}
        dims   = self._substrate.dimensions()

        for dim in dims:
            dim_upper = dim.upper()
            result[dim_upper] = {}

            rows = self._substrate._conn.execute(
                "SELECT semantic_key, "
                "COUNT(DISTINCT entity_id) as distinct_entities, "
                "COUNT(*) as fact_count "
                "FROM snf_spoke "
                "WHERE dimension = ? AND lens_id = ? "
                "GROUP BY semantic_key "
                "ORDER BY fact_count DESC",
                [dim, self._lens_id]
            ).fetchall()

            for semantic_key, distinct_entities, fact_count in rows:
                field_name = (
                    semantic_key.split(".")[-1]
                    if "." in semantic_key
                    else semantic_key
                )
                result[dim_upper][field_name] = {
                    "fact_count":      fact_count,
                    "distinct_values": distinct_entities,
                    "value_type":      self._infer_value_type(field_name, distinct_entities),
                }

        return result

    def values(
        self,
        dimension: str,
        field:     str,
        limit:     int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Distinct values for a specific field, ordered by count descending.
        """
        rows = self._substrate._conn.execute(
            "SELECT value, COUNT(DISTINCT entity_id) as cnt "
            "FROM snf_spoke "
            "WHERE dimension = ? AND semantic_key = ? AND lens_id = ? "
            "GROUP BY value "
            "ORDER BY cnt DESC "
            "LIMIT ?",
            [dimension.lower(), field, self._lens_id, limit]
        ).fetchall()

        return [{"value": row[0], "count": row[1]} for row in rows]

    def hydrate(
        self,
        entity_ids:          List[str],
        matched_coordinates: Dict[str, List[str]],
        semantic_keys:       Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch full coordinates for matched entity IDs.
        matched_coordinates is passed in from the application layer —
        this adapter staples it onto results, never recomputes it.
        """
        if not entity_ids:
            return []

        placeholders = ", ".join("?" * len(entity_ids))
        rows = self._substrate._conn.execute(
            f"SELECT entity_id, dimension, semantic_key, value, coordinate "
            f"FROM snf_spoke "
            f"WHERE entity_id IN ({placeholders}) "
            f"AND lens_id = ? "
            f"ORDER BY entity_id, dimension, semantic_key",
            entity_ids + [self._lens_id]
        ).fetchall()

        # Group facts by entity_id
        by_entity: Dict[str, Dict[str, List[dict]]] = {
            eid: {} for eid in entity_ids
        }

        for entity_id, dimension, semantic_key, value, coordinate in rows:
            # Apply semantic_keys filter if specified
            if semantic_keys:
                key_part = (
                    semantic_key.split(".")[-1]
                    if "." in semantic_key
                    else semantic_key
                )
                if key_part not in semantic_keys and semantic_key not in semantic_keys:
                    continue

            dim_upper  = dimension.upper()
            field_name = (
                semantic_key.split(".")[-1]
                if "." in semantic_key
                else semantic_key
            )

            if dim_upper not in by_entity[entity_id]:
                by_entity[entity_id][dim_upper] = []

            by_entity[entity_id][dim_upper].append({
                "field":      field_name,
                "value":      value,
                "coordinate": coordinate,
            })

        # Build result objects, preserving input order
        results = []
        for entity_id in entity_ids:
            # Build matched_because from passed-in coordinates
            matched = []
            for coord in matched_coordinates.get(entity_id, []):
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
                    matched.append({"coordinate": coord, "matched": True})

            results.append({
                "id":              entity_id,
                "coordinates":     by_entity.get(entity_id, {}),
                "matched_because": matched,
            })

        return results
