"""
postgres_adapter.py — PostgresAdapter

Concrete SubstrateAdapter backed by a Postgres SNF substrate.

Three spoke schema shapes exist — the adapter detects which shape it is
dealing with at instantiation time and routes internally to the correct
execution path. The caller sees no difference.

SHAPE A — "split" (legal schema):
    Columns: entity_id, semantic_key, value, coordinate
    Coordinate column: dot-equals format (WHAT.matter_type=advisory) — stale,
                       do not use for routing. Route via semantic_key + value.
    Hub: entity_id, grain

SHAPE B — "coordinate_only" (dms, dms_m schemas):
    Columns: document_id, coordinate
    Coordinate column: pipe format (WHAT|matter_type|advisory) — correct.
                       Route by parsing coordinate.
    Hub: document_id, grain

SHAPE C — "model_builder" (substrates produced by model_builder.py C4 emitter):
    Columns: entity_id, dimension, semantic_key, value, coordinate,
             lens_id, translator_version
    Coordinate column: pipe format — correct.
    Hub: entity_id, nucleus, label, sublabel, lens_id, translator_version
    Routes identically to Shape A (split columns).
    Supports from_substrate() — lens_id and translator_version are stamped.

Routing form (all shapes):
    Anchored JOIN — not INTERSECT. Benchmark (snf_benchmark_v2.sql) showed
    INTERSECT runs at 165-200ms on Postgres while the equivalent JOIN form
    runs at 1-40ms. Postgres cannot reorder INTERSECT chains by selectivity;
    JOIN exposes the query shape to the optimizer's join reordering logic.
    The most selective dimension is chosen as the anchor via cardinality probe.

Provenance source:
    Shape A/B: Source 2 (binding manifest). Use from_binding().
    Shape C:   Source 1 (substrate). Use from_substrate() — normative path,
               now unblocked for model_builder.py substrates.

    See Result Set Identity Model — Addendum: Allowed Provenance Sources.

Dependencies:
    pip install psycopg2-binary

Usage:
    import psycopg2
    conn = psycopg2.connect("host=... dbname=... user=... password=...")

    # Shape C — model_builder.py substrate (normative, from_substrate)
    adapter = PostgresAdapter.from_substrate(conn, schema="discogs")

    # Shape A — legal schema (binding manifest)
    adapter = PostgresAdapter.from_binding(
        conn=conn,
        schema="legal",
        manifest={
            "substrate_id":       "legal-prod",
            "lens_id":            "legal-v1",
            "translator_version": "1.0.0",
        }
    )

    # Shape B — dms schema (binding manifest)
    adapter = PostgresAdapter.from_binding(
        conn=conn,
        schema="dms",
        manifest={
            "substrate_id":       "dms-prod",
            "lens_id":            "dms-v1",
            "translator_version": "1.0.0",
        }
    )
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Tuple

from adapter import (
    SubstrateAdapter,
    QueryResult,
    DiscoverResult,
    ProvenanceRecord,
)

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    raise ImportError(
        "psycopg2 required. Install with:\n"
        "  pip install psycopg2-binary"
    )

# Canonical spoke table names — same across all Postgres SNF schemas
_SPOKE_TABLES = ["snf_who", "snf_what", "snf_when", "snf_where", "snf_why", "snf_how"]

_TABLE_TO_DIM = {
    "snf_who":   "who",
    "snf_what":  "what",
    "snf_when":  "when",
    "snf_where": "where",
    "snf_why":   "why",
    "snf_how":   "how",
}

_DIM_TO_TABLE = {v: k for k, v in _TABLE_TO_DIM.items()}

SpokeShape = Literal["split", "coordinate_only"]


class PostgresAdapter(SubstrateAdapter):
    """
    SubstrateAdapter backed by a Postgres SNF substrate.

    Detects spoke schema shape at instantiation. Routes internally via
    the correct execution path. The SubstrateAdapter interface is identical
    regardless of which shape the underlying schema uses.
    """

    def __init__(
        self,
        conn,
        schema:     str,
        provenance: ProvenanceRecord,
    ):
        """
        Internal constructor. Takes an already-resolved ProvenanceRecord.
        Use from_binding() or from_substrate() — not this directly.
        """
        self._conn       = conn
        self._schema     = schema
        self._provenance = provenance
        self._lens_id    = provenance.lens_id
        self._shape      = self._detect_shape()

    # ── Factory methods ───────────────────────────────────────────────────────

    @classmethod
    def from_binding(
        cls,
        conn,
        schema:   str,
        manifest: Dict[str, str],
    ) -> "PostgresAdapter":
        """
        Transitional factory. Use when the substrate was not stamped at ingest.

        manifest must contain:
            substrate_id       — stable name for this corpus endpoint
            lens_id            — lens this substrate was compiled under
            translator_version — version of translator that produced the coordinates

        provenance_source will be "binding". Declared transitional debt.
        The normative path is from_substrate() once the emitter stamps snf_hub.
        """
        required = ("substrate_id", "lens_id", "translator_version")
        missing  = [k for k in required if not manifest.get(k)]
        if missing:
            raise ValueError(
                f"Binding manifest is missing required fields: {missing}. "
                "These fields must be explicitly declared in the manifest. "
                "They may not be derived, defaulted, or omitted. "
                f"This is a CADP ingest responsibility not yet fulfilled for schema '{schema}'. "
                "See Result Set Identity Model — Addendum: Allowed Provenance Sources."
            )

        prov = ProvenanceRecord(
            lens_id           = manifest["lens_id"],
            provenance_source = "binding",
            extra             = {
                "substrate_id":       manifest["substrate_id"],
                "translator_version": manifest["translator_version"],
                "schema":             schema,
            },
        )
        return cls(conn, schema, prov)

    @classmethod
    def from_substrate(cls, conn, schema: str) -> "PostgresAdapter":
        """
        Normative factory. Reads identity from snf_hub (Source 1).

        Available for Shape C substrates produced by model_builder.py C4 emitter,
        which stamps lens_id and translator_version into snf_hub at ingest time.

        Still blocked for Shape A (legal) and Shape B (dms, dms_m) substrates
        which were not stamped at ingest. Use from_binding() for those.
        """
        # Check if snf_hub has the required stamped columns
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'snf_hub'",
                    (schema,)
                )
                hub_cols = {row[0] for row in cur.fetchall()}
        except Exception:
            conn.rollback()
            hub_cols = set()

        if "lens_id" not in hub_cols or "translator_version" not in hub_cols:
            raise NotImplementedError(
                f"PostgresAdapter.from_substrate() is not available for schema '{schema}'. "
                "snf_hub does not have lens_id and translator_version columns — "
                "this substrate was not produced by model_builder.py. "
                "Use from_binding() with an explicit manifest instead. "
                "See Result Set Identity Model — Addendum: Allowed Provenance Sources."
            )

        # Read identity from snf_hub (Source 1 — normative)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT lens_id, translator_version FROM "{schema}".snf_hub LIMIT 1'
                )
                row = cur.fetchone()
        except Exception:
            conn.rollback()
            raise ValueError(
                f"Could not read provenance from \"{schema}\".snf_hub. "
                "Ensure the substrate was loaded correctly."
            )

        if not row:
            raise ValueError(
                f"snf_hub in schema '{schema}' is empty — cannot read provenance."
            )

        lens_id, translator_version = row[0], row[1]

        prov = ProvenanceRecord(
            lens_id           = lens_id,
            provenance_source = "substrate",
            extra             = {
                "substrate_id":       schema,
                "translator_version": translator_version,
                "schema":             schema,
            },
        )
        return cls(conn, schema, prov)

    # ── Shape detection ───────────────────────────────────────────────────────

    def _detect_shape(self) -> SpokeShape:
        """
        Detect spoke schema shape by inspecting snf_what column names.
        Fails loudly — never silently falls back to a wrong execution path.

        Shape A ("split"):           entity_id, semantic_key, value, coordinate
        Shape B ("coordinate_only"): document_id, coordinate
        Shape C ("model_builder"):   entity_id, dimension, semantic_key, value,
                                     coordinate, lens_id, translator_version
                                     (produced by model_builder.py C4 emitter)
        """
        rows = self._execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = 'snf_what' "
            "ORDER BY ordinal_position",
            (self._schema,)
        )
        cols = {row[0] for row in rows}

        if not cols:
            raise ValueError(
                f"Schema '{self._schema}' has no snf_what table or is not accessible."
            )

        # Shape C — model_builder.py C4 output (7 columns, fully stamped)
        if "lens_id" in cols and "translator_version" in cols and "entity_id" in cols:
            return "split"  # routes identically to Shape A — same column names

        if "semantic_key" in cols and "value" in cols and "entity_id" in cols:
            return "split"

        if "coordinate" in cols and "document_id" in cols:
            return "coordinate_only"

        raise ValueError(
            f"Schema '{self._schema}'.snf_what has unrecognised column set: {cols}. "
            "Expected Shape A (entity_id, semantic_key, value, coordinate), "
            "Shape B (document_id, coordinate), or "
            "Shape C (entity_id, dimension, semantic_key, value, coordinate, lens_id, translator_version)."
        )

    @property
    def _entity_id_col(self) -> str:
        return "entity_id" if self._shape == "split" else "document_id"

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _table(self, dimension: str) -> str:
        table = _DIM_TO_TABLE.get(dimension.lower())
        if not table:
            raise ValueError(f"Unknown dimension: '{dimension}'")
        return f"{self._schema}.{table}"

    def _execute(self, sql: str, params: tuple = ()) -> List[Tuple]:
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        except Exception:
            self._conn.rollback()
            raise

    def _populated_dimensions(self) -> List[str]:
        populated = []
        for table_name, dim in _TABLE_TO_DIM.items():
            fq   = f"{self._schema}.{table_name}"
            rows = self._execute(f"SELECT 1 FROM {fq} LIMIT 1")
            if rows:
                populated.append(dim)
        return populated

    @staticmethod
    def _infer_value_type(field_name: str, distinct_values: int) -> str:
        name = field_name.lower()
        if any(kw in name for kw in ["year", "date", "month", "day", "release", "activity"]):
            return "date"
        if any(kw in name for kw in ["count", "amount", "price", "size"]):
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
        col  = self._entity_id_col
        rows = self._execute(
            f"SELECT COUNT(DISTINCT {col}) FROM {self._schema}.snf_hub"
        )
        return rows[0][0] if rows else 0

    def dimensions(self) -> List[str]:
        return self._populated_dimensions()

    def provenance(self) -> ProvenanceRecord:
        return self._provenance

    # ── Routing ───────────────────────────────────────────────────────────────

    def query(self, peirce_string: str, limit: int = 100) -> QueryResult:
        constraints = _parse_peirce(peirce_string)
        if not constraints:
            raise ValueError(
                f"No parseable constraints in Peirce string: '{peirce_string}'"
            )
        entity_ids, trace = self._route(constraints)
        return QueryResult(
            entity_ids = entity_ids[:limit],
            count      = len(entity_ids),
            trace      = trace,
        )

    def _route(self, constraints: List[Dict[str, str]]) -> tuple:
        standard, text_search = _split_constraints(constraints)
        if self._shape == "split":
            return self._route_split(standard, text_search)
        return self._route_coordinate_only(standard, text_search)

    def _route_split(self, constraints: List[Dict[str, str]], text_constraints: List[Dict[str, str]] = None) -> List[str]:
        """
        Anchored JOIN routing for Shape A (split columns).
        Anchor = most selective dimension by cardinality probe.
        Mirrors SNF_JOIN form from snf_benchmark_v2.sql.
        CONTAINS/PREFIX constraints are applied as a post-filter subquery.
        """
        text_constraints = text_constraints or []
        by_dim = _group_by_dim(constraints)

        # Pure text search — no standard constraints, only CONTAINS/PREFIX
        if not by_dim and text_constraints:
            # Use first text constraint's dimension table as anchor
            tc         = text_constraints[0]
            table_name = _DIM_TO_TABLE.get(tc["dimension"].lower())
            if not table_name:
                return [], []
            fq      = f"{self._schema}.{table_name}"
            pattern = f"%{tc['value']}%" if tc["op"] == "contains" else f"{tc['value']}%"
            rows    = self._execute(
                f"SELECT DISTINCT entity_id FROM {fq} "
                f"WHERE semantic_key = %s AND value ILIKE %s",
                (tc["field"], pattern)
            )
            entity_ids = [str(row[0]) for row in rows]
            # Apply any remaining text constraints as post-filters
            for tc2 in text_constraints[1:]:
                tn2     = _DIM_TO_TABLE.get(tc2["dimension"].lower())
                fq2     = f"{self._schema}.{tn2}"
                pat2    = f"%{tc2['value']}%" if tc2["op"] == "contains" else f"{tc2['value']}%"
                if not entity_ids:
                    break
                phs  = ", ".join(["%s"] * len(entity_ids))
                rows2 = self._execute(
                    f"SELECT DISTINCT entity_id FROM {fq2} "
                    f"WHERE entity_id IN ({phs}) "
                    f"AND semantic_key = %s AND value ILIKE %s",
                    (*entity_ids, tc2["field"], pat2)
                )
                entity_ids = [str(row[0]) for row in rows2]
            return entity_ids, []

        # Cardinality probe
        dim_counts = {}
        for dim, fields in by_dim.items():
            table        = self._table(dim)
            where_parts  = []
            probe_params: List[Any] = []
            for field, values in fields.items():
                phs = ", ".join(["%s"] * len(values))
                where_parts.append(f"(semantic_key = %s AND value IN ({phs}))")
                probe_params.append(field)
                probe_params.extend(values)
            where_sql = " OR ".join(where_parts)
            count = self._execute(
                f"SELECT COUNT(DISTINCT entity_id) FROM {table} WHERE {where_sql}",
                tuple(probe_params)
            )[0][0]
            dim_counts[dim] = count

        ordered_dims = sorted(by_dim.keys(), key=lambda d: dim_counts[d])
        anchor_dim   = ordered_dims[0]
        anchor_table = self._table(anchor_dim)

        anchor_field_items          = list(by_dim[anchor_dim].items())
        anchor_field, anchor_values = anchor_field_items[0]

        if len(anchor_values) == 1:
            anchor_where  = "a.semantic_key = %s AND a.value = %s"
            params: List[Any] = [anchor_field, anchor_values[0]]
        else:
            phs = ", ".join(["%s"] * len(anchor_values))
            anchor_where  = f"a.semantic_key = %s AND a.value IN ({phs})"
            params = [anchor_field] + list(anchor_values)

        join_clauses  = []
        alias_counter = 0

        # Remaining fields in anchor dimension — self-joins
        for field, values in anchor_field_items[1:]:
            alias_counter += 1
            alias = f"t{alias_counter}"
            if len(values) == 1:
                join_clauses.append(
                    f"JOIN {anchor_table} {alias} "
                    f"ON {alias}.entity_id = a.entity_id "
                    f"AND {alias}.semantic_key = %s AND {alias}.value = %s"
                )
                params += [field, values[0]]
            else:
                phs = ", ".join(["%s"] * len(values))
                join_clauses.append(
                    f"JOIN {anchor_table} {alias} "
                    f"ON {alias}.entity_id = a.entity_id "
                    f"AND {alias}.semantic_key = %s AND {alias}.value IN ({phs})"
                )
                params += [field] + list(values)

        # Remaining dimensions
        for dim in ordered_dims[1:]:
            table  = self._table(dim)
            for field, values in by_dim[dim].items():
                alias_counter += 1
                alias = f"t{alias_counter}"
                if len(values) == 1:
                    join_clauses.append(
                        f"JOIN {table} {alias} "
                        f"ON {alias}.entity_id = a.entity_id "
                        f"AND {alias}.semantic_key = %s AND {alias}.value = %s"
                    )
                    params += [field, values[0]]
                else:
                    phs = ", ".join(["%s"] * len(values))
                    join_clauses.append(
                        f"JOIN {table} {alias} "
                        f"ON {alias}.entity_id = a.entity_id "
                        f"AND {alias}.semantic_key = %s AND {alias}.value IN ({phs})"
                    )
                    params += [field] + list(values)

        joins_sql = "\n            ".join(join_clauses)

        # Apply CONTAINS/PREFIX text search as additional WHERE clauses
        text_where = ""
        if text_constraints:
            # Find the right table for each text constraint
            for tc in text_constraints:
                table_name = _DIM_TO_TABLE.get(tc["dimension"].lower())
                if not table_name:
                    continue
                fq      = f"{self._schema}.{table_name}"
                pattern = f"%{tc['value']}%" if tc["op"] == "contains" else f"{tc['value']}%"
                text_where += (
                    f" AND a.entity_id IN ("
                    f"SELECT entity_id FROM {fq} "
                    f"WHERE semantic_key = %s AND value ILIKE %s)"
                )
                params += [tc["field"], pattern]

        sql = (
            f"SELECT DISTINCT a.entity_id "
            f"FROM {anchor_table} a "
            f"{joins_sql} "
            f"WHERE {anchor_where}{text_where}"
        )
        rows = self._execute(sql, tuple(params))
        entity_ids = [str(row[0]) for row in rows]
        # Trace for split shape — probe counts only (stepdown not yet implemented for Shape A)
        trace = [
            {
                "dimension":   dim.upper(),
                "cardinality": dim_counts[dim],
                "fields":      [{"field": f, "values": list(v)} for f, v in by_dim[dim].items()],
            }
            for dim in ordered_dims
        ]
        return entity_ids, trace

    def _route_coordinate_only(self, constraints: List[Dict[str, str]], text_constraints: List[Dict[str, str]] = None) -> List[str]:
        """
        Anchored JOIN routing for Shape B (coordinate-only, pipe format).
        Same JOIN form as _route_split but matches against coordinate string.
        """
        by_dim = _group_by_dim(constraints)
        text_constraints = text_constraints or []

        # Pure text search — no standard constraints, only CONTAINS/PREFIX
        if not by_dim and text_constraints:
            tc         = text_constraints[0]
            table_name = _DIM_TO_TABLE.get(tc["dimension"].lower())
            if not table_name:
                return [], []
            fq      = f"{self._schema}.{table_name}"
            pattern = f"%{tc['value']}%" if tc["op"] == "contains" else f"{tc['value']}%"
            dim_upper = tc["dimension"].upper()
            rows    = self._execute(
                f"SELECT DISTINCT document_id FROM {fq} "
                f"WHERE coordinate LIKE %s "
                f"AND SPLIT_PART(coordinate,'|',3) ILIKE %s",
                (f"{dim_upper}|{tc['field']}|%", pattern)
            )
            entity_ids = [str(row[0]) for row in rows]
            for tc2 in text_constraints[1:]:
                tn2       = _DIM_TO_TABLE.get(tc2["dimension"].lower())
                fq2       = f"{self._schema}.{tn2}"
                pat2      = f"%{tc2['value']}%" if tc2["op"] == "contains" else f"{tc2['value']}%"
                dim2_upper = tc2["dimension"].upper()
                if not entity_ids:
                    break
                phs   = ", ".join(["%s"] * len(entity_ids))
                rows2 = self._execute(
                    f"SELECT DISTINCT document_id FROM {fq2} "
                    f"WHERE document_id IN ({phs}) "
                    f"AND coordinate LIKE %s "
                    f"AND SPLIT_PART(coordinate,'|',3) ILIKE %s",
                    (*entity_ids, f"{dim2_upper}|{tc2['field']}|%", pat2)
                )
                entity_ids = [str(row[0]) for row in rows2]
            return entity_ids, []

        # Cardinality probe
        dim_counts = {}
        for dim, fields in by_dim.items():
            table      = self._table(dim)
            coord_list = _build_coordinate_list(dim, fields)
            phs        = ", ".join(["%s"] * len(coord_list))
            count = self._execute(
                f"SELECT COUNT(DISTINCT document_id) FROM {table} "
                f"WHERE coordinate IN ({phs})",
                tuple(coord_list)
            )[0][0]
            dim_counts[dim] = count

        ordered_dims = sorted(by_dim.keys(), key=lambda d: dim_counts[d])
        anchor_dim   = ordered_dims[0]
        anchor_table = self._table(anchor_dim)

        # ── Stepdown probe — real Portolan narrowing counts ───────────────────
        eid = self._entity_id_col
        anchor_field_items          = list(by_dim[anchor_dim].items())
        anchor_field, anchor_values = anchor_field_items[0]
        anchor_coords = [
            f"{anchor_dim.upper()}|{anchor_field}|{v}" for v in anchor_values
        ]
        stepdown_counts = []
        if len(ordered_dims) > 1:
            anchor_coords_probe = _build_coordinate_list(anchor_dim, by_dim[anchor_dim])
            phs_a = ", ".join(["%s"] * len(anchor_coords_probe))
            count_a = self._execute(
                f"SELECT COUNT(DISTINCT {eid}) FROM {anchor_table} "
                f"WHERE coordinate IN ({phs_a})",
                tuple(anchor_coords_probe)
            )[0][0]
            stepdown_counts.append(count_a)
            running_dims = [anchor_dim]
            for dim in ordered_dims[1:]:
                running_dims.append(dim)
                base_table  = self._table(running_dims[0])
                base_coords = _build_coordinate_list(running_dims[0], by_dim[running_dims[0]])
                phs_b       = ", ".join(["%s"] * len(base_coords))
                step_joins:  List[str] = []
                step_params: List[Any] = []
                step_alias   = 0
                for d in running_dims[1:]:
                    step_alias += 1
                    alias  = f"s{step_alias}"
                    t      = self._table(d)
                    coords = _build_coordinate_list(d, by_dim[d])
                    phs_c  = ", ".join(["%s"] * len(coords))
                    step_joins.append(
                        f"JOIN {t} {alias} ON {alias}.{eid} = base.{eid} "
                        f"AND {alias}.coordinate IN ({phs_c})"
                    )
                    step_params.extend(coords)
                count_step = self._execute(
                    f"SELECT COUNT(DISTINCT base.{eid}) "
                    f"FROM {base_table} base {' '.join(step_joins)} "
                    f"WHERE base.coordinate IN ({phs_b})",
                    tuple(step_params + list(base_coords))
                )[0][0]
                stepdown_counts.append(count_step)
        else:
            stepdown_counts.append(dim_counts[ordered_dims[0]])

        if len(anchor_coords) == 1:
            anchor_where   = "a.coordinate = %s"
            anchor_params: List[Any] = list(anchor_coords)
        else:
            phs = ", ".join(["%s"] * len(anchor_coords))
            anchor_where   = f"a.coordinate IN ({phs})"
            anchor_params  = list(anchor_coords)

        join_clauses  = []
        join_params:  List[Any] = []
        alias_counter = 0

        for field, values in anchor_field_items[1:]:
            alias_counter += 1
            alias  = f"t{alias_counter}"
            coords = [f"{anchor_dim.upper()}|{field}|{v}" for v in values]
            if len(coords) == 1:
                join_clauses.append(
                    f"JOIN {anchor_table} {alias} "
                    f"ON {alias}.document_id = a.document_id "
                    f"AND {alias}.coordinate = %s"
                )
                join_params.append(coords[0])
            else:
                phs = ", ".join(["%s"] * len(coords))
                join_clauses.append(
                    f"JOIN {anchor_table} {alias} "
                    f"ON {alias}.document_id = a.document_id "
                    f"AND {alias}.coordinate IN ({phs})"
                )
                join_params.extend(coords)

        for dim in ordered_dims[1:]:
            table = self._table(dim)
            for field, values in by_dim[dim].items():
                alias_counter += 1
                alias  = f"t{alias_counter}"
                coords = [f"{dim.upper()}|{field}|{v}" for v in values]
                if len(coords) == 1:
                    join_clauses.append(
                        f"JOIN {table} {alias} "
                        f"ON {alias}.document_id = a.document_id "
                        f"AND {alias}.coordinate = %s"
                    )
                    join_params.append(coords[0])
                else:
                    phs = ", ".join(["%s"] * len(coords))
                    join_clauses.append(
                        f"JOIN {table} {alias} "
                        f"ON {alias}.document_id = a.document_id "
                        f"AND {alias}.coordinate IN ({phs})"
                    )
                    join_params.extend(coords)

        # JOIN params come first — JOINs appear before WHERE in the SQL text
        params: List[Any] = join_params + anchor_params

        joins_sql = "\n            ".join(join_clauses)

        # Apply CONTAINS/PREFIX text search as additional WHERE clauses
        text_constraints = text_constraints or []
        text_where = ""
        for tc in text_constraints:
            table_name = _DIM_TO_TABLE.get(tc["dimension"].lower())
            if not table_name:
                continue
            fq      = f"{self._schema}.{table_name}"
            pattern = f"%{tc['value']}%" if tc["op"] == "contains" else f"{tc['value']}%"
            dim_upper = tc["dimension"].upper()
            text_where += (
                f" AND a.document_id IN ("
                f"SELECT document_id FROM {fq} "
                f"WHERE coordinate LIKE %s "
                f"AND SPLIT_PART(coordinate,'|',3) ILIKE %s)"
            )
            params += [f"{dim_upper}|{tc['field']}|%", pattern]

        sql = (
            f"SELECT DISTINCT a.document_id "
            f"FROM {anchor_table} a "
            f"{joins_sql} "
            f"WHERE {anchor_where}{text_where}"
        )
        rows = self._execute(sql, tuple(params))
        entity_ids = [str(row[0]) for row in rows]

        # Build trace with real stepdown counts
        trace = [
            {
                "dimension":   dim.upper(),
                "cardinality": stepdown_counts[i],
                "fields":      [
                    {"field": f, "values": list(v)}
                    for f, v in by_dim[dim].items()
                ],
            }
            for i, dim in enumerate(ordered_dims)
        ]
        return entity_ids, trace

    # ── Discovery ─────────────────────────────────────────────────────────────

    def discover(self, expression: str, limit: Optional[int] = None) -> DiscoverResult:
        parts = expression.strip().split("|")
        if parts == ["*"]:
            return self._discover_dimensions()
        if len(parts) == 2 and parts[1] == "*":
            return self._discover_fields(parts[0], limit=limit)
        if len(parts) == 3 and parts[2] == "*":
            return self._discover_values(parts[0], parts[1], limit=limit)
        raise ValueError(
            f"Unrecognised discovery expression: '{expression}'. "
            "Expected: * | DIM|* | DIM|field|*"
        )

    def _discover_dimensions(self) -> DiscoverResult:
        rows = []
        for table_name, dim in _TABLE_TO_DIM.items():
            fq    = f"{self._schema}.{table_name}"
            count = self._execute(f"SELECT COUNT(*) FROM {fq}")[0][0]
            if count > 0:
                rows.append({"dimension": dim.upper(), "fact_count": count})
        return DiscoverResult(scope="dimensions", dimension=None, field=None, rows=rows)

    def _discover_fields(
        self, dimension: str, limit: Optional[int] = None
    ) -> DiscoverResult:
        table = self._table(dimension)
        eid   = self._entity_id_col

        if self._shape == "split":
            sql    = (
                f"SELECT semantic_key AS field, "
                f"COUNT(DISTINCT {eid}) AS entity_count, "
                f"COUNT(*) AS fact_count "
                f"FROM {table} GROUP BY semantic_key ORDER BY fact_count DESC"
            )
            params: tuple = ()
        else:
            sql    = (
                f"SELECT SPLIT_PART(coordinate, '|', 2) AS field, "
                f"COUNT(DISTINCT {eid}) AS entity_count, "
                f"COUNT(*) AS fact_count "
                f"FROM {table} WHERE coordinate LIKE %s "
                f"GROUP BY SPLIT_PART(coordinate, '|', 2) ORDER BY fact_count DESC"
            )
            params = (f"{dimension.upper()}|%",)

        if limit:
            sql += f" LIMIT {int(limit)}"

        rows_raw = self._execute(sql, params)
        rows = [
            {"field": r[0], "entity_count": r[1], "fact_count": r[2]}
            for r in rows_raw
        ]
        return DiscoverResult(
            scope="fields", dimension=dimension.upper(), field=None, rows=rows
        )

    def _discover_values(
        self, dimension: str, field: str, limit: Optional[int] = None
    ) -> DiscoverResult:
        table = self._table(dimension)
        eid   = self._entity_id_col

        if self._shape == "split":
            sql    = (
                f"SELECT value, COUNT(DISTINCT {eid}) AS entity_count "
                f"FROM {table} WHERE semantic_key = %s "
                f"GROUP BY value ORDER BY entity_count DESC"
            )
            params: tuple = (field,)
        else:
            sql    = (
                f"SELECT SPLIT_PART(coordinate, '|', 3) AS value, "
                f"COUNT(DISTINCT {eid}) AS entity_count "
                f"FROM {table} WHERE coordinate LIKE %s "
                f"GROUP BY value ORDER BY entity_count DESC"
            )
            params = (f"{dimension.upper()}|{field}|%",)

        if limit:
            sql += f" LIMIT {int(limit)}"

        rows_raw = self._execute(sql, params)
        rows = [{"value": r[0], "count": r[1]} for r in rows_raw]
        return DiscoverResult(
            scope="values", dimension=dimension.upper(), field=field, rows=rows
        )

    # ── UI support ────────────────────────────────────────────────────────────

    def _has_affordances_view(self) -> bool:
        """Check whether a pre-computed snf_affordances materialized view exists."""
        rows = self._execute(
            "SELECT 1 FROM pg_matviews "
            "WHERE schemaname = %s AND matviewname = 'snf_affordances'",
            (self._schema,)
        )
        return len(rows) > 0

    def affordances(self) -> Dict[str, Dict]:
        # Fast path — use materialized view if present
        if self._has_affordances_view():
            return self._affordances_from_view()
        # Slow path — live aggregation
        return self._affordances_live()

    def _affordances_from_view(self) -> Dict[str, Dict]:
        """
        Read affordances from snf_affordances materialized view.
        Expected columns: dimension, field, distinct_entities, fact_count.
        """
        rows = self._execute(
            f"SELECT dimension, field, distinct_entities, fact_count "
            f"FROM {self._schema}.snf_affordances "
            f"ORDER BY dimension, fact_count DESC"
        )
        result: Dict[str, Dict] = {}
        for dimension, field, distinct_entities, fact_count in rows:
            dim_upper = dimension.upper()
            if dim_upper not in result:
                result[dim_upper] = {}
            result[dim_upper][field] = {
                "fact_count":      fact_count,
                "distinct_values": distinct_entities,
                "value_type":      self._infer_value_type(field, distinct_entities),
            }
        return result

    def _affordances_live(self) -> Dict[str, Dict]:
        """Live aggregation fallback — used when no materialized view exists."""
        result = {}
        eid    = self._entity_id_col

        for table_name, dim in _TABLE_TO_DIM.items():
            fq    = f"{self._schema}.{table_name}"
            count = self._execute(f"SELECT COUNT(*) FROM {fq}")[0][0]
            if count == 0:
                continue

            dim_upper = dim.upper()
            result[dim_upper] = {}

            if self._shape == "split":
                rows = self._execute(
                    f"SELECT semantic_key, "
                    f"COUNT(DISTINCT {eid}) AS distinct_entities, "
                    f"COUNT(*) AS fact_count "
                    f"FROM {fq} GROUP BY semantic_key ORDER BY fact_count DESC"
                )
            else:
                rows = self._execute(
                    f"SELECT SPLIT_PART(coordinate, '|', 2) AS semantic_key, "
                    f"COUNT(DISTINCT {eid}) AS distinct_entities, "
                    f"COUNT(*) AS fact_count "
                    f"FROM {fq} WHERE coordinate LIKE %s "
                    f"GROUP BY SPLIT_PART(coordinate, '|', 2) ORDER BY fact_count DESC",
                    (f"{dim_upper}|%",)
                )

            for semantic_key, distinct_entities, fact_count in rows:
                result[dim_upper][semantic_key] = {
                    "fact_count":      fact_count,
                    "distinct_values": distinct_entities,
                    "value_type":      self._infer_value_type(
                        semantic_key, distinct_entities
                    ),
                }

        return result

    def values(
        self,
        dimension: str,
        field:     str,
        limit:     int = 200,
    ) -> List[Dict[str, Any]]:
        table = self._table(dimension)
        eid   = self._entity_id_col

        if self._shape == "split":
            rows = self._execute(
                f"SELECT value, COUNT(DISTINCT {eid}) AS cnt "
                f"FROM {table} WHERE semantic_key = %s "
                f"GROUP BY value ORDER BY cnt DESC LIMIT %s",
                (field, limit)
            )
        else:
            rows = self._execute(
                f"SELECT SPLIT_PART(coordinate, '|', 3) AS value, "
                f"COUNT(DISTINCT {eid}) AS cnt "
                f"FROM {table} WHERE coordinate LIKE %s "
                f"GROUP BY value ORDER BY cnt DESC LIMIT %s",
                (f"{dimension.upper()}|{field}|%", limit)
            )

        return [{"value": row[0], "count": row[1]} for row in rows]

    def values_conditional(
        self,
        dimension:  str,
        field:      str,
        entity_ids: List[str],
        limit:      int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Return value counts for a field filtered to a specific set of entity_ids.
        Used by conditional cardinality in the trie panel.
        """
        if not entity_ids:
            return []

        table = self._table(dimension)
        eid   = self._entity_id_col
        phs   = ", ".join(["%s"] * len(entity_ids))

        if self._shape == "split":
            rows = self._execute(
                f"SELECT value, COUNT(DISTINCT {eid}) AS cnt "
                f"FROM {table} "
                f"WHERE semantic_key = %s "
                f"AND {eid}::TEXT IN ({phs}) "
                f"GROUP BY value ORDER BY cnt DESC LIMIT %s",
                (field, *entity_ids, limit)
            )
        else:
            rows = self._execute(
                f"SELECT SPLIT_PART(coordinate, '|', 3) AS value, "
                f"COUNT(DISTINCT {eid}) AS cnt "
                f"FROM {table} "
                f"WHERE coordinate LIKE %s "
                f"AND {eid} IN ({phs}) "
                f"GROUP BY SPLIT_PART(coordinate, '|', 3) ORDER BY cnt DESC LIMIT %s",
                (f"{dimension.upper()}|{field}|%", *entity_ids, limit)
            )

        return [{"value": row[0], "count": row[1]} for row in rows]

    def hydrate(
        self,
        entity_ids:          List[str],
        matched_coordinates: Dict[str, List[str]],
        semantic_keys:       Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        if not entity_ids:
            return []

        eid          = self._entity_id_col
        placeholders = ", ".join(["%s"] * len(entity_ids))

        if self._shape == "split":
            union_parts = [
                f"SELECT {eid}::TEXT AS entity_id, "
                f"'{dim.upper()}' AS dimension, "
                f"semantic_key, value::TEXT AS value "
                f"FROM {self._schema}.{table_name} "
                f"WHERE {eid}::TEXT IN ({placeholders})"
                for table_name, dim in _TABLE_TO_DIM.items()
            ]
            all_params = tuple(entity_ids) * len(_SPOKE_TABLES)
            union_sql  = " UNION ALL ".join(union_parts)

            rows = self._execute(
                f"SELECT entity_id, dimension, semantic_key, value "
                f"FROM ({union_sql}) AS all_facts "
                f"ORDER BY entity_id, dimension, semantic_key",
                all_params
            )

            by_entity: Dict[str, Dict[str, List[dict]]] = {
                e: {} for e in entity_ids
            }
            for entity_id, dimension, semantic_key, value in rows:
                if semantic_keys and semantic_key not in semantic_keys:
                    continue
                dim_upper = dimension.upper()
                if dim_upper not in by_entity[entity_id]:
                    by_entity[entity_id][dim_upper] = []
                coordinate = f"{dim_upper}|{semantic_key}|{value}"
                by_entity[entity_id][dim_upper].append({
                    "field":      semantic_key,
                    "value":      value,
                    "coordinate": coordinate,
                })

        else:
            union_parts = [
                f"SELECT {eid} AS entity_id, coordinate "
                f"FROM {self._schema}.{table_name} "
                f"WHERE {eid} IN ({placeholders})"
                for table_name in _SPOKE_TABLES
            ]
            all_params = tuple(entity_ids) * len(_SPOKE_TABLES)
            union_sql  = " UNION ALL ".join(union_parts)

            rows = self._execute(
                f"SELECT entity_id, coordinate "
                f"FROM ({union_sql}) AS all_coords "
                f"ORDER BY entity_id, coordinate",
                all_params
            )

            by_entity = {e: {} for e in entity_ids}
            for entity_id, coordinate in rows:
                parts = coordinate.split("|", 2)
                if len(parts) != 3:
                    continue
                dim_upper, semantic_key, value = (
                    parts[0].upper(), parts[1], parts[2]
                )
                if semantic_keys and semantic_key not in semantic_keys:
                    continue
                if dim_upper not in by_entity[entity_id]:
                    by_entity[entity_id][dim_upper] = []
                by_entity[entity_id][dim_upper].append({
                    "field":      semantic_key,
                    "value":      value,
                    "coordinate": coordinate,
                })

        results = []
        for entity_id in entity_ids:
            matched = []
            for coord in matched_coordinates.get(entity_id, []):
                coord_parts = coord.split("|", 2)
                if len(coord_parts) == 3:
                    matched.append({
                        "dimension":  coord_parts[0],
                        "field":      coord_parts[1],
                        "value":      coord_parts[2],
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


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _group_by_dim(
    constraints: List[Dict[str, str]]
) -> Dict[str, Dict[str, List[str]]]:
    """Group constraints into { dimension → { field → [values] } }.
    Only includes eq/gte/lte/gt/lt/not_eq constraints — CONTAINS/PREFIX
    are handled separately via _split_constraints."""
    by_dim: Dict[str, Dict[str, List[str]]] = {}
    for c in constraints:
        if c.get("op") in ("contains", "prefix"):
            continue  # handled separately
        dim   = c["dimension"].lower()
        field = c["field"]
        value = c["value"]
        by_dim.setdefault(dim, {}).setdefault(field, []).append(value)
    return by_dim


def _split_constraints(
    constraints: List[Dict[str, str]]
) -> tuple:
    """Split constraints into (standard, text_search).
    standard:    eq/not_eq/gte/lte/gt/lt — handled by _group_by_dim + JOIN routing
    text_search: contains/prefix — handled by LIKE/ILIKE in a subquery filter
    """
    standard    = [c for c in constraints if c.get("op") not in ("contains", "prefix")]
    text_search = [c for c in constraints if c.get("op") in ("contains", "prefix")]
    return standard, text_search


def _build_text_search_subquery(
    schema: str,
    table_name: str,
    entity_id_col: str,
    text_constraints: List[Dict[str, str]],
    shape: str,
) -> tuple:
    """Build a subquery that filters entity_ids by CONTAINS/PREFIX constraints.
    Returns (sql_fragment, params) where sql_fragment is:
      entity_id_col IN (SELECT {eid} FROM {table} WHERE ...)
    """
    parts  = []
    params: List[Any] = []
    fq = f"{schema}.{table_name}"

    for c in text_constraints:
        field = c["field"]
        value = c["value"]
        op    = c["op"]
        pattern = f"%{value}%" if op == "contains" else f"{value}%"

        if shape == "split":
            parts.append(
                f"{entity_id_col} IN ("
                f"SELECT {entity_id_col} FROM {fq} "
                f"WHERE semantic_key = %s AND value ILIKE %s)"
            )
            params += [field, pattern]
        else:
            dim_upper = c["dimension"].upper()
            parts.append(
                f"{entity_id_col} IN ("
                f"SELECT {entity_id_col} FROM {fq} "
                f"WHERE coordinate LIKE %s AND SPLIT_PART(coordinate,'|',3) ILIKE %s)"
            )
            params += [f"{dim_upper}|{field}|%", pattern]

    return " AND ".join(parts), params


def _build_coordinate_list(
    dim: str, fields: Dict[str, List[str]]
) -> List[str]:
    """Flat list of pipe-format coordinates for a dimension. Used by cardinality probe."""
    coords = []
    for field, values in fields.items():
        for v in values:
            coords.append(f"{dim.upper()}|{field}|{v}")
    return coords


# ─────────────────────────────────────────────────────────────────────────────
# Peirce string parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_peirce(peirce_string: str) -> List[Dict[str, str]]:
    """
    Parse a Peirce constraint string into a list of constraint dicts.

    Handles the subset of Peirce used by the current workbench:
        WHAT.matter_type = "litigation"
        WHAT.matter_type = "litigation" AND WHAT.task_code = "A103"
        WHEN.year BETWEEN "2019" AND "2023"

    Raises ValueError on unparseable clauses — never silently drops.
    """
    import re
    constraints = []

    # Split on AND and OR — OR within same dimension+field becomes IN (...)
    # which is handled by _group_by_dim accumulating multiple values
    clauses = re.split(r'\b(?:AND|OR)\b', peirce_string, flags=re.IGNORECASE)

    for clause in clauses:
        clause = clause.strip()
        if not clause:
            continue

        # BETWEEN: DIM.field BETWEEN "v1" AND "v2"
        between = re.match(
            r'(\w+)\.(\w+)\s+BETWEEN\s+"([^"]+)"\s+AND\s+"([^"]+)"',
            clause, re.IGNORECASE
        )
        if between:
            dim, field, low, high = between.groups()
            constraints.append(
                {"dimension": dim.upper(), "field": field, "op": "gte", "value": low}
            )
            constraints.append(
                {"dimension": dim.upper(), "field": field, "op": "lte", "value": high}
            )
            continue

        eq = re.match(r'(\w+)\.(\w+)\s*=\s*"([^"]*)"', clause)
        if eq:
            dim, field, value = eq.groups()
            constraints.append(
                {"dimension": dim.upper(), "field": field, "op": "eq", "value": value}
            )
            continue

        not_eq = re.match(r'(\w+)\.(\w+)\s*!=\s*"([^"]*)"', clause)
        if not_eq:
            dim, field, value = not_eq.groups()
            constraints.append(
                {"dimension": dim.upper(), "field": field, "op": "not_eq", "value": value}
            )
            continue

        cmp = re.match(r'(\w+)\.(\w+)\s*(>=|<=|>|<)\s*"([^"]*)"', clause)
        if cmp:
            dim, field, op_str, value = cmp.groups()
            op_map = {">": "gt", ">=": "gte", "<": "lt", "<=": "lte"}
            constraints.append({
                "dimension": dim.upper(), "field": field,
                "op": op_map[op_str], "value": value,
            })
            continue

        contains = re.match(r'(\w+)\.(\w+)\s+CONTAINS\s+"([^"]*)"', clause, re.IGNORECASE)
        if contains:
            dim, field, value = contains.groups()
            constraints.append(
                {"dimension": dim.upper(), "field": field, "op": "contains", "value": value}
            )
            continue

        prefix = re.match(r'(\w+)\.(\w+)\s+PREFIX\s+"([^"]*)"', clause, re.IGNORECASE)
        if prefix:
            dim, field, value = prefix.groups()
            constraints.append(
                {"dimension": dim.upper(), "field": field, "op": "prefix", "value": value}
            )
            continue

        raise ValueError(
            f"Unparseable Peirce clause: '{clause}'. "
            "PostgresAdapter supports: eq, not_eq, gt/gte/lt/lte, BETWEEN, CONTAINS, PREFIX. "
            "For full Peirce support use a substrate backed by snf-peirce."
        )

    return constraints
