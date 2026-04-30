"""
test_postgres_adapter_contract.py — PostgresAdapter contract tests

Verifies that PostgresAdapter satisfies the SubstrateAdapter contract
when backed by the 'legal' Postgres schema (Shape A: split columns).

Known geometry (deterministic — tests assert exact counts):

    WHAT.matter_type = "litigation"                          -> 54,637 entities
    WHAT.matter_type = "transactional"                       -> 37,691 entities
    WHAT.matter_type = "advisory"                            -> 30,006 entities
    WHAT.task_code   = "A103"                                -> 15,277 entities
    WHAT.matter_type = "litigation" AND WHAT.task_code = "A103" -> 4,518 entities

Schema shape: entity_id, semantic_key, value, coordinate
Coordinate column: dot-equals format (stale — not used for routing)
Routing: via semantic_key + value columns directly

Usage:
    Set DATABASE_URL in .env or environment before running:
        DATABASE_URL=postgresql://postgres:password@localhost:5432/postgres

    py -m pytest tests/test_postgres_adapter_contract.py -v

Tests are skipped automatically if DATABASE_URL is not set or the
Postgres connection cannot be established.
"""

from __future__ import annotations

import os
import pytest

# Load .env if present — does nothing if python-dotenv is not installed
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

from postgres_adapter import PostgresAdapter
from adapter import ProvenanceRecord

# ─────────────────────────────────────────────────────────────────────────────
# Fixture
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA   = "legal"
MANIFEST = {
    "substrate_id":       "legal-prod",
    "lens_id":            "legal-v1",
    "translator_version": "1.0.0",
}


def _get_connection():
    """
    Open a psycopg2 connection from DATABASE_URL.
    Returns None if DATABASE_URL is not set.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None
    try:
        return psycopg2.connect(url)
    except Exception:
        return None


def _postgres_available() -> bool:
    conn = _get_connection()
    if conn is None:
        return False
    conn.close()
    return True


pytestmark = pytest.mark.skipif(
    not PSYCOPG2_AVAILABLE or not _postgres_available(),
    reason=(
        "Postgres not available. Set DATABASE_URL in .env to run these tests. "
        "Example: DATABASE_URL=postgresql://postgres:password@localhost:5432/postgres"
    )
)


@pytest.fixture(scope="module")
def adapter():
    conn = _get_connection()
    return PostgresAdapter.from_binding(conn=conn, schema=SCHEMA, manifest=MANIFEST)


# ─────────────────────────────────────────────────────────────────────────────
# Shape detection
# ─────────────────────────────────────────────────────────────────────────────

class TestShapeDetection:

    def test_detects_split_shape(self, adapter):
        assert adapter._shape == "split", (
            f"Expected shape 'split' for legal schema, got '{adapter._shape}'"
        )

    def test_entity_id_col_is_entity_id(self, adapter):
        assert adapter._entity_id_col == "entity_id"


# ─────────────────────────────────────────────────────────────────────────────
# Provenance and identity
# ─────────────────────────────────────────────────────────────────────────────

class TestProvenance:

    def test_lens_id_matches_manifest(self, adapter):
        assert adapter.lens_id == MANIFEST["lens_id"]

    def test_provenance_returns_record(self, adapter):
        prov = adapter.provenance()
        assert isinstance(prov, ProvenanceRecord)

    def test_provenance_source_is_binding(self, adapter):
        prov = adapter.provenance()
        assert prov.provenance_source == "binding", (
            f"Expected provenance_source='binding', got '{prov.provenance_source}'"
        )

    def test_provenance_lens_id_matches(self, adapter):
        assert adapter.provenance().lens_id == MANIFEST["lens_id"]

    def test_provenance_extra_contains_substrate_id(self, adapter):
        extra = adapter.provenance().extra
        assert extra.get("substrate_id") == MANIFEST["substrate_id"]

    def test_provenance_extra_contains_translator_version(self, adapter):
        extra = adapter.provenance().extra
        assert extra.get("translator_version") == MANIFEST["translator_version"]

    def test_from_substrate_raises(self):
        """from_substrate() must remain blocked until emitter stamping is available."""
        conn = _get_connection()
        with pytest.raises(NotImplementedError):
            PostgresAdapter.from_substrate(conn, SCHEMA)

    def test_from_binding_missing_field_raises(self):
        conn = _get_connection()
        with pytest.raises(ValueError, match="missing required fields"):
            PostgresAdapter.from_binding(
                conn=conn,
                schema=SCHEMA,
                manifest={"substrate_id": "x", "lens_id": "y"}
                # translator_version missing
            )


# ─────────────────────────────────────────────────────────────────────────────
# Metadata
# ─────────────────────────────────────────────────────────────────────────────

class TestMetadata:

    def test_entity_count_returns_int(self, adapter):
        count = adapter.entity_count()
        assert isinstance(count, int)
        assert count > 0

    def test_entity_count_is_substantial(self, adapter):
        """legal schema has ~140K+ entities — assert a meaningful floor."""
        assert adapter.entity_count() > 100_000, (
            f"Expected >100,000 entities, got {adapter.entity_count()}"
        )

    def test_dimensions_returns_list(self, adapter):
        dims = adapter.dimensions()
        assert isinstance(dims, list)
        assert len(dims) > 0

    def test_dimensions_lowercase(self, adapter):
        dims = adapter.dimensions()
        for d in dims:
            assert d == d.lower(), f"Dimension '{d}' is not lowercase"

    def test_what_dimension_present(self, adapter):
        dims = adapter.dimensions()
        assert "what" in dims, f"'what' not in dimensions: {dims}"


# ─────────────────────────────────────────────────────────────────────────────
# Query routing — contract shape
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryContract:

    def test_returns_query_result(self, adapter):
        result = adapter.query('WHAT.matter_type = "litigation"')
        assert hasattr(result, "entity_ids")
        assert hasattr(result, "count")

    def test_entity_ids_is_list(self, adapter):
        result = adapter.query('WHAT.matter_type = "litigation"')
        assert isinstance(result.entity_ids, list)

    def test_entity_ids_are_strings(self, adapter):
        result = adapter.query('WHAT.matter_type = "litigation"')
        for eid in result.entity_ids:
            assert isinstance(eid, str), f"entity_id {eid!r} is {type(eid)}"

    def test_limit_respected(self, adapter):
        result = adapter.query('WHAT.matter_type = "litigation"', limit=10)
        assert len(result.entity_ids) <= 10

    def test_count_reflects_total_not_limit(self, adapter):
        """count must reflect total matches, not the returned page."""
        result = adapter.query('WHAT.matter_type = "litigation"', limit=10)
        assert result.count > 10, (
            "count should reflect total matches (54,637), not the limit (10)"
        )

    def test_no_match_returns_empty(self, adapter):
        result = adapter.query('WHAT.matter_type = "nonexistent_xyz_abc"')
        assert result.count == 0
        assert result.entity_ids == []

    def test_unparseable_peirce_raises(self, adapter):
        with pytest.raises(ValueError):
            adapter.query("WHAT matter_type is litigation")


# ─────────────────────────────────────────────────────────────────────────────
# Query routing — known geometry
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryGeometry:

    def test_litigation_count(self, adapter):
        result = adapter.query('WHAT.matter_type = "litigation"', limit=None)
        assert result.count == 54_637, (
            f"Expected 54,637 litigation matters, got {result.count}"
        )

    def test_transactional_count(self, adapter):
        result = adapter.query('WHAT.matter_type = "transactional"', limit=None)
        assert result.count == 37_691, (
            f"Expected 37,691 transactional matters, got {result.count}"
        )

    def test_task_code_a103_count(self, adapter):
        result = adapter.query('WHAT.task_code = "A103"', limit=None)
        assert result.count == 15_277, (
            f"Expected 15,277 A103 tasks, got {result.count}"
        )

    def test_and_litigation_and_a103(self, adapter):
        """Cross-dimension AND — known intersection count."""
        result = adapter.query(
            'WHAT.matter_type = "litigation" AND WHAT.task_code = "A103"',
            limit=None
        )
        assert result.count == 4_518, (
            f"Expected 4,518 litigation+A103 matters, got {result.count}"
        )

    def test_and_result_subset_of_larger(self, adapter):
        """Intersection must be <= the smaller input set."""
        result = adapter.query(
            'WHAT.matter_type = "litigation" AND WHAT.task_code = "A103"',
            limit=None
        )
        assert result.count <= 15_277, (
            "Intersection count exceeds task_code cardinality — impossible"
        )
        assert result.count <= 54_637, (
            "Intersection count exceeds matter_type cardinality — impossible"
        )

    def test_or_two_values_same_field(self, adapter):
        """OR semantics: litigation + transactional > either alone."""
        lit  = adapter.query('WHAT.matter_type = "litigation"', limit=None)
        txn  = adapter.query('WHAT.matter_type = "transactional"', limit=None)
        both = adapter.query(
            'WHAT.matter_type = "litigation" OR WHAT.matter_type = "transactional"',
            limit=None
        )
        assert both.count == lit.count + txn.count, (
            f"Expected {lit.count + txn.count} (sum, no overlap), got {both.count}"
        )

    def test_entity_ids_deduplicated(self, adapter):
        result = adapter.query(
            'WHAT.matter_type = "litigation" AND WHAT.task_code = "A103"',
            limit=None
        )
        assert len(result.entity_ids) == len(set(result.entity_ids)), (
            "Duplicate entity IDs in result"
        )

    def test_anchor_ordering_consistent(self, adapter):
        """
        Same query with constraints in different logical order must return
        identical results. The cardinality probe ensures the anchor is always
        the most selective dimension regardless of constraint order.
        """
        r1 = adapter.query(
            'WHAT.matter_type = "litigation" AND WHAT.task_code = "A103"',
            limit=None
        )
        r2 = adapter.query(
            'WHAT.task_code = "A103" AND WHAT.matter_type = "litigation"',
            limit=None
        )
        assert r1.count == r2.count
        assert set(r1.entity_ids) == set(r2.entity_ids)


# ─────────────────────────────────────────────────────────────────────────────
# Discover
# ─────────────────────────────────────────────────────────────────────────────

class TestDiscover:

    def test_discover_all_dimensions(self, adapter):
        result = adapter.discover("*")
        assert hasattr(result, "rows")
        assert isinstance(result.rows, list)
        assert len(result.rows) > 0

    def test_discover_scope_is_dimensions(self, adapter):
        result = adapter.discover("*")
        assert result.scope == "dimensions"

    def test_discover_what_fields(self, adapter):
        result = adapter.discover("WHAT|*")
        assert result.scope == "fields"
        assert result.dimension == "WHAT"
        fields = [r["field"] for r in result.rows]
        assert "matter_type" in fields, (
            f"'matter_type' not found in WHAT fields: {fields}"
        )

    def test_discover_matter_type_values(self, adapter):
        result = adapter.discover("WHAT|matter_type|*")
        assert result.scope == "values"
        assert result.dimension == "WHAT"
        assert result.field == "matter_type"
        values = [r["value"] for r in result.rows]
        assert "litigation" in values, (
            f"'litigation' not found in matter_type values: {values[:10]}"
        )

    def test_discover_values_ordered_by_count(self, adapter):
        result = adapter.discover("WHAT|matter_type|*")
        counts = [r["count"] for r in result.rows]
        assert counts == sorted(counts, reverse=True), (
            "Values not ordered by count descending"
        )

    def test_discover_litigation_count_matches_query(self, adapter):
        """discover() count for a value must match query() count."""
        result    = adapter.discover("WHAT|matter_type|*")
        rows_by_value = {r["value"]: r["count"] for r in result.rows}
        query_result  = adapter.query('WHAT.matter_type = "litigation"', limit=None)
        assert rows_by_value.get("litigation") == query_result.count, (
            f"discover count ({rows_by_value.get('litigation')}) != "
            f"query count ({query_result.count}) for 'litigation'"
        )

    def test_discover_unknown_expression_raises(self, adapter):
        with pytest.raises(ValueError):
            adapter.discover("WHAT|matter_type|litigation|extra")


# ─────────────────────────────────────────────────────────────────────────────
# Affordances
# ─────────────────────────────────────────────────────────────────────────────

class TestAffordances:

    def test_returns_dict(self, adapter):
        aff = adapter.affordances()
        assert isinstance(aff, dict)
        assert len(aff) > 0

    def test_what_dimension_present(self, adapter):
        aff = adapter.affordances()
        assert "WHAT" in aff, f"'WHAT' not in affordances: {list(aff.keys())}"

    def test_matter_type_field_present(self, adapter):
        aff = adapter.affordances()
        assert "matter_type" in aff["WHAT"], (
            f"'matter_type' not in WHAT affordances: {list(aff['WHAT'].keys())}"
        )

    def test_field_has_required_keys(self, adapter):
        aff   = adapter.affordances()
        field = aff["WHAT"]["matter_type"]
        for key in ("fact_count", "distinct_values", "value_type"):
            assert key in field, f"'{key}' missing from affordance field: {field}"

    def test_value_type_is_valid(self, adapter):
        aff = adapter.affordances()
        for dim, fields in aff.items():
            for field_name, meta in fields.items():
                assert meta["value_type"] in ("text", "enum", "number", "date"), (
                    f"{dim}.{field_name} has invalid value_type: {meta['value_type']}"
                )

    def test_fact_count_positive(self, adapter):
        aff = adapter.affordances()
        for dim, fields in aff.items():
            for field_name, meta in fields.items():
                assert meta["fact_count"] > 0, (
                    f"{dim}.{field_name} has fact_count=0"
                )


# ─────────────────────────────────────────────────────────────────────────────
# Values
# ─────────────────────────────────────────────────────────────────────────────

class TestValues:

    def test_returns_list(self, adapter):
        vals = adapter.values("what", "matter_type")
        assert isinstance(vals, list)
        assert len(vals) > 0

    def test_each_row_has_value_and_count(self, adapter):
        vals = adapter.values("what", "matter_type")
        for row in vals:
            assert "value" in row, f"'value' missing from row: {row}"
            assert "count" in row, f"'count' missing from row: {row}"

    def test_ordered_by_count_descending(self, adapter):
        vals   = adapter.values("what", "matter_type")
        counts = [r["count"] for r in vals]
        assert counts == sorted(counts, reverse=True), (
            "values() not ordered by count descending"
        )

    def test_litigation_appears_first(self, adapter):
        """litigation is the most common matter_type — must be first."""
        vals = adapter.values("what", "matter_type")
        assert vals[0]["value"] == "litigation", (
            f"Expected 'litigation' first, got '{vals[0]['value']}'"
        )

    def test_litigation_count_matches_query(self, adapter):
        vals         = adapter.values("what", "matter_type")
        by_value     = {r["value"]: r["count"] for r in vals}
        query_result = adapter.query('WHAT.matter_type = "litigation"', limit=None)
        assert by_value["litigation"] == query_result.count, (
            f"values() count ({by_value['litigation']}) != "
            f"query() count ({query_result.count}) for 'litigation'"
        )

    def test_limit_respected(self, adapter):
        vals = adapter.values("what", "matter_type", limit=3)
        assert len(vals) <= 3

    def test_unknown_field_returns_empty(self, adapter):
        vals = adapter.values("what", "nonexistent_field_xyz")
        assert vals == []


# ─────────────────────────────────────────────────────────────────────────────
# Hydrate
# ─────────────────────────────────────────────────────────────────────────────

class TestHydrate:

    def _get_entity_ids(self, adapter, peirce: str, n: int = 3) -> list:
        result = adapter.query(peirce, limit=n)
        return result.entity_ids[:n]

    def test_returns_list(self, adapter):
        eids   = self._get_entity_ids(adapter, 'WHAT.matter_type = "litigation"')
        result = adapter.hydrate(eids, {})
        assert isinstance(result, list)
        assert len(result) == len(eids)

    def test_each_result_has_id(self, adapter):
        eids   = self._get_entity_ids(adapter, 'WHAT.matter_type = "litigation"')
        result = adapter.hydrate(eids, {})
        for r in result:
            assert "id" in r, f"'id' missing from hydrate result: {r}"

    def test_each_result_has_coordinates(self, adapter):
        eids   = self._get_entity_ids(adapter, 'WHAT.matter_type = "litigation"')
        result = adapter.hydrate(eids, {})
        for r in result:
            assert "coordinates" in r

    def test_each_result_has_matched_because(self, adapter):
        eids   = self._get_entity_ids(adapter, 'WHAT.matter_type = "litigation"')
        result = adapter.hydrate(eids, {})
        for r in result:
            assert "matched_because" in r

    def test_input_order_preserved(self, adapter):
        eids   = self._get_entity_ids(adapter, 'WHAT.matter_type = "litigation"', n=5)
        result = adapter.hydrate(eids, {})
        returned_ids = [r["id"] for r in result]
        assert returned_ids == eids, (
            f"Input order not preserved.\nExpected: {eids}\nGot:      {returned_ids}"
        )

    def test_what_dimension_in_coordinates(self, adapter):
        eids   = self._get_entity_ids(adapter, 'WHAT.matter_type = "litigation"')
        result = adapter.hydrate(eids, {})
        for r in result:
            assert "WHAT" in r["coordinates"], (
                f"'WHAT' not in coordinates for entity {r['id']}: "
                f"{list(r['coordinates'].keys())}"
            )

    def test_coordinate_has_pipe_format(self, adapter):
        """
        Hydrate must reconstruct pipe-format coordinates even though
        the legal schema stores stale dot-equals format in the coordinate column.
        """
        eids   = self._get_entity_ids(adapter, 'WHAT.matter_type = "litigation"')
        result = adapter.hydrate(eids, {})
        for r in result:
            for dim, facts in r["coordinates"].items():
                for fact in facts:
                    coord = fact["coordinate"]
                    assert "|" in coord, (
                        f"Coordinate is not pipe-format: '{coord}'. "
                        "Adapter must reconstruct pipe-format from split columns."
                    )
                    assert "=" not in coord, (
                        f"Coordinate contains dot-equals format: '{coord}'. "
                        "Stale coordinate column must not be used."
                    )

    def test_matched_because_populated_when_passed(self, adapter):
        eids = self._get_entity_ids(adapter, 'WHAT.matter_type = "litigation"', n=1)
        eid  = eids[0]
        matched_coords = {eid: ["WHAT|matter_type|litigation"]}
        result = adapter.hydrate([eid], matched_coords)
        assert len(result[0]["matched_because"]) > 0

    def test_empty_entity_ids_returns_empty(self, adapter):
        result = adapter.hydrate([], {})
        assert result == []

    def test_semantic_keys_filter(self, adapter):
        eids   = self._get_entity_ids(adapter, 'WHAT.matter_type = "litigation"')
        result = adapter.hydrate(eids, {}, semantic_keys=["matter_type"])
        for r in result:
            for dim, facts in r["coordinates"].items():
                for fact in facts:
                    assert fact["field"] == "matter_type", (
                        f"semantic_keys filter not applied: got field '{fact['field']}'"
                    )
