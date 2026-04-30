"""
test_adapter_contract.py - SubstrateAdapter contract tests

Verifies that the snf-peirce Substrate satisfies the contract
required by reckoner_api.py and DuckDBAdapter.

Usage:
    py -m pytest tests/test_adapter_contract.py -v

The fixture is discogs_mini: 12 entities, 3 dimensions (WHO/WHAT/WHEN).
Known geometry (deterministic - tests assert exact counts):

    WHO.artist = "Miles Davis"       -> 5 entities (001,002,003,004,012)
    WHO.artist = "John Coltrane"     -> 2 entities (005,006)
    WHEN.year  = "1959"              -> 3 entities (001,008,011)
    WHAT.format = "LP"               -> 11 entities (all except 012)
    WHAT.format = "CD"               -> 1 entity  (012)

    Miles Davis AND 1959             -> 1 entity  (001 - Kind of Blue)
    Miles Davis AND LP               -> 4 entities (001,002,003,004)
    Miles Davis OR John Coltrane     -> 7 entities
    NOT CD                           -> 11 entities

    substrate.count()      = 60  (facts: 12 entities x 5 fields)
    substrate.entity_count = 12  (distinct entities)
"""

from __future__ import annotations

import io
import pytest

try:
    import pandas as pd
    from snf_peirce import suggest, compile_data, query, discover
    SNF_AVAILABLE = True
except ImportError:
    SNF_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not SNF_AVAILABLE,
    reason="snf-peirce not installed. Run: pip install snf-peirce"
)


DISCOGS_MINI_CSV = """release_id,artist,title,format,year
discogs:release:001,Miles Davis,Kind of Blue,LP,1959
discogs:release:002,Miles Davis,Sketches of Spain,LP,1960
discogs:release:003,Miles Davis,Bitches Brew,LP,1970
discogs:release:004,Miles Davis,Miles Ahead,LP,1957
discogs:release:005,John Coltrane,A Love Supreme,LP,1965
discogs:release:006,John Coltrane,Giant Steps,LP,1960
discogs:release:007,Bill Evans,Waltz for Debby,LP,1961
discogs:release:008,Bill Evans,Portrait in Jazz,LP,1959
discogs:release:009,Thelonious Monk,Monk's Dream,LP,1963
discogs:release:010,Thelonious Monk,Brilliant Corners,LP,1956
discogs:release:011,Charles Mingus,Mingus Ah Um,LP,1959
discogs:release:012,Miles Davis,In a Silent Way,CD,1969
"""


def build_discogs_mini():
    df = pd.read_csv(io.StringIO(DISCOGS_MINI_CSV))
    draft = suggest(df)
    draft.nucleus("release_id")
    draft.map("artist", "who",  "artist")
    draft.map("title",  "what", "title")
    draft.map("format", "what", "format")
    draft.map("year",   "when", "year")
    lens = draft.to_lens(lens_id="discogs_v1", authority="test")
    return compile_data(df, lens)


@pytest.fixture(scope="module")
def substrate():
    return build_discogs_mini()


class TestSubstrateProperties:

    def test_count_returns_int(self, substrate):
        c = substrate.count()
        assert isinstance(c, int), f"count() returned {type(c)}, expected int"
        assert c > 0, "count() returned 0"

    def test_count_is_facts(self, substrate):
        """count() returns total facts (60 = 12 entities x 5 fields)."""
        assert substrate.count() == 60, (
            f"Expected 60 facts, got {substrate.count()}"
        )

    def test_entity_count_is_12(self, substrate):
        """entity_count returns distinct entities (12)."""
        assert substrate.entity_count() == 12, (
            f"Expected 12 entities, got {substrate.entity_count()}"
        )

    def test_dimensions_returns_list(self, substrate):
        dims = substrate.dimensions()
        assert isinstance(dims, list)
        assert len(dims) > 0

    def test_dimensions_contains_expected(self, substrate):
        dims = [d.lower() for d in substrate.dimensions()]
        for expected in ("who", "what", "when"):
            assert expected in dims, (
                f"Expected dimension '{expected}' not found in {dims}"
            )

    def test_lens_id_is_string(self, substrate):
        assert isinstance(substrate.lens_id, str)
        assert substrate.lens_id != ""

    def test_lens_id_matches_fixture(self, substrate):
        assert substrate.lens_id == "discogs_v1", (
            f"Expected lens_id 'discogs_v1', got '{substrate.lens_id}'"
        )


class TestDirectConnection:

    def test_conn_attribute_exists(self, substrate):
        assert hasattr(substrate, "_conn"), "substrate has no _conn attribute"

    def test_conn_execute_is_callable(self, substrate):
        assert callable(substrate._conn.execute), "_conn.execute is not callable"

    def test_basic_count_query(self, substrate):
        rows = substrate._conn.execute(
            "SELECT COUNT(*) FROM snf_spoke"
        ).fetchall()
        assert rows[0][0] > 0, "snf_spoke table is empty"

    def test_parameterised_query(self, substrate):
        rows = substrate._conn.execute(
            "SELECT entity_id, dimension, semantic_key, value "
            "FROM snf_spoke "
            "WHERE dimension = ? AND lens_id = ? "
            "LIMIT 5",
            ["who", substrate.lens_id]
        ).fetchall()
        assert isinstance(rows, list)
        assert len(rows) > 0, "No WHO rows found"

    def test_snf_spoke_has_required_columns(self, substrate):
        rows = substrate._conn.execute(
            "SELECT entity_id, dimension, semantic_key, value, coordinate "
            "FROM snf_spoke LIMIT 1"
        ).fetchall()
        assert len(rows) > 0, "snf_spoke is empty"
        assert len(rows[0]) == 5, (
            f"Expected 5 columns, got {len(rows[0])}"
        )

    def test_lens_id_column_present(self, substrate):
        rows = substrate._conn.execute(
            "SELECT lens_id FROM snf_spoke WHERE lens_id = ? LIMIT 1",
            [substrate.lens_id]
        ).fetchall()
        assert len(rows) > 0, (
            f"lens_id '{substrate.lens_id}' not found in snf_spoke"
        )


class TestQueryRouting:

    def test_returns_result_with_entity_ids(self, substrate):
        result = query(substrate, 'WHO.artist = "Miles Davis"', limit=None)
        assert hasattr(result, "entity_ids")
        assert hasattr(result, "count")

    def test_entity_ids_is_list(self, substrate):
        result = query(substrate, 'WHO.artist = "Miles Davis"', limit=None)
        assert isinstance(result.entity_ids, list)

    def test_count_matches_entity_ids_length(self, substrate):
        result = query(substrate, 'WHO.artist = "Miles Davis"', limit=None)
        assert result.count == len(result.entity_ids), (
            f"count ({result.count}) != len(entity_ids) ({len(result.entity_ids)})"
        )

    def test_miles_davis_count(self, substrate):
        result = query(substrate, 'WHO.artist = "Miles Davis"', limit=None)
        assert result.count == 5, (
            f"Expected 5 Miles Davis releases, got {result.count}"
        )

    def test_miles_davis_entity_ids(self, substrate):
        result = query(substrate, 'WHO.artist = "Miles Davis"', limit=None)
        expected = {
            "discogs:release:001",
            "discogs:release:002",
            "discogs:release:003",
            "discogs:release:004",
            "discogs:release:012",
        }
        assert set(result.entity_ids) == expected, (
            f"Expected: {sorted(expected)}\nGot:      {sorted(result.entity_ids)}"
        )

    def test_year_1959_count(self, substrate):
        result = query(substrate, 'WHEN.year = "1959"', limit=None)
        assert result.count == 3, (
            f"Expected 3 releases from 1959, got {result.count}"
        )

    def test_format_lp_count(self, substrate):
        result = query(substrate, 'WHAT.format = "LP"', limit=None)
        assert result.count == 11, (
            f"Expected 11 LP releases, got {result.count}"
        )

    def test_format_cd_count(self, substrate):
        result = query(substrate, 'WHAT.format = "CD"', limit=None)
        assert result.count == 1, (
            f"Expected 1 CD release, got {result.count}"
        )
        assert "discogs:release:012" in result.entity_ids

    def test_and_miles_davis_and_1959(self, substrate):
        result = query(
            substrate,
            'WHO.artist = "Miles Davis" AND WHEN.year = "1959"',
            limit=None
        )
        assert result.count == 1, (
            f"Expected 1 result (Kind of Blue), got {result.count}"
        )
        assert "discogs:release:001" in result.entity_ids

    def test_and_miles_davis_and_lp(self, substrate):
        result = query(
            substrate,
            'WHO.artist = "Miles Davis" AND WHAT.format = "LP"',
            limit=None
        )
        assert result.count == 4, (
            f"Expected 4 Miles Davis LPs, got {result.count}"
        )
        expected = {
            "discogs:release:001",
            "discogs:release:002",
            "discogs:release:003",
            "discogs:release:004",
        }
        assert set(result.entity_ids) == expected

    def test_and_empty_result(self, substrate):
        result = query(
            substrate,
            'WHO.artist = "Miles Davis" AND WHAT.format = "CD" AND WHEN.year = "1959"',
            limit=None
        )
        assert result.count == 0
        assert result.entity_ids == []

    def test_or_miles_davis_or_coltrane(self, substrate):
        result = query(
            substrate,
            'WHO.artist = "Miles Davis" OR WHO.artist = "John Coltrane"',
            limit=None
        )
        assert result.count == 7, (
            f"Expected 7 (5 Davis + 2 Coltrane), got {result.count}"
        )

    def test_or_results_deduplicated(self, substrate):
        result = query(
            substrate,
            'WHO.artist = "Miles Davis" OR WHO.artist = "John Coltrane"',
            limit=None
        )
        assert len(result.entity_ids) == len(set(result.entity_ids)), (
            "Duplicate entity IDs in OR result"
        )

    def test_not_cd(self, substrate):
        result = query(substrate, 'NOT WHAT.format = "CD"', limit=None)
        assert result.count == 11, (
            f"Expected 11 non-CD releases, got {result.count}"
        )

    def test_dnf_two_conjuncts(self, substrate):
        result = query(
            substrate,
            '(WHO.artist = "Miles Davis" AND WHEN.year = "1959") OR '
            '(WHO.artist = "John Coltrane" AND WHEN.year = "1960")',
            limit=None
        )
        assert result.count == 2, (
            f"Expected 2 from DNF query, got {result.count}"
        )
        expected = {"discogs:release:001", "discogs:release:006"}
        assert set(result.entity_ids) == expected

    def test_dnf_deduplication(self, substrate):
        result = query(
            substrate,
            '(WHO.artist = "Miles Davis" AND WHEN.year = "1959") OR '
            '(WHO.artist = "Miles Davis" AND WHAT.format = "LP" AND WHEN.year = "1959")',
            limit=None
        )
        ids = result.entity_ids
        assert len(ids) == len(set(ids)), (
            f"Duplicate entity IDs in DNF result: {ids}"
        )


class TestDiscover:

    def test_discover_all_dimensions(self, substrate):
        result = discover(substrate, "*")
        assert hasattr(result, "rows")
        assert isinstance(result.rows, list)
        assert len(result.rows) > 0

    def test_discover_dimension_fields(self, substrate):
        result = discover(substrate, "WHO|*")
        assert hasattr(result, "rows")
        assert len(result.rows) > 0

    def test_discover_field_values(self, substrate):
        result = discover(substrate, "WHO|artist|*")
        assert hasattr(result, "rows")
        assert len(result.rows) > 0
        values = [str(r.get("value", r)) for r in result.rows]
        assert any("Miles Davis" in v for v in values), (
            f"'Miles Davis' not found in discover results: {values[:10]}"
        )

    def test_discover_scope_attribute(self, substrate):
        result = discover(substrate, "*")
        assert hasattr(result, "scope")


class TestProvenance:

    def test_lens_id_is_discogs_v1(self, substrate):
        assert substrate.lens_id == "discogs_v1"

    def test_lens_id_consistent_with_spoke(self, substrate):
        rows = substrate._conn.execute(
            "SELECT DISTINCT lens_id FROM snf_spoke"
        ).fetchall()
        spoke_lens_ids = {row[0] for row in rows}
        assert substrate.lens_id in spoke_lens_ids, (
            f"substrate.lens_id='{substrate.lens_id}' not in snf_spoke: {spoke_lens_ids}"
        )


class TestEdgeCases:

    def test_no_match_returns_empty(self, substrate):
        result = query(substrate, 'WHO.artist = "Nonexistent Artist XYZ"', limit=None)
        assert result.count == 0
        assert result.entity_ids == []

    def test_single_entity_result(self, substrate):
        result = query(substrate, 'WHAT.title = "Kind of Blue"', limit=None)
        assert result.count == 1
        assert "discogs:release:001" in result.entity_ids

    def test_entity_ids_are_strings(self, substrate):
        result = query(substrate, 'WHO.artist = "Miles Davis"', limit=None)
        for eid in result.entity_ids:
            assert isinstance(eid, str), f"entity_id {eid!r} is {type(eid)}"

    def test_default_limit_respected(self, substrate):
        result = query(substrate, 'WHAT.format = "LP"')
        assert result is not None
