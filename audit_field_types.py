"""
audit_field_types.py — fills in the snf_field_types verification matrix.

READ-ONLY. Opens DuckDB files read_only=True, issues only SELECTs.
Prints field names and type vocabulary. Never prints a data value,
a connection string, or a password.

Run from the reckoner folder:
    python audit_field_types.py

Optional:
    python audit_field_types.py --artifacts "C:\\path\\to\\artifacts"
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ACCEPTED_VOCAB = {"text", "enum", "number", "date"}
SAMPLE = 10


def line(ch="-", n=72):
    print(ch * n)


# ─────────────────────────────────────────────────────────────────────────────
# DuckDB
# ─────────────────────────────────────────────────────────────────────────────

def audit_duckdb(path: Path):
    import duckdb

    print(f"\n### {path.name}")
    try:
        con = duckdb.connect(str(path), read_only=True)
    except Exception as e:
        print(f"  OPEN FAILED: {type(e).__name__}: {e}")
        return

    try:
        # 1. Does the table exist? Check for naming drift too.
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE lower(table_name) LIKE '%field%type%' "
            "   OR lower(table_name) LIKE '%snf%type%'"
        ).fetchall()
        names = [t[0] for t in tables]
        print(f"  candidate type tables: {names or 'NONE'}")

        if not any(n.lower() == "snf_field_types" for n in names):
            print("  → snf_field_types ABSENT. Heuristic path only. (Outcome 1)")
            # Still report the fact-side forms, needed for the key convention.
            report_fact_forms(con)
            return

        # 2. Exact columns the reader expects.
        cols = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE lower(table_name) = 'snf_field_types' ORDER BY ordinal_position"
        ).fetchall()
        colnames = [c[0] for c in cols]
        print(f"  columns: {colnames}")
        required = {"dimension", "semantic_key", "value_type"}
        missing = required - {c.lower() for c in colnames}
        if missing:
            print(f"  → READER WOULD FAIL. Missing columns: {sorted(missing)} (Outcome 2)")
            print("     The bare `except Exception: pass` hides this at runtime.")
            report_fact_forms(con)
            return

        # 3. Exactly the reader's query.
        try:
            rows = con.execute(
                "SELECT dimension, semantic_key, value_type FROM snf_field_types"
            ).fetchall()
        except Exception as e:
            print(f"  → READER QUERY FAILS: {type(e).__name__}: {e} (Outcome 2)")
            report_fact_forms(con)
            return

        print(f"  rows: {len(rows)}")
        if not rows:
            print("  → Table present but EMPTY. Authority never fires. (Outcome 3)")
            report_fact_forms(con)
            return

        # 4. Storage forms.
        dims = {r[0] for r in rows}
        print(f"  dimension forms in table: {sorted(dims)}")
        dotted = sum(1 for r in rows if "." in (r[1] or ""))
        print(f"  semantic_key: {dotted} dotted / {len(rows) - dotted} bare")

        # 5. Vocabulary — Outcome 4.
        vocab = {r[2] for r in rows}
        bad = vocab - ACCEPTED_VOCAB
        print(f"  value_type vocabulary: {sorted(vocab)}")
        if bad:
            print(f"  → VOCAB MISMATCH: {sorted(bad)} not in {sorted(ACCEPTED_VOCAB)}")
            print("     These propagate raw to the API and break operator selection. (Outcome 4)")
        else:
            print("  → vocabulary OK")

        # 6. THE DECISIVE CHECK — does the reader's key actually match?
        compiled = {f"{r[0]}|{r[1]}": r[2] for r in rows}

        facts = con.execute(
            "SELECT DISTINCT dimension, semantic_key FROM snf_spoke"
        ).fetchall()

        hits, misses = [], []
        for d, sk in facts:
            # This is the reader's construction, verbatim: f"{dim}|{semantic_key}"
            # where `dim` comes from substrate.dimensions() — see note below.
            if f"{d}|{sk}" in compiled:
                hits.append((d, sk))
            else:
                misses.append((d, sk))

        print(f"  EXACT MATCHES: {len(hits)} / {len(facts)} fact fields")
        if not hits:
            print("  → AUTHORITY NEVER FIRES. Key drift. (Outcome 3 — invisible even with logging)")
            for d, sk in misses[:SAMPLE]:
                print(f"       fact key: {d}|{sk}")
            for k in list(compiled)[:SAMPLE]:
                print(f"       type key: {k}")
            # Diagnose which drift explains it.
            ci = {f"{d.lower()}|{sk}" for d, sk in facts}
            if any(f"{r[0].lower()}|{r[1]}" in ci for r in rows):
                print("     → CASE DRIFT explains the misses.")
            bare = {f"{d}|{sk.split('.')[-1]}" for d, sk in facts}
            if any(f"{r[0]}|{r[1].split('.')[-1]}" in bare for r in rows):
                print("     → DOTTED/BARE DRIFT explains the misses.")
        else:
            print(f"  → authority fires on {len(hits)} fields")
            if misses:
                print(f"     {len(misses)} fields fall through to heuristic (may be correct)")

        # 7. Demonstrated override — the acceptance criterion.
        overrides = []
        for d, sk in hits:
            declared = compiled[f"{d}|{sk}"]
            fname = sk.split(".")[-1] if "." in sk else sk
            # NOTE: no lens_id filter. Correct for single-lens substrates.
            # Would over-count on a multi-lens substrate; the API query
            # filters on lens_id as well.
            n = con.execute(
                "SELECT COUNT(DISTINCT value) FROM snf_spoke "
                "WHERE dimension = ? AND semantic_key = ?", [d, sk]
            ).fetchone()[0]
            if heuristic(fname, n) != declared:
                overrides.append((d, sk, heuristic(fname, n), declared))

        print(f"  DEMONSTRATED OVERRIDES: {len(overrides)}")
        for d, sk, h, decl in overrides[:SAMPLE]:
            print(f"       {d}|{sk}: heuristic={h} declared={decl}")
        if hits and not overrides:
            print("     → table matches but every declaration agrees with the heuristic.")
            print("       Authority is structurally live but has never changed an outcome.")

    finally:
        con.close()


def report_fact_forms(con):
    """Needed regardless — establishes the canonical key convention."""
    try:
        facts = con.execute(
            "SELECT DISTINCT dimension, semantic_key FROM snf_spoke LIMIT 200"
        ).fetchall()
    except Exception as e:
        print(f"  (snf_spoke unreadable: {type(e).__name__})")
        return
    dims = {d for d, _ in facts}
    dotted = sum(1 for _, sk in facts if "." in (sk or ""))
    print(f"  fact-side dimension forms: {sorted(dims)}")
    print(f"  fact-side semantic_key: {dotted} dotted / {len(facts) - dotted} bare")


def heuristic(field_name: str, n: int) -> str:
    """The API's keyword fast paths, in order. Sampling branch omitted —
    this is for spotting overrides, not reproducing inference exactly."""
    name = field_name.lower()
    if any(k in name for k in ["year", "date", "month", "day", "release", "activity"]):
        return "date"
    if any(k in name for k in ["count", "amount", "price", "cmc", "size"]):
        return "number"
    if any(k in name for k in ["_id", "code", "number", "ref", "key", "system", "status"]):
        return "text"
    return "enum" if n <= 25 else "text"


# ─────────────────────────────────────────────────────────────────────────────
# Postgres
# ─────────────────────────────────────────────────────────────────────────────

def audit_postgres():
    """Reads PG_n_* from the environment. Prints schema names, never DSNs."""
    try:
        import psycopg2
    except ImportError:
        print("\n(psycopg2 not available — skipping Postgres)")
        return

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("\n(DATABASE_URL not set — skipping Postgres)")
        return

    schemas = []
    for i in range(1, 20):
        s = os.environ.get(f"PG_{i}_SCHEMA") or os.environ.get(f"PG_{i}_NAME")
        if s:
            schemas.append(s)
    if not schemas:
        print("\n(no PG_n_* declared — skipping Postgres)")
        return

    print(f"\n### Postgres — schemas declared: {schemas}")
    try:
        con = psycopg2.connect(dsn)
    except Exception as e:
        print(f"  CONNECT FAILED: {type(e).__name__}")   # message may carry the DSN
        return

    with con, con.cursor() as cur:
        cur.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE lower(table_name) LIKE '%field%type%' "
            "   OR lower(table_name) LIKE '%snf%type%' "
            "   OR lower(table_name) = 'snf_affordances'"
        )
        for sch, tbl in cur.fetchall():
            print(f"  {sch}.{tbl}")
    con.close()
    print("  NOTE: no adapter reads snf_field_types on the Postgres path.")
    print("        Presence here means Model Builder emits it and nothing consumes it.")


# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts", default=None)
    args = ap.parse_args()

    root = Path(args.artifacts) if args.artifacts else Path(
        os.environ.get("LOCALAPPDATA", "")) / "Reckoner" / "artifacts"

    line("=")
    print("snf_field_types audit — READ ONLY")
    print(f"artifacts: {root}")
    line("=")

    if root.exists():
        files = sorted(root.rglob("*.duckdb"))
        print(f"{len(files)} DuckDB substrate(s)")
        for f in files:
            audit_duckdb(f)
    else:
        print(f"artifacts dir not found: {root}")
        print("pass --artifacts explicitly")

    audit_postgres()

    line("=")
    print("Fill the matrix from the above. The decisive line is")
    print("DEMONSTRATED OVERRIDES > 0 on at least one substrate.")
    line("=")


if __name__ == "__main__":
    main()
