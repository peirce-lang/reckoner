"""
model_builder/compiler/duckdb.py — DuckDB substrate emitter.

Emits a DuckDB .duckdb file from a DataFrame + BuildSpec dict.
Returns (dim_rows, meta_rows, result_dict) so the caller
(compiler/__init__.py) can pass dim_rows and meta_rows to
compiler/srf.py without this module knowing SRF exists.

Rule: this module emits substrate artifacts only.
It does not emit SRF records. It does not assert identity.
It does not reconcile or cleanse data.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

from ..config import OUTPUT_DIR
from ..inference import infer_snf_type


def emit_duckdb(
    df:   pd.DataFrame,
    spec: Dict[str, Any],
) -> Tuple[Dict[str, List[Dict]], List[Dict], Dict[str, Any]]:
    """
    Emit a DuckDB substrate artifact.

    Returns:
        (dim_rows, meta_rows, result_dict)

        dim_rows   — { dimension → [spoke row dicts] } for SRF emission
        meta_rows  — [ entity meta dicts ] for SRF emission
        result_dict — { output_path, download_url, entity_count, fact_count,
                        facts_by_dim, warnings, lens_id }
    """
    try:
        import duckdb
    except ImportError:
        raise RuntimeError("duckdb required for DuckDB output. pip install duckdb")

    mapping      = spec["mapping"]
    nucleus_spec = spec["nucleus"]
    lens_id      = spec["lens"]["lens_id"]
    output_name  = spec["target"]["output_name"]
    output_path  = OUTPUT_DIR / f"{output_name}.duckdb"

    translator_version = spec.get("provenance", {}).get("translator_version", "1.0.0")

    # ── Entity ID builder ─────────────────────────────────────────────────────
    def make_entity_id(row) -> str:
        cols  = nucleus_spec["columns"]
        sep   = nucleus_spec.get("separator", "-")
        pfx   = nucleus_spec.get("prefix", "")
        parts = [str(row.get(c, "")).strip() for c in cols]
        base  = sep.join(p for p in parts if p)
        return f"{pfx}:{base}" if pfx else base

    # ── Correlated facts index ────────────────────────────────────────────────
    # column_name → group_name lookup for correlation_id stamping.
    # correlation_id is a passenger column only — never indexed, never routed.
    correlations  = spec.get("correlations") or []
    col_to_group: Dict[str, tuple] = {}
    for grp_idx, grp in enumerate(correlations):
        group_name = grp["group"] if isinstance(grp, dict) else grp.group
        group_type = grp.get("group_type", group_name) if isinstance(grp, dict) else getattr(grp, "group_type", group_name)
        members    = grp["members"] if isinstance(grp, dict) else grp.members
        for member in members:
            col = member["column"] if isinstance(member, dict) else member.column
            col_to_group[col] = (group_name, grp_idx, group_type)

    # ── Row emission ──────────────────────────────────────────────────────────
    dim_rows: Dict[str, List[Dict]] = {d: [] for d in ["WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"]}
    meta_rows: List[Dict]           = []
    warnings:  List[str]            = []
    entity_ids_seen: Set[str]       = set()

    for row_idx, (_, row) in enumerate(df.iterrows()):
        eid = make_entity_id(row)
        if not eid:
            warnings.append(f"Skipped row with empty entity ID: {dict(row)}")
            continue

        if eid not in entity_ids_seen:
            entity_ids_seen.add(eid)
            nucleus_val = nucleus_spec.get("separator", "-").join(
                str(row.get(c, "")).strip() for c in nucleus_spec["columns"]
            )
            label    = ""
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

            raw_str = str(raw).strip()
            values  = [v.strip() for v in raw_str.split(",") if v.strip()] if "," in raw_str else [raw_str]

            for val in values:
                skey_clean = skey.replace(" ", "_")

                # ── WHEN: normalize dates and fan out granularities ───────────
                if dim == "WHEN":
                    date_match = re.match(r'^(\d{4}-\d{2}-\d{2})', val)
                    if date_match:
                        date_str = date_match.group(1)
                        try:
                            dt = datetime.strptime(date_str, "%Y-%m-%d")
                            # Inherit correlation_id if this column is a correlation member
                            if col in col_to_group:
                                w_grp_name, w_grp_idx, w_grp_type = col_to_group[col]
                                when_correlation_id = f"{w_grp_name}_{row_idx:03d}_{w_grp_idx:02d}"
                            else:
                                w_grp_type          = None
                                when_correlation_id = None
                            gran_facts = [
                                ("full_date",   date_str),
                                ("year",        str(dt.year)),
                                ("month",       date_str[:7]),
                                ("month_name",  dt.strftime("%B")),
                                ("day_of_week", dt.strftime("%A")),
                            ]
                            for gran_key, gran_val in gran_facts:
                                # Prefix with semantic_key so two date columns
                                # (e.g. watch_date and release_year) don't collide
                                # on the same bare "year" / "month" coordinate.
                                prefixed_key = f"{skey_clean}_{gran_key}"
                                dim_rows[dim].append({
                                    "entity_id":          eid,
                                    "dimension":          dim,
                                    "semantic_key":       prefixed_key,
                                    "value":              gran_val,
                                    "coordinate":         f"{dim.lower()}|{prefixed_key}|{gran_val}",
                                    "lens_id":            lens_id,
                                    "translator_version": translator_version,
                                    "correlation_id":     when_correlation_id,
                                    "group_type":         w_grp_type,
                                })
                            continue
                        except ValueError:
                            pass

                # ── Default single fact ───────────────────────────────────────
                if col in col_to_group:
                    grp_name, grp_idx, grp_type = col_to_group[col]
                    correlation_id = f"{grp_name}_{row_idx:03d}_{grp_idx:02d}"
                else:
                    grp_type       = None
                    correlation_id = None
                dim_rows[dim].append({
                    "entity_id":          eid,
                    "dimension":          dim,
                    "semantic_key":       skey,
                    "value":              val,
                    "coordinate":         f"{dim.lower()}|{skey_clean}|{val}",
                    "lens_id":            lens_id,
                    "translator_version": translator_version,
                    "correlation_id":     correlation_id,
                    "group_type":         grp_type,
                })

    # ── Write DuckDB ──────────────────────────────────────────────────────────
    con = duckdb.connect(str(output_path))

    con.execute("""
        CREATE OR REPLACE TABLE snf_spoke (
            entity_id      VARCHAR,
            dimension      VARCHAR,
            semantic_key   VARCHAR,
            value          VARCHAR,
            coordinate     VARCHAR,
            lens_id        VARCHAR,
            correlation_id VARCHAR,
            group_type     VARCHAR
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
                r.get("correlation_id"),
                r.get("group_type"),
            ))

    if all_spoke_rows:
        con.executemany("INSERT INTO snf_spoke VALUES (?, ?, ?, ?, ?, ?, ?, ?)", all_spoke_rows)

    con.execute("CREATE INDEX IF NOT EXISTS idx_spoke_coord ON snf_spoke(coordinate)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_spoke_eid   ON snf_spoke(entity_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_spoke_dim   ON snf_spoke(dimension, semantic_key)")

    # snf_field_types — compile-time type metadata so Reckoner never has to guess
    con.execute("""
        CREATE OR REPLACE TABLE snf_field_types (
            dimension    VARCHAR NOT NULL,
            semantic_key VARCHAR NOT NULL,
            value_type   VARCHAR NOT NULL,
            PRIMARY KEY (dimension, semantic_key)
        )
    """)
    field_type_rows = []
    for m in mapping:
        col  = m.get("column")
        dim  = m.get("dimension")
        skey = m.get("semantic_key", "").replace(" ", "_")
        if not col or not dim or col not in df.columns:
            continue
        vtype = "date" if dim == "WHEN" else infer_snf_type(df[col].astype(str))
        field_type_rows.append((dim.lower(), skey, vtype))
    if field_type_rows:
        con.executemany("INSERT OR REPLACE INTO snf_field_types VALUES (?, ?, ?)", field_type_rows)

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

    facts_by_dim = {dim: len(dim_rows[dim]) for dim in dim_rows}
    total_facts  = sum(facts_by_dim.values())

    result = {
        "output_path":  str(output_path),
        "download_url": f"/api/mb/download/{output_path.name}",
        "entity_count": len(entity_ids_seen),
        "fact_count":   total_facts,
        "facts_by_dim": facts_by_dim,
        "warnings":     warnings,
        "lens_id":      lens_id,
    }

    return dim_rows, meta_rows, result
