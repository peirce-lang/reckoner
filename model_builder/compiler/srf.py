"""
model_builder/compiler/srf.py — SRF record emission.

Emits one .srf file per entity into the SRF imports directory.
This is a post-compile artifact — it runs after the DuckDB substrate
is written and closed, not inside it.

SRF is an interchange artifact. DuckDB is a substrate artifact.
They are related but not the same responsibility. Keeping them
separate here makes that boundary physically real.

compile_artifact() in compiler/__init__.py calls emit_srf_records()
after emit_duckdb() returns. Neither emitter knows about the other.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from ..config import SRF_IMPORTS_DIR


def emit_srf_records(
    dim_rows:   Dict[str, List[Dict]],
    meta_rows:  List[Dict],
    spec:       Dict[str, Any],
) -> Tuple[int, List[str]]:
    """
    Write one .srf file per entity into SRF_IMPORTS_DIR/<lens_id>/.

    Args:
        dim_rows:  { dimension → [spoke row dicts] } as produced by emit_duckdb
        meta_rows: list of entity meta dicts (entity_id, nucleus, label, ...)
        spec:      the raw BuildSpec dict

    Returns:
        (srf_count, warnings) — count of records written, list of error strings
    """
    warnings: List[str] = []

    try:
        from snf_peirce.srf import SRFRecord, SRFValidationError
    except ImportError:
        warnings.append(
            "SRF export skipped: snf-peirce >= 0.1.10 required. "
            "pip install snf-peirce>=0.1.10"
        )
        return 0, warnings

    lens_id        = spec["lens"]["lens_id"]
    nucleus_spec   = spec["nucleus"]
    source_name    = spec.get("source", {}).get("filename") or spec.get("target", {}).get("output_name", "unknown")
    translator_ver = spec.get("provenance", {}).get("translator_version", "1.0.0")
    now            = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    srf_imports_dir = SRF_IMPORTS_DIR / lens_id
    srf_imports_dir.mkdir(parents=True, exist_ok=True)

    # Build facts per entity_id from dim_rows
    facts_by_entity: Dict[str, list] = {}
    for dim in ["WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"]:
        for r in dim_rows.get(dim, []):
            eid = r["entity_id"]
            facts_by_entity.setdefault(eid, []).append({
                "dimension":    dim,
                "semantic_key": r["semantic_key"],
                "value":        r["value"],
            })

    srf_count  = 0
    srf_errors = 0

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
            record   = SRFRecord.from_dict(record_dict)
            safe_eid = eid.replace(":", "_").replace("/", "_")
            out_file = srf_imports_dir / f"{safe_eid}.srf"
            out_file.write_text(
                json.dumps(record.to_dict(), indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
            srf_count += 1
        except Exception as e:
            srf_errors += 1
            warnings.append(f"SRF emit failed for {eid}: {e}")

    if srf_count > 0:
        print(f"[compiler/srf] Emitted {srf_count} SRF records to {srf_imports_dir}")
    if srf_errors > 0:
        print(f"[compiler/srf] {srf_errors} SRF emit errors (see warnings)")

    return srf_count, warnings
