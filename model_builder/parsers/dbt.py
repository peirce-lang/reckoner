"""
model_builder/parsers/dbt.py — dbt schema.yml parser integration.

Wraps the external dbt_parser module. The route in api.py delegates here.
dbt produces the same column response shape as /upload so the wizard
handles steps 2–6 without modification.

Dimension mappings are pre-filled at three confidence levels:
    deterministic — entity:primary/foreign, dimension:time, metrics
    strong_hint   — is_/has_* booleans, *_date/*_id/*_region suffixes
    needs_review  — categorical dimensions with no stronger signal
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd

try:
    from dbt_parser import parse_dbt_schema as _parse_dbt_schema
    AVAILABLE = True
except ImportError:
    AVAILABLE = False


def parse_dbt_upload(content: bytes, filename: str) -> Dict[str, Any]:
    """
    Parse a dbt schema.yml file and return a columns payload + metadata.

    Returns the same shape as /upload so the wizard handles steps 2–6
    without modification.

    Raises:
        RuntimeError  if dbt_parser is not installed
        ValueError    for fatal parse errors
    """
    if not AVAILABLE:
        raise RuntimeError(
            "dbt_parser.py required. Ensure dbt_parser.py is in the same "
            "directory as model_builder_api.py."
        )

    ext = Path(filename).suffix.lower()
    if ext not in {".yaml", ".yml"}:
        raise ValueError(f"Unsupported file type: {ext}. dbt parser accepts .yaml or .yml files.")

    result = _parse_dbt_schema(content.decode("utf-8"))

    if result.get("errors"):
        fatal = [e for e in result["errors"] if not e.startswith("Warning")]
        if fatal:
            raise ValueError("; ".join(fatal))

    if not result["models"]:
        raise ValueError(
            "No models found in schema.yml. "
            "Check that the file is a valid dbt version 2 schema."
        )

    columns         = []
    nucleus_hints   = {}
    lens_candidates = {}
    dbt_meta        = {}

    for model in result["models"]:
        model_name = model["model_name"]

        if model["nucleus"]:
            nucleus_hints[model_name] = model["nucleus"]
        if model["lens_candidates"]:
            lens_candidates[model_name] = model["lens_candidates"]

        dbt_meta[model_name] = {
            "description":        model["description"],
            "agg_time_dimension": model["agg_time_dimension"],
            "group":              model["meta"].get("group"),
        }

        if model["nucleus"]:
            columns.append({
                "name":           model["nucleus"],
                "suggested_dim":  "skip",
                "suggested_key":  model["nucleus"],
                "is_nucleus":     True,
                "confidence":     "deterministic",
                "mapping_source": model["nucleus_source"],
                "sample_values":  [],
                "model":          model_name,
            })

        for m in model["mappings"]:
            columns.append({
                "name":           m["column_name"],
                "suggested_dim":  m["dimension"] or "skip",
                "suggested_key":  m["semantic_key"],
                "is_nucleus":     False,
                "confidence":     m["confidence"],
                "mapping_source": m["mapping_source"],
                "description":    m["description"],
                "notes":          m["notes"],
                "sample_values":  [],
                "model":          model_name,
            })

    col_names   = [c["name"] for c in columns]
    df_dbt      = pd.DataFrame(columns=col_names)
    field_count = len([c for c in columns if not c.get("is_nucleus")])

    return {
        "df":             df_dbt,
        "source_info":    {"type": "dbt", "filename": filename, "format": "yaml"},
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
