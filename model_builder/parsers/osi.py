"""
model_builder/parsers/osi.py — OSI semantic model parser integration.

Wraps the external osi_parser module. The route in api.py delegates here;
this module owns the OSI-specific logic so api.py stays thin.

OSI produces the same column response shape as /upload so the wizard
handles steps 2–6 without modification.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    from osi_parser import parse_osi_file as _parse_osi_file
    from osi_parser import export_snf_as_osi as _export_snf_as_osi
    AVAILABLE = True
except ImportError:
    AVAILABLE = False


def parse_osi_upload(content: bytes, filename: str) -> Dict[str, Any]:
    """
    Parse an OSI YAML/JSON file and return a columns payload + metadata.

    Returns the same shape as /upload so the wizard handles steps 2–6
    without modification.

    Raises:
        RuntimeError  if osi_parser is not installed
        ValueError    if the file content is invalid
    """
    if not AVAILABLE:
        raise RuntimeError(
            "osi_parser.py required. Ensure osi_parser.py is in the same "
            "directory as model_builder_api.py."
        )

    ext = Path(filename).suffix.lower()
    if ext not in {".yaml", ".yml", ".json"}:
        raise ValueError(f"Unsupported file type: {ext}. OSI parser accepts .yaml, .yml, or .json")

    result = _parse_osi_file(content, filename)

    # OSI has no row data — empty DataFrame with field names as columns.
    col_names = [c["name"] for c in result["columns"]]
    df_osi    = pd.DataFrame(columns=col_names)

    return {
        "df":            df_osi,
        "source_info":   {"type": "osi", "filename": filename, "format": ext.lstrip(".")},
        "columns":       result["columns"],
        "row_count":     result["field_count"],
        "nucleus_hints": result["nucleus_hints"],
        "relationships": result["relationships"],
        "osi_meta":      result["osi_meta"],
        "dataset_count": result["dataset_count"],
        "field_count":   result["field_count"],
        "source_type":   "osi",
    }


def export_osi_model(
    spoke_rows:  List[Dict],
    lens_id:     str,
    description: Optional[str] = "",
) -> Dict[str, Any]:
    """
    Project a compiled SNF substrate as an OSI SemanticModel.

    Raises:
        RuntimeError if osi_parser is not installed
    """
    if not AVAILABLE:
        raise RuntimeError(
            "osi_parser.py required. Ensure osi_parser.py is in the same "
            "directory as model_builder_api.py."
        )

    osi_model = _export_snf_as_osi(
        spoke_rows  = spoke_rows,
        lens_id     = lens_id,
        description = description or "",
    )

    field_count = sum(
        len(ds.get("fields", []))
        for sm in osi_model.get("semantic_model", [])
        for ds in sm.get("datasets", [])
    )
    metric_count = sum(
        len(sm.get("metrics", []))
        for sm in osi_model.get("semantic_model", [])
    )

    return {
        "lens_id":      lens_id,
        "osi_model":    osi_model,
        "field_count":  field_count,
        "metric_count": metric_count,
    }
