# dbt_parser.py
# Model Builder — dbt schema.yml vocabulary parser (MB-7)
#
# Pattern: sibling to osi_parser.py
# Input:   dbt schema.yml (version 2, semantic_model block optional)
# Output:  parsed model with SNF dimension mappings, confidence levels,
#          nucleus declaration, lens candidates from metrics block
#
# Confidence levels:
#   deterministic — no human review needed
#   strong_hint   — pre-filled, human confirms
#   needs_review  — flagged for human mapping decision
#
# Mapping rules:
#   entity: primary            → nucleus            (deterministic)
#   entity: foreign            → WHO                (deterministic)
#   dimension: type: time      → WHEN               (deterministic)
#   metrics block              → lens candidates    (deterministic)
#   is_* / has_* boolean       → HOW or WHY         (strong_hint)
#   *_id suffix, non-FK        → WHO                (strong_hint)
#   *_date / *_at suffix       → WHEN               (strong_hint)
#   *_reason / *_purpose       → WHY                (strong_hint)
#   *_region / *_country / *_city → WHERE           (strong_hint)
#   dimension: type: categorical → WHAT             (needs_review)
#   everything else            → needs_review

import yaml
import re
from typing import Optional


# ---------------------------------------------------------------------------
# Column name heuristics
# ---------------------------------------------------------------------------

_WHEN_SUFFIXES = ("_date", "_at", "_time", "_year", "_month", "_day", "_ts")
_WHO_SUFFIXES = ("_id",)
_WHERE_PATTERNS = re.compile(
    r"(region|country|city|location|site|address|territory|zone|office)", re.IGNORECASE
)
_WHY_PATTERNS = re.compile(
    r"(reason|purpose|justification|cause|rationale|exception)", re.IGNORECASE
)
_HOW_BOOLEAN_PREFIXES = ("is_", "has_", "can_", "did_", "was_")


def _classify_by_name(col_name: str) -> tuple[str, str]:
    """
    Heuristic dimension classification from column name alone.
    Returns (dimension, confidence).
    Only called when dbt structural typing is absent or ambiguous.
    """
    name = col_name.lower()

    # Boolean prefix → HOW (state) or WHY (condition)
    for prefix in _HOW_BOOLEAN_PREFIXES:
        if name.startswith(prefix):
            # exception/budget flags lean WHY; state flags lean HOW
            if _WHY_PATTERNS.search(name):
                return "WHY", "strong_hint"
            return "HOW", "strong_hint"

    # *_reason, *_purpose etc → WHY
    if _WHY_PATTERNS.search(name):
        return "WHY", "strong_hint"

    # Geographic signals → WHERE
    if _WHERE_PATTERNS.search(name):
        return "WHERE", "strong_hint"

    # Time suffixes → WHEN
    for suffix in _WHEN_SUFFIXES:
        if name.endswith(suffix):
            return "WHEN", "strong_hint"

    # *_id suffix (without FK declaration) → WHO
    for suffix in _WHO_SUFFIXES:
        if name.endswith(suffix):
            return "WHO", "strong_hint"

    # Fall through — categorical, needs human review
    return "WHAT", "needs_review"


# ---------------------------------------------------------------------------
# Column parser
# ---------------------------------------------------------------------------

def _parse_column(col: dict) -> dict:
    """
    Parse a single dbt column definition into a mapped field record.
    Applies structural typing first, falls back to name heuristics.
    """
    name = col.get("name", "")
    description = col.get("description", "")
    data_type = col.get("data_type", "")
    entity_block = col.get("entity")
    dimension_block = col.get("dimension")

    # Guard against malformed YAML where these blocks are strings instead of dicts
    if isinstance(entity_block, str):
        entity_block = None
    if isinstance(dimension_block, str):
        dimension_block = None

    result = {
        "column_name": name,
        "description": description,
        "dimension": None,
        "semantic_key": name,
        "confidence": None,
        "mapping_source": None,
        "is_nucleus": False,
        "lens_candidate": False,
        "notes": [],
    }

    # --- Structural typing (deterministic) ---

    # Primary entity → nucleus
    if entity_block and entity_block.get("type") == "primary":
        result["is_nucleus"] = True
        result["dimension"] = None  # nucleus is not a dimension fact
        result["confidence"] = "deterministic"
        result["mapping_source"] = "dbt:entity:primary"
        return result

    # Foreign entity → WHO
    if entity_block and entity_block.get("type") in ("foreign", "unique", "natural"):
        result["dimension"] = "WHO"
        result["confidence"] = "deterministic"
        result["mapping_source"] = f"dbt:entity:{entity_block.get('type')}"
        entity_name = entity_block.get("name")
        if entity_name:
            result["semantic_key"] = entity_name
        return result

    # Time dimension → WHEN
    if dimension_block and dimension_block.get("type") == "time":
        result["dimension"] = "WHEN"
        result["confidence"] = "deterministic"
        result["mapping_source"] = "dbt:dimension:time"
        dim_name = dimension_block.get("name")
        if dim_name:
            result["semantic_key"] = dim_name
        granularity = col.get("granularity")
        if granularity:
            result["notes"].append(f"granularity: {granularity}")
        return result

    # Boolean data type declared explicitly
    if data_type and data_type.lower() == "boolean":
        dim, confidence = _classify_by_name(name)
        result["dimension"] = dim
        result["confidence"] = confidence
        result["mapping_source"] = "dbt:data_type:boolean + name_heuristic"
        return result

    # Categorical dimension — type is explicit but value is ambiguous
    if dimension_block and dimension_block.get("type") == "categorical":
        # Try name heuristic to improve on WHAT/needs_review default
        dim, confidence = _classify_by_name(name)
        result["dimension"] = dim
        result["confidence"] = confidence
        result["mapping_source"] = "dbt:dimension:categorical + name_heuristic"
        dim_name = dimension_block.get("name")
        if dim_name:
            result["semantic_key"] = dim_name
        return result

    # --- Name heuristics only (no structural typing) ---
    dim, confidence = _classify_by_name(name)
    result["dimension"] = dim
    result["confidence"] = confidence
    result["mapping_source"] = "name_heuristic"
    return result


# ---------------------------------------------------------------------------
# Metrics parser
# ---------------------------------------------------------------------------

def _parse_metrics(metrics: list) -> list:
    """
    Parse dbt metrics block into lens candidates.
    Metrics are not coordinate facts — they are aggregations over facts.
    Each metric becomes a lens candidate, not a dimension mapping.
    """
    candidates = []
    for m in metrics:
        candidates.append({
            "lens_id": m.get("name"),
            "description": m.get("description", ""),
            "agg": m.get("agg"),
            "expr": m.get("expr"),
            "type": m.get("type", "simple"),
            "notes": "Certified warehouse metric — calculation stays in warehouse. "
                     "SNF routes to this coordinate without touching the math.",
        })
    return candidates


# ---------------------------------------------------------------------------
# Model parser
# ---------------------------------------------------------------------------

def _parse_model(model: dict) -> dict:
    """
    Parse a single dbt model block into a Model Builder intake record.
    """
    name = model.get("name", "")
    description = model.get("description", "")
    semantic_model = model.get("semantic_model", {})
    # semantic_model can be True/False (shorthand) or a dict — normalize
    if not isinstance(semantic_model, dict):
        semantic_model = {"enabled": bool(semantic_model)}
    agg_time_dimension = model.get("agg_time_dimension")
    primary_entity = model.get("primary_entity")
    columns = model.get("columns", [])
    metrics = model.get("metrics", [])

    # Parse all columns
    parsed_columns = [_parse_column(col) for col in columns]

    # Identify nucleus
    nucleus_cols = [c for c in parsed_columns if c["is_nucleus"]]
    nucleus = nucleus_cols[0]["column_name"] if nucleus_cols else primary_entity

    # Separate mappings from nucleus
    mappings = [c for c in parsed_columns if not c["is_nucleus"]]

    # Confidence summary
    deterministic = [c for c in mappings if c["confidence"] == "deterministic"]
    strong_hint = [c for c in mappings if c["confidence"] == "strong_hint"]
    needs_review = [c for c in mappings if c["confidence"] == "needs_review"]

    # Lens candidates from metrics
    lens_candidates = _parse_metrics(metrics)

    return {
        "model_name": name,
        "description": description,
        "source_vocabulary": "dbt_schema_yml",
        "nucleus": nucleus,
        "nucleus_source": "dbt:entity:primary" if nucleus_cols else "dbt:primary_entity_field",
        "agg_time_dimension": agg_time_dimension,
        "mappings": mappings,
        "lens_candidates": lens_candidates,
        "summary": {
            "total_columns": len(columns),
            "nucleus_columns": len(nucleus_cols),
            "deterministic_mappings": len(deterministic),
            "strong_hint_mappings": len(strong_hint),
            "needs_review_mappings": len(needs_review),
            "lens_candidates": len(lens_candidates),
        },
        "meta": {
            "group":            semantic_model.get("group"),
            "semantic_model_enabled": semantic_model.get("enabled", True),
        },
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_dbt_schema(yaml_text: str) -> dict:
    """
    Parse a dbt schema.yml file.

    Args:
        yaml_text: Raw YAML string content of the schema.yml file.

    Returns:
        dict with:
          - models: list of parsed model records
          - summary: aggregate counts across all models
          - errors: any parse errors encountered
    """
    errors = []

    try:
        doc = yaml.safe_load(yaml_text)
    except yaml.YAMLError as e:
        return {"models": [], "summary": {}, "errors": [f"YAML parse error: {e}"]}

    if not isinstance(doc, dict):
        return {"models": [], "summary": {}, "errors": ["Top-level YAML is not a mapping."]}

    # Detect dbt_project.yml uploaded by mistake — it has config-version, model-paths, profile
    if any(k in doc for k in ("config-version", "model-paths", "profile", "clean-targets")):
        return {"models": [], "summary": {}, "errors": [
            "This looks like dbt_project.yml — that's the project config file, not the model definitions. "
            "Upload your schema.yml instead. It's usually at models/schema.yml inside your dbt project."
        ]}

    version = doc.get("version")
    if version and version != 2:
        errors.append(f"Warning: expected dbt schema version 2, got {version}. Proceeding.")

    raw_models = doc.get("models", [])
    if not raw_models:
        return {"models": [], "summary": {}, "errors": ["No models found in schema.yml."]}

    parsed_models = []
    for m in raw_models:
        try:
            parsed_models.append(_parse_model(m))
        except Exception as e:
            errors.append(f"Error parsing model '{m.get('name', '?')}': {e}")

    total_columns = sum(m["summary"]["total_columns"] for m in parsed_models)
    total_det = sum(m["summary"]["deterministic_mappings"] for m in parsed_models)
    total_hint = sum(m["summary"]["strong_hint_mappings"] for m in parsed_models)
    total_review = sum(m["summary"]["needs_review_mappings"] for m in parsed_models)
    total_lens = sum(m["summary"]["lens_candidates"] for m in parsed_models)

    return {
        "models": parsed_models,
        "summary": {
            "model_count": len(parsed_models),
            "total_columns": total_columns,
            "deterministic_mappings": total_det,
            "strong_hint_mappings": total_hint,
            "needs_review_mappings": total_review,
            "lens_candidates": total_lens,
        },
        "errors": errors,
    }


def parse_dbt_schema_file(path: str) -> dict:
    """Convenience wrapper — read file then parse."""
    with open(path, "r", encoding="utf-8") as f:
        return parse_dbt_schema(f.read())
