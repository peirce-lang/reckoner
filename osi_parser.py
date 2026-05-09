"""
osi_parser.py — OSI Model Builder Format Parser (MB-6)

Part of snf-toolkit / reckoner (Model Builder backend).

Bidirectional OSI ↔ SNF translator.

INGEST direction (OSI → SNF):
    Parse an OSI YAML or JSON semantic model definition.
    Walk SemanticModel → Dataset → Field structure.
    Map fields to SNF dimensions using OSI's own semantic signals:
        - Field.dimension.is_time = true  → WHEN (certain)
        - Field.ai_context.instructions   → dimension assignment hints
        - Field.name / description        → semantic_key + coordinate label
        - Dataset.primary_key             → nucleus declaration
        - Relationship (from/to)          → spoke connections
    Produce the same `columns` response shape as /upload so the
    existing wizard (steps 2–6) handles everything downstream.

EXPORT direction (SNF → OSI):
    Walk a compiled SNF coordinate space and emit an OSI
    SemanticModel structure (Dataset / Field / Relationship).
    Enables Peirce queries against warehouse data via OSI bridge.

Usage:
    from osi_parser import parse_osi_file, export_snf_as_osi

    # Ingest
    result = parse_osi_file(file_bytes, filename="model.yaml")
    # result = { upload_token, columns, row_count, osi_meta }

    # Export
    osi_model = export_snf_as_osi(spoke_rows, lens_id="my_lens")
    # osi_model = { version, semantic_model: [...] }

Dependencies:
    PyYAML  — pip install pyyaml   (YAML input)
    Standard library only otherwise.

OSI schema reference:
    https://github.com/open-semantic-interchange/OSI/core-spec/osi-schema.json
    v0.1.1 — SemanticModel → Dataset → Field / Metric / Relationship
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# Dimension inference from OSI field signals
# ─────────────────────────────────────────────────────────────────────────────

# Keyword sets for dimension inference from field names / descriptions.
# Same token-matching approach as model_builder_api._suggest_dim_key.
# OSI-specific additions: metric-flavoured terms route to HOW.
_DIM_KEYWORDS: List[Tuple[List[str], str]] = [
    # WHEN — explicit OSI flag handles this first; keywords are fallback
    (["date", "time", "year", "month", "day", "period",
      "created", "modified", "updated", "published", "filed",
      "opened", "closed", "released", "timestamp"],              "WHEN"),
    # WHO
    (["artist", "author", "creator", "composer", "writer",
      "performer", "attorney", "lawyer", "client", "customer",
      "person", "name", "individual", "organization", "company",
      "owner", "assigned", "publisher", "label"],                "WHO"),
    # WHERE
    (["place", "location", "city", "state", "country", "region",
      "venue", "address", "office", "jurisdiction", "geography"], "WHERE"),
    # WHY
    (["reason", "purpose", "cause", "status", "disposition",
      "privilege", "classification", "tag", "category",
      "flag", "type"],                                            "WHY"),
    # HOW — metrics, quantities, measures
    (["amount", "count", "total", "sum", "average", "rate",
      "ratio", "score", "metric", "measure", "quantity",
      "revenue", "cost", "price", "hours", "duration",
      "percentage", "pct"],                                       "HOW"),
    # WHAT — broad catch-all for entity descriptors
    (["title", "subject", "heading", "description", "summary",
      "abstract", "note", "genre", "format", "identifier",
      "id", "code", "isbn", "issn", "matter", "case"],            "WHAT"),
]

# Keywords found in ai_context.instructions that hint at a dimension.
_AI_CONTEXT_DIM_HINTS: List[Tuple[List[str], str]] = [
    (["when", "time", "date", "temporal", "period"],   "WHEN"),
    (["who", "person", "author", "artist", "client"],  "WHO"),
    (["where", "location", "place", "geography"],      "WHERE"),
    (["why", "reason", "purpose", "cause", "status"],  "WHY"),
    (["how", "metric", "measure", "quantity", "amount"], "HOW"),
    (["what", "title", "subject", "description"],      "WHAT"),
]


def _normalize(s: str) -> str:
    """Lowercase, strip, split on non-alphanumeric into tokens."""
    return s.lower().strip()


def _tokens(s: str) -> set:
    return set(re.split(r'[_\s\-/\.]+', _normalize(s)))


def _infer_dim_from_name(name: str, description: str = "") -> str:
    """
    Infer SNF dimension from field name + description tokens.
    Returns dimension string or 'WHAT' as default.
    """
    text  = f"{name} {description}"
    norm  = _normalize(text)
    toks  = _tokens(text)

    for keywords, dim in _DIM_KEYWORDS:
        if any(kw in norm or kw in toks for kw in keywords):
            return dim

    return "WHAT"  # safe default — most OSI fields are entity descriptors


def _infer_dim_from_ai_context(ai_context: Any) -> Optional[str]:
    """
    Extract dimension hint from ai_context.instructions if present.
    Returns dimension string or None if no strong signal.
    """
    if not ai_context:
        return None

    instructions = ""
    if isinstance(ai_context, str):
        instructions = ai_context
    elif isinstance(ai_context, dict):
        instructions = ai_context.get("instructions", "")

    if not instructions:
        return None

    norm = _normalize(instructions)
    toks = _tokens(instructions)

    for keywords, dim in _AI_CONTEXT_DIM_HINTS:
        if any(kw in norm or kw in toks for kw in keywords):
            return dim

    return None


def _extract_synonyms(ai_context: Any) -> List[str]:
    """Extract ai_context.synonyms list if present."""
    if isinstance(ai_context, dict):
        return ai_context.get("synonyms", [])
    return []


def _extract_examples(ai_context: Any) -> List[str]:
    """Extract ai_context.examples list — these become search_hints."""
    if isinstance(ai_context, dict):
        return ai_context.get("examples", [])
    return []


def _semantic_key(field_name: str) -> str:
    """Normalize field name to snake_case semantic key."""
    s = re.sub(r'[^a-zA-Z0-9_\s]', '', field_name)
    s = re.sub(r'[\s\-]+', '_', s.strip())
    return s.lower()


# ─────────────────────────────────────────────────────────────────────────────
# OSI file loading
# ─────────────────────────────────────────────────────────────────────────────

def _load_osi(content: bytes, filename: str) -> Dict:
    """
    Parse OSI YAML or JSON bytes into a dict.
    Raises ValueError with a human-readable message on failure.
    """
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""

    if ext in ("yaml", "yml"):
        try:
            import yaml
            data = yaml.safe_load(content.decode("utf-8"))
        except ImportError:
            raise ValueError(
                "PyYAML required for YAML OSI files. pip install pyyaml"
            )
        except Exception as e:
            raise ValueError(f"Could not parse YAML: {e}")
    elif ext == "json":
        try:
            data = json.loads(content.decode("utf-8"))
        except Exception as e:
            raise ValueError(f"Could not parse JSON: {e}")
    else:
        # Try JSON first, then YAML
        try:
            data = json.loads(content.decode("utf-8"))
        except Exception:
            try:
                import yaml
                data = yaml.safe_load(content.decode("utf-8"))
            except Exception as e:
                raise ValueError(
                    f"Could not parse as JSON or YAML: {e}. "
                    "Use a .json or .yaml extension for explicit format detection."
                )

    if not isinstance(data, dict):
        raise ValueError("OSI file must be a JSON/YAML object at the top level.")

    if "semantic_model" not in data:
        raise ValueError(
            "No 'semantic_model' key found. "
            "Is this a valid OSI file? Expected shape: { version, semantic_model: [...] }"
        )

    return data


# ─────────────────────────────────────────────────────────────────────────────
# Core field mapper — OSI Field → Model Builder column entry
# ─────────────────────────────────────────────────────────────────────────────

def _map_field(
    field:          Dict,
    dataset_name:   str,
    is_primary_key: bool = False,
    is_foreign_key: bool = False,
) -> Dict:
    """
    Map one OSI Field to a Model Builder column entry.

    Column entry shape (matches /upload response):
        {
            name:          str   — column identifier (dataset.field_name)
            samples:       list  — from ai_context.examples (used as search hints)
            suggested_dim: str   — WHO/WHAT/WHEN/WHERE/WHY/HOW/skip
            suggested_key: str   — snake_case semantic key
            # OSI-specific extras (not part of core column spec but passed through):
            osi_label:     str   — Field.label (human-readable display name)
            osi_synonyms:  list  — ai_context.synonyms (coordinate aliases)
            confidence:    str   — "certain" | "high" | "inferred"
            is_nucleus:    bool  — true if this field is the dataset primary_key
            is_foreign_key: bool — true if this field is a FK to another dataset
        }
    """
    name        = field.get("name", "")
    label       = field.get("label", name)
    description = field.get("description", "")
    ai_context  = field.get("ai_context")
    dimension   = field.get("dimension", {}) or {}

    # Dimension assignment — priority order:
    # 1. Primary key → skip (nucleus, not a coordinate)
    # 2. OSI is_time flag → WHEN (certain)
    # 3. Foreign key → WHO (high — FK references an entity in another dataset)
    # 4. ai_context.instructions hint → (high)
    # 5. Name + description inference → (inferred)

    if is_primary_key:
        suggested_dim = "skip"
        confidence    = "certain"
    elif dimension.get("is_time"):
        suggested_dim = "WHEN"
        confidence    = "certain"
    elif is_foreign_key:
        suggested_dim = "WHO"
        confidence    = "high"
    else:
        ai_hint = _infer_dim_from_ai_context(ai_context)
        if ai_hint:
            suggested_dim = ai_hint
            confidence    = "high"
        else:
            suggested_dim = _infer_dim_from_name(name, description)
            confidence    = "inferred"

    skey     = _semantic_key(name)
    examples = _extract_examples(ai_context)
    synonyms = _extract_synonyms(ai_context)

    # Qualify column name with dataset to avoid collisions across datasets
    qualified_name = f"{dataset_name}.{name}" if dataset_name else name

    return {
        "name":           qualified_name,
        "samples":        examples[:10],
        "suggested_dim":  suggested_dim,
        "suggested_key":  skey,
        "osi_label":      label,
        "osi_synonyms":   synonyms,
        "confidence":     confidence,
        "is_nucleus":     is_primary_key,
        "is_foreign_key": is_foreign_key,
    }


def _map_metric(metric: Dict, dataset_name: str) -> Dict:
    """
    Map an OSI Metric to a HOW coordinate column entry.
    Metrics are quantitative measures — they always map to HOW.
    """
    name       = metric.get("name", "")
    ai_context = metric.get("ai_context")
    examples   = _extract_examples(ai_context)
    synonyms   = _extract_synonyms(ai_context)

    qualified_name = f"{dataset_name}.{name}" if dataset_name else name

    return {
        "name":          qualified_name,
        "samples":       examples[:10],
        "suggested_dim": "HOW",
        "suggested_key": _semantic_key(name),
        "osi_label":     metric.get("description", name),
        "osi_synonyms":  synonyms,
        "confidence":    "certain",   # metrics are always HOW
        "is_nucleus":    False,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Ingest direction: OSI → Model Builder columns
# ─────────────────────────────────────────────────────────────────────────────

def parse_osi_model(data: Dict) -> Dict:
    """
    Walk an OSI model dict and produce the Model Builder parse result.

    Returns:
        {
            columns:      list   — column entries for wizard step 2
            nucleus_hints: list  — primary_key fields per dataset (for step 4)
            relationships: list  — Relationship entries (for cross-entity linking)
            osi_meta:     dict   — top-level OSI metadata (name, description, etc.)
            dataset_count: int
            field_count:   int
        }
    """
    columns:       List[Dict] = []
    nucleus_hints: List[Dict] = []
    relationships: List[Dict] = []

    semantic_models = data.get("semantic_model", [])
    if not semantic_models:
        raise ValueError("semantic_model array is empty — nothing to parse.")

    # Use first SemanticModel (most OSI files have one top-level model)
    model      = semantic_models[0]
    model_name = model.get("name", "osi_model")
    datasets   = model.get("datasets", [])
    metrics    = model.get("metrics", [])
    rels       = model.get("relationships", [])

    total_fields = 0

    # Build foreign key set before walking fields so we can detect FK columns
    # during field mapping. FK columns (from_columns in relationships) reference
    # WHO entities in another dataset — they should map to WHO, not be inferred.
    fk_columns: set = set()
    for rel in rels:
        for col in rel.get("from_columns", []):
            fk_columns.add(col)

    for dataset in datasets:
        ds_name    = dataset.get("name", "dataset")
        pk_columns = set(dataset.get("primary_key") or [])
        fields     = dataset.get("fields") or []

        # Track nucleus hint for this dataset
        if pk_columns:
            nucleus_hints.append({
                "dataset":    ds_name,
                "pk_columns": list(pk_columns),
                "type":       "compound" if len(pk_columns) > 1 else "single",
            })

        for field in fields:
            field_name      = field.get("name", "")
            is_pk           = field_name in pk_columns
            is_fk           = field_name in fk_columns and not is_pk
            col             = _map_field(field, ds_name, is_primary_key=is_pk, is_foreign_key=is_fk)
            columns.append(col)
            total_fields   += 1

    # Top-level metrics → HOW coordinates
    for metric in metrics:
        col = _map_metric(metric, model_name)
        columns.append(col)
        total_fields += 1

    # Relationships → spoke connection hints
    for rel in rels:
        relationships.append({
            "name":         rel.get("name", ""),
            "from_dataset": rel.get("from", ""),
            "to_dataset":   rel.get("to", ""),
            "from_columns": rel.get("from_columns", []),
            "to_columns":   rel.get("to_columns", []),
        })

    return {
        "columns":       columns,
        "nucleus_hints": nucleus_hints,
        "relationships": relationships,
        "osi_meta": {
            "model_name":  model_name,
            "description": model.get("description", ""),
            "ai_context":  model.get("ai_context"),
            "version":     data.get("version", "0.1.1"),
        },
        "dataset_count": len(datasets),
        "field_count":   total_fields,
    }


def parse_osi_file(content: bytes, filename: str) -> Dict:
    """
    Entry point for the /api/mb/osi/parse endpoint.

    Loads the file, parses the OSI model, returns the wizard-ready response.
    The upload_token is handled by the caller (model_builder_api.py) which
    creates the session after this function returns the parsed columns.

    Args:
        content:  Raw file bytes from the upload
        filename: Original filename (used for format detection)

    Returns:
        {
            columns:       list  — wizard step 2 column entries
            nucleus_hints: list  — primary key suggestions for step 4
            relationships: list  — relationship hints
            osi_meta:      dict  — model name, description, version
            dataset_count: int
            field_count:   int
        }

    Raises:
        ValueError: with human-readable message on parse or validation failure
    """
    data = _load_osi(content, filename)
    return parse_osi_model(data)


# ─────────────────────────────────────────────────────────────────────────────
# Export direction: SNF spoke rows → OSI model
# ─────────────────────────────────────────────────────────────────────────────

def export_snf_as_osi(
    spoke_rows: List[Dict],
    lens_id:    str,
    description: str = "",
) -> Dict:
    """
    Project SNF spoke rows back out as an OSI SemanticModel structure.

    Enables warehouse tools that speak OSI to consume a Reckoner substrate
    without knowing anything about SNF internals.

    Args:
        spoke_rows:  List of dicts with keys:
                         entity_id, dimension, semantic_key, value, lens_id
                     Shape matches snf_spoke table from model_builder_api.
        lens_id:     The lens identifier — becomes SemanticModel.name
        description: Optional human-readable description

    Returns:
        OSI-shaped dict ready for json.dumps or yaml.dump:
        {
            version: "0.1.1",
            semantic_model: [
                {
                    name: lens_id,
                    description: ...,
                    datasets: [ { name, fields: [...] } ],
                    metrics:  [ ... ],   # HOW dimension facts
                    relationships: []    # not inferrable from spoke rows alone
                }
            ]
        }

    Note on relationships:
        Relationship structure (foreign keys between datasets) is not
        recoverable from spoke rows alone — it requires the original
        BuildSpec or schema metadata. The export emits an empty
        relationships array. If you have the BuildSpec, pass relationship
        hints to the caller to merge in.
    """
    # Group semantic_keys by dimension
    dim_keys: Dict[str, Dict[str, List[str]]] = {}
    for row in spoke_rows:
        dim  = (row.get("dimension") or "").upper()
        skey = row.get("semantic_key", "")
        val  = row.get("value", "")
        if not dim or not skey:
            continue
        if dim not in dim_keys:
            dim_keys[dim] = {}
        if skey not in dim_keys[dim]:
            dim_keys[dim][skey] = []
        if val and len(dim_keys[dim][skey]) < 5:
            dim_keys[dim][skey].append(val)   # keep up to 5 sample values

    # Build OSI Fields from non-HOW dimensions
    fields: List[Dict] = []
    metrics: List[Dict] = []

    _SNF_TO_OSI_DIM: Dict[str, Dict] = {
        "WHEN": {"is_time": True},
    }

    for dim in ["WHO", "WHAT", "WHEN", "WHERE", "WHY"]:
        for skey, samples in (dim_keys.get(dim) or {}).items():
            field: Dict[str, Any] = {
                "name":  skey,
                "label": skey.replace("_", " ").title(),
                "expression": {
                    "dialects": [
                        {"dialect": "ANSI_SQL", "expression": skey}
                    ]
                },
            }
            if dim in _SNF_TO_OSI_DIM:
                field["dimension"] = _SNF_TO_OSI_DIM[dim]
            if samples:
                field["ai_context"] = {
                    "instructions": f"SNF dimension: {dim}",
                    "examples":     samples,
                }
            fields.append(field)

    # HOW dimension → OSI Metrics
    for skey, samples in (dim_keys.get("HOW") or {}).items():
        metric: Dict[str, Any] = {
            "name":        skey,
            "description": skey.replace("_", " ").title(),
            "expression": {
                "dialects": [
                    {"dialect": "ANSI_SQL", "expression": skey}
                ]
            },
        }
        if samples:
            metric["ai_context"] = {
                "instructions": "SNF HOW dimension — quantitative measure",
                "examples":     samples,
            }
        metrics.append(metric)

    dataset = {
        "name":        lens_id,
        "source":      lens_id,
        "description": description or f"Exported from SNF substrate: {lens_id}",
        "fields":      fields,
    }

    osi_model: Dict[str, Any] = {
        "version": "0.1.1",
        "semantic_model": [
            {
                "name":          lens_id,
                "description":   description or f"SNF substrate export: {lens_id}",
                "datasets":      [dataset],
                "relationships": [],   # see docstring — not recoverable from spoke rows
            }
        ],
    }

    if metrics:
        osi_model["semantic_model"][0]["metrics"] = metrics

    return osi_model


# ─────────────────────────────────────────────────────────────────────────────
# Flat JSON array support (for /upload extension)
# ─────────────────────────────────────────────────────────────────────────────

def parse_json_array(content: bytes, filename: str) -> Dict:
    """
    Parse a flat JSON array of objects into a DataFrame-compatible structure.

    Accepts:
        [ { "key": "value", ... }, ... ]   — array of flat objects (primary case)
        { "data": [ ... ] }                — common API wrapper shape
        { "results": [ ... ] }             — another common API wrapper shape
        { "items": [ ... ] }               — another common API wrapper shape

    Returns the same shape as _build_columns() in model_builder_api.py:
        {
            rows:     list[dict]  — the flattened records (for DataFrame creation)
            columns:  list        — column entries for wizard step 2
            row_count: int
        }

    Raises:
        ValueError: if the content is not a parseable flat JSON array
    """
    try:
        data = json.loads(content.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"Could not parse JSON: {e}")

    # Unwrap common API envelope shapes
    if isinstance(data, dict):
        for key in ("data", "results", "items", "records", "entries"):
            if key in data and isinstance(data[key], list):
                data = data[key]
                break
        else:
            raise ValueError(
                "JSON object doesn't contain a recognizable array key "
                "(expected 'data', 'results', 'items', 'records', or 'entries'). "
                "For nested JSON, use an OSI model or a custom translator."
            )

    if not isinstance(data, list):
        raise ValueError("JSON must be an array of objects at the top level.")

    if not data:
        raise ValueError("JSON array is empty.")

    # Validate flat structure — warn on nested values but don't block
    # Flatten one level of nesting using dot notation
    flat_rows = []
    for row in data:
        if not isinstance(row, dict):
            raise ValueError(
                "JSON array must contain objects (dicts), not primitives."
            )
        flat_row = {}
        for k, v in row.items():
            if isinstance(v, dict):
                # One level of flattening: {"artist": {"name": "X"}} → {"artist.name": "X"}
                for nested_k, nested_v in v.items():
                    flat_row[f"{k}.{nested_k}"] = str(nested_v) if nested_v is not None else ""
            elif isinstance(v, list):
                # Lists become pipe-joined strings
                flat_row[k] = " | ".join(str(i) for i in v) if v else ""
            else:
                flat_row[k] = str(v) if v is not None else ""
        flat_rows.append(flat_row)

    return {
        "rows":      flat_rows,
        "row_count": len(flat_rows),
    }
