"""
model_builder/inference.py — Column → dimension/key suggestion and type inference.

Suggests SNF dimension and semantic_key from column names.
Infers value_type (number, date, text) from column contents.
Builds the columns payload for the wizard upload step.

These are suggestions only. The human confirms or overrides.
MB may suggest; it may not silently assert meaning.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .config import SAMPLE_ROWS


# Keyword → (dimension, semantic_key) mappings.
# Ordered from most specific to least — first match wins.
_INFER_RULES: List[Tuple[List[str], str, Optional[str]]] = [
    # WHO
    (["artist", "author", "creator", "composer", "writer", "performer"],     "WHO", None),
    (["attorney", "lawyer", "counsel", "timekeeper"],                         "WHO", None),
    (["client", "customer", "patron", "borrower"],                            "WHO", None),
    (["publisher", "label", "imprint", "distributor"],                        "WHO", None),
    (["person", "name", "individual", "contact"],                             "WHO", None),
    (["organization", "org", "company", "firm", "institution"],               "WHO", None),
    (["assigned_to", "owner", "responsible"],                                 "WHO", None),
    # WHAT
    (["title", "name", "subject", "heading"],                                 "WHAT", None),
    (["matter", "case", "docket", "proceeding"],                              "WHAT", None),
    (["type", "format", "genre", "category", "kind", "class"],                "WHAT", None),
    (["description", "summary", "abstract", "note"],                          "WHAT", None),
    (["isbn", "issn", "identifier", "id", "number", "code"],                  "WHAT", None),
    # WHEN
    (["date", "year", "month", "day", "time", "period", "released",
      "published", "created", "modified", "filed", "closed", "opened"],       "WHEN", None),
    # WHERE
    (["place", "location", "city", "state", "country", "region",
      "jurisdiction", "venue", "address", "office"],                          "WHERE", None),
    (["label", "where"],                                                       "WHERE", None),
    # WHY
    (["reason", "purpose", "cause", "status", "disposition",
      "privilege", "classification", "tag"],                                   "WHY",  None),
    # HOW
    (["condition", "method", "format", "medium", "rating",
      "quality", "version", "edition"],                                        "HOW",  None),
]


def suggest_dim_key(col_name: str) -> Tuple[str, str]:
    """
    Infer (dimension, semantic_key) from a column name.
    Returns ('skip', col_name_normalized) if no rule matches.
    """
    normalized = col_name.lower().strip()
    normalized = re.sub(r'^(tbl_|t_|f_|col_)', '', normalized)
    tokens = set(re.split(r'[_\s\-]+', normalized))

    for keywords, dim, key_override in _INFER_RULES:
        if any(kw in normalized or kw in tokens for kw in keywords):
            key = key_override or normalized
            return dim, key

    return 'skip', normalized.replace(" ", "_")


def infer_snf_type(series: pd.Series) -> str:
    """
    Infer the SNF value_type for a column from its raw string values.

    Called AFTER read with dtype=str, so we coerce rather than trust pandas.
    Returns one of: "number", "date", "text".
    Threshold: 95%+ of non-empty values must parse cleanly.

    "enum" is NOT returned here — that is a Reckoner-side decision based on
    distinct_values cardinality, not a property of the source data.
    """
    non_empty = series.dropna().loc[lambda s: s.str.strip() != ""]
    if len(non_empty) == 0:
        return "text"

    numeric = pd.to_numeric(non_empty, errors="coerce")
    if numeric.notna().mean() >= 0.95:
        return "number"

    try:
        parsed = pd.to_datetime(non_empty, errors="coerce")
        if parsed.notna().mean() >= 0.95:
            return "date"
    except Exception:
        pass

    return "text"


def build_columns(df: pd.DataFrame) -> List[Dict]:
    """
    Build the columns payload from a DataFrame:
    [{ name, inferred_type, samples, suggested_dim, suggested_key }]
    """
    cols = []
    for col in df.columns:
        samples = (
            df[col]
            .dropna()
            .astype(str)
            .loc[lambda s: s.str.strip() != '']
            .head(SAMPLE_ROWS)
            .tolist()
        )
        suggested_dim, suggested_key = suggest_dim_key(str(col))
        inferred_type = infer_snf_type(df[col].astype(str))
        cols.append({
            "name":          str(col),
            "inferred_type": inferred_type,
            "samples":       samples,
            "suggested_dim": suggested_dim,
            "suggested_key": suggested_key,
        })
    return cols
