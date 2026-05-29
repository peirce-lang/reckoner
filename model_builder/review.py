"""
model_builder/review.py — Pre-ingest review: flag generation.

Produces warnings and info flags before a BuildSpec is compiled.
Flags are advisory — the human reviews and decides what to do.
MB may flag; it may not silently correct or assert identity.

Calls resolver.find_variants() for near-duplicate detection.
The D12 invariant requires that the same resolver powers /resolve.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd

from .config import NULL_PCT_WARN_THRESH, SINGLETON_MAX_COUNT
from .resolver import find_variants


def run_review(df: pd.DataFrame, mapping: List[Dict]) -> List[Dict]:
    """
    Run pre-ingest review against the mapped DataFrame.

    Steps:
      1. Translate rows to coordinate triples per the declared mapping.
      2. For each semantic_key, collect all distinct values.
      3. Run variant detection on values within the same semantic_key.
         (Values from different semantic_keys are never compared — coordinate
          namespace separation prevents WHAT.title tokens from matching WHO.artist.)
      4. Check null coverage per mapped field.
      5. Check for singleton values.
      6. Check nucleus stability (no stable shared ID heuristic).

    Returns list of flag dicts matching the flag taxonomy in the spec.
    """
    flags   = []
    flag_id = 0

    def next_id():
        nonlocal flag_id
        flag_id += 1
        return f"flag_{flag_id}"

    # Build { coord_key → [distinct values] } and null stats
    key_values: Dict[str, List[str]]        = {}
    null_stats: Dict[str, Tuple[int, int]]  = {}

    for row in mapping:
        col       = row["column"]
        dim       = row["dimension"]
        skey      = row["semantic_key"]
        coord_key = f"{dim}|{skey}"

        if col not in df.columns:
            continue

        series     = df[col].astype(str).str.strip()
        null_mask  = df[col].isna() | (series == '') | (series.str.lower() == 'nan')
        non_null   = series[~null_mask]
        null_count = int(null_mask.sum())
        total      = len(df)

        null_stats[coord_key] = (null_count, total)

        distinct = non_null.unique().tolist()
        key_values.setdefault(coord_key, []).extend(
            v for v in distinct if v not in key_values.get(coord_key, [])
        )

    # ── Variant candidates ───────────────────────────────────────────────────
    for coord_key, values in key_values.items():
        if len(values) < 2:
            continue
        candidates, engine = find_variants(values)
        for val_a, val_b, score in candidates:
            flags.append({
                "id":       next_id(),
                "type":     "variant_candidates",
                "severity": "warning",
                "message":  f"Possible duplicates in {coord_key}",
                "details": (
                    f'"{val_a}" and "{val_b}" share significant tokens '
                    f'(similarity {score:.0%}) and may refer to the same entity. '
                    f'Confirm whether these should map to one entity ID. '
                    f'(Detection engine: {engine})'
                ),
            })

    # ── Null coordinate coverage ─────────────────────────────────────────────
    for coord_key, (null_count, total) in null_stats.items():
        if total == 0:
            continue
        null_pct = null_count / total
        if null_pct > NULL_PCT_WARN_THRESH:
            flags.append({
                "id":       next_id(),
                "type":     "null_coordinates",
                "severity": "info",
                "message":  f"{null_pct:.0%} of rows have no value for {coord_key}",
                "details": (
                    f"{null_count} of {total} rows are null for this field. "
                    "Those entities will have incomplete dimension coverage in the substrate. "
                    "This may be expected — review before committing."
                ),
            })

    # ── Singleton values ─────────────────────────────────────────────────────
    for coord_key, values in key_values.items():
        for row in mapping:
            if f"{row['dimension']}|{row['semantic_key']}" == coord_key and row["column"] in df.columns:
                counts     = df[row["column"]].astype(str).str.strip().value_counts()
                singletons = [v for v, c in counts.items() if c <= SINGLETON_MAX_COUNT and v.strip()]
                if len(singletons) > 5:
                    flags.append({
                        "id":       next_id(),
                        "type":     "singleton_values",
                        "severity": "info",
                        "message":  f"{len(singletons)} values appear only once in {coord_key}",
                        "details": (
                            f"Examples: {', '.join(repr(s) for s in singletons[:5])}"
                            f"{'…' if len(singletons) > 5 else ''}. "
                            "Singletons may be data entry errors or genuinely unique values. "
                            "Review if this field is expected to have repeating values."
                        ),
                    })
                break

    # ── No stable shared ID heuristic ────────────────────────────────────────
    high_card_fields = [
        row["column"] for row in mapping
        if row["column"] in df.columns
        and len(df) > 0
        and df[row["column"]].nunique() / len(df) > 0.95
    ]

    if len(high_card_fields) == len(mapping) and len(mapping) > 0:
        flags.append({
            "id":       next_id(),
            "type":     "no_stable_id",
            "severity": "warning",
            "message":  "No obvious stable unique identifier found",
            "details": (
                "All mapped columns have high cardinality — no single column is clearly "
                "a stable ID. You may need a compound nucleus (combining two columns) "
                "to produce reliable entity IDs. Review in the next step."
            ),
        })

    return flags
