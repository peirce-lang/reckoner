"""
model_builder/resolver.py — Near-duplicate variant detection engine.

INVARIANT (D12): This is the normative implementation.
When /resolve is built for Crosswalk, it must use this module — not a
separate implementation. The engine must not be duplicated.

find_variants() is the public entry point.
Uses Tantivy if available, falls back to Jaccard similarity otherwise.
Callers receive (candidates, engine_name) so the engine is auditable.
"""

from __future__ import annotations

import re
from typing import List, Tuple

from .config import VARIANT_SCORE_THRESH

try:
    import tantivy
    TANTIVY_AVAILABLE = True
except ImportError:
    TANTIVY_AVAILABLE = False


def _find_variants_tantivy(values: List[str]) -> List[Tuple[str, str, float]]:
    """
    Find near-duplicate string pairs using Tantivy in-memory index.

    Builds a temporary index, indexes all values, then queries each value
    as a tokenized phrase and collects hits above VARIANT_SCORE_THRESH.

    Returns list of (value_a, value_b, score) for candidate pairs.
    Never modifies values. Never writes to disk. Read-only operation.
    """
    if len(values) < 2:
        return []

    schema_builder = tantivy.SchemaBuilder()
    schema_builder.add_text_field("value", stored=True)
    schema_builder.add_unsigned_field("idx", stored=True)
    schema = schema_builder.build()

    index  = tantivy.Index(schema, path=None)  # in-memory
    writer = index.writer(heap_size=15_000_000)

    for i, v in enumerate(values):
        doc = tantivy.Document()
        doc.add_text("value", str(v))
        doc.add_unsigned("idx", i)
        writer.add_document(doc)
    writer.commit()

    searcher   = index.searcher()
    candidates = []
    seen       = set()

    for i, query_val in enumerate(values):
        tokens = re.sub(r'[^\w\s]', ' ', str(query_val).lower()).split()
        if not tokens:
            continue

        query = index.parse_query(" ".join(tokens), ["value"])
        hits  = searcher.search(query, limit=10).hits

        for score, doc_address in hits:
            doc = searcher.doc(doc_address)
            j   = doc.get_first("idx")
            if j == i or score < VARIANT_SCORE_THRESH:
                continue
            pair_key = (min(i, j), max(i, j))
            if pair_key in seen:
                continue
            seen.add(pair_key)
            candidates.append((values[i], values[j], round(score, 3)))

    return candidates


def _find_variants_fallback(values: List[str]) -> List[Tuple[str, str, float]]:
    """
    Fallback variant detection without Tantivy.
    Uses token-overlap (Jaccard similarity) on normalized tokens.
    Catches obvious cases like '20,000 Leagues' vs '20000 Leagues'.
    Clearly marked in flag details so the caller knows this engine ran.
    """
    def normalize(s: str) -> set:
        return set(re.sub(r'[^\w\s]', ' ', s.lower()).split())

    candidates = []
    seen       = set()

    for i, a in enumerate(values):
        ta = normalize(a)
        if not ta:
            continue
        for j, b in enumerate(values):
            if j <= i:
                continue
            pair_key = (i, j)
            if pair_key in seen:
                continue
            tb = normalize(b)
            if not tb:
                continue
            intersection = len(ta & tb)
            union        = len(ta | tb)
            if union == 0:
                continue
            jaccard = intersection / union
            if jaccard >= VARIANT_SCORE_THRESH:
                seen.add(pair_key)
                candidates.append((a, b, round(jaccard, 3)))

    return candidates


def find_variants(values: List[str]) -> Tuple[List[Tuple[str, str, float]], str]:
    """
    Public entry point for variant detection.
    Uses Tantivy if available, falls back to Jaccard similarity otherwise.
    Returns (candidates, engine_name) — engine is auditable by the caller.
    """
    if TANTIVY_AVAILABLE:
        try:
            return _find_variants_tantivy(values), "tantivy"
        except Exception:
            pass
    return _find_variants_fallback(values), "fallback"
