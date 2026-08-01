"""
snf/address.py — the SNF address encoder.

    CanonicalFact ──► address bytes

This is the only place in the system where an address is constructed. Nothing
else may build one by string concatenation. See the Address Encoding Contract.

── The constitutional invariant ──────────────────────────────────────────────

  The CanonicalFact is the COMPLETE input to address derivation. No source
  schema, no declaration, no manifest, no substrate, and no environment may
  influence address generation.

  The encoder is deterministic: identical canonical inputs produce identical
  address bytes across operating systems, locales, processes, database
  engines, execution order, and time.

Do not add a parameter to encode_coordinate(). If it ever accepts a
declaration, two installations can produce different addresses from an
identical fact and grammar_version cannot detect it.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional


GRAMMAR_VERSION = "snf-coordinate-1"

DIMENSIONS = frozenset({"WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"})

DELIMITER = "|"

# Canonical semantic keys come in two namespaces.
#
#   USER    ^[a-z][a-z0-9_]*$     declared from a source mapping
#   SYSTEM  ^_[a-z][a-z0-9_]*$    produced by the pipeline itself
#
# The leading underscore is RESERVED, not merely permitted. Keys beginning with
# `_` mark assertions the system generated rather than read from a source —
# `_match_status`, `_composite_id` — and an ordinary source mapping may not
# declare one. Permitting the underscore everywhere would turn it into optional
# punctuation and lose the distinction that makes it worth having.
#
# The encoder accepts both, because both are canonical and it must compile any
# canonical fact. The restriction on who may DECLARE a system key belongs in
# Model Builder, where a human is present to answer for it. Putting it here
# would mean a parameter, and a parameter is how two installations end up
# producing different addresses from the same fact.
USER_SEMANTIC_KEY_PATTERN   = re.compile(r"^[a-z][a-z0-9_]*$")
SYSTEM_SEMANTIC_KEY_PATTERN = re.compile(r"^_[a-z][a-z0-9_]*$")
SEMANTIC_KEY_PATTERN        = re.compile(r"^_?[a-z][a-z0-9_]*$")


def is_system_key(key: str) -> bool:
    """True if this key is in the reserved system namespace."""
    return bool(SYSTEM_SEMANTIC_KEY_PATTERN.match(key))

VALUE_TYPES = frozenset({"boolean", "integer", "float", "decimal", "string", "date"})


class NonCanonicalFact(ValueError):
    """
    Raised when a fact violates the CADP/MB output boundary.

    This is a refusal, not a repair. Normalizing here instead of refusing is
    what allows a source label to leak into identity.
    """


@dataclass(frozen=True)
class CanonicalFact:
    dimension: str
    semantic_key: str
    value: Any
    value_type: str


def encode_coordinate(fact: CanonicalFact, grammar_version: str) -> Optional[str]:
    """
    Compile a canonical fact into an address.

    Returns the address string, or None when the fact is pruned (empty or
    NULL value). Pruning is not an error — a missing dimension is pruned
    early, not evaluated.

    Raises NonCanonicalFact when the input violates the boundary contract.
    """
    if grammar_version != GRAMMAR_VERSION:
        raise NonCanonicalFact(
            f"unsupported grammar version {grammar_version!r}; "
            f"this encoder implements {GRAMMAR_VERSION!r}"
        )

    dimension = _validate_dimension(fact.dimension)
    semantic_key = _validate_semantic_key(fact.semantic_key)

    rendered = _render_value(fact.value, fact.value_type)
    if rendered is None:
        return None                                  # pruned — A7 / A8

    if DELIMITER in rendered:
        raise NonCanonicalFact(
            f"value contains {DELIMITER!r}: {rendered!r}. Addresses have no "
            f"escaping mechanism by design. Map this value to a "
            f"delimiter-free canonical form in the manifest."
        )

    return f"{dimension}{DELIMITER}{semantic_key}{DELIMITER}{rendered}"


# ── validation ───────────────────────────────────────────────────────────────

def _validate_dimension(dimension: Any) -> str:
    if not isinstance(dimension, str) or dimension not in DIMENSIONS:
        raise NonCanonicalFact(
            f"dimension must be one of {sorted(DIMENSIONS)}, got {dimension!r}"
        )
    # The envelope carries WHO; the address carries who. The encoder folds.
    return dimension.lower()


def _validate_semantic_key(key: Any) -> str:
    if not isinstance(key, str):
        raise NonCanonicalFact(f"semantic_key must be a string, got {type(key).__name__}")
    if not SEMANTIC_KEY_PATTERN.match(key):
        raise NonCanonicalFact(
            f"semantic_key {key!r} is not canonical. Expected lowercase "
            f"letters, digits and underscores, starting with a letter (or an "
            f"underscore for reserved system keys). A source column label is "
            f"not a semantic key — declare one in the manifest and keep the "
            f"original in provenance."
        )
    return key


# ── value rendering ──────────────────────────────────────────────────────────

def _render_value(value: Any, value_type: Any) -> Optional[str]:
    """
    Render a typed value. Returns None if the fact should be pruned.

    Every branch checks the Python type against the declared type. A bare
    string where the declaration says float is not a canonical fact — it is
    an un-parsed source value, and accepting it would let parsing behaviour
    vary by caller.
    """
    if value_type not in VALUE_TYPES:
        raise NonCanonicalFact(
            f"value_type must be one of {sorted(VALUE_TYPES)}, got {value_type!r}"
        )

    if value is None:
        return None                                  # A8 — NULL emits no fact

    if value_type == "boolean":
        if not isinstance(value, bool):
            raise NonCanonicalFact(f"declared boolean, got {type(value).__name__}: {value!r}")
        return "true" if value else "false"          # V1 — settled with receipts

    if value_type == "integer":
        # bool is a subclass of int in Python; reject it explicitly so that
        # True cannot silently become the address `1`.
        if isinstance(value, bool) or not isinstance(value, int):
            raise NonCanonicalFact(f"declared integer, got {type(value).__name__}: {value!r}")
        return str(value)                            # V4 — no decimal point

    if value_type == "float":
        if isinstance(value, bool) or not isinstance(value, float):
            raise NonCanonicalFact(f"declared float, got {type(value).__name__}: {value!r}")
        if value != value or value in (float("inf"), float("-inf")):
            raise NonCanonicalFact(f"non-finite float is not addressable: {value!r}")
        # str() on a Python float is the shortest representation that
        # round-trips, and retains `.0` on integral values. V2 and V3 in one.
        return str(value)

    if value_type == "decimal":
        # V5 — declared scale is part of the canonical value. Trailing zeros
        # are meaningful here and are retained, unlike float.
        if isinstance(value, Decimal):
            return format(value, "f")
        if isinstance(value, str) and value.strip():
            return value.strip()
        raise NonCanonicalFact(f"declared decimal, got {type(value).__name__}: {value!r}")

    # string and date. Dates arrive pre-rendered by the WHEN fan-out; the
    # encoder does not parse or format dates, so no locale can reach it.
    if not isinstance(value, str):
        raise NonCanonicalFact(f"declared {value_type}, got {type(value).__name__}: {value!r}")

    text = unicodedata.normalize("NFC", value).strip()   # A5 then A9
    if text == "":
        return None                                       # A7 — empty prunes
    return text
