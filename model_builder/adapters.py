"""
model_builder/adapters.py — MB IR → legacy emitter adapter.

This file is the visible boundary between MB IR concepts and the legacy
emitter-facing shapes. It exists because:

  StructuralGroup  = canonical MB IR (WS-3 and forward)
  CorrelationGroup = compatibility shape consumed by compile_artifact()

Once the emitter is updated to consume StructuralGroup directly, this
adapter becomes trivial and can be inlined or deleted. Until then, it is
the only place CorrelationGroup is constructed from WS-3 input.

INVARIANT: No semantic meaning is added here. The adapter is a mechanical
shape translation. It does not name groups, infer types, or assert meaning.
group names are generated as 'group_001', 'group_002', etc. — opaque tokens.
"""

from __future__ import annotations

from typing import List

from .models import CorrelationGroup, CorrelationMember, StructuralGroup


def structural_to_correlation(
    groups: List[StructuralGroup],
) -> List[CorrelationGroup]:
    """
    Translate a list of StructuralGroups (MB IR) into CorrelationGroups
    (legacy emitter shape) for passing to compile_artifact().

    One StructuralGroup → one CorrelationGroup.
    StructuralGroup.id becomes CorrelationGroup.group (opaque token).
    StructuralGroup.members (column names) become CorrelationMember entries.

    The 'group' field on CorrelationGroup was previously used as a semantic
    label (e.g. 'ingredient_measure'). Under WS-3 it is an opaque structural
    ID only. Semantic labeling belongs to Crosswalk.
    """
    result: List[CorrelationGroup] = []

    for sg in groups:
        result.append(CorrelationGroup(
            group   = sg.id,                                      # opaque: 'group_001'
            members = [CorrelationMember(column=col) for col in sg.members],
        ))

    return result
