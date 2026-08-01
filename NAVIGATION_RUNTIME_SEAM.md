# A Seam the Prototypes Revealed

**Status:** Architectural observation. Not a contract requirement.
**Origin:** The WHO/Tree Reckoner prototype, July 2026.
**Referenced by:** `session_handoff_july31_evening.md` §5.1 #5, §7.

---

## The principle

> **A navigation projection should not be the authoritative owner of
> semantic-role knowledge or runtime responsibilities.**

The qualifier is load-bearing. A projection will inevitably *use* semantic
roles — Tree cannot render without knowing what "group" means. The claim is
about origination, not consumption: a projection should not be the place those
meanings are defined. This leaves room for local overrides and experimentation
without weakening the principle.

---

## What the prototype revealed

The WHO Reckoner was written against a single music substrate, with three
hardwires: a trunk key, a ring axis, and no initial root. The obvious reading
was "too hardcoded — generalize it."

An endpoint inventory contradicted that reading. WHO is a **thin consumer** of
the Reckoner runtime. It already relies on Big R for substrate discovery,
execution, and conditional discovery. It duplicates far less infrastructure
than assumed.

The unresolved issue is not infrastructure. It is that WHO has to know three
things the runtime **cannot currently express**:

- which field **groups**
- which field **owns**
- which field **orders**

These are semantic facts about a substrate's fields. They exist nowhere in the
current architecture. Tree supplies them by hardcoding, because there is
nothing to ask.

---

## Why the claim shrank twice

The reasoning passed through three stages, and the shrinkage is the result:

**Stage 1 — "Tree is too hardcoded."**
A defect in one prototype. Fix: generalize it.

**Stage 2 — "Maybe all Reckoners should become plugins."**
An ecosystem claim: instruments as orbital extensions that swap sets with the
workbench. Larger, more speculative, and not evidenced by anything observed.

**Stage 3 — "Tree exposed semantic-role knowledge that has no home."**
One missing capability, directly observable, and independent of whether a
plugin ecosystem is ever built.

Stage 3 is the stronger result *because* it is smaller. Stages 1 and 2 are
recorded here so they are not re-derived: the plugin framing is not wrong, but
it is unevidenced, and it is not what the prototype demonstrated.

---

## Why this is not being designed now

A generalized capability vocabulary derived from **one** projection would
encode Tree's assumptions as though they were universal — the precise failure
the observation exists to prevent.

The right generalization belongs **after multiple projections exist**. Until a
second projection provides independent evidence about what a projection needs
to ask, there is nothing to design against.

**Do not refactor the WHO Reckoner to satisfy this note.** The note is the
preserved value; the prototype no longer has to carry the lesson itself.

---

## Distinction worth keeping

Two questions were conflated early and should stay separate:

| Question | Kind |
|---|---|
| How does a projection *receive* its configuration — derived from `/api/affordances`, or handed by the workbench? | **Transport** |
| What semantic facts (groups / owns / orders) exist nowhere in the architecture? | **Representation** |

The transport question is answerable today and does not matter yet. The
representation question is the actual gap and is not answerable yet.

---

## Related

- Portolan isolation (A-1): instruments are **views over** the routing
  substrate, never routers. The endpoint inventory suggests WHO already
  respects this; it is not evidence against the concern generally.
- Result-set identity model: if instruments ever exchange data, the thing
  crossing the boundary is a saved set or a constraint expression — existing
  primitives, not a new one. Recorded as a constraint on any future design, not
  as a design.
