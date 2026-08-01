# The Sentence Test

**Status:** Working rule, previously unwritten.
**Scope:** Assigning a field to a dimension during Model Builder mapping.

---

## The rule

> **Say what you would ask about the field in an ordinary sentence. The
> interrogative you naturally reach for is its dimension.**

```
"Tell me about the liquor quantities."   → what are they      → WHAT
"Tell me about the delivery date."       → when did it happen → WHEN
"Tell me about the distributor."         → who supplied it    → WHO
```

If no interrogative fits naturally, that is information: either the field does
not belong in the model, or the sentence being asked is not the one a user
would ask.

---

## Why it works

The hub is journalistic before it is technical. 5W1H persists because a reader
can hold six questions without training — the frame was not designed for data
modelling and was not adapted to it. Nothing was added, renamed, or
specialised.

The consequence is that **the dimensions need no explanation but the procedure
for applying them does.** A stranger recognises WHO and WHEN immediately, then
has no method for deciding where a field goes, and falls back on taxonomy
reasoning — asking what *category* of thing the field is. That produces worse
mappings.

---

## The failure this prevents

The taxonomy reading treats WHAT as **identity** — *what kind of thing is
this*. Under that reading a quantity such as `unit_cost_usd` looks like a
category error: a cost is not what a thing *is*, so it appears to land in WHAT
only because nothing else will take it.

Under the sentence test it is a positive assignment, not a fallback. *"Tell me
about the unit cost"* → **what is it** → WHAT. Correct, and arrived at without
deliberation.

The dimension is **the interrogative the field answers**, not the category the
field belongs to. These diverge often enough to matter, and the taxonomy
reading is the more natural mistake for someone reasoning from a definition
rather than from speech.

---

## It also explains empty dimensions

A substrate with WHO and WHAT populated and WHEN, WHERE, WHY, HOW empty is not
deficient. **The questions do not arise for that data.** Nobody asks an
inventory snapshot when something happened, because it does not record one.

This is worth saying at first open. Four dashes read either as *this data has
no time or place* (correct) or as *those dimensions are decorative* (wrong).
Nothing on screen distinguishes them, and the rule supplies the distinction for
free.

---

## Where it belongs

1. **Model Builder, at the mapping step** — one line of helper text where the
   assignment is actually made. This is where a stranger currently has nothing.
2. **README / first-run text** — because it also accounts for empty dimensions.

---

## Provenance

Recorded August 1, 2026. It had been in continuous use during mapping and had
never been written down. Found the same way as
`navigation-runtime-seam.md`: by explaining it out loud to someone who had
applied the wrong rule and produced the predicted wrong objection.
