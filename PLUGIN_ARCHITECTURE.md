# Plugin Architecture: Design Principles

This document captures the conceptual architecture for plugins in the Reckoner / Model Builder / SNF ecosystem. It is not a specification — actual contract details will follow when implementation work begins. The purpose here is to record the principles that should govern the implementation, so that future work has the design intent in front of it.

## Two Separate Plugin Ecosystems

Reckoner and Model Builder each have their own plugin ecosystem. The two are conceptually distinct and should not be conflated.

**Model Builder plugins are translators.** They take source data in some external format — MARC, FHIR, COBOL copybook, OSI semantic models, BIBFRAME, OMOP, EAD, ISO 20022, HL7v2, EDI, LEDES — and produce SNF-coordinate-shaped data. Their job is bounded: read the source format, map its fields onto the six SNF primitives, hand off to Model Builder for the user to verify and adjust.

**Reckoner plugins are post-retrieval operations.** They take already-saved named sets as input and produce something — a refined set, a summary, an export, an analytic output. They do not handle source-format conversion. They do not issue queries. They operate on what Reckoner has already retrieved.

The two ecosystems serve different audiences. Translator authors are domain experts in formal data standards (catalogers who know MARC, healthcare informaticists who know FHIR, mainframe engineers who know COBOL copybook). Reckoner plugin authors are domain experts in operations their work requires (medical researchers who know cohort analysis, librarians who know authority control, legal analysts who know realization analysis).

Translator targets should be formal, stable, community-maintained vocabularies. Vendor-specific exports and firm-specific data shapes are not good translator targets — they belong to the Model Builder wizard for ad-hoc mapping rather than to translator plugins.

## Reckoner Plugin Rule: Saved Sets Only

The strict rule for Reckoner plugins: **plugins operate exclusively on already-saved sets. They do not issue queries.**

Querying is Reckoner's job. The drawer-and-chip workflow is the canonical interface for set construction. Plugins extend what users can do *with* sets they have already built; they do not extend or replace the construction process itself.

This rule is structural, not conventional. It protects the division of labor between core and plugin layers. Without it, plugin authors would inevitably build query interfaces inside their plugins, fragmenting the canonical UI into many domain-specific entry points and diluting the coherence of the system.

The technical capability exists for plugins to import `snf-peirce` and issue queries. The rule says they shouldn't, even though they could. The walls are architectural discipline backed by the plugin contract design, not enforced by removing the capability entirely.

A consequence: if a domain expert finds themselves wanting a plugin that issues queries, that's a signal that the canonical UI needs to support their pattern as a first-class feature. The right response is improving Reckoner, not relaxing the plugin rule.

## Two-Layer Plugin Model for Non-Programmers

Plugins are written by people who know Python and their domain. Plugins are *used* by everyone else in their domain.

The plugin author writes the operation's logic in Python and declares the parameters the operation needs. Parameters can include:

- Named set references
- Date and date-range inputs
- Number and threshold inputs
- Enumerated dropdown options
- Boolean toggles
- String inputs with validation

Reckoner's UI renders those parameters as form controls automatically. The user sees a form with the right input types for the operation, fills it in, and runs the plugin. The plugin's Python code receives validated, typed inputs and does its work.

This is the Winamp model. Domain experts who can't write Python download plugins, configure them through forms, and use them without coding. The plugin author's effort multiplies across the larger user population in their domain.

Parameter design should expose the natural axes of variation in the operation. A temporal cohort plugin should let users choose the time unit (hours, days, weeks, months, years), not bake in days as a default. Where parameters interact (e.g., the unit dropdown affects what the number input means), labels and validation should reflect the relationship.

Progressive disclosure is appropriate for plugins with many parameters. Primary parameters visible by default; secondary parameters available behind an expander. The plugin author marks each parameter's prominence; the framework renders accordingly.

## Substrate-Level Computation and Plugins Are Complementary

There are two layers where domain knowledge can encode itself into computed values:

**Substrate-level computation** pushes the math into the substrate. A computed value with a semantic name produces a queryable coordinate that anyone querying the substrate can filter on during query construction. The specific mechanism varies by substrate — Postgres views with computed columns, Pinot expressions and derived columns, SQL Server computed columns, DuckDB analytical functions — but the architectural role is the same. Each substrate can produce values like `WHAT|write_down_percentage|0.23`, and from SNF's perspective the coordinate means the same thing regardless of which substrate computed it.

**Reckoner plugins** do post-retrieval computation. The plugin reads base values from a result set and computes whatever is needed in Python. The computation happens after the user has narrowed to a specific set.

These are complementary, not competing.

Use substrate-level computation when the computed value should be queryable as a constraint. "Show me matters where write-down percentage is over 20%" needs the percentage to be a coordinate. Filtering on plugin-computed values would require retrieving everything first and filtering after, which doesn't scale. Substrate-level computation also benefits from the substrate's query optimization and indexing.

Use plugins when the computation is post-narrowing analysis, when it needs Python's data ecosystem (pandas, scipy, statistical libraries), or when the logic doesn't fit cleanly into the substrate's analytical capabilities. "I have my Q3 matters; show me the write-down distribution" doesn't need the percentage to be queryable; the user has already narrowed. Python doing the math after retrieval is fine.

Most domains will use both. The values users filter on routinely live in substrate-level computation. The values users compute occasionally as part of analysis, or that need Python-specific capabilities, stay as plugin work on base coordinates.

The substrate's specific compute capabilities affect what fits well there. Substrates with rich analytical features (Pinot, modern SQL engines) can do substantial math at the substrate layer. Substrates with simpler analytical features can do less. SNF inherits each substrate's strengths rather than flattening them — substrate-neutrality means SNF queries against whatever the substrate exposes, not lowest-common-denominator capability.

## Granular Base Coordinates

The substrate should expose base values granular enough to enable exploration and plugin flexibility, rather than only exposing pre-computed aggregates.

If the substrate exposes "billed amount" and "paid amount" as separate base coordinates, plugins (and substrate-level computations) can compute any derivative — write-down dollars, write-down percentage, payment shortfall, payment ratio. If the substrate only exposes pre-computed "write-down," the downstream layers are limited to what that aggregate supports.

This means substrate setup involves a granularity decision. The temptation is to expose only pre-computed values that match common queries (saves storage). The architectural argument is to expose granular base values and let computation happen at substrate or plugin layers as appropriate. Granular base coordinates support more derived computations and enable broader exploration.

## Labor Allocation: Substrate Specialist vs. Domain Expert

The choice between substrate-level computation and plugin is partly technical and partly organizational. It allocates labor and authority differently.

The substrate path puts the burden on the substrate specialist (DBA, data engineer, or whoever administers the substrate). They understand the data shape and the substrate's analytical capabilities, write the computed values, name them, handle indexing or partitioning decisions. Domain experts inherit this work — every domain expert in the organization gets the computed value as a queryable coordinate. The cost: every new computed value requires substrate-specialist time, and they become the bottleneck for what coordinates exist.

The plugin path puts the burden on the domain expert. They retrieve sets through Reckoner, run plugins that do the math on their pulled data. Autonomy without dependency on substrate-specialist time. The cost: plugin ecosystem maturity becomes the bottleneck instead. Domain experts who can't write Python depend on plugins existing for their needs.

Different organizations will allocate this differently. Big firms with responsive data engineering can lean on substrate-level computation. Smaller teams or domain groups without dedicated data engineering benefit from plugin-heavy workflows. The architecture supports both because both are legitimate.

## Exploration-to-Codification Maturation Cycle

The two layers create a natural maturation path for analytical knowledge.

A computation starts as plugin work — one analyst's exploration. If it's only useful for that one question, it stays in plugin form. If it turns out to be useful for many questions, it gets wrapped as a reusable plugin colleagues use. If it turns out to be a stable, broadly-used pattern that should be queryable as a constraint, it gets promoted to substrate-level computation.

This dynamic inverts the typical bottleneck. The substrate specialist isn't a gatekeeper deciding what gets built speculatively. They're a codifier turning successful exploration into stable infrastructure. The computed value they encode at the substrate layer represents something already proven valuable, not speculation about what might be useful.

This is the architectural equivalent of healthy cross-functional collaboration. Domain experts explore through plugins. Validated patterns surface to the substrate specialist. The specialist codifies them at the substrate layer. Each role does what it's best at. Tacit analytical knowledge becomes explicit institutional infrastructure over time.

The implication for adoption: organizations don't need significant data-engineering investment upfront. The substrate exposes base coordinates initially. As Reckoner gets used, valuable patterns emerge through plugin work. Substrate-level codification scales with demonstrated value rather than speculative anticipation.

## What Each Layer Produces for Downstream BI

The two layers produce different kinds of artifacts when results leave Reckoner.

Substrate-derived values are stable, named, source-embedded. The computation lives in the substrate where it can't drift between query executions. The semantic name and the computation are bound together. This is what supports live BI dashboards — the dashboard refreshes against current data with the computation defined once at the substrate layer.

Plugin-derived values are situational snapshots. The plugin computed something on a specific set during a specific analytical session. Exporting the result to BI gives you the values, but the computation that produced them is implicit, not part of shared infrastructure. This is appropriate for ad-hoc analysis where the result isn't meant to become institutional infrastructure.

Both feed BI tools, but for different purposes. Stable institutional reporting (executive dashboards, regulatory reporting, operational metrics) uses substrate-level computation. Ad-hoc investigation that needs visualization or further analysis uses plugin exports.

## The Swiss Army Knife Framing

The canonical Reckoner is the body of the Swiss army knife — drawer-and-chip workflow, dimensional scaffolding, lens architecture, substrate-neutrality, planner, set operations. These define what Reckoner *is*.

Plugins are the configurable tools — the corkscrew, the awl, the scissors. Different domains carry different blade configurations. A medical researcher's Reckoner has cohort-analysis plugins. A librarian's has authority-control plugins. A legal analyst's has reconciliation plugins. The canonical version is the same; the plugin set varies.

The body has primacy. Adding or removing a particular plugin doesn't change what Reckoner is. Adding or removing the lens architecture would. The plugin layer is configuration; the core is identity.

This means the canonical version can stay relaxed about feature scope. Bloat is prevented by keeping specialized operations as plugins. Identity is preserved by making the architectural commitments structural rather than feature-list-based. Different professionals carry different blade configurations of the same recognizable knife.

## Plugin Discovery and Distribution

Plugins should be installable through standard Python mechanisms (pip-installable packages with declared entry points that Reckoner discovers automatically). The user-facing flow should be effortless — paste a package name, click install, plugin appears in the operations menu.

Discovery is largely social. Communities will share plugins through their own channels — domain mailing lists, conferences, recommendations from colleagues. The architecture's job is making plugins shareable and installable; the actual sharing happens in communities the architecture doesn't manage.

Plugin documentation should serve both audiences: author documentation for those extending or maintaining plugins, user documentation in domain vocabulary for those configuring plugins through the parameter UI. User documentation should not require programming knowledge to read.

## Plugin Composition

All plugins consume named sets and produce named sets (or terminal artifacts like exports/visualizations). This shared interface lets plugins compose: the output of one plugin can be the input to another. A user might use four plugins in sequence to do their actual analysis. Each plugin does one thing; composition handles complexity.

Plugins should not call each other directly. They communicate through the named-set abstraction at the core layer. This keeps the plugin ecosystem decoupled — installing or uninstalling one plugin doesn't break others.

## Summary

Two plugin ecosystems, separate by audience and by what they extend:

- **Model Builder plugins (translators)**: bring source data into SNF. Target formal, stable, community-maintained vocabularies. Author audience: format specialists.
- **Reckoner plugins (post-retrieval operations)**: do useful things with already-saved sets. Open-ended operation space. Author audience: domain operation specialists.

Reckoner plugin rule: saved sets only. No queries.

Two-layer plugin model: Python author + parameter declaration → form-driven UI for non-Python users.

Two computation layers for different needs:

- **Substrate-level computation** for queryable computed coordinates that should feed stable dashboards. Mechanism varies by substrate (Postgres views, Pinot expressions, SQL Server computed columns, DuckDB analytical functions); architectural role is the same.
- **Plugins** for post-retrieval computation in Python on retrieved sets, especially when Python's data ecosystem or domain-specific logic is needed.
- Migration from plugin to substrate-level computation as patterns mature and prove valuable.

Granular base coordinates in the substrate enable both flexible plugins and richer substrate-level computation possibilities later.

Labor allocation between substrate specialist and domain expert is the underlying choice; the architecture supports both paths and a healthy collaboration pattern between them.

The canonical Reckoner is identity; plugins are configuration. The body of the knife stays focused; the blade set varies by domain.
