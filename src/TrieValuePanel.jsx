// ─────────────────────────────────────────────────────────────────────────────
// TrieValuePanel.jsx
//
// Drop-in replacement for the ValuePanel component in Reckoner.
//
// What changed from ValuePanel:
//   - Enum fields now use trie-style narrowing instead of a flat loaded list
//   - Keystroke → debounced API call with search prefix
//   - Values show cardinality counts
//   - Near-duplicate detection flags visually similar values
//   - has_more indicator when results are truncated
//   - Multiple selection for OR within dimension (shift+click or checkbox)
//   - "Add X selected" button when multiple values chosen
//   - Text/number fields unchanged — same operator + input behavior
//
// Dependencies: same as existing frontend (React, framer-motion, lucide-react)
//
// Usage: replace <ValuePanel /> with <TrieValuePanel ... /> passing same props
// ─────────────────────────────────────────────────────────────────────────────

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { motion } from "framer-motion";
import { AlertTriangle, ChevronDown, Plus, Search, X } from "lucide-react";

const API_URL = import.meta.env.VITE_API_URL || "/api";
const DEBOUNCE_MS = 200;
const DEFAULT_LIMIT = 100;

// ─────────────────────────────────────────────────────────────────────────────
// Near-duplicate detection
//
// Two values are considered near-duplicates if they are very similar after
// normalizing whitespace, punctuation, and case.
// This is intentionally simple — it surfaces obvious data quality issues
// (Author vs author vs author:) without trying to be a fuzzy matcher.
// ─────────────────────────────────────────────────────────────────────────────

function normalizeForDupDetection(str) {
  return String(str)
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, "")
    .trim()
    .split(/\s+/)
    .sort()
    .join("");
}

function detectNearDuplicates(values) {
  // Returns a Set of value strings that have at least one near-duplicate
  const normalized = values.map(v => ({
    original:   v.value,
    normalized: normalizeForDupDetection(v.value),
  }));

  const dupSet = new Set();

  for (let i = 0; i < normalized.length; i++) {
    for (let j = i + 1; j < normalized.length; j++) {
      if (
        normalized[i].normalized === normalized[j].normalized &&
        normalized[i].original   !== normalized[j].original
      ) {
        dupSet.add(normalized[i].original);
        dupSet.add(normalized[j].original);
      }
    }
  }

  return dupSet;
}

// ─────────────────────────────────────────────────────────────────────────────
// Trie input — the narrowing search box
// ─────────────────────────────────────────────────────────────────────────────

function TrieSearch({ value, onChange, placeholder, isLoading }) {
  return (
    <div className="relative">
      <Search
        size={14}
        className="absolute left-3 top-1/2 -translate-y-1/2 opacity-40 pointer-events-none"
      />
      {isLoading && (
        <div className="absolute right-3 top-1/2 -translate-y-1/2 w-3 h-3 border border-current border-t-transparent rounded-full animate-spin opacity-40" />
      )}
      <input
        type="text"
        value={value}
        onChange={e => onChange(e.target.value)}
        placeholder={placeholder || "Type to narrow…"}
        className="w-full pl-8 pr-8 py-2 text-sm border border-gray-300 rounded-md
                   focus:outline-none focus:ring-2 focus:ring-blue-500
                   text-gray-900 bg-white"
        autoFocus
      />
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Value row — single selectable value with count and optional dup flag
// ─────────────────────────────────────────────────────────────────────────────

function ValueRow({ item, isSelected, isDuplicate, onToggle, searchTerm }) {
  // Highlight matching prefix in the value string
  const highlightPrefix = (str, prefix) => {
    if (!prefix || !str.toLowerCase().startsWith(prefix.toLowerCase())) {
      return <span>{str}</span>;
    }
    return (
      <>
        <span className="font-semibold">{str.slice(0, prefix.length)}</span>
        <span>{str.slice(prefix.length)}</span>
      </>
    );
  };

  return (
    <button
      onClick={() => onToggle(item.value)}
      className={`w-full text-left px-3 py-2 rounded-md text-sm transition-colors
                  flex items-center justify-between gap-2 group
                  ${isSelected
                    ? "bg-blue-600 text-white"
                    : "hover:bg-white/40 text-gray-900"
                  }`}
      title={isDuplicate ? "Near-duplicate values detected in this field" : item.value}
    >
      <div className="flex items-center gap-2 min-w-0">
        {/* Checkbox indicator */}
        <div className={`w-3.5 h-3.5 rounded border flex-shrink-0 flex items-center justify-center
                         ${isSelected
                           ? "bg-white border-white"
                           : "border-current opacity-30 group-hover:opacity-60"
                         }`}>
          {isSelected && (
            <svg width="8" height="8" viewBox="0 0 8 8">
              <path d="M1 4l2 2 4-4" stroke="#2563EB" strokeWidth="1.5" fill="none" strokeLinecap="round"/>
            </svg>
          )}
        </div>

        {/* Near-dup flag */}
        {isDuplicate && !isSelected && (
          <AlertTriangle
            size={12}
            className="text-amber-500 flex-shrink-0"
            title="Near-duplicate values detected"
          />
        )}

        {/* Value with prefix highlight */}
        <span className="truncate">
          {highlightPrefix(item.value, searchTerm)}
        </span>
      </div>

      {/* Count badge */}
      <span className={`text-xs flex-shrink-0 tabular-nums
                        ${isSelected ? "text-white/80" : "opacity-50"}`}>
        {item.count.toLocaleString()}
      </span>
    </button>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Main TrieValuePanel component
// ─────────────────────────────────────────────────────────────────────────────

export function TrieValuePanel({
  activeDrawer,
  activeField,
  activeFieldMeta,
  activeFieldType,
  schema,
  onAddConstraint,   // (field, value, dimension, op, value2?) => void
  onClose,
  getPanelClasses,   // (dimensionKey) => className string

  // For text/number/date fields — same as before
  selectedOp,
  setSelectedOp,
  opsForType,

  // Conditional cardinality — entity_ids from current result set, if any
  resultEntityIds,   // string[] | null
  onAggregateSearch, // (entityIds, label) => void — called when count filter search runs

  // Active constraints — for Narrow mode pre-query (I18)
  activeConstraints, // constraint[] | null — current constraint set before query runs

  // Controlled re-fetch signal — incremented by parent once per completed query.
  // Panels respond to this instead of watching resultEntityIds directly,
  // preventing a cascade of conditional discover calls on every result array rebuild.
  queryVersion,      // number
}) {
  if (!activeDrawer || !activeField || !activeFieldMeta) return null;

  const valueType = activeFieldType || "text";
  const isEnum = valueType === "enum" || valueType === "text";

  return (
    <motion.div
      initial={{ x: -50, opacity: 0 }}
      animate={{ x: 0,  opacity: 1 }}
      exit={  { x: -50, opacity: 0 }}
      transition={{ type: "tween", duration: 0.26, ease: [0.16, 1, 0.3, 1] }}
      className={`w-80 h-full p-3 border-l-4 ${getPanelClasses(activeDrawer)}`}
    >
      {/* Header */}
      <div className="flex justify-end mb-3">
        <button
          onClick={onClose}
          className="opacity-40 hover:opacity-100 transition-opacity"
          title="Close"
        >
          <X size={20} />
        </button>
      </div>

      <div className="flex items-center gap-2 mb-2">
        <div className="text-xs font-bold uppercase tracking-wide opacity-70">
          {activeDrawer}
        </div>
        <div className="text-xs opacity-70">/</div>
        <div className="text-sm font-bold truncate">{activeField}</div>
      </div>

      <div className="text-xs opacity-70 mb-3">
        {activeFieldMeta.fact_count?.toLocaleString?.() || activeFieldMeta.fact_count || 0} facts
        {activeFieldMeta.distinct_values != null && (
          <> • {activeFieldMeta.distinct_values.toLocaleString?.() || activeFieldMeta.distinct_values} distinct</>
        )}
        {" "}• {String(valueType).toUpperCase()}
      </div>

      {/* Enum: trie narrowing */}
      {isEnum ? (
        <TrieEnumPanel
          dimension={activeDrawer}
          field={activeField}
          schema={schema}
          onAddConstraint={onAddConstraint}
          onClose={onClose}
          resultEntityIds={resultEntityIds}
          onAggregateSearch={onAggregateSearch}
          activeConstraints={activeConstraints}
          queryVersion={queryVersion}
        />
      ) : (
        /* Text / number / date: same as before */
        <TextInputPanel
          activeDrawer={activeDrawer}
          activeField={activeField}
          activeFieldMeta={activeFieldMeta}
          valueType={valueType}
          selectedOp={selectedOp}
          setSelectedOp={setSelectedOp}
          opsForType={opsForType}
          onAddConstraint={onAddConstraint}
          onClose={onClose}
          schema={schema}
          resultEntityIds={resultEntityIds}
          queryVersion={queryVersion}
        />
      )}
    </motion.div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// TrieEnumPanel — the new trie narrowing experience for enum fields
// ─────────────────────────────────────────────────────────────────────────────

function TrieEnumPanel({ dimension, field, schema, onAddConstraint, onClose, resultEntityIds, onAggregateSearch, activeConstraints, queryVersion }) {
  const [searchTerm, setSearchTerm] = useState("");
  const [isLoading,  setIsLoading]  = useState(false);
  const [hasMore,    setHasMore]    = useState(false);
  const [selected,   setSelected]   = useState(new Set());
  const [error,      setError]      = useState(null);
  const [sortMode,   setSortMode]   = useState("freq");
  const [countMin,   setCountMin]   = useState("");
  const [countMax,   setCountMax]   = useState("");
  const debounceRef = useRef(null);

  const [allValues, setAllValues] = useState([]);
  const [isConditional, setIsConditional] = useState(false);

  // I18 — Narrow/Browse toggle
  // Narrow = values filtered to entities matching current constraints
  // Browse = full corpus values
  // Only relevant when constraints are active but no result set yet
  const hasConstraints = activeConstraints && activeConstraints.length > 0;
  const hasResultSet   = resultEntityIds && resultEntityIds.length > 0;
  const canNarrow      = hasConstraints || hasResultSet;
  const [narrowMode, setNarrowMode] = useState(true); // default Narrow when constraints active

  const fetchValues = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      let data;

      if (hasResultSet && narrowMode) {
        // Post-query narrow: counts within current result set (existing behavior)
        const resp = await fetch(`${API_URL}/discover/conditional`, {
          method:  "POST",
          headers: { "Content-Type": "application/json" },
          body:    JSON.stringify({
            expression: `${dimension}|${field}|*`,
            entity_ids: resultEntityIds,
            schema,
            limit: null,
          }),
        });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        data = await resp.json();
        setIsConditional(true);

      } else if (hasConstraints && narrowMode && !hasResultSet) {
        // Pre-query narrow: values for entities matching active constraints (I18)
        const resp = await fetch(`${API_URL}/discover/conditional`, {
          method:  "POST",
          headers: { "Content-Type": "application/json" },
          body:    JSON.stringify({
            expression:  `${dimension}|${field}|*`,
            constraints: activeConstraints.map(({ id, ...rest }) => rest),
            schema,
            limit: null,
          }),
        });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        data = await resp.json();
        setIsConditional(true);

      } else {
        // Browse: full corpus values
        const resp = await fetch(`${API_URL}/discover`, {
          method:  "POST",
          headers: { "Content-Type": "application/json" },
          body:    JSON.stringify({
            expression: `${dimension}|${field}|*`,
            schema,
            limit: null,
          }),
        });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        data = await resp.json();
        setIsConditional(false);
      }

      const rows   = Array.isArray(data.rows) ? data.rows : [];
      const mapped = rows.map(r => ({ value: r.value, count: r.count ?? r.entities ?? 0 }));
      setAllValues(mapped);
      setHasMore(false);
    } catch (err) {
      setError("Failed to load values.");
      setAllValues([]);
    } finally {
      setIsLoading(false);
    }
  // queryVersion is the controlled re-fetch signal from the parent.
  // resultEntityIds/activeConstraints are read inside via closure but not deps —
  // we don't want every entity ID array rebuild to trigger a refetch cascade.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dimension, field, schema, queryVersion, narrowMode]);

  useEffect(() => { fetchValues(); }, [fetchValues]);

  // Client-side search narrowing — no extra API calls needed
  const values = useMemo(() => {
    if (!searchTerm) return allValues;
    const lower = searchTerm.toLowerCase();
    return allValues.filter(v => v.value.toLowerCase().includes(lower));
  }, [allValues, searchTerm]);

  useEffect(() => {
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, []);

  const handleSearchChange = (term) => {
    setSearchTerm(term);
    setSelected(new Set());
    // No API call needed — filtering is client-side against allValues
  };

  const toggleSelected = (value) => {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(value)) next.delete(value);
      else next.add(value);
      return next;
    });
  };

  const addSelected = () => {
    for (const value of selected) onAddConstraint(field, value, dimension, "eq");
    setSelected(new Set()); // clear selection but stay open
  };

  const addSingle = (value) => {
    onAddConstraint(field, value, dimension, "eq");
    // Stay open — user can keep selecting values
  };

  // ── Near-duplicate detection ───────────────────────────────────────────────
  const dupSet = detectNearDuplicates(values);

  // ── Sort and filter based on mode ──────────────────────────────────────────
  const displayValues = useMemo(() => {
    let list = [...values];

    // Apply count filter if set
    const minN = countMin !== "" ? parseInt(countMin, 10) : null;
    const maxN = countMax !== "" ? parseInt(countMax, 10) : null;
    if (minN !== null && !isNaN(minN)) list = list.filter(v => v.count >= minN);
    if (maxN !== null && !isNaN(maxN)) list = list.filter(v => v.count <= maxN);

    if (sortMode === "alpha") {
      list.sort((a, b) => {
        const clean = s => String(s).replace(/[\s:;,./]+$/, "").toLowerCase();
        // For date-like values (contain digits and dashes in ISO pattern)
        // strip timestamps and sort chronologically
        const isDate = s => /^\d{4}-\d{2}-\d{2}/.test(String(s).trim());
        if (isDate(a.value) && isDate(b.value)) {
          const da = String(a.value).split(' ')[0];
          const db = String(b.value).split(' ')[0];
          return da < db ? -1 : da > db ? 1 : 0;
        }
        return clean(a.value).localeCompare(clean(b.value));
      });
    } else if (sortMode === "flagged") {
      // Only show values that have a near-duplicate
      list = list.filter(v => dupSet.has(v.value));
      // Sort flagged values alphabetically so variants sit adjacent
      list.sort((a, b) => {
        const clean = s => String(s).replace(/[\s:;,./]+$/, "").toLowerCase();
        return clean(a.value).localeCompare(clean(b.value));
      });
    }
    // freq mode: already sorted by count from API
    return list;
  }, [values, sortMode, dupSet, countMin, countMax]);

  const flaggedCount = dupSet.size;

  return (
    <div className="space-y-2">

      {/* 30i — Mode indicator + Narrow/Browse toggle */}
      {canNarrow ? (
        <div className="flex items-center justify-between px-2 py-1 bg-blue-50 border border-blue-200 rounded text-xs text-blue-700">
          <span>
            {narrowMode
              ? hasResultSet
                ? `Within your ${resultEntityIds.length.toLocaleString()} results`
                : `Matching your ${activeConstraints.length} constraint${activeConstraints.length !== 1 ? "s" : ""}`
              : "All values in corpus"
            }
          </span>
          <button
            onClick={() => setNarrowMode(v => !v)}
            className="ml-2 underline hover:no-underline flex-shrink-0"
          >
            {narrowMode ? "show all" : "narrow"}
          </button>
        </div>
      ) : (
        <div className="text-xs px-2 py-1 text-blue-600 opacity-60">
          All values in corpus
        </div>
      )}

      {/* Sort mode toggle */}
      <div className="flex items-center gap-1">
        <span className="text-xs opacity-50 mr-1">Sort:</span>
        {[
          { key: "freq",    label: "# freq" },
          { key: "alpha",   label: "A–Z" },
          { key: "flagged", label: `⚠ flagged${flaggedCount > 0 ? ` (${flaggedCount})` : ""}` },
        ].map(mode => (
          <button
            key={mode.key}
            onClick={() => setSortMode(mode.key)}
            className={`text-xs px-2 py-0.5 rounded transition-colors ${
              sortMode === mode.key
                ? "bg-current/20 font-semibold"
                : "opacity-50 hover:opacity-80"
            } ${mode.key === "flagged" && flaggedCount > 0 ? "text-amber-600" : ""}`}
          >
            {mode.label}
          </button>
        ))}
      </div>

      {/* Count filter — min/max range on cardinality */}
      <div className="flex items-center gap-1">
        <span className="text-xs opacity-50 flex-shrink-0">Count:</span>
        <input
          type="number"
          min="0"
          placeholder="min"
          value={countMin}
          onChange={e => setCountMin(e.target.value)}
          className="w-16 px-2 py-1 text-xs border border-gray-300 rounded text-gray-900 focus:outline-none focus:ring-1 focus:ring-blue-400"
        />
        <span className="text-xs opacity-40">–</span>
        <input
          type="number"
          min="0"
          placeholder="max"
          value={countMax}
          onChange={e => setCountMax(e.target.value)}
          className="w-16 px-2 py-1 text-xs border border-gray-300 rounded text-gray-900 focus:outline-none focus:ring-1 focus:ring-blue-400"
        />
        {(countMin !== "" || countMax !== "") && (
          <button
            onClick={() => { setCountMin(""); setCountMax(""); }}
            className="text-xs px-1.5 py-1 rounded border border-gray-200 text-gray-400 hover:text-gray-600 transition-colors"
            title="Clear count filter"
          >✕</button>
        )}
        {(countMin !== "" || countMax !== "") && displayValues.length > 0 && (
          <span className="text-xs opacity-50 ml-1">{displayValues.length} shown</span>
        )}
      </div>

      {/* Aggregate search — when count filter is active, offer direct entity search */}
      {(countMin !== "" || countMax !== "") && onAggregateSearch && (
        <button
          onClick={async () => {
            try {
              const resp = await fetch(`${API_URL}/aggregate`, {
                method:  "POST",
                headers: { "Content-Type": "application/json" },
                body:    JSON.stringify({
                  dimension:  dimension,
                  field:      field,
                  schema:     schema,
                  count_min:  countMin !== "" ? parseInt(countMin, 10) : null,
                  count_max:  countMax !== "" ? parseInt(countMax, 10) : null,
                }),
              });
              if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
              const data = await resp.json();
              const label = `${dimension}.${field} count ${countMin || ""}${countMin && countMax ? "–" : ""}${countMax || ""}`;
              onAggregateSearch(data.entity_ids || [], label, data);
            } catch (err) {
              console.error("Aggregate search failed:", err);
            }
          }}
          className="w-full text-xs px-3 py-2 rounded border border-indigo-300 bg-indigo-50 text-indigo-700 hover:bg-indigo-100 transition-colors font-semibold"
        >
          Search entities with count {countMin && countMax && countMin === countMax
            ? `= ${countMin}`
            : `${countMin ? "≥"+countMin : ""}${countMin && countMax ? " " : ""}${countMax ? "≤"+countMax : ""}`
          } →
        </button>
      )}

      {/* Trie search input + select all visible */}
      <div className="flex items-center gap-1">
        <div className="flex-1">
          <TrieSearch
            value={searchTerm}
            onChange={handleSearchChange}
            placeholder={`Search ${field}…`}
            isLoading={isLoading}
          />
        </div>
        {displayValues.length > 0 && (
          <button
            onClick={async () => {
              // If search term is active AND we have onAggregateSearch,
              // do a server-side CONTAINS search instead of adding OR constraints
              if (searchTerm && searchTerm.trim() && onAggregateSearch) {
                try {
                  const resp = await fetch(`${API_URL}/aggregate`, {
                    method:  "POST",
                    headers: { "Content-Type": "application/json" },
                    body:    JSON.stringify({
                      dimension:   dimension,
                      field:       field,
                      schema:      schema,
                      search_term: searchTerm.trim(),
                    }),
                  });
                  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
                  const data = await resp.json();
                  const label = `${dimension}.${field} contains "${searchTerm.trim()}"`;
                  onAggregateSearch(data.entity_ids || [], label, data);
                } catch (err) {
                  console.error("Aggregate search failed:", err);
                }
                return;
              }
              // No search term — select all visible as OR constraints (original behavior)
              const allVisible = new Set(displayValues.map(v => v.value));
              setSelected(prev => {
                const allAlreadySelected = displayValues.every(v => prev.has(v.value));
                if (allAlreadySelected) return new Set();
                return new Set([...prev, ...allVisible]);
              });
            }}
            className={`flex-shrink-0 text-xs px-2 py-2 rounded border transition-colors ${
              searchTerm && searchTerm.trim() && onAggregateSearch
                ? "border-indigo-300 bg-indigo-50 text-indigo-700 hover:bg-indigo-100"
                : "border-gray-300 bg-white text-gray-500 hover:border-gray-400 hover:text-gray-700"
            }`}
            title={searchTerm && searchTerm.trim()
              ? `Search all records where ${field} contains "${searchTerm}"`
              : `Select all ${displayValues.length} visible values`}
          >
            {searchTerm && searchTerm.trim() && onAggregateSearch
              ? `search "${searchTerm}"`
              : displayValues.every(v => selected.has(v.value)) ? '−all' : '+all'}
          </button>
        )}
      </div>

      {/* Near-dup notice — only show in freq/alpha mode, not flagged */}
      {dupSet.size > 0 && sortMode !== "flagged" && (
        <div className="flex items-start gap-2 px-2 py-1.5 bg-amber-50 border border-amber-200 rounded text-xs text-amber-800">
          <AlertTriangle size={12} className="flex-shrink-0 mt-0.5" />
          <span>
            Near-duplicate values detected.{" "}
            <button
              className="underline hover:no-underline"
              onClick={() => setSortMode("flagged")}
            >
              Show flagged only
            </button>
          </span>
        </div>
      )}

      {/* Flagged mode header */}
      {sortMode === "flagged" && (
        <div className="flex items-start gap-2 px-2 py-1.5 bg-amber-50 border border-amber-200 rounded text-xs text-amber-800">
          <AlertTriangle size={12} className="flex-shrink-0 mt-0.5" />
          <span>
            Showing {flaggedCount} flagged value{flaggedCount !== 1 ? "s" : ""} — variants sorted adjacent.{" "}
            <button
              className="underline hover:no-underline"
              onClick={() => setSortMode("freq")}
            >
              Show all
            </button>
          </span>
        </div>
      )}

      {/* Value list */}
      {error ? (
        <div className="text-sm text-red-700 px-2">{error}</div>
      ) : displayValues.length === 0 && !isLoading ? (
        <div className="text-sm opacity-60 italic px-2">
          {sortMode === "flagged"
            ? "No near-duplicate values detected in this field."
            : searchTerm
            ? `No values matching "${searchTerm}"`
            : "No values found"}
        </div>
      ) : (
        <div className="max-h-[52vh] overflow-auto space-y-0.5 pr-1">
          {displayValues.map((item, idx) => (
            <ValueRow
              key={`${item.value}-${idx}`}
              item={item}
              isSelected={selected.has(item.value)}
              isDuplicate={dupSet.has(item.value)}
              onToggle={(value) => {
                if (selected.size === 0 && !narrowMode) addSingle(value);
                else toggleSelected(value);
              }}
              searchTerm={searchTerm}
            />
          ))}
        </div>
      )}

      {/* has_more notice — not shown in flagged mode since it's a subset */}
      {hasMore && sortMode !== "flagged" && (
        <div className="text-xs opacity-50 px-2 text-center">
          Showing top {DEFAULT_LIMIT} — type to narrow further
        </div>
      )}

      {/* Multi-select action bar */}
      {selected.size > 0 && (
        <div className="pt-2 border-t border-current border-opacity-20 space-y-2">
          <div className="text-xs opacity-70">
            {selected.size} value{selected.size > 1 ? "s" : ""} selected
            {selected.size > 1 && " — will add as OR conditions"}
          </div>
          <div className="flex gap-2">
            <button
              onClick={addSelected}
              className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 bg-blue-600 text-white rounded-md text-sm hover:bg-blue-700 transition-colors"
            >
              <Plus size={14} />
              Add {selected.size} constraint{selected.size > 1 ? "s" : ""}
            </button>
            {/* ONLY — single value only; logically undefined for multiple values */}
            {selected.size === 1 && (
              <button
                onClick={() => {
                  const value = [...selected][0];
                  onAddConstraint(field, value, dimension, "only");
                  setSelected(new Set());
                }}
                className="flex items-center justify-center gap-1 px-3 py-2 border border-current border-opacity-30 rounded-md text-sm hover:bg-white/20 transition-colors"
                title={`Only "${[...selected][0]}" — exclude entities that also have other values for this field`}
              >
                Only
              </button>
            )}
            <button
              onClick={() => setSelected(new Set())}
              className="px-3 py-2 border border-current border-opacity-30 rounded-md text-sm hover:bg-white/20 transition-colors"
            >
              Clear
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// TextInputPanel — unchanged behavior for text/number/date fields
// Extracted from original ValuePanel for clarity
// ─────────────────────────────────────────────────────────────────────────────

const TEXT_OPS = [
  { key: "contains", label: "contains" },
  { key: "prefix",   label: "starts with" },
  { key: "eq",       label: "equals" },
];
const NUMBER_OPS = [
  { key: "eq",      label: "equals" },
  { key: "gt",      label: "greater than" },
  { key: "lt",      label: "less than" },
  { key: "between", label: "between" },
];
const DATE_OPS = [
  { key: "eq",      label: "on" },
  { key: "gt",      label: "after" },
  { key: "lt",      label: "before" },
  { key: "between", label: "between" },
];

function TextInputPanel({
  activeDrawer, activeField, valueType,
  selectedOp, setSelectedOp,
  onAddConstraint, onClose,
  schema,
  resultEntityIds,
  queryVersion,
}) {
  const [val,        setVal]        = useState("");
  const [val2,       setVal2]       = useState("");
  const [refValues,  setRefValues]  = useState([]);
  const [refFilter,  setRefFilter]  = useState("");
  const [refLoading, setRefLoading] = useState(false);

  const ops = valueType === "number" ? NUMBER_OPS
            : valueType === "date"   ? DATE_OPS
            : TEXT_OPS;

  const op     = selectedOp || ops[0].key;
  const canAdd = op === "between"
                 ? Boolean(val.trim()) && Boolean(val2.trim())
                 : Boolean(val.trim());

  // Load reference values for date/number fields so user knows what's available
  // Uses conditional endpoint when a result set exists — shows only values present
  // in the current result set, not global values.
  useEffect(() => {
    if (valueType !== "date" && valueType !== "number") return;
    setRefLoading(true);
    const hasResultSet = resultEntityIds && resultEntityIds.length > 0;
    const url     = hasResultSet ? `${API_URL}/discover/conditional` : `${API_URL}/discover`;
    const payload = hasResultSet
      ? { expression: `${activeDrawer}|${activeField}|*`, entity_ids: resultEntityIds, schema, limit: null }
      : { expression: `${activeDrawer}|${activeField}|*`, schema, limit: null };
    fetch(url, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify(payload),
    })
      .then(r => r.json())
      .then(d => setRefValues(Array.isArray(d.rows) ? d.rows : []))
      .catch(() => setRefValues([]))
      .finally(() => setRefLoading(false));
  }, [activeDrawer, activeField, valueType, schema, queryVersion]);

  // Filter reference values by what's been typed
  const filteredRef = refValues.filter(r =>
    !refFilter || String(r.value).includes(refFilter)
  );

  const submit = () => {
    if (!canAdd) return;
    if (op === "between") onAddConstraint(activeField, val.trim(), activeDrawer, op, val2.trim());
    else                  onAddConstraint(activeField, val.trim(), activeDrawer, op);
    setVal("");
    setVal2("");
  };

  return (
    <div className="space-y-3">
      <div className="space-y-2">
        <div className="text-xs opacity-70">Operator</div>
        <div className="relative">
          <select
            value={op}
            onChange={e => setSelectedOp(e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md
                       focus:outline-none focus:ring-2 focus:ring-blue-500
                       text-gray-900 appearance-none"
          >
            {ops.map(o => (
              <option key={o.key} value={o.key}>{o.label}</option>
            ))}
          </select>
          <ChevronDown size={16} className="absolute right-3 top-3 opacity-60 pointer-events-none" />
        </div>
      </div>

      <div className="space-y-2">
        <div className="flex items-center gap-2">
          <Search size={16} className="opacity-60" />
          <span className="text-xs opacity-70">
            {op === "between" ? "Enter two values" : "Enter a value"}
          </span>
        </div>

        <input
          type="text"
          placeholder={op === "between" ? `From…` : `Enter value for ${activeField}…`}
          value={val}
          onChange={e => { setVal(e.target.value); setRefFilter(e.target.value); }}
          onKeyDown={e => { if (e.key === "Enter") submit(); }}
          className="w-full px-3 py-2 border border-gray-300 rounded-md
                     focus:outline-none focus:ring-2 focus:ring-blue-500
                     text-gray-900"
          autoFocus
        />

        {op === "between" && (
          <input
            type="text"
            placeholder="To…"
            value={val2}
            onChange={e => { setVal2(e.target.value); setRefFilter(e.target.value); }}
            onKeyDown={e => { if (e.key === "Enter") submit(); }}
            className="w-full px-3 py-2 border border-gray-300 rounded-md
                       focus:outline-none focus:ring-2 focus:ring-blue-500
                       text-gray-900"
          />
        )}

        <button
          onClick={submit}
          disabled={!canAdd}
          className={`w-full px-3 py-2 rounded-md transition-colors ${
            canAdd
              ? "bg-blue-600 text-white hover:bg-blue-700"
              : "bg-gray-300 text-gray-600 cursor-not-allowed"
          }`}
        >
          Add constraint
        </button>
      </div>

      {/* Reference values — shown for date/number fields so user knows what's available */}
      {(valueType === "date" || valueType === "number") && (
        <div className="border-t border-current border-opacity-20 pt-2">
          <div className="text-xs opacity-50 mb-1.5">
            {refLoading ? "Loading values…" : `Available values${refFilter ? ` matching "${refFilter}"` : ""}`}
          </div>
          {filteredRef.length === 0 && !refLoading ? (
            <div className="text-xs opacity-40 italic">No matching values</div>
          ) : (
            <div className="max-h-40 overflow-auto space-y-0.5">
              {filteredRef.map((r, i) => (
                <button
                  key={i}
                  onClick={() => {
                    // Click to fill — first click fills "from", second fills "to"
                    if (!val) setVal(String(r.value));
                    else if (op === "between" && !val2) setVal2(String(r.value));
                    else setVal(String(r.value));
                    setRefFilter("");
                  }}
                  className="w-full text-left flex justify-between items-center
                             px-2 py-1 rounded text-xs hover:bg-white/30 transition-colors"
                >
                  <span>{r.value}</span>
                  <span className="opacity-40 tabular-nums">{r.entities}</span>
                </button>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
