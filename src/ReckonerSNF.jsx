import React, { useEffect, useMemo, useState, useCallback, startTransition } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  Table, X, Calendar, Scale, Users, FileText,
  HelpCircle, MapPin, Settings, Search, ChevronDown,
  Bookmark, FolderOpen, Save, Trash2, GitMerge,
} from "lucide-react";
import { TrieValuePanel } from "./TrieValuePanel";
import ResultCard from "./ResultCard";

const API_URL = import.meta.env.VITE_API_URL || "/api";
const SAVED_QUERIES_KEY  = "reckoner_saved_queries";
const SAVED_SETS_KEY     = "reckoner_saved_sets";
const HEADER_PREFS_KEY   = "reckoner_header_prefs"; // prefix — keyed by substrate id

// ─────────────────────────────────────────────────────────────────────────────
// Peirce serializer (inlined — no require() in JSX)
// ─────────────────────────────────────────────────────────────────────────────

const OP_TO_PEIRCE = {
  eq: "=", not_eq: "!=", gt: ">", lt: "<",
  gte: ">=", lte: "<=", contains: "CONTAINS", prefix: "PREFIX",
};

function serializeValue(value) {
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return String(value);
  return `"${String(value).replace(/"/g, '\\"')}"`;
}

function toPeirce(constraints) {
  if (!constraints || constraints.length === 0) return "";
  const parts = constraints.map((c) => {
    const dim   = (c.category || c.dimension || "").toUpperCase();
    const field = (c.field || "").toLowerCase();
    if (!dim || !field) return null;
    let expr;
    if (c.op === "between") {
      expr = `${dim}.${field} BETWEEN ${serializeValue(c.value)} AND ${serializeValue(c.value2 ?? c.value)}`;
    } else if (c.op === "only") {
      expr = `${dim}.${field} ONLY ${serializeValue(c.value)}`;
    } else {
      const op = OP_TO_PEIRCE[c.op] || "=";
      expr = `${dim}.${field} ${op} ${serializeValue(c.value)}`;
    }
    if (c.negated) expr = `NOT ${expr}`;
    return expr;
  }).filter(Boolean);
  return parts.join("\nAND ");
}

// ─────────────────────────────────────────────────────────────────────────────
// Saved queries — localStorage persistence
// ─────────────────────────────────────────────────────────────────────────────

function loadSavedSets() {
  try {
    const raw = localStorage.getItem(SAVED_SETS_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch { return []; }
}

function persistSavedSets(sets) {
  try {
    localStorage.setItem(SAVED_SETS_KEY, JSON.stringify(sets));
  } catch (e) {
    console.error("Could not persist saved sets:", e);
  }
}

function loadSavedQueries() {
  try {
    const raw = localStorage.getItem(SAVED_QUERIES_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch { return []; }
}

function persistSavedQueries(queries) {
  try {
    localStorage.setItem(SAVED_QUERIES_KEY, JSON.stringify(queries));
  } catch (e) {
    console.error("Could not persist saved queries:", e);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Header prefs — primary/secondary label overrides per substrate
// Fallback chain: lens-level → substrate-level → null (use default in ResultCard)
// Stored shape: [{ dim: 'WHO', alwaysVisible: true }, ...]
// ─────────────────────────────────────────────────────────────────────────────

// Header prefs — { primary: {dim, field} | null, secondary: {dim, field} | null }
// Stored per substrate. null means use automatic logic in extractPrimaryLabel.
function loadHeaderPrefs(substrateId) {
  try {
    const raw = localStorage.getItem(`${HEADER_PREFS_KEY}::${substrateId}`);
    return raw ? JSON.parse(raw) : { primary: null, secondary: null };
  } catch { return { primary: null, secondary: null }; }
}

function saveHeaderPrefs(prefs, substrateId) {
  try {
    localStorage.setItem(`${HEADER_PREFS_KEY}::${substrateId}`, JSON.stringify(prefs));
  } catch (e) {
    console.error("Could not persist header prefs:", e);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 5W1H Query Summary helpers
// ─────────────────────────────────────────────────────────────────────────────

const DIM_ACCENT = {
  WHO:   "#3b82f6", WHAT:  "#8b5cf6", WHEN:  "#22c55e",
  WHERE: "#f59e0b", WHY:   "#f43f5e", HOW:   "#64748b",
};
const DIM_ACCENT_LIGHT = {
  WHO:   "#eff6ff", WHAT:  "#f5f3ff", WHEN:  "#f0fdf4",
  WHERE: "#fffbeb", WHY:   "#fff1f2", HOW:   "#f8fafc",
};
const DIM_ACCENT_TEXT = {
  WHO:   "#1d4ed8", WHAT:  "#6d28d9", WHEN:  "#15803d",
  WHERE: "#b45309", WHY:   "#be123c", HOW:   "#475569",
};

function humanizeField(field) {
  return String(field).replace(/_/g, " ");
}

// How many chips before a dimension row auto-collapses
const COLLAPSE_THRESHOLD = 3;

function chipLabel(c) {
  return c.op === "between"  ? `${c.value} – ${c.value2}`
       : c.op === "contains" ? `~ ${c.value}`
       : c.op === "gt"       ? `> ${c.value}`
       : c.op === "lt"       ? `< ${c.value}`
       : c.op === "gte"      ? `≥ ${c.value}`
       : c.op === "lte"      ? `≤ ${c.value}`
       : c.op === "not_eq"   ? `≠ ${c.value}`
       : c.op === "only"     ? `only "${c.value}"`
       : String(c.value);
}

function DimRow({ dimKey, dimConstraints, onRemove }) {
  const totalChips = Object.values(dimConstraints).flat().length;
  const autoCollapse = totalChips > COLLAPSE_THRESHOLD;
  const [collapsed, setCollapsed] = useState(autoCollapse);

  // If threshold changes (constraints added/removed) keep in sync
  useEffect(() => {
    setCollapsed(totalChips > COLLAPSE_THRESHOLD);
  }, [totalChips]);

  return (
    <div style={{ display: "flex", alignItems: "flex-start", minHeight: 24, marginBottom: 2 }}>
      {/* Dimension label — clickable to expand/collapse if over threshold */}
      <div
        onClick={() => autoCollapse || totalChips > COLLAPSE_THRESHOLD ? setCollapsed(v => !v) : null}
        style={{
          width: 56, flexShrink: 0, fontSize: 11, fontWeight: 700,
          letterSpacing: "0.05em", textTransform: "uppercase",
          color: DIM_ACCENT[dimKey],
          paddingTop: 3, userSelect: "none",
          cursor: totalChips > COLLAPSE_THRESHOLD ? "pointer" : "default",
        }}
      >
        {dimKey}
      </div>

      {/* Chips or collapsed summary */}
      <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 4, flex: 1, paddingTop: 2 }}>
        {collapsed ? (
          // Collapsed — show count badge, click to expand
          <span
            onClick={() => setCollapsed(false)}
            style={{
              background: DIM_ACCENT_LIGHT[dimKey] || "#f9fafb",
              color: DIM_ACCENT_TEXT[dimKey] || "#374151",
              borderLeft: `2px solid ${DIM_ACCENT[dimKey] || "#6b7280"}`,
              padding: "1px 8px 1px 6px",
              borderRadius: "2px",
              fontSize: 11,
              cursor: "pointer",
              userSelect: "none",
            }}
          >
            {totalChips} values ▸
          </span>
        ) : (
          // Expanded — show all chips
          Object.entries(dimConstraints).map(([field, fieldConstraints], fi) => (
            <span key={field} style={{ display: "inline-flex", alignItems: "center", flexWrap: "wrap", gap: 3 }}>
              {fi > 0 && (
                <span style={{ color: "#9ca3af", fontSize: 10, padding: "0 4px", fontWeight: 500 }}>AND</span>
              )}
              <span style={{ color: "#9ca3af", fontSize: 11, marginRight: 3 }}>
                {humanizeField(field)}
              </span>
              {fieldConstraints.map((c, ci) => (
                <span key={c.id} style={{ display: "inline-flex", alignItems: "center", gap: 3 }}>
                  {ci > 0 && (
                    <span style={{ color: "#9ca3af", fontSize: 10, padding: "0 3px", fontStyle: "italic" }}>or</span>
                  )}
                  <span style={{
                    background: DIM_ACCENT_LIGHT[c.category] || "#f9fafb",
                    color: DIM_ACCENT_TEXT[c.category] || "#374151",
                    borderLeft: `2px solid ${DIM_ACCENT[c.category] || "#6b7280"}`,
                    padding: "1px 7px 1px 5px",
                    borderRadius: "2px",
                    fontSize: 12,
                    display: "inline-flex", alignItems: "center", gap: 5,
                  }}>
                    {chipLabel(c)}
                    <button
                      onClick={() => onRemove(c.id)}
                      style={{ opacity: 0.4, cursor: "pointer", background: "none", border: "none", padding: 0, lineHeight: 1, color: "inherit" }}
                      onMouseEnter={e => e.target.style.opacity = 1}
                      onMouseLeave={e => e.target.style.opacity = 0.4}
                      title="Remove"
                    >✕</button>
                  </span>
                </span>
              ))}
              {/* Collapse toggle when expanded and over threshold */}
              {fi === Object.entries(dimConstraints).length - 1 && totalChips > COLLAPSE_THRESHOLD && (
                <span
                  onClick={() => setCollapsed(true)}
                  style={{ color: "#9ca3af", fontSize: 10, cursor: "pointer", paddingLeft: 4, userSelect: "none" }}
                >▴</span>
              )}
            </span>
          ))
        )}
      </div>
    </div>
  );
}

function QuerySummary({ constraints, DIMENSIONS, onRemove }) {
  if (!constraints || constraints.length === 0) return null;

  // Group constraints: { DIM: { field: [constraint, ...] } }
  const grouped = {};
  for (const c of constraints) {
    const dim = c.category;
    if (!grouped[dim]) grouped[dim] = {};
    if (!grouped[dim][c.field]) grouped[dim][c.field] = [];
    grouped[dim][c.field].push(c);
  }

  const hasAny = Object.keys(grouped).length > 0;
  if (!hasAny) return null;

  return (
    <div style={{ marginTop: 12 }}>
      {DIMENSIONS.map((d) => {
        const dimConstraints = grouped[d.key];
        if (!dimConstraints) return (
          <div key={d.key} style={{ display: "flex", alignItems: "flex-start", minHeight: 24, marginBottom: 2 }}>
            <div style={{
              width: 56, flexShrink: 0, fontSize: 11, fontWeight: 700,
              letterSpacing: "0.05em", textTransform: "uppercase",
              color: "#d1d5db", paddingTop: 3, userSelect: "none",
            }}>{d.key}</div>
            <span style={{ color: "#d1d5db", fontStyle: "italic", fontSize: 11, paddingTop: 3 }}>—</span>
          </div>
        );
        return (
          <DimRow key={d.key} dimKey={d.key} dimConstraints={dimConstraints} onRemove={onRemove} />
        );
      })}
    </div>
  );
}



const DEFAULT_OP_BY_TYPE = { enum: "eq", text: "contains", number: "eq", date: "eq" };

const TEXT_OPS    = [{ key: "contains", label: "contains" }, { key: "prefix", label: "starts with" }, { key: "eq", label: "equals" }];
const NUMBER_OPS  = [{ key: "eq", label: "equals" }, { key: "gt", label: "greater than" }, { key: "lt", label: "less than" }, { key: "between", label: "between" }];
const DATE_OPS    = [{ key: "eq", label: "on" }, { key: "gt", label: "after" }, { key: "lt", label: "before" }, { key: "between", label: "between" }];

function safeLower(s) { return String(s ?? "").toLowerCase(); }
function clampStr(s, n = 80) { const t = String(s ?? ""); return t.length > n ? t.slice(0, n - 1) + "…" : t; }

// ─────────────────────────────────────────────────────────────────────────────
// Component
// ─────────────────────────────────────────────────────────────────────────────

export default function ReckonerSNF() {
  const [constraints, setConstraints]   = useState([]);
  const [activeDrawer, setActiveDrawer] = useState(null);
  const [activeField, setActiveField]   = useState(null);
  const [affordances, setAffordances]   = useState(null);
  const [valuesCache, setValuesCache]   = useState({});
  const [valuesLoading, setValuesLoading] = useState(false);
  const [valuesError, setValuesError]   = useState(null);
  const [showResults, setShowResults]   = useState(false);
  const [results, setResults]           = useState([]);
  const [selectedIds, setSelectedIds]   = useState(new Set());
  const [loading, setLoading]           = useState(false);
  const [queryStats, setQueryStats]     = useState(null); // { probe_ms, execution_ms, total_ms, row_count, trace }
  const [queryVersion, setQueryVersion] = useState(0);    // incremented once per completed query — controls TrieValuePanel refetch
  const [pageOffset, setPageOffset]     = useState(0);    // current pagination offset
  const [searchTerm, setSearchTerm]     = useState("");
  const [searchTerm2, setSearchTerm2]   = useState("");
  const [selectedOp, setSelectedOp]     = useState(null);
  const [apiStatus, setApiStatus]       = useState(null);

  // ── Projection — field picker ─────────────────────────────────────────────
  // projectedFields: Set of field names to show. Empty = show all (default).
  const [projectedFields, setProjectedFields] = useState(new Set());
  const [showFieldPicker, setShowFieldPicker] = useState(false);

  // ── Header prefs — { primary: {dim,field}|null, secondary: {dim,field}|null }
  const [headerPrefs, setHeaderPrefs] = useState({ primary: null, secondary: null });


  // ── Sort ──────────────────────────────────────────────────────────────────
  // sortField: field name to sort by, or null (no sort = result order).
  // sortDir:   'asc' | 'desc'
  const [sortField, setSortField] = useState(null);
  const [sortDir,   setSortDir]   = useState('asc');

  // ── Group by ───────────────────────────────────────────────────────────────
  // groupByField: field name to group results by, or null (no grouping).
  // groupExpanded: Set of group keys that are expanded in the UI.
  const [groupByField,  setGroupByField]  = useState(null);
  const [groupExpanded, setGroupExpanded] = useState(new Set());

  // ── Schema selector ───────────────────────────────────────────────────────
  const [activeSchema, setActiveSchema]   = useState("");

  // Pin a field as primary (title) or secondary (subtitle) for this substrate.
  // Persists to localStorage. Toggling the same field again clears the pin.
  const handlePinHeader = useCallback((role, dim, field) => {
    setHeaderPrefs(prev => {
      const existing = prev[role];
      const isAlreadyPinned = existing?.dim === dim && existing?.field === field;
      const next = { ...prev, [role]: isAlreadyPinned ? null : { dim, field } };
      saveHeaderPrefs(next, activeSchema);
      return next;
    });
  }, [activeSchema]);
  const [schemas, setSchemas]             = useState([]);

  useEffect(() => {
    fetch(`${API_URL}/schemas`)
      .then((r) => r.json())
      .then((d) => {
        const list = Array.isArray(d.schemas) ? d.schemas : [];
        setSchemas(list);
        // Set active schema to first available if not already set
        if (list.length > 0 && !activeSchema) {
          setActiveSchema(list[0].schema);
        }
      })
      .catch(() => {});
  }, []);

  const switchSchema = (schema) => {
    if (schema === activeSchema) return;
    setActiveSchema(schema);
    setConstraints([]);
    setResults([]);
    setShowResults(false);
    setValuesCache({});
    setAffordances(null);
    setActiveDrawer(null);
    setActiveField(null);
    setProjectedFields(new Set());
    setShowFieldPicker(false);
    setSortField(null);
    setSortDir('asc');
    setHeaderPrefs(loadHeaderPrefs(schema));
    fetch(`${API_URL}/health?schema=${schema}`)
      .then(r => r.json()).then(setApiStatus).catch(() => {});
    fetch(`${API_URL}/affordances?schema=${schema}`)
      .then((r) => r.json())
      .then((data) => {
        setAffordances(data);
        const firstDim = DIMENSIONS.map((d) => d.key).find((k) => data?.[k] && Object.keys(data[k]).length > 0) || null;
        if (firstDim) { setActiveDrawer(firstDim); setActiveField(Object.keys(data[firstDim] || {})[0] || null); }
      })
      .catch(() => {});
  };

  // ── P5: Peirce display toggle ──────────────────────────────────────────────
  const [showPeirce, setShowPeirce] = useState(false);

  // ── P7: Save / load state ──────────────────────────────────────────────────
  const [savedQueries, setSavedQueries] = useState(() => loadSavedQueries());
  const [showSaveDialog, setShowSaveDialog] = useState(false);
  const [showLoadPanel, setShowLoadPanel]   = useState(false);
  const [saveNameInput, setSaveNameInput]   = useState("");

  // Persist whenever savedQueries changes
  useEffect(() => { persistSavedQueries(savedQueries); }, [savedQueries]);

  // ── Phase 2: Named result sets ────────────────────────────────────────────
  const [savedSets, setSavedSets]             = useState(() => loadSavedSets());
  const [showDiffPanel, setShowDiffPanel]     = useState(false);
  const [diffSetA, setDiffSetA]               = useState(null);
  const [diffSetB, setDiffSetB]               = useState(null);
  const [diffResult, setDiffResult]           = useState(null); // { onlyA, onlyB, both }
  const [diffInspectLabel, setDiffInspectLabel] = useState(null); // label for inspected group
  const [setOperation, setSetOperation]         = useState('diff'); // 'diff' | 'union' | 'intersect'
  const [setOpResult, setSetOpResult]           = useState(null);  // { ids, label, operation }
  const [showSaveSetDialog, setShowSaveSetDialog] = useState(false);
  const [saveSetNameInput, setSaveSetNameInput]   = useState("");

  // Persist whenever savedSets changes
  useEffect(() => { persistSavedSets(savedSets); }, [savedSets]);

  // Current Peirce string — live derived from constraints
  const currentPeirce = useMemo(() => toPeirce(constraints), [constraints]);

  const DIMENSIONS = useMemo(() => [
    { key: "WHO",   icon: Users,       color: "blue"   },
    { key: "WHAT",  icon: Scale,       color: "purple" },
    { key: "WHEN",  icon: Calendar,    color: "green"  },
    { key: "WHERE", icon: MapPin,      color: "amber"  },
    { key: "WHY",   icon: HelpCircle,  color: "rose"   },
    { key: "HOW",   icon: Settings,    color: "slate"  },
  ], []);

  const dimensionMeta = useMemo(() => {
    const m = {}; DIMENSIONS.forEach((d) => (m[d.key] = d)); return m;
  }, [DIMENSIONS]);

  useEffect(() => { checkApiHealth(); loadAffordances(); }, []);

  // Re-check health whenever activeSchema changes so statistics.total_entities
  // is populated with the correct substrate's entity count
  useEffect(() => { if (activeSchema) checkApiHealth(); }, [activeSchema]);

  const checkApiHealth = async () => {
    try { const r = await fetch(`${API_URL}/health?schema=${activeSchema}`); setApiStatus(await r.json()); }
    catch { setApiStatus({ status: "error", error: "Cannot connect to API" }); }
  };

  const loadAffordances = async (schema = activeSchema) => {
    try {
      const r    = await fetch(`${API_URL}/affordances?schema=${schema}`);
      const data = await r.json();
      setAffordances(data);
      const firstDim = DIMENSIONS.map((d) => d.key).find((k) => data?.[k] && Object.keys(data[k]).length > 0) || null;
      if (firstDim) { setActiveDrawer(firstDim); setActiveField(Object.keys(data[firstDim] || {})[0] || null); }
      else { setActiveDrawer(null); setActiveField(null); }
    } catch { setAffordances(null); setActiveDrawer(null); setActiveField(null); }
  };

  const getDrawerClasses = (k) => {
    const c = dimensionMeta[k]?.color || "gray";
    return ({ blue: "bg-blue-800 border-blue-600", purple: "bg-purple-800 border-purple-600", green: "bg-green-800 border-green-600", amber: "bg-amber-800 border-amber-600", rose: "bg-rose-800 border-rose-600", slate: "bg-slate-800 border-slate-600", gray: "bg-gray-800 border-gray-600" })[c] || "bg-gray-800 border-gray-600";
  };

  const getValuePanelClasses = (k) => {
    const c = dimensionMeta[k]?.color || "gray";
    return ({ blue: "bg-blue-100 text-blue-900 border-blue-600", purple: "bg-purple-100 text-purple-900 border-purple-600", green: "bg-green-100 text-green-900 border-green-600", amber: "bg-amber-100 text-amber-900 border-amber-600", rose: "bg-rose-100 text-rose-900 border-rose-600", slate: "bg-slate-100 text-slate-900 border-slate-600", gray: "bg-gray-100 text-gray-900 border-gray-600" })[c] || "bg-gray-100 text-gray-900 border-gray-600";
  };

  const getConstraintColor = (k) => {
    const c = dimensionMeta[k]?.color || "gray";
    return ({ blue: "bg-blue-100 text-blue-800 border-blue-300", purple: "bg-purple-100 text-purple-800 border-purple-300", green: "bg-green-100 text-green-800 border-green-300", amber: "bg-amber-100 text-amber-800 border-amber-300", rose: "bg-rose-100 text-rose-800 border-rose-300", slate: "bg-slate-100 text-slate-800 border-slate-300", gray: "bg-gray-100 text-gray-800 border-gray-300" })[c] || "bg-gray-100 text-gray-800 border-gray-300";
  };

  const inferValueType = (meta) => {
    if (meta?.value_type) return meta.value_type;
    if (typeof meta?.distinct_values === "number" && meta.distinct_values <= 25) return "enum";
    return "text";
  };

  const opsForType = (valueType) => {
    if (valueType === "enum")   return [{ key: "eq", label: "equals" }];
    if (valueType === "number") return NUMBER_OPS;
    if (valueType === "date")   return DATE_OPS;
    return TEXT_OPS;
  };

  const defaultOpForField = (dim, field) => {
    const meta = affordances?.[dim]?.[field];
    return DEFAULT_OP_BY_TYPE[inferValueType(meta)] || "contains";
  };

  // ── Selection helpers ─────────────────────────────────────────────────────
  const toggleSelected = (id) => {
    setSelectedIds(prev => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  const selectAll = () => {
    setSelectedIds(new Set(sortedResults.map(r => r.id)));
  };

  const clearSelection = () => setSelectedIds(new Set());

  const addConstraint = (field, value, dim, opOverride, value2Override) => {
    if (!field || value == null || value === "" || !dim) return;
    const op = opOverride || defaultOpForField(dim, field);

    let v1 = value;
    let v2 = value2Override ?? "";

    // BETWEEN: silently swap if low > high (user typed them in reverse order)
    if (op === "between" && v2 !== "") {
      const n1 = parseFloat(v1);
      const n2 = parseFloat(v2);
      if (!isNaN(n1) && !isNaN(n2) && n1 > n2) {
        [v1, v2] = [v2, v1];
      }
    }

    const c = { id: Date.now(), category: dim, field, value: v1, op, ...(op === "between" ? { value2: v2 } : {}) };
    if (op === "between" && (c.value2 == null || c.value2 === "")) return;
    setConstraints((prev) => [...prev, c]);
    setSearchTerm(""); setSearchTerm2(""); setShowResults(false);
  };

  const removeConstraint = (id) => { setConstraints((prev) => prev.filter((c) => c.id !== id)); setShowResults(false); };

  const resetQuery = () => {
    setConstraints([]); setSearchTerm(""); setSearchTerm2(""); setSelectedOp(null);
    setShowResults(false); setResults([]); setValuesError(null); setQueryStats(null);
    setShowSaveDialog(false); setShowLoadPanel(false);
    setProjectedFields(new Set()); setShowFieldPicker(false);
    setSortField(null); setSortDir('asc');
    setGroupByField(null); setGroupExpanded(new Set());
    setPageOffset(0);
    setShowSaveSetDialog(false); setSaveSetNameInput("");
    setShowDiffPanel(false); setDiffSetA(null); setDiffSetB(null); setDiffResult(null); setSetOpResult(null); setSetOperation("diff");
    // Close any open drawer so date picker and value panels reset cleanly
    setActiveDrawer(null); setActiveField(null);
  };

  const fieldsForActiveDrawer = useMemo(() => {
    if (!affordances || !activeDrawer || !affordances[activeDrawer]) return [];
    return Object.entries(affordances[activeDrawer]).sort((a, b) => (b[1]?.fact_count || 0) - (a[1]?.fact_count || 0));
  }, [affordances, activeDrawer]);

  const activeFieldMeta = useMemo(() => affordances?.[activeDrawer]?.[activeField] || null, [affordances, activeDrawer, activeField]);
  const activeFieldType = useMemo(() => activeFieldMeta ? inferValueType(activeFieldMeta) : null, [activeFieldMeta]);

  useEffect(() => {
    if (!activeDrawer || !activeField || !activeFieldMeta) { setSelectedOp(null); setSearchTerm(""); setSearchTerm2(""); return; }
    setSelectedOp(DEFAULT_OP_BY_TYPE[inferValueType(activeFieldMeta)] || "contains");
    setSearchTerm(""); setSearchTerm2("");
  }, [activeDrawer, activeField, activeFieldMeta]);

  useEffect(() => {
    if (!activeDrawer || !activeField || !activeFieldMeta) return;
    if (inferValueType(activeFieldMeta) !== "enum") return;
    const cacheKey = `${activeDrawer}:${activeField}`;
    if (valuesCache[cacheKey]) return;
    let cancelled = false;
    const load = async () => {
      setValuesLoading(true); setValuesError(null);
      try {
        const r = await fetch(`${API_URL}/values/${encodeURIComponent(activeDrawer)}/${encodeURIComponent(activeField)}?schema=${activeSchema}`);
        const d = await r.json();
        if (!cancelled) setValuesCache((p) => ({ ...p, [cacheKey]: Array.isArray(d.values) ? d.values : [] }));
      } catch { if (!cancelled) setValuesError("Failed to load values for this field."); }
      finally { if (!cancelled) setValuesLoading(false); }
    };
    load();
    return () => { cancelled = true; };
  }, [activeDrawer, activeField, activeFieldMeta, valuesCache]);

  const executeQuery = async () => {
    if (constraints.length === 0) { alert("Please add at least one constraint"); return; }

    // Contradiction check — eq + not_eq on the same dimension + field + value
    // is logically unsatisfiable regardless of data. Reject before firing.
    const eqKeys   = new Set(constraints.filter(c => c.op === "eq").map(c => `${c.category}|${c.field}|${c.value}`));
    const notEqHit = constraints.find(c => c.op === "not_eq" && eqKeys.has(`${c.category}|${c.field}|${c.value}`));
    if (notEqHit) {
      alert(
        `Unsatisfiable query: ${notEqHit.category}.${notEqHit.field} cannot equal and not equal "${notEqHit.value}" at the same time.\n\nNo result set can satisfy this — remove one of the conflicting constraints.`
      );
      return;
    }

    setLoading(true); setShowResults(true); setShowLoadPanel(false); setDiffInspectLabel(null); setPageOffset(0);
    try {
      const body = { constraints, schema: activeSchema };
      // Pass projected fields to backend when user has selected a subset
      if (projectedFields.size > 0) {
        body.fields = Array.from(projectedFields);
      }
      const r = await fetch(`${API_URL}/query`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const d = await r.json();
      startTransition(() => {
        setResults(Array.isArray(d.results) ? d.results : []);
        setSelectedIds(new Set());
        setQueryStats({
          probe_ms:       d.probe_ms,
          execution_ms:   d.execution_ms,
          total_ms:       d.total_ms,
          row_count:      d.row_count,
          trace:          d.trace || [],
          portolan_order: d.portolan_order || [],
          // Provenance — for XLSX Sheet 2 and result set identity model
          substrate_id:   d.query_identity?.substrate_id || activeSchema,
          lens_id:        d.query_identity?.lens_id || d.lens_id || '',
          translator_version: d.query_identity?.translator_version || '',
          query_hash:     d.query_identity?.query_hash || null,
          executed_at:    d.query_identity?.executed_at || new Date().toISOString(),
          peirce:         toPeirce(constraints),
          constraints:    constraints.map(({ id, ...rest }) => rest),
          projected_fields: projectedFields.size > 0 ? Array.from(projectedFields) : null,
          sort_field:     sortField,
          sort_dir:       sortDir,
        });
        setQueryVersion(v => v + 1); // signal TrieValuePanel instances to refetch
      });
    } catch { console.error("Query failed"); alert("Query failed. Make sure the API server is running."); setResults([]); }
    finally { setLoading(false); }
  };

  const generateNaturalLanguage = () => {
    if (constraints.length === 0) return "Add constraints to begin searching";
    const renderOne = (c) => c.op === "between"
      ? `${c.category}.${c.field} between "${c.value}" and "${c.value2}"`
      : `${c.category}.${c.field} ${c.op} "${c.value}"`;
    return `Entities where ${constraints.map(renderOne).join(" AND ")}`;
  };

  // ── P7: Save / load handlers ───────────────────────────────────────────────

  const handleSaveQuery = () => {
    const name = saveNameInput.trim();
    if (!name) return;
    if (constraints.length === 0) { alert("No constraints to save"); return; }
    const entry = {
      id:                 Date.now(),
      name,
      peirce:             currentPeirce,
      schema:             activeSchema,
      constraints:        constraints.map(({ id, ...rest }) => rest),
      saved_at:           new Date().toISOString(),
      query_hash:         queryStats?.query_hash || null,
      lens_id:            queryStats?.lens_id || null,
      translator_version: queryStats?.translator_version || null,
      substrate_id:       queryStats?.substrate_id || activeSchema,
    };
    setSavedQueries((prev) => [entry, ...prev]);
    setSaveNameInput("");
    setShowSaveDialog(false);
  };

  const handleLoadQuery = (entry) => {
    const rebuilt = entry.constraints.map((c) => ({ ...c, id: Date.now() + Math.random() }));
    if (entry.schema && entry.schema !== activeSchema) {
      switchSchema(entry.schema);
    }
    setConstraints(rebuilt);
    setShowLoadPanel(false);
    setShowResults(false);
    setResults([]);
  };

  const handleDeleteQuery = (id, e) => {
    e.stopPropagation();
    setSavedQueries((prev) => prev.filter((q) => q.id !== id));
  };

  // ── Phase 2: Named result set handlers ────────────────────────────────────

  const handleSaveSet = () => {
    const name = saveSetNameInput.trim();
    if (!name || !queryStats || results.length === 0) return;

    // Sanitize name to be filename-safe
    const setId = name.replace(/[^a-zA-Z0-9_\-. ]/g, "").trim().replace(/\s+/g, "-");

    const rset = {
      set_id:       setId,
      query: {
        substrate_id:        queryStats.substrate_id || activeSchema,
        lens_id:             queryStats.lens_id || "",
        translator_version:  queryStats.translator_version || "",
        constraints:         queryStats.constraints || constraints.map(({ id, ...rest }) => rest),
        query_hash:          queryStats.query_hash || null,
        executed_at:         queryStats.executed_at || new Date().toISOString(),
        peirce:              queryStats.peirce || toPeirce(constraints),
      },
      results: {
        entity_ids:  results.map(r => r.id),
        count:       queryStats.row_count ?? results.length,
        captured_at: new Date().toISOString(),
      },
      projection: projectedFields.size > 0 ? Array.from(projectedFields) : null,
      sort:       sortField ? { field: sortField, dir: sortDir } : null,
      // Internal metadata for LOAD panel display
      _saved_at:  new Date().toISOString(),
      _id:        Date.now(),
    };

    setSavedSets(prev => [rset, ...prev]);
    setSaveSetNameInput("");
    setShowSaveSetDialog(false);
  };

  const handleDownloadSet = (rset) => {
    // Export .peirce — strips internal _* metadata fields
    const { _saved_at, _id, ...exportable } = rset;
    const blob = new Blob([JSON.stringify(exportable, null, 2)], { type: "application/json" });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href     = url;
    a.download = `${rset.set_id}.peirce`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleDeleteSet = (id, e) => {
    e.stopPropagation();
    setSavedSets(prev => prev.filter(s => s._id !== id));
  };

  const handleLoadSetConstraints = (rset) => {
    // Restore constraints from saved set — same pattern as handleLoadQuery
    const rebuilt = (rset.query.constraints || []).map(c => ({ ...c, id: Date.now() + Math.random() }));
    if (rset.query.substrate_id && rset.query.substrate_id !== activeSchema) {
      switchSchema(rset.query.substrate_id);
    }
    setConstraints(rebuilt);
    setShowLoadPanel(false);
    setShowResults(false);
    setResults([]);
  };

  const handleLoadRsetFile = (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (evt) => {
      try {
        const parsed = JSON.parse(evt.target.result);

        // Validate minimum required shape
        const missing = [];
        if (!parsed.set_id)              missing.push("set_id");
        if (!parsed.query?.substrate_id) missing.push("query.substrate_id");
        if (!parsed.results?.entity_ids) missing.push("results.entity_ids");
        if (!parsed.results?.count)      missing.push("results.count");

        if (missing.length > 0) {
          alert(`Invalid .peirce file — missing fields: ${missing.join(", ")}`);
          return;
        }

        // Check for duplicate
        const isDuplicate = savedSets.some(
          s => s.set_id === parsed.set_id &&
               s.results?.captured_at === parsed.results?.captured_at
        );
        if (isDuplicate) {
          alert(`"${parsed.set_id}" is already in your saved sets (same capture time).`);
          return;
        }

        // Add internal metadata and push to savedSets
        const rset = {
          ...parsed,
          _id:       Date.now(),
          _saved_at: new Date().toISOString(),
          _source:   "file",  // distinguish from locally-saved sets
        };
        setSavedSets(prev => [rset, ...prev]);
      } catch {
        alert("Could not parse file — make sure it is a valid .peirce file.");
      }
    };
    reader.readAsText(file);
    // Reset input so same file can be loaded again if needed
    e.target.value = "";
  };

  // ── Phase 2: Diff handler ────────────────────────────────────────────────

  const computeSetOperation = (setA, setB, operation) => {
    if (!setA || !setB) return;

    const warnings = [];
    if (setA.query.substrate_id !== setB.query.substrate_id) {
      warnings.push(`Substrate mismatch: ${setA.query.substrate_id} vs ${setB.query.substrate_id}`);
    }
    if (setA.query.lens_id !== setB.query.lens_id) {
      warnings.push(`Lens mismatch: ${setA.query.lens_id} vs ${setB.query.lens_id}`);
    }

    const idsA = new Set(setA.results.entity_ids);
    const idsB = new Set(setB.results.entity_ids);

    if (operation === 'diff') {
      const onlyA = setA.results.entity_ids.filter(id => !idsB.has(id));
      const onlyB = setB.results.entity_ids.filter(id => !idsA.has(id));
      const both  = setA.results.entity_ids.filter(id => idsB.has(id));
      setDiffResult({ onlyA, onlyB, both, warnings });
      setSetOpResult(null);
    } else if (operation === 'union') {
      // A ∪ B — all unique ids from both sets, A-order first then B-only
      const union = [
        ...setA.results.entity_ids,
        ...setB.results.entity_ids.filter(id => !idsA.has(id)),
      ];
      setSetOpResult({ ids: union, operation: 'union', warnings,
        label: `${setA.set_id} ∪ ${setB.set_id}` });
      setDiffResult(null);
    } else if (operation === 'intersect') {
      // A ∩ B — ids that appear in both
      const intersect = setA.results.entity_ids.filter(id => idsB.has(id));
      setSetOpResult({ ids: intersect, operation: 'intersect', warnings,
        label: `${setA.set_id} ∩ ${setB.set_id}` });
      setDiffResult(null);
    }
  };

  const computeDiff = (setA, setB) => {
    if (!setA || !setB) return;

    // Compatibility check
    const warnings = [];
    if (setA.query.substrate_id !== setB.query.substrate_id) {
      warnings.push(`Substrate mismatch: ${setA.query.substrate_id} vs ${setB.query.substrate_id}`);
    }
    if (setA.query.lens_id !== setB.query.lens_id) {
      warnings.push(`Lens mismatch: ${setA.query.lens_id} vs ${setB.query.lens_id}`);
    }
    if (setA.query.translator_version && setB.query.translator_version &&
        setA.query.translator_version !== setB.query.translator_version) {
      warnings.push(`Translator version mismatch: ${setA.query.translator_version} vs ${setB.query.translator_version}`);
    }

    const idsA = new Set(setA.results.entity_ids);
    const idsB = new Set(setB.results.entity_ids);

    const onlyA = setA.results.entity_ids.filter(id => !idsB.has(id));
    const onlyB = setB.results.entity_ids.filter(id => !idsA.has(id));
    const both  = setA.results.entity_ids.filter(id => idsB.has(id));

    setDiffResult({ onlyA, onlyB, both, warnings });
  };

  const inspectDiffGroup = async (entityIds, label) => {
    if (!entityIds || entityIds.length === 0) return;
    setLoading(true);
    setShowDiffPanel(false);  // hide panel but remember we came from it
    setShowResults(true);
    setDiffInspectLabel(label);
    setResults([]);
    try {
      const body = { entity_ids: entityIds, schema: activeSchema };
      if (projectedFields.size > 0) body.fields = Array.from(projectedFields);
      const r = await fetch(`${API_URL}/hydrate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const d = await r.json();
      setResults(Array.isArray(d.results) ? d.results : []);
      setQueryStats(null); // no query provenance — this came from a diff
    } catch {
      console.error("Hydrate failed");
      alert("Hydrate failed. Make sure the API server is running.");
      setResults([]);
    } finally {
      setLoading(false);
    }
  };

  // ── Set operation export ─────────────────────────────────────────────────

  const exportSetOperation = (format) => {
    const XLSX = window.XLSX;

    // Gather the data depending on what's showing
    const isDiff   = !!diffResult;
    const isSetOp  = !!setOpResult;
    if (!isDiff && !isSetOp) return;

    if (format === 'json') {
      const payload = isDiff ? {
        operation:  'diff',
        set_a:      diffSetA?.set_id,
        set_b:      diffSetB?.set_id,
        only_a:     { count: diffResult.onlyA.length, entity_ids: diffResult.onlyA },
        only_b:     { count: diffResult.onlyB.length, entity_ids: diffResult.onlyB },
        both:       { count: diffResult.both.length,  entity_ids: diffResult.both  },
        warnings:   diffResult.warnings || [],
        exported_at: new Date().toISOString(),
      } : {
        operation:   setOpResult.operation,
        label:       setOpResult.label,
        set_a:       diffSetA?.set_id,
        set_b:       diffSetB?.set_id,
        entity_ids:  setOpResult.ids,
        count:       setOpResult.ids.length,
        warnings:    setOpResult.warnings || [],
        exported_at: new Date().toISOString(),
      };
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
      const url  = URL.createObjectURL(blob);
      const a    = document.createElement('a');
      a.href     = url;
      a.download = `reckoner_setop_${Date.now()}.json`;
      a.click();
      URL.revokeObjectURL(url);
      return;
    }

    if (format === 'xlsx') {
      if (!XLSX) { alert('SheetJS not available.'); return; }
      const wb = XLSX.utils.book_new();

      if (isDiff) {
        // Three sheets for diff
        const toRows = ids => ids.map(id => ({ entity_id: id }));
        XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(toRows(diffResult.onlyA)), `Only in ${diffSetA?.set_id}`.slice(0, 31));
        XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(toRows(diffResult.onlyB)), `Only in ${diffSetB?.set_id}`.slice(0, 31));
        XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(toRows(diffResult.both)),  'In Both');
      } else {
        // One sheet for union/intersect
        const rows = setOpResult.ids.map(id => ({ entity_id: id }));
        XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(rows), setOpResult.label.slice(0, 31));
      }

      // Always add a provenance sheet
      const meta = [
        { field: 'operation',    value: isDiff ? 'diff' : setOpResult.operation },
        { field: 'set_a',        value: diffSetA?.set_id || '' },
        { field: 'set_b',        value: diffSetB?.set_id || '' },
        { field: 'set_a_count',  value: String(diffSetA?.results?.count ?? '') },
        { field: 'set_b_count',  value: String(diffSetB?.results?.count ?? '') },
        { field: 'set_a_peirce', value: diffSetA?.query?.peirce || '' },
        { field: 'set_b_peirce', value: diffSetB?.query?.peirce || '' },
        { field: 'exported_at',  value: new Date().toISOString() },
      ];
      XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(meta), 'Provenance');
      XLSX.writeFile(wb, `reckoner_setop_${Date.now()}.xlsx`);
    }
  };

  // ─────────────────────────────────────────────────────────────────────────────
  // Sub-components
  // ─────────────────────────────────────────────────────────────────────────────

  const DrawerContent = ({ dimensionKey }) => {
    if (!affordances || !dimensionKey || !affordances[dimensionKey])
      return <div className="text-sm opacity-60 italic p-4">No fields available</div>;
    return (
      <div className="space-y-1">
        {Object.entries(affordances[dimensionKey])
          .sort((a, b) => (b[1]?.fact_count || 0) - (a[1]?.fact_count || 0))
          .map(([fieldKey, meta]) => {
            const isConstrained = constraints.some((c) => c.category === dimensionKey && c.field === fieldKey);
            return (
              <button key={fieldKey}
                onClick={() => setActiveField(activeField === fieldKey ? null : fieldKey)}
                className={`w-full text-left px-2 py-2 rounded transition-all ${isConstrained ? "underline underline-offset-4 decoration-white font-semibold" : ""} ${activeField === fieldKey ? "opacity-90 bg-white/10" : "hover:bg-white/10"}`}
                title={`${meta?.fact_count?.toLocaleString?.() || ""} facts • ${meta?.distinct_values?.toLocaleString?.() || ""} distinct`}
              >
                <div className="flex justify-between items-center">
                  <span className="text-sm">{fieldKey}</span>
                  <span className="text-xs opacity-60 ml-2">
                    {inferValueType(meta).toUpperCase()} · {
                      meta?.distinct_values >= 1000
                        ? `${Math.round(meta.distinct_values / 1000)}k`
                        : meta?.distinct_values
                    }d
                  </span>
                </div>
              </button>
            );
          })}
      </div>
    );
  };

  const OperatorDropdown = ({ valueType, value, onChange }) => {
    const ops = opsForType(valueType);
    if (ops.length <= 1) return null;
    return (
      <div className="relative">
        <select value={value || ""} onChange={(e) => onChange(e.target.value)}
          className="w-full px-3 py-2 border border-gray-300 rounded-md text-gray-900 focus:outline-none focus:ring-2 focus:ring-blue-500 appearance-none bg-white"
        >
          {ops.map((op) => <option key={op.key} value={op.key}>{op.label}</option>)}
        </select>
        <ChevronDown size={14} className="absolute right-2 top-1/2 -translate-y-1/2 pointer-events-none text-gray-500" />
      </div>
    );
  };

  const ValuePanel = () => {
    if (!activeDrawer || !activeField) return null;
    const meta      = activeFieldMeta;
    const valueType = activeFieldType || "text";
    const op        = selectedOp || DEFAULT_OP_BY_TYPE[valueType] || "contains";
    const cacheKey  = `${activeDrawer}:${activeField}`;
    const enumVals  = valuesCache[cacheKey] || [];
    const panelCls  = getValuePanelClasses(activeDrawer);
    const canAdd    = searchTerm.trim() !== "" && (op !== "between" || searchTerm2.trim() !== "");

    const addFromInputs = () => {
      if (!canAdd) return;
      addConstraint(activeField, searchTerm.trim(), activeDrawer, op, op === "between" ? searchTerm2.trim() : undefined);
    };

    return (
      <motion.div
        initial={{ x: -300, opacity: 0 }} animate={{ x: 0, opacity: 1 }}
        exit={{ x: -300, opacity: 0 }} transition={{ type: "spring", stiffness: 260, damping: 20 }}
        className={`w-80 border-l-4 overflow-auto ${panelCls}`}
      >
        <div className="p-4">
          <div className="flex justify-between items-center mb-3">
            <div className="font-bold text-sm uppercase tracking-wide">
              {activeDrawer} / <span className="font-mono">{activeField}</span>
            </div>
            <button onClick={() => setActiveField(null)} className="opacity-40 hover:opacity-100 transition-opacity">
              <X size={16} />
            </button>
          </div>
          {meta && (
            <div className="text-xs opacity-60 mb-3">
              {meta.fact_count?.toLocaleString?.() || meta.fact_count} facts · {meta.distinct_values?.toLocaleString?.() || meta.distinct_values} distinct · {inferValueType(meta).toUpperCase()}
            </div>
          )}

          {valueType === "enum" ? (
            <div>
              {valuesLoading && <div className="text-xs opacity-60 italic">Loading values…</div>}
              {valuesError  && <div className="text-xs text-red-600">{valuesError}</div>}
              {!valuesLoading && enumVals.length > 0 && (
                <div className="space-y-1 max-h-80 overflow-auto">
                  {enumVals.map((v) => (
                    <button key={v} onClick={() => addConstraint(activeField, v, activeDrawer, "eq")}
                      className="w-full text-left px-2 py-1 rounded text-sm hover:bg-black/10 transition-colors font-mono truncate"
                    >{v}</button>
                  ))}
                </div>
              )}
            </div>
          ) : (
            <div className="space-y-2">
              <OperatorDropdown valueType={valueType} value={op} onChange={setSelectedOp} />
              <div className="flex items-center gap-2">
                <Search size={16} className="opacity-60" />
                <span className="text-xs opacity-70">{op === "between" ? "Enter two values" : "Enter a value"}</span>
              </div>
              <input type="text" placeholder={`Enter value for ${activeField}…`} value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter") addFromInputs(); }}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-gray-900"
                autoFocus
              />
              {op === "between" && (
                <input type="text" placeholder="Enter second value…" value={searchTerm2}
                  onChange={(e) => setSearchTerm2(e.target.value)}
                  onKeyDown={(e) => { if (e.key === "Enter") addFromInputs(); }}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-gray-900"
                />
              )}
              <button onClick={addFromInputs} disabled={!canAdd}
                className={`w-full px-3 py-2 rounded-md transition-colors ${canAdd ? "bg-blue-600 text-white hover:bg-blue-700" : "bg-gray-300 text-gray-600 cursor-not-allowed"}`}
              >Add constraint</button>
            </div>
          )}
        </div>
      </motion.div>
    );
  };

  // ─────────────────────────────────────────────────────────────────────────────
  // Projection helpers
  // ─────────────────────────────────────────────────────────────────────────────

  // Derive the list of all field names present in the current result set.
  // Format: [{ dim: "WHO", field: "artist" }, ...]
  const availableFields = useMemo(() => {
    if (!results || results.length === 0) return [];
    const seen = new Map(); // "dim:field" -> { dim, field }
    for (const item of results) {
      for (const [dim, facts] of Object.entries(item.coordinates || {})) {
        for (const fact of facts || []) {
          const key = `${dim}:${fact.field}`;
          if (!seen.has(key)) seen.set(key, { dim, field: fact.field });
        }
      }
    }
    // Sort by DIM_ORDER then field name
    const dimOrder = ["WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"];
    return Array.from(seen.values()).sort((a, b) => {
      const di = dimOrder.indexOf(a.dim) - dimOrder.indexOf(b.dim);
      if (di !== 0) return di;
      return a.field.localeCompare(b.field);
    });
  }, [results]);

  // Fields available for sort — projected fields when active, otherwise all available.
  const sortableFields = useMemo(() => {
    if (projectedFields.size > 0) {
      return availableFields.filter(f => projectedFields.has(f.field));
    }
    return availableFields;
  }, [availableFields, projectedFields]);

  // Sort results — pure derived value, no mutation of results state.
  const sortedResults = useMemo(() => {
    if (!sortField || results.length === 0) return results;
    const DIM_ORDER_SORT = ['WHO', 'WHAT', 'WHEN', 'WHERE', 'WHY', 'HOW'];
    return [...results].sort((a, b) => {
      // Extract sort value deterministically:
      // - traverse dimensions in fixed DIM_ORDER
      // - collect ALL values for sortField, sort them, take the first alphabetically
      // This makes multi-value fields (e.g. multiple labels) sort consistently
      const getValue = (item) => {
        const coords = item.coordinates || {};
        const allValues = [];
        for (const dim of DIM_ORDER_SORT) {
          const facts = coords[dim] || [];
          for (const f of facts) {
            if (f.field === sortField && f.value != null && f.value !== '') {
              allValues.push(String(f.value));
            }
          }
        }
        if (allValues.length === 0) return '';
        // For numeric fields all values will be numbers — take the min for asc sort
        const nums = allValues.map(v => parseFloat(v));
        if (nums.every(n => !isNaN(n))) return String(Math.min(...nums));
        // For text fields — sort and take first alphabetically so sort is stable
        return allValues.sort((x, y) => x.localeCompare(y, undefined, { sensitivity: 'base' }))[0];
      };
      const av = getValue(a);
      const bv = getValue(b);
      const an = parseFloat(av);
      const bn = parseFloat(bv);
      const numericSort = !isNaN(an) && !isNaN(bn);
      let cmp;
      if (numericSort) {
        cmp = an - bn;
      } else {
        cmp = String(av).localeCompare(String(bv), undefined, { sensitivity: 'base' });
      }
      return sortDir === 'asc' ? cmp : -cmp;
    });
  }, [results, sortField, sortDir]);

  // ── Grouped results — derived from sortedResults ─────────────────────────
  // Returns Map<groupKey, { dim, field, value, items: [] }> when groupByField is set.
  const groupedResults = useMemo(() => {
    if (!groupByField || sortedResults.length === 0) return null;
    const groups = new Map();
    for (const item of sortedResults) {
      // Find the value for groupByField in any dimension
      let groupValue = null;
      let groupDim   = null;
      for (const [dim, facts] of Object.entries(item.coordinates || {})) {
        const match = (facts || []).find(f => f.field === groupByField);
        if (match) { groupValue = match.value; groupDim = dim; break; }
      }
      const key = groupValue ?? '(none)';
      if (!groups.has(key)) {
        groups.set(key, { dim: groupDim, field: groupByField, value: key, items: [] });
      }
      groups.get(key).items.push(item);
    }
    // Sort groups — date fields sort chronologically, numeric fields sort
    // numerically, text fields sort by count descending (most common first).
    const keys = [...groups.keys()].filter(k => k !== '(none)');
    const isDateField = groupByField && /date|year|month|day|at|time/i.test(groupByField);
    const isNumeric = !isDateField && keys.every(k => !isNaN(parseFloat(k)));

    return new Map([...groups.entries()].sort((a, b) => {
      if (a[0] === '(none)') return 1;
      if (b[0] === '(none)') return -1;
      if (isDateField) {
        // Normalize datetime strings to date-only for correct chronological sort
        const dateA = a[0].split(' ')[0];
        const dateB = b[0].split(' ')[0];
        return dateA < dateB ? -1 : dateA > dateB ? 1 : 0;
      }
      if (isNumeric) {
        const an = parseFloat(a[0]);
        const bn = parseFloat(b[0]);
        if (!isNaN(an) && !isNaN(bn)) return an - bn;
        return a[0].localeCompare(b[0]);
      }
      return b[1].items.length - a[1].items.length; // count desc for text
    }));
  }, [sortedResults, groupByField]);

  const toggleGroup = (key) => {
    setGroupExpanded(prev => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key); else next.add(key);
      return next;
    });
  };

  const toggleField = (field) => {
    setProjectedFields(prev => {
      const next = new Set(prev);
      if (next.has(field)) next.delete(field); else next.add(field);
      return next;
    });
  };

  const FieldPicker = () => {
    if (availableFields.length === 0) return null;
    return (
      <div className="mb-4 p-3 border border-gray-200 rounded-lg bg-gray-50">
        <div className="flex justify-between items-center mb-2">
          <span className="text-xs font-semibold text-gray-600 uppercase tracking-wide">
            Display fields {projectedFields.size > 0 ? `(${projectedFields.size} selected)` : "(all)"}
          </span>
          {projectedFields.size > 0 && (
            <button
              onClick={() => setProjectedFields(new Set())}
              className="text-xs text-gray-400 hover:text-gray-600 underline"
            >
              show all
            </button>
          )}
        </div>
        <div className="flex flex-wrap gap-2">
          {availableFields.map(({ dim, field }) => {
            const selected = projectedFields.size === 0 || projectedFields.has(field);
            const active   = projectedFields.has(field);
            const accent   = DIM_ACCENT[dim] || "#6b7280";
            const accentLight = DIM_ACCENT_LIGHT[dim] || "#f9fafb";
            return (
              <button
                key={`${dim}:${field}`}
                onClick={() => toggleField(field)}
                style={{
                  borderColor: active ? accent : "#e5e7eb",
                  background:  active ? accentLight : "#ffffff",
                  color:       active ? DIM_ACCENT_TEXT[dim] || "#374151" : "#6b7280",
                }}
                className={`inline-flex items-center gap-1 px-2 py-0.5 rounded border text-xs transition-colors cursor-pointer`}
                title={`${dim}.${field}`}
              >
                <span className="font-semibold opacity-60" style={{ fontSize: 9 }}>{dim}</span>
                <span>{field.replace(/_/g, " ")}</span>
              </button>
            );
          })}
        </div>
        {projectedFields.size > 0 && (
          <div className="mt-2 text-xs text-gray-400">
            Re-run search to apply to new results · current cards reflect selected fields
          </div>
        )}
      </div>
    );
  };

  // ─────────────────────────────────────────────────────────────────────────────
  // ─────────────────────────────────────────────────────────────────────────────
  // Export helpers
  // ─────────────────────────────────────────────────────────────────────────────

  // Flatten sortedResults into row objects — one row per entity, one column per field.
  // Respects projection: if projectedFields is set, only those fields appear.
  const buildFlatRows = () => {
    const rows = selectedIds.size > 0
      ? sortedResults.filter(item => selectedIds.has(item.id))
      : sortedResults;
    return rows.map(item => {
      const row = { entity_id: item.id };
      for (const [dim, facts] of Object.entries(item.coordinates || {})) {
        for (const fact of facts || []) {
          if (projectedFields.size > 0 && !projectedFields.has(fact.field)) continue;
          // If a field appears multiple times (multi-value), join with '; '
          const key = `${dim.toLowerCase()}_${fact.field}`;
          if (row[key] !== undefined) {
            row[key] = `${row[key]}; ${fact.value}`;
          } else {
            row[key] = fact.value;
          }
        }
      }
      return row;
    });
  };

  const buildQueryRecord = () => ({
    substrate:           queryStats?.substrate_id || activeSchema,
    lens_id:             queryStats?.lens_id || '',
    translator_version:  queryStats?.translator_version || '',
    query_hash:          queryStats?.query_hash || '',
    peirce:              queryStats?.peirce || toPeirce(constraints),
    executed_at:         queryStats?.executed_at || new Date().toISOString(),
    result_count:        queryStats?.row_count ?? sortedResults.length,
    projected_fields:    queryStats?.projected_fields ? queryStats.projected_fields.join(', ') : '(all)',
    sort_field:          queryStats?.sort_field || '(none)',
    sort_dir:            queryStats?.sort_field ? (queryStats?.sort_dir || 'asc') : '—',
    probe_ms:            queryStats?.probe_ms ?? '',
    execution_ms:        queryStats?.execution_ms ?? '',
  });

  const exportCSV = () => {
    const rows = buildFlatRows();
    if (rows.length === 0) return;
    const cols = Object.keys(rows[0]);
    const escape = (v) => {
      const s = String(v ?? '');
      return s.includes(',') || s.includes('"') || s.includes('\n')
        ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const csv = [
      cols.join(','),
      ...rows.map(r => cols.map(c => escape(r[c] ?? '')).join(','))
    ].join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = `reckoner_${activeSchema}_${Date.now()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const exportJSON = () => {
    const payload = {
      query:   buildQueryRecord(),
      results: sortedResults,
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = `reckoner_${activeSchema}_${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const exportXLSX = async () => {
    // SheetJS — expect XLSX to be available on window (loaded via CDN or npm)
    const XLSX = window.XLSX;
    if (!XLSX) { alert('SheetJS not available. Add XLSX to the page.'); return; }

    const rows    = buildFlatRows();
    const qRecord = buildQueryRecord();

    // Sheet 1 — result rows
    const ws1 = XLSX.utils.json_to_sheet(rows);

    // Sheet 2 — query record (key / value pairs)
    const qRows = Object.entries(qRecord).map(([k, v]) => ({ field: k, value: String(v ?? '') }));
    const ws2   = XLSX.utils.json_to_sheet(qRows);

    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws1, 'Results');
    XLSX.utils.book_append_sheet(wb, ws2, 'Query Record');

    XLSX.writeFile(wb, `reckoner_${activeSchema}_${Date.now()}.xlsx`);
  };

  const exportParquet = async () => {
    // Parquet is backend-generated — POST constraints, receive file download.
    try {
      const body = { constraints, schema: activeSchema };
      if (projectedFields.size > 0) body.fields = Array.from(projectedFields);
      if (sortField) { body.sort_field = sortField; body.sort_dir = sortDir; }
      if (selectedIds.size > 0) body.entity_ids = Array.from(selectedIds);

      const r = await fetch(`${API_URL}/export/parquet`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });

      if (!r.ok) { alert(`Parquet export failed: ${r.status}`); return; }

      const blob     = await r.blob();
      const url      = URL.createObjectURL(blob);
      const a        = document.createElement('a');
      a.href         = url;
      a.download     = `reckoner_${activeSchema}_${Date.now()}.parquet`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error('Parquet export failed:', err);
      alert('Parquet export failed. Check that the API server supports /export/parquet.');
    }
  };

  // ─────────────────────────────────────────────────────────────────────────────
  // Render
  // ─────────────────────────────────────────────────────────────────────────────
  return (
    <div className="h-screen flex flex-col bg-white text-black">

      {/* Header — two fixed zones, never reflowed by content */}
      <header className="flex-shrink-0 bg-white border-b border-gray-200">

        {/* Zone 1: Top chrome — fixed height, never moves */}
        <div className="flex items-center justify-between px-6 h-14 border-b border-gray-100">
          <div className="flex items-center gap-3">
            <h1 className="text-xl font-bold tracking-tight">Reckoner</h1>
            <span className="text-xs text-gray-400">semantic workbench</span>
          </div>
          <div className="flex items-center gap-3">
            {apiStatus && (
              <div className={`text-xs px-3 py-1 rounded-full ${apiStatus.status === "ok" ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"}`}>
                {apiStatus.status === "ok"
                  ? `Connected · ${(apiStatus.statistics?.total_facts ?? apiStatus.total_facts)?.toLocaleString?.() ?? ""} facts`
                  : "API Disconnected"}
              </div>
            )}
            {schemas.length > 0 && (
              <select
                value={activeSchema}
                onChange={(e) => switchSchema(e.target.value)}
                className="text-xs px-3 py-1 rounded border border-gray-300 bg-white text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                {schemas.map((s) => (
                  <option key={s.schema} value={s.schema}>{s.label}</option>
                ))}
              </select>
            )}
          </div>
        </div>

        {/* Zone 2: Constraint strip — bounded, scrolls internally, never pushes chrome */}
        <div className="px-6 py-2 max-h-40 overflow-y-auto">
          {constraints.length === 0 ? (
            <div className="text-sm text-gray-400 italic py-1">Add constraints to begin searching</div>
          ) : (
            <>
              <QuerySummary
                constraints={constraints}
                DIMENSIONS={DIMENSIONS}
                onRemove={removeConstraint}
              />
              {/* Peirce display — toggleable, lives with constraints */}
              <div className="mt-1">
                <button
                  onClick={() => setShowPeirce(v => !v)}
                  className="text-xs text-gray-400 hover:text-gray-600 transition-colors"
                >
                  {showPeirce ? '▾ hide peirce' : '▸ show peirce'}
                </button>
                {showPeirce && (
                  <div className="mt-1 bg-gray-900 rounded px-3 py-2 font-mono text-xs text-green-400 whitespace-pre">
                    {currentPeirce}
                  </div>
                )}
              </div>
              {/* Save dialog */}
              {showSaveDialog && (
                <div className="mt-2 flex gap-2 items-center">
                  <input
                    type="text"
                    placeholder="Name this query…"
                    value={saveNameInput}
                    onChange={(e) => setSaveNameInput(e.target.value)}
                    onKeyDown={(e) => { if (e.key === "Enter") handleSaveQuery(); if (e.key === "Escape") setShowSaveDialog(false); }}
                    className="flex-1 px-3 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    autoFocus
                  />
                  <button onClick={handleSaveQuery}
                    disabled={!saveNameInput.trim()}
                    className={`px-3 py-1.5 rounded text-sm ${saveNameInput.trim() ? "bg-blue-600 text-white hover:bg-blue-700" : "bg-gray-200 text-gray-500 cursor-not-allowed"}`}
                  >Save</button>
                  <button onClick={() => setShowSaveDialog(false)} className="px-3 py-1.5 rounded text-sm bg-gray-100 hover:bg-gray-200">Cancel</button>
                </div>
              )}
            </>
          )}
        </div>

      </header>

      {/* Main */}
      <div className="flex flex-1 relative overflow-hidden">

        {/* Left nav */}
        <nav className="w-28 bg-black text-white py-10 flex flex-col justify-center items-center text-sm font-semibold flex-shrink-0">
          <div className="flex flex-col gap-8 items-center">
            {DIMENSIONS.map((d) => {
              const Icon = d.icon;
              const hasDimension  = !!affordances?.[d.key];
              const hasConstraints = constraints.some((c) => c.category === d.key);
              return (
                <button key={d.key}
                  onClick={() => { if (!hasDimension) return; setActiveDrawer(activeDrawer === d.key ? null : d.key); setActiveField(null); }}
                  className={`flex flex-col items-center gap-2 transition duration-200 relative ${!hasDimension ? "opacity-25 cursor-not-allowed" : "hover:text-blue-300"} ${activeDrawer === d.key ? "text-blue-300" : ""}`}
                  title={hasDimension ? d.key : `${d.key} not present in dataset`}
                >
                  <Icon size={20} />
                  <span className="text-xs">{d.key}</span>
                  {hasConstraints && <div className="absolute -top-1 -right-1 w-2 h-2 bg-blue-400 rounded-full" />}
                </button>
              );
            })}

            <div className="h-px w-12 bg-gray-700 my-2" />

            {/* SEARCH */}
            <button onClick={executeQuery} disabled={constraints.length === 0}
              className={`flex flex-col items-center gap-2 transition duration-200 ${constraints.length === 0 ? "opacity-30 cursor-not-allowed" : "hover:text-green-300"} ${showResults ? "text-green-300" : ""}`}
              title="Execute query"
            >
              <Table size={20} />
              <span className="text-xs">SEARCH</span>
            </button>

            {/* SAVE — P7 */}
            <button
              onClick={() => { if (constraints.length === 0) return; setShowSaveDialog((v) => !v); setShowLoadPanel(false); }}
              disabled={constraints.length === 0}
              className={`flex flex-col items-center gap-2 transition duration-200 ${constraints.length === 0 ? "opacity-30 cursor-not-allowed" : "hover:text-yellow-300"} ${showSaveDialog ? "text-yellow-300" : ""}`}
              title="Save query"
            >
              <Bookmark size={20} />
              <span className="text-xs">SAVE</span>
            </button>

            {/* LOAD — P7 */}
            <button
              onClick={() => { setShowLoadPanel((v) => !v); setShowSaveDialog(false); setShowResults(false); }}
              className={`flex flex-col items-center gap-2 transition duration-200 hover:text-purple-300 ${showLoadPanel ? "text-purple-300" : ""}`}
              title={`Saved queries (${savedQueries.length})`}
            >
              <FolderOpen size={20} />
              <span className="text-xs">LOAD</span>
              {savedQueries.length > 0 && (
                <span className="absolute text-[9px] bg-purple-500 text-white rounded-full px-1 -mt-1 ml-3">{savedQueries.length}</span>
              )}
            </button>

            {/* DIFF */}
            <button
              onClick={() => { setShowDiffPanel(v => !v); setShowLoadPanel(false); setShowResults(false); }}
              disabled={savedSets.length < 2}
              className={`flex flex-col items-center gap-2 transition duration-200 relative
                ${savedSets.length < 2 ? "opacity-30 cursor-not-allowed" : "hover:text-orange-300"}
                ${showDiffPanel ? "text-orange-300" : ""}`}
              title={savedSets.length < 2 ? "Save at least 2 result sets to use set operations" : `Set operations (${savedSets.length} sets available)`}
            >
              <GitMerge size={20} />
              <span className="text-xs">DIFF</span>
              {savedSets.length >= 2 && (
                <span className="absolute text-[9px] bg-orange-500 text-white rounded-full px-1 -mt-1 ml-3">{savedSets.length}</span>
              )}
            </button>

            {/* RESET */}
            <button onClick={resetQuery}
              className="flex flex-col items-center gap-2 transition duration-200 hover:text-red-300"
              title="Reset"
            >
              <X size={20} />
              <span className="text-xs">RESET</span>
            </button>
          </div>
        </nav>

        {/* Drawer */}
        <AnimatePresence>
          {activeDrawer && (
            <motion.div
              initial={{ x: -400, opacity: 0 }} animate={{ x: 0, opacity: 1 }}
              exit={{ x: -400, opacity: 0 }} transition={{ type: "spring", stiffness: 260, damping: 20 }}
              className={`absolute top-0 bottom-0 w-80 p-3 border-l-4 text-white z-20 overflow-auto ${getDrawerClasses(activeDrawer)}`}
              style={{ left: 112 }}
            >
              <div className="flex justify-end mb-3">
                <button onClick={() => { setActiveDrawer(null); setActiveField(null); }} className="opacity-40 hover:opacity-100 transition-opacity" title="Close">
                  <X size={20} />
                </button>
              </div>
              <div className="text-xs uppercase tracking-wide font-bold opacity-80 mb-3">{activeDrawer} fields</div>
              <DrawerContent dimensionKey={activeDrawer} />
            </motion.div>
          )}
        </AnimatePresence>

        {/* Value Panel — TrieValuePanel with near-dup detection */}
        <AnimatePresence>
          {activeDrawer && activeField && (
            <div className="absolute top-0 bottom-0 z-30 overflow-auto" style={{ left: 112 + 320 }}>
            <TrieValuePanel
              activeDrawer={activeDrawer}
              activeField={activeField}
              activeFieldMeta={activeFieldMeta}
              activeFieldType={activeFieldType}
              schema={activeSchema}
              onAddConstraint={addConstraint}
              onClose={() => setActiveField(null)}
              getPanelClasses={getValuePanelClasses}
              selectedOp={selectedOp}
              setSelectedOp={setSelectedOp}
              opsForType={opsForType}
              resultEntityIds={showResults && results.length > 0
                ? results.map(r => r.id)
                : null}
              activeConstraints={constraints.length > 0 ? constraints : null}
              queryVersion={queryVersion}
              onAggregateSearch={async (entityIds, label, meta) => {
                if (!entityIds || entityIds.length === 0) {
                  alert(`No entities found matching that count filter.`);
                  return;
                }
                setLoading(true);
                setShowResults(true);
                setDiffInspectLabel(`${label} (${entityIds.length.toLocaleString()} entities)`);
                setResults([]);
                setActiveField(null);
                try {
                  const body = { entity_ids: entityIds.slice(0, 200), schema: activeSchema };
                  const r = await fetch(`${API_URL}/hydrate`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(body),
                  });
                  const d = await r.json();
                  setResults(Array.isArray(d.results) ? d.results : []);
                  setQueryStats({
                    row_count: entityIds.length,
                    probe_ms: null, execution_ms: null, total_ms: null,
                    trace: [], portolan_order: [],
                  });
                } catch { alert("Aggregate search failed."); }
                finally { setLoading(false); }
              }}
            />
            </div>
          )}
        </AnimatePresence>

        {/* Results / Load panel */}
        <div className="flex-1 p-8 overflow-auto" style={{ marginLeft: 640 }}>

          {/* ── Load panel — P7 + Phase 2 ── */}
          {showLoadPanel && (
            <div>
              <div className="flex justify-between items-center mb-4">
                <h2 className="text-xl font-semibold">Saved queries & sets</h2>
                <div className="flex items-center gap-3">
                  {/* Load .peirce file from disk */}
                  <label
                    className="text-xs px-3 py-1.5 rounded border border-blue-300 text-blue-600 hover:bg-blue-50 cursor-pointer transition-colors"
                    title="Load a .peirce file shared by a colleague"
                  >
                    ↑ Load .peirce
                    <input
                      type="file"
                      accept=".json,.peirce"
                      onChange={handleLoadRsetFile}
                      className="hidden"
                    />
                  </label>
                  <button onClick={() => setShowLoadPanel(false)} className="text-gray-400 hover:text-gray-600"><X size={20} /></button>
                </div>
              </div>

              {/* ── Saved result sets ── */}
              {savedSets.length > 0 && (
                <div className="mb-6">
                  <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
                    Result sets ({savedSets.length})
                  </div>
                  <div className="space-y-3">
                    {savedSets.map((s) => (
                      <div key={s._id}
                        className="border border-blue-200 rounded-lg p-4 bg-blue-50 group"
                      >
                        <div className="flex justify-between items-start">
                          <div className="flex items-center gap-2">
                            <div className="font-semibold text-blue-900">{s.set_id}</div>
                            {s._source === "file" && (
                              <span className="text-[10px] px-1.5 py-0.5 rounded bg-blue-200 text-blue-700 font-medium">
                                imported
                              </span>
                            )}
                          </div>
                          <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                            <button
                              onClick={() => handleDownloadSet(s)}
                              className="text-xs text-blue-600 hover:text-blue-800 underline"
                              title="Download .peirce"
                            >↓ .peirce</button>
                            <button
                              onClick={(e) => handleDeleteSet(s._id, e)}
                              className="text-red-400 hover:text-red-600"
                              title="Delete saved set"
                            ><Trash2 size={14} /></button>
                          </div>
                        </div>
                        <div className="mt-1.5 bg-blue-900 rounded px-2 py-1.5 font-mono text-xs text-blue-200 whitespace-pre">
                          {s.query.peirce}
                        </div>
                        <div className="mt-2 text-xs text-blue-700 flex flex-wrap gap-3">
                          <span>{s.results.count.toLocaleString()} entities</span>
                          <span>{s.query.substrate_id}</span>
                          <span>captured {new Date(s.results.captured_at).toLocaleDateString()}</span>
                          {s.projection && <span>fields: {s.projection.join(", ")}</span>}
                          {s.sort && <span>sorted by {s.sort.field} {s.sort.dir}</span>}
                        </div>
                        <button
                          onClick={() => handleLoadSetConstraints(s)}
                          className="mt-2 text-xs text-blue-600 hover:text-blue-800 underline"
                        >Re-run query</button>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* ── Saved queries ── */}
              <div>
                {savedSets.length > 0 && (
                  <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
                    Saved queries ({savedQueries.length})
                  </div>
                )}
                {savedQueries.length === 0 ? (
                  <div className="p-8 text-center text-gray-400 border-2 border-dashed border-gray-200 rounded-lg">
                    No saved queries yet. Build a query and click SAVE.
                  </div>
                ) : (
                  <div className="space-y-3">
                    {savedQueries.map((q) => (
                      <div key={q.id}
                        onClick={() => handleLoadQuery(q)}
                        className="border rounded-lg p-4 bg-gray-50 hover:bg-blue-50 hover:border-blue-300 cursor-pointer transition-colors group"
                      >
                        <div className="flex justify-between items-start">
                          <div className="font-semibold text-gray-900 group-hover:text-blue-800">{q.name}</div>
                          <button
                            onClick={(e) => handleDeleteQuery(q.id, e)}
                            className="opacity-0 group-hover:opacity-100 text-red-400 hover:text-red-600 transition-opacity ml-2"
                            title="Delete saved query"
                          >
                            <Trash2 size={14} />
                          </button>
                        </div>
                        <div className="mt-2 bg-gray-900 rounded px-2 py-1.5 font-mono text-xs text-green-400 whitespace-pre">
                          {q.peirce}
                        </div>
                        <div className="mt-2 text-xs text-gray-400">
                          Saved {new Date(q.saved_at).toLocaleDateString()} · {q.substrate_id || q.schema || "unknown"} · {q.constraints?.length} constraint{q.constraints?.length !== 1 ? "s" : ""}
                          {q.query_hash && (
                            <span className="ml-2 font-mono text-gray-300" title={q.query_hash}>
                              #{q.query_hash.slice(0, 8)}
                            </span>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}

          {/* ── Diff panel ── */}
          {showDiffPanel && (
            <div>
              <div className="flex justify-between items-center mb-4">
                <div className="flex items-center gap-4">
                  <h2 className="text-xl font-semibold">Set operations</h2>
                  {/* Operation selector */}
                  <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-1">
                    {[
                      { key: 'diff',      label: 'Diff',      symbol: 'A − B' },
                      { key: 'union',     label: 'Union',     symbol: 'A ∪ B' },
                      { key: 'intersect', label: 'Intersect', symbol: 'A ∩ B' },
                    ].map(op => (
                      <button
                        key={op.key}
                        onClick={() => { setSetOperation(op.key); setDiffResult(null); setSetOpResult(null); }}
                        className={`text-xs px-3 py-1.5 rounded-md transition-colors font-medium ${
                          setOperation === op.key
                            ? 'bg-white shadow-sm text-gray-900'
                            : 'text-gray-500 hover:text-gray-700'
                        }`}
                        title={op.symbol}
                      >
                        {op.label}
                        <span className="ml-1 font-mono opacity-60 text-[10px]">{op.symbol}</span>
                      </button>
                    ))}
                  </div>
                </div>
                <button onClick={() => setShowDiffPanel(false)} className="text-gray-400 hover:text-gray-600"><X size={20} /></button>
              </div>

              {savedSets.length < 2 ? (
                <div className="p-8 text-center text-gray-400 border-2 border-dashed border-gray-200 rounded-lg">
                  Save at least 2 result sets to diff.
                </div>
              ) : (
                <div>
                  {/* Set selectors */}
                  <div className="flex gap-4 mb-4">
                    {["A", "B"].map(label => {
                      const selected = label === "A" ? diffSetA : diffSetB;
                      const setSelected = label === "A" ? setDiffSetA : setDiffSetB;
                      return (
                        <div key={label} className="flex-1">
                          <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1">
                            Set {label}
                          </div>
                          <select
                            value={selected?._id || ""}
                            onChange={e => {
                              const s = savedSets.find(s => String(s._id) === e.target.value) || null;
                              if (label === "A") { setDiffSetA(s); setDiffResult(null); }
                              else               { setDiffSetB(s); setDiffResult(null); }
                            }}
                            className="w-full px-3 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-orange-400"
                          >
                            <option value="">Select a result set…</option>
                            {savedSets.map(s => (
                              <option key={s._id} value={s._id}>
                                {s.set_id} ({s.results.count.toLocaleString()} entities)
                              </option>
                            ))}
                          </select>
                          {selected && (
                            <div className="mt-1 text-xs text-gray-400 font-mono truncate">
                              {selected.query.peirce}
                            </div>
                          )}
                        </div>
                      );
                    })}
                    <div className="flex items-end pb-1">
                      <button
                        onClick={() => computeSetOperation(diffSetA, diffSetB, setOperation)}
                        disabled={!diffSetA || !diffSetB || diffSetA._id === diffSetB._id}
                        className={`px-4 py-2 rounded text-sm font-semibold transition-colors ${
                          diffSetA && diffSetB && diffSetA._id !== diffSetB._id
                            ? "bg-orange-500 text-white hover:bg-orange-600"
                            : "bg-gray-200 text-gray-400 cursor-not-allowed"
                        }`}
                      >
                        {setOperation === 'diff' ? 'Diff' : setOperation === 'union' ? 'Union' : 'Intersect'}
                      </button>
                    </div>
                  </div>

                  {/* Warnings */}
                  {diffResult?.warnings?.length > 0 && (
                    <div className="mb-4 p-3 bg-amber-50 border border-amber-200 rounded text-xs text-amber-800 space-y-1">
                      {diffResult.warnings.map((w, i) => <div key={i}>⚠ {w}</div>)}
                    </div>
                  )}

                  {/* Diff results */}
                  {diffResult && (
                    <div className="space-y-4">

                      {/* Summary bar */}
                      <div className="flex gap-4 text-sm font-mono">
                        <span className="px-3 py-1 bg-red-50 border border-red-200 rounded text-red-700">
                          − {diffResult.onlyA.length} only in {diffSetA.set_id}
                        </span>
                        <span className="px-3 py-1 bg-green-50 border border-green-200 rounded text-green-700">
                          + {diffResult.onlyB.length} only in {diffSetB.set_id}
                        </span>
                        <span className="px-3 py-1 bg-gray-50 border border-gray-200 rounded text-gray-600">
                          = {diffResult.both.length} in both
                        </span>
                      </div>

                      {/* Three groups */}
                      {[
                        { ids: diffResult.onlyA, label: `Only in ${diffSetA.set_id}`, bg: "bg-red-50",   border: "border-red-200",   text: "text-red-800",   btnCls: "border-red-300 text-red-600 hover:bg-red-100"   },
                        { ids: diffResult.onlyB, label: `Only in ${diffSetB.set_id}`, bg: "bg-green-50", border: "border-green-200", text: "text-green-800", btnCls: "border-green-300 text-green-600 hover:bg-green-100" },
                        { ids: diffResult.both,  label: "In both",                    bg: "bg-gray-50",  border: "border-gray-200",  text: "text-gray-700",  btnCls: "border-gray-300 text-gray-500 hover:bg-gray-100"  },
                      ].map(({ ids, label, bg, border, text, btnCls }) => ids.length > 0 && (
                        <div key={label}>
                          <div className="flex items-center justify-between mb-2">
                            <div className={`text-xs font-semibold uppercase tracking-wide ${text}`}>
                              {label} — {ids.length} {ids.length === 1 ? "entity" : "entities"}
                            </div>
                            <button
                              onClick={() => inspectDiffGroup(ids, label)}
                              className={`text-xs px-2 py-0.5 rounded border transition-colors ${btnCls}`}
                              title="Inspect these entities in the result panel"
                            >
                              Inspect →
                            </button>
                          </div>
                          <div className={`rounded border ${bg} ${border} p-3 space-y-1 max-h-48 overflow-auto`}>
                            {ids.map(id => (
                              <div key={id} className={`text-xs font-mono ${text}`}>{id}</div>
                            ))}
                          </div>
                        </div>
                      ))}

                      {/* Load group into query hint + export */}
                      <div className="flex items-center justify-between mt-3 pt-2 border-t border-gray-100">
                        <div className="text-xs text-gray-400">
                          Tip: inspect a group to see full record cards.
                        </div>
                        <div className="flex items-center gap-1">
                          <span className="text-xs text-gray-400 mr-1">Export:</span>
                          <button onClick={() => exportSetOperation('xlsx')}
                            className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-500 hover:border-gray-300 hover:text-gray-700 transition-colors"
                            title="Export diff as XLSX (3 sheets + provenance)">XLSX</button>
                          <button onClick={() => exportSetOperation('json')}
                            className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-500 hover:border-gray-300 hover:text-gray-700 transition-colors"
                            title="Export diff as JSON">JSON</button>
                        </div>
                      </div>
                    </div>
                  )}

                  {/* ── Union / Intersect result ── */}
                  {setOpResult && (
                    <div className="space-y-4">

                      {/* Warnings */}
                      {setOpResult.warnings?.length > 0 && (
                        <div className="p-3 bg-amber-50 border border-amber-200 rounded text-xs text-amber-800 space-y-1">
                          {setOpResult.warnings.map((w, i) => <div key={i}>⚠ {w}</div>)}
                        </div>
                      )}

                      {/* Summary */}
                      <div className="flex items-center gap-3">
                        <span className={`px-3 py-1 rounded border text-sm font-mono font-semibold ${
                          setOpResult.operation === 'union'
                            ? 'bg-purple-50 border-purple-200 text-purple-700'
                            : 'bg-blue-50 border-blue-200 text-blue-700'
                        }`}>
                          {setOpResult.operation === 'union' ? '∪' : '∩'} {setOpResult.ids.length.toLocaleString()} entities
                        </span>
                        <span className="text-sm text-gray-500">{setOpResult.label}</span>
                      </div>

                      {/* Empty state for intersect */}
                      {setOpResult.ids.length === 0 ? (
                        <div className="p-6 text-center text-gray-400 border-2 border-dashed border-gray-200 rounded-lg">
                          <div className="text-2xl mb-2">∅</div>
                          <div className="text-sm">No entities appear in both sets.</div>
                          <div className="text-xs mt-1 opacity-60">The intersection is empty.</div>
                        </div>
                      ) : (
                        <div>
                          <div className="flex items-center justify-between mb-2">
                            <div className={`text-xs font-semibold uppercase tracking-wide ${
                              setOpResult.operation === 'union' ? 'text-purple-700' : 'text-blue-700'
                            }`}>
                              {setOpResult.ids.length} {setOpResult.ids.length === 1 ? 'entity' : 'entities'}
                            </div>
                            <button
                              onClick={() => inspectDiffGroup(setOpResult.ids, setOpResult.label)}
                              className={`text-xs px-2 py-0.5 rounded border transition-colors ${
                                setOpResult.operation === 'union'
                                  ? 'border-purple-300 text-purple-600 hover:bg-purple-50'
                                  : 'border-blue-300 text-blue-600 hover:bg-blue-50'
                              }`}
                            >
                              Inspect →
                            </button>
                          </div>
                          <div className={`rounded border p-3 space-y-1 max-h-64 overflow-auto ${
                            setOpResult.operation === 'union'
                              ? 'bg-purple-50 border-purple-200'
                              : 'bg-blue-50 border-blue-200'
                          }`}>
                            {setOpResult.ids.map(id => (
                              <div key={id} className={`text-xs font-mono ${
                                setOpResult.operation === 'union' ? 'text-purple-800' : 'text-blue-800'
                              }`}>{id}</div>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* Export union/intersect */}
                      {setOpResult.ids.length > 0 && (
                        <div className="flex items-center justify-end gap-1 mt-3 pt-2 border-t border-gray-100">
                          <span className="text-xs text-gray-400 mr-1">Export:</span>
                          <button onClick={() => exportSetOperation('xlsx')}
                            className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-500 hover:border-gray-300 hover:text-gray-700 transition-colors"
                            title="Export as XLSX">XLSX</button>
                          <button onClick={() => exportSetOperation('json')}
                            className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-500 hover:border-gray-300 hover:text-gray-700 transition-colors"
                            title="Export as JSON">JSON</button>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {/* ── Results ── */}
          {showResults && (
            <div>
              <div className="flex justify-between items-center mb-3">
                <h2 className="text-xl font-semibold">
                  {loading ? "Searching…" : diffInspectLabel ? (
                    <span className="flex items-center gap-3">
                      <button
                        onClick={() => { setShowResults(false); setShowDiffPanel(true); setDiffInspectLabel(null); }}
                        className="text-sm px-2 py-1 rounded border border-orange-200 text-orange-500 hover:bg-orange-50 transition-colors font-normal"
                        title="Back to set operations"
                      >
                        ← Set ops
                      </button>
                      <span>
                        <span className="text-gray-400 text-base font-normal mr-2">Inspecting:</span>
                        {diffInspectLabel}
                        <span className="text-gray-400 text-base font-normal ml-2">({results.length} {results.length === 1 ? "entity" : "entities"})</span>
                      </span>
                    </span>
                  ) : (() => {
                    const showing = results.length;
                    const total   = queryStats?.row_count ?? showing;
                    if (total === 0) return "No results";
                    if (showing < total) return `Results (${showing} of ${total.toLocaleString()})`;
                    return `Results (${total.toLocaleString()})`;
                  })()}
                </h2>
                <div className="flex items-center gap-3">
                  {/* Sort control — only shown when results are loaded */}
                  {!loading && results.length > 0 && sortableFields.length > 0 && (
                    <div className="flex items-center gap-1">
                      <select
                        value={sortField || ''}
                        onChange={e => setSortField(e.target.value || null)}
                        className="text-xs px-2 py-1 rounded border border-gray-200 bg-white text-gray-600 focus:outline-none focus:ring-1 focus:ring-blue-400"
                      >
                        <option value=''>Sort by…</option>
                        {sortableFields.map(({ dim, field }) => (
                          <option key={`${dim}:${field}`} value={field}>
                            {dim} · {field.replace(/_/g, ' ')}
                          </option>
                        ))}
                      </select>
                      {sortField && (
                        <button
                          onClick={() => setSortDir(d => d === 'asc' ? 'desc' : 'asc')}
                          className="text-xs px-2 py-1 rounded border border-gray-200 bg-white text-gray-600 hover:border-gray-300 transition-colors font-mono"
                          title={sortDir === 'asc' ? 'Ascending — click to flip' : 'Descending — click to flip'}
                        >
                          {sortDir === 'asc' ? '↑' : '↓'}
                        </button>
                      )}
                    </div>
                  )}
                  {/* Group by control — sits alongside sort */}
                  {!loading && results.length > 0 && sortableFields.length > 0 && (
                    <div className="flex items-center gap-1">
                      <select
                        value={groupByField || ''}
                        onChange={e => {
                          setGroupByField(e.target.value || null);
                          setGroupExpanded(new Set()); // reset expanded state on field change
                        }}
                        className={`text-xs px-2 py-1 rounded border transition-colors bg-white text-gray-600 focus:outline-none focus:ring-1 focus:ring-blue-400 ${
                          groupByField
                            ? 'border-indigo-300 text-indigo-700 bg-indigo-50'
                            : 'border-gray-200'
                        }`}
                      >
                        <option value=''>Group by…</option>
                        {sortableFields.map(({ dim, field }) => (
                          <option key={`${dim}:${field}`} value={field}>
                            {dim} · {field.replace(/_/g, ' ')}
                          </option>
                        ))}
                      </select>
                      {groupByField && (
                        <button
                          onClick={() => { setGroupByField(null); setGroupExpanded(new Set()); }}
                          className="text-xs px-1.5 py-1 rounded border border-indigo-200 text-indigo-400 hover:text-indigo-600 transition-colors"
                          title="Clear grouping"
                        >✕</button>
                      )}
                    </div>
                  )}

                  {/* Field picker toggle — only shown when results are loaded */}
                  {!loading && results.length > 0 && (
                    <button
                      onClick={() => setShowFieldPicker(v => !v)}
                      className={`text-xs px-2 py-1 rounded border transition-colors ${
                        showFieldPicker
                          ? "bg-blue-50 border-blue-300 text-blue-700"
                          : "border-gray-200 text-gray-500 hover:border-gray-300 hover:text-gray-700"
                      }`}
                    >
                      {projectedFields.size > 0 ? `Fields (${projectedFields.size})` : "Fields"}
                    </button>
                  )}

                  {/* Save Set button — Option C: inline with results header */}
                  {!loading && results.length > 0 && (
                    <div className="relative">
                      {showSaveSetDialog ? (
                        <div className="flex items-center gap-1">
                          <input
                            type="text"
                            placeholder="Name this set…"
                            value={saveSetNameInput}
                            onChange={e => setSaveSetNameInput(e.target.value)}
                            onKeyDown={e => {
                              if (e.key === "Enter") handleSaveSet();
                              if (e.key === "Escape") { setShowSaveSetDialog(false); setSaveSetNameInput(""); }
                            }}
                            className="text-xs px-2 py-1 rounded border border-blue-300 focus:outline-none focus:ring-1 focus:ring-blue-400 w-36"
                            autoFocus
                          />
                          <button
                            onClick={handleSaveSet}
                            disabled={!saveSetNameInput.trim()}
                            className={`text-xs px-2 py-1 rounded border transition-colors ${saveSetNameInput.trim() ? "bg-blue-600 text-white border-blue-600 hover:bg-blue-700" : "bg-gray-100 text-gray-400 border-gray-200 cursor-not-allowed"}`}
                          >Save</button>
                          <button
                            onClick={() => { setShowSaveSetDialog(false); setSaveSetNameInput(""); }}
                            className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-400 hover:text-gray-600 transition-colors"
                          >✕</button>
                        </div>
                      ) : (
                        <button
                          onClick={() => setShowSaveSetDialog(true)}
                          className="text-xs px-2 py-1 rounded border border-blue-200 text-blue-600 hover:border-blue-400 hover:bg-blue-50 transition-colors"
                          title="Save this result set for workbench operations"
                        >Save Set</button>
                      )}
                    </div>
                  )}

                  {/* Selection status bar — shown when results are loaded */}
                  {!loading && sortedResults.length > 0 && (
                    <div className="flex items-center gap-2 text-xs text-gray-400">
                      {selectedIds.size > 0 ? (
                        <>
                          <span className="text-blue-600 font-medium">{selectedIds.size} selected</span>
                          <button onClick={selectAll} className="hover:text-gray-600 underline">all {sortedResults.length}</button>
                          <button onClick={clearSelection} className="hover:text-gray-600 underline">clear</button>
                        </>
                      ) : (
                        <button onClick={selectAll} className="hover:text-gray-600 underline">select all</button>
                      )}
                    </div>
                  )}

                  {/* Export controls — only shown when results are loaded */}
                  {!loading && sortedResults.length > 0 && (
                    <div className="flex items-center gap-1">
                      <button
                        onClick={exportCSV}
                        className={`text-xs px-2 py-1 rounded border transition-colors
                          ${selectedIds.size > 0
                            ? 'border-blue-300 text-blue-600 hover:border-blue-400 hover:text-blue-700'
                            : 'border-gray-200 text-gray-500 hover:border-gray-300 hover:text-gray-700'}`}
                        title={selectedIds.size > 0 ? `Export ${selectedIds.size} selected as CSV` : 'Export as CSV'}
                      >CSV</button>
                      <button
                        onClick={exportXLSX}
                        className={`text-xs px-2 py-1 rounded border transition-colors
                          ${selectedIds.size > 0
                            ? 'border-blue-300 text-blue-600 hover:border-blue-400 hover:text-blue-700'
                            : 'border-gray-200 text-gray-500 hover:border-gray-300 hover:text-gray-700'}`}
                        title={selectedIds.size > 0 ? `Export ${selectedIds.size} selected as XLSX` : 'Export as XLSX (Sheet 1: results, Sheet 2: query record)'}
                      >XLSX</button>
                      <button
                        onClick={exportJSON}
                        className={`text-xs px-2 py-1 rounded border transition-colors
                          ${selectedIds.size > 0
                            ? 'border-blue-300 text-blue-600 hover:border-blue-400 hover:text-blue-700'
                            : 'border-gray-200 text-gray-500 hover:border-gray-300 hover:text-gray-700'}`}
                        title={selectedIds.size > 0 ? `Export ${selectedIds.size} selected as JSON` : 'Export as JSON'}
                      >JSON</button>
                      <button
                        onClick={exportParquet}
                        className={`text-xs px-2 py-1 rounded border transition-colors
                          ${selectedIds.size > 0
                            ? 'border-blue-300 text-blue-600 hover:border-blue-400 hover:text-blue-700'
                            : 'border-gray-200 text-gray-500 hover:border-gray-300 hover:text-gray-700'}`}
                        title={selectedIds.size > 0 ? `Export ${selectedIds.size} selected as Parquet` : 'Export as Parquet'}
                      >Parquet</button>
                    </div>
                  )}
                  {/* Load more — visible when results are paginated */}
                  {!loading && queryStats && results.length < (queryStats.row_count ?? 0) && (
                    <button
                      onClick={async () => {
                        const nextOffset = pageOffset + results.length;
                        setLoading(true);
                        try {
                          const body = {
                            constraints,
                            schema: activeSchema,
                            offset: nextOffset,
                          };
                          if (projectedFields.size > 0) body.fields = Array.from(projectedFields);
                          const r = await fetch(`${API_URL}/query`, {
                            method: "POST",
                            headers: { "Content-Type": "application/json" },
                            body: JSON.stringify(body),
                          });
                          const d = await r.json();
                          // Append new results to existing
                          setResults(prev => [...prev, ...(Array.isArray(d.results) ? d.results : [])]);
                          setPageOffset(nextOffset);
                          setQueryStats(prev => ({ ...prev, row_count: d.row_count }));
                        } catch { console.error("Load more failed"); }
                        finally { setLoading(false); }
                      }}
                      className="text-xs px-2 py-1 rounded border border-blue-200 text-blue-600 hover:bg-blue-50 transition-colors"
                    >
                      Load more ({((queryStats.row_count ?? 0) - results.length).toLocaleString()} remaining)
                    </button>
                  )}
                </div>
              </div>

              {/* Field picker — shown when toggled */}
              {showFieldPicker && <FieldPicker />}


              {/* Query stats — probe / execution / total */}
              {!loading && queryStats && (
                <div className="mb-4 font-mono text-xs border-b border-gray-100 pb-3">
                  {/* Timing row */}
                  <div className="flex flex-wrap gap-4 text-gray-500">
                    <span>
                      <span className="text-gray-400 uppercase tracking-wide mr-1">probe</span>
                      <span className="text-gray-700 font-semibold">{queryStats.probe_ms}ms</span>
                    </span>
                    <span>
                      <span className="text-gray-400 uppercase tracking-wide mr-1">exec</span>
                      <span className="text-gray-700 font-semibold">{queryStats.execution_ms}ms</span>
                    </span>
                    <span>
                      <span className="text-gray-400 uppercase tracking-wide mr-1">total</span>
                      <span className="text-gray-700 font-semibold">{queryStats.total_ms}ms</span>
                    </span>
                  </div>
                  {/* Portolan stepdown — Query trace */}
                  {queryStats.trace.length > 0 && (
                    <div className="mt-2 space-y-0.5">
                      <div className="text-gray-400 uppercase tracking-wide text-xs mb-1">Query trace</div>
                      {queryStats.trace.map((t, i) => {
                        const isAnchor  = i === 0;
                        const isZero    = t.cardinality === 0;
                        const prevCard  = i > 0 ? queryStats.trace[i - 1].cardinality : null;
                        const reduction = prevCard !== null && prevCard > 0
                          ? Math.round((1 - t.cardinality / prevCard) * 100)
                          : null;
                        return (
                          <div key={i} className="flex items-baseline gap-2">
                            {/* Step index */}
                            <span className="text-gray-300 w-3 text-right flex-shrink-0">{i + 1}</span>
                            {/* Dimension */}
                            <span className="font-bold w-10 flex-shrink-0" style={{ color: DIM_ACCENT[t.dimension] || '#6b7280' }}>
                              {t.dimension}
                            </span>
                            {/* Fields + values */}
                            <span className="text-gray-500 mr-2">
                              {(t.fields || []).map((f, fi) => (
                                <span key={fi}>
                                  {fi > 0 && <span className="text-gray-300 mx-1">·</span>}
                                  <span className="text-gray-400">{f.field}</span>
                                  {f.values && f.values.length <= 3
                                    ? <span className="ml-1 text-gray-600">{f.values.map(v => `"${v}"`).join(' or ')}</span>
                                    : <span className="ml-1 text-gray-400">({f.values?.length} values)</span>
                                  }
                                </span>
                              ))}
                              {isAnchor && <span className="ml-2 text-blue-400">anchor</span>}
                            </span>
                            {/* Cardinality */}
                            <span className={`font-semibold flex-shrink-0 ${isZero ? 'text-red-500' : 'text-gray-700'}`}>
                              {isZero ? '0' : t.cardinality?.toLocaleString()}
                            </span>
                            {/* Reduction */}
                            {reduction !== null && reduction > 0 && !isZero && (
                              <span className="text-gray-300 flex-shrink-0">−{reduction}%</span>
                            )}
                            {isZero && <span className="text-red-400 flex-shrink-0">✗</span>}
                          </div>
                        );
                      })}
                    </div>
                  )}
                  {/* Scanned N of M — the warehouse pitch line */}
                  {queryStats.row_count != null && (() => {
                    const scanned  = queryStats.row_count;
                    const total    = apiStatus?.statistics?.total_entities;
                    if (!total || total === 0) return null;
                    const skipped  = total - scanned;
                    const pct      = Math.round((skipped / total) * 100);
                    return (
                      <div className="mt-2 pt-2 border-t border-gray-100 text-gray-400">
                        <span className="text-gray-500 font-semibold">{scanned.toLocaleString()}</span>
                        {" of "}
                        <span>{total.toLocaleString()}</span>
                        {" entities matched"}
                        {pct > 0 && (
                          <span className="ml-2 text-green-600 font-semibold">{pct}% skipped</span>
                        )}
                      </div>
                    );
                  })()}
                </div>
              )}
              {loading ? (
                <div className="flex items-center justify-center h-64">
                  <div className="text-gray-500">Querying SNF database…</div>
                </div>
              ) : results.length === 0 ? (
                <div className="p-8 text-center text-gray-500 border-2 border-dashed border-gray-300 rounded-lg">
                  No entities match your constraints. Try adjusting your search.
                </div>
              ) : groupedResults ? (
                /* ── Grouped view ── */
                <div className="space-y-3">
                  {[...groupedResults.entries()].map(([key, group]) => {
                    const isExpanded = groupExpanded.has(key);
                    const dimColors = {
                      WHO: 'bg-blue-50 border-blue-200 text-blue-800',
                      WHAT: 'bg-purple-50 border-purple-200 text-purple-800',
                      WHEN: 'bg-green-50 border-green-200 text-green-800',
                      WHERE: 'bg-amber-50 border-amber-200 text-amber-800',
                      WHY: 'bg-rose-50 border-rose-200 text-rose-800',
                      HOW: 'bg-slate-50 border-slate-200 text-slate-800',
                    };
                    const colorCls = dimColors[group.dim] || 'bg-gray-50 border-gray-200 text-gray-800';
                    return (
                      <div key={key} className="border border-gray-200 rounded-lg overflow-hidden">
                        {/* Group header */}
                        <button
                          onClick={() => toggleGroup(key)}
                          className="w-full flex items-center justify-between px-4 py-3 bg-white hover:bg-gray-50 transition-colors text-left"
                        >
                          <div className="flex items-center gap-3">
                            <span className={`text-xs font-semibold px-2 py-0.5 rounded border ${colorCls}`}>
                              {group.dim || ''} {group.field.replace(/_/g, ' ')}
                            </span>
                            <span className="font-semibold text-gray-900">{key}</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <span className="text-xs text-gray-400 tabular-nums">
                              {group.items.length} {group.items.length === 1 ? 'entity' : 'entities'}
                            </span>
                            <span className="text-gray-400 text-sm">{isExpanded ? '▲' : '▼'}</span>
                          </div>
                        </button>
                        {/* Group items */}
                        {isExpanded && (
                          <div className="border-t border-gray-100 p-3 space-y-3 bg-gray-50">
                            {group.items.map((item, idx) => (
                              <ResultCard
                                key={item.id ?? idx}
                                item={item}
                                schema={activeSchema}
                                idx={idx}
                                projectedFields={projectedFields.size > 0 ? projectedFields : null}
                                selected={selectedIds.has(item.id)}
                                onToggle={toggleSelected}
                                headerPrefs={headerPrefs}
                                onPinHeader={handlePinHeader}
                              />
                            ))}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              ) : (
                /* ── Flat card view (default) ── */
                <div className="space-y-4">
                  {sortedResults.map((item, idx) => (
                    <ResultCard
                      key={item.id ?? idx}
                      item={item}
                      schema={activeSchema}
                      idx={idx}
                      projectedFields={projectedFields.size > 0 ? projectedFields : null}
                      selected={selectedIds.has(item.id)}
                      onToggle={toggleSelected}
                      headerPrefs={headerPrefs}
                      onPinHeader={handlePinHeader}
                    />
                  ))}
                </div>
              )}
            </div>
          )}

          {/* ── Empty state ── */}
          {!showResults && !showLoadPanel && !showDiffPanel && (
            <div className="flex items-center justify-center h-full">
              <div className="text-center text-gray-400">
                <FileText size={64} className="mx-auto mb-4 opacity-20" />
                <p className="text-lg">
                  {affordances ? "Pick a dimension and field to add constraints, then SEARCH" : "Loading schema affordances…"}
                </p>
                {savedQueries.length > 0 && (
                  <button onClick={() => setShowLoadPanel(true)} className="mt-4 text-sm text-purple-500 hover:text-purple-700 underline">
                    Load a saved query ({savedQueries.length})
                  </button>
                )}
                {savedSets.length >= 2 && (
                  <button onClick={() => setShowDiffPanel(true)} className="mt-2 text-sm text-orange-500 hover:text-orange-700 underline">
                    Set operations ({savedSets.length} sets available)
                  </button>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
