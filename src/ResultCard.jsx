/**
 * ResultCard.jsx
 *
 * Generic SNF coordinate card — works for any domain, any substrate.
 *
 * The server returns coordinates, not schema-specific display objects.
 * This card renders whatever coordinates it receives.
 * It knows nothing about DMS, legal, Discogs, Magic cards, or libraries.
 * Domain knowledge lives in the lens, not in this component.
 *
 * Item shape from Python API (Option B):
 * {
 *   id: "DiscogID:1157990",
 *   coordinates: {
 *     WHO:   [{ field: "artist",  value: "Miles Davis", coordinate: "WHO|artist|Miles Davis" }],
 *     WHAT:  [{ field: "title",   value: "Kind of Blue", coordinate: "WHAT|title|Kind of Blue" }],
 *     WHEN:  [{ field: "released", value: "1959",        coordinate: "WHEN|released|1959" }],
 *     WHERE: [{ field: "label",   value: "Columbia",     coordinate: "WHERE|label|Columbia" }],
 *   },
 *   matched_because: [
 *     { dimension: "WHO", field: "artist", value: "Miles Davis",
 *       coordinate: "WHO|artist|Miles Davis", matched: true }
 *   ]
 * }
 *
 * Props:
 *   item   — result object from Python API
 *   schema — current substrate name (not used for display logic — just passed through)
 */

import React, { useState } from 'react';
import { ChevronDown, ChevronUp, Heading1, Heading2 } from 'lucide-react';

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

const DIM_COLORS = {
  WHO:   { bg: 'bg-blue-50',   border: 'border-blue-200',  text: 'text-blue-800',   label: 'text-blue-500'  },
  WHAT:  { bg: 'bg-purple-50', border: 'border-purple-200',text: 'text-purple-800', label: 'text-purple-500'},
  WHEN:  { bg: 'bg-green-50',  border: 'border-green-200', text: 'text-green-800',  label: 'text-green-500' },
  WHERE: { bg: 'bg-amber-50',  border: 'border-amber-200', text: 'text-amber-800',  label: 'text-amber-500' },
  WHY:   { bg: 'bg-rose-50',   border: 'border-rose-200',  text: 'text-rose-800',   label: 'text-rose-500'  },
  HOW:   { bg: 'bg-slate-50',  border: 'border-slate-200', text: 'text-slate-800',  label: 'text-slate-500' },
};

const DIM_ORDER = ['WHO', 'WHAT', 'WHEN', 'WHERE', 'WHY', 'HOW'];

// Fields that are less useful to show prominently — shown last or hidden
const SECONDARY_FIELDS = new Set(['release_id', 'collection_folder', 'rating']);

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function humanizeField(field) {
  return String(field)
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

function clampStr(s, n = 60) {
  const t = String(s ?? '');
  return t.length > n ? t.slice(0, n - 1) + '…' : t;
}

// Extract the primary label for the card header.
// Looks for title, name, or subject in WHAT — falls back to entity ID.
function extractPrimaryLabel(coordinates, entityId, headerPrefs) {
  // 1. Pinned preference — substrate-level override
  const pin = headerPrefs?.primary;
  if (pin) {
    const facts = coordinates?.[pin.dim] || [];
    const match = facts.find(f => f.field === pin.field);
    if (match) return match.value;
  }

  // 2. Default: WHAT name/title first, then WHO
  // WHO is typically a person or source — WHAT.name is usually the entity's own name.
  // Checking WHAT first prevents source attribution from becoming the card title.
  const what = coordinates?.WHAT || [];
  const whatPriority = ['title', 'name', 'subject', 'description', 'matter_name', 'matter_id'];
  for (const p of whatPriority) {
    const found = what.find(f => f.field === p);
    if (found) return found.value;
  }

  const who = coordinates?.WHO || [];
  if (who.length > 0) return who[0].value;

  if (what.length > 0) return what[0].value;

  for (const dim of ['WHEN', 'WHERE', 'WHY', 'HOW']) {
    const facts = coordinates?.[dim] || [];
    if (facts.length > 0) return facts[0].value;
  }

  return entityId;
}

// Extract the secondary label.
// Shown below the primary label for additional context.
function extractSecondaryLabel(coordinates, primaryLabel, headerPrefs) {
  // Pinned preference
  const pin = headerPrefs?.secondary;
  if (pin) {
    const facts = coordinates?.[pin.dim] || [];
    const match = facts.find(f => f.field === pin.field && f.value !== primaryLabel);
    if (match) return match.value;
  }

  // Default: WHO first (source/attribution), then other WHAT fields
  // Since WHAT.name is now the primary label, WHO makes a natural subtitle.
  const who  = coordinates?.WHO  || [];
  const what = coordinates?.WHAT || [];

  for (const f of who) {
    if (f.value !== primaryLabel) return f.value;
  }
  const whatPriority = ['title', 'name', 'subject', 'matter_name'];
  for (const p of whatPriority) {
    const found = what.find(f => f.field === p);
    if (found && found.value !== primaryLabel) return found.value;
  }
  for (const f of what) {
    if (f.value !== primaryLabel) return f.value;
  }

  return null;
}

// Extract HOW.image_url if present — returns null if not found.
// Only fires for substrates that emit image_url facts (art, film, etc).
// All other substrates are completely unaffected.
function extractImageUrl(coordinates) {
  const how = coordinates?.HOW || [];
  const found = how.find(f => f.field === 'image_url');
  return found ? found.value : null;
}

// ─────────────────────────────────────────────────────────────────────────────
// CoordinatePill — for matched_because section
// Uses pipe-format coordinate: "WHO|artist|Miles Davis"
// ─────────────────────────────────────────────────────────────────────────────

function CoordinatePill({ coordinate, matched = true }) {
  if (!coordinate) return null;

  // Parse pipe format: "WHO|artist|Miles Davis"
  const parts = coordinate.split('|');
  const dim   = parts[0] || '';
  const field = parts[1] || '';
  const value = parts.slice(2).join('|') || '';

  const colors = DIM_COLORS[dim] || {
    bg: 'bg-gray-50', border: 'border-gray-200', text: 'text-gray-800', label: 'text-gray-500'
  };

  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded border text-xs
      ${colors.bg} ${colors.border} ${colors.text} ${matched ? '' : 'opacity-50'}`}>
      <span className="font-semibold mr-1">{dim}</span>
      <span>{humanizeField(field)}: {clampStr(value, 40)}</span>
    </span>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// MatchedOn — "Matched on" pills at the bottom of every card
// ─────────────────────────────────────────────────────────────────────────────

function MatchedOn({ matchedBecause }) {
  if (!matchedBecause || matchedBecause.length === 0) return null;
  return (
    <div className="mt-2 pt-2 border-t border-gray-100">
      <div className="text-xs text-gray-400 mb-1.5">Matched on</div>
      <div className="flex flex-wrap gap-1.5">
        {matchedBecause
          .filter(m => m.coordinate)
          .map((m, i) => (
            <CoordinatePill key={i} coordinate={m.coordinate} matched={m.matched} />
          ))
        }
      </div>
    </div>
  );
}


// ─────────────────────────────────────────────────────────────────────────────
// CorrelatedGroup — renders facts that share an correlation_id as a paired row
// e.g. ingredient + amount side by side instead of separate lines
// ─────────────────────────────────────────────────────────────────────────────

function CorrelatedGroup({ facts, colors, onPinHeader, headerPrefs, dim, hoveredField, setHoveredField }) {
  if (!facts || facts.length === 0) return null;

  // Single fact in group — render normally
  if (facts.length === 1) {
    const fact = facts[0];
    const isPrimary   = headerPrefs?.primary?.dim   === dim && headerPrefs?.primary?.field   === fact.field;
    const isSecondary = headerPrefs?.secondary?.dim === dim && headerPrefs?.secondary?.field === fact.field;
    return (
      <div
        className="text-xs text-gray-600 flex gap-1 min-w-0 items-start"
        onMouseEnter={() => setHoveredField(fact.field)}
        onMouseLeave={() => setHoveredField(null)}
      >
        <span className={`${colors.label} flex-shrink-0`}>{humanizeField(fact.field)}:</span>
        <span className="text-gray-800 break-words flex-1">{clampStr(fact.value, 60)}</span>
        {onPinHeader && hoveredField === fact.field && (
          <span style={{display:'flex', gap:2, flexShrink:0, marginLeft:4}}>
            <button onClick={() => onPinHeader('primary', dim, fact.field)} title="Pin as card title"
              style={{ display:'flex', alignItems:'center', padding:'1px 3px', borderRadius:3, border:'1px solid', cursor:'pointer',
                background: isPrimary ? '#dbeafe' : 'transparent', borderColor: isPrimary ? '#93c5fd' : 'transparent',
                color: isPrimary ? '#1d4ed8' : '#9ca3af' }}
            ><Heading1 size={12} /></button>
            <button onClick={() => onPinHeader('secondary', dim, fact.field)} title="Pin as card subtitle"
              style={{ display:'flex', alignItems:'center', padding:'1px 3px', borderRadius:3, border:'1px solid', cursor:'pointer',
                background: isSecondary ? '#f3e8ff' : 'transparent', borderColor: isSecondary ? '#d8b4fe' : 'transparent',
                color: isSecondary ? '#7c3aed' : '#9ca3af' }}
            ><Heading2 size={12} /></button>
          </span>
        )}
      </div>
    );
  }

  // Multiple facts in group — render as inline pairs: "field: value · field: value"
  // This is the correlated display: ingredient + amount on one line.
  return (
    <div className="text-xs text-gray-600 flex gap-1 min-w-0 items-start flex-wrap">
      {facts.map((fact, i) => (
        <span key={i} className="flex gap-1 items-baseline">
          {i > 0 && <span className="text-gray-300 mx-0.5">·</span>}
          <span className={`${colors.label} flex-shrink-0`}>{humanizeField(fact.field)}:</span>
          <span className="text-gray-800">{clampStr(fact.value, 40)}</span>
        </span>
      ))}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// DimensionSection — one dimension's facts rendered as a row of field: value pairs
// ─────────────────────────────────────────────────────────────────────────────

function DimensionSection({ dim, facts, projectedFields, onPinHeader, headerPrefs }) {
  if (!facts || facts.length === 0) return null;

  const visibleFacts = projectedFields
    ? facts.filter(f => projectedFields.has(f.field))
    : facts.filter(f => !SECONDARY_FIELDS.has(f.field));

  if (visibleFacts.length === 0) return null;

  const colors = DIM_COLORS[dim] || {
    bg: 'bg-gray-50', border: 'border-gray-200', text: 'text-gray-800', label: 'text-gray-400'
  };

  const [hoveredField, setHoveredField] = useState(null);

  // Group facts by correlation_id — correlated facts (e.g. ingredient + amount) share an correlation_id
  // and should render as paired rows. Facts without correlation_id each get their own group.
  const groups = [];
  const groupMap = {};
  for (const fact of visibleFacts) {
    if (fact.correlation_id) {
      if (!groupMap[fact.correlation_id]) {
        groupMap[fact.correlation_id] = [];
        groups.push({ key: fact.correlation_id, facts: groupMap[fact.correlation_id] });
      }
      groupMap[fact.correlation_id].push(fact);
    } else {
      groups.push({ key: `solo_${groups.length}`, facts: [fact] });
    }
  }

  return (
    <div className={`rounded px-2 py-1.5 mb-1 ${colors.bg} ${colors.border} border`}>
      <div className="flex gap-2">
        <span className={`text-xs font-bold ${colors.text} w-10 flex-shrink-0 pt-0.5`}>{dim}</span>
        <div className="flex flex-col gap-0.5 min-w-0 flex-1">
          {groups.map((group) => (
            <CorrelatedGroup
              key={group.key}
              facts={group.facts}
              colors={colors}
              onPinHeader={onPinHeader}
              headerPrefs={headerPrefs}
              dim={dim}
              hoveredField={hoveredField}
              setHoveredField={setHoveredField}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// CoordinateCard — the generic card
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// PloverMeta — web display layer for Plover substrates
// Renders only when entityMeta is present. Inert for all other substrates.
// ─────────────────────────────────────────────────────────────────────────────

function PloverMeta({ entityMeta }) {
  if (!entityMeta) return null;
  const { url, label, description, source_domain, thumbnail_url, provider, date } = entityMeta;
  return (
    <div className="flex gap-3 mb-3 pb-3 border-b border-gray-100">
      {thumbnail_url && (
        <img
          src={thumbnail_url}
          alt={label || ''}
          className="flex-shrink-0 w-16 h-16 object-cover rounded border border-gray-200 bg-gray-100"
          onError={e => { e.currentTarget.style.display = 'none'; }}
        />
      )}
      <div className="flex-1 min-w-0">
        {url ? (
          <a
            href={url}
            target="_blank"
            rel="noopener noreferrer"
            className="font-semibold text-sm text-blue-700 hover:text-blue-900 hover:underline block truncate"
          >
            {label || source_domain || url}
          </a>
        ) : (
          <div className="font-semibold text-sm text-gray-800 truncate">{label}</div>
        )}
        {source_domain && (
          <div className="text-xs text-gray-400 mt-0.5 font-mono">
            {source_domain}
            {provider && provider !== source_domain && (
              <span className="text-gray-300 mx-1">·</span>
            )}
            {provider && provider !== source_domain && <span>{provider}</span>}
            {date && <span className="text-gray-300 mx-1">·</span>}
            {date && <span>{date}</span>}
          </div>
        )}
        {description && (
          <div className="text-xs text-gray-500 mt-1 line-clamp-2">
            {description.replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 180)}
            {description.length > 180 ? '…' : ''}
          </div>
        )}
      </div>
    </div>
  );
}

function CoordinateCard({ item, projectedFields, selected, onToggle, headerPrefs, onPinHeader, entityMeta }) {
  const [expanded, setExpanded] = useState(false);

  const coordinates    = item.coordinates || {};
  const primaryLabel   = extractPrimaryLabel(coordinates, item.id, headerPrefs);
  const secondaryLabel = extractSecondaryLabel(coordinates, primaryLabel, headerPrefs);
  // Use HOW.image_url for non-Plover substrates. Plover uses entityMeta.thumbnail_url via PloverMeta.
  const imageUrl       = entityMeta ? null : extractImageUrl(coordinates);

  const presentDims   = DIM_ORDER.filter(d => coordinates[d] && coordinates[d].length > 0);
  const primaryDims   = presentDims.filter(d => ['WHO', 'WHAT', 'WHEN', 'WHERE'].includes(d));
  const secondaryDims = presentDims.filter(d => ['WHY', 'HOW'].includes(d));

  return (
    <div className={`border rounded-lg p-4 bg-white shadow-sm hover:shadow-md transition-shadow
      ${selected ? 'ring-2 ring-blue-400 border-blue-300' : ''}`}>

      {/* Plover web display layer — inert for non-Plover substrates */}
      <PloverMeta entityMeta={entityMeta} />

      {/* Header — checkbox + thumbnail (if art) + primary label + entity ID */}
      <div className="flex items-start gap-3 mb-3">
        {onToggle && (
          <input
            type="checkbox"
            checked={!!selected}
            onChange={() => onToggle(item.id)}
            className="mt-1 flex-shrink-0 h-4 w-4 rounded border-gray-300 text-blue-600 cursor-pointer accent-blue-600"
            onClick={e => e.stopPropagation()}
          />
        )}
        {/* Thumbnail — only rendered when HOW.image_url is present */}
        {imageUrl && (
          <img
            src={imageUrl}
            alt={primaryLabel}
            className="flex-shrink-0 w-14 h-14 object-cover rounded border border-gray-200 bg-gray-100"
            onError={e => { e.currentTarget.style.display = 'none'; }}
          />
        )}
        <div className="flex-1 flex items-start justify-between gap-4 min-w-0">
          <div className="min-w-0">
            <div className="font-bold text-base text-gray-900 truncate">
              {clampStr(primaryLabel, 70)}
            </div>
            {secondaryLabel && primaryLabel !== secondaryLabel && (
              <div className="text-xs text-gray-500 mt-0.5">
                {clampStr(secondaryLabel, 60)}
              </div>
            )}
          </div>
          <div className="flex-shrink-0 text-xs text-gray-400 font-mono text-right">
            {item.id}
          </div>
        </div>
      </div>

      {/* Primary dimensions */}
      <div className="space-y-1 mb-2">
        {primaryDims.map(dim => (
          <DimensionSection key={dim} dim={dim} facts={coordinates[dim]} projectedFields={projectedFields} onPinHeader={onPinHeader} headerPrefs={headerPrefs} />
        ))}
      </div>

      {/* Expand/collapse for WHY and HOW if present */}
      {secondaryDims.length > 0 && (
        <>
          <button
            onClick={() => setExpanded(e => !e)}
            className="flex items-center gap-1 text-xs text-gray-400 hover:text-gray-600 mt-1 mb-1"
          >
            {expanded ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
            {expanded ? 'less' : `+${secondaryDims.length} more`}
          </button>
          {expanded && (
            <div className="space-y-1 mb-2">
              {secondaryDims.map(dim => (
                <DimensionSection key={dim} dim={dim} facts={coordinates[dim]} projectedFields={projectedFields} onPinHeader={onPinHeader} headerPrefs={headerPrefs} />
              ))}
            </div>
          )}
        </>
      )}

      {/* Matched on */}
      <MatchedOn matchedBecause={item.matched_because} />
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Main export
// ─────────────────────────────────────────────────────────────────────────────

export default function ResultCard({ item, schema, idx, projectedFields, selected, onToggle, headerPrefs, onPinHeader, entityMeta }) {
  if (item.coordinates) {
    return (
      <CoordinateCard
        item={item}
        projectedFields={projectedFields}
        selected={selected}
        onToggle={onToggle}
        headerPrefs={headerPrefs}
        onPinHeader={onPinHeader}
        entityMeta={entityMeta}
      />
    );
  }

  // Fallback for legacy results without coordinates
  return (
    <div className={`border rounded-lg p-4 bg-white shadow-sm
      ${selected ? 'ring-2 ring-blue-400 border-blue-300' : ''}`}>
      <div className="flex items-start gap-3 mb-3">
        {onToggle && (
          <input
            type="checkbox"
            checked={!!selected}
            onChange={() => onToggle(item.id)}
            className="mt-1 h-4 w-4 rounded border-gray-300 text-blue-600 cursor-pointer accent-blue-600"
          />
        )}
        <div className="font-bold text-base font-mono text-gray-700">{item.id}</div>
      </div>
      {item.matched_because && item.matched_because.length > 0 && (
        <div className="mt-2 pt-2 border-t border-gray-100">
          <div className="text-xs text-gray-400 mb-1.5">Matched on</div>
          <div className="flex flex-wrap gap-1.5">
            {item.matched_because
              .filter(m => m.coordinate)
              .map((m, i) => (
                <CoordinatePill key={i} coordinate={m.coordinate} matched={m.matched} />
              ))
            }
          </div>
        </div>
      )}
    </div>
  );
}
