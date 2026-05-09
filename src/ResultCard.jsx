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
import { ChevronDown, ChevronUp } from 'lucide-react';

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
function extractPrimaryLabel(coordinates, entityId) {
  // 1. Priority WHAT fields — the canonical title/name fields
  const what = coordinates?.WHAT || [];
  const whatPriority = ['title', 'name', 'subject', 'description', 'matter_name', 'matter_id'];
  for (const p of whatPriority) {
    const found = what.find(f => f.field === p);
    if (found) return found.value;
  }

  // 2. First WHO value — partner, timekeeper, artist, author, whoever is the subject
  const who = coordinates?.WHO || [];
  if (who.length > 0) return who[0].value;

  // 3. First WHAT value — something is better than an ID
  if (what.length > 0) return what[0].value;

  // 4. First fact from any populated dimension
  const dimOrder = ['WHEN', 'WHERE', 'WHY', 'HOW'];
  for (const dim of dimOrder) {
    const facts = coordinates?.[dim] || [];
    if (facts.length > 0) return facts[0].value;
  }

  // 5. Last resort — entity ID
  return entityId;
}

// Extract the secondary label.
// Shown below the primary label for additional context.
function extractSecondaryLabel(coordinates, primaryLabel) {
  // If primary came from WHO, show first WHAT instead
  const who = coordinates?.WHO || [];
  const what = coordinates?.WHAT || [];

  // Try priority WHO fields first
  const whoPriority = ['artist', 'author', 'creator', 'attorney', 'person', 'name', 'partner_id', 'timekeeper_id'];
  for (const p of whoPriority) {
    const found = who.find(f => f.field === p);
    if (found && found.value !== primaryLabel) return found.value;
  }

  // First WHO value that isn't the primary label
  for (const f of who) {
    if (f.value !== primaryLabel) return f.value;
  }

  // First WHAT value that isn't the primary label
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
// DimensionSection — one dimension's facts rendered as a row of field: value pairs
// ─────────────────────────────────────────────────────────────────────────────

function DimensionSection({ dim, facts, projectedFields }) {
  if (!facts || facts.length === 0) return null;

  // Filter to projected fields if a projection is active
  const visibleFacts = projectedFields
    ? facts.filter(f => projectedFields.has(f.field))
    : facts;

  if (visibleFacts.length === 0) return null;

  const colors = DIM_COLORS[dim] || {
    bg: 'bg-gray-50', border: 'border-gray-200', text: 'text-gray-800', label: 'text-gray-400'
  };

  // Sort — primary fields first, secondary fields last
  const sorted = [...visibleFacts].sort((a, b) => {
    const aSecondary = SECONDARY_FIELDS.has(a.field) ? 1 : 0;
    const bSecondary = SECONDARY_FIELDS.has(b.field) ? 1 : 0;
    return aSecondary - bSecondary;
  });

  return (
    <div className={`rounded px-2 py-1.5 mb-1 ${colors.bg} ${colors.border} border`}>
      <div className="flex gap-2">
        {/* Dimension label — fixed-width left anchor */}
        <span className={`text-xs font-bold ${colors.text} w-10 flex-shrink-0 pt-0.5`}>{dim}</span>
        {/* Field-value pairs stacked vertically */}
        <div className="flex flex-col gap-0.5 min-w-0">
          {sorted.map((fact, i) => (
            !SECONDARY_FIELDS.has(fact.field) && (
              <div key={i} className="text-xs text-gray-600 flex gap-1 min-w-0">
                <span className={`${colors.label} flex-shrink-0`}>{humanizeField(fact.field)}:</span>
                <span className="text-gray-800 break-words">{clampStr(fact.value, 60)}</span>
              </div>
            )
          ))}
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// CoordinateCard — the generic card
// ─────────────────────────────────────────────────────────────────────────────

function CoordinateCard({ item, projectedFields, selected, onToggle }) {
  const [expanded, setExpanded] = useState(false);

  const coordinates    = item.coordinates || {};
  const primaryLabel   = extractPrimaryLabel(coordinates, item.id);
  const secondaryLabel = extractSecondaryLabel(coordinates, primaryLabel);
  const imageUrl       = extractImageUrl(coordinates);

  const presentDims   = DIM_ORDER.filter(d => coordinates[d] && coordinates[d].length > 0);
  const primaryDims   = presentDims.filter(d => ['WHO', 'WHAT', 'WHEN', 'WHERE'].includes(d));
  const secondaryDims = presentDims.filter(d => ['WHY', 'HOW'].includes(d));

  return (
    <div className={`border rounded-lg p-4 bg-white shadow-sm hover:shadow-md transition-shadow
      ${selected ? 'ring-2 ring-blue-400 border-blue-300' : ''}`}>

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
          <DimensionSection key={dim} dim={dim} facts={coordinates[dim]} projectedFields={projectedFields} />
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
                <DimensionSection key={dim} dim={dim} facts={coordinates[dim]} projectedFields={projectedFields} />
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

export default function ResultCard({ item, schema, idx, projectedFields, selected, onToggle }) {
  if (item.coordinates) {
    return (
      <CoordinateCard
        item={item}
        projectedFields={projectedFields}
        selected={selected}
        onToggle={onToggle}
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
