import React, { useState, useEffect, useRef } from 'react';
import { Plus, Trash2, Database, ChevronRight, ChevronDown, BarChart3, LogOut, X, Play, ArrowLeft, Table2, Loader2, Download } from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { Metric, Variation, Page } from './types';
import { handleAuthCallback, getIdToken, decodeJwt, logout, getLoginUrl } from './auth';
import * as api from './api';

const METRIC_ID_RE = /^[a-z][a-z0-9_]{0,19}$/;

// ── helpers ──────────────────────────────────────────────────────────────────

const ChipInput = ({ items, setItems, placeholder }: { items: string[]; setItems: (v: string[]) => void; placeholder: string }) => {
  const [draft, setDraft] = useState('');
  const add = () => { const v = draft.trim(); if (v && !items.includes(v)) setItems([...items, v]); setDraft(''); };
  return (
    <div className="flex flex-wrap gap-1.5 items-center">
      {items.map((item, i) => (
        <span key={i} className="flex items-center gap-1 text-xs bg-slate-100 text-slate-700 px-2 py-1 rounded-lg border border-slate-200">
          {item}
          <button onClick={() => setItems(items.filter((_, j) => j !== i))} className="text-slate-400 hover:text-red-500"><X size={10} /></button>
        </span>
      ))}
      <input type="text" placeholder={placeholder} value={draft}
        className="text-xs px-2 py-1 border border-slate-200 rounded-lg outline-none focus:ring-1 focus:ring-blue-400 min-w-[120px]"
        onChange={(e: React.ChangeEvent<HTMLInputElement>) => setDraft(e.target.value)}
        onKeyDown={(e: React.KeyboardEvent) => { if (e.key === 'Enter') { e.preventDefault(); add(); } }}
        onBlur={add} />
    </div>
  );
};

function downloadCsv(columns: string[], rows: string[][], filename = 'results.csv') {
  const lines = [columns.join(','), ...rows.map(r => r.map(c => `"${String(c).replace(/"/g, '""')}"`).join(','))];
  const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

// ── Sidebar ───────────────────────────────────────────────────────────────────

const Sidebar = ({ currentPage, setCurrentPage, userEmail }: { currentPage: Page; setCurrentPage: (p: Page) => void; userEmail: string }) => (
  <div className="w-64 h-screen glass-panel border-r flex flex-col sticky top-0">
    <div className="p-6">
      <h1 className="text-2xl font-bold tracking-tighter text-blue-600 flex items-center gap-2">
        <div className="w-8 h-8 bg-blue-600 rounded-lg flex items-center justify-center text-white text-lg">S</div>
        Selvy
      </h1>
    </div>
    <nav className="flex-1 px-4 space-y-1">
      {([{ id: 'metrics', label: 'Metrics', icon: Database }, { id: 'analytics', label: 'Analytics', icon: BarChart3 }] as const).map(item => (
        <button key={item.id} onClick={() => setCurrentPage(item.id as Page)}
          className={`w-full flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-200 ${currentPage === item.id ? 'bg-blue-50 text-blue-600 font-semibold shadow-sm' : 'text-slate-500 hover:bg-slate-100 hover:text-slate-900'}`}>
          <item.icon size={20} />{item.label}
        </button>
      ))}
    </nav>
    <div className="p-4 border-t border-slate-100 space-y-2">
      <div className="px-4 py-2 text-xs text-slate-400 truncate">{userEmail}</div>
      <button onClick={logout} className="w-full flex items-center gap-3 px-4 py-2 text-slate-500 hover:text-red-600 hover:bg-red-50 rounded-xl transition-all">
        <LogOut size={18} /> Sign Out
      </button>
    </div>
  </div>
);

// ── Filter detection & template generation ───────────────────────────────────

type FilterMode = 'required' | 'fixed' | 'dynamic';

interface DetectedFilter {
  column: string;
  operator: string;
  value: string;
  clause: string;
  mode: FilterMode;
}

function escapeRe(s: string): string { return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }
function filterKey(column: string): string { return column.replace(/\W/g, '_').toLowerCase(); }
function persistedModeForFilter(f: DetectedFilter, requiredSet: Set<string>, dynamicSet: Set<string>): FilterMode | null {
  const base = filterKey(f.column);
  const reqKey = f.operator === 'BETWEEN_START' ? `${base}_start` : (f.operator === 'BETWEEN_END' ? `${base}_end` : base);
  if (requiredSet.has(reqKey)) return 'required';
  if (dynamicSet.has(base)) return 'dynamic';
  return null;
}

/** Parse WHERE conditions from real SQL into filter descriptors */
function detectFilters(sql: string): DetectedFilter[] {
  const filters: DetectedFilter[] = [];
  const whereMatch = sql.match(/\bWHERE\s+([\s\S]*?)(?=\s+GROUP\s+|\s+ORDER\s+|\s+LIMIT\s+|\s+HAVING\s+|$)/i);
  if (!whereMatch) return filters;

  let whereClause = whereMatch[1];

  // Step 1: Extract BETWEEN clauses first (they contain AND that isn't a separator)
  const betweenRe = /(\S[\s\S]*?\bBETWEEN\b[\s\S]*?\bAND\b[\s\S]*?)(?=\s+AND\s+|\s*$)/gi;
  const betweens: { full: string; col: string; v1: string; v2: string }[] = [];
  // Use a more specific regex for CAST(...) BETWEEN CAST(...) AND CAST(...)
  const castBetweenRe = /(CAST\s*\([^)]+\)\s+BETWEEN\s+CAST\s*\([^)]+\)\s+AND\s+CAST\s*\([^)]+\))/gi;
  let cbMatch;
  while ((cbMatch = castBetweenRe.exec(whereClause)) !== null) {
    const full = cbMatch[1];
    const inner = full.match(/^(CAST\s*\((.+?)\s+AS\s+\w+\s*\))\s+BETWEEN\s+CAST\s*\((.+?)\s+AS\s+\w+\s*\)\s+AND\s+CAST\s*\((.+?)\s+AS\s+\w+\s*\)$/i);
    if (inner) {
      betweens.push({ full, col: inner[2].trim(), v1: inner[3].trim().replace(/^'|'$/g, ''), v2: inner[4].trim().replace(/^'|'$/g, '') });
    }
  }

  // Also handle simple BETWEEN (no CAST): col BETWEEN 'v1' AND 'v2'
  const simpleBetweenRe = /(\b([A-Za-z_]\w*)\s+BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)')/gi;
  let sbMatch;
  while ((sbMatch = simpleBetweenRe.exec(whereClause)) !== null) {
    // Skip if already captured by CAST version
    if (!betweens.some(b => b.full.includes(sbMatch[1]))) {
      betweens.push({ full: sbMatch[1], col: sbMatch[2], v1: sbMatch[3], v2: sbMatch[4] });
    }
  }

  // Replace BETWEEN clauses with placeholders before splitting on AND
  let masked = whereClause;
  betweens.forEach((b, i) => { masked = masked.replace(b.full, `__BETWEEN_${i}__`); });

  // Step 2: Split remaining on AND
  const rawParts = masked.split(/\s+AND\s+/i).map(s => s.trim()).filter(Boolean);

  // Step 3: Process each part
  for (const raw of rawParts) {
    // Restore BETWEEN placeholder
    const betweenIdx = raw.match(/^__BETWEEN_(\d+)__$/);
    if (betweenIdx) {
      const b = betweens[Number(betweenIdx[1])];
      if (b) {
        filters.push({ column: b.col, operator: 'BETWEEN_START', value: b.v1, clause: b.full, mode: 'required' });
        filters.push({ column: b.col, operator: 'BETWEEN_END', value: b.v2, clause: '', mode: 'required' });
      }
      continue;
    }

    const part = raw.trim();
    if (!part || part.includes('{{')) continue;

    // Simple: col op value
    const sm = part.match(/^(.+?)\s+(=|!=|<>|>=?|<=?|LIKE)\s+(.+)$/i);
    if (sm) {
      const val = sm[3].trim().replace(/^'|'$/g, '');
      filters.push({ column: sm[1].trim(), operator: sm[2].trim(), value: val, clause: part, mode: 'fixed' });
      continue;
    }

    // IN
    const im = part.match(/^(.+?)\s+IN\s*\((.+)\)$/i);
    if (im) {
      // Strip surrounding quotes from each value in the IN list
      const rawValues = im[2].trim();
      const stripped = rawValues.replace(/'/g, '').trim();
      filters.push({ column: im[1].trim(), operator: 'IN', value: stripped, clause: part, mode: 'fixed' });
    }
  }
  return filters;
}

/** Generate template SQL from example SQL + filter toggle decisions */
function buildTemplateSql(exampleSql: string, filters: DetectedFilter[]): string {
  let sql = exampleSql;
  const hasDynamic = filters.some(f => f.mode === 'dynamic');

  // Process BETWEEN pairs
  for (let i = 0; i < filters.length; i++) {
    const f = filters[i];
    if (f.operator !== 'BETWEEN_START') continue;
    const f2 = (i + 1 < filters.length && filters[i + 1].operator === 'BETWEEN_END') ? filters[i + 1] : null;
    if (!f2 || f.mode !== f2.mode) continue;

    if (f.mode === 'required') {
      const pStart = f.column.replace(/\W/g, '_').toLowerCase() + '_start';
      const pEnd = f.column.replace(/\W/g, '_').toLowerCase() + '_end';
      // Replace values inside the BETWEEN clause
      let newClause = f.clause;
      newClause = newClause.replace(new RegExp("'" + escapeRe(f.value) + "'", 'g'), `':${pStart}'`);
      newClause = newClause.replace(new RegExp("'" + escapeRe(f2.value) + "'", 'g'), `':${pEnd}'`);
      // Also handle unquoted
      newClause = newClause.replace(new RegExp('\\b' + escapeRe(f.value) + '\\b', 'g'), `:${pStart}`);
      newClause = newClause.replace(new RegExp('\\b' + escapeRe(f2.value) + '\\b', 'g'), `:${pEnd}`);
      sql = sql.replace(f.clause, newClause);
    } else if (f.mode === 'dynamic') {
      // Remove the BETWEEN clause
      sql = sql.replace(new RegExp('\\s*AND\\s*' + escapeRe(f.clause), 'i'), '');
      sql = sql.replace(new RegExp(escapeRe(f.clause) + '\\s*AND\\s*', 'i'), '');
    }
    i++; // skip the END pair
    continue;
  }

  // Process simple filters
  for (const f of filters) {
    if (f.operator === 'BETWEEN_START' || f.operator === 'BETWEEN_END') continue;
    if (f.mode === 'required') {
      const pName = ':' + f.column.replace(/\W/g, '_').toLowerCase();
      if (f.operator === 'IN') {
        // For IN clauses, rebuild as col = :param (single required value)
        const newClause = `${f.column} = ${pName}`;
        sql = sql.replace(f.clause, newClause);
      } else {
        // Replace the value in the clause
        sql = sql.replace(f.clause, f.clause.replace(new RegExp("'" + escapeRe(f.value) + "'", 'g'), pName).replace(new RegExp('\\b' + escapeRe(f.value) + '\\b'), pName));
      }
    } else if (f.mode === 'dynamic') {
      sql = sql.replace(new RegExp('\\s+AND\\s+' + escapeRe(f.clause), 'i'), '');
      sql = sql.replace(new RegExp(escapeRe(f.clause) + '\\s+AND\\s+', 'i'), '');
      sql = sql.replace(f.clause, '');
    }
  }

  // Inject {{dynamic_filters}} if needed
  if (hasDynamic && !sql.includes('{{dynamic_filters}}')) {
    const pt = sql.search(/\s+(GROUP|ORDER|LIMIT|HAVING)\s+/i);
    if (pt > 0) sql = sql.slice(0, pt) + '{{dynamic_filters}}' + sql.slice(pt);
    else sql += '{{dynamic_filters}}';
  }

  return sql;
}

// ── Filter toggle UI ─────────────────────────────────────────────────────────

const MODE_STYLES: Record<FilterMode, { bg: string; text: string; label: string }> = {
  required: { bg: 'bg-red-50 border-red-200', text: 'text-red-700', label: 'REQ' },
  fixed: { bg: 'bg-slate-100 border-slate-300', text: 'text-slate-700', label: 'FIX' },
  dynamic: { bg: 'bg-amber-50 border-amber-200', text: 'text-amber-700', label: 'DYN' },
};

const FilterToggle = ({ mode, onChange }: { mode: FilterMode; onChange: (m: FilterMode) => void }) => (
  <div className="flex gap-0.5">
    {(['required', 'fixed', 'dynamic'] as FilterMode[]).map(m => (
      <button key={m} onClick={() => onChange(m)}
        className={`px-2 py-0.5 text-[10px] font-bold rounded border transition-colors ${mode === m ? MODE_STYLES[m].bg + ' ' + MODE_STYLES[m].text : 'bg-white text-slate-400 border-slate-100 hover:border-slate-300'}`}>
        {MODE_STYLES[m].label}
      </button>
    ))}
  </div>
);

// ── Variation Editor ─────────────────────────────────────────────────────────

function emptyVariation(): Variation {
  return { variationId: '', label: '', description: '', sql: '', exampleSql: '', summaryHint: '', outputColumns: [], isCsvEnabled: false, requiredFilters: [] };
}

const VariationEditor = ({ metric, onBack, onSave }: { metric: Metric; onBack: () => void; onSave: (m: Metric) => void }) => {
  const [variations, setVariations] = useState<Variation[]>(metric.variations);
  const [activeIdx, setActiveIdx] = useState<number | null>(variations.length > 0 ? 0 : null);
  const [tables, setTables] = useState<api.AndesTable[]>([]);
  const [tablesLoading, setTablesLoading] = useState(true);
  const [expandedTable, setExpandedTable] = useState<string | null>(null);
  const [queryResult, setQueryResult] = useState<api.QueryResult | null>(null);
  const [queryRunning, setQueryRunning] = useState(false);
  const [queryError, setQueryError] = useState('');
  const [saving, setSaving] = useState(false);
  const sqlRef = useRef<HTMLTextAreaElement>(null);

  // Per-variation filter toggles (ephemeral UI state, not persisted directly)
  const [allFilters, setAllFilters] = useState<Record<number, DetectedFilter[]>>({});

  useEffect(() => { api.fetchAndesTables().then(setTables).catch(() => setTables([])).finally(() => setTablesLoading(false)); }, []);
  useEffect(() => { setQueryResult(null); setQueryError(''); }, [activeIdx]);

  const active = activeIdx !== null ? variations[activeIdx] : null;
  const activeFilters = activeIdx !== null ? (allFilters[activeIdx] || []) : [];
  const dynamicSet = new Set(Object.keys(metric.filterMap || {}));

  const hydrateFilters = (v: Variation, existing: DetectedFilter[] = []): DetectedFilter[] => {
    const src = v.exampleSql || v.sql || '';
    const detected = detectFilters(src);
    const requiredSet = new Set(v.requiredFilters || []);
    return detected.map(d => {
      const prev = existing.find(e => e.column === d.column && e.operator === d.operator);
      if (prev) return { ...d, mode: prev.mode };
      const persisted = persistedModeForFilter(d, requiredSet, dynamicSet);
      return persisted ? { ...d, mode: persisted } : d;
    });
  };

  // Init filters from exampleSql when switching to a variation
  useEffect(() => {
    if (activeIdx === null) return;
    const v = variations[activeIdx];
    if (!v) return;
    if (!allFilters[activeIdx]) {
      setAllFilters(prev => ({ ...prev, [activeIdx]: hydrateFilters(v, prev[activeIdx] || []) }));
    }
  }, [activeIdx, variations, allFilters, metric.filterMap]);

  const updateExampleSql = (sql: string) => {
    if (activeIdx === null) return;
    setVariations(prev => prev.map((v, i) => i === activeIdx ? { ...v, exampleSql: sql } : v));
    const current = variations[activeIdx];
    const existing = allFilters[activeIdx] || [];
    const merged = current ? hydrateFilters({ ...current, exampleSql: sql }, existing) : [];
    setAllFilters(prev => ({ ...prev, [activeIdx]: merged }));
  };

  const setFilterMode = (fi: number, mode: FilterMode) => {
    if (activeIdx === null) return;
    setAllFilters(prev => ({ ...prev, [activeIdx]: (prev[activeIdx] || []).map((f, i) => i === fi ? { ...f, mode } : f) }));
  };

  const updateActive = (p: Partial<Variation>) => {
    if (activeIdx === null) return;
    setVariations(prev => prev.map((v, i) => i === activeIdx ? { ...v, ...p } : v));
  };

  const addVariation = () => { const next = [...variations, emptyVariation()]; setVariations(next); setActiveIdx(next.length - 1); };
  const removeVariation = (idx: number) => { const next = variations.filter((_, i) => i !== idx); setVariations(next); setActiveIdx(next.length > 0 ? Math.min(idx, next.length - 1) : null); };

  const insertAtCursor = (text: string) => {
    const el = sqlRef.current;
    if (!el || !active) return;
    const s = el.selectionStart, e = el.selectionEnd;
    const next = (active.exampleSql || '').substring(0, s) + text + (active.exampleSql || '').substring(e);
    updateExampleSql(next);
    setTimeout(() => { el.focus(); el.setSelectionRange(s + text.length, s + text.length); }, 0);
  };

  const runSql = async () => {
    if (!active?.exampleSql?.trim()) return;
    setQueryRunning(true); setQueryError(''); setQueryResult(null);
    try { setQueryResult(await api.runQuery(active.exampleSql)); }
    catch (e: unknown) { setQueryError(e instanceof Error ? e.message : String(e)); }
    finally { setQueryRunning(false); }
  };

  const save = async () => {
    setSaving(true);
    try {
      const finalVariations = variations.map((v, idx) => {
        const filters = allFilters[idx] || hydrateFilters(v);
        const templateSql = filters.length > 0 ? buildTemplateSql(v.exampleSql || v.sql, filters) : (v.exampleSql || v.sql);
        const reqFilters = filters.filter(f => f.mode === 'required').map(f => f.column.replace(/\W/g, '_').toLowerCase());
        // Dedupe: BETWEEN produces _start and _end
        const reqSet = new Set<string>();
        for (const f of filters) {
          if (f.mode !== 'required') continue;
          if (f.operator === 'BETWEEN_START') reqSet.add(f.column.replace(/\W/g, '_').toLowerCase() + '_start');
          else if (f.operator === 'BETWEEN_END') reqSet.add(f.column.replace(/\W/g, '_').toLowerCase() + '_end');
          else reqSet.add(f.column.replace(/\W/g, '_').toLowerCase());
        }
        return { ...v, sql: templateSql, requiredFilters: [...reqSet] };
      });
      // Build filterMap from DYNAMIC filters
      const dynMap: Record<string, string> = {};
      const nonDynKeys = new Set<string>();
      for (const idx of Object.keys(allFilters)) {
        for (const f of allFilters[Number(idx)] || []) {
          const key = f.column.replace(/\W/g, '_').toLowerCase();
          if (f.mode === 'dynamic') dynMap[key] = f.column;
          else nonDynKeys.add(key);
        }
      }
      const mergedFilterMap = { ...metric.filterMap };
      // Remove filters that are now required/fixed (they live in the SQL template, not dynamic)
      for (const k of nonDynKeys) { delete mergedFilterMap[k]; }
      for (const [k, col] of Object.entries(dynMap)) { if (!mergedFilterMap[k]) mergedFilterMap[k] = col; }
      const updated = await api.updateMetric(metric.id, { variations: finalVariations, filterMap: mergedFilterMap });
      onSave(updated);
    } catch (e) { console.error(e); } finally { setSaving(false); }
  };

  const lbl = (t: string) => <label className="block text-xs font-medium text-slate-600 mb-1">{t}</label>;
  const cls = "w-full px-3 py-2 rounded-lg border border-slate-200 focus:ring-2 focus:ring-blue-500 outline-none text-sm";

  return (
    <div className="max-w-7xl mx-auto space-y-6 pb-20">
      <div className="flex items-center gap-4">
        <button onClick={onBack} className="flex items-center gap-2 text-sm text-slate-500 hover:text-slate-900 transition-colors"><ArrowLeft size={16} /> Back</button>
        <div>
          <h2 className="text-2xl font-bold text-slate-900">{metric.metricId}</h2>
          <p className="text-sm text-slate-500">{metric.description}</p>
        </div>
        <div className="ml-auto flex gap-3">
          <button onClick={addVariation} className="flex items-center gap-2 px-4 py-2 rounded-lg border border-slate-200 text-sm font-medium text-slate-600 hover:bg-slate-50 transition-colors"><Plus size={15} /> Add Variation</button>
          <button onClick={save} disabled={saving} className="flex items-center gap-2 bg-blue-600 text-white px-5 py-2 rounded-lg text-sm font-semibold hover:bg-blue-700 disabled:opacity-40 transition-colors">
            {saving ? <Loader2 size={15} className="animate-spin" /> : null} Save Variations
          </button>
        </div>
      </div>

      <div className="grid grid-cols-12 gap-6">
        {/* Left: variation tabs + table browser */}
        <div className="col-span-3 space-y-4">
          <div className="glass-panel rounded-2xl overflow-hidden">
            <div className="px-4 py-3 border-b border-slate-100 text-xs font-semibold text-slate-500 uppercase tracking-wide">Variations</div>
            {variations.length === 0 && <p className="px-4 py-4 text-xs text-slate-400">No variations yet.</p>}
            {variations.map((v, idx) => (
              <button key={idx} onClick={() => setActiveIdx(idx)}
                className={`w-full flex items-center justify-between px-4 py-3 text-sm transition-colors border-b border-slate-50 last:border-0 ${activeIdx === idx ? 'bg-blue-50 text-blue-700 font-medium' : 'text-slate-600 hover:bg-slate-50'}`}>
                <span className="truncate">{v.variationId || `Variation ${idx + 1}`}</span>
                <button onClick={(e: React.MouseEvent) => { e.stopPropagation(); removeVariation(idx); }} className="text-slate-300 hover:text-red-500 ml-2 flex-shrink-0"><Trash2 size={13} /></button>
              </button>
            ))}
          </div>
          <div className="glass-panel rounded-2xl overflow-hidden">
            <div className="px-4 py-3 border-b border-slate-100 flex items-center gap-2 text-xs font-semibold text-slate-500 uppercase tracking-wide"><Table2 size={13} /> Andes Tables</div>
            {tablesLoading && <div className="px-4 py-4 flex items-center gap-2 text-xs text-slate-400"><Loader2 size={13} className="animate-spin" /> Loading…</div>}
            <div className="max-h-80 overflow-y-auto">
              {tables.map(t => (
                <div key={t.name}>
                  <button onClick={() => setExpandedTable(expandedTable === t.name ? null : t.name)}
                    className="w-full flex items-center justify-between px-4 py-2.5 text-xs text-slate-700 hover:bg-slate-50 transition-colors border-b border-slate-50">
                    <span className="font-mono truncate">{t.name}</span>
                    {expandedTable === t.name ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                  </button>
                  {expandedTable === t.name && (
                    <div className="bg-slate-50 px-4 py-2 space-y-1">
                      {t.columns.map(col => (
                        <button key={col.name} onClick={() => insertAtCursor(col.name)}
                          className="w-full flex items-center justify-between text-[11px] text-slate-600 hover:text-blue-600 py-0.5 text-left transition-colors">
                          <span className="font-mono">{col.name}</span>
                          <span className="text-slate-400 ml-2">{col.type}</span>
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Right: SQL editor + filters + results */}
        <div className="col-span-9 space-y-4">
          {active ? (
            <>
              <div className="glass-panel p-5 rounded-2xl">
                <div className="grid grid-cols-2 gap-4 mb-4">
                  <div>{lbl('Variation ID')}<input type="text" placeholder="order_volume" className={cls} value={active.variationId} onChange={(e: React.ChangeEvent<HTMLInputElement>) => updateActive({ variationId: e.target.value })} /></div>
                  <div>{lbl('Label')}<input type="text" placeholder="Order volume by marketplace" className={cls} value={active.label} onChange={(e: React.ChangeEvent<HTMLInputElement>) => updateActive({ label: e.target.value })} /></div>
                </div>
                <div className="mb-4">{lbl('Description')}<textarea className={`${cls} h-16 resize-none`} placeholder="What does this variation answer? When should the agent pick it?" value={active.description} onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => updateActive({ description: e.target.value })} /></div>
                <div className="mb-4">{lbl('Summary Hint')}<textarea className={`${cls} h-16 resize-none`} placeholder="Guidance for the LLM on how to present results…" value={active.summaryHint} onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => updateActive({ summaryHint: e.target.value })} /></div>
                <label className="flex items-center gap-3 cursor-pointer">
                  <div className={`w-10 h-5 rounded-full transition-colors relative ${active.isCsvEnabled ? 'bg-blue-600' : 'bg-slate-200'}`}>
                    <div className={`absolute top-0.5 w-4 h-4 bg-white rounded-full transition-all ${active.isCsvEnabled ? 'left-5' : 'left-0.5'}`} />
                  </div>
                  <input type="checkbox" className="hidden" checked={active.isCsvEnabled} onChange={(e: React.ChangeEvent<HTMLInputElement>) => updateActive({ isCsvEnabled: e.target.checked })} />
                  <span className="text-xs font-medium text-slate-600">Enable CSV Export</span>
                </label>
              </div>

              {/* SQL editor — user writes REAL SQL with real values */}
              <div className="glass-panel rounded-2xl overflow-hidden">
                <div className="flex items-center justify-between px-4 py-3 border-b border-slate-100">
                  <span className="text-xs font-semibold text-slate-500 uppercase tracking-wide">Example SQL (real values)</span>
                  <button onClick={runSql} disabled={queryRunning || !(active.exampleSql || '').trim()}
                    className="flex items-center gap-2 px-4 py-1.5 bg-green-600 text-white rounded-lg text-xs font-semibold hover:bg-green-700 disabled:opacity-40 transition-colors">
                    {queryRunning ? <Loader2 size={13} className="animate-spin" /> : <Play size={13} />}
                    {queryRunning ? 'Running…' : 'Run SQL'}
                  </button>
                </div>
                <textarea ref={sqlRef} value={active.exampleSql || ''}
                  onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => updateExampleSql(e.target.value)}
                  className="w-full px-4 py-3 font-mono text-sm bg-slate-950 text-green-300 outline-none resize-none h-48"
                  placeholder={"SELECT marketplace_id, COUNT(DISTINCT order_id) AS order_count\nFROM \"andes\".\"ufg_mena_bi.anow_orders_master\"\nWHERE CAST(order_day AS DATE) BETWEEN CAST('2026-04-01' AS DATE) AND CAST('2026-04-14' AS DATE)\n  AND marketplace_id = '338801'\nGROUP BY marketplace_id"} />
              </div>

              {/* Detected filters with 3-way toggle */}
              {activeFilters.length > 0 && (
                <div className="glass-panel p-4 rounded-2xl">
                  <div className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-3">Detected Filters</div>
                  <div className="space-y-2">
                    {activeFilters.map((f, i) => (
                      <div key={i} className={`flex items-center gap-3 px-3 py-2 rounded-lg border ${MODE_STYLES[f.mode].bg}`}>
                        <span className="font-mono text-xs text-slate-700 flex-1 truncate">
                          {f.column} <span className="text-slate-400">{f.operator === 'BETWEEN_START' ? '>=' : f.operator === 'BETWEEN_END' ? '<=' : f.operator}</span> <span className="text-slate-500">{f.value}</span>
                        </span>
                        <FilterToggle mode={f.mode} onChange={m => setFilterMode(i, m)} />
                      </div>
                    ))}
                  </div>
                  <p className="text-[10px] text-slate-400 mt-2">REQ = user must provide at runtime · FIX = hardcoded in SQL · DYN = optional via Slack</p>
                </div>
              )}

              {queryError && <div className="glass-panel p-4 rounded-2xl border border-red-100 bg-red-50 text-sm text-red-600">{queryError}</div>}

              {queryResult && (
                <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} className="glass-panel rounded-2xl overflow-hidden">
                  <div className="flex items-center justify-between px-4 py-3 border-b border-slate-100">
                    <span className="text-xs font-semibold text-slate-500 uppercase tracking-wide">
                      Results — {queryResult.rows.length} row{queryResult.rows.length !== 1 ? 's' : ''}
                      {queryResult.truncatedColumns && <span className="ml-2 text-amber-500">(columns truncated)</span>}
                    </span>
                    <button onClick={() => downloadCsv(queryResult.columns, queryResult.rows, `${metric.metricId}_${active.variationId || 'results'}.csv`)}
                      className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-medium text-slate-600 hover:bg-slate-50 transition-colors">
                      <Download size={13} /> Download CSV
                    </button>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-xs text-left">
                      <thead className="bg-slate-50 border-b border-slate-100">
                        <tr>{queryResult.columns.map(col => <th key={col} className="px-4 py-2.5 font-semibold text-slate-600 whitespace-nowrap">{col}</th>)}</tr>
                      </thead>
                      <tbody className="divide-y divide-slate-50">
                        {queryResult.rows.map((row, i) => (
                          <tr key={i} className="hover:bg-slate-50/60 transition-colors">
                            {row.map((cell, j) => <td key={j} className="px-4 py-2.5 text-slate-700 whitespace-nowrap font-mono">{cell}</td>)}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </motion.div>
              )}
            </>
          ) : (
            <div className="glass-panel rounded-2xl flex items-center justify-center h-64 text-slate-400 text-sm">Add a variation to get started</div>
          )}
        </div>
      </div>
    </div>
  );
};

// ── Metric metadata form ─────────────────────────────────────────────────────

function emptyMetricDraft(): Omit<Metric, 'id'> {
  return { metricId: '', description: '', dataSource: '', aliases: [], exampleQuestions: [], filterMap: {}, variations: [] };
}

const MetricsPage = ({ metrics, setMetrics, loading, onOpenMetric }: {
  metrics: Metric[]; setMetrics: React.Dispatch<React.SetStateAction<Metric[]>>; loading: boolean; onOpenMetric: (m: Metric) => void;
}) => {
  const [draft, setDraft] = useState<Omit<Metric, 'id'>>(emptyMetricDraft());
  const [editingId, setEditingId] = useState<string | null>(null);
  const [idError, setIdError] = useState('');
  const [saving, setSaving] = useState(false);

  const patch = (p: Partial<Omit<Metric, 'id'>>) => setDraft(prev => ({ ...prev, ...p }));
  const validateId = (v: string) => { setIdError(!v ? '' : METRIC_ID_RE.test(v) ? '' : 'lowercase, digits, underscores; starts with letter; max 20'); };

  const save = async () => {
    if (!draft.metricId || !draft.description || idError) return;
    setSaving(true);
    try {
      if (editingId) {
        const { variations: _, ...meta } = draft;
        const updated = await api.updateMetric(editingId, meta);
        setMetrics(prev => prev.map(m => m.id === editingId ? updated : m));
      } else {
        const created = await api.createMetric(draft);
        setMetrics(prev => [...prev, created]);
      }
      setDraft(emptyMetricDraft()); setEditingId(null);
    } catch (err) { console.error('Save failed', err); } finally { setSaving(false); }
  };

  const startEdit = (m: Metric) => { const { id, ...rest } = m; setDraft(rest); setEditingId(id); };
  const cancelEdit = () => { setDraft(emptyMetricDraft()); setEditingId(null); setIdError(''); };
  const handleDelete = async (id: string) => {
    try { await api.deleteMetric(id); setMetrics(prev => prev.filter(m => m.id !== id)); if (editingId === id) cancelEdit(); } catch (err) { console.error(err); }
  };

  const lbl = (t: string) => <label className="block text-sm font-medium text-slate-700 mb-1">{t}</label>;
  const cls = "w-full px-3 py-2 rounded-lg border border-slate-200 focus:ring-2 focus:ring-blue-500 outline-none text-sm";

  return (
    <div className="max-w-5xl mx-auto space-y-8 pb-20">
      <header>
        <h2 className="text-3xl font-bold text-slate-900">Metrics</h2>
        <p className="text-slate-500 mt-2">Define metrics, then click one to manage its SQL variations.</p>
      </header>
      {loading && <p className="text-sm text-slate-400">Loading metrics…</p>}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        <div className="lg:col-span-2 space-y-6">
          <div className="glass-panel p-6 rounded-2xl space-y-5">
            <div>{lbl('Metric ID')}<input type="text" placeholder="orders_lost" className={cls} value={draft.metricId} onChange={(e: React.ChangeEvent<HTMLInputElement>) => { patch({ metricId: e.target.value }); validateId(e.target.value); }} />{idError && <p className="text-xs text-red-500 mt-1">{idError}</p>}</div>
            <div>{lbl('Description')}<input type="text" placeholder="Number of orders lost due to store unavailability or closure" className={cls} value={draft.description} onChange={(e: React.ChangeEvent<HTMLInputElement>) => patch({ description: e.target.value })} /></div>
            <div>{lbl('Data Source')}<input type="text" placeholder="ufg_mena_bi.anow_orders_master" className={cls} value={draft.dataSource} onChange={(e: React.ChangeEvent<HTMLInputElement>) => patch({ dataSource: e.target.value })} /></div>
            <div>{lbl('Aliases')}<ChipInput items={draft.aliases} setItems={v => patch({ aliases: v })} placeholder="Add alias…" /></div>
            <div>{lbl('Example Questions')}<ChipInput items={draft.exampleQuestions} setItems={v => patch({ exampleQuestions: v })} placeholder="Add example question…" /></div>
            <div>{lbl('Filter Map (JSON)')}<textarea className={`${cls} h-24 font-mono text-xs resize-none`} placeholder='{"marketplace": {"column": "marketplace_id", "transform": "marketplace"}}' value={JSON.stringify(draft.filterMap, null, 2)} onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => { try { patch({ filterMap: JSON.parse(e.target.value) }); } catch { /* typing */ } }} /></div>
            <div className="flex justify-end gap-3 pt-2">
              {editingId && <button onClick={cancelEdit} className="px-4 py-2 rounded-lg text-sm font-semibold text-slate-600 hover:bg-slate-100 transition-colors">Cancel</button>}
              <button onClick={save} disabled={!draft.metricId || !draft.description || !!idError || saving}
                className="bg-blue-600 text-white px-6 py-2 rounded-lg text-sm font-semibold hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
                {saving ? 'Saving…' : editingId ? 'Update Metric' : 'Save Metric'}
              </button>
            </div>
          </div>
        </div>
        <div className="space-y-4">
          <h3 className="text-lg font-semibold px-2">All Metrics</h3>
          {metrics.length === 0 && !loading ? (
            <div className="text-center py-10 glass-panel rounded-2xl text-slate-400">No metrics defined yet.</div>
          ) : (
            <div className="space-y-2">
              {metrics.map(metric => (
                <div key={metric.id} className="glass-panel p-4 rounded-xl hover:shadow-md transition-all group">
                  <div className="flex justify-between items-start mb-2">
                    <div className="cursor-pointer" onClick={() => startEdit(metric)}>
                      <h5 className="font-semibold text-slate-800 text-sm">{metric.metricId}</h5>
                      <p className="text-xs text-slate-500 mt-0.5">{metric.description}</p>
                    </div>
                    <button onClick={() => handleDelete(metric.id)} className="text-slate-300 hover:text-red-500 opacity-0 group-hover:opacity-100 transition-all"><Trash2 size={14} /></button>
                  </div>
                  <div className="flex flex-wrap gap-1.5 mt-2">
                    {metric.dataSource && <span className="text-[10px] bg-blue-50 text-blue-600 px-1.5 py-0.5 rounded border border-blue-100 font-mono">{metric.dataSource}</span>}
                    {metric.aliases.map((a, i) => <span key={i} className="text-[10px] bg-purple-50 text-purple-600 px-1.5 py-0.5 rounded border border-purple-100">{a}</span>)}
                  </div>
                  <div className="flex items-center justify-between mt-3">
                    <span className="text-[10px] text-slate-400">{metric.variations.length} variation{metric.variations.length !== 1 ? 's' : ''}</span>
                    <button onClick={() => onOpenMetric(metric)} className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 font-medium transition-colors">Manage Variations <ChevronRight size={13} /></button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

const AnalyticsPage = () => (
  <div className="max-w-4xl mx-auto text-center py-20 space-y-6">
    <div className="w-20 h-20 bg-blue-50 text-blue-600 rounded-3xl flex items-center justify-center mx-auto mb-8"><BarChart3 size={40} /></div>
    <h2 className="text-3xl font-bold text-slate-900">Analytics Dashboard</h2>
    <p className="text-slate-500 max-w-md mx-auto">Coming soon.</p>
  </div>
);

// ── Main App ─────────────────────────────────────────────────────────────────

export default function App() {
  const [currentPage, setCurrentPage] = useState<Page>('metrics');
  const [metrics, setMetrics] = useState<Metric[]>([]);
  const [userEmail, setUserEmail] = useState('');
  const [authed, setAuthed] = useState(false);
  const [authChecked, setAuthChecked] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [openMetric, setOpenMetric] = useState<Metric | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const didAuth = await handleAuthCallback();
        const token = getIdToken();
        if (!token && !didAuth) { window.location.href = getLoginUrl(); return; }
        const claims = decodeJwt(token || getIdToken() || '');
        if (cancelled) return;
        setUserEmail(claims?.email || '');
        setAuthed(true);
        api.fetchMetrics().then(setMetrics).catch(console.error).finally(() => setLoading(false));
      } catch (err) {
        if (cancelled) return;
        setAuthError(err instanceof Error ? err.message : String(err));
        setLoading(false);
      } finally { if (!cancelled) setAuthChecked(true); }
    })();
    return () => { cancelled = true; };
  }, []);

  if (!authChecked) return <div className="min-h-screen flex items-center justify-center bg-slate-50 text-slate-600"><div className="glass-panel rounded-2xl px-6 py-5 text-sm">Checking session...</div></div>;
  if (authError) return (
    <div className="min-h-screen flex items-center justify-center bg-slate-50 text-slate-700">
      <div className="glass-panel rounded-2xl px-6 py-5 text-sm max-w-lg">
        <div className="font-semibold text-slate-900 mb-2">Authentication failed</div>
        <div className="text-slate-600 break-words">{authError}</div>
        <button onClick={() => { window.location.href = getLoginUrl(); }} className="mt-4 inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-semibold hover:bg-blue-700 transition-colors">Retry login</button>
      </div>
    </div>
  );
  if (!authed) return null;

  const handleVariationSave = (updated: Metric) => { setMetrics(prev => prev.map(m => m.id === updated.id ? updated : m)); setOpenMetric(null); };

  return (
    <div className="flex min-h-screen bg-slate-50 font-sans">
      <Sidebar currentPage={currentPage} setCurrentPage={(p) => { setCurrentPage(p); setOpenMetric(null); }} userEmail={userEmail} />
      <main className="flex-1 p-8 overflow-y-auto h-screen">
        <div className="max-w-7xl mx-auto">
          <AnimatePresence mode="wait">
            <motion.div key={openMetric ? `var-${openMetric.id}` : currentPage}
              initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -10 }} transition={{ duration: 0.2 }}>
              {openMetric ? (
                <VariationEditor metric={openMetric} onBack={() => setOpenMetric(null)} onSave={handleVariationSave} />
              ) : currentPage === 'metrics' ? (
                <MetricsPage metrics={metrics} setMetrics={setMetrics} loading={loading} onOpenMetric={setOpenMetric} />
              ) : (
                <AnalyticsPage />
              )}
            </motion.div>
          </AnimatePresence>
        </div>
      </main>
    </div>
  );
}
