import React, { useState } from 'react';
import { Plus, Trash2, Database, ChevronDown, ChevronRight, BarChart3, Settings, X } from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { Metric, Variation, Page } from './types';

const KNOWN_GRAINS = ['day', 'week', 'month'];
const KNOWN_VARIATION_TYPES = ['base_scalar', 'time_series', 'dimension_breakdown', 'ranked_dimension_breakdown', 'period_comparison'];
const METRIC_ID_RE = /^[a-z][a-z0-9_]{0,19}$/;

const ChipInput = ({ items, setItems, placeholder }: { items: string[]; setItems: (v: string[]) => void; placeholder: string }) => {
  const [draft, setDraft] = useState('');
  const add = () => { const v = draft.trim(); if (v && !items.includes(v)) { setItems([...items, v]); } setDraft(''); };
  return (
    <div className="flex flex-wrap gap-1.5 items-center">
      {items.map((item, i) => (
        <span key={i} className="flex items-center gap-1 text-xs bg-slate-100 text-slate-700 px-2 py-1 rounded-lg border border-slate-200">
          {item}
          <button onClick={() => setItems(items.filter((_, j) => j !== i))} className="text-slate-400 hover:text-red-500"><X size={10} /></button>
        </span>
      ))}
      <input type="text" placeholder={placeholder} className="text-xs px-2 py-1 border border-slate-200 rounded-lg outline-none focus:ring-1 focus:ring-blue-400 min-w-[120px]"
        value={draft} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setDraft(e.target.value)}
        onKeyDown={(e: React.KeyboardEvent) => { if (e.key === 'Enter') { e.preventDefault(); add(); } }} onBlur={add} />
    </div>
  );
};

const ToggleChips = ({ options, selected, setSelected }: { options: string[]; selected: string[]; setSelected: (v: string[]) => void }) => (
  <div className="flex flex-wrap gap-1.5">
    {options.map(opt => {
      const active = selected.includes(opt);
      return (
        <button key={opt} onClick={() => setSelected(active ? selected.filter(s => s !== opt) : [...selected, opt])}
          className={`text-xs px-2.5 py-1 rounded-lg border transition-colors ${active ? 'bg-blue-50 text-blue-600 border-blue-200 font-medium' : 'bg-white text-slate-500 border-slate-200 hover:border-blue-200'}`}>
          {opt}
        </button>
      );
    })}
  </div>
);

const Sidebar = ({ currentPage, setCurrentPage }: { currentPage: Page; setCurrentPage: (p: Page) => void }) => {
  const menuItems = [
    { id: 'metrics' as Page, label: 'Metrics', icon: Database },
    { id: 'analytics' as Page, label: 'Analytics', icon: BarChart3 },
  ];
  return (
    <div className="w-64 h-screen glass-panel border-r flex flex-col sticky top-0">
      <div className="p-6">
        <h1 className="text-2xl font-bold tracking-tighter text-blue-600 flex items-center gap-2">
          <div className="w-8 h-8 bg-blue-600 rounded-lg flex items-center justify-center text-white text-lg">S</div>
          Selvy
        </h1>
      </div>
      <nav className="flex-1 px-4 space-y-1">
        {menuItems.map((item) => (
          <button key={item.id} onClick={() => setCurrentPage(item.id)}
            className={`w-full flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-200 ${currentPage === item.id ? 'bg-blue-50 text-blue-600 font-semibold shadow-sm' : 'text-slate-500 hover:bg-slate-100 hover:text-slate-900'}`}>
            <item.icon size={20} />{item.label}
          </button>
        ))}
      </nav>
      <div className="p-4 border-t border-slate-100">
        <div className="flex items-center gap-3 px-4 py-3 text-slate-400">
          <Settings size={20} /><span className="text-sm">Settings</span>
        </div>
      </div>
    </div>
  );
};

function emptyVariation(): Variation {
  return { variationId: '', tag: '', sql: '', templateOutput: '', isCsvEnabled: false, requiredFilters: [] };
}

function emptyMetric(): Omit<Metric, 'id'> {
  return { metricId: '', description: '', aliases: [], exampleQuestions: [], allowedGroupBy: [], allowedFilters: [], allowedGrains: [], supportedVariations: [], variations: [] };
}

const MetricsPage = ({ metrics, setMetrics }: { metrics: Metric[]; setMetrics: React.Dispatch<React.SetStateAction<Metric[]>> }) => {
  const [draft, setDraft] = useState<Omit<Metric, 'id'>>(emptyMetric());
  const [editingId, setEditingId] = useState<string | null>(null);
  const [expandedVar, setExpandedVar] = useState<number | null>(null);
  const [idError, setIdError] = useState('');

  const patch = (p: Partial<Omit<Metric, 'id'>>) => setDraft(prev => ({ ...prev, ...p }));
  const validateId = (v: string) => { setIdError(!v ? '' : METRIC_ID_RE.test(v) ? '' : 'lowercase letters, digits, underscores; starts with letter; max 20 chars'); };

  const save = () => {
    if (!draft.metricId || !draft.description || idError) return;
    if (editingId) {
      setMetrics(prev => prev.map(m => m.id === editingId ? { ...draft, id: editingId } : m));
    } else {
      setMetrics(prev => [...prev, { ...draft, id: Date.now().toString() }]);
    }
    setDraft(emptyMetric()); setEditingId(null); setExpandedVar(null);
  };

  const startEdit = (m: Metric) => { const { id, ...rest } = m; setDraft(rest); setEditingId(id); setExpandedVar(null); };
  const cancelEdit = () => { setDraft(emptyMetric()); setEditingId(null); setExpandedVar(null); setIdError(''); };

  const addVar = () => { patch({ variations: [...draft.variations, emptyVariation()] }); setExpandedVar(draft.variations.length); };
  const updateVar = (idx: number, p: Partial<Variation>) => { patch({ variations: draft.variations.map((v, i) => i === idx ? { ...v, ...p } : v) }); };
  const removeVar = (idx: number) => { patch({ variations: draft.variations.filter((_, i) => i !== idx) }); setExpandedVar(null); };

  const lbl = (t: string) => <label className="block text-sm font-medium text-slate-700 mb-1">{t}</label>;
  const cls = "w-full px-3 py-2 rounded-lg border border-slate-200 focus:ring-2 focus:ring-blue-500 outline-none text-sm";

  return (
    <div className="max-w-5xl mx-auto space-y-8 pb-20">
      <header>
        <h2 className="text-3xl font-bold text-slate-900">Metrics</h2>
        <p className="text-slate-500 mt-2">Define metrics the agent can query. Each metric carries its own metadata and optional SQL variations.</p>
      </header>
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        <div className="lg:col-span-2 space-y-6">
          <div className="glass-panel p-6 rounded-2xl space-y-5">
            <div>{lbl('Metric ID')}<input type="text" placeholder="orders_lost" className={cls} value={draft.metricId} onChange={(e: React.ChangeEvent<HTMLInputElement>) => { patch({ metricId: e.target.value }); validateId(e.target.value); }} />{idError && <p className="text-xs text-red-500 mt-1">{idError}</p>}</div>
            <div>{lbl('Description')}<input type="text" placeholder="Number of orders lost due to store unavailability or closure" className={cls} value={draft.description} onChange={(e: React.ChangeEvent<HTMLInputElement>) => patch({ description: e.target.value })} /></div>
            <div>{lbl('Aliases')}<ChipInput items={draft.aliases} setItems={v => patch({ aliases: v })} placeholder="Add alias…" /></div>
            <div>{lbl('Example Questions')}<ChipInput items={draft.exampleQuestions} setItems={v => patch({ exampleQuestions: v })} placeholder="Add example question…" /></div>
            <div>{lbl('Allowed Group By')}<ChipInput items={draft.allowedGroupBy} setItems={v => patch({ allowedGroupBy: v })} placeholder="Add dimension…" /></div>
            <div>{lbl('Allowed Filters')}<ChipInput items={draft.allowedFilters} setItems={v => patch({ allowedFilters: v })} placeholder="Add filter field…" /></div>
            <div>{lbl('Allowed Grains')}<ToggleChips options={KNOWN_GRAINS} selected={draft.allowedGrains} setSelected={v => patch({ allowedGrains: v })} /></div>
            <div>{lbl('Supported Variations')}<ToggleChips options={KNOWN_VARIATION_TYPES} selected={draft.supportedVariations} setSelected={v => patch({ supportedVariations: v })} /></div>

            {/* Variations */}
            <div>
              <div className="flex items-center justify-between mb-2">
                {lbl('SQL Variations')}
                <button onClick={addVar} className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 font-medium"><Plus size={14} /> Add Variation</button>
              </div>
              {draft.variations.length === 0 && <p className="text-xs text-slate-400">No variations yet.</p>}
              <div className="space-y-2">
                {draft.variations.map((v, idx) => {
                  const open = expandedVar === idx;
                  return (
                    <div key={idx} className="border border-slate-200 rounded-xl overflow-hidden">
                      <button className="w-full flex items-center justify-between px-4 py-2.5 bg-slate-50 hover:bg-slate-100 transition-colors" onClick={() => setExpandedVar(open ? null : idx)}>
                        <span className="flex items-center gap-2 text-sm font-medium text-slate-700">
                          {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                          {v.variationId || `Variation ${idx + 1}`}
                          {v.tag && <span className="text-[10px] bg-blue-50 text-blue-600 px-1.5 py-0.5 rounded border border-blue-100">{v.tag}</span>}
                        </span>
                        <button onClick={(e: React.MouseEvent) => { e.stopPropagation(); removeVar(idx); }} className="text-slate-400 hover:text-red-500"><Trash2 size={14} /></button>
                      </button>
                      {open && (
                        <div className="p-4 space-y-3 bg-white">
                          <div className="grid grid-cols-2 gap-3">
                            <div>{lbl('Variation ID')}<input type="text" placeholder="base_scalar" className={cls} value={v.variationId} onChange={(e: React.ChangeEvent<HTMLInputElement>) => updateVar(idx, { variationId: e.target.value })} /></div>
                            <div>{lbl('Tag')}<input type="text" placeholder="ranking" className={cls} value={v.tag} onChange={(e: React.ChangeEvent<HTMLInputElement>) => updateVar(idx, { tag: e.target.value })} /></div>
                          </div>
                          <div>{lbl('SQL Template')}<textarea className="w-full px-3 py-2 rounded-lg border border-slate-200 focus:ring-2 focus:ring-blue-500 outline-none text-sm font-mono h-28 resize-y" placeholder="SELECT {{group_by}}, SUM(orders_lost) …" value={v.sql} onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => updateVar(idx, { sql: e.target.value })} /></div>
                          <div className="grid grid-cols-2 gap-3">
                            <div>{lbl('Template Output')}<input type="text" placeholder="JSON, Table, Chart" className={cls} value={v.templateOutput} onChange={(e: React.ChangeEvent<HTMLInputElement>) => updateVar(idx, { templateOutput: e.target.value })} /></div>
                            <div>{lbl('Required Filters')}<ChipInput items={v.requiredFilters} setItems={f => updateVar(idx, { requiredFilters: f })} placeholder="Add filter…" /></div>
                          </div>
                          <label className="flex items-center gap-3 cursor-pointer">
                            <div className={`w-10 h-5 rounded-full transition-colors relative ${v.isCsvEnabled ? 'bg-blue-600' : 'bg-slate-200'}`}>
                              <div className={`absolute top-0.5 w-4 h-4 bg-white rounded-full transition-all ${v.isCsvEnabled ? 'left-5' : 'left-0.5'}`} />
                            </div>
                            <input type="checkbox" className="hidden" checked={v.isCsvEnabled} onChange={(e: React.ChangeEvent<HTMLInputElement>) => updateVar(idx, { isCsvEnabled: e.target.checked })} />
                            <span className="text-xs font-medium text-slate-600">Enable CSV Export</span>
                          </label>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Actions */}
            <div className="flex justify-end gap-3 pt-2">
              {editingId && <button onClick={cancelEdit} className="px-4 py-2 rounded-lg text-sm font-semibold text-slate-600 hover:bg-slate-100 transition-colors">Cancel</button>}
              <button onClick={save} disabled={!draft.metricId || !draft.description || !!idError}
                className="bg-blue-600 text-white px-6 py-2 rounded-lg text-sm font-semibold hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
                {editingId ? 'Update Metric' : 'Save Metric'}
              </button>
            </div>
          </div>
        </div>

        {/* Metric list */}
        <div className="space-y-4">
          <h3 className="text-lg font-semibold px-2">All Metrics</h3>
          {metrics.length === 0 ? (
            <div className="text-center py-10 glass-panel rounded-2xl text-slate-400">No metrics defined yet.</div>
          ) : (
            <div className="space-y-2">
              {metrics.map(metric => (
                <div key={metric.id} className="glass-panel p-4 rounded-xl hover:shadow-md transition-all group cursor-pointer" onClick={() => startEdit(metric)}>
                  <div className="flex justify-between items-start mb-2">
                    <div>
                      <h5 className="font-semibold text-slate-800 text-sm">{metric.metricId}</h5>
                      <p className="text-xs text-slate-500 mt-0.5">{metric.description}</p>
                    </div>
                    <button onClick={(e: React.MouseEvent) => { e.stopPropagation(); setMetrics(prev => prev.filter(m => m.id !== metric.id)); }}
                      className="text-slate-300 hover:text-red-500 opacity-0 group-hover:opacity-100 transition-all"><Trash2 size={14} /></button>
                  </div>
                  <div className="flex flex-wrap gap-1.5 mt-2">
                    {metric.aliases.map((a, i) => <span key={i} className="text-[10px] bg-purple-50 text-purple-600 px-1.5 py-0.5 rounded border border-purple-100">{a}</span>)}
                    {metric.allowedGrains.map((g, i) => <span key={i} className="text-[10px] bg-green-50 text-green-600 px-1.5 py-0.5 rounded border border-green-100">{g}</span>)}
                    {metric.supportedVariations.map((sv, i) => <span key={i} className="text-[10px] bg-amber-50 text-amber-600 px-1.5 py-0.5 rounded border border-amber-100">{sv}</span>)}
                  </div>
                  <div className="text-[10px] text-slate-400 mt-2">{metric.variations.length} variation{metric.variations.length !== 1 ? 's' : ''}</div>
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
    <p className="text-slate-500 max-w-md mx-auto">Detailed insights into agent performance, query frequency, and data utilization will be available here soon.</p>
    <div className="grid grid-cols-3 gap-6 mt-12">{[1, 2, 3].map(i => <div key={i} className="glass-panel h-32 rounded-2xl animate-pulse" />)}</div>
  </div>
);

export default function App() {
  const [currentPage, setCurrentPage] = useState<Page>('metrics');
  const [metrics, setMetrics] = useState<Metric[]>([]);
  return (
    <div className="flex min-h-screen bg-slate-50 font-sans">
      <Sidebar currentPage={currentPage} setCurrentPage={setCurrentPage} />
      <main className="flex-1 p-8 overflow-y-auto h-screen">
        <div className="max-w-6xl mx-auto">
          <AnimatePresence mode="wait">
            <motion.div key={currentPage} initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -10 }} transition={{ duration: 0.2 }}>
              {currentPage === 'metrics' && <MetricsPage metrics={metrics} setMetrics={setMetrics} />}
              {currentPage === 'analytics' && <AnalyticsPage />}
            </motion.div>
          </AnimatePresence>
        </div>
      </main>
    </div>
  );
}
