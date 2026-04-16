import { CONFIG } from './config';
import { getAccessToken } from './auth';
import { Metric } from './types';

function normalizeMetric(raw: Metric): Metric {
  return {
    ...raw,
    dataSource: raw.dataSource ?? '',
    aliases: raw.aliases ?? [],
    exampleQuestions: raw.exampleQuestions ?? [],
    filterMap: raw.filterMap ?? {},
    variations: (raw.variations ?? []).map(v => ({
      ...v,
      label: v.label ?? v.variationId ?? '',
      description: v.description ?? '',
      exampleSql: v.exampleSql ?? '',
      summaryHint: v.summaryHint ?? '',
      outputColumns: v.outputColumns ?? [],
      requiredFilters: v.requiredFilters ?? [],
    })),
  };
}

function headers() {
  const token = getAccessToken();
  return {
    'Content-Type': 'application/json',
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  };
}

export async function fetchMetrics(): Promise<Metric[]> {
  const res = await fetch(`${CONFIG.apiUrl}/metrics`, { headers: headers() });
  if (!res.ok) throw new Error('Failed to fetch metrics');
  const data = await res.json();
  return Array.isArray(data) ? data.map(normalizeMetric) : [];
}

export async function createMetric(data: Omit<Metric, 'id'>): Promise<Metric> {
  const res = await fetch(`${CONFIG.apiUrl}/metrics`, {
    method: 'POST', headers: headers(), body: JSON.stringify(data),
  });
  if (!res.ok) throw new Error('Failed to create metric');
  return normalizeMetric(await res.json());
}

export async function updateMetric(id: string, data: Partial<Metric>): Promise<Metric> {
  const res = await fetch(`${CONFIG.apiUrl}/metrics/${id}`, {
    method: 'PUT', headers: headers(), body: JSON.stringify(data),
  });
  if (!res.ok) throw new Error('Failed to update metric');
  return normalizeMetric(await res.json());
}

export async function deleteMetric(id: string): Promise<void> {
  await fetch(`${CONFIG.apiUrl}/metrics/${id}`, { method: 'DELETE', headers: headers() });
}

// Query API (Andes / Athena)
export interface AndesTable {
  name: string;
  isResourceLink: boolean;
  columns: { name: string; type: string }[];
}

export interface QueryResult {
  columns: string[];
  rows: string[][];
  queryId?: string;
  totalColumns?: number;
  truncatedColumns?: boolean;
  error?: string;
}

export async function fetchAndesTables(): Promise<AndesTable[]> {
  const res = await fetch(`${CONFIG.queryApiUrl}/query/tables`, { headers: headers() });
  if (!res.ok) throw new Error('Failed to fetch tables');
  return res.json();
}

export async function startQuery(sql: string): Promise<{ queryId: string }> {
  const res = await fetch(`${CONFIG.queryApiUrl}/query/start`, {
    method: 'POST', headers: headers(), body: JSON.stringify({ sql }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || 'Failed to start query');
  return data;
}

export async function getQueryStatus(queryId: string): Promise<{ queryId: string; state: string; stateChangeReason: string | null }> {
  const res = await fetch(`${CONFIG.queryApiUrl}/query/status/${queryId}`, { headers: headers() });
  if (!res.ok) throw new Error('Failed to get query status');
  return res.json();
}

export async function getQueryResults(queryId: string): Promise<QueryResult> {
  const res = await fetch(`${CONFIG.queryApiUrl}/query/results/${queryId}`, { headers: headers() });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || 'Failed to get results');
  return data;
}

export async function runQuery(sql: string): Promise<QueryResult> {
  const { queryId } = await startQuery(sql);
  for (let i = 0; i < 120; i++) {
    const status = await getQueryStatus(queryId);
    if (status.state === 'SUCCEEDED') return getQueryResults(queryId);
    if (status.state === 'FAILED' || status.state === 'CANCELLED') throw new Error(status.stateChangeReason || status.state);
    await new Promise(r => setTimeout(r, 1500));
  }
  throw new Error('Query timed out after 3 minutes');
}
