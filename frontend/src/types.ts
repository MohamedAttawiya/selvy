// ── Filter mapping ────────────────────────────────────────────────────────────

export interface FilterMapEntry {
  /** Actual SQL column name. Defaults to the filter key if omitted. */
  column: string;
  /** Value transform applied before SQL generation (e.g. "marketplace" → ID lookup). */
  transform?: 'marketplace' | string;
  /** Match mode: exact (default), prefix, or case-insensitive contains. */
  mode?: 'exact' | 'prefix' | 'contains_ci';
}

export type FilterMapValue = string | FilterMapEntry;

// ── Output column descriptor ─────────────────────────────────────────────────

export interface OutputColumn {
  name: string;
  type: 'STRING' | 'NUMBER' | 'DATE' | 'TIMESTAMP' | 'BOOLEAN';
}

// ── Variation ────────────────────────────────────────────────────────────────

export interface Variation {
  /** Unique ID within the metric (e.g. "order_volume", "top_stores"). */
  variationId: string;
  /** Human-readable label shown in the UI and variation list. */
  label: string;
  /** Longer description of what this variation answers and when to use it. */
  description: string;
  /** SQL template with :param placeholders and {{dynamic_filters}}. Auto-generated from exampleSql + filter toggles. */
  sql: string;
  /** The original SQL with real values as written by the user. Used for testing and as the source of truth for filter detection. */
  exampleSql: string;
  /**
   * Guidance for the summary LLM on how to present results.
   * Not a rigid schema — more of a recommendation. The LLM decides the
   * best response shape, but this nudges it toward the right framing.
   */
  summaryHint: string;
  /** Describes the columns in the result set (for frontend table / CSV). */
  outputColumns: OutputColumn[];
  /** Whether the user can download this variation's results as CSV. */
  isCsvEnabled: boolean;
  /** Filters that MUST be provided before this variation can execute (auto-derived from REQUIRED toggle). */
  requiredFilters: string[];
}

// ── Metric ───────────────────────────────────────────────────────────────────

export interface Metric {
  id: string;
  metricId: string;
  description: string;
  dataSource: string;
  aliases: string[];
  exampleQuestions: string[];
  filterMap: Record<string, FilterMapValue>;
  variations: Variation[];
  createdAt?: string;
  updatedAt?: string;
}

export type Page = 'metrics' | 'analytics';
