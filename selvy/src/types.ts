// ── Filter mapping ────────────────────────────────────────────────────────────

export interface FilterMapEntry {
    column: string;
    transform?: 'marketplace' | string;
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
    variationId: string;
    label: string;
    description: string;
    sql: string;
    exampleSql: string;
    summaryHint: string;
    outputColumns: OutputColumn[];
    isCsvEnabled: boolean;
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
