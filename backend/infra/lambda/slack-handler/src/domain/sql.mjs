import { MARKETPLACE_ALIAS_TO_ID } from "../config.mjs";
import { escapeRegex } from "./common.mjs";

export const toSqlValue = (value) => {
  if (value === null || value === undefined) return "NULL";
  if (Array.isArray(value)) return value.map(toSqlValue).join(", ");
  if (typeof value === "number") return String(value);
  const str = String(value);
  if (/^-?\d+(\.\d+)?$/.test(str)) return str;
  return `'${str.replace(/'/g, "''")}'`;
};

export function parseFilterValues(filter) {
  const op = String(filter?.operator || "eq").toLowerCase();
  const raw = Array.isArray(filter?.value) ? filter.value : [filter?.value];
  let values = raw;
  if (op === "in") {
    values = raw.flatMap((v) => {
      if (Array.isArray(v)) return v;
      if (typeof v === "string") {
        return v
          .split(/[\n,]/g)
          .map((s) => s.trim())
          .filter(Boolean);
      }
      return [v];
    });
  }
  return values
    .filter((v) => v !== null && v !== undefined)
    .map((v) => (typeof v === "string" ? v.trim() : v))
    .filter((v) => !(typeof v === "string" && v.length === 0));
}

function normalizeMarketplaceValue(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return raw;
  const upper = raw.toUpperCase();
  return MARKETPLACE_ALIAS_TO_ID[upper] || raw;
}

export function hasRequiredBindingInTemplate(sqlTemplate, requiredField, schema) {
  const template = String(sqlTemplate || "");
  if (!template) return false;

  if (requiredField === "start") {
    return /:start\b/.test(template) || /:[A-Za-z_][A-Za-z0-9_]*_start\b/.test(template);
  }
  if (requiredField === "end") {
    return /:end\b/.test(template) || /:[A-Za-z_][A-Za-z0-9_]*_end\b/.test(template);
  }
  if (new RegExp(`:${escapeRegex(requiredField)}\\b`).test(template)) {
    return true;
  }

  const hasDynamicPlaceholder = /\{\{\s*dynamic_filters\s*\}\}/i.test(template);
  if (!hasDynamicPlaceholder) return false;
  const mapping = (schema?.filter_map && typeof schema.filter_map === "object") ? schema.filter_map : {};
  return Object.prototype.hasOwnProperty.call(mapping, requiredField);
}

export function compileSql(template, extraction) {
  let sql = template || "";
  sql = sql.replace(/':([A-Za-z_][A-Za-z0-9_]*)'/g, ":$1");
  const params = {};
  if (extraction?.time_range?.start) {
    params.start = extraction.time_range.start;
    const startRe = /:([a-z_]+)_start\b/g;
    let sm;
    while ((sm = startRe.exec(sql)) !== null) {
      params[`${sm[1]}_start`] = extraction.time_range.start;
    }
  }
  if (extraction?.time_range?.end) {
    params.end = extraction.time_range.end;
    const endRe = /:([a-z_]+)_end\b/g;
    let em;
    while ((em = endRe.exec(sql)) !== null) {
      params[`${em[1]}_end`] = extraction.time_range.end;
    }
  }
  for (const filter of extraction?.filters || []) {
    if (!filter?.field) continue;
    const values = parseFilterValues(filter);
    const op = String(filter?.operator || "eq").toLowerCase();
    if (!values.length) continue;
    params[filter.field] = (op === "in" && values.length > 1) ? values : values[0];
  }

  for (const [key, val] of Object.entries(params)) {
    if (Array.isArray(val)) {
      const inValues = val.map((v) => toSqlValue(v)).join(", ");
      sql = sql.replace(new RegExp(`\\b([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*:${escapeRegex(key)}\\b`, "g"), `$1 IN (${inValues})`);
      sql = sql.replaceAll(`:${key}`, inValues);
      continue;
    }
    sql = sql.replaceAll(`:${key}`, toSqlValue(val));
  }
  return sql;
}

function buildFilterClause(filter, mapping = {}) {
  const field = String(filter?.field || "").trim();
  if (!field) return null;
  const mapValue = mapping[field];
  const config = typeof mapValue === "string" ? { column: mapValue } : (mapValue || {});
  const column = String(config.column || field).trim();
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(column)) return null;

  const transform = config.transform || null;
  const mode = config.mode || null;
  const op = String(filter?.operator || "eq").toLowerCase();
  const normalize = (v) => transform === "marketplace" ? normalizeMarketplaceValue(v) : v;
  const values = parseFilterValues(filter);
  const normalizedValues = values.map(normalize).filter((v) => v !== null && v !== undefined && String(v).trim() !== "");
  if (!normalizedValues.length) return null;

  if (mode === "prefix") {
    if (normalizedValues.length === 1) return `${column} LIKE ${toSqlValue(`${normalizedValues[0]}%`)}`;
    return `(${normalizedValues.map((v) => `${column} LIKE ${toSqlValue(`${v}%`)}`).join(" OR ")})`;
  }
  if (mode === "contains_ci") {
    if (normalizedValues.length === 1) return `LOWER(${column}) LIKE LOWER(${toSqlValue(`%${normalizedValues[0]}%`)})`;
    return `(${normalizedValues.map((v) => `LOWER(${column}) LIKE LOWER(${toSqlValue(`%${v}%`)})`).join(" OR ")})`;
  }

  const opMap = { eq: "=", neq: "!=", gt: ">", gte: ">=", lt: "<", lte: "<=" };
  if (op === "in" || normalizedValues.length > 1) {
    return `${column} IN (${normalizedValues.map((v) => toSqlValue(v)).join(", ")})`;
  }
  if (!opMap[op]) return null;
  return `${column} ${opMap[op]} ${toSqlValue(normalizedValues[0])}`;
}

function buildDynamicFiltersSql(extraction, schema) {
  const filters = Array.isArray(extraction?.filters) ? extraction.filters : [];
  if (!filters.length) return "";
  const mapping = (schema?.filter_map && typeof schema.filter_map === "object") ? schema.filter_map : {};
  const clauses = filters.map((f) => buildFilterClause(f, mapping)).filter(Boolean);
  if (!clauses.length) return "";
  return ` AND ${clauses.join(" AND ")}`;
}

export function compileSqlWithDynamicFilters(template, extraction, schema) {
  const dynamicFiltersSql = buildDynamicFiltersSql(extraction, schema);
  const withDynamic = String(template || "").replaceAll("{{dynamic_filters}}", dynamicFiltersSql);
  return compileSql(withDynamic, extraction);
}

export function getUnresolvedSqlParams(sql) {
  const unresolved = new Set();
  const re = /(^|[^:]):([A-Za-z_][A-Za-z0-9_]*)/g;
  let match;
  while ((match = re.exec(sql)) !== null) unresolved.add(match[2]);
  return [...unresolved];
}

