import { START, END, MessagesValue, StateGraph, StateSchema } from "@langchain/langgraph";
import { z } from "zod/v4";
import { ATHENA_DATABASE, ATHENA_WORKGROUP, METRICS_TABLE } from "../../config.mjs";
import { getRecentUserText, hasValue } from "../../domain/common.mjs";
import {
  compileSqlWithDynamicFilters,
  getUnresolvedSqlParams,
  hasRequiredBindingInTemplate,
} from "../../domain/sql.mjs";
import {
  detectWeekComparisonPlan,
  parseTimeIntent,
  resolveReportingDates,
} from "../../domain/dates.mjs";
import {
  formatSummaryStructured,
  renderStructuredToSlackMrkdwn,
} from "../../domain/summary-format.mjs";
import { runAthenaQuery } from "../../services/athena.mjs";
import {
  loadMetricFromMetricsTable,
  loadSchemaFromMetricsTable,
  mapMetricVariationToSchema,
} from "./clients.mjs";
import { extractFromSchemas } from "./extraction.mjs";

function chooseAlternativeSchema(metric, currentVariationId, provided, contextText) {
  if (!metric || !Array.isArray(metric.variations)) return null;
  const context = String(contextText || "").toLowerCase();
  const hasOrderIdMention = /\b\d{3}-\d{7}-\d{7,8}\b/.test(context);

  const candidates = metric.variations
    .filter((variation) => variation?.variationId && variation.variationId !== currentVariationId)
    .map((variation) => mapMetricVariationToSchema(metric, variation))
    .filter((schema) => {
      const required = Array.isArray(schema.required_filters) ? schema.required_filters : [];
      if (!hasOrderIdMention && required.includes("order_id")) return false;
      return required.every((field) => provided.has(field));
    });

  if (!candidates.length) return null;

  const scoreSchema = (schema) => {
    const text = `${schema.variation_id} ${schema.variation_label || ""} ${schema.variation_description || ""}`.toLowerCase();
    let score = 0;
    if (/(^|\s)cities?(\s|$)|(^|\s)city(\s|$)/.test(context) && /city/.test(text)) score += 10;
    if (/(^|\s)stores?(\s|$)/.test(context) && /store/.test(text)) score += 8;
    if (/(customer|prime|segment)/.test(context) && /(customer|prime|segment)/.test(text)) score += 8;
    if (/(category|categories|revenue|units)/.test(context) && /(category|top_categories)/.test(text)) score += 7;
    if (/(volume|overview|how many|orders?)/.test(context) && /(volume|overview|order_volume)/.test(text)) score += 5;
    if (/trace|specific order/.test(context) && /order_trace/.test(text)) score += 6;
    if (schema.variation_id === "order_volume") score += 1;
    return score;
  };

  candidates.sort((a, b) => scoreSchema(b) - scoreSchema(a));
  return candidates[0];
}

const WorkflowState = new StateSchema({
  messages: MessagesValue,
  schemas: z.array(z.any()).default(() => []),
  extraction: z.any().default(() => null),
  matchedSchema: z.any().nullable().default(() => null),
  execution_ready: z.boolean().default(() => false),
  execution_path: z.string().default(() => "single"),
  comparison_periods: z.array(z.any()).default(() => []),
  sql: z.string().nullable().default(() => null),
  data: z.any().nullable().default(() => null),
  summary_structured: z.any().nullable().default(() => null),
  summary: z.string().nullable().default(() => null),
});

export function createOrdersAnalyticsWorkflow({ services, checkpointer }) {
  let compiledSubgraph;

  const workflow = {
    id: "orders_analytics",
    intentSignals: [
      "order",
      "orders",
      "week",
      "w13",
      "w14",
      "w15",
      "city",
      "cities",
      "store",
      "marketplace",
      "uae",
      "ksa",
      "egypt",
      "sales",
      "promo",
      "trace",
    ],
    capabilities: ["tabular_summary"],
    buildSubgraph() {
      if (compiledSubgraph) return compiledSubgraph;

      compiledSubgraph = new StateGraph(WorkflowState)
        .addNode("extract", async (state) => {
          const contextText = getRecentUserText(state.messages, 3);
          console.log("SELVY_DEBUG extract-input", JSON.stringify({
            contextText,
            schemaCount: state.schemas?.length,
            schemaIds: (state.schemas || []).map((s) => `${s.metric_id}:${s.variation_id}`),
          }));

          const extraction = await extractFromSchemas(services, contextText, state.schemas || []);
          if (extraction?.time_range?.start && extraction?.time_range?.end && !extraction?.time_range_source) {
            extraction.time_range_source = "prompt_inference";
          }

          console.log("SELVY_DEBUG extract-output", JSON.stringify(extraction));
          return { extraction };
        })
        .addNode("resolve_dates", async (state) => {
          const extraction = state.extraction;
          if (!extraction) return {};

          const latestText = getRecentUserText(state.messages, 1);
          const contextText = getRecentUserText(state.messages, 3);
          const comparisonPlan = detectWeekComparisonPlan(latestText, contextText);
          if (comparisonPlan) {
            const periods = [comparisonPlan.focus, comparisonPlan.baseline];
            const sortedByStart = [...periods].sort((a, b) => a.start.localeCompare(b.start));
            return {
              extraction: {
                ...extraction,
                time_range: {
                  ...extraction.time_range,
                  start: sortedByStart[0].start,
                  end: sortedByStart[sortedByStart.length - 1].end,
                  grain: "week",
                },
                comparison_periods: periods.map((period) => ({
                  label: period.label,
                  week: period.week,
                  year: period.year,
                  start: period.start,
                  end: period.end,
                })),
                time_range_source: "reporting_calendar",
              },
            };
          }

          const intent = parseTimeIntent(latestText);
          if (!intent) return {};

          const hasConcreteDates = !!(extraction.time_range?.start && extraction.time_range?.end);
          const shouldForceReportingWeekResolution = intent.type === "week" || intent.type === "last_week" || intent.type === "this_week";
          if (hasConcreteDates && !shouldForceReportingWeekResolution) return {};

          const resolved = await resolveReportingDates(intent);
          if (!resolved) return {};

          return {
            extraction: {
              ...extraction,
              time_range: { ...extraction.time_range, start: resolved.start, end: resolved.end },
              time_range_source: "reporting_calendar",
            },
          };
        })
        .addNode("execute_prepare", async (state) => {
          const extraction = state.extraction;
          if (!extraction?.metric_id || !extraction?.variation_id) {
            console.log("SELVY_DEBUG execute-skip", JSON.stringify({ reason: "no metric_id or variation_id", extraction }));
            return {
              execution_ready: false,
              comparison_periods: [],
              sql: null,
              data: null,
              matchedSchema: null,
            };
          }

          if (!ATHENA_WORKGROUP || !ATHENA_DATABASE) {
            console.log("SELVY_DEBUG execute-skip", JSON.stringify({ reason: "no athena config" }));
            return {
              extraction: { ...extraction, nudge: "Athena config is missing." },
              execution_ready: false,
              comparison_periods: [],
              sql: null,
              data: null,
              matchedSchema: null,
            };
          }

          const schemaCandidates = (state.schemas || []).filter(
            (schema) => schema.metric_id === extraction.metric_id && schema.variation_id === extraction.variation_id,
          );

          if (!schemaCandidates.length) {
            return {
              extraction: { ...extraction, nudge: "Schema not found for selected metric/variation." },
              execution_ready: false,
              comparison_periods: [],
              sql: null,
              data: null,
              matchedSchema: null,
            };
          }

          let schema = schemaCandidates.find((candidate) => {
            const required = Array.isArray(candidate?.required_filters) ? candidate.required_filters : [];
            return required.every((field) => hasRequiredBindingInTemplate(candidate?.sql || "", field, candidate));
          }) || schemaCandidates[0];

          try {
            const authoritativeSchema = await loadSchemaFromMetricsTable(
              services,
              METRICS_TABLE,
              extraction.metric_id,
              extraction.variation_id,
            );
            if (authoritativeSchema) {
              schema = authoritativeSchema;
              console.log("SELVY_DEBUG execute-schema-authoritative", JSON.stringify({
                metric_id: extraction.metric_id,
                variation_id: extraction.variation_id,
                source: "metrics_table",
              }));
            }
          } catch (err) {
            console.error("schema-authoritative-error", {
              message: err?.message,
              metric_id: extraction.metric_id,
              variation_id: extraction.variation_id,
            });
          }

          let required = Array.isArray(schema.required_filters) ? schema.required_filters : [];
          const provided = new Set(
            (extraction.filters || [])
              .filter((filter) => filter?.field && hasValue(filter.value))
              .map((filter) => filter.field),
          );

          if (hasValue(extraction?.time_range?.start)) {
            provided.add("start");
            provided.add("order_day_start");
            provided.add("snapshot_date_start");
          }
          if (hasValue(extraction?.time_range?.end)) {
            provided.add("end");
            provided.add("order_day_end");
            provided.add("snapshot_date_end");
          }

          let missing = required.filter((field) => !provided.has(field));
          if (missing.length) {
            try {
              const contextText = getRecentUserText(state.messages, 1);
              const metricDoc = await loadMetricFromMetricsTable(services, METRICS_TABLE, extraction.metric_id);
              const alternativeSchema = chooseAlternativeSchema(metricDoc, schema.variation_id, provided, contextText);
              if (alternativeSchema) {
                const fromVariation = schema.variation_id;
                schema = alternativeSchema;
                required = Array.isArray(schema.required_filters) ? schema.required_filters : [];
                missing = required.filter((field) => !provided.has(field));
                console.log("SELVY_DEBUG execute-schema-autoswitch", JSON.stringify({
                  metric_id: extraction.metric_id,
                  from_variation: fromVariation,
                  to_variation: schema.variation_id,
                  provided: [...provided],
                  contextText,
                }));
              }
            } catch (err) {
              console.error("schema-autoswitch-error", {
                message: err?.message,
                metric_id: extraction.metric_id,
                variation_id: extraction.variation_id,
              });
            }
          }

          if (missing.length) {
            console.log("SELVY_DEBUG execute-missing-filters", JSON.stringify({ required, provided: [...provided], missing, extraction }));
            return {
              extraction: { ...extraction, nudge: `Missing required filters: ${missing.join(", ")}` },
              execution_ready: false,
              comparison_periods: [],
              sql: null,
              data: null,
              matchedSchema: schema,
            };
          }

          const unboundRequired = required.filter((field) => !hasRequiredBindingInTemplate(schema.sql || "", field, schema));
          if (unboundRequired.length) {
            console.log("SELVY_DEBUG execute-unbound-required", JSON.stringify({ required, unboundRequired, sqlTemplate: schema.sql || "" }));
            return {
              extraction: {
                ...extraction,
                nudge: `Schema configuration error: required filters are not bound in SQL template (${unboundRequired.join(", ")}). Use :param placeholders or {{dynamic_filters}}.`,
              },
              execution_ready: false,
              comparison_periods: [],
              sql: null,
              data: null,
              matchedSchema: schema,
            };
          }

          let comparisonPeriods = Array.isArray(extraction?.comparison_periods) ? extraction.comparison_periods : [];
          if (comparisonPeriods.length < 2) {
            const latestText = getRecentUserText(state.messages, 1);
            const contextText = getRecentUserText(state.messages, 3);
            const derivedPlan = detectWeekComparisonPlan(latestText, contextText);
            if (derivedPlan) comparisonPeriods = [derivedPlan.focus, derivedPlan.baseline];
          }

          return {
            matchedSchema: schema,
            execution_ready: true,
            comparison_periods: comparisonPeriods.slice(0, 2).map((period) => ({
              label: period.label,
              week: period.week,
              year: period.year,
              start: period.start,
              end: period.end,
            })),
          };
        })
        .addNode("detect_comparison", async (state) => {
          const ready = !!state.execution_ready;
          const periods = Array.isArray(state.comparison_periods) ? state.comparison_periods : [];
          const executionPath = ready && periods.length >= 2 ? "comparison" : "single";
          return { execution_path: executionPath };
        })
        .addNode("execute_single", async (state) => {
          const extraction = state.extraction;
          const schema = state.matchedSchema;
          if (!state.execution_ready || !extraction || !schema) return {};

          const sql = compileSqlWithDynamicFilters(schema.sql || "", extraction, schema);
          const unresolved = getUnresolvedSqlParams(sql);
          if (unresolved.length) {
            return {
              extraction: { ...extraction, nudge: `Missing required parameters: ${unresolved.join(", ")}` },
              sql,
              data: null,
            };
          }

          try {
            const data = await runAthenaQuery(services, sql);
            return { sql, data };
          } catch (err) {
            console.error("athena-query-error", {
              metric_id: extraction.metric_id,
              variation_id: schema.variation_id || extraction.variation_id,
              message: err?.message,
            });
            return {
              extraction: { ...extraction, nudge: `Athena query failed: ${err?.message || "unknown error"}` },
              sql,
              data: null,
            };
          }
        })
        .addNode("execute_comparison", async (state) => {
          const extraction = state.extraction;
          const schema = state.matchedSchema;
          if (!state.execution_ready || !extraction || !schema) return {};

          const periods = Array.isArray(state.comparison_periods) ? state.comparison_periods.slice(0, 2) : [];
          if (periods.length < 2) return {};

          const sqlParts = [];
          let mergedColumns = null;
          const mergedRows = [];

          for (const period of periods) {
            const periodExtraction = {
              ...extraction,
              time_range: {
                ...extraction.time_range,
                start: period.start,
                end: period.end,
                grain: "week",
              },
              time_range_source: "reporting_calendar",
            };
            const periodSql = compileSqlWithDynamicFilters(schema.sql || "", periodExtraction, schema);
            const unresolvedPeriod = getUnresolvedSqlParams(periodSql);
            if (unresolvedPeriod.length) {
              return {
                extraction: {
                  ...extraction,
                  nudge: `Missing required parameters for ${period.label || "comparison period"}: ${unresolvedPeriod.join(", ")}`,
                },
                sql: periodSql,
                data: null,
              };
            }

            const periodData = await runAthenaQuery(services, periodSql);
            if (!mergedColumns) mergedColumns = periodData?.columns || [];
            for (const row of periodData?.rows || []) {
              mergedRows.push([period.label || `${period.start} to ${period.end}`, ...row]);
            }
            sqlParts.push(`-- ${period.label || "period"} (${period.start} to ${period.end})\n${periodSql}`);
          }

          const sorted = [...periods].sort((a, b) => String(a.start).localeCompare(String(b.start)));
          const data = { columns: ["period", ...(mergedColumns || [])], rows: mergedRows };
          const comparisonExtraction = {
            ...extraction,
            time_range: {
              ...extraction.time_range,
              start: sorted[0]?.start || extraction?.time_range?.start || null,
              end: sorted[sorted.length - 1]?.end || extraction?.time_range?.end || null,
              grain: "week",
            },
            comparison_periods: periods.map((period) => ({
              label: period.label,
              week: period.week,
              year: period.year,
              start: period.start,
              end: period.end,
            })),
            time_range_source: "reporting_calendar",
          };

          return {
            extraction: comparisonExtraction,
            sql: sqlParts.join("\n\n"),
            data,
          };
        })
        .addNode("merge_result", async () => {
          return {};
        })
        .addNode("format", async (state) => {
          if (!state.data) return { summary_structured: null };
          const contextText = getRecentUserText(state.messages, 1);
          const summaryHint = state.matchedSchema?.summary_hint || "";
          const summaryStructured = await formatSummaryStructured(services, contextText, state.data, summaryHint);
          return { summary_structured: summaryStructured };
        })
        .addNode("slack_mrkdwn", async (state) => {
          if (!state.summary_structured) return { summary: null };
          const summary = renderStructuredToSlackMrkdwn(state.summary_structured);
          return { summary };
        })
        .addEdge(START, "extract")
        .addEdge("extract", "resolve_dates")
        .addEdge("resolve_dates", "execute_prepare")
        .addEdge("execute_prepare", "detect_comparison")
        .addConditionalEdges(
          "detect_comparison",
          (state) => (state.execution_path === "comparison" ? "execute_comparison" : "execute_single"),
          {
            execute_single: "execute_single",
            execute_comparison: "execute_comparison",
          },
        )
        .addEdge("execute_single", "merge_result")
        .addEdge("execute_comparison", "merge_result")
        .addEdge("merge_result", "format")
        .addEdge("format", "slack_mrkdwn")
        .addEdge("slack_mrkdwn", END)
        .compile({ checkpointer });

      return compiledSubgraph;
    },
    async run({ input, threadId }) {
      const subgraph = workflow.buildSubgraph();
      const state = await subgraph.invoke(
        {
          messages: input.messages || [],
          schemas: input.schemas || [],
        },
        {
          configurable: {
            thread_id: threadId,
            checkpoint_ns: workflow.id,
          },
        },
      );

      return {
        extraction: state.extraction || null,
        matchedSchema: state.matchedSchema || null,
        sql: state.sql || null,
        data: state.data || null,
        summary_structured: state.summary_structured || null,
        summary: state.summary || null,
      };
    },
  };

  return workflow;
}
