import { ScanCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { getSearchConfig } from "../../services/runtime-config.mjs";

export function mapMetricVariationToSchema(metric, variation) {
  return {
    metric_id: metric.metricId,
    variation_id: variation.variationId,
    variation_label: variation.label || "",
    variation_description: variation.description || "",
    sql: variation.sql || "",
    filter_map: metric.filterMap || {},
    required_filters: variation.requiredFilters || [],
    summary_hint: variation.summaryHint || "",
  };
}

export async function loadMetricFromMetricsTable(services, metricsTableName, metricId) {
  if (!metricsTableName || !metricId) return null;
  const res = await services.clients.getDdb().send(new ScanCommand({
    TableName: metricsTableName,
    FilterExpression: "#metricId = :metricId",
    ExpressionAttributeNames: { "#metricId": "metricId" },
    ExpressionAttributeValues: { ":metricId": { S: String(metricId) } },
    Limit: 10,
  }));
  return (res.Items || []).map(unmarshall).find((m) => m?.metricId === metricId) || null;
}

export async function loadSchemaFromMetricsTable(services, metricsTableName, metricId, variationId) {
  if (!metricId || !variationId) return null;
  const metric = await loadMetricFromMetricsTable(services, metricsTableName, metricId);
  if (!metric) return null;
  const variation = (metric.variations || []).find((v) => v?.variationId === variationId);
  if (!variation) return null;
  return mapMetricVariationToSchema(metric, variation);
}

export async function searchSchemas(services, embedding, topK, metricsTableName) {
  const { endpoint, index } = await getSearchConfig(services);
  if (!endpoint) throw new Error("Missing OpenSearch endpoint");
  if (!index) throw new Error("Missing OpenSearch index");

  const client = services.clients.getOpenSearch(endpoint);
  const res = await client.search({
    index,
    size: topK,
    body: {
      query: {
        knn: {
          embedding: {
            vector: embedding,
            k: topK,
          },
        },
      },
    },
  });

  const hits = res.body?.hits?.hits || res.hits?.hits || [];
  const baseSchemas = hits.map((h) => h._source?.schema).filter(Boolean);

  const dedup = [];
  const seen = new Set();
  for (const schema of baseSchemas) {
    const key = `${schema?.metric_id || ""}:${schema?.variation_id || ""}`;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    dedup.push(schema);
  }

  const metricIds = [...new Set(dedup.map((s) => s?.metric_id).filter(Boolean))];
  if (!metricIds.length) return dedup;

  const expanded = [];
  for (const metricId of metricIds.slice(0, 2)) {
    try {
      const metric = await loadMetricFromMetricsTable(services, metricsTableName, metricId);
      if (!metric?.variations?.length) continue;
      for (const variation of metric.variations) {
        expanded.push(mapMetricVariationToSchema(metric, variation));
      }
    } catch (err) {
      console.error("search-schemas-expand-error", { metricId, message: err?.message });
    }
  }

  if (!expanded.length) return dedup;

  const final = [];
  const finalSeen = new Set();
  for (const schema of [...expanded, ...dedup]) {
    const key = `${schema?.metric_id || ""}:${schema?.variation_id || ""}`;
    if (!key || finalSeen.has(key)) continue;
    finalSeen.add(key);
    final.push(schema);
  }
  return final;
}
