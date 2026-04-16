import { DynamoDBClient, PutItemCommand, UpdateItemCommand, DeleteItemCommand, GetItemCommand } from "@aws-sdk/client-dynamodb";
import { BedrockRuntimeClient, InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { Client as OpenSearchClient } from "@opensearch-project/opensearch";
import { AwsSigv4Signer } from "@opensearch-project/opensearch/aws";

const ddb = new DynamoDBClient();
const bedrock = new BedrockRuntimeClient({ region: process.env.BEDROCK_REGION || "us-east-1" });
const ssm = new SSMClient();

const OPENSEARCH_REGION = process.env.OPENSEARCH_REGION || process.env.AWS_REGION || "eu-central-1";
const OPENSEARCH_ENDPOINT = process.env.OPENSEARCH_ENDPOINT || "";
const OPENSEARCH_INDEX = process.env.OPENSEARCH_INDEX || "";
const SEARCH_CONFIG_TTL_MS = Number(process.env.SEARCH_CONFIG_TTL_MS || "60000");
const SSM_PREFIX = process.env.SSM_PREFIX || "selvy-dev";

const BEDROCK_EMBED_MODEL_ID = process.env.BEDROCK_EMBED_MODEL_ID || "amazon.titan-embed-text-v2:0";
const BEDROCK_EMBED_DIMENSIONS = Number(process.env.BEDROCK_EMBED_DIMENSIONS || "256");

const TABLES = {
  metrics: process.env.METRICS_TABLE,
};
const ID_LENGTHS = { metrics: 5 };

let cachedSearchConfig;
let cachedSearchConfigAt = 0;
let osClient;

function generateId(len) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < len; i++) id += chars[Math.floor(Math.random() * chars.length)];
  return id;
}

async function getSearchConfig() {
  if (cachedSearchConfig && (Date.now() - cachedSearchConfigAt) < SEARCH_CONFIG_TTL_MS) return cachedSearchConfig;
  if (OPENSEARCH_ENDPOINT && OPENSEARCH_INDEX) {
    cachedSearchConfig = { endpoint: OPENSEARCH_ENDPOINT, index: OPENSEARCH_INDEX };
    cachedSearchConfigAt = Date.now();
    return cachedSearchConfig;
  }
  const endpointParam = `/${SSM_PREFIX}/search/opensearch-endpoint`;
  const indexParam = `/${SSM_PREFIX}/search/metrics-index`;
  const [endpointRes, indexRes] = await Promise.all([
    ssm.send(new GetParameterCommand({ Name: endpointParam })),
    ssm.send(new GetParameterCommand({ Name: indexParam })),
  ]);
  cachedSearchConfig = {
    endpoint: endpointRes.Parameter?.Value || "",
    index: indexRes.Parameter?.Value || "",
  };
  cachedSearchConfigAt = Date.now();
  return cachedSearchConfig;
}

async function getOpenSearch() {
  if (osClient) return osClient;
  const { endpoint } = await getSearchConfig();
  if (!endpoint) throw new Error("Missing OpenSearch endpoint");
  osClient = new OpenSearchClient({
    ...AwsSigv4Signer({ region: OPENSEARCH_REGION, service: "aoss" }),
    node: endpoint,
  });
  return osClient;
}

async function ensureIndex(client, index) {
  let already = false;
  try {
    const exists = await client.indices.exists({ index });
    already = exists.body ?? exists;
  } catch (err) {
    if (err?.statusCode !== 404) throw err;
    already = false;
  }
  if (already) return;
  await client.indices.create({
    index,
    body: {
      settings: { index: { knn: true } },
      mappings: {
        properties: {
          embedding: {
            type: "knn_vector",
            dimension: BEDROCK_EMBED_DIMENSIONS,
            method: {
              name: "hnsw",
              engine: "faiss",
              space_type: "cosinesimil",
              parameters: { ef_construction: 128, m: 16 },
            },
          },
        },
      },
    },
  });
}

async function embedText(text) {
  const response = await bedrock.send(new InvokeModelCommand({
    modelId: BEDROCK_EMBED_MODEL_ID,
    contentType: "application/json",
    accept: "application/json",
    body: JSON.stringify({
      inputText: text,
      dimensions: BEDROCK_EMBED_DIMENSIONS,
      normalize: true,
    }),
  }));
  const raw = JSON.parse(new TextDecoder().decode(response.body));
  return raw.embedding || [];
}

function buildSchema(metric, variation) {
  return {
    metric_id: metric.metricId,
    variation_id: variation.variationId,
    label: variation.label || variation.variationId || "",
    variation_description: variation.description || "",
    description: metric.description || "",
    data_source: metric.dataSource || "",
    aliases: metric.aliases || [],
    example_questions: metric.exampleQuestions || [],
    sql: variation.sql || "",
    example_sql: variation.exampleSql || "",
    summary_hint: variation.summaryHint || "",
    output_columns: variation.outputColumns || [],
    required_filters: variation.requiredFilters || [],
    is_csv_enabled: !!variation.isCsvEnabled,
    filter_map: metric.filterMap || {},
  };
}

function buildContentText(metric, variation) {
  return [
    metric.metricId,
    metric.description,
    metric.dataSource,
    ...(metric.aliases || []),
    ...(metric.exampleQuestions || []),
    variation.variationId,
    variation.label,
    variation.description,
    variation.sql,
    ...(variation.requiredFilters || []),
  ].filter(Boolean).join(" | ");
}

async function deleteMetricDocs(client, index, metricId) {
  if (!metricId) return;
  try {
    await client.deleteByQuery({
      index,
      body: {
        query: {
          bool: {
            should: [
              { term: { "metric_id.keyword": metricId } },
              { term: { metric_id: metricId } },
            ],
            minimum_should_match: 1,
          },
        },
      },
    });
  } catch (err) {
    if (err?.statusCode !== 404) throw err;
  }
}

async function indexMetric(metric) {
  const { index } = await getSearchConfig();
  if (!index) throw new Error("Missing OpenSearch index");
  const client = await getOpenSearch();
  await ensureIndex(client, index);
  await deleteMetricDocs(client, index, metric.metricId);

  const variations = Array.isArray(metric.variations) ? metric.variations : [];
  for (const variation of variations) {
    const schema = buildSchema(metric, variation);
    const contentText = buildContentText(metric, variation);
    const embedding = await embedText(contentText);
    const docId = `${metric.metricId}:${variation.variationId}`;
    const doc = {
      doc_id: docId,
      ...schema,
      schema,
      embedding,
    };
    await client.index({
      index,
      body: doc,
    });
  }
}

async function getMetricById(tableName, id) {
  const res = await ddb.send(new GetItemCommand({
    TableName: tableName,
    Key: { id: { S: id } },
  }));
  if (!res.Item) return null;
  return unmarshall(res.Item);
}

export const handler = async (event) => {
  const { rawPath, pathParameters, requestContext } = event;
  const method = requestContext.http.method;
  const resource = rawPath.split("/")[1];
  const tableName = TABLES[resource];

  if (!tableName) return { statusCode: 404, body: JSON.stringify({ error: "Not found" }) };

  try {
    if (method === "POST") {
      const body = JSON.parse(event.body || "{}");
      const id = generateId(ID_LENGTHS[resource]);
      const item = { id, ...body, createdAt: new Date().toISOString() };
      await ddb.send(new PutItemCommand({ TableName: tableName, Item: marshall(item) }));
      await indexMetric(item);
      return { statusCode: 201, body: JSON.stringify(item) };
    }

    const id = pathParameters?.id;
    if (!id) return { statusCode: 400, body: JSON.stringify({ error: "Missing id" }) };

    if (method === "DELETE") {
      const existing = await getMetricById(tableName, id);
      await ddb.send(new DeleteItemCommand({ TableName: tableName, Key: marshall({ id }) }));
      if (existing?.metricId) {
        const { index } = await getSearchConfig();
        if (index) {
          const client = await getOpenSearch();
          await deleteMetricDocs(client, index, existing.metricId);
        }
      }
      return { statusCode: 204, body: "" };
    }

    if (method === "PUT") {
      const body = JSON.parse(event.body || "{}");
      body.updatedAt = new Date().toISOString();
      const updates = Object.entries(body).filter(([k]) => k !== "id");
      if (!updates.length) return { statusCode: 400, body: JSON.stringify({ error: "No fields to update" }) };

      const expr = updates.map(([k], i) => `#k${i} = :v${i}`).join(", ");
      const names = Object.fromEntries(updates.map(([k], i) => [`#k${i}`, k]));
      const values = marshall(Object.fromEntries(updates.map(([, v], i) => [`:v${i}`, v])));

      await ddb.send(new UpdateItemCommand({
        TableName: tableName,
        Key: marshall({ id }),
        UpdateExpression: `SET ${expr}`,
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
      }));

      const updated = await getMetricById(tableName, id);
      if (updated) await indexMetric(updated);
      return { statusCode: 200, body: JSON.stringify(updated || { id, ...body }) };
    }

    return { statusCode: 405, body: JSON.stringify({ error: "Method not allowed" }) };
  } catch (err) {
    console.error("write-error", {
      message: err?.message,
      name: err?.name,
      statusCode: err?.statusCode,
      stack: err?.stack,
      meta: err?.meta,
      request: err?.meta?.meta?.request?.params,
      body: err?.body,
    });
    return { statusCode: 500, body: JSON.stringify({ error: err?.message || "unknown error" }) };
  }
};
