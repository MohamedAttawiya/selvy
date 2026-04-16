import crypto from "crypto";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { DynamoDBClient, PutItemCommand, QueryCommand, ScanCommand } from "@aws-sdk/client-dynamodb";
import { BedrockRuntimeClient, InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime";
import { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand, GetQueryResultsCommand } from "@aws-sdk/client-athena";
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { StateGraph, StateSchema, MessagesValue, START, END, MemorySaver } from "@langchain/langgraph";
import { HumanMessage, AIMessage } from "@langchain/core/messages";
import { z } from "zod/v4";
import { Client as OpenSearchClient } from "@opensearch-project/opensearch";
import { AwsSigv4Signer } from "@opensearch-project/opensearch/aws";

const SELF_FUNCTION_NAME = process.env.AWS_LAMBDA_FUNCTION_NAME || "";
const BEDROCK_MODEL_ID = process.env.BEDROCK_MODEL_ID || "anthropic.claude-3-haiku-20240307-v1:0";
const BEDROCK_EXTRACT_MODEL_ID = process.env.BEDROCK_EXTRACT_MODEL_ID || BEDROCK_MODEL_ID;
const BEDROCK_REGION = process.env.BEDROCK_REGION || "us-east-1";
const BEDROCK_EMBED_MODEL_ID = process.env.BEDROCK_EMBED_MODEL_ID || "amazon.titan-embed-text-v2:0";
const BEDROCK_EMBED_DIMENSIONS = Number(process.env.BEDROCK_EMBED_DIMENSIONS || "256");
const BEDROCK_SUMMARY_MODEL_ID = process.env.BEDROCK_SUMMARY_MODEL_ID || BEDROCK_EXTRACT_MODEL_ID;
const OPENSEARCH_REGION = process.env.OPENSEARCH_REGION || process.env.AWS_REGION || "eu-central-1";
const OPENSEARCH_ENDPOINT = process.env.OPENSEARCH_ENDPOINT || "";
const OPENSEARCH_INDEX = process.env.OPENSEARCH_INDEX || "";
const SEARCH_CONFIG_TTL_MS = Number(process.env.SEARCH_CONFIG_TTL_MS || "60000");
const SSM_PREFIX = process.env.SSM_PREFIX || "selvy-dev";
const SLACK_SECRET_ARN = process.env.SLACK_SECRET_ARN || "";
const ATHENA_WORKGROUP = process.env.ATHENA_WORKGROUP || "selvy-dev-andes";
const ATHENA_DATABASE = process.env.GLUE_DATABASE || "andes";
const ATHENA_RESULTS_BUCKET = process.env.ATHENA_RESULTS_BUCKET || "";
const ATHENA_REGION = process.env.ATHENA_REGION || "us-east-1";
const ATHENA_QUERY_LAMBDA_NAME = process.env.ATHENA_QUERY_LAMBDA_NAME || "";
const { SLACK_REQUESTS_TABLE, CONVERSATIONS_TABLE, METRICS_TABLE } = process.env;
const MARKETPLACE_ALIAS_TO_ID = {
  AE: "338801",
  UAE: "338801",
  SA: "338811",
  KSA: "338811",
  EG: "623225021",
  EGYPT: "623225021",
};

let lambdaClient, athenaLambdaClient, ddb, bedrock, smClient, ssmClient, osClient, athenaClient, cachedSecrets, cachedSearchConfig, cachedSearchConfigAt = 0;
const getLambda = () => (lambdaClient ??= new LambdaClient());
const getAthenaLambda = () => (athenaLambdaClient ??= new LambdaClient({ region: ATHENA_REGION }));
const getSm = () => (smClient ??= new SecretsManagerClient());
const getSsm = () => (ssmClient ??= new SSMClient());
const getDdb = () => (ddb ??= new DynamoDBClient());
const getBedrock = () => (bedrock ??= new BedrockRuntimeClient({ region: BEDROCK_REGION }));
const getAthena = () => (athenaClient ??= new AthenaClient({ region: ATHENA_REGION }));

async function getSlackSecrets() {
  if (cachedSecrets) return cachedSecrets;
  const res = await getSm().send(new GetSecretValueCommand({ SecretId: SLACK_SECRET_ARN }));
  cachedSecrets = JSON.parse(res.SecretString);
  return cachedSecrets;
}

const json = (statusCode, body) => ({
  statusCode,
  headers: { "content-type": "application/json" },
  body: JSON.stringify(body),
});

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
    getSsm().send(new GetParameterCommand({ Name: endpointParam })),
    getSsm().send(new GetParameterCommand({ Name: indexParam })),
  ]);
  cachedSearchConfig = { endpoint: endpointRes.Parameter?.Value || "", index: indexRes.Parameter?.Value || "" };
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

function redact(value) {
  if (Array.isArray(value)) return value.map(redact);
  if (!value || typeof value !== "object") return value;
  const out = {};
  for (const [key, val] of Object.entries(value)) {
    if (/token|authorization|secret|signature|cookie/i.test(key)) {
      out[key] = "[redacted]";
    } else {
      out[key] = redact(val);
    }
  }
  return out;
}

function verifySlackSignature(event, signingSecret) {
  if (!signingSecret) return true;
  const timestamp = event.headers?.["x-slack-request-timestamp"];
  const signature = event.headers?.["x-slack-signature"];
  if (!timestamp || !signature) return false;
  if (Math.abs(Math.floor(Date.now() / 1000) - Number(timestamp)) > 300) return false;
  const baseString = `v0:${timestamp}:${event.body}`;
  const computed = "v0=" + crypto.createHmac("sha256", signingSecret).update(baseString).digest("hex");
  return crypto.timingSafeEqual(Buffer.from(computed), Buffer.from(signature));
}

function buildConversationItem(slackEvent, envelope) {
  if (!slackEvent?.channel || !slackEvent?.ts) return null;
  const rootTs = slackEvent.thread_ts || slackEvent.ts;
  const conversationId = `${slackEvent.channel}:${rootTs}`;
  const isBot = !!slackEvent.bot_id || slackEvent.subtype === "bot_message";
  return {
    conversation_id: conversationId,
    message_ts: slackEvent.ts,
    root_ts: rootTs,
    thread_ts: slackEvent.thread_ts,
    channel: slackEvent.channel,
    channel_type: slackEvent.channel_type,
    user_id: slackEvent.user,
    bot_id: slackEvent.bot_id,
    app_id: slackEvent.app_id,
    text: slackEvent.text || "",
    event_type: slackEvent.type,
    subtype: slackEvent.subtype,
    event_id: envelope?.event_id,
    client_msg_id: slackEvent.client_msg_id,
    is_bot: isBot,
    source: "slack",
    received_at: new Date().toISOString(),
    payload: JSON.stringify(slackEvent),
  };
}

async function storeConversationEvent(slackEvent, envelope) {
  if (!CONVERSATIONS_TABLE) return;
  const item = buildConversationItem(slackEvent, envelope);
  if (!item) return;
  const db = getDdb();
  await db.send(new PutItemCommand({
    TableName: CONVERSATIONS_TABLE,
    Item: marshall(item, { removeUndefinedValues: true }),
  }));
}

async function postToSlack(channel, blocks, threadTs, botToken) {
  const payload = {
    channel,
    blocks,
    text: blocks[0]?.text?.text || "Selvy response",
    ...(threadTs && { thread_ts: threadTs }),
  };
  const res = await fetch("https://slack.com/api/chat.postMessage", {
    method: "POST",
    headers: { "content-type": "application/json; charset=utf-8", authorization: `Bearer ${botToken}` },
    body: JSON.stringify(payload),
  });
  const result = await res.json();
  if (!result.ok) console.error("slack-post-error", result);
  return result;
}

async function embedText(text) {
  const response = await getBedrock().send(new InvokeModelCommand({
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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const toSqlValue = (value) => {
  if (value === null || value === undefined) return "NULL";
  if (Array.isArray(value)) return value.map(toSqlValue).join(", ");
  if (typeof value === "number") return String(value);
  const str = String(value);
  if (/^-?\d+(\.\d+)?$/.test(str)) return str;
  return `'${str.replace(/'/g, "''")}'`;
};

const hasValue = (value) => {
  if (value === null || value === undefined) return false;
  if (Array.isArray(value)) return value.some((v) => hasValue(v));
  if (typeof value === "string") return value.trim().length > 0;
  return true;
};

const escapeRegex = (s) => String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

function parseFilterValues(filter) {
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

function hasRequiredBindingInTemplate(sqlTemplate, requiredField, schema) {
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

function compileSql(template, extraction) {
  let sql = template || "";
  // Support legacy templates that stored placeholders inside quotes, e.g. ':start'
  // so value substitution does not produce doubled quotes.
  sql = sql.replace(/':([A-Za-z_][A-Za-z0-9_]*)'/g, ":$1");
  const params = {};
  if (extraction?.time_range?.start) {
    params.start = extraction.time_range.start;
    // Also populate column-specific _start params (e.g. :order_day_start, :snapshot_date_start)
    const startRe = /:([a-z_]+)_start\b/g;
    let sm;
    while ((sm = startRe.exec(sql)) !== null) {
      params[sm[1] + '_start'] = extraction.time_range.start;
    }
  }
  if (extraction?.time_range?.end) {
    params.end = extraction.time_range.end;
    const endRe = /:([a-z_]+)_end\b/g;
    let em;
    while ((em = endRe.exec(sql)) !== null) {
      params[em[1] + '_end'] = extraction.time_range.end;
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
      // If template has "column = :param", upgrade it to "column IN (...)" for multi-value inputs.
      sql = sql.replace(new RegExp(`\\b([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*:${escapeRegex(key)}\\b`, "g"), `$1 IN (${inValues})`);
      sql = sql.replaceAll(`:${key}`, inValues);
      continue;
    }
    sql = sql.replaceAll(`:${key}`, toSqlValue(val));
  }
  return sql;
}

function normalizeMarketplaceValue(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return raw;
  const upper = raw.toUpperCase();
  return MARKETPLACE_ALIAS_TO_ID[upper] || raw;
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
  const op = String((filter?.operator || "eq")).toLowerCase();
  const normalize = (v) => transform === "marketplace" ? normalizeMarketplaceValue(v) : v;
  const values = parseFilterValues(filter);
  const normalizedValues = values.map(normalize).filter((v) => v !== null && v !== undefined && String(v).trim() !== "");
  if (!normalizedValues.length) return null;

  if (mode === "prefix") {
    if (normalizedValues.length === 1) {
      return `${column} LIKE ${toSqlValue(`${normalizedValues[0]}%`)}`;
    }
    const ors = normalizedValues.map((v) => `${column} LIKE ${toSqlValue(`${v}%`)}`);
    return `(${ors.join(" OR ")})`;
  }

  if (mode === "contains_ci") {
    if (normalizedValues.length === 1) {
      return `LOWER(${column}) LIKE LOWER(${toSqlValue(`%${normalizedValues[0]}%`)})`;
    }
    const ors = normalizedValues.map((v) => `LOWER(${column}) LIKE LOWER(${toSqlValue(`%${v}%`)})`);
    return `(${ors.join(" OR ")})`;
  }

  const opMap = { eq: "=", neq: "!=", gt: ">", gte: ">=", lt: "<", lte: "<=" };
  if (op === "in" || normalizedValues.length > 1) {
    const inValues = normalizedValues.map((v) => toSqlValue(v)).join(", ");
    return `${column} IN (${inValues})`;
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

function compileSqlWithDynamicFilters(template, extraction, schema) {
  const dynamicFiltersSql = buildDynamicFiltersSql(extraction, schema);
  const withDynamic = String(template || "").replaceAll("{{dynamic_filters}}", dynamicFiltersSql);
  return compileSql(withDynamic, extraction);
}

function getUnresolvedSqlParams(sql) {
  const unresolved = new Set();
  const re = /(^|[^:]):([A-Za-z_][A-Za-z0-9_]*)/g;
  let match;
  while ((match = re.exec(sql)) !== null) {
    unresolved.add(match[2]);
  }
  return [...unresolved];
}

async function runAthenaQuery(sql) {
  if (ATHENA_QUERY_LAMBDA_NAME) {
    return runAthenaViaQueryLambda(sql);
  }

  const client = getAthena();
  const outputLocation = ATHENA_RESULTS_BUCKET ? `s3://${ATHENA_RESULTS_BUCKET}/` : undefined;

  const exec = await client.send(new StartQueryExecutionCommand({
    QueryString: sql,
    WorkGroup: ATHENA_WORKGROUP,
    QueryExecutionContext: { Database: ATHENA_DATABASE },
    ...(outputLocation ? { ResultConfiguration: { OutputLocation: outputLocation } } : {}),
  }));

  const queryId = exec.QueryExecutionId;
  let status = "QUEUED";
  for (let i = 0; i < 60; i += 1) {
    const detail = await client.send(new GetQueryExecutionCommand({ QueryExecutionId: queryId }));
    status = detail.QueryExecution?.Status?.State || "UNKNOWN";
    if (status === "SUCCEEDED") break;
    if (status === "FAILED" || status === "CANCELLED") {
      throw new Error(detail.QueryExecution?.Status?.StateChangeReason || `Athena query failed: ${status}`);
    }
    await sleep(500);
  }
  if (status !== "SUCCEEDED") throw new Error("Athena query timed out");

  const results = await client.send(new GetQueryResultsCommand({
    QueryExecutionId: queryId,
    MaxResults: 101,
  }));

  const rows = results.ResultSet?.Rows || [];
  if (!rows.length) return { columns: [], rows: [], queryId };
  const columns = rows[0].Data.map((d) => d.VarCharValue || "");
  const dataRows = rows.slice(1).map((row) => row.Data.map((d) => d.VarCharValue ?? ""));
  return { columns, rows: dataRows, queryId };
}

async function invokeQueryLambda(rawPath, method, bodyObj = null) {
  const payload = {
    rawPath,
    requestContext: { http: { method } },
    ...(bodyObj ? { body: JSON.stringify(bodyObj) } : {}),
  };
  const response = await getAthenaLambda().send(new InvokeCommand({
    FunctionName: ATHENA_QUERY_LAMBDA_NAME,
    InvocationType: "RequestResponse",
    Payload: Buffer.from(JSON.stringify(payload)),
  }));
  const raw = JSON.parse(Buffer.from(response.Payload || []).toString("utf8"));
  const statusCode = Number(raw?.statusCode || 500);
  const parsedBody = raw?.body ? JSON.parse(raw.body) : {};
  if (statusCode >= 400) {
    throw new Error(parsedBody?.error || `Query lambda failed with status ${statusCode}`);
  }
  return parsedBody;
}

async function runAthenaViaQueryLambda(sql) {
  const started = await invokeQueryLambda("/query/start", "POST", { sql });
  const queryId = started?.queryId;
  if (!queryId) throw new Error("Query lambda did not return queryId");

  for (let i = 0; i < 60; i += 1) {
    const status = await invokeQueryLambda(`/query/status/${queryId}`, "GET");
    const state = status?.state;
    if (state === "SUCCEEDED") break;
    if (state === "FAILED" || state === "CANCELLED") {
      throw new Error(status?.stateChangeReason || `Athena query ${state}`);
    }
    await sleep(500);
  }

  const result = await invokeQueryLambda(`/query/results/${queryId}`, "GET");
  return {
    columns: result?.columns || [],
    rows: result?.rows || [],
    queryId,
  };
}

function mapMetricVariationToSchema(metric, variation) {
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

async function loadMetricFromMetricsTable(metricId) {
  if (!METRICS_TABLE || !metricId) return null;
  const db = getDdb();
  const res = await db.send(new ScanCommand({
    TableName: METRICS_TABLE,
    FilterExpression: "#metricId = :metricId",
    ExpressionAttributeNames: { "#metricId": "metricId" },
    ExpressionAttributeValues: { ":metricId": { S: String(metricId) } },
    Limit: 10,
  }));
  return (res.Items || []).map(unmarshall).find((m) => m?.metricId === metricId) || null;
}

async function loadSchemaFromMetricsTable(metricId, variationId) {
  if (!variationId) return null;
  const metric = await loadMetricFromMetricsTable(metricId);
  if (!metric) return null;
  const variation = (metric.variations || []).find((v) => v?.variationId === variationId);
  if (!variation) return null;
  return mapMetricVariationToSchema(metric, variation);
}

function chooseAlternativeSchema(metric, currentVariationId, provided, contextText) {
  if (!metric || !Array.isArray(metric.variations)) return null;
  const context = String(contextText || "").toLowerCase();
  const hasOrderIdMention = /\b\d{3}-\d{7}-\d{7,8}\b/.test(context);
  const candidates = metric.variations
    .filter((v) => v?.variationId && v.variationId !== currentVariationId)
    .map((v) => mapMetricVariationToSchema(metric, v))
    .filter((s) => {
      const req = Array.isArray(s.required_filters) ? s.required_filters : [];
      if (!hasOrderIdMention && req.includes("order_id")) return false;
      return req.every((f) => provided.has(f));
    });
  if (!candidates.length) return null;

  const scoreSchema = (s) => {
    const text = `${s.variation_id} ${s.variation_label || ""} ${s.variation_description || ""}`.toLowerCase();
    let score = 0;
    if (/(^|\s)cities?(\s|$)|(^|\s)city(\s|$)/.test(context) && /city/.test(text)) score += 10;
    if (/(^|\s)cities?(\s|$)|(^|\s)city(\s|$)/.test(context) && /_city\b|by_city\b/.test(s.variation_id || "")) score += 4;
    if (/(^|\s)stores?(\s|$)/.test(context) && /store/.test(text)) score += 8;
    if (/(customer|prime|segment)/.test(context) && /(customer|prime|segment)/.test(text)) score += 8;
    if (/(category|categories|revenue|units)/.test(context) && /(category|top_categories)/.test(text)) score += 7;
    if (/(volume|overview|how many|orders?)/.test(context) && /(volume|overview|order_volume)/.test(text)) score += 5;
    if (/trace|specific order/.test(context) && /order_trace/.test(text)) score += 6;
    if (s.variation_id === "order_volume") score += 1;
    return score;
  };
  candidates.sort((a, b) => scoreSchema(b) - scoreSchema(a));
  return candidates[0];
}

async function formatSummaryStructured(question, data, summaryHint) {
  const hintClause = summaryHint
    ? `\n\nThe metric author provided this guidance on how to present results:\n"${summaryHint}"\nUse it as a recommendation, but ultimately craft the clearest, most useful response for the user.`
    : "";
  const response = await getBedrock().send(new InvokeModelCommand({
    modelId: BEDROCK_SUMMARY_MODEL_ID,
    contentType: "application/json",
    accept: "application/json",
    body: JSON.stringify({
      anthropic_version: "bedrock-2023-05-31",
      max_tokens: 700,
      temperature: 0.1,
      system: `Summarize the query result for a business user as structured JSON only.${hintClause}`,
      messages: [{
        role: "user",
        content: `User question: ${question}

Raw data:
${JSON.stringify(data, null, 2)}

Return ONLY valid JSON in this exact shape:
{
  "title": "string",
  "summary_lines": ["string"],
  "key_points": ["string"],
  "table": { "headers": ["string"], "rows": [["string"]] } | null,
  "next_steps": ["string"]
}

Rules:
- If there are 0 rows, make that explicit in summary_lines.
- Do not invent fallback orders or "closest match" unless explicitly present in raw data.
- Keep each array concise and actionable.`,
      }],
    }),
  }));
  const raw = JSON.parse(new TextDecoder().decode(response.body));
  const text = (raw.content?.[0]?.text || "").trim();
  const parseJsonFlexible = (input) => {
    if (!input) return null;
    const candidates = [];
    const trimmed = String(input).trim();
    candidates.push(trimmed);

    const fenced = trimmed.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
    if (fenced?.[1]) candidates.push(fenced[1].trim());

    const braceStart = trimmed.indexOf("{");
    const braceEnd = trimmed.lastIndexOf("}");
    if (braceStart >= 0 && braceEnd > braceStart) {
      candidates.push(trimmed.slice(braceStart, braceEnd + 1).trim());
    }

    for (const c of candidates) {
      const normalized = c
        .replace(/^\uFEFF/, "")
        .replace(/,\s*([}\]])/g, "$1");
      try {
        return JSON.parse(normalized);
      } catch {
        // try next candidate
      }
    }
    return null;
  };

  const normalizeStructured = (obj) => ({
    title: String(obj?.title || "Summary"),
    summary_lines: Array.isArray(obj?.summary_lines) ? obj.summary_lines.map((x) => String(x)) : [],
    key_points: Array.isArray(obj?.key_points) ? obj.key_points.map((x) => String(x)) : [],
    table: (obj?.table && Array.isArray(obj.table.headers) && Array.isArray(obj.table.rows))
      ? {
        headers: obj.table.headers.map((x) => String(x)),
        rows: obj.table.rows.map((r) => (Array.isArray(r) ? r.map((x) => String(x ?? "")) : [])),
      }
      : null,
    next_steps: Array.isArray(obj?.next_steps) ? obj.next_steps.map((x) => String(x)) : [],
  });

  const requestedOrderIds = Array.from(new Set((String(question || "").match(/\b\d{3}-\d{7}-\d{7,8}\b/g) || [])));
  const buildFallbackStructured = () => {
    const columns = Array.isArray(data?.columns) ? data.columns : [];
    const rows = Array.isArray(data?.rows) ? data.rows : [];
    const lowerCols = columns.map((c) => String(c || "").toLowerCase());
    const idx = (name) => lowerCols.indexOf(name);
    const idxOrder = idx("order_id");
    const idxMarketplace = idx("marketplace_id");
    const idxCity = idx("city");
    const idxStore = idx("store_name");
    const idxOtp = idx("otp");
    const idxLate = idx("mins_late");
    const idxPeriod = idx("period");

    if (idxPeriod >= 0 && rows.length >= 2) {
      const parseNum = (v) => {
        const n = Number(v);
        return Number.isFinite(n) ? n : null;
      };
      const parsePeriodSortKey = (label) => {
        const m = String(label || "").match(/w(\d{1,2})\s+(\d{4})/i);
        if (m) return Number(m[2]) * 100 + Number(m[1]);
        return null;
      };
      const periodRows = rows
        .map((r) => ({ label: String(r[idxPeriod] ?? "").trim(), row: r }))
        .filter((x) => x.label.length > 0);
      if (periodRows.length >= 2) {
        periodRows.sort((a, b) => {
          const ak = parsePeriodSortKey(a.label);
          const bk = parsePeriodSortKey(b.label);
          if (ak !== null && bk !== null) return ak - bk;
          return a.label.localeCompare(b.label);
        });
        const from = periodRows[0];
        const to = periodRows[periodRows.length - 1];
        const metricDefs = [
          { col: "order_count", label: "Order Count", kind: "int" },
          { col: "item_count", label: "Item Count", kind: "int" },
          { col: "upo", label: "Units Per Order (UPO)", kind: "float", digits: 2 },
          { col: "avg_items_per_order", label: "Avg Items Per Order", kind: "float", digits: 2 },
          { col: "avg_units_per_order", label: "Avg Units Per Order", kind: "float", digits: 2 },
          { col: "avg_order_value", label: "Avg Order Value", kind: "currency" },
          { col: "ops_usd", label: "Revenue (OPS)", kind: "currency" },
          { col: "promo_usd", label: "Promo Spend", kind: "currency" },
          { col: "total_units", label: "Total Units", kind: "int" },
        ];
        const formatValue = (n, def) => {
          if (n === null) return "N/A";
          if (def.kind === "currency") return `$${n.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
          if (def.kind === "float") return n.toLocaleString(undefined, { minimumFractionDigits: def.digits || 2, maximumFractionDigits: def.digits || 2 });
          return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
        };
        const formatDeltaPct = (a, b) => {
          if (a === null || b === null || a === 0) return "N/A";
          const pct = ((b - a) / a) * 100;
          const sign = pct > 0 ? "+" : "";
          return `${sign}${pct.toFixed(1)}%`;
        };

        const tableRows = [];
        const keyPoints = [];
        for (const def of metricDefs) {
          const idxMetric = idx(def.col);
          if (idxMetric < 0) continue;
          const a = parseNum(from.row[idxMetric]);
          const b = parseNum(to.row[idxMetric]);
          if (a === null && b === null) continue;
          const change = formatDeltaPct(a, b);
          tableRows.push([
            def.label,
            formatValue(a, def),
            formatValue(b, def),
            change,
          ]);
          if (keyPoints.length < 5) {
            keyPoints.push(`${def.label}: ${formatValue(a, def)} -> ${formatValue(b, def)} (${change})`);
          }
        }

        if (tableRows.length) {
          const marketplaceFrom = idxMarketplace >= 0 ? String(from.row[idxMarketplace] ?? "").trim() : "";
          return {
            title: `Period Comparison — ${from.label} vs ${to.label}`,
            summary_lines: [
              marketplaceFrom ? `Marketplace ${marketplaceFrom} compared across ${from.label} and ${to.label}.` : `Compared ${from.label} against ${to.label}.`,
              "The same metric query was executed for each period and compared side by side.",
            ],
            key_points: keyPoints,
            table: {
              headers: ["Metric", from.label, to.label, "Change"],
              rows: tableRows,
            },
            next_steps: [
              "Drill down by city or store to identify what drove the period shift.",
              "Repeat the same comparison for adjacent weeks to validate trend consistency.",
            ],
          };
        }
      }
    }

    if (idxOrder >= 0 && requestedOrderIds.length > 0) {
      const foundOrderIds = Array.from(new Set(rows.map((r) => String(r[idxOrder] ?? "").trim()).filter(Boolean)));
      const missingOrderIds = requestedOrderIds.filter((id) => !foundOrderIds.includes(id));
      const orderFirstRow = new Map();
      for (const r of rows) {
        const id = String(r[idxOrder] ?? "").trim();
        if (id && !orderFirstRow.has(id)) orderFirstRow.set(id, r);
      }
      const tableRows = foundOrderIds.map((id) => {
        const r = orderFirstRow.get(id) || [];
        return [
          id,
          idxMarketplace >= 0 ? String(r[idxMarketplace] ?? "") : "",
          idxCity >= 0 ? String(r[idxCity] ?? "") : "",
          idxStore >= 0 ? String(r[idxStore] ?? "") : "",
          idxOtp >= 0 ? String(r[idxOtp] ?? "") : "",
          idxLate >= 0 ? String(r[idxLate] ?? "") : "",
        ];
      });
      return {
        title: "Order Lookup Results",
        summary_lines: [
          `Found ${foundOrderIds.length} of ${requestedOrderIds.length} requested orders.`,
          ...(missingOrderIds.length ? [`Missing: ${missingOrderIds.join(", ")}`] : []),
        ],
        key_points: [
          `Returned ${rows.length} line items across ${foundOrderIds.length} orders.`,
        ],
        table: tableRows.length ? {
          headers: ["Order ID", "Marketplace", "City", "Store", "OTP", "Minutes Late"],
          rows: tableRows.slice(0, 20),
        } : null,
        next_steps: missingOrderIds.length
          ? ["Verify missing IDs or check if they are outside this dataset/window."]
          : [],
      };
    }

    const idxStoreName = idx("store_name");
    const idxOrderCount = idx("order_count");
    const idxItemCount = idx("item_count");
    const idxTotalUnits = idx("total_units");
    if (idxStoreName >= 0 && idxOrderCount >= 0 && rows.length) {
      const topRows = rows
        .slice()
        .sort((a, b) => Number(b[idxOrderCount] || 0) - Number(a[idxOrderCount] || 0))
        .slice(0, 5);
      const topTotal = topRows.reduce((sum, r) => sum + Number(r[idxOrderCount] || 0), 0);
      const allTotal = rows.reduce((sum, r) => sum + Number(r[idxOrderCount] || 0), 0);
      const pct = allTotal > 0 ? ((topTotal / allTotal) * 100).toFixed(1) : "0.0";
      return {
        title: "Top Stores Summary",
        summary_lines: [
          `Returned ${rows.length} stores ranked by order volume.`,
          `Top 5 stores account for ${pct}% of total listed orders.`,
        ],
        key_points: topRows.map((r, i) => {
          const parts = [
            `${i + 1}. ${String(r[idxStoreName] ?? "")}`,
            `orders: ${Number(r[idxOrderCount] || 0).toLocaleString()}`,
          ];
          if (idxItemCount >= 0) parts.push(`items: ${Number(r[idxItemCount] || 0).toLocaleString()}`);
          if (idxTotalUnits >= 0) parts.push(`units: ${Number(r[idxTotalUnits] || 0).toLocaleString()}`);
          return parts.join(" | ");
        }),
        table: {
          headers: columns.map((c) => String(c)),
          rows: rows.slice(0, 12).map((r) => (Array.isArray(r) ? r.map((x) => String(x ?? "")) : [])),
        },
        next_steps: [
          "Ask for a city split of top stores to understand geographic concentration.",
          "Ask for month-over-month comparison to identify trend changes.",
        ],
      };
    }

    const genericTable = (columns.length && rows.length) ? {
      headers: columns.map((c) => String(c)),
      rows: rows.slice(0, 10).map((r) => (Array.isArray(r) ? r.map((x) => String(x ?? "")) : [])),
    } : null;
    return {
      title: "Query Results",
      summary_lines: [`Returned ${rows.length} rows.`],
      key_points: [],
      table: genericTable,
      next_steps: [],
    };
  };
  const parsed = parseJsonFlexible(text);
  if (parsed) {
    return normalizeStructured(parsed);
  }
  {
    console.log("SELVY_DEBUG format-structured-fallback", JSON.stringify({ rawPreview: text.slice(0, 500) }));
    return buildFallbackStructured();
  }
}

function normalizeSlackMrkdwn(text) {
  let out = String(text || "").trim();
  if (!out) return out;
  out = out.replace(/\*\*(.*?)\*\*/g, "*$1*");
  out = out.replace(/__(.*?)__/g, "_$1_");
  out = out.replace(/^#{1,6}\s+(.+)$/gm, "*$1*");

  // Convert markdown table blocks to code blocks for Slack readability.
  const lines = out.split("\n");
  const next = [];
  let i = 0;
  while (i < lines.length) {
    if (lines[i].trim().startsWith("|")) {
      const block = [];
      while (i < lines.length && lines[i].trim().startsWith("|")) {
        block.push(lines[i].trim());
        i += 1;
      }
      const normalized = block
        .filter((l, idx) => !(idx === 1 && /^\|\s*[-:| ]+\|\s*$/.test(l)))
        .map((l) => l.replace(/^\||\|$/g, "").split("|").map((c) => c.trim()).join(" | "));
      next.push("```");
      next.push(...normalized);
      next.push("```");
      continue;
    }
    next.push(lines[i]);
    i += 1;
  }
  return next.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

function buildDateWindowFootnote(extraction) {
  const comparisonPeriods = Array.isArray(extraction?.comparison_periods) ? extraction.comparison_periods : [];
  if (comparisonPeriods.length >= 2) {
    const parts = comparisonPeriods
      .map((p) => `${p.label || "period"}: ${p.start} to ${p.end}`)
      .join(" | ");
    return `_Date windows used: ${parts} (reporting calendar resolution)._`;
  }
  const start = extraction?.time_range?.start;
  const end = extraction?.time_range?.end;
  if (!start || !end) return "";
  const grain = extraction?.time_range?.grain ? `, grain: ${extraction.time_range.grain}` : "";
  const source = extraction?.time_range_source === "reporting_calendar"
    ? "reporting calendar resolution"
    : "prompt/intent inference";
  return `_Date window used: ${start} to ${end}${grain} (${source})._`;
}

function renderStructuredToSlackMrkdwn(structured) {
  if (!structured || typeof structured !== "object") return "";
  const lines = [];
  const title = String(structured.title || "").trim();
  if (title) lines.push(`*${title}*`);

  const summaryLines = Array.isArray(structured.summary_lines) ? structured.summary_lines : [];
  for (const line of summaryLines) {
    const text = String(line || "").trim();
    if (text) lines.push(text);
  }

  const keyPoints = Array.isArray(structured.key_points) ? structured.key_points : [];
  if (keyPoints.length) {
    if (lines.length) lines.push("");
    lines.push("*Key Points*");
    for (const point of keyPoints) {
      const text = String(point || "").trim();
      if (text) lines.push(`- ${text}`);
    }
  }

  const table = structured.table && typeof structured.table === "object" ? structured.table : null;
  const headers = table && Array.isArray(table.headers) ? table.headers.map((h) => String(h || "").trim()) : [];
  const rows = table && Array.isArray(table.rows) ? table.rows.map((r) => (Array.isArray(r) ? r.map((c) => String(c ?? "").trim()) : [])) : [];
  if (headers.length && rows.length) {
    if (lines.length) lines.push("");
    lines.push("*Data*");
    lines.push("```");
    lines.push(headers.join(" | "));
    for (const row of rows.slice(0, 12)) {
      lines.push(row.join(" | "));
    }
    lines.push("```");
  }

  const nextSteps = Array.isArray(structured.next_steps) ? structured.next_steps : [];
  if (nextSteps.length) {
    if (lines.length) lines.push("");
    lines.push("*Next Steps*");
    let i = 1;
    for (const step of nextSteps) {
      const text = String(step || "").trim();
      if (!text) continue;
      lines.push(`${i}. ${text}`);
      i += 1;
    }
  }

  return normalizeSlackMrkdwn(lines.join("\n"));
}

// ── Reporting calendar date resolution ──────────────────────────────────────
const MONTH_NAMES = { jan: 1, january: 1, feb: 2, february: 2, mar: 3, march: 3, apr: 4, april: 4, may: 5, jun: 6, june: 6, jul: 7, july: 7, aug: 8, august: 8, sep: 9, september: 9, oct: 10, october: 10, nov: 11, november: 11, dec: 12, december: 12 };

function toIsoDate(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function addDaysUtc(date, days) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate() + days));
}

function getReportingWeekWindow(year, week) {
  const safeYear = Number(year);
  const safeWeek = Number(week);
  if (!Number.isFinite(safeYear) || !Number.isFinite(safeWeek) || safeWeek < 1 || safeWeek > 53) return null;
  const jan1 = new Date(Date.UTC(safeYear, 0, 1));
  const week1Start = addDaysUtc(jan1, -jan1.getUTCDay()); // Sunday start
  const start = addDaysUtc(week1Start, (safeWeek - 1) * 7);
  const end = addDaysUtc(start, 6);
  return { year: safeYear, week: safeWeek, start: toIsoDate(start), end: toIsoDate(end) };
}

function getReportingWeekForDate(dateLike) {
  const d0 = dateLike instanceof Date ? dateLike : new Date(dateLike);
  const d = new Date(Date.UTC(d0.getUTCFullYear(), d0.getUTCMonth(), d0.getUTCDate()));
  const year = d.getUTCFullYear();
  const jan1 = new Date(Date.UTC(year, 0, 1));
  const week1Start = addDaysUtc(jan1, -jan1.getUTCDay());
  const diffDays = Math.floor((d.getTime() - week1Start.getTime()) / 86400000);
  const week = Math.floor(diffDays / 7) + 1;
  if (week >= 1 && week <= 53) return { year, week };
  if (week < 1) return getReportingWeekForDate(new Date(Date.UTC(year - 1, 11, 31)));
  return getReportingWeekForDate(new Date(Date.UTC(year + 1, 0, 1)));
}

function extractWeekMentions(text, fallbackYear = new Date().getUTCFullYear()) {
  const out = [];
  const re = /\b(?:reporting\s*week|week|wk|w)\s*([0-5]?\d)(?:\D+(\d{4}))?\b/gi;
  let m;
  while ((m = re.exec(String(text || ""))) !== null) {
    const week = Number(m[1]);
    const year = m[2] ? Number(m[2]) : fallbackYear;
    if (week >= 1 && week <= 53 && year >= 2000 && year <= 2100) out.push({ week, year });
  }
  return out;
}

function parseTimeIntent(text) {
  const lower = String(text || "").toLowerCase();
  const currentYear = new Date().getUTCFullYear();

  // "W5", "week 5", "WK5", "reporting week 5" (latest mention wins)
  const weekMentions = extractWeekMentions(lower, currentYear);
  if (weekMentions.length) {
    const last = weekMentions[weekMentions.length - 1];
    return { type: "week", week: last.week, year: last.year };
  }

  // "March", "march 2025", "feb" (latest mention wins)
  let lastMonthIntent = null;
  for (const [name, num] of Object.entries(MONTH_NAMES)) {
    const monthRe = new RegExp(`\\b${name}\\b(?:\\s+(\\d{4}))?`, "ig");
    let mm;
    while ((mm = monthRe.exec(lower)) !== null) {
      lastMonthIntent = { type: "month", month: num, year: mm[1] ? parseInt(mm[1], 10) : currentYear };
    }
  }
  if (lastMonthIntent) return lastMonthIntent;

  // "last week", "this week"
  if (/\blast\s+week\b/.test(lower)) return { type: "last_week", year: currentYear };
  if (/\bthis\s+week\b/.test(lower)) return { type: "this_week", year: currentYear };
  if (/\blast\s+month\b/.test(lower)) return { type: "last_month", year: currentYear };
  if (/\bthis\s+month\b/.test(lower)) return { type: "this_month", year: currentYear };

  return null;
}

async function resolveReportingDates(intent) {
  if (!intent) return null;
  if (intent.type === "week") {
    const weekWindow = getReportingWeekWindow(intent.year, intent.week);
    if (!weekWindow) return null;
    return { start: weekWindow.start, end: weekWindow.end };
  }
  if (intent.type === "last_week" || intent.type === "this_week") {
    const current = getReportingWeekForDate(new Date());
    if (!current) return null;
    let week = current.week;
    let year = current.year;
    if (intent.type === "last_week") {
      week -= 1;
      if (week < 1) {
        year -= 1;
        week = 53;
      }
    }
    let weekWindow = getReportingWeekWindow(year, week);
    if (!weekWindow && week === 53) weekWindow = getReportingWeekWindow(year, 52);
    if (!weekWindow) return null;
    return { start: weekWindow.start, end: weekWindow.end };
  }
  if (intent.type === "month") {
    const year = Number(intent.year);
    const month = Number(intent.month);
    if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) return null;
    const start = new Date(Date.UTC(year, month - 1, 1));
    const end = new Date(Date.UTC(year, month, 0));
    return { start: toIsoDate(start), end: toIsoDate(end) };
  }
  if (intent.type === "last_month" || intent.type === "this_month") {
    const now = new Date();
    let month = now.getUTCMonth() + 1;
    let year = now.getUTCFullYear();
    if (intent.type === "last_month") {
      month -= 1;
      if (month < 1) {
        month = 12;
        year -= 1;
      }
    }
    const start = new Date(Date.UTC(year, month - 1, 1));
    const end = new Date(Date.UTC(year, month, 0));
    return { start: toIsoDate(start), end: toIsoDate(end) };
  }
  return null;
}

function detectWeekComparisonPlan(latestText, threadContextText) {
  const latest = String(latestText || "");
  if (!/\b(compare|comparison|vs|versus|against)\b/i.test(latest)) return null;
  const currentYear = new Date().getUTCFullYear();
  const latestWeeks = extractWeekMentions(latest, currentYear);
  const contextWeeks = extractWeekMentions(String(threadContextText || ""), currentYear);

  let focus = null;
  let baseline = null;
  if (latestWeeks.length >= 2) {
    focus = latestWeeks[0];
    baseline = latestWeeks[1];
  } else if (latestWeeks.length === 1) {
    baseline = latestWeeks[0];
    focus = [...contextWeeks].reverse().find((w) => !(w.week === baseline.week && w.year === baseline.year)) || null;
    if (!focus) {
      const nextWeek = baseline.week + 1;
      focus = nextWeek <= 53
        ? { week: nextWeek, year: baseline.year }
        : { week: 1, year: baseline.year + 1 };
    }
  } else if (contextWeeks.length >= 2) {
    focus = contextWeeks[contextWeeks.length - 1];
    baseline = contextWeeks[contextWeeks.length - 2];
  }

  if (!focus || !baseline) return null;
  const focusWindow = getReportingWeekWindow(focus.year, focus.week);
  const baselineWindow = getReportingWeekWindow(baseline.year, baseline.week);
  if (!focusWindow || !baselineWindow) return null;
  return {
    focus: { ...focusWindow, label: `W${focusWindow.week} ${focusWindow.year}` },
    baseline: { ...baselineWindow, label: `W${baselineWindow.week} ${baselineWindow.year}` },
  };
}

const GraphState = new StateSchema({
  messages: MessagesValue,
  embedding: z.array(z.number()).default(() => []),
  schemas: z.array(z.any()).default(() => []),
  extraction: z.any().default(() => null),
  matchedSchema: z.any().nullable().default(() => null),
  sql: z.string().nullable().default(() => null),
  data: z.any().nullable().default(() => null),
  summary_structured: z.any().nullable().default(() => null),
  summary: z.string().nullable().default(() => null),
});

const checkpointer = new MemorySaver();
const getRecentUserText = (messages, maxMessages = 3) => {
  const userTexts = messages
    .filter((m) => {
      const role = m.role || (typeof m._getType === "function" ? m._getType() : null);
      return role === "user" || role === "human";
    })
    .map((m) => String(m.content || "").trim())
    .filter((t) => t.length > 0);
  const recent = userTexts.slice(-maxMessages);
  return recent.join(" | ");
};

const graph = new StateGraph(GraphState)
  .addNode("embed", async (state) => {
    const contextText = getRecentUserText(state.messages, 3);
    const embedding = contextText ? await embedText(contextText) : [];
    return { embedding };
  })
  .addNode("search", async (state) => {
    if (!state.embedding?.length) return { schemas: [] };
    const schemas = await searchSchemas(state.embedding, 5);
    return { schemas };
  })
  .addNode("extract", async (state) => {
    const contextText = getRecentUserText(state.messages, 3);
    console.log("SELVY_DEBUG extract-input", JSON.stringify({ contextText, schemaCount: state.schemas?.length, schemaIds: (state.schemas || []).map(s => `${s.metric_id}:${s.variation_id}`) }));
    const extraction = await extractFromSchemas(contextText, state.schemas || []);
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
          comparison_periods: periods.map((p) => ({ label: p.label, week: p.week, year: p.year, start: p.start, end: p.end })),
          time_range_source: "reporting_calendar",
        },
      };
    }

    // Parse time intent from latest user message only (avoid stale week values from prior turns)
    const intent = parseTimeIntent(latestText);
    if (!intent) {
      // No parsable reporting intent; keep extractor-provided dates as-is.
      return {};
    }

    const hasConcreteDates = !!(extraction.time_range?.start && extraction.time_range?.end);
    const shouldForceReportingWeekResolution = intent.type === "week" || intent.type === "last_week" || intent.type === "this_week";
    if (hasConcreteDates && !shouldForceReportingWeekResolution) {
      return {};
    }

    console.log("resolve-dates-intent", intent);
    const resolved = await resolveReportingDates(intent);
    if (!resolved) return {};
    console.log("resolve-dates-resolved", resolved);
    return {
      extraction: {
        ...extraction,
        time_range: { ...extraction.time_range, start: resolved.start, end: resolved.end },
        time_range_source: "reporting_calendar",
      },
    };
  })
  .addNode("execute", async (state) => {
    const extraction = state.extraction;
    if (!extraction?.metric_id || !extraction?.variation_id) {
      console.log("SELVY_DEBUG execute-skip", JSON.stringify({ reason: "no metric_id or variation_id", extraction }));
      return { sql: null, data: null, matchedSchema: null };
    }
    if (!ATHENA_WORKGROUP || !ATHENA_DATABASE) {
      console.log("SELVY_DEBUG execute-skip", JSON.stringify({ reason: "no athena config" }));
      return { extraction: { ...extraction, nudge: "Athena config is missing." }, sql: null, data: null, matchedSchema: null };
    }
    const schemaCandidates = (state.schemas || []).filter(
      (s) => s.metric_id === extraction.metric_id && s.variation_id === extraction.variation_id
    );
    if (!schemaCandidates.length) {
      return { extraction: { ...extraction, nudge: "Schema not found for selected metric/variation." }, sql: null, data: null, matchedSchema: null };
    }
    let schema = schemaCandidates.find((candidate) => {
      const req = Array.isArray(candidate?.required_filters) ? candidate.required_filters : [];
      return req.every((f) => hasRequiredBindingInTemplate(candidate?.sql || "", f, candidate));
    }) || schemaCandidates[0];

    // Always prefer authoritative schema from metrics table to avoid stale OpenSearch copies.
    try {
      const authoritativeSchema = await loadSchemaFromMetricsTable(extraction.metric_id, extraction.variation_id);
      if (authoritativeSchema) {
        schema = authoritativeSchema;
        console.log("SELVY_DEBUG execute-schema-authoritative", JSON.stringify({ metric_id: extraction.metric_id, variation_id: extraction.variation_id, source: "metrics_table" }));
      }
    } catch (err) {
      console.error("schema-authoritative-error", { message: err?.message, metric_id: extraction.metric_id, variation_id: extraction.variation_id });
    }

    let required = schema.required_filters || [];
    const provided = new Set(
      (extraction.filters || [])
        .filter((f) => f?.field && hasValue(f.value))
        .map((f) => f.field)
    );
    // time_range.start/end satisfy date-related required filters
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
    let missing = required.filter((f) => !provided.has(f));
    if (missing.length) {
      try {
        const contextText = getRecentUserText(state.messages, 1);
        const metricDoc = await loadMetricFromMetricsTable(extraction.metric_id);
        const altSchema = chooseAlternativeSchema(metricDoc, schema.variation_id, provided, contextText);
        if (altSchema) {
          const prevVariation = schema.variation_id;
          schema = altSchema;
          required = schema.required_filters || [];
          missing = required.filter((f) => !provided.has(f));
          console.log("SELVY_DEBUG execute-schema-autoswitch", JSON.stringify({
            metric_id: extraction.metric_id,
            from_variation: prevVariation,
            to_variation: schema.variation_id,
            provided: [...provided],
            contextText,
          }));
        }
      } catch (err) {
        console.error("schema-autoswitch-error", { message: err?.message, metric_id: extraction.metric_id, variation_id: extraction.variation_id });
      }
      if (missing.length) {
        console.log("SELVY_DEBUG execute-missing-filters", JSON.stringify({ required, provided: [...provided], missing, extraction }));
        return { extraction: { ...extraction, nudge: `Missing required filters: ${missing.join(", ")}` }, sql: null, data: null, matchedSchema: schema };
      }
    }

    let unboundRequired = required.filter((f) => !hasRequiredBindingInTemplate(schema.sql || "", f, schema));
    if (unboundRequired.length) {
      try {
        const fallbackSchema = await loadSchemaFromMetricsTable(extraction.metric_id, extraction.variation_id);
        if (fallbackSchema) {
          const fallbackRequired = fallbackSchema.required_filters || [];
          const fallbackMissing = fallbackRequired.filter((f) => !provided.has(f));
          const fallbackUnbound = fallbackRequired.filter((f) => !hasRequiredBindingInTemplate(fallbackSchema.sql || "", f, fallbackSchema));
          if (!fallbackMissing.length && !fallbackUnbound.length) {
            schema = fallbackSchema;
            required = fallbackRequired;
            missing = fallbackMissing;
            unboundRequired = fallbackUnbound;
            console.log("SELVY_DEBUG execute-schema-fallback", JSON.stringify({ metric_id: extraction.metric_id, variation_id: extraction.variation_id, source: "metrics_table" }));
          }
        }
      } catch (err) {
        console.error("schema-fallback-error", { message: err?.message, metric_id: extraction.metric_id, variation_id: extraction.variation_id });
      }
    }

    if (missing.length) {
      console.log("SELVY_DEBUG execute-missing-filters", JSON.stringify({ required, provided: [...provided], missing, extraction }));
      return { extraction: { ...extraction, nudge: `Missing required filters: ${missing.join(", ")}` }, sql: null, data: null, matchedSchema: schema };
    }
    if (unboundRequired.length) {
      console.log("SELVY_DEBUG execute-unbound-required", JSON.stringify({ required, unboundRequired, sqlTemplate: schema.sql || "" }));
      return {
        extraction: {
          ...extraction,
          nudge: `Schema configuration error: required filters are not bound in SQL template (${unboundRequired.join(", ")}). Use :param placeholders or {{dynamic_filters}}.`,
        },
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
    if (comparisonPeriods.length >= 2) {
      const sqlParts = [];
      let mergedColumns = null;
      const mergedRows = [];
      const periods = comparisonPeriods.slice(0, 2);
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
            extraction: { ...extraction, nudge: `Missing required parameters for ${period.label || "comparison period"}: ${unresolvedPeriod.join(", ")}` },
            sql: periodSql,
            data: null,
            matchedSchema: schema,
          };
        }
        const periodData = await runAthenaQuery(periodSql);
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
        comparison_periods: periods.map((p) => ({ label: p.label, week: p.week, year: p.year, start: p.start, end: p.end })),
        time_range_source: "reporting_calendar",
      };
      console.log("SELVY_DEBUG execute-comparison", JSON.stringify({ periods: comparisonExtraction.comparison_periods, rowCount: data.rows.length }));
      return {
        extraction: comparisonExtraction,
        sql: sqlParts.join("\n\n"),
        data,
        matchedSchema: schema,
      };
    }

    const sql = compileSqlWithDynamicFilters(schema.sql || "", extraction, schema);
    console.log("SELVY_DEBUG execute-sql", JSON.stringify({ metric_id: extraction.metric_id, variation_id: schema.variation_id || extraction.variation_id, sql }));
    const unresolved = getUnresolvedSqlParams(sql);
    if (unresolved.length) {
      console.log("SELVY_DEBUG execute-unresolved", JSON.stringify({ unresolved }));
      return {
        extraction: { ...extraction, nudge: `Missing required parameters: ${unresolved.join(", ")}` },
        sql,
        data: null,
        matchedSchema: schema,
      };
    }
    try {
      const data = await runAthenaQuery(sql);
      console.log("SELVY_DEBUG execute-result", JSON.stringify({ rowCount: data?.rows?.length, columns: data?.columns, firstRows: (data?.rows || []).slice(0, 3) }));
      return { sql, data, matchedSchema: schema };
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
        matchedSchema: schema,
      };
    }
  })
  .addNode("format", async (state) => {
    if (!state.data) {
      console.log("SELVY_DEBUG format-skip", JSON.stringify({ reason: "no data", nudge: state.extraction?.nudge }));
      return { summary_structured: null };
    }
    const contextText = getRecentUserText(state.messages, 1);
    const summaryHint = state.matchedSchema?.summary_hint || "";
    console.log("SELVY_DEBUG format-input", JSON.stringify({ contextText, rowCount: state.data?.rows?.length, summaryHint }));
    const summaryStructured = await formatSummaryStructured(contextText, state.data, summaryHint);
    console.log("SELVY_DEBUG format-output", JSON.stringify({ summaryStructured }));
    return { summary_structured: summaryStructured };
  })
  .addNode("slack_mrkdwn", async (state) => {
    if (!state.summary_structured) return { summary: null };
    const summary = await renderStructuredToSlackMrkdwn(state.summary_structured);
    console.log("SELVY_DEBUG slack-mrkdwn-output", JSON.stringify({ summaryPreview: summary?.slice(0, 500) }));
    return { summary };
  })
  .addEdge(START, "embed")
  .addEdge("embed", "search")
  .addEdge("search", "extract")
  .addEdge("extract", "resolve_dates")
  .addEdge("resolve_dates", "execute")
  .addEdge("execute", "format")
  .addEdge("format", "slack_mrkdwn")
  .addEdge("slack_mrkdwn", END)
  .compile({ checkpointer });

async function loadConversationMessages(conversationId) {
  if (!CONVERSATIONS_TABLE) return [];
  const db = getDdb();
  const res = await db.send(new QueryCommand({
    TableName: CONVERSATIONS_TABLE,
    KeyConditionExpression: "conversation_id = :cid",
    ExpressionAttributeValues: { ":cid": { S: conversationId } },
    ScanIndexForward: true,
    ConsistentRead: true,
  }));
  const items = (res.Items || []).map(unmarshall);
  return items
    .filter((i) => typeof i.text === "string")
    .map((i) => (i.is_bot ? new AIMessage(i.text) : new HumanMessage(i.text)));
}

async function searchSchemas(embedding, topK) {
  const { index } = await getSearchConfig();
  if (!index) throw new Error("Missing OpenSearch index");
  const client = await getOpenSearch();
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
      const metric = await loadMetricFromMetricsTable(metricId);
      if (!metric?.variations?.length) continue;
      for (const variation of metric.variations) {
        expanded.push(mapMetricVariationToSchema(metric, variation));
      }
    } catch (err) {
      console.error("search-schemas-expand-error", { metricId, message: err?.message });
    }
  }
  if (!expanded.length) return dedup;
  const all = [...expanded, ...dedup];
  const final = [];
  const seenFinal = new Set();
  for (const schema of all) {
    const key = `${schema?.metric_id || ""}:${schema?.variation_id || ""}`;
    if (!key || seenFinal.has(key)) continue;
    seenFinal.add(key);
    final.push(schema);
  }
  return final;
}

function chooseFilterField(schema, candidates) {
  const map = (schema?.filter_map && typeof schema.filter_map === "object") ? schema.filter_map : {};
  for (const c of candidates) {
    if (Object.prototype.hasOwnProperty.call(map, c)) return c;
  }
  return candidates[0] || null;
}

function detectMarketplaceAlias(contextText) {
  const text = String(contextText || "").toLowerCase();
  if (/\buae\b|\bu\.a\.e\b|\bae\b/.test(text)) return "UAE";
  if (/\bksa\b|\bsaudi\b|\bsa\b/.test(text)) return "KSA";
  if (/\begypt\b|\begyptian\b|\beg\b/.test(text)) return "EG";
  return null;
}

function buildHeuristicExtraction(contextText, schemas) {
  const candidates = Array.isArray(schemas) ? schemas : [];
  if (!candidates.length) return null;
  const context = String(contextText || "").toLowerCase();
  const orderIds = [...new Set((String(contextText || "").match(/\b\d{3}-\d{7}-\d{7,8}\b/g) || []))];
  const hasOrderIds = orderIds.length > 0;

  const scoreSchema = (schema) => {
    const variation = String(schema?.variation_id || "").toLowerCase();
    let score = 0;
    if (hasOrderIds) {
      if (variation === "order_trace") score += 200;
      else score -= 50;
    } else if (variation === "order_trace") {
      score -= 120;
    }
    if (/\b(compare|comparison|vs|versus|against)\b/.test(context)) {
      if (variation === "order_overview") score += 40;
      if (variation === "order_volume") score += 35;
      if (variation === "avg_items_per_order") score += 20;
    }
    if (/\bcit(y|ies)\b/.test(context) && variation.includes("city")) score += 30;
    if (/\bstores?\b/.test(context) && variation.includes("store")) score += 30;
    if (/\b(category|categories)\b/.test(context) && variation.includes("categor")) score += 30;
    if (/\b(customer|prime|segment)\b/.test(context) && variation.includes("customer")) score += 25;
    if (/\b(basket|avg items|order value)\b/.test(context) && variation.includes("avg_items")) score += 25;
    if (/\b(overview|performance|performing|orders?|volume)\b/.test(context)) {
      if (variation === "order_overview") score += 20;
      if (variation === "order_volume") score += 18;
    }
    if (variation === "order_overview") score += 2;
    return score;
  };

  const sorted = [...candidates].sort((a, b) => scoreSchema(b) - scoreSchema(a));
  const schema = sorted[0];
  if (!schema?.metric_id || !schema?.variation_id) return null;

  const filters = [];
  if (hasOrderIds) {
    const orderField = chooseFilterField(schema, ["order_id"]);
    if (orderField) {
      filters.push({
        field: orderField,
        operator: orderIds.length > 1 ? "in" : "eq",
        value: orderIds.length > 1 ? orderIds : orderIds[0],
      });
    }
  }
  const marketplaceAlias = detectMarketplaceAlias(contextText);
  if (marketplaceAlias) {
    const geoField = chooseFilterField(schema, ["country_code", "marketplace", "marketplace_id"]);
    if (geoField) filters.push({ field: geoField, operator: "eq", value: marketplaceAlias });
  }

  return {
    metric_id: schema.metric_id,
    variation_id: schema.variation_id,
    filters,
    group_by: [],
    time_range: { start: null, end: null, grain: null },
    limit: null,
    confidence: 0.35,
    nudge: null,
  };
}

// -- Schema-based extraction --
async function extractFromSchemas(contextText, schemas) {
  if (!schemas.length) {
    return { metric_id: null, variation_id: null, confidence: 0, nudge: "No matching schemas found." };
  }

  const today = new Date().toISOString().split("T")[0];
  const systemPrompt = `You are an extraction engine for Selvy.
Today's date is ${today}.

You receive:
- the user's recent messages (most recent last)
- a list of candidate metric schemas (each schema includes metric_id, variation_id, label, description, data_source, required_filters, and output_columns).

Your job:
1) pick the BEST schema from the list
2) extract the required parameters
3) return ONLY a JSON object, no markdown.

Return JSON with this EXACT shape:
{
  "metric_id": "string or null",
  "variation_id": "string or null",
  "filters": [
    { "field": "string", "operator": "eq|neq|gt|lt|gte|lte|in", "value": "string or array of strings" }
  ],
  "group_by": ["string"],
  "time_range": { "start": "YYYY-MM-DD or null", "end": "YYYY-MM-DD or null", "grain": "day|week|month|null" },
  "limit": number or null,
  "confidence": 0.0 to 1.0,
  "nudge": "string or null"
}

Rules:
- metric_id and variation_id MUST come from one of the provided schemas.
- Use the filter_map keys from the chosen schema to determine valid filter fields.
- ALWAYS resolve relative time references to concrete YYYY-MM-DD dates using today's date:
  - "last week" → Monday to Sunday of the previous week
  - "this month" → 1st of current month to today
  - "March" or "march" → 2026-03-01 to 2026-03-31 (current year unless context says otherwise)
  - "last month" → 1st to last day of previous month
  - "yesterday" → yesterday's date for both start and end
  - "last 7 days" → 7 days ago to today
- If the user mentions ANY time period, you MUST populate start and end. Never leave them null when a time reference is present.
- Only set start/end to null if the user truly gives no time indication at all.
- If user asks for multiple order IDs, set operator to "in" and set value to an array of order IDs (not one comma-joined string).
- Select "order_trace" ONLY when the user explicitly provides at least one order ID.
- If no order ID is present, prefer a non-trace variation such as order_by_city/order_volume/customer_mix based on intent.
- If no schema fits, set metric_id and variation_id to null and provide a nudge.
`;

  const response = await getBedrock().send(new InvokeModelCommand({
    modelId: BEDROCK_EXTRACT_MODEL_ID,
    contentType: "application/json",
    accept: "application/json",
    body: JSON.stringify({
      anthropic_version: "bedrock-2023-05-31",
      max_tokens: 512,
      temperature: 0,
      system: systemPrompt,
      messages: [{
        role: "user",
        content: `Recent user messages (most recent last):\n${contextText}\n\nCandidate schemas:\n${JSON.stringify(schemas, null, 2)}`,
      }],
    }),
  }));

  const raw = JSON.parse(new TextDecoder().decode(response.body));
  const text = (raw.content?.[0]?.text || "").trim();
  try {
    const parsed = JSON.parse(text);
    if (parsed?.metric_id && parsed?.variation_id) return parsed;
    const fallback = buildHeuristicExtraction(contextText, schemas);
    if (fallback?.metric_id && fallback?.variation_id) {
      console.log("SELVY_DEBUG extract-fallback-used", JSON.stringify({ reason: "missing_metric_or_variation", fallback }));
      return fallback;
    }
    return parsed;
  } catch {
    // Attempt to recover a JSON object from the response
    const match = text.match(/\{[\s\S]*\}/);
    if (match) {
      try {
        const recovered = JSON.parse(match[0]);
        if (recovered?.metric_id && recovered?.variation_id) return recovered;
      } catch {
        // fall through
      }
    }
    console.error("extract-parse-error", { rawPreview: text.slice(0, 500) });
    const fallback = buildHeuristicExtraction(contextText, schemas);
    if (fallback?.metric_id && fallback?.variation_id) {
      console.log("SELVY_DEBUG extract-fallback-used", JSON.stringify({ reason: "parse_error", fallback }));
      return fallback;
    }
    return { metric_id: null, variation_id: null, confidence: 0, nudge: "I couldn't parse the extraction output." };
  }
}

async function processMessage(channel, question, messageTs, threadTs, userId, userAlias) {
  const db = getDdb();
  const secrets = await getSlackSecrets();
  const requestId = crypto.randomUUID();

  try {
    await postToSlack(channel, [{ type: "section", text: { type: "mrkdwn", text: "Working on it..." } }], threadTs, secrets.SLACK_BOT_TOKEN);

    const rootTs = threadTs || messageTs;
    const conversationId = `${channel}:${rootTs}`;
    const history = await loadConversationMessages(conversationId);
    const state = await graph.invoke(
      { messages: history },
      { configurable: { thread_id: conversationId } }
    );

    const item = {
      id: requestId,
      timestamp: new Date().toISOString(),
      userId,
      channel,
      slackTs: messageTs,
      threadTs: rootTs,
      message: question,
      status: "retrieved",
      schemaCount: state.schemas?.length || 0,
      sql: state.sql || null,
      rowCount: state.data?.rows?.length || 0,
      summary: state.summary || null,
    };
    if (userAlias) item.userAlias = userAlias;

    await db.send(new PutItemCommand({
      TableName: SLACK_REQUESTS_TABLE,
      Item: marshall(item),
    }));

    const extraction = state.extraction || { metric_id: null, variation_id: null, confidence: 0, nudge: "No extraction result." };
    let replyText = state.summary;
    if (!replyText) {
      replyText = extraction.nudge ? extraction.nudge : JSON.stringify(extraction, null, 2);
    }
    const dateFootnote = buildDateWindowFootnote(extraction);
    const hasDateFootnote = !!dateFootnote && !/Date windows? used:/i.test(replyText);
    const isJsonReply = replyText.startsWith("{");
    const replyBlocks = isJsonReply
      ? [{ type: "section", text: { type: "mrkdwn", text: "```" + replyText + "```" } }]
      : [{ type: "section", text: { type: "mrkdwn", text: hasDateFootnote ? `${replyText}\n\n${dateFootnote}` : replyText } }];
    if (isJsonReply && hasDateFootnote) {
      replyBlocks.push({ type: "section", text: { type: "mrkdwn", text: dateFootnote } });
    }
    await postToSlack(channel, replyBlocks, threadTs, secrets.SLACK_BOT_TOKEN);
  } catch (err) {
    console.error("process-error", { message: err.message, stack: err.stack });
    const errorItem = {
      id: requestId,
      timestamp: new Date().toISOString(),
      userId,
      channel,
      slackTs: messageTs,
      message: question,
      status: "error",
      error: err.message,
    };
    if (userAlias) errorItem.userAlias = userAlias;
    await db.send(new PutItemCommand({
      TableName: SLACK_REQUESTS_TABLE,
      Item: marshall(errorItem),
    }));
    await postToSlack(channel, [{ type: "section", text: { type: "mrkdwn", text: "Something went wrong. Please try again." } }], threadTs, secrets.SLACK_BOT_TOKEN);
  }
}

export const handler = async (event, context) => {
  if (event.selvy_async) {
    await processMessage(event.channel, event.question, event.message_ts, event.thread_ts, event.user_id, event.user_alias);
    return;
  }

  const rawBody = event.body || "";
  const body = event.isBase64Encoded ? Buffer.from(rawBody, "base64").toString("utf8") : rawBody;
  const parsed = JSON.parse(body || "{}");
  const requestId = context?.awsRequestId || event.requestContext?.requestId || "unknown";
  console.log("slack-event", JSON.stringify({
    requestId,
    isBase64Encoded: !!event.isBase64Encoded,
    rawBodyBytes: body.length,
    slackPayload: redact(parsed),
  }));

  if (parsed.type === "url_verification") {
    return json(200, { challenge: parsed.challenge });
  }

  if (event.headers?.["x-slack-retry-num"]) {
    return json(200, { ok: true });
  }

  const secrets = await getSlackSecrets();
  if (!verifySlackSignature({ ...event, body }, secrets.SLACK_SIGNING_SECRET)) {
    return json(401, { error: "invalid signature" });
  }

  const slackEvent = parsed.event;
  if (slackEvent?.type === "message" || slackEvent?.type === "app_mention") {
    try {
      await storeConversationEvent(slackEvent, parsed);
    } catch (err) {
      console.error("conversation-store-error", { message: err.message, stack: err.stack });
    }
  }

  const isDm = slackEvent?.type === "message" && slackEvent?.channel_type === "im" && !slackEvent?.bot_id && !slackEvent?.subtype;
  const isMention = slackEvent?.type === "app_mention";

  if (isDm || isMention) {
    const channel = slackEvent.channel;
    const messageTs = slackEvent.ts;
    const threadTs = slackEvent.thread_ts || slackEvent.ts;
    const userId = slackEvent.user || "unknown";
    const question = slackEvent.text.replace(/<@[A-Z0-9]+>/g, "").trim();
    const userAlias = null;

    if (!question) {
      await postToSlack(channel, [{ type: "section", text: { type: "mrkdwn", text: "Ask me a question!" } }], messageTs, secrets.SLACK_BOT_TOKEN);
      return json(200, { ok: true });
    }

    await getLambda().send(new InvokeCommand({
      FunctionName: SELF_FUNCTION_NAME,
      InvocationType: "Event",
      Payload: Buffer.from(JSON.stringify({
        selvy_async: true,
        channel,
        question,
        message_ts: messageTs,
        thread_ts: threadTs,
        user_id: userId,
        user_alias: userAlias,
      })),
    }));

    return json(200, { ok: true });
  }

  return json(200, { ok: true });
};
