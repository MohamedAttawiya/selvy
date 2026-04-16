import { InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime";
import { BEDROCK_EXTRACT_MODEL_ID } from "../../config.mjs";

function chooseFilterField(schema, candidates) {
  const map = (schema?.filter_map && typeof schema.filter_map === "object") ? schema.filter_map : {};
  for (const candidate of candidates) {
    if (Object.prototype.hasOwnProperty.call(map, candidate)) return candidate;
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

export async function extractFromSchemas(services, contextText, schemas) {
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
- ALWAYS resolve relative time references to concrete YYYY-MM-DD dates using today's date.
- If user asks for multiple order IDs, set operator to "in" and value to an array.
- Select "order_trace" ONLY when the user explicitly provides at least one order ID.
- If no order ID is present, prefer a non-trace variation such as order_by_city/order_volume/customer_mix based on intent.
- If no schema fits, set metric_id and variation_id to null and provide a nudge.`;

  const response = await services.clients.getBedrock().send(new InvokeModelCommand({
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
    const match = text.match(/\{[\s\S]*\}/);
    if (match) {
      try {
        const recovered = JSON.parse(match[0]);
        if (recovered?.metric_id && recovered?.variation_id) return recovered;
      } catch {
        // ignore
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
