import { InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime";
import { BEDROCK_SUMMARY_MODEL_ID } from "../config.mjs";

export async function formatSummaryStructured(services, question, data, summaryHint) {
  const hintClause = summaryHint
    ? `\n\nThe metric author provided this guidance on how to present results:\n"${summaryHint}"\nUse it as a recommendation, but ultimately craft the clearest, most useful response for the user.`
    : "";

  const response = await services.clients.getBedrock().send(new InvokeModelCommand({
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
    if (braceStart >= 0 && braceEnd > braceStart) candidates.push(trimmed.slice(braceStart, braceEnd + 1).trim());

    for (const c of candidates) {
      const normalized = c.replace(/^\uFEFF/, "").replace(/,\s*([}\]])/g, "$1");
      try {
        return JSON.parse(normalized);
      } catch {
        // keep trying
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
      const periodRows = rows.map((r) => ({ label: String(r[idxPeriod] ?? "").trim(), row: r })).filter((x) => x.label.length > 0);
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
          tableRows.push([def.label, formatValue(a, def), formatValue(b, def), change]);
          if (keyPoints.length < 5) keyPoints.push(`${def.label}: ${formatValue(a, def)} -> ${formatValue(b, def)} (${change})`);
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
        key_points: [`Returned ${rows.length} line items across ${foundOrderIds.length} orders.`],
        table: tableRows.length ? {
          headers: ["Order ID", "Marketplace", "City", "Store", "OTP", "Minutes Late"],
          rows: tableRows.slice(0, 20),
        } : null,
        next_steps: missingOrderIds.length ? ["Verify missing IDs or check if they are outside this dataset/window."] : [],
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
  if (parsed) return normalizeStructured(parsed);
  console.log("SELVY_DEBUG format-structured-fallback", JSON.stringify({ rawPreview: text.slice(0, 500) }));
  return buildFallbackStructured();
}

export function normalizeSlackMrkdwn(text) {
  let out = String(text || "").trim();
  if (!out) return out;
  out = out.replace(/\*\*(.*?)\*\*/g, "*$1*");
  out = out.replace(/__(.*?)__/g, "_$1_");
  out = out.replace(/^#{1,6}\s+(.+)$/gm, "*$1*");

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

export function buildDateWindowFootnote(extraction) {
  const comparisonPeriods = Array.isArray(extraction?.comparison_periods) ? extraction.comparison_periods : [];
  if (comparisonPeriods.length >= 2) {
    const parts = comparisonPeriods.map((p) => `${p.label || "period"}: ${p.start} to ${p.end}`).join(" | ");
    const source = extraction?.time_range_source === "reporting_calendar"
      ? "reporting calendar resolution"
      : "prompt/intent inference";
    return `_Date windows used: ${parts} (${source})._`;
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

export function renderStructuredToSlackMrkdwn(structured) {
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
    for (const row of rows.slice(0, 12)) lines.push(row.join(" | "));
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
