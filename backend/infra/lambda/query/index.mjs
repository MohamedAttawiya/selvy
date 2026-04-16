import { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand, GetQueryResultsCommand } from "@aws-sdk/client-athena";
import { GlueClient, GetTablesCommand } from "@aws-sdk/client-glue";

const WORKGROUP = process.env.ATHENA_WORKGROUP || "selvy-dev-andes";
const DATABASE = process.env.GLUE_DATABASE || "andes";
const REGION = process.env.AWS_REGION || "us-east-1";

const athena = new AthenaClient({ region: REGION });
const glue = new GlueClient({ region: REGION });

const MAX_COLS = 20;
const MAX_ROWS = 100;

const cors = {
  "content-type": "application/json",
  "access-control-allow-origin": "*",
  "access-control-allow-headers": "Authorization,Content-Type",
  "access-control-allow-methods": "GET,POST,OPTIONS",
};

const json = (statusCode, body) => ({ statusCode, headers: cors, body: JSON.stringify(body) });

async function listTables() {
  const res = await glue.send(new GetTablesCommand({ DatabaseName: DATABASE }));
  return (res.TableList || []).map(t => ({
    name: t.Name,
    isResourceLink: !!t.TargetTable,
    columns: (t.StorageDescriptor?.Columns || []).map(c => ({ name: c.Name, type: c.Type })),
  }));
}

async function startQuery(sql) {
  const res = await athena.send(new StartQueryExecutionCommand({
    QueryString: sql,
    WorkGroup: WORKGROUP,
    QueryExecutionContext: { Database: DATABASE },
  }));
  return { queryId: res.QueryExecutionId };
}

async function getQueryStatus(queryId) {
  const res = await athena.send(new GetQueryExecutionCommand({ QueryExecutionId: queryId }));
  const exec = res.QueryExecution;
  return {
    queryId,
    state: exec?.Status?.State,
    stateChangeReason: exec?.Status?.StateChangeReason || null,
  };
}

async function getQueryResults(queryId) {
  const data = await athena.send(new GetQueryResultsCommand({
    QueryExecutionId: queryId,
    MaxResults: MAX_ROWS + 1,
  }));

  const resultRows = data.ResultSet?.Rows || [];
  if (resultRows.length === 0) return { columns: [], rows: [], queryId };

  const allColumns = resultRows[0].Data.map(d => d.VarCharValue || "");
  const columns = allColumns.slice(0, MAX_COLS);

  const rows = resultRows.slice(1, MAX_ROWS + 1).map(row => {
    const vals = row.Data.map(d => d.VarCharValue ?? "");
    return vals.slice(0, MAX_COLS);
  });

  return { columns, rows, queryId, totalColumns: allColumns.length, truncatedColumns: allColumns.length > MAX_COLS };
}

export const handler = async (event) => {
  if (event.requestContext?.http?.method === "OPTIONS") return json(200, { ok: true });

  const path = event.rawPath;
  const method = event.requestContext?.http?.method;

  try {
    if (path === "/query/tables" && method === "GET") {
      return json(200, await listTables());
    }

    // Start a query — returns immediately with queryId
    if (path === "/query/start" && method === "POST") {
      const body = JSON.parse(event.body || "{}");
      if (!body.sql) return json(400, { error: "Missing sql field" });
      return json(200, await startQuery(body.sql));
    }

    // Poll query status
    if (path.startsWith("/query/status/") && method === "GET") {
      const queryId = path.split("/query/status/")[1];
      if (!queryId) return json(400, { error: "Missing queryId" });
      return json(200, await getQueryStatus(queryId));
    }

    // Fetch results (only call after status === SUCCEEDED)
    if (path.startsWith("/query/results/") && method === "GET") {
      const queryId = path.split("/query/results/")[1];
      if (!queryId) return json(400, { error: "Missing queryId" });
      return json(200, await getQueryResults(queryId));
    }

    return json(404, { error: "Not found" });
  } catch (err) {
    console.error("query-error", err);
    return json(500, { error: err.message });
  }
};
