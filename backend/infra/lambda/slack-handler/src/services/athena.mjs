import { InvokeCommand } from "@aws-sdk/client-lambda";
import {
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
} from "@aws-sdk/client-athena";
import {
  ATHENA_DATABASE,
  ATHENA_QUERY_LAMBDA_NAME,
  ATHENA_RESULTS_BUCKET,
  ATHENA_WORKGROUP,
} from "../config.mjs";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function invokeQueryLambda(services, rawPath, method, bodyObj = null) {
  const payload = {
    rawPath,
    requestContext: { http: { method } },
    ...(bodyObj ? { body: JSON.stringify(bodyObj) } : {}),
  };
  const response = await services.clients.getAthenaLambda().send(new InvokeCommand({
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

async function runAthenaViaQueryLambda(services, sql) {
  const started = await invokeQueryLambda(services, "/query/start", "POST", { sql });
  const queryId = started?.queryId;
  if (!queryId) throw new Error("Query lambda did not return queryId");

  for (let i = 0; i < 60; i += 1) {
    const status = await invokeQueryLambda(services, `/query/status/${queryId}`, "GET");
    const state = status?.state;
    if (state === "SUCCEEDED") break;
    if (state === "FAILED" || state === "CANCELLED") {
      throw new Error(status?.stateChangeReason || `Athena query ${state}`);
    }
    await sleep(500);
  }

  const result = await invokeQueryLambda(services, `/query/results/${queryId}`, "GET");
  return {
    columns: result?.columns || [],
    rows: result?.rows || [],
    queryId,
  };
}

export async function runAthenaQuery(services, sql) {
  if (ATHENA_QUERY_LAMBDA_NAME) {
    return runAthenaViaQueryLambda(services, sql);
  }

  const outputLocation = ATHENA_RESULTS_BUCKET ? `s3://${ATHENA_RESULTS_BUCKET}/` : undefined;
  const client = services.clients.getAthena();
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

