import { GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import { GetParameterCommand } from "@aws-sdk/client-ssm";
import {
  OPENSEARCH_ENDPOINT,
  OPENSEARCH_INDEX,
  SEARCH_CONFIG_TTL_MS,
  SSM_PREFIX,
  SLACK_SECRET_ARN,
} from "../config.mjs";

let cachedSecrets;
let cachedSearchConfig;
let cachedSearchConfigAt = 0;

export async function getSlackSecrets(services) {
  if (cachedSecrets) return cachedSecrets;
  const res = await services.clients.getSm().send(new GetSecretValueCommand({ SecretId: SLACK_SECRET_ARN }));
  cachedSecrets = JSON.parse(res.SecretString || "{}");
  return cachedSecrets;
}

export async function getSearchConfig(services) {
  if (cachedSearchConfig && (Date.now() - cachedSearchConfigAt) < SEARCH_CONFIG_TTL_MS) {
    return cachedSearchConfig;
  }
  if (OPENSEARCH_ENDPOINT && OPENSEARCH_INDEX) {
    cachedSearchConfig = { endpoint: OPENSEARCH_ENDPOINT, index: OPENSEARCH_INDEX };
    cachedSearchConfigAt = Date.now();
    return cachedSearchConfig;
  }
  const endpointParam = `/${SSM_PREFIX}/search/opensearch-endpoint`;
  const indexParam = `/${SSM_PREFIX}/search/metrics-index`;
  const [endpointRes, indexRes] = await Promise.all([
    services.clients.getSsm().send(new GetParameterCommand({ Name: endpointParam })),
    services.clients.getSsm().send(new GetParameterCommand({ Name: indexParam })),
  ]);
  cachedSearchConfig = {
    endpoint: endpointRes.Parameter?.Value || "",
    index: indexRes.Parameter?.Value || "",
  };
  cachedSearchConfigAt = Date.now();
  return cachedSearchConfig;
}

