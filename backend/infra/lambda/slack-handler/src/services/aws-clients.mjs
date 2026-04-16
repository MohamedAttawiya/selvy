import { LambdaClient } from "@aws-sdk/client-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { BedrockRuntimeClient } from "@aws-sdk/client-bedrock-runtime";
import { SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { SSMClient } from "@aws-sdk/client-ssm";
import { AthenaClient } from "@aws-sdk/client-athena";
import { Client as OpenSearchClient } from "@opensearch-project/opensearch";
import { AwsSigv4Signer } from "@opensearch-project/opensearch/aws";
import {
  ATHENA_REGION,
  BEDROCK_REGION,
  OPENSEARCH_REGION,
} from "../config.mjs";

let lambdaClient;
let athenaLambdaClient;
let ddbClient;
let bedrockClient;
let secretsClient;
let ssmClient;
let athenaClient;
let osClient;

export const getLambda = () => (lambdaClient ??= new LambdaClient());
export const getAthenaLambda = () => (athenaLambdaClient ??= new LambdaClient({ region: ATHENA_REGION }));
export const getDdb = () => (ddbClient ??= new DynamoDBClient());
export const getBedrock = () => (bedrockClient ??= new BedrockRuntimeClient({ region: BEDROCK_REGION }));
export const getSm = () => (secretsClient ??= new SecretsManagerClient());
export const getSsm = () => (ssmClient ??= new SSMClient());
export const getAthena = () => (athenaClient ??= new AthenaClient({ region: ATHENA_REGION }));

export function getOpenSearch(endpoint) {
  if (osClient) return osClient;
  if (!endpoint) throw new Error("Missing OpenSearch endpoint");
  osClient = new OpenSearchClient({
    ...AwsSigv4Signer({ region: OPENSEARCH_REGION, service: "aoss" }),
    node: endpoint,
  });
  return osClient;
}

export function buildServices(overrides = {}) {
  const hooks = overrides.hooks || {};
  return {
    clients: {
      getLambda,
      getAthenaLambda,
      getDdb,
      getBedrock,
      getSm,
      getSsm,
      getAthena,
      getOpenSearch,
    },
    hooks: {
      beforeRequest: hooks.beforeRequest || (async () => {}),
      afterRequest: hooks.afterRequest || (async () => {}),
      onError: hooks.onError || (async () => {}),
    },
  };
}

