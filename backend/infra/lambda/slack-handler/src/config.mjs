export const SELF_FUNCTION_NAME = process.env.AWS_LAMBDA_FUNCTION_NAME || "";
export const BEDROCK_MODEL_ID = process.env.BEDROCK_MODEL_ID || "anthropic.claude-3-haiku-20240307-v1:0";
export const BEDROCK_EXTRACT_MODEL_ID = process.env.BEDROCK_EXTRACT_MODEL_ID || BEDROCK_MODEL_ID;
export const BEDROCK_REGION = process.env.BEDROCK_REGION || "us-east-1";
export const BEDROCK_EMBED_MODEL_ID = process.env.BEDROCK_EMBED_MODEL_ID || "amazon.titan-embed-text-v2:0";
export const BEDROCK_EMBED_DIMENSIONS = Number(process.env.BEDROCK_EMBED_DIMENSIONS || "256");
export const BEDROCK_SUMMARY_MODEL_ID = process.env.BEDROCK_SUMMARY_MODEL_ID || BEDROCK_EXTRACT_MODEL_ID;
export const OPENSEARCH_REGION = process.env.OPENSEARCH_REGION || process.env.AWS_REGION || "eu-central-1";
export const OPENSEARCH_ENDPOINT = process.env.OPENSEARCH_ENDPOINT || "";
export const OPENSEARCH_INDEX = process.env.OPENSEARCH_INDEX || "";
export const SEARCH_CONFIG_TTL_MS = Number(process.env.SEARCH_CONFIG_TTL_MS || "60000");
export const SSM_PREFIX = process.env.SSM_PREFIX || "selvy-dev";
export const SLACK_SECRET_ARN = process.env.SLACK_SECRET_ARN || "";
export const ATHENA_WORKGROUP = process.env.ATHENA_WORKGROUP || "selvy-dev-andes";
export const ATHENA_DATABASE = process.env.GLUE_DATABASE || "andes";
export const ATHENA_RESULTS_BUCKET = process.env.ATHENA_RESULTS_BUCKET || "";
export const ATHENA_REGION = process.env.ATHENA_REGION || "us-east-1";
export const ATHENA_QUERY_LAMBDA_NAME = process.env.ATHENA_QUERY_LAMBDA_NAME || "";
export const CHECKPOINTS_TABLE = process.env.CHECKPOINTS_TABLE || "";
export const CHECKPOINT_TTL_SECONDS = Number(process.env.CHECKPOINT_TTL_SECONDS || "2592000");

export const { SLACK_REQUESTS_TABLE, CONVERSATIONS_TABLE, METRICS_TABLE } = process.env;

export const MARKETPLACE_ALIAS_TO_ID = {
  AE: "338801",
  UAE: "338801",
  SA: "338811",
  KSA: "338811",
  EG: "623225021",
  EGYPT: "623225021",
};

