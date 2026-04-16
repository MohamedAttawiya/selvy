import { InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime";
import { BEDROCK_EMBED_DIMENSIONS, BEDROCK_EMBED_MODEL_ID } from "../config.mjs";

export async function embedText(services, text) {
  const response = await services.clients.getBedrock().send(new InvokeModelCommand({
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
