import { MemorySaver } from "@langchain/langgraph";
import { CHECKPOINTS_TABLE, CHECKPOINT_TTL_SECONDS } from "../config.mjs";
import { DynamoCheckpointSaver } from "./dynamo-checkpointer.mjs";

export function createCheckpointSaver(services) {
  if (!CHECKPOINTS_TABLE) {
    console.warn("checkpointer-warning", "CHECKPOINTS_TABLE not configured; falling back to MemorySaver");
    return new MemorySaver();
  }

  return new DynamoCheckpointSaver({
    client: services.clients.getDdb(),
    tableName: CHECKPOINTS_TABLE,
    ttlSeconds: CHECKPOINT_TTL_SECONDS,
  });
}
