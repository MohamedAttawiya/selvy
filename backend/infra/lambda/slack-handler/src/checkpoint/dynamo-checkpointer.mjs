import {
  BatchWriteItemCommand,
  GetItemCommand,
  PutItemCommand,
  QueryCommand,
} from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import {
  BaseCheckpointSaver,
  WRITES_IDX_MAP,
  copyCheckpoint,
  getCheckpointId,
} from "@langchain/langgraph-checkpoint";

function encodeCheckpointNamespace(checkpointNs) {
  return encodeURIComponent(String(checkpointNs ?? ""));
}

function decodeCheckpointNamespace(encodedNs) {
  return decodeURIComponent(String(encodedNs || ""));
}

function checkpointSortKey(checkpointNs, checkpointId) {
  return `NS#${encodeCheckpointNamespace(checkpointNs)}#C#${checkpointId}`;
}

function writeSortKey(checkpointNs, checkpointId, taskId, index) {
  return `NS#${encodeCheckpointNamespace(checkpointNs)}#W#${checkpointId}#${taskId}#${String(index).padStart(6, "0")}`;
}

function checkpointPrefix(checkpointNs) {
  return `NS#${encodeCheckpointNamespace(checkpointNs)}#C#`;
}

function writePrefix(checkpointNs, checkpointId) {
  return `NS#${encodeCheckpointNamespace(checkpointNs)}#W#${checkpointId}#`;
}

async function serializeTyped(serde, value) {
  const [type, bytes] = await serde.dumpsTyped(value);
  return {
    type,
    payload: Buffer.from(bytes).toString("base64"),
  };
}

async function deserializeTyped(serde, type, payload) {
  if (!type || !payload) return null;
  const bytes = Buffer.from(String(payload), "base64");
  return serde.loadsTyped(type, bytes);
}

export class DynamoCheckpointSaver extends BaseCheckpointSaver {
  constructor({ client, tableName, ttlSeconds = 2592000, serde }) {
    super(serde);
    this.client = client;
    this.tableName = tableName;
    this.ttlSeconds = ttlSeconds;
  }

  _ensureThreadId(config, operation) {
    const threadId = config?.configurable?.thread_id;
    if (!threadId) {
      throw new Error(`Failed to ${operation}. RunnableConfig is missing configurable.thread_id.`);
    }
    return threadId;
  }

  _getCheckpointNamespace(config) {
    return config?.configurable?.checkpoint_ns ?? "";
  }

  _checkpointListItemToConfig(item) {
    return {
      configurable: {
        thread_id: item.thread_id,
        checkpoint_ns: item.checkpoint_ns || "",
        checkpoint_id: item.checkpoint_id,
      },
    };
  }

  async _readWrites(threadId, checkpointNs, checkpointId) {
    const prefix = writePrefix(checkpointNs, checkpointId);
    let exclusiveStartKey;
    const writeItems = [];

    while (true) {
      const query = await this.client.send(new QueryCommand({
        TableName: this.tableName,
        KeyConditionExpression: "thread_id = :thread_id AND begins_with(entry_key, :prefix)",
        ExpressionAttributeValues: {
          ":thread_id": { S: threadId },
          ":prefix": { S: prefix },
        },
        ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
      }));

      for (const rawItem of query.Items || []) {
        const item = unmarshall(rawItem);
        if (item.entry_type === "write") writeItems.push(item);
      }

      if (!query.LastEvaluatedKey) break;
      exclusiveStartKey = query.LastEvaluatedKey;
    }

    writeItems.sort((a, b) => Number(a.sort_index || 0) - Number(b.sort_index || 0));

    return Promise.all(writeItems.map(async (item) => [
      item.task_id,
      item.channel,
      await deserializeTyped(this.serde, item.value_type, item.value_payload),
    ]));
  }

  async _readCheckpointTuple(threadId, checkpointNs, checkpointId, configOverride) {
    const getRes = await this.client.send(new GetItemCommand({
      TableName: this.tableName,
      Key: marshall({
        thread_id: threadId,
        entry_key: checkpointSortKey(checkpointNs, checkpointId),
      }),
    }));

    if (!getRes.Item) return undefined;

    const item = unmarshall(getRes.Item);
    if (item.entry_type !== "checkpoint") return undefined;

    const checkpoint = await deserializeTyped(this.serde, item.checkpoint_type, item.checkpoint_payload);
    const metadata = await deserializeTyped(this.serde, item.metadata_type, item.metadata_payload);
    const pendingWrites = await this._readWrites(threadId, checkpointNs, checkpointId);

    const checkpointTuple = {
      config: configOverride || {
        configurable: {
          thread_id: threadId,
          checkpoint_ns: checkpointNs,
          checkpoint_id: checkpointId,
        },
      },
      checkpoint,
      metadata,
      pendingWrites,
    };

    if (item.parent_checkpoint_id) {
      checkpointTuple.parentConfig = {
        configurable: {
          thread_id: threadId,
          checkpoint_ns: checkpointNs,
          checkpoint_id: item.parent_checkpoint_id,
        },
      };
    }

    return checkpointTuple;
  }

  async getTuple(config) {
    const threadId = this._ensureThreadId(config, "get checkpoint tuple");
    const checkpointNs = this._getCheckpointNamespace(config);
    let checkpointId = getCheckpointId(config);

    if (!checkpointId) {
      const latest = await this.client.send(new QueryCommand({
        TableName: this.tableName,
        KeyConditionExpression: "thread_id = :thread_id AND begins_with(entry_key, :prefix)",
        ExpressionAttributeValues: {
          ":thread_id": { S: threadId },
          ":prefix": { S: checkpointPrefix(checkpointNs) },
        },
        ScanIndexForward: false,
        Limit: 1,
      }));

      const latestItem = latest.Items?.[0] ? unmarshall(latest.Items[0]) : null;
      checkpointId = latestItem?.checkpoint_id;
      if (!checkpointId) return undefined;

      return this._readCheckpointTuple(threadId, checkpointNs, checkpointId, {
        configurable: {
          thread_id: threadId,
          checkpoint_ns: checkpointNs,
          checkpoint_id: checkpointId,
        },
      });
    }

    return this._readCheckpointTuple(threadId, checkpointNs, checkpointId, config);
  }

  async *list(config, options = {}) {
    const threadId = config?.configurable?.thread_id;
    if (!threadId) return;

    const checkpointNs = config?.configurable?.checkpoint_ns;
    const prefix = checkpointNs === undefined ? "NS#" : checkpointPrefix(checkpointNs);

    let exclusiveStartKey;
    const rows = [];

    while (true) {
      const result = await this.client.send(new QueryCommand({
        TableName: this.tableName,
        KeyConditionExpression: "thread_id = :thread_id AND begins_with(entry_key, :prefix)",
        ExpressionAttributeValues: {
          ":thread_id": { S: threadId },
          ":prefix": { S: prefix },
        },
        ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
      }));

      for (const rawItem of result.Items || []) {
        const item = unmarshall(rawItem);
        if (item.entry_type !== "checkpoint") continue;
        rows.push(item);
      }

      if (!result.LastEvaluatedKey) break;
      exclusiveStartKey = result.LastEvaluatedKey;
    }

    rows.sort((a, b) => String(b.checkpoint_id || "").localeCompare(String(a.checkpoint_id || "")));

    let remaining = Number.isFinite(options.limit) ? Number(options.limit) : Number.POSITIVE_INFINITY;
    for (const item of rows) {
      if (!Number.isFinite(remaining) ? false : remaining <= 0) break;
      if (options.before?.configurable?.checkpoint_id && String(item.checkpoint_id) >= String(options.before.configurable.checkpoint_id)) {
        continue;
      }

      const metadata = await deserializeTyped(this.serde, item.metadata_type, item.metadata_payload);
      if (options.filter && typeof options.filter === "object") {
        const matched = Object.entries(options.filter).every(([key, value]) => metadata?.[key] === value);
        if (!matched) continue;
      }

      const tuple = await this._readCheckpointTuple(item.thread_id, item.checkpoint_ns || "", item.checkpoint_id);
      if (!tuple) continue;

      yield tuple;
      if (Number.isFinite(remaining)) remaining -= 1;
    }
  }

  async put(config, checkpoint, metadata) {
    const preparedCheckpoint = copyCheckpoint(checkpoint);
    const threadId = this._ensureThreadId(config, "put checkpoint");
    const checkpointNs = this._getCheckpointNamespace(config);
    const parentCheckpointId = config?.configurable?.checkpoint_id;
    const checkpointData = await serializeTyped(this.serde, preparedCheckpoint);
    const metadataData = await serializeTyped(this.serde, metadata);

    const item = {
      thread_id: threadId,
      entry_key: checkpointSortKey(checkpointNs, preparedCheckpoint.id),
      entry_type: "checkpoint",
      checkpoint_ns: checkpointNs,
      checkpoint_id: preparedCheckpoint.id,
      parent_checkpoint_id: parentCheckpointId || null,
      checkpoint_type: checkpointData.type,
      checkpoint_payload: checkpointData.payload,
      metadata_type: metadataData.type,
      metadata_payload: metadataData.payload,
      created_at: new Date().toISOString(),
      expires_at: Math.floor(Date.now() / 1000) + this.ttlSeconds,
    };

    await this.client.send(new PutItemCommand({
      TableName: this.tableName,
      Item: marshall(item, { removeUndefinedValues: true }),
    }));

    return {
      configurable: {
        thread_id: threadId,
        checkpoint_ns: checkpointNs,
        checkpoint_id: preparedCheckpoint.id,
      },
    };
  }

  async putWrites(config, writes, taskId) {
    const threadId = this._ensureThreadId(config, "put writes");
    const checkpointId = config?.configurable?.checkpoint_id;
    const checkpointNs = this._getCheckpointNamespace(config);

    if (!checkpointId) {
      throw new Error("Failed to put writes. RunnableConfig is missing configurable.checkpoint_id.");
    }

    for (let idx = 0; idx < writes.length; idx += 1) {
      const [channel, value] = writes[idx];
      const writeIndex = WRITES_IDX_MAP[channel] || idx;
      const serializedValue = await serializeTyped(this.serde, value);

      const item = {
        thread_id: threadId,
        entry_key: writeSortKey(checkpointNs, checkpointId, taskId, writeIndex),
        entry_type: "write",
        checkpoint_ns: checkpointNs,
        checkpoint_id: checkpointId,
        task_id: taskId,
        channel,
        sort_index: writeIndex,
        value_type: serializedValue.type,
        value_payload: serializedValue.payload,
        created_at: new Date().toISOString(),
        expires_at: Math.floor(Date.now() / 1000) + this.ttlSeconds,
      };

      try {
        await this.client.send(new PutItemCommand({
          TableName: this.tableName,
          Item: marshall(item, { removeUndefinedValues: true }),
          ...(writeIndex >= 0
            ? { ConditionExpression: "attribute_not_exists(thread_id) AND attribute_not_exists(entry_key)" }
            : {}),
        }));
      } catch (err) {
        if (writeIndex >= 0 && err?.name === "ConditionalCheckFailedException") {
          continue;
        }
        throw err;
      }
    }
  }

  async deleteThread(threadId) {
    let exclusiveStartKey;
    const keys = [];

    while (true) {
      const result = await this.client.send(new QueryCommand({
        TableName: this.tableName,
        KeyConditionExpression: "thread_id = :thread_id",
        ExpressionAttributeValues: {
          ":thread_id": { S: threadId },
        },
        ProjectionExpression: "thread_id, entry_key",
        ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
      }));

      for (const rawItem of result.Items || []) {
        const item = unmarshall(rawItem);
        keys.push({ thread_id: item.thread_id, entry_key: item.entry_key });
      }

      if (!result.LastEvaluatedKey) break;
      exclusiveStartKey = result.LastEvaluatedKey;
    }

    for (let i = 0; i < keys.length; i += 25) {
      const chunk = keys.slice(i, i + 25);
      await this.client.send(new BatchWriteItemCommand({
        RequestItems: {
          [this.tableName]: chunk.map((key) => ({ DeleteRequest: { Key: marshall(key) } })),
        },
      }));
    }
  }
}

export function parseNamespaceFromEntryKey(entryKey) {
  const match = String(entryKey || "").match(/^NS#([^#]*)#/);
  if (!match) return "";
  return decodeCheckpointNamespace(match[1]);
}

export const checkpointKeyUtils = {
  encodeCheckpointNamespace,
  decodeCheckpointNamespace,
  checkpointSortKey,
  writeSortKey,
  checkpointPrefix,
  writePrefix,
};
