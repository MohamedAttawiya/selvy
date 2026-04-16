import { PutItemCommand, QueryCommand } from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { AIMessage, HumanMessage } from "@langchain/core/messages";
import { CONVERSATIONS_TABLE } from "../config.mjs";

export function buildConversationItem(slackEvent, envelope) {
  if (!slackEvent?.channel || !slackEvent?.ts) return null;
  const rootTs = slackEvent.thread_ts || slackEvent.ts;
  const conversationId = `${slackEvent.channel}:${rootTs}`;
  const isBot = !!slackEvent.bot_id || slackEvent.subtype === "bot_message";
  return {
    conversation_id: conversationId,
    message_ts: slackEvent.ts,
    root_ts: rootTs,
    thread_ts: slackEvent.thread_ts,
    channel: slackEvent.channel,
    channel_type: slackEvent.channel_type,
    user_id: slackEvent.user,
    bot_id: slackEvent.bot_id,
    app_id: slackEvent.app_id,
    text: slackEvent.text || "",
    event_type: slackEvent.type,
    subtype: slackEvent.subtype,
    event_id: envelope?.event_id,
    client_msg_id: slackEvent.client_msg_id,
    is_bot: isBot,
    source: "slack",
    received_at: new Date().toISOString(),
    payload: JSON.stringify(slackEvent),
  };
}

export async function storeConversationEvent(services, slackEvent, envelope) {
  if (!CONVERSATIONS_TABLE) return;
  const item = buildConversationItem(slackEvent, envelope);
  if (!item) return;
  await services.clients.getDdb().send(new PutItemCommand({
    TableName: CONVERSATIONS_TABLE,
    Item: marshall(item, { removeUndefinedValues: true }),
  }));
}

export async function loadConversationMessages(services, conversationId) {
  if (!CONVERSATIONS_TABLE) return [];
  const res = await services.clients.getDdb().send(new QueryCommand({
    TableName: CONVERSATIONS_TABLE,
    KeyConditionExpression: "conversation_id = :cid",
    ExpressionAttributeValues: { ":cid": { S: conversationId } },
    ScanIndexForward: true,
    ConsistentRead: true,
  }));
  const items = (res.Items || []).map(unmarshall);
  return items
    .filter((i) => typeof i.text === "string")
    .map((i) => (i.is_bot ? new AIMessage(i.text) : new HumanMessage(i.text)));
}

