import crypto from "crypto";
import { PutItemCommand } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import { SLACK_REQUESTS_TABLE } from "../config.mjs";
import { loadConversationMessages } from "../services/conversations.mjs";
import { getSlackSecrets } from "../services/runtime-config.mjs";
import { postToSlack } from "../services/slack.mjs";

export async function processMessage(runtime, params) {
  const {
    channel,
    question,
    messageTs,
    threadTs,
    userId,
    userAlias,
  } = params;

  const requestId = crypto.randomUUID();
  const services = runtime.services;
  const ddb = services.clients.getDdb();
  const secrets = await getSlackSecrets(services);

  try {
    await postToSlack(
      channel,
      [{ type: "section", text: { type: "mrkdwn", text: "Working on it..." } }],
      threadTs,
      secrets.SLACK_BOT_TOKEN,
    );

    const rootTs = threadTs || messageTs;
    const conversationId = `${channel}:${rootTs}`;
    const history = await loadConversationMessages(services, conversationId);

    const state = await runtime.graph.invoke(
      {
        conversation_id: conversationId,
        messages: history,
      },
      {
        configurable: {
          thread_id: conversationId,
          checkpoint_ns: "base_orchestrator",
        },
      },
    );

    const item = {
      id: requestId,
      timestamp: new Date().toISOString(),
      userId,
      channel,
      slackTs: messageTs,
      threadTs: rootTs,
      message: question,
      status: "retrieved",
      workflowId: state.routing?.workflowId || null,
      workflowConfidence: state.routing?.confidence || null,
      candidateIds: state.routing?.candidateIds || [],
      sql: state.workflow_result?.sql || null,
      rowCount: state.workflow_result?.data?.rows?.length || 0,
      summary: state.reply_text || null,
      dateResolution: state.date_resolution || null,
    };
    if (userAlias) item.userAlias = userAlias;

    if (SLACK_REQUESTS_TABLE) {
      await ddb.send(new PutItemCommand({
        TableName: SLACK_REQUESTS_TABLE,
        Item: marshall(item, { removeUndefinedValues: true }),
      }));
    }

    const replyBlocks = Array.isArray(state.reply_blocks) && state.reply_blocks.length
      ? state.reply_blocks
      : [{ type: "section", text: { type: "mrkdwn", text: state.reply_text || "No response generated." } }];

    await postToSlack(channel, replyBlocks, threadTs, secrets.SLACK_BOT_TOKEN);
  } catch (err) {
    console.error("process-error", { message: err?.message, stack: err?.stack });

    if (SLACK_REQUESTS_TABLE) {
      const errorItem = {
        id: requestId,
        timestamp: new Date().toISOString(),
        userId,
        channel,
        slackTs: messageTs,
        message: question,
        status: "error",
        error: err?.message || "unknown error",
      };
      if (userAlias) errorItem.userAlias = userAlias;

      await ddb.send(new PutItemCommand({
        TableName: SLACK_REQUESTS_TABLE,
        Item: marshall(errorItem, { removeUndefinedValues: true }),
      }));
    }

    await postToSlack(
      channel,
      [{ type: "section", text: { type: "mrkdwn", text: "Something went wrong. Please try again." } }],
      threadTs,
      secrets.SLACK_BOT_TOKEN,
    );
  }
}
