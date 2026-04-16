import { InvokeCommand } from "@aws-sdk/client-lambda";
import { SELF_FUNCTION_NAME } from "../config.mjs";
import { json } from "../domain/common.mjs";
import { getRuntime } from "../runtime.mjs";
import { storeConversationEvent } from "../services/conversations.mjs";
import { getSlackSecrets } from "../services/runtime-config.mjs";
import { postToSlack, redact, verifySlackSignature } from "../services/slack.mjs";
import { processMessage } from "./process-message.mjs";

export const handler = async (event, context) => {
  const runtime = getRuntime();

  if (event.selvy_async) {
    await processMessage(runtime, {
      channel: event.channel,
      question: event.question,
      messageTs: event.message_ts,
      threadTs: event.thread_ts,
      userId: event.user_id,
      userAlias: event.user_alias,
    });
    return;
  }

  const rawBody = event.body || "";
  const body = event.isBase64Encoded ? Buffer.from(rawBody, "base64").toString("utf8") : rawBody;
  const parsed = JSON.parse(body || "{}");
  const requestId = context?.awsRequestId || event.requestContext?.requestId || "unknown";

  console.log("slack-event", JSON.stringify({
    requestId,
    isBase64Encoded: !!event.isBase64Encoded,
    rawBodyBytes: body.length,
    slackPayload: redact(parsed),
  }));

  if (parsed.type === "url_verification") {
    return json(200, { challenge: parsed.challenge });
  }

  if (event.headers?.["x-slack-retry-num"]) {
    return json(200, { ok: true });
  }

  const secrets = await getSlackSecrets(runtime.services);
  if (!verifySlackSignature({ ...event, body }, secrets.SLACK_SIGNING_SECRET)) {
    return json(401, { error: "invalid signature" });
  }

  const slackEvent = parsed.event;
  if (slackEvent?.type === "message" || slackEvent?.type === "app_mention") {
    try {
      await storeConversationEvent(runtime.services, slackEvent, parsed);
    } catch (err) {
      console.error("conversation-store-error", { message: err?.message, stack: err?.stack });
    }
  }

  const isDm = slackEvent?.type === "message"
    && slackEvent?.channel_type === "im"
    && !slackEvent?.bot_id
    && !slackEvent?.subtype;
  const isMention = slackEvent?.type === "app_mention";

  if (isDm || isMention) {
    const channel = slackEvent.channel;
    const messageTs = slackEvent.ts;
    const threadTs = slackEvent.thread_ts || slackEvent.ts;
    const userId = slackEvent.user || "unknown";
    const question = String(slackEvent.text || "").replace(/<@[A-Z0-9]+>/g, "").trim();
    const userAlias = null;

    if (!question) {
      await postToSlack(channel, [{ type: "section", text: { type: "mrkdwn", text: "Ask me a question!" } }], messageTs, secrets.SLACK_BOT_TOKEN);
      return json(200, { ok: true });
    }

    await runtime.services.clients.getLambda().send(new InvokeCommand({
      FunctionName: SELF_FUNCTION_NAME,
      InvocationType: "Event",
      Payload: Buffer.from(JSON.stringify({
        selvy_async: true,
        channel,
        question,
        message_ts: messageTs,
        thread_ts: threadTs,
        user_id: userId,
        user_alias: userAlias,
      })),
    }));

    return json(200, { ok: true });
  }

  return json(200, { ok: true });
};
