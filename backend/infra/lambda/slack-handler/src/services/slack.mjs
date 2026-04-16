import crypto from "crypto";

export function redact(value) {
  if (Array.isArray(value)) return value.map(redact);
  if (!value || typeof value !== "object") return value;
  const out = {};
  for (const [key, val] of Object.entries(value)) {
    if (/token|authorization|secret|signature|cookie/i.test(key)) {
      out[key] = "[redacted]";
    } else {
      out[key] = redact(val);
    }
  }
  return out;
}

export function verifySlackSignature(event, signingSecret) {
  if (!signingSecret) return true;
  const timestamp = event.headers?.["x-slack-request-timestamp"];
  const signature = event.headers?.["x-slack-signature"];
  if (!timestamp || !signature) return false;
  if (Math.abs(Math.floor(Date.now() / 1000) - Number(timestamp)) > 300) return false;
  const baseString = `v0:${timestamp}:${event.body}`;
  const computed = `v0=${crypto.createHmac("sha256", signingSecret).update(baseString).digest("hex")}`;
  return crypto.timingSafeEqual(Buffer.from(computed), Buffer.from(signature));
}

export async function postToSlack(channel, blocks, threadTs, botToken) {
  const payload = {
    channel,
    blocks,
    text: blocks[0]?.text?.text || "Selvy response",
    ...(threadTs && { thread_ts: threadTs }),
  };
  const res = await fetch("https://slack.com/api/chat.postMessage", {
    method: "POST",
    headers: {
      "content-type": "application/json; charset=utf-8",
      authorization: `Bearer ${botToken}`,
    },
    body: JSON.stringify(payload),
  });
  const result = await res.json();
  if (!result.ok) console.error("slack-post-error", result);
  return result;
}

