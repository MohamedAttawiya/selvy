export const json = (statusCode, body) => ({
  statusCode,
  headers: { "content-type": "application/json" },
  body: JSON.stringify(body),
});

export const hasValue = (value) => {
  if (value === null || value === undefined) return false;
  if (Array.isArray(value)) return value.some((v) => hasValue(v));
  if (typeof value === "string") return value.trim().length > 0;
  return true;
};

export const escapeRegex = (s) => String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

export function getRecentUserText(messages, maxMessages = 3) {
  const userTexts = messages
    .filter((m) => {
      const role = m.role || (typeof m._getType === "function" ? m._getType() : null);
      return role === "user" || role === "human";
    })
    .map((m) => String(m.content || "").trim())
    .filter((t) => t.length > 0);
  const recent = userTexts.slice(-maxMessages);
  return recent.join(" | ");
}

