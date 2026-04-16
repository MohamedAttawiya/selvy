import test from "node:test";
import assert from "node:assert/strict";
import { buildDateWindowFootnote, renderStructuredToSlackMrkdwn } from "../src/domain/summary-format.mjs";

test("slack renderer emits mrkdwn-safe table in code block", () => {
  const output = renderStructuredToSlackMrkdwn({
    title: "UAE Week 14",
    summary_lines: ["Overview line"],
    key_points: ["Orders increased"],
    table: {
      headers: ["Metric", "Value"],
      rows: [["Order Count", "249425"]],
    },
    next_steps: ["Compare with week 13"],
  });

  assert.match(output, /\*UAE Week 14\*/);
  assert.match(output, /```[\s\S]*Metric \| Value[\s\S]*```/);
  assert.ok(!output.includes("|---"));
});

test("date footnote includes explicit comparison windows", () => {
  const footnote = buildDateWindowFootnote({
    comparison_periods: [
      { label: "W13 2026", start: "2026-03-22", end: "2026-03-28" },
      { label: "W14 2026", start: "2026-03-29", end: "2026-04-04" },
    ],
  });

  assert.match(footnote, /W13 2026: 2026-03-22 to 2026-03-28/);
  assert.match(footnote, /W14 2026: 2026-03-29 to 2026-04-04/);
});
