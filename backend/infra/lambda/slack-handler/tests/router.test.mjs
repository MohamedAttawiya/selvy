import test from "node:test";
import assert from "node:assert/strict";
import { routeCandidatesDeterministic, routeSelectHybrid } from "../src/graph/router.mjs";

function human(text) {
  return { role: "human", content: text };
}

test("deterministic router narrows candidates by intent signals", () => {
  const registry = new Map([
    ["orders_analytics", { intentSignals: ["order", "week"] }],
    ["charts", { intentSignals: ["chart", "plot"] }],
  ]);

  const routed = routeCandidatesDeterministic({
    messages: [human("how are orders in week 14 in uae")],
    registry,
  });

  assert.deepEqual(routed.candidateIds, ["orders_analytics"]);
  assert.match(routed.reason, /deterministic/i);
});

test("deterministic router falls back to all candidates when no signal matches", () => {
  const registry = new Map([
    ["orders_analytics", { intentSignals: ["order", "week"] }],
    ["charts", { intentSignals: ["chart", "plot"] }],
  ]);

  const routed = routeCandidatesDeterministic({
    messages: [human("hello there")],
    registry,
  });

  assert.deepEqual(routed.candidateIds.sort(), ["charts", "orders_analytics"]);
});

test("hybrid selector returns single deterministic candidate without LLM", async () => {
  const decision = await routeSelectHybrid({
    services: {},
    messages: [human("orders in week 14")],
    registry: new Map(),
    candidateIds: ["orders_analytics"],
  });

  assert.equal(decision.workflowId, "orders_analytics");
  assert.equal(decision.confidence, 1);
  assert.deepEqual(decision.candidateIds, ["orders_analytics"]);
});
