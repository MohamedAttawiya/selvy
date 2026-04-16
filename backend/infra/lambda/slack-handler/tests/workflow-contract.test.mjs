import test from "node:test";
import assert from "node:assert/strict";
import { assertWorkflowContract } from "../src/workflows/contract.mjs";
import { createWorkflowRegistry } from "../src/workflows/registry.mjs";

const stubServices = {
  clients: {
    getBedrock: () => ({ send: async () => ({}) }),
    getDdb: () => ({ send: async () => ({}) }),
    getOpenSearch: () => ({ search: async () => ({ hits: { hits: [] } }) }),
    getSsm: () => ({ send: async () => ({ Parameter: { Value: "" } }) }),
    getSm: () => ({ send: async () => ({ SecretString: "{}" }) }),
    getAthena: () => ({ send: async () => ({}) }),
    getAthenaLambda: () => ({ send: async () => ({}) }),
    getLambda: () => ({ send: async () => ({}) }),
  },
};

const stubCheckpointer = {
  getTuple: async () => undefined,
  list: async function* list() {},
  put: async () => ({ configurable: {} }),
  putWrites: async () => {},
  deleteThread: async () => {},
};

test("workflow registry enforces WorkflowContract", () => {
  const registry = createWorkflowRegistry({ services: stubServices, checkpointer: stubCheckpointer });
  assert.equal(registry.size >= 1, true);

  for (const workflow of registry.values()) {
    assert.doesNotThrow(() => assertWorkflowContract(workflow));
    assert.equal(typeof workflow.id, "string");
    assert.equal(Array.isArray(workflow.capabilities), true);
    assert.equal(typeof workflow.buildSubgraph, "function");
    assert.equal(typeof workflow.run, "function");
  }
});

test("invalid workflow is rejected", () => {
  assert.throws(() => assertWorkflowContract({ id: "bad" }), /Workflow contract violation/);
});
