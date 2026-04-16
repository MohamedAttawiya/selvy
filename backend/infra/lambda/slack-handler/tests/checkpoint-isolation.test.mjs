import test from "node:test";
import assert from "node:assert/strict";
import { checkpointKeyUtils, parseNamespaceFromEntryKey } from "../src/checkpoint/dynamo-checkpointer.mjs";

test("checkpoint keys isolate namespaces for same thread", () => {
  const keyA = checkpointKeyUtils.checkpointSortKey("orders_analytics", "abc123");
  const keyB = checkpointKeyUtils.checkpointSortKey("charting", "abc123");

  assert.notEqual(keyA, keyB);
  assert.equal(parseNamespaceFromEntryKey(keyA), "orders_analytics");
  assert.equal(parseNamespaceFromEntryKey(keyB), "charting");
});

test("write prefixes are workflow-scoped", () => {
  const writePrefixA = checkpointKeyUtils.writePrefix("orders_analytics", "cp_1");
  const writePrefixB = checkpointKeyUtils.writePrefix("orders_analytics", "cp_2");

  assert.notEqual(writePrefixA, writePrefixB);
  assert.match(writePrefixA, /orders_analytics/);
});
