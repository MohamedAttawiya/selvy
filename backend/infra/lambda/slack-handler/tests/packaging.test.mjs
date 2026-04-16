import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const lambdaDir = path.resolve(process.cwd());

test("lambda package path excludes debug artifact files", () => {
  const names = fs.readdirSync(lambdaDir);
  const forbidden = names.filter((name) => {
    const lower = name.toLowerCase();
    if (!lower.endsWith(".json")) return false;
    if (lower === "package.json" || lower === "package-lock.json") return false;
    return /^(last|recent|window|read-|write-|req-)/.test(lower) || lower.includes("orders_analytics_item");
  });

  assert.deepEqual(forbidden, []);
});
