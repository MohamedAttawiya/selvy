import test from "node:test";
import assert from "node:assert/strict";
import { detectWeekComparisonPlan, getReportingWeekWindow } from "../src/domain/dates.mjs";

test("reporting week boundaries align with reporting calendar", () => {
  assert.deepEqual(getReportingWeekWindow(2026, 13), {
    year: 2026,
    week: 13,
    start: "2026-03-22",
    end: "2026-03-28",
  });
  assert.deepEqual(getReportingWeekWindow(2026, 14), {
    year: 2026,
    week: 14,
    start: "2026-03-29",
    end: "2026-04-04",
  });
  assert.deepEqual(getReportingWeekWindow(2026, 15), {
    year: 2026,
    week: 15,
    start: "2026-04-05",
    end: "2026-04-11",
  });
});

test("comparison plan derives two windows from user compare request", () => {
  const plan = detectWeekComparisonPlan(
    "compare week 9 with week 15 in UAE",
    "egypt overview w13",
  );

  assert.ok(plan);
  assert.equal(plan.focus.week, 9);
  assert.equal(plan.baseline.week, 15);
  assert.equal(plan.focus.start, "2026-02-22");
  assert.equal(plan.baseline.start, "2026-04-05");
});
