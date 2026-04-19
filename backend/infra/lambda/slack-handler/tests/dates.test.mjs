import test from "node:test";
import assert from "node:assert/strict";
import { detectComparisonPlan, getReportingWeekWindow } from "../src/domain/dates.mjs";

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
  const plan = detectComparisonPlan(
    "compare week 9 with week 15 in UAE",
    "egypt overview w13",
  );

  assert.ok(plan);
  assert.equal(plan.grain, "week");
  assert.equal(plan.focus.week, 9);
  assert.equal(plan.baseline.week, 15);
  assert.equal(plan.focus.start, "2026-02-22");
  assert.equal(plan.baseline.start, "2026-04-05");
});

test("comparison plan derives month-vs-month windows", () => {
  const plan = detectComparisonPlan(
    "compare march and feb egypt",
    "",
  );

  assert.ok(plan);
  assert.equal(plan.grain, "month");
  assert.equal(plan.focus.start, "2026-03-01");
  assert.equal(plan.focus.end, "2026-03-31");
  assert.equal(plan.baseline.start, "2026-02-01");
  assert.equal(plan.baseline.end, "2026-02-28");
});

test("comparison plan derives quarter-vs-quarter windows", () => {
  const plan = detectComparisonPlan(
    "compare q1 vs q2 2026 in uae",
    "",
  );

  assert.ok(plan);
  assert.equal(plan.grain, "quarter");
  assert.equal(plan.focus.start, "2026-01-01");
  assert.equal(plan.focus.end, "2026-03-31");
  assert.equal(plan.baseline.start, "2026-04-01");
  assert.equal(plan.baseline.end, "2026-06-30");
});

test("comparison plan supports first N months vs last N months", () => {
  const plan = detectComparisonPlan(
    "first 2 months vs last 2 months egypt",
    "",
  );

  assert.ok(plan);
  assert.equal(plan.grain, "month_bucket");
  assert.equal(plan.focus.start, "2026-01-01");
  assert.equal(plan.focus.end, "2026-02-28");
});

test("comparison plan supports explicit date-range vs date-range", () => {
  const plan = detectComparisonPlan(
    "compare 2026-01-01 to 2026-01-31 vs 2026-02-01 to 2026-02-28",
    "",
  );

  assert.ok(plan);
  assert.equal(plan.grain, "custom_range");
  assert.equal(plan.focus.start, "2026-01-01");
  assert.equal(plan.focus.end, "2026-01-31");
  assert.equal(plan.baseline.start, "2026-02-01");
  assert.equal(plan.baseline.end, "2026-02-28");
});
