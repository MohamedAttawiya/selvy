function toIsoDate(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function addDaysUtc(date, days) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate() + days));
}

export function getReportingWeekWindow(year, week) {
  const safeYear = Number(year);
  const safeWeek = Number(week);
  if (!Number.isFinite(safeYear) || !Number.isFinite(safeWeek) || safeWeek < 1 || safeWeek > 53) return null;
  const jan1 = new Date(Date.UTC(safeYear, 0, 1));
  const week1Start = addDaysUtc(jan1, -jan1.getUTCDay());
  const start = addDaysUtc(week1Start, (safeWeek - 1) * 7);
  const end = addDaysUtc(start, 6);
  return { year: safeYear, week: safeWeek, start: toIsoDate(start), end: toIsoDate(end) };
}

export function getReportingWeekForDate(dateLike) {
  const d0 = dateLike instanceof Date ? dateLike : new Date(dateLike);
  const d = new Date(Date.UTC(d0.getUTCFullYear(), d0.getUTCMonth(), d0.getUTCDate()));
  const year = d.getUTCFullYear();
  const jan1 = new Date(Date.UTC(year, 0, 1));
  const week1Start = addDaysUtc(jan1, -jan1.getUTCDay());
  const diffDays = Math.floor((d.getTime() - week1Start.getTime()) / 86400000);
  const week = Math.floor(diffDays / 7) + 1;
  if (week >= 1 && week <= 53) return { year, week };
  if (week < 1) return getReportingWeekForDate(new Date(Date.UTC(year - 1, 11, 31)));
  return getReportingWeekForDate(new Date(Date.UTC(year + 1, 0, 1)));
}

export function extractWeekMentions(text, fallbackYear = new Date().getUTCFullYear()) {
  const out = [];
  const re = /\b(?:reporting\s*week|week|wk|w)\s*([0-5]?\d)(?:\D+(\d{4}))?\b/gi;
  let m;
  while ((m = re.exec(String(text || ""))) !== null) {
    const week = Number(m[1]);
    const year = m[2] ? Number(m[2]) : fallbackYear;
    if (week >= 1 && week <= 53 && year >= 2000 && year <= 2100) out.push({ week, year });
  }
  return out;
}

const MONTH_NAMES = { jan: 1, january: 1, feb: 2, february: 2, mar: 3, march: 3, apr: 4, april: 4, may: 5, jun: 6, june: 6, jul: 7, july: 7, aug: 8, august: 8, sep: 9, september: 9, oct: 10, october: 10, nov: 11, november: 11, dec: 12, december: 12 };
const MONTH_SHORT_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

export function getMonthWindow(year, month) {
  const safeYear = Number(year);
  const safeMonth = Number(month);
  if (!Number.isFinite(safeYear) || !Number.isFinite(safeMonth) || safeMonth < 1 || safeMonth > 12) return null;
  const start = new Date(Date.UTC(safeYear, safeMonth - 1, 1));
  const end = new Date(Date.UTC(safeYear, safeMonth, 0));
  return { year: safeYear, month: safeMonth, start: toIsoDate(start), end: toIsoDate(end) };
}

export function extractMonthMentions(text, fallbackYear = new Date().getUTCFullYear()) {
  const out = [];
  const re = /\b(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sep|october|oct|november|nov|december|dec)\b(?:\s+(\d{4}))?/gi;
  let m;
  while ((m = re.exec(String(text || ""))) !== null) {
    const token = String(m[1] || "").toLowerCase();
    const month = MONTH_NAMES[token];
    const year = m[2] ? Number(m[2]) : fallbackYear;
    if (month && year >= 2000 && year <= 2100) out.push({ month, year });
  }
  return out;
}

export function getQuarterWindow(year, quarter) {
  const safeYear = Number(year);
  const safeQuarter = Number(quarter);
  if (!Number.isFinite(safeYear) || !Number.isFinite(safeQuarter) || safeQuarter < 1 || safeQuarter > 4) return null;
  const startMonth = ((safeQuarter - 1) * 3) + 1;
  const start = new Date(Date.UTC(safeYear, startMonth - 1, 1));
  const end = new Date(Date.UTC(safeYear, startMonth + 2, 0));
  return { year: safeYear, quarter: safeQuarter, start: toIsoDate(start), end: toIsoDate(end) };
}

export function extractQuarterMentions(text, fallbackYear = new Date().getUTCFullYear()) {
  const out = [];
  const re = /\b(?:q([1-4])|quarter\s*([1-4]))(?:\s+(\d{4}))?\b/gi;
  let m;
  while ((m = re.exec(String(text || ""))) !== null) {
    const quarter = Number(m[1] || m[2]);
    const year = m[3] ? Number(m[3]) : fallbackYear;
    if (quarter >= 1 && quarter <= 4 && year >= 2000 && year <= 2100) out.push({ quarter, year });
  }
  return out;
}

function isValidIsoDate(dateText) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(dateText || ""))) return false;
  const d = new Date(`${dateText}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return false;
  return toIsoDate(d) === dateText;
}

function normalizeRange(start, end) {
  if (!isValidIsoDate(start) || !isValidIsoDate(end)) return null;
  return start <= end ? { start, end } : { start: end, end: start };
}

export function extractIsoDateRanges(text) {
  const ranges = [];
  const re = /(\d{4}-\d{2}-\d{2})\s*(?:to|through|until|-)\s*(\d{4}-\d{2}-\d{2})/gi;
  let m;
  while ((m = re.exec(String(text || ""))) !== null) {
    const range = normalizeRange(m[1], m[2]);
    if (range) ranges.push(range);
  }
  return ranges;
}

function shiftMonth(year, month, delta) {
  const d = new Date(Date.UTC(Number(year), Number(month) - 1 + Number(delta), 1));
  return { year: d.getUTCFullYear(), month: d.getUTCMonth() + 1 };
}

function resolveMonthBucketWindow(kind, count, anchorYear, anchorMonth) {
  const safeCount = Number(count);
  if (!Number.isFinite(safeCount) || safeCount < 1 || safeCount > 12) return null;
  if (kind === "first") {
    return {
      start: `${anchorYear}-01-01`,
      end: getMonthWindow(anchorYear, safeCount)?.end || null,
      label: `First ${safeCount} months ${anchorYear}`,
      grain: "month_bucket",
    };
  }
  if (kind === "last") {
    const endMonth = Number(anchorMonth);
    const startPoint = shiftMonth(anchorYear, endMonth, -(safeCount - 1));
    const start = getMonthWindow(startPoint.year, startPoint.month)?.start || null;
    const end = getMonthWindow(anchorYear, endMonth)?.end || null;
    return {
      start,
      end,
      label: `Last ${safeCount} months ending ${MONTH_SHORT_NAMES[endMonth - 1]} ${anchorYear}`,
      grain: "month_bucket",
    };
  }
  return null;
}

function getLastReportingWeekOfYear(year) {
  const d = new Date(Date.UTC(Number(year), 11, 31));
  return getReportingWeekForDate(d)?.week || 53;
}

function shiftWeek(year, week, delta) {
  const base = getReportingWeekWindow(year, week);
  if (!base) return null;
  const shifted = addDaysUtc(new Date(`${base.start}T00:00:00Z`), Number(delta) * 7);
  const shiftedWeek = getReportingWeekForDate(shifted);
  if (!shiftedWeek) return null;
  return shiftedWeek;
}

function resolveWeekBucketWindow(kind, count, anchorYear, anchorWeek) {
  const safeCount = Number(count);
  if (!Number.isFinite(safeCount) || safeCount < 1 || safeCount > 53) return null;

  if (kind === "first") {
    const startWeek = getReportingWeekWindow(anchorYear, 1);
    const endWeek = getReportingWeekWindow(anchorYear, safeCount);
    if (!startWeek || !endWeek) return null;
    return {
      start: startWeek.start,
      end: endWeek.end,
      label: `First ${safeCount} weeks ${anchorYear}`,
      grain: "week_bucket",
    };
  }

  if (kind === "last") {
    const endWeek = getReportingWeekWindow(anchorYear, anchorWeek);
    const startRef = shiftWeek(anchorYear, anchorWeek, -(safeCount - 1));
    const startWeek = startRef ? getReportingWeekWindow(startRef.year, startRef.week) : null;
    if (!startWeek || !endWeek) return null;
    return {
      start: startWeek.start,
      end: endWeek.end,
      label: `Last ${safeCount} weeks ending W${anchorWeek} ${anchorYear}`,
      grain: "week_bucket",
    };
  }

  return null;
}

export function parseTimeIntent(text) {
  const lower = String(text || "").toLowerCase();
  const currentYear = new Date().getUTCFullYear();

  const weekMentions = extractWeekMentions(lower, currentYear);
  if (weekMentions.length) {
    const last = weekMentions[weekMentions.length - 1];
    return { type: "week", week: last.week, year: last.year };
  }

  const quarterMentions = extractQuarterMentions(lower, currentYear);
  if (quarterMentions.length) {
    const last = quarterMentions[quarterMentions.length - 1];
    return { type: "quarter", quarter: last.quarter, year: last.year };
  }

  let lastMonthIntent = null;
  for (const [name, num] of Object.entries(MONTH_NAMES)) {
    const monthRe = new RegExp(`\\b${name}\\b(?:\\s+(\\d{4}))?`, "ig");
    let mm;
    while ((mm = monthRe.exec(lower)) !== null) {
      lastMonthIntent = { type: "month", month: num, year: mm[1] ? parseInt(mm[1], 10) : currentYear };
    }
  }
  if (lastMonthIntent) return lastMonthIntent;

  if (/\blast\s+week\b/.test(lower)) return { type: "last_week", year: currentYear };
  if (/\bthis\s+week\b/.test(lower)) return { type: "this_week", year: currentYear };
  if (/\blast\s+month\b/.test(lower)) return { type: "last_month", year: currentYear };
  if (/\bthis\s+month\b/.test(lower)) return { type: "this_month", year: currentYear };

  return null;
}

export async function resolveReportingDates(intent) {
  if (!intent) return null;
  if (intent.type === "week") {
    const weekWindow = getReportingWeekWindow(intent.year, intent.week);
    if (!weekWindow) return null;
    return { start: weekWindow.start, end: weekWindow.end };
  }
  if (intent.type === "last_week" || intent.type === "this_week") {
    const current = getReportingWeekForDate(new Date());
    if (!current) return null;
    let week = current.week;
    let year = current.year;
    if (intent.type === "last_week") {
      week -= 1;
      if (week < 1) {
        year -= 1;
        week = 53;
      }
    }
    let weekWindow = getReportingWeekWindow(year, week);
    if (!weekWindow && week === 53) weekWindow = getReportingWeekWindow(year, 52);
    if (!weekWindow) return null;
    return { start: weekWindow.start, end: weekWindow.end };
  }
  if (intent.type === "month") {
    const window = getMonthWindow(intent.year, intent.month);
    if (!window) return null;
    return { start: window.start, end: window.end };
  }
  if (intent.type === "quarter") {
    const window = getQuarterWindow(intent.year, intent.quarter);
    if (!window) return null;
    return { start: window.start, end: window.end };
  }
  if (intent.type === "last_month" || intent.type === "this_month") {
    const now = new Date();
    let month = now.getUTCMonth() + 1;
    let year = now.getUTCFullYear();
    if (intent.type === "last_month") {
      month -= 1;
      if (month < 1) {
        month = 12;
        year -= 1;
      }
    }
    const start = new Date(Date.UTC(year, month - 1, 1));
    const end = new Date(Date.UTC(year, month, 0));
    return { start: toIsoDate(start), end: toIsoDate(end) };
  }
  return null;
}

export function detectComparisonPlan(latestText, threadContextText) {
  const latest = String(latestText || "");
  if (!/\b(compare|comparison|vs|versus|against)\b/i.test(latest)) return null;
  const currentYear = new Date().getUTCFullYear();
  const context = String(threadContextText || "");

  const latestRanges = extractIsoDateRanges(latest);
  const contextRanges = extractIsoDateRanges(context);
  if (latestRanges.length >= 2) {
    return {
      grain: "custom_range",
      source: "prompt_inference",
      focus: { ...latestRanges[0], grain: "custom_range", label: `${latestRanges[0].start} to ${latestRanges[0].end}` },
      baseline: { ...latestRanges[1], grain: "custom_range", label: `${latestRanges[1].start} to ${latestRanges[1].end}` },
    };
  }
  if (latestRanges.length === 1 && contextRanges.length >= 1) {
    const other = contextRanges.find((r) => !(r.start === latestRanges[0].start && r.end === latestRanges[0].end));
    if (other) {
      return {
        grain: "custom_range",
        source: "prompt_inference",
        focus: { ...latestRanges[0], grain: "custom_range", label: `${latestRanges[0].start} to ${latestRanges[0].end}` },
        baseline: { ...other, grain: "custom_range", label: `${other.start} to ${other.end}` },
      };
    }
  }

  const monthBucketMatch = latest.match(/\b(first|last)\s+(\d{1,2})\s+months?\s+vs\s+(first|last)\s+(\d{1,2})\s+months?(?:\s+(\d{4}))?/i);
  if (monthBucketMatch) {
    const leftKind = String(monthBucketMatch[1]).toLowerCase();
    const leftCount = Number(monthBucketMatch[2]);
    const rightKind = String(monthBucketMatch[3]).toLowerCase();
    const rightCount = Number(monthBucketMatch[4]);
    const hintedYear = monthBucketMatch[5] ? Number(monthBucketMatch[5]) : currentYear;
    const anchorYear = Number.isFinite(hintedYear) ? hintedYear : currentYear;
    const anchorMonth = anchorYear === currentYear ? (new Date().getUTCMonth() + 1) : 12;
    const leftWindow = resolveMonthBucketWindow(leftKind, leftCount, anchorYear, anchorMonth);
    const rightWindow = resolveMonthBucketWindow(rightKind, rightCount, anchorYear, anchorMonth);
    if (leftWindow?.start && leftWindow?.end && rightWindow?.start && rightWindow?.end) {
      return {
        grain: "month_bucket",
        source: "reporting_calendar",
        focus: leftWindow,
        baseline: rightWindow,
      };
    }
  }

  const weekBucketMatch = latest.match(/\b(first|last)\s+(\d{1,2})\s+weeks?\s+vs\s+(first|last)\s+(\d{1,2})\s+weeks?(?:\s+(\d{4}))?/i);
  if (weekBucketMatch) {
    const leftKind = String(weekBucketMatch[1]).toLowerCase();
    const leftCount = Number(weekBucketMatch[2]);
    const rightKind = String(weekBucketMatch[3]).toLowerCase();
    const rightCount = Number(weekBucketMatch[4]);
    const hintedYear = weekBucketMatch[5] ? Number(weekBucketMatch[5]) : currentYear;
    const anchorYear = Number.isFinite(hintedYear) ? hintedYear : currentYear;
    const anchorWeek = anchorYear === currentYear
      ? (getReportingWeekForDate(new Date())?.week || 53)
      : getLastReportingWeekOfYear(anchorYear);

    const leftWindow = resolveWeekBucketWindow(leftKind, leftCount, anchorYear, anchorWeek);
    const rightWindow = resolveWeekBucketWindow(rightKind, rightCount, anchorYear, anchorWeek);
    if (leftWindow?.start && leftWindow?.end && rightWindow?.start && rightWindow?.end) {
      return {
        grain: "week_bucket",
        source: "reporting_calendar",
        focus: leftWindow,
        baseline: rightWindow,
      };
    }
  }

  const latestWeeks = extractWeekMentions(latest, currentYear);
  const contextWeeks = extractWeekMentions(context, currentYear);

  let focus = null;
  let baseline = null;
  if (latestWeeks.length >= 2) {
    focus = latestWeeks[0];
    baseline = latestWeeks[1];
  } else if (latestWeeks.length === 1) {
    baseline = latestWeeks[0];
    focus = [...contextWeeks].reverse().find((w) => !(w.week === baseline.week && w.year === baseline.year)) || null;
    if (!focus) {
      const nextWeek = baseline.week + 1;
      focus = nextWeek <= 53 ? { week: nextWeek, year: baseline.year } : { week: 1, year: baseline.year + 1 };
    }
  } else if (contextWeeks.length >= 2) {
    focus = contextWeeks[contextWeeks.length - 1];
    baseline = contextWeeks[contextWeeks.length - 2];
  }

  if (focus && baseline) {
    const focusWindow = getReportingWeekWindow(focus.year, focus.week);
    const baselineWindow = getReportingWeekWindow(baseline.year, baseline.week);
    if (!focusWindow || !baselineWindow) return null;
    return {
      grain: "week",
      source: "reporting_calendar",
      focus: { ...focusWindow, grain: "week", label: `W${focusWindow.week} ${focusWindow.year}` },
      baseline: { ...baselineWindow, grain: "week", label: `W${baselineWindow.week} ${baselineWindow.year}` },
    };
  }

  const latestQuarters = extractQuarterMentions(latest, currentYear);
  const contextQuarters = extractQuarterMentions(context, currentYear);
  let focusQuarter = null;
  let baselineQuarter = null;
  if (latestQuarters.length >= 2) {
    focusQuarter = latestQuarters[0];
    baselineQuarter = latestQuarters[1];
  } else if (latestQuarters.length === 1) {
    baselineQuarter = latestQuarters[0];
    focusQuarter = [...contextQuarters]
      .reverse()
      .find((q) => !(q.quarter === baselineQuarter.quarter && q.year === baselineQuarter.year)) || null;
    if (!focusQuarter) {
      let quarter = baselineQuarter.quarter - 1;
      let year = baselineQuarter.year;
      if (quarter < 1) {
        quarter = 4;
        year -= 1;
      }
      focusQuarter = { quarter, year };
    }
  } else if (contextQuarters.length >= 2) {
    focusQuarter = contextQuarters[contextQuarters.length - 1];
    baselineQuarter = contextQuarters[contextQuarters.length - 2];
  }
  if (focusQuarter && baselineQuarter) {
    const focusQuarterWindow = getQuarterWindow(focusQuarter.year, focusQuarter.quarter);
    const baselineQuarterWindow = getQuarterWindow(baselineQuarter.year, baselineQuarter.quarter);
    if (focusQuarterWindow && baselineQuarterWindow) {
      return {
        grain: "quarter",
        source: "reporting_calendar",
        focus: { ...focusQuarterWindow, grain: "quarter", label: `Q${focusQuarterWindow.quarter} ${focusQuarterWindow.year}` },
        baseline: { ...baselineQuarterWindow, grain: "quarter", label: `Q${baselineQuarterWindow.quarter} ${baselineQuarterWindow.year}` },
      };
    }
  }

  const latestMonths = extractMonthMentions(latest, currentYear);
  const contextMonths = extractMonthMentions(context, currentYear);

  let focusMonth = null;
  let baselineMonth = null;
  if (latestMonths.length >= 2) {
    focusMonth = latestMonths[0];
    baselineMonth = latestMonths[1];
  } else if (latestMonths.length === 1) {
    baselineMonth = latestMonths[0];
    focusMonth = [...contextMonths]
      .reverse()
      .find((m) => !(m.month === baselineMonth.month && m.year === baselineMonth.year)) || null;
    if (!focusMonth) {
      let month = baselineMonth.month - 1;
      let year = baselineMonth.year;
      if (month < 1) {
        month = 12;
        year -= 1;
      }
      focusMonth = { month, year };
    }
  } else if (contextMonths.length >= 2) {
    focusMonth = contextMonths[contextMonths.length - 1];
    baselineMonth = contextMonths[contextMonths.length - 2];
  }

  if (!focusMonth || !baselineMonth) return null;
  const focusMonthWindow = getMonthWindow(focusMonth.year, focusMonth.month);
  const baselineMonthWindow = getMonthWindow(baselineMonth.year, baselineMonth.month);
  if (!focusMonthWindow || !baselineMonthWindow) return null;

  const toLabel = (w) => `${MONTH_SHORT_NAMES[w.month - 1]} ${w.year}`;
  return {
    grain: "month",
    source: "prompt_inference",
    focus: { ...focusMonthWindow, grain: "month", label: toLabel(focusMonthWindow) },
    baseline: { ...baselineMonthWindow, grain: "month", label: toLabel(baselineMonthWindow) },
  };
}

export function detectWeekComparisonPlan(latestText, threadContextText) {
  const plan = detectComparisonPlan(latestText, threadContextText);
  if (!plan || plan.grain !== "week") return null;
  return { focus: plan.focus, baseline: plan.baseline };
}
