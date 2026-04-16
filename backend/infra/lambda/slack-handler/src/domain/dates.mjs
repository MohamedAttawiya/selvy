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

export function parseTimeIntent(text) {
  const lower = String(text || "").toLowerCase();
  const currentYear = new Date().getUTCFullYear();

  const weekMentions = extractWeekMentions(lower, currentYear);
  if (weekMentions.length) {
    const last = weekMentions[weekMentions.length - 1];
    return { type: "week", week: last.week, year: last.year };
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
    const year = Number(intent.year);
    const month = Number(intent.month);
    if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) return null;
    const start = new Date(Date.UTC(year, month - 1, 1));
    const end = new Date(Date.UTC(year, month, 0));
    return { start: toIsoDate(start), end: toIsoDate(end) };
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

export function detectWeekComparisonPlan(latestText, threadContextText) {
  const latest = String(latestText || "");
  if (!/\b(compare|comparison|vs|versus|against)\b/i.test(latest)) return null;
  const currentYear = new Date().getUTCFullYear();
  const latestWeeks = extractWeekMentions(latest, currentYear);
  const contextWeeks = extractWeekMentions(String(threadContextText || ""), currentYear);

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

  if (!focus || !baseline) return null;
  const focusWindow = getReportingWeekWindow(focus.year, focus.week);
  const baselineWindow = getReportingWeekWindow(baseline.year, baseline.week);
  if (!focusWindow || !baselineWindow) return null;
  return {
    focus: { ...focusWindow, label: `W${focusWindow.week} ${focusWindow.year}` },
    baseline: { ...baselineWindow, label: `W${baselineWindow.week} ${baselineWindow.year}` },
  };
}

