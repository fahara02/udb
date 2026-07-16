//! Minimal self-contained cron evaluator (no external crate) plus PURE missed-run
//! accounting.
//!
//! Supports standard 5-field cron (minute hour day-of-month month day-of-week)
//! with `*`, `*/step`, `a-b`, `a-b/step`, and comma lists, plus the common
//! `@hourly`/`@daily`/`@weekly`/`@monthly`/`@yearly` macros. Day-of-week is 0-6
//! (0 = Sunday). When BOTH day-of-month and day-of-week are restricted, a match on
//! EITHER fires (Vixie-cron semantics). Returns the first matching minute strictly
//! after `after`, searching up to ~366 days; `None` if the expression is invalid
//! or unsatisfiable within that horizon.

use chrono::{DateTime, Datelike, Duration as ChronoDuration, Timelike, Utc};

/// Safety cap on the missed-run counting loop: bounds how many elapsed cron
/// occurrences one fire will enumerate, so a job that slept for months cannot
/// spin the tick. A count equal to the cap means "at least this many".
pub(crate) const MAX_MISSED_RUNS_COUNTED: i64 = 1000;

/// PURE missed-run accounting: count the cron occurrences that elapsed strictly
/// after the stored due time (`due_at`, the occurrence being fired now) up to
/// and including `now` — i.e. the windows this single fire collapses. An
/// on-time fire (next occurrence still in the future) yields 0. The loop is
/// bounded by [`MAX_MISSED_RUNS_COUNTED`] so an ancient due time cannot spin;
/// hitting the cap means "at least this many missed".
pub(crate) fn missed_cron_occurrences(
    cron: &str,
    due_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> i64 {
    let mut missed = 0i64;
    let mut cursor = due_at;
    while missed < MAX_MISSED_RUNS_COUNTED {
        match next_cron_after(cron, cursor) {
            Some(next) if next <= now => {
                missed += 1;
                cursor = next;
            }
            _ => break,
        }
    }
    missed
}

fn expand_cron_macro(expr: &str) -> String {
    match expr.trim() {
        "@hourly" => "0 * * * *".to_string(),
        "@daily" | "@midnight" => "0 0 * * *".to_string(),
        "@weekly" => "0 0 * * 0".to_string(),
        "@monthly" => "0 0 1 * *".to_string(),
        "@yearly" | "@annually" => "0 0 1 1 *".to_string(),
        other => other.to_string(),
    }
}

fn parse_cron_field(field: &str, min: u8, max: u8) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    for part in field.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return None;
        }
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => (r, s.trim().parse::<u8>().ok().filter(|v| *v > 0)?),
            None => (part, 1u8),
        };
        let (lo, hi) = if range == "*" {
            (min, max)
        } else if let Some((a, b)) = range.split_once('-') {
            (a.trim().parse::<u8>().ok()?, b.trim().parse::<u8>().ok()?)
        } else {
            let v = range.trim().parse::<u8>().ok()?;
            (v, v)
        };
        if lo < min || hi > max || lo > hi {
            return None;
        }
        let mut v = lo;
        while v <= hi {
            out.push(v);
            v = v.saturating_add(step);
            if v == 0 {
                break;
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    if out.is_empty() { None } else { Some(out) }
}

pub(crate) fn next_cron_after(expr: &str, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let normalized = expand_cron_macro(expr);
    let fields: Vec<&str> = normalized.split_whitespace().collect();
    if fields.len() != 5 {
        return None;
    }
    let minutes = parse_cron_field(fields[0], 0, 59)?;
    let hours = parse_cron_field(fields[1], 0, 23)?;
    let doms = parse_cron_field(fields[2], 1, 31)?;
    let months = parse_cron_field(fields[3], 1, 12)?;
    let dows = parse_cron_field(fields[4], 0, 6)?;
    let dom_restricted = fields[2].trim() != "*";
    let dow_restricted = fields[4].trim() != "*";

    let mut t = (after + ChronoDuration::minutes(1))
        .with_second(0)?
        .with_nanosecond(0)?;
    let horizon = after + ChronoDuration::days(366);
    while t <= horizon {
        let minute = t.minute() as u8;
        let hour = t.hour() as u8;
        let dom = t.day() as u8;
        let month = t.month() as u8;
        let dow = t.weekday().num_days_from_sunday() as u8; // 0 = Sunday
        let day_ok = match (dom_restricted, dow_restricted) {
            (false, false) => true,
            (true, false) => doms.contains(&dom),
            (false, true) => dows.contains(&dow),
            (true, true) => doms.contains(&dom) || dows.contains(&dow),
        };
        if minutes.contains(&minute) && hours.contains(&hour) && months.contains(&month) && day_ok {
            return Some(t);
        }
        t = t + ChronoDuration::minutes(1);
    }
    None
}
