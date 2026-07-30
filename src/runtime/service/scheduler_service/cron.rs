//! Minimal self-contained cron evaluator (no external crate for parsing) plus
//! PURE missed-run accounting, with optional per-zone DST-aware evaluation.
//!
//! Supports standard 5-field cron (minute hour day-of-month month day-of-week)
//! with `*`, `*/step`, `a-b`, `a-b/step`, and comma lists, plus the common
//! `@hourly`/`@daily`/`@weekly`/`@monthly`/`@yearly` macros. Day-of-week is 0-6
//! (0 = Sunday). When BOTH day-of-month and day-of-week are restricted, a match on
//! EITHER fires (Vixie-cron semantics). Returns the first matching minute strictly
//! after `after`, searching up to [`NEXT_FIRE_SEARCH_DAYS`]; `None` if the
//! expression is invalid or unsatisfiable within that horizon.
//!
//! TIMEZONE: evaluation defaults to UTC. A job may carry an IANA timezone name in
//! its opaque `payload` JSON under the `"timezone"` key (e.g. `"America/New_York"`);
//! a process-wide default is available via `UDB_SCHEDULER_TZ`. When a zone is in
//! effect the cron fields are interpreted as LOCAL wall-clock time in that zone and
//! the resulting local time is converted back to a UTC instant for storage/firing,
//! so a wall-clock schedule tracks the zone's DST rule instead of drifting twice a
//! year. DST discontinuities are handled explicitly: a nonexistent local time
//! (spring-forward gap) resolves to the post-transition instant, and an ambiguous
//! local time (fall-back overlap) resolves to the EARLIER of the two instants. With
//! no zone the behavior is byte-for-byte the historical UTC evaluation.

use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, MappedLocalTime, NaiveDateTime, TimeZone,
    Timelike, Utc,
};
use chrono_tz::Tz;
use tonic::Status;

use super::config::scheduler_default_tz;
use super::errors::scheduler_invalid_timezone;

/// Horizon for the next-fire search. Must comfortably exceed the longest gap
/// between occurrences of any valid cron — the widest is the quadrennial
/// leap-day expression `0 0 29 2 *`, whose occurrences are ~4 years apart. A
/// 366-day bound wrongly returned `None` for it (rejecting a valid job at create
/// or dead-lettering it at fire time); ~5 years covers a full leap cycle with
/// margin so a sparse-but-valid cron always resolves.
pub(crate) const NEXT_FIRE_SEARCH_DAYS: i64 = 1830;

/// Safety cap on the missed-run counting loop: bounds how many elapsed cron
/// occurrences one fire will enumerate, so a job that slept for months cannot
/// spin the tick. A count equal to the cap means "at least this many".
pub(crate) const MAX_MISSED_RUNS_COUNTED: i64 = 1000;

/// Upper bound on how far forward we probe to escape a spring-forward gap when
/// resolving a nonexistent local time. Real DST gaps are ~1 hour (a few historical
/// zones jumped up to ~2h); 4 hours is a safe, tightly-bounded margin.
const MAX_DST_GAP_PROBE_MINUTES: i64 = 240;

/// PURE missed-run accounting: count the cron occurrences that elapsed strictly
/// after the stored due time (`due_at`, the occurrence being fired now) up to
/// and including `now` — i.e. the windows this single fire collapses. An
/// on-time fire (next occurrence still in the future) yields 0. The loop is
/// bounded by [`MAX_MISSED_RUNS_COUNTED`] so an ancient due time cannot spin;
/// hitting the cap means "at least this many missed". Occurrences are counted in
/// `tz` when a zone is in effect (so DST windows are not miscounted), UTC otherwise.
pub(crate) fn missed_cron_occurrences(
    cron: &str,
    due_at: DateTime<Utc>,
    now: DateTime<Utc>,
    tz: Option<Tz>,
) -> i64 {
    let mut missed = 0i64;
    let mut cursor = due_at;
    while missed < MAX_MISSED_RUNS_COUNTED {
        match next_cron_after_tz(cron, cursor, tz) {
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

/// A parsed 5-field cron expression: the allowed value set per field plus which
/// day fields were restricted (`*` vs an explicit set), so the day match can apply
/// Vixie-cron OR semantics. Parsing once lets the same spec drive both the UTC and
/// the local (naive) minute walks.
struct CronSpec {
    minutes: Vec<u8>,
    hours: Vec<u8>,
    doms: Vec<u8>,
    months: Vec<u8>,
    dows: Vec<u8>,
    dom_restricted: bool,
    dow_restricted: bool,
}

fn parse_cron_spec(expr: &str) -> Option<CronSpec> {
    let normalized = expand_cron_macro(expr);
    let fields: Vec<&str> = normalized.split_whitespace().collect();
    if fields.len() != 5 {
        return None;
    }
    Some(CronSpec {
        minutes: parse_cron_field(fields[0], 0, 59)?,
        hours: parse_cron_field(fields[1], 0, 23)?,
        doms: parse_cron_field(fields[2], 1, 31)?,
        months: parse_cron_field(fields[3], 1, 12)?,
        dows: parse_cron_field(fields[4], 0, 6)?,
        dom_restricted: fields[2].trim() != "*",
        dow_restricted: fields[4].trim() != "*",
    })
}

/// Does the wall-clock instant `t` match the cron spec? Generic over any
/// calendar-aware value (`DateTime<Utc>` for the UTC walk, `NaiveDateTime` for the
/// local-wall-clock walk) so the match rule is single-sourced.
fn cron_matches<T: Datelike + Timelike>(spec: &CronSpec, t: &T) -> bool {
    let minute = t.minute() as u8;
    let hour = t.hour() as u8;
    let dom = t.day() as u8;
    let month = t.month() as u8;
    let dow = t.weekday().num_days_from_sunday() as u8; // 0 = Sunday
    let day_ok = match (spec.dom_restricted, spec.dow_restricted) {
        (false, false) => true,
        (true, false) => spec.doms.contains(&dom),
        (false, true) => spec.dows.contains(&dow),
        (true, true) => spec.doms.contains(&dom) || spec.dows.contains(&dow),
    };
    spec.minutes.contains(&minute)
        && spec.hours.contains(&hour)
        && spec.months.contains(&month)
        && day_ok
}

/// UTC cron evaluation: the first matching UTC minute strictly after `after`,
/// within [`NEXT_FIRE_SEARCH_DAYS`]. This is the historical, timezone-free path;
/// its behavior is unchanged.
pub(crate) fn next_cron_after(expr: &str, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let spec = parse_cron_spec(expr)?;
    let mut t = (after + ChronoDuration::minutes(1))
        .with_second(0)?
        .with_nanosecond(0)?;
    let horizon = after + ChronoDuration::days(NEXT_FIRE_SEARCH_DAYS);
    while t <= horizon {
        if cron_matches(&spec, &t) {
            return Some(t);
        }
        t = t + ChronoDuration::minutes(1);
    }
    None
}

/// DST-aware cron evaluation in `tz`: interpret the cron fields as LOCAL
/// wall-clock time in the zone and return the UTC instant of the first matching
/// local minute strictly after `after`. The minute walk is over LOCAL naive time
/// (so the schedule is a wall-clock schedule that tracks DST), and each match is
/// mapped back to UTC via [`resolve_local_wall_clock`], which handles spring-forward
/// gaps and fall-back overlaps. Forward progress is enforced in UTC (`> after`) so
/// a fall-back overlap can never yield a non-advancing fire.
pub(crate) fn next_cron_after_in_zone(
    expr: &str,
    after: DateTime<Utc>,
    tz: Tz,
) -> Option<DateTime<Utc>> {
    let spec = parse_cron_spec(expr)?;
    let start = after.with_timezone(&tz).naive_local();
    let mut t = (start + ChronoDuration::minutes(1))
        .with_second(0)?
        .with_nanosecond(0)?;
    let horizon = start + ChronoDuration::days(NEXT_FIRE_SEARCH_DAYS);
    while t <= horizon {
        if cron_matches(&spec, &t)
            && let Some(utc) = resolve_local_wall_clock(tz, t)
            && utc > after
        {
            return Some(utc);
        }
        t = t + ChronoDuration::minutes(1);
    }
    None
}

/// Next fire honoring an optional zone: local DST-aware evaluation when a zone is
/// given, the historical UTC path otherwise. The single entry point callers use so
/// the UTC-vs-zone choice lives in one place.
pub(crate) fn next_cron_after_tz(
    expr: &str,
    after: DateTime<Utc>,
    tz: Option<Tz>,
) -> Option<DateTime<Utc>> {
    match tz {
        Some(zone) => next_cron_after_in_zone(expr, after, zone),
        None => next_cron_after(expr, after),
    }
}

/// Resolve a local wall-clock `NaiveDateTime` in `tz` to a UTC instant, handling
/// the two DST discontinuities per the scheduler contract:
/// - unambiguous local time → that instant;
/// - fall-back overlap (ambiguous) → the EARLIER of the two candidate instants;
/// - spring-forward gap (nonexistent) → the POST-transition instant, i.e. the
///   instant the wall clock jumps to, found by advancing to the first local minute
///   that exists after the gap (bounded by [`MAX_DST_GAP_PROBE_MINUTES`]).
fn resolve_local_wall_clock(tz: Tz, local: NaiveDateTime) -> Option<DateTime<Utc>> {
    match tz.from_local_datetime(&local) {
        MappedLocalTime::Single(dt) => Some(dt.with_timezone(&Utc)),
        // Fall-back overlap: the earlier instant is the first field.
        MappedLocalTime::Ambiguous(earliest, _latest) => Some(earliest.with_timezone(&Utc)),
        MappedLocalTime::None => {
            // Spring-forward gap: the requested wall-clock minute never occurs.
            // Walk forward to the first existing local minute (the clock's
            // post-jump reading) and take that instant.
            let mut probe = local;
            for _ in 0..MAX_DST_GAP_PROBE_MINUTES {
                probe += ChronoDuration::minutes(1);
                match tz.from_local_datetime(&probe) {
                    MappedLocalTime::Single(dt) => return Some(dt.with_timezone(&Utc)),
                    MappedLocalTime::Ambiguous(dt, _) => return Some(dt.with_timezone(&Utc)),
                    MappedLocalTime::None => continue,
                }
            }
            None
        }
    }
}

/// Extract an explicit per-job timezone from the opaque `payload` JSON. Reads the
/// optional `"timezone"` IANA name (parsed case-insensitively). Returns
/// `Ok(Some(tz))` for a valid name, `Ok(None)` when absent/empty/not-an-object (the
/// caller then applies the process default or UTC), and `Err(status)` when a
/// NON-EMPTY name is present but does not resolve — so an invalid explicit timezone
/// fails closed at create rather than silently drifting. A payload that is not valid
/// JSON carries no timezone (`Ok(None)`); it is rejected downstream by the JSONB bind.
pub(crate) fn timezone_from_payload(payload: &str) -> Result<Option<Tz>, Status> {
    let trimmed = payload.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let value: serde_json::Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let name = match value.get("timezone").and_then(|v| v.as_str()) {
        Some(s) => s.trim(),
        None => return Ok(None),
    };
    if name.is_empty() {
        return Ok(None);
    }
    match Tz::from_str_insensitive(name) {
        Ok(tz) => Ok(Some(tz)),
        Err(_) => Err(scheduler_invalid_timezone(name)),
    }
}

/// Effective zone for a job: its explicit `payload` timezone if present (rejecting
/// an invalid explicit name), otherwise the process default (`UDB_SCHEDULER_TZ`),
/// otherwise `None` (UTC). The single resolver both the create validation and the
/// tick advance use so they agree on which zone a job fires in.
pub(crate) fn effective_tz(payload: &str) -> Result<Option<Tz>, Status> {
    match timezone_from_payload(payload)? {
        Some(tz) => Ok(Some(tz)),
        None => Ok(scheduler_default_tz()),
    }
}
