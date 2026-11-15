//! Audit volume of delegated ALLOW decisions.
//!
//! A backend that gates every request on a delegated `CheckAccess` asks the
//! same question ("may user U do A on R?") many times a second. Writing every
//! identical allow costs one audit row, one outbox event, one CDC journal row
//! and one auth-audit-log row per request, and those tables then hold almost
//! nothing but repeats.
//!
//! Denies are never affected: every deny is written. For allows, the first
//! allow of an identical decision (same tenant, project, caller, subject,
//! resource, action, matched rule and policy/relationship versions) in a window
//! is written; repeats inside the window are counted, and the next written row
//! for that decision carries the count as `suppressed_identical_allows` in its
//! `decision_input`. A changed policy, rule or subject is a different decision
//! and is written at once.
//!
//! `UDB_AUTHZ_DELEGATED_ALLOW_AUDIT_WINDOW` (or `udb serve
//! --delegated-allow-audit-window`) sets the window (default `1h`;
//! `0` writes every allow). The state is per broker process and bounded: when
//! it is full the allow is written (the gate fails toward auditing).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::runtime::authz::{Decision, Principal, ResourceRef};

use super::delegation::DelegatedEvaluation;

/// Environment variable naming the window.
pub(crate) const ALLOW_AUDIT_WINDOW_ENV: &str = "UDB_AUTHZ_DELEGATED_ALLOW_AUDIT_WINDOW";
/// Environment variable naming how many distinct decisions are tracked.
pub(crate) const ALLOW_AUDIT_MAX_TRACKED_ENV: &str = "UDB_AUTHZ_DELEGATED_ALLOW_AUDIT_MAX_TRACKED";
const DEFAULT_WINDOW: Duration = Duration::from_secs(3600);
const DEFAULT_MAX_TRACKED_DECISIONS: usize = 100_000;

/// The window in force (`UDB_AUTHZ_DELEGATED_ALLOW_AUDIT_WINDOW`, default
/// `1h`). A value that is not a duration audits every allow and is logged.
pub(crate) fn configured_window() -> Duration {
    match std::env::var(ALLOW_AUDIT_WINDOW_ENV) {
        Ok(raw) if !raw.trim().is_empty() => match parse_window(&raw) {
            Some(window) => window,
            None => {
                tracing::error!(
                    value = %raw,
                    "{ALLOW_AUDIT_WINDOW_ENV} is not a duration (e.g. 60s, 5m, 1h, 0); \
                     every delegated allow is audited"
                );
                Duration::ZERO
            }
        },
        _ => DEFAULT_WINDOW,
    }
}

/// Distinct decisions tracked at once per process
/// (`UDB_AUTHZ_DELEGATED_ALLOW_AUDIT_MAX_TRACKED`, default 100000).
pub(crate) fn configured_max_tracked() -> usize {
    std::env::var(ALLOW_AUDIT_MAX_TRACKED_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|max| *max > 0)
        .unwrap_or(DEFAULT_MAX_TRACKED_DECISIONS)
}

/// What to do with one delegated allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AllowAudit {
    /// Write it; `suppressed_before` identical allows were not written since
    /// the last written row for this decision.
    Write { suppressed_before: u64 },
    /// An identical allow was written inside the window: count it only.
    Suppress,
}

#[derive(Debug)]
struct Slot {
    opened: Instant,
    suppressed: u64,
}

#[derive(Debug)]
pub(crate) struct AllowAuditWindow {
    window: Duration,
    capacity: usize,
    slots: Mutex<HashMap<String, Slot>>,
}

impl AllowAuditWindow {
    pub(crate) fn new(window: Duration, capacity: usize) -> Self {
        Self {
            window,
            capacity: capacity.max(1),
            slots: Mutex::new(HashMap::new()),
        }
    }

    /// The process-wide gate, configured from the environment on first use.
    pub(crate) fn global() -> &'static AllowAuditWindow {
        static GATE: OnceLock<AllowAuditWindow> = OnceLock::new();
        GATE.get_or_init(|| AllowAuditWindow::new(configured_window(), configured_max_tracked()))
    }

    pub(crate) fn admit(&self, key: &str, now: Instant) -> AllowAudit {
        if self.window.is_zero() {
            return AllowAudit::Write {
                suppressed_before: 0,
            };
        }
        let Ok(mut slots) = self.slots.lock() else {
            return AllowAudit::Write {
                suppressed_before: 0,
            };
        };
        if let Some(slot) = slots.get_mut(key) {
            if now.saturating_duration_since(slot.opened) < self.window {
                slot.suppressed = slot.suppressed.saturating_add(1);
                return AllowAudit::Suppress;
            }
            let suppressed_before = slot.suppressed;
            *slot = Slot {
                opened: now,
                suppressed: 0,
            };
            return AllowAudit::Write { suppressed_before };
        }
        if slots.len() >= self.capacity {
            // Drop decisions whose window closed with nothing left to report.
            let window = self.window;
            slots.retain(|_, slot| {
                now.saturating_duration_since(slot.opened) < window || slot.suppressed > 0
            });
            if slots.len() >= self.capacity {
                return AllowAudit::Write {
                    suppressed_before: 0,
                };
            }
        }
        slots.insert(
            key.to_string(),
            Slot {
                opened: now,
                suppressed: 0,
            },
        );
        AllowAudit::Write {
            suppressed_before: 0,
        }
    }
}

/// The identity of a delegated decision for the window.
pub(crate) fn decision_key(
    delegation: &DelegatedEvaluation,
    subject: &Principal,
    resource: &ResourceRef,
    action: &str,
    decision: &Decision,
) -> String {
    [
        subject.tenant_id.as_str(),
        subject.project_id.as_str(),
        delegation.evaluated_by.as_str(),
        delegation.evaluated_by_service_identity.as_str(),
        subject.subject.as_str(),
        resource.resource_name.as_str(),
        action,
        &decision.matched_policy_ids.join(","),
        decision.policy_version.as_str(),
        decision.relationship_version.as_str(),
    ]
    .join("\u{1f}")
}

/// Parse `60s`, `5m`, `1h`, `500ms` or a bare number of seconds.
pub(crate) fn parse_window(raw: &str) -> Option<Duration> {
    let raw = raw.trim().to_ascii_lowercase();
    let (number, unit) = match raw.find(|c: char| !c.is_ascii_digit()) {
        Some(at) => raw.split_at(at),
        None => (raw.as_str(), "s"),
    };
    let value: u64 = number.parse().ok()?;
    match unit.trim() {
        "ms" => Some(Duration::from_millis(value)),
        "s" => Some(Duration::from_secs(value)),
        "m" => value.checked_mul(60).map(Duration::from_secs),
        "h" => value.checked_mul(3600).map(Duration::from_secs),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeats_inside_the_window_are_counted_and_reported_on_the_next_write() {
        let gate = AllowAuditWindow::new(Duration::from_secs(60), 10);
        let start = Instant::now();
        assert_eq!(
            gate.admit("a", start),
            AllowAudit::Write {
                suppressed_before: 0
            }
        );
        for second in 1..=5 {
            assert_eq!(
                gate.admit("a", start + Duration::from_secs(second)),
                AllowAudit::Suppress
            );
        }
        // A different decision is written at once.
        assert_eq!(
            gate.admit("b", start + Duration::from_secs(2)),
            AllowAudit::Write {
                suppressed_before: 0
            }
        );
        assert_eq!(
            gate.admit("a", start + Duration::from_secs(61)),
            AllowAudit::Write {
                suppressed_before: 5
            }
        );
        assert_eq!(
            gate.admit("a", start + Duration::from_secs(62)),
            AllowAudit::Suppress
        );
    }

    #[test]
    fn a_zero_window_writes_every_allow() {
        let gate = AllowAuditWindow::new(Duration::ZERO, 10);
        let now = Instant::now();
        for _ in 0..3 {
            assert_eq!(
                gate.admit("a", now),
                AllowAudit::Write {
                    suppressed_before: 0
                }
            );
        }
    }

    #[test]
    fn a_full_gate_writes_rather_than_drops() {
        let gate = AllowAuditWindow::new(Duration::from_secs(60), 2);
        let now = Instant::now();
        gate.admit("a", now);
        gate.admit("b", now);
        for _ in 0..3 {
            assert_eq!(
                gate.admit("c", now),
                AllowAudit::Write {
                    suppressed_before: 0
                }
            );
        }
        // Once a tracked window closes with nothing to report, room is made.
        let later = now + Duration::from_secs(61);
        assert_eq!(
            gate.admit("c", later),
            AllowAudit::Write {
                suppressed_before: 0
            }
        );
        assert_eq!(gate.admit("c", later), AllowAudit::Suppress);
    }

    #[test]
    fn window_values_parse() {
        assert_eq!(parse_window("60s"), Some(Duration::from_secs(60)));
        assert_eq!(parse_window("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_window("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_window("250ms"), Some(Duration::from_millis(250)));
        assert_eq!(parse_window("0"), Some(Duration::ZERO));
        assert_eq!(parse_window("30"), Some(Duration::from_secs(30)));
        assert_eq!(parse_window("soon"), None);
        assert_eq!(parse_window("5d"), None);
    }
}
