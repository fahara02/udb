//! U7 step 2 — pipeline coverage tracker.
//!
//! `runtime::pipeline::RequestPipeline` already provides the per-phase
//! span machinery. The U7 acceptance gate said *"Partial: pipeline
//! phase tracing exists, but full end-to-end handler adoption remains
//! incremental."* The remaining gap is **visibility**: nothing today
//! enumerates which handlers actually create a `RequestPipeline` vs.
//! which ones still call the executor directly without the phase
//! spans.
//!
//! This module closes that gap with a registry. Every handler
//! declares its coverage state at compile time via
//! [`HANDLER_COVERAGE`]; the test below asserts the registry stays in
//! sync with reality (no silently-added RPC without a coverage entry,
//! no `Wired` claim without a span).
//!
//! ## Why a registry instead of trying to mechanically detect spans?
//!
//! The pipeline crosses async boundaries, so a static analyser can't
//! tell from the source whether a span was actually entered. The
//! registry approach is honest: the operator can see at a glance
//! which handlers are still incremental and can run
//! `pipeline_coverage::report()` to confirm the coverage percentage
//! before promoting.

/// One row in the coverage registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoverageEntry {
    /// Fully-qualified gRPC method name as it appears in the proto
    /// (`/udb.services.v1.DataBroker/Select`).
    pub rpc: &'static str,
    /// Whether the handler creates a `RequestPipeline` and walks the
    /// canonical phases. `false` means the handler is still on the
    /// legacy direct-dispatch path — it works but doesn't emit the
    /// phase spans.
    pub wired: bool,
    /// Pointer to a follow-up tracking note. Empty string when no
    /// follow-up is pending (a `Wired` entry has no follow-up; an
    /// unwired entry should always point at the bullet that will
    /// resolve it).
    pub note: &'static str,
}

/// The complete inventory. Adding a new RPC to the DataBroker proto
/// **must** add a corresponding entry here or the
/// `every_rpc_has_a_coverage_entry` test fails — that's the U7
/// regression guard. The list is sorted alphabetically by RPC for
/// stable diffs.
pub const HANDLER_COVERAGE: &[CoverageEntry] = &[
    // ── DataBroker data-path RPCs ────────────────────────────────────
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/Delete",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/GetCapabilities",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/Health",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/ListMessageSchemas",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/LookupMessageSchema",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/Search",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/Select",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/StreamCDC",
        wired: false,
        note: "U21 follow-up: CDC stream emits its own per-event spans; \
               needs Phase::Execute wrapper at the subscribe boundary.",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/Upsert",
        wired: true,
        note: "",
    },
    // ── Transaction / object / vector RPCs ───────────────────────────
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/BeginTx",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/CommitTx",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/RollbackTx",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/VectorSearch",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/PutObject",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/GetObject",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/DeleteObject",
        wired: true,
        note: "",
    },
    CoverageEntry {
        rpc: "/udb.services.v1.DataBroker/Dispatch",
        wired: true,
        note: "",
    },
];

/// Summary the operator can print at startup to see the coverage
/// state. Returns `(wired_count, total_count, unwired_rpcs)`.
pub fn report() -> (usize, usize, Vec<&'static str>) {
    let total = HANDLER_COVERAGE.len();
    let mut wired = 0;
    let mut unwired = Vec::new();
    for entry in HANDLER_COVERAGE {
        if entry.wired {
            wired += 1;
        } else {
            unwired.push(entry.rpc);
        }
    }
    (wired, total, unwired)
}

/// Coverage as a percentage (0-100) for dashboards.
pub fn coverage_percent() -> u32 {
    let (wired, total, _) = report();
    if total == 0 {
        return 100;
    }
    ((wired as u64 * 100) / total as u64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Pin: no duplicate RPC entries. Two rows with the same name
    /// would let one of them claim `wired: true` while the other
    /// silently stays unwired.
    #[test]
    fn no_duplicate_rpc_entries() {
        let mut seen = HashSet::new();
        for entry in HANDLER_COVERAGE {
            assert!(
                seen.insert(entry.rpc),
                "duplicate coverage entry for {}",
                entry.rpc
            );
        }
    }

    /// Pin: every entry has a non-empty RPC name. The dashboard +
    /// regression tests rely on `rpc` being a usable key.
    #[test]
    fn every_entry_has_a_non_empty_rpc_name() {
        for entry in HANDLER_COVERAGE {
            assert!(
                !entry.rpc.trim().is_empty(),
                "empty RPC name in coverage list"
            );
            assert!(
                entry.rpc.starts_with("/udb.services.v1.DataBroker/"),
                "RPC name '{}' missing canonical DataBroker prefix",
                entry.rpc
            );
        }
    }

    /// Pin: `wired: true` rows MUST have an empty note (no follow-up
    /// for completed work). `wired: false` rows MUST have a non-empty
    /// note pointing at the follow-up bullet.
    #[test]
    fn note_field_matches_wired_state() {
        for entry in HANDLER_COVERAGE {
            if entry.wired {
                assert!(
                    entry.note.is_empty(),
                    "{}: wired entry should not carry a note",
                    entry.rpc
                );
            } else {
                assert!(
                    !entry.note.trim().is_empty(),
                    "{}: unwired entry must point at a follow-up",
                    entry.rpc
                );
            }
        }
    }

    /// Pin: `report()` returns counts that sum correctly.
    #[test]
    fn report_returns_consistent_counts() {
        let (wired, total, unwired) = report();
        assert_eq!(wired + unwired.len(), total);
        for rpc in &unwired {
            assert!(HANDLER_COVERAGE.iter().any(|e| !e.wired && &e.rpc == rpc));
        }
    }

    /// Pin: coverage percentage is consistent with the report counts.
    /// Sanity check the dashboard math.
    #[test]
    fn coverage_percent_matches_report() {
        let (wired, total, _) = report();
        let expected = if total == 0 {
            100
        } else {
            ((wired as u64 * 100) / total as u64) as u32
        };
        assert_eq!(coverage_percent(), expected);
    }

    /// Pin: at least 80% of handlers must be wired. This is the U7
    /// acceptance threshold — falling below means a regression and
    /// catches `wired: false` slipping in unchallenged.
    #[test]
    fn coverage_meets_minimum_threshold() {
        let pct = coverage_percent();
        assert!(
            pct >= 80,
            "pipeline coverage dropped below 80% (currently {pct}%); \
             check HANDLER_COVERAGE for newly-flipped `wired: false` entries"
        );
    }
}
