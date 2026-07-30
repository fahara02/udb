//! Retention pruning for the native `BackupService`.
//!
//! A `BackupPolicy` advertises `retention_days` / `max_retained_backups`, but
//! nothing enforced them: completed backup runs and their encrypted object
//! artifacts accumulated without bound. This module is the bounded, fail-safe
//! routine that closes that gap — it deletes the oldest runs (journal rows +
//! their objects) beyond the policy window, tenant-scoped, and never on an
//! unconfigured policy.
//!
//! Split responsibilities:
//!   * [`runs_to_prune`] is the PURE selector (unit-tested without Postgres): it
//!     decides which run ids fall outside `retention_days` / `max_retained_backups`.
//!   * [`prune_tenant_backups`] is the executor: it bounds the run enumeration,
//!     runs the selector, and deletes the losers best-effort so a transient
//!     object-store error can never wedge retention.
//!
//! TODO(leader-wire): the periodic, leader-elected trigger lives in the shared
//! scheduler lane (`service::serve()` leader election), not in this dir. Wire a
//! leader-only interval task that, per enabled `BackupPolicy`, calls
//! `backup_service::retention::prune_tenant_backups(&svc, &policy.tenant_id,
//! policy.retention_days, policy.max_retained_backups)`. Enumerating enabled
//! policies across tenants is a cross-tenant control-plane read and must stay in
//! the scheduler/leader lane (never a per-tenant handler). Until that spawn
//! lands, retention runs only when this routine is invoked explicitly; the
//! scheduled-BACKUP trigger (fire a StartTenantBackup on a due policy) remains
//! unimplemented and is owned by the same scheduler lane.

// Ready-to-wire primitive: `prune_tenant_backups` and its helpers are invoked by
// the leader-elected scheduler spawn (see the `TODO(leader-wire)` above), which
// lives in the shared scheduler lane and is not yet wired. Allow dead_code here
// so the not-yet-called routine does not warn until that spawn lands; the pure
// `runs_to_prune` selector is exercised by the unit tests below.
#![allow(dead_code)]

use tonic::Status;

use crate::ir::{ComparisonOp, LogicalDelete, LogicalFilter};
use crate::runtime::DataBrokerRuntime;

use super::BackupServiceImpl;
use super::config::{BACKUP_RUN_MSG, KIND_BACKUP, MANIFEST_SUFFIX, MAX_LIST_ROWS};
use super::model::run_summary_from_json;
use super::store::{logical_string, runs_list_read};

/// Hard upper bound on runs scanned in one prune pass so a single invocation can
/// never walk an unbounded run history (the very failure retention exists to
/// prevent). A pass that hits the cap still prunes what it found; the next tick
/// continues shrinking the tail.
const PRUNE_SCAN_CAP: usize = 5_000;

const SECONDS_PER_DAY: i64 = 86_400;

/// What a prune pass removed. Returned for logging/metrics; callers may ignore it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PruneOutcome {
    pub runs_pruned: u64,
    pub objects_deleted: u64,
}

/// PURE selector: given a tenant's completed BACKUP runs newest-first as
/// `(backup_id, created_at_unix)`, return the ids to PRUNE under the policy.
///
/// Fail-safe rules:
///   * an unconfigured policy (both knobs `<= 0`) prunes NOTHING — retention only
///     ever deletes when a bound is explicitly set;
///   * `max_retained_backups > 0` keeps the newest N, prunes the overflow;
///   * `retention_days > 0` prunes runs strictly older than the cutoff;
///   * an undated run (`created_at_unix <= 0`) is never age-pruned (only pruned by
///     the count bound), so a missing timestamp never triggers deletion;
///   * a run is pruned if it violates EITHER active bound (union).
pub(crate) fn runs_to_prune(
    runs_newest_first: &[(String, i64)],
    retention_days: i32,
    max_retained_backups: i32,
    now_unix: i64,
) -> Vec<String> {
    if retention_days <= 0 && max_retained_backups <= 0 {
        return Vec::new();
    }
    let age_cutoff = (retention_days > 0).then(|| {
        now_unix.saturating_sub(i64::from(retention_days).saturating_mul(SECONDS_PER_DAY))
    });
    let keep_count = (max_retained_backups > 0).then_some(max_retained_backups as usize);

    let mut prune = Vec::new();
    for (index, (backup_id, created_at_unix)) in runs_newest_first.iter().enumerate() {
        if backup_id.trim().is_empty() {
            continue;
        }
        let over_count = keep_count.is_some_and(|keep| index >= keep);
        let too_old =
            age_cutoff.is_some_and(|cutoff| *created_at_unix > 0 && *created_at_unix < cutoff);
        if over_count || too_old {
            prune.push(backup_id.clone());
        }
    }
    prune
}

/// Enforce a tenant's retention policy: prune the oldest completed BACKUP runs
/// (journal rows + their encrypted objects) beyond `retention_days` /
/// `max_retained_backups`. Bounded (`PRUNE_SCAN_CAP`) and fail-safe: an
/// unconfigured policy is a no-op, and object/journal deletions are best-effort
/// so one transient error never wedges the whole pass. Tenant-scoped — only the
/// given tenant's runs are ever touched.
pub(crate) async fn prune_tenant_backups(
    svc: &BackupServiceImpl,
    tenant_id: &str,
    retention_days: i32,
    max_retained_backups: i32,
) -> Result<PruneOutcome, Status> {
    let tenant_id = tenant_id.trim();
    if tenant_id.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "tenant_id is required",
            [("tenant_id", "must be a non-empty tenant id")],
        ));
    }
    // Fail-safe: nothing configured → no-op (never prune on an empty policy).
    if retention_days <= 0 && max_retained_backups <= 0 {
        return Ok(PruneOutcome::default());
    }
    let runtime = svc.require_runtime()?;
    // Tenant-scoped context; retention is not project-scoped (a run belongs to a
    // tenant), so project stays empty and the entity dispatch defaults it.
    let context = crate::RequestContext {
        tenant_id: tenant_id.to_string(),
        ..crate::RequestContext::default()
    };

    // Bounded enumeration of this tenant's completed BACKUP runs, newest-first.
    let mut runs: Vec<(String, i64, String)> = Vec::new(); // (backup_id, created_at, object_prefix)
    let page: u32 = MAX_LIST_ROWS;
    let mut offset: u64 = 0;
    loop {
        let rows = runtime
            .native_entity_read_for_service(
                "backup",
                &context,
                runs_list_read(tenant_id, Some(KIND_BACKUP), page, offset),
            )
            .await?;
        let fetched = rows.len();
        for row in &rows {
            let run = run_summary_from_json(row);
            runs.push((run.backup_id, run.created_at_unix, run.object_prefix));
        }
        if fetched < page as usize || runs.len() >= PRUNE_SCAN_CAP {
            break;
        }
        offset = offset.saturating_add(fetched as u64);
    }

    let ids: Vec<(String, i64)> = runs.iter().map(|(id, ts, _)| (id.clone(), *ts)).collect();
    let now_unix = chrono::Utc::now().timestamp();
    let prune_ids = runs_to_prune(&ids, retention_days, max_retained_backups, now_unix);
    if prune_ids.is_empty() {
        return Ok(PruneOutcome::default());
    }
    let prune_set: std::collections::HashSet<&str> = prune_ids.iter().map(String::as_str).collect();

    let mut outcome = PruneOutcome::default();
    for (backup_id, _ts, object_prefix) in &runs {
        if !prune_set.contains(backup_id.as_str()) {
            continue;
        }
        outcome.objects_deleted += delete_run_objects(svc, runtime, &context, object_prefix).await;
        delete_run_journal_row(runtime, &context, tenant_id, backup_id).await;
        outcome.runs_pruned += 1;
    }
    Ok(outcome)
}

/// Best-effort deletion of a run's object artifacts. The manifest lists the
/// per-table encrypted objects and records the backend/bucket they live under
/// (mirroring restore); we delete those, then the manifest itself. Best-effort:
/// a missing/unreadable manifest or a transient object-store error is logged and
/// never fails retention (the journal row is still pruned by the caller).
async fn delete_run_objects(
    svc: &BackupServiceImpl,
    runtime: &DataBrokerRuntime,
    context: &crate::RequestContext,
    object_prefix: &str,
) -> u64 {
    let object_prefix = object_prefix.trim();
    if object_prefix.is_empty() {
        return 0;
    }
    let manifest_key = format!("{object_prefix}{MANIFEST_SUFFIX}");
    let run_backend = svc.object_backend.clone();
    let run_bucket = svc.object_bucket.clone();
    let manifest_get = crate::runtime::core::setup_data::object_request_json(
        "get",
        &run_bucket,
        &manifest_key,
        "",
    );
    let mut deleted: u64 = 0;
    if let Ok(bytes) = runtime
        .get_object_backend_target_for_project(
            &run_backend,
            None,
            &context.project_id,
            &manifest_get,
        )
        .await
        && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes)
    {
        let object_backend = value
            .get("object_backend")
            .and_then(|v| v.as_str())
            .filter(|v| !v.trim().is_empty())
            .unwrap_or(run_backend.as_str())
            .to_string();
        let object_bucket = value
            .get("object_bucket")
            .and_then(|v| v.as_str())
            .filter(|v| !v.trim().is_empty())
            .unwrap_or(run_bucket.as_str())
            .to_string();
        if let Some(tables) = value.get("tables").and_then(|v| v.as_array()) {
            for entry in tables {
                let Some(object_key) = entry
                    .get("object_key")
                    .and_then(|v| v.as_str())
                    .filter(|k| !k.trim().is_empty())
                else {
                    continue;
                };
                let del = crate::runtime::core::setup_data::object_request_json(
                    "delete",
                    &object_bucket,
                    object_key,
                    "",
                );
                if runtime
                    .delete_object_backend_target(&object_backend, None, &context.project_id, &del)
                    .await
                    .is_ok()
                {
                    deleted += 1;
                } else {
                    tracing::warn!(
                        target: "udb.backup.retention",
                        object_key,
                        "retention: best-effort table-object delete failed"
                    );
                }
            }
        }
    }
    // Delete the manifest object last (after the artifacts it points at).
    let manifest_del = crate::runtime::core::setup_data::object_request_json(
        "delete",
        &run_bucket,
        &manifest_key,
        "",
    );
    if runtime
        .delete_object_backend_target(&run_backend, None, &context.project_id, &manifest_del)
        .await
        .is_ok()
    {
        deleted += 1;
    }
    deleted
}

/// Best-effort deletion of a run's durable journal row, tenant-scoped. Logged and
/// swallowed on failure so one bad row never aborts the remaining prunes.
async fn delete_run_journal_row(
    runtime: &DataBrokerRuntime,
    context: &crate::RequestContext,
    tenant_id: &str,
    backup_id: &str,
) {
    let op = LogicalDelete {
        message_type: BACKUP_RUN_MSG.to_string(),
        filter: LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(tenant_id),
            },
            LogicalFilter::Comparison {
                field: "backup_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(backup_id),
            },
        ]),
        return_fields: Vec::new(),
    };
    if let Err(err) = runtime
        .native_entity_delete_for_service("backup", context, op)
        .await
    {
        tracing::warn!(
            target: "udb.backup.retention",
            backup_id,
            error = %err,
            "retention: best-effort journal-row delete failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::runs_to_prune;

    fn runs(ids_ages: &[(&str, i64)]) -> Vec<(String, i64)> {
        ids_ages
            .iter()
            .map(|(id, age)| ((*id).to_string(), *age))
            .collect()
    }

    /// An unconfigured policy (both bounds unset) prunes NOTHING — retention must
    /// never delete on an empty/inert policy (the fail-safe core).
    #[test]
    fn unconfigured_policy_prunes_nothing() {
        let now = 1_000_000;
        let history = runs(&[("a", now - 10), ("b", now - 20)]);
        assert!(runs_to_prune(&history, 0, 0, now).is_empty());
        assert!(runs_to_prune(&history, -5, -1, now).is_empty());
    }

    /// `max_retained_backups` keeps the newest N (list is newest-first) and prunes
    /// the older overflow.
    #[test]
    fn count_bound_prunes_oldest_overflow() {
        let now = 1_000_000;
        let history = runs(&[("new", now - 1), ("mid", now - 2), ("old", now - 3)]);
        let prune = runs_to_prune(&history, 0, 2, now);
        assert_eq!(prune, vec!["old".to_string()]);
    }

    /// `retention_days` prunes runs strictly older than the cutoff; a fresh run
    /// inside the window survives.
    #[test]
    fn age_bound_prunes_only_stale_runs() {
        let now = 10 * super::SECONDS_PER_DAY;
        let history = runs(&[
            ("fresh", now - super::SECONDS_PER_DAY),     // 1 day old
            ("stale", now - 5 * super::SECONDS_PER_DAY), // 5 days old
        ]);
        // Retain 2 days: the 5-day-old run is pruned, the 1-day-old kept.
        let prune = runs_to_prune(&history, 2, 0, now);
        assert_eq!(prune, vec!["stale".to_string()]);
    }

    /// An undated run (`created_at_unix <= 0`) is never age-pruned — a missing
    /// timestamp must not trigger deletion (fail-safe).
    #[test]
    fn undated_run_is_never_age_pruned() {
        let now = 10 * super::SECONDS_PER_DAY;
        let history = runs(&[("undated", 0)]);
        assert!(runs_to_prune(&history, 1, 0, now).is_empty());
    }

    /// The two bounds are a UNION: a run pruned by EITHER age or count is removed.
    #[test]
    fn bounds_union_removes_either_violation() {
        let now = 10 * super::SECONDS_PER_DAY;
        let history = runs(&[
            ("keep", now - super::SECONDS_PER_DAY),
            ("too_old", now - 8 * super::SECONDS_PER_DAY),
            ("overflow", now - super::SECONDS_PER_DAY),
        ]);
        // Keep newest 2 AND retain 3 days: index-2 "overflow" fails the count
        // bound, "too_old" fails the age bound; both are pruned.
        let mut prune = runs_to_prune(&history, 3, 2, now);
        prune.sort();
        assert_eq!(prune, vec!["overflow".to_string(), "too_old".to_string()]);
    }

    /// Empty backup ids are skipped defensively (never emitted as prune targets).
    #[test]
    fn blank_ids_are_skipped() {
        let now = 1_000_000;
        let history = runs(&[("  ", now - 100), ("real", now - 200)]);
        let prune = runs_to_prune(&history, 0, 1, now);
        assert_eq!(prune, vec!["real".to_string()]);
    }
}
