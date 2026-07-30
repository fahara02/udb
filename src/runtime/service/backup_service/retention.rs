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
//! Leader-lane drivers (the periodic sweep the leader-elected worker calls):
//!   * [`enabled_backup_policies`] is the CROSS-TENANT control-plane read: one
//!     bounded, RLS-bypassing raw-SQL scan of the backup-policy relation (owner
//!     connection, `enable_rls` not FORCE — the same posture the lock/scheduler
//!     sweeps use) returning every ENABLED policy across ALL tenants. This is a
//!     control-plane enumeration and stays in the leader lane, never a per-tenant
//!     handler.
//!   * [`run_backup_retention_once`] enumerates enabled policies and calls
//!     [`prune_tenant_backups`] per tenant (log-and-continue so one failure never
//!     aborts the sweep).
//!   * [`run_scheduled_backups_once`] fires a due scheduled backup per enabled
//!     policy through the SAME internal routine `StartTenantBackup` uses
//!     ([`super::export::run_tenant_backup`]) — never through the gRPC layer. The
//!     due decision ([`backup_due`]) is a PURE, unit-tested comparison of the
//!     cron's next expected fire against the tenant's most-recent completed run.
//!
//! TODO(leader-wire): the periodic, leader-elected trigger lives in the shared
//! scheduler lane (`service::serve()` leader election), not in this dir. Wire a
//! leader-only interval task under `singleton::WORKER_BACKUP_RETENTION` that
//! builds a `BackupServiceImpl` (mirroring the webhook/vault worker blocks via
//! `DataBrokerService::build_backup_service`) and, each tick, calls
//! `run_backup_retention_once(&svc)` and
//! `run_scheduled_backups_once(&svc, scheduler_service::cron::next_cron_after)`
//! on a `UDB_BACKUP_MAINTENANCE_INTERVAL_SECS` cadence (see
//! `config::backup_maintenance_interval`). `run_scheduled_backups_once` takes the
//! cron next-fire evaluator as an injected parameter because the shared
//! `scheduler_service::cron` module is currently private to that service; the
//! spawn must promote it (`pub(crate) mod cron;` / re-export `next_cron_after`)
//! and pass it in, rather than this dir duplicating the cron grammar (no island).
//! Until that spawn lands, retention + scheduled backups run only when these
//! drivers are invoked explicitly.

// Leader-lane entry points: `enabled_backup_policies`, `run_backup_retention_once`,
// `run_scheduled_backups_once`, and `backup_due` are invoked by the leader-elected
// backup-maintenance spawn (see the `TODO(leader-wire)` above), which lives in the
// shared scheduler lane and is not yet wired. Allow dead_code here so those
// not-yet-called drivers do not warn until that spawn lands; the pure
// `runs_to_prune`/`backup_due` selectors are exercised by the unit tests below,
// and `prune_tenant_backups` is now a live dependency of `run_backup_retention_once`.
#![allow(dead_code)]

use chrono::{DateTime, Utc};
use sqlx::Row;
use tonic::Status;

use crate::ir::{ComparisonOp, LogicalDelete, LogicalFilter};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::BackupServiceImpl;
use super::config::{
    BACKUP_POLICY_MSG, BACKUP_RUN_MSG, KIND_BACKUP, MANIFEST_SUFFIX, MAX_LIST_ROWS,
};
use super::errors::backup_internal_status;
use super::export::run_tenant_backup;
use super::model::run_summary_from_json;
use super::store::{logical_string, runs_list_read};

/// Hard upper bound on runs scanned in one prune pass so a single invocation can
/// never walk an unbounded run history (the very failure retention exists to
/// prevent). A pass that hits the cap still prunes what it found; the next tick
/// continues shrinking the tail.
const PRUNE_SCAN_CAP: usize = 5_000;

const SECONDS_PER_DAY: i64 = 86_400;

/// Hard upper bound on ENABLED policies enumerated in one maintenance pass, so a
/// single cross-tenant control-plane scan can never walk an unbounded policy set
/// (the same bounding discipline `PRUNE_SCAN_CAP` gives the per-tenant run scan).
const MAX_ENABLED_POLICIES_SCAN: i64 = 10_000;

/// When a policy has NEVER produced a completed backup, the scheduled-backup due
/// check anchors the cron search this far back from `now` rather than at the
/// epoch — so the first-ever backup fires on the first cron occurrence that has
/// already elapsed within a bounded, recent window (keeping the cron search
/// cheap) instead of either never firing or replaying years of occurrences.
const NEVER_BACKED_UP_CATCHUP_SECS: i64 = SECONDS_PER_DAY;

/// One ENABLED backup policy, projected to just the fields the leader-lane
/// maintenance drivers need. Read cross-tenant by [`enabled_backup_policies`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnabledBackupPolicy {
    pub tenant_id: String,
    pub retention_days: i32,
    pub max_retained_backups: i32,
    pub schedule_cron: String,
}

/// Manifest-derived model for the durable backup-policy table, so the
/// cross-tenant enumeration SQL below follows the same single-source-of-truth
/// rule as the lock/scheduler sweeps (no hand-maintained schema copies). Uses the
/// SAME relation/columns the policy RPC handlers read/write.
fn backup_policy_model() -> NativeModel {
    native_model(
        BACKUP_POLICY_MSG,
        &[
            "tenant_id",
            "retention_days",
            "max_retained_backups",
            "schedule_cron",
            "enabled",
        ],
    )
}

/// The bounded cross-tenant enumeration SQL: every ENABLED policy's
/// `(tenant_id, retention_days, max_retained_backups, schedule_cron)` across ALL
/// tenants, capped by `LIMIT $1`. Exposed (and unit-tested) so the read-only,
/// bounded, enabled-only shape is asserted on the rendered SQL, mirroring
/// `lock_service::expired_locks_claim_sql`. Columns are cast to stable scalar
/// types so a UUID tenant column and a nullable cron both decode uniformly.
pub(crate) fn enabled_policies_sql(m: &NativeModel) -> String {
    format!(
        "SELECT {tenant}::text AS tenant_id, \
                {retention}::bigint AS retention_days, \
                {max_ret}::bigint AS max_retained_backups, \
                COALESCE({cron}::text, '') AS schedule_cron \
         FROM {rel} \
         WHERE {enabled} = TRUE \
         ORDER BY {tenant} \
         LIMIT $1",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        retention = m.q("retention_days"),
        max_ret = m.q("max_retained_backups"),
        cron = m.q("schedule_cron"),
        enabled = m.q("enabled"),
    )
}

/// CROSS-TENANT control-plane read: every ENABLED backup policy across ALL
/// tenants. This is RLS-bypassing by design (the native-store pool connects as
/// the table owner, which `enable_rls` — not FORCE — exempts from the tenant RLS
/// policy, the identical posture the lock expiry reaper and scheduler tick use)
/// and belongs to the leader/scheduler lane, never a per-tenant handler. Bounded
/// by `MAX_ENABLED_POLICIES_SCAN`; fail-safe (a blank tenant row is skipped, not
/// acted on). Requires the Postgres-backed store — fails closed otherwise.
pub(crate) async fn enabled_backup_policies(
    svc: &BackupServiceImpl,
) -> Result<Vec<EnabledBackupPolicy>, Status> {
    let pool = svc.require_pool()?;
    let model = backup_policy_model();
    let sql = enabled_policies_sql(&model);
    let rows = sqlx::query(&sql)
        .bind(MAX_ENABLED_POLICIES_SCAN)
        .fetch_all(pool)
        .await
        .map_err(|err| {
            backup_internal_status(
                "enumerate_enabled_policies",
                format!("backup policy enumeration failed: {err}"),
            )
        })?;
    let mut policies = Vec::with_capacity(rows.len());
    for row in &rows {
        let tenant_id: String = row.try_get("tenant_id").unwrap_or_default();
        if tenant_id.trim().is_empty() {
            continue;
        }
        let retention_days: i64 = row.try_get("retention_days").unwrap_or(0);
        let max_retained_backups: i64 = row.try_get("max_retained_backups").unwrap_or(0);
        let schedule_cron: String = row.try_get("schedule_cron").unwrap_or_default();
        policies.push(EnabledBackupPolicy {
            tenant_id,
            retention_days: clamp_i32(retention_days),
            max_retained_backups: clamp_i32(max_retained_backups),
            schedule_cron,
        });
    }
    Ok(policies)
}

/// Clamp a stored BIGINT policy bound into `i32` (the proto width), saturating
/// rather than wrapping so an out-of-range value can never flip sign and disable
/// a bound it was meant to enforce.
fn clamp_i32(value: i64) -> i32 {
    value.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32
}

/// RETENTION driver (leader-lane): enumerate every enabled policy and prune each
/// tenant's over-retention runs. Bounded (the enumeration cap + `PRUNE_SCAN_CAP`)
/// and fail-safe per tenant — an unconfigured policy (both bounds `<= 0`) is
/// skipped, and a per-tenant prune error is logged and skipped so ONE tenant's
/// failure never aborts the whole sweep. Returns the total number of runs pruned
/// across all tenants (for the worker's `acted = n` log line).
pub(crate) async fn run_backup_retention_once(svc: &BackupServiceImpl) -> Result<i64, Status> {
    let policies = enabled_backup_policies(svc).await?;
    let mut runs_pruned: i64 = 0;
    for policy in policies {
        // Skip unconfigured policies early (prune_tenant_backups is also a no-op
        // for them, but skipping avoids a needless run enumeration per tenant).
        if policy.retention_days <= 0 && policy.max_retained_backups <= 0 {
            continue;
        }
        match prune_tenant_backups(
            svc,
            &policy.tenant_id,
            policy.retention_days,
            policy.max_retained_backups,
        )
        .await
        {
            Ok(outcome) => {
                runs_pruned = runs_pruned.saturating_add(outcome.runs_pruned as i64);
            }
            Err(err) => {
                tracing::warn!(
                    target: "udb.backup.retention",
                    tenant_id = %policy.tenant_id,
                    error = %err,
                    "retention sweep: per-tenant prune failed; continuing"
                );
            }
        }
    }
    Ok(runs_pruned)
}

/// PURE due decision (unit-tested without Postgres or a real cron engine): given
/// a next-fire evaluator, decide whether a scheduled backup is DUE for a policy.
///
/// A backup is due when a scheduled cron occurrence has ELAPSED since the
/// tenant's most-recent completed backup: `next_fire(cron, anchor)` yields the
/// first fire strictly after the anchor, and the policy is due when that fire is
/// at or before `now`. The anchor is the most-recent completed run
/// (`last_backup_unix`), or — when the policy has never produced one — a bounded
/// recent window (`NEVER_BACKED_UP_CATCHUP_SECS`) so the first backup fires on
/// its first already-elapsed occurrence.
///
/// Fail-safe: a blank cron, an out-of-range `now`, or an unparseable cron
/// (`next_fire` returns `None`) is NEVER due.
pub(crate) fn backup_due<F>(next_fire: F, cron: &str, last_backup_unix: i64, now_unix: i64) -> bool
where
    F: Fn(&str, DateTime<Utc>) -> Option<DateTime<Utc>>,
{
    let cron = cron.trim();
    if cron.is_empty() {
        return false;
    }
    let Some(now) = DateTime::<Utc>::from_timestamp(now_unix, 0) else {
        return false;
    };
    let anchor_unix = if last_backup_unix > 0 {
        last_backup_unix
    } else {
        now_unix.saturating_sub(NEVER_BACKED_UP_CATCHUP_SECS)
    };
    let Some(anchor) = DateTime::<Utc>::from_timestamp(anchor_unix, 0) else {
        return false;
    };
    match next_fire(cron, anchor) {
        Some(fire) => fire <= now,
        None => false,
    }
}

/// The tenant's most-recent completed BACKUP run time (unix seconds), or 0 when
/// the tenant has none. Reads the newest journal row via the SAME tenant-scoped
/// native-entity dispatch the handlers use (`runs_list_read` sorts newest-first),
/// preferring `completed_at` and falling back to `created_at`.
async fn most_recent_backup_unix(svc: &BackupServiceImpl, tenant_id: &str) -> Result<i64, Status> {
    let runtime = svc.require_runtime()?;
    let context = crate::RequestContext {
        tenant_id: tenant_id.to_string(),
        ..crate::RequestContext::default()
    };
    let rows = runtime
        .native_entity_read_for_service(
            "backup",
            &context,
            runs_list_read(tenant_id, Some(KIND_BACKUP), 1, 0),
        )
        .await?;
    let last = rows
        .first()
        .map(run_summary_from_json)
        .map(|run| {
            if run.completed_at_unix > 0 {
                run.completed_at_unix
            } else {
                run.created_at_unix
            }
        })
        .unwrap_or(0);
    Ok(last)
}

/// SCHEDULED-BACKUP driver (leader-lane): enumerate every enabled policy and, for
/// each with a non-empty `schedule_cron` that is DUE, fire a backup through the
/// SAME internal routine `StartTenantBackup` uses ([`run_tenant_backup`]) — NOT
/// through the gRPC layer. The cron next-fire evaluator is INJECTED (rather than
/// this dir duplicating the shared `scheduler_service::cron` grammar): the leader
/// spawn passes `scheduler_service::cron::next_cron_after`. Bounded (the
/// enumeration cap) and fail-safe per tenant — a per-tenant read/fire error is
/// logged and skipped so ONE tenant never aborts the sweep. Returns the number of
/// backups fired.
pub(crate) async fn run_scheduled_backups_once<F>(
    svc: &BackupServiceImpl,
    next_fire: F,
) -> Result<i64, Status>
where
    F: Fn(&str, DateTime<Utc>) -> Option<DateTime<Utc>>,
{
    let policies = enabled_backup_policies(svc).await?;
    let now_unix = Utc::now().timestamp();
    let mut fired: i64 = 0;
    for policy in policies {
        if policy.schedule_cron.trim().is_empty() {
            continue;
        }
        let last_backup_unix = match most_recent_backup_unix(svc, &policy.tenant_id).await {
            Ok(unix) => unix,
            Err(err) => {
                tracing::warn!(
                    target: "udb.backup.scheduled",
                    tenant_id = %policy.tenant_id,
                    error = %err,
                    "scheduled backup: reading most-recent run failed; skipping tenant"
                );
                continue;
            }
        };
        if !backup_due(
            &next_fire,
            &policy.schedule_cron,
            last_backup_unix,
            now_unix,
        ) {
            continue;
        }
        // Fire through the SAME internal routine the RPC uses. The policy's own
        // tenant is the verified tenant here (read cross-tenant in the leader
        // lane), so no request-scoped identity is smuggled. Object target falls
        // back to the service default (the enumerator projects only the retention
        // + schedule columns); the run is journaled and emits its outbox event
        // exactly as an operator-triggered backup.
        let context = crate::RequestContext {
            tenant_id: policy.tenant_id.clone(),
            ..crate::RequestContext::default()
        };
        match run_tenant_backup(svc, &policy.tenant_id, "", "", context).await {
            Ok(_) => {
                fired = fired.saturating_add(1);
            }
            Err(err) => {
                tracing::warn!(
                    target: "udb.backup.scheduled",
                    tenant_id = %policy.tenant_id,
                    error = %err,
                    "scheduled backup: fire failed; continuing"
                );
            }
        }
    }
    Ok(fired)
}

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
    use super::{
        NEVER_BACKED_UP_CATCHUP_SECS, backup_due, backup_policy_model, clamp_i32,
        enabled_policies_sql, runs_to_prune,
    };
    use chrono::{DateTime, Duration, Utc};
    use std::cell::Cell;

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

    // ── cross-tenant enumerator SQL shape ─────────────────────────────────────

    /// The enabled-policy enumeration SQL must be a READ-ONLY, ENABLED-only,
    /// BOUNDED scan over the manifest-derived backup-policy relation, projecting
    /// exactly the four columns the leader-lane drivers consume. Asserted on the
    /// rendered SQL (no Postgres), mirroring `lock_service::expired_locks_claim_sql`.
    #[test]
    fn enabled_policies_sql_shape() {
        let sql = enabled_policies_sql(&backup_policy_model());
        assert!(sql.starts_with("SELECT"), "read-only projection: {sql}");
        for alias in [
            "AS tenant_id",
            "AS retention_days",
            "AS max_retained_backups",
            "AS schedule_cron",
        ] {
            assert!(sql.contains(alias), "must project {alias}: {sql}");
        }
        assert!(sql.contains("= TRUE"), "enabled-only filter: {sql}");
        assert!(sql.contains("LIMIT $1"), "bounded scan: {sql}");
        // A control-plane READ must never mutate the policy relation.
        for forbidden in ["UPDATE", "DELETE", "INSERT"] {
            assert!(
                !sql.contains(forbidden),
                "enumeration must stay read-only ({forbidden}): {sql}"
            );
        }
    }

    // ── pure scheduled-backup due decision ────────────────────────────────────

    /// A next-fire evaluator that fires `step` after the anchor — a deterministic
    /// stand-in for the real cron engine so the due logic is tested in isolation.
    fn fires_after(step: i64) -> impl Fn(&str, DateTime<Utc>) -> Option<DateTime<Utc>> {
        move |_cron: &str, anchor: DateTime<Utc>| Some(anchor + Duration::seconds(step))
    }

    /// A blank cron is NEVER due (fail-safe) — the evaluator is not even consulted.
    #[test]
    fn due_blank_cron_is_never_due() {
        let called = Cell::new(false);
        let probe = |_c: &str, a: DateTime<Utc>| {
            called.set(true);
            Some(a)
        };
        assert!(!backup_due(probe, "   ", 1_000, 2_000));
        assert!(
            !called.get(),
            "blank cron must short-circuit before the cron eval"
        );
    }

    /// An unparseable cron (evaluator returns `None`) is NEVER due (fail-safe).
    #[test]
    fn due_unparseable_cron_is_never_due() {
        let never = |_c: &str, _a: DateTime<Utc>| None;
        assert!(!backup_due(never, "not-a-cron", 1_000, 10_000));
    }

    /// Due exactly when a scheduled occurrence has ELAPSED (fire <= now) since the
    /// most-recent backup; a fire still in the future is not yet due.
    #[test]
    fn due_when_occurrence_elapsed_since_last_backup() {
        let last_backup = 1_000;
        // Fire lands at last_backup + 60 = 1060.
        assert!(
            backup_due(fires_after(60), "* * * * *", last_backup, 1_060),
            "fire at 1060 <= now 1060 is due"
        );
        assert!(
            !backup_due(fires_after(60), "* * * * *", last_backup, 1_059),
            "fire at 1060 > now 1059 is not yet due"
        );
    }

    /// A policy that has NEVER produced a backup anchors the cron search a bounded
    /// window back from `now`, so its first already-elapsed occurrence fires it.
    #[test]
    fn due_never_backed_up_uses_catchup_anchor() {
        let now = 10_000_000;
        let seen_anchor = Cell::new(i64::MIN);
        let resolver = |_c: &str, a: DateTime<Utc>| {
            seen_anchor.set(a.timestamp());
            Some(a + Duration::seconds(60))
        };
        // last_backup_unix == 0 → anchor at now - catchup; the +60s fire is well
        // within the day-wide window, so a never-run sub-daily policy is due.
        assert!(backup_due(resolver, "* * * * *", 0, now));
        assert_eq!(
            seen_anchor.get(),
            now - NEVER_BACKED_UP_CATCHUP_SECS,
            "never-backed-up policy must anchor at the bounded catch-up window"
        );
    }

    /// An out-of-range `now` decodes to no timestamp → NEVER due (fail-safe).
    #[test]
    fn due_invalid_now_is_never_due() {
        assert!(!backup_due(fires_after(1), "* * * * *", 1_000, i64::MAX));
    }

    /// Policy bounds stored as BIGINT saturate into the proto `i32` width rather
    /// than wrapping (a wrap could flip a positive bound negative and disable it).
    #[test]
    fn clamp_i32_saturates_out_of_range_bounds() {
        assert_eq!(clamp_i32(30), 30);
        assert_eq!(clamp_i32(0), 0);
        assert_eq!(clamp_i32(i64::MAX), i32::MAX);
        assert_eq!(clamp_i32(i64::MIN), i32::MIN);
        assert_eq!(clamp_i32(i64::from(i32::MAX) + 1), i32::MAX);
    }
}
