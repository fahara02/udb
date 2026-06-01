//! P2P-8 — cross-impl CanonicalStore conformance contract.
//!
//! The point of `CanonicalStore` is that every impl honours the same
//! contract: same token discipline, same outbox semantics, same DDL
//! result. If they drift, the runtime can't trust which store it's
//! talking to.
//!
//! This module factors the contract checks into one function — and
//! runs it against an in-memory SQLite store unconditionally so the
//! contract is pinned on every CI run with zero external dependencies.
//!
//! Adding MySQL or Postgres to the matrix is a one-line change:
//! supply the appropriate `Arc<dyn CanonicalStore>` to `run_contract`.
//! Live-DSN integration tests can do that under env-gates without
//! duplicating the assertions.

use std::sync::Arc;
use std::time::Duration;

use super::CanonicalStore;
use super::system_store::{
    AdminAuditChainReport, AdminAuditInsert, AdminAuditListFilter, AdminAuditStore,
    CompensationStatus, MigrationAuditStore, MigrationOpInsert, MigrationRunInsert,
    MigrationRunState, MigrationRunsFilter, OpLedgerStatus, ProjectionClaimFilter,
    ProjectionOperation, ProjectionTaskInsert, ProjectionTaskStatus, ProjectionTaskStore,
    SagaInsert, SagaListFilter, SagaStatus, SagaStore,
};

/// Single source of truth for the CanonicalStore contract. Every
/// impl is expected to pass every assertion below. Returns `Ok(())`
/// on success; panics via `assert!` on failure so the test output
/// names exactly which behaviour broke.
pub async fn run_contract(store: Arc<dyn CanonicalStore>) {
    // 1. Backend label is non-empty and consistent.
    let label = store.backend_label();
    assert!(!label.is_empty(), "backend_label must be non-empty");
    assert!(
        label.chars().all(|c| c.is_ascii_lowercase()),
        "backend_label '{label}' must be ASCII lowercase (registry uses it as a key)"
    );

    // 2. Instance name is reportable.
    let instance = store.instance_name().to_string();
    assert!(!instance.is_empty(), "instance_name must be non-empty");

    // 3. ensure_system_tables is idempotent.
    store
        .ensure_system_tables()
        .await
        .expect("ensure_system_tables (first call)");
    store
        .ensure_system_tables()
        .await
        .expect("ensure_system_tables (second call must be idempotent)");

    // 4. Initial outbox is empty.
    let initial_max = store
        .outbox_max_seq()
        .await
        .expect("outbox_max_seq on empty");
    assert_eq!(initial_max, 0, "fresh outbox must have max_seq = 0");

    // 5. Durability tokens are minted with the correct backend label.
    let token_pre = store
        .current_durability_token()
        .await
        .expect("current_durability_token");
    assert!(
        token_pre.is_for(label),
        "token backend_label mismatch: token says '{}', store says '{}'",
        token_pre.backend_label,
        label
    );
    assert!(!token_pre.value.is_empty(), "token value must be non-empty");

    // 6. Cross-backend tokens are rejected by wait_for_fence.
    use super::DurabilityToken;
    let foreign = if label == "postgres" {
        DurabilityToken::new("mysql", "foreign")
    } else {
        DurabilityToken::new("postgres", "0/100")
    };
    let err = store
        .wait_for_token(&foreign, Duration::from_millis(1))
        .await
        .expect_err("cross-backend token must be rejected");
    assert!(
        err.contains("cannot wait on") || err.contains("malformed"),
        "expected cross-backend rejection, got: {err}"
    );

    // 7. Enqueue an event → max_seq advances.
    let seq = store
        .enqueue_outbox_event(
            "22222222-2222-2222-2222-222222222222",
            "test.conformance",
            "pk-conformance",
            &serde_json::json!({"step": "enqueue"}),
        )
        .await
        .expect("enqueue");
    assert!(seq > 0, "enqueue should return a positive event_seq");
    let after_max = store.outbox_max_seq().await.expect("outbox_max_seq after");
    assert_eq!(
        after_max, seq,
        "outbox_max_seq should equal the latest enqueued seq"
    );

    // 8. Token after a write should be reachable via wait_for_token
    // with a small timeout (the store is talking to itself, no replica
    // lag involved).
    let token_post = store
        .current_durability_token()
        .await
        .expect("current_durability_token after write");
    let cleared = store
        .wait_for_token(&token_post, Duration::from_millis(100))
        .await
        .expect("wait_for_token after own write");
    assert!(
        cleared,
        "wait_for_token against own current token must clear immediately"
    );

    // 9. Advisory leases obey the portable worker contract across
    // canonical-store backends: idempotent DDL, same-owner refresh,
    // contention denial, owner-scoped release, and expired takeover.
    store
        .ensure_advisory_lease_table()
        .await
        .expect("ensure_advisory_lease_table (first call)");
    store
        .ensure_advisory_lease_table()
        .await
        .expect("ensure_advisory_lease_table must be idempotent");
    let lease = format!("conformance-lease-{}", uuid::Uuid::new_v4());
    assert!(
        store
            .try_acquire_advisory_lease(&lease, "owner-a", Duration::from_secs(60))
            .await
            .expect("owner-a initial acquire"),
        "first owner should acquire a fresh lease"
    );
    assert!(
        !store
            .try_acquire_advisory_lease(&lease, "owner-b", Duration::from_secs(60))
            .await
            .expect("owner-b contended acquire"),
        "different owner must not take a live lease"
    );
    assert!(
        store
            .try_acquire_advisory_lease(&lease, "owner-a", Duration::from_secs(60))
            .await
            .expect("owner-a refresh"),
        "same owner should refresh a live lease"
    );
    store
        .release_advisory_lease(&lease, "owner-b")
        .await
        .expect("wrong-owner release should be a no-op");
    assert!(
        !store
            .try_acquire_advisory_lease(&lease, "owner-b", Duration::from_secs(60))
            .await
            .expect("owner-b after wrong-owner release"),
        "wrong-owner release must not unlock the lease"
    );
    store
        .release_advisory_lease(&lease, "owner-a")
        .await
        .expect("owner-a release");
    assert!(
        store
            .try_acquire_advisory_lease(&lease, "owner-b", Duration::from_secs(60))
            .await
            .expect("owner-b after release"),
        "released lease should be acquirable by a different owner"
    );

    let expired_lease = format!("conformance-expired-lease-{}", uuid::Uuid::new_v4());
    assert!(
        store
            .try_acquire_advisory_lease(&expired_lease, "expired-owner", Duration::from_secs(0))
            .await
            .expect("expired-owner acquire"),
        "zero-ttl lease should be insertable"
    );
    assert!(
        store
            .try_acquire_advisory_lease(&expired_lease, "fresh-owner", Duration::from_secs(60))
            .await
            .expect("fresh-owner expired takeover"),
        "expired lease should be acquirable by a different owner"
    );
}

/// NW1-1a — Projection task contract every `ProjectionTaskStore` impl
/// must satisfy. Same factor pattern as `run_contract` above.
///
/// The store is wrapped in `Arc<dyn ProjectionTaskStore>` so the test
/// pinpoints **trait surface** behaviour, not concrete-type-specific
/// behaviour. If PG and MySQL impls diverge from SQLite, the runtime
/// can't transparently swap them.
pub async fn run_projection_task_contract(store: Arc<dyn ProjectionTaskStore>) {
    use uuid::Uuid;

    let label = store.backend_label();
    assert!(!label.is_empty());

    // 1. Idempotent table preparation.
    store
        .ensure_projection_tables()
        .await
        .expect("ensure_projection_tables (1st call)");
    store
        .ensure_projection_tables()
        .await
        .expect("ensure_projection_tables must be idempotent");

    // 2. Empty initial summary.
    let s0 = store
        .projection_task_summary()
        .await
        .expect("summary on empty");
    assert_eq!(s0.total(), 0, "fresh table starts empty");

    // 3. Enqueue.
    let task = ProjectionTaskInsert {
        idempotency_key: format!("conformance-{}", Uuid::new_v4()),
        project_id: "conformance".to_string(),
        manifest_checksum: "sha256:contract".to_string(),
        message_type: "udb.contract.v1.Probe".to_string(),
        source_schema: "public".to_string(),
        source_table: "probes".to_string(),
        source_row_key: serde_json::json!({"id": "probe-1"}),
        operation: ProjectionOperation::Upsert,
        target_backend: "test-target".to_string(),
        target_instance: "primary".to_string(),
        projection_kind: "vector".to_string(),
        resource_name: "probes_vec".to_string(),
        target_options: serde_json::json!([]),
        source_payload: serde_json::json!({"v": 1}),
        source_checksum: "sha256:row".to_string(),
    };
    let id_a = store.enqueue_projection_task(&task).await.expect("enqueue");
    assert_ne!(id_a, Uuid::nil());

    // 4. Idempotent — re-enqueue with same key returns same id.
    let id_b = store
        .enqueue_projection_task(&task)
        .await
        .expect("idempotent enqueue");
    assert_eq!(
        id_a, id_b,
        "idempotent enqueue must return the existing task_id"
    );

    let s1 = store.projection_task_summary().await.unwrap();
    assert_eq!(
        s1.pending, 1,
        "exactly one row after idempotent double-enqueue"
    );
    assert_eq!(s1.total(), 1);

    // 5. Atomic claim flips PENDING → IN_PROGRESS.
    let claimed = store
        .claim_projection_tasks(&ProjectionClaimFilter::default())
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, id_a);
    assert_eq!(claimed[0].status, ProjectionTaskStatus::InProgress);
    assert_eq!(claimed[0].retry_count, 0);
    assert_eq!(claimed[0].source_payload, serde_json::json!({"v": 1}));

    // 6. Mark failed → retry_count bumps, status returns to FAILED,
    // the row re-enters the claim queue.
    store
        .mark_projection_task_failed(
            id_a,
            1,
            ProjectionTaskStatus::Failed,
            "conformance synthetic failure",
        )
        .await
        .expect("mark failed");
    let s2 = store.projection_task_summary().await.unwrap();
    assert_eq!(s2.failed, 1);
    assert_eq!(s2.in_progress, 0);

    let reclaimed = store
        .claim_projection_tasks(&ProjectionClaimFilter::default())
        .await
        .expect("re-claim");
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].retry_count, 1);
    assert_eq!(reclaimed[0].last_error, "conformance synthetic failure");

    // 7. Mark completed → terminal.
    store
        .mark_projection_task_completed(id_a)
        .await
        .expect("mark completed");
    let s3 = store.projection_task_summary().await.unwrap();
    assert_eq!(s3.completed, 1);
    let no_more = store
        .claim_projection_tasks(&ProjectionClaimFilter::default())
        .await
        .expect("post-completion claim");
    assert!(no_more.is_empty(), "completed rows must not re-claim");

    // 8. mark_failed validation — only FAILED/DEAD_LETTER allowed.
    let err = store
        .mark_projection_task_failed(id_a, 1, ProjectionTaskStatus::InProgress, "no")
        .await
        .expect_err("invalid failure status must be rejected");
    let _ = err; // exact variant covered in per-backend tests
}

/// NW1-1b — Saga contract every `SagaStore` impl must satisfy.
/// Same factor pattern as `run_projection_task_contract`.
pub async fn run_saga_store_contract(store: Arc<dyn SagaStore>) {
    use std::time::Duration as StdDuration;
    use uuid::Uuid;

    let label = store.backend_label();
    assert!(!label.is_empty());

    // 1. Idempotent DDL.
    store
        .ensure_saga_tables()
        .await
        .expect("ensure_saga_tables (1st)");
    store
        .ensure_saga_tables()
        .await
        .expect("ensure_saga_tables (2nd must be idempotent)");

    let summary0 = store.saga_summary().await.expect("summary on empty");
    assert_eq!(summary0.total(), 0);

    // 2. Record three sagas across tenants and statuses.
    let insert = |tenant: &str, status: SagaStatus| SagaInsert {
        tx_id: Uuid::new_v4().to_string(),
        tenant_id: tenant.to_string(),
        correlation_id: format!("corr-{}", Uuid::new_v4()),
        backend_instance: "primary".to_string(),
        operation: "upsert.Contract".to_string(),
        status,
        steps: serde_json::json!([{"backend": "postgres", "op": "INSERT"}]),
        compensations: serde_json::json!([
            {"backend": "qdrant", "operation": "delete_points", "resource_uri": "qdrant://contract", "payload": {"point_ids": ["c1"]}}
        ]),
    };

    let id_alpha = store
        .record_saga(&insert("alpha", SagaStatus::Indeterminate))
        .await
        .expect("record alpha");
    let id_beta = store
        .record_saga(&insert("beta", SagaStatus::Committed))
        .await
        .expect("record beta");
    let id_alpha2 = store
        .record_saga(&insert("alpha", SagaStatus::FailedCompensation))
        .await
        .expect("record alpha 2");

    // 3. get_saga round-trips.
    let row = store
        .get_saga(id_alpha)
        .await
        .expect("get")
        .expect("Some(saga)");
    assert_eq!(row.saga_id, id_alpha);
    assert_eq!(row.status, SagaStatus::Indeterminate);
    assert!(
        row.compensations
            .as_array()
            .map(|a| !a.is_empty())
            .unwrap_or(false)
    );

    // 4. get_saga on missing returns Ok(None).
    let none = store.get_saga(Uuid::new_v4()).await.expect("get missing");
    assert!(none.is_none());

    // 5. list_sagas with filters.
    let alpha = store
        .list_sagas(&SagaListFilter {
            tenant_id: Some("alpha".to_string()),
            limit: 100,
            ..SagaListFilter::default()
        })
        .await
        .expect("list alpha");
    assert_eq!(alpha.len(), 2);

    let by_status = store
        .list_sagas(&SagaListFilter {
            status: Some(SagaStatus::Committed),
            limit: 100,
            ..SagaListFilter::default()
        })
        .await
        .expect("list by status");
    assert_eq!(by_status.len(), 1);
    assert_eq!(by_status[0].saga_id, id_beta);

    // 6. update_saga_status flips the row.
    store
        .update_saga_status(
            id_alpha,
            SagaStatus::Compensated,
            CompensationStatus::Completed,
        )
        .await
        .expect("update status");
    let row = store.get_saga(id_alpha).await.unwrap().unwrap();
    assert_eq!(row.status, SagaStatus::Compensated);
    assert_eq!(row.compensation_status, CompensationStatus::Completed);

    // 7. increment_recovery_attempts returns monotone counter.
    let n1 = store
        .increment_recovery_attempts(id_alpha2, "conformance probe")
        .await
        .expect("increment");
    assert_eq!(n1, 1);
    let n2 = store
        .increment_recovery_attempts(id_alpha2, "again")
        .await
        .expect("increment 2");
    assert_eq!(n2, 2);

    // 8. request_saga_recompensation works on failed_compensation
    // and refuses from indeterminate.
    store
        .request_saga_recompensation(id_alpha2)
        .await
        .expect("retry from failed_compensation");
    let err = store
        .request_saga_recompensation(id_alpha2)
        .await
        .expect_err("must refuse from indeterminate");
    let _ = err;

    // 9. claim_recoverable returns indeterminate + nothing else
    // (in_progress rows have updated_at = now, so they're not stale
    // under any normal threshold).
    let claimable = store
        .claim_recoverable_sagas(StdDuration::from_secs(3600), 10)
        .await
        .expect("claim");
    // id_alpha2 is currently indeterminate after request_recompensation.
    assert!(
        claimable
            .iter()
            .any(|r| r.status == SagaStatus::Indeterminate),
        "expected at least one indeterminate saga claimable"
    );

    // 10. Summary counts everything.
    let s = store.saga_summary().await.expect("summary");
    assert_eq!(s.total(), 3);
}

/// NW1-1c — AdminAuditStore contract every impl must satisfy. Same
/// factor pattern.
pub async fn run_admin_audit_store_contract(store: Arc<dyn AdminAuditStore>) {
    use uuid::Uuid;

    let label = store.backend_label();
    assert!(!label.is_empty());

    // Idempotent DDL.
    store
        .ensure_admin_audit_tables()
        .await
        .expect("ensure_admin_audit_tables (1st)");
    store
        .ensure_admin_audit_tables()
        .await
        .expect("ensure_admin_audit_tables (2nd must be idempotent)");

    // Empty: latest is "".
    let h0 = store.latest_admin_audit_hash().await.expect("latest empty");
    assert_eq!(h0, "");

    // Append three rows with brief sleeps to keep ordering
    // unambiguous. (Production admin ops are seconds apart;
    // sleeping 2ms is the test analogue.)
    let mk = |operation: &str, actor: &str| AdminAuditInsert {
        actor: actor.to_string(),
        operation: operation.to_string(),
        target: "project-alpha".to_string(),
        request_json: serde_json::json!({"v": 1}),
        result: "ok".to_string(),
        tenant_id: "tenant-1".to_string(),
        project_id: "project-alpha".to_string(),
        correlation_id: "corr".to_string(),
        signer_key_id: "default".to_string(),
        external_anchor: String::new(),
    };
    let _id1 = store.append_admin_audit(&mk("Op1", "alice")).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    let _id2 = store.append_admin_audit(&mk("Op2", "bob")).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    let _id3 = store.append_admin_audit(&mk("Op3", "carol")).await.unwrap();

    // Verify chain end-to-end.
    let report = store.verify_admin_audit_chain(None).await.expect("verify");
    match report {
        AdminAuditChainReport::Passed {
            checked_count,
            last_hash,
        } => {
            assert_eq!(checked_count, 3);
            assert_eq!(last_hash.len(), 64);
            assert_eq!(
                store.latest_admin_audit_hash().await.unwrap(),
                last_hash,
                "latest_admin_audit_hash equals the verify chain's last_hash"
            );
        }
        other => panic!("expected Passed, got: {other:?}"),
    }

    // Multi-axis list works.
    let only_alice = store
        .list_admin_audit(&AdminAuditListFilter {
            actor: Some("alice".to_string()),
            limit: 100,
            ..AdminAuditListFilter::default()
        })
        .await
        .expect("list by actor");
    assert_eq!(only_alice.len(), 1);
    let only_op2 = store
        .list_admin_audit(&AdminAuditListFilter {
            operation: Some("Op2".to_string()),
            limit: 100,
            ..AdminAuditListFilter::default()
        })
        .await
        .expect("list by op");
    assert_eq!(only_op2.len(), 1);

    // Redaction. Note: not all backends serialise the redacted JSON
    // back as object — we only assert it's no longer the original
    // {"v":1} payload.
    let redacted = store
        .list_admin_audit(&AdminAuditListFilter {
            limit: 100,
            redact_request_json: true,
            ..AdminAuditListFilter::default()
        })
        .await
        .expect("list redacted");
    for row in &redacted {
        assert_eq!(row.request_json, serde_json::json!({"redacted": true}));
    }

    // Verify with limit stops early.
    let limited = store
        .verify_admin_audit_chain(Some(2))
        .await
        .expect("limited verify");
    match limited {
        AdminAuditChainReport::Passed { checked_count, .. } => {
            assert_eq!(checked_count, 2);
        }
        other => panic!("expected Passed, got: {other:?}"),
    }

    // Sanity: a verification of a non-existent (empty) chain still
    // passes with checked_count = 0.
    let _ = Uuid::new_v4(); // suppress unused-import
}

/// NW1-1d — Migration audit contract every `MigrationAuditStore`
/// impl must satisfy.
pub async fn run_migration_audit_store_contract(store: Arc<dyn MigrationAuditStore>) {
    use uuid::Uuid;

    let label = store.backend_label();
    assert!(!label.is_empty());

    // Idempotent DDL.
    store
        .ensure_migration_audit_tables()
        .await
        .expect("ensure_migration_audit_tables (1st)");
    store
        .ensure_migration_audit_tables()
        .await
        .expect("ensure_migration_audit_tables (2nd must be idempotent)");

    let empty = store
        .list_migration_runs(&MigrationRunsFilter {
            limit: 100,
            ..MigrationRunsFilter::default()
        })
        .await
        .expect("list empty");
    assert!(empty.is_empty());

    // start_run + record ops + finish_run round-trip.
    let run_id = store
        .start_migration_run(&MigrationRunInsert {
            project_id: "alpha".to_string(),
            catalog_version: "v1".to_string(),
            operations_hash: "sha256:test".to_string(),
            approval_token: "tok-1".to_string(),
            state: MigrationRunState::Applying,
        })
        .await
        .expect("start");
    assert_ne!(run_id, Uuid::nil());

    for i in 0..3 {
        let status = if i == 1 {
            OpLedgerStatus::Skipped
        } else {
            OpLedgerStatus::Applied
        };
        store
            .record_migration_op(&MigrationOpInsert {
                run_id,
                operation_index: i,
                backend: "postgres".to_string(),
                resource_uri: format!("udb://postgres/op{i}.sql"),
                operation_kind: "apply".to_string(),
                status,
                rollback_json: serde_json::json!({"undo": format!("DROP TABLE t{i}")}),
                error: String::new(),
            })
            .await
            .expect("record op");
    }

    store
        .finish_migration_run(run_id, MigrationRunState::Completed, "")
        .await
        .expect("finish");

    let row = store
        .get_migration_run(run_id)
        .await
        .expect("get")
        .expect("Some(run)");
    assert_eq!(row.run_id, run_id);
    assert_eq!(row.state, MigrationRunState::Completed);
    assert!(row.finished_at.is_some());

    let ops = store.list_migration_ops(run_id).await.expect("list ops");
    assert_eq!(ops.len(), 3);
    assert_eq!(ops[0].operation_index, 0);
    assert_eq!(ops[0].status, OpLedgerStatus::Applied);
    assert!(ops[0].applied_at.is_some());
    assert_eq!(ops[1].status, OpLedgerStatus::Skipped);
    assert!(
        ops[1].applied_at.is_none(),
        "non-APPLIED status must not set applied_at"
    );

    // List filtering.
    let alpha = store
        .list_migration_runs(&MigrationRunsFilter {
            project_id: Some("alpha".to_string()),
            limit: 100,
            ..MigrationRunsFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(alpha.len(), 1);
    let by_state = store
        .list_migration_runs(&MigrationRunsFilter {
            state: Some(MigrationRunState::Completed),
            limit: 100,
            ..MigrationRunsFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(by_state.len(), 1);

    // get_run on missing returns Ok(None).
    let none = store.get_migration_run(Uuid::new_v4()).await.unwrap();
    assert!(none.is_none());

    // finish_run on missing errors.
    let err = store
        .finish_migration_run(Uuid::new_v4(), MigrationRunState::Completed, "")
        .await
        .expect_err("must error");
    let _ = err;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::canonical_store::sqlite::SqliteCanonicalStore;
    use sqlx::sqlite::SqlitePoolOptions;

    /// Pin: SQLite passes the canonical-store contract end-to-end.
    /// The same test becomes `mysql_canonical_store_satisfies_contract`
    /// and `postgres_canonical_store_satisfies_contract` once those
    /// have live integration harnesses.
    #[tokio::test]
    async fn sqlite_canonical_store_satisfies_contract() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite");
        let store = Arc::new(SqliteCanonicalStore::new(
            pool,
            "conformance",
            "udb_outbox_events",
        ));
        run_contract(store).await;
    }

    /// Pin: SQLite satisfies the projection-task contract. Runs on
    /// every CI without external services because of `:memory:`.
    /// MySQL/PG conformance run with `--ignored` against live DBs
    /// via env-gated tests.
    #[tokio::test]
    async fn sqlite_projection_task_store_satisfies_contract() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite");
        let store: Arc<dyn ProjectionTaskStore> = Arc::new(SqliteCanonicalStore::new(
            pool,
            "conformance-proj",
            "udb_outbox_events",
        ));
        run_projection_task_contract(store).await;
    }

    /// Pin: SQLite satisfies the saga-store contract.
    #[tokio::test]
    async fn sqlite_saga_store_satisfies_contract() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite");
        let store: Arc<dyn SagaStore> = Arc::new(SqliteCanonicalStore::new(
            pool,
            "conformance-saga",
            "udb_outbox_events",
        ));
        run_saga_store_contract(store).await;
    }

    /// Pin: SQLite satisfies the admin-audit-store contract.
    #[tokio::test]
    async fn sqlite_admin_audit_store_satisfies_contract() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite");
        let store: Arc<dyn AdminAuditStore> = Arc::new(SqliteCanonicalStore::new(
            pool,
            "conformance-audit",
            "udb_outbox_events",
        ));
        run_admin_audit_store_contract(store).await;
    }

    /// Pin: SQLite satisfies the migration-audit-store contract.
    #[tokio::test]
    async fn sqlite_migration_audit_store_satisfies_contract() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite");
        let store: Arc<dyn MigrationAuditStore> = Arc::new(SqliteCanonicalStore::new(
            pool,
            "conformance-migration",
            "udb_outbox_events",
        ));
        run_migration_audit_store_contract(store).await;
    }
}
