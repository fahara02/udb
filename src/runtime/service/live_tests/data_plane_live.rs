//! Env-gated LIVE served-path tests for this release's DATA-PLANE fixes.
//!
//! Every test here drives the **real DataBroker gRPC handlers**
//! (`DataBrokerService::{upsert, delete, update, select}`, the exact methods the
//! tonic server dispatches) against a **live Postgres**, exactly the way a
//! customer SDK hit each reported bug. They are NOT unit tests of internal
//! helpers — the request enters through `security_from_request` + `authorize`
//! (deny-by-default Casbin gate, opened here with the same `abac_default_allow`
//! dev escape hatch the in-tree `open_service` test harness uses) and flows all
//! the way to committed SQL + the durable system catalog.
//!
//! ## Isolation
//! Each test provisions a throwaway per-run SCHEMA (`udb_dp_<uuid>`) for its user
//! table and points a hand-built `CatalogManifest` at it. The broker's own system
//! catalog (`udb_idempotency_keys`, `udb_row_revisions`, projection/outbox tables)
//! is bootstrapped idempotently via [`ensure_system_catalog`]; every test uses a
//! fresh UUID tenant, so the salted per-(tenant,project,type,row) dedup/revision
//! keys never collide across runs. The user schema is dropped CASCADE and the
//! tenant's system rows are best-effort deleted at the end.
//!
//! ## Env gating
//! Skipped (runtime `return`) unless one of `UDB_LIVE_NATIVE_PG_DSN` /
//! `UDB_LIVE_AUTH_PG_DSN` / `UDB_INTEGRATION_PG_DSN` / `UDB_PG_DSN` is set, so the
//! default `cargo test --lib` stays green with zero external dependencies while CI
//! / docker (which export the DSN) RUNS them. Mirrors the sibling native
//! `live_tests` modules and `canonical_store::conformance_live_tests`.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde_json::json;
use sqlx::Row;
use tonic::{Code, Request};
use uuid::Uuid;

use crate::engine::FsmState;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable, ManifestTableSecurity};
use crate::metrics::{MetricsRecorder, PrometheusMetrics};
use crate::proto::data_broker_client::DataBrokerClient;
use crate::proto::data_broker_server::{DataBroker, DataBrokerServer};
use crate::proto::udb::core::lock::services::v1 as lock_pb;
use crate::proto::udb::core::lock::services::v1::lock_service_server::LockService;
use crate::proto::{
    BulkCasItem, BulkCasRequest, DeleteRequest, Mutation, SelectRequest, UpdateRequest,
    UpsertRequest, tx_status,
};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::config::UdbConfig;
use crate::runtime::executor_utils::json_to_struct;
use crate::runtime::security::SecurityConfig;
use crate::runtime::service::DataBrokerService;
use crate::runtime::system::{SystemCatalogConfig, ensure_system_catalog};

// ── env-gated DSN resolution ──────────────────────────────────────────────────

fn dp_live_pg_dsn() -> Option<String> {
    for key in [
        "UDB_LIVE_NATIVE_PG_DSN",
        "UDB_LIVE_AUTH_PG_DSN",
        "UDB_INTEGRATION_PG_DSN",
        "UDB_PG_DSN",
    ] {
        if let Ok(value) = std::env::var(key)
            && !value.trim().is_empty()
        {
            return Some(value);
        }
    }
    None
}

async fn dp_pool(dsn: &str) -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(6)
        .acquire_timeout(Duration::from_secs(10))
        .connect(dsn)
        .await
        .unwrap_or_else(|err| panic!("connect live data-plane postgres at {dsn}: {err}"))
}

/// Install a dev security profile so `security_from_request` takes the header
/// credential path (no JWT/mTLS). Same fields the in-tree `install_test_security`
/// helper sets for the unit served-path tests.
fn install_dp_security() {
    SecurityConfig::install_global(SecurityConfig {
        tls_required: false,
        service_identity_required: false,
        mtls_required: false,
        allow_header_scopes: true,
        ..SecurityConfig::default()
    });
}

/// Build a live served DataBrokerService: a real PG-backed runtime pointed at
/// `dsn` (so the served mutations commit to the same database the test seeded),
/// lifecycle=Completed, and `abac_default_allow=true` so a tenant+purpose request
/// clears the deny-by-default Casbin gate (mirrors the `open_service` harness).
async fn dp_service(dsn: &str, manifest: CatalogManifest) -> DataBrokerService {
    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = dsn.to_string();
    let runtime = DataBrokerRuntime::from_config(config).await;
    let lifecycle = Arc::new(RwLock::new(FsmState::Completed));
    let metrics: Arc<dyn MetricsRecorder> = Arc::new(PrometheusMetrics::new().expect("metrics"));
    DataBrokerService::with_runtime_and_state(manifest, runtime, lifecycle, metrics, None, true)
}

/// Attach the caller identity a client sends as gRPC metadata: tenant + purpose
/// (both required by `authorize`) and a scope. The header path resolves these
/// into the `SecurityContext` / `RequestContext` the handler runs under.
fn with_ctx<T>(message: T, tenant: &str) -> Request<T> {
    let mut req = Request::new(message);
    let md = req.metadata_mut();
    md.insert("x-tenant-id", tenant.parse().unwrap());
    md.insert("x-purpose", "admin".parse().unwrap());
    md.insert("x-scopes", "udb:admin".parse().unwrap());
    req
}

fn col(name: &str, sql_type: &str, is_primary: bool) -> ManifestColumn {
    ManifestColumn {
        field_name: name.to_string(),
        column_name: name.to_string(),
        proto_type: "string".to_string(),
        sql_type: sql_type.to_string(),
        is_primary,
        not_null: name == "tenant_id",
        ..ManifestColumn::default()
    }
}

/// One-table manifest whose FQN is `acme.dp.v1.<message>` and whose physical
/// relation is `<schema>.<table>`, tenant-scoped on `tenant_id` (so the planner's
/// tenant-isolation gate is satisfied by a `tenant_id` filter). When `soft_delete`
/// is set, adds a `deleted_at` tombstone column and marks the table soft-delete.
fn widget_manifest(
    schema: &str,
    table: &str,
    message: &str,
    soft_delete: bool,
) -> CatalogManifest {
    let mut columns = vec![
        col("id", "TEXT", true),
        col("tenant_id", "TEXT", false),
        col("status", "TEXT", false),
    ];
    let mut manifest_table = ManifestTable {
        proto_package: "acme.dp.v1".to_string(),
        message_name: message.to_string(),
        schema: schema.to_string(),
        table: table.to_string(),
        primary_key: vec!["id".to_string()],
        table_security: ManifestTableSecurity {
            tenant_column: "tenant_id".to_string(),
            ..ManifestTableSecurity::default()
        },
        ..ManifestTable::default()
    };
    if soft_delete {
        columns.push(col("deleted_at", "TIMESTAMPTZ", false));
        manifest_table.soft_delete = true;
        manifest_table.soft_delete_column = "deleted_at".to_string();
    }
    manifest_table.columns = columns;
    CatalogManifest {
        tables: vec![manifest_table],
        ..CatalogManifest::default()
    }
}

async fn create_schema(pool: &sqlx::PgPool, schema: &str) {
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(pool)
        .await
        .expect("create throwaway data-plane schema");
}

async fn teardown(pool: &sqlx::PgPool, schema: &str, tenant: &str) {
    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(pool)
        .await;
    // Best-effort: remove this run's rows from the SHARED system catalog so the
    // dedup / revision tables don't accumulate across CI runs (keys are salted per
    // tenant, so this is scoped to exactly this test).
    let config = SystemCatalogConfig::current();
    for relation in [
        config.idempotency_keys_relation(),
        config.row_revisions_relation(),
    ] {
        let _ = sqlx::query(&format!("DELETE FROM {relation} WHERE tenant_id = $1"))
            .bind(tenant)
            .execute(pool)
            .await;
    }
}

// Response/record helpers -------------------------------------------------------

fn record_status(bytes: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    value.get("status")?.as_str().map(str::to_string)
}

fn record_id(bytes: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    value.get("id")?.as_str().map(str::to_string)
}

fn rev_int(token: &str) -> i64 {
    token
        .parse()
        .unwrap_or_else(|_| panic!("revision token must be numeric, got {token:?}"))
}

// Served-RPC convenience wrappers (build the request, attach ctx, drive handler).

async fn served_upsert(
    svc: &DataBrokerService,
    tenant: &str,
    message: &str,
    record: serde_json::Value,
    idempotency_key: &str,
) -> Result<crate::proto::MutationResponse, tonic::Status> {
    let request = UpsertRequest {
        message_type: message.to_string(),
        record_json: serde_json::to_vec(&record).unwrap(),
        idempotency_key: idempotency_key.to_string(),
        ..UpsertRequest::default()
    };
    svc.upsert(with_ctx(request, tenant))
        .await
        .map(tonic::Response::into_inner)
}

async fn served_select_rows(
    svc: &DataBrokerService,
    tenant: &str,
    message: &str,
    filter: serde_json::Value,
    include_revision: bool,
) -> crate::proto::RecordSet {
    let request = SelectRequest {
        message_type: message.to_string(),
        filter: json_to_struct(&filter),
        include_revision,
        ..SelectRequest::default()
    };
    svc.select(with_ctx(request, tenant))
        .await
        .expect("served Select must succeed")
        .into_inner()
}

// ── #6 (CRITICAL) idempotency-input-mismatch, threading #1 replay ordering ─────

/// A keyed mutation that reuses an idempotency key with DIFFERENT authoritative
/// inputs MUST be refused `FAILED_PRECONDITION` — never replayed as a bogus
/// success for an operation that never ran — and its target MUST NOT be applied.
/// Same key + IDENTICAL inputs still replays the stored response (applied once).
///
/// Revert-proof: the fix stores a canonical `request_hash` on the winning dedup
/// claim and, on a non-fresh claim, refuses when the stored hash differs from the
/// current request's (`idempotency_request_mismatch_status`). Revert that compare
/// (or the stored hash) and the second, DIFFERENT request falls through to
/// `mutation_response_from_idempotency_json_for_claim` → returns `Ok(success)`,
/// so the `FAILED_PRECONDITION` assertions below fail.
#[tokio::test]
async fn served_idempotency_key_reuse_with_different_input_is_refused_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping idempotency-input-mismatch (#6)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create widgets");
    const MSG: &str = "acme.dp.v1.Widget";
    let svc = dp_service(&dsn, widget_manifest(&schema, "widgets", "Widget", false)).await;

    // Loop the whole sequence: the bug is a keyed-request sequence, and looping
    // proves the guard holds per distinct key, not by accident on the first.
    for iteration in 0..3 {
        let key = format!("idem-mismatch-{iteration}-{}", Uuid::new_v4().simple());
        let id_a = format!("w-{iteration}-a-{}", Uuid::new_v4().simple());
        let id_b = format!("w-{iteration}-b-{}", Uuid::new_v4().simple());

        // (a) First keyed upsert establishes the claim (record A, status "A").
        let first = served_upsert(
            &svc,
            &tenant,
            MSG,
            json!({"id": id_a, "tenant_id": tenant, "status": "A"}),
            &key,
        )
        .await
        .expect("first keyed upsert succeeds");
        assert_eq!(first.affected_rows, 1, "first upsert writes exactly one row");

        // (b1) SAME key, DIFFERENT record targeting a DIFFERENT primary key (id_b).
        // Reverting the request-hash guard replays (a)'s stored success — a bogus
        // success claiming a write that never touched id_b.
        let mismatch = served_upsert(
            &svc,
            &tenant,
            MSG,
            json!({"id": id_b, "tenant_id": tenant, "status": "Z"}),
            &key,
        )
        .await
        .expect_err("key reused with a DIFFERENT record must be refused, not replayed");
        assert_eq!(
            mismatch.code(),
            Code::FailedPrecondition,
            "input-mismatch replay must be FAILED_PRECONDITION, got {mismatch:?}"
        );
        // The second request's target must NOT be applied (follow-up read of id_b).
        let id_b_rows = served_select_rows(
            &svc,
            &tenant,
            MSG,
            json!({"id": id_b, "tenant_id": tenant}),
            false,
        )
        .await;
        assert!(
            id_b_rows.records_json.is_empty(),
            "the mismatched idempotent request must not have written id_b"
        );

        // (b2) SAME key, SAME target id_a but DIFFERENT assignment (status "B").
        let mismatch2 = served_upsert(
            &svc,
            &tenant,
            MSG,
            json!({"id": id_a, "tenant_id": tenant, "status": "B"}),
            &key,
        )
        .await
        .expect_err("key reused with a DIFFERENT assignment must be refused");
        assert_eq!(mismatch2.code(), Code::FailedPrecondition);
        // And id_a still carries the FIRST writer's value — no bogus overwrite.
        let id_a_rows = served_select_rows(
            &svc,
            &tenant,
            MSG,
            json!({"id": id_a, "tenant_id": tenant}),
            false,
        )
        .await;
        assert_eq!(id_a_rows.records_json.len(), 1);
        assert_eq!(
            record_status(&id_a_rows.records_json[0]).as_deref(),
            Some("A"),
            "the mismatched request must not have mutated id_a"
        );

        // (c) SAME key, IDENTICAL inputs → replays the stored response (once).
        let replay = served_upsert(
            &svc,
            &tenant,
            MSG,
            json!({"id": id_a, "tenant_id": tenant, "status": "A"}),
            &key,
        )
        .await
        .expect("identical keyed retry replays the stored success");
        assert_eq!(
            replay.mutation_id, first.mutation_id,
            "identical retry returns the ORIGINAL stored response (replayed, not re-run)"
        );
        assert_eq!(replay.affected_rows, first.affected_rows);
    }

    teardown(&pool, &schema, &tenant).await;
}

// ── #1 delete-cas-replay-order ────────────────────────────────────────────────

/// A keyed conditional Delete whose response was lost is retried after the row is
/// already gone; it MUST replay the stored success, NOT raise a spurious
/// `FAILED_PRECONDITION` because the (now-absent) row "changed".
///
/// Revert-proof: the fix moves the durable idempotency CLAIM ahead of the CAS
/// precondition. On the retry the claim is non-fresh (same key + identical
/// filter+expected ⇒ identical `request_hash`) and returns the stored response
/// BEFORE the CAS runs. Revert the ordering (CAS before claim) and the retry
/// evaluates `expected` against the deleted row → `FAILED_PRECONDITION`, so the
/// `Ok(replay)` assertion below fails.
#[tokio::test]
async fn served_keyed_conditional_delete_replays_after_row_gone_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping delete-cas-replay-order (#1)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create widgets");
    const MSG: &str = "acme.dp.v1.Widget";
    let svc = dp_service(&dsn, widget_manifest(&schema, "widgets", "Widget", false)).await;

    for iteration in 0..3 {
        let id = format!("w-{iteration}-{}", Uuid::new_v4().simple());
        let key = format!("del-replay-{iteration}-{}", Uuid::new_v4().simple());

        // Seed a row, then conditionally delete it (CAS: status still "A").
        served_upsert(
            &svc,
            &tenant,
            MSG,
            json!({"id": id, "tenant_id": tenant, "status": "A"}),
            "",
        )
        .await
        .expect("seed row");

        let delete_request = |key: &str| DeleteRequest {
            message_type: MSG.to_string(),
            filter: json_to_struct(&json!({"id": id, "tenant_id": tenant})),
            idempotency_key: key.to_string(),
            expected: json_to_struct(&json!({"status": "A"})),
            ..DeleteRequest::default()
        };

        let first = svc
            .delete(with_ctx(delete_request(&key), &tenant))
            .await
            .expect("first conditional delete succeeds")
            .into_inner();
        assert_eq!(first.affected_rows, 1, "first delete removes the row");

        // Row is gone.
        let gone = served_select_rows(
            &svc,
            &tenant,
            MSG,
            json!({"id": id, "tenant_id": tenant}),
            false,
        )
        .await;
        assert!(gone.records_json.is_empty(), "row deleted");

        // Simulated response-loss retry: SAME key, SAME filter+expected, row gone.
        let retry = svc
            .delete(with_ctx(delete_request(&key), &tenant))
            .await
            .expect("retry after row gone must REPLAY the stored success, not FAILED_PRECONDITION")
            .into_inner();
        assert_eq!(
            retry.mutation_id, first.mutation_id,
            "the retry returns the ORIGINAL stored delete response"
        );
        assert_eq!(
            retry.affected_rows, first.affected_rows,
            "the replayed delete reports the original affected-row count"
        );
    }

    teardown(&pool, &schema, &tenant).await;
}

// ── #3 soft-delete-is-hard-delete + #4 soft-delete read scope ─────────────────

/// On a `soft_delete`-annotated entity a served Delete MUST tombstone (physical
/// row stays, `deleted_at` set) — never physically remove — and ordinary served
/// reads (list, get-by-id) MUST NOT return the tombstoned row, while a privileged
/// raw read still sees it. Covers the read cardinality / count path too.
///
/// Revert-proof: `build_delete_plan` emits `UPDATE ... SET deleted_at = NOW()` for
/// soft-delete tables; revert it to a physical `DELETE` and the raw
/// `deleted_at IS NOT NULL` assertion fails (row gone). `build_select_query_plan`
/// injects `deleted_at IS NULL`; revert that and the tombstoned row leaks into the
/// served list/get, failing the exclusion + count assertions.
#[tokio::test]
async fn served_soft_delete_tombstones_and_reads_exclude_it_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping soft-delete (#3)/read-scope (#4)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".sd_widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT, deleted_at TIMESTAMPTZ)"
    ))
    .execute(&pool)
    .await
    .expect("create sd_widgets");
    const MSG: &str = "acme.dp.v1.SoftWidget";
    let svc = dp_service(&dsn, widget_manifest(&schema, "sd_widgets", "SoftWidget", true)).await;

    let id_kept = format!("keep-{}", Uuid::new_v4().simple());
    let id_deleted = format!("del-{}", Uuid::new_v4().simple());
    for id in [&id_kept, &id_deleted] {
        served_upsert(
            &svc,
            &tenant,
            MSG,
            json!({"id": id, "tenant_id": tenant, "status": "LIVE"}),
            "",
        )
        .await
        .expect("seed soft-delete row");
    }

    // Served Delete of one row → tombstone, not physical removal.
    let deleted = svc
        .delete(with_ctx(
            DeleteRequest {
                message_type: MSG.to_string(),
                filter: json_to_struct(&json!({"id": id_deleted, "tenant_id": tenant})),
                ..DeleteRequest::default()
            },
            &tenant,
        ))
        .await
        .expect("served delete on soft-delete table")
        .into_inner();
    assert_eq!(deleted.affected_rows, 1, "one row tombstoned");

    // Privileged raw read (bypasses the served read scope): BOTH rows physically
    // present; the deleted one carries a non-null tombstone, the kept one null.
    let raw = sqlx::query(&format!(
        "SELECT id, deleted_at FROM \"{schema}\".sd_widgets WHERE tenant_id = $1 ORDER BY id"
    ))
    .bind(&tenant)
    .fetch_all(&pool)
    .await
    .expect("raw privileged read");
    assert_eq!(
        raw.len(),
        2,
        "soft delete must keep BOTH rows physically (not a hard delete)"
    );
    for row in &raw {
        let id: String = row.get("id");
        let deleted_at: Option<chrono::DateTime<chrono::Utc>> = row.get("deleted_at");
        if id == id_deleted {
            assert!(
                deleted_at.is_some(),
                "the deleted row must survive physically with its tombstone column set"
            );
        } else {
            assert!(deleted_at.is_none(), "the kept row must not be tombstoned");
        }
    }
    // Raw COUNT confirms the aggregate over the PHYSICAL table is 2.
    let raw_count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM \"{schema}\".sd_widgets WHERE tenant_id = $1"
    ))
    .bind(&tenant)
    .fetch_one(&pool)
    .await
    .expect("raw count");
    assert_eq!(raw_count, 2);

    // Served list read: only the live row (tombstone excluded). This is also the
    // served count/aggregate-over-read path — it returns 1, not 2.
    let listed = served_select_rows(&svc, &tenant, MSG, json!({"tenant_id": tenant}), false).await;
    assert_eq!(
        listed.records_json.len(),
        1,
        "an ordinary served read must exclude the tombstoned row (count reflects live rows only)"
    );
    assert_eq!(
        record_id(&listed.records_json[0]).as_deref(),
        Some(id_kept.as_str()),
        "the only visible row is the non-deleted one"
    );

    // Served get-by-id of the tombstoned row: zero rows (a direct id lookup must
    // not surface a soft-deleted row either).
    let get_deleted = served_select_rows(
        &svc,
        &tenant,
        MSG,
        json!({"id": id_deleted, "tenant_id": tenant}),
        false,
    )
    .await;
    assert!(
        get_deleted.records_json.is_empty(),
        "a get-by-id on the tombstoned row must return nothing on the served read path"
    );

    teardown(&pool, &schema, &tenant).await;
}

// ── #5 opaque row-revision CAS (+ ABA) ────────────────────────────────────────

/// A served read (`include_revision`) returns an opaque revision per row; a served
/// Update with a STALE `expected_revision` is `FAILED_PRECONDITION` (unchanged),
/// with the CURRENT one succeeds and ADVANCES the revision; and two concurrent
/// CAS updaters cannot both win (ABA-safe — the loser conflicts).
///
/// Revert-proof: the fix maintains a broker-side `udb_row_revisions` map, bumps it
/// in the write tx of every single-row mutation, surfaces it on reads, and gates
/// Update/Delete on `enforce_expected_revision_in_tx` (a `SELECT ... FOR UPDATE`
/// compare). Revert the read-side and `record_revisions` comes back empty; revert
/// the enforce and the STALE update SUCCEEDS and both concurrent updaters win —
/// each of which fails an assertion below.
#[tokio::test]
async fn served_row_revision_cas_gates_updates_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping row-revision CAS (#5)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create widgets");
    const MSG: &str = "acme.dp.v1.Widget";
    let svc = Arc::new(dp_service(&dsn, widget_manifest(&schema, "widgets", "Widget", false)).await);

    let id = format!("rev-{}", Uuid::new_v4().simple());

    // Upsert → the response carries the first opaque revision.
    let seeded = served_upsert(
        &svc,
        &tenant,
        MSG,
        json!({"id": id, "tenant_id": tenant, "status": "A"}),
        "",
    )
    .await
    .expect("seed row");
    let rev1 = seeded.revision.clone();
    assert!(
        !rev1.is_empty(),
        "an upsert must return the row's opaque revision"
    );

    // Served include_revision read returns that same opaque token, index-aligned.
    let read = served_select_rows(
        &svc,
        &tenant,
        MSG,
        json!({"id": id, "tenant_id": tenant}),
        true,
    )
    .await;
    assert_eq!(read.records_json.len(), 1);
    assert_eq!(
        read.record_revisions.len(),
        1,
        "include_revision must fill one revision slot per returned row"
    );
    assert_eq!(
        read.record_revisions[0], rev1,
        "the read-returned revision matches the write-returned revision"
    );

    let update_request = |expected_rev: &str, status: &str| UpdateRequest {
        message_type: MSG.to_string(),
        filter: json_to_struct(&json!({"id": id, "tenant_id": tenant})),
        changes: json_to_struct(&json!({"status": status})),
        expected_revision: expected_rev.to_string(),
        ..UpdateRequest::default()
    };

    // CURRENT revision → succeeds and advances the revision.
    let updated = svc
        .update(with_ctx(update_request(&rev1, "B"), &tenant))
        .await
        .expect("update with the current revision succeeds")
        .into_inner();
    let rev2 = updated.revision.clone();
    assert!(
        rev_int(&rev2) > rev_int(&rev1),
        "a successful CAS update must advance the opaque revision ({rev1} -> {rev2})"
    );

    // STALE revision (rev1 is now superseded) → FAILED_PRECONDITION, no write.
    let stale = svc
        .update(with_ctx(update_request(&rev1, "C"), &tenant))
        .await
        .expect_err("a stale expected_revision must be rejected");
    assert_eq!(stale.code(), Code::FailedPrecondition);
    let after_stale = served_select_rows(
        &svc,
        &tenant,
        MSG,
        json!({"id": id, "tenant_id": tenant}),
        false,
    )
    .await;
    assert_eq!(
        record_status(&after_stale.records_json[0]).as_deref(),
        Some("B"),
        "the stale-revision update must not have been applied"
    );

    // ABA / two concurrent updaters both asserting the CURRENT revision (rev2):
    // the row-revision FOR UPDATE lock serializes them, so exactly one wins and the
    // loser observes the bumped revision and conflicts.
    let (svc_a, svc_b) = (svc.clone(), svc.clone());
    let (tenant_a, tenant_b) = (tenant.clone(), tenant.clone());
    let (rev_a, rev_b) = (rev2.clone(), rev2.clone());
    let (id_a, id_b) = (id.clone(), id.clone());
    let racer_a = tokio::spawn(async move {
        svc_a
            .update(with_ctx(
                UpdateRequest {
                    message_type: MSG.to_string(),
                    filter: json_to_struct(&json!({"id": id_a, "tenant_id": tenant_a})),
                    changes: json_to_struct(&json!({"status": "X"})),
                    expected_revision: rev_a,
                    ..UpdateRequest::default()
                },
                &tenant_a,
            ))
            .await
    });
    let racer_b = tokio::spawn(async move {
        svc_b
            .update(with_ctx(
                UpdateRequest {
                    message_type: MSG.to_string(),
                    filter: json_to_struct(&json!({"id": id_b, "tenant_id": tenant_b})),
                    changes: json_to_struct(&json!({"status": "Y"})),
                    expected_revision: rev_b,
                    ..UpdateRequest::default()
                },
                &tenant_b,
            ))
            .await
    });
    let (res_a, res_b) = tokio::join!(racer_a, racer_b);
    let results = [res_a.expect("racer A joins"), res_b.expect("racer B joins")];
    let winners = results.iter().filter(|r| r.is_ok()).count();
    let losers = results
        .iter()
        .filter(|r| r.as_ref().err().map(tonic::Status::code) == Some(Code::FailedPrecondition))
        .count();
    assert_eq!(
        winners, 1,
        "exactly one concurrent CAS updater may win (ABA-safe)"
    );
    assert_eq!(
        losers, 1,
        "the losing concurrent CAS updater must get FAILED_PRECONDITION"
    );

    teardown(&pool, &schema, &tenant).await;
}

// ── shared helpers for the gate23 / gate25 / BeginTx served tests ──────────────

async fn served_update(
    svc: &DataBrokerService,
    tenant: &str,
    message: &str,
    filter: serde_json::Value,
    changes: serde_json::Value,
) -> Result<crate::proto::MutationResponse, tonic::Status> {
    let request = UpdateRequest {
        message_type: message.to_string(),
        filter: json_to_struct(&filter),
        changes: json_to_struct(&changes),
        ..UpdateRequest::default()
    };
    svc.update(with_ctx(request, tenant))
        .await
        .map(tonic::Response::into_inner)
}

/// The served status of a single row (or `None` when the served read returns it as
/// absent), used to assert what a mutation did/did not apply.
async fn served_status_of(
    svc: &DataBrokerService,
    tenant: &str,
    message: &str,
    id: &str,
) -> Option<String> {
    let rows = served_select_rows(svc, tenant, message, json!({"id": id, "tenant_id": tenant}), false).await;
    rows.records_json.first().and_then(|bytes| record_status(bytes))
}

/// The broker's opaque revision for a single row (via the served `include_revision`
/// read), used to prove a rejected write neither committed nor bumped the revision.
async fn served_row_revision(
    svc: &DataBrokerService,
    tenant: &str,
    message: &str,
    id: &str,
) -> Option<String> {
    let rows = served_select_rows(svc, tenant, message, json!({"id": id, "tenant_id": tenant}), true).await;
    rows.record_revisions.first().cloned()
}

async fn served_bulk_cas(
    svc: &DataBrokerService,
    tenant: &str,
    message: &str,
    items: Vec<BulkCasItem>,
    idempotency_key: &str,
    max_rows: i32,
) -> Result<crate::proto::BulkCasResponse, tonic::Status> {
    let request = BulkCasRequest {
        message_type: message.to_string(),
        items,
        idempotency_key: idempotency_key.to_string(),
        max_rows,
        ..BulkCasRequest::default()
    };
    svc.bulk_cas(with_ctx(request, tenant))
        .await
        .map(tonic::Response::into_inner)
}

/// One single-row bulk-CAS item: pin the PK by equality in `filter`, SET `changes`,
/// optionally assert a field-map `expected` and/or an `expected_revision`.
fn bulk_item(
    filter: serde_json::Value,
    changes: serde_json::Value,
    expected: Option<serde_json::Value>,
    expected_revision: &str,
) -> BulkCasItem {
    BulkCasItem {
        filter: json_to_struct(&filter),
        changes: json_to_struct(&changes),
        expected: expected.and_then(|value| json_to_struct(&value)),
        expected_revision: expected_revision.to_string(),
        ..BulkCasItem::default()
    }
}

/// Attach the verified-tenant header the native `LockService` checks against the
/// request body (`validate_request_tenant`).
fn lock_request<T>(message: T, tenant: &str) -> Request<T> {
    let mut req = Request::new(message);
    req.metadata_mut()
        .insert("x-tenant-id", tenant.parse().unwrap());
    req
}

// ── gate 23: bounded, tenant-scoped, request-hash-idempotent bulk CAS ──────────

/// Through the real `DataBroker::bulk_cas` RPC: a batch mixing revision-CAS,
/// field-CAS, stale, and missing items applies ONLY the items whose per-item
/// precondition holds, counts the rest as conflicts (never a batch error), and a
/// caller-tenant batch cannot mutate a foreign tenant's row.
///
/// Revert-proof: revert the per-item CAS (apply every matched item regardless) and
/// the field-mismatched `r2` + the stale-revision `r4` would change to "B" — the
/// `changed == 2` count and the "r2/r4 unchanged" served reads both fail.
#[tokio::test]
async fn served_bulk_cas_applies_only_passing_items_and_is_tenant_scoped_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping bulk-CAS per-item (gate 23)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    let foreign_tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create widgets");
    const MSG: &str = "acme.dp.v1.Widget";
    let svc = dp_service(&dsn, widget_manifest(&schema, "widgets", "Widget", false)).await;

    let r1 = format!("r1-{}", Uuid::new_v4().simple());
    let r2 = format!("r2-{}", Uuid::new_v4().simple());
    let r3 = format!("r3-{}", Uuid::new_v4().simple());
    let r4 = format!("r4-{}", Uuid::new_v4().simple());
    let r_foreign = format!("rf-{}", Uuid::new_v4().simple());
    let missing = format!("gone-{}", Uuid::new_v4().simple());

    let r1_seed = served_upsert(&svc, &tenant, MSG, json!({"id": r1, "tenant_id": tenant, "status": "A"}), "")
        .await
        .expect("seed r1");
    for id in [&r2, &r3] {
        served_upsert(&svc, &tenant, MSG, json!({"id": id, "tenant_id": tenant, "status": "A"}), "")
            .await
            .expect("seed row");
    }
    // r4: seed, capture its revision, then bump it so that captured token is STALE.
    served_upsert(&svc, &tenant, MSG, json!({"id": r4, "tenant_id": tenant, "status": "A"}), "")
        .await
        .expect("seed r4");
    let r4_stale_rev = served_row_revision(&svc, &tenant, MSG, &r4)
        .await
        .expect("r4 revision");
    served_update(&svc, &tenant, MSG, json!({"id": r4, "tenant_id": tenant}), json!({"status": "A4"}))
        .await
        .expect("bump r4 so its captured revision is stale");
    // A row owned by a DIFFERENT tenant.
    served_upsert(&svc, &foreign_tenant, MSG, json!({"id": r_foreign, "tenant_id": foreign_tenant, "status": "FA"}), "")
        .await
        .expect("seed foreign-tenant row");

    let r1_rev = r1_seed.revision.clone();
    assert!(!r1_rev.is_empty(), "the upsert returns r1's opaque revision");

    let items = vec![
        // r1: CURRENT revision → applies.
        bulk_item(json!({"id": r1, "tenant_id": tenant}), json!({"status": "B"}), None, &r1_rev),
        // r2: a WRONG field-CAS assertion → conflict (unchanged).
        bulk_item(json!({"id": r2, "tenant_id": tenant}), json!({"status": "B"}), Some(json!({"status": "WRONG"})), ""),
        // r3: the CURRENT field value asserted → applies.
        bulk_item(json!({"id": r3, "tenant_id": tenant}), json!({"status": "B"}), Some(json!({"status": "A"})), ""),
        // r4: a now-STALE revision → conflict (unchanged).
        bulk_item(json!({"id": r4, "tenant_id": tenant}), json!({"status": "B"}), None, &r4_stale_rev),
        // a non-existent id → conflict, not matched.
        bulk_item(json!({"id": missing, "tenant_id": tenant}), json!({"status": "B"}), None, ""),
    ];
    let resp = served_bulk_cas(&svc, &tenant, MSG, items, "", 0)
        .await
        .expect("bulk cas batch");
    assert_eq!(resp.matched, 4, "r1..r4 exist; the missing id is not matched");
    assert_eq!(resp.changed, 2, "only revision-current r1 and field-current r3 apply");
    assert_eq!(resp.conflicted, 3, "r2 field-CAS + r4 stale-revision + missing id");
    assert_eq!(resp.results.len(), 5, "per-item results are index-aligned with the request");
    assert!(resp.results[0].matched && resp.results[0].changed);
    assert!(resp.results[1].matched && resp.results[1].conflicted);
    assert!(resp.results[2].changed);
    assert!(resp.results[3].matched && resp.results[3].conflicted);
    assert!(!resp.results[4].matched && resp.results[4].conflicted);

    // Served reads: only the passing items applied.
    assert_eq!(served_status_of(&svc, &tenant, MSG, &r1).await.as_deref(), Some("B"));
    assert_eq!(served_status_of(&svc, &tenant, MSG, &r3).await.as_deref(), Some("B"));
    assert_eq!(
        served_status_of(&svc, &tenant, MSG, &r2).await.as_deref(),
        Some("A"),
        "a field-CAS conflict must not apply the item"
    );
    assert_eq!(
        served_status_of(&svc, &tenant, MSG, &r4).await.as_deref(),
        Some("A4"),
        "a stale-revision conflict must not apply the item"
    );

    // Tenant scope: a caller-tenant batch targeting a FOREIGN row's id changes
    // nothing in the foreign tenant (the write is tenant-filtered).
    let attack = vec![bulk_item(
        json!({"id": r_foreign, "tenant_id": tenant}),
        json!({"status": "HACKED"}),
        None,
        "",
    )];
    let _ = served_bulk_cas(&svc, &tenant, MSG, attack, "", 0)
        .await
        .expect("cross-tenant bulk cas call");
    assert_eq!(
        served_status_of(&svc, &foreign_tenant, MSG, &r_foreign).await.as_deref(),
        Some("FA"),
        "a caller-tenant batch must not mutate another tenant's row"
    );

    teardown(&pool, &schema, &tenant).await;
}

/// Through the real `DataBroker::bulk_cas` RPC: an over-ceiling batch is rejected
/// `INVALID_ARGUMENT` before any write, and a keyed batch re-sent with the SAME key
/// + items replays its counts WITHOUT re-applying (request-hash idempotent).
///
/// Revert-proof: revert the row-ceiling check and the 3-item batch under a
/// `max_rows = 2` ceiling proceeds (no `INVALID_ARGUMENT`); revert the whole-batch
/// dedup and the keyed replay re-runs the item, bumping the row revision a second
/// time — the `rev_after_replay == rev_after_first` assertion fails.
#[tokio::test]
async fn served_bulk_cas_is_bounded_and_request_hash_idempotent_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping bulk-CAS bound/idempotency (gate 23)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create widgets");
    const MSG: &str = "acme.dp.v1.Widget";
    let svc = dp_service(&dsn, widget_manifest(&schema, "widgets", "Widget", false)).await;

    let a = format!("a-{}", Uuid::new_v4().simple());
    let b = format!("b-{}", Uuid::new_v4().simple());
    let c = format!("c-{}", Uuid::new_v4().simple());
    for id in [&a, &b, &c] {
        served_upsert(&svc, &tenant, MSG, json!({"id": id, "tenant_id": tenant, "status": "A"}), "")
            .await
            .expect("seed row");
    }

    // Bounded: a 3-item batch under an explicit 2-row ceiling is rejected up front.
    let over = vec![
        bulk_item(json!({"id": a, "tenant_id": tenant}), json!({"status": "B"}), None, ""),
        bulk_item(json!({"id": b, "tenant_id": tenant}), json!({"status": "B"}), None, ""),
        bulk_item(json!({"id": c, "tenant_id": tenant}), json!({"status": "B"}), None, ""),
    ];
    let err = served_bulk_cas(&svc, &tenant, MSG, over, "", 2)
        .await
        .expect_err("a batch over the effective ceiling must be rejected");
    assert_eq!(
        err.code(),
        Code::InvalidArgument,
        "an over-ceiling batch is INVALID_ARGUMENT, got {err:?}"
    );
    assert_eq!(
        served_status_of(&svc, &tenant, MSG, &a).await.as_deref(),
        Some("A"),
        "a rejected over-ceiling batch must apply nothing"
    );

    // Idempotent: the SAME keyed batch replays counts and does not re-apply.
    let key = format!("bulk-idem-{}", Uuid::new_v4().simple());
    let batch = || {
        vec![bulk_item(
            json!({"id": a, "tenant_id": tenant}),
            json!({"status": "X"}),
            None,
            "",
        )]
    };
    let first = served_bulk_cas(&svc, &tenant, MSG, batch(), &key, 0)
        .await
        .expect("first keyed bulk cas");
    assert_eq!(first.changed, 1, "the unconditioned item applies once");
    let rev_after_first = served_row_revision(&svc, &tenant, MSG, &a)
        .await
        .expect("row a revision after first");

    let replay = served_bulk_cas(&svc, &tenant, MSG, batch(), &key, 0)
        .await
        .expect("keyed replay bulk cas");
    assert_eq!(replay.matched, first.matched, "replay reports the original matched count");
    assert_eq!(replay.changed, first.changed, "replay reports the original changed count");
    assert_eq!(replay.conflicted, first.conflicted);
    let rev_after_replay = served_row_revision(&svc, &tenant, MSG, &a)
        .await
        .expect("row a revision after replay");
    assert_eq!(
        rev_after_replay, rev_after_first,
        "a keyed replay must NOT re-apply the batch (no second revision bump)"
    );

    teardown(&pool, &schema, &tenant).await;
}

// ── gate 25: lock-fencing-at-commit ────────────────────────────────────────────

/// Through the real `DataBroker::update` RPC carrying `lock_name` + `fencing_token`
/// and the real `LockService` acquire/release: the current token holder commits;
/// after a NEWER owner takes token N+1, a delayed writer still presenting token N is
/// fenced off fail-closed with NO row change, NO revision bump, and NO idempotency
/// side effect.
///
/// Revert-proof: revert `enforce_fencing_token_in_tx` in the mutation path and the
/// stale-token write commits — the `FAILED_PRECONDITION` expectation, the "row
/// unchanged", the "revision unchanged", and the "no idempotency claim" assertions
/// all fail.
#[tokio::test]
async fn served_mutation_fences_stale_lock_token_with_no_side_effect_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping lock-fencing-at-commit (gate 25)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    // The fencing gate reads the durable LockService row, so the native catalog
    // (incl. the lock table) must exist. `migrate_native_service_db` rebuilds it
    // (dropping everything first), so (re)bootstrap the system catalog afterward and
    // only THEN carve the throwaway user table.
    super::support::migrate_native_service_db(&pool).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create widgets");
    const MSG: &str = "acme.dp.v1.Widget";
    let svc = dp_service(&dsn, widget_manifest(&schema, "widgets", "Widget", false)).await;
    let lock_svc = super::support::lock_service(pool.clone()).await;

    let lock_name = format!("fence-{}", Uuid::new_v4().simple());

    // Owner A acquires the lock → fencing token N.
    let acquired_a = lock_svc
        .acquire_lock(lock_request(
            lock_pb::AcquireLockRequest {
                tenant_id: tenant.clone(),
                lock_name: lock_name.clone(),
                owner_id: "owner-a".to_string(),
                lease_ttl_seconds: 120,
                ..Default::default()
            },
            &tenant,
        ))
        .await
        .expect("owner A acquires the lock")
        .into_inner();
    assert!(acquired_a.acquired, "owner A holds the lock");
    let token_n = acquired_a.fencing_token;
    assert!(token_n >= 1, "the fencing token is monotone and positive");

    // The CURRENT token holder commits a mutation.
    let id = format!("w-{}", Uuid::new_v4().simple());
    served_upsert(&svc, &tenant, MSG, json!({"id": id, "tenant_id": tenant, "status": "A"}), "")
        .await
        .expect("seed row");
    let fenced_update = |token: i64, status: &str, key: &str| UpdateRequest {
        message_type: MSG.to_string(),
        filter: json_to_struct(&json!({"id": id, "tenant_id": tenant})),
        changes: json_to_struct(&json!({"status": status})),
        lock_name: lock_name.clone(),
        fencing_token: token,
        idempotency_key: key.to_string(),
        ..UpdateRequest::default()
    };
    let ok = svc
        .update(with_ctx(fenced_update(token_n, "HELD-BY-A", ""), &tenant))
        .await
        .expect("the current-token holder's mutation commits")
        .into_inner();
    assert_eq!(ok.affected_rows, 1, "the current fencing-token holder writes");
    let rev_after_ok = ok.revision.clone();
    assert!(!rev_after_ok.is_empty(), "the committed write returns a revision");
    assert_eq!(served_status_of(&svc, &tenant, MSG, &id).await.as_deref(), Some("HELD-BY-A"));

    // A NEWER owner takes over: A releases, B acquires → token N+1.
    lock_svc
        .release_lock(lock_request(
            lock_pb::ReleaseLockRequest {
                tenant_id: tenant.clone(),
                lock_name: lock_name.clone(),
                owner_id: "owner-a".to_string(),
                fencing_token: token_n,
            },
            &tenant,
        ))
        .await
        .expect("owner A releases the lock");
    let acquired_b = lock_svc
        .acquire_lock(lock_request(
            lock_pb::AcquireLockRequest {
                tenant_id: tenant.clone(),
                lock_name: lock_name.clone(),
                owner_id: "owner-b".to_string(),
                lease_ttl_seconds: 120,
                ..Default::default()
            },
            &tenant,
        ))
        .await
        .expect("owner B acquires the lock")
        .into_inner();
    assert!(acquired_b.acquired, "owner B now holds the lock");
    assert!(
        acquired_b.fencing_token > token_n,
        "the successor token is strictly greater ({} > {token_n})",
        acquired_b.fencing_token
    );

    // Baseline for the "no idempotency side effect" proof.
    let idem_rel = SystemCatalogConfig::current().idempotency_keys_relation();
    let idem_before: i64 =
        sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {idem_rel} WHERE tenant_id = $1"))
            .bind(&tenant)
            .fetch_one(&pool)
            .await
            .expect("count idempotency rows before the fenced write");

    // The fenced-off writer still presents the STALE token N; it must be rejected
    // fail-closed. It carries an idempotency key so we can prove no dedup claim was
    // burned.
    let stale_key = format!("stale-{}", Uuid::new_v4().simple());
    let stale = svc
        .update(with_ctx(fenced_update(token_n, "HACKED-BY-A", &stale_key), &tenant))
        .await
        .expect_err("a stale fencing token must be rejected fail-closed");
    assert_eq!(
        stale.code(),
        Code::FailedPrecondition,
        "a stale-token write is fenced with FAILED_PRECONDITION, got {stale:?}"
    );

    // NO row change.
    assert_eq!(
        served_status_of(&svc, &tenant, MSG, &id).await.as_deref(),
        Some("HELD-BY-A"),
        "the fenced write must not modify the row"
    );
    // NO revision bump (proxy: nothing committed in the write tx).
    assert_eq!(
        served_row_revision(&svc, &tenant, MSG, &id).await.as_deref(),
        Some(rev_after_ok.as_str()),
        "the fenced write must not bump the row revision"
    );
    // NO idempotency side effect: the fencing gate runs BEFORE the dedup claim, so
    // the rolled-back tx persists no claim for the stale attempt's key.
    let idem_after: i64 =
        sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {idem_rel} WHERE tenant_id = $1"))
            .bind(&tenant)
            .fetch_one(&pool)
            .await
            .expect("count idempotency rows after the fenced write");
    assert_eq!(
        idem_after, idem_before,
        "a fenced-off write must not persist an idempotency claim"
    );
    // NOTE (documented, not a fake pass): CDC and audit are NOT independently
    // observable for this throwaway entity — it declares no `cdc_topic` (so the CDC
    // emit is a no-op even on success) and the test config has no durable audit
    // sink. They share the SAME rolled-back-tx / never-reached-commit fate as the
    // row write, the revision bump, and the idempotency claim asserted above, which
    // are the observable, revert-proof proxies that the write was fully fenced.

    teardown(&pool, &schema, &tenant).await;
}

// ── #8.1 / #8.2: transactional CAS + cdc_required over the REAL served BeginTx ──

/// Spin up the real DataBroker gRPC server on an ephemeral loopback port (bare — the
/// dev header-credential path, identical to the direct-handler tests) and return a
/// connected client plus a shutdown handle. This is the transport a client SDK uses
/// to drive the client-streaming `BeginTx` RPC.
async fn serve_data_broker(
    svc: DataBrokerService,
) -> (
    DataBrokerClient<tonic::transport::Channel>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind data-plane listener");
    let address = listener.local_addr().expect("listener address");
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let connection = listener.accept().await.map(|(stream, _)| stream);
        Some((connection, listener))
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(DataBrokerServer::new(svc))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve DataBroker");
    });
    let client = DataBrokerClient::connect(format!("http://{address}"))
        .await
        .expect("connect DataBroker client");
    (client, shutdown_tx, handle)
}

/// Drive one client-streaming `BeginTx` to completion, returning whether it
/// committed and the terminating error status (if any). A per-mutation failure
/// surfaces as an `Err` stream item (the runtime wraps the underlying precondition
/// error in the rollback/compensation envelope), so we read the stream to its end.
async fn drive_begin_tx(
    client: &mut DataBrokerClient<tonic::transport::Channel>,
    tenant: &str,
    mutations: Vec<Mutation>,
) -> (bool, Option<tonic::Status>) {
    let request = with_ctx(futures::stream::iter(mutations), tenant);
    let mut stream = client
        .begin_tx(request)
        .await
        .expect("begin_tx call accepted")
        .into_inner();
    let mut committed = false;
    let mut error = None;
    loop {
        match stream.message().await {
            Ok(Some(status)) => {
                if status.state == tx_status::State::TxStateCommitted as i32 {
                    committed = true;
                }
            }
            Ok(None) => break,
            Err(status) => {
                error = Some(status);
                break;
            }
        }
    }
    (committed, error)
}

/// Through the REAL client-streaming `BeginTx`: a transactional `update` whose
/// `expected` CAS mismatches the current row aborts the whole transaction with
/// nothing applied (bug #8.1).
///
/// Revert-proof: revert the transactional CAS (`enforce_tx_cas_precondition`) and
/// the mismatched update applies + the tx commits — `committed` is true, no error
/// is surfaced, and the served read shows "B". All three assertions fail.
#[tokio::test]
async fn served_begin_tx_expected_cas_mismatch_aborts_with_nothing_applied_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping BeginTx expected-CAS (#8.1)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create widgets");
    const MSG: &str = "acme.dp.v1.Widget";
    // Two runtimes on the SAME DSN + manifest: one is moved into the served server,
    // the other seeds and verifies through the direct handler path.
    let server_svc = dp_service(&dsn, widget_manifest(&schema, "widgets", "Widget", false)).await;
    let reader = dp_service(&dsn, widget_manifest(&schema, "widgets", "Widget", false)).await;

    let id = format!("tx-{}", Uuid::new_v4().simple());
    served_upsert(&reader, &tenant, MSG, json!({"id": id, "tenant_id": tenant, "status": "A"}), "")
        .await
        .expect("seed row");

    let (mut client, shutdown, handle) = serve_data_broker(server_svc).await;

    let mutations = vec![Mutation {
        message_type: MSG.to_string(),
        operation: "update".to_string(),
        filter: json_to_struct(&json!({"id": id, "tenant_id": tenant})),
        changes: json_to_struct(&json!({"status": "B"})),
        expected: json_to_struct(&json!({"status": "WRONG"})),
        commit: true,
        tx_id: Uuid::new_v4().to_string(),
        ..Mutation::default()
    }];
    let (committed, error) = drive_begin_tx(&mut client, &tenant, mutations).await;
    assert!(!committed, "a CAS-mismatched transaction must NOT commit");
    assert!(
        error.is_some(),
        "the mismatched transaction must surface an error status, not a silent success"
    );
    assert_eq!(
        served_status_of(&reader, &tenant, MSG, &id).await.as_deref(),
        Some("A"),
        "a CAS-mismatched BeginTx must apply nothing"
    );

    let _ = shutdown.send(());
    let _ = handle.await;
    teardown(&pool, &schema, &tenant).await;
}

/// Through the REAL client-streaming `BeginTx`: a `cdc_required` mutation on an
/// entity whose change event cannot be durably delivered (here: it declares no
/// `cdc_topic`) FAILS CLOSED and aborts the transaction — it does NOT silently skip
/// the event (bug #8.2).
///
/// Revert-proof: revert the `cdc_required` fail-closed check (best-effort emit) and
/// the update applies + the tx commits — `committed` is true, no `cdc_required`
/// error is surfaced, and the served read shows "C". The assertions fail.
#[tokio::test]
async fn served_begin_tx_cdc_required_fails_closed_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping BeginTx cdc_required (#8.2)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create widgets");
    const MSG: &str = "acme.dp.v1.Widget";
    let server_svc = dp_service(&dsn, widget_manifest(&schema, "widgets", "Widget", false)).await;
    let reader = dp_service(&dsn, widget_manifest(&schema, "widgets", "Widget", false)).await;

    let id = format!("tx-{}", Uuid::new_v4().simple());
    served_upsert(&reader, &tenant, MSG, json!({"id": id, "tenant_id": tenant, "status": "A"}), "")
        .await
        .expect("seed row");

    let (mut client, shutdown, handle) = serve_data_broker(server_svc).await;

    let mutations = vec![Mutation {
        message_type: MSG.to_string(),
        operation: "update".to_string(),
        filter: json_to_struct(&json!({"id": id, "tenant_id": tenant})),
        changes: json_to_struct(&json!({"status": "C"})),
        cdc_required: true,
        commit: true,
        tx_id: Uuid::new_v4().to_string(),
        ..Mutation::default()
    }];
    let (committed, error) = drive_begin_tx(&mut client, &tenant, mutations).await;
    assert!(!committed, "a cdc_required mutation that cannot be delivered must NOT commit");
    let error = error.expect("an undeliverable cdc_required mutation must surface an error status");
    assert!(
        error.message().contains("cdc_required"),
        "the failure must name the cdc_required contract, got: {}",
        error.message()
    );
    assert_eq!(
        served_status_of(&reader, &tenant, MSG, &id).await.as_deref(),
        Some("A"),
        "a fail-closed cdc_required mutation must apply nothing"
    );

    let _ = shutdown.send(());
    let _ = handle.await;
    teardown(&pool, &schema, &tenant).await;
}
