//! Live seam tests for the ops guarantees (fix-ledger section H + startup DDL).
//!
//! Each test crosses a real seam — a SERVED RPC, a worker pass, or a store —
//! against a live backend, and asserts both the user-visible outcome and the
//! durable read-back:
//!
//! - H1/H2: a served `BeginTx` writes its saga row into the SAME relation the
//!   recovery store reads; the owner/age-scoped startup sweep marks only this
//!   node's (and abandoned) sagas; `SagaRecoveryWorker::run_once` then claims
//!   them. An unwritable saga ledger aborts the served `BeginTx` with nothing
//!   committed.
//! - H5: an XA recovery pass whose singleton lease was taken over by a peer
//!   issues no COMMIT; the current holder's pass does.
//! - H6: a failing durable audit sink takes the DataBroker listener's
//!   `grpc.health.v1` status to NOT_SERVING, and it returns to SERVING once the
//!   sink has been durable for the whole (windowed) health window.
//! - H7: Cassandra admin-audit chain stays verifiable under concurrent
//!   appenders plus an append that crashed between its row insert and the head
//!   CAS.
//! - H8: a quorum-2 signed migration approval whose two signatures were both
//!   minted with the shared key is refused at the serve-side apply gate.
//! - 1.1 (H4): two concurrent `with_startup_ddl_lock` callers serialize on the
//!   startup advisory lock and both succeed.
//!
//! ## Env gating
//! Every live test is `#[ignore]`d and runs in the CI `--ignored` live step.
//! DSNs resolve through [`super::support::require_live_dsn_any`]: outside a
//! live lane a missing DSN skips, inside one (`UDB_LIVE_AUTH_TESTS=1`) it fails.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde_json::json;
use tonic::Request;
use uuid::Uuid;

use crate::engine::FsmState;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable, ManifestTableSecurity};
use crate::metrics::{MetricsRecorder, PrometheusMetrics};
use crate::proto::data_broker_client::DataBrokerClient;
use crate::proto::data_broker_server::DataBrokerServer;
use crate::proto::{Mutation, tx_status};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::config::UdbConfig;
use crate::runtime::security::SecurityConfig;
use crate::runtime::service::DataBrokerService;
use crate::runtime::system::{SystemCatalogConfig, ensure_system_catalog};

// ── shared harness ────────────────────────────────────────────────────────────

const PG_DSN_KEYS: [&str; 4] = [
    "UDB_LIVE_NATIVE_PG_DSN",
    "UDB_LIVE_AUTH_PG_DSN",
    "UDB_INTEGRATION_PG_DSN",
    "UDB_PG_DSN",
];

fn ops_pg_dsn() -> Option<String> {
    super::support::require_live_dsn_any(&PG_DSN_KEYS)
}

async fn ops_pool(dsn: &str) -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(6)
        .acquire_timeout(Duration::from_secs(10))
        .connect(dsn)
        .await
        .unwrap_or_else(|err| panic!("connect live ops postgres at {dsn}: {err}"))
}

/// Dev header-credential posture (no JWT/mTLS), same as the data-plane harness.
fn ops_test_security() -> SecurityConfig {
    SecurityConfig {
        tls_required: false,
        service_identity_required: false,
        mtls_required: false,
        allow_header_scopes: true,
        ..SecurityConfig::default()
    }
}

fn ops_config(dsn: &str) -> UdbConfig {
    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = dsn.to_string();
    config.security = ops_test_security();
    config
}

/// A live served DataBrokerService on `dsn` (default-allow authz, lifecycle
/// Completed) — mirrors `data_plane_live::dp_service`.
async fn ops_service(dsn: &str, mut manifest: CatalogManifest) -> DataBrokerService {
    if manifest.checksum_sha256.trim().is_empty() {
        use sha2::Digest as _;
        let digest = sha2::Sha256::digest(
            serde_json::to_vec(&manifest.tables).expect("serialize test manifest tables"),
        );
        manifest.checksum_sha256 = format!("sha256:{digest:x}");
    }
    SecurityConfig::install_global(ops_test_security());
    let runtime = DataBrokerRuntime::from_config(ops_config(dsn)).await;
    let lifecycle = Arc::new(RwLock::new(FsmState::Completed));
    let metrics: Arc<dyn MetricsRecorder> = Arc::new(PrometheusMetrics::new().expect("metrics"));
    DataBrokerService::with_runtime_and_state(manifest, runtime, lifecycle, metrics, None, true)
}

fn with_ctx<T>(message: T, tenant: &str) -> Request<T> {
    let mut req = Request::new(message);
    let md = req.metadata_mut();
    md.insert("x-tenant-id", tenant.parse().unwrap());
    md.insert("x-purpose", "admin".parse().unwrap());
    md.insert("x-scopes", "udb:admin,udb:read,udb:write".parse().unwrap());
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

const WIDGET_MSG: &str = "acme.ops.v1.Widget";

fn widget_manifest(schema: &str) -> CatalogManifest {
    CatalogManifest {
        tables: vec![ManifestTable {
            proto_package: "acme.ops.v1".to_string(),
            message_name: "Widget".to_string(),
            schema: schema.to_string(),
            table: "widgets".to_string(),
            primary_key: vec!["id".to_string()],
            table_security: ManifestTableSecurity {
                tenant_column: "tenant_id".to_string(),
                ..ManifestTableSecurity::default()
            },
            columns: vec![
                col("id", "TEXT", true),
                col("tenant_id", "TEXT", false),
                col("status", "TEXT", false),
            ],
            ..ManifestTable::default()
        }],
        ..CatalogManifest::default()
    }
}

async fn create_widget_schema(pool: &sqlx::PgPool, schema: &str) {
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(pool)
        .await
        .expect("create throwaway ops schema");
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(pool)
    .await
    .expect("create widgets");
}

async fn widget_count(pool: &sqlx::PgPool, schema: &str, id: &str) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM \"{schema}\".widgets WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("count widget rows")
}

/// Serve DataBroker on an ephemeral loopback port (the transport an SDK uses
/// for the client-streaming `BeginTx`).
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

/// Drive one `BeginTx` to completion: `(committed, terminating error)`.
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

fn upsert_mutation(id: &str, tenant: &str, tx_id: &str) -> Mutation {
    Mutation {
        message_type: WIDGET_MSG.to_string(),
        operation: "upsert".to_string(),
        record_json: serde_json::to_vec(&json!({"id": id, "tenant_id": tenant, "status": "A"}))
            .unwrap(),
        commit: true,
        tx_id: tx_id.to_string(),
        ..Mutation::default()
    }
}

// ── H1 / H2: saga ledger relation, owner-scoped sweep, recovery pickup ───────

#[cfg(feature = "postgres")]
fn saga_store(
    pool: &sqlx::PgPool,
) -> crate::runtime::canonical_store::postgres::PostgresCanonicalStore {
    crate::runtime::canonical_store::postgres::PostgresCanonicalStore::new(
        pool.clone(),
        "primary",
        crate::runtime::cdc::CdcConfig::current().outbox_relation(),
    )
    .with_saga_relation(crate::runtime::saga::data_plane_saga_relation())
}

/// Insert a data-plane saga row exactly as `saga_begin` does, owned by `owner`.
#[cfg(feature = "postgres")]
async fn insert_data_plane_saga(pool: &sqlx::PgPool, owner: &str) -> Uuid {
    let saga_id = Uuid::new_v4();
    sqlx::query(&crate::runtime::saga::data_plane_saga_begin_sql(
        &crate::runtime::saga::data_plane_saga_relation(),
    ))
    .bind(saga_id.to_string())
    .bind(Uuid::new_v4().to_string())
    .bind(Uuid::new_v4().to_string())
    .bind("ops-seam-peer")
    .bind("primary")
    .bind("upsert")
    .bind(owner)
    .execute(pool)
    .await
    .expect("insert peer data-plane saga");
    saga_id
}

#[cfg(feature = "postgres")]
async fn saga_status_of(
    store: &crate::runtime::canonical_store::postgres::PostgresCanonicalStore,
    saga_id: Uuid,
) -> crate::runtime::canonical_store::system_store::SagaRow {
    use crate::runtime::canonical_store::system_store::SagaStore;
    SagaStore::get_saga(store, saga_id)
        .await
        .expect("recovery store get_saga")
        .unwrap_or_else(|| panic!("saga {saga_id} must be readable through the recovery store"))
}

/// H1/H2 through the SERVED `BeginTx`.
///
/// 1. A committed BeginTx leaves its saga row in the relation the production
///    recovery store (`with_saga_relation(data_plane_saga_relation())`) reads,
///    stamped with this node as owner.
/// 2. Crash simulation: the row is put back to `in_progress` (the terminal
///    status write never happened). A peer node's FRESH in-flight saga and a
///    peer's ABANDONED (idle > stale threshold) saga sit beside it.
/// 3. `mark_indeterminate_sagas_with` marks ours and the abandoned one, never
///    the peer's live saga.
/// 4. `SagaRecoveryWorker::run_once` claims ours (no compensations recorded →
///    escalated to manual_review at poison threshold 1); the peer's live saga
///    is still untouched.
///
/// Revert-proof: split the relations again and step 1's list is empty; drop
/// the owner/age filter and step 3 flips the peer's live saga.
#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn served_begin_tx_saga_is_swept_by_owner_and_recovered_live() {
    use crate::runtime::canonical_store::SystemStores;
    use crate::runtime::canonical_store::system_store::{SagaListFilter, SagaStatus, SagaStore};
    use crate::runtime::config::SagaSettings;
    use crate::runtime::saga::{SagaRecoveryWorker, data_plane_saga_relation, local_saga_owner};
    use crate::runtime::saga_compensators::QuarantinePolicy;

    let Some(dsn) = ops_pg_dsn() else {
        eprintln!("ops live PG DSN unset — skipping served BeginTx saga recovery (H1/H2)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let pool = ops_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");
    let store = saga_store(&pool);
    SagaStore::ensure_saga_tables(&store)
        .await
        .expect("ensure saga tables on the shared data-plane relation");
    let relation = data_plane_saga_relation();

    let schema = format!("udb_ops_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_widget_schema(&pool, &schema).await;
    let server_svc = ops_service(&dsn, widget_manifest(&schema)).await;
    let runtime = server_svc.runtime.load_full();
    let (mut client, shutdown, handle) = serve_data_broker(server_svc).await;

    // 1. Served BeginTx commits and its saga lands in the recovery relation.
    let tx_id = Uuid::new_v4().to_string();
    let id = format!("saga-{}", Uuid::new_v4().simple());
    let (committed, error) = drive_begin_tx(
        &mut client,
        &tenant,
        vec![upsert_mutation(&id, &tenant, &tx_id)],
    )
    .await;
    assert!(committed, "the BeginTx must commit, got error {error:?}");
    assert_eq!(
        widget_count(&pool, &schema, &id).await,
        1,
        "the committed row is durable"
    );
    let listed = SagaStore::list_sagas(
        &store,
        &SagaListFilter {
            tx_id: Some(tx_id.clone()),
            ..SagaListFilter::default()
        },
    )
    .await
    .expect("recovery store lists data-plane sagas");
    assert_eq!(
        listed.len(),
        1,
        "the served BeginTx saga must be visible to the recovery store ({relation})"
    );
    let ours = listed[0].saga_id;
    assert_eq!(listed[0].status, SagaStatus::Committed);
    let owner: String = sqlx::query_scalar(&format!(
        "SELECT owner_node FROM {relation} WHERE saga_id = $1"
    ))
    .bind(ours)
    .fetch_one(&pool)
    .await
    .expect("read saga owner");
    assert_eq!(owner, local_saga_owner(), "saga_begin stamps this node");

    // 2. Crash simulation + peers.
    sqlx::query(&format!(
        "UPDATE {relation} SET status = 'in_progress', compensation_status = 'none' \
         WHERE saga_id = $1"
    ))
    .bind(ours)
    .execute(&pool)
    .await
    .expect("simulate crash before the terminal status write");
    let peer_owner = format!("peer-node-{}", Uuid::new_v4().simple());
    let peer_live = insert_data_plane_saga(&pool, &peer_owner).await;
    let peer_abandoned = insert_data_plane_saga(&pool, &peer_owner).await;
    sqlx::query(&format!(
        "UPDATE {relation} SET updated_at = NOW() - INTERVAL '2 hours' WHERE saga_id = $1"
    ))
    .bind(peer_abandoned)
    .execute(&pool)
    .await
    .expect("age the abandoned peer saga");

    // 3. Startup sweep: owner OR age.
    let settings = SagaSettings {
        stale_threshold_secs: 3600,
        poison_threshold: 1,
        recovery_batch_size: 10_000,
        ..SagaSettings::default()
    };
    runtime.mark_indeterminate_sagas_with(&settings).await;
    assert_eq!(
        saga_status_of(&store, ours).await.status,
        SagaStatus::Indeterminate,
        "this node's crashed saga must be marked indeterminate"
    );
    assert_eq!(
        saga_status_of(&store, peer_abandoned).await.status,
        SagaStatus::Indeterminate,
        "a saga idle past the stale threshold is reclaimed whoever owned it"
    );
    assert_eq!(
        saga_status_of(&store, peer_live).await.status,
        SagaStatus::InProgress,
        "a peer node's fresh in-flight saga must NOT be touched by this node's sweep"
    );

    // 4. Recovery worker claims ours.
    let worker_store: Arc<dyn SystemStores> = Arc::new(saga_store(&pool));
    let worker = SagaRecoveryWorker::with_settings(worker_store, &settings).with_quarantine_policy(
        QuarantinePolicy {
            max_attempts: 5,
            cooldown_secs: 0,
        },
    );
    let report = worker.run_once().await;
    assert!(report.lease_acquired, "worker lease: {report:?}");
    assert!(
        report.scanned >= 2,
        "worker must claim our sagas: {report:?}"
    );
    let recovered = saga_status_of(&store, ours).await;
    assert_eq!(
        recovered.status,
        SagaStatus::ManualReview,
        "the recovery pass must pick up the indeterminate data-plane saga"
    );
    assert_eq!(recovered.recovery_attempts, 1);
    assert_eq!(
        saga_status_of(&store, peer_live).await.status,
        SagaStatus::InProgress,
        "recovery must not claim a peer's live saga either"
    );

    let _ = shutdown.send(());
    let _ = handle.await;
    let _ = sqlx::query(&format!("DELETE FROM {relation} WHERE saga_id = ANY($1)"))
        .bind(vec![ours, peer_live, peer_abandoned])
        .execute(&pool)
        .await;
    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
}

/// H2: when the saga ledger cannot be written, the served `BeginTx` fails with
/// a retryable error and NOTHING is committed — no untracked side effects.
/// The ledger is made unwritable for exactly this transaction with a
/// `NOT VALID` CHECK on its tx_id, so no other saga writer is affected.
///
/// Revert-proof: go back to logging the `saga_begin` failure and continuing,
/// and the transaction commits — `committed` is true and the row exists.
#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn served_begin_tx_aborts_when_saga_ledger_is_unwritable_live() {
    use crate::runtime::canonical_store::system_store::{SagaListFilter, SagaStore};
    use crate::runtime::saga::data_plane_saga_relation;

    let Some(dsn) = ops_pg_dsn() else {
        eprintln!("ops live PG DSN unset — skipping unwritable saga ledger (H2)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let pool = ops_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");
    let store = saga_store(&pool);
    SagaStore::ensure_saga_tables(&store)
        .await
        .expect("ensure saga tables on the shared data-plane relation");
    let relation = data_plane_saga_relation();

    let schema = format!("udb_ops_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_widget_schema(&pool, &schema).await;
    let tx_id = Uuid::new_v4().to_string();
    let constraint = format!("udb_ops_reject_{}", Uuid::new_v4().simple());
    sqlx::query(&format!(
        "ALTER TABLE {relation} ADD CONSTRAINT \"{constraint}\" \
         CHECK (tx_id <> '{tx_id}') NOT VALID"
    ))
    .execute(&pool)
    .await
    .expect("make the saga ledger reject this transaction");

    let server_svc = ops_service(&dsn, widget_manifest(&schema)).await;
    let (mut client, shutdown, handle) = serve_data_broker(server_svc).await;
    let id = format!("nosaga-{}", Uuid::new_v4().simple());
    let (committed, error) = drive_begin_tx(
        &mut client,
        &tenant,
        vec![upsert_mutation(&id, &tenant, &tx_id)],
    )
    .await;

    let _ = sqlx::query(&format!(
        "ALTER TABLE {relation} DROP CONSTRAINT IF EXISTS \"{constraint}\""
    ))
    .execute(&pool)
    .await;

    assert!(
        !committed,
        "a BeginTx whose saga ledger write failed must not commit"
    );
    let error = error.expect("the failed saga ledger write must surface an error");
    assert_eq!(error.code(), tonic::Code::Unavailable, "{error:?}");
    assert!(
        error.message().contains("saga ledger"),
        "the error must name the saga ledger, got: {}",
        error.message()
    );
    assert_eq!(
        widget_count(&pool, &schema, &id).await,
        0,
        "nothing may be committed when the saga ledger is unwritable"
    );
    let listed = SagaStore::list_sagas(
        &store,
        &SagaListFilter {
            tx_id: Some(tx_id.clone()),
            ..SagaListFilter::default()
        },
    )
    .await
    .expect("list sagas");
    assert!(listed.is_empty(), "no saga row may exist: {listed:?}");

    let _ = shutdown.send(());
    let _ = handle.await;
    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
}

// ── H5: XA recovery pass under a superseded lease issues no COMMIT ───────────

/// In-doubt participant that records every recovery call instead of talking
/// to MySQL (MySQL is not in the native live lane).
struct RecordingParticipant {
    label: String,
    prepared: Arc<std::sync::Mutex<Vec<String>>>,
    commits: Arc<std::sync::Mutex<Vec<String>>>,
    rollbacks: Arc<std::sync::Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl crate::runtime::xa_recovery::XaInDoubtParticipant for RecordingParticipant {
    fn backend_label(&self) -> &str {
        &self.label
    }

    async fn list_prepared_xids(&self) -> Result<Vec<String>, String> {
        Ok(self.prepared.lock().expect("prepared lock").clone())
    }

    async fn commit_prepared(&self, xid: &str) -> Result<(), String> {
        self.prepared
            .lock()
            .expect("prepared lock")
            .retain(|p| p != xid);
        self.commits
            .lock()
            .expect("commits lock")
            .push(xid.to_string());
        Ok(())
    }

    async fn rollback_prepared(&self, xid: &str) -> Result<(), String> {
        self.prepared
            .lock()
            .expect("prepared lock")
            .retain(|p| p != xid);
        self.rollbacks
            .lock()
            .expect("rollbacks lock")
            .push(xid.to_string());
        Ok(())
    }
}

async fn xa_ledger_state(pool: &sqlx::PgPool, relation: &str, xid: &str) -> (String, i32) {
    sqlx::query_as(&format!(
        "SELECT decision, recovery_attempts FROM {relation} WHERE xid = $1"
    ))
    .bind(xid)
    .fetch_one(pool)
    .await
    .expect("read XA ledger row")
}

/// H5: a holder whose XA-recovery lease was taken over by a peer (expired, then
/// re-acquired with an advanced fencing token) runs `run_xa_recovery_pass_fenced`
/// and drives NOTHING: no COMMIT reaches the participant and the ledger row is
/// still `in_doubt` with zero attempts. The current holder's pass then commits
/// it (positive control: the participant and ledger are wired).
///
/// The ledger is an isolated per-run relation so the pass never touches other
/// tests' in-doubt rows.
///
/// Revert-proof: drop the `fence.check()` calls and the stale holder's pass
/// commits the xid — `commits` is non-empty and the ledger reads `committed`.
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn xa_recovery_pass_under_superseded_lease_commits_nothing_live() {
    use crate::runtime::singleton::{PostgresSingletonLease, worker_lock_key};
    use crate::runtime::xa::{XaCoordinator, XaDecision, XaLedgerEntry};
    use crate::runtime::xa_recovery::{
        InDoubtRegistry, RecoveryConfig, record_xa_ledger_entry, run_xa_recovery_pass_fenced,
    };

    let Some(dsn) = ops_pg_dsn() else {
        eprintln!("ops live PG DSN unset — skipping XA lease takeover (H5)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let pool = ops_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let mut config = SystemCatalogConfig::current();
    config.xa_ledger_table = format!("udb_xa_ledger_ops_{}", Uuid::new_v4().simple());
    let ledger_relation = config.xa_ledger_relation();

    let xid = XaCoordinator::new_xid();
    let participant_label = "recording:ops".to_string();
    record_xa_ledger_entry(
        &pool,
        &config,
        &XaLedgerEntry::new(
            xid.clone(),
            Uuid::new_v4().to_string(),
            "",
            "ops_seams_live",
            "corr-ops-xa",
            vec![participant_label.clone()],
            XaDecision::InDoubt,
        ),
    )
    .await
    .expect("seed in-doubt ledger row");

    let prepared = Arc::new(std::sync::Mutex::new(vec![xid.clone()]));
    let commits = Arc::new(std::sync::Mutex::new(Vec::new()));
    let rollbacks = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut registry = InDoubtRegistry::new();
    registry.register(Arc::new(RecordingParticipant {
        label: participant_label,
        prepared: prepared.clone(),
        commits: commits.clone(),
        rollbacks: rollbacks.clone(),
    }));
    let recovery = RecoveryConfig {
        interval: Duration::from_secs(30),
        max_attempts: 0,
    };
    // Far beyond any real prepared xact's age: the presumed-abort sweep must
    // not touch another test's prepared transactions.
    let grace_secs: i64 = 10 * 365 * 24 * 3600;

    // Lease held, then taken over by a peer while this holder was "paused".
    let lock_relation = crate::runtime::cdc::CdcConfig::current().lock_log_relation();
    let worker = format!("udb:test:xa-fence:{}", Uuid::new_v4());
    let lease = PostgresSingletonLease::try_acquire(
        pool.clone(),
        lock_relation.clone(),
        worker.clone(),
        Duration::from_secs(5),
    )
    .await
    .expect("acquire")
    .expect("free lease");
    let stale_fence = lease.fence();
    stale_fence
        .check()
        .await
        .expect("the fresh holder passes the fence");
    sqlx::query(&format!(
        "UPDATE {lock_relation} SET acquired_at = NOW() - INTERVAL '1 hour' WHERE lock_key = $1"
    ))
    .bind(worker_lock_key(&worker))
    .execute(&pool)
    .await
    .expect("expire the lease");
    let peer = PostgresSingletonLease::try_acquire(
        pool.clone(),
        lock_relation.clone(),
        worker.clone(),
        Duration::from_secs(5),
    )
    .await
    .expect("peer acquire")
    .expect("expired lease is taken over");
    assert!(peer.fencing_token() > lease.fencing_token());

    // The superseded holder's pass: refused before any participant call.
    let err = run_xa_recovery_pass_fenced(
        &pool,
        &config,
        &registry,
        &recovery,
        grace_secs,
        &stale_fence,
    )
    .await
    .expect_err("a superseded lease holder's XA recovery pass must abort");
    assert!(err.contains("superseded"), "{err}");
    assert!(
        commits.lock().expect("commits lock").is_empty(),
        "a superseded holder must issue NO XA COMMIT"
    );
    assert!(rollbacks.lock().expect("rollbacks lock").is_empty());
    assert_eq!(
        xa_ledger_state(&pool, &ledger_relation, &xid).await,
        ("in_doubt".to_string(), 0),
        "the in-doubt ledger row must be untouched by the fenced-off pass"
    );

    // Positive control: the current holder drives it terminal.
    let (driven, _) = run_xa_recovery_pass_fenced(
        &pool,
        &config,
        &registry,
        &recovery,
        grace_secs,
        &peer.fence(),
    )
    .await
    .expect("the current lease holder's pass runs");
    assert_eq!(driven, 1, "the current holder drives the in-doubt xid");
    assert_eq!(*commits.lock().expect("commits lock"), vec![xid.clone()]);
    assert_eq!(
        xa_ledger_state(&pool, &ledger_relation, &xid).await.0,
        "committed"
    );

    let _ = peer.release().await;
    let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {ledger_relation}"))
        .execute(&pool)
        .await;
}

// ── H6: durable-audit degradation drives grpc.health ─────────────────────────

/// Restores (or removes) an env var on drop.
struct EnvRestore {
    key: &'static str,
    previous: Option<String>,
}

impl EnvRestore {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        // SAFETY: the live lane runs `--test-threads=1`; no other thread
        // reads or writes the environment concurrently.
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        // SAFETY: see `EnvRestore::set`.
        unsafe {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

async fn health_status(
    client: &mut tonic_health::pb::health_client::HealthClient<tonic::transport::Channel>,
    service: &str,
) -> i32 {
    client
        .check(tonic_health::pb::HealthCheckRequest {
            service: service.to_string(),
        })
        .await
        .expect("grpc.health Check")
        .into_inner()
        .status
}

/// Emit one audit event through a configured-but-unusable Postgres sink (no
/// pool): the real `emit_audit` degradation path.
fn degrade_audit_once() {
    use crate::planning::broker::AuditEvent;
    use crate::runtime::config::{AuditSinkConfig, AuditSinkKind};
    crate::runtime::core::audit::emit_audit(
        &AuditSinkConfig {
            kind: AuditSinkKind::Postgres,
            pg_table: Some("udb_system.audit_ops_seam_unreached".to_string()),
            ..Default::default()
        },
        &AuditEvent {
            event_type: "upsert-ops-seam".to_string(),
            tenant_id: "tenant-ops-seam".to_string(),
            user_id: "user-ops-seam".to_string(),
            correlation_id: "corr-ops-seam".to_string(),
            purpose: "ops-seam-health".to_string(),
            resource_uri: "udb://ops/health".to_string(),
            checksum_sha256: "0".repeat(64),
        },
        None,
    );
}

/// H6: the DataBroker listener's served `grpc.health.v1.Health/Check` goes
/// NOT_SERVING while the durable audit sink is failing and back to SERVING
/// once it has been durable for the whole health window. The window is the
/// shortest the ring supports (60s = the current minute bucket), so recovery
/// takes at most one bucket rollover plus one 5s refresh tick; there is no
/// test hook to clear the ring, so this is the minimum real wait.
///
/// Revert-proof: drop `audit_health_ok` from the readiness refresh and the
/// status never leaves SERVING; go back to the lifetime counter and it never
/// returns to SERVING.
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn audit_sink_failure_flips_databroker_health_and_recovers_live() {
    use crate::runtime::core::audit::audit_degraded_events_within;
    use crate::runtime::service::handlers_meta::{HealthPlane, build_listener_health_service};
    use tonic_health::pb::health_check_response::ServingStatus;

    let Some(dsn) = ops_pg_dsn() else {
        eprintln!("ops live PG DSN unset — skipping audit degradation health probe (H6)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let pool = ops_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");
    let _window = EnvRestore::set("UDB_AUDIT_DEGRADED_HEALTH_WINDOW_SECS", "60");

    // Precondition: earlier tests in this process may have degraded audit;
    // start from a clean window so the SERVING baseline is real.
    let clean_deadline = tokio::time::Instant::now() + Duration::from_secs(75);
    while audit_degraded_events_within(60) > 0 {
        assert!(
            tokio::time::Instant::now() < clean_deadline,
            "the audit degradation window never cleared before the test started"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let config = ops_config(&dsn);
    let runtime = DataBrokerRuntime::from_config(config.clone()).await;
    let health =
        build_listener_health_service(HealthPlane::DataBroker, &config, Some(&runtime)).await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind health listener");
    let address = listener.local_addr().expect("listener address");
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let connection = listener.accept().await.map(|(stream, _)| stream);
        Some((connection, listener))
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(health)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve grpc.health");
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .expect("health endpoint")
        .connect()
        .await
        .expect("connect health client");
    let mut client = tonic_health::pb::health_client::HealthClient::new(channel);
    let service = <DataBrokerServer<DataBrokerService> as tonic::server::NamedService>::NAME;

    // Baseline: SERVING (boot readiness passed, durable audit healthy).
    let baseline = health_status(&mut client, service).await;
    if baseline != ServingStatus::Serving as i32 {
        let statuses =
            crate::runtime::service::native_registry::resolved_native_service_statuses(&config);
        let auth =
            crate::runtime::service::auth_readiness_triples(&SecurityConfig::current()).await;
        let errors =
            crate::runtime::slo::build_readiness_facts(runtime.init_report(), &statuses, &auth)
                .errors();
        panic!(
            "DataBroker health must start SERVING (got {baseline}); boot readiness errors: \
             {errors:?}"
        );
    }

    // Sustained sink failure → NOT_SERVING within a refresh tick or two.
    let degraded_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        degrade_audit_once();
        if health_status(&mut client, service).await == ServingStatus::NotServing as i32 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < degraded_deadline,
            "a failing durable audit sink must take the DataBroker listener NOT_SERVING"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(audit_degraded_events_within(60) > 0);

    // Sink recovered (no further degradation) → SERVING once the window is
    // clean: at most one 60s bucket rollover + one 5s refresh tick.
    let recovery_deadline = tokio::time::Instant::now() + Duration::from_secs(130);
    loop {
        if health_status(&mut client, service).await == ServingStatus::Serving as i32 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < recovery_deadline,
            "the DataBroker listener must return to SERVING once audit is durable for the \
             whole window (windowed rate, not a lifetime total)"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    assert_eq!(audit_degraded_events_within(60), 0);

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

// ── H8: shared-key "quorum" is refused at the apply gate ─────────────────────

/// H8: an approval plan file claiming a 2-person quorum whose two signatures
/// were BOTH minted with the shared `UDB_APPROVAL_SIGNING_KEY` (so one key
/// holder signed as "alice" and "bob") is refused by `ready_to_apply` — the
/// single call serve's migration gate makes on a loaded sealed plan:
/// - quorum 2 with only the shared key → `QuorumRequiresPerApproverKeys`;
/// - quorum 2 with per-approver keys → `BadSignature` (the shared key is not
///   alice's key).
///
/// The positive control (each approver signs with their own key) is accepted.
#[test]
fn shared_key_quorum_plan_is_refused_at_apply_gate() {
    use crate::control::plan_approval::{
        ApprovalConfig, ApprovalError, ApprovalSignature, ApprovedPlan, PlanMatchResult,
        build_exported_plan, compute_signature,
    };
    use crate::migration::diff::{ChangeKind, ChangeOperation, ChangeSafety};

    let manifest = CatalogManifest {
        checksum_sha256: "sha256:ops-seam-h8".to_string(),
        ..CatalogManifest::default()
    };
    let changes = vec![ChangeOperation {
        kind: ChangeKind::AddColumn,
        safety: ChangeSafety::SafeAuto,
        schema: "ops".to_string(),
        table: "cases".to_string(),
        fingerprint: "ops-seam-h8-add-column".to_string(),
        ..Default::default()
    }];
    let shared_key = b"shared-approval-signing-key-32-bytes".to_vec();
    let sign = |plan_hash: &str, approver: &str, key: &[u8]| ApprovalSignature {
        approver_id: approver.to_string(),
        approver_role: "sre".to_string(),
        approved_at_unix_ms: 1_000,
        reason: format!("{approver} approves"),
        signature: compute_signature(plan_hash, approver, &format!("{approver} approves"), key),
    };
    let base = ApprovalConfig {
        quorum_size: 1,
        allowed_roles: Vec::new(),
        expiry: Duration::from_secs(3600),
        signing_key: shared_key.clone(),
        approver_keys: std::collections::BTreeMap::new(),
    };

    // The forged file: one shared-key holder signs as two approvers. It seals
    // fine under single-party rules, which is how such a file gets produced.
    let plan = build_exported_plan(&manifest, &changes);
    let forged = ApprovedPlan::create(
        plan.clone(),
        vec![
            sign(&plan.operations_hash, "alice", &shared_key),
            sign(&plan.operations_hash, "bob", &shared_key),
        ],
        &base,
        0,
    )
    .expect("single-party sealing of the forged file");
    // Serve loads it from disk.
    let loaded: ApprovedPlan =
        serde_json::from_str(&serde_json::to_string(&forged).expect("serialize plan file"))
            .expect("parse plan file");
    let now = 1_000;

    let shared_only = ApprovalConfig {
        quorum_size: 2,
        ..base.clone()
    };
    let err = loaded
        .ready_to_apply(&shared_only, &manifest, &changes, now)
        .expect_err("a shared-key 'quorum' must be refused");
    assert_eq!(
        err,
        ApprovalError::QuorumRequiresPerApproverKeys { required: 2 }
    );
    assert!(
        err.to_string().contains("UDB_APPROVAL_APPROVER_KEYS"),
        "the refusal must name the knob that fixes it: {err}"
    );

    let mut approver_keys = std::collections::BTreeMap::new();
    approver_keys.insert("alice".to_string(), b"alice-own-key-.......".to_vec());
    approver_keys.insert("bob".to_string(), b"bob-own-key-.........".to_vec());
    let per_approver = ApprovalConfig {
        quorum_size: 2,
        approver_keys: approver_keys.clone(),
        ..base.clone()
    };
    let err = loaded
        .ready_to_apply(&per_approver, &manifest, &changes, now)
        .expect_err("shared-key signatures must not verify under per-approver keys");
    assert_eq!(
        err,
        ApprovalError::BadSignature {
            approver_id: "alice".to_string()
        }
    );

    // Positive control: two distinct approvers, each with their own key.
    let genuine = ApprovedPlan::create(
        plan.clone(),
        vec![
            sign(&plan.operations_hash, "alice", &approver_keys["alice"]),
            sign(&plan.operations_hash, "bob", &approver_keys["bob"]),
        ],
        &per_approver,
        0,
    )
    .expect("a genuine two-key quorum seals");
    assert_eq!(
        genuine
            .ready_to_apply(&per_approver, &manifest, &changes, now)
            .expect("a genuine quorum is accepted"),
        PlanMatchResult::Matches
    );
}

// ── H7: Cassandra admin-audit chain under concurrency + crashed append ───────

/// H7: concurrent appenders plus an append that crashed between its row
/// insert and the head CAS (simulated: the row is written on the current head
/// and the head is never advanced) — and a second such orphan at the tip —
/// leave a chain that verifies end to end, with every successful append linked
/// exactly once and the walk ending at the head.
///
/// Revert-proof: with head-before-row ordering a crashed append leaves the head
/// naming a missing row; with strict verify the mid-chain orphan breaks it.
#[cfg(feature = "cassandra")]
#[tokio::test]
#[ignore = "requires live Cassandra (UDB_CASSANDRA_DSN); runs in the CI --ignored live step"]
async fn cassandra_admin_audit_chain_survives_concurrency_and_crashed_append_live() {
    use crate::runtime::canonical_store::cassandra::CassandraCanonicalStore;
    use crate::runtime::canonical_store::system_store::{
        AdminAuditInsert, AdminAuditListFilter, AdminAuditStore, compute_admin_audit_hash,
    };
    use crate::runtime::executors::cassandra::CassandraClient;

    let Some(dsn) = super::support::require_live_dsn("UDB_CASSANDRA_DSN") else {
        eprintln!("UDB_CASSANDRA_DSN unset — skipping Cassandra admin-audit chain (H7)");
        return;
    };
    let client = CassandraClient::connect(&dsn)
        .await
        .expect("connect to live Cassandra (UDB_CASSANDRA_DSN)");
    let keyspace = format!("udb_ops_{}", Uuid::new_v4().simple());
    let store = Arc::new(CassandraCanonicalStore::new(
        client.clone(),
        "ops-seam-live",
        keyspace.clone(),
        "udb_outbox_events",
    ));
    AdminAuditStore::ensure_admin_audit_tables(store.as_ref())
        .await
        .expect("ensure admin audit tables");

    let entry = |actor: String| AdminAuditInsert {
        actor,
        operation: "ops.seam".to_string(),
        target: "chain".to_string(),
        request_json: json!({"seam": "h7"}),
        result: "ok".to_string(),
        tenant_id: "tenant-ops".to_string(),
        project_id: "project-ops".to_string(),
        correlation_id: "corr-ops".to_string(),
        signer_key_id: String::new(),
        external_anchor: String::new(),
    };
    // A crashed append: row written on the current head, head never advanced.
    let crash_append = |actor: &'static str| {
        let store = store.clone();
        let client = client.clone();
        let keyspace = keyspace.clone();
        async move {
            let head = AdminAuditStore::latest_admin_audit_hash(store.as_ref())
                .await
                .expect("read head");
            let request_json = json!({"seam": "h7-crashed"});
            let current = compute_admin_audit_hash(
                &head,
                actor,
                "ops.seam",
                "chain",
                &request_json,
                "ok",
                "tenant-ops",
                "project-ops",
                "corr-ops",
                "",
                "",
            );
            client
                .cql_execute(
                    &format!(
                        "INSERT INTO \"{keyspace}\".\"udb_admin_audit_log\" ( \
                            chain, created_at, audit_id, actor, operation, target, \
                            request_json, result, tenant_id, project_id, correlation_id, \
                            previous_hash, current_hash, signer_key_id, external_anchor \
                         ) VALUES ('main', toTimestamp(now()), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                         ?, ?)"
                    ),
                    (
                        Uuid::new_v4().to_string(),
                        actor,
                        "ops.seam",
                        "chain",
                        request_json.to_string(),
                        "ok",
                        "tenant-ops",
                        "project-ops",
                        "corr-ops",
                        head.clone(),
                        current,
                        "",
                        "",
                    ),
                )
                .await
                .expect("write the crashed append's row");
            head
        }
    };

    let mut appended = Vec::new();
    for i in 0..3 {
        appended.push(
            AdminAuditStore::append_admin_audit(store.as_ref(), &entry(format!("seed-{i}")))
                .await
                .expect("sequential append"),
        );
    }
    let head_at_crash = crash_append("crashed-mid").await;
    assert_eq!(
        AdminAuditStore::latest_admin_audit_hash(store.as_ref())
            .await
            .expect("read head"),
        head_at_crash,
        "a crashed append must not move the head"
    );

    const APPENDERS: usize = 4;
    const PER_APPENDER: usize = 5;
    let mut tasks = Vec::new();
    for a in 0..APPENDERS {
        let store = store.clone();
        let entries: Vec<AdminAuditInsert> = (0..PER_APPENDER)
            .map(|i| entry(format!("appender-{a}-{i}")))
            .collect();
        tasks.push(tokio::spawn(async move {
            let mut ids = Vec::new();
            for e in &entries {
                ids.push(
                    AdminAuditStore::append_admin_audit(store.as_ref(), e)
                        .await
                        .expect("concurrent append"),
                );
            }
            ids
        }));
    }
    for task in tasks {
        appended.extend(task.await.expect("appender task"));
    }
    crash_append("crashed-tip").await;

    let head = AdminAuditStore::latest_admin_audit_hash(store.as_ref())
        .await
        .expect("read head");
    let report = AdminAuditStore::verify_admin_audit_chain(store.as_ref(), None)
        .await
        .expect("verify chain");
    assert!(report.is_passed(), "chain must verify: {report:?}");
    assert_eq!(
        report.checked_count(),
        appended.len() as i64,
        "every successful append is linked exactly once: {report:?}"
    );
    match &report {
        crate::runtime::canonical_store::system_store::AdminAuditChainReport::Passed {
            last_hash,
            ..
        } => assert_eq!(last_hash, &head, "the walk must end at the chain head"),
        other => panic!("expected a passing chain, got {other:?}"),
    }
    let listed = AdminAuditStore::list_admin_audit(
        store.as_ref(),
        &AdminAuditListFilter {
            limit: 1_000,
            ..AdminAuditListFilter::default()
        },
    )
    .await
    .expect("list admin audit");
    for id in &appended {
        assert!(
            listed.iter().any(|row| &row.audit_id == id),
            "appended audit row {id} must be durably readable"
        );
    }

    drop(store);
    let _ = client
        .cql_execute(&format!("DROP KEYSPACE IF EXISTS \"{keyspace}\""), ())
        .await;
}

// ── 1.1 (H4): startup DDL serialized on the startup advisory lock ────────────

/// Two concurrent startup DDL runs against the same fresh schema both succeed,
/// never overlap (the advisory lock serializes them), and leave exactly one
/// usable table.
///
/// Revert-proof: run the DDL without the lock and the two bodies overlap
/// (`peak == 2`) and can collide on `pg_type_typname_nsp_index`.
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn concurrent_startup_ddl_serializes_on_advisory_lock_live() {
    use crate::runtime::system::with_startup_ddl_lock;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let Some(dsn) = ops_pg_dsn() else {
        eprintln!("ops live PG DSN unset — skipping startup DDL lock (1.1)");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let pool = ops_pool(&dsn).await;
    let schema = format!("udb_ddl_lock_{}", Uuid::new_v4().simple());
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let runs = Arc::new(AtomicUsize::new(0));

    let make_run = |pool: sqlx::PgPool, schema: String| {
        let active = active.clone();
        let peak = peak.clone();
        let runs = runs.clone();
        move || {
            let pool = pool.clone();
            let schema = schema.clone();
            let active = active.clone();
            let peak = peak.clone();
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                let result = async {
                    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
                        .execute(&pool)
                        .await?;
                    sqlx::query("SELECT pg_sleep(0.3)").execute(&pool).await?;
                    sqlx::query(&format!(
                        "CREATE TABLE IF NOT EXISTS \"{schema}\".t (id BIGINT PRIMARY KEY, note TEXT)"
                    ))
                    .execute(&pool)
                    .await?;
                    Ok::<(), sqlx::Error>(())
                }
                .await;
                active.fetch_sub(1, Ordering::SeqCst);
                result
            }
        }
    };

    let (a, b) = tokio::join!(
        with_startup_ddl_lock(&pool, "test", make_run(pool.clone(), schema.clone())),
        with_startup_ddl_lock(&pool, "test", make_run(pool.clone(), schema.clone())),
    );
    a.expect("first concurrent startup DDL succeeds");
    b.expect("second concurrent startup DDL succeeds");
    assert_eq!(
        peak.load(Ordering::SeqCst),
        1,
        "startup DDL bodies must never overlap under the advisory lock"
    );
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "no collision retry is needed when the lock serializes the runs"
    );

    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = 't' AND c.relkind = 'r'",
    )
    .bind(&schema)
    .fetch_one(&pool)
    .await
    .expect("count created tables");
    assert_eq!(tables, 1, "the table exists exactly once");
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".t (id, note) VALUES (1, 'ok')"
    ))
    .execute(&pool)
    .await
    .expect("the created table is usable");
    let note: String = sqlx::query_scalar(&format!("SELECT note FROM \"{schema}\".t WHERE id = 1"))
        .fetch_one(&pool)
        .await
        .expect("read back");
    assert_eq!(note, "ok");

    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
}

#[test]
fn concurrent_ddl_collision_messages_are_recognized() {
    use crate::runtime::system::is_concurrent_ddl_collision;
    assert!(is_concurrent_ddl_collision(
        r#"error returned from database: relation "outbox_events" already exists"#
    ));
    assert!(is_concurrent_ddl_collision(
        r#"error returned from database: duplicate key value violates unique constraint "pg_type_typname_nsp_index""#
    ));
    assert!(!is_concurrent_ddl_collision(
        r#"error returned from database: relation "outbox_events" does not exist"#
    ));
    assert!(!is_concurrent_ddl_collision(
        "error returned from database: permission denied for schema udb_system"
    ));
}
