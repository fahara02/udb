//! Worker and handler seam tests for native-service ledger items that run on the
//! CI live lane's Postgres + MinIO:
//!
//! * G4 — the lock expiry reaper sweeps EVERY active project, so a lease written
//!   to a project bound to its own Postgres instance is expired too;
//! * G6 — TURN credentials in production require the dedicated
//!   `UDB_TURN_SECRET` (never the master key or the dev constant);
//! * G7 — the compliance evidence export is off unless enabled, and when enabled
//!   it writes the general data-plane audit table to the object store.

use super::support::*;
use crate::proto::udb::core::webrtc::services::v1 as webrtc_pb;
use crate::proto::udb::core::webrtc::services::v1::peer_service_server::PeerService;
use crate::proto::udb::core::webrtc::services::v1::room_service_server::RoomService;
use crate::proto::udb::core::webrtc::services::v1::turn_service_server::TurnService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::config::{
    BackendInstance, BackendInstanceConfig, BackendInstanceRole, UdbConfig,
};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// The live DSN with its database path swapped for `database`.
fn dsn_for_database(base_dsn: &str, database: &str) -> String {
    let (without_query, query) = base_dsn
        .split_once('?')
        .map_or((base_dsn, None), |(base, query)| (base, Some(query)));
    let slash = without_query
        .rfind('/')
        .expect("live PostgreSQL DSN must contain a database path");
    let mut dsn = format!("{}{database}", &without_query[..=slash]);
    if let Some(query) = query {
        dsn.push('?');
        dsn.push_str(query);
    }
    dsn
}

/// Apply the native catalog DDL + system catalog to a freshly created database,
/// exactly as a project-bound instance is provisioned.
async fn provision_native_database(dsn: &str) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(dsn)
        .await
        .unwrap_or_else(|err| panic!("connect project database at {dsn}: {err}"));
    for stmt in crate::runtime::native_catalog::native_service_catalog_ddl() {
        sqlx::raw_sql(&stmt)
            .execute(&pool)
            .await
            .unwrap_or_else(|err| panic!("project native DDL failed: {err}\nSQL:\n{stmt}"));
    }
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("bootstrap project-local UDB system catalog");
    pool.close().await;
}

// ── G4: lock expiry reaper scans every active project ──────────────────────

const LOCK_PROJECT: &str = "lock-reaper-project-g4";

async fn insert_lapsed_lock(pool: &sqlx::PgPool, tenant: &str, name: &str) -> String {
    let lock_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO udb_lock.locks \
            (lock_id, tenant_id, lock_name, owner_id, fencing_token, status, \
             acquired_at, expires_at) \
         VALUES ($1::UUID, $2, $3, 'owner-g4', 3, 'HELD', \
             NOW() - INTERVAL '2 minutes', NOW() - INTERVAL '1 minute')",
    )
    .bind(&lock_id)
    .bind(tenant)
    .bind(name)
    .execute(pool)
    .await
    .unwrap_or_else(|err| panic!("insert lapsed lock {name}: {err}"));
    lock_id
}

async fn lock_status(pool: &sqlx::PgPool, lock_id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM udb_lock.locks WHERE lock_id = $1::UUID")
        .bind(lock_id)
        .fetch_one(pool)
        .await
        .expect("load lock status")
}

/// A lapsed lease written to a project bound to its OWN Postgres instance stays
/// HELD when the reaper sweeps only the default store, and is EXPIRED — with its
/// `lock.expired` outbox row in that project's database — once the reaper is
/// handed the active project list (the shape `serve()` runs).
#[tokio::test]
#[ignore = "requires live Postgres with CREATE DATABASE; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_lock_reaper_expires_leases_in_every_project_store -- --ignored --nocapture"]
async fn live_lock_reaper_expires_leases_in_every_project_store() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;

    let base_dsn = live_pg_dsn();
    let database = format!("udb_lock_reaper_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {}", quote_ident(&database)))
        .execute(&pool)
        .await
        .unwrap_or_else(|err| panic!("create project database {database}: {err}"));
    let project_dsn = dsn_for_database(&base_dsn, &database);
    provision_native_database(&project_dsn).await;

    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = base_dsn.clone();
    // Strict routing: the labelled instance serves ONLY its project and the
    // unlabelled primary serves only the default project.
    config.project_routing_mode = "strict".to_string();
    config.backend_instances = BackendInstanceConfig {
        instances: vec![BackendInstance {
            name: "lock-project-g4".to_string(),
            backend: "postgres".to_string(),
            role: BackendInstanceRole::ReadWrite,
            dsn: Some(project_dsn.clone()),
            dsn_env: None,
            enabled: true,
            read_weight: 1,
            write_weight: 1,
            labels: BTreeMap::from([("project_id".to_string(), LOCK_PROJECT.to_string())]),
            capabilities: BTreeSet::new(),
        }],
    };
    let runtime = DataBrokerRuntime::from_config(config).await;
    let outbox = runtime.config().cdc.outbox_relation();

    let project_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&project_dsn)
        .await
        .expect("connect project database");
    let tenant = Uuid::new_v4().to_string();
    let default_lock = insert_lapsed_lock(&pool, &tenant, "g4-default-lease").await;
    let project_lock = insert_lapsed_lock(&project_pool, &tenant, "g4-project-lease").await;

    // Control: a default-only sweep never reaches the project's own store.
    crate::runtime::service::lock_service::run_lock_expiry_all_projects(
        &runtime,
        Vec::new(),
        Some(&outbox),
        100,
    )
    .await
    .expect("default-only expiry pass");
    assert_eq!(lock_status(&pool, &default_lock).await, "EXPIRED");
    assert_eq!(
        lock_status(&project_pool, &project_lock).await,
        "HELD",
        "a default-only sweep must not touch the project-bound store"
    );

    // The all-projects sweep reaches the project instance.
    let expired = crate::runtime::service::lock_service::run_lock_expiry_all_projects(
        &runtime,
        vec![LOCK_PROJECT.to_string()],
        Some(&outbox),
        100,
    )
    .await
    .expect("all-projects expiry pass");
    assert!(
        expired >= 1,
        "the project-bound lapsed lease must be expired, got {expired}"
    );
    assert_eq!(
        lock_status(&project_pool, &project_lock).await,
        "EXPIRED",
        "the reaper must sweep the project's own lock store"
    );
    let events: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {outbox} \
         WHERE topic = 'udb.lock.lock.expired.v1' \
           AND payload->'payload'->>'tenant_id' = $1 \
           AND payload->'payload'->>'lock_name' = 'g4-project-lease'"
    ))
    .bind(&tenant)
    .fetch_one(&project_pool)
    .await
    .expect("count project-local expired events");
    assert_eq!(
        events, 1,
        "the expiry event commits in the project's own outbox, exactly once"
    );

    project_pool.close().await;
    drop(runtime);
    sqlx::query(&format!(
        "DROP DATABASE IF EXISTS {} WITH (FORCE)",
        quote_ident(&database)
    ))
    .execute(&pool)
    .await
    .unwrap_or_else(|err| panic!("drop project database {database}: {err}"));
}

// ── G6: TURN secret required in production ─────────────────────────────────

/// Restores the listed environment variables to their prior values on drop, so
/// a failing assertion cannot leak `UDB_ENV=production` into later tests.
struct EnvRestore(Vec<(&'static str, Option<String>)>);

impl EnvRestore {
    fn capture(names: &[&'static str]) -> Self {
        Self(
            names
                .iter()
                .map(|name| (*name, std::env::var(name).ok()))
                .collect(),
        )
    }
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        for (name, value) in &self.0 {
            // SAFETY: the CI live lane runs these tests single-threaded
            // (`--test-threads=1`) and the native-service lock is held.
            unsafe {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }
}

/// With `UDB_ENV=production` and no `UDB_TURN_SECRET`, `IssueCredentials` for a
/// real active peer fails `FailedPrecondition` / `TURN_NOT_CONFIGURED` instead
/// of minting with a fallback secret. With the dedicated secret set (still in
/// production) the credential is the HMAC of exactly that secret.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_turn_credentials_require_the_dedicated_secret_in_production -- --ignored --nocapture"]
async fn live_turn_credentials_require_the_dedicated_secret_in_production() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;

    // A real room + ACTIVE peer, created before production mode is switched on.
    let svc = webrtc_service(pool.clone()).await;
    let tenant_id = Uuid::new_v4().to_string();
    let room = RoomService::create_room(
        &svc,
        Request::new(webrtc_pb::CreateRoomRequest {
            tenant_id: tenant_id.clone(),
            name: "g6-turn".to_string(),
            max_participants: 4,
            ..Default::default()
        }),
    )
    .await
    .expect("create_room")
    .into_inner();
    let peer = PeerService::join_room(
        &svc,
        Request::new(webrtc_pb::JoinRoomRequest {
            tenant_id: tenant_id.clone(),
            room_id: room.room_id.clone(),
            display_name: "g6".to_string(),
            ..Default::default()
        }),
    )
    .await
    .expect("join_room")
    .into_inner()
    .peer
    .expect("joined peer");
    let issue = || webrtc_pb::IssueCredentialsRequest {
        tenant_id: tenant_id.clone(),
        room_id: room.room_id.clone(),
        peer_id: peer.peer_id.clone(),
        ttl_seconds: 300,
    };

    let _restore = EnvRestore::capture(&["UDB_ENV", "UDB_TURN_SECRET"]);
    // SAFETY: single-threaded live lane; `_restore` puts both back on drop.
    unsafe {
        std::env::set_var("UDB_ENV", "production");
        std::env::remove_var("UDB_TURN_SECRET");
    }
    assert!(crate::runtime::security::SecurityConfig::current().is_production());

    // The TURN secret is resolved when the service is built, as at startup.
    let production = crate::runtime::service::webrtc_service::WebrtcServiceImpl::new()
        .with_postgres(Some(pool.clone()));
    let refused = TurnService::issue_credentials(&production, Request::new(issue()))
        .await
        .expect_err("production without UDB_TURN_SECRET must not mint TURN credentials");
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
    assert_eq!(
        refused
            .metadata()
            .get("error-reason")
            .and_then(|value| value.to_str().ok()),
        Some("TURN_NOT_CONFIGURED"),
        "{refused}"
    );
    assert!(
        refused.message().contains("UDB_TURN_SECRET"),
        "the refusal must name the missing secret: {refused}"
    );

    // Production WITH the dedicated secret: the credential is HMAC(secret).
    let dedicated = "g6-dedicated-turn-secret";
    // SAFETY: as above.
    unsafe {
        std::env::set_var("UDB_TURN_SECRET", dedicated);
    }
    let configured = crate::runtime::service::webrtc_service::WebrtcServiceImpl::new()
        .with_postgres(Some(pool.clone()));
    let issued = TurnService::issue_credentials(&configured, Request::new(issue()))
        .await
        .expect("production with UDB_TURN_SECRET issues credentials")
        .into_inner();
    let (expiry, principal) = issued
        .username
        .split_once(':')
        .expect("TURN username is <expiry>:<principal>");
    let expiry: i64 = expiry.parse().expect("TURN username expiry");
    let (_, expected) =
        crate::runtime::security::turn_rest_credential(dedicated.as_bytes(), principal, expiry);
    assert_eq!(
        issued.credential, expected,
        "the credential must be keyed by UDB_TURN_SECRET"
    );
    let (_, dev_fallback) =
        crate::runtime::security::turn_rest_credential(b"udb-dev-turn-secret", principal, expiry);
    assert_ne!(issued.credential, dev_fallback);
    if let Ok(master) = std::env::var("UDB_ENCRYPTION_KEY") {
        let (_, from_master) =
            crate::runtime::security::turn_rest_credential(master.as_bytes(), principal, expiry);
        assert_ne!(
            issued.credential, from_master,
            "the TURN secret must never be the data-at-rest master key"
        );
    }
}

// ── G7: compliance evidence export ─────────────────────────────────────────

#[cfg(feature = "s3")]
const EVIDENCE_AUDIT_RELATION: &str = "udb_system.evidence_g7_audit";

#[cfg(feature = "s3")]
async fn list_object_keys(client: &aws_sdk_s3::Client, bucket: &str, prefix: &str) -> Vec<String> {
    let listed = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await
        .unwrap_or_else(|err| panic!("list {bucket}/{prefix}: {err}"));
    let mut keys: Vec<String> = listed
        .contents()
        .iter()
        .filter_map(|object| object.key().map(str::to_string))
        .collect();
    keys.sort();
    keys
}

/// The export writes nothing (no state, no objects) while disabled. Enabled, it
/// drains the general data-plane audit table (`UDB_AUDIT_SINK=postgres`) into a
/// chain-hashed JSONL bundle + manifest under `<prefix>/data-audit/` in MinIO,
/// advances that stream's durable watermark, and a second pass exports nothing.
#[cfg(feature = "s3")]
#[tokio::test]
#[ignore = "requires live Postgres + MinIO; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_evidence_export_writes_the_general_audit_table_only_when_enabled -- --ignored --nocapture"]
async fn live_evidence_export_writes_the_general_audit_table_only_when_enabled() {
    use crate::runtime::catalog::DEFAULT_PROJECT_ID;
    use crate::runtime::core::setup_data::object_request_json;
    use crate::runtime::evidence_export::{EvidenceExportConfig, run_evidence_export_once};
    use crate::runtime::service::native_helpers::DEFAULT_OBJECT_BUCKET;

    if require_live_dsn("UDB_MINIO_ENDPOINT").is_none() {
        eprintln!("set UDB_MINIO_ENDPOINT to run the evidence export MinIO seam");
        return;
    }
    let _guard = live_native_service_db_lock().lock().await;
    // SAFETY: single-threaded live lane; only fills unset MinIO defaults.
    unsafe {
        for (name, default) in [
            ("UDB_MINIO_ACCESS_KEY", "minio"),
            ("UDB_MINIO_SECRET_KEY", "minio123"),
            ("UDB_MINIO_REGION", "us-east-1"),
        ] {
            if std::env::var(name).is_err() {
                std::env::set_var(name, default);
            }
        }
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;

    // The general audit table exactly as the durable Postgres audit sink creates it.
    let marker = Uuid::new_v4().simple().to_string();
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA IF NOT EXISTS udb_system; \
         DROP TABLE IF EXISTS {rel}; \
         CREATE TABLE {rel} ( \
             audit_id BIGSERIAL PRIMARY KEY, \
             event_type VARCHAR(80) NOT NULL DEFAULT '', \
             tenant_id VARCHAR(64) NOT NULL DEFAULT '', \
             user_id VARCHAR(200) NOT NULL DEFAULT '', \
             correlation_id VARCHAR(120) NOT NULL DEFAULT '', \
             purpose VARCHAR(120) NOT NULL DEFAULT '', \
             resource_uri VARCHAR(400) NOT NULL DEFAULT '', \
             checksum_sha256 VARCHAR(80) NOT NULL DEFAULT '', \
             occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()); \
         INSERT INTO {rel} (event_type, tenant_id, user_id, purpose, resource_uri, occurred_at) \
         VALUES \
             ('data.upsert', 'g7-tenant', 'g7-user-a', 'write', 'udb://g7/{marker}/a', \
              '2026-01-01T00:00:00Z'), \
             ('data.delete', 'g7-tenant', 'g7-user-b', 'erase', 'udb://g7/{marker}/b', \
              '2026-01-01T00:00:01Z');",
        rel = EVIDENCE_AUDIT_RELATION
    ))
    .execute(&pool)
    .await
    .expect("create general audit table");

    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = live_pg_dsn();
    config.audit_sink.kind = crate::runtime::config::AuditSinkKind::Postgres;
    config.audit_sink.pg_table = Some(EVIDENCE_AUDIT_RELATION.to_string());
    let runtime = DataBrokerRuntime::from_config(config).await;
    let client = runtime
        .s3_for_instance_for_project(None, DEFAULT_PROJECT_ID)
        .expect("MinIO client")
        .clone();
    let bucket = DEFAULT_OBJECT_BUCKET;
    let prefix = format!("g7-evidence-{marker}");
    let mut export = EvidenceExportConfig {
        backend: "minio".to_string(),
        bucket: bucket.to_string(),
        prefix: prefix.clone(),
        project: DEFAULT_PROJECT_ID.to_string(),
        batch_limit: 500,
        interval: Duration::from_secs(300),
        enabled: false,
    };

    // Disabled: nothing exported, no state table, no objects.
    let disabled = run_evidence_export_once(&runtime, &pool, &export)
        .await
        .expect("disabled export pass");
    assert_eq!(disabled, 0);
    let state_table: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('udb_system.evidence_export_state')::text")
            .fetch_one(&pool)
            .await
            .expect("probe evidence state table");
    assert!(
        state_table.is_none(),
        "a disabled export must not touch durable state"
    );
    assert!(list_object_keys(&client, bucket, &prefix).await.is_empty());

    // Enabled: the general audit table is exported.
    export.enabled = true;
    let exported = run_evidence_export_once(&runtime, &pool, &export)
        .await
        .expect("enabled export pass");
    assert!(
        exported >= 2,
        "both general audit rows must be exported, got {exported}"
    );
    let data_prefix = format!("{prefix}/data-audit/");
    let keys = list_object_keys(&client, bucket, &data_prefix).await;
    assert_eq!(keys.len(), 2, "one bundle + one manifest: {keys:?}");
    let bundle_key = keys
        .iter()
        .find(|key| key.ends_with(".jsonl"))
        .expect("evidence bundle object")
        .clone();
    let manifest_key = keys
        .iter()
        .find(|key| key.ends_with(".manifest.json"))
        .expect("evidence manifest object")
        .clone();

    let get = |key: &str| object_request_json("get", bucket, key, "");
    let bundle = runtime
        .get_object_backend_target_for_project(
            "minio",
            None,
            DEFAULT_PROJECT_ID,
            &get(bundle_key.as_str()),
        )
        .await
        .expect("read evidence bundle back");
    let lines: Vec<serde_json::Value> = String::from_utf8(bundle)
        .expect("bundle is UTF-8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("bundle line is JSON"))
        .collect();
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert_eq!(
        lines[0]["target_resource"],
        serde_json::json!(format!("udb://g7/{marker}/a"))
    );
    assert_eq!(lines[0]["operation"], serde_json::json!("write"));
    assert_eq!(lines[0]["actor"], serde_json::json!("g7-user-a"));
    assert_eq!(
        lines[1]["target_resource"],
        serde_json::json!(format!("udb://g7/{marker}/b"))
    );
    assert_eq!(
        lines[1]["prev_hash"], lines[0]["chain_hash"],
        "the bundle is one hash chain"
    );

    let manifest_bytes = runtime
        .get_object_backend_target_for_project(
            "minio",
            None,
            DEFAULT_PROJECT_ID,
            &get(manifest_key.as_str()),
        )
        .await
        .expect("read evidence manifest back");
    let manifest: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).expect("manifest is JSON");
    assert_eq!(
        manifest["source_relation"],
        serde_json::json!(EVIDENCE_AUDIT_RELATION)
    );
    assert_eq!(manifest["record_count"], serde_json::json!(2));
    assert_eq!(manifest["chain"]["head"], lines[1]["chain_hash"]);
    assert_eq!(
        manifest["evidence_object"]["object_key"],
        serde_json::json!(bundle_key)
    );

    // Durable watermark for the general-audit stream.
    let (chain_head, exported_count): (String, i64) = sqlx::query_as(
        "SELECT chain_head, exported_count FROM udb_system.evidence_export_state \
         WHERE worker = $1",
    )
    .bind(format!(
        "{}:data_audit",
        crate::runtime::singleton::WORKER_EVIDENCE_EXPORT
    ))
    .fetch_one(&pool)
    .await
    .expect("general-audit export watermark row");
    assert_eq!(serde_json::json!(chain_head), lines[1]["chain_hash"]);
    assert_eq!(exported_count, 2);

    // Past the watermark: nothing new to export, no new objects.
    let again = run_evidence_export_once(&runtime, &pool, &export)
        .await
        .expect("second export pass");
    assert_eq!(again, 0, "a second pass must not re-export the window");
    assert_eq!(
        list_object_keys(&client, bucket, &data_prefix).await.len(),
        2
    );

    for key in list_object_keys(&client, bucket, &prefix).await {
        runtime
            .delete_object_backend_target(
                "minio",
                None,
                DEFAULT_PROJECT_ID,
                &object_request_json("delete", bucket, &key, ""),
            )
            .await
            .unwrap_or_else(|err| panic!("delete evidence object {key}: {err}"));
    }
    sqlx::query(&format!("DROP TABLE IF EXISTS {EVIDENCE_AUDIT_RELATION}"))
        .execute(&pool)
        .await
        .expect("drop general audit table");
}
