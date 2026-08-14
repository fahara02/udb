use crate::runtime::config::UdbConfig;
use crate::runtime::core::setup_data::object_request_json;
use crate::runtime::service::DataBrokerService;
use crate::runtime::service::asset_service::AssetServiceImpl;
use crate::runtime::service::native_helpers::DEFAULT_OBJECT_BUCKET;
use crate::runtime::service::scheduler_service::SchedulerServiceImpl;
use crate::runtime::service::storage_service::StorageServiceImpl;
use crate::runtime::service::webrtc_service::WebrtcServiceImpl;
use crate::runtime::service::workflow_service::WorkflowServiceImpl;
use crate::runtime::{DataBrokerRuntime, native_catalog};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

pub(super) fn live_pg_dsn() -> String {
    std::env::var("UDB_LIVE_NATIVE_PG_DSN")
        .or_else(|_| std::env::var("UDB_LIVE_AUTH_PG_DSN"))
        .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
        .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string())
}

pub(super) fn live_native_service_db_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

pub(super) async fn live_pg_pool() -> sqlx::PgPool {
    let dsn = live_pg_dsn();
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&dsn)
        .await
        .unwrap_or_else(|err| panic!("connect live native-service postgres at {dsn}: {err}"))
}

pub(super) async fn cleanup_native_service_db(pool: &sqlx::PgPool) {
    // Drop EVERY native `udb_*` schema present plus the lifecycle's `public`
    // migration tables — NOT just the migration-enabled subset returned by
    // `native_schema_names()`. `native_service_catalog_ddl()` creates schemas that
    // subset omits (e.g. the control-plane registry `udb_control`) and the
    // migration-tracking tables live in `public`, so listing only the subset
    // leaks them across runs: the next `CREATE SCHEMA` fails with a duplicate and
    // control-plane rows accumulate. Enumerating live objects drops whatever
    // migrate created, regardless of that filter.
    let schemas: Vec<String> = sqlx::query_scalar(
        "SELECT nspname FROM pg_namespace WHERE nspname LIKE 'udb\\_%' ESCAPE '\\'",
    )
    .fetch_all(pool)
    .await
    .expect("list native udb_* schemas");
    for schema in schemas {
        let stmt = format!("DROP SCHEMA IF EXISTS {} CASCADE", quote_ident(&schema));
        sqlx::query(&stmt)
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("drop native schema {schema}: {err}"));
    }
    // Skip tables an EXTENSION owns: PostGIS installs `public.spatial_ref_sys`,
    // and dropping it fails ("extension postgis requires it"), which would fail
    // every later live test on a database where any test enabled PostGIS.
    // Extension-owned tables are not test residue, so they are not ours to drop.
    let public_tables: Vec<String> = sqlx::query_scalar(
        "SELECT t.tablename FROM pg_tables t \
         JOIN pg_class c ON c.relname = t.tablename \
         JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.schemaname \
         WHERE t.schemaname = 'public' \
           AND NOT EXISTS ( \
             SELECT 1 FROM pg_depend d \
             WHERE d.objid = c.oid AND d.classid = 'pg_class'::regclass AND d.deptype = 'e' \
           )",
    )
    .fetch_all(pool)
    .await
    .expect("list public tables");
    for table in public_tables {
        let stmt = format!(
            "DROP TABLE IF EXISTS public.{} CASCADE",
            quote_ident(&table)
        );
        sqlx::query(&stmt)
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("drop public table {table}: {err}"));
    }
    sqlx::query("DROP EXTENSION IF EXISTS pg_partman CASCADE")
        .execute(pool)
        .await
        .expect("drop pg_partman extension");
    sqlx::query("DROP SCHEMA IF EXISTS partman CASCADE")
        .execute(pool)
        .await
        .expect("drop partman schema");
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(super) async fn migrate_native_service_db(pool: &sqlx::PgPool) {
    cleanup_native_service_db(pool).await;
    let ddl = crate::runtime::native_catalog::native_service_catalog_ddl();
    assert!(
        !ddl.is_empty(),
        "native service DDL must be generated from embedded UDB protos"
    );
    for stmt in ddl {
        sqlx::raw_sql(&stmt)
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("native service DDL failed: {err}\nSQL:\n{stmt}"));
    }
}

pub(super) async fn reset_native_outbox(pool: &sqlx::PgPool) {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS udb_system")
        .execute(pool)
        .await
        .expect("create udb_system schema");
    sqlx::query("DROP TABLE IF EXISTS udb_system.outbox_events CASCADE")
        .execute(pool)
        .await
        .expect("drop native-service test outbox");
    sqlx::query(
        "CREATE TABLE udb_system.outbox_events ( \
            event_seq BIGSERIAL PRIMARY KEY, event_id UUID NOT NULL UNIQUE, \
            topic TEXT NOT NULL, partition_key TEXT NOT NULL DEFAULT '', \
            payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW() )",
    )
    .execute(pool)
    .await
    .expect("create native-service test outbox");
}

pub(super) async fn live_runtime() -> Arc<DataBrokerRuntime> {
    Arc::new(DataBrokerRuntime::from_config(live_native_config()).await)
}

async fn native_broker_service() -> DataBrokerService {
    let config = live_native_config();
    DataBrokerService::with_runtime(
        native_catalog::native_manifest().clone(),
        DataBrokerRuntime::from_config(config).await,
    )
}

fn live_native_config() -> UdbConfig {
    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = live_pg_dsn();
    if config.kafka_brokers.is_none()
        && let Ok(brokers) = std::env::var("UDB_INTEGRATION_KAFKA_BROKERS")
        && !brokers.trim().is_empty()
    {
        config.kafka_brokers = Some(brokers);
    }
    config
}

pub(super) async fn storage_service(_pool: sqlx::PgPool) -> StorageServiceImpl {
    native_broker_service().await.build_storage_service()
}

pub(super) async fn scheduler_service(_pool: sqlx::PgPool) -> SchedulerServiceImpl {
    native_broker_service().await.build_scheduler_service()
}

pub(super) async fn workflow_service(_pool: sqlx::PgPool) -> WorkflowServiceImpl {
    native_broker_service().await.build_workflow_service()
}

pub(super) async fn asset_service(_pool: sqlx::PgPool) -> AssetServiceImpl {
    native_broker_service().await.build_asset_service()
}

pub(super) async fn search_service(
    _pool: sqlx::PgPool,
) -> crate::runtime::service::search_service::SearchServiceImpl {
    native_broker_service().await.build_search_service()
}

pub(super) async fn webrtc_service(_pool: sqlx::PgPool) -> WebrtcServiceImpl {
    native_broker_service().await.build_webrtc_service()
}

/// Build the native `LockService` on the SAME live PG the data-plane broker uses,
/// so a lock acquired/released through the real LockService RPCs is the exact
/// durable row the data-plane fencing gate (`enforce_fencing_token_in_tx`) reads.
/// Mirrors `storage_service` / `asset_service`.
pub(super) async fn lock_service(
    _pool: sqlx::PgPool,
) -> crate::runtime::service::lock_service::LockServiceImpl {
    native_broker_service().await.build_lock_service()
}

pub(super) async fn put_storage_object(object_key: &str, content_type: &str, bytes: &[u8]) {
    let runtime = DataBrokerRuntime::from_config(live_native_config()).await;
    let request = object_request_json("put", DEFAULT_OBJECT_BUCKET, object_key, content_type);
    runtime
        .put_object_backend_target("minio", None, &request, bytes.to_vec())
        .await
        .expect("put storage object bytes");
}

/// Register a PENDING storage file for `tenant_id` and return its `file_id`.
pub(super) async fn seed_storage_file(pool: &sqlx::PgPool, tenant_id: &str) -> String {
    use crate::proto::udb::core::storage::services::v1 as storage_pb;
    use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageService;

    storage_service(pool.clone())
        .await
        .register_upload(tonic::Request::new(storage_pb::RegisterUploadRequest {
            tenant_id: tenant_id.to_string(),
            filename: "seed.txt".to_string(),
            content_type: "text/plain".to_string(),
            file_type: "DOCUMENT".to_string(),
            ..Default::default()
        }))
        .await
        .expect("seed storage file")
        .into_inner()
        .file_id
}

/// §1 read-after-write served-path assertion (13.7.1.2). A create-returning-id RPC
/// must return a NON-EMPTY id that is IMMEDIATELY gettable on the SAME served path
/// with the SAME tenant/project metadata a client uses. `created_id` is the id the
/// create RPC returned; `get` performs the served Get with that id and resolves
/// `true` when the row is present. Reverting any read-after-write guarantee (a
/// create that returns an id not gettable) fails this assertion.
pub(super) async fn assert_create_then_get<F, Fut>(label: &str, created_id: &str, get: F)
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Result<bool, tonic::Status>>,
{
    assert!(
        !created_id.is_empty(),
        "{label}: create RPC must return a non-empty id"
    );
    let present = get(created_id.to_string())
        .await
        .unwrap_or_else(|err| panic!("{label}: served Get for created id failed: {err}"));
    assert!(
        present,
        "{label}: id returned by create ({created_id}) must be immediately gettable on the served path"
    );
}

pub(super) async fn assert_native_table_columns(
    pool: &sqlx::PgPool,
    message_type: &str,
    fields: &[&str],
) {
    let (schema, table) = crate::runtime::native_catalog::native_relation(message_type)
        .unwrap_or_else(|| panic!("missing native relation for {message_type}"));
    let model = crate::runtime::native_catalog::native_model(message_type, fields);
    let regclass = format!("{schema}.{table}");
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(&regclass)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|err| panic!("check native relation {regclass}: {err}"));
    assert!(
        exists,
        "native relation {regclass} should exist after proto migration"
    );

    for field in fields {
        let column = model.column(field);
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
            )",
        )
        .bind(&schema)
        .bind(&table)
        .bind(column)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|err| panic!("check native column {regclass}.{column}: {err}"));
        assert!(
            exists,
            "native column {message_type}.{field} -> {schema}.{table}.{column} should exist"
        );
    }
}
