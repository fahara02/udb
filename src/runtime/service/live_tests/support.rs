use crate::runtime::service::asset_service::AssetServiceImpl;
use crate::runtime::service::storage_service::StorageServiceImpl;
use crate::runtime::service::webrtc_service::WebrtcServiceImpl;
use std::sync::OnceLock;
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
    for schema in crate::runtime::native_catalog::native_schema_names() {
        let stmt = format!("DROP SCHEMA IF EXISTS {} CASCADE", quote_ident(&schema));
        sqlx::query(&stmt)
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("drop native schema {schema}: {err}"));
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

pub(super) fn storage_service(pool: sqlx::PgPool) -> StorageServiceImpl {
    StorageServiceImpl::new().with_postgres(Some(pool))
}

pub(super) fn asset_service(pool: sqlx::PgPool) -> AssetServiceImpl {
    AssetServiceImpl::new().with_postgres(Some(pool))
}

pub(super) fn webrtc_service(pool: sqlx::PgPool) -> WebrtcServiceImpl {
    WebrtcServiceImpl::new().with_postgres(Some(pool))
}

/// Register a PENDING storage file for `tenant_id` and return its `file_id`.
pub(super) async fn seed_storage_file(pool: &sqlx::PgPool, tenant_id: &str) -> String {
    use crate::proto::udb::core::storage::services::v1 as storage_pb;
    use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageService;

    storage_service(pool.clone())
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
