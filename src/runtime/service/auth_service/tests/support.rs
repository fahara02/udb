use super::super::{ApiKeyServiceImpl, AuthnServiceImpl, AuthzServiceImpl};
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::authn::{
    AuthnConfig, PostgresApiKeyStore, PostgresSessionStore, PostgresUserStore, SessionStore,
};
use crate::runtime::authz::AuthzSnapshot;
use crate::runtime::security::SecurityConfig;
use crate::runtime::service::analytics_service::AnalyticsServiceImpl;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

pub(super) fn live_pg_dsn() -> String {
    std::env::var("UDB_LIVE_AUTH_PG_DSN")
        .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
        .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string())
}

pub(super) fn live_auth_db_lock() -> &'static tokio::sync::Mutex<()> {
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
        .unwrap_or_else(|err| panic!("connect live auth postgres at {dsn}: {err}"))
}

pub(super) async fn cleanup_native_auth_db(pool: &sqlx::PgPool) {
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

pub(super) async fn migrate_native_auth_db(pool: &sqlx::PgPool) {
    cleanup_native_auth_db(pool).await;
    let ddl = crate::runtime::native_catalog::native_service_catalog_ddl();
    assert!(
        !ddl.is_empty(),
        "native auth DDL must be generated from embedded UDB protos"
    );
    for stmt in ddl {
        sqlx::raw_sql(&stmt)
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("native auth DDL failed: {err}\nSQL:\n{stmt}"));
    }
}

pub(super) fn authn_service(pool: sqlx::PgPool) -> AuthnServiceImpl {
    // OTP cooldown disabled here so existing tests can send/resend OTPs back to
    // back; the cooldown itself is covered by a dedicated test that opts in.
    let config = AuthnConfig {
        session_enabled: true,
        session_hash_secret: "live-auth-test-secret".to_string(),
        otp_cooldown_secs: 0,
        ..AuthnConfig::default()
    };
    let sessions: Arc<dyn SessionStore> = Arc::new(PostgresSessionStore::new(pool.clone(), ""));
    AuthnServiceImpl::with_stores(
        config,
        SecurityConfig::current(),
        sessions,
        Arc::new(PostgresApiKeyStore::new(pool.clone(), "")),
        Arc::new(PostgresUserStore::new(pool.clone(), "")),
    )
    .with_postgres(Some(pool))
}

/// Like [`authn_service`] but with an explicit OTP cooldown, for the cooldown test.
pub(super) fn authn_service_with_cooldown(
    pool: sqlx::PgPool,
    otp_cooldown_secs: u64,
) -> AuthnServiceImpl {
    let config = AuthnConfig {
        session_enabled: true,
        session_hash_secret: "live-auth-test-secret".to_string(),
        otp_cooldown_secs,
        ..AuthnConfig::default()
    };
    let sessions: Arc<dyn SessionStore> = Arc::new(PostgresSessionStore::new(pool.clone(), ""));
    AuthnServiceImpl::with_stores(
        config,
        SecurityConfig::current(),
        sessions,
        Arc::new(PostgresApiKeyStore::new(pool.clone(), "")),
        Arc::new(PostgresUserStore::new(pool.clone(), "")),
    )
    .with_postgres(Some(pool))
}

pub(super) fn api_key_service(pool: sqlx::PgPool) -> ApiKeyServiceImpl {
    let config = AuthnConfig {
        session_enabled: true,
        session_hash_secret: "live-auth-test-secret".to_string(),
        ..AuthnConfig::default()
    };
    ApiKeyServiceImpl::with_store(config, Arc::new(PostgresApiKeyStore::new(pool.clone(), "")))
        .with_postgres(Some(pool))
}

pub(super) fn authz_service(pool: sqlx::PgPool) -> AuthzServiceImpl {
    AuthzServiceImpl::new(AuthzSnapshot::default()).with_postgres(Some(pool))
}

pub(super) fn analytics_service(pool: sqlx::PgPool) -> AnalyticsServiceImpl {
    AnalyticsServiceImpl::new().with_postgres(Some(pool))
}

pub(super) fn tenant_service(
    pool: sqlx::PgPool,
) -> super::super::super::tenant_service::TenantServiceImpl {
    super::super::super::tenant_service::TenantServiceImpl::new().with_postgres(Some(pool))
}

pub(super) fn notification_service(
    pool: sqlx::PgPool,
) -> super::super::super::notification_service::NotificationServiceImpl {
    super::super::super::notification_service::NotificationServiceImpl::new()
        .with_postgres(Some(pool))
}

/// Notification service wired to the shared outbox so `SendNotification`
/// publishes to `udb_system.outbox_events` (→ CDC → Kafka).
pub(super) fn notification_service_with_outbox(
    pool: sqlx::PgPool,
) -> super::super::super::notification_service::NotificationServiceImpl {
    super::super::super::notification_service::NotificationServiceImpl::new()
        .with_postgres(Some(pool))
        .with_outbox(Some("udb_system.outbox_events".to_string()))
}

/// Create the shared transactional-outbox table the CDC engine tails (same shape
/// as the system bootstrap / `prepare_outbox_envelope` insert).
pub(super) async fn ensure_outbox_table(pool: &sqlx::PgPool) {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS udb_system")
        .execute(pool)
        .await
        .expect("create udb_system schema");
    sqlx::query("DROP TABLE IF EXISTS udb_system.outbox_events CASCADE")
        .execute(pool)
        .await
        .expect("drop outbox");
    sqlx::query(
        "CREATE TABLE udb_system.outbox_events ( \
            event_seq     BIGSERIAL PRIMARY KEY, \
            event_id      UUID NOT NULL UNIQUE, \
            topic         TEXT NOT NULL, \
            partition_key TEXT NOT NULL DEFAULT '', \
            payload       JSONB NOT NULL, \
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW() )",
    )
    .execute(pool)
    .await
    .expect("create outbox table");
}

/// Seed a default tenant through the native `TenantService` and return its id.
pub(super) async fn seed_default_tenant(pool: &sqlx::PgPool) -> String {
    use crate::proto::udb::core::tenant::services::v1 as tenant_pb;
    use crate::proto::udb::core::tenant::services::v1::tenant_service_server::TenantService;
    let svc = tenant_service(pool.clone());
    let created = svc
        .create_tenant(Request::new(tenant_pb::CreateTenantRequest {
            code: format!("acme_{}", Uuid::new_v4().simple()),
            name: "Acme Default".to_string(),
            r#type: "ORGANIZATION".to_string(),
            config: r#"{"plan":"enterprise"}"#.to_string(),
            ..Default::default()
        }))
        .await
        .expect("seed default tenant")
        .into_inner();
    assert!(Uuid::parse_str(&created.tenant_id).is_ok());
    created.tenant_id
}

/// Seed channel-wide notification subscriptions (opt-ins) for a user across
/// every channel through the native `NotificationService`.
pub(super) async fn seed_notification_subscriptions(
    pool: &sqlx::PgPool,
    user_id: &str,
    tenant_id: &str,
) {
    use crate::proto::udb::core::notification::entity::v1::NotificationChannel as Channel;
    use crate::proto::udb::core::notification::services::v1 as notif_pb;
    use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationService;
    let svc = notification_service(pool.clone());
    for channel in [
        Channel::Email,
        Channel::Sms,
        Channel::Push,
        Channel::InApp,
        Channel::Webhook,
    ] {
        svc.set_preference(Request::new(notif_pb::SetPreferenceRequest {
            user_id: user_id.to_string(),
            tenant_id: tenant_id.to_string(),
            channel: channel as i32,
            event_type: String::new(),
            is_opted_out: false,
            ..Default::default()
        }))
        .await
        .expect("seed notification subscription");
    }
}

pub(super) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub(super) fn issued_test_otp_code(otp_id: &str) -> String {
    super::super::authn::test_otp_codes()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(otp_id)
        .cloned()
        .unwrap_or_else(|| panic!("no issued OTP code captured for otp_id {otp_id}"))
}

pub(super) async fn verify_issued_otp(
    svc: &AuthnServiceImpl,
    otp_id: &str,
) -> authn_pb::VerifyOtpResponse {
    svc.verify_otp(Request::new(authn_pb::VerifyOtpRequest {
        otp_id: otp_id.to_string(),
        code: issued_test_otp_code(otp_id),
    }))
    .await
    .expect("verify issued OTP")
    .into_inner()
}

pub(super) async fn create_verified_user(
    svc: &AuthnServiceImpl,
    prefix: &str,
    password: &str,
) -> authn_entity_pb::User {
    let suffix = Uuid::new_v4().simple().to_string();
    let created = svc
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("{prefix}_{suffix}"),
            email: format!("{prefix}_{suffix}@example.com"),
            password: password.to_string(),
            tenant_id: "acme".to_string(),
            full_name: format!("{prefix} Live"),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("create verified live user")
        .into_inner();
    let user = created.user.expect("created user");
    let verified = verify_issued_otp(svc, &created.otp_id).await;
    assert!(verified.verified);
    user
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

#[cfg(feature = "kafka")]
pub(super) async fn ensure_kafka_topic(brokers: &str, topic: &str) {
    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap_or_else(|err| panic!("create Kafka admin client for {brokers}: {err}"));
    match admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
    {
        Ok(results) => {
            for result in results {
                if let Err((name, code)) = result
                    && !format!("{code:?}").contains("TopicAlreadyExists")
                {
                    panic!("create Kafka topic {name} failed: {code:?}");
                }
            }
        }
        Err(err) => panic!("create Kafka topic {topic} request failed: {err}"),
    }
    admin
        .inner()
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .unwrap_or_else(|err| panic!("Kafka topic {topic} metadata was not visible: {err}"));
}
