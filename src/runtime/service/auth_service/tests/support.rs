use super::super::{ApiKeyServiceImpl, AuthnServiceImpl, AuthzServiceImpl};
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::authn::{
    AuthnConfig, PostgresApiKeyStore, PostgresSessionStore, PostgresUserStore, SessionStore,
};
use crate::runtime::config::{DbConfig, UdbConfig};
use crate::runtime::security::SecurityConfig;
use crate::runtime::service::DataBrokerService;
use crate::runtime::service::analytics_service::AnalyticsServiceImpl;
use crate::runtime::{DataBrokerRuntime, native_catalog};
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
    // Drop EVERY native `udb_*` schema present, not just the migration-enabled
    // subset returned by `native_schema_names()`. `native_service_catalog_ddl()`
    // creates schemas that subset omits (e.g. the control-plane registry
    // `udb_control`), so listing only the subset LEAKS them across runs: the next
    // run's `CREATE SCHEMA` then fails with a duplicate-namespace error and
    // control-plane rows (the `cp-world-N` versions) accumulate. Enumerating the
    // live schemas drops whatever migrate created, regardless of that filter.
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
    // The migration-tracking tables (`schema_migrations`, `proto_schema_versions`,
    // `migration_error_log`, `migration_runtime_state`) are created in `public` by
    // the startup lifecycle, OUTSIDE the `udb_*` schemas — so they also leak across
    // runs and a re-migrate collides on their row types. The test DB's `public`
    // holds only these native tables, so dropping all of them is safe and avoids
    // hardcoding their names.
    let public_tables: Vec<String> =
        sqlx::query_scalar("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
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
    // The data-plane paths some auth tests exercise (API-key-authenticated
    // Upsert/Select over the data-only listener) require the `udb_system`
    // catalog — the row-revision store (CAS), CDC lock/outbox, idempotency keys.
    // `cleanup_native_auth_db` drops every `udb_*` schema, so re-provision the
    // system catalog here (idempotent `CREATE ... IF NOT EXISTS`).
    crate::runtime::system::ensure_system_catalog(pool)
        .await
        .expect("bootstrap udb_system catalog for native auth tests");
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

/// Like [`authn_service`] but with RS256 JWT signing/verification keys configured
/// (from `testdata/`), so `login` issues a real UDB access token and
/// `ValidateToken`(JWT) can verify it — used by the JWT-lifecycle tests.
pub(super) fn authn_service_with_jwt(pool: sqlx::PgPool) -> AuthnServiceImpl {
    let config = AuthnConfig {
        session_enabled: true,
        session_hash_secret: "live-auth-test-secret".to_string(),
        otp_cooldown_secs: 0,
        ..AuthnConfig::default()
    };
    let security = SecurityConfig {
        jwt_private_key: Some(include_str!("../../../testdata/jwt_rs256_private.pem").to_string()),
        jwt_public_key: Some(include_str!("../../../testdata/jwt_rs256_public.pem").to_string()),
        ..SecurityConfig::default()
    };
    let sessions: Arc<dyn SessionStore> = Arc::new(PostgresSessionStore::new(pool.clone(), ""));
    AuthnServiceImpl::with_stores(
        config,
        security,
        sessions,
        Arc::new(PostgresApiKeyStore::new(pool.clone(), "")),
        Arc::new(PostgresUserStore::new(pool.clone(), "")),
    )
    .with_postgres(Some(pool))
}

/// An [`AuthEventSink`] whose transactional write always fails. Injected into a
/// service to prove enterprise auth-mutation atomicity (§7): when the audit/outbox
/// write inside the handler's transaction fails, the handler must propagate the
/// error and the whole transaction (the mutation + the event) must roll back, so
/// the DB is left exactly as it was before the call.
pub(super) struct FailingAuthEventSink;

#[async_trait::async_trait]
impl super::super::events::AuthEventSink for FailingAuthEventSink {
    async fn emit(&self, _event: super::super::events::AuthEvent) -> Result<(), String> {
        Err("forced audit emit failure (atomicity test)".to_string())
    }

    async fn write_in_tx(
        &self,
        _conn: &mut sqlx::PgConnection,
        _event: super::super::events::AuthEvent,
    ) -> Result<(), String> {
        Err("forced transactional audit write failure (atomicity test)".to_string())
    }
}

/// Like [`authn_service`] but with an event sink whose transactional write always
/// fails, so a handler's `emit_event_in_tx` returns an error and rolls back.
pub(super) fn authn_service_with_failing_sink(pool: sqlx::PgPool) -> AuthnServiceImpl {
    authn_service(pool).with_event_sink(Arc::new(FailingAuthEventSink))
}

pub(super) fn api_key_service(pool: sqlx::PgPool) -> ApiKeyServiceImpl {
    let config = AuthnConfig {
        session_enabled: true,
        session_hash_secret: "live-auth-test-secret".to_string(),
        ..AuthnConfig::default()
    };
    ApiKeyServiceImpl::with_store(config, Arc::new(PostgresApiKeyStore::new(pool.clone(), "")))
        // AUTH-006: wire the user store so live tests exercise the best-effort
        // service-identity lineage derivation (soft — unknown owners still work).
        .with_user_store(Arc::new(PostgresUserStore::new(pool.clone(), "")))
        .with_postgres(Some(pool))
}

async fn native_broker_service() -> DataBrokerService {
    let config = UdbConfig {
        primary: DbConfig {
            direct_dsn: live_pg_dsn(),
            ..DbConfig::default()
        },
        ..UdbConfig::default()
    };
    DataBrokerService::with_runtime(
        native_catalog::native_manifest().clone(),
        DataBrokerRuntime::from_config(config).await,
    )
}

pub(super) async fn authz_service(_pool: sqlx::PgPool) -> AuthzServiceImpl {
    let (_, authz, _) = native_broker_service().await.build_auth_services();
    authz
}

pub(super) fn analytics_service(pool: sqlx::PgPool) -> AnalyticsServiceImpl {
    AnalyticsServiceImpl::new().with_postgres(Some(pool))
}

pub(super) async fn tenant_service(
    pool: sqlx::PgPool,
) -> super::super::super::tenant_service::TenantServiceImpl {
    let _ = pool;
    native_broker_service().await.build_tenant_service()
}

pub(super) async fn notification_service(
    pool: sqlx::PgPool,
) -> super::super::super::notification_service::NotificationServiceImpl {
    let _ = pool;
    native_broker_service().await.build_notification_service()
}

/// Notification service wired to the shared outbox so `SendNotification`
/// publishes to `udb_system.outbox_events` (→ CDC → Kafka).
pub(super) async fn notification_service_with_outbox(
    pool: sqlx::PgPool,
) -> super::super::super::notification_service::NotificationServiceImpl {
    let _ = pool;
    native_broker_service().await.build_notification_service()
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
    let svc = tenant_service(pool.clone()).await;
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
    let svc = notification_service(pool.clone()).await;
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
    // `create_user` returns the user as PENDING_VERIFICATION; verifying the email
    // OTP activates it in the store (`mfa::verify_otp_impl`). Re-read so the helper
    // returns the ACTUAL post-verification (ACTIVE) record rather than the stale
    // create-time snapshot — a "verified user" must reflect the verified state.
    svc.get_user(Request::new(authn_pb::GetUserRequest {
        user_id: user.user_id.clone(),
        ..Default::default()
    }))
    .await
    .expect("re-read verified user")
    .into_inner()
    .user
    .expect("verified user present")
}

/// Create and activate a service account, then install its canonical typed
/// grant through the same Authn RPC surface production operators use. API-key,
/// password, refresh, and certificate tests share this helper so none can
/// accidentally rely on legacy profile attributes or a human owner.
pub(super) async fn create_service_account_with_grant(
    svc: &AuthnServiceImpl,
    prefix: &str,
    password: &str,
    approved_scopes: &[&str],
) -> (authn_entity_pb::User, authn_entity_pb::ServiceAccountGrant) {
    let suffix = Uuid::new_v4().simple().to_string();
    let created = svc
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("{prefix}_{suffix}"),
            email: format!("{prefix}_{suffix}@example.com"),
            password: password.to_string(),
            account_kind: authn_entity_pb::AccountKind::ServiceAccount as i32,
            tenant_id: "acme".to_string(),
            full_name: format!("{prefix} Service Account"),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("create service account")
        .into_inner();
    let created_user = created.user.expect("created service account");
    assert!(verify_issued_otp(svc, &created.otp_id).await.verified);
    let user = svc
        .get_user(Request::new(authn_pb::GetUserRequest {
            user_id: created_user.user_id,
            ..Default::default()
        }))
        .await
        .expect("re-read active service account")
        .into_inner()
        .user
        .expect("active service account present");
    let service_identity = format!("spiffe://test.udb/{prefix}/{suffix}");
    let grant = svc
        .create_service_account_grant(Request::new(authn_pb::CreateServiceAccountGrantRequest {
            tenant_id: user.tenant_id.clone(),
            user_id: user.user_id.clone(),
            service_identity,
            project_id: user.project_id.clone(),
            approved_scopes: approved_scopes
                .iter()
                .map(|scope| (*scope).to_string())
                .collect(),
            reason: "live test setup".to_string(),
        }))
        .await
        .expect("create typed service-account grant")
        .into_inner()
        .grant
        .expect("created typed grant");
    (user, grant)
}

/// §1 read-after-write served-path assertion (13.7.1.3). A create-returning-id RPC
/// must return a NON-EMPTY id that is IMMEDIATELY gettable on the SAME served path
/// with the SAME tenant/project metadata (and, where applicable, the SAME validated
/// claim context) a client uses. `created_id` is the id the create RPC returned;
/// `get` performs the served Get with that id and resolves `true` when the row is
/// present. Reverting any read-after-write guarantee (a create that returns an id
/// not gettable) fails this assertion. The closure owns the per-service Get wiring
/// so each service supplies its own metadata/claim context.
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
