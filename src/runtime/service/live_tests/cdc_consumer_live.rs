//! Real DataBroker settlement and named delivery against PostgreSQL. Signed
//! credentials traverse the production resolver; ordinary scopes stay narrow.
//! Building the existing idle CdcEngine requires the Kafka feature, as in the
//! LiveQuery journal tests. No tailer or Kafka delivery runs in this fixture.
use super::data_plane_live::{
    dp_insert_allow_rule, dp_live_pg_dsn, dp_pool, dp_prepare_deny_db, dp_service_deny,
    dp_warm_authz,
};
use crate::generation::CatalogManifest;
use crate::proto::data_broker_client::DataBrokerClient;
use crate::proto::data_broker_server::{DataBroker, DataBrokerServer};
use crate::proto::udb::core::authn::entity::v1 as authn_entity;
use crate::proto::udb::core::authn::services::v1 as authn;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::{AckCdcEventsRequest, CdcSubscriptionRequest};
use crate::runtime::cdc::{CdcConfig, CdcEngine};
use crate::runtime::credential_layer::{CredentialResolveLayer, PreresolvedCredentials};
use crate::runtime::security::{SecurityConfig, sign_access_token};
use crate::runtime::service::DataBrokerService;
use crate::runtime::system::{SystemCatalogConfig, ensure_system_catalog};
use futures::StreamExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::{Code, Request};
use uuid::Uuid;

// Capture actual resolved evidence for deterministic lazy-stream polling below.
// This transport adapter never manufactures a principal or revalidation handle.
#[derive(Clone)]
struct CaptureLayer(Arc<Mutex<Option<PreresolvedCredentials>>>);
#[derive(Clone)]
struct CaptureService<S> {
    inner: S,
    captured: Arc<Mutex<Option<PreresolvedCredentials>>>,
}
impl<S> tower::Layer<S> for CaptureLayer {
    type Service = CaptureService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        CaptureService {
            inner,
            captured: self.0.clone(),
        }
    }
}
impl<S, B> tower::Service<tonic::codegen::http::Request<B>> for CaptureService<S>
where
    S: tower::Service<tonic::codegen::http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        if let Some(principal) = request.extensions().get::<PreresolvedCredentials>() {
            *self.captured.lock().expect("credential capture lock") = Some(principal.clone());
        }
        self.inner.call(request)
    }
}

struct SecurityRestore(SecurityConfig);
impl Drop for SecurityRestore {
    fn drop(&mut self) {
        SecurityConfig::install_global(self.0.clone());
    }
}

struct Fixture {
    pool: sqlx::PgPool,
    service: DataBrokerService,
    engine: Arc<CdcEngine>,
    remote: Arc<CdcEngine>,
    client: DataBrokerClient<tonic::transport::Channel>,
    captured: Arc<Mutex<Option<PreresolvedCredentials>>>,
    shutdown: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
    tenant: String,
    topic: String,
    users: Vec<String>,
    tokens: Vec<String>,
    security: SecurityConfig,
    _restore: SecurityRestore,
}

async fn build_engine(pool: &sqlx::PgPool, dsn: &str, load_policy: bool) -> Arc<CdcEngine> {
    let metrics: Arc<dyn crate::metrics::MetricsRecorder> = Arc::new(crate::metrics::NoopMetrics);
    let config = CdcConfig::default();
    #[cfg(feature = "redis")]
    let engine = CdcEngine::new(
        pool.clone(),
        None,
        "127.0.0.1:1",
        dsn.to_string(),
        metrics,
        config,
    );
    #[cfg(not(feature = "redis"))]
    let engine = CdcEngine::new(
        pool.clone(),
        "127.0.0.1:1",
        dsn.to_string(),
        metrics,
        config,
    );
    let engine = Arc::new(engine.expect("idle CDC engine for actual journal reads"));
    if load_policy {
        engine
            .load_topic_policies()
            .await
            .expect("load actual topic policies");
    }
    engine
}

fn token(
    security: &SecurityConfig,
    tenant: &str,
    user: &str,
    identity: &str,
    scopes: &[String],
) -> String {
    sign_access_token(
        security,
        user,
        tenant,
        "default",
        scopes,
        &[],
        identity,
        &format!("cdc-fixture-{}", Uuid::new_v4()),
        "pwd",
        if identity.is_empty() {
            0
        } else {
            authn_entity::AccountKind::ServiceAccount as i32
        },
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .expect("sign actual fixture bearer")
    .expect("fixture signing key configured")
    .0
}

fn request<T>(body: T, token: &str) -> Request<T> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
        .metadata_mut()
        .insert("x-purpose", "cdc.consumer.ci".parse().unwrap());
    request.metadata_mut().insert(
        "x-correlation-id",
        Uuid::new_v4().to_string().parse().unwrap(),
    );
    request
}

fn acknowledgment(name: &str, topic: &str, event: Uuid) -> AckCdcEventsRequest {
    AckCdcEventsRequest {
        consumer_name: name.into(),
        topic_pattern: topic.into(),
        event_id: event.to_string(),
        ..Default::default()
    }
}
fn subscription(name: &str, topic: &str) -> CdcSubscriptionRequest {
    CdcSubscriptionRequest {
        consumer_name: name.into(),
        topic_pattern: topic.into(),
        ..Default::default()
    }
}

async fn fixture() -> Fixture {
    let restore = SecurityRestore(SecurityConfig::current());
    let dsn = dp_live_pg_dsn().expect("G6 serving CI requires an actual PostgreSQL DSN");
    let pool = dp_pool(&dsn).await;
    dp_prepare_deny_db(&pool).await;
    ensure_system_catalog(&pool)
        .await
        .expect("durable system catalog");
    let tenant = Uuid::new_v4().to_string();
    let topic = format!("udb.cdc.consumer.{}.v1", Uuid::new_v4().simple());
    let policies = SystemCatalogConfig::current().topic_policy_relation();
    sqlx::query(&format!("INSERT INTO {policies} (topic, tenant_id, owning_project, owning_service, enabled) VALUES ($1, $2, 'default', 'consumer-ci', TRUE)"))
        .bind(&topic).bind(&tenant).execute(&pool).await.expect("narrow topic policy");
    let mut service = dp_service_deny(&dsn, CatalogManifest::default()).await;
    // Own the fixture's hash key before constructing any authn adapter. The
    // production constructor reads process environment; this fixture instead
    // supplies its real PostgreSQL stores and the same resolver/runtime wiring.
    let authn_config = crate::runtime::authn::AuthnConfig {
        session_hash_secret: format!("cdc-fixture-{}", Uuid::new_v4()),
        ..crate::runtime::authn::AuthnConfig::from_env()
    };
    let runtime = service.runtime_snapshot();
    let authn_service = crate::runtime::service::auth_service::AuthnServiceImpl::with_stores(
        authn_config.clone(),
        SecurityConfig::current(),
        Arc::new(crate::runtime::authn::PostgresSessionStore::new(
            pool.clone(),
            "",
        )),
        Arc::new(crate::runtime::authn::PostgresApiKeyStore::new(
            pool.clone(),
            "",
        )),
        Arc::new(crate::runtime::authn::PostgresUserStore::new(
            pool.clone(),
            "",
        )),
    )
    .with_runtime(Some(runtime.clone()))
    .with_authz_snapshot(Some(service.authz_snapshot()))
    .with_event_sink(Arc::new(
        crate::runtime::service::auth_service::events::OutboxAuthEventSink::new(
            pool.clone(),
            runtime.config().cdc.outbox_relation(),
        ),
    ));
    crate::runtime::service::auth_service::install_data_plane_credential_resolvers(
        pool.clone(),
        &authn_config,
        Arc::new(authn_service.clone()),
    );
    let mut users = Vec::new();
    for identity in ["", "", "unknown"] {
        let label = format!("cdc_{}", Uuid::new_v4().simple());
        let created = authn_service
            .create_user(Request::new(authn::CreateUserRequest {
                username: label.clone(),
                email: format!("{label}@example.test"),
                password: "CorrectHorse1!".into(),
                tenant_id: tenant.clone(),
                project_id: "default".into(),
                account_kind: if identity.is_empty() {
                    0
                } else {
                    authn_entity::AccountKind::ServiceAccount as i32
                },
                ..Default::default()
            }))
            .await
            .expect("create canonical fixture account")
            .into_inner()
            .user
            .unwrap();
        authn_service
            .change_user_status(Request::new(authn::ChangeUserStatusRequest {
                user_id: created.user_id.clone(),
                new_status: authn_entity::UserStatus::Active as i32,
                reason: "CDC fixture activation".into(),
                ..Default::default()
            }))
            .await
            .expect("activate canonical fixture account");
        if !identity.is_empty() {
            authn_service
                .create_service_account_grant(Request::new(
                    authn::CreateServiceAccountGrantRequest {
                        tenant_id: tenant.clone(),
                        user_id: created.user_id.clone(),
                        service_identity: identity.into(),
                        project_id: "default".into(),
                        approved_scopes: vec!["udb:cdc:read".into()],
                        reason: "CDC fixture".into(),
                    },
                ))
                .await
                .expect("canonical typed service grant");
        }
        users.push(created.user_id);
    }
    let mut ids = Vec::new();
    for user in &users {
        ids.push(dp_insert_allow_rule(&pool, &tenant, "default", user, &topic, "PublishCDC").await);
    }
    dp_warm_authz(
        &service,
        &ids.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await;
    let security = SecurityConfig {
        tls_required: false,
        service_identity_required: false,
        mtls_required: false,
        allow_header_scopes: false,
        jwt_private_key: Some(include_str!("../../testdata/jwt_rs256_private.pem").into()),
        jwt_public_key: Some(include_str!("../../testdata/jwt_rs256_public.pem").into()),
        ..SecurityConfig::default()
    };
    SecurityConfig::install_global(security.clone());
    let tokens = users
        .iter()
        .enumerate()
        .map(|(index, user)| {
            token(
                &security,
                &tenant,
                user,
                if index == 2 { "unknown" } else { "" },
                &["udb:cdc:read".into()],
            )
        })
        .collect();
    let engine = build_engine(&pool, &dsn, true).await;
    let remote = build_engine(&pool, &dsn, true).await;
    service.cdc_engine = Some(engine.clone());
    let captured = Arc::new(Mutex::new(None));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind actual broker");
    let address = listener.local_addr().unwrap();
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let connection = listener.accept().await.map(|(stream, _)| stream);
        Some((connection, listener))
    });
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    let served = service.clone();
    let layers = tower::ServiceBuilder::new()
        .layer(CredentialResolveLayer::new())
        .layer(CaptureLayer(captured.clone()));
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(layers.into_inner())
            .add_service(DataBrokerServer::new(served))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve actual credential-resolved DataBroker");
    });
    let client = DataBrokerClient::connect(format!("http://{address}"))
        .await
        .expect("connect actual DataBroker");
    Fixture {
        pool,
        service,
        engine,
        remote,
        client,
        captured,
        shutdown,
        server,
        tenant,
        topic,
        users,
        tokens,
        security,
        _restore: restore,
    }
}

async fn seed(f: &Fixture, topic: &str, tenant: &str, project: &str, second: i32) -> Uuid {
    let id = Uuid::new_v4();
    let payload = serde_json::json!({"event_id":id,"event_type":topic,"tenant_id":tenant,"project_id":project});
    let journal = SystemCatalogConfig::current().cdc_journal_relation();
    sqlx::query(&format!("INSERT INTO {journal} (event_id, topic, payload, published_at) VALUES ($1,$2,$3,'2026-01-01'::timestamptz + make_interval(secs => $4::double precision))"))
        .bind(id).bind(topic).bind(payload).bind(f64::from(second)).execute(&f.pool).await.expect("seed retained event");
    id
}

/// Deliver a retained fixture row through the engine's actual fast-path sender.
/// The row is committed before this wake; the named stream must still drain the
/// durable journal instead of accepting this envelope ahead of an older row.
async fn broadcast_retained_fixture(f: &Fixture, engine: &CdcEngine, event: Uuid) {
    let journal = SystemCatalogConfig::current().cdc_journal_relation();
    let (topic, partition_key, payload, published_at): (
        String,
        String,
        serde_json::Value,
        chrono::DateTime<chrono::Utc>,
    ) = sqlx::query_as(&format!(
        "SELECT topic, partition_key, payload, published_at FROM {journal} WHERE event_id=$1"
    ))
    .bind(event)
    .fetch_one(&f.pool)
    .await
    .expect("broadcast uses the actual retained journal row");
    let _ = engine
        .broadcast_sender_for_live_test()
        .send(crate::runtime::cdc::CdcEnvelope {
            event_id: event.to_string(),
            topic,
            partition_key,
            payload_json: payload.to_string(),
            published_at,
        });
}
async fn cursor(f: &Fixture, name: &str) -> (Uuid, String) {
    let table = SystemCatalogConfig::current().cdc_consumer_cursors_relation();
    sqlx::query_as(&format!("SELECT last_event_id, owner_identity FROM {table} WHERE tenant_id=$1 AND consumer_name=$2 AND topic_pattern=$3"))
        .bind(&f.tenant).bind(name).bind(&f.topic).fetch_one(&f.pool).await.expect("read actual durable cursor")
}
async fn next(stream: &mut tonic::Streaming<crate::proto::CdcEnvelope>) -> String {
    tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("three-second delivery deadline")
        .expect("open CDC stream")
        .expect("actual served event")
        .event_id
}

impl Fixture {
    async fn close(self) {
        let _ = self.shutdown.send(());
        tokio::time::timeout(Duration::from_secs(5), self.server)
            .await
            .expect("broker streams/permits released")
            .expect("broker task completes");
        let config = SystemCatalogConfig::current();
        for (table, column) in [
            (config.cdc_consumer_cursors_relation(), "tenant_id"),
            (config.cdc_journal_relation(), "payload->>'tenant_id'"),
            (config.topic_policy_relation(), "tenant_id"),
            (config.cdc.outbox_relation(), "payload->>'tenant_id'"),
        ] {
            sqlx::query(&format!("DELETE FROM {table} WHERE {column}=$1"))
                .bind(&self.tenant)
                .execute(&self.pool)
                .await
                .expect("remove fixture CDC rows");
        }
        for message in [
            "udb.core.authz.entity.v1.PolicyRule",
            "udb.core.authn.entity.v1.ServiceAccountGrant",
            "udb.core.authn.entity.v1.User",
        ] {
            let model = crate::runtime::native_catalog::native_model(message, &["tenant_id"]);
            sqlx::query(&format!(
                "DELETE FROM {} WHERE {}=$1",
                model.relation,
                model.q("tenant_id")
            ))
            .bind(&self.tenant)
            .execute(&self.pool)
            .await
            .expect("remove fixture identity/policy rows");
        }
    }
}

#[tokio::test]
#[ignore = "requires real PostgreSQL; CI runs all ignored native lib tests"]
async fn live_cdc_ack_binds_real_signed_owners_and_explicit_resume_claims() {
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let mut f = fixture().await;
    let first = seed(&f, &f.topic, &f.tenant, "default", 1).await;
    let mut stream = f
        .client
        .publish_cdc(request(subscription("reader", &f.topic), &f.tokens[0]))
        .await
        .expect("Alice claims before first ACK")
        .into_inner();
    assert_eq!(next(&mut stream).await, first.to_string());
    drop(stream);
    let mut explicit = subscription("reader", &f.topic);
    explicit.since_event_id = first.to_string();
    assert_eq!(
        f.client
            .publish_cdc(request(explicit, &f.tokens[1]))
            .await
            .err()
            .expect("g6: explicit resume must not bypass consumer ownership")
            .code(),
        Code::PermissionDenied,
        "g6: explicit resume must not bypass consumer ownership"
    );
    assert_eq!(
        f.client
            .ack_cdc_events(request(
                acknowledgment("reader", &f.topic, first),
                &f.tokens[1]
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied,
        "g6: distinct verified users cannot share the audit sentinel as owner"
    );
    f.client
        .ack_cdc_events(request(
            acknowledgment("reader", &f.topic, first),
            &f.tokens[0],
        ))
        .await
        .expect("actual owner ACK");
    assert_eq!(cursor(&f, "reader").await.1, format!("user:{}", f.users[0]));
    f.client
        .ack_cdc_events(request(
            acknowledgment("service-reader", &f.topic, first),
            &f.tokens[2],
        ))
        .await
        .expect("literal unknown canonical service remains valid");
    assert_eq!(cursor(&f, "service-reader").await.1, "service:unknown");
    assert_eq!(
        f.client
            .ack_cdc_events(request(
                acknowledgment("service-reader", &f.topic, first),
                &f.tokens[0]
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    // Direct signed JWT fallback carries exactly the same owner as the served layer.
    f.service
        .ack_cdc_events(request(
            acknowledgment("reader", &f.topic, first),
            &f.tokens[0],
        ))
        .await
        .expect("direct signed JWT owner remains identical");
    let cursors = SystemCatalogConfig::current().cdc_consumer_cursors_relation();
    sqlx::query(&format!("INSERT INTO {cursors} (tenant_id,project_id,consumer_name,topic_pattern,last_event_id) VALUES ($1,'default','legacy',$2,$3)"))
        .bind(&f.tenant).bind(&f.topic).bind(first).execute(&f.pool).await.unwrap();
    assert_eq!(
        f.client
            .ack_cdc_events(request(
                acknowledgment("legacy", &f.topic, first),
                &f.tokens[0]
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        cursor(&f, "legacy").await,
        (first, String::new()),
        "legacy refusal must preserve the original cursor"
    );
    f.close().await;
}

#[tokio::test]
#[ignore = "requires real PostgreSQL; CI runs all ignored native lib tests"]
async fn live_cdc_ack_validates_event_policy_and_keeps_monotonic_retained_cursor() {
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let mut f = fixture().await;
    let first = seed(&f, &f.topic, &f.tenant, "default", 1).await;
    let second = seed(&f, &f.topic, &f.tenant, "default", 2).await;
    let third = seed(&f, &f.topic, &f.tenant, "default", 3).await;
    f.client
        .ack_cdc_events(request(
            acknowledgment("reader", &f.topic, second),
            &f.tokens[0],
        ))
        .await
        .expect("valid retained ACK can claim owner atomically");
    f.client
        .ack_cdc_events(request(
            acknowledgment("reader", &f.topic, first),
            &f.tokens[0],
        ))
        .await
        .expect("stale ACK is idempotent success");
    let foreign = seed(&f, &f.topic, "foreign-tenant", "default", 4).await;
    let project = seed(&f, &f.topic, &f.tenant, "foreign-project", 5).await;
    let topic = seed(&f, "udb.cdc.foreign.v1", &f.tenant, "default", 6).await;
    let unstamped = seed(&f, &f.topic, "", "", 7).await;
    for event in [foreign, project, topic, unstamped] {
        assert_eq!(
            f.client
                .ack_cdc_events(request(
                    acknowledgment("reader", &f.topic, event),
                    &f.tokens[0]
                ))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
    assert_eq!(
        f.client
            .ack_cdc_events(request(
                acknowledgment("reader", &f.topic, Uuid::new_v4()),
                &f.tokens[0]
            ))
            .await
            .err()
            .expect("g6: arbitrary UUID must not poison a consumer cursor")
            .code(),
        Code::NotFound,
        "g6: arbitrary UUID must not poison a consumer cursor"
    );
    let no_scope = token(&f.security, &f.tenant, &f.users[0], "", &[]);
    assert_eq!(
        f.client
            .ack_cdc_events(request(
                acknowledgment("scope-missing", &f.topic, second),
                &no_scope
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut unavailable = f.service.clone();
    unavailable.cdc_engine = Some(build_engine(&f.pool, &dp_live_pg_dsn().unwrap(), false).await);
    assert_eq!(
        unavailable
            .ack_cdc_events(request(
                acknowledgment("reader", &f.topic, third),
                &f.tokens[0]
            ))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable,
        "unavailable topic-policy authority must fail closed before cursor advancement",
    );
    let cursors = SystemCatalogConfig::current().cdc_consumer_cursors_relation();
    let absent: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {cursors} WHERE tenant_id=$1 AND consumer_name='scope-missing'"
    ))
    .bind(&f.tenant)
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(
        absent, 0,
        "scope refusal cannot register or mutate a cursor"
    );
    let non_udb = format!("customer.consumer.{}", Uuid::new_v4().simple());
    let policies = SystemCatalogConfig::current().topic_policy_relation();
    sqlx::query(&format!("INSERT INTO {policies} (topic, tenant_id, owning_project, owning_service, enabled) VALUES ($1,$2,'default','consumer-ci',TRUE)"))
        .bind(&non_udb).bind(&f.tenant).execute(&f.pool).await.unwrap();
    let policy = dp_insert_allow_rule(
        &f.pool,
        &f.tenant,
        "default",
        &f.users[0],
        &non_udb,
        "PublishCDC",
    )
    .await;
    dp_warm_authz(&f.service, &[&policy]).await;
    f.engine.load_topic_policies().await.unwrap();
    let non_udb_unstamped = seed(&f, &non_udb, "", "", 8).await;
    assert_eq!(
        f.client
            .ack_cdc_events(request(
                acknowledgment("policy-owned", &non_udb, non_udb_unstamped),
                &f.tokens[0]
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied,
        "policy-owned non-UDB topics must require the same tenant/project stamps as actual delivery",
    );
    let policies = SystemCatalogConfig::current().topic_policy_relation();
    sqlx::query(&format!(
        "UPDATE {policies} SET enabled=FALSE WHERE topic=$1"
    ))
    .bind(&f.topic)
    .execute(&f.pool)
    .await
    .unwrap();
    f.engine.load_topic_policies().await.unwrap();
    assert_eq!(
        f.client
            .ack_cdc_events(request(
                acknowledgment("reader", &f.topic, third),
                &f.tokens[0]
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    sqlx::query(&format!(
        "UPDATE {policies} SET enabled=TRUE WHERE topic=$1"
    ))
    .bind(&f.topic)
    .execute(&f.pool)
    .await
    .unwrap();
    f.engine.load_topic_policies().await.unwrap();
    assert_eq!(
        cursor(&f, "reader").await.0,
        second,
        "g6: refused and stale ACKs must leave the highest valid cursor unchanged"
    );
    let journal = SystemCatalogConfig::current().cdc_journal_relation();
    sqlx::query(&format!("DELETE FROM {journal} WHERE event_id=$1"))
        .bind(second)
        .execute(&f.pool)
        .await
        .unwrap();
    let mut stream = f
        .client
        .publish_cdc(request(subscription("reader", &f.topic), &f.tokens[0]))
        .await
        .expect("retained watermark survives deleted anchor row")
        .into_inner();
    assert_eq!(next(&mut stream).await, third.to_string());
    drop(stream);
    sqlx::query(&format!("DELETE FROM {journal} WHERE event_id = ANY($1)"))
        .bind(vec![foreign, project, topic, unstamped, non_udb_unstamped])
        .execute(&f.pool)
        .await
        .unwrap();
    f.close().await;
}

#[tokio::test]
#[ignore = "requires real PostgreSQL; CI runs all ignored native lib tests"]
async fn live_cdc_named_delivery_orders_cross_replica_journal_before_local_broadcast() {
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let mut f = fixture().await;
    for round in 0..4 {
        let name = format!("ordered-{round}");
        let anchor = seed(&f, &f.topic, &f.tenant, "default", round * 3).await;
        f.client
            .ack_cdc_events(request(
                acknowledgment(&name, &f.topic, anchor),
                &f.tokens[0],
            ))
            .await
            .unwrap();
        let resolved = f
            .captured
            .lock()
            .unwrap()
            .clone()
            .expect("actual production resolver evidence");
        assert_eq!(
            resolved.bearer.as_ref().unwrap().as_ref().unwrap().subject,
            f.users[0]
        );
        assert!(resolved.revalidator.is_some());
        let mut body = subscription(&name, &f.topic);
        if round % 2 == 1 {
            body.since_event_id = anchor.to_string();
        }
        let mut req = request(body, &f.tokens[0]);
        req.extensions_mut().insert(resolved);
        // Drive the actual lazy broker stream, so journal/broadcast timing is
        // controlled by this caller instead of HTTP/2 prefetch.
        let mut stream = f.service.publish_cdc(req).await.unwrap().into_inner();
        assert!(
            tokio::time::timeout(Duration::from_millis(5), stream.next())
                .await
                .is_err(),
            "fixture begins caught up before either publisher writes"
        );
        let started = tokio::time::Instant::now();
        let older = seed(&f, &f.topic, &f.tenant, "default", round * 3 + 1).await;
        broadcast_retained_fixture(&f, f.remote.as_ref(), older).await;
        let newer = seed(&f, &f.topic, &f.tenant, "default", round * 3 + 2).await;
        broadcast_retained_fixture(&f, f.engine.as_ref(), newer).await;
        let delivered = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            delivered.event_id,
            older.to_string(),
            "g6: local broadcast must not overtake an older durable cross-replica event"
        );
        eprintln!(
            "g6 named-order round={round} publish-and-delivery-ms={}",
            started.elapsed().as_millis()
        );
        f.client
            .ack_cdc_events(request(
                acknowledgment(&name, &f.topic, older),
                &f.tokens[0],
            ))
            .await
            .unwrap();
        drop(stream);
        let mut resumed = f
            .client
            .publish_cdc(request(subscription(&name, &f.topic), &f.tokens[0]))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            next(&mut resumed).await,
            newer.to_string(),
            "disconnect after ordered ACK must preserve the remaining event"
        );
        drop(resumed);
    }
    f.close().await;
}
