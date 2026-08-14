//! Live CDC tests that exercise failure/replay behavior against real Postgres
//! and real Kafka. These intentionally avoid in-memory CDC sources.

use super::support::*;
use crate::proto::data_broker_client::DataBrokerClient;
use crate::proto::data_broker_server::DataBrokerServer;
use crate::proto::udb::core::apikey::services::v1 as apikey_pb;
use crate::proto::udb::core::apikey::services::v1::api_key_service_server::ApiKeyService;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::udb::core::common::v1 as common_pb;
use crate::runtime::authz::AuthzSnapshot;
use crate::runtime::cdc::{CdcConfig, CdcEngine, CdcEnvelope};
use crate::runtime::metrics::NoopMetrics;
use crate::runtime::service::method_security::{scope_claim_context_for_test, test_claim_context};
use futures::StreamExt;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio_stream::Stream;
use tonic::Request;
use uuid::Uuid;

fn kafka_brokers() -> String {
    std::env::var("UDB_INTEGRATION_KAFKA_BROKERS")
        .or_else(|_| std::env::var("UDB_KAFKA_BROKERS"))
        .unwrap_or_else(|_| "localhost:59192".to_string())
}

fn cdc_config_for_live_outbox(dlq_topic: impl Into<String>) -> CdcConfig {
    CdcConfig {
        outbox_table: "outbox_events".to_string(),
        dlq_topic: dlq_topic.into(),
        ..CdcConfig::default()
    }
}

async fn insert_outbox_envelope(
    pool: &sqlx::PgPool,
    event_id: Uuid,
    topic: &str,
    partition_key: &str,
    payload: serde_json::Value,
) {
    sqlx::query(
        "INSERT INTO udb_system.outbox_events (event_id, topic, partition_key, payload, created_at) \
         VALUES ($1, $2, $3, $4::JSONB, NOW())",
    )
    .bind(event_id)
    .bind(topic)
    .bind(partition_key)
    .bind(payload)
    .execute(pool)
    .await
    .expect("insert live CDC outbox event");
}

async fn insert_cdc_journal_envelope(
    pool: &sqlx::PgPool,
    event_id: Uuid,
    topic: &str,
    partition_key: &str,
    payload: serde_json::Value,
) {
    sqlx::query(
        "INSERT INTO udb_system.udb_cdc_event_journal \
         (event_id, topic, partition_key, payload, published_at, delivery_state) \
         VALUES ($1, $2, $3, $4::JSONB, NOW(), 'published') \
         ON CONFLICT (event_id) DO UPDATE SET \
           topic = EXCLUDED.topic, \
           partition_key = EXCLUDED.partition_key, \
           payload = EXCLUDED.payload, \
           published_at = EXCLUDED.published_at, \
           delivery_state = EXCLUDED.delivery_state",
    )
    .bind(event_id)
    .bind(topic)
    .bind(partition_key)
    .bind(payload)
    .execute(pool)
    .await
    .expect("insert live CDC journal event");
}

async fn dlq_record(pool: &sqlx::PgPool, event_id: Uuid) -> (String, String, serde_json::Value) {
    sqlx::query_as(
        "SELECT error_type, error_message, payload FROM udb_system.udb_cdc_dlq_events \
         WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await
    .expect("CDC DLQ row")
}

async fn outbox_count(pool: &sqlx::PgPool, event_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM udb_system.outbox_events WHERE event_id = $1")
        .bind(event_id)
        .fetch_one(pool)
        .await
        .expect("count outbox event")
}

async fn next_cdc_item(
    stream: &mut Pin<Box<dyn Stream<Item = Result<CdcEnvelope, tonic::Status>> + Send + 'static>>,
) -> CdcEnvelope {
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("CDC replay item should arrive")
        .expect("CDC replay stream should not end")
        .expect("CDC replay item should be ok")
}

fn served_bearer_cdc_request(
    token: &str,
    correlation_id: &str,
) -> Request<crate::proto::CdcSubscriptionRequest> {
    let mut request = Request::new(crate::proto::CdcSubscriptionRequest {
        topic_pattern: "udb.authn.*".to_string(),
        ..Default::default()
    });
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .expect("bearer authorization metadata"),
    );
    add_served_cdc_scope_metadata(&mut request, correlation_id);
    request
}

fn served_api_key_cdc_request(
    plain_key: &str,
    correlation_id: &str,
) -> Request<crate::proto::CdcSubscriptionRequest> {
    let mut request = Request::new(crate::proto::CdcSubscriptionRequest {
        topic_pattern: "udb.authn.*".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-api-key", plain_key.parse().expect("API-key metadata"));
    add_served_cdc_scope_metadata(&mut request, correlation_id);
    request
}

fn add_served_cdc_scope_metadata<T>(request: &mut Request<T>, correlation_id: &str) {
    let metadata = request.metadata_mut();
    metadata.insert("x-tenant-id", "acme".parse().unwrap());
    metadata.insert("x-udb-project-id", "billing".parse().unwrap());
    metadata.insert("x-purpose", "cdc-live-authorization".parse().unwrap());
    metadata.insert(
        "x-correlation-id",
        correlation_id.parse().expect("CDC correlation metadata"),
    );
}

async fn expect_served_cdc_stream_error(
    stream: &mut tonic::Streaming<crate::proto::CdcEnvelope>,
    label: &str,
) -> tonic::Status {
    match tokio::time::timeout(Duration::from_secs(15), stream.message())
        .await
        .unwrap_or_else(|_| panic!("{label}: CDC stream did not terminate within recheck bound"))
    {
        Err(status) => status,
        Ok(Some(envelope)) => panic!(
            "{label}: CDC stream delivered event {} after authority changed",
            envelope.event_id
        ),
        Ok(None) => panic!("{label}: CDC stream ended without an authorization status"),
    }
}

#[tokio::test]
#[ignore = "requires live Postgres + Kafka. UDB_LIVE_AUTH_TESTS=1 \
            UDB_INTEGRATION_KAFKA_BROKERS=localhost:59192 cargo test --lib cdc_live -- --ignored --nocapture"]
async fn live_cdc_topic_policy_rejection_routes_to_dlq_and_acks() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog");
    ensure_outbox_table(&pool).await;

    let brokers = kafka_brokers();
    let dlq_topic = format!("udb.cdc.dlq.{}.v1", Uuid::new_v4().simple());
    ensure_kafka_topic(&brokers, &dlq_topic).await;

    sqlx::query(
        "INSERT INTO udb_system.udb_topic_policy \
         (topic, owning_project, owning_service, schema_uri, enabled) \
         VALUES ($1, 'billing', 'authn', 'udb.authn.events.v1.Message', TRUE) \
         ON CONFLICT (topic) DO UPDATE SET enabled = TRUE, updated_at = NOW()",
    )
    .bind("udb.authn.allowed.only.v1")
    .execute(&pool)
    .await
    .expect("insert restrictive topic policy");

    let event_id = Uuid::new_v4();
    let user_id = Uuid::new_v4().to_string();
    let rejected_topic = "udb.authn.user.registered.v1";
    insert_outbox_envelope(
        &pool,
        event_id,
        rejected_topic,
        &user_id,
        serde_json::json!({
            "event_id": event_id.to_string(),
            "event_type": rejected_topic,
            "correlation_id": format!("cdc-policy:{event_id}"),
            "document_id": user_id,
            "tenant_id": "acme",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "payload": {"user_id": user_id}
        }),
    )
    .await;

    let mut engine = CdcEngine::new(
        pool.clone(),
        None,
        &brokers,
        live_pg_dsn(),
        Arc::new(NoopMetrics),
        cdc_config_for_live_outbox(dlq_topic),
    )
    .expect("build CDC engine");
    engine
        .load_topic_policies()
        .await
        .expect("load live topic policies");
    engine
        .process_outbox_event(
            event_id,
            rejected_topic.to_string(),
            user_id,
            serde_json::json!({
                "event_id": event_id.to_string(),
                "event_type": rejected_topic,
                "correlation_id": format!("cdc-policy:{event_id}"),
                "tenant_id": "acme",
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "payload": {}
            }),
            chrono::Utc::now(),
            77,
            None,
        )
        .await;

    let (error_type, error_message, payload) = dlq_record(&pool, event_id).await;
    assert_eq!(error_type, "TopicPolicyRejected");
    assert!(
        error_message.contains(rejected_topic),
        "DLQ message should name rejected topic: {error_message}"
    );
    assert_eq!(
        payload["failure_metadata"]["error_type"],
        "TopicPolicyRejected"
    );
    assert_eq!(outbox_count(&pool, event_id).await, 0);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + Kafka. UDB_LIVE_AUTH_TESTS=1 \
            UDB_INTEGRATION_KAFKA_BROKERS=localhost:59192 cargo test --lib cdc_live -- --ignored --nocapture"]
async fn live_cdc_mismatched_envelope_event_id_routes_to_dlq() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog");
    ensure_outbox_table(&pool).await;

    let brokers = kafka_brokers();
    let dlq_topic = format!("udb.cdc.dlq.{}.v1", Uuid::new_v4().simple());
    ensure_kafka_topic(&brokers, &dlq_topic).await;

    let event_id = Uuid::new_v4();
    let payload_event_id = Uuid::new_v4();
    let topic = "udb.authn.user.registered.v1";
    let payload = serde_json::json!({
        "event_id": payload_event_id.to_string(),
        "event_type": topic,
        "correlation_id": format!("cdc-mismatch:{event_id}"),
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "payload": {"user_id": Uuid::new_v4().to_string()}
    });
    insert_outbox_envelope(&pool, event_id, topic, "mismatch-user", payload.clone()).await;

    let engine = CdcEngine::new(
        pool.clone(),
        None,
        &brokers,
        live_pg_dsn(),
        Arc::new(NoopMetrics),
        cdc_config_for_live_outbox(dlq_topic),
    )
    .expect("build CDC engine");
    engine
        .process_outbox_event(
            event_id,
            topic.to_string(),
            "mismatch-user".to_string(),
            payload,
            chrono::Utc::now(),
            88,
            None,
        )
        .await;

    let (error_type, error_message, payload) = dlq_record(&pool, event_id).await;
    assert_eq!(error_type, "EnvelopeEventIdMismatch");
    assert!(
        error_message.contains(&payload_event_id.to_string()),
        "DLQ message should include payload event id: {error_message}"
    );
    assert_eq!(
        payload["failure_metadata"]["error_type"],
        "EnvelopeEventIdMismatch"
    );
    assert_eq!(outbox_count(&pool, event_id).await, 0);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + Kafka. UDB_LIVE_AUTH_TESTS=1 \
            UDB_INTEGRATION_KAFKA_BROKERS=localhost:59192 cargo test --lib cdc_live -- --ignored --nocapture"]
async fn live_cdc_stream_replay_filters_by_scope_topic_and_anchor() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog");
    ensure_outbox_table(&pool).await;

    let brokers = kafka_brokers();
    let engine = CdcEngine::new(
        pool.clone(),
        None,
        &brokers,
        live_pg_dsn(),
        Arc::new(NoopMetrics),
        cdc_config_for_live_outbox("udb.cdc.dlq.replay.v1"),
    )
    .expect("build CDC engine");

    let denied = match engine
        .stream_cdc(Vec::new(), "udb.authn.*".to_string(), None, None, None)
        .await
    {
        Ok(_) => panic!("missing CDC scope should be denied"),
        Err(status) => status,
    };
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    let malformed = match engine
        .stream_cdc(
            vec!["udb:cdc:read".to_string()],
            "udb.authn.*".to_string(),
            Some("not-a-uuid".to_string()),
            Some("tenant-a".to_string()),
            None,
        )
        .await
    {
        Ok(_) => panic!("malformed CDC cursor should be rejected"),
        Err(status) => status,
    };
    assert_eq!(malformed.code(), tonic::Code::InvalidArgument);

    let unknown = match engine
        .stream_cdc(
            vec!["udb:cdc:read".to_string()],
            "udb.authn.*".to_string(),
            Some(Uuid::new_v4().to_string()),
            Some("tenant-a".to_string()),
            None,
        )
        .await
    {
        Ok(_) => panic!("unknown CDC cursor should fail closed"),
        Err(status) => status,
    };
    assert_eq!(unknown.code(), tonic::Code::NotFound);

    let anchor_id = Uuid::new_v4();
    let replay_id = Uuid::new_v4();
    let skipped_id = Uuid::new_v4();
    insert_cdc_journal_envelope(
        &pool,
        anchor_id,
        "udb.authn.anchor.v1",
        "anchor",
        serde_json::json!({
            "event_id": anchor_id.to_string(),
            "event_type": "udb.authn.anchor.v1",
            "tenant_id": "tenant-a",
            "correlation_id": "cdc-replay-anchor",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "payload": {}
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    insert_cdc_journal_envelope(
        &pool,
        skipped_id,
        "udb.notification.sent.v1",
        "skip",
        serde_json::json!({
            "event_id": skipped_id.to_string(),
            "event_type": "udb.notification.sent.v1",
            "tenant_id": "tenant-b",
            "correlation_id": "cdc-replay-skip",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "payload": {}
        }),
    )
    .await;
    insert_cdc_journal_envelope(
        &pool,
        replay_id,
        "udb.authn.user.registered.v1",
        "replay",
        serde_json::json!({
            "event_id": replay_id.to_string(),
            "event_type": "udb.authn.user.registered.v1",
            "tenant_id": "tenant-a",
            "correlation_id": "cdc-replay-hit",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "payload": {"user_id": "replay"}
        }),
    )
    .await;

    let mut stream = engine
        .stream_cdc(
            vec!["udb:cdc:read".to_string()],
            "udb.authn.*".to_string(),
            Some(anchor_id.to_string()),
            Some("tenant-a".to_string()),
            None,
        )
        .await
        .expect("authorized CDC replay stream");
    let envelope = next_cdc_item(&mut stream).await;
    assert_eq!(envelope.event_id, replay_id.to_string());
    assert_eq!(envelope.topic, "udb.authn.user.registered.v1");
    assert_eq!(envelope.partition_key, "replay");

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres + Kafka. UDB_LIVE_AUTH_TESTS=1 \
            UDB_INTEGRATION_KAFKA_BROKERS=localhost:59192 cargo test --lib \
            live_served_cdc_stream_revalidates_bearer_api_key_and_policy -- --ignored --nocapture"]
async fn live_served_cdc_stream_revalidates_bearer_api_key_and_policy() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    let password = "CorrectHorse1!";
    let authn = authn_service_with_jwt(pool.clone());
    let apikey = api_key_service(pool.clone());
    let (account, _) = create_service_account_with_grant(
        &authn,
        "served_cdc_revalidation",
        password,
        &["udb:cdc:read"],
    )
    .await;
    let login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: account.email.clone(),
            password: password.to_string(),
            device_name: "served-cdc-bearer".to_string(),
            tenant_hint: "acme".to_string(),
            project_hint: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("mint served CDC bearer")
        .into_inner();
    let created_key = scope_claim_context_for_test(
        test_claim_context(&account.user_id, "acme", "billing", &[], &[]),
        apikey.create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
            name: "served-cdc-revalidation".to_string(),
            owner_id: account.user_id.clone(),
            scopes: vec!["udb:cdc:read".to_string()],
            context: Some(common_pb::RequestContext {
                principal_id: account.user_id.clone(),
                tenant: Some(common_pb::TenantContext {
                    tenant_id: "acme".to_string(),
                    project_id: "billing".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })),
    )
    .await
    .expect("create served CDC API key")
    .into_inner();
    let key_id = created_key
        .key
        .as_ref()
        .expect("created CDC API key record")
        .key_id
        .clone();

    let security = crate::runtime::security::SecurityConfig {
        allow_header_scopes: false,
        service_identity_required: true,
        jwt_private_key: Some(include_str!("../../../testdata/jwt_rs256_private.pem").to_string()),
        jwt_public_key: Some(include_str!("../../../testdata/jwt_rs256_public.pem").to_string()),
        ..crate::runtime::security::SecurityConfig::default()
    };
    crate::runtime::security::SecurityConfig::install_global(security.clone());
    super::super::install_data_plane_credential_resolvers(
        pool.clone(),
        &crate::runtime::authn::AuthnConfig {
            session_enabled: true,
            session_hash_secret: "live-auth-test-secret".to_string(),
            ..crate::runtime::authn::AuthnConfig::default()
        },
        Arc::new(authn.clone()),
    );

    let brokers = kafka_brokers();
    let engine = CdcEngine::new(
        pool.clone(),
        None,
        &brokers,
        live_pg_dsn(),
        Arc::new(NoopMetrics),
        cdc_config_for_live_outbox(format!(
            "udb.cdc.dlq.authorization.{}.v1",
            Uuid::new_v4().simple()
        )),
    )
    .expect("build served CDC engine");
    engine
        .load_topic_policies()
        .await
        .expect("load served CDC topic-policy snapshot");

    let runtime =
        crate::runtime::DataBrokerRuntime::from_config(crate::runtime::config::UdbConfig {
            primary: crate::runtime::config::DbConfig {
                direct_dsn: live_pg_dsn(),
                ..crate::runtime::config::DbConfig::default()
            },
            // Runtime construction republishes `config.security` as the process
            // authority. Carry the same verifier used above; leaving the default
            // here would erase the test JWT key after minting the token and make
            // the served layer reject a valid bearer before CDC is reached.
            security,
            ..crate::runtime::config::UdbConfig::default()
        })
        .await;
    let broker = crate::runtime::service::DataBrokerService::with_runtime_and_state(
        crate::runtime::native_catalog::native_manifest().clone(),
        runtime,
        Arc::new(RwLock::new(crate::FsmState::Completed)),
        Arc::new(NoopMetrics),
        Some(Arc::new(engine)),
        true,
    );
    let authz_snapshot = broker.authz_snapshot();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind served CDC listener");
    let address = listener.local_addr().expect("served CDC listener address");
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let connection = listener.accept().await.map(|(stream, _)| stream);
        Some((connection, listener))
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(crate::runtime::credential_layer::CredentialResolveLayer::new())
            .add_service(DataBrokerServer::new(broker))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve CDC revalidation listener");
    });
    let mut client = DataBrokerClient::connect(format!("http://{address}"))
        .await
        .expect("connect served CDC client");

    let mut bearer_stream = client
        .publish_cdc(served_bearer_cdc_request(
            &login.access_token,
            "cdc-bearer-revocation",
        ))
        .await
        .expect("open bearer-authorized CDC stream")
        .into_inner();
    authn
        .revoke_session(Request::new(authn_pb::RevokeSessionRequest {
            session_id: login.session_id,
            revoke_reason: "served CDC authorization lifetime test".to_string(),
            ..Default::default()
        }))
        .await
        .expect("revoke CDC bearer issuing session");
    let bearer_status = expect_served_cdc_stream_error(&mut bearer_stream, "revoked bearer").await;
    assert_eq!(bearer_status.code(), tonic::Code::Unauthenticated);

    let mut api_key_stream = client
        .publish_cdc(served_api_key_cdc_request(
            &created_key.plain_key,
            "cdc-api-key-revocation",
        ))
        .await
        .expect("open API-key-authorized CDC stream")
        .into_inner();
    scope_claim_context_for_test(
        test_claim_context(&account.user_id, "acme", "billing", &[], &[]),
        apikey.revoke_api_key(Request::new(apikey_pb::RevokeApiKeyRequest {
            key_id,
            revoke_reason: "served CDC authorization lifetime test".to_string(),
            ..Default::default()
        })),
    )
    .await
    .expect("revoke CDC API key");
    let api_key_status =
        expect_served_cdc_stream_error(&mut api_key_stream, "revoked API key").await;
    assert_eq!(api_key_status.code(), tonic::Code::Unauthenticated);

    let policy_login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: account.email,
            password: password.to_string(),
            device_name: "served-cdc-policy".to_string(),
            tenant_hint: "acme".to_string(),
            project_hint: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("mint bearer for live policy recheck")
        .into_inner();
    let mut policy_stream = client
        .publish_cdc(served_bearer_cdc_request(
            &policy_login.access_token,
            "cdc-policy-revocation",
        ))
        .await
        .expect("open CDC stream before policy withdrawal")
        .into_inner();
    authz_snapshot.store(Arc::new(AuthzSnapshot::default()));
    let policy_status =
        expect_served_cdc_stream_error(&mut policy_stream, "withdrawn Casbin policy").await;
    assert_eq!(policy_status.code(), tonic::Code::PermissionDenied);

    let _ = shutdown_tx.send(());
    server.await.expect("join CDC revalidation listener");
    cleanup_native_auth_db(&pool).await;
}
