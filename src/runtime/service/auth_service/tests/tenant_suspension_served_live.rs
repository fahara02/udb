//! G5: tenant suspension is enforced from the DURABLE tenant row.
//!
//! * Served: a live bearer of an ACTIVE tenant reaches a native RPC through the
//!   credential + method-security layers; once the tenant is SUSPENDED the SAME
//!   bearer is refused `FailedPrecondition` (`tenant_not_active`), and after
//!   reactivation it is admitted again.
//! * Cross-replica: a suspension committed by ANOTHER replica (row update +
//!   `pg_notify` on the tenant-status channel, in one transaction) evicts this
//!   process's cached ACTIVE status well inside the cache TTL, via the LISTEN
//!   task `register_tenant_status_store` starts.

use super::support::*;
use crate::proto::udb::core::tenant::entity::v1 as tenant_entity_pb;
use crate::proto::udb::core::tenant::services::v1 as tenant_pb;
use crate::proto::udb::core::tenant::services::v1::tenant_service_client::TenantServiceClient;
use crate::proto::udb::core::tenant::services::v1::tenant_service_server::{
    TenantService, TenantServiceServer,
};
use crate::runtime::service::method_security::MethodSecurityLayer;
use crate::runtime::service::tenant_service::{
    register_tenant_status_store, tenant_status_gate, tenant_status_gate_durable,
};
use sqlx::Connection as _;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::Request;

/// Mirror of the gate's `TENANT_STATUS_CHANNEL` (private to the gate module).
const TENANT_STATUS_CHANNEL: &str = "udb_tenant_status_changed";
/// Mirror of the gate's `TENANT_STATUS_CACHE_TTL`: a deny observed sooner than
/// this after caching ACTIVE can only come from the invalidation path.
const TENANT_STATUS_CACHE_TTL: Duration = Duration::from_secs(5);
/// How long the cross-replica eviction may take before the test fails.
const INVALIDATION_DEADLINE: Duration = Duration::from_secs(2);

fn error_detail(status: &tonic::Status) -> Option<crate::proto::ErrorDetail> {
    use prost::Message as _;
    let raw = status
        .metadata()
        .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)?
        .to_bytes()
        .ok()?;
    crate::proto::ErrorDetail::decode(raw.as_ref()).ok()
}

fn assert_tenant_not_active(status: &tonic::Status, context: &str) {
    assert_eq!(
        status.code(),
        tonic::Code::FailedPrecondition,
        "{context}: {status}"
    );
    assert!(
        status.message().contains("not active"),
        "{context}: {status}"
    );
    let detail = error_detail(status).unwrap_or_else(|| panic!("{context}: no typed detail"));
    assert_eq!(
        detail.policy_decision_id, "tenant_not_active",
        "{context}: {status}"
    );
}

/// The tenant's stored status, read under its own RLS tenant context.
async fn durable_tenant_status(pool: &sqlx::PgPool, tenant_id: &str) -> String {
    let mut tx = pool.begin().await.expect("begin tenant status read");
    sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .expect("set tenant read context");
    let status: String =
        sqlx::query_scalar("SELECT status FROM udb_tenant.tenants WHERE tenant_id = $1::UUID")
            .bind(tenant_id)
            .fetch_one(&mut *tx)
            .await
            .expect("read durable tenant status");
    tx.commit().await.expect("commit tenant status read");
    status
}

fn bearer_tenant_request<T>(message: T, token: &str, tenant_id: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .expect("bearer authorization metadata"),
    );
    request
        .metadata_mut()
        .insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    request.metadata_mut().insert(
        "x-request-id",
        "tenant-suspension-served".parse().expect("request id"),
    );
    request
}

async fn set_tenant_status(svc: &impl TenantService, tenant_id: &str, status: &str) {
    svc.update_tenant(Request::new(tenant_pb::UpdateTenantRequest {
        tenant_id: tenant_id.to_string(),
        status: status.to_string(),
        ..Default::default()
    }))
    .await
    .unwrap_or_else(|err| panic!("set tenant {tenant_id} {status}: {err}"));
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_served_tenant_suspension_revokes_live_bearer_until_reactivated -- --ignored --nocapture"]
async fn live_served_tenant_suspension_revokes_live_bearer_until_reactivated() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    let admin = tenant_service(pool.clone()).await;
    // The bearer must belong to a real ACTIVE user: durable bearer validation
    // rejects a token whose subject is not an active account. Suspend the
    // user's own (canonical) tenant.
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "tenant_suspension", "CorrectHorse1!").await;
    let tenant_id = user.tenant_id.clone();

    let security = crate::runtime::security::SecurityConfig {
        jwt_private_key: Some(include_str!("../../../testdata/jwt_rs256_private.pem").to_string()),
        jwt_public_key: Some(include_str!("../../../testdata/jwt_rs256_public.pem").to_string()),
        ..crate::runtime::security::SecurityConfig::default()
    };
    let token = crate::runtime::security::sign_access_token(
        &security,
        &user.user_id,
        &tenant_id,
        &user.project_id,
        &["udb:tenant:get-tenant".to_string()],
        &[],
        "",
        "tenant-suspension-served",
        "password",
        0,
        now_unix(),
    )
    .expect("sign tenant bearer")
    .expect("signing key configured")
    .0;
    crate::runtime::security::SecurityConfig::install_global(security);
    super::super::install_data_plane_credential_resolvers(
        pool.clone(),
        &crate::runtime::authn::AuthnConfig {
            session_enabled: true,
            session_hash_secret: "live-auth-test-secret".to_string(),
            ..crate::runtime::authn::AuthnConfig::default()
        },
        Arc::new(authn),
    );

    let served = tenant_service(pool.clone()).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind served tenant listener");
    let address = listener.local_addr().expect("served tenant address");
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let connection = listener.accept().await.map(|(stream, _)| stream);
        Some((connection, listener))
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(crate::runtime::credential_layer::CredentialResolveLayer::new())
            .add_service(MethodSecurityLayer::new().wrap(TenantServiceServer::new(served)))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve tenant listener");
    });
    let mut client = TenantServiceClient::connect(format!("http://{address}"))
        .await
        .expect("connect served tenant client");
    let get = || {
        bearer_tenant_request(
            tenant_pb::GetTenantRequest {
                tenant_id: tenant_id.clone(),
            },
            &token,
            &tenant_id,
        )
    };

    // ACTIVE: the bearer is admitted.
    let active = client
        .get_tenant(get())
        .await
        .expect("an ACTIVE tenant's bearer reaches GetTenant")
        .into_inner()
        .tenant
        .expect("served tenant");
    assert_eq!(active.tenant_id, tenant_id);
    assert_eq!(active.status, tenant_entity_pb::TenantStatus::Active as i32);

    // SUSPENDED: the SAME, still-valid bearer is refused.
    set_tenant_status(&admin, &tenant_id, "SUSPENDED").await;
    assert_eq!(durable_tenant_status(&pool, &tenant_id).await, "SUSPENDED");
    let refused = client
        .get_tenant(get())
        .await
        .expect_err("a suspended tenant's live bearer must be refused");
    assert_tenant_not_active(&refused, "served GetTenant while SUSPENDED");

    // Reactivated: admitted again.
    set_tenant_status(&admin, &tenant_id, "ACTIVE").await;
    assert_eq!(durable_tenant_status(&pool, &tenant_id).await, "ACTIVE");
    let restored = client
        .get_tenant(get())
        .await
        .expect("a reactivated tenant's bearer is admitted again")
        .into_inner()
        .tenant
        .expect("served tenant after reactivation");
    assert_eq!(
        restored.status,
        tenant_entity_pb::TenantStatus::Active as i32
    );

    let _ = shutdown_tx.send(());
    server.await.expect("join served tenant listener");
    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_peer_replica_suspension_evicts_cached_tenant_status_before_ttl -- --ignored --nocapture"]
async fn live_peer_replica_suspension_evicts_cached_tenant_status_before_ttl() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    let tenant_id = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO udb_tenant.tenants (tenant_id, code, name, type, status) \
         VALUES ($1::UUID, $2, 'G5 cross-replica', 'ORGANIZATION', 'ACTIVE')",
    )
    .bind(&tenant_id)
    .bind(format!("g5_{}", uuid::Uuid::new_v4().simple()))
    .execute(&pool)
    .await
    .expect("insert ACTIVE tenant");

    // (Re)start this process's invalidation listener and wait until its LISTEN
    // is live, so the peer's NOTIFY cannot race the subscription. The pause lets
    // the listener the fixture started settle first, so its LISTEN predates the
    // timestamp and cannot be mistaken for the new one.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let registered_at: String = sqlx::query_scalar("SELECT clock_timestamp()::text")
        .fetch_one(&pool)
        .await
        .expect("read server clock");
    register_tenant_status_store(Some(pool.clone()));
    let listen_sql = format!(r#"LISTEN "{TENANT_STATUS_CHANNEL}""#);
    let listening_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let listening: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pg_stat_activity \
             WHERE pid <> pg_backend_pid() AND state = 'idle' \
               AND query = $1 AND state_change >= $2::timestamptz",
        )
        .bind(&listen_sql)
        .bind(&registered_at)
        .fetch_one(&pool)
        .await
        .expect("probe tenant-status listener");
        if listening > 0 {
            break;
        }
        assert!(
            Instant::now() < listening_deadline,
            "the tenant-status LISTEN task never subscribed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Cache ACTIVE on this replica.
    tenant_status_gate_durable(&tenant_id)
        .await
        .expect("an ACTIVE tenant passes the durable gate");
    let cached_at = Instant::now();

    // The PEER replica: a separate connection commits the status write and its
    // notification together.
    let mut peer = sqlx::PgConnection::connect(&live_pg_dsn())
        .await
        .expect("connect peer replica");
    let mut tx = peer.begin().await.expect("begin peer suspension");
    sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
        .bind(&tenant_id)
        .execute(&mut *tx)
        .await
        .expect("set peer tenant context");
    let updated = sqlx::query(
        "UPDATE udb_tenant.tenants SET status = 'SUSPENDED' WHERE tenant_id = $1::UUID",
    )
    .bind(&tenant_id)
    .execute(&mut *tx)
    .await
    .expect("peer suspends the tenant")
    .rows_affected();
    assert_eq!(updated, 1, "the peer must suspend exactly the tenant row");
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(TENANT_STATUS_CHANNEL)
        .bind(&tenant_id)
        .execute(&mut *tx)
        .await
        .expect("peer publishes the status change");
    tx.commit().await.expect("commit peer suspension");
    peer.close().await.expect("close peer connection");

    // This replica must deny well before its cached ACTIVE would expire.
    let denied = loop {
        match tenant_status_gate_durable(&tenant_id).await {
            Err(status) => break status,
            Ok(()) => {
                assert!(
                    cached_at.elapsed() < INVALIDATION_DEADLINE,
                    "the peer's suspension was not applied within {INVALIDATION_DEADLINE:?}"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    };
    assert!(
        cached_at.elapsed() < TENANT_STATUS_CACHE_TTL,
        "the deny must come from invalidation, not cache expiry"
    );
    assert_tenant_not_active(&denied, "durable gate after peer suspension");
    tenant_status_gate(&tenant_id).expect_err("the refreshed cache now holds SUSPENDED");
    assert_eq!(durable_tenant_status(&pool, &tenant_id).await, "SUSPENDED");

    cleanup_native_auth_db(&pool).await;
}
