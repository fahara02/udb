//! Phase 6 HA / cluster-convergence tests (plan §"Add HA tests with multiple UDB
//! nodes"). Two independent service instances share ONE Postgres pool to simulate
//! two cluster nodes, each with its own in-process caches over the shared durable
//! store. A mutation performed on one instance ("node 1") must converge on the
//! other instance ("node 2") that did NOT perform it — proving cluster behavior
//! that single-instance tests cannot. See `docs/auth-ha-test-plan.md`.

use super::super::AuthnServiceImpl;
use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::udb::core::common::v1 as common_pb;
use tonic::Request;

fn ha_principal(user_id: &str, fingerprint: &str) -> authn_pb::CreateSessionRequest {
    authn_pb::CreateSessionRequest {
        principal: Some(authn_pb::Principal {
            principal_id: user_id.to_string(),
            subject: user_id.to_string(),
            user_id: user_id.to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            scopes: vec!["data:read".to_string()],
            auth_method: "session".to_string(),
            ..Default::default()
        }),
        ttl_seconds: 300,
        client_fingerprint: fingerprint.to_string(),
    }
}

async fn validate_session(node: &AuthnServiceImpl, session_id: &str) -> bool {
    node.validate_token(Request::new(authn_pb::ValidateTokenRequest {
        token: session_id.to_string(),
        token_type: authn_entity_pb::TokenType::Session as i32,
    }))
    .await
    .expect("validate session token")
    .into_inner()
    .valid
}

/// HA-1: session revocation propagates across nodes. A session created on node 1
/// validates on node 2 (shared durable session store); after node 1 revokes it,
/// node 2 rejects it within the same request — revocation is a durable store read
/// (`jwt_persisted_state_valid`/`is_token_revoked`), not a per-node cache, so
/// there is no propagation gap.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_ha_session_revocation_propagation -- --ignored --nocapture"]
async fn live_postgres_ha_session_revocation_propagation() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    // Two "nodes": independent service instances over one shared Postgres store.
    let node1 = authn_service(pool.clone());
    let node2 = authn_service(pool.clone());

    let user = create_verified_user(&node1, "ha-revoke", "CorrectHorse1!").await;

    // Node 1 creates a session.
    let session = node1
        .create_session(Request::new(ha_principal(&user.user_id, "ha-node1")))
        .await
        .expect("node1 create session")
        .into_inner();
    assert!(session.session_id.starts_with("sess_"));

    // Node 2 (which did NOT create it) validates it — shared durable store.
    assert!(
        validate_session(&node2, &session.session_id).await,
        "session created on node1 must validate on node2"
    );

    // Node 1 revokes the session.
    let revoked = node1
        .revoke_session(Request::new(authn_pb::RevokeSessionRequest {
            session_id: session.session_id.clone(),
            revoke_reason: "ha_convergence_test".to_string(),
            ..Default::default()
        }))
        .await
        .expect("node1 revoke session")
        .into_inner();
    assert_eq!(revoked.revoked_count, 1);

    // Node 2 must now reject it immediately — no per-node revocation cache to lag.
    assert!(
        !validate_session(&node2, &session.session_id).await,
        "revocation on node1 must propagate to node2 within the propagation window (durable read)"
    );

    cleanup_native_auth_db(&pool).await;
}

/// HA-2: `LogoutAllSessions` propagates across nodes. Two sessions for one user
/// are created on node 1 and validate on node 2; after node 1 logs the user out
/// of all sessions, node 2 rejects BOTH — the global revocation is durable, so a
/// node that cached neither session honors the kill immediately.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_ha_logout_all_propagation -- --ignored --nocapture"]
async fn live_postgres_ha_logout_all_propagation() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    let node1 = authn_service(pool.clone());
    let node2 = authn_service(pool.clone());

    let user = create_verified_user(&node1, "ha-logout", "CorrectHorse1!").await;

    let s1 = node1
        .create_session(Request::new(ha_principal(&user.user_id, "ha-dev-a")))
        .await
        .expect("node1 create session 1")
        .into_inner();
    let s2 = node1
        .create_session(Request::new(ha_principal(&user.user_id, "ha-dev-b")))
        .await
        .expect("node1 create session 2")
        .into_inner();

    // Both visible on node 2.
    assert!(validate_session(&node2, &s1.session_id).await);
    assert!(validate_session(&node2, &s2.session_id).await);

    // Node 1 logs the user out of ALL sessions (`all_sessions: true`).
    node1
        .logout(Request::new(authn_pb::LogoutRequest {
            session_id: s1.session_id.clone(),
            all_sessions: true,
            revoke_reason: "ha_logout_all".to_string(),
            context: Some(common_pb::RequestContext {
                principal_id: user.user_id.clone(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .expect("node1 logout all sessions");

    // Node 2 rejects BOTH sessions — global durable revocation converged.
    assert!(
        !validate_session(&node2, &s1.session_id).await,
        "logout-all on node1 must invalidate session 1 on node2"
    );
    assert!(
        !validate_session(&node2, &s2.session_id).await,
        "logout-all on node1 must invalidate session 2 on node2"
    );

    cleanup_native_auth_db(&pool).await;
}
