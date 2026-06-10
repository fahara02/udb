//! #28 (Tier 5) — signing-key compromise / JWKS rotation propagation across nodes.
//!
//! Completes the multi-node HA matrix (the 5th scenario from `urgent_fix.md` #28 /
//! `docs/auth-ha-test-plan.md`) alongside `ha_convergence_live` (HA-1/HA-2 session
//! + logout-all) and `ha_multinode_live` (authz-revision, policy-bundle,
//! policy-distribution ACK/NACK/rollback, apikey revocation, refresh-replay race,
//! CDC idempotency, native-service matrix).
//!
//! Topology, exactly as the rest of the suite: two independent `AuthnServiceImpl`
//! instances (`node1`/`node2`) share ONE live Postgres signing-key registry. JWKS
//! is served from ACTIVE+VERIFYING registry rows read FRESH per request
//! (`jwks_document` → `jwks_registry_keys`), so a key rotated-in or compromised on
//! one node must appear / disappear in the OTHER node's JWKS within the same
//! request — there is no per-node JWKS cache to lag.
//!
//! Run: UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_ha_signing_key_jwks_rotation -- --ignored --nocapture

use super::super::AuthnServiceImpl;
use super::support::*;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::authn::signing_keys as skd;
use crate::runtime::native_catalog::native_model;
use tonic::Request;

/// Valid RS256 public PEM so `rsa_jwk_value_from_pem` parses each registry key
/// into a real JWK (the same fixture key the JWT-keyed service uses).
const TEST_RSA_PUBLIC_PEM: &str = include_str!("../../../testdata/jwt_rs256_public.pem");

fn signing_key_model() -> crate::runtime::native_catalog::NativeModel {
    native_model(
        "udb.core.authn.entity.v1.SigningKey",
        &[
            "key_id",
            "tenant_id",
            "algorithm",
            "public_material",
            "encrypted_private_material",
            "state",
            "created_by",
        ],
    )
}

/// Insert (or re-state) a registry signing key directly in the shared store —
/// stands in for an operator rotation / the signing-key registry write path that
/// any node performs against the same durable table.
async fn upsert_signing_key(pool: &sqlx::PgPool, kid: &str, state: &str) {
    let m = signing_key_model();
    let sql = format!(
        "INSERT INTO {rel} ({kid}, {tenant}, {alg}, {pubm}, {privm}, {state}, {by}) \
         VALUES ($1, '', $2, $3, 'sealed-test', $4, 'ha-jwks-test') \
         ON CONFLICT ({kid}) DO UPDATE SET {state} = EXCLUDED.{state}",
        rel = m.relation,
        kid = m.q("key_id"),
        tenant = m.q("tenant_id"),
        alg = m.q("algorithm"),
        pubm = m.q("public_material"),
        privm = m.q("encrypted_private_material"),
        state = m.q("state"),
        by = m.q("created_by"),
    );
    sqlx::query(&sql)
        .bind(kid)
        .bind(skd::DEFAULT_SIGNING_ALGORITHM)
        .bind(TEST_RSA_PUBLIC_PEM)
        .bind(state)
        .execute(pool)
        .await
        .expect("upsert signing key");
}

/// Transition an existing registry key to a new state (e.g. compromise).
async fn set_state(pool: &sqlx::PgPool, kid: &str, state: &str) {
    let m = signing_key_model();
    let sql = format!(
        "UPDATE {rel} SET {state} = $2 WHERE {kid} = $1",
        rel = m.relation,
        state = m.q("state"),
        kid = m.q("key_id"),
    );
    sqlx::query(&sql)
        .bind(kid)
        .bind(state)
        .execute(pool)
        .await
        .expect("update signing key state");
}

/// The `kid`s a node currently publishes in its JWKS document.
async fn published_kids(node: &AuthnServiceImpl) -> Vec<String> {
    let resp = node
        .get_jwks(Request::new(authn_pb::GetJwksRequest { context: None }))
        .await
        .expect("get_jwks")
        .into_inner();
    let doc: serde_json::Value =
        serde_json::from_str(&resp.jwks_json).expect("jwks_json is valid JSON");
    doc["keys"]
        .as_array()
        .map(|keys| {
            keys.iter()
                .filter_map(|k| k["kid"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// HA-7: signing-key rotation + compromise propagate across nodes via the durable
/// JWKS registry. `node1` rotates in an ACTIVE key (plus a VERIFYING overlap key);
/// `node2` — which performed neither write — publishes BOTH in its JWKS. When
/// `node1` then compromises the ACTIVE key, `node2` immediately stops publishing
/// it (compromised/retired keys are excluded) while the VERIFYING key remains,
/// proving JWKS is a fresh durable read with no per-node cache to lag.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_ha_signing_key_jwks_rotation -- --ignored --nocapture"]
async fn live_postgres_ha_signing_key_jwks_rotation() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    // Two "nodes": independent service instances over one shared Postgres store.
    let node1 = authn_service_with_jwt(pool.clone());
    let node2 = authn_service_with_jwt(pool.clone());

    // node1 rotates in a new ACTIVE key and keeps the previous one VERIFYING
    // (the rotation overlap window where both must verify).
    upsert_signing_key(&pool, "ha-key-active", skd::STATE_ACTIVE).await;
    upsert_signing_key(&pool, "ha-key-verifying", skd::STATE_VERIFYING).await;
    let _ = &node1; // node1 is the writer's vantage point; the assertions read node2.

    // node2 (which wrote neither) publishes BOTH — JWKS is read fresh from the
    // shared registry, so the rotation converged with no propagation gap.
    let kids = published_kids(&node2).await;
    assert!(
        kids.contains(&"ha-key-active".to_string()),
        "rotated-in ACTIVE key must appear in node2 JWKS: {kids:?}"
    );
    assert!(
        kids.contains(&"ha-key-verifying".to_string()),
        "VERIFYING overlap key must appear in node2 JWKS: {kids:?}"
    );

    // node1 compromises the ACTIVE key (durable registry mutation, as
    // `emergency_revoke` / `compromise_signing_key` would).
    set_state(&pool, "ha-key-active", skd::STATE_COMPROMISED).await;

    // node2 immediately stops publishing the compromised key; the VERIFYING key
    // (still publishable) remains. No per-node JWKS cache lag.
    let kids_after = published_kids(&node2).await;
    assert!(
        !kids_after.contains(&"ha-key-active".to_string()),
        "compromised key must vanish from node2 JWKS within the request: {kids_after:?}"
    );
    assert!(
        kids_after.contains(&"ha-key-verifying".to_string()),
        "non-compromised VERIFYING key must still be published: {kids_after:?}"
    );

    cleanup_native_auth_db(&pool).await;
}
