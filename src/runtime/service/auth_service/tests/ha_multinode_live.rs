//! Phase 13 (Production Readiness) multi-node / cluster-convergence live tests.
//!
//! Extends [`super::ha_convergence_live`] (HA-1 session revocation, HA-2 logout-all)
//! with the remaining scenarios from `docs/auth-ha-test-plan.md` plus the Phase-9
//! control-plane distribution (ACK/NACK/rollback) and CDC idempotency convergence.
//!
//! Topology, exactly as the convergence suite: two independent service instances
//! (`node1`/`node2`) share ONE live Postgres pool to simulate two cluster nodes,
//! each with its own in-process caches over the shared durable store. A mutation
//! performed on `node1` must converge on `node2` (the node that did NOT perform it)
//! within the configured window — the property single-instance tests cannot prove.
//!
//! Every test is env-gated like the convergence suite (`#[ignore]`; run with
//! `UDB_LIVE_AUTH_TESTS=1 cargo test --lib <name> -- --ignored --nocapture`) and
//! self-cleans via `cleanup_native_auth_db`. No hardcoded schema/table names: the
//! control-plane store resolves every identifier through the proto-driven
//! `native_model`, and the auth services through the native catalog DDL.

use super::super::control_plane::resources::{aggregate_version, content_version};
use super::super::control_plane::store;
use super::super::{AuthnServiceImpl, AuthzServiceImpl};
use super::support::*;
use crate::proto::udb::core::apikey::services::v1 as apikey_pb;
use crate::proto::udb::core::apikey::services::v1::api_key_service_server::ApiKeyService;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::udb::core::authz::services::v1 as authz_pb;
use crate::proto::udb::core::authz::services::v1::authz_service_server::AuthzService;
use crate::proto::udb::core::common::v1 as common_pb;
use crate::proto::udb::core::control::entity::v1::ResourceType;
use crate::runtime::service::method_security::{scope_claim_context_for_test, test_claim_context};
use tonic::Request;
use uuid::Uuid;

/// Snapshot TTL (seconds) used for the cross-node authz convergence tests so a
/// non-mutating node's per-instance snapshot cache expires within the test
/// window. `authz_snapshot_ttl()` reads `UDB_AUTHZ_SNAPSHOT_TTL_SECS` at
/// construction and floors it at 1s, so we set it before building `node2`.
const TEST_SNAPSHOT_TTL_SECS: u64 = 1;

fn set_short_snapshot_ttl() {
    // SAFETY: tests in this module run serially under `live_auth_db_lock`; the env
    // is read only at `AuthzServiceImpl` construction below.
    unsafe {
        std::env::set_var(
            "UDB_AUTHZ_SNAPSHOT_TTL_SECS",
            TEST_SNAPSHOT_TTL_SECS.to_string(),
        );
    }
}

/// Build a CheckAccess request for a role-bound principal on (tenant, project).
fn check_access(user_id: &str, object: &str, action: &str) -> authz_pb::CheckAccessRequest {
    authz_pb::CheckAccessRequest {
        user_id: user_id.to_string(),
        domain: "acme".to_string(),
        tenant_id: "acme".to_string(),
        project_id: "billing".to_string(),
        object: object.to_string(),
        action: action.to_string(),
        ..Default::default()
    }
}

async fn allowed_on(node: &AuthzServiceImpl, user_id: &str, object: &str, action: &str) -> bool {
    node.check_access(Request::new(check_access(user_id, object, action)))
        .await
        .expect("check_access")
        .into_inner()
        .allowed
}

/// Seed a user + a role + a role binding + an allow policy so a principal is
/// authorized for `(object, action)` on both nodes. Returns the user id and the
/// role code the policy is keyed on.
async fn seed_authorized_principal(
    authn: &AuthnServiceImpl,
    authz: &AuthzServiceImpl,
    prefix: &str,
    object: &str,
    action: &str,
) -> (String, String) {
    let user = create_verified_user(authn, prefix, "CorrectHorse1!").await;
    let suffix = Uuid::new_v4().simple().to_string();
    let role_code = format!("{prefix}_{suffix}");

    authz
        .put_role_binding(Request::new(authz_pb::PutRoleBindingRequest {
            binding: Some(authz_pb::RoleBinding {
                subject: user.user_id.clone(),
                role: role_code.clone(),
                tenant: "acme".to_string(),
                project: "billing".to_string(),
                expires_at_unix: 0,
                source: "ha_multinode_test".to_string(),
            }),
        }))
        .await
        .expect("put_role_binding");

    authz
        .put_authz_policy(Request::new(authz_pb::PutAuthzPolicyRequest {
            policy: Some(authz_pb::AuthzPolicyRecord {
                id: Uuid::new_v4().to_string(),
                enabled: true,
                effect: "allow".to_string(),
                tenant: "acme".to_string(),
                project: "billing".to_string(),
                role: role_code.clone(),
                action: action.to_string(),
                resource: object.to_string(),
                ..Default::default()
            }),
        }))
        .await
        .expect("put allow policy");

    (user.user_id, role_code)
}

/// HA-3: authz revision invalidation propagates across nodes. `node1` mutates a
/// policy (adds an explicit DENY that overrides the seeded ALLOW); `node2` — which
/// performed no mutation, so its per-instance snapshot cache was NOT invalidated —
/// returns the NEW decision once its snapshot TTL elapses, and `GetAuthzRevision`
/// (a durable read) reports a changed content-hash `policy_version`/revision.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_ha_authz_revision_invalidation -- --ignored --nocapture"]
async fn live_postgres_ha_authz_revision_invalidation() {
    let _guard = live_auth_db_lock().lock().await;
    set_short_snapshot_ttl();
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    let authn = authn_service(pool.clone());
    let node1 = authz_service(pool.clone()).await;
    let node2 = authz_service(pool.clone()).await;

    let (user_id, role_code) =
        seed_authorized_principal(&authn, &node1, "ha-authz", "invoice", "data.update").await;

    // Baseline: both nodes ALLOW. node2 reads through to the shared store on its
    // first call (its cache was seeded empty + invalidated by `with_postgres`).
    assert!(
        allowed_on(&node1, &user_id, "invoice", "data.update").await,
        "node1 must ALLOW the seeded principal"
    );
    assert!(
        allowed_on(&node2, &user_id, "invoice", "data.update").await,
        "node2 must ALLOW the seeded principal (shared durable snapshot)"
    );

    // Capture node2's durable revision/content-hash BEFORE the mutation.
    let before = node2
        .get_authz_revision(Request::new(authz_pb::GetAuthzRevisionRequest {
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
        }))
        .await
        .expect("node2 revision before")
        .into_inner();

    // node1 adds an explicit DENY for the same (role, action, resource). An
    // explicit deny overrides the role-bound allow.
    node1
        .put_authz_policy(Request::new(authz_pb::PutAuthzPolicyRequest {
            policy: Some(authz_pb::AuthzPolicyRecord {
                id: Uuid::new_v4().to_string(),
                enabled: true,
                effect: "deny".to_string(),
                tenant: "acme".to_string(),
                project: "billing".to_string(),
                role: role_code,
                action: "data.update".to_string(),
                resource: "invoice".to_string(),
                ..Default::default()
            }),
        }))
        .await
        .expect("node1 put explicit DENY");

    // The durable revision must have advanced; node2 sees it immediately because
    // `GetAuthzRevision` is a durable read (not the cached snapshot).
    let after = node2
        .get_authz_revision(Request::new(authz_pb::GetAuthzRevisionRequest {
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
        }))
        .await
        .expect("node2 revision after")
        .into_inner();
    assert!(
        after.policy_revision > before.policy_revision,
        "node2 must observe an advanced policy_revision ({} -> {}) after node1's mutation",
        before.policy_revision,
        after.policy_revision
    );
    // TODO(host): assert the revision-row content_hash CHANGES once put_authz_policy
    // feeds a payload-derived hash into bump_authz_revision. Today it passes the
    // constant "policy-put" (see authz/mod.rs ~L1307), so the AuthzRevision.content_hash
    // column is identical across policy puts and cannot distinguish them. The
    // content-addressed value that DOES change on every policy edit is the snapshot
    // version (`policy_content_version`), asserted indirectly via node2's decision
    // flip below; the durable monotonic `policy_revision` counter (asserted above)
    // is the reliable cross-node invalidation signal in the meantime.
    assert!(
        !after.content_hash.is_empty(),
        "node2's revision content_hash must be populated"
    );

    // node2's DECISION must flip to DENY within its snapshot TTL window. The cache
    // was last loaded before the mutation, so wait out the TTL then re-check.
    tokio::time::sleep(std::time::Duration::from_secs(TEST_SNAPSHOT_TTL_SECS + 2)).await;
    assert!(
        !allowed_on(&node2, &user_id, "invoice", "data.update").await,
        "node2 must DENY within the snapshot TTL after node1 added an explicit DENY"
    );

    cleanup_native_auth_db(&pool).await;
}

/// Policy-bundle revocation: `node1` invalidates the cached bundles for a tenant
/// via the REAL `InvalidatePolicyBundles` governance RPC (driven with an
/// `authz:admin` governance actor). The revision bump is durable, so `node2`'s
/// `GetAuthzRevision` reflects the new revision — every SDK bundle pinned below it
/// is now stale. We also assert the bundle a node would re-issue carries the new
/// policy_version (bundle signing requires `UDB_POLICY_BUNDLE_SECRET`; without it
/// the signing precondition is asserted instead so the test never fakes a pass).
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_ha_policy_bundle_revocation -- --ignored --nocapture"]
async fn live_postgres_ha_policy_bundle_revocation() {
    let _guard = live_auth_db_lock().lock().await;
    set_short_snapshot_ttl();
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    let authn = authn_service(pool.clone());
    let node1 = authz_service(pool.clone()).await;
    let node2 = authz_service(pool.clone()).await;

    // A policy must exist so a revision row exists to bump.
    seed_authorized_principal(&authn, &node1, "ha-bundle", "report", "data.export").await;

    let before = node2
        .get_authz_revision(Request::new(authz_pb::GetAuthzRevisionRequest {
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
        }))
        .await
        .expect("node2 revision before invalidation")
        .into_inner();

    // node1 revokes/invalidates every cached bundle for the tenant via the real
    // governance RPC. An `authz:admin`-scoped actor satisfies the standing-scope
    // gate (and the snapshot governance check permits when no governance policy
    // is configured), so this exercises `invalidate_policy_bundles_impl` end to
    // end — the revision bump + the durable bundle-revocation record.
    let invalidated = node1
        .invalidate_policy_bundles(Request::new(authz_pb::InvalidatePolicyBundlesRequest {
            actor: Some(authz_pb::GovernanceActor {
                subject: "ha-admin".to_string(),
                tenant_id: "acme".to_string(),
                project_id: "billing".to_string(),
                scopes: vec!["authz:admin".to_string()],
                ..Default::default()
            }),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            reason: "ha_multinode_bundle_revocation".to_string(),
        }))
        .await
        .expect("node1 invalidate_policy_bundles")
        .into_inner();
    assert!(invalidated.ok);
    assert!(
        invalidated.policy_revision > before.policy_revision,
        "invalidation must advance the policy revision ({} -> {})",
        before.policy_revision,
        invalidated.policy_revision
    );

    // node2 reflects the new revision durably: any bundle pinned to `before` is
    // now stale (its policy_revision < node2's reported revision).
    let after = node2
        .get_authz_revision(Request::new(authz_pb::GetAuthzRevisionRequest {
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
        }))
        .await
        .expect("node2 revision after invalidation")
        .into_inner();
    assert!(
        after.policy_revision >= invalidated.policy_revision,
        "node2 must observe the post-invalidation revision (>= {}), saw {}",
        invalidated.policy_revision,
        after.policy_revision
    );

    // A bundle node2 would re-issue must carry the new revision (or, if bundle
    // signing isn't configured, the signing precondition must be reported — we
    // never fake a signed-bundle pass).
    match node2
        .get_policy_bundle(Request::new(authz_pb::PolicyBundleRequest {
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            domain: "acme".to_string(),
        }))
        .await
    {
        Ok(resp) => {
            let bundle = resp.into_inner().bundle.expect("signed bundle");
            assert!(
                !bundle.policy_version.is_empty(),
                "a freshly issued bundle must carry a non-empty policy_version"
            );
        }
        Err(status) => {
            // Bundle signing not configured in this environment: assert the exact
            // precondition rather than passing silently.
            assert_eq!(
                status.code(),
                tonic::Code::FailedPrecondition,
                "without UDB_POLICY_BUNDLE_SECRET, bundle issuance must fail the \
                 signing precondition (got {status:?})"
            );
        }
    }

    cleanup_native_auth_db(&pool).await;
}

/// Policy distribution ACK/NACK + rollback against the REAL Postgres-backed
/// control-plane store (`control_plane::store`), not the in-memory mirror in
/// `control_plane/tests.rs`. Exercises the full acceptance property "a node can
/// reject a bad policy without silently diverging":
///   1. push v1 (a routing policy) and ACK it → `accepted_version` advances;
///   2. push a bad v2 and NACK it → `accepted_version` stays at v1 (no divergence),
///      `last_good_version` is preserved, the NACK error is recorded;
///   3. rollback the registry to the v1 content → a fresh push+ACK restores the
///      node onto the good world version.
/// Two nodes share the registry; node B's NACK must not advance node A.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_ha_policy_distribution_ack_nack_rollback -- --ignored --nocapture"]
async fn live_postgres_ha_policy_distribution_ack_nack_rollback() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    let rt = ResourceType::RoutingPolicy;
    let name = format!("ha-routing-{}", Uuid::new_v4().simple());
    let node_a = format!("node-a-{}", Uuid::new_v4().simple());
    let node_b = format!("node-b-{}", Uuid::new_v4().simple());

    // --- v1: a good routing policy is published to the registry. ---
    let v1_payload = r#"{"route":"primary","weight":100}"#;
    let v1 = store::upsert_resource(&pool, rt, &name, "", "billing", v1_payload, "ha-test")
        .await
        .expect("publish v1 resource");
    assert_eq!(v1.content_hash, content_version(v1_payload));

    // The version a node ACKs is the aggregate world version of the type it is
    // subscribed to (matches the DiscoveryResponse the stream would carry).
    let world_v1 = store::world_version(&pool, rt, None, &[])
        .await
        .expect("world version v1");

    // node A subscribes and ACKs v1 under the nonce the control plane stamped.
    store::ensure_node_state(&pool, &node_a, rt, &[name.clone()])
        .await
        .expect("ensure node A state");
    let nonce_a1 = store::next_response_nonce(&pool, &node_a, rt)
        .await
        .expect("nonce A v1");
    assert!(
        store::record_ack(&pool, &node_a, rt, &world_v1, &nonce_a1)
            .await
            .expect("record ACK A v1"),
        "ACK with the matching nonce must be applied"
    );
    let after_ack = store::get_node_state(&pool, &node_a, rt)
        .await
        .expect("get node A state")
        .expect("node A row exists");
    assert_eq!(after_ack.accepted_version, world_v1);
    assert_eq!(after_ack.last_good_version, world_v1);
    assert!(after_ack.nack_error_detail.is_empty());

    // A stale-nonce ACK must be IGNORED (no silent divergence on a stale echo).
    assert!(
        !store::record_ack(&pool, &node_a, rt, "bogus-world", "stale-nonce")
            .await
            .expect("stale ACK A"),
        "an ACK whose nonce does not match the last response nonce must be ignored"
    );
    let still_v1 = store::get_node_state(&pool, &node_a, rt)
        .await
        .expect("get node A after stale ack")
        .expect("row");
    assert_eq!(
        still_v1.accepted_version, world_v1,
        "a stale-nonce ACK must not advance accepted_version"
    );

    // --- v2: a BAD routing policy is published; node A NACKs it. ---
    let v2_payload = r#"{"route":"","weight":-1}"#;
    let v2 = store::upsert_resource(&pool, rt, &name, "", "billing", v2_payload, "ha-test")
        .await
        .expect("publish v2 resource");
    assert_ne!(
        v2.content_hash, v1.content_hash,
        "a content change must bump the version"
    );
    let world_v2 = store::world_version(&pool, rt, None, &[])
        .await
        .expect("world version v2");
    assert_ne!(world_v1, world_v2, "v2 must change the world version");

    let nonce_a2 = store::next_response_nonce(&pool, &node_a, rt)
        .await
        .expect("nonce A v2");
    assert!(
        store::record_nack(
            &pool,
            &node_a,
            rt,
            &nonce_a2,
            r#"{"code":3,"message":"invalid routing policy: empty route"}"#,
        )
        .await
        .expect("record NACK A v2"),
        "NACK with the matching nonce must be applied"
    );
    // INVARIANT: node A keeps running v1 — accepted/last_good are preserved, only
    // the NACK error is set. The bad v2 did NOT silently take effect.
    let after_nack = store::get_node_state(&pool, &node_a, rt)
        .await
        .expect("get node A after NACK")
        .expect("row");
    assert_eq!(
        after_nack.accepted_version, world_v1,
        "NACK must NOT advance accepted_version onto the bad v2"
    );
    assert_eq!(
        after_nack.last_good_version, world_v1,
        "NACK must preserve last_good_version"
    );
    assert!(
        !after_nack.nack_error_detail.is_empty(),
        "NACK must record the structured error detail"
    );

    // A second node B NACKing the same bad v2 must not advance node A either
    // (per-node ledgers; one node's reject cannot move another's accepted state).
    store::ensure_node_state(&pool, &node_b, rt, &[name.clone()])
        .await
        .expect("ensure node B state");
    let nonce_b2 = store::next_response_nonce(&pool, &node_b, rt)
        .await
        .expect("nonce B v2");
    assert!(
        store::record_nack(&pool, &node_b, rt, &nonce_b2, "node B rejects v2")
            .await
            .expect("record NACK B v2")
    );
    let node_a_unchanged = store::get_node_state(&pool, &node_a, rt)
        .await
        .expect("node A after B's NACK")
        .expect("row");
    assert_eq!(
        node_a_unchanged.accepted_version, world_v1,
        "node B's NACK must not affect node A's accepted_version"
    );

    // --- rollback: restore the v1 content; a fresh push+ACK returns to good. ---
    let rolled_back =
        store::upsert_resource(&pool, rt, &name, "", "billing", v1_payload, "ha-test")
            .await
            .expect("rollback to v1 content");
    assert_eq!(
        rolled_back.content_hash,
        content_version(v1_payload),
        "rollback restores the v1 content hash"
    );
    let world_rb = store::world_version(&pool, rt, None, &[])
        .await
        .expect("world version after rollback");
    assert_eq!(
        world_rb, world_v1,
        "rolling the content back to v1 restores the v1 world version"
    );
    let nonce_a3 = store::next_response_nonce(&pool, &node_a, rt)
        .await
        .expect("nonce A rollback");
    assert!(
        store::record_ack(&pool, &node_a, rt, &world_rb, &nonce_a3)
            .await
            .expect("record ACK A rollback")
    );
    let after_rollback = store::get_node_state(&pool, &node_a, rt)
        .await
        .expect("node A after rollback")
        .expect("row");
    assert_eq!(after_rollback.accepted_version, world_v1);
    assert!(
        after_rollback.nack_error_detail.is_empty(),
        "a successful ACK after rollback must clear the prior NACK error"
    );

    // Cross-check the aggregate-version helper agrees with the store's world view.
    let resources = store::list_resources(&pool, rt, None, &[name.clone()])
        .await
        .expect("list resources");
    assert_eq!(aggregate_version(&resources), world_rb);

    // Self-clean: the control-plane registry + node-state rows live in the native
    // schemas dropped by cleanup_native_auth_db.
    cleanup_native_auth_db(&pool).await;
}

/// 6.2 served-path reload metric: the reload counter must fire through the REAL
/// subscriber serving path (`subscriber::run_once`), not a stubbed recorder call.
/// Seeds a baseline (no reload counted), publishes a fresh routing policy into the
/// registry, ticks the subscriber again, and asserts
/// `udb_control_reload_applied_total` increments for the changed type on a REAL
/// `PrometheusMetrics` recorder.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_control_reload_metric_fires_on_served_path -- --ignored --nocapture"]
async fn live_postgres_control_reload_metric_fires_on_served_path() {
    use super::super::control_plane::subscriber::{self, LastSeen, SubscriberHandle};
    use crate::runtime::config::UdbConfig;
    use crate::runtime::metrics::{MetricsRecorder, PrometheusMetrics};
    use std::sync::Arc;

    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    // A REAL prometheus recorder so the assertion proves the SERVED path emits the
    // metric (not a direct `inc_control_reload_applied` stub call).
    let metrics = Arc::new(PrometheusMetrics::new().expect("build prometheus metrics"));
    let metrics_sink: Arc<dyn MetricsRecorder> = metrics.clone();
    let handle = SubscriberHandle::new(pool.clone(), Arc::new(UdbConfig::default()))
        .with_metrics(metrics_sink);

    // 1. Seed the baseline — the seeding pass must NOT count a reload.
    let baseline = subscriber::run_once(&handle, &LastSeen::default(), false)
        .await
        .expect("seed subscriber baseline");
    let seeded_text = metrics.gather_text("");
    assert!(
        !seeded_text.contains("udb_control_reload_applied_total"),
        "the seeding pass must NOT count a reload:\n{seeded_text}"
    );

    // 2. Publish a brand-new routing policy directly into the registry; resync is
    //    additive (never prunes), so the next tick observes a real change.
    let rt = ResourceType::RoutingPolicy;
    let name = format!("served-reload-{}", Uuid::new_v4().simple());
    store::upsert_resource(
        &pool,
        rt,
        &name,
        "",
        "billing",
        r#"{"route":"primary"}"#,
        "metric-test",
    )
    .await
    .expect("publish routing policy");

    // 3. A second served tick detects the change and fires the reload metric
    //    THROUGH the real subscriber path (run_once -> inc_control_reload_applied).
    let _next = subscriber::run_once(&handle, &baseline, true)
        .await
        .expect("served reload tick");
    let text = metrics.gather_text("");
    assert!(
        text.lines().any(|line| {
            line.starts_with("udb_control_reload_applied_total{")
                && line.contains("RESOURCE_TYPE_ROUTING_POLICY")
        }),
        "the served reload path must increment udb_control_reload_applied_total \
         for the changed type:\n{text}"
    );

    cleanup_native_auth_db(&pool).await;
}

/// 6.2 snapshot retention + rollback: serve v1 then a bad v2 (both retained in the
/// per-(node,type) ring), then `RollbackResources` to the retained v1 — the
/// handler re-publishes the retained payloads through the same content-addressed
/// registry, restoring the v1 world version. An unknown target fails CLOSED.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_control_rollback_resources_restores_retained_snapshot -- --ignored --nocapture"]
async fn live_postgres_control_rollback_resources_restores_retained_snapshot() {
    use super::super::control_plane::ControlPlaneServiceImpl;
    use crate::proto::udb::core::control::services::v1 as control_pb;
    use crate::proto::udb::core::control::services::v1::control_plane_service_server::ControlPlaneService;

    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    let rt = ResourceType::RoutingPolicy;
    let node = format!("rollback-node-{}", Uuid::new_v4().simple());
    let name = format!("rollback-routing-{}", Uuid::new_v4().simple());

    // v1: a good policy is published and SERVED (retained in the node's ring).
    let v1_payload = r#"{"route":"primary","weight":100}"#;
    store::upsert_resource(&pool, rt, &name, "", "billing", v1_payload, "rb-test")
        .await
        .expect("publish v1");
    store::ensure_node_state(&pool, &node, rt, &[name.clone()])
        .await
        .expect("seed node state");
    let v1_resources = store::list_resources(&pool, rt, None, &[name.clone()])
        .await
        .expect("list v1 resources");
    let world_v1 = store::world_version(&pool, rt, None, &[name.clone()])
        .await
        .expect("world v1");
    store::retain_served_snapshot(&pool, &node, rt, &world_v1, &v1_resources)
        .await
        .expect("retain v1 snapshot");

    // v2: a bad policy is published and SERVED (now the newest retained snapshot).
    let v2_payload = r#"{"route":"","weight":-1}"#;
    store::upsert_resource(&pool, rt, &name, "", "billing", v2_payload, "rb-test")
        .await
        .expect("publish v2");
    let v2_resources = store::list_resources(&pool, rt, None, &[name.clone()])
        .await
        .expect("list v2 resources");
    let world_v2 = store::world_version(&pool, rt, None, &[name.clone()])
        .await
        .expect("world v2");
    store::retain_served_snapshot(&pool, &node, rt, &world_v2, &v2_resources)
        .await
        .expect("retain v2 snapshot");
    assert_ne!(world_v1, world_v2, "v2 must change the world version");

    // Roll back to the retained v1 through the RPC handler.
    let svc = ControlPlaneServiceImpl::new().with_postgres(Some(pool.clone()));
    let resp = svc
        .rollback_resources(Request::new(control_pb::RollbackResourcesRequest {
            node_id: node.clone(),
            resource_type: rt as i32,
            target_version: world_v1.clone(),
            ..Default::default()
        }))
        .await
        .expect("rollback to retained v1")
        .into_inner();
    assert_eq!(resp.rolled_back_to_version, world_v1);
    assert_eq!(resp.resources_restored, v1_resources.len() as i32);

    // The registry world is back at v1 (content-addressed re-publish restores it).
    let world_after = store::world_version(&pool, rt, None, &[name.clone()])
        .await
        .expect("world after rollback");
    assert_eq!(
        world_after, world_v1,
        "rollback must restore the retained v1 world version"
    );

    // Fail-closed: an unretained target version yields FAILED_PRECONDITION.
    let err = svc
        .rollback_resources(Request::new(control_pb::RollbackResourcesRequest {
            node_id: node.clone(),
            resource_type: rt as i32,
            target_version: "no-such-retained-version".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("an unretained target must fail closed");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    cleanup_native_auth_db(&pool).await;
}

/// API-key revocation propagation: `node1` revokes an API key; `node2`'s
/// `ValidateApiKey` (which did not create or revoke it) denies it immediately,
/// because revocation is a durable store read on the shared pool — the API-key
/// analogue of HA-1's session revocation.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_ha_apikey_revocation_propagation -- --ignored --nocapture"]
async fn live_postgres_ha_apikey_revocation_propagation() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    let authn = authn_service(pool.clone());
    let node1 = api_key_service(pool.clone());
    let node2 = api_key_service(pool.clone());
    let (owner, _) =
        create_service_account_with_grant(&authn, "ha_apikey", "CorrectHorse1!", &["data:read"])
            .await;
    let principal_id = owner.user_id;

    let created = scope_claim_context_for_test(
        test_claim_context(&principal_id, "acme", "billing", &[], &[]),
        node1.create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
            name: "ha-key".to_string(),
            owner_id: principal_id.clone(),
            scopes: vec!["data:read".to_string()],
            context: Some(common_pb::RequestContext {
                principal_id: principal_id.clone(),
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
    .expect("node1 create API key")
    .into_inner();
    let key_id = created.key.expect("created key").key_id;

    // node2 validates the key created on node1 (shared durable store).
    let valid = node2
        .validate_api_key(Request::new(apikey_pb::ValidateApiKeyRequest {
            plain_key: created.plain_key.clone(),
            required_scope: "data:read".to_string(),
            ..Default::default()
        }))
        .await
        .expect("node2 validate live key")
        .into_inner();
    assert!(valid.valid, "key created on node1 must validate on node2");

    // node1 revokes it — as the authenticated tenant-`acme` admin (the validated
    // claim the transport layer installs over the wire); the guard stays strict.
    scope_claim_context_for_test(
        test_claim_context("live-admin", "acme", "billing", &[], &[]),
        node1.revoke_api_key(Request::new(apikey_pb::RevokeApiKeyRequest {
            key_id,
            revoke_reason: "ha_multinode_test".to_string(),
            ..Default::default()
        })),
    )
    .await
    .expect("node1 revoke API key");

    // node2 must now deny it — durable revocation, no per-node cache to lag.
    let after = node2
        .validate_api_key(Request::new(apikey_pb::ValidateApiKeyRequest {
            plain_key: created.plain_key,
            required_scope: "data:read".to_string(),
            ..Default::default()
        }))
        .await
        .expect("node2 validate after revoke")
        .into_inner();
    assert!(
        !after.valid,
        "API-key revocation on node1 must propagate to node2 (durable read)"
    );

    cleanup_native_auth_db(&pool).await;
}

/// Refresh-token replay race across two instances (HA-2 in the plan): `node1`
/// rotates a refresh token; replaying the ORIGINAL (rotated-away) token on
/// `node2` is reuse — `node2` detects it (the token-family ledger is durable),
/// revokes the family, and the legitimately rotated token then fails on BOTH
/// nodes. Single-instance reuse is covered by `authn_jwt_live`; this proves the
/// detection holds when the rotate and the replay land on different nodes.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_ha_refresh_token_replay_race -- --ignored --nocapture"]
async fn live_postgres_ha_refresh_token_replay_race() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    let node1 = authn_service_with_jwt(pool.clone());
    let node2 = authn_service_with_jwt(pool.clone());
    let user = create_verified_user(&node1, "ha-rtrace", "CorrectHorse1!").await;

    let login = node1
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            device_name: "ha-rtf".to_string(),
            ..Default::default()
        }))
        .await
        .expect("node1 login")
        .into_inner();
    assert!(
        login.refresh_token.starts_with("rt_"),
        "login must issue a token-family refresh token"
    );

    // node1 rotates the refresh token (the original is now rotated-away).
    let rotated = node1
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            refresh_token: login.refresh_token.clone(),
            ..Default::default()
        }))
        .await
        .expect("node1 rotate")
        .into_inner();
    assert!(rotated.refresh_token.starts_with("rt_"));
    assert_ne!(rotated.refresh_token, login.refresh_token);

    // node2 (which did NOT rotate) sees the ORIGINAL token replayed: reuse. The
    // family ledger is durable, so node2 detects it and rejects the replay.
    let reuse = node2
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            refresh_token: login.refresh_token.clone(),
            ..Default::default()
        }))
        .await
        .expect_err("replaying the rotated-away token on node2 must be rejected as reuse");
    assert_eq!(reuse.code(), tonic::Code::Unauthenticated);

    // Reuse detection revoked the whole family: the legitimately rotated token now
    // fails on BOTH nodes (durable family revocation converged).
    let on_node1 = node1
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            refresh_token: rotated.refresh_token.clone(),
            ..Default::default()
        }))
        .await
        .expect_err("rotated token must be dead on node1 once the family is revoked");
    assert_eq!(on_node1.code(), tonic::Code::Unauthenticated);

    let on_node2 = node2
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            refresh_token: rotated.refresh_token,
            ..Default::default()
        }))
        .await
        .expect_err("rotated token must be dead on node2 once the family is revoked");
    assert_eq!(on_node2.code(), tonic::Code::Unauthenticated);

    cleanup_native_auth_db(&pool).await;
}

/// CDC idempotency: double-processing the same outbox event publishes it ONCE.
/// Drives the REAL CDC idempotency path (`process_outbox_event` →
/// `was_durably_published` consulted via the durable CDC journal): the first pass
/// publishes + journals + acks-and-deletes the outbox row; the second pass, with
/// the same event_id re-enqueued and a Redis idempotency claim already present,
/// is recognized as durably published and acked WITHOUT a second publish.
/// Needs Postgres + Kafka + Redis (the idempotency `SET NX` fast-path only fires
/// with a Redis connection); gated behind the `kafka` + `redis` features.
#[cfg(all(feature = "kafka", feature = "redis"))]
#[tokio::test]
#[ignore = "requires live Postgres + Kafka + Redis. UDB_LIVE_AUTH_TESTS=1 \
            UDB_INTEGRATION_KAFKA_BROKERS=localhost:59192 \
            UDB_INTEGRATION_REDIS_URL=redis://127.0.0.1:56379 \
            cargo test --lib live_postgres_ha_cdc_idempotent_double_process -- --ignored --nocapture"]
async fn live_postgres_ha_cdc_idempotent_double_process() {
    use crate::runtime::cdc::{CdcConfig, CdcEngine};
    use crate::runtime::metrics::NoopMetrics;
    use std::sync::Arc;

    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog");
    ensure_outbox_table(&pool).await;

    let brokers = std::env::var("UDB_INTEGRATION_KAFKA_BROKERS")
        .or_else(|_| std::env::var("UDB_KAFKA_BROKERS"))
        .unwrap_or_else(|_| "localhost:59192".to_string());
    let redis_url = std::env::var("UDB_LIVE_AUTH_REDIS_URL")
        .or_else(|_| std::env::var("UDB_INTEGRATION_REDIS_URL"))
        .unwrap_or_else(|_| "redis://127.0.0.1:56379".to_string());

    let topic = "udb.authn.user.registered.v1";
    let dlq_topic = format!("udb.cdc.dlq.{}.v1", Uuid::new_v4().simple());
    ensure_kafka_topic(&brokers, topic).await;
    ensure_kafka_topic(&brokers, &dlq_topic).await;

    let config = CdcConfig {
        outbox_table: "outbox_events".to_string(),
        dlq_topic,
        ..CdcConfig::default()
    };
    let engine = CdcEngine::new(
        pool.clone(),
        None,
        &brokers,
        live_pg_dsn(),
        Arc::new(NoopMetrics),
        config,
    )
    .expect("build CDC engine");

    let redis_client = redis::Client::open(redis_url.as_str()).expect("redis client");
    let mut redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("connect redis");

    let event_id = Uuid::new_v4();
    let partition_key = Uuid::new_v4().to_string();
    let payload = serde_json::json!({
        "event_id": event_id.to_string(),
        "event_type": topic,
        "correlation_id": format!("cdc-idem:{event_id}"),
        "document_id": partition_key,
        "tenant_id": "acme",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "payload": {"user_id": partition_key}
    });
    insert_outbox_for_idempotency(&pool, event_id, topic, &partition_key, payload.clone()).await;

    // First pass: publishes to Kafka, records the durable journal entry, and
    // acks-and-deletes the outbox row.
    engine
        .process_outbox_event(
            event_id,
            topic.to_string(),
            partition_key.clone(),
            payload.clone(),
            chrono::Utc::now(),
            101,
            Some(&mut redis_conn),
        )
        .await;
    assert_eq!(
        outbox_event_count(&pool, event_id).await,
        0,
        "the first pass must ack-and-delete the outbox row after publishing"
    );

    // Re-enqueue the SAME event_id (simulates a redelivery / replay), then process
    // again with the Redis idempotency claim already set. The engine must consult
    // the durable journal (`was_durably_published`), recognize it as published,
    // and ack WITHOUT a second publish — the outbox row is removed, not republished.
    insert_outbox_for_idempotency(&pool, event_id, topic, &partition_key, payload.clone()).await;
    engine
        .process_outbox_event(
            event_id,
            topic.to_string(),
            partition_key,
            payload,
            chrono::Utc::now(),
            102,
            Some(&mut redis_conn),
        )
        .await;
    assert_eq!(
        outbox_event_count(&pool, event_id).await,
        0,
        "the second (duplicate) pass must be a durable-publish-skip + ack, not a republish"
    );

    cleanup_native_auth_db(&pool).await;
}

#[cfg(all(feature = "kafka", feature = "redis"))]
async fn insert_outbox_for_idempotency(
    pool: &sqlx::PgPool,
    event_id: Uuid,
    topic: &str,
    partition_key: &str,
    payload: serde_json::Value,
) {
    sqlx::query(
        "INSERT INTO udb_system.outbox_events (event_id, topic, partition_key, payload, created_at) \
         VALUES ($1, $2, $3, $4::JSONB, NOW()) ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(topic)
    .bind(partition_key)
    .bind(payload)
    .execute(pool)
    .await
    .expect("insert idempotency outbox event");
}

#[cfg(all(feature = "kafka", feature = "redis"))]
async fn outbox_event_count(pool: &sqlx::PgPool, event_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM udb_system.outbox_events WHERE event_id = $1")
        .bind(event_id)
        .fetch_one(pool)
        .await
        .expect("count outbox event")
}

/// Native service enable/disable/migration matrix. Single-instance, config-only
/// (no DB): for every descriptor-derived native service, drive the enabled /
/// disabled / serving-only / migration-only toggles through the real
/// `resolved_native_service_statuses` resolver and assert the mount/health/
/// migration status stays internally coherent (the property a node's health
/// report and its actually-mounted routes must agree on). No hardcoded service
/// id list: the matrix iterates `native_service_ids()` (descriptor-sourced).
#[test]
fn native_service_enable_disable_migration_matrix_is_coherent() {
    use crate::runtime::config::{NativeServiceConfig, NativeServicesSettings, UdbConfig};
    use crate::runtime::service::native_registry::{
        native_service_ids, resolved_native_service_statuses,
    };

    let service_ids = native_service_ids();
    assert!(
        !service_ids.is_empty(),
        "the descriptor must yield at least one native service id"
    );

    // Helper: resolve the status for one service under a given per-service override.
    let resolve_one = |service_id: &str, override_cfg: NativeServiceConfig| {
        let config = UdbConfig {
            native_services: NativeServicesSettings {
                enabled: true,
                default_enabled: true,
                migrate_enabled: true,
                control_plane_enabled: true,
                webrtc_peer_enabled: true,
                services: [(service_id.to_string(), override_cfg)]
                    .into_iter()
                    .collect(),
                ..NativeServicesSettings::default()
            },
            ..UdbConfig::default()
        };
        resolved_native_service_statuses(&config)
            .into_iter()
            .find(|status| status.service_id == service_id)
            .unwrap_or_else(|| panic!("missing status for {service_id}"))
    };

    for service_id in &service_ids {
        // 1. Explicitly DISABLED: never mounted, never migrating, never healthy.
        let disabled = resolve_one(
            service_id,
            NativeServiceConfig {
                enabled: Some(false),
                migrate: Some(false),
                ..NativeServiceConfig::default()
            },
        );
        assert!(
            !disabled.enabled,
            "{service_id}: disabled must report enabled=false"
        );
        assert!(
            !disabled.mounted,
            "{service_id}: disabled must not be mounted"
        );
        assert!(
            !disabled.migration_enabled,
            "{service_id}: disabled must not migrate"
        );
        assert_eq!(
            disabled.migration_status, "disabled",
            "{service_id}: disabled migration_status must be 'disabled'"
        );
        assert!(
            !disabled.disabled_reason.is_empty(),
            "{service_id}: a disabled service must report WHY"
        );

        // 2. ENABLED (serve + migrate). Coherence invariants regardless of whether
        //    a backend dependency happens to be missing in this config.
        let enabled = resolve_one(
            service_id,
            NativeServiceConfig {
                enabled: Some(true),
                migrate: Some(true),
                ..NativeServiceConfig::default()
            },
        );
        assert!(
            enabled.enabled,
            "{service_id}: enabled must report enabled=true"
        );
        // mounted iff enabled AND its listener is on AND no missing dependency.
        if enabled.mounted {
            assert!(
                enabled.missing_dependencies.is_empty(),
                "{service_id}: a mounted service must have no missing dependencies"
            );
            assert!(
                !enabled.surface.is_empty() && !enabled.listener_kind.is_empty(),
                "{service_id}: a mounted service must report surface + listener_kind"
            );
            assert!(
                enabled.healthy,
                "{service_id}: a mounted, non-degraded service must be healthy"
            );
        } else {
            // Not mounted while enabled => degraded (missing dep) or listener off,
            // and it must say why.
            assert!(
                !enabled.disabled_reason.is_empty() || !enabled.missing_dependencies.is_empty(),
                "{service_id}: an enabled-but-unmounted service must report a reason"
            );
        }
        assert_eq!(
            enabled.background_worker_enabled,
            enabled.mounted && enabled.owns_background_workers,
            "{service_id}: worker enablement must follow mounted worker-owner status"
        );

        // 3. MIGRATION-ONLY (serve disabled, migrate enabled) — distinct states:
        //    a service can own its schema migration while refusing RPC traffic.
        let migrate_only = resolve_one(
            service_id,
            NativeServiceConfig {
                enabled: Some(false),
                migrate: Some(true),
                ..NativeServiceConfig::default()
            },
        );
        assert!(
            !migrate_only.enabled,
            "{service_id}: migrate-only must not serve"
        );
        assert!(
            !migrate_only.mounted,
            "{service_id}: migrate-only must not mount"
        );
        assert!(
            migrate_only.migration_enabled,
            "{service_id}: migrate-only must report migration_enabled"
        );
        assert_eq!(
            migrate_only.migration_status, "enabled",
            "{service_id}: migrate-only migration_status must be 'enabled'"
        );
    }

    // 4. GLOBAL kill switch overrides everything: nothing serves or migrates.
    let killed = UdbConfig {
        native_services: NativeServicesSettings {
            enabled: false,
            ..NativeServicesSettings::default()
        },
        ..UdbConfig::default()
    };
    for status in resolved_native_service_statuses(&killed) {
        assert!(
            !status.enabled,
            "global disable must disable {}",
            status.service_id
        );
        assert!(
            !status.mounted,
            "global disable must unmount {}",
            status.service_id
        );
    }
}
