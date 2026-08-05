//! Live Postgres acceptance for `TransferServiceAccountGrant` (identity #2 — the
//! customer's atomic-cutover need).
//!
//! Customer symptom this reproduces: the account currently bound to a stable
//! service identity `I` is unavailable, and the operator must hand `I` (and its
//! approved scopes) to a fresh account WITHOUT (a) a window in which nobody owns
//! `I`, (b) the old account still authenticating as `I`, and (c) a collision on
//! the deployment-wide `service_identity` unique index that retains revoked rows.
//! The supported path is `TransferServiceAccountGrant`, which RE-POINTS the single
//! grant row from A to B in one transaction under revision CAS.
//!
//! These tests drive the SERVED RPC (`AuthnService::transfer_service_account_grant`)
//! through the claim-bound path (`scope_claim_context_for_test` installs the
//! `VerifiedClaimContext` the tower `MethodSecurityLayer` would install on the
//! wire) and assert the observable outcome via the same served surfaces a client
//! uses (login → JWT → ValidateToken) plus the durable grant store.
//!
//! Revert-proofing: if the re-point / CAS / source-revocation is reverted (e.g.
//! back to a rotate-then-create), then A keeps resolving `I`
//! (`get_grant_by_user(A)` stays `Some`, A's pre-transfer JWT stays valid), or a
//! second row appears for `I`, or a no-owner window opens — each of which fails a
//! dedicated assertion below.
//!
//! Run: UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_transfer_service_account_grant -- --ignored --nocapture

use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::native_catalog::native_model;
use crate::runtime::service::method_security::{scope_claim_context_for_test, test_claim_context};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tonic::Request;
use uuid::Uuid;

const GRANT_MSG: &str = "udb.core.authn.entity.v1.ServiceAccountGrant";

fn login_request(username: &str, password: &str) -> authn_pb::LoginRequest {
    authn_pb::LoginRequest {
        username: username.to_string(),
        password: password.to_string(),
        device_name: "transfer-grant-live-test".to_string(),
        tenant_hint: "acme".to_string(),
        project_hint: "billing".to_string(),
        ..Default::default()
    }
}

async fn validate_access_token(
    svc: &super::super::AuthnServiceImpl,
    token: String,
) -> authn_pb::ValidateTokenResponse {
    svc.validate_token(Request::new(authn_pb::ValidateTokenRequest {
        token,
        token_type: authn_entity_pb::TokenType::JwtAccess as i32,
    }))
    .await
    .expect("validate service access token")
    .into_inner()
}

/// Create + activate a SERVICE_ACCOUNT with a known password but NO typed grant —
/// the valid transfer DESTINATION shape (an active service account in the same
/// tenant/project that holds no grant, revoked or otherwise). Mirrors the first
/// half of [`create_service_account_with_grant`] without the grant install.
async fn create_active_service_account(
    svc: &super::super::AuthnServiceImpl,
    prefix: &str,
    password: &str,
) -> authn_entity_pb::User {
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
        .expect("create ungranted service account")
        .into_inner();
    let created_user = created.user.expect("created service account");
    assert!(verify_issued_otp(svc, &created.otp_id).await.verified);
    svc.get_user(Request::new(authn_pb::GetUserRequest {
        user_id: created_user.user_id,
        ..Default::default()
    }))
    .await
    .expect("re-read active service account")
    .into_inner()
    .user
    .expect("active service account present")
}

async fn transfer(
    svc: &super::super::AuthnServiceImpl,
    from: &str,
    to: &str,
    expected_revision: i64,
    reason: &str,
) -> Result<authn_pb::TransferServiceAccountGrantResponse, tonic::Status> {
    scope_claim_context_for_test(
        test_claim_context(
            "grant-admin",
            "acme",
            "billing",
            &["udb:authn:manage-grants"],
            &[],
        ),
        svc.transfer_service_account_grant(Request::new(
            authn_pb::TransferServiceAccountGrantRequest {
                tenant_id: "acme".to_string(),
                from_user_id: from.to_string(),
                to_user_id: to.to_string(),
                expected_revision,
                reason: reason.to_string(),
            },
        )),
    )
    .await
    .map(|r| r.into_inner())
}

/// Total ACTIVE grant rows carrying `service_identity = identity`, deployment-wide.
/// The atomic re-point keeps exactly ONE such row at all times; a rotate-then-create
/// revert would leave TWO (or violate the retained-revoked-row unique index).
async fn active_owner_count(pool: &sqlx::PgPool, identity: &str) -> i64 {
    let m = native_model(GRANT_MSG, &["service_identity", "status"]);
    let sql = format!(
        "SELECT COUNT(*) FROM {rel} WHERE {ident} = $1 AND {status} = $2",
        rel = m.relation,
        ident = m.q("service_identity"),
        status = m.q("status"),
    );
    sqlx::query_scalar(&sql)
        .bind(identity)
        .bind(super::super::grants::STATUS_ACTIVE)
        .fetch_one(pool)
        .await
        .expect("count active owners of identity")
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_transfer_service_account_grant_atomic_cutover -- --ignored --nocapture"]
async fn live_postgres_transfer_service_account_grant_atomic_cutover() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service_with_jwt(pool.clone());
    let password = "CorrectHorse1!";

    // Service account A holds the grant + stable identity I.
    let (a, grant) =
        create_service_account_with_grant(&svc, "xfer_src", password, &["udb:read", "udb:write"])
            .await;
    let identity = grant.service_identity.clone();
    // Destination B: active service account, same tenant/project, NO grant.
    let b = create_active_service_account(&svc, "xfer_dst", password).await;

    // A mints a real service JWT bound to identity I BEFORE the cutover.
    let a_login = svc
        .login(Request::new(login_request(&a.email, password)))
        .await
        .expect("A service login")
        .into_inner();
    let a_before = validate_access_token(&svc, a_login.access_token.clone()).await;
    assert!(a_before.valid, "A resolves I before the transfer");
    assert_eq!(
        a_before
            .principal
            .as_ref()
            .expect("A principal")
            .service_identity,
        identity
    );
    assert_eq!(
        active_owner_count(&pool, &identity).await,
        1,
        "identity I has exactly one owner before the transfer"
    );

    // (c) STALE expected_revision → FAILED_PRECONDITION (nothing moves).
    let stale = transfer(&svc, &a.user_id, &b.user_id, grant.revision + 99, "stale")
        .await
        .expect_err("stale-revision transfer must be rejected");
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition);
    assert!(
        super::super::grants::get_grant_by_user(&pool, "acme", &a.user_id)
            .await
            .expect("read A grant after rejected stale transfer")
            .is_some(),
        "a rejected transfer must not move the grant"
    );

    // Self-transfer (from == to) → INVALID_ARGUMENT.
    let self_xfer = transfer(&svc, &a.user_id, &a.user_id, grant.revision, "self")
        .await
        .expect_err("self-transfer must be rejected");
    assert_eq!(self_xfer.code(), tonic::Code::InvalidArgument);

    // Cross-tenant claim must NOT move a tenant-A grant (served body-tenant guard).
    let cross_tenant = scope_claim_context_for_test(
        test_claim_context(
            "tenant-b-admin",
            "tenant-b",
            "billing",
            &["udb:authn:manage-grants"],
            &[],
        ),
        svc.transfer_service_account_grant(Request::new(
            authn_pb::TransferServiceAccountGrantRequest {
                tenant_id: "acme".to_string(),
                from_user_id: a.user_id.clone(),
                to_user_id: b.user_id.clone(),
                expected_revision: grant.revision,
                reason: "cross-tenant must fail".to_string(),
            },
        )),
    )
    .await
    .expect_err("tenant-B claim must not transfer a tenant-A grant");
    assert_eq!(cross_tenant.code(), tonic::Code::PermissionDenied);

    // The real, served, claim-bound cutover A -> B under the current revision CAS.
    let resp = transfer(
        &svc,
        &a.user_id,
        &b.user_id,
        grant.revision,
        "atomic cutover",
    )
    .await
    .expect("served grant transfer A->B");
    let moved = resp.grant.expect("transferred grant");
    assert_eq!(moved.user_id, b.user_id, "grant is now owned by B");
    assert_eq!(
        moved.service_identity, identity,
        "identity I rides the same row unchanged"
    );
    assert_eq!(
        moved.revision,
        grant.revision + 1,
        "CAS re-point bumps the revision by one"
    );
    assert_eq!(
        resp.previous_user_id, a.user_id,
        "response reports A as the previous owner (for a reverse transfer)"
    );

    // (a) B now resolves identity I — durable store AND the served login path.
    let b_grant = super::super::grants::get_grant_by_user(&pool, "acme", &b.user_id)
        .await
        .expect("read B grant")
        .expect("B owns the grant after transfer");
    assert_eq!(b_grant.service_identity, identity);
    assert_eq!(b_grant.user_id, b.user_id);
    let b_login = svc
        .login(Request::new(login_request(&b.email, password)))
        .await
        .expect("B service login after transfer")
        .into_inner();
    let b_validated = validate_access_token(&svc, b_login.access_token).await;
    assert!(b_validated.valid);
    assert_eq!(
        b_validated
            .principal
            .as_ref()
            .expect("B principal")
            .service_identity,
        identity,
        "B resolves identity I on the served path"
    );

    // (b) A no longer resolves identity I.
    assert!(
        super::super::grants::get_grant_by_user(&pool, "acme", &a.user_id)
            .await
            .expect("read A grant after transfer")
            .is_none(),
        "A must own no grant after the re-point (revoke-then-create would leave a retained revoked row)"
    );
    let a_after = validate_access_token(&svc, a_login.access_token.clone()).await;
    assert!(
        !a_after.valid,
        "A's already-issued service JWT must stop resolving to identity I"
    );
    // A can no longer obtain a credential that resolves to I: either login fails
    // closed, or the minted principal is not identity I.
    if let Ok(relogin) = svc
        .login(Request::new(login_request(&a.email, password)))
        .await
    {
        let relogged = validate_access_token(&svc, relogin.into_inner().access_token).await;
        let minted_identity = relogged
            .principal
            .map(|p| p.service_identity)
            .unwrap_or_default();
        assert_ne!(
            minted_identity, identity,
            "A must never mint a fresh credential that resolves to the transferred identity I"
        );
    }

    // (d) Atomicity invariant: exactly ONE active row carries I, owned by B; A none.
    let m = native_model(GRANT_MSG, &["service_identity", "user_id", "status"]);
    let rows: Vec<(String, String)> = sqlx::query_as(&format!(
        // `user_id` is a UUID column; cast to text so it decodes into `String`
        // and compares against the proto `user_id` string form.
        "SELECT {uid}::text, {status} FROM {rel} WHERE {ident} = $1",
        uid = m.q("user_id"),
        status = m.q("status"),
        rel = m.relation,
        ident = m.q("service_identity"),
    ))
    .bind(identity.as_str())
    .fetch_all(&pool)
    .await
    .expect("read every grant row for identity I");
    assert_eq!(
        rows.len(),
        1,
        "identity I must be carried by exactly one row deployment-wide (no retained revoked twin)"
    );
    assert_eq!(rows[0].0, b.user_id, "the single I-row is owned by B");
    assert_eq!(rows[0].1, moved.status, "the single I-row is ACTIVE");
    assert_eq!(active_owner_count(&pool, &identity).await, 1);

    // Destination-already-has-grant guard: C already owns its own grant, so the
    // grant cannot be transferred onto it (would collide the per-user unique index).
    let (c, _c_grant) =
        create_service_account_with_grant(&svc, "xfer_occupied", password, &["udb:read"]).await;
    let occupied = transfer(
        &svc,
        &b.user_id,
        &c.user_id,
        moved.revision,
        "onto occupied",
    )
    .await
    .expect_err("transfer onto an account that already has a grant must be rejected");
    assert_eq!(occupied.code(), tonic::Code::FailedPrecondition);

    // Deterministic inverse: transfer BACK B -> A restores A as owner of I.
    let back = transfer(
        &svc,
        &b.user_id,
        &a.user_id,
        moved.revision,
        "reverse cutover",
    )
    .await
    .expect("reverse transfer B->A");
    let back_grant = back.grant.expect("reverse-transferred grant");
    assert_eq!(back_grant.user_id, a.user_id);
    assert_eq!(back_grant.service_identity, identity);
    assert_eq!(back.previous_user_id, b.user_id);
    assert!(
        super::super::grants::get_grant_by_user(&pool, "acme", &b.user_id)
            .await
            .expect("read B grant after reverse transfer")
            .is_none(),
        "after the reverse transfer B owns no grant"
    );
    assert_eq!(active_owner_count(&pool, &identity).await, 1);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_transfer_service_account_grant_no_no_owner_window -- --ignored --nocapture"]
async fn live_postgres_transfer_service_account_grant_no_no_owner_window() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service_with_jwt(pool.clone());
    let password = "CorrectHorse1!";

    let (a, grant) =
        create_service_account_with_grant(&svc, "gap_src", password, &["udb:read"]).await;
    let identity = grant.service_identity.clone();
    let b = create_active_service_account(&svc, "gap_dst", password).await;

    // A concurrent reader samples the owner-count of identity I as fast as it can
    // while a burst of transfers hammers the identity across A<->B. Because the
    // re-point is a single atomic UPDATE, an MVCC reader ALWAYS sees exactly one
    // owner — never a 0-owner gap and never a 2-owner overlap. A rotate-then-create
    // revert (revoke in one statement/tx, create in another) would let the reader
    // observe a 0-owner (or 2-owner) sample, tripping the min/max assertions.
    let reader_pool = pool.clone();
    let probe_identity = identity.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_reader = stop.clone();
    let reader = tokio::spawn(async move {
        let mut min_seen = i64::MAX;
        let mut max_seen = i64::MIN;
        while !stop_reader.load(Ordering::Relaxed) {
            let n = active_owner_count(&reader_pool, &probe_identity).await;
            min_seen = min_seen.min(n);
            max_seen = max_seen.max(n);
            tokio::task::yield_now().await;
        }
        (min_seen, max_seen)
    });

    // Alternate A<->B under CAS, threading the revision from each response.
    let mut owner = a.user_id.clone();
    let mut other = b.user_id.clone();
    let mut rev = grant.revision;
    for i in 0..8 {
        let resp = transfer(&svc, &owner, &other, rev, &format!("gap-loop-{i}"))
            .await
            .unwrap_or_else(|err| panic!("gap-loop transfer #{i} failed: {err}"));
        let g = resp.grant.expect("looped grant");
        assert_eq!(g.service_identity, identity);
        assert_eq!(g.user_id, other);
        rev = g.revision;
        std::mem::swap(&mut owner, &mut other);
    }

    stop.store(true, Ordering::Relaxed);
    let (min_seen, max_seen) = reader.await.expect("join owner-count reader");
    assert_eq!(
        min_seen, 1,
        "identity I must ALWAYS have an owner during transfers — a 0 sample is an atomicity gap"
    );
    assert_eq!(
        max_seen, 1,
        "identity I must NEVER have two owners during transfers — a 2 sample is a duplicate window"
    );

    cleanup_native_auth_db(&pool).await;
}
