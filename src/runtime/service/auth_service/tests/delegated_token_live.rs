//! Live Postgres acceptance for delegated job tokens (`ExchangeTokenForJob`):
//! a service holding udb:token:delegate exchanges a user's access token for a
//! token a durable job uses to act for that user. The token is re-validated on
//! every use (user, jti, acting service), and every mint and use is audited.

use super::super::delegated_tokens::{
    DELEGATE_TOKEN_SCOPE, DELEGATED_TOKEN_JTI_PREFIX, TOPIC_DELEGATED_TOKEN_USED,
    TOPIC_TOKEN_DELEGATED,
};
use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::service::method_security::{
    VerifiedClaimContext, scope_claim_context_for_test, test_claim_context,
};
use std::sync::Arc;
use tonic::Request;
use uuid::Uuid;

const PASSWORD: &str = "CorrectHorse1!";
const OUTBOX_RELATION: &str = "udb_system.outbox_events";

fn security() -> crate::runtime::security::SecurityConfig {
    crate::runtime::security::SecurityConfig {
        jwt_private_key: Some(include_str!("../../../testdata/jwt_rs256_private.pem").to_string()),
        jwt_public_key: Some(include_str!("../../../testdata/jwt_rs256_public.pem").to_string()),
        ..Default::default()
    }
}

/// A user's own access token, signed with the broker's key as a sign-in would
/// sign it (`issued_at` lets a test mint an already-expired one).
fn user_token(user_id: &str, tenant: &str, scopes: &[&str], issued_at: u64) -> String {
    let scopes: Vec<String> = scopes.iter().map(|scope| scope.to_string()).collect();
    crate::runtime::security::sign_access_token(
        &security(),
        user_id,
        tenant,
        "billing",
        &scopes,
        &[],
        "",
        &Uuid::new_v4().to_string(),
        "pwd",
        authn_entity_pb::AccountKind::Person as i32,
        issued_at,
    )
    .expect("sign the user's token")
    .expect("a signing key is configured")
    .0
}

fn job_request(
    subject_token: &str,
    job_id: &str,
    ttl_seconds: i64,
) -> Request<authn_pb::ExchangeTokenForJobRequest> {
    Request::new(authn_pb::ExchangeTokenForJobRequest {
        subject_token: subject_token.to_string(),
        purpose: "export".to_string(),
        job_id: job_id.to_string(),
        ttl_seconds,
    })
}

fn decode_detail(status: &tonic::Status) -> crate::proto::ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

async fn validate(
    authn: &super::super::AuthnServiceImpl,
    token: &str,
) -> authn_pb::ValidateTokenResponse {
    authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: token.to_string(),
            token_type: authn_entity_pb::TokenType::JwtAccess as i32,
        }))
        .await
        .expect("validate token")
        .into_inner()
}

async fn authenticate(
    authn: &super::super::AuthnServiceImpl,
    token: &str,
) -> Result<authn_pb::AuthnResponse, tonic::Status> {
    authn
        .authenticate(Request::new(authn_pb::AuthnRequest {
            bearer_token: token.to_string(),
            tenant_hint: "acme".to_string(),
            project_hint: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .map(tonic::Response::into_inner)
}

/// `(subject, actor, actor_account_id)` of every outbox event of `topic` for
/// one delegated token.
async fn delegation_events(
    pool: &sqlx::PgPool,
    topic: &str,
    jti: &str,
) -> Vec<(String, String, String)> {
    sqlx::query_as(
        "SELECT payload->'payload'->>'subject', payload->'payload'->>'actor', \
         payload->'payload'->>'actor_account_id' FROM udb_system.outbox_events \
         WHERE topic = $1 AND payload->'payload'->>'jti' = $2",
    )
    .bind(topic)
    .bind(jti)
    .fetch_all(pool)
    .await
    .expect("read delegation events")
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_delegated_job_tokens -- --ignored --nocapture"]
async fn live_postgres_delegated_job_tokens() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    ensure_outbox_table(&pool).await;
    let authn = authn_service_with_jwt(pool.clone()).with_event_sink(Arc::new(
        super::super::events::OutboxAuthEventSink::new(pool.clone(), OUTBOX_RELATION),
    ));
    let (account, grant) = create_service_account_with_grant(
        &authn,
        "job_runner",
        PASSWORD,
        &["udb:read", "udb:write", DELEGATE_TOKEN_SCOPE],
    )
    .await;
    let person = create_verified_user(&authn, "job_owner", PASSWORD).await;
    let now = now_unix();
    // The user's own token allows read and stream; the grant allows read,
    // write and delegation: the delegated token gets read only.
    let user_bearer = user_token(&person.user_id, "acme", &["udb:read", "udb:stream"], now);

    // The service's own bearer, as the transport would install it.
    let login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: account.email.clone(),
            password: PASSWORD.to_string(),
            device_name: "delegated-token-live".to_string(),
            ..Default::default()
        }))
        .await
        .expect("service-account login")
        .into_inner();
    let service_bearer = validate(&authn, &login.access_token).await;
    assert!(service_bearer.valid);
    let service_ctx = VerifiedClaimContext {
        subject: service_bearer.user_id.clone(),
        service_identity: grant.service_identity.clone(),
        tenant_id: service_bearer.tenant_id.clone(),
        project_id: service_bearer.project_id.clone(),
        scopes: service_bearer.scopes.clone(),
        roles: service_bearer.roles.clone(),
        // CREDENTIAL_TYPE_SERVICE_ACCOUNT
        credential_type: 4,
        credential_id: service_bearer.token_id.clone(),
        auth_method: "pwd".to_string(),
        authenticated: true,
    };
    let exchange = |ctx: VerifiedClaimContext, request| {
        scope_claim_context_for_test(ctx, authn.exchange_token_for_job(request))
    };

    // ── Mint ────────────────────────────────────────────────────────────────
    let minted = exchange(
        service_ctx.clone(),
        job_request(&user_bearer, "export-1", 600),
    )
    .await
    .expect("the service mints a delegated token")
    .into_inner();
    assert!(minted.jti.starts_with(DELEGATED_TOKEN_JTI_PREFIX));
    assert_eq!(minted.subject, person.user_id);
    assert_eq!(minted.actor, grant.service_identity);
    assert_eq!(minted.purpose, "export");
    assert_eq!(minted.job_id, "export-1");
    assert_eq!(minted.scopes, vec!["udb:read"]);
    assert_eq!(minted.tenant_id, "acme");
    assert_eq!(minted.project_id, "billing");
    let after = now_unix() as i64;
    assert!(minted.expires_at_unix > after && minted.expires_at_unix <= after + 600 + 5);
    let claims = crate::runtime::security::validate_bearer_token(&security(), &minted.access_token)
        .expect("the delegated token verifies");
    assert_eq!(claims.delegated, Some(true));
    assert_eq!(claims.job_id.as_deref(), Some("export-1"));
    assert_eq!(claims.purpose.as_deref(), Some("export"));
    assert_eq!(claims.service_identity, None);
    let act = claims.act.clone().expect("act claim");
    assert_eq!(act.sub.as_deref(), Some(grant.service_identity.as_str()));
    assert_eq!(act.account_id.as_deref(), Some(account.user_id.as_str()));
    // The signed purpose is a verified purpose on the served principal.
    let principal =
        crate::runtime::credential_layer::VerifiedPrincipal::from_verified_bearer_claims(&claims);
    assert_eq!(principal.purpose.as_deref(), Some("export"));
    assert_eq!(principal.subject, person.user_id);

    // A longer request is capped by UDB_DELEGATED_TOKEN_MAX_TTL (24h default).
    let capped = exchange(
        service_ctx.clone(),
        job_request(&user_bearer, "export-2", 999_999),
    )
    .await
    .expect("mint with a capped ttl")
    .into_inner();
    let after = now_unix() as i64;
    assert!(capped.expires_at_unix <= after + 24 * 60 * 60 + 5);
    assert!(capped.expires_at_unix > after + 24 * 60 * 60 - 60);

    // ── Use, repeatedly: the user's identity, the delegated scopes ─────────
    for _ in 0..3 {
        let used = validate(&authn, &minted.access_token).await;
        assert!(used.valid, "the delegated token validates");
        assert_eq!(used.user_id, person.user_id);
        assert_eq!(used.tenant_id, "acme");
        assert_eq!(used.scopes, vec!["udb:read"]);
        assert_eq!(used.token_id, minted.jti);
        let who = authenticate(&authn, &minted.access_token)
            .await
            .expect("Authenticate accepts the delegated token")
            .principal
            .expect("principal");
        assert_eq!(who.user_id, person.user_id);
    }

    // ── Audited: the mint and the use, with actor, subject and jti ──────────
    let expected = (
        person.user_id.clone(),
        grant.service_identity.clone(),
        account.user_id.clone(),
    );
    assert_eq!(
        delegation_events(&pool, TOPIC_TOKEN_DELEGATED, &minted.jti).await,
        vec![expected.clone()]
    );
    let used = delegation_events(&pool, TOPIC_DELEGATED_TOKEN_USED, &minted.jti).await;
    assert!(!used.is_empty(), "every use is audited");
    assert!(used.iter().all(|event| *event == expected), "{used:?}");

    // ── Refusals at mint ────────────────────────────────────────────────────
    // A delegated token is never exchanged again.
    let again = exchange(
        service_ctx.clone(),
        job_request(&minted.access_token, "export-3", 600),
    )
    .await
    .expect_err("no re-exchange");
    assert_eq!(again.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        decode_detail(&again).policy_decision_id,
        "delegated_token_not_exchangeable"
    );
    // A service's token is not a user's token.
    let service_subject = exchange(
        service_ctx.clone(),
        job_request(&login.access_token, "export-4", 600),
    )
    .await
    .expect_err("a service token is not a user token");
    assert_eq!(service_subject.code(), tonic::Code::PermissionDenied);
    // Another tenant's user.
    let elsewhere = user_token(
        &person.user_id,
        &Uuid::new_v4().to_string(),
        &["udb:read"],
        now,
    );
    let mismatch = exchange(
        service_ctx.clone(),
        job_request(&elsewhere, "export-5", 600),
    )
    .await
    .expect_err("the user's tenant must be the service's");
    assert_eq!(mismatch.code(), tonic::Code::PermissionDenied);
    // An expired user token (well past the clock-skew leeway).
    let expired = user_token(&person.user_id, "acme", &["udb:read"], now - 10_000);
    let stale = exchange(service_ctx.clone(), job_request(&expired, "export-6", 600))
        .await
        .expect_err("an expired user token cannot be exchanged");
    assert_eq!(stale.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        decode_detail(&stale).policy_decision_id,
        "delegated_token_subject_expired"
    );
    // A person cannot mint, even holding the scope.
    let human = test_claim_context(
        &person.user_id,
        "acme",
        "billing",
        &[DELEGATE_TOKEN_SCOPE],
        &[],
    );
    let refused = exchange(human, job_request(&user_bearer, "export-7", 600))
        .await
        .expect_err("only a service mints delegated tokens");
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);
    // An expired token is refused on use with UDB_TOKEN_EXPIRED.
    let expired_use = authenticate(&authn, &expired)
        .await
        .expect_err("an expired token is refused");
    assert_eq!(expired_use.code(), tonic::Code::Unauthenticated);
    assert_eq!(
        crate::runtime::error_reasons::reason_of(&expired_use).as_deref(),
        Some("UDB_TOKEN_EXPIRED")
    );

    // ── Revocation: the jti ─────────────────────────────────────────────────
    authn
        .revoke_jti_for_test(&minted.jti, "acme", minted.expires_at_unix as u64)
        .await
        .expect("revoke the delegated token");
    assert!(!validate(&authn, &minted.access_token).await.valid);
    let revoked = authenticate(&authn, &minted.access_token)
        .await
        .expect_err("a revoked jti is refused");
    assert_eq!(revoked.code(), tonic::Code::Unauthenticated);
    assert_eq!(
        crate::runtime::error_reasons::reason_of(&revoked).as_deref(),
        Some("UDB_TOKEN_REVOKED")
    );
    // The other token minted for the same user still works.
    assert!(validate(&authn, &capped.access_token).await.valid);

    // ── Revocation: the actor loses udb:token:delegate ──────────────────────
    authn
        .replace_service_account_grant(Request::new(authn_pb::ReplaceServiceAccountGrantRequest {
            tenant_id: grant.tenant_id.clone(),
            user_id: account.user_id.clone(),
            approved_scopes: vec!["udb:read".to_string(), "udb:write".to_string()],
            project_id: grant.project_id.clone(),
            reason: "live: drop delegation".to_string(),
            expected_revision: grant.revision,
        }))
        .await
        .expect("replace the grant without the delegate scope");
    assert!(
        !validate(&authn, &capped.access_token).await.valid,
        "a service that lost udb:token:delegate no longer acts for anyone"
    );
    let actor_gone = authenticate(&authn, &capped.access_token)
        .await
        .expect_err("the actor no longer delegates");
    assert_eq!(
        crate::runtime::error_reasons::reason_of(&actor_gone).as_deref(),
        Some("UDB_TOKEN_REVOKED")
    );

    // ── Revocation: the user is suspended ──────────────────────────────────
    let (account2, grant2) = create_service_account_with_grant(
        &authn,
        "job_runner_two",
        PASSWORD,
        &["udb:read", DELEGATE_TOKEN_SCOPE],
    )
    .await;
    let login2 = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: account2.email.clone(),
            password: PASSWORD.to_string(),
            device_name: "delegated-token-live-2".to_string(),
            ..Default::default()
        }))
        .await
        .expect("second service-account login")
        .into_inner();
    let bearer2 = validate(&authn, &login2.access_token).await;
    let ctx2 = VerifiedClaimContext {
        subject: bearer2.user_id.clone(),
        service_identity: grant2.service_identity.clone(),
        tenant_id: bearer2.tenant_id.clone(),
        project_id: bearer2.project_id.clone(),
        scopes: bearer2.scopes.clone(),
        roles: bearer2.roles.clone(),
        credential_type: 4,
        credential_id: bearer2.token_id.clone(),
        auth_method: "pwd".to_string(),
        authenticated: true,
    };
    let for_user = exchange(ctx2.clone(), job_request(&user_bearer, "export-8", 600))
        .await
        .expect("a second service mints for the user")
        .into_inner();
    assert!(validate(&authn, &for_user.access_token).await.valid);
    authn
        .change_user_status(Request::new(authn_pb::ChangeUserStatusRequest {
            user_id: person.user_id.clone(),
            new_status: authn_entity_pb::UserStatus::Suspended as i32,
            reason: "live: suspend the job owner".to_string(),
            ..Default::default()
        }))
        .await
        .expect("suspend the user");
    assert!(
        !validate(&authn, &for_user.access_token).await.valid,
        "a suspended user's delegated token stops working"
    );
    let user_gone = exchange(ctx2.clone(), job_request(&user_bearer, "export-9", 600))
        .await
        .expect_err("a suspended user's token cannot be exchanged");
    assert_eq!(user_gone.code(), tonic::Code::PermissionDenied);

    // ── Revocation: the actor's grant is revoked ────────────────────────────
    let person2 = create_verified_user(&authn, "job_owner_two", PASSWORD).await;
    let user2_bearer = user_token(&person2.user_id, "acme", &["udb:*"], now_unix());
    let for_user2 = exchange(ctx2.clone(), job_request(&user2_bearer, "export-10", 0))
        .await
        .expect("mint for the second user")
        .into_inner();
    // An owner's wildcard is narrowed to the grant, never the delegate scope.
    assert_eq!(for_user2.scopes, vec!["udb:read"]);
    assert!(validate(&authn, &for_user2.access_token).await.valid);
    authn
        .revoke_service_account_grant(Request::new(authn_pb::RevokeServiceAccountGrantRequest {
            tenant_id: grant2.tenant_id.clone(),
            user_id: account2.user_id.clone(),
            reason: "live: revoke the actor".to_string(),
        }))
        .await
        .expect("revoke the second service's grant");
    assert!(
        !validate(&authn, &for_user2.access_token).await.valid,
        "revoking the actor's grant kills its delegated tokens"
    );

    cleanup_native_auth_db(&pool).await;
}
