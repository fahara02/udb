//! Phase 0 (seal sensitive surfaces) regression tests.
//!
//! These tests prove that credential material persisted in Postgres is never
//! surfaced through an outbound DTO. They deliberately put real secrets in the
//! database first (a hashed password via `create_user`, an encrypted TOTP secret
//! via MFA enrollment) and then assert that:
//!   * the stored columns are non-empty (the secret really exists), and
//!   * every outbound `User` / `Session` response blanks the sensitive fields.
//! If a future change re-copies a hash/secret into a response, these tests fail.

use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::authn::totp;
use crate::runtime::native_catalog::{native_model, native_relation};
use tonic::Request;

/// Read the raw stored values of two credential columns for a user, proving the
/// secret material exists in Postgres independent of any redaction in the API.
async fn stored_user_secrets(pool: &sqlx::PgPool, user_id: &str) -> (String, String) {
    let msg = "udb.core.authn.entity.v1.User";
    let (schema, table) = native_relation(msg).expect("native relation for User");
    let model = native_model(msg, &["user_id", "password_hash", "totp_secret_enc"]);
    let id_col = model.column("user_id");
    let pw_col = model.column("password_hash");
    let totp_col = model.column("totp_secret_enc");
    let sql = format!(
        "SELECT COALESCE({pw_col}, '') AS pw, COALESCE({totp_col}, '') AS totp \
         FROM {schema}.{table} WHERE {id_col} = $1::uuid"
    );
    let row: (String, String) = sqlx::query_as(&sql)
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|err| panic!("read stored user secrets: {err}"));
    row
}

fn assert_user_redacted(user: &authn_entity_pb::User) {
    assert!(
        user.password_hash.is_empty(),
        "password_hash must never be surfaced (got {} chars)",
        user.password_hash.len()
    );
    assert!(
        user.totp_secret_enc.is_empty(),
        "totp_secret_enc must never be surfaced (got {} chars)",
        user.totp_secret_enc.len()
    );
}

fn assert_session_redacted(session: &authn_entity_pb::Session) {
    assert!(
        session.session_token_lookup.is_empty(),
        "session_token_lookup must never be surfaced"
    );
    assert!(
        session.session_token_hash.is_empty(),
        "session_token_hash must never be surfaced"
    );
    assert!(
        session.csrf_token_hash.is_empty(),
        "csrf_token_hash must never be surfaced"
    );
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_user_responses_redact_secrets -- --ignored --nocapture"]
async fn live_postgres_user_responses_redact_secrets() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "noleak", "CorrectHorse1!").await;

    // Enroll + confirm TOTP so the stored row holds a real encrypted secret.
    let enroll = authn
        .enroll_mfa(Request::new(authn_pb::EnrollMfaRequest {
            user_id: user.user_id.clone(),
            mfa_type: authn_entity_pb::AuthFactorKind::Totp as i32,
            ..Default::default()
        }))
        .await
        .expect("enroll MFA")
        .into_inner();
    let totp_code = totp::current_code(&enroll.totp_secret, now_unix()).expect("totp");
    authn
        .confirm_mfa_enrollment(Request::new(authn_pb::ConfirmMfaEnrollmentRequest {
            user_id: user.user_id.clone(),
            otp_id: enroll.verify_otp_id.clone(),
            code: totp_code,
            ..Default::default()
        }))
        .await
        .expect("confirm MFA");

    // Storage genuinely holds the password hash and TOTP secret.
    let (stored_pw, stored_totp) = stored_user_secrets(&pool, &user.user_id).await;
    assert!(
        !stored_pw.is_empty(),
        "precondition: password hash must be persisted"
    );
    assert!(
        !stored_totp.is_empty(),
        "precondition: TOTP secret must be persisted after enrollment"
    );

    // GetUser must redact both.
    let got = authn
        .get_user(Request::new(authn_pb::GetUserRequest {
            user_id: user.user_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("get user")
        .into_inner()
        .user
        .expect("user");
    assert!(got.mfa_enabled, "the user really is MFA-enrolled");
    assert_user_redacted(&got);

    // ListUsers must redact for every returned user.
    let listed = authn
        .list_users(Request::new(authn_pb::ListUsersRequest {
            tenant_id: "acme".to_string(),
            ..Default::default()
        }))
        .await
        .expect("list users")
        .into_inner();
    assert!(listed.users.iter().any(|u| u.user_id == user.user_id));
    for u in &listed.users {
        assert_user_redacted(u);
    }

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_session_responses_redact_hashes -- --ignored --nocapture"]
async fn live_postgres_session_responses_redact_hashes() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "noleaksess", "CorrectHorse1!").await;

    let session = authn
        .create_session(Request::new(authn_pb::CreateSessionRequest {
            principal: Some(authn_pb::Principal {
                principal_id: user.user_id.clone(),
                subject: user.user_id.clone(),
                user_id: user.user_id.clone(),
                tenant_id: "acme".to_string(),
                project_id: "billing".to_string(),
                scopes: vec!["data:read".to_string()],
                auth_method: "session".to_string(),
                ..Default::default()
            }),
            ttl_seconds: 120,
            client_fingerprint: "noleak-session".to_string(),
        }))
        .await
        .expect("create session")
        .into_inner();

    let got = authn
        .get_session(Request::new(authn_pb::GetSessionRequest {
            session_id: session.session_id.clone(),
        }))
        .await
        .expect("get session")
        .into_inner()
        .session
        .expect("session");
    assert_eq!(got.user_id, user.user_id);
    assert_session_redacted(&got);

    let listed = authn
        .list_sessions(Request::new(authn_pb::ListSessionsRequest {
            user_id: user.user_id.clone(),
            active_only: false,
            ..Default::default()
        }))
        .await
        .expect("list sessions")
        .into_inner();
    assert!(!listed.sessions.is_empty(), "the session should be listed");
    for s in &listed.sessions {
        assert_session_redacted(s);
    }

    cleanup_native_auth_db(&pool).await;
}
