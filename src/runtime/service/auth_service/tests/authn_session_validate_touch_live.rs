//! Actual native-session persistence: atomic validation/activity update and
//! verified JWT identity binding. The unfiltered native CI lane runs these.

use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::authn::{
    PostgresSessionStore, SessionRecord, SessionStore, SessionValidationScope, hash_secret,
    validate_session, validate_session_for_scope,
};
use crate::runtime::native_catalog::{NativeModel, native_model};
use std::sync::Arc;
use std::time::Duration;
use tonic::Request;

const KEY: &[u8] = b"live-session-validation-secret";

fn session_model() -> NativeModel {
    native_model(
        "udb.core.authn.entity.v1.Session",
        &["session_token_lookup", "is_active", "last_active_at"],
    )
}

fn scope(record: &SessionRecord) -> SessionValidationScope {
    SessionValidationScope {
        principal_id: record.principal_id.clone(),
        tenant_id: record.tenant_id.clone(),
        project_id: record.project_id.clone(),
        service_identity: record.service_identity.clone(),
    }
}

fn record(raw: &str, user_id: &str, now: u64) -> SessionRecord {
    SessionRecord {
        session_id_hash: hash_secret(raw, KEY),
        principal_id: user_id.to_string(),
        user_id: user_id.to_string(),
        tenant_id: "acme".to_string(),
        project_id: "billing".to_string(),
        scopes: vec!["udb:read".to_string()],
        roles: vec!["reader".to_string()],
        relationship_version: "live-session-validation".to_string(),
        client_fingerprint: "live-session-validation-device".to_string(),
        created_at_unix: now,
        updated_at_unix: now - 1,
        expires_at_unix: now + 600,
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires live Postgres; run in the unfiltered native live lane"]
async fn live_postgres_session_validate_touch_preserves_boundaries_and_identity() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    let user = create_verified_user(&authn, "validate-touch", "CorrectHorse1!").await;
    let store = PostgresSessionStore::new(pool.clone(), "");
    let now = now_unix();
    let raw = format!("sess_{}", uuid::Uuid::new_v4().simple());
    let base = record(&raw, &user.user_id, now);
    let expected = scope(&base);

    for (label, updated, expires, revoked, idle, accepted) in [
        ("active", now - 1, now + 600, 0, 30, true),
        ("absolute equality", now - 1, now, 0, 0, false),
        ("absolute expired", now - 1, now - 1, 0, 0, false),
        ("idle equality", now - 30, now + 600, 0, 30, true),
        ("idle expired", now - 31, now + 600, 0, 30, false),
        ("future activity", now + 100, now + 600, 0, 1, true),
        ("zero activity", 0, now + 600, 0, 1, true),
        ("indefinite expiry", now - 1, 0, 0, 0, true),
        ("huge idle TTL", 1, now + 600, 0, u64::MAX, true),
        ("revoked", now - 1, now + 600, now - 1, 0, false),
    ] {
        let candidate = SessionRecord {
            updated_at_unix: updated,
            expires_at_unix: expires,
            revoked_at_unix: revoked,
            ..base.clone()
        };
        store.put(&candidate).await.expect("seed boundary session");
        let validated = validate_session_for_scope(&store, &raw, KEY, now, idle, &expected)
            .await
            .expect("validate actual session");
        assert_eq!(validated.is_some(), accepted, "{label}");
        let persisted = store.get(&base.session_id_hash).await.unwrap().unwrap();
        assert_eq!(
            persisted.updated_at_unix,
            if accepted { now } else { updated },
            "{label}"
        );
        if let Some(validated) = validated {
            assert_eq!(validated.updated_at_unix, now, "{label}");
            assert_eq!(validated.principal_id, base.principal_id);
            assert_eq!(validated.user_id, base.user_id);
            assert_eq!(validated.tenant_id, base.tenant_id);
            assert_eq!(validated.project_id, base.project_id);
            assert_eq!(validated.scopes, base.scopes);
            assert_eq!(validated.roles, base.roles);
            assert_eq!(validated.relationship_version, base.relationship_version);
            assert_eq!(validated.client_fingerprint, base.client_fingerprint);
        }
    }

    // A rejection must not extend activity, including an otherwise active row.
    for mismatch in ["principal", "tenant", "project", "service"] {
        store.put(&base).await.unwrap();
        let mut wrong = scope(&base);
        match mismatch {
            "principal" => wrong.principal_id = uuid::Uuid::new_v4().to_string(),
            "tenant" => wrong.tenant_id = "other-tenant".to_string(),
            "project" => wrong.project_id = "other-project".to_string(),
            "service" => wrong.service_identity = "other-service".to_string(),
            _ => unreachable!(),
        }
        assert!(
            validate_session_for_scope(&store, &raw, KEY, now, 0, &wrong)
                .await
                .unwrap()
                .is_none(),
            "{mismatch}"
        );
        assert_eq!(
            store
                .get(&base.session_id_hash)
                .await
                .unwrap()
                .unwrap()
                .updated_at_unix,
            base.updated_at_unix,
            "{mismatch}"
        );
    }
    assert!(
        validate_session(&store, "sess_unknown", KEY, now, 0)
            .await
            .unwrap()
            .is_none()
    );

    // NativeModel's decoder rounds epoch values to BIGINT. Fractional database
    // timestamps must use those same boundaries in the atomic predicate.
    let m = session_model();
    for (offset, accepted) in [(0.49_f64, true), (0.51_f64, false)] {
        store.put(&base).await.unwrap();
        sqlx::query(&format!(
            "UPDATE {} SET {} = to_timestamp($2::DOUBLE PRECISION) WHERE {} = $1",
            m.relation,
            m.q("last_active_at"),
            m.q("session_token_lookup")
        ))
        .bind(&base.session_id_hash)
        .bind(now as f64 - 30.0 - offset)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            validate_session_for_scope(&store, &raw, KEY, now, 30, &expected)
                .await
                .unwrap()
                .is_some(),
            accepted,
            "fractional idle {offset}"
        );
    }
    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run in the unfiltered native live lane"]
async fn live_postgres_session_validate_touch_rechecks_concurrent_revoke() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    let user = create_verified_user(&authn, "validate-revoke", "CorrectHorse1!").await;
    let store = Arc::new(PostgresSessionStore::new(pool.clone(), ""));
    let now = now_unix();
    let raw = format!("sess_{}", uuid::Uuid::new_v4().simple());
    let base = record(&raw, &user.user_id, now);
    store.put(&base).await.unwrap();
    let m = session_model();
    let mut revoke = pool.begin().await.unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *revoke)
        .await
        .unwrap();
    sqlx::query(&format!(
        "UPDATE {} SET {} = FALSE, {} = to_timestamp($2::BIGINT::DOUBLE PRECISION) WHERE {} = $1",
        m.relation,
        m.q("is_active"),
        m.q("last_active_at"),
        m.q("session_token_lookup")
    ))
    .bind(&base.session_id_hash)
    .bind((now - 10) as i64)
    .execute(&mut *revoke)
    .await
    .unwrap();
    let expected = scope(&base);
    let pending_store = store.clone();
    let mut pending = tokio::spawn(async move {
        validate_session_for_scope(pending_store.as_ref(), &raw, KEY, now, 0, &expected).await
    });
    // Observe the real lock wait, not an assumed pg_sleep duration. This also
    // observes the old get→touch implementation waiting on its UPDATE.
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1::INT = ANY(pg_blocking_pids(pid)))")
                .bind(blocker).fetch_one(&pool).await.unwrap();
            if blocked { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await;
    if observed.is_err() {
        pending.abort();
        let _ = pending.await;
        revoke.rollback().await.unwrap();
        panic!("session validation did not enter the expected real row-lock wait");
    }
    revoke.commit().await.unwrap();
    let validated = tokio::time::timeout(Duration::from_secs(5), &mut pending)
        .await
        .expect("validation progresses after revoke commits")
        .expect("validation task joins")
        .expect("validation store succeeds");
    assert!(
        validated.is_none(),
        "a concurrently revoked row cannot yield a stale valid session"
    );
    let persisted = store.get(&base.session_id_hash).await.unwrap().unwrap();
    assert!(persisted.is_revoked());
    assert_eq!(
        persisted.updated_at_unix,
        now - 10,
        "failed validation cannot touch the revoked row"
    );
    drop(
        tokio::time::timeout(Duration::from_secs(3), pool.acquire())
            .await
            .expect("pool capacity remains usable")
            .unwrap(),
    );
    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run in the unfiltered native live lane"]
async fn live_postgres_session_jwt_refuses_durable_identity_change_without_touch() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    let user = create_verified_user(&authn, "jwt-session-scope", "CorrectHorse1!").await;
    let login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(login.session_id.starts_with("sess_"));
    let hash = hash_secret(&login.session_id, &authn.hash_key());
    let store = PostgresSessionStore::new(pool.clone(), "");
    let mut base = store.get(&hash).await.unwrap().unwrap();
    base.updated_at_unix = now_unix() - 60;
    for mismatch in ["principal", "tenant", "project", "service"] {
        let mut changed = base.clone();
        match mismatch {
            "principal" => changed.principal_id = uuid::Uuid::new_v4().to_string(),
            "tenant" => changed.tenant_id = "other-tenant".to_string(),
            "project" => changed.project_id = "other-project".to_string(),
            "service" => changed.service_identity = "other-service".to_string(),
            _ => unreachable!(),
        }
        store.put(&changed).await.unwrap();
        let validated = authn
            .validate_token(Request::new(authn_pb::ValidateTokenRequest {
                token: login.access_token.clone(),
                token_type: authn_entity_pb::TokenType::JwtAccess as i32,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(
            !validated.valid,
            "native ValidateToken must refuse {mismatch} change"
        );
        let denied = authn
            .authenticate(Request::new(authn_pb::AuthnRequest {
                bearer_token: login.access_token.clone(),
                ..Default::default()
            }))
            .await
            .expect_err("native bearer authentication must refuse changed session identity");
        assert_eq!(denied.code(), tonic::Code::Unauthenticated);
        assert_eq!(
            store.get(&hash).await.unwrap().unwrap().updated_at_unix,
            changed.updated_at_unix,
            "{mismatch} refusal must not touch"
        );
    }
    store.put(&base).await.unwrap();
    assert!(
        authn
            .validate_token(Request::new(authn_pb::ValidateTokenRequest {
                token: login.access_token,
                token_type: authn_entity_pb::TokenType::JwtAccess as i32,
            }))
            .await
            .unwrap()
            .into_inner()
            .valid,
        "the unchanged durable identity remains valid"
    );
    cleanup_native_auth_db(&pool).await;
}
