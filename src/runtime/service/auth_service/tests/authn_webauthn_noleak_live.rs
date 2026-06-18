//! Negative (no-leak) classification test for the WebAuthn challenge write path
//! (06.5.7.1, depends on 06.5.1.1 + 06.5.2.1).
//!
//! `store_webauthn_challenge` inserts a challenge row keyed by `challenge_id`
//! (the table's primary key). A duplicate `challenge_id` is a unique violation
//! (`23505`), which the raw-sqlx fallback now classifies through
//! `executor_utils::sqlx_error_to_status` instead of wrapping the raw error in
//! `Status::internal`. This test forces that exact duplicate write against the
//! proto-derived `WebAuthnChallenge` table and asserts the classified status is
//! `AlreadyExists` and carries NO raw DB text (no "duplicate key" / "23505" /
//! "constraint" leak).
//!
//! The `WebAuthnChallenge` table is a proto-derived native entity, so it is
//! present in `native_service_catalog_ddl()` regardless of the `webauthn` cargo
//! feature; this test therefore needs no feature gate. It is live-gated exactly
//! like its sibling no-leak test (`authn_noleak_live`).

use super::support::*;
use crate::runtime::native_catalog::native_model;
use uuid::Uuid;

/// Build the exact INSERT `store_webauthn_challenge` raw-sqlx fallback emits, for
/// a fixed `challenge_id`, so two calls with the same id collide on the PK.
async fn insert_webauthn_challenge(
    pool: &sqlx::PgPool,
    challenge_id: &str,
) -> Result<(), sqlx::Error> {
    let model = native_model(
        "udb.core.authn.entity.v1.WebAuthnChallenge",
        &[
            "challenge_id",
            "user_id",
            "ceremony",
            "state_json",
            "tenant_id",
            "project_id",
            "expires_at",
        ],
    );
    let rel = &model.relation;
    let sql = format!(
        "INSERT INTO {rel} ({}, {}, {}, {}, {}, {}, {}) \
         VALUES ($1::UUID, $2::UUID, $3, $4::JSONB, $5, $6, to_timestamp($7::DOUBLE PRECISION))",
        model.q("challenge_id"),
        model.q("user_id"),
        model.q("ceremony"),
        model.q("state_json"),
        model.q("tenant_id"),
        model.q("project_id"),
        model.q("expires_at"),
    );
    sqlx::query(&sql)
        .bind(challenge_id)
        .bind(Uuid::new_v4().to_string())
        .bind("registration")
        .bind("{}")
        .bind("acme")
        .bind("billing")
        .bind(now_unix() as f64 + 300.0)
        .execute(pool)
        .await
        .map(|_| ())
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_webauthn_duplicate_write_classified_no_leak -- --ignored --nocapture"]
async fn live_postgres_webauthn_duplicate_write_classified_no_leak() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    // A FIXED challenge_id so the second insert collides on the primary key.
    let challenge_id = Uuid::new_v4().to_string();

    // First write must succeed.
    insert_webauthn_challenge(&pool, &challenge_id)
        .await
        .expect("first WebAuthn challenge insert should succeed");

    // Second write with the SAME id is a unique violation. Classify it exactly the
    // way `store_webauthn_challenge`'s raw-sqlx fallback now does (06.5.2.1).
    let err = insert_webauthn_challenge(&pool, &challenge_id)
        .await
        .expect_err("duplicate WebAuthn challenge insert must fail");
    let status = crate::runtime::executor_utils::sqlx_error_to_status(
        "store WebAuthn challenge failed",
        &err,
    );

    assert_eq!(
        status.code(),
        tonic::Code::AlreadyExists,
        "duplicate native write must map to AlreadyExists, got {:?}: {}",
        status.code(),
        status.message()
    );

    // No raw DB text may leak through the classified status message. The helper is
    // allowed to name the conflicting constraint (schema metadata, not row data),
    // so "constraint" itself is permitted — what must NOT appear is the driver's
    // raw error string: the SQLSTATE code, the "duplicate key" phrase, or the
    // "Key (...)=(...)" value detail that would echo row contents.
    let msg = status.message().to_ascii_lowercase();
    for banned in [
        "duplicate key",
        "23505",
        "sqlstate",
        "key (",
        "already exists with",
    ] {
        assert!(
            !msg.contains(banned),
            "classified status leaked raw DB text ({banned}): {}",
            status.message()
        );
    }
    assert!(
        msg.contains("already exists"),
        "AlreadyExists status should carry a safe 'already exists' message, got: {}",
        status.message()
    );

    cleanup_native_auth_db(&pool).await;
}
