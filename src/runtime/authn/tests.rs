use super::*;

const KEY: &[u8] = b"test-hash-secret";

// ── Live-Postgres test harness ───────────────────────────────────────────────
// In-memory stores are forbidden — they pass while the real Postgres path is
// broken. The store-level tests below run against a live Postgres (gated by
// `UDB_LIVE_AUTH_TESTS=1` / `UDB_LIVE_AUTH_PG_DSN`) and reset the native auth
// schema first so each test starts clean. They are `#[ignore]`d so the default
// `cargo test` does not require a database; run them as the final check with:
//   UDB_LIVE_AUTH_TESTS=1 cargo test --lib runtime::authn -- --ignored --nocapture

fn live_pg_dsn() -> Option<String> {
    std::env::var("UDB_LIVE_AUTH_PG_DSN")
        .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
        .ok()
        .or_else(|| {
            std::env::var("UDB_LIVE_AUTH_TESTS")
                .ok()
                .filter(|v| matches!(v.as_str(), "1" | "true" | "yes"))
                .map(|_| "postgres://udb:udb@localhost:55432/udb".to_string())
        })
}

/// Connect to the live auth Postgres and re-create the native auth schema from
/// the proto-generated DDL, or return `None` when no live DB is configured.
async fn live_auth_pool() -> Option<sqlx::PgPool> {
    let dsn = live_pg_dsn()?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&dsn)
        .await
        .unwrap_or_else(|err| panic!("connect live auth postgres at {dsn}: {err}"));
    cleanup_live_auth_db(&pool).await;
    for stmt in crate::runtime::native_catalog::native_service_catalog_ddl() {
        sqlx::raw_sql(&stmt)
            .execute(&pool)
            .await
            .unwrap_or_else(|err| panic!("native auth DDL failed: {err}\nSQL:\n{stmt}"));
    }
    Some(pool)
}

async fn cleanup_live_auth_db(pool: &sqlx::PgPool) {
    sqlx::query("DROP SCHEMA IF EXISTS udb_authz CASCADE")
        .execute(pool)
        .await
        .expect("drop authz schema");
    sqlx::query("DROP SCHEMA IF EXISTS udb_authn CASCADE")
        .execute(pool)
        .await
        .expect("drop authn schema");
    sqlx::query("DROP EXTENSION IF EXISTS pg_partman CASCADE")
        .execute(pool)
        .await
        .expect("drop pg_partman extension");
    sqlx::query("DROP SCHEMA IF EXISTS partman CASCADE")
        .execute(pool)
        .await
        .expect("drop partman schema");
}

fn test_now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

async fn seed_live_user(pool: &sqlx::PgPool, label: &str) -> String {
    let user_id = uuid::Uuid::new_v4().to_string();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let now = test_now_unix();
    PostgresUserStore::new(pool.clone(), "")
        .put_user(UserRecord {
            user_id: user_id.clone(),
            username: format!("{label}_{suffix}"),
            email: format!("{label}_{suffix}@example.com"),
            password_hash: hash_secret("password", KEY),
            account_kind: authn_entity_pb::AccountKind::Person as i32,
            status: authn_entity_pb::UserStatus::Active as i32,
            tenant_id: "acme".into(),
            full_name: label.to_string(),
            created_at_unix: now,
            updated_at_unix: now,
            profile_attributes_json: "{}".into(),
            ..Default::default()
        })
        .await
        .expect("seed live auth user");
    user_id
}

#[test]
fn hash_secret_is_deterministic_and_keyed() {
    let a = hash_secret("session-abc", KEY);
    let b = hash_secret("session-abc", KEY);
    assert_eq!(a, b, "same secret + key → same hash");
    assert!(a.starts_with("hmac-sha256:"));
    // Different key → different hash (a leaked DB without the key can't match).
    assert_ne!(a, hash_secret("session-abc", b"other-key"));
    // Different secret → different hash.
    assert_ne!(a, hash_secret("session-xyz", KEY));
    // The raw secret never appears in the stored form.
    assert!(!a.contains("session-abc"));
}

#[tokio::test]
#[ignore = "requires Postgres; run with UDB_LIVE_AUTH_TESTS=1 ... -- --ignored"]
async fn session_lifecycle_create_validate_expire_revoke() {
    let Some(pool) = live_auth_pool().await else {
        eprintln!("skipped: set UDB_LIVE_AUTH_TESTS=1 or UDB_LIVE_AUTH_PG_DSN");
        return;
    };
    let user_id = seed_live_user(&pool, "session_lifecycle").await;
    let store = PostgresSessionStore::new(pool, "");
    let raw = "sess-raw-123";
    let rec = SessionRecord {
        session_id_hash: hash_secret(raw, KEY),
        principal_id: "user_1".into(),
        user_id,
        tenant_id: "acme".into(),
        created_at_unix: test_now_unix(),
        updated_at_unix: test_now_unix(),
        expires_at_unix: 2000,
        ..Default::default()
    };
    store.put(&rec).await.unwrap();

    // Active before expiry.
    assert!(
        validate_session(&store, raw, KEY, 1500, 0)
            .await
            .unwrap()
            .is_some()
    );
    // Wrong raw id → no match (hash differs).
    assert!(
        validate_session(&store, "wrong", KEY, 1500, 0)
            .await
            .unwrap()
            .is_none()
    );
    // Expired at/after expires_at.
    assert!(
        validate_session(&store, raw, KEY, 2000, 0)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        validate_session(&store, raw, KEY, 2500, 0)
            .await
            .unwrap()
            .is_none()
    );

    // Revoke → inactive even before expiry.
    assert!(store.revoke(&hash_secret(raw, KEY), 1200).await.unwrap());
    assert!(
        validate_session(&store, raw, KEY, 1500, 0)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires Postgres; run with UDB_LIVE_AUTH_TESTS=1 ... -- --ignored"]
async fn refresh_session_extends_active_but_not_expired() {
    let Some(pool) = live_auth_pool().await else {
        eprintln!("skipped: set UDB_LIVE_AUTH_TESTS=1 or UDB_LIVE_AUTH_PG_DSN");
        return;
    };
    let user_id = seed_live_user(&pool, "refresh_session").await;
    let store = PostgresSessionStore::new(pool, "");
    let raw = "s1";
    let record = SessionRecord {
        session_id_hash: hash_secret(raw, KEY),
        principal_id: "u".into(),
        user_id,
        created_at_unix: test_now_unix(),
        updated_at_unix: test_now_unix(),
        expires_at_unix: 2000,
        ..Default::default()
    };
    store.put(&record).await.unwrap();
    // Active at 1500: refresh with ttl 1000 → new expiry 2500.
    let r = refresh_session(&store, raw, KEY, 1500, 1000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.expires_at_unix, 2500);
    assert_eq!(r.updated_at_unix, 1500);
    assert!(
        validate_session(&store, raw, KEY, 2400, 0)
            .await
            .unwrap()
            .is_some()
    );
    // Once expired, it cannot be refreshed back to life.
    assert!(
        refresh_session(&store, raw, KEY, 3000, 1000)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires Postgres; run with UDB_LIVE_AUTH_TESTS=1 ... -- --ignored"]
async fn revoke_all_for_principal_revokes_only_that_principal() {
    let Some(pool) = live_auth_pool().await else {
        eprintln!("skipped: set UDB_LIVE_AUTH_TESTS=1 or UDB_LIVE_AUTH_PG_DSN");
        return;
    };
    let user_1_id = seed_live_user(&pool, "revoke_user_1").await;
    let user_2_id = seed_live_user(&pool, "revoke_user_2").await;
    let store = PostgresSessionStore::new(pool, "");
    for (raw, principal) in [("a", "user_1"), ("b", "user_1"), ("c", "user_2")] {
        let user_id = if principal == "user_1" {
            user_1_id.clone()
        } else {
            user_2_id.clone()
        };
        let record = SessionRecord {
            session_id_hash: hash_secret(raw, KEY),
            principal_id: principal.into(),
            user_id,
            created_at_unix: test_now_unix(),
            updated_at_unix: test_now_unix(),
            expires_at_unix: 9999,
            ..Default::default()
        };
        store.put(&record).await.unwrap();
    }
    let revoked = store.revoke_all_for_principal("user_1", 500).await.unwrap();
    assert_eq!(revoked, 2);
    assert!(
        validate_session(&store, "a", KEY, 100, 0)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        validate_session(&store, "b", KEY, 100, 0)
            .await
            .unwrap()
            .is_none()
    );
    // user_2's session is untouched.
    assert!(
        validate_session(&store, "c", KEY, 100, 0)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
#[ignore = "requires Postgres; run with UDB_LIVE_AUTH_TESTS=1 ... -- --ignored"]
async fn api_key_validate_hash_lookup_expiry_revocation() {
    let Some(pool) = live_auth_pool().await else {
        eprintln!("skipped: set UDB_LIVE_AUTH_TESTS=1 or UDB_LIVE_AUTH_PG_DSN");
        return;
    };
    let store = PostgresApiKeyStore::new(pool, "");
    let raw = "udbk_abc123.secretpart";
    store
        .put(ApiKeyRecord {
            key_prefix: api_key_prefix(raw),
            key_hash: hash_secret(raw, KEY),
            principal_id: "svc-1".into(),
            tenant_id: "acme".into(),
            scopes: vec!["udb:read".into()],
            expires_at_unix: 2000,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(api_key_prefix(raw), "udbk_abc123");
    // Valid before expiry.
    assert!(
        validate_api_key(&store, raw, KEY, 1000)
            .await
            .unwrap()
            .is_some()
    );
    // Tampered secret with the same prefix → hash mismatch → reject.
    assert!(
        validate_api_key(&store, "udbk_abc123.WRONG", KEY, 1000)
            .await
            .unwrap()
            .is_none()
    );
    // Expired.
    assert!(
        validate_api_key(&store, raw, KEY, 2000)
            .await
            .unwrap()
            .is_none()
    );
    // Revoked.
    assert!(store.revoke("udbk_abc123", 500).await.unwrap());
    assert!(
        validate_api_key(&store, raw, KEY, 1000)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires Postgres; run with UDB_LIVE_AUTH_TESTS=1 ... -- --ignored"]
async fn rotate_api_key_issues_new_and_revokes_old() {
    let Some(pool) = live_auth_pool().await else {
        eprintln!("skipped: set UDB_LIVE_AUTH_TESTS=1 or UDB_LIVE_AUTH_PG_DSN");
        return;
    };
    let store = PostgresApiKeyStore::new(pool, "");
    let old = "udbk_old.s1";
    store
        .put(ApiKeyRecord {
            key_prefix: api_key_prefix(old),
            key_hash: hash_secret(old, KEY),
            principal_id: "svc".into(),
            scopes: vec!["udb:read".into()],
            expires_at_unix: 9999,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        validate_api_key(&store, old, KEY, 100)
            .await
            .unwrap()
            .is_some()
    );

    let new = "udbk_new.s2";
    let template = ApiKeyRecord {
        principal_id: "svc".into(),
        scopes: vec!["udb:read".into()],
        expires_at_unix: 9999,
        ..Default::default()
    };
    let new_rec = rotate_api_key(&store, &api_key_prefix(old), new, KEY, 500, template)
        .await
        .unwrap();
    assert_eq!(new_rec.key_prefix, "udbk_new");

    // Old key no longer valid; new key valid.
    assert!(
        validate_api_key(&store, old, KEY, 600)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        validate_api_key(&store, new, KEY, 600)
            .await
            .unwrap()
            .is_some()
    );
}

#[test]
fn auth_catalog_ddl_is_idempotent_and_covers_all_tables() {
    let ddl = auth_catalog_ddl("udb_system");
    let joined = ddl.join("\n");
    for fragment in [
        "CREATE TABLE IF NOT EXISTS \"udb_authn\".\"users\"",
        "CREATE TABLE IF NOT EXISTS \"udb_authn\".\"sessions\"",
        "CREATE TABLE IF NOT EXISTS \"udb_authn\".\"otps\"",
        "CREATE TABLE IF NOT EXISTS \"udb_authn\".\"api_keys\"",
    ] {
        assert!(
            joined.contains(fragment),
            "missing generated DDL fragment {fragment}"
        );
    }
    assert!(
        joined.contains("UDB:proto_manifest_checksum"),
        "native authn DDL must carry proto manifest metadata"
    );
}

#[test]
fn authn_config_defaults_and_sessions_usable_gate() {
    let cfg = AuthnConfig::default();
    assert!(!cfg.session_enabled);
    assert_eq!(cfg.session_backend, "postgres");
    assert_eq!(cfg.session_ttl_secs, 86_400);
    assert_eq!(cfg.session_idle_ttl_secs, 3_600);
    // Sessions require BOTH enabled and a hash secret (no raw-id storage).
    assert!(!cfg.sessions_usable());
    let ok = AuthnConfig {
        session_enabled: true,
        session_hash_secret: "secret".into(),
        ..AuthnConfig::default()
    };
    assert!(ok.sessions_usable());
    // Enabled but no secret → not usable.
    let no_secret = AuthnConfig {
        session_enabled: true,
        ..AuthnConfig::default()
    };
    assert!(!no_secret.sessions_usable());
}

#[test]
fn external_jwt_provider_maps_configured_claims() {
    let provider = ExternalJwtProvider::new(ExternalProviderConfig {
        provider_id: "auth0".into(),
        tenant_claim: "org_id".into(),
        roles_claim: "https://udb/roles".into(),
        ..ExternalProviderConfig::default()
    });
    let claims = r#"{
            "sub": "u1",
            "org_id": "acme",
            "project_id": "billing",
            "https://udb/roles": ["admin", "writer"],
            "scopes": "udb:read udb:write"
        }"#;
    let p = provider.map_identity("", claims).unwrap();
    assert_eq!(p.provider_id, "auth0");
    assert_eq!(p.subject, "u1");
    assert_eq!(p.principal_id, "auth0:u1");
    assert_eq!(p.tenant_id, "acme");
    assert_eq!(p.project_id, "billing");
    assert_eq!(p.roles, vec!["admin", "writer"]);
    // Space-separated scope string is split into a list.
    assert_eq!(p.scopes, vec!["udb:read", "udb:write"]);
    assert_eq!(p.auth_method, "external");
}

#[test]
fn external_roles_alone_do_not_bypass_udb_policy() {
    use crate::runtime::authz::{
        Authorizer, AuthzPolicy, AuthzQuery, AuthzSnapshot, Effect, ResourceRef,
    };
    use std::collections::BTreeMap;

    let provider = ExternalJwtProvider::new(ExternalProviderConfig::default());
    let principal = provider
        .map_identity("", r#"{"sub":"u1","tenant_id":"acme","roles":["admin"]}"#)
        .unwrap();
    let res = ResourceRef::message("acme.Invoice");
    let attrs = BTreeMap::new();
    let q = AuthzQuery {
        principal: &principal,
        resource: &res,
        action: "data.delete",
        purpose: "x",
        attributes: &attrs,
    };

    // No UDB policy → an externally-asserted "admin" role grants nothing.
    let empty = AuthzSnapshot {
        version: "v".into(),
        ..Default::default()
    };
    assert!(
        !empty.authorize(&q).allowed,
        "external roles must not bypass UDB authorization"
    );

    // Only a UDB-owned policy granting that role allows the action.
    let snap = AuthzSnapshot {
        version: "v".into(),
        policies: vec![AuthzPolicy {
            id: "p".into(),
            role: "admin".into(),
            action: "*".into(),
            effect: Effect::Allow,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(snap.authorize(&q).allowed);
}
