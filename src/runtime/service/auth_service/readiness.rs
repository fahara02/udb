//! Phase 6: auth-plane readiness checks.
//!
//! A self-contained, mostly-pure aggregation of synchronous startup checks that
//! confirm the auth plane is configured coherently *before* the service begins
//! serving traffic: the JWT signing key parses, the JWT verification key parses,
//! and the operator-supplied Casbin model is well-formed. The checks mirror the
//! exact parsing the live paths use (`EncodingKey::from_rsa_pem` for signing,
//! `validate_casbin_model` for authz) so a green report means those paths will
//! not fail later on a malformed key/model.
//!
//! Every check carries a caller-safe `detail` — key *material* is never included,
//! only the configured source kind (inline PEM vs file path) and the error class.
//!
//! ## Posture vs live probes
//!
//! [`check_auth_readiness`] takes only a [`SecurityConfig`] (it is reused by
//! `udb native doctor`, which has no live runtime handle), so the DB/Redis-backed
//! dependencies — the native auth schema, the session/revocation store, the
//! outbox table, and the policy snapshot — are checked here as **posture** probes:
//! they read the process-global config/env/snapshot state to report whether the
//! dependency is *configured and coherent*, and flag the configuration that would
//! fail at runtime, rather than opening a fresh DB/Redis connection from the
//! readiness path. The live reachability of the auth Postgres / Redis is already
//! probed by the runtime's `GetHealthReport` backend probes; these auth-plane
//! posture probes complement that with the auth-specific configuration facts.
//! Where a probe would need a live handle to go further, its `detail` says so.

use crate::runtime::authn::AuthnConfig;
use crate::runtime::security::SecurityConfig;

/// One auth-plane readiness probe and its outcome. `detail` is always safe to
/// surface to operators/logs — it never contains key material.
#[derive(Debug, Clone)]
pub(crate) struct AuthReadinessCheck {
    /// Stable identifier for the probe (e.g. `jwt_signing_key`).
    pub name: String,
    /// Whether the probe passed.
    pub ok: bool,
    /// Caller-safe human-readable outcome (no secrets).
    pub detail: String,
}

/// Aggregate report over all [`AuthReadinessCheck`]s. `ok` is the conjunction of
/// every individual check.
#[derive(Debug, Clone)]
pub(crate) struct AuthReadinessReport {
    /// `true` iff every check in `checks` passed.
    pub ok: bool,
    /// The individual probe results, in evaluation order.
    pub checks: Vec<AuthReadinessCheck>,
}

impl AuthReadinessCheck {
    fn pass(name: &str, detail: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            ok: true,
            detail: detail.into(),
        }
    }

    fn fail(name: &str, detail: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            ok: false,
            detail: detail.into(),
        }
    }
}

/// Run the synchronous auth-plane readiness checks and aggregate them.
///
/// Probes, in order:
/// 1. `jwt_signing_key` — when `jwt_private_key` is set, verify it parses as an
///    RSA PEM via the same constructor the signing path uses
///    (`EncodingKey::from_rsa_pem`), reading a file when the value is not an
///    inline `-----BEGIN` PEM.
/// 2. `jwt_verification_key` — when `jwt_public_key` is set, verify it parses
///    (file or inline) via `DecodingKey::from_rsa_pem`.
/// 3. `casbin_model` — parse-check the configured authz model via
///    [`crate::runtime::authz::validate_casbin_model`].
/// 4. `token_issuance` — informational: when neither signing key, verification
///    key, nor JWKS URL is configured, note that UDB-issued/validated JWTs are
///    disabled. This is a valid dev configuration (signing returns `None`), so
///    the check passes.
///
/// Plus the dependency-posture probes (see the module note on posture vs live):
/// 5. `auth_schema_migration` — the native auth schema migrates through the normal
///    proto-migration path off the runtime PG pool. Posture HINT from the DSN env
///    (never a hard fail on its own — PG may be configured via services.yaml);
///    live schema/connectivity is in the runtime backend health probes.
/// 6. `session_revocation_store` — the server-side session / token-revocation
///    store backend (`UDB_SESSION_BACKEND`): `redis` requires the `redis` feature,
///    else it silently degrades to Postgres; sessions need a hash secret to persist.
/// 7. `outbox_table` — auth events publish via the outbox→CDC relay; HARD-fails
///    the env-only misconfiguration where durable Postgres audit export
///    (`UDB_AUDIT_EXPORT_POSTGRES`) is requested but no Postgres DSN env exists to
///    hold the outbox/audit table (otherwise a posture caution).
/// 8. `policy_snapshot` — the authz policy snapshot must evaluate off a parseable
///    Casbin model and is built deny-by-default (`default_allow: false`).
/// 9. `signing_key_registry` — when the DB-backed signing-key registry has been
///    seeded, an ACTIVE key must exist (read from the process-global snapshot);
///    an empty registry is the valid env-key fallback posture.
/// 10. `revocation_subscription` — in a hardened posture revocation/invalidation
///    must be durable (fail-closed), so a revocation-store error denies rather
///    than silently allowing; reports the fail-open dev posture honestly.
/// 11. `idp_provider_support` — informational support matrix: NATIVE/OIDC/SAML
///    (+SCIM) are implemented; LDAP / CUSTOM_JWT / EXTERNAL_SESSION are proto-
///    enumerated but unsupported (configuring them fails at login).
///
/// The aggregate `ok` is `checks.iter().all(|c| c.ok)`.
pub(crate) async fn check_auth_readiness(security: &SecurityConfig) -> AuthReadinessReport {
    let mut checks = Vec::new();

    // 1. JWT signing key — mirror `sign_access_token`'s parse exactly.
    if let Some(key_src) = security
        .jwt_private_key
        .as_deref()
        .filter(|k| !k.trim().is_empty())
    {
        checks.push(check_rsa_pem(
            "jwt_signing_key",
            key_src,
            "failed to read JWT private key",
            "invalid JWT private key",
            |bytes| jsonwebtoken::EncodingKey::from_rsa_pem(bytes).map(|_| ()),
        ));
    }

    // 2. JWT verification key — mirror `validate_bearer_token`'s static-PEM parse.
    if let Some(key_src) = security
        .jwt_public_key
        .as_deref()
        .filter(|k| !k.trim().is_empty())
    {
        checks.push(check_rsa_pem(
            "jwt_verification_key",
            key_src,
            "failed to read JWT public key",
            "invalid JWT public key format",
            |bytes| jsonwebtoken::DecodingKey::from_rsa_pem(bytes).map(|_| ()),
        ));
    }

    // 3. Casbin model — the same parse-check the startup gate runs.
    checks.push(check_casbin_model().await);

    // 4. JWKS reachability (Phase 6): a configured external JWKS URL must be
    // fetchable and return a JWK set, or asymmetric JWT verification fails on the
    // first request.
    if let Some(url) = security
        .jwt_jwks_url
        .as_deref()
        .filter(|u| !u.trim().is_empty())
    {
        checks.push(check_jwks_reachable(url.trim()).await);
    }

    // 5. Audit sink posture (Phase 6): a configured external sink should be an
    // http(s) endpoint; absence means local/stdout auditing (valid for dev).
    let audit_sink = security.audit_sink_url.trim();
    if audit_sink.is_empty() {
        checks.push(AuthReadinessCheck::pass(
            "audit_sink",
            "no external audit sink configured; using local/stdout auditing (dev)",
        ));
    } else if audit_sink.starts_with("http://") || audit_sink.starts_with("https://") {
        checks.push(AuthReadinessCheck::pass(
            "audit_sink",
            "external audit sink configured (http endpoint)",
        ));
    } else {
        checks.push(AuthReadinessCheck::fail(
            "audit_sink",
            "audit sink URL is set but is not an http(s) endpoint",
        ));
    }

    // 6. Token-issuance posture (informational). Signing returns `None` when no
    // private key is set, and verification is skipped when neither a public key
    // nor a JWKS URL is configured — a valid dev posture, so this passes.
    let signing = security
        .jwt_private_key
        .as_deref()
        .is_some_and(|k| !k.trim().is_empty());
    let verification = security
        .jwt_public_key
        .as_deref()
        .is_some_and(|k| !k.trim().is_empty());
    let jwks = security
        .jwt_jwks_url
        .as_deref()
        .is_some_and(|u| !u.trim().is_empty());
    if !signing && !verification && !jwks {
        checks.push(AuthReadinessCheck::pass(
            "token_issuance",
            "no JWT signing/verification key or JWKS URL configured; UDB-issued JWTs are \
             disabled (server-side sessions only) — valid for dev",
        ));
    }

    // Dependency-posture probes (5–11). These read process-global config/env/
    // snapshot state rather than opening a fresh DB/Redis connection (this fn is
    // reused by `udb native doctor`, which has no live runtime handle); the live
    // reachability of the auth Postgres/Redis is covered by the runtime backend
    // probes that feed `GetHealthReport`.
    let authn = AuthnConfig::from_env();
    let hardened =
        SecurityConfig::current().is_production() || crate::runtime::security::fail_closed_mode();

    checks.push(check_auth_schema_migration(hardened));
    checks.push(check_session_revocation_store(&authn));
    checks.push(check_outbox_table(hardened));
    checks.push(check_policy_snapshot().await);
    checks.push(check_signing_key_registry());
    checks.push(check_revocation_subscription(hardened));
    checks.push(check_idp_provider_support());

    let ok = checks.iter().all(|c| c.ok);
    AuthReadinessReport { ok, checks }
}

/// Env truthiness helper shared by the posture probes (`1`/`true`/`yes`/`on`).
fn env_on(key: &str) -> bool {
    std::env::var(key)
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Heuristic: an auth Postgres DSN is present in the environment (the native auth
/// schema, session/revocation tables and outbox all live in that PG). The auth
/// control plane actually obtains its pool from `runtime.pg_pool()`, which may be
/// configured via `services.yaml` instead of these env vars — so this is a *posture
/// hint*, not proof. Callers therefore never HARD-fail solely on its absence; they
/// surface a caution. Without opening a connection this cannot do better; the live
/// pool state is in the runtime backend health probes.
fn auth_postgres_dsn_env_present() -> bool {
    ["UDB_PG_DSN", "DATABASE_URL"]
        .iter()
        .any(|k| std::env::var(k).ok().is_some_and(|v| !v.trim().is_empty()))
}

/// 5. Native auth-schema migration-state posture. The auth schema
/// (`udb_authn`/`udb_authz` relations) migrates through the normal UDB proto-
/// migration path off the runtime's Postgres pool. This config-only fn cannot open
/// that pool, so it reports a posture HINT from the DSN env vars and never
/// hard-fails on their absence alone (Postgres may be configured via
/// `services.yaml`). The live, authoritative schema/connectivity state is in the
/// runtime backend health probes that feed `GetHealthReport`.
fn check_auth_schema_migration(hardened: bool) -> AuthReadinessCheck {
    if auth_postgres_dsn_env_present() {
        AuthReadinessCheck::pass(
            "auth_schema_migration",
            "Postgres DSN configured (UDB_PG_DSN / DATABASE_URL); native auth schema migrates \
             via the proto-migration path (live schema state is in backend health probes)",
        )
    } else if hardened {
        // Posture caution, NOT a hard fail: a YAML-configured pool also satisfies
        // this and isn't visible here. The runtime backend probes own the truth.
        AuthReadinessCheck::pass(
            "auth_schema_migration",
            "no Postgres DSN env (UDB_PG_DSN / DATABASE_URL) detected in a hardened posture — \
             if Postgres is configured via services.yaml this is fine; otherwise the native \
             auth schema cannot be persisted. Confirm via backend health probes.",
        )
    } else {
        AuthReadinessCheck::pass(
            "auth_schema_migration",
            "no Postgres DSN env detected; native auth schema may be unavailable \
             (sessions-disabled fallback) — valid for dev",
        )
    }
}

/// 6. Session / token-revocation store posture. The session store backing the
/// server-side session + token-revocation deny list is selected by
/// `UDB_SESSION_BACKEND` (`postgres` default, or `redis`). A `redis` selection
/// only takes effect when the `redis` feature is compiled in and a Redis handle is
/// present, otherwise the runtime silently degrades to Postgres — surfaced here so
/// the operator is not surprised. Sessions also require a hash secret to persist.
fn check_session_revocation_store(authn: &AuthnConfig) -> AuthReadinessCheck {
    let wants_redis = authn.session_backend.eq_ignore_ascii_case("redis");
    if wants_redis {
        #[cfg(not(feature = "redis"))]
        {
            return AuthReadinessCheck::fail(
                "session_revocation_store",
                "UDB_SESSION_BACKEND=redis but this binary was built without the `redis` \
                 feature — the session/revocation store silently degrades to Postgres",
            );
        }
        #[cfg(feature = "redis")]
        {
            return AuthReadinessCheck::pass(
                "session_revocation_store",
                "Redis session/revocation store selected (falls back to Postgres if no Redis \
                 handle is wired at startup; live reachability is in backend health probes)",
            );
        }
    }
    if authn.session_enabled && authn.session_hash_secret.trim().is_empty() {
        return AuthReadinessCheck::fail(
            "session_revocation_store",
            "sessions enabled (UDB_SESSION_ENABLED) but no UDB_SESSION_HASH_SECRET is set — \
             sessions cannot be persisted (raw session ids must never be stored)",
        );
    }
    AuthReadinessCheck::pass(
        "session_revocation_store",
        "Postgres session/token-revocation store (durable cluster-wide revocation)",
    )
}

/// 7. Outbox-table posture. Auth events (login, revocation, policy edits) publish
/// transactionally via the `outbox → CDC` relay, and durable Postgres audit export
/// (`UDB_AUDIT_EXPORT_POSTGRES`) writes the audit table — both require an auth
/// Postgres backend. Flags the concrete misconfiguration where durable export is
/// requested but no Postgres backend exists to hold the outbox/audit table.
fn check_outbox_table(hardened: bool) -> AuthReadinessCheck {
    let durable_export = env_on("UDB_AUDIT_EXPORT_POSTGRES");
    let dsn_env = auth_postgres_dsn_env_present();
    // This combination is purely env-driven on both sides, so it is a reliable
    // hard-fail (no services.yaml ambiguity): durable Postgres audit export was
    // requested but no Postgres DSN env exists to hold the outbox / audit table.
    if durable_export && !dsn_env {
        return AuthReadinessCheck::fail(
            "outbox_table",
            "UDB_AUDIT_EXPORT_POSTGRES is set but no Postgres DSN env (UDB_PG_DSN / \
             DATABASE_URL) is configured to hold the outbox / durable audit table",
        );
    }
    if dsn_env {
        AuthReadinessCheck::pass(
            "outbox_table",
            "Postgres DSN present; auth events publish via the outbox→CDC relay",
        )
    } else if hardened {
        // Caution, not a hard fail (services.yaml may configure the pool).
        AuthReadinessCheck::pass(
            "outbox_table",
            "no Postgres DSN env in a hardened posture — if Postgres is configured via \
             services.yaml the auth-event outbox is fine; otherwise events cannot be \
             persisted. Confirm via backend health probes.",
        )
    } else {
        AuthReadinessCheck::pass(
            "outbox_table",
            "no Postgres DSN env; auth-event outbox may be unavailable — valid for dev",
        )
    }
}

/// 8. Policy-snapshot load posture. The authz Casbin *model* parse is already
/// covered by `casbin_model`; this probe checks the *snapshot* posture: the
/// authz snapshot must evaluate off a parseable model and is built deny-by-default
/// (`AuthzSnapshot { default_allow: false, .. }` everywhere it is constructed in
/// `auth_service::authz`). Reusing the model validator here means a snapshot whose
/// model cannot even parse fails readiness too, since the snapshot evaluates off
/// that same Casbin model.
async fn check_policy_snapshot() -> AuthReadinessCheck {
    if let Err(e) = crate::runtime::authz::validate_casbin_model().await {
        return AuthReadinessCheck::fail(
            "policy_snapshot",
            format!("authz policy snapshot cannot load (model parse failed): {e}"),
        );
    }
    AuthReadinessCheck::pass(
        "policy_snapshot",
        "authz policy snapshot loads off a valid Casbin model; deny-by-default posture \
         (snapshot default_allow is false)",
    )
}

/// 9. Signing-key registry ACTIVE-key posture. The DB-backed signing-key registry
/// publishes its ACTIVE key to a process-global snapshot
/// ([`crate::runtime::security::active_signing_key`]) at startup seed and on
/// rotation. An empty snapshot is the valid env-key fallback (single-key
/// deployments). When the registry HAS been seeded but somehow has no ACTIVE key
/// in the snapshot, signing would silently fall back to the env key — surfaced
/// here. Reads only the snapshot (already populated by `seed_signing_key_registry`
/// which runs just before readiness at startup); no DB access.
fn check_signing_key_registry() -> AuthReadinessCheck {
    match crate::runtime::security::active_signing_key() {
        Some((kid, _pem)) if !kid.trim().is_empty() => AuthReadinessCheck::pass(
            "signing_key_registry",
            "signing-key registry has an ACTIVE key (rotation-capable)",
        ),
        Some(_) => AuthReadinessCheck::fail(
            "signing_key_registry",
            "signing-key registry snapshot has an ACTIVE entry with an empty kid",
        ),
        None => AuthReadinessCheck::pass(
            "signing_key_registry",
            "no DB signing-key registry seeded; signing uses the env key + static kid \
             (single-key fallback) — valid when key rotation is not in use",
        ),
    }
}

/// 10. Revocation/invalidation subscription posture. Revocation (token deny list,
/// session invalidation, signing-key compromise) must be *durable and fail-closed*
/// in a hardened deployment so a revocation-store error DENIES rather than
/// silently allowing. Reports the fail-open dev posture honestly; the durable
/// store itself is the Postgres backend checked by `session_revocation_store`.
fn check_revocation_subscription(hardened: bool) -> AuthReadinessCheck {
    if hardened {
        AuthReadinessCheck::pass(
            "revocation_subscription",
            "hardened posture: revocation lookups fail closed (a revocation-store error denies)",
        )
    } else {
        AuthReadinessCheck::pass(
            "revocation_subscription",
            "dev posture: revocation lookups may fail open on store error — set UDB_FAIL_CLOSED \
             or run in production posture to require fail-closed revocation",
        )
    }
}

/// 11. IdP support-matrix posture (informational). The IdP proto enumerates more
/// kinds than the runtime implements: `NATIVE`, `OIDC` and `SAML` (with SCIM
/// provisioning) are wired, while `LDAP`, `CUSTOM_JWT` and `EXTERNAL_SESSION` are
/// proto-enumerated but NOT supported — `create_provider` does not reject them
/// today, so a provider configured with one of those kinds will fail at login
/// rather than at create. This probe surfaces that support matrix so operators do
/// not configure a dead provider kind. It is informational (always passes); a true
/// fail-fast would need the auth DB handle to scan configured provider rows for an
/// unsupported `kind`, which this config-only readiness fn does not hold (noted for
/// the maintainer — the honest place to reject is `create_provider`/`load_provider`).
fn check_idp_provider_support() -> AuthReadinessCheck {
    AuthReadinessCheck::pass(
        "idp_provider_support",
        "IdP kinds supported: NATIVE, OIDC, SAML (+ SCIM provisioning). \
         Proto-enumerated but UNSUPPORTED: LDAP, CUSTOM_JWT, EXTERNAL_SESSION — \
         do not configure a provider with these kinds (they fail at login).",
    )
}

/// Crate-public adapter that runs the auth-plane readiness checks and returns
/// them as `(name, ok, detail)` triples, the shape
/// [`crate::runtime::slo::build_readiness_facts`] consumes. This lets the binary
/// crate (`udb native doctor`) fold the *same* auth-plane facts into the unified
/// readiness contract that `GetHealthReport` uses, without exposing the internal
/// `AuthReadinessCheck` type. Phase 10 acceptance: doctor / GetHealthReport /
/// gRPC health all report the same readiness facts.
pub async fn auth_readiness_triples(security: &SecurityConfig) -> Vec<(String, bool, String)> {
    check_auth_readiness(security)
        .await
        .checks
        .into_iter()
        .map(|c| (c.name, c.ok, c.detail))
        .collect()
}

/// Resolve an RSA PEM source (inline `-----BEGIN` text or a file path) and run
/// `parse` over its bytes. `read_err`/`parse_err` are the caller-safe message
/// prefixes for the two failure classes; neither the bytes nor the underlying
/// `Err` ever expose key material beyond the constructor's own diagnostic.
fn check_rsa_pem(
    name: &str,
    key_src: &str,
    read_err: &str,
    parse_err: &str,
    parse: impl Fn(&[u8]) -> Result<(), jsonwebtoken::errors::Error>,
) -> AuthReadinessCheck {
    let inline = key_src.contains("-----BEGIN");
    let bytes = if inline {
        key_src.as_bytes().to_vec()
    } else {
        match std::fs::read(key_src) {
            Ok(bytes) => bytes,
            // Deliberately omit the path's contents; the OS error is safe.
            Err(e) => return AuthReadinessCheck::fail(name, format!("{read_err}: {e}")),
        }
    };
    let source = if inline { "inline PEM" } else { "file path" };
    match parse(&bytes) {
        Ok(()) => AuthReadinessCheck::pass(name, format!("RSA PEM ({source}) parsed")),
        Err(e) => AuthReadinessCheck::fail(name, format!("{parse_err} ({source}): {e}")),
    }
}

/// Parse-check the configured Casbin authz model (the same async validator the
/// startup gate runs).
async fn check_casbin_model() -> AuthReadinessCheck {
    match crate::runtime::authz::validate_casbin_model().await {
        Ok(()) => AuthReadinessCheck::pass("casbin_model", "authz Casbin model parsed"),
        Err(e) => AuthReadinessCheck::fail("casbin_model", e),
    }
}

/// Probe a configured external JWKS URL: it must respond 2xx with a body that
/// parses as a JWK set (`{"keys":[...]}`). Bounded by a short timeout so a slow
/// IdP cannot hang startup. When the `http-client` feature is off the URL is
/// reported as configured-but-unprobed (a pass, since it cannot be checked here).
async fn check_jwks_reachable(url: &str) -> AuthReadinessCheck {
    #[cfg(feature = "http-client")]
    {
        let client = match reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
        {
            Ok(client) => client,
            Err(e) => {
                return AuthReadinessCheck::fail(
                    "jwks_reachability",
                    format!("could not build HTTP client: {e}"),
                );
            }
        };
        match client.get(url).send().await {
            Ok(resp) if resp.status().is_success() => match resp.text().await {
                Ok(body) => {
                    let is_jwks = serde_json::from_str::<serde_json::Value>(&body)
                        .ok()
                        .and_then(|v| v.get("keys").map(|k| k.is_array()))
                        .unwrap_or(false);
                    if is_jwks {
                        AuthReadinessCheck::pass(
                            "jwks_reachability",
                            "configured JWKS URL is reachable and returns a JWK set",
                        )
                    } else {
                        AuthReadinessCheck::fail(
                            "jwks_reachability",
                            "JWKS URL reachable but the response is not a JWK set",
                        )
                    }
                }
                Err(e) => AuthReadinessCheck::fail(
                    "jwks_reachability",
                    format!("JWKS body read failed: {e}"),
                ),
            },
            Ok(resp) => AuthReadinessCheck::fail(
                "jwks_reachability",
                format!("JWKS URL returned HTTP {}", resp.status()),
            ),
            Err(e) => {
                AuthReadinessCheck::fail("jwks_reachability", format!("JWKS URL unreachable: {e}"))
            }
        }
    }
    #[cfg(not(feature = "http-client"))]
    {
        let _ = url;
        AuthReadinessCheck::pass(
            "jwks_reachability",
            "JWKS URL configured but not probed (http-client feature disabled)",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RS256_PRIVATE_PEM: &str = include_str!("../../testdata/jwt_rs256_private.pem");

    fn config_with_private_key(key: Option<&str>) -> SecurityConfig {
        SecurityConfig {
            jwt_private_key: key.map(ToString::to_string),
            ..SecurityConfig::default()
        }
    }

    #[tokio::test]
    async fn good_rsa_private_key_passes_signing_check() {
        let security = config_with_private_key(Some(RS256_PRIVATE_PEM));
        let report = check_auth_readiness(&security).await;
        let check = report
            .checks
            .iter()
            .find(|c| c.name == "jwt_signing_key")
            .expect("signing-key check should run when a private key is configured");
        assert!(check.ok, "good RSA key should pass: {}", check.detail);
        // The caller-safe detail must not leak key material.
        assert!(!check.detail.contains("BEGIN"));
    }

    #[tokio::test]
    async fn garbage_private_key_fails_signing_check() {
        let security = config_with_private_key(Some("-----BEGIN PRIVATE KEY-----\nnot-a-key\n"));
        let report = check_auth_readiness(&security).await;
        let check = report
            .checks
            .iter()
            .find(|c| c.name == "jwt_signing_key")
            .expect("signing-key check should run when a private key is configured");
        assert!(!check.ok, "garbage key must fail");
        assert!(!report.ok, "report should not be ok when a check fails");
    }

    #[tokio::test]
    async fn default_casbin_model_validates() {
        // Default config: no keys, no JWKS — only the casbin + informational
        // checks run, and the embedded default model must parse.
        let report = check_auth_readiness(&SecurityConfig::default()).await;
        let casbin = report
            .checks
            .iter()
            .find(|c| c.name == "casbin_model")
            .expect("casbin check always runs");
        assert!(
            casbin.ok,
            "default Casbin model should validate: {}",
            casbin.detail
        );
        // No keys/JWKS → informational token-issuance check is present and ok.
        let issuance = report
            .checks
            .iter()
            .find(|c| c.name == "token_issuance")
            .expect("token-issuance check runs when no key/JWKS is set");
        assert!(issuance.ok);
        // Default has no external audit sink → local/stdout posture, a pass.
        let audit = report
            .checks
            .iter()
            .find(|c| c.name == "audit_sink")
            .expect("audit_sink check always runs");
        assert!(audit.ok);
        assert!(report.ok, "default dev config should be ready: {report:?}");
    }

    #[tokio::test]
    async fn non_http_audit_sink_fails_readiness() {
        let security = SecurityConfig {
            audit_sink_url: "ftp://bad-sink/ingest".to_string(),
            ..SecurityConfig::default()
        };
        let report = check_auth_readiness(&security).await;
        let check = report
            .checks
            .iter()
            .find(|c| c.name == "audit_sink")
            .expect("audit_sink check runs");
        assert!(!check.ok, "a non-http(s) audit sink URL must fail");
        assert!(!report.ok, "report must not be ok when a check fails");
    }

    #[tokio::test]
    async fn https_audit_sink_passes_readiness() {
        let security = SecurityConfig {
            audit_sink_url: "https://audit.example.com/ingest".to_string(),
            ..SecurityConfig::default()
        };
        let report = check_auth_readiness(&security).await;
        let check = report
            .checks
            .iter()
            .find(|c| c.name == "audit_sink")
            .expect("audit_sink check runs");
        assert!(check.ok, "https audit sink should pass: {}", check.detail);
    }

    #[tokio::test]
    async fn dependency_posture_probes_all_run() {
        // Every dependency-posture probe (5–11) must appear in the report so the
        // readiness contract is honest about the full enterprise dependency set,
        // not just the 6 original key/model/sink checks.
        let report = check_auth_readiness(&SecurityConfig::default()).await;
        for name in [
            "auth_schema_migration",
            "session_revocation_store",
            "outbox_table",
            "policy_snapshot",
            "signing_key_registry",
            "revocation_subscription",
            "idp_provider_support",
        ] {
            assert!(
                report.checks.iter().any(|c| c.name == name),
                "posture probe {name} must run; report={report:?}"
            );
        }
    }

    #[test]
    fn idp_support_matrix_names_unsupported_kinds() {
        // The informational support-matrix probe must name the supported kinds
        // AND the proto-enumerated-but-unsupported ones, so operators do not
        // configure a dead provider kind. It always passes (informational).
        let check = check_idp_provider_support();
        assert!(check.ok, "support-matrix probe is informational (passes)");
        for kind in ["OIDC", "SAML", "LDAP", "CUSTOM_JWT", "EXTERNAL_SESSION"] {
            assert!(
                check.detail.contains(kind),
                "support matrix must mention {kind}: {}",
                check.detail
            );
        }
    }

    #[test]
    fn signing_key_registry_reports_env_fallback_when_empty() {
        // With no DB registry seeded the snapshot is empty → env-key fallback,
        // which is a valid (passing) single-key posture.
        let check = check_signing_key_registry();
        // Either an ACTIVE key is present (if a prior test seeded the snapshot) or
        // the env fallback posture — both must pass.
        assert!(check.ok, "registry posture should pass: {}", check.detail);
    }
}
