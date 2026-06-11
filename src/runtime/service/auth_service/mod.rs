//! Stage 1 auth gRPC handlers, split across files so authn/authz can be worked
//! on in parallel:
//! - [`mappings`]: proto↔runtime conversions + Postgres row helpers.
//! - [`authn`]: [`AuthnServiceImpl`] implementing `AuthnService`.
//! - [`authz`]: [`AuthzServiceImpl`] implementing `AuthzService`.
//!
//! Scope is the Stage-1 control-plane surface from `AUTH_NATIVE_ACCESS_PLAN.md`
//! plus the Stage-2 native-access/policy-bundle additions: `Authenticate`,
//! session lifecycle, JWT signing + refresh, TOTP MFA, CSRF validation,
//! `Authorize`/`GetNativeAccess`/`GetPolicyBundle`, the `Put*`/`Lint` mutators,
//! the snapshot-backed policy/check endpoints, role/policy CRUD, and API-key
//! lifecycle + usage stats. Every RPC in these services is implemented. Outbound
//! OTP delivery is wired via a best-effort HTTP webhook to the operator's channel
//! gateway (`UDB_OTP_DELIVERY_WEBHOOK_URL`); see `authn::deliver_otp`.
//!
//! gRPC handlers return the concrete generated response types; the REST
//! `ApiResponse`/`RawJsonResponse` envelope (`core/common/v1`) is applied by the
//! gateway transcoding layer, not inside these handlers.

mod apikey;
mod audit_export;
mod authn;
mod authz;
mod control_plane;
// `pub(crate)` so the native data-plane helper (`service::native_helpers`) can
// reuse the ONE shared compliance-envelope builder/validator (Phase 10 telemetry
// coherence) instead of emitting a second divergent envelope shape.
pub(crate) mod events;
mod idp;
mod mappings;
pub(crate) mod readiness;
// Phase 10: re-export the auth-plane readiness adapter so the parent `service`
// module can re-export it one level up (`pub use auth_service::auth_readiness_triples;`)
// for the binary crate's `udb native doctor`, keeping doctor / GetHealthReport /
// gRPC health on the same unified readiness fact set.
pub use readiness::auth_readiness_triples;

#[cfg(test)]
mod tests;

use std::sync::Arc;

pub use crate::proto::udb::core::apikey::services::v1::api_key_service_server::ApiKeyServiceServer;
pub use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnServiceServer;
pub use crate::proto::udb::core::authz::services::v1::authz_service_server::AuthzServiceServer;
pub use idp::IdentityProviderServiceServer;
// Tier-7 #31: optional SCIM 2.0 HTTP/REST surface for off-the-shelf provisioners
// (Okta/Entra/OneLogin). OFF by default; binds only when UDB_SCIM_HTTP_ADDR is
// set. Reuses the gRPC SCIM handlers (and thus store::* + IdP events). The
// `IdentityProviderServiceImpl` itself is mounted via `IdentityProviderServiceServer`
// (above) and built/owned inside `service::mod` through
// `DataBrokerService::build_identity_provider_service`, so it needs no parent
// re-export — only the HTTP-spawn entry point is surfaced here.
pub(crate) use idp::spawn_scim_http_from_env;
// Phase 9: versioned control-plane policy distribution (xDS-style). The parent
// `service` module mounts `ControlPlaneServiceServer` on the native auth listener
// alongside Authn/Authz/ApiKey/Idp, wrapped by the proto method-security layer.
pub use control_plane::ControlPlaneServiceServer;

pub use apikey::ApiKeyServiceImpl;
pub use authn::AuthnServiceImpl;
pub use authz::AuthzServiceImpl;

#[cfg(feature = "redis")]
use crate::runtime::authn::RedisSessionStore;
use crate::runtime::authn::{
    AccountStatus, AuthnConfig, PostgresApiKeyStore, PostgresSessionStore, PostgresUserStore,
    SessionStore, UserStore,
};
use crate::runtime::authz::AuthzSnapshot;
use crate::runtime::security::SecurityConfig;

use super::DataBrokerService;

/// Current unix time in seconds.
pub(super) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl DataBrokerService {
    /// Build the Stage-1 auth services for the gRPC server, seeding the authz
    /// snapshot from the broker's currently-loaded ABAC policies so both share
    /// one policy view during migration. When a Postgres pool is available the
    /// authz service reads/writes its durable policy/role/relationship tables.
    pub(crate) fn build_auth_services(
        &self,
    ) -> (AuthnServiceImpl, AuthzServiceImpl, ApiKeyServiceImpl) {
        let policies = self
            .abac_policies
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        let mut snapshot = AuthzSnapshot::from_abac_policies("live-abac", &policies);
        snapshot.default_allow = self.abac_default_allow;
        // Tier-0 #5 (D2-full): share ONE atomically-swappable snapshot cell between
        // the authz service (which owns reloads) and the authn admin-mutation
        // handlers, so those handlers invoke the SAME native authz DECISION ENGINE
        // per action — not a divergent copy. The authz service is built from this
        // shared cell via `AuthzServiceImpl::shared(...)` below.
        let shared_snapshot = Arc::new(arc_swap::ArcSwap::from_pointee(snapshot));
        let runtime = self.runtime.load_full();
        let pg_pool = runtime.pg_pool().ok().cloned();
        let authn_config = AuthnConfig::from_env();
        let security = SecurityConfig::current();
        // Wire the outbox-backed event sink so native auth mutations publish
        // their domain events to Kafka via the CDC relay. Falls back to a no-op
        // when no Postgres pool is available (the events have nowhere durable to
        // land without the outbox table).
        let event_sink: Arc<dyn events::AuthEventSink> = match pg_pool.clone() {
            Some(pool) => Arc::new(
                events::OutboxAuthEventSink::new(
                    pool.clone(),
                    runtime.config().cdc.outbox_relation(),
                )
                // Phase L3 task2: attach the configured immutable export sinks
                // (Postgres durable audit table, stdout/file, SIEM webhook) and
                // the shared metrics recorder so audit-sink failures are tracked.
                .with_exports(audit_export::export_sinks_from_env(Some(&pool)))
                .with_metrics(self.metrics.clone()),
            ),
            None => events::noop_sink(),
        };
        let (authn_service, api_key_service) = if let Some(pool) = pg_pool.clone() {
            // Short-TTL cluster jti denylist (Tier-1 #13): an acceleration layer
            // over the durable `token_revocations` table so a revoke propagates
            // fast across nodes instead of waiting on each node's DB read. TTL =
            // access-token max lifetime so an entry expires when the token would
            // anyway. Wired when a Redis client exists (independent of the session
            // backend); `None` otherwise → DB-only behavior unchanged.
            #[cfg(feature = "redis")]
            let jti_denylist = runtime.redis_clone().map(|redis| {
                crate::runtime::authn::revocation::JtiDenylist::new(
                    redis,
                    security.jwt_access_ttl_secs,
                )
            });
            let api_key_store = Arc::new(PostgresApiKeyStore::new(pool.clone(), ""));
            let session_store: Arc<dyn SessionStore> =
                if authn_config.session_backend.eq_ignore_ascii_case("redis") {
                    #[cfg(feature = "redis")]
                    {
                        if let Some(redis) = runtime.redis_clone() {
                            Arc::new(RedisSessionStore::new(
                                redis,
                                "udb:authn",
                                authn_config.session_ttl_secs,
                            ))
                        } else {
                            Arc::new(PostgresSessionStore::new(pool.clone(), ""))
                        }
                    }
                    #[cfg(not(feature = "redis"))]
                    {
                        Arc::new(PostgresSessionStore::new(pool.clone(), ""))
                    }
                } else {
                    Arc::new(PostgresSessionStore::new(pool.clone(), ""))
                };
            let authn = AuthnServiceImpl::with_stores(
                authn_config.clone(),
                security,
                session_store,
                api_key_store.clone(),
                Arc::new(PostgresUserStore::new(pool.clone(), "")),
            )
            .with_postgres(Some(pool.clone()))
            .with_event_sink(event_sink.clone())
            .with_metrics(self.metrics.clone())
            // Tier-0 #5 (D2-full): wire the shared authz snapshot so the admin-
            // mutation handlers invoke the native authz decision engine per action.
            .with_authz_snapshot(Some(shared_snapshot.clone()));
            // Acceleration layer: SET on revoke + check-first on validate.
            #[cfg(feature = "redis")]
            let authn = authn.with_jti_denylist(jti_denylist);
            (
                authn,
                ApiKeyServiceImpl::with_store(authn_config, api_key_store)
                    .with_postgres(Some(pool.clone()))
                    .with_event_sink(event_sink.clone()),
            )
        } else {
            (
                AuthnServiceImpl::new(authn_config.clone(), security)
                    .with_event_sink(event_sink.clone())
                    .with_metrics(self.metrics.clone())
                    .with_authz_snapshot(Some(shared_snapshot.clone())),
                ApiKeyServiceImpl::new(authn_config).with_event_sink(event_sink.clone()),
            )
        };
        (
            authn_service,
            AuthzServiceImpl::shared(shared_snapshot)
                .with_postgres(pg_pool)
                .with_event_sink(event_sink)
                .with_metrics(self.metrics.clone())
                // Tier-0 #1: wire the shared per-tenant fair-admission manager so
                // the hot authz decision RPCs admit per validated tenant (same
                // path the data/media planes use).
                .with_channels(Some(runtime.channels().clone())),
            api_key_service,
        )
    }
}

/// urgent_fix #20: OFFLINE root bootstrap — create the first verified admin user
/// directly against the database.
///
/// The native control-plane listener is PEP-fronted: every credential-minting RPC
/// (`CreateUser`, `Login`, `ApiKeyService`) requires an existing bearer, and the
/// public `Authenticate` needs a user that already exists — a circular dependency
/// that leaves a fresh deployment with no way to mint its first principal. This
/// constructs the authn service in-process (the same `with_stores` path the live
/// auth tests use, which bypasses the listener's per-action gate — `authorize_action`
/// is permissive when no claim context is installed), creates the user, and marks
/// it ACTIVE. After this, clients `Authenticate` normally. Returns the new user id.
pub async fn bootstrap_admin_user(
    dsn: &str,
    username: &str,
    email: &str,
    password: &str,
    tenant: &str,
    project: &str,
) -> Result<String, String> {
    use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
    use crate::proto::udb::core::authn::services::v1 as authn_pb;
    use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
    use tonic::Request;

    // Connect directly (the bootstrap is a standalone offline utility; it must not
    // depend on the full runtime config-plumbing that `serve()` uses).
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(dsn)
        .await
        .map_err(|err| format!("connect postgres '{dsn}': {err}"))?;

    // Best-effort: ensure the native auth schema exists (idempotent
    // `CREATE … IF NOT EXISTS`) so a fresh database can be bootstrapped in one shot.
    // Per-statement errors are tolerated — on an ALREADY-migrated database some
    // statements are redundant no-ops, and the broker's normal startup owns full
    // migration. If the schema is genuinely missing, `create_user` below fails with
    // a clear "relation does not exist" error.
    for stmt in crate::runtime::native_catalog::native_service_catalog_ddl() {
        if let Err(err) = sqlx::raw_sql(&stmt).execute(&pool).await {
            tracing::debug!(error = %err, "bootstrap schema-ensure statement skipped");
        }
    }

    let session_store: Arc<dyn SessionStore> =
        Arc::new(PostgresSessionStore::new(pool.clone(), ""));
    let user_store: Arc<dyn UserStore> = Arc::new(PostgresUserStore::new(pool.clone(), ""));
    let svc = AuthnServiceImpl::with_stores(
        AuthnConfig::from_env(),
        SecurityConfig::current(),
        session_store,
        Arc::new(PostgresApiKeyStore::new(pool.clone(), "")),
        user_store.clone(),
    )
    .with_postgres(Some(pool.clone()));

    // Idempotent: reuse an existing admin with the same username instead of
    // colliding on the unique username index. This lets a re-run bind an admin
    // that was created before the role-binding step existed (Block 1), rather
    // than failing at `create_user`. Usernames are stored lowercased.
    let login_name = username.trim().to_ascii_lowercase();
    let (user_id, needs_activation) = match user_store
        .get_user_by_username(&login_name)
        .await
        .map_err(|err| format!("lookup user failed: {err}"))?
    {
        Some(existing) => (existing.user_id, existing.status != AccountStatus::Active),
        None => {
            let created = svc
                .create_user(Request::new(authn_pb::CreateUserRequest {
                    username: username.to_string(),
                    email: email.to_string(),
                    password: password.to_string(),
                    tenant_id: tenant.to_string(),
                    project_id: project.to_string(),
                    full_name: "Bootstrap Admin".to_string(),
                    ..Default::default()
                }))
                .await
                .map_err(|err| format!("create_user failed: {err}"))?
                .into_inner();
            let new_id = created
                .user
                .ok_or_else(|| "create_user returned no user".to_string())?
                .user_id;
            (new_id, true)
        }
    };

    // Admin-provisioned bootstrap has no OTP delivery channel, so activate
    // directly — but only when not already ACTIVE (the status FSM rejects a
    // no-op self-transition).
    if needs_activation {
        svc.change_user_status(Request::new(authn_pb::ChangeUserStatusRequest {
            user_id: user_id.clone(),
            new_status: authn_entity_pb::UserStatus::Active as i32,
            reason: "offline bootstrap".to_string(),
            ..Default::default()
        }))
        .await
        .map_err(|err| format!("activate user failed: {err}"))?;
    }

    // Block 1 (auth_fix.md, Decision C): bootstrap writes only the user + the
    // role BINDING — it never authors policy. Ensure the system defaults exist
    // first (the broker's startup seed may not have run yet if this is an
    // offline-first bootstrap), then bind the user to `organization_owner` via
    // the authz central-plane `assign_role` path (idempotent ON CONFLICT).
    seed_system_authz_defaults(&pool)
        .await
        .map_err(|err| format!("seed system authz defaults failed: {err}"))?;
    {
        use crate::proto::udb::core::authz::services::v1 as authz_pb;
        use crate::proto::udb::core::authz::services::v1::authz_service_server::AuthzService;
        let authz = AuthzServiceImpl::shared(Arc::new(arc_swap::ArcSwap::from_pointee(
            AuthzSnapshot::default(),
        )))
        .with_postgres(Some(pool.clone()));
        authz
            .assign_role(Request::new(authz_pb::AssignRoleRequest {
                user_id: user_id.clone(),
                role_id: SYSTEM_ORG_OWNER_ROLE_ID.to_string(),
                assigned_by: user_id.clone(),
                tenant_id: tenant.to_string(),
                ..Default::default()
            }))
            .await
            .map_err(|err| format!("assign organization_owner role failed: {err}"))?;
    }

    Ok(user_id)
}

/// Well-known id of the system `organization_owner` role row seeded by
/// [`seed_system_authz_defaults`]. Fixed so the bootstrap binding can target it
/// directly and re-seeding is a no-op.
pub(crate) const SYSTEM_ORG_OWNER_ROLE_ID: &str = "00000000-0000-0000-0000-0000000a0001";
/// Well-known id of the system `organization_owner ⇒ allow(*, *)` policy row.
pub(crate) const SYSTEM_ORG_OWNER_POLICY_ID: &str = "00000000-0000-0000-0000-0000000a0002";
/// Role code the bootstrap binding and the login role→scope projection share.
pub(crate) const ORG_OWNER_ROLE_CODE: &str = "organization_owner";

/// Idempotently seed the system authz defaults the bootstrap admin path depends
/// on (auth_fix.md Block 1, Decision C / change-point 1):
///
/// 1. a global `organization_owner` **role row** (`tenant = '*'`, well-known id)
///    so a `user_roles` binding has a real FK target and the snapshot loader
///    resolves its `role_code` (else the binding join falls back to `role_id`
///    text and the role→scope projection never matches);
/// 2. the `organization_owner ⇒ allow(*, *)` **policy row** so the Casbin /
///    data-plane decision path authorizes the owner. The policy is role-gated
///    (subject empty, `role` carried in `attributes_json`, which the snapshot
///    loader reads and strips from conditions), so it never self-blocks and does
///    not trip the `broad_wildcard` lint (role is concrete, not a wildcard).
///
/// Both rows are global (`tenant = '*'`); per-tenant scoping comes from the
/// binding. `ON CONFLICT DO NOTHING` makes this safe to run on every broker
/// startup and again from `bootstrap_admin_user`.
pub(crate) async fn seed_system_authz_defaults(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    use crate::runtime::native_catalog::native_model;

    let role = native_model(
        "udb.core.authz.entity.v1.Role",
        &[
            "role_id",
            "name",
            "description",
            "is_system",
            "is_active",
            "tenant_id",
            "project_id",
            "role_code",
            "scope_type",
        ],
    );
    sqlx::query(&format!(
        "INSERT INTO {rel} \
           ({role_id}, {name}, {description}, {is_system}, {is_active}, {tenant_id}, {project_id}, {role_code}, {scope_type}) \
         VALUES ($1::UUID, 'Organization Owner', 'Full control across the organization', TRUE, TRUE, '*', '', $2, 'TENANT') \
         ON CONFLICT DO NOTHING",
        rel = role.relation,
        role_id = role.q("role_id"),
        name = role.q("name"),
        description = role.q("description"),
        is_system = role.q("is_system"),
        is_active = role.q("is_active"),
        tenant_id = role.q("tenant_id"),
        project_id = role.q("project_id"),
        role_code = role.q("role_code"),
        scope_type = role.q("scope_type"),
    ))
    .bind(SYSTEM_ORG_OWNER_ROLE_ID)
    .bind(ORG_OWNER_ROLE_CODE)
    .execute(pool)
    .await?;

    // Role-gated allow policy. Column shape mirrors `put_authz_policy`: subject
    // empty, `role`/reserved keys live in `attributes_json` (the loader reads
    // `role` from there and removes the reserved keys from `conditions`), effect
    // is the canonical uppercase `ALLOW`, tenant `*` = any.
    let policy = native_model(
        "udb.core.authz.entity.v1.PolicyRule",
        &[
            "policy_id",
            "subject",
            "domain",
            "object",
            "action",
            "effect",
            "condition",
            "description",
            "is_active",
            "tenant_id",
            "project_id",
            "attributes_json",
        ],
    );
    let attributes = serde_json::json!({
        "role": ORG_OWNER_ROLE_CODE,
        "priority": "0",
        "purpose": "",
        "relationship": "",
        "required_scopes": "",
    })
    .to_string();
    sqlx::query(&format!(
        "INSERT INTO {rel} \
           ({policy_id}, {subject}, {domain}, {object}, {action}, {effect}, {condition}, {description}, {is_active}, {tenant_id}, {project_id}, {attributes_json}) \
         VALUES ($1::UUID, '', '*', '*', '*', 'ALLOW', '', 'system: organization_owner full control', TRUE, '*', '', $2::JSONB) \
         ON CONFLICT DO NOTHING",
        rel = policy.relation,
        policy_id = policy.q("policy_id"),
        subject = policy.q("subject"),
        domain = policy.q("domain"),
        object = policy.q("object"),
        action = policy.q("action"),
        effect = policy.q("effect"),
        condition = policy.q("condition"),
        description = policy.q("description"),
        is_active = policy.q("is_active"),
        tenant_id = policy.q("tenant_id"),
        project_id = policy.q("project_id"),
        attributes_json = policy.q("attributes_json"),
    ))
    .bind(SYSTEM_ORG_OWNER_POLICY_ID)
    .bind(attributes)
    .execute(pool)
    .await?;

    Ok(())
}
