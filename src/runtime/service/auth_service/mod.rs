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
pub(crate) use idp::spawn_saml_http_from_env;
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
use crate::runtime::security::SecurityConfig;

use super::DataBrokerService;

/// Current unix time in seconds.
pub(super) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// AUTH-004 producer: bridges the sync `security::ApiKeyPrincipalResolver` seam to
/// the async keyed-HMAC validator so `x-api-key` authenticates DataBroker CRUD from
/// the stored key record. Reuses `authn::validate_api_key` (no second validator);
/// `block_in_place` yields the multi-thread runtime worker while the DB lookup runs
/// (`resolve` only ever runs inside a tonic handler, i.e. on a runtime worker).
struct ApiKeyPrincipalResolverImpl {
    store: Arc<PostgresApiKeyStore>,
    hash_key: Vec<u8>,
}

impl crate::runtime::security::ApiKeyPrincipalResolver for ApiKeyPrincipalResolverImpl {
    fn resolve(
        &self,
        raw_api_key: &str,
    ) -> Result<Option<crate::runtime::security::ApiKeyPrincipal>, String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let record = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(crate::runtime::authn::validate_api_key(
                self.store.as_ref(),
                raw_api_key,
                &self.hash_key,
                now,
            ))
        })?;
        Ok(record.map(|rec| crate::runtime::security::ApiKeyPrincipal {
            subject: rec.principal_id,
            service_identity: rec.service_identity,
            tenant_id: rec.tenant_id,
            project_id: rec.project_id,
            scopes: rec.scopes,
        }))
    }
}

impl DataBrokerService {
    /// Build the Stage-1 auth services for the gRPC server, seeding the authz
    /// snapshot from the broker's currently-loaded ABAC policies so both share
    /// one policy view during migration. When a Postgres pool is available the
    /// authz service reads/writes its durable policy/role/relationship tables.
    pub(crate) fn build_auth_services(
        &self,
    ) -> (AuthnServiceImpl, AuthzServiceImpl, ApiKeyServiceImpl) {
        // Share the broker's SINGLE authorization snapshot cell (empty +
        // deny-by-default at startup, plus the dev `UDB_ABAC_DEFAULT_ALLOW` escape
        // hatch). The AuthzService built below PG-warms this exact cell from
        // `udb_authz.policy_rules`, and the broker's data-plane `authorize()` reads
        // it — so the authn admin-mutation handlers, the authz service, and the data
        // plane all enforce ONE Casbin policy set. There is no env-JSON ABAC lane.
        let shared_snapshot = self.authz_snapshot();
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam (extend_udb.md):
        // the backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("authn", true, "")
            .ok();
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
            // AUTH-004: ACTIVATE `x-api-key` auth on the DataBroker data plane by
            // installing the principal resolver over THIS key store. The sync,
            // header-only `security_from_request` bridges to the async keyed-HMAC
            // validator `authn::validate_api_key` (no second validator, no in-memory
            // store); the principal/tenant/project/scopes come only from the stored
            // record, so a caller cannot widen a key via headers.
            crate::runtime::security::install_api_key_principal_resolver(Arc::new(
                ApiKeyPrincipalResolverImpl {
                    store: api_key_store.clone(),
                    hash_key: authn_config.api_key_hash_secret().as_bytes().to_vec(),
                },
            ));
            let user_store: Arc<dyn UserStore> = Arc::new(PostgresUserStore::new(pool.clone(), ""));
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
                user_store.clone(),
            )
            .with_postgres(Some(pool.clone()))
            .with_runtime(Some(runtime.clone()))
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
                    // AUTH-006: owner lookup for service-identity lineage —
                    // best-effort inside CreateApiKey, never a hard gate.
                    .with_user_store(user_store)
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
                .with_runtime(Some(runtime.clone()))
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
) -> Result<BootstrapAdmin, String> {
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

    // Tenant-first: resolve the human tenant `code` (or an explicit UUID) to its
    // CANONICAL UUID, creating the `tenants` row if absent. The principal is bound
    // to this UUID — not the free-text code — so the Login JWT tenant claim is a UUID
    // that EVERY service accepts (the UUID-strict storage/webrtc/asset path and the
    // free-text control-plane path alike). This is the fix that removes the need for
    // a second "uuid tenant" admin.
    let tenant_uuid = ensure_tenant(&pool, tenant, None).await?;

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
                    tenant_id: tenant_uuid.clone(),
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
        // Bind the admin to `organization_owner` directly (bootstrap-exception raw
        // SQL, like the schema DDL + `seed_system_authz_defaults` above). The offline
        // bootstrap deliberately has NO `DataBrokerRuntime`, so it must not route
        // through the P6.10 runtime-backed `assign_role` handler. This mirrors the
        // exact user-role upsert that handler emits: idempotent
        // `ON CONFLICT (user_id, role_id, domain)`, refreshing assigner/tenant.
        use crate::runtime::native_catalog::native_model;
        let ur = native_model(
            "udb.core.authz.entity.v1.UserRole",
            &[
                "user_role_id",
                "user_id",
                "role_id",
                "domain",
                "assigned_by",
                "tenant_id",
                "created_by",
            ],
        );
        sqlx::query(&format!(
            "INSERT INTO {rel} \
               ({user_role_id}, {user_id}, {role_id}, {domain}, {assigned_by}, {tenant_id}, {created_by}) \
             VALUES (gen_random_uuid(), $1::UUID, $2::UUID, '', $1::UUID, $3, $1::UUID) \
             ON CONFLICT ({user_id}, {role_id}, {domain}) DO UPDATE SET \
               {assigned_by} = EXCLUDED.{assigned_by}, {tenant_id} = EXCLUDED.{tenant_id}, {created_by} = EXCLUDED.{created_by}",
            rel = ur.relation,
            user_role_id = ur.q("user_role_id"),
            user_id = ur.q("user_id"),
            role_id = ur.q("role_id"),
            domain = ur.q("domain"),
            assigned_by = ur.q("assigned_by"),
            tenant_id = ur.q("tenant_id"),
            created_by = ur.q("created_by"),
        ))
        .bind(&user_id)
        .bind(SYSTEM_ORG_OWNER_ROLE_ID)
        .bind(&tenant_uuid)
        .execute(&pool)
        .await
        .map_err(|err| format!("assign organization_owner role failed: {err}"))?;
    }

    Ok(BootstrapAdmin {
        user_id,
        tenant_id: tenant_uuid,
    })
}

/// Fixed schema/table/key for the durable single-use bootstrap marker (06.4.2.1).
/// A singleton row (one FIXED uuid) records that a *served* privilege-creating
/// bootstrap has already been consumed, so [`served_bootstrap_admin`] is one-shot
/// across process restarts — the env gate alone is not durable.
const SERVED_BOOTSTRAP_MARKER_ID: &str = "00000000-0000-0000-0000-0000000b0001";

/// Served (online) single-use wrapper around [`bootstrap_admin_user`] (06.4.1.1).
///
/// Privilege-creating bootstrap is FAIL-CLOSED and HARD-GUARDED: unlike the
/// offline CLI utility, this entry point can be reached from a running broker, so
/// minting the first `organization_owner` here is gated on BOTH
///   1. an explicit operator opt-in env (`UDB_ALLOW_SERVED_BOOTSTRAP` = `1`/`true`), and
///   2. a durable single-use marker (`udb_system.bootstrap_state`) — once a served
///      bootstrap succeeds the marker row exists and every later call is rejected.
///
/// The env check runs BEFORE any DB connection, so a disabled deployment never
/// touches Postgres. All user/role/tenant binding is delegated verbatim to
/// [`bootstrap_admin_user`] — this wrapper adds only the gate + the marker and
/// never re-implements credential logic.
pub async fn served_bootstrap_admin(
    dsn: &str,
    username: &str,
    email: &str,
    password: &str,
    tenant: &str,
    project: &str,
) -> Result<BootstrapAdmin, String> {
    // Gate 1 (no DB): operator opt-in. Fail closed when unset/anything else.
    let allowed = std::env::var("UDB_ALLOW_SERVED_BOOTSTRAP")
        .ok()
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true"
        })
        .unwrap_or(false);
    if !allowed {
        return Err(
            "served bootstrap is disabled (set UDB_ALLOW_SERVED_BOOTSTRAP=1 to allow the \
             one-time privilege-creating bootstrap)"
                .to_string(),
        );
    }

    // Gate 2 (durable single-use marker). Connect with the SAME approach
    // `bootstrap_admin_user` uses (it connects to `dsn` directly), ensure the
    // singleton marker table exists (idempotent CREATE … IF NOT EXISTS, mirroring
    // the schema-ensure idiom), and reject if the marker row is already present.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(dsn)
        .await
        .map_err(|err| format!("connect postgres '{dsn}': {err}"))?;

    sqlx::raw_sql("CREATE SCHEMA IF NOT EXISTS udb_system")
        .execute(&pool)
        .await
        .map_err(|err| format!("ensure udb_system schema failed: {err}"))?;
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS udb_system.bootstrap_state ( \
            id          UUID PRIMARY KEY, \
            consumed_at TIMESTAMPTZ NOT NULL DEFAULT NOW() )",
    )
    .execute(&pool)
    .await
    .map_err(|err| format!("ensure bootstrap_state table failed: {err}"))?;

    let already_consumed: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM udb_system.bootstrap_state WHERE id = $1::UUID)",
    )
    .bind(SERVED_BOOTSTRAP_MARKER_ID)
    .fetch_one(&pool)
    .await
    .map_err(|err| format!("read bootstrap marker failed: {err}"))?;
    if already_consumed {
        return Err("served bootstrap already consumed (single-use)".to_string());
    }

    // Delegate ALL user/role/tenant binding to the existing offline path — no
    // duplication of credential logic.
    let admin = bootstrap_admin_user(dsn, username, email, password, tenant, project).await?;

    // Persist the durable single-use marker. Idempotent: a concurrent winner may
    // have inserted it, so DO NOTHING on conflict keeps this one-shot rather than
    // erroring after a successful bind.
    sqlx::query(
        "INSERT INTO udb_system.bootstrap_state (id) VALUES ($1::UUID) \
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(SERVED_BOOTSTRAP_MARKER_ID)
    .execute(&pool)
    .await
    .map_err(|err| format!("persist bootstrap marker failed: {err}"))?;

    Ok(admin)
}

/// Result of [`bootstrap_admin_user`]: the created/bound principal plus the
/// CANONICAL tenant UUID it was bound to (resolved from the human code). Callers
/// surface the tenant id so clients/tests use the same UUID the Login JWT claim
/// carries — request bodies must match the claim or the per-RPC tenant cross-check
/// rejects them.
pub struct BootstrapAdmin {
    pub user_id: String,
    pub tenant_id: String,
}

/// Well-known id of the system `organization_owner` role row seeded by
/// [`seed_system_authz_defaults`]. Fixed so the bootstrap binding can target it
/// directly and re-seeding is a no-op.
pub(crate) const SYSTEM_ORG_OWNER_ROLE_ID: &str = "00000000-0000-0000-0000-0000000a0001";
/// Well-known id of the system `organization_owner ⇒ allow(*, *)` policy row.
pub(crate) const SYSTEM_ORG_OWNER_POLICY_ID: &str = "00000000-0000-0000-0000-0000000a0002";
/// Role code the bootstrap binding and the login role→scope projection share.
pub(crate) const ORG_OWNER_ROLE_CODE: &str = "organization_owner";

/// Human name (`code`) of the zero-config DEFAULT tenant. This is only a fallback
/// for first-run / CI / demo — the same role the Kubernetes `default` namespace or
/// the Postgres `public` schema plays. Real deployments create their own tenants
/// (each a fresh random UUID) and never share this one. Override:
/// `UDB_DEFAULT_TENANT_CODE`.
pub(crate) const DEFAULT_TENANT_CODE: &str = "acme";
/// Predictable, FIXED uuid of the DEFAULT tenant — NEVER random, so a zero-config
/// bootstrap always lands on the same well-known id (same convention as
/// [`SYSTEM_ORG_OWNER_ROLE_ID`]). Only the DEFAULT tenant is predictable; every
/// real tenant minted via [`ensure_tenant`] / `CreateTenant` gets an unguessable
/// random v4 id so one customer can never guess another's. Override:
/// `UDB_DEFAULT_TENANT_ID`.
pub(crate) const DEFAULT_TENANT_ID: &str = "00000000-0000-0000-0000-0000000d0001";

/// The DEFAULT tenant `code`, honoring a `UDB_DEFAULT_TENANT_CODE` override.
pub(crate) fn default_tenant_code() -> String {
    std::env::var("UDB_DEFAULT_TENANT_CODE")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_TENANT_CODE.to_string())
}
/// The DEFAULT tenant canonical UUID, honoring a `UDB_DEFAULT_TENANT_ID` override.
pub(crate) fn default_tenant_id() -> String {
    std::env::var("UDB_DEFAULT_TENANT_ID")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_TENANT_ID.to_string())
}

/// Resolve a tenant identifier to its CANONICAL UUID, creating the `tenants` row if
/// absent (idempotent on `code`). This is the single chokepoint that makes the
/// tenant-identity contract uniform: every principal is bound to the tenant UUID, so
/// the Login JWT claim is a UUID that BOTH the UUID-strict services
/// (storage/webrtc/asset — `parse_uuid`) and the free-text services accept.
///
/// - `tenant_in` may be a human `code` (e.g. `acme`) OR an explicit UUID.
/// - If it is already a UUID → that is the canonical id (resolve-or-create by id).
/// - If `code` == the configured DEFAULT → the fixed [`default_tenant_id`] is used
///   and a loud warning is logged (so nobody ships production on the shared dev
///   tenant by accident).
/// - Any other code → reuse the existing row's id if present, else mint a random v4.
pub(crate) async fn ensure_tenant(
    pool: &sqlx::PgPool,
    tenant_in: &str,
    name: Option<&str>,
) -> Result<String, String> {
    use crate::runtime::native_catalog::native_model;

    let trimmed = tenant_in.trim();
    if trimmed.is_empty() {
        return Err("tenant code or id is required".to_string());
    }
    let is_uuid = uuid::Uuid::parse_str(trimmed).is_ok();
    let m = native_model(
        "udb.core.tenant.entity.v1.Tenant",
        &[
            "tenant_id",
            "code",
            "name",
            "type",
            "status",
            "config",
            "branding",
            "audit_info",
            "deleted_at",
        ],
    );

    // 1. Resolve an existing tenant (by canonical id when a UUID was supplied, else
    //    by its unique human code). Soft-deleted rows do not count.
    let lookup_sql = if is_uuid {
        format!(
            "SELECT {tid}::text FROM {rel} WHERE {tid} = $1::UUID AND {del} IS NULL",
            rel = m.relation,
            tid = m.q("tenant_id"),
            del = m.q("deleted_at"),
        )
    } else {
        format!(
            "SELECT {tid}::text FROM {rel} WHERE {code} = $1 AND {del} IS NULL",
            rel = m.relation,
            tid = m.q("tenant_id"),
            code = m.q("code"),
            del = m.q("deleted_at"),
        )
    };
    if let Some(found) = sqlx::query_scalar::<_, String>(&lookup_sql)
        .bind(trimmed)
        .fetch_optional(pool)
        .await
        .map_err(|err| format!("tenant lookup failed: {err}"))?
    {
        return Ok(found);
    }

    // 2. Not found — choose the canonical id. The DEFAULT tenant is the fixed
    //    well-known id (predictable); everything else gets an unguessable v4.
    let default_code = default_tenant_code();
    let (canonical_id, code, is_default) = if is_uuid {
        (
            trimmed.to_string(),
            format!("tenant-{}", &trimmed[..8.min(trimmed.len())]),
            false,
        )
    } else if trimmed.eq_ignore_ascii_case(&default_code) {
        (default_tenant_id(), trimmed.to_string(), true)
    } else {
        (uuid::Uuid::new_v4().to_string(), trimmed.to_string(), false)
    };
    if is_default {
        tracing::warn!(
            tenant_code = %code,
            tenant_id = %canonical_id,
            "bootstrapping the well-known DEFAULT tenant — set UDB_DEFAULT_TENANT_CODE or create your own tenant for production"
        );
    }
    let tenant_name = name
        .map(str::to_string)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| format!("{code} (bootstrap)"));

    // 3. Insert; ON CONFLICT (code) DO NOTHING makes this safe under concurrent
    //    bootstraps. JSONB columns default to '{}'.
    let insert_sql = format!(
        "INSERT INTO {rel} ({tid}, {code}, {name}, {type_c}, {status}, {config}, {branding}, {audit}) \
         VALUES ($1::UUID, $2, $3, 'ORGANIZATION', 'ACTIVE', '{{}}'::JSONB, '{{}}'::JSONB, '{{}}'::JSONB) \
         ON CONFLICT ({code}) DO NOTHING",
        rel = m.relation,
        tid = m.q("tenant_id"),
        code = m.q("code"),
        name = m.q("name"),
        type_c = m.q("type"),
        status = m.q("status"),
        config = m.q("config"),
        branding = m.q("branding"),
        audit = m.q("audit_info"),
    );
    sqlx::query(&insert_sql)
        .bind(&canonical_id)
        .bind(&code)
        .bind(&tenant_name)
        .execute(pool)
        .await
        .map_err(|err| format!("create tenant '{code}' failed: {err}"))?;

    // 4. Re-resolve (race-safe: a concurrent writer may have won the ON CONFLICT, in
    //    which case the surviving row's id — not ours — is canonical).
    sqlx::query_scalar::<_, String>(&format!(
        "SELECT {tid}::text FROM {rel} WHERE {code} = $1 AND {del} IS NULL",
        rel = m.relation,
        tid = m.q("tenant_id"),
        code = m.q("code"),
        del = m.q("deleted_at"),
    ))
    .bind(&code)
    .fetch_optional(pool)
    .await
    .map_err(|err| format!("tenant re-lookup failed: {err}"))?
    .ok_or_else(|| "tenant ensure failed: row missing after insert".to_string())
}

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
