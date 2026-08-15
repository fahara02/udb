//! `ApiKeyService` handler over the UDB-owned API-key primitives.
//!
//! ## P4 native-store posture (extend_udb.md) — kept raw, capability-gated
//!
//! ApiKey persistence deliberately does NOT ride the typed `native_entity_*`
//! data-plane path. Each raw site below is a documented escape hatch (P4.D/P4.F
//! honest degradation), not un-migrated debt — none is expressible in the
//! current neutral IR:
//!
//! - **Store CRUD (`put`/`rotate`/`get_by_prefix`/`revoke`/…)** lives behind the
//!   [`ApiKeyStore`] trait, which is also driven by the internal
//!   `validate_api_key` auth path that has no runtime/`RequestContext`. Per the
//!   P4.C row-8 doctrine, trait-backed stores extend the store trait per backend
//!   rather than route through the data plane. Additionally `put`/`rotate` upsert
//!   `ON CONFLICT (key_hash)` — an ALTERNATE unique key, not the primary key —
//!   and `ConflictStrategy` only targets the manifest PK, so the IR cannot
//!   express it (same limitation that keeps `tenant_service::create_tenant` raw).
//! - **Usage analytics (`recent_request_count`, `distinct_ip_count`,
//!   `get_api_key_usage_stats`)** are COUNT / COUNT(DISTINCT) / GROUP-BY
//!   aggregates over `api_key_usages`; `LogicalRead` has no aggregate surface
//!   (P4.D — "do not emulate in the service"; precedent: `list_tenants` count).
//! - **`record_usage`** is an `INSERT … SELECT` that resolves the `key_id` FK via
//!   a subquery and binds an `INET` column — neither the cross-row subquery nor
//!   the `INET` type is representable in the IR.
//! - **`emergency_revoke_api_keys`** filters on a `scopes_json @>` JSON-containment
//!   predicate and a unix→`TIMESTAMPTZ` bound; keeping this security-critical
//!   admin scan raw avoids an unverified typed-read regression.
//!
//! Reads that DO map cleanly already ride the typed path elsewhere (see
//! `webrtc_service` get/list and `tenant_service::get_tenant`). Unlocking a fuller
//! ApiKey migration needs IR support for alternate-key upserts, an aggregate-count
//! read, `INET`, and JSON-containment filters — tracked in extend_udb.md P4.

use std::sync::Arc;

use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::apikey::entity::v1 as apikey_entity_pb;
use crate::proto::udb::core::apikey::services::v1 as apikey_pb;
use crate::proto::udb::core::common::v1 as common_pb;
use apikey_pb::api_key_service_server::ApiKeyService;

use crate::runtime::authn::{
    self, AccountKind, AccountStatus, ApiKeyRecord, ApiKeyStore, AuthnConfig,
    UnavailableApiKeyStore, UnavailableUserStore, UserStore,
};
use crate::runtime::service::native_helpers::{update_mask_allows, update_mask_path_set};

use super::events::{self, AuthEvent, AuthEventSink, ComplianceEnvelope, topics};
use super::mappings::{bounded_page_response, bounded_page_window, timestamp_from_unix};
use super::now_unix;

#[derive(Clone)]
pub struct ApiKeyServiceImpl {
    api_keys: Arc<dyn ApiKeyStore>,
    /// Read-only owner lookup used to verify current service-account state.
    /// An unwired store fails grant-backed key issuance closed.
    users: Arc<dyn UserStore>,
    config: AuthnConfig,
    event_sink: Arc<dyn AuthEventSink>,
    /// Direct pool for read-only usage aggregation over `api_key_usages`
    /// (the trait-based store does not expose analytic queries).
    pg_pool: Option<sqlx::PgPool>,
}

impl ApiKeyServiceImpl {
    pub fn new(config: AuthnConfig) -> Self {
        Self {
            api_keys: Arc::new(UnavailableApiKeyStore),
            users: Arc::new(UnavailableUserStore),
            config,
            event_sink: events::noop_sink(),
            pg_pool: None,
        }
    }

    pub fn with_store(config: AuthnConfig, api_keys: Arc<dyn ApiKeyStore>) -> Self {
        Self {
            api_keys,
            users: Arc::new(UnavailableUserStore),
            config,
            event_sink: events::noop_sink(),
            pg_pool: None,
        }
    }

    /// AUTH-006: attach the read-only user store used to resolve a created
    /// key's service-account owner and persist its service-identity lineage.
    pub fn with_user_store(mut self, users: Arc<dyn UserStore>) -> Self {
        self.users = users;
        self
    }

    /// Attach the Postgres pool used for usage-stat aggregation.
    pub(crate) fn with_postgres(mut self, pool: Option<sqlx::PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    /// Attach the domain-event sink (outbox → Kafka). Defaults to a no-op.
    pub(crate) fn with_event_sink(mut self, sink: Arc<dyn AuthEventSink>) -> Self {
        self.event_sink = sink;
        self
    }

    fn hash_key(&self) -> Vec<u8> {
        self.config.api_key_hash_secret().as_bytes().to_vec()
    }

    fn required_field(
        field: &'static str,
        description: &'static str,
        message: &'static str,
    ) -> Status {
        crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
    }

    fn capability_status(
        operation: &'static str,
        capability_required: &'static str,
        message: &'static str,
    ) -> Status {
        crate::runtime::executor_utils::capability_status(
            "api_key",
            operation,
            capability_required,
            message,
        )
    }

    fn api_key_not_found_status(operation: &'static str) -> Status {
        crate::runtime::executor_utils::schema_status(
            Code::NotFound,
            "api_key",
            operation,
            "api_key_not_found",
            "api key not found",
        )
    }

    fn policy_status_with_code(
        code: Code,
        operation: &'static str,
        policy_decision_id: &'static str,
        message: &'static str,
    ) -> Status {
        crate::runtime::executor_utils::policy_status_with_code(
            code,
            operation,
            policy_decision_id,
            message,
        )
    }

    fn internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
        crate::runtime::executor_utils::internal_status("api_key", operation, message)
    }

    fn grant_precondition_status(
        message: impl Into<String>,
        field: &'static str,
        description: impl Into<String>,
    ) -> Status {
        crate::runtime::executor_utils::failed_precondition_fields(
            message,
            [(field.to_string(), description.into())],
        )
    }

    /// Resolve the only owner kind this store actually persists. The durable
    /// API-key row is written as `SERVICE_ACCOUNT`, so accepting another owner
    /// kind or treating owner/grant lookup as best-effort would create a
    /// credential with invented lineage.
    async fn active_service_account_grant(
        &self,
        owner_id: &str,
        expected_tenant: &str,
        expected_project: &str,
    ) -> Result<super::grants::GrantRecord, Status> {
        let owner = self
            .users
            .get_user_by_id(owner_id.trim())
            .await
            .map_err(|err| Self::internal_status("api_key_owner_load", err))?
            .ok_or_else(|| {
                Self::grant_precondition_status(
                    "API key owner must be an existing active service account",
                    "owner_id",
                    "no matching service account",
                )
            })?;
        if owner.account_kind != AccountKind::ServiceAccount
            || owner.status != AccountStatus::Active
        {
            return Err(Self::grant_precondition_status(
                "API key owner must be an active service account",
                "owner_id",
                "account kind must be SERVICE_ACCOUNT and status must be ACTIVE",
            ));
        }
        let pool = self.pg_pool.as_ref().ok_or_else(|| {
            Self::capability_status(
                "grant_resolution",
                "postgres_auth_store",
                "API key service-account grant resolution requires the durable Postgres auth store",
            )
        })?;
        let grant = super::grants::get_grant_by_user(pool, &owner.tenant_id, &owner.user_id)
            .await?
            .filter(|grant| grant.status.eq_ignore_ascii_case("ACTIVE"))
            .ok_or_else(|| {
                Self::grant_precondition_status(
                    "API key owner has no active typed service-account grant",
                    "owner_id",
                    "create or reactivate the typed grant before issuing a key",
                )
            })?;
        if grant.tenant_id.trim() != owner.tenant_id.trim()
            || grant.project_id.trim() != owner.project_id.trim()
        {
            return Err(Self::grant_precondition_status(
                "service-account grant does not match the owner's tenant/project binding",
                "owner_id",
                "grant lineage is stale or misbound",
            ));
        }
        if !expected_tenant.trim().is_empty() && expected_tenant.trim() != grant.tenant_id.trim() {
            return Err(Self::tenant_mismatch_status());
        }
        if !expected_project.trim().is_empty() && expected_project.trim() != grant.project_id.trim()
        {
            return Err(Self::project_mismatch_status());
        }
        Ok(grant)
    }

    fn tenant_context_required_status() -> Status {
        Self::policy_status_with_code(
            Code::PermissionDenied,
            "api_key_tenant_scope",
            "caller_tenant_context_required",
            "api key is tenant-scoped; caller tenant context is required",
        )
    }

    fn tenant_mismatch_status() -> Status {
        Self::policy_status_with_code(
            Code::PermissionDenied,
            "api_key_tenant_scope",
            "caller_tenant_mismatch",
            "api key belongs to a different tenant",
        )
    }

    fn project_mismatch_status() -> Status {
        Self::policy_status_with_code(
            Code::PermissionDenied,
            "api_key_project_scope",
            "caller_project_mismatch",
            "API key belongs to a different project",
        )
    }

    fn read_tenant_required_status() -> Status {
        Self::policy_status_with_code(
            Code::PermissionDenied,
            "api_key_read_scope",
            "tenant_scoped_bearer_required",
            "api key read requires a tenant-scoped bearer token or a cross-tenant admin role",
        )
    }

    /// Default per-minute request ceiling enforced by `ValidateApiKey`
    /// (`UDB_API_KEY_RATE_LIMIT_PER_MINUTE`, default 60 — matches the proto
    /// `rate_limit_per_minute` column default). 0 disables rate limiting.
    fn config_rate_limit_per_minute(&self) -> i64 {
        std::env::var("UDB_API_KEY_RATE_LIMIT_PER_MINUTE")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(60)
    }

    /// Distinct-source-IP anomaly threshold for `ValidateApiKey`
    /// (`UDB_API_KEY_DISTINCT_IP_ANOMALY`, default 5). When a single key is
    /// successfully used from MORE than this many distinct source IPs inside the
    /// trailing window ([`Self::ANOMALY_WINDOW_SECS`]), an `API_KEY_ANOMALOUS_USE`
    /// event is emitted — a credential-sharing / stolen-key signal that is
    /// orthogonal to rate limiting (which counts *volume*, not *spread*). 0
    /// disables the check.
    fn config_distinct_ip_anomaly(&self) -> i64 {
        std::env::var("UDB_API_KEY_DISTINCT_IP_ANOMALY")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(5)
    }

    /// Trailing window (seconds) over which the distinct-source-IP anomaly is
    /// evaluated. One hour balances catching a spread-out credential leak against
    /// false-positives from a single client's NAT/proxy churn.
    const ANOMALY_WINDOW_SECS: i64 = 3600;

    async fn emit_event(&self, event: AuthEvent) {
        let topic = event.topic;
        if let Err(err) = self.event_sink.emit(event).await {
            tracing::warn!(topic, error = %err, "failed to publish apikey event");
        }
    }

    /// Emit a `ValidateApiKey`-path security event (`API_KEY_VALIDATE_FAILED` /
    /// `API_KEY_RATE_LIMITED`) with a fully-populated compliance envelope so it
    /// passes enterprise-audit validation. `ValidateApiKeyRequest` carries no
    /// `RequestContext`, so actor/auth_method/trace are sourced from the ambient
    /// request scope (the OTel/principal task-locals the method-security gate set)
    /// rather than an unverified body.
    ///
    /// NEVER carries key material: the partition/document id is the public
    /// `key_prefix` (or `"unknown"` when the key did not resolve), and the body
    /// carries only `endpoint`/`outcome`/`reason_code` — never the plain key.
    async fn emit_validate_event(
        &self,
        topic: &'static str,
        key_prefix: &str,
        tenant_id: &str,
        endpoint: &str,
        ip_address: &str,
        outcome: &str,
        reason_code: &str,
    ) {
        // Never use the plain key as a document id; fall back to a non-secret
        // marker when the key did not resolve to a prefix.
        let document_id = if key_prefix.trim().is_empty() {
            "unknown".to_string()
        } else {
            key_prefix.to_string()
        };
        let correlation_id = Uuid::new_v4().to_string();
        let actor = {
            let a = crate::runtime::otel::current_actor();
            if a.trim().is_empty() {
                document_id.clone()
            } else {
                a
            }
        };
        let auth_method = {
            let m = crate::runtime::otel::current_auth_method();
            if m.trim().is_empty() {
                "api_key".to_string()
            } else {
                m
            }
        };
        self.emit_event(
            AuthEvent::new(
                topic,
                document_id.clone(),
                tenant_id.to_string(),
                serde_json::json!({
                    "key_id": document_id.clone(),
                    "key_prefix": document_id.clone(),
                    "endpoint": endpoint,
                    "outcome": outcome,
                    "reason_code": reason_code,
                }),
            )
            .with_correlation(correlation_id)
            .with_compliance(ComplianceEnvelope {
                actor,
                target_resource: format!("apikey:{document_id}"),
                target_tenant: tenant_id.to_string(),
                operation: "validate".to_string(),
                outcome: outcome.to_string(),
                reason_code: reason_code.to_string(),
                auth_method,
                source_ip: ip_address.to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
    }

    /// Enforce the caller's tenant against a resolved key record's tenant
    /// (resolve-before-mutate, I2.5). A caller carrying tenant `T` may only
    /// mutate keys whose tenant is `T`. Keys with an empty (global) tenant are
    /// admin-only and accepted from any authenticated caller (the broker authz
    /// layer already gates the admin scope).
    ///
    /// **Fail-closed (I2.5 hardening).** For a tenant-scoped target key, an
    /// absent/empty caller tenant is NO LONGER treated as an unrestricted admin
    /// call: a caller could otherwise bypass tenant isolation simply by omitting
    /// `context.tenant`. An empty caller tenant against a tenant-scoped key is
    /// DENIED unless the caller carries a genuine cross-tenant admin role
    /// ([`caller_is_cross_tenant_admin`]). Same-tenant and the legitimate
    /// cross-tenant-admin paths keep working; the global-key path (empty record
    /// tenant) is unchanged.
    fn enforce_caller_tenant(
        &self,
        context: Option<&common_pb::RequestContext>,
        record_tenant: &str,
    ) -> Result<(), Status> {
        let record_tenant = record_tenant.trim();
        // Global/admin-scoped key (no tenant boundary): the broker authz layer
        // already gates the admin scope, so accept from any authenticated caller.
        if record_tenant.is_empty() {
            return Ok(());
        }
        let caller_tenant = context
            .and_then(|c| c.tenant.as_ref())
            .map(|t| t.tenant_id.trim().to_string())
            .unwrap_or_default();
        // Tenant-scoped target with an absent/empty caller tenant: FAIL CLOSED.
        // Only a genuine cross-tenant admin may reach across the boundary; a
        // caller that merely omitted its tenant context is denied.
        if caller_tenant.is_empty() {
            if Self::caller_is_cross_tenant_admin(context) {
                return Ok(());
            }
            return Err(Self::tenant_context_required_status());
        }
        if caller_tenant != record_tenant && !Self::caller_is_cross_tenant_admin(context) {
            return Err(Self::tenant_mismatch_status());
        }
        Ok(())
    }

    /// Enforce the full tenant/project lineage of a resolved API-key record.
    /// A project-bound claim is restricted to that exact project, including
    /// denial of tenant-wide (empty-project) keys. A deliberately tenant-bound
    /// claim with no project retains tenant-wide administration inside its own
    /// tenant. The existing platform-admin escape hatch remains authoritative.
    fn enforce_caller_scope(
        &self,
        context: Option<&common_pb::RequestContext>,
        record_tenant: &str,
        record_project: &str,
    ) -> Result<(), Status> {
        // `enforce_caller_tenant` already grants a cross-tenant admin its
        // tenant reach. Returning Ok here as well made that admin scope skip the
        // PROJECT check too, so a caller whose claim is bound to project-a could
        // act on project-b purely by holding `udb:auth:admin` — the admin scope
        // silently dissolved project separation. A claim that names a concrete
        // project stays bound to it; an admin wanting cross-project reach simply
        // does not carry a project claim (empty claim project still passes).
        self.enforce_caller_tenant(context, record_tenant)?;
        let caller_project = context
            .and_then(|c| c.tenant.as_ref())
            .map(|tenant| tenant.project_id.trim())
            .unwrap_or_default();
        if !caller_project.is_empty() && caller_project != record_project.trim() {
            return Err(Self::project_mismatch_status());
        }
        Ok(())
    }

    /// Whether the caller holds a genuine cross-tenant (platform) admin role,
    /// the only identity permitted to act on a tenant-scoped key without
    /// supplying a matching tenant context. Mirrors the `app.platform_admin`
    /// escape hatch used by the native RLS policies (storage/webrtc/tenant) so
    /// the trait-level gate and the row-level gate recognize the SAME role set.
    /// Empty caller / no recognized role → not an admin (fail-closed default).
    fn caller_is_cross_tenant_admin(context: Option<&common_pb::RequestContext>) -> bool {
        const CROSS_TENANT_ADMIN_ROLES: &[&str] = &[
            "platform_admin",
            "udb:platform_admin",
            "superadmin",
            "super_admin",
        ];
        const CROSS_TENANT_ADMIN_SCOPES: &[&str] = &["*", "udb:*", "udb:admin", "udb:auth:admin"];
        context
            .map(|c| {
                let role_admin = c.roles.iter().any(|role| {
                    let role = role.trim();
                    CROSS_TENANT_ADMIN_ROLES
                        .iter()
                        .any(|admin| role.eq_ignore_ascii_case(admin))
                });
                let scope_admin = c.scopes.iter().any(|scope| {
                    let scope = scope.trim();
                    CROSS_TENANT_ADMIN_SCOPES.contains(&scope)
                });
                role_admin || scope_admin
            })
            .unwrap_or(false)
    }

    fn current_claim_request_context() -> Option<common_pb::RequestContext> {
        if !crate::runtime::service::method_security::claim_context_present() {
            return None;
        }
        let claim = crate::runtime::service::method_security::current_claim_context();
        let tenant = if claim.tenant_id.trim().is_empty() && claim.project_id.trim().is_empty() {
            None
        } else {
            Some(common_pb::TenantContext {
                tenant_id: claim.tenant_id,
                project_id: claim.project_id,
                ..Default::default()
            })
        };
        Some(common_pb::RequestContext {
            tenant,
            principal_id: claim.subject,
            scopes: claim.scopes,
            roles: claim.roles,
            ..Default::default()
        })
    }

    fn read_tenant_filter(
        context: Option<&common_pb::RequestContext>,
    ) -> Result<Option<String>, Status> {
        let Some(context) = context else {
            return Ok(None);
        };
        if Self::caller_is_cross_tenant_admin(Some(context)) {
            return Ok(None);
        }
        let caller_tenant = context
            .tenant
            .as_ref()
            .map(|tenant| tenant.tenant_id.trim().to_string())
            .unwrap_or_default();
        if caller_tenant.is_empty() {
            return Err(Self::read_tenant_required_status());
        }
        Ok(Some(caller_tenant))
    }

    /// Optional project restriction for list/read surfaces. Platform admins and
    /// intentionally tenant-bound callers retain their broader posture; a
    /// project-bound claim is filtered before pagination.
    fn read_project_filter(context: Option<&common_pb::RequestContext>) -> Option<String> {
        if Self::caller_is_cross_tenant_admin(context) {
            return None;
        }
        context
            .and_then(|c| c.tenant.as_ref())
            .map(|tenant| tenant.project_id.trim().to_string())
            .filter(|project| !project.is_empty())
    }

    fn api_key_matches_status(
        rec: &ApiKeyRecord,
        status: authn::ApiKeyStatus,
        now_unix: u64,
    ) -> bool {
        match status {
            authn::ApiKeyStatus::Unspecified => true,
            authn::ApiKeyStatus::Active => !rec.is_revoked() && !rec.is_expired(now_unix),
            authn::ApiKeyStatus::Revoked => rec.is_revoked(),
            authn::ApiKeyStatus::Expired => !rec.is_revoked() && rec.is_expired(now_unix),
        }
    }

    /// Count this key's requests in the trailing `window_secs` for a meaningful
    /// `rate_limited` decision.
    ///
    /// Returns `Ok(count)` on a successful count (where no usage table/pool is
    /// wired is a successful `Ok(0)` — there is simply nothing to count), and
    /// `Err(())` only when the rate-limit store **errored** (query failure /
    /// missing column). The caller decides what a store error means via
    /// [`rate_limit_decision`]: in fail-closed mode it denies, in fail-open mode
    /// it degrades to "not limited". This keeps store-error distinguishable from
    /// a legitimate zero so a DB outage cannot silently disable rate limiting.
    async fn recent_request_count(&self, key_prefix: &str, window_secs: i64) -> Result<i64, ()> {
        use crate::runtime::native_catalog::native_model;
        use sqlx::Row;
        let Some(pool) = self.pg_pool.as_ref() else {
            // No usage table/pool wired: nothing to count, not a store error.
            return Ok(0);
        };
        let usage = native_model(
            "udb.core.apikey.entity.v1.ApiKeyUsage",
            &["key_id", "requested_at"],
        );
        let ak = native_model(
            "udb.core.apikey.entity.v1.ApiKey",
            &["key_id", "key_prefix"],
        );
        let sql = format!(
            "SELECT COUNT(*)::bigint AS cnt FROM {rel} \
             WHERE {key_id} = (SELECT {ak_id} FROM {ak_rel} WHERE {ak_prefix} = $1 LIMIT 1) \
               AND {req_at} >= NOW() - ($2 || ' seconds')::interval",
            rel = usage.relation,
            key_id = usage.q("key_id"),
            ak_id = ak.q("key_id"),
            ak_rel = ak.relation,
            ak_prefix = ak.q("key_prefix"),
            req_at = usage.q("requested_at"),
        );
        match sqlx::query(&sql)
            .bind(key_prefix)
            .bind(window_secs.to_string())
            .fetch_one(pool)
            .await
        {
            Ok(row) => Ok(row.try_get::<i64, _>("cnt").unwrap_or(0)),
            Err(err) => {
                tracing::warn!(error = %err, "api-key rate-limit count query failed");
                Err(())
            }
        }
    }

    /// Count the DISTINCT non-null source IPs this key was used from in the
    /// trailing `window_secs` (the prior requests already persisted by
    /// [`Self::record_usage`]). This is the trustworthy, cheap anomaly signal the
    /// validate path lacked: a single API key fanning out across many client IPs
    /// indicates credential sharing or theft — orthogonal to the per-minute
    /// volume that drives `rate_limited`. Returns 0 when no usage table/pool is
    /// wired, or on a store error (anomaly detection fails OPEN — it must never
    /// turn into a second denial path; rate limiting owns the deny decision).
    async fn distinct_ip_count(&self, key_prefix: &str, window_secs: i64) -> i64 {
        use crate::runtime::native_catalog::native_model;
        use sqlx::Row;
        let Some(pool) = self.pg_pool.as_ref() else {
            return 0;
        };
        let usage = native_model(
            "udb.core.apikey.entity.v1.ApiKeyUsage",
            &["key_id", "ip_address", "requested_at"],
        );
        let ak = native_model(
            "udb.core.apikey.entity.v1.ApiKey",
            &["key_id", "key_prefix"],
        );
        let sql = format!(
            "SELECT COUNT(DISTINCT {ip})::bigint AS cnt FROM {rel} \
             WHERE {key_id} = (SELECT {ak_id} FROM {ak_rel} WHERE {ak_prefix} = $1 LIMIT 1) \
               AND {ip} IS NOT NULL \
               AND {req_at} >= NOW() - ($2 || ' seconds')::interval",
            rel = usage.relation,
            key_id = usage.q("key_id"),
            ip = usage.q("ip_address"),
            ak_id = ak.q("key_id"),
            ak_rel = ak.relation,
            ak_prefix = ak.q("key_prefix"),
            req_at = usage.q("requested_at"),
        );
        match sqlx::query(&sql)
            .bind(key_prefix)
            .bind(window_secs.to_string())
            .fetch_one(pool)
            .await
        {
            Ok(row) => row.try_get::<i64, _>("cnt").unwrap_or(0),
            Err(err) => {
                tracing::warn!(error = %err, "api-key distinct-IP anomaly query failed");
                0
            }
        }
    }

    /// Persist one `api_key_usages` row for a `ValidateApiKey` call so the
    /// rate-limit window ([`recent_request_count`]) and the usage-stats
    /// aggregation operate on REAL data — previously NOTHING ever inserted into
    /// this table, so the COUNT was always ~0 and `rate_limited` could never
    /// fire (a capability-lie). One bounded INSERT per validate on the hot path.
    ///
    /// **Non-fatal:** a usage-write failure is logged and swallowed — it must
    /// never fail the validate (the rate-limit COUNT keeps its own fail-closed
    /// semantics independently). The FK `key_id` is resolved from the public
    /// `key_prefix` via the `api_keys` table inside the INSERT, mirroring the
    /// resolution in `recent_request_count`/`get_api_key_usage_stats`. All
    /// table/column names come from the proto manifest (`native_model`) — no
    /// hardcoded SQL identifiers.
    async fn record_usage(
        &self,
        key_prefix: &str,
        tenant_id: &str,
        endpoint: &str,
        ip_address: &str,
        http_status: i32,
        rate_limited: bool,
    ) {
        use crate::runtime::native_catalog::native_model;
        let Some(pool) = self.pg_pool.as_ref() else {
            // No usage table/pool wired (e.g. non-Postgres or test): nothing to
            // record. Not an error — the count path degrades to 0 the same way.
            return;
        };
        // requested_at / usage_id are excluded-from-insert (DB defaults:
        // CURRENT_TIMESTAMP / gen_random_uuid()), so they are NOT written here.
        let usage = native_model(
            "udb.core.apikey.entity.v1.ApiKeyUsage",
            &[
                "key_id",
                "endpoint",
                "ip_address",
                "http_status",
                "latency_ms",
                "rate_limited",
                "tenant_id",
            ],
        );
        let ak = native_model(
            "udb.core.apikey.entity.v1.ApiKey",
            &["key_id", "key_prefix"],
        );
        // `ip_address` is INET; an empty/invalid string would error the cast, so
        // bind NULL for a blank IP (the column is nullable) and cast non-empty
        // values to INET.
        let sql = format!(
            "INSERT INTO {rel} ({key_id}, {endpoint}, {ip}, {status}, {latency}, {rl}, {tenant}) \
             SELECT {ak_id}, $2, NULLIF($3, '')::INET, $4, NULL, $5, $6 \
             FROM {ak_rel} WHERE {ak_prefix} = $1 LIMIT 1",
            rel = usage.relation,
            key_id = usage.q("key_id"),
            endpoint = usage.q("endpoint"),
            ip = usage.q("ip_address"),
            status = usage.q("http_status"),
            latency = usage.q("latency_ms"),
            rl = usage.q("rate_limited"),
            tenant = usage.q("tenant_id"),
            ak_id = ak.q("key_id"),
            ak_rel = ak.relation,
            ak_prefix = ak.q("key_prefix"),
        );
        if let Err(err) = sqlx::query(&sql)
            .bind(key_prefix)
            .bind(endpoint)
            .bind(ip_address)
            .bind(http_status)
            .bind(rate_limited)
            .bind(tenant_id)
            .execute(pool)
            .await
        {
            // Swallow: the usage log is observability/rate-limit feed, not the
            // auth decision. Never fail the validate because of it.
            tracing::warn!(error = %err, "api-key usage-row insert failed (validate still served)");
        }
    }
}

/// Pure rate-limit decision so the fail-open/closed branch is unit-testable
/// without a live DB.
///
/// - `limit <= 0` → rate limiting disabled, never limited (regardless of mode).
/// - `Ok(count)` → limited iff `count >= limit` (the normal counting path).
/// - `Err(())` (rate-limit store error) → limited when `fail_closed` is true
///   (deny), otherwise `false` (legacy fail-open / degrade to "not limited").
fn rate_limit_decision(count: Result<i64, ()>, limit: i64, fail_closed: bool) -> bool {
    if limit <= 0 {
        return false;
    }
    match count {
        Ok(count) => count >= limit,
        Err(()) => fail_closed,
    }
}

fn expires_at_unix(ts: Option<prost_types::Timestamp>) -> u64 {
    ts.map(|ts| ts.seconds.max(0) as u64).unwrap_or(0)
}

fn status_for(rec: &ApiKeyRecord, now_unix: u64) -> apikey_entity_pb::ApiKeyStatus {
    if rec.is_revoked() {
        apikey_entity_pb::ApiKeyStatus::Revoked
    } else if rec.is_expired(now_unix) {
        apikey_entity_pb::ApiKeyStatus::Expired
    } else {
        apikey_entity_pb::ApiKeyStatus::Active
    }
}

fn api_key_to_pb(rec: &ApiKeyRecord, now_unix: u64) -> apikey_entity_pb::ApiKey {
    let mut dto = apikey_entity_pb::ApiKey {
        key_id: rec.key_prefix.clone(),
        key_prefix: rec.key_prefix.clone(),
        // Never return the stored key hash over the read API — the digest should
        // not leave the storage layer (defense-in-depth if the hash secret leaks).
        key_hash: String::new(),
        // Return the STORED name so a provisioner that reconciles by exact name
        // can match an existing key. Falling back to the prefix (the persisted
        // store applies the same fallback) keeps unnamed legacy rows non-empty
        // instead of reconciling against "".
        name: if rec.name.trim().is_empty() {
            rec.key_prefix.clone()
        } else {
            rec.name.clone()
        },
        description: rec.description.clone(),
        owner_type: apikey_entity_pb::ApiKeyOwnerType::ServiceAccount as i32,
        owner_id: rec.principal_id.clone(),
        scopes_json: serde_json::to_string(&rec.scopes).unwrap_or_else(|_| "[]".to_string()),
        status: status_for(rec, now_unix) as i32,
        ip_allowlist_json: "[]".to_string(),
        rate_limit_per_minute: 60,
        rate_limit_per_day: 10_000,
        created_by: String::new(),
        revoked_by: String::new(),
        revoke_reason: String::new(),
        expires_at: timestamp_from_unix(rec.expires_at_unix),
        last_used_at: timestamp_from_unix(rec.last_used_at_unix),
        created_at: timestamp_from_unix(rec.created_at_unix),
        updated_at: timestamp_from_unix(rec.created_at_unix),
        deleted_at: timestamp_from_unix(rec.revoked_at_unix),
        deleted_by: String::new(),
        tenant_id: rec.tenant_id.clone(),
        project_id: rec.project_id.clone(),
        allowed_resources_json: "[]".to_string(),
        metadata_json: serde_json::json!({
            "service_identity": rec.service_identity.clone(),
            "grant_revision": rec.grant_revision,
            "native_model": "udb.core.apikey.entity.v1.ApiKey"
        })
        .to_string(),
    };
    // §7: structurally blank OUTPUT_VIEW_STORAGE_ONLY fields (key_hash) via
    // descriptor-driven codegen.
    crate::proto_redaction::RedactStorageOnly::redact_storage_only(&mut dto);
    dto
}

#[tonic::async_trait]
impl ApiKeyService for ApiKeyServiceImpl {
    async fn create_api_key(
        &self,
        request: Request<apikey_pb::CreateApiKeyRequest>,
    ) -> Result<Response<apikey_pb::CreateApiKeyResponse>, Status> {
        if self.hash_key().is_empty() {
            return Err(Self::capability_status(
                "create_hashing",
                "api_key_hash_secret",
                "API key hashing requires UDB_SESSION_HASH_SECRET",
            ));
        }
        let req = request.into_inner();
        if req.owner_id.trim().is_empty() {
            return Err(Self::required_field(
                "owner_id",
                "must be a non-empty API key owner id",
                "owner_id is required",
            ));
        }
        if req.owner_type != apikey_entity_pb::ApiKeyOwnerType::Unspecified as i32
            && req.owner_type != apikey_entity_pb::ApiKeyOwnerType::ServiceAccount as i32
        {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "only service-account API keys are supported by the durable auth store",
                [("owner_type", "must be API_KEY_OWNER_TYPE_SERVICE_ACCOUNT")],
            ));
        }
        let prefix = format!(
            "udbk_{}",
            Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(12)
                .collect::<String>()
        );
        let plain_key = format!("{}.{}", prefix, Uuid::new_v4().simple());
        let now = now_unix();
        let requested_tenant = req
            .context
            .as_ref()
            .and_then(|ctx| ctx.tenant.as_ref())
            .map(|tenant| tenant.tenant_id.clone())
            .unwrap_or_default();
        let requested_project = req
            .context
            .as_ref()
            .and_then(|ctx| ctx.tenant.as_ref())
            .map(|tenant| tenant.project_id.clone())
            .unwrap_or_default();
        let grant = self
            .active_service_account_grant(
                req.owner_id.trim(),
                &requested_tenant,
                &requested_project,
            )
            .await?;
        // A4: bind the VALIDATED caller claim (never the spoofable/omittable body
        // `req.context`) to the grant's authoritative tenant — a tenant-A caller
        // must not mint a key inheriting a tenant-B service account's grant/scopes.
        // Mirrors the sibling get/update/revoke/rotate handlers.
        let caller_context = Self::current_claim_request_context();
        self.enforce_caller_scope(caller_context.as_ref(), &grant.tenant_id, &grant.project_id)?;
        let key_scopes = super::grants::validate_service_scopes(&req.scopes, &grant.scopes)?;
        let rec = ApiKeyRecord {
            key_prefix: authn::api_key_prefix(&plain_key),
            key_hash: authn::hash_secret(&plain_key, &self.hash_key()),
            name: req.name.trim().to_string(),
            description: req.description.trim().to_string(),
            principal_id: req.owner_id,
            service_identity: grant.service_identity,
            tenant_id: grant.tenant_id,
            project_id: grant.project_id,
            scopes: key_scopes,
            grant_revision: grant.revision,
            created_at_unix: now,
            last_used_at_unix: 0,
            expires_at_unix: expires_at_unix(req.expires_at),
            revoked_at_unix: 0,
            // Per-key budgets from the request (0 = use the column default). The
            // opt-in per-key DataBroker limiter reads `rate_limit_per_minute` to
            // raise this key above the tenant default.
            rate_limit_per_minute: req.rate_limit_per_minute as i64,
            rate_limit_per_day: req.rate_limit_per_day,
        };
        // UDB-AUTH-009: enforce ACTIVE-name uniqueness per owner so an
        // idempotent provisioner reconciles instead of silently minting a
        // duplicate active key. A non-empty name that already names an ACTIVE
        // key for this owner is rejected with FailedPrecondition; blank names
        // (which default to the key prefix) are inherently unique and skip the
        // check.
        if !rec.name.trim().is_empty() {
            let existing = self
                .api_keys
                .list_for_principal(&rec.principal_id, true, now)
                .await
                .map_err(|err| Self::internal_status("create_api_key_uniqueness", err))?;
            if existing
                .iter()
                .any(|key| key.name.trim().eq_ignore_ascii_case(rec.name.trim()))
            {
                return Err(crate::runtime::executor_utils::failed_precondition_fields(
                    "an ACTIVE API key with this name already exists for the owner",
                    [(
                        "name".to_string(),
                        "revoke or rotate the existing key, or choose a distinct name".to_string(),
                    )],
                ));
            }
        }
        // Q#16: persist through the SAME store the reads use (`self.api_keys` →
        // `self.pool`), exactly like update/revoke/rotate. Previously CreateApiKey
        // alone diverged to the runtime's native-entity write (a different instance
        // pool), so a freshly created key could be invisible to `get_by_prefix`
        // (split-brain create-vs-read). `api_keys` is `enable_rls: false`, so the
        // store's typed `put` is sufficient; the domain event is emitted below.
        self.api_keys
            .put(rec.clone())
            .await
            .map_err(|err| Self::internal_status("create_api_key_store", err))?;
        self.emit_event(AuthEvent::new(
            topics::API_KEY_CREATED,
            rec.key_prefix.clone(),
            rec.tenant_id.clone(),
            serde_json::json!({
                "key_id": rec.key_prefix.clone(),
                "key_prefix": rec.key_prefix.clone(),
                "owner_id": rec.principal_id.clone(),
                "scopes": rec.scopes.clone(),
                "tenant_id": rec.tenant_id.clone(),
                "project_id": rec.project_id.clone(),
            }),
        ))
        .await;
        Ok(Response::new(apikey_pb::CreateApiKeyResponse {
            key: Some(api_key_to_pb(&rec, now)),
            plain_key,
        }))
    }

    async fn get_api_key(
        &self,
        request: Request<apikey_pb::GetApiKeyRequest>,
    ) -> Result<Response<apikey_pb::GetApiKeyResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let caller_context = Self::current_claim_request_context();
        let key = match self
            .api_keys
            .get_by_prefix(&req.key_id)
            .await
            .map_err(|err| Self::internal_status("get_api_key_load", err))?
        {
            Some(rec) => {
                self.enforce_caller_scope(
                    caller_context.as_ref(),
                    &rec.tenant_id,
                    &rec.project_id,
                )?;
                Some(api_key_to_pb(&rec, now))
            }
            None => return Err(Self::api_key_not_found_status("get_api_key")),
        };
        Ok(Response::new(apikey_pb::GetApiKeyResponse { key }))
    }

    async fn list_api_keys(
        &self,
        request: Request<apikey_pb::ListApiKeysRequest>,
    ) -> Result<Response<apikey_pb::ListApiKeysResponse>, Status> {
        let req = request.into_inner();
        if req.owner_id.trim().is_empty() {
            return Err(Self::required_field(
                "owner_id",
                "must be a non-empty API key owner id",
                "owner_id is required",
            ));
        }
        let now = now_unix();
        // Convert the proto status filter to the domain enum the store expects
        // (proto-enum-out-of-domain: the store API takes `authn::ApiKeyStatus`).
        let status = authn::ApiKeyStatus::from_i32(req.status);
        let page = req.page.as_ref();
        let (limit, offset, _) = bounded_page_window(page);
        let caller_context = Self::current_claim_request_context();
        let tenant_filter = Self::read_tenant_filter(caller_context.as_ref())?;
        let project_filter = Self::read_project_filter(caller_context.as_ref());
        let mut records = self
            .api_keys
            .list_for_principal(&req.owner_id, false, now)
            .await
            .map_err(|err| Self::internal_status("list_api_keys_store", err))?;
        records.retain(|rec| Self::api_key_matches_status(rec, status, now));
        if let Some(tenant_filter) = tenant_filter {
            records.retain(|rec| rec.tenant_id.trim() == tenant_filter);
        }
        if let Some(project_filter) = project_filter {
            records.retain(|rec| rec.project_id.trim() == project_filter);
        }
        for rec in &records {
            self.enforce_caller_scope(caller_context.as_ref(), &rec.tenant_id, &rec.project_id)?;
        }
        let total = records.len();
        let keys = records
            .iter()
            .skip(offset)
            .take(limit)
            .map(|rec| api_key_to_pb(rec, now))
            .collect();
        Ok(Response::new(apikey_pb::ListApiKeysResponse {
            keys,
            page: Some(bounded_page_response(total, page)),
        }))
    }

    async fn update_api_key(
        &self,
        request: Request<apikey_pb::UpdateApiKeyRequest>,
    ) -> Result<Response<apikey_pb::UpdateApiKeyResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let mut rec = self
            .api_keys
            .get_by_prefix(&req.key_id)
            .await
            .map_err(|err| Self::internal_status("update_api_key_load", err))?
            .ok_or_else(|| Self::api_key_not_found_status("update_api_key"))?;
        // Resolve-before-mutate (I2.5): enforce caller tenant against the record.
        // The caller tenant comes from the VALIDATED claim (installed by the
        // method-security layer), NOT the client-supplied `req.context` body —
        // matching the read paths (`list`/`get`/`validate`) and never trusting a
        // spoofable/absent body tenant.
        let caller_context = Self::current_claim_request_context();
        self.enforce_caller_scope(caller_context.as_ref(), &rec.tenant_id, &rec.project_id)?;
        let grant = self
            .active_service_account_grant(&rec.principal_id, &rec.tenant_id, &rec.project_id)
            .await?;
        let update_mask =
            update_mask_path_set(req.update_mask.as_ref(), &["scopes", "expires_at"])?;
        if update_mask_allows(&update_mask, "scopes", !req.scopes.is_empty()) {
            rec.scopes = super::grants::validate_service_scopes(&req.scopes, &grant.scopes)?;
            rec.grant_revision = grant.revision;
        } else if rec.grant_revision != grant.revision {
            return Err(Self::grant_precondition_status(
                "API key was reviewed against an older service-account grant revision",
                "scopes",
                "include scopes in the update to explicitly review the current grant",
            ));
        }
        rec.service_identity = grant.service_identity;
        if update_mask_allows(&update_mask, "expires_at", req.expires_at.is_some()) {
            rec.expires_at_unix = expires_at_unix(req.expires_at);
        }
        self.api_keys
            .put(rec.clone())
            .await
            .map_err(|err| Self::internal_status("update_api_key_store", err))?;
        self.emit_event(AuthEvent::new(
            topics::API_KEY_UPDATED,
            rec.key_prefix.clone(),
            String::new(),
            serde_json::json!({
                "key_id": req.key_id.clone(),
                "key_prefix": rec.key_prefix.clone(),
                "scopes": rec.scopes.clone(),
                "expires_at_unix": rec.expires_at_unix,
            }),
        ))
        .await;
        Ok(Response::new(apikey_pb::UpdateApiKeyResponse {
            key: Some(api_key_to_pb(&rec, now)),
        }))
    }

    async fn revoke_api_key(
        &self,
        request: Request<apikey_pb::RevokeApiKeyRequest>,
    ) -> Result<Response<apikey_pb::RevokeApiKeyResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        // Resolve-before-mutate (I2.5): enforce caller tenant against the record
        // BEFORE the revoke (no prefix-only mutation).
        let existing = self
            .api_keys
            .get_by_prefix(&req.key_id)
            .await
            .map_err(|err| Self::internal_status("revoke_api_key_load", err))?
            .ok_or_else(|| Self::api_key_not_found_status("revoke_api_key"))?;
        // Caller tenant from the VALIDATED claim, not the spoofable body context.
        let caller_context = Self::current_claim_request_context();
        self.enforce_caller_scope(
            caller_context.as_ref(),
            &existing.tenant_id,
            &existing.project_id,
        )?;
        let ok = self
            .api_keys
            .revoke(&req.key_id, now)
            .await
            .map_err(|err| Self::internal_status("revoke_api_key_store", err))?;
        if !ok {
            return Err(Self::api_key_not_found_status("revoke_api_key"));
        }
        self.emit_event(AuthEvent::new(
            topics::API_KEY_REVOKED,
            req.key_id.clone(),
            String::new(),
            serde_json::json!({
                "key_id": req.key_id.clone(),
                "key_prefix": req.key_id.clone(),
            }),
        ))
        .await;
        Ok(Response::new(apikey_pb::RevokeApiKeyResponse {
            key_id: req.key_id,
            revoked_at: timestamp_from_unix(now),
            operation_id: Uuid::new_v4().to_string(),
        }))
    }

    async fn rotate_api_key(
        &self,
        request: Request<apikey_pb::RotateApiKeyRequest>,
    ) -> Result<Response<apikey_pb::RotateApiKeyResponse>, Status> {
        if self.hash_key().is_empty() {
            return Err(Self::capability_status(
                "rotate_hashing",
                "api_key_hash_secret",
                "API key hashing requires UDB_SESSION_HASH_SECRET",
            ));
        }
        let req = request.into_inner();
        let key_ref = req.key_id.trim();
        if key_ref.is_empty() {
            return Err(Self::required_field(
                "key_id",
                "must be a non-empty API key id",
                "key_id is required",
            ));
        }
        let now = now_unix();
        // Resolve the record FIRST, then enforce caller tenant against it (no
        // prefix-only mutation): rotation keeps the same lineage (owner, scopes,
        // tenant, project) under a fresh secret.
        let existing = self
            .api_keys
            .get_by_prefix(key_ref)
            .await
            .map_err(|err| Self::internal_status("rotate_api_key_load", err))?
            .ok_or_else(|| Self::api_key_not_found_status("rotate_api_key"))?;
        // Caller tenant from the VALIDATED claim, not the spoofable body context.
        let caller_context = Self::current_claim_request_context();
        self.enforce_caller_scope(
            caller_context.as_ref(),
            &existing.tenant_id,
            &existing.project_id,
        )?;
        let grant = self
            .active_service_account_grant(
                &existing.principal_id,
                &existing.tenant_id,
                &existing.project_id,
            )
            .await?;
        let rotated_scopes =
            super::grants::validate_service_scopes(&existing.scopes, &grant.scopes)?;
        // Mint a fresh secret under a new prefix; lineage carried via the event +
        // metadata. The new key inherits scopes/owner/tenant/project.
        let prefix = format!(
            "udbk_{}",
            Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(12)
                .collect::<String>()
        );
        let plain_key = format!("{}.{}", prefix, Uuid::new_v4().simple());
        let new_prefix = authn::api_key_prefix(&plain_key);
        let new_rec = ApiKeyRecord {
            key_prefix: new_prefix.clone(),
            key_hash: authn::hash_secret(&plain_key, &self.hash_key()),
            name: existing.name.clone(),
            description: existing.description.clone(),
            principal_id: existing.principal_id.clone(),
            service_identity: grant.service_identity,
            tenant_id: grant.tenant_id,
            project_id: grant.project_id,
            scopes: rotated_scopes,
            grant_revision: grant.revision,
            created_at_unix: now,
            last_used_at_unix: 0,
            expires_at_unix: existing.expires_at_unix,
            revoked_at_unix: 0,
            // Preserve the key's per-key budget across rotation.
            rate_limit_per_minute: existing.rate_limit_per_minute,
            rate_limit_per_day: existing.rate_limit_per_day,
        };
        // Atomic insert-new + revoke-old (store override is a single tx).
        self.api_keys
            .rotate(&existing.key_prefix, new_rec.clone(), now)
            .await
            .map_err(|err| Self::internal_status("rotate_api_key_store", err))?;
        self.emit_event(AuthEvent::new(
            topics::API_KEY_ROTATED,
            new_rec.key_prefix.clone(),
            new_rec.tenant_id.clone(),
            serde_json::json!({
                "key_id": new_rec.key_prefix.clone(),
                "key_prefix": new_rec.key_prefix.clone(),
                "previous_key_id": existing.key_prefix.clone(),
                "owner_id": new_rec.principal_id.clone(),
                "tenant_id": new_rec.tenant_id.clone(),
                "project_id": new_rec.project_id.clone(),
                "rotation_reason": req.rotation_reason.clone(),
                "operation": "rotate",
                "result": "ok",
            }),
        ))
        .await;
        Ok(Response::new(apikey_pb::RotateApiKeyResponse {
            key: Some(api_key_to_pb(&new_rec, now)),
            plain_key,
            previous_key_id: existing.key_prefix,
        }))
    }

    async fn emergency_revoke_api_keys(
        &self,
        request: Request<apikey_pb::EmergencyRevokeApiKeysRequest>,
    ) -> Result<Response<apikey_pb::EmergencyRevokeApiKeysResponse>, Status> {
        use crate::runtime::native_catalog::native_model;
        use sqlx::Row;

        let req = request.into_inner();
        // At least one selector must be present so this never revokes "everything".
        let has_selector = !req.key_prefix.trim().is_empty()
            || !req.owner_id.trim().is_empty()
            || !req.tenant_id.trim().is_empty()
            || !req.project_id.trim().is_empty()
            || !req.scope.trim().is_empty()
            || req.created_before.is_some();
        if !has_selector {
            return Err(Self::required_field(
                "selectors",
                "must include at least one of key_prefix, owner_id, tenant_id, project_id, scope, or created_before",
                "at least one selector is required (prefix/owner/tenant/project/scope/created_before)",
            ));
        }
        // Caller tenant must match the tenant selector when one is given; an
        // empty tenant selector requires the caller's own tenant (no cross-tenant
        // global wipe from a tenant-scoped caller). The caller tenant comes from
        // the VALIDATED claim, not the spoofable body context.
        let caller_context = Self::current_claim_request_context();
        let caller_tenant = caller_context
            .as_ref()
            .and_then(|c| c.tenant.as_ref())
            .map(|t| t.tenant_id.clone())
            .unwrap_or_default();
        let caller_project = caller_context
            .as_ref()
            .and_then(|c| c.tenant.as_ref())
            .map(|t| t.project_id.trim().to_string())
            .unwrap_or_default();
        let tenant_filter = if !req.tenant_id.trim().is_empty() {
            self.enforce_caller_tenant(caller_context.as_ref(), &req.tenant_id)?;
            req.tenant_id.clone()
        } else {
            caller_tenant
        };
        if tenant_filter.trim().is_empty() {
            return Err(Self::required_field(
                "tenant_id",
                "must be supplied directly or by caller tenant context",
                "emergency revoke requires tenant_id or tenant context",
            ));
        }
        if !req.project_id.trim().is_empty() {
            self.enforce_caller_scope(
                caller_context.as_ref(),
                &tenant_filter,
                req.project_id.trim(),
            )?;
        }
        // A project-bound claim can never widen the bulk selector by omitting
        // project_id. Tenant-bound callers (empty claim project) retain their
        // intentional tenant-wide posture; platform admins may choose either.
        let project_filter = if Self::caller_is_cross_tenant_admin(caller_context.as_ref())
            || caller_project.is_empty()
        {
            req.project_id.trim().to_string()
        } else {
            caller_project.clone()
        };
        let Some(pool) = self.pg_pool.as_ref() else {
            return Err(Self::capability_status(
                "emergency_revoke",
                "postgres_backend",
                "emergency revoke requires a Postgres backend",
            ));
        };
        let actor = req
            .context
            .as_ref()
            .and_then(|c| {
                [
                    c.user_id.as_str(),
                    c.principal_id.as_str(),
                    c.service_identity.as_str(),
                ]
                .into_iter()
                .find(|value| !value.trim().is_empty())
                .map(str::to_string)
            })
            .unwrap_or_else(|| crate::runtime::otel::current_actor());
        let auth_method = crate::runtime::otel::current_auth_method();
        let trace = req
            .context
            .as_ref()
            .map(|c| {
                (
                    c.trace_id.clone(),
                    c.span_id.clone(),
                    c.ip_address.clone(),
                    c.user_agent.clone(),
                )
            })
            .unwrap_or_default();
        let m = native_model(
            "udb.core.apikey.entity.v1.ApiKey",
            &[
                "key_prefix",
                "owner_id",
                "tenant_id",
                "project_id",
                "scopes_json",
                "created_at",
                "status",
                "deleted_at",
                "deleted_by",
                "revoked_by",
                "revoke_reason",
            ],
        );
        let created_before = req.created_before.as_ref().map(|t| t.seconds);
        // Resolve matching active prefixes first (resolve-before-mutate), then
        // revoke and report exactly which keys were affected.
        // Active-row + tenant/project filters are derived from the manifest
        // table-security model so the column names track the proto descriptor.
        let active = m
            .soft_delete_is_null()
            .unwrap_or_else(|| format!("{} IS NULL", m.q("deleted_at")));
        let tenant = m.tenant_column().unwrap_or_else(|| m.q("tenant_id"));
        let project = m.project_column().unwrap_or_else(|| m.q("project_id"));
        let select_sql = format!(
            "SELECT {prefix}::TEXT AS key_prefix FROM {rel} WHERE {active} \
               AND ($1 = '' OR {tenant} = $1) \
               AND ($2 = '' OR {prefix} LIKE $2 || '%') \
               AND ($3 = '' OR {owner} = $3) \
               AND ($4 = '' OR {project} = $4) \
               AND ($5 = '' OR {scopes} @> to_jsonb($5::text)) \
               AND ($6::BIGINT IS NULL OR {created} <= to_timestamp($6))",
            prefix = m.q("key_prefix"),
            rel = m.relation,
            active = active,
            tenant = tenant,
            owner = m.q("owner_id"),
            project = project,
            scopes = m.q("scopes_json"),
            created = m.q("created_at"),
        );
        let rows = sqlx::query(&select_sql)
            .bind(&tenant_filter)
            .bind(req.key_prefix.trim())
            .bind(req.owner_id.trim())
            .bind(&project_filter)
            .bind(req.scope.trim())
            .bind(created_before)
            .fetch_all(pool)
            .await
            .map_err(|err| {
                Self::internal_status(
                    "emergency_revoke_select",
                    format!("emergency revoke select failed: {err}"),
                )
            })?;
        let prefixes: Vec<String> = rows
            .iter()
            .filter_map(|r| r.try_get::<String, _>("key_prefix").ok())
            .collect();
        let now = now_unix();
        let mut revoked = Vec::new();
        for prefix in &prefixes {
            if self
                .api_keys
                .revoke(prefix, now)
                .await
                .map_err(|err| Self::internal_status("emergency_revoke_store", err))?
            {
                revoked.push(prefix.clone());
            }
        }
        let operation_id = Uuid::new_v4().to_string();
        let payload = serde_json::json!({
            "operation": "emergency_revoke",
            "result": "ok",
            "tenant_id": tenant_filter.clone(),
            "project_id": project_filter.clone(),
            "revoked_count": revoked.len(),
            "revoked_key_ids": revoked.clone(),
            "reason": req.reason.clone(),
            "redaction_profile": "standard",
        });
        self.emit_event(
            AuthEvent::new(
                topics::API_KEY_REVOKED,
                operation_id.clone(),
                tenant_filter.clone(),
                payload,
            )
            .with_correlation(operation_id.clone())
            .with_compliance(ComplianceEnvelope {
                actor,
                actor_project: caller_project,
                target_resource: if req.key_prefix.trim().is_empty() {
                    format!("apikey:tenant:{}", tenant_filter)
                } else {
                    format!("apikey:{}", req.key_prefix)
                },
                target_tenant: tenant_filter.clone(),
                target_project: project_filter,
                operation: "emergency_revoke".to_string(),
                outcome: "success".to_string(),
                reason_code: if req.reason.trim().is_empty() {
                    "emergency_revoke".to_string()
                } else {
                    req.reason.clone()
                },
                auth_method,
                source_ip: trace.2,
                user_agent: trace.3,
                trace_id: trace.0,
                span_id: trace.1,
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
        Ok(Response::new(apikey_pb::EmergencyRevokeApiKeysResponse {
            revoked_count: revoked.len() as i64,
            revoked_key_ids: revoked,
            operation_id,
        }))
    }

    async fn validate_api_key(
        &self,
        request: Request<apikey_pb::ValidateApiKeyRequest>,
    ) -> Result<Response<apikey_pb::ValidateApiKeyResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let Some(mut rec) = authn::validate_api_key(
            self.api_keys.as_ref(),
            &req.plain_key,
            &self.hash_key(),
            now,
        )
        .await
        .map_err(|err| Self::internal_status("validate_api_key_store", err))?
        else {
            // Validation failed (no matching/active key): emit the security
            // event so a bad/expired/revoked key attempt is audited. There is no
            // resolved prefix here, so the usage row is skipped (no FK to attach).
            self.emit_validate_event(
                topics::API_KEY_VALIDATE_FAILED,
                "",
                "",
                &req.endpoint,
                &req.ip_address,
                "failure",
                "invalid_or_revoked_key",
            )
            .await;
            return Ok(Response::new(apikey_pb::ValidateApiKeyResponse {
                valid: false,
                ..Default::default()
            }));
        };
        let pool = self.pg_pool.as_ref().ok_or_else(|| {
            Self::capability_status(
                "validate_grant_resolution",
                "postgres_auth_store",
                "API key validation requires the durable Postgres auth store",
            )
        })?;
        rec.scopes = match super::grants::attenuate_key_scopes_against_grant(
            pool,
            &rec.tenant_id,
            &rec.principal_id,
            rec.grant_revision,
            &rec.scopes,
        )
        .await
        {
            Ok(Some(scopes)) => scopes,
            Ok(None) => {
                self.emit_validate_event(
                    topics::API_KEY_VALIDATE_FAILED,
                    &rec.key_prefix,
                    &rec.tenant_id,
                    &req.endpoint,
                    &req.ip_address,
                    "failure",
                    "inactive_or_missing_service_account_grant",
                )
                .await;
                return Ok(Response::new(apikey_pb::ValidateApiKeyResponse {
                    valid: false,
                    ..Default::default()
                }));
            }
            Err(error) => {
                return Err(Self::internal_status("validate_api_key_grant", error));
            }
        };
        // Wildcard handling must match `SecurityContext::has_scope` (`*` and
        // `udb:*`) so an API key's scope semantics are uniform with the JWT/ABAC
        // path rather than only honoring a bare `*`.
        let scope_ok = req.required_scope.trim().is_empty()
            || rec
                .scopes
                .iter()
                .any(|scope| scope == "*" || scope == "udb:*" || scope == &req.required_scope);
        // Meaningful rate-limit decision (I2.5): compare the key's trailing-minute
        // request count against the per-minute ceiling (default 60 — matches the
        // proto column default). 0 ceiling = unlimited. When no usage table/pool
        // is wired the count is a legitimate 0, so this degrades to "not limited".
        //
        // Phase 5 fail-closed hardening: a rate-limit-store **error** (DB outage)
        // is distinguished from a real 0 count. In a hardened/production posture
        // a store error denies the request (rate_limited=true) rather than
        // silently letting it through; in dev/test it fails open as before.
        let per_minute_limit = self.config_rate_limit_per_minute();
        // Hot-path COUNT elision: when the key is UNLIMITED (`per_minute_limit == 0`)
        // the trailing-window COUNT can never change the decision (see
        // `rate_limit_decision`: `limit <= 0` → never limited), so we skip the DB
        // round-trip entirely and treat the count as a real 0. Only when a finite
        // ceiling is configured do we pay for the COUNT.
        //
        // Ordering invariant: when we DO count, the COUNT must run BEFORE this
        // request's own usage row is inserted, so the trailing-window count
        // reflects PRIOR requests (this request is the (count+1)th). Inserting
        // first would let a key count itself. `record_usage` is therefore spawned
        // AFTER this synchronous COUNT, never before it.
        let count = if per_minute_limit <= 0 {
            Ok(0)
        } else {
            self.recent_request_count(&rec.key_prefix, 60).await
        };
        let rate_limited = rate_limit_decision(
            count,
            per_minute_limit,
            crate::runtime::security::fail_closed_mode(),
        );
        let valid = scope_ok && !rate_limited;
        // Representative HTTP status for the persisted usage row: 429 when
        // rate-limited, 403 on a scope miss, else 200.
        let http_status: i32 = if rate_limited {
            429
        } else if !scope_ok {
            403
        } else {
            200
        };
        // The auth DECISION (valid/rate_limited/scopes) is now fully computed, so
        // build the response up front and run every best-effort side-effect off
        // the hot path. None of the following writes feed back into THIS response,
        // and the COUNT above already observed PRIOR requests, so deferring the
        // usage-row insert keeps the "this request's row is visible to the NEXT
        // request's COUNT" guarantee without adding latency to THIS response.
        let response = apikey_pb::ValidateApiKeyResponse {
            valid,
            key_id: rec.key_prefix.clone(),
            owner_id: rec.principal_id,
            owner_type: apikey_entity_pb::ApiKeyOwnerType::ServiceAccount as i32,
            scopes: rec.scopes,
            rate_limited,
        };

        // Detached best-effort writes: last_used_at stamp, the usage-row INSERT
        // (the NEXT request's COUNT feed), the validate audit events, and the
        // distinct-IP anomaly probe. A clone of the service carries the cheap Arc
        // handles (`api_keys`, `event_sink`, `pg_pool`, `config`) into the task;
        // every helper here is already non-fatal (logged + swallowed) so a spawned
        // failure can never affect the response, which has already returned.
        let svc = self.clone();
        let key_prefix = rec.key_prefix;
        let tenant_id = rec.tenant_id;
        let endpoint = req.endpoint;
        let ip_address = req.ip_address;
        tokio::spawn(async move {
            // Best-effort last_used_at stamp on a successful match. Fire-and-forget
            // so it never adds latency to (or fails) the auth hot path.
            let _ = svc.api_keys.touch_last_used(&key_prefix, now).await;
            // Persist this request into `api_key_usages` so the NEXT validate's
            // COUNT (and the usage-stats aggregation) act on real data — this is
            // the fix for the rate-limit capability-lie. Runs AFTER the
            // synchronous COUNT above so a key never counts itself. Non-fatal
            // (logged-only).
            svc.record_usage(
                &key_prefix,
                &tenant_id,
                &endpoint,
                &ip_address,
                http_status,
                rate_limited,
            )
            .await;
            // Emit the matching security event so the validate path is auditable:
            // rate-limit trips and scope failures were previously silent. (A
            // not-found key is handled on the synchronous path above.)
            if rate_limited {
                svc.emit_validate_event(
                    topics::API_KEY_RATE_LIMITED,
                    &key_prefix,
                    &tenant_id,
                    &endpoint,
                    &ip_address,
                    "deny",
                    "rate_limited",
                )
                .await;
            } else if !scope_ok {
                svc.emit_validate_event(
                    topics::API_KEY_VALIDATE_FAILED,
                    &key_prefix,
                    &tenant_id,
                    &endpoint,
                    &ip_address,
                    "failure",
                    "insufficient_scope",
                )
                .await;
            }
            // Anomalous-use detection (distinct-source-IP spread). A trustworthy,
            // cheap signal that does NOT duplicate rate limiting: rate limiting
            // counts request VOLUME, this counts how many distinct client IPs a
            // single key is used from in the trailing window. A key fanning out
            // across many IPs is a credential-sharing / stolen-key indicator. We
            // evaluate it AFTER `record_usage` (so this request's own IP is
            // included). It is observability/audit only — it never changes the
            // `valid` decision (anomaly detection must not become a second silent
            // deny path), so the response is unchanged. Disabled when the
            // threshold is 0 or no source IP was supplied.
            let anomaly_threshold = svc.config_distinct_ip_anomaly();
            if anomaly_threshold > 0 && !ip_address.trim().is_empty() {
                let distinct_ips = svc
                    .distinct_ip_count(&key_prefix, ApiKeyServiceImpl::ANOMALY_WINDOW_SECS)
                    .await;
                if distinct_ips > anomaly_threshold {
                    svc.emit_validate_event(
                        topics::API_KEY_ANOMALOUS_USE,
                        &key_prefix,
                        &tenant_id,
                        &endpoint,
                        &ip_address,
                        "anomaly",
                        "distinct_source_ip_spread",
                    )
                    .await;
                }
            }
        });

        Ok(Response::new(response))
    }

    async fn get_api_key_usage_stats(
        &self,
        request: Request<apikey_pb::GetApiKeyUsageStatsRequest>,
    ) -> Result<Response<apikey_pb::GetApiKeyUsageStatsResponse>, Status> {
        use std::collections::BTreeMap;

        use crate::runtime::native_catalog::native_model;
        use sqlx::Row;

        let req = request.into_inner();
        let key_ref = req.key_id.trim();
        if key_ref.is_empty() {
            return Err(Self::required_field(
                "key_id",
                "must be a non-empty API key id",
                "key_id is required",
            ));
        }
        let Some(pool) = self.pg_pool.as_ref() else {
            return Err(Self::capability_status(
                "usage_stats",
                "postgres_backend",
                "api-key usage stats require a Postgres backend",
            ));
        };

        // Resolve every column + the relation through the proto manifest so the
        // aggregation never hardcodes table/column names (proto is the source of
        // truth, per the native-service rule).
        let m = native_model(
            "udb.core.apikey.entity.v1.ApiKeyUsage",
            &[
                "key_id",
                "http_status",
                "latency_ms",
                "rate_limited",
                "requested_at",
            ],
        );
        // The API-key surface exposes the public `key_prefix` as the caller-facing
        // id (see `api_key_to_pb`), so `key_id` here is normally a prefix, not the
        // internal UUID FK on `api_key_usages`. Accept either: a UUID is matched
        // directly; otherwise resolve the prefix → UUID via the `api_keys` table.
        let ak = native_model(
            "udb.core.apikey.entity.v1.ApiKey",
            &["key_id", "key_prefix", "tenant_id", "project_id"],
        );
        let target_pred = if Uuid::parse_str(key_ref).is_ok() {
            format!("{key_id} = $1::UUID", key_id = ak.q("key_id"))
        } else {
            format!("{key_prefix} = $1", key_prefix = ak.q("key_prefix"))
        };
        let active = ak
            .soft_delete_is_null()
            .unwrap_or_else(|| "TRUE".to_string());
        let target_sql = format!(
            "SELECT {tenant}::TEXT AS tenant_id, COALESCE({project}::TEXT, '') AS project_id \
             FROM {rel} WHERE {active} AND {target_pred} LIMIT 1",
            tenant = ak.q("tenant_id"),
            project = ak.q("project_id"),
            rel = ak.relation,
            active = active,
            target_pred = target_pred,
        );
        let target = sqlx::query(&target_sql)
            .bind(key_ref)
            .fetch_optional(pool)
            .await
            .map_err(|err| {
                Self::internal_status(
                    "usage_stats_key_scope",
                    format!("usage stats key scope lookup failed: {err}"),
                )
            })?
            .ok_or_else(|| Self::api_key_not_found_status("get_api_key_usage_stats"))?;
        let target_tenant: String = target.try_get("tenant_id").map_err(|err| {
            Self::internal_status(
                "usage_stats_key_scope",
                format!("decode usage stats key tenant failed: {err}"),
            )
        })?;
        let target_project: String = target.try_get("project_id").map_err(|err| {
            Self::internal_status(
                "usage_stats_key_scope",
                format!("decode usage stats key project failed: {err}"),
            )
        })?;
        let caller_context = Self::current_claim_request_context();
        self.enforce_caller_scope(caller_context.as_ref(), &target_tenant, &target_project)?;
        let key_pred = if Uuid::parse_str(key_ref).is_ok() {
            format!("{key_id} = $1::UUID", key_id = m.q("key_id"))
        } else {
            format!(
                "{key_id} = (SELECT {ak_id} FROM {ak_rel} WHERE {ak_prefix} = $1 LIMIT 1)",
                key_id = m.q("key_id"),
                ak_id = ak.q("key_id"),
                ak_rel = ak.relation,
                ak_prefix = ak.q("key_prefix"),
            )
        };
        let from_secs = req.from.as_ref().map(|t| t.seconds);
        let to_secs = req.to.as_ref().map(|t| t.seconds);
        let sql = format!(
            "SELECT to_char(date_trunc('day', {req_at}), 'YYYY-MM-DD') AS day, \
                    COALESCE({status}, 0)::int AS status, \
                    COUNT(*)::bigint AS cnt, \
                    COUNT(*) FILTER (WHERE {rate_limited})::bigint AS rl, \
                    COALESCE(SUM({latency}), 0)::bigint AS lat_sum \
             FROM {rel} \
             WHERE {key_pred} \
               AND ($2::bigint IS NULL OR {req_at} >= to_timestamp($2)) \
               AND ($3::bigint IS NULL OR {req_at} <= to_timestamp($3)) \
             GROUP BY day, status \
             ORDER BY day",
            req_at = m.q("requested_at"),
            status = m.q("http_status"),
            rate_limited = m.q("rate_limited"),
            latency = m.q("latency_ms"),
            rel = m.relation,
            key_pred = key_pred,
        );
        let rows = sqlx::query(&sql)
            .bind(req.key_id.trim())
            .bind(from_secs)
            .bind(to_secs)
            .fetch_all(pool)
            .await
            .map_err(|err| {
                Self::internal_status(
                    "usage_stats_query",
                    format!("usage stats query failed: {err}"),
                )
            })?;

        // Fold (day, status) rows into one ApiKeyDailyStat per day.
        struct DayAgg {
            total: i64,
            rate_limited: i64,
            latency_sum: i64,
            status_counts: BTreeMap<String, i64>,
        }
        let mut by_day: BTreeMap<String, DayAgg> = BTreeMap::new();
        let mut overall_total: i64 = 0;
        for row in rows {
            let day: String = row.try_get("day").unwrap_or_default();
            let status: i32 = row.try_get("status").unwrap_or_default();
            let cnt: i64 = row.try_get("cnt").unwrap_or_default();
            let rl: i64 = row.try_get("rl").unwrap_or_default();
            let lat_sum: i64 = row.try_get("lat_sum").unwrap_or_default();
            overall_total += cnt;
            let agg = by_day.entry(day).or_insert(DayAgg {
                total: 0,
                rate_limited: 0,
                latency_sum: 0,
                status_counts: BTreeMap::new(),
            });
            agg.total += cnt;
            agg.rate_limited += rl;
            agg.latency_sum += lat_sum;
            if status != 0 {
                *agg.status_counts.entry(status.to_string()).or_insert(0) += cnt;
            }
        }
        let stats = by_day
            .into_iter()
            .map(|(date, agg)| apikey_pb::ApiKeyDailyStat {
                date,
                total_requests: agg.total,
                rate_limited_count: agg.rate_limited,
                avg_latency_ms: if agg.total > 0 {
                    agg.latency_sum as f64 / agg.total as f64
                } else {
                    0.0
                },
                status_counts: agg.status_counts.into_iter().collect(),
            })
            .collect();
        Ok(Response::new(apikey_pb::GetApiKeyUsageStatsResponse {
            stats,
            total_requests: overall_total,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::rate_limit_decision;
    use super::{ApiKeyRecord, api_key_to_pb};
    use std::collections::BTreeSet;

    /// The descriptor's `OUTPUT_VIEW_STORAGE_ONLY` field set for a proto message.
    fn storage_only_field_names(message_full_name: &str) -> BTreeSet<String> {
        const OUTPUT_VIEW_STORAGE_ONLY: i32 = 1;
        let manifest = crate::runtime::descriptor_manifest::descriptor_contract_manifest_static();
        let message = manifest
            .messages
            .iter()
            .find(|message| message.full_name == message_full_name)
            .unwrap_or_else(|| panic!("descriptor message {message_full_name} must exist"));
        message
            .fields
            .iter()
            .filter(|field| {
                field
                    .db_column_security
                    .as_ref()
                    .is_some_and(|security| security.output_view == OUTPUT_VIEW_STORAGE_ONLY)
            })
            .map(|field| field.name.clone())
            .collect()
    }

    // §7 (Phase 6): the outbound ApiKey mapper must structurally blank exactly the
    // descriptor's OUTPUT_VIEW_STORAGE_ONLY field set (key_hash), via the
    // build.rs-generated `redact_storage_only()` — not a hand-maintained list. If a
    // new ApiKey field is annotated STORAGE_ONLY, this asserts the mapper blanks it.
    #[test]
    fn api_key_mapper_blanks_descriptor_storage_only_fields() {
        let storage_only = storage_only_field_names("udb.core.apikey.entity.v1.ApiKey");
        assert_eq!(
            storage_only,
            ["key_hash"].into_iter().map(str::to_string).collect()
        );

        let rec = ApiKeyRecord {
            key_prefix: "udbk_abc123".to_string(),
            key_hash: "hmac-sha256:deadbeef".to_string(),
            principal_id: "svc-1".to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            scopes: vec!["data:read".to_string()],
            created_at_unix: 1,
            ..Default::default()
        };
        let pb = api_key_to_pb(&rec, 100);
        for field in storage_only {
            match field.as_str() {
                "key_hash" => assert!(
                    pb.key_hash.is_empty(),
                    "descriptor storage-only ApiKey field key_hash must be blanked by the mapper"
                ),
                other => panic!("ApiKey mapper has no storage-only assertion for {other}"),
            }
        }
    }

    // A provisioner reconciles keys by the exact name it created them with, so the
    // read API must echo the STORED name rather than the generated key prefix —
    // otherwise reconcile never matches, create is retried, and the duplicate-name
    // precondition deadlocks provisioning.
    #[test]
    fn api_key_mapper_returns_stored_name_and_description() {
        let rec = ApiKeyRecord {
            key_prefix: "udbk_abc123".to_string(),
            name: "billing-projector".to_string(),
            description: "issues invoices".to_string(),
            ..Default::default()
        };
        let pb = api_key_to_pb(&rec, 100);
        assert_eq!(pb.name, "billing-projector");
        assert_eq!(pb.description, "issues invoices");
        assert_eq!(pb.key_prefix, "udbk_abc123");
    }

    // Legacy rows persisted before names were captured must still round-trip a
    // non-empty identifier instead of reconciling against an empty string.
    #[test]
    fn api_key_mapper_falls_back_to_prefix_when_name_blank() {
        let rec = ApiKeyRecord {
            key_prefix: "udbk_abc123".to_string(),
            name: "   ".to_string(),
            ..Default::default()
        };
        assert_eq!(api_key_to_pb(&rec, 100).name, "udbk_abc123");
    }

    #[test]
    fn disabled_limit_is_never_rate_limited() {
        // limit <= 0 means unlimited regardless of count or mode.
        assert!(!rate_limit_decision(Ok(1_000_000), 0, true));
        assert!(!rate_limit_decision(Err(()), 0, true));
        assert!(!rate_limit_decision(Err(()), -1, true));
    }

    #[test]
    fn normal_counting_path_unaffected_by_mode() {
        // Under the limit → allowed; at/over the limit → limited, in both modes.
        assert!(!rate_limit_decision(Ok(59), 60, false));
        assert!(!rate_limit_decision(Ok(59), 60, true));
        assert!(rate_limit_decision(Ok(60), 60, false));
        assert!(rate_limit_decision(Ok(61), 60, true));
    }

    #[test]
    fn store_error_fails_closed_when_hardened() {
        // Fail-closed mode: a rate-limit-store error denies the request.
        assert!(rate_limit_decision(Err(()), 60, true));
    }

    #[test]
    fn store_error_fails_open_when_not_hardened() {
        // Dev/test default: a rate-limit-store error degrades to "not limited".
        assert!(!rate_limit_decision(Err(()), 60, false));
    }

    use super::ApiKeyServiceImpl;
    use crate::proto::udb::core::apikey::entity::v1 as apikey_entity_pb;
    use crate::proto::udb::core::apikey::services::v1 as apikey_pb;
    use crate::proto::udb::core::apikey::services::v1::api_key_service_server::ApiKeyService;
    use crate::proto::udb::core::common::v1 as common_pb;
    use crate::proto::udb::core::common::v1::{RequestContext, TenantContext};
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::authn::{ApiKeyStore, AuthnConfig};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use std::sync::Arc;
    use tonic::{Code, Request, Status};

    #[derive(Clone, Default)]
    struct MemoryApiKeyStore {
        records: Vec<ApiKeyRecord>,
    }

    #[async_trait::async_trait]
    impl ApiKeyStore for MemoryApiKeyStore {
        async fn put(&self, _record: ApiKeyRecord) -> Result<(), String> {
            Ok(())
        }

        async fn get_by_prefix(&self, key_prefix: &str) -> Result<Option<ApiKeyRecord>, String> {
            Ok(self
                .records
                .iter()
                .find(|rec| rec.key_prefix == key_prefix)
                .cloned())
        }

        async fn list_for_principal(
            &self,
            principal_id: &str,
            _active_only: bool,
            _now_unix: u64,
        ) -> Result<Vec<ApiKeyRecord>, String> {
            Ok(self
                .records
                .iter()
                .filter(|rec| {
                    rec.principal_id == principal_id || rec.service_identity == principal_id
                })
                .cloned()
                .collect())
        }

        async fn revoke(&self, _key_prefix: &str, _now_unix: u64) -> Result<bool, String> {
            Ok(false)
        }
    }

    fn ctx(tenant: &str, roles: &[&str]) -> RequestContext {
        ctx_project(tenant, "", roles)
    }

    fn ctx_project(tenant: &str, project: &str, roles: &[&str]) -> RequestContext {
        RequestContext {
            tenant: if tenant.is_empty() && project.is_empty() {
                None
            } else {
                Some(TenantContext {
                    tenant_id: tenant.to_string(),
                    project_id: project.to_string(),
                    ..Default::default()
                })
            },
            roles: roles.iter().map(|r| r.to_string()).collect(),
            ..Default::default()
        }
    }

    fn svc() -> ApiKeyServiceImpl {
        ApiKeyServiceImpl::new(AuthnConfig::default())
    }

    fn svc_with_records(records: Vec<ApiKeyRecord>) -> ApiKeyServiceImpl {
        ApiKeyServiceImpl::with_store(
            AuthnConfig::default(),
            Arc::new(MemoryApiKeyStore { records }),
        )
    }

    fn svc_with_config_records(
        config: AuthnConfig,
        records: Vec<ApiKeyRecord>,
    ) -> ApiKeyServiceImpl {
        ApiKeyServiceImpl::with_store(config, Arc::new(MemoryApiKeyStore { records }))
    }

    fn config_with_api_key_hash_secret() -> AuthnConfig {
        AuthnConfig {
            api_key_hash_secret: "test-hash-secret".to_string(),
            ..Default::default()
        }
    }

    fn api_key_rec(prefix: &str, owner: &str, tenant: &str) -> ApiKeyRecord {
        api_key_rec_in_project(prefix, owner, tenant, "")
    }

    fn api_key_rec_in_project(
        prefix: &str,
        owner: &str,
        tenant: &str,
        project: &str,
    ) -> ApiKeyRecord {
        ApiKeyRecord {
            key_prefix: prefix.to_string(),
            key_hash: "hmac-sha256:test".to_string(),
            principal_id: owner.to_string(),
            tenant_id: tenant.to_string(),
            project_id: project.to_string(),
            scopes: vec!["data:read".to_string()],
            created_at_unix: 1,
            ..Default::default()
        }
    }

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_validation_field(status: &Status, field: &str, description: &str) {
        assert_eq!(status.code(), Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_capability_detail(
        status: &Status,
        operation: &str,
        capability_required: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "api_key");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability_required);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_schema_detail(status: &Status, operation: &str, schema_code: &str, message: &str) {
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "api_key");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, schema_code);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_policy_detail(
        status: &Status,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), Code::PermissionDenied);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "api_key");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn api_key_internal_status_carries_typed_detail() {
        let status = ApiKeyServiceImpl::internal_status(
            "validate_api_key_store",
            "validate API key failed: store closed",
        );
        assert_internal_detail(
            &status,
            "validate_api_key_store",
            "validate API key failed: store closed",
        );
    }

    #[tokio::test]
    async fn create_api_key_missing_hash_secret_carries_capability_detail() {
        let svc = ApiKeyServiceImpl::with_store(
            AuthnConfig::default(),
            Arc::new(MemoryApiKeyStore::default()),
        );

        let err = svc
            .create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
                owner_id: "owner-1".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing API-key hash secret must fail before persistence");

        assert_capability_detail(
            &err,
            "create_hashing",
            "api_key_hash_secret",
            "API key hashing requires UDB_SESSION_HASH_SECRET",
        );
    }

    #[tokio::test]
    async fn rotate_api_key_missing_hash_secret_carries_capability_detail() {
        let svc = svc_with_records(Vec::new());

        let err = svc
            .rotate_api_key(Request::new(apikey_pb::RotateApiKeyRequest {
                key_id: "key-1".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing API-key hash secret must fail before key lookup");

        assert_capability_detail(
            &err,
            "rotate_hashing",
            "api_key_hash_secret",
            "API key hashing requires UDB_SESSION_HASH_SECRET",
        );
    }

    #[tokio::test]
    async fn emergency_revoke_missing_postgres_carries_capability_detail() {
        let svc = svc();
        let claim = crate::runtime::service::method_security::test_claim_context(
            "admin-1",
            "acme",
            "",
            &["auth:admin"],
            &["admin"],
        );

        let err =
            crate::runtime::service::method_security::scope_claim_context_for_test(claim, async {
                svc.emergency_revoke_api_keys(Request::new(
                    apikey_pb::EmergencyRevokeApiKeysRequest {
                        owner_id: "owner-1".to_string(),
                        ..Default::default()
                    },
                ))
                .await
            })
            .await
            .expect_err("missing Postgres backend must fail after selector/tenant validation");

        assert_capability_detail(
            &err,
            "emergency_revoke",
            "postgres_backend",
            "emergency revoke requires a Postgres backend",
        );
    }

    #[tokio::test]
    async fn get_api_key_usage_stats_missing_postgres_carries_capability_detail() {
        let svc = svc();

        let err = svc
            .get_api_key_usage_stats(Request::new(apikey_pb::GetApiKeyUsageStatsRequest {
                key_id: "key-1".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing Postgres backend must fail after key_id validation");

        assert_capability_detail(
            &err,
            "usage_stats",
            "postgres_backend",
            "api-key usage stats require a Postgres backend",
        );
    }

    #[tokio::test]
    async fn create_api_key_missing_owner_id_carries_field_violation() {
        let svc = ApiKeyServiceImpl::with_store(
            config_with_api_key_hash_secret(),
            Arc::new(MemoryApiKeyStore::default()),
        );

        let err = svc
            .create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
                owner_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing owner_id must fail");

        assert_eq!(err.message(), "owner_id is required");
        assert_validation_field(&err, "owner_id", "must be a non-empty API key owner id");
    }

    #[tokio::test]
    async fn list_api_keys_missing_owner_id_carries_field_violation() {
        let svc = svc_with_records(Vec::new());

        let err = svc
            .list_api_keys(Request::new(apikey_pb::ListApiKeysRequest {
                owner_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing owner_id must fail");

        assert_eq!(err.message(), "owner_id is required");
        assert_validation_field(&err, "owner_id", "must be a non-empty API key owner id");
    }

    #[tokio::test]
    async fn rotate_api_key_missing_key_id_carries_field_violation() {
        let svc = ApiKeyServiceImpl::with_store(
            config_with_api_key_hash_secret(),
            Arc::new(MemoryApiKeyStore::default()),
        );

        let err = svc
            .rotate_api_key(Request::new(apikey_pb::RotateApiKeyRequest {
                key_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing key_id must fail");

        assert_eq!(err.message(), "key_id is required");
        assert_validation_field(&err, "key_id", "must be a non-empty API key id");
    }

    #[tokio::test]
    async fn get_api_key_usage_stats_missing_key_id_carries_field_violation() {
        let svc = svc();

        let err = svc
            .get_api_key_usage_stats(Request::new(apikey_pb::GetApiKeyUsageStatsRequest {
                key_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing key_id must fail before Postgres availability");

        assert_eq!(err.message(), "key_id is required");
        assert_validation_field(&err, "key_id", "must be a non-empty API key id");
    }

    #[tokio::test]
    async fn emergency_revoke_missing_selector_carries_field_violation() {
        let svc = svc();

        let err = svc
            .emergency_revoke_api_keys(Request::new(
                apikey_pb::EmergencyRevokeApiKeysRequest::default(),
            ))
            .await
            .expect_err("selectorless emergency revoke must fail before Postgres availability");

        assert_eq!(
            err.message(),
            "at least one selector is required (prefix/owner/tenant/project/scope/created_before)"
        );
        assert_validation_field(
            &err,
            "selectors",
            "must include at least one of key_prefix, owner_id, tenant_id, project_id, scope, or created_before",
        );
    }

    #[tokio::test]
    async fn emergency_revoke_missing_tenant_context_carries_field_violation() {
        let svc = svc();

        let err = svc
            .emergency_revoke_api_keys(Request::new(apikey_pb::EmergencyRevokeApiKeysRequest {
                owner_id: "owner-1".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("tenantless emergency revoke must fail before Postgres availability");

        assert_eq!(
            err.message(),
            "emergency revoke requires tenant_id or tenant context"
        );
        assert_validation_field(
            &err,
            "tenant_id",
            "must be supplied directly or by caller tenant context",
        );
    }

    #[tokio::test]
    async fn get_api_key_enforces_claim_tenant_after_resolve() {
        let svc = svc_with_records(vec![api_key_rec("key-acme", "owner-1", "acme")]);

        let other_claim = crate::runtime::service::method_security::test_claim_context(
            "user-1",
            "other",
            "",
            &["data:read"],
            &[],
        );
        let err = crate::runtime::service::method_security::scope_claim_context_for_test(
            other_claim,
            async {
                svc.get_api_key(Request::new(apikey_pb::GetApiKeyRequest {
                    key_id: "key-acme".to_string(),
                }))
                .await
            },
        )
        .await
        .expect_err("cross-tenant GetApiKey must be denied");
        assert_policy_detail(
            &err,
            "api_key_tenant_scope",
            "caller_tenant_mismatch",
            "api key belongs to a different tenant",
        );

        let acme_claim = crate::runtime::service::method_security::test_claim_context(
            "user-1",
            "acme",
            "",
            &["data:read"],
            &[],
        );
        let got = crate::runtime::service::method_security::scope_claim_context_for_test(
            acme_claim,
            async {
                svc.get_api_key(Request::new(apikey_pb::GetApiKeyRequest {
                    key_id: "key-acme".to_string(),
                }))
                .await
            },
        )
        .await
        .expect("same-tenant GetApiKey must be allowed")
        .into_inner()
        .key
        .expect("resolved API key");
        assert_eq!(got.key_id, "key-acme");
        assert_eq!(got.tenant_id, "acme");
    }

    #[tokio::test]
    async fn get_api_key_enforces_claim_project_after_resolve() {
        let svc = svc_with_records(vec![api_key_rec_in_project(
            "key-billing",
            "owner-1",
            "acme",
            "billing",
        )]);
        let other_project = crate::runtime::service::method_security::test_claim_context(
            "user-1",
            "acme",
            "support",
            &["data:read"],
            &[],
        );

        let err = crate::runtime::service::method_security::scope_claim_context_for_test(
            other_project,
            async {
                svc.get_api_key(Request::new(apikey_pb::GetApiKeyRequest {
                    key_id: "key-billing".to_string(),
                }))
                .await
            },
        )
        .await
        .expect_err("cross-project GetApiKey must be denied");
        assert_policy_detail(
            &err,
            "api_key_project_scope",
            "caller_project_mismatch",
            "API key belongs to a different project",
        );
    }

    #[tokio::test]
    async fn get_api_key_miss_returns_not_found() {
        let svc = svc_with_records(Vec::new());

        let err = svc
            .get_api_key(Request::new(apikey_pb::GetApiKeyRequest {
                key_id: "missing-key".to_string(),
            }))
            .await
            .expect_err("missing API key must not be masked as OK+null");

        assert_schema_detail(
            &err,
            "get_api_key",
            "api_key_not_found",
            "api key not found",
        );
    }

    #[tokio::test]
    async fn list_api_keys_filters_to_claim_tenant_before_paging() {
        let svc = svc_with_records(vec![
            api_key_rec("key-acme-1", "owner-1", "acme"),
            api_key_rec("key-other", "owner-1", "other"),
            api_key_rec("key-acme-2", "owner-1", "acme"),
        ]);
        let acme_claim = crate::runtime::service::method_security::test_claim_context(
            "user-1",
            "acme",
            "",
            &["data:read"],
            &[],
        );

        let listed = crate::runtime::service::method_security::scope_claim_context_for_test(
            acme_claim,
            async {
                svc.list_api_keys(Request::new(apikey_pb::ListApiKeysRequest {
                    owner_id: "owner-1".to_string(),
                    status: apikey_entity_pb::ApiKeyStatus::Active as i32,
                    page: Some(common_pb::PageRequest {
                        page: 2,
                        page_size: 1,
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
                .await
            },
        )
        .await
        .expect("same-tenant ListApiKeys must be allowed")
        .into_inner();

        assert_eq!(listed.keys.len(), 1);
        assert_eq!(listed.keys[0].key_id, "key-acme-2");
        let page = listed.page.expect("page metadata");
        assert_eq!(page.total_items, 2);
        assert_eq!(page.total_pages, 2);
    }

    #[tokio::test]
    async fn list_api_keys_filters_to_claim_project_before_paging() {
        let svc = svc_with_records(vec![
            api_key_rec_in_project("key-a-1", "owner-1", "acme", "project-a"),
            api_key_rec_in_project("key-b", "owner-1", "acme", "project-b"),
            api_key_rec_in_project("key-a-2", "owner-1", "acme", "project-a"),
            api_key_rec("key-tenant-wide", "owner-1", "acme"),
        ]);
        let project_claim = crate::runtime::service::method_security::test_claim_context(
            "user-1",
            "acme",
            "project-a",
            &["data:read"],
            &[],
        );

        let listed = crate::runtime::service::method_security::scope_claim_context_for_test(
            project_claim,
            async {
                svc.list_api_keys(Request::new(apikey_pb::ListApiKeysRequest {
                    owner_id: "owner-1".to_string(),
                    status: apikey_entity_pb::ApiKeyStatus::Active as i32,
                    page: Some(common_pb::PageRequest {
                        page: 2,
                        page_size: 1,
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
                .await
            },
        )
        .await
        .expect("same-project ListApiKeys must be allowed")
        .into_inner();

        assert_eq!(listed.keys.len(), 1);
        assert_eq!(listed.keys[0].key_id, "key-a-2");
        let page = listed.page.expect("page metadata");
        assert_eq!(page.total_items, 2);
        assert_eq!(page.total_pages, 2);
    }

    #[tokio::test]
    async fn api_key_mutations_deny_cross_project_before_store_access() {
        let record = api_key_rec_in_project("key-b", "owner-1", "acme", "project-b");
        let assert_project_denied = |err: &Status| {
            assert_policy_detail(
                err,
                "api_key_project_scope",
                "caller_project_mismatch",
                "API key belongs to a different project",
            );
        };

        let svc = svc_with_records(vec![record.clone()]);
        let claim = crate::runtime::service::method_security::test_claim_context(
            "user-1",
            "acme",
            "project-a",
            &["data:write"],
            &[],
        );
        let err =
            crate::runtime::service::method_security::scope_claim_context_for_test(claim, async {
                svc.update_api_key(Request::new(apikey_pb::UpdateApiKeyRequest {
                    key_id: "key-b".to_string(),
                    ..Default::default()
                }))
                .await
            })
            .await
            .expect_err("cross-project UpdateApiKey must be denied");
        assert_project_denied(&err);

        let svc = svc_with_records(vec![record.clone()]);
        let claim = crate::runtime::service::method_security::test_claim_context(
            "user-1",
            "acme",
            "project-a",
            &["data:write"],
            &[],
        );
        let err =
            crate::runtime::service::method_security::scope_claim_context_for_test(claim, async {
                svc.revoke_api_key(Request::new(apikey_pb::RevokeApiKeyRequest {
                    key_id: "key-b".to_string(),
                    ..Default::default()
                }))
                .await
            })
            .await
            .expect_err("cross-project RevokeApiKey must be denied");
        assert_project_denied(&err);

        let svc = svc_with_config_records(config_with_api_key_hash_secret(), vec![record]);
        let claim = crate::runtime::service::method_security::test_claim_context(
            "user-1",
            "acme",
            "project-a",
            &["data:write"],
            &[],
        );
        let err =
            crate::runtime::service::method_security::scope_claim_context_for_test(claim, async {
                svc.rotate_api_key(Request::new(apikey_pb::RotateApiKeyRequest {
                    key_id: "key-b".to_string(),
                    ..Default::default()
                }))
                .await
            })
            .await
            .expect_err("cross-project RotateApiKey must be denied");
        assert_project_denied(&err);
    }

    #[tokio::test]
    async fn emergency_revoke_denies_cross_project_before_postgres_access() {
        let svc = svc();
        let claim = crate::runtime::service::method_security::test_claim_context(
            "user-1",
            "acme",
            "project-a",
            &["udb:auth:admin"],
            &[],
        );
        let err =
            crate::runtime::service::method_security::scope_claim_context_for_test(claim, async {
                svc.emergency_revoke_api_keys(Request::new(
                    apikey_pb::EmergencyRevokeApiKeysRequest {
                        tenant_id: "acme".to_string(),
                        project_id: "project-b".to_string(),
                        ..Default::default()
                    },
                ))
                .await
            })
            .await
            .expect_err("cross-project emergency revoke must fail before Postgres access");
        assert_policy_detail(
            &err,
            "api_key_project_scope",
            "caller_project_mismatch",
            "API key belongs to a different project",
        );
    }

    #[tokio::test]
    async fn list_api_keys_cross_tenant_admin_scope_sees_all_tenants() {
        let svc = svc_with_records(vec![
            api_key_rec("key-acme", "owner-1", "acme"),
            api_key_rec("key-other", "owner-1", "other"),
        ]);
        let admin_claim = crate::runtime::service::method_security::test_claim_context(
            "admin-1",
            "",
            "",
            &["udb:admin"],
            &[],
        );

        let listed = crate::runtime::service::method_security::scope_claim_context_for_test(
            admin_claim,
            async {
                svc.list_api_keys(Request::new(apikey_pb::ListApiKeysRequest {
                    owner_id: "owner-1".to_string(),
                    status: apikey_entity_pb::ApiKeyStatus::Active as i32,
                    ..Default::default()
                }))
                .await
            },
        )
        .await
        .expect("cross-tenant admin ListApiKeys must be allowed")
        .into_inner();

        assert_eq!(listed.keys.len(), 2);
        assert_eq!(listed.page.expect("page metadata").total_items, 2);
    }

    // I2.5 fail-closed: a tenant-scoped key with an absent/empty caller tenant is
    // DENIED (the prior code returned Ok and let the caller bypass isolation by
    // simply omitting context.tenant).
    #[test]
    fn enforce_caller_tenant_denies_empty_caller_for_tenant_scoped_key() {
        let svc = svc();
        // No context at all.
        let err = svc
            .enforce_caller_tenant(None, "acme")
            .expect_err("tenant-scoped key requires caller context");
        assert_policy_detail(
            &err,
            "api_key_tenant_scope",
            "caller_tenant_context_required",
            "api key is tenant-scoped; caller tenant context is required",
        );
        // Context present but no tenant set.
        let err = svc
            .enforce_caller_tenant(Some(&ctx("", &[])), "acme")
            .expect_err("empty caller tenant must be denied");
        assert_policy_detail(
            &err,
            "api_key_tenant_scope",
            "caller_tenant_context_required",
            "api key is tenant-scoped; caller tenant context is required",
        );
        // Context with a tenant that does not match.
        let err = svc
            .enforce_caller_tenant(Some(&ctx("other", &[])), "acme")
            .expect_err("cross-tenant caller must be denied");
        assert_policy_detail(
            &err,
            "api_key_tenant_scope",
            "caller_tenant_mismatch",
            "api key belongs to a different tenant",
        );
    }

    #[test]
    fn read_tenant_filter_requires_tenant_scope_with_policy_detail() {
        let err = ApiKeyServiceImpl::read_tenant_filter(Some(&ctx("", &[])))
            .expect_err("tenantless non-admin reads must be denied");
        assert_policy_detail(
            &err,
            "api_key_read_scope",
            "tenant_scoped_bearer_required",
            "api key read requires a tenant-scoped bearer token or a cross-tenant admin role",
        );
    }

    #[test]
    fn api_key_not_found_statuses_carry_schema_detail() {
        for operation in [
            "get_api_key",
            "update_api_key",
            "revoke_api_key",
            "rotate_api_key",
        ] {
            assert_schema_detail(
                &ApiKeyServiceImpl::api_key_not_found_status(operation),
                operation,
                "api_key_not_found",
                "api key not found",
            );
        }
    }

    // Same-tenant caller is allowed; a global (empty-tenant) key is accepted from
    // any caller (broker authz already gates the admin scope).
    #[test]
    fn enforce_caller_tenant_allows_same_tenant_and_global_key() {
        let svc = svc();
        assert!(
            svc.enforce_caller_tenant(Some(&ctx("acme", &[])), "acme")
                .is_ok()
        );
        // Global/admin key: empty record tenant → allowed regardless of caller.
        assert!(svc.enforce_caller_tenant(None, "").is_ok());
        assert!(
            svc.enforce_caller_tenant(Some(&ctx("acme", &[])), "")
                .is_ok()
        );
    }

    // A genuine cross-tenant (platform) admin may reach a tenant-scoped key even
    // without a matching tenant context — the only permitted escape hatch.
    #[test]
    fn enforce_caller_tenant_allows_cross_tenant_admin() {
        let svc = svc();
        for role in ["platform_admin", "udb:platform_admin", "SUPERADMIN"] {
            assert!(
                svc.enforce_caller_tenant(Some(&ctx("", &[role])), "acme")
                    .is_ok(),
                "role {role} must satisfy the cross-tenant admin escape hatch"
            );
        }
        // A non-admin role is NOT an escape hatch.
        assert!(
            svc.enforce_caller_tenant(Some(&ctx("", &["viewer"])), "acme")
                .is_err()
        );
    }

    #[test]
    fn enforce_caller_scope_separates_projects_and_preserves_tenant_admin() {
        let svc = svc();
        assert!(
            svc.enforce_caller_scope(
                Some(&ctx_project("acme", "project-a", &[])),
                "acme",
                "project-a",
            )
            .is_ok()
        );
        assert!(
            svc.enforce_caller_scope(
                Some(&ctx_project("acme", "project-a", &[])),
                "acme",
                "project-b",
            )
            .is_err()
        );
        assert!(
            svc.enforce_caller_scope(Some(&ctx_project("acme", "project-a", &[])), "acme", "",)
                .is_err(),
            "project-bound callers must not manage tenant-wide keys"
        );
        assert!(
            svc.enforce_caller_scope(Some(&ctx("acme", &[])), "acme", "project-b")
                .is_ok(),
            "tenant-bound callers intentionally retain cross-project administration"
        );
    }

    #[test]
    fn cross_tenant_admin_role_detection_is_case_insensitive_and_default_deny() {
        assert!(ApiKeyServiceImpl::caller_is_cross_tenant_admin(Some(&ctx(
            "",
            &["Platform_Admin"]
        ))));
        assert!(!ApiKeyServiceImpl::caller_is_cross_tenant_admin(None));
        assert!(!ApiKeyServiceImpl::caller_is_cross_tenant_admin(Some(
            &ctx("acme", &["member"])
        )));
    }
}
