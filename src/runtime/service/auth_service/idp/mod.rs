//! `IdentityProviderService` — enterprise identity-provider lifecycle, SAML 2.0
//! web SSO, SCIM 2.0 provisioning, JIT user provisioning, and external-identity
//! linking (Phase J of the enterprise auth upgrade).
//!
//! Structure mirrors the other native services (`tenant_service`, `apikey`):
//! proto is the source of truth, all SQL goes through [`native_catalog`], no
//! in-memory state, every RPC has a real handler that fails closed without a
//! Postgres pool. Submodules:
//!   * [`mapping`] — pure claim/group/JIT/assurance logic (unit-tested).
//!   * [`oidc`]    — per-tenant provider registry + bounded-TTL JWKS cache.
//!   * [`saml`]    — SAML metadata import, AuthnRequest, assertion validation.
//!   * [`scim`]    — SCIM 2.0 JSON parsing + PATCH application.
//!   * [`store`]   — Postgres persistence over the `udb_idp.*` tables.
//!   * [`events`]  — IdP Kafka topic constants + emit helper.

mod c14n;
mod events;
mod mapping;
mod oidc;
mod saml;
mod saml_http;
mod scim;
mod scim_http;
mod store;

pub(crate) use scim_http::spawn_from_env_with_shutdown as spawn_scim_http_from_env;
// W9: leader wires spawn_saml_http_from_env into service/mod.rs serve()
pub(crate) use saml_http::spawn_from_env_with_shutdown as spawn_saml_http_from_env;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use serde_json::{Value, json};
use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::proto::udb::core::idp::entity::v1 as idp_entity_pb;
use crate::proto::udb::core::idp::services::v1 as idp_pb;
use idp_pb::identity_provider_service_server::IdentityProviderService;

pub use crate::proto::udb::core::idp::services::v1::identity_provider_service_server::IdentityProviderServiceServer;

use super::DataBrokerService;
use super::events::{AuthEventSink, noop_sink};
use super::mappings::{bounded_page_response, bounded_page_window, timestamp_from_unix};
#[cfg(any(feature = "oidc", test))]
use crate::runtime::authz::Principal;
use crate::runtime::core::DataBrokerRuntime;

use mapping::{
    AccountLinkDecision, JitDecision, JitPolicy, MappedClaims, apply_claim_mapping,
    evaluate_account_linking, evaluate_jit, map_groups_to_roles,
};
use store::{ExternalIdentityRow, ProviderRow};

fn idp_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("identity_provider", operation, message)
}

/// Resolved OIDC provider config for the authn OIDC verify path (J2.1: move
/// OIDC config out of process globals). Empty fields mean "fall back to the
/// `UDB_OIDC_*` development defaults".
#[derive(Debug, Clone, Default)]
#[cfg_attr(not(feature = "oidc"), allow(dead_code))]
pub struct ResolvedOidcProvider {
    pub issuer: String,
    pub jwks_url: String,
    pub client_ids: Vec<String>,
    pub audiences: Vec<String>,
    pub enabled: bool,
    pub claim_mapping_json: String,
    pub group_mapping_json: String,
}

/// Build the Authn principal from already-verified OIDC claims using the same
/// provider claim/group mapping rules as the rest of the IdP plane.
#[cfg(any(feature = "oidc", test))]
pub(super) fn oidc_principal_from_verified_claims(
    provider_id: &str,
    fallback_subject: &str,
    tenant_id: &str,
    project_id: &str,
    scopes: Vec<String>,
    claim_mapping_json: &str,
    group_mapping_json: &str,
    claims: &Value,
) -> Principal {
    let mapped = apply_claim_mapping(claim_mapping_json, claims);
    let subject = if mapped.subject.trim().is_empty() {
        fallback_subject.trim().to_string()
    } else {
        mapped.subject
    };
    let (roles, _) = map_groups_to_roles(group_mapping_json, &mapped.groups);
    Principal {
        principal_id: format!("{provider_id}:{subject}"),
        subject: subject.clone(),
        user_id: subject,
        service_identity: String::new(),
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        scopes,
        roles,
        provider_id: provider_id.to_string(),
        auth_method: "oidc".to_string(),
    }
}

/// Resolve a tenant's OIDC provider by id from the registry. Returns `Ok(None)`
/// when no pool/provider is configured so the caller keeps its env defaults.
/// Used by `authn::authenticate_oidc_token` to replace process-global lookups.
#[cfg_attr(not(feature = "oidc"), allow(dead_code))]
pub(crate) async fn resolve_oidc_provider(
    pool: Option<&PgPool>,
    tenant_id: &str,
    provider_id: &str,
) -> Result<Option<ResolvedOidcProvider>, Status> {
    let Some(pool) = pool else {
        return Ok(None);
    };
    if tenant_id.trim().is_empty() || provider_id.trim().is_empty() {
        return Ok(None);
    }
    // provider_id may be a UUID (registry id) or a logical name; the registry is
    // keyed by UUID, so a non-UUID id simply yields None (env fallback).
    let row = match store::get_provider(pool, tenant_id, provider_id).await {
        Ok(opt) => opt,
        Err(_) => return Ok(None),
    };
    Ok(row.map(|p| ResolvedOidcProvider {
        issuer: p.issuer,
        jwks_url: p.jwks_url,
        client_ids: json_array_to_vec(&p.client_ids_json),
        audiences: json_array_to_vec(&p.audiences_json),
        enabled: p.enabled,
        claim_mapping_json: p.claim_mapping_json,
        group_mapping_json: p.group_mapping_json,
    }))
}

/// Default SAML clock-skew tolerance (seconds). `UDB_SAML_CLOCK_SKEW_SECS`.
fn saml_clock_skew_secs() -> i64 {
    std::env::var("UDB_SAML_CLOCK_SKEW_SECS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v >= 0)
        .unwrap_or(120)
}

fn now_unix_i64() -> i64 {
    chrono::Utc::now().timestamp()
}

fn idp_invalid_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, fields)
}

fn idp_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "identity_provider",
        operation,
        capability_required,
        message,
    )
}

fn idp_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message)
}

fn idp_permission_policy_status(
    operation: impl Into<String>,
    policy_decision_id: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        operation,
        policy_decision_id,
        message,
    )
}

fn idp_schema_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "identity_provider",
        operation,
        schema_code,
        message,
    )
}

fn idp_schema_already_exists_status(
    operation: &'static str,
    schema_code: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::AlreadyExists,
        "identity_provider",
        operation,
        schema_code,
        message,
    )
}

fn idp_provider_not_found_status() -> Status {
    idp_schema_not_found_status(
        "provider_lookup",
        "identity_provider_not_found",
        "identity provider not found for this tenant",
    )
}

fn idp_scim_user_not_found_status() -> Status {
    idp_schema_not_found_status(
        "scim_user_lookup",
        "scim_user_not_found",
        "SCIM user not found",
    )
}

fn idp_scim_group_not_found_status() -> Status {
    idp_schema_not_found_status(
        "scim_group_mapping_lookup",
        "scim_group_not_found",
        "SCIM group not found in group mapping",
    )
}

fn idp_saml_replay_rejected_status() -> Status {
    idp_permission_policy_status(
        "saml_acs",
        "saml_assertion_replay",
        "SAML assertion has already been consumed (replay rejected)",
    )
}

fn idp_jit_provisioning_rejected_status(reason: impl std::fmt::Display) -> Status {
    idp_permission_policy_status(
        "idp_jit_provisioning",
        "jit_policy_rejected",
        format!("JIT provisioning rejected: {reason}"),
    )
}

fn idp_required_field_status(field: &'static str, description: &'static str) -> Status {
    idp_invalid_fields(format!("{field} is required"), [(field, description)])
}

fn idp_tenant_id_required_status() -> Status {
    idp_required_field_status("tenant_id", "must be a non-empty tenant id")
}

fn idp_display_name_required_status() -> Status {
    idp_required_field_status("display_name", "must be a non-empty display name")
}

fn idp_claims_json_invalid_status(err: impl std::fmt::Display) -> Status {
    idp_invalid_fields(
        format!("claims_json is not valid JSON: {err}"),
        [("claims_json", "must decode as a JSON object of IdP claims")],
    )
}

fn idp_subject_user_required_status() -> Status {
    idp_invalid_fields(
        "subject and user_id are required",
        [
            ("subject", "must be a non-empty external subject"),
            ("user_id", "must be a non-empty UDB user id"),
        ],
    )
}

fn idp_claims_subject_required_status() -> Status {
    idp_invalid_fields(
        "claims have no resolvable subject",
        [(
            "claims_json",
            "must map to a non-empty external subject claim",
        )],
    )
}

fn idp_saml_metadata_required_status() -> Status {
    idp_invalid_fields(
        "metadata_xml is required (or set the provider's saml_metadata_url)",
        [(
            "metadata_xml",
            "must contain SAML metadata XML when the provider has no metadata URL",
        )],
    )
}

fn idp_saml_metadata_invalid_status(err: impl std::fmt::Display) -> Status {
    idp_invalid_fields(
        format!("invalid SAML metadata: {err}"),
        [("metadata_xml", "must decode as valid SAML metadata XML")],
    )
}

fn idp_metadata_fetch_failed_status(err: impl std::fmt::Display) -> Status {
    idp_capability_status(
        "metadata_fetch",
        "saml_metadata_url",
        format!("metadata fetch failed: {err}"),
    )
}

fn idp_saml_sso_url_missing_status() -> Status {
    idp_capability_status(
        "saml_login",
        "saml_sso_url",
        "provider has no SAML SSO URL; import metadata first",
    )
}

fn idp_provider_disabled_status(display_name: &str) -> Status {
    idp_capability_status(
        "provider_login",
        "provider_enabled",
        format!("identity provider '{display_name}' is disabled"),
    )
}

fn idp_provider_disabled_static_status() -> Status {
    idp_capability_status(
        "provider_login",
        "provider_enabled",
        "identity provider is disabled",
    )
}

fn idp_scim_user_json_invalid_status(err: impl Into<String>) -> Status {
    idp_invalid_fields(
        err.into(),
        [(
            "scim_user_json",
            "must decode as a valid SCIM User resource",
        )],
    )
}

fn idp_scim_patch_invalid_status(err: impl Into<String>) -> Status {
    idp_invalid_fields(
        err.into(),
        [(
            "operations",
            "must contain supported SCIM PATCH operations for a User resource",
        )],
    )
}

fn idp_scim_group_json_invalid_status(err: impl Into<String>) -> Status {
    idp_invalid_fields(
        err.into(),
        [(
            "scim_group_json",
            "must decode as a valid SCIM Group resource",
        )],
    )
}

fn idp_scim_group_mapping_required_status() -> Status {
    idp_policy_status(
        "scim_create_group",
        "scim_group_mapping_required",
        "group must match a configured group mapping key; \
         groups are mapping-driven and not persisted",
    )
}

fn idp_account_linking_explicit_required_status() -> Status {
    idp_policy_status(
        "idp_account_linking",
        "explicit_link_required",
        "an account with this email exists; explicit account linking is required",
    )
}

/// Postgres-backed `IdentityProviderService` handler.
pub struct IdentityProviderServiceImpl {
    pg_pool: Option<PgPool>,
    runtime: Arc<DataBrokerRuntime>,
    event_sink: Arc<dyn AuthEventSink>,
    jwks_cache: oidc::OidcJwksCache,
    /// Records IdP discovery/JWKS and SCIM failures (Phase L3 task6). Defaults to
    /// a no-op; production wires the broker's shared recorder.
    metrics: Arc<dyn crate::metrics::MetricsRecorder>,
    /// Redis-backed principal cutoff denylist for fast post-SCIM-delete token rejection.
    #[cfg(feature = "redis")]
    jti_denylist: Option<crate::runtime::authn::revocation::JtiDenylist>,
}

impl IdentityProviderServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: Arc::new(DataBrokerRuntime::planning_only()),
            event_sink: noop_sink(),
            jwks_cache: oidc::OidcJwksCache::new(),
            metrics: Arc::new(crate::metrics::NoopMetrics),
            #[cfg(feature = "redis")]
            jti_denylist: None,
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_runtime(mut self, runtime: Arc<DataBrokerRuntime>) -> Self {
        self.runtime = runtime;
        self
    }

    pub(crate) fn with_event_sink(mut self, sink: Arc<dyn AuthEventSink>) -> Self {
        self.event_sink = sink;
        self
    }

    pub(crate) fn with_metrics(
        mut self,
        metrics: Arc<dyn crate::metrics::MetricsRecorder>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    #[cfg(feature = "redis")]
    pub(crate) fn with_jti_denylist(
        mut self,
        denylist: Option<crate::runtime::authn::revocation::JtiDenylist>,
    ) -> Self {
        self.jti_denylist = denylist;
        self
    }

    /// IdP control plane is durable-only: fail closed when no pool exists.
    fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            crate::runtime::executor_utils::capability_status(
                "identity_provider",
                "postgres_store",
                "postgres_store",
                "identity-provider service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }

    /// Resolve a provider scoped to the tenant, erroring if absent.
    async fn load_provider(
        &self,
        pool: &PgPool,
        tenant_id: &str,
        provider_id: &str,
    ) -> Result<ProviderRow, Status> {
        if tenant_id.trim().is_empty() {
            return Err(idp_tenant_id_required_status());
        }
        store::get_provider(pool, tenant_id, provider_id)
            .await?
            .ok_or_else(idp_provider_not_found_status)
    }
}

impl Default for IdentityProviderServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

// ── proto conversions ────────────────────────────────────────────────────────

fn json_array_to_vec(json_str: &str) -> Vec<String> {
    serde_json::from_str::<Value>(json_str.trim())
        .ok()
        .and_then(|v| v.as_array().cloned())
        .map(|arr| {
            arr.into_iter()
                .filter_map(|i| i.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn vec_to_json_array(items: &[String]) -> String {
    serde_json::to_string(items).unwrap_or_else(|_| "[]".to_string())
}

fn kind_to_pb(kind: &str) -> i32 {
    use idp_entity_pb::IdpKind as K;
    // Accept BOTH the canonical SHORT stored token (e.g. "OIDC") and the legacy
    // proto-prefixed form ("IDP_KIND_OIDC") so rows written before the §2 fix still
    // read back correctly.
    (match kind {
        "NATIVE" | "IDP_KIND_NATIVE" => K::Native,
        "OIDC" | "IDP_KIND_OIDC" => K::Oidc,
        "SAML" | "IDP_KIND_SAML" => K::Saml,
        "LDAP" | "IDP_KIND_LDAP" => K::Ldap,
        "CUSTOM_JWT" | "IDP_KIND_CUSTOM_JWT" => K::CustomJwt,
        "EXTERNAL_SESSION" | "IDP_KIND_EXTERNAL_SESSION" => K::ExternalSession,
        _ => K::Unspecified,
    }) as i32
}

fn kind_to_db(kind: i32) -> String {
    use idp_entity_pb::IdpKind as K;
    // §2 fix: store the SHORT canonical token, NOT the verbose "IDP_KIND_*" name —
    // "IDP_KIND_EXTERNAL_SESSION" (25 chars) overflowed the `kind` VARCHAR(24) column
    // and failed every CreateProvider. The short form fits comfortably and mirrors
    // the tenant-service convention (`tenant_type_to_db` stores "ORGANIZATION").
    match idp_entity_pb::IdpKind::try_from(kind).unwrap_or(K::Unspecified) {
        K::Native => "NATIVE",
        K::Oidc => "OIDC",
        K::Saml => "SAML",
        K::Ldap => "LDAP",
        K::CustomJwt => "CUSTOM_JWT",
        K::ExternalSession => "EXTERNAL_SESSION",
        K::Unspecified => "UNSPECIFIED",
    }
    .to_string()
}

fn health_to_pb(health: &str) -> i32 {
    use idp_entity_pb::ProviderHealth as H;
    // Accept BOTH the short token and the legacy "PROVIDER_HEALTH_*" form.
    (match health {
        "HEALTHY" | "PROVIDER_HEALTH_HEALTHY" => H::Healthy,
        "DEGRADED" | "PROVIDER_HEALTH_DEGRADED" => H::Degraded,
        "UNREACHABLE" | "PROVIDER_HEALTH_UNREACHABLE" => H::Unreachable,
        _ => H::Unspecified,
    }) as i32
}

fn provider_to_pb(row: &ProviderRow) -> idp_entity_pb::IdentityProvider {
    idp_entity_pb::IdentityProvider {
        provider_id: row.provider_id.clone(),
        tenant_id: row.tenant_id.clone(),
        kind: kind_to_pb(&row.kind),
        display_name: row.display_name.clone(),
        issuer: row.issuer.clone(),
        entity_id: row.entity_id.clone(),
        jwks_url: row.jwks_url.clone(),
        saml_metadata_url: row.saml_metadata_url.clone(),
        client_ids_json: row.client_ids_json.clone(),
        audiences_json: row.audiences_json.clone(),
        claim_mapping_json: row.claim_mapping_json.clone(),
        group_mapping_json: row.group_mapping_json.clone(),
        jit_policy_json: row.jit_policy_json.clone(),
        account_linking_policy: row.account_linking_policy.clone(),
        enabled: row.enabled,
        // Secret columns are never returned on read (storage-only by annotation).
        client_secret: String::new(),
        saml_signing_key_pem: String::new(),
        saml_idp_certs_json: row.saml_idp_certs_json.clone(),
        saml_sso_url: row.saml_sso_url.clone(),
        health: health_to_pb(&row.health),
        last_jwks_refresh_at: timestamp_from_unix(row.last_jwks_refresh_at_unix.max(0) as u64),
        last_jwks_refresh_status: row.last_jwks_refresh_status.clone(),
        created_by: row.created_by.clone(),
        updated_by: row.updated_by.clone(),
        created_at: timestamp_from_unix(row.created_at_unix.max(0) as u64),
        updated_at: timestamp_from_unix(row.updated_at_unix.max(0) as u64),
        deleted_at: None,
    }
}

fn external_identity_to_pb(row: &ExternalIdentityRow) -> idp_entity_pb::ExternalIdentity {
    idp_entity_pb::ExternalIdentity {
        external_identity_id: row.external_identity_id.clone(),
        tenant_id: row.tenant_id.clone(),
        provider_id: row.provider_id.clone(),
        subject: row.subject.clone(),
        user_id: row.user_id.clone(),
        email: row.email.clone(),
        email_verified: row.email_verified,
        linked_at: timestamp_from_unix(row.linked_at_unix.max(0) as u64),
        last_login_at: timestamp_from_unix(row.last_login_at_unix.max(0) as u64),
        deleted_at: None,
    }
}

fn assurance_to_pb(a: idp_entity_pb::AssuranceLevel) -> i32 {
    a as i32
}

/// Union a provider's JIT `default_roles` into the group-mapped role set. Every
/// federated/JIT-provisioned user receives these baseline roles in addition to any
/// roles granted by the group→role mapping (J2.3). Result is sorted + de-duped so
/// the granted-role audit is stable.
fn merge_default_roles(mut roles: Vec<String>, default_roles: Vec<String>) -> Vec<String> {
    for role in default_roles {
        let role = role.trim();
        if !role.is_empty() {
            roles.push(role.to_string());
        }
    }
    roles.sort();
    roles.dedup();
    roles
}

/// A disabled provider must block new login but preserve audit history (J3).
fn ensure_provider_active(row: &ProviderRow) -> Result<(), Status> {
    if !row.enabled {
        return Err(idp_provider_disabled_status(&row.display_name));
    }
    Ok(())
}

// ── handler impl ─────────────────────────────────────────────────────────────

#[tonic::async_trait]
impl IdentityProviderService for IdentityProviderServiceImpl {
    async fn create_provider(
        &self,
        request: Request<idp_pb::CreateProviderRequest>,
    ) -> Result<Response<idp_pb::CreateProviderResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        if req.tenant_id.trim().is_empty() {
            return Err(idp_tenant_id_required_status());
        }
        if req.display_name.trim().is_empty() {
            return Err(idp_display_name_required_status());
        }
        let row = ProviderRow {
            tenant_id: req.tenant_id.clone(),
            kind: kind_to_db(req.kind),
            display_name: req.display_name.clone(),
            issuer: req.issuer.clone(),
            entity_id: req.entity_id.clone(),
            jwks_url: req.jwks_url.clone(),
            saml_metadata_url: req.saml_metadata_url.clone(),
            client_ids_json: vec_to_json_array(&req.client_ids),
            audiences_json: vec_to_json_array(&req.audiences),
            claim_mapping_json: non_empty_json_obj(&req.claim_mapping_json),
            group_mapping_json: non_empty_json_obj(&req.group_mapping_json),
            jit_policy_json: non_empty_json_obj(&req.jit_policy_json),
            account_linking_policy: if req.account_linking_policy.trim().is_empty() {
                "explicit".to_string()
            } else {
                req.account_linking_policy.clone()
            },
            enabled: req.enabled,
            saml_idp_certs_json: "[]".to_string(),
            created_by: req.created_by.clone(),
            updated_by: req.created_by.clone(),
            ..Default::default()
        };
        let provider_id = store::insert_provider(
            self.runtime.as_ref(),
            pool,
            &row,
            &req.client_secret,
            &req.saml_signing_key_pem,
        )
        .await?;
        let created = self
            .load_provider(pool, &req.tenant_id, &provider_id)
            .await?;
        events::emit(
            self.event_sink.as_ref(),
            events::PROVIDER_CREATED,
            provider_id.clone(),
            req.tenant_id.clone(),
            json!({
                "provider_id": provider_id,
                "tenant_id": req.tenant_id,
                "kind": created.kind,
                "display_name": created.display_name,
                "created_by": req.created_by,
            }),
        )
        .await;
        Ok(Response::new(idp_pb::CreateProviderResponse {
            provider: Some(provider_to_pb(&created)),
        }))
    }

    async fn update_provider(
        &self,
        request: Request<idp_pb::UpdateProviderRequest>,
    ) -> Result<Response<idp_pb::UpdateProviderResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        // Resolve-before-mutate: the provider must exist for this tenant.
        let _ = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        let update_mask = crate::runtime::service::native_helpers::update_mask_path_set(
            req.update_mask.as_ref(),
            &[
                "display_name",
                "issuer",
                "entity_id",
                "jwks_url",
                "saml_metadata_url",
                "client_ids",
                "audiences",
                "claim_mapping_json",
                "group_mapping_json",
                "jit_policy_json",
                "account_linking_policy",
                "client_secret",
                "saml_signing_key_pem",
                "updated_by",
            ],
        )?;
        let update_field = |field: &str, legacy_present: bool| {
            crate::runtime::service::native_helpers::update_mask_allows(
                &update_mask,
                field,
                legacy_present,
            )
        };
        let fields = ProviderRow {
            display_name: if update_field("display_name", !req.display_name.trim().is_empty()) {
                req.display_name.clone()
            } else {
                String::new()
            },
            issuer: if update_field("issuer", !req.issuer.trim().is_empty()) {
                req.issuer.clone()
            } else {
                String::new()
            },
            entity_id: if update_field("entity_id", !req.entity_id.trim().is_empty()) {
                req.entity_id.clone()
            } else {
                String::new()
            },
            jwks_url: if update_field("jwks_url", !req.jwks_url.trim().is_empty()) {
                req.jwks_url.clone()
            } else {
                String::new()
            },
            saml_metadata_url: if update_field(
                "saml_metadata_url",
                !req.saml_metadata_url.trim().is_empty(),
            ) {
                req.saml_metadata_url.clone()
            } else {
                String::new()
            },
            client_ids_json: if !update_field("client_ids", !req.client_ids.is_empty()) {
                String::new()
            } else if req.client_ids.is_empty() {
                "[]".to_string()
            } else {
                vec_to_json_array(&req.client_ids)
            },
            audiences_json: if !update_field("audiences", !req.audiences.is_empty()) {
                String::new()
            } else if req.audiences.is_empty() {
                "[]".to_string()
            } else {
                vec_to_json_array(&req.audiences)
            },
            claim_mapping_json: if update_field(
                "claim_mapping_json",
                !req.claim_mapping_json.trim().is_empty(),
            ) {
                req.claim_mapping_json.clone()
            } else {
                String::new()
            },
            group_mapping_json: if update_field(
                "group_mapping_json",
                !req.group_mapping_json.trim().is_empty(),
            ) {
                req.group_mapping_json.clone()
            } else {
                String::new()
            },
            jit_policy_json: if update_field(
                "jit_policy_json",
                !req.jit_policy_json.trim().is_empty(),
            ) {
                req.jit_policy_json.clone()
            } else {
                String::new()
            },
            account_linking_policy: if update_field(
                "account_linking_policy",
                !req.account_linking_policy.trim().is_empty(),
            ) {
                req.account_linking_policy.clone()
            } else {
                String::new()
            },
            updated_by: if update_field("updated_by", !req.updated_by.trim().is_empty()) {
                req.updated_by.clone()
            } else {
                String::new()
            },
            ..Default::default()
        };
        let client_secret = if update_field("client_secret", !req.client_secret.trim().is_empty()) {
            req.client_secret.as_str()
        } else {
            ""
        };
        let saml_signing_key_pem = if update_field(
            "saml_signing_key_pem",
            !req.saml_signing_key_pem.trim().is_empty(),
        ) {
            req.saml_signing_key_pem.as_str()
        } else {
            ""
        };
        let updated = store::update_provider(
            self.runtime.as_ref(),
            pool,
            &req.tenant_id,
            &req.provider_id,
            &fields,
            client_secret,
            saml_signing_key_pem,
        )
        .await?
        .ok_or_else(idp_provider_not_found_status)?;
        // Invalidate the JWKS cache: issuer/jwks_url may have changed.
        self.jwks_cache.invalidate(&req.tenant_id, &req.provider_id);
        events::emit(
            self.event_sink.as_ref(),
            events::PROVIDER_UPDATED,
            req.provider_id.clone(),
            req.tenant_id.clone(),
            json!({ "provider_id": req.provider_id, "tenant_id": req.tenant_id, "updated_by": req.updated_by }),
        )
        .await;
        Ok(Response::new(idp_pb::UpdateProviderResponse {
            provider: Some(provider_to_pb(&updated)),
        }))
    }

    async fn disable_provider(
        &self,
        request: Request<idp_pb::DisableProviderRequest>,
    ) -> Result<Response<idp_pb::DisableProviderResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let _ = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        let disabled =
            store::disable_provider(pool, &req.tenant_id, &req.provider_id, &req.updated_by)
                .await?
                .ok_or_else(idp_provider_not_found_status)?;
        events::emit(
            self.event_sink.as_ref(),
            events::PROVIDER_DISABLED,
            req.provider_id.clone(),
            req.tenant_id.clone(),
            json!({ "provider_id": req.provider_id, "tenant_id": req.tenant_id, "updated_by": req.updated_by }),
        )
        .await;
        Ok(Response::new(idp_pb::DisableProviderResponse {
            provider: Some(provider_to_pb(&disabled)),
        }))
    }

    async fn get_provider(
        &self,
        request: Request<idp_pb::GetProviderRequest>,
    ) -> Result<Response<idp_pb::GetProviderResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        Ok(Response::new(idp_pb::GetProviderResponse {
            provider: Some(provider_to_pb(&provider)),
        }))
    }

    async fn list_providers(
        &self,
        request: Request<idp_pb::ListProvidersRequest>,
    ) -> Result<Response<idp_pb::ListProvidersResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        if req.tenant_id.trim().is_empty() {
            return Err(idp_tenant_id_required_status());
        }
        let (limit, offset, _) = bounded_page_window(req.page.as_ref());
        let kind = if req.kind == idp_entity_pb::IdpKind::Unspecified as i32 {
            String::new()
        } else {
            kind_to_db(req.kind)
        };
        let (rows, total) = store::list_providers(
            pool,
            &req.tenant_id,
            &kind,
            req.enabled_only,
            limit as i64,
            offset as i64,
        )
        .await?;
        Ok(Response::new(idp_pb::ListProvidersResponse {
            providers: rows.iter().map(provider_to_pb).collect(),
            page: Some(bounded_page_response(total as usize, req.page.as_ref())),
        }))
    }

    async fn test_provider_discovery(
        &self,
        request: Request<idp_pb::TestProviderDiscoveryRequest>,
    ) -> Result<Response<idp_pb::TestProviderDiscoveryResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        match self
            .jwks_cache
            .resolve(
                &req.tenant_id,
                &req.provider_id,
                &provider.issuer,
                &provider.jwks_url,
                true,
            )
            .await
        {
            Ok(res) => Ok(Response::new(idp_pb::TestProviderDiscoveryResponse {
                reachable: true,
                health: idp_entity_pb::ProviderHealth::Healthy as i32,
                resolved_issuer: provider.issuer,
                resolved_jwks_url: provider.jwks_url,
                key_count: res.kids.len() as i32,
                key_ids: res.kids,
                detail: "discovery + JWKS reachable".to_string(),
            })),
            Err(err) => Ok(Response::new(idp_pb::TestProviderDiscoveryResponse {
                reachable: false,
                health: idp_entity_pb::ProviderHealth::Unreachable as i32,
                resolved_issuer: provider.issuer,
                resolved_jwks_url: provider.jwks_url,
                key_count: 0,
                key_ids: Vec::new(),
                detail: err,
            })),
        }
    }

    async fn force_jwks_refresh(
        &self,
        request: Request<idp_pb::ForceJwksRefreshRequest>,
    ) -> Result<Response<idp_pb::ForceJwksRefreshResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        self.jwks_cache.invalidate(&req.tenant_id, &req.provider_id);
        let result = self
            .jwks_cache
            .resolve(
                &req.tenant_id,
                &req.provider_id,
                &provider.issuer,
                &provider.jwks_url,
                true,
            )
            .await;
        let (ok, kids, health, status) = match &result {
            // A forced refresh must reach the IdP: a cache-served result (from_cache)
            // or an empty JWKS document is treated as degraded, not healthy, so the
            // operator sees that the federation edge did not actually re-fetch keys.
            Ok(res) if res.from_cache || res.jwks_json.trim().is_empty() => (
                false,
                res.kids.clone(),
                "DEGRADED",
                if res.from_cache {
                    "forced refresh served stale cache (live JWKS fetch did not occur)".to_string()
                } else {
                    "JWKS endpoint returned an empty key set".to_string()
                },
            ),
            Ok(res) => (
                true,
                res.kids.clone(),
                "HEALTHY",
                format!(
                    "refreshed {} key(s) ({} bytes)",
                    res.kids.len(),
                    res.jwks_json.len()
                ),
            ),
            Err(err) => (false, Vec::new(), "DEGRADED", err.clone()),
        };
        // Phase L3 task6: count discovery/JWKS refresh failures so operators can
        // alert on a degraded federation edge.
        // Tier-7 #32: also emit an auditable event (previously metric-only) so the
        // degraded edge appears in the IdP event stream, not just the scrape gauge.
        if !ok {
            self.metrics.record_idp_refresh_failure("jwks");
            events::emit(
                self.event_sink.as_ref(),
                events::PROVIDER_REFRESH_FAILED,
                req.provider_id.clone(),
                req.tenant_id.clone(),
                json!({
                    "provider_id": req.provider_id,
                    "tenant_id": req.tenant_id,
                    "kind": "jwks",
                    "health": health,
                    "status": status,
                }),
            )
            .await;
        }
        // Persist provider health + last refresh status (J2.1).
        store::record_jwks_refresh(pool, &req.tenant_id, &req.provider_id, health, &status).await?;
        events::emit(
            self.event_sink.as_ref(),
            events::JWKS_REFRESHED,
            req.provider_id.clone(),
            req.tenant_id.clone(),
            json!({ "provider_id": req.provider_id, "ok": ok, "key_count": kids.len(), "status": status }),
        )
        .await;
        Ok(Response::new(idp_pb::ForceJwksRefreshResponse {
            ok,
            key_count: kids.len() as i32,
            key_ids: kids,
            refreshed_at: timestamp_from_unix(now_unix_i64().max(0) as u64),
            status,
        }))
    }

    async fn preview_claim_mapping(
        &self,
        request: Request<idp_pb::PreviewClaimMappingRequest>,
    ) -> Result<Response<idp_pb::PreviewClaimMappingResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        let claims: Value =
            serde_json::from_str(req.claims_json.trim()).map_err(idp_claims_json_invalid_status)?;
        let mapping_json = if req.claim_mapping_json.trim().is_empty() {
            provider.claim_mapping_json.clone()
        } else {
            req.claim_mapping_json.clone()
        };
        let mapped = apply_claim_mapping(&mapping_json, &claims);
        let principal_json = json!({
            "subject": mapped.subject,
            "email": mapped.email,
            "email_verified": mapped.email_verified,
            "display_name": mapped.display_name,
            "groups": mapped.groups,
            "assurance": format!("{:?}", mapped.assurance),
        });
        Ok(Response::new(idp_pb::PreviewClaimMappingResponse {
            subject: mapped.subject,
            email: mapped.email,
            email_verified: mapped.email_verified,
            display_name: mapped.display_name,
            groups: mapped.groups,
            assurance: assurance_to_pb(mapped.assurance),
            mapped_principal_json: principal_json.to_string(),
        }))
    }

    async fn preview_group_mapping(
        &self,
        request: Request<idp_pb::PreviewGroupMappingRequest>,
    ) -> Result<Response<idp_pb::PreviewGroupMappingResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        let mapping_json = if req.group_mapping_json.trim().is_empty() {
            provider.group_mapping_json.clone()
        } else {
            req.group_mapping_json.clone()
        };
        let (roles, unmapped) = map_groups_to_roles(&mapping_json, &req.groups);
        Ok(Response::new(idp_pb::PreviewGroupMappingResponse {
            roles,
            unmapped_groups: unmapped,
        }))
    }

    async fn list_external_identities(
        &self,
        request: Request<idp_pb::ListExternalIdentitiesRequest>,
    ) -> Result<Response<idp_pb::ListExternalIdentitiesResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        if req.tenant_id.trim().is_empty() {
            return Err(idp_tenant_id_required_status());
        }
        let (limit, offset, _) = bounded_page_window(req.page.as_ref());
        let (rows, total) = store::list_external_identities(
            pool,
            &req.tenant_id,
            &req.provider_id,
            &req.user_id,
            limit as i64,
            offset as i64,
        )
        .await?;
        Ok(Response::new(idp_pb::ListExternalIdentitiesResponse {
            identities: rows.iter().map(external_identity_to_pb).collect(),
            page: Some(bounded_page_response(total as usize, req.page.as_ref())),
        }))
    }

    async fn link_identity(
        &self,
        request: Request<idp_pb::LinkIdentityRequest>,
    ) -> Result<Response<idp_pb::LinkIdentityResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        // Provider must exist for the tenant (prevents cross-tenant linking).
        let _ = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        if req.subject.trim().is_empty() || req.user_id.trim().is_empty() {
            return Err(idp_subject_user_required_status());
        }
        let row = store::upsert_external_identity(
            pool,
            &req.tenant_id,
            &req.provider_id,
            &req.subject,
            &req.user_id,
            &req.email,
            req.email_verified,
        )
        .await?;
        events::emit(
            self.event_sink.as_ref(),
            events::IDENTITY_LINKED,
            row.external_identity_id.clone(),
            req.tenant_id.clone(),
            json!({ "provider_id": req.provider_id, "subject": req.subject, "user_id": req.user_id }),
        )
        .await;
        Ok(Response::new(idp_pb::LinkIdentityResponse {
            identity: Some(external_identity_to_pb(&row)),
        }))
    }

    async fn unlink_identity(
        &self,
        request: Request<idp_pb::UnlinkIdentityRequest>,
    ) -> Result<Response<idp_pb::UnlinkIdentityResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        if req.tenant_id.trim().is_empty() {
            return Err(idp_tenant_id_required_status());
        }
        let unlinked =
            store::unlink_external_identity(pool, &req.tenant_id, &req.external_identity_id)
                .await?;
        if unlinked {
            events::emit(
                self.event_sink.as_ref(),
                events::IDENTITY_UNLINKED,
                req.external_identity_id.clone(),
                req.tenant_id.clone(),
                json!({ "external_identity_id": req.external_identity_id, "tenant_id": req.tenant_id }),
            )
            .await;
        }
        Ok(Response::new(idp_pb::UnlinkIdentityResponse { unlinked }))
    }

    // ── SAML 2.0 ──────────────────────────────────────────────────────────────
    async fn import_saml_metadata(
        &self,
        request: Request<idp_pb::ImportSamlMetadataRequest>,
    ) -> Result<Response<idp_pb::ImportSamlMetadataResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        let xml = if !req.metadata_xml.trim().is_empty() {
            req.metadata_xml.clone()
        } else if !provider.saml_metadata_url.trim().is_empty() {
            oidc::fetch_text(&provider.saml_metadata_url)
                .await
                .map_err(idp_metadata_fetch_failed_status)?
        } else {
            return Err(idp_saml_metadata_required_status());
        };
        let meta = saml::parse_metadata(&xml).map_err(idp_saml_metadata_invalid_status)?;
        let certs_json = vec_to_json_array(&meta.signing_certs_b64);
        let updated = store::update_saml_metadata(
            pool,
            &req.tenant_id,
            &req.provider_id,
            &meta.entity_id,
            &meta.sso_url,
            &certs_json,
            &req.updated_by,
        )
        .await?
        .ok_or_else(idp_provider_not_found_status)?;
        // Tier-7 #32: emit a SAML metadata-update event. Only non-secret descriptor
        // fields (entity_id, SSO URL, signing-cert COUNT) are recorded — never the
        // certificate bytes or any signing key.
        events::emit(
            self.event_sink.as_ref(),
            events::SAML_METADATA_UPDATED,
            req.provider_id.clone(),
            req.tenant_id.clone(),
            json!({
                "provider_id": req.provider_id,
                "tenant_id": req.tenant_id,
                "entity_id": meta.entity_id,
                "sso_url": meta.sso_url,
                "cert_count": meta.signing_certs_b64.len(),
                "updated_by": req.updated_by,
            }),
        )
        .await;
        Ok(Response::new(idp_pb::ImportSamlMetadataResponse {
            entity_id: meta.entity_id,
            sso_url: meta.sso_url,
            cert_count: meta.signing_certs_b64.len() as i32,
            provider: Some(provider_to_pb(&updated)),
        }))
    }

    async fn start_saml_login(
        &self,
        request: Request<idp_pb::StartSamlLoginRequest>,
    ) -> Result<Response<idp_pb::StartSamlLoginResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        ensure_provider_active(&provider)?;
        if provider.saml_sso_url.trim().is_empty() {
            return Err(idp_saml_sso_url_missing_status());
        }
        let sp_entity_id = if provider.entity_id.trim().is_empty() {
            format!("urn:udb:sp:{}", req.tenant_id)
        } else {
            provider.entity_id.clone()
        };
        let acs_url = std::env::var("UDB_SAML_ACS_URL")
            .unwrap_or_else(|_| format!("/v1/idp/providers/{}:saml-acs", req.provider_id));
        let (saml_request, request_id) =
            saml::build_authn_request(&sp_entity_id, &provider.saml_sso_url, &acs_url).map_err(
                |e| {
                    idp_internal_status(
                        "start_saml_login",
                        format!("AuthnRequest build failed: {e}"),
                    )
                },
            )?;
        // Request signing requires the SP private key + XML-DSig signature
        // construction (host-blocked: no XML C14N). We return the (unsigned)
        // request rather than claim it is signed. `signed=false` is honest.
        use urlencoding::encode;
        let mut redirect = format!(
            "{}?SAMLRequest={}",
            provider.saml_sso_url,
            encode(&saml_request)
        );
        if !req.relay_state.trim().is_empty() {
            redirect.push_str(&format!("&RelayState={}", encode(&req.relay_state)));
        }
        Ok(Response::new(idp_pb::StartSamlLoginResponse {
            redirect_url: redirect,
            saml_request,
            request_id,
            signed: false,
        }))
    }

    async fn saml_acs(
        &self,
        request: Request<idp_pb::SamlAcsRequest>,
    ) -> Result<Response<idp_pb::SamlAcsResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        // A disabled provider blocks new login but the call is still audited.
        if !provider.enabled {
            events::emit(
                self.event_sink.as_ref(),
                events::SAML_ASSERTION_CONSUMED,
                req.provider_id.clone(),
                req.tenant_id.clone(),
                json!({ "provider_id": req.provider_id, "result": "rejected", "reason": "provider_disabled" }),
            )
            .await;
            return Err(idp_provider_disabled_static_status());
        }
        let certs = json_array_to_vec(&provider.saml_idp_certs_json);
        let audience = provider.entity_id.clone();
        // #12-13: dev self-asserted IdP path. With UDB_SAML_TEST_MODE on, a sentinel
        // saml_response (`__UDB_SAML_TEST__[:name_id]`) makes the broker mint a REAL
        // XML-DSig-signed assertion with the dev IdP key and verify it through the
        // SAME validate_response path below — never an accept-any bypass. Fail-closed
        // off in production. See `saml::dev_self_assert`.
        let (saml_response, certs) = if saml::dev_test_mode_enabled()
            && req
                .saml_response
                .trim_start()
                .starts_with(saml::DEV_SENTINEL)
        {
            let name_id = req
                .saml_response
                .trim_start()
                .strip_prefix(saml::DEV_SENTINEL)
                .map(|rest| rest.trim_start_matches(':').trim())
                .filter(|s| !s.is_empty())
                .unwrap_or("dev-user@udb.local")
                .to_string();
            let (resp_b64, cert_b64) = saml::dev_self_assert(
                &provider.entity_id,
                &name_id,
                &audience,
                &std::collections::BTreeMap::new(),
            )
            .map_err(|e| {
                idp_internal_status(
                    "saml_acs_dev_self_assert",
                    format!("dev SAML self-assert failed: {e}"),
                )
            })?;
            (resp_b64, vec![cert_b64])
        } else {
            (req.saml_response.clone(), certs)
        };
        let (assertion, signature_verified) = match saml::validate_response(
            &saml_response,
            &certs,
            &audience,
            saml_clock_skew_secs(),
            now_unix_i64(),
        ) {
            Ok(v) => v,
            Err(err) => {
                events::emit(
                    self.event_sink.as_ref(),
                    events::SAML_ASSERTION_CONSUMED,
                    req.provider_id.clone(),
                    req.tenant_id.clone(),
                    json!({ "provider_id": req.provider_id, "result": "rejected", "reason": err.to_string() }),
                )
                .await;
                // Fail closed: never authenticate on a signature/parse failure.
                return Ok(Response::new(idp_pb::SamlAcsResponse {
                    authenticated: false,
                    signature_verified: false,
                    detail: err.to_string(),
                    ..Default::default()
                }));
            }
        };

        // Durable replay guard (J3): first-seen wins, a duplicate is rejected.
        let not_after = assertion.not_on_or_after_unix.max(now_unix_i64() + 300);
        let first_seen = store::record_saml_assertion(
            pool,
            &req.tenant_id,
            &req.provider_id,
            &assertion.assertion_id,
            not_after,
        )
        .await?;
        if !first_seen {
            events::emit(
                self.event_sink.as_ref(),
                events::SAML_REPLAY_REJECTED,
                req.provider_id.clone(),
                req.tenant_id.clone(),
                json!({ "provider_id": req.provider_id, "assertion_id": assertion.assertion_id }),
            )
            .await;
            return Err(idp_saml_replay_rejected_status());
        }

        // Map NameID + attributes → principal via the provider claim mapping.
        let claims = saml_assertion_to_claims(&assertion);
        let mapped = apply_claim_mapping(&provider.claim_mapping_json, &claims);
        let subject = if mapped.subject.is_empty() {
            assertion.name_id.clone()
        } else {
            mapped.subject.clone()
        };
        let (roles, unmapped_groups) =
            map_groups_to_roles(&provider.group_mapping_json, &mapped.groups);
        // Union the provider's JIT baseline roles (assigned to every federated
        // user, in addition to any group-mapped roles) so `default_roles` from the
        // provider policy is actually granted, not just parsed.
        let roles = merge_default_roles(
            roles,
            JitPolicy::from_json(&provider.jit_policy_json).default_roles,
        );

        // Resolve/JIT-provision the UDB user, then link the external identity.
        let resolved = self
            .resolve_or_provision(pool, &provider, &subject, &mapped, &req.tenant_id)
            .await?;

        events::emit(
            self.event_sink.as_ref(),
            events::SAML_ASSERTION_CONSUMED,
            req.provider_id.clone(),
            req.tenant_id.clone(),
            // `granted_roles` (the mapped roles) and `unmapped_groups` make the
            // group→role decision fully auditable: a group claim only grants a
            // role through the configured allowlist mapping, and any group that
            // mapped to nothing is recorded so a sync cannot silently bypass UDB
            // role mapping. Field names mirror the SCIM `granted_roles` event.
            json!({
                "provider_id": req.provider_id,
                "result": "ok",
                "subject": subject,
                "user_id": resolved.user_id,
                "assurance": format!("{:?}", mapped.assurance),
                "signature_verified": signature_verified,
                "granted_roles": roles,
                "unmapped_groups": unmapped_groups,
            }),
        )
        .await;

        Ok(Response::new(idp_pb::SamlAcsResponse {
            authenticated: true,
            subject,
            user_id: resolved.user_id,
            email: mapped.email,
            email_verified: mapped.email_verified,
            groups: mapped.groups,
            roles,
            assurance: assurance_to_pb(mapped.assurance),
            signature_verified,
            detail: "assertion accepted".to_string(),
            attributes_json: claims.to_string(),
        }))
    }

    // ── JIT provisioning + assurance ───────────────────────────────────────────
    async fn resolve_external_identity(
        &self,
        request: Request<idp_pb::ResolveExternalIdentityRequest>,
    ) -> Result<Response<idp_pb::ResolveExternalIdentityResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        ensure_provider_active(&provider)?;
        let claims: Value =
            serde_json::from_str(req.claims_json.trim()).map_err(idp_claims_json_invalid_status)?;
        let mapped = apply_claim_mapping(&provider.claim_mapping_json, &claims);
        let subject = if mapped.subject.is_empty() {
            return Err(idp_claims_subject_required_status());
        } else {
            mapped.subject.clone()
        };
        let (roles, _unmapped) = map_groups_to_roles(&provider.group_mapping_json, &mapped.groups);
        // Also grant the provider's JIT baseline roles (`default_roles`).
        let roles = merge_default_roles(
            roles,
            JitPolicy::from_json(&provider.jit_policy_json).default_roles,
        );
        let resolved = self
            .resolve_or_provision(pool, &provider, &subject, &mapped, &req.tenant_id)
            .await?;
        Ok(Response::new(idp_pb::ResolveExternalIdentityResponse {
            user_id: resolved.user_id,
            subject,
            email: mapped.email,
            provisioned: resolved.provisioned,
            linked: resolved.linked,
            roles,
            assurance: assurance_to_pb(mapped.assurance),
            detail: resolved.detail,
        }))
    }

    // ── SCIM 2.0 ────────────────────────────────────────────────────────────────
    async fn scim_create_user(
        &self,
        request: Request<idp_pb::ScimCreateUserRequest>,
    ) -> Result<Response<idp_pb::ScimCreateUserResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        let view = match scim::parse_scim_user(&req.scim_user_json) {
            Ok(view) => view,
            Err(e) => {
                // Phase L3 task6: count SCIM provisioning failures (metric) AND
                // persist the failure to the durable per-provider directory state
                // (J2.3) so failure_count/last_error survive a scrape gap.
                self.metrics.record_scim_failure("provision");
                let _ =
                    store::record_scim_sync(pool, &req.tenant_id, &req.provider_id, false, "", &e)
                        .await;
                return Err(idp_scim_user_json_invalid_status(e));
            }
        };
        // Provision the user (no group→role binding here — only via mappings).
        let default_project = JitPolicy::from_json(&provider.jit_policy_json).default_project;
        let user_id = store::create_external_user(
            pool,
            &req.tenant_id,
            &default_project,
            &req.provider_id,
            &view.user_name,
            &view.email,
            &view.display_name,
            !view.email.is_empty(),
            "scim",
        )
        .await?;
        let link = store::upsert_external_identity(
            pool,
            &req.tenant_id,
            &req.provider_id,
            &view.user_name,
            &user_id,
            &view.email,
            !view.email.is_empty(),
        )
        .await?;
        // SCIM deactivate-on-create when active=false.
        if !view.active {
            let _ = store::deactivate_user(pool, &req.tenant_id, &user_id).await?;
        }
        events::emit(
            self.event_sink.as_ref(),
            events::SCIM_USER_PROVISIONED,
            link.external_identity_id.clone(),
            req.tenant_id.clone(),
            json!({ "provider_id": req.provider_id, "user_id": user_id, "active": view.active }),
        )
        .await;
        // Record a successful directory sync: resets failure_count, stamps
        // last_sync_at, clears last_error on the per-provider state row (J2.3).
        let _ = store::record_scim_sync(pool, &req.tenant_id, &req.provider_id, true, "", "").await;
        Ok(Response::new(idp_pb::ScimCreateUserResponse {
            user: Some(scim_user_pb(&link.external_identity_id, &view)),
        }))
    }

    async fn scim_get_user(
        &self,
        request: Request<idp_pb::ScimGetUserRequest>,
    ) -> Result<Response<idp_pb::ScimGetUserResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let _ = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        // scim_user_id is the external_identity_id OR the subject; look up by
        // subject when it is not a UUID.
        let row = self
            .find_scim_identity(pool, &req.tenant_id, &req.provider_id, &req.scim_user_id)
            .await?
            .ok_or_else(idp_scim_user_not_found_status)?;
        let view = scim::ScimUserView {
            user_name: row.subject.clone(),
            email: row.email.clone(),
            active: true,
            ..Default::default()
        };
        Ok(Response::new(idp_pb::ScimGetUserResponse {
            user: Some(scim_user_pb(&row.external_identity_id, &view)),
        }))
    }

    async fn scim_list_users(
        &self,
        request: Request<idp_pb::ScimListUsersRequest>,
    ) -> Result<Response<idp_pb::ScimListUsersResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let _ = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        let (limit, offset, _) = bounded_page_window(req.page.as_ref());
        let (rows, total) = store::list_external_identities(
            pool,
            &req.tenant_id,
            &req.provider_id,
            "",
            limit as i64,
            offset as i64,
        )
        .await?;
        let users = rows
            .iter()
            .map(|row| {
                let view = scim::ScimUserView {
                    user_name: row.subject.clone(),
                    email: row.email.clone(),
                    active: true,
                    ..Default::default()
                };
                scim_user_pb(&row.external_identity_id, &view)
            })
            .collect();
        Ok(Response::new(idp_pb::ScimListUsersResponse {
            users,
            page: Some(bounded_page_response(total as usize, req.page.as_ref())),
        }))
    }

    async fn scim_replace_user(
        &self,
        request: Request<idp_pb::ScimReplaceUserRequest>,
    ) -> Result<Response<idp_pb::ScimReplaceUserResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let _ = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        let view = scim::parse_scim_user(&req.scim_user_json)
            .map_err(idp_scim_user_json_invalid_status)?;
        let row = self
            .find_scim_identity(pool, &req.tenant_id, &req.provider_id, &req.scim_user_id)
            .await?
            .ok_or_else(idp_scim_user_not_found_status)?;
        // Re-upsert the link with the replaced email; deactivate when inactive.
        store::upsert_external_identity(
            pool,
            &req.tenant_id,
            &req.provider_id,
            &row.subject,
            &row.user_id,
            &view.email,
            !view.email.is_empty(),
        )
        .await?;
        if !view.active {
            let _ = store::deactivate_user(pool, &req.tenant_id, &row.user_id).await?;
            // Deactivate path keeps its existing dedicated event below; here we
            // only emit the UPDATE event for the active (in-place replace) case.
            events::emit(
                self.event_sink.as_ref(),
                events::SCIM_USER_DEACTIVATED,
                row.external_identity_id.clone(),
                req.tenant_id.clone(),
                json!({ "provider_id": req.provider_id, "user_id": row.user_id, "via": "scim_replace" }),
            )
            .await;
        } else {
            // Tier-7 #32: SCIM user UPDATE event on the active replace path. Only
            // identifiers are recorded (no credentials/PII beyond the subject).
            events::emit(
                self.event_sink.as_ref(),
                events::SCIM_USER_UPDATED,
                row.external_identity_id.clone(),
                req.tenant_id.clone(),
                json!({
                    "provider_id": req.provider_id,
                    "user_id": row.user_id,
                    "via": "scim_replace",
                    "active": view.active,
                }),
            )
            .await;
        }
        Ok(Response::new(idp_pb::ScimReplaceUserResponse {
            user: Some(scim_user_pb(&row.external_identity_id, &view)),
        }))
    }

    async fn scim_patch_user(
        &self,
        request: Request<idp_pb::ScimPatchUserRequest>,
    ) -> Result<Response<idp_pb::ScimPatchUserResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let _ = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        let row = self
            .find_scim_identity(pool, &req.tenant_id, &req.provider_id, &req.scim_user_id)
            .await?
            .ok_or_else(idp_scim_user_not_found_status)?;
        let base = scim::ScimUserView {
            user_name: row.subject.clone(),
            email: row.email.clone(),
            active: true,
            ..Default::default()
        };
        let ops: Vec<scim::PatchOp> = req
            .operations
            .iter()
            .map(|o| scim::PatchOp {
                op: o.op.clone(),
                path: o.path.clone(),
                value: serde_json::from_str(&o.value_json).unwrap_or(Value::Null),
            })
            .collect();
        let patched = scim::apply_user_patch(base, &ops).map_err(idp_scim_patch_invalid_status)?;
        // Map active=false → deactivate + session revoke (deprovision mapping).
        if !patched.active {
            let _ = store::deactivate_user(pool, &req.tenant_id, &row.user_id).await?;
            events::emit(
                self.event_sink.as_ref(),
                events::SCIM_USER_DEACTIVATED,
                row.external_identity_id.clone(),
                req.tenant_id.clone(),
                json!({ "provider_id": req.provider_id, "user_id": row.user_id }),
            )
            .await;
        } else {
            // Tier-7 #32: SCIM user UPDATE event on the active patch path. Records
            // identifiers and the patched op paths only (never op values, which may
            // carry credentials/PII).
            let op_paths: Vec<String> = req.operations.iter().map(|o| o.path.clone()).collect();
            events::emit(
                self.event_sink.as_ref(),
                events::SCIM_USER_UPDATED,
                row.external_identity_id.clone(),
                req.tenant_id.clone(),
                json!({
                    "provider_id": req.provider_id,
                    "user_id": row.user_id,
                    "via": "scim_patch",
                    "active": patched.active,
                    "op_paths": op_paths,
                }),
            )
            .await;
        }
        Ok(Response::new(idp_pb::ScimPatchUserResponse {
            user: Some(scim_user_pb(&row.external_identity_id, &patched)),
        }))
    }

    async fn scim_delete_user(
        &self,
        request: Request<idp_pb::ScimDeleteUserRequest>,
    ) -> Result<Response<idp_pb::ScimDeleteUserResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let _ = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        let row = self
            .find_scim_identity(pool, &req.tenant_id, &req.provider_id, &req.scim_user_id)
            .await?
            .ok_or_else(idp_scim_user_not_found_status)?;
        // SCIM DELETE is the principal-scoped right-to-be-forgotten bridge: the
        // resolved external identity selects exactly one UDB principal, then the
        // store removes that principal's auth/session/credential rows in one
        // transaction and records the principal cutoff when Redis is available.
        let now_unix = chrono::Utc::now().timestamp().max(0) as u64;
        let report = {
            #[cfg(feature = "redis")]
            {
                store::hard_delete_scim_principal(
                    pool,
                    &req.tenant_id,
                    &row.user_id,
                    self.jti_denylist.as_ref(),
                    now_unix,
                )
                .await?
            }
            #[cfg(not(feature = "redis"))]
            {
                store::hard_delete_scim_principal(pool, &req.tenant_id, &row.user_id, now_unix)
                    .await?
            }
        };
        events::emit(
            self.event_sink.as_ref(),
            events::SCIM_USER_DEACTIVATED,
            row.external_identity_id.clone(),
            req.tenant_id.clone(),
            json!({
                "provider_id": req.provider_id,
                "tenant_id": &report.tenant_id,
                "user_id": &report.user_id,
                "via": "scim_delete_hard",
                "deleted_external_identities": report.deleted_external_identities,
                "deleted_api_keys": report.deleted_api_keys,
                "deleted_token_families": report.deleted_token_families,
                "deleted_sessions": report.deleted_sessions,
                "deleted_devices": report.deleted_devices,
                "deleted_mfa_challenges": report.deleted_mfa_challenges,
                "deleted_otps": report.deleted_otps,
                "deleted_recovery_codes": report.deleted_recovery_codes,
                "deleted_webauthn_credentials": report.deleted_webauthn_credentials,
                "deleted_webauthn_challenges": report.deleted_webauthn_challenges,
                "deleted_users": report.deleted_users,
                "total_deleted": report.total_deleted(),
                "principal_denylisted": report.principal_denylisted,
            }),
        )
        .await;
        Ok(Response::new(idp_pb::ScimDeleteUserResponse {
            deactivated: report.total_deleted() > 0,
        }))
    }

    async fn scim_create_group(
        &self,
        request: Request<idp_pb::ScimCreateGroupRequest>,
    ) -> Result<Response<idp_pb::ScimCreateGroupResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        let view = scim::parse_scim_group(&req.scim_group_json)
            .map_err(idp_scim_group_json_invalid_status)?;
        // Groups are not persisted as separate rows; they drive role binding only
        // through the configured group mapping. The group id must therefore be a
        // configured mapping key (mapping-driven), not a random UUID, so the
        // resolver (`scim_get_group`) can find it. Fail closed otherwise.
        let keys = group_keys(&provider.group_mapping_json);
        if !keys.contains(&view.display_name) {
            return Err(idp_scim_group_mapping_required_status());
        }
        let id = view.display_name.clone();
        Ok(Response::new(idp_pb::ScimCreateGroupResponse {
            group: Some(scim_group_pb(&id, &view)),
        }))
    }

    async fn scim_get_group(
        &self,
        request: Request<idp_pb::ScimGetGroupRequest>,
    ) -> Result<Response<idp_pb::ScimGetGroupResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let _ = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        // Groups are not stored; report the configured group→role mapping keys
        // as the canonical group set.
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        let groups = group_keys(&provider.group_mapping_json);
        if !groups.contains(&req.scim_group_id) {
            return Err(idp_scim_group_not_found_status());
        }
        let view = scim::ScimGroupView {
            display_name: req.scim_group_id.clone(),
            members: Vec::new(),
        };
        Ok(Response::new(idp_pb::ScimGetGroupResponse {
            group: Some(scim_group_pb(&req.scim_group_id, &view)),
        }))
    }

    async fn scim_list_groups(
        &self,
        request: Request<idp_pb::ScimListGroupsRequest>,
    ) -> Result<Response<idp_pb::ScimListGroupsResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        let groups: Vec<idp_pb::ScimGroup> = group_keys(&provider.group_mapping_json)
            .into_iter()
            .map(|g| {
                scim_group_pb(
                    &g,
                    &scim::ScimGroupView {
                        display_name: g.clone(),
                        members: Vec::new(),
                    },
                )
            })
            .collect();
        let total = groups.len();
        Ok(Response::new(idp_pb::ScimListGroupsResponse {
            groups,
            page: Some(bounded_page_response(total, req.page.as_ref())),
        }))
    }

    async fn scim_patch_group(
        &self,
        request: Request<idp_pb::ScimPatchGroupRequest>,
    ) -> Result<Response<idp_pb::ScimPatchGroupResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        if !group_keys(&provider.group_mapping_json).contains(&req.scim_group_id) {
            return Err(idp_scim_group_not_found_status());
        }
        // Group membership changes only ever grant roles through the configured
        // mapping. Compute the roles this group grants (if any) for the response;
        // membership itself is not separately persisted.
        let (granted, _unmapped) =
            map_groups_to_roles(&provider.group_mapping_json, &[req.scim_group_id.clone()]);
        events::emit(
            self.event_sink.as_ref(),
            events::SCIM_GROUP_CHANGED,
            req.scim_group_id.clone(),
            req.tenant_id.clone(),
            json!({ "provider_id": req.provider_id, "group": req.scim_group_id, "granted_roles": granted }),
        )
        .await;
        let view = scim::ScimGroupView {
            display_name: req.scim_group_id.clone(),
            members: Vec::new(),
        };
        Ok(Response::new(idp_pb::ScimPatchGroupResponse {
            group: Some(scim_group_pb(&req.scim_group_id, &view)),
            granted_roles: granted,
        }))
    }

    async fn scim_delete_group(
        &self,
        request: Request<idp_pb::ScimDeleteGroupRequest>,
    ) -> Result<Response<idp_pb::ScimDeleteGroupResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        // IDP2: bind the request-body tenant to the VALIDATED bearer claim. The
        // method-security tower only checks headers + scope, never the decoded
        // body, so without this a tenant-A caller could target tenant B via
        // `req.tenant_id` (e.g. ScimDeleteUser{tenant_id:"B"}) — cross-tenant
        // provisioning / hard-delete. Fail-closed; genuine cross-tenant admins are
        // exempt inside the helper; an empty body tenant inherits the claim scope.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &req.tenant_id,
            "",
        )?;
        let provider = self
            .load_provider(pool, &req.tenant_id, &req.provider_id)
            .await?;
        if !group_keys(&provider.group_mapping_json).contains(&req.scim_group_id) {
            return Err(idp_scim_group_not_found_status());
        }
        // Groups are mapping-driven; "deletion" is a no-op on stored data (the
        // operator removes the mapping entry to revoke the binding). Reported as
        // deleted=true so SCIM clients converge.
        Ok(Response::new(idp_pb::ScimDeleteGroupResponse {
            deleted: true,
        }))
    }
}

// ── private resolution helpers ────────────────────────────────────────────────

struct ResolvedUser {
    user_id: String,
    provisioned: bool,
    linked: bool,
    detail: String,
}

impl IdentityProviderServiceImpl {
    /// Resolve a UDB user for a verified external subject: returns the existing
    /// link if present; otherwise applies the account-linking policy (link to an
    /// existing email-matched user) or JIT-provisions a fresh user. Always
    /// (re)links the external identity. J2.4 conflict handling + J2.5 assurance.
    async fn resolve_or_provision(
        &self,
        pool: &PgPool,
        provider: &ProviderRow,
        subject: &str,
        mapped: &MappedClaims,
        tenant_id: &str,
    ) -> Result<ResolvedUser, Status> {
        // 1. Existing link wins (idempotent re-login).
        if let Some(existing) =
            store::get_external_identity(pool, tenant_id, &provider.provider_id, subject).await?
        {
            return Ok(ResolvedUser {
                user_id: existing.user_id,
                provisioned: false,
                linked: false,
                detail: "existing external identity".to_string(),
            });
        }

        // 2. Email conflict → apply account-linking policy.
        if !mapped.email.trim().is_empty() {
            if let Some((user_id, _verified)) =
                store::find_user_by_email(pool, tenant_id, &mapped.email).await?
            {
                match evaluate_account_linking(
                    &provider.account_linking_policy,
                    mapped.email_verified,
                ) {
                    AccountLinkDecision::Deny => {
                        return Err(idp_schema_already_exists_status(
                            "idp_account_linking",
                            "idp_email_account_already_exists",
                            "an account with this email already exists and linking is denied",
                        ));
                    }
                    AccountLinkDecision::LinkExisting => {
                        let row = store::upsert_external_identity(
                            pool,
                            tenant_id,
                            &provider.provider_id,
                            subject,
                            &user_id,
                            &mapped.email,
                            mapped.email_verified,
                        )
                        .await?;
                        return Ok(ResolvedUser {
                            user_id: row.user_id,
                            provisioned: false,
                            linked: true,
                            detail: "auto-linked to existing verified email".to_string(),
                        });
                    }
                    // Unknown/explicit policy values and unverified auto-link
                    // attempts do NOT silently link; require an explicit
                    // LinkIdentity call.
                    AccountLinkDecision::RequireExplicit => {
                        return Err(idp_account_linking_explicit_required_status());
                    }
                }
            }
        }

        // 3. JIT provisioning, gated by the provider policy (J2.4 / J3).
        let policy = JitPolicy::from_json(&provider.jit_policy_json);
        if let JitDecision::Reject(reason) = evaluate_jit(&policy, mapped) {
            return Err(idp_jit_provisioning_rejected_status(reason));
        }
        let project = if policy.default_project.is_empty() {
            String::new()
        } else {
            policy.default_project.clone()
        };
        let user_id = store::create_external_user(
            pool,
            tenant_id,
            &project,
            &provider.provider_id,
            subject,
            &mapped.email,
            &mapped.display_name,
            mapped.email_verified,
            "jit",
        )
        .await?;
        let row = store::upsert_external_identity(
            pool,
            tenant_id,
            &provider.provider_id,
            subject,
            &user_id,
            &mapped.email,
            mapped.email_verified,
        )
        .await?;
        events::emit(
            self.event_sink.as_ref(),
            events::IDENTITY_PROVISIONED,
            row.external_identity_id.clone(),
            tenant_id.to_string(),
            json!({ "provider_id": provider.provider_id, "subject": subject, "user_id": user_id }),
        )
        .await;
        Ok(ResolvedUser {
            user_id,
            provisioned: true,
            linked: false,
            detail: "JIT-provisioned new external user".to_string(),
        })
    }

    /// Resolve a SCIM user id (UUID external_identity_id OR subject) to its link.
    async fn find_scim_identity(
        &self,
        pool: &PgPool,
        tenant_id: &str,
        provider_id: &str,
        scim_user_id: &str,
    ) -> Result<Option<ExternalIdentityRow>, Status> {
        // SCIM-1 (bug_report.md G): ScimCreateUser returns id = external_identity_id
        // (a UUID), so the standard Create→{id}→Get/Patch/Put/Delete-by-id flow
        // passes that UUID. Resolve by external_identity_id first; otherwise fall
        // back to subject (the SCIM userName == subject case).
        if let Ok(eid) = uuid::Uuid::parse_str(scim_user_id.trim()) {
            if let Some(row) =
                store::get_external_identity_by_id(pool, tenant_id, provider_id, &eid.to_string())
                    .await?
            {
                return Ok(Some(row));
            }
        }
        store::get_external_identity(pool, tenant_id, provider_id, scim_user_id).await
    }
}

fn non_empty_json_obj(s: &str) -> String {
    if s.trim().is_empty() {
        "{}".to_string()
    } else {
        s.to_string()
    }
}

/// Keys of a group→role mapping object (the canonical configured group set).
fn group_keys(group_mapping_json: &str) -> Vec<String> {
    serde_json::from_str::<Value>(group_mapping_json.trim())
        .ok()
        .and_then(|v| v.as_object().map(|m| m.keys().cloned().collect()))
        .unwrap_or_default()
}

fn saml_assertion_to_claims(a: &saml::SamlAssertion) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("sub".to_string(), json!(a.name_id));
    obj.insert("name_id".to_string(), json!(a.name_id));
    if !a.authn_context.is_empty() {
        // Map the SAML AuthnContextClassRef into an `amr`-like hint so the shared
        // assurance derivation works for SAML too.
        let lc = a.authn_context.to_ascii_lowercase();
        let mut amr: Vec<String> = Vec::new();
        if lc.contains("password") {
            amr.push("pwd".to_string());
        }
        if lc.contains("mfa") || lc.contains("multifactor") || lc.contains("smartcard") {
            amr.push("mfa".to_string());
        }
        if lc.contains("x509") || lc.contains("smartcardpki") || lc.contains("hardware") {
            amr.push("hwk".to_string());
        }
        obj.insert("amr".to_string(), json!(amr));
        obj.insert("acr".to_string(), json!(a.authn_context));
    }
    for (k, vals) in &a.attributes {
        if vals.len() == 1 {
            obj.insert(k.clone(), json!(vals[0]));
        } else {
            obj.insert(k.clone(), json!(vals));
        }
    }
    Value::Object(obj)
}

fn scim_user_pb(id: &str, view: &scim::ScimUserView) -> idp_pb::ScimUser {
    idp_pb::ScimUser {
        id: id.to_string(),
        user_name: view.user_name.clone(),
        display_name: view.display_name.clone(),
        email: view.email.clone(),
        active: view.active,
        groups: view.groups.clone(),
        raw_json: json!({
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
            "id": id,
            "userName": view.user_name,
            "displayName": view.display_name,
            "active": view.active,
            "emails": [{ "value": view.email, "primary": true }],
            "meta": {
                "resourceType": "User",
                "location": format!("/scim/v2/Users/{id}"),
            },
        })
        .to_string(),
    }
}

fn scim_group_pb(id: &str, view: &scim::ScimGroupView) -> idp_pb::ScimGroup {
    idp_pb::ScimGroup {
        id: id.to_string(),
        display_name: view.display_name.clone(),
        members: view.members.clone(),
        raw_json: json!({
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
            "id": id,
            "displayName": view.display_name,
            "meta": {
                "resourceType": "Group",
                "location": format!("/scim/v2/Groups/{id}"),
            },
        })
        .to_string(),
    }
}

#[cfg(test)]
mod oidc_authn_mapping_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present")
            .to_bytes()
            .expect("typed detail trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "identity_provider");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn idp_internal_status_carries_typed_detail() {
        let status = idp_internal_status(
            "start_saml_login",
            "AuthnRequest build failed: invalid endpoint",
        );
        assert_internal_detail(
            &status,
            "start_saml_login",
            "AuthnRequest build failed: invalid endpoint",
        );
    }

    #[test]
    fn oidc_principal_uses_provider_claim_and_group_mapping() {
        let claims = json!({
            "oid": "subject-42",
            "roles": ["eng", "admins", "unknown"],
        });
        let claim_mapping = json!({
            "subject": "oid",
            "groups": "roles",
        })
        .to_string();
        let group_mapping = json!({
            "eng": "role:developer",
            "admins": ["role:admin", "role:auditor"],
        })
        .to_string();

        let principal = oidc_principal_from_verified_claims(
            "provider-1",
            "fallback-subject",
            "tenant-a",
            "project-a",
            vec!["udb:read".to_string()],
            &claim_mapping,
            &group_mapping,
            &claims,
        );

        assert_eq!(principal.principal_id, "provider-1:subject-42");
        assert_eq!(principal.subject, "subject-42");
        assert_eq!(principal.user_id, "subject-42");
        assert_eq!(principal.tenant_id, "tenant-a");
        assert_eq!(principal.project_id, "project-a");
        assert_eq!(principal.scopes, vec!["udb:read"]);
        assert_eq!(
            principal.roles,
            vec!["role:admin", "role:auditor", "role:developer"]
        );
        assert_eq!(principal.auth_method, "oidc");
    }

    #[test]
    fn oidc_principal_falls_back_to_verified_subject_when_mapping_is_empty() {
        let principal = oidc_principal_from_verified_claims(
            "provider-1",
            "verified-subject",
            "tenant-a",
            "",
            Vec::new(),
            r#"{"subject":"missing"}"#,
            "{}",
            &json!({}),
        );

        assert_eq!(principal.principal_id, "provider-1:verified-subject");
        assert_eq!(principal.subject, "verified-subject");
        assert!(principal.roles.is_empty());
    }

    #[test]
    fn idp_email_conflict_carries_schema_detail() {
        let err = idp_schema_already_exists_status(
            "idp_account_linking",
            "idp_email_account_already_exists",
            "an account with this email already exists and linking is denied",
        );
        assert_eq!(err.code(), tonic::Code::AlreadyExists);
        assert_eq!(
            err.message(),
            "an account with this email already exists and linking is denied"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "identity_provider");
        assert_eq!(detail.operation, "idp_account_linking");
        assert_eq!(
            detail.capability_required,
            "idp_email_account_already_exists"
        );
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }
}

// ── service wiring ────────────────────────────────────────────────────────────

impl DataBrokerService {
    /// Build the native `IdentityProviderService`, wired to the broker's Postgres
    /// pool and the shared auth outbox event sink (when a pool exists).
    pub(crate) fn build_identity_provider_service(&self) -> IdentityProviderServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam (extend_udb.md):
        // the backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime.native_store_pool_for_service("idp", true, "").ok();
        #[cfg(feature = "redis")]
        let jti_denylist = runtime.redis_clone().map(|redis| {
            crate::runtime::authn::revocation::JtiDenylist::new(
                redis,
                crate::runtime::security::SecurityConfig::current().jwt_access_ttl_secs,
            )
        });
        let event_sink: Arc<dyn AuthEventSink> = match pg_pool.clone() {
            Some(pool) => Arc::new(
                super::events::OutboxAuthEventSink::new(
                    pool.clone(),
                    runtime.config().cdc.outbox_relation(),
                )
                // Phase L3 task2: IdP events also fan out to the configured
                // immutable export sinks.
                .with_exports(super::audit_export::export_sinks_from_env(Some(&pool)))
                .with_metrics(self.metrics.clone()),
            ),
            None => noop_sink(),
        };
        let service = IdentityProviderServiceImpl::new()
            .with_runtime(runtime)
            .with_postgres(pg_pool)
            .with_event_sink(event_sink)
            .with_metrics(self.metrics.clone());
        #[cfg(feature = "redis")]
        let service = service.with_jti_denylist(jti_denylist);
        service
    }
}
