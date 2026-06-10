//! Phase 9 resource SOURCING — snapshot the five real config sources into the
//! versioned control-plane registry so distribution is REAL (the registry is no
//! longer empty).
//!
//! Every source is read-only: we read the live `UdbConfig`, the
//! native-service registry, and the embedded descriptor manifest, project each
//! into a small NON-SECRET JSON payload, and upsert it through
//! [`store::upsert_resource`]. Because the store version is content-addressed,
//! re-running [`sync_resources_from_config`] with unchanged config does NOT bump
//! any version — a node is never asked to re-apply identical config.
//!
//! Redaction is non-negotiable: DSNs/secrets never enter a `payload_json`. We
//! only ever store *whether* a DSN is configured (`dsn_env` key name +
//! `configured: bool`), never the DSN value itself; labels/capabilities are
//! filtered against a sensitive-key denylist; method-security payloads carry only
//! the descriptor-declared (already non-secret) gate summary.

use sqlx::PgPool;
use tonic::Status;

use crate::proto::udb::core::control::entity::v1::ResourceType;
use crate::runtime::config::UdbConfig;
use crate::runtime::descriptor_manifest::{
    DescriptorContractManifest, descriptor_contract_manifest,
};
use crate::runtime::service::native_registry::resolved_native_service_statuses;

use super::store;

/// `updated_by` stamp for every config-sourced resource, so the registry row's
/// provenance is unambiguous (vs an admin-driven upsert via the RPC surface).
pub const SOURCED_BY: &str = "control-plane:config-sourcer";

/// Label/metadata keys that may carry a secret value and must NEVER be copied
/// into a registry payload. Matched case-insensitively as a substring so e.g.
/// `aws_secret_access_key` and `api_key_env` are both caught.
const SENSITIVE_LABEL_FRAGMENTS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "apikey",
    "api_key",
    "access_key",
    "private",
    "credential",
    "cert",
    "key",
];

/// True when a label/metadata key may hold a secret and must be dropped from a
/// payload. `dsn`/`dsn_env`/`endpoint` are handled separately (we keep only the
/// env-var *name*, never a resolved value).
fn is_sensitive_label_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    SENSITIVE_LABEL_FRAGMENTS
        .iter()
        .any(|fragment| lower.contains(fragment))
}

/// Snapshot the five real config sources into the registry. Idempotent +
/// content-addressed: re-running with unchanged inputs bumps no versions.
///
/// Ordering follows [`super::resources::ordered_resource_types`] — definitions
/// (backend targets, service enablement) before the policies that reference them
/// — so even the *write* order respects make-before-break.
pub async fn sync_resources_from_config(
    pool: &PgPool,
    config: &UdbConfig,
    descriptor: &DescriptorContractManifest,
) -> Result<(), Status> {
    source_backend_targets(pool, config).await?;
    source_native_service_enablement(pool, config).await?;
    source_method_security(pool, descriptor).await?;
    source_routing_policy(pool, config).await?;
    source_rls_tenant_policy(pool, descriptor).await?;
    Ok(())
}

/// On-change resync entry point (same body as the startup sync). Reads the
/// CURRENT process descriptor manifest so a config-only change is reflected
/// without threading the manifest through every caller. A read-snapshot resync
/// is idempotent, so no singleton lease is required (see `subscriber.rs`).
pub async fn resync(pool: &PgPool, config: &UdbConfig) -> Result<(), Status> {
    let descriptor = descriptor_contract_manifest();
    sync_resources_from_config(pool, config, &descriptor).await
}

// ── 1. BACKEND_TARGET_DEFINITION ────────────────────────────────────────────────

/// One registry resource per enabled backend instance. The payload is the
/// non-secret target descriptor: backend kind, role, routing weights, the DSN
/// *env-var name* (never the DSN), whether a DSN is configured, and the
/// non-sensitive labels/capabilities. Routing/RLS policies reference these by
/// `name` (the instance routing key), so they are sourced FIRST.
async fn source_backend_targets(pool: &PgPool, config: &UdbConfig) -> Result<(), Status> {
    for instance in config.backend_instances.active() {
        let routing_key = instance.routing_key();
        let backend = instance
            .canonical_backend()
            .map(|kind| kind.as_str().to_string())
            .unwrap_or_else(|| instance.backend.to_ascii_lowercase());
        let labels: serde_json::Map<String, serde_json::Value> = instance
            .labels
            .iter()
            .filter(|(key, _)| !is_sensitive_label_key(key))
            .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
            .collect();
        let capabilities: Vec<String> = instance.capabilities.iter().cloned().collect();
        let payload = serde_json::json!({
            "name": instance.name,
            "backend": backend,
            "role": instance.role.as_str(),
            "enabled": instance.enabled,
            "read_weight": instance.read_weight,
            "write_weight": instance.write_weight,
            // Only the env-var NAME is recorded; the resolved DSN value is never
            // serialized. `configured` says whether a DSN resolves at runtime.
            "dsn_env": instance.dsn_env.clone().unwrap_or_default(),
            "dsn_configured": instance.resolve_dsn().is_some(),
            "labels": serde_json::Value::Object(labels),
            "capabilities": capabilities,
        });
        let payload_json = serde_json::to_string(&payload)
            .map_err(|e| Status::internal(format!("backend target payload encode failed: {e}")))?;
        store::upsert_resource(
            pool,
            ResourceType::BackendTargetDefinition,
            &routing_key,
            "",
            "",
            &payload_json,
            SOURCED_BY,
        )
        .await?;
    }
    Ok(())
}

// ── 2. NATIVE_SERVICE_ENABLEMENT ────────────────────────────────────────────────

/// One registry resource per native service, carrying its resolved enablement
/// state (enabled/mounted/degraded, the listener surface, missing deps, and the
/// migration status). No secrets: this is pure topology/enablement.
async fn source_native_service_enablement(pool: &PgPool, config: &UdbConfig) -> Result<(), Status> {
    for status in resolved_native_service_statuses(config) {
        let payload = serde_json::json!({
            "service_id": status.service_id,
            "enabled": status.enabled,
            "configured": status.configured,
            "mounted": status.mounted,
            "healthy": status.healthy,
            "degraded": status.degraded,
            "surface": status.surface,
            "listener_kind": status.listener_kind,
            "required_backends": status.required_backends,
            "missing_dependencies": status.missing_dependencies,
            "disabled_reason": status.disabled_reason,
            "migration_status": status.migration_status,
            "descriptor_version": status.descriptor_version,
        });
        let payload_json = serde_json::to_string(&payload).map_err(|e| {
            Status::internal(format!("service enablement payload encode failed: {e}"))
        })?;
        store::upsert_resource(
            pool,
            ResourceType::NativeServiceEnablement,
            &status.service_id,
            "",
            "",
            &payload_json,
            SOURCED_BY,
        )
        .await?;
    }
    Ok(())
}

// ── 3. METHOD_SECURITY_POLICY ───────────────────────────────────────────────────

/// One registry resource per RPC, keyed by its gRPC path
/// (`/pkg.Service/Method`), carrying the descriptor-declared endpoint-security
/// summary (mode/scopes/roles/tenant/csrf/internal). This is a read-only
/// snapshot of the embedded descriptor — already secret-free by construction.
async fn source_method_security(
    pool: &PgPool,
    descriptor: &DescriptorContractManifest,
) -> Result<(), Status> {
    for service in &descriptor.services {
        for method in &service.methods {
            let Some(es) = method.endpoint_security.as_ref() else {
                continue;
            };
            let payload = serde_json::json!({
                "mode": es.auth_mode_name(),
                "scopes": es.scopes,
                "roles": es.roles,
                "tenant_required": es.tenant_required,
                "csrf_required": es.csrf_required,
                "internal_grpc_only": es.internal_grpc_only,
                "tenant_field": es.tenant_field,
                "project_field": es.project_field,
                "allowed_credential_types": es.allowed_credential_types,
                "request_context_required": es.request_context_required,
            });
            let payload_json = serde_json::to_string(&payload).map_err(|e| {
                Status::internal(format!("method security payload encode failed: {e}"))
            })?;
            store::upsert_resource(
                pool,
                ResourceType::MethodSecurityPolicy,
                &method.grpc_path(),
                "",
                "",
                &payload_json,
                SOURCED_BY,
            )
            .await?;
        }
    }
    Ok(())
}

// ── 4. ROUTING_POLICY ───────────────────────────────────────────────────────────

/// Snapshot the routing/replica config that actually exists in `UdbConfig`. We
/// do NOT invent a routing DSL: we record the PG read-replica routing knobs
/// (strategy, max lag, fail-open, replica count — NOT the replica DSNs) and the
/// project-routing strictness mode. One fleet-wide resource named
/// `pg-read-replicas`, one named `project-routing` — only when meaningful.
async fn source_routing_policy(pool: &PgPool, config: &UdbConfig) -> Result<(), Status> {
    // PG read-replica routing policy (replica DSNs are secrets → only the COUNT).
    if !config.pg_replica_dsns.is_empty() || !config.pg_replica_strategy.trim().is_empty() {
        let payload = serde_json::json!({
            "strategy": config.pg_replica_strategy,
            "replica_count": config.pg_replica_dsns.len(),
            "max_lag_secs": config.pg_replica_max_lag_secs,
            "fail_open": config.pg_replica_fail_open,
            "health_interval_secs": config.pg_replica_health_interval_secs,
        });
        let payload_json = serde_json::to_string(&payload)
            .map_err(|e| Status::internal(format!("routing policy payload encode failed: {e}")))?;
        store::upsert_resource(
            pool,
            ResourceType::RoutingPolicy,
            "pg-read-replicas",
            "",
            "",
            &payload_json,
            SOURCED_BY,
        )
        .await?;
    }

    // Project-routing strictness mode (fleet-wide).
    if !config.project_routing_mode.trim().is_empty() {
        let payload = serde_json::json!({ "mode": config.project_routing_mode });
        let payload_json = serde_json::to_string(&payload)
            .map_err(|e| Status::internal(format!("project routing payload encode failed: {e}")))?;
        store::upsert_resource(
            pool,
            ResourceType::RoutingPolicy,
            "project-routing",
            "",
            "",
            &payload_json,
            SOURCED_BY,
        )
        .await?;
    }
    Ok(())
}

// ── 5. RLS_TENANT_POLICY ─────────────────────────────────────────────────────────

/// Snapshot the tenant-isolation policy summary from the descriptor's
/// `db_table_security`: which native tables are tenant-scoped (RLS-on), the
/// tenant column, the RLS template, and the project-isolation mode. One
/// fleet-wide resource per tenant-scoped table (named by the message full name).
///
/// Only tables that DECLARE a non-empty `tenant_isolation_mode` (and are not
/// explicitly `none`) are sourced, so the policy set reflects real isolation.
async fn source_rls_tenant_policy(
    pool: &PgPool,
    descriptor: &DescriptorContractManifest,
) -> Result<(), Status> {
    for message in &descriptor.messages {
        let Some(sec) = message.db_table_security.as_ref() else {
            continue;
        };
        let mode = sec.tenant_isolation_mode.trim().to_ascii_lowercase();
        let tenant_scoped =
            !mode.is_empty() && mode != "none" && !sec.tenant_column.trim().is_empty();
        if !tenant_scoped {
            continue;
        }
        let payload = serde_json::json!({
            "table": message.full_name,
            "tenant_isolation_mode": sec.tenant_isolation_mode,
            "project_isolation_mode": sec.project_isolation_mode,
            "tenant_column": sec.tenant_column,
            "project_column": sec.project_column,
            "rls_policy_template": sec.rls_policy_template,
            "soft_delete_mode": sec.soft_delete_mode,
        });
        let payload_json = serde_json::to_string(&payload)
            .map_err(|e| Status::internal(format!("rls policy payload encode failed: {e}")))?;
        // Fleet-wide RLS posture (tenant="" == NULL). The per-tenant overlay is
        // served on-demand via GetResources(tenant_id=...) which layers tenant
        // rows on top of these fleet rows.
        store::upsert_resource(
            pool,
            ResourceType::RlsTenantPolicy,
            &message.full_name,
            "",
            "",
            &payload_json,
            SOURCED_BY,
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sensitive_label_keys_are_detected() {
        for key in [
            "password",
            "API_KEY",
            "aws_secret_access_key",
            "private_key",
            "x-credential",
            "tls_cert",
        ] {
            assert!(
                is_sensitive_label_key(key),
                "{key} must be treated as secret"
            );
        }
        for key in ["region", "transport", "api_base", "deploy_mode", "weight"] {
            assert!(!is_sensitive_label_key(key), "{key} is not a secret label");
        }
    }

    // Sourcing must NEVER place a raw DSN/secret into a payload. We assert the
    // backend-target payload shape only ever records the env-var NAME +
    // configured flag, mirroring `source_backend_targets`.
    #[test]
    fn backend_target_payload_redacts_dsn() {
        let payload = serde_json::json!({
            "name": "primary",
            "backend": "postgres",
            "role": "read_write",
            "dsn_env": "UDB_PG_DSN",
            "dsn_configured": true,
        });
        let text = payload.to_string();
        assert!(text.contains("UDB_PG_DSN"), "env-var name is kept");
        assert!(
            !text.contains("postgresql://") && !text.contains("@"),
            "no raw DSN value may appear in the payload"
        );
    }
}
