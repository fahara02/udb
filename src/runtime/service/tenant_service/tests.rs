//! Unit guards for the native `TenantService`: request-body cross-tenant
//! rejection, field-violation shapes, typed capability/not-found/internal
//! details, the contract-declared event topic/type pairing and no-secrets
//! payloads, and the fail-closed `ListTenants` scope resolution. Copied verbatim
//! from the former god file; imports are explicit (no `use super::*`).

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::proto::udb::core::tenant::services::v1 as tenant_pb;
use crate::proto::udb::core::tenant::services::v1::tenant_service_server::TenantService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
use crate::runtime::service::method_security::VerifiedClaimContext;

use super::TenantServiceImpl;
use super::config::{
    DEFAULT_TENANT_LIST_PAGE_SIZE, DEFAULT_TENANT_TYPE_DB, EVENT_TYPE_TENANT_CONFIG_UPDATED,
    EVENT_TYPE_TENANT_CREATED, EVENT_TYPE_TENANT_UPDATED, TENANT_STATUS_ACTIVE_DB,
    TOPIC_TENANT_CONFIG_UPDATED, TOPIC_TENANT_CREATED, TOPIC_TENANT_UPDATED,
};
use super::errors::{tenant_capability_status, tenant_internal_status, tenant_not_found_status};
use super::events::{tenant_config_event_payload, tenant_lifecycle_event_payload};
use super::model::{config_type_to_db, tenant_status_to_db, tenant_type_to_db};
use super::store::{list_tenants_scope, list_tenants_subtree_predicate};

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

fn assert_single_field_violation(status: &Status, field: &str, description: &str) {
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, field);
    assert_eq!(detail.field_violations[0].description, description);
}

fn assert_schema_not_found_detail(status: &Status, operation: &str) {
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert_eq!(status.message(), "tenant not found");
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Schema as i32);
    assert_eq!(detail.backend, "tenant");
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.capability_required, "tenant_not_found");
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Internal as i32);
    assert_eq!(detail.backend, "tenant");
    assert_eq!(detail.operation, operation);
    assert!(detail.capability_required.is_empty());
    assert!(detail.policy_decision_id.is_empty());
    assert!(detail.field_violations.is_empty());
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

/// A caller scoped to tenant-a must not read another tenant by putting a
/// foreign tenant_id in the request BODY; the scope guard rejects this before
/// any pool/DB access (no Postgres needed).
#[tokio::test]
async fn get_tenant_rejects_cross_tenant_body() {
    let svc = TenantServiceImpl::new(); // no pool, no channels (admit no-op)
    let mut request = Request::new(tenant_pb::GetTenantRequest {
        tenant_id: "tenant-b".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .get_tenant(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn create_tenant_missing_code_and_name_carries_field_violations() {
    let svc = TenantServiceImpl::new(); // no pool, no channels (admit no-op)
    let request = Request::new(tenant_pb::CreateTenantRequest {
        code: "  ".to_string(),
        name: String::new(),
        ..Default::default()
    });
    let err = svc
        .create_tenant(request)
        .await
        .expect_err("missing create fields must be rejected before pool access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "code and name are required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 2);
    assert_eq!(detail.field_violations[0].field, "code");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty tenant code"
    );
    assert_eq!(detail.field_violations[1].field, "name");
    assert_eq!(
        detail.field_violations[1].description,
        "must be a non-empty tenant name"
    );
}

#[tokio::test]
async fn purge_tenant_missing_tenant_id_carries_field_violation() {
    let svc = TenantServiceImpl::new(); // no pool/manifest; validation must fire first
    let request = Request::new(tenant_pb::PurgeTenantRequest {
        tenant_id: "  ".to_string(),
        confirmation_token: "confirm".to_string(),
        ..Default::default()
    });
    let err = svc
        .purge_tenant(request)
        .await
        .expect_err("missing tenant_id must be rejected before manifest/pool access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "tenant_id is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "tenant_id");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty tenant id"
    );
}

#[tokio::test]
async fn purge_tenant_missing_confirmation_token_carries_field_violation() {
    let svc = TenantServiceImpl::new(); // no pool/manifest; validation must fire first
    let tenant_id = "11111111-1111-1111-1111-111111111111";
    let mut request = Request::new(tenant_pb::PurgeTenantRequest {
        tenant_id: tenant_id.to_string(),
        confirmation_token: " ".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static(tenant_id));
    let err = svc
        .purge_tenant(request)
        .await
        .expect_err("missing confirmation_token must be rejected before manifest/pool access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "PurgeTenant is an irreversible hard delete; confirmation_token is required"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "confirmation_token");
    assert_eq!(
        detail.field_violations[0].description,
        "must be present to purge tenant data"
    );
}

#[tokio::test]
async fn update_tenant_config_missing_key_carries_field_violation() {
    let svc = TenantServiceImpl::new(); // no runtime, no channels (admit no-op)
    let tenant_id = "11111111-1111-1111-1111-111111111111";
    let mut request = Request::new(tenant_pb::UpdateTenantConfigRequest {
        tenant_id: tenant_id.to_string(),
        config_key: "  ".to_string(),
        config_value: "on".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static(tenant_id));
    let err = svc
        .update_tenant_config(request)
        .await
        .expect_err("missing config_key must be rejected before runtime access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "config_key is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "config_key");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty config key"
    );
}

#[test]
fn tenant_enum_normalizers_carry_field_violations() {
    let tenant_type =
        tenant_type_to_db("enterprise", "ORGANIZATION").expect_err("unknown tenant type must fail");
    assert_eq!(tenant_type.message(), "unknown tenant type: ENTERPRISE");
    assert_single_field_violation(&tenant_type, "type", "unsupported tenant type ENTERPRISE");

    let tenant_status =
        tenant_status_to_db("paused", "ACTIVE").expect_err("unknown tenant status must fail");
    assert_eq!(tenant_status.message(), "unknown tenant status: PAUSED");
    assert_single_field_violation(&tenant_status, "status", "unsupported tenant status PAUSED");

    let config_type =
        config_type_to_db("object", "STRING").expect_err("unknown config type must fail");
    assert_eq!(config_type.message(), "unknown config type: OBJECT");
    assert_single_field_violation(&config_type, "type", "unsupported config type OBJECT");
}

#[test]
fn tenant_missing_setup_capabilities_carry_typed_detail() {
    for (operation, capability, message) in [
        (
            "purge_tenant",
            "catalog_manifest",
            "tenant service requires the catalog manifest for purge",
        ),
        (
            "native_entity_dispatch",
            "runtime_native_entity_dispatch",
            "tenant service requires runtime native entity dispatch",
        ),
        (
            "postgres_store",
            "postgres_store",
            "tenant service requires a Postgres-backed store (no PG pool configured)",
        ),
    ] {
        let err = tenant_capability_status(operation, capability, message);
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(err.message(), message);
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "tenant");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability);
        assert!(!detail.retryable);
    }
}

#[test]
fn tenant_not_found_statuses_carry_schema_detail() {
    for operation in ["get_tenant", "update_tenant"] {
        assert_schema_not_found_detail(&tenant_not_found_status(operation), operation);
    }
}

#[test]
fn tenant_internal_status_carries_typed_detail() {
    assert_internal_detail(
        &tenant_internal_status(
            "resolve_tenant_after_create",
            "resolve tenant after create failed: database is unavailable",
        ),
        "resolve_tenant_after_create",
        "resolve tenant after create failed: database is unavailable",
    );
}

/// The three lifecycle emits use the versioned runtime topics paired with the
/// EXACT proto-declared `method_event_contract.event_type` strings
/// (tenant_service.proto) — the topic/type pairing is load-bearing for audit
/// traceability, so pin both sides byte-for-byte.
#[test]
fn tenant_event_topic_and_type_pairs_follow_the_declared_contract() {
    let pairs = [
        (TOPIC_TENANT_CREATED, EVENT_TYPE_TENANT_CREATED),
        (TOPIC_TENANT_UPDATED, EVENT_TYPE_TENANT_UPDATED),
        (
            TOPIC_TENANT_CONFIG_UPDATED,
            EVENT_TYPE_TENANT_CONFIG_UPDATED,
        ),
    ];
    for (topic, event_type) in pairs {
        // Versioned dot topic in the tenant namespace (tenant-scoped + covered
        // by the security-sensitive `udb.tenant.` compliance prefix).
        assert!(topic.starts_with("udb.tenant."), "topic {topic}");
        assert!(topic.ends_with(".v1"), "topic {topic}");
        assert!(
            crate::runtime::cdc::tenant_scoped_topic(topic),
            "topic {topic}"
        );
        // Proto-declared event type, never invented at the emit site.
        assert!(event_type.starts_with("tenant."), "event type {event_type}");
    }
    assert_eq!(TOPIC_TENANT_CREATED, "udb.tenant.created.v1");
    assert_eq!(TOPIC_TENANT_UPDATED, "udb.tenant.updated.v1");
    assert_eq!(TOPIC_TENANT_CONFIG_UPDATED, "udb.tenant.config-updated.v1");
    assert_eq!(EVENT_TYPE_TENANT_CREATED, "tenant.CreateTenant");
    assert_eq!(EVENT_TYPE_TENANT_UPDATED, "tenant.UpdateTenant");
    assert_eq!(
        EVENT_TYPE_TENANT_CONFIG_UPDATED,
        "tenant.UpdateTenantConfig"
    );
}

/// Event payloads carry identifiers + status (config: the key) ONLY — no
/// config/branding bodies and no config VALUE (it may hold secrets).
#[test]
fn tenant_event_payloads_carry_identifiers_only() {
    let lifecycle = tenant_lifecycle_event_payload("tenant-1", "acme", "ACTIVE");
    let mut keys: Vec<&str> = lifecycle
        .as_object()
        .expect("lifecycle payload is an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["code", "status", "tenant_id"]);
    assert_eq!(lifecycle["tenant_id"], "tenant-1");
    assert_eq!(lifecycle["code"], "acme");
    assert_eq!(lifecycle["status"], "ACTIVE");

    let config = tenant_config_event_payload("tenant-1", "features.beta");
    let mut keys: Vec<&str> = config
        .as_object()
        .expect("config payload is an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["config_key", "tenant_id"]);
    assert!(
        config.get("config_value").is_none(),
        "config VALUE must never reach the outbox payload"
    );
}

/// The non-admin ListTenants filter restricts to the caller's own row plus
/// direct children, text-compared so a malformed claim tenant matches nothing.
#[test]
fn non_admin_list_filter_includes_subtree_predicate() {
    let predicate = list_tenants_subtree_predicate("\"tenant_id\"", "\"parent_tenant_id\"", "$3");
    assert_eq!(
        predicate,
        "(\"tenant_id\"::text = $3 OR \"parent_tenant_id\"::text = $3)"
    );
}

#[test]
fn list_tenants_scope_restricts_non_admin_to_own_subtree() {
    let ctx = crate::runtime::service::method_security::test_claim_context(
        "user-1",
        "tenant-a",
        "",
        &["udb:read"],
        &["member"],
    );
    let scope = list_tenants_scope(true, &ctx).expect("tenant-bound non-admin may list");
    // Admission is charged to the caller's REAL claim tenant, not "".
    assert_eq!(scope.admit_tenant, "tenant-a");
    // And the row set is anchored on the same claim tenant.
    assert_eq!(scope.subtree_of.as_deref(), Some("tenant-a"));
}

#[test]
fn list_tenants_scope_keeps_cross_tenant_admin_unscoped() {
    // Broad admin scope.
    let ctx = crate::runtime::service::method_security::test_claim_context(
        "op-1",
        "tenant-root",
        "",
        &["udb:admin"],
        &[],
    );
    let scope = list_tenants_scope(true, &ctx).expect("admin may list");
    assert_eq!(scope.admit_tenant, "tenant-root");
    assert!(scope.subtree_of.is_none(), "admin list stays unscoped");
    // Platform-admin role, tenant-less claim.
    let ctx = crate::runtime::service::method_security::test_claim_context(
        "op-2",
        "",
        "",
        &[],
        &["platform_admin"],
    );
    let scope = list_tenants_scope(true, &ctx).expect("platform admin may list");
    assert_eq!(scope.admit_tenant, "");
    assert!(scope.subtree_of.is_none());
}

#[test]
fn list_tenants_scope_fails_closed_for_tenantless_non_admin() {
    let ctx = crate::runtime::service::method_security::test_claim_context(
        "user-1",
        "  ",
        "",
        &["udb:read"],
        &[],
    );
    let err = list_tenants_scope(true, &ctx)
        .expect_err("tenant-less non-admin must not enumerate the platform");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.policy_decision_id, "tenant_list_scope_required");
}

#[test]
fn list_tenants_scope_without_claim_context_stays_unscoped() {
    // No claim context installed = in-process/trusted caller (the tower layer
    // ALWAYS installs one for over-the-wire requests): historical behavior.
    let scope = list_tenants_scope(false, &VerifiedClaimContext::default())
        .expect("in-process caller keeps the unscoped list");
    assert_eq!(scope.admit_tenant, "");
    assert!(scope.subtree_of.is_none());
}

/// 16.4 audit hardcodes → named consts, with the SAME values (no behavior change).
#[test]
fn tenant_named_defaults_preserve_prior_literals() {
    assert_eq!(DEFAULT_TENANT_TYPE_DB, "ORGANIZATION");
    assert_eq!(TENANT_STATUS_ACTIVE_DB, "ACTIVE");
    assert_eq!(DEFAULT_TENANT_LIST_PAGE_SIZE, 50);
}
