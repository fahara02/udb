use super::*;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::planning::broker::{DeletePlanRequest, build_delete_plan};
use crate::runtime::config::UdbConfig;
use crate::runtime::core::DataBrokerRuntime;
use serde_json::json;
use std::sync::Arc;

fn install_test_security() {
    crate::runtime::security::SecurityConfig::install_global(
        crate::runtime::security::SecurityConfig {
            tls_required: false,
            service_identity_required: false,
            mtls_required: false,
            allow_header_scopes: true,
            ..crate::runtime::security::SecurityConfig::default()
        },
    );
}

fn test_manifest() -> CatalogManifest {
    let col_id = ManifestColumn {
        field_name: "id".to_string(),
        column_name: "id".to_string(),
        proto_type: "string".to_string(),
        sql_type: "TEXT".to_string(),
        is_primary: true,
        ..ManifestColumn::default()
    };
    let col_tenant = ManifestColumn {
        field_name: "tenant_id".to_string(),
        column_name: "tenant_id".to_string(),
        proto_type: "string".to_string(),
        sql_type: "TEXT".to_string(),
        not_null: true,
        ..ManifestColumn::default()
    };
    let table = ManifestTable {
        message_name: "Payment".to_string(),
        schema: "payments".to_string(),
        table: "payments".to_string(),
        columns: vec![col_id, col_tenant],
        primary_key: vec!["id".to_string()],
        ..ManifestTable::default()
    };
    CatalogManifest {
        tables: vec![table],
        ..CatalogManifest::default()
    }
}

fn ready_service() -> DataBrokerService {
    install_test_security();
    let svc = DataBrokerService::with_runtime(test_manifest(), DataBrokerRuntime::planning_only());
    if let Ok(mut s) = svc.lifecycle_state.write() {
        *s = FsmState::Completed;
    }
    svc
}

#[test]
fn descriptor_set_exposes_databroker_reflection_surface() {
    let descriptor =
        <prost_types::FileDescriptorSet as prost::Message>::decode(UDB_FILE_DESCRIPTOR_SET)
            .expect("descriptor set should decode");
    assert!(descriptor.file.iter().any(|file| {
        file.package.as_deref() == Some("udb.services.v1")
            || file.package.as_deref() == Some("lifeplus.udb.services.v1")
    }));

    let service = descriptor
        .file
        .iter()
        .flat_map(|file| file.service.iter())
        .find(|service| service.name.as_deref() == Some("DataBroker"))
        .expect("DataBroker service should be in descriptor set");
    let methods = service
        .method
        .iter()
        .filter_map(|method| method.name.as_deref())
        .collect::<std::collections::BTreeSet<_>>();

    assert!(methods.contains("Select"));
    assert!(methods.contains("LookupMessageSchema"));
    assert!(methods.contains("GetHealthReport"));
}

#[tokio::test]
async fn service_runtime_snapshot_is_shared_across_clones() {
    let svc = DataBrokerService::with_runtime(test_manifest(), DataBrokerRuntime::planning_only());
    let clone = svc.clone();
    let next = DataBrokerRuntime::from_config(UdbConfig {
        default_limit: 321,
        ..UdbConfig::default()
    })
    .await;

    svc.runtime.store(Arc::new(next));

    assert_eq!(clone.runtime_snapshot().config().default_limit, 321);
}

#[test]
fn catalog_response_headers_carry_version_and_consistency() {
    let svc = ready_service();
    let response = svc.with_catalog_response_headers(
        Response::new(()),
        &crate::RequestContext {
            project_id: "default".to_string(),
            consistency: "read_your_writes".to_string(),
            primary_read: false,
            ..Default::default()
        },
    );
    let metadata = response.metadata();
    assert!(metadata.contains_key("x-udb-project-id"));
    assert_eq!(
        metadata
            .get("x-udb-consistency-mode")
            .and_then(|value| value.to_str().ok()),
        Some("read_your_writes")
    );
    assert_eq!(
        metadata
            .get("x-udb-primary-read")
            .and_then(|value| value.to_str().ok()),
        Some("false")
    );
}

#[test]
fn catalog_response_headers_report_read_fence_state() {
    let svc = ready_service();
    let fence = crate::runtime::consistency::ReadFence {
        min_outbox_lsn: "0/100".to_string(),
        max_wait_ms: 250,
        ..Default::default()
    };
    let response = svc.with_catalog_response_headers(
        Response::new(()),
        &crate::RequestContext {
            consistency: "read_your_writes".to_string(),
            read_fence_json: serde_json::to_string(&fence).unwrap(),
            ..Default::default()
        },
    );
    let metadata = response.metadata();
    assert_eq!(
        metadata
            .get("x-udb-read-fence-present")
            .and_then(|value| value.to_str().ok()),
        Some("true")
    );
    assert_eq!(
        metadata
            .get("x-udb-read-fence-honored")
            .and_then(|value| value.to_str().ok()),
        Some("true")
    );
}

#[tokio::test]
async fn mutation_response_headers_attach_write_receipt() {
    let svc = ready_service();
    let response = svc
        .with_mutation_response_headers(
            MutationResponse {
                mutation_id: "m1".to_string(),
                ..Default::default()
            },
            &crate::RequestContext::default(),
        )
        .await;
    let receipt_json = &response.get_ref().write_receipt_json;
    let receipt: crate::runtime::consistency::WriteReceipt =
        serde_json::from_str(receipt_json).unwrap();
    assert_eq!(
        receipt.manifest_checksum,
        svc.catalog.active().metadata.checksum
    );
    assert!(
        response
            .metadata()
            .get("x-udb-write-receipt")
            .and_then(|value| value.to_str().ok())
            .is_some()
    );
}

// ── Delete planning ───────────────────────────────────────────────────────

#[test]
fn delete_plan_rejects_unknown_message_type() {
    let manifest = test_manifest();
    let req = DeletePlanRequest {
        message_type: "NonExistent".to_string(),
        filter: json!({"id": {"eq": "x"}}),
        context: crate::RequestContext {
            tenant_id: "t1".to_string(),
            ..Default::default()
        },
    };
    let plan = build_delete_plan(&manifest, &req);
    assert!(
        !plan.errors.is_empty(),
        "expected error for unknown message_type"
    );
    assert!(
        plan.errors[0].contains("unknown message_type"),
        "got: {:?}",
        plan.errors
    );
}

#[test]
fn delete_plan_requires_filter_predicate() {
    let manifest = test_manifest();
    let req = DeletePlanRequest {
        message_type: "Payment".to_string(),
        filter: json!({}),
        context: crate::RequestContext {
            tenant_id: "t1".to_string(),
            ..Default::default()
        },
    };
    let plan = build_delete_plan(&manifest, &req);
    assert!(
        !plan.errors.is_empty(),
        "expected error for empty filter; got: {:?}",
        plan.errors
    );
}

#[test]
fn delete_plan_generates_valid_sql() {
    let manifest = test_manifest();
    // validate_write_context requires purpose + udb:write scope;
    // tenant isolation requires a filter on tenant_id.
    // Use $eq operator ("eq" is not a recognised SQL operator key).
    let req = DeletePlanRequest {
        message_type: "Payment".to_string(),
        filter: json!({"id": {"$eq": "abc"}, "tenant_id": {"$eq": "t1"}}),
        context: crate::RequestContext {
            tenant_id: "t1".to_string(),
            purpose: "test".to_string(),
            scopes: vec!["udb:write".to_string()],
            ..Default::default()
        },
    };
    let plan = build_delete_plan(&manifest, &req);
    assert!(
        plan.errors.is_empty(),
        "unexpected errors: {:?}",
        plan.errors
    );
    assert!(
        plan.sql.to_ascii_uppercase().starts_with("DELETE FROM"),
        "sql: {}",
        plan.sql
    );
    assert!(plan.sql.contains("payments"), "sql: {}", plan.sql);
}

// ── Resource admin: unknown backend → INVALID_ARGUMENT ───────────────────

#[tokio::test]
async fn ensure_resource_unknown_backend() {
    let rt = DataBrokerRuntime::planning_only();
    let err = rt
        .ensure_resource_backend("totally_nonexistent_backend", "ks", "{}")
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("totally_nonexistent_backend"),
        "got: {}",
        err.message()
    );
}

#[tokio::test]
async fn drop_resource_unknown_backend() {
    let rt = DataBrokerRuntime::planning_only();
    let err = rt
        .drop_resource_backend("totally_nonexistent_backend", "ks")
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn list_resources_unknown_backend() {
    let rt = DataBrokerRuntime::planning_only();
    let err = rt
        .list_resources_backend("totally_nonexistent_backend")
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ── Resource admin: unconfigured backend → FAILED_PRECONDITION ───────────

#[tokio::test]
async fn ensure_resource_mongodb_unconfigured() {
    let rt = DataBrokerRuntime::planning_only();
    let err = rt
        .ensure_resource_backend("mongodb", "col", "{}")
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("mongodb not configured"),
        "got: {}",
        err.message()
    );
}

#[tokio::test]
async fn drop_resource_neo4j_unconfigured() {
    let rt = DataBrokerRuntime::planning_only();
    let err = rt.drop_resource_backend("neo4j", "lbl").await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("neo4j not configured"),
        "got: {}",
        err.message()
    );
}

#[tokio::test]
async fn list_resources_clickhouse_unconfigured() {
    let rt = DataBrokerRuntime::planning_only();
    let err = rt.list_resources_backend("clickhouse").await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("clickhouse not configured"),
        "got: {}",
        err.message()
    );
}

#[tokio::test]
async fn list_resources_s3_unconfigured() {
    let rt = DataBrokerRuntime::planning_only();
    let err = rt.list_resources_backend("s3").await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

// ── GetCapabilities: supported_rpcs completeness ──────────────────────────

fn capabilities_request_with_tenant() -> Request<CapabilitiesRequest> {
    // Safety: test-only; no concurrent env mutation in this test module.
    unsafe {
        std::env::set_var("UDB_MTLS_REQUIRED", "false");
        std::env::set_var("UDB_ALLOW_HEADER_SCOPES", "1");
    }
    let mut req = Request::new(CapabilitiesRequest {
        context: None,
        project_id: String::new(),
    });
    req.metadata_mut()
        .insert("x-tenant-id", "test-tenant".parse().unwrap());
    req.metadata_mut()
        .insert("x-purpose", "admin".parse().unwrap());
    req.metadata_mut()
        .insert("x-scopes", "udb:admin".parse().unwrap());
    req
}

/// Like `ready_service` but with ABAC default-allow, so tests that exercise the
/// full RPC path (rather than auth denial) can reach the handler body.
fn open_service() -> DataBrokerService {
    install_test_security();
    let lifecycle = Arc::new(RwLock::new(FsmState::Completed));
    let policies = Arc::new(RwLock::new(Vec::new()));
    let metrics = Arc::new(PrometheusMetrics::new().expect("metrics"));
    DataBrokerService::with_runtime_and_state(
        test_manifest(),
        DataBrokerRuntime::planning_only(),
        lifecycle,
        policies,
        metrics,
        None,
        true, // abac_default_allow
    )
}

#[tokio::test]
async fn get_capabilities_includes_resource_admin_rpcs() {
    let svc = open_service();
    let resp = svc
        .get_capabilities(capabilities_request_with_tenant())
        .await
        .unwrap();
    let rpcs = &resp.get_ref().supported_rpcs;
    for name in [
        "EnsureResource",
        "DropResource",
        "ListResources",
        "GenericDispatch",
    ] {
        assert!(
            rpcs.contains(&name.to_string()),
            "missing '{name}' in supported_rpcs"
        );
    }
}

#[tokio::test]
async fn get_capabilities_includes_cdc_rpcs() {
    let svc = open_service();
    let resp = svc
        .get_capabilities(capabilities_request_with_tenant())
        .await
        .unwrap();
    let rpcs = &resp.get_ref().supported_rpcs;
    for name in ["PauseCdc", "ResumeCdc", "GetCdcStatus", "StepDownCdcLeader"] {
        assert!(
            rpcs.contains(&name.to_string()),
            "missing '{name}' in supported_rpcs"
        );
    }
}

#[tokio::test]
async fn get_capabilities_includes_policy_rpcs() {
    let svc = open_service();
    let resp = svc
        .get_capabilities(capabilities_request_with_tenant())
        .await
        .unwrap();
    let rpcs = &resp.get_ref().supported_rpcs;
    for name in [
        "ListPolicies",
        "PutPolicy",
        "DeletePolicy",
        "ReloadPolicies",
        "LintPolicies",
    ] {
        assert!(
            rpcs.contains(&name.to_string()),
            "missing '{name}' in supported_rpcs"
        );
    }
}

#[tokio::test]
async fn get_capabilities_includes_schema_registry_rpcs() {
    let svc = open_service();
    let resp = svc
        .get_capabilities(capabilities_request_with_tenant())
        .await
        .unwrap();
    let rpcs = &resp.get_ref().supported_rpcs;
    for name in ["LookupMessageSchema", "ListMessageSchemas"] {
        assert!(
            rpcs.contains(&name.to_string()),
            "missing '{name}' in supported_rpcs"
        );
    }
}

#[tokio::test]
async fn lookup_message_schema_returns_descriptor() {
    let svc = open_service();
    let mut req = Request::new(MessageSchemaLookupRequest {
        context: None,
        project_id: String::new(),
        message_type: "Payment".to_string(),
        client_catalog_version: String::new(),
    });
    req.metadata_mut()
        .insert("x-tenant-id", "test-tenant".parse().unwrap());
    req.metadata_mut()
        .insert("x-purpose", "schema".parse().unwrap());
    req.metadata_mut()
        .insert("x-scopes", "udb:admin".parse().unwrap());

    let resp = svc.lookup_message_schema(req).await.unwrap();
    let descriptor = resp
        .get_ref()
        .descriptor
        .as_ref()
        .expect("descriptor should be present");
    assert_eq!(descriptor.message_type, "Payment");
    assert_eq!(descriptor.table, "payments");
    assert!(descriptor.fields.iter().any(|field| field.name == "id"));
}

#[tokio::test]
async fn lookup_message_schema_rejects_cross_project_non_admin() {
    let svc = open_service();
    let mut req = Request::new(MessageSchemaLookupRequest {
        context: None,
        project_id: "other-project".to_string(),
        message_type: "Payment".to_string(),
        client_catalog_version: String::new(),
    });
    req.metadata_mut()
        .insert("x-tenant-id", "test-tenant".parse().unwrap());
    req.metadata_mut()
        .insert("x-project-id", "bound-project".parse().unwrap());
    req.metadata_mut()
        .insert("x-purpose", "schema".parse().unwrap());
    req.metadata_mut()
        .insert("x-scopes", "udb:read".parse().unwrap());

    let err = svc.lookup_message_schema(req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn list_message_schemas_returns_active_messages() {
    let svc = open_service();
    let mut req = Request::new(MessageSchemaListRequest {
        context: None,
        project_id: String::new(),
        client_catalog_version: String::new(),
    });
    req.metadata_mut()
        .insert("x-tenant-id", "test-tenant".parse().unwrap());
    req.metadata_mut()
        .insert("x-purpose", "schema".parse().unwrap());
    req.metadata_mut()
        .insert("x-scopes", "udb:admin".parse().unwrap());

    let resp = svc.list_message_schemas(req).await.unwrap();
    assert!(
        resp.get_ref()
            .message_types
            .contains(&"Payment".to_string()),
        "active message list should contain Payment"
    );
}

#[tokio::test]
async fn list_message_schemas_rejects_cross_project_non_admin() {
    let svc = open_service();
    let mut req = Request::new(MessageSchemaListRequest {
        context: None,
        project_id: "other-project".to_string(),
        client_catalog_version: String::new(),
    });
    req.metadata_mut()
        .insert("x-tenant-id", "test-tenant".parse().unwrap());
    req.metadata_mut()
        .insert("x-project-id", "bound-project".parse().unwrap());
    req.metadata_mut()
        .insert("x-purpose", "schema".parse().unwrap());
    req.metadata_mut()
        .insert("x-scopes", "udb:read".parse().unwrap());

    let err = svc.list_message_schemas(req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn get_capabilities_includes_backend_capability_matrix() {
    let svc = open_service();
    let resp = svc
        .get_capabilities(capabilities_request_with_tenant())
        .await
        .unwrap();
    let matrix = &resp.get_ref().backend_capabilities;
    // C9 complete: every backend in the enum is now wired. No more
    // metadata-only exclusions to assert against.
    let _ = matrix;
    #[cfg(feature = "redis")]
    {
        let redis = matrix
            .iter()
            .find(|entry| entry.backend == "redis")
            .expect("redis capability entry");
        assert!(redis.operations.contains(&"query".to_string()));
        assert!(redis.operations.contains(&"mutate".to_string()));
        assert_eq!(redis.unsupported_error_code, "UDB_UNSUPPORTED_OPERATION");
    }
}

#[test]
fn pagination_helpers_bound_limit_and_emit_next_token() {
    assert_eq!(bounded_list_limit(0), 100);
    assert_eq!(bounded_list_limit(5000), 1000);
    assert_eq!(page_offset("25"), 25);
    assert_eq!(page_offset("bad"), 0);
    assert_eq!(next_page_token(20, 10, 10), "30");
    assert_eq!(next_page_token(20, 10, 3), "");
}

#[test]
fn portal_permissions_distinguish_viewer_and_operator() {
    let svc = ready_service();
    let viewer = SecurityContext {
        scopes: vec!["udb:portal:viewer".to_string()],
        ..SecurityContext::default()
    };
    assert!(
        svc.require_portal_permission(&viewer, "GetAdminSummary", false)
            .is_ok()
    );
    assert_eq!(
        svc.require_portal_permission(&viewer, "RetrySagaCompensation", true)
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );

    let operator = SecurityContext {
        scopes: vec!["udb:portal:operator".to_string()],
        ..SecurityContext::default()
    };
    assert!(
        svc.require_portal_permission(&operator, "RetrySagaCompensation", true)
            .is_ok()
    );
}

// ── Broker v2 authz gate (UDB_AUTHZ_V2) — items 124-131 ──────────────────
// These drive `DataBrokerService::authorize()` with the v2 decision engine
// forced on per-instance (`set_authz_v2_override`), so the broker-level
// allow/deny path is exercised deterministically — no live Postgres, no gRPC
// server. The env flag `UDB_AUTHZ_V2` is a cached `OnceLock` and cannot vary
// per test, which is exactly why the per-instance override exists. The broker
// passes the raw RPC name (`Select`/`Upsert`/`PutPolicy`) as the operation, so
// a matching policy carries the same operation string.

fn v2_service(policies: Vec<crate::runtime::security::AbacPolicy>) -> DataBrokerService {
    let mut svc = ready_service(); // abac_default_allow = false
    if let Ok(mut p) = svc.abac_policies.write() {
        *p = policies;
    }
    svc.set_authz_v2_override(true);
    svc
}

fn billing_ctx(scopes: &[&str]) -> SecurityContext {
    SecurityContext {
        tenant_id: "acme".to_string(),
        purpose: "billing".to_string(),
        service_identity: "svc:billing".to_string(),
        scopes: scopes.iter().map(|s| s.to_string()).collect(),
        ..SecurityContext::default()
    }
}

fn allow_policy(operation: &str, scope: &str) -> crate::runtime::security::AbacPolicy {
    crate::runtime::security::AbacPolicy {
        effect: crate::runtime::security::PolicyEffect::Allow,
        service_identity: "svc:billing".to_string(),
        tenant_id: "*".to_string(),
        purpose: "billing".to_string(),
        message_type: "*".to_string(),
        operation: operation.to_string(),
        required_scope: scope.to_string(),
    }
}

#[tokio::test]
async fn broker_v2_select_allowed_with_matching_policy() {
    // 124: a Select policy granting the caller's scope/purpose → authorized.
    let svc = v2_service(vec![allow_policy("Select", "udb:read")]);
    let ctx = billing_ctx(&["udb:read"]);
    assert!(
        svc.authorize(&ctx, "Payment", "Select").await.is_ok(),
        "matching v2 policy must authorize Select"
    );
}

#[tokio::test]
async fn broker_v2_select_denied_without_policy() {
    // 125: no policy + deny-by-default → PermissionDenied.
    let svc = v2_service(vec![]);
    let ctx = billing_ctx(&["udb:read"]);
    let err = svc.authorize(&ctx, "Payment", "Select").await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn broker_v2_upsert_allowed_with_matching_policy() {
    // 126: an Upsert policy authorizes the matching write.
    let svc = v2_service(vec![allow_policy("Upsert", "udb:write")]);
    let ctx = billing_ctx(&["udb:write"]);
    assert!(
        svc.authorize(&ctx, "Payment", "Upsert").await.is_ok(),
        "matching v2 policy must authorize Upsert"
    );
}

#[tokio::test]
async fn broker_v2_admin_rpc_denied_without_grant() {
    // 127: a read grant does not authorize an admin mutation under v2 — the
    // operation selector must match, so PutPolicy is denied.
    let svc = v2_service(vec![allow_policy("Select", "udb:read")]);
    let ctx = billing_ctx(&["udb:read", "udb:admin"]);
    let err = svc
        .authorize(&ctx, "Policy", "PutPolicy")
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn broker_v2_external_roles_alone_do_not_bypass_policy() {
    // 128: a caller carrying external IdP "roles" as scopes but with no UDB
    // policy granting the operation is denied. External identity is mapped, but
    // UDB authorization is never bypassed by the upstream roles alone.
    let svc = v2_service(vec![]);
    let ctx = billing_ctx(&["role:admin", "role:billing-manager"]);
    let err = svc.authorize(&ctx, "Payment", "Select").await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn broker_v2_policy_reload_updates_decisions() {
    // 131: hot-reloading the shared ABAC store changes the decision without
    // rebuilding the service.
    let svc = v2_service(vec![]);
    let ctx = billing_ctx(&["udb:read"]);
    assert_eq!(
        svc.authorize(&ctx, "Payment", "Select")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied,
        "deny-by-default before any policy is loaded"
    );
    svc.abac_policies
        .write()
        .unwrap()
        .push(allow_policy("Select", "udb:read"));
    assert!(
        svc.authorize(&ctx, "Payment", "Select").await.is_ok(),
        "the reloaded grant must take effect on the next decision"
    );
}

#[tokio::test]
async fn broker_v2_matches_legacy_abac_decisions() {
    // 132 gate: the v2 decision engine must agree with the legacy `evaluate_abac`
    // path on allow/deny for the same inputs, so flipping the broker default to
    // v2 is safe. We run an identical matrix through both paths (toggled by the
    // per-instance override) and assert the outcomes match.
    let policies = vec![
        allow_policy("Select", "udb:read"),
        crate::runtime::security::AbacPolicy {
            effect: crate::runtime::security::PolicyEffect::Deny,
            service_identity: "svc:billing".to_string(),
            tenant_id: "*".to_string(),
            purpose: "billing".to_string(),
            message_type: "*".to_string(),
            operation: "Delete".to_string(),
            required_scope: String::new(),
        },
    ];
    let cases = [
        (billing_ctx(&["udb:read"]), "Payment", "Select"), // matching allow
        (billing_ctx(&[]), "Payment", "Select"),           // missing required scope
        (billing_ctx(&["udb:read"]), "Payment", "Delete"), // explicit deny wins
        (billing_ctx(&["udb:write"]), "Payment", "Upsert"), // no matching policy
    ];
    for (ctx, msg, op) in cases {
        let mut legacy = v2_service(policies.clone());
        legacy.set_authz_v2_override(false);
        let legacy_ok = legacy.authorize(&ctx, msg, op).await.is_ok();

        let v2 = v2_service(policies.clone()); // override = true
        let v2_ok = v2.authorize(&ctx, msg, op).await.is_ok();

        assert_eq!(
            legacy_ok, v2_ok,
            "v2 and legacy ABAC disagree for {msg}/{op} (legacy={legacy_ok}, v2={v2_ok})"
        );
    }
}

// ── Authorization: deny-by-default scope enforcement ─────────────────────

#[tokio::test]
async fn ensure_resource_handler_denied_without_policy() {
    let svc = ready_service(); // abac_default_allow = false, no policies
    let req = Request::new(ResourceAdminRequest {
        context: None,
        backend: "mongodb".to_string(),
        resource_name: "col".to_string(),
        spec_json: "{}".to_string(),
        idempotency_key: String::new(),
        dry_run: false,
    });
    let err = svc.ensure_resource(req).await.unwrap_err();
    // Without credentials: Unauthenticated (missing tenant_id) or PermissionDenied (ABAC deny)
    assert!(
        err.code() == tonic::Code::PermissionDenied
            || err.code() == tonic::Code::Unavailable
            || err.code() == tonic::Code::Unauthenticated,
        "expected auth denial, got {:?}",
        err.code()
    );
}

#[tokio::test]
async fn drop_resource_handler_denied_without_policy() {
    let svc = ready_service();
    let req = Request::new(ResourceAdminRequest {
        context: None,
        backend: "neo4j".to_string(),
        resource_name: "label".to_string(),
        spec_json: String::new(),
        idempotency_key: String::new(),
        dry_run: false,
    });
    let err = svc.drop_resource(req).await.unwrap_err();
    assert!(
        err.code() == tonic::Code::PermissionDenied
            || err.code() == tonic::Code::Unavailable
            || err.code() == tonic::Code::Unauthenticated,
        "expected auth denial, got {:?}",
        err.code()
    );
}

#[tokio::test]
async fn list_resources_handler_denied_without_policy() {
    let svc = ready_service();
    let req = Request::new(ResourceAdminRequest {
        context: None,
        backend: "clickhouse".to_string(),
        resource_name: String::new(),
        spec_json: String::new(),
        idempotency_key: String::new(),
        dry_run: false,
    });
    let err = svc.list_resources(req).await.unwrap_err();
    assert!(
        err.code() == tonic::Code::PermissionDenied
            || err.code() == tonic::Code::Unavailable
            || err.code() == tonic::Code::Unauthenticated,
        "expected auth denial, got {:?}",
        err.code()
    );
}
