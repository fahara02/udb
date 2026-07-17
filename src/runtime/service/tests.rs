use super::*;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::planning::broker::{DeletePlanRequest, build_delete_plan};
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::config::UdbConfig;
use crate::runtime::core::DataBrokerRuntime;
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
use serde_json::json;
use std::sync::Arc;
use tonic::Status;

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

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("typed detail trailer is present");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

fn assert_validation_field(status: &Status, field: &str, description: &str) {
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, field);
    assert_eq!(detail.field_violations[0].description, description);
}

fn assert_capability_detail(
    status: &Status,
    backend: &str,
    operation: &str,
    capability_required: &str,
    message: &str,
) {
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Capability as i32);
    assert_eq!(detail.backend, backend);
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.capability_required, capability_required);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
    assert!(detail.field_violations.is_empty());
}

fn assert_policy_detail(status: &Status, operation: &str, policy_decision_id: &str, message: &str) {
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(status.message(), message);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.policy_decision_id, policy_decision_id);
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
    assert!(detail.field_violations.is_empty());
}

#[test]
fn catalog_payload_version_preserves_active_version_for_roundtrip_manifest() {
    let manifest = CatalogManifest {
        generator_version: "3".to_string(),
        checksum_sha256: "abc123".to_string(),
        ..CatalogManifest::default()
    };
    let bytes = serde_json::to_vec(&manifest).expect("manifest json");

    assert_eq!(catalog_payload_version(&bytes, &manifest, "1.0.0"), "1.0.0");
}

#[test]
fn parse_catalog_manifest_payload_empty_carries_field_violation() {
    let err = parse_catalog_manifest_payload(&[])
        .expect_err("empty manifest payload must fail before catalog reload");

    assert_eq!(err.message(), "manifest_json is required");
    assert_validation_field(
        &err,
        "manifest_json",
        "must contain a CatalogManifest JSON payload",
    );
}

#[test]
fn parse_catalog_manifest_payload_invalid_json_carries_field_violation() {
    let err = parse_catalog_manifest_payload(b"{")
        .expect_err("invalid manifest payload must fail before catalog reload");

    assert!(
        err.message()
            .starts_with("manifest_json is not a CatalogManifest:")
    );
    assert_validation_field(&err, "manifest_json", "must decode as a CatalogManifest");
}

#[test]
fn unknown_backend_status_carries_field_violation() {
    let err = unknown_backend_status("totally_nonexistent_backend");

    assert_eq!(
        err.message(),
        "unknown backend 'totally_nonexistent_backend'"
    );
    assert_validation_field(&err, "backend", "must name a supported backend");
}

#[test]
fn unknown_generic_operation_status_carries_field_violation() {
    let err = unknown_generic_operation_status("teleport");

    assert_eq!(
        err.message(),
        "unknown operation 'teleport'; allowed: ping, probe, ensure_resource, drop_resource, list_resources, query, mutate, transaction, search, get_object, put_object, delete_object"
    );
    assert_validation_field(
        &err,
        "operation",
        "must be a supported generic dispatch operation",
    );
}

#[test]
fn backend_runtime_unsupported_status_carries_capability_detail() {
    let err = backend_runtime_unsupported_status(
        "azureblob",
        "query",
        "Azure Blob support is not available in this binary".to_string(),
    );

    assert_capability_detail(
        &err,
        "azureblob",
        "query",
        "backend_runtime_support",
        "Azure Blob support is not available in this binary",
    );
}

#[test]
fn catalog_compatibility_status_carries_schema_detail() {
    let err = catalog_compatibility_status(
        "Select",
        "incompatible catalog version: client is '1', active is '2': stale".to_string(),
    );
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "incompatible catalog version: client is '1', active is '2': stale"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Schema as i32);
    assert_eq!(detail.backend, "catalog");
    assert_eq!(detail.operation, "Select");
    assert_eq!(detail.capability_required, "catalog_version_incompatible");
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

#[test]
fn catalog_payload_version_honors_explicit_payload_version() {
    let manifest = CatalogManifest {
        generator_version: "3".to_string(),
        checksum_sha256: "abc123".to_string(),
        ..CatalogManifest::default()
    };
    let bytes = br#"{"version":"2.4.0","generator_version":"3"}"#;

    assert_eq!(catalog_payload_version(bytes, &manifest, "1.0.0"), "2.4.0");
}

#[test]
fn secure_transport_gate_requires_server_identity_when_enabled() {
    let mut service = crate::runtime::config::ServiceSettings {
        require_secure_transport: true,
        ..crate::runtime::config::ServiceSettings::default()
    };
    assert!(validate_secure_transport(&service).is_err());

    service.tls.cert_pem = Some("-----BEGIN CERTIFICATE-----\ntest\n".to_string());
    service.tls.key_pem = Some("-----BEGIN PRIVATE KEY-----\ntest\n".to_string());
    assert!(validate_secure_transport(&service).is_ok());
}

#[test]
fn mtls_gate_requires_client_ca() {
    let mut service = crate::runtime::config::ServiceSettings {
        mtls_required: true,
        ..crate::runtime::config::ServiceSettings::default()
    };
    service.tls.cert_pem = Some("cert".to_string());
    service.tls.key_pem = Some("key".to_string());

    let err = validate_secure_transport(&service).expect_err("client CA required");
    assert!(err.contains("CLIENT_CA"));

    service.tls.client_ca_pem = Some("ca".to_string());
    assert!(validate_secure_transport(&service).is_ok());
}

#[test]
fn broker_to_broker_mtls_gate_requires_client_ca() {
    let mut service = crate::runtime::config::ServiceSettings {
        broker_to_broker_mtls_required: true,
        ..crate::runtime::config::ServiceSettings::default()
    };
    service.tls.cert_pem = Some("cert".to_string());
    service.tls.key_pem = Some("key".to_string());

    let err = validate_secure_transport(&service).expect_err("client CA required");
    assert!(err.contains("CLIENT_CA"));

    service.tls.client_ca_pem = Some("ca".to_string());
    assert!(validate_secure_transport(&service).is_ok());
}

#[test]
fn internal_control_mtls_gate_requires_client_ca() {
    let mut service = crate::runtime::config::ServiceSettings {
        internal_control_mtls_required: true,
        ..crate::runtime::config::ServiceSettings::default()
    };
    service.tls.cert_pem = Some("cert".to_string());
    service.tls.key_pem = Some("key".to_string());

    let err = validate_secure_transport(&service).expect_err("client CA required");
    assert!(err.contains("CLIENT_CA"));

    service.tls.client_ca_pem = Some("ca".to_string());
    assert!(validate_secure_transport(&service).is_ok());
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
fn rls_bypass_guard_blocks_resource_drop_without_ack() {
    let err = guard_rls_bypass_operation("drop_resource", "{}")
        .expect_err("resource drops must require explicit RLS-bypass acknowledgement");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "operation may bypass tenant isolation/RLS; set spec_json.udb_allow_rls_bypass=true after explicit tenant-scope review"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, "generic_dispatch_rls_bypass");
    assert_eq!(detail.policy_decision_id, "rls_bypass_review_required");
    assert!(!detail.retryable);
}

#[test]
fn rls_bypass_guard_allows_resource_drop_with_ack() {
    guard_rls_bypass_operation("drop_resource", r#"{"udb_allow_rls_bypass":true}"#)
        .expect("explicitly reviewed resource drop should pass");
}

#[test]
fn rls_bypass_guard_blocks_truncate_sql_without_ack() {
    let err = guard_rls_bypass_operation("transaction", r#"{"sql":"TRUNCATE tenant.orders"}"#)
        .expect_err("TRUNCATE can bypass tenant isolation and must be reviewed");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn rls_bypass_guard_blocks_fk_cascade_without_ack() {
    let err = guard_rls_bypass_operation(
        "mutate",
        r#"{"sql":"ALTER TABLE tenant.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES tenant.users(id) ON DELETE CASCADE"}"#,
    )
    .expect_err("FK cascade effects can cross tenant scope and must be reviewed");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn rls_bypass_guard_blocks_unique_constraint_without_ack() {
    let err = guard_rls_bypass_operation(
        "query",
        r#"{"sql":"CREATE UNIQUE INDEX users_email_uq ON tenant.users (email)"}"#,
    )
    .expect_err("unique constraints can leak cross-tenant existence and must be reviewed");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn descriptor_set_exposes_databroker_reflection_surface() {
    let descriptor =
        <prost_types::FileDescriptorSet as prost::Message>::decode(UDB_FILE_DESCRIPTOR_SET)
            .expect("descriptor set should decode");
    assert!(descriptor.file.iter().any(|file| {
        file.package.as_deref() == Some("udb.services.v1")
            || file.package.as_deref() == Some("legacy.udb.services.v1")
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

#[test]
fn supported_rpcs_match_databroker_descriptor_surface() {
    let descriptor =
        <prost_types::FileDescriptorSet as prost::Message>::decode(UDB_FILE_DESCRIPTOR_SET)
            .expect("descriptor set should decode");
    let descriptor_methods = descriptor
        .file
        .iter()
        .filter(|file| file.package.as_deref() == Some("udb.services.v1"))
        .flat_map(|file| file.service.iter())
        .find(|service| service.name.as_deref() == Some("DataBroker"))
        .expect("udb.services.v1.DataBroker service should be in descriptor set")
        .method
        .iter()
        .filter_map(|method| method.name.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let advertised = SUPPORTED_RPC_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        advertised, descriptor_methods,
        "GetCapabilities.supported_rpcs must advertise exactly the DataBroker RPC surface"
    );
    for typed_store_rpc in [
        "CacheGet",
        "CacheSet",
        "CacheDelete",
        "CacheScan",
        "DocumentGet",
        "DocumentFind",
        "DocumentUpsert",
        "DocumentDelete",
        "GraphQuery",
        "GraphMutate",
        "TimeSeriesWrite",
        "TimeSeriesQuery",
        "AnalyticalQuery",
    ] {
        assert!(
            advertised.contains(typed_store_rpc),
            "typed-store RPC {typed_store_rpc} must be advertised"
        );
    }
}

/// C.1 — native proto baseline lock. Decodes the compiled descriptor set and
/// pins the native control-plane services and the catalog-version compatibility
/// signal, so a breaking change (dropped/renamed service, dropped baseline RPC,
/// or a moved `client_catalog_version` field) fails HERE before runtime code is
/// touched. Additive RPCs/fields are allowed; only the stable baseline the
/// runtime depends on is asserted. This is the in-repo analog of `buf breaking`.
#[test]
fn native_service_descriptor_baseline_is_locked() {
    let descriptor =
        <prost_types::FileDescriptorSet as prost::Message>::decode(UDB_FILE_DESCRIPTOR_SET)
            .expect("descriptor set should decode");

    let methods_of = |pkg: &str, svc: &str| -> std::collections::BTreeSet<String> {
        descriptor
            .file
            .iter()
            .filter(|file| file.package.as_deref() == Some(pkg))
            .flat_map(|file| file.service.iter())
            .find(|service| service.name.as_deref() == Some(svc))
            .unwrap_or_else(|| panic!("native service {pkg}.{svc} must be in the descriptor set"))
            .method
            .iter()
            .filter_map(|method| method.name.clone())
            .collect()
    };

    // Every native control-plane service must be present and expose RPCs.
    for (pkg, svc) in [
        ("udb.core.authn.services.v1", "AuthnService"),
        ("udb.core.authz.services.v1", "AuthzService"),
        ("udb.core.apikey.services.v1", "ApiKeyService"),
        ("udb.core.tenant.services.v1", "TenantService"),
        ("udb.core.notification.services.v1", "NotificationService"),
        ("udb.core.analytics.services.v1", "AnalyticsService"),
    ] {
        assert!(
            !methods_of(pkg, svc).is_empty(),
            "native service {pkg}.{svc} must expose at least one RPC"
        );
    }

    // Lock the security-critical AuthnService baseline (the surface the runtime
    // and the NativeControlPlaneAuth interceptor depend on). Removing/renaming
    // any of these is a breaking change that must fail here.
    let authn = methods_of("udb.core.authn.services.v1", "AuthnService");
    for required in [
        "Authenticate",
        "Login",
        "Logout",
        "ValidateToken",
        "RefreshToken",
        "ChangePassword",
        "CreateSession",
        "RevokeSession",
        "StartWebAuthnRegistration",
        "FinishWebAuthnRegistration",
        "StartWebAuthnAuthentication",
        "FinishWebAuthnAuthentication",
    ] {
        assert!(
            authn.contains(required),
            "AuthnService baseline RPC {required} must remain (breaking change otherwise)"
        );
    }

    // Compatibility signal (C.1 step 3): `client_catalog_version` is the native
    // catalog-version negotiation field. Pin its field number so it cannot be
    // silently renumbered, anywhere it is declared.
    let client_catalog_version_pinned = descriptor
        .file
        .iter()
        .flat_map(|file| file.message_type.iter())
        .flat_map(|message| message.field.iter())
        .any(|field| {
            field.name.as_deref() == Some("client_catalog_version") && field.number == Some(17)
        });
    assert!(
        client_catalog_version_pinned,
        "client_catalog_version must remain field 17 (catalog-version compatibility signal)"
    );
}

/// E.1 step 3 — full service-descriptor snapshot gate.
///
/// `native_service_descriptor_baseline_is_locked` pins that every service is
/// PRESENT and locks the AuthnService RPC baseline. This complements it with a
/// comprehensive snapshot of the ENTIRE gRPC surface — all seven services and
/// every RPC each one shipped at lock time — so a proto change can never
/// silently DROP or RENAME a generated service descriptor or RPC. Additive RPCs
/// are allowed (the golden is asserted as a subset of the live descriptor);
/// removing or renaming one fails here, the in-repo analog of `buf breaking`.
///
/// When you intentionally remove/rename an RPC, update this golden in the SAME
/// commit — that is the deliberate-change checkpoint E.1 requires.
#[test]
fn full_service_descriptor_surface_snapshot() {
    // Golden inventory captured from `UDB_FILE_DESCRIPTOR_SET` (proto source of
    // truth): 16 UDB service descriptors — the native control-plane set
    // (authn/authz/apikey/idp/control/tenant/notification/analytics/storage/asset
    // + the five WebRTC services) plus the DataBroker data plane. The per-service
    // method lists are a SUBSET assertion (new RPCs may be added without editing
    // here); only the service SET is matched exactly.
    const GOLDEN: &[(&str, &[&str])] = &[
        (
            "udb.core.analytics.services.v1.AnalyticsService",
            &[
                "GetExecutorPerformance",
                "GetPipelineSummary",
                "GetReconciliationAnalytics",
                "GetSlaCompliance",
                "GetThroughput",
                "RecordPipelineMetric",
                "TriggerSnapshot",
            ],
        ),
        (
            "udb.core.apikey.services.v1.ApiKeyService",
            &[
                "CreateApiKey",
                "GetApiKey",
                "GetApiKeyUsageStats",
                "ListApiKeys",
                "RevokeApiKey",
                "UpdateApiKey",
                "ValidateApiKey",
            ],
        ),
        (
            "udb.core.idp.services.v1.IdentityProviderService",
            &[
                "CreateProvider",
                "DisableProvider",
                "ForceJwksRefresh",
                "GetProvider",
                "ImportSamlMetadata",
                "LinkIdentity",
                "ListExternalIdentities",
                "ListProviders",
                "PreviewClaimMapping",
                "PreviewGroupMapping",
                "ResolveExternalIdentity",
                "SamlAcs",
                "ScimCreateGroup",
                "ScimCreateUser",
                "ScimDeleteGroup",
                "ScimDeleteUser",
                "ScimGetGroup",
                "ScimGetUser",
                "ScimListGroups",
                "ScimListUsers",
                "ScimPatchGroup",
                "ScimPatchUser",
                "ScimReplaceUser",
                "StartSamlLogin",
                "TestProviderDiscovery",
                "UnlinkIdentity",
                "UpdateProvider",
            ],
        ),
        (
            "udb.core.control.services.v1.ControlPlaneService",
            &[
                "AckStatus",
                "DeltaResources",
                "GetResources",
                "ListNodeStates",
                "StreamResources",
            ],
        ),
        (
            "udb.core.authn.services.v1.AuthnService",
            &[
                "AdminResetPassword",
                "Authenticate",
                "ChangePassword",
                "ChangeUserStatus",
                "ConfirmMFAEnrollment",
                "CreateSession",
                "CreateUser",
                "EnrollMFA",
                "FinishWebAuthnAuthentication",
                "FinishWebAuthnRegistration",
                "GetSession",
                "GetUser",
                "ListSessions",
                "ListUsers",
                "Login",
                "Logout",
                "RefreshSession",
                "RefreshToken",
                "ResendOTP",
                "RevokeSession",
                "SendOTP",
                "StartWebAuthnAuthentication",
                "StartWebAuthnRegistration",
                "UpdateUser",
                "ValidateCSRF",
                "ValidateToken",
                "VerifyOTP",
            ],
        ),
        (
            "udb.core.authz.services.v1.AuthzService",
            &[
                "AssignRole",
                "Authorize",
                "BatchCheckPermissions",
                "CheckAccess",
                "CreatePolicyRule",
                "CreateRole",
                "DeletePolicyRule",
                "DeleteRole",
                "GetNativeAccess",
                "GetPolicyBundle",
                "GetPolicyRule",
                "GetRole",
                "LintAuthzPolicies",
                "ListAccessDecisionAudits",
                "ListPolicyRules",
                "ListRoles",
                "ListUserPermissions",
                "ListUserRoles",
                "PutAuthzPolicy",
                "PutRelationship",
                "PutRoleBinding",
                "RevokeRole",
                "UpdateRole",
            ],
        ),
        (
            "udb.core.storage.services.v1.StorageService",
            &[
                "DeleteFile",
                "FinalizeUpload",
                "GetDownloadUrl",
                "GetFile",
                "ListFiles",
                "RegisterUpload",
                "ReissueUploadUrl",
                "UpdateFile",
            ],
        ),
        (
            "udb.core.asset.services.v1.AssetService",
            &[
                "CompleteStep",
                "CreatePipelineDefinition",
                "GetAsset",
                "GetPipeline",
                "GetPipelineDefinition",
                "ListAssets",
                "RegisterAsset",
                "StartPipeline",
            ],
        ),
        (
            "udb.core.webrtc.services.v1.RoomService",
            &[
                "CloseRoom",
                "CreateRoom",
                "GetRoom",
                "ListRooms",
                "UpdateRoom",
            ],
        ),
        (
            "udb.core.webrtc.services.v1.PeerService",
            &["GetPeer", "JoinRoom", "LeaveRoom", "ListPeers"],
        ),
        (
            "udb.core.webrtc.services.v1.TrackService",
            &["ListTracks", "MuteTrack", "PublishTrack", "UnpublishTrack"],
        ),
        (
            "udb.core.webrtc.services.v1.TurnService",
            &["IssueCredentials"],
        ),
        ("udb.core.webrtc.services.v1.SignalingService", &["Signal"]),
        (
            "udb.core.notification.services.v1.NotificationService",
            &[
                "GetDeliveryStats",
                "GetNotification",
                "GetPreference",
                "GetTemplate",
                "ListNotifications",
                "ListPreferences",
                "ListTemplates",
                "RetryNotification",
                "SendNotification",
                "SetPreference",
                "UpsertTemplate",
            ],
        ),
        (
            "udb.core.tenant.services.v1.TenantService",
            &[
                "CreateTenant",
                "GetTenant",
                "GetTenantConfig",
                "ListTenants",
                "UpdateTenant",
                "UpdateTenantConfig",
            ],
        ),
        // Phase 9 native services (master-plan 9.x).
        (
            "udb.core.backup.services.v1.BackupService",
            &[
                "DeleteBackupPolicy",
                "GetBackup",
                "GetBackupPolicy",
                "ListBackupPolicies",
                "ListBackups",
                "PutBackupPolicy",
                "RestoreTenant",
                "StartTenantBackup",
            ],
        ),
        (
            "udb.core.cache.services.v1.CacheService",
            &[
                "CreateNamespace",
                "Delete",
                "DeleteNamespace",
                "Get",
                "GetNamespaceStats",
                "Scan",
                "Set",
            ],
        ),
        (
            "udb.core.config.services.v1.ConfigService",
            &[
                "DeleteFlag",
                "EvaluateFlags",
                "GetFlag",
                "ListFlags",
                "PutFlag",
            ],
        ),
        (
            "udb.core.embedding.services.v1.EmbeddingService",
            &[
                "Backfill",
                "DeleteSource",
                "ListSources",
                "RegisterSource",
                "ReportEmbedding",
                "Retrieve",
            ],
        ),
        (
            "udb.core.livequery.services.v1.LiveQueryService",
            &["Subscribe"],
        ),
        (
            "udb.core.lock.services.v1.LockService",
            &[
                "AcquireLock",
                "GetLock",
                "ListLocks",
                "ReleaseLock",
                "RenewLock",
            ],
        ),
        (
            "udb.core.metering.services.v1.MeteringService",
            &[
                "CheckQuota",
                "GetQuota",
                "ListQuotas",
                "PutQuota",
                "QueryUsage",
                "RecordUsage",
            ],
        ),
        (
            "udb.core.scheduler.services.v1.SchedulerService",
            &[
                "CreateJob",
                "DeleteJob",
                "GetJob",
                "ListJobs",
                "PauseJob",
                "ResumeJob",
            ],
        ),
        (
            "udb.core.search.services.v1.SearchService",
            &[
                "CreateIndex",
                "DeleteIndex",
                "ListIndexes",
                "Reindex",
                "Search",
            ],
        ),
        (
            "udb.core.vault.services.v1.VaultService",
            &[
                "BatchDecrypt",
                "BatchEncrypt",
                "CreateTransitKey",
                "Decrypt",
                "DeleteSecret",
                "DestroySecret",
                "Encrypt",
                "GenerateDataKey",
                "GenerateDatabaseCredentials",
                "GetSecret",
                "GetTransitPublicKey",
                "Hmac",
                "ListSecrets",
                "PutSecret",
                "Rewrap",
                "RotateTransitKey",
                "SealStatus",
                "Sign",
                "UndeleteSecret",
                "Verify",
            ],
        ),
        (
            "udb.core.webhook.services.v1.WebhookService",
            &[
                "CreateEndpoint",
                "DeleteEndpoint",
                "GetEndpoint",
                "ListDeliveries",
                "ListEndpoints",
                "UpdateEndpoint",
            ],
        ),
        (
            "udb.core.workflow.services.v1.WorkflowService",
            &[
                "CancelWorkflow",
                "GetWorkflow",
                "ListWorkflows",
                "SignalWorkflow",
                "StartWorkflow",
            ],
        ),
        (
            "udb.services.v1.DataBroker",
            &[
                "ActivateCatalog",
                "AnalyticalQuery",
                "ApplyMigration",
                "ApproveMigrationPlan",
                "BatchSelect",
                "BatchUpsert",
                "BeginTx",
                "CacheDelete",
                "CacheGet",
                "CacheScan",
                "CacheSet",
                "CreateMaterializedView",
                "Delete",
                "DeletePolicy",
                "DismissDlqEvent",
                "DocumentDelete",
                "DocumentFind",
                "DocumentGet",
                "DocumentUpsert",
                "DropResource",
                "EnqueueOutboxEvent",
                "EnsureProject",
                "EnsureResource",
                "GeneratePresignedUrl",
                "GenericDispatch",
                "GetAdminSummary",
                "GetCapabilities",
                "GetCatalogManifest",
                "GetCatalogVersion",
                "GetCatalogVersions",
                "GetCdcStatus",
                "GetDlqEvent",
                "GetHealthReport",
                "GetMigrationStatus",
                "GetObject",
                "GetSaga",
                "GraphMutate",
                "GraphQuery",
                "InitiateMultipartUpload",
                "LintPolicies",
                "ListAdminAuditLogs",
                "ListDlqEvents",
                "ListMessageSchemas",
                "ListMigrationRuns",
                "ListPolicies",
                "ListProjects",
                "ListResources",
                "ListSagas",
                "LookupMessageSchema",
                "MarkSagaReviewed",
                "PauseCdc",
                "PlanMigration",
                "PreviewCdcRedaction",
                "PublishCDC",
                "PutObject",
                "PutPolicy",
                "QuarantineDlqEvent",
                "ReloadPolicies",
                "ReplayDlqEvent",
                "ResumeCdc",
                "RetrySagaCompensation",
                "RollbackCatalog",
                "ScanProjectionDrift",
                "Select",
                "SelectV2",
                "StageCatalog",
                "StepDownCdcLeader",
                "TimeSeriesQuery",
                "TimeSeriesWrite",
                "Upsert",
                "ValidateCatalog",
                "VectorBatchUpsert",
                "VectorHybridSearch",
                "VectorSearch",
                "VectorUpsert",
                "VerifyAdminAuditLog",
            ],
        ),
    ];

    let descriptor =
        <prost_types::FileDescriptorSet as prost::Message>::decode(UDB_FILE_DESCRIPTOR_SET)
            .expect("descriptor set should decode");

    // Build the live {service_fqn -> method names} map from the descriptor.
    let mut live: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
        std::collections::BTreeMap::new();
    for file in &descriptor.file {
        let pkg = file.package.as_deref().unwrap_or("");
        for svc in &file.service {
            let Some(name) = svc.name.as_deref() else {
                continue;
            };
            let fqn = format!("{pkg}.{name}");
            let methods = svc.method.iter().filter_map(|m| m.name.clone()).collect();
            live.insert(fqn, methods);
        }
    }

    // 1. Every golden service descriptor must still exist (no silent service drop).
    for (svc, golden_methods) in GOLDEN {
        let live_methods = live.get(*svc).unwrap_or_else(|| {
            panic!("service descriptor {svc} was dropped from UDB_FILE_DESCRIPTOR_SET")
        });
        // 2. Every golden RPC must still exist (no silent RPC drop/rename).
        for method in *golden_methods {
            assert!(
                live_methods.contains(*method),
                "RPC {svc}.{method} was dropped/renamed — update the proto AND this golden \
                 deliberately if intended (E.1 breaking-change gate)"
            );
        }
    }

    // 3. The set of UDB service descriptors must match the golden exactly, so a
    //    NEW service is also a deliberate, reviewed addition here (catches
    //    accidentally-shipped or accidentally-renamed services). Only UDB
    //    services are inventoried (vendored google.* descriptors are ignored).
    let golden_services: std::collections::BTreeSet<&str> =
        GOLDEN.iter().map(|(s, _)| *s).collect();
    let live_udb_services: std::collections::BTreeSet<&str> = live
        .keys()
        .map(String::as_str)
        .filter(|fqn| fqn.starts_with("udb."))
        .collect();
    assert_eq!(
        live_udb_services, golden_services,
        "the set of UDB service descriptors changed — a service was added/removed/renamed; \
         update this golden snapshot in the same commit"
    );
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
    let typed_receipt = response
        .get_ref()
        .write_receipt
        .as_ref()
        .map(crate::runtime::consistency::WriteReceipt::from_proto)
        .expect("typed write_receipt should be attached");
    assert_eq!(typed_receipt, receipt);
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
        .schema
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

/// A.1 — V1 compatibility freeze. Pins every existing CapabilitiesResponse V1
/// field (schema_checksum=1, protocol_version=2, enabled_backends=3,
/// degraded_backends=4, system_catalog_relations=5, supported_rpcs=6,
/// backend_instances=7, backend_capabilities=8). This test FAILS if a V2 change
/// drops or renumbers a V1 field — the field accesses below stop compiling on a
/// rename/removal, and the assertions catch a field that goes silently empty.
#[tokio::test]
async fn get_capabilities_v1_fields_frozen() {
    let svc = open_service();
    let resp = svc
        .get_capabilities(capabilities_request_with_tenant())
        .await
        .unwrap();
    let caps = resp.get_ref();

    // Field 1: schema_checksum — always populated (active manifest checksum or
    // a computed fallback hash; never empty).
    assert!(
        !caps.schema_checksum.is_empty(),
        "schema_checksum (field 1) must be populated"
    );
    // Field 2: protocol_version — pinned to the server constant.
    assert_eq!(
        caps.protocol_version, UDB_PROTOCOL_VERSION,
        "protocol_version (field 2) must equal UDB_PROTOCOL_VERSION"
    );
    // Field 3/4: enabled/degraded backends. planning_only() has no live pools,
    // so enabled may be empty, but the degraded list (all known plugins) must
    // not be — proving the field is still wired.
    assert!(
        !caps.degraded_backends.is_empty(),
        "degraded_backends (field 4) must list configured-but-unconnected backends"
    );
    // Field 5: system_catalog_relations — the fixed system relations are always
    // present.
    assert!(
        !caps.system_catalog_relations.is_empty(),
        "system_catalog_relations (field 5) must be populated"
    );
    // Field 6: supported_rpcs — mirrors SUPPORTED_RPC_NAMES.
    assert_eq!(
        caps.supported_rpcs.len(),
        SUPPORTED_RPC_NAMES.len(),
        "supported_rpcs (field 6) must mirror SUPPORTED_RPC_NAMES"
    );
    assert!(caps.supported_rpcs.contains(&"Select".to_string()));
    // Field 7: backend_instances — field is wired (Vec accessor compiles).
    let _backend_instances: &Vec<_> = &caps.backend_instances;
    // Field 8: backend_capabilities — one descriptor per backend plugin.
    assert!(
        !caps.backend_capabilities.is_empty(),
        "backend_capabilities (field 8) must be populated"
    );
}

/// A.2 — additive protocol negotiation. Proves the new V2 fields
/// (protocol_support=9, backend_protocol_support=10) are populated AND that the
/// V1 fields (1-8) are still present, so V2 never regresses the V1 surface.
#[tokio::test]
async fn get_capabilities_populates_protocol_support() {
    let svc = open_service();
    let resp = svc
        .get_capabilities(capabilities_request_with_tenant())
        .await
        .unwrap();
    let caps = resp.get_ref();

    // V1 fields still present (regression guard).
    assert!(!caps.schema_checksum.is_empty());
    assert_eq!(caps.protocol_version, UDB_PROTOCOL_VERSION);
    assert!(!caps.supported_rpcs.is_empty());
    assert!(!caps.backend_capabilities.is_empty());

    // Field 9: protocol_support.
    let ps = caps
        .protocol_support
        .as_ref()
        .expect("protocol_support (field 9) must be populated");
    assert!(
        !ps.min_protocol_version.is_empty(),
        "min_protocol_version must be non-empty"
    );
    assert!(
        !ps.max_protocol_version.is_empty(),
        "max_protocol_version must be non-empty"
    );
    assert_eq!(ps.min_protocol_version, UDB_PROTOCOL_VERSION);
    assert_eq!(ps.max_protocol_version, UDB_PROTOCOL_VERSION);
    assert!(
        ps.encodings.contains(&"record_set_v1".to_string()),
        "encodings must advertise the V1 row encoding"
    );
    assert!(
        ps.encodings.contains(&"record_batch_v2".to_string()),
        "A.4 shipped SelectV2, so record_batch_v2 must now be advertised"
    );
    assert!(
        !ps.supported_rpcs.is_empty(),
        "protocol_support.supported_rpcs must mirror the RPC list"
    );
    assert_eq!(ps.supported_rpcs.len(), caps.supported_rpcs.len());
    assert!(
        ps.supports_streaming_reads,
        "runtime provides a default query streaming path"
    );

    // Field 10: backend_protocol_support — one entry per backend.
    assert_eq!(
        caps.backend_protocol_support.len(),
        caps.backend_capabilities.len(),
        "backend_protocol_support (field 10) must have one entry per backend"
    );
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
    let err = svc
        .require_portal_permission(&viewer, "RetrySagaCompensation", true)
        .expect_err("viewer cannot run portal mutations");
    assert_policy_detail(
        &err,
        "portal_permission",
        "portal_operator_required",
        "scope udb:admin or udb:portal:operator is required for RetrySagaCompensation",
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

#[test]
fn admin_scope_denial_carries_policy_detail() {
    let viewer = SecurityContext {
        scopes: vec!["udb:portal:viewer".to_string()],
        ..SecurityContext::default()
    };
    let err = require_admin_scope(&viewer).expect_err("portal viewer is not a broker admin");
    assert_policy_detail(
        &err,
        "admin_scope",
        "admin_scope_required",
        "scope udb:admin is required",
    );
}

#[test]
fn webrtc_peer_policy_denials_carry_policy_detail() {
    let scope_err = service_policy_denied(
        "webrtc_peer_token",
        "webrtc_peer_scope_required",
        "scope udb:webrtc:peer or udb:webrtc:signal is required",
    );
    assert_policy_detail(
        &scope_err,
        "webrtc_peer_token",
        "webrtc_peer_scope_required",
        "scope udb:webrtc:peer or udb:webrtc:signal is required",
    );

    let tenant_err = service_policy_denied(
        "webrtc_peer_token",
        "webrtc_peer_tenant_mismatch",
        "x-tenant-id must match the peer token tenant",
    );
    assert_policy_detail(
        &tenant_err,
        "webrtc_peer_token",
        "webrtc_peer_tenant_mismatch",
        "x-tenant-id must match the peer token tenant",
    );
}

// ── Broker authz gate (Casbin) — items 124-131 ──────────────────────────
// These drive `DataBrokerService::authorize()` through the Casbin decision
// engine (the ONLY broker authz path), so the broker-level allow/deny is
// exercised deterministically — no live Postgres, no gRPC server. The broker
// passes the raw RPC name (`Select`/`Upsert`/`PutPolicy`) as the operation, so
// a matching policy carries the same operation string.

fn v2_service(policies: Vec<crate::runtime::security::AbacPolicy>) -> DataBrokerService {
    let svc = ready_service(); // abac_default_allow = false
    if let Ok(mut p) = svc.abac_policies.write() {
        *p = policies;
    }
    // Casbin is the only authz engine; refresh the snapshot so the new policies
    // take effect (the gate reads the cached `Arc<AuthzSnapshot>`).
    svc.refresh_abac_snapshot();
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
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, "data_plane_authorize");
    assert!(detail.policy_decision_id.starts_with("authz_"));
    assert_eq!(
        err.message(),
        "no authz policy (default deny); seed ABAC policies or set UDB_ABAC_DEFAULT_ALLOW=true"
    );
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
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, "data_plane_authorize");
    assert!(detail.policy_decision_id.starts_with("authz_"));
}

#[tokio::test]
async fn broker_v2_batch_item_denial_carries_policy_detail() {
    let svc = v2_service(vec![]);
    let ctx = billing_ctx(&["udb:read"]);
    let snapshot = svc.current_abac_snapshot();
    let err = DataBrokerService::authorize_message_item(&snapshot, &ctx, "Payment", "Select")
        .await
        .expect_err("batch item with no matching policy must be denied");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, "data_plane_authorize_item");
    assert!(detail.policy_decision_id.starts_with("authz_"));
    assert_eq!(
        err.message(),
        "no authz policy (default deny); seed ABAC policies or set UDB_ABAC_DEFAULT_ALLOW=true"
    );
}

#[tokio::test]
async fn broker_v2_control_rpc_not_locked_out_by_nonmatching_policy() {
    // §5 lockout fix (regression guard): a data-plane policy set that matches no
    // control RPC must NOT lock out control/meta RPCs. Control RPCs reach the gate
    // with the WILDCARD message_type ("*", from `authorized_call!`) and are governed
    // by their own coarse `require_admin_scope` gate — NEVER by the data-plane policy
    // set. Without the carve-out (`service::mod::authorize` returns early on `"*"`),
    // the FIRST non-matching `PutPolicy` would deny `GetCapabilities`, `PutPolicy`,
    // `DeletePolicy` and every control RPC, making the cluster unrecoverable without a
    // broker restart. This test pins that the carve-out holds while data ops stay
    // deny-by-default.
    let svc = v2_service(vec![allow_policy("Select", "udb:read")]); // matches data Select only
    let ctx = billing_ctx(&["udb:admin"]);
    assert!(
        svc.authorize(&ctx, "*", "GetCapabilities").await.is_ok(),
        "control RPC (wildcard message_type) must survive a non-matching policy set"
    );
    assert!(
        svc.authorize(&ctx, "*", "PutPolicy").await.is_ok(),
        "PutPolicy must stay reachable so a bad policy is removable WITHOUT a restart"
    );
    assert!(
        svc.authorize(&ctx, "*", "DeletePolicy").await.is_ok(),
        "DeletePolicy must stay reachable so the cluster can recover from a bad policy"
    );
    // A real data op with no matching policy is still deny-by-default — the intended
    // opt-in-to-enforcement behavior, NOT a lockout.
    assert_eq!(
        svc.authorize(&ctx, "Payment", "Upsert")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied,
        "data ops stay deny-by-default once any policy is present"
    );
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
async fn broker_casbin_enforces_legacy_policy_semantics() {
    // 132 gate (Casbin-only): the broker authz engine must produce the expected
    // deny-by-default outcomes for the canonical allow / missing-scope / explicit-
    // deny / no-match matrix. (Formerly compared a v2 path against the deleted
    // legacy `evaluate_abac`; Casbin is now the only engine, so we assert the
    // outcomes directly.)
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
    // (ctx, message_type, operation, expected_allow)
    let cases = [
        (billing_ctx(&["udb:read"]), "Payment", "Select", true), // matching allow
        (billing_ctx(&[]), "Payment", "Select", false),          // missing required scope
        (billing_ctx(&["udb:read"]), "Payment", "Delete", false), // explicit deny wins
        (billing_ctx(&["udb:write"]), "Payment", "Upsert", false), // no matching policy
    ];
    for (ctx, msg, op, expected) in cases {
        let svc = v2_service(policies.clone());
        let allowed = svc.authorize(&ctx, msg, op).await.is_ok();
        assert_eq!(
            allowed, expected,
            "Casbin decision for {msg}/{op} was {allowed}, expected {expected}"
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
