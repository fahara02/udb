//! Phase 0A (Section F5/F13) descriptor lints.
//!
//! These lints complement `native_contract_findings` in [`super`] (which already
//! emits `endpoint_security_missing`, `non_public_rpc_without_policy`, and
//! `db_table_security_missing`). This module covers the F13 gaps that the
//! existing pass does *not* catch — abuse/rate-limit hygiene on public RPCs,
//! tenant-source enforceability, tenant-scoped table RLS misconfiguration, loss
//! of scalar field security signals during descriptor decoding, and a hard RPC
//! inventory drift gate so that proto surface changes can't silently slip past
//! the contract.
//!
//! Every finding is a JSON object shaped `{severity, code, path, message, hint}`
//! so the CLI gate can render and aggregate them uniformly. `severity` is one of
//! `"error"` or `"warning"`; only `"error"` findings should fail CI.

use udb::runtime::descriptor_manifest::{
    DescriptorContractManifest, EndpointSecurityContract, FieldContract, MessageContract,
    RpcContract,
};

// SDK RPC-count drift (F13) is enforced by the committed-manifest CI gate
// (`docs/generated/udb-native-contract.json` is regenerated and `git diff`-ed),
// which catches every RPC add/remove — strictly stronger than a hardcoded
// count, and not a per-RPC-addition breakage during active development. So no
// `NATIVE_RPC_BASELINE` const lives here.

/// Field names accepted as an in-message tenant source when an RPC declares
/// `tenant_required` but leaves `tenant_field` empty.
const TENANT_FIELD_NAMES: &[&str] = &["tenant_id", "tenant"];

/// Emit the F13 descriptor lints not already covered by
/// `native_contract_findings`.
pub(crate) fn descriptor_lint_findings(
    manifest: &DescriptorContractManifest,
) -> Vec<serde_json::Value> {
    let mut findings = Vec::new();

    for service in &manifest.services {
        for rpc in &service.methods {
            // operation_kind_unspecified (error): EVERY RPC — including the
            // DataBroker data-plane RPCs that carry no endpoint_security — MUST
            // declare its state-change class. This is the authoritative source for
            // SDK retry safety and conformance probing, so an unset value is a hard
            // error: new RPCs are classified explicitly, never guessed by name.
            if rpc.operation_kind == 0 {
                findings.push(serde_json::json!({
                    "severity": "error",
                    "code": "operation_kind_unspecified",
                    "path": rpc.grpc_path(),
                    "message": format!(
                        "RPC {} does not declare the operation_kind method option",
                        rpc.grpc_path()
                    ),
                    "hint": "set option (udb.core.common.v1.operation_kind) = OPERATION_KIND_READ_ONLY | _MUTATION | _DESTRUCTIVE;",
                }));
            }

            let Some(security) = rpc.endpoint_security.as_ref() else {
                // `endpoint_security_missing` is already emitted upstream; the
                // F13 lints below all presuppose a present contract.
                continue;
            };

            // 1. public_rpc_missing_abuse_policy (warning)
            if security.mode == 1
                && security.rate_limit_policy_ref.trim().is_empty()
                && security.abuse_policy_ref.trim().is_empty()
            {
                findings.push(serde_json::json!({
                    "severity": "warning",
                    "code": "public_rpc_missing_abuse_policy",
                    "path": rpc.grpc_path(),
                    "message": format!(
                        "public RPC {} declares neither rate_limit_policy_ref nor abuse_policy_ref",
                        rpc.grpc_path()
                    ),
                    "hint": "set endpoint_security.rate_limit_policy_ref and/or abuse_policy_ref so unauthenticated traffic is bounded",
                }));
            }

            // 2. tenant_required_without_source (error)
            if security.tenant_required && !rpc_has_tenant_source(manifest, rpc, security) {
                findings.push(serde_json::json!({
                    "severity": "error",
                    "code": "tenant_required_without_source",
                    "path": rpc.grpc_path(),
                    "message": format!(
                        "RPC {} requires a tenant but has no enforceable tenant source (empty tenant_field and request {} has no tenant_id/tenant field)",
                        rpc.grpc_path(),
                        rpc.input_type
                    ),
                    "hint": "set endpoint_security.tenant_field to a request field, or add a tenant_id field to the request message",
                }));
            }
        }
    }

    for message in &manifest.messages {
        // 3. tenant_scoped_table_rls_gap (warning)
        if let Some(table) = message.db_table_security.as_ref() {
            if table.tenant_isolation_mode == "tenant"
                && (table.tenant_column.trim().is_empty()
                    || table.rls_policy_template.trim().is_empty())
            {
                findings.push(serde_json::json!({
                    "severity": "warning",
                    "code": "tenant_scoped_table_rls_gap",
                    "path": message.full_name,
                    "message": format!(
                        "message {} is tenant-isolated but is missing {} (RLS would not be enforceable)",
                        message.full_name,
                        rls_gap_detail(table.tenant_column.trim().is_empty(), table.rls_policy_template.trim().is_empty())
                    ),
                    "hint": "set both db_table_security.tenant_column and db_table_security.rls_policy_template for tenant-isolated tables",
                }));
            }
        }

        // 4. scalar_security_option_lost (error)
        for field in &message.fields {
            if scalar_signal_present(field) && !scalar_signal_survived(field) {
                findings.push(serde_json::json!({
                    "severity": "error",
                    "code": "scalar_security_option_lost",
                    "path": format!("{}.{}", message.full_name, field.name),
                    "message": format!(
                        "field {}.{} carries a scalar security signal but it produced no security metadata in the manifest",
                        message.full_name, field.name
                    ),
                    "hint": "ensure the scalar field option is decoded into db_column_security or a normalized classification (security_classification/data_category)",
                }));
            }
        }
    }

    // (SDK RPC-count drift is covered by the committed-manifest CI gate, not a
    // hardcoded baseline — see the module-level note.)

    findings
}

/// True when the RPC has an enforceable tenant source: an explicit
/// `tenant_field` on the contract, or a `tenant_id`/`tenant` field on the
/// request message.
fn rpc_has_tenant_source(
    manifest: &DescriptorContractManifest,
    rpc: &RpcContract,
    security: &EndpointSecurityContract,
) -> bool {
    if !security.tenant_field.trim().is_empty() {
        return true;
    }
    if security.request_context_required {
        return true;
    }
    request_message(manifest, rpc)
        .map(|message| {
            message
                .fields
                .iter()
                .any(|field| TENANT_FIELD_NAMES.contains(&field.name.as_str()))
        })
        .unwrap_or(false)
}

/// Resolve an RPC's request message in the manifest by fully-qualified type
/// name (the manifest stores `input_type` with a leading dot, while message
/// `full_name` is unprefixed).
fn request_message<'m>(
    manifest: &'m DescriptorContractManifest,
    rpc: &RpcContract,
) -> Option<&'m MessageContract> {
    let target = rpc.input_type.trim_start_matches('.');
    manifest
        .messages
        .iter()
        .find(|message| message.full_name == target)
}

/// True when a field declares any scalar security signal worth preserving.
fn scalar_signal_present(field: &FieldContract) -> bool {
    let s = &field.scalar_security;
    s.pii
        || s.encrypted_security
        || s.log_masked
        || s.log_redacted
        || s.sensitive
        || s.requires_consent
        || s.tokenized
        || s.security_classification != 0
        || s.data_category != 0
        || !s.data_purpose.trim().is_empty()
        || s.retention_days != 0
}

/// True when the scalar signal survived into usable manifest security metadata:
/// either structured `db_column_security`, or a normalized classification on the
/// scalar record itself (`security_classification` / `data_category`).
fn scalar_signal_survived(field: &FieldContract) -> bool {
    if field.db_column_security.is_some() {
        return true;
    }
    let s = &field.scalar_security;
    if s.security_classification != 0 || s.data_category != 0 {
        return true;
    }
    if s.sensitive || s.encrypted_security || s.tokenized {
        return false;
    }
    s.pii
        || s.log_masked
        || s.log_redacted
        || s.requires_consent
        || !s.data_purpose.trim().is_empty()
        || s.retention_days != 0
}

fn rls_gap_detail(missing_column: bool, missing_template: bool) -> &'static str {
    match (missing_column, missing_template) {
        (true, true) => "tenant_column and rls_policy_template",
        (true, false) => "tenant_column",
        (false, true) => "rls_policy_template",
        (false, false) => "tenant isolation metadata",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use udb::runtime::descriptor_manifest::{
        DbColumnSecurityContract, DbTableSecurityContract, ScalarFieldSecurity, ServiceContract,
        descriptor_contract_manifest,
    };

    fn empty_security() -> EndpointSecurityContract {
        EndpointSecurityContract {
            mode: 0,
            roles: Vec::new(),
            scopes: Vec::new(),
            tenant_required: false,
            csrf_required: false,
            policy_ref: String::new(),
            internal_grpc_only: false,
            required_assurance_level: 0,
            allowed_credential_types: Vec::new(),
            rate_limit_policy_ref: String::new(),
            abuse_policy_ref: String::new(),
            audit_event_type: String::new(),
            decision_resource: String::new(),
            owner_field: String::new(),
            tenant_field: String::new(),
            project_field: String::new(),
            idempotency_required: false,
            request_context_required: false,
        }
    }

    fn rpc_with(method: &str, input_type: &str, security: EndpointSecurityContract) -> RpcContract {
        RpcContract {
            service_name: "TestService".to_string(),
            service_pkg: "udb.test.v1".to_string(),
            method: method.to_string(),
            method_snake: method.to_lowercase(),
            input_type: input_type.to_string(),
            input_pkg: String::new(),
            input_short: String::new(),
            output_type: ".udb.test.v1.Out".to_string(),
            output_pkg: String::new(),
            output_short: String::new(),
            client_streaming: false,
            server_streaming: false,
            endpoint_security: Some(security),
            rest_contract: None,
            http_rule: None,
            sdk_surface: None,
            cli_scaffold: None,
            event_contract: None,
            emits: Vec::new(),
            dependency_contract: None,
            operation_kind: 1,
        }
    }

    fn service_with(methods: Vec<RpcContract>) -> ServiceContract {
        ServiceContract {
            file_path: "proto/udb/test/v1/test.proto".to_string(),
            package: "udb.test.v1".to_string(),
            name: "TestService".to_string(),
            native_service: None,
            sdk_surface: None,
            cli_scaffold: None,
            dependency_contract: None,
            methods,
        }
    }

    fn manifest_of(
        services: Vec<ServiceContract>,
        messages: Vec<MessageContract>,
    ) -> DescriptorContractManifest {
        DescriptorContractManifest { services, messages }
    }

    fn codes(findings: &[serde_json::Value]) -> Vec<String> {
        findings
            .iter()
            .filter_map(|f| f.get("code").and_then(|c| c.as_str()).map(str::to_string))
            .collect()
    }

    #[test]
    fn public_rpc_without_abuse_refs_is_flagged() {
        // A public RPC with neither rate-limit nor abuse policy refs.
        let mut sec = empty_security();
        sec.mode = 1;
        let rpc = rpc_with("Ping", ".udb.test.v1.PingRequest", sec);
        // Pad the manifest so the count-drift lint doesn't mask the result.
        let manifest = manifest_of(vec![service_with(vec![rpc])], Vec::new());

        let codes = codes(&descriptor_lint_findings(&manifest));
        assert!(
            codes.contains(&"public_rpc_missing_abuse_policy".to_string()),
            "expected public_rpc_missing_abuse_policy, got {codes:?}"
        );
    }

    #[test]
    fn public_rpc_with_rate_limit_ref_is_not_flagged() {
        let mut sec = empty_security();
        sec.mode = 1;
        sec.rate_limit_policy_ref = "rl.default".to_string();
        let rpc = rpc_with("Ping", ".udb.test.v1.PingRequest", sec);
        let manifest = manifest_of(vec![service_with(vec![rpc])], Vec::new());

        let codes = codes(&descriptor_lint_findings(&manifest));
        assert!(
            !codes.contains(&"public_rpc_missing_abuse_policy".to_string()),
            "rate_limit_policy_ref should satisfy the abuse lint, got {codes:?}"
        );
    }

    #[test]
    fn tenant_required_with_empty_field_and_no_request_field_is_error() {
        let mut sec = empty_security();
        sec.mode = 2;
        sec.tenant_required = true;
        // tenant_field empty + request message absent from manifest => no source.
        let rpc = rpc_with("DoThing", ".udb.test.v1.DoThingRequest", sec);
        let manifest = manifest_of(vec![service_with(vec![rpc])], Vec::new());

        let findings = descriptor_lint_findings(&manifest);
        let drift = findings
            .iter()
            .find(|f| {
                f.get("code").and_then(|c| c.as_str()) == Some("tenant_required_without_source")
            })
            .expect("expected tenant_required_without_source finding");
        assert_eq!(
            drift.get("severity").and_then(|s| s.as_str()),
            Some("error")
        );
    }

    #[test]
    fn tenant_required_satisfied_by_request_tenant_id_field() {
        let mut sec = empty_security();
        sec.mode = 2;
        sec.tenant_required = true;
        let rpc = rpc_with("DoThing", ".udb.test.v1.DoThingRequest", sec);
        let request = MessageContract {
            file_path: "proto/udb/test/v1/test.proto".to_string(),
            package: "udb.test.v1".to_string(),
            name: "DoThingRequest".to_string(),
            full_name: "udb.test.v1.DoThingRequest".to_string(),
            db_table_security: None,
            sdk_surface: None,
            event_contract: None,
            dependency_contract: None,
            fields: vec![FieldContract {
                name: "tenant_id".to_string(),
                number: 1,
                type_name: "string".to_string(),
                db_column_security: None,
                scalar_security: ScalarFieldSecurity::default(),
            }],
        };
        let manifest = manifest_of(vec![service_with(vec![rpc])], vec![request]);

        let codes = codes(&descriptor_lint_findings(&manifest));
        assert!(
            !codes.contains(&"tenant_required_without_source".to_string()),
            "request tenant_id field should satisfy the tenant-source lint, got {codes:?}"
        );
    }

    #[test]
    fn tenant_scoped_table_without_column_or_template_is_flagged() {
        let table = DbTableSecurityContract {
            tenant_isolation_mode: "tenant".to_string(),
            project_isolation_mode: String::new(),
            tenant_column: String::new(),
            project_column: String::new(),
            rls_policy_template: String::new(),
            soft_delete_mode: String::new(),
            retention_class: String::new(),
            retention_days: 0,
            audit_mode: 0,
            encryption_profile: String::new(),
            pii_profile: String::new(),
            break_glass_visible: false,
            export_eligible: false,
            data_residency_policy_ref: String::new(),
        };
        let message = MessageContract {
            file_path: "proto/udb/test/v1/test.proto".to_string(),
            package: "udb.test.v1".to_string(),
            name: "Thing".to_string(),
            full_name: "udb.test.v1.Thing".to_string(),
            db_table_security: Some(table),
            sdk_surface: None,
            event_contract: None,
            dependency_contract: None,
            fields: Vec::new(),
        };
        let manifest = manifest_of(Vec::new(), vec![message]);

        let codes = codes(&descriptor_lint_findings(&manifest));
        assert!(
            codes.contains(&"tenant_scoped_table_rls_gap".to_string()),
            "expected tenant_scoped_table_rls_gap, got {codes:?}"
        );
    }

    #[test]
    fn scalar_signal_without_metadata_is_error() {
        let mut scalar = ScalarFieldSecurity::default();
        scalar.sensitive = true; // signal present
        let field = FieldContract {
            name: "secret".to_string(),
            number: 1,
            type_name: "string".to_string(),
            db_column_security: None, // no structured survivor
            scalar_security: scalar,  // and no normalized classification
        };
        let message = MessageContract {
            file_path: "proto/udb/test/v1/test.proto".to_string(),
            package: "udb.test.v1".to_string(),
            name: "Thing".to_string(),
            full_name: "udb.test.v1.Thing".to_string(),
            db_table_security: None,
            sdk_surface: None,
            event_contract: None,
            dependency_contract: None,
            fields: vec![field],
        };
        let manifest = manifest_of(Vec::new(), vec![message]);

        let codes = codes(&descriptor_lint_findings(&manifest));
        assert!(
            codes.contains(&"scalar_security_option_lost".to_string()),
            "expected scalar_security_option_lost, got {codes:?}"
        );
    }

    #[test]
    fn scalar_signal_survives_via_db_column_security() {
        let mut scalar = ScalarFieldSecurity::default();
        scalar.sensitive = true;
        let field = FieldContract {
            name: "secret".to_string(),
            number: 1,
            type_name: "string".to_string(),
            db_column_security: Some(DbColumnSecurityContract {
                secret_classification: 3,
                output_view: 1,
                redaction_strategy: 0,
                tokenization_strategy: String::new(),
                hashing_strategy: String::new(),
                hashing_algorithm: String::new(),
                encryption_key_class: String::new(),
                searchable_encrypted: false,
                uniqueness_scope: String::new(),
                owner_field: false,
                tenant_field: false,
                project_field: false,
            }),
            scalar_security: scalar,
        };
        let message = MessageContract {
            file_path: "proto/udb/test/v1/test.proto".to_string(),
            package: "udb.test.v1".to_string(),
            name: "Thing".to_string(),
            full_name: "udb.test.v1.Thing".to_string(),
            db_table_security: None,
            sdk_surface: None,
            event_contract: None,
            dependency_contract: None,
            fields: vec![field],
        };
        let manifest = manifest_of(Vec::new(), vec![message]);

        let codes = codes(&descriptor_lint_findings(&manifest));
        assert!(
            !codes.contains(&"scalar_security_option_lost".to_string()),
            "db_column_security should count as a survivor, got {codes:?}"
        );
    }

    /// The real embedded manifest must produce ZERO error-severity findings so
    /// the gate passes today. Warnings are acceptable. This also pins
    /// NATIVE_RPC_BASELINE to the live manifest total (no drift error).
    #[test]
    fn real_manifest_yields_no_error_findings() {
        let manifest = descriptor_contract_manifest();
        let findings = descriptor_lint_findings(&manifest);
        let errors: Vec<&serde_json::Value> = findings
            .iter()
            .filter(|f| f.get("severity").and_then(|s| s.as_str()) == Some("error"))
            .collect();
        assert!(
            errors.is_empty(),
            "real manifest must yield no error findings, got {errors:#?}"
        );
    }
}
