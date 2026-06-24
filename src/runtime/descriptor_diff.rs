//! Native-service contract version + descriptor-diff classifier (Phase 0A / F6).
//!
//! This module gives the native-service surface its own **contract semver**,
//! independent of the crate/package version and of the wire `protocol_version()`
//! ([`crate::runtime::native_catalog::protocol_version`]). It also classifies the
//! differences between two [`DescriptorContractManifest`] snapshots so contract
//! drift can be triaged (additive vs. auth/db/sdk breaking vs. removed).
//!
//! Note: [`DescriptorContractManifest`] (in `descriptor_manifest.rs`) does NOT
//! derive `serde::{Serialize, Deserialize}` — and that file is maintainer-hot, so
//! we deliberately do not add derives there. Consequently there is no
//! `parse_manifest_json` here: [`diff_manifests`] operates on two live
//! `DescriptorContractManifest` values. Callers that need a persisted baseline
//! should rebuild the live manifest from a stored `FileDescriptorSet` via
//! [`crate::runtime::descriptor_manifest::descriptor_contract_manifest_from_bytes`].

use std::collections::BTreeMap;

use crate::runtime::descriptor_manifest::{
    DescriptorContractManifest, EmittedEvent, EndpointSecurityContract, EventContract,
    MessageContract, RpcContract, ServiceContract,
};

/// Native-service CONTRACT semver.
///
/// Bump this on any contract-affecting change to the native-service surface
/// (RPC shape, endpoint security, persisted table/column security, SDK surface).
/// This is **independent** of the crate/package version (`Cargo.toml`) and of the
/// wire `protocol_version()` ([`crate::runtime::native_catalog::protocol_version`]):
/// the wire version tracks on-the-wire framing, whereas this tracks the semantic
/// contract that native-service consumers and SDKs depend on.
pub const NATIVE_CONTRACT_VERSION: &str = "4.0.0";

/// Classification of a single contract difference.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ChangeKind {
    /// New surface area that does not break existing consumers (added service/RPC).
    Additive,
    /// A behavioral toggle that does not, on its own, tighten or break auth/db/sdk
    /// contracts (e.g. tenant requirement relaxed, non-auth flag flip).
    BehavioralChange,
    /// A change to authentication/authorization enforcement (mode/scope/role/policy,
    /// or a tightening of tenant/csrf/internal-only gating).
    AuthBreaking,
    /// A change to persisted data security/shape (table or column security, output
    /// view, or fields added/removed on a persisted message).
    DbBreaking,
    /// A change that breaks generated SDKs (removed RPC/service, request/response
    /// type change).
    SdkBreaking,
    /// A change to an event contract (outbox topic, event type, delivery
    /// guarantee, partition key, or redaction profile) that downstream CDC/event
    /// consumers depend on.
    EventBreaking,
    /// A service or RPC that existed in `old` but is gone in `new`.
    Removed,
}

/// A single classified contract difference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContractChange {
    pub kind: ChangeKind,
    /// Stable identity of the changed element: service full name,
    /// `/Package.Service/Method` gRPC path, or message full name.
    pub path: String,
    pub detail: String,
}

impl ContractChange {
    fn new(kind: ChangeKind, path: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            kind,
            path: path.into(),
            detail: detail.into(),
        }
    }
}

/// Classify the differences between two contract manifests.
///
/// Elements are indexed by stable identity (service full name, `/Service/Method`
/// gRPC path, message full name) and compared pairwise. The classification is
/// intentionally pragmatic — it covers the documented categories and does not do
/// exhaustive field-by-field diffing beyond what those categories require.
pub fn diff_manifests(
    old: &DescriptorContractManifest,
    new: &DescriptorContractManifest,
) -> Vec<ContractChange> {
    let mut changes = Vec::new();

    // ---- services + RPCs -------------------------------------------------
    let old_services: BTreeMap<String, &ServiceContract> =
        old.services.iter().map(|s| (s.full_name(), s)).collect();
    let new_services: BTreeMap<String, &ServiceContract> =
        new.services.iter().map(|s| (s.full_name(), s)).collect();

    for name in new_services.keys() {
        if !old_services.contains_key(name) {
            changes.push(ContractChange::new(
                ChangeKind::Additive,
                name.clone(),
                "service added",
            ));
        }
    }
    for (name, old_svc) in &old_services {
        match new_services.get(name) {
            None => {
                changes.push(ContractChange::new(
                    ChangeKind::Removed,
                    name.clone(),
                    "service removed",
                ));
                changes.push(ContractChange::new(
                    ChangeKind::SdkBreaking,
                    name.clone(),
                    "service removed (breaks generated SDKs)",
                ));
            }
            Some(new_svc) => diff_service_rpcs(old_svc, new_svc, &mut changes),
        }
    }

    // ---- messages --------------------------------------------------------
    let old_messages: BTreeMap<&str, &MessageContract> = old
        .messages
        .iter()
        .map(|m| (m.full_name.as_str(), m))
        .collect();
    let new_messages: BTreeMap<&str, &MessageContract> = new
        .messages
        .iter()
        .map(|m| (m.full_name.as_str(), m))
        .collect();

    for (name, new_msg) in &new_messages {
        match old_messages.get(name) {
            None => {
                // Only persisted messages carry a DB contract worth flagging; a
                // brand-new persisted table is additive surface but DB-relevant.
                if new_msg.db_table_security.is_some() {
                    changes.push(ContractChange::new(
                        ChangeKind::DbBreaking,
                        (*name).to_string(),
                        "persisted message added",
                    ));
                } else {
                    changes.push(ContractChange::new(
                        ChangeKind::Additive,
                        (*name).to_string(),
                        "message added",
                    ));
                }
            }
            Some(old_msg) => diff_message(old_msg, new_msg, &mut changes),
        }
    }
    for (name, old_msg) in &old_messages {
        if !new_messages.contains_key(name) {
            if old_msg.db_table_security.is_some() {
                changes.push(ContractChange::new(
                    ChangeKind::DbBreaking,
                    (*name).to_string(),
                    "persisted message removed",
                ));
            } else {
                changes.push(ContractChange::new(
                    ChangeKind::Removed,
                    (*name).to_string(),
                    "message removed",
                ));
            }
        }
    }

    changes
}

fn diff_service_rpcs(
    old_svc: &ServiceContract,
    new_svc: &ServiceContract,
    changes: &mut Vec<ContractChange>,
) {
    let old_rpcs: BTreeMap<String, &RpcContract> =
        old_svc.methods.iter().map(|m| (m.grpc_path(), m)).collect();
    let new_rpcs: BTreeMap<String, &RpcContract> =
        new_svc.methods.iter().map(|m| (m.grpc_path(), m)).collect();

    for (path, new_rpc) in &new_rpcs {
        match old_rpcs.get(path) {
            None => changes.push(ContractChange::new(
                ChangeKind::Additive,
                path.clone(),
                "rpc added",
            )),
            Some(old_rpc) => diff_rpc(path, old_rpc, new_rpc, changes),
        }
    }
    for (path, _old_rpc) in &old_rpcs {
        if !new_rpcs.contains_key(path) {
            changes.push(ContractChange::new(
                ChangeKind::Removed,
                path.clone(),
                "rpc removed",
            ));
            changes.push(ContractChange::new(
                ChangeKind::SdkBreaking,
                path.clone(),
                "rpc removed (breaks generated SDKs)",
            ));
        }
    }
}

fn diff_rpc(
    path: &str,
    old_rpc: &RpcContract,
    new_rpc: &RpcContract,
    changes: &mut Vec<ContractChange>,
) {
    // Request/response type change → SdkBreaking.
    if old_rpc.input_type != new_rpc.input_type {
        changes.push(ContractChange::new(
            ChangeKind::SdkBreaking,
            path.to_string(),
            format!(
                "request type changed: {} -> {}",
                old_rpc.input_type, new_rpc.input_type
            ),
        ));
    }
    if old_rpc.output_type != new_rpc.output_type {
        changes.push(ContractChange::new(
            ChangeKind::SdkBreaking,
            path.to_string(),
            format!(
                "response type changed: {} -> {}",
                old_rpc.output_type, new_rpc.output_type
            ),
        ));
    }
    if old_rpc.client_streaming != new_rpc.client_streaming
        || old_rpc.server_streaming != new_rpc.server_streaming
    {
        changes.push(ContractChange::new(
            ChangeKind::SdkBreaking,
            path.to_string(),
            format!(
                "streaming kind changed: {} -> {}",
                old_rpc.kind(),
                new_rpc.kind()
            ),
        ));
    }

    diff_endpoint_security(
        path,
        old_rpc.endpoint_security.as_ref(),
        new_rpc.endpoint_security.as_ref(),
        changes,
    );

    diff_event_contract(
        path,
        old_rpc.event_contract.as_ref(),
        new_rpc.event_contract.as_ref(),
        changes,
    );
    diff_emitted_events(path, &old_rpc.emits, &new_rpc.emits, changes);
}

/// Classify an event-contract difference. Any add/remove/field change to an
/// event contract is `EventBreaking`: CDC/outbox consumers subscribe by
/// `outbox_topic`/`event_type` and depend on the partition key, delivery
/// guarantee, and redaction profile staying stable.
fn diff_event_contract(
    path: &str,
    old: Option<&EventContract>,
    new: Option<&EventContract>,
    changes: &mut Vec<ContractChange>,
) {
    match (old, new) {
        (None, None) => {}
        (None, Some(n)) => changes.push(ContractChange::new(
            ChangeKind::EventBreaking,
            path.to_string(),
            format!("event_contract added (topic {:?})", n.outbox_topic),
        )),
        (Some(o), None) => changes.push(ContractChange::new(
            ChangeKind::EventBreaking,
            path.to_string(),
            format!("event_contract removed (was topic {:?})", o.outbox_topic),
        )),
        (Some(o), Some(n)) => {
            if o.outbox_topic != n.outbox_topic {
                changes.push(ContractChange::new(
                    ChangeKind::EventBreaking,
                    path.to_string(),
                    format!(
                        "outbox_topic changed: {:?} -> {:?}",
                        o.outbox_topic, n.outbox_topic
                    ),
                ));
            }
            if o.event_type != n.event_type {
                changes.push(ContractChange::new(
                    ChangeKind::EventBreaking,
                    path.to_string(),
                    format!(
                        "event_type changed: {:?} -> {:?}",
                        o.event_type, n.event_type
                    ),
                ));
            }
            if o.partition_key_field != n.partition_key_field {
                changes.push(ContractChange::new(
                    ChangeKind::EventBreaking,
                    path.to_string(),
                    format!(
                        "partition_key_field changed: {:?} -> {:?}",
                        o.partition_key_field, n.partition_key_field
                    ),
                ));
            }
            if o.delivery_guarantee != n.delivery_guarantee {
                changes.push(ContractChange::new(
                    ChangeKind::EventBreaking,
                    path.to_string(),
                    format!(
                        "delivery_guarantee changed: {:?} -> {:?}",
                        o.delivery_guarantee, n.delivery_guarantee
                    ),
                ));
            }
            if o.payload_redaction_profile != n.payload_redaction_profile {
                changes.push(ContractChange::new(
                    ChangeKind::EventBreaking,
                    path.to_string(),
                    format!(
                        "payload_redaction_profile changed: {:?} -> {:?}",
                        o.payload_redaction_profile, n.payload_redaction_profile
                    ),
                ));
            }
            if o.replay_compatibility != n.replay_compatibility {
                changes.push(ContractChange::new(
                    ChangeKind::EventBreaking,
                    path.to_string(),
                    format!(
                        "replay_compatibility changed: {:?} -> {:?}",
                        o.replay_compatibility, n.replay_compatibility
                    ),
                ));
            }
        }
    }
}

fn diff_emitted_events(
    path: &str,
    old: &[EmittedEvent],
    new: &[EmittedEvent],
    changes: &mut Vec<ContractChange>,
) {
    let old_sig = emitted_event_signature(old);
    let new_sig = emitted_event_signature(new);
    if old_sig == new_sig {
        return;
    }
    changes.push(ContractChange::new(
        ChangeKind::EventBreaking,
        path.to_string(),
        format!("emits[] changed: {old_sig:?} -> {new_sig:?}"),
    ));
}

fn emitted_event_signature(events: &[EmittedEvent]) -> Vec<(String, String, String, String, bool)> {
    let mut out: Vec<_> = events
        .iter()
        .map(|event| {
            (
                event.topic.clone(),
                event.partition_key_field.clone(),
                event.delivery_guarantee.clone(),
                event.payload_redaction_profile.clone(),
                event.conditional,
            )
        })
        .collect();
    out.sort();
    out
}

fn diff_endpoint_security(
    path: &str,
    old: Option<&EndpointSecurityContract>,
    new: Option<&EndpointSecurityContract>,
    changes: &mut Vec<ContractChange>,
) {
    match (old, new) {
        (None, None) => {}
        (None, Some(_)) => {
            // Endpoint security newly applied tightens enforcement.
            changes.push(ContractChange::new(
                ChangeKind::AuthBreaking,
                path.to_string(),
                "endpoint_security added",
            ));
        }
        (Some(_), None) => {
            changes.push(ContractChange::new(
                ChangeKind::AuthBreaking,
                path.to_string(),
                "endpoint_security removed",
            ));
        }
        (Some(o), Some(n)) => {
            if o.mode != n.mode {
                changes.push(ContractChange::new(
                    ChangeKind::AuthBreaking,
                    path.to_string(),
                    format!(
                        "auth mode changed: {} -> {}",
                        o.auth_mode_name(),
                        n.auth_mode_name()
                    ),
                ));
            }
            if o.scopes != n.scopes {
                changes.push(ContractChange::new(
                    ChangeKind::AuthBreaking,
                    path.to_string(),
                    format!("scopes changed: {:?} -> {:?}", o.scopes, n.scopes),
                ));
            }
            if o.roles != n.roles {
                changes.push(ContractChange::new(
                    ChangeKind::AuthBreaking,
                    path.to_string(),
                    format!("roles changed: {:?} -> {:?}", o.roles, n.roles),
                ));
            }
            if o.policy_ref != n.policy_ref {
                changes.push(ContractChange::new(
                    ChangeKind::AuthBreaking,
                    path.to_string(),
                    format!(
                        "policy_ref changed: {:?} -> {:?}",
                        o.policy_ref, n.policy_ref
                    ),
                ));
            }
            if o.required_assurance_level != n.required_assurance_level {
                changes.push(ContractChange::new(
                    ChangeKind::AuthBreaking,
                    path.to_string(),
                    format!(
                        "required_assurance_level changed: {} -> {}",
                        o.required_assurance_level, n.required_assurance_level
                    ),
                ));
            }

            // tenant_required: tightening (false->true) is auth-breaking; relaxing
            // (true->false) is a behavioral change.
            if o.tenant_required != n.tenant_required {
                let kind = if n.tenant_required {
                    ChangeKind::AuthBreaking
                } else {
                    ChangeKind::BehavioralChange
                };
                changes.push(ContractChange::new(
                    kind,
                    path.to_string(),
                    format!(
                        "tenant_required changed: {} -> {}",
                        o.tenant_required, n.tenant_required
                    ),
                ));
            }
            // csrf_required: tightening is auth-breaking, relaxing is behavioral.
            if o.csrf_required != n.csrf_required {
                let kind = if n.csrf_required {
                    ChangeKind::AuthBreaking
                } else {
                    ChangeKind::BehavioralChange
                };
                changes.push(ContractChange::new(
                    kind,
                    path.to_string(),
                    format!(
                        "csrf_required changed: {} -> {}",
                        o.csrf_required, n.csrf_required
                    ),
                ));
            }
            // internal_grpc_only: tightening (false->true, i.e. now internal-only)
            // is auth-breaking, opening up is behavioral.
            if o.internal_grpc_only != n.internal_grpc_only {
                let kind = if n.internal_grpc_only {
                    ChangeKind::AuthBreaking
                } else {
                    ChangeKind::BehavioralChange
                };
                changes.push(ContractChange::new(
                    kind,
                    path.to_string(),
                    format!(
                        "internal_grpc_only changed: {} -> {}",
                        o.internal_grpc_only, n.internal_grpc_only
                    ),
                ));
            }
        }
    }
}

fn diff_message(
    old_msg: &MessageContract,
    new_msg: &MessageContract,
    changes: &mut Vec<ContractChange>,
) {
    let path = new_msg.full_name.clone();

    // Table-level security change → DbBreaking.
    if old_msg.db_table_security != new_msg.db_table_security {
        changes.push(ContractChange::new(
            ChangeKind::DbBreaking,
            path.clone(),
            "db_table_security changed",
        ));
    }

    // Message-level event contract change → EventBreaking.
    diff_event_contract(
        &path,
        old_msg.event_contract.as_ref(),
        new_msg.event_contract.as_ref(),
        changes,
    );

    // Whether this message participates in persistence at all.
    let persisted = old_msg.db_table_security.is_some() || new_msg.db_table_security.is_some();

    let old_fields: BTreeMap<&str, &_> = old_msg
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f))
        .collect();
    let new_fields: BTreeMap<&str, &_> = new_msg
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f))
        .collect();

    for (name, new_field) in &new_fields {
        match old_fields.get(name) {
            None => {
                if persisted {
                    changes.push(ContractChange::new(
                        ChangeKind::DbBreaking,
                        format!("{path}.{name}"),
                        "field added on persisted message",
                    ));
                }
            }
            Some(old_field) => {
                if old_field.db_column_security != new_field.db_column_security {
                    changes.push(ContractChange::new(
                        ChangeKind::DbBreaking,
                        format!("{path}.{name}"),
                        "db_column_security changed",
                    ));
                } else if let (Some(o), Some(n)) = (
                    old_field.db_column_security.as_ref(),
                    new_field.db_column_security.as_ref(),
                ) {
                    // Covered by the inequality above, but keep output_view explicit
                    // for callers scanning details.
                    if o.output_view != n.output_view {
                        changes.push(ContractChange::new(
                            ChangeKind::DbBreaking,
                            format!("{path}.{name}"),
                            format!(
                                "output_view changed: {} -> {}",
                                o.output_view, n.output_view
                            ),
                        ));
                    }
                }
            }
        }
    }
    for (name, _old_field) in &old_fields {
        if !new_fields.contains_key(name) && persisted {
            changes.push(ContractChange::new(
                ChangeKind::DbBreaking,
                format!("{path}.{name}"),
                "field removed on persisted message",
            ));
        }
    }
}

/// Summarize a set of classified changes into a JSON object with per-kind counts
/// and the full change list.
pub fn summarize(changes: &[ContractChange]) -> serde_json::Value {
    let count = |k: ChangeKind| changes.iter().filter(|c| c.kind == k).count();
    let items: Vec<serde_json::Value> = changes
        .iter()
        .map(|c| {
            serde_json::json!({
                "kind": kind_str(c.kind),
                "path": c.path,
                "detail": c.detail,
            })
        })
        .collect();
    serde_json::json!({
        "contract_version": NATIVE_CONTRACT_VERSION,
        "additive": count(ChangeKind::Additive),
        "behavioral": count(ChangeKind::BehavioralChange),
        "auth_breaking": count(ChangeKind::AuthBreaking),
        "db_breaking": count(ChangeKind::DbBreaking),
        "sdk_breaking": count(ChangeKind::SdkBreaking),
        "event_breaking": count(ChangeKind::EventBreaking),
        "removed": count(ChangeKind::Removed),
        "changes": items,
    })
}

fn kind_str(kind: ChangeKind) -> &'static str {
    match kind {
        ChangeKind::Additive => "additive",
        ChangeKind::BehavioralChange => "behavioral",
        ChangeKind::AuthBreaking => "auth_breaking",
        ChangeKind::DbBreaking => "db_breaking",
        ChangeKind::SdkBreaking => "sdk_breaking",
        ChangeKind::EventBreaking => "event_breaking",
        ChangeKind::Removed => "removed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::descriptor_manifest::descriptor_contract_manifest;

    fn endpoint(mode: i32) -> EndpointSecurityContract {
        EndpointSecurityContract {
            mode,
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

    fn rpc(service_pkg: &str, service_name: &str, method: &str, mode: i32) -> RpcContract {
        RpcContract {
            service_name: service_name.to_string(),
            service_pkg: service_pkg.to_string(),
            method: method.to_string(),
            method_snake: method.to_lowercase(),
            input_type: format!(".{service_pkg}.{method}Request"),
            input_pkg: service_pkg.to_string(),
            input_short: format!("{method}Request"),
            output_type: format!(".{service_pkg}.{method}Response"),
            output_pkg: service_pkg.to_string(),
            output_short: format!("{method}Response"),
            client_streaming: false,
            server_streaming: false,
            endpoint_security: Some(endpoint(mode)),
            rest_contract: None,
            http_rule: None,
            sdk_surface: None,
            cli_scaffold: None,
            event_contract: None,
            emits: Vec::new(),
            dependency_contract: None,
            operation_kind: 1,
            precondition_contract: None,
            readback_contract: None,
            lifecycle_contract: None,
            idempotency_contract: None,
            error_contract: None,
        }
    }

    fn service(pkg: &str, name: &str, methods: Vec<RpcContract>) -> ServiceContract {
        ServiceContract {
            file_path: "test.proto".to_string(),
            package: pkg.to_string(),
            name: name.to_string(),
            native_service: None,
            sdk_surface: None,
            cli_scaffold: None,
            dependency_contract: None,
            methods,
        }
    }

    fn manifest(services: Vec<ServiceContract>) -> DescriptorContractManifest {
        DescriptorContractManifest {
            services,
            messages: Vec::new(),
        }
    }

    #[test]
    fn added_rpc_is_additive() {
        let pkg = "udb.test.v1";
        let old = manifest(vec![service(pkg, "Svc", vec![rpc(pkg, "Svc", "Alpha", 2)])]);
        let new = manifest(vec![service(
            pkg,
            "Svc",
            vec![rpc(pkg, "Svc", "Alpha", 2), rpc(pkg, "Svc", "Beta", 2)],
        )]);
        let changes = diff_manifests(&old, &new);
        assert!(
            changes
                .iter()
                .any(|c| c.kind == ChangeKind::Additive && c.path == "/udb.test.v1.Svc/Beta")
        );
        let summary = summarize(&changes);
        assert_eq!(summary["additive"], 1);
        assert_eq!(summary["auth_breaking"], 0);
        assert_eq!(summary["removed"], 0);
    }

    #[test]
    fn changed_auth_mode_is_auth_breaking() {
        let pkg = "udb.test.v1";
        // mode 1 = public -> mode 2 = bearer
        let old = manifest(vec![service(pkg, "Svc", vec![rpc(pkg, "Svc", "Alpha", 1)])]);
        let new = manifest(vec![service(pkg, "Svc", vec![rpc(pkg, "Svc", "Alpha", 2)])]);
        let changes = diff_manifests(&old, &new);
        assert!(changes.iter().any(|c| c.kind == ChangeKind::AuthBreaking
            && c.path == "/udb.test.v1.Svc/Alpha"
            && c.detail.contains("auth mode")));
        let summary = summarize(&changes);
        assert_eq!(summary["auth_breaking"], 1);
    }

    #[test]
    fn removed_service_is_removed_and_sdk_breaking() {
        let pkg = "udb.test.v1";
        let old = manifest(vec![
            service(pkg, "Svc", vec![rpc(pkg, "Svc", "Alpha", 2)]),
            service(pkg, "Gone", vec![rpc(pkg, "Gone", "Bye", 2)]),
        ]);
        let new = manifest(vec![service(pkg, "Svc", vec![rpc(pkg, "Svc", "Alpha", 2)])]);
        let changes = diff_manifests(&old, &new);
        assert!(
            changes
                .iter()
                .any(|c| c.kind == ChangeKind::Removed && c.path == "udb.test.v1.Gone")
        );
        assert!(
            changes
                .iter()
                .any(|c| c.kind == ChangeKind::SdkBreaking && c.path == "udb.test.v1.Gone")
        );
        let summary = summarize(&changes);
        assert_eq!(summary["removed"], 1);
        assert_eq!(summary["sdk_breaking"], 1);
        assert_eq!(summary["contract_version"], NATIVE_CONTRACT_VERSION);
    }

    #[test]
    fn changed_event_topic_is_event_breaking() {
        let pkg = "udb.test.v1";
        let event = |topic: &str| EventContract {
            event_type: "udb.test.thing.created.v1".to_string(),
            outbox_topic: topic.to_string(),
            partition_key_field: "id".to_string(),
            payload_redaction_profile: String::new(),
            delivery_guarantee: "at_least_once".to_string(),
            replay_compatibility: String::new(),
        };
        let mut old_rpc = rpc(pkg, "Svc", "Emit", 2);
        old_rpc.event_contract = Some(event("udb.test.thing.created.v1"));
        let mut new_rpc = rpc(pkg, "Svc", "Emit", 2);
        new_rpc.event_contract = Some(event("udb.test.thing.created.v2"));
        let old = manifest(vec![service(pkg, "Svc", vec![old_rpc])]);
        let new = manifest(vec![service(pkg, "Svc", vec![new_rpc])]);
        let changes = diff_manifests(&old, &new);
        assert!(
            changes.iter().any(|c| c.kind == ChangeKind::EventBreaking
                && c.path == "/udb.test.v1.Svc/Emit"
                && c.detail.contains("outbox_topic")),
            "topic rename must be event-breaking, got: {changes:?}"
        );
        let summary = summarize(&changes);
        assert_eq!(summary["event_breaking"], 1);
        assert_eq!(summary["auth_breaking"], 0);
    }

    #[test]
    fn changed_emits_topic_is_event_breaking() {
        let pkg = "udb.test.v1";
        let emit = |topic: &str| EmittedEvent {
            topic: topic.to_string(),
            partition_key_field: "id".to_string(),
            delivery_guarantee: "at_least_once".to_string(),
            payload_redaction_profile: "standard".to_string(),
            conditional: false,
        };
        let mut old_rpc = rpc(pkg, "Svc", "Emit", 2);
        old_rpc.emits = vec![emit("udb.test.thing.created.v1")];
        let mut new_rpc = rpc(pkg, "Svc", "Emit", 2);
        new_rpc.emits = vec![emit("udb.test.thing.created.v2")];
        let old = manifest(vec![service(pkg, "Svc", vec![old_rpc])]);
        let new = manifest(vec![service(pkg, "Svc", vec![new_rpc])]);
        let changes = diff_manifests(&old, &new);
        assert!(
            changes.iter().any(|c| c.kind == ChangeKind::EventBreaking
                && c.path == "/udb.test.v1.Svc/Emit"
                && c.detail.contains("emits[] changed")),
            "emits[] topic rename must be event-breaking, got: {changes:?}"
        );
        let summary = summarize(&changes);
        assert_eq!(summary["event_breaking"], 1);
    }

    #[test]
    fn live_manifest_diff_against_self_is_empty() {
        let live = descriptor_contract_manifest();
        let changes = diff_manifests(&live, &live);
        assert!(
            changes.is_empty(),
            "diffing the live manifest against itself must yield zero changes, got: {changes:?}"
        );
        let summary = summarize(&changes);
        assert_eq!(summary["additive"], 0);
        assert_eq!(summary["auth_breaking"], 0);
        assert_eq!(summary["db_breaking"], 0);
        assert_eq!(summary["sdk_breaking"], 0);
        assert_eq!(summary["event_breaking"], 0);
        assert_eq!(summary["removed"], 0);
        assert_eq!(summary["behavioral"], 0);
    }
}
