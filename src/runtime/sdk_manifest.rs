//! SDK-generation RPC manifest.
//!
//! `udb sdk generate` renders per-language client layers from templates, and the
//! list of RPCs it iterates is derived here from the *same* embedded
//! `FileDescriptorSet` the gRPC reflection service serves
//! ([`crate::runtime::native_catalog::embedded_file_descriptor_set`]). Proto stays
//! the single source of truth: there is no second, hand-maintained RPC list that
//! could drift from the wire contract.
//!
//! Only UDB services (package starting with `udb`) are surfaced; the vendored
//! `google.*` descriptors carry no services and are skipped regardless.

use std::collections::BTreeMap;

use crate::runtime::descriptor_manifest::descriptor_contract_manifest;

/// One RPC method, flattened with its owning service for easy per-RPC template
/// iteration. All names are the raw proto identifiers; mapping them to a
/// language's generated type names (e.g. Python `types_pb2.SelectRequest`, Go
/// `entityv1.SelectRequest`) is the template's job, since that mapping is
/// codegen-specific.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcDescriptor {
    /// Service short name, e.g. `DataBroker`.
    pub service_name: String,
    /// Service package, e.g. `udb.services.v1`.
    pub service_pkg: String,
    /// Method name, e.g. `Select`.
    pub method: String,
    /// `method` in snake_case, e.g. `select`.
    pub method_snake: String,
    /// Request message short name, e.g. `SelectRequest`.
    pub input_short: String,
    /// Request message package, e.g. `udb.entity.v1`.
    pub input_pkg: String,
    /// Response message short name.
    pub output_short: String,
    /// Response message package.
    pub output_pkg: String,
    pub client_streaming: bool,
    pub server_streaming: bool,
    pub native_service_id: String,
    pub logical_service_id: String,
    pub sdk_facade_name: String,
    pub cli_scaffold_group: String,
    pub auth_mode: String,
    pub roles: Vec<String>,
    pub scopes: Vec<String>,
    pub policy_ref: String,
    pub tenant_required: bool,
    pub tenant_field: String,
    pub project_field: String,
    pub credential_types: Vec<String>,
    pub requires_postgres: bool,
    pub requires_redis: bool,
    pub requires_object_store: bool,
    pub requires_kafka: bool,
    pub requires_feature: String,
    pub default_enabled: bool,
    pub surface: String,
    pub listener_kind: String,
    pub global_enablement_key: String,
    pub service_enablement_key: String,
    pub required_dependencies: Vec<String>,
    pub disabled_service_error_contract: String,
    pub browser_safe: bool,
    pub server_only: bool,
    pub default_deadline_ms: i32,
    pub default_max_attempts: i32,
    /// From the method's `EndpointSecurityContract`: whether CSRF is required.
    pub csrf_required: bool,
    /// From the method's `EndpointSecurityContract`: whether the RPC is only
    /// reachable on the internal gRPC (loopback) listener.
    pub internal_grpc_only: bool,
    /// From the owning service's `NativeServiceContract`: may bind the public
    /// data-plane listener.
    pub public_listener_allowed: bool,
    /// From the owning service's `NativeServiceContract`: may bind the
    /// control-plane listener.
    pub control_plane_listener_allowed: bool,
    /// From the owning service's `NativeServiceContract`: may bind the WebRTC
    /// peer listener.
    pub peer_listener_allowed: bool,
    /// From the method's `EndpointSecurity.operation_kind`: the authoritative
    /// state-change class (`read_only` | `mutation` | `destructive` |
    /// `unspecified`). The SINGLE source for client retry-safety and SDK probe
    /// classification — replaces any method-name guessing.
    pub operation_kind: String,
    /// Convenience: `operation_kind == read_only`. Only read-only RPCs are
    /// safe to auto-retry / safe to fully field-populate in conformance probes.
    pub read_only: bool,
    /// From the method's `IdempotencyContract`: whether the RPC is safe to
    /// replay (e.g. keyed mutations like `DataBroker/Upsert` and
    /// `DataBroker/Delete`). Drives SDK retry-on-transient-failure for
    /// mutations. Defaults `false` when no idempotency contract is annotated.
    pub replay_safe: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityDescriptor {
    pub message_type: String,
    pub short_name: String,
    pub table: String,
    pub primary_keys: Vec<String>,
    pub tenant_field: String,
    pub project_field: String,
    pub soft_delete_field: String,
    pub proto_package: String,
    pub language_classes: BTreeMap<String, String>,
    pub json_field_names: Vec<String>,
}

impl RpcDescriptor {
    /// Fully-qualified service name, e.g. `udb.services.v1.DataBroker`.
    pub fn service_full(&self) -> String {
        format!("{}.{}", self.service_pkg, self.service_name)
    }

    /// gRPC method path, e.g. `/udb.services.v1.DataBroker/Select`.
    pub fn grpc_path(&self) -> String {
        format!("/{}/{}", self.service_full(), self.method)
    }

    /// Streaming kind token used by template block filters.
    pub fn kind(&self) -> &'static str {
        match (self.client_streaming, self.server_streaming) {
            (false, false) => "unary",
            (false, true) => "server_streaming",
            (true, false) => "client_streaming",
            (true, true) => "bidi",
        }
    }
}

/// All UDB RPCs across every embedded service, sorted by
/// `(service_full, method)` for deterministic codegen output.
pub fn rpc_manifest() -> Vec<RpcDescriptor> {
    let manifest = descriptor_contract_manifest();
    let registry = crate::runtime::service::native_registry::native_service_registry();
    let mut out = Vec::new();
    for service in &manifest.services {
        let native = service.native_service.as_ref();
        let registry_entry = native.and_then(|_| {
            registry.iter().find(|entry| {
                entry
                    .proto_services
                    .iter()
                    .any(|full| full == &service.full_name())
            })
        });
        for method in &service.methods {
            let endpoint = method.endpoint_security.as_ref();
            let sdk = method.sdk_surface.as_ref().or(service.sdk_surface.as_ref());
            out.push(RpcDescriptor {
                service_name: service.name.clone(),
                service_pkg: service.package.clone(),
                method: method.method.clone(),
                method_snake: method.method_snake.clone(),
                input_short: method.input_short.clone(),
                input_pkg: method.input_pkg.clone(),
                output_short: method.output_short.clone(),
                output_pkg: method.output_pkg.clone(),
                client_streaming: method.client_streaming,
                server_streaming: method.server_streaming,
                native_service_id: native
                    .map(|n| {
                        crate::runtime::service::native_registry::canonical_service_id(
                            &n.service_id,
                        )
                    })
                    .unwrap_or_default(),
                logical_service_id: native
                    .map(|n| n.logical_service_id.clone())
                    .unwrap_or_default(),
                sdk_facade_name: native
                    .map(|n| n.sdk_facade_name.clone())
                    .unwrap_or_default(),
                cli_scaffold_group: native
                    .map(|n| n.cli_scaffold_group.clone())
                    .unwrap_or_default(),
                auth_mode: endpoint
                    .map(|security| security.auth_mode_name().to_string())
                    .unwrap_or_default(),
                roles: endpoint
                    .map(|security| security.roles.clone())
                    .unwrap_or_default(),
                scopes: endpoint
                    .map(|security| security.scopes.clone())
                    .unwrap_or_default(),
                policy_ref: endpoint
                    .map(|security| security.policy_ref.clone())
                    .unwrap_or_default(),
                tenant_required: endpoint
                    .map(|security| security.tenant_required)
                    .unwrap_or(false),
                tenant_field: endpoint
                    .map(|security| security.tenant_field.clone())
                    .unwrap_or_default(),
                project_field: endpoint
                    .map(|security| security.project_field.clone())
                    .unwrap_or_default(),
                credential_types: endpoint
                    .map(|security| {
                        security
                            .allowed_credential_types
                            .iter()
                            .map(|value| credential_type_name(*value).to_string())
                            .collect()
                    })
                    .unwrap_or_default(),
                requires_postgres: native.map(|n| n.requires_postgres).unwrap_or(false),
                requires_redis: native.map(|n| n.requires_redis).unwrap_or(false),
                requires_object_store: native.map(|n| n.requires_object_store).unwrap_or(false),
                requires_kafka: native.map(|n| n.requires_kafka).unwrap_or(false),
                requires_feature: native
                    .map(|n| n.requires_feature.clone())
                    .unwrap_or_default(),
                default_enabled: native.map(|n| n.default_enabled).unwrap_or(true),
                surface: registry_entry
                    .map(|entry| entry.surface.as_str().to_string())
                    .unwrap_or_else(|| "data_plane".to_string()),
                listener_kind: registry_entry
                    .map(|entry| entry.listener_kind.as_str().to_string())
                    .unwrap_or_else(|| "public".to_string()),
                global_enablement_key: if native.is_some() {
                    "UDB_NATIVE_SERVICES_ENABLED".to_string()
                } else {
                    String::new()
                },
                service_enablement_key: native
                    .map(|n| {
                        let service_id =
                            crate::runtime::service::native_registry::canonical_service_id(
                                &n.service_id,
                            );
                        format!("UDB_NATIVE_{}_ENABLED", service_id.to_ascii_uppercase())
                    })
                    .unwrap_or_default(),
                required_dependencies: registry_entry
                    .map(|entry| entry.required_backends.clone())
                    .unwrap_or_default(),
                disabled_service_error_contract: if native.is_some() {
                    "UNIMPLEMENTED: native service '<service_id>' is disabled".to_string()
                } else {
                    String::new()
                },
                browser_safe: sdk.map(|s| s.browser_safe).unwrap_or(false),
                server_only: sdk.map(|s| s.server_only).unwrap_or(false),
                default_deadline_ms: sdk.map(|s| s.default_deadline_ms).unwrap_or_default(),
                default_max_attempts: sdk.map(|s| s.default_max_attempts).unwrap_or_default(),
                csrf_required: endpoint
                    .map(|security| security.csrf_required)
                    .unwrap_or(false),
                internal_grpc_only: endpoint
                    .map(|security| security.internal_grpc_only)
                    .unwrap_or(false),
                public_listener_allowed: native.map(|n| n.public_listener_allowed).unwrap_or(false),
                control_plane_listener_allowed: native
                    .map(|n| n.control_plane_listener_allowed)
                    .unwrap_or(false),
                peer_listener_allowed: native.map(|n| n.peer_listener_allowed).unwrap_or(false),
                operation_kind: crate::runtime::descriptor_manifest::operation_kind_name(
                    method.operation_kind,
                )
                .to_string(),
                read_only: method.operation_kind == 1,
                replay_safe: method
                    .idempotency_contract
                    .as_ref()
                    .map(|c| c.replay_safe)
                    .unwrap_or(false),
            });
        }
    }
    out.sort_by(|a, b| {
        a.service_full()
            .cmp(&b.service_full())
            .then_with(|| a.method.cmp(&b.method))
    });
    out
}

pub fn entity_manifest() -> Vec<EntityDescriptor> {
    let descriptor = descriptor_contract_manifest();
    let catalog = crate::runtime::native_catalog::native_manifest();
    let mut out = Vec::new();
    for message in descriptor
        .messages
        .iter()
        .filter(|message| message.db_table_security.is_some())
    {
        let Some(table) = crate::broker::table_for_message(catalog, &message.full_name)
            .or_else(|| crate::broker::table_for_message(catalog, &message.name))
        else {
            continue;
        };
        out.push(EntityDescriptor {
            message_type: message.name.clone(),
            short_name: message.name.clone(),
            table: table.table.clone(),
            primary_keys: table.primary_key.clone(),
            tenant_field: table.table_security.tenant_column.clone(),
            project_field: table.table_security.project_column.clone(),
            soft_delete_field: if table.soft_delete {
                table.soft_delete_column.clone()
            } else {
                String::new()
            },
            proto_package: table.proto_package.clone(),
            language_classes: table.language_classes.clone(),
            json_field_names: table
                .columns
                .iter()
                .map(|column| column.field_name.clone())
                .collect(),
        });
    }
    out.sort_by(|a, b| {
        a.message_type
            .cmp(&b.message_type)
            .then_with(|| a.table.cmp(&b.table))
    });
    out
}

fn credential_type_name(value: i32) -> &'static str {
    match value {
        1 => "bearer_jwt",
        2 => "session",
        3 => "api_key",
        4 => "service_account",
        5 => "mtls",
        6 => "oidc",
        7 => "saml",
        8 => "webauthn",
        9 => "external_jwt",
        _ => "unspecified",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_derived_from_embedded_descriptors() {
        let manifest = rpc_manifest();
        assert!(
            !manifest.is_empty(),
            "embedded descriptor set yielded no RPCs"
        );
        // Every entry must be a UDB service with a non-empty method + types.
        for rpc in &manifest {
            assert!(rpc.service_pkg.starts_with("udb"), "non-udb service leaked");
            assert!(!rpc.method.is_empty());
            assert!(!rpc.input_short.is_empty());
            assert!(!rpc.output_short.is_empty());
        }
    }

    #[test]
    fn data_broker_select_surface_is_present() {
        let manifest = rpc_manifest();
        let select = manifest
            .iter()
            .find(|r| r.service_name == "DataBroker" && r.method == "Select")
            .expect("DataBroker/Select must be in the manifest");
        assert_eq!(select.service_pkg, "udb.services.v1");
        assert_eq!(select.method_snake, "select");
        assert_eq!(select.input_short, "SelectRequest");
        assert_eq!(select.kind(), "unary");
        assert_eq!(select.grpc_path(), "/udb.services.v1.DataBroker/Select");
    }

    #[test]
    fn native_control_plane_services_are_present() {
        let manifest = rpc_manifest();
        let services: std::collections::BTreeSet<String> =
            manifest.iter().map(|r| r.service_full()).collect();
        // Public broker plus the native control-plane services that generated
        // SDKs must expose with metadata parity.
        for expected in [
            "udb.services.v1.DataBroker",
            "udb.core.authn.services.v1.AuthnService",
            "udb.core.authz.services.v1.AuthzService",
            "udb.core.apikey.services.v1.ApiKeyService",
            "udb.core.tenant.services.v1.TenantService",
            "udb.core.notification.services.v1.NotificationService",
            "udb.core.analytics.services.v1.AnalyticsService",
            "udb.core.storage.services.v1.StorageService",
            "udb.core.asset.services.v1.AssetService",
            "udb.core.webrtc.services.v1.RoomService",
            "udb.core.webrtc.services.v1.PeerService",
            "udb.core.webrtc.services.v1.TrackService",
            "udb.core.webrtc.services.v1.TurnService",
            "udb.core.webrtc.services.v1.SignalingService",
        ] {
            assert!(
                services.iter().any(|s| s == expected),
                "expected service {expected} not found in manifest; present: {services:?}"
            );
        }
    }

    #[test]
    fn every_descriptor_native_service_id_reaches_sdk_manifest() {
        let descriptor = descriptor_contract_manifest();
        let expected: std::collections::BTreeSet<String> = descriptor
            .services
            .iter()
            .filter_map(|service| service.native_service.as_ref())
            .map(|native| {
                crate::runtime::service::native_registry::canonical_service_id(&native.service_id)
            })
            .collect();
        let actual: std::collections::BTreeSet<String> = rpc_manifest()
            .into_iter()
            .filter_map(|rpc| {
                (!rpc.native_service_id.trim().is_empty()).then_some(rpc.native_service_id)
            })
            .collect();

        assert!(
            expected.is_subset(&actual),
            "descriptor native service ids must all appear in SDK manifest; missing: {:?}",
            expected.difference(&actual).collect::<Vec<_>>()
        );
    }

    #[test]
    fn entity_manifest_resolves_catalog_tables() {
        let entities = entity_manifest();
        assert!(
            !entities.is_empty(),
            "db_table_security messages should produce SDK entity descriptors"
        );
        for entity in &entities {
            assert!(
                !entity.table.trim().is_empty(),
                "entity {} must resolve a physical table",
                entity.message_type
            );
            assert!(
                !entity.primary_keys.is_empty(),
                "entity {} must carry primary keys",
                entity.message_type
            );
        }
        assert!(
            entities
                .iter()
                .any(|entity| entity.message_type == "Tenant"),
            "expected native Tenant entity descriptor in manifest"
        );
    }

    #[test]
    fn native_services_include_phase_o_selection_metadata() {
        let manifest = rpc_manifest();
        let storage = manifest
            .iter()
            .find(|r| r.native_service_id == "storage" && r.service_name == "StorageService")
            .expect("StorageService RPC must be in the SDK manifest");
        assert_eq!(storage.surface, "native_control_plane");
        assert_eq!(storage.listener_kind, "control_plane");
        assert_eq!(storage.global_enablement_key, "UDB_NATIVE_SERVICES_ENABLED");
        assert_eq!(storage.service_enablement_key, "UDB_NATIVE_STORAGE_ENABLED");
        assert!(
            storage
                .required_dependencies
                .iter()
                .any(|dep| dep == "postgres")
        );
        assert!(
            storage
                .disabled_service_error_contract
                .contains("UNIMPLEMENTED")
        );

        let webrtc = manifest
            .iter()
            .find(|r| r.native_service_id == "webrtc_signaling")
            .expect("SignalingService RPC must be in the SDK manifest");
        assert_eq!(webrtc.surface, "webrtc_peer_plane");
        assert_eq!(webrtc.listener_kind, "webrtc_peer");
    }

    #[test]
    fn server_streaming_select_v2_is_flagged() {
        let manifest = rpc_manifest();
        if let Some(v2) = manifest
            .iter()
            .find(|r| r.service_name == "DataBroker" && r.method == "SelectV2")
        {
            assert!(v2.server_streaming, "SelectV2 should be server-streaming");
            assert_eq!(v2.kind(), "server_streaming");
        }
    }

    /// urgent_fix #19: every descriptor service the SDK generator emits must have
    /// a generated robustness client in EVERY language. The generated clients are
    /// committed; this gate fails when a service is missing from any of them
    /// (which is exactly how Storage/Asset/IdP/WebRTC silently went un-generated
    /// in all six SDKs). Driven from `rpc_manifest()` — the same source the
    /// generator iterates — so it cannot drift from what `udb sdk generate` emits.
    /// Run `udb sdk generate all` and commit the result to satisfy it.
    #[test]
    fn every_manifest_service_has_a_generated_client_in_every_language() {
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        // Skip only when the SDK tree is entirely absent (e.g. a packaged crate
        // without `sdk/`); in the repo checkout this is a hard gate.
        if !repo.join("sdk").is_dir() {
            return;
        }

        // (language label, generated robustness client path).
        let clients = [
            ("typescript", "sdk/typescript/generatedClient.ts"),
            ("python", "sdk/python/udb_client/generated_client.py"),
            ("go", "sdk/go/udbclient/generated_client.go"),
            (
                "java",
                "sdk/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java",
            ),
            ("csharp", "sdk/csharp/Udb.Client/GeneratedClient.cs"),
            ("php", "sdk/php/src/Generated/GeneratedClient.php"),
        ];

        // Distinct services the generator emits (full proto name + short name).
        let mut services: std::collections::BTreeSet<(String, String)> =
            std::collections::BTreeSet::new();
        for rpc in rpc_manifest() {
            services.insert((rpc.service_full(), rpc.service_name.clone()));
        }
        assert!(!services.is_empty(), "rpc_manifest yielded no services");

        let mut missing: Vec<String> = Vec::new();
        for (lang, rel) in clients {
            let path = repo.join(rel);
            let body = std::fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("cannot read generated client {rel}: {err}"));
            for (full, short) in &services {
                // A client "covers" a service if it references its full proto
                // name or its short service name anywhere (interface, comment,
                // or grpc path).
                if !body.contains(full.as_str()) && !body.contains(short.as_str()) {
                    missing.push(format!("{lang}: {full}"));
                }
            }
        }

        assert!(
            missing.is_empty(),
            "generated SDK clients are missing services (run `udb sdk generate all` and commit): {missing:?}"
        );
    }
}
