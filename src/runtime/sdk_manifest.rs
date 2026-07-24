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

use crate::runtime::descriptor_manifest::descriptor_contract_manifest_static;

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
    /// SDK-facing method alias from `SdkSurfaceOptions.method_alias`, or the
    /// wire method name when no alias is annotated.
    pub method_alias: String,
    /// SDK alias in snake_case.
    pub method_alias_snake: String,
    /// SDK alias in lowerCamelCase.
    pub method_alias_camel: String,
    /// SDK alias in PascalCase.
    pub method_alias_pascal: String,
    /// REST/OpenAPI operation id from `SdkSurfaceOptions.rest_operation_id`,
    /// falling back to the SDK alias in lowerCamelCase for old descriptors.
    pub rest_operation_id: String,
    /// Request message short name, e.g. `SelectRequest`.
    pub input_short: String,
    /// Request message package, e.g. `udb.entity.v1`.
    pub input_pkg: String,
    /// Response message short name.
    pub output_short: String,
    /// Response message package.
    pub output_pkg: String,
    pub http_verb: String,
    pub http_path: String,
    pub http_body: String,
    pub http_response_body: String,
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
    pub version_field: String,
    pub proto_package: String,
    pub language_classes: BTreeMap<String, String>,
    pub json_field_names: Vec<String>,
    pub relations: Vec<EntityRelationDescriptor>,
    /// Per-column type/security metadata (B2) — what a typed-marshalling generator
    /// (e.g. the Go `toUDBRecord`/`fromUDBRow` emitter) needs to turn a proto
    /// message into a UDB record and back without hand-written `map[string]any`.
    pub columns: Vec<EntityColumnDescriptor>,
}

/// Per-column metadata for typed codegen (B2). Sourced from `ManifestColumn`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct EntityColumnDescriptor {
    /// proto field name (snake_case) — the record key.
    pub field_name: String,
    pub column_name: String,
    /// proto scalar/message type token (e.g. `string`, `int64`, `bool`,
    /// `.google.protobuf.Timestamp`, or an enum type name).
    pub proto_type: String,
    pub sql_type: String,
    pub not_null: bool,
    pub is_array: bool,
    /// proto3 `optional` ⇒ protoc-gen-go renders this field as a POINTER, and an
    /// unset value must be omitted from the record rather than written as a zero.
    pub has_presence: bool,
    /// Non-empty ⇒ this column is a proto enum; the generator emits a
    /// short-token ⇄ enum map from these values.
    pub enum_values: Vec<String>,
    pub is_json: bool,
    pub is_jsonb: bool,
    /// Column is server-populated (e.g. default/PK); omit from generated INSERTs.
    pub exclude_from_insert: bool,
    /// PII blind-index column — the generator scaffolds the HMAC lookup plumbing.
    pub is_blind_index: bool,
    pub is_pii: bool,
    /// Whether the consumer's PROTO MESSAGE declares this field. UDB injects
    /// audit columns (`created_at`/`updated_at`/`created_by`) into the TABLE
    /// when the proto lacks them; those have no Go getter, so typed marshalling
    /// must skip them (the broker fills their defaults server-side). `true` on
    /// the embedded-descriptor path, whose entities declare audit fields
    /// in-proto. `#[serde(skip)]`: the sdk-manifest JSON surface is unchanged.
    #[serde(skip)]
    pub declared_in_proto: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct EntityRelationDescriptor {
    pub name: String,
    pub kind: String,
    pub local_fields: Vec<String>,
    pub target_message_type: String,
    pub target_table: String,
    pub target_fields: Vec<String>,
    pub on_delete: String,
    pub on_update: String,
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
    let manifest = descriptor_contract_manifest_static();
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
            let method_alias = sdk
                .map(|surface| surface.method_alias.trim())
                .filter(|alias| !alias.is_empty())
                .unwrap_or(&method.method)
                .to_string();
            let method_alias_snake = alias_snake_case(&method_alias);
            let method_alias_camel = alias_camel_case(&method_alias);
            let method_alias_pascal = alias_pascal_case(&method_alias);
            let rest_operation_id = sdk
                .map(|surface| surface.rest_operation_id.trim())
                .filter(|id| !id.is_empty())
                .unwrap_or(&method_alias_camel)
                .to_string();
            let http = method.http_rule.as_ref();
            out.push(RpcDescriptor {
                service_name: service.name.clone(),
                service_pkg: service.package.clone(),
                method: method.method.clone(),
                method_snake: method.method_snake.clone(),
                method_alias,
                method_alias_snake,
                method_alias_camel,
                method_alias_pascal,
                rest_operation_id,
                input_short: method.input_short.clone(),
                input_pkg: method.input_pkg.clone(),
                output_short: method.output_short.clone(),
                output_pkg: method.output_pkg.clone(),
                http_verb: http.map(|h| h.verb.clone()).unwrap_or_default(),
                http_path: http.map(|h| h.path.clone()).unwrap_or_default(),
                http_body: http.map(|h| h.body.clone()).unwrap_or_default(),
                http_response_body: http.map(|h| h.response_body.clone()).unwrap_or_default(),
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
    let descriptor = descriptor_contract_manifest_static();
    let catalog = crate::runtime::native_catalog::native_manifest();
    let mut out = Vec::new();
    for message in descriptor
        .messages
        .iter()
        .filter(|message| message.db_table_security.is_some())
    {
        let Ok(table) = crate::broker::resolve_table_for_message(catalog, &message.full_name)
        else {
            continue;
        };
        out.push(entity_descriptor_from_table(
            message.full_name.clone(),
            message.name.clone(),
            table,
            &catalog.tables,
            None,
            None,
        ));
    }
    out.sort_by(|a, b| {
        a.message_type
            .cmp(&b.message_type)
            .then_with(|| a.table.cmp(&b.table))
    });
    out
}

/// Enum registry for typed codegen: `(proto_package, enum_name)` → the enum's
/// full value NAMES (e.g. `ORDER_STATUS_ACTIVE`), harvested from file-level and
/// message-nested enum declarations across the parsed proto set.
type EnumRegistry = BTreeMap<(String, String), Vec<String>>;

/// Build one [`EntityDescriptor`] from a resolved [`ManifestTable`]. Shared by the
/// embedded [`entity_manifest`] and the consumer-proto [`entity_manifest_from_proto_dir`]
/// so both emit identically-shaped entities.
///
/// `declared_fields` — the field names the entity's PROTO MESSAGE actually
/// declares (None ⇒ treat every column as declared, the embedded-descriptor
/// case). `enum_registry` — parsed enum declarations used to auto-fill
/// `enum_values` for enum-typed columns without an explicit annotation.
fn entity_descriptor_from_table(
    message_type: String,
    short_name: String,
    table: &crate::generation::manifest::ManifestTable,
    all_tables: &[crate::generation::manifest::ManifestTable],
    declared_fields: Option<&std::collections::BTreeSet<String>>,
    enum_registry: Option<&EnumRegistry>,
) -> EntityDescriptor {
    EntityDescriptor {
        message_type,
        short_name,
        table: table.table.clone(),
        primary_keys: table.primary_key.clone(),
        tenant_field: table.table_security.tenant_column.clone(),
        project_field: table.table_security.project_column.clone(),
        soft_delete_field: if table.soft_delete {
            table.soft_delete_column.clone()
        } else {
            String::new()
        },
        version_field: entity_version_field(table).unwrap_or_default(),
        proto_package: table.proto_package.clone(),
        language_classes: table.language_classes.clone(),
        json_field_names: table
            .columns
            .iter()
            .map(|column| column.field_name.clone())
            .collect(),
        relations: entity_relations(table, all_tables),
        columns: table
            .columns
            .iter()
            .map(|column| EntityColumnDescriptor {
                field_name: column.field_name.clone(),
                column_name: column.column_name.clone(),
                proto_type: column.proto_type.clone(),
                sql_type: column.sql_type.clone(),
                not_null: column.not_null,
                is_array: column.is_array,
                has_presence: column.has_presence,
                enum_values: if column.enum_values.is_empty() {
                    resolve_enum_values(&column.proto_type, &table.proto_package, enum_registry)
                } else {
                    column.enum_values.clone()
                },
                is_json: column.is_json,
                is_jsonb: column.is_jsonb,
                exclude_from_insert: column.exclude_from_insert,
                is_blind_index: column.security.is_blind_index,
                is_pii: column.security.is_pii,
                declared_in_proto: declared_fields
                    .map(|fields| fields.contains(&column.field_name))
                    .unwrap_or(true),
            })
            .collect(),
    }
}

/// Build the entity manifest from a CONSUMER's own `.proto` tree (B1 —
/// `udb sdk generate --project-proto <dir>`). Every `*.proto` under `dir` is parsed
/// for `pg_table`/`pg_column`-style annotations (namespace-agnostic — the consumer
/// may use their OWN annotation package, e.g. `acme.common.v1.table`), a
/// [`CatalogManifest`](crate::generation::CatalogManifest) is built from the parsed
/// schemas, and one typed [`EntityDescriptor`] is derived per table. Only the ENTITY
/// surface is sourced from the consumer; the RPC surface stays UDB's own DataBroker
/// contract (the generated typed repositories call those RPCs with the consumer's
/// message types).
pub fn entity_manifest_from_proto_dir(
    dir: &std::path::Path,
) -> Result<Vec<EntityDescriptor>, String> {
    let config = crate::parser::ParserConfig::default();
    let mut schemas = Vec::new();
    let mut file_enums = Vec::new();
    parse_project_proto_dir(dir, &config, &mut schemas, &mut file_enums)?;

    // The proto-DECLARED field set per message, captured BEFORE the catalog
    // build injects audit columns into tables. Typed marshalling must only
    // cover fields the message actually has a getter for (V19-3).
    let mut declared: BTreeMap<(String, String), std::collections::BTreeSet<String>> =
        BTreeMap::new();
    // Enum registry from file-level AND message-nested enum declarations, so an
    // enum-typed column resolves without an explicit `enum_values` annotation
    // (V19-1). Same-package only — a cross-package enum falls back to the
    // symmetric unsupported-column TODO (its Go type needs its own import
    // alias, a documented follow-up).
    let mut enum_registry: EnumRegistry = BTreeMap::new();
    for decl in &file_enums {
        enum_registry
            .entry((decl.proto_package.clone(), decl.enum_def.name.clone()))
            .or_insert_with(|| {
                decl.enum_def
                    .values
                    .iter()
                    .map(|value| value.name.clone())
                    .collect()
            });
    }
    for schema in &schemas {
        declared.insert(
            (schema.proto_package.clone(), schema.message_name.clone()),
            schema
                .columns
                .iter()
                .map(|column| column.field_name.clone())
                .collect(),
        );
        for nested in &schema.nested_enums {
            enum_registry
                .entry((schema.proto_package.clone(), nested.name.clone()))
                .or_insert_with(|| {
                    nested
                        .values
                        .iter()
                        .map(|value| value.name.clone())
                        .collect()
                });
        }
    }

    let catalog = crate::generation::CatalogManifest::from_schemas(&schemas)
        .map_err(|err| format!("failed to build catalog from project protos: {err}"))?;
    let mut out = Vec::new();
    for table in &catalog.tables {
        let message_type = if table.proto_package.trim().is_empty() {
            table.message_name.clone()
        } else {
            format!("{}.{}", table.proto_package, table.message_name)
        };
        let declared_fields =
            declared.get(&(table.proto_package.clone(), table.message_name.clone()));
        out.push(entity_descriptor_from_table(
            message_type,
            table.message_name.clone(),
            table,
            &catalog.tables,
            declared_fields,
            Some(&enum_registry),
        ));
    }
    if out.is_empty() {
        return Err(format!(
            "no entity-annotated messages found under {} (expected pg_table/table annotations)",
            dir.display()
        ));
    }
    out.sort_by(|a, b| {
        a.message_type
            .cmp(&b.message_type)
            .then_with(|| a.table.cmp(&b.table))
    });
    Ok(out)
}

/// Resolve an enum-typed column against the parsed enum registry by the type's
/// trailing name segment. Fills value names only for a SAME-PACKAGE enum (bare
/// or package-qualified reference); a cross-package enum stays empty so the
/// emitter's symmetric unsupported-column TODO applies instead of emitting a
/// Go type lookup under the wrong import alias.
fn resolve_enum_values(
    proto_type: &str,
    entity_package: &str,
    enum_registry: Option<&EnumRegistry>,
) -> Vec<String> {
    let Some(registry) = enum_registry else {
        return Vec::new();
    };
    let trimmed = proto_type.trim_start_matches('.');
    let Some(name) = trimmed.rsplit('.').next() else {
        return Vec::new();
    };
    // Scalars/messages never resolve: registry keys are enum declarations only.
    registry
        .get(&(entity_package.to_string(), name.to_string()))
        .cloned()
        .unwrap_or_default()
}

/// Recursively parse every `*.proto` under `dir` into [`ProtoSchema`]s, also
/// collecting file-level enum declarations for the codegen enum registry.
fn parse_project_proto_dir(
    dir: &std::path::Path,
    config: &crate::parser::ParserConfig,
    out: &mut Vec<crate::ast::ProtoSchema>,
    file_enums: &mut Vec<crate::parser::FileEnumDecl>,
) -> Result<(), String> {
    let entries = std::fs::read_dir(dir).map_err(|err| {
        format!(
            "cannot read project proto directory {}: {err}",
            dir.display()
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("cannot read directory entry: {err}"))?;
        let path = entry.path();
        if path.is_dir() {
            parse_project_proto_dir(&path, config, out, file_enums)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("proto") {
            let source = std::fs::read(&path)
                .map_err(|err| format!("cannot read {}: {err}", path.display()))?;
            let mut report = crate::parser::parse_proto_source(
                &source,
                path.to_string_lossy().to_string(),
                config,
            )
            .map_err(|err| format!("failed to parse {}: {err}", path.display()))?;
            out.extend(report.schemas);
            file_enums.append(&mut report.file_enums);
        }
    }
    Ok(())
}

fn entity_relations(
    table: &crate::generation::manifest::ManifestTable,
    tables: &[crate::generation::manifest::ManifestTable],
) -> Vec<EntityRelationDescriptor> {
    let mut out = Vec::new();
    for fk in &table.foreign_keys {
        if fk.columns.is_empty() || fk.ref_table.trim().is_empty() {
            continue;
        }
        let target = tables.iter().find(|candidate| {
            candidate.table == fk.ref_table
                && (fk.ref_schema.trim().is_empty() || candidate.schema == fk.ref_schema)
        });
        let Some(target) = target else {
            continue;
        };
        let local_fields = fk
            .columns
            .iter()
            .filter_map(|column| manifest_field_for_column(table, column))
            .collect::<Vec<_>>();
        let target_columns = if fk.ref_columns.is_empty() {
            target.primary_key.as_slice()
        } else {
            fk.ref_columns.as_slice()
        };
        let target_fields = target_columns
            .iter()
            .filter_map(|column| manifest_field_for_column(target, column))
            .collect::<Vec<_>>();
        if local_fields.is_empty() || target_fields.is_empty() {
            continue;
        }
        out.push(EntityRelationDescriptor {
            name: relation_name(&local_fields, target),
            kind: "belongs_to".to_string(),
            local_fields,
            target_message_type: manifest_message_type(target),
            target_table: format!("{}.{}", target.schema, target.table),
            target_fields,
            on_delete: fk.on_delete.clone(),
            on_update: fk.on_update.clone(),
        });
    }
    for child in tables {
        for fk in &child.foreign_keys {
            if fk.columns.is_empty() || fk.ref_table != table.table {
                continue;
            }
            if !fk.ref_schema.trim().is_empty() && fk.ref_schema != table.schema {
                continue;
            }
            let local_columns = if fk.ref_columns.is_empty() {
                table.primary_key.as_slice()
            } else {
                fk.ref_columns.as_slice()
            };
            let local_fields = local_columns
                .iter()
                .filter_map(|column| manifest_field_for_column(table, column))
                .collect::<Vec<_>>();
            let target_fields = fk
                .columns
                .iter()
                .filter_map(|column| manifest_field_for_column(child, column))
                .collect::<Vec<_>>();
            if local_fields.is_empty() || target_fields.is_empty() {
                continue;
            }
            out.push(EntityRelationDescriptor {
                name: has_many_relation_name(child),
                kind: "has_many".to_string(),
                local_fields,
                target_message_type: manifest_message_type(child),
                target_table: format!("{}.{}", child.schema, child.table),
                target_fields,
                on_delete: fk.on_delete.clone(),
                on_update: fk.on_update.clone(),
            });
        }
    }
    out.sort_by(|a, b| {
        a.name
            .cmp(&b.name)
            .then_with(|| a.target_message_type.cmp(&b.target_message_type))
            .then_with(|| a.local_fields.cmp(&b.local_fields))
    });
    out
}

fn entity_version_field(table: &crate::generation::manifest::ManifestTable) -> Option<String> {
    const VERSION_FIELD_NAMES: [&str; 4] = ["version", "revision", "row_version", "lock_version"];
    table
        .columns
        .iter()
        .find(|column| {
            VERSION_FIELD_NAMES.contains(&column.field_name.as_str())
                || VERSION_FIELD_NAMES.contains(&column.column_name.as_str())
        })
        .map(|column| column.field_name.clone())
}

fn manifest_field_for_column(
    table: &crate::generation::manifest::ManifestTable,
    column_name: &str,
) -> Option<String> {
    table
        .columns
        .iter()
        .find(|column| column.column_name == column_name)
        .map(|column| column.field_name.clone())
}

fn manifest_message_type(table: &crate::generation::manifest::ManifestTable) -> String {
    if table.proto_package.trim().is_empty() {
        table.message_name.clone()
    } else {
        format!("{}.{}", table.proto_package, table.message_name)
    }
}

fn relation_name(
    local_fields: &[String],
    target: &crate::generation::manifest::ManifestTable,
) -> String {
    local_fields
        .first()
        .map(|field| {
            field
                .strip_suffix("_id")
                .or_else(|| field.strip_suffix("_uuid"))
                .unwrap_or(field)
                .to_string()
        })
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| alias_snake_case(&target.message_name))
}

fn has_many_relation_name(table: &crate::generation::manifest::ManifestTable) -> String {
    let table_name = alias_snake_case(&table.table);
    if !table_name.trim().is_empty() {
        return table_name;
    }
    let alias = alias_snake_case(&table.message_name);
    if alias.ends_with('s') {
        alias
    } else if let Some(stem) = alias.strip_suffix('y') {
        format!("{stem}ies")
    } else {
        format!("{alias}s")
    }
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

fn alias_words(value: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = value.trim().chars().collect();
    for (idx, ch) in chars.iter().copied().enumerate() {
        if !ch.is_ascii_alphanumeric() {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            continue;
        }
        if !current.is_empty() {
            let prev = current.chars().last().unwrap_or_default();
            let next = chars.get(idx + 1).copied();
            let lower_to_upper = prev.is_ascii_lowercase() && ch.is_ascii_uppercase();
            let acronym_to_word = prev.is_ascii_uppercase()
                && ch.is_ascii_uppercase()
                && next.is_some_and(|n| n.is_ascii_lowercase());
            let alpha_digit = prev.is_ascii_alphabetic() && ch.is_ascii_digit();
            let digit_alpha = prev.is_ascii_digit() && ch.is_ascii_alphabetic();
            if lower_to_upper || acronym_to_word || alpha_digit || digit_alpha {
                words.push(std::mem::take(&mut current));
            }
        }
        current.push(ch);
    }
    if !current.is_empty() {
        words.push(current);
    }
    if words.is_empty() {
        vec![value.trim().to_string()]
    } else {
        words
    }
}

fn title_word(word: &str) -> String {
    let mut chars = word.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut out = String::new();
    out.push(first.to_ascii_uppercase());
    for ch in chars {
        out.push(ch.to_ascii_lowercase());
    }
    out
}

fn alias_snake_case(value: &str) -> String {
    alias_words(value)
        .into_iter()
        .map(|word| word.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join("_")
}

fn alias_pascal_case(value: &str) -> String {
    alias_words(value)
        .into_iter()
        .map(|word| title_word(&word))
        .collect::<String>()
}

fn alias_camel_case(value: &str) -> String {
    let words = alias_words(value);
    let mut out = String::new();
    for (idx, word) in words.iter().enumerate() {
        if idx == 0 {
            out.push_str(&word.to_ascii_lowercase());
        } else {
            out.push_str(&title_word(word));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::manifest::{ManifestColumn, ManifestForeignKey, ManifestTable};

    fn manifest_column(field_name: &str, column_name: &str) -> ManifestColumn {
        ManifestColumn {
            field_name: field_name.to_string(),
            column_name: column_name.to_string(),
            ..ManifestColumn::default()
        }
    }

    // End-to-end V19-1/V19-3/V19-4 shapes through the REAL `--project-proto`
    // path: file-level same-package enum resolves; cross-package enum does not;
    // broker-injected audit columns are marked undeclared; repeated stays
    // `is_array`. Uses a scratch proto tree on disk because this is exactly how
    // consumers invoke it.
    #[test]
    fn project_proto_descriptors_resolve_enums_and_mark_injected_columns() {
        let root = std::env::temp_dir().join(format!(
            "udb-sdkm-projproto-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("create scratch proto dir");
        std::fs::write(
            root.join("common.proto"),
            r#"syntax = "proto3";
package acme.order.v1;
enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_ACTIVE = 1;
  ORDER_STATUS_CLOSED = 2;
}
"#,
        )
        .expect("write common.proto");
        std::fs::write(
            root.join("region.proto"),
            r#"syntax = "proto3";
package acme.geo.v1;
enum Region {
  REGION_UNSPECIFIED = 0;
  REGION_EAST = 1;
}
"#,
        )
        .expect("write region.proto");
        std::fs::write(
            root.join("order.proto"),
            r#"syntax = "proto3";
package acme.order.v1;
message Order {
  option (table) = { table_name: "orders" schema_name: "acme" migration_order: 1 audit_fields: true };
  string id = 1;
  OrderStatus status = 2;
  acme.geo.v1.Region region = 3;
  repeated string tags = 4;
}
"#,
        )
        .expect("write order.proto");

        let entities = entity_manifest_from_proto_dir(&root).expect("build descriptors");
        std::fs::remove_dir_all(&root).ok();
        let order = entities
            .iter()
            .find(|e| e.short_name == "Order")
            .expect("Order entity");
        let col = |name: &str| {
            order
                .columns
                .iter()
                .find(|c| c.field_name == name)
                .unwrap_or_else(|| panic!("column {name} missing"))
        };

        // V19-1: same-package file-level enum resolves without an annotation.
        assert_eq!(
            col("status").enum_values,
            [
                "ORDER_STATUS_UNSPECIFIED",
                "ORDER_STATUS_ACTIVE",
                "ORDER_STATUS_CLOSED"
            ],
            "same-package file-level enum must auto-resolve"
        );
        // Cross-package enum stays unresolved → symmetric TODO downstream.
        assert!(
            col("region").enum_values.is_empty(),
            "cross-package enum must NOT resolve (wrong import alias otherwise)"
        );
        // V19-4 input: repeated survives as is_array for the emitter's skip.
        assert!(col("tags").is_array, "repeated field must carry is_array");
        // V19-3: proto-declared vs broker-injected audit columns.
        assert!(col("id").declared_in_proto);
        assert!(col("status").declared_in_proto);
        for injected in ["created_at", "updated_at", "created_by"] {
            assert!(
                !col(injected).declared_in_proto,
                "injected {injected} must be marked undeclared"
            );
        }
    }

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
        assert_eq!(select.method_alias_snake, "select");
        assert_eq!(select.method_alias_camel, "select");
        assert_eq!(select.method_alias_pascal, "Select");
        assert_eq!(select.rest_operation_id, "select");
        assert_eq!(select.input_short, "SelectRequest");
        assert_eq!(select.kind(), "unary");
        assert_eq!(select.grpc_path(), "/udb.services.v1.DataBroker/Select");
        assert_eq!(select.operation_kind, "read_only");
        assert!(select.read_only);

        let upsert = manifest
            .iter()
            .find(|r| r.service_name == "DataBroker" && r.method == "Upsert")
            .expect("DataBroker/Upsert must be in the manifest");
        assert_eq!(upsert.operation_kind, "mutation");
        assert!(!upsert.read_only);
    }

    #[test]
    fn sdk_alias_identity_is_acronym_safe() {
        let manifest = rpc_manifest();
        let jwks = manifest
            .iter()
            .find(|r| r.service_name == "AuthnService" && r.method == "GetJwks")
            .expect("AuthnService/GetJwks must be in the manifest");
        assert_eq!(jwks.method_alias, "get_jwks");
        assert_eq!(jwks.method_alias_snake, "get_jwks");
        assert_eq!(jwks.method_alias_camel, "getJwks");
        assert_eq!(jwks.method_alias_pascal, "GetJwks");
        assert_eq!(jwks.rest_operation_id, "getJwks");

        assert_eq!(alias_snake_case("CreateAPIKey"), "create_api_key");
        assert_eq!(alias_camel_case("CreateAPIKey"), "createApiKey");
        assert_eq!(alias_pascal_case("CreateAPIKey"), "CreateApiKey");
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
        let descriptor = descriptor_contract_manifest_static();
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
                .any(|entity| entity.message_type == "udb.core.tenant.entity.v1.Tenant"),
            "expected native Tenant entity descriptor in manifest"
        );
    }

    #[test]
    fn entity_relations_are_derived_from_foreign_keys() {
        let child = ManifestTable {
            message_name: "Invoice".to_string(),
            proto_package: "billing.v1".to_string(),
            schema: "billing".to_string(),
            table: "invoices".to_string(),
            columns: vec![
                manifest_column("invoice_id", "invoice_id"),
                manifest_column("customer_id", "customer_id"),
            ],
            foreign_keys: vec![ManifestForeignKey {
                columns: vec!["customer_id".to_string()],
                ref_schema: "crm".to_string(),
                ref_table: "customers".to_string(),
                ref_columns: vec!["customer_id".to_string()],
                on_delete: "cascade".to_string(),
                ..ManifestForeignKey::default()
            }],
            ..ManifestTable::default()
        };
        let target = ManifestTable {
            message_name: "Customer".to_string(),
            proto_package: "crm.v1".to_string(),
            schema: "crm".to_string(),
            table: "customers".to_string(),
            columns: vec![manifest_column("customer_id", "customer_id")],
            primary_key: vec!["customer_id".to_string()],
            ..ManifestTable::default()
        };

        let relations = entity_relations(&child, &[child.clone(), target.clone()]);

        assert_eq!(relations.len(), 1);
        let relation = &relations[0];
        assert_eq!(relation.name, "customer");
        assert_eq!(relation.kind, "belongs_to");
        assert_eq!(relation.local_fields, ["customer_id"]);
        assert_eq!(relation.target_message_type, "crm.v1.Customer");
        assert_eq!(relation.target_table, "crm.customers");
        assert_eq!(relation.target_fields, ["customer_id"]);
        assert_eq!(relation.on_delete, "cascade");

        let inverse = entity_relations(&target, &[child, target.clone()]);
        assert_eq!(inverse.len(), 1);
        let relation = &inverse[0];
        assert_eq!(relation.name, "invoices");
        assert_eq!(relation.kind, "has_many");
        assert_eq!(relation.local_fields, ["customer_id"]);
        assert_eq!(relation.target_message_type, "billing.v1.Invoice");
        assert_eq!(relation.target_table, "billing.invoices");
        assert_eq!(relation.target_fields, ["customer_id"]);
        assert_eq!(relation.on_delete, "cascade");
    }

    #[test]
    fn entity_version_field_is_conservative() {
        let table = ManifestTable {
            columns: vec![
                manifest_column("updated_at", "updated_at"),
                manifest_column("lock_version", "lock_version"),
            ],
            ..ManifestTable::default()
        };
        assert_eq!(
            entity_version_field(&table).as_deref(),
            Some("lock_version")
        );

        let timestamp_only = ManifestTable {
            columns: vec![manifest_column("updated_at", "updated_at")],
            ..ManifestTable::default()
        };
        assert_eq!(entity_version_field(&timestamp_only), None);
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
