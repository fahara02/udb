//! Shared descriptor-derived contract manifest.
//!
//! The embedded `FileDescriptorSet` keeps UDB proto as the source of truth for
//! runtime enforcement, SDK generation, CLI scaffolds, and contract tests. This
//! module decodes the custom option extension fields directly from descriptor
//! bytes; `prost_types::*Options` drops unknown extensions, so consumers must not
//! decode those options independently.

use prost::Message;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescriptorContractManifest {
    pub services: Vec<ServiceContract>,
    pub messages: Vec<MessageContract>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceContract {
    pub file_path: String,
    pub package: String,
    pub name: String,
    pub native_service: Option<NativeServiceContract>,
    pub sdk_surface: Option<SdkSurfaceContract>,
    pub cli_scaffold: Option<CliScaffoldContract>,
    pub dependency_contract: Option<DependencyContract>,
    pub methods: Vec<RpcContract>,
}

impl ServiceContract {
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.package, self.name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcContract {
    pub service_name: String,
    pub service_pkg: String,
    pub method: String,
    pub method_snake: String,
    pub input_type: String,
    pub input_pkg: String,
    pub input_short: String,
    pub output_type: String,
    pub output_pkg: String,
    pub output_short: String,
    pub client_streaming: bool,
    pub server_streaming: bool,
    pub endpoint_security: Option<EndpointSecurityContract>,
    pub rest_contract: Option<RestContractContract>,
    pub http_rule: Option<HttpRuleContract>,
    pub sdk_surface: Option<SdkSurfaceContract>,
    pub cli_scaffold: Option<CliScaffoldContract>,
    pub event_contract: Option<EventContract>,
    /// Canonical per-event contract: the versioned domain events this RPC may
    /// emit (0..N). Decoded from the `emits` (field 7) of `method_event_contract`.
    /// The scalar `event_contract` above is the legacy coarse per-RPC contract,
    /// kept for back-compat. See docs/event-contract-model.md.
    pub emits: Vec<EmittedEvent>,
    pub dependency_contract: Option<DependencyContract>,
    pub precondition_contract: Option<MethodPreconditionContract>,
    pub readback_contract: Option<ReadAfterWriteContract>,
    pub lifecycle_contract: Option<LifecycleContract>,
    pub idempotency_contract: Option<IdempotencyContract>,
    pub error_contract: Option<ErrorContract>,
    /// `operation_kind` method option (proto OperationKind enum): 0=unspecified,
    /// 1=read_only, 2=mutation, 3=destructive. Authoritative state-change class —
    /// drives client retry safety + SDK probe classification (never the name).
    pub operation_kind: i32,
}

impl RpcContract {
    pub fn service_full(&self) -> String {
        format!("{}.{}", self.service_pkg, self.service_name)
    }

    pub fn grpc_path(&self) -> String {
        format!("/{}/{}", self.service_full(), self.method)
    }

    pub fn kind(&self) -> &'static str {
        match (self.client_streaming, self.server_streaming) {
            (false, false) => "unary",
            (false, true) => "server_streaming",
            (true, false) => "client_streaming",
            (true, true) => "bidi",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageContract {
    pub file_path: String,
    pub package: String,
    pub name: String,
    pub full_name: String,
    pub db_table_security: Option<DbTableSecurityContract>,
    pub sdk_surface: Option<SdkSurfaceContract>,
    pub event_contract: Option<EventContract>,
    pub dependency_contract: Option<DependencyContract>,
    pub fields: Vec<FieldContract>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldContract {
    pub name: String,
    pub number: i32,
    pub type_name: String,
    pub db_column_security: Option<DbColumnSecurityContract>,
    pub scalar_security: ScalarFieldSecurity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointSecurityContract {
    pub mode: i32,
    pub roles: Vec<String>,
    pub scopes: Vec<String>,
    pub tenant_required: bool,
    pub csrf_required: bool,
    pub policy_ref: String,
    pub internal_grpc_only: bool,
    pub required_assurance_level: i32,
    pub allowed_credential_types: Vec<i32>,
    pub rate_limit_policy_ref: String,
    pub abuse_policy_ref: String,
    pub audit_event_type: String,
    pub decision_resource: String,
    pub owner_field: String,
    pub tenant_field: String,
    pub project_field: String,
    pub idempotency_required: bool,
    pub request_context_required: bool,
}

impl EndpointSecurityContract {
    pub fn auth_mode_name(&self) -> &'static str {
        match self.mode {
            1 => "public",
            2 => "bearer",
            3 => "api_key",
            4 => "service_account",
            _ => "unspecified",
        }
    }
}

/// Canonical `operation_kind` token (proto OperationKind enum) for the
/// generator/lint. The SINGLE source of truth for client retry safety and SDK
/// probe classification — never derived from the method name.
pub fn operation_kind_name(value: i32) -> &'static str {
    match value {
        1 => "read_only",
        2 => "mutation",
        3 => "destructive",
        _ => "unspecified",
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeServiceContract {
    pub service_id: String,
    pub logical_service_id: String,
    pub proto_service_id: String,
    pub display_name: String,
    pub category: String,
    pub default_enabled: bool,
    pub requires_postgres: bool,
    pub requires_redis: bool,
    pub requires_object_store: bool,
    pub requires_kafka: bool,
    pub requires_feature: String,
    pub public_listener_allowed: bool,
    pub control_plane_listener_allowed: bool,
    pub peer_listener_allowed: bool,
    pub sdk_facade_name: String,
    pub cli_scaffold_group: String,
    pub health_check_ref: String,
    pub capability_ref: String,
    /// Whether this service owns background workers (storage orphan-reaper,
    /// webrtc TURN/SFU loops) that run beyond request handling. Explicit in the
    /// contract so worker ownership is not inferred from runtime behavior.
    pub owns_background_workers: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SdkSurfaceContract {
    pub include_in_facade: bool,
    pub method_alias: String,
    pub rest_operation_id: String,
    pub required_credential_provider: String,
    pub streaming_helper_type: String,
    pub default_deadline_ms: i32,
    pub default_max_attempts: i32,
    pub browser_safe: bool,
    pub server_only: bool,
    pub boilerplate_recipe_tags: Vec<String>,
    pub generate_minimal_example: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CliScaffoldContract {
    pub scaffold_package: String,
    pub import_path: String,
    pub required_env: Vec<String>,
    pub generated_files: Vec<String>,
    pub route_name: String,
    pub middleware_name: String,
    pub required_native_services: Vec<String>,
    pub optional_native_services: Vec<String>,
    pub secret_placeholders: Vec<String>,
    pub post_generation_commands: Vec<String>,
    pub smoke_test_command: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestContractContract {
    pub response_envelope: bool,
    pub api_error: bool,
    pub pagination_meta: bool,
    pub explicit_nulls: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpRuleContract {
    pub verb: String,
    pub path: String,
    pub body: String,
    pub response_body: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventContract {
    pub event_type: String,
    pub outbox_topic: String,
    pub partition_key_field: String,
    pub payload_redaction_profile: String,
    pub delivery_guarantee: String,
    pub replay_compatibility: String,
}

/// One versioned domain event an RPC may publish (mirrors
/// `EventContractOptions.EmittedEvent` in security.proto). `topic` is the
/// canonical versioned Kafka topic, e.g. `udb.webrtc.peer.left.v1`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmittedEvent {
    pub topic: String,
    pub partition_key_field: String,
    pub delivery_guarantee: String,
    pub payload_redaction_profile: String,
    pub conditional: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DependencyContract {
    pub required_native_services: Vec<String>,
    pub optional_native_services: Vec<String>,
    pub required_backends: Vec<String>,
    pub optional_backends: Vec<String>,
    pub required_features: Vec<String>,
    pub required_env: Vec<String>,
    pub degraded_when_missing: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MethodPreconditionContract {
    pub required_resources: Vec<String>,
    pub required_prior_result_fields: Vec<String>,
    pub required_source_states: Vec<String>,
    pub missing_code: String,
    pub wrong_state_code: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadAfterWriteContract {
    pub returned_id_field: String,
    pub readback_rpc: String,
    pub readback_request_field: String,
    pub requires_read_fence: bool,
    pub requires_primary_read: bool,
    pub no_readback_reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleContract {
    pub entity: String,
    pub legal_source_states: Vec<String>,
    pub target_state: String,
    pub terminal_states: Vec<String>,
    pub input_consumed: bool,
    pub destructive: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdempotencyContract {
    pub request_key_field: String,
    pub server_generated_key: bool,
    pub duplicate_response_field: String,
    pub replay_safe: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorContract {
    pub cases: Vec<ErrorCase>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorCase {
    pub canonical_code: String,
    pub grpc_status: String,
    pub retryable: bool,
    pub details_type: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbTableSecurityContract {
    pub tenant_isolation_mode: String,
    pub project_isolation_mode: String,
    pub tenant_column: String,
    pub project_column: String,
    pub rls_policy_template: String,
    pub soft_delete_mode: String,
    pub retention_class: String,
    pub retention_days: i32,
    pub audit_mode: i32,
    pub encryption_profile: String,
    pub pii_profile: String,
    pub break_glass_visible: bool,
    pub export_eligible: bool,
    pub data_residency_policy_ref: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbColumnSecurityContract {
    pub secret_classification: i32,
    pub output_view: i32,
    pub redaction_strategy: i32,
    pub tokenization_strategy: String,
    pub hashing_strategy: String,
    pub hashing_algorithm: String,
    pub encryption_key_class: String,
    pub searchable_encrypted: bool,
    pub uniqueness_scope: String,
    pub owner_field: bool,
    pub tenant_field: bool,
    pub project_field: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScalarFieldSecurity {
    pub pii: bool,
    pub encrypted_security: bool,
    pub log_masked: bool,
    pub log_redacted: bool,
    pub sensitive: bool,
    pub requires_consent: bool,
    pub data_purpose: String,
    pub retention_days: i32,
    pub tokenized: bool,
    pub security_classification: i32,
    pub data_category: i32,
}

/// Decode the embedded descriptor contract manifest, **failing closed**.
///
/// The `FileDescriptorSet` is a compile-time artifact embedded in the binary. It
/// is the single source of truth for per-RPC method security
/// ([`crate::runtime::service::method_security`]), native-service routing
/// ([`crate::runtime::service::native_registry`]), and DB table/column security.
/// A decode failure — or a manifest that decodes to zero `udb` services — means
/// the process has *no trustworthy security contract*; silently returning an
/// empty manifest would disable per-RPC auth enforcement and native routing
/// without any signal. We therefore abort rather than degrade into an unsecured
/// state. In a correctly built binary this branch is unreachable; if it is ever
/// hit, the binary is corrupt and must not serve traffic.
pub fn descriptor_contract_manifest() -> DescriptorContractManifest {
    descriptor_contract_manifest_static().clone()
}

/// Process-wide cached decode of the embedded descriptor (mirrors
/// `method_security_registry`): the `FileDescriptorSet` is a compile-time
/// constant, so `FdSet::decode` runs at most once per process instead of on
/// every registry/security lookup (which sat on the per-RPC path). Same
/// fail-closed panic semantics as before — a corrupt descriptor aborts on first
/// use and the `OnceLock` stays uninitialized.
pub fn descriptor_contract_manifest_static() -> &'static DescriptorContractManifest {
    static MANIFEST: std::sync::OnceLock<DescriptorContractManifest> = std::sync::OnceLock::new();
    MANIFEST.get_or_init(|| {
        try_descriptor_contract_manifest().unwrap_or_else(|err| {
            panic!("{err}");
        })
    })
}

pub fn try_descriptor_contract_manifest() -> Result<DescriptorContractManifest, String> {
    let bytes = crate::runtime::native_catalog::embedded_file_descriptor_set();
    let manifest = descriptor_contract_manifest_from_bytes(bytes).map_err(|err| {
        format!(
            "fatal: embedded descriptor contract manifest failed to decode ({err}); \
             refusing to start with no enforceable security contract"
        )
    })?;
    if manifest.services.is_empty() {
        return Err(
            "fatal: embedded descriptor contract manifest decoded to zero udb services; \
             refusing to start with an empty security/native-routing contract"
                .to_string(),
        );
    }
    Ok(manifest)
}

pub fn descriptor_contract_manifest_from_bytes(
    bytes: &[u8],
) -> Result<DescriptorContractManifest, prost::DecodeError> {
    let set = FdSet::decode(bytes)?;
    Ok(build_contract_manifest(&set))
}

fn build_contract_manifest(set: &FdSet) -> DescriptorContractManifest {
    let mut services = Vec::new();
    let mut messages = Vec::new();
    for file in &set.file {
        let file_path = file.name.clone().unwrap_or_default();
        let package = file.package.clone().unwrap_or_default();
        if !package.starts_with("udb") {
            continue;
        }
        for message in &file.message_type {
            collect_message_contracts(&mut messages, &file_path, &package, "", message);
        }
        for service in &file.service {
            let service_name = service.name.clone().unwrap_or_default();
            let native_service = service
                .options
                .as_ref()
                .and_then(|options| options.native_service.as_ref())
                .map(NativeServiceContract::from);
            let sdk_surface = service
                .options
                .as_ref()
                .and_then(|options| options.sdk_surface.as_ref())
                .map(SdkSurfaceContract::from);
            let cli_scaffold = service
                .options
                .as_ref()
                .and_then(|options| options.cli_scaffold.as_ref())
                .map(CliScaffoldContract::from);
            let dependency_contract = service
                .options
                .as_ref()
                .and_then(|options| options.dependency_contract.as_ref())
                .map(DependencyContract::from);
            let mut methods = Vec::new();
            for method in &service.method {
                let (input_pkg, input_short) =
                    split_type(method.input_type.as_deref().unwrap_or_default());
                let (output_pkg, output_short) =
                    split_type(method.output_type.as_deref().unwrap_or_default());
                methods.push(RpcContract {
                    service_name: service_name.clone(),
                    service_pkg: package.clone(),
                    method: method.name.clone().unwrap_or_default(),
                    method_snake: to_snake(method.name.as_deref().unwrap_or_default()),
                    input_type: method.input_type.clone().unwrap_or_default(),
                    input_pkg,
                    input_short,
                    output_type: method.output_type.clone().unwrap_or_default(),
                    output_pkg,
                    output_short,
                    client_streaming: method.client_streaming.unwrap_or(false),
                    server_streaming: method.server_streaming.unwrap_or(false),
                    endpoint_security: method
                        .options
                        .as_ref()
                        .and_then(|options| options.endpoint_security.as_ref())
                        .map(EndpointSecurityContract::from),
                    rest_contract: method
                        .options
                        .as_ref()
                        .and_then(|options| options.rest_contract.as_ref())
                        .map(RestContractContract::from),
                    http_rule: method
                        .options
                        .as_ref()
                        .and_then(|options| options.http_rule.as_ref())
                        .and_then(HttpRuleContract::from_proto),
                    sdk_surface: method
                        .options
                        .as_ref()
                        .and_then(|options| options.sdk_surface.as_ref())
                        .map(SdkSurfaceContract::from),
                    cli_scaffold: method
                        .options
                        .as_ref()
                        .and_then(|options| options.cli_scaffold.as_ref())
                        .map(CliScaffoldContract::from),
                    event_contract: method
                        .options
                        .as_ref()
                        .and_then(|options| options.event_contract.as_ref())
                        .map(EventContract::from),
                    emits: method
                        .options
                        .as_ref()
                        .and_then(|options| options.event_contract.as_ref())
                        .map(|ev| ev.emits.iter().map(EmittedEvent::from).collect())
                        .unwrap_or_default(),
                    dependency_contract: method
                        .options
                        .as_ref()
                        .and_then(|options| options.dependency_contract.as_ref())
                        .map(DependencyContract::from),
                    precondition_contract: method
                        .options
                        .as_ref()
                        .and_then(|options| options.precondition_contract.as_ref())
                        .map(MethodPreconditionContract::from),
                    readback_contract: method
                        .options
                        .as_ref()
                        .and_then(|options| options.readback_contract.as_ref())
                        .map(ReadAfterWriteContract::from),
                    lifecycle_contract: method
                        .options
                        .as_ref()
                        .and_then(|options| options.lifecycle_contract.as_ref())
                        .map(LifecycleContract::from),
                    idempotency_contract: method
                        .options
                        .as_ref()
                        .and_then(|options| options.idempotency_contract.as_ref())
                        .map(IdempotencyContract::from),
                    error_contract: method
                        .options
                        .as_ref()
                        .and_then(|options| options.error_contract.as_ref())
                        .map(ErrorContract::from),
                    operation_kind: method
                        .options
                        .as_ref()
                        .map(|options| options.operation_kind)
                        .unwrap_or(0),
                });
            }
            methods.sort_by(|a, b| a.method.cmp(&b.method));
            services.push(ServiceContract {
                file_path: file_path.clone(),
                package: package.clone(),
                name: service_name,
                native_service,
                sdk_surface,
                cli_scaffold,
                dependency_contract,
                methods,
            });
        }
    }
    services.sort_by(|a, b| a.full_name().cmp(&b.full_name()));
    messages.sort_by(|a, b| a.full_name.cmp(&b.full_name));
    DescriptorContractManifest { services, messages }
}

fn collect_message_contracts(
    out: &mut Vec<MessageContract>,
    file_path: &str,
    package: &str,
    parent: &str,
    message: &DescriptorProto,
) {
    let name = message.name.clone().unwrap_or_default();
    let short_name = if parent.is_empty() {
        name.clone()
    } else {
        format!("{parent}.{name}")
    };
    let full_name = if package.is_empty() {
        short_name.clone()
    } else {
        format!("{package}.{short_name}")
    };
    let fields = message
        .field
        .iter()
        .map(|field| FieldContract {
            name: field.name.clone().unwrap_or_default(),
            number: field.number.unwrap_or_default(),
            type_name: field.type_name.clone().unwrap_or_else(|| {
                field
                    .r#type
                    .map(|value| value.to_string())
                    .unwrap_or_default()
            }),
            db_column_security: field
                .options
                .as_ref()
                .and_then(|options| options.db_column_security.as_ref())
                .map(DbColumnSecurityContract::from),
            scalar_security: field
                .options
                .as_ref()
                .map(ScalarFieldSecurity::from)
                .unwrap_or_default(),
        })
        .collect();
    out.push(MessageContract {
        file_path: file_path.to_string(),
        package: package.to_string(),
        name,
        full_name,
        db_table_security: message
            .options
            .as_ref()
            .and_then(|options| options.db_table_security.as_ref())
            .map(DbTableSecurityContract::from),
        sdk_surface: message
            .options
            .as_ref()
            .and_then(|options| options.sdk_surface.as_ref())
            .map(SdkSurfaceContract::from),
        event_contract: message
            .options
            .as_ref()
            .and_then(|options| options.event_contract.as_ref())
            .map(EventContract::from),
        dependency_contract: message
            .options
            .as_ref()
            .and_then(|options| options.dependency_contract.as_ref())
            .map(DependencyContract::from),
        fields,
    });
    for nested in &message.nested_type {
        collect_message_contracts(out, file_path, package, &short_name, nested);
    }
}

fn split_type(fq: &str) -> (String, String) {
    let trimmed = fq.strip_prefix('.').unwrap_or(fq);
    match trimmed.rsplit_once('.') {
        Some((pkg, short)) => (pkg.to_string(), short.to_string()),
        None => (String::new(), trimmed.to_string()),
    }
}

fn to_snake(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (i, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

impl From<&EndpointSec> for EndpointSecurityContract {
    fn from(value: &EndpointSec) -> Self {
        Self {
            mode: value.mode,
            roles: value.roles.clone(),
            scopes: value.scopes.clone(),
            tenant_required: value.tenant_required,
            csrf_required: value.csrf_required,
            policy_ref: value.policy_ref.clone(),
            internal_grpc_only: value.internal_grpc_only,
            required_assurance_level: value.required_assurance_level,
            allowed_credential_types: value.allowed_credential_types.clone(),
            rate_limit_policy_ref: value.rate_limit_policy_ref.clone(),
            abuse_policy_ref: value.abuse_policy_ref.clone(),
            audit_event_type: value.audit_event_type.clone(),
            decision_resource: value.decision_resource.clone(),
            owner_field: value.owner_field.clone(),
            tenant_field: value.tenant_field.clone(),
            project_field: value.project_field.clone(),
            idempotency_required: value.idempotency_required,
            request_context_required: value.request_context_required,
        }
    }
}

impl From<&NativeSvcOpts> for NativeServiceContract {
    fn from(value: &NativeSvcOpts) -> Self {
        Self {
            service_id: value.service_id.clone(),
            logical_service_id: value.logical_service_id.clone(),
            proto_service_id: value.proto_service_id.clone(),
            display_name: value.display_name.clone(),
            category: value.category.clone(),
            default_enabled: value.default_enabled,
            requires_postgres: value.requires_postgres,
            requires_redis: value.requires_redis,
            requires_object_store: value.requires_object_store,
            requires_kafka: value.requires_kafka,
            requires_feature: value.requires_feature.clone(),
            public_listener_allowed: value.public_listener_allowed,
            control_plane_listener_allowed: value.control_plane_listener_allowed,
            peer_listener_allowed: value.peer_listener_allowed,
            sdk_facade_name: value.sdk_facade_name.clone(),
            cli_scaffold_group: value.cli_scaffold_group.clone(),
            health_check_ref: value.health_check_ref.clone(),
            capability_ref: value.capability_ref.clone(),
            owns_background_workers: value.owns_background_workers,
        }
    }
}

impl From<&SdkSurfaceOpts> for SdkSurfaceContract {
    fn from(value: &SdkSurfaceOpts) -> Self {
        Self {
            include_in_facade: value.include_in_facade,
            method_alias: value.method_alias.clone(),
            rest_operation_id: value.rest_operation_id.clone(),
            required_credential_provider: value.required_credential_provider.clone(),
            streaming_helper_type: value.streaming_helper_type.clone(),
            default_deadline_ms: value.default_deadline_ms,
            default_max_attempts: value.default_max_attempts,
            browser_safe: value.browser_safe,
            server_only: value.server_only,
            boilerplate_recipe_tags: value.boilerplate_recipe_tags.clone(),
            generate_minimal_example: value.generate_minimal_example,
        }
    }
}

impl From<&CliScaffoldOpts> for CliScaffoldContract {
    fn from(value: &CliScaffoldOpts) -> Self {
        Self {
            scaffold_package: value.scaffold_package.clone(),
            import_path: value.import_path.clone(),
            required_env: value.required_env.clone(),
            generated_files: value.generated_files.clone(),
            route_name: value.route_name.clone(),
            middleware_name: value.middleware_name.clone(),
            required_native_services: value.required_native_services.clone(),
            optional_native_services: value.optional_native_services.clone(),
            secret_placeholders: value.secret_placeholders.clone(),
            post_generation_commands: value.post_generation_commands.clone(),
            smoke_test_command: value.smoke_test_command.clone(),
        }
    }
}

impl From<&RestContractOpts> for RestContractContract {
    fn from(value: &RestContractOpts) -> Self {
        Self {
            response_envelope: value.response_envelope,
            api_error: value.api_error,
            pagination_meta: value.pagination_meta,
            explicit_nulls: value.explicit_nulls,
        }
    }
}

impl HttpRuleContract {
    fn from_proto(value: &HttpRuleOpts) -> Option<Self> {
        let (verb, path) = [
            ("get", &value.get),
            ("put", &value.put),
            ("post", &value.post),
            ("delete", &value.delete),
            ("patch", &value.patch),
        ]
        .into_iter()
        .find(|(_, path)| !path.trim().is_empty())?;
        Some(Self {
            verb: verb.to_string(),
            path: path.clone(),
            body: value.body.clone(),
            response_body: value.response_body.clone(),
        })
    }
}

impl From<&EventContractOpts> for EventContract {
    fn from(value: &EventContractOpts) -> Self {
        Self {
            event_type: value.event_type.clone(),
            outbox_topic: value.outbox_topic.clone(),
            partition_key_field: value.partition_key_field.clone(),
            payload_redaction_profile: value.payload_redaction_profile.clone(),
            delivery_guarantee: value.delivery_guarantee.clone(),
            replay_compatibility: value.replay_compatibility.clone(),
        }
    }
}

impl From<&EmittedEventOpts> for EmittedEvent {
    fn from(value: &EmittedEventOpts) -> Self {
        Self {
            topic: value.topic.clone(),
            partition_key_field: value.partition_key_field.clone(),
            delivery_guarantee: value.delivery_guarantee.clone(),
            payload_redaction_profile: value.payload_redaction_profile.clone(),
            conditional: value.conditional,
        }
    }
}

impl From<&DependencyContractOpts> for DependencyContract {
    fn from(value: &DependencyContractOpts) -> Self {
        Self {
            required_native_services: value.required_native_services.clone(),
            optional_native_services: value.optional_native_services.clone(),
            required_backends: value.required_backends.clone(),
            optional_backends: value.optional_backends.clone(),
            required_features: value.required_features.clone(),
            required_env: value.required_env.clone(),
            degraded_when_missing: value.degraded_when_missing.clone(),
        }
    }
}

impl From<&PreconditionContractOpts> for MethodPreconditionContract {
    fn from(value: &PreconditionContractOpts) -> Self {
        Self {
            required_resources: value.required_resources.clone(),
            required_prior_result_fields: value.required_prior_result_fields.clone(),
            required_source_states: value.required_source_states.clone(),
            missing_code: value.missing_code.clone(),
            wrong_state_code: value.wrong_state_code.clone(),
        }
    }
}

impl From<&ReadAfterWriteContractOpts> for ReadAfterWriteContract {
    fn from(value: &ReadAfterWriteContractOpts) -> Self {
        Self {
            returned_id_field: value.returned_id_field.clone(),
            readback_rpc: value.readback_rpc.clone(),
            readback_request_field: value.readback_request_field.clone(),
            requires_read_fence: value.requires_read_fence,
            requires_primary_read: value.requires_primary_read,
            no_readback_reason: value.no_readback_reason.clone(),
        }
    }
}

impl From<&LifecycleContractOpts> for LifecycleContract {
    fn from(value: &LifecycleContractOpts) -> Self {
        Self {
            entity: value.entity.clone(),
            legal_source_states: value.legal_source_states.clone(),
            target_state: value.target_state.clone(),
            terminal_states: value.terminal_states.clone(),
            input_consumed: value.input_consumed,
            destructive: value.destructive,
        }
    }
}

impl From<&IdempotencyContractOpts> for IdempotencyContract {
    fn from(value: &IdempotencyContractOpts) -> Self {
        Self {
            request_key_field: value.request_key_field.clone(),
            server_generated_key: value.server_generated_key,
            duplicate_response_field: value.duplicate_response_field.clone(),
            replay_safe: value.replay_safe,
        }
    }
}

impl From<&ErrorContractOpts> for ErrorContract {
    fn from(value: &ErrorContractOpts) -> Self {
        Self {
            cases: value.cases.iter().map(ErrorCase::from).collect(),
        }
    }
}

impl From<&ErrorCaseOpts> for ErrorCase {
    fn from(value: &ErrorCaseOpts) -> Self {
        Self {
            canonical_code: value.canonical_code.clone(),
            grpc_status: value.grpc_status.clone(),
            retryable: value.retryable,
            details_type: value.details_type.clone(),
        }
    }
}

impl From<&DbTableSecurityOpts> for DbTableSecurityContract {
    fn from(value: &DbTableSecurityOpts) -> Self {
        Self {
            tenant_isolation_mode: value.tenant_isolation_mode.clone(),
            project_isolation_mode: value.project_isolation_mode.clone(),
            tenant_column: value.tenant_column.clone(),
            project_column: value.project_column.clone(),
            rls_policy_template: value.rls_policy_template.clone(),
            soft_delete_mode: value.soft_delete_mode.clone(),
            retention_class: value.retention_class.clone(),
            retention_days: value.retention_days,
            audit_mode: value.audit_mode,
            encryption_profile: value.encryption_profile.clone(),
            pii_profile: value.pii_profile.clone(),
            break_glass_visible: value.break_glass_visible,
            export_eligible: value.export_eligible,
            data_residency_policy_ref: value.data_residency_policy_ref.clone(),
        }
    }
}

impl From<&DbColumnSecurityOpts> for DbColumnSecurityContract {
    fn from(value: &DbColumnSecurityOpts) -> Self {
        Self {
            secret_classification: value.secret_classification,
            output_view: value.output_view,
            redaction_strategy: value.redaction_strategy,
            tokenization_strategy: value.tokenization_strategy.clone(),
            hashing_strategy: value.hashing_strategy.clone(),
            hashing_algorithm: value.hashing_algorithm.clone(),
            encryption_key_class: value.encryption_key_class.clone(),
            searchable_encrypted: value.searchable_encrypted,
            uniqueness_scope: value.uniqueness_scope.clone(),
            owner_field: value.owner_field,
            tenant_field: value.tenant_field,
            project_field: value.project_field,
        }
    }
}

impl From<&FieldOpts> for ScalarFieldSecurity {
    fn from(value: &FieldOpts) -> Self {
        Self {
            pii: value.pii,
            encrypted_security: value.encrypted_security,
            log_masked: value.log_masked,
            log_redacted: value.log_redacted,
            sensitive: value.sensitive,
            requires_consent: value.requires_consent,
            data_purpose: value.data_purpose.clone(),
            retention_days: value.retention_days,
            tokenized: value.tokenized,
            security_classification: value.security_classification,
            data_category: value.data_category,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct FdSet {
    #[prost(message, repeated, tag = "1")]
    file: Vec<FdProto>,
}

#[derive(Clone, PartialEq, Message)]
struct FdProto {
    #[prost(string, optional, tag = "1")]
    name: Option<String>,
    #[prost(string, optional, tag = "2")]
    package: Option<String>,
    #[prost(message, repeated, tag = "4")]
    message_type: Vec<DescriptorProto>,
    #[prost(message, repeated, tag = "6")]
    service: Vec<SvcProto>,
}

#[derive(Clone, PartialEq, Message)]
struct DescriptorProto {
    #[prost(string, optional, tag = "1")]
    name: Option<String>,
    #[prost(message, repeated, tag = "2")]
    field: Vec<FieldProto>,
    #[prost(message, repeated, tag = "3")]
    nested_type: Vec<DescriptorProto>,
    #[prost(message, optional, tag = "7")]
    options: Option<MessageOpts>,
}

#[derive(Clone, PartialEq, Message)]
struct FieldProto {
    #[prost(string, optional, tag = "1")]
    name: Option<String>,
    #[prost(int32, optional, tag = "3")]
    number: Option<i32>,
    #[prost(int32, optional, tag = "5")]
    r#type: Option<i32>,
    #[prost(string, optional, tag = "6")]
    type_name: Option<String>,
    #[prost(message, optional, tag = "8")]
    options: Option<FieldOpts>,
}

#[derive(Clone, PartialEq, Message)]
struct SvcProto {
    #[prost(string, optional, tag = "1")]
    name: Option<String>,
    #[prost(message, repeated, tag = "2")]
    method: Vec<MethodProto>,
    #[prost(message, optional, tag = "3")]
    options: Option<ServiceOpts>,
}

#[derive(Clone, PartialEq, Message)]
struct MethodProto {
    #[prost(string, optional, tag = "1")]
    name: Option<String>,
    #[prost(string, optional, tag = "2")]
    input_type: Option<String>,
    #[prost(string, optional, tag = "3")]
    output_type: Option<String>,
    #[prost(message, optional, tag = "4")]
    options: Option<MethodOpts>,
    #[prost(bool, optional, tag = "5")]
    client_streaming: Option<bool>,
    #[prost(bool, optional, tag = "6")]
    server_streaming: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
struct FieldOpts {
    #[prost(bool, tag = "50010")]
    pii: bool,
    #[prost(bool, tag = "50011")]
    encrypted_security: bool,
    #[prost(bool, tag = "50012")]
    log_masked: bool,
    #[prost(bool, tag = "50013")]
    log_redacted: bool,
    #[prost(bool, tag = "50014")]
    sensitive: bool,
    #[prost(bool, tag = "50015")]
    requires_consent: bool,
    #[prost(string, tag = "50016")]
    data_purpose: String,
    #[prost(int32, tag = "50017")]
    retention_days: i32,
    #[prost(bool, tag = "50018")]
    tokenized: bool,
    #[prost(int32, tag = "50019")]
    security_classification: i32,
    #[prost(int32, tag = "50020")]
    data_category: i32,
    #[prost(message, optional, tag = "50033")]
    db_column_security: Option<DbColumnSecurityOpts>,
}

#[derive(Clone, PartialEq, Message)]
struct MethodOpts {
    #[prost(message, optional, tag = "51001")]
    endpoint_security: Option<EndpointSec>,
    #[prost(message, optional, tag = "51002")]
    rest_contract: Option<RestContractOpts>,
    #[prost(message, optional, tag = "51003")]
    sdk_surface: Option<SdkSurfaceOpts>,
    #[prost(message, optional, tag = "51004")]
    cli_scaffold: Option<CliScaffoldOpts>,
    #[prost(message, optional, tag = "51005")]
    event_contract: Option<EventContractOpts>,
    #[prost(message, optional, tag = "51006")]
    dependency_contract: Option<DependencyContractOpts>,
    #[prost(int32, tag = "51007")]
    operation_kind: i32,
    #[prost(message, optional, tag = "51008")]
    precondition_contract: Option<PreconditionContractOpts>,
    #[prost(message, optional, tag = "51009")]
    readback_contract: Option<ReadAfterWriteContractOpts>,
    #[prost(message, optional, tag = "51010")]
    lifecycle_contract: Option<LifecycleContractOpts>,
    #[prost(message, optional, tag = "51011")]
    idempotency_contract: Option<IdempotencyContractOpts>,
    #[prost(message, optional, tag = "51012")]
    error_contract: Option<ErrorContractOpts>,
    #[prost(message, optional, tag = "72295728")]
    http_rule: Option<HttpRuleOpts>,
}

#[derive(Clone, PartialEq, Message)]
struct MessageOpts {
    #[prost(message, optional, tag = "52001")]
    db_table_security: Option<DbTableSecurityOpts>,
    #[prost(message, optional, tag = "52002")]
    event_contract: Option<EventContractOpts>,
    #[prost(message, optional, tag = "52003")]
    sdk_surface: Option<SdkSurfaceOpts>,
    #[prost(message, optional, tag = "52004")]
    dependency_contract: Option<DependencyContractOpts>,
}

#[derive(Clone, PartialEq, Message)]
struct ServiceOpts {
    #[prost(message, optional, tag = "53001")]
    native_service: Option<NativeSvcOpts>,
    #[prost(message, optional, tag = "53002")]
    sdk_surface: Option<SdkSurfaceOpts>,
    #[prost(message, optional, tag = "53003")]
    cli_scaffold: Option<CliScaffoldOpts>,
    #[prost(message, optional, tag = "53004")]
    dependency_contract: Option<DependencyContractOpts>,
}

#[derive(Clone, PartialEq, Message)]
struct EndpointSec {
    #[prost(int32, tag = "1")]
    mode: i32,
    #[prost(string, repeated, tag = "2")]
    roles: Vec<String>,
    #[prost(string, repeated, tag = "3")]
    scopes: Vec<String>,
    #[prost(bool, tag = "4")]
    tenant_required: bool,
    #[prost(bool, tag = "5")]
    csrf_required: bool,
    #[prost(string, tag = "6")]
    policy_ref: String,
    #[prost(bool, tag = "7")]
    internal_grpc_only: bool,
    #[prost(int32, tag = "8")]
    required_assurance_level: i32,
    #[prost(int32, repeated, tag = "9")]
    allowed_credential_types: Vec<i32>,
    #[prost(string, tag = "10")]
    rate_limit_policy_ref: String,
    #[prost(string, tag = "11")]
    abuse_policy_ref: String,
    #[prost(string, tag = "12")]
    audit_event_type: String,
    #[prost(string, tag = "13")]
    decision_resource: String,
    #[prost(string, tag = "14")]
    owner_field: String,
    #[prost(string, tag = "15")]
    tenant_field: String,
    #[prost(string, tag = "16")]
    project_field: String,
    #[prost(bool, tag = "17")]
    idempotency_required: bool,
    #[prost(bool, tag = "18")]
    request_context_required: bool,
}

#[derive(Clone, PartialEq, Message)]
struct NativeSvcOpts {
    #[prost(string, tag = "1")]
    service_id: String,
    #[prost(string, tag = "2")]
    logical_service_id: String,
    #[prost(string, tag = "3")]
    proto_service_id: String,
    #[prost(string, tag = "4")]
    display_name: String,
    #[prost(string, tag = "5")]
    category: String,
    #[prost(bool, tag = "6")]
    default_enabled: bool,
    #[prost(bool, tag = "7")]
    requires_postgres: bool,
    #[prost(bool, tag = "8")]
    requires_redis: bool,
    #[prost(bool, tag = "9")]
    requires_object_store: bool,
    #[prost(bool, tag = "10")]
    requires_kafka: bool,
    #[prost(string, tag = "11")]
    requires_feature: String,
    #[prost(bool, tag = "12")]
    public_listener_allowed: bool,
    #[prost(bool, tag = "13")]
    control_plane_listener_allowed: bool,
    #[prost(bool, tag = "14")]
    peer_listener_allowed: bool,
    #[prost(string, tag = "15")]
    sdk_facade_name: String,
    #[prost(string, tag = "16")]
    cli_scaffold_group: String,
    #[prost(string, tag = "17")]
    health_check_ref: String,
    #[prost(string, tag = "18")]
    capability_ref: String,
    #[prost(bool, tag = "19")]
    owns_background_workers: bool,
}

#[derive(Clone, PartialEq, Message)]
struct SdkSurfaceOpts {
    #[prost(bool, tag = "1")]
    include_in_facade: bool,
    #[prost(string, tag = "2")]
    method_alias: String,
    #[prost(string, tag = "3")]
    required_credential_provider: String,
    #[prost(string, tag = "4")]
    streaming_helper_type: String,
    #[prost(int32, tag = "5")]
    default_deadline_ms: i32,
    #[prost(int32, tag = "6")]
    default_max_attempts: i32,
    #[prost(bool, tag = "7")]
    browser_safe: bool,
    #[prost(bool, tag = "8")]
    server_only: bool,
    #[prost(string, repeated, tag = "9")]
    boilerplate_recipe_tags: Vec<String>,
    #[prost(bool, tag = "10")]
    generate_minimal_example: bool,
    #[prost(string, tag = "11")]
    rest_operation_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct CliScaffoldOpts {
    #[prost(string, tag = "1")]
    scaffold_package: String,
    #[prost(string, tag = "2")]
    import_path: String,
    #[prost(string, repeated, tag = "3")]
    required_env: Vec<String>,
    #[prost(string, repeated, tag = "4")]
    generated_files: Vec<String>,
    #[prost(string, tag = "5")]
    route_name: String,
    #[prost(string, tag = "6")]
    middleware_name: String,
    #[prost(string, repeated, tag = "7")]
    required_native_services: Vec<String>,
    #[prost(string, repeated, tag = "8")]
    optional_native_services: Vec<String>,
    #[prost(string, repeated, tag = "9")]
    secret_placeholders: Vec<String>,
    #[prost(string, repeated, tag = "10")]
    post_generation_commands: Vec<String>,
    #[prost(string, tag = "11")]
    smoke_test_command: String,
}

#[derive(Clone, PartialEq, Message)]
struct RestContractOpts {
    #[prost(bool, tag = "1")]
    response_envelope: bool,
    #[prost(bool, tag = "2")]
    api_error: bool,
    #[prost(bool, tag = "3")]
    pagination_meta: bool,
    #[prost(bool, tag = "4")]
    explicit_nulls: bool,
}

#[derive(Clone, PartialEq, Message)]
struct HttpRuleOpts {
    #[prost(string, tag = "2")]
    get: String,
    #[prost(string, tag = "3")]
    put: String,
    #[prost(string, tag = "4")]
    post: String,
    #[prost(string, tag = "5")]
    delete: String,
    #[prost(string, tag = "6")]
    patch: String,
    #[prost(string, tag = "7")]
    body: String,
    #[prost(string, tag = "12")]
    response_body: String,
}

#[derive(Clone, PartialEq, Message)]
struct EventContractOpts {
    #[prost(string, tag = "1")]
    event_type: String,
    #[prost(string, tag = "2")]
    outbox_topic: String,
    #[prost(string, tag = "3")]
    partition_key_field: String,
    #[prost(string, tag = "4")]
    payload_redaction_profile: String,
    #[prost(string, tag = "5")]
    delivery_guarantee: String,
    #[prost(string, tag = "6")]
    replay_compatibility: String,
    #[prost(message, repeated, tag = "7")]
    emits: Vec<EmittedEventOpts>,
}

#[derive(Clone, PartialEq, Message)]
struct EmittedEventOpts {
    #[prost(string, tag = "1")]
    topic: String,
    #[prost(string, tag = "2")]
    partition_key_field: String,
    #[prost(string, tag = "3")]
    delivery_guarantee: String,
    #[prost(string, tag = "4")]
    payload_redaction_profile: String,
    #[prost(bool, tag = "5")]
    conditional: bool,
}

#[derive(Clone, PartialEq, Message)]
struct DependencyContractOpts {
    #[prost(string, repeated, tag = "1")]
    required_native_services: Vec<String>,
    #[prost(string, repeated, tag = "2")]
    optional_native_services: Vec<String>,
    #[prost(string, repeated, tag = "3")]
    required_backends: Vec<String>,
    #[prost(string, repeated, tag = "4")]
    optional_backends: Vec<String>,
    #[prost(string, repeated, tag = "5")]
    required_features: Vec<String>,
    #[prost(string, repeated, tag = "6")]
    required_env: Vec<String>,
    #[prost(string, repeated, tag = "7")]
    degraded_when_missing: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
struct PreconditionContractOpts {
    #[prost(string, repeated, tag = "1")]
    required_resources: Vec<String>,
    #[prost(string, repeated, tag = "2")]
    required_prior_result_fields: Vec<String>,
    #[prost(string, repeated, tag = "3")]
    required_source_states: Vec<String>,
    #[prost(string, tag = "4")]
    missing_code: String,
    #[prost(string, tag = "5")]
    wrong_state_code: String,
}

#[derive(Clone, PartialEq, Message)]
struct ReadAfterWriteContractOpts {
    #[prost(string, tag = "1")]
    returned_id_field: String,
    #[prost(string, tag = "2")]
    readback_rpc: String,
    #[prost(string, tag = "3")]
    readback_request_field: String,
    #[prost(bool, tag = "4")]
    requires_read_fence: bool,
    #[prost(bool, tag = "5")]
    requires_primary_read: bool,
    #[prost(string, tag = "6")]
    no_readback_reason: String,
}

#[derive(Clone, PartialEq, Message)]
struct LifecycleContractOpts {
    #[prost(string, tag = "1")]
    entity: String,
    #[prost(string, repeated, tag = "2")]
    legal_source_states: Vec<String>,
    #[prost(string, tag = "3")]
    target_state: String,
    #[prost(string, repeated, tag = "4")]
    terminal_states: Vec<String>,
    #[prost(bool, tag = "5")]
    input_consumed: bool,
    #[prost(bool, tag = "6")]
    destructive: bool,
}

#[derive(Clone, PartialEq, Message)]
struct IdempotencyContractOpts {
    #[prost(string, tag = "1")]
    request_key_field: String,
    #[prost(bool, tag = "2")]
    server_generated_key: bool,
    #[prost(string, tag = "3")]
    duplicate_response_field: String,
    #[prost(bool, tag = "4")]
    replay_safe: bool,
}

#[derive(Clone, PartialEq, Message)]
struct ErrorContractOpts {
    #[prost(message, repeated, tag = "1")]
    cases: Vec<ErrorCaseOpts>,
}

#[derive(Clone, PartialEq, Message)]
struct ErrorCaseOpts {
    #[prost(string, tag = "1")]
    canonical_code: String,
    #[prost(string, tag = "2")]
    grpc_status: String,
    #[prost(bool, tag = "3")]
    retryable: bool,
    #[prost(string, tag = "4")]
    details_type: String,
}

#[derive(Clone, PartialEq, Message)]
struct DbTableSecurityOpts {
    #[prost(string, tag = "1")]
    tenant_isolation_mode: String,
    #[prost(string, tag = "2")]
    project_isolation_mode: String,
    #[prost(string, tag = "3")]
    tenant_column: String,
    #[prost(string, tag = "4")]
    project_column: String,
    #[prost(string, tag = "5")]
    rls_policy_template: String,
    #[prost(string, tag = "6")]
    soft_delete_mode: String,
    #[prost(string, tag = "7")]
    retention_class: String,
    #[prost(int32, tag = "8")]
    retention_days: i32,
    #[prost(int32, tag = "9")]
    audit_mode: i32,
    #[prost(string, tag = "10")]
    encryption_profile: String,
    #[prost(string, tag = "11")]
    pii_profile: String,
    #[prost(bool, tag = "12")]
    break_glass_visible: bool,
    #[prost(bool, tag = "13")]
    export_eligible: bool,
    #[prost(string, tag = "14")]
    data_residency_policy_ref: String,
}

#[derive(Clone, PartialEq, Message)]
struct DbColumnSecurityOpts {
    #[prost(int32, tag = "1")]
    secret_classification: i32,
    #[prost(int32, tag = "2")]
    output_view: i32,
    #[prost(int32, tag = "3")]
    redaction_strategy: i32,
    #[prost(string, tag = "4")]
    tokenization_strategy: String,
    #[prost(string, tag = "5")]
    hashing_strategy: String,
    #[prost(string, tag = "6")]
    hashing_algorithm: String,
    #[prost(string, tag = "7")]
    encryption_key_class: String,
    #[prost(bool, tag = "8")]
    searchable_encrypted: bool,
    #[prost(string, tag = "9")]
    uniqueness_scope: String,
    #[prost(bool, tag = "10")]
    owner_field: bool,
    #[prost(bool, tag = "11")]
    tenant_field: bool,
    #[prost(bool, tag = "12")]
    project_field: bool,
}

#[cfg(all(test, not(udb_portable)))]
mod tests {
    use super::*;

    /// The embedded-contract tests below assert properties of the SERVER's
    /// compiled-in descriptor. `udb-portable` `#[path]`-shares this file but
    /// stubs `embedded_file_descriptor_set()` to empty (browser/edge callers
    /// supply the bytes at runtime), so on that target there is no contract to
    /// check — skip instead of tripping the fail-closed decode panic. The real
    /// gate always runs in the main `udb` crate, where the set is never empty.
    fn embedded_contract_available() -> bool {
        !crate::runtime::native_catalog::embedded_file_descriptor_set().is_empty()
    }

    #[test]
    fn descriptor_manifest_decodes_native_service_and_method_security() {
        if !embedded_contract_available() {
            return;
        }
        let manifest = descriptor_contract_manifest();
        let authn = manifest
            .services
            .iter()
            .find(|service| service.full_name() == "udb.core.authn.services.v1.AuthnService")
            .expect("AuthnService must be present");
        let native = authn
            .native_service
            .as_ref()
            .expect("AuthnService must carry native_service options");
        assert_eq!(native.service_id, "authn");
        assert!(native.default_enabled);
        assert!(native.control_plane_listener_allowed);

        let authenticate = authn
            .methods
            .iter()
            .find(|rpc| rpc.method == "Authenticate")
            .expect("Authenticate must be present");
        let security = authenticate
            .endpoint_security
            .as_ref()
            .expect("Authenticate must carry endpoint_security");
        assert_eq!(security.auth_mode_name(), "public");
        assert!(!security.rate_limit_policy_ref.is_empty());
    }

    #[test]
    fn notification_retry_descriptor_allows_failed_only() {
        if !embedded_contract_available() {
            return;
        }
        let manifest = descriptor_contract_manifest();
        let retry = manifest
            .services
            .iter()
            .find(|service| {
                service.full_name() == "udb.core.notification.services.v1.NotificationService"
            })
            .and_then(|service| {
                service
                    .methods
                    .iter()
                    .find(|rpc| rpc.method == "RetryNotification")
            })
            .expect("NotificationService.RetryNotification must be present");
        let lifecycle = retry
            .lifecycle_contract
            .as_ref()
            .expect("RetryNotification must carry a lifecycle contract");

        assert_eq!(lifecycle.legal_source_states, vec!["FAILED".to_string()]);
        assert_eq!(lifecycle.target_state, "PENDING");
        assert!(
            !lifecycle
                .legal_source_states
                .iter()
                .any(|state| state == "SUPPRESSED"),
            "an opt-out suppression must never be advertised as retryable"
        );
    }

    #[test]
    fn notification_sent_descriptors_use_recipient_ref_partition() {
        if !embedded_contract_available() {
            return;
        }
        let manifest = descriptor_contract_manifest();
        let sent_message = manifest
            .messages
            .iter()
            .find(|message| {
                message.full_name == "udb.core.notification.events.v1.NotificationSentEvent"
            })
            .and_then(|message| message.event_contract.as_ref())
            .expect("NotificationSentEvent must carry an event contract");
        assert_eq!(sent_message.outbox_topic, "udb.notification.sent.v1");
        assert_eq!(sent_message.partition_key_field, "recipient_ref");

        let send = manifest
            .services
            .iter()
            .find(|service| {
                service.full_name() == "udb.core.notification.services.v1.NotificationService"
            })
            .and_then(|service| {
                service
                    .methods
                    .iter()
                    .find(|rpc| rpc.method == "SendNotification")
            })
            .expect("NotificationService.SendNotification must be present");
        let sent_emit = send
            .emits
            .iter()
            .find(|emit| emit.topic == "udb.notification.sent.v1")
            .expect("SendNotification must declare the sent event");
        assert_eq!(sent_emit.partition_key_field, "recipient_ref");
        assert!(sent_emit.conditional);
        assert!(
            send.emits.iter().any(|emit| {
                emit.topic == "udb.notification.suppressed.v1"
                    && emit.partition_key_field == "tenant_id"
                    && emit.conditional
            }),
            "SendNotification must declare its conditional suppression event"
        );
    }

    // Phase 7 conformance (final_task.md §8 "Reconcile proto event contracts with
    // runtime topics" + "conformance tests proving every declared mutation event
    // is emitted or explicitly marked no-event"). Every event contract declared by
    // a native service must use a well-formed, dot-delimited, Kafka-safe topic
    // (no '/', ':', or whitespace) for BOTH its event_type and outbox_topic, and
    // must carry a partition key + delivery guarantee — so no native event can be
    // published unpartitioned or to a malformed/unroutable Kafka topic.
    #[test]
    fn native_service_event_contracts_are_kafka_safe_dot_topics() {
        if !embedded_contract_available() {
            return;
        }
        // A Kafka-routable dot topic: non-empty, dotted, no path/auth separators.
        fn kafka_safe(topic: &str) -> bool {
            !topic.is_empty()
                && topic.contains('.')
                && !topic.contains('/')
                && !topic.contains(':')
                && !topic.chars().any(|c| c.is_whitespace())
        }
        let manifest = descriptor_contract_manifest();
        let mut checked = 0usize;
        for service in &manifest.services {
            // Only native services emit outbox→CDC events.
            if service.native_service.is_none() {
                continue;
            }
            for rpc in &service.methods {
                let Some(ev) = rpc.event_contract.as_ref() else {
                    continue;
                };
                let path = rpc.grpc_path();
                assert!(
                    kafka_safe(&ev.event_type),
                    "{path}: event_type '{}' must be a non-empty, dot-delimited, Kafka-safe topic (no '/', ':', or whitespace)",
                    ev.event_type
                );
                assert!(
                    kafka_safe(&ev.outbox_topic),
                    "{path}: outbox_topic '{}' must be a non-empty, dot-delimited, Kafka-safe topic",
                    ev.outbox_topic
                );
                assert!(
                    !ev.partition_key_field.is_empty(),
                    "{path}: event contract must declare a partition_key_field"
                );
                assert!(
                    !ev.delivery_guarantee.is_empty(),
                    "{path}: event contract must declare a delivery_guarantee"
                );
                checked += 1;
            }
        }
        assert!(
            checked > 0,
            "expected native services to declare event contracts in the descriptor"
        );
    }

    // Phase 7 acceptance (final_task.md §8: "Tenant, notification, storage, asset,
    // and WebRTC event contracts match emitted Kafka topics."). The canonical model
    // (docs/event-contract-model.md) is the runtime's versioned per-event topics; an
    // RPC's `method_event_contract.emits[]` must declare exactly the versioned topics
    // its handler actually publishes. This test reconciles the *declared* `emits[]`
    // against a curated `actual` map derived from the runtime emit sites
    // (`*_service/mod.rs` `emit_event(...)`/topic consts, cross-checked 2026-06-09)
    // in BOTH directions:
    //   - declared ⊆ actual: no RPC may claim to emit a topic its handler never does.
    //   - actual ⊆ declared: every topic a handler actually emits must be declared.
    // It also re-asserts each declared topic is a well-formed versioned
    // `udb.<...>.vN` dot topic with a partition key + delivery guarantee. An RPC with
    // a `method_event_contract` but an empty `emits[]` (e.g. tenant CRUD, webrtc read
    // RPCs, UnpublishTrack) is an explicit, recorded "no event".
    #[test]
    fn declared_emits_reconcile_with_runtime_emitted_topics() {
        if !embedded_contract_available() {
            return;
        }
        // Curated actual emit map: grpc_path -> set of versioned topics the handler
        // actually publishes. Sourced from the runtime emit sites (verified, not
        // assumed): a topic added/removed in `*_service/mod.rs` must be reflected
        // here AND in the proto `emits[]`, or this test fails.
        let actual: &[(&str, &[&str])] = &[
            // webrtc (src/runtime/service/webrtc_service/mod.rs)
            (
                "/udb.core.webrtc.services.v1.RoomService/CreateRoom",
                &["udb.webrtc.room.created.v1"],
            ),
            (
                "/udb.core.webrtc.services.v1.RoomService/CloseRoom",
                &[
                    "udb.webrtc.peer.left.v1",
                    "udb.webrtc.track.ended.v1",
                    "udb.webrtc.room.closed.v1",
                ],
            ),
            (
                "/udb.core.webrtc.services.v1.PeerService/JoinRoom",
                &["udb.webrtc.peer.joined.v1"],
            ),
            (
                "/udb.core.webrtc.services.v1.PeerService/LeaveRoom",
                &["udb.webrtc.peer.left.v1"],
            ),
            (
                "/udb.core.webrtc.services.v1.TrackService/PublishTrack",
                &["udb.webrtc.track.published.v1"],
            ),
            // storage (src/runtime/service/storage_service/mod.rs)
            (
                "/udb.core.storage.services.v1.StorageService/RegisterUpload",
                &["udb.storage.file.upload_url_issued.v1"],
            ),
            (
                "/udb.core.storage.services.v1.StorageService/FinalizeUpload",
                &["udb.storage.file.finalized.v1"],
            ),
            (
                "/udb.core.storage.services.v1.StorageService/UpdateFile",
                &["udb.storage.file.metadata_updated.v1"],
            ),
            (
                "/udb.core.storage.services.v1.StorageService/DeleteFile",
                &["udb.storage.file.deleted.v1"],
            ),
            // asset (src/runtime/service/asset_service/mod.rs)
            (
                "/udb.core.asset.services.v1.AssetService/RegisterAsset",
                &["udb.asset.asset.registered.v1"],
            ),
            (
                "/udb.core.asset.services.v1.AssetService/StartPipeline",
                &["udb.asset.pipeline.started.v1"],
            ),
            (
                "/udb.core.asset.services.v1.AssetService/CompleteStep",
                &[
                    "udb.asset.pipeline.step_completed.v1",
                    "udb.asset.pipeline.completed.v1",
                    "udb.asset.pipeline.failed.v1",
                ],
            ),
            // notification (src/runtime/service/notification_service/mod.rs)
            (
                "/udb.core.notification.services.v1.NotificationService/SendNotification",
                &["udb.notification.sent.v1"],
            ),
            // ReportDelivery publishes `udb.notification.delivery.<status>.v1`
            // (emit_delivery_event/delivery_event_topic). The contract declares the
            // canonical conditional emit (delivered.v1); the handler emits it on the
            // delivered path. Other terminal statuses (sent/failed/pending) reuse the
            // same conditional emit family.
            (
                "/udb.core.notification.services.v1.NotificationService/ReportDelivery",
                &["udb.notification.delivery.delivered.v1"],
            ),
            // webrtc (src/runtime/service/webrtc_service/mod.rs)
            (
                "/udb.core.webrtc.services.v1.RoomService/StartRoomComposite",
                &[
                    "udb.webrtc.egress.started.v1",
                    "udb.webrtc.egress.failed.v1",
                ],
            ),
            (
                "/udb.core.webrtc.services.v1.RoomService/StartTrackEgress",
                &[
                    "udb.webrtc.egress.started.v1",
                    "udb.webrtc.egress.failed.v1",
                ],
            ),
            (
                "/udb.core.webrtc.services.v1.RoomService/StopEgress",
                &[
                    "udb.webrtc.egress.stopped.v1",
                    "udb.webrtc.egress.failed.v1",
                ],
            ),
            // tenant: no runtime emits — intentional no-event (empty emits[]).
        ];

        // A well-formed versioned dot topic: udb.<...>.vN, dotted, Kafka-safe.
        fn versioned_topic(topic: &str) -> bool {
            topic.starts_with("udb.")
                && topic.contains('.')
                && !topic.contains('/')
                && !topic.contains(':')
                && !topic.chars().any(|c| c.is_whitespace())
                && topic
                    .rsplit('.')
                    .next()
                    .is_some_and(|seg| seg.starts_with('v') && seg[1..].parse::<u32>().is_ok())
        }

        use std::collections::{BTreeMap, BTreeSet};
        let actual_map: BTreeMap<&str, BTreeSet<&str>> = actual
            .iter()
            .map(|(path, topics)| (*path, topics.iter().copied().collect()))
            .collect();

        let manifest = descriptor_contract_manifest();
        // Declared emits keyed by grpc_path, for the five native event-owning services.
        let mut declared_map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for service in &manifest.services {
            if service.native_service.is_none() {
                continue;
            }
            for rpc in &service.methods {
                let path = rpc.grpc_path();
                for emit in &rpc.emits {
                    assert!(
                        versioned_topic(&emit.topic),
                        "{path}: emits topic '{}' must be a well-formed versioned udb.*.vN dot topic",
                        emit.topic
                    );
                    assert!(
                        !emit.partition_key_field.is_empty(),
                        "{path}: emits '{}' must declare a partition_key_field",
                        emit.topic
                    );
                    assert!(
                        !emit.delivery_guarantee.is_empty(),
                        "{path}: emits '{}' must declare a delivery_guarantee",
                        emit.topic
                    );
                    declared_map
                        .entry(path.clone())
                        .or_default()
                        .insert(emit.topic.clone());
                }
            }
        }

        // declared ⊆ actual: every declared emit must be a real runtime emission.
        for (path, declared) in &declared_map {
            let actual_for = actual_map.get(path.as_str()).unwrap_or_else(|| {
                panic!(
                    "{path}: declares emits[] {declared:?} but is not in the curated runtime emit map; \
                     re-verify the handler in *_service/mod.rs and update the fixture"
                )
            });
            for topic in declared {
                assert!(
                    actual_for.contains(topic.as_str()),
                    "{path}: declares emit '{topic}' that the runtime handler does not emit \
                     (actual: {actual_for:?})"
                );
            }
        }

        // actual ⊆ declared: every runtime emission must be declared in the contract.
        for (path, topics) in &actual_map {
            let declared = declared_map.get(*path).unwrap_or_else(|| {
                panic!(
                    "{path}: runtime emits {topics:?} but the proto method_event_contract declares \
                     no emits[]; re-annotate the RPC"
                )
            });
            for topic in topics {
                assert!(
                    declared.contains(*topic),
                    "{path}: runtime emits '{topic}' but it is not declared in emits[] \
                     (declared: {declared:?})"
                );
            }
        }
    }

    // Phase 6 (final_task.md §7 "Make no-leak tests descriptor-derived so new
    // sensitive fields are automatically covered"): every field a proto marks
    // `OUTPUT_VIEW_STORAGE_ONLY` must be covered by the generated outbound DTO
    // redaction layer. The assertion is keyed by full message name and field
    // name so same-named secret fields cannot accidentally mask each other.
    #[test]
    fn storage_only_fields_match_generated_redaction_coverage() {
        if !embedded_contract_available() {
            return;
        }
        const OUTPUT_VIEW_STORAGE_ONLY: i32 = 1;
        let manifest = descriptor_contract_manifest();
        let mut descriptor_fields: std::collections::BTreeMap<
            String,
            std::collections::BTreeSet<String>,
        > = std::collections::BTreeMap::new();
        for message in &manifest.messages {
            for field in &message.fields {
                let storage_only = field
                    .db_column_security
                    .as_ref()
                    .is_some_and(|cs| cs.output_view == OUTPUT_VIEW_STORAGE_ONLY);
                if storage_only {
                    descriptor_fields
                        .entry(message.full_name.clone())
                        .or_default()
                        .insert(field.name.clone());
                }
            }
        }

        let generated_fields: std::collections::BTreeMap<
            String,
            std::collections::BTreeSet<String>,
        > = crate::proto_redaction::GENERATED_STORAGE_ONLY_REDACTION_FIELDS
            .iter()
            .map(|(message, fields)| {
                (
                    (*message).to_string(),
                    fields.iter().map(|field| (*field).to_string()).collect(),
                )
            })
            .collect();

        assert_eq!(
            descriptor_fields, generated_fields,
            "OUTPUT_VIEW_STORAGE_ONLY descriptor fields must exactly match the \
             generated RedactStorageOnly coverage map"
        );
    }

    // Pure (no live DB) no-leak coverage for the Phase 9 vault/webhook secret
    // columns. The build.rs `RedactStorageOnly` codegen blanks every
    // OUTPUT_VIEW_STORAGE_ONLY field before an entity leaves the broker in a
    // response; this exercises that generated redaction layer directly and asserts
    // the four newly-annotated storage-only fields are actually stripped, so they
    // can never be surfaced in an RPC response. (The live auth `*_noleak_live`
    // tests prove the same for the User/Session columns against real Postgres; this
    // gives the vault/webhook columns equivalent coverage without live infra.)
    #[test]
    fn phase9_storage_only_fields_are_blanked_by_generated_redaction() {
        use crate::proto_redaction::RedactStorageOnly;

        let mut secret = crate::proto::udb::core::vault::entity::v1::VaultSecret {
            ciphertext: "sealed-secret-bytes".to_string(),
            data_key_wrapped: "wrapped-dek".to_string(),
            ..Default::default()
        };
        secret.redact_storage_only();
        assert!(
            secret.ciphertext.is_empty(),
            "VaultSecret.ciphertext must be blanked by the storage-only redaction layer"
        );
        assert!(
            secret.data_key_wrapped.is_empty(),
            "VaultSecret.data_key_wrapped must be blanked by the storage-only redaction layer"
        );

        let mut key = crate::proto::udb::core::vault::entity::v1::VaultTransitKey {
            wrapped_key_material: "wrapped-transit-dek".to_string(),
            ..Default::default()
        };
        key.redact_storage_only();
        assert!(
            key.wrapped_key_material.is_empty(),
            "VaultTransitKey.wrapped_key_material must be blanked by the storage-only \
             redaction layer"
        );

        let mut endpoint = crate::proto::udb::core::webhook::entity::v1::WebhookEndpoint {
            signing_secret: "hmac-signing-secret".to_string(),
            ..Default::default()
        };
        endpoint.redact_storage_only();
        assert!(
            endpoint.signing_secret.is_empty(),
            "WebhookEndpoint.signing_secret must be blanked by the storage-only redaction layer"
        );
    }

    #[test]
    fn descriptor_manifest_decodes_http_and_column_security() {
        if !embedded_contract_available() {
            return;
        }
        let manifest = descriptor_contract_manifest();
        let authn = manifest
            .services
            .iter()
            .find(|service| service.full_name() == "udb.core.authn.services.v1.AuthnService")
            .expect("AuthnService must be present");
        assert!(
            authn.methods.iter().any(|rpc| rpc
                .http_rule
                .as_ref()
                .is_some_and(|http| http.verb == "post")),
            "native service RPCs must expose google.api.http mappings"
        );

        let user = manifest
            .messages
            .iter()
            .find(|message| message.full_name == "udb.core.authn.entity.v1.User")
            .expect("User message must be present");
        let password = user
            .fields
            .iter()
            .find(|field| field.name == "password_hash")
            .expect("password_hash field must be present");
        assert!(password.scalar_security.sensitive);
        assert!(password.scalar_security.log_redacted);
        let structured = password
            .db_column_security
            .as_ref()
            .expect("password_hash must carry structured db_column_security");
        assert_eq!(structured.secret_classification, 3);
        assert_eq!(structured.output_view, 1);
    }

    #[test]
    fn sdk_surface_contract_decodes_rest_operation_id() {
        let surface = SdkSurfaceOpts {
            include_in_facade: true,
            method_alias: "get_jwks".to_string(),
            required_credential_provider: "udb".to_string(),
            streaming_helper_type: String::new(),
            default_deadline_ms: 30000,
            default_max_attempts: 3,
            browser_safe: true,
            server_only: false,
            boilerplate_recipe_tags: vec!["auth".to_string()],
            generate_minimal_example: true,
            rest_operation_id: "authn.getJwks".to_string(),
        };
        let contract = SdkSurfaceContract::from(&surface);
        assert_eq!(contract.method_alias, "get_jwks");
        assert_eq!(contract.rest_operation_id, "authn.getJwks");
    }
}
