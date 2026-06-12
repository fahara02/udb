from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OperationKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OPERATION_KIND_UNSPECIFIED: _ClassVar[OperationKind]
    OPERATION_KIND_READ_ONLY: _ClassVar[OperationKind]
    OPERATION_KIND_MUTATION: _ClassVar[OperationKind]
    OPERATION_KIND_DESTRUCTIVE: _ClassVar[OperationKind]

class AuthMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AUTH_MODE_UNSPECIFIED: _ClassVar[AuthMode]
    AUTH_MODE_PUBLIC: _ClassVar[AuthMode]
    AUTH_MODE_BEARER: _ClassVar[AuthMode]
    AUTH_MODE_API_KEY: _ClassVar[AuthMode]
    AUTH_MODE_SERVICE_ACCOUNT: _ClassVar[AuthMode]

class CredentialType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CREDENTIAL_TYPE_UNSPECIFIED: _ClassVar[CredentialType]
    CREDENTIAL_TYPE_BEARER_JWT: _ClassVar[CredentialType]
    CREDENTIAL_TYPE_SESSION: _ClassVar[CredentialType]
    CREDENTIAL_TYPE_API_KEY: _ClassVar[CredentialType]
    CREDENTIAL_TYPE_SERVICE_ACCOUNT: _ClassVar[CredentialType]
    CREDENTIAL_TYPE_MTLS: _ClassVar[CredentialType]
    CREDENTIAL_TYPE_OIDC: _ClassVar[CredentialType]
    CREDENTIAL_TYPE_SAML: _ClassVar[CredentialType]
    CREDENTIAL_TYPE_WEBAUTHN: _ClassVar[CredentialType]
    CREDENTIAL_TYPE_EXTERNAL_JWT: _ClassVar[CredentialType]

class AuditMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AUDIT_MODE_UNSPECIFIED: _ClassVar[AuditMode]
    AUDIT_MODE_NONE: _ClassVar[AuditMode]
    AUDIT_MODE_MUTATION: _ClassVar[AuditMode]
    AUDIT_MODE_DECISION: _ClassVar[AuditMode]
    AUDIT_MODE_FULL: _ClassVar[AuditMode]

class SecretClassification(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SECRET_CLASSIFICATION_UNSPECIFIED: _ClassVar[SecretClassification]
    SECRET_CLASSIFICATION_PUBLIC: _ClassVar[SecretClassification]
    SECRET_CLASSIFICATION_INTERNAL: _ClassVar[SecretClassification]
    SECRET_CLASSIFICATION_CREDENTIAL: _ClassVar[SecretClassification]
    SECRET_CLASSIFICATION_TOKEN: _ClassVar[SecretClassification]
    SECRET_CLASSIFICATION_KEY: _ClassVar[SecretClassification]
    SECRET_CLASSIFICATION_BIOMETRIC: _ClassVar[SecretClassification]
    SECRET_CLASSIFICATION_IDENTITY: _ClassVar[SecretClassification]
    SECRET_CLASSIFICATION_PII: _ClassVar[SecretClassification]
    SECRET_CLASSIFICATION_OPERATIONAL: _ClassVar[SecretClassification]
    SECRET_CLASSIFICATION_PRIVATE_KEY: _ClassVar[SecretClassification]

class OutputView(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OUTPUT_VIEW_UNSPECIFIED: _ClassVar[OutputView]
    OUTPUT_VIEW_STORAGE_ONLY: _ClassVar[OutputView]
    OUTPUT_VIEW_PUBLIC: _ClassVar[OutputView]
    OUTPUT_VIEW_SELF: _ClassVar[OutputView]
    OUTPUT_VIEW_ADMIN: _ClassVar[OutputView]
    OUTPUT_VIEW_AUDIT: _ClassVar[OutputView]
    OUTPUT_VIEW_NEVER: _ClassVar[OutputView]

class RedactionStrategy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REDACTION_STRATEGY_UNSPECIFIED: _ClassVar[RedactionStrategy]
    REDACTION_STRATEGY_NONE: _ClassVar[RedactionStrategy]
    REDACTION_STRATEGY_MASK: _ClassVar[RedactionStrategy]
    REDACTION_STRATEGY_REDACT: _ClassVar[RedactionStrategy]
    REDACTION_STRATEGY_HASH_ONLY: _ClassVar[RedactionStrategy]
    REDACTION_STRATEGY_LAST4: _ClassVar[RedactionStrategy]

class SecurityClassification(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SECURITY_CLASSIFICATION_UNSPECIFIED: _ClassVar[SecurityClassification]
    SECURITY_CLASSIFICATION_PUBLIC: _ClassVar[SecurityClassification]
    SECURITY_CLASSIFICATION_INTERNAL: _ClassVar[SecurityClassification]
    SECURITY_CLASSIFICATION_CONFIDENTIAL: _ClassVar[SecurityClassification]
    SECURITY_CLASSIFICATION_RESTRICTED: _ClassVar[SecurityClassification]

class DataCategory(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DATA_CATEGORY_UNSPECIFIED: _ClassVar[DataCategory]
    DATA_CATEGORY_PERSONAL: _ClassVar[DataCategory]
    DATA_CATEGORY_FINANCIAL: _ClassVar[DataCategory]
    DATA_CATEGORY_BIOMETRIC: _ClassVar[DataCategory]
    DATA_CATEGORY_IDENTITY: _ClassVar[DataCategory]
    DATA_CATEGORY_OPERATIONAL: _ClassVar[DataCategory]
    DATA_CATEGORY_SYSTEM: _ClassVar[DataCategory]
OPERATION_KIND_UNSPECIFIED: OperationKind
OPERATION_KIND_READ_ONLY: OperationKind
OPERATION_KIND_MUTATION: OperationKind
OPERATION_KIND_DESTRUCTIVE: OperationKind
AUTH_MODE_UNSPECIFIED: AuthMode
AUTH_MODE_PUBLIC: AuthMode
AUTH_MODE_BEARER: AuthMode
AUTH_MODE_API_KEY: AuthMode
AUTH_MODE_SERVICE_ACCOUNT: AuthMode
CREDENTIAL_TYPE_UNSPECIFIED: CredentialType
CREDENTIAL_TYPE_BEARER_JWT: CredentialType
CREDENTIAL_TYPE_SESSION: CredentialType
CREDENTIAL_TYPE_API_KEY: CredentialType
CREDENTIAL_TYPE_SERVICE_ACCOUNT: CredentialType
CREDENTIAL_TYPE_MTLS: CredentialType
CREDENTIAL_TYPE_OIDC: CredentialType
CREDENTIAL_TYPE_SAML: CredentialType
CREDENTIAL_TYPE_WEBAUTHN: CredentialType
CREDENTIAL_TYPE_EXTERNAL_JWT: CredentialType
AUDIT_MODE_UNSPECIFIED: AuditMode
AUDIT_MODE_NONE: AuditMode
AUDIT_MODE_MUTATION: AuditMode
AUDIT_MODE_DECISION: AuditMode
AUDIT_MODE_FULL: AuditMode
SECRET_CLASSIFICATION_UNSPECIFIED: SecretClassification
SECRET_CLASSIFICATION_PUBLIC: SecretClassification
SECRET_CLASSIFICATION_INTERNAL: SecretClassification
SECRET_CLASSIFICATION_CREDENTIAL: SecretClassification
SECRET_CLASSIFICATION_TOKEN: SecretClassification
SECRET_CLASSIFICATION_KEY: SecretClassification
SECRET_CLASSIFICATION_BIOMETRIC: SecretClassification
SECRET_CLASSIFICATION_IDENTITY: SecretClassification
SECRET_CLASSIFICATION_PII: SecretClassification
SECRET_CLASSIFICATION_OPERATIONAL: SecretClassification
SECRET_CLASSIFICATION_PRIVATE_KEY: SecretClassification
OUTPUT_VIEW_UNSPECIFIED: OutputView
OUTPUT_VIEW_STORAGE_ONLY: OutputView
OUTPUT_VIEW_PUBLIC: OutputView
OUTPUT_VIEW_SELF: OutputView
OUTPUT_VIEW_ADMIN: OutputView
OUTPUT_VIEW_AUDIT: OutputView
OUTPUT_VIEW_NEVER: OutputView
REDACTION_STRATEGY_UNSPECIFIED: RedactionStrategy
REDACTION_STRATEGY_NONE: RedactionStrategy
REDACTION_STRATEGY_MASK: RedactionStrategy
REDACTION_STRATEGY_REDACT: RedactionStrategy
REDACTION_STRATEGY_HASH_ONLY: RedactionStrategy
REDACTION_STRATEGY_LAST4: RedactionStrategy
SECURITY_CLASSIFICATION_UNSPECIFIED: SecurityClassification
SECURITY_CLASSIFICATION_PUBLIC: SecurityClassification
SECURITY_CLASSIFICATION_INTERNAL: SecurityClassification
SECURITY_CLASSIFICATION_CONFIDENTIAL: SecurityClassification
SECURITY_CLASSIFICATION_RESTRICTED: SecurityClassification
DATA_CATEGORY_UNSPECIFIED: DataCategory
DATA_CATEGORY_PERSONAL: DataCategory
DATA_CATEGORY_FINANCIAL: DataCategory
DATA_CATEGORY_BIOMETRIC: DataCategory
DATA_CATEGORY_IDENTITY: DataCategory
DATA_CATEGORY_OPERATIONAL: DataCategory
DATA_CATEGORY_SYSTEM: DataCategory
PII_FIELD_NUMBER: _ClassVar[int]
pii: _descriptor.FieldDescriptor
ENCRYPTED_SECURITY_FIELD_NUMBER: _ClassVar[int]
encrypted_security: _descriptor.FieldDescriptor
LOG_MASKED_FIELD_NUMBER: _ClassVar[int]
log_masked: _descriptor.FieldDescriptor
LOG_REDACTED_FIELD_NUMBER: _ClassVar[int]
log_redacted: _descriptor.FieldDescriptor
SENSITIVE_FIELD_NUMBER: _ClassVar[int]
sensitive: _descriptor.FieldDescriptor
REQUIRES_CONSENT_FIELD_NUMBER: _ClassVar[int]
requires_consent: _descriptor.FieldDescriptor
DATA_PURPOSE_FIELD_NUMBER: _ClassVar[int]
data_purpose: _descriptor.FieldDescriptor
RETENTION_DAYS_FIELD_NUMBER: _ClassVar[int]
retention_days: _descriptor.FieldDescriptor
TOKENIZED_FIELD_NUMBER: _ClassVar[int]
tokenized: _descriptor.FieldDescriptor
SECURITY_CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
security_classification: _descriptor.FieldDescriptor
DATA_CATEGORY_FIELD_NUMBER: _ClassVar[int]
data_category: _descriptor.FieldDescriptor
DB_COLUMN_SECURITY_FIELD_NUMBER: _ClassVar[int]
db_column_security: _descriptor.FieldDescriptor
ENDPOINT_SECURITY_FIELD_NUMBER: _ClassVar[int]
endpoint_security: _descriptor.FieldDescriptor
REST_CONTRACT_FIELD_NUMBER: _ClassVar[int]
rest_contract: _descriptor.FieldDescriptor
SDK_SURFACE_FIELD_NUMBER: _ClassVar[int]
sdk_surface: _descriptor.FieldDescriptor
METHOD_CLI_SCAFFOLD_FIELD_NUMBER: _ClassVar[int]
method_cli_scaffold: _descriptor.FieldDescriptor
METHOD_EVENT_CONTRACT_FIELD_NUMBER: _ClassVar[int]
method_event_contract: _descriptor.FieldDescriptor
METHOD_DEPENDENCY_CONTRACT_FIELD_NUMBER: _ClassVar[int]
method_dependency_contract: _descriptor.FieldDescriptor
OPERATION_KIND_FIELD_NUMBER: _ClassVar[int]
operation_kind: _descriptor.FieldDescriptor
DB_TABLE_SECURITY_FIELD_NUMBER: _ClassVar[int]
db_table_security: _descriptor.FieldDescriptor
MESSAGE_EVENT_CONTRACT_FIELD_NUMBER: _ClassVar[int]
message_event_contract: _descriptor.FieldDescriptor
MESSAGE_SDK_SURFACE_FIELD_NUMBER: _ClassVar[int]
message_sdk_surface: _descriptor.FieldDescriptor
MESSAGE_DEPENDENCY_CONTRACT_FIELD_NUMBER: _ClassVar[int]
message_dependency_contract: _descriptor.FieldDescriptor
NATIVE_SERVICE_FIELD_NUMBER: _ClassVar[int]
native_service: _descriptor.FieldDescriptor
SERVICE_SDK_SURFACE_FIELD_NUMBER: _ClassVar[int]
service_sdk_surface: _descriptor.FieldDescriptor
SERVICE_CLI_SCAFFOLD_FIELD_NUMBER: _ClassVar[int]
service_cli_scaffold: _descriptor.FieldDescriptor
SERVICE_DEPENDENCY_CONTRACT_FIELD_NUMBER: _ClassVar[int]
service_dependency_contract: _descriptor.FieldDescriptor

class EndpointSecurity(_message.Message):
    __slots__ = ("mode", "roles", "scopes", "tenant_required", "csrf_required", "policy_ref", "internal_grpc_only", "required_assurance_level", "allowed_credential_types", "rate_limit_policy_ref", "abuse_policy_ref", "audit_event_type", "decision_resource", "owner_field", "tenant_field", "project_field", "idempotency_required", "request_context_required")
    MODE_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    TENANT_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    CSRF_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    POLICY_REF_FIELD_NUMBER: _ClassVar[int]
    INTERNAL_GRPC_ONLY_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_ASSURANCE_LEVEL_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_CREDENTIAL_TYPES_FIELD_NUMBER: _ClassVar[int]
    RATE_LIMIT_POLICY_REF_FIELD_NUMBER: _ClassVar[int]
    ABUSE_POLICY_REF_FIELD_NUMBER: _ClassVar[int]
    AUDIT_EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    DECISION_RESOURCE_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    REQUEST_CONTEXT_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    mode: AuthMode
    roles: _containers.RepeatedScalarFieldContainer[str]
    scopes: _containers.RepeatedScalarFieldContainer[str]
    tenant_required: bool
    csrf_required: bool
    policy_ref: str
    internal_grpc_only: bool
    required_assurance_level: int
    allowed_credential_types: _containers.RepeatedScalarFieldContainer[CredentialType]
    rate_limit_policy_ref: str
    abuse_policy_ref: str
    audit_event_type: str
    decision_resource: str
    owner_field: str
    tenant_field: str
    project_field: str
    idempotency_required: bool
    request_context_required: bool
    def __init__(self, mode: _Optional[_Union[AuthMode, str]] = ..., roles: _Optional[_Iterable[str]] = ..., scopes: _Optional[_Iterable[str]] = ..., tenant_required: bool = ..., csrf_required: bool = ..., policy_ref: _Optional[str] = ..., internal_grpc_only: bool = ..., required_assurance_level: _Optional[int] = ..., allowed_credential_types: _Optional[_Iterable[_Union[CredentialType, str]]] = ..., rate_limit_policy_ref: _Optional[str] = ..., abuse_policy_ref: _Optional[str] = ..., audit_event_type: _Optional[str] = ..., decision_resource: _Optional[str] = ..., owner_field: _Optional[str] = ..., tenant_field: _Optional[str] = ..., project_field: _Optional[str] = ..., idempotency_required: bool = ..., request_context_required: bool = ...) -> None: ...

class RestContract(_message.Message):
    __slots__ = ("response_envelope", "api_error", "pagination_meta", "explicit_nulls")
    RESPONSE_ENVELOPE_FIELD_NUMBER: _ClassVar[int]
    API_ERROR_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_META_FIELD_NUMBER: _ClassVar[int]
    EXPLICIT_NULLS_FIELD_NUMBER: _ClassVar[int]
    response_envelope: bool
    api_error: bool
    pagination_meta: bool
    explicit_nulls: bool
    def __init__(self, response_envelope: bool = ..., api_error: bool = ..., pagination_meta: bool = ..., explicit_nulls: bool = ...) -> None: ...

class NativeServiceOptions(_message.Message):
    __slots__ = ("service_id", "logical_service_id", "proto_service_id", "display_name", "category", "default_enabled", "requires_postgres", "requires_redis", "requires_object_store", "requires_kafka", "requires_feature", "public_listener_allowed", "control_plane_listener_allowed", "peer_listener_allowed", "sdk_facade_name", "cli_scaffold_group", "health_check_ref", "capability_ref", "owns_background_workers")
    SERVICE_ID_FIELD_NUMBER: _ClassVar[int]
    LOGICAL_SERVICE_ID_FIELD_NUMBER: _ClassVar[int]
    PROTO_SERVICE_ID_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_ENABLED_FIELD_NUMBER: _ClassVar[int]
    REQUIRES_POSTGRES_FIELD_NUMBER: _ClassVar[int]
    REQUIRES_REDIS_FIELD_NUMBER: _ClassVar[int]
    REQUIRES_OBJECT_STORE_FIELD_NUMBER: _ClassVar[int]
    REQUIRES_KAFKA_FIELD_NUMBER: _ClassVar[int]
    REQUIRES_FEATURE_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_LISTENER_ALLOWED_FIELD_NUMBER: _ClassVar[int]
    CONTROL_PLANE_LISTENER_ALLOWED_FIELD_NUMBER: _ClassVar[int]
    PEER_LISTENER_ALLOWED_FIELD_NUMBER: _ClassVar[int]
    SDK_FACADE_NAME_FIELD_NUMBER: _ClassVar[int]
    CLI_SCAFFOLD_GROUP_FIELD_NUMBER: _ClassVar[int]
    HEALTH_CHECK_REF_FIELD_NUMBER: _ClassVar[int]
    CAPABILITY_REF_FIELD_NUMBER: _ClassVar[int]
    OWNS_BACKGROUND_WORKERS_FIELD_NUMBER: _ClassVar[int]
    service_id: str
    logical_service_id: str
    proto_service_id: str
    display_name: str
    category: str
    default_enabled: bool
    requires_postgres: bool
    requires_redis: bool
    requires_object_store: bool
    requires_kafka: bool
    requires_feature: str
    public_listener_allowed: bool
    control_plane_listener_allowed: bool
    peer_listener_allowed: bool
    sdk_facade_name: str
    cli_scaffold_group: str
    health_check_ref: str
    capability_ref: str
    owns_background_workers: bool
    def __init__(self, service_id: _Optional[str] = ..., logical_service_id: _Optional[str] = ..., proto_service_id: _Optional[str] = ..., display_name: _Optional[str] = ..., category: _Optional[str] = ..., default_enabled: bool = ..., requires_postgres: bool = ..., requires_redis: bool = ..., requires_object_store: bool = ..., requires_kafka: bool = ..., requires_feature: _Optional[str] = ..., public_listener_allowed: bool = ..., control_plane_listener_allowed: bool = ..., peer_listener_allowed: bool = ..., sdk_facade_name: _Optional[str] = ..., cli_scaffold_group: _Optional[str] = ..., health_check_ref: _Optional[str] = ..., capability_ref: _Optional[str] = ..., owns_background_workers: bool = ...) -> None: ...

class DbTableSecurityOptions(_message.Message):
    __slots__ = ("tenant_isolation_mode", "project_isolation_mode", "tenant_column", "project_column", "rls_policy_template", "soft_delete_mode", "retention_class", "retention_days", "audit_mode", "encryption_profile", "pii_profile", "break_glass_visible", "export_eligible", "data_residency_policy_ref")
    TENANT_ISOLATION_MODE_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ISOLATION_MODE_FIELD_NUMBER: _ClassVar[int]
    TENANT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    PROJECT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    RLS_POLICY_TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    SOFT_DELETE_MODE_FIELD_NUMBER: _ClassVar[int]
    RETENTION_CLASS_FIELD_NUMBER: _ClassVar[int]
    RETENTION_DAYS_FIELD_NUMBER: _ClassVar[int]
    AUDIT_MODE_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTION_PROFILE_FIELD_NUMBER: _ClassVar[int]
    PII_PROFILE_FIELD_NUMBER: _ClassVar[int]
    BREAK_GLASS_VISIBLE_FIELD_NUMBER: _ClassVar[int]
    EXPORT_ELIGIBLE_FIELD_NUMBER: _ClassVar[int]
    DATA_RESIDENCY_POLICY_REF_FIELD_NUMBER: _ClassVar[int]
    tenant_isolation_mode: str
    project_isolation_mode: str
    tenant_column: str
    project_column: str
    rls_policy_template: str
    soft_delete_mode: str
    retention_class: str
    retention_days: int
    audit_mode: AuditMode
    encryption_profile: str
    pii_profile: str
    break_glass_visible: bool
    export_eligible: bool
    data_residency_policy_ref: str
    def __init__(self, tenant_isolation_mode: _Optional[str] = ..., project_isolation_mode: _Optional[str] = ..., tenant_column: _Optional[str] = ..., project_column: _Optional[str] = ..., rls_policy_template: _Optional[str] = ..., soft_delete_mode: _Optional[str] = ..., retention_class: _Optional[str] = ..., retention_days: _Optional[int] = ..., audit_mode: _Optional[_Union[AuditMode, str]] = ..., encryption_profile: _Optional[str] = ..., pii_profile: _Optional[str] = ..., break_glass_visible: bool = ..., export_eligible: bool = ..., data_residency_policy_ref: _Optional[str] = ...) -> None: ...

class DbColumnSecurityOptions(_message.Message):
    __slots__ = ("secret_classification", "output_view", "redaction_strategy", "tokenization_strategy", "hashing_strategy", "hashing_algorithm", "encryption_key_class", "searchable_encrypted", "uniqueness_scope", "owner_field", "tenant_field", "project_field")
    SECRET_CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_VIEW_FIELD_NUMBER: _ClassVar[int]
    REDACTION_STRATEGY_FIELD_NUMBER: _ClassVar[int]
    TOKENIZATION_STRATEGY_FIELD_NUMBER: _ClassVar[int]
    HASHING_STRATEGY_FIELD_NUMBER: _ClassVar[int]
    HASHING_ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTION_KEY_CLASS_FIELD_NUMBER: _ClassVar[int]
    SEARCHABLE_ENCRYPTED_FIELD_NUMBER: _ClassVar[int]
    UNIQUENESS_SCOPE_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_FIELD_NUMBER: _ClassVar[int]
    secret_classification: SecretClassification
    output_view: OutputView
    redaction_strategy: RedactionStrategy
    tokenization_strategy: str
    hashing_strategy: str
    hashing_algorithm: str
    encryption_key_class: str
    searchable_encrypted: bool
    uniqueness_scope: str
    owner_field: bool
    tenant_field: bool
    project_field: bool
    def __init__(self, secret_classification: _Optional[_Union[SecretClassification, str]] = ..., output_view: _Optional[_Union[OutputView, str]] = ..., redaction_strategy: _Optional[_Union[RedactionStrategy, str]] = ..., tokenization_strategy: _Optional[str] = ..., hashing_strategy: _Optional[str] = ..., hashing_algorithm: _Optional[str] = ..., encryption_key_class: _Optional[str] = ..., searchable_encrypted: bool = ..., uniqueness_scope: _Optional[str] = ..., owner_field: bool = ..., tenant_field: bool = ..., project_field: bool = ...) -> None: ...

class SdkSurfaceOptions(_message.Message):
    __slots__ = ("include_in_facade", "method_alias", "required_credential_provider", "streaming_helper_type", "default_deadline_ms", "default_max_attempts", "browser_safe", "server_only", "boilerplate_recipe_tags", "generate_minimal_example")
    INCLUDE_IN_FACADE_FIELD_NUMBER: _ClassVar[int]
    METHOD_ALIAS_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_CREDENTIAL_PROVIDER_FIELD_NUMBER: _ClassVar[int]
    STREAMING_HELPER_TYPE_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    BROWSER_SAFE_FIELD_NUMBER: _ClassVar[int]
    SERVER_ONLY_FIELD_NUMBER: _ClassVar[int]
    BOILERPLATE_RECIPE_TAGS_FIELD_NUMBER: _ClassVar[int]
    GENERATE_MINIMAL_EXAMPLE_FIELD_NUMBER: _ClassVar[int]
    include_in_facade: bool
    method_alias: str
    required_credential_provider: str
    streaming_helper_type: str
    default_deadline_ms: int
    default_max_attempts: int
    browser_safe: bool
    server_only: bool
    boilerplate_recipe_tags: _containers.RepeatedScalarFieldContainer[str]
    generate_minimal_example: bool
    def __init__(self, include_in_facade: bool = ..., method_alias: _Optional[str] = ..., required_credential_provider: _Optional[str] = ..., streaming_helper_type: _Optional[str] = ..., default_deadline_ms: _Optional[int] = ..., default_max_attempts: _Optional[int] = ..., browser_safe: bool = ..., server_only: bool = ..., boilerplate_recipe_tags: _Optional[_Iterable[str]] = ..., generate_minimal_example: bool = ...) -> None: ...

class CliScaffoldOptions(_message.Message):
    __slots__ = ("scaffold_package", "import_path", "required_env", "generated_files", "route_name", "middleware_name", "required_native_services", "optional_native_services", "secret_placeholders", "post_generation_commands", "smoke_test_command")
    SCAFFOLD_PACKAGE_FIELD_NUMBER: _ClassVar[int]
    IMPORT_PATH_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_ENV_FIELD_NUMBER: _ClassVar[int]
    GENERATED_FILES_FIELD_NUMBER: _ClassVar[int]
    ROUTE_NAME_FIELD_NUMBER: _ClassVar[int]
    MIDDLEWARE_NAME_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_NATIVE_SERVICES_FIELD_NUMBER: _ClassVar[int]
    OPTIONAL_NATIVE_SERVICES_FIELD_NUMBER: _ClassVar[int]
    SECRET_PLACEHOLDERS_FIELD_NUMBER: _ClassVar[int]
    POST_GENERATION_COMMANDS_FIELD_NUMBER: _ClassVar[int]
    SMOKE_TEST_COMMAND_FIELD_NUMBER: _ClassVar[int]
    scaffold_package: str
    import_path: str
    required_env: _containers.RepeatedScalarFieldContainer[str]
    generated_files: _containers.RepeatedScalarFieldContainer[str]
    route_name: str
    middleware_name: str
    required_native_services: _containers.RepeatedScalarFieldContainer[str]
    optional_native_services: _containers.RepeatedScalarFieldContainer[str]
    secret_placeholders: _containers.RepeatedScalarFieldContainer[str]
    post_generation_commands: _containers.RepeatedScalarFieldContainer[str]
    smoke_test_command: str
    def __init__(self, scaffold_package: _Optional[str] = ..., import_path: _Optional[str] = ..., required_env: _Optional[_Iterable[str]] = ..., generated_files: _Optional[_Iterable[str]] = ..., route_name: _Optional[str] = ..., middleware_name: _Optional[str] = ..., required_native_services: _Optional[_Iterable[str]] = ..., optional_native_services: _Optional[_Iterable[str]] = ..., secret_placeholders: _Optional[_Iterable[str]] = ..., post_generation_commands: _Optional[_Iterable[str]] = ..., smoke_test_command: _Optional[str] = ...) -> None: ...

class EventContractOptions(_message.Message):
    __slots__ = ("event_type", "outbox_topic", "partition_key_field", "payload_redaction_profile", "delivery_guarantee", "replay_compatibility", "emits")
    class EmittedEvent(_message.Message):
        __slots__ = ("topic", "partition_key_field", "delivery_guarantee", "payload_redaction_profile", "conditional")
        TOPIC_FIELD_NUMBER: _ClassVar[int]
        PARTITION_KEY_FIELD_FIELD_NUMBER: _ClassVar[int]
        DELIVERY_GUARANTEE_FIELD_NUMBER: _ClassVar[int]
        PAYLOAD_REDACTION_PROFILE_FIELD_NUMBER: _ClassVar[int]
        CONDITIONAL_FIELD_NUMBER: _ClassVar[int]
        topic: str
        partition_key_field: str
        delivery_guarantee: str
        payload_redaction_profile: str
        conditional: bool
        def __init__(self, topic: _Optional[str] = ..., partition_key_field: _Optional[str] = ..., delivery_guarantee: _Optional[str] = ..., payload_redaction_profile: _Optional[str] = ..., conditional: bool = ...) -> None: ...
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    OUTBOX_TOPIC_FIELD_NUMBER: _ClassVar[int]
    PARTITION_KEY_FIELD_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_REDACTION_PROFILE_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_GUARANTEE_FIELD_NUMBER: _ClassVar[int]
    REPLAY_COMPATIBILITY_FIELD_NUMBER: _ClassVar[int]
    EMITS_FIELD_NUMBER: _ClassVar[int]
    event_type: str
    outbox_topic: str
    partition_key_field: str
    payload_redaction_profile: str
    delivery_guarantee: str
    replay_compatibility: str
    emits: _containers.RepeatedCompositeFieldContainer[EventContractOptions.EmittedEvent]
    def __init__(self, event_type: _Optional[str] = ..., outbox_topic: _Optional[str] = ..., partition_key_field: _Optional[str] = ..., payload_redaction_profile: _Optional[str] = ..., delivery_guarantee: _Optional[str] = ..., replay_compatibility: _Optional[str] = ..., emits: _Optional[_Iterable[_Union[EventContractOptions.EmittedEvent, _Mapping]]] = ...) -> None: ...

class DependencyContractOptions(_message.Message):
    __slots__ = ("required_native_services", "optional_native_services", "required_backends", "optional_backends", "required_features", "required_env", "degraded_when_missing")
    REQUIRED_NATIVE_SERVICES_FIELD_NUMBER: _ClassVar[int]
    OPTIONAL_NATIVE_SERVICES_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_BACKENDS_FIELD_NUMBER: _ClassVar[int]
    OPTIONAL_BACKENDS_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_FEATURES_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_ENV_FIELD_NUMBER: _ClassVar[int]
    DEGRADED_WHEN_MISSING_FIELD_NUMBER: _ClassVar[int]
    required_native_services: _containers.RepeatedScalarFieldContainer[str]
    optional_native_services: _containers.RepeatedScalarFieldContainer[str]
    required_backends: _containers.RepeatedScalarFieldContainer[str]
    optional_backends: _containers.RepeatedScalarFieldContainer[str]
    required_features: _containers.RepeatedScalarFieldContainer[str]
    required_env: _containers.RepeatedScalarFieldContainer[str]
    degraded_when_missing: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, required_native_services: _Optional[_Iterable[str]] = ..., optional_native_services: _Optional[_Iterable[str]] = ..., required_backends: _Optional[_Iterable[str]] = ..., optional_backends: _Optional[_Iterable[str]] = ..., required_features: _Optional[_Iterable[str]] = ..., required_env: _Optional[_Iterable[str]] = ..., degraded_when_missing: _Optional[_Iterable[str]] = ...) -> None: ...
