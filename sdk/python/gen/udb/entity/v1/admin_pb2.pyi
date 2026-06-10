from udb.entity.v1 import context_pb2 as _context_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CapabilitiesRequest(_message.Message):
    __slots__ = ("context", "project_id")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    project_id: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., project_id: _Optional[str] = ...) -> None: ...

class CapabilitiesResponse(_message.Message):
    __slots__ = ("schema_checksum", "protocol_version", "enabled_backends", "degraded_backends", "system_catalog_relations", "supported_rpcs", "backend_instances", "backend_capabilities", "protocol_support", "backend_protocol_support", "native_services")
    SCHEMA_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    ENABLED_BACKENDS_FIELD_NUMBER: _ClassVar[int]
    DEGRADED_BACKENDS_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_CATALOG_RELATIONS_FIELD_NUMBER: _ClassVar[int]
    SUPPORTED_RPCS_FIELD_NUMBER: _ClassVar[int]
    BACKEND_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    BACKEND_CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_SUPPORT_FIELD_NUMBER: _ClassVar[int]
    BACKEND_PROTOCOL_SUPPORT_FIELD_NUMBER: _ClassVar[int]
    NATIVE_SERVICES_FIELD_NUMBER: _ClassVar[int]
    schema_checksum: str
    protocol_version: str
    enabled_backends: _containers.RepeatedScalarFieldContainer[str]
    degraded_backends: _containers.RepeatedScalarFieldContainer[str]
    system_catalog_relations: _containers.RepeatedScalarFieldContainer[str]
    supported_rpcs: _containers.RepeatedScalarFieldContainer[str]
    backend_instances: _containers.RepeatedCompositeFieldContainer[BackendInstanceStatus]
    backend_capabilities: _containers.RepeatedCompositeFieldContainer[BackendCapabilityDescriptor]
    protocol_support: ProtocolSupport
    backend_protocol_support: _containers.RepeatedCompositeFieldContainer[BackendProtocolSupport]
    native_services: _containers.RepeatedCompositeFieldContainer[NativeServiceStatus]
    def __init__(self, schema_checksum: _Optional[str] = ..., protocol_version: _Optional[str] = ..., enabled_backends: _Optional[_Iterable[str]] = ..., degraded_backends: _Optional[_Iterable[str]] = ..., system_catalog_relations: _Optional[_Iterable[str]] = ..., supported_rpcs: _Optional[_Iterable[str]] = ..., backend_instances: _Optional[_Iterable[_Union[BackendInstanceStatus, _Mapping]]] = ..., backend_capabilities: _Optional[_Iterable[_Union[BackendCapabilityDescriptor, _Mapping]]] = ..., protocol_support: _Optional[_Union[ProtocolSupport, _Mapping]] = ..., backend_protocol_support: _Optional[_Iterable[_Union[BackendProtocolSupport, _Mapping]]] = ..., native_services: _Optional[_Iterable[_Union[NativeServiceStatus, _Mapping]]] = ...) -> None: ...

class ProtocolSupport(_message.Message):
    __slots__ = ("min_protocol_version", "max_protocol_version", "encodings", "compression", "supports_streaming_reads", "supports_object_streaming", "max_recv_message_bytes", "max_send_message_bytes", "supported_rpcs")
    MIN_PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    MAX_PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    ENCODINGS_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    SUPPORTS_STREAMING_READS_FIELD_NUMBER: _ClassVar[int]
    SUPPORTS_OBJECT_STREAMING_FIELD_NUMBER: _ClassVar[int]
    MAX_RECV_MESSAGE_BYTES_FIELD_NUMBER: _ClassVar[int]
    MAX_SEND_MESSAGE_BYTES_FIELD_NUMBER: _ClassVar[int]
    SUPPORTED_RPCS_FIELD_NUMBER: _ClassVar[int]
    min_protocol_version: str
    max_protocol_version: str
    encodings: _containers.RepeatedScalarFieldContainer[str]
    compression: _containers.RepeatedScalarFieldContainer[str]
    supports_streaming_reads: bool
    supports_object_streaming: bool
    max_recv_message_bytes: int
    max_send_message_bytes: int
    supported_rpcs: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, min_protocol_version: _Optional[str] = ..., max_protocol_version: _Optional[str] = ..., encodings: _Optional[_Iterable[str]] = ..., compression: _Optional[_Iterable[str]] = ..., supports_streaming_reads: bool = ..., supports_object_streaming: bool = ..., max_recv_message_bytes: _Optional[int] = ..., max_send_message_bytes: _Optional[int] = ..., supported_rpcs: _Optional[_Iterable[str]] = ...) -> None: ...

class BackendProtocolSupport(_message.Message):
    __slots__ = ("backend", "supports_streaming_reads", "supports_object_streaming", "encodings")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    SUPPORTS_STREAMING_READS_FIELD_NUMBER: _ClassVar[int]
    SUPPORTS_OBJECT_STREAMING_FIELD_NUMBER: _ClassVar[int]
    ENCODINGS_FIELD_NUMBER: _ClassVar[int]
    backend: str
    supports_streaming_reads: bool
    supports_object_streaming: bool
    encodings: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, backend: _Optional[str] = ..., supports_streaming_reads: bool = ..., supports_object_streaming: bool = ..., encodings: _Optional[_Iterable[str]] = ...) -> None: ...

class BackendCapabilityDescriptor(_message.Message):
    __slots__ = ("backend", "tier", "operations", "unsupported_error_code", "consistency_model", "max_payload_bytes", "supports_xa", "supports_two_phase_commit")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    TIER_FIELD_NUMBER: _ClassVar[int]
    OPERATIONS_FIELD_NUMBER: _ClassVar[int]
    UNSUPPORTED_ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    CONSISTENCY_MODEL_FIELD_NUMBER: _ClassVar[int]
    MAX_PAYLOAD_BYTES_FIELD_NUMBER: _ClassVar[int]
    SUPPORTS_XA_FIELD_NUMBER: _ClassVar[int]
    SUPPORTS_TWO_PHASE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    backend: str
    tier: str
    operations: _containers.RepeatedScalarFieldContainer[str]
    unsupported_error_code: str
    consistency_model: str
    max_payload_bytes: int
    supports_xa: bool
    supports_two_phase_commit: bool
    def __init__(self, backend: _Optional[str] = ..., tier: _Optional[str] = ..., operations: _Optional[_Iterable[str]] = ..., unsupported_error_code: _Optional[str] = ..., consistency_model: _Optional[str] = ..., max_payload_bytes: _Optional[int] = ..., supports_xa: bool = ..., supports_two_phase_commit: bool = ...) -> None: ...

class BackendInstanceStatus(_message.Message):
    __slots__ = ("backend", "instance_name", "role", "enabled", "configured", "connected", "read_weight", "write_weight", "labels", "capabilities", "routing_status", "healthy", "circuit_open")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_NAME_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    CONFIGURED_FIELD_NUMBER: _ClassVar[int]
    CONNECTED_FIELD_NUMBER: _ClassVar[int]
    READ_WEIGHT_FIELD_NUMBER: _ClassVar[int]
    WRITE_WEIGHT_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    ROUTING_STATUS_FIELD_NUMBER: _ClassVar[int]
    HEALTHY_FIELD_NUMBER: _ClassVar[int]
    CIRCUIT_OPEN_FIELD_NUMBER: _ClassVar[int]
    backend: str
    instance_name: str
    role: str
    enabled: bool
    configured: bool
    connected: bool
    read_weight: int
    write_weight: int
    labels: _containers.ScalarMap[str, str]
    capabilities: _containers.RepeatedScalarFieldContainer[str]
    routing_status: str
    healthy: bool
    circuit_open: bool
    def __init__(self, backend: _Optional[str] = ..., instance_name: _Optional[str] = ..., role: _Optional[str] = ..., enabled: bool = ..., configured: bool = ..., connected: bool = ..., read_weight: _Optional[int] = ..., write_weight: _Optional[int] = ..., labels: _Optional[_Mapping[str, str]] = ..., capabilities: _Optional[_Iterable[str]] = ..., routing_status: _Optional[str] = ..., healthy: bool = ..., circuit_open: bool = ...) -> None: ...

class NativeServiceStatus(_message.Message):
    __slots__ = ("service_id", "proto_service_names", "enabled", "configured", "mounted", "healthy", "degraded", "surface", "listener_kind", "supported_rpcs", "capabilities", "required_backends", "missing_dependencies", "disabled_reason", "migration_status", "descriptor_version", "owns_background_workers", "background_worker_enabled", "background_workers")
    SERVICE_ID_FIELD_NUMBER: _ClassVar[int]
    PROTO_SERVICE_NAMES_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    CONFIGURED_FIELD_NUMBER: _ClassVar[int]
    MOUNTED_FIELD_NUMBER: _ClassVar[int]
    HEALTHY_FIELD_NUMBER: _ClassVar[int]
    DEGRADED_FIELD_NUMBER: _ClassVar[int]
    SURFACE_FIELD_NUMBER: _ClassVar[int]
    LISTENER_KIND_FIELD_NUMBER: _ClassVar[int]
    SUPPORTED_RPCS_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_BACKENDS_FIELD_NUMBER: _ClassVar[int]
    MISSING_DEPENDENCIES_FIELD_NUMBER: _ClassVar[int]
    DISABLED_REASON_FIELD_NUMBER: _ClassVar[int]
    MIGRATION_STATUS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTOR_VERSION_FIELD_NUMBER: _ClassVar[int]
    OWNS_BACKGROUND_WORKERS_FIELD_NUMBER: _ClassVar[int]
    BACKGROUND_WORKER_ENABLED_FIELD_NUMBER: _ClassVar[int]
    BACKGROUND_WORKERS_FIELD_NUMBER: _ClassVar[int]
    service_id: str
    proto_service_names: _containers.RepeatedScalarFieldContainer[str]
    enabled: bool
    configured: bool
    mounted: bool
    healthy: bool
    degraded: bool
    surface: str
    listener_kind: str
    supported_rpcs: _containers.RepeatedScalarFieldContainer[str]
    capabilities: _containers.RepeatedScalarFieldContainer[str]
    required_backends: _containers.RepeatedScalarFieldContainer[str]
    missing_dependencies: _containers.RepeatedScalarFieldContainer[str]
    disabled_reason: str
    migration_status: str
    descriptor_version: str
    owns_background_workers: bool
    background_worker_enabled: bool
    background_workers: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, service_id: _Optional[str] = ..., proto_service_names: _Optional[_Iterable[str]] = ..., enabled: bool = ..., configured: bool = ..., mounted: bool = ..., healthy: bool = ..., degraded: bool = ..., surface: _Optional[str] = ..., listener_kind: _Optional[str] = ..., supported_rpcs: _Optional[_Iterable[str]] = ..., capabilities: _Optional[_Iterable[str]] = ..., required_backends: _Optional[_Iterable[str]] = ..., missing_dependencies: _Optional[_Iterable[str]] = ..., disabled_reason: _Optional[str] = ..., migration_status: _Optional[str] = ..., descriptor_version: _Optional[str] = ..., owns_background_workers: bool = ..., background_worker_enabled: bool = ..., background_workers: _Optional[_Iterable[str]] = ...) -> None: ...

class CatalogManifestRequest(_message.Message):
    __slots__ = ("context", "redact")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    REDACT_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    redact: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., redact: bool = ...) -> None: ...

class CatalogManifestResponse(_message.Message):
    __slots__ = ("manifest_json",)
    MANIFEST_JSON_FIELD_NUMBER: _ClassVar[int]
    manifest_json: bytes
    def __init__(self, manifest_json: _Optional[bytes] = ...) -> None: ...

class MessageSchemaLookupRequest(_message.Message):
    __slots__ = ("context", "project_id", "message_type", "client_catalog_version")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    CLIENT_CATALOG_VERSION_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    project_id: str
    message_type: str
    client_catalog_version: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., project_id: _Optional[str] = ..., message_type: _Optional[str] = ..., client_catalog_version: _Optional[str] = ...) -> None: ...

class MessageFieldDescriptor(_message.Message):
    __slots__ = ("name", "column_name", "proto_type", "sql_type", "not_null", "is_primary", "is_array")
    NAME_FIELD_NUMBER: _ClassVar[int]
    COLUMN_NAME_FIELD_NUMBER: _ClassVar[int]
    PROTO_TYPE_FIELD_NUMBER: _ClassVar[int]
    SQL_TYPE_FIELD_NUMBER: _ClassVar[int]
    NOT_NULL_FIELD_NUMBER: _ClassVar[int]
    IS_PRIMARY_FIELD_NUMBER: _ClassVar[int]
    IS_ARRAY_FIELD_NUMBER: _ClassVar[int]
    name: str
    column_name: str
    proto_type: str
    sql_type: str
    not_null: bool
    is_primary: bool
    is_array: bool
    def __init__(self, name: _Optional[str] = ..., column_name: _Optional[str] = ..., proto_type: _Optional[str] = ..., sql_type: _Optional[str] = ..., not_null: bool = ..., is_primary: bool = ..., is_array: bool = ...) -> None: ...

class MessageSchemaDescriptor(_message.Message):
    __slots__ = ("message_type", "project_id", "catalog_version", "manifest_checksum", "schema", "table", "primary_key", "fields")
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_VERSION_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_KEY_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    message_type: str
    project_id: str
    catalog_version: str
    manifest_checksum: str
    schema: str
    table: str
    primary_key: _containers.RepeatedScalarFieldContainer[str]
    fields: _containers.RepeatedCompositeFieldContainer[MessageFieldDescriptor]
    def __init__(self, message_type: _Optional[str] = ..., project_id: _Optional[str] = ..., catalog_version: _Optional[str] = ..., manifest_checksum: _Optional[str] = ..., schema: _Optional[str] = ..., table: _Optional[str] = ..., primary_key: _Optional[_Iterable[str]] = ..., fields: _Optional[_Iterable[_Union[MessageFieldDescriptor, _Mapping]]] = ...) -> None: ...

class MessageSchemaLookupResponse(_message.Message):
    __slots__ = ("schema",)
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    schema: MessageSchemaDescriptor
    def __init__(self, schema: _Optional[_Union[MessageSchemaDescriptor, _Mapping]] = ...) -> None: ...

class MessageSchemaListRequest(_message.Message):
    __slots__ = ("context", "project_id", "client_catalog_version")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CLIENT_CATALOG_VERSION_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    project_id: str
    client_catalog_version: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., project_id: _Optional[str] = ..., client_catalog_version: _Optional[str] = ...) -> None: ...

class MessageSchemaListResponse(_message.Message):
    __slots__ = ("project_id", "catalog_version", "manifest_checksum", "message_types")
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_VERSION_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPES_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    catalog_version: str
    manifest_checksum: str
    message_types: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, project_id: _Optional[str] = ..., catalog_version: _Optional[str] = ..., manifest_checksum: _Optional[str] = ..., message_types: _Optional[_Iterable[str]] = ...) -> None: ...

class HealthReportRequest(_message.Message):
    __slots__ = ("context", "with_probes", "project_id")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    WITH_PROBES_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    with_probes: bool
    project_id: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., with_probes: bool = ..., project_id: _Optional[str] = ...) -> None: ...

class HealthReportResponse(_message.Message):
    __slots__ = ("passed", "postgres_configured", "redis_configured", "qdrant_configured", "s3_configured", "errors", "warnings", "privileges_json", "probes_json", "backend_instances", "native_services")
    PASSED_FIELD_NUMBER: _ClassVar[int]
    POSTGRES_CONFIGURED_FIELD_NUMBER: _ClassVar[int]
    REDIS_CONFIGURED_FIELD_NUMBER: _ClassVar[int]
    QDRANT_CONFIGURED_FIELD_NUMBER: _ClassVar[int]
    S3_CONFIGURED_FIELD_NUMBER: _ClassVar[int]
    ERRORS_FIELD_NUMBER: _ClassVar[int]
    WARNINGS_FIELD_NUMBER: _ClassVar[int]
    PRIVILEGES_JSON_FIELD_NUMBER: _ClassVar[int]
    PROBES_JSON_FIELD_NUMBER: _ClassVar[int]
    BACKEND_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    NATIVE_SERVICES_FIELD_NUMBER: _ClassVar[int]
    passed: bool
    postgres_configured: bool
    redis_configured: bool
    qdrant_configured: bool
    s3_configured: bool
    errors: _containers.RepeatedScalarFieldContainer[str]
    warnings: _containers.RepeatedScalarFieldContainer[str]
    privileges_json: bytes
    probes_json: bytes
    backend_instances: _containers.RepeatedCompositeFieldContainer[BackendInstanceStatus]
    native_services: _containers.RepeatedCompositeFieldContainer[NativeServiceStatus]
    def __init__(self, passed: bool = ..., postgres_configured: bool = ..., redis_configured: bool = ..., qdrant_configured: bool = ..., s3_configured: bool = ..., errors: _Optional[_Iterable[str]] = ..., warnings: _Optional[_Iterable[str]] = ..., privileges_json: _Optional[bytes] = ..., probes_json: _Optional[bytes] = ..., backend_instances: _Optional[_Iterable[_Union[BackendInstanceStatus, _Mapping]]] = ..., native_services: _Optional[_Iterable[_Union[NativeServiceStatus, _Mapping]]] = ...) -> None: ...

class GenericDispatchRequest(_message.Message):
    __slots__ = ("context", "backend", "operation", "resource_kind", "resource_name", "resource_uri", "spec_json", "idempotency_key", "dry_run")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_KIND_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_URI_FIELD_NUMBER: _ClassVar[int]
    SPEC_JSON_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    DRY_RUN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    backend: str
    operation: str
    resource_kind: str
    resource_name: str
    resource_uri: str
    spec_json: str
    idempotency_key: str
    dry_run: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., backend: _Optional[str] = ..., operation: _Optional[str] = ..., resource_kind: _Optional[str] = ..., resource_name: _Optional[str] = ..., resource_uri: _Optional[str] = ..., spec_json: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., dry_run: bool = ...) -> None: ...

class GenericDispatchResponse(_message.Message):
    __slots__ = ("backend", "operation", "resource_uri", "result_json", "errors")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_URI_FIELD_NUMBER: _ClassVar[int]
    RESULT_JSON_FIELD_NUMBER: _ClassVar[int]
    ERRORS_FIELD_NUMBER: _ClassVar[int]
    backend: str
    operation: str
    resource_uri: str
    result_json: str
    errors: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, backend: _Optional[str] = ..., operation: _Optional[str] = ..., resource_uri: _Optional[str] = ..., result_json: _Optional[str] = ..., errors: _Optional[_Iterable[str]] = ...) -> None: ...

class ResourceAdminRequest(_message.Message):
    __slots__ = ("context", "backend", "resource_name", "spec_json", "idempotency_key", "dry_run")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    SPEC_JSON_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    DRY_RUN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    backend: str
    resource_name: str
    spec_json: str
    idempotency_key: str
    dry_run: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., backend: _Optional[str] = ..., resource_name: _Optional[str] = ..., spec_json: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., dry_run: bool = ...) -> None: ...

class ResourceListResponse(_message.Message):
    __slots__ = ("backend", "resources")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    backend: str
    resources: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, backend: _Optional[str] = ..., resources: _Optional[_Iterable[str]] = ...) -> None: ...

class StageCatalogRequest(_message.Message):
    __slots__ = ("context", "manifest_json", "project_id", "reason", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_JSON_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    manifest_json: bytes
    project_id: str
    reason: str
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., manifest_json: _Optional[bytes] = ..., project_id: _Optional[str] = ..., reason: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class CatalogVersionRequest(_message.Message):
    __slots__ = ("context", "project_id", "version", "reason", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    project_id: str
    version: str
    reason: str
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., project_id: _Optional[str] = ..., version: _Optional[str] = ..., reason: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class CatalogVersionResponse(_message.Message):
    __slots__ = ("catalog_id", "project_id", "version", "status", "checksum_sha256", "created_at_unix", "errors", "warnings")
    CATALOG_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_SHA256_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    ERRORS_FIELD_NUMBER: _ClassVar[int]
    WARNINGS_FIELD_NUMBER: _ClassVar[int]
    catalog_id: str
    project_id: str
    version: str
    status: str
    checksum_sha256: str
    created_at_unix: int
    errors: _containers.RepeatedScalarFieldContainer[str]
    warnings: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, catalog_id: _Optional[str] = ..., project_id: _Optional[str] = ..., version: _Optional[str] = ..., status: _Optional[str] = ..., checksum_sha256: _Optional[str] = ..., created_at_unix: _Optional[int] = ..., errors: _Optional[_Iterable[str]] = ..., warnings: _Optional[_Iterable[str]] = ...) -> None: ...

class CatalogValidationResponse(_message.Message):
    __slots__ = ("valid", "checksum_sha256", "errors", "warnings")
    VALID_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_SHA256_FIELD_NUMBER: _ClassVar[int]
    ERRORS_FIELD_NUMBER: _ClassVar[int]
    WARNINGS_FIELD_NUMBER: _ClassVar[int]
    valid: bool
    checksum_sha256: str
    errors: _containers.RepeatedScalarFieldContainer[str]
    warnings: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, valid: bool = ..., checksum_sha256: _Optional[str] = ..., errors: _Optional[_Iterable[str]] = ..., warnings: _Optional[_Iterable[str]] = ...) -> None: ...

class CatalogVersionListResponse(_message.Message):
    __slots__ = ("project_id", "versions", "active_version")
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_VERSION_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    versions: _containers.RepeatedCompositeFieldContainer[CatalogVersionResponse]
    active_version: str
    def __init__(self, project_id: _Optional[str] = ..., versions: _Optional[_Iterable[_Union[CatalogVersionResponse, _Mapping]]] = ..., active_version: _Optional[str] = ...) -> None: ...

class MigrationPlanRequest(_message.Message):
    __slots__ = ("context", "project_id", "dry_run")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DRY_RUN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    project_id: str
    dry_run: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., project_id: _Optional[str] = ..., dry_run: bool = ...) -> None: ...

class MigrationPlanResponse(_message.Message):
    __slots__ = ("run_id", "project_id", "catalog_version", "state", "operations", "requires_review", "blocked", "operations_hash")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_VERSION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    OPERATIONS_FIELD_NUMBER: _ClassVar[int]
    REQUIRES_REVIEW_FIELD_NUMBER: _ClassVar[int]
    BLOCKED_FIELD_NUMBER: _ClassVar[int]
    OPERATIONS_HASH_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    project_id: str
    catalog_version: str
    state: str
    operations: _containers.RepeatedScalarFieldContainer[str]
    requires_review: _containers.RepeatedScalarFieldContainer[str]
    blocked: _containers.RepeatedScalarFieldContainer[str]
    operations_hash: str
    def __init__(self, run_id: _Optional[str] = ..., project_id: _Optional[str] = ..., catalog_version: _Optional[str] = ..., state: _Optional[str] = ..., operations: _Optional[_Iterable[str]] = ..., requires_review: _Optional[_Iterable[str]] = ..., blocked: _Optional[_Iterable[str]] = ..., operations_hash: _Optional[str] = ...) -> None: ...

class MigrationApplyRequest(_message.Message):
    __slots__ = ("context", "run_id", "project_id", "approval_token", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    APPROVAL_TOKEN_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    run_id: str
    project_id: str
    approval_token: str
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., run_id: _Optional[str] = ..., project_id: _Optional[str] = ..., approval_token: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class MigrationRunRequest(_message.Message):
    __slots__ = ("context", "run_id", "project_id", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    run_id: str
    project_id: str
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., run_id: _Optional[str] = ..., project_id: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class MigrationRunListRequest(_message.Message):
    __slots__ = ("context", "project_id", "state_filter", "limit", "page_token")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FILTER_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    project_id: str
    state_filter: str
    limit: int
    page_token: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., project_id: _Optional[str] = ..., state_filter: _Optional[str] = ..., limit: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class MigrationRunListResponse(_message.Message):
    __slots__ = ("runs", "next_page_token", "total_count")
    RUNS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    runs: _containers.RepeatedCompositeFieldContainer[MigrationStatusResponse]
    next_page_token: str
    total_count: int
    def __init__(self, runs: _Optional[_Iterable[_Union[MigrationStatusResponse, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., total_count: _Optional[int] = ...) -> None: ...

class MigrationStatusResponse(_message.Message):
    __slots__ = ("run_id", "project_id", "catalog_version", "state", "started_at", "finished_at", "operations", "error")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_VERSION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    FINISHED_AT_FIELD_NUMBER: _ClassVar[int]
    OPERATIONS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    project_id: str
    catalog_version: str
    state: str
    started_at: str
    finished_at: str
    operations: _containers.RepeatedCompositeFieldContainer[MigrationOperationStatus]
    error: str
    def __init__(self, run_id: _Optional[str] = ..., project_id: _Optional[str] = ..., catalog_version: _Optional[str] = ..., state: _Optional[str] = ..., started_at: _Optional[str] = ..., finished_at: _Optional[str] = ..., operations: _Optional[_Iterable[_Union[MigrationOperationStatus, _Mapping]]] = ..., error: _Optional[str] = ...) -> None: ...

class MigrationOperationStatus(_message.Message):
    __slots__ = ("index", "backend", "resource_uri", "operation_kind", "status", "error")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_URI_FIELD_NUMBER: _ClassVar[int]
    OPERATION_KIND_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    index: int
    backend: str
    resource_uri: str
    operation_kind: str
    status: str
    error: str
    def __init__(self, index: _Optional[int] = ..., backend: _Optional[str] = ..., resource_uri: _Optional[str] = ..., operation_kind: _Optional[str] = ..., status: _Optional[str] = ..., error: _Optional[str] = ...) -> None: ...

class DlqListRequest(_message.Message):
    __slots__ = ("context", "topic", "status_filter", "limit", "page_token")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    STATUS_FILTER_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    topic: str
    status_filter: str
    limit: int
    page_token: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., topic: _Optional[str] = ..., status_filter: _Optional[str] = ..., limit: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class DlqEventRecord(_message.Message):
    __slots__ = ("dlq_id", "event_id", "topic", "payload_json", "error_type", "error_message", "status", "created_at_unix", "updated_at_unix")
    DLQ_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    dlq_id: str
    event_id: str
    topic: str
    payload_json: bytes
    error_type: str
    error_message: str
    status: str
    created_at_unix: int
    updated_at_unix: int
    def __init__(self, dlq_id: _Optional[str] = ..., event_id: _Optional[str] = ..., topic: _Optional[str] = ..., payload_json: _Optional[bytes] = ..., error_type: _Optional[str] = ..., error_message: _Optional[str] = ..., status: _Optional[str] = ..., created_at_unix: _Optional[int] = ..., updated_at_unix: _Optional[int] = ...) -> None: ...

class DlqListResponse(_message.Message):
    __slots__ = ("events", "next_page_token", "total_count")
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    events: _containers.RepeatedCompositeFieldContainer[DlqEventRecord]
    next_page_token: str
    total_count: int
    def __init__(self, events: _Optional[_Iterable[_Union[DlqEventRecord, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., total_count: _Optional[int] = ...) -> None: ...

class DlqEventRequest(_message.Message):
    __slots__ = ("context", "dlq_id")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    DLQ_ID_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    dlq_id: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., dlq_id: _Optional[str] = ...) -> None: ...

class DlqEventResponse(_message.Message):
    __slots__ = ("event",)
    EVENT_FIELD_NUMBER: _ClassVar[int]
    event: DlqEventRecord
    def __init__(self, event: _Optional[_Union[DlqEventRecord, _Mapping]] = ...) -> None: ...

class DlqActionRequest(_message.Message):
    __slots__ = ("context", "dlq_id", "preserve_event_id", "reason")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    DLQ_ID_FIELD_NUMBER: _ClassVar[int]
    PRESERVE_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    dlq_id: str
    preserve_event_id: bool
    reason: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., dlq_id: _Optional[str] = ..., preserve_event_id: bool = ..., reason: _Optional[str] = ...) -> None: ...

class CdcRedactionPreviewRequest(_message.Message):
    __slots__ = ("context", "message_type", "topic", "schema_uri", "payload_json", "redaction_mode", "redaction_version")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_URI_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    REDACTION_MODE_FIELD_NUMBER: _ClassVar[int]
    REDACTION_VERSION_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    message_type: str
    topic: str
    schema_uri: str
    payload_json: bytes
    redaction_mode: str
    redaction_version: int
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., message_type: _Optional[str] = ..., topic: _Optional[str] = ..., schema_uri: _Optional[str] = ..., payload_json: _Optional[bytes] = ..., redaction_mode: _Optional[str] = ..., redaction_version: _Optional[int] = ...) -> None: ...

class CdcRedactionPreviewResponse(_message.Message):
    __slots__ = ("payload_json", "redacted_fields", "redaction_mode", "redaction_version", "would_redact")
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    REDACTED_FIELDS_FIELD_NUMBER: _ClassVar[int]
    REDACTION_MODE_FIELD_NUMBER: _ClassVar[int]
    REDACTION_VERSION_FIELD_NUMBER: _ClassVar[int]
    WOULD_REDACT_FIELD_NUMBER: _ClassVar[int]
    payload_json: bytes
    redacted_fields: _containers.RepeatedScalarFieldContainer[str]
    redaction_mode: str
    redaction_version: int
    would_redact: bool
    def __init__(self, payload_json: _Optional[bytes] = ..., redacted_fields: _Optional[_Iterable[str]] = ..., redaction_mode: _Optional[str] = ..., redaction_version: _Optional[int] = ..., would_redact: bool = ...) -> None: ...

class ProjectionDriftScanRequest(_message.Message):
    __slots__ = ("context", "project_id", "message_type", "scan_mode", "rows_per_target", "repair", "limit")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SCAN_MODE_FIELD_NUMBER: _ClassVar[int]
    ROWS_PER_TARGET_FIELD_NUMBER: _ClassVar[int]
    REPAIR_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    project_id: str
    message_type: str
    scan_mode: str
    rows_per_target: int
    repair: bool
    limit: int
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., project_id: _Optional[str] = ..., message_type: _Optional[str] = ..., scan_mode: _Optional[str] = ..., rows_per_target: _Optional[int] = ..., repair: bool = ..., limit: _Optional[int] = ...) -> None: ...

class ProjectionDriftDivergentRow(_message.Message):
    __slots__ = ("row_key_json", "source_checksum", "target_checksum", "kind")
    ROW_KEY_JSON_FIELD_NUMBER: _ClassVar[int]
    SOURCE_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    TARGET_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    row_key_json: bytes
    source_checksum: str
    target_checksum: str
    kind: str
    def __init__(self, row_key_json: _Optional[bytes] = ..., source_checksum: _Optional[str] = ..., target_checksum: _Optional[str] = ..., kind: _Optional[str] = ...) -> None: ...

class ProjectionDriftTargetReport(_message.Message):
    __slots__ = ("target_backend", "target_instance", "target_resource", "source_rows_scanned", "divergent_rows", "rows_to_repair", "estimated_cost_units", "repair_tasks_enqueued", "warnings")
    TARGET_BACKEND_FIELD_NUMBER: _ClassVar[int]
    TARGET_INSTANCE_FIELD_NUMBER: _ClassVar[int]
    TARGET_RESOURCE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ROWS_SCANNED_FIELD_NUMBER: _ClassVar[int]
    DIVERGENT_ROWS_FIELD_NUMBER: _ClassVar[int]
    ROWS_TO_REPAIR_FIELD_NUMBER: _ClassVar[int]
    ESTIMATED_COST_UNITS_FIELD_NUMBER: _ClassVar[int]
    REPAIR_TASKS_ENQUEUED_FIELD_NUMBER: _ClassVar[int]
    WARNINGS_FIELD_NUMBER: _ClassVar[int]
    target_backend: str
    target_instance: str
    target_resource: str
    source_rows_scanned: int
    divergent_rows: _containers.RepeatedCompositeFieldContainer[ProjectionDriftDivergentRow]
    rows_to_repair: int
    estimated_cost_units: float
    repair_tasks_enqueued: int
    warnings: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, target_backend: _Optional[str] = ..., target_instance: _Optional[str] = ..., target_resource: _Optional[str] = ..., source_rows_scanned: _Optional[int] = ..., divergent_rows: _Optional[_Iterable[_Union[ProjectionDriftDivergentRow, _Mapping]]] = ..., rows_to_repair: _Optional[int] = ..., estimated_cost_units: _Optional[float] = ..., repair_tasks_enqueued: _Optional[int] = ..., warnings: _Optional[_Iterable[str]] = ...) -> None: ...

class ProjectionDriftScanResponse(_message.Message):
    __slots__ = ("project_id", "message_type", "scan_mode", "source_rows_loaded", "reports", "summary_json", "warnings")
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SCAN_MODE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ROWS_LOADED_FIELD_NUMBER: _ClassVar[int]
    REPORTS_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_JSON_FIELD_NUMBER: _ClassVar[int]
    WARNINGS_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    message_type: str
    scan_mode: str
    source_rows_loaded: int
    reports: _containers.RepeatedCompositeFieldContainer[ProjectionDriftTargetReport]
    summary_json: bytes
    warnings: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, project_id: _Optional[str] = ..., message_type: _Optional[str] = ..., scan_mode: _Optional[str] = ..., source_rows_loaded: _Optional[int] = ..., reports: _Optional[_Iterable[_Union[ProjectionDriftTargetReport, _Mapping]]] = ..., summary_json: _Optional[bytes] = ..., warnings: _Optional[_Iterable[str]] = ...) -> None: ...

class SagaListRequest(_message.Message):
    __slots__ = ("context", "tenant_id_filter", "status_filter", "tx_id_filter", "correlation_id_filter", "limit", "page_token")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FILTER_FIELD_NUMBER: _ClassVar[int]
    STATUS_FILTER_FIELD_NUMBER: _ClassVar[int]
    TX_ID_FILTER_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FILTER_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    tenant_id_filter: str
    status_filter: str
    tx_id_filter: str
    correlation_id_filter: str
    limit: int
    page_token: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., tenant_id_filter: _Optional[str] = ..., status_filter: _Optional[str] = ..., tx_id_filter: _Optional[str] = ..., correlation_id_filter: _Optional[str] = ..., limit: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class SagaRecord(_message.Message):
    __slots__ = ("saga_id", "tx_id", "tenant_id", "correlation_id", "status", "current_step", "steps_json", "compensations_json", "last_error", "created_at_unix", "updated_at_unix")
    SAGA_ID_FIELD_NUMBER: _ClassVar[int]
    TX_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CURRENT_STEP_FIELD_NUMBER: _ClassVar[int]
    STEPS_JSON_FIELD_NUMBER: _ClassVar[int]
    COMPENSATIONS_JSON_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    saga_id: str
    tx_id: str
    tenant_id: str
    correlation_id: str
    status: str
    current_step: int
    steps_json: bytes
    compensations_json: bytes
    last_error: str
    created_at_unix: int
    updated_at_unix: int
    def __init__(self, saga_id: _Optional[str] = ..., tx_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., status: _Optional[str] = ..., current_step: _Optional[int] = ..., steps_json: _Optional[bytes] = ..., compensations_json: _Optional[bytes] = ..., last_error: _Optional[str] = ..., created_at_unix: _Optional[int] = ..., updated_at_unix: _Optional[int] = ...) -> None: ...

class SagaListResponse(_message.Message):
    __slots__ = ("sagas", "next_page_token", "total_count")
    SAGAS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    sagas: _containers.RepeatedCompositeFieldContainer[SagaRecord]
    next_page_token: str
    total_count: int
    def __init__(self, sagas: _Optional[_Iterable[_Union[SagaRecord, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., total_count: _Optional[int] = ...) -> None: ...

class SagaRequest(_message.Message):
    __slots__ = ("context", "saga_id", "reason", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    SAGA_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    saga_id: str
    reason: str
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., saga_id: _Optional[str] = ..., reason: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class SagaResponse(_message.Message):
    __slots__ = ("saga", "errors")
    SAGA_FIELD_NUMBER: _ClassVar[int]
    ERRORS_FIELD_NUMBER: _ClassVar[int]
    saga: SagaRecord
    errors: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, saga: _Optional[_Union[SagaRecord, _Mapping]] = ..., errors: _Optional[_Iterable[str]] = ...) -> None: ...

class PolicyRecord(_message.Message):
    __slots__ = ("policy_id", "effect", "service_identity", "tenant_id", "purpose", "message_type", "operation", "required_scope", "priority", "enabled")
    POLICY_ID_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    SERVICE_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PURPOSE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_SCOPE_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    policy_id: int
    effect: str
    service_identity: str
    tenant_id: str
    purpose: str
    message_type: str
    operation: str
    required_scope: str
    priority: int
    enabled: bool
    def __init__(self, policy_id: _Optional[int] = ..., effect: _Optional[str] = ..., service_identity: _Optional[str] = ..., tenant_id: _Optional[str] = ..., purpose: _Optional[str] = ..., message_type: _Optional[str] = ..., operation: _Optional[str] = ..., required_scope: _Optional[str] = ..., priority: _Optional[int] = ..., enabled: bool = ...) -> None: ...

class PolicyListRequest(_message.Message):
    __slots__ = ("context", "include_disabled", "limit", "page_token")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_DISABLED_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    include_disabled: bool
    limit: int
    page_token: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., include_disabled: bool = ..., limit: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class PolicyListResponse(_message.Message):
    __slots__ = ("policies", "next_page_token", "total_count")
    POLICIES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    policies: _containers.RepeatedCompositeFieldContainer[PolicyRecord]
    next_page_token: str
    total_count: int
    def __init__(self, policies: _Optional[_Iterable[_Union[PolicyRecord, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., total_count: _Optional[int] = ...) -> None: ...

class PutPolicyRequest(_message.Message):
    __slots__ = ("context", "policy")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    policy: PolicyRecord
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., policy: _Optional[_Union[PolicyRecord, _Mapping]] = ...) -> None: ...

class PolicyRequest(_message.Message):
    __slots__ = ("context", "policy_id")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    POLICY_ID_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    policy_id: int
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., policy_id: _Optional[int] = ...) -> None: ...

class PolicyLintResponse(_message.Message):
    __slots__ = ("passed", "findings")
    PASSED_FIELD_NUMBER: _ClassVar[int]
    FINDINGS_FIELD_NUMBER: _ClassVar[int]
    passed: bool
    findings: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, passed: bool = ..., findings: _Optional[_Iterable[str]] = ...) -> None: ...

class EnsureProjectRequest(_message.Message):
    __slots__ = ("context", "project_id", "name", "cdc_topic_prefix")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CDC_TOPIC_PREFIX_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    project_id: str
    name: str
    cdc_topic_prefix: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., project_id: _Optional[str] = ..., name: _Optional[str] = ..., cdc_topic_prefix: _Optional[str] = ...) -> None: ...

class ProjectRecord(_message.Message):
    __slots__ = ("project_id", "name", "cdc_topic_prefix", "active_catalog_version", "created_at_unix")
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CDC_TOPIC_PREFIX_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_CATALOG_VERSION_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    name: str
    cdc_topic_prefix: str
    active_catalog_version: str
    created_at_unix: int
    def __init__(self, project_id: _Optional[str] = ..., name: _Optional[str] = ..., cdc_topic_prefix: _Optional[str] = ..., active_catalog_version: _Optional[str] = ..., created_at_unix: _Optional[int] = ...) -> None: ...

class ProjectListRequest(_message.Message):
    __slots__ = ("context", "limit", "page_token")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    limit: int
    page_token: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., limit: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ProjectListResponse(_message.Message):
    __slots__ = ("projects", "next_page_token", "total_count")
    PROJECTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    projects: _containers.RepeatedCompositeFieldContainer[ProjectRecord]
    next_page_token: str
    total_count: int
    def __init__(self, projects: _Optional[_Iterable[_Union[ProjectRecord, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., total_count: _Optional[int] = ...) -> None: ...

class AdminSummaryRequest(_message.Message):
    __slots__ = ("context", "project_id", "with_probes", "redact")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    WITH_PROBES_FIELD_NUMBER: _ClassVar[int]
    REDACT_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    project_id: str
    with_probes: bool
    redact: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., project_id: _Optional[str] = ..., with_probes: bool = ..., redact: bool = ...) -> None: ...

class AdminAuditLogRequest(_message.Message):
    __slots__ = ("context", "operation_filter", "actor_filter", "tenant_id_filter", "project_id_filter", "limit", "page_token", "redact")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FILTER_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FILTER_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FILTER_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FILTER_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    REDACT_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    operation_filter: str
    actor_filter: str
    tenant_id_filter: str
    project_id_filter: str
    limit: int
    page_token: str
    redact: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., operation_filter: _Optional[str] = ..., actor_filter: _Optional[str] = ..., tenant_id_filter: _Optional[str] = ..., project_id_filter: _Optional[str] = ..., limit: _Optional[int] = ..., page_token: _Optional[str] = ..., redact: bool = ...) -> None: ...

class AdminAuditLogRecord(_message.Message):
    __slots__ = ("audit_id", "actor", "operation", "target", "request_json", "result", "tenant_id", "project_id", "correlation_id", "created_at_unix", "previous_hash", "current_hash", "signer_key_id", "external_anchor")
    AUDIT_ID_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    REQUEST_JSON_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_HASH_FIELD_NUMBER: _ClassVar[int]
    CURRENT_HASH_FIELD_NUMBER: _ClassVar[int]
    SIGNER_KEY_ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_ANCHOR_FIELD_NUMBER: _ClassVar[int]
    audit_id: str
    actor: str
    operation: str
    target: str
    request_json: bytes
    result: str
    tenant_id: str
    project_id: str
    correlation_id: str
    created_at_unix: int
    previous_hash: str
    current_hash: str
    signer_key_id: str
    external_anchor: str
    def __init__(self, audit_id: _Optional[str] = ..., actor: _Optional[str] = ..., operation: _Optional[str] = ..., target: _Optional[str] = ..., request_json: _Optional[bytes] = ..., result: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., created_at_unix: _Optional[int] = ..., previous_hash: _Optional[str] = ..., current_hash: _Optional[str] = ..., signer_key_id: _Optional[str] = ..., external_anchor: _Optional[str] = ...) -> None: ...

class AdminAuditLogResponse(_message.Message):
    __slots__ = ("logs", "next_page_token", "total_count")
    LOGS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    logs: _containers.RepeatedCompositeFieldContainer[AdminAuditLogRecord]
    next_page_token: str
    total_count: int
    def __init__(self, logs: _Optional[_Iterable[_Union[AdminAuditLogRecord, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., total_count: _Optional[int] = ...) -> None: ...

class AdminAuditVerifyRequest(_message.Message):
    __slots__ = ("context", "limit")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    limit: int
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., limit: _Optional[int] = ...) -> None: ...

class AdminAuditVerifyResponse(_message.Message):
    __slots__ = ("passed", "checked_count", "first_broken_audit_id", "reason", "expected_previous_hash", "actual_previous_hash", "expected_current_hash", "actual_current_hash", "last_hash")
    PASSED_FIELD_NUMBER: _ClassVar[int]
    CHECKED_COUNT_FIELD_NUMBER: _ClassVar[int]
    FIRST_BROKEN_AUDIT_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_PREVIOUS_HASH_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_PREVIOUS_HASH_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_CURRENT_HASH_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_CURRENT_HASH_FIELD_NUMBER: _ClassVar[int]
    LAST_HASH_FIELD_NUMBER: _ClassVar[int]
    passed: bool
    checked_count: int
    first_broken_audit_id: str
    reason: str
    expected_previous_hash: str
    actual_previous_hash: str
    expected_current_hash: str
    actual_current_hash: str
    last_hash: str
    def __init__(self, passed: bool = ..., checked_count: _Optional[int] = ..., first_broken_audit_id: _Optional[str] = ..., reason: _Optional[str] = ..., expected_previous_hash: _Optional[str] = ..., actual_previous_hash: _Optional[str] = ..., expected_current_hash: _Optional[str] = ..., actual_current_hash: _Optional[str] = ..., last_hash: _Optional[str] = ...) -> None: ...

class AdminBackendSummary(_message.Message):
    __slots__ = ("backend", "status", "transport", "consistency_model", "supports_transactions", "supports_schema_migration", "supports_vector_search", "supports_hybrid_search", "max_payload_bytes", "probe_ok", "probe_latency_ms", "instance_name", "role", "labels", "routing_status")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TRANSPORT_FIELD_NUMBER: _ClassVar[int]
    CONSISTENCY_MODEL_FIELD_NUMBER: _ClassVar[int]
    SUPPORTS_TRANSACTIONS_FIELD_NUMBER: _ClassVar[int]
    SUPPORTS_SCHEMA_MIGRATION_FIELD_NUMBER: _ClassVar[int]
    SUPPORTS_VECTOR_SEARCH_FIELD_NUMBER: _ClassVar[int]
    SUPPORTS_HYBRID_SEARCH_FIELD_NUMBER: _ClassVar[int]
    MAX_PAYLOAD_BYTES_FIELD_NUMBER: _ClassVar[int]
    PROBE_OK_FIELD_NUMBER: _ClassVar[int]
    PROBE_LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_NAME_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    ROUTING_STATUS_FIELD_NUMBER: _ClassVar[int]
    backend: str
    status: str
    transport: str
    consistency_model: str
    supports_transactions: bool
    supports_schema_migration: bool
    supports_vector_search: bool
    supports_hybrid_search: bool
    max_payload_bytes: int
    probe_ok: bool
    probe_latency_ms: int
    instance_name: str
    role: str
    labels: _containers.ScalarMap[str, str]
    routing_status: str
    def __init__(self, backend: _Optional[str] = ..., status: _Optional[str] = ..., transport: _Optional[str] = ..., consistency_model: _Optional[str] = ..., supports_transactions: bool = ..., supports_schema_migration: bool = ..., supports_vector_search: bool = ..., supports_hybrid_search: bool = ..., max_payload_bytes: _Optional[int] = ..., probe_ok: bool = ..., probe_latency_ms: _Optional[int] = ..., instance_name: _Optional[str] = ..., role: _Optional[str] = ..., labels: _Optional[_Mapping[str, str]] = ..., routing_status: _Optional[str] = ...) -> None: ...

class AdminCdcSummary(_message.Message):
    __slots__ = ("is_leader", "paused", "slot_name", "last_event_id", "lag_seconds", "outbox_depth", "dlq_open_count")
    IS_LEADER_FIELD_NUMBER: _ClassVar[int]
    PAUSED_FIELD_NUMBER: _ClassVar[int]
    SLOT_NAME_FIELD_NUMBER: _ClassVar[int]
    LAST_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    LAG_SECONDS_FIELD_NUMBER: _ClassVar[int]
    OUTBOX_DEPTH_FIELD_NUMBER: _ClassVar[int]
    DLQ_OPEN_COUNT_FIELD_NUMBER: _ClassVar[int]
    is_leader: bool
    paused: bool
    slot_name: str
    last_event_id: str
    lag_seconds: float
    outbox_depth: int
    dlq_open_count: int
    def __init__(self, is_leader: bool = ..., paused: bool = ..., slot_name: _Optional[str] = ..., last_event_id: _Optional[str] = ..., lag_seconds: _Optional[float] = ..., outbox_depth: _Optional[int] = ..., dlq_open_count: _Optional[int] = ...) -> None: ...

class AdminSagaSummary(_message.Message):
    __slots__ = ("active", "compensated", "failed_compensation", "manual_review", "indeterminate")
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    COMPENSATED_FIELD_NUMBER: _ClassVar[int]
    FAILED_COMPENSATION_FIELD_NUMBER: _ClassVar[int]
    MANUAL_REVIEW_FIELD_NUMBER: _ClassVar[int]
    INDETERMINATE_FIELD_NUMBER: _ClassVar[int]
    active: int
    compensated: int
    failed_compensation: int
    manual_review: int
    indeterminate: int
    def __init__(self, active: _Optional[int] = ..., compensated: _Optional[int] = ..., failed_compensation: _Optional[int] = ..., manual_review: _Optional[int] = ..., indeterminate: _Optional[int] = ...) -> None: ...

class AdminCatalogSummary(_message.Message):
    __slots__ = ("project_id", "active_version", "active_checksum", "active_since", "table_count", "store_count", "pending_migration_state")
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_VERSION_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_SINCE_FIELD_NUMBER: _ClassVar[int]
    TABLE_COUNT_FIELD_NUMBER: _ClassVar[int]
    STORE_COUNT_FIELD_NUMBER: _ClassVar[int]
    PENDING_MIGRATION_STATE_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    active_version: str
    active_checksum: str
    active_since: str
    table_count: int
    store_count: int
    pending_migration_state: str
    def __init__(self, project_id: _Optional[str] = ..., active_version: _Optional[str] = ..., active_checksum: _Optional[str] = ..., active_since: _Optional[str] = ..., table_count: _Optional[int] = ..., store_count: _Optional[int] = ..., pending_migration_state: _Optional[str] = ...) -> None: ...

class AdminSummaryResponse(_message.Message):
    __slots__ = ("catalog", "cdc", "sagas", "backends", "active_policy_count", "snapshot_at_unix_ms", "warnings")
    CATALOG_FIELD_NUMBER: _ClassVar[int]
    CDC_FIELD_NUMBER: _ClassVar[int]
    SAGAS_FIELD_NUMBER: _ClassVar[int]
    BACKENDS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_POLICY_COUNT_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_AT_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    WARNINGS_FIELD_NUMBER: _ClassVar[int]
    catalog: _containers.RepeatedCompositeFieldContainer[AdminCatalogSummary]
    cdc: AdminCdcSummary
    sagas: AdminSagaSummary
    backends: _containers.RepeatedCompositeFieldContainer[AdminBackendSummary]
    active_policy_count: int
    snapshot_at_unix_ms: int
    warnings: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, catalog: _Optional[_Iterable[_Union[AdminCatalogSummary, _Mapping]]] = ..., cdc: _Optional[_Union[AdminCdcSummary, _Mapping]] = ..., sagas: _Optional[_Union[AdminSagaSummary, _Mapping]] = ..., backends: _Optional[_Iterable[_Union[AdminBackendSummary, _Mapping]]] = ..., active_policy_count: _Optional[int] = ..., snapshot_at_unix_ms: _Optional[int] = ..., warnings: _Optional[_Iterable[str]] = ...) -> None: ...
