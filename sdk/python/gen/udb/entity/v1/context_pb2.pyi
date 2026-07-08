from udb.entity.v1 import consistency_pb2 as _consistency_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RequestContext(_message.Message):
    __slots__ = ("tenant_id", "user_id", "correlation_id", "purpose", "scopes", "service_identity", "trace_id", "target_backend", "target_instance", "routing_policy", "primary_read", "max_replica_lag_ms", "eventual_consistency_allowed", "read_fence_json", "project_id", "client_catalog_version", "client_id", "consistency", "region", "attributes", "read_fence", "consistency_mode")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    PURPOSE_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    SERVICE_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_BACKEND_FIELD_NUMBER: _ClassVar[int]
    TARGET_INSTANCE_FIELD_NUMBER: _ClassVar[int]
    ROUTING_POLICY_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_READ_FIELD_NUMBER: _ClassVar[int]
    MAX_REPLICA_LAG_MS_FIELD_NUMBER: _ClassVar[int]
    EVENTUAL_CONSISTENCY_ALLOWED_FIELD_NUMBER: _ClassVar[int]
    READ_FENCE_JSON_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CLIENT_CATALOG_VERSION_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    CONSISTENCY_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    READ_FENCE_FIELD_NUMBER: _ClassVar[int]
    CONSISTENCY_MODE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    user_id: str
    correlation_id: str
    purpose: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    service_identity: str
    trace_id: str
    target_backend: str
    target_instance: str
    routing_policy: str
    primary_read: bool
    max_replica_lag_ms: int
    eventual_consistency_allowed: bool
    read_fence_json: str
    project_id: str
    client_catalog_version: str
    client_id: str
    consistency: str
    region: str
    attributes: _containers.ScalarMap[str, str]
    read_fence: _consistency_pb2.ReadFence
    consistency_mode: _consistency_pb2.ConsistencyMode
    def __init__(self, tenant_id: _Optional[str] = ..., user_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., purpose: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., service_identity: _Optional[str] = ..., trace_id: _Optional[str] = ..., target_backend: _Optional[str] = ..., target_instance: _Optional[str] = ..., routing_policy: _Optional[str] = ..., primary_read: bool = ..., max_replica_lag_ms: _Optional[int] = ..., eventual_consistency_allowed: bool = ..., read_fence_json: _Optional[str] = ..., project_id: _Optional[str] = ..., client_catalog_version: _Optional[str] = ..., client_id: _Optional[str] = ..., consistency: _Optional[str] = ..., region: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ..., read_fence: _Optional[_Union[_consistency_pb2.ReadFence, _Mapping]] = ..., consistency_mode: _Optional[_Union[_consistency_pb2.ConsistencyMode, str]] = ...) -> None: ...
