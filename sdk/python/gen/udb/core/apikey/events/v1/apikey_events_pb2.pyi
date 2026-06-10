import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.apikey.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ApiKeyCreatedEvent(_message.Message):
    __slots__ = ("event_id", "key_id", "key_prefix", "name", "owner_type", "owner_id", "scopes", "created_by", "correlation_id", "occurred_at", "tenant_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_PREFIX_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_TYPE_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    key_id: str
    key_prefix: str
    name: str
    owner_type: _enums_pb2.ApiKeyOwnerType
    owner_id: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    created_by: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, event_id: _Optional[str] = ..., key_id: _Optional[str] = ..., key_prefix: _Optional[str] = ..., name: _Optional[str] = ..., owner_type: _Optional[_Union[_enums_pb2.ApiKeyOwnerType, str]] = ..., owner_id: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., created_by: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class ApiKeyRevokedEvent(_message.Message):
    __slots__ = ("event_id", "key_id", "key_prefix", "revoked_by", "revoke_reason", "correlation_id", "occurred_at", "tenant_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_PREFIX_FIELD_NUMBER: _ClassVar[int]
    REVOKED_BY_FIELD_NUMBER: _ClassVar[int]
    REVOKE_REASON_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    key_id: str
    key_prefix: str
    revoked_by: str
    revoke_reason: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, event_id: _Optional[str] = ..., key_id: _Optional[str] = ..., key_prefix: _Optional[str] = ..., revoked_by: _Optional[str] = ..., revoke_reason: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class ApiKeyRateLimitedEvent(_message.Message):
    __slots__ = ("event_id", "key_id", "key_prefix", "endpoint", "ip_address", "requests_in_window", "limit", "correlation_id", "occurred_at", "tenant_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_PREFIX_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    REQUESTS_IN_WINDOW_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    key_id: str
    key_prefix: str
    endpoint: str
    ip_address: str
    requests_in_window: int
    limit: int
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, event_id: _Optional[str] = ..., key_id: _Optional[str] = ..., key_prefix: _Optional[str] = ..., endpoint: _Optional[str] = ..., ip_address: _Optional[str] = ..., requests_in_window: _Optional[int] = ..., limit: _Optional[int] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...
