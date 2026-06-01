import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.apikey.entity.v1 import api_key_pb2 as _api_key_pb2
from udb.core.apikey.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateApiKeyRequest(_message.Message):
    __slots__ = ("name", "description", "owner_type", "owner_id", "scopes", "ip_allowlist", "rate_limit_per_minute", "rate_limit_per_day", "expires_at", "context")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    OWNER_TYPE_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    IP_ALLOWLIST_FIELD_NUMBER: _ClassVar[int]
    RATE_LIMIT_PER_MINUTE_FIELD_NUMBER: _ClassVar[int]
    RATE_LIMIT_PER_DAY_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    owner_type: _enums_pb2.ApiKeyOwnerType
    owner_id: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    ip_allowlist: _containers.RepeatedScalarFieldContainer[str]
    rate_limit_per_minute: int
    rate_limit_per_day: int
    expires_at: _timestamp_pb2.Timestamp
    context: _types_pb2.RequestContext
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., owner_type: _Optional[_Union[_enums_pb2.ApiKeyOwnerType, str]] = ..., owner_id: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., ip_allowlist: _Optional[_Iterable[str]] = ..., rate_limit_per_minute: _Optional[int] = ..., rate_limit_per_day: _Optional[int] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class CreateApiKeyResponse(_message.Message):
    __slots__ = ("key", "plain_key")
    KEY_FIELD_NUMBER: _ClassVar[int]
    PLAIN_KEY_FIELD_NUMBER: _ClassVar[int]
    key: _api_key_pb2.ApiKey
    plain_key: str
    def __init__(self, key: _Optional[_Union[_api_key_pb2.ApiKey, _Mapping]] = ..., plain_key: _Optional[str] = ...) -> None: ...

class GetApiKeyRequest(_message.Message):
    __slots__ = ("key_id",)
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    key_id: str
    def __init__(self, key_id: _Optional[str] = ...) -> None: ...

class GetApiKeyResponse(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: _api_key_pb2.ApiKey
    def __init__(self, key: _Optional[_Union[_api_key_pb2.ApiKey, _Mapping]] = ...) -> None: ...

class ListApiKeysRequest(_message.Message):
    __slots__ = ("owner_id", "owner_type", "status", "page")
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    owner_id: str
    owner_type: _enums_pb2.ApiKeyOwnerType
    status: _enums_pb2.ApiKeyStatus
    page: _dto_pb2.PageRequest
    def __init__(self, owner_id: _Optional[str] = ..., owner_type: _Optional[_Union[_enums_pb2.ApiKeyOwnerType, str]] = ..., status: _Optional[_Union[_enums_pb2.ApiKeyStatus, str]] = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListApiKeysResponse(_message.Message):
    __slots__ = ("keys", "page")
    KEYS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedCompositeFieldContainer[_api_key_pb2.ApiKey]
    page: _dto_pb2.PageResponse
    def __init__(self, keys: _Optional[_Iterable[_Union[_api_key_pb2.ApiKey, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class RevokeApiKeyRequest(_message.Message):
    __slots__ = ("key_id", "revoke_reason", "context")
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    REVOKE_REASON_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    key_id: str
    revoke_reason: str
    context: _types_pb2.RequestContext
    def __init__(self, key_id: _Optional[str] = ..., revoke_reason: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class RevokeApiKeyResponse(_message.Message):
    __slots__ = ("key_id", "revoked_at", "operation_id")
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    REVOKED_AT_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    key_id: str
    revoked_at: _timestamp_pb2.Timestamp
    operation_id: str
    def __init__(self, key_id: _Optional[str] = ..., revoked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., operation_id: _Optional[str] = ...) -> None: ...

class UpdateApiKeyRequest(_message.Message):
    __slots__ = ("key_id", "name", "description", "scopes", "ip_allowlist", "rate_limit_per_minute", "rate_limit_per_day", "expires_at", "context")
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    IP_ALLOWLIST_FIELD_NUMBER: _ClassVar[int]
    RATE_LIMIT_PER_MINUTE_FIELD_NUMBER: _ClassVar[int]
    RATE_LIMIT_PER_DAY_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    key_id: str
    name: str
    description: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    ip_allowlist: _containers.RepeatedScalarFieldContainer[str]
    rate_limit_per_minute: int
    rate_limit_per_day: int
    expires_at: _timestamp_pb2.Timestamp
    context: _types_pb2.RequestContext
    def __init__(self, key_id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., ip_allowlist: _Optional[_Iterable[str]] = ..., rate_limit_per_minute: _Optional[int] = ..., rate_limit_per_day: _Optional[int] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class UpdateApiKeyResponse(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: _api_key_pb2.ApiKey
    def __init__(self, key: _Optional[_Union[_api_key_pb2.ApiKey, _Mapping]] = ...) -> None: ...

class ValidateApiKeyRequest(_message.Message):
    __slots__ = ("plain_key", "endpoint", "required_scope", "ip_address")
    PLAIN_KEY_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_SCOPE_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    plain_key: str
    endpoint: str
    required_scope: str
    ip_address: str
    def __init__(self, plain_key: _Optional[str] = ..., endpoint: _Optional[str] = ..., required_scope: _Optional[str] = ..., ip_address: _Optional[str] = ...) -> None: ...

class ValidateApiKeyResponse(_message.Message):
    __slots__ = ("valid", "key_id", "owner_id", "owner_type", "scopes", "rate_limited")
    VALID_FIELD_NUMBER: _ClassVar[int]
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_TYPE_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    RATE_LIMITED_FIELD_NUMBER: _ClassVar[int]
    valid: bool
    key_id: str
    owner_id: str
    owner_type: _enums_pb2.ApiKeyOwnerType
    scopes: _containers.RepeatedScalarFieldContainer[str]
    rate_limited: bool
    def __init__(self, valid: bool = ..., key_id: _Optional[str] = ..., owner_id: _Optional[str] = ..., owner_type: _Optional[_Union[_enums_pb2.ApiKeyOwnerType, str]] = ..., scopes: _Optional[_Iterable[str]] = ..., rate_limited: bool = ...) -> None: ...

class GetApiKeyUsageStatsRequest(_message.Message):
    __slots__ = ("key_id", "to")
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    key_id: str
    to: _timestamp_pb2.Timestamp
    def __init__(self, key_id: _Optional[str] = ..., to: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., **kwargs) -> None: ...

class ApiKeyDailyStat(_message.Message):
    __slots__ = ("date", "total_requests", "rate_limited_count", "avg_latency_ms", "status_counts")
    class StatusCountsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    DATE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    RATE_LIMITED_COUNT_FIELD_NUMBER: _ClassVar[int]
    AVG_LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    STATUS_COUNTS_FIELD_NUMBER: _ClassVar[int]
    date: str
    total_requests: int
    rate_limited_count: int
    avg_latency_ms: float
    status_counts: _containers.ScalarMap[str, int]
    def __init__(self, date: _Optional[str] = ..., total_requests: _Optional[int] = ..., rate_limited_count: _Optional[int] = ..., avg_latency_ms: _Optional[float] = ..., status_counts: _Optional[_Mapping[str, int]] = ...) -> None: ...

class GetApiKeyUsageStatsResponse(_message.Message):
    __slots__ = ("stats", "total_requests")
    STATS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    stats: _containers.RepeatedCompositeFieldContainer[ApiKeyDailyStat]
    total_requests: int
    def __init__(self, stats: _Optional[_Iterable[_Union[ApiKeyDailyStat, _Mapping]]] = ..., total_requests: _Optional[int] = ...) -> None: ...
