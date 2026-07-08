from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QuotaState(_message.Message):
    __slots__ = ("tenant_id", "project_id", "metric", "limit_value", "window_seconds", "enabled", "revision", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    METRIC_FIELD_NUMBER: _ClassVar[int]
    LIMIT_VALUE_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    metric: str
    limit_value: int
    window_seconds: int
    enabled: bool
    revision: int
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., metric: _Optional[str] = ..., limit_value: _Optional[int] = ..., window_seconds: _Optional[int] = ..., enabled: bool = ..., revision: _Optional[int] = ..., metadata_json: _Optional[str] = ...) -> None: ...

class RecordUsageRequest(_message.Message):
    __slots__ = ("tenant_id", "principal_id", "method", "unit", "quantity", "occurred_at_unix", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    UNIT_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    principal_id: str
    method: str
    unit: str
    quantity: int
    occurred_at_unix: int
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., principal_id: _Optional[str] = ..., method: _Optional[str] = ..., unit: _Optional[str] = ..., quantity: _Optional[int] = ..., occurred_at_unix: _Optional[int] = ..., metadata_json: _Optional[str] = ...) -> None: ...

class RecordUsageResponse(_message.Message):
    __slots__ = ("recorded", "message", "error")
    RECORDED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    recorded: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, recorded: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class QueryUsageRequest(_message.Message):
    __slots__ = ("tenant_id", "metric", "window_seconds")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    METRIC_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SECONDS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    metric: str
    window_seconds: int
    def __init__(self, tenant_id: _Optional[str] = ..., metric: _Optional[str] = ..., window_seconds: _Optional[int] = ...) -> None: ...

class QueryUsageResponse(_message.Message):
    __slots__ = ("metric", "used", "window_seconds", "from_unix", "to_unix", "message", "error")
    METRIC_FIELD_NUMBER: _ClassVar[int]
    USED_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SECONDS_FIELD_NUMBER: _ClassVar[int]
    FROM_UNIX_FIELD_NUMBER: _ClassVar[int]
    TO_UNIX_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    metric: str
    used: int
    window_seconds: int
    from_unix: int
    to_unix: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, metric: _Optional[str] = ..., used: _Optional[int] = ..., window_seconds: _Optional[int] = ..., from_unix: _Optional[int] = ..., to_unix: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class PutQuotaRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id", "metric", "limit_value", "window_seconds", "enabled", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    METRIC_FIELD_NUMBER: _ClassVar[int]
    LIMIT_VALUE_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    metric: str
    limit_value: int
    window_seconds: int
    enabled: bool
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., metric: _Optional[str] = ..., limit_value: _Optional[int] = ..., window_seconds: _Optional[int] = ..., enabled: bool = ..., metadata_json: _Optional[str] = ...) -> None: ...

class PutQuotaResponse(_message.Message):
    __slots__ = ("stored", "metric", "revision", "message", "error")
    STORED_FIELD_NUMBER: _ClassVar[int]
    METRIC_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    stored: bool
    metric: str
    revision: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, stored: bool = ..., metric: _Optional[str] = ..., revision: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetQuotaRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id", "metric")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    METRIC_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    metric: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., metric: _Optional[str] = ...) -> None: ...

class GetQuotaResponse(_message.Message):
    __slots__ = ("found", "quota", "message", "error")
    FOUND_FIELD_NUMBER: _ClassVar[int]
    QUOTA_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    found: bool
    quota: QuotaState
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, found: bool = ..., quota: _Optional[_Union[QuotaState, _Mapping]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListQuotasRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id", "limit", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    limit: int
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., limit: _Optional[int] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListQuotasResponse(_message.Message):
    __slots__ = ("quotas", "message", "error", "next_page_token")
    QUOTAS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    quotas: _containers.RepeatedCompositeFieldContainer[QuotaState]
    message: str
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, quotas: _Optional[_Iterable[_Union[QuotaState, _Mapping]]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class CheckQuotaRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id", "metric")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    METRIC_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    metric: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., metric: _Optional[str] = ...) -> None: ...

class CheckQuotaResponse(_message.Message):
    __slots__ = ("allowed", "used", "limit_value", "remaining", "unlimited", "message", "error")
    ALLOWED_FIELD_NUMBER: _ClassVar[int]
    USED_FIELD_NUMBER: _ClassVar[int]
    LIMIT_VALUE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_FIELD_NUMBER: _ClassVar[int]
    UNLIMITED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    allowed: bool
    used: int
    limit_value: int
    remaining: int
    unlimited: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, allowed: bool = ..., used: _Optional[int] = ..., limit_value: _Optional[int] = ..., remaining: _Optional[int] = ..., unlimited: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
