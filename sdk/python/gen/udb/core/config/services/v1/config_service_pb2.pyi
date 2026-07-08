from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FlagValue(_message.Message):
    __slots__ = ("bool_value", "string_value", "number_value", "json_value")
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    NUMBER_VALUE_FIELD_NUMBER: _ClassVar[int]
    JSON_VALUE_FIELD_NUMBER: _ClassVar[int]
    bool_value: bool
    string_value: str
    number_value: float
    json_value: str
    def __init__(self, bool_value: bool = ..., string_value: _Optional[str] = ..., number_value: _Optional[float] = ..., json_value: _Optional[str] = ...) -> None: ...

class FlagState(_message.Message):
    __slots__ = ("tenant_id", "project_id", "environment", "flag_key", "value", "enabled", "rollout_percentage", "rollout_context_key", "revision", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    FLAG_KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    ROLLOUT_PERCENTAGE_FIELD_NUMBER: _ClassVar[int]
    ROLLOUT_CONTEXT_KEY_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    environment: str
    flag_key: str
    value: FlagValue
    enabled: bool
    rollout_percentage: int
    rollout_context_key: str
    revision: int
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., environment: _Optional[str] = ..., flag_key: _Optional[str] = ..., value: _Optional[_Union[FlagValue, _Mapping]] = ..., enabled: bool = ..., rollout_percentage: _Optional[int] = ..., rollout_context_key: _Optional[str] = ..., revision: _Optional[int] = ..., metadata_json: _Optional[str] = ...) -> None: ...

class EvaluateContext(_message.Message):
    __slots__ = ("project_id", "environment", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    environment: str
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, project_id: _Optional[str] = ..., environment: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class PutFlagRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id", "environment", "flag_key", "value", "enabled", "rollout_percentage", "rollout_context_key", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    FLAG_KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    ROLLOUT_PERCENTAGE_FIELD_NUMBER: _ClassVar[int]
    ROLLOUT_CONTEXT_KEY_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    environment: str
    flag_key: str
    value: FlagValue
    enabled: bool
    rollout_percentage: int
    rollout_context_key: str
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., environment: _Optional[str] = ..., flag_key: _Optional[str] = ..., value: _Optional[_Union[FlagValue, _Mapping]] = ..., enabled: bool = ..., rollout_percentage: _Optional[int] = ..., rollout_context_key: _Optional[str] = ..., metadata_json: _Optional[str] = ...) -> None: ...

class PutFlagResponse(_message.Message):
    __slots__ = ("stored", "flag_key", "revision", "message", "error")
    STORED_FIELD_NUMBER: _ClassVar[int]
    FLAG_KEY_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    stored: bool
    flag_key: str
    revision: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, stored: bool = ..., flag_key: _Optional[str] = ..., revision: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetFlagRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id", "environment", "flag_key")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    FLAG_KEY_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    environment: str
    flag_key: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., environment: _Optional[str] = ..., flag_key: _Optional[str] = ...) -> None: ...

class GetFlagResponse(_message.Message):
    __slots__ = ("found", "flag", "message", "error")
    FOUND_FIELD_NUMBER: _ClassVar[int]
    FLAG_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    found: bool
    flag: FlagState
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, found: bool = ..., flag: _Optional[_Union[FlagState, _Mapping]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListFlagsRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id", "environment", "limit", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    environment: str
    limit: int
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., environment: _Optional[str] = ..., limit: _Optional[int] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListFlagsResponse(_message.Message):
    __slots__ = ("flags", "message", "error", "next_page_token")
    FLAGS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    flags: _containers.RepeatedCompositeFieldContainer[FlagState]
    message: str
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, flags: _Optional[_Iterable[_Union[FlagState, _Mapping]]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class DeleteFlagRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id", "environment", "flag_key")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    FLAG_KEY_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    environment: str
    flag_key: str
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., environment: _Optional[str] = ..., flag_key: _Optional[str] = ...) -> None: ...

class DeleteFlagResponse(_message.Message):
    __slots__ = ("deleted", "revision", "message", "error")
    DELETED_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    revision: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, deleted: bool = ..., revision: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class EvaluateFlagsRequest(_message.Message):
    __slots__ = ("tenant_id", "keys", "context")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    keys: _containers.RepeatedScalarFieldContainer[str]
    context: EvaluateContext
    def __init__(self, tenant_id: _Optional[str] = ..., keys: _Optional[_Iterable[str]] = ..., context: _Optional[_Union[EvaluateContext, _Mapping]] = ...) -> None: ...

class EvaluateFlagsResponse(_message.Message):
    __slots__ = ("values", "server_ttl_seconds", "config_revision", "message", "error")
    class ValuesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: FlagValue
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[FlagValue, _Mapping]] = ...) -> None: ...
    VALUES_FIELD_NUMBER: _ClassVar[int]
    SERVER_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    CONFIG_REVISION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    values: _containers.MessageMap[str, FlagValue]
    server_ttl_seconds: int
    config_revision: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, values: _Optional[_Mapping[str, FlagValue]] = ..., server_ttl_seconds: _Optional[int] = ..., config_revision: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
