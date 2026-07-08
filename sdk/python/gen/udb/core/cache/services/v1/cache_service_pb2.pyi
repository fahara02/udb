from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetRequest(_message.Message):
    __slots__ = ("tenant_id", "namespace", "key")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    namespace: str
    key: str
    def __init__(self, tenant_id: _Optional[str] = ..., namespace: _Optional[str] = ..., key: _Optional[str] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ("found", "value", "ttl_remaining_seconds", "error")
    FOUND_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    TTL_REMAINING_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    found: bool
    value: bytes
    ttl_remaining_seconds: int
    error: _dto_pb2.ApiError
    def __init__(self, found: bool = ..., value: _Optional[bytes] = ..., ttl_remaining_seconds: _Optional[int] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class SetRequest(_message.Message):
    __slots__ = ("tenant_id", "namespace", "key", "value", "ttl_seconds")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    namespace: str
    key: str
    value: bytes
    ttl_seconds: int
    def __init__(self, tenant_id: _Optional[str] = ..., namespace: _Optional[str] = ..., key: _Optional[str] = ..., value: _Optional[bytes] = ..., ttl_seconds: _Optional[int] = ...) -> None: ...

class SetResponse(_message.Message):
    __slots__ = ("stored", "used_bytes", "max_bytes", "message", "error")
    STORED_FIELD_NUMBER: _ClassVar[int]
    USED_BYTES_FIELD_NUMBER: _ClassVar[int]
    MAX_BYTES_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    stored: bool
    used_bytes: int
    max_bytes: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, stored: bool = ..., used_bytes: _Optional[int] = ..., max_bytes: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class DeleteRequest(_message.Message):
    __slots__ = ("tenant_id", "namespace", "key")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    namespace: str
    key: str
    def __init__(self, tenant_id: _Optional[str] = ..., namespace: _Optional[str] = ..., key: _Optional[str] = ...) -> None: ...

class DeleteResponse(_message.Message):
    __slots__ = ("deleted", "used_bytes", "message", "error")
    DELETED_FIELD_NUMBER: _ClassVar[int]
    USED_BYTES_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    used_bytes: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, deleted: bool = ..., used_bytes: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ScanRequest(_message.Message):
    __slots__ = ("tenant_id", "namespace", "key_prefix", "limit", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    KEY_PREFIX_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    namespace: str
    key_prefix: str
    limit: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., namespace: _Optional[str] = ..., key_prefix: _Optional[str] = ..., limit: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ScanResponse(_message.Message):
    __slots__ = ("items", "next_page_token", "error")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[CacheItem]
    next_page_token: str
    error: _dto_pb2.ApiError
    def __init__(self, items: _Optional[_Iterable[_Union[CacheItem, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class CacheItem(_message.Message):
    __slots__ = ("key", "value", "ttl_remaining_seconds")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    TTL_REMAINING_SECONDS_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    ttl_remaining_seconds: int
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ..., ttl_remaining_seconds: _Optional[int] = ...) -> None: ...

class CreateNamespaceRequest(_message.Message):
    __slots__ = ("tenant_id", "namespace", "max_bytes", "default_ttl_seconds")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    MAX_BYTES_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    namespace: str
    max_bytes: int
    default_ttl_seconds: int
    def __init__(self, tenant_id: _Optional[str] = ..., namespace: _Optional[str] = ..., max_bytes: _Optional[int] = ..., default_ttl_seconds: _Optional[int] = ...) -> None: ...

class CreateNamespaceResponse(_message.Message):
    __slots__ = ("namespace", "max_bytes", "default_ttl_seconds", "message", "error")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    MAX_BYTES_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    max_bytes: int
    default_ttl_seconds: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, namespace: _Optional[str] = ..., max_bytes: _Optional[int] = ..., default_ttl_seconds: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class DeleteNamespaceRequest(_message.Message):
    __slots__ = ("tenant_id", "namespace", "confirmation_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    CONFIRMATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    namespace: str
    confirmation_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., namespace: _Optional[str] = ..., confirmation_token: _Optional[str] = ...) -> None: ...

class DeleteNamespaceResponse(_message.Message):
    __slots__ = ("namespace", "keys_deleted", "message", "error")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    KEYS_DELETED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    keys_deleted: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, namespace: _Optional[str] = ..., keys_deleted: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetNamespaceStatsRequest(_message.Message):
    __slots__ = ("tenant_id", "namespace")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    namespace: str
    def __init__(self, tenant_id: _Optional[str] = ..., namespace: _Optional[str] = ...) -> None: ...

class GetNamespaceStatsResponse(_message.Message):
    __slots__ = ("namespace", "used_bytes", "max_bytes", "item_count", "error")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    USED_BYTES_FIELD_NUMBER: _ClassVar[int]
    MAX_BYTES_FIELD_NUMBER: _ClassVar[int]
    ITEM_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    used_bytes: int
    max_bytes: int
    item_count: int
    error: _dto_pb2.ApiError
    def __init__(self, namespace: _Optional[str] = ..., used_bytes: _Optional[int] = ..., max_bytes: _Optional[int] = ..., item_count: _Optional[int] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
