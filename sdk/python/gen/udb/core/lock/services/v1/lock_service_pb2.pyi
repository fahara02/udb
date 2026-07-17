from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AcquireLockRequest(_message.Message):
    __slots__ = ("tenant_id", "lock_name", "owner_id", "lease_ttl_seconds", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    LOCK_NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    lock_name: str
    owner_id: str
    lease_ttl_seconds: int
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., lock_name: _Optional[str] = ..., owner_id: _Optional[str] = ..., lease_ttl_seconds: _Optional[int] = ..., metadata_json: _Optional[str] = ...) -> None: ...

class AcquireLockResponse(_message.Message):
    __slots__ = ("acquired", "fencing_token", "lock_name", "expires_at_unix", "message", "error")
    ACQUIRED_FIELD_NUMBER: _ClassVar[int]
    FENCING_TOKEN_FIELD_NUMBER: _ClassVar[int]
    LOCK_NAME_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    acquired: bool
    fencing_token: int
    lock_name: str
    expires_at_unix: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, acquired: bool = ..., fencing_token: _Optional[int] = ..., lock_name: _Optional[str] = ..., expires_at_unix: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class RenewLockRequest(_message.Message):
    __slots__ = ("tenant_id", "lock_name", "owner_id", "fencing_token", "lease_ttl_seconds")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    LOCK_NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    FENCING_TOKEN_FIELD_NUMBER: _ClassVar[int]
    LEASE_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    lock_name: str
    owner_id: str
    fencing_token: int
    lease_ttl_seconds: int
    def __init__(self, tenant_id: _Optional[str] = ..., lock_name: _Optional[str] = ..., owner_id: _Optional[str] = ..., fencing_token: _Optional[int] = ..., lease_ttl_seconds: _Optional[int] = ...) -> None: ...

class RenewLockResponse(_message.Message):
    __slots__ = ("renewed", "fencing_token", "expires_at_unix", "message", "error")
    RENEWED_FIELD_NUMBER: _ClassVar[int]
    FENCING_TOKEN_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    renewed: bool
    fencing_token: int
    expires_at_unix: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, renewed: bool = ..., fencing_token: _Optional[int] = ..., expires_at_unix: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ReleaseLockRequest(_message.Message):
    __slots__ = ("tenant_id", "lock_name", "owner_id", "fencing_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    LOCK_NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    FENCING_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    lock_name: str
    owner_id: str
    fencing_token: int
    def __init__(self, tenant_id: _Optional[str] = ..., lock_name: _Optional[str] = ..., owner_id: _Optional[str] = ..., fencing_token: _Optional[int] = ...) -> None: ...

class ReleaseLockResponse(_message.Message):
    __slots__ = ("released", "message", "error")
    RELEASED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    released: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, released: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetLockRequest(_message.Message):
    __slots__ = ("tenant_id", "lock_name")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    LOCK_NAME_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    lock_name: str
    def __init__(self, tenant_id: _Optional[str] = ..., lock_name: _Optional[str] = ...) -> None: ...

class GetLockResponse(_message.Message):
    __slots__ = ("lock", "found", "message", "error")
    LOCK_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    lock: Lock
    found: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, lock: _Optional[_Union[Lock, _Mapping]] = ..., found: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListLocksRequest(_message.Message):
    __slots__ = ("tenant_id", "status_filter", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    status_filter: str
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., status_filter: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListLocksResponse(_message.Message):
    __slots__ = ("locks", "next_page_token", "message", "error")
    LOCKS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    locks: _containers.RepeatedCompositeFieldContainer[Lock]
    next_page_token: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, locks: _Optional[_Iterable[_Union[Lock, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class Lock(_message.Message):
    __slots__ = ("lock_id", "tenant_id", "lock_name", "owner_id", "fencing_token", "lease_ttl_seconds", "status", "acquired_at_unix", "expires_at_unix", "metadata_json")
    LOCK_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    LOCK_NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    FENCING_TOKEN_FIELD_NUMBER: _ClassVar[int]
    LEASE_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ACQUIRED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    lock_id: str
    tenant_id: str
    lock_name: str
    owner_id: str
    fencing_token: int
    lease_ttl_seconds: int
    status: str
    acquired_at_unix: int
    expires_at_unix: int
    metadata_json: str
    def __init__(self, lock_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., lock_name: _Optional[str] = ..., owner_id: _Optional[str] = ..., fencing_token: _Optional[int] = ..., lease_ttl_seconds: _Optional[int] = ..., status: _Optional[str] = ..., acquired_at_unix: _Optional[int] = ..., expires_at_unix: _Optional[int] = ..., metadata_json: _Optional[str] = ...) -> None: ...
