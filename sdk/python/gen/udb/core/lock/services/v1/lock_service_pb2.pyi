from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
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
