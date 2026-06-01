import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RoleAssignedEvent(_message.Message):
    __slots__ = ("event_id", "user_role_id", "user_id", "role_id", "role_code", "tenant_id", "assigned_by", "occurred_at", "domain", "project_id", "access_surface")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_CODE_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ASSIGNED_BY_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ACCESS_SURFACE_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    user_role_id: str
    user_id: str
    role_id: str
    role_code: str
    tenant_id: str
    assigned_by: str
    occurred_at: _timestamp_pb2.Timestamp
    domain: str
    project_id: str
    access_surface: str
    def __init__(self, event_id: _Optional[str] = ..., user_role_id: _Optional[str] = ..., user_id: _Optional[str] = ..., role_id: _Optional[str] = ..., role_code: _Optional[str] = ..., tenant_id: _Optional[str] = ..., assigned_by: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., domain: _Optional[str] = ..., project_id: _Optional[str] = ..., access_surface: _Optional[str] = ...) -> None: ...

class RoleRevokedEvent(_message.Message):
    __slots__ = ("event_id", "user_role_id", "user_id", "role_code", "tenant_id", "revoked_by", "reason", "occurred_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_CODE_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    REVOKED_BY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    user_role_id: str
    user_id: str
    role_code: str
    tenant_id: str
    revoked_by: str
    reason: str
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., user_role_id: _Optional[str] = ..., user_id: _Optional[str] = ..., role_code: _Optional[str] = ..., tenant_id: _Optional[str] = ..., revoked_by: _Optional[str] = ..., reason: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class AccessDeniedEvent(_message.Message):
    __slots__ = ("event_id", "user_id", "tenant_id", "resource", "action", "deny_reason", "occurred_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    DENY_REASON_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    user_id: str
    tenant_id: str
    resource: str
    action: str
    deny_reason: str
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., user_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., resource: _Optional[str] = ..., action: _Optional[str] = ..., deny_reason: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class RoleCreatedEvent(_message.Message):
    __slots__ = ("event_id", "role_id", "role_code", "tenant_id", "created_by", "occurred_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_CODE_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    role_id: str
    role_code: str
    tenant_id: str
    created_by: str
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., role_id: _Optional[str] = ..., role_code: _Optional[str] = ..., tenant_id: _Optional[str] = ..., created_by: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class RoleUpdatedEvent(_message.Message):
    __slots__ = ("event_id", "role_id", "role_code", "tenant_id", "updated_by", "added_permissions", "removed_permissions", "occurred_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_CODE_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    UPDATED_BY_FIELD_NUMBER: _ClassVar[int]
    ADDED_PERMISSIONS_FIELD_NUMBER: _ClassVar[int]
    REMOVED_PERMISSIONS_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    role_id: str
    role_code: str
    tenant_id: str
    updated_by: str
    added_permissions: _containers.RepeatedScalarFieldContainer[str]
    removed_permissions: _containers.RepeatedScalarFieldContainer[str]
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., role_id: _Optional[str] = ..., role_code: _Optional[str] = ..., tenant_id: _Optional[str] = ..., updated_by: _Optional[str] = ..., added_permissions: _Optional[_Iterable[str]] = ..., removed_permissions: _Optional[_Iterable[str]] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class RoleRemovedEvent(_message.Message):
    __slots__ = ("event_id", "user_role_id", "user_id", "role_id", "role_code", "tenant_id", "removed_by", "reason", "occurred_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_CODE_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    REMOVED_BY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    user_role_id: str
    user_id: str
    role_id: str
    role_code: str
    tenant_id: str
    removed_by: str
    reason: str
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., user_role_id: _Optional[str] = ..., user_id: _Optional[str] = ..., role_id: _Optional[str] = ..., role_code: _Optional[str] = ..., tenant_id: _Optional[str] = ..., removed_by: _Optional[str] = ..., reason: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class AccessSurfaceUpdatedEvent(_message.Message):
    __slots__ = ("event_id", "principal_id", "old_access_surface", "new_access_surface", "tenant_id", "project_id", "updated_by", "correlation_id", "occurred_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    OLD_ACCESS_SURFACE_FIELD_NUMBER: _ClassVar[int]
    NEW_ACCESS_SURFACE_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    UPDATED_BY_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    principal_id: str
    old_access_surface: str
    new_access_surface: str
    tenant_id: str
    project_id: str
    updated_by: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., principal_id: _Optional[str] = ..., old_access_surface: _Optional[str] = ..., new_access_surface: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., updated_by: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
