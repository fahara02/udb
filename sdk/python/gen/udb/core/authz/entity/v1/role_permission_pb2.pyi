import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RolePermission(_message.Message):
    __slots__ = ("role_permission_id", "role_id", "permission_code", "granted_by", "granted_at", "tenant_id")
    ROLE_PERMISSION_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    PERMISSION_CODE_FIELD_NUMBER: _ClassVar[int]
    GRANTED_BY_FIELD_NUMBER: _ClassVar[int]
    GRANTED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    role_permission_id: str
    role_id: str
    permission_code: str
    granted_by: str
    granted_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, role_permission_id: _Optional[str] = ..., role_id: _Optional[str] = ..., permission_code: _Optional[str] = ..., granted_by: _Optional[str] = ..., granted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...
