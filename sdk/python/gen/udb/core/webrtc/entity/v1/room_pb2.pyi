import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from udb.core.webrtc.entity.v1 import enums_pb2 as _enums_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Room(_message.Message):
    __slots__ = ("room_id", "tenant_id", "name", "state", "max_participants", "participant_count", "config", "created_by", "audit_info", "deleted_at", "deleted_by")
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    MAX_PARTICIPANTS_FIELD_NUMBER: _ClassVar[int]
    PARTICIPANT_COUNT_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    AUDIT_INFO_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    room_id: str
    tenant_id: str
    name: str
    state: _enums_pb2.RoomState
    max_participants: int
    participant_count: int
    config: str
    created_by: str
    audit_info: _types_pb2.AuditInfo
    deleted_at: _timestamp_pb2.Timestamp
    deleted_by: str
    def __init__(self, room_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., name: _Optional[str] = ..., state: _Optional[_Union[_enums_pb2.RoomState, str]] = ..., max_participants: _Optional[int] = ..., participant_count: _Optional[int] = ..., config: _Optional[str] = ..., created_by: _Optional[str] = ..., audit_info: _Optional[_Union[_types_pb2.AuditInfo, _Mapping]] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_by: _Optional[str] = ...) -> None: ...
