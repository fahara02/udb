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

class Peer(_message.Message):
    __slots__ = ("peer_id", "room_id", "tenant_id", "display_name", "state", "metadata", "user_agent", "joined_at", "left_at", "audit_info", "deleted_at", "deleted_by")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    USER_AGENT_FIELD_NUMBER: _ClassVar[int]
    JOINED_AT_FIELD_NUMBER: _ClassVar[int]
    LEFT_AT_FIELD_NUMBER: _ClassVar[int]
    AUDIT_INFO_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    room_id: str
    tenant_id: str
    display_name: str
    state: _enums_pb2.PeerState
    metadata: str
    user_agent: str
    joined_at: _timestamp_pb2.Timestamp
    left_at: _timestamp_pb2.Timestamp
    audit_info: _types_pb2.AuditInfo
    deleted_at: _timestamp_pb2.Timestamp
    deleted_by: str
    def __init__(self, peer_id: _Optional[str] = ..., room_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., display_name: _Optional[str] = ..., state: _Optional[_Union[_enums_pb2.PeerState, str]] = ..., metadata: _Optional[str] = ..., user_agent: _Optional[str] = ..., joined_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., left_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., audit_info: _Optional[_Union[_types_pb2.AuditInfo, _Mapping]] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_by: _Optional[str] = ...) -> None: ...
