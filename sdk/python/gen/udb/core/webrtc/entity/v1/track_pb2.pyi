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

class Track(_message.Message):
    __slots__ = ("track_id", "room_id", "peer_id", "tenant_id", "kind", "label", "state", "settings", "metadata", "audit_info")
    TRACK_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    AUDIT_INFO_FIELD_NUMBER: _ClassVar[int]
    track_id: str
    room_id: str
    peer_id: str
    tenant_id: str
    kind: _enums_pb2.TrackKind
    label: str
    state: _enums_pb2.TrackState
    settings: str
    metadata: str
    audit_info: _types_pb2.AuditInfo
    def __init__(self, track_id: _Optional[str] = ..., room_id: _Optional[str] = ..., peer_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., kind: _Optional[_Union[_enums_pb2.TrackKind, str]] = ..., label: _Optional[str] = ..., state: _Optional[_Union[_enums_pb2.TrackState, str]] = ..., settings: _Optional[str] = ..., metadata: _Optional[str] = ..., audit_info: _Optional[_Union[_types_pb2.AuditInfo, _Mapping]] = ...) -> None: ...
