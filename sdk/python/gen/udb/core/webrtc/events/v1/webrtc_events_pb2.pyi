import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RoomCreated(_message.Message):
    __slots__ = ("event_id", "room_id", "tenant_id", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    room_id: str
    tenant_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., room_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class RoomClosed(_message.Message):
    __slots__ = ("event_id", "room_id", "tenant_id", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    room_id: str
    tenant_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., room_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PeerJoined(_message.Message):
    __slots__ = ("event_id", "room_id", "peer_id", "tenant_id", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    room_id: str
    peer_id: str
    tenant_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., room_id: _Optional[str] = ..., peer_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PeerLeft(_message.Message):
    __slots__ = ("event_id", "room_id", "peer_id", "tenant_id", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    room_id: str
    peer_id: str
    tenant_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., room_id: _Optional[str] = ..., peer_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class TrackPublished(_message.Message):
    __slots__ = ("event_id", "room_id", "peer_id", "track_id", "tenant_id", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    TRACK_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    room_id: str
    peer_id: str
    track_id: str
    tenant_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., room_id: _Optional[str] = ..., peer_id: _Optional[str] = ..., track_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
