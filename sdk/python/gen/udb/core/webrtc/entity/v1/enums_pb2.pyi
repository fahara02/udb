from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class RoomState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ROOM_STATE_UNSPECIFIED: _ClassVar[RoomState]
    ROOM_STATE_ACTIVE: _ClassVar[RoomState]
    ROOM_STATE_IDLE: _ClassVar[RoomState]
    ROOM_STATE_CLOSED: _ClassVar[RoomState]

class PeerState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PEER_STATE_UNSPECIFIED: _ClassVar[PeerState]
    PEER_STATE_NEW: _ClassVar[PeerState]
    PEER_STATE_CONNECTING: _ClassVar[PeerState]
    PEER_STATE_CONNECTED: _ClassVar[PeerState]
    PEER_STATE_DISCONNECTED: _ClassVar[PeerState]
    PEER_STATE_FAILED: _ClassVar[PeerState]
    PEER_STATE_CLOSED: _ClassVar[PeerState]

class TrackKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRACK_KIND_UNSPECIFIED: _ClassVar[TrackKind]
    TRACK_KIND_AUDIO: _ClassVar[TrackKind]
    TRACK_KIND_VIDEO: _ClassVar[TrackKind]
    TRACK_KIND_SCREEN: _ClassVar[TrackKind]
    TRACK_KIND_DATA: _ClassVar[TrackKind]

class TrackState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRACK_STATE_UNSPECIFIED: _ClassVar[TrackState]
    TRACK_STATE_ACTIVE: _ClassVar[TrackState]
    TRACK_STATE_MUTED: _ClassVar[TrackState]
    TRACK_STATE_ENDED: _ClassVar[TrackState]
ROOM_STATE_UNSPECIFIED: RoomState
ROOM_STATE_ACTIVE: RoomState
ROOM_STATE_IDLE: RoomState
ROOM_STATE_CLOSED: RoomState
PEER_STATE_UNSPECIFIED: PeerState
PEER_STATE_NEW: PeerState
PEER_STATE_CONNECTING: PeerState
PEER_STATE_CONNECTED: PeerState
PEER_STATE_DISCONNECTED: PeerState
PEER_STATE_FAILED: PeerState
PEER_STATE_CLOSED: PeerState
TRACK_KIND_UNSPECIFIED: TrackKind
TRACK_KIND_AUDIO: TrackKind
TRACK_KIND_VIDEO: TrackKind
TRACK_KIND_SCREEN: TrackKind
TRACK_KIND_DATA: TrackKind
TRACK_STATE_UNSPECIFIED: TrackState
TRACK_STATE_ACTIVE: TrackState
TRACK_STATE_MUTED: TrackState
TRACK_STATE_ENDED: TrackState
