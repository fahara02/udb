import datetime

from google.api import annotations_pb2 as _annotations_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.webrtc.entity.v1 import room_pb2 as _room_pb2
from udb.core.webrtc.entity.v1 import peer_pb2 as _peer_pb2
from udb.core.webrtc.entity.v1 import track_pb2 as _track_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateRoomRequest(_message.Message):
    __slots__ = ("tenant_id", "name", "max_participants", "config", "created_by")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    MAX_PARTICIPANTS_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    name: str
    max_participants: int
    config: str
    created_by: str
    def __init__(self, tenant_id: _Optional[str] = ..., name: _Optional[str] = ..., max_participants: _Optional[int] = ..., config: _Optional[str] = ..., created_by: _Optional[str] = ...) -> None: ...

class CreateRoomResponse(_message.Message):
    __slots__ = ("room_id", "message", "error")
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    room_id: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, room_id: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetRoomRequest(_message.Message):
    __slots__ = ("tenant_id", "room_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    room_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ...) -> None: ...

class GetRoomResponse(_message.Message):
    __slots__ = ("room", "error")
    ROOM_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    room: _room_pb2.Room
    error: _dto_pb2.ApiError
    def __init__(self, room: _Optional[_Union[_room_pb2.Room, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class UpdateRoomRequest(_message.Message):
    __slots__ = ("tenant_id", "room_id", "name", "state", "config")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    room_id: str
    name: str
    state: str
    config: str
    def __init__(self, tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ..., name: _Optional[str] = ..., state: _Optional[str] = ..., config: _Optional[str] = ...) -> None: ...

class UpdateRoomResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class CloseRoomRequest(_message.Message):
    __slots__ = ("tenant_id", "room_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    room_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ...) -> None: ...

class CloseRoomResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListRoomsRequest(_message.Message):
    __slots__ = ("tenant_id", "state", "page", "page_size")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    state: str
    page: int
    page_size: int
    def __init__(self, tenant_id: _Optional[str] = ..., state: _Optional[str] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ...) -> None: ...

class ListRoomsResponse(_message.Message):
    __slots__ = ("rooms", "total_count", "error")
    ROOMS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    rooms: _containers.RepeatedCompositeFieldContainer[_room_pb2.Room]
    total_count: int
    error: _dto_pb2.ApiError
    def __init__(self, rooms: _Optional[_Iterable[_Union[_room_pb2.Room, _Mapping]]] = ..., total_count: _Optional[int] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class JoinRoomRequest(_message.Message):
    __slots__ = ("tenant_id", "room_id", "display_name", "metadata", "user_agent")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    USER_AGENT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    room_id: str
    display_name: str
    metadata: str
    user_agent: str
    def __init__(self, tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ..., display_name: _Optional[str] = ..., metadata: _Optional[str] = ..., user_agent: _Optional[str] = ...) -> None: ...

class JoinRoomResponse(_message.Message):
    __slots__ = ("peer", "existing_peers", "error")
    PEER_FIELD_NUMBER: _ClassVar[int]
    EXISTING_PEERS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    peer: _peer_pb2.Peer
    existing_peers: _containers.RepeatedCompositeFieldContainer[_peer_pb2.Peer]
    error: _dto_pb2.ApiError
    def __init__(self, peer: _Optional[_Union[_peer_pb2.Peer, _Mapping]] = ..., existing_peers: _Optional[_Iterable[_Union[_peer_pb2.Peer, _Mapping]]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class JoinSessionRequest(_message.Message):
    __slots__ = ("tenant_id", "room_id", "display_name", "metadata", "user_agent", "ttl_seconds")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    USER_AGENT_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    room_id: str
    display_name: str
    metadata: str
    user_agent: str
    ttl_seconds: int
    def __init__(self, tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ..., display_name: _Optional[str] = ..., metadata: _Optional[str] = ..., user_agent: _Optional[str] = ..., ttl_seconds: _Optional[int] = ...) -> None: ...

class JoinSessionResponse(_message.Message):
    __slots__ = ("peer", "existing_peers", "ice_servers", "expires_at", "error")
    PEER_FIELD_NUMBER: _ClassVar[int]
    EXISTING_PEERS_FIELD_NUMBER: _ClassVar[int]
    ICE_SERVERS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    peer: _peer_pb2.Peer
    existing_peers: _containers.RepeatedCompositeFieldContainer[_peer_pb2.Peer]
    ice_servers: _containers.RepeatedCompositeFieldContainer[IceServer]
    expires_at: _timestamp_pb2.Timestamp
    error: _dto_pb2.ApiError
    def __init__(self, peer: _Optional[_Union[_peer_pb2.Peer, _Mapping]] = ..., existing_peers: _Optional[_Iterable[_Union[_peer_pb2.Peer, _Mapping]]] = ..., ice_servers: _Optional[_Iterable[_Union[IceServer, _Mapping]]] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class LeaveRoomRequest(_message.Message):
    __slots__ = ("tenant_id", "room_id", "peer_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    room_id: str
    peer_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ..., peer_id: _Optional[str] = ...) -> None: ...

class LeaveRoomResponse(_message.Message):
    __slots__ = ("success", "error")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: _dto_pb2.ApiError
    def __init__(self, success: bool = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetPeerRequest(_message.Message):
    __slots__ = ("tenant_id", "peer_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    peer_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., peer_id: _Optional[str] = ...) -> None: ...

class GetPeerResponse(_message.Message):
    __slots__ = ("peer", "error")
    PEER_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    peer: _peer_pb2.Peer
    error: _dto_pb2.ApiError
    def __init__(self, peer: _Optional[_Union[_peer_pb2.Peer, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListPeersRequest(_message.Message):
    __slots__ = ("tenant_id", "room_id", "state")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    room_id: str
    state: str
    def __init__(self, tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ..., state: _Optional[str] = ...) -> None: ...

class ListPeersResponse(_message.Message):
    __slots__ = ("peers", "error")
    PEERS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    peers: _containers.RepeatedCompositeFieldContainer[_peer_pb2.Peer]
    error: _dto_pb2.ApiError
    def __init__(self, peers: _Optional[_Iterable[_Union[_peer_pb2.Peer, _Mapping]]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class PublishTrackRequest(_message.Message):
    __slots__ = ("tenant_id", "room_id", "peer_id", "kind", "label", "settings", "metadata")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    room_id: str
    peer_id: str
    kind: str
    label: str
    settings: str
    metadata: str
    def __init__(self, tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ..., peer_id: _Optional[str] = ..., kind: _Optional[str] = ..., label: _Optional[str] = ..., settings: _Optional[str] = ..., metadata: _Optional[str] = ...) -> None: ...

class PublishTrackResponse(_message.Message):
    __slots__ = ("track_id", "message", "error")
    TRACK_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    track_id: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, track_id: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class UnpublishTrackRequest(_message.Message):
    __slots__ = ("tenant_id", "track_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TRACK_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    track_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., track_id: _Optional[str] = ...) -> None: ...

class UnpublishTrackResponse(_message.Message):
    __slots__ = ("success", "error")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: _dto_pb2.ApiError
    def __init__(self, success: bool = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class MuteTrackRequest(_message.Message):
    __slots__ = ("tenant_id", "track_id", "muted")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TRACK_ID_FIELD_NUMBER: _ClassVar[int]
    MUTED_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    track_id: str
    muted: bool
    def __init__(self, tenant_id: _Optional[str] = ..., track_id: _Optional[str] = ..., muted: bool = ...) -> None: ...

class MuteTrackResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListTracksRequest(_message.Message):
    __slots__ = ("tenant_id", "room_id", "peer_id", "kind")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    room_id: str
    peer_id: str
    kind: str
    def __init__(self, tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ..., peer_id: _Optional[str] = ..., kind: _Optional[str] = ...) -> None: ...

class ListTracksResponse(_message.Message):
    __slots__ = ("tracks", "error")
    TRACKS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    tracks: _containers.RepeatedCompositeFieldContainer[_track_pb2.Track]
    error: _dto_pb2.ApiError
    def __init__(self, tracks: _Optional[_Iterable[_Union[_track_pb2.Track, _Mapping]]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class IceServer(_message.Message):
    __slots__ = ("urls", "username", "credential")
    URLS_FIELD_NUMBER: _ClassVar[int]
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    urls: _containers.RepeatedScalarFieldContainer[str]
    username: str
    credential: str
    def __init__(self, urls: _Optional[_Iterable[str]] = ..., username: _Optional[str] = ..., credential: _Optional[str] = ...) -> None: ...

class IssueCredentialsRequest(_message.Message):
    __slots__ = ("tenant_id", "room_id", "peer_id", "ttl_seconds")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    room_id: str
    peer_id: str
    ttl_seconds: int
    def __init__(self, tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ..., peer_id: _Optional[str] = ..., ttl_seconds: _Optional[int] = ...) -> None: ...

class IssueCredentialsResponse(_message.Message):
    __slots__ = ("ice_servers", "username", "credential", "ttl_seconds", "expires_at", "error", "allowed_action")
    ICE_SERVERS_FIELD_NUMBER: _ClassVar[int]
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_ACTION_FIELD_NUMBER: _ClassVar[int]
    ice_servers: _containers.RepeatedCompositeFieldContainer[IceServer]
    username: str
    credential: str
    ttl_seconds: int
    expires_at: _timestamp_pb2.Timestamp
    error: _dto_pb2.ApiError
    allowed_action: str
    def __init__(self, ice_servers: _Optional[_Iterable[_Union[IceServer, _Mapping]]] = ..., username: _Optional[str] = ..., credential: _Optional[str] = ..., ttl_seconds: _Optional[int] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., allowed_action: _Optional[str] = ...) -> None: ...

class SignalRequest(_message.Message):
    __slots__ = ("room_id", "peer_id", "tenant_id", "offer_sdp", "answer_sdp", "ice_candidate", "ping")
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    OFFER_SDP_FIELD_NUMBER: _ClassVar[int]
    ANSWER_SDP_FIELD_NUMBER: _ClassVar[int]
    ICE_CANDIDATE_FIELD_NUMBER: _ClassVar[int]
    PING_FIELD_NUMBER: _ClassVar[int]
    room_id: str
    peer_id: str
    tenant_id: str
    offer_sdp: str
    answer_sdp: str
    ice_candidate: str
    ping: bool
    def __init__(self, room_id: _Optional[str] = ..., peer_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., offer_sdp: _Optional[str] = ..., answer_sdp: _Optional[str] = ..., ice_candidate: _Optional[str] = ..., ping: bool = ...) -> None: ...

class SignalResponse(_message.Message):
    __slots__ = ("offer_sdp", "answer_sdp", "ice_candidate", "peer_joined", "peer_left", "track_published", "pong")
    OFFER_SDP_FIELD_NUMBER: _ClassVar[int]
    ANSWER_SDP_FIELD_NUMBER: _ClassVar[int]
    ICE_CANDIDATE_FIELD_NUMBER: _ClassVar[int]
    PEER_JOINED_FIELD_NUMBER: _ClassVar[int]
    PEER_LEFT_FIELD_NUMBER: _ClassVar[int]
    TRACK_PUBLISHED_FIELD_NUMBER: _ClassVar[int]
    PONG_FIELD_NUMBER: _ClassVar[int]
    offer_sdp: str
    answer_sdp: str
    ice_candidate: str
    peer_joined: str
    peer_left: str
    track_published: str
    pong: bool
    def __init__(self, offer_sdp: _Optional[str] = ..., answer_sdp: _Optional[str] = ..., ice_candidate: _Optional[str] = ..., peer_joined: _Optional[str] = ..., peer_left: _Optional[str] = ..., track_published: _Optional[str] = ..., pong: bool = ...) -> None: ...
