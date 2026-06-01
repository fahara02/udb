from udb.entity.v1 import context_pb2 as _context_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CDCSubscriptionRequest(_message.Message):
    __slots__ = ("context", "topic_pattern", "since_event_id")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    TOPIC_PATTERN_FIELD_NUMBER: _ClassVar[int]
    SINCE_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    topic_pattern: str
    since_event_id: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., topic_pattern: _Optional[str] = ..., since_event_id: _Optional[str] = ...) -> None: ...

class CdcControlRequest(_message.Message):
    __slots__ = ("context", "slot_name", "reason")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    SLOT_NAME_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    slot_name: str
    reason: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., slot_name: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class CdcStatusResponse(_message.Message):
    __slots__ = ("slot_name", "is_leader", "paused", "pause_reason", "last_event_id", "lag_seconds", "outbox_depth", "updated_at_unix")
    SLOT_NAME_FIELD_NUMBER: _ClassVar[int]
    IS_LEADER_FIELD_NUMBER: _ClassVar[int]
    PAUSED_FIELD_NUMBER: _ClassVar[int]
    PAUSE_REASON_FIELD_NUMBER: _ClassVar[int]
    LAST_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    LAG_SECONDS_FIELD_NUMBER: _ClassVar[int]
    OUTBOX_DEPTH_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    slot_name: str
    is_leader: bool
    paused: bool
    pause_reason: str
    last_event_id: str
    lag_seconds: float
    outbox_depth: int
    updated_at_unix: int
    def __init__(self, slot_name: _Optional[str] = ..., is_leader: bool = ..., paused: bool = ..., pause_reason: _Optional[str] = ..., last_event_id: _Optional[str] = ..., lag_seconds: _Optional[float] = ..., outbox_depth: _Optional[int] = ..., updated_at_unix: _Optional[int] = ...) -> None: ...
