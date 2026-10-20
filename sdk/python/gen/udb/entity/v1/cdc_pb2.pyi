from udb.entity.v1 import context_pb2 as _context_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CdcConsumerStart(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CDC_CONSUMER_START_UNSPECIFIED: _ClassVar[CdcConsumerStart]
    CDC_CONSUMER_START_LATEST: _ClassVar[CdcConsumerStart]
    CDC_CONSUMER_START_EARLIEST: _ClassVar[CdcConsumerStart]
CDC_CONSUMER_START_UNSPECIFIED: CdcConsumerStart
CDC_CONSUMER_START_LATEST: CdcConsumerStart
CDC_CONSUMER_START_EARLIEST: CdcConsumerStart

class CDCSubscriptionRequest(_message.Message):
    __slots__ = ("context", "topic_pattern", "since_event_id", "consumer_name", "progress_markers", "start")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    TOPIC_PATTERN_FIELD_NUMBER: _ClassVar[int]
    SINCE_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    CONSUMER_NAME_FIELD_NUMBER: _ClassVar[int]
    PROGRESS_MARKERS_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    topic_pattern: str
    since_event_id: str
    consumer_name: str
    progress_markers: bool
    start: CdcConsumerStart
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., topic_pattern: _Optional[str] = ..., since_event_id: _Optional[str] = ..., consumer_name: _Optional[str] = ..., progress_markers: bool = ..., start: _Optional[_Union[CdcConsumerStart, str]] = ...) -> None: ...

class AckCdcEventsRequest(_message.Message):
    __slots__ = ("context", "consumer_name", "topic_pattern", "event_id")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    CONSUMER_NAME_FIELD_NUMBER: _ClassVar[int]
    TOPIC_PATTERN_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    consumer_name: str
    topic_pattern: str
    event_id: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., consumer_name: _Optional[str] = ..., topic_pattern: _Optional[str] = ..., event_id: _Optional[str] = ...) -> None: ...

class AckCdcEventsResponse(_message.Message):
    __slots__ = ("consumer_name", "topic_pattern", "event_id", "acked_at_unix")
    CONSUMER_NAME_FIELD_NUMBER: _ClassVar[int]
    TOPIC_PATTERN_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ACKED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    consumer_name: str
    topic_pattern: str
    event_id: str
    acked_at_unix: int
    def __init__(self, consumer_name: _Optional[str] = ..., topic_pattern: _Optional[str] = ..., event_id: _Optional[str] = ..., acked_at_unix: _Optional[int] = ...) -> None: ...

class NackCdcEventsRequest(_message.Message):
    __slots__ = ("context", "consumer_name", "topic_pattern", "event_id", "attempts", "failure_reason")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    CONSUMER_NAME_FIELD_NUMBER: _ClassVar[int]
    TOPIC_PATTERN_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    consumer_name: str
    topic_pattern: str
    event_id: str
    attempts: int
    failure_reason: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., consumer_name: _Optional[str] = ..., topic_pattern: _Optional[str] = ..., event_id: _Optional[str] = ..., attempts: _Optional[int] = ..., failure_reason: _Optional[str] = ...) -> None: ...

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
    __slots__ = ("slot_name", "is_leader", "paused", "pause_reason", "last_event_id", "lag_seconds", "outbox_depth", "updated_at_unix", "consumers")
    SLOT_NAME_FIELD_NUMBER: _ClassVar[int]
    IS_LEADER_FIELD_NUMBER: _ClassVar[int]
    PAUSED_FIELD_NUMBER: _ClassVar[int]
    PAUSE_REASON_FIELD_NUMBER: _ClassVar[int]
    LAST_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    LAG_SECONDS_FIELD_NUMBER: _ClassVar[int]
    OUTBOX_DEPTH_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    CONSUMERS_FIELD_NUMBER: _ClassVar[int]
    slot_name: str
    is_leader: bool
    paused: bool
    pause_reason: str
    last_event_id: str
    lag_seconds: float
    outbox_depth: int
    updated_at_unix: int
    consumers: _containers.RepeatedCompositeFieldContainer[CdcConsumerStatus]
    def __init__(self, slot_name: _Optional[str] = ..., is_leader: bool = ..., paused: bool = ..., pause_reason: _Optional[str] = ..., last_event_id: _Optional[str] = ..., lag_seconds: _Optional[float] = ..., outbox_depth: _Optional[int] = ..., updated_at_unix: _Optional[int] = ..., consumers: _Optional[_Iterable[_Union[CdcConsumerStatus, _Mapping]]] = ...) -> None: ...

class CdcConsumerStatus(_message.Message):
    __slots__ = ("consumer_name", "topic_pattern", "last_event_id", "lag_seconds", "acked_at_unix", "last_published_at_unix", "tenant_id", "project_id", "owner_identity", "owner_migration_required")
    CONSUMER_NAME_FIELD_NUMBER: _ClassVar[int]
    TOPIC_PATTERN_FIELD_NUMBER: _ClassVar[int]
    LAST_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    LAG_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ACKED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    LAST_PUBLISHED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    OWNER_MIGRATION_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    consumer_name: str
    topic_pattern: str
    last_event_id: str
    lag_seconds: float
    acked_at_unix: int
    last_published_at_unix: int
    tenant_id: str
    project_id: str
    owner_identity: str
    owner_migration_required: bool
    def __init__(self, consumer_name: _Optional[str] = ..., topic_pattern: _Optional[str] = ..., last_event_id: _Optional[str] = ..., lag_seconds: _Optional[float] = ..., acked_at_unix: _Optional[int] = ..., last_published_at_unix: _Optional[int] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., owner_identity: _Optional[str] = ..., owner_migration_required: bool = ...) -> None: ...
