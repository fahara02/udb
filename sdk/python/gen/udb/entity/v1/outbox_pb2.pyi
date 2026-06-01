from google.protobuf import struct_pb2 as _struct_pb2
from udb.entity.v1 import context_pb2 as _context_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EnqueueOutboxEventRequest(_message.Message):
    __slots__ = ("context", "topic", "partition_key", "payload", "schema_uri", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    PARTITION_KEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_URI_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    topic: str
    partition_key: str
    payload: _struct_pb2.Struct
    schema_uri: str
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., topic: _Optional[str] = ..., partition_key: _Optional[str] = ..., payload: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., schema_uri: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class EnqueueOutboxEventResponse(_message.Message):
    __slots__ = ("event_id", "enqueued", "was_duplicate")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ENQUEUED_FIELD_NUMBER: _ClassVar[int]
    WAS_DUPLICATE_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    enqueued: bool
    was_duplicate: bool
    def __init__(self, event_id: _Optional[str] = ..., enqueued: bool = ..., was_duplicate: bool = ...) -> None: ...
