import datetime

from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CDCEnvelope(_message.Message):
    __slots__ = ("event_id", "topic", "partition_key", "payload_json", "published_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    PARTITION_KEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    PUBLISHED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    topic: str
    partition_key: str
    payload_json: str
    published_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., topic: _Optional[str] = ..., partition_key: _Optional[str] = ..., payload_json: _Optional[str] = ..., published_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class EventEnvelope(_message.Message):
    __slots__ = ("event_id", "event_type", "timestamp", "correlation_id", "document_id", "schema_uri", "payload")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_URI_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    event_type: str
    timestamp: _timestamp_pb2.Timestamp
    correlation_id: str
    document_id: str
    schema_uri: str
    payload: _struct_pb2.Struct
    def __init__(self, event_id: _Optional[str] = ..., event_type: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., correlation_id: _Optional[str] = ..., document_id: _Optional[str] = ..., schema_uri: _Optional[str] = ..., payload: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class DriftDetectedEvent(_message.Message):
    __slots__ = ("event_id", "schema_checksum_sha256", "blocked_operation_count", "report_json", "correlation_id", "occurred_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_CHECKSUM_SHA256_FIELD_NUMBER: _ClassVar[int]
    BLOCKED_OPERATION_COUNT_FIELD_NUMBER: _ClassVar[int]
    REPORT_JSON_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    schema_checksum_sha256: str
    blocked_operation_count: int
    report_json: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., schema_checksum_sha256: _Optional[str] = ..., blocked_operation_count: _Optional[int] = ..., report_json: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ProvisioningCompletedEvent(_message.Message):
    __slots__ = ("event_id", "schema_checksum_sha256", "applied_operation_count", "correlation_id", "occurred_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_CHECKSUM_SHA256_FIELD_NUMBER: _ClassVar[int]
    APPLIED_OPERATION_COUNT_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    schema_checksum_sha256: str
    applied_operation_count: int
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., schema_checksum_sha256: _Optional[str] = ..., applied_operation_count: _Optional[int] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
