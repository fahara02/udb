import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TenantCreatedEvent(_message.Message):
    __slots__ = ("event_id", "tenant_id", "tenant_code", "tenant_name", "type", "correlation_id", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_CODE_FIELD_NUMBER: _ClassVar[int]
    TENANT_NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    tenant_id: str
    tenant_code: str
    tenant_name: str
    type: str
    correlation_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., tenant_code: _Optional[str] = ..., tenant_name: _Optional[str] = ..., type: _Optional[str] = ..., correlation_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class TenantStatusChangedEvent(_message.Message):
    __slots__ = ("event_id", "tenant_id", "old_status", "new_status", "correlation_id", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    OLD_STATUS_FIELD_NUMBER: _ClassVar[int]
    NEW_STATUS_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    tenant_id: str
    old_status: str
    new_status: str
    correlation_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., old_status: _Optional[str] = ..., new_status: _Optional[str] = ..., correlation_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
