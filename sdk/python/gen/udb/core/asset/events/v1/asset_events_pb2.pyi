import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AssetRegistered(_message.Message):
    __slots__ = ("event_id", "asset_id", "tenant_id", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    asset_id: str
    tenant_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., asset_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PipelineStarted(_message.Message):
    __slots__ = ("event_id", "instance_id", "asset_id", "tenant_id", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    instance_id: str
    asset_id: str
    tenant_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., instance_id: _Optional[str] = ..., asset_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class StepCompleted(_message.Message):
    __slots__ = ("event_id", "step_id", "instance_id", "tenant_id", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    step_id: str
    instance_id: str
    tenant_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., step_id: _Optional[str] = ..., instance_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PipelineCompleted(_message.Message):
    __slots__ = ("event_id", "instance_id", "asset_id", "tenant_id", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    instance_id: str
    asset_id: str
    tenant_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., instance_id: _Optional[str] = ..., asset_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PipelineFailed(_message.Message):
    __slots__ = ("event_id", "instance_id", "asset_id", "tenant_id", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    instance_id: str
    asset_id: str
    tenant_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., instance_id: _Optional[str] = ..., asset_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
