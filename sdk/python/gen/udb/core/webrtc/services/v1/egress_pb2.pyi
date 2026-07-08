import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EgressStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EGRESS_STATUS_UNSPECIFIED: _ClassVar[EgressStatus]
    EGRESS_STATUS_STARTING: _ClassVar[EgressStatus]
    EGRESS_STATUS_ACTIVE: _ClassVar[EgressStatus]
    EGRESS_STATUS_STOPPING: _ClassVar[EgressStatus]
    EGRESS_STATUS_STOPPED: _ClassVar[EgressStatus]
    EGRESS_STATUS_FAILED: _ClassVar[EgressStatus]
EGRESS_STATUS_UNSPECIFIED: EgressStatus
EGRESS_STATUS_STARTING: EgressStatus
EGRESS_STATUS_ACTIVE: EgressStatus
EGRESS_STATUS_STOPPING: EgressStatus
EGRESS_STATUS_STOPPED: EgressStatus
EGRESS_STATUS_FAILED: EgressStatus

class EgressInfo(_message.Message):
    __slots__ = ("egress_id", "tenant_id", "room_id", "track_id", "kind", "status", "destination", "started_at", "stopped_at")
    EGRESS_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    TRACK_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    STOPPED_AT_FIELD_NUMBER: _ClassVar[int]
    egress_id: str
    tenant_id: str
    room_id: str
    track_id: str
    kind: str
    status: EgressStatus
    destination: str
    started_at: _timestamp_pb2.Timestamp
    stopped_at: _timestamp_pb2.Timestamp
    def __init__(self, egress_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ..., track_id: _Optional[str] = ..., kind: _Optional[str] = ..., status: _Optional[_Union[EgressStatus, str]] = ..., destination: _Optional[str] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., stopped_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class StartRoomCompositeRequest(_message.Message):
    __slots__ = ("tenant_id", "room_id", "format", "destination", "options")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    room_id: str
    format: str
    destination: str
    options: str
    def __init__(self, tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ..., format: _Optional[str] = ..., destination: _Optional[str] = ..., options: _Optional[str] = ...) -> None: ...

class StartRoomCompositeResponse(_message.Message):
    __slots__ = ("egress_id", "status", "message", "error")
    EGRESS_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    egress_id: str
    status: EgressStatus
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, egress_id: _Optional[str] = ..., status: _Optional[_Union[EgressStatus, str]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class StartTrackEgressRequest(_message.Message):
    __slots__ = ("tenant_id", "room_id", "track_id", "format", "destination", "options")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    TRACK_ID_FIELD_NUMBER: _ClassVar[int]
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    room_id: str
    track_id: str
    format: str
    destination: str
    options: str
    def __init__(self, tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ..., track_id: _Optional[str] = ..., format: _Optional[str] = ..., destination: _Optional[str] = ..., options: _Optional[str] = ...) -> None: ...

class StartTrackEgressResponse(_message.Message):
    __slots__ = ("egress_id", "status", "message", "error")
    EGRESS_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    egress_id: str
    status: EgressStatus
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, egress_id: _Optional[str] = ..., status: _Optional[_Union[EgressStatus, str]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class StopEgressRequest(_message.Message):
    __slots__ = ("tenant_id", "egress_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    EGRESS_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    egress_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., egress_id: _Optional[str] = ...) -> None: ...

class StopEgressResponse(_message.Message):
    __slots__ = ("egress_id", "status", "message", "error")
    EGRESS_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    egress_id: str
    status: EgressStatus
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, egress_id: _Optional[str] = ..., status: _Optional[_Union[EgressStatus, str]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListEgressRequest(_message.Message):
    __slots__ = ("tenant_id", "room_id", "status", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOM_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    room_id: str
    status: EgressStatus
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., room_id: _Optional[str] = ..., status: _Optional[_Union[EgressStatus, str]] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListEgressResponse(_message.Message):
    __slots__ = ("egresses", "total_count", "error", "next_page_token")
    EGRESSES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    egresses: _containers.RepeatedCompositeFieldContainer[EgressInfo]
    total_count: int
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, egresses: _Optional[_Iterable[_Union[EgressInfo, _Mapping]]] = ..., total_count: _Optional[int] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...
