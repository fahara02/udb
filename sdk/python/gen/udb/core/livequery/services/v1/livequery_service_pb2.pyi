from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LiveQueryComparison(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LIVE_QUERY_COMPARISON_UNSPECIFIED: _ClassVar[LiveQueryComparison]
    LIVE_QUERY_COMPARISON_EQ: _ClassVar[LiveQueryComparison]
    LIVE_QUERY_COMPARISON_NE: _ClassVar[LiveQueryComparison]
    LIVE_QUERY_COMPARISON_LT: _ClassVar[LiveQueryComparison]
    LIVE_QUERY_COMPARISON_LE: _ClassVar[LiveQueryComparison]
    LIVE_QUERY_COMPARISON_GT: _ClassVar[LiveQueryComparison]
    LIVE_QUERY_COMPARISON_GE: _ClassVar[LiveQueryComparison]

class LiveQueryChangeOp(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LIVE_QUERY_CHANGE_OP_UNSPECIFIED: _ClassVar[LiveQueryChangeOp]
    LIVE_QUERY_CHANGE_OP_INSERT: _ClassVar[LiveQueryChangeOp]
    LIVE_QUERY_CHANGE_OP_UPDATE: _ClassVar[LiveQueryChangeOp]
    LIVE_QUERY_CHANGE_OP_DELETE: _ClassVar[LiveQueryChangeOp]
LIVE_QUERY_COMPARISON_UNSPECIFIED: LiveQueryComparison
LIVE_QUERY_COMPARISON_EQ: LiveQueryComparison
LIVE_QUERY_COMPARISON_NE: LiveQueryComparison
LIVE_QUERY_COMPARISON_LT: LiveQueryComparison
LIVE_QUERY_COMPARISON_LE: LiveQueryComparison
LIVE_QUERY_COMPARISON_GT: LiveQueryComparison
LIVE_QUERY_COMPARISON_GE: LiveQueryComparison
LIVE_QUERY_CHANGE_OP_UNSPECIFIED: LiveQueryChangeOp
LIVE_QUERY_CHANGE_OP_INSERT: LiveQueryChangeOp
LIVE_QUERY_CHANGE_OP_UPDATE: LiveQueryChangeOp
LIVE_QUERY_CHANGE_OP_DELETE: LiveQueryChangeOp

class LiveQueryPredicate(_message.Message):
    __slots__ = ("field", "op", "value")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    OP_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    field: str
    op: LiveQueryComparison
    value: str
    def __init__(self, field: _Optional[str] = ..., op: _Optional[_Union[LiveQueryComparison, str]] = ..., value: _Optional[str] = ...) -> None: ...

class SubscribeRequest(_message.Message):
    __slots__ = ("tenant_id", "message_type", "filters", "project_id", "snapshot_limit")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_LIMIT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    message_type: str
    filters: _containers.RepeatedCompositeFieldContainer[LiveQueryPredicate]
    project_id: str
    snapshot_limit: int
    def __init__(self, tenant_id: _Optional[str] = ..., message_type: _Optional[str] = ..., filters: _Optional[_Iterable[_Union[LiveQueryPredicate, _Mapping]]] = ..., project_id: _Optional[str] = ..., snapshot_limit: _Optional[int] = ...) -> None: ...

class SubscribeResponse(_message.Message):
    __slots__ = ("snapshot", "change", "error")
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    CHANGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    snapshot: LiveQuerySnapshot
    change: LiveQueryChange
    error: _dto_pb2.ApiError
    def __init__(self, snapshot: _Optional[_Union[LiveQuerySnapshot, _Mapping]] = ..., change: _Optional[_Union[LiveQueryChange, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class LiveQuerySnapshot(_message.Message):
    __slots__ = ("rows_json", "row_count")
    ROWS_JSON_FIELD_NUMBER: _ClassVar[int]
    ROW_COUNT_FIELD_NUMBER: _ClassVar[int]
    rows_json: _containers.RepeatedScalarFieldContainer[str]
    row_count: int
    def __init__(self, rows_json: _Optional[_Iterable[str]] = ..., row_count: _Optional[int] = ...) -> None: ...

class LiveQueryChange(_message.Message):
    __slots__ = ("op", "row_json", "event_id")
    OP_FIELD_NUMBER: _ClassVar[int]
    ROW_JSON_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    op: LiveQueryChangeOp
    row_json: str
    event_id: str
    def __init__(self, op: _Optional[_Union[LiveQueryChangeOp, str]] = ..., row_json: _Optional[str] = ..., event_id: _Optional[str] = ...) -> None: ...
