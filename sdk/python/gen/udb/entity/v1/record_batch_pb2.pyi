from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ColumnType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COLUMN_TYPE_UNSPECIFIED: _ClassVar[ColumnType]
    COLUMN_TYPE_NULL: _ClassVar[ColumnType]
    COLUMN_TYPE_BOOL: _ClassVar[ColumnType]
    COLUMN_TYPE_INT64: _ClassVar[ColumnType]
    COLUMN_TYPE_DOUBLE: _ClassVar[ColumnType]
    COLUMN_TYPE_STRING: _ClassVar[ColumnType]
    COLUMN_TYPE_BYTES: _ClassVar[ColumnType]
    COLUMN_TYPE_JSON: _ClassVar[ColumnType]
COLUMN_TYPE_UNSPECIFIED: ColumnType
COLUMN_TYPE_NULL: ColumnType
COLUMN_TYPE_BOOL: ColumnType
COLUMN_TYPE_INT64: ColumnType
COLUMN_TYPE_DOUBLE: ColumnType
COLUMN_TYPE_STRING: ColumnType
COLUMN_TYPE_BYTES: ColumnType
COLUMN_TYPE_JSON: ColumnType

class ColumnBatch(_message.Message):
    __slots__ = ("name", "type", "bool_values", "int64_values", "double_values", "string_values", "bytes_values", "json_values", "nulls")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    BOOL_VALUES_FIELD_NUMBER: _ClassVar[int]
    INT64_VALUES_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_VALUES_FIELD_NUMBER: _ClassVar[int]
    STRING_VALUES_FIELD_NUMBER: _ClassVar[int]
    BYTES_VALUES_FIELD_NUMBER: _ClassVar[int]
    JSON_VALUES_FIELD_NUMBER: _ClassVar[int]
    NULLS_FIELD_NUMBER: _ClassVar[int]
    name: str
    type: ColumnType
    bool_values: _containers.RepeatedScalarFieldContainer[bool]
    int64_values: _containers.RepeatedScalarFieldContainer[int]
    double_values: _containers.RepeatedScalarFieldContainer[float]
    string_values: _containers.RepeatedScalarFieldContainer[str]
    bytes_values: _containers.RepeatedScalarFieldContainer[bytes]
    json_values: _containers.RepeatedScalarFieldContainer[str]
    nulls: _containers.RepeatedScalarFieldContainer[bool]
    def __init__(self, name: _Optional[str] = ..., type: _Optional[_Union[ColumnType, str]] = ..., bool_values: _Optional[_Iterable[bool]] = ..., int64_values: _Optional[_Iterable[int]] = ..., double_values: _Optional[_Iterable[float]] = ..., string_values: _Optional[_Iterable[str]] = ..., bytes_values: _Optional[_Iterable[bytes]] = ..., json_values: _Optional[_Iterable[str]] = ..., nulls: _Optional[_Iterable[bool]] = ...) -> None: ...

class RecordBatchV2(_message.Message):
    __slots__ = ("columns", "row_count", "schema_version", "field_order", "next_page_token", "total_count")
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    ROW_COUNT_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    FIELD_ORDER_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedCompositeFieldContainer[ColumnBatch]
    row_count: int
    schema_version: str
    field_order: _containers.RepeatedScalarFieldContainer[str]
    next_page_token: str
    total_count: int
    def __init__(self, columns: _Optional[_Iterable[_Union[ColumnBatch, _Mapping]]] = ..., row_count: _Optional[int] = ..., schema_version: _Optional[str] = ..., field_order: _Optional[_Iterable[str]] = ..., next_page_token: _Optional[str] = ..., total_count: _Optional[int] = ...) -> None: ...
