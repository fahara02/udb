from google.protobuf import struct_pb2 as _struct_pb2
from udb.entity.v1 import context_pb2 as _context_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Sort(_message.Message):
    __slots__ = ("field", "descending")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    DESCENDING_FIELD_NUMBER: _ClassVar[int]
    field: str
    descending: bool
    def __init__(self, field: _Optional[str] = ..., descending: bool = ...) -> None: ...

class CacheOptions(_message.Message):
    __slots__ = ("bypass_read", "bypass_write", "ttl_seconds")
    BYPASS_READ_FIELD_NUMBER: _ClassVar[int]
    BYPASS_WRITE_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    bypass_read: bool
    bypass_write: bool
    ttl_seconds: int
    def __init__(self, bypass_read: bool = ..., bypass_write: bool = ..., ttl_seconds: _Optional[int] = ...) -> None: ...

class Row(_message.Message):
    __slots__ = ("fields",)
    class FieldsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _struct_pb2.Value
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_struct_pb2.Value, _Mapping]] = ...) -> None: ...
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    fields: _containers.MessageMap[str, _struct_pb2.Value]
    def __init__(self, fields: _Optional[_Mapping[str, _struct_pb2.Value]] = ...) -> None: ...

class RecordSet(_message.Message):
    __slots__ = ("records_json", "rows", "next_page_token", "total_count", "record_revisions")
    RECORDS_JSON_FIELD_NUMBER: _ClassVar[int]
    ROWS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    RECORD_REVISIONS_FIELD_NUMBER: _ClassVar[int]
    records_json: _containers.RepeatedScalarFieldContainer[bytes]
    rows: _containers.RepeatedCompositeFieldContainer[Row]
    next_page_token: str
    total_count: int
    record_revisions: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, records_json: _Optional[_Iterable[bytes]] = ..., rows: _Optional[_Iterable[_Union[Row, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., total_count: _Optional[int] = ..., record_revisions: _Optional[_Iterable[str]] = ...) -> None: ...

class SelectRequest(_message.Message):
    __slots__ = ("context", "message_type", "filter", "fields", "limit", "page_token", "sort", "cache", "include_revision")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    SORT_FIELD_NUMBER: _ClassVar[int]
    CACHE_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_REVISION_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    message_type: str
    filter: _struct_pb2.Struct
    fields: _containers.RepeatedScalarFieldContainer[str]
    limit: int
    page_token: str
    sort: _containers.RepeatedCompositeFieldContainer[Sort]
    cache: CacheOptions
    include_revision: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., message_type: _Optional[str] = ..., filter: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., fields: _Optional[_Iterable[str]] = ..., limit: _Optional[int] = ..., page_token: _Optional[str] = ..., sort: _Optional[_Iterable[_Union[Sort, _Mapping]]] = ..., cache: _Optional[_Union[CacheOptions, _Mapping]] = ..., include_revision: bool = ...) -> None: ...

class UpsertRequest(_message.Message):
    __slots__ = ("context", "message_type", "record_json", "payload", "conflict_fields", "return_record", "cache", "idempotency_key", "expected", "lock_name", "fencing_token")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RECORD_JSON_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    CONFLICT_FIELDS_FIELD_NUMBER: _ClassVar[int]
    RETURN_RECORD_FIELD_NUMBER: _ClassVar[int]
    CACHE_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    LOCK_NAME_FIELD_NUMBER: _ClassVar[int]
    FENCING_TOKEN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    message_type: str
    record_json: bytes
    payload: _struct_pb2.Struct
    conflict_fields: _containers.RepeatedScalarFieldContainer[str]
    return_record: bool
    cache: CacheOptions
    idempotency_key: str
    expected: _struct_pb2.Struct
    lock_name: str
    fencing_token: int
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., message_type: _Optional[str] = ..., record_json: _Optional[bytes] = ..., payload: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., conflict_fields: _Optional[_Iterable[str]] = ..., return_record: bool = ..., cache: _Optional[_Union[CacheOptions, _Mapping]] = ..., idempotency_key: _Optional[str] = ..., expected: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., lock_name: _Optional[str] = ..., fencing_token: _Optional[int] = ...) -> None: ...

class DeleteRequest(_message.Message):
    __slots__ = ("context", "message_type", "filter", "idempotency_key", "expected", "expected_revision", "lock_name", "fencing_token")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_REVISION_FIELD_NUMBER: _ClassVar[int]
    LOCK_NAME_FIELD_NUMBER: _ClassVar[int]
    FENCING_TOKEN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    message_type: str
    filter: _struct_pb2.Struct
    idempotency_key: str
    expected: _struct_pb2.Struct
    expected_revision: str
    lock_name: str
    fencing_token: int
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., message_type: _Optional[str] = ..., filter: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., idempotency_key: _Optional[str] = ..., expected: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., expected_revision: _Optional[str] = ..., lock_name: _Optional[str] = ..., fencing_token: _Optional[int] = ...) -> None: ...

class UpdateRequest(_message.Message):
    __slots__ = ("context", "message_type", "filter", "changes", "expected", "increments", "idempotency_key", "return_record", "expected_revision", "lock_name", "fencing_token")
    class Increment(_message.Message):
        __slots__ = ("column", "delta")
        COLUMN_FIELD_NUMBER: _ClassVar[int]
        DELTA_FIELD_NUMBER: _ClassVar[int]
        column: str
        delta: float
        def __init__(self, column: _Optional[str] = ..., delta: _Optional[float] = ...) -> None: ...
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    CHANGES_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    INCREMENTS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    RETURN_RECORD_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_REVISION_FIELD_NUMBER: _ClassVar[int]
    LOCK_NAME_FIELD_NUMBER: _ClassVar[int]
    FENCING_TOKEN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    message_type: str
    filter: _struct_pb2.Struct
    changes: _struct_pb2.Struct
    expected: _struct_pb2.Struct
    increments: _containers.RepeatedCompositeFieldContainer[UpdateRequest.Increment]
    idempotency_key: str
    return_record: bool
    expected_revision: str
    lock_name: str
    fencing_token: int
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., message_type: _Optional[str] = ..., filter: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., changes: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., expected: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., increments: _Optional[_Iterable[_Union[UpdateRequest.Increment, _Mapping]]] = ..., idempotency_key: _Optional[str] = ..., return_record: bool = ..., expected_revision: _Optional[str] = ..., lock_name: _Optional[str] = ..., fencing_token: _Optional[int] = ...) -> None: ...

class ViewDefinition(_message.Message):
    __slots__ = ("context", "schema", "name", "query", "with_data", "ttl_days")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    WITH_DATA_FIELD_NUMBER: _ClassVar[int]
    TTL_DAYS_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    schema: str
    name: str
    query: str
    with_data: bool
    ttl_days: int
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., schema: _Optional[str] = ..., name: _Optional[str] = ..., query: _Optional[str] = ..., with_data: bool = ..., ttl_days: _Optional[int] = ...) -> None: ...

class BulkCasItem(_message.Message):
    __slots__ = ("filter", "changes", "expected_revision", "expected", "increments")
    FILTER_FIELD_NUMBER: _ClassVar[int]
    CHANGES_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_REVISION_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    INCREMENTS_FIELD_NUMBER: _ClassVar[int]
    filter: _struct_pb2.Struct
    changes: _struct_pb2.Struct
    expected_revision: str
    expected: _struct_pb2.Struct
    increments: _containers.RepeatedCompositeFieldContainer[UpdateRequest.Increment]
    def __init__(self, filter: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., changes: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., expected_revision: _Optional[str] = ..., expected: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., increments: _Optional[_Iterable[_Union[UpdateRequest.Increment, _Mapping]]] = ...) -> None: ...

class BulkCasRequest(_message.Message):
    __slots__ = ("context", "message_type", "items", "idempotency_key", "max_rows")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    MAX_ROWS_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    message_type: str
    items: _containers.RepeatedCompositeFieldContainer[BulkCasItem]
    idempotency_key: str
    max_rows: int
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., message_type: _Optional[str] = ..., items: _Optional[_Iterable[_Union[BulkCasItem, _Mapping]]] = ..., idempotency_key: _Optional[str] = ..., max_rows: _Optional[int] = ...) -> None: ...

class BulkCasItemResult(_message.Message):
    __slots__ = ("matched", "changed", "conflicted", "revision")
    MATCHED_FIELD_NUMBER: _ClassVar[int]
    CHANGED_FIELD_NUMBER: _ClassVar[int]
    CONFLICTED_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    matched: bool
    changed: bool
    conflicted: bool
    revision: str
    def __init__(self, matched: bool = ..., changed: bool = ..., conflicted: bool = ..., revision: _Optional[str] = ...) -> None: ...

class BulkCasResponse(_message.Message):
    __slots__ = ("matched", "changed", "conflicted", "write_receipt_json", "results")
    MATCHED_FIELD_NUMBER: _ClassVar[int]
    CHANGED_FIELD_NUMBER: _ClassVar[int]
    CONFLICTED_FIELD_NUMBER: _ClassVar[int]
    WRITE_RECEIPT_JSON_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    matched: int
    changed: int
    conflicted: int
    write_receipt_json: str
    results: _containers.RepeatedCompositeFieldContainer[BulkCasItemResult]
    def __init__(self, matched: _Optional[int] = ..., changed: _Optional[int] = ..., conflicted: _Optional[int] = ..., write_receipt_json: _Optional[str] = ..., results: _Optional[_Iterable[_Union[BulkCasItemResult, _Mapping]]] = ...) -> None: ...
