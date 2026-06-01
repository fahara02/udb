import datetime

from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.entity.v1 import context_pb2 as _context_pb2
from udb.entity.v1 import operation_pb2 as _operation_pb2
from udb.entity.v1 import relational_pb2 as _relational_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CacheGetRequest(_message.Message):
    __slots__ = ("context", "resource", "key", "touch")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    TOUCH_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    resource: _operation_pb2.StoreResource
    key: str
    touch: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., resource: _Optional[_Union[_operation_pb2.StoreResource, _Mapping]] = ..., key: _Optional[str] = ..., touch: bool = ...) -> None: ...

class CacheGetResponse(_message.Message):
    __slots__ = ("found", "value", "content_type", "ttl_seconds", "metadata")
    FOUND_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    found: bool
    value: bytes
    content_type: str
    ttl_seconds: int
    metadata: _struct_pb2.Struct
    def __init__(self, found: bool = ..., value: _Optional[bytes] = ..., content_type: _Optional[str] = ..., ttl_seconds: _Optional[int] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class CacheSetRequest(_message.Message):
    __slots__ = ("context", "resource", "key", "value", "content_type", "ttl_seconds", "only_if_absent", "only_if_present", "idempotency_key", "metadata")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ONLY_IF_ABSENT_FIELD_NUMBER: _ClassVar[int]
    ONLY_IF_PRESENT_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    resource: _operation_pb2.StoreResource
    key: str
    value: bytes
    content_type: str
    ttl_seconds: int
    only_if_absent: bool
    only_if_present: bool
    idempotency_key: str
    metadata: _struct_pb2.Struct
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., resource: _Optional[_Union[_operation_pb2.StoreResource, _Mapping]] = ..., key: _Optional[str] = ..., value: _Optional[bytes] = ..., content_type: _Optional[str] = ..., ttl_seconds: _Optional[int] = ..., only_if_absent: bool = ..., only_if_present: bool = ..., idempotency_key: _Optional[str] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class CacheDeleteRequest(_message.Message):
    __slots__ = ("context", "resource", "key", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    resource: _operation_pb2.StoreResource
    key: str
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., resource: _Optional[_Union[_operation_pb2.StoreResource, _Mapping]] = ..., key: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class CacheScanRequest(_message.Message):
    __slots__ = ("context", "resource", "key_pattern", "limit", "page_token")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    KEY_PATTERN_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    resource: _operation_pb2.StoreResource
    key_pattern: str
    limit: int
    page_token: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., resource: _Optional[_Union[_operation_pb2.StoreResource, _Mapping]] = ..., key_pattern: _Optional[str] = ..., limit: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class CacheEntry(_message.Message):
    __slots__ = ("key", "value", "content_type", "ttl_seconds", "metadata")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    content_type: str
    ttl_seconds: int
    metadata: _struct_pb2.Struct
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ..., content_type: _Optional[str] = ..., ttl_seconds: _Optional[int] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class CacheScanResponse(_message.Message):
    __slots__ = ("entries", "next_page_token", "stats")
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[CacheEntry]
    next_page_token: str
    stats: _operation_pb2.OperationStats
    def __init__(self, entries: _Optional[_Iterable[_Union[CacheEntry, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., stats: _Optional[_Union[_operation_pb2.OperationStats, _Mapping]] = ...) -> None: ...

class DocumentGetRequest(_message.Message):
    __slots__ = ("context", "resource", "document_id", "fields")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    resource: _operation_pb2.StoreResource
    document_id: str
    fields: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., resource: _Optional[_Union[_operation_pb2.StoreResource, _Mapping]] = ..., document_id: _Optional[str] = ..., fields: _Optional[_Iterable[str]] = ...) -> None: ...

class DocumentFindRequest(_message.Message):
    __slots__ = ("context", "resource", "filter", "fields", "limit", "page_token", "sort")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    SORT_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    resource: _operation_pb2.StoreResource
    filter: _struct_pb2.Struct
    fields: _containers.RepeatedScalarFieldContainer[str]
    limit: int
    page_token: str
    sort: _containers.RepeatedCompositeFieldContainer[_relational_pb2.Sort]
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., resource: _Optional[_Union[_operation_pb2.StoreResource, _Mapping]] = ..., filter: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., fields: _Optional[_Iterable[str]] = ..., limit: _Optional[int] = ..., page_token: _Optional[str] = ..., sort: _Optional[_Iterable[_Union[_relational_pb2.Sort, _Mapping]]] = ...) -> None: ...

class DocumentUpsertRequest(_message.Message):
    __slots__ = ("context", "resource", "document_id", "document", "merge_fields", "replace", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    MERGE_FIELDS_FIELD_NUMBER: _ClassVar[int]
    REPLACE_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    resource: _operation_pb2.StoreResource
    document_id: str
    document: _struct_pb2.Struct
    merge_fields: _containers.RepeatedScalarFieldContainer[str]
    replace: bool
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., resource: _Optional[_Union[_operation_pb2.StoreResource, _Mapping]] = ..., document_id: _Optional[str] = ..., document: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., merge_fields: _Optional[_Iterable[str]] = ..., replace: bool = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class DocumentDeleteRequest(_message.Message):
    __slots__ = ("context", "resource", "document_id", "filter", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    resource: _operation_pb2.StoreResource
    document_id: str
    filter: _struct_pb2.Struct
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., resource: _Optional[_Union[_operation_pb2.StoreResource, _Mapping]] = ..., document_id: _Optional[str] = ..., filter: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class DocumentSet(_message.Message):
    __slots__ = ("documents", "next_page_token", "stats")
    DOCUMENTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    documents: _containers.RepeatedCompositeFieldContainer[_struct_pb2.Struct]
    next_page_token: str
    stats: _operation_pb2.OperationStats
    def __init__(self, documents: _Optional[_Iterable[_Union[_struct_pb2.Struct, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., stats: _Optional[_Union[_operation_pb2.OperationStats, _Mapping]] = ...) -> None: ...

class GraphQueryRequest(_message.Message):
    __slots__ = ("context", "resource", "query", "parameters", "limit", "page_token", "read_only")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    READ_ONLY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    resource: _operation_pb2.StoreResource
    query: str
    parameters: _struct_pb2.Struct
    limit: int
    page_token: str
    read_only: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., resource: _Optional[_Union[_operation_pb2.StoreResource, _Mapping]] = ..., query: _Optional[str] = ..., parameters: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., limit: _Optional[int] = ..., page_token: _Optional[str] = ..., read_only: bool = ...) -> None: ...

class GraphMutationRequest(_message.Message):
    __slots__ = ("context", "resource", "query", "parameters", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    resource: _operation_pb2.StoreResource
    query: str
    parameters: _struct_pb2.Struct
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., resource: _Optional[_Union[_operation_pb2.StoreResource, _Mapping]] = ..., query: _Optional[str] = ..., parameters: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class GraphResultSet(_message.Message):
    __slots__ = ("records", "next_page_token", "stats")
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[_struct_pb2.Struct]
    next_page_token: str
    stats: _operation_pb2.OperationStats
    def __init__(self, records: _Optional[_Iterable[_Union[_struct_pb2.Struct, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., stats: _Optional[_Union[_operation_pb2.OperationStats, _Mapping]] = ...) -> None: ...

class TimeSeriesPoint(_message.Message):
    __slots__ = ("timestamp", "tags", "values", "fields")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class ValuesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    timestamp: _timestamp_pb2.Timestamp
    tags: _containers.ScalarMap[str, str]
    values: _containers.ScalarMap[str, float]
    fields: _struct_pb2.Struct
    def __init__(self, timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tags: _Optional[_Mapping[str, str]] = ..., values: _Optional[_Mapping[str, float]] = ..., fields: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class TimeSeriesWriteRequest(_message.Message):
    __slots__ = ("context", "resource", "points", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    POINTS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    resource: _operation_pb2.StoreResource
    points: _containers.RepeatedCompositeFieldContainer[TimeSeriesPoint]
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., resource: _Optional[_Union[_operation_pb2.StoreResource, _Mapping]] = ..., points: _Optional[_Iterable[_Union[TimeSeriesPoint, _Mapping]]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class TimeSeriesQueryRequest(_message.Message):
    __slots__ = ("context", "resource", "to", "filter", "fields", "group_by", "aggregate", "window", "limit", "page_token")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    FROM_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_FIELD_NUMBER: _ClassVar[int]
    WINDOW_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    resource: _operation_pb2.StoreResource
    to: _timestamp_pb2.Timestamp
    filter: _struct_pb2.Struct
    fields: _containers.RepeatedScalarFieldContainer[str]
    group_by: str
    aggregate: str
    window: str
    limit: int
    page_token: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., resource: _Optional[_Union[_operation_pb2.StoreResource, _Mapping]] = ..., to: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., filter: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., fields: _Optional[_Iterable[str]] = ..., group_by: _Optional[str] = ..., aggregate: _Optional[str] = ..., window: _Optional[str] = ..., limit: _Optional[int] = ..., page_token: _Optional[str] = ..., **kwargs) -> None: ...

class TimeSeriesQueryResponse(_message.Message):
    __slots__ = ("points", "next_page_token", "stats")
    POINTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedCompositeFieldContainer[TimeSeriesPoint]
    next_page_token: str
    stats: _operation_pb2.OperationStats
    def __init__(self, points: _Optional[_Iterable[_Union[TimeSeriesPoint, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., stats: _Optional[_Union[_operation_pb2.OperationStats, _Mapping]] = ...) -> None: ...

class AnalyticalQueryRequest(_message.Message):
    __slots__ = ("context", "resource", "query", "parameters", "limit", "page_token", "dry_run")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    DRY_RUN_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    resource: _operation_pb2.StoreResource
    query: str
    parameters: _struct_pb2.Struct
    limit: int
    page_token: str
    dry_run: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., resource: _Optional[_Union[_operation_pb2.StoreResource, _Mapping]] = ..., query: _Optional[str] = ..., parameters: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., limit: _Optional[int] = ..., page_token: _Optional[str] = ..., dry_run: bool = ...) -> None: ...

class AnalyticalQueryResponse(_message.Message):
    __slots__ = ("rows", "next_page_token", "stats", "warnings")
    ROWS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    WARNINGS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[_relational_pb2.Row]
    next_page_token: str
    stats: _operation_pb2.OperationStats
    warnings: _containers.RepeatedCompositeFieldContainer[_operation_pb2.OperationWarning]
    def __init__(self, rows: _Optional[_Iterable[_Union[_relational_pb2.Row, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., stats: _Optional[_Union[_operation_pb2.OperationStats, _Mapping]] = ..., warnings: _Optional[_Iterable[_Union[_operation_pb2.OperationWarning, _Mapping]]] = ...) -> None: ...
