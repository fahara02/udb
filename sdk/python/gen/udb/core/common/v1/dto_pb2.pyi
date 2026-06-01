from google.protobuf import any_pb2 as _any_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PageRequest(_message.Message):
    __slots__ = ("page", "page_size", "page_token")
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    page: int
    page_size: int
    page_token: str
    def __init__(self, page: _Optional[int] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class PageResponse(_message.Message):
    __slots__ = ("page", "page_size", "total_items", "total_pages", "next_page_token", "total_count", "has_next", "has_previous")
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ITEMS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_PAGES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    HAS_NEXT_FIELD_NUMBER: _ClassVar[int]
    HAS_PREVIOUS_FIELD_NUMBER: _ClassVar[int]
    page: int
    page_size: int
    total_items: int
    total_pages: int
    next_page_token: str
    total_count: int
    has_next: bool
    has_previous: bool
    def __init__(self, page: _Optional[int] = ..., page_size: _Optional[int] = ..., total_items: _Optional[int] = ..., total_pages: _Optional[int] = ..., next_page_token: _Optional[str] = ..., total_count: _Optional[int] = ..., has_next: bool = ..., has_previous: bool = ...) -> None: ...

class ApiResponse(_message.Message):
    __slots__ = ("success", "data", "error", "meta")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    success: bool
    data: _any_pb2.Any
    error: ApiError
    meta: ResponseMeta
    def __init__(self, success: bool = ..., data: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., error: _Optional[_Union[ApiError, _Mapping]] = ..., meta: _Optional[_Union[ResponseMeta, _Mapping]] = ...) -> None: ...

class ApiError(_message.Message):
    __slots__ = ("code", "message", "error_id", "http_status_code", "retryable", "field_violations")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_ID_FIELD_NUMBER: _ClassVar[int]
    HTTP_STATUS_CODE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    FIELD_VIOLATIONS_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    error_id: str
    http_status_code: int
    retryable: bool
    field_violations: _containers.RepeatedCompositeFieldContainer[FieldViolation]
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ..., error_id: _Optional[str] = ..., http_status_code: _Optional[int] = ..., retryable: bool = ..., field_violations: _Optional[_Iterable[_Union[FieldViolation, _Mapping]]] = ...) -> None: ...

class FieldViolation(_message.Message):
    __slots__ = ("field", "description")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    field: str
    description: str
    def __init__(self, field: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class ResponseMeta(_message.Message):
    __slots__ = ("request_id", "timestamp", "pagination")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    timestamp: str
    pagination: PaginationMeta
    def __init__(self, request_id: _Optional[str] = ..., timestamp: _Optional[str] = ..., pagination: _Optional[_Union[PaginationMeta, _Mapping]] = ...) -> None: ...

class PaginationMeta(_message.Message):
    __slots__ = ("page", "page_size", "total_count", "total_pages", "has_next", "has_prev")
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_PAGES_FIELD_NUMBER: _ClassVar[int]
    HAS_NEXT_FIELD_NUMBER: _ClassVar[int]
    HAS_PREV_FIELD_NUMBER: _ClassVar[int]
    page: int
    page_size: int
    total_count: int
    total_pages: int
    has_next: bool
    has_prev: bool
    def __init__(self, page: _Optional[int] = ..., page_size: _Optional[int] = ..., total_count: _Optional[int] = ..., total_pages: _Optional[int] = ..., has_next: bool = ..., has_prev: bool = ...) -> None: ...

class RawJsonResponse(_message.Message):
    __slots__ = ("success", "data_json", "error", "meta")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    DATA_JSON_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    success: bool
    data_json: bytes
    error: ApiError
    meta: ResponseMeta
    def __init__(self, success: bool = ..., data_json: _Optional[bytes] = ..., error: _Optional[_Union[ApiError, _Mapping]] = ..., meta: _Optional[_Union[ResponseMeta, _Mapping]] = ...) -> None: ...

class ErrorInfo(_message.Message):
    __slots__ = ("code", "message", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    code: int
    message: str
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, code: _Optional[int] = ..., message: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...
