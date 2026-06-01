from udb.entity.v1 import context_pb2 as _context_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Chunk(_message.Message):
    __slots__ = ("context", "bucket", "object_key", "data", "final_chunk", "content_type", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    FINAL_CHUNK_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    bucket: str
    object_key: str
    data: bytes
    final_chunk: bool
    content_type: str
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., bucket: _Optional[str] = ..., object_key: _Optional[str] = ..., data: _Optional[bytes] = ..., final_chunk: bool = ..., content_type: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class ObjectRequest(_message.Message):
    __slots__ = ("context", "bucket", "object_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    bucket: str
    object_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., bucket: _Optional[str] = ..., object_key: _Optional[str] = ...) -> None: ...

class UrlRequest(_message.Message):
    __slots__ = ("context", "bucket", "object_key", "method", "ttl_seconds", "content_type")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    bucket: str
    object_key: str
    method: str
    ttl_seconds: int
    content_type: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., bucket: _Optional[str] = ..., object_key: _Optional[str] = ..., method: _Optional[str] = ..., ttl_seconds: _Optional[int] = ..., content_type: _Optional[str] = ...) -> None: ...

class UrlResponse(_message.Message):
    __slots__ = ("url", "expires_at_unix")
    URL_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    url: str
    expires_at_unix: int
    def __init__(self, url: _Optional[str] = ..., expires_at_unix: _Optional[int] = ...) -> None: ...

class MultipartUploadRequest(_message.Message):
    __slots__ = ("context", "bucket", "object_key", "content_type", "part_count", "ttl_seconds", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    PART_COUNT_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    bucket: str
    object_key: str
    content_type: str
    part_count: int
    ttl_seconds: int
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., bucket: _Optional[str] = ..., object_key: _Optional[str] = ..., content_type: _Optional[str] = ..., part_count: _Optional[int] = ..., ttl_seconds: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class MultipartUploadResponse(_message.Message):
    __slots__ = ("upload_id", "part_urls", "expires_at_unix")
    UPLOAD_ID_FIELD_NUMBER: _ClassVar[int]
    PART_URLS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    upload_id: str
    part_urls: _containers.RepeatedScalarFieldContainer[str]
    expires_at_unix: int
    def __init__(self, upload_id: _Optional[str] = ..., part_urls: _Optional[_Iterable[str]] = ..., expires_at_unix: _Optional[int] = ...) -> None: ...
