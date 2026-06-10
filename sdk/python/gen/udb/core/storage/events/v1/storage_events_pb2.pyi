import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FileUploadUrlIssued(_message.Message):
    __slots__ = ("event_id", "file_id", "tenant_id", "object_key", "upload_url", "expires_at", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    UPLOAD_URL_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    file_id: str
    tenant_id: str
    object_key: str
    upload_url: str
    expires_at: _timestamp_pb2.Timestamp
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., file_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., object_key: _Optional[str] = ..., upload_url: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class FileUploaded(_message.Message):
    __slots__ = ("event_id", "file_id", "tenant_id", "object_key", "size_bytes", "uploaded_by", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    UPLOADED_BY_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    file_id: str
    tenant_id: str
    object_key: str
    size_bytes: int
    uploaded_by: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., file_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., object_key: _Optional[str] = ..., size_bytes: _Optional[int] = ..., uploaded_by: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class FileFinalized(_message.Message):
    __slots__ = ("event_id", "file_id", "tenant_id", "content_type", "file_type", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    file_id: str
    tenant_id: str
    content_type: str
    file_type: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., file_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., content_type: _Optional[str] = ..., file_type: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class FileMetadataUpdated(_message.Message):
    __slots__ = ("event_id", "file_id", "tenant_id", "filename", "is_public", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    IS_PUBLIC_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    file_id: str
    tenant_id: str
    filename: str
    is_public: bool
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., file_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., filename: _Optional[str] = ..., is_public: bool = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class FileDeleted(_message.Message):
    __slots__ = ("event_id", "file_id", "tenant_id", "deleted_by", "timestamp")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    file_id: str
    tenant_id: str
    deleted_by: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., file_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., deleted_by: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
