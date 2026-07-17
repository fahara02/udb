import datetime

from google.api import annotations_pb2 as _annotations_pb2
from google.protobuf import field_mask_pb2 as _field_mask_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.storage.entity.v1 import file_pb2 as _file_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RegisterUploadRequest(_message.Message):
    __slots__ = ("tenant_id", "project_id", "filename", "content_type", "file_type", "reference_id", "reference_type", "is_public", "expires_in_minutes", "size_bytes")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_ID_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    IS_PUBLIC_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_IN_MINUTES_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    project_id: str
    filename: str
    content_type: str
    file_type: str
    reference_id: str
    reference_type: str
    is_public: bool
    expires_in_minutes: int
    size_bytes: int
    def __init__(self, tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., filename: _Optional[str] = ..., content_type: _Optional[str] = ..., file_type: _Optional[str] = ..., reference_id: _Optional[str] = ..., reference_type: _Optional[str] = ..., is_public: bool = ..., expires_in_minutes: _Optional[int] = ..., size_bytes: _Optional[int] = ...) -> None: ...

class RegisterUploadResponse(_message.Message):
    __slots__ = ("file_id", "upload_url", "object_key", "error", "expires_at")
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    UPLOAD_URL_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    file_id: str
    upload_url: str
    object_key: str
    error: _dto_pb2.ApiError
    expires_at: int
    def __init__(self, file_id: _Optional[str] = ..., upload_url: _Optional[str] = ..., object_key: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., expires_at: _Optional[int] = ...) -> None: ...

class ReissueUploadUrlRequest(_message.Message):
    __slots__ = ("tenant_id", "file_id", "expires_in_minutes")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_IN_MINUTES_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    file_id: str
    expires_in_minutes: int
    def __init__(self, tenant_id: _Optional[str] = ..., file_id: _Optional[str] = ..., expires_in_minutes: _Optional[int] = ...) -> None: ...

class ReissueUploadUrlResponse(_message.Message):
    __slots__ = ("file_id", "upload_url", "object_key", "error", "expires_at")
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    UPLOAD_URL_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    file_id: str
    upload_url: str
    object_key: str
    error: _dto_pb2.ApiError
    expires_at: int
    def __init__(self, file_id: _Optional[str] = ..., upload_url: _Optional[str] = ..., object_key: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., expires_at: _Optional[int] = ...) -> None: ...

class FinalizeUploadRequest(_message.Message):
    __slots__ = ("tenant_id", "file_id", "content_type", "file_type", "reference_id", "reference_type", "is_public", "size_bytes", "checksum", "etag")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_ID_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    IS_PUBLIC_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    ETAG_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    file_id: str
    content_type: str
    file_type: str
    reference_id: str
    reference_type: str
    is_public: bool
    size_bytes: int
    checksum: str
    etag: str
    def __init__(self, tenant_id: _Optional[str] = ..., file_id: _Optional[str] = ..., content_type: _Optional[str] = ..., file_type: _Optional[str] = ..., reference_id: _Optional[str] = ..., reference_type: _Optional[str] = ..., is_public: bool = ..., size_bytes: _Optional[int] = ..., checksum: _Optional[str] = ..., etag: _Optional[str] = ...) -> None: ...

class FinalizeUploadResponse(_message.Message):
    __slots__ = ("file", "error")
    FILE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    file: _file_pb2.File
    error: _dto_pb2.ApiError
    def __init__(self, file: _Optional[_Union[_file_pb2.File, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetDownloadUrlRequest(_message.Message):
    __slots__ = ("tenant_id", "file_id", "expires_in_minutes")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_IN_MINUTES_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    file_id: str
    expires_in_minutes: int
    def __init__(self, tenant_id: _Optional[str] = ..., file_id: _Optional[str] = ..., expires_in_minutes: _Optional[int] = ...) -> None: ...

class GetDownloadUrlResponse(_message.Message):
    __slots__ = ("download_url", "expires_at", "error")
    DOWNLOAD_URL_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    download_url: str
    expires_at: _timestamp_pb2.Timestamp
    error: _dto_pb2.ApiError
    def __init__(self, download_url: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class DownloadFileRequest(_message.Message):
    __slots__ = ("tenant_id", "file_id", "chunk_size_bytes")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    CHUNK_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    file_id: str
    chunk_size_bytes: int
    def __init__(self, tenant_id: _Optional[str] = ..., file_id: _Optional[str] = ..., chunk_size_bytes: _Optional[int] = ...) -> None: ...

class DownloadFileChunk(_message.Message):
    __slots__ = ("data", "content_type", "total_size", "etag")
    DATA_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_SIZE_FIELD_NUMBER: _ClassVar[int]
    ETAG_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    content_type: str
    total_size: int
    etag: str
    def __init__(self, data: _Optional[bytes] = ..., content_type: _Optional[str] = ..., total_size: _Optional[int] = ..., etag: _Optional[str] = ...) -> None: ...

class GetFileRequest(_message.Message):
    __slots__ = ("tenant_id", "file_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    file_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., file_id: _Optional[str] = ...) -> None: ...

class GetFileResponse(_message.Message):
    __slots__ = ("file", "error")
    FILE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    file: _file_pb2.File
    error: _dto_pb2.ApiError
    def __init__(self, file: _Optional[_Union[_file_pb2.File, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class UpdateFileRequest(_message.Message):
    __slots__ = ("tenant_id", "file_id", "filename", "content_type", "file_type", "reference_id", "reference_type", "is_public", "update_mask")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_ID_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    IS_PUBLIC_FIELD_NUMBER: _ClassVar[int]
    UPDATE_MASK_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    file_id: str
    filename: str
    content_type: str
    file_type: str
    reference_id: str
    reference_type: str
    is_public: bool
    update_mask: _field_mask_pb2.FieldMask
    def __init__(self, tenant_id: _Optional[str] = ..., file_id: _Optional[str] = ..., filename: _Optional[str] = ..., content_type: _Optional[str] = ..., file_type: _Optional[str] = ..., reference_id: _Optional[str] = ..., reference_type: _Optional[str] = ..., is_public: bool = ..., update_mask: _Optional[_Union[_field_mask_pb2.FieldMask, _Mapping]] = ...) -> None: ...

class UpdateFileResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class DeleteFileRequest(_message.Message):
    __slots__ = ("tenant_id", "file_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    file_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., file_id: _Optional[str] = ...) -> None: ...

class DeleteFileResponse(_message.Message):
    __slots__ = ("success", "error")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    success: bool
    error: _dto_pb2.ApiError
    def __init__(self, success: bool = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListFilesRequest(_message.Message):
    __slots__ = ("tenant_id", "file_type", "reference_id", "reference_type", "uploaded_by", "page", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_ID_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    UPLOADED_BY_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    file_type: str
    reference_id: str
    reference_type: str
    uploaded_by: str
    page: int
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., file_type: _Optional[str] = ..., reference_id: _Optional[str] = ..., reference_type: _Optional[str] = ..., uploaded_by: _Optional[str] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListFilesResponse(_message.Message):
    __slots__ = ("files", "total_count", "error", "next_page_token")
    FILES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    files: _containers.RepeatedCompositeFieldContainer[_file_pb2.File]
    total_count: int
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, files: _Optional[_Iterable[_Union[_file_pb2.File, _Mapping]]] = ..., total_count: _Optional[int] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...
