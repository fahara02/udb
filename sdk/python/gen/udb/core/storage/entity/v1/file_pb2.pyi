import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from udb.core.storage.entity.v1 import enums_pb2 as _enums_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class File(_message.Message):
    __slots__ = ("file_id", "tenant_id", "project_id", "filename", "content_type", "size_bytes", "backend", "bucket", "object_key", "url", "cdn_url", "file_type", "reference_id", "reference_type", "is_public", "status", "checksum", "expires_at", "uploaded_by", "audit_info", "deleted_at", "deleted_by")
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    CDN_URL_FIELD_NUMBER: _ClassVar[int]
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_ID_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    IS_PUBLIC_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    UPLOADED_BY_FIELD_NUMBER: _ClassVar[int]
    AUDIT_INFO_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    file_id: str
    tenant_id: str
    project_id: str
    filename: str
    content_type: str
    size_bytes: int
    backend: str
    bucket: str
    object_key: str
    url: str
    cdn_url: str
    file_type: _enums_pb2.FileType
    reference_id: str
    reference_type: str
    is_public: bool
    status: _enums_pb2.FileStatus
    checksum: str
    expires_at: _timestamp_pb2.Timestamp
    uploaded_by: str
    audit_info: _types_pb2.AuditInfo
    deleted_at: _timestamp_pb2.Timestamp
    deleted_by: str
    def __init__(self, file_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., filename: _Optional[str] = ..., content_type: _Optional[str] = ..., size_bytes: _Optional[int] = ..., backend: _Optional[str] = ..., bucket: _Optional[str] = ..., object_key: _Optional[str] = ..., url: _Optional[str] = ..., cdn_url: _Optional[str] = ..., file_type: _Optional[_Union[_enums_pb2.FileType, str]] = ..., reference_id: _Optional[str] = ..., reference_type: _Optional[str] = ..., is_public: bool = ..., status: _Optional[_Union[_enums_pb2.FileStatus, str]] = ..., checksum: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., uploaded_by: _Optional[str] = ..., audit_info: _Optional[_Union[_types_pb2.AuditInfo, _Mapping]] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_by: _Optional[str] = ...) -> None: ...
