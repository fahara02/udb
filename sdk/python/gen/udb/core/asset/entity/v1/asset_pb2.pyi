import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from udb.core.asset.entity.v1 import enums_pb2 as _enums_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Asset(_message.Message):
    __slots__ = ("asset_id", "tenant_id", "project_id", "file_id", "name", "media_type", "status", "metadata", "audit_info", "deleted_at", "deleted_by")
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    MEDIA_TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    AUDIT_INFO_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    asset_id: str
    tenant_id: str
    project_id: str
    file_id: str
    name: str
    media_type: str
    status: _enums_pb2.AssetStatus
    metadata: str
    audit_info: _types_pb2.AuditInfo
    deleted_at: _timestamp_pb2.Timestamp
    deleted_by: str
    def __init__(self, asset_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., file_id: _Optional[str] = ..., name: _Optional[str] = ..., media_type: _Optional[str] = ..., status: _Optional[_Union[_enums_pb2.AssetStatus, str]] = ..., metadata: _Optional[str] = ..., audit_info: _Optional[_Union[_types_pb2.AuditInfo, _Mapping]] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_by: _Optional[str] = ...) -> None: ...
