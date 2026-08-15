import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BackupRun(_message.Message):
    __slots__ = ("backup_id", "tenant_id", "project_id", "kind", "status", "object_prefix", "manifest_checksum", "table_count", "total_rows", "excluded_count", "source_tenant_id", "target_tenant_id", "error_message", "created_at", "completed_at", "metadata_json")
    BACKUP_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    OBJECT_PREFIX_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    TABLE_COUNT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ROWS_FIELD_NUMBER: _ClassVar[int]
    EXCLUDED_COUNT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    backup_id: str
    tenant_id: str
    project_id: str
    kind: str
    status: str
    object_prefix: str
    manifest_checksum: str
    table_count: int
    total_rows: int
    excluded_count: int
    source_tenant_id: str
    target_tenant_id: str
    error_message: str
    created_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    metadata_json: str
    def __init__(self, backup_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., kind: _Optional[str] = ..., status: _Optional[str] = ..., object_prefix: _Optional[str] = ..., manifest_checksum: _Optional[str] = ..., table_count: _Optional[int] = ..., total_rows: _Optional[int] = ..., excluded_count: _Optional[int] = ..., source_tenant_id: _Optional[str] = ..., target_tenant_id: _Optional[str] = ..., error_message: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., metadata_json: _Optional[str] = ...) -> None: ...
