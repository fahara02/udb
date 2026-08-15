import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BackupPolicy(_message.Message):
    __slots__ = ("policy_id", "tenant_id", "project_id", "policy_name", "schedule_cron", "retention_days", "max_retained_backups", "enabled", "object_backend", "object_bucket", "created_at", "updated_at", "metadata_json")
    POLICY_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_CRON_FIELD_NUMBER: _ClassVar[int]
    RETENTION_DAYS_FIELD_NUMBER: _ClassVar[int]
    MAX_RETAINED_BACKUPS_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    OBJECT_BACKEND_FIELD_NUMBER: _ClassVar[int]
    OBJECT_BUCKET_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    policy_id: str
    tenant_id: str
    project_id: str
    policy_name: str
    schedule_cron: str
    retention_days: int
    max_retained_backups: int
    enabled: bool
    object_backend: str
    object_bucket: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    metadata_json: str
    def __init__(self, policy_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., policy_name: _Optional[str] = ..., schedule_cron: _Optional[str] = ..., retention_days: _Optional[int] = ..., max_retained_backups: _Optional[int] = ..., enabled: bool = ..., object_backend: _Optional[str] = ..., object_bucket: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., metadata_json: _Optional[str] = ...) -> None: ...
