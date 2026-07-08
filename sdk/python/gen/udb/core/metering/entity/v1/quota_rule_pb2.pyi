from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class QuotaRule(_message.Message):
    __slots__ = ("quota_id", "tenant_id", "project_id", "metric", "limit_value", "window_seconds", "enabled", "revision", "metadata_json")
    QUOTA_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    METRIC_FIELD_NUMBER: _ClassVar[int]
    LIMIT_VALUE_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    quota_id: str
    tenant_id: str
    project_id: str
    metric: str
    limit_value: int
    window_seconds: int
    enabled: bool
    revision: int
    metadata_json: str
    def __init__(self, quota_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., metric: _Optional[str] = ..., limit_value: _Optional[int] = ..., window_seconds: _Optional[int] = ..., enabled: bool = ..., revision: _Optional[int] = ..., metadata_json: _Optional[str] = ...) -> None: ...
