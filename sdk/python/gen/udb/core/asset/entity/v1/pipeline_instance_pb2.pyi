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

class PipelineInstance(_message.Message):
    __slots__ = ("instance_id", "definition_id", "asset_id", "tenant_id", "status", "current_step", "context", "correlation_id", "started_at", "completed_at", "audit_info")
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    DEFINITION_ID_FIELD_NUMBER: _ClassVar[int]
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CURRENT_STEP_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    AUDIT_INFO_FIELD_NUMBER: _ClassVar[int]
    instance_id: str
    definition_id: str
    asset_id: str
    tenant_id: str
    status: _enums_pb2.PipelineStatus
    current_step: str
    context: str
    correlation_id: str
    started_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    audit_info: _types_pb2.AuditInfo
    def __init__(self, instance_id: _Optional[str] = ..., definition_id: _Optional[str] = ..., asset_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., status: _Optional[_Union[_enums_pb2.PipelineStatus, str]] = ..., current_step: _Optional[str] = ..., context: _Optional[str] = ..., correlation_id: _Optional[str] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., audit_info: _Optional[_Union[_types_pb2.AuditInfo, _Mapping]] = ...) -> None: ...
