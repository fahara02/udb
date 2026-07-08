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

class PipelineStep(_message.Message):
    __slots__ = ("step_id", "instance_id", "tenant_id", "step_name", "step_type", "status", "result", "params", "error", "retry_count", "started_at", "completed_at", "audit_info")
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_NAME_FIELD_NUMBER: _ClassVar[int]
    STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RETRY_COUNT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    AUDIT_INFO_FIELD_NUMBER: _ClassVar[int]
    step_id: str
    instance_id: str
    tenant_id: str
    step_name: str
    step_type: _enums_pb2.StepType
    status: _enums_pb2.StepStatus
    result: str
    params: str
    error: str
    retry_count: int
    started_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    audit_info: _types_pb2.AuditInfo
    def __init__(self, step_id: _Optional[str] = ..., instance_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., step_name: _Optional[str] = ..., step_type: _Optional[_Union[_enums_pb2.StepType, str]] = ..., status: _Optional[_Union[_enums_pb2.StepStatus, str]] = ..., result: _Optional[str] = ..., params: _Optional[str] = ..., error: _Optional[str] = ..., retry_count: _Optional[int] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., audit_info: _Optional[_Union[_types_pb2.AuditInfo, _Mapping]] = ...) -> None: ...
