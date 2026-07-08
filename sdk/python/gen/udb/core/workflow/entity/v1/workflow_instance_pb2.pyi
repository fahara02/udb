import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from udb.core.workflow.entity.v1 import enums_pb2 as _enums_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WorkflowInstance(_message.Message):
    __slots__ = ("workflow_id", "tenant_id", "project_id", "workflow_type", "status", "current_step", "total_steps", "payload", "compensations", "correlation_id", "saga_id", "pending_signal", "last_error", "next_run_at", "last_transition_at", "audit_info", "deleted_at", "deleted_by")
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CURRENT_STEP_FIELD_NUMBER: _ClassVar[int]
    TOTAL_STEPS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    COMPENSATIONS_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    SAGA_ID_FIELD_NUMBER: _ClassVar[int]
    PENDING_SIGNAL_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_RUN_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_TRANSITION_AT_FIELD_NUMBER: _ClassVar[int]
    AUDIT_INFO_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    tenant_id: str
    project_id: str
    workflow_type: str
    status: _enums_pb2.WorkflowStatus
    current_step: int
    total_steps: int
    payload: str
    compensations: str
    correlation_id: str
    saga_id: str
    pending_signal: str
    last_error: str
    next_run_at: _timestamp_pb2.Timestamp
    last_transition_at: _timestamp_pb2.Timestamp
    audit_info: _types_pb2.AuditInfo
    deleted_at: _timestamp_pb2.Timestamp
    deleted_by: str
    def __init__(self, workflow_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., workflow_type: _Optional[str] = ..., status: _Optional[_Union[_enums_pb2.WorkflowStatus, str]] = ..., current_step: _Optional[int] = ..., total_steps: _Optional[int] = ..., payload: _Optional[str] = ..., compensations: _Optional[str] = ..., correlation_id: _Optional[str] = ..., saga_id: _Optional[str] = ..., pending_signal: _Optional[str] = ..., last_error: _Optional[str] = ..., next_run_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_transition_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., audit_info: _Optional[_Union[_types_pb2.AuditInfo, _Mapping]] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_by: _Optional[str] = ...) -> None: ...
