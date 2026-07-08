import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from udb.core.scheduler.entity.v1 import enums_pb2 as _enums_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ScheduledJob(_message.Message):
    __slots__ = ("job_id", "tenant_id", "project_id", "name", "schedule_type", "cron_expression", "payload", "target_topic", "status", "next_fire_at", "last_fired_at", "max_attempts", "attempt_count", "backoff_seconds", "audit_info", "deleted_at", "deleted_by")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_TYPE_FIELD_NUMBER: _ClassVar[int]
    CRON_EXPRESSION_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    TARGET_TOPIC_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    NEXT_FIRE_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_FIRED_AT_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_COUNT_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_SECONDS_FIELD_NUMBER: _ClassVar[int]
    AUDIT_INFO_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    tenant_id: str
    project_id: str
    name: str
    schedule_type: _enums_pb2.ScheduleType
    cron_expression: str
    payload: str
    target_topic: str
    status: _enums_pb2.JobStatus
    next_fire_at: _timestamp_pb2.Timestamp
    last_fired_at: _timestamp_pb2.Timestamp
    max_attempts: int
    attempt_count: int
    backoff_seconds: int
    audit_info: _types_pb2.AuditInfo
    deleted_at: _timestamp_pb2.Timestamp
    deleted_by: str
    def __init__(self, job_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., name: _Optional[str] = ..., schedule_type: _Optional[_Union[_enums_pb2.ScheduleType, str]] = ..., cron_expression: _Optional[str] = ..., payload: _Optional[str] = ..., target_topic: _Optional[str] = ..., status: _Optional[_Union[_enums_pb2.JobStatus, str]] = ..., next_fire_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_fired_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., max_attempts: _Optional[int] = ..., attempt_count: _Optional[int] = ..., backoff_seconds: _Optional[int] = ..., audit_info: _Optional[_Union[_types_pb2.AuditInfo, _Mapping]] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_by: _Optional[str] = ...) -> None: ...
