import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PipelineSnapshotCommittedEvent(_message.Message):
    __slots__ = ("event_id", "stage_name", "snapshot_hour", "total_requests", "error_rate", "p99_latency_ms", "correlation_id", "occurred_at", "tenant_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_NAME_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_HOUR_FIELD_NUMBER: _ClassVar[int]
    TOTAL_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    ERROR_RATE_FIELD_NUMBER: _ClassVar[int]
    P99_LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    stage_name: str
    snapshot_hour: str
    total_requests: int
    error_rate: float
    p99_latency_ms: float
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, event_id: _Optional[str] = ..., stage_name: _Optional[str] = ..., snapshot_hour: _Optional[str] = ..., total_requests: _Optional[int] = ..., error_rate: _Optional[float] = ..., p99_latency_ms: _Optional[float] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class SlaBreachDetectedEvent(_message.Message):
    __slots__ = ("event_id", "stage_name", "breach_type", "observed_value", "threshold", "severity", "correlation_id", "occurred_at", "tenant_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_NAME_FIELD_NUMBER: _ClassVar[int]
    BREACH_TYPE_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_VALUE_FIELD_NUMBER: _ClassVar[int]
    THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    stage_name: str
    breach_type: str
    observed_value: float
    threshold: float
    severity: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, event_id: _Optional[str] = ..., stage_name: _Optional[str] = ..., breach_type: _Optional[str] = ..., observed_value: _Optional[float] = ..., threshold: _Optional[float] = ..., severity: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class DailySummaryGeneratedEvent(_message.Message):
    __slots__ = ("event_id", "summary_date", "executor_summaries_count", "reconciliation_summary_ready", "correlation_id", "occurred_at", "tenant_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_DATE_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_SUMMARIES_COUNT_FIELD_NUMBER: _ClassVar[int]
    RECONCILIATION_SUMMARY_READY_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    summary_date: str
    executor_summaries_count: int
    reconciliation_summary_ready: bool
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, event_id: _Optional[str] = ..., summary_date: _Optional[str] = ..., executor_summaries_count: _Optional[int] = ..., reconciliation_summary_ready: bool = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...
