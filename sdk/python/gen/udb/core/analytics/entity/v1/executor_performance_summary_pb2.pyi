import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ExecutorPerformanceSummary(_message.Message):
    __slots__ = ("summary_id", "summary_date", "executor_identity", "workload_kind", "total_dispatches", "successful_results", "timeout_count", "error_count", "avg_execution_ms", "p99_execution_ms", "avg_confidence", "success_rate", "avg_capacity_utilisation", "recorded_at")
    SUMMARY_ID_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_DATE_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    WORKLOAD_KIND_FIELD_NUMBER: _ClassVar[int]
    TOTAL_DISPATCHES_FIELD_NUMBER: _ClassVar[int]
    SUCCESSFUL_RESULTS_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_COUNT_FIELD_NUMBER: _ClassVar[int]
    AVG_EXECUTION_MS_FIELD_NUMBER: _ClassVar[int]
    P99_EXECUTION_MS_FIELD_NUMBER: _ClassVar[int]
    AVG_CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_RATE_FIELD_NUMBER: _ClassVar[int]
    AVG_CAPACITY_UTILISATION_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    summary_id: str
    summary_date: _timestamp_pb2.Timestamp
    executor_identity: str
    workload_kind: str
    total_dispatches: int
    successful_results: int
    timeout_count: int
    error_count: int
    avg_execution_ms: float
    p99_execution_ms: float
    avg_confidence: float
    success_rate: float
    avg_capacity_utilisation: float
    recorded_at: _timestamp_pb2.Timestamp
    def __init__(self, summary_id: _Optional[str] = ..., summary_date: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., executor_identity: _Optional[str] = ..., workload_kind: _Optional[str] = ..., total_dispatches: _Optional[int] = ..., successful_results: _Optional[int] = ..., timeout_count: _Optional[int] = ..., error_count: _Optional[int] = ..., avg_execution_ms: _Optional[float] = ..., p99_execution_ms: _Optional[float] = ..., avg_confidence: _Optional[float] = ..., success_rate: _Optional[float] = ..., avg_capacity_utilisation: _Optional[float] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
