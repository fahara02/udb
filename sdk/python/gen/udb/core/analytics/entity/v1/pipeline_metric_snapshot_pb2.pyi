import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PipelineMetricSnapshot(_message.Message):
    __slots__ = ("snapshot_id", "snapshot_hour", "stage_name", "tenant_id", "total_requests", "successful", "failed", "p50_latency_ms", "p95_latency_ms", "p99_latency_ms", "avg_latency_ms", "error_rate", "throughput_rps", "recorded_at")
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_HOUR_FIELD_NUMBER: _ClassVar[int]
    STAGE_NAME_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TOTAL_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    SUCCESSFUL_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    P50_LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    P95_LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    P99_LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    AVG_LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    ERROR_RATE_FIELD_NUMBER: _ClassVar[int]
    THROUGHPUT_RPS_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    snapshot_id: str
    snapshot_hour: _timestamp_pb2.Timestamp
    stage_name: str
    tenant_id: str
    total_requests: int
    successful: int
    failed: int
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    avg_latency_ms: float
    error_rate: float
    throughput_rps: float
    recorded_at: _timestamp_pb2.Timestamp
    def __init__(self, snapshot_id: _Optional[str] = ..., snapshot_hour: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., stage_name: _Optional[str] = ..., tenant_id: _Optional[str] = ..., total_requests: _Optional[int] = ..., successful: _Optional[int] = ..., failed: _Optional[int] = ..., p50_latency_ms: _Optional[float] = ..., p95_latency_ms: _Optional[float] = ..., p99_latency_ms: _Optional[float] = ..., avg_latency_ms: _Optional[float] = ..., error_rate: _Optional[float] = ..., throughput_rps: _Optional[float] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
