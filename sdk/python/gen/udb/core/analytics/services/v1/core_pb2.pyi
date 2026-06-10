from udb.core.analytics.entity.v1 import executor_performance_summary_pb2 as _executor_performance_summary_pb2
from udb.core.analytics.entity.v1 import pipeline_metric_snapshot_pb2 as _pipeline_metric_snapshot_pb2
from udb.core.analytics.entity.v1 import reconciliation_analytics_summary_pb2 as _reconciliation_analytics_summary_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RecordPipelineMetricRequest(_message.Message):
    __slots__ = ("stage_name", "tenant_id", "latency_ms", "is_success", "context")
    STAGE_NAME_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    IS_SUCCESS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    stage_name: str
    tenant_id: str
    latency_ms: float
    is_success: bool
    context: _types_pb2.RequestContext
    def __init__(self, stage_name: _Optional[str] = ..., tenant_id: _Optional[str] = ..., latency_ms: _Optional[float] = ..., is_success: bool = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class RecordPipelineMetricResponse(_message.Message):
    __slots__ = ("accepted",)
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    accepted: bool
    def __init__(self, accepted: bool = ...) -> None: ...

class GetPipelineSummaryRequest(_message.Message):
    __slots__ = ("stage_name", "tenant_id", "hour_from", "hour_to", "page")
    STAGE_NAME_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    HOUR_FROM_FIELD_NUMBER: _ClassVar[int]
    HOUR_TO_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    stage_name: str
    tenant_id: str
    hour_from: str
    hour_to: str
    page: _dto_pb2.PageRequest
    def __init__(self, stage_name: _Optional[str] = ..., tenant_id: _Optional[str] = ..., hour_from: _Optional[str] = ..., hour_to: _Optional[str] = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class GetPipelineSummaryResponse(_message.Message):
    __slots__ = ("snapshots", "page")
    SNAPSHOTS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    snapshots: _containers.RepeatedCompositeFieldContainer[_pipeline_metric_snapshot_pb2.PipelineMetricSnapshot]
    page: _dto_pb2.PageResponse
    def __init__(self, snapshots: _Optional[_Iterable[_Union[_pipeline_metric_snapshot_pb2.PipelineMetricSnapshot, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class GetExecutorPerformanceRequest(_message.Message):
    __slots__ = ("executor_identity", "workload_kind", "date_from", "date_to")
    EXECUTOR_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    WORKLOAD_KIND_FIELD_NUMBER: _ClassVar[int]
    DATE_FROM_FIELD_NUMBER: _ClassVar[int]
    DATE_TO_FIELD_NUMBER: _ClassVar[int]
    executor_identity: str
    workload_kind: str
    date_from: str
    date_to: str
    def __init__(self, executor_identity: _Optional[str] = ..., workload_kind: _Optional[str] = ..., date_from: _Optional[str] = ..., date_to: _Optional[str] = ...) -> None: ...

class GetExecutorPerformanceResponse(_message.Message):
    __slots__ = ("summaries",)
    SUMMARIES_FIELD_NUMBER: _ClassVar[int]
    summaries: _containers.RepeatedCompositeFieldContainer[_executor_performance_summary_pb2.ExecutorPerformanceSummary]
    def __init__(self, summaries: _Optional[_Iterable[_Union[_executor_performance_summary_pb2.ExecutorPerformanceSummary, _Mapping]]] = ...) -> None: ...

class GetReconciliationAnalyticsRequest(_message.Message):
    __slots__ = ("date_from", "date_to")
    DATE_FROM_FIELD_NUMBER: _ClassVar[int]
    DATE_TO_FIELD_NUMBER: _ClassVar[int]
    date_from: str
    date_to: str
    def __init__(self, date_from: _Optional[str] = ..., date_to: _Optional[str] = ...) -> None: ...

class GetReconciliationAnalyticsResponse(_message.Message):
    __slots__ = ("summaries", "overall_resolution_rate", "avg_reconciliation_ms")
    SUMMARIES_FIELD_NUMBER: _ClassVar[int]
    OVERALL_RESOLUTION_RATE_FIELD_NUMBER: _ClassVar[int]
    AVG_RECONCILIATION_MS_FIELD_NUMBER: _ClassVar[int]
    summaries: _containers.RepeatedCompositeFieldContainer[_reconciliation_analytics_summary_pb2.ReconciliationAnalyticsSummary]
    overall_resolution_rate: float
    avg_reconciliation_ms: float
    def __init__(self, summaries: _Optional[_Iterable[_Union[_reconciliation_analytics_summary_pb2.ReconciliationAnalyticsSummary, _Mapping]]] = ..., overall_resolution_rate: _Optional[float] = ..., avg_reconciliation_ms: _Optional[float] = ...) -> None: ...

class GetThroughputRequest(_message.Message):
    __slots__ = ("tenant_id", "hour_from", "hour_to")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    HOUR_FROM_FIELD_NUMBER: _ClassVar[int]
    HOUR_TO_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    hour_from: str
    hour_to: str
    def __init__(self, tenant_id: _Optional[str] = ..., hour_from: _Optional[str] = ..., hour_to: _Optional[str] = ...) -> None: ...

class GetThroughputResponse(_message.Message):
    __slots__ = ("avg_rps", "peak_rps", "total_requests", "overall_success_rate")
    AVG_RPS_FIELD_NUMBER: _ClassVar[int]
    PEAK_RPS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    OVERALL_SUCCESS_RATE_FIELD_NUMBER: _ClassVar[int]
    avg_rps: float
    peak_rps: float
    total_requests: int
    overall_success_rate: float
    def __init__(self, avg_rps: _Optional[float] = ..., peak_rps: _Optional[float] = ..., total_requests: _Optional[int] = ..., overall_success_rate: _Optional[float] = ...) -> None: ...

class GetSlaComplianceRequest(_message.Message):
    __slots__ = ("stage_name", "date_from", "date_to", "p99_threshold_ms", "error_rate_threshold")
    STAGE_NAME_FIELD_NUMBER: _ClassVar[int]
    DATE_FROM_FIELD_NUMBER: _ClassVar[int]
    DATE_TO_FIELD_NUMBER: _ClassVar[int]
    P99_THRESHOLD_MS_FIELD_NUMBER: _ClassVar[int]
    ERROR_RATE_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    stage_name: str
    date_from: str
    date_to: str
    p99_threshold_ms: float
    error_rate_threshold: float
    def __init__(self, stage_name: _Optional[str] = ..., date_from: _Optional[str] = ..., date_to: _Optional[str] = ..., p99_threshold_ms: _Optional[float] = ..., error_rate_threshold: _Optional[float] = ...) -> None: ...

class SlaComplianceEntry(_message.Message):
    __slots__ = ("stage_name", "period", "p99_latency_ms", "error_rate", "p99_sla_met", "error_rate_sla_met")
    STAGE_NAME_FIELD_NUMBER: _ClassVar[int]
    PERIOD_FIELD_NUMBER: _ClassVar[int]
    P99_LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    ERROR_RATE_FIELD_NUMBER: _ClassVar[int]
    P99_SLA_MET_FIELD_NUMBER: _ClassVar[int]
    ERROR_RATE_SLA_MET_FIELD_NUMBER: _ClassVar[int]
    stage_name: str
    period: str
    p99_latency_ms: float
    error_rate: float
    p99_sla_met: bool
    error_rate_sla_met: bool
    def __init__(self, stage_name: _Optional[str] = ..., period: _Optional[str] = ..., p99_latency_ms: _Optional[float] = ..., error_rate: _Optional[float] = ..., p99_sla_met: bool = ..., error_rate_sla_met: bool = ...) -> None: ...

class GetSlaComplianceResponse(_message.Message):
    __slots__ = ("entries", "overall_p99_compliance_rate", "overall_error_rate_compliance_rate")
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    OVERALL_P99_COMPLIANCE_RATE_FIELD_NUMBER: _ClassVar[int]
    OVERALL_ERROR_RATE_COMPLIANCE_RATE_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[SlaComplianceEntry]
    overall_p99_compliance_rate: float
    overall_error_rate_compliance_rate: float
    def __init__(self, entries: _Optional[_Iterable[_Union[SlaComplianceEntry, _Mapping]]] = ..., overall_p99_compliance_rate: _Optional[float] = ..., overall_error_rate_compliance_rate: _Optional[float] = ...) -> None: ...

class TriggerSnapshotRequest(_message.Message):
    __slots__ = ("stage_name", "hour", "context")
    STAGE_NAME_FIELD_NUMBER: _ClassVar[int]
    HOUR_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    stage_name: str
    hour: str
    context: _types_pb2.RequestContext
    def __init__(self, stage_name: _Optional[str] = ..., hour: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class TriggerSnapshotResponse(_message.Message):
    __slots__ = ("snapshots_written",)
    SNAPSHOTS_WRITTEN_FIELD_NUMBER: _ClassVar[int]
    snapshots_written: int
    def __init__(self, snapshots_written: _Optional[int] = ...) -> None: ...
