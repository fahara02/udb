import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ReconciliationAnalyticsSummary(_message.Message):
    __slots__ = ("summary_id", "summary_date", "total_reconciliations", "exact_matches", "partial_conflicts", "hard_conflicts", "low_confidence_flagged", "avg_reconciliation_ms", "resolution_rate", "avg_record_confidence", "recorded_at")
    SUMMARY_ID_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_DATE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_RECONCILIATIONS_FIELD_NUMBER: _ClassVar[int]
    EXACT_MATCHES_FIELD_NUMBER: _ClassVar[int]
    PARTIAL_CONFLICTS_FIELD_NUMBER: _ClassVar[int]
    HARD_CONFLICTS_FIELD_NUMBER: _ClassVar[int]
    LOW_CONFIDENCE_FLAGGED_FIELD_NUMBER: _ClassVar[int]
    AVG_RECONCILIATION_MS_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_RATE_FIELD_NUMBER: _ClassVar[int]
    AVG_RECORD_CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    summary_id: str
    summary_date: _timestamp_pb2.Timestamp
    total_reconciliations: int
    exact_matches: int
    partial_conflicts: int
    hard_conflicts: int
    low_confidence_flagged: int
    avg_reconciliation_ms: float
    resolution_rate: float
    avg_record_confidence: float
    recorded_at: _timestamp_pb2.Timestamp
    def __init__(self, summary_id: _Optional[str] = ..., summary_date: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., total_reconciliations: _Optional[int] = ..., exact_matches: _Optional[int] = ..., partial_conflicts: _Optional[int] = ..., hard_conflicts: _Optional[int] = ..., low_confidence_flagged: _Optional[int] = ..., avg_reconciliation_ms: _Optional[float] = ..., resolution_rate: _Optional[float] = ..., avg_record_confidence: _Optional[float] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
