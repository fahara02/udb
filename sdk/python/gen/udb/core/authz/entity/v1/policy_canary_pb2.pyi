import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CanaryScopeKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CANARY_SCOPE_KIND_UNSPECIFIED: _ClassVar[CanaryScopeKind]
    CANARY_SCOPE_KIND_NODE: _ClassVar[CanaryScopeKind]
    CANARY_SCOPE_KIND_TENANT: _ClassVar[CanaryScopeKind]
    CANARY_SCOPE_KIND_PERCENT: _ClassVar[CanaryScopeKind]

class CanaryState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CANARY_STATE_UNSPECIFIED: _ClassVar[CanaryState]
    CANARY_STATE_ACTIVE: _ClassVar[CanaryState]
    CANARY_STATE_PROMOTED: _ClassVar[CanaryState]
    CANARY_STATE_ROLLED_BACK: _ClassVar[CanaryState]
    CANARY_STATE_PAUSED: _ClassVar[CanaryState]
CANARY_SCOPE_KIND_UNSPECIFIED: CanaryScopeKind
CANARY_SCOPE_KIND_NODE: CanaryScopeKind
CANARY_SCOPE_KIND_TENANT: CanaryScopeKind
CANARY_SCOPE_KIND_PERCENT: CanaryScopeKind
CANARY_STATE_UNSPECIFIED: CanaryState
CANARY_STATE_ACTIVE: CanaryState
CANARY_STATE_PROMOTED: CanaryState
CANARY_STATE_ROLLED_BACK: CanaryState
CANARY_STATE_PAUSED: CanaryState

class PolicyCanary(_message.Message):
    __slots__ = ("canary_id", "policy_set_id", "policy_version_id", "scope_kind", "scope_values", "state", "started_at", "success_window_secs", "metric_threshold", "created_by", "tenant_id", "project_id", "min_samples", "rollback_version_id", "outcome_reason", "revision")
    CANARY_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_SET_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPE_KIND_FIELD_NUMBER: _ClassVar[int]
    SCOPE_VALUES_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_WINDOW_SECS_FIELD_NUMBER: _ClassVar[int]
    METRIC_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    MIN_SAMPLES_FIELD_NUMBER: _ClassVar[int]
    ROLLBACK_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_REASON_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    canary_id: str
    policy_set_id: str
    policy_version_id: str
    scope_kind: CanaryScopeKind
    scope_values: str
    state: CanaryState
    started_at: _timestamp_pb2.Timestamp
    success_window_secs: int
    metric_threshold: float
    created_by: str
    tenant_id: str
    project_id: str
    min_samples: int
    rollback_version_id: str
    outcome_reason: str
    revision: int
    def __init__(self, canary_id: _Optional[str] = ..., policy_set_id: _Optional[str] = ..., policy_version_id: _Optional[str] = ..., scope_kind: _Optional[_Union[CanaryScopeKind, str]] = ..., scope_values: _Optional[str] = ..., state: _Optional[_Union[CanaryState, str]] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., success_window_secs: _Optional[int] = ..., metric_threshold: _Optional[float] = ..., created_by: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., min_samples: _Optional[int] = ..., rollback_version_id: _Optional[str] = ..., outcome_reason: _Optional[str] = ..., revision: _Optional[int] = ...) -> None: ...
