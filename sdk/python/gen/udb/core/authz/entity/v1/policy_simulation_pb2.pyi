import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PolicySimulation(_message.Message):
    __slots__ = ("simulation_id", "policy_version_id", "principal_json", "resource_json", "action", "purpose", "active_decision_json", "draft_decision_json", "diff_json", "tenant_id", "project_id", "created_at")
    SIMULATION_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_JSON_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_JSON_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    PURPOSE_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_DECISION_JSON_FIELD_NUMBER: _ClassVar[int]
    DRAFT_DECISION_JSON_FIELD_NUMBER: _ClassVar[int]
    DIFF_JSON_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    simulation_id: str
    policy_version_id: str
    principal_json: str
    resource_json: str
    action: str
    purpose: str
    active_decision_json: str
    draft_decision_json: str
    diff_json: str
    tenant_id: str
    project_id: str
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, simulation_id: _Optional[str] = ..., policy_version_id: _Optional[str] = ..., principal_json: _Optional[str] = ..., resource_json: _Optional[str] = ..., action: _Optional[str] = ..., purpose: _Optional[str] = ..., active_decision_json: _Optional[str] = ..., draft_decision_json: _Optional[str] = ..., diff_json: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
