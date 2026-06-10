import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.authz.entity.v1 import governance_enums_pb2 as _governance_enums_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PolicyVersion(_message.Message):
    __slots__ = ("policy_version_id", "policy_set_id", "version_number", "state", "snapshot_hash", "created_by", "created_at", "activated_by", "activated_at", "rollback_of", "change_reason", "revision", "content_hash", "tenant_id", "project_id", "payload_json", "high_risk", "submitted_by", "source_draft_id")
    POLICY_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_SET_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_NUMBER_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_HASH_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    ACTIVATED_BY_FIELD_NUMBER: _ClassVar[int]
    ACTIVATED_AT_FIELD_NUMBER: _ClassVar[int]
    ROLLBACK_OF_FIELD_NUMBER: _ClassVar[int]
    CHANGE_REASON_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    HIGH_RISK_FIELD_NUMBER: _ClassVar[int]
    SUBMITTED_BY_FIELD_NUMBER: _ClassVar[int]
    SOURCE_DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    policy_version_id: str
    policy_set_id: str
    version_number: int
    state: _governance_enums_pb2.PolicyVersionState
    snapshot_hash: str
    created_by: str
    created_at: _timestamp_pb2.Timestamp
    activated_by: str
    activated_at: _timestamp_pb2.Timestamp
    rollback_of: str
    change_reason: str
    revision: int
    content_hash: str
    tenant_id: str
    project_id: str
    payload_json: str
    high_risk: bool
    submitted_by: str
    source_draft_id: str
    def __init__(self, policy_version_id: _Optional[str] = ..., policy_set_id: _Optional[str] = ..., version_number: _Optional[int] = ..., state: _Optional[_Union[_governance_enums_pb2.PolicyVersionState, str]] = ..., snapshot_hash: _Optional[str] = ..., created_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., activated_by: _Optional[str] = ..., activated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., rollback_of: _Optional[str] = ..., change_reason: _Optional[str] = ..., revision: _Optional[int] = ..., content_hash: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., payload_json: _Optional[str] = ..., high_risk: bool = ..., submitted_by: _Optional[str] = ..., source_draft_id: _Optional[str] = ...) -> None: ...
