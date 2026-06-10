import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PolicyDraft(_message.Message):
    __slots__ = ("draft_id", "tenant_id", "project_id", "title", "description", "proposed_policies_json", "proposed_tuples_json", "base_version_id", "status", "author", "high_risk", "created_at", "updated_at")
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PROPOSED_POLICIES_JSON_FIELD_NUMBER: _ClassVar[int]
    PROPOSED_TUPLES_JSON_FIELD_NUMBER: _ClassVar[int]
    BASE_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    HIGH_RISK_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    draft_id: str
    tenant_id: str
    project_id: str
    title: str
    description: str
    proposed_policies_json: str
    proposed_tuples_json: str
    base_version_id: str
    status: str
    author: str
    high_risk: bool
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, draft_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., title: _Optional[str] = ..., description: _Optional[str] = ..., proposed_policies_json: _Optional[str] = ..., proposed_tuples_json: _Optional[str] = ..., base_version_id: _Optional[str] = ..., status: _Optional[str] = ..., author: _Optional[str] = ..., high_risk: bool = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
