import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PolicyApproval(_message.Message):
    __slots__ = ("approval_id", "draft_id", "tenant_id", "actor", "role", "decision", "reason", "created_at")
    APPROVAL_ID_FIELD_NUMBER: _ClassVar[int]
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    DECISION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    approval_id: str
    draft_id: str
    tenant_id: str
    actor: str
    role: str
    decision: str
    reason: str
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, approval_id: _Optional[str] = ..., draft_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., actor: _Optional[str] = ..., role: _Optional[str] = ..., decision: _Optional[str] = ..., reason: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
