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

class AuthzRevision(_message.Message):
    __slots__ = ("revision_id", "tenant_id", "project_id", "policy_revision", "relationship_revision", "content_hash", "changed_by", "changed_at", "change_type")
    REVISION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_REVISION_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIP_REVISION_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    CHANGED_BY_FIELD_NUMBER: _ClassVar[int]
    CHANGED_AT_FIELD_NUMBER: _ClassVar[int]
    CHANGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    revision_id: str
    tenant_id: str
    project_id: str
    policy_revision: int
    relationship_revision: int
    content_hash: str
    changed_by: str
    changed_at: _timestamp_pb2.Timestamp
    change_type: _governance_enums_pb2.AuthzChangeType
    def __init__(self, revision_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., policy_revision: _Optional[int] = ..., relationship_revision: _Optional[int] = ..., content_hash: _Optional[str] = ..., changed_by: _Optional[str] = ..., changed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., change_type: _Optional[_Union[_governance_enums_pb2.AuthzChangeType, str]] = ...) -> None: ...
