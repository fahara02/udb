import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ServiceAccountTenantMembership(_message.Message):
    __slots__ = ("membership_id", "user_id", "home_tenant_id", "target_tenant_id", "target_project_id", "approved_scopes_json", "status", "revision", "approved_by", "reason", "expires_at", "created_at", "updated_at")
    MEMBERSHIP_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    HOME_TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    APPROVED_SCOPES_JSON_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    APPROVED_BY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    membership_id: str
    user_id: str
    home_tenant_id: str
    target_tenant_id: str
    target_project_id: str
    approved_scopes_json: str
    status: str
    revision: int
    approved_by: str
    reason: str
    expires_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, membership_id: _Optional[str] = ..., user_id: _Optional[str] = ..., home_tenant_id: _Optional[str] = ..., target_tenant_id: _Optional[str] = ..., target_project_id: _Optional[str] = ..., approved_scopes_json: _Optional[str] = ..., status: _Optional[str] = ..., revision: _Optional[int] = ..., approved_by: _Optional[str] = ..., reason: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
