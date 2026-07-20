import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CertificateBinding(_message.Message):
    __slots__ = ("binding_id", "selector_kind", "selector_value", "user_id", "tenant_id", "grant_revision", "scope_subset_json", "status", "not_before", "not_after", "revoked_at", "revoke_reason", "updated_by", "reason", "created_at", "updated_at")
    BINDING_ID_FIELD_NUMBER: _ClassVar[int]
    SELECTOR_KIND_FIELD_NUMBER: _ClassVar[int]
    SELECTOR_VALUE_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GRANT_REVISION_FIELD_NUMBER: _ClassVar[int]
    SCOPE_SUBSET_JSON_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    NOT_BEFORE_FIELD_NUMBER: _ClassVar[int]
    NOT_AFTER_FIELD_NUMBER: _ClassVar[int]
    REVOKED_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKE_REASON_FIELD_NUMBER: _ClassVar[int]
    UPDATED_BY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    binding_id: str
    selector_kind: str
    selector_value: str
    user_id: str
    tenant_id: str
    grant_revision: int
    scope_subset_json: str
    status: str
    not_before: _timestamp_pb2.Timestamp
    not_after: _timestamp_pb2.Timestamp
    revoked_at: _timestamp_pb2.Timestamp
    revoke_reason: str
    updated_by: str
    reason: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, binding_id: _Optional[str] = ..., selector_kind: _Optional[str] = ..., selector_value: _Optional[str] = ..., user_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., grant_revision: _Optional[int] = ..., scope_subset_json: _Optional[str] = ..., status: _Optional[str] = ..., not_before: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., not_after: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revoked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revoke_reason: _Optional[str] = ..., updated_by: _Optional[str] = ..., reason: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
