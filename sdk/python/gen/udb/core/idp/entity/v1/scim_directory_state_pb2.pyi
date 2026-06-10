import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.idp.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ScimDirectoryState(_message.Message):
    __slots__ = ("scim_directory_state_id", "tenant_id", "provider_id", "cursor", "last_sync_at", "failure_count", "last_error", "deprovision_policy", "created_at", "updated_at")
    SCIM_DIRECTORY_STATE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    LAST_SYNC_AT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_COUNT_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    DEPROVISION_POLICY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    scim_directory_state_id: str
    tenant_id: str
    provider_id: str
    cursor: str
    last_sync_at: _timestamp_pb2.Timestamp
    failure_count: int
    last_error: str
    deprovision_policy: _enums_pb2.DeprovisionPolicy
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, scim_directory_state_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., cursor: _Optional[str] = ..., last_sync_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., failure_count: _Optional[int] = ..., last_error: _Optional[str] = ..., deprovision_policy: _Optional[_Union[_enums_pb2.DeprovisionPolicy, str]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
