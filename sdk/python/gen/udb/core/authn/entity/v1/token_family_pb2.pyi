import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TokenFamily(_message.Message):
    __slots__ = ("family_id", "session_id", "user_id", "principal_id", "tenant_id", "project_id", "device_id", "current_refresh_jti_hash", "previous_refresh_jti_hash", "reuse_detected_at", "revoked_at", "revocation_reason", "created_at", "updated_at")
    FAMILY_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    CURRENT_REFRESH_JTI_HASH_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_REFRESH_JTI_HASH_FIELD_NUMBER: _ClassVar[int]
    REUSE_DETECTED_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKED_AT_FIELD_NUMBER: _ClassVar[int]
    REVOCATION_REASON_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    family_id: str
    session_id: str
    user_id: str
    principal_id: str
    tenant_id: str
    project_id: str
    device_id: str
    current_refresh_jti_hash: str
    previous_refresh_jti_hash: str
    reuse_detected_at: _timestamp_pb2.Timestamp
    revoked_at: _timestamp_pb2.Timestamp
    revocation_reason: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, family_id: _Optional[str] = ..., session_id: _Optional[str] = ..., user_id: _Optional[str] = ..., principal_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., device_id: _Optional[str] = ..., current_refresh_jti_hash: _Optional[str] = ..., previous_refresh_jti_hash: _Optional[str] = ..., reuse_detected_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revoked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revocation_reason: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
