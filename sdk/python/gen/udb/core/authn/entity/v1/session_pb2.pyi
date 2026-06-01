import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.authn.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Session(_message.Message):
    __slots__ = ("session_id", "user_id", "session_type", "session_token_lookup", "session_token_hash", "csrf_token_hash", "access_token_jti", "refresh_token_jti", "device_type", "device_name", "ip_address", "user_agent", "is_active", "expires_at", "last_active_at", "revoked_by", "revoke_reason", "created_at", "tenant_id", "project_id", "principal_id", "provider_id", "auth_method", "scopes_json", "metadata_json")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_TYPE_FIELD_NUMBER: _ClassVar[int]
    SESSION_TOKEN_LOOKUP_FIELD_NUMBER: _ClassVar[int]
    SESSION_TOKEN_HASH_FIELD_NUMBER: _ClassVar[int]
    CSRF_TOKEN_HASH_FIELD_NUMBER: _ClassVar[int]
    ACCESS_TOKEN_JTI_FIELD_NUMBER: _ClassVar[int]
    REFRESH_TOKEN_JTI_FIELD_NUMBER: _ClassVar[int]
    DEVICE_TYPE_FIELD_NUMBER: _ClassVar[int]
    DEVICE_NAME_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    USER_AGENT_FIELD_NUMBER: _ClassVar[int]
    IS_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_ACTIVE_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKED_BY_FIELD_NUMBER: _ClassVar[int]
    REVOKE_REASON_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    AUTH_METHOD_FIELD_NUMBER: _ClassVar[int]
    SCOPES_JSON_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    user_id: str
    session_type: _enums_pb2.SessionType
    session_token_lookup: str
    session_token_hash: str
    csrf_token_hash: str
    access_token_jti: str
    refresh_token_jti: str
    device_type: _enums_pb2.DeviceType
    device_name: str
    ip_address: str
    user_agent: str
    is_active: bool
    expires_at: _timestamp_pb2.Timestamp
    last_active_at: _timestamp_pb2.Timestamp
    revoked_by: str
    revoke_reason: str
    created_at: _timestamp_pb2.Timestamp
    tenant_id: str
    project_id: str
    principal_id: str
    provider_id: str
    auth_method: str
    scopes_json: str
    metadata_json: str
    def __init__(self, session_id: _Optional[str] = ..., user_id: _Optional[str] = ..., session_type: _Optional[_Union[_enums_pb2.SessionType, str]] = ..., session_token_lookup: _Optional[str] = ..., session_token_hash: _Optional[str] = ..., csrf_token_hash: _Optional[str] = ..., access_token_jti: _Optional[str] = ..., refresh_token_jti: _Optional[str] = ..., device_type: _Optional[_Union[_enums_pb2.DeviceType, str]] = ..., device_name: _Optional[str] = ..., ip_address: _Optional[str] = ..., user_agent: _Optional[str] = ..., is_active: bool = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_active_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revoked_by: _Optional[str] = ..., revoke_reason: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., principal_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., auth_method: _Optional[str] = ..., scopes_json: _Optional[str] = ..., metadata_json: _Optional[str] = ...) -> None: ...
