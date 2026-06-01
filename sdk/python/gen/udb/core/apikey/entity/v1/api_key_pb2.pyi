import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.apikey.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ApiKey(_message.Message):
    __slots__ = ("key_id", "key_prefix", "key_hash", "name", "description", "owner_type", "owner_id", "scopes_json", "status", "ip_allowlist_json", "rate_limit_per_minute", "rate_limit_per_day", "created_by", "revoked_by", "revoke_reason", "expires_at", "last_used_at", "created_at", "updated_at", "deleted_at", "deleted_by", "tenant_id", "project_id", "allowed_resources_json", "metadata_json")
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_PREFIX_FIELD_NUMBER: _ClassVar[int]
    KEY_HASH_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    OWNER_TYPE_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPES_JSON_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    IP_ALLOWLIST_JSON_FIELD_NUMBER: _ClassVar[int]
    RATE_LIMIT_PER_MINUTE_FIELD_NUMBER: _ClassVar[int]
    RATE_LIMIT_PER_DAY_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    REVOKED_BY_FIELD_NUMBER: _ClassVar[int]
    REVOKE_REASON_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_USED_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_RESOURCES_JSON_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    key_id: str
    key_prefix: str
    key_hash: str
    name: str
    description: str
    owner_type: _enums_pb2.ApiKeyOwnerType
    owner_id: str
    scopes_json: str
    status: _enums_pb2.ApiKeyStatus
    ip_allowlist_json: str
    rate_limit_per_minute: int
    rate_limit_per_day: int
    created_by: str
    revoked_by: str
    revoke_reason: str
    expires_at: _timestamp_pb2.Timestamp
    last_used_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    deleted_at: _timestamp_pb2.Timestamp
    deleted_by: str
    tenant_id: str
    project_id: str
    allowed_resources_json: str
    metadata_json: str
    def __init__(self, key_id: _Optional[str] = ..., key_prefix: _Optional[str] = ..., key_hash: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., owner_type: _Optional[_Union[_enums_pb2.ApiKeyOwnerType, str]] = ..., owner_id: _Optional[str] = ..., scopes_json: _Optional[str] = ..., status: _Optional[_Union[_enums_pb2.ApiKeyStatus, str]] = ..., ip_allowlist_json: _Optional[str] = ..., rate_limit_per_minute: _Optional[int] = ..., rate_limit_per_day: _Optional[int] = ..., created_by: _Optional[str] = ..., revoked_by: _Optional[str] = ..., revoke_reason: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_used_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_by: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., allowed_resources_json: _Optional[str] = ..., metadata_json: _Optional[str] = ...) -> None: ...
