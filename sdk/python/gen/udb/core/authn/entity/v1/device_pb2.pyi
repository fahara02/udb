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

class Device(_message.Message):
    __slots__ = ("device_id", "user_id", "tenant_id", "project_id", "device_name", "device_type", "fingerprint_hash", "last_ip_masked", "last_user_agent_hash", "last_seen_at", "created_at", "revoked_at", "revoked_by")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_NAME_FIELD_NUMBER: _ClassVar[int]
    DEVICE_TYPE_FIELD_NUMBER: _ClassVar[int]
    FINGERPRINT_HASH_FIELD_NUMBER: _ClassVar[int]
    LAST_IP_MASKED_FIELD_NUMBER: _ClassVar[int]
    LAST_USER_AGENT_HASH_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKED_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKED_BY_FIELD_NUMBER: _ClassVar[int]
    device_id: str
    user_id: str
    tenant_id: str
    project_id: str
    device_name: str
    device_type: _enums_pb2.DeviceType
    fingerprint_hash: str
    last_ip_masked: str
    last_user_agent_hash: str
    last_seen_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    revoked_at: _timestamp_pb2.Timestamp
    revoked_by: str
    def __init__(self, device_id: _Optional[str] = ..., user_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., device_name: _Optional[str] = ..., device_type: _Optional[_Union[_enums_pb2.DeviceType, str]] = ..., fingerprint_hash: _Optional[str] = ..., last_ip_masked: _Optional[str] = ..., last_user_agent_hash: _Optional[str] = ..., last_seen_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revoked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revoked_by: _Optional[str] = ...) -> None: ...
