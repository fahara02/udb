import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Lock(_message.Message):
    __slots__ = ("lock_id", "tenant_id", "lock_name", "owner_id", "fencing_token", "lease_ttl_seconds", "status", "acquired_at", "expires_at", "metadata_json")
    LOCK_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    LOCK_NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    FENCING_TOKEN_FIELD_NUMBER: _ClassVar[int]
    LEASE_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ACQUIRED_AT_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    lock_id: str
    tenant_id: str
    lock_name: str
    owner_id: str
    fencing_token: int
    lease_ttl_seconds: int
    status: str
    acquired_at: _timestamp_pb2.Timestamp
    expires_at: _timestamp_pb2.Timestamp
    metadata_json: str
    def __init__(self, lock_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., lock_name: _Optional[str] = ..., owner_id: _Optional[str] = ..., fencing_token: _Optional[int] = ..., lease_ttl_seconds: _Optional[int] = ..., status: _Optional[str] = ..., acquired_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., metadata_json: _Optional[str] = ...) -> None: ...
