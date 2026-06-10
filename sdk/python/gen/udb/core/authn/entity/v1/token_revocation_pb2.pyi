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

class TokenRevocation(_message.Message):
    __slots__ = ("jti_hash", "token_type", "tenant_id", "expires_at", "revoked_at", "revoked_by", "reason")
    JTI_HASH_FIELD_NUMBER: _ClassVar[int]
    TOKEN_TYPE_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKED_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKED_BY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    jti_hash: str
    token_type: _enums_pb2.TokenType
    tenant_id: str
    expires_at: _timestamp_pb2.Timestamp
    revoked_at: _timestamp_pb2.Timestamp
    revoked_by: str
    reason: str
    def __init__(self, jti_hash: _Optional[str] = ..., token_type: _Optional[_Union[_enums_pb2.TokenType, str]] = ..., tenant_id: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revoked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revoked_by: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...
