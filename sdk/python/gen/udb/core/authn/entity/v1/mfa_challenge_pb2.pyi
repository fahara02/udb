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

class MfaChallenge(_message.Message):
    __slots__ = ("challenge_id", "user_id", "tenant_id", "project_id", "factor_kind", "purpose", "device_fingerprint_hash", "ip_address_masked", "attempt_count", "expires_at", "consumed_at", "created_at")
    CHALLENGE_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    FACTOR_KIND_FIELD_NUMBER: _ClassVar[int]
    PURPOSE_FIELD_NUMBER: _ClassVar[int]
    DEVICE_FINGERPRINT_HASH_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_MASKED_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_COUNT_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    CONSUMED_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    challenge_id: str
    user_id: str
    tenant_id: str
    project_id: str
    factor_kind: _enums_pb2.AuthFactorKind
    purpose: _enums_pb2.MfaChallengePurpose
    device_fingerprint_hash: str
    ip_address_masked: str
    attempt_count: int
    expires_at: _timestamp_pb2.Timestamp
    consumed_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, challenge_id: _Optional[str] = ..., user_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., factor_kind: _Optional[_Union[_enums_pb2.AuthFactorKind, str]] = ..., purpose: _Optional[_Union[_enums_pb2.MfaChallengePurpose, str]] = ..., device_fingerprint_hash: _Optional[str] = ..., ip_address_masked: _Optional[str] = ..., attempt_count: _Optional[int] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., consumed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
