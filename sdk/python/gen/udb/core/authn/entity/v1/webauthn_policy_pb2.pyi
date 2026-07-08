import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WebAuthnPolicy(_message.Message):
    __slots__ = ("policy_id", "tenant_id", "required_user_verification", "required_resident_key", "allowed_attestation_conveyance", "created_at", "updated_at")
    POLICY_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_USER_VERIFICATION_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_RESIDENT_KEY_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_ATTESTATION_CONVEYANCE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    policy_id: str
    tenant_id: str
    required_user_verification: str
    required_resident_key: str
    allowed_attestation_conveyance: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, policy_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., required_user_verification: _Optional[str] = ..., required_resident_key: _Optional[str] = ..., allowed_attestation_conveyance: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
