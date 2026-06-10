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

class SigningKey(_message.Message):
    __slots__ = ("key_id", "tenant_id", "algorithm", "public_material", "encrypted_private_material", "kms_key_ref", "state", "not_before", "not_after", "created_at", "retired_at", "created_by", "retired_by", "rotation_reason")
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_MATERIAL_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTED_PRIVATE_MATERIAL_FIELD_NUMBER: _ClassVar[int]
    KMS_KEY_REF_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    NOT_BEFORE_FIELD_NUMBER: _ClassVar[int]
    NOT_AFTER_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    RETIRED_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    RETIRED_BY_FIELD_NUMBER: _ClassVar[int]
    ROTATION_REASON_FIELD_NUMBER: _ClassVar[int]
    key_id: str
    tenant_id: str
    algorithm: str
    public_material: str
    encrypted_private_material: str
    kms_key_ref: str
    state: _enums_pb2.SigningKeyState
    not_before: _timestamp_pb2.Timestamp
    not_after: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    retired_at: _timestamp_pb2.Timestamp
    created_by: str
    retired_by: str
    rotation_reason: str
    def __init__(self, key_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., algorithm: _Optional[str] = ..., public_material: _Optional[str] = ..., encrypted_private_material: _Optional[str] = ..., kms_key_ref: _Optional[str] = ..., state: _Optional[_Union[_enums_pb2.SigningKeyState, str]] = ..., not_before: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., not_after: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., retired_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_by: _Optional[str] = ..., retired_by: _Optional[str] = ..., rotation_reason: _Optional[str] = ...) -> None: ...
