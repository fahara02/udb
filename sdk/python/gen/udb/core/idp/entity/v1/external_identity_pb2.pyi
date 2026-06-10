import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ExternalIdentity(_message.Message):
    __slots__ = ("external_identity_id", "tenant_id", "provider_id", "subject", "user_id", "email", "email_verified", "linked_at", "last_login_at", "deleted_at")
    EXTERNAL_IDENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    EMAIL_VERIFIED_FIELD_NUMBER: _ClassVar[int]
    LINKED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_LOGIN_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    external_identity_id: str
    tenant_id: str
    provider_id: str
    subject: str
    user_id: str
    email: str
    email_verified: bool
    linked_at: _timestamp_pb2.Timestamp
    last_login_at: _timestamp_pb2.Timestamp
    deleted_at: _timestamp_pb2.Timestamp
    def __init__(self, external_identity_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., subject: _Optional[str] = ..., user_id: _Optional[str] = ..., email: _Optional[str] = ..., email_verified: bool = ..., linked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_login_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
