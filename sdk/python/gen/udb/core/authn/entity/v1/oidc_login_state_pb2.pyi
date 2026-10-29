import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OidcLoginState(_message.Message):
    __slots__ = ("oidc_login_state_id", "state_hash", "tenant_id", "provider_id", "redirect_uri", "nonce", "code_verifier", "invitation_id", "expires_at", "consumed_at", "created_at")
    OIDC_LOGIN_STATE_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_HASH_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    REDIRECT_URI_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    CODE_VERIFIER_FIELD_NUMBER: _ClassVar[int]
    INVITATION_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    CONSUMED_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    oidc_login_state_id: str
    state_hash: str
    tenant_id: str
    provider_id: str
    redirect_uri: str
    nonce: str
    code_verifier: str
    invitation_id: str
    expires_at: _timestamp_pb2.Timestamp
    consumed_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, oidc_login_state_id: _Optional[str] = ..., state_hash: _Optional[str] = ..., tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., redirect_uri: _Optional[str] = ..., nonce: _Optional[str] = ..., code_verifier: _Optional[str] = ..., invitation_id: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., consumed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
