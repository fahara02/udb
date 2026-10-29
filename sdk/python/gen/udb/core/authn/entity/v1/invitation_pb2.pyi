import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Invitation(_message.Message):
    __slots__ = ("invitation_id", "tenant_id", "project_id", "channel", "address", "idp_provider_id", "roles_json", "full_name", "status", "token_hash", "expires_at", "created_by", "invitee_user_id", "accepted_user_id", "accepted_at", "revoked_by", "revoke_reason", "send_count", "last_sent_at", "revision", "created_at", "updated_at", "locale")
    INVITATION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    IDP_PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    ROLES_JSON_FIELD_NUMBER: _ClassVar[int]
    FULL_NAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TOKEN_HASH_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    INVITEE_USER_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_USER_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKED_BY_FIELD_NUMBER: _ClassVar[int]
    REVOKE_REASON_FIELD_NUMBER: _ClassVar[int]
    SEND_COUNT_FIELD_NUMBER: _ClassVar[int]
    LAST_SENT_AT_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    LOCALE_FIELD_NUMBER: _ClassVar[int]
    invitation_id: str
    tenant_id: str
    project_id: str
    channel: str
    address: str
    idp_provider_id: str
    roles_json: str
    full_name: str
    status: str
    token_hash: str
    expires_at: _timestamp_pb2.Timestamp
    created_by: str
    invitee_user_id: str
    accepted_user_id: str
    accepted_at: _timestamp_pb2.Timestamp
    revoked_by: str
    revoke_reason: str
    send_count: int
    last_sent_at: _timestamp_pb2.Timestamp
    revision: int
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    locale: str
    def __init__(self, invitation_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., channel: _Optional[str] = ..., address: _Optional[str] = ..., idp_provider_id: _Optional[str] = ..., roles_json: _Optional[str] = ..., full_name: _Optional[str] = ..., status: _Optional[str] = ..., token_hash: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_by: _Optional[str] = ..., invitee_user_id: _Optional[str] = ..., accepted_user_id: _Optional[str] = ..., accepted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revoked_by: _Optional[str] = ..., revoke_reason: _Optional[str] = ..., send_count: _Optional[int] = ..., last_sent_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revision: _Optional[int] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., locale: _Optional[str] = ...) -> None: ...
