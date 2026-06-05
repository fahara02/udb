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

class User(_message.Message):
    __slots__ = ("user_id", "username", "email", "password_hash", "account_kind", "status", "tenant_id", "full_name", "totp_secret_enc", "mfa_enabled", "failed_login_count", "locked_until", "email_verified_at", "last_login_at", "created_by", "created_at", "updated_at", "deleted_at", "deleted_by", "project_id", "external_provider_id", "external_subject", "locale", "timezone", "profile_attributes_json", "external_references_json", "phone", "phone_verified_at")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_HASH_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_KIND_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    FULL_NAME_FIELD_NUMBER: _ClassVar[int]
    TOTP_SECRET_ENC_FIELD_NUMBER: _ClassVar[int]
    MFA_ENABLED_FIELD_NUMBER: _ClassVar[int]
    FAILED_LOGIN_COUNT_FIELD_NUMBER: _ClassVar[int]
    LOCKED_UNTIL_FIELD_NUMBER: _ClassVar[int]
    EMAIL_VERIFIED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_LOGIN_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_SUBJECT_FIELD_NUMBER: _ClassVar[int]
    LOCALE_FIELD_NUMBER: _ClassVar[int]
    TIMEZONE_FIELD_NUMBER: _ClassVar[int]
    PROFILE_ATTRIBUTES_JSON_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_REFERENCES_JSON_FIELD_NUMBER: _ClassVar[int]
    PHONE_FIELD_NUMBER: _ClassVar[int]
    PHONE_VERIFIED_AT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    username: str
    email: str
    password_hash: str
    account_kind: _enums_pb2.AccountKind
    status: _enums_pb2.UserStatus
    tenant_id: str
    full_name: str
    totp_secret_enc: str
    mfa_enabled: bool
    failed_login_count: int
    locked_until: _timestamp_pb2.Timestamp
    email_verified_at: _timestamp_pb2.Timestamp
    last_login_at: _timestamp_pb2.Timestamp
    created_by: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    deleted_at: _timestamp_pb2.Timestamp
    deleted_by: str
    project_id: str
    external_provider_id: str
    external_subject: str
    locale: str
    timezone: str
    profile_attributes_json: str
    external_references_json: str
    phone: str
    phone_verified_at: _timestamp_pb2.Timestamp
    def __init__(self, user_id: _Optional[str] = ..., username: _Optional[str] = ..., email: _Optional[str] = ..., password_hash: _Optional[str] = ..., account_kind: _Optional[_Union[_enums_pb2.AccountKind, str]] = ..., status: _Optional[_Union[_enums_pb2.UserStatus, str]] = ..., tenant_id: _Optional[str] = ..., full_name: _Optional[str] = ..., totp_secret_enc: _Optional[str] = ..., mfa_enabled: bool = ..., failed_login_count: _Optional[int] = ..., locked_until: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., email_verified_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_login_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_by: _Optional[str] = ..., project_id: _Optional[str] = ..., external_provider_id: _Optional[str] = ..., external_subject: _Optional[str] = ..., locale: _Optional[str] = ..., timezone: _Optional[str] = ..., profile_attributes_json: _Optional[str] = ..., external_references_json: _Optional[str] = ..., phone: _Optional[str] = ..., phone_verified_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
