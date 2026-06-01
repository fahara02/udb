import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.authn.entity.v1 import enums_pb2 as _enums_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class UserRegisteredEvent(_message.Message):
    __slots__ = ("event_id", "user_id", "username", "email", "tenant_id", "created_by", "correlation_id", "occurred_at", "access_surface", "project_id", "account_kind", "contact_address", "provider_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    ACCESS_SURFACE_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_KIND_FIELD_NUMBER: _ClassVar[int]
    CONTACT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    user_id: str
    username: str
    email: str
    tenant_id: str
    created_by: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    access_surface: str
    project_id: str
    account_kind: _enums_pb2.AccountKind
    contact_address: str
    provider_id: str
    def __init__(self, event_id: _Optional[str] = ..., user_id: _Optional[str] = ..., username: _Optional[str] = ..., email: _Optional[str] = ..., tenant_id: _Optional[str] = ..., created_by: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., access_surface: _Optional[str] = ..., project_id: _Optional[str] = ..., account_kind: _Optional[_Union[_enums_pb2.AccountKind, str]] = ..., contact_address: _Optional[str] = ..., provider_id: _Optional[str] = ...) -> None: ...

class UserLoggedInEvent(_message.Message):
    __slots__ = ("event_id", "user_id", "session_id", "session_type", "device_type", "ip_address", "tenant_id", "correlation_id", "occurred_at", "project_id", "principal_id", "access_surface")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_TYPE_FIELD_NUMBER: _ClassVar[int]
    DEVICE_TYPE_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    ACCESS_SURFACE_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    user_id: str
    session_id: str
    session_type: _enums_pb2.SessionType
    device_type: _enums_pb2.DeviceType
    ip_address: str
    tenant_id: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    project_id: str
    principal_id: str
    access_surface: str
    def __init__(self, event_id: _Optional[str] = ..., user_id: _Optional[str] = ..., session_id: _Optional[str] = ..., session_type: _Optional[_Union[_enums_pb2.SessionType, str]] = ..., device_type: _Optional[_Union[_enums_pb2.DeviceType, str]] = ..., ip_address: _Optional[str] = ..., tenant_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., project_id: _Optional[str] = ..., principal_id: _Optional[str] = ..., access_surface: _Optional[str] = ...) -> None: ...

class SessionRevokedEvent(_message.Message):
    __slots__ = ("event_id", "session_id", "user_id", "revoked_by", "revoke_reason", "correlation_id", "occurred_at", "tenant_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    REVOKED_BY_FIELD_NUMBER: _ClassVar[int]
    REVOKE_REASON_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    session_id: str
    user_id: str
    revoked_by: str
    revoke_reason: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, event_id: _Optional[str] = ..., session_id: _Optional[str] = ..., user_id: _Optional[str] = ..., revoked_by: _Optional[str] = ..., revoke_reason: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class UserLockedEvent(_message.Message):
    __slots__ = ("event_id", "user_id", "attempt_count", "ip_address", "locked_until", "correlation_id", "occurred_at", "tenant_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_COUNT_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    LOCKED_UNTIL_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    user_id: str
    attempt_count: int
    ip_address: str
    locked_until: _timestamp_pb2.Timestamp
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, event_id: _Optional[str] = ..., user_id: _Optional[str] = ..., attempt_count: _Optional[int] = ..., ip_address: _Optional[str] = ..., locked_until: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class PasswordChangedEvent(_message.Message):
    __slots__ = ("event_id", "user_id", "is_reset", "changed_by", "correlation_id", "occurred_at", "tenant_id", "ip_address")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    IS_RESET_FIELD_NUMBER: _ClassVar[int]
    CHANGED_BY_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    user_id: str
    is_reset: bool
    changed_by: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    ip_address: str
    def __init__(self, event_id: _Optional[str] = ..., user_id: _Optional[str] = ..., is_reset: bool = ..., changed_by: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ..., ip_address: _Optional[str] = ...) -> None: ...

class OTPSentEvent(_message.Message):
    __slots__ = ("event_id", "otp_id", "user_id", "otp_type", "delivery_channel", "expires_in_seconds", "correlation_id", "occurred_at", "tenant_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    OTP_TYPE_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_IN_SECONDS_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    otp_id: str
    user_id: str
    otp_type: _enums_pb2.OTPType
    delivery_channel: str
    expires_in_seconds: int
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, event_id: _Optional[str] = ..., otp_id: _Optional[str] = ..., user_id: _Optional[str] = ..., otp_type: _Optional[_Union[_enums_pb2.OTPType, str]] = ..., delivery_channel: _Optional[str] = ..., expires_in_seconds: _Optional[int] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class UserStatusChangedEvent(_message.Message):
    __slots__ = ("event_id", "user_id", "old_status", "new_status", "changed_by", "reason", "correlation_id", "occurred_at", "tenant_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    OLD_STATUS_FIELD_NUMBER: _ClassVar[int]
    NEW_STATUS_FIELD_NUMBER: _ClassVar[int]
    CHANGED_BY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    user_id: str
    old_status: _enums_pb2.UserStatus
    new_status: _enums_pb2.UserStatus
    changed_by: str
    reason: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, event_id: _Optional[str] = ..., user_id: _Optional[str] = ..., old_status: _Optional[_Union[_enums_pb2.UserStatus, str]] = ..., new_status: _Optional[_Union[_enums_pb2.UserStatus, str]] = ..., changed_by: _Optional[str] = ..., reason: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class EmailVerifiedEvent(_message.Message):
    __slots__ = ("event_id", "user_id", "email", "correlation_id", "tenant_id", "occurred_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    user_id: str
    email: str
    correlation_id: str
    tenant_id: str
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., user_id: _Optional[str] = ..., email: _Optional[str] = ..., correlation_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PasswordResetRequestedEvent(_message.Message):
    __slots__ = ("event_id", "user_id", "contact_address", "ip_address", "device_type", "otp_id", "correlation_id", "occurred_at", "tenant_id", "project_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    CONTACT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    DEVICE_TYPE_FIELD_NUMBER: _ClassVar[int]
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    user_id: str
    contact_address: str
    ip_address: str
    device_type: _enums_pb2.DeviceType
    otp_id: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    project_id: str
    def __init__(self, event_id: _Optional[str] = ..., user_id: _Optional[str] = ..., contact_address: _Optional[str] = ..., ip_address: _Optional[str] = ..., device_type: _Optional[_Union[_enums_pb2.DeviceType, str]] = ..., otp_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ...) -> None: ...

class PasswordResetByEmailRequestedEvent(_message.Message):
    __slots__ = ("event_id", "user_id", "email_masked", "otp_id", "ip_address", "correlation_id", "occurred_at", "tenant_id")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    EMAIL_MASKED_FIELD_NUMBER: _ClassVar[int]
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    user_id: str
    email_masked: str
    otp_id: str
    ip_address: str
    correlation_id: str
    occurred_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, event_id: _Optional[str] = ..., user_id: _Optional[str] = ..., email_masked: _Optional[str] = ..., otp_id: _Optional[str] = ..., ip_address: _Optional[str] = ..., correlation_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...
