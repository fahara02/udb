import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.authn.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.authn.entity.v1 import session_pb2 as _session_pb2
from udb.core.authn.entity.v1 import user_pb2 as _user_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateUserRequest(_message.Message):
    __slots__ = ("username", "email", "password", "tenant_id", "full_name", "context", "account_kind", "project_id", "external_provider_id", "external_subject", "profile_attributes")
    class ProfileAttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    FULL_NAME_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_KIND_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_SUBJECT_FIELD_NUMBER: _ClassVar[int]
    PROFILE_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    username: str
    email: str
    password: str
    tenant_id: str
    full_name: str
    context: _types_pb2.RequestContext
    account_kind: _enums_pb2.AccountKind
    project_id: str
    external_provider_id: str
    external_subject: str
    profile_attributes: _containers.ScalarMap[str, str]
    def __init__(self, username: _Optional[str] = ..., email: _Optional[str] = ..., password: _Optional[str] = ..., tenant_id: _Optional[str] = ..., full_name: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ..., account_kind: _Optional[_Union[_enums_pb2.AccountKind, str]] = ..., project_id: _Optional[str] = ..., external_provider_id: _Optional[str] = ..., external_subject: _Optional[str] = ..., profile_attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CreateUserResponse(_message.Message):
    __slots__ = ("user", "otp_id")
    USER_FIELD_NUMBER: _ClassVar[int]
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    user: _user_pb2.User
    otp_id: str
    def __init__(self, user: _Optional[_Union[_user_pb2.User, _Mapping]] = ..., otp_id: _Optional[str] = ...) -> None: ...

class GetUserRequest(_message.Message):
    __slots__ = ("user_id", "username", "email")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    username: str
    email: str
    def __init__(self, user_id: _Optional[str] = ..., username: _Optional[str] = ..., email: _Optional[str] = ...) -> None: ...

class GetUserResponse(_message.Message):
    __slots__ = ("user",)
    USER_FIELD_NUMBER: _ClassVar[int]
    user: _user_pb2.User
    def __init__(self, user: _Optional[_Union[_user_pb2.User, _Mapping]] = ...) -> None: ...

class ListUsersRequest(_message.Message):
    __slots__ = ("tenant_id", "account_kind", "status", "page")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_KIND_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    account_kind: _enums_pb2.AccountKind
    status: _enums_pb2.UserStatus
    page: _dto_pb2.PageRequest
    def __init__(self, tenant_id: _Optional[str] = ..., account_kind: _Optional[_Union[_enums_pb2.AccountKind, str]] = ..., status: _Optional[_Union[_enums_pb2.UserStatus, str]] = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListUsersResponse(_message.Message):
    __slots__ = ("users", "page")
    USERS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    users: _containers.RepeatedCompositeFieldContainer[_user_pb2.User]
    page: _dto_pb2.PageResponse
    def __init__(self, users: _Optional[_Iterable[_Union[_user_pb2.User, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class UpdateUserRequest(_message.Message):
    __slots__ = ("user_id", "full_name", "email", "tenant_id", "context", "account_kind", "project_id", "profile_attributes", "external_provider_id", "external_subject")
    class ProfileAttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    FULL_NAME_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_KIND_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    PROFILE_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_SUBJECT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    full_name: str
    email: str
    tenant_id: str
    context: _types_pb2.RequestContext
    account_kind: _enums_pb2.AccountKind
    project_id: str
    profile_attributes: _containers.ScalarMap[str, str]
    external_provider_id: str
    external_subject: str
    def __init__(self, user_id: _Optional[str] = ..., full_name: _Optional[str] = ..., email: _Optional[str] = ..., tenant_id: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ..., account_kind: _Optional[_Union[_enums_pb2.AccountKind, str]] = ..., project_id: _Optional[str] = ..., profile_attributes: _Optional[_Mapping[str, str]] = ..., external_provider_id: _Optional[str] = ..., external_subject: _Optional[str] = ...) -> None: ...

class UpdateUserResponse(_message.Message):
    __slots__ = ("user",)
    USER_FIELD_NUMBER: _ClassVar[int]
    user: _user_pb2.User
    def __init__(self, user: _Optional[_Union[_user_pb2.User, _Mapping]] = ...) -> None: ...

class ChangeUserStatusRequest(_message.Message):
    __slots__ = ("user_id", "new_status", "reason", "context")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    NEW_STATUS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    new_status: _enums_pb2.UserStatus
    reason: str
    context: _types_pb2.RequestContext
    def __init__(self, user_id: _Optional[str] = ..., new_status: _Optional[_Union[_enums_pb2.UserStatus, str]] = ..., reason: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class ChangeUserStatusResponse(_message.Message):
    __slots__ = ("user",)
    USER_FIELD_NUMBER: _ClassVar[int]
    user: _user_pb2.User
    def __init__(self, user: _Optional[_Union[_user_pb2.User, _Mapping]] = ...) -> None: ...

class AdminResetPasswordRequest(_message.Message):
    __slots__ = ("user_id", "context")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    context: _types_pb2.RequestContext
    def __init__(self, user_id: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class AdminResetPasswordResponse(_message.Message):
    __slots__ = ("otp_id",)
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    otp_id: str
    def __init__(self, otp_id: _Optional[str] = ...) -> None: ...

class SendOTPRequest(_message.Message):
    __slots__ = ("user_id", "otp_type", "correlation_id", "context")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    OTP_TYPE_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    otp_type: _enums_pb2.OTPType
    correlation_id: str
    context: _types_pb2.RequestContext
    def __init__(self, user_id: _Optional[str] = ..., otp_type: _Optional[_Union[_enums_pb2.OTPType, str]] = ..., correlation_id: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class SendOTPResponse(_message.Message):
    __slots__ = ("otp_id", "expires_in_seconds", "cooldown_seconds")
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_IN_SECONDS_FIELD_NUMBER: _ClassVar[int]
    COOLDOWN_SECONDS_FIELD_NUMBER: _ClassVar[int]
    otp_id: str
    expires_in_seconds: int
    cooldown_seconds: int
    def __init__(self, otp_id: _Optional[str] = ..., expires_in_seconds: _Optional[int] = ..., cooldown_seconds: _Optional[int] = ...) -> None: ...

class VerifyOTPRequest(_message.Message):
    __slots__ = ("otp_id", "code")
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    otp_id: str
    code: str
    def __init__(self, otp_id: _Optional[str] = ..., code: _Optional[str] = ...) -> None: ...

class VerifyOTPResponse(_message.Message):
    __slots__ = ("verified", "user_id", "otp_type")
    VERIFIED_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    OTP_TYPE_FIELD_NUMBER: _ClassVar[int]
    verified: bool
    user_id: str
    otp_type: _enums_pb2.OTPType
    def __init__(self, verified: bool = ..., user_id: _Optional[str] = ..., otp_type: _Optional[_Union[_enums_pb2.OTPType, str]] = ...) -> None: ...

class ResendOTPRequest(_message.Message):
    __slots__ = ("original_otp_id", "reason")
    ORIGINAL_OTP_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    original_otp_id: str
    reason: str
    def __init__(self, original_otp_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class ResendOTPResponse(_message.Message):
    __slots__ = ("otp_id", "expires_in_seconds", "cooldown_seconds", "attempts_remaining")
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_IN_SECONDS_FIELD_NUMBER: _ClassVar[int]
    COOLDOWN_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_REMAINING_FIELD_NUMBER: _ClassVar[int]
    otp_id: str
    expires_in_seconds: int
    cooldown_seconds: int
    attempts_remaining: int
    def __init__(self, otp_id: _Optional[str] = ..., expires_in_seconds: _Optional[int] = ..., cooldown_seconds: _Optional[int] = ..., attempts_remaining: _Optional[int] = ...) -> None: ...

class Principal(_message.Message):
    __slots__ = ("principal_id", "subject", "user_id", "service_identity", "tenant_id", "project_id", "scopes", "roles", "provider_id", "auth_method", "expires_at_unix", "account_kind", "domain", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SERVICE_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    AUTH_METHOD_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_KIND_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    principal_id: str
    subject: str
    user_id: str
    service_identity: str
    tenant_id: str
    project_id: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    roles: _containers.RepeatedScalarFieldContainer[str]
    provider_id: str
    auth_method: str
    expires_at_unix: int
    account_kind: _enums_pb2.AccountKind
    domain: str
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, principal_id: _Optional[str] = ..., subject: _Optional[str] = ..., user_id: _Optional[str] = ..., service_identity: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., roles: _Optional[_Iterable[str]] = ..., provider_id: _Optional[str] = ..., auth_method: _Optional[str] = ..., expires_at_unix: _Optional[int] = ..., account_kind: _Optional[_Union[_enums_pb2.AccountKind, str]] = ..., domain: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class AuthnRequest(_message.Message):
    __slots__ = ("bearer_token", "session_id", "api_key", "external_provider_id", "external_token", "tenant_hint", "project_hint", "requested_scopes", "attributes", "credential_type", "client_id", "audience", "issuer")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    BEARER_TOKEN_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    API_KEY_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TENANT_HINT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_HINT_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_SCOPES_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_FIELD_NUMBER: _ClassVar[int]
    ISSUER_FIELD_NUMBER: _ClassVar[int]
    bearer_token: str
    session_id: str
    api_key: str
    external_provider_id: str
    external_token: str
    tenant_hint: str
    project_hint: str
    requested_scopes: _containers.RepeatedScalarFieldContainer[str]
    attributes: _containers.ScalarMap[str, str]
    credential_type: _enums_pb2.AuthCredentialType
    client_id: str
    audience: str
    issuer: str
    def __init__(self, bearer_token: _Optional[str] = ..., session_id: _Optional[str] = ..., api_key: _Optional[str] = ..., external_provider_id: _Optional[str] = ..., external_token: _Optional[str] = ..., tenant_hint: _Optional[str] = ..., project_hint: _Optional[str] = ..., requested_scopes: _Optional[_Iterable[str]] = ..., attributes: _Optional[_Mapping[str, str]] = ..., credential_type: _Optional[_Union[_enums_pb2.AuthCredentialType, str]] = ..., client_id: _Optional[str] = ..., audience: _Optional[str] = ..., issuer: _Optional[str] = ...) -> None: ...

class AuthnResponse(_message.Message):
    __slots__ = ("principal", "session_id", "access_token", "expires_at_unix", "relationship_version", "warnings")
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    ACCESS_TOKEN_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIP_VERSION_FIELD_NUMBER: _ClassVar[int]
    WARNINGS_FIELD_NUMBER: _ClassVar[int]
    principal: Principal
    session_id: str
    access_token: str
    expires_at_unix: int
    relationship_version: str
    warnings: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, principal: _Optional[_Union[Principal, _Mapping]] = ..., session_id: _Optional[str] = ..., access_token: _Optional[str] = ..., expires_at_unix: _Optional[int] = ..., relationship_version: _Optional[str] = ..., warnings: _Optional[_Iterable[str]] = ...) -> None: ...

class LoginRequest(_message.Message):
    __slots__ = ("username", "password", "device_type", "device_name", "ip_address", "user_agent", "device_id", "mfa_otp_id", "totp_code", "tenant_hint", "project_hint", "access_surface", "recovery_code")
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    DEVICE_TYPE_FIELD_NUMBER: _ClassVar[int]
    DEVICE_NAME_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    USER_AGENT_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MFA_OTP_ID_FIELD_NUMBER: _ClassVar[int]
    TOTP_CODE_FIELD_NUMBER: _ClassVar[int]
    TENANT_HINT_FIELD_NUMBER: _ClassVar[int]
    PROJECT_HINT_FIELD_NUMBER: _ClassVar[int]
    ACCESS_SURFACE_FIELD_NUMBER: _ClassVar[int]
    RECOVERY_CODE_FIELD_NUMBER: _ClassVar[int]
    username: str
    password: str
    device_type: _enums_pb2.DeviceType
    device_name: str
    ip_address: str
    user_agent: str
    device_id: str
    mfa_otp_id: str
    totp_code: str
    tenant_hint: str
    project_hint: str
    access_surface: str
    recovery_code: str
    def __init__(self, username: _Optional[str] = ..., password: _Optional[str] = ..., device_type: _Optional[_Union[_enums_pb2.DeviceType, str]] = ..., device_name: _Optional[str] = ..., ip_address: _Optional[str] = ..., user_agent: _Optional[str] = ..., device_id: _Optional[str] = ..., mfa_otp_id: _Optional[str] = ..., totp_code: _Optional[str] = ..., tenant_hint: _Optional[str] = ..., project_hint: _Optional[str] = ..., access_surface: _Optional[str] = ..., recovery_code: _Optional[str] = ...) -> None: ...

class LoginResponse(_message.Message):
    __slots__ = ("user_id", "session_id", "access_token", "refresh_token", "access_token_expires_in", "session_token", "csrf_token", "mfa_required", "mfa_otp_id")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    ACCESS_TOKEN_FIELD_NUMBER: _ClassVar[int]
    REFRESH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    ACCESS_TOKEN_EXPIRES_IN_FIELD_NUMBER: _ClassVar[int]
    SESSION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    CSRF_TOKEN_FIELD_NUMBER: _ClassVar[int]
    MFA_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    MFA_OTP_ID_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    session_id: str
    access_token: str
    refresh_token: str
    access_token_expires_in: int
    session_token: str
    csrf_token: str
    mfa_required: bool
    mfa_otp_id: str
    def __init__(self, user_id: _Optional[str] = ..., session_id: _Optional[str] = ..., access_token: _Optional[str] = ..., refresh_token: _Optional[str] = ..., access_token_expires_in: _Optional[int] = ..., session_token: _Optional[str] = ..., csrf_token: _Optional[str] = ..., mfa_required: bool = ..., mfa_otp_id: _Optional[str] = ...) -> None: ...

class RefreshTokenRequest(_message.Message):
    __slots__ = ("refresh_token", "session_id")
    REFRESH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    refresh_token: str
    session_id: str
    def __init__(self, refresh_token: _Optional[str] = ..., session_id: _Optional[str] = ...) -> None: ...

class RefreshTokenResponse(_message.Message):
    __slots__ = ("access_token", "access_token_expires_in")
    ACCESS_TOKEN_FIELD_NUMBER: _ClassVar[int]
    ACCESS_TOKEN_EXPIRES_IN_FIELD_NUMBER: _ClassVar[int]
    access_token: str
    access_token_expires_in: int
    def __init__(self, access_token: _Optional[str] = ..., access_token_expires_in: _Optional[int] = ...) -> None: ...

class LogoutRequest(_message.Message):
    __slots__ = ("session_id", "all_sessions", "revoke_reason", "context")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    ALL_SESSIONS_FIELD_NUMBER: _ClassVar[int]
    REVOKE_REASON_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    all_sessions: bool
    revoke_reason: str
    context: _types_pb2.RequestContext
    def __init__(self, session_id: _Optional[str] = ..., all_sessions: bool = ..., revoke_reason: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class LogoutResponse(_message.Message):
    __slots__ = ("sessions_revoked",)
    SESSIONS_REVOKED_FIELD_NUMBER: _ClassVar[int]
    sessions_revoked: int
    def __init__(self, sessions_revoked: _Optional[int] = ...) -> None: ...

class ChangePasswordRequest(_message.Message):
    __slots__ = ("user_id", "current_password", "new_password", "otp_id", "context")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    CURRENT_PASSWORD_FIELD_NUMBER: _ClassVar[int]
    NEW_PASSWORD_FIELD_NUMBER: _ClassVar[int]
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    current_password: str
    new_password: str
    otp_id: str
    context: _types_pb2.RequestContext
    def __init__(self, user_id: _Optional[str] = ..., current_password: _Optional[str] = ..., new_password: _Optional[str] = ..., otp_id: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class ChangePasswordResponse(_message.Message):
    __slots__ = ("user_id", "changed_at", "operation_id")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    CHANGED_AT_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    changed_at: _timestamp_pb2.Timestamp
    operation_id: str
    def __init__(self, user_id: _Optional[str] = ..., changed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., operation_id: _Optional[str] = ...) -> None: ...

class ValidateTokenRequest(_message.Message):
    __slots__ = ("token", "token_type")
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOKEN_TYPE_FIELD_NUMBER: _ClassVar[int]
    token: str
    token_type: _enums_pb2.TokenType
    def __init__(self, token: _Optional[str] = ..., token_type: _Optional[_Union[_enums_pb2.TokenType, str]] = ...) -> None: ...

class ValidateTokenResponse(_message.Message):
    __slots__ = ("valid", "user_id", "session_id", "account_kind", "tenant_id", "roles", "expires_at", "access_surface", "device_id", "token_id", "session_type", "principal", "project_id", "scopes", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    VALID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_KIND_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    ACCESS_SURFACE_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    TOKEN_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_TYPE_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    valid: bool
    user_id: str
    session_id: str
    account_kind: _enums_pb2.AccountKind
    tenant_id: str
    roles: _containers.RepeatedScalarFieldContainer[str]
    expires_at: _timestamp_pb2.Timestamp
    access_surface: str
    device_id: str
    token_id: str
    session_type: _enums_pb2.SessionType
    principal: Principal
    project_id: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, valid: bool = ..., user_id: _Optional[str] = ..., session_id: _Optional[str] = ..., account_kind: _Optional[_Union[_enums_pb2.AccountKind, str]] = ..., tenant_id: _Optional[str] = ..., roles: _Optional[_Iterable[str]] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., access_surface: _Optional[str] = ..., device_id: _Optional[str] = ..., token_id: _Optional[str] = ..., session_type: _Optional[_Union[_enums_pb2.SessionType, str]] = ..., principal: _Optional[_Union[Principal, _Mapping]] = ..., project_id: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CreateSessionRequest(_message.Message):
    __slots__ = ("principal", "ttl_seconds", "client_fingerprint")
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    CLIENT_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    principal: Principal
    ttl_seconds: int
    client_fingerprint: str
    def __init__(self, principal: _Optional[_Union[Principal, _Mapping]] = ..., ttl_seconds: _Optional[int] = ..., client_fingerprint: _Optional[str] = ...) -> None: ...

class CreateSessionResponse(_message.Message):
    __slots__ = ("session_id", "expires_at_unix")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    expires_at_unix: int
    def __init__(self, session_id: _Optional[str] = ..., expires_at_unix: _Optional[int] = ...) -> None: ...

class RefreshSessionRequest(_message.Message):
    __slots__ = ("session_id", "ttl_seconds")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    ttl_seconds: int
    def __init__(self, session_id: _Optional[str] = ..., ttl_seconds: _Optional[int] = ...) -> None: ...

class RefreshSessionResponse(_message.Message):
    __slots__ = ("expires_at_unix", "active")
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    expires_at_unix: int
    active: bool
    def __init__(self, expires_at_unix: _Optional[int] = ..., active: bool = ...) -> None: ...

class GetSessionRequest(_message.Message):
    __slots__ = ("session_id",)
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    def __init__(self, session_id: _Optional[str] = ...) -> None: ...

class GetSessionResponse(_message.Message):
    __slots__ = ("session",)
    SESSION_FIELD_NUMBER: _ClassVar[int]
    session: _session_pb2.Session
    def __init__(self, session: _Optional[_Union[_session_pb2.Session, _Mapping]] = ...) -> None: ...

class ListSessionsRequest(_message.Message):
    __slots__ = ("user_id", "active_only", "page")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_ONLY_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    active_only: bool
    page: _dto_pb2.PageRequest
    def __init__(self, user_id: _Optional[str] = ..., active_only: bool = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListSessionsResponse(_message.Message):
    __slots__ = ("sessions", "page")
    SESSIONS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    sessions: _containers.RepeatedCompositeFieldContainer[_session_pb2.Session]
    page: _dto_pb2.PageResponse
    def __init__(self, sessions: _Optional[_Iterable[_Union[_session_pb2.Session, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class RevokeSessionRequest(_message.Message):
    __slots__ = ("session_id", "revoke_reason", "context", "principal_id", "all_for_principal")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    REVOKE_REASON_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    ALL_FOR_PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    revoke_reason: str
    context: _types_pb2.RequestContext
    principal_id: str
    all_for_principal: bool
    def __init__(self, session_id: _Optional[str] = ..., revoke_reason: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ..., principal_id: _Optional[str] = ..., all_for_principal: bool = ...) -> None: ...

class RevokeSessionResponse(_message.Message):
    __slots__ = ("session_id", "revoked_at", "operation_id", "revoked_count")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    REVOKED_AT_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    REVOKED_COUNT_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    revoked_at: _timestamp_pb2.Timestamp
    operation_id: str
    revoked_count: int
    def __init__(self, session_id: _Optional[str] = ..., revoked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., operation_id: _Optional[str] = ..., revoked_count: _Optional[int] = ...) -> None: ...

class ValidateCSRFRequest(_message.Message):
    __slots__ = ("session_id", "csrf_token")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CSRF_TOKEN_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    csrf_token: str
    def __init__(self, session_id: _Optional[str] = ..., csrf_token: _Optional[str] = ...) -> None: ...

class ValidateCSRFResponse(_message.Message):
    __slots__ = ("valid",)
    VALID_FIELD_NUMBER: _ClassVar[int]
    valid: bool
    def __init__(self, valid: bool = ...) -> None: ...

class EnrollMFARequest(_message.Message):
    __slots__ = ("user_id", "mfa_type", "context")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    MFA_TYPE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    mfa_type: _enums_pb2.AuthFactorKind
    context: _types_pb2.RequestContext
    def __init__(self, user_id: _Optional[str] = ..., mfa_type: _Optional[_Union[_enums_pb2.AuthFactorKind, str]] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class EnrollMFAResponse(_message.Message):
    __slots__ = ("totp_secret", "totp_qr_uri", "verify_otp_id")
    TOTP_SECRET_FIELD_NUMBER: _ClassVar[int]
    TOTP_QR_URI_FIELD_NUMBER: _ClassVar[int]
    VERIFY_OTP_ID_FIELD_NUMBER: _ClassVar[int]
    totp_secret: str
    totp_qr_uri: str
    verify_otp_id: str
    def __init__(self, totp_secret: _Optional[str] = ..., totp_qr_uri: _Optional[str] = ..., verify_otp_id: _Optional[str] = ...) -> None: ...

class ConfirmMFAEnrollmentRequest(_message.Message):
    __slots__ = ("user_id", "otp_id", "code", "context")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    otp_id: str
    code: str
    context: _types_pb2.RequestContext
    def __init__(self, user_id: _Optional[str] = ..., otp_id: _Optional[str] = ..., code: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class ConfirmMFAEnrollmentResponse(_message.Message):
    __slots__ = ("enrolled",)
    ENROLLED_FIELD_NUMBER: _ClassVar[int]
    enrolled: bool
    def __init__(self, enrolled: bool = ...) -> None: ...

class GenerateRecoveryCodesRequest(_message.Message):
    __slots__ = ("user_id", "count", "context")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    count: int
    context: _types_pb2.RequestContext
    def __init__(self, user_id: _Optional[str] = ..., count: _Optional[int] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class GenerateRecoveryCodesResponse(_message.Message):
    __slots__ = ("codes", "generated")
    CODES_FIELD_NUMBER: _ClassVar[int]
    GENERATED_FIELD_NUMBER: _ClassVar[int]
    codes: _containers.RepeatedScalarFieldContainer[str]
    generated: int
    def __init__(self, codes: _Optional[_Iterable[str]] = ..., generated: _Optional[int] = ...) -> None: ...

class PutMfaPolicyRequest(_message.Message):
    __slots__ = ("tenant_id", "require_mfa", "context")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    REQUIRE_MFA_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    require_mfa: bool
    context: _types_pb2.RequestContext
    def __init__(self, tenant_id: _Optional[str] = ..., require_mfa: bool = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class PutMfaPolicyResponse(_message.Message):
    __slots__ = ("tenant_id", "require_mfa")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    REQUIRE_MFA_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    require_mfa: bool
    def __init__(self, tenant_id: _Optional[str] = ..., require_mfa: bool = ...) -> None: ...

class GetMfaPolicyRequest(_message.Message):
    __slots__ = ("tenant_id", "context")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    context: _types_pb2.RequestContext
    def __init__(self, tenant_id: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class GetMfaPolicyResponse(_message.Message):
    __slots__ = ("tenant_id", "require_mfa")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    REQUIRE_MFA_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    require_mfa: bool
    def __init__(self, tenant_id: _Optional[str] = ..., require_mfa: bool = ...) -> None: ...

class ForgotPasswordRequest(_message.Message):
    __slots__ = ("identifier", "context")
    IDENTIFIER_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    identifier: str
    context: _types_pb2.RequestContext
    def __init__(self, identifier: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class ForgotPasswordResponse(_message.Message):
    __slots__ = ("otp_id",)
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    otp_id: str
    def __init__(self, otp_id: _Optional[str] = ...) -> None: ...

class ResetPasswordRequest(_message.Message):
    __slots__ = ("otp_id", "code", "new_password", "context")
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    NEW_PASSWORD_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    otp_id: str
    code: str
    new_password: str
    context: _types_pb2.RequestContext
    def __init__(self, otp_id: _Optional[str] = ..., code: _Optional[str] = ..., new_password: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class ResetPasswordResponse(_message.Message):
    __slots__ = ("user_id", "changed_at_unix")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    CHANGED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    changed_at_unix: int
    def __init__(self, user_id: _Optional[str] = ..., changed_at_unix: _Optional[int] = ...) -> None: ...

class IntrospectTokenRequest(_message.Message):
    __slots__ = ("token", "context")
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    token: str
    context: _types_pb2.RequestContext
    def __init__(self, token: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class IntrospectTokenResponse(_message.Message):
    __slots__ = ("active", "subject", "tenant_id", "service_identity", "scopes", "expires_at_unix")
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SERVICE_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    active: bool
    subject: str
    tenant_id: str
    service_identity: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    expires_at_unix: int
    def __init__(self, active: bool = ..., subject: _Optional[str] = ..., tenant_id: _Optional[str] = ..., service_identity: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., expires_at_unix: _Optional[int] = ...) -> None: ...

class GetJwksRequest(_message.Message):
    __slots__ = ("context",)
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    context: _types_pb2.RequestContext
    def __init__(self, context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class GetJwksResponse(_message.Message):
    __slots__ = ("jwks_json",)
    JWKS_JSON_FIELD_NUMBER: _ClassVar[int]
    jwks_json: str
    def __init__(self, jwks_json: _Optional[str] = ...) -> None: ...

class SendPhoneVerificationRequest(_message.Message):
    __slots__ = ("user_id", "phone", "context")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    PHONE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    phone: str
    context: _types_pb2.RequestContext
    def __init__(self, user_id: _Optional[str] = ..., phone: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class SendPhoneVerificationResponse(_message.Message):
    __slots__ = ("otp_id",)
    OTP_ID_FIELD_NUMBER: _ClassVar[int]
    otp_id: str
    def __init__(self, otp_id: _Optional[str] = ...) -> None: ...

class StartWebAuthnRegistrationRequest(_message.Message):
    __slots__ = ("user_id", "label", "tenant_id", "project_id", "context")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    label: str
    tenant_id: str
    project_id: str
    context: _types_pb2.RequestContext
    def __init__(self, user_id: _Optional[str] = ..., label: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class StartWebAuthnRegistrationResponse(_message.Message):
    __slots__ = ("challenge_id", "public_key_credential_creation_options_json", "expires_at_unix")
    CHALLENGE_ID_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_KEY_CREDENTIAL_CREATION_OPTIONS_JSON_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    challenge_id: str
    public_key_credential_creation_options_json: str
    expires_at_unix: int
    def __init__(self, challenge_id: _Optional[str] = ..., public_key_credential_creation_options_json: _Optional[str] = ..., expires_at_unix: _Optional[int] = ...) -> None: ...

class FinishWebAuthnRegistrationRequest(_message.Message):
    __slots__ = ("challenge_id", "public_key_credential_json", "label", "context")
    CHALLENGE_ID_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_KEY_CREDENTIAL_JSON_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    challenge_id: str
    public_key_credential_json: str
    label: str
    context: _types_pb2.RequestContext
    def __init__(self, challenge_id: _Optional[str] = ..., public_key_credential_json: _Optional[str] = ..., label: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class FinishWebAuthnRegistrationResponse(_message.Message):
    __slots__ = ("registered", "credential_id", "user_id")
    REGISTERED_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    registered: bool
    credential_id: str
    user_id: str
    def __init__(self, registered: bool = ..., credential_id: _Optional[str] = ..., user_id: _Optional[str] = ...) -> None: ...

class StartWebAuthnAuthenticationRequest(_message.Message):
    __slots__ = ("user_id", "tenant_id", "project_id", "context")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    tenant_id: str
    project_id: str
    context: _types_pb2.RequestContext
    def __init__(self, user_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class StartWebAuthnAuthenticationResponse(_message.Message):
    __slots__ = ("challenge_id", "public_key_credential_request_options_json", "expires_at_unix")
    CHALLENGE_ID_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_KEY_CREDENTIAL_REQUEST_OPTIONS_JSON_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    challenge_id: str
    public_key_credential_request_options_json: str
    expires_at_unix: int
    def __init__(self, challenge_id: _Optional[str] = ..., public_key_credential_request_options_json: _Optional[str] = ..., expires_at_unix: _Optional[int] = ...) -> None: ...

class FinishWebAuthnAuthenticationRequest(_message.Message):
    __slots__ = ("challenge_id", "public_key_credential_json", "context")
    CHALLENGE_ID_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_KEY_CREDENTIAL_JSON_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    challenge_id: str
    public_key_credential_json: str
    context: _types_pb2.RequestContext
    def __init__(self, challenge_id: _Optional[str] = ..., public_key_credential_json: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class FinishWebAuthnAuthenticationResponse(_message.Message):
    __slots__ = ("principal", "session_id", "access_token", "expires_at_unix", "credential_id")
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    ACCESS_TOKEN_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_ID_FIELD_NUMBER: _ClassVar[int]
    principal: Principal
    session_id: str
    access_token: str
    expires_at_unix: int
    credential_id: str
    def __init__(self, principal: _Optional[_Union[Principal, _Mapping]] = ..., session_id: _Optional[str] = ..., access_token: _Optional[str] = ..., expires_at_unix: _Optional[int] = ..., credential_id: _Optional[str] = ...) -> None: ...
