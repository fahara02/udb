from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class AccountKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACCOUNT_KIND_UNSPECIFIED: _ClassVar[AccountKind]
    ACCOUNT_KIND_PERSON: _ClassVar[AccountKind]
    ACCOUNT_KIND_SERVICE_ACCOUNT: _ClassVar[AccountKind]
    ACCOUNT_KIND_WORKLOAD: _ClassVar[AccountKind]
    ACCOUNT_KIND_EXTERNAL_IDENTITY: _ClassVar[AccountKind]
    ACCOUNT_KIND_SYSTEM: _ClassVar[AccountKind]
    ACCOUNT_KIND_ANONYMOUS: _ClassVar[AccountKind]

class UserStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    USER_STATUS_UNSPECIFIED: _ClassVar[UserStatus]
    USER_STATUS_PENDING_VERIFICATION: _ClassVar[UserStatus]
    USER_STATUS_ACTIVE: _ClassVar[UserStatus]
    USER_STATUS_SUSPENDED: _ClassVar[UserStatus]
    USER_STATUS_LOCKED: _ClassVar[UserStatus]
    USER_STATUS_DEACTIVATED: _ClassVar[UserStatus]

class SessionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SESSION_TYPE_UNSPECIFIED: _ClassVar[SessionType]
    SESSION_TYPE_SERVER_SIDE: _ClassVar[SessionType]
    SESSION_TYPE_JWT: _ClassVar[SessionType]
    SESSION_TYPE_API_KEY: _ClassVar[SessionType]
    SESSION_TYPE_MTLS: _ClassVar[SessionType]
    SESSION_TYPE_EXTERNAL: _ClassVar[SessionType]

class DeviceType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DEVICE_TYPE_UNSPECIFIED: _ClassVar[DeviceType]
    DEVICE_TYPE_WEB: _ClassVar[DeviceType]
    DEVICE_TYPE_API: _ClassVar[DeviceType]
    DEVICE_TYPE_DESKTOP: _ClassVar[DeviceType]
    DEVICE_TYPE_MOBILE: _ClassVar[DeviceType]
    DEVICE_TYPE_WORKER: _ClassVar[DeviceType]
    DEVICE_TYPE_CLI: _ClassVar[DeviceType]

class OTPType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OTP_TYPE_UNSPECIFIED: _ClassVar[OTPType]
    OTP_TYPE_EMAIL_VERIFICATION: _ClassVar[OTPType]
    OTP_TYPE_LOGIN_2FA: _ClassVar[OTPType]
    OTP_TYPE_PASSWORD_RESET: _ClassVar[OTPType]
    OTP_TYPE_SENSITIVE_OPERATION: _ClassVar[OTPType]
    OTP_TYPE_PHONE_VERIFICATION: _ClassVar[OTPType]

class OTPStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OTP_STATUS_UNSPECIFIED: _ClassVar[OTPStatus]
    OTP_STATUS_PENDING: _ClassVar[OTPStatus]
    OTP_STATUS_USED: _ClassVar[OTPStatus]
    OTP_STATUS_EXPIRED: _ClassVar[OTPStatus]
    OTP_STATUS_INVALIDATED: _ClassVar[OTPStatus]

class IdentityProviderKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    IDENTITY_PROVIDER_KIND_UNSPECIFIED: _ClassVar[IdentityProviderKind]
    IDENTITY_PROVIDER_KIND_NATIVE: _ClassVar[IdentityProviderKind]
    IDENTITY_PROVIDER_KIND_OIDC: _ClassVar[IdentityProviderKind]
    IDENTITY_PROVIDER_KIND_SAML: _ClassVar[IdentityProviderKind]
    IDENTITY_PROVIDER_KIND_LDAP: _ClassVar[IdentityProviderKind]
    IDENTITY_PROVIDER_KIND_CUSTOM_JWT: _ClassVar[IdentityProviderKind]
    IDENTITY_PROVIDER_KIND_EXTERNAL_SESSION: _ClassVar[IdentityProviderKind]

class AuthFactorKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AUTH_FACTOR_KIND_UNSPECIFIED: _ClassVar[AuthFactorKind]
    AUTH_FACTOR_KIND_PASSWORD: _ClassVar[AuthFactorKind]
    AUTH_FACTOR_KIND_EMAIL_OTP: _ClassVar[AuthFactorKind]
    AUTH_FACTOR_KIND_SMS_OTP: _ClassVar[AuthFactorKind]
    AUTH_FACTOR_KIND_TOTP: _ClassVar[AuthFactorKind]
    AUTH_FACTOR_KIND_WEBAUTHN: _ClassVar[AuthFactorKind]
    AUTH_FACTOR_KIND_RECOVERY_CODE: _ClassVar[AuthFactorKind]

class AuthCredentialType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AUTH_CREDENTIAL_TYPE_UNSPECIFIED: _ClassVar[AuthCredentialType]
    AUTH_CREDENTIAL_TYPE_BEARER_TOKEN: _ClassVar[AuthCredentialType]
    AUTH_CREDENTIAL_TYPE_SESSION: _ClassVar[AuthCredentialType]
    AUTH_CREDENTIAL_TYPE_API_KEY: _ClassVar[AuthCredentialType]
    AUTH_CREDENTIAL_TYPE_MTLS: _ClassVar[AuthCredentialType]
    AUTH_CREDENTIAL_TYPE_OIDC_TOKEN: _ClassVar[AuthCredentialType]
    AUTH_CREDENTIAL_TYPE_SAML_ASSERTION: _ClassVar[AuthCredentialType]
    AUTH_CREDENTIAL_TYPE_PASSWORD: _ClassVar[AuthCredentialType]
    AUTH_CREDENTIAL_TYPE_CUSTOM: _ClassVar[AuthCredentialType]

class TokenType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TOKEN_TYPE_UNSPECIFIED: _ClassVar[TokenType]
    TOKEN_TYPE_JWT_ACCESS: _ClassVar[TokenType]
    TOKEN_TYPE_JWT_REFRESH: _ClassVar[TokenType]
    TOKEN_TYPE_SESSION: _ClassVar[TokenType]
    TOKEN_TYPE_API_KEY: _ClassVar[TokenType]
    TOKEN_TYPE_EXTERNAL: _ClassVar[TokenType]

class SigningKeyState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SIGNING_KEY_STATE_UNSPECIFIED: _ClassVar[SigningKeyState]
    SIGNING_KEY_STATE_NEXT: _ClassVar[SigningKeyState]
    SIGNING_KEY_STATE_ACTIVE: _ClassVar[SigningKeyState]
    SIGNING_KEY_STATE_VERIFYING: _ClassVar[SigningKeyState]
    SIGNING_KEY_STATE_RETIRED: _ClassVar[SigningKeyState]
    SIGNING_KEY_STATE_COMPROMISED: _ClassVar[SigningKeyState]

class MfaChallengePurpose(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MFA_CHALLENGE_PURPOSE_UNSPECIFIED: _ClassVar[MfaChallengePurpose]
    MFA_CHALLENGE_PURPOSE_LOGIN_STEP_UP: _ClassVar[MfaChallengePurpose]
    MFA_CHALLENGE_PURPOSE_SENSITIVE_OPERATION: _ClassVar[MfaChallengePurpose]
    MFA_CHALLENGE_PURPOSE_ENROLLMENT: _ClassVar[MfaChallengePurpose]
    MFA_CHALLENGE_PURPOSE_RECOVERY: _ClassVar[MfaChallengePurpose]
ACCOUNT_KIND_UNSPECIFIED: AccountKind
ACCOUNT_KIND_PERSON: AccountKind
ACCOUNT_KIND_SERVICE_ACCOUNT: AccountKind
ACCOUNT_KIND_WORKLOAD: AccountKind
ACCOUNT_KIND_EXTERNAL_IDENTITY: AccountKind
ACCOUNT_KIND_SYSTEM: AccountKind
ACCOUNT_KIND_ANONYMOUS: AccountKind
USER_STATUS_UNSPECIFIED: UserStatus
USER_STATUS_PENDING_VERIFICATION: UserStatus
USER_STATUS_ACTIVE: UserStatus
USER_STATUS_SUSPENDED: UserStatus
USER_STATUS_LOCKED: UserStatus
USER_STATUS_DEACTIVATED: UserStatus
SESSION_TYPE_UNSPECIFIED: SessionType
SESSION_TYPE_SERVER_SIDE: SessionType
SESSION_TYPE_JWT: SessionType
SESSION_TYPE_API_KEY: SessionType
SESSION_TYPE_MTLS: SessionType
SESSION_TYPE_EXTERNAL: SessionType
DEVICE_TYPE_UNSPECIFIED: DeviceType
DEVICE_TYPE_WEB: DeviceType
DEVICE_TYPE_API: DeviceType
DEVICE_TYPE_DESKTOP: DeviceType
DEVICE_TYPE_MOBILE: DeviceType
DEVICE_TYPE_WORKER: DeviceType
DEVICE_TYPE_CLI: DeviceType
OTP_TYPE_UNSPECIFIED: OTPType
OTP_TYPE_EMAIL_VERIFICATION: OTPType
OTP_TYPE_LOGIN_2FA: OTPType
OTP_TYPE_PASSWORD_RESET: OTPType
OTP_TYPE_SENSITIVE_OPERATION: OTPType
OTP_TYPE_PHONE_VERIFICATION: OTPType
OTP_STATUS_UNSPECIFIED: OTPStatus
OTP_STATUS_PENDING: OTPStatus
OTP_STATUS_USED: OTPStatus
OTP_STATUS_EXPIRED: OTPStatus
OTP_STATUS_INVALIDATED: OTPStatus
IDENTITY_PROVIDER_KIND_UNSPECIFIED: IdentityProviderKind
IDENTITY_PROVIDER_KIND_NATIVE: IdentityProviderKind
IDENTITY_PROVIDER_KIND_OIDC: IdentityProviderKind
IDENTITY_PROVIDER_KIND_SAML: IdentityProviderKind
IDENTITY_PROVIDER_KIND_LDAP: IdentityProviderKind
IDENTITY_PROVIDER_KIND_CUSTOM_JWT: IdentityProviderKind
IDENTITY_PROVIDER_KIND_EXTERNAL_SESSION: IdentityProviderKind
AUTH_FACTOR_KIND_UNSPECIFIED: AuthFactorKind
AUTH_FACTOR_KIND_PASSWORD: AuthFactorKind
AUTH_FACTOR_KIND_EMAIL_OTP: AuthFactorKind
AUTH_FACTOR_KIND_SMS_OTP: AuthFactorKind
AUTH_FACTOR_KIND_TOTP: AuthFactorKind
AUTH_FACTOR_KIND_WEBAUTHN: AuthFactorKind
AUTH_FACTOR_KIND_RECOVERY_CODE: AuthFactorKind
AUTH_CREDENTIAL_TYPE_UNSPECIFIED: AuthCredentialType
AUTH_CREDENTIAL_TYPE_BEARER_TOKEN: AuthCredentialType
AUTH_CREDENTIAL_TYPE_SESSION: AuthCredentialType
AUTH_CREDENTIAL_TYPE_API_KEY: AuthCredentialType
AUTH_CREDENTIAL_TYPE_MTLS: AuthCredentialType
AUTH_CREDENTIAL_TYPE_OIDC_TOKEN: AuthCredentialType
AUTH_CREDENTIAL_TYPE_SAML_ASSERTION: AuthCredentialType
AUTH_CREDENTIAL_TYPE_PASSWORD: AuthCredentialType
AUTH_CREDENTIAL_TYPE_CUSTOM: AuthCredentialType
TOKEN_TYPE_UNSPECIFIED: TokenType
TOKEN_TYPE_JWT_ACCESS: TokenType
TOKEN_TYPE_JWT_REFRESH: TokenType
TOKEN_TYPE_SESSION: TokenType
TOKEN_TYPE_API_KEY: TokenType
TOKEN_TYPE_EXTERNAL: TokenType
SIGNING_KEY_STATE_UNSPECIFIED: SigningKeyState
SIGNING_KEY_STATE_NEXT: SigningKeyState
SIGNING_KEY_STATE_ACTIVE: SigningKeyState
SIGNING_KEY_STATE_VERIFYING: SigningKeyState
SIGNING_KEY_STATE_RETIRED: SigningKeyState
SIGNING_KEY_STATE_COMPROMISED: SigningKeyState
MFA_CHALLENGE_PURPOSE_UNSPECIFIED: MfaChallengePurpose
MFA_CHALLENGE_PURPOSE_LOGIN_STEP_UP: MfaChallengePurpose
MFA_CHALLENGE_PURPOSE_SENSITIVE_OPERATION: MfaChallengePurpose
MFA_CHALLENGE_PURPOSE_ENROLLMENT: MfaChallengePurpose
MFA_CHALLENGE_PURPOSE_RECOVERY: MfaChallengePurpose
