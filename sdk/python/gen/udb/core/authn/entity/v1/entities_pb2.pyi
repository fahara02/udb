from udb.core.authn.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.authn.entity.v1 import otp_pb2 as _otp_pb2
from udb.core.authn.entity.v1 import session_pb2 as _session_pb2
from udb.core.authn.entity.v1 import user_pb2 as _user_pb2
from udb.core.authn.entity.v1 import webauthn_pb2 as _webauthn_pb2
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar
from udb.core.authn.entity.v1.enums_pb2 import AccountKind as AccountKind
from udb.core.authn.entity.v1.enums_pb2 import UserStatus as UserStatus
from udb.core.authn.entity.v1.enums_pb2 import SessionType as SessionType
from udb.core.authn.entity.v1.enums_pb2 import DeviceType as DeviceType
from udb.core.authn.entity.v1.enums_pb2 import OTPType as OTPType
from udb.core.authn.entity.v1.enums_pb2 import OTPStatus as OTPStatus
from udb.core.authn.entity.v1.enums_pb2 import IdentityProviderKind as IdentityProviderKind
from udb.core.authn.entity.v1.enums_pb2 import AuthFactorKind as AuthFactorKind
from udb.core.authn.entity.v1.enums_pb2 import AuthCredentialType as AuthCredentialType
from udb.core.authn.entity.v1.enums_pb2 import TokenType as TokenType
from udb.core.authn.entity.v1.otp_pb2 import OTP as OTP
from udb.core.authn.entity.v1.session_pb2 import Session as Session
from udb.core.authn.entity.v1.user_pb2 import User as User
from udb.core.authn.entity.v1.webauthn_pb2 import WebAuthnCredential as WebAuthnCredential
from udb.core.authn.entity.v1.webauthn_pb2 import WebAuthnChallenge as WebAuthnChallenge

DESCRIPTOR: _descriptor.FileDescriptor
ACCOUNT_KIND_UNSPECIFIED: _enums_pb2.AccountKind
ACCOUNT_KIND_PERSON: _enums_pb2.AccountKind
ACCOUNT_KIND_SERVICE_ACCOUNT: _enums_pb2.AccountKind
ACCOUNT_KIND_WORKLOAD: _enums_pb2.AccountKind
ACCOUNT_KIND_EXTERNAL_IDENTITY: _enums_pb2.AccountKind
ACCOUNT_KIND_SYSTEM: _enums_pb2.AccountKind
ACCOUNT_KIND_ANONYMOUS: _enums_pb2.AccountKind
USER_STATUS_UNSPECIFIED: _enums_pb2.UserStatus
USER_STATUS_PENDING_VERIFICATION: _enums_pb2.UserStatus
USER_STATUS_ACTIVE: _enums_pb2.UserStatus
USER_STATUS_SUSPENDED: _enums_pb2.UserStatus
USER_STATUS_LOCKED: _enums_pb2.UserStatus
USER_STATUS_DEACTIVATED: _enums_pb2.UserStatus
SESSION_TYPE_UNSPECIFIED: _enums_pb2.SessionType
SESSION_TYPE_SERVER_SIDE: _enums_pb2.SessionType
SESSION_TYPE_JWT: _enums_pb2.SessionType
SESSION_TYPE_API_KEY: _enums_pb2.SessionType
SESSION_TYPE_MTLS: _enums_pb2.SessionType
SESSION_TYPE_EXTERNAL: _enums_pb2.SessionType
DEVICE_TYPE_UNSPECIFIED: _enums_pb2.DeviceType
DEVICE_TYPE_WEB: _enums_pb2.DeviceType
DEVICE_TYPE_API: _enums_pb2.DeviceType
DEVICE_TYPE_DESKTOP: _enums_pb2.DeviceType
DEVICE_TYPE_MOBILE: _enums_pb2.DeviceType
DEVICE_TYPE_WORKER: _enums_pb2.DeviceType
DEVICE_TYPE_CLI: _enums_pb2.DeviceType
OTP_TYPE_UNSPECIFIED: _enums_pb2.OTPType
OTP_TYPE_EMAIL_VERIFICATION: _enums_pb2.OTPType
OTP_TYPE_LOGIN_2FA: _enums_pb2.OTPType
OTP_TYPE_PASSWORD_RESET: _enums_pb2.OTPType
OTP_TYPE_SENSITIVE_OPERATION: _enums_pb2.OTPType
OTP_STATUS_UNSPECIFIED: _enums_pb2.OTPStatus
OTP_STATUS_PENDING: _enums_pb2.OTPStatus
OTP_STATUS_USED: _enums_pb2.OTPStatus
OTP_STATUS_EXPIRED: _enums_pb2.OTPStatus
OTP_STATUS_INVALIDATED: _enums_pb2.OTPStatus
IDENTITY_PROVIDER_KIND_UNSPECIFIED: _enums_pb2.IdentityProviderKind
IDENTITY_PROVIDER_KIND_NATIVE: _enums_pb2.IdentityProviderKind
IDENTITY_PROVIDER_KIND_OIDC: _enums_pb2.IdentityProviderKind
IDENTITY_PROVIDER_KIND_SAML: _enums_pb2.IdentityProviderKind
IDENTITY_PROVIDER_KIND_LDAP: _enums_pb2.IdentityProviderKind
IDENTITY_PROVIDER_KIND_CUSTOM_JWT: _enums_pb2.IdentityProviderKind
IDENTITY_PROVIDER_KIND_EXTERNAL_SESSION: _enums_pb2.IdentityProviderKind
AUTH_FACTOR_KIND_UNSPECIFIED: _enums_pb2.AuthFactorKind
AUTH_FACTOR_KIND_PASSWORD: _enums_pb2.AuthFactorKind
AUTH_FACTOR_KIND_EMAIL_OTP: _enums_pb2.AuthFactorKind
AUTH_FACTOR_KIND_SMS_OTP: _enums_pb2.AuthFactorKind
AUTH_FACTOR_KIND_TOTP: _enums_pb2.AuthFactorKind
AUTH_FACTOR_KIND_WEBAUTHN: _enums_pb2.AuthFactorKind
AUTH_FACTOR_KIND_RECOVERY_CODE: _enums_pb2.AuthFactorKind
AUTH_CREDENTIAL_TYPE_UNSPECIFIED: _enums_pb2.AuthCredentialType
AUTH_CREDENTIAL_TYPE_BEARER_TOKEN: _enums_pb2.AuthCredentialType
AUTH_CREDENTIAL_TYPE_SESSION: _enums_pb2.AuthCredentialType
AUTH_CREDENTIAL_TYPE_API_KEY: _enums_pb2.AuthCredentialType
AUTH_CREDENTIAL_TYPE_MTLS: _enums_pb2.AuthCredentialType
AUTH_CREDENTIAL_TYPE_OIDC_TOKEN: _enums_pb2.AuthCredentialType
AUTH_CREDENTIAL_TYPE_SAML_ASSERTION: _enums_pb2.AuthCredentialType
AUTH_CREDENTIAL_TYPE_PASSWORD: _enums_pb2.AuthCredentialType
AUTH_CREDENTIAL_TYPE_CUSTOM: _enums_pb2.AuthCredentialType
TOKEN_TYPE_UNSPECIFIED: _enums_pb2.TokenType
TOKEN_TYPE_JWT_ACCESS: _enums_pb2.TokenType
TOKEN_TYPE_JWT_REFRESH: _enums_pb2.TokenType
TOKEN_TYPE_SESSION: _enums_pb2.TokenType
TOKEN_TYPE_API_KEY: _enums_pb2.TokenType
TOKEN_TYPE_EXTERNAL: _enums_pb2.TokenType
