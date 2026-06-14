## AuthnService

_proto: core/authn/services/v1/authn_service.proto · 50 RPCs_

All request/response message bodies live in `core/authn/services/v1/core.proto`. Enum fields resolve against `core/authn/entity/v1/enums.proto`; `context` is `udb.core.common.v1.RequestContext` (from `common/v1/types.proto`, fields: `tenant{TenantContext}`, `request_id`, `correlation_id`, `user_id`, `headers`, `trace_id`, `ip_address`, `user_agent`, `idempotency_key`, `scopes`, `roles`, ...). Most authn fields read from `RequestContext` are populated server-side from the bearer/session, so the column below lists only the *message body* fields you set.

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
|------|-----|---------|-------------|------------|-------------------|
| [ ] | CreateUser | MUTATION | CreateUserRequest | `username: "alice"`, `email: "alice@acme.test"`, `password: "Str0ng!Passw0rd"`, `tenant_id: "<seed:tenant_id>"`, `full_name: "Alice A"`, `account_kind: ACCOUNT_KIND_PERSON` | password policy: min 10, 1 upper/lower/digit/special. `account_kind` enum (AccountKind). Optional: `project_id`, `external_provider_id`, `external_subject`, `profile_attributes` map, `context`. |
| [ ] | GetUser | READ_ONLY | GetUserRequest | `user_id: "<seed:user_id>"` | Lookup-by oneof-style: provide ONE of `user_id` / `username` / `email` (not a proto oneof, but only one needed). |
| [ ] | ListUsers | READ_ONLY | ListUsersRequest | `tenant_id: "<seed:tenant_id>"` | Optional filters: `account_kind` (AccountKind), `status` (UserStatus), `page` (PageRequest). |
| [ ] | UpdateUser | MUTATION | UpdateUserRequest | `user_id: "<seed:user_id>"`, `full_name: "Alice B"`, `email: "alice2@acme.test"`, `tenant_id: "<seed:tenant_id>"` | Optional: `account_kind` (AccountKind), `project_id`, `profile_attributes`, `external_provider_id`, `external_subject`, `context`. |
| [ ] | ChangeUserStatus | DESTRUCTIVE | ChangeUserStatusRequest | `user_id: "<seed:user_id>"`, `new_status: USER_STATUS_SUSPENDED`, `reason: "admin action"` | `new_status` enum (UserStatus, non-UNSPECIFIED). Optional `context`. |
| [ ] | AdminResetPassword | DESTRUCTIVE | AdminResetPasswordRequest | `user_id: "<seed:user_id>"` | Sends email OTP; response returns `otp_id`. Optional `context`. |
| [ ] | SendOTP | MUTATION | SendOTPRequest | `user_id: "<seed:user_id>"`, `otp_type: OTP_TYPE_EMAIL_VERIFICATION` | `otp_type` enum (OTPType, non-UNSPECIFIED). Optional `correlation_id`, `context`. |
| [ ] | VerifyOTP | READ_ONLY | VerifyOTPRequest | `otp_id: "<seed:code>"` (the OTP id from SendOTP), `code: "123456"` | `otp_id` is an OTP-handle ref (depends on a prior SendOTP); `code` is the 6-digit plaintext. NOTE: a *correct* code cannot be grounded from proto alone — depends on the OTP issued at runtime. |
| [ ] | ResendOTP | MUTATION | ResendOTPRequest | `original_otp_id: "<seed:code>"`, `reason: "not_received"` | `original_otp_id` refs a prior OTP id. `reason` is free string (suggested: not_received \| expired \| delivery_failed). |
| [ ] | Authenticate | READ_ONLY | AuthnRequest | `bearer_token: "<seed:token>"`, `credential_type: AUTH_CREDENTIAL_TYPE_BEARER_TOKEN` | PUBLIC. Provide ONE proof: `bearer_token` / `session_id` / `api_key` / (`external_provider_id`+`external_token`). `credential_type` enum (AuthCredentialType). Optional `tenant_hint`, `project_hint`, `requested_scopes`, `client_id`, `audience`, `issuer`, `attributes`. |
| [ ] | Login | MUTATION | LoginRequest | `username: "alice"`, `password: "Str0ng!Passw0rd"`, `device_type: DEVICE_TYPE_API`, `device_name: "cli"` | PUBLIC. `device_type` enum (DeviceType, non-UNSPECIFIED). MFA step-2 fields (`mfa_otp_id`,`totp_code`,`recovery_code`) only on second call after `mfa_required=true`. Optional `ip_address`,`user_agent`,`device_id`,`tenant_hint`,`project_hint`,`access_surface`. |
| [ ] | RefreshToken | MUTATION | RefreshTokenRequest | `refresh_token: "<seed:refresh_token>"` | PUBLIC. Provide `refresh_token` (token-family credential) OR legacy `session_id`. |
| [ ] | Logout | MUTATION | LogoutRequest | `session_id: "<seed:session_id>"` | Optional `all_sessions: true` to revoke all, `revoke_reason`, `context`. |
| [ ] | ChangePassword | MUTATION | ChangePasswordRequest | `user_id: "<seed:user_id>"`, `current_password: "Str0ng!Passw0rd"`, `new_password: "N3w!Passw0rd9"`, `otp_id: "<seed:code>"` | `otp_id` = 2FA OTP confirming the change (runtime-issued). Optional `context`. |
| [ ] | ValidateToken | READ_ONLY | ValidateTokenRequest | `token: "<seed:token>"`, `token_type: TOKEN_TYPE_JWT_ACCESS` | `token_type` enum (TokenType, non-UNSPECIFIED). `token` = raw JWT or session_token. |
| [ ] | CreateSession | MUTATION | CreateSessionRequest | `principal: { principal_id: "<seed:user_id>", subject: "<seed:subject>", user_id: "<seed:user_id>", tenant_id: "<seed:tenant_id>" }`, `ttl_seconds: 3600` | Nested `Principal` message (expand required ids). Optional `client_fingerprint`. |
| [ ] | RefreshSession | MUTATION | RefreshSessionRequest | `session_id: "<seed:session_id>"`, `ttl_seconds: 3600` | session_id ref. |
| [ ] | GetSession | READ_ONLY | GetSessionRequest | `session_id: "<seed:session_id>"` | session_id ref. |
| [ ] | ListSessions | READ_ONLY | ListSessionsRequest | `user_id: "<seed:user_id>"` | Optional `active_only: true`, `page` (PageRequest). |
| [ ] | RevokeSession | MUTATION | RevokeSessionRequest | `session_id: "<seed:session_id>"`, `revoke_reason: "user logout"` | Or revoke all for a principal: `principal_id: "<seed:subject>"`, `all_for_principal: true`. Optional `context`. |
| [ ] | ValidateCSRF | READ_ONLY | ValidateCSRFRequest | `session_id: "<seed:session_id>"`, `csrf_token: "<seed:csrf_token>"` | Server-side sessions only. csrf_token = value from csrf cookie/header (runtime-issued at Login). |
| [ ] | EnrollMFA | MUTATION | EnrollMFARequest | `user_id: "<seed:user_id>"`, `mfa_type: AUTH_FACTOR_KIND_TOTP` | `mfa_type` enum (AuthFactorKind, non-UNSPECIFIED). Optional `context`. |
| [ ] | ConfirmMFAEnrollment | MUTATION | ConfirmMFAEnrollmentRequest | `user_id: "<seed:user_id>"`, `otp_id: "<seed:code>"`, `code: "123456"` | `otp_id` = verify_otp_id from EnrollMFA; `code` = TOTP/email code. NOTE: correct TOTP code not groundable from proto (computed from totp_secret at runtime). |
| [ ] | GenerateRecoveryCodes | MUTATION | GenerateRecoveryCodesRequest | `user_id: "<seed:user_id>"`, `count: 10` | `count` clamped server-side (default 10). Optional `context`. |
| [ ] | PutMfaPolicy | MUTATION | PutMfaPolicyRequest | `tenant_id: "<seed:tenant_id>"`, `require_mfa: true` | Optional `context`. |
| [ ] | GetMfaPolicy | READ_ONLY | GetMfaPolicyRequest | `tenant_id: "<seed:tenant_id>"` | Optional `context`. |
| [ ] | ForgotPassword | MUTATION | ForgotPasswordRequest | `identifier: "alice@acme.test"` | PUBLIC. `identifier` = username or email. Optional `context`. |
| [ ] | ResetPassword | MUTATION | ResetPasswordRequest | `otp_id: "<seed:code>"`, `code: "123456"`, `new_password: "N3w!Passw0rd9"` | PUBLIC. `otp_id`/`code` from ForgotPassword (runtime-issued PASSWORD_RESET OTP). Optional `context`. |
| [ ] | IntrospectToken | READ_ONLY | IntrospectTokenRequest | `token: "<seed:token>"` | Optional `context`. |
| [ ] | SendPhoneVerification | MUTATION | SendPhoneVerificationRequest | `user_id: "<seed:user_id>"`, `phone: "+15551234567"` | `phone` = E.164. Complete with VerifyOTP. Optional `context`. |
| [ ] | GetJwks | READ_ONLY | GetJwksRequest | `{}` (empty) | PUBLIC. Only optional `context`; no required fields. |
| [ ] | StartWebAuthnRegistration | MUTATION | StartWebAuthnRegistrationRequest | `user_id: "<seed:user_id>"`, `label: "yubikey"`, `tenant_id: "<seed:tenant_id>"` | Optional `project_id`, `context`. |
| [ ] | FinishWebAuthnRegistration | MUTATION | FinishWebAuthnRegistrationRequest | `challenge_id: "<seed:code>"`, `public_key_credential_json: "{...}"`, `label: "yubikey"` | `challenge_id` from Start...; `public_key_credential_json` = WebAuthn attestation JSON. NOTE: a valid credential JSON requires a real authenticator/browser — not groundable from proto. Optional `context`. |
| [ ] | StartWebAuthnAuthentication | MUTATION | StartWebAuthnAuthenticationRequest | `user_id: "<seed:user_id>"`, `tenant_id: "<seed:tenant_id>"` | PUBLIC. Optional `project_id`, `context`. |
| [ ] | FinishWebAuthnAuthentication | MUTATION | FinishWebAuthnAuthenticationRequest | `challenge_id: "<seed:code>"`, `public_key_credential_json: "{...}"` | PUBLIC. `challenge_id` from Start...; assertion JSON requires a real authenticator — not groundable from proto. Optional `context`. |
| [ ] | ListDevices | READ_ONLY | ListDevicesRequest | `user_id: "<seed:user_id>"` | Optional `page` (PageRequest), `context`. |
| [ ] | RevokeDevice | MUTATION | RevokeDeviceRequest | `device_id: "<seed:record_id>"`, `reason: "lost device"` | `device_id` ref (a Device id; no dedicated seed key — use record_id). Optional `context`. |
| [ ] | AdminRevokeSession | DESTRUCTIVE | AdminRevokeSessionRequest | `user_id: "<seed:user_id>"`, `session_id: "<seed:session_id>"`, `reason: "compromised"` | Optional `context`. |
| [ ] | AdminRevokeAllUserSessions | DESTRUCTIVE | AdminRevokeAllUserSessionsRequest | `user_id: "<seed:user_id>"`, `reason: "compromised"` | Optional `context`. |
| [ ] | AdminRevokeAllTenantSessions | DESTRUCTIVE | AdminRevokeAllTenantSessionsRequest | `tenant_id: "<seed:tenant_id>"`, `reason: "incident"` | Optional `context`. |
| [ ] | EmergencyRevoke | DESTRUCTIVE | EmergencyRevokeRequest | `tenant_id: "<seed:tenant_id>"`, `reason: "incident"` | Provide at least one revoke target: `signing_key_id` (<seed:key_id>) / `token_family_id` / `tenant_id` / `principal_id` (<seed:subject>). Optional `context`. |
| [ ] | IssueMfaChallenge | MUTATION | IssueMfaChallengeRequest | `user_id: "<seed:user_id>"`, `factor_kind: AUTH_FACTOR_KIND_TOTP`, `purpose: MFA_CHALLENGE_PURPOSE_SENSITIVE_OPERATION` | `factor_kind` enum (AuthFactorKind), `purpose` enum (MfaChallengePurpose), both non-UNSPECIFIED. Optional `device_fingerprint`, `ip_address`, `context`. |
| [ ] | VerifyMfaChallenge | READ_ONLY | VerifyMfaChallengeRequest | `challenge_id: "<seed:code>"`, `code: "123456"` | `challenge_id` from IssueMfaChallenge; `code` = TOTP/OTP/recovery proof (runtime-dependent). Optional `device_fingerprint`, `context`. |
| [ ] | ListMfaFactors | READ_ONLY | ListMfaFactorsRequest | `user_id: "<seed:user_id>"` | Optional `context`. |
| [ ] | DisableMfaFactor | MUTATION | DisableMfaFactorRequest | `user_id: "<seed:user_id>"`, `factor_kind: AUTH_FACTOR_KIND_TOTP` | `factor_kind` enum (AuthFactorKind, non-UNSPECIFIED). Optional `context`. |
| [ ] | RenamePasskey | MUTATION | RenamePasskeyRequest | `user_id: "<seed:user_id>"`, `credential_id: "<seed:record_id>"`, `new_label: "work key"` | `credential_id` ref (WebAuthn credential id; no dedicated seed key — use record_id). Optional `context`. |
| [ ] | RevokeRecoveryCodes | MUTATION | RevokeRecoveryCodesRequest | `user_id: "<seed:user_id>"` | Optional `context`. |
| [ ] | AdminResetMfa | DESTRUCTIVE | AdminResetMfaRequest | `user_id: "<seed:user_id>"`, `reason: "lost device"` | Optional `context`. |
| [ ] | ListWebAuthnCredentials | READ_ONLY | ListWebAuthnCredentialsRequest | `user_id: "<seed:user_id>"` | Optional `context`. |
| [ ] | DeleteWebAuthnCredential | MUTATION | DeleteWebAuthnCredentialRequest | `user_id: "<seed:user_id>"`, `credential_id: "<seed:record_id>"` | `credential_id` ref (WebAuthn credential id; no dedicated seed key — use record_id). Optional `context`. |
