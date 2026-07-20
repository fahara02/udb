<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Authn\Services\V1;

/**
 * ---------------------------------------------------------------------------
 * AuthnService — native and hybrid authentication for UDB-backed projects.
 *
 * HTTP prefix: /v1/auth
 * URL conventions (Rule 07): snake_case paths, :<verb> custom method suffix, kebab-case query params.
 *
 * Auth method routing is policy-driven. Typical deployments use server-side
 * sessions for browser clients, JWT for APIs/desktop/mobile clients, API keys
 * for service integrations, and external OIDC/SAML/JWT proofs for hybrid auth.
 * ---------------------------------------------------------------------------
 */
class AuthnServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * ── User management (admin-only) ─────────────────────────────────────────
     * @param \Udb\Core\Authn\Services\V1\CreateUserRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\CreateUserResponse>
     */
    public function CreateUser(\Udb\Core\Authn\Services\V1\CreateUserRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/CreateUser',
        $argument,
        ['\Udb\Core\Authn\Services\V1\CreateUserResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\GetUserRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\GetUserResponse>
     */
    public function GetUser(\Udb\Core\Authn\Services\V1\GetUserRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/GetUser',
        $argument,
        ['\Udb\Core\Authn\Services\V1\GetUserResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\ListUsersRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ListUsersResponse>
     */
    public function ListUsers(\Udb\Core\Authn\Services\V1\ListUsersRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ListUsers',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ListUsersResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\UpdateUserRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\UpdateUserResponse>
     */
    public function UpdateUser(\Udb\Core\Authn\Services\V1\UpdateUserRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/UpdateUser',
        $argument,
        ['\Udb\Core\Authn\Services\V1\UpdateUserResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\ChangeUserStatusRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ChangeUserStatusResponse>
     */
    public function ChangeUserStatus(\Udb\Core\Authn\Services\V1\ChangeUserStatusRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ChangeUserStatus',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ChangeUserStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Admin-triggered password reset — sends email OTP to complete flow
     * @param \Udb\Core\Authn\Services\V1\AdminResetPasswordRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\AdminResetPasswordResponse>
     */
    public function AdminResetPassword(\Udb\Core\Authn\Services\V1\AdminResetPasswordRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/AdminResetPassword',
        $argument,
        ['\Udb\Core\Authn\Services\V1\AdminResetPasswordResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── OTP ──────────────────────────────────────────────────────────────────
     * @param \Udb\Core\Authn\Services\V1\SendOTPRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\SendOTPResponse>
     */
    public function SendOTP(\Udb\Core\Authn\Services\V1\SendOTPRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/SendOTP',
        $argument,
        ['\Udb\Core\Authn\Services\V1\SendOTPResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\VerifyOTPRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\VerifyOTPResponse>
     */
    public function VerifyOTP(\Udb\Core\Authn\Services\V1\VerifyOTPRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/VerifyOTP',
        $argument,
        ['\Udb\Core\Authn\Services\V1\VerifyOTPResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\ResendOTPRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ResendOTPResponse>
     */
    public function ResendOTP(\Udb\Core\Authn\Services\V1\ResendOTPRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ResendOTP',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ResendOTPResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Authentication ───────────────────────────────────────────────────────
     * @param \Udb\Core\Authn\Services\V1\AuthnRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\AuthnResponse>
     */
    public function Authenticate(\Udb\Core\Authn\Services\V1\AuthnRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/Authenticate',
        $argument,
        ['\Udb\Core\Authn\Services\V1\AuthnResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\LoginRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\LoginResponse>
     */
    public function Login(\Udb\Core\Authn\Services\V1\LoginRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/Login',
        $argument,
        ['\Udb\Core\Authn\Services\V1\LoginResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\RefreshTokenRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\RefreshTokenResponse>
     */
    public function RefreshToken(\Udb\Core\Authn\Services\V1\RefreshTokenRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/RefreshToken',
        $argument,
        ['\Udb\Core\Authn\Services\V1\RefreshTokenResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\LogoutRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\LogoutResponse>
     */
    public function Logout(\Udb\Core\Authn\Services\V1\LogoutRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/Logout',
        $argument,
        ['\Udb\Core\Authn\Services\V1\LogoutResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\ChangePasswordRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ChangePasswordResponse>
     */
    public function ChangePassword(\Udb\Core\Authn\Services\V1\ChangePasswordRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ChangePassword',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ChangePasswordResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Token validation (called by gateway + per-service interceptors) ───────
     * @param \Udb\Core\Authn\Services\V1\ValidateTokenRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ValidateTokenResponse>
     */
    public function ValidateToken(\Udb\Core\Authn\Services\V1\ValidateTokenRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ValidateToken',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ValidateTokenResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Session management ───────────────────────────────────────────────────
     * @param \Udb\Core\Authn\Services\V1\CreateSessionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\CreateSessionResponse>
     */
    public function CreateSession(\Udb\Core\Authn\Services\V1\CreateSessionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/CreateSession',
        $argument,
        ['\Udb\Core\Authn\Services\V1\CreateSessionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\RefreshSessionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\RefreshSessionResponse>
     */
    public function RefreshSession(\Udb\Core\Authn\Services\V1\RefreshSessionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/RefreshSession',
        $argument,
        ['\Udb\Core\Authn\Services\V1\RefreshSessionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\GetSessionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\GetSessionResponse>
     */
    public function GetSession(\Udb\Core\Authn\Services\V1\GetSessionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/GetSession',
        $argument,
        ['\Udb\Core\Authn\Services\V1\GetSessionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\ListSessionsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ListSessionsResponse>
     */
    public function ListSessions(\Udb\Core\Authn\Services\V1\ListSessionsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ListSessions',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ListSessionsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\RevokeSessionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\RevokeSessionResponse>
     */
    public function RevokeSession(\Udb\Core\Authn\Services\V1\RevokeSessionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/RevokeSession',
        $argument,
        ['\Udb\Core\Authn\Services\V1\RevokeSessionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── CSRF (server-side sessions only) ────────────────────────────────────
     * @param \Udb\Core\Authn\Services\V1\ValidateCSRFRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ValidateCSRFResponse>
     */
    public function ValidateCSRF(\Udb\Core\Authn\Services\V1\ValidateCSRFRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ValidateCSRF',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ValidateCSRFResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── MFA enrollment ───────────────────────────────────────────────────────
     * Step 1: initiate enrollment — returns TOTP secret / QR URI
     * @param \Udb\Core\Authn\Services\V1\EnrollMFARequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\EnrollMFAResponse>
     */
    public function EnrollMFA(\Udb\Core\Authn\Services\V1\EnrollMFARequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/EnrollMFA',
        $argument,
        ['\Udb\Core\Authn\Services\V1\EnrollMFAResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Step 2: confirm with first TOTP code (or email OTP)
     * @param \Udb\Core\Authn\Services\V1\ConfirmMFAEnrollmentRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ConfirmMFAEnrollmentResponse>
     */
    public function ConfirmMFAEnrollment(\Udb\Core\Authn\Services\V1\ConfirmMFAEnrollmentRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ConfirmMFAEnrollment',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ConfirmMFAEnrollmentResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Generate a fresh set of single-use MFA recovery/backup codes (returned once;
     * any prior codes for the user are invalidated).
     * @param \Udb\Core\Authn\Services\V1\GenerateRecoveryCodesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\GenerateRecoveryCodesResponse>
     */
    public function GenerateRecoveryCodes(\Udb\Core\Authn\Services\V1\GenerateRecoveryCodesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/GenerateRecoveryCodes',
        $argument,
        ['\Udb\Core\Authn\Services\V1\GenerateRecoveryCodesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Set the per-tenant MFA enforcement policy.
     * @param \Udb\Core\Authn\Services\V1\PutMfaPolicyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\PutMfaPolicyResponse>
     */
    public function PutMfaPolicy(\Udb\Core\Authn\Services\V1\PutMfaPolicyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/PutMfaPolicy',
        $argument,
        ['\Udb\Core\Authn\Services\V1\PutMfaPolicyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Read the per-tenant MFA enforcement policy.
     * @param \Udb\Core\Authn\Services\V1\GetMfaPolicyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\GetMfaPolicyResponse>
     */
    public function GetMfaPolicy(\Udb\Core\Authn\Services\V1\GetMfaPolicyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/GetMfaPolicy',
        $argument,
        ['\Udb\Core\Authn\Services\V1\GetMfaPolicyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * User-initiated password reset: issues a PASSWORD_RESET OTP (delivered to the
     * account's channel). Public — no bearer required.
     * @param \Udb\Core\Authn\Services\V1\ForgotPasswordRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ForgotPasswordResponse>
     */
    public function ForgotPassword(\Udb\Core\Authn\Services\V1\ForgotPasswordRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ForgotPassword',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ForgotPasswordResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Complete a password reset with the OTP from ForgotPassword (no current
     * password required). Public — the OTP is the proof of control.
     * @param \Udb\Core\Authn\Services\V1\ResetPasswordRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ResetPasswordResponse>
     */
    public function ResetPassword(\Udb\Core\Authn\Services\V1\ResetPasswordRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ResetPassword',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ResetPasswordResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * OAuth2-style token introspection for a UDB-issued JWT.
     * @param \Udb\Core\Authn\Services\V1\IntrospectTokenRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\IntrospectTokenResponse>
     */
    public function IntrospectToken(\Udb\Core\Authn\Services\V1\IntrospectTokenRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/IntrospectToken',
        $argument,
        ['\Udb\Core\Authn\Services\V1\IntrospectTokenResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Set the user's phone number and send an SMS verification OTP. Complete with
     * VerifyOTP (the response is verified the same way as email).
     * @param \Udb\Core\Authn\Services\V1\SendPhoneVerificationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\SendPhoneVerificationResponse>
     */
    public function SendPhoneVerification(\Udb\Core\Authn\Services\V1\SendPhoneVerificationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/SendPhoneVerification',
        $argument,
        ['\Udb\Core\Authn\Services\V1\SendPhoneVerificationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * JSON Web Key Set for verifying UDB-issued JWTs. Public.
     * @param \Udb\Core\Authn\Services\V1\GetJwksRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\GetJwksResponse>
     */
    public function GetJwks(\Udb\Core\Authn\Services\V1\GetJwksRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/GetJwks',
        $argument,
        ['\Udb\Core\Authn\Services\V1\GetJwksResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── WebAuthn / passkeys ─────────────────────────────────────────────────
     * @param \Udb\Core\Authn\Services\V1\StartWebAuthnRegistrationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\StartWebAuthnRegistrationResponse>
     */
    public function StartWebAuthnRegistration(\Udb\Core\Authn\Services\V1\StartWebAuthnRegistrationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/StartWebAuthnRegistration',
        $argument,
        ['\Udb\Core\Authn\Services\V1\StartWebAuthnRegistrationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\FinishWebAuthnRegistrationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\FinishWebAuthnRegistrationResponse>
     */
    public function FinishWebAuthnRegistration(\Udb\Core\Authn\Services\V1\FinishWebAuthnRegistrationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/FinishWebAuthnRegistration',
        $argument,
        ['\Udb\Core\Authn\Services\V1\FinishWebAuthnRegistrationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\StartWebAuthnAuthenticationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\StartWebAuthnAuthenticationResponse>
     */
    public function StartWebAuthnAuthentication(\Udb\Core\Authn\Services\V1\StartWebAuthnAuthenticationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/StartWebAuthnAuthentication',
        $argument,
        ['\Udb\Core\Authn\Services\V1\StartWebAuthnAuthenticationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\FinishWebAuthnAuthenticationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\FinishWebAuthnAuthenticationResponse>
     */
    public function FinishWebAuthnAuthentication(\Udb\Core\Authn\Services\V1\FinishWebAuthnAuthenticationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/FinishWebAuthnAuthentication',
        $argument,
        ['\Udb\Core\Authn\Services\V1\FinishWebAuthnAuthenticationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Device + session revocation lifecycle (Phase 3 / I2.4) ───────────────
     * @param \Udb\Core\Authn\Services\V1\ListDevicesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ListDevicesResponse>
     */
    public function ListDevices(\Udb\Core\Authn\Services\V1\ListDevicesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ListDevices',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ListDevicesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\RevokeDeviceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\RevokeDeviceResponse>
     */
    public function RevokeDevice(\Udb\Core\Authn\Services\V1\RevokeDeviceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/RevokeDevice',
        $argument,
        ['\Udb\Core\Authn\Services\V1\RevokeDeviceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\AdminRevokeSessionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\AdminRevokeSessionResponse>
     */
    public function AdminRevokeSession(\Udb\Core\Authn\Services\V1\AdminRevokeSessionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/AdminRevokeSession',
        $argument,
        ['\Udb\Core\Authn\Services\V1\AdminRevokeSessionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\AdminRevokeAllUserSessionsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\AdminRevokeAllUserSessionsResponse>
     */
    public function AdminRevokeAllUserSessions(\Udb\Core\Authn\Services\V1\AdminRevokeAllUserSessionsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/AdminRevokeAllUserSessions',
        $argument,
        ['\Udb\Core\Authn\Services\V1\AdminRevokeAllUserSessionsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\AdminRevokeAllTenantSessionsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\AdminRevokeAllTenantSessionsResponse>
     */
    public function AdminRevokeAllTenantSessions(\Udb\Core\Authn\Services\V1\AdminRevokeAllTenantSessionsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/AdminRevokeAllTenantSessions',
        $argument,
        ['\Udb\Core\Authn\Services\V1\AdminRevokeAllTenantSessionsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\EmergencyRevokeRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\EmergencyRevokeResponse>
     */
    public function EmergencyRevoke(\Udb\Core\Authn\Services\V1\EmergencyRevokeRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/EmergencyRevoke',
        $argument,
        ['\Udb\Core\Authn\Services\V1\EmergencyRevokeResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── MFA challenge + factor lifecycle (Phase 3 / I2.6) ────────────────────
     * @param \Udb\Core\Authn\Services\V1\IssueMfaChallengeRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\IssueMfaChallengeResponse>
     */
    public function IssueMfaChallenge(\Udb\Core\Authn\Services\V1\IssueMfaChallengeRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/IssueMfaChallenge',
        $argument,
        ['\Udb\Core\Authn\Services\V1\IssueMfaChallengeResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\VerifyMfaChallengeRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\VerifyMfaChallengeResponse>
     */
    public function VerifyMfaChallenge(\Udb\Core\Authn\Services\V1\VerifyMfaChallengeRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/VerifyMfaChallenge',
        $argument,
        ['\Udb\Core\Authn\Services\V1\VerifyMfaChallengeResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\ListMfaFactorsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ListMfaFactorsResponse>
     */
    public function ListMfaFactors(\Udb\Core\Authn\Services\V1\ListMfaFactorsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ListMfaFactors',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ListMfaFactorsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\DisableMfaFactorRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\DisableMfaFactorResponse>
     */
    public function DisableMfaFactor(\Udb\Core\Authn\Services\V1\DisableMfaFactorRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/DisableMfaFactor',
        $argument,
        ['\Udb\Core\Authn\Services\V1\DisableMfaFactorResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\RenamePasskeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\RenamePasskeyResponse>
     */
    public function RenamePasskey(\Udb\Core\Authn\Services\V1\RenamePasskeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/RenamePasskey',
        $argument,
        ['\Udb\Core\Authn\Services\V1\RenamePasskeyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\RevokeRecoveryCodesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\RevokeRecoveryCodesResponse>
     */
    public function RevokeRecoveryCodes(\Udb\Core\Authn\Services\V1\RevokeRecoveryCodesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/RevokeRecoveryCodes',
        $argument,
        ['\Udb\Core\Authn\Services\V1\RevokeRecoveryCodesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\AdminResetMfaRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\AdminResetMfaResponse>
     */
    public function AdminResetMfa(\Udb\Core\Authn\Services\V1\AdminResetMfaRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/AdminResetMfa',
        $argument,
        ['\Udb\Core\Authn\Services\V1\AdminResetMfaResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── WebAuthn enterprise credential lifecycle (Phase 3 / I2.7) ────────────
     * @param \Udb\Core\Authn\Services\V1\ListWebAuthnCredentialsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ListWebAuthnCredentialsResponse>
     */
    public function ListWebAuthnCredentials(\Udb\Core\Authn\Services\V1\ListWebAuthnCredentialsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ListWebAuthnCredentials',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ListWebAuthnCredentialsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Authn\Services\V1\DeleteWebAuthnCredentialRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\DeleteWebAuthnCredentialResponse>
     */
    public function DeleteWebAuthnCredential(\Udb\Core\Authn\Services\V1\DeleteWebAuthnCredentialRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/DeleteWebAuthnCredential',
        $argument,
        ['\Udb\Core\Authn\Services\V1\DeleteWebAuthnCredentialResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Typed service-account grants + mTLS certificate bindings (UDB-AUTH-003/007) ──
     * Create the single typed grant for a service account: immutable service
     * identity, tenant/project binding, and operator-approved scopes.
     * Admin/owner/wildcard scopes are rejected at write time (fail closed).
     * @param \Udb\Core\Authn\Services\V1\CreateServiceAccountGrantRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\CreateServiceAccountGrantResponse>
     */
    public function CreateServiceAccountGrant(\Udb\Core\Authn\Services\V1\CreateServiceAccountGrantRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/CreateServiceAccountGrant',
        $argument,
        ['\Udb\Core\Authn\Services\V1\CreateServiceAccountGrantResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Read the current typed grant for a service account; NOT_FOUND when the
     * account has no grant (the account then cannot authenticate — fail closed).
     * @param \Udb\Core\Authn\Services\V1\GetServiceAccountGrantRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\GetServiceAccountGrantResponse>
     */
    public function GetServiceAccountGrant(\Udb\Core\Authn\Services\V1\GetServiceAccountGrantRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/GetServiceAccountGrant',
        $argument,
        ['\Udb\Core\Authn\Services\V1\GetServiceAccountGrantResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Page through the tenant's typed service-account grants (tenant-scoped;
     * cross-tenant reads are rejected).
     * @param \Udb\Core\Authn\Services\V1\ListServiceAccountGrantsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ListServiceAccountGrantsResponse>
     */
    public function ListServiceAccountGrants(\Udb\Core\Authn\Services\V1\ListServiceAccountGrantsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ListServiceAccountGrants',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ListServiceAccountGrantsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Replace a grant's approved scopes/project atomically, bumping `revision` so
     * dependent credentials and bindings detect staleness. A stale
     * expected_revision fails with FAILED_PRECONDITION (fail closed).
     * @param \Udb\Core\Authn\Services\V1\ReplaceServiceAccountGrantRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ReplaceServiceAccountGrantResponse>
     */
    public function ReplaceServiceAccountGrant(\Udb\Core\Authn\Services\V1\ReplaceServiceAccountGrantRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ReplaceServiceAccountGrant',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ReplaceServiceAccountGrantResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Rotate the immutable service identity through an explicit audited CAS.
     * The revision bump invalidates all API keys and certificate bindings
     * reviewed against the prior identity; already-issued service JWTs fail the
     * current-grant identity check immediately.
     * @param \Udb\Core\Authn\Services\V1\RotateServiceAccountIdentityRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\RotateServiceAccountIdentityResponse>
     */
    public function RotateServiceAccountIdentity(\Udb\Core\Authn\Services\V1\RotateServiceAccountIdentityRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/RotateServiceAccountIdentity',
        $argument,
        ['\Udb\Core\Authn\Services\V1\RotateServiceAccountIdentityResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Revoke a service account's grant. The account (and every credential or
     * certificate binding that resolves through the grant) stops authenticating
     * immediately — fail closed, audited.
     * @param \Udb\Core\Authn\Services\V1\RevokeServiceAccountGrantRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\RevokeServiceAccountGrantResponse>
     */
    public function RevokeServiceAccountGrant(\Udb\Core\Authn\Services\V1\RevokeServiceAccountGrantRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/RevokeServiceAccountGrant',
        $argument,
        ['\Udb\Core\Authn\Services\V1\RevokeServiceAccountGrantResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Bind an mTLS certificate selector to a service account. The principal is
     * always derived from the account's CURRENT grant at request time (optionally
     * attenuated by scope_subset); an unknown or misbound certificate fails closed.
     * @param \Udb\Core\Authn\Services\V1\CreateCertificateBindingRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\CreateCertificateBindingResponse>
     */
    public function CreateCertificateBinding(\Udb\Core\Authn\Services\V1\CreateCertificateBindingRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/CreateCertificateBinding',
        $argument,
        ['\Udb\Core\Authn\Services\V1\CreateCertificateBindingResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Page through the tenant's mTLS certificate bindings (tenant-scoped;
     * cross-tenant reads are rejected).
     * @param \Udb\Core\Authn\Services\V1\ListCertificateBindingsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\ListCertificateBindingsResponse>
     */
    public function ListCertificateBindings(\Udb\Core\Authn\Services\V1\ListCertificateBindingsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/ListCertificateBindings',
        $argument,
        ['\Udb\Core\Authn\Services\V1\ListCertificateBindingsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Revoke a certificate binding. Certificates matching the selector stop
     * authenticating immediately — fail closed, audited.
     * @param \Udb\Core\Authn\Services\V1\RevokeCertificateBindingRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Authn\Services\V1\RevokeCertificateBindingResponse>
     */
    public function RevokeCertificateBinding(\Udb\Core\Authn\Services\V1\RevokeCertificateBindingRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.authn.services.v1.AuthnService/RevokeCertificateBinding',
        $argument,
        ['\Udb\Core\Authn\Services\V1\RevokeCertificateBindingResponse', 'decode'],
        $metadata, $options);
    }

}
