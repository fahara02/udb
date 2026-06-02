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

}
