package udbclient

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"database/sql"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	enumsv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
)

// ── Stage 2: native database fast-path access (item 138) ─────────────────────
//
// GetNativeAccess runs the same UDB authorization decision and, when allowed,
// returns a short-lived grant: a restricted role, a scoped DSN, and the exact
// app.current_* session variables the caller must SET LOCAL per transaction so
// the broker-generated RLS policies still apply on the direct connection.

// GetNativeAccess forwards a fully-formed NativeAccessRequest and returns the
// response (decision + optional grant). The grant is present only when the
// decision allowed and the server has native access configured.
func (c *AuthClient) GetNativeAccess(ctx context.Context, req *authzv1.NativeAccessRequest) (*authzv1.NativeAccessResponse, error) {
	return c.Authz.GetNativeAccess(c.Context(ctx), req)
}

// NativeAccess is a convenience over GetNativeAccess: it builds the request from
// the caller Metadata plus the supplied resource/action/purpose and returns the
// grant. It returns (nil, nil) when access is allowed but no native grant was
// minted (native access not configured server-side), and an error when the
// decision denied access.
func (c *AuthClient) NativeAccess(ctx context.Context, resource *authzv1.ResourceRef, action, purpose string) (*authzv1.NativeAccessGrant, error) {
	if purpose == "" {
		purpose = c.Meta.Purpose
	}
	resp, err := c.GetNativeAccess(ctx, &authzv1.NativeAccessRequest{
		Principal: &authzv1.Principal{
			UserId:          c.Meta.UserID,
			ServiceIdentity: c.Meta.ServiceIdentity,
			TenantId:        c.Meta.TenantID,
			ProjectId:       c.Meta.ProjectID,
			Scopes:          c.Meta.Scopes,
		},
		TenantId:        c.Meta.TenantID,
		ProjectId:       c.Meta.ProjectID,
		Resource:        resource,
		Action:          action,
		Purpose:         purpose,
		RequestedScopes: c.Meta.Scopes,
	})
	if err != nil {
		return nil, err
	}
	if d := resp.GetDecision(); d != nil && !d.GetAllowed() {
		return nil, fmt.Errorf("udb: native access denied: %s", d.GetDenyReason())
	}
	return resp.GetGrant(), nil
}

// WithNativeTx opens a transaction on the caller-provided *sql.DB (which should
// be connected using grant.Dsn), applies the grant's app.current_* session
// variables with SET LOCAL so RLS sees the same request context the broker
// enforced, runs fn, and commits — or rolls back on error. Uses only the Go
// standard library so the SDK pulls in no database driver of its own; the
// caller chooses the driver (pgx stdlib, lib/pq, …) when opening db.
func WithNativeTx(ctx context.Context, db *sql.DB, grant *authzv1.NativeAccessGrant, fn func(tx *sql.Tx) error) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if grant != nil {
		for key, value := range grant.GetSessionVariables() {
			// set_config(name, value, true) == SET LOCAL but parameterizable,
			// avoiding identifier-injection on the variable name/value.
			if _, e := tx.ExecContext(ctx, "SELECT set_config($1, $2, true)", key, value); e != nil {
				err = fmt.Errorf("udb: apply session variable %q: %w", key, e)
				return err
			}
		}
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// ── Conformance proof (chapter 13.2.1) ───────────────────────────────────────
//
// ConformanceProof surfaces the broker's GATED proof material a headless
// conformance harness needs. It does NOT bypass verification — it only reads the
// dev_otp_code the broker echoes under its non-production echo gate (empty in
// production), or computes the current TOTP code from the totp_secret EnrollMFA
// returns (no broker echo for TOTP). It returns an error when the proof field is
// empty so callers fail loud, not silent.

// ConformanceKind selects which proof material ConformanceProof returns.
type ConformanceKind string

const (
	ConformanceOTP           ConformanceKind = "otp"
	ConformancePasswordReset ConformanceKind = "password_reset"
	ConformancePhone         ConformanceKind = "phone"
	ConformanceTOTP          ConformanceKind = "totp"
)

// ConformanceProofRequest carries the per-kind inputs ConformanceProof needs to
// drive the issuing RPC.
type ConformanceProofRequest struct {
	UserID     string // SendOTP / EnrollMFA target
	Identifier string // ForgotPassword identifier (email/username)
	Phone      string // SendPhoneVerification phone number
}

// ConformanceProof drives the issuing RPC for kind and extracts the gated proof.
// OTP -> SendOTP.dev_otp_code; PASSWORD_RESET -> ForgotPassword.dev_otp_code;
// PHONE -> SendPhoneVerification.dev_otp_code; TOTP -> the current 6-digit code
// computed from EnrollMFA.totp_secret. An empty echo (broker not in conformance
// mode) returns an error.
func (c *AuthClient) ConformanceProof(ctx context.Context, kind ConformanceKind, req ConformanceProofRequest) (string, error) {
	switch kind {
	case ConformanceOTP:
		resp, err := c.Authn.SendOTP(c.Context(ctx), &authnv1.SendOTPRequest{
			UserId:  req.UserID,
			OtpType: enumsv1.OTPType_OTP_TYPE_SENSITIVE_OPERATION,
		})
		if err != nil {
			return "", err
		}
		return requireProof("SendOTP", resp.GetDevOtpCode())
	case ConformancePasswordReset:
		resp, err := c.Authn.ForgotPassword(c.Context(ctx), &authnv1.ForgotPasswordRequest{
			Identifier: req.Identifier,
		})
		if err != nil {
			return "", err
		}
		return requireProof("ForgotPassword", resp.GetDevOtpCode())
	case ConformancePhone:
		resp, err := c.Authn.SendPhoneVerification(c.Context(ctx), &authnv1.SendPhoneVerificationRequest{
			UserId: req.UserID,
			Phone:  req.Phone,
		})
		if err != nil {
			return "", err
		}
		return requireProof("SendPhoneVerification", resp.GetDevOtpCode())
	case ConformanceTOTP:
		resp, err := c.Authn.EnrollMFA(c.Context(ctx), &authnv1.EnrollMFARequest{
			UserId:  req.UserID,
			MfaType: enumsv1.AuthFactorKind_AUTH_FACTOR_KIND_TOTP,
		})
		if err != nil {
			return "", err
		}
		secret := resp.GetTotpSecret()
		if secret == "" {
			return "", fmt.Errorf("udb: EnrollMFA returned no totp_secret (conformance mode off?)")
		}
		return totpNow(secret)
	default:
		return "", fmt.Errorf("udb: unknown conformance kind %q", kind)
	}
}

func requireProof(rpc, code string) (string, error) {
	if code == "" {
		return "", fmt.Errorf("udb: %s returned an empty dev_otp_code (broker not in conformance/dev-echo mode)", rpc)
	}
	return code, nil
}

// totpNow computes the current RFC-6238 6-digit TOTP code (SHA-1, 30s step) from
// a base32 secret. Uses only the standard library so the SDK pulls in no TOTP
// dependency.
func totpNow(base32Secret string) (string, error) {
	key, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(strings.TrimSpace(base32Secret)))
	if err != nil {
		return "", fmt.Errorf("udb: decode totp secret: %w", err)
	}
	counter := uint64(time.Now().Unix()) / 30
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], counter)
	mac := hmac.New(sha1.New, key)
	mac.Write(buf[:])
	sum := mac.Sum(nil)
	offset := sum[len(sum)-1] & 0x0f
	code := (uint32(sum[offset]&0x7f)<<24 |
		uint32(sum[offset+1])<<16 |
		uint32(sum[offset+2])<<8 |
		uint32(sum[offset+3])) % 1_000_000
	return fmt.Sprintf("%06d", code), nil
}

// ── Passkeys (chapter 13.2.2) ────────────────────────────────────────────────
//
// PasskeyHelper sequences the WebAuthn ceremony RPCs honestly: Start mints a
// single-use challenge that is threaded into Finish — exactly two RPCs per flow,
// no hidden Get. A headless harness can drive the dev soft-authenticator by
// supplying the TEST_CREDENTIAL_SENTINEL credential JSON.

// PasskeyHelper exposes Register/Authenticate passkey flows.
type PasskeyHelper struct {
	client *AuthClient
}

// Passkeys returns the passkey ceremony helper.
func (c *AuthClient) Passkeys() *PasskeyHelper { return &PasskeyHelper{client: c} }

// Register runs StartWebAuthnRegistration -> FinishWebAuthnRegistration. The
// caller supplies the credential JSON the authenticator produced (or the dev
// soft-authenticator sentinel). The Start challenge_id is threaded into Finish.
func (p *PasskeyHelper) Register(ctx context.Context, userID, label, credentialJSON string) (*authnv1.FinishWebAuthnRegistrationResponse, error) {
	start, err := p.client.Authn.StartWebAuthnRegistration(p.client.Context(ctx), &authnv1.StartWebAuthnRegistrationRequest{
		UserId:    userID,
		Label:     label,
		TenantId:  p.client.Meta.TenantID,
		ProjectId: p.client.Meta.ProjectID,
	})
	if err != nil {
		return nil, err
	}
	return p.client.Authn.FinishWebAuthnRegistration(p.client.Context(ctx), &authnv1.FinishWebAuthnRegistrationRequest{
		ChallengeId:             start.GetChallengeId(),
		PublicKeyCredentialJson: credentialJSON,
		Label:                   label,
	})
}

// Authenticate runs StartWebAuthnAuthentication -> FinishWebAuthnAuthentication,
// threading the single-use Start challenge into Finish.
func (p *PasskeyHelper) Authenticate(ctx context.Context, userID, credentialJSON string) (*authnv1.FinishWebAuthnAuthenticationResponse, error) {
	start, err := p.client.Authn.StartWebAuthnAuthentication(p.client.Context(ctx), &authnv1.StartWebAuthnAuthenticationRequest{
		UserId:    userID,
		TenantId:  p.client.Meta.TenantID,
		ProjectId: p.client.Meta.ProjectID,
	})
	if err != nil {
		return nil, err
	}
	return p.client.Authn.FinishWebAuthnAuthentication(p.client.Context(ctx), &authnv1.FinishWebAuthnAuthenticationRequest{
		ChallengeId:             start.GetChallengeId(),
		PublicKeyCredentialJson: credentialJSON,
	})
}
