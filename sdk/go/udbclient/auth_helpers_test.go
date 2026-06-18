package udbclient

import (
	"context"
	"reflect"
	"testing"

	enumsv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	"google.golang.org/grpc"
)

// fakeAuthn embeds the generated AuthnServiceClient so only exercised RPCs need
// bodies; it records the ordered method list to enforce the RPC-sequence gate.
type fakeAuthn struct {
	authnv1.AuthnServiceClient
	seq         *[]string
	devOTP      string
	totpSecret  string
	loginResp   *authnv1.LoginResponse
	lastLogin   *authnv1.LoginRequest
	authnResp   *authnv1.AuthnResponse
	startRegID  string
	startAuthID string
	lastFinReg  *authnv1.FinishWebAuthnRegistrationRequest
	lastFinAuth *authnv1.FinishWebAuthnAuthenticationRequest
}

func (f *fakeAuthn) rec(m string) { *f.seq = append(*f.seq, m) }

func (f *fakeAuthn) SendOTP(_ context.Context, _ *authnv1.SendOTPRequest, _ ...grpc.CallOption) (*authnv1.SendOTPResponse, error) {
	f.rec("SendOTP")
	return &authnv1.SendOTPResponse{DevOtpCode: f.devOTP}, nil
}

func (f *fakeAuthn) ForgotPassword(_ context.Context, _ *authnv1.ForgotPasswordRequest, _ ...grpc.CallOption) (*authnv1.ForgotPasswordResponse, error) {
	f.rec("ForgotPassword")
	return &authnv1.ForgotPasswordResponse{DevOtpCode: f.devOTP}, nil
}

func (f *fakeAuthn) SendPhoneVerification(_ context.Context, _ *authnv1.SendPhoneVerificationRequest, _ ...grpc.CallOption) (*authnv1.SendPhoneVerificationResponse, error) {
	f.rec("SendPhoneVerification")
	return &authnv1.SendPhoneVerificationResponse{DevOtpCode: f.devOTP}, nil
}

func (f *fakeAuthn) EnrollMFA(_ context.Context, _ *authnv1.EnrollMFARequest, _ ...grpc.CallOption) (*authnv1.EnrollMFAResponse, error) {
	f.rec("EnrollMFA")
	return &authnv1.EnrollMFAResponse{TotpSecret: f.totpSecret}, nil
}

func (f *fakeAuthn) Login(_ context.Context, in *authnv1.LoginRequest, _ ...grpc.CallOption) (*authnv1.LoginResponse, error) {
	f.rec("Login")
	f.lastLogin = in
	return f.loginResp, nil
}

func (f *fakeAuthn) Authenticate(_ context.Context, _ *authnv1.AuthnRequest, _ ...grpc.CallOption) (*authnv1.AuthnResponse, error) {
	f.rec("AuthenticateBearer")
	return f.authnResp, nil
}

func (f *fakeAuthn) StartWebAuthnRegistration(_ context.Context, _ *authnv1.StartWebAuthnRegistrationRequest, _ ...grpc.CallOption) (*authnv1.StartWebAuthnRegistrationResponse, error) {
	f.rec("StartWebAuthnRegistration")
	return &authnv1.StartWebAuthnRegistrationResponse{ChallengeId: f.startRegID}, nil
}

func (f *fakeAuthn) FinishWebAuthnRegistration(_ context.Context, in *authnv1.FinishWebAuthnRegistrationRequest, _ ...grpc.CallOption) (*authnv1.FinishWebAuthnRegistrationResponse, error) {
	f.rec("FinishWebAuthnRegistration")
	f.lastFinReg = in
	return &authnv1.FinishWebAuthnRegistrationResponse{}, nil
}

func (f *fakeAuthn) StartWebAuthnAuthentication(_ context.Context, _ *authnv1.StartWebAuthnAuthenticationRequest, _ ...grpc.CallOption) (*authnv1.StartWebAuthnAuthenticationResponse, error) {
	f.rec("StartWebAuthnAuthentication")
	return &authnv1.StartWebAuthnAuthenticationResponse{ChallengeId: f.startAuthID}, nil
}

func (f *fakeAuthn) FinishWebAuthnAuthentication(_ context.Context, in *authnv1.FinishWebAuthnAuthenticationRequest, _ ...grpc.CallOption) (*authnv1.FinishWebAuthnAuthenticationResponse, error) {
	f.rec("FinishWebAuthnAuthentication")
	f.lastFinAuth = in
	return &authnv1.FinishWebAuthnAuthenticationResponse{}, nil
}

func TestConformanceProofOTP(t *testing.T) {
	var seq []string
	fa := &fakeAuthn{seq: &seq, devOTP: "123456"}
	c := &AuthClient{Authn: fa, Meta: Metadata{TenantID: "t-1"}}
	got, err := c.ConformanceProof(context.Background(), ConformanceOTP, ConformanceProofRequest{UserID: "u1"})
	if err != nil {
		t.Fatalf("ConformanceProof: %v", err)
	}
	if got != "123456" {
		t.Fatalf("OTP proof wrong: %q", got)
	}
}

func TestConformanceProofEmptyErrors(t *testing.T) {
	var seq []string
	fa := &fakeAuthn{seq: &seq, devOTP: ""} // broker not in conformance mode
	c := &AuthClient{Authn: fa, Meta: Metadata{}}
	for _, k := range []ConformanceKind{ConformanceOTP, ConformancePasswordReset, ConformancePhone} {
		if _, err := c.ConformanceProof(context.Background(), k, ConformanceProofRequest{}); err == nil {
			t.Fatalf("empty echo for %s should error (fail loud)", k)
		}
	}
}

func TestConformanceProofTOTP(t *testing.T) {
	var seq []string
	// "JBSWY3DPEHPK3PXP" is a standard RFC base32 test secret.
	fa := &fakeAuthn{seq: &seq, totpSecret: "JBSWY3DPEHPK3PXP"}
	c := &AuthClient{Authn: fa, Meta: Metadata{}}
	got, err := c.ConformanceProof(context.Background(), ConformanceTOTP, ConformanceProofRequest{UserID: "u1"})
	if err != nil {
		t.Fatalf("TOTP proof: %v", err)
	}
	if len(got) != 6 {
		t.Fatalf("TOTP code not 6 digits: %q", got)
	}
}

func TestPasskeysRegisterTwoRPCs(t *testing.T) {
	var seq []string
	fa := &fakeAuthn{seq: &seq, startRegID: "chal-1"}
	c := &AuthClient{Authn: fa, Meta: Metadata{TenantID: "t-1", ProjectID: "p-1"}}
	if _, err := c.Passkeys().Register(context.Background(), "u1", "yk", "{cred}"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if !reflect.DeepEqual(seq, []string{"StartWebAuthnRegistration", "FinishWebAuthnRegistration"}) {
		t.Fatalf("passkey register RPC drift: %v", seq)
	}
	if fa.lastFinReg.ChallengeId != "chal-1" {
		t.Fatalf("start challenge not threaded into finish: %q", fa.lastFinReg.ChallengeId)
	}
}

func TestPasskeysAuthenticateTwoRPCs(t *testing.T) {
	var seq []string
	fa := &fakeAuthn{seq: &seq, startAuthID: "chal-2"}
	c := &AuthClient{Authn: fa, Meta: Metadata{}}
	if _, err := c.Passkeys().Authenticate(context.Background(), "u1", "{cred}"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if !reflect.DeepEqual(seq, []string{"StartWebAuthnAuthentication", "FinishWebAuthnAuthentication"}) {
		t.Fatalf("passkey auth RPC drift: %v", seq)
	}
	if fa.lastFinAuth.ChallengeId != "chal-2" {
		t.Fatalf("challenge not threaded: %q", fa.lastFinAuth.ChallengeId)
	}
}

func TestLoginWithDeviceSendsDeviceID(t *testing.T) {
	var seq []string
	fa := &fakeAuthn{seq: &seq, loginResp: &authnv1.LoginResponse{AccessToken: "tok", SessionId: "s1", AccessTokenExpiresIn: 3600}}
	c := &AuthClient{Authn: fa, Meta: Metadata{}}
	tm := NewTokenManager(c, nil)
	tok, err := tm.LoginWithDevice(context.Background(), &authnv1.LoginRequest{Username: "u", DeviceId: "dev-stable"})
	if err != nil {
		t.Fatalf("LoginWithDevice: %v", err)
	}
	if !reflect.DeepEqual(seq, []string{"Login"}) {
		t.Fatalf("LoginWithDevice must issue only Login (no GenericDispatch): %v", seq)
	}
	if fa.lastLogin.DeviceId != "dev-stable" {
		t.Fatalf("device id not sent: %q", fa.lastLogin.DeviceId)
	}
	if tok.AccessToken != "tok" || tok.SessionID != "s1" {
		t.Fatalf("token not threaded: %+v", tok)
	}
}

var _ = enumsv1.OTPType_OTP_TYPE_SENSITIVE_OPERATION
