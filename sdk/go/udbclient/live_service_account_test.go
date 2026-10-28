package udbclient

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	authnentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Live service-account conformance. Skipped unless UDB_LIVE_SDK_TESTS=1; CI's
// "SDK service accounts (live)" job runs it against the broker it just built,
// after scripts/provision-service-account-live.py has created the identities
// it reads from the environment.
//
// It proves, through the Go SDK against the served broker:
//  1. Authenticate names the account (Principal.username);
//  2. a service-account bearer carries its bound roles, never a withheld
//     admin-segment role, and its typed-grant scopes;
//  3. a service account with udb:authz:check-access-for evaluates CheckAccess
//     for a person of its tenant (evaluated_subject), and is refused for an id
//     that is not one;
//  4. ExchangeServiceTokenForTenant mints a short-lived target-tenant token
//     that authenticates and authorizes IN the target tenant, and is refused
//     for a tenant with no approved membership;
//  5. a phone-only CreateUser is verified by a PHONE_VERIFICATION OTP and the
//     account becomes ACTIVE.
func TestLiveServiceAccountDelegationConformance(t *testing.T) {
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" {
		t.Skip("requires live UDB broker")
	}
	// Needs the provisioned service-account fixtures and the OTP dev-echo
	// broker posture of the dedicated CI job, which requires this test's PASS.
	if os.Getenv("UDB_LIVE_SERVICE_ACCOUNT_TESTS") != "1" {
		t.Skip("requires the dedicated CI service-account opt-in")
	}
	target := requiredLiveEnv(t, "UDB_GRPC_TARGET")
	authTarget := liveEnv("UDB_AUTH_GRPC_TARGET", target)
	tenant := requiredLiveEnv(t, "UDB_LIVE_TENANT")
	project := requiredLiveEnv(t, "UDB_LIVE_PROJECT")
	humanUsername := requiredLiveEnv(t, "UDB_LIVE_USERNAME")
	humanPassword := requiredLiveEnv(t, "UDB_LIVE_PASSWORD")
	serviceUsername := requiredLiveEnv(t, "UDB_LIVE_SERVICE_USERNAME")
	servicePassword := requiredLiveEnv(t, "UDB_LIVE_SERVICE_PASSWORD")
	serviceUserID := requiredLiveEnv(t, "UDB_LIVE_SERVICE_USER_ID")
	serviceRole := requiredLiveEnv(t, "UDB_LIVE_SERVICE_ROLE")
	withheldRole := requiredLiveEnv(t, "UDB_LIVE_SERVICE_WITHHELD_ROLE")
	delegatedUserID := requiredLiveEnv(t, "UDB_LIVE_DELEGATED_USER_ID")
	targetTenantID := requiredLiveEnv(t, "UDB_LIVE_TARGET_TENANT_ID")
	targetProject := requiredLiveEnv(t, "UDB_LIVE_TARGET_PROJECT")
	unapprovedTenantID := requiredLiveEnv(t, "UDB_LIVE_UNAPPROVED_TENANT_ID")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := grpc.NewClient(authTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial auth: %v", err)
	}
	defer conn.Close()

	// Login and Authenticate are public: they carry the human tenant code.
	public := svcAcctClient(conn, tenant, project, "public")

	// ── 1. Principal.username ──────────────────────────────────────────────
	humanLogin, err := public.Authn.Login(public.Context(ctx), &authnv1.LoginRequest{
		Username: humanUsername, Password: humanPassword,
		TenantHint: tenant, ProjectHint: project, DeviceName: "go-sdk-service-accounts-human",
	})
	if err != nil || humanLogin.GetAccessToken() == "" {
		t.Fatalf("step 1: human login failed: code=%s %v", status.Code(err), err)
	}
	humanAuth, err := public.Authn.Authenticate(public.Context(ctx), &authnv1.AuthnRequest{
		BearerToken: humanLogin.GetAccessToken(), TenantHint: tenant, ProjectHint: project,
	})
	if err != nil {
		t.Fatalf("step 1: Authenticate(human bearer) failed: code=%s %v", status.Code(err), err)
	}
	human := humanAuth.GetPrincipal()
	if !strings.EqualFold(human.GetUsername(), humanUsername) {
		t.Fatalf("step 1: principal.username = %q, want %q", human.GetUsername(), humanUsername)
	}
	homeTenantID := human.GetTenantId()
	if homeTenantID == "" {
		t.Fatalf("step 1: the human principal has no canonical tenant_id")
	}
	humanProject := svcAcctFirstNonEmpty(human.GetProjectId(), project)
	humanClient := svcAcctClient(conn, homeTenantID, humanProject, "human")
	humanCtx := func() context.Context { return svcAcctBearer(ctx, humanClient, humanLogin.GetAccessToken()) }

	// ── 2. service bearer roles + username ─────────────────────────────────
	serviceLogin, err := public.Authn.Login(public.Context(ctx), &authnv1.LoginRequest{
		Username: serviceUsername, Password: servicePassword,
		TenantHint: tenant, ProjectHint: project, DeviceName: "go-sdk-service-accounts-service",
	})
	if err != nil || serviceLogin.GetAccessToken() == "" {
		t.Fatalf("step 2: service-account login failed: code=%s %v", status.Code(err), err)
	}
	serviceAuth, err := public.Authn.Authenticate(public.Context(ctx), &authnv1.AuthnRequest{
		BearerToken: serviceLogin.GetAccessToken(), TenantHint: tenant, ProjectHint: project,
	})
	if err != nil {
		t.Fatalf("step 2: Authenticate(service bearer) failed: code=%s %v", status.Code(err), err)
	}
	svc := serviceAuth.GetPrincipal()
	if !strings.EqualFold(svc.GetUsername(), serviceUsername) {
		t.Fatalf("step 2: principal.username = %q, want %q", svc.GetUsername(), serviceUsername)
	}
	if svc.GetUserId() != serviceUserID {
		t.Fatalf("step 2: principal.user_id = %q, want %q", svc.GetUserId(), serviceUserID)
	}
	if svc.GetAccountKind() != authnentpb.AccountKind_ACCOUNT_KIND_SERVICE_ACCOUNT {
		t.Fatalf("step 2: principal.account_kind = %s, want ACCOUNT_KIND_SERVICE_ACCOUNT", svc.GetAccountKind())
	}
	if svc.GetTenantId() != homeTenantID {
		t.Fatalf("step 2: principal.tenant_id = %q, want the home tenant %q", svc.GetTenantId(), homeTenantID)
	}
	if !svcAcctContains(svc.GetRoles(), serviceRole) {
		t.Fatalf("step 2: principal.roles = %v, missing the bound role %q", svc.GetRoles(), serviceRole)
	}
	for _, forbidden := range []string{withheldRole, "platform_admin", "organization_owner"} {
		if svcAcctContains(svc.GetRoles(), forbidden) {
			t.Fatalf("step 2: principal.roles = %v carries the withheld role %q", svc.GetRoles(), forbidden)
		}
	}
	if !svcAcctContains(svc.GetScopes(), "udb:authz:check-access-for") {
		t.Fatalf("step 2: principal.scopes = %v, missing udb:authz:check-access-for", svc.GetScopes())
	}
	serviceClient := svcAcctClient(conn, homeTenantID, svcAcctFirstNonEmpty(svc.GetProjectId(), project), "service")
	serviceCtx := func() context.Context {
		return svcAcctBearer(ctx, serviceClient, serviceLogin.GetAccessToken())
	}

	// ── 3. delegated CheckAccess ───────────────────────────────────────────
	delegated, err := serviceClient.Authz.CheckAccess(serviceCtx(), &authzv1.CheckAccessRequest{
		UserId: delegatedUserID, Object: "sdk-live-chart", Action: "read",
	})
	if err != nil {
		t.Fatalf("step 3: delegated CheckAccess for %s failed: code=%s %v", delegatedUserID, status.Code(err), err)
	}
	if delegated.GetEvaluatedSubject() != delegatedUserID {
		t.Fatalf("step 3: evaluated_subject = %q, want the delegated user %q", delegated.GetEvaluatedSubject(), delegatedUserID)
	}
	stranger := svcAcctUUIDv4(t)
	_, err = serviceClient.Authz.CheckAccess(serviceCtx(), &authzv1.CheckAccessRequest{
		UserId: stranger, Object: "sdk-live-chart", Action: "read",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("step 3: delegated CheckAccess for the unknown id %s must be PERMISSION_DENIED, got code=%s %v", stranger, status.Code(err), err)
	}

	// ── 4. tenant token exchange ───────────────────────────────────────────
	const exchangedScope = "udb:authz:check-access"
	exchanged, err := serviceClient.Authn.ExchangeServiceTokenForTenant(serviceCtx(), &authnv1.ExchangeServiceTokenForTenantRequest{
		TargetTenantId: targetTenantID, TargetProjectId: targetProject, RequestedScopes: []string{exchangedScope},
	})
	if err != nil {
		t.Fatalf("step 4: ExchangeServiceTokenForTenant(%s) failed: code=%s %v", targetTenantID, status.Code(err), err)
	}
	if exchanged.GetAccessToken() == "" {
		t.Fatalf("step 4: the exchange returned no access token")
	}
	if exchanged.GetTenantId() != targetTenantID {
		t.Fatalf("step 4: exchanged tenant_id = %q, want %q", exchanged.GetTenantId(), targetTenantID)
	}
	if exchanged.GetProjectId() != targetProject {
		t.Fatalf("step 4: exchanged project_id = %q, want %q", exchanged.GetProjectId(), targetProject)
	}
	if scopes := exchanged.GetScopes(); len(scopes) != 1 || scopes[0] != exchangedScope {
		t.Fatalf("step 4: exchanged scopes = %v, want [%s]", scopes, exchangedScope)
	}
	if exchanged.GetMembershipId() == "" {
		t.Fatalf("step 4: the exchange returned no membership_id")
	}
	now := time.Now().Unix()
	if exp := exchanged.GetExpiresAtUnix(); exp <= now || exp > now+15*60+60 {
		t.Fatalf("step 4: expires_at_unix = %d, want in (%d, %d]", exp, now, now+15*60+60)
	}
	targetPublic := svcAcctClient(conn, targetTenantID, targetProject, "exchanged-authn")
	exchangedAuth, err := targetPublic.Authn.Authenticate(targetPublic.Context(ctx), &authnv1.AuthnRequest{
		BearerToken: exchanged.GetAccessToken(), TenantHint: targetTenantID, ProjectHint: targetProject,
	})
	if err != nil {
		t.Fatalf("step 4: Authenticate(exchanged bearer) failed: code=%s %v", status.Code(err), err)
	}
	if got := exchangedAuth.GetPrincipal().GetTenantId(); got != targetTenantID {
		t.Fatalf("step 4: exchanged principal.tenant_id = %q, want %q", got, targetTenantID)
	}
	if got := exchangedAuth.GetPrincipal().GetUserId(); got != serviceUserID {
		t.Fatalf("step 4: exchanged principal.user_id = %q, want %q", got, serviceUserID)
	}
	targetClient := svcAcctClient(conn, targetTenantID, targetProject, "exchanged")
	inTarget, err := targetClient.Authz.CheckAccess(
		svcAcctBearer(ctx, targetClient, exchanged.GetAccessToken()),
		// CheckAccess requires a user_id; naming the caller itself is a self-check.
		&authzv1.CheckAccessRequest{UserId: serviceUserID, Object: "sdk-live-chart", Action: "read"},
	)
	if err != nil {
		t.Fatalf("step 4: CheckAccess in the target tenant with the exchanged bearer failed: code=%s %v", status.Code(err), err)
	}
	if inTarget.GetEvaluatedSubject() != serviceUserID {
		t.Fatalf("step 4: target-tenant evaluated_subject = %q, want the service account %q", inTarget.GetEvaluatedSubject(), serviceUserID)
	}
	_, err = serviceClient.Authn.ExchangeServiceTokenForTenant(serviceCtx(), &authnv1.ExchangeServiceTokenForTenantRequest{
		TargetTenantId: unapprovedTenantID, TargetProjectId: targetProject,
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("step 4: exchange for the unapproved tenant %s must be PERMISSION_DENIED, got code=%s %v", unapprovedTenantID, status.Code(err), err)
	}

	// ── 5. phone-only CreateUser ───────────────────────────────────────────
	phone := "+1555" + svcAcctDigits(t, 7)
	created, err := humanClient.Authn.CreateUser(humanCtx(), &authnv1.CreateUserRequest{
		Username: "sdk-live-phone-go-" + svcAcctHex(t, 6), Phone: phone, Password: "SdkLivePhone#2026",
		TenantId: homeTenantID, ProjectId: humanProject,
	})
	if err != nil {
		t.Fatalf("step 5: phone-only CreateUser failed: code=%s %v", status.Code(err), err)
	}
	user := created.GetUser()
	if user.GetUserId() == "" {
		t.Fatalf("step 5: CreateUser returned no user_id")
	}
	if user.GetEmail() != "" {
		t.Fatalf("step 5: user.email = %q, want empty", user.GetEmail())
	}
	if user.GetPhone() != phone {
		t.Fatalf("step 5: user.phone = %q, want %q", user.GetPhone(), phone)
	}
	if user.GetStatus() != authnentpb.UserStatus_USER_STATUS_PENDING_VERIFICATION {
		t.Fatalf("step 5: user.status = %s, want USER_STATUS_PENDING_VERIFICATION", user.GetStatus())
	}
	if created.GetOtpId() == "" {
		t.Fatalf("step 5: CreateUser returned no otp_id")
	}
	sent, err := humanClient.Authn.SendOTP(humanCtx(), &authnv1.SendOTPRequest{
		UserId: user.GetUserId(), OtpType: authnentpb.OTPType_OTP_TYPE_PHONE_VERIFICATION,
	})
	if err != nil {
		t.Fatalf("step 5: SendOTP(PHONE_VERIFICATION) failed: code=%s %v", status.Code(err), err)
	}
	if sent.GetDevOtpCode() == "" {
		t.Fatalf("step 5: SendOTP returned no dev_otp_code (is UDB_OTP_DEV_ECHO=1 set on the broker?)")
	}
	// The echoed code belongs to the OTP SendOTP just issued.
	otpID := svcAcctFirstNonEmpty(sent.GetOtpId(), created.GetOtpId())
	verified, err := humanClient.Authn.VerifyOTP(humanCtx(), &authnv1.VerifyOTPRequest{
		OtpId: otpID, Code: sent.GetDevOtpCode(),
	})
	if err != nil {
		t.Fatalf("step 5: VerifyOTP(%s) failed: code=%s %v", otpID, status.Code(err), err)
	}
	if !verified.GetVerified() {
		t.Fatalf("step 5: VerifyOTP verified = false")
	}
	if verified.GetOtpType() != authnentpb.OTPType_OTP_TYPE_PHONE_VERIFICATION {
		t.Fatalf("step 5: VerifyOTP otp_type = %s, want OTP_TYPE_PHONE_VERIFICATION", verified.GetOtpType())
	}
	got, err := humanClient.Authn.GetUser(humanCtx(), &authnv1.GetUserRequest{UserId: user.GetUserId()})
	if err != nil {
		t.Fatalf("step 5: GetUser(%s) failed: code=%s %v", user.GetUserId(), status.Code(err), err)
	}
	if got.GetUser().GetStatus() != authnentpb.UserStatus_USER_STATUS_ACTIVE {
		t.Fatalf("step 5: after VerifyOTP user.status = %s, want USER_STATUS_ACTIVE", got.GetUser().GetStatus())
	}
	if got.GetUser().GetPhone() != phone {
		t.Fatalf("step 5: after VerifyOTP user.phone = %q, want %q", got.GetUser().GetPhone(), phone)
	}
}

// svcAcctClient builds an AuthClient whose metadata asserts tenantID and
// projectID and carries a correlation id (authenticated native calls require
// a request-context header).
func svcAcctClient(conn grpc.ClientConnInterface, tenantID, projectID, role string) *AuthClient {
	return NewAuthClient(conn, Metadata{
		TenantID: tenantID, ProjectID: projectID,
		Purpose:              "go.live.service-accounts",
		CorrelationID:        "go-live-service-accounts-" + role,
		ServiceIdentity:      "go.sdk.live",
		ClientCatalogVersion: ProtocolVersion,
	})
}

// svcAcctBearer is client's outgoing context plus the bearer credential.
func svcAcctBearer(ctx context.Context, client *AuthClient, bearer string) context.Context {
	return metadata.AppendToOutgoingContext(client.Context(ctx), "authorization", "Bearer "+bearer)
}

func svcAcctContains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func svcAcctFirstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func svcAcctRandom(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("crypto/rand: %v", err)
	}
	return b
}

func svcAcctHex(t *testing.T, n int) string { return hex.EncodeToString(svcAcctRandom(t, n)) }

func svcAcctDigits(t *testing.T, n int) string {
	var sb strings.Builder
	for _, b := range svcAcctRandom(t, n) {
		sb.WriteByte('0' + b%10)
	}
	return sb.String()
}

// svcAcctUUIDv4 is a random RFC 4122 version-4 UUID.
func svcAcctUUIDv4(t *testing.T) string {
	b := svcAcctRandom(t, 16)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
