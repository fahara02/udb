package udbclient

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Live delegated job tokens (AuthnService.ExchangeTokenForJob). Skipped unless
// UDB_LIVE_SDK_TESTS=1 and UDB_LIVE_SERVICE_ACCOUNT_TESTS=1; CI's "SDK service
// accounts (live)" job runs it after scripts/provision-service-account-live.py
// gave the service account's grant udb:token:delegate.
//
// It proves, through AuthClient.ExchangeTokenForJob against the served broker:
//  1. the service mints a delegated token for the human three times in a row:
//     subject = the human, actor = the service, purpose and job id echoed, a
//     dlg_ jti, a bounded lifetime, never the delegate scope;
//  2. each token authenticates as the human and authorizes CheckAccess for the
//     human on the native listener, repeatedly;
//  3. a delegated token is never exchanged again, a person cannot mint one,
//     and a token that is not an access token is refused (PERMISSION_DENIED).
func TestLiveDelegatedJobToken(t *testing.T) {
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" {
		t.Skip("requires live UDB broker")
	}
	if os.Getenv("UDB_LIVE_SERVICE_ACCOUNT_TESTS") != "1" {
		t.Skip("requires the dedicated CI service-account opt-in")
	}
	const (
		delegateScope    = "udb:token:delegate"
		checkAccessScope = "udb:authz:check-access"
		clockSkew        = 60
		maxTTL           = 24 * 60 * 60
	)
	target := requiredLiveEnv(t, "UDB_GRPC_TARGET")
	authTarget := liveEnv("UDB_AUTH_GRPC_TARGET", target)
	tenant := requiredLiveEnv(t, "UDB_LIVE_TENANT")
	project := requiredLiveEnv(t, "UDB_LIVE_PROJECT")
	humanUsername := requiredLiveEnv(t, "UDB_LIVE_USERNAME")
	humanPassword := requiredLiveEnv(t, "UDB_LIVE_PASSWORD")
	serviceUsername := requiredLiveEnv(t, "UDB_LIVE_SERVICE_USERNAME")
	servicePassword := requiredLiveEnv(t, "UDB_LIVE_SERVICE_PASSWORD")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := grpc.NewClient(authTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial auth: %v", err)
	}
	defer conn.Close()

	public := svcAcctClient(conn, tenant, project, "public")
	login := func(username, password string) string {
		res, err := public.Authn.Login(public.Context(ctx), &authnv1.LoginRequest{
			Username: username, Password: password,
			TenantHint: tenant, ProjectHint: project, DeviceName: "go-sdk-delegated-job-token",
		})
		if err != nil || res.GetAccessToken() == "" {
			t.Fatalf("login %s failed: code=%s %v", username, status.Code(err), err)
		}
		return res.GetAccessToken()
	}
	principalOf := func(bearer string) *authnv1.Principal {
		res, err := public.Authn.Authenticate(public.Context(ctx), &authnv1.AuthnRequest{
			BearerToken: bearer, TenantHint: tenant, ProjectHint: project,
		})
		if err != nil {
			t.Fatalf("Authenticate failed: code=%s %v", status.Code(err), err)
		}
		return res.GetPrincipal()
	}

	humanBearer := login(humanUsername, humanPassword)
	human := principalOf(humanBearer)
	homeTenantID := human.GetTenantId()
	humanProject := svcAcctFirstNonEmpty(human.GetProjectId(), project)
	serviceBearer := login(serviceUsername, servicePassword)
	svc := principalOf(serviceBearer)
	if !svcAcctContains(svc.GetScopes(), delegateScope) {
		t.Fatalf("the service bearer must carry %s (provisioned grant), has %v", delegateScope, svc.GetScopes())
	}
	serviceClient := svcAcctClient(conn, homeTenantID, svcAcctFirstNonEmpty(svc.GetProjectId(), project), "service")
	serviceClient.Meta.UserID = svc.GetUserId()
	serviceClient.Meta.ServiceIdentity = svc.GetServiceIdentity()
	serviceCtx := func() context.Context { return svcAcctBearer(ctx, serviceClient, serviceBearer) }

	// ── 1. mint, repeatedly ────────────────────────────────────────────────
	var minted []*DelegatedJobToken
	for i := 0; i < 3; i++ {
		jobID := "go-live-export-" + svcAcctUUIDv4(t)
		before := time.Now().Unix()
		token, err := serviceClient.ExchangeTokenForJob(serviceCtx(), ExchangeTokenForJobRequest{
			SubjectToken: humanBearer, Purpose: "export", JobID: jobID, TTLSeconds: 600,
		})
		if err != nil {
			t.Fatalf("step 1: ExchangeTokenForJob #%d failed: code=%s %v", i, status.Code(err), err)
		}
		if !strings.HasPrefix(token.JTI, "dlg_") {
			t.Fatalf("step 1: jti = %q, want a dlg_ prefix", token.JTI)
		}
		if token.Subject != human.GetUserId() || token.Actor != svc.GetServiceIdentity() {
			t.Fatalf("step 1: subject/actor = %q/%q, want %q/%q", token.Subject, token.Actor, human.GetUserId(), svc.GetServiceIdentity())
		}
		if token.Purpose != "export" || token.JobID != jobID || token.TenantID != homeTenantID {
			t.Fatalf("step 1: purpose/job/tenant = %q/%q/%q", token.Purpose, token.JobID, token.TenantID)
		}
		if token.ExpiresAtUnix <= before || token.ExpiresAtUnix > before+600+clockSkew {
			t.Fatalf("step 1: expires_at_unix = %d, want in (%d, %d]", token.ExpiresAtUnix, before, before+600+clockSkew)
		}
		if !svcAcctContains(token.Scopes, checkAccessScope) || svcAcctContains(token.Scopes, delegateScope) {
			t.Fatalf("step 1: scopes = %v, want %s and never %s", token.Scopes, checkAccessScope, delegateScope)
		}
		minted = append(minted, token)
	}
	before := time.Now().Unix()
	capped, err := serviceClient.ExchangeTokenForJob(serviceCtx(), ExchangeTokenForJobRequest{
		SubjectToken: humanBearer, Purpose: "export", JobID: "go-live-cap-" + svcAcctUUIDv4(t), TTLSeconds: 10_000_000,
	})
	if err != nil {
		t.Fatalf("step 1: capped ExchangeTokenForJob failed: code=%s %v", status.Code(err), err)
	}
	if capped.ExpiresAtUnix > before+maxTTL+clockSkew {
		t.Fatalf("step 1: a long request was not capped: expires_at_unix = %d", capped.ExpiresAtUnix)
	}

	// ── 2. use, repeatedly ─────────────────────────────────────────────────
	asUser := svcAcctClient(conn, homeTenantID, humanProject, "delegated")
	asUser.Meta.UserID = human.GetUserId()
	for _, token := range minted {
		for i := 0; i < 2; i++ {
			who := principalOf(token.AccessToken)
			if who.GetUserId() != human.GetUserId() || who.GetTenantId() != homeTenantID {
				t.Fatalf("step 2: the delegated token authenticates as %q in %q, want %q in %q", who.GetUserId(), who.GetTenantId(), human.GetUserId(), homeTenantID)
			}
		}
		decision, err := asUser.Authz.CheckAccess(svcAcctBearer(ctx, asUser, token.AccessToken), &authzv1.CheckAccessRequest{
			UserId: human.GetUserId(), Object: "sdk-live-chart", Action: "read",
		})
		if err != nil {
			t.Fatalf("step 2: CheckAccess with the delegated token failed: code=%s %v", status.Code(err), err)
		}
		if decision.GetEvaluatedSubject() != human.GetUserId() {
			t.Fatalf("step 2: evaluated_subject = %q, want the user %q", decision.GetEvaluatedSubject(), human.GetUserId())
		}
	}

	// ── 3. refusals ────────────────────────────────────────────────────────
	_, err = serviceClient.ExchangeTokenForJob(serviceCtx(), ExchangeTokenForJobRequest{
		SubjectToken: minted[0].AccessToken, Purpose: "export", JobID: "go-live-again-" + svcAcctUUIDv4(t),
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("step 3: re-exchange must be PERMISSION_DENIED, got code=%s %v", status.Code(err), err)
	}
	humanClient := svcAcctClient(conn, homeTenantID, humanProject, "human")
	humanClient.Meta.UserID = human.GetUserId()
	_, err = humanClient.ExchangeTokenForJob(svcAcctBearer(ctx, humanClient, humanBearer), ExchangeTokenForJobRequest{
		SubjectToken: humanBearer, Purpose: "export", JobID: "go-live-person-" + svcAcctUUIDv4(t),
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("step 3: a person minting must be PERMISSION_DENIED, got code=%s %v", status.Code(err), err)
	}
	_, err = serviceClient.ExchangeTokenForJob(serviceCtx(), ExchangeTokenForJobRequest{
		SubjectToken: "not.a.token", Purpose: "export", JobID: "go-live-garbage-" + svcAcctUUIDv4(t),
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("step 3: a non-token subject must be PERMISSION_DENIED, got code=%s %v", status.Code(err), err)
	}
}
