package udbclient

import (
	"context"
	"os"
	"testing"
	"time"

	authnentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Live refresh-token rotation conformance. Skipped unless UDB_LIVE_SDK_TESTS=1;
// CI's "SDK refresh-token rotation (live)" job runs it against the broker it
// just built, on every push to main.
//
// The broker rotates the refresh token on every AuthnService.RefreshToken call
// and each one works once. An SDK that kept the token it logged in with
// refreshed once and then failed with UNAUTHENTICATED "invalid credential" —
// a production outage no single-refresh test could catch. This drives the
// SDK's own refresh path (TokenManager.Token over a stale stored token)
// several times in a row.
func TestLiveRefreshTokenRotationSurvivesRepeatedRefreshes(t *testing.T) {
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" {
		t.Skip("requires live UDB broker")
	}
	const refreshes = 3
	target := requiredLiveEnv(t, "UDB_GRPC_TARGET")
	authTarget := liveEnv("UDB_AUTH_GRPC_TARGET", target)
	tenant := liveEnv("UDB_LIVE_TENANT", "sdk-live")
	project := liveEnv("UDB_LIVE_PROJECT", "default")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := grpc.NewClient(authTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial auth: %v", err)
	}
	defer conn.Close()
	auth := NewAuthClient(conn, Metadata{
		TenantID: tenant, ProjectID: project,
		Purpose: "go.live.refresh-rotation", CorrelationID: "go-live-refresh-rotation",
		ServiceIdentity: "go.sdk.live", ClientCatalogVersion: ProtocolVersion,
	})
	store := &MemoryTokenStore{}
	tm := auth.LoginSession(store)
	login, err := tm.LoginWithDevice(ctx, &authnv1.LoginRequest{
		Username: requiredLiveEnv(t, "UDB_LIVE_USERNAME"), Password: requiredLiveEnv(t, "UDB_LIVE_PASSWORD"),
		TenantHint: tenant, ProjectHint: project, DeviceName: "go-sdk-refresh-rotation",
	})
	if err != nil {
		t.Fatalf("login: code=%s %v", status.Code(err), err)
	}
	first := login.RefreshToken
	if login.AccessToken == "" || first == "" {
		t.Fatal("login must store an access and a refresh token")
	}
	// The login JWT carries the canonical tenant UUID, not the human code;
	// authenticated calls must assert that tenant.
	who, err := auth.AuthenticateBearer(ctx, login.AccessToken)
	if err != nil || who.GetPrincipal().GetTenantId() == "" {
		t.Fatalf("the login token must authenticate to a canonical tenant: code=%s", status.Code(err))
	}
	auth.Meta.TenantID = who.GetPrincipal().GetTenantId()
	if p := who.GetPrincipal().GetProjectId(); p != "" {
		auth.Meta.ProjectID = p
	}

	previous := first
	for i := 1; i <= refreshes; i++ {
		// Expire the stored access token so Token() performs a real refresh.
		stale, err := store.Load(ctx)
		if err != nil {
			t.Fatalf("load stored token: %v", err)
		}
		stale.ExpiresAt = time.Now().Add(-time.Second)
		if err := store.Save(ctx, stale); err != nil {
			t.Fatalf("save stale token: %v", err)
		}
		tok, err := tm.Token(ctx)
		if err != nil {
			t.Fatalf("refresh %d through TokenManager.Token failed: code=%s %v", i, status.Code(err), err)
		}
		if tok.AccessToken == "" || tok.RefreshToken == "" {
			t.Fatalf("refresh %d stored an empty access or refresh token", i)
		}
		if tok.RefreshToken == previous {
			t.Fatalf("refresh %d: the refresh token did not change — the broker stopped rotating it or the SDK kept the spent one", i)
		}

		// An authenticated call carrying the refreshed bearer.
		bearerCtx := metadata.AppendToOutgoingContext(auth.Context(ctx), "authorization", "Bearer "+tok.AccessToken)
		validated, err := auth.Authn.ValidateToken(bearerCtx, &authnv1.ValidateTokenRequest{
			Token: tok.AccessToken, TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS,
		})
		if err != nil || !validated.GetValid() {
			t.Fatalf("refresh %d: the refreshed access token does not validate: code=%s", i, status.Code(err))
		}
		previous = tok.RefreshToken
	}

	// The first refresh token was spent three rotations ago: the broker must
	// refuse it. (Replaying the immediately-previous token instead would trip
	// reuse detection and revoke every session of this shared live user.)
	_, err = auth.Authn.RefreshToken(auth.Context(ctx), &authnv1.RefreshTokenRequest{RefreshToken: first})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("a spent refresh token must be refused with UNAUTHENTICATED, got code=%s", status.Code(err))
	}
}
