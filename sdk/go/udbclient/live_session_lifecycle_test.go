package udbclient

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	authnentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	commonpb "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// This proof gets its own candidate-only CI step after every ordinary correctness
// test and measured sweep. Those fixtures keep their normal access-token TTL.
// No injected clocks, TokenStore mutation or manual background refresh is used.
func TestLiveG1SessionLifecycle(t *testing.T) {
	if os.Getenv("UDB_LIVE_G1_SESSION_PROOF") != "1" {
		t.Skip("requires the dedicated 20-second candidate broker restart")
	}
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" || os.Getenv("UDB_JWT_ACCESS_TTL_SECONDS") != "20" {
		t.Fatal("dedicated session proof requires live SDK opt-in and a 20-second broker TTL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	connectCtx, connectCancel := context.WithTimeout(ctx, 10*time.Second)
	sess, err := ConnectEnterprise(connectCtx, EnterpriseConfig{
		Target: requiredLiveEnv(t, "UDB_GRPC_TARGET"), AuthTarget: requiredLiveEnv(t, "UDB_AUTH_GRPC_TARGET"),
		Username: requiredLiveEnv(t, "UDB_LIVE_USERNAME"), Password: requiredLiveEnv(t, "UDB_LIVE_PASSWORD"),
		TenantCode: requiredLiveEnv(t, "UDB_LIVE_TENANT"), ProjectID: requiredLiveEnv(t, "UDB_LIVE_PROJECT"),
		Purpose: "go.live.g1.session", Deadline: 5 * time.Second,
	})
	connectCancel()
	if err != nil {
		t.Fatalf("fresh ConnectEnterprise failed: code=%s", status.Code(err))
	}
	defer sess.Close()
	identity := sess.Meta
	identity.Scopes = slices.Clone(identity.Scopes)
	if identity.TenantID == "" || identity.ProjectID == "" || identity.UserID == "" {
		t.Fatal("initial login did not adopt verified tenant/project/user identity")
	}
	initial, err := sess.tm.store.Load(ctx)
	if err != nil || initial.AccessToken == "" || initial.RefreshToken == "" || initial.SessionID == "" {
		t.Fatal("fresh login must persist access, refresh and session credentials")
	}
	initialClaims := liveG1TokenClaims(t, initial.AccessToken, identity)
	initialVerified := liveG1VerifyToken(t, ctx, sess, initial, identity)
	publicSession := initialVerified.GetSessionPublicId()
	if publicSession == "" {
		t.Fatal("initial verified token must expose its stable public session id")
	}
	beforeData, beforeAuth := sess.Data, sess.Auth
	recordID := "g1-session-" + uuid4()
	requestCtx := liveRequestContext(identity.TenantID, identity.ProjectID, identity.Purpose)
	requestCtx.ServiceIdentity = identity.ServiceIdentity
	writeCtx, writeCancel := context.WithTimeout(ctx, 5*time.Second)
	written, err := sess.Data.Upsert(writeCtx, &entityv1.UpsertRequest{
		Context: requestCtx, MessageType: liveMessageType,
		RecordJson:     liveRecordJSON(t, recordID, identity.TenantID, identity.ProjectID, recordID, "g1-session-live", 1),
		ConflictFields: []string{"record_id"}, ReturnRecord: true,
	})
	writeCancel()
	if err != nil || written.GetAffectedRows() != 1 {
		t.Fatalf("session fixture Upsert failed: code=%s", status.Code(err))
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := sess.Data.Delete(cleanupCtx, &entityv1.DeleteRequest{
			Context: requestCtx, MessageType: liveMessageType,
			Filter: liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": identity.TenantID, "project_id": identity.ProjectID}),
		}); err != nil {
			t.Errorf("session fixture cleanup failed: code=%s", status.Code(err))
		}
	}()
	selectReq := &entityv1.SelectRequest{
		Context: requestCtx, MessageType: liveMessageType, Limit: 1,
		Filter: liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": identity.TenantID, "project_id": identity.ProjectID}),
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	previous, previousClaims := initial, initialClaims
	rotations, calls := 0, 0
	var previousExpiryHorizon time.Time
	for {
		current, err := sess.tm.store.Load(ctx)
		if err != nil {
			t.Fatal("could not inspect the current session token")
		}
		if current.AccessToken != previous.AccessToken && liveG1TokenInstalled(sess, current) {
			claims := liveG1TokenClaims(t, current.AccessToken, identity)
			verified := liveG1VerifyToken(t, ctx, sess, current, identity)
			if current.SessionID != initial.SessionID || verified.GetSessionPublicId() != publicSession {
				t.Fatal("ordinary automatic refresh changed the connected session")
			}
			if current.RefreshToken == previous.RefreshToken || claims.IssuedAt <= previousClaims.IssuedAt {
				t.Fatal("automatic refresh did not rotate its actual credential and issuance time")
			}
			previousExpiryHorizon = time.Unix(previousClaims.ExpiresAt, 0).Add(time.Second)
			previous, previousClaims = current, claims
			rotations++
			t.Logf("verified automatic renewal %d: issued TTL=20s, identity unchanged", rotations)
		}
		liveG1ExercisePaths(t, ctx, sess, selectReq, identity)
		calls++
		if sess.RefreshErr() != nil {
			t.Fatal("ordinary automatic renewal reported a credential failure")
		}
		if !reflect.DeepEqual(sess.Meta, identity) || sess.Data != beforeData || sess.Auth != beforeAuth {
			t.Fatal("automatic renewal changed canonical metadata or facade handles")
		}
		// Continue successful calls past the prior token's real JWT expiry, not
		// merely until a third refresh response or a fake timer count appears.
		if rotations >= 3 && time.Now().After(previousExpiryHorizon) {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("three actual renewals did not complete: observed=%d", rotations)
		}
	}

	// Revoke the actual session with the same verified ordinary tenant authority.
	// This must revoke its family too; a surviving refresh is a server defect.
	revoked := liveG1RevokeCurrentSession(t, ctx, sess, identity, initial.SessionID, publicSession)
	for {
		current, err := sess.tm.store.Load(ctx)
		if err != nil {
			t.Fatal("could not inspect automatic re-login state")
		}
		if current.SessionID != revoked.SessionID && current.SessionID != "" && liveG1TokenInstalled(sess, current) {
			liveG1TokenClaims(t, current.AccessToken, identity)
			verified := liveG1VerifyToken(t, ctx, sess, current, identity)
			if verified.GetSessionPublicId() == "" || verified.GetSessionPublicId() == publicSession {
				t.Fatal("automatic re-login must create a different verified session")
			}
			if sess.RefreshErr() != nil || !reflect.DeepEqual(sess.Meta, identity) || sess.Data != beforeData || sess.Auth != beforeAuth {
				t.Fatal("automatic re-login did not restore the original stable identity")
			}
			liveG1ExercisePaths(t, ctx, sess, selectReq, identity)
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("real refused refresh did not recover through automatic re-login")
		}
	}
	t.Logf("G1 password-session proof: %d automatic renewals, %d successful path sweeps, issued TTL=20s, real revocation and re-login", rotations, calls)
}

// Pin the latest unconsumed credential across revoke and probe. Otherwise a
// concurrent normal rotation could make the probe fail on single-use reuse,
// falsely passing even if RevokeSession left the actual family active. The
// production manager's flight lock prevents another refresh from starting;
// existing flights are awaited outside it. No token/expiry state is changed.
func liveG1RevokeCurrentSession(t *testing.T, ctx context.Context, sess *EnterpriseSession, identity Metadata, initialSession, publicSession string) Token {
	t.Helper()
	probeConn, err := grpc.NewClient(requiredLiveEnv(t, "UDB_AUTH_GRPC_TARGET"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal("could not open the public refresh-refusal probe")
	}
	defer probeConn.Close()
	probeAuth := NewAuthClient(probeConn, identity)
	for {
		sess.tm.mu.Lock()
		if flight := sess.tm.inflight; flight != nil {
			sess.tm.mu.Unlock()
			select {
			case <-flight.done:
				if flight.err != nil {
					t.Fatal("ordinary credential refresh failed before the revocation probe")
				}
				continue
			case <-ctx.Done():
				t.Fatal("active credential rotation did not finish before revocation")
			}
		}
		break
	}
	defer sess.tm.mu.Unlock()
	probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer probeCancel()
	current, err := sess.tm.store.Load(probeCtx)
	if err != nil || current.SessionID != initialSession || current.RefreshToken == "" {
		t.Fatal("could not pin the original session's latest unconsumed refresh credential")
	}
	liveG1TokenClaims(t, current.AccessToken, identity)
	// A plain native channel avoids any credential resolver awaiting the same
	// manager lock. Revoke retains the genuine verified ordinary bearer; the
	// PUBLIC refresh probe carries only metadata and the refresh credential.
	publicCtx := metadata.AppendToOutgoingContext(probeAuth.Context(probeCtx), "x-request-id", uuid4())
	authorizedCtx := metadata.AppendToOutgoingContext(publicCtx, "authorization", "Bearer "+current.AccessToken)
	verified, err := probeAuth.Authn.ValidateToken(authorizedCtx, &authnv1.ValidateTokenRequest{
		Token: current.AccessToken, TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS,
	})
	if err != nil || !verified.GetValid() || verified.GetPrincipal() == nil || verified.GetSessionPublicId() != publicSession {
		t.Fatalf("pinned session must still validate immediately before revocation: code=%s", status.Code(err))
	}
	p := verified.GetPrincipal()
	liveG1AssertIdentity(t, identity, p.GetTenantId(), p.GetProjectId(), p.GetUserId(), p.GetServiceIdentity(), p.GetScopes())
	revoke, err := probeAuth.Authn.RevokeSession(authorizedCtx, &authnv1.RevokeSessionRequest{
		SessionId: current.SessionID, RevokeReason: "go_live_g1_relogin_proof", PrincipalId: identity.UserID,
		Context: &commonpb.RequestContext{
			Tenant: &commonpb.TenantContext{TenantId: identity.TenantID, ProjectId: identity.ProjectID},
			UserId: identity.UserID, PrincipalId: identity.UserID, Purpose: identity.Purpose,
		},
	})
	if err != nil || revoke.GetRevokedCount() != 1 {
		t.Fatalf("real RevokeSession must revoke exactly this session: code=%s", status.Code(err))
	}
	_, err = probeAuth.Authn.RefreshToken(publicCtx, &authnv1.RefreshTokenRequest{
		RefreshToken: current.RefreshToken, SessionId: current.SessionID,
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("revoked session's unconsumed refresh credential must be refused: code=%s", status.Code(err))
	}
	return current
}

type liveG1Claims struct {
	IssuedAt        int64    `json:"iat"`
	ExpiresAt       int64    `json:"exp"`
	UserID          string   `json:"sub"`
	TenantID        string   `json:"tenant_id"`
	ProjectID       string   `json:"project_id"`
	ServiceIdentity string   `json:"service_identity"`
	Scopes          []string `json:"scopes"`
}

// Decoding only inspects the server-issued lifetime/identity. Native ValidateToken
// below independently verifies each token's signature and persisted authority.
func liveG1TokenClaims(t *testing.T, bearer string, identity Metadata) liveG1Claims {
	t.Helper()
	parts := strings.Split(bearer, ".")
	if len(parts) != 3 {
		t.Fatal("broker did not issue a JWT access token")
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal("broker JWT payload is not decodable")
	}
	var claims liveG1Claims
	if err := json.Unmarshal(body, &claims); err != nil {
		t.Fatal("broker JWT payload is not a valid claims object")
	}
	if claims.ExpiresAt-claims.IssuedAt != 20 || claims.IssuedAt <= 0 || !time.Now().Before(time.Unix(claims.ExpiresAt, 0)) {
		t.Fatal("broker-issued access token must have a real, currently live 20-second lifetime")
	}
	liveG1AssertIdentity(t, identity, claims.TenantID, claims.ProjectID, claims.UserID, claims.ServiceIdentity, claims.Scopes)
	return claims
}

func liveG1AssertIdentity(t *testing.T, identity Metadata, tenant, project, user, service string, scopes []string) {
	t.Helper()
	for _, field := range []struct {
		name    string
		matches bool
	}{
		{"tenant", tenant == identity.TenantID}, {"project", project == identity.ProjectID},
		{"user", user == identity.UserID}, {"service identity", service == identity.ServiceIdentity},
	} {
		if !field.matches {
			t.Fatalf("broker-issued token changed %s", field.name)
		}
	}
	want, got := slices.Clone(identity.Scopes), slices.Clone(scopes)
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(slices.Compact(want), slices.Compact(got)) {
		t.Fatal("broker-issued token changed scopes")
	}
}

func liveG1TokenInstalled(sess *EnterpriseSession, tok Token) bool {
	bearer := "Bearer " + tok.AccessToken
	return tok.AccessToken != "" && sess.Bearer() == bearer && sess.Generated.options().Authorization == bearer
}

func liveG1VerifyToken(t *testing.T, ctx context.Context, sess *EnterpriseSession, tok Token, identity Metadata) *authnv1.ValidateTokenResponse {
	t.Helper()
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	verified, err := sess.Auth.Authn.ValidateToken(callCtx, &authnv1.ValidateTokenRequest{
		Token: tok.AccessToken, TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS,
	})
	if err != nil || !verified.GetValid() || verified.GetPrincipal() == nil {
		t.Fatalf("native authority rejected a current broker-issued token: code=%s", status.Code(err))
	}
	p := verified.GetPrincipal()
	liveG1AssertIdentity(t, identity, p.GetTenantId(), p.GetProjectId(), p.GetUserId(), p.GetServiceIdentity(), p.GetScopes())
	return verified
}

func liveG1ExercisePaths(t *testing.T, ctx context.Context, sess *EnterpriseSession, req *entityv1.SelectRequest, identity Metadata) {
	t.Helper()
	for _, path := range []string{"data", "raw broker", "generated"} {
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		var rows *entityv1.RecordSet
		var err error
		switch path {
		case "data":
			rows, err = sess.Data.Select(callCtx, req)
		case "raw broker":
			rows, err = sess.Data.Broker.Select(callCtx, req)
		case "generated":
			rows = &entityv1.RecordSet{}
			err = sess.Generated.InvokeUnary(callCtx, "/udb.services.v1.DataBroker/Select", req, rows)
		}
		cancel()
		if err != nil {
			t.Fatalf("ordinary %s call failed across automatic renewal: code=%s", path, status.Code(err))
		}
		if len(rows.GetRecordsJson()) != 1 || liveRecordPayload(t, rows, 0) != "g1-session-live" {
			t.Fatalf("ordinary %s call did not read the authorized fixture row", path)
		}
	}
	tok, err := sess.tm.store.Load(ctx)
	if err != nil {
		t.Fatal("could not inspect the token for a native path sweep")
	}
	liveG1VerifyToken(t, ctx, sess, tok, identity)
}
