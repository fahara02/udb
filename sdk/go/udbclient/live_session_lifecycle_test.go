package udbclient

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	apikeyv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/services/v1"
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
	defer liveG1CleanupOwnedSession(t, sess)
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
			liveG1ExerciseTransactionStream(t, ctx, sess, identity, recordID)
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
			liveG1ExerciseTransactionStream(t, ctx, sess, identity, recordID)
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

// This independent served proof runs beside the password-session proof after
// the same dedicated 20s-TTL restart. It owns its service account, grant, key and
// row; three natural exchanges and a real key revocation require no fake clock.
func TestLiveG1APIKeyCredentialLifecycle(t *testing.T) {
	if os.Getenv("UDB_LIVE_G1_SESSION_PROOF") != "1" {
		t.Skip("requires dedicated 20-second candidate broker restart")
	}
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" || os.Getenv("UDB_JWT_ACCESS_TTL_SECONDS") != "20" {
		t.Fatal("API-key proof requires actual live opt-in and 20-second TTL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	operator, err := ConnectEnterprise(ctx, EnterpriseConfig{Target: requiredLiveEnv(t, "UDB_GRPC_TARGET"), AuthTarget: requiredLiveEnv(t, "UDB_AUTH_GRPC_TARGET"), Username: requiredLiveEnv(t, "UDB_LIVE_USERNAME"), Password: requiredLiveEnv(t, "UDB_LIVE_PASSWORD"), TenantCode: requiredLiveEnv(t, "UDB_LIVE_TENANT"), ProjectID: requiredLiveEnv(t, "UDB_LIVE_PROJECT"), Purpose: "go.live.g1.api-key", Deadline: 5 * time.Second, Retry: RetryConfig{MaxAttempts: 1}})
	if err != nil {
		t.Fatalf("owned operator ConnectEnterprise: code=%s", status.Code(err))
	}
	defer operator.Close()
	identity := operator.Meta
	defer liveG1CleanupOwnedSession(t, operator)
	commonContext := func(principal string) *commonpb.RequestContext {
		return &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: identity.TenantID, ProjectId: identity.ProjectID}, UserId: principal, PrincipalId: principal, Purpose: identity.Purpose}
	}
	serviceID, keyID, recordID := "", "", "g1-api-key-"+uuid4()
	grantCreated := false
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := operator.Data.Delete(cleanupCtx, &entityv1.DeleteRequest{Context: liveRequestContext(identity.TenantID, identity.ProjectID, identity.Purpose), MessageType: liveMessageType, Filter: liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": identity.TenantID, "project_id": identity.ProjectID})}); err != nil {
			t.Errorf("owned row cleanup: code=%s", status.Code(err))
		}
		if keyID != "" {
			if _, err := operator.ApiKey.Raw.RevokeApiKey(cleanupCtx, &apikeyv1.RevokeApiKeyRequest{KeyId: keyID, RevokeReason: "G1 owned fixture cleanup", Context: commonContext(serviceID)}); err != nil {
				t.Errorf("owned key cleanup: code=%s", status.Code(err))
			}
		}
		if grantCreated {
			if _, err := operator.Auth.Authn.RevokeServiceAccountGrant(cleanupCtx, &authnv1.RevokeServiceAccountGrantRequest{TenantId: identity.TenantID, UserId: serviceID, Reason: "G1 owned fixture cleanup"}); err != nil {
				t.Errorf("owned grant cleanup: code=%s", status.Code(err))
			}
		}
		if serviceID != "" {
			if _, err := operator.Auth.Authn.ChangeUserStatus(cleanupCtx, &authnv1.ChangeUserStatusRequest{UserId: serviceID, NewStatus: authnentpb.UserStatus_USER_STATUS_DEACTIVATED, Reason: "G1 owned fixture cleanup", Context: commonContext(identity.UserID)}); err != nil {
				t.Errorf("owned account cleanup: code=%s", status.Code(err))
			}
		}
	}()
	name := "go-g1-key-" + strings.ReplaceAll(uuid4(), "-", "")
	created, err := operator.Auth.Authn.CreateUser(ctx, &authnv1.CreateUserRequest{Username: name, Email: name + "@example.invalid", Password: "CorrectHorse1!", TenantId: identity.TenantID, ProjectId: identity.ProjectID, FullName: "G1 owned service credential", AccountKind: authnentpb.AccountKind_ACCOUNT_KIND_SERVICE_ACCOUNT})
	if err == nil {
		serviceID = created.GetUser().GetUserId()
	}
	if err != nil || serviceID == "" || serviceID == identity.UserID {
		t.Fatalf("owned service creation: code=%s", status.Code(err))
	}
	if _, err := operator.Auth.Authn.ChangeUserStatus(ctx, &authnv1.ChangeUserStatusRequest{UserId: serviceID, NewStatus: authnentpb.UserStatus_USER_STATUS_ACTIVE, Reason: "G1 owned fixture activation", Context: commonContext(identity.UserID)}); err != nil {
		t.Fatalf("owned service activation: code=%s", status.Code(err))
	}
	scopes := []string{"data:read", "udb:authn:validate-token"}
	if _, err := operator.Auth.Authn.CreateServiceAccountGrant(ctx, &authnv1.CreateServiceAccountGrantRequest{TenantId: identity.TenantID, ProjectId: identity.ProjectID, UserId: serviceID, ServiceIdentity: name, ApprovedScopes: scopes, Reason: "G1 owned read and token verification"}); err != nil {
		t.Fatalf("owned service grant: code=%s", status.Code(err))
	}
	grantCreated = true
	key, err := operator.ApiKey.Raw.CreateApiKey(ctx, &apikeyv1.CreateApiKeyRequest{Name: name, OwnerId: serviceID, Scopes: scopes, Context: commonContext(serviceID)})
	if err == nil {
		keyID = key.GetKey().GetKeyId()
	}
	if err != nil || keyID == "" || key.GetPlainKey() == "" {
		t.Fatalf("owned key creation: code=%s", status.Code(err))
	}
	service, err := Connect(ctx, Config{Target: requiredLiveEnv(t, "UDB_GRPC_TARGET"), AuthTarget: requiredLiveEnv(t, "UDB_AUTH_GRPC_TARGET"), TenantID: identity.TenantID, ProjectID: identity.ProjectID, Purpose: identity.Purpose, Credentials: Credentials{APIKey: key.GetPlainKey()}, Deadline: 5 * time.Second, Retry: RetryConfig{MaxAttempts: 1}})
	if err != nil {
		t.Fatalf("actual API-key Connect: code=%s", status.Code(err))
	}
	defer service.Close()
	principal := service.Principal()
	if principal == nil || principal.GetPrincipalId() != serviceID || principal.GetUserId() != "" || principal.GetSubject() != name || principal.GetTenantId() != identity.TenantID || principal.GetProjectId() != identity.ProjectID || principal.GetServiceIdentity() != name || principal.GetAccountKind() != authnentpb.AccountKind_ACCOUNT_KIND_SERVICE_ACCOUNT {
		t.Fatal("actual key must return its owned canonical service principal")
	}
	beforeMeta, beforeData, beforeAuth := service.Meta, service.Data, service.Auth
	beforeMeta.Scopes = slices.Clone(service.Meta.Scopes)
	requestCtx := liveRequestContext(identity.TenantID, identity.ProjectID, identity.Purpose)
	if result, err := operator.Data.Upsert(ctx, &entityv1.UpsertRequest{Context: requestCtx, MessageType: liveMessageType, RecordJson: liveRecordJSON(t, recordID, identity.TenantID, identity.ProjectID, recordID, "g1-api-key-live", 1), ConflictFields: []string{"record_id"}}); err != nil || result.GetAffectedRows() != 1 {
		t.Fatalf("owned API-key row: code=%s", status.Code(err))
	}
	read := &entityv1.SelectRequest{Context: requestCtx, MessageType: liveMessageType, Limit: 1, Filter: liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": identity.TenantID, "project_id": identity.ProjectID})}
	currentBearer := func() string {
		service.apiKey.mu.Lock()
		defer service.apiKey.mu.Unlock()
		return strings.TrimPrefix(service.apiKey.bearer, "Bearer ")
	}
	verify := func(token string) liveG1Claims {
		parts := strings.Split(token, ".")
		if len(parts) != 3 {
			t.Fatal("API exchange did not issue actual JWT")
		}
		raw, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			t.Fatal("API JWT claims decoding")
		}
		var claims liveG1Claims
		if json.Unmarshal(raw, &claims) != nil || claims.ExpiresAt-claims.IssuedAt != 20 || claims.IssuedAt <= 0 || !time.Now().Before(time.Unix(claims.ExpiresAt, 0)) || claims.UserID != serviceID || claims.TenantID != identity.TenantID || claims.ProjectID != identity.ProjectID || claims.ServiceIdentity != name {
			t.Fatal("API JWT must have actual20s TTL and owned identity")
		}
		claimIdentity := beforeMeta
		claimIdentity.UserID = serviceID
		liveG1AssertIdentity(t, claimIdentity, claims.TenantID, claims.ProjectID, claims.UserID, claims.ServiceIdentity, claims.Scopes)
		// Exercise the managed service's native transport with its own narrow
		// validate-token grant, rather than verifying through the operator channel.
		verified, err := service.Auth.Authn.ValidateToken(ctx, &authnv1.ValidateTokenRequest{Token: token, TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS})
		if err != nil || !verified.GetValid() || verified.GetPrincipal() == nil {
			t.Fatalf("native verification of actual API JWT: code=%s", status.Code(err))
		}
		p := verified.GetPrincipal()
		if p.GetPrincipalId() != serviceID || p.GetAccountKind() != authnentpb.AccountKind_ACCOUNT_KIND_SERVICE_ACCOUNT {
			t.Fatal("native API JWT principal changed")
		}
		// JWT verification names its durable subject as UserId; the initial
		// API-key exchange deliberately leaves the service facade UserID empty.
		liveG1AssertIdentity(t, claimIdentity, p.GetTenantId(), p.GetProjectId(), p.GetUserId(), p.GetServiceIdentity(), p.GetScopes())
		return claims
	}
	previous := currentBearer()
	previousClaims := verify(previous)
	rotations := 0
	horizon := time.Time{}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if current := currentBearer(); current != previous {
			claims := verify(current)
			if claims.IssuedAt <= previousClaims.IssuedAt {
				t.Fatal("API exchange did not advance actual JWT issuance")
			}
			horizon = time.Unix(previousClaims.ExpiresAt, 0).Add(time.Second)
			previous, previousClaims = current, claims
			rotations++
		}
		for _, path := range []string{"data", "raw", "generated"} {
			var rows *entityv1.RecordSet
			var err error
			switch path {
			case "data":
				rows, err = service.Data.Select(ctx, read)
			case "raw":
				rows, err = service.Data.Broker.Select(ctx, read)
			case "generated":
				rows = &entityv1.RecordSet{}
				err = service.Generated.InvokeUnary(ctx, "/udb.services.v1.DataBroker/Select", read, rows)
			}
			if err != nil || len(rows.GetRecordsJson()) != 1 || liveRecordPayload(t, rows, 0) != "g1-api-key-live" {
				t.Fatalf("API key %s after renewal: code=%s", path, status.Code(err))
			}
		}
		if !reflect.DeepEqual(beforeMeta, service.Meta) || beforeData != service.Data || beforeAuth != service.Auth || service.CredentialErr() != nil {
			t.Fatal("automatic API exchange changed identity/facades or failed")
		}
		if rotations >= 3 && time.Now().After(horizon) {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("three actual API exchanges incomplete: %d", rotations)
		}
	}
	if _, err := operator.ApiKey.Raw.RevokeApiKey(ctx, &apikeyv1.RevokeApiKeyRequest{KeyId: keyID, RevokeReason: "G1 actual renewal refusal", Context: commonContext(serviceID)}); err != nil {
		t.Fatalf("real key revocation: code=%s", status.Code(err))
	}
	keyID = "" // The owned key was already successfully revoked.
	// Wait for the natural renewal error; no fake expiry/store mutation. Once
	// recorded, direct transports must not emit the prior still-live bearer.
	for service.CredentialErr() == nil {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("revoked key never reached natural exchange refusal")
		}
	}
	if _, err := service.Data.Broker.Select(ctx, read); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("revoked API direct transport: code=%s", status.Code(err))
	}
	if err := service.Generated.InvokeUnary(ctx, "/udb.services.v1.DataBroker/Select", read, &entityv1.RecordSet{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("revoked API generated transport: code=%s", status.Code(err))
	}
	if _, err := service.Auth.Authn.ValidateToken(ctx, &authnv1.ValidateTokenRequest{Token: previous, TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("revoked API native transport: code=%s", status.Code(err))
	}
	t.Logf("G1 API-key proof: %d natural actual20s renewals, immutable identity, direct/generated local refusal after real revocation", rotations)
}

// Stop and join the real loop before revoking the owned login session, so
// cleanup cannot trigger a background re-login after its successful revocation.
func liveG1CleanupOwnedSession(t *testing.T, sess *EnterpriseSession) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sess.stopOnce.Do(func() { close(sess.stopRefresh) })
	select {
	case <-sess.refreshDone:
	case <-ctx.Done():
		t.Error("owned session renewal loop did not stop for cleanup")
		return
	}
	tok, err := sess.tm.Token(ctx)
	if err != nil || tok.SessionID == "" {
		t.Errorf("owned session cleanup credential: code=%s", status.Code(err))
		return
	}
	identity := sess.Meta
	if _, err := sess.Auth.Authn.RevokeSession(ctx, &authnv1.RevokeSessionRequest{SessionId: tok.SessionID, PrincipalId: identity.UserID, RevokeReason: "G1 owned lifecycle cleanup", Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: identity.TenantID, ProjectId: identity.ProjectID}, UserId: identity.UserID, PrincipalId: identity.UserID, Purpose: identity.Purpose}}); err != nil {
		t.Errorf("owned session cleanup: code=%s", status.Code(err))
	}
}

// A real native BeginTx opening is repeated across each natural password
// rotation; its COMMITTED frame proves successful stream authentication.
func liveG1ExerciseTransactionStream(t *testing.T, ctx context.Context, sess *EnterpriseSession, identity Metadata, recordID string) {
	t.Helper()
	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := sess.Data.Broker.BeginTx(streamCtx)
	if err != nil {
		t.Fatalf("actual transaction stream opening: code=%s", status.Code(err))
	}
	txID := uuid4()
	requestCtx := liveRequestContext(identity.TenantID, identity.ProjectID, identity.Purpose)
	if err := stream.Send(&entityv1.Mutation{Context: requestCtx, TxId: txID, Operation: "upsert", MessageType: liveMessageType, RecordJson: liveRecordJSON(t, recordID, identity.TenantID, identity.ProjectID, recordID, "g1-session-live", 1)}); err != nil {
		t.Fatalf("transaction stream send: code=%s", status.Code(err))
	}
	if err := stream.Send(&entityv1.Mutation{Context: requestCtx, TxId: txID, Commit: true}); err != nil {
		t.Fatalf("transaction stream commit send: code=%s", status.Code(err))
	}
	_ = stream.CloseSend()
	committed := false
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("transaction stream receive: code=%s", status.Code(err))
		}
		if frame.GetState() == entityv1.TxStatus_TX_STATE_ERROR || frame.GetState() == entityv1.TxStatus_TX_STATE_ROLLED_BACK {
			t.Fatal("transaction stream refused owned write")
		}
		committed = committed || frame.GetState() == entityv1.TxStatus_TX_STATE_COMMITTED
	}
	if !committed {
		t.Fatal("transaction stream did not return COMMITTED")
	}
}
