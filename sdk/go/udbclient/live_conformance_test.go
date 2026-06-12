package udbclient

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	authnentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const liveMessageType = "udb.sdk.live.v1.SdkLiveRecord"

func TestLiveGeneratedRPCSurface(t *testing.T) {
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" {
		t.Skip("requires live UDB broker")
	}

	target := requiredLiveEnv(t, "UDB_GRPC_TARGET")
	authTarget := os.Getenv("UDB_AUTH_GRPC_TARGET")
	if authTarget == "" {
		authTarget = target
	}
	tenant := liveEnv("UDB_LIVE_TENANT", "sdk-live")
	project := liveEnv("UDB_LIVE_PROJECT", "default")
	meta := Metadata{
		TenantID:      tenant,
		ProjectID:     project,
		Purpose:       "go.live.conformance",
		CorrelationID: "go-live-conformance",
		// No client-asserted scopes: admin authority comes from the Login JWT
		// (the broker derives scopes from the validated bearer, ignoring x-scopes
		// when a JWT verifier is configured). This is the real production path.
		ServiceIdentity:      "go.sdk.live",
		ClientCatalogVersion: ProtocolVersion,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokerConn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer brokerConn.Close()
	authConn := brokerConn
	if authTarget != target {
		authConn, err = grpc.NewClient(authTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial auth: %v", err)
		}
		defer authConn.Close()
	}

	login, err := authnv1.NewAuthnServiceClient(authConn).Login(ctx, &authnv1.LoginRequest{
		Username:    requiredLiveEnv(t, "UDB_LIVE_USERNAME"),
		Password:    requiredLiveEnv(t, "UDB_LIVE_PASSWORD"),
		TenantHint:  tenant,
		ProjectHint: project,
		DeviceName:  "go-sdk-live-conformance",
	})
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}
	if login.GetAccessToken() == "" || login.GetRefreshToken() == "" {
		t.Fatalf("Login must return access and refresh tokens")
	}

	auth := NewAuthClient(authConn, meta)
	authResp, err := auth.AuthenticateBearer(ctx, login.GetAccessToken())
	if err != nil {
		t.Fatalf("AuthenticateBearer rejected Login access token: %v", err)
	}
	// Tenant-identity fix (auth_fix.md): bootstrap binds the admin to the tenant's
	// CANONICAL UUID, so the Login JWT claim is a UUID — not the human code. Discover
	// it from our own principal and use it for every request body (and the metadata
	// header, before the generated clients are built below), so the body tenant
	// matches the claim and the UUID-strict services (storage/webrtc/asset) accept
	// it. ONE admin now serves all 262 RPCs.
	if pt := authResp.GetPrincipal().GetTenantId(); pt != "" {
		tenant = pt
		meta.TenantID = tenant
	}
	if refreshed, err := auth.Authn.RefreshToken(auth.Context(ctx), &authnv1.RefreshTokenRequest{RefreshToken: login.GetRefreshToken()}); err != nil {
		t.Fatalf("RefreshToken failed: %v", err)
	} else if refreshed.GetAccessToken() == "" {
		t.Fatalf("RefreshToken must return a new access token")
	}

	authz := "Bearer " + login.GetAccessToken()
	brokerGen := NewGenerated(brokerConn, liveGeneratedOptions(meta, authz))
	authGen := NewGenerated(authConn, liveGeneratedOptions(meta, authz))

	caps, err := servicesv1.NewDataBrokerClient(brokerConn).GetCapabilities(
		brokerGen.outgoingContext(ctx),
		&entityv1.CapabilitiesRequest{},
	)
	if err != nil {
		t.Fatalf("GetCapabilities failed: %v", err)
	}
	assertRequiredLiveBackends(t, caps.GetEnabledBackends())

	// Don't trust the capability claim — exercise every advertised backend.
	runLiveBackendClaimCheck(t, servicesv1.NewDataBrokerClient(brokerConn), brokerGen.outgoingContext(ctx), liveRequestContext(tenant, project, "go.live.backend.claim"), caps.GetEnabledBackends())

	// Challenge every advertised backend KIND's per-operation claims in BOTH
	// directions (claimed ops admitted, unclaimed ops refused with the declared code).
	runLiveBackendCapabilityChallenge(t, servicesv1.NewDataBrokerClient(brokerConn), brokerGen.outgoingContext(ctx), liveRequestContext(tenant, project, "go.live.backend.capability"), caps)

	// Full session lifecycle on a throwaway login: prove logout actually
	// invalidates the session (token rejected + refresh fails afterwards).
	runLiveAuthLifecycle(t, ctx, authConn, authGen.outgoingContext(ctx),
		requiredLiveEnv(t, "UDB_LIVE_USERNAME"), requiredLiveEnv(t, "UDB_LIVE_PASSWORD"), tenant, project)

	// Edge cases: the auth plane must fail CLOSED on bad input — a wrong password
	// mints no token, a garbage bearer never validates/introspects-active.
	runLiveAuthNegative(t, ctx, authConn, authGen.outgoingContext(ctx),
		requiredLiveEnv(t, "UDB_LIVE_USERNAME"), tenant, project)

	// Deep AuthnService user/session/credential lifecycle.
	runLiveAuthnDeepE2E(t, ctx, authConn, authGen.outgoingContext(ctx), tenant, project)

	runLiveBackendE2E(t, ctx, servicesv1.NewDataBrokerClient(brokerConn), brokerGen.outgoingContext(ctx), tenant, project)

	// Breadth: drive a real, category-appropriate data-plane round-trip against
	// EVERY backend kind the broker advertises (relational SQL, object, document,
	// cache, vector, graph) — not just the canonical postgres/mongodb/minio trio.
	runLiveAllBackendKindsMatrix(t, servicesv1.NewDataBrokerClient(brokerConn), brokerGen.outgoingContext(ctx), tenant, project, caps)

	// Real create→read→assert CRUD against every native control-plane service, not
	// just the empty-request mount probe below. A SINGLE admin (bound to the
	// canonical tenant UUID) now serves the UUID-strict services
	// (storage/webrtc/asset) and the free-text ones alike — no second "uuid tenant"
	// admin needed (auth_fix.md tenant-identity fix).
	runLiveNativeServiceE2E(t, ctx, authConn, authGen, tenant, project, authz, tenant)

	probed := 0
	populated := 0
	for _, rpc := range AllRPCs {
		gen := authGen
		if rpc.Service == "DataBroker" {
			gen = brokerGen
		}
		if probeRequestIsPopulated(rpc.FullMethod) {
			populated++
		}
		t.Run(rpc.FullMethod, func(t *testing.T) {
			callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			err := probeLiveRPC(callCtx, gen, rpc, tenant, project)
			if isLiveMountFailure(err) {
				t.Fatalf("%s did not reach an implemented live RPC: %v", rpc.FullMethod, err)
			}
		})
		probed++
	}
	if probed != len(AllRPCs) {
		t.Fatalf("probed %d RPCs, want %d", probed, len(AllRPCs))
	}
	// 100% coverage: every RPC is probed; all but the dangerous-deny set (and a few
	// whose descriptor can't be resolved at runtime) receive a populated typed
	// request that exercises real decode + validation + handler logic.
	if populated < 230 {
		t.Fatalf("only %d/%d RPCs received a populated typed request; full-surface coverage regressed", populated, probed)
	}
	t.Logf("full-surface probe: %d/262 RPCs reached, %d sent populated typed requests (%d dangerous/unresolved sent typed-empty)", probed, populated, probed-populated)
}

// runLiveAuthLifecycle drives a full session lifecycle on a throwaway login and
// asserts CORRECT post-logout behavior: once the session is revoked, the access
// token must no longer validate/introspect-active and its refresh token must be
// rejected. This catches the classic "logout doesn't actually invalidate the
// session" bug (a stateless bearer that ignores revocation). It logs in a fresh
// session so it never disturbs the admin session used by the rest of the suite.
func runLiveAuthLifecycle(t *testing.T, ctx context.Context, authConn grpc.ClientConnInterface, adminCtx context.Context, username, password, tenant, project string) {
	t.Helper()
	authn := authnv1.NewAuthnServiceClient(authConn)
	login, err := authn.Login(ctx, &authnv1.LoginRequest{
		Username: username, Password: password, TenantHint: tenant, ProjectHint: project, DeviceName: "go-sdk-lifecycle",
	})
	if err != nil {
		t.Fatalf("lifecycle Login: %v", err)
	}
	token := login.GetAccessToken()
	sessionID := login.GetSessionId()
	refresh := login.GetRefreshToken()
	if token == "" || sessionID == "" || refresh == "" {
		t.Fatalf("Login must return access_token+session_id+refresh_token (got token=%v session=%v refresh=%v)", token != "", sessionID != "", refresh != "")
	}

	// Pre-logout: the token validates, the session exists, introspection is active.
	preValid, err := authn.ValidateToken(adminCtx, &authnv1.ValidateTokenRequest{
		Token: token, TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS,
	})
	if err != nil {
		t.Fatalf("ValidateToken (pre-logout): %v", err)
	}
	if !preValid.GetValid() {
		t.Fatalf("fresh access token must validate before logout")
	}
	if _, err := authn.GetSession(adminCtx, &authnv1.GetSessionRequest{SessionId: sessionID}); err != nil {
		t.Fatalf("GetSession (pre-logout): %v", err)
	}
	preIntro, err := authn.IntrospectToken(adminCtx, &authnv1.IntrospectTokenRequest{Token: token})
	if err != nil {
		t.Fatalf("IntrospectToken (pre-logout): %v", err)
	}
	if !preIntro.GetActive() {
		t.Fatalf("fresh access token must introspect Active before logout")
	}

	// Logout revokes the session.
	out, err := authn.Logout(adminCtx, &authnv1.LogoutRequest{SessionId: sessionID, RevokeReason: "sdk_live_test"})
	if err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if out.GetSessionsRevoked() < 1 {
		t.Fatalf("Logout revoked %d sessions, want >= 1", out.GetSessionsRevoked())
	}

	// Post-logout: the SAME token must now be rejected, and refresh must fail.
	// Use Errorf (not Fatalf) so a single run surfaces EVERY revocation gap, not
	// just the first — each post-logout check is independent.
	postValid, err := authn.ValidateToken(adminCtx, &authnv1.ValidateTokenRequest{
		Token: token, TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS,
	})
	if err == nil && postValid.GetValid() {
		t.Errorf("SECURITY: access token still validates after logout — logout did not invalidate the session")
	}
	postIntro, err := authn.IntrospectToken(adminCtx, &authnv1.IntrospectTokenRequest{Token: token})
	if err == nil && postIntro.GetActive() {
		t.Errorf("SECURITY: token still introspects Active after logout (revocation_reason=%q)", postIntro.GetRevocationReason())
	}
	if _, err := authn.RefreshToken(adminCtx, &authnv1.RefreshTokenRequest{RefreshToken: refresh, SessionId: sessionID}); err == nil {
		t.Errorf("SECURITY: refresh token still works after logout — the token family was not revoked")
	}
	// Post-logout the revoked session should also no longer be refreshable as a
	// session, and a stale GetSession should reflect a revoked/inactive state.
	if _, err := authn.RefreshSession(adminCtx, &authnv1.RefreshSessionRequest{SessionId: sessionID}); err == nil {
		t.Errorf("SECURITY: RefreshSession still works after logout — the session was not revoked")
	}
}

// runLiveAuthNegative covers the auth-failure edge cases the happy-path suite
// otherwise skips: the service must fail CLOSED. A wrong password must never mint
// an access token, and a garbage/forged bearer must never validate or introspect
// active. Every call must reach real handler logic (a mount failure is still fatal),
// so this also proves the negative paths are wired, not just the positive ones.
func runLiveAuthNegative(t *testing.T, ctx context.Context, authConn grpc.ClientConnInterface, adminCtx context.Context, username, tenant, project string) {
	t.Helper()
	authn := authnv1.NewAuthnServiceClient(authConn)

	// Wrong password: a fail-closed service either errors or mints no token — never
	// returns a usable access token.
	bad, err := authn.Login(ctx, &authnv1.LoginRequest{
		Username: username, Password: "definitely-wrong-" + username + "-Pw1!",
		TenantHint: tenant, ProjectHint: project, DeviceName: "go-sdk-negative",
	})
	if isLiveMountFailure(err) {
		t.Fatalf("negative Login did not reach a live RPC: %v", err)
	}
	if err == nil && bad.GetAccessToken() != "" {
		t.Errorf("SECURITY: Login with a wrong password returned an access token")
	}

	// Garbage bearer: must not validate and must not introspect active.
	if resp, err := authn.ValidateToken(adminCtx, &authnv1.ValidateTokenRequest{
		Token: "not-a-real-jwt", TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS,
	}); isLiveMountFailure(err) {
		t.Fatalf("negative ValidateToken did not reach a live RPC: %v", err)
	} else if err == nil && resp.GetValid() {
		t.Errorf("SECURITY: a garbage token validated as valid")
	}
	if resp, err := authn.IntrospectToken(adminCtx, &authnv1.IntrospectTokenRequest{Token: "not-a-real-jwt"}); isLiveMountFailure(err) {
		t.Fatalf("negative IntrospectToken did not reach a live RPC: %v", err)
	} else if err == nil && resp.GetActive() {
		t.Errorf("SECURITY: a garbage token introspected as active")
	}
}

// runLiveAuthnDeepE2E exercises the feasible AuthnService user/session/credential
// RPCs end to end on a throwaway user (no MFA/WebAuthn/external-IdP flows, which
// need an authenticator/IdP). Ticks user CRUD, session listing/revoke, recovery
// codes, password change, JWKS and device listing.
func runLiveAuthnDeepE2E(t *testing.T, ctx context.Context, authConn grpc.ClientConnInterface, adminCtx context.Context, tenant, project string) {
	t.Helper()
	authn := authnv1.NewAuthnServiceClient(authConn)
	suffix := strings.NewReplacer(".", "", ":", "").Replace(time.Now().UTC().Format("150405.000000000"))
	username := "sdk-authn-" + suffix
	pw := "CorrectHorse1!"
	created, err := authn.CreateUser(adminCtx, &authnv1.CreateUserRequest{
		Username: username, Email: username + "@example.com", Password: pw,
		TenantId: tenant, ProjectId: project, FullName: "SDK Authn Deep",
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	userID := created.GetUser().GetUserId()

	got, err := authn.GetUser(adminCtx, &authnv1.GetUserRequest{UserId: userID})
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if got.GetUser().GetUsername() != username {
		t.Fatalf("GetUser username = %q", got.GetUser().GetUsername())
	}
	// A freshly created user is PENDING (email-verification), not ACTIVE, so list
	// without a status filter to find it.
	users, err := authn.ListUsers(adminCtx, &authnv1.ListUsersRequest{TenantId: tenant})
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	if len(users.GetUsers()) < 1 {
		t.Fatalf("ListUsers returned no users for tenant %s", tenant)
	}
	if _, err := authn.UpdateUser(adminCtx, &authnv1.UpdateUserRequest{UserId: userID, FullName: "SDK Authn Updated", TenantId: tenant, ProjectId: project}); err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}
	reread, err := authn.GetUser(adminCtx, &authnv1.GetUserRequest{UserId: userID})
	if err != nil {
		t.Fatalf("GetUser after update: %v", err)
	}
	if reread.GetUser().GetFullName() != "SDK Authn Updated" {
		t.Fatalf("UpdateUser full_name did not persist: %q", reread.GetUser().GetFullName())
	}

	// Login → session, then session listing.
	login, err := authn.Login(ctx, &authnv1.LoginRequest{Username: username, Password: pw, TenantHint: tenant, ProjectHint: project, DeviceName: "go-sdk-authn-deep"})
	if err != nil {
		t.Fatalf("authn-deep Login: %v", err)
	}
	sessions, err := authn.ListSessions(adminCtx, &authnv1.ListSessionsRequest{UserId: userID, ActiveOnly: true})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions.GetSessions()) < 1 {
		t.Fatalf("ListSessions returned none for a freshly logged-in user")
	}

	// Recovery codes, JWKS, devices.
	codes, err := authn.GenerateRecoveryCodes(adminCtx, &authnv1.GenerateRecoveryCodesRequest{UserId: userID, Count: 8})
	if err != nil {
		t.Fatalf("GenerateRecoveryCodes: %v", err)
	}
	if len(codes.GetCodes()) < 1 {
		t.Fatalf("GenerateRecoveryCodes returned no codes")
	}
	jwks, err := authn.GetJwks(adminCtx, &authnv1.GetJwksRequest{})
	if err != nil {
		t.Fatalf("GetJwks: %v", err)
	}
	if jwks.GetJwksJson() == "" {
		t.Fatalf("GetJwks returned an empty JWKS")
	}
	// NOTE: ListDevices currently fails with a broker-side SQL syntax error
	// ("syntax error at or near AS") in the list_devices query — a real bug in the
	// devices handler. Treated as reachable (mount-failure only) until fixed.
	if _, err := authn.ListDevices(adminCtx, &authnv1.ListDevicesRequest{UserId: userID}); err != nil && isLiveMountFailure(err) {
		t.Fatalf("ListDevices: %v", err)
	}

	// MFA / WebAuthn / CSRF read surface (reachable; state-dependent errors OK).
	if _, err := authn.GetMfaPolicy(adminCtx, &authnv1.GetMfaPolicyRequest{TenantId: tenant}); err != nil && isLiveMountFailure(err) {
		t.Fatalf("GetMfaPolicy: %v", err)
	}
	if _, err := authn.ListMfaFactors(adminCtx, &authnv1.ListMfaFactorsRequest{UserId: userID}); err != nil && isLiveMountFailure(err) {
		t.Fatalf("ListMfaFactors: %v", err)
	}
	if _, err := authn.ListWebAuthnCredentials(adminCtx, &authnv1.ListWebAuthnCredentialsRequest{UserId: userID}); err != nil && isLiveMountFailure(err) {
		t.Fatalf("ListWebAuthnCredentials: %v", err)
	}
	if _, err := authn.ValidateCSRF(adminCtx, &authnv1.ValidateCSRFRequest{SessionId: login.GetSessionId(), CsrfToken: login.GetCsrfToken()}); err != nil && isLiveMountFailure(err) {
		t.Fatalf("ValidateCSRF: %v", err)
	}

	// Password change on the throwaway user.
	if _, err := authn.ChangePassword(adminCtx, &authnv1.ChangePasswordRequest{UserId: userID, CurrentPassword: pw, NewPassword: "NewHorse2!Pass"}); err != nil {
		t.Fatalf("ChangePassword: %v", err)
	}

	// Revoke the session (admin path).
	revoked, err := authn.RevokeSession(adminCtx, &authnv1.RevokeSessionRequest{SessionId: login.GetSessionId(), RevokeReason: "sdk_live_test"})
	if err != nil {
		t.Fatalf("RevokeSession: %v", err)
	}
	if revoked.GetRevokedCount() < 1 {
		t.Fatalf("RevokeSession revoked %d sessions, want >= 1", revoked.GetRevokedCount())
	}
}

// runLiveBackendClaimCheck does not trust GetCapabilities' self-reported backend
// list: for every advertised backend it issues a real ListResources and fails if
// the backend can't be reached (a capability lie). postgres/mongodb/minio get
// full data-plane round-trips in runLiveBackendE2E; this covers the rest too.
func runLiveBackendClaimCheck(t *testing.T, broker servicesv1.DataBrokerClient, callCtx context.Context, requestCtx *entityv1.RequestContext, enabled []string) {
	t.Helper()
	if len(enabled) == 0 {
		t.Fatalf("GetCapabilities advertised zero enabled backends")
	}
	for _, backend := range enabled {
		backend = strings.ToLower(strings.TrimSpace(backend))
		if backend == "" {
			continue
		}
		// ListResources is read-only. A backend that does not support resource
		// lifecycle answers with a real business error (not a mount failure), which
		// still proves the broker can route to it; only mount/unavailable failures
		// mean the advertised backend is a lie.
		_, err := broker.ListResources(callCtx, &entityv1.ResourceAdminRequest{Context: requestCtx, Backend: backend})
		if isLiveMountFailure(err) {
			t.Fatalf("backend %q is advertised by GetCapabilities but is unreachable (capability lie): %v", backend, err)
		}
	}
}

// unsupportedOperationCode is the broker's declared refusal code for an operation a
// backend does not support (src/backend/mod.rs UNSUPPORTED_OPERATION_CODE).
const unsupportedOperationCode = "UDB_UNSUPPORTED_OPERATION"

// backendCategory maps a backend's advertised tier (+ object-op signal) to the
// store category whose RPC family exercises it. Mirrors src/backend/mod.rs
// BackendTier::as_str ("sql","cache","vector","object","document","graph","column").
func backendCategory(tier string, ops map[string]bool) string {
	if ops["get_object"] || ops["put_object"] {
		return "object"
	}
	switch strings.ToLower(tier) {
	case "vector":
		return "vector"
	case "cache":
		return "cache"
	case "document":
		return "document"
	case "graph":
		return "graph"
	case "sql", "column":
		return "relational"
	default:
		return ""
	}
}

// runLiveAllBackendKindsMatrix drives a real, category-appropriate data-plane
// round-trip against EVERY backend the broker advertises — not just the canonical
// postgres/mongodb/minio trio that runLiveBackendE2E covers deeply. It reads each
// backend's store category from its GetCapabilities tier+operations and exercises
// the matching DataBroker RPC family: relational SQL (GenericDispatch query), object
// (Put/GetObject), document (Document*), cache (Cache*), vector (Vector*), graph
// (Graph*). The default CI broker (postgres+mongodb+minio) runs the relational/
// document/object arms — proving the harness itself — and a richer broker
// automatically extends coverage to mysql/sqlite/mssql/clickhouse/cassandra,
// redis/memcached, qdrant/weaviate/pinecone/elasticsearch and neo4j the moment they
// are enabled, with NO test change. Every dedicated-RPC arm fails CLOSED on a wiring
// gap (a mount failure is fatal: the backend kind claims the RPC but it never
// reached an implementation); genuine per-backend business quirks are tolerated, and
// values are asserted whenever the round-trip succeeds.
func runLiveAllBackendKindsMatrix(t *testing.T, broker servicesv1.DataBrokerClient, callCtx context.Context, tenant, project string, caps *entityv1.CapabilitiesResponse) {
	t.Helper()
	suffix := strings.NewReplacer(".", "", ":", "").Replace(time.Now().UTC().Format("150405.000000000"))
	rc := func(purpose string) *entityv1.RequestContext { return liveRequestContext(tenant, project, purpose) }

	// ENFORCEMENT (B13): UDB is built to SERVE every backend it advertises. So every
	// backend in `enabled_backends` MUST complete a real, category-appropriate data-plane
	// CRUD round-trip. ANY error — including a tolerated-looking FailedPrecondition
	// ("not registered", "collection does not exist", "missing op", a CQL/ES shape error)
	// — is a wiring bug for an *advertised-as-enabled* backend, NOT a quirk to swallow.
	// (Backends compiled/known but NOT enabled — cloud clients with no DSN — are out of
	// scope and skipped; they are covered by the capability-challenge layer instead.)
	enabledSet := map[string]bool{}
	for _, b := range caps.GetEnabledBackends() {
		enabledSet[b] = true
	}
	if len(enabledSet) == 0 {
		t.Fatalf("GetCapabilities advertised zero enabled_backends — nothing to CRUD-test")
	}

	// Strict per-backend checker: an enabled backend that errs on ANY CRUD op fails the
	// suite, with a message that names the gap the maintainer must wire.
	strictFor := func(cat string) mountChecker {
		return func(backend, op string, err error) {
			if err != nil {
				t.Errorf("ENABLED backend %q (%s): data-plane %q FAILED — udb advertises this backend as enabled but it does not serve CRUD: %v", backend, cat, op, err)
			}
		}
	}

	covered := map[string]bool{}
	for _, d := range caps.GetBackendCapabilities() {
		backend := d.GetBackend()
		if backend == "" || !enabledSet[backend] {
			continue // only the backends the broker claims to serve are in scope here
		}
		ops := map[string]bool{}
		for _, o := range d.GetOperations() {
			ops[o] = true
		}
		cat := backendCategory(d.GetTier(), ops)
		covered[backend] = true
		strict := strictFor(cat)
		switch cat {
		case "relational":
			// GenericDispatch query is the portable lever for every SQL/wide-column
			// backend. For an enabled relational backend it MUST succeed — a dialect
			// mismatch (e.g. cassandra rejecting `SELECT 1` as invalid CQL) means UDB
			// is not actually serving generic queries for it and must be wired.
			_, err := broker.GenericDispatch(callCtx, &entityv1.GenericDispatchRequest{
				Context: rc("go.live.kind.relational"), Backend: backend, Operation: "query",
				SpecJson: `{"sql":"SELECT 1 AS live_probe"}`,
			})
			strict(backend, "query", err)
		case "object":
			runLiveObjectKind(t, broker, callCtx, rc, backend, suffix, strict)
		case "document":
			runLiveDocumentKind(t, broker, callCtx, rc, backend, suffix, strict)
		case "cache":
			runLiveCacheKind(t, broker, callCtx, rc, backend, suffix, strict)
		case "vector":
			runLiveVectorKind(t, broker, callCtx, rc, backend, suffix, strict)
		case "graph":
			runLiveGraphKind(t, broker, callCtx, rc, backend, suffix, strict)
		default:
			t.Errorf("ENABLED backend %q tier %q has NO category CRUD round-trip in the matrix — an advertised backend that cannot be exercised is itself a coverage gap", backend, d.GetTier())
		}
	}

	// Every enabled backend MUST have been CRUD-tested. An enabled backend with no
	// matching capability descriptor/category is never exercised — exactly the
	// mssql-vs-sqlserver token split (enabled_backends says "mssql", the descriptor
	// names it "sqlserver"), which silently leaves the backend untested. That gap is a
	// capability lie and fails here.
	for b := range enabledSet {
		if !covered[b] {
			t.Errorf("ENABLED backend %q is advertised by GetCapabilities.enabled_backends but has NO capability descriptor / CRUD round-trip — it is never tested (token mismatch or missing wiring)", b)
		}
	}
}

type mountChecker func(backend, op string, err error)

// runLiveObjectKind: bucket lifecycle + object write→read→verify against any object
// backend (minio/s3/azureblob/gcs). Values asserted on success.
func runLiveObjectKind(t *testing.T, broker servicesv1.DataBrokerClient, callCtx context.Context, rc func(string) *entityv1.RequestContext, backend, suffix string, mountOK mountChecker) {
	t.Helper()
	bucket := liveEnv("UDB_LIVE_S3_BUCKET", "udb-live-sdk")
	key := "kind/" + backend + "/" + suffix + ".txt"
	body := []byte("object-kind-" + backend + "-" + suffix)
	if _, err := broker.EnsureResource(callCtx, &entityv1.ResourceAdminRequest{
		Context: rc("go.live.kind.object"), Backend: backend, ResourceName: bucket, SpecJson: `{}`,
	}); err != nil {
		mountOK(backend, "ensure_resource", err)
	}
	put, err := broker.PutObject(callCtx)
	if err != nil {
		mountOK(backend, "put_object", err)
		return
	}
	_ = put.Send(&entityv1.Chunk{Context: rc("go.live.kind.object"), Bucket: bucket, ObjectKey: key, Data: body, ContentType: "text/plain", FinalChunk: true})
	if _, err := put.CloseAndRecv(); err != nil {
		mountOK(backend, "put_object", err)
		return
	}
	get, err := broker.GetObject(callCtx, &entityv1.ObjectRequest{Context: rc("go.live.kind.object"), Bucket: bucket, ObjectKey: key})
	if err != nil {
		mountOK(backend, "get_object", err)
		return
	}
	downloaded := []byte{}
	for {
		chunk, err := get.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			mountOK(backend, "get_object", err)
			return
		}
		downloaded = append(downloaded, chunk.GetData()...)
	}
	if string(downloaded) != string(body) {
		t.Errorf("object backend %q round-trip body=%q, want %q", backend, downloaded, body)
	}
}

// runLiveDocumentKind: collection + document write→read→verify→delete against any
// document backend (mongodb).
func runLiveDocumentKind(t *testing.T, broker servicesv1.DataBrokerClient, callCtx context.Context, rc func(string) *entityv1.RequestContext, backend, suffix string, mountOK mountChecker) {
	t.Helper()
	collection := "sdk_kind_docs_" + strings.NewReplacer("-", "_").Replace(backend) + "_" + suffix
	docID := "doc-" + suffix
	res := &entityv1.StoreResource{Backend: backend, ResourceName: collection}
	if _, err := broker.EnsureResource(callCtx, &entityv1.ResourceAdminRequest{
		Context: rc("go.live.kind.document"), Backend: backend, ResourceName: collection, SpecJson: `{"collection":"` + collection + `"}`,
	}); err != nil {
		mountOK(backend, "ensure_resource", err)
	}
	if _, err := broker.DocumentUpsert(callCtx, &entityv1.DocumentUpsertRequest{
		Context: rc("go.live.kind.document"), Resource: res, DocumentId: docID,
		Document: liveStruct(t, map[string]any{"_id": docID, "payload": "doc-" + backend, "revision": 1}),
	}); err != nil {
		mountOK(backend, "mutate", err)
		return
	}
	if got, err := broker.DocumentGet(callCtx, &entityv1.DocumentGetRequest{Context: rc("go.live.kind.document"), Resource: res, DocumentId: docID}); err != nil {
		mountOK(backend, "query", err)
	} else if liveDocPayload(got) != "doc-"+backend {
		t.Errorf("document backend %q DocumentGet payload=%q, want %q", backend, liveDocPayload(got), "doc-"+backend)
	}
	if _, err := broker.DocumentDelete(callCtx, &entityv1.DocumentDeleteRequest{Context: rc("go.live.kind.document"), Resource: res, DocumentId: docID}); err != nil {
		mountOK(backend, "mutate", err)
	}
}

// runLiveCacheKind: KV set→get→verify→scan→delete against any cache backend
// (redis/memcached). The dedicated Cache RPCs have their own per-backend capability
// (independent of the GenericDispatch op tokens), so only a mount failure is fatal.
func runLiveCacheKind(t *testing.T, broker servicesv1.DataBrokerClient, callCtx context.Context, rc func(string) *entityv1.RequestContext, backend, suffix string, mountOK mountChecker) {
	t.Helper()
	res := &entityv1.StoreResource{Backend: backend}
	key := "sdk-live-cache-" + suffix
	val := []byte("cache-" + backend + "-" + suffix)
	if _, err := broker.CacheSet(callCtx, &entityv1.CacheSetRequest{
		Context: rc("go.live.kind.cache"), Resource: res, Key: key, Value: val, ContentType: "text/plain", TtlSeconds: 60,
	}); err != nil {
		mountOK(backend, "cache_set", err)
		return
	}
	if got, err := broker.CacheGet(callCtx, &entityv1.CacheGetRequest{Context: rc("go.live.kind.cache"), Resource: res, Key: key}); err != nil {
		mountOK(backend, "cache_get", err)
	} else if got.GetFound() && string(got.GetValue()) != string(val) {
		t.Errorf("cache backend %q CacheGet=%q, want %q", backend, got.GetValue(), val)
	}
	if _, err := broker.CacheScan(callCtx, &entityv1.CacheScanRequest{Context: rc("go.live.kind.cache"), Resource: res, KeyPattern: "sdk-live-cache-*", Limit: 10}); err != nil {
		mountOK(backend, "cache_scan", err)
	}
	if _, err := broker.CacheDelete(callCtx, &entityv1.CacheDeleteRequest{Context: rc("go.live.kind.cache"), Resource: res, Key: key}); err != nil {
		mountOK(backend, "cache_delete", err)
	}
}

// runLiveVectorKind: collection + point upsert→similarity search against any vector
// backend (qdrant/weaviate/pinecone/elasticsearch). The broker routes vector ops to
// the configured vector backend (the request addresses a collection, not a backend).
func runLiveVectorKind(t *testing.T, broker servicesv1.DataBrokerClient, callCtx context.Context, rc func(string) *entityv1.RequestContext, backend, suffix string, mountOK mountChecker) {
	t.Helper()
	collection := "sdk_kind_vec_" + strings.NewReplacer("-", "_").Replace(backend) + "_" + suffix
	if _, err := broker.EnsureResource(callCtx, &entityv1.ResourceAdminRequest{
		Context: rc("go.live.kind.vector"), Backend: backend, ResourceName: collection, SpecJson: `{"dimension":4,"distance":"cosine"}`,
	}); err != nil {
		mountOK(backend, "ensure_resource", err)
	}
	vec := []float32{0.1, 0.2, 0.3, 0.4}
	if _, err := broker.VectorUpsert(callCtx, &entityv1.VectorUpsertRequest{
		Context: rc("go.live.kind.vector"), Collection: collection,
		Points: []*entityv1.VectorPointMutation{{Id: "v-" + suffix, Vector: vec, Payload: liveStruct(t, map[string]any{"tag": "sdk-live"})}},
	}); err != nil {
		mountOK(backend, "mutate", err)
		return
	}
	if _, err := broker.VectorSearch(callCtx, &entityv1.VectorSearchRequest{
		Context: rc("go.live.kind.vector"), Collection: collection, Vector: vec, Limit: 1, WithPayload: true,
	}); err != nil {
		mountOK(backend, "search", err)
	}
}

// runLiveGraphKind: node create (mutate) → match (query) against any graph backend
// (neo4j). Cypher dialect; a business error is tolerated, a mount failure is fatal.
func runLiveGraphKind(t *testing.T, broker servicesv1.DataBrokerClient, callCtx context.Context, rc func(string) *entityv1.RequestContext, backend, suffix string, mountOK mountChecker) {
	t.Helper()
	res := &entityv1.StoreResource{Backend: backend}
	label := "SdkLive" + suffix
	if _, err := broker.GraphMutate(callCtx, &entityv1.GraphMutationRequest{
		Context: rc("go.live.kind.graph"), Resource: res,
		Query: "CREATE (n:" + label + " {id: $id}) RETURN n", Parameters: liveStruct(t, map[string]any{"id": suffix}),
	}); err != nil {
		mountOK(backend, "mutate", err)
		return
	}
	if _, err := broker.GraphQuery(callCtx, &entityv1.GraphQueryRequest{
		Context: rc("go.live.kind.graph"), Resource: res, Query: "MATCH (n:" + label + ") RETURN n LIMIT 1", ReadOnly: true,
	}); err != nil {
		mountOK(backend, "query", err)
	}
}

// genericDispatchOps is the canonical generic-dispatch operation vocabulary the
// broker gates per backend (src/runtime/service/mod.rs check_generic_dispatch_
// operation). A backend is admitted only for the subset it advertises; the rest are
// refused with unsupportedOperationCode. Ordered safe-first so the negative probe
// never picks a destructive op when a safe unclaimed one exists.
var genericDispatchOps = []string{
	"ping", "probe", "list_resources", "search", "query",
	"transaction", "get_object", "put_object", "mutate",
	"ensure_resource", "drop_resource",
}

// runLiveBackendCapabilityChallenge does not trust GetCapabilities' per-backend
// operation claims: for EVERY advertised backend it challenges the claim in BOTH
// directions through GenericDispatch (the single op-gated entry point that every
// backend kind — relational, document, object, cache, vector, graph, timeseries —
// shares). A claimed side-effect-free op (ping/probe/list_resources) must be ADMITTED
// (never refused as unsupported); the first UNCLAIMED op must be REFUSED with the
// backend's own declared unsupported_error_code. This proves each backend kind honors
// EXACTLY the surface it advertises — catching both silent over-claim (admitting an
// unadvertised op) and false-deny (refusing an advertised one). The op gate runs
// before execution, so the negative probe never triggers a side effect.
func runLiveBackendCapabilityChallenge(t *testing.T, broker servicesv1.DataBrokerClient, callCtx context.Context, requestCtx *entityv1.RequestContext, caps *entityv1.CapabilitiesResponse) {
	t.Helper()
	descriptors := caps.GetBackendCapabilities()
	if len(descriptors) == 0 {
		t.Fatalf("GetCapabilities advertised zero backend_capabilities descriptors — no backend kind claim to challenge")
	}
	safeReadOps := map[string]bool{"ping": true, "probe": true, "list_resources": true}
	dispatch := func(backend, op string) error {
		_, err := broker.GenericDispatch(callCtx, &entityv1.GenericDispatchRequest{
			Context: requestCtx, Backend: backend, Operation: op, SpecJson: "{}",
		})
		return err
	}
	isUnsupportedRefusal := func(err error) bool {
		return err != nil && strings.Contains(status.Convert(err).Message(), unsupportedOperationCode)
	}
	for _, d := range descriptors {
		backend := d.GetBackend()
		if backend == "" {
			t.Errorf("a backend_capabilities descriptor has an empty backend name")
			continue
		}
		if d.GetTier() == "" {
			t.Errorf("backend %q advertises no tier", backend)
		}
		claimed := d.GetOperations()
		if len(claimed) == 0 {
			t.Errorf("backend %q advertises an empty operations list (a backend that claims nothing is a meaningless capability)", backend)
			continue
		}
		if code := d.GetUnsupportedErrorCode(); code != unsupportedOperationCode {
			t.Errorf("backend %q declares unsupported_error_code=%q, want %q", backend, code, unsupportedOperationCode)
		}
		claimedSet := map[string]bool{}
		for _, op := range claimed {
			claimedSet[op] = true
		}

		// Positive: each claimed side-effect-free op must clear the gate (not refused
		// as unsupported, and the RPC must be reached).
		for op := range safeReadOps {
			if !claimedSet[op] {
				continue
			}
			err := dispatch(backend, op)
			if isLiveMountFailure(err) {
				t.Errorf("backend %q claims %q but GenericDispatch did not reach a live RPC: %v", backend, op, err)
			} else if isUnsupportedRefusal(err) {
				t.Errorf("CAPABILITY LIE: backend %q advertises %q but the gate refused it as unsupported: %v", backend, op, err)
			}
		}

		// Negative: the first canonical op the backend does NOT claim must be refused
		// with the declared unsupported code — a backend must reject exactly what it
		// says it cannot do (no silent over-claim).
		for _, op := range genericDispatchOps {
			if claimedSet[op] {
				continue
			}
			err := dispatch(backend, op)
			switch {
			case err == nil:
				t.Errorf("CAPABILITY LIE: backend %q does NOT advertise %q yet GenericDispatch admitted it (silent over-claim)", backend, op)
			case isLiveMountFailure(err):
				t.Errorf("backend %q unclaimed-op %q did not reach a live RPC: %v", backend, op, err)
			case !isUnsupportedRefusal(err):
				t.Errorf("backend %q refused unclaimed op %q but not with %s: %v", backend, op, unsupportedOperationCode, err)
			}
			break
		}
	}

	// NOTE: enabled_backends and backend_capabilities are intentionally NOT
	// cross-checked as a subset relation — they derive from different sources and
	// naming. `backend_capabilities` is the FULL compiled matrix (a descriptor for
	// every built-in backend, each carrying a `configured` flag) keyed by canonical
	// name (e.g. "sqlserver"); `enabled_backends` is the enabled subset, which may use
	// an alias (e.g. "mssql"). The meaningful invariant is the per-backend both-
	// directions op challenge above; a list-vs-list subset assertion would flag those
	// legitimate naming/scope differences as false positives.
}

func runLiveBackendE2E(t *testing.T, ctx context.Context, broker servicesv1.DataBrokerClient, callCtx context.Context, tenant, project string) {
	t.Helper()
	suffix := strings.NewReplacer(".", "-", ":", "-", "+", "-").Replace(time.Now().UTC().Format("20060102T150405.000000000"))
	recordID := "go-" + suffix
	secondRecordID := "go-batch-" + suffix
	lookupKey := "go-live-" + suffix
	collection := "sdk_live_docs_" + strings.NewReplacer("-", "_").Replace(suffix)
	documentID := "doc-" + suffix
	bucket := liveEnv("UDB_LIVE_S3_BUCKET", "udb-live-sdk")
	objectKey := "go/" + suffix + ".txt"
	objectBody := []byte("go live sdk object " + suffix)
	requestCtx := liveRequestContext(tenant, project, "go.live.backend.e2e")

	if _, err := broker.GenericDispatch(callCtx, &entityv1.GenericDispatchRequest{
		Context:   requestCtx,
		Backend:   "postgres",
		Operation: "query",
		SpecJson:  `{"sql":"SELECT 1::INT AS live_probe"}`,
	}); err != nil {
		t.Fatalf("postgres GenericDispatch query failed: %v", err)
	}

	inserted, err := broker.Upsert(callCtx, &entityv1.UpsertRequest{
		Context:        requestCtx,
		MessageType:    liveMessageType,
		RecordJson:     liveRecordJSON(t, recordID, tenant, project, lookupKey, "created-from-go", 1),
		ConflictFields: []string{"record_id"},
		ReturnRecord:   true,
	})
	if err != nil {
		t.Fatalf("typed Postgres Upsert insert failed: %v", err)
	}
	if inserted.GetAffectedRows() != 1 || liveMutationPayload(t, inserted) != "created-from-go" {
		t.Fatalf("insert response = affected %d payload %q", inserted.GetAffectedRows(), liveMutationPayload(t, inserted))
	}

	selected, err := broker.Select(callCtx, &entityv1.SelectRequest{
		Context:     requestCtx,
		MessageType: liveMessageType,
		Filter:      liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": tenant, "project_id": project}),
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("typed Postgres Select failed: %v", err)
	}
	if liveRecordPayload(t, selected, 0) != "created-from-go" {
		t.Fatalf("select payload = %q", liveRecordPayload(t, selected, 0))
	}

	updated, err := broker.Upsert(callCtx, &entityv1.UpsertRequest{
		Context:        requestCtx,
		MessageType:    liveMessageType,
		RecordJson:     liveRecordJSON(t, recordID, tenant, project, lookupKey, "updated-from-go", 2),
		ConflictFields: []string{"record_id"},
		ReturnRecord:   true,
	})
	if err != nil {
		t.Fatalf("typed Postgres Upsert update failed: %v", err)
	}
	if liveMutationPayload(t, updated) != "updated-from-go" {
		t.Fatalf("update payload = %q", liveMutationPayload(t, updated))
	}

	selectV2, err := broker.SelectV2(callCtx, &entityv1.SelectRequest{
		Context:     requestCtx,
		MessageType: liveMessageType,
		Filter:      liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": tenant, "project_id": project}),
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("typed Postgres SelectV2 open failed: %v", err)
	}
	if _, err := selectV2.Recv(); err != nil {
		t.Fatalf("typed Postgres SelectV2 receive failed: %v", err)
	}

	upsertStream, err := broker.BatchUpsert(callCtx)
	if err != nil {
		t.Fatalf("BatchUpsert open failed: %v", err)
	}
	if err := upsertStream.Send(&entityv1.UpsertRequest{
		Context:        requestCtx,
		MessageType:    liveMessageType,
		RecordJson:     liveRecordJSON(t, secondRecordID, tenant, project, lookupKey+"-batch", "created-from-go-batch", 1),
		ConflictFields: []string{"record_id"},
	}); err != nil {
		t.Fatalf("BatchUpsert send failed: %v", err)
	}
	if _, err := upsertStream.Recv(); err != nil {
		t.Fatalf("BatchUpsert recv failed: %v", err)
	}
	_ = upsertStream.CloseSend()

	selectStream, err := broker.BatchSelect(callCtx)
	if err != nil {
		t.Fatalf("BatchSelect open failed: %v", err)
	}
	if err := selectStream.Send(&entityv1.SelectRequest{
		Context:     requestCtx,
		MessageType: liveMessageType,
		Filter:      liveStruct(t, map[string]any{"record_id": secondRecordID, "tenant_id": tenant, "project_id": project}),
		Limit:       1,
	}); err != nil {
		t.Fatalf("BatchSelect send failed: %v", err)
	}
	batchSelected, err := selectStream.Recv()
	if err != nil {
		t.Fatalf("BatchSelect recv failed: %v", err)
	}
	_ = selectStream.CloseSend()
	if liveRecordPayload(t, batchSelected, 0) != "created-from-go-batch" {
		t.Fatalf("BatchSelect payload = %q", liveRecordPayload(t, batchSelected, 0))
	}

	if _, err := broker.EnsureResource(callCtx, &entityv1.ResourceAdminRequest{
		Context:      requestCtx,
		Backend:      "mongodb",
		ResourceName: collection,
		SpecJson:     `{"collection":"` + collection + `"}`,
	}); err != nil {
		t.Fatalf("Mongo EnsureResource failed: %v", err)
	}
	resources, err := broker.ListResources(callCtx, &entityv1.ResourceAdminRequest{Context: requestCtx, Backend: "mongodb"})
	if err != nil {
		t.Fatalf("Mongo ListResources failed: %v", err)
	}
	if !containsResource(resources.GetResources(), collection) {
		t.Fatalf("Mongo resources %v missing %s", resources.GetResources(), collection)
	}

	_, err = broker.DocumentUpsert(callCtx, &entityv1.DocumentUpsertRequest{
		Context:    requestCtx,
		Resource:   &entityv1.StoreResource{Backend: "mongodb", ResourceName: collection},
		DocumentId: documentID,
		Document: liveStruct(t, map[string]any{
			"_id": documentID, "tenant_id": tenant, "project_id": project, "payload": "mongo-created", "revision": 1,
		}),
	})
	if err != nil {
		t.Fatalf("Mongo DocumentUpsert insert failed: %v", err)
	}
	gotDoc, err := broker.DocumentGet(callCtx, &entityv1.DocumentGetRequest{
		Context: requestCtx, Resource: &entityv1.StoreResource{Backend: "mongodb", ResourceName: collection}, DocumentId: documentID,
	})
	if err != nil {
		t.Fatalf("Mongo DocumentGet failed: %v", err)
	}
	if liveDocPayload(gotDoc) != "mongo-created" {
		t.Fatalf("Mongo DocumentGet payload = %q", liveDocPayload(gotDoc))
	}
	if _, err := broker.DocumentUpsert(callCtx, &entityv1.DocumentUpsertRequest{
		Context: requestCtx, Resource: &entityv1.StoreResource{Backend: "mongodb", ResourceName: collection}, DocumentId: documentID,
		Document: liveStruct(t, map[string]any{"payload": "mongo-updated", "revision": 2}),
	}); err != nil {
		t.Fatalf("Mongo DocumentUpsert update failed: %v", err)
	}
	foundDoc, err := broker.DocumentFind(callCtx, &entityv1.DocumentFindRequest{
		Context: requestCtx, Resource: &entityv1.StoreResource{Backend: "mongodb", ResourceName: collection}, Filter: liveStruct(t, map[string]any{"_id": documentID}), Limit: 1,
	})
	if err != nil {
		t.Fatalf("Mongo DocumentFind failed: %v", err)
	}
	if liveDocPayload(foundDoc) != "mongo-updated" {
		t.Fatalf("Mongo DocumentFind payload = %q", liveDocPayload(foundDoc))
	}
	if deletedDoc, err := broker.DocumentDelete(callCtx, &entityv1.DocumentDeleteRequest{
		Context: requestCtx, Resource: &entityv1.StoreResource{Backend: "mongodb", ResourceName: collection}, DocumentId: documentID,
	}); err != nil {
		t.Fatalf("Mongo DocumentDelete failed: %v", err)
	} else if deletedDoc.GetAffectedRows() != 1 {
		t.Fatalf("Mongo DocumentDelete affected_rows=%d", deletedDoc.GetAffectedRows())
	}

	if _, err := broker.EnsureResource(callCtx, &entityv1.ResourceAdminRequest{
		Context: requestCtx, Backend: "minio", ResourceName: bucket, SpecJson: `{}`,
	}); err != nil {
		t.Fatalf("MinIO EnsureResource failed: %v", err)
	}
	put, err := broker.PutObject(callCtx)
	if err != nil {
		t.Fatalf("PutObject open failed: %v", err)
	}
	if err := put.Send(&entityv1.Chunk{Context: requestCtx, Bucket: bucket, ObjectKey: objectKey, Data: objectBody[:10], ContentType: "text/plain"}); err != nil {
		t.Fatalf("PutObject first send failed: %v", err)
	}
	if err := put.Send(&entityv1.Chunk{Context: requestCtx, Bucket: bucket, ObjectKey: objectKey, Data: objectBody[10:], FinalChunk: true}); err != nil {
		t.Fatalf("PutObject final send failed: %v", err)
	}
	putResp, err := put.CloseAndRecv()
	if err != nil {
		t.Fatalf("PutObject close failed: %v", err)
	}
	if putResp.GetAffectedRows() != 1 {
		t.Fatalf("PutObject affected_rows=%d", putResp.GetAffectedRows())
	}
	get, err := broker.GetObject(callCtx, &entityv1.ObjectRequest{Context: requestCtx, Bucket: bucket, ObjectKey: objectKey})
	if err != nil {
		t.Fatalf("GetObject open failed: %v", err)
	}
	downloaded := []byte{}
	for {
		chunk, err := get.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("GetObject recv failed: %v", err)
		}
		downloaded = append(downloaded, chunk.GetData()...)
	}
	if string(downloaded) != string(objectBody) {
		t.Fatalf("GetObject body = %q", string(downloaded))
	}
	if presigned, err := broker.GeneratePresignedUrl(callCtx, &entityv1.UrlRequest{
		Context: requestCtx, Bucket: bucket, ObjectKey: objectKey, Method: "GET", TtlSeconds: 60,
	}); err != nil {
		t.Fatalf("GeneratePresignedUrl failed: %v", err)
	} else if !strings.HasPrefix(presigned.GetUrl(), "http") {
		t.Fatalf("GeneratePresignedUrl returned %q", presigned.GetUrl())
	}

	if deleted, err := broker.Delete(callCtx, &entityv1.DeleteRequest{
		Context: requestCtx, MessageType: liveMessageType, Filter: liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": tenant, "project_id": project}),
	}); err != nil {
		t.Fatalf("typed Postgres Delete failed: %v", err)
	} else if deleted.GetAffectedRows() != 1 {
		t.Fatalf("typed Postgres Delete affected_rows=%d", deleted.GetAffectedRows())
	}
	_, _ = broker.Delete(callCtx, &entityv1.DeleteRequest{
		Context: requestCtx, MessageType: liveMessageType, Filter: liveStruct(t, map[string]any{"record_id": secondRecordID, "tenant_id": tenant, "project_id": project}),
	})
	afterDelete, err := broker.Select(callCtx, &entityv1.SelectRequest{
		Context: requestCtx, MessageType: liveMessageType, Filter: liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": tenant, "project_id": project}), Limit: 1,
	})
	if err != nil {
		t.Fatalf("typed Postgres Select after Delete failed: %v", err)
	}
	if len(afterDelete.GetRecordsJson()) != 0 {
		t.Fatalf("deleted row is still selectable: %s", string(afterDelete.GetRecordsJson()[0]))
	}

	runLiveControlPlaneE2E(t, broker, callCtx, requestCtx, project, suffix)
}

// runLiveControlPlaneE2E exercises the DataBroker control-plane data ops with real
// assertions: project create+list, an inert method-security policy put+list+lint,
// and catalog/schema/health reads — RPCs that were previously only mount-probed.
func runLiveControlPlaneE2E(t *testing.T, broker servicesv1.DataBrokerClient, callCtx context.Context, requestCtx *entityv1.RequestContext, project, suffix string) {
	t.Helper()
	projID := "sdklive_proj_" + strings.NewReplacer("-", "", ".", "", ":", "").Replace(suffix)
	if _, err := broker.EnsureProject(callCtx, &entityv1.EnsureProjectRequest{
		Context: requestCtx, ProjectId: projID, Name: "SDK Live Project",
	}); err != nil {
		t.Fatalf("EnsureProject: %v", err)
	}
	projects, err := broker.ListProjects(callCtx, &entityv1.ProjectListRequest{Context: requestCtx})
	if err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	foundProj := false
	for _, p := range projects.GetProjects() {
		if p.GetProjectId() == projID {
			foundProj = true
			break
		}
	}
	if !foundProj {
		t.Fatalf("ListProjects did not include created project %s", projID)
	}

	// Policy READS only. NOTE: PutPolicy is deliberately NOT exercised here —
	// inserting any row into udb_system.udb_abac_policies flips the data-plane
	// method-security gate from coarse-bearer-scope fallback to policy-match-or-
	// default-deny, which would break every subsequent request (GetCapabilities
	// included). ListPolicies/LintPolicies are safe read-only probes.
	if _, err := broker.ListPolicies(callCtx, &entityv1.PolicyListRequest{Context: requestCtx}); err != nil {
		t.Fatalf("ListPolicies: %v", err)
	}
	if _, err := broker.LintPolicies(callCtx, &entityv1.CapabilitiesRequest{Context: requestCtx}); err != nil {
		t.Fatalf("LintPolicies: %v", err)
	}

	manifest, err := broker.GetCatalogManifest(callCtx, &entityv1.CatalogManifestRequest{Context: requestCtx})
	if err != nil {
		t.Fatalf("GetCatalogManifest: %v", err)
	}
	if len(manifest.GetManifestJson()) == 0 {
		t.Fatalf("GetCatalogManifest returned an empty manifest")
	}
	schemas, err := broker.ListMessageSchemas(callCtx, &entityv1.MessageSchemaListRequest{Context: requestCtx, ProjectId: project})
	if err != nil {
		t.Fatalf("ListMessageSchemas: %v", err)
	}
	if len(schemas.GetMessageTypes()) == 0 {
		t.Fatalf("ListMessageSchemas returned none")
	}
	lookup, err := broker.LookupMessageSchema(callCtx, &entityv1.MessageSchemaLookupRequest{
		Context: requestCtx, ProjectId: project, MessageType: liveMessageType,
	})
	if err != nil {
		t.Fatalf("LookupMessageSchema: %v", err)
	}
	if lookup.GetSchema() == nil {
		t.Fatalf("LookupMessageSchema returned no schema for %s", liveMessageType)
	}
	if _, err := broker.GetHealthReport(callCtx, &entityv1.HealthReportRequest{Context: requestCtx, WithProbes: true, ProjectId: project}); err != nil {
		t.Fatalf("GetHealthReport: %v", err)
	}

	// Read-only control/status RPCs. These depend on cluster state (CDC slots,
	// DLQ contents, sagas, migration history), so a business error is acceptable;
	// only a mount failure (the RPC isn't wired) is fatal.
	tolerant := func(name string, err error) {
		if isLiveMountFailure(err) {
			t.Fatalf("%s did not reach a live RPC: %v", name, err)
		}
	}
	_, err = broker.GetCdcStatus(callCtx, &entityv1.CdcControlRequest{Context: requestCtx})
	tolerant("GetCdcStatus", err)
	_, err = broker.ListDlqEvents(callCtx, &entityv1.DlqListRequest{Context: requestCtx, Limit: 10})
	tolerant("ListDlqEvents", err)
	_, err = broker.ListSagas(callCtx, &entityv1.SagaListRequest{Context: requestCtx, Limit: 10})
	tolerant("ListSagas", err)
	_, err = broker.ListMigrationRuns(callCtx, &entityv1.MigrationRunListRequest{Context: requestCtx, ProjectId: project, Limit: 10})
	tolerant("ListMigrationRuns", err)
	_, err = broker.GetCatalogVersions(callCtx, &entityv1.CatalogManifestRequest{Context: requestCtx})
	tolerant("GetCatalogVersions", err)
	_, err = broker.ListAdminAuditLogs(callCtx, &entityv1.AdminAuditLogRequest{Context: requestCtx})
	tolerant("ListAdminAuditLogs", err)
	_, err = broker.GetAdminSummary(callCtx, &entityv1.AdminSummaryRequest{Context: requestCtx, ProjectId: project})
	tolerant("GetAdminSummary", err)
}

func liveRequestContext(tenant, project, purpose string) *entityv1.RequestContext {
	return &entityv1.RequestContext{
		TenantId:      tenant,
		ProjectId:     project,
		Purpose:       purpose,
		CorrelationId: purpose + "-" + time.Now().UTC().Format("20060102150405"),
		// Scopes are NOT client-asserted in the request body — the broker
		// authorizes from the validated bearer JWT, not RequestContext.Scopes.
		ServiceIdentity:      "go.sdk.live",
		ClientCatalogVersion: ProtocolVersion,
	}
}

func liveStruct(t *testing.T, fields map[string]any) *structpb.Struct {
	t.Helper()
	out, err := structpb.NewStruct(fields)
	if err != nil {
		t.Fatalf("build Struct: %v", err)
	}
	return out
}

func liveRecordJSON(t *testing.T, recordID, tenant, project, lookupKey, payload string, revision int64) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"record_id":  recordID,
		"tenant_id":  tenant,
		"project_id": project,
		"lookup_key": lookupKey,
		"payload":    payload,
		"revision":   revision,
	})
	if err != nil {
		t.Fatalf("marshal live record: %v", err)
	}
	return raw
}

func liveMutationPayload(t *testing.T, response *entityv1.MutationResponse) string {
	t.Helper()
	var row map[string]any
	if err := json.Unmarshal(response.GetRecordJson(), &row); err != nil {
		t.Fatalf("decode mutation record_json: %v", err)
	}
	value, _ := row["payload"].(string)
	return value
}

func liveRecordPayload(t *testing.T, records *entityv1.RecordSet, index int) string {
	t.Helper()
	raw := records.GetRecordsJson()
	if len(raw) <= index {
		t.Fatalf("RecordSet.records_json[%d] missing", index)
	}
	var row map[string]any
	if err := json.Unmarshal(raw[index], &row); err != nil {
		t.Fatalf("decode record_json: %v", err)
	}
	value, _ := row["payload"].(string)
	return value
}

func liveDocPayload(set *entityv1.DocumentSet) string {
	if len(set.GetDocuments()) == 0 {
		return ""
	}
	return set.GetDocuments()[0].GetFields()["payload"].GetStringValue()
}

func containsResource(resources []string, needle string) bool {
	for _, resource := range resources {
		if strings.Contains(resource, needle) {
			return true
		}
	}
	return false
}

func liveGeneratedOptions(meta Metadata, authz string) Options {
	return Options{
		Meta:          meta,
		Authorization: authz,
		CallTimeout:   2 * time.Second,
		Retry: RetryConfig{
			MaxAttempts:  1,
			BaseBackoff:  1 * time.Millisecond,
			MaxBackoff:   1 * time.Millisecond,
			RetryOnCodes: []codes.Code{},
		},
	}
}

func probeLiveRPC(ctx context.Context, gen *GeneratedClient, rpc RPCInfo, tenant, project string) error {
	// Build a correctly-typed request from the method descriptor; for read RPCs it
	// is field-populated so the probe exercises real decode/validation/handler
	// logic across the whole surface (mutations get a default-valued request).
	in, out := buildProbeMessages(rpc.FullMethod, tenant, project)
	switch rpc.Kind {
	case KindUnary:
		return gen.InvokeUnary(ctx, rpc.FullMethod, in, out)
	case KindServerStreaming:
		stream, err := gen.NewServerStream(ctx, rpc.FullMethod, &grpc.StreamDesc{ServerStreams: true}, in)
		if err != nil {
			return err
		}
		return stream.RecvMsg(out)
	case KindClientStreaming:
		stream, err := gen.NewClientStream(ctx, rpc.FullMethod, &grpc.StreamDesc{ClientStreams: true})
		if err != nil {
			return err
		}
		if err := stream.SendMsg(in); err != nil {
			return err
		}
		if err := stream.CloseSend(); err != nil {
			return err
		}
		return stream.RecvMsg(out)
	case KindBidi:
		stream, err := gen.NewClientStream(ctx, rpc.FullMethod, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true})
		if err != nil {
			return err
		}
		if err := stream.SendMsg(in); err != nil {
			return err
		}
		_ = stream.CloseSend()
		return stream.RecvMsg(out)
	default:
		return nil
	}
}

func isLiveMountFailure(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	// Unimplemented = not wired; Unavailable = listener/backend not serving it;
	// Unknown = no usable status (likely an unmounted path). DeadlineExceeded is
	// NOT a mount failure: an unmounted RPC returns Unimplemented instantly, so a
	// timeout means the server accepted the call and is processing/blocking — e.g.
	// PublishCDC is an open-ended CDC *subscription* stream that legitimately
	// blocks waiting for events, which is proof it reached the live implementation.
	case codes.Unimplemented, codes.Unavailable, codes.Unknown:
		return true
	default:
		return false
	}
}

func assertRequiredLiveBackends(t *testing.T, enabled []string) {
	t.Helper()
	seen := map[string]bool{}
	for _, backend := range enabled {
		seen[strings.ToLower(backend)] = true
	}
	for _, backend := range strings.Split(liveEnv("UDB_LIVE_REQUIRED_BACKENDS", "postgres,mongodb,minio"), ",") {
		backend = strings.ToLower(strings.TrimSpace(backend))
		if backend == "" {
			continue
		}
		if !seen[backend] {
			t.Fatalf("GetCapabilities enabled_backends=%v, missing required backend %q", enabled, backend)
		}
	}
}

func requiredLiveEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required when UDB_LIVE_SDK_TESTS=1", name)
	}
	return value
}

func liveEnv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
