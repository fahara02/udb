package udbclient

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ── Phase 7 (M9): outbound conformance unit tests ────────────────────────────
//
// These tests pin the SDK's outbound contract with the broker: the requested
// scopes carried on Authorize / GetNativeAccess, the canonical metadata headers
// attached to every call, the AuthzCache TTL behaviour, and the policy-bundle
// HMAC verification. They use an in-process fake AuthzServiceClient that records
// the outgoing request + context, so no network or running broker is needed.

// fakeAuthz is a test double for authzv1.AuthzServiceClient. It embeds the
// generated interface (so it satisfies the full method set) and overrides only
// the RPCs the tests drive, recording the last request and context seen.
type fakeAuthz struct {
	authzv1.AuthzServiceClient

	lastCtx        context.Context
	lastAuthzReq   *authzv1.AuthzRequest
	lastNativeReq  *authzv1.NativeAccessRequest
	decision       *authzv1.Decision
	nativeGrant    *authzv1.NativeAccessGrant
	authorizeCalls int

	// rpcSeq records the ordered method names for the RPC-sequence gate; the
	// AllowRole/BindRole alias tests assert exactly one declared RPC, no probes.
	rpcSeq           *[]string
	lastPolicyRule   *authzv1.CreatePolicyRuleRequest
	lastAssignRole   *authzv1.AssignRoleRequest
	createdPolicyID  string
	assignedRoleName string
}

func (f *fakeAuthz) rec(m string) {
	if f.rpcSeq != nil {
		*f.rpcSeq = append(*f.rpcSeq, m)
	}
}

func (f *fakeAuthz) CreatePolicyRule(ctx context.Context, in *authzv1.CreatePolicyRuleRequest, _ ...grpc.CallOption) (*authzv1.CreatePolicyRuleResponse, error) {
	f.rec("CreatePolicyRule")
	f.lastCtx = ctx
	f.lastPolicyRule = in
	return &authzv1.CreatePolicyRuleResponse{}, nil
}

func (f *fakeAuthz) AssignRole(ctx context.Context, in *authzv1.AssignRoleRequest, _ ...grpc.CallOption) (*authzv1.AssignRoleResponse, error) {
	f.rec("AssignRole")
	f.lastCtx = ctx
	f.lastAssignRole = in
	return &authzv1.AssignRoleResponse{}, nil
}

func TestAuthnAuthzResponseJSONFixturesDoNotExposePersistedCredentials(t *testing.T) {
	payload, err := json.Marshal([]any{
		map[string]any{"user": map[string]any{"user_id": "u1", "username": "ada"}},
		map[string]any{"session": map[string]any{"session_id": "sesspub_abc"}},
		map[string]any{"key": map[string]any{"key_id": "udbk_abc", "key_prefix": "udbk_abc"}},
		map[string]any{"audits": []any{map[string]any{"decision_audit_id": "a1", "user_id": "u1"}}},
	})
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	text := string(payload)
	for _, banned := range []string{
		"argon2id$",
		"hmac-sha256:",
		"password_hash",
		"passwordHash",
		"totp_secret_enc",
		"totpSecretEnc",
		"session_token_lookup",
		"sessionTokenLookup",
		"session_token_hash",
		"sessionTokenHash",
		"csrf_token_hash",
		"csrfTokenHash",
		"key_hash",
		"keyHash",
	} {
		if strings.Contains(text, banned) {
			t.Fatalf("response JSON fixture leaked %q in %s", banned, text)
		}
	}
}

func (f *fakeAuthz) Authorize(ctx context.Context, in *authzv1.AuthzRequest, _ ...grpc.CallOption) (*authzv1.AuthzResponse, error) {
	f.lastCtx = ctx
	f.lastAuthzReq = in
	f.authorizeCalls++
	return &authzv1.AuthzResponse{Decision: f.decision}, nil
}

func (f *fakeAuthz) GetNativeAccess(ctx context.Context, in *authzv1.NativeAccessRequest, _ ...grpc.CallOption) (*authzv1.NativeAccessResponse, error) {
	f.lastCtx = ctx
	f.lastNativeReq = in
	return &authzv1.NativeAccessResponse{Decision: f.decision, Grant: f.nativeGrant}, nil
}

func testMeta() Metadata {
	return Metadata{
		TenantID:        "tenant-1",
		UserID:          "user-1",
		Purpose:         "billing",
		CorrelationID:   "corr-123",
		Scopes:          []string{"read", "write"},
		ServiceIdentity: "svc-a",
		ProjectID:       "proj-9",
	}
}

func newTestAuthClient(fake *fakeAuthz) *AuthClient {
	return &AuthClient{Authz: fake, Meta: testMeta()}
}

func TestCanPopulatesRequestedScopes(t *testing.T) {
	fake := &fakeAuthz{decision: &authzv1.Decision{Allowed: true}}
	c := newTestAuthClient(fake)

	allowed, _, err := c.Can(context.Background(), &authzv1.ResourceRef{Table: "invoices"}, "read", "")
	if err != nil {
		t.Fatalf("Can: %v", err)
	}
	if !allowed {
		t.Fatal("expected allowed")
	}
	if got := fake.lastAuthzReq.GetRequestedScopes(); !equalStrings(got, []string{"read", "write"}) {
		t.Fatalf("AuthzRequest.RequestedScopes = %v, want [read write]", got)
	}
	// Empty purpose falls back to the caller Metadata purpose.
	if got := fake.lastAuthzReq.GetPurpose(); got != "billing" {
		t.Fatalf("AuthzRequest.Purpose = %q, want billing", got)
	}
	if got := fake.lastAuthzReq.GetPrincipal().GetScopes(); !equalStrings(got, []string{"read", "write"}) {
		t.Fatalf("Principal.Scopes = %v, want [read write]", got)
	}
}

func TestNativeAccessPopulatesRequestedScopes(t *testing.T) {
	fake := &fakeAuthz{
		decision:    &authzv1.Decision{Allowed: true},
		nativeGrant: &authzv1.NativeAccessGrant{},
	}
	c := newTestAuthClient(fake)

	_, err := c.NativeAccess(context.Background(), &authzv1.ResourceRef{Table: "ledger"}, "read", "")
	if err != nil {
		t.Fatalf("NativeAccess: %v", err)
	}
	if got := fake.lastNativeReq.GetRequestedScopes(); !equalStrings(got, []string{"read", "write"}) {
		t.Fatalf("NativeAccessRequest.RequestedScopes = %v, want [read write]", got)
	}
}

func TestCanonicalMetadataHeaders(t *testing.T) {
	fake := &fakeAuthz{decision: &authzv1.Decision{Allowed: true}}
	c := newTestAuthClient(fake)
	c.Meta.ClientCatalogVersion = "cat-7"

	if _, _, err := c.Can(context.Background(), &authzv1.ResourceRef{Table: "t"}, "read", "p"); err != nil {
		t.Fatalf("Can: %v", err)
	}

	md, ok := metadata.FromOutgoingContext(fake.lastCtx)
	if !ok {
		t.Fatal("expected outgoing metadata on the authz context")
	}
	want := map[string]string{
		"x-tenant-id":                  "tenant-1",
		"x-user-id":                    "user-1",
		"x-purpose":                    "billing",
		"x-correlation-id":             "corr-123",
		"x-service-identity":           "svc-a",
		"x-udb-project-id":             "proj-9",
		"x-udb-client-catalog-version": "cat-7",
		"x-scopes":                     "read,write",
	}
	for k, v := range want {
		got := md.Get(k)
		if len(got) == 0 || got[0] != v {
			t.Errorf("header %q = %v, want %q", k, got, v)
		}
	}
}

// TestGeneratedClientAPIKeyHeader covers the x-api-key / x-request-id headers,
// which ride on the GeneratedClient robustness layer rather than AuthClient.
func TestGeneratedClientAPIKeyHeader(t *testing.T) {
	g := NewGenerated(nil, Options{
		Meta:          testMeta(),
		APIKey:        "key-abc",
		Authorization: "Bearer jwt-xyz",
	})
	ctx := g.outgoingContext(context.Background())
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	for k, v := range map[string]string{
		"x-api-key":        "key-abc",
		"authorization":    "Bearer jwt-xyz",
		"x-request-id":     "corr-123", // falls back to correlation id
		"x-correlation-id": "corr-123",
	} {
		got := md.Get(k)
		if len(got) == 0 || got[0] != v {
			t.Errorf("header %q = %v, want %q", k, got, v)
		}
	}
}

func TestAuthzCacheTTLHitAndExpiry(t *testing.T) {
	fake := &fakeAuthz{decision: &authzv1.Decision{Allowed: true, CacheTtlSeconds: 60}}
	c := newTestAuthClient(fake)
	cache := NewAuthzCache(c)

	clock := time.Unix(1000, 0)
	cache.now = func() time.Time { return clock }

	res := &authzv1.ResourceRef{Table: "invoices"}

	// First call hits the server.
	if _, _, err := cache.Can(context.Background(), res, "read", "p"); err != nil {
		t.Fatalf("Can #1: %v", err)
	}
	if fake.authorizeCalls != 1 {
		t.Fatalf("after first call, authorizeCalls = %d, want 1", fake.authorizeCalls)
	}

	// Within TTL: served from cache, no new RPC.
	clock = clock.Add(30 * time.Second)
	if _, _, err := cache.Can(context.Background(), res, "read", "p"); err != nil {
		t.Fatalf("Can #2: %v", err)
	}
	if fake.authorizeCalls != 1 {
		t.Fatalf("within TTL, authorizeCalls = %d, want 1 (cache hit expected)", fake.authorizeCalls)
	}

	// Past TTL: cache expires, server is hit again.
	clock = clock.Add(31 * time.Second) // now t=1061 > 1000+60
	if _, _, err := cache.Can(context.Background(), res, "read", "p"); err != nil {
		t.Fatalf("Can #3: %v", err)
	}
	if fake.authorizeCalls != 2 {
		t.Fatalf("past TTL, authorizeCalls = %d, want 2 (cache expiry expected)", fake.authorizeCalls)
	}
}

func TestAuthzCacheZeroTTLNotCached(t *testing.T) {
	fake := &fakeAuthz{decision: &authzv1.Decision{Allowed: true, CacheTtlSeconds: 0}}
	c := newTestAuthClient(fake)
	cache := NewAuthzCache(c)
	clock := time.Unix(1000, 0)
	cache.now = func() time.Time { return clock }

	res := &authzv1.ResourceRef{Table: "t"}
	for i := 0; i < 3; i++ {
		if _, _, err := cache.Can(context.Background(), res, "read", "p"); err != nil {
			t.Fatalf("Can #%d: %v", i, err)
		}
	}
	if fake.authorizeCalls != 3 {
		t.Fatalf("zero TTL must never cache: authorizeCalls = %d, want 3", fake.authorizeCalls)
	}
}

func signBundle(bundle, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(bundle)
	return hex.EncodeToString(mac.Sum(nil))
}

func TestVerifyPolicyBundleAcceptsCorrect(t *testing.T) {
	secret := []byte("shared-secret")
	bundle := []byte(`{"policies":["p1","p2"]}`)
	signed := &authzv1.SignedPolicyBundle{
		Bundle:    bundle,
		Signature: signBundle(bundle, secret),
		Algorithm: "HMAC-SHA256",
	}
	if err := VerifyPolicyBundle(signed, secret); err != nil {
		t.Fatalf("VerifyPolicyBundle on a correctly signed bundle: %v", err)
	}
}

func TestVerifyPolicyBundleRejectsTampered(t *testing.T) {
	secret := []byte("shared-secret")
	bundle := []byte(`{"policies":["p1","p2"]}`)
	signed := &authzv1.SignedPolicyBundle{
		Bundle:    bundle,
		Signature: signBundle(bundle, secret),
		KeyId:     "k1",
		Algorithm: "HMAC-SHA256",
	}
	// Tamper with the payload after signing.
	signed.Bundle = []byte(`{"policies":["p1","p2","p3-injected"]}`)

	err := VerifyPolicyBundle(signed, secret)
	if err == nil {
		t.Fatal("expected a signature mismatch error for a tampered bundle")
	}
	if !errors.Is(err, ErrPolicyBundleSignature) {
		t.Fatalf("error %v should match ErrPolicyBundleSignature", err)
	}
	var sigErr *PolicyBundleSignatureError
	if !errors.As(err, &sigErr) {
		t.Fatalf("error %v should be a *PolicyBundleSignatureError", err)
	}
	if sigErr.KeyID != "k1" {
		t.Errorf("PolicyBundleSignatureError.KeyID = %q, want k1", sigErr.KeyID)
	}
}

func TestVerifyPolicyBundleRejectsWrongSecret(t *testing.T) {
	bundle := []byte("payload")
	signed := &authzv1.SignedPolicyBundle{
		Bundle:    bundle,
		Signature: signBundle(bundle, []byte("real-secret")),
	}
	if err := VerifyPolicyBundle(signed, []byte("attacker-secret")); !errors.Is(err, ErrPolicyBundleSignature) {
		t.Fatalf("wrong secret should mismatch, got %v", err)
	}
}

// ── framework adapter (M5.6) tests ───────────────────────────────────────────

func TestHTTPMiddlewareExtractsContext(t *testing.T) {
	var seen Metadata
	var found bool
	h := HTTPMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen, found = FromContext(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("x-tenant-id", "tenant-1")
	req.Header.Set("x-user-id", "user-1")
	req.Header.Set("x-correlation-id", "corr-123")
	req.Header.Set("x-udb-project-id", "proj-9")
	req.Header.Set("x-purpose", "billing")
	req.Header.Set("x-scopes", "read,write")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if !found {
		t.Fatal("expected Metadata in request context")
	}
	if seen.TenantID != "tenant-1" || seen.UserID != "user-1" || seen.ProjectID != "proj-9" {
		t.Fatalf("unexpected metadata: %+v", seen)
	}
	if seen.CorrelationID != "corr-123" {
		t.Fatalf("CorrelationID = %q, want corr-123", seen.CorrelationID)
	}
	if !equalStrings(seen.Scopes, []string{"read", "write"}) {
		t.Fatalf("Scopes = %v, want [read write]", seen.Scopes)
	}
	if got := CorrelationID(req.Context()); got != "" {
		t.Fatalf("original request ctx should not carry metadata, got %q", got)
	}
}

func TestHTTPMiddlewareRequestIDFallback(t *testing.T) {
	var seen Metadata
	h := HTTPMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = MetadataFromContext(r.Context())
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("x-request-id", "req-42") // no correlation id present
	h.ServeHTTP(httptest.NewRecorder(), req)
	if seen.CorrelationID != "req-42" {
		t.Fatalf("CorrelationID should fall back to x-request-id, got %q", seen.CorrelationID)
	}
}

func TestUnaryServerInterceptorExtractsContext(t *testing.T) {
	md := metadata.Pairs(
		"x-tenant-id", "tenant-1",
		"x-user-id", "user-1",
		"x-scopes", "read,write",
	)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	var seen Metadata
	handler := func(c context.Context, _ any) (any, error) {
		seen = MetadataFromContext(c)
		return nil, nil
	}
	if _, err := UnaryServerInterceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler); err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	if seen.TenantID != "tenant-1" || seen.UserID != "user-1" {
		t.Fatalf("unexpected metadata: %+v", seen)
	}
	if !equalStrings(seen.Scopes, []string{"read", "write"}) {
		t.Fatalf("Scopes = %v, want [read write]", seen.Scopes)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	ac := append([]string(nil), a...)
	bc := append([]string(nil), b...)
	sort.Strings(ac)
	sort.Strings(bc)
	for i := range ac {
		if ac[i] != bc[i] {
			return false
		}
	}
	return true
}
