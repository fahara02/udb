package udbclient

// V23-2 served-path regression: ConnectEnterprise must carry EXACTLY ONE value
// per identity/audit header on both the data plane (DataContext) and the control
// plane (NativeContext), and it must be the CANONICAL post-adoption value — never
// a duplicate that also carries the stale pre-login tenant/project hint.
//
// Before the fix, u.Generated was a second GeneratedClient distinct from the one
// whose interceptors were on the connections, so the connection interceptor kept
// appending the pre-login hint while DataContext/NativeContext appended the
// canonical value — producing conflicting repeated x-tenant-id / x-udb-project-id
// headers. The fix (a) shares the one interceptor-owning GeneratedClient as
// u.Generated so adoption/refresh update it, and (b) makes the interceptor
// idempotent so it never re-appends a header the facade already set.
//
// This uses two real ephemeral gRPC listeners (separate data + auth targets, per
// acceptance criterion 5) with insecure transport, since ConnectEnterprise dials
// through its own newGrpcClient and TLS==nil selects insecure credentials.

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	// Canonical identity returned by the verified principal (differs from hints).
	v232Tenant  = "11111111-1111-4111-8111-111111111111"
	v232Project = "22222222-2222-4222-8222-222222222222"
	v232User    = "33333333-3333-4333-8333-333333333333"
	// Pre-login human hints — these must NOT survive on any served header.
	v232HintTenant  = "acme-tenant-hint"
	v232HintProject = "acme-project-hint"
)

// v232MDCapture records the incoming metadata of the last served RPC.
type v232MDCapture struct {
	mu sync.Mutex
	md metadata.MD
}

func (c *v232MDCapture) set(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	c.mu.Lock()
	c.md = md.Copy()
	c.mu.Unlock()
}

func (c *v232MDCapture) get() metadata.MD {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.md
}

type v232Broker struct {
	servicesv1.UnimplementedDataBrokerServer
	cap *v232MDCapture
}

func (b *v232Broker) Select(ctx context.Context, _ *entityv1.SelectRequest) (*entityv1.RecordSet, error) {
	b.cap.set(ctx)
	return &entityv1.RecordSet{}, nil
}

type v232Authn struct {
	authnv1.UnimplementedAuthnServiceServer
	cap         *v232MDCapture
	mu          sync.Mutex
	refreshHits int
}

func (a *v232Authn) Login(context.Context, *authnv1.LoginRequest) (*authnv1.LoginResponse, error) {
	return &authnv1.LoginResponse{
		AccessToken:          "token1",
		RefreshToken:         "refresh1",
		SessionId:            "sess1",
		AccessTokenExpiresIn: 3600,
	}, nil
}

func (a *v232Authn) Authenticate(ctx context.Context, _ *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	// Captures NativeContext metadata when we probe post-adoption; the pre-adopt
	// call made during login is overwritten by that later probe.
	a.cap.set(ctx)
	return &authnv1.AuthnResponse{
		Principal: &authnv1.Principal{
			TenantId:  v232Tenant,
			ProjectId: v232Project,
			UserId:    v232User,
		},
	}, nil
}

func (a *v232Authn) RefreshToken(context.Context, *authnv1.RefreshTokenRequest) (*authnv1.RefreshTokenResponse, error) {
	a.mu.Lock()
	a.refreshHits++
	a.mu.Unlock()
	return &authnv1.RefreshTokenResponse{
		AccessToken:          "token2",
		RefreshToken:         "refresh2",
		AccessTokenExpiresIn: 3600,
	}, nil
}

// v232AssertCanonicalSingletons enforces acceptance criterion 3: every listed
// header appears at most once, tenant/project are exactly the canonical UUIDs,
// authorization is the single expected bearer, and no pre-login hint leaks.
func v232AssertCanonicalSingletons(t *testing.T, label string, md metadata.MD, wantBearer string) {
	t.Helper()
	for _, h := range []string{
		"x-tenant-id", "x-udb-project-id", "x-user-id", "x-purpose",
		"x-scopes", "x-service-identity", "x-correlation-id", "x-request-id",
		"authorization",
	} {
		if got := md.Get(h); len(got) > 1 {
			t.Errorf("%s: header %q must be a singleton, got %d values: %v", label, h, len(got), got)
		}
	}
	if got := md.Get("x-tenant-id"); len(got) != 1 || got[0] != v232Tenant {
		t.Errorf("%s: x-tenant-id = %v, want [%s] (canonical, no pre-login hint)", label, got, v232Tenant)
	}
	if got := md.Get("x-udb-project-id"); len(got) != 1 || got[0] != v232Project {
		t.Errorf("%s: x-udb-project-id = %v, want [%s]", label, got, v232Project)
	}
	if got := md.Get("authorization"); len(got) != 1 || got[0] != wantBearer {
		t.Errorf("%s: authorization = %v, want [%s]", label, got, wantBearer)
	}
	for h, vals := range md {
		for _, v := range vals {
			if v == v232HintTenant || v == v232HintProject {
				t.Errorf("%s: stale pre-login hint %q leaked in header %q", label, v, h)
			}
		}
	}
}

func TestConnectEnterpriseEmitsCanonicalSingletonMetadata(t *testing.T) {
	dataCap, authCap := &v232MDCapture{}, &v232MDCapture{}

	dataLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen data: %v", err)
	}
	authLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen auth: %v", err)
	}
	dataSrv, authSrv := grpc.NewServer(), grpc.NewServer()
	servicesv1.RegisterDataBrokerServer(dataSrv, &v232Broker{cap: dataCap})
	authFake := &v232Authn{cap: authCap}
	authnv1.RegisterAuthnServiceServer(authSrv, authFake)
	go func() { _ = dataSrv.Serve(dataLis) }()
	go func() { _ = authSrv.Serve(authLis) }()
	defer dataSrv.Stop()
	defer authSrv.Stop()

	ctx := context.Background()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{
		Target:     dataLis.Addr().String(),
		AuthTarget: authLis.Addr().String(),
		Username:   "u",
		Password:   "p",
		TenantCode: v232HintTenant,   // pre-login hint — replaced by the canonical UUID
		ProjectID:  v232HintProject,  // pre-login hint
		Purpose:    "v232-unit",
		Scopes:     []string{"udb:read", "udb:write"},
	})
	if err != nil {
		t.Fatalf("ConnectEnterprise: %v", err)
	}
	defer sess.Close()

	if sess.CanonicalTenantID != v232Tenant || sess.CanonicalProjectID != v232Project {
		t.Fatalf("adopted canonical = (%s, %s), want (%s, %s)",
			sess.CanonicalTenantID, sess.CanonicalProjectID, v232Tenant, v232Project)
	}

	// ── Data plane (DataContext) ──────────────────────────────────────────────
	if _, err := sess.Data.Broker.Select(sess.DataContext(ctx), &entityv1.SelectRequest{MessageType: "acme.v1.Thing"}); err != nil {
		t.Fatalf("data Select: %v", err)
	}
	v232AssertCanonicalSingletons(t, "DataContext", dataCap.get(), "Bearer token1")

	// ── Control plane (NativeContext), on the SEPARATE auth target ─────────────
	if _, err := sess.Auth.Authn.Authenticate(sess.NativeContext(ctx), &authnv1.AuthnRequest{BearerToken: "probe"}); err != nil {
		t.Fatalf("native Authenticate: %v", err)
	}
	v232AssertCanonicalSingletons(t, "NativeContext", authCap.get(), "Bearer token1")

	// ── Raw data call (no facade context): ONLY the connection interceptor
	// supplies metadata. It must now carry the CANONICAL tenant/project because
	// the interceptor's GeneratedClient is the shared, adopted one (Fix A) — not
	// a divergent client still holding the pre-login hint. ────────────────────────
	if _, err := sess.Data.Broker.Select(ctx, &entityv1.SelectRequest{MessageType: "acme.v1.Thing"}); err != nil {
		t.Fatalf("raw data Select: %v", err)
	}
	raw := dataCap.get()
	if got := raw.Get("x-tenant-id"); len(got) != 1 || got[0] != v232Tenant {
		t.Errorf("raw call: interceptor x-tenant-id = %v, want [%s] (shared adopted GeneratedClient)", got, v232Tenant)
	}
	if got := raw.Get("x-udb-project-id"); len(got) != 1 || got[0] != v232Project {
		t.Errorf("raw call: interceptor x-udb-project-id = %v, want [%s]", got, v232Project)
	}

	// ── Force a bearer refresh (criterion 4): expire the stored token so the next
	// DataContext resolves a fresh bearer; the initial bearer must not remain and
	// authorization must stay a single current value. ────────────────────────────
	_ = sess.tm.store.Save(ctx, Token{
		AccessToken:  "token1",
		RefreshToken: "refresh1",
		SessionID:    "sess1",
		ExpiresAt:    time.Now().Add(-time.Hour),
	})
	if _, err := sess.Data.Broker.Select(sess.DataContext(ctx), &entityv1.SelectRequest{MessageType: "acme.v1.Thing"}); err != nil {
		t.Fatalf("data Select after refresh: %v", err)
	}
	refreshed := dataCap.get()
	if got := refreshed.Get("authorization"); len(got) != 1 || got[0] != "Bearer token2" {
		t.Errorf("after refresh: authorization = %v, want [Bearer token2] (single current bearer, initial gone)", got)
	}
	v232AssertCanonicalSingletons(t, "DataContext(after refresh)", refreshed, "Bearer token2")

	authFake.mu.Lock()
	hits := authFake.refreshHits
	authFake.mu.Unlock()
	if hits == 0 {
		t.Error("expected at least one RefreshToken RPC to be served")
	}
}
