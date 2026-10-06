package udbclient

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// keyAuthn issues a short-lived bearer per Authenticate{api_key} call and
// records what every call carried.
type keyAuthn struct {
	authnv1.UnimplementedAuthnServiceServer
	mu        sync.Mutex
	exchanges int
	sawRawKey bool
	fail      bool
	ttl       time.Duration
}

func (a *keyAuthn) Authenticate(ctx context.Context, req *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get("x-api-key")) > 0 {
		a.sawRawKey = true
	}
	if a.fail {
		return nil, errors.New("key revoked")
	}
	if req.GetApiKey() != "svc-key" {
		return nil, errors.New("unknown key")
	}
	a.exchanges++
	return &authnv1.AuthnResponse{
		AccessToken:   "bearer-" + time.Now().Format("150405.000000"),
		ExpiresAtUnix: time.Now().Add(a.ttl).Unix(),
	}, nil
}

func (a *keyAuthn) snapshot() (int, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.exchanges, a.sawRawKey
}

type mdBroker struct {
	servicesv1.UnimplementedDataBrokerServer
	mu sync.Mutex
	md metadata.MD
}

func (b *mdBroker) Select(ctx context.Context, _ *entityv1.SelectRequest) (*entityv1.RecordSet, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	b.mu.Lock()
	b.md = md.Copy()
	b.mu.Unlock()
	return &entityv1.RecordSet{}, nil
}

func serveKeyFakes(t *testing.T, authn *keyAuthn, broker *mdBroker) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	authnv1.RegisterAuthnServiceServer(srv, authn)
	servicesv1.RegisterDataBrokerServer(srv, broker)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// An API key is exchanged at connect, never sent raw, carried as a bearer on
// data calls, and re-exchanged before the bearer expires.
func TestConnectExchangesAPIKeyAndKeepsTheBearerFresh(t *testing.T) {
	authn := &keyAuthn{ttl: 2 * time.Second}
	broker := &mdBroker{}
	target := serveKeyFakes(t, authn, broker)

	u, err := Connect(context.Background(), Config{
		Target:      target,
		TenantID:    "00000000-0000-0000-0000-000000000001",
		Purpose:     "test",
		Credentials: Credentials{APIKey: "svc-key"},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer u.Close()

	if n, raw := authn.snapshot(); n != 1 || raw {
		t.Fatalf("after connect: exchanges=%d sawRawKey=%v, want 1/false", n, raw)
	}
	if u.BearerExpiresAt().IsZero() {
		t.Fatal("BearerExpiresAt is zero after an exchange")
	}

	if _, err := u.Data.Broker.Select(context.Background(), &entityv1.SelectRequest{MessageType: "x"}); err != nil {
		t.Fatalf("select: %v", err)
	}
	broker.mu.Lock()
	md := broker.md
	broker.mu.Unlock()
	if got := md.Get("authorization"); len(got) != 1 || got[0] == "" || got[0][:7] != "Bearer " {
		t.Fatalf("data call authorization = %v, want one exchanged bearer", got)
	}
	if got := md.Get("x-api-key"); len(got) != 0 {
		t.Fatalf("data call carried the raw key: %v", got)
	}

	// 4/5 of a 2s life (floored at 1s) → a re-exchange within ~2s.
	deadline := time.Now().Add(4 * time.Second)
	for {
		if n, _ := authn.snapshot(); n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the bearer was not re-exchanged before it expired")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := u.CredentialErr(); err != nil {
		t.Fatalf("CredentialErr after a good refresh: %v", err)
	}
}

// A refresh that keeps failing is reported by CredentialErr instead of
// silently leaving a dead bearer in place.
func TestAPIKeyRefreshFailureIsReported(t *testing.T) {
	authn := &keyAuthn{ttl: 1 * time.Second}
	target := serveKeyFakes(t, authn, &mdBroker{})

	u, err := Connect(context.Background(), Config{
		Target:      target,
		TenantID:    "00000000-0000-0000-0000-000000000001",
		Deadline:    200 * time.Millisecond, // retry cadence after a failed exchange
		Credentials: Credentials{APIKey: "svc-key"},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer u.Close()

	authn.mu.Lock()
	authn.fail = true
	authn.mu.Unlock()

	deadline := time.Now().Add(4 * time.Second)
	for u.CredentialErr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("a failing re-exchange was never reported by CredentialErr")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A key the broker will not exchange fails Connect with a clear error.
func TestConnectFailsWhenTheKeyCannotBeExchanged(t *testing.T) {
	target := serveKeyFakes(t, &keyAuthn{ttl: time.Minute}, &mdBroker{})
	_, err := Connect(context.Background(), Config{
		Target:      target,
		Credentials: Credentials{APIKey: "wrong-key"},
	})
	if err == nil {
		t.Fatal("connect with an unexchangeable key must fail")
	}
}

// RawAPIKey keeps the legacy header mode for callers that need it until 0.6.0.
func TestRawAPIKeyModeSendsTheHeaderAndDoesNotExchange(t *testing.T) {
	authn := &keyAuthn{ttl: time.Minute}
	broker := &mdBroker{}
	target := serveKeyFakes(t, authn, broker)
	u, err := Connect(context.Background(), Config{
		Target:      target,
		Credentials: Credentials{APIKey: "svc-key", RawAPIKey: true},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer u.Close()
	if n, _ := authn.snapshot(); n != 0 {
		t.Fatalf("raw mode exchanged the key %d times", n)
	}
	if _, err := u.Data.Broker.Select(context.Background(), &entityv1.SelectRequest{MessageType: "x"}); err != nil {
		t.Fatalf("select: %v", err)
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if got := broker.md.Get("x-api-key"); len(got) != 1 || got[0] != "svc-key" {
		t.Fatalf("raw mode x-api-key = %v", got)
	}
}

// Every missing variable is reported in one error.
func TestConfigFromEnvReportsEveryProblemAtOnce(t *testing.T) {
	for _, name := range []string{"TARGET", "TENANT_ID", "PURPOSE", "API_KEY", "BEARER", "DEADLINE"} {
		t.Setenv("SVC_"+name, "")
	}
	t.Setenv("SVC_DEADLINE", "soon")
	_, err := ConfigFromEnv("SVC_")
	if err == nil {
		t.Fatal("an empty environment must fail")
	}
	for _, want := range []string{"SVC_TARGET", "SVC_TENANT_ID", "SVC_PURPOSE", "SVC_API_KEY", "SVC_DEADLINE"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}

	t.Setenv("SVC_TARGET", "127.0.0.1:50051")
	t.Setenv("SVC_TENANT_ID", "00000000-0000-0000-0000-000000000001")
	t.Setenv("SVC_PURPOSE", "notes")
	t.Setenv("SVC_API_KEY", "svc-key")
	t.Setenv("SVC_DEADLINE", "10s")
	t.Setenv("SVC_SCOPES", "udb:read, udb:write udb:read")
	t.Setenv("SVC_PROJECT", "smart-notes")
	cfg, err := ConfigFromEnv("SVC_")
	if err != nil {
		t.Fatalf("complete environment: %v", err)
	}
	if cfg.Deadline != 10*time.Second || cfg.ProjectID != "smart-notes" || len(cfg.Scopes) != 2 {
		t.Fatalf("cfg = %+v", cfg)
	}
}

// principalAuthn returns a principal with the exchanged bearer.
type principalAuthn struct {
	keyAuthn
}

func (a *principalAuthn) Authenticate(ctx context.Context, req *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	res, err := a.keyAuthn.Authenticate(ctx, req)
	if err != nil {
		return nil, err
	}
	res.Principal = &authnv1.Principal{
		TenantId:        "00000000-0000-0000-0000-000000000001",
		ProjectId:       "smart-notes",
		ServiceIdentity: "svc_notes",
		Scopes:          []string{"udb:read", "udb:write"},
	}
	return res, nil
}

// Verify names every mismatch between the expected and the verified principal.
func TestVerifyReportsEveryMismatch(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	authnv1.RegisterAuthnServiceServer(srv, &principalAuthn{keyAuthn{ttl: time.Minute}})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	u, err := Connect(context.Background(), Config{Target: lis.Addr().String(), Credentials: Credentials{APIKey: "svc-key"}})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer u.Close()

	if err := u.Verify(Expect{
		TenantID:        "00000000-0000-0000-0000-000000000001",
		ServiceIdentity: "svc_notes",
		RequiredScopes:  []string{"udb:read"},
	}); err != nil {
		t.Fatalf("a matching principal must verify: %v", err)
	}
	err = u.Verify(Expect{
		TenantID:        "00000000-0000-0000-0000-000000000002",
		ServiceIdentity: "svc_billing",
		RequiredScopes:  []string{"udb:read", "udb:pii:read"},
	})
	if err == nil {
		t.Fatal("a mismatching principal must fail Verify")
	}
	for _, want := range []string{"tenant", "service identity", "udb:pii:read"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Verify error %q does not mention %s", err, want)
		}
	}
}

// reloginAuthn refuses every refresh (a revoked refresh token) and counts logins.
type reloginAuthn struct {
	authnv1.UnimplementedAuthnServiceServer
	mu     sync.Mutex
	logins int
}

func (a *reloginAuthn) Login(context.Context, *authnv1.LoginRequest) (*authnv1.LoginResponse, error) {
	a.mu.Lock()
	a.logins++
	n := a.logins
	a.mu.Unlock()
	return &authnv1.LoginResponse{
		AccessToken:          "access-" + time.Now().Format("150405.000000") + "-" + string(rune('a'+n)),
		RefreshToken:         "refresh",
		SessionId:            "sess",
		AccessTokenExpiresIn: 1,
	}, nil
}

func (a *reloginAuthn) Authenticate(context.Context, *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	return &authnv1.AuthnResponse{Principal: &authnv1.Principal{
		TenantId: "00000000-0000-0000-0000-000000000001", UserId: "svc",
	}}, nil
}

func (a *reloginAuthn) RefreshToken(context.Context, *authnv1.RefreshTokenRequest) (*authnv1.RefreshTokenResponse, error) {
	return nil, errors.New("refresh token revoked")
}

// A session whose refresh token stops working logs in again instead of
// holding a bearer it can no longer renew.
func TestEnterpriseSessionLogsInAgainWhenRefreshIsRefused(t *testing.T) {
	authn := &reloginAuthn{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	authnv1.RegisterAuthnServiceServer(srv, authn)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	sess, err := ConnectEnterprise(context.Background(), EnterpriseConfig{
		Target: lis.Addr().String(), Username: "svc", Password: "pw", TenantCode: "acme",
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer sess.Close()
	first := sess.Bearer()

	deadline := time.Now().Add(6 * time.Second)
	for {
		authn.mu.Lock()
		logins := authn.logins
		authn.mu.Unlock()
		if logins >= 2 && sess.Bearer() != first && sess.RefreshErr() == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no re-login after a refused refresh: logins=%d bearerChanged=%v refreshErr=%v",
				logins, sess.Bearer() != first, sess.RefreshErr())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// One connect per tenant, shared by concurrent callers, and a refusal for a
// tenant the pool has no key for.
func TestTenantSessionPoolSharesOneClientPerTenant(t *testing.T) {
	authn := &principalAuthn{keyAuthn{ttl: time.Minute}}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	authnv1.RegisterAuthnServiceServer(srv, authn)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	tenant := "00000000-0000-0000-0000-000000000001"
	pool := NewAPIKeyTenantPool(Config{Target: lis.Addr().String()}, map[string]string{tenant: "svc-key"})
	defer pool.Close()

	var wg sync.WaitGroup
	clients := make([]*Udb, 8)
	for i := range clients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			u, err := pool.Get(context.Background(), tenant)
			if err != nil {
				t.Errorf("get: %v", err)
				return
			}
			clients[i] = u
		}(i)
	}
	wg.Wait()
	for _, u := range clients[1:] {
		if u != clients[0] {
			t.Fatal("concurrent Gets for one tenant returned different clients")
		}
	}
	if n, _ := authn.snapshot(); n != 1 {
		t.Fatalf("the tenant's key was exchanged %d times, want 1", n)
	}
	if _, err := pool.Get(context.Background(), "00000000-0000-0000-0000-000000000009"); err == nil {
		t.Fatal("a tenant without a key must be refused")
	}
	if got := pool.Tenants(); len(got) != 1 || got[0] != tenant {
		t.Fatalf("Tenants() = %v", got)
	}
}
