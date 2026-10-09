package udbclient

import (
	"context"
	"errors"
	"net"
	"reflect"
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
		Principal: &authnv1.Principal{
			TenantId: "00000000-0000-0000-0000-000000000001",
		},
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

// Unsupported raw keys must fail before either auth or business transport.
func TestRawAPIKeyModeRefusesBeforeTransport(t *testing.T) {
	authn := &keyAuthn{ttl: time.Minute}
	broker := &mdBroker{}
	target := serveKeyFakes(t, authn, broker)
	u, err := Connect(context.Background(), Config{
		Target:      target,
		Credentials: Credentials{APIKey: "svc-key", RawAPIKey: true},
	})
	if u != nil {
		defer u.Close()
		t.Fatal("unsupported raw API-key mode returned a client")
	}
	if err == nil || !strings.Contains(err.Error(), "Credentials.RawAPIKey is unsupported") {
		t.Fatal("raw API-key mode must return its named refusal")
	}
	if n, raw := authn.snapshot(); n != 0 || raw {
		t.Fatal("unsupported raw API-key mode reached authentication transport")
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if len(broker.md) != 0 {
		t.Fatal("unsupported raw API-key mode reached business transport")
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

func TestAPIKeyConnectAdoptsVerifiedIdentityOnEveryFacade(t *testing.T) {
	authn := &sessionIdentityAuthn{principal: sessionTestPrincipal()}
	broker := &mdBroker{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u, err := Connect(ctx, Config{
		Target: serveSessionAuthn(t, authn, broker), TenantID: "tenant-hint", ProjectID: "project-hint",
		UserID: "user-hint", ServiceIdentity: "service-hint", Scopes: []string{"ungranted-hint"},
		Purpose: "request-purpose", CorrelationID: "request-correlation", Credentials: Credentials{APIKey: "fixture-key"},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer u.Close()
	want := Metadata{
		TenantID: v232Tenant, ProjectID: v232Project, UserID: v232User,
		ServiceIdentity: "session-fixture", Scopes: []string{"data:read", "data:write"},
		Purpose: "request-purpose", CorrelationID: "request-correlation",
	}
	for name, got := range map[string]Metadata{
		"project": u.Meta, "generated": u.Generated.Meta(), "data": u.Data.Meta, "auth": u.Auth.Meta,
		"api key": u.ApiKey.meta, "tenant": u.Tenant.meta, "notification": u.Notification.meta,
		"storage": u.Storage.meta, "asset": u.Asset.meta,
	} {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s retained caller identity instead of the verified principal", name)
		}
	}
	beforeData, beforeAuth := u.Data, u.Auth
	if _, err := u.exchangeAPIKey(ctx, u.apiKey); err != nil {
		t.Fatalf("same-identity renewal: %v", err)
	}
	if u.Data != beforeData || u.Auth != beforeAuth {
		t.Fatal("routine renewal rebuilt unchanged facade handles")
	}
	for _, direct := range []bool{true, false} {
		if direct {
			_, err = u.Data.Broker.Select(ctx, &entityv1.SelectRequest{MessageType: "fixture"})
		} else {
			_, err = u.Data.Select(ctx, &entityv1.SelectRequest{MessageType: "fixture"})
		}
		if err != nil {
			t.Fatalf("canonical data call: %v", err)
		}
		broker.mu.Lock()
		md := broker.md.Copy()
		broker.mu.Unlock()
		for key, expected := range map[string]string{
			"x-tenant-id": want.TenantID, "x-udb-project-id": want.ProjectID, "x-user-id": want.UserID,
			"x-service-identity": want.ServiceIdentity, "x-scopes": "data:read,data:write",
		} {
			if got := md.Get(key); len(got) != 1 || got[0] != expected {
				t.Errorf("%s must carry one canonical principal value", key)
			}
		}
		if len(md.Get("x-api-key")) != 0 || len(md.Get("authorization")) != 1 {
			t.Fatal("canonical call must carry one exchanged bearer and no raw key")
		}
	}
}

func TestAPIKeyRenewalRejectsChangedIdentityBeforeInstallation(t *testing.T) {
	for _, field := range []string{"project", "scopes"} {
		t.Run(field, func(t *testing.T) {
			authn := &sessionIdentityAuthn{principal: sessionTestPrincipal()}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			u, err := Connect(ctx, Config{
				Target: serveSessionAuthn(t, authn, nil), Credentials: Credentials{APIKey: "fixture-key"},
			})
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer u.Close()
			beforeMeta, beforeOptions := u.Meta, u.Generated.options()
			beforeData, beforeAuth := u.Data, u.Auth
			beforePrincipal := u.Principal()
			beforeExpiry := u.BearerExpiresAt()
			authn.mu.Lock()
			if field == "project" {
				authn.principal.ProjectId = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
			} else {
				authn.principal.Scopes = []string{"data:read"}
			}
			authn.mu.Unlock()
			if _, err := u.exchangeAPIKey(ctx, u.apiKey); err == nil || !strings.Contains(err.Error(), field) {
				t.Fatal("renewal must be rejected with a named identity error")
			}
			if !reflect.DeepEqual(u.Meta, beforeMeta) || !reflect.DeepEqual(u.Generated.options(), beforeOptions) ||
				!reflect.DeepEqual(u.Principal(), beforePrincipal) || u.BearerExpiresAt() != beforeExpiry ||
				u.Data != beforeData || u.Auth != beforeAuth {
				t.Fatal("rejected key renewal replaced a connected identity, credential, expiry or facade")
			}
		})
	}
}

func TestAPIKeyRenewalKeepsFacadesForReorderedScopes(t *testing.T) {
	authn := &sessionIdentityAuthn{principal: sessionTestPrincipal()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u, err := Connect(ctx, Config{
		Target: serveSessionAuthn(t, authn, &mdBroker{}), Credentials: Credentials{APIKey: "fixture-key"},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer u.Close()
	exerciseStableFacadesDuringRenewal(t, u, func(ctx context.Context, i int) error {
		authn.mu.Lock()
		if i%2 == 0 {
			authn.principal.Scopes = []string{"data:write", "data:read", "data:read"}
		} else {
			authn.principal.Scopes = []string{"data:read", "data:write"}
		}
		authn.mu.Unlock()
		_, err := u.exchangeAPIKey(ctx, u.apiKey)
		return err
	})
}

func TestAPIKeyRenewalTreatsNilAndEmptyScopesAsSameSet(t *testing.T) {
	principal := sessionTestPrincipal()
	principal.Scopes = nil
	authn := &sessionIdentityAuthn{principal: principal}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u, err := Connect(ctx, Config{
		Target: serveSessionAuthn(t, authn, nil), Credentials: Credentials{APIKey: "fixture-key"},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer u.Close()
	beforeMeta, beforeData, beforeAuth := u.Meta, u.Data, u.Auth
	authn.mu.Lock()
	authn.principal.Scopes = []string{}
	authn.mu.Unlock()
	if _, err := u.exchangeAPIKey(ctx, u.apiKey); err != nil {
		t.Fatalf("empty scope-set renewal: %v", err)
	}
	if !reflect.DeepEqual(u.Meta, beforeMeta) || u.Data != beforeData || u.Auth != beforeAuth {
		t.Fatal("empty-set renewal wrote public metadata or facades")
	}
}

func TestAPIKeyRefreshWaitPreservesHealthyShortLifetime(t *testing.T) {
	for _, tc := range []struct {
		remaining time.Duration
		want      time.Duration
	}{
		{time.Hour, 48 * time.Minute},
		{time.Second, 800 * time.Millisecond},
		{250 * time.Millisecond, 200 * time.Millisecond},
		{time.Nanosecond, time.Nanosecond},
		{0, apiKeyRefreshFloor},
		{-time.Second, apiKeyRefreshFloor},
	} {
		if got := apiKeyRefreshWait(tc.remaining); got != tc.want {
			t.Errorf("remaining=%v renewal wait=%v, want %v", tc.remaining, got, tc.want)
		}
	}
}

func TestAPIKeyRenewalRejectsChangedCurrentAdoption(t *testing.T) {
	for _, field := range []string{"project", "scopes"} {
		t.Run(field, func(t *testing.T) {
			authn := &sessionIdentityAuthn{principal: sessionTestPrincipal()}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			u, err := Connect(ctx, Config{
				Target: serveSessionAuthn(t, authn, nil), Credentials: Credentials{APIKey: "fixture-key"},
			})
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer u.Close()
			authn.mu.Lock()
			if field == "project" {
				authn.principal.ProjectId = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
			} else {
				authn.principal.Scopes = []string{"data:read"}
			}
			authn.mu.Unlock()
			if _, err := u.LoginAndAdoptTenant(ctx, &authnv1.LoginRequest{Username: "fixture", Password: "fixture"}); err != nil {
				t.Fatalf("explicit adoption: %v", err)
			}
			beforeMeta, beforeOptions := u.Meta, u.Generated.options()
			beforeData, beforeAuth := u.Data, u.Auth
			beforePrincipal, beforeExpiry := u.Principal(), u.BearerExpiresAt()
			authn.mu.Lock()
			authn.principal = sessionTestPrincipal()
			authn.mu.Unlock()
			if _, err := u.exchangeAPIKey(ctx, u.apiKey); err == nil || !strings.Contains(err.Error(), field) {
				t.Fatal("renewal must validate the current adoption as well as the pinned identity")
			}
			if !reflect.DeepEqual(u.Meta, beforeMeta) || !reflect.DeepEqual(u.Generated.options(), beforeOptions) ||
				!reflect.DeepEqual(u.Principal(), beforePrincipal) || u.BearerExpiresAt() != beforeExpiry ||
				u.Data != beforeData || u.Auth != beforeAuth {
				t.Fatal("rejected current-identity renewal replaced installed state")
			}
		})
	}
}

func TestAPIKeyConnectRequiresVerifiedPrincipal(t *testing.T) {
	authn := &sessionIdentityAuthn{omitPrincipal: true}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if u, err := Connect(ctx, Config{
		Target: serveSessionAuthn(t, authn, nil), Credentials: Credentials{APIKey: "fixture-key"},
	}); err == nil {
		_ = u.Close()
		t.Fatal("exchange without a verified principal must not return a connected client")
	} else if !strings.Contains(err.Error(), "verified principal") || strings.Contains(err.Error(), "fixture-key") {
		t.Fatal("missing-principal error must identify the contract without exposing the key")
	}
}
