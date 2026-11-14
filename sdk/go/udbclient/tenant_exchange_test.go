package udbclient

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	cachev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/cache/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	exchangeHomeTenant    = "11111111-1111-4111-8111-111111111111"
	exchangeMemberTenant  = "22222222-2222-4222-8222-222222222222"
	exchangeMemberProject = "33333333-3333-4333-8333-333333333333"
	exchangeServiceUser   = "44444444-4444-4444-8444-444444444444"
)

// tenantExchangeBroker issues a home bearer for an API key or a password login,
// mints short-lived member-tenant tokens for that home bearer, and serves a
// CacheService.Increment that accepts only the newest member token.
type tenantExchangeBroker struct {
	authnv1.UnimplementedAuthnServiceServer
	cachev1.UnimplementedCacheServiceServer

	mu             sync.Mutex
	ttl            time.Duration
	homeBearer     string
	exchanges      int
	memberToken    string // the only member token Increment accepts
	exchangeMD     []metadata.MD
	incrementMD    []metadata.MD
	refused        int
	refuseExchange bool
	wrongTenant    string // when set, the exchange answers for this tenant
}

func (b *tenantExchangeBroker) principal(tenant, project string) *authnv1.Principal {
	return &authnv1.Principal{PrincipalId: "svc", Subject: "svc", TenantId: tenant, ProjectId: project, UserId: exchangeServiceUser}
}

func (b *tenantExchangeBroker) Login(context.Context, *authnv1.LoginRequest) (*authnv1.LoginResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.homeBearer = "home-login"
	return &authnv1.LoginResponse{AccessToken: b.homeBearer, RefreshToken: "home-refresh", SessionId: "home-session", AccessTokenExpiresIn: 3600}, nil
}

func (b *tenantExchangeBroker) Authenticate(_ context.Context, req *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case req.GetApiKey() == "svc-key":
		b.homeBearer = "home-key"
		return &authnv1.AuthnResponse{AccessToken: b.homeBearer, ExpiresAtUnix: time.Now().Add(time.Hour).Unix(), Principal: b.principal(exchangeHomeTenant, "")}, nil
	case req.GetBearerToken() != "" && req.GetBearerToken() == b.homeBearer:
		return &authnv1.AuthnResponse{ExpiresAtUnix: time.Now().Add(time.Hour).Unix(), Principal: b.principal(exchangeHomeTenant, "")}, nil
	case strings.HasPrefix(req.GetBearerToken(), "member-"):
		return &authnv1.AuthnResponse{ExpiresAtUnix: time.Now().Add(b.ttl).Unix(), Principal: b.principal(exchangeMemberTenant, exchangeMemberProject)}, nil
	}
	return nil, status.Error(codes.Unauthenticated, "unknown credential")
}

func (b *tenantExchangeBroker) ExchangeServiceTokenForTenant(ctx context.Context, req *authnv1.ExchangeServiceTokenForTenantRequest) (*authnv1.ExchangeServiceTokenForTenantResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.exchangeMD = append(b.exchangeMD, md.Copy())
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer "+b.homeBearer {
		return nil, status.Error(codes.Unauthenticated, "the exchange requires the home bearer")
	}
	if b.refuseExchange {
		return nil, status.Error(codes.PermissionDenied, "no active membership")
	}
	b.exchanges++
	tenant := req.GetTargetTenantId()
	if b.wrongTenant != "" {
		tenant = b.wrongTenant
	}
	b.memberToken = fmt.Sprintf("member-%d", b.exchanges)
	return &authnv1.ExchangeServiceTokenForTenantResponse{
		AccessToken: b.memberToken, ExpiresAtUnix: time.Now().Add(b.ttl).Unix(),
		TenantId: tenant, ProjectId: exchangeMemberProject, MembershipId: "membership-1",
	}, nil
}

func (b *tenantExchangeBroker) Increment(ctx context.Context, _ *cachev1.IncrementRequest) (*cachev1.IncrementResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.incrementMD = append(b.incrementMD, md.Copy())
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer "+b.memberToken {
		b.refused++
		return nil, status.Error(codes.Unauthenticated, "invalid credential")
	}
	return &cachev1.IncrementResponse{Value: 1}, nil
}

// revoke makes the broker refuse every member token issued so far.
func (b *tenantExchangeBroker) revoke(refuseExchange bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.memberToken = "revoked"
	b.refuseExchange = refuseExchange
}

func (b *tenantExchangeBroker) counts() (exchanges, refused int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.exchanges, b.refused
}

func serveTenantExchangeBroker(t *testing.T, b *tenantExchangeBroker) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	authnv1.RegisterAuthnServiceServer(srv, b)
	cachev1.RegisterCacheServiceServer(srv, b)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func connectExchangeHome(t *testing.T, ctx context.Context, b *tenantExchangeBroker) *Udb {
	t.Helper()
	home, err := Connect(ctx, Config{
		Target: serveTenantExchangeBroker(t, b), TenantID: exchangeHomeTenant,
		Credentials: Credentials{APIKey: "svc-key"}, Deadline: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("connect home: %v", err)
	}
	t.Cleanup(func() { _ = home.Close() })
	return home
}

// The member client exchanges the home bearer, acts in the member tenant, and
// exchanges again before the short-lived token expires.
func TestForTenantExchangesHomeBearerAndKeepsTheMemberTokenFresh(t *testing.T) {
	b := &tenantExchangeBroker{ttl: 2 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	home := connectExchangeHome(t, ctx, b)

	member, err := home.ForTenant(ctx, exchangeMemberTenant, exchangeMemberProject)
	if err != nil {
		t.Fatalf("ForTenant: %v", err)
	}
	defer member.Close()
	if member.Meta.TenantID != exchangeMemberTenant || member.Meta.ProjectID != exchangeMemberProject || member.Meta.UserID != exchangeServiceUser {
		t.Fatalf("member identity = %+v, want the exchanged principal", member.Meta)
	}
	if home.Meta.TenantID != exchangeHomeTenant {
		t.Fatalf("ForTenant changed the home identity to %q", home.Meta.TenantID)
	}
	if member.BearerExpiresAt().IsZero() || member.Principal().GetTenantId() != exchangeMemberTenant {
		t.Fatal("member client reports no exchanged token or principal")
	}

	cache := cachev1.NewCacheServiceClient(member.authConn)
	for i := 0; i < 3; i++ {
		if _, err := cache.Increment(ctx, &cachev1.IncrementRequest{Delta: 1}); err != nil {
			t.Fatalf("member call %d: %v", i, err)
		}
	}
	if n, _ := b.counts(); n != 1 {
		t.Fatalf("%d exchanges for three calls on a fresh token, want 1", n)
	}
	b.mu.Lock()
	callMD, exchangeMD := b.incrementMD[0], b.exchangeMD[0]
	b.mu.Unlock()
	if got := callMD.Get("x-tenant-id"); len(got) != 1 || got[0] != exchangeMemberTenant {
		t.Fatalf("member call x-tenant-id = %v, want only the member tenant", got)
	}
	if got := exchangeMD.Get("x-tenant-id"); len(got) != 1 || got[0] != exchangeHomeTenant {
		t.Fatalf("exchange x-tenant-id = %v, want only the home tenant", got)
	}

	// 4/5 of a 2s life (floored at 1s): a re-exchange before expiry, while
	// calls run, never with the expired token.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, _ := b.counts(); n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the member token was not exchanged again before it expired")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := cache.Increment(ctx, &cachev1.IncrementRequest{Delta: 1}); err != nil {
		t.Fatalf("call after the re-exchange: %v", err)
	}
	if err := member.CredentialErr(); err != nil {
		t.Fatalf("CredentialErr after a good re-exchange: %v", err)
	}
	if _, refused := b.counts(); refused != 0 {
		t.Fatalf("%d member calls carried a stale token", refused)
	}
}

// A member token the broker refuses early is exchanged again at once. While
// that exchange is refused, calls are refused locally (never one exchange per
// call, never the refused token resent); once the membership is restored the
// background loop recovers the same client.
func TestForTenantRefusedTokenFailsClosedWhileTheReexchangeBacksOff(t *testing.T) {
	b := &tenantExchangeBroker{ttl: time.Hour}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	home := connectExchangeHome(t, ctx, b)
	member, err := home.ForTenant(ctx, exchangeMemberTenant, "")
	if err != nil {
		t.Fatalf("ForTenant: %v", err)
	}
	defer member.Close()
	cache := cachev1.NewCacheServiceClient(member.authConn)

	b.revoke(true)
	if _, err := cache.Increment(ctx, &cachev1.IncrementRequest{Delta: 1}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("refused token: got %v, want UNAUTHENTICATED", err)
	}
	for range 20 {
		if _, err := cache.Increment(ctx, &cachev1.IncrementRequest{Delta: 1}); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("call while the re-exchange is refused: got %v, want a local refusal", err)
		}
	}
	if member.CredentialErr() == nil {
		t.Fatal("a member client whose exchange is refused reports no credential failure")
	}
	b.mu.Lock()
	attempts, refused := len(b.exchangeMD), b.refused
	b.mu.Unlock()
	if refused != 1 {
		t.Fatalf("the refused member token was sent %d times, want once", refused)
	}
	if attempts > 4 {
		t.Fatalf("%d exchange attempts for 21 calls: the refusal turned into one exchange per call", attempts)
	}

	b.mu.Lock()
	b.refuseExchange = false
	b.mu.Unlock()
	deadline := time.Now().Add(10 * time.Second)
	for member.CredentialErr() != nil {
		if time.Now().After(deadline) {
			t.Fatalf("the member client did not recover once the membership was restored: %v", member.CredentialErr())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := cache.Increment(ctx, &cachev1.IncrementRequest{Delta: 1}); err != nil {
		t.Fatalf("call after recovery: %v", err)
	}
}

// Negative: a token for another tenant than the one asked for is refused, and a
// tenant that is not a canonical UUID is refused before any RPC.
func TestForTenantRefusesAWrongTenant(t *testing.T) {
	b := &tenantExchangeBroker{ttl: time.Hour, wrongTenant: "55555555-5555-4555-8555-555555555555"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	home := connectExchangeHome(t, ctx, b)
	if member, err := home.ForTenant(ctx, exchangeMemberTenant, ""); err == nil || !strings.Contains(err.Error(), "returned a token for tenant") {
		if member != nil {
			_ = member.Close()
		}
		t.Fatalf("token for another tenant: got %v, want a named refusal", err)
	}
	for _, bad := range []string{"", "acme", "ABCDEF01-2345-4678-89AB-CDEF01234567"} {
		if _, err := home.ForTenant(ctx, bad, ""); err == nil || !strings.Contains(err.Error(), "canonical") {
			t.Fatalf("tenant %q: got %v, want a canonical-UUID refusal", bad, err)
		}
	}
	b.mu.Lock()
	attempts := len(b.exchangeMD)
	b.mu.Unlock()
	if attempts != 1 {
		t.Fatalf("%d exchange attempts, want only the wrong-tenant one", attempts)
	}
}

// A password-signed-in EnterpriseSession yields a member EnterpriseSession whose
// Bearer and canonical tenant are the exchanged ones.
func TestEnterpriseSessionForTenant(t *testing.T) {
	b := &tenantExchangeBroker{ttl: time.Hour}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{Target: serveTenantExchangeBroker(t, b), Username: "svc", Password: "secret"})
	if err != nil {
		t.Fatalf("ConnectEnterprise: %v", err)
	}
	defer sess.Close()
	member, err := sess.ForTenant(ctx, exchangeMemberTenant, exchangeMemberProject)
	if err != nil {
		t.Fatalf("ForTenant: %v", err)
	}
	defer member.Close()
	if member.CanonicalTenantID != exchangeMemberTenant || member.CanonicalProjectID != exchangeMemberProject {
		t.Fatalf("member session tenant/project = %q/%q", member.CanonicalTenantID, member.CanonicalProjectID)
	}
	if member.Bearer() != "Bearer member-1" || sess.Bearer() != "Bearer home-login" {
		t.Fatalf("bearers member=%q home=%q", member.Bearer(), sess.Bearer())
	}
	if err := member.ValidateTenant(exchangeHomeTenant); err == nil {
		t.Fatal("member session accepted a home-tenant record")
	}
	cache := cachev1.NewCacheServiceClient(member.NativeConn())
	if _, err := cache.Increment(member.NativeContext(ctx), &cachev1.IncrementRequest{Delta: 1}); err != nil {
		t.Fatalf("member session call: %v", err)
	}
	if err := member.RefreshErr(); err != nil {
		t.Fatalf("member RefreshErr: %v", err)
	}
}
