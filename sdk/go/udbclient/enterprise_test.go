package udbclient

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	authnentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// A valid (unexpired) token: the background refresher must cache the bearer and
// clear any prior poison/error, WITHOUT issuing a RefreshToken RPC (so a nil
// AuthClient is safe — RefreshIfNeeded no-ops for a still-valid token).
func TestEnterpriseSession_BackgroundRefreshClearsPoisonOnValidToken(t *testing.T) {
	store := &MemoryTokenStore{}
	_ = store.Save(context.Background(), Token{AccessToken: "tok-a", ExpiresAt: time.Now().Add(time.Hour)})
	s := &EnterpriseSession{tm: NewTokenManager(nil, store), poisoned: true, lastRefreshErr: errors.New("stale")}

	s.backgroundRefresh()

	if err := s.RefreshErr(); err != nil {
		t.Fatalf("RefreshErr should be nil after a successful refresh, got %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned {
		t.Fatal("session should not be poisoned after a valid token")
	}
	if s.bearer != "Bearer tok-a" {
		t.Fatalf("bearer not updated: got %q", s.bearer)
	}
}

// UDB-GO-002: a refreshed bearer must reach EVERY consumer, including the embedded
// generated client used for escape-hatch calls (s.Generated.InvokeUnary). Before the
// fix the generated client kept the initial static bearer LoginAndAdoptTenant set and
// went Unauthenticated after the access token expired; currentBearer must propagate
// the fresh token to it (setBearerLocked) alongside Bearer() and DataContext.
func TestEnterpriseSession_CurrentBearerPropagatesToGeneratedClient(t *testing.T) {
	store := &MemoryTokenStore{}
	_ = store.Save(context.Background(), Token{AccessToken: "fresh", ExpiresAt: time.Now().Add(time.Hour)})
	u := &Udb{Generated: NewGenerated(nil, Options{Authorization: "Bearer stale-initial"})}
	s := &EnterpriseSession{Udb: u, tm: NewTokenManager(nil, store), bearer: "Bearer stale-initial"}

	got, err := s.currentBearer(context.Background())
	if err != nil {
		t.Fatalf("current bearer: %v", err)
	}

	if got != "Bearer fresh" {
		t.Fatalf("currentBearer returned %q, want %q", got, "Bearer fresh")
	}
	if auth := u.Generated.options().Authorization; auth != "Bearer fresh" {
		t.Fatalf("generated-client authorization not refreshed: got %q, want %q", auth, "Bearer fresh")
	}
	if s.Bearer() != "Bearer fresh" {
		t.Fatalf("Bearer() not refreshed: got %q, want %q", s.Bearer(), "Bearer fresh")
	}
}

// The background refresher must ALSO propagate the rotated bearer to the generated
// client, so a long-running service's generated-client calls never freeze on the
// initial token.
func TestEnterpriseSession_BackgroundRefreshPropagatesToGeneratedClient(t *testing.T) {
	store := &MemoryTokenStore{}
	_ = store.Save(context.Background(), Token{AccessToken: "rotated", ExpiresAt: time.Now().Add(time.Hour)})
	u := &Udb{Generated: NewGenerated(nil, Options{Authorization: "Bearer old"})}
	s := &EnterpriseSession{Udb: u, tm: NewTokenManager(nil, store), bearer: "Bearer old"}

	s.backgroundRefresh()

	if auth := u.Generated.options().Authorization; auth != "Bearer rotated" {
		t.Fatalf("generated-client authorization not refreshed by background loop: got %q, want %q", auth, "Bearer rotated")
	}
	if s.Bearer() != "Bearer rotated" {
		t.Fatalf("Bearer() not refreshed by background loop: got %q, want %q", s.Bearer(), "Bearer rotated")
	}
}

// When poisoned, DataContext/NativeContext must fail CLOSED locally: the returned
// context is already Done and its cancel cause carries the refresh failure.
func TestEnterpriseSession_PoisonedContextFailsClosed(t *testing.T) {
	boom := errors.New("refresh token revoked")
	s := &EnterpriseSession{poisoned: true, lastRefreshErr: boom}

	pctx, poisoned := s.poisonedContext(context.Background())
	if !poisoned {
		t.Fatal("expected the session to report poisoned")
	}
	if pctx.Err() == nil {
		t.Fatal("poisoned context must be Done so the RPC never sends a dead bearer")
	}
	if cause := context.Cause(pctx); !errors.Is(cause, boom) {
		t.Fatalf("cancel cause should wrap the refresh error, got %v", cause)
	}
}

// A healthy session returns the caller's context untouched (no accidental poison).
func TestEnterpriseSession_NotPoisonedPassthrough(t *testing.T) {
	s := &EnterpriseSession{}
	ctx := context.Background()
	got, poisoned := s.poisonedContext(ctx)
	if poisoned {
		t.Fatal("a healthy session must not be poisoned")
	}
	if got != ctx {
		t.Fatal("healthy session must return the original context unchanged")
	}
}

// The refresher schedules just before (expiry - skew), floors an expired token to
// bgRefreshMin (never busy-loops), and uses the idle cadence with no expiry info.
func TestEnterpriseSession_NextRefreshWait(t *testing.T) {
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := &MemoryTokenStore{}
	tm := NewTokenManager(nil, store)
	tm.now = func() time.Time { return fixed }
	s := &EnterpriseSession{tm: tm}

	_ = store.Save(context.Background(), Token{AccessToken: "a", ExpiresAt: fixed.Add(time.Hour)})
	if got := s.nextRefreshWait(); got < 59*time.Minute || got > time.Hour {
		t.Fatalf("valid token: want ~1h-skew, got %v", got)
	}

	_ = store.Save(context.Background(), Token{AccessToken: "a", ExpiresAt: fixed.Add(-time.Minute)})
	if got := s.nextRefreshWait(); got != bgRefreshMin {
		t.Fatalf("expired token: want bgRefreshMin, got %v", got)
	}

	_ = store.Save(context.Background(), Token{AccessToken: "a"})
	if got := s.nextRefreshWait(); got != bgRefreshIdle {
		t.Fatalf("no-expiry token: want bgRefreshIdle, got %v", got)
	}

	_ = store.Save(context.Background(), Token{
		AccessToken: "a", IssuedAt: fixed, ExpiresAt: fixed.Add(20 * time.Second),
	})
	if got := s.nextRefreshWait(); got != 16*time.Second {
		t.Fatalf("20s token must wait for its actual renewal boundary, got %v", got)
	}
	_ = store.Save(context.Background(), Token{AccessToken: "a", IssuedAt: fixed, ExpiresAt: fixed.Add(time.Second)})
	if got := s.nextRefreshWait(); got != 800*time.Millisecond {
		t.Fatalf("a healthy 1s token must renew before expiry, got %v", got)
	}
}

// sessionIdentityAuthn returns a verified, mutable fixture principal. It uses
// the real Connect/ConnectEnterprise transport so rejection must occur before
// live facade metadata or interceptor credentials are installed.
type sessionIdentityAuthn struct {
	authnv1.UnimplementedAuthnServiceServer
	mu            sync.Mutex
	principal     *authnv1.Principal
	authenticate  int
	logins        int
	refreshes     int
	nativeWire    []metadata.MD
	verifiedLogin map[string]bool
	omitPrincipal bool
}

func (a *sessionIdentityAuthn) Login(context.Context, *authnv1.LoginRequest) (*authnv1.LoginResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.logins++
	return &authnv1.LoginResponse{
		AccessToken: fmt.Sprintf("test-login-access-%d", a.logins), RefreshToken: "test-login-refresh",
		SessionId: "test-login-session", AccessTokenExpiresIn: 3600,
	}, nil
}

func (a *sessionIdentityAuthn) Authenticate(ctx context.Context, req *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if req.GetBearerToken() == "fixture-request" {
		md, _ := metadata.FromIncomingContext(ctx)
		if values := md.Get("authorization"); len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") || strings.TrimSpace(strings.TrimPrefix(values[0], "Bearer ")) == "" {
			return nil, status.Error(codes.Unauthenticated, "native fixture requires one nonempty owned bearer")
		}
		for _, key := range []string{"x-api-key", "x-udb-api-key"} {
			for _, value := range md.Get(key) {
				if value != "" {
					return nil, status.Error(codes.InvalidArgument, "native fixture received a raw API key")
				}
			}
		}
		a.nativeWire = append(a.nativeWire, md.Copy())
	}
	if strings.HasPrefix(req.GetBearerToken(), "test-login-access-") {
		if a.verifiedLogin == nil {
			a.verifiedLogin = make(map[string]bool)
		}
		a.verifiedLogin[req.GetBearerToken()] = true
	}
	a.authenticate++
	res := &authnv1.AuthnResponse{
		AccessToken:   fmt.Sprintf("test-exchange-access-%d", a.authenticate),
		ExpiresAtUnix: time.Now().Add(time.Hour).Unix(),
	}
	if !a.omitPrincipal && a.principal != nil {
		res.Principal = proto.Clone(a.principal).(*authnv1.Principal)
	}
	return res, nil
}

func (a *sessionIdentityAuthn) RefreshToken(context.Context, *authnv1.RefreshTokenRequest) (*authnv1.RefreshTokenResponse, error) {
	a.mu.Lock()
	a.refreshes++
	a.mu.Unlock()
	return nil, status.Error(codes.Unauthenticated, "refresh fixture refusal")
}

func sessionTestPrincipal() *authnv1.Principal {
	return &authnv1.Principal{
		TenantId: v232Tenant, ProjectId: v232Project, UserId: v232User,
		ServiceIdentity: "session-fixture", Scopes: []string{"data:read", "data:write"},
	}
}

func TestEnterpriseSessionReloginRejectsChangedIdentityBeforeInstallation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*authnv1.Principal)
	}{
		{"tenant", func(p *authnv1.Principal) { p.TenantId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" }},
		{"project", func(p *authnv1.Principal) { p.ProjectId = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" }},
		{"user", func(p *authnv1.Principal) { p.UserId = "cccccccc-cccc-4ccc-8ccc-cccccccccccc" }},
		{"service identity", func(p *authnv1.Principal) { p.ServiceIdentity = "other-service" }},
		{"scopes", func(p *authnv1.Principal) { p.Scopes = []string{"data:read"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authn := &sessionIdentityAuthn{principal: sessionTestPrincipal()}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			sess, err := ConnectEnterprise(ctx, EnterpriseConfig{
				Target: serveSessionAuthn(t, authn, nil), Username: "fixture", Password: "fixture", TenantCode: "hint",
			})
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer sess.Close()
			// Drive the exact background recovery pass ourselves after expiring the
			// stored bearer; the fixture's normal one-hour timer is stopped.
			sess.stopOnce.Do(func() { close(sess.stopRefresh) })
			beforeMeta, beforeOpts := sess.Meta, sess.Generated.options()
			beforeData, beforeAuth := sess.Data, sess.Auth
			beforeBearer := sess.Bearer()
			tok, err := sess.tm.store.Load(ctx)
			if err != nil {
				t.Fatalf("load token: %v", err)
			}
			tok.ExpiresAt = time.Now().Add(-time.Minute)
			if err := sess.tm.store.Save(ctx, tok); err != nil {
				t.Fatalf("expire fixture token: %v", err)
			}
			authn.mu.Lock()
			tc.change(authn.principal)
			authn.mu.Unlock()
			sess.backgroundRefresh()

			if err := sess.RefreshErr(); err == nil || !strings.Contains(err.Error(), tc.name) {
				t.Fatalf("renewal must name the rejected identity field %s", tc.name)
			}
			if !reflect.DeepEqual(sess.Meta, beforeMeta) || !reflect.DeepEqual(sess.Generated.options(), beforeOpts) {
				t.Fatal("rejected renewal changed live metadata or credentials")
			}
			if sess.Data != beforeData || sess.Auth != beforeAuth || sess.Bearer() != beforeBearer {
				t.Fatal("rejected renewal replaced live facade or bearer state")
			}
			if _, poisoned := sess.poisonedContext(ctx); !poisoned {
				t.Fatal("expired rejected renewal must leave the session locally poisoned")
			}
		})
	}
}

// Keep real facade calls active during credential renewal. In the race-enabled
// CI run, any renewal write to public metadata or facade fields is observable;
// every invocation must also retain the original canonical metadata order.
func exerciseStableFacadesDuringRenewal(t *testing.T, u *Udb, renew func(context.Context, int) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	beforeMeta := u.Meta
	beforeData, beforeAuth, beforeEvents, beforeAuthz := u.Data, u.Auth, u.Events, u.Authz
	ready := make(chan struct{})
	readerErr := make(chan error, 1)
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		first := true
		for ctx.Err() == nil {
			if !reflect.DeepEqual(u.Meta, beforeMeta) || !reflect.DeepEqual(u.Generated.Meta(), beforeMeta) ||
				u.Data != beforeData || u.Auth != beforeAuth || u.Events != beforeEvents || u.Authz != beforeAuthz {
				readerErr <- errors.New("renewal wrote public facade identity or handles")
				return
			}
			if _, err := u.Data.Select(ctx, &entityv1.SelectRequest{MessageType: "fixture"}); err != nil {
				if ctx.Err() == nil {
					readerErr <- fmt.Errorf("data call failed during same-identity renewal: %w", err)
				}
				return
			}
			if _, err := u.Auth.AuthenticateBearer(ctx, "fixture-request"); err != nil {
				if ctx.Err() == nil {
					readerErr <- fmt.Errorf("native call failed during same-identity renewal: %w", err)
				}
				return
			}
			if first {
				close(ready)
				first = false
			}
		}
	}()
	defer func() { cancel(); reader.Wait() }()
	select {
	case <-ready:
	case err := <-readerErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("facade reader did not become ready")
	}
	for i := 0; i < 12; i++ {
		if err := renew(ctx, i); err != nil {
			t.Fatalf("same-scope renewal %d failed: %v", i, err)
		}
	}
	cancel()
	reader.Wait()
	select {
	case err := <-readerErr:
		t.Fatal(err)
	default:
	}
	if !reflect.DeepEqual(u.Meta, beforeMeta) || !reflect.DeepEqual(u.Generated.Meta(), beforeMeta) ||
		u.Data != beforeData || u.Auth != beforeAuth || u.Events != beforeEvents || u.Authz != beforeAuthz {
		t.Fatal("renewal changed original canonical ordering or facade handles")
	}
}

func TestEnterpriseSessionReloginKeepsFacadesForReorderedScopes(t *testing.T) {
	authn := &sessionIdentityAuthn{principal: sessionTestPrincipal()}
	broker := &mdBroker{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{
		Target: serveSessionAuthn(t, authn, broker), Username: "fixture", Password: "fixture", TenantCode: "hint",
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer sess.Close()
	sess.stopOnce.Do(func() { close(sess.stopRefresh) })
	select {
	case <-sess.refreshDone:
	case <-ctx.Done():
		t.Fatal("owned background loop did not stop before the fixture deadline")
	}
	initialBearer := sess.Bearer()
	exerciseStableFacadesDuringRenewal(t, sess.Udb, func(ctx context.Context, i int) error {
		authn.mu.Lock()
		if i%2 == 0 {
			authn.principal.Scopes = []string{"data:write", "data:read", "data:read"}
		} else {
			authn.principal.Scopes = []string{"data:read", "data:write"}
		}
		authn.mu.Unlock()
		tok, err := sess.tm.store.Load(ctx)
		if err != nil {
			return err
		}
		// Enter the real proactive renewal window while the prior bearer remains
		// valid. Expiring it outside the flight races publication and correctly
		// refuses concurrent callers before the scope-order behavior is tested.
		boundary := time.Now()
		tok.IssuedAt = boundary.Add(-time.Hour)
		tok.ExpiresAt = boundary.Add(10 * time.Second)
		if tok.Valid(boundary, sess.tm.RefreshSkew) || !tok.Valid(boundary, 0) {
			return errors.New("fixture must be renewal-due and still valid")
		}
		if err := sess.tm.store.Save(ctx, tok); err != nil {
			return err
		}
		sess.backgroundRefresh()
		return sess.RefreshErr()
	})
	if sess.Bearer() == initialBearer {
		t.Fatal("twelve real recovery passes retained the initial bearer")
	}
	if _, err := sess.Data.Select(ctx, &entityv1.SelectRequest{MessageType: "fixture"}); err != nil {
		t.Fatalf("final data call: %v", err)
	}
	if _, err := sess.Auth.AuthenticateBearer(ctx, "fixture-request"); err != nil {
		t.Fatalf("final native call: %v", err)
	}
	want := sess.Bearer()
	broker.mu.Lock()
	dataBearer := append([]string(nil), broker.md.Get("authorization")...)
	broker.mu.Unlock()
	if len(dataBearer) != 1 || dataBearer[0] != want {
		t.Fatal("final data transport did not emit the recovered singleton bearer")
	}
	authn.mu.Lock()
	defer authn.mu.Unlock()
	if authn.logins < 13 || authn.refreshes < 12 || authn.authenticate < 13 || len(authn.nativeWire) < 2 {
		t.Fatalf("real recovery/native calls missing: login=%d refresh=%d authenticate=%d native=%d", authn.logins, authn.refreshes, authn.authenticate, len(authn.nativeWire))
	}
	for login := 1; login <= authn.logins; login++ {
		if !authn.verifiedLogin[fmt.Sprintf("test-login-access-%d", login)] {
			t.Fatalf("owned login %d was not actually verified over native transport", login)
		}
	}
	if values := authn.nativeWire[len(authn.nativeWire)-1].Get("authorization"); len(values) != 1 || values[0] != want {
		t.Fatal("final native transport did not emit the recovered singleton bearer")
	}
}

func TestEnterpriseSessionDelayedCallerCannotRestoreOlderBearer(t *testing.T) {
	authn := &rotatingRefreshAuthn{current: "test-refresh-initial"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	u, err := Connect(ctx, Config{
		Target: serveSessionAuthn(t, authn, nil), Credentials: Credentials{Bearer: "test-access-initial"},
	})
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer u.Close()
	store := &gatedRefreshStore{loaded: make(chan struct{}), release: make(chan struct{})}
	tm := NewTokenManager(u.Auth, store)
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tm.now = func() time.Time { return fixed }
	old := Token{
		AccessToken: "test-access-initial", RefreshToken: "test-refresh-initial", SessionID: "test-session",
		IssuedAt: fixed, ExpiresAt: fixed.Add(time.Hour),
	}
	_ = store.Save(ctx, old)
	sess := &EnterpriseSession{Udb: u, tm: tm, bearer: "Bearer " + old.AccessToken}
	var workers sync.WaitGroup
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(store.release) }) }
	defer func() { release(); cancel(); workers.Wait() }()
	done := make(chan string, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		bearer, _ := sess.currentBearer(context.WithValue(ctx, refreshLoadGateKey{}, true))
		done <- bearer
	}()
	select {
	case <-store.loaded:
	case <-ctx.Done():
		t.Fatal("delayed caller did not snapshot the original bearer")
	}
	// Rotate through the actual manager/RPC while the caller retains a valid old
	// snapshot. The shared TokenStore is the supported source of current state.
	expired := old
	expired.ExpiresAt = fixed.Add(-time.Second)
	_ = store.Save(ctx, expired)
	sess.backgroundRefresh()
	want := sess.Bearer()
	if want == "Bearer "+old.AccessToken || sess.RefreshErr() != nil || authn.refreshCount() != 1 {
		t.Fatal("background did not install the single rotated credential")
	}
	release()
	select {
	case got := <-done:
		if got != want || sess.Bearer() != want || u.Generated.options().Authorization != want {
			t.Fatal("delayed caller restored an older credential after rotation")
		}
	case <-ctx.Done():
		t.Fatal("delayed caller did not finish")
	}
}

type gatedPublicationStore struct {
	MemoryTokenStore
	mu      sync.Mutex
	loads   int
	loaded  chan struct{}
	release chan struct{}
}

func (s *gatedPublicationStore) Load(ctx context.Context) (Token, error) {
	tok, err := s.MemoryTokenStore.Load(ctx)
	s.mu.Lock()
	s.loads++
	gate := s.loads == 2
	s.mu.Unlock()
	if gate {
		close(s.loaded)
		select {
		case <-s.release:
		case <-ctx.Done():
			return Token{}, ctx.Err()
		}
	}
	return tok, err
}

func TestEnterpriseSessionBackgroundPublicationSerializesStoreReload(t *testing.T) {
	authn := &rotatingRefreshAuthn{current: "test-refresh-initial"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	u, err := Connect(ctx, Config{
		Target: serveSessionAuthn(t, authn, nil), Credentials: Credentials{Bearer: "test-access-initial"},
	})
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer u.Close()
	store := &gatedPublicationStore{loaded: make(chan struct{}), release: make(chan struct{})}
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	old := Token{
		AccessToken: "test-access-initial", RefreshToken: "test-refresh-initial", SessionID: "test-session",
		IssuedAt: fixed, ExpiresAt: fixed.Add(time.Hour),
	}
	_ = store.Save(ctx, old)
	tm := NewTokenManager(u.Auth, store)
	tm.now = func() time.Time { return fixed }
	sess := &EnterpriseSession{Udb: u, tm: tm, bearer: "Bearer " + old.AccessToken}
	var workers sync.WaitGroup
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(store.release) }) }
	defer func() { release(); cancel(); workers.Wait() }()
	workers.Add(1)
	go func() { defer workers.Done(); sess.backgroundRefresh() }()
	select {
	case <-store.loaded:
	case <-ctx.Done():
		t.Fatal("background publication did not reach its store reload")
	}
	// Reload and publication must form one serialized operation. If the load
	// precedes the lock, another publisher can install B before this pass writes A.
	if sess.mu.TryLock() {
		sess.mu.Unlock()
		t.Fatal("background snapshot was loaded outside publication serialization")
	}
	expired := old
	expired.ExpiresAt = fixed.Add(-time.Second)
	_ = store.Save(ctx, expired)
	rotated, err := tm.Token(ctx) // real RPC; no refresh waits under sess.mu
	if err != nil || authn.refreshCount() != 1 {
		t.Fatalf("concurrent credential rotation: %v", err)
	}
	done := make(chan string, 1)
	workers.Add(1)
	go func() { defer workers.Done(); bearer, _ := sess.currentBearer(ctx); done <- bearer }()
	release()
	select {
	case got := <-done:
		want := "Bearer " + rotated.AccessToken
		if got != want || sess.Bearer() != want || u.Generated.options().Authorization != want {
			t.Fatal("background publication overtook a newer installed credential")
		}
	case <-ctx.Done():
		t.Fatal("serialized publications did not finish")
	}
}

// Close stops the background refresher and is safe to call more than once. (Uses a
// nil embedded *Udb, so we exercise only the stop path, not Udb.Close.)
func TestEnterpriseSession_CloseStopIdempotent(t *testing.T) {
	s := &EnterpriseSession{stopRefresh: make(chan struct{})}
	s.stopOnce.Do(func() { close(s.stopRefresh) })
	// A second Do must not double-close (which would panic).
	s.stopOnce.Do(func() { close(s.stopRefresh) })
	select {
	case <-s.stopRefresh:
	default:
		t.Fatal("stopRefresh should be closed")
	}
}

// The generated TCP fixture checks delegation on the actual owned enterprise
// connection. Its expired credential is deliberate test state: explicit user
// delegation must not touch that provider, while an ordinary call still renews.
type enterpriseAsUserAuthn struct {
	authnv1.UnimplementedAuthnServiceServer
	wire      *asUserWireFixture
	mu        sync.Mutex
	refreshes int
}

func (a *enterpriseAsUserAuthn) Login(context.Context, *authnv1.LoginRequest) (*authnv1.LoginResponse, error) {
	return &authnv1.LoginResponse{
		AccessToken: "fixture-service-bearer", RefreshToken: "fixture-session-refresh",
		SessionId: "fixture-session", AccessTokenExpiresIn: 3600,
	}, nil
}

func (a *enterpriseAsUserAuthn) Authenticate(ctx context.Context, request *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	if request.GetBearerToken() != "fixture-service-bearer" {
		return nil, status.Error(codes.Unauthenticated, "fixture enterprise bearer required")
	}
	return a.wire.Authenticate(ctx, &authnv1.AuthnRequest{ApiKey: "fixture-service-key"})
}

func (a *enterpriseAsUserAuthn) ValidateToken(ctx context.Context, request *authnv1.ValidateTokenRequest) (*authnv1.ValidateTokenResponse, error) {
	return a.wire.ValidateToken(ctx, request)
}

func (a *enterpriseAsUserAuthn) RefreshToken(_ context.Context, request *authnv1.RefreshTokenRequest) (*authnv1.RefreshTokenResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refreshes++
	if request.GetRefreshToken() != "fixture-session-refresh" {
		return nil, status.Error(codes.Unauthenticated, "fixture refresh required")
	}
	return &authnv1.RefreshTokenResponse{
		AccessToken: "fixture-service-bearer", RefreshToken: "fixture-session-refresh", AccessTokenExpiresIn: 3600,
	}, nil
}

func TestEnterpriseSessionAsUserPreservesDelegationAndSessionRefusals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wire := &asUserWireFixture{}
	authn := &enterpriseAsUserAuthn{wire: wire}
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{
		Target: serveSessionAuthn(t, authn, wire), Username: "fixture", Password: "fixture",
		TenantCode: "fixture-hint", Purpose: "service-purpose", Retry: RetryConfig{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatalf("production enterprise Connect failed: code=%s", status.Code(err))
	}
	defer sess.Close()
	// Stop the idle test timer without closing the actual owned connection.
	sess.stopOnce.Do(func() { close(sess.stopRefresh) })
	select {
	case <-sess.refreshDone:
	case <-ctx.Done():
		t.Fatal("owned enterprise background loop did not stop before the due-token probe")
	}
	token, err := sess.tm.store.Load(ctx)
	if err != nil {
		t.Fatal("could not inspect the fixture's initial credential")
	}
	token.ExpiresAt = time.Now().Add(-time.Second)
	if err := sess.tm.store.Save(ctx, token); err != nil {
		t.Fatal("could not make the service fixture credential due")
	}
	beforeOptions, beforeBearer := sess.Generated.options(), sess.Bearer()
	audit := Metadata{Purpose: "delegated-purpose", CorrelationID: "delegated-correlation", ClientCatalogVersion: "delegated-catalog"}
	delegated := sess.AsUser(WithMetadata(ctx, audit), "fixture-user-bearer")
	dataCtx, nativeCtx := sess.DataContext(delegated), sess.NativeContext(delegated)
	for _, callCtx := range []context.Context{dataCtx, nativeCtx} {
		md, _ := metadata.FromOutgoingContext(callCtx)
		if values := md.Get("authorization"); len(values) != 1 || values[0] != asUserFixtureBearer {
			t.Fatal("enterprise context appended a service credential to delegation")
		}
		if values := md.Get("x-scopes"); len(values) != 0 {
			t.Fatal("enterprise context retained service scopes")
		}
	}
	if _, err := sess.Data.Broker.GetCapabilities(dataCtx, &entityv1.CapabilitiesRequest{}); err != nil {
		t.Fatalf("enterprise delegated data call failed: code=%s", status.Code(err))
	}
	if _, err := sess.Auth.Authn.ValidateToken(nativeCtx, &authnv1.ValidateTokenRequest{Token: "fixture-user-bearer", TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS}); err != nil {
		t.Fatalf("enterprise delegated native call failed: code=%s", status.Code(err))
	}
	var caps entityv1.CapabilitiesResponse
	if err := sess.Generated.InvokeUnary(dataCtx, "/udb.services.v1.DataBroker/GetCapabilities", &entityv1.CapabilitiesRequest{}, &caps); err != nil {
		t.Fatalf("enterprise delegated generated call failed: code=%s", status.Code(err))
	}
	authn.mu.Lock()
	refreshes := authn.refreshes
	authn.mu.Unlock()
	if refreshes != 0 || !reflect.DeepEqual(beforeOptions, sess.Generated.options()) || sess.Bearer() != beforeBearer {
		t.Fatal("explicit delegation refreshed or installed the service provider credential")
	}
	if _, err := sess.Data.Broker.GetCapabilities(sess.DataContext(ctx), &entityv1.CapabilitiesRequest{}); err != nil {
		t.Fatalf("ordinary enterprise data renewal failed: code=%s", status.Code(err))
	}
	if _, err := sess.Auth.Authn.ValidateToken(sess.NativeContext(ctx), &authnv1.ValidateTokenRequest{Token: "fixture-user-bearer", TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS}); err != nil {
		t.Fatalf("ordinary enterprise native call failed: code=%s", status.Code(err))
	}
	authn.mu.Lock()
	refreshes = authn.refreshes
	authn.mu.Unlock()
	if refreshes != 1 {
		t.Fatalf("ordinary service provider must still renew once, got %d attempts", refreshes)
	}
	refused := errors.New("fixture expired provider refusal")
	sess.mu.Lock()
	sess.poisoned, sess.lastRefreshErr = true, refused
	sess.mu.Unlock()
	for _, callCtx := range []context.Context{sess.DataContext(delegated), sess.NativeContext(delegated)} {
		if callCtx.Err() == nil || !errors.Is(context.Cause(callCtx), refused) {
			t.Fatal("explicit delegation bypassed local session poison refusal")
		}
		if _, err := sess.Data.Broker.GetCapabilities(callCtx, &entityv1.CapabilitiesRequest{}); status.Code(err) != codes.Canceled {
			t.Fatalf("poisoned delegation must refuse before transport: code=%s", status.Code(err))
		}
	}
	sess.mu.Lock()
	sess.poisoned, sess.lastRefreshErr = false, nil
	sess.mu.Unlock()
	canceled, cancelCall := context.WithCancelCause(delegated)
	cause := errors.New("fixture caller cancellation")
	cancelCall(cause)
	for _, callCtx := range []context.Context{sess.DataContext(canceled), sess.NativeContext(canceled)} {
		if !errors.Is(context.Cause(callCtx), cause) {
			t.Fatal("enterprise delegation lost caller cancellation")
		}
		if _, err := sess.Data.Broker.GetCapabilities(callCtx, &entityv1.CapabilitiesRequest{}); status.Code(err) != codes.Canceled {
			t.Fatalf("canceled delegation must refuse before transport: code=%s", status.Code(err))
		}
	}
	if err := sess.Close(); err != nil {
		t.Fatal("could not close the owned enterprise connection")
	}
	for _, callCtx := range []context.Context{sess.DataContext(delegated), sess.NativeContext(delegated)} {
		if _, err := sess.Data.Broker.GetCapabilities(callCtx, &entityv1.CapabilitiesRequest{}); status.Code(err) != codes.Canceled {
			t.Fatalf("closed enterprise connection must refuse delegation: code=%s", status.Code(err))
		}
	}
	wire.mu.Lock()
	defer wire.mu.Unlock()
	if wire.user != 3 || wire.service != 2 {
		t.Fatalf("enterprise served coverage changed or a refused call reached the fixture: ordinary=%d delegated=%d", wire.service, wire.user)
	}
}
