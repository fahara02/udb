package udbclient

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// These fixtures use the public generated Authn transport and the production
// TokenManager. The store gate controls only the stale-load interleaving.
func serveSessionAuthn(t *testing.T, authn authnv1.AuthnServiceServer, broker servicesv1.DataBrokerServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	authnv1.RegisterAuthnServiceServer(srv, authn)
	if broker != nil {
		servicesv1.RegisterDataBrokerServer(srv, broker)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

type rotatingRefreshAuthn struct {
	authnv1.UnimplementedAuthnServiceServer
	mu        sync.Mutex
	current   string
	refreshes int
	ttl       int32
}

func (a *rotatingRefreshAuthn) Login(context.Context, *authnv1.LoginRequest) (*authnv1.LoginResponse, error) {
	return &authnv1.LoginResponse{
		AccessToken: "test-access-initial", RefreshToken: "test-refresh-initial",
		SessionId: "test-session", AccessTokenExpiresIn: a.lifetime(),
	}, nil
}

func (a *rotatingRefreshAuthn) RefreshToken(_ context.Context, req *authnv1.RefreshTokenRequest) (*authnv1.RefreshTokenResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refreshes++
	if req.GetRefreshToken() != a.current {
		return nil, status.Error(codes.Unauthenticated, "retired refresh credential")
	}
	a.current = "test-refresh-rotated"
	return &authnv1.RefreshTokenResponse{
		AccessToken: "test-access-rotated", RefreshToken: a.current, AccessTokenExpiresIn: a.lifetime(),
	}, nil
}

func (a *rotatingRefreshAuthn) lifetime() int32 {
	if a.ttl > 0 {
		return a.ttl
	}
	return 20
}

func (a *rotatingRefreshAuthn) refreshCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.refreshes
}

type refreshLoadGateKey struct{}

type gatedRefreshStore struct {
	MemoryTokenStore
	loaded  chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *gatedRefreshStore) Load(ctx context.Context) (Token, error) {
	tok, err := s.MemoryTokenStore.Load(ctx)
	if ctx.Value(refreshLoadGateKey{}) == true {
		gated := false
		s.once.Do(func() {
			gated = true
			close(s.loaded)
		})
		if gated {
			select {
			case <-s.release:
			case <-ctx.Done():
				return Token{}, ctx.Err()
			}
		}
	}
	return tok, err
}

func tokenRefreshClient(t *testing.T, target string) *AuthClient {
	t.Helper()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial auth fixture: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return NewAuthClient(conn, Metadata{})
}

func TestTokenManagerRechecksCredentialAfterConcurrentRotation(t *testing.T) {
	authn := &rotatingRefreshAuthn{current: "test-refresh-initial"}
	auth := tokenRefreshClient(t, serveSessionAuthn(t, authn, nil))
	store := &gatedRefreshStore{loaded: make(chan struct{}), release: make(chan struct{})}
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(store.release) }) }
	defer release()
	_ = store.Save(ctx, Token{
		AccessToken: "test-access-initial", RefreshToken: "test-refresh-initial",
		SessionID: "test-session", ExpiresAt: fixed.Add(-time.Second),
	})
	tm := NewTokenManager(auth, store)
	tm.now = func() time.Time { return fixed }
	tm.RefreshSkew = time.Second // isolate serialization from short-token pacing

	// B snapshots the old token, then stops before taking the manager mutex.
	done := make(chan error, 1)
	go func() { done <- tm.RefreshIfNeeded(context.WithValue(ctx, refreshLoadGateKey{}, true)) }()
	select {
	case <-store.loaded:
	case <-ctx.Done():
		t.Fatal("stale-load caller did not reach the gate")
	}
	// A completes a full real gRPC refresh and retires the old credential.
	if err := tm.RefreshIfNeeded(ctx); err != nil {
		t.Fatalf("first rotation failed: %v", err)
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stale-load caller submitted a retired credential: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("stale-load caller did not finish")
	}
	if got := authn.refreshCount(); got != 1 {
		t.Fatalf("completed rotation was repeated: refresh RPCs=%d, want 1", got)
	}
}

func TestTokenManagerTwentySecondBearerRenewsAtSharedBoundary(t *testing.T) {
	authn := &rotatingRefreshAuthn{current: "test-refresh-initial"}
	auth := tokenRefreshClient(t, serveSessionAuthn(t, authn, nil))
	tm := NewTokenManager(auth, nil)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tm.now = func() time.Time { return now }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	initial, err := tm.LoginWithDevice(ctx, &authnv1.LoginRequest{Username: "fixture", Password: "fixture"})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if _, err := tm.Token(ctx); err != nil || authn.refreshCount() != 0 {
		t.Fatal("a fresh 20-second bearer was refreshed immediately")
	}
	now = initial.IssuedAt.Add(16*time.Second - time.Nanosecond)
	if _, err := tm.Token(ctx); err != nil || authn.refreshCount() != 0 {
		t.Fatal("demand refresh ran before the renewal boundary")
	}
	now = initial.IssuedAt.Add(16 * time.Second)
	renewed, err := tm.Token(ctx)
	if err != nil || authn.refreshCount() != 1 {
		t.Fatalf("renewal boundary did not issue one refresh: %v", err)
	}
	if renewed.IssuedAt != now || renewed.ExpiresAt.Sub(renewed.IssuedAt) != 20*time.Second {
		t.Fatal("rotated bearer lost its issued lifetime")
	}
	if _, err := tm.Token(ctx); err != nil || authn.refreshCount() != 1 {
		t.Fatal("a newly rotated bearer was refreshed again immediately")
	}
	s := &EnterpriseSession{tm: tm}
	if wait := s.nextRefreshWait(); wait != 16*time.Second {
		t.Fatalf("background renewal disagrees with demand: wait=%v, want 16s", wait)
	}
}

func TestTokenManagerOneSecondBearerRenewsBeforeExpiry(t *testing.T) {
	authn := &rotatingRefreshAuthn{current: "test-refresh-initial", ttl: 1}
	auth := tokenRefreshClient(t, serveSessionAuthn(t, authn, nil))
	tm := NewTokenManager(auth, nil)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tm.now = func() time.Time { return now }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	initial, err := tm.LoginWithDevice(ctx, &authnv1.LoginRequest{Username: "fixture", Password: "fixture"})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	s := &EnterpriseSession{tm: tm}
	if got := s.nextRefreshWait(); got != 800*time.Millisecond {
		t.Fatalf("1s token background renewal must precede expiry, got %v", got)
	}
	now = initial.IssuedAt.Add(800*time.Millisecond - time.Nanosecond)
	if _, err := tm.Token(ctx); err != nil || authn.refreshCount() != 0 {
		t.Fatal("1s demand refresh ran before the shared renewal boundary")
	}
	now = initial.IssuedAt.Add(800 * time.Millisecond)
	if got := s.nextRefreshWait(); got != bgRefreshMin {
		t.Fatal("an already-due attempt must retain the retry floor")
	}
	if _, err := tm.Token(ctx); err != nil || authn.refreshCount() != 1 {
		t.Fatalf("1s demand renewal did not run at 800ms: %v", err)
	}
	if got := s.nextRefreshWait(); got != 800*time.Millisecond {
		t.Fatalf("a renewed 1s token lost its healthy cadence, got %v", got)
	}
}

// Blocking Done evaluation pauses a follower after it joins the production
// manager's current flight, before it can observe that flight's completion.
type gatedRefreshWaitContext struct {
	context.Context
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *gatedRefreshWaitContext) Done() <-chan struct{} {
	c.once.Do(func() {
		close(c.entered)
		<-c.release
	})
	return c.Context.Done()
}

type flightResultAuthn struct {
	authnv1.UnimplementedAuthnServiceServer
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	release chan struct{}
}

func (a *flightResultAuthn) RefreshToken(ctx context.Context, _ *authnv1.RefreshTokenRequest) (*authnv1.RefreshTokenResponse, error) {
	a.mu.Lock()
	a.calls++
	call := a.calls
	a.mu.Unlock()
	if call == 1 {
		close(a.entered)
		select {
		case <-a.release:
			return nil, status.Error(codes.Unauthenticated, "first refresh fixture refusal")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &authnv1.RefreshTokenResponse{AccessToken: "test-second-flight", AccessTokenExpiresIn: 3600}, nil
}

func TestTokenManagerFollowerKeepsItsCompletedFlightResult(t *testing.T) {
	authn := &flightResultAuthn{entered: make(chan struct{}), release: make(chan struct{})}
	auth := tokenRefreshClient(t, serveSessionAuthn(t, authn, nil))
	store := &MemoryTokenStore{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	waitCtx := &gatedRefreshWaitContext{Context: ctx, entered: make(chan struct{}), release: make(chan struct{})}
	var serverOnce, waiterOnce sync.Once
	releaseServer := func() { serverOnce.Do(func() { close(authn.release) }) }
	releaseWaiter := func() { waiterOnce.Do(func() { close(waitCtx.release) }) }
	var workers sync.WaitGroup
	defer func() { releaseServer(); releaseWaiter(); cancel(); workers.Wait() }()
	_ = store.Save(ctx, Token{AccessToken: "expired-fixture", ExpiresAt: time.Now().Add(-time.Second)})
	tm := NewTokenManager(auth, store)
	leaderDone, followerDone := make(chan error, 1), make(chan error, 1)
	workers.Add(1)
	go func() { defer workers.Done(); leaderDone <- tm.RefreshIfNeeded(ctx) }()
	select {
	case <-authn.entered:
	case <-ctx.Done():
		t.Fatal("first refresh did not reach the service gate")
	}
	workers.Add(1)
	go func() { defer workers.Done(); followerDone <- tm.RefreshIfNeeded(waitCtx) }()
	select {
	case <-waitCtx.entered:
	case <-ctx.Done():
		t.Fatal("follower did not join the first flight")
	}
	releaseServer()
	var firstErr error
	select {
	case firstErr = <-leaderDone:
		if status.Code(firstErr) != codes.Unauthenticated {
			t.Fatal("first flight must fail with the fixture refusal")
		}
	case <-ctx.Done():
		t.Fatal("first flight did not complete")
	}
	// A different flight completes successfully while the original follower
	// still cannot observe its already-completed first flight.
	if err := tm.RefreshIfNeeded(ctx); err != nil {
		t.Fatalf("second flight should succeed: %v", err)
	}
	releaseWaiter()
	select {
	case err := <-followerDone:
		if !errors.Is(err, firstErr) {
			t.Fatal("a later flight replaced the original follower's result")
		}
	case <-ctx.Done():
		t.Fatal("original follower did not complete")
	}
}
