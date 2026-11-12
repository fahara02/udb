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
	"google.golang.org/protobuf/proto"
)

// restartBroker is an in-process broker that can be stopped and started again
// on the same address. Every start is a new generation that has lost the
// previous one's sessions: bearers and refresh tokens it issued are refused,
// as a broker restart that loses its session state refuses them.
type restartBroker struct {
	authnv1.UnimplementedAuthnServiceServer
	cachev1.UnimplementedCacheServiceServer

	t    *testing.T
	addr string

	mu        sync.Mutex
	srv       *grpc.Server
	gen       int
	principal *authnv1.Principal
	// counters
	logins, exchanges, refreshes int
	increments                   int // increments the broker applied
	refusedBearers               []string
	// refuseSignIn makes Login, RefreshToken and the key exchange refuse.
	refuseSignIn bool
}

func newRestartBroker(t *testing.T) *restartBroker {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &restartBroker{t: t, addr: lis.Addr().String(), principal: lifecyclePrincipal()}
	b.serve(lis)
	t.Cleanup(b.stop)
	return b
}

func (b *restartBroker) serve(lis net.Listener) {
	srv := grpc.NewServer()
	authnv1.RegisterAuthnServiceServer(srv, b)
	cachev1.RegisterCacheServiceServer(srv, b)
	b.mu.Lock()
	b.srv = srv
	b.mu.Unlock()
	go func() { _ = srv.Serve(lis) }()
}

// stop takes the broker down: every connection is closed and nothing listens.
func (b *restartBroker) stop() {
	b.mu.Lock()
	srv := b.srv
	b.srv = nil
	b.mu.Unlock()
	if srv != nil {
		srv.Stop()
	}
}

// start brings the broker back on the same address as a new generation.
func (b *restartBroker) start() {
	b.t.Helper()
	var lis net.Listener
	var err error
	for deadline := time.Now().Add(5 * time.Second); ; {
		if lis, err = net.Listen("tcp", b.addr); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		b.t.Fatalf("restart on %s: %v", b.addr, err)
	}
	b.mu.Lock()
	b.gen++
	b.mu.Unlock()
	b.serve(lis)
}

func (b *restartBroker) issued(kind string, n int) string {
	return fmt.Sprintf("g%d-%s-%d", b.gen, kind, n)
}

func (b *restartBroker) current(token string) bool {
	return strings.HasPrefix(token, fmt.Sprintf("g%d-", b.gen))
}

func (b *restartBroker) Login(context.Context, *authnv1.LoginRequest) (*authnv1.LoginResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.logins++
	if b.refuseSignIn {
		return nil, status.Error(codes.Unauthenticated, "invalid credential")
	}
	return &authnv1.LoginResponse{AccessToken: b.issued("access", b.logins), RefreshToken: b.issued("refresh", b.logins), SessionId: b.issued("session", b.logins), AccessTokenExpiresIn: 3600}, nil
}

func (b *restartBroker) Authenticate(_ context.Context, req *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if req.GetApiKey() != "" {
		b.exchanges++
		if b.refuseSignIn {
			return nil, status.Error(codes.Unauthenticated, "invalid API key")
		}
		return &authnv1.AuthnResponse{AccessToken: b.issued("key", b.exchanges), ExpiresAtUnix: time.Now().Add(time.Hour).Unix(), Principal: proto.Clone(b.principal).(*authnv1.Principal)}, nil
	}
	return &authnv1.AuthnResponse{ExpiresAtUnix: time.Now().Add(time.Hour).Unix(), Principal: proto.Clone(b.principal).(*authnv1.Principal)}, nil
}

func (b *restartBroker) RefreshToken(_ context.Context, req *authnv1.RefreshTokenRequest) (*authnv1.RefreshTokenResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refreshes++
	if b.refuseSignIn || !b.current(req.GetRefreshToken()) {
		return nil, status.Error(codes.Unauthenticated, "refresh token is not known")
	}
	return &authnv1.RefreshTokenResponse{AccessToken: b.issued("refreshed", b.refreshes), RefreshToken: b.issued("refresh-next", b.refreshes), AccessTokenExpiresIn: 3600}, nil
}

// Increment is the request-limit counter of the incident: a mutation that is
// neither read-only nor replay-safe. Authentication runs before it applies.
func (b *restartBroker) Increment(ctx context.Context, _ *cachev1.IncrementRequest) (*cachev1.IncrementResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	bearer := strings.TrimPrefix(strings.Join(md.Get("authorization"), ","), "Bearer ")
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.current(bearer) {
		b.refusedBearers = append(b.refusedBearers, bearer)
		return nil, status.Error(codes.Unauthenticated, "invalid credential")
	}
	b.increments++
	return &cachev1.IncrementResponse{Value: int64(b.increments)}, nil
}

func (b *restartBroker) snapshot() (logins, exchanges, increments int, refused []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.logins, b.exchanges, b.increments, append([]string(nil), b.refusedBearers...)
}

// incrementAfterRestart calls Increment until the restarted broker answers.
// While the channel reconnects the call may fail UNAVAILABLE (a mutation is
// never repeated on its own); any other failure is returned at once.
func incrementAfterRestart(ctx context.Context, cache cachev1.CacheServiceClient, callCtx func(context.Context) context.Context) error {
	for {
		_, err := cache.Increment(callCtx(ctx), &cachev1.IncrementRequest{Namespace: "limits", Key: "gateway", Delta: 1})
		if status.Code(err) != codes.Unavailable {
			return err
		}
		if ctx.Err() != nil {
			return fmt.Errorf("broker did not become reachable again: %w", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestBrokerRestartEnterpriseSessionRecoversWithoutRecreate(t *testing.T) {
	b := newRestartBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{Target: b.addr, Username: "gateway", Password: "secret", Deadline: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	cache := cachev1.NewCacheServiceClient(sess.NativeConn())
	if _, err := cache.Increment(sess.NativeContext(ctx), &cachev1.IncrementRequest{Namespace: "limits", Key: "gateway", Delta: 1}); err != nil {
		t.Fatalf("before restart: %v", err)
	}
	before := sess.Bearer()

	b.stop()
	_, err = cache.Increment(sess.NativeContext(ctx), &cachev1.IncrementRequest{Namespace: "limits", Key: "gateway", Delta: 1})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("broker down: got %v, want UNAVAILABLE", err)
	}
	// Let the channel fail a few dials, growing its reconnect backoff.
	time.Sleep(1500 * time.Millisecond)
	b.start()

	recoverCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	if err := incrementAfterRestart(recoverCtx, cache, sess.NativeContext); err != nil {
		t.Fatalf("same session after restart: %v", err)
	}
	logins, _, increments, refused := b.snapshot()
	if logins != 2 {
		t.Fatalf("session did not sign in again after its refresh token was refused: %d logins", logins)
	}
	if increments != 2 {
		t.Fatalf("refused increment applied or repeated: %d increments, want 2", increments)
	}
	if len(refused) != 1 || "Bearer "+refused[0] != before {
		t.Fatalf("broker saw %v refused bearers, want only the pre-restart one", refused)
	}
	if after := sess.Bearer(); after == before || !strings.HasPrefix(after, "Bearer g1-") {
		t.Fatalf("session kept the refused bearer: %q", after)
	}
	if err := sess.RefreshErr(); err != nil {
		t.Fatalf("recovered session reports %v", err)
	}
}

func TestBrokerRestartServiceAccountReexchangesKey(t *testing.T) {
	b := newRestartBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	u, err := Connect(ctx, Config{Target: b.addr, Credentials: Credentials{APIKey: "svc-key"}, Deadline: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	cache := cachev1.NewCacheServiceClient(u.authConn)
	if _, err := cache.Increment(ctx, &cachev1.IncrementRequest{Delta: 1}); err != nil {
		t.Fatalf("before restart: %v", err)
	}

	b.stop()
	time.Sleep(500 * time.Millisecond)
	b.start()

	recoverCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	if err := incrementAfterRestart(recoverCtx, cache, func(ctx context.Context) context.Context { return ctx }); err != nil {
		t.Fatalf("same client after restart: %v", err)
	}
	_, exchanges, increments, refused := b.snapshot()
	if exchanges != 2 || increments != 2 || len(refused) != 1 {
		t.Fatalf("exchanges=%d increments=%d refused=%v, want 2, 2 and one refusal", exchanges, increments, refused)
	}
	if err := u.CredentialErr(); err != nil {
		t.Fatalf("recovered client reports %v", err)
	}
}

func TestConnectEnterpriseWaitsForRestartingBroker(t *testing.T) {
	b := newRestartBroker(t)
	b.stop()
	go func() {
		time.Sleep(200 * time.Millisecond)
		b.start()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{Target: b.addr, Username: "gateway", Password: "secret", Deadline: 2 * time.Second})
	if err != nil {
		t.Fatalf("ConnectEnterprise during a broker restart: %v", err)
	}
	_ = sess.Close()
}

// Negative: a broker that, after its restart, no longer accepts the session's
// credential at all gets one sign-in attempt and the call fails UNAUTHENTICATED;
// the refused call is never applied and never resent with the refused bearer.
func TestBrokerRestartRefusedCredentialFailsClosed(t *testing.T) {
	b := newRestartBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{Target: b.addr, Username: "gateway", Password: "secret", Deadline: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	cache := cachev1.NewCacheServiceClient(sess.NativeConn())

	b.stop()
	b.mu.Lock()
	b.refuseSignIn = true
	b.mu.Unlock()
	b.start()

	recoverCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	err = incrementAfterRestart(recoverCtx, cache, sess.NativeContext)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("refused credential: got %v, want UNAUTHENTICATED", err)
	}
	// Later calls are refused locally while the session is unauthorized; they
	// never turn into one sign-in attempt per call.
	for range 20 {
		_, err := cache.Increment(sess.NativeContext(ctx), &cachev1.IncrementRequest{Delta: 1})
		if status.Code(err) != codes.Unauthenticated && status.Code(err) != codes.Canceled {
			t.Fatalf("call after the refusal: got %v, want a local refusal", err)
		}
	}
	if sess.RefreshErr() == nil {
		t.Fatal("refused session reports no credential failure")
	}
	logins, _, increments, refused := b.snapshot()
	if increments != 0 || len(refused) != 1 {
		t.Fatalf("increments=%d refused=%v: the refused bearer was resent or the call applied", increments, refused)
	}
	if logins < 2 || logins > 3 {
		t.Fatalf("%d sign-in attempts after the refusal, want 1 or 2 (demand + background), never one per call", logins-1)
	}
}

// Negative: a service account whose key the restarted broker no longer accepts
// fails UNAUTHENTICATED and is refused locally afterwards; it never turns into
// one key exchange per call.
func TestBrokerRestartRefusedKeyFailsClosed(t *testing.T) {
	b := newRestartBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	u, err := Connect(ctx, Config{Target: b.addr, Credentials: Credentials{APIKey: "svc-key"}, Deadline: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	cache := cachev1.NewCacheServiceClient(u.authConn)

	b.stop()
	b.mu.Lock()
	b.refuseSignIn = true
	b.mu.Unlock()
	b.start()

	recoverCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	plain := func(ctx context.Context) context.Context { return ctx }
	if err := incrementAfterRestart(recoverCtx, cache, plain); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("refused key: got %v, want UNAUTHENTICATED", err)
	}
	for range 20 {
		if _, err := cache.Increment(ctx, &cachev1.IncrementRequest{Delta: 1}); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("call after the refusal: got %v, want a local refusal", err)
		}
	}
	if u.CredentialErr() == nil {
		t.Fatal("refused client reports no credential failure")
	}
	_, exchanges, increments, refused := b.snapshot()
	if increments != 0 || len(refused) != 1 {
		t.Fatalf("increments=%d refused=%v: the refused bearer was resent or the call applied", increments, refused)
	}
	if exchanges < 2 || exchanges > 3 {
		t.Fatalf("%d key exchanges after the refusal, want 1 or 2 (demand + background), never one per call", exchanges-1)
	}
}

// Negative: a bearer the client does not own (a static Credentials.Bearer, a
// delegated AsUser bearer) is never renewed or resent after a refusal.
func TestBrokerRefusalOfUnownedBearerIsNotRepeated(t *testing.T) {
	b := newRestartBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	static, err := Connect(ctx, Config{Target: b.addr, Credentials: Credentials{Bearer: "g0-static"}, Retry: DefaultRetryConfig()})
	if err != nil {
		t.Fatal(err)
	}
	defer static.Close()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{Target: b.addr, Username: "gateway", Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	b.stop()
	b.start()

	recoverCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	staticCache := cachev1.NewCacheServiceClient(static.authConn)
	if err := incrementAfterRestart(recoverCtx, staticCache, func(ctx context.Context) context.Context { return ctx }); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("static bearer: got %v, want UNAUTHENTICATED", err)
	}
	sessCache := cachev1.NewCacheServiceClient(sess.NativeConn())
	delegated := func(ctx context.Context) context.Context { return sess.AsUser(ctx, "g0-person") }
	if err := incrementAfterRestart(recoverCtx, sessCache, delegated); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("delegated bearer: got %v, want UNAUTHENTICATED", err)
	}
	logins, _, increments, refused := b.snapshot()
	if increments != 0 || len(refused) != 2 || logins != 1 {
		t.Fatalf("logins=%d increments=%d refused=%v: an unowned bearer was renewed or resent", logins, increments, refused)
	}
}
