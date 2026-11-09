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

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Session renewal recovery: a renewal that fails is retried (bounded, jittered);
// a session holding a durable credential re-authenticates with it and never
// resubmits a refresh token that may have been consumed; a session that cannot
// renew fails its calls locally with ErrSessionExpired; concurrent callers at
// expiry share one renewal.

const renewalTestTenant = "00000000-0000-0000-0000-000000000001"

func TestTokenManagerRefreshSkewIsSixtySecondsOrTwentyPercent(t *testing.T) {
	tm := NewTokenManager(nil, nil)
	if tm.RefreshSkew != 60*time.Second {
		t.Fatalf("default RefreshSkew = %v, want 60s", tm.RefreshSkew)
	}
	issued := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct{ lifetime, skew time.Duration }{
		{15 * time.Minute, 3 * time.Minute}, // 20% of the lifetime exceeds 60s
		{2 * time.Minute, 60 * time.Second}, // the 60s floor
		{20 * time.Second, 4 * time.Second}, // the floor exceeds half the life: renew at 80%
	} {
		tok := Token{AccessToken: "a", IssuedAt: issued, ExpiresAt: issued.Add(c.lifetime)}
		if got := tok.refreshSkew(tm.RefreshSkew); got != c.skew {
			t.Fatalf("lifetime %v: skew %v, want %v", c.lifetime, got, c.skew)
		}
	}
}

func TestJitteredBackoffDoublesToTheCap(t *testing.T) {
	for failures, ceiling := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 4: 8 * time.Second, 7: 60 * time.Second, 30: 60 * time.Second} {
		for range 50 {
			got := jitteredBackoff(failures, time.Second, 60*time.Second)
			if got < ceiling/2 || got > ceiling {
				t.Fatalf("failure %d: wait %v outside [%v, %v]", failures, got, ceiling/2, ceiling)
			}
		}
	}
}

// lostReplyRefreshAuthn consumes the presented refresh token but loses the first
// reply; a later presentation of the consumed token is refused.
type lostReplyRefreshAuthn struct {
	authnv1.UnimplementedAuthnServiceServer
	mu        sync.Mutex
	presented []string
}

func (a *lostReplyRefreshAuthn) RefreshToken(_ context.Context, req *authnv1.RefreshTokenRequest) (*authnv1.RefreshTokenResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.presented = append(a.presented, req.GetRefreshToken())
	if len(a.presented) == 1 {
		return nil, status.Error(codes.Unavailable, "reply lost after the token was consumed")
	}
	return nil, status.Error(codes.Unauthenticated, "invalid refresh token")
}

func TestRefreshOnlySessionFailsCallsWithErrSessionExpired(t *testing.T) {
	authn := &lostReplyRefreshAuthn{}
	auth := tokenRefreshClient(t, serveSessionAuthn(t, authn, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &MemoryTokenStore{}
	_ = store.Save(ctx, Token{AccessToken: "expired", RefreshToken: "r-1", ExpiresAt: time.Now().Add(-time.Second)})
	tm := NewTokenManager(auth, store)
	tm.backoff = time.Millisecond
	s := &EnterpriseSession{tm: tm}

	_, err := s.currentBearer(ctx)
	if !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("call error does not wrap ErrSessionExpired: %v", err)
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("call error code = %v, want Unauthenticated", status.Code(err))
	}
	if !strings.Contains(err.Error(), "invalid refresh token") {
		t.Fatalf("call error lost the refresh failure: %v", err)
	}
	authn.mu.Lock()
	presented := append([]string(nil), authn.presented...)
	authn.mu.Unlock()
	if !reflect.DeepEqual(presented, []string{"r-1", "r-1"}) {
		t.Fatalf("presented %v: want one transient retry, then the refusal is final", presented)
	}
	pctx := s.DataContext(ctx)
	if pctx.Err() == nil {
		t.Fatal("an expired, unrenewable session must refuse calls locally")
	}
	if cause := context.Cause(pctx); !errors.Is(cause, ErrSessionExpired) {
		t.Fatalf("refusal cause does not wrap ErrSessionExpired: %v", cause)
	}
}

// lostRefreshLoginAuthn logs in with fresh tokens and, unless refreshOK is set,
// consumes every presented refresh token and loses the reply.
type lostRefreshLoginAuthn struct {
	authnv1.UnimplementedAuthnServiceServer
	mu           sync.Mutex
	logins       int
	refreshes    int
	presented    []string
	refreshOK    bool
	refreshDelay time.Duration
}

func (a *lostRefreshLoginAuthn) Login(context.Context, *authnv1.LoginRequest) (*authnv1.LoginResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.logins++
	return &authnv1.LoginResponse{
		AccessToken: fmt.Sprintf("access-%d", a.logins), RefreshToken: fmt.Sprintf("r-%d", a.logins),
		SessionId: "session", AccessTokenExpiresIn: 3600,
	}, nil
}

func (a *lostRefreshLoginAuthn) RefreshToken(_ context.Context, req *authnv1.RefreshTokenRequest) (*authnv1.RefreshTokenResponse, error) {
	a.mu.Lock()
	a.refreshes++
	a.presented = append(a.presented, req.GetRefreshToken())
	ok, delay := a.refreshOK, a.refreshDelay
	a.mu.Unlock()
	if !ok {
		return nil, status.Error(codes.DeadlineExceeded, "reply lost after the token was consumed")
	}
	time.Sleep(delay)
	return &authnv1.RefreshTokenResponse{AccessToken: "refreshed", RefreshToken: "r-rotated", AccessTokenExpiresIn: 3600}, nil
}

func (a *lostRefreshLoginAuthn) Authenticate(context.Context, *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	return &authnv1.AuthnResponse{Principal: &authnv1.Principal{TenantId: renewalTestTenant, UserId: "svc"}}, nil
}

func connectRenewalSession(t *testing.T, ctx context.Context, authn *lostRefreshLoginAuthn, broker *mdBroker) *EnterpriseSession {
	t.Helper()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{
		Target: serveSessionAuthn(t, authn, broker), Username: "svc", Password: "pw", TenantCode: "acme",
		Retry: RetryConfig{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	// Calls drive renewal on demand; the background loop is stopped.
	sess.stopOnce.Do(func() { close(sess.stopRefresh) })
	<-sess.refreshDone
	return sess
}

func expireSessionToken(t *testing.T, ctx context.Context, sess *EnterpriseSession) {
	t.Helper()
	tok, err := sess.tm.store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tok.ExpiresAt = time.Now().Add(-time.Second)
	if err := sess.tm.store.Save(ctx, tok); err != nil {
		t.Fatal(err)
	}
}

func TestPasswordSessionLogsInAgainAfterALostRefreshReplyWithoutResubmitting(t *testing.T) {
	authn := &lostRefreshLoginAuthn{}
	broker := &mdBroker{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sess := connectRenewalSession(t, ctx, authn, broker)

	for cycle := 1; cycle <= 3; cycle++ {
		expireSessionToken(t, ctx, sess)
		if _, err := sess.Data.Broker.Select(ctx, &entityv1.SelectRequest{MessageType: "x"}); err != nil {
			t.Fatalf("cycle %d: call after a lost refresh reply: %v", cycle, err)
		}
		broker.mu.Lock()
		got := broker.md.Get("authorization")
		broker.mu.Unlock()
		if want := fmt.Sprintf("Bearer access-%d", cycle+1); len(got) != 1 || got[0] != want {
			t.Fatalf("cycle %d: call carried %v, want the re-login bearer %q", cycle, got, want)
		}
	}
	authn.mu.Lock()
	defer authn.mu.Unlock()
	if !reflect.DeepEqual(authn.presented, []string{"r-1", "r-2", "r-3"}) {
		t.Fatalf("refresh tokens presented %v: each must be presented exactly once", authn.presented)
	}
	if authn.logins != 4 {
		t.Fatalf("logins = %d, want connect + one per cycle", authn.logins)
	}
	if err := sess.RefreshErr(); err != nil {
		t.Fatalf("RefreshErr after recovery: %v", err)
	}
}

func TestEnterpriseSessionConcurrentCallsAtExpiryShareOneRenewal(t *testing.T) {
	authn := &lostRefreshLoginAuthn{refreshOK: true, refreshDelay: 50 * time.Millisecond}
	broker := &mdBroker{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sess := connectRenewalSession(t, ctx, authn, broker)
	expireSessionToken(t, ctx, sess)

	var workers sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := sess.Data.Broker.Select(ctx, &entityv1.SelectRequest{MessageType: "x"})
			errs <- err
		}()
	}
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent call: %v", err)
		}
	}
	authn.mu.Lock()
	defer authn.mu.Unlock()
	if authn.refreshes != 1 || authn.logins != 1 {
		t.Fatalf("refreshes=%d logins=%d: want exactly one renewal shared by every caller", authn.refreshes, authn.logins)
	}
	if sess.Bearer() != "Bearer refreshed" {
		t.Fatalf("bearer %q, want the renewed one", sess.Bearer())
	}
}

// lossyKeyAuthn exchanges the API key for a bearer; loseNext loses one reply.
type lossyKeyAuthn struct {
	authnv1.UnimplementedAuthnServiceServer
	mu       sync.Mutex
	attempts int
	minted   int
	loseNext bool
}

func (a *lossyKeyAuthn) Authenticate(_ context.Context, req *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if req.GetApiKey() != "svc-key" {
		return nil, status.Error(codes.Unauthenticated, "unknown key")
	}
	a.attempts++
	if a.loseNext {
		a.loseNext = false
		return nil, status.Error(codes.Unavailable, "connection reset before the reply")
	}
	a.minted++
	return &authnv1.AuthnResponse{
		AccessToken:   fmt.Sprintf("key-bearer-%d", a.minted),
		ExpiresAtUnix: time.Now().Add(time.Hour).Unix(),
		Principal:     &authnv1.Principal{TenantId: renewalTestTenant},
	}, nil
}

func TestAPIKeySessionReauthenticatesAfterALostReplyAndTheNextCallSucceeds(t *testing.T) {
	authn := &lossyKeyAuthn{}
	broker := &mdBroker{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	u, err := Connect(ctx, Config{
		Target: serveSessionAuthn(t, authn, broker), TenantID: renewalTestTenant, Purpose: "test",
		Credentials: Credentials{APIKey: "svc-key"}, Retry: RetryConfig{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer u.Close()
	u.apiKey.mu.Lock()
	u.apiKey.backoff = time.Millisecond
	u.apiKey.mu.Unlock()

	for cycle := 1; cycle <= 3; cycle++ {
		authn.mu.Lock()
		authn.loseNext = true
		authn.mu.Unlock()
		u.apiKey.mu.Lock()
		u.apiKey.renewAt = time.Now().Add(-time.Second) // due for renewal
		u.apiKey.mu.Unlock()
		if _, err := u.Data.Broker.Select(ctx, &entityv1.SelectRequest{MessageType: "x"}); err != nil {
			t.Fatalf("cycle %d: call after a lost exchange reply: %v", cycle, err)
		}
		broker.mu.Lock()
		got := broker.md.Get("authorization")
		broker.mu.Unlock()
		if want := fmt.Sprintf("Bearer key-bearer-%d", cycle+1); len(got) != 1 || got[0] != want {
			t.Fatalf("cycle %d: call carried %v, want the re-exchanged bearer %q", cycle, got, want)
		}
		if err := u.CredentialErr(); err != nil {
			t.Fatalf("cycle %d: CredentialErr after recovery: %v", cycle, err)
		}
	}
	authn.mu.Lock()
	defer authn.mu.Unlock()
	if authn.attempts != 7 || authn.minted != 4 {
		t.Fatalf("attempts=%d minted=%d, want connect + (lost + retried) exchange per cycle", authn.attempts, authn.minted)
	}
}

func TestAPIKeySessionRefusedKeyFailsCallsWithErrSessionExpired(t *testing.T) {
	authn := &lossyKeyAuthn{}
	broker := &mdBroker{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	u, err := Connect(ctx, Config{
		Target: serveSessionAuthn(t, authn, broker), TenantID: renewalTestTenant, Purpose: "test",
		Credentials: Credentials{APIKey: "svc-key"}, Retry: RetryConfig{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer u.Close()
	u.apiKey.mu.Lock()
	u.apiKey.key = "revoked-key"
	u.apiKey.renewAt = time.Now().Add(-time.Second)
	u.apiKey.mu.Unlock()
	_, err = u.Data.Broker.Select(ctx, &entityv1.SelectRequest{MessageType: "x"})
	if !errors.Is(err, ErrSessionExpired) || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("refused key: %v (code %v), want ErrSessionExpired / Unauthenticated", err, status.Code(err))
	}
}
