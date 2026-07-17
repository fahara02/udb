package udbclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	"google.golang.org/grpc/metadata"
)

// EnterpriseConfig drives ConnectEnterprise, the one-call production-path setup:
// dial data + auth targets, log in with username/password, verify the bearer,
// adopt the canonical tenant UUID, and carry that bearer on every subsequent
// call. Mirrors examples/ts_enterprise's flow in Go.
type EnterpriseConfig struct {
	Target     string // data-plane target, e.g. "127.0.0.1:50051" (required)
	AuthTarget string // control-plane target, e.g. "127.0.0.1:50061"; defaults to Target
	Username   string // required
	Password   string // required
	// TenantCode is the human tenant code hint used pre-login (e.g. "acme").
	// The verified canonical tenant UUID replaces it after login.
	TenantCode string
	ProjectID  string
	Purpose    string
	Scopes     []string
	UserID     string
	TLS        *tls.Config
	Deadline   time.Duration
	Retry      RetryConfig
}

// EnterpriseSession bundles the authenticated Udb with the VERIFIED canonical
// tenant state and the bearer.
//
// IMPORTANT: after login the broker connection's interceptor (set at dial time)
// does NOT pick up the post-login token, so raw u.Data / native calls would be
// Unauthenticated. Use DataContext / NativeContext (which append the bearer
// explicitly) for any call you make through the embedded *Udb, and use
// CanonicalTenantID — never the human code — in tenant-scoped records/filters.
type EnterpriseSession struct {
	*Udb
	// CanonicalTenantID is the verified tenant UUID (use this in all filters).
	CanonicalTenantID string
	// CanonicalProjectID is the verified project from the principal.
	CanonicalProjectID string
	// Principal is the verified login principal (for inspection).
	Principal *authnv1.Principal
	// Tenant tracks the code -> canonical-UUID transition + the fail-fast guard.
	Tenant TenantState

	// tm resolves + refreshes the bearer for the session's whole lifetime, instead
	// of freezing the single initial access token (which expires and would drop a
	// long-running service). Composed from the login token in ConnectEnterprise.
	tm *TokenManager
	// mu guards the cached bearer + refresh state against concurrent access.
	mu     sync.Mutex
	bearer string // last-known "Bearer <access-token>", refreshed via tm
	// lastRefreshErr / poisoned are the background refresher's fail-closed state
	// (guarded by mu). poisoned is set only when the stored token is EXPIRED and
	// can no longer be refreshed, so DataContext/NativeContext refuse the call
	// locally instead of sending a dead credential.
	lastRefreshErr error
	poisoned       bool

	// stopRefresh stops the background bearer refresher; closed once by Close.
	stopRefresh chan struct{}
	stopOnce    sync.Once
}

// Background bearer-refresher tuning.
const (
	bgRefreshMin     = 1 * time.Second  // floor between attempts (never busy-loop)
	bgRefreshIdle    = 5 * time.Minute  // cadence when the token carries no expiry
	bgRefreshRetry   = 5 * time.Second  // cadence after a store-load error
	bgRefreshTimeout = 30 * time.Second // per-attempt RefreshToken deadline
)

// ConnectEnterprise runs the full enterprise flow in one call and returns a
// session whose canonical tenant is verified and whose bearer is ready to attach
// via DataContext / NativeContext.
func ConnectEnterprise(ctx context.Context, cfg EnterpriseConfig) (*EnterpriseSession, error) {
	if cfg.Target == "" || cfg.Username == "" || cfg.Password == "" {
		return nil, fmt.Errorf("udb: ConnectEnterprise requires Target, Username, and Password")
	}
	if cfg.AuthTarget == "" {
		cfg.AuthTarget = cfg.Target
	}

	tenant := NewTenantState(cfg.TenantCode)
	hint := cfg.TenantCode
	if hint == "" {
		hint = "default"
	}

	u, err := NewUdb(ctx, Config{
		Target:     cfg.Target,
		AuthTarget: cfg.AuthTarget,
		TenantID:   hint, // pre-login hint; replaced by the canonical UUID below
		ProjectID:  cfg.ProjectID,
		Purpose:    cfg.Purpose,
		Scopes:     cfg.Scopes,
		UserID:     cfg.UserID,
		TLS:        cfg.TLS,
		Deadline:   cfg.Deadline,
		Retry:      cfg.Retry,
	})
	if err != nil {
		return nil, err
	}

	adopted, err := u.LoginAndAdoptTenant(ctx, &authnv1.LoginRequest{
		Username: cfg.Username,
		Password: cfg.Password,
		// UDB-GO-001: transmit the documented pre-login tenant/project selection so
		// the broker can resolve/verify the intended identity. The post-login source
		// of truth is unchanged — only the VERIFIED principal (adopted below) is ever
		// used for tenant/project scope, never these input hints.
		TenantHint:  cfg.TenantCode,
		ProjectHint: cfg.ProjectID,
	})
	if err != nil {
		_ = u.Close()
		return nil, fmt.Errorf("udb: ConnectEnterprise login: %w", err)
	}
	principal := adopted.Principal
	if principal == nil {
		_ = u.Close()
		return nil, fmt.Errorf("udb: ConnectEnterprise: no principal returned")
	}
	if err := tenant.Adopt(principal.GetTenantId()); err != nil {
		_ = u.Close()
		return nil, fmt.Errorf("udb: ConnectEnterprise adopt tenant: %w", err)
	}

	// Seed a TokenManager with the token we just obtained so the session refreshes
	// its bearer for its whole lifetime — a long-running service must not lose UDB
	// access when the initial access token expires. Refresh (RefreshToken RPC) uses
	// the login response's refresh_token + session_id, carried on adopted.Token.
	store := &MemoryTokenStore{}
	if err := store.Save(ctx, adopted.Token); err != nil {
		_ = u.Close()
		return nil, fmt.Errorf("udb: ConnectEnterprise seed token: %w", err)
	}

	sess := &EnterpriseSession{
		Udb:                u,
		CanonicalTenantID:  principal.GetTenantId(),
		CanonicalProjectID: principal.GetProjectId(),
		Principal:          principal,
		Tenant:             tenant,
		tm:                 NewTokenManager(u.Auth, store),
		bearer:             "Bearer " + adopted.Token.AccessToken,
		stopRefresh:        make(chan struct{}),
	}
	// Refresh the bearer proactively in the background so no data-plane call pays
	// the RefreshToken round-trip, and so an unrefreshable (revoked/expired) token
	// fails closed locally. Stopped by Close.
	sess.startRefreshLoop()
	return sess, nil
}

// DataContext returns a context for DataBroker calls (s.Data.Broker.*) carrying
// the verified metadata AND the bearer. Use it for every data-plane call so the
// post-login token is sent (the dial-time interceptor does not carry it).
func (s *EnterpriseSession) DataContext(ctx context.Context) context.Context {
	if pctx, poisoned := s.poisonedContext(ctx); poisoned {
		return pctx
	}
	return metadata.AppendToOutgoingContext(s.Udb.Data.Context(ctx), "authorization", s.currentBearer(ctx))
}

// NativeContext returns a context for native control-plane calls (ApiKey/Tenant/
// Notification/…) carrying the verified metadata AND the bearer.
func (s *EnterpriseSession) NativeContext(ctx context.Context) context.Context {
	if pctx, poisoned := s.poisonedContext(ctx); poisoned {
		return pctx
	}
	return metadata.AppendToOutgoingContext(s.Udb.Auth.Context(ctx), "authorization", s.currentBearer(ctx))
}

// startRefreshLoop launches the background bearer refresher: it wakes shortly
// before the access token's expiry, refreshes it (sharing the TokenManager's
// single-flight RefreshToken RPC), and keeps the poison state current. A no-op
// when there is no TokenManager. Stopped by Close.
func (s *EnterpriseSession) startRefreshLoop() {
	if s.tm == nil {
		return
	}
	go func() {
		for {
			timer := time.NewTimer(s.nextRefreshWait())
			select {
			case <-s.stopRefresh:
				timer.Stop()
				return
			case <-timer.C:
			}
			s.backgroundRefresh()
		}
	}()
}

// nextRefreshWait returns how long to sleep before the next refresh attempt: just
// before (expiry - RefreshSkew), floored so a repeatedly-failing refresh never
// busy-loops, and a fixed idle cadence when the token carries no expiry.
func (s *EnterpriseSession) nextRefreshWait() time.Duration {
	tok, err := s.tm.store.Load(context.Background())
	if err != nil {
		return bgRefreshRetry
	}
	if tok.ExpiresAt.IsZero() {
		return bgRefreshIdle
	}
	d := tok.ExpiresAt.Add(-s.tm.RefreshSkew).Sub(s.tm.now())
	if d < bgRefreshMin {
		return bgRefreshMin
	}
	return d
}

// backgroundRefresh refreshes the bearer when stale and updates the fail-closed
// state: on success it caches the new bearer and clears any prior failure; on
// failure it records the error and poisons the session ONLY if the stored token
// is actually expired (a transient blip while the token is still valid must not
// fail closed).
func (s *EnterpriseSession) backgroundRefresh() {
	ctx, cancel := context.WithTimeout(context.Background(), bgRefreshTimeout)
	defer cancel()
	refErr := s.tm.RefreshIfNeeded(ctx)
	tok, loadErr := s.tm.store.Load(context.Background())
	now := s.tm.now()

	s.mu.Lock()
	defer s.mu.Unlock()
	if refErr == nil && loadErr == nil && tok.AccessToken != "" {
		s.bearer = "Bearer " + tok.AccessToken
		s.lastRefreshErr = nil
		s.poisoned = false
		return
	}
	switch {
	case refErr != nil:
		s.lastRefreshErr = refErr
	case loadErr != nil:
		s.lastRefreshErr = loadErr
	}
	s.poisoned = loadErr != nil || !tok.Valid(now, 0)
}

// poisonedContext returns an already-cancelled context whose cancel cause is the
// refresh failure when the session is poisoned, so the next RPC fails CLOSED
// locally (never sending a dead bearer) with a clear reason — retrievable via
// context.Cause(ctx) or RefreshErr. Otherwise it returns ctx unchanged.
func (s *EnterpriseSession) poisonedContext(ctx context.Context) (context.Context, bool) {
	s.mu.Lock()
	poisoned := s.poisoned
	cause := s.lastRefreshErr
	s.mu.Unlock()
	if !poisoned {
		return ctx, false
	}
	reason := fmt.Errorf("udb: bearer refresh failed, session not authorized")
	if cause != nil {
		reason = fmt.Errorf("udb: bearer refresh failed, session not authorized: %w", cause)
	}
	pctx, cancel := context.WithCancelCause(ctx)
	cancel(reason)
	return pctx, true
}

// RefreshErr returns the most recent background bearer-refresh error, or nil if
// the last refresh succeeded. Useful for health checks and logging.
func (s *EnterpriseSession) RefreshErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRefreshErr
}

// Close stops the background bearer refresher and closes the underlying
// connections. Safe to call more than once.
func (s *EnterpriseSession) Close() error {
	if s.stopRefresh != nil {
		s.stopOnce.Do(func() { close(s.stopRefresh) })
	}
	return s.Udb.Close()
}

// Bearer is the "Bearer <token>" credential, for callers that build their own
// metadata.
func (s *EnterpriseSession) Bearer() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bearer
}

// currentBearer resolves the live bearer, refreshing the access token once when it
// is expired or within the manager's refresh skew (concurrent callers share ONE
// RefreshToken RPC via the TokenManager's single-flight). On success it atomically
// updates the cached bearer so Bearer() reflects the refreshed token too. If
// refresh fails it returns the last-known bearer, which the broker rejects as
// Unauthenticated — so a call never silently succeeds on an expired credential
// (fail closed) rather than proceeding with a stale-but-valid-looking token.
func (s *EnterpriseSession) currentBearer(ctx context.Context) string {
	if s.tm == nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.bearer
	}
	tok, err := s.tm.Token(ctx)
	if err != nil || tok.AccessToken == "" {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.bearer
	}
	bearer := "Bearer " + tok.AccessToken
	s.mu.Lock()
	s.bearer = bearer
	s.mu.Unlock()
	return bearer
}

// ValidateTenant fails fast (naming both values) if recordTenantID differs from
// the verified canonical tenant — call it before a tenant-scoped write.
func (s *EnterpriseSession) ValidateTenant(recordTenantID string) error {
	return s.Tenant.ValidateTenantID(recordTenantID)
}
