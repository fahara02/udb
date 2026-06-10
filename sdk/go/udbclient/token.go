package udbclient

import (
	"context"
	"sync"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
)

// ── Phase 7: token / session lifecycle ───────────────────────────────────────
//
// TokenManager wraps an AuthClient with a stored Token and a single-flighted
// refresh: when many goroutines hit an expired token at once, exactly one
// RefreshToken RPC runs and the rest wait on its result (hand-rolled with a
// sync.Mutex guard + a shared in-flight channel — no x/sync/singleflight dep,
// which the SDK does not pull in).
//
// Shape notes (verified against the generated authn stub):
//   - Login uses AuthnService.Authenticate -> AuthnResponse{access_token,
//     session_id, expires_at_unix, ...}. The native authn flow returns the
//     bearer there; there is no separate refresh_token on AuthnResponse, so the
//     session_id is what RefreshToken consumes.
//   - Refresh uses AuthnService.RefreshToken(RefreshTokenRequest{refresh_token,
//     session_id}) -> RefreshTokenResponse{access_token, access_token_expires_in}.
//     We pass the stored session id; if the deployment also issues a discrete
//     refresh token, set Token.RefreshToken and it is sent too.

// Token is the credential set a TokenManager stores. ExpiresAt is absolute; a
// zero value means "unknown / never auto-refresh".
type Token struct {
	AccessToken  string
	RefreshToken string
	SessionID    string
	ExpiresAt    time.Time
}

// Valid reports whether the token is non-empty and not within skew of expiry.
func (t Token) Valid(now time.Time, skew time.Duration) bool {
	if t.AccessToken == "" {
		return false
	}
	if t.ExpiresAt.IsZero() {
		return true // no expiry info; treat as valid until an explicit refresh
	}
	return now.Add(skew).Before(t.ExpiresAt)
}

// TokenStore persists a Token across calls (and, optionally, processes). The
// in-memory MemoryTokenStore is the default; callers can supply a file/keyring
// backed implementation.
type TokenStore interface {
	Load(ctx context.Context) (Token, error)
	Save(ctx context.Context, tok Token) error
}

// MemoryTokenStore is a concurrency-safe in-process TokenStore.
type MemoryTokenStore struct {
	mu  sync.RWMutex
	tok Token
}

func (m *MemoryTokenStore) Load(ctx context.Context) (Token, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tok, nil
}

func (m *MemoryTokenStore) Save(ctx context.Context, tok Token) error {
	m.mu.Lock()
	m.tok = tok
	m.mu.Unlock()
	return nil
}

// TokenManager logs in, stores the token, and refreshes it on demand with a
// single-flight guard so concurrent callers share one refresh round-trip.
type TokenManager struct {
	auth  *AuthClient
	store TokenStore

	// RefreshSkew refreshes this long before actual expiry. Default 30s.
	RefreshSkew time.Duration
	// now is injectable for tests.
	now func() time.Time

	mu       sync.Mutex
	inflight chan struct{} // non-nil while a refresh is running
	refErr   error         // result of the in-flight refresh
}

// NewTokenManager builds a manager over an AuthClient. A nil store defaults to
// an in-memory store.
func NewTokenManager(auth *AuthClient, store TokenStore) *TokenManager {
	if store == nil {
		store = &MemoryTokenStore{}
	}
	return &TokenManager{
		auth:        auth,
		store:       store,
		RefreshSkew: 30 * time.Second,
		now:         time.Now,
	}
}

// Login authenticates with a fully-formed AuthnRequest (use AuthClient's typed
// helpers to build it), stores the resulting Token, and returns it. The access
// token + session id + absolute expiry are derived from AuthnResponse.
func (m *TokenManager) Login(ctx context.Context, req *authnv1.AuthnRequest) (Token, error) {
	resp, err := m.auth.Authenticate(ctx, req)
	if err != nil {
		return Token{}, err
	}
	tok := tokenFromAuthn(resp, m.now())
	if err := m.store.Save(ctx, tok); err != nil {
		return Token{}, err
	}
	return tok, nil
}

func tokenFromAuthn(resp *authnv1.AuthnResponse, now time.Time) Token {
	tok := Token{
		AccessToken: resp.GetAccessToken(),
		SessionID:   resp.GetSessionId(),
	}
	if exp := resp.GetExpiresAtUnix(); exp > 0 {
		tok.ExpiresAt = time.Unix(exp, 0)
	}
	return tok
}

// Token returns the stored token, refreshing it first when it is expired or
// within RefreshSkew of expiry. Concurrent callers that all see a stale token
// share exactly one RefreshToken RPC.
func (m *TokenManager) Token(ctx context.Context) (Token, error) {
	tok, err := m.store.Load(ctx)
	if err != nil {
		return Token{}, err
	}
	if tok.Valid(m.now(), m.RefreshSkew) {
		return tok, nil
	}
	if err := m.RefreshIfNeeded(ctx); err != nil {
		return Token{}, err
	}
	return m.store.Load(ctx)
}

// RefreshIfNeeded refreshes the stored token if it is stale, sharing one
// in-flight refresh among concurrent callers. If the token is already fresh it
// returns immediately.
func (m *TokenManager) RefreshIfNeeded(ctx context.Context) error {
	tok, err := m.store.Load(ctx)
	if err != nil {
		return err
	}
	if tok.Valid(m.now(), m.RefreshSkew) {
		return nil
	}

	m.mu.Lock()
	if m.inflight != nil {
		// A refresh is already running; wait for it and adopt its result.
		done := m.inflight
		m.mu.Unlock()
		select {
		case <-done:
			m.mu.Lock()
			rerr := m.refErr
			m.mu.Unlock()
			return rerr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// We are the leader: start the in-flight refresh.
	done := make(chan struct{})
	m.inflight = done
	m.mu.Unlock()

	rerr := m.doRefresh(ctx, tok)

	m.mu.Lock()
	m.refErr = rerr
	m.inflight = nil
	m.mu.Unlock()
	close(done)
	return rerr
}

// doRefresh performs the actual RefreshToken RPC and persists the new token.
func (m *TokenManager) doRefresh(ctx context.Context, prev Token) error {
	resp, err := m.auth.Authn.RefreshToken(m.auth.Context(ctx), &authnv1.RefreshTokenRequest{
		RefreshToken: prev.RefreshToken,
		SessionId:    prev.SessionID,
	})
	if err != nil {
		return err
	}
	next := prev
	next.AccessToken = resp.GetAccessToken()
	if secs := resp.GetAccessTokenExpiresIn(); secs > 0 {
		next.ExpiresAt = m.now().Add(time.Duration(secs) * time.Second)
	}
	return m.store.Save(ctx, next)
}
