package udbclient

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrSessionExpired reports that a session can no longer authorize calls: its
// bearer could not be renewed (and, for a session with no credential to log in
// again, has expired), or a renewal verified as a different identity. Calls fail
// locally with an error that wraps it and the last renewal failure, instead of
// reaching the broker with no or a dead bearer:
//
//	if errors.Is(err, udbclient.ErrSessionExpired) { /* reconnect or log in again */ }
//
// The error carries gRPC status Unauthenticated.
var ErrSessionExpired = errors.New("udb: session expired")

// sessionExpiredError wraps ErrSessionExpired and the renewal failure behind it.
type sessionExpiredError struct{ cause error }

func (e *sessionExpiredError) Error() string {
	return "udb: session expired, the bearer could not be renewed: " + e.cause.Error()
}

func (e *sessionExpiredError) Unwrap() []error { return []error{ErrSessionExpired, e.cause} }

// GRPCStatus makes status.Code report Unauthenticated, like the broker would.
func (e *sessionExpiredError) GRPCStatus() *status.Status {
	return status.New(codes.Unauthenticated, e.Error())
}

// sessionExpired wraps a renewal failure for a caller whose context is still
// live. A context error inside the cause is kept as text only, so the refusal is
// not mistaken for the caller's own cancellation.
func sessionExpired(cause error) error {
	if cause == nil {
		cause = errors.New("no credential can renew it")
	}
	var already *sessionExpiredError
	if errors.As(cause, &already) {
		return cause
	}
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		cause = errors.New(cause.Error())
	}
	return &sessionExpiredError{cause: cause}
}

// Renewal tuning shared by the token manager and the API-key session.
const (
	renewalAttempts    = 3                // attempts per renewal before it fails
	renewalBackoffBase = 1 * time.Second  // first wait; doubles per failure
	renewalBackoffCap  = 60 * time.Second // longest wait
	defaultRefreshSkew = 60 * time.Second
)

// jitteredBackoff is the wait after the n-th consecutive failure: a random time
// in [ceiling/2, ceiling], where ceiling = base * 2^(n-1), capped at limit.
func jitteredBackoff(failures int, base, limit time.Duration) time.Duration {
	if base <= 0 {
		base = renewalBackoffBase
	}
	ceiling := base
	for i := 1; i < failures && ceiling < limit; i++ {
		ceiling *= 2
	}
	ceiling = min(ceiling, limit)
	half := ceiling / 2
	return half + time.Duration(rand.Int63n(int64(ceiling-half)+1))
}

// transientRenewalError reports whether repeating a failed renewal step can
// help: the broker was unreachable, overloaded, or the reply was lost. A refused
// credential or a changed identity is final; so is the caller's own cancellation.
func transientRenewalError(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	var binding *credentialBindingError
	if errors.As(err, &binding) {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	}
	return false
}

// sleepContext waits d, or returns the context's error if it ends first.
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

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
	// IssuedAt records when this credential was received, allowing the refresh
	// margin to be bounded by its lifetime. Older stored tokens may omit it.
	IssuedAt time.Time
}

// Valid reports whether the token is non-empty and not within skew of expiry.
func (t Token) Valid(now time.Time, skew time.Duration) bool {
	if t.AccessToken == "" {
		return false
	}
	if t.ExpiresAt.IsZero() {
		return true // no expiry info; treat as valid until an explicit refresh
	}
	return now.Add(t.refreshSkew(skew)).Before(t.ExpiresAt)
}

// refreshSkew shares the same renewal boundary with demand and background
// refresh: the larger of the configured margin and 20% of a known lifetime, so a
// renewal has room for retries before expiry. A token too short for the margin
// (more than half its life) renews at 80% of its lifetime instead. A zero margin
// stays zero: Valid(now, 0) asks only whether the token has expired.
func (t Token) refreshSkew(skew time.Duration) time.Duration {
	if skew <= 0 {
		return 0
	}
	if !t.IssuedAt.IsZero() && !t.ExpiresAt.IsZero() {
		if lifetime := t.ExpiresAt.Sub(t.IssuedAt); lifetime > 0 {
			fraction := lifetime / 5
			skew = max(skew, fraction)
			if skew > lifetime/2 {
				skew = fraction
			}
		}
	}
	return skew
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

	// RefreshSkew is the minimum margin before expiry at which a token is
	// renewed. Default 60s; a token with a known issuance time renews at the larger
	// of this and 20% of its lifetime (80% of its lifetime when this margin would
	// exceed half of it).
	RefreshSkew time.Duration
	// now is injectable for tests.
	now func() time.Time
	// backoff is the first wait between renewal attempts (default 1s); tests
	// shorten it.
	backoff time.Duration

	mu       sync.Mutex
	inflight *tokenRefreshFlight // non-nil while a refresh is running
	// Owned enterprise lifecycle callbacks are installed before publication.
	// Refresh, permitted recovery, validation and publication share this flight.
	recoveryOwner *credentialOwner
	validate      func(context.Context, *Token) error
	recover       func(context.Context) (Token, error)
	publish       func(context.Context) error
	needsRecovery bool
	terminalErr   error
}

// A completed flight retains its own result for every waiter. A later refresh
// must not replace the result a delayed follower is about to observe.
type tokenRefreshFlight struct {
	done chan struct{}
	err  error // written before done closes, then immutable
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
		RefreshSkew: defaultRefreshSkew,
		now:         time.Now,
	}
}

// LoginSession is the canonical naming-contract accessor for the login/refresh
// session lifecycle: it constructs a TokenManager bound to this AuthClient
// (single-flight refresh, pluggable TokenStore — a nil store defaults to an
// in-memory one). It issues NO RPC itself; call Login/LoginWithDevice on the
// returned manager to authenticate. Alias of NewTokenManager(c, store).
func (c *AuthClient) LoginSession(store TokenStore) *TokenManager {
	return NewTokenManager(c, store)
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

// LoginWithDevice authenticates via the native AuthnService.Login RPC (rather
// than the generic Authenticate path), so a stable LoginRequest.DeviceId is sent
// to the broker — which mints a LISTABLE device row only when device_id is
// non-empty. This removes the need for a GenericDispatch device-seed workaround
// without any SDK-side proof read. The resulting Token is stored and returned.
func (m *TokenManager) LoginWithDevice(ctx context.Context, req *authnv1.LoginRequest) (Token, error) {
	resp, err := m.auth.Authn.Login(m.auth.Context(ctx), req)
	if err != nil {
		return Token{}, err
	}
	tok := tokenFromLogin(resp, m.now())
	if err := m.store.Save(ctx, tok); err != nil {
		return Token{}, err
	}
	return tok, nil
}

func tokenFromLogin(resp *authnv1.LoginResponse, now time.Time) Token {
	tok := Token{
		AccessToken:  resp.GetAccessToken(),
		RefreshToken: resp.GetRefreshToken(),
		SessionID:    resp.GetSessionId(),
		IssuedAt:     now,
	}
	if secs := resp.GetAccessTokenExpiresIn(); secs > 0 {
		tok.ExpiresAt = now.Add(time.Duration(secs) * time.Second)
	}
	return tok
}

func tokenFromAuthn(resp *authnv1.AuthnResponse, now time.Time) Token {
	tok := Token{
		AccessToken: resp.GetAccessToken(),
		SessionID:   resp.GetSessionId(),
		IssuedAt:    now,
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
	if err := ctx.Err(); err != nil {
		return Token{}, err
	}
	m.mu.Lock()
	terminal := m.terminalErr
	m.mu.Unlock()
	if terminal != nil {
		return Token{}, terminal
	}
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
	tok, err = m.store.Load(ctx)
	if err == nil && !tok.Valid(m.now(), 0) {
		return Token{}, fmt.Errorf("udb: renewed bearer is empty or expired")
	}
	return tok, err
}

// RefreshIfNeeded refreshes the stored token if it is stale, sharing one
// in-flight refresh among concurrent callers. If the token is already fresh it
// returns immediately.
func (m *TokenManager) RefreshIfNeeded(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	terminal := m.terminalErr
	m.mu.Unlock()
	if terminal != nil {
		return terminal
	}
	tok, err := m.store.Load(ctx)
	if err != nil {
		return err
	}
	if tok.Valid(m.now(), m.RefreshSkew) {
		return nil
	}

	m.mu.Lock()
	if m.terminalErr != nil {
		err := m.terminalErr
		m.mu.Unlock()
		return err
	}
	if m.inflight != nil {
		// A refresh is already running; wait for it and adopt its result.
		flight := m.inflight
		m.mu.Unlock()
		select {
		case <-flight.done:
			return flight.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Another caller may have rotated the credential after our initial Load,
	// completed its refresh, and cleared inflight before we acquired this lock.
	// Recheck the current token before becoming leader, never submit that retired
	// refresh token (the broker treats reuse as a revoked token family).
	tok, err = m.store.Load(ctx)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if tok.Valid(m.now(), m.RefreshSkew) {
		m.mu.Unlock()
		return nil
	}
	// We are the leader: start the in-flight refresh with the current credential.
	flight := &tokenRefreshFlight{done: make(chan struct{})}
	m.inflight = flight
	needsRecovery := m.needsRecovery
	m.mu.Unlock()

	rerr := m.renew(ctx, tok, needsRecovery)
	if rerr == nil && m.publish != nil {
		rerr = m.publish(ctx)
	}

	var binding *credentialBindingError
	m.mu.Lock()
	if errors.As(rerr, &binding) {
		m.terminalErr = rerr
	}
	// A managed next flight must not resubmit an ambiguously consumed token.
	// Successful login/verification/publication clears this retirement state.
	if m.recover != nil {
		m.needsRecovery = rerr != nil
	}
	flight.err = rerr
	m.inflight = nil
	close(flight.done)
	m.mu.Unlock()
	return rerr
}

// renew makes up to renewalAttempts attempts, waiting a jittered, doubling
// interval between them. Only a transient failure is retried.
func (m *TokenManager) renew(ctx context.Context, tok Token, needsRecovery bool) error {
	var lastErr error
	for attempt := 0; attempt < renewalAttempts; attempt++ {
		if attempt > 0 {
			if sleepContext(ctx, jitteredBackoff(attempt, m.backoff, renewalBackoffCap)) != nil {
				return lastErr
			}
			cur, err := m.store.Load(ctx)
			if err != nil {
				return err
			}
			if cur.Valid(m.now(), m.RefreshSkew) {
				return nil
			}
			tok = cur
		}
		err, decisive := m.renewOnce(ctx, tok, &needsRecovery)
		if err == nil {
			return nil
		}
		lastErr = err
		if !transientRenewalError(ctx, decisive) {
			return err
		}
	}
	return lastErr
}

// renewOnce refreshes, and when a session that can log in again (recover) fails
// to refresh, logs in again in the same attempt. A refresh that failed may have
// been consumed by the broker before its reply was lost, so such a session never
// submits that refresh token again. A manager with no recovery has nothing else
// to try: it repeats the refresh only after a transient failure. decisive is the
// failure that decides whether another attempt can help.
func (m *TokenManager) renewOnce(ctx context.Context, tok Token, needsRecovery *bool) (err, decisive error) {
	var rerr error
	if *needsRecovery && m.recover != nil {
		rerr = fmt.Errorf("udb: prior refresh outcome requires re-login")
	} else {
		rerr = m.doRefresh(ctx, tok)
		if rerr == nil {
			return nil, nil
		}
	}
	var binding *credentialBindingError
	if errors.As(rerr, &binding) || ctx.Err() != nil || m.recover == nil {
		return rerr, rerr
	}
	*needsRecovery = true
	fresh, err := m.recover(ctx)
	if err == nil && !fresh.Valid(m.now(), 0) {
		err = fmt.Errorf("udb: recovery returned an empty or expired bearer")
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = m.store.Save(ctx, fresh)
	}
	if err == nil {
		*needsRecovery = false
		return nil, nil
	}
	return fmt.Errorf("udb: renewal and re-login failed: %w", errors.Join(rerr, err)), err
}

// doRefresh performs the actual RefreshToken RPC and persists the new token.
func (m *TokenManager) doRefresh(ctx context.Context, prev Token) error {
	if m.auth == nil {
		return fmt.Errorf("udb: token refresh has no authentication client")
	}
	if m.recoveryOwner != nil {
		ctx = m.recoveryOwner.recoveryContext(ctx, credentialRefresh)
	}
	receivedAt := m.now()
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
		next.IssuedAt = receivedAt
		next.ExpiresAt = next.IssuedAt.Add(time.Duration(secs) * time.Second)
	}
	// Persist the ROTATED refresh token. The broker mints a new one on every
	// successful refresh and invalidates the presented one atomically — it is
	// single-use. Keeping `prev.RefreshToken` meant the second refresh submitted
	// a credential the broker had already revoked, so a long-running service
	// authenticated, refreshed once, and then failed with
	// `Unauthenticated: invalid credential` at the next boundary.
	//
	// Guarded on non-empty: the response omits it when the caller refreshed with
	// a legacy server-side session id rather than a token-family credential, and
	// blindly assigning would erase a working credential.
	if rotated := resp.GetRefreshToken(); rotated != "" {
		next.RefreshToken = rotated
	}
	if m.validate != nil {
		if err := m.validate(ctx, &next); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !next.Valid(m.now(), 0) {
		return fmt.Errorf("udb: refresh returned an empty or expired bearer")
	}
	return m.store.Save(ctx, next)
}
