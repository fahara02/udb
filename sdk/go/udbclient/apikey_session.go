package udbclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	"google.golang.org/protobuf/proto"
)

// Service-account API keys are exchanged, never sent raw.
//
// The broker's native control plane refuses a raw `x-api-key` (and refuses a
// request that carries two credential types), so a service that connected with
// Credentials{APIKey} used to have to call AuthenticateAPIKey itself, install
// the bearer, clear the key, and re-exchange before the bearer expired —
// service accounts get no refresh token. Every consumer wrote that loop; one
// that let it lapse stopped every call it made. NewUdb now does it: the key is
// exchanged once at connect, the bearer is carried on every connection, and a
// background loop re-exchanges at 4/5 of the bearer's life, retrying on failure
// until Close.

// apiKeySession is the exchanged-bearer state a *Udb owns: a service-account
// key exchanged for a service bearer, or (ForTenant) the home bearer exchanged
// for a member-tenant token. Neither kind of bearer has a refresh token, so
// both are kept fresh by exchanging again.
type apiKeySession struct {
	key string
	// exchange performs one exchange; nil exchanges key with AuthenticateAPIKey.
	exchange func(ctx context.Context) (exchangedBearer, error)
	// label names the exchange in errors ("exchange API key" by default).
	label string
	retry time.Duration
	// backoff is the first wait between attempts of one exchange (default 1s);
	// tests shorten it.
	backoff time.Duration

	mu         sync.Mutex
	expiresAt  time.Time
	refreshErr error
	principal  *authnv1.Principal
	identity   *Metadata
	bearer     string
	renewAt    time.Time
	inflight   *apiKeyRenewalFlight
	// rejected: the broker refused the current bearer before its expiry. Until
	// a re-exchange succeeds a failed one refuses calls locally, and wake makes
	// the background loop retry with backoff instead of one exchange per call.
	rejected bool
	wake     chan struct{}

	stop     chan struct{}
	stopOnce sync.Once
}

// exchangedBearer is one exchange's result before it is verified and installed.
type exchangedBearer struct {
	accessToken   string
	expiresAtUnix int64
	principal     *authnv1.Principal
}

type apiKeyRenewalFlight struct {
	done      chan struct{}
	expiresAt time.Time
	err       error
}

const (
	apiKeyRetryDefault  = 5 * time.Second
	apiKeyRefreshFloor  = 1 * time.Second
	apiKeyExchangeLimit = 30 * time.Second
)

// startAPIKeyExchange swaps cfg's API key for a service bearer and keeps it
// fresh. The key travels only inside the typed exchange request; no call ever
// carries it as a header.
func (u *Udb) startAPIKeyExchange(ctx context.Context, key string, retry time.Duration) error {
	if retry <= 0 {
		retry = apiKeyRetryDefault
	}
	return u.startBearerExchange(ctx, &apiKeySession{key: key, retry: retry})
}

// startBearerExchange performs s's first exchange, installs its bearer as the
// connection's credential, and keeps it fresh until Close. A bearer the broker
// refuses before its expiry is exchanged again; until that succeeds a failed
// exchange refuses calls locally instead of sending the refused bearer.
func (u *Udb) startBearerExchange(ctx context.Context, s *apiKeySession) error {
	if s.retry <= 0 {
		s.retry = apiKeyRetryDefault
	}
	if s.label == "" {
		s.label = "exchange API key"
	}
	s.stop, s.wake = make(chan struct{}), make(chan struct{}, 1)
	u.apiKey = s
	exp, err := u.exchangeAPIKey(ctx, s)
	if err != nil {
		return err
	}
	u.credential.provider.Store(&credentialProvider{
		resolve: func(ctx context.Context) (string, error) {
			if _, err := u.renewAPIKey(ctx, s, false); err != nil {
				return "", renewalRefusal(ctx, err)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if !time.Now().Before(s.expiresAt) {
				return "", credentialRefusal(sessionExpired(fmt.Errorf("%s: the bearer is expired", s.label)))
			}
			return s.bearer, nil
		},
		refusal: func() error {
			s.mu.Lock()
			defer s.mu.Unlock()
			var binding *credentialBindingError
			if s.refreshErr != nil && (errors.As(s.refreshErr, &binding) || s.rejected || !time.Now().Before(s.expiresAt)) {
				return credentialRefusal(sessionExpired(s.refreshErr))
			}
			return nil
		},
		current: func() string {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.bearer
		},
		invalidate: func(bearer string) {
			// The broker refused this bearer before its local expiry (a
			// restarted broker, a revoked grant): exchange the key again on the
			// next call instead of waiting for the renewal boundary.
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.bearer == bearer {
				s.renewAt, s.rejected = time.Time{}, true
				select {
				case s.wake <- struct{}{}:
				default:
				}
			}
		},
	})
	go u.keepAPIKeyBearer(s, exp)
	return nil
}

func (u *Udb) exchangeAPIKey(ctx context.Context, s *apiKeySession) (time.Time, error) {
	return u.renewAPIKey(ctx, s, true)
}

func (u *Udb) renewAPIKey(ctx context.Context, s *apiKeySession, force bool) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	s.mu.Lock()
	if flight := s.inflight; flight != nil {
		s.mu.Unlock()
		select {
		case <-flight.done:
			return flight.expiresAt, flight.err
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		}
	}
	if !force && s.refreshErr == nil && s.bearer != "" && time.Now().Before(s.renewAt) {
		exp := s.expiresAt
		s.mu.Unlock()
		return exp, nil
	}
	flight := &apiKeyRenewalFlight{done: make(chan struct{})}
	s.inflight = flight
	s.mu.Unlock()
	exp, err := u.exchangeWithRetry(ctx, s)
	s.mu.Lock()
	if err != nil {
		s.refreshErr = err
	}
	flight.expiresAt, flight.err = exp, err
	s.inflight = nil
	close(flight.done)
	s.mu.Unlock()
	return exp, err
}

// exchangeWithRetry re-authenticates with the key, making up to renewalAttempts
// attempts with a jittered, doubling wait between them. Only a transient failure
// (broker unreachable, overloaded, or the reply lost) is retried.
func (u *Udb) exchangeWithRetry(ctx context.Context, s *apiKeySession) (time.Time, error) {
	s.mu.Lock()
	backoff := s.backoff
	s.mu.Unlock()
	var lastErr error
	for attempt := 0; attempt < renewalAttempts; attempt++ {
		if attempt > 0 {
			if sleepContext(ctx, jitteredBackoff(attempt, backoff, renewalBackoffCap)) != nil {
				return time.Time{}, lastErr
			}
			select {
			case <-s.stop:
				return time.Time{}, lastErr
			default:
			}
		}
		exp, err := u.performAPIKeyExchange(ctx, s)
		if err == nil {
			return exp, nil
		}
		lastErr = err
		// Report each failed attempt as it happens, so CredentialErr shows the problem
		// while the retries continue rather than only once they are all spent.
		s.mu.Lock()
		s.refreshErr = err
		s.mu.Unlock()
		if !transientRenewalError(ctx, err) {
			break
		}
	}
	return time.Time{}, lastErr
}

func (u *Udb) performAPIKeyExchange(ctx context.Context, s *apiKeySession) (time.Time, error) {
	if _, has := ctx.Deadline(); !has {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, apiKeyExchangeLimit)
		defer cancel()
	}
	exchange := s.exchange
	if exchange == nil {
		exchange = func(ctx context.Context) (exchangedBearer, error) { return u.authenticateAPIKey(ctx, s.key) }
	}
	res, err := exchange(ctx)
	if err != nil {
		return time.Time{}, err
	}
	principal := res.principal
	if principal == nil {
		return time.Time{}, fmt.Errorf("udb: %s: the broker returned no verified principal", s.label)
	}
	exp := time.Unix(res.expiresAtUnix, 0)
	if !time.Now().Before(exp) {
		return time.Time{}, fmt.Errorf("udb: %s: returned bearer is expired", s.label)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	select {
	case <-s.stop:
		return time.Time{}, context.Canceled
	default:
	}
	if s.identity != nil {
		if err := principalCredentialBinding(s.principal).validate(principal); err != nil {
			return time.Time{}, &credentialBindingError{cause: err}
		}
		if err := u.renewPrincipal(principal, res.accessToken, *s.identity); err != nil {
			return time.Time{}, &credentialBindingError{cause: fmt.Errorf("udb: %s: %w", s.label, err)}
		}
	} else {
		u.adoptPrincipal(principal, res.accessToken)
		identity := u.Meta
		identity.Scopes = append([]string(nil), identity.Scopes...)
		s.identity = &identity
	}
	s.expiresAt = exp
	s.renewAt = time.Now().Add(apiKeyRefreshWait(time.Until(exp)))
	s.bearer = "Bearer " + res.accessToken
	s.refreshErr, s.rejected = nil, false
	s.principal = proto.Clone(principal).(*authnv1.Principal)
	return exp, nil
}

// authenticateAPIKey exchanges a service-account key for a service bearer. The
// key travels only inside the typed request.
func (u *Udb) authenticateAPIKey(ctx context.Context, key string) (exchangedBearer, error) {
	res, err := u.Auth.AuthenticateAPIKey(u.recoveryContext(ctx, credentialAuthenticate), key)
	if err != nil {
		return exchangedBearer{}, fmt.Errorf("udb: exchange API key for a service bearer: %w", err)
	}
	if res.GetAccessToken() == "" || res.GetExpiresAtUnix() == 0 {
		return exchangedBearer{}, errors.New("udb: exchange API key: the broker returned no bearer (is the key active and bound to a service account grant?)")
	}
	return exchangedBearer{accessToken: res.GetAccessToken(), expiresAtUnix: res.GetExpiresAtUnix(), principal: res.GetPrincipal()}, nil
}

// keepAPIKeyBearer re-exchanges at 4/5 of the bearer's life. A failed exchange
// is reported by CredentialErr and retried after a jittered exponential backoff
// (starting at the smaller of s.retry and 1s, doubling to 60s). Demand joins this
// same flight and refuses locally when renewal fails; it never emits old auth.
func (u *Udb) keepAPIKeyBearer(s *apiKeySession, exp time.Time) {
	for {
		wait := apiKeyRefreshWait(time.Until(exp))
		select {
		case <-s.stop:
			return
		case <-time.After(wait):
		case <-s.wake:
		}
		for failures := 1; ; failures++ {
			base := context.Background()
			if u.credential != nil {
				base = u.credential.ctx
			}
			next, err := u.renewAPIKey(base, s, false)
			if err == nil {
				exp = next
				break
			}
			s.mu.Lock()
			s.refreshErr = err
			s.mu.Unlock()
			select {
			case <-s.stop:
				return
			case <-time.After(jitteredBackoff(failures, min(s.retry, renewalBackoffBase), renewalBackoffCap)):
			}
		}
	}
}

// A healthy bearer must reach its renewal boundary before expiry even when the
// absolute Unix expiry leaves less than one second. Floor only an already-due
// attempt; failed exchanges retain the separate retry cadence above.
func apiKeyRefreshWait(remaining time.Duration) time.Duration {
	if remaining <= 0 {
		return apiKeyRefreshFloor
	}
	return remaining - remaining/5
}

// CredentialErr reports why the background credential refresh is failing, or
// nil while the connection's credential is current. Use it in a readiness probe:
// A failed demand renewal refuses that call without emitting the old bearer.
func (u *Udb) CredentialErr() error {
	if u.credential != nil {
		if err := u.credential.closedError(); err != nil {
			return err
		}
	}
	if u.apiKey == nil {
		return nil
	}
	u.apiKey.mu.Lock()
	defer u.apiKey.mu.Unlock()
	return u.apiKey.refreshErr
}

// BearerExpiresAt is when the current exchanged bearer expires (zero when the
// connection does not exchange an API key).
func (u *Udb) BearerExpiresAt() time.Time {
	if u.apiKey == nil {
		return time.Time{}
	}
	u.apiKey.mu.Lock()
	defer u.apiKey.mu.Unlock()
	return u.apiKey.expiresAt
}

// Principal is the verified principal the broker returned for the exchanged
// API key (nil when the connection does not exchange one).
func (u *Udb) Principal() *authnv1.Principal {
	if u.apiKey == nil {
		return nil
	}
	u.apiKey.mu.Lock()
	defer u.apiKey.mu.Unlock()
	if u.apiKey.principal == nil {
		return nil
	}
	return proto.Clone(u.apiKey.principal).(*authnv1.Principal)
}

func (u *Udb) stopAPIKeyRefresh() {
	if u.apiKey != nil {
		u.apiKey.stopOnce.Do(func() { close(u.apiKey.stop) })
	}
}
