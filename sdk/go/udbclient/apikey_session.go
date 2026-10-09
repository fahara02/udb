package udbclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
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

// apiKeySession is the exchanged-key state a *Udb owns.
type apiKeySession struct {
	key   string
	retry time.Duration

	mu         sync.Mutex
	expiresAt  time.Time
	refreshErr error
	principal  *authnv1.Principal
	identity   *Metadata

	stop     chan struct{}
	stopOnce sync.Once
}

const (
	apiKeyRetryDefault  = 5 * time.Second
	apiKeyRefreshFloor  = 1 * time.Second
	apiKeyExchangeLimit = 30 * time.Second
)

// startAPIKeyExchange swaps cfg's API key for a service bearer and keeps it
// fresh. The key is cleared from the outgoing headers first, so no call (the
// exchange included) ever carries the raw key.
func (u *Udb) startAPIKeyExchange(ctx context.Context, key string, retry time.Duration) error {
	if retry <= 0 {
		retry = apiKeyRetryDefault
	}
	u.Generated.SetAPIKey("")
	s := &apiKeySession{key: key, retry: retry, stop: make(chan struct{})}
	exp, err := u.exchangeAPIKey(ctx, s)
	if err != nil {
		return err
	}
	u.apiKey = s
	go u.keepAPIKeyBearer(s, exp)
	return nil
}

func (u *Udb) exchangeAPIKey(ctx context.Context, s *apiKeySession) (time.Time, error) {
	if _, has := ctx.Deadline(); !has {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, apiKeyExchangeLimit)
		defer cancel()
	}
	res, err := u.Auth.AuthenticateAPIKey(ctx, s.key)
	if err != nil {
		return time.Time{}, fmt.Errorf("udb: exchange API key for a service bearer: %w", err)
	}
	if res.GetAccessToken() == "" || res.GetExpiresAtUnix() == 0 {
		return time.Time{}, errors.New("udb: exchange API key: the broker returned no bearer (is the key active and bound to a service account grant?)")
	}
	principal := res.GetPrincipal()
	if principal == nil {
		return time.Time{}, errors.New("udb: exchange API key: the broker returned no verified principal")
	}
	exp := time.Unix(res.GetExpiresAtUnix(), 0)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.identity != nil {
		if err := u.renewPrincipal(principal, res.GetAccessToken(), *s.identity); err != nil {
			return time.Time{}, fmt.Errorf("udb: exchange API key: %w", err)
		}
	} else {
		u.adoptPrincipal(principal, res.GetAccessToken())
		identity := u.Meta
		identity.Scopes = append([]string(nil), identity.Scopes...)
		s.identity = &identity
	}
	s.expiresAt = exp
	s.refreshErr = nil
	s.principal = principal
	return exp, nil
}

// keepAPIKeyBearer re-exchanges at 4/5 of the bearer's life. A failed exchange
// is retried every s.retry and reported by CredentialErr; once the bearer has
// actually expired, calls fail at the broker with Unauthenticated until an
// exchange succeeds again.
func (u *Udb) keepAPIKeyBearer(s *apiKeySession, exp time.Time) {
	for {
		wait := apiKeyRefreshWait(time.Until(exp))
		select {
		case <-s.stop:
			return
		case <-time.After(wait):
		}
		for {
			next, err := u.exchangeAPIKey(context.Background(), s)
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
			case <-time.After(s.retry):
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
// a non-nil error means calls will start failing once the current bearer expires.
func (u *Udb) CredentialErr() error {
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
	return u.apiKey.principal
}

func (u *Udb) stopAPIKeyRefresh() {
	if u.apiKey != nil {
		u.apiKey.stopOnce.Do(func() { close(u.apiKey.stop) })
	}
}
