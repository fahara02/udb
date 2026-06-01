package udbclient

import (
	"context"
	"strings"
	"sync"
	"time"

	authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
)

// ── Stage 2: local SDK authorization cache (item 140) ────────────────────────
//
// AuthzCache wraps AuthClient.Can with a small in-process TTL cache keyed by the
// (principal, resource, action, purpose) tuple. The TTL comes from the server's
// Decision.cache_ttl_seconds, so UDB stays in control of how long a decision may
// be reused; a zero TTL is never cached. This cuts the per-request round-trip
// for hot allow/deny checks without weakening the authority's freshness control.

type cachedDecision struct {
	decision *authzv1.Decision
	expires  time.Time
}

// AuthzCache is a concurrency-safe decision cache over an AuthClient.
type AuthzCache struct {
	client *AuthClient
	mu     sync.RWMutex
	cache  map[string]cachedDecision
	now    func() time.Time // injectable clock for tests
}

// NewAuthzCache builds a cache over the given AuthClient.
func NewAuthzCache(client *AuthClient) *AuthzCache {
	return &AuthzCache{
		client: client,
		cache:  make(map[string]cachedDecision),
		now:    time.Now,
	}
}

func cacheKey(meta Metadata, resource *authzv1.ResourceRef, action, purpose string) string {
	subject := meta.UserID
	if subject == "" {
		subject = meta.ServiceIdentity
	}
	res := ""
	if resource != nil {
		res = resource.GetMessageType()
		if res == "" {
			res = resource.GetResourceName()
		}
		if res == "" {
			res = resource.GetTable()
		}
	}
	return strings.Join([]string{meta.TenantID, meta.ProjectID, subject, res, action, purpose}, "\x1f")
}

// Can answers the authorization question from the local cache when a fresh
// decision is present, otherwise it calls the server and caches the result for
// Decision.cache_ttl_seconds. The full Decision is returned for inspection.
func (a *AuthzCache) Can(ctx context.Context, resource *authzv1.ResourceRef, action, purpose string) (bool, *authzv1.Decision, error) {
	if purpose == "" {
		purpose = a.client.Meta.Purpose
	}
	key := cacheKey(a.client.Meta, resource, action, purpose)

	a.mu.RLock()
	entry, ok := a.cache[key]
	a.mu.RUnlock()
	if ok && a.now().Before(entry.expires) {
		return entry.decision.GetAllowed(), entry.decision, nil
	}

	allowed, decision, err := a.client.Can(ctx, resource, action, purpose)
	if err != nil {
		return false, nil, err
	}
	if ttl := decision.GetCacheTtlSeconds(); ttl > 0 {
		a.mu.Lock()
		a.cache[key] = cachedDecision{
			decision: decision,
			expires:  a.now().Add(time.Duration(ttl) * time.Second),
		}
		a.mu.Unlock()
	}
	return allowed, decision, nil
}

// Invalidate drops all cached decisions (e.g. after a known policy change).
func (a *AuthzCache) Invalidate() {
	a.mu.Lock()
	a.cache = make(map[string]cachedDecision)
	a.mu.Unlock()
}

// GetPolicyBundle fetches a signed policy bundle for the caller's tenant/project
// scope. SDKs can persist and verify it to drive a fully offline cache; the
// signature + expiry let a caller trust a cached bundle between fetches.
func (c *AuthClient) GetPolicyBundle(ctx context.Context) (*authzv1.SignedPolicyBundle, error) {
	resp, err := c.Authz.GetPolicyBundle(c.Context(ctx), &authzv1.PolicyBundleRequest{
		TenantId:  c.Meta.TenantID,
		ProjectId: c.Meta.ProjectID,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetBundle(), nil
}
