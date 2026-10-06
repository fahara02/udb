package udbclient

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// TenantSessionPool keeps one connected client per tenant for a service that
// serves several tenants (each with its own service-account key). Clients are
// created on first use, shared afterwards, and replaced when their credential
// stops refreshing. Services used to hand-write this pool, each with its own
// locking and staleness rules.
type TenantSessionPool struct {
	connect func(ctx context.Context, tenantID string) (*Udb, error)

	mu       sync.Mutex
	sessions map[string]*Udb
	pending  map[string]*poolDial
	closed   bool
}

type poolDial struct {
	done chan struct{}
	u    *Udb
	err  error
}

// NewTenantSessionPool builds a pool whose clients come from connect.
func NewTenantSessionPool(connect func(ctx context.Context, tenantID string) (*Udb, error)) *TenantSessionPool {
	return &TenantSessionPool{
		connect:  connect,
		sessions: map[string]*Udb{},
		pending:  map[string]*poolDial{},
	}
}

// NewAPIKeyTenantPool is a pool where every tenant connects with base and its
// own API key from keys (tenant UUID → key). A tenant without a key is refused.
func NewAPIKeyTenantPool(base Config, keys map[string]string) *TenantSessionPool {
	return NewTenantSessionPool(func(ctx context.Context, tenantID string) (*Udb, error) {
		key, ok := keys[tenantID]
		if !ok || key == "" {
			return nil, fmt.Errorf("udb: tenant pool has no API key for tenant %q", tenantID)
		}
		cfg := base
		cfg.TenantID = tenantID
		cfg.Credentials = Credentials{APIKey: key}
		u, err := Connect(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if err := u.Verify(Expect{TenantID: tenantID}); err != nil {
			_ = u.Close()
			return nil, err
		}
		return u, nil
	})
}

// Get returns the tenant's client, connecting it on first use. Concurrent
// first calls for one tenant share a single connect. A client whose credential
// refresh is failing is closed and replaced.
func (p *TenantSessionPool) Get(ctx context.Context, tenantID string) (*Udb, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("udb: tenant pool is closed")
	}
	if u, ok := p.sessions[tenantID]; ok {
		if u.CredentialErr() == nil {
			p.mu.Unlock()
			return u, nil
		}
		delete(p.sessions, tenantID)
		go func() { _ = u.Close() }()
	}
	if dial, ok := p.pending[tenantID]; ok {
		p.mu.Unlock()
		select {
		case <-dial.done:
			return dial.u, dial.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	dial := &poolDial{done: make(chan struct{})}
	p.pending[tenantID] = dial
	p.mu.Unlock()

	dial.u, dial.err = p.connect(ctx, tenantID)

	p.mu.Lock()
	delete(p.pending, tenantID)
	if dial.err == nil {
		if p.closed {
			_ = dial.u.Close()
			dial.u, dial.err = nil, fmt.Errorf("udb: tenant pool is closed")
		} else {
			p.sessions[tenantID] = dial.u
		}
	}
	p.mu.Unlock()
	close(dial.done)
	return dial.u, dial.err
}

// Tenants lists the tenants with a live client, sorted.
func (p *TenantSessionPool) Tenants() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.sessions))
	for tenant := range p.sessions {
		out = append(out, tenant)
	}
	sort.Strings(out)
	return out
}

// Evict closes and forgets one tenant's client; the next Get reconnects.
func (p *TenantSessionPool) Evict(tenantID string) {
	p.mu.Lock()
	u, ok := p.sessions[tenantID]
	delete(p.sessions, tenantID)
	p.mu.Unlock()
	if ok {
		_ = u.Close()
	}
}

// Close closes every client. Later Gets fail.
func (p *TenantSessionPool) Close() error {
	p.mu.Lock()
	p.closed = true
	sessions := p.sessions
	p.sessions = map[string]*Udb{}
	p.mu.Unlock()
	var first error
	for _, u := range sessions {
		if err := u.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
