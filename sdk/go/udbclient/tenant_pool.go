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
	done             chan struct{}
	ctx              context.Context
	cancel           context.CancelFunc
	stopCancellation func() bool
	completed        bool // guarded by the pool mutex; results never change after done
	u                *Udb
	err              error
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
	ownedKeys := make(map[string]string, len(keys))
	for tenant, key := range keys {
		ownedKeys[tenant] = key
	}
	return NewTenantSessionPool(func(ctx context.Context, tenantID string) (*Udb, error) {
		key, ok := ownedKeys[tenantID]
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
// refresh is failing is closed and replaced. The initiating caller owns the
// connect context; followers can cancel their own wait without canceling it.
func (p *TenantSessionPool) Get(ctx context.Context, tenantID string) (*Udb, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("udb: tenant pool is closed")
	}
	var stale *Udb
	if u, ok := p.sessions[tenantID]; ok {
		if u.CredentialErr() == nil {
			if err := ctx.Err(); err != nil {
				p.mu.Unlock()
				return nil, err
			}
			p.mu.Unlock()
			return u, nil
		}
		delete(p.sessions, tenantID)
		stale = u
	}
	if dial, ok := p.pending[tenantID]; ok {
		if err := dial.ctx.Err(); err != nil {
			// Do not make a new caller wait for a canceled connector that ignores
			// its context. Its eventual result belongs to the retired generation.
			p.completeDialLocked(tenantID, dial, nil, err)
		} else {
			p.mu.Unlock()
			closeStalePoolClient(stale)
			return p.waitDial(ctx, tenantID, dial)
		}
	}
	if p.connect == nil {
		p.mu.Unlock()
		closeStalePoolClient(stale)
		return nil, fmt.Errorf("udb: tenant pool connect is required")
	}
	dialCtx, cancel := context.WithCancel(ctx)
	dial := &poolDial{done: make(chan struct{}), ctx: dialCtx, cancel: cancel}
	p.pending[tenantID] = dial
	dial.stopCancellation = context.AfterFunc(dialCtx, func() {
		p.cancelDial(tenantID, dial, dialCtx.Err())
	})
	p.mu.Unlock()
	closeStalePoolClient(stale)
	go p.runDial(tenantID, dial)
	return p.waitDial(ctx, tenantID, dial)
}

// Detached failed clients must not live until a replacement connector returns.
// Keep their cleanup independent: a blocked transport Close cannot hold up a
// new generation or an Evict/Close that cancels its replacement attempt.
func closeStalePoolClient(u *Udb) {
	if u != nil {
		go func() { _ = u.Close() }()
	}
}

func (p *TenantSessionPool) waitDial(ctx context.Context, tenantID string, dial *poolDial) (*Udb, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-dial.done:
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if dial.err != nil {
		return nil, dial.err
	}
	if p.closed {
		return nil, fmt.Errorf("udb: tenant pool is closed")
	}
	if p.sessions[tenantID] != dial.u {
		return nil, tenantPoolEvicted(tenantID)
	}
	return dial.u, dial.err
}

// Every result is fenced by the exact pending object. Eviction, closure and
// cancellation publish the old flight's refusal immediately; its late worker
// cannot delete a replacement flight or install a client into that generation.
func (p *TenantSessionPool) completeDialLocked(tenantID string, dial *poolDial, u *Udb, err error) {
	if dial.completed {
		return
	}
	if p.pending[tenantID] == dial {
		delete(p.pending, tenantID)
	}
	dial.u, dial.err, dial.completed = u, err, true
	close(dial.done)
}

func (p *TenantSessionPool) cancelDial(tenantID string, dial *poolDial, err error) {
	p.mu.Lock()
	if p.pending[tenantID] == dial {
		p.completeDialLocked(tenantID, dial, nil, err)
	}
	p.mu.Unlock()
}

func (p *TenantSessionPool) runDial(tenantID string, dial *poolDial) {
	var u *Udb
	err := dial.ctx.Err()
	if err == nil {
		u, err = p.connect(dial.ctx, tenantID)
	}
	if err == nil && u == nil {
		err = fmt.Errorf("udb: tenant pool connect returned a nil client")
	}
	installed := false
	p.mu.Lock()
	if !dial.completed && p.pending[tenantID] == dial {
		if p.closed {
			err = fmt.Errorf("udb: tenant pool is closed")
		} else if cancelErr := dial.ctx.Err(); cancelErr != nil {
			err = cancelErr
		}
		if err == nil {
			p.sessions[tenantID] = u
			installed = true
		}
		result := u
		if !installed {
			result = nil
		}
		p.completeDialLocked(tenantID, dial, result, err)
	}
	p.mu.Unlock()
	dial.stopCancellation()
	dial.cancel()
	// Closing can perform transport cleanup. It never runs under the pool lock.
	if !installed && u != nil {
		_ = u.Close()
	}
}

func tenantPoolEvicted(tenantID string) error {
	return fmt.Errorf("udb: tenant pool session for tenant %q was evicted", tenantID)
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
	dial := p.pending[tenantID]
	if dial != nil {
		p.completeDialLocked(tenantID, dial, nil, tenantPoolEvicted(tenantID))
	}
	p.mu.Unlock()
	if dial != nil {
		dial.stopCancellation()
		dial.cancel()
	}
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
	dials := make([]*poolDial, 0, len(p.pending))
	for tenant, dial := range p.pending {
		p.completeDialLocked(tenant, dial, nil, fmt.Errorf("udb: tenant pool is closed"))
		dials = append(dials, dial)
	}
	p.mu.Unlock()
	for _, dial := range dials {
		dial.stopCancellation()
		dial.cancel()
	}
	var first error
	for _, u := range sessions {
		if err := u.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
