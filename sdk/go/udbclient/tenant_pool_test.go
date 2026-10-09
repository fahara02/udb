package udbclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const tenantPoolFixtureID = "00000000-0000-0000-0000-000000000001"

type tenantPoolGate struct {
	ch   chan struct{}
	once sync.Once
}

func newTenantPoolGate(t *testing.T) *tenantPoolGate {
	t.Helper()
	gate := &tenantPoolGate{ch: make(chan struct{})}
	t.Cleanup(gate.open)
	return gate
}

func (gate *tenantPoolGate) open() { gate.once.Do(func() { close(gate.ch) }) }

type tenantPoolResult struct {
	u   *Udb
	err error
}

func tenantPoolGetAsync(pool *TenantSessionPool, ctx context.Context) <-chan tenantPoolResult {
	result := make(chan tenantPoolResult, 1)
	go func() { u, err := pool.Get(ctx, tenantPoolFixtureID); result <- tenantPoolResult{u, err} }()
	return result
}

func tenantPoolAwait(t *testing.T, ctx context.Context, result <-chan tenantPoolResult) tenantPoolResult {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-ctx.Done():
		t.Fatal("pool Get did not complete within the owned test context")
		return tenantPoolResult{}
	}
}

func tenantPoolWait(t *testing.T, ctx context.Context, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatal("owned pool fixture did not reach its expected lifecycle boundary")
	}
}

// Done is evaluated only after Get has selected its flight and released the
// pool lock. This observes a follower's actual wait without sleeps or polling.
type tenantPoolWaitingContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *tenantPoolWaitingContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func tenantPoolFollower(pool *TenantSessionPool, ctx context.Context) (<-chan tenantPoolResult, <-chan struct{}) {
	waiter := &tenantPoolWaitingContext{Context: ctx, waiting: make(chan struct{})}
	return tenantPoolGetAsync(pool, waiter), waiter.waiting
}

func tenantPoolOwnedClient(t *testing.T, target string) *Udb {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u, err := Connect(ctx, Config{Target: target, TenantID: tenantPoolFixtureID, Purpose: "test.pool", Credentials: Credentials{Bearer: "pool-owned-fixture"}})
	if err != nil {
		t.Fatal("owned TCP pool client construction failed")
	}
	t.Cleanup(func() { _ = u.Close() })
	return u
}

func tenantPoolAssertRefused(t *testing.T, result tenantPoolResult, reason string) {
	t.Helper()
	if result.u != nil || result.err == nil || !strings.Contains(result.err.Error(), reason) {
		t.Fatalf("retired pool generation must refuse with %q", reason)
	}
}

func TestTenantSessionPoolEvictFencesPendingGenerationAndLateClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	target := serveKeyFakes(t, &keyAuthn{ttl: time.Hour}, &mdBroker{})
	oldClient, newClient := tenantPoolOwnedClient(t, target), tenantPoolOwnedClient(t, target)
	oldRelease, newRelease, closeRelease := newTenantPoolGate(t), newTenantPoolGate(t), newTenantPoolGate(t)
	oldEntered, newEntered := make(chan context.Context, 1), make(chan struct{})
	closeEntered := make(chan struct{})
	originalCancel := oldClient.credential.cancel
	oldClient.credential.cancel = func() {
		originalCancel()
		close(closeEntered)
		<-closeRelease.ch
	}
	var calls atomic.Int32
	pool := NewTenantSessionPool(func(dialCtx context.Context, _ string) (*Udb, error) {
		switch calls.Add(1) {
		case 1:
			oldEntered <- dialCtx
			// Deliberately return a real owned client after cancellation, proving
			// the generation fence also handles non-cooperative connectors.
			<-oldRelease.ch
			return oldClient, nil
		case 2:
			close(newEntered)
			select {
			case <-newRelease.ch:
				return newClient, nil
			case <-dialCtx.Done():
				return nil, dialCtx.Err()
			}
		default:
			return nil, fmt.Errorf("unexpected extra pool dial")
		}
	})
	t.Cleanup(func() { _ = pool.Close() })
	leader := tenantPoolGetAsync(pool, ctx)
	var oldContext context.Context
	select {
	case oldContext = <-oldEntered:
	case <-ctx.Done():
		t.Fatal("first owned pool dial did not start")
	}
	follower, waiting := tenantPoolFollower(pool, ctx)
	tenantPoolWait(t, ctx, waiting)
	pool.Evict(tenantPoolFixtureID)
	tenantPoolAssertRefused(t, tenantPoolAwait(t, ctx, leader), "evicted")
	tenantPoolAssertRefused(t, tenantPoolAwait(t, ctx, follower), "evicted")
	tenantPoolWait(t, ctx, oldContext.Done())
	newLeader := tenantPoolGetAsync(pool, ctx)
	tenantPoolWait(t, ctx, newEntered)
	oldRelease.open()
	tenantPoolWait(t, ctx, closeEntered)
	// A late Close is intentionally held. Joining the replacement must still
	// acquire the pool lock, and must not start a third connection attempt.
	newFollower, newWaiting := tenantPoolFollower(pool, ctx)
	tenantPoolWait(t, ctx, newWaiting)
	if calls.Load() != 2 {
		t.Fatal("late completion deleted the replacement pending generation")
	}
	newRelease.open()
	for _, result := range []<-chan tenantPoolResult{newLeader, newFollower} {
		value := tenantPoolAwait(t, ctx, result)
		if value.err != nil || value.u != newClient {
			t.Fatal("replacement flight was overwritten by the retired result")
		}
	}
	closeRelease.open()
	closed := make(chan struct{})
	go func() { _ = oldClient.Close(); close(closed) }()
	tenantPoolWait(t, ctx, closed)
	if tenants := pool.Tenants(); len(tenants) != 1 || tenants[0] != tenantPoolFixtureID {
		t.Fatal("only the replacement tenant session should be installed")
	}
}

func TestTenantSessionPoolCloseRefusesWaitersBeforeLateDialReturns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	target := serveKeyFakes(t, &keyAuthn{ttl: time.Hour}, &mdBroker{})
	client := tenantPoolOwnedClient(t, target)
	release := newTenantPoolGate(t)
	entered := make(chan context.Context, 1)
	pool := NewTenantSessionPool(func(dialCtx context.Context, _ string) (*Udb, error) {
		entered <- dialCtx
		<-release.ch
		return client, nil
	})
	t.Cleanup(func() { _ = pool.Close() })
	leader := tenantPoolGetAsync(pool, ctx)
	var dialCtx context.Context
	select {
	case dialCtx = <-entered:
	case <-ctx.Done():
		t.Fatal("owned pool dial did not start")
	}
	follower, waiting := tenantPoolFollower(pool, ctx)
	tenantPoolWait(t, ctx, waiting)
	if err := pool.Close(); err != nil {
		t.Fatal("pending-only pool Close failed")
	}
	tenantPoolAssertRefused(t, tenantPoolAwait(t, ctx, leader), "closed")
	tenantPoolAssertRefused(t, tenantPoolAwait(t, ctx, follower), "closed")
	tenantPoolWait(t, ctx, dialCtx.Done())
	if u, err := pool.Get(ctx, tenantPoolFixtureID); u != nil || err == nil {
		t.Fatal("closed pool accepted a new Get")
	}
	release.open()
	tenantPoolWait(t, ctx, client.credential.ctx.Done())
	if len(pool.Tenants()) != 0 {
		t.Fatal("late client resurrected a closed pool")
	}
}

func TestTenantSessionPoolStaleCleanupDoesNotWaitForReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	authn := &keyAuthn{ttl: time.Hour}
	target := serveKeyFakes(t, authn, &mdBroker{})
	stale, err := Connect(ctx, Config{Target: target, TenantID: tenantPoolFixtureID, Purpose: "test.pool.stale", Credentials: Credentials{APIKey: "svc-key"}})
	if err != nil {
		t.Fatal("owned API-key TCP client construction failed")
	}
	t.Cleanup(func() { _ = stale.Close() })
	late, replacement := tenantPoolOwnedClient(t, target), tenantPoolOwnedClient(t, target)
	dialRelease, closeRelease := newTenantPoolGate(t), newTenantPoolGate(t)
	dialEntered, closeEntered := make(chan struct{}), make(chan struct{})
	originalCancel := stale.credential.cancel
	stale.credential.cancel = func() {
		originalCancel()
		close(closeEntered)
		select {
		case <-closeRelease.ch:
		case <-ctx.Done():
		}
	}
	var calls atomic.Int32
	pool := NewTenantSessionPool(func(context.Context, string) (*Udb, error) {
		switch calls.Add(1) {
		case 1:
			return stale, nil
		case 2:
			close(dialEntered)
			<-dialRelease.ch
			return late, nil
		case 3:
			return replacement, nil
		default:
			return nil, fmt.Errorf("unexpected extra pool dial")
		}
	})
	t.Cleanup(func() { _ = pool.Close() })
	// Release a cached client's deliberately blocked Close before pool cleanup
	// on any failed assertion as well as on the successful path below.
	t.Cleanup(closeRelease.open)
	if first, err := pool.Get(ctx, tenantPoolFixtureID); err != nil || first != stale {
		t.Fatal("initial owned client was not cached")
	}
	authn.mu.Lock()
	authn.fail = true
	authn.mu.Unlock()
	// Create the readiness failure through the real exchange entrypoint and
	// TCP server, without assigning a synthetic pool or credential error.
	if _, err := stale.exchangeAPIKey(ctx, stale.apiKey); err == nil || stale.CredentialErr() == nil {
		t.Fatal("actual API-key refusal did not mark the cached client stale")
	}
	leader := tenantPoolGetAsync(pool, ctx)
	tenantPoolWait(t, ctx, dialEntered)
	tenantPoolWait(t, ctx, closeEntered)
	pool.Evict(tenantPoolFixtureID)
	tenantPoolAssertRefused(t, tenantPoolAwait(t, ctx, leader), "evicted")
	value := tenantPoolAwait(t, ctx, tenantPoolGetAsync(pool, ctx))
	if value.err != nil || value.u != replacement || calls.Load() != 3 {
		t.Fatal("blocked stale Close or replacement dial held up a new generation")
	}
	closeRelease.open()
	dialRelease.open()
	tenantPoolWait(t, ctx, late.credential.ctx.Done())
	if cached, err := pool.Get(ctx, tenantPoolFixtureID); err != nil || cached != replacement {
		t.Fatal("late stale replacement altered the current client")
	}
}

func TestTenantSessionPoolInitiatingCancellationAllowsNewGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	target := serveKeyFakes(t, &keyAuthn{ttl: time.Hour}, &mdBroker{})
	oldClient, newClient := tenantPoolOwnedClient(t, target), tenantPoolOwnedClient(t, target)
	release := newTenantPoolGate(t)
	entered := make(chan struct{})
	var calls atomic.Int32
	pool := NewTenantSessionPool(func(context.Context, string) (*Udb, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release.ch
			return oldClient, nil
		}
		return newClient, nil
	})
	t.Cleanup(func() { _ = pool.Close() })
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	defer leaderCancel()
	leader := tenantPoolGetAsync(pool, leaderCtx)
	tenantPoolWait(t, ctx, entered)
	follower, waiting := tenantPoolFollower(pool, ctx)
	tenantPoolWait(t, ctx, waiting)
	leaderCancel()
	for _, result := range []<-chan tenantPoolResult{leader, follower} {
		value := tenantPoolAwait(t, ctx, result)
		if value.u != nil || !errors.Is(value.err, context.Canceled) {
			t.Fatal("canceled owned dial must refuse its prior generation")
		}
	}
	value := tenantPoolAwait(t, ctx, tenantPoolGetAsync(pool, ctx))
	if value.err != nil || value.u != newClient || calls.Load() != 2 {
		t.Fatal("new Get waited for or reused the canceled connector")
	}
	release.open()
	tenantPoolWait(t, ctx, oldClient.credential.ctx.Done())
	if cached, err := pool.Get(ctx, tenantPoolFixtureID); err != nil || cached != newClient {
		t.Fatal("canceled late result replaced the new cached client")
	}
}

func TestTenantSessionPoolFollowerCancellationAndCanceledCacheGet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	target := serveKeyFakes(t, &keyAuthn{ttl: time.Hour}, &mdBroker{})
	client := tenantPoolOwnedClient(t, target)
	release := newTenantPoolGate(t)
	entered := make(chan context.Context, 1)
	var calls atomic.Int32
	pool := NewTenantSessionPool(func(dialCtx context.Context, _ string) (*Udb, error) {
		calls.Add(1)
		entered <- dialCtx
		select {
		case <-release.ch:
			return client, nil
		case <-dialCtx.Done():
			return nil, dialCtx.Err()
		}
	})
	t.Cleanup(func() { _ = pool.Close() })
	leader := tenantPoolGetAsync(pool, ctx)
	var dialCtx context.Context
	select {
	case dialCtx = <-entered:
	case <-ctx.Done():
		t.Fatal("owned dial did not start")
	}
	followerCtx, followerCancel := context.WithCancel(ctx)
	defer followerCancel()
	follower, waiting := tenantPoolFollower(pool, followerCtx)
	tenantPoolWait(t, ctx, waiting)
	followerCancel()
	value := tenantPoolAwait(t, ctx, follower)
	if value.u != nil || !errors.Is(value.err, context.Canceled) || dialCtx.Err() != nil {
		t.Fatal("canceling a follower must leave the initiating dial alive")
	}
	release.open()
	value = tenantPoolAwait(t, ctx, leader)
	if value.err != nil || value.u != client || calls.Load() != 1 {
		t.Fatal("the initiating caller lost its shared client")
	}
	if u, err := pool.Get(followerCtx, tenantPoolFixtureID); u != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("a canceled Get must not receive a cached client")
	}
}

func TestTenantSessionPoolRefusesNilSuccessfulConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var calls atomic.Int32
	pool := NewTenantSessionPool(func(context.Context, string) (*Udb, error) { calls.Add(1); return nil, nil })
	defer pool.Close()
	for range 2 {
		u, err := pool.Get(ctx, tenantPoolFixtureID)
		if u != nil || err == nil || !strings.Contains(err.Error(), "nil client") {
			t.Fatal("nil successful connection must be refused without caching")
		}
	}
	if len(pool.Tenants()) != 0 || calls.Load() != 2 {
		t.Fatal("nil client was cached or blocked a later retry")
	}
}

func TestTenantSessionPoolReconnectsAfterReturnedClientClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	target := serveKeyFakes(t, &keyAuthn{ttl: time.Hour}, &mdBroker{})
	initial, replacement := tenantPoolOwnedClient(t, target), tenantPoolOwnedClient(t, target)
	var calls atomic.Int32
	pool := NewTenantSessionPool(func(context.Context, string) (*Udb, error) {
		switch calls.Add(1) {
		case 1:
			return initial, nil
		case 2:
			return replacement, nil
		default:
			return nil, fmt.Errorf("unexpected extra pool dial")
		}
	})
	t.Cleanup(func() { _ = pool.Close() })
	first, err := pool.Get(ctx, tenantPoolFixtureID)
	if err != nil || first != initial || first.CredentialErr() != nil {
		t.Fatal("healthy owned static client was not cached")
	}
	if err := first.Close(); err != nil {
		t.Fatal("returned owned client Close failed")
	}
	if err := first.CredentialErr(); status.Code(err) != codes.Canceled || !strings.Contains(err.Error(), "credential owner is closed") {
		t.Fatal("closed static client must report its named owner refusal")
	}
	second, err := pool.Get(ctx, tenantPoolFixtureID)
	if err != nil || second != replacement || calls.Load() != 2 {
		t.Fatal("pool reused an externally closed client instead of reconnecting")
	}
	if _, err := second.Data.Broker.Select(ctx, &entityv1.SelectRequest{MessageType: "pool.fixture"}); err != nil {
		t.Fatal("replacement did not retain a working actual TCP channel")
	}
	if cached, err := pool.Get(ctx, tenantPoolFixtureID); err != nil || cached != replacement || calls.Load() != 2 {
		t.Fatal("healthy replacement was not shared after reconnect")
	}
}

func TestTenantSessionPoolAPIKeyMapIsOwnedSnapshotOverTCP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	authn, broker := &keyAuthn{ttl: time.Hour}, &mdBroker{}
	keys := map[string]string{tenantPoolFixtureID: "svc-key"}
	pool := NewAPIKeyTenantPool(Config{Target: serveKeyFakes(t, authn, broker), Purpose: "test.pool.snapshot", Deadline: 5 * time.Second}, keys)
	defer pool.Close()
	keys[tenantPoolFixtureID] = "changed-after-construction"
	otherTenant := "00000000-0000-0000-0000-000000000002"
	keys[otherTenant] = "svc-key"
	u, err := pool.Get(ctx, tenantPoolFixtureID)
	if err != nil || u == nil {
		t.Fatal("caller mutation changed the owned key used by actual API-key exchange")
	}
	if _, err := u.Data.Broker.Select(ctx, &entityv1.SelectRequest{MessageType: "pool.fixture"}); err != nil {
		t.Fatal("snapshot client could not use its actual exchanged bearer")
	}
	broker.mu.Lock()
	md := broker.md.Copy()
	broker.mu.Unlock()
	if values := md.Get("authorization"); len(values) != 1 || values[0] == "" || values[0] != u.Generated.options().Authorization {
		t.Fatal("snapshot client did not send exactly its current exchanged bearer")
	}
	for _, header := range []string{"x-api-key", "x-udb-api-key"} {
		for _, value := range md.Get(header) {
			if value != "" {
				t.Fatal("snapshot client sent a raw key with its exchanged bearer")
			}
		}
	}
	if exchanges, raw := authn.snapshot(); exchanges != 1 || raw {
		t.Fatal("pool must exchange the captured key once without sending raw headers")
	}
	if added, err := pool.Get(ctx, otherTenant); added != nil || err == nil {
		t.Fatal("a key added by the caller after construction must not enter the pool")
	}
}
