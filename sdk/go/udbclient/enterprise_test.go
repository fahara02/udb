package udbclient

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A valid (unexpired) token: the background refresher must cache the bearer and
// clear any prior poison/error, WITHOUT issuing a RefreshToken RPC (so a nil
// AuthClient is safe — RefreshIfNeeded no-ops for a still-valid token).
func TestEnterpriseSession_BackgroundRefreshClearsPoisonOnValidToken(t *testing.T) {
	store := &MemoryTokenStore{}
	_ = store.Save(context.Background(), Token{AccessToken: "tok-a", ExpiresAt: time.Now().Add(time.Hour)})
	s := &EnterpriseSession{tm: NewTokenManager(nil, store), poisoned: true, lastRefreshErr: errors.New("stale")}

	s.backgroundRefresh()

	if err := s.RefreshErr(); err != nil {
		t.Fatalf("RefreshErr should be nil after a successful refresh, got %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned {
		t.Fatal("session should not be poisoned after a valid token")
	}
	if s.bearer != "Bearer tok-a" {
		t.Fatalf("bearer not updated: got %q", s.bearer)
	}
}

// When poisoned, DataContext/NativeContext must fail CLOSED locally: the returned
// context is already Done and its cancel cause carries the refresh failure.
func TestEnterpriseSession_PoisonedContextFailsClosed(t *testing.T) {
	boom := errors.New("refresh token revoked")
	s := &EnterpriseSession{poisoned: true, lastRefreshErr: boom}

	pctx, poisoned := s.poisonedContext(context.Background())
	if !poisoned {
		t.Fatal("expected the session to report poisoned")
	}
	if pctx.Err() == nil {
		t.Fatal("poisoned context must be Done so the RPC never sends a dead bearer")
	}
	if cause := context.Cause(pctx); !errors.Is(cause, boom) {
		t.Fatalf("cancel cause should wrap the refresh error, got %v", cause)
	}
}

// A healthy session returns the caller's context untouched (no accidental poison).
func TestEnterpriseSession_NotPoisonedPassthrough(t *testing.T) {
	s := &EnterpriseSession{}
	ctx := context.Background()
	got, poisoned := s.poisonedContext(ctx)
	if poisoned {
		t.Fatal("a healthy session must not be poisoned")
	}
	if got != ctx {
		t.Fatal("healthy session must return the original context unchanged")
	}
}

// The refresher schedules just before (expiry - skew), floors an expired token to
// bgRefreshMin (never busy-loops), and uses the idle cadence with no expiry info.
func TestEnterpriseSession_NextRefreshWait(t *testing.T) {
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := &MemoryTokenStore{}
	tm := NewTokenManager(nil, store)
	tm.now = func() time.Time { return fixed }
	s := &EnterpriseSession{tm: tm}

	_ = store.Save(context.Background(), Token{AccessToken: "a", ExpiresAt: fixed.Add(time.Hour)})
	if got := s.nextRefreshWait(); got < 59*time.Minute || got > time.Hour {
		t.Fatalf("valid token: want ~1h-skew, got %v", got)
	}

	_ = store.Save(context.Background(), Token{AccessToken: "a", ExpiresAt: fixed.Add(-time.Minute)})
	if got := s.nextRefreshWait(); got != bgRefreshMin {
		t.Fatalf("expired token: want bgRefreshMin, got %v", got)
	}

	_ = store.Save(context.Background(), Token{AccessToken: "a"})
	if got := s.nextRefreshWait(); got != bgRefreshIdle {
		t.Fatalf("no-expiry token: want bgRefreshIdle, got %v", got)
	}
}

// Close stops the background refresher and is safe to call more than once. (Uses a
// nil embedded *Udb, so we exercise only the stop path, not Udb.Close.)
func TestEnterpriseSession_CloseStopIdempotent(t *testing.T) {
	s := &EnterpriseSession{stopRefresh: make(chan struct{})}
	s.stopOnce.Do(func() { close(s.stopRefresh) })
	// A second Do must not double-close (which would panic).
	s.stopOnce.Do(func() { close(s.stopRefresh) })
	select {
	case <-s.stopRefresh:
	default:
		t.Fatal("stopRefresh should be closed")
	}
}
