package udbclient

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// singleUseRefreshAuthn mints a new refresh token on every RefreshToken call and
// refuses any other value, like the broker: each refresh token works once.
type singleUseRefreshAuthn struct {
	authnv1.UnimplementedAuthnServiceServer
	mu        sync.Mutex
	live      string
	minted    int
	presented []string
}

func (a *singleUseRefreshAuthn) RefreshToken(_ context.Context, req *authnv1.RefreshTokenRequest) (*authnv1.RefreshTokenResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.presented = append(a.presented, req.GetRefreshToken())
	if req.GetRefreshToken() != a.live {
		return nil, status.Error(codes.Unauthenticated, "invalid credential")
	}
	a.minted++
	a.live = fmt.Sprintf("refresh-%d", a.minted)
	return &authnv1.RefreshTokenResponse{
		AccessToken: fmt.Sprintf("access-%d", a.minted), AccessTokenExpiresIn: 900,
		RefreshToken: a.live, RefreshTokenExpiresIn: 86400, SessionPublicId: "public-session",
	}, nil
}

func (a *singleUseRefreshAuthn) presentedTokens() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.presented)
}

// A manager that kept the login refresh token refreshed once and then failed
// every later refresh with "invalid credential". Each refresh must present the
// token the previous one returned.
func TestTokenManagerPresentsEachRotatedRefreshTokenAcrossRepeatedRefreshes(t *testing.T) {
	authn := &singleUseRefreshAuthn{live: "refresh-1", minted: 1}
	auth := tokenRefreshClient(t, serveSessionAuthn(t, authn, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &MemoryTokenStore{}
	if err := store.Save(ctx, Token{AccessToken: "access-1", RefreshToken: "refresh-1", SessionID: "legacy-session"}); err != nil {
		t.Fatalf("seed token: %v", err)
	}
	tm := NewTokenManager(auth, store)

	for i := 1; i <= 4; i++ {
		// Expire the stored access token so Token() performs a real refresh.
		stale, _ := store.Load(ctx)
		stale.ExpiresAt = time.Now().Add(-time.Second)
		if err := store.Save(ctx, stale); err != nil {
			t.Fatalf("expire token: %v", err)
		}
		if _, err := tm.Token(ctx); err != nil {
			t.Fatalf("refresh %d failed: %v", i, err)
		}
	}

	want := []string{"refresh-1", "refresh-2", "refresh-3", "refresh-4"}
	if got := authn.presentedTokens(); !slices.Equal(got, want) {
		t.Fatalf("presented refresh tokens = %v, want %v", got, want)
	}
	tok, _ := store.Load(ctx)
	if tok.RefreshToken != "refresh-5" || tok.AccessToken != "access-5" {
		t.Fatalf("stored token = (%s, %s), want (access-5, refresh-5)", tok.AccessToken, tok.RefreshToken)
	}
	if tok.SessionID != "legacy-session" {
		t.Fatal("the legacy refresh session id must be kept")
	}
}
