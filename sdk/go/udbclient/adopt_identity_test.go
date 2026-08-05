package udbclient

import (
	"context"
	"reflect"
	"testing"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
)

// TestLoginAndAdoptTenantCanonicalIdentity asserts LoginAndAdoptTenant reconciles
// the WHOLE identity — user, service identity, AND scopes — from the VERIFIED
// principal, overriding any caller hint, and that a subsequent bearer refresh (a
// re-login returning a different principal) leaves EXACTLY the new canonical set
// with no stale carryover from the first login. Reverting the fix (conditional
// UserID copy / no ServiceIdentity+Scopes reconciliation) fails this test.
func TestLoginAndAdoptTenantCanonicalIdentity(t *testing.T) {
	var seq []string
	fa := &fakeAuthn{
		seq:       &seq,
		loginResp: &authnv1.LoginResponse{AccessToken: "tok-1", SessionId: "s1", AccessTokenExpiresIn: 3600},
		authnResp: &authnv1.AuthnResponse{
			Principal: &authnv1.Principal{
				TenantId:        "canonical-tenant",
				ProjectId:       "canonical-project",
				UserId:          "u-1",
				ServiceIdentity: "svc-1",
				Scopes:          []string{"read", "write"},
			},
		},
	}
	// Seed the client with CALLER HINTS the verified principal must override.
	hints := Metadata{
		TenantID:        "hint-tenant",
		UserID:          "hint-user",
		ServiceIdentity: "hint-svc",
		Scopes:          []string{"hint:scope"},
	}
	auth := &AuthClient{Authn: fa, Meta: hints}
	u := &Udb{Meta: hints, Auth: auth, Generated: NewGenerated(nil, Options{Meta: hints})}

	if _, err := u.LoginAndAdoptTenant(context.Background(), &authnv1.LoginRequest{Username: "u", Password: "p"}); err != nil {
		t.Fatalf("LoginAndAdoptTenant (login): %v", err)
	}
	// Exactly ONE canonical set, entirely from the verified principal — every
	// caller hint rejected.
	if u.Meta.UserID != "u-1" {
		t.Fatalf("user hint survived login: got %q want %q", u.Meta.UserID, "u-1")
	}
	if u.Meta.ServiceIdentity != "svc-1" {
		t.Fatalf("service identity not adopted / hint survived: got %q want %q", u.Meta.ServiceIdentity, "svc-1")
	}
	if !reflect.DeepEqual(u.Meta.Scopes, []string{"read", "write"}) {
		t.Fatalf("scopes not reconciled to the principal (merge/hint leak?): %v", u.Meta.Scopes)
	}
	if u.Generated.Meta().ServiceIdentity != "svc-1" ||
		!reflect.DeepEqual(u.Generated.Meta().Scopes, []string{"read", "write"}) {
		t.Fatalf("generated layer identity/scopes not swapped: %+v", u.Generated.Meta())
	}

	// Bearer refresh: a re-login returns a DIFFERENT principal whose user is EMPTY
	// and whose scope set shrank. adoptMetadata rebuilt u.Auth over the (nil) conn,
	// so re-wire the fake before driving the refresh.
	u.Auth.Authn = fa
	fa.loginResp = &authnv1.LoginResponse{AccessToken: "tok-2", SessionId: "s2", AccessTokenExpiresIn: 3600}
	fa.authnResp = &authnv1.AuthnResponse{
		Principal: &authnv1.Principal{
			TenantId:        "canonical-tenant",
			ProjectId:       "canonical-project",
			UserId:          "", // verified principal carries NO user this time
			ServiceIdentity: "svc-2",
			Scopes:          []string{"read"},
		},
	}
	if _, err := u.LoginAndAdoptTenant(context.Background(), &authnv1.LoginRequest{Username: "u", Password: "p"}); err != nil {
		t.Fatalf("LoginAndAdoptTenant (refresh): %v", err)
	}
	// The stale "u-1" user MUST be cleared by the unconditional copy, not retained.
	if u.Meta.UserID != "" {
		t.Fatalf("stale user survived bearer refresh: got %q want empty", u.Meta.UserID)
	}
	if u.Meta.ServiceIdentity != "svc-2" {
		t.Fatalf("service identity not re-reconciled on refresh: got %q want %q", u.Meta.ServiceIdentity, "svc-2")
	}
	if !reflect.DeepEqual(u.Meta.Scopes, []string{"read"}) {
		t.Fatalf("stale scope survived bearer refresh: got %v want [read]", u.Meta.Scopes)
	}
	if u.Generated.options().Authorization != "Bearer tok-2" {
		t.Fatalf("refreshed bearer not installed: %q", u.Generated.options().Authorization)
	}
}
