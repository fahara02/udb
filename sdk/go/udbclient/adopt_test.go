package udbclient

import (
	"context"
	"reflect"
	"testing"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
)

// TestLoginAndAdoptTenantTwoRPCs asserts LoginAndAdoptTenant issues EXACTLY
// [Login, AuthenticateBearer] and adopts the canonical tenant/project from the
// VERIFIED principal (not a body hint), plus installs the bearer.
func TestLoginAndAdoptTenantTwoRPCs(t *testing.T) {
	var seq []string
	fa := &fakeAuthn{
		seq:       &seq,
		loginResp: &authnv1.LoginResponse{AccessToken: "tok-xyz", SessionId: "sess-1", AccessTokenExpiresIn: 3600},
		authnResp: &authnv1.AuthnResponse{
			Principal: &authnv1.Principal{TenantId: "canonical-tenant", ProjectId: "canonical-project", UserId: "u-9"},
		},
	}
	auth := &AuthClient{Authn: fa, Meta: Metadata{TenantID: "hint-tenant"}}
	gen := NewGenerated(nil, Options{})
	u := &Udb{
		Meta:      Metadata{TenantID: "hint-tenant"},
		Auth:      auth,
		Generated: gen,
		// conns nil: adoptMetadata rebuilds facades over nil conns (no dial); the
		// test only inspects adopted Meta + the generated header swap + sequence.
	}

	res, err := u.LoginAndAdoptTenant(context.Background(), &authnv1.LoginRequest{Username: "u", Password: "p"})
	if err != nil {
		t.Fatalf("LoginAndAdoptTenant: %v", err)
	}
	if !reflect.DeepEqual(seq, []string{"Login", "AuthenticateBearer"}) {
		t.Fatalf("must be EXACTLY [Login, AuthenticateBearer]: %v", seq)
	}
	if u.Meta.TenantID != "canonical-tenant" || u.Meta.ProjectID != "canonical-project" {
		t.Fatalf("canonical tenant/project not adopted from verified principal: %+v", u.Meta)
	}
	if u.Meta.UserID != "u-9" {
		t.Fatalf("user id not adopted: %q", u.Meta.UserID)
	}
	if gen.options().Authorization != "Bearer tok-xyz" {
		t.Fatalf("authorization not installed: %q", gen.options().Authorization)
	}
	if res.Token.AccessToken != "tok-xyz" || res.Principal.GetTenantId() != "canonical-tenant" {
		t.Fatalf("result not threaded: %+v", res)
	}
	if u.Generated.Meta().TenantID != "canonical-tenant" {
		t.Fatalf("generated layer meta not swapped: %q", u.Generated.Meta().TenantID)
	}
}

// TestSetMetaAtomicSwap asserts SetMeta/SetAuthorization swap atomically.
func TestSetMetaAtomicSwap(t *testing.T) {
	gen := NewGenerated(nil, Options{Meta: Metadata{TenantID: "old"}})
	gen.SetMeta(Metadata{TenantID: "new"})
	gen.SetAuthorization("Bearer t")
	if gen.Meta().TenantID != "new" {
		t.Fatalf("SetMeta did not apply: %q", gen.Meta().TenantID)
	}
	if gen.options().Authorization != "Bearer t" {
		t.Fatalf("SetAuthorization did not apply: %q", gen.options().Authorization)
	}
}
