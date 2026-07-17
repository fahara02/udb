package udbclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	"google.golang.org/grpc/metadata"
)

// EnterpriseConfig drives ConnectEnterprise, the one-call production-path setup
// (critic.md §11): dial data + auth targets, log in with username/password,
// verify the bearer, adopt the canonical tenant UUID, and carry that bearer on
// every subsequent call. Mirrors examples/ts_enterprise's flow in Go.
type EnterpriseConfig struct {
	Target     string // data-plane target, e.g. "127.0.0.1:50051" (required)
	AuthTarget string // control-plane target, e.g. "127.0.0.1:50061"; defaults to Target
	Username   string // required
	Password   string // required
	// TenantCode is the human tenant code hint used pre-login (e.g. "acme").
	// The verified canonical tenant UUID replaces it after login.
	TenantCode string
	ProjectID  string
	Purpose    string
	Scopes     []string
	UserID     string
	TLS        *tls.Config
	Deadline   time.Duration
	Retry      RetryConfig
}

// EnterpriseSession bundles the authenticated Udb with the VERIFIED canonical
// tenant state and the bearer.
//
// IMPORTANT: after login the broker connection's interceptor (set at dial time)
// does NOT pick up the post-login token, so raw u.Data / native calls would be
// Unauthenticated. Use DataContext / NativeContext (which append the bearer
// explicitly) for any call you make through the embedded *Udb, and use
// CanonicalTenantID — never the human code — in tenant-scoped records/filters.
type EnterpriseSession struct {
	*Udb
	// CanonicalTenantID is the verified tenant UUID (use this in all filters).
	CanonicalTenantID string
	// CanonicalProjectID is the verified project from the principal.
	CanonicalProjectID string
	// Principal is the verified login principal (for inspection).
	Principal *authnv1.Principal
	// Tenant tracks the code -> canonical-UUID transition + the fail-fast guard.
	Tenant TenantState

	bearer string // "Bearer <access-token>"
}

// ConnectEnterprise runs the full enterprise flow in one call and returns a
// session whose canonical tenant is verified and whose bearer is ready to attach
// via DataContext / NativeContext.
func ConnectEnterprise(ctx context.Context, cfg EnterpriseConfig) (*EnterpriseSession, error) {
	if cfg.Target == "" || cfg.Username == "" || cfg.Password == "" {
		return nil, fmt.Errorf("udb: ConnectEnterprise requires Target, Username, and Password")
	}
	if cfg.AuthTarget == "" {
		cfg.AuthTarget = cfg.Target
	}

	tenant := NewTenantState(cfg.TenantCode)
	hint := cfg.TenantCode
	if hint == "" {
		hint = "default"
	}

	u, err := NewUdb(ctx, Config{
		Target:     cfg.Target,
		AuthTarget: cfg.AuthTarget,
		TenantID:   hint, // pre-login hint; replaced by the canonical UUID below
		ProjectID:  cfg.ProjectID,
		Purpose:    cfg.Purpose,
		Scopes:     cfg.Scopes,
		UserID:     cfg.UserID,
		TLS:        cfg.TLS,
		Deadline:   cfg.Deadline,
		Retry:      cfg.Retry,
	})
	if err != nil {
		return nil, err
	}

	adopted, err := u.LoginAndAdoptTenant(ctx, &authnv1.LoginRequest{
		Username: cfg.Username,
		Password: cfg.Password,
		// UDB-GO-001: transmit the documented pre-login tenant/project selection so
		// the broker can resolve/verify the intended identity. The post-login source
		// of truth is unchanged — only the VERIFIED principal (adopted below) is ever
		// used for tenant/project scope, never these input hints.
		TenantHint:  cfg.TenantCode,
		ProjectHint: cfg.ProjectID,
	})
	if err != nil {
		_ = u.Close()
		return nil, fmt.Errorf("udb: ConnectEnterprise login: %w", err)
	}
	principal := adopted.Principal
	if principal == nil {
		_ = u.Close()
		return nil, fmt.Errorf("udb: ConnectEnterprise: no principal returned")
	}
	if err := tenant.Adopt(principal.GetTenantId()); err != nil {
		_ = u.Close()
		return nil, fmt.Errorf("udb: ConnectEnterprise adopt tenant: %w", err)
	}

	return &EnterpriseSession{
		Udb:                u,
		CanonicalTenantID:  principal.GetTenantId(),
		CanonicalProjectID: principal.GetProjectId(),
		Principal:          principal,
		Tenant:             tenant,
		bearer:             "Bearer " + adopted.Token.AccessToken,
	}, nil
}

// DataContext returns a context for DataBroker calls (s.Data.Broker.*) carrying
// the verified metadata AND the bearer. Use it for every data-plane call so the
// post-login token is sent (the dial-time interceptor does not carry it).
func (s *EnterpriseSession) DataContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(s.Udb.Data.Context(ctx), "authorization", s.bearer)
}

// NativeContext returns a context for native control-plane calls (ApiKey/Tenant/
// Notification/…) carrying the verified metadata AND the bearer.
func (s *EnterpriseSession) NativeContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(s.Udb.Auth.Context(ctx), "authorization", s.bearer)
}

// Bearer is the "Bearer <token>" credential, for callers that build their own
// metadata.
func (s *EnterpriseSession) Bearer() string { return s.bearer }

// ValidateTenant fails fast (naming both values) if recordTenantID differs from
// the verified canonical tenant — call it before a tenant-scoped write.
func (s *EnterpriseSession) ValidateTenant(recordTenantID string) error {
	return s.Tenant.ValidateTenantID(recordTenantID)
}
