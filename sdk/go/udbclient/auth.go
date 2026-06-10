package udbclient

import (
	"context"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// AuthClient is a thin, hand-written convenience wrapper over the generated
// UDB AuthnService and AuthzService stubs. It mirrors Client: the same caller
// Metadata is attached to every outgoing call so the broker sees a consistent
// tenant/identity/scope context, and the raw generated requests stay reachable
// for anything the convenience helpers don't cover.
type AuthClient struct {
	Authn authnv1.AuthnServiceClient
	Authz authzv1.AuthzServiceClient
	Meta  Metadata

	// authzCache, when attached via AttachAuthzCache, makes Require/Explain
	// reuse server-blessed Decision.cache_ttl_seconds windows. nil = no cache.
	authzCache *AuthzCache

	// policyBundleSecret, when set via SetPolicyBundleSecret, makes
	// GetPolicyBundle verify the HMAC signature on each fetched bundle and
	// reject a tampered or mis-signed bundle. Empty = no verification.
	policyBundleSecret []byte
}

// SetPolicyBundleSecret configures the shared HMAC secret used to verify signed
// policy bundles. When set, GetPolicyBundle verifies every fetched bundle and
// returns an *PolicyBundleSignatureError on mismatch. Pass nil/empty to disable.
func (c *AuthClient) SetPolicyBundleSecret(secret []byte) { c.policyBundleSecret = secret }

// NewAuthClient builds an AuthClient from an existing gRPC connection.
func NewAuthClient(conn grpc.ClientConnInterface, meta Metadata) *AuthClient {
	return &AuthClient{
		Authn: authnv1.NewAuthnServiceClient(conn),
		Authz: authzv1.NewAuthzServiceClient(conn),
		Meta:  meta,
	}
}

// Context attaches the caller Metadata as gRPC headers, matching Client.Context.
func (c *AuthClient) Context(ctx context.Context) context.Context {
	pairs := []string{
		"x-tenant-id", c.Meta.TenantID,
		"x-user-id", c.Meta.UserID,
		"x-purpose", c.Meta.Purpose,
		"x-correlation-id", c.Meta.CorrelationID,
		"x-service-identity", c.Meta.ServiceIdentity,
		"x-udb-project-id", c.Meta.ProjectID,
		"x-udb-client-catalog-version", c.Meta.ClientCatalogVersion,
	}
	if len(c.Meta.Scopes) > 0 {
		pairs = append(pairs, "x-scopes", joinScopes(c.Meta.Scopes))
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

// ── Authentication ──────────────────────────────────────────────────────────

// Authenticate forwards a fully-formed AuthnRequest. Use this when you need to
// set fields the typed helpers below don't expose (external providers, audience,
// issuer, credential_type, …).
func (c *AuthClient) Authenticate(ctx context.Context, req *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	return c.Authn.Authenticate(c.Context(ctx), req)
}

// AuthenticateBearer validates a native JWT bearer token and returns the
// resolved principal. The caller Metadata seeds the tenant/project hints and
// requested scopes.
func (c *AuthClient) AuthenticateBearer(ctx context.Context, token string) (*authnv1.AuthnResponse, error) {
	return c.Authenticate(ctx, &authnv1.AuthnRequest{
		BearerToken:     token,
		TenantHint:      c.Meta.TenantID,
		ProjectHint:     c.Meta.ProjectID,
		RequestedScopes: c.Meta.Scopes,
	})
}

// AuthenticateAPIKey validates an API/service key and returns the principal.
func (c *AuthClient) AuthenticateAPIKey(ctx context.Context, apiKey string) (*authnv1.AuthnResponse, error) {
	return c.Authenticate(ctx, &authnv1.AuthnRequest{
		ApiKey:          apiKey,
		TenantHint:      c.Meta.TenantID,
		ProjectHint:     c.Meta.ProjectID,
		RequestedScopes: c.Meta.Scopes,
	})
}

// AuthenticateSession validates a server-side session id and returns the
// principal bound to that session.
func (c *AuthClient) AuthenticateSession(ctx context.Context, sessionID string) (*authnv1.AuthnResponse, error) {
	return c.Authenticate(ctx, &authnv1.AuthnRequest{
		SessionId:   sessionID,
		TenantHint:  c.Meta.TenantID,
		ProjectHint: c.Meta.ProjectID,
	})
}

// ── Authorization ───────────────────────────────────────────────────────────

// Authorize forwards a fully-formed AuthzRequest and returns the decision.
func (c *AuthClient) Authorize(ctx context.Context, req *authzv1.AuthzRequest) (*authzv1.Decision, error) {
	resp, err := c.Authz.Authorize(c.Context(ctx), req)
	if err != nil {
		return nil, err
	}
	return resp.GetDecision(), nil
}

// Can is a convenience over Authorize: it builds an AuthzRequest from the caller
// Metadata plus the supplied resource/action/purpose and reports whether access
// is allowed. The full Decision is returned for matched-policy/deny-reason
// inspection.
func (c *AuthClient) Can(ctx context.Context, resource *authzv1.ResourceRef, action, purpose string) (bool, *authzv1.Decision, error) {
	if purpose == "" {
		purpose = c.Meta.Purpose
	}
	decision, err := c.Authorize(ctx, &authzv1.AuthzRequest{
		Principal: &authzv1.Principal{
			UserId:          c.Meta.UserID,
			ServiceIdentity: c.Meta.ServiceIdentity,
			TenantId:        c.Meta.TenantID,
			ProjectId:       c.Meta.ProjectID,
			Scopes:          c.Meta.Scopes,
		},
		TenantId:        c.Meta.TenantID,
		ProjectId:       c.Meta.ProjectID,
		Resource:        resource,
		Action:          action,
		Purpose:         purpose,
		RequestedScopes: c.Meta.Scopes,
	})
	if err != nil {
		return false, nil, err
	}
	return decision.GetAllowed(), decision, nil
}

// CheckAccess forwards a Casbin-style (user, domain, object, action) request.
func (c *AuthClient) CheckAccess(ctx context.Context, req *authzv1.CheckAccessRequest) (*authzv1.CheckAccessResponse, error) {
	return c.Authz.CheckAccess(c.Context(ctx), req)
}
