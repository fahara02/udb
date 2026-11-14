package udbclient

import (
	"context"
	"errors"
	"fmt"
	"strings"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	"google.golang.org/grpc/metadata"
)

// A service connected to its home tenant acts in a MEMBER tenant (one where an
// approved ServiceAccountTenantMembership names it) with a token minted by
// AuthnService.ExchangeServiceTokenForTenant. That token is short-lived (about
// fifteen minutes) and carries no refresh token, so every consumer had to call
// the exchange itself, install the token, and exchange again before it expired.
// ForTenant does it: the member-tenant client exchanges the home bearer at
// creation, carries the member token on every call, exchanges again at 4/5 of
// the token's life, and exchanges again at once when the broker refuses the
// token early. While such a re-exchange is failing it refuses calls locally
// instead of sending the refused token, and retries with backoff.

// ForTenant returns a client that acts in the member tenant tenantID (and
// projectID; empty lets the membership choose) with a token exchanged from this
// client's bearer. The member client dials its own connections to the same
// targets and keeps its token fresh until Close; close it before (or with) the
// home client, whose bearer every re-exchange uses. tenantID must be the
// tenant's canonical lowercase UUID.
func (u *Udb) ForTenant(ctx context.Context, tenantID, projectID string) (*Udb, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tenantID, projectID = strings.TrimSpace(tenantID), strings.TrimSpace(projectID)
	if !isUUIDString(tenantID) || tenantID != strings.ToLower(tenantID) {
		return nil, fmt.Errorf("udb: ForTenant: tenant %q is not a canonical (lowercase hyphenated) UUID", tenantID)
	}
	if u.dialConfig.Target == "" || u.Auth == nil {
		return nil, errors.New("udb: ForTenant requires a client built by Connect/NewUdb")
	}
	cfg := u.dialConfig
	cfg.TenantID, cfg.ProjectID = tenantID, projectID
	// The member identity comes from the exchanged token's verified principal.
	cfg.UserID, cfg.ServiceIdentity, cfg.Scopes = "", "", nil
	cfg.Credentials = Credentials{}
	member, err := NewUdb(ctx, cfg)
	if err != nil {
		return nil, err
	}
	session := &apiKeySession{
		label: fmt.Sprintf("exchange the service token for tenant %s", tenantID),
		retry: cfg.Deadline,
		exchange: func(ctx context.Context) (exchangedBearer, error) {
			return u.exchangeForTenant(ctx, member, tenantID, projectID)
		},
	}
	if err := member.startBearerExchange(ctx, session); err != nil {
		_ = member.Close()
		return nil, err
	}
	return member, nil
}

// exchangeForTenant mints a member-tenant token with the home bearer and
// verifies it through the member client. A token for another tenant or project
// than the one asked for is refused, never adopted.
func (u *Udb) exchangeForTenant(ctx context.Context, member *Udb, tenantID, projectID string) (exchangedBearer, error) {
	// A demand exchange runs inside a member call whose outgoing metadata names
	// the member tenant; the home call must carry only the home identity.
	ctx = metadata.NewOutgoingContext(ctx, metadata.MD{})
	res, err := u.Auth.Authn.ExchangeServiceTokenForTenant(u.Auth.Context(ctx), &authnv1.ExchangeServiceTokenForTenantRequest{
		TargetTenantId:  tenantID,
		TargetProjectId: projectID,
	})
	if err != nil {
		return exchangedBearer{}, fmt.Errorf("udb: exchange the service token for tenant %s: %w", tenantID, err)
	}
	if res.GetAccessToken() == "" || res.GetExpiresAtUnix() == 0 {
		return exchangedBearer{}, fmt.Errorf("udb: exchange the service token for tenant %s: the broker returned no token (does the service have an ACTIVE membership in that tenant?)", tenantID)
	}
	if res.GetTenantId() != tenantID {
		return exchangedBearer{}, &credentialBindingError{cause: fmt.Errorf("udb: exchange the service token for tenant %s: the broker returned a token for tenant %q", tenantID, res.GetTenantId())}
	}
	if projectID != "" && res.GetProjectId() != projectID {
		return exchangedBearer{}, &credentialBindingError{cause: fmt.Errorf("udb: exchange the service token for tenant %s: the broker returned a token for project %q, want %q", tenantID, res.GetProjectId(), projectID)}
	}
	who, err := member.Auth.AuthenticateBearer(member.recoveryContext(ctx, credentialAuthenticate), res.GetAccessToken())
	if err != nil {
		return exchangedBearer{}, fmt.Errorf("udb: verify the token exchanged for tenant %s: %w", tenantID, err)
	}
	principal := who.GetPrincipal()
	if principal != nil && principal.GetTenantId() != tenantID {
		return exchangedBearer{}, &credentialBindingError{cause: fmt.Errorf("udb: the token exchanged for tenant %s authenticates in tenant %q", tenantID, principal.GetTenantId())}
	}
	return exchangedBearer{accessToken: res.GetAccessToken(), expiresAtUnix: res.GetExpiresAtUnix(), principal: principal}, nil
}

// ForTenant returns an EnterpriseSession acting in the member tenant tenantID
// with a token exchanged from this session's bearer (see (*Udb).ForTenant). The
// member session re-exchanges before the token expires; RefreshErr and Bearer
// report its exchanged token. Close it before (or with) this session.
func (s *EnterpriseSession) ForTenant(ctx context.Context, tenantID, projectID string) (*EnterpriseSession, error) {
	if pctx, poisoned := s.poisonedContext(ctx); poisoned {
		return nil, context.Cause(pctx)
	}
	member, err := s.Udb.ForTenant(ctx, tenantID, projectID)
	if err != nil {
		return nil, err
	}
	tenant := NewTenantState(tenantID)
	if err := tenant.Adopt(member.Meta.TenantID); err != nil {
		_ = member.Close()
		return nil, fmt.Errorf("udb: ForTenant adopt tenant: %w", err)
	}
	return &EnterpriseSession{
		Udb:                member,
		CanonicalTenantID:  member.Meta.TenantID,
		CanonicalProjectID: member.Meta.ProjectID,
		Principal:          member.Principal(),
		Tenant:             tenant,
	}, nil
}

// exchangedSession is the member-tenant token state of a ForTenant session
// (nil for a session that signed in itself).
func (s *EnterpriseSession) exchangedSession() *apiKeySession {
	if s.tm != nil || s.Udb == nil {
		return nil
	}
	return s.Udb.apiKey
}
