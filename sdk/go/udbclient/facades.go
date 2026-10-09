package udbclient

import (
	"context"
	"fmt"
	"strings"
	"time"

	authnentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	configv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/config/services/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/services/v1"
	"google.golang.org/grpc/metadata"
)

// Small facades over native services that every service otherwise wrapped by
// hand: delete a file with an explicit mode, evaluate feature flags, validate a
// user's session, and call as an end user.

// DeleteFileMode deletes a file with an explicit mode: SOFT keeps the metadata
// tombstoned and removes the bytes best-effort, HARD records a durable
// object-GC intent. reason is recorded on a HARD delete.
func (f *StorageFacade) DeleteFileMode(ctx context.Context, fileID string, mode storagev1.DeleteMode, reason string) (*storagev1.DeleteFileResponse, error) {
	return f.Raw.DeleteFile(ctx, &storagev1.DeleteFileRequest{
		TenantId: f.meta.TenantID,
		FileId:   fileID,
		Mode:     mode,
		Reason:   reason,
	})
}

// FlagsFacade evaluates feature flags (ConfigService).
type FlagsFacade struct {
	Raw  configv1.ConfigServiceClient
	meta Metadata
}

// Flags returns the feature-flag facade (served on the control-plane target).
func (u *Udb) Flags() *FlagsFacade {
	return &FlagsFacade{Raw: configv1.NewConfigServiceClient(u.authConn), meta: u.Meta}
}

// Evaluate resolves keys for the caller's tenant/project with attrs (for
// example {"user_id": "..."}). Unknown keys are absent from the result.
func (f *FlagsFacade) Evaluate(ctx context.Context, attrs map[string]string, keys ...string) (map[string]*configv1.FlagValue, error) {
	res, err := f.Raw.EvaluateFlags(ctx, &configv1.EvaluateFlagsRequest{
		TenantId: f.meta.TenantID,
		Keys:     keys,
		Context:  &configv1.EvaluateContext{ProjectId: f.meta.ProjectID, Attributes: attrs},
	})
	if err != nil {
		return nil, err
	}
	return res.GetValues(), nil
}

// Enabled reports whether boolean flag key is on for attrs. A flag that does
// not exist is off.
func (f *FlagsFacade) Enabled(ctx context.Context, key string, attrs map[string]string) (bool, error) {
	values, err := f.Evaluate(ctx, attrs, key)
	if err != nil {
		return false, err
	}
	return values[key].GetBoolValue(), nil
}

// UserSession is a validated user session.
type UserSession struct {
	// PublicID is the stable public session id (`sesspub_…`), the same for the
	// login token and every token refreshed from it. Use it in logs and
	// revocation lists; never the internal session row id.
	PublicID  string
	UserID    string
	TenantID  string
	ProjectID string
	Scopes    []string
	Roles     []string
	ExpiresAt time.Time
	Principal *authnv1.Principal
}

// ValidateSession checks a user's bearer token (with or without the "Bearer "
// prefix). An invalid or expired token is an error.
func (c *AuthClient) ValidateSession(ctx context.Context, token string) (*UserSession, error) {
	token = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(token), "Bearer "))
	res, err := c.Authn.ValidateToken(c.Context(ctx), &authnv1.ValidateTokenRequest{Token: token, TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS})
	if err != nil {
		return nil, err
	}
	if !res.GetValid() {
		return nil, fmt.Errorf("%w: the session token is not valid", ErrNotFound)
	}
	s := &UserSession{
		PublicID:  res.GetSessionPublicId(),
		UserID:    res.GetUserId(),
		TenantID:  res.GetTenantId(),
		ProjectID: res.GetProjectId(),
		Scopes:    res.GetScopes(),
		Roles:     res.GetRoles(),
		Principal: res.GetPrincipal(),
	}
	if ts := res.GetExpiresAt(); ts != nil {
		s.ExpiresAt = ts.AsTime()
	}
	return s, nil
}

// AsUser returns a context whose calls carry the end user's bearer instead of
// the service's own credential, for operations the broker must authorize as
// that user (their files, their sessions). The token may carry the "Bearer "
// prefix. The broker verifies the token; AsUser does not adopt claims or change
// the connected tenant/project. The user must belong to that same scope.
func (u *Udb) AsUser(ctx context.Context, bearer string) context.Context {
	bearer = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(bearer), "Bearer "))
	existing, _ := metadata.FromOutgoingContext(ctx)
	md := existing.Copy()
	if md == nil {
		md = metadata.MD{}
	}
	first := func(key string) string {
		if values := md.Get(key); len(values) > 0 {
			return values[0]
		}
		return ""
	}
	audit := Metadata{
		Purpose: first("x-purpose"), CorrelationID: first("x-correlation-id"),
		ClientCatalogVersion: first("x-udb-client-catalog-version"),
	}
	if previous, ok := ctx.Value(asUserContextKey{}).(asUserContext); ok {
		audit.Purpose = firstNonEmptyValue(audit.Purpose, previous.inheritedAudit.Purpose)
		audit.CorrelationID = firstNonEmptyValue(audit.CorrelationID, previous.inheritedAudit.CorrelationID)
		audit.ClientCatalogVersion = firstNonEmptyValue(audit.ClientCatalogVersion, previous.inheritedAudit.ClientCatalogVersion)
	}
	// A caller may already have a Data/Auth context. Rebuild its SDK defaults
	// for delegation, preserving request audit values, rather than retaining or
	// appending the connected service's principal. Other outgoing metadata stays.
	for _, key := range []string{
		"x-tenant-id", "x-udb-project-id", "x-user-id", "x-service-identity",
		"x-scopes", "x-purpose", "x-correlation-id", "x-udb-client-catalog-version",
	} {
		md.Delete(key)
	}
	md.Set("authorization", "Bearer "+bearer)
	// An explicit empty value prevents a generated interceptor from restoring
	// a legacy raw-key default. It is not a second authentication credential.
	md.Set("x-api-key", "")
	md.Delete("x-udb-api-key")
	ctx = context.WithValue(ctx, asUserContextKey{}, asUserContext{inheritedAudit: audit})
	return metadata.NewOutgoingContext(ctx, md)
}
