package udbclient

import (
	"context"
	"fmt"
	"strings"
	"time"

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
	res, err := c.Authn.ValidateToken(c.Context(ctx), &authnv1.ValidateTokenRequest{Token: token})
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
// prefix.
func (u *Udb) AsUser(ctx context.Context, bearer string) context.Context {
	bearer = strings.TrimSpace(bearer)
	if !strings.HasPrefix(bearer, "Bearer ") {
		bearer = "Bearer " + bearer
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", bearer)
}
