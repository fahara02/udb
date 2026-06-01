package udbclient

import (
	"context"
	"database/sql"
	"fmt"

	authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
)

// ── Stage 2: native database fast-path access (item 138) ─────────────────────
//
// GetNativeAccess runs the same UDB authorization decision and, when allowed,
// returns a short-lived grant: a restricted role, a scoped DSN, and the exact
// app.current_* session variables the caller must SET LOCAL per transaction so
// the broker-generated RLS policies still apply on the direct connection.

// GetNativeAccess forwards a fully-formed NativeAccessRequest and returns the
// response (decision + optional grant). The grant is present only when the
// decision allowed and the server has native access configured.
func (c *AuthClient) GetNativeAccess(ctx context.Context, req *authzv1.NativeAccessRequest) (*authzv1.NativeAccessResponse, error) {
	return c.Authz.GetNativeAccess(c.Context(ctx), req)
}

// NativeAccess is a convenience over GetNativeAccess: it builds the request from
// the caller Metadata plus the supplied resource/action/purpose and returns the
// grant. It returns (nil, nil) when access is allowed but no native grant was
// minted (native access not configured server-side), and an error when the
// decision denied access.
func (c *AuthClient) NativeAccess(ctx context.Context, resource *authzv1.ResourceRef, action, purpose string) (*authzv1.NativeAccessGrant, error) {
	if purpose == "" {
		purpose = c.Meta.Purpose
	}
	resp, err := c.GetNativeAccess(ctx, &authzv1.NativeAccessRequest{
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
		return nil, err
	}
	if d := resp.GetDecision(); d != nil && !d.GetAllowed() {
		return nil, fmt.Errorf("udb: native access denied: %s", d.GetDenyReason())
	}
	return resp.GetGrant(), nil
}

// WithNativeTx opens a transaction on the caller-provided *sql.DB (which should
// be connected using grant.Dsn), applies the grant's app.current_* session
// variables with SET LOCAL so RLS sees the same request context the broker
// enforced, runs fn, and commits — or rolls back on error. Uses only the Go
// standard library so the SDK pulls in no database driver of its own; the
// caller chooses the driver (pgx stdlib, lib/pq, …) when opening db.
func WithNativeTx(ctx context.Context, db *sql.DB, grant *authzv1.NativeAccessGrant, fn func(tx *sql.Tx) error) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if grant != nil {
		for key, value := range grant.GetSessionVariables() {
			// set_config(name, value, true) == SET LOCAL but parameterizable,
			// avoiding identifier-injection on the variable name/value.
			if _, e := tx.ExecContext(ctx, "SELECT set_config($1, $2, true)", key, value); e != nil {
				err = fmt.Errorf("udb: apply session variable %q: %w", key, e)
				return err
			}
		}
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
