package udbclient

import (
	"context"

	apikeyv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/services/v1"
	notificationv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/notification/services/v1"
	tenantv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/tenant/services/v1"
)

// ── Phase 7: control-plane convenience facades ───────────────────────────────
//
// Each facade wraps a raw generated service client (reachable via .Raw) and adds
// a handful of one-line helpers for the most common operations. The caller
// Metadata seeds tenant/project defaults so callers don't repeat them. Only
// methods that actually exist on the generated stub are exposed.

// ── ApiKey ───────────────────────────────────────────────────────────────────

// ApiKeyFacade wraps ApiKeyServiceClient.
type ApiKeyFacade struct {
	Raw  apikeyv1.ApiKeyServiceClient
	meta Metadata
}

// Create issues a new API key. The plaintext key is returned ONCE on
// CreateApiKeyResponse.PlainKey — persist it; the server does not store it.
// Scopes default to the caller Metadata scopes when none are supplied.
func (f *ApiKeyFacade) Create(ctx context.Context, name string, scopes []string) (*apikeyv1.CreateApiKeyResponse, error) {
	if len(scopes) == 0 {
		scopes = f.meta.Scopes
	}
	return f.Raw.CreateApiKey(ctx, &apikeyv1.CreateApiKeyRequest{
		Name:   name,
		Scopes: scopes,
	})
}

// Revoke revokes an API key by id with an optional reason.
func (f *ApiKeyFacade) Revoke(ctx context.Context, keyID, reason string) (*apikeyv1.RevokeApiKeyResponse, error) {
	return f.Raw.RevokeApiKey(ctx, &apikeyv1.RevokeApiKeyRequest{
		KeyId:        keyID,
		RevokeReason: reason,
	})
}

// NOTE: the ApiKeyService stub has no Rotate RPC (only Create/Get/List/Update/
// Revoke/Validate/UsageStats). The idiomatic rotation is Create a replacement
// key then Revoke the old one; do that explicitly via Create + Revoke rather
// than a fake atomic Rotate that the server can't honor.

// ── Tenant ───────────────────────────────────────────────────────────────────

// TenantFacade wraps TenantServiceClient.
type TenantFacade struct {
	Raw  tenantv1.TenantServiceClient
	meta Metadata
}

// Create provisions a new tenant from a code + display name.
func (f *TenantFacade) Create(ctx context.Context, code, name string) (*tenantv1.CreateTenantResponse, error) {
	return f.Raw.CreateTenant(ctx, &tenantv1.CreateTenantRequest{
		Code: code,
		Name: name,
	})
}

// Onboard is the fuller create path: code, name, type, and JSON config/branding.
// Empty config/branding are sent as-is (the server treats them as defaults).
func (f *TenantFacade) Onboard(ctx context.Context, code, name, tenantType, configJSON, brandingJSON string) (*tenantv1.CreateTenantResponse, error) {
	return f.Raw.CreateTenant(ctx, &tenantv1.CreateTenantRequest{
		Code:     code,
		Name:     name,
		Type:     tenantType,
		Config:   configJSON,
		Branding: brandingJSON,
	})
}

// ── Notification ─────────────────────────────────────────────────────────────

// NotificationFacade wraps NotificationServiceClient.
type NotificationFacade struct {
	Raw  notificationv1.NotificationServiceClient
	meta Metadata
}

// Send dispatches a notification for eventType to a recipient. The tenant and
// project default to the caller Metadata. variables fill the template; pass nil
// when the template needs none. Channels are left empty so the template's
// default channels are used.
func (f *NotificationFacade) Send(ctx context.Context, eventType, recipientID string, variables map[string]string) (*notificationv1.SendNotificationResponse, error) {
	return f.Raw.SendNotification(ctx, &notificationv1.SendNotificationRequest{
		EventType:   eventType,
		RecipientId: recipientID,
		TenantId:    f.meta.TenantID,
		ProjectId:   f.meta.ProjectID,
		Variables:   variables,
	})
}
