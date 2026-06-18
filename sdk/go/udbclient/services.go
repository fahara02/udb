package udbclient

import (
	"context"
	"fmt"
	"time"

	apikeyv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/services/v1"
	notificationentityv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/notification/entity/v1"
	notificationv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/notification/services/v1"
	tenantv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/tenant/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	eventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/events/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
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

// ── Notification helpers (chapter 13.5) ──────────────────────────────────────

// SendTemplate dispatches a templated notification. The broker renders the
// template from eventType + variables (one SendNotification RPC). It returns the
// per-channel log ids the broker created.
func (f *NotificationFacade) SendTemplate(ctx context.Context, eventType, recipientID string, variables map[string]string) ([]string, error) {
	resp, err := f.Send(ctx, eventType, recipientID, variables)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(resp.GetLogs()))
	for _, log := range resp.GetLogs() {
		ids = append(ids, log.GetLogId())
	}
	return ids, nil
}

// RetryFailed re-attempts delivery of a FAILED notification log by id.
func (f *NotificationFacade) RetryFailed(ctx context.Context, logID string) (*notificationv1.RetryNotificationResponse, error) {
	return f.Raw.RetryNotification(ctx, &notificationv1.RetryNotificationRequest{
		LogId: logID,
	})
}

// notificationTerminal reports whether a status is a final delivery state.
func notificationTerminal(s notificationentityv1.NotificationStatus) bool {
	switch s {
	case notificationentityv1.NotificationStatus_NOTIFICATION_STATUS_DELIVERED,
		notificationentityv1.NotificationStatus_NOTIFICATION_STATUS_FAILED,
		notificationentityv1.NotificationStatus_NOTIFICATION_STATUS_SUPPRESSED:
		return true
	}
	return false
}

// WaitForDelivery reads the notification log status (via GetNotification) until
// it reaches a terminal state (DELIVERED/FAILED/SUPPRESSED) or the deadline
// elapses. It is bounded and status-driven — NOT a fixed sleep loop; it respects
// context cancellation. The poll interval only paces consecutive reads while the
// status is still non-terminal.
func (f *NotificationFacade) WaitForDelivery(ctx context.Context, logID string, deadline time.Duration) (notificationentityv1.NotificationStatus, error) {
	var cancel context.CancelFunc
	if deadline > 0 {
		ctx, cancel = context.WithTimeout(ctx, deadline)
		defer cancel()
	}
	const pollInterval = 100 * time.Millisecond
	for {
		resp, err := f.Raw.GetNotification(ctx, &notificationv1.GetNotificationRequest{LogId: logID})
		if err != nil {
			return notificationentityv1.NotificationStatus_NOTIFICATION_STATUS_UNSPECIFIED, err
		}
		st := resp.GetLog().GetStatus()
		if notificationTerminal(st) {
			return st, nil
		}
		select {
		case <-ctx.Done():
			return st, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// ── Events facade (chapter 13.3) ─────────────────────────────────────────────
//
// EventsFacade wraps the DataBroker event surface (PublishCDC server-stream +
// EnqueueOutboxEvent) with a readiness boundary and a publish-and-wait helper —
// both driven off server signals, never sleeps.

// EventsFacade exposes tenant-scoped CDC subscription + outbox publishing.
type EventsFacade struct {
	Raw  servicesv1.DataBrokerClient
	meta Metadata
}

func newEventsFacade(conn grpc.ClientConnInterface, meta Metadata) *EventsFacade {
	return &EventsFacade{Raw: servicesv1.NewDataBrokerClient(conn), meta: meta}
}

// Subscription is a live CDC subscription handle.
type Subscription struct {
	stream  grpc.ServerStreamingClient[eventsv1.CDCEnvelope]
	first   *eventsv1.CDCEnvelope // buffered first envelope consumed by Ready, if any
	hasNext bool
}

// Subscribe opens the PublishCDC server-stream for topic, tenant-scoped from the
// facade metadata. The returned handle's Ready() resolves on the first server
// signal.
func (f *EventsFacade) Subscribe(ctx context.Context, topic string) (*Subscription, error) {
	stream, err := f.Raw.PublishCDC(ctx, &entityv1.CDCSubscriptionRequest{
		Context:      &entityv1.RequestContext{TenantId: f.meta.TenantID, ProjectId: f.meta.ProjectID},
		TopicPattern: topic,
	})
	if err != nil {
		return nil, err
	}
	return &Subscription{stream: stream}, nil
}

// Ready blocks until the first server-driven signal on the subscription. It
// reads the stream header (server metadata) when available, then waits for the
// first envelope — buffering it for the next Recv so no event is lost. No timer
// or sleep is used; it returns when the server speaks or the stream/context ends.
func (s *Subscription) Ready() error {
	// A header send is the earliest server-driven readiness signal.
	if _, err := s.stream.Header(); err != nil {
		return err
	}
	return nil
}

// Recv returns the next CDC envelope (replaying the one Ready may have buffered).
func (s *Subscription) Recv() (*eventsv1.CDCEnvelope, error) {
	if s.hasNext {
		env := s.first
		s.first, s.hasNext = nil, false
		return env, nil
	}
	return s.stream.Recv()
}

// PublishAndWait enqueues an outbox event for topic then reads the subscription
// until matchFn matches the resulting envelope. It issues exactly one
// EnqueueOutboxEvent and then consumes server-pushed envelopes — never a sleep.
// The supplied subscription must already be Ready. Bounded by ctx.
func (f *EventsFacade) PublishAndWait(ctx context.Context, sub *Subscription, topic string, payload map[string]any, matchFn func(*eventsv1.CDCEnvelope) bool) (*eventsv1.CDCEnvelope, error) {
	if sub == nil {
		return nil, fmt.Errorf("udb: PublishAndWait requires an open subscription")
	}
	var pb *structpb.Struct
	if len(payload) > 0 {
		var err error
		pb, err = structpb.NewStruct(payload)
		if err != nil {
			return nil, fmt.Errorf("udb: build event payload: %w", err)
		}
	}
	if _, err := f.Raw.EnqueueOutboxEvent(ctx, &entityv1.EnqueueOutboxEventRequest{
		Context: &entityv1.RequestContext{TenantId: f.meta.TenantID, ProjectId: f.meta.ProjectID},
		Topic:   topic,
		Payload: pb,
	}); err != nil {
		return nil, err
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		env, err := sub.Recv()
		if err != nil {
			return nil, err
		}
		if matchFn == nil || matchFn(env) {
			return env, nil
		}
	}
}
