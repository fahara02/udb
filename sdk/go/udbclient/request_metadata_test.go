package udbclient

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/grpc/metadata"
)

// UDB-GO-006: Client.Context must merge REQUEST-SCOPED audit metadata
// (correlation id, purpose, client catalog version) from WithMetadata(ctx),
// while identity (tenant/user/project/scopes/service identity) stays
// authoritative from the connected client and is never overridable per request.
func TestContextMergesRequestScopedAuditMetadata(t *testing.T) {
	c := &Client{Meta: Metadata{
		TenantID:             "tenant-fixed",
		UserID:               "user-fixed",
		ProjectID:            "project-fixed",
		ServiceIdentity:      "svc-fixed",
		Scopes:               []string{"data:read"},
		Purpose:              "client-default-purpose",
		CorrelationID:        "client-default-corr",
		ClientCatalogVersion: "client-default-catalog",
	}}

	ctx := WithMetadata(context.Background(), Metadata{
		CorrelationID:        "req-corr",
		Purpose:              "req-purpose",
		ClientCatalogVersion: "req-catalog",
		// Identity fields in the request context MUST be ignored.
		TenantID:  "attacker-tenant",
		UserID:    "attacker-user",
		ProjectID: "attacker-project",
	})

	md, ok := metadata.FromOutgoingContext(c.Context(ctx))
	if !ok {
		t.Fatalf("no outgoing metadata")
	}
	// Each header emitted exactly once.
	for _, key := range []string{
		"x-correlation-id", "x-purpose", "x-udb-client-catalog-version",
		"x-tenant-id", "x-user-id", "x-udb-project-id",
	} {
		if got := len(md.Get(key)); got != 1 {
			t.Fatalf("%s emitted %d times, want 1", key, got)
		}
	}
	// Request-scoped audit values win.
	if got := md.Get("x-correlation-id")[0]; got != "req-corr" {
		t.Fatalf("correlation id = %q, want req-corr", got)
	}
	if got := md.Get("x-purpose")[0]; got != "req-purpose" {
		t.Fatalf("purpose = %q, want req-purpose", got)
	}
	if got := md.Get("x-udb-client-catalog-version")[0]; got != "req-catalog" {
		t.Fatalf("catalog version = %q, want req-catalog", got)
	}
	// Identity stays authoritative — request context can NEVER override it.
	if got := md.Get("x-tenant-id")[0]; got != "tenant-fixed" {
		t.Fatalf("tenant = %q, want tenant-fixed (non-overridable)", got)
	}
	if got := md.Get("x-user-id")[0]; got != "user-fixed" {
		t.Fatalf("user = %q, want user-fixed (non-overridable)", got)
	}
	if got := md.Get("x-udb-project-id")[0]; got != "project-fixed" {
		t.Fatalf("project = %q, want project-fixed (non-overridable)", got)
	}
}

// An absent per-request value falls back to the client default (no regression).
func TestContextFallsBackToClientMetaWhenRequestUnset(t *testing.T) {
	c := &Client{Meta: Metadata{CorrelationID: "client-corr", Purpose: "client-purpose"}}
	md, _ := metadata.FromOutgoingContext(c.Context(context.Background()))
	if got := md.Get("x-correlation-id")[0]; got != "client-corr" {
		t.Fatalf("correlation id = %q, want client-corr fallback", got)
	}
	if got := md.Get("x-purpose")[0]; got != "client-purpose" {
		t.Fatalf("purpose = %q, want client-purpose fallback", got)
	}
}

// Two concurrent contexts must preserve their OWN audit metadata without either
// mutating the shared Client.Meta (the race the report calls out).
func TestConcurrentContextsPreserveDistinctMetadataWithoutMutatingClientMeta(t *testing.T) {
	c := &Client{Meta: Metadata{TenantID: "t", CorrelationID: "base-corr"}}
	beforeTenant, beforeCorr := c.Meta.TenantID, c.Meta.CorrelationID

	var wg sync.WaitGroup
	results := make([]string, 2)
	corrs := [2]string{"corr-A", "corr-B"}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx := WithMetadata(context.Background(), Metadata{CorrelationID: corrs[idx]})
			md, _ := metadata.FromOutgoingContext(c.Context(ctx))
			results[idx] = md.Get("x-correlation-id")[0]
		}(i)
	}
	wg.Wait()

	if results[0] != "corr-A" || results[1] != "corr-B" {
		t.Fatalf("concurrent correlation ids crossed: %v", results)
	}
	if c.Meta.TenantID != beforeTenant || c.Meta.CorrelationID != beforeCorr {
		t.Fatalf("Client.Meta was mutated during concurrent requests: %+v", c.Meta)
	}
}

// The generated dial interceptor carries every NATIVE-service call (storage,
// asset, webrtc, notification, tenant, api-key, analytics). It must resolve
// request-scoped audit metadata exactly like Client.Context, otherwise a native
// RPC is either rejected for a missing request context or audited against the
// stale connection-level correlation id.
func TestOutgoingContextMergesRequestScopedAuditMetadata(t *testing.T) {
	g := NewGenerated(nil, Options{Meta: Metadata{
		TenantID:             "tenant-fixed",
		UserID:               "user-fixed",
		ProjectID:            "project-fixed",
		ServiceIdentity:      "svc-fixed",
		Scopes:               []string{"data:read"},
		Purpose:              "client-default-purpose",
		CorrelationID:        "client-default-corr",
		ClientCatalogVersion: "client-default-catalog",
	}})

	ctx := WithMetadata(context.Background(), Metadata{
		CorrelationID:        "req-corr",
		Purpose:              "req-purpose",
		ClientCatalogVersion: "req-catalog",
		// Identity in the request context MUST still be ignored here.
		TenantID: "attacker-tenant",
		UserID:   "attacker-user",
	})

	md, ok := metadata.FromOutgoingContext(g.outgoingContext(ctx))
	if !ok {
		t.Fatalf("no outgoing metadata")
	}
	if got := md.Get("x-correlation-id")[0]; got != "req-corr" {
		t.Fatalf("correlation id = %q, want req-corr", got)
	}
	if got := md.Get("x-purpose")[0]; got != "req-purpose" {
		t.Fatalf("purpose = %q, want req-purpose", got)
	}
	if got := md.Get("x-udb-client-catalog-version")[0]; got != "req-catalog" {
		t.Fatalf("catalog version = %q, want req-catalog", got)
	}
	// The server accepts x-request-id as a request-context carrier; it must be
	// the per-request value, not the connection-level baseline.
	if got := md.Get("x-request-id"); len(got) != 1 || got[0] != "req-corr" {
		t.Fatalf("x-request-id = %v, want [req-corr]", got)
	}
	// Identity stays authoritative.
	if got := md.Get("x-tenant-id")[0]; got != "tenant-fixed" {
		t.Fatalf("tenant = %q, want tenant-fixed (non-overridable)", got)
	}
	if got := md.Get("x-user-id")[0]; got != "user-fixed" {
		t.Fatalf("user = %q, want user-fixed (non-overridable)", got)
	}
}

// Without request-scoped metadata the interceptor keeps its previous behaviour.
func TestOutgoingContextFallsBackToConnectionMeta(t *testing.T) {
	g := NewGenerated(nil, Options{Meta: Metadata{CorrelationID: "client-corr", Purpose: "client-purpose"}})
	md, _ := metadata.FromOutgoingContext(g.outgoingContext(context.Background()))
	if got := md.Get("x-correlation-id")[0]; got != "client-corr" {
		t.Fatalf("correlation id = %q, want client-corr fallback", got)
	}
	if got := md.Get("x-purpose")[0]; got != "client-purpose" {
		t.Fatalf("purpose = %q, want client-purpose fallback", got)
	}
}
