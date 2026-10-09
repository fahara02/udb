package udbclient

import (
	"context"
	"strings"
	"testing"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// runLiveDataBrokerOpsE2E deepens the operational DataBroker RPCs from a surface
// "reached a handler" probe to a real RESULT-ASSERTED check: each call's response
// is decoded and meaningful invariants are asserted. These are the CDC / DLQ / saga /
// catalog / schema / health / admin / project family — the part of DataBroker the
// all-backend CRUD matrix does NOT cover.
//
// Goal (honest e2e per RPC): not just "it didn't mount-fail", but "it returned a
// structurally valid, semantically sensible response".
func runLiveDataBrokerOpsE2E(t *testing.T, broker servicesv1.DataBrokerClient, callCtx, platformCtx context.Context, tenant, project string) {
	t.Helper()
	rc := func(purpose string) *entityv1.RequestContext { return liveRequestContext(tenant, project, purpose) }

	t.Run("GetCapabilities", func(t *testing.T) {
		caps, err := broker.GetCapabilities(callCtx, &entityv1.CapabilitiesRequest{Context: rc("db.ops.caps")})
		if err != nil {
			t.Fatalf("GetCapabilities: %v", err)
		}
		if len(caps.GetEnabledBackends()) == 0 {
			t.Fatalf("GetCapabilities advertised zero enabled backends")
		}
		if len(caps.GetBackendCapabilities()) == 0 {
			t.Fatalf("GetCapabilities advertised zero backend capability descriptors")
		}
	})

	t.Run("GetCatalogManifest", func(t *testing.T) {
		m, err := broker.GetCatalogManifest(callCtx, &entityv1.CatalogManifestRequest{Context: rc("db.ops.manifest")})
		if err != nil {
			t.Fatalf("GetCatalogManifest: %v", err)
		}
		if len(m.GetManifestJson()) == 0 {
			t.Fatalf("GetCatalogManifest returned an empty manifest_json")
		}
	})

	t.Run("GetCatalogVersions", func(t *testing.T) {
		// Result-asserted but not over-strict: the call must succeed and the version
		// list must be internally consistent (active_version, when set, must appear in
		// the list). A broker with no staged/activated catalog legitimately has 0
		// versions, so an empty list is NOT a failure.
		v, err := broker.GetCatalogVersions(callCtx, &entityv1.CatalogManifestRequest{Context: rc("db.ops.versions")})
		if err != nil {
			t.Fatalf("GetCatalogVersions: %v", err)
		}
		if av := v.GetActiveVersion(); av != "" {
			found := false
			for _, ver := range v.GetVersions() {
				if ver.GetVersion() == av {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("GetCatalogVersions active_version=%q not present in the version list", av)
			}
		}
	})

	t.Run("GetCdcStatusRejectsUnknownSlot", func(t *testing.T) {
		// This fixture has no registered WAL source. Unknown slots must return
		// typed NotFound rather than fabricate a healthy empty pipeline. Actual
		// registered-source status and WAL delivery are proved by the CDC lane.
		for _, slot := range []string{"", "sdk_unknown_" + strings.ReplaceAll(uuid4(), "-", "")} {
			var trailer metadata.MD
			response, err := broker.GetCdcStatus(callCtx, &entityv1.CdcControlRequest{
				Context: rc("db.ops.cdc.unknown"), SlotName: slot,
			}, grpc.Trailer(&trailer))
			if response != nil || status.Code(err) != codes.NotFound {
				t.Fatalf("GetCdcStatus must refuse an unregistered slot: response=%v code=%s", response != nil, status.Code(err))
			}
			mapped, ok := AsError(mapError("/udb.services.v1.DataBroker/GetCdcStatus", err, trailer))
			if !ok {
				t.Fatal("unknown CDC status must retain typed broker details")
			}
			detail, ok := mapped.Detail()
			if !ok || detail.GetPolicyDecisionId() != "cdc_slot_not_controllable" || detail.GetOperation() != "catalog_admin.get_cdc_status" {
				t.Fatal("unknown CDC status must identify the actual owned-slot refusal")
			}
		}
	})

	t.Run("ListDlqEvents", func(t *testing.T) {
		r, err := broker.ListDlqEvents(callCtx, &entityv1.DlqListRequest{Context: rc("db.ops.dlq")})
		if err != nil {
			t.Fatalf("ListDlqEvents: %v", err)
		}
		if r.GetTotalCount() < 0 || int32(len(r.GetEvents())) > r.GetTotalCount()+1 {
			t.Fatalf("ListDlqEvents inconsistent: events=%d total=%d", len(r.GetEvents()), r.GetTotalCount())
		}
	})

	t.Run("ListSagas", func(t *testing.T) {
		r, err := broker.ListSagas(callCtx, &entityv1.SagaListRequest{Context: rc("db.ops.sagas")})
		if err != nil {
			t.Fatalf("ListSagas: %v", err)
		}
		if r.GetTotalCount() < 0 {
			t.Fatalf("ListSagas negative total")
		}
	})

	t.Run("ListMessageSchemas+Lookup", func(t *testing.T) {
		l, err := broker.ListMessageSchemas(callCtx, &entityv1.MessageSchemaListRequest{Context: rc("db.ops.schemas")})
		if err != nil {
			t.Fatalf("ListMessageSchemas: %v", err)
		}
		if len(l.GetMessageTypes()) == 0 {
			t.Fatalf("ListMessageSchemas returned no message types")
		}
		// LookupMessageSchema for a real, listed type must resolve to that type.
		mt := l.GetMessageTypes()[0]
		look, err := broker.LookupMessageSchema(callCtx, &entityv1.MessageSchemaLookupRequest{
			Context: rc("db.ops.schema.lookup"), MessageType: mt,
		})
		if err != nil {
			t.Fatalf("LookupMessageSchema(%q): %v", mt, err)
		}
		if look.GetSchema() == nil || look.GetSchema().GetMessageType() != mt {
			t.Fatalf("LookupMessageSchema resolved %q, want a schema for %q", look.GetSchema().GetMessageType(), mt)
		}
		if len(look.GetSchema().GetFields()) == 0 {
			t.Fatalf("LookupMessageSchema(%q) returned a schema with no fields", mt)
		}
	})

	t.Run("GetHealthReport", func(t *testing.T) {
		h, err := broker.GetHealthReport(callCtx, &entityv1.HealthReportRequest{Context: rc("db.ops.health")})
		if err != nil {
			t.Fatalf("GetHealthReport: %v", err)
		}
		if !h.GetPostgresConfigured() {
			t.Fatalf("GetHealthReport says postgres not configured — the broker's primary store")
		}
	})

	t.Run("GetAdminSummary", func(t *testing.T) {
		if _, err := broker.GetAdminSummary(callCtx, &entityv1.AdminSummaryRequest{Context: rc("db.ops.admin")}); err != nil {
			t.Fatalf("GetAdminSummary: %v", err)
		}
	})

	t.Run("ListProjects", func(t *testing.T) {
		p, err := broker.ListProjects(platformCtx, &entityv1.ProjectListRequest{Context: rc("db.ops.projects")})
		if err != nil {
			t.Fatalf("ListProjects: %v", err)
		}
		if p.GetTotalCount() < 0 {
			t.Fatalf("ListProjects negative total")
		}
	})
}
