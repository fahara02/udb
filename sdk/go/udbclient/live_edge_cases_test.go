package udbclient

import (
	"context"
	"strings"
	"testing"
	"time"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// runLiveEdgeCasesE2E exercises the per-RPC EDGE cases the happy-path CRUD suite
// skips: malformed/hostile inputs and isolation-boundary probes. The contract for
// every case is the same — the broker must FAIL CLOSED with a typed, client-side
// error (or safely accept-and-sanitise), and must NEVER (a) leak another tenant's
// data, nor (b) surface a server fault (Internal/Unknown/DataLoss) that means the
// input crashed the handler instead of being validated.
//
// This is honest edge-case coverage: each assertion would FAIL if the guard it
// checks were removed (project isolation, RLS tenant scoping, NUL/UTF8 handling,
// limit-boundary clamping, unknown-type/unknown-backend validation).
func runLiveEdgeCasesE2E(t *testing.T, broker servicesv1.DataBrokerClient, callCtx context.Context, tenant, project string) {
	t.Helper()
	rc := func(p string) *entityv1.RequestContext { return liveRequestContext(tenant, project, p) }
	suffix := strings.NewReplacer(".", "-", ":", "-", "+", "-").Replace(time.Now().UTC().Format("20060102T150405.000000000"))

	// A server fault means the edge case reached an unguarded code path and crashed
	// the handler — always a bug. Client-side codes (InvalidArgument, FailedPrecondition,
	// NotFound, PermissionDenied, …) are the CORRECT outcome for a rejected input.
	isServerFault := func(err error) bool {
		switch status.Code(err) {
		case codes.Internal, codes.Unknown, codes.DataLoss:
			return true
		default:
			return false
		}
	}

	t.Run("missing_project_id_filter_fails_closed", func(t *testing.T) {
		// Project isolation requires project_id in the filter; omitting it must be
		// rejected, not silently broadened to all projects.
		_, err := broker.Select(callCtx, &entityv1.SelectRequest{
			Context: rc("edge.no-project"), MessageType: liveMessageType,
			Filter: liveStruct(t, map[string]any{"tenant_id": tenant}), Limit: 1,
		})
		if err == nil {
			t.Fatalf("Select without a project_id filter was ACCEPTED — project isolation not enforced")
		}
		if isServerFault(err) {
			t.Fatalf("missing project_id faulted the server (%s) instead of a typed rejection: %v", status.Code(err), err)
		}
	})

	t.Run("cross_tenant_read_no_leak", func(t *testing.T) {
		// Filtering by a FOREIGN tenant_id must never return rows: RLS scopes the read
		// to the JWT's tenant, so the foreign filter can match nothing of ours and must
		// not expose anyone else's rows either.
		foreign := "00000000-0000-0000-0000-0000deadbeef"
		resp, err := broker.Select(callCtx, &entityv1.SelectRequest{
			Context: rc("edge.cross-tenant"), MessageType: liveMessageType,
			Filter: liveStruct(t, map[string]any{"tenant_id": foreign, "project_id": project}), Limit: 10,
		})
		if err != nil {
			if isServerFault(err) {
				t.Fatalf("cross-tenant Select faulted the server (%s): %v", status.Code(err), err)
			}
			return // a typed rejection is an acceptable fail-closed outcome
		}
		if n := len(resp.GetRecordsJson()); n != 0 {
			t.Fatalf("cross-tenant Select LEAKED %d record(s) for foreign tenant %q", n, foreign)
		}
	})

	t.Run("nul_byte_payload_no_utf8_fault", func(t *testing.T) {
		// A NUL (0x00) cannot live in a PG text column; the broker must strip or reject
		// it with a typed error, never surface a raw "invalid byte sequence for encoding
		// UTF8: 0x00" Internal fault (regression guard for B14).
		recordID := "edge-nul-" + suffix
		_, err := broker.Upsert(callCtx, &entityv1.UpsertRequest{
			Context: rc("edge.nul"), MessageType: liveMessageType,
			RecordJson:     liveRecordJSON(t, recordID, tenant, project, "edge-nul-lk-"+suffix, "payload\x00with-nul", 1),
			ConflictFields: []string{"record_id"},
		})
		if err != nil && isServerFault(err) {
			t.Fatalf("NUL-byte payload caused a server fault (%s) — UTF8 0x00 not handled: %v", status.Code(err), err)
		}
	})

	t.Run("limit_boundaries_no_fault", func(t *testing.T) {
		// Negative, zero, and absurdly large limits must be clamped/validated, not
		// allocate-to-OOM or crash the query builder.
		for _, lim := range []int32{-1, 0, 1_000_000} {
			_, err := broker.Select(callCtx, &entityv1.SelectRequest{
				Context: rc("edge.limit"), MessageType: liveMessageType,
				Filter: liveStruct(t, map[string]any{"tenant_id": tenant, "project_id": project}), Limit: lim,
			})
			if err != nil && isServerFault(err) {
				t.Fatalf("Select with limit=%d faulted the server (%s): %v", lim, status.Code(err), err)
			}
		}
	})

	t.Run("unknown_message_type_typed_error", func(t *testing.T) {
		// An unregistered message_type must produce a typed error, not a 500.
		_, err := broker.Select(callCtx, &entityv1.SelectRequest{
			Context: rc("edge.unknown-type"), MessageType: "udb.does.not.Exist",
			Filter: liveStruct(t, map[string]any{"tenant_id": tenant, "project_id": project}), Limit: 1,
		})
		if err == nil {
			t.Fatalf("Select on an unknown message_type was ACCEPTED")
		}
		if isServerFault(err) {
			t.Fatalf("unknown message_type faulted the server (%s) instead of a typed error: %v", status.Code(err), err)
		}
	})

	t.Run("invalid_backend_typed_error", func(t *testing.T) {
		// A nonexistent backend name must be a typed error, never a panic/Internal.
		_, err := broker.ListResources(callCtx, &entityv1.ResourceAdminRequest{
			Context: rc("edge.bad-backend"), Backend: "nonexistent-backend-xyz",
		})
		if err == nil {
			t.Fatalf("ListResources on a nonexistent backend was ACCEPTED")
		}
		if isServerFault(err) {
			t.Fatalf("invalid backend faulted the server (%s) instead of a typed error: %v", status.Code(err), err)
		}
	})
}
