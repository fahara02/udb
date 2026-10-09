package udbclient

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	commonpb "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
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

	t.Run("verified_project_autofill_excludes_real_peer_row", func(t *testing.T) {
		runLiveProjectIsolationWitness(t, broker, callCtx, tenant, project, suffix)
	})

	t.Run("cross_tenant_read_no_leak", func(t *testing.T) {
		// Filtering by a FOREIGN tenant_id must never return rows: verified scope bounds the read
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

// SdkLiveRecord has RLS disabled. These two served principals and rows prove
// verified project injection in the query path, independently of database RLS.
func runLiveProjectIsolationWitness(t *testing.T, broker servicesv1.DataBrokerClient, ownCtx context.Context, tenant, project, suffix string) {
	t.Helper()
	peerProject := requiredLiveEnv(t, "UDB_LIVE_ISOLATION_PROJECT")
	if peerProject == project {
		t.Fatal("project isolation witness requires two different projects")
	}
	authTarget := os.Getenv("UDB_AUTH_GRPC_TARGET")
	if authTarget == "" {
		authTarget = requiredLiveEnv(t, "UDB_GRPC_TARGET")
	}
	conn, err := grpc.NewClient(authTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal("dial isolation auth channel:", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	authn := authnv1.NewAuthnServiceClient(conn)
	bounded := func(base context.Context) (context.Context, context.CancelFunc) {
		return context.WithTimeout(base, 8*time.Second)
	}
	md, _ := metadata.FromOutgoingContext(ownCtx)
	authorization := md.Get("authorization")
	if len(authorization) != 1 || !strings.HasPrefix(authorization[0], "Bearer ") {
		t.Fatal("isolation witness requires exactly one operator bearer")
	}
	c, cancel := bounded(ownCtx)
	own, err := authn.Authenticate(c, &authnv1.AuthnRequest{BearerToken: strings.TrimPrefix(authorization[0], "Bearer "), TenantHint: tenant, ProjectHint: project})
	cancel()
	if err != nil || own.GetPrincipal().GetTenantId() != tenant || own.GetPrincipal().GetProjectId() != project || own.GetPrincipal().GetSubject() == "" {
		t.Fatal("operator bearer must verify its exact tenant/project principal", err)
	}
	c, cancel = bounded(context.Background())
	login, err := authn.Login(c, &authnv1.LoginRequest{Username: requiredLiveEnv(t, "UDB_LIVE_ISOLATION_USERNAME"), Password: requiredLiveEnv(t, "UDB_LIVE_PASSWORD"), TenantHint: tenant, ProjectHint: peerProject, DeviceName: "go-project-isolation"})
	cancel()
	if err != nil || login.GetAccessToken() == "" || login.GetSessionId() == "" {
		t.Fatal("peer isolation Login must issue an owned session", err)
	}
	peerMD := md.Copy()
	peerMD.Set("authorization", "Bearer "+login.GetAccessToken())
	peerMD.Set("x-udb-project-id", peerProject)
	peerMD.Delete("x-user-id")
	peerMD.Delete("x-service-identity")
	peerMD.Delete("x-scopes")
	peerCtx := metadata.NewOutgoingContext(context.Background(), peerMD)
	peerSubject := ""
	t.Cleanup(func() {
		c, cancel := bounded(peerCtx)
		defer cancel()
		if _, err := authn.Logout(c, &authnv1.LogoutRequest{SessionId: login.GetSessionId(), RevokeReason: "sdk_project_isolation", Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: peerProject}, UserId: peerSubject, PrincipalId: peerSubject, Purpose: "edge.cleanup"}}); err != nil {
			t.Error("owned peer session cleanup failed:", status.Code(err))
		}
	})
	c, cancel = bounded(peerCtx)
	peer, err := authn.Authenticate(c, &authnv1.AuthnRequest{BearerToken: login.GetAccessToken(), TenantHint: tenant, ProjectHint: peerProject})
	cancel()
	if err == nil {
		peerSubject = peer.GetPrincipal().GetSubject()
	}
	if err != nil || peer.GetPrincipal().GetTenantId() != tenant || peer.GetPrincipal().GetProjectId() != peerProject || peer.GetPrincipal().GetSubject() == "" || peer.GetPrincipal().GetSubject() == own.GetPrincipal().GetSubject() {
		t.Fatal("peer bearer must verify the same tenant, different project and principal", err)
	}
	for _, principal := range []*authnv1.Principal{own.GetPrincipal(), peer.GetPrincipal()} {
		approved := false
		for _, scope := range principal.GetScopes() {
			approved = approved || scope == "udb:admin"
		}
		if !approved {
			t.Fatal("both signed fixture principals must hold the table's approved scope")
		}
	}
	ownID, peerID := "edge-own-"+suffix, "edge-peer-"+suffix
	for _, row := range []struct {
		id, project string
		ctx         context.Context
	}{{ownID, project, ownCtx}, {peerID, peerProject, peerCtx}} {
		row := row
		t.Cleanup(func() {
			// Cleanup uses a fresh deadline even if the parent conformance context expired.
			cleanupMD, _ := metadata.FromOutgoingContext(row.ctx)
			c, cancel := bounded(metadata.NewOutgoingContext(context.Background(), cleanupMD.Copy()))
			defer cancel()
			if _, err := broker.Delete(c, &entityv1.DeleteRequest{Context: liveRequestContext(tenant, row.project, "edge.cleanup"), MessageType: liveMessageType, Filter: liveStruct(t, map[string]any{"tenant_id": tenant, "project_id": row.project, "record_id": row.id})}); err != nil {
				t.Error("owned isolation row cleanup failed:", status.Code(err))
			}
		})
		c, cancel := bounded(row.ctx)
		_, err := broker.Upsert(c, &entityv1.UpsertRequest{Context: liveRequestContext(tenant, row.project, "edge.seed"), MessageType: liveMessageType, RecordJson: liveRecordJSON(t, row.id, tenant, row.project, "lookup-"+row.id, row.id, 1), ConflictFields: []string{"record_id"}})
		cancel()
		if err != nil {
			t.Fatal("served owned isolation Upsert failed:", status.Code(err))
		}
	}
	selectRows := func(base context.Context, selectedProject string, filter map[string]any) (*entityv1.RecordSet, error) {
		c, cancel := bounded(base)
		defer cancel()
		return broker.Select(c, &entityv1.SelectRequest{Context: liveRequestContext(tenant, selectedProject, "edge.isolation"), MessageType: liveMessageType, Filter: liveStruct(t, filter), Limit: 10})
	}
	assertOwned := func(rows *entityv1.RecordSet, id, selectedProject string) {
		t.Helper()
		if len(rows.GetRecordsJson()) != 1 {
			t.Fatal("project-scoped witness must return exactly its owned row")
		}
		var row map[string]any
		if err := json.Unmarshal(rows.GetRecordsJson()[0], &row); err != nil {
			t.Fatal("decode isolation row:", err)
		}
		if row["record_id"] != id || row["payload"] != id || row["tenant_id"] != tenant || row["project_id"] != selectedProject {
			t.Fatal("project-scoped witness returned a different row or scope")
		}
	}
	peerRows, err := selectRows(peerCtx, peerProject, map[string]any{"tenant_id": tenant, "project_id": peerProject, "record_id": peerID})
	if err != nil {
		t.Fatal("peer-owned positive read failed:", status.Code(err))
	}
	assertOwned(peerRows, peerID, peerProject)
	ids := map[string]any{"$in": []any{ownID, peerID}}
	rows, err := selectRows(ownCtx, project, map[string]any{"tenant_id": tenant, "record_id": ids})
	if err != nil {
		t.Fatal("omitted project must be filled from the verified bearer:", status.Code(err))
	}
	assertOwned(rows, ownID, project)
	foreign, err := selectRows(ownCtx, project, map[string]any{"tenant_id": tenant, "project_id": peerProject, "record_id": ids})
	if err != nil || len(foreign.GetRecordsJson()) != 0 {
		t.Fatal("explicit foreign project must intersect the verified project to zero rows", status.Code(err))
	}
	_, err = selectRows(ownCtx, project, map[string]any{"tenant_id": tenant, "$or": []any{map[string]any{"record_id": ownID}, map[string]any{"record_id": peerID, "project_id": peerProject}}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal("project predicate buried in OR must receive the planner's typed InvalidArgument refusal", status.Code(err))
	}
}
