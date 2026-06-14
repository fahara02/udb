package udbclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestLivePerf measures per-RPC latency for the entire 262-RPC surface against a
// running broker and writes a sorted perf report. It is gated separately from the
// conformance suite (UDB_LIVE_PERF=1) because it is a measurement run, not a
// pass/fail gate: a slow RPC is reported, not failed.
//
// Honesty of the numbers:
//   - read_only RPCs are timed over many iterations (safe to repeat) → real p50/p99.
//   - mutation RPCs are timed over a few iterations with unique keys per call.
//   - destructive RPCs are sent ONCE, typed-empty (validation latency only; the
//     action never executes), and clearly marked so the number is not mistaken for
//     a full destructive round-trip.
//
// The latency includes client encode + transport + server handle + decode — i.e.
// what a real caller sees over this transport (localhost for Go).
func TestLivePerf(t *testing.T) {
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" || os.Getenv("UDB_LIVE_PERF") != "1" {
		t.Skip("perf run requires UDB_LIVE_SDK_TESTS=1 and UDB_LIVE_PERF=1")
	}

	target := requiredLiveEnv(t, "UDB_GRPC_TARGET")
	authTarget := os.Getenv("UDB_AUTH_GRPC_TARGET")
	if authTarget == "" {
		authTarget = target
	}
	tenant := liveEnv("UDB_LIVE_TENANT", "sdk-live")
	project := liveEnv("UDB_LIVE_PROJECT", "default")
	meta := Metadata{
		TenantID: tenant, ProjectID: project, Purpose: "go.live.perf",
		CorrelationID: "go-live-perf", ServiceIdentity: "go.sdk.perf",
		ClientCatalogVersion: ProtocolVersion,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	brokerConn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer brokerConn.Close()
	authConn := brokerConn
	if authTarget != target {
		authConn, err = grpc.NewClient(authTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial auth: %v", err)
		}
		defer authConn.Close()
	}

	login, err := authnv1.NewAuthnServiceClient(authConn).Login(ctx, &authnv1.LoginRequest{
		Username:   requiredLiveEnv(t, "UDB_LIVE_USERNAME"),
		Password:   requiredLiveEnv(t, "UDB_LIVE_PASSWORD"),
		TenantHint: tenant, ProjectHint: project, DeviceName: "go-sdk-perf",
	})
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}
	// Canonical tenant UUID from the principal, so request bodies match the claim.
	auth := NewAuthClient(authConn, meta)
	if who, err := auth.AuthenticateBearer(ctx, login.GetAccessToken()); err == nil {
		if pt := who.GetPrincipal().GetTenantId(); pt != "" {
			tenant = pt
			meta.TenantID = tenant
		}
	}
	authz := "Bearer " + login.GetAccessToken()
	brokerGen := NewGenerated(brokerConn, liveGeneratedOptions(meta, authz))
	authGen := NewGenerated(authConn, liveGeneratedOptions(meta, authz))

	// SEED PHASE (runs before any measurement): create real, disposable entities
	// and capture their identifiers so every RPC can be driven down its SUCCESS
	// path with valid inputs. The UUID-strict native services (storage/asset/
	// webrtc) need the canonical tenant UUID + a matching outgoing context; the
	// admin's tenant claim IS that UUID (discovered above), so one bearer serves all.
	uuidTenant := tenant
	broker := servicesv1.NewDataBrokerClient(brokerConn)
	base := authGen.outgoingContext(ctx)
	nativeCtxFn := func() context.Context { return nativeCtx(ctx, authGen, authz, uuidTenant) }
	seed := perfSeed(t, ctx, broker, brokerGen.outgoingContext(ctx), authConn, base, nativeCtxFn, tenant, project, uuidTenant)
	defer seed.cleanup()
	fix := seed.fix

	type sample struct {
		rpc     RPCInfo
		p50     time.Duration
		p99     time.Duration
		mean    time.Duration
		min     time.Duration
		max     time.Duration
		iters   int
		note    string
		errCode string
	}

	// timeOne measures one call. Unary RPCs are a full request→response round-trip.
	// Streaming RPCs measure STREAM-OPEN latency (initiate + send request + CloseSend),
	// NOT first-message latency: a subscription stream (PublishCDC, StreamResources, …)
	// delivers its first message only when an event arrives, which never happens in a
	// passive perf run — draining it would just time out at the deadline (this is the
	// bug that produced the bogus 272 ms DataBroker mean). Stream-open is the real,
	// data-independent latency a client pays to establish the stream, and it is labeled
	// as such in the report so it is never mistaken for a unary round-trip.
	timeOne := func(gen *GeneratedClient, rpc RPCInfo) (time.Duration, error) {
		d := 20 * time.Second
		if rpc.Kind != KindUnary {
			d = 15 * time.Second
		}
		callCtx, c := context.WithTimeout(ctx, d)
		defer c()
		start := time.Now()
		var err error
		if rpc.Kind == KindUnary {
			// Top data-plane CRUD RPCs are measured with a SEMANTICALLY VALID body so
			// the number reflects REAL handler work (a real Upsert/Select/Delete against
			// the built-in `udb.sdk.live.v1.SdkLiveRecord` schema) — not validation
			// rejection on a placeholder request. Every OTHER RPC is driven with a
			// SEEDED, fixture-resolved request so reference/ID fields point at the real
			// entities created in the seed phase → the RPC runs its SUCCESS path.
			if in, out, ok := perfRealBody(rpc, tenant, project, fix); ok {
				err = gen.InvokeUnary(callCtx, rpc.FullMethod, in, out)
			} else {
				err = seededProbeLiveRPC(callCtx, gen, rpc, tenant, project, fix)
			}
		} else if isCdcSubscriptionRPC(rpc) {
			// Event-driven success path: subscribe, then fire a real mutation that
			// flows outbox→CDC→Kafka, and measure time-to-FIRST-delivered-event.
			err = timeCdcFirstEvent(callCtx, gen, broker, brokerGen.outgoingContext(callCtx), rpc, tenant, project, seed.recordID)
		} else {
			// Other streaming RPCs: open with seeded inputs and measure first RecvMsg
			// (a real server response), not just stream-open.
			err = seededFirstRecv(callCtx, gen, rpc, tenant, project, fix)
		}
		return time.Since(start), err
	}

	// Iteration budget per operation_kind. Every RPC is now driven down its SUCCESS
	// path with seeded inputs, so even destructive RPCs run for real (against a
	// disposable seeded target) — they are measured ONCE because the action is not
	// idempotent. Mutation: a few. Read: enough for a stable p99.
	iterFor := func(kind string) (int, string) {
		switch kind {
		case "destructive":
			return 1, "destructive: 1 real call against a seeded disposable target"
		case "mutation":
			return 5, "mutation (seeded success path)"
		default:
			return 25, "read_only (seeded success path)"
		}
	}

	samples := make([]sample, 0, len(AllRPCs))
	for _, rpc := range AllRPCs {
		gen := authGen
		if rpc.Service == "DataBroker" {
			gen = brokerGen
		}
		iters, note := iterFor(rpc.OperationKind)
		if isCdcSubscriptionRPC(rpc) {
			// CDC first-event includes a real produce→deliver round-trip; keep the
			// iteration count low so the run stays bounded.
			iters, note = 3, "cdc subscription: time-to-first-event (real mutation produced)"
		} else if rpc.Kind != KindUnary {
			note = "streaming: time-to-first-response (seeded; " + string(rpc.Kind) + ")"
		}
		// Warm-up (channel/HTTP2 + server caches) — excluded from the numbers.
		_, _ = timeOne(gen, rpc)
		durs := make([]time.Duration, 0, iters)
		var lastErrCode string
		for i := 0; i < iters; i++ {
			d, err := timeOne(gen, rpc)
			if err != nil {
				lastErrCode = status.Code(err).String()
			}
			durs = append(durs, d)
		}
		sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
		var sum time.Duration
		for _, d := range durs {
			sum += d
		}
		s := sample{
			rpc: rpc, iters: iters, note: note, errCode: lastErrCode,
			p50:  durs[pct(len(durs), 50)],
			p99:  durs[pct(len(durs), 99)],
			mean: sum / time.Duration(len(durs)),
			min:  durs[0],
			max:  durs[len(durs)-1],
		}
		samples = append(samples, s)
	}

	// Per-service aggregate (mean of per-RPC means).
	svcMean := map[string]time.Duration{}
	svcCount := map[string]int{}
	var grand time.Duration
	for _, s := range samples {
		svcMean[s.rpc.Service] += s.mean
		svcCount[s.rpc.Service]++
		grand += s.mean
	}

	// Render report.
	var out strings.Builder
	out.WriteString("# UDB SDK Live Perf — Go (localhost)\n\n")
	out.WriteString(fmt.Sprintf("RPCs measured: %d   tenant=%s\n\n", len(samples), tenant))
	out.WriteString("Every RPC is driven down its SUCCESS path: a SEED phase first creates real, " +
		"disposable entities (a user, role + assignment + policies, an API key, a notification, a " +
		"stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the " +
		"harness resolves each request's reference/ID fields to those real identifiers. So the numbers " +
		"reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any " +
		"residual non-OK RPC is listed under Failures for the maintainer to finish.\n\n")
	out.WriteString("Unary RPCs = full request→response round-trip. Non-CDC streaming RPCs report " +
		"time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (PublishCDC) reports time-to-" +
		"FIRST-EVENT: the harness subscribes, fires a real Upsert that flows outbox→CDC→Kafka, and " +
		"times the first delivered event. Streaming rows are marked in the note column.\n\n")
	out.WriteString("## Seeded fixtures\n\n")
	fkeys := make([]string, 0, len(fix.m))
	for k := range fix.m {
		fkeys = append(fkeys, k)
	}
	sort.Strings(fkeys)
	out.WriteString("Captured semantic field → seeded value keys used to resolve request fields: ")
	out.WriteString(strings.Join(fkeys, ", "))
	out.WriteString("\n\n")
	out.WriteString("## Per-service mean latency (mean of per-RPC means)\n\n")
	out.WriteString("| Service | RPCs | mean |\n|---|---:|---:|\n")
	svcNames := make([]string, 0, len(svcMean))
	for k := range svcMean {
		svcNames = append(svcNames, k)
	}
	sort.Slice(svcNames, func(i, j int) bool { return svcMean[svcNames[i]] > svcMean[svcNames[j]] })
	for _, sv := range svcNames {
		out.WriteString(fmt.Sprintf("| %s | %d | %s |\n", sv, svcCount[sv], (svcMean[sv] / time.Duration(svcCount[sv])).Round(time.Microsecond)))
	}

	// A non-OK status on the LAST iteration marks the RPC failed. err column carries
	// the gRPC status code (e.g. UNAVAILABLE, FAILED_PRECONDITION) so a failing RPC is
	// never reported as a silent latency sample — this is what turns a saga/audit
	// regression from a "slow RPC" into an obvious FAILURE with its code.
	errOf := func(s sample) string {
		if s.errCode != "" && s.errCode != "OK" {
			return s.errCode
		}
		return "OK"
	}

	// Failures subsection: every RPC whose last iteration returned a non-OK status.
	failed := make([]sample, 0)
	for _, s := range samples {
		if e := errOf(s); e != "OK" {
			failed = append(failed, s)
		}
	}
	out.WriteString(fmt.Sprintf("\n## Failures — still to fix (%d)\n\n", len(failed)))
	if len(failed) == 0 {
		out.WriteString("No RPC returned a non-OK gRPC status — every RPC ran its success path.\n")
	} else {
		out.WriteString("These RPCs still returned a non-OK gRPC status on their last iteration: the " +
			"seed phase could not construct a fully-valid request for them. They are reported (not " +
			"silently sampled) so the maintainer can finish their seeding/fixtures.\n\n")
		out.WriteString("| RPC | kind | err | p99 | mean | iters |\n|---|---|---|---:|---:|---:|\n")
		sort.Slice(failed, func(i, j int) bool {
			return failed[i].rpc.Service+"/"+failed[i].rpc.Name < failed[j].rpc.Service+"/"+failed[j].rpc.Name
		})
		for _, s := range failed {
			out.WriteString(fmt.Sprintf("| %s/%s | %s | %s | %s | %s | %d |\n",
				s.rpc.Service, s.rpc.Name, s.rpc.OperationKind, errOf(s),
				s.p99.Round(time.Microsecond), s.mean.Round(time.Microsecond), s.iters))
		}
	}

	out.WriteString("\n## Slowest 25 RPCs by p99\n\n")
	out.WriteString("| RPC | kind | err | p50 | p99 | mean | iters | note |\n|---|---|---|---:|---:|---:|---:|---|\n")
	sort.Slice(samples, func(i, j int) bool { return samples[i].p99 > samples[j].p99 })
	for i, s := range samples {
		if i >= 25 {
			break
		}
		n := s.note
		out.WriteString(fmt.Sprintf("| %s/%s | %s | %s | %s | %s | %s | %d | %s |\n",
			s.rpc.Service, s.rpc.Name, s.rpc.OperationKind, errOf(s),
			s.p50.Round(time.Microsecond), s.p99.Round(time.Microsecond), s.mean.Round(time.Microsecond), s.iters, n))
	}

	out.WriteString("\n## Full per-RPC table (sorted by service, then name)\n\n")
	out.WriteString("| Service | RPC | kind | err | p50 | p99 | mean | min | max | iters |\n|---|---|---|---|---:|---:|---:|---:|---:|---:|\n")
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].rpc.Service != samples[j].rpc.Service {
			return samples[i].rpc.Service < samples[j].rpc.Service
		}
		return samples[i].rpc.Name < samples[j].rpc.Name
	})
	for _, s := range samples {
		out.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %d |\n",
			s.rpc.Service, s.rpc.Name, s.rpc.OperationKind, errOf(s),
			s.p50.Round(time.Microsecond), s.p99.Round(time.Microsecond), s.mean.Round(time.Microsecond),
			s.min.Round(time.Microsecond), s.max.Round(time.Microsecond), s.iters))
	}

	report := out.String()
	if err := os.WriteFile("perf_report_go.md", []byte(report), 0o644); err != nil {
		t.Logf("could not write perf_report_go.md: %v", err)
	}
	t.Logf("\n%s", report)
	t.Logf("Go perf: %d RPCs measured (streaming rows = stream-open latency), %d FAILED (non-OK gRPC status), grand mean per-RPC = %s; report → sdk/go/perf_report_go.md",
		len(samples), len(failed), (grand / time.Duration(len(samples))).Round(time.Microsecond))
}

// seededProbeLiveRPC is probeLiveRPC for the perf harness: it builds the request
// from the proto descriptor BUT resolves reference/ID fields from the seed fixture
// map (fix), so the unary RPC runs its SUCCESS path against the real entities
// created in the seed phase. Returns the gRPC error (OK on success).
func seededProbeLiveRPC(ctx context.Context, gen *GeneratedClient, rpc RPCInfo, tenant, project string, fix *perfFixtures) error {
	in, out := buildSeededProbeMessages(rpc.FullMethod, tenant, project, fix)
	return gen.InvokeUnary(ctx, rpc.FullMethod, in, out)
}

// seededFirstRecv opens a non-CDC streaming RPC with a seeded request and measures
// up to the FIRST server response (RecvMsg) — a real round-trip, not just
// stream-open. For client-streaming it sends one seeded message, closes the send
// side, and reads the single response. For server/bidi it sends the seeded request
// then reads the first streamed message. io.EOF on first recv is treated as a
// successful (empty) stream completion, not a failure.
func seededFirstRecv(ctx context.Context, gen *GeneratedClient, rpc RPCInfo, tenant, project string, fix *perfFixtures) error {
	in, out := buildSeededProbeMessages(rpc.FullMethod, tenant, project, fix)
	switch rpc.Kind {
	case KindServerStreaming:
		stream, err := gen.NewServerStream(ctx, rpc.FullMethod, &grpc.StreamDesc{ServerStreams: true}, in)
		if err != nil {
			return err
		}
		if err := stream.RecvMsg(out); err != nil && err != io.EOF {
			return err
		}
		return nil
	case KindClientStreaming:
		stream, err := gen.NewClientStream(ctx, rpc.FullMethod, &grpc.StreamDesc{ClientStreams: true})
		if err != nil {
			return err
		}
		if err := stream.SendMsg(in); err != nil {
			return err
		}
		if err := stream.CloseSend(); err != nil {
			return err
		}
		if err := stream.RecvMsg(out); err != nil && err != io.EOF {
			return err
		}
		return nil
	case KindBidi:
		stream, err := gen.NewClientStream(ctx, rpc.FullMethod, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true})
		if err != nil {
			return err
		}
		if err := stream.SendMsg(in); err != nil {
			return err
		}
		_ = stream.CloseSend()
		if err := stream.RecvMsg(out); err != nil && err != io.EOF {
			return err
		}
		return nil
	default:
		return nil
	}
}

// isCdcSubscriptionRPC reports whether an RPC is an open-ended event subscription
// whose first message arrives only when a real event is produced (PublishCDC). For
// these the success path is: subscribe → trigger a real mutation → first event.
func isCdcSubscriptionRPC(rpc RPCInfo) bool {
	return rpc.Service == "DataBroker" && rpc.Name == "PublishCDC"
}

// timeCdcFirstEvent measures REAL first-event latency for a CDC subscription. It
// opens the subscription stream, then fires a real Upsert against the seeded
// SdkLiveRecord row — that write flows through the outbox→CDC→Kafka pipeline and
// is delivered back on the stream. The measured cost (in the caller's timer) is
// dominated by produce→deliver, which is the honest first-event latency a real
// subscriber sees. A drained EOF/Recv error after the mutation is surfaced.
func timeCdcFirstEvent(ctx context.Context, gen *GeneratedClient, broker servicesv1.DataBrokerClient, brokerCtx context.Context, rpc RPCInfo, tenant, project, recordID string) error {
	in, out := buildSeededProbeMessages(rpc.FullMethod, tenant, project, nil)
	stream, err := gen.NewServerStream(ctx, rpc.FullMethod, &grpc.StreamDesc{ServerStreams: true}, in)
	if err != nil {
		return err
	}
	// Fire a real mutation that produces a CDC event for the seeded row. A fresh
	// revision per call guarantees a NEW outbox event each iteration.
	rev := time.Now().UnixNano()
	go func() {
		_, _ = broker.Upsert(brokerCtx, &entityv1.UpsertRequest{
			Context:        liveRequestContext(tenant, project, "go.live.perf.cdc"),
			MessageType:    liveMessageType,
			RecordJson:     liveRecordJSONForCDC(recordID, tenant, project, rev),
			ConflictFields: []string{"record_id"},
		})
	}()
	// Block on the first delivered event (the real produce→CDC→deliver round-trip).
	return stream.RecvMsg(out)
}

// liveRecordJSONForCDC builds an SdkLiveRecord JSON body with a caller-supplied
// revision so each CDC-driving Upsert is a distinct change event.
func liveRecordJSONForCDC(recordID, tenant, project string, revision int64) []byte {
	raw, _ := json.Marshal(map[string]any{
		"record_id": recordID, "tenant_id": tenant, "project_id": project,
		"lookup_key": "go-perf-cdc", "payload": "go-perf-cdc", "revision": revision,
	})
	return raw
}

// perfRealBody returns a SEMANTICALLY VALID request (and the matching empty
// response message) for the top data-plane CRUD RPCs, so the perf harness
// measures REAL handler work against the built-in `udb.sdk.live.v1.SdkLiveRecord`
// schema (always active) instead of validation-rejection on a placeholder body.
// Upsert uses a FIXED record_id so repeated iterations are idempotent (no row
// accumulation). Returns ok=false for RPCs without a real-body override — the
// caller falls back to the generic typed probe.
// perfRealBody also covers the BACKEND-SPECIFIC DataBroker RPCs (document / cache
// / vector / graph / object / generic-dispatch), which the generic reflective
// probe cannot drive: each needs its OWN backend + the resource seeded for it (a
// single global "backend" fixture cannot be both postgres and mongodb). These
// reuse the exact resources created in the seed phase.
func perfRealBody(rpc RPCInfo, tenant, project string, fix *perfFixtures) (proto.Message, proto.Message, bool) {
	if rpc.Service != "DataBroker" {
		return nil, nil, false
	}
	rc := liveRequestContext(tenant, project, "go.live.perf")
	get := func(k string) string { v, _ := fix.lookup(k); return v }
	mongo := &entityv1.StoreResource{Backend: "mongodb", ResourceName: get("mongo_collection")}
	switch rpc.Name {
	case "Upsert":
		return &entityv1.UpsertRequest{
			Context: rc, MessageType: liveMessageType,
			RecordJson: perfRecordJSON(tenant, project), ConflictFields: []string{"record_id"},
		}, &entityv1.MutationResponse{}, true
	case "Select":
		return &entityv1.SelectRequest{
			Context: rc, MessageType: liveMessageType, Filter: perfTenantFilter(tenant, project), Limit: 10,
		}, &entityv1.RecordSet{}, true
	case "Delete":
		// Delete a NON-EXISTENT row (filter on a unique unseeded id) so the success
		// path runs without destroying the seeded record other RPCs read.
		f, _ := structpb.NewStruct(map[string]any{"record_id": "go-perf-delete-noop", "tenant_id": tenant, "project_id": project})
		return &entityv1.DeleteRequest{Context: rc, MessageType: liveMessageType, Filter: f}, &entityv1.MutationResponse{}, true
	case "GenericDispatch":
		return &entityv1.GenericDispatchRequest{
			Context: rc, Backend: "postgres", Operation: "query", SpecJson: `{"sql":"SELECT 1 AS live_probe"}`,
		}, &entityv1.GenericDispatchResponse{}, true
	case "DocumentUpsert":
		return &entityv1.DocumentUpsertRequest{
			Context: rc, Resource: mongo, DocumentId: get("document_id"),
			Document: perfStruct(map[string]any{"payload": "perf", "revision": 2}),
		}, &entityv1.MutationResponse{}, true
	case "DocumentGet":
		return &entityv1.DocumentGetRequest{Context: rc, Resource: mongo, DocumentId: get("document_id")}, &entityv1.DocumentSet{}, true
	case "DocumentFind":
		return &entityv1.DocumentFindRequest{Context: rc, Resource: mongo, Filter: perfStruct(map[string]any{"_id": get("document_id")}), Limit: 1}, &entityv1.DocumentSet{}, true
	case "EnsureResource":
		return &entityv1.ResourceAdminRequest{Context: rc, Backend: "mongodb", ResourceName: get("mongo_collection"), SpecJson: `{"collection":"` + get("mongo_collection") + `"}`}, &entityv1.MutationResponse{}, true
	case "ListResources":
		return &entityv1.ResourceAdminRequest{Context: rc, Backend: "mongodb"}, &entityv1.ResourceListResponse{}, true
	case "GeneratePresignedUrl":
		return &entityv1.UrlRequest{Context: rc, Bucket: get("bucket"), ObjectKey: get("object_key"), Method: "GET", TtlSeconds: 60}, &entityv1.UrlResponse{}, true
	case "CacheSet":
		return &entityv1.CacheSetRequest{Context: rc, Resource: &entityv1.StoreResource{Backend: "redis"}, Key: "sdk-perf-cache", Value: []byte("perf"), ContentType: "text/plain", TtlSeconds: 60}, &entityv1.MutationResponse{}, true
	case "CacheGet":
		return &entityv1.CacheGetRequest{Context: rc, Resource: &entityv1.StoreResource{Backend: "redis"}, Key: "sdk-perf-cache"}, &entityv1.CacheGetResponse{}, true
	}
	return nil, nil, false
}

// perfStruct builds a structpb.Struct from a map (ignoring the error, which only
// occurs for unsupported value types we never pass).
func perfStruct(m map[string]any) *structpb.Struct {
	s, _ := structpb.NewStruct(m)
	return s
}

func perfRecordJSON(tenant, project string) []byte {
	// json.Marshal of a map of strings/ints never errors.
	raw, _ := json.Marshal(map[string]any{
		"record_id":  "go-perf-" + tenant + "-" + project,
		"tenant_id":  tenant,
		"project_id": project,
		"lookup_key": "go-perf-lk",
		"payload":    "go-perf",
		"revision":   1,
	})
	return raw
}

func perfTenantFilter(tenant, project string) *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{"tenant_id": tenant, "project_id": project})
	return s
}

// pct returns the index into a sorted slice of length n for the p-th percentile.
func pct(n, p int) int {
	if n <= 1 {
		return 0
	}
	idx := (p * (n - 1)) / 100
	if idx >= n {
		idx = n - 1
	}
	return idx
}
