package udbclient

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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
			d = 5 * time.Second
		}
		callCtx, c := context.WithTimeout(ctx, d)
		defer c()
		start := time.Now()
		var err error
		if rpc.Kind == KindUnary {
			err = probeLiveRPC(callCtx, gen, rpc, tenant, project)
		} else {
			err = openLiveStream(callCtx, gen, rpc, tenant, project)
		}
		return time.Since(start), err
	}

	// Iteration budget per operation_kind. Destructive: one typed-empty validation
	// call. Mutation: a few. Read: enough for a stable p99.
	iterFor := func(kind string) (int, string) {
		switch kind {
		case "destructive":
			return 1, "destructive: 1 typed-empty validation call (action suppressed)"
		case "mutation":
			return 5, "mutation"
		default:
			return 25, "read_only"
		}
	}

	samples := make([]sample, 0, len(AllRPCs))
	for _, rpc := range AllRPCs {
		gen := authGen
		if rpc.Service == "DataBroker" {
			gen = brokerGen
		}
		iters, note := iterFor(rpc.OperationKind)
		if rpc.Kind != KindUnary {
			note = "stream-open (" + string(rpc.Kind) + "; not a unary round-trip)"
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
	out.WriteString("Unary RPCs = full request→response round-trip. Streaming RPCs (server/client/bidi) " +
		"report STREAM-OPEN latency (initiate + send request + CloseSend), NOT first-message latency: " +
		"a subscription stream's first message arrives only on an event, so draining it in a passive run " +
		"would just hit the deadline. Streaming rows are marked in the note column.\n\n")
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

	out.WriteString("\n## Slowest 25 RPCs by p99\n\n")
	out.WriteString("| RPC | kind | p50 | p99 | mean | iters | note |\n|---|---|---:|---:|---:|---:|---|\n")
	sort.Slice(samples, func(i, j int) bool { return samples[i].p99 > samples[j].p99 })
	for i, s := range samples {
		if i >= 25 {
			break
		}
		n := s.note
		if s.errCode != "" {
			n += " (last code=" + s.errCode + ")"
		}
		out.WriteString(fmt.Sprintf("| %s/%s | %s | %s | %s | %s | %d | %s |\n",
			s.rpc.Service, s.rpc.Name, s.rpc.OperationKind,
			s.p50.Round(time.Microsecond), s.p99.Round(time.Microsecond), s.mean.Round(time.Microsecond), s.iters, n))
	}

	out.WriteString("\n## Full per-RPC table (sorted by service, then name)\n\n")
	out.WriteString("| Service | RPC | kind | p50 | p99 | mean | min | max | iters |\n|---|---|---|---:|---:|---:|---:|---:|---:|\n")
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].rpc.Service != samples[j].rpc.Service {
			return samples[i].rpc.Service < samples[j].rpc.Service
		}
		return samples[i].rpc.Name < samples[j].rpc.Name
	})
	for _, s := range samples {
		out.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s | %s | %d |\n",
			s.rpc.Service, s.rpc.Name, s.rpc.OperationKind,
			s.p50.Round(time.Microsecond), s.p99.Round(time.Microsecond), s.mean.Round(time.Microsecond),
			s.min.Round(time.Microsecond), s.max.Round(time.Microsecond), s.iters))
	}

	report := out.String()
	if err := os.WriteFile("perf_report_go.md", []byte(report), 0o644); err != nil {
		t.Logf("could not write perf_report_go.md: %v", err)
	}
	t.Logf("\n%s", report)
	t.Logf("Go perf: %d RPCs measured (streaming rows = stream-open latency), grand mean per-RPC = %s; report → sdk/go/perf_report_go.md",
		len(samples), (grand / time.Duration(len(samples))).Round(time.Microsecond))
}

// openLiveStream establishes a streaming RPC and returns as soon as the stream is
// open (request sent, send-side closed) WITHOUT waiting for a server response. This
// is the data-independent latency a client pays to start the stream — unlike
// probeLiveRPC's RecvMsg, it does not block waiting for a first message that a
// passive subscription never emits. The caller's deferred context-cancel tears the
// stream down.
func openLiveStream(ctx context.Context, gen *GeneratedClient, rpc RPCInfo, tenant, project string) error {
	in, _ := buildProbeMessages(rpc.FullMethod, tenant, project)
	switch rpc.Kind {
	case KindServerStreaming:
		// NewServerStream sends the request as part of opening the stream.
		_, err := gen.NewServerStream(ctx, rpc.FullMethod, &grpc.StreamDesc{ServerStreams: true}, in)
		return err
	case KindClientStreaming:
		stream, err := gen.NewClientStream(ctx, rpc.FullMethod, &grpc.StreamDesc{ClientStreams: true})
		if err != nil {
			return err
		}
		if err := stream.SendMsg(in); err != nil {
			return err
		}
		return stream.CloseSend()
	case KindBidi:
		stream, err := gen.NewClientStream(ctx, rpc.FullMethod, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true})
		if err != nil {
			return err
		}
		if err := stream.SendMsg(in); err != nil {
			return err
		}
		return stream.CloseSend()
	default:
		return nil
	}
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
