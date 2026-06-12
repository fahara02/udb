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

	timeOne := func(gen *GeneratedClient, rpc RPCInfo) (time.Duration, error) {
		callCtx, c := context.WithTimeout(ctx, 20*time.Second)
		defer c()
		start := time.Now()
		err := probeLiveRPC(callCtx, gen, rpc, tenant, project)
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

	// Streaming RPCs are EXCLUDED from the latency aggregate: a server-streaming
	// subscription (PublishCDC, SelectV2, GetObject, …) or a client-streaming
	// upload has no well-defined single request→response latency — draining it to
	// the deadline would inject a 20 s "timeout" into the mean and make the
	// per-service number a lie (this is exactly what inflated DataBroker to 272 ms).
	// We measure only unary RPCs, where round-trip latency is meaningful, and list
	// the excluded streaming RPCs explicitly so the omission is honest, not hidden.
	streamingExcluded := make([]string, 0, 12)
	samples := make([]sample, 0, len(AllRPCs))
	for _, rpc := range AllRPCs {
		if rpc.Kind != KindUnary {
			streamingExcluded = append(streamingExcluded, fmt.Sprintf("%s/%s (%s)", rpc.Service, rpc.Name, rpc.Kind))
			continue
		}
		gen := authGen
		if rpc.Service == "DataBroker" {
			gen = brokerGen
		}
		iters, note := iterFor(rpc.OperationKind)
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
	out.WriteString(fmt.Sprintf("Unary RPCs measured: %d   tenant=%s\n\n", len(samples), tenant))
	out.WriteString(fmt.Sprintf("Streaming RPCs excluded from latency (no well-defined request/response latency — a subscription/upload stream stays open): %d — %s\n\n",
		len(streamingExcluded), strings.Join(streamingExcluded, ", ")))
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
	t.Logf("Go perf: %d unary RPCs measured (%d streaming excluded), grand mean per-RPC = %s; report → sdk/go/perf_report_go.md",
		len(samples), len(streamingExcluded), (grand / time.Duration(len(samples))).Round(time.Microsecond))
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
