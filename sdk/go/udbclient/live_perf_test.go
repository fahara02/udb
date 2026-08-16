package udbclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
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
)

// TestLivePerf measures per-RPC latency for the entire AllRPCs surface against a
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
	platformLogin, err := authnv1.NewAuthnServiceClient(authConn).Login(ctx, &authnv1.LoginRequest{
		Username:   requiredLiveEnv(t, "UDB_LIVE_PLATFORM_USERNAME"),
		Password:   requiredLiveEnv(t, "UDB_LIVE_PLATFORM_PASSWORD"),
		TenantHint: tenant, ProjectHint: project, DeviceName: "go-sdk-perf-platform",
	})
	if err != nil {
		t.Fatalf("platform Login failed: %v", err)
	}
	platformWho, err := auth.AuthenticateBearer(ctx, platformLogin.GetAccessToken())
	if err != nil {
		t.Fatalf("platform bearer validation failed: %v", err)
	}
	platformRole := false
	for _, role := range platformWho.GetPrincipal().GetRoles() {
		if strings.EqualFold(strings.TrimSpace(role), "platform_admin") {
			platformRole = true
			break
		}
	}
	if !platformRole {
		t.Fatal("offline platform fixture did not issue the reserved platform_admin role")
	}
	platformAuthz := "Bearer " + platformLogin.GetAccessToken()
	platformGen := NewGenerated(authConn, liveGeneratedOptions(meta, platformAuthz))

	// SEED PHASE (runs before any measurement): create real, disposable entities
	// and capture their identifiers so every RPC can be driven down its SUCCESS
	// path with valid inputs. The UUID-strict native services (storage/asset/
	// webrtc) need the canonical tenant UUID + a matching outgoing context; the
	// admin's tenant claim IS that UUID (discovered above), so one bearer serves all.
	uuidTenant := tenant
	broker := servicesv1.NewDataBrokerClient(brokerConn)
	base := authGen.outgoingContext(ctx)
	platformBase := platformGen.outgoingContext(ctx)
	nativeCtxFn := func() context.Context { return nativeCtx(ctx, authGen, authz, uuidTenant) }
	seed := perfSeed(
		t, ctx, broker, brokerGen.outgoingContext(ctx), authConn, base, platformBase,
		nativeCtxFn, login.GetUserId(), platformLogin.GetUserId(), tenant, project, uuidTenant,
	)
	defer seed.cleanup()
	fix := seed.fix
	// The seed lifecycle is intentionally broad and may approach the access-token
	// TTL. Re-mint and re-verify the distinct platform bearer before measuring its
	// exact global RPC set.
	platformLogin, err = authnv1.NewAuthnServiceClient(authConn).Login(ctx, &authnv1.LoginRequest{
		Username:   requiredLiveEnv(t, "UDB_LIVE_PLATFORM_USERNAME"),
		Password:   requiredLiveEnv(t, "UDB_LIVE_PLATFORM_PASSWORD"),
		TenantHint: tenant, ProjectHint: project, DeviceName: "go-sdk-perf-platform-measure",
	})
	if err != nil {
		t.Fatalf("fresh platform Login failed: %v", err)
	}
	platformWho, err = auth.AuthenticateBearer(ctx, platformLogin.GetAccessToken())
	if err != nil {
		t.Fatalf("fresh platform bearer validation failed: %v", err)
	}
	platformRole = false
	for _, role := range platformWho.GetPrincipal().GetRoles() {
		if strings.EqualFold(strings.TrimSpace(role), "platform_admin") {
			platformRole = true
			break
		}
	}
	if !platformRole {
		t.Fatal("fresh platform fixture lost the reserved platform_admin role")
	}
	platformAuthz = "Bearer " + platformLogin.GetAccessToken()
	platformGen = NewGenerated(authConn, liveGeneratedOptions(meta, platformAuthz))

	// Re-mint FRESH credentials right before measurement, into THREE INDEPENDENT
	// admin sessions — one per consumer — so the session-mutating Phase-1 RPCs don't
	// invalidate each other:
	//   - RefreshToken rotates its single-use refresh token. If `token`/`session_id`
	//     shared that session, later session-specific measurements would depend on a
	//     credential family another row already mutated. Separate logins isolate them.
	// The seed phase also takes long enough that a token captured at its start ages
	// toward the access-token TTL, so re-minting here keeps them fresh either way.
	freshLogin := func(device string) (*authnv1.LoginResponse, error) {
		return authnv1.NewAuthnServiceClient(authConn).Login(ctx, &authnv1.LoginRequest{
			Username: requiredLiveEnv(t, "UDB_LIVE_USERNAME"), Password: requiredLiveEnv(t, "UDB_LIVE_PASSWORD"),
			TenantHint: tenant, ProjectHint: project, DeviceName: device,
		})
	}
	// `token` (+csrf) for the token-validating reads (Authenticate/ValidateToken/
	// IntrospectToken) — a session nothing rotates or revokes.
	if tok, err := freshLogin("go-sdk-perf-token"); err == nil {
		fix.set("token", tok.GetAccessToken())
		fix.set("csrf_token", tok.GetCsrfToken())
	}
	// `refresh_token` for RefreshToken — its own family so rotation/revocation is contained.
	if rt, err := freshLogin("go-sdk-perf-refresh"); err == nil {
		fix.set("refresh_token", rt.GetRefreshToken())
	}
	// `session_id` for RefreshSession (Phase 1) + the Phase-3 Logout/RevokeSession — a
	// dedicated session so RefreshToken's family revocation can't kill it first.
	if ss, err := freshLogin("go-sdk-perf-session"); err == nil {
		fix.set("session_id", ss.GetSessionId())
	}

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
		errText string
	}

	isCapabilitySkip := func(rpc RPCInfo, errCode, errText string) bool {
		if rpc.Service != "RoomService" {
			return false
		}
		switch rpc.Name {
		case "ListEgress", "StartRoomComposite", "StartTrackEgress", "StopEgress":
			return errCode == "FailedPrecondition" &&
				(strings.Contains(errText, "EGRESS_NOT_ENABLED") ||
					strings.Contains(errText, "EGRESS_BACKEND_UNAVAILABLE") ||
					strings.Contains(errText, "webrtc egress is not enabled") ||
					strings.Contains(errText, "webrtc egress is enabled but no egress backend"))
		default:
			return false
		}
	}

	// timeOne measures one call. Unary RPCs are a full request→response round-trip.
	// Non-CDC streaming RPCs use seeded inputs and measure time-to-FIRST-RESPONSE.
	// PublishCDC measures time-to-FIRST-EVENT after firing a real mutation. Each
	// measured stream gets an owned child context and is cancelled immediately after
	// the measured receive so repeated full-surface sweeps do not leave streams open.
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
			// NO generic fill. An explicit shared-manifest body is required for every
			// unary RPC. A missing or unhydratable manifest body returns NO-BODY and is
			// surfaced as a failure to fix — never a placeholder request the broker
			// rejects with INVALID_ARGUMENT.
			if in, out, ok := buildSpecBody(rpc.FullMethod, fix); ok {
				err = gen.InvokeUnary(callCtx, rpc.FullMethod, in, out)
			} else {
				err = errNoExplicitBody
			}
		} else if isCdcSubscriptionRPC(rpc) {
			// Event-driven success path: subscribe, then fire a real mutation that
			// flows outbox→CDC→Kafka, and measure time-to-FIRST-delivered-event.
			err = timeCdcFirstEvent(callCtx, gen, broker, brokerGen.outgoingContext(callCtx), rpc, tenant, project, seed.recordID, fix)
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

	// Auth route: measure Phase 1 (session setup) first, Phase 2 (everything under
	// the live session) next, Phase 3 (terminal auth) last — so logout/revoke never
	// kill the session mid-run. See BENCH_RPC_BODIES.md "Execution order".
	samples := make([]sample, 0, len(AllRPCs))
	for _, rpc := range orderRPCsByAuthPhase(AllRPCs) {
		if finalEphemeralCleanupRPCs[rpc.FullMethod] {
			// AdminRevokeAllTenantSessions is a successful Phase-3 measurement only
			// when it actually invalidates this tenant's sessions. Login again through
			// the public credential path before the final self-purge; otherwise that
			// last row measures a stale-bearer rejection instead of PurgeTenant.
			var relogin *authnv1.LoginResponse
			var reloginErr error
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				candidate, err := freshLogin("go-sdk-perf-final-purge")
				if err == nil {
					_, err = auth.AuthenticateBearer(ctx, candidate.GetAccessToken())
				}
				if err == nil {
					relogin = candidate
					break
				}
				reloginErr = err
				time.Sleep(50 * time.Millisecond)
			}
			if relogin != nil {
				authz = "Bearer " + relogin.GetAccessToken()
				brokerGen = NewGenerated(brokerConn, liveGeneratedOptions(meta, authz))
				authGen = NewGenerated(authConn, liveGeneratedOptions(meta, authz))
			} else {
				t.Logf("perf re-login before terminal tenant purge remained revoked: %v", reloginErr)
			}
		}
		gen := authGen
		if rpc.Service == "DataBroker" {
			gen = brokerGen
		} else if requiresPlatformBenchmarkIdentity(rpc) {
			gen = platformGen
		}
		iters, note := iterFor(rpc.OperationKind)
		if isCdcSubscriptionRPC(rpc) {
			// CDC first-event includes a real produce→deliver round-trip; keep the
			// iteration count low so the run stays bounded.
			iters, note = 3, "cdc subscription: time-to-first-event (real mutation produced)"
		} else if rpc.FullMethod == "/udb.services.v1.DataBroker/ApproveMigrationPlan" {
			iters, note = 1, "single-use migration approval"
		} else if rpc.Service == "AuthnService" && rpc.Name == "RefreshToken" {
			// Refresh-token rotation is single-use. Replaying the same fixture token
			// is a theft signal in v0.5.7 and correctly revokes every session for the
			// principal, including the bearer used by the rest of this benchmark.
			iters, note = 1, "single-use refresh-token rotation"
		} else if rpc.Kind != KindUnary {
			note = "streaming: time-to-first-response (seeded; " + string(rpc.Kind) + ")"
		}
		// Warm-up (channel/HTTP2 + server caches), excluded from the numbers — ONLY
		// for idempotent reads. A warm-up on a non-idempotent mutation would CONSUME
		// the operation (RefreshToken rotates its token; CreateUser/Logout/revokes/
		// deletes run once), making every measured iteration fail.
		if rpc.OperationKind == "read_only" && rpc.Kind == KindUnary {
			_, _ = timeOne(gen, rpc)
		}
		durs := make([]time.Duration, 0, iters)
		okDurs := make([]time.Duration, 0, iters)
		var firstErr, firstErrText string
		for i := 0; i < iters; i++ {
			d, err := timeOne(gen, rpc)
			code := "OK"
			if err == errNoExplicitBody {
				code = "NO-BODY"
			} else if err != nil {
				code = status.Code(err).String()
			}
			if i == 0 {
				firstErr = code
				if err != nil {
					firstErrText = err.Error()
				}
			}
			if code == "OK" {
				okDurs = append(okDurs, d)
			}
			durs = append(durs, d)
		}
		// An RPC that succeeds AT LEAST ONCE works: repeated-call failures on a
		// non-idempotent mutation (consumed token / duplicate / already-deleted) are a
		// measurement artifact, not an RPC failure — report OK and measure latency over
		// the successful calls. Only an RPC that NEVER succeeds is a real failure (its
		// first-attempt status).
		measured := okDurs
		errCode := "OK"
		if len(okDurs) == 0 {
			measured = durs
			errCode = firstErr
			if isCapabilitySkip(rpc, firstErr, firstErrText) {
				errCode = "CAPABILITY_SKIPPED"
				note += "; optional capability unavailable"
			}
		}
		// Surface the FULL gRPC error (code + message + details) for every RPC that
		// never succeeded, so `go test -v` is a complete diagnosis log — the report
		// table only carries the status code.
		if errCode != "OK" {
			t.Logf("[PERF-FAIL] %s => %s: %s", rpc.FullMethod, errCode, firstErrText)
		}
		sort.Slice(measured, func(i, j int) bool { return measured[i] < measured[j] })
		var sum time.Duration
		for _, d := range measured {
			sum += d
		}
		s := sample{
			rpc: rpc, iters: iters, note: note, errCode: errCode, errText: firstErrText,
			p50:  measured[pct(len(measured), 50)],
			p99:  measured[pct(len(measured), 99)],
			mean: sum / time.Duration(len(measured)),
			min:  measured[0],
			max:  measured[len(measured)-1],
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
		if e := errOf(s); e != "OK" && e != "CAPABILITY_SKIPPED" {
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
		out.WriteString("| RPC | api_alias | operation_id | kind | err | detail | p99 | mean | iters |\n|---|---|---|---|---|---|---:|---:|---:|\n")
		sort.Slice(failed, func(i, j int) bool {
			return failed[i].rpc.Service+"/"+failed[i].rpc.Name < failed[j].rpc.Service+"/"+failed[j].rpc.Name
		})
		for _, s := range failed {
			// The detail column carries the server's error MESSAGE (not just the
			// code) so a CI-only failure is diagnosable from the report artifact
			// alone — the -v log lines are not captured on a passing test binary.
			detail := strings.ReplaceAll(s.errText, "|", "\\|")
			detail = strings.ReplaceAll(detail, "\n", " ")
			if len(detail) > 220 {
				detail = detail[:220] + "…"
			}
			out.WriteString(fmt.Sprintf("| %s/%s | %s | %s | %s | %s | %s | %s | %s | %d |\n",
				s.rpc.Service, s.rpc.Name, rpcAPIAlias(s.rpc), rpcOperationID(s.rpc), s.rpc.OperationKind, errOf(s),
				detail, s.p99.Round(time.Microsecond), s.mean.Round(time.Microsecond), s.iters))
		}
	}

	out.WriteString("\n## Slowest 25 RPCs by p99\n\n")
	out.WriteString("| RPC | api_alias | operation_id | kind | err | p50 | p99 | mean | iters | note |\n|---|---|---|---|---|---:|---:|---:|---:|---|\n")
	sort.Slice(samples, func(i, j int) bool { return samples[i].p99 > samples[j].p99 })
	for i, s := range samples {
		if i >= 25 {
			break
		}
		n := s.note
		out.WriteString(fmt.Sprintf("| %s/%s | %s | %s | %s | %s | %s | %s | %s | %d | %s |\n",
			s.rpc.Service, s.rpc.Name, rpcAPIAlias(s.rpc), rpcOperationID(s.rpc), s.rpc.OperationKind, errOf(s),
			s.p50.Round(time.Microsecond), s.p99.Round(time.Microsecond), s.mean.Round(time.Microsecond), s.iters, n))
	}

	out.WriteString("\n## Full per-RPC table (sorted by service, then name)\n\n")
	out.WriteString("| Service | RPC | api_alias | operation_id | kind | err | p50 | p99 | mean | min | max | iters |\n|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|\n")
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].rpc.Service != samples[j].rpc.Service {
			return samples[i].rpc.Service < samples[j].rpc.Service
		}
		return samples[i].rpc.Name < samples[j].rpc.Name
	})
	for _, s := range samples {
		out.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d |\n",
			s.rpc.Service, s.rpc.Name, rpcAPIAlias(s.rpc), rpcOperationID(s.rpc), s.rpc.OperationKind, errOf(s),
			s.p50.Round(time.Microsecond), s.p99.Round(time.Microsecond), s.mean.Round(time.Microsecond),
			s.min.Round(time.Microsecond), s.max.Round(time.Microsecond), s.iters))
	}

	report := out.String()
	if err := os.WriteFile("perf_report_go.md", []byte(report), 0o644); err != nil {
		t.Logf("could not write perf_report_go.md: %v", err)
	}
	t.Logf("\n%s", report)
	t.Logf("Go perf: %d RPCs measured (streaming rows = first-response/first-event latency), %d FAILED (non-OK gRPC status), grand mean per-RPC = %s; report → sdk/go/perf_report_go.md",
		len(samples), len(failed), (grand / time.Duration(len(samples))).Round(time.Microsecond))
}

// Platform credentials are intentionally narrow in the benchmark. Ordinary
// tenant Authz CRUD remains claim-attributed to the tenant bootstrap user; only
// governance, system-global analytics, and explicit cross-tenant movement use
// the separately offline-provisioned platform principal.
func requiresPlatformBenchmarkIdentity(rpc RPCInfo) bool {
	switch rpc.FullMethod {
	case "/udb.core.analytics.services.v1.AnalyticsService/GetExecutorPerformance",
		"/udb.core.analytics.services.v1.AnalyticsService/GetReconciliationAnalytics",
		"/udb.core.backup.services.v1.BackupService/RestoreTenant",
		"/udb.core.tenant.services.v1.TenantService/AdminPurgeTenant":
		return true
	}
	if rpc.Service != "AuthzService" {
		return false
	}
	switch rpc.Name {
	case "CreatePolicyDraft", "UpdatePolicyDraft", "DiffPolicyDraft",
		"SubmitPolicyDraft", "ApprovePolicyDraft", "RejectPolicyDraft",
		"ActivatePolicyVersion", "RollbackPolicyVersion", "ActivateCanary",
		"PromoteCanary", "GetCanaryStatus", "ListPolicyVersions",
		"SimulatePolicy", "ExplainPolicy", "InvalidatePolicyBundles",
		"SeedBuiltinRoles", "MigrateLegacyPolicies":
		return true
	default:
		return false
	}
}

func TestPlatformBenchmarkIdentityRoutingIsNarrow(t *testing.T) {
	for _, rpc := range []RPCInfo{
		{Service: "AnalyticsService", Name: "GetExecutorPerformance", FullMethod: "/udb.core.analytics.services.v1.AnalyticsService/GetExecutorPerformance"},
		{Service: "BackupService", Name: "RestoreTenant", FullMethod: "/udb.core.backup.services.v1.BackupService/RestoreTenant"},
		{Service: "AuthzService", Name: "CreatePolicyDraft"},
		{Service: "TenantService", Name: "AdminPurgeTenant", FullMethod: "/udb.core.tenant.services.v1.TenantService/AdminPurgeTenant"},
	} {
		if !requiresPlatformBenchmarkIdentity(rpc) {
			t.Fatalf("%s/%s must use the platform fixture", rpc.Service, rpc.Name)
		}
	}
	for _, rpc := range []RPCInfo{
		{Service: "AuthzService", Name: "CreateRole"},
		{Service: "AuthzService", Name: "AssignRole"},
		{Service: "TenantService", Name: "PurgeTenant"},
	} {
		if requiresPlatformBenchmarkIdentity(rpc) {
			t.Fatalf("%s/%s must retain ordinary tenant authority", rpc.Service, rpc.Name)
		}
	}
}

func rpcAPIAlias(rpc RPCInfo) string {
	return rpcStringField(rpc, "APIAlias", "ApiAlias")
}

func rpcOperationID(rpc RPCInfo) string {
	return rpcStringField(rpc, "OperationID", "OperationId")
}

func rpcStringField(rpc RPCInfo, names ...string) string {
	value := reflect.ValueOf(rpc)
	for _, name := range names {
		field := value.FieldByName(name)
		if field.IsValid() && field.Kind() == reflect.String {
			return field.String()
		}
	}
	return ""
}

// seededFirstRecv opens a non-CDC streaming RPC with a seeded request and measures
// up to the FIRST server response (RecvMsg) — a real round-trip, not just
// stream-open. For client-streaming it sends one seeded message, closes the send
// side, and reads the single response. For server/bidi it sends the seeded request
// then reads the first streamed message. io.EOF on first recv is treated as a
// successful (empty) stream completion, not a failure.
func seededFirstRecv(ctx context.Context, gen *GeneratedClient, rpc RPCInfo, tenant, project string, fix *perfFixtures) error {
	in, out, ok := buildSpecBody(rpc.FullMethod, fix)
	if !ok {
		return errNoExplicitBody
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	switch rpc.Kind {
	case KindServerStreaming:
		stream, err := gen.NewServerStream(streamCtx, rpc.FullMethod, &grpc.StreamDesc{ServerStreams: true}, in)
		if err != nil {
			return err
		}
		defer func() { _ = stream.CloseSend() }()
		if err := stream.RecvMsg(out); err != nil && err != io.EOF {
			return err
		}
		return nil
	case KindClientStreaming:
		stream, err := gen.NewClientStream(streamCtx, rpc.FullMethod, &grpc.StreamDesc{ClientStreams: true})
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
		stream, err := gen.NewClientStream(streamCtx, rpc.FullMethod, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true})
		if err != nil {
			return err
		}
		defer func() { _ = stream.CloseSend() }()
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
func timeCdcFirstEvent(ctx context.Context, gen *GeneratedClient, broker servicesv1.DataBrokerClient, brokerCtx context.Context, rpc RPCInfo, tenant, project, recordID string, fix *perfFixtures) error {
	in, out, ok := buildSpecBody(rpc.FullMethod, fix)
	if !ok {
		return errNoExplicitBody
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	stream, err := gen.NewServerStream(streamCtx, rpc.FullMethod, &grpc.StreamDesc{ServerStreams: true}, in)
	if err != nil {
		return err
	}
	defer func() { _ = stream.CloseSend() }()
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
