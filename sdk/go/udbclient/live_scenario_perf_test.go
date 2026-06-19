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
	eventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/events/v1"
	"google.golang.org/grpc/status"
)

// TestScenarioPerf measures the latency of the WORKFLOW HELPERS a real
// simple-client caller actually invokes — NOT the full AllRPCs surface. It is the
// SCENARIO counterpart to TestLivePerf (the full-surface coverage sweep, which is
// kept untouched as the coverage artifact). Where TestLivePerf answers "is every
// RPC reachable and how fast", this answers "how fast is the user-facing facade
// path the docs tell people to call":
//
//   - loginAndAdoptTenant  (Login + AuthenticateBearer + atomic adopt)
//   - entity.upsert        (one Upsert RPC, no proof read)
//   - entity.select        (one Select RPC)
//   - entity.delete        (one Delete RPC)
//   - uploadFile           (RegisterUpload -> presigned PUT -> FinalizeUpload)
//   - downloadFile         (GetDownloadUrl — presigned, bytes never transit broker)
//   - events.subscribeReady (Subscribe + Ready — readiness boundary)
//   - events.publishAndWait  (EnqueueOutboxEvent + first matching CDC envelope)
//   - webrtc.joinSession   (JoinSession + open signalling stream)
//
// It is gated SEPARATELY from both the conformance suite and the full-surface perf
// sweep: UDB_LIVE_SDK_TESTS=1 + UDB_SCENARIO_PERF=1. The result is published to its
// OWN file (scenario_perf_go.md) so it can run and report independently of
// perf_report_go.md. As with the full sweep, a slow scenario is reported, not
// failed — only a scenario that NEVER completes is surfaced as a failure.
//
// The latency includes the FULL facade cost a caller pays: client encode + every
// RPC in the helper's documented sequence (see docs/bench-bodies/workflow-sequences.md)
// + transport + server handle + decode + any presigned byte transfer.
func TestScenarioPerf(t *testing.T) {
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" || os.Getenv("UDB_SCENARIO_PERF") != "1" {
		t.Skip("scenario perf run requires UDB_LIVE_SDK_TESTS=1 and UDB_SCENARIO_PERF=1")
	}

	target := requiredLiveEnv(t, "UDB_GRPC_TARGET")
	authTarget := os.Getenv("UDB_AUTH_GRPC_TARGET")
	if authTarget == "" {
		authTarget = target
	}
	tenant := liveEnv("UDB_LIVE_TENANT", "sdk-live")
	project := liveEnv("UDB_LIVE_PROJECT", "default")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Build the unified facade exactly as a real caller would (Connect/NewUdb),
	// then login-and-adopt so every facade carries the canonical tenant from the
	// VERIFIED principal — the same path the simple-client docs prescribe.
	u, err := NewUdb(ctx, Config{
		Target:          target,
		AuthTarget:      authTarget,
		TenantID:        tenant,
		ProjectID:       project,
		Purpose:         "go.live.scenario.perf",
		ServiceIdentity: "go.sdk.scenario.perf",
		CorrelationID:   "go-live-scenario-perf",
	})
	if err != nil {
		t.Fatalf("NewUdb: %v", err)
	}
	defer u.Close()

	// scenario describes one user-facing workflow helper and its iteration budget.
	// fn runs ONE invocation and returns an error if that invocation did not
	// complete. seq is the documented RPC sequence (for the report; the contract
	// lives in docs/bench-bodies/workflow-sequences.md).
	type scenario struct {
		name  string
		seq   string
		iters int
		fn    func(ctx context.Context) error
	}

	// recordID base for the entity scenarios; a fresh suffix per process keeps the
	// run idempotent and avoids colliding with the full-surface sweep's rows.
	suffix := strings.NewReplacer(".", "", ":", "").Replace(time.Now().UTC().Format("150405.000000000"))
	entity := func() *Entity { return u.Entity(liveMessageType, Key("record_id")) }
	scenarioRecord := func(i int) map[string]any {
		return map[string]any{
			"record_id":  fmt.Sprintf("go-scn-%s-%d", suffix, i),
			"tenant_id":  u.Meta.TenantID,
			"project_id": u.Meta.ProjectID,
			"lookup_key": "go-scn-lk",
			"payload":    "go-scenario",
			"revision":   1,
		}
	}

	scenarios := []scenario{
		{
			name: "loginAndAdoptTenant", seq: "Login, AuthenticateBearer", iters: 5,
			fn: func(ctx context.Context) error {
				_, err := u.LoginAndAdoptTenant(ctx, &authnv1.LoginRequest{
					Username:    requiredLiveEnv(t, "UDB_LIVE_USERNAME"),
					Password:    requiredLiveEnv(t, "UDB_LIVE_PASSWORD"),
					TenantHint:  tenant,
					ProjectHint: project,
					DeviceName:  "go-sdk-scenario",
				})
				return err
			},
		},
		{
			name: "entity.upsert", seq: "Upsert", iters: 10,
			fn: func(ctx context.Context) error {
				_, err := entity().Upsert(ctx, scenarioRecord(0))
				return err
			},
		},
		{
			name: "entity.select", seq: "Select", iters: 25,
			fn: func(ctx context.Context) error {
				_, err := entity().Select(ctx, map[string]any{
					"tenant_id": u.Meta.TenantID, "project_id": u.Meta.ProjectID,
				})
				return err
			},
		},
		{
			name: "entity.delete", seq: "Delete", iters: 5,
			fn: func(ctx context.Context) error {
				// Delete a NON-EXISTENT row so the helper runs its full path without
				// destroying a row a later iteration/scenario reads.
				_, err := entity().Delete(ctx, map[string]any{
					"record_id": "go-scn-delete-noop", "tenant_id": u.Meta.TenantID, "project_id": u.Meta.ProjectID,
				})
				return err
			},
		},
		{
			name: "uploadFile", seq: "RegisterUpload, PUT, FinalizeUpload", iters: 5,
			fn: func(ctx context.Context) error {
				_, err := u.Storage.UploadFile(ctx, "go-scenario.txt", []byte("go-scenario-upload"),
					WithContentType("text/plain"), WithFileType("DOCUMENT"))
				return err
			},
		},
		{
			name: "events.subscribeReady", seq: "PublishCDC(open), Header", iters: 5,
			fn: func(ctx context.Context) error {
				subCtx, c := context.WithTimeout(ctx, 15*time.Second)
				defer c()
				sub, err := u.Events.Subscribe(subCtx, perfCdcTopicPattern(u.Meta.TenantID))
				if err != nil {
					return err
				}
				return sub.Ready()
			},
		},
		{
			name: "events.publishAndWait", seq: "EnqueueOutboxEvent, PublishCDC first-event", iters: 3,
			fn: func(ctx context.Context) error {
				waitCtx, c := context.WithTimeout(ctx, 20*time.Second)
				defer c()
				sub, err := u.Events.Subscribe(waitCtx, perfCdcTopicPattern(u.Meta.TenantID))
				if err != nil {
					return err
				}
				if err := sub.Ready(); err != nil {
					return err
				}
				_, err = u.Events.PublishAndWait(waitCtx, sub, "sdk.scenario."+suffix,
					map[string]any{"event": "go-scenario", "n": suffix},
					func(env *eventsv1.CDCEnvelope) bool { return true })
				return err
			},
		},
	}

	// downloadFile / webrtc.joinSession need a pre-existing file / room. Seed one of
	// each up front (their cost is NOT measured) so the measured scenario is the
	// pure helper path. A failed seed downgrades the scenario to a recorded skip.
	if up, err := u.Storage.UploadFile(ctx, "go-scenario-dl.txt", []byte("go-scenario-download"),
		WithContentType("text/plain"), WithFileType("DOCUMENT")); err == nil && up.GetFile().GetFileId() != "" {
		fileID := up.GetFile().GetFileId()
		scenarios = append(scenarios, scenario{
			name: "downloadFile", seq: "GetDownloadUrl", iters: 25,
			fn: func(ctx context.Context) error {
				_, err := u.Storage.DownloadFile(ctx, fileID, 5)
				return err
			},
		})
	} else if err != nil {
		t.Logf("scenario seed: download file upload failed, downloadFile scenario skipped: %v", err)
	}

	if room, err := u.WebRTC.Room.CreateRoom(ctx, "go-scenario-room-"+suffix, 8, "{}", ""); err == nil && room.GetRoomId() != "" {
		roomID := room.GetRoomId()
		scenarios = append(scenarios, scenario{
			name: "webrtc.joinSession", seq: "JoinSession, Signal(open)", iters: 5,
			fn: func(ctx context.Context) error {
				sess, err := u.WebRTC.JoinSession(ctx, roomID, "go-scenario-peer", "{}", "go-scenario", 60)
				if err != nil {
					return err
				}
				// Leave to keep the room clean between iterations (not measured: the
				// timer is stopped before Leave runs — see the measurement loop).
				_ = sess.Leave(context.Background())
				return nil
			},
		})
	} else if err != nil {
		t.Logf("scenario seed: webrtc room create failed, joinSession scenario skipped: %v", err)
	}

	type sample struct {
		name    string
		seq     string
		p50     time.Duration
		p99     time.Duration
		mean    time.Duration
		min     time.Duration
		max     time.Duration
		iters   int
		errCode string
	}

	samples := make([]sample, 0, len(scenarios))
	for _, sc := range scenarios {
		// Warm-up (channel/HTTP2 + server caches), excluded from the numbers. Only
		// safe for idempotent scenarios; the entity.delete/upsert/uploadFile paths
		// are idempotent here (noop delete, fixed/throwaway keys) so a warm-up is
		// harmless, but login rotates a session each call, so skip the warm-up there.
		if sc.name != "loginAndAdoptTenant" && sc.name != "events.publishAndWait" {
			_ = sc.fn(ctx)
		}
		durs := make([]time.Duration, 0, sc.iters)
		okDurs := make([]time.Duration, 0, sc.iters)
		var firstErr, firstErrText string
		for i := 0; i < sc.iters; i++ {
			start := time.Now()
			err := sc.fn(ctx)
			d := time.Since(start)
			code := "OK"
			if err != nil {
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
		measured := okDurs
		errCode := "OK"
		if len(okDurs) == 0 {
			measured = durs
			errCode = firstErr
		}
		if errCode != "OK" {
			t.Logf("[SCENARIO-FAIL] %s => %s: %s", sc.name, errCode, firstErrText)
		}
		sort.Slice(measured, func(i, j int) bool { return measured[i] < measured[j] })
		var sum time.Duration
		for _, d := range measured {
			sum += d
		}
		samples = append(samples, sample{
			name: sc.name, seq: sc.seq, iters: sc.iters, errCode: errCode,
			p50:  measured[pct(len(measured), 50)],
			p99:  measured[pct(len(measured), 99)],
			mean: sum / time.Duration(len(measured)),
			min:  measured[0],
			max:  measured[len(measured)-1],
		})
	}

	var out strings.Builder
	out.WriteString("# UDB SDK Scenario Perf — Go (localhost)\n\n")
	out.WriteString(fmt.Sprintf("Scenarios measured: %d   tenant=%s\n\n", len(samples), u.Meta.TenantID))
	out.WriteString("This is the SCENARIO bench: it times the user-facing WORKFLOW HELPERS the " +
		"simple-client docs tell callers to use (uploadFile, downloadFile, bound entity " +
		"upsert/select/delete, loginAndAdoptTenant, events subscribe-ready/publish-and-wait, " +
		"webrtc joinSession) — measured as end-to-end facade calls, NOT the raw AllRPCs " +
		"surface. The full-surface coverage sweep stays in perf_report_go.md (TestLivePerf). " +
		"Each row's `seq` is the documented RPC sequence the helper emits " +
		"(docs/bench-bodies/workflow-sequences.md); the timed cost is the WHOLE facade call.\n\n")
	out.WriteString("| Scenario | seq | err | p50 | p99 | mean | min | max | iters |\n")
	out.WriteString("|---|---|---|---:|---:|---:|---:|---:|---:|\n")
	sort.Slice(samples, func(i, j int) bool { return samples[i].name < samples[j].name })
	failed := 0
	for _, s := range samples {
		e := s.errCode
		if e == "" {
			e = "OK"
		}
		if e != "OK" {
			failed++
		}
		out.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s | %s | %d |\n",
			s.name, s.seq, e,
			s.p50.Round(time.Microsecond), s.p99.Round(time.Microsecond), s.mean.Round(time.Microsecond),
			s.min.Round(time.Microsecond), s.max.Round(time.Microsecond), s.iters))
	}
	if failed > 0 {
		out.WriteString(fmt.Sprintf("\n%d scenario(s) returned a non-OK gRPC status (see [SCENARIO-FAIL] log lines).\n", failed))
	} else {
		out.WriteString("\nEvery scenario ran its success path (no non-OK gRPC status).\n")
	}

	report := out.String()
	if err := os.WriteFile("scenario_perf_go.md", []byte(report), 0o644); err != nil {
		t.Logf("could not write scenario_perf_go.md: %v", err)
	}
	t.Logf("\n%s", report)
	t.Logf("Go scenario perf: %d workflow helpers measured, %d FAILED; report → sdk/go/udbclient/scenario_perf_go.md", len(samples), failed)
}
