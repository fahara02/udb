package udbclient

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	cachev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/cache/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Separate diagnostic artifact: canonical benchmark bodies and headline means
// stay unchanged. This runs before the special short-token broker restart.
func TestLiveLatencyProfile(t *testing.T) {
	if os.Getenv("UDB_LIVE_LATENCY_PROFILE") != "1" {
		t.Skip("requires the dedicated CI latency-profile opt-in")
	}
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" {
		t.Fatal("latency profile requires an actual served live SDK fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	target := requiredLiveEnv(t, "UDB_GRPC_TARGET")
	authTarget := liveEnv("UDB_AUTH_GRPC_TARGET", target)
	brokerConn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal("could not construct the served data channel")
	}
	defer brokerConn.Close()
	authConn := brokerConn
	if authTarget != target {
		authConn, err = grpc.NewClient(authTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal("could not construct the served auth channel")
		}
		defer authConn.Close()
	}
	username, password := requiredLiveEnv(t, "UDB_LIVE_USERNAME"), requiredLiveEnv(t, "UDB_LIVE_PASSWORD")
	tenant, project := requiredLiveEnv(t, "UDB_LIVE_TENANT"), requiredLiveEnv(t, "UDB_LIVE_PROJECT")
	authn := authnv1.NewAuthnServiceClient(authConn)
	loginCtx, loginCancel := context.WithTimeout(ctx, 10*time.Second)
	login, err := authn.Login(loginCtx, &authnv1.LoginRequest{
		Username: username, Password: password, TenantHint: tenant, ProjectHint: project,
		DeviceName: "go-live-latency-profile",
	})
	loginCancel()
	if err != nil || login.GetAccessToken() == "" || login.GetSessionId() == "" {
		t.Fatalf("profile initial login failed: code=%s", status.Code(err))
	}
	meta := Metadata{TenantID: tenant, ProjectID: project, Purpose: "go.live.latency.profile", ServiceIdentity: "go.sdk.live", ClientCatalogVersion: ProtocolVersion}
	verifyCtx, verifyCancel := context.WithTimeout(ctx, 5*time.Second)
	verified, err := NewAuthClient(authConn, meta).AuthenticateBearer(verifyCtx, login.GetAccessToken())
	verifyCancel()
	if err != nil || verified.GetPrincipal().GetTenantId() == "" || verified.GetPrincipal().GetProjectId() != project || verified.GetPrincipal().GetUserId() != login.GetUserId() {
		t.Fatalf("profile initial identity verification failed: code=%s", status.Code(err))
	}
	meta.TenantID, meta.UserID = verified.GetPrincipal().GetTenantId(), verified.GetPrincipal().GetUserId()
	tenant = meta.TenantID
	gen := NewGenerated(brokerConn, liveGeneratedOptions(meta, "Bearer "+login.GetAccessToken()))
	authGen := NewGenerated(authConn, liveGeneratedOptions(meta, "Bearer "+login.GetAccessToken()))
	broker := servicesv1.NewDataBrokerClient(brokerConn)
	recordID := "latency-profile-" + uuid4()
	// Credentials and session identifiers are retained only for owned cleanup;
	// none is serialized into the profile or included in failure messages.
	var sessionsMu sync.Mutex
	sessions := []string{login.GetSessionId()}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, cleanupErr := broker.Delete(gen.outgoingContext(cleanupCtx), &entityv1.DeleteRequest{
			Context: liveRequestContext(tenant, project, meta.Purpose), MessageType: liveMessageType,
			Filter: liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": tenant, "project_id": project}),
		})
		cleanupCancel()
		if cleanupErr != nil {
			t.Errorf("profile record cleanup failed: code=%s", status.Code(cleanupErr))
		}
		// Revoke the credential carrying cleanup authority only after all the
		// other owned sessions and the record have been cleaned up.
		for _, sessionID := range append(append([]string{}, sessions[1:]...), sessions[0]) {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, cleanupErr := authn.Logout(authGen.outgoingContext(cleanupCtx), &authnv1.LogoutRequest{SessionId: sessionID, RevokeReason: "latency-profile cleanup"})
			cleanupCancel()
			if cleanupErr != nil {
				t.Errorf("profile session cleanup failed: code=%s", status.Code(cleanupErr))
			}
		}
	}()
	writeCtx, writeCancel := context.WithTimeout(ctx, 5*time.Second)
	written, err := broker.Upsert(gen.outgoingContext(writeCtx), &entityv1.UpsertRequest{
		Context: liveRequestContext(tenant, project, meta.Purpose), MessageType: liveMessageType,
		RecordJson:     liveRecordJSON(t, recordID, tenant, project, recordID, "latency-profile", 1),
		ConflictFields: []string{"record_id"}, ReturnRecord: true,
	})
	writeCancel()
	if err != nil || written.GetAffectedRows() != 1 {
		t.Fatalf("profile record fixture failed: code=%s", status.Code(err))
	}
	reads := []liveLatencyRead{
		{"DataBroker/Select", func(callCtx context.Context) error {
			rows, callErr := broker.Select(gen.outgoingContext(callCtx), &entityv1.SelectRequest{
				Context: liveRequestContext(tenant, project, meta.Purpose), MessageType: liveMessageType, Limit: 1,
				Filter: liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": tenant, "project_id": project}),
			})
			if callErr == nil && len(rows.GetRecordsJson()) != 1 {
				return errors.New("profile_select_record_missing")
			}
			return callErr
		}},
		{"DataBroker/GetCapabilities", func(callCtx context.Context) error {
			caps, callErr := broker.GetCapabilities(gen.outgoingContext(callCtx), &entityv1.CapabilitiesRequest{Context: liveRequestContext(tenant, project, meta.Purpose)})
			if callErr == nil && len(caps.GetEnabledBackends()) == 0 {
				return errors.New("profile_capabilities_empty")
			}
			return callErr
		}},
	}
	var active atomic.Int64
	for i := 0; i < 5; i++ {
		for _, read := range reads {
			if attempt := liveLatencyCall(ctx, read, &active); !attempt.OK {
				t.Fatalf("profile warmup failed: rpc=%s code=%s", read.Name, attempt.Code)
			}
		}
	}
	clockTicks := liveLatencyClockTicks(ctx)
	report := liveLatencyReport{
		SchemaVersion: 1, GeneratedAt: time.Now().UTC(), LogicalCPUs: runtime.NumCPU(), ClockTicksPerSecond: clockTicks,
		SamplesPerRead: 25, LoginConcurrency: 4, LoginCallsPerWorker: 2,
		Omissions: []string{"AuthzService/Authorize: no independently seeded allowed PDP decision in this narrow fixture; policy mutations and denial timing are excluded"},
		Notes: []string{
			"Every read uses the same owned record/identity and persistent served channels",
			"Warmup is excluded; every measured attempt is retained",
			"The idle and login-burst phases have no hard latency threshold; the page-load phase fails over its p95 budget (UDB_LIVE_PAGE_P95_BUDGET_MS, default 250) and, with UDB_LIVE_DB_DSN set, over its database transactions per call (UDB_LIVE_PAGE_XACT_BUDGET, default 2)",
			"Login overlap counts are client in-flight calls, not measured server KDF concurrency",
			"Core PostgreSQL histograms exclude authn durable-store SQL and SQLx protocol pings; complete query counts require database diagnostics",
			"Optional cgroup counters describe the visible mount-root group; absent counters remain absent rather than implying zero throttling",
		},
	}
	idle := liveLatencyPhase{Name: "idle", Before: liveLatencySnapshot(ctx)}
	idle.Attempts = liveLatencyReads(ctx, reads, &active)
	idle.After = liveLatencySnapshot(ctx)
	idle.Statistics = liveLatencyStatistics(idle.Attempts)
	report.Phases = append(report.Phases, idle)

	burst := liveLatencyPhase{Name: "login_burst", Before: liveLatencySnapshot(ctx)}
	start := make(chan struct{})
	var ready, launched, workers sync.WaitGroup
	ready.Add(report.LoginConcurrency)
	launched.Add(report.LoginConcurrency)
	workers.Add(report.LoginConcurrency)
	var loginAttemptsMu sync.Mutex
	for worker := 0; worker < report.LoginConcurrency; worker++ {
		go func() {
			defer workers.Done()
			ready.Done()
			<-start
			for call := 0; call < report.LoginCallsPerWorker; call++ {
				attempt := liveLatencyCall(ctx, liveLatencyRead{"AuthnService/Login", func(callCtx context.Context) error {
					active.Add(1)
					defer active.Add(-1)
					if call == 0 {
						launched.Done()
					}
					issued, callErr := authn.Login(callCtx, &authnv1.LoginRequest{
						Username: username, Password: password, TenantHint: tenant, ProjectHint: project,
						DeviceName: "go-live-latency-burst",
					})
					if callErr == nil && (issued.GetAccessToken() == "" || issued.GetRefreshToken() == "" || issued.GetSessionId() == "" || issued.GetUserId() != meta.UserID) {
						return errors.New("profile_login_identity_or_credentials_missing")
					}
					if callErr == nil {
						sessionsMu.Lock()
						sessions = append(sessions, issued.GetSessionId())
						sessionsMu.Unlock()
					}
					return callErr
				}}, &active)
				loginAttemptsMu.Lock()
				burst.LoginAttempts = append(burst.LoginAttempts, attempt)
				loginAttemptsMu.Unlock()
			}
		}()
	}
	ready.Wait()
	close(start)
	launched.Wait()
	burst.Attempts = liveLatencyReads(ctx, reads, &active)
	// Always join the bounded login workers before snapshots, cleanup or failure.
	workers.Wait()
	burst.After = liveLatencySnapshot(ctx)
	burst.Statistics = liveLatencyStatistics(burst.Attempts)
	burst.LoginStatistics = liveLatencyStatistics(burst.LoginAttempts)
	report.Phases = append(report.Phases, burst)

	// Page load: what one signed-in user's page does to the broker. A gateway
	// authenticates and counts the request, then the route fans out data and
	// cache reads, all on the same session at once. This phase has a hard
	// budget: calls of one session must run in parallel, never queue behind
	// each other (a per-session row lock or a per-tenant rate limit did).
	// Database work per call: transactions counted over the page load, minus
	// what the idle broker runs in the same time (its background loops).
	// PostgreSQL publishes a backend's counters up to ~10 s late, so each
	// reading waits for the previous activity to be published first.
	xactDSN := os.Getenv("UDB_LIVE_DB_DSN")
	var xactIdlePerSec float64
	var xactBefore int64
	xactReady := false
	if xactDSN != "" {
		time.Sleep(liveLatencyXactSettle)
		if idleStart, ok := liveLatencyXacts(xactDSN); ok {
			time.Sleep(liveLatencyXactSettle)
			if idleEnd, ok := liveLatencyXacts(xactDSN); ok {
				xactIdlePerSec = float64(idleEnd-idleStart) / liveLatencyXactSettle.Seconds()
				xactBefore, xactReady = idleEnd, true
			}
		}
		if !xactReady {
			t.Errorf("page load: could not read database transaction counts with psql (UDB_LIVE_DB_DSN is set)")
		}
	}
	pageStarted := time.Now()
	page := liveLatencyPhase{Name: "page_load", Before: liveLatencySnapshot(ctx)}
	pageCalls := append([]liveLatencyRead{}, reads[0],
		liveLatencyRead{"AuthnService/Authenticate", func(callCtx context.Context) error {
			who, callErr := NewAuthClient(authConn, meta).AuthenticateBearer(callCtx, login.GetAccessToken())
			if callErr == nil && who.GetPrincipal().GetUserId() != meta.UserID {
				return errors.New("profile_authenticate_identity_changed")
			}
			return callErr
		}},
		liveLatencyRead{"CacheService/Increment", func(callCtx context.Context) error {
			return authGen.InvokeUnary(callCtx, "/udb.core.cache.services.v1.CacheService/Increment",
				&cachev1.IncrementRequest{TenantId: tenant, Namespace: "latency-profile", Key: recordID, Delta: 1},
				&cachev1.IncrementResponse{})
		}},
		liveLatencyRead{"CacheService/Get", func(callCtx context.Context) error {
			return authGen.InvokeUnary(callCtx, "/udb.core.cache.services.v1.CacheService/Get",
				&cachev1.GetRequest{TenantId: tenant, Namespace: "latency-profile", Key: recordID},
				&cachev1.GetResponse{})
		}},
	)
	const pageRounds, pageCallsPerRound = 5, 50
	for round := 0; round < pageRounds; round++ {
		var wg sync.WaitGroup
		var mu sync.Mutex
		wg.Add(pageCallsPerRound)
		for i := 0; i < pageCallsPerRound; i++ {
			read := pageCalls[i%len(pageCalls)]
			go func() {
				defer wg.Done()
				attempt := liveLatencyCall(ctx, read, &active)
				mu.Lock()
				page.Attempts = append(page.Attempts, attempt)
				mu.Unlock()
			}()
		}
		wg.Wait()
	}
	page.After = liveLatencySnapshot(ctx)
	page.Statistics = liveLatencyStatistics(page.Attempts)
	report.Phases = append(report.Phases, page)
	if xactReady {
		time.Sleep(liveLatencyXactSettle)
		if xactAfter, ok := liveLatencyXacts(xactDSN); ok {
			background := xactIdlePerSec * time.Since(pageStarted).Seconds()
			perCall := math.Max(0, float64(xactAfter-xactBefore)-background) / float64(len(page.Attempts))
			report.PageTransactionsPerCall = &perCall
			xactBudget := 2.0
			if raw := os.Getenv("UDB_LIVE_PAGE_XACT_BUDGET"); raw != "" {
				if parsed, parseErr := strconv.ParseFloat(raw, 64); parseErr == nil && parsed > 0 {
					xactBudget = parsed
				}
			}
			t.Logf("page load: %.2f database transactions per call (budget %.1f, idle background %.1f/s)", perCall, xactBudget, xactIdlePerSec)
			if perCall > xactBudget {
				t.Errorf("page load: %.2f database transactions per call is over the budget of %.1f; a call gained a round trip", perCall, xactBudget)
			}
		} else {
			t.Errorf("page load: could not read database transaction counts after the page load")
		}
	}
	pageBudgetMS := 250.0
	if raw := os.Getenv("UDB_LIVE_PAGE_P95_BUDGET_MS"); raw != "" {
		if parsed, parseErr := strconv.ParseFloat(raw, 64); parseErr == nil && parsed > 0 {
			pageBudgetMS = parsed
		}
	}
	for name, stat := range page.Statistics {
		if stat.P95MS > pageBudgetMS {
			t.Errorf("page load: %s p95 %.1f ms is over the %.0f ms budget for %d parallel calls of one session",
				name, stat.P95MS, pageBudgetMS, pageCallsPerRound)
		}
	}

	output := os.Getenv("UDB_LIVE_LATENCY_REPORT")
	if output == "" {
		output = filepath.Join("..", "..", "..", "bench-output", "latency-profile.json")
	}
	if err := os.MkdirAll(filepath.Dir(output), 0700); err != nil {
		t.Fatal("could not create private latency artifact directory")
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil || os.WriteFile(output, append(encoded, '\n'), 0600) != nil {
		t.Fatal("could not write private latency artifact")
	}
	for _, phase := range report.Phases {
		for _, attempt := range append(append([]liveLatencyAttempt{}, phase.Attempts...), phase.LoginAttempts...) {
			if !attempt.OK {
				t.Errorf("profile call failed: phase=%s rpc=%s code=%s", phase.Name, attempt.RPC, attempt.Code)
			}
		}
	}
	t.Logf("latency profile recorded three phases, %d reads per RPC per phase, %d bounded login attempts", report.SamplesPerRead, len(burst.LoginAttempts))
}

// How long PostgreSQL may take to publish a backend's transaction counters.
const liveLatencyXactSettle = 11 * time.Second

// Transactions committed or rolled back so far in the broker's database,
// summed over every server in the comma-separated DSN list (primary and
// read replicas).
func liveLatencyXacts(dsns string) (int64, bool) {
	var total int64
	for _, dsn := range strings.Split(dsns, ",") {
		dsn = strings.TrimSpace(dsn)
		if dsn == "" {
			continue
		}
		out, err := exec.Command("psql", dsn, "-Atqc",
			"SELECT xact_commit + xact_rollback FROM pg_stat_database WHERE datname = current_database()").Output()
		if err != nil {
			return 0, false
		}
		value, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
		if err != nil {
			return 0, false
		}
		total += value
	}
	return total, true
}

type liveLatencyRead struct {
	Name string
	Call func(context.Context) error
}

type liveLatencyAttempt struct {
	RPC                  string    `json:"rpc"`
	StartedAt            time.Time `json:"started_at"`
	DurationMS           float64   `json:"duration_ms"`
	OK                   bool      `json:"ok"`
	Code                 string    `json:"code"`
	LoginInFlightAtStart int64     `json:"login_in_flight_at_start"`
	LoginInFlightAtEnd   int64     `json:"login_in_flight_at_end"`
}

type liveLatencyStats struct {
	Attempts                 int     `json:"attempts"`
	Successful               int     `json:"successful"`
	Failed                   int     `json:"failed"`
	OverlappingLoginAttempts int     `json:"overlapping_login_attempts"`
	MeanMS                   float64 `json:"mean_ms"`
	P50MS                    float64 `json:"p50_ms"`
	P95MS                    float64 `json:"p95_ms"`
	P99MS                    float64 `json:"p99_ms"`
}

type liveLatencyPhase struct {
	Name            string                      `json:"name"`
	Before          liveLatencyHostSnapshot     `json:"before"`
	After           liveLatencyHostSnapshot     `json:"after"`
	Attempts        []liveLatencyAttempt        `json:"attempts"`
	LoginAttempts   []liveLatencyAttempt        `json:"login_attempts,omitempty"`
	Statistics      map[string]liveLatencyStats `json:"statistics_all_attempts"`
	LoginStatistics map[string]liveLatencyStats `json:"login_statistics_all_attempts,omitempty"`
}

type liveLatencyReport struct {
	SchemaVersion       int                `json:"schema_version"`
	GeneratedAt         time.Time          `json:"generated_at"`
	LogicalCPUs         int                `json:"logical_cpus"`
	ClockTicksPerSecond int64              `json:"clock_ticks_per_second,omitempty"`
	SamplesPerRead      int                `json:"samples_per_read"`
	LoginConcurrency    int                `json:"login_concurrency"`
	LoginCallsPerWorker int                `json:"login_calls_per_worker"`
	Phases              []liveLatencyPhase `json:"phases"`
	// Database transactions the broker ran per page-load call, net of what the
	// idle broker runs in the same time. Absent when UDB_LIVE_DB_DSN is unset.
	PageTransactionsPerCall *float64 `json:"page_transactions_per_call,omitempty"`
	Omissions               []string `json:"omissions"`
	Notes                   []string `json:"notes"`
}

func liveLatencyCall(ctx context.Context, read liveLatencyRead, active *atomic.Int64) liveLatencyAttempt {
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	started := time.Now()
	attempt := liveLatencyAttempt{RPC: read.Name, StartedAt: started.UTC(), LoginInFlightAtStart: active.Load()}
	err := read.Call(callCtx)
	attempt.DurationMS, attempt.OK = float64(time.Since(started))/float64(time.Millisecond), err == nil
	attempt.Code, attempt.LoginInFlightAtEnd = status.Code(err).String(), active.Load()
	if err != nil && attempt.Code == "Unknown" {
		attempt.Code = "invalid_response_or_unknown_error"
	}
	return attempt
}

func liveLatencyReads(ctx context.Context, reads []liveLatencyRead, active *atomic.Int64) []liveLatencyAttempt {
	attempts := make([]liveLatencyAttempt, 0, 25*len(reads))
	for sample := 0; sample < 25; sample++ {
		for _, read := range reads {
			attempts = append(attempts, liveLatencyCall(ctx, read, active))
		}
	}
	return attempts
}

func liveLatencyStatistics(attempts []liveLatencyAttempt) map[string]liveLatencyStats {
	values := map[string][]float64{}
	stats := map[string]liveLatencyStats{}
	for _, attempt := range attempts {
		stat := stats[attempt.RPC]
		stat.Attempts++
		if attempt.OK {
			stat.Successful++
		} else {
			stat.Failed++
		}
		if attempt.LoginInFlightAtStart > 0 || attempt.LoginInFlightAtEnd > 0 {
			stat.OverlappingLoginAttempts++
		}
		stat.MeanMS += attempt.DurationMS
		stats[attempt.RPC] = stat
		values[attempt.RPC] = append(values[attempt.RPC], attempt.DurationMS)
	}
	for name, durations := range values {
		sort.Float64s(durations)
		stat := stats[name]
		stat.MeanMS /= float64(stat.Attempts)
		quantile := func(p float64) float64 { return durations[int(math.Ceil(p*float64(len(durations))))-1] }
		stat.P50MS, stat.P95MS, stat.P99MS = quantile(.50), quantile(.95), quantile(.99)
		stats[name] = stat
	}
	return stats
}

type liveLatencyHostSnapshot struct {
	CapturedAt       time.Time          `json:"captured_at"`
	CPUTotalTicks    uint64             `json:"cpu_total_ticks,omitempty"`
	CPUStealTicks    uint64             `json:"cpu_steal_ticks,omitempty"`
	CPUAllowedList   string             `json:"cpu_allowed_list,omitempty"`
	CPUQuota         string             `json:"cgroup_cpu_max,omitempty"`
	CPUThrottling    map[string]uint64  `json:"cgroup_cpu_stat,omitempty"`
	BrokerPID        int                `json:"broker_pid,omitempty"`
	BrokerStartTicks uint64             `json:"broker_start_ticks,omitempty"`
	BrokerCPUTicks   uint64             `json:"broker_cpu_ticks,omitempty"`
	BrokerThreads    uint64             `json:"broker_threads,omitempty"`
	MetricsStatus    string             `json:"metrics_status"`
	Metrics          map[string]float64 `json:"metrics,omitempty"`
}

func liveLatencySnapshot(ctx context.Context) liveLatencyHostSnapshot {
	snapshot := liveLatencyHostSnapshot{CapturedAt: time.Now().UTC(), MetricsStatus: "not_configured"}
	if raw, err := os.ReadFile("/proc/stat"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 9 || fields[0] != "cpu" {
				continue
			}
			for index := 1; index <= 8; index++ {
				value, err := strconv.ParseUint(fields[index], 10, 64)
				if err == nil {
					snapshot.CPUTotalTicks += value
					if index == 8 {
						snapshot.CPUStealTicks = value
					}
				}
			}
			break
		}
	}
	if raw, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "Cpus_allowed_list:") {
				snapshot.CPUAllowedList = strings.TrimSpace(strings.TrimPrefix(line, "Cpus_allowed_list:"))
			}
		}
	}
	if raw, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		snapshot.CPUQuota = strings.TrimSpace(string(raw))
	}
	if raw, err := os.ReadFile("/sys/fs/cgroup/cpu.stat"); err == nil {
		snapshot.CPUThrottling = map[string]uint64{}
		for _, line := range strings.Split(string(raw), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 {
				if value, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
					snapshot.CPUThrottling[fields[0]] = value
				}
			}
		}
	}
	pidText := os.Getenv("UDB_BROKER_PID")
	if pidText == "" {
		if raw, err := os.ReadFile("/tmp/udb-bench.pid"); err == nil {
			pidText = strings.TrimSpace(string(raw))
		}
	}
	if pid, err := strconv.Atoi(pidText); err == nil && pid > 1 {
		if raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat")); err == nil {
			if closing := strings.LastIndex(string(raw), ")"); closing >= 0 {
				fields := strings.Fields(string(raw)[closing+1:])
				if len(fields) > 19 {
					snapshot.BrokerPID = pid
					user, _ := strconv.ParseUint(fields[11], 10, 64)
					system, _ := strconv.ParseUint(fields[12], 10, 64)
					snapshot.BrokerCPUTicks = user + system
					snapshot.BrokerThreads, _ = strconv.ParseUint(fields[17], 10, 64)
					snapshot.BrokerStartTicks, _ = strconv.ParseUint(fields[19], 10, 64)
				}
			}
		}
	}
	metricsURL := os.Getenv("UDB_METRICS_URL")
	if metricsURL == "" {
		if address := os.Getenv("UDB_METRICS_ADDR"); address != "" {
			metricsURL = "http://" + address + "/metrics"
		}
	}
	if metricsURL == "" {
		return snapshot
	}
	metricsCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(metricsCtx, http.MethodGet, metricsURL, nil)
	if err != nil {
		snapshot.MetricsStatus = "invalid_url"
		return snapshot
	}
	response, err := (&http.Client{Timeout: 2 * time.Second}).Do(request)
	if err != nil {
		snapshot.MetricsStatus = "unavailable"
		return snapshot
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		snapshot.MetricsStatus = "non_success_response"
		return snapshot
	}
	snapshot.MetricsStatus, snapshot.Metrics = "available", map[string]float64{}
	scanner := bufio.NewScanner(io.LimitReader(response.Body, 2<<20))
	for scanner.Scan() {
		line := scanner.Text()
		if !(strings.HasPrefix(line, "udb_grpc_duration_seconds_") || strings.HasPrefix(line, "udb_grpc_requests_total") || strings.HasPrefix(line, "udb_pg_query_duration_seconds_") || strings.HasPrefix(line, "udb_channel_latency_seconds_") && strings.Contains(line, `channel="read"`)) {
			continue
		}
		separator := strings.LastIndexByte(line, ' ')
		if separator <= 0 {
			continue
		}
		if value, err := strconv.ParseFloat(line[separator+1:], 64); err == nil && !math.IsNaN(value) && !math.IsInf(value, 0) {
			snapshot.Metrics[line[:separator]] = value
		}
	}
	if scanner.Err() != nil {
		snapshot.MetricsStatus = "truncated_or_unreadable"
	}
	return snapshot
}

func liveLatencyClockTicks(ctx context.Context) int64 {
	clockCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	output, err := exec.CommandContext(clockCtx, "getconf", "CLK_TCK").Output()
	if err != nil {
		return 0
	}
	ticks, _ := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
	return ticks
}
