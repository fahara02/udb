package udbclient

// Gate item 26 (enterprise native-connection parity): when AuthTarget != Target an
// app must be able to reach a native service that has NO typed facade on the broker
// (Data) target — e.g. LockService — over the SESSION-OWNED authenticated
// control-plane channel, without redialing or duplicating the TLS/bearer lifecycle.
//
// This exercises the public enterprise surface end to end on TWO real ephemeral
// gRPC listeners (separate data + auth targets): ConnectEnterprise dials both,
// LockService() binds to NativeConn (the AuthTarget channel), and Acquire/Renew/
// Release flow through NativeContext so each served call carries the canonical
// adopted tenant + the live (refreshed) bearer. It reuses the package's existing
// fake harness (v232Authn / v232Broker / v232MDCapture, defined in
// enterprise_metadata_served_test.go).

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	lockv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/lock/services/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
)

// fakeLockServer records the incoming metadata of the last served lock RPC and
// returns deterministic grants so the test can assert the acquire→renew→release
// flow AND that the served call carried the canonical tenant + expected bearer.
type fakeLockServer struct {
	lockv1.UnimplementedLockServiceServer
	cap *v232MDCapture

	mu       sync.Mutex
	acquires int
	renews   int
	releases int
}

func (f *fakeLockServer) AcquireLock(ctx context.Context, req *lockv1.AcquireLockRequest) (*lockv1.AcquireLockResponse, error) {
	f.cap.set(ctx)
	f.mu.Lock()
	f.acquires++
	f.mu.Unlock()
	return &lockv1.AcquireLockResponse{Acquired: true, FencingToken: 7, LockName: req.GetLockName()}, nil
}

func (f *fakeLockServer) RenewLock(ctx context.Context, req *lockv1.RenewLockRequest) (*lockv1.RenewLockResponse, error) {
	f.cap.set(ctx)
	f.mu.Lock()
	f.renews++
	f.mu.Unlock()
	return &lockv1.RenewLockResponse{Renewed: true, FencingToken: req.GetFencingToken()}, nil
}

func (f *fakeLockServer) ReleaseLock(ctx context.Context, req *lockv1.ReleaseLockRequest) (*lockv1.ReleaseLockResponse, error) {
	f.cap.set(ctx)
	f.mu.Lock()
	f.releases++
	f.mu.Unlock()
	return &lockv1.ReleaseLockResponse{Released: true}, nil
}

// TestEnterpriseSession_LockServiceSplitTargetLifecycle drives acquire→renew→
// release of a distributed lock through the public enterprise surface with
// AuthTarget != Target, asserting the session-owned control-plane channel carries
// the canonical adopted tenant and the refreshed bearer at each step.
func TestEnterpriseSession_LockServiceSplitTargetLifecycle(t *testing.T) {
	authCap, lockCap := &v232MDCapture{}, &v232MDCapture{}

	dataLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen data: %v", err)
	}
	authLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen auth: %v", err)
	}

	dataSrv, authSrv := grpc.NewServer(), grpc.NewServer()
	// Data plane: only the DataBroker (ConnectEnterprise dials it but the lock
	// flow never touches it — LockService lives on the SEPARATE control plane).
	servicesv1.RegisterDataBrokerServer(dataSrv, &v232Broker{cap: &v232MDCapture{}})
	// Control plane (AuthTarget): auth for login/adopt + the LockService escape
	// hatch that has no typed facade on the broker target.
	authFake := &v232Authn{cap: authCap}
	authnv1.RegisterAuthnServiceServer(authSrv, authFake)
	lockFake := &fakeLockServer{cap: lockCap}
	lockv1.RegisterLockServiceServer(authSrv, lockFake)
	go func() { _ = dataSrv.Serve(dataLis) }()
	go func() { _ = authSrv.Serve(authLis) }()
	defer dataSrv.Stop()
	defer authSrv.Stop()

	ctx := context.Background()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{
		Target:     dataLis.Addr().String(),
		AuthTarget: authLis.Addr().String(), // split target: distinct control plane
		Username:   "u",
		Password:   "p",
		TenantCode: v232HintTenant, // pre-login hint, replaced by the canonical UUID
		ProjectID:  v232HintProject,
		Purpose:    "gate26-lock",
	})
	if err != nil {
		t.Fatalf("ConnectEnterprise: %v", err)
	}
	defer sess.Close()

	// The escape hatch is the SESSION-OWNED control-plane channel — never nil,
	// caller never dials or closes it.
	if sess.NativeConn() == nil {
		t.Fatal("NativeConn must return the session-owned control-plane channel")
	}

	lock := sess.LockService()

	// ── Acquire ───────────────────────────────────────────────────────────────
	acq, err := lock.Acquire(ctx, &lockv1.AcquireLockRequest{LockName: "job-x", OwnerId: "worker-1", LeaseTtlSeconds: 30})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !acq.GetAcquired() || acq.GetFencingToken() != 7 {
		t.Fatalf("Acquire = (acquired=%v, token=%d), want (true, 7)", acq.GetAcquired(), acq.GetFencingToken())
	}
	// The served call must carry the canonical adopted tenant + the login bearer,
	// each exactly once — proving adopted identity + bearer are preserved on the
	// session-owned channel (not the pre-login hint).
	v232AssertCanonicalSingletons(t, "LockService.Acquire", lockCap.get(), "Bearer token1")

	// ── Renew (present the fencing token) ───────────────────────────────────────
	ren, err := lock.Renew(ctx, &lockv1.RenewLockRequest{
		LockName: "job-x", OwnerId: "worker-1", FencingToken: acq.GetFencingToken(), LeaseTtlSeconds: 30,
	})
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if !ren.GetRenewed() || ren.GetFencingToken() != 7 {
		t.Fatalf("Renew = (renewed=%v, token=%d), want (true, 7)", ren.GetRenewed(), ren.GetFencingToken())
	}
	v232AssertCanonicalSingletons(t, "LockService.Renew", lockCap.get(), "Bearer token1")

	// ── Force a bearer refresh, then Release ────────────────────────────────────
	// Expire the stored token so the next NativeContext resolves a fresh bearer
	// (token2). The lock facade routes through NativeContext, so the refreshed
	// bearer must reach the session-owned channel — proving refreshed-bearer
	// handling is preserved on the escape hatch, not just the typed data plane.
	_ = sess.tm.store.Save(ctx, Token{
		AccessToken:  "token1",
		RefreshToken: "refresh1",
		SessionID:    "sess1",
		ExpiresAt:    time.Now().Add(-time.Hour),
	})
	rel, err := lock.Release(ctx, &lockv1.ReleaseLockRequest{LockName: "job-x", OwnerId: "worker-1", FencingToken: acq.GetFencingToken()})
	if err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !rel.GetReleased() {
		t.Fatal("Release = false, want true")
	}
	v232AssertCanonicalSingletons(t, "LockService.Release(after refresh)", lockCap.get(), "Bearer token2")

	// Every stage was actually served on the control plane.
	lockFake.mu.Lock()
	a, r, rel2 := lockFake.acquires, lockFake.renews, lockFake.releases
	lockFake.mu.Unlock()
	if a != 1 || r != 1 || rel2 != 1 {
		t.Fatalf("served lock RPCs = (acquire=%d, renew=%d, release=%d), want (1, 1, 1)", a, r, rel2)
	}
	if hits := func() int { authFake.mu.Lock(); defer authFake.mu.Unlock(); return authFake.refreshHits }(); hits == 0 {
		t.Error("expected at least one RefreshToken RPC to be served for the release-time bearer refresh")
	}
}
