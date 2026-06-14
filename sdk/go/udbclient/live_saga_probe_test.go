package udbclient

// Diagnostic probe (luna): empirically test whether the saga/admin-audit
// DataBroker RPCs actually return UNAVAILABLE against a live, properly-provisioned
// broker, or whether they return OK/data. Gated on UDB_LIVE_SDK_TESTS=1.
//
//   go test -run TestLunaSagaProbe -v ./udbclient
//
// It logs the per-RPC gRPC status code + latency so we can confirm/refute the
// "saga RPCs fail with UNAVAILABLE" claim from the CI bench.

import (
	"context"
	"os"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestLunaSagaProbe(t *testing.T) {
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" {
		t.Skip("set UDB_LIVE_SDK_TESTS=1")
	}
	target := requiredLiveEnv(t, "UDB_GRPC_TARGET")
	authTarget := os.Getenv("UDB_AUTH_GRPC_TARGET")
	if authTarget == "" {
		authTarget = target
	}
	tenant := liveEnv("UDB_LIVE_TENANT", "sdk-live")
	project := liveEnv("UDB_LIVE_PROJECT", "default")
	meta := Metadata{
		TenantID: tenant, ProjectID: project, Purpose: "luna.saga.probe",
		CorrelationID: "luna-saga-probe", ServiceIdentity: "luna.probe",
		ClientCatalogVersion: ProtocolVersion,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
		TenantHint: tenant, ProjectHint: project, DeviceName: "luna-saga-probe",
	})
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}
	auth := NewAuthClient(authConn, meta)
	if who, err := auth.AuthenticateBearer(ctx, login.GetAccessToken()); err == nil {
		if pt := who.GetPrincipal().GetTenantId(); pt != "" {
			tenant = pt
			meta.TenantID = tenant
		}
	}
	authz := "Bearer " + login.GetAccessToken()
	brokerGen := NewGenerated(brokerConn, liveGeneratedOptions(meta, authz))
	broker := servicesv1.NewDataBrokerClient(brokerConn)
	callCtx := brokerGen.outgoingContext(ctx)
	rc := liveRequestContext(tenant, project, "luna.saga.probe")

	t.Logf("LUNA-PROBE login OK; canonical tenant=%s", tenant)

	probe := func(name string, fn func() error) {
		start := time.Now()
		err := fn()
		d := time.Since(start)
		code := "OK"
		if err != nil {
			code = status.Code(err).String()
		}
		t.Logf("LUNA-PROBE %-22s %8.2fms  status=%-16s err=%v",
			name, float64(d.Microseconds())/1000.0, code, err)
	}

	// Read-only saga/audit RPCs — the ones the CI bench flags at ~700ms.
	probe("ListSagas", func() error {
		_, e := broker.ListSagas(callCtx, &entityv1.SagaListRequest{Context: rc, Limit: 10})
		return e
	})
	probe("ListAdminAuditLogs", func() error {
		_, e := broker.ListAdminAuditLogs(callCtx, &entityv1.AdminAuditLogRequest{Context: rc, Limit: 10})
		return e
	})
	probe("VerifyAdminAuditLog", func() error {
		_, e := broker.VerifyAdminAuditLog(callCtx, &entityv1.AdminAuditVerifyRequest{Context: rc, Limit: 10})
		return e
	})
	probe("GetSaga(empty-id)", func() error {
		_, e := broker.GetSaga(callCtx, &entityv1.SagaRequest{Context: rc})
		return e
	})
}
