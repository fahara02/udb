package udbtest_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/fahara02/udb/sdk/go/udbclient"
	"github.com/fahara02/udb/sdk/go/udbtest/conformance"
)

// The live broker meets the same typed-store contract as the fake. Runs when
// UDB_LIVE_SDK_TESTS=1 (the CI live lane), as the live test login.
func connectLive(t *testing.T, ctx context.Context) *udbclient.EnterpriseSession {
	t.Helper()
	return connectLiveIdentity(t, ctx, false)
}

func connectLiveIdentity(t *testing.T, ctx context.Context, peer bool) *udbclient.EnterpriseSession {
	t.Helper()
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" {
		t.Skip("requires a live UDB broker (UDB_LIVE_SDK_TESTS=1)")
	}
	required := func(name string) string {
		value := os.Getenv(name)
		if value == "" {
			t.Fatalf("%s is required when UDB_LIVE_SDK_TESTS=1", name)
		}
		return value
	}
	target := required("UDB_GRPC_TARGET")
	authTarget := os.Getenv("UDB_AUTH_GRPC_TARGET")
	if authTarget == "" {
		authTarget = target
	}
	username, password, tenant, project := required("UDB_LIVE_USERNAME"), required("UDB_LIVE_PASSWORD"), os.Getenv("UDB_LIVE_TENANT"), os.Getenv("UDB_LIVE_PROJECT")
	if peer {
		username, password, tenant = required("UDB_LIVE_PEER_USERNAME"), required("UDB_LIVE_PEER_PASSWORD"), required("UDB_LIVE_PEER_TENANT")
	}
	sess, err := udbclient.ConnectEnterprise(ctx, udbclient.EnterpriseConfig{
		Target:     target,
		AuthTarget: authTarget,
		Username:   username,
		Password:   password,
		TenantCode: tenant,
		ProjectID:  project,
		Purpose:    "go.live.table-conformance",
		Deadline:   30 * time.Second,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return sess
}

func TestLiveBrokerMeetsTheTableContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sess := connectLive(t, ctx)
	defer sess.Close()
	peer := connectLiveIdentity(t, ctx, true)
	defer peer.Close()
	if sess.CanonicalTenantID == "" || peer.CanonicalTenantID == "" || sess.CanonicalTenantID == peer.CanonicalTenantID {
		t.Fatal("live conformance requires two distinct verified tenant IDs")
	}
	connect := func(t testing.TB, tenant string) *udbclient.Udb {
		t.Helper()
		if tenant == peer.CanonicalTenantID {
			return peer.Udb
		}
		return sess.Udb
	}
	conformance.RunTable(t, connect, conformance.Tenants(sess.CanonicalTenantID, peer.CanonicalTenantID))
}
