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
func TestLiveBrokerMeetsTheTableContract(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sess, err := udbclient.ConnectEnterprise(ctx, udbclient.EnterpriseConfig{
		Target:     target,
		AuthTarget: authTarget,
		Username:   required("UDB_LIVE_USERNAME"),
		Password:   required("UDB_LIVE_PASSWORD"),
		TenantCode: os.Getenv("UDB_LIVE_TENANT"),
		ProjectID:  os.Getenv("UDB_LIVE_PROJECT"),
		Purpose:    "go.live.table-conformance",
		Deadline:   30 * time.Second,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer sess.Close()
	conformance.RunTable(t, func(testing.TB, string) *udbclient.Udb { return sess.Udb }, conformance.SingleTenant())
}
