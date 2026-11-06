package udbclient

import (
	"context"
	"os"
	"testing"
	"time"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// No catalog pin unless the caller asks for one, and a pin is honoured when it is sent. Skipped
// unless UDB_LIVE_SDK_TESTS=1; CI's "SDK refresh-token rotation (live)" job runs it.
func TestLiveNoDefaultCatalogPinAndASentPinIsHonoured(t *testing.T) {
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" {
		t.Skip("requires live UDB broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	target := requiredLiveEnv(t, "UDB_GRPC_TARGET")
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{
		Target:     target,
		AuthTarget: liveEnv("UDB_AUTH_GRPC_TARGET", target),
		Username:   requiredLiveEnv(t, "UDB_LIVE_USERNAME"),
		Password:   requiredLiveEnv(t, "UDB_LIVE_PASSWORD"),
		TenantCode: requiredLiveEnv(t, "UDB_LIVE_TENANT"),
		ProjectID:  liveEnv("UDB_LIVE_PROJECT", "default"),
		Purpose:    "go.live.catalog-pin",
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer sess.Close()
	if sess.Meta.ClientCatalogVersion != "" {
		t.Fatalf("a default pin: %q", sess.Meta.ClientCatalogVersion)
	}
	read := &entityv1.SelectRequest{MessageType: "udb.core.workflow.entity.v1.WorkflowInstance", Limit: 1}
	for i := 0; i < 3; i++ {
		if _, err := sess.Data.Select(sess.DataContext(ctx), read); err != nil {
			t.Fatalf("an unpinned read: %v", err)
		}
	}
	pinned := WithMetadata(sess.DataContext(ctx), Metadata{ClientCatalogVersion: "999.0.0"})
	for i := 0; i < 3; i++ {
		_, err := sess.Data.Select(pinned, read)
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("a pin the active catalog cannot serve: code=%s %v", status.Code(err), err)
		}
	}
}
