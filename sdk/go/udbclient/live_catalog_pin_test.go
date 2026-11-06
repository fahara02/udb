package udbclient

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	workflowentityv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/workflow/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
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
		if _, err := sess.Data.Select(ctx, read); err != nil {
			t.Fatalf("an unpinned read: %v", err)
		}
	}
	pinned := WithMetadata(ctx, Metadata{ClientCatalogVersion: "999.0.0"})
	for _, call := range []context.Context{pinned, sess.DataContext(pinned), WithMetadata(sess.DataContext(ctx), Metadata{ClientCatalogVersion: "999.0.0"})} {
		for i := 0; i < 3; i++ {
			_, err := sess.Data.Select(call, read)
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("a pin the active catalog cannot serve: code=%s %v", status.Code(err), err)
			}
		}
	}
}

// A JSON column reads and writes as JSON text, through the codec and a real broker.
func TestLiveAJSONStringColumnReadsAndWritesAsJSONText(t *testing.T) {
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
		Purpose:    "go.live.json-string",
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer sess.Close()
	const message = "udb.core.workflow.entity.v1.WorkflowInstance"
	for _, payload := range []string{`"My"`, `"5"`, `""`, `5`, `[1]`, `{"a":1}`} {
		id := svcAcctUUIDv4(t)
		row := &workflowentityv1.WorkflowInstance{
			WorkflowId:   id,
			TenantId:     sess.CanonicalTenantID,
			WorkflowType: "go.live.json-string",
			TotalSteps:   1,
			Payload:      payload,
		}
		for i := 0; i < 3; i++ {
			rec, err := EncodeRecord(row, WriteInsert)
			if err != nil {
				t.Fatalf("encode %s: %v", payload, err)
			}
			body, err := json.Marshal(rec)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sess.Data.Upsert(ctx, &entityv1.UpsertRequest{MessageType: message, RecordJson: body}); err != nil {
				t.Fatalf("upsert %s: %v", payload, err)
			}
			filter, err := structpb.NewStruct(map[string]any{"workflow_id": id})
			if err != nil {
				t.Fatal(err)
			}
			found, err := sess.Data.Select(ctx, &entityv1.SelectRequest{MessageType: message, Filter: filter, Limit: 1})
			if err != nil || len(found.GetRecordsJson()) != 1 {
				t.Fatalf("read %s: %v", payload, err)
			}
			var stored Record
			if err := json.Unmarshal(found.GetRecordsJson()[0], &stored); err != nil {
				t.Fatal(err)
			}
			var back workflowentityv1.WorkflowInstance
			if err := DecodeRecord(stored, &back); err != nil {
				t.Fatalf("decode %s: %v", payload, err)
			}
			if back.GetPayload() != payload {
				t.Fatalf("%s read back as %q", payload, back.GetPayload())
			}
			row.Payload = back.GetPayload()
		}
	}
}
