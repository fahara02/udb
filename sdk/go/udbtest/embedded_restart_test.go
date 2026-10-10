package udbtest_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
)

// The CI owner runs seed before stopping the first broker process and verify
// only after a second process has opened the same persisted PostgreSQL cluster.
func TestEmbeddedRestartPreservesCommittedRow(t *testing.T) {
	if os.Getenv("UDB_EMBEDDED_RESTART_TESTS") != "1" {
		t.Skip("requires the CI-owned embedded restart fixture")
	}
	id, phase := os.Getenv("UDB_EMBEDDED_RESTART_ROW_ID"), os.Getenv("UDB_EMBEDDED_RESTART_PHASE")
	if id == "" || (phase != "seed" && phase != "verify") {
		t.Fatal("restart proof requires an owned row and explicit seed or verify phase")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	session := connectLive(t, ctx)
	defer session.Close()
	if session.CanonicalTenantID == "" {
		t.Fatal("restart proof requires a verified canonical tenant")
	}
	files := udbclient.TableOf[*storagev1.File](session.Udb)
	key := udbclient.RowKey{"file_id": id}
	name, object := "persisted-"+id+".txt", "embedded-restart/"+id
	if phase == "seed" {
		if _, err := files.Get(ctx, key); !errors.Is(err, udbclient.ErrNotFound) {
			t.Fatalf("owned seed row must start absent: %v", err)
		}
		if err := files.Upsert(ctx, &storagev1.File{
			FileId: id, Filename: name, ObjectKey: object,
			Status: storagev1.FileStatus_FILE_STATUS_PENDING,
		}); err != nil {
			t.Fatalf("commit the owned row before restart: %v", err)
		}
	}
	got, err := files.Get(ctx, key)
	if err != nil {
		t.Fatalf("read committed row in phase %s: %v", phase, err)
	}
	if got.GetFileId() != id || got.GetFilename() != name || got.GetObjectKey() != object ||
		got.GetTenantId() != session.CanonicalTenantID || got.GetStatus() != storagev1.FileStatus_FILE_STATUS_PENDING {
		t.Fatal("persisted row or verified tenant differs across broker restart")
	}
	peer := connectLiveIdentity(t, ctx, true)
	defer peer.Close()
	if peer.CanonicalTenantID == "" || peer.CanonicalTenantID == session.CanonicalTenantID {
		t.Fatal("restart isolation proof requires a second verified tenant")
	}
	if _, err := udbclient.TableOf[*storagev1.File](peer.Udb).Get(ctx, key); !errors.Is(err, udbclient.ErrNotFound) {
		t.Fatalf("foreign tenant can observe the owned persisted row: %v", err)
	}
	if phase == "verify" {
		if err := files.Delete(ctx, key); err != nil {
			t.Fatalf("clean the owned restart row: %v", err)
		}
	}
}
