package udbtest_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"github.com/fahara02/udb/sdk/go/udbtest"
)

const (
	bulkFileType  = "udb.core.storage.entity.v1.File"
	bulkFileTopic = "udb.storage.files.cdc"
)

func bulkFileEvents(fake *udbtest.Fake) []udbtest.Event {
	var out []udbtest.Event
	for _, event := range fake.Events() {
		if event.Topic == bulkFileTopic {
			out = append(out, event)
		}
	}
	return out
}

func bulkFileRow(id, name string) map[string]any {
	return map[string]any{"file_id": id, "filename": name, "object_key": "bulk/" + id, "status": "PENDING"}
}

// BulkLoad splits a load into batches; each batch is one transaction with one
// change event on the entity's cdc_topic.
func TestFakeBulkLoadWritesBatchesWithOneEventEach(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	tenant := "0190f1b2-0000-4000-8000-0000000000b1"
	files := udbclient.TableOf[*storagev1.File](fake.Client(t, tenant))
	rows := make([]*storagev1.File, 2500)
	for i := range rows {
		id := fmt.Sprintf("0190f1b2-0000-4000-8001-%012d", i)
		rows[i] = &storagev1.File{FileId: id, Filename: fmt.Sprintf("f%d.pdf", i), ObjectKey: "bulk/" + id, Status: storagev1.FileStatus_FILE_STATUS_PENDING}
	}
	var seen []int
	result, err := files.BulkLoad(context.Background(), rows, udbclient.BulkLoadOptions{
		BatchRows:     1000,
		BatchIDPrefix: "load",
		OnBatch: func(index int, resp *entityv1.BulkUpsertResponse) error {
			seen = append(seen, index)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("bulk load: %v", err)
	}
	if result.Batches != 3 || result.Rows != 2500 || result.AffectedRows != 2500 || len(result.Responses) != 3 {
		t.Fatalf("result = %d batches, %d rows, %d affected, %d responses", result.Batches, result.Rows, result.AffectedRows, len(result.Responses))
	}
	if fmt.Sprint(seen) != "[0 1 2]" {
		t.Fatalf("OnBatch indexes = %v", seen)
	}
	if stored := fake.Rows(bulkFileType, tenant); len(stored) != 2500 {
		t.Fatalf("stored %d rows, want 2500", len(stored))
	}
	events := bulkFileEvents(fake)
	if len(events) != 3 {
		t.Fatalf("%d events on %s, want 3 (one per batch)", len(events), bulkFileTopic)
	}
	for i, want := range []float64{1000, 1000, 500} {
		event := events[i]
		batchID := fmt.Sprintf("load/%06d", i)
		payload, _ := event.Envelope["payload"].(map[string]any)
		if event.Envelope["operation"] != "bulk_upsert" || payload["row_count"] != want ||
			event.PartitionKey != batchID || payload["batch_id"] != batchID || event.Envelope["document_id"] != batchID {
			t.Fatalf("event %d = %v (partition %q)", i, event.Envelope, event.PartitionKey)
		}
		response := result.Responses[i]
		if response.GetBatchId() != batchID || response.GetEventId() != event.Envelope["event_id"] ||
			response.GetRowCount() != uint32(want) || response.GetStatementCount() != 1 || len(response.GetKeysSha256()) != 64 ||
			response.GetFirstKey() != "s:"+rows[i*1000].GetFileId() {
			t.Fatalf("response %d = %v", i, response)
		}
	}
}

// A batch that repeats a key, or whose rows differ in shape, writes nothing
// and emits nothing.
func TestFakeBulkUpsertRefusesABadBatchWhole(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	tenant := "0190f1b2-0000-4000-8000-0000000000b2"
	client := fake.Client(t, tenant)
	ctx := context.Background()
	fresh, repeated := "0190f1b2-0000-4000-8002-000000000001", "0190f1b2-0000-4000-8002-000000000002"

	_, err := client.BulkUpsert(ctx, bulkFileType, []map[string]any{
		bulkFileRow(fresh, "a.pdf"), bulkFileRow(repeated, "b.pdf"), bulkFileRow(repeated, "c.pdf"),
	}, udbclient.BulkUpsertBatchOptions{})
	if info := udbclient.Inspect(err); info.Code != udbclient.CodeInvalid || info.Reason != "UDB_BULK_DUPLICATE_KEY" {
		t.Fatalf("duplicate key batch = %v (%s), want invalid UDB_BULK_DUPLICATE_KEY", err, info.Reason)
	}
	if stored := fake.Rows(bulkFileType, tenant); len(stored) != 0 {
		t.Fatalf("a refused batch wrote %d rows", len(stored))
	}
	if events := bulkFileEvents(fake); len(events) != 0 {
		t.Fatalf("a refused batch emitted %d events", len(events))
	}

	short := bulkFileRow(repeated, "b.pdf")
	delete(short, "status")
	_, err = client.BulkUpsert(ctx, bulkFileType, []map[string]any{bulkFileRow(fresh, "a.pdf"), short}, udbclient.BulkUpsertBatchOptions{})
	if info := udbclient.Inspect(err); info.Code != udbclient.CodeInvalid || info.Reason != "UDB_BULK_ROW_SHAPE_MISMATCH" {
		t.Fatalf("shape mismatch batch = %v (%s), want invalid UDB_BULK_ROW_SHAPE_MISMATCH", err, info.Reason)
	}
	if stored := fake.Rows(bulkFileType, tenant); len(stored) != 0 {
		t.Fatalf("a refused batch wrote %d rows", len(stored))
	}

	// A load stops at the first refused batch and reports what committed.
	rows := []map[string]any{bulkFileRow(fresh, "a.pdf"), bulkFileRow(repeated, "b.pdf"),
		bulkFileRow("0190f1b2-0000-4000-8002-000000000003", "c.pdf"), short}
	result, err := client.BulkLoad(ctx, bulkFileType, rows, udbclient.BulkLoadOptions{BatchRows: 2})
	var stopped *udbclient.BulkLoadError
	if !errors.As(err, &stopped) || stopped.Result.Batches != 1 || result.Batches != 1 || result.Rows != 2 ||
		udbclient.Inspect(err).Reason != "UDB_BULK_ROW_SHAPE_MISMATCH" {
		t.Fatalf("load = %+v, %v", result, err)
	}
	if stored := fake.Rows(bulkFileType, tenant); len(stored) != 2 {
		t.Fatalf("the load kept %d rows, want the first batch's 2", len(stored))
	}
}

// The same idempotency key replays an identical batch without writing it again
// and refuses a different batch.
func TestFakeBulkUpsertReplaysAKeyedBatch(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	tenant := "0190f1b2-0000-4000-8000-0000000000b3"
	client := fake.Client(t, tenant)
	ctx := context.Background()
	rows := []map[string]any{bulkFileRow("0190f1b2-0000-4000-8003-000000000001", "a.pdf")}
	opts := udbclient.BulkUpsertBatchOptions{IdempotencyKey: "bulk-key-1"}
	first, err := client.BulkUpsert(ctx, bulkFileType, rows, opts)
	if err != nil || first.GetWasDuplicate() {
		t.Fatalf("first = %v, %v", first, err)
	}
	again, err := client.BulkUpsert(ctx, bulkFileType, rows, opts)
	if err != nil || !again.GetWasDuplicate() || again.GetBatchId() != first.GetBatchId() {
		t.Fatalf("replay = %v, %v", again, err)
	}
	if events := bulkFileEvents(fake); len(events) != 1 {
		t.Fatalf("replay emitted again: %d events", len(events))
	}
	other := []map[string]any{bulkFileRow("0190f1b2-0000-4000-8003-000000000002", "b.pdf")}
	if _, err := client.BulkUpsert(ctx, bulkFileType, other, opts); udbclient.Inspect(err).Reason != "UDB_IDEMPOTENCY_REUSE" {
		t.Fatalf("reused key = %v, want UDB_IDEMPOTENCY_REUSE", err)
	}
}
