package conformance

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// These requests travel over the same served client for fake, live and embedded
// runners. A stale write, a rolled-back revision or an ABA reuse cannot pass.
func runRevisions(t *testing.T, connect Connect, tenant func() string) {
	t.Run("opaque revisions reject stale writes and survive transaction rollback", func(t *testing.T) {
		u := connect(t, tenant())
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		row := file("", NewID(), "revision-original.pdf")
		record, err := udbclient.EncodeRecord(row, udbclient.WriteInsert)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		insert := &entityv1.UpsertRequest{MessageType: udbclient.MessageType(row), RecordJson: raw}
		written, err := u.Data.Broker.Insert(ctx, insert)
		if err != nil || written.GetRevision() == "" {
			t.Fatalf("Insert omitted its committed revision: %v, %v", written, err)
		}
		initial := written.GetRevision()
		filter, _ := structpb.NewStruct(map[string]any{"file_id": row.GetFileId()})
		read := func() string {
			t.Helper()
			result, err := u.Data.Broker.Select(ctx, &entityv1.SelectRequest{
				MessageType: insert.GetMessageType(), Filter: filter,
				Fields: []string{"filename"}, IncludeRevision: true,
			})
			if err != nil || len(result.GetRecordsJson()) != 1 || len(result.GetRecordRevisions()) != 1 || result.GetRecordRevisions()[0] == "" {
				t.Fatalf("Select omitted or misaligned the row revision: %v, %v", result, err)
			}
			var record map[string]any
			if err := json.Unmarshal(result.GetRecordsJson()[0], &record); err != nil || record["file_id"] != row.GetFileId() {
				t.Fatalf("revision read did not include the primary key: %v, %v", record, err)
			}
			return result.GetRecordRevisions()[0]
		}
		if got := read(); got != initial {
			t.Fatalf("served revision differs from Insert: %q, want %q", got, initial)
		}
		changes, _ := structpb.NewStruct(map[string]any{"filename": "revision-updated.pdf"})
		update := &entityv1.UpdateRequest{MessageType: insert.GetMessageType(), Filter: filter,
			Changes: changes, ExpectedRevision: initial, RequireAffected: 1, IdempotencyKey: NewID()}
		first, err := u.Data.Broker.Update(ctx, update)
		if err != nil || first.GetRevision() == "" || first.GetRevision() == initial || read() != first.GetRevision() {
			t.Fatalf("Update did not publish a new revision atomically: %v, %v", first, err)
		}
		replay, err := u.Data.Broker.Update(ctx, update)
		if err != nil || !matchesKeyedReplay(first, replay) || read() != first.GetRevision() {
			t.Fatalf("lost-response retry rechecked or bumped its revision: %v, %v", replay, err)
		}
		changedRevision := proto.Clone(update).(*entityv1.UpdateRequest)
		changedRevision.ExpectedRevision = first.GetRevision()
		changedCount := proto.Clone(update).(*entityv1.UpdateRequest)
		changedCount.RequireAffected = 2
		for _, request := range []*entityv1.UpdateRequest{changedRevision, changedCount} {
			_, err := u.Data.Broker.Update(ctx, request)
			if info := udbclient.Inspect(err); info.Reason != "UDB_IDEMPOTENCY_REUSE" || info.Code != udbclient.CodePrecondition || read() != first.GetRevision() {
				t.Fatalf("changed concurrency guard replayed the old receipt: %+v, %v", info, err)
			}
		}
		assertRevisionRefusal := func(request *entityv1.UpdateRequest, reason string) {
			t.Helper()
			_, err := u.Data.Broker.Update(ctx, request)
			detail, ok := udbclient.AsError(err)
			if info := udbclient.Inspect(err); info.Reason != reason || !ok || len(detail.FieldViolations()) == 0 {
				t.Fatalf("revision refusal = %+v, %v; want %s with a field violation", info, err, reason)
			}
			if read() != first.GetRevision() {
				t.Fatal("refused Update changed the revision")
			}
		}
		stale := proto.Clone(update).(*entityv1.UpdateRequest)
		stale.IdempotencyKey = ""
		assertRevisionRefusal(stale, "UDB_REVISION_CONFLICT")
		rangeUpdate := proto.Clone(stale).(*entityv1.UpdateRequest)
		rangeUpdate.Filter, _ = structpb.NewStruct(map[string]any{"filename": "revision-updated.pdf"})
		rangeUpdate.ExpectedRevision = first.GetRevision()
		assertRevisionRefusal(rangeUpdate, "UDB_CAS_KEY_NOT_PK")
		malformed := proto.Clone(stale).(*entityv1.UpdateRequest)
		malformed.ExpectedRevision = "invalid-revision"
		if _, err := u.Data.Broker.Update(ctx, malformed); udbclient.CodeOf(err) != udbclient.CodeInvalid {
			t.Fatalf("malformed revision = %v, want Invalid", err)
		}
		deleteRequest := &entityv1.DeleteRequest{MessageType: insert.GetMessageType(), Filter: filter,
			ExpectedRevision: initial, RequireAffected: 1}
		if _, err := u.Data.Broker.Delete(ctx, deleteRequest); udbclient.Inspect(err).Reason != "UDB_REVISION_CONFLICT" {
			t.Fatalf("stale Delete = %v, want revision conflict", err)
		}
		key := udbclient.RowKey{"file_id": row.GetFileId()}
		if err := u.Tx(ctx, func(tx *udbclient.TxScope) error {
			return tx.Patch(&storagev1.File{}, key, udbclient.Record{"filename": "revision-transaction.pdf"})
		}); err != nil {
			t.Fatal(err)
		}
		transactionRevision := read()
		if transactionRevision == first.GetRevision() {
			t.Fatal("BeginTx Update left the pre-transaction revision valid")
		}
		stale.ExpectedRevision = first.GetRevision()
		if _, err := u.Data.Broker.Update(ctx, stale); udbclient.Inspect(err).Reason != "UDB_REVISION_CONFLICT" {
			t.Fatalf("pre-transaction revision remained writable: %v", err)
		}
		err = u.Tx(ctx, func(tx *udbclient.TxScope) error {
			if err := tx.Patch(&storagev1.File{}, key, udbclient.Record{"filename": "must-roll-back.pdf"}); err != nil {
				return err
			}
			return tx.Patch(&storagev1.File{}, key, udbclient.Record{"file_id": row.GetFileId()})
		})
		if udbclient.CodeOf(err) != udbclient.CodeInvalid || read() != transactionRevision {
			t.Fatalf("transaction rollback retained a revision change: %v", err)
		}
		files := udbclient.TableOf[*storagev1.File](u)
		if got, err := files.Get(ctx, key); err != nil || got.GetFilename() != "revision-transaction.pdf" {
			t.Fatalf("refused writes changed the stored row: %v, %v", got, err)
		}
		deleteRequest.ExpectedRevision = transactionRevision
		if _, err := u.Data.Broker.Delete(ctx, deleteRequest); err != nil {
			t.Fatal(err)
		}
		// File is soft-deleted. Upsert restores the same primary key; an
		// Insert would correctly refuse the tombstone's unique constraint.
		written, err = u.Data.Broker.Upsert(ctx, insert)
		if err != nil || written.GetRevision() == "" || written.GetRevision() == transactionRevision {
			t.Fatalf("delete/reinsert revived the previous revision: %v, %v", written, err)
		}
		stale.ExpectedRevision = transactionRevision
		if _, err := u.Data.Broker.Update(ctx, stale); udbclient.Inspect(err).Reason != "UDB_REVISION_CONFLICT" {
			t.Fatalf("pre-delete revision was accepted after reinsert: %v", err)
		}
		withoutRevision, err := u.Data.Broker.Select(ctx, &entityv1.SelectRequest{MessageType: insert.GetMessageType(), Filter: filter})
		if err != nil || len(withoutRevision.GetRecordRevisions()) != 0 {
			t.Fatalf("default Select unexpectedly included revisions: %v, %v", withoutRevision, err)
		}
	})
}
