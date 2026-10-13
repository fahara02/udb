package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func matchesKeyedReplay(first, replay *entityv1.MutationResponse) bool {
	if first == nil || replay == nil || first.GetWasDuplicate() || !replay.GetWasDuplicate() {
		return false
	}
	expected := proto.Clone(first).(*entityv1.MutationResponse)
	expected.WasDuplicate = true
	return proto.Equal(expected, replay)
}

// These cases exercise database behavior through the ordinary served client,
// so a fake which accepts a write production refuses cannot pass conformance.
func runConstraints(t *testing.T, connect Connect, tenant func() string) {
	runRevisions(t, connect, tenant)
	t.Run("update admission preserves identity and rolls back transaction prefixes", func(t *testing.T) {
		u := connect(t, tenant())
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		files := udbclient.TableOf[*storagev1.File](u)
		row := file("", NewID(), "immutable-original.pdf")
		if err := files.Insert(ctx, row); err != nil {
			t.Fatal(err)
		}
		key := udbclient.RowKey{"file_id": row.GetFileId()}
		if got, err := files.Get(ctx, key); err != nil || got.GetProjectId() != u.Meta.ProjectID {
			t.Fatalf("stored project differs from verified project: %v, %v", got, err)
		}
		filter, _ := structpb.NewStruct(map[string]any(key))
		messageType := udbclient.MessageType(row)
		for _, column := range []string{"file_id", "tenant_id", "project_id"} {
			// Even resending the current identity is forbidden by Update's
			// admission contract; the setter must never silently ignore it.
			values := map[string]any{"file_id": row.GetFileId(), "tenant_id": u.Meta.TenantID, "project_id": u.Meta.ProjectID}
			changes := udbclient.Record{column: values[column], "filename": "must-not-change.pdf"}
			if err := files.Patch(ctx, key, changes); udbclient.CodeOf(err) != udbclient.CodeInvalid {
				t.Fatalf("immutable %s update = %v, want Invalid", column, err)
			}
			prefix := file("", NewID(), "must-roll-back.pdf")
			err := u.Tx(ctx, func(tx *udbclient.TxScope) error {
				if err := tx.Insert(prefix); err != nil {
					return err
				}
				return tx.Patch(&storagev1.File{}, key, changes)
			})
			if udbclient.CodeOf(err) != udbclient.CodeInvalid {
				t.Fatalf("immutable %s transaction = %v, want Invalid", column, err)
			}
			if _, err := files.Get(ctx, udbclient.RowKey{"file_id": prefix.GetFileId()}); !errors.Is(err, udbclient.ErrNotFound) {
				t.Fatalf("immutable %s refusal committed its transaction prefix: %v", column, err)
			}
		}
		changes, _ := structpb.NewStruct(map[string]any{"size_bytes": "1"})
		for _, request := range []*entityv1.UpdateRequest{
			{MessageType: messageType, Filter: filter},
			{MessageType: messageType, Changes: changes},
			{MessageType: messageType, Filter: filter, Changes: changes,
				Increments: []*entityv1.UpdateRequest_Increment{{Column: "size_bytes", Delta: 2}}},
		} {
			if _, err := u.Data.Broker.Update(ctx, request); udbclient.CodeOf(err) != udbclient.CodeInvalid {
				t.Fatalf("invalid Update shape = %v, want Invalid", err)
			}
		}
		got, err := files.Get(ctx, key)
		if err != nil || got.GetFilename() != row.GetFilename() || got.GetSizeBytes() != 0 || got.GetFileId() != row.GetFileId() {
			t.Fatalf("refused update changed its target: %v, %v", got, err)
		}
	})
	t.Run("unary keys replay before CAS and reject changed input", func(t *testing.T) {
		u := connect(t, tenant())
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		row := file("", NewID(), "keyed-original.pdf")
		record, err := udbclient.EncodeRecord(row, udbclient.WriteInsert)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		insert := &entityv1.UpsertRequest{MessageType: udbclient.MessageType(row), RecordJson: raw, IdempotencyKey: NewID()}
		first, err := u.Data.Broker.Insert(ctx, insert)
		if err != nil {
			t.Fatal(err)
		}
		replay, err := u.Data.Broker.Insert(ctx, insert)
		if err != nil || !matchesKeyedReplay(first, replay) {
			t.Fatalf("identical keyed insert did not replay its original receipt: %v", err)
		}
		// Insert and Upsert are distinct operations even when their key and
		// target match. An Upsert must replace the row, not replay the Insert.
		record["filename"] = "keyed-upserted.pdf"
		raw, err = json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		upsert := proto.Clone(insert).(*entityv1.UpsertRequest)
		upsert.RecordJson = raw
		first, err = u.Data.Broker.Upsert(ctx, upsert)
		if err != nil || first.GetWasDuplicate() {
			t.Fatalf("Insert and Upsert collided in the key namespace: %v", err)
		}
		replay, err = u.Data.Broker.Upsert(ctx, upsert)
		if err != nil || !matchesKeyedReplay(first, replay) {
			t.Fatalf("identical keyed upsert did not replay its original receipt: %v", err)
		}
		filter, _ := structpb.NewStruct(map[string]any{"file_id": row.GetFileId()})
		expected, _ := structpb.NewStruct(map[string]any{"size_bytes": "0"})
		update := &entityv1.UpdateRequest{MessageType: insert.GetMessageType(), Filter: filter,
			Expected: expected, RequireAffected: 1, IdempotencyKey: NewID(),
			Increments: []*entityv1.UpdateRequest_Increment{{Column: "size_bytes", Delta: 7}}}
		first, err = u.Data.Broker.Update(ctx, update)
		if err != nil {
			t.Fatal(err)
		}
		replay, err = u.Data.Broker.Update(ctx, update)
		if err != nil || !matchesKeyedReplay(first, replay) {
			t.Fatalf("keyed update replay checked the already-changed CAS value: %v", err)
		}
		changed := proto.Clone(update).(*entityv1.UpdateRequest)
		changed.Increments[0].Delta = 11
		_, err = u.Data.Broker.Update(ctx, changed)
		info := udbclient.Inspect(err)
		if info.Reason != "UDB_IDEMPOTENCY_REUSE" || info.Code != udbclient.CodePrecondition || info.FixHint == "" {
			t.Fatalf("changed input must have a distinct, actionable key-reuse refusal: %+v, %v", info, err)
		}
		files := udbclient.TableOf[*storagev1.File](u)
		got, err := files.Get(ctx, udbclient.RowKey{"file_id": row.GetFileId()})
		if err != nil || got.GetSizeBytes() != 7 || got.GetFilename() != "keyed-upserted.pdf" {
			t.Fatalf("keyed increment ran more than once or changed-input request wrote: %v, %v", got, err)
		}
		deleteRequest := &entityv1.DeleteRequest{MessageType: insert.GetMessageType(), Filter: filter,
			RequireAffected: 1, IdempotencyKey: NewID()}
		first, err = u.Data.Broker.Delete(ctx, deleteRequest)
		if err != nil {
			t.Fatal(err)
		}
		replay, err = u.Data.Broker.Delete(ctx, deleteRequest)
		if err != nil || !matchesKeyedReplay(first, replay) {
			t.Fatalf("keyed delete replay checked the already-deleted row: %v", err)
		}
		if _, err := files.Get(ctx, udbclient.RowKey{"file_id": row.GetFileId()}); !errors.Is(err, udbclient.ErrNotFound) {
			t.Fatalf("keyed delete left a visible row: %v", err)
		}
	})
	t.Run("explicit primary conflict target replaces the original row", func(t *testing.T) {
		u := connect(t, tenant())
		row := file("", NewID(), "original.pdf")
		files := udbclient.TableOf[*storagev1.File](u)
		ctx := context.Background()
		if err := files.Insert(ctx, row); err != nil {
			t.Fatal(err)
		}
		row.Filename = "replaced.pdf"
		record, err := udbclient.EncodeRecord(row, udbclient.WriteInsert)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := u.Data.Broker.Upsert(ctx, &entityv1.UpsertRequest{
			MessageType: udbclient.MessageType(row), RecordJson: raw, ConflictFields: udbclient.PrimaryKeys(row),
		}); err != nil {
			t.Fatal(err)
		}
		got, err := files.Get(ctx, udbclient.RowKey{"file_id": row.GetFileId()})
		if err != nil || got.GetFilename() != row.GetFilename() {
			t.Fatalf("explicit conflict target did not replace the row: %v, %v", got, err)
		}
	})
	t.Run("unsupported transaction idempotency key refuses the entire batch", func(t *testing.T) {
		u := connect(t, tenant())
		for _, operation := range []string{"upsert", "enqueue_outbox_event"} {
			t.Run(operation, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				row := file("", NewID(), "must-not-commit.pdf")
				record, err := udbclient.EncodeRecord(row, udbclient.WriteInsert)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := json.Marshal(record)
				if err != nil {
					t.Fatal(err)
				}
				payload, _ := structpb.NewStruct(map[string]any{
					"event_id": NewID(), "event_type": "conformance",
					"document_id": row.GetFileId(), "correlation_id": NewID(),
					"envelope_version": udbclient.EventEnvelopeVersion, "payload": map[string]any{},
				})
				stream, err := u.Data.Broker.BeginTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				keyed := &entityv1.Mutation{Operation: operation,
					MessageType: udbclient.MessageType(row), RecordJson: raw, IdempotencyKey: NewID()}
				if operation == "enqueue_outbox_event" {
					keyed.RecordJson = nil
					keyed.Collection, keyed.ObjectKey, keyed.Payload = "udb.conformance.v1", row.GetFileId(), payload
				}
				for _, mutation := range []*entityv1.Mutation{
					{Operation: "upsert", MessageType: udbclient.MessageType(row), RecordJson: raw},
					keyed,
					{Commit: true},
				} {
					if err := stream.Send(mutation); err != nil {
						if !errors.Is(err, io.EOF) {
							t.Fatal(err)
						}
						break // drain the authoritative refusal below
					}
				}
				_ = stream.CloseSend()
				var details []*entityv1.ErrorDetail
				for {
					frame, err := stream.Recv()
					if err != nil {
						break
					}
					details = append(details, frame.GetErrorDetail())
				}
				for _, raw := range stream.Trailer().Get("udb-error-detail-bin") {
					var detail entityv1.ErrorDetail
					if err := proto.Unmarshal([]byte(raw), &detail); err != nil {
						t.Fatal(err)
					}
					details = append(details, &detail)
				}
				refusedKey := false
				for _, detail := range details {
					for _, violation := range detail.GetFieldViolations() {
						refusedKey = refusedKey || violation.GetField() == "mutations[1].idempotency_key"
					}
				}
				if !refusedKey {
					t.Fatalf("ignored key was not explicitly refused: %v", details)
				}
				if _, err := udbclient.TableOf[*storagev1.File](u).Get(ctx, udbclient.RowKey{"file_id": row.GetFileId()}); !errors.Is(err, udbclient.ErrNotFound) {
					t.Fatalf("refused transaction kept its prefix: %v", err)
				}
			})
		}
	})
	t.Run("secondary uniqueness refuses insert and rolls back a multirow update", func(t *testing.T) {
		ctx := context.Background()
		u := connect(t, tenant())
		files := udbclient.TableOf[*storagev1.File](u)
		first, second := file("", NewID(), "first.pdf"), file("", NewID(), "second.pdf")
		for _, row := range []*storagev1.File{first, second} {
			if err := files.Insert(ctx, row); err != nil {
				t.Fatal(err)
			}
		}
		duplicate := file("", NewID(), "duplicate.pdf")
		duplicate.ObjectKey = first.GetObjectKey()
		if err := files.Insert(ctx, duplicate); !udbclient.IsUnique(err) || udbclient.Inspect(err).Constraint == "" {
			t.Fatalf("secondary duplicate: %v (%+v)", err, udbclient.Inspect(err))
		}
		filter, _ := structpb.NewStruct(map[string]any{"file_id": map[string]any{"$in": []any{first.GetFileId(), second.GetFileId()}}})
		changes, _ := structpb.NewStruct(map[string]any{"object_key": "conformance/shared/" + NewID(), "filename": "must-rollback.pdf"})
		_, err := u.Data.Broker.Update(ctx, &entityv1.UpdateRequest{MessageType: udbclient.MessageType(first), Filter: filter, Changes: changes})
		if !udbclient.IsUnique(err) {
			t.Fatalf("multirow collision: %v", err)
		}
		for _, original := range []*storagev1.File{first, second} {
			got, err := files.Get(ctx, udbclient.RowKey{"file_id": original.GetFileId()})
			if err != nil || got.GetFilename() != original.GetFilename() || got.GetObjectKey() != original.GetObjectKey() {
				t.Fatalf("failed statement changed a row: %v, %v", got, err)
			}
		}
		fresh := file("", NewID(), "tx-prefix.pdf")
		err = u.Tx(ctx, func(tx *udbclient.TxScope) error {
			if err := tx.Insert(fresh); err != nil {
				return err
			}
			return tx.Insert(duplicate)
		})
		if !udbclient.IsUnique(err) {
			t.Fatalf("transaction secondary collision: %v", err)
		}
		if _, err := files.Get(ctx, udbclient.RowKey{"file_id": fresh.GetFileId()}); !errors.Is(err, udbclient.ErrNotFound) {
			t.Fatalf("unique refusal kept transaction prefix: %v", err)
		}
	})
	t.Run("NOT NULL refusal names the column", func(t *testing.T) {
		u := connect(t, tenant())
		raw := []byte(fmt.Sprintf(`{"file_id":%q,"filename":null,"object_key":%q}`, NewID(), "conformance/"+NewID()))
		_, err := u.Data.Broker.Upsert(context.Background(), &entityv1.UpsertRequest{MessageType: udbclient.MessageType(&storagev1.File{}), RecordJson: raw})
		if info := udbclient.Inspect(err); info.Code != udbclient.CodeNotNull || info.Column != "filename" {
			t.Fatalf("NULL refusal: %v (%+v)", err, info)
		}
	})
	t.Run("int64 stays exact through storage and an atomic increment", func(t *testing.T) {
		files := udbclient.TableOf[*storagev1.File](connect(t, tenant()))
		row := file("", NewID(), "large.pdf")
		row.SizeBytes = 9007199254740993
		ctx := context.Background()
		if err := files.Insert(ctx, row); err != nil {
			t.Fatal(err)
		}
		key := udbclient.RowKey{"file_id": row.GetFileId()}
		got, err := files.Get(ctx, key)
		if err != nil || got.GetSizeBytes() != row.GetSizeBytes() {
			t.Fatalf("rounded stored int64: %v, %v", got, err)
		}
		if err := files.Increment(ctx, key, map[string]float64{"size_bytes": 1}); err != nil {
			t.Fatal(err)
		}
		got, err = files.Get(ctx, key)
		if err != nil || got.GetSizeBytes() != row.GetSizeBytes()+1 {
			t.Fatalf("rounded increment: %v, %v", got, err)
		}
	})
	t.Run("soft delete retains unique keys and upsert revives the row", func(t *testing.T) {
		files := udbclient.TableOf[*storagev1.File](connect(t, tenant()))
		row := file("", NewID(), "deleted.pdf")
		ctx := context.Background()
		if err := files.Insert(ctx, row); err != nil {
			t.Fatal(err)
		}
		key := udbclient.RowKey{"file_id": row.GetFileId()}
		if err := files.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
		if _, err := files.Get(ctx, key); !errors.Is(err, udbclient.ErrNotFound) {
			t.Fatalf("tombstone remained visible: %v", err)
		}
		if err := files.Insert(ctx, row); !udbclient.IsUnique(err) {
			t.Fatalf("strict insert reused a tombstone key: %v", err)
		}
		row.Filename = "revived.pdf"
		if err := files.Upsert(ctx, row); err != nil {
			t.Fatal(err)
		}
		if got, err := files.Get(ctx, key); err != nil || got.GetFilename() != row.GetFilename() || got.GetDeletedAt() != nil {
			t.Fatalf("upsert did not revive tombstone: %v, %v", got, err)
		}
	})
	t.Run("default page caps at 100 and SelectAll visits every row", func(t *testing.T) {
		u := connect(t, tenant())
		files := udbclient.TableOf[*storagev1.File](u)
		ids := make([]any, 0, 103)
		ctx := context.Background()
		if err := u.Tx(ctx, func(tx *udbclient.TxScope) error {
			for i := 0; i < 103; i++ {
				row := file("", NewID(), fmt.Sprintf("page-%03d.pdf", i))
				ids = append(ids, row.GetFileId())
				if err := tx.Insert(row); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		filter := udbclient.Filter{"file_id": udbclient.Filter{"$in": ids}}
		page, err := files.Select(ctx, filter, udbclient.SelectOptions{IncludeTotal: true})
		if err != nil || len(page.Rows) != 100 || !page.HasMore || page.Total != 103 {
			t.Fatalf("default page: rows=%d hasMore=%v total=%d err=%v", len(page.Rows), page.HasMore, page.Total, err)
		}
		seen := map[string]bool{}
		if err := files.SelectAll(ctx, filter, func(row *storagev1.File) error {
			if seen[row.GetFileId()] {
				return fmt.Errorf("duplicate row %s", row.GetFileId())
			}
			seen[row.GetFileId()] = true
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(seen) != 103 {
			t.Fatalf("SelectAll visited %d rows", len(seen))
		}
	})
}
