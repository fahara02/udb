package udbtest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	apikeyeventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/events/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	workflowentityv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/workflow/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"github.com/fahara02/udb/sdk/go/udbtest"
	"github.com/fahara02/udb/sdk/go/udbtest/conformance"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// The fake meets the same typed-store contract the live broker does.
func TestFakeMeetsTheTableContract(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	conformance.RunTable(t, func(t testing.TB, tenant string) *udbclient.Udb {
		return fake.Client(t, tenant)
	})
}

// A string written to a JSON column is JSON text, as on the broker, so every kind of value reads
// back as what was written.
func TestFakeHoldsAJSONColumnAsTheBrokerDoes(t *testing.T) {
	const message = "udb.core.workflow.entity.v1.WorkflowInstance"
	fake := udbtest.New(t, &workflowentityv1.WorkflowInstance{})
	tenant := "0190f1b2-0000-4000-8000-0000000000aa"
	client := fake.Client(t, tenant)
	ctx := context.Background()
	for i, payload := range []string{`"My"`, `"5"`, `""`, `5`, `[1]`, `{"a":1}`} {
		id := fmt.Sprintf("0190f1b2-0000-4000-8000-%012d", i+1)
		row := &workflowentityv1.WorkflowInstance{WorkflowId: id, TenantId: tenant, WorkflowType: "json", Payload: payload}
		for attempt := 0; attempt < 3; attempt++ {
			rec, err := udbclient.EncodeRecord(row, udbclient.WriteInsert)
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(rec)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.Data.Upsert(ctx, &entityv1.UpsertRequest{MessageType: message, RecordJson: body}); err != nil {
				t.Fatalf("upsert %s: %v", payload, err)
			}
			filter, err := structpb.NewStruct(map[string]any{"workflow_id": id})
			if err != nil {
				t.Fatal(err)
			}
			found, err := client.Data.Select(ctx, &entityv1.SelectRequest{MessageType: message, Filter: filter, Limit: 1})
			if err != nil || len(found.GetRecordsJson()) != 1 {
				t.Fatalf("read %s: %v", payload, err)
			}
			var stored udbclient.Record
			if err := json.Unmarshal(found.GetRecordsJson()[0], &stored); err != nil {
				t.Fatal(err)
			}
			var back workflowentityv1.WorkflowInstance
			if err := udbclient.DecodeRecord(stored, &back); err != nil {
				t.Fatal(err)
			}
			if back.GetPayload() != payload {
				t.Fatalf("%s read back as %q", payload, back.GetPayload())
			}
			row.Payload = back.GetPayload()
		}
	}
	for _, text := range []string{`My`, `null`} {
		body, _ := json.Marshal(map[string]any{"workflow_id": "0190f1b2-0000-4000-8000-000000000099", "workflow_type": "json", "payload": text})
		_, err := client.Data.Upsert(ctx, &entityv1.UpsertRequest{MessageType: message, RecordJson: body})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("the text %s must be refused: code=%s %v", text, status.Code(err), err)
		}
	}
}

// The broker compares `expected` on a JSON column as the document its text encodes.
// The fake compared the text with the stored document, so a row whose JSON was "[]"
// could not be the expectation of its own next write.
func TestFakeComparesExpectedJSONTextAsTheStoredDocument(t *testing.T) {
	const message = "udb.core.workflow.entity.v1.WorkflowInstance"
	fake := udbtest.New(t, &workflowentityv1.WorkflowInstance{})
	tenant := "0190f1b2-0000-4000-8000-0000000000aa"
	client := fake.Client(t, tenant)
	ctx := context.Background()
	for i, payload := range []string{`[]`, `{}`, `[1]`, `{"a":[]}`} {
		id := fmt.Sprintf("0190f1b2-0000-4000-8000-%012d", i+100)
		row := map[string]any{"workflow_id": id, "workflow_type": "json", "payload": payload}
		body, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Data.Upsert(ctx, &entityv1.UpsertRequest{MessageType: message, RecordJson: body}); err != nil {
			t.Fatalf("upsert %s: %v", payload, err)
		}
		expected, err := structpb.NewStruct(row)
		if err != nil {
			t.Fatal(err)
		}
		next, err := json.Marshal(map[string]any{"workflow_id": id, "workflow_type": "json-2", "payload": payload})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Data.Upsert(ctx, &entityv1.UpsertRequest{MessageType: message, RecordJson: next, Expected: expected}); err != nil {
			t.Fatalf("expected %s: %v", payload, err)
		}
	}
}

// The fake passes the cross-SDK cases the TypeScript runner shares.
func TestFakeMeetsTheSharedTableCases(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	conformance.RunTableCases(t, func(t testing.TB, tenant string) *udbclient.Udb {
		return fake.Client(t, tenant)
	}, "../../conformance/table-cases.json")
}

// Emit builds the envelope from the event's own contract and commits it with
// the transaction's writes.
func TestEmitCommitsTheEventWithTheTransaction(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	tenant := "0190f1b2-0000-4000-8000-0000000000aa"
	u := fake.Client(t, tenant)
	err := u.Tx(context.Background(), func(tx *udbclient.TxScope) error {
		if err := tx.Upsert(&storagev1.File{FileId: "0190f1b2-0000-4000-8000-000000000001", Filename: "a.pdf", ObjectKey: "k"}); err != nil {
			return err
		}
		return tx.Emit(&apikeyeventsv1.ApiKeyCreatedEvent{KeyId: "key-1", TenantId: tenant, Name: "ci"})
	})
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	events := fake.Events()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	e := events[0]
	if e.Topic != "udb.apikey.created.v1" || e.PartitionKey != tenant {
		t.Fatalf("topic/partition = %q/%q", e.Topic, e.PartitionKey)
	}
	if e.Envelope["event_type"] != "udb.core.apikey.events.v1.ApiKeyCreatedEvent" || e.Envelope["envelope_version"] != float64(udbclient.EventEnvelopeVersion) {
		t.Fatalf("envelope = %v", e.Envelope)
	}
	payload, _ := e.Envelope["payload"].(map[string]any)
	if payload["key_id"] != "key-1" {
		t.Fatalf("the event's fields must ride under payload, got %v", e.Envelope)
	}

	// An event with no partition-key value fails before anything is sent.
	err = u.Tx(context.Background(), func(tx *udbclient.TxScope) error {
		return tx.Emit(&apikeyeventsv1.ApiKeyCreatedEvent{KeyId: "key-2"})
	})
	if err == nil {
		t.Fatal("an event without its partition key must be refused")
	}
	if len(fake.Events()) != 1 {
		t.Fatal("a refused emit must not be committed")
	}
}

// Inspect turns a refusal into the closed error code with the broker's reason.
func TestInspectReadsTheBrokersReason(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	u := fake.Client(t, "0190f1b2-0000-4000-8000-0000000000bb")
	files := udbclient.TableOf[*storagev1.File](u)
	err := files.Delete(context.Background(), udbclient.RowKey{"file_id": "0190f1b2-0000-4000-8000-000000000009"})
	info := udbclient.Inspect(err)
	if info.Code != udbclient.CodeNotFound || info.Reason != "UDB_NO_ROWS_AFFECTED" || !udbclient.IsNotFound(err) {
		t.Fatalf("Inspect = %+v", info)
	}
	_, err = files.Select(context.Background(), udbclient.Filter{"filename": udbclient.Filter{"$regex": "x"}}, udbclient.SelectOptions{})
	if got := udbclient.Inspect(err); got.Code != udbclient.CodeInvalid || got.Reason != "UDB_UNSUPPORTED_FILTER_OPERATOR" {
		t.Fatalf("unknown operator = %+v", got)
	}
}
