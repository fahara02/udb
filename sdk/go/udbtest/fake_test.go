package udbtest_test

import (
	"context"
	"testing"

	apikeyeventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/events/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"github.com/fahara02/udb/sdk/go/udbtest"
	"github.com/fahara02/udb/sdk/go/udbtest/conformance"
)

// The fake meets the same typed-store contract the live broker does.
func TestFakeMeetsTheTableContract(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	conformance.RunTable(t, func(t testing.TB, tenant string) *udbclient.Udb {
		return fake.Client(t, tenant)
	})
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
