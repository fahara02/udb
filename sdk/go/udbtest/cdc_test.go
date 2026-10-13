package udbtest_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	apikeyeventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/events/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"github.com/fahara02/udb/sdk/go/udbtest"
	"github.com/fahara02/udb/sdk/go/udbtest/conformance"
	"google.golang.org/grpc/codes"
)

func TestFakeCDCPositionsAreMonotonicAndOwnedByTheVerifiedPrincipal(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	tenant := conformance.NewID()
	u := fake.Client(t, tenant)
	const topic = "udb.apikey.created.v1"
	first := fake.Emit(tenant, topic, tenant, map[string]any{"key_id": "first"})
	second := fake.Emit(tenant, topic, tenant, map[string]any{"key_id": "second"})
	third := fake.Emit(tenant, topic, tenant, map[string]any{"key_id": "third"})
	ack := func(client *udbclient.Udb, id string) error {
		_, err := client.Data.Broker.AckCdcEvents(context.Background(), &entityv1.AckCdcEventsRequest{ConsumerName: "owned", TopicPattern: topic, EventId: id})
		return err
	}
	if err := ack(u, second); err != nil {
		t.Fatal(err)
	}
	if err := ack(u, first); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := u.Data.Broker.PublishCDC(ctx, &entityv1.CDCSubscriptionRequest{ConsumerName: "owned", TopicPattern: topic})
	if err != nil {
		t.Fatal(err)
	}
	event, err := stream.Recv()
	if err != nil || event.GetEventId() != third {
		t.Fatalf("older ACK moved cursor backward: %v, %v", event, err)
	}
	fake.AddIdentityAPIKey("other-owner", udbtest.Identity{TenantID: tenant, ServiceIdentity: "other-owner", Scopes: []string{"udb:admin", "udb:read", "udb:write"}})
	other := fake.ClientForKey(t, "other-owner")
	if err := ack(other, third); udbclient.CodeOf(err) != udbclient.CodePolicyDenied {
		t.Fatalf("another owner acknowledged: %v", err)
	}
	foreign := fake.Emit(conformance.NewID(), topic, tenant, map[string]any{"key_id": "foreign"})
	if err := ack(u, foreign); udbclient.CodeOf(err) != udbclient.CodePolicyDenied {
		t.Fatalf("foreign event acknowledged: %v", err)
	}
	if err := ack(u, "not-a-uuid"); udbclient.CodeOf(err) != udbclient.CodeInvalid {
		t.Fatalf("invalid event ID: %v", err)
	}
}

func TestFakePoisonSettlementIsAtomicAndOperatorReplaySurvivesRetry(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	tenant := conformance.NewID()
	u := fake.Client(t, tenant)
	const topic = "udb.apikey.created.v1"
	failed := fake.Emit(tenant, topic, tenant, map[string]any{"key_id": "poison"})
	request := &entityv1.NackCdcEventsRequest{ConsumerName: "consumer-a", TopicPattern: topic, EventId: failed, Attempts: 2, FailureReason: "handler failed"}
	if err := fake.InjectFault(udbtest.Fault{Method: "NackCdcEvents", Code: codes.Internal}); err != nil {
		t.Fatal(err)
	}
	if _, err := u.Data.Broker.NackCdcEvents(context.Background(), request); udbclient.CodeOf(err) != udbclient.CodeInternal {
		t.Fatalf("settlement injection: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	stream, err := u.Data.Broker.PublishCDC(ctx, &entityv1.CDCSubscriptionRequest{ConsumerName: "consumer-a", TopicPattern: topic})
	if err != nil {
		t.Fatal(err)
	}
	event, err := stream.Recv()
	cancel()
	if err != nil || event.GetEventId() != failed {
		t.Fatalf("failed settlement advanced cursor: %v, %v", event, err)
	}
	listed, err := u.Data.Broker.ListDlqEvents(context.Background(), &entityv1.DlqListRequest{Topic: topic})
	if err != nil || len(listed.GetEvents()) != 0 {
		t.Fatalf("failed settlement retained DLQ row: %v, %v", listed, err)
	}
	if _, err := u.Data.Broker.NackCdcEvents(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	request.ConsumerName = "consumer-b"
	if _, err := u.Data.Broker.NackCdcEvents(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	listed, err = u.Data.Broker.ListDlqEvents(context.Background(), &entityv1.DlqListRequest{Topic: topic})
	if err != nil || len(listed.GetEvents()) != 1 {
		t.Fatalf("independent consumers duplicated DLQ event: %v, %v", listed, err)
	}
	record := listed.GetEvents()[0]
	var payload struct {
		FailedEvent map[string]any `json:"failed_event"`
		Failures    map[string]any `json:"consumer_failures"`
	}
	if err := json.Unmarshal(record.GetPayloadJson(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.FailedEvent["event_id"] != failed || len(payload.Failures) != 2 || record.GetStatus() != "QUARANTINED" {
		t.Fatalf("noncanonical DLQ payload: %s", record.GetPayloadJson())
	}
	if _, err := u.Data.Broker.ReplayDlqEvent(context.Background(), &entityv1.DlqActionRequest{DlqId: record.GetDlqId()}); err != nil {
		t.Fatal(err)
	}
	if _, err := u.Data.Broker.NackCdcEvents(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	got, err := u.Data.Broker.GetDlqEvent(context.Background(), &entityv1.DlqEventRequest{DlqId: record.GetDlqId()})
	if err != nil || got.GetEvent().GetStatus() != "REPLAYED" {
		t.Fatalf("late settlement undid operator replay: %v, %v", got, err)
	}
	foreign := fake.Client(t, conformance.NewID())
	if _, err := foreign.Data.Broker.GetDlqEvent(context.Background(), &entityv1.DlqEventRequest{DlqId: record.GetDlqId()}); !udbclient.IsNotFound(err) {
		t.Fatalf("foreign operator read DLQ: %v", err)
	}
}

func TestFakeConsumeDeadLettersPoisonAndHandlesTheNextEvent(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	tenant := conformance.NewID()
	u := fake.Client(t, tenant)
	const topic = "udb.apikey.created.v1"
	for _, name := range []string{"prefix", "poison", "next"} {
		fake.Emit(tenant, topic, tenant, map[string]any{"key_id": name})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	counts := map[string]int{}
	err := udbclient.Consume(ctx, u, "poison-test", topic, func(_ context.Context, event udbclient.ConsumedEvent[*apikeyeventsv1.ApiKeyCreatedEvent]) error {
		name := event.Payload.GetKeyId()
		counts[name]++
		if name == "poison" {
			return errors.New("permanent handler error")
		}
		if name == "next" {
			cancel()
		}
		return nil
	}, udbclient.ConsumeOptions{MaxAttempts: 2, Backoff: time.Millisecond})
	if !errors.Is(err, context.Canceled) || counts["prefix"] != 1 || counts["poison"] != 2 || counts["next"] != 1 {
		t.Fatalf("consume counts=%v err=%v", counts, err)
	}
	rows, err := u.Data.Broker.ListDlqEvents(context.Background(), &entityv1.DlqListRequest{Topic: topic})
	if err != nil || len(rows.GetEvents()) != 1 {
		t.Fatalf("consumer DLQ: %v, %v", rows, err)
	}
}
