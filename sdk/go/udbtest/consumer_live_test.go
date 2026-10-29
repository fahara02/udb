package udbtest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	storageeventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/events/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// Observe the real server's acknowledgments; handler completion alone does not
// prove a durable cursor write completed before the consumer is interrupted.
type observedConsumerBroker struct {
	servicesv1.DataBrokerClient
	acknowledged chan string
	deadLettered chan *entityv1.NackCdcEventsRequest
	watched      sync.Map
}

func (b *observedConsumerBroker) AckCdcEvents(ctx context.Context, req *entityv1.AckCdcEventsRequest, opts ...grpc.CallOption) (*entityv1.AckCdcEventsResponse, error) {
	response, err := b.DataBrokerClient.AckCdcEvents(ctx, req, opts...)
	if _, watched := b.watched.Load(req.GetEventId()); err == nil && watched {
		b.acknowledged <- req.GetEventId()
	}
	return response, err
}

func (b *observedConsumerBroker) NackCdcEvents(ctx context.Context, req *entityv1.NackCdcEventsRequest, opts ...grpc.CallOption) (*entityv1.AckCdcEventsResponse, error) {
	response, err := b.DataBrokerClient.NackCdcEvents(ctx, req, opts...)
	if _, watched := b.watched.Load(req.GetEventId()); err == nil && watched && b.deadLettered != nil {
		b.deadLettered <- req
	}
	return response, err
}

func TestLiveDurableConsumerRedeliversUntilAcknowledged(t *testing.T) {
	if os.Getenv("UDB_LIVE_CDC_TESTS") != "1" {
		t.Skip("requires the CI served CDC lane (UDB_LIVE_CDC_TESTS=1)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sess := connectLive(t, ctx)
	defer sess.Close()
	broker := &observedConsumerBroker{DataBrokerClient: sess.Udb.Data.Broker, acknowledged: make(chan string, 4)}
	sess.Udb.Data.Broker = broker
	name := fmt.Sprintf("go-live-%d", time.Now().UnixNano())
	filePrefix := name + "/"
	topic := udbclient.Topic(&storageeventsv1.FileUploaded{})
	emit := func(file string) {
		t.Helper()
		if err := sess.Udb.Tx(ctx, func(tx *udbclient.TxScope) error {
			return tx.Emit(&storageeventsv1.FileUploaded{FileId: filePrefix + file, TenantId: sess.Udb.Meta.TenantID, ObjectKey: "consumer/" + file})
		}); err != nil {
			t.Fatalf("commit typed outbox event: %v", err)
		}
	}
	emit("first")
	firstContext, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	received := make(chan string, 1)
	firstStopped := make(chan error, 1)
	go func() {
		firstStopped <- udbclient.Consume(firstContext, sess.Udb, name, topic,
			func(handlerContext context.Context, event udbclient.ConsumedEvent[*storageeventsv1.FileUploaded]) error {
				if event.Payload.GetFileId() != filePrefix+"first" {
					return nil
				}
				broker.watched.Store(event.ID, true)
				received <- event.ID
				<-handlerContext.Done()
				return handlerContext.Err()
			}, udbclient.ConsumeOptions{FromEarliest: true, MaxAttempts: 1, Backoff: time.Millisecond})
	}()
	var original string
	select {
	case original = <-received:
	case <-ctx.Done():
		t.Fatal("retained outbox event did not reach the first consumer")
	}
	stopFirst()
	select {
	case <-firstStopped:
	case <-ctx.Done():
		t.Fatal("interrupted consumer did not stop")
	}
	select {
	case <-broker.acknowledged:
		t.Fatal("interrupted handler was acknowledged")
	default:
	}
	// A fresh Consume call has an empty SDK dedup set. Its position must come
	// from the broker, first redelivering the interrupted event, then moving on.
	for _, expected := range []string{"first", "second"} {
		if expected == "second" {
			emit("second")
		}
		runContext, stop := context.WithCancel(ctx)
		finished := make(chan error, 1)
		go func() {
			finished <- udbclient.Consume(runContext, sess.Udb, name, topic,
				func(_ context.Context, event udbclient.ConsumedEvent[*storageeventsv1.FileUploaded]) error {
					if !strings.HasPrefix(event.Payload.GetFileId(), filePrefix) {
						return nil
					}
					if event.Payload.GetFileId() != filePrefix+expected || (expected == "first" && event.ID != original) {
						t.Errorf("unexpected redelivery: file=%s event=%s", event.Payload.GetFileId(), event.ID)
						return fmt.Errorf("unexpected redelivery: file=%s event=%s", event.Payload.GetFileId(), event.ID)
					}
					broker.watched.Store(event.ID, true)
					return nil
				}, udbclient.ConsumeOptions{FromEarliest: true, MaxAttempts: 1, Backoff: time.Millisecond})
		}()
		select {
		case <-broker.acknowledged:
		case err := <-finished:
			stop()
			t.Fatalf("consumer ended before durable acknowledgment: %v", err)
		case <-ctx.Done():
			stop()
			t.Fatalf("consumer did not acknowledge %s", expected)
		}
		stop()
		select {
		case err := <-finished:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("consumer stopped with %v", err)
			}
		case <-ctx.Done():
			t.Fatal("consumer shutdown did not finish")
		}
	}
}

// This uses the actual TCP broker, journal, durable cursor and operator DLQ
// APIs. Handler completion, mock acknowledgments and raw SQL alone are weaker.
func TestLiveConsumerBatchesDeadLettersAndContinues(t *testing.T) {
	if os.Getenv("UDB_LIVE_CDC_TESTS") != "1" {
		t.Skip("requires the CI served CDC lane")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sess := connectLive(t, ctx)
	defer sess.Close()
	broker := &observedConsumerBroker{DataBrokerClient: sess.Udb.Data.Broker,
		acknowledged: make(chan string, 8), deadLettered: make(chan *entityv1.NackCdcEventsRequest, 2)}
	sess.Udb.Data.Broker = broker
	name := fmt.Sprintf("go-poison-%d", time.Now().UnixNano())
	prefix := name + "/"
	topic := udbclient.Topic(&storageeventsv1.FileUploaded{})
	emit := func(file string) {
		t.Helper()
		if err := sess.Udb.Tx(ctx, func(tx *udbclient.TxScope) error {
			return tx.Emit(&storageeventsv1.FileUploaded{FileId: prefix + file, TenantId: sess.Udb.Meta.TenantID, ObjectKey: "consumer/" + prefix + file})
		}); err != nil {
			t.Fatalf("emit %s: %v", file, err)
		}
	}
	running, stop := context.WithCancel(ctx)
	defer stop()
	handled := make(chan struct{ file, id string }, 4)
	finished := make(chan error, 1)
	poisonCalls := 0
	go func() {
		finished <- udbclient.Consume(running, sess.Udb, name, topic,
			func(_ context.Context, event udbclient.ConsumedEvent[*storageeventsv1.FileUploaded]) error {
				file := event.Payload.GetFileId()
				if !strings.HasPrefix(file, prefix) {
					return nil
				}
				file = strings.TrimPrefix(file, prefix)
				broker.watched.Store(event.ID, true)
				if file == "poison" {
					poisonCalls++
					return errors.New("consumer live fixture refused")
				}
				handled <- struct{ file, id string }{file, event.ID}
				if file == "next" {
					stop()
				}
				return nil
				// Batched acks: the successful prefix is acknowledged as one batch
				// when the poison event is reached, and `next` on shutdown. The
				// batch exceeds every replayed event plus the replay-end marker,
				// and the interval keeps the timer from flushing good-a alone.
			}, udbclient.ConsumeOptions{FromEarliest: true, MaxAttempts: 2, Backoff: time.Millisecond, AckBatchSize: 64, AckInterval: time.Hour})
	}()
	waitHandled := func(file string) string {
		t.Helper()
		select {
		case event := <-handled:
			if event.file != file {
				t.Fatalf("handled %s before %s", event.file, file)
			}
			return event.id
		case err := <-finished:
			t.Fatalf("consumer stopped before %s: %v", file, err)
		case <-ctx.Done():
			t.Fatalf("consumer did not receive %s", file)
		}
		return ""
	}
	emit("good-a")
	first := waitHandled("good-a")
	emit("good-b")
	second := waitHandled("good-b")
	emit("poison")
	var poison *entityv1.NackCdcEventsRequest
	select {
	case poison = <-broker.deadLettered:
	case err := <-finished:
		t.Fatalf("poison stopped consumer: %v", err)
	case <-ctx.Done():
		t.Fatal("poison was not durably dead-lettered")
	}
	if poison.Attempts != 2 || poison.ConsumerName != name || poison.TopicPattern != topic {
		t.Fatalf("wrong poison settlement: %v", poison)
	}
	emit("next")
	next := waitHandled("next")
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown flush failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("consumer did not stop after continuing")
	}
	if poisonCalls != 2 {
		t.Fatalf("handler attempts=%d", poisonCalls)
	}
	var acks []string
	for {
		select {
		case id := <-broker.acknowledged:
			acks = append(acks, id)
		default:
			goto acked
		}
	}
acked:
	if len(acks) != 2 || acks[0] != second || acks[1] != next || first == second {
		t.Fatalf("successful prefix must batch before poison, then flush next on shutdown: %v", acks)
	}
	list, err := sess.Udb.Data.Broker.ListDlqEvents(ctx, &entityv1.DlqListRequest{Topic: topic, StatusFilter: "QUARANTINED", Limit: 100})
	if err != nil {
		t.Fatalf("operator DLQ list: %v", err)
	}
	var dlqID string
	for _, event := range list.Events {
		if event.EventId == poison.EventId {
			dlqID = event.DlqId
		}
	}
	if dlqID == "" {
		t.Fatal("consumer poison absent from ordinary operator DLQ")
	}
	got, err := sess.Udb.Data.Broker.GetDlqEvent(ctx, &entityv1.DlqEventRequest{DlqId: dlqID})
	if err != nil {
		t.Fatalf("operator DLQ get: %v", err)
	}
	var retained struct {
		FailedEvent struct {
			Payload json.RawMessage `json:"payload"`
		} `json:"failed_event"`
		Failures map[string]struct {
			Consumer string `json:"consumer_name"`
			Attempts int    `json:"attempts"`
		} `json:"consumer_failures"`
	}
	if err := json.Unmarshal(got.GetEvent().GetPayloadJson(), &retained); err != nil {
		t.Fatalf("DLQ envelope: %v", err)
	}
	var payload storageeventsv1.FileUploaded
	if err := protojson.Unmarshal(retained.FailedEvent.Payload, &payload); err != nil || payload.GetFileId() != prefix+"poison" {
		t.Fatalf("DLQ did not retain typed poison payload: %v %v", &payload, err)
	}
	found := false
	for _, failure := range retained.Failures {
		if failure.Consumer == name && failure.Attempts == 2 {
			found = true
		}
	}
	if !found {
		t.Fatal("DLQ lost this consumer's retry metadata")
	}
	replay, err := sess.Udb.Data.Broker.ReplayDlqEvent(ctx, &entityv1.DlqActionRequest{DlqId: dlqID})
	if err != nil || replay.GetMutationId() == "" || replay.GetMutationId() == poison.EventId {
		t.Fatalf("operator replay: %v %v", replay, err)
	}
}

// The original stream really reaches the expiry of its server-verified bearer;
// changing a fake clock or injecting UNAUTHENTICATED would not prove this path.
func TestLiveConsumerReauthenticatesAfterBearerExpiry(t *testing.T) {
	if os.Getenv("UDB_LIVE_CDC_EXPIRY_TESTS") != "1" {
		t.Skip("requires the CI short-bearer live lane")
	}
	if os.Getenv("UDB_JWT_ACCESS_TTL_SECONDS") != "20" {
		t.Fatal("expiry proof requires the CI-configured 20 second bearer")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sess := connectLive(t, ctx)
	defer sess.Close()
	broker := &observedConsumerBroker{DataBrokerClient: sess.Udb.Data.Broker, acknowledged: make(chan string, 4)}
	sess.Udb.Data.Broker = broker
	name := fmt.Sprintf("go-expiry-%d", time.Now().UnixNano())
	prefix := name + "/"
	topic := udbclient.Topic(&storageeventsv1.FileUploaded{})
	expiry := make(chan struct{}, 1)
	counts := map[string]int{}
	running, stop := context.WithCancel(ctx)
	defer stop()
	finished := make(chan error, 1)
	go func() {
		finished <- udbclient.Consume(running, sess.Udb, name, topic,
			func(_ context.Context, event udbclient.ConsumedEvent[*storageeventsv1.FileUploaded]) error {
				file := event.Payload.GetFileId()
				if !strings.HasPrefix(file, prefix) {
					return nil
				}
				counts[file]++
				broker.watched.Store(event.ID, true)
				return nil
			}, udbclient.ConsumeOptions{FromEarliest: true, Backoff: 10 * time.Millisecond,
				OnError: func(err error) {
					if status.Code(err) == codes.Unauthenticated && strings.Contains(err.Error(), "expired") {
						select {
						case expiry <- struct{}{}:
						default:
						}
					}
				}})
	}()
	emit := func(file string) {
		t.Helper()
		if err := sess.Udb.Tx(ctx, func(tx *udbclient.TxScope) error {
			return tx.Emit(&storageeventsv1.FileUploaded{FileId: prefix + file, TenantId: sess.Udb.Meta.TenantID, ObjectKey: "consumer/" + prefix + file})
		}); err != nil {
			t.Fatalf("emit %s: %v", file, err)
		}
	}
	waitAck := func(file string) string {
		t.Helper()
		select {
		case id := <-broker.acknowledged:
			return id
		case err := <-finished:
			t.Fatalf("consumer stopped before %s: %v", file, err)
		case <-ctx.Done():
			t.Fatalf("consumer did not acknowledge %s", file)
		}
		return ""
	}
	emit("before")
	before := waitAck("before")
	select {
	case <-expiry:
	case err := <-finished:
		t.Fatalf("consumer stopped at credential expiry: %v", err)
	case <-ctx.Done():
		t.Fatal("original stream never reported actual bearer expiry")
	}
	// The same Consume call must reconnect using the session's refreshed
	// bearer and stored cursor, then settle the next event exactly once.
	emit("after")
	after := waitAck("after")
	stop()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("consumer stop: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("consumer did not stop")
	}
	if before == after || counts[prefix+"before"] != 1 || counts[prefix+"after"] != 1 {
		t.Fatalf("expiry lost or repeated event handling: before=%s after=%s counts=%v", before, after, counts)
	}
}
