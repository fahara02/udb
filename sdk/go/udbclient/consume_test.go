package udbclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	eventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/events/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type consumerTestStream struct {
	grpc.ServerStreamingClient[eventsv1.CDCEnvelope]
	context context.Context
	queue   []*eventsv1.CDCEnvelope
	idle    bool
}

func (s *consumerTestStream) Recv() (*eventsv1.CDCEnvelope, error) {
	if len(s.queue) > 0 {
		envelope := s.queue[0]
		s.queue = s.queue[1:]
		return envelope, nil
	}
	if s.idle {
		<-s.context.Done()
		return nil, s.context.Err()
	}
	return nil, io.EOF
}

type consumerTestBroker struct {
	servicesv1.DataBrokerClient
	queue    []*eventsv1.CDCEnvelope
	idle     bool
	acks     []string
	nacks    []*entityv1.NackCdcEventsRequest
	order    []string
	ack      func(context.Context) error
	nack     func(context.Context) error
	observed chan string
	requests []*entityv1.CDCSubscriptionRequest
}

func (b *consumerTestBroker) PublishCDC(ctx context.Context, req *entityv1.CDCSubscriptionRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[eventsv1.CDCEnvelope], error) {
	b.requests = append(b.requests, req)
	b.order = append(b.order, "subscribe")
	return &consumerTestStream{context: ctx, queue: append([]*eventsv1.CDCEnvelope(nil), b.queue...), idle: b.idle}, nil
}
func (b *consumerTestBroker) AckCdcEvents(ctx context.Context, req *entityv1.AckCdcEventsRequest, _ ...grpc.CallOption) (*entityv1.AckCdcEventsResponse, error) {
	b.acks = append(b.acks, req.EventId)
	b.order = append(b.order, "ack:"+req.EventId)
	if b.ack != nil {
		if err := b.ack(ctx); err != nil {
			return nil, err
		}
	}
	if b.observed != nil {
		b.observed <- req.EventId
	}
	return &entityv1.AckCdcEventsResponse{}, nil
}
func (b *consumerTestBroker) NackCdcEvents(ctx context.Context, req *entityv1.NackCdcEventsRequest, _ ...grpc.CallOption) (*entityv1.AckCdcEventsResponse, error) {
	b.nacks = append(b.nacks, req)
	b.order = append(b.order, "nack:"+req.EventId)
	if b.nack != nil {
		if err := b.nack(ctx); err != nil {
			return nil, err
		}
	}
	return &entityv1.AckCdcEventsResponse{}, nil
}
func consumerFixture(ids ...string) []*eventsv1.CDCEnvelope {
	var queue []*eventsv1.CDCEnvelope
	for _, id := range ids {
		queue = append(queue, &eventsv1.CDCEnvelope{
			EventId: id, Topic: "fixture", PartitionKey: "row",
			PayloadJson: `{"envelope_version":2,"payload":{"value":"ok"}}`,
		})
	}
	return queue
}
func consumerClient(b *consumerTestBroker) *Udb { return &Udb{Data: &Client{Broker: b}} }

// progressMarker is what a broker sends when the stream has read the journal
// up to position without an event for this consumer.
func progressMarker(id string, position int64) *eventsv1.CDCEnvelope {
	return &eventsv1.CDCEnvelope{
		EventId:         id,
		Topic:           "udb.cdc.progress.v1",
		PayloadJson:     fmt.Sprintf(`{"kind":"progress","journal_position":"%d","caught_up":true}`, position),
		ProgressOnly:    true,
		JournalPosition: position,
	}
}

func TestConsumerAcknowledgesProgressMarkersWithoutHandlingThem(t *testing.T) {
	b := &consumerTestBroker{queue: consumerFixture("a", "b")}
	b.queue = []*eventsv1.CDCEnvelope{b.queue[0], progressMarker("m1", 10), b.queue[1], progressMarker("m2", 20)}
	var handled []string
	handler := func(_ context.Context, event ConsumedEvent[*structpb.Struct]) error {
		handled = append(handled, event.ID)
		return nil
	}
	_ = consumeOnce(context.Background(), consumerClient(b), "consumer", "fixture", handler,
		ConsumeOptions{AckInterval: time.Hour}, newRecentIDs(4096))
	if !reflect.DeepEqual(handled, []string{"a", "b"}) {
		t.Fatalf("markers reached the handler: %v", handled)
	}
	if want := []string{"subscribe", "ack:a", "ack:m1", "ack:b", "ack:m2"}; !reflect.DeepEqual(b.order, want) {
		t.Fatalf("a marker must be acknowledged after every earlier event: got %v want %v", b.order, want)
	}
	if req := b.requests[0]; !req.GetProgressMarkers() || req.GetStart() != entityv1.CdcConsumerStart_CDC_CONSUMER_START_LATEST {
		t.Fatalf("consumer subscription options: markers=%v start=%v", req.GetProgressMarkers(), req.GetStart())
	}

	// Batched: the marker is the newest watermark of its batch, never ahead of
	// an event handled before it.
	b = &consumerTestBroker{queue: consumerFixture("a", "b", "c")}
	b.queue = []*eventsv1.CDCEnvelope{b.queue[0], progressMarker("m1", 10), b.queue[1], b.queue[2], progressMarker("m2", 20)}
	handled = nil
	_ = consumeOnce(context.Background(), consumerClient(b), "consumer", "fixture", handler,
		ConsumeOptions{AckBatchSize: 3, AckInterval: time.Hour, FromEarliest: true}, newRecentIDs(4096))
	if !reflect.DeepEqual(handled, []string{"a", "b", "c"}) || !reflect.DeepEqual(b.acks, []string{"b", "m2"}) {
		t.Fatalf("batched markers: handled=%v acks=%v", handled, b.acks)
	}
	if got := b.requests[0].GetStart(); got != entityv1.CdcConsumerStart_CDC_CONSUMER_START_EARLIEST {
		t.Fatalf("FromEarliest must request the earliest start, got %v", got)
	}
}

func TestConsumerRetriesLostAckBeforeReplayWithoutHandlingAgain(t *testing.T) {
	b := &consumerTestBroker{queue: consumerFixture("event")}
	b.ack = func(context.Context) error {
		if len(b.acks) == 1 {
			return status.Error(codes.Unavailable, "lost acknowledgment")
		}
		return nil
	}
	progress := newRecentIDs(4096)
	handled := 0
	handler := func(context.Context, ConsumedEvent[*structpb.Struct]) error { handled++; return nil }
	opts := ConsumeOptions{MaxAttempts: 1, Backoff: time.Millisecond, AckBatchSize: 1}
	err := consumeOnce(context.Background(), consumerClient(b), "consumer", "fixture", handler, opts, progress)
	// The handler completed, so the event is remembered, but the broker never
	// confirmed the cursor: it is not durable, and that is what forces a retry.
	if status.Code(err) != codes.Unavailable || progress.durableHas("event") {
		t.Fatalf("lost ack must leave the event undurable: %v durable=%v", err, progress.durableHas("event"))
	}
	_ = consumeOnce(context.Background(), consumerClient(b), "consumer", "fixture", handler, opts, progress)
	// Two acknowledgements. At AckBatchSize 1 nothing is retained across the
	// reconnect, so the redelivered event is acknowledged again in place: the
	// handler does not run a second time, and re-sending the acknowledgement is
	// what advances the cursor the broker never saw confirmed. (Retention plus a
	// pre-subscription settlement is the batched model; see
	// TestConsumerRetriesLostDeadLetterBeforeHandlingLaterEvents.)
	if handled != 1 || len(b.acks) != 2 || !progress.durableHas("event") {
		t.Fatalf("settlement retry: handled=%d acks=%v durable=%v", handled, b.acks, progress.durableHas("event"))
	}
	if want := []string{"subscribe", "ack:event", "subscribe", "ack:event"}; !reflect.DeepEqual(b.order, want) {
		t.Fatalf("a redelivery must be re-acknowledged without re-handling: got %v want %v", b.order, want)
	}
}

func TestConsumerBatchesContiguousSuccessAndFlushesRemainder(t *testing.T) {
	b := &consumerTestBroker{queue: consumerFixture("a", "b", "b", "c", "d", "e")}
	var handled []string
	progress := newRecentIDs(4096)
	_ = consumeOnce(context.Background(), consumerClient(b), "consumer", "fixture",
		func(_ context.Context, event ConsumedEvent[*structpb.Struct]) error {
			handled = append(handled, event.ID)
			return nil
		},
		ConsumeOptions{AckBatchSize: 3, AckInterval: time.Hour}, progress)
	if !reflect.DeepEqual(handled, []string{"a", "b", "c", "d", "e"}) || !reflect.DeepEqual(b.acks, []string{"c", "e"}) {
		t.Fatalf("batch watermarks or pending duplicate suppression: handled=%v acks=%v", handled, b.acks)
	}
	for _, id := range handled {
		if !progress.has(id) {
			t.Fatalf("event %s not durably settled", id)
		}
	}
}

func TestConsumerFlushesIdlePartialBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	b := &consumerTestBroker{queue: consumerFixture("one"), idle: true, observed: make(chan string, 2)}
	stopped := make(chan error, 1)
	go func() {
		stopped <- Consume(ctx, consumerClient(b), "consumer", "fixture",
			func(context.Context, ConsumedEvent[*structpb.Struct]) error { return nil },
			ConsumeOptions{AckBatchSize: 32, AckInterval: 10 * time.Millisecond})
	}()
	select {
	case id := <-b.observed:
		if id != "one" {
			t.Errorf("idle watermark %s", id)
		}
	case <-ctx.Done():
		t.Fatal("idle stream never flushed its partial batch")
	}
	cancel()
	select {
	case err := <-stopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("stop: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("receiver did not stop with cancellation")
	}
	if len(b.acks) != 1 {
		t.Fatalf("partial batch acknowledged %d times", len(b.acks))
	}
}

func TestConsumerDeadLettersPoisonAfterPrefixAndContinues(t *testing.T) {
	b := &consumerTestBroker{queue: consumerFixture("good", "poison", "next")}
	calls := map[string]int{}
	progress := newRecentIDs(4096)
	_ = consumeOnce(context.Background(), consumerClient(b), "consumer", "fixture",
		func(_ context.Context, event ConsumedEvent[*structpb.Struct]) error {
			calls[event.ID]++
			if event.ID == "poison" {
				return errors.New("invalid business state")
			}
			return nil
		}, ConsumeOptions{MaxAttempts: 2, Backoff: time.Millisecond, AckInterval: time.Hour}, progress)
	if calls["poison"] != 2 || calls["next"] != 1 || len(b.nacks) != 1 {
		t.Fatalf("poison stopped consumer: calls=%v nacks=%v", calls, b.nacks)
	}
	if want := []string{"subscribe", "ack:good", "nack:poison", "ack:next"}; !reflect.DeepEqual(b.order, want) {
		t.Fatalf("settlement crossed an unhandled gap: got %v want %v", b.order, want)
	}
	if req := b.nacks[0]; req.Attempts != 2 || req.FailureReason != "invalid business state" || req.ConsumerName != "consumer" || req.TopicPattern != "fixture" {
		t.Fatalf("dead-letter identity/attempts: %v", req)
	}
	if !progress.has("poison") {
		t.Fatal("confirmed poison settlement not deduplicated")
	}
}

func TestConsumerRetriesLostDeadLetterBeforeHandlingLaterEvents(t *testing.T) {
	b := &consumerTestBroker{queue: consumerFixture("poison", "next")}
	b.nack = func(context.Context) error {
		if len(b.nacks) == 1 {
			return status.Error(codes.Unavailable, "lost DLQ response")
		}
		return nil
	}
	progress := newRecentIDs(4096)
	calls := map[string]int{}
	handler := func(_ context.Context, event ConsumedEvent[*structpb.Struct]) error {
		calls[event.ID]++
		if event.ID == "poison" {
			return errors.New("poison")
		}
		return nil
	}
	opts := ConsumeOptions{MaxAttempts: 1, AckInterval: time.Hour}
	err := consumeOnce(context.Background(), consumerClient(b), "consumer", "fixture", handler, opts, progress)
	if status.Code(err) != codes.Unavailable || progress.has("poison") || calls["next"] != 0 {
		t.Fatalf("unsettled poison allowed progress: %v calls=%v", err, calls)
	}
	_ = consumeOnce(context.Background(), consumerClient(b), "consumer", "fixture", handler, opts, progress)
	if calls["poison"] != 1 || calls["next"] != 1 || len(b.nacks) != 2 || !progress.has("poison") {
		t.Fatalf("lost poison response replay: calls=%v nacks=%d", calls, len(b.nacks))
	}
}

func TestConsumerOldBrokerCannotAcknowledgeAndDiscardPoison(t *testing.T) {
	b := &consumerTestBroker{queue: consumerFixture("poison", "next")}
	b.nack = func(context.Context) error { return status.Error(codes.Unimplemented, "old broker") }
	handled := 0
	err := Consume(context.Background(), consumerClient(b), "consumer", "fixture",
		func(context.Context, ConsumedEvent[*structpb.Struct]) error { handled++; return errors.New("poison") },
		ConsumeOptions{MaxAttempts: 1})
	if status.Code(err) != codes.Unimplemented || handled != 1 || len(b.acks) != 0 {
		t.Fatalf("old broker discarded poison: err=%v handled=%d acks=%v", err, handled, b.acks)
	}
}

func TestConsumerCancellationNeverSettlesInterruptedHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := &consumerTestBroker{queue: consumerFixture("interrupted")}
	err := Consume(ctx, consumerClient(b), "consumer", "fixture",
		func(context.Context, ConsumedEvent[*structpb.Struct]) error { cancel(); return context.Canceled },
		ConsumeOptions{MaxAttempts: 1})
	if !errors.Is(err, context.Canceled) || len(b.acks) != 0 || len(b.nacks) != 0 {
		t.Fatalf("interrupted handler settled: err=%v acks=%v nacks=%v", err, b.acks, b.nacks)
	}
}

func TestConsumerShutdownFlushRetainsValuesAndReportsTimeout(t *testing.T) {
	type contextKey struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "identity"))
	defer cancel()
	b := &consumerTestBroker{queue: consumerFixture("completed"), idle: true}
	b.ack = func(flush context.Context) error {
		if flush.Value(contextKey{}) != "identity" || flush.Err() != nil {
			return fmt.Errorf("shutdown lost context values or retained cancellation")
		}
		<-flush.Done()
		return flush.Err()
	}
	err := Consume(ctx, consumerClient(b), "consumer", "fixture",
		func(context.Context, ConsumedEvent[*structpb.Struct]) error { cancel(); return nil },
		ConsumeOptions{AckInterval: time.Hour, AckTimeout: 20 * time.Millisecond})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded) || len(b.acks) != 1 || len(b.nacks) != 0 {
		t.Fatalf("shutdown silently lost a batch or timeout: err=%v acks=%v nacks=%v", err, b.acks, b.nacks)
	}
}

func TestConsumerUnknownEnvelopeFlushesOnlyEarlierSuccess(t *testing.T) {
	b := &consumerTestBroker{queue: consumerFixture("good", "unknown", "next")}
	b.queue[1].PayloadJson = `{"envelope_version":999,"payload":{}}`
	var handled []string
	err := Consume(context.Background(), consumerClient(b), "consumer", "fixture",
		func(_ context.Context, event ConsumedEvent[*structpb.Struct]) error {
			handled = append(handled, event.ID)
			return nil
		},
		ConsumeOptions{AckInterval: time.Hour})
	if err == nil || !strings.Contains(err.Error(), "newer") || !reflect.DeepEqual(handled, []string{"good"}) || !reflect.DeepEqual(b.acks, []string{"good"}) || len(b.nacks) != 0 {
		t.Fatalf("unknown envelope advanced cursor: err=%v handled=%v acks=%v", err, handled, b.acks)
	}
}

func TestConsumerFailureDiagnosticTruncationPreservesUTF8(t *testing.T) {
	reason := consumerFailureReason(errors.New(strings.Repeat("ক", 1000)))
	if len(reason) > 2048 || !utf8.ValidString(reason) || len(reason) < 2046 {
		t.Fatalf("invalid bounded diagnostic: bytes=%d valid=%v", len(reason), utf8.ValidString(reason))
	}
}
