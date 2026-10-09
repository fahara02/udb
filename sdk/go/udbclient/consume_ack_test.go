package udbclient

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	eventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/events/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	consumeAckFirstEvent = "00000000-0000-4000-8000-000000000001"
	consumeAckLaterEvent = "00000000-0000-4000-8000-000000000002"
)

// Script the transport failure, not Consume's state machine. Both subscriptions
// and acknowledgements cross the generated gRPC clients and SDK interceptors.
type consumeAckBroker struct {
	servicesv1.UnimplementedDataBrokerServer
	mu             sync.Mutex
	subscriptions  int
	ackIDs         []string
	holdFirst      bool
	permanent      codes.Code
	refusalTrailer metadata.MD
	firstCanceled  chan struct{}
	laterAcked     chan struct{}
	finishOnce     sync.Once
}

func newConsumeAckBroker() *consumeAckBroker {
	return &consumeAckBroker{firstCanceled: make(chan struct{}), laterAcked: make(chan struct{})}
}

func (s *consumeAckBroker) PublishCDC(req *entityv1.CDCSubscriptionRequest, stream grpc.ServerStreamingServer[eventsv1.CDCEnvelope]) error {
	if req.GetConsumerName() != "ack-regression" || req.GetTopicPattern() != "fixture.events" {
		return status.Error(codes.InvalidArgument, "consumer subscription changed its identity")
	}
	s.mu.Lock()
	s.subscriptions++
	attempt := s.subscriptions
	s.mu.Unlock()
	if attempt == 1 && (s.holdFirst || s.permanent != codes.OK) {
		defer close(s.firstCanceled)
	}
	if err := stream.SendHeader(metadata.Pairs("x-udb-version", SDKVersion)); err != nil {
		return err
	}
	if attempt > 1 && s.holdFirst {
		// A second attempt must not retain the first subscription's serving permit.
		select {
		case <-s.firstCanceled:
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-time.After(2 * time.Second):
			return status.Error(codes.FailedPrecondition, "previous subscription was not canceled")
		}
	}
	ids := []string{consumeAckFirstEvent}
	if attempt > 1 {
		ids = append(ids, consumeAckFirstEvent, consumeAckLaterEvent)
	} else if s.holdFirst && s.permanent == codes.OK {
		ids = append(ids, consumeAckLaterEvent)
	}
	for _, id := range ids {
		if err := stream.Send(&eventsv1.CDCEnvelope{
			EventId: id, Topic: "fixture.events",
			PayloadJson: `{"envelope_version":1,"event_type":"fixture.event","payload":{"value":"` + id + `"}}`,
		}); err != nil {
			return err
		}
	}
	if attempt == 1 && !s.holdFirst && s.permanent == codes.OK {
		return nil // An independent disconnect redelivers the unacknowledged event.
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *consumeAckBroker) AckCdcEvents(ctx context.Context, req *entityv1.AckCdcEventsRequest) (*entityv1.AckCdcEventsResponse, error) {
	if req.GetConsumerName() != "ack-regression" || req.GetTopicPattern() != "fixture.events" {
		return nil, status.Error(codes.InvalidArgument, "acknowledgement changed its consumer identity")
	}
	if err := grpc.SendHeader(ctx, metadata.Pairs("x-udb-version", SDKVersion)); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.ackIDs = append(s.ackIDs, req.GetEventId())
	firstAck := len(s.ackIDs) == 1
	s.mu.Unlock()
	if s.permanent != codes.OK {
		grpc.SetTrailer(ctx, s.refusalTrailer)
		return nil, status.Error(s.permanent, "consumer acknowledgement refused")
	}
	if firstAck {
		return nil, status.Error(codes.Unavailable, "lost acknowledgement response")
	}
	if req.GetEventId() == consumeAckLaterEvent {
		s.finishOnce.Do(func() { close(s.laterAcked) })
	}
	return &entityv1.AckCdcEventsResponse{
		ConsumerName: req.GetConsumerName(), TopicPattern: req.GetTopicPattern(), EventId: req.GetEventId(),
	}, nil
}

func (s *consumeAckBroker) observations() (int, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subscriptions, append([]string(nil), s.ackIDs...)
}

func connectConsumeAckBroker(t *testing.T, served *consumeAckBroker) *Udb {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	servicesv1.RegisterDataBrokerServer(server, served)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	t.Cleanup(func() { _ = listener.Close() })
	gen := NewGenerated(nil, Options{Retry: RetryConfig{MaxAttempts: 1}})
	dialOptions := append(gen.DialOptions(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	conn, err := grpc.NewClient("passthrough:///consumer-ack-regression", dialOptions...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &Udb{Data: New(conn, Metadata{})}
}

func TestConsumeG6RetriesLostAckWithoutRehandling(t *testing.T) {
	served := newConsumeAckBroker()
	u := connectConsumeAckBroker(t, served)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handled := map[string]int{}
	var mu sync.Mutex
	finished := make(chan error, 1)
	go func() {
		finished <- Consume[*structpb.Struct](ctx, u, "ack-regression", "fixture.events", func(_ context.Context, event ConsumedEvent[*structpb.Struct]) error {
			mu.Lock()
			handled[event.ID]++
			mu.Unlock()
			return nil
		}, ConsumeOptions{Backoff: time.Millisecond})
	}()
	select {
	case <-served.laterAcked:
	case <-ctx.Done():
		t.Fatal("g6: consumer did not acknowledge the event after redelivery")
	}
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume stopped with %v, want cancellation", err)
	}
	_, ackIDs := served.observations()
	want := []string{consumeAckFirstEvent, consumeAckFirstEvent, consumeAckFirstEvent, consumeAckLaterEvent}
	if !reflect.DeepEqual(ackIDs, want) {
		t.Fatalf("g6: every redelivery must retry acknowledgement: got %v, want %v", ackIDs, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if handled[consumeAckFirstEvent] != 1 || handled[consumeAckLaterEvent] != 1 {
		t.Fatalf("g6: acknowledged redeliveries must not rerun completed handlers: %v", handled)
	}
}

func TestConsumeG6AckFailurePreservesCursorOrderAndCancelsStream(t *testing.T) {
	served := newConsumeAckBroker()
	served.holdFirst = true
	u := connectConsumeAckBroker(t, served)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	var laterAttempt int
	go func() {
		finished <- Consume[*structpb.Struct](ctx, u, "ack-regression", "fixture.events", func(_ context.Context, event ConsumedEvent[*structpb.Struct]) error {
			if event.ID == consumeAckLaterEvent {
				laterAttempt, _ = served.observations()
			}
			return nil
		}, ConsumeOptions{Backoff: time.Millisecond})
	}()
	select {
	case <-served.laterAcked:
	case <-ctx.Done():
		t.Fatal("g6: consumer failed to reconnect after the acknowledgement refusal")
	}
	select {
	case <-served.firstCanceled:
	default:
		t.Error("g6: acknowledgement failure must cancel the old stream before caller cancellation")
	}
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume stopped with %v, want cancellation", err)
	}
	if laterAttempt != 2 {
		t.Fatalf("g6: acknowledgement failure must stop reading later events: handled later event on attempt %d", laterAttempt)
	}
	_, ackIDs := served.observations()
	want := []string{consumeAckFirstEvent, consumeAckFirstEvent, consumeAckFirstEvent, consumeAckLaterEvent}
	if !reflect.DeepEqual(ackIDs, want) {
		t.Fatalf("g6: cursor advanced before retrying the failed acknowledgement: %v", ackIDs)
	}
}

func TestConsumeG6PermanentAckRefusalPreservesTypedError(t *testing.T) {
	for _, code := range []codes.Code{codes.InvalidArgument, codes.PermissionDenied, codes.NotFound, codes.FailedPrecondition, codes.OutOfRange, codes.Unimplemented} {
		t.Run(code.String(), func(t *testing.T) {
			served := newConsumeAckBroker()
			served.permanent = code
			if code == codes.PermissionDenied {
				served.refusalTrailer = detailTrailer(t, &entityv1.ErrorDetail{
					Kind: entityv1.ErrorKind_ERROR_KIND_PERMISSION, Reason: "UDB_SCOPE_MISSING",
					Missing: map[string]string{"scope": "events:consume"},
				})
			}
			u := connectConsumeAckBroker(t, served)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			handled := 0
			err := Consume[*structpb.Struct](ctx, u, "ack-regression", "fixture.events", func(context.Context, ConsumedEvent[*structpb.Struct]) error {
				handled++
				return nil
			}, ConsumeOptions{Backoff: time.Millisecond})
			if Inspect(err).GRPC != code {
				t.Fatalf("g6: permanent acknowledgement refusal must return its original status: got %v, want %s", err, code)
			}
			if code == codes.PermissionDenied {
				info := Inspect(err)
				if info.Reason != "UDB_SCOPE_MISSING" || info.Missing["scope"] != "events:consume" {
					t.Fatalf("g6: acknowledgement refusal lost its served binary detail: %+v", info)
				}
			}
			if calls, ids := served.observations(); calls != 1 || len(ids) != 1 || handled != 1 {
				t.Fatalf("g6: permanent acknowledgement refusal must stop without resubscribing: subscriptions=%d acks=%v handled=%d", calls, ids, handled)
			}
			select {
			case <-served.firstCanceled:
			case <-time.After(time.Second):
				t.Fatal("g6: permanent acknowledgement refusal retained its old stream")
			}
		})
	}
}
