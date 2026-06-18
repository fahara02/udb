package udbclient

import (
	"context"
	"reflect"
	"testing"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	eventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/events/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// fakeCDCStream is a server-streaming client returning queued envelopes; Header()
// resolves Ready() immediately (server-driven readiness, no timer).
type fakeCDCStream struct {
	grpc.ServerStreamingClient[eventsv1.CDCEnvelope]
	queue []*eventsv1.CDCEnvelope
	pos   int
}

func (s *fakeCDCStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *fakeCDCStream) Recv() (*eventsv1.CDCEnvelope, error) {
	if s.pos >= len(s.queue) {
		return nil, context.Canceled
	}
	env := s.queue[s.pos]
	s.pos++
	return env, nil
}

type fakeEventsBroker struct {
	servicesv1.DataBrokerClient
	seq   *[]string
	queue []*eventsv1.CDCEnvelope
}

func (b *fakeEventsBroker) PublishCDC(_ context.Context, _ *entityv1.CDCSubscriptionRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[eventsv1.CDCEnvelope], error) {
	*b.seq = append(*b.seq, "PublishCDC")
	return &fakeCDCStream{queue: b.queue}, nil
}

func (b *fakeEventsBroker) EnqueueOutboxEvent(_ context.Context, _ *entityv1.EnqueueOutboxEventRequest, _ ...grpc.CallOption) (*entityv1.EnqueueOutboxEventResponse, error) {
	*b.seq = append(*b.seq, "EnqueueOutboxEvent")
	return &entityv1.EnqueueOutboxEventResponse{EventId: "evt-1", Enqueued: true}, nil
}

func TestEventsSubscribeReadyAndPublishAndWait(t *testing.T) {
	var seq []string
	broker := &fakeEventsBroker{
		seq:   &seq,
		queue: []*eventsv1.CDCEnvelope{{EventId: "evt-1", Topic: "orders"}},
	}
	f := &EventsFacade{Raw: broker, meta: Metadata{TenantID: "t-1"}}

	sub, err := f.Subscribe(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := sub.Ready(); err != nil { // server header => ready, no timer
		t.Fatalf("Ready: %v", err)
	}
	got, err := f.PublishAndWait(context.Background(), sub, "orders", map[string]any{"id": "1"},
		func(e *eventsv1.CDCEnvelope) bool { return e.GetEventId() == "evt-1" })
	if err != nil {
		t.Fatalf("PublishAndWait: %v", err)
	}
	if got.GetEventId() != "evt-1" {
		t.Fatalf("matched wrong envelope: %q", got.GetEventId())
	}
	if !reflect.DeepEqual(seq, []string{"PublishCDC", "EnqueueOutboxEvent"}) {
		t.Fatalf("events RPC drift (exactly one PublishCDC + one EnqueueOutboxEvent): %v", seq)
	}
}
