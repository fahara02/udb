package udbclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Durable consumers.
//
// Consume reads a topic as a named consumer: the broker stores how far the
// consumer has got (AckCdcEvents) and resumes it there after a restart, a
// reconnect or a credential refresh. Services used to keep their own cursor
// table, CAS-commit it, reconnect by hand and handle expired credentials in the
// loop; one such loop that stopped retrying left every consumer of a service
// idle for eight hours without an error.

// ConsumedEvent is one event delivered to a Consume handler.
type ConsumedEvent[T proto.Message] struct {
	ID            string
	Type          string
	Topic         string
	PartitionKey  string
	CorrelationID string
	PublishedAt   time.Time
	// Payload is the event message decoded from the envelope's payload.
	Payload T
	// Envelope is the full envelope as delivered, for fields Payload lacks.
	Envelope map[string]any
}

// ConsumeOptions tune a consumer. The zero value is sensible.
type ConsumeOptions struct {
	// MaxAttempts is how often a failing handler is retried for one event
	// before Consume returns the error (default 5).
	MaxAttempts int
	// Backoff is the first retry delay, doubled per attempt up to 30s
	// (default 500ms). It also paces reconnects.
	Backoff time.Duration
	// OnError observes handler failures and reconnects (optional).
	OnError func(err error)
}

// Consume delivers every event on topicPattern to handle, as the durable
// consumer name, until ctx ends or handle keeps failing on one event. An event
// is acknowledged after handle returns nil, so a crash redelivers at most the
// event in flight (at-least-once). Recently completed handlers are deduplicated,
// but redelivered events are still acknowledged. A failed acknowledgement stops
// the stream before later events can advance the cursor. Envelopes newer than
// this SDK understands stop the consumer rather than being misread.
func Consume[T proto.Message](ctx context.Context, u *Udb, name, topicPattern string, handle func(context.Context, ConsumedEvent[T]) error, opts ...ConsumeOptions) error {
	var o ConsumeOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 5
	}
	if o.Backoff <= 0 {
		o.Backoff = 500 * time.Millisecond
	}
	seen := newRecentIDs(4096)
	reconnectDelay := o.Backoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := consumeOnce(ctx, u, name, topicPattern, handle, o, seen)
		if err == nil || ctx.Err() != nil {
			return ctx.Err()
		}
		var fatal *consumeFatal
		if errors.As(err, &fatal) {
			return fatal.err
		}
		if o.OnError != nil {
			o.OnError(fmt.Errorf("udb: consumer %s reconnecting: %w", name, err))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reconnectDelay):
		}
		reconnectDelay = min(reconnectDelay*2, 30*time.Second)
	}
}

// consumeFatal marks an error that reconnecting cannot fix.
type consumeFatal struct{ err error }

func (f *consumeFatal) Error() string { return f.err.Error() }

func (f *consumeFatal) Unwrap() error { return f.err }

func consumeAttemptError(err error) error {
	switch Inspect(err).GRPC {
	case codes.InvalidArgument, codes.PermissionDenied, codes.NotFound,
		codes.FailedPrecondition, codes.OutOfRange, codes.Unimplemented:
		// Invalid consumer/cursor, missing authority, and unavailable contracts
		// require caller intervention; reconnecting cannot repair these refusals.
		return &consumeFatal{err: err}
	default:
		return err
	}
}

func consumeOnce[T proto.Message](ctx context.Context, u *Udb, name, topicPattern string, handle func(context.Context, ConsumedEvent[T]) error, o ConsumeOptions, seen *recentIDs) error {
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := u.Data.Broker.PublishCDC(attemptCtx, &entityv1.CDCSubscriptionRequest{
		TopicPattern: topicPattern,
		ConsumerName: name,
	})
	if err != nil {
		return consumeAttemptError(fmt.Errorf("udb: consumer %s: %w", name, err))
	}
	for {
		envelope, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return errors.New("the event stream ended")
		}
		if err != nil {
			return consumeAttemptError(fmt.Errorf("udb: consumer %s: %w", name, err))
		}
		id := envelope.GetEventId()
		if !seen.has(id) {
			event, err := decodeConsumedEvent[T](id, envelope.GetTopic(), envelope.GetPartitionKey(), envelope.GetPayloadJson())
			if err != nil {
				return &consumeFatal{err: fmt.Errorf("udb: consumer %s: event %s: %w", name, id, err)}
			}
			if ts := envelope.GetPublishedAt(); ts != nil {
				event.PublishedAt = ts.AsTime()
			}
			if err := handleWithRetry(attemptCtx, event, handle, o); err != nil {
				return &consumeFatal{err: fmt.Errorf("udb: consumer %s gave up on event %s: %w", name, id, err)}
			}
			// The handler completed even if the ACK response is subsequently lost.
			// Remember its work so redelivery retries only the acknowledgement.
			seen.add(id)
		}
		if _, err := u.Data.Broker.AckCdcEvents(attemptCtx, &entityv1.AckCdcEventsRequest{
			ConsumerName: name,
			TopicPattern: topicPattern,
			EventId:      id,
		}); err != nil {
			return consumeAttemptError(fmt.Errorf("udb: consumer %s ack %s: %w", name, id, err))
		}
	}
}

func handleWithRetry[T proto.Message](ctx context.Context, event ConsumedEvent[T], handle func(context.Context, ConsumedEvent[T]) error, o ConsumeOptions) error {
	delay := o.Backoff
	var err error
	for attempt := 1; attempt <= o.MaxAttempts; attempt++ {
		if err = handle(ctx, event); err == nil {
			return nil
		}
		if o.OnError != nil {
			o.OnError(fmt.Errorf("event %s attempt %d: %w", event.ID, attempt, err))
		}
		if attempt == o.MaxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, 30*time.Second)
	}
	return err
}

// decodeConsumedEvent reads the envelope a stream delivered. The domain event
// is under "payload"; a journal row that wraps the envelope once more is
// unwrapped.
func decodeConsumedEvent[T proto.Message](id, topic, partition, payloadJSON string) (ConsumedEvent[T], error) {
	var zero ConsumedEvent[T]
	var envelope map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &envelope); err != nil {
		return zero, fmt.Errorf("envelope is not JSON: %w", err)
	}
	if inner, ok := envelope["payload"].(map[string]any); ok {
		if _, wrapped := inner["event_type"]; wrapped {
			if _, hasPayload := inner["payload"]; hasPayload {
				envelope = inner
			}
		}
	}
	if version, ok := envelope["envelope_version"].(float64); ok && int(version) > EventEnvelopeVersion {
		return zero, fmt.Errorf("envelope_version %d is newer than this SDK understands (%d); upgrade the SDK", int(version), EventEnvelopeVersion)
	}
	var newT T
	payload := newT.ProtoReflect().Type().New().Interface().(T)
	if domain, ok := envelope["payload"]; ok && domain != nil {
		raw, _ := json.Marshal(domain)
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, payload); err != nil {
			return zero, fmt.Errorf("payload does not decode as %s: %w", MessageType(payload), err)
		}
	}
	str := func(key string) string { s, _ := envelope[key].(string); return s }
	return ConsumedEvent[T]{
		ID:            id,
		Type:          str("event_type"),
		Topic:         topic,
		PartitionKey:  partition,
		CorrelationID: str("correlation_id"),
		Payload:       payload,
		Envelope:      envelope,
	}, nil
}

// recentIDs remembers the last n event ids handled.
type recentIDs struct {
	order []string
	set   map[string]struct{}
	limit int
}

func newRecentIDs(limit int) *recentIDs {
	return &recentIDs{set: make(map[string]struct{}, limit), limit: limit}
}

func (r *recentIDs) has(id string) bool { _, ok := r.set[id]; return ok }

func (r *recentIDs) add(id string) {
	if r.has(id) {
		return
	}
	r.set[id] = struct{}{}
	r.order = append(r.order, id)
	if len(r.order) > r.limit {
		delete(r.set, r.order[0])
		r.order = r.order[1:]
	}
}
