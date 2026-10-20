package udbclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	eventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/events/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	// AckBatchSize acknowledges this many successful events together
	// (default 1, maximum 1024).
	//
	// The default of 1 preserves the contract documented on Consume: an event is
	// acknowledged before the next one is read, a failed acknowledgement is
	// retried against its redelivery, and a failed acknowledgement stops the
	// stream before later events can advance the cursor.
	//
	// Raising it trades those for throughput. The batch is acknowledged as a
	// single watermark (the newest event id in it), so events handled inside an
	// unflushed batch are read and handled before that watermark settles; a
	// refusal or a crash in between redelivers the whole batch, which is
	// retained and re-settled before the next subscription opens. Handlers must
	// be idempotent, which at-least-once delivery already requires.
	AckBatchSize int
	// AckInterval flushes a partial successful batch even on an idle stream
	// (default 1s). Only meaningful when AckBatchSize is greater than 1.
	AckInterval time.Duration
	// AckTimeout bounds each settlement, including a shutdown flush
	// (default 5s, maximum 30s).
	AckTimeout time.Duration
	// FromEarliest makes a consumer name that has never subscribed replay the
	// retained journal history. By default a new name starts at the journal
	// head and receives only events committed after its first subscription.
	// Once the name has a stored position this has no effect.
	FromEarliest bool
	// OnError observes handler failures and reconnects (optional).
	OnError func(err error)
}

// maxConsumerAttempts bounds how often one event is retried in memory.
const maxConsumerAttempts = 1000

// maxConsumerAckBatch bounds one acknowledgement watermark.
const maxConsumerAckBatch = 1024

// normalizeConsumeOptions fills the zero value and clamps every bound.
func normalizeConsumeOptions(o ConsumeOptions) ConsumeOptions {
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 5
	}
	o.MaxAttempts = min(o.MaxAttempts, maxConsumerAttempts)
	if o.Backoff <= 0 {
		o.Backoff = 500 * time.Millisecond
	}
	o.Backoff = min(o.Backoff, 30*time.Second)
	if o.AckBatchSize <= 0 {
		o.AckBatchSize = 1
	}
	o.AckBatchSize = min(o.AckBatchSize, maxConsumerAckBatch)
	if o.AckInterval <= 0 {
		o.AckInterval = time.Second
	}
	if o.AckTimeout <= 0 {
		o.AckTimeout = settlementTimeout
	}
	o.AckTimeout = min(o.AckTimeout, 30*time.Second)
	return o
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
	o = normalizeConsumeOptions(o)
	progress := newRecentIDs(4096)
	// finish flushes a retained batch before returning, so an event whose
	// handler completed just before shutdown is still acknowledged and the next
	// run does not hand the caller the same event again. A settlement the broker
	// definitively refused is not reissued: the batch stays retained for a later
	// run, and retrying here could only repeat a call that cannot succeed.
	finish := func(cause error) error {
		var refused *settlementRefused
		if errors.As(cause, &refused) {
			return cause
		}
		return errors.Join(cause, progress.settle(ctx, u, name, topicPattern, o))
	}
	reconnectDelay := o.Backoff
	for {
		if err := ctx.Err(); err != nil {
			return finish(err)
		}
		before := progress.settlements
		err := consumeOnce(ctx, u, name, topicPattern, handle, o, progress)
		if ctx.Err() != nil {
			// Join the attempt error with the cancellation so a settlement that
			// failed during shutdown is still reported: the caller can see both
			// that it was cancelled and that a flush timed out or was refused.
			if err != nil {
				return finish(errors.Join(ctx.Err(), err))
			}
			return finish(ctx.Err())
		}
		if err == nil {
			return finish(nil)
		}
		var fatal *consumeFatal
		if errors.As(err, &fatal) {
			return finish(fatal.err)
		}
		// Ordinary credential expiry and progress on a healthy stream must not
		// accumulate an ever-longer outage across a routine reauthentication.
		if progress.settlements != before || status.Code(err) == codes.Unauthenticated {
			reconnectDelay = o.Backoff
		}
		if o.OnError != nil {
			o.OnError(fmt.Errorf("udb: consumer %s reconnecting: %w", name, err))
		}
		timer := time.NewTimer(reconnectDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return finish(ctx.Err())
		case <-timer.C:
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

// consumerReceive hands one stream read to the event loop so the loop can also
// flush a partial batch on its ticker while Recv is blocked.
type consumerReceive struct {
	envelope *eventsv1.CDCEnvelope
	err      error
}

func consumeOnce[T proto.Message](ctx context.Context, u *Udb, name, topicPattern string, handle func(context.Context, ConsumedEvent[T]) error, o ConsumeOptions, progress *recentIDs) error {
	o = normalizeConsumeOptions(o)
	// Only a batched consumer can carry an unflushed watermark across attempts;
	// at the default AckBatchSize of 1 every event is acknowledged inline, so the
	// only thing retained here is a dead-letter the broker has not confirmed.
	// Settling first retries that confirmation before the stream reopens.
	batched := o.AckBatchSize > 1
	if err := progress.settle(ctx, u, name, topicPattern, o); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := entityv1.CdcConsumerStart_CDC_CONSUMER_START_LATEST
	if o.FromEarliest {
		start = entityv1.CdcConsumerStart_CDC_CONSUMER_START_EARLIEST
	}
	stream, err := u.Data.Broker.PublishCDC(attemptCtx, &entityv1.CDCSubscriptionRequest{
		TopicPattern: topicPattern,
		ConsumerName: name,
		// Progress markers let a consumer of a quiet topic move its stored
		// position past the rest of the journal; they never reach handle.
		ProgressMarkers: true,
		Start:           start,
	})
	if err != nil {
		return consumeAttemptError(fmt.Errorf("udb: consumer %s: %w", name, err))
	}
	received := make(chan consumerReceive)
	go func() {
		for {
			envelope, err := stream.Recv()
			select {
			case received <- consumerReceive{envelope: envelope, err: err}:
			case <-attemptCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	ticker := time.NewTicker(o.AckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if !batched {
				// AckBatchSize 1 settles inline, so there is never a partial batch
				// to flush on an idle stream.
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := progress.settle(ctx, u, name, topicPattern, o); err != nil {
				return err
			}
		case next := <-received:
			if next.err != nil {
				if batched {
					// Settle the retained watermark before reporting the stream
					// error, so a reconnect does not replay acknowledged events.
					if err := progress.settle(ctx, u, name, topicPattern, o); err != nil {
						return err
					}
				}
				if errors.Is(next.err, io.EOF) {
					return errors.New("the event stream ended")
				}
				return consumeAttemptError(fmt.Errorf("udb: consumer %s: %w", name, next.err))
			}
			// A cancelled consumer never starts another handler, even for an event
			// the stream had already buffered: it stays unacknowledged and is
			// redelivered to the next run.
			if err := ctx.Err(); err != nil {
				return err
			}
			envelope := next.envelope
			id := envelope.GetEventId()
			if progress.pendingHas(id) {
				continue
			}
			if envelope.GetProgressOnly() {
				// A progress marker says the stream read every journal row up to
				// its position. It is not an event, so the handler never sees it.
				// Every earlier event is already settled or queued ahead of it,
				// so its token goes last into the batch and is acknowledged only
				// together with, or after, them.
				if progress.durableHas(id) {
					continue
				}
				progress.enqueue(id)
				if !batched || progress.pendingCount() >= o.AckBatchSize {
					if err := progress.settle(ctx, u, name, topicPattern, o); err != nil {
						return err
					}
				}
				continue
			}
			// A durable id needs nothing at all; a handled-but-not-durable id is
			// acknowledged again below, which is what advances a cursor the broker
			// never saw confirmed.
			if !progress.has(id) && !progress.durableHas(id) {
				event, err := decodeConsumedEvent[T](id, envelope.GetTopic(), envelope.GetPartitionKey(), envelope.GetPayloadJson())
				if err != nil {
					return &consumeFatal{err: fmt.Errorf("udb: consumer %s: event %s: %w", name, id, err)}
				}
				if ts := envelope.GetPublishedAt(); ts != nil {
					event.PublishedAt = ts.AsTime()
				}
				if err := handleWithRetry(attemptCtx, event, handle, o); err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return &consumeFatal{err: fmt.Errorf("udb: consumer %s gave up on event %s: %w", name, id, err)}
					}
					// A poison event is dead-lettered: the broker stores it in its
					// DLQ and advances this consumer in one transaction, so later
					// events keep flowing. A broker that cannot (Unimplemented)
					// stops the consumer instead of discarding the event.
					progress.poison = &consumerPoison{
						id:       id,
						attempts: uint32(o.MaxAttempts),
						reason:   consumerFailureReason(err),
					}
					if err := progress.settle(ctx, u, name, topicPattern, o); err != nil {
						return err
					}
					continue
				}
				// The handler completed even if the ACK response is subsequently
				// lost. Remember its work so a redelivery retries only the
				// acknowledgement and never reruns the handler.
				progress.add(id)
			}
			progress.enqueue(id)
			if !batched || progress.pendingCount() >= o.AckBatchSize {
				if err := progress.settle(ctx, u, name, topicPattern, o); err != nil {
					return err
				}
			}
		}
	}
}

// settlementTimeout bounds one acknowledgement or dead-letter call.
const settlementTimeout = 5 * time.Second

// consumerPoison is a handler failure that exhausted its attempts and is waiting
// for the next settlement to dead-letter it.
type consumerPoison struct {
	id       string
	attempts uint32
	reason   string
}

// settlementRefused marks an error that came out of a settlement call rather
// than the event stream, so Consume can tell "this batch was refused, do not
// reissue it on the way out" from "flush the batch behind this failure". It
// wraps whatever consumeAttemptError produced, so consumeFatal stays reachable
// through Unwrap.
type settlementRefused struct{ err error }

func (s *settlementRefused) Error() string { return s.err.Error() }
func (s *settlementRefused) Unwrap() error { return s.err }

// settle acknowledges the open batch as one watermark and dead-letters a poison
// event. It outlives a cancelled consumer for one bounded call, so a handler that
// completed before shutdown is still acknowledged and a restart does not hand the
// caller the same event again. It never marks an event durable before the broker
// confirms the cursor or the atomic DLQ transaction, so a lost response leaves
// the batch retained and a retry is safe.
func (r *recentIDs) settle(ctx context.Context, u *Udb, name, pattern string, o ConsumeOptions) error {
	if len(r.pending) == 0 && r.poison == nil {
		return nil
	}
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.AckTimeout)
	defer cancel()
	if len(r.pending) != 0 {
		last := r.pending[len(r.pending)-1]
		if _, err := u.Data.Broker.AckCdcEvents(settleCtx, &entityv1.AckCdcEventsRequest{
			ConsumerName: name,
			TopicPattern: pattern,
			EventId:      last,
		}); err != nil {
			if o.OnError != nil {
				o.OnError(fmt.Errorf("udb: consumer %s ack %s: %w", name, last, err))
			}
			if o.AckBatchSize <= 1 {
				// Nothing to carry across the reconnect: the broker never saw this
				// acknowledgement, so it redelivers the event and the already
				// handled path acknowledges it again. Retaining it here instead
				// would acknowledge the same event twice on the next attempt.
				for _, id := range r.pending {
					delete(r.pendingSet, id)
				}
				r.pending = r.pending[:0]
			}
			return &settlementRefused{err: consumeAttemptError(
				fmt.Errorf("udb: consumer %s ack %s: %w", name, last, err))}
		}
		for _, id := range r.pending {
			r.add(id)
			r.durable[id] = struct{}{}
			delete(r.pendingSet, id)
		}
		r.pending = r.pending[:0]
		r.settlements++
	}
	if failed := r.poison; failed != nil {
		if _, err := u.Data.Broker.NackCdcEvents(settleCtx, &entityv1.NackCdcEventsRequest{
			ConsumerName:  name,
			TopicPattern:  pattern,
			EventId:       failed.id,
			Attempts:      failed.attempts,
			FailureReason: failed.reason,
		}); err != nil {
			if o.OnError != nil {
				o.OnError(fmt.Errorf("udb: consumer %s dead-letter %s: %w", name, failed.id, err))
			}
			return &settlementRefused{err: consumeAttemptError(
				fmt.Errorf("udb: consumer %s dead-letter %s: %w", name, failed.id, err))}
		}
		r.add(failed.id)
		r.durable[failed.id] = struct{}{}
		r.poison = nil
		r.settlements++
	}
	return nil
}

// maxConsumerFailureReasonBytes bounds the diagnostic sent with a dead-letter.
const maxConsumerFailureReasonBytes = 2048

// consumerFailureReason is the handler error as valid UTF-8, cut to the bound
// on a rune boundary.
func consumerFailureReason(err error) string {
	reason := strings.ToValidUTF8(err.Error(), "?")
	if len(reason) > maxConsumerFailureReasonBytes {
		reason = reason[:maxConsumerFailureReasonBytes]
		for !utf8.ValidString(reason) {
			reason = reason[:len(reason)-1]
		}
	}
	return reason
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
	version, ok := envelope["envelope_version"].(float64)
	switch {
	case !ok:
		return zero, fmt.Errorf("envelope has no envelope_version; unversioned envelopes are not supported (this SDK reads version %d)", EventEnvelopeVersion)
	case int(version) > EventEnvelopeVersion:
		return zero, fmt.Errorf("envelope_version %d is newer than this SDK understands (%d); upgrade the SDK", int(version), EventEnvelopeVersion)
	case int(version) != EventEnvelopeVersion:
		return zero, fmt.Errorf("envelope_version %d is not supported; this SDK reads version %d", int(version), EventEnvelopeVersion)
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

// recentIDs tracks three distinct states for an event id, which must not be
// conflated: the handler completed (so a redelivery never runs it again), the
// broker confirmed the cursor or the dead-letter (so the event is durable), and
// the id is inside the currently open acknowledgement batch.
type recentIDs struct {
	order      []string
	set        map[string]struct{}
	durable    map[string]struct{}
	limit      int
	pending    []string
	pendingSet map[string]struct{}
	poison     *consumerPoison
	// settlements counts successful acknowledgements, which lets Consume tell a
	// routine reauthentication from a stream that made no progress.
	settlements uint64
}

func newRecentIDs(limit int) *recentIDs {
	return &recentIDs{
		set:        make(map[string]struct{}, limit),
		durable:    make(map[string]struct{}, limit),
		limit:      limit,
		pendingSet: make(map[string]struct{}),
	}
}

// has reports whether this event's handler already completed in this call.
func (r *recentIDs) has(id string) bool { _, ok := r.set[id]; return ok }

// durableHas reports whether the broker confirmed this event's cursor or its
// dead-letter. A lost acknowledgement leaves it false, which is what makes a
// retry necessary.
func (r *recentIDs) durableHas(id string) bool { _, ok := r.durable[id]; return ok }

func (r *recentIDs) pendingHas(id string) bool { _, ok := r.pendingSet[id]; return ok }

func (r *recentIDs) pendingCount() int { return len(r.pending) }

// enqueue queues id as the newest acknowledgement watermark. A redelivery of an
// already handled event is queued again on purpose: the broker never saw the lost
// acknowledgement, so re-sending it is what advances the cursor.
func (r *recentIDs) enqueue(id string) {
	r.pending = append(r.pending, id)
	r.pendingSet[id] = struct{}{}
}

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
