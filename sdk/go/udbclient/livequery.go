package udbclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	livequeryv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/livequery/services/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// Live queries.
//
// LiveQuery keeps a typed view of the rows matching a filter: it delivers the
// current rows, then every insert, update and delete as it happens, and
// reconnects on its own, resuming after the last change it delivered. Callers
// used to hand-roll the subscribe loop, the resume header, keepalive detection
// and reconnect backoff for every view.

// ErrNoDeltaFeed reports a broker without a CDC change feed: the snapshot was
// delivered but no change ever will be. It needs UDB_CDC_ENABLED (default on)
// and UDB_KAFKA_BROKERS on the broker; reconnecting cannot fix it.
var ErrNoDeltaFeed = errors.New("udb: live query: the broker has no CDC change feed (set UDB_KAFKA_BROKERS and leave UDB_CDC_ENABLED on)")

// LiveQueryEvent is one delivery to a LiveQuery handler: either a snapshot of
// every matching row (on the first connect and after each reconnect) or one
// change.
type LiveQueryEvent[T proto.Message] struct {
	// Snapshot is set on snapshot deliveries: the rows matching now.
	Snapshot []T
	// Resumed is true for a snapshot that follows a reconnect. Changes missed
	// while disconnected are replayed after it, so a view that applies changes
	// by key converges either way.
	Resumed bool
	// Op is the change kind (INSERT, UPDATE, DELETE); UNSPECIFIED on snapshots.
	Op livequeryv1.LiveQueryChangeOp
	// Row is the changed row on change deliveries.
	Row T
	// EventID identifies the change; LiveQuery resumes after the last one.
	EventID string
}

// IsSnapshot reports whether the event is a snapshot delivery.
func (e LiveQueryEvent[T]) IsSnapshot() bool {
	return e.EventID == "" && e.Op == livequeryv1.LiveQueryChangeOp_LIVE_QUERY_CHANGE_OP_UNSPECIFIED
}

// LiveQueryOptions describe the view. The zero value watches every row of the
// caller's tenant.
type LiveQueryOptions struct {
	// Where predicates are AND-ed together (see LQEq, LQIn, LQIsNull, ...).
	Where []*livequeryv1.LiveQueryPredicate
	// AnyOf groups are each an OR of predicates, AND-ed with Where and with
	// each other: status IN (open, held) AND (owner = me OR assignee = me).
	AnyOf [][]*livequeryv1.LiveQueryPredicate
	// SnapshotLimit caps the snapshot rows (the broker clamps it).
	SnapshotLimit int32
	// SinceEventID resumes after a change delivered by an earlier process.
	SinceEventID string
	// Backoff is the first reconnect delay, doubled up to 30s (default 500ms).
	Backoff time.Duration
	// OnError observes reconnects (optional).
	OnError func(err error)
}

// LQEq and the helpers below build live-query predicates. Values are strings
// on the wire and compare numerically when both sides are numbers.
func LQEq(field, value string) *livequeryv1.LiveQueryPredicate {
	return lqPredicate(field, livequeryv1.LiveQueryComparison_LIVE_QUERY_COMPARISON_EQ, value)
}

// LQNe matches rows whose field differs from value.
func LQNe(field, value string) *livequeryv1.LiveQueryPredicate {
	return lqPredicate(field, livequeryv1.LiveQueryComparison_LIVE_QUERY_COMPARISON_NE, value)
}

// LQLt matches rows whose field is below value.
func LQLt(field, value string) *livequeryv1.LiveQueryPredicate {
	return lqPredicate(field, livequeryv1.LiveQueryComparison_LIVE_QUERY_COMPARISON_LT, value)
}

// LQLe matches rows whose field is at most value.
func LQLe(field, value string) *livequeryv1.LiveQueryPredicate {
	return lqPredicate(field, livequeryv1.LiveQueryComparison_LIVE_QUERY_COMPARISON_LE, value)
}

// LQGt matches rows whose field is above value.
func LQGt(field, value string) *livequeryv1.LiveQueryPredicate {
	return lqPredicate(field, livequeryv1.LiveQueryComparison_LIVE_QUERY_COMPARISON_GT, value)
}

// LQGe matches rows whose field is at least value.
func LQGe(field, value string) *livequeryv1.LiveQueryPredicate {
	return lqPredicate(field, livequeryv1.LiveQueryComparison_LIVE_QUERY_COMPARISON_GE, value)
}

// LQIn matches rows whose field equals one of values.
func LQIn(field string, values ...string) *livequeryv1.LiveQueryPredicate {
	p := lqPredicate(field, livequeryv1.LiveQueryComparison_LIVE_QUERY_COMPARISON_IN, "")
	p.Values = values
	return p
}

// LQNotIn matches rows whose field is set and equals none of values.
func LQNotIn(field string, values ...string) *livequeryv1.LiveQueryPredicate {
	p := lqPredicate(field, livequeryv1.LiveQueryComparison_LIVE_QUERY_COMPARISON_NOT_IN, "")
	p.Values = values
	return p
}

// LQIsNull matches rows whose field is null or absent.
func LQIsNull(field string) *livequeryv1.LiveQueryPredicate {
	return lqPredicate(field, livequeryv1.LiveQueryComparison_LIVE_QUERY_COMPARISON_IS_NULL, "")
}

// LQIsNotNull matches rows whose field is set.
func LQIsNotNull(field string) *livequeryv1.LiveQueryPredicate {
	return lqPredicate(field, livequeryv1.LiveQueryComparison_LIVE_QUERY_COMPARISON_IS_NOT_NULL, "")
}

func lqPredicate(field string, op livequeryv1.LiveQueryComparison, value string) *livequeryv1.LiveQueryPredicate {
	return &livequeryv1.LiveQueryPredicate{Field: field, Op: op, Value: value}
}

// LiveQuery watches the rows of T matching opts in the caller's tenant (and
// project) and calls handle for the snapshot and for every change, until ctx
// ends, handle returns an error, or the broker refuses the view. Keepalive
// frames are absorbed. Transient failures reconnect with backoff and resume
// after the last delivered change. A broker without a change feed returns
// ErrNoDeltaFeed after delivering the snapshot.
func LiveQuery[T proto.Message](ctx context.Context, u *Udb, opts LiveQueryOptions, handle func(context.Context, LiveQueryEvent[T]) error) error {
	var zero T
	messageType := MessageType(zero.ProtoReflect().Type().New().Interface())
	client := livequeryv1.NewLiveQueryServiceClient(u.authConn)
	if opts.Backoff <= 0 {
		opts.Backoff = 500 * time.Millisecond
	}
	request := &livequeryv1.SubscribeRequest{
		TenantId:      u.Meta.TenantID,
		ProjectId:     u.Meta.ProjectID,
		MessageType:   messageType,
		Filters:       opts.Where,
		SnapshotLimit: opts.SnapshotLimit,
	}
	for _, group := range opts.AnyOf {
		request.AnyOf = append(request.AnyOf, &livequeryv1.LiveQueryAnyOf{Predicates: group})
	}
	lastEventID := opts.SinceEventID
	delay := opts.Backoff
	connects := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		request.SinceEventId = lastEventID
		progressed, err := liveQueryOnce(ctx, client, request, connects > 0, &lastEventID, handle)
		connects++
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var fatal *consumeFatal
		if errors.As(err, &fatal) {
			return fatal.err
		}
		if progressed {
			delay = opts.Backoff
		}
		if opts.OnError != nil {
			opts.OnError(fmt.Errorf("udb: live query %s reconnecting: %w", messageType, err))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, 30*time.Second)
	}
}

// liveQueryOnce runs one subscription. progressed reports whether anything was
// delivered, which resets the reconnect backoff.
func liveQueryOnce[T proto.Message](ctx context.Context, client livequeryv1.LiveQueryServiceClient, request *livequeryv1.SubscribeRequest, resumed bool, lastEventID *string, handle func(context.Context, LiveQueryEvent[T]) error) (bool, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if request.GetSinceEventId() != "" {
		// Brokers before 0.5.29 read the resume point from this header only.
		ctx = metadata.AppendToOutgoingContext(ctx, liveQueryResumeHeader, request.GetSinceEventId())
	}
	stream, err := client.Subscribe(ctx, request)
	if err != nil {
		return false, liveQueryStreamError(mapError(liveQuerySubscribePath, err, nil))
	}
	progressed := false
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return progressed, errors.New("the live query stream ended")
		}
		if err != nil {
			return progressed, liveQueryStreamError(mapError(liveQuerySubscribePath, err, stream.Trailer()))
		}
		switch payload := frame.GetPayload().(type) {
		case *livequeryv1.SubscribeResponse_Heartbeat:
			continue // Liveness only: no callback or resume cursor change.
		case *livequeryv1.SubscribeResponse_Snapshot:
			rows := make([]T, 0, len(payload.Snapshot.GetRowsJson()))
			for _, raw := range payload.Snapshot.GetRowsJson() {
				row, err := decodeLiveRow[T](raw)
				if err != nil {
					return progressed, &consumeFatal{err: err}
				}
				rows = append(rows, row)
			}
			if err := handle(ctx, LiveQueryEvent[T]{Snapshot: rows, Resumed: resumed}); err != nil {
				return progressed, &consumeFatal{err: err}
			}
			progressed = true
		case *livequeryv1.SubscribeResponse_Change:
			change := payload.Change
			if change.GetEventId() == "" && change.GetOp() == livequeryv1.LiveQueryChangeOp_LIVE_QUERY_CHANGE_OP_UNSPECIFIED {
				continue // Legacy keepalive from brokers before explicit Heartbeat.
			}
			row, err := decodeLiveRow[T](change.GetRowJson())
			if err != nil {
				return progressed, &consumeFatal{err: err}
			}
			if err := handle(ctx, LiveQueryEvent[T]{Op: change.GetOp(), Row: row, EventID: change.GetEventId()}); err != nil {
				return progressed, &consumeFatal{err: err}
			}
			if change.GetEventId() != "" {
				*lastEventID = change.GetEventId()
			}
			progressed = true
		}
	}
}

// liveQueryStreamError separates refusals reconnecting cannot fix from
// transient failures.
func liveQueryStreamError(err error) error {
	switch Inspect(err).GRPC {
	case codes.FailedPrecondition:
		if e, ok := AsError(err); ok {
			if detail, ok := e.Detail(); ok && detail.GetCapabilityRequired() == "cdc_change_feed" {
				return &consumeFatal{err: fmt.Errorf("%w: %w", ErrNoDeltaFeed, err)}
			}
		}
		return &consumeFatal{err: err}
	case codes.InvalidArgument, codes.PermissionDenied, codes.NotFound, codes.Unimplemented:
		return &consumeFatal{err: err}
	}
	return err
}

func decodeLiveRow[T proto.Message](raw string) (T, error) {
	var zero T
	row := zero.ProtoReflect().Type().New().Interface().(T)
	var record Record
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return zero, fmt.Errorf("udb: live query row is not a JSON object: %w", err)
	}
	if err := DecodeRecord(record, row); err != nil {
		return zero, err
	}
	return row, nil
}

// liveQueryResumeHeader is the legacy resume header older brokers read; the
// typed SinceEventId field supersedes it (both carry the same value).
const liveQueryResumeHeader = "x-udb-livequery-resume"

const liveQuerySubscribePath = "/udb.core.livequery.services.v1.LiveQueryService/Subscribe"
