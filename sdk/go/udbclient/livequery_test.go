package udbclient

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	livequeryv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/livequery/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type admissionLimitedLiveQueryServer struct {
	livequeryv1.UnimplementedLiveQueryServiceServer
	calls   atomic.Int32
	trailer metadata.MD
}

func (s *admissionLimitedLiveQueryServer) Subscribe(_ *livequeryv1.SubscribeRequest, stream livequeryv1.LiveQueryService_SubscribeServer) error {
	s.calls.Add(1)
	stream.SetTrailer(s.trailer)
	return status.Error(codes.ResourceExhausted, "connection admission window")
}

// Exercise the real public reconnect loop and served binary trailer. A short
// local backoff must not send another RPC before the broker's retry window.
func TestLiveQueryReconnectHonorsServedRetryAfter(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	served := &admissionLimitedLiveQueryServer{trailer: detailTrailer(t, &entityv1.ErrorDetail{
		Backend: "channel", Operation: "read_fair_admission",
		Kind: entityv1.ErrorKind_ERROR_KIND_QUOTA, Retryable: true, RetryAfterMs: 2000,
	})}
	livequeryv1.RegisterLiveQueryServiceServer(server, served)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///live-query-retry",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	notified := make(chan struct{})
	var first atomic.Bool
	finished := make(chan error, 1)
	go func() {
		finished <- LiveQuery[*structpb.Struct](ctx, &Udb{authConn: conn}, LiveQueryOptions{
			Backoff: time.Millisecond,
			OnError: func(err error) {
				if first.CompareAndSwap(false, true) {
					if got := Inspect(err).RetryAfter; got != 2*time.Second {
						t.Errorf("served retry delay = %s, want 2s", got)
					}
					close(notified)
				}
			},
		}, func(context.Context, LiveQueryEvent[*structpb.Struct]) error {
			t.Error("an admission refusal cannot deliver rows")
			return nil
		})
	}()
	select {
	case <-notified:
	case <-time.After(5 * time.Second):
		t.Fatal("no served admission refusal reached the client")
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("watch returned %v, want context cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not stop after cancellation")
	}
	if calls := served.calls.Load(); calls != 1 {
		t.Fatalf("sent %d subscriptions inside the broker's retry window, want 1", calls)
	}
}

// scriptedLiveStream replays frames, then ends with err and trailer.
type scriptedLiveStream struct {
	grpc.ClientStream
	frames  []*livequeryv1.SubscribeResponse
	err     error
	trailer metadata.MD
}

func (s *scriptedLiveStream) Recv() (*livequeryv1.SubscribeResponse, error) {
	if len(s.frames) == 0 {
		return nil, s.err
	}
	frame := s.frames[0]
	s.frames = s.frames[1:]
	return frame, nil
}

func (s *scriptedLiveStream) Trailer() metadata.MD { return s.trailer }

type scriptedLiveClient struct {
	livequeryv1.LiveQueryServiceClient
	streams  []*scriptedLiveStream
	requests []*livequeryv1.SubscribeRequest
	headers  []string
}

func (c *scriptedLiveClient) Subscribe(ctx context.Context, in *livequeryv1.SubscribeRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[livequeryv1.SubscribeResponse], error) {
	c.requests = append(c.requests, proto.Clone(in).(*livequeryv1.SubscribeRequest))
	md, _ := metadata.FromOutgoingContext(ctx)
	c.headers = append(c.headers, firstMD(md, liveQueryResumeHeader))
	stream := c.streams[0]
	c.streams = c.streams[1:]
	return stream, nil
}

func firstMD(md metadata.MD, key string) string {
	if values := md.Get(key); len(values) > 0 {
		return values[0]
	}
	return ""
}

func snapshotFrame(rows ...string) *livequeryv1.SubscribeResponse {
	return &livequeryv1.SubscribeResponse{Payload: &livequeryv1.SubscribeResponse_Snapshot{
		Snapshot: &livequeryv1.LiveQuerySnapshot{RowsJson: rows, RowCount: int64(len(rows))},
	}}
}

func changeFrame(op livequeryv1.LiveQueryChangeOp, row, eventID string) *livequeryv1.SubscribeResponse {
	return &livequeryv1.SubscribeResponse{Payload: &livequeryv1.SubscribeResponse_Change{
		Change: &livequeryv1.LiveQueryChange{Op: op, RowJson: row, EventId: eventID},
	}}
}

func detailTrailer(t *testing.T, detail *entityv1.ErrorDetail) metadata.MD {
	t.Helper()
	raw, err := proto.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	return metadata.Pairs("udb-error-detail-bin", string(raw))
}

// One subscription delivers the snapshot and changes, skips keepalives, and
// classifies the stream end; the loop is driven through liveQueryOnce so the
// test needs no broker.
func TestLiveQueryOnceDeliversSnapshotAndChangesAndSkipsKeepalives(t *testing.T) {
	insert := livequeryv1.LiveQueryChangeOp_LIVE_QUERY_CHANGE_OP_INSERT
	client := &scriptedLiveClient{streams: []*scriptedLiveStream{{
		frames: []*livequeryv1.SubscribeResponse{
			snapshotFrame(`{}`),
			{Payload: &livequeryv1.SubscribeResponse_Heartbeat{Heartbeat: &livequeryv1.LiveQueryHeartbeat{}}},
			changeFrame(livequeryv1.LiveQueryChangeOp_LIVE_QUERY_CHANGE_OP_UNSPECIFIED, "", ""),
			changeFrame(insert, `{}`, "evt-1"),
			{Payload: &livequeryv1.SubscribeResponse_Heartbeat{Heartbeat: &livequeryv1.LiveQueryHeartbeat{}}},
		},
		err: status.Error(codes.Unavailable, "replica going away"),
	}}}
	var events []LiveQueryEvent[*structpb.Struct]
	last := ""
	progressed, err := liveQueryOnce(context.Background(), client, &livequeryv1.SubscribeRequest{}, false, &last,
		func(_ context.Context, e LiveQueryEvent[*structpb.Struct]) error {
			events = append(events, e)
			return nil
		})
	if !progressed || err == nil {
		t.Fatalf("progressed=%v err=%v", progressed, err)
	}
	var fatal *consumeFatal
	if errors.As(err, &fatal) {
		t.Fatalf("Unavailable must be transient, got fatal %v", err)
	}
	if len(events) != 2 || !events[0].IsSnapshot() || events[1].EventID != "evt-1" || events[1].Op != insert {
		t.Fatalf("unexpected events %+v", events)
	}
	if last != "evt-1" {
		t.Fatalf("resume cursor = %q, want evt-1", last)
	}
}

// A reconnect resumes after the last delivered change, through both the typed
// field and the legacy header, and a broker without a change feed ends the loop
// with ErrNoDeltaFeed.
func TestLiveQueryOnceResumesAndReportsMissingDeltaFeed(t *testing.T) {
	client := &scriptedLiveClient{streams: []*scriptedLiveStream{{
		frames: []*livequeryv1.SubscribeResponse{snapshotFrame()},
		err:    status.Error(codes.FailedPrecondition, "live query deltas require the CDC change feed"),
		trailer: detailTrailer(t, &entityv1.ErrorDetail{
			Kind:               entityv1.ErrorKind_ERROR_KIND_CAPABILITY,
			CapabilityRequired: "cdc_change_feed",
		}),
	}}}
	last := "evt-9"
	request := &livequeryv1.SubscribeRequest{SinceEventId: last}
	_, err := liveQueryOnce(context.Background(), client, request, true, &last,
		func(_ context.Context, e LiveQueryEvent[*structpb.Struct]) error {
			if !e.Resumed {
				t.Errorf("snapshot after a reconnect must be marked Resumed")
			}
			return nil
		})
	if !errors.Is(err, ErrNoDeltaFeed) {
		t.Fatalf("want ErrNoDeltaFeed, got %v", err)
	}
	if got := client.requests[0].GetSinceEventId(); got != "evt-9" {
		t.Fatalf("since_event_id = %q", got)
	}
	if client.headers[0] != "evt-9" {
		t.Fatalf("legacy resume header = %q", client.headers[0])
	}
}

// A handler error stops the loop as fatal; a clean EOF is transient.
func TestLiveQueryOnceHandlerErrorIsFatal(t *testing.T) {
	boom := errors.New("view rejected the row")
	client := &scriptedLiveClient{streams: []*scriptedLiveStream{{
		frames: []*livequeryv1.SubscribeResponse{snapshotFrame()},
		err:    io.EOF,
	}}}
	last := ""
	_, err := liveQueryOnce(context.Background(), client, &livequeryv1.SubscribeRequest{}, false, &last,
		func(context.Context, LiveQueryEvent[*structpb.Struct]) error { return boom })
	var fatal *consumeFatal
	if !errors.As(err, &fatal) || !errors.Is(fatal.err, boom) {
		t.Fatalf("want fatal handler error, got %v", err)
	}
}

// The predicate helpers fill the wire fields the broker validates.
func TestLiveQueryPredicateHelpers(t *testing.T) {
	in := LQIn("status", "open", "held")
	if in.GetOp() != livequeryv1.LiveQueryComparison_LIVE_QUERY_COMPARISON_IN || len(in.GetValues()) != 2 || in.GetValue() != "" {
		t.Fatalf("LQIn = %+v", in)
	}
	if LQIsNull("closed_at").GetOp() != livequeryv1.LiveQueryComparison_LIVE_QUERY_COMPARISON_IS_NULL {
		t.Fatal("LQIsNull op")
	}
	if eq := LQEq("owner", "me"); eq.GetValue() != "me" || len(eq.GetValues()) != 0 {
		t.Fatalf("LQEq = %+v", eq)
	}
}
