package udbclient

import (
	"context"
	"testing"
	"time"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ── predicate-level coverage ─────────────────────────────────────────────────

func TestRetryConfigReadOnlyTransientRetriesUnchanged(t *testing.T) {
	rc := DefaultRetryConfig()

	// Read-only RPCs retry on the configured transient codes plus DEADLINE.
	for _, code := range []codes.Code{codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded} {
		if !rc.retryableForRPC(code, true /*readOnly*/, false, false) {
			t.Fatalf("retryableForRPC(%s, readOnly) = false, want true", code)
		}
	}
	// Non-transient codes are never retried even for reads.
	if rc.retryableForRPC(codes.InvalidArgument, true, false, false) {
		t.Fatal("retryableForRPC(InvalidArgument, readOnly) = true, want false")
	}
}

func TestRetryConfigReplaySafeMutationRequiresKey(t *testing.T) {
	rc := DefaultRetryConfig()

	// Replay-safe mutation WITH an idempotency key retries on a transient code.
	if !rc.retryableForRPC(codes.Unavailable, false, true /*replaySafe*/, true /*hasKey*/) {
		t.Fatal("replay-safe mutation with key on Unavailable = false, want true")
	}
	// Same mutation WITHOUT a key does not retry.
	if rc.retryableForRPC(codes.Unavailable, false, true, false) {
		t.Fatal("replay-safe mutation without key = true, want false")
	}
	// Non-replay-safe mutation never retries, even with a key.
	if rc.retryableForRPC(codes.Unavailable, false, false, true) {
		t.Fatal("non-replay-safe mutation with key = true, want false")
	}
	// Replay-safe mutation does NOT retry on DEADLINE_EXCEEDED (write may have
	// landed server-side; fail-closed).
	if rc.retryableForRPC(codes.DeadlineExceeded, false, true, true) {
		t.Fatal("replay-safe mutation on DeadlineExceeded = true, want false")
	}
}

func TestHasIdempotencyKey(t *testing.T) {
	if hasIdempotencyKey(&entityv1.UpsertRequest{IdempotencyKey: "k1"}) != true {
		t.Fatal("UpsertRequest with key: hasIdempotencyKey = false, want true")
	}
	if hasIdempotencyKey(&entityv1.UpsertRequest{}) != false {
		t.Fatal("UpsertRequest without key: hasIdempotencyKey = true, want false")
	}
	// A request type with no idempotency_key field never satisfies the gate.
	if hasIdempotencyKey(&entityv1.SelectRequest{}) != false {
		t.Fatal("SelectRequest (no key field): hasIdempotencyKey = true, want false")
	}
}

// ── full retry-path coverage via a fake connection ───────────────────────────

// fakeConn is a grpc.ClientConnInterface whose Invoke returns a scripted
// sequence of errors (one per attempt). It records how many attempts occurred.
type fakeConn struct {
	results  []error // returned in order; the last entry repeats if exhausted
	attempts int
}

func (f *fakeConn) Invoke(_ context.Context, _ string, _, _ any, _ ...grpc.CallOption) error {
	idx := f.attempts
	f.attempts++
	if idx >= len(f.results) {
		idx = len(f.results) - 1
	}
	return f.results[idx]
}

func (f *fakeConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Error(codes.Unimplemented, "fakeConn has no streams")
}

// fastRetry keeps the backoff negligible so tests stay quick but still exercise
// the real retry loop.
func fastRetry() RetryConfig {
	rc := DefaultRetryConfig()
	rc.BaseBackoff = time.Millisecond
	rc.MaxBackoff = time.Millisecond
	rc.Jitter = 0
	return rc
}

func newTestClient(conn grpc.ClientConnInterface) *GeneratedClient {
	return NewGenerated(conn, Options{Retry: fastRetry()})
}

const (
	mUpsert       = "/udb.services.v1.DataBroker/Upsert"       // mutation, ReplaySafe=true
	mDeletePolicy = "/udb.services.v1.DataBroker/DeletePolicy" // mutation, ReplaySafe=false
	mSelect       = "/udb.services.v1.DataBroker/Select"       // read_only
)

// (1) Replay-safe mutation + idempotency key retries on a transient code, then
// succeeds.
func TestRetryReplaySafeMutationWithKeyRetriesThenSucceeds(t *testing.T) {
	conn := &fakeConn{results: []error{
		status.Error(codes.Unavailable, "transient"),
		nil,
	}}
	g := newTestClient(conn)

	req := &entityv1.UpsertRequest{IdempotencyKey: "idem-1"}
	if err := g.InvokeUnary(context.Background(), mUpsert, req, &entityv1.MutationResponse{}); err != nil {
		t.Fatalf("InvokeUnary(Upsert, key) = %v, want nil after retry", err)
	}
	if conn.attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (one retry then success)", conn.attempts)
	}
}

// (2) Replay-safe mutation WITHOUT an idempotency key does NOT retry.
func TestRetryReplaySafeMutationWithoutKeyDoesNotRetry(t *testing.T) {
	conn := &fakeConn{results: []error{
		status.Error(codes.Unavailable, "transient"),
		nil, // would succeed if it retried — it must not
	}}
	g := newTestClient(conn)

	req := &entityv1.UpsertRequest{} // no idempotency key
	err := g.InvokeUnary(context.Background(), mUpsert, req, &entityv1.MutationResponse{})
	if err == nil {
		t.Fatal("InvokeUnary(Upsert, no key) = nil, want error (no retry allowed)")
	}
	if conn.attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (no retry without key)", conn.attempts)
	}
}

// (3) Non-replay-safe mutation does not retry, even with an idempotency key.
func TestRetryNonReplaySafeMutationDoesNotRetry(t *testing.T) {
	conn := &fakeConn{results: []error{
		status.Error(codes.Unavailable, "transient"),
		nil,
	}}
	g := newTestClient(conn)

	// UpsertRequest carries a key, but DeletePolicy is ReplaySafe=false, so the
	// key is irrelevant: a non-replay-safe mutation must never be retried.
	req := &entityv1.UpsertRequest{IdempotencyKey: "idem-1"}
	err := g.InvokeUnary(context.Background(), mDeletePolicy, req, &entityv1.MutationResponse{})
	if err == nil {
		t.Fatal("InvokeUnary(DeletePolicy) = nil, want error (non-replay-safe, no retry)")
	}
	if conn.attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (non-replay-safe never retries)", conn.attempts)
	}
}

// (4) Read-only retry behavior is unchanged: a read retries on a transient code
// then succeeds, with no idempotency key in play.
func TestRetryReadOnlyUnchanged(t *testing.T) {
	conn := &fakeConn{results: []error{
		status.Error(codes.Unavailable, "transient"),
		status.Error(codes.DeadlineExceeded, "slow"),
		nil,
	}}
	g := newTestClient(conn)

	if err := g.InvokeUnary(context.Background(), mSelect, &entityv1.SelectRequest{}, &entityv1.RecordSet{}); err != nil {
		t.Fatalf("InvokeUnary(Select) = %v, want nil after retries", err)
	}
	if conn.attempts != 3 {
		t.Fatalf("attempts = %d, want 3 (two retries then success)", conn.attempts)
	}
}
