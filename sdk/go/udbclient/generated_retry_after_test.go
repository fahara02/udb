package udbclient

import (
	"context"
	"testing"
	"time"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// trailerConn is a grpc.ClientConnInterface whose first attempts fail with a
// scripted error AND an `udb-error-detail-bin` trailer, the way the broker
// refuses a rate-limited / fair-admission / overloaded call.
type trailerConn struct {
	errs     []error  // per attempt; past the end the call succeeds
	details  [][]byte // per attempt trailer detail (nil = none)
	attempts int
	at       []time.Time
}

func (f *trailerConn) Invoke(_ context.Context, _ string, _, _ any, opts ...grpc.CallOption) error {
	idx := f.attempts
	f.attempts++
	f.at = append(f.at, time.Now())
	if idx >= len(f.errs) {
		return nil
	}
	if idx < len(f.details) && f.details[idx] != nil {
		for _, o := range opts {
			if tr, ok := o.(grpc.TrailerCallOption); ok && tr.TrailerAddr != nil {
				*tr.TrailerAddr = metadata.Pairs(errorDetailTrailer, string(f.details[idx]))
			}
		}
	}
	return f.errs[idx]
}

func (f *trailerConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Error(codes.Unimplemented, "trailerConn has no streams")
}

func quotaDetail(t *testing.T, retryAfterMs int64) []byte {
	t.Helper()
	return marshalDetail(t, &entityv1.ErrorDetail{
		Backend:      "admission",
		Operation:    "tenant budget",
		Retryable:    true,
		RetryAfterMs: retryAfterMs,
		Kind:         entityv1.ErrorKind_ERROR_KIND_QUOTA,
	})
}

func exhaustedThenOK(t *testing.T, retryAfterMs int64) *trailerConn {
	return &trailerConn{
		errs:    []error{status.Error(codes.ResourceExhausted, "rate limited")},
		details: [][]byte{quotaDetail(t, retryAfterMs)},
	}
}

// recordSleeps replaces the retry sleep seam with one that records each wait
// and returns at once; the original is restored when the test ends.
func recordSleeps(t *testing.T) *[]time.Duration {
	t.Helper()
	var got []time.Duration
	orig := retrySleep
	retrySleep = func(_ context.Context, d time.Duration) error {
		got = append(got, d)
		return nil
	}
	t.Cleanup(func() { retrySleep = orig })
	return &got
}

// (a) The broker's retry_after_ms is honoured with a real wait, not only the
// SDK's own (here 1ms) backoff. Repeated 3 times.
func TestRetryHonoursServerRetryAfter(t *testing.T) {
	for i := 0; i < 3; i++ {
		conn := exhaustedThenOK(t, 1200)
		g := newTestClient(conn)
		start := time.Now()
		if err := g.InvokeUnary(context.Background(), mSelect, &entityv1.SelectRequest{}, &entityv1.RecordSet{}); err != nil {
			t.Fatalf("run %d: InvokeUnary = %v, want nil after retry", i, err)
		}
		if conn.attempts != 2 {
			t.Fatalf("run %d: attempts = %d, want 2", i, conn.attempts)
		}
		if gap := conn.at[1].Sub(conn.at[0]); gap < 1200*time.Millisecond {
			t.Fatalf("run %d: waited %v between attempts, want >= 1200ms", i, gap)
		}
		if total := time.Since(start); total < 1200*time.Millisecond {
			t.Fatalf("run %d: call took %v, want >= 1200ms", i, total)
		}
	}
}

// (a') The dial-option interceptor path honours retry_after the same way.
func TestRetryInterceptorHonoursServerRetryAfter(t *testing.T) {
	sleeps := recordSleeps(t)
	conn := exhaustedThenOK(t, 1200)
	g := newTestClient(conn)
	invoker := func(ctx context.Context, method string, req, reply any, _ *grpc.ClientConn, opts ...grpc.CallOption) error {
		return conn.Invoke(ctx, method, req, reply, opts...)
	}
	err := g.unaryInterceptor()(context.Background(), mSelect, &entityv1.SelectRequest{}, &entityv1.RecordSet{}, nil, invoker)
	if err != nil {
		t.Fatalf("interceptor = %v, want nil after retry", err)
	}
	if len(*sleeps) != 1 || (*sleeps)[0] != 1200*time.Millisecond {
		t.Fatalf("sleeps = %v, want [1.2s]", *sleeps)
	}
}

// (b) A retry_after that would pass the caller's deadline is not slept: the
// call stops and returns the broker's error with its detail intact.
func TestRetryAfterPastDeadlineReturnsLastError(t *testing.T) {
	sleeps := recordSleeps(t)
	conn := exhaustedThenOK(t, 1200)
	g := newTestClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := g.InvokeUnary(ctx, mSelect, &entityv1.SelectRequest{}, &entityv1.RecordSet{})
	if err == nil {
		t.Fatal("InvokeUnary = nil, want the rate-limit error")
	}
	if conn.attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (no retry past the deadline)", conn.attempts)
	}
	if len(*sleeps) != 0 {
		t.Fatalf("slept %v, want no sleep", *sleeps)
	}
	if status.Code(err) != codes.ResourceExhausted && Inspect(err).GRPC != codes.ResourceExhausted {
		t.Fatalf("err code = %v, want ResourceExhausted", Inspect(err).GRPC)
	}
	if got := Inspect(err).RetryAfter; got != 1200*time.Millisecond {
		t.Fatalf("Inspect(err).RetryAfter = %v, want 1.2s", got)
	}
	if e, ok := AsError(err); !ok || e.RetryAfter() != 1200*time.Millisecond {
		t.Fatalf("(*Error).RetryAfter() lost: %v", err)
	}
}

// (c) A broker-requested delay longer than MaxRetryAfter is not waited for:
// the call returns the broker's error at once, retry_after intact, so the
// caller decides. Zero MaxRetryAfter means the 10s default.
func TestRetryAfterAboveCapReturnsAtOnce(t *testing.T) {
	if got := DefaultRetryConfig().MaxRetryAfter; got != 10*time.Second {
		t.Fatalf("DefaultRetryConfig().MaxRetryAfter = %v, want 10s", got)
	}
	if got := (RetryConfig{}).maxRetryAfter(); got != 10*time.Second {
		t.Fatalf("zero MaxRetryAfter = %v, want the 10s default", got)
	}
	cases := []struct {
		name         string
		maxAfter     time.Duration
		retryAfterMs int64
	}{
		{"explicit cap", 50 * time.Millisecond, 5000},
		{"default cap", 0, 60000},
	}
	for _, tc := range cases {
		sleeps := recordSleeps(t)
		conn := exhaustedThenOK(t, tc.retryAfterMs)
		rc := fastRetry()
		rc.MaxRetryAfter = tc.maxAfter
		g := NewGenerated(conn, Options{Retry: rc})
		err := g.InvokeUnary(context.Background(), mSelect, &entityv1.SelectRequest{}, &entityv1.RecordSet{})
		if err == nil {
			t.Fatalf("%s: InvokeUnary = nil, want the rate-limit error", tc.name)
		}
		if conn.attempts != 1 {
			t.Fatalf("%s: attempts = %d, want 1 (no retry above the cap)", tc.name, conn.attempts)
		}
		if len(*sleeps) != 0 {
			t.Fatalf("%s: slept %v, want no sleep", tc.name, *sleeps)
		}
		if Inspect(err).GRPC != codes.ResourceExhausted {
			t.Fatalf("%s: err code = %v, want ResourceExhausted", tc.name, Inspect(err).GRPC)
		}
		want := time.Duration(tc.retryAfterMs) * time.Millisecond
		if got := Inspect(err).RetryAfter; got != want {
			t.Fatalf("%s: Inspect(err).RetryAfter = %v, want %v", tc.name, got, want)
		}
	}

	// A delay at or under the default cap is still waited for.
	sleeps := recordSleeps(t)
	conn := exhaustedThenOK(t, 10000)
	rc := fastRetry()
	rc.MaxRetryAfter = 0
	g := NewGenerated(conn, Options{Retry: rc})
	if err := g.InvokeUnary(context.Background(), mSelect, &entityv1.SelectRequest{}, &entityv1.RecordSet{}); err != nil {
		t.Fatalf("InvokeUnary = %v, want nil", err)
	}
	if len(*sleeps) != 1 || (*sleeps)[0] != 10*time.Second {
		t.Fatalf("sleeps = %v, want [10s]", *sleeps)
	}
}

// Without a retry_after the wait stays the SDK's own backoff, and a mutation
// that is not replay-safe still never retries even when the broker names a delay.
func TestRetryAfterDoesNotChangeRetrySafety(t *testing.T) {
	sleeps := recordSleeps(t)
	conn := &trailerConn{errs: []error{status.Error(codes.ResourceExhausted, "busy")}}
	g := newTestClient(conn)
	if err := g.InvokeUnary(context.Background(), mSelect, &entityv1.SelectRequest{}, &entityv1.RecordSet{}); err != nil {
		t.Fatalf("InvokeUnary = %v, want nil", err)
	}
	if len(*sleeps) != 1 || (*sleeps)[0] != time.Millisecond {
		t.Fatalf("sleeps = %v, want [1ms] (own backoff)", *sleeps)
	}

	*sleeps = nil
	conn = exhaustedThenOK(t, 1200)
	g = newTestClient(conn)
	if err := g.InvokeUnary(context.Background(), mDeletePolicy, &entityv1.UpsertRequest{}, &entityv1.MutationResponse{}); err == nil {
		t.Fatal("non-replay-safe mutation retried, want the error")
	}
	if conn.attempts != 1 || len(*sleeps) != 0 {
		t.Fatalf("attempts=%d sleeps=%v, want 1 attempt and no sleep", conn.attempts, *sleeps)
	}
}
