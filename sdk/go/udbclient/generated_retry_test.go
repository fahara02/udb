package udbclient

import (
	"testing"

	"google.golang.org/grpc/codes"
)

func TestRetryConfigDoesNotRetryMutations(t *testing.T) {
	rc := DefaultRetryConfig()

	for _, code := range []codes.Code{codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded} {
		if rc.retryableForRPC(code, false) {
			t.Fatalf("retryableForRPC(%s, readOnly=false) = true, want false", code)
		}
	}
}

func TestRetryConfigKeepsReadOnlyTransientRetries(t *testing.T) {
	rc := DefaultRetryConfig()

	for _, code := range []codes.Code{codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded} {
		if !rc.retryableForRPC(code, true) {
			t.Fatalf("retryableForRPC(%s, readOnly=true) = false, want true", code)
		}
	}
	if rc.retryableForRPC(codes.InvalidArgument, true) {
		t.Fatal("retryableForRPC(InvalidArgument, readOnly=true) = true, want false")
	}
}
