package udbclient

import (
	"errors"
	"testing"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func TestTypedReasonsControlCASRetryDespiteMessage(t *testing.T) {
	for _, reason := range []string{"UDB_IDEMPOTENCY_REUSE", "UDB_ENVELOPE_VERSION_UNSUPPORTED"} {
		detail, err := proto.Marshal(&entityv1.ErrorDetail{Reason: reason, Kind: entityv1.ErrorKind_ERROR_KIND_CONFLICT})
		if err != nil {
			t.Fatal(err)
		}
		call := &Error{Code: codes.FailedPrecondition,
			Message: "compare-and-swap precondition failed", DetailBin: detail}
		if CodeOf(call) != CodePrecondition || IsCASConflict(call) || errors.Is(classify(call), ErrConflict) {
			t.Fatalf("non-CAS reason %s became retryable from coarse kind/message: %v", reason, call)
		}
	}
	for _, reason := range []string{"UDB_CAS_CONFLICT", "UDB_CAS_ROW_MISSING", "UDB_REVISION_CONFLICT"} {
		detail, err := proto.Marshal(&entityv1.ErrorDetail{Reason: reason})
		if err != nil {
			t.Fatal(err)
		}
		call := &Error{Code: codes.FailedPrecondition, Message: "opaque operator text", DetailBin: detail}
		if !IsCASConflict(call) || !errors.Is(classify(call), ErrConflict) {
			t.Fatalf("typed CAS reason %s depended on message text: %v", reason, call)
		}
	}
}

func TestTokenExpiredAndRevokedReasonsStayUnauthenticated(t *testing.T) {
	for reason, want := range map[string][2]bool{
		"UDB_TOKEN_EXPIRED":         {true, false},
		"UDB_TOKEN_REVOKED":         {false, true},
		"UDB_AUTHENTICATION_FAILED": {false, false},
	} {
		detail, err := proto.Marshal(&entityv1.ErrorDetail{Reason: reason, Kind: entityv1.ErrorKind_ERROR_KIND_POLICY})
		if err != nil {
			t.Fatal(err)
		}
		call := &Error{Code: codes.Unauthenticated, Message: "opaque text", DetailBin: detail}
		if CodeOf(call) != CodeUnauthenticated {
			t.Fatalf("%s: CodeOf = %s, want unauthenticated", reason, CodeOf(call))
		}
		if IsTokenExpired(call) != want[0] || IsTokenRevoked(call) != want[1] {
			t.Fatalf("%s: IsTokenExpired=%v IsTokenRevoked=%v, want %v", reason, IsTokenExpired(call), IsTokenRevoked(call), want)
		}
	}
	if IsTokenExpired(errors.New("UDB_TOKEN_EXPIRED")) {
		t.Fatal("message text must never classify an error")
	}
}
