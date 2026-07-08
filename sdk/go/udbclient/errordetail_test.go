package udbclient

import (
	"testing"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func marshalDetail(t *testing.T, d *entityv1.ErrorDetail) []byte {
	t.Helper()
	b, err := proto.Marshal(d)
	if err != nil {
		t.Fatalf("marshal ErrorDetail: %v", err)
	}
	return b
}

func TestErrorDetailDecode(t *testing.T) {
	bin := marshalDetail(t, &entityv1.ErrorDetail{
		Retryable:    false,
		RetryAfterMs: 0,
		Kind:         entityv1.ErrorKind_ERROR_KIND_VALIDATION,
		Backend:      "postgres",
		FieldViolations: []*entityv1.ErrorFieldViolation{
			{Field: "email", Description: "must be a valid email"},
		},
	})
	e := &Error{DetailBin: bin}
	d, ok := e.Detail()
	if !ok {
		t.Fatal("Detail() returned false for a valid trailer")
	}
	if d.GetRetryable() || d.GetRetryAfterMs() != 0 || d.GetKind() != entityv1.ErrorKind_ERROR_KIND_VALIDATION {
		t.Fatalf("decoded detail wrong: %+v", d)
	}
	if e.Retryable() {
		t.Fatal("Retryable() should be false for validation detail")
	}
	if e.Kind() != entityv1.ErrorKind_ERROR_KIND_VALIDATION {
		t.Fatalf("Kind() wrong: %v", e.Kind())
	}
	got := e.FieldViolations()
	if len(got) != 1 || got[0].Field != "email" || got[0].Description != "must be a valid email" {
		t.Fatalf("FieldViolations() wrong: %+v", got)
	}
}

func TestErrorDetailQuotaRetryAfterDecode(t *testing.T) {
	bin := marshalDetail(t, &entityv1.ErrorDetail{
		Backend:      "admission",
		Operation:    "tenant budget",
		Retryable:    true,
		RetryAfterMs: 250,
		Kind:         entityv1.ErrorKind_ERROR_KIND_QUOTA,
	})
	e := &Error{DetailBin: bin}
	d, ok := e.Detail()
	if !ok {
		t.Fatal("Detail() returned false for a valid quota trailer")
	}
	if !e.Retryable() {
		t.Fatal("Retryable() should be true for retryable quota detail")
	}
	if d.GetRetryAfterMs() != 250 {
		t.Fatalf("RetryAfterMs wrong: %d", d.GetRetryAfterMs())
	}
	if e.Kind() != entityv1.ErrorKind_ERROR_KIND_QUOTA {
		t.Fatalf("Kind() wrong: %v", e.Kind())
	}
}

func TestTransportErrorDetailSynthesized(t *testing.T) {
	err := mapError("/svc/DoThing", status.Error(codes.DeadlineExceeded, "deadline"), metadata.MD{})
	mapped, ok := err.(*Error)
	if !ok {
		t.Fatalf("mapError returned %T, want *Error", err)
	}
	d, ok := mapped.Detail()
	if !ok {
		t.Fatal("Detail() returned false for synthesized transport detail")
	}
	if d.GetBackend() != "transport" || d.GetOperation() != "deadline_exceeded" {
		t.Fatalf("transport detail target wrong: %+v", d)
	}
	if !mapped.Retryable() || d.GetRetryAfterMs() != 0 || mapped.Kind() != entityv1.ErrorKind_ERROR_KIND_RETRYABLE {
		t.Fatalf("transport detail retry fields wrong: %+v", d)
	}
}

func TestCancelledTransportErrorDetailIsNotRetryable(t *testing.T) {
	err := mapError("/svc/DoThing", status.Error(codes.Canceled, "cancelled"), metadata.MD{})
	mapped, ok := err.(*Error)
	if !ok {
		t.Fatalf("mapError returned %T, want *Error", err)
	}
	d, ok := mapped.Detail()
	if !ok {
		t.Fatal("Detail() returned false for synthesized cancellation detail")
	}
	if d.GetBackend() != "transport" || d.GetOperation() != "cancelled" {
		t.Fatalf("cancellation transport detail target wrong: %+v", d)
	}
	if mapped.Retryable() || d.GetRetryAfterMs() != 0 || mapped.Kind() != entityv1.ErrorKind_ERROR_KIND_RETRYABLE {
		t.Fatalf("cancellation transport detail retry fields wrong: %+v", d)
	}
}

func TestErrorDetailAbsent(t *testing.T) {
	e := &Error{}
	if _, ok := e.Detail(); ok {
		t.Fatal("Detail() should be false with no DetailBin")
	}
	if e.Retryable() {
		t.Fatal("Retryable() should be false with no detail")
	}
	if e.Kind() != entityv1.ErrorKind_ERROR_KIND_UNSPECIFIED {
		t.Fatalf("Kind() should be UNSPECIFIED with no detail: %v", e.Kind())
	}
	if got := e.FieldViolations(); got != nil {
		t.Fatalf("FieldViolations() should be nil with no detail: %+v", got)
	}
}
