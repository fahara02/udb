package udbclient

import (
	"testing"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
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
		Retryable: true,
		Kind:      entityv1.ErrorKind_ERROR_KIND_RETRYABLE,
		Backend:   "postgres",
	})
	e := &Error{DetailBin: bin}
	d, ok := e.Detail()
	if !ok {
		t.Fatal("Detail() returned false for a valid trailer")
	}
	if !d.GetRetryable() || d.GetKind() != entityv1.ErrorKind_ERROR_KIND_RETRYABLE {
		t.Fatalf("decoded detail wrong: %+v", d)
	}
	if !e.Retryable() {
		t.Fatal("Retryable() should be true")
	}
	if e.Kind() != entityv1.ErrorKind_ERROR_KIND_RETRYABLE {
		t.Fatalf("Kind() wrong: %v", e.Kind())
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
}
