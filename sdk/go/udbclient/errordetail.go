package udbclient

import (
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ── Typed error-detail trailer decode (chapter 08.2) ─────────────────────────
//
// The broker encodes a udb.entity.v1.ErrorDetail into the udb-error-detail-bin
// binary trailer; the generated layer already captures those raw bytes into
// (*Error).DetailBin (preserved as raw bytes for compat). These accessors decode
// them into the generated ErrorDetail so callers branch on retryable()/kind()
// without string-parsing the gRPC message. Kept in a hand-written file so the
// generated_client.go template needs no regen for this surface.

// Detail prost-decodes the raw DetailBin trailer into the generated ErrorDetail.
// It returns (nil, false) when no detail was attached or the bytes fail to
// decode; the raw bytes remain available on Error.DetailBin either way.
func (e *Error) Detail() (*entityv1.ErrorDetail, bool) {
	if e == nil || len(e.DetailBin) == 0 {
		return nil, false
	}
	var d entityv1.ErrorDetail
	if err := proto.Unmarshal(e.DetailBin, &d); err != nil {
		return nil, false
	}
	return &d, true
}

// Retryable reports the broker's typed retry signal (false when no detail was
// attached). This is the authoritative retry/escalate hint — never parse the
// message string.
func (e *Error) Retryable() bool {
	d, ok := e.Detail()
	if !ok {
		return false
	}
	return d.GetRetryable()
}

// Kind returns the broker's typed error classification (ERROR_KIND_UNSPECIFIED
// when no detail was attached).
func (e *Error) Kind() entityv1.ErrorKind {
	d, ok := e.Detail()
	if !ok {
		return entityv1.ErrorKind_ERROR_KIND_UNSPECIFIED
	}
	return d.GetKind()
}

// FieldViolation is the SDK-level view of one structured validation failure.
// It deliberately does not depend on regenerated ErrorFieldViolation classes, so
// this helper compiles before SDK regen and starts returning entries as soon as
// the generated ErrorDetail descriptor includes field_violations.
type FieldViolation struct {
	Field       string
	Description string
}

// FieldViolations returns decoded validation field violations, or nil when no
// typed detail was attached, decoding failed, or the checked-in generated
// ErrorDetail class has not yet been refreshed with field_violations.
func (e *Error) FieldViolations() []FieldViolation {
	d, ok := e.Detail()
	if !ok {
		return nil
	}
	msg := d.ProtoReflect()
	fd := msg.Descriptor().Fields().ByName("field_violations")
	if fd == nil || !fd.IsList() {
		return nil
	}
	values := msg.Get(fd).List()
	if values.Len() == 0 {
		return nil
	}
	out := make([]FieldViolation, 0, values.Len())
	for i := 0; i < values.Len(); i++ {
		item := values.Get(i).Message()
		fields := item.Descriptor().Fields()
		out = append(out, FieldViolation{
			Field:       stringValue(item, fields.ByName("field")),
			Description: stringValue(item, fields.ByName("description")),
		})
	}
	return out
}

func stringValue(msg protoreflect.Message, fd protoreflect.FieldDescriptor) string {
	if fd == nil {
		return ""
	}
	return msg.Get(fd).String()
}
