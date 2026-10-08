package udbclient

import (
	"errors"
	"strings"
	"time"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
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

// RetryAfter is the broker-suggested backoff before retrying, or 0 when none was
// provided (or no typed detail was attached). Pair with Retryable().
func (e *Error) RetryAfter() time.Duration {
	d, ok := e.Detail()
	if !ok {
		return 0
	}
	return time.Duration(d.GetRetryAfterMs()) * time.Millisecond
}

// Reason returns the stable machine-readable reason a caller can branch on: the
// broker's `UDB_*` reason code (see docs/error-reasons.md) when it sent one, else
// the policy decision id for auth/policy denials, else the capability token,
// else "". Prefer this over matching the human-readable message, which may change.
func (e *Error) Reason() string {
	d, ok := e.Detail()
	if !ok {
		return ""
	}
	if r := d.GetReason(); r != "" {
		return r
	}
	if r := d.GetPolicyDecisionId(); r != "" {
		return r
	}
	return d.GetCapabilityRequired()
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

// IsCASConflict reports whether err is a compare-and-swap precondition failure —
// the target row was absent or a field no longer matched the expected value.
// Check it after Upsert(WithExpected) or Delete(WithDeleteExpected) to decide
// whether to re-read and retry the optimistic operation. Detected by the
// stable typed reason, with message fallback for older brokers. Other
// FAILED_PRECONDITION refusals —
// e.g. a conditional mutation whose filter does not pin the primary key — are
// usage errors that a retry can never fix, and are NOT conflicts.
func IsCASConflict(err error) bool {
	var e *Error
	if !errors.As(err, &e) || e.Code != codes.FailedPrecondition {
		return false
	}
	if detail, ok := e.Detail(); ok && detail.GetReason() != "" {
		switch detail.GetReason() {
		case "UDB_CAS_CONFLICT", "UDB_CAS_ROW_MISSING", "UDB_REVISION_CONFLICT":
			return true
		default:
			return false
		}
	}
	return strings.HasPrefix(e.Message, "compare-and-swap precondition failed") ||
		strings.HasPrefix(e.Message, "revision precondition failed")
}
