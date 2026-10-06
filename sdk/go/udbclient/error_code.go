package udbclient

import (
	"errors"
	"time"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorCode is the closed set of ways a udb call fails. Branch on it (or on
// the helpers below) instead of matching error text: the broker's stable
// `UDB_*` reason decides it when present, then the typed kind, then the gRPC
// code.
type ErrorCode int

const (
	CodeUnknown ErrorCode = iota
	// CodeConflict: a conditional write lost — re-read and retry.
	CodeConflict
	// CodeNotFound: the addressed row or resource does not exist.
	CodeNotFound
	// CodeUnique: a unique constraint was violated (Inspect(err).Constraint names it).
	CodeUnique
	// CodeNotNull: a NOT NULL column got no value (Inspect(err).Column names it).
	CodeNotNull
	// CodeForeignKey: a foreign-key constraint was violated.
	CodeForeignKey
	// CodePrecondition: the request is not valid in the current state (for
	// example a conditional write not keyed by a primary or unique key).
	CodePrecondition
	// CodeScopeMissing: the caller lacks a scope (Inspect(err).Missing["scope"]).
	CodeScopeMissing
	// CodePolicyDenied: no policy rule allows the call (Inspect(err).Missing["rule"]).
	CodePolicyDenied
	// CodeRedacted: a write tried to store a redacted placeholder.
	CodeRedacted
	// CodeRateLimited: a rate limit was hit (Inspect(err).RetryAfter says when to retry).
	CodeRateLimited
	// CodeUnavailable: transient; retry with backoff.
	CodeUnavailable
	// CodeInvalid: the request itself is malformed.
	CodeInvalid
	// CodeUnauthenticated: the credential is missing, expired or invalid.
	CodeUnauthenticated
	// CodeInternal: a broker defect; report it.
	CodeInternal
)

func (c ErrorCode) String() string {
	return [...]string{"unknown", "conflict", "not_found", "unique", "not_null", "foreign_key",
		"precondition", "scope_missing", "policy_denied", "redacted", "rate_limited",
		"unavailable", "invalid", "unauthenticated", "internal"}[c]
}

// ErrorInfo is everything the broker said about a failure.
type ErrorInfo struct {
	Code ErrorCode
	// Reason is the stable `UDB_*` reason ("" when the broker sent none).
	Reason     string
	Constraint string
	Column     string
	// FixHint is one sentence on how to fix the request.
	FixHint string
	// Missing names what the caller lacks (scope, rule, purpose, ...).
	Missing    map[string]string
	RetryAfter time.Duration
	// GRPC is the underlying gRPC code.
	GRPC codes.Code
}

var reasonCodes = map[string]ErrorCode{
	"UDB_CAS_CONFLICT":                 CodeConflict,
	"UDB_CAS_ROW_MISSING":              CodeConflict,
	"UDB_REVISION_CONFLICT":            CodeConflict,
	"UDB_GRANT_OWNED_BY_OTHER":         CodeConflict,
	"UDB_CAS_KEY_NOT_PK":               CodePrecondition,
	"UDB_NO_ROWS_AFFECTED":             CodeNotFound,
	"UDB_UNIQUE_VIOLATION":             CodeUnique,
	"UDB_NOT_NULL_VIOLATION":           CodeNotNull,
	"UDB_UPSERT_INCOMPLETE_ROW":        CodeNotNull,
	"UDB_FOREIGN_KEY_VIOLATION":        CodeForeignKey,
	"UDB_TYPE_MISMATCH":                CodeInvalid,
	"UDB_DECODE_FAILED":                CodeInternal,
	"UDB_UNSUPPORTED_FILTER_OPERATOR":  CodeInvalid,
	"UDB_NULL_COMPARISON":              CodeInvalid,
	"UDB_TENANT_MISMATCH":              CodePolicyDenied,
	"UDB_TABLE_NOT_TENANT_SCOPED":      CodeInvalid,
	"UDB_SCOPE_MISSING":                CodeScopeMissing,
	"UDB_POLICY_DENIED":                CodePolicyDenied,
	"UDB_REDACTED_VALUE_WRITE":         CodeRedacted,
	"UDB_RATE_LIMITED":                 CodeRateLimited,
	"UDB_UNKNOWN_MESSAGE_TYPE":         CodeInvalid,
	"UDB_ENVELOPE_VERSION_UNSUPPORTED": CodePrecondition,
}

var kindCodes = map[entityv1.ErrorKind]ErrorCode{
	entityv1.ErrorKind_ERROR_KIND_CONFLICT:     CodeConflict,
	entityv1.ErrorKind_ERROR_KIND_NOT_FOUND:    CodeNotFound,
	entityv1.ErrorKind_ERROR_KIND_UNIQUE:       CodeUnique,
	entityv1.ErrorKind_ERROR_KIND_NOT_NULL:     CodeNotNull,
	entityv1.ErrorKind_ERROR_KIND_FOREIGN_KEY:  CodeForeignKey,
	entityv1.ErrorKind_ERROR_KIND_PERMISSION:   CodePolicyDenied,
	entityv1.ErrorKind_ERROR_KIND_REDACTED:     CodeRedacted,
	entityv1.ErrorKind_ERROR_KIND_RATE_LIMITED: CodeRateLimited,
	entityv1.ErrorKind_ERROR_KIND_QUOTA:        CodeRateLimited,
	entityv1.ErrorKind_ERROR_KIND_RETRYABLE:    CodeUnavailable,
	entityv1.ErrorKind_ERROR_KIND_VALIDATION:   CodeInvalid,
	entityv1.ErrorKind_ERROR_KIND_INTERNAL:     CodeInternal,
}

var grpcCodes = map[codes.Code]ErrorCode{
	codes.NotFound:           CodeNotFound,
	codes.AlreadyExists:      CodeUnique,
	codes.FailedPrecondition: CodePrecondition,
	codes.PermissionDenied:   CodePolicyDenied,
	codes.ResourceExhausted:  CodeRateLimited,
	codes.Unavailable:        CodeUnavailable,
	codes.DeadlineExceeded:   CodeUnavailable,
	codes.InvalidArgument:    CodeInvalid,
	codes.Unauthenticated:    CodeUnauthenticated,
	codes.Internal:           CodeInternal,
}

// Inspect reads everything the broker said about err. A nil err is the zero
// ErrorInfo; an error that is not a udb call failure is CodeUnknown.
func Inspect(err error) ErrorInfo {
	if err == nil {
		return ErrorInfo{}
	}
	info := ErrorInfo{Code: CodeUnknown, GRPC: status.Code(err)}
	if e, ok := AsError(err); ok {
		info.GRPC = e.Code
		if d, ok := e.Detail(); ok {
			info.Reason = d.GetReason()
			info.Constraint = d.GetConstraint()
			info.Column = d.GetColumn()
			info.FixHint = d.GetFixHint()
			info.Missing = d.GetMissing()
			info.RetryAfter = time.Duration(d.GetRetryAfterMs()) * time.Millisecond
			if code, ok := reasonCodes[info.Reason]; ok {
				info.Code = code
				return info
			}
			if code, ok := kindCodes[d.GetKind()]; ok && d.GetKind() != entityv1.ErrorKind_ERROR_KIND_VALIDATION {
				info.Code = code
				return info
			}
		}
	}
	switch {
	case errors.Is(err, ErrConflict):
		info.Code = CodeConflict
	case errors.Is(err, ErrNotFound):
		info.Code = CodeNotFound
	default:
		if code, ok := grpcCodes[info.GRPC]; ok {
			info.Code = code
		}
	}
	return info
}

// CodeOf is Inspect(err).Code.
func CodeOf(err error) ErrorCode { return Inspect(err).Code }

// IsUnique reports a unique-constraint violation; with a constraint name it
// matches only that constraint.
func IsUnique(err error, constraint ...string) bool {
	info := Inspect(err)
	if info.Code != CodeUnique {
		return false
	}
	return len(constraint) == 0 || info.Constraint == constraint[0]
}

// IsNotFound reports a missing row or resource.
func IsNotFound(err error) bool { return CodeOf(err) == CodeNotFound }

// IsConflict reports a lost conditional write.
func IsConflict(err error) bool { return CodeOf(err) == CodeConflict }

// MissingScope is the scope the broker said the caller lacks ("" when the
// failure is not a missing scope).
func MissingScope(err error) string {
	info := Inspect(err)
	if info.Code != CodeScopeMissing {
		return ""
	}
	return info.Missing["scope"]
}
