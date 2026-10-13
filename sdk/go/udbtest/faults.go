package udbtest

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
)

// Fault injects a bounded number of RPC failures, before any mutation applies.
// Method is a bare RPC name or full path; empty matches any RPC. Authenticate
// failures exercise API-key re-exchange/refresh through the unmodified SDK.
type Fault struct {
	Method     string
	TenantID   string
	Code       codes.Code
	Reason     string
	Times      int
	RetryAfter time.Duration
}

func (f *Fake) InjectFault(fault Fault) error {
	if fault.Code == codes.OK || fault.Times < 0 || fault.RetryAfter < 0 {
		return fmt.Errorf("udbtest: fault needs a failure code and non-negative count/delay")
	}
	if fault.Times == 0 {
		fault.Times = 1
	}
	if slash := strings.LastIndex(fault.Method, "/"); slash >= 0 {
		fault.Method = fault.Method[slash+1:]
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults = append(f.faults, fault)
	return nil
}

// SetTokenLifetime tunes subsequently-issued bearers, including refreshes.
// Existing bearers retain their original expiry, as they do in production.
func (f *Fake) SetTokenLifetime(lifetime time.Duration) error {
	if lifetime < time.Second {
		return fmt.Errorf("udbtest: token lifetime must be at least one second")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tokenLifetime = lifetime
	return nil
}

type fakeRateLimit struct {
	limit        int
	window       time.Duration
	perPrincipal bool
	buckets      map[string]fakeRateBucket
}
type fakeRateBucket struct {
	start time.Time
	count int
}

// SetRateLimit uses the broker's default tenant/operation fixed-window bucket.
// A zero limit disables the test limiter; negative or zero windows refuse.
func (f *Fake) SetRateLimit(limit int, window time.Duration) error {
	if limit < 0 || (limit > 0 && window <= 0) {
		return fmt.Errorf("udbtest: invalid rate limit")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rate = fakeRateLimit{limit: limit, window: window, buckets: map[string]fakeRateBucket{}}
	return nil
}

// SetRateLimitPerPrincipal enables the broker's optional per-principal bucket.
func (f *Fake) SetRateLimitPerPrincipal(limit int, window time.Duration) error {
	if err := f.SetRateLimit(limit, window); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rate.perPrincipal = true
	return nil
}
func (f *Fake) admit(ctx context.Context, method string, request any) error {
	identity := callerIdentity(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	if method == "Authenticate" {
		if req, ok := request.(interface{ GetApiKey() string }); ok {
			identity = f.identities[req.GetApiKey()]
		}
	}
	for i := range f.faults {
		fault := &f.faults[i]
		if fault.Times == 0 || (fault.Method != "" && fault.Method != method) ||
			(fault.TenantID != "" && fault.TenantID != identity.TenantID) {
			continue
		}
		fault.Times--
		reason := fault.Reason
		if reason == "" {
			switch fault.Code {
			case codes.Unavailable:
				reason = "UDB_BACKEND_UNAVAILABLE"
			case codes.ResourceExhausted:
				reason = "UDB_RATE_LIMITED"
			case codes.Aborted:
				reason = "UDB_CAS_CONFLICT"
			}
		}
		refused := refuse(fault.Code, reason, "injected "+method+" failure").(*refusal)
		refused.detail = &entityv1.ErrorDetail{Reason: reason, Kind: errorKind(fault.Code, reason),
			Retryable:    fault.Code == codes.Unavailable || fault.Code == codes.ResourceExhausted,
			RetryAfterMs: fault.RetryAfter.Milliseconds()}
		return refused
	}
	if method == "Authenticate" || f.rate.limit == 0 {
		return nil
	}
	key := identity.TenantID + "\x00" + method
	if f.rate.perPrincipal {
		key += "\x00" + identity.Subject
	}
	now := time.Now()
	bucket := f.rate.buckets[key]
	if bucket.start.IsZero() || now.Sub(bucket.start) >= f.rate.window {
		start := time.Unix(0, (now.UnixNano()/int64(f.rate.window))*int64(f.rate.window))
		bucket = fakeRateBucket{start: start}
	}
	bucket.count++
	f.rate.buckets[key] = bucket
	if bucket.count <= f.rate.limit {
		return nil
	}
	delay := max(time.Millisecond, f.rate.window-now.Sub(bucket.start))
	refused := refuse(codes.ResourceExhausted, "UDB_RATE_LIMITED", "verified caller exceeded the configured rate limit").(*refusal)
	refused.detail = &entityv1.ErrorDetail{Reason: refused.reason, Kind: entityv1.ErrorKind_ERROR_KIND_RATE_LIMITED,
		Retryable: true, RetryAfterMs: delay.Milliseconds(), Operation: method,
		Missing: map[string]string{"bucket": key, "limit": strconv.Itoa(f.rate.limit), "window_ms": strconv.FormatInt(f.rate.window.Milliseconds(), 10), "principal": identity.Subject}}
	return refused
}

func (f *Fake) authenticate(key string) (*authnv1.AuthnResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	identity, ok := f.identities[key]
	if !ok || key == "" || identity.TenantID == "" {
		return nil, refuse(codes.Unauthenticated, "", "unknown API key")
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, refuse(codes.Internal, "", "token generation failed")
	}
	token := base64.RawURLEncoding.EncodeToString(random[:])
	expires := time.Unix(time.Now().Add(f.tokenLifetime).Unix(), 0)
	f.tokens[token] = fakeToken{identity: identity, expires: expires}
	return &authnv1.AuthnResponse{
		AccessToken: token, ExpiresAtUnix: expires.Unix(),
		Principal: &authnv1.Principal{TenantId: identity.TenantID, ProjectId: identity.ProjectID,
			PrincipalId: identity.Subject, Subject: identity.Subject,
			UserId: identity.UserID, ServiceIdentity: identity.ServiceIdentity,
			Scopes: append([]string(nil), identity.Scopes...), Roles: append([]string(nil), identity.Roles...),
			ExpiresAtUnix: expires.Unix()},
	}, nil
}

func errorKind(code codes.Code, reason string) entityv1.ErrorKind {
	switch reason {
	case "UDB_CAS_CONFLICT", "UDB_CAS_ROW_MISSING":
		return entityv1.ErrorKind_ERROR_KIND_CONFLICT
	case "UDB_CAS_KEY_NOT_PK", "UDB_TABLE_NOT_TENANT_SCOPED", "UDB_NULL_COMPARISON", "UDB_UNSUPPORTED_FILTER_OPERATOR":
		return entityv1.ErrorKind_ERROR_KIND_VALIDATION
	case "UDB_UNIQUE_VIOLATION":
		return entityv1.ErrorKind_ERROR_KIND_UNIQUE
	case "UDB_NOT_NULL_VIOLATION":
		return entityv1.ErrorKind_ERROR_KIND_NOT_NULL
	case "UDB_SCOPE_MISSING", "UDB_TENANT_MISMATCH", "UDB_PROJECT_MISMATCH":
		return entityv1.ErrorKind_ERROR_KIND_PERMISSION
	case "UDB_POLICY_DENIED":
		return entityv1.ErrorKind_ERROR_KIND_POLICY
	case "UDB_UNKNOWN_MESSAGE_TYPE":
		return entityv1.ErrorKind_ERROR_KIND_SCHEMA
	case "UDB_RATE_LIMITED":
		return entityv1.ErrorKind_ERROR_KIND_RATE_LIMITED
	case "UDB_NO_ROWS_AFFECTED":
		return entityv1.ErrorKind_ERROR_KIND_NOT_FOUND
	}
	switch code {
	case codes.Unavailable:
		return entityv1.ErrorKind_ERROR_KIND_RETRYABLE
	case codes.Internal:
		return entityv1.ErrorKind_ERROR_KIND_INTERNAL
	case codes.NotFound:
		return entityv1.ErrorKind_ERROR_KIND_NOT_FOUND
	case codes.InvalidArgument:
		return entityv1.ErrorKind_ERROR_KIND_VALIDATION
	case codes.PermissionDenied:
		return entityv1.ErrorKind_ERROR_KIND_POLICY
	}
	return entityv1.ErrorKind_ERROR_KIND_UNSPECIFIED
}
