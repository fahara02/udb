package udbclient

import (
	"context"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ── Phase 7 (M5.6): framework adapters ───────────────────────────────────────
//
// These adapters let a server built on net/http or grpc lift the canonical UDB
// request context (tenant / user / correlation / request-id / project / purpose /
// scopes) off an inbound request and stash it on the request's context.Context
// as a Metadata, so downstream handlers — and any outbound UDB client built from
// FromContext — propagate the same identity without re-parsing headers.
//
// The header names mirror the ones Client.Context / GeneratedClient emit on the
// wire, so an inbound request that itself came from a UDB client round-trips its
// context faithfully. Request-id falls back to correlation-id when absent.

// metaContextKey is the unexported context key under which the per-request
// Metadata is stored. Unexported so only this package can set it; readers use
// FromContext.
type metaContextKey struct{}

// canonical inbound header names. These match the outbound headers attached by
// Client.Context and GeneratedClient.outgoingContext.
const (
	headerTenantID      = "x-tenant-id"
	headerUserID        = "x-user-id"
	headerPurpose       = "x-purpose"
	headerCorrelationID = "x-correlation-id"
	headerServiceID     = "x-service-identity"
	headerProjectID     = "x-udb-project-id"
	headerCatalogVer    = "x-udb-client-catalog-version"
	headerScopes        = "x-scopes"
	headerRequestID     = "x-request-id"
)

// WithMetadata returns a copy of ctx carrying meta, retrievable with FromContext.
func WithMetadata(ctx context.Context, meta Metadata) context.Context {
	return context.WithValue(ctx, metaContextKey{}, meta)
}

// FromContext returns the per-request Metadata stashed by a UDB adapter and true,
// or the zero Metadata and false when none is present.
func FromContext(ctx context.Context) (Metadata, bool) {
	meta, ok := ctx.Value(metaContextKey{}).(Metadata)
	return meta, ok
}

// MetadataFromContext returns the per-request Metadata, or the zero Metadata when
// none has been set. Convenience for callers that don't care about presence.
func MetadataFromContext(ctx context.Context) Metadata {
	meta, _ := FromContext(ctx)
	return meta
}

// CorrelationID returns the request's correlation id, or "" when unset.
func CorrelationID(ctx context.Context) string {
	return MetadataFromContext(ctx).CorrelationID
}

// TenantID returns the request's tenant id, or "" when unset.
func TenantID(ctx context.Context) string {
	return MetadataFromContext(ctx).TenantID
}

// UserID returns the request's user id, or "" when unset.
func UserID(ctx context.Context) string {
	return MetadataFromContext(ctx).UserID
}

// ── net/http middleware ──────────────────────────────────────────────────────

// HTTPMiddleware is a standard net/http middleware: it reads the canonical UDB
// headers off each inbound request, builds a Metadata, and stashes it on the
// request context before delegating to next. Downstream handlers read it with
// FromContext / MetadataFromContext.
//
// Usage:
//
//	mux := http.NewServeMux()
//	srv := &http.Server{Handler: udbclient.HTTPMiddleware(mux)}
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		meta := metadataFromHTTP(r.Header)
		next.ServeHTTP(w, r.WithContext(WithMetadata(r.Context(), meta)))
	})
}

func metadataFromHTTP(h http.Header) Metadata {
	correlation := h.Get(headerCorrelationID)
	if correlation == "" {
		correlation = h.Get(headerRequestID)
	}
	return Metadata{
		TenantID:             h.Get(headerTenantID),
		UserID:               h.Get(headerUserID),
		Purpose:              h.Get(headerPurpose),
		CorrelationID:        correlation,
		Scopes:               splitScopes(h.Get(headerScopes)),
		ServiceIdentity:      h.Get(headerServiceID),
		ProjectID:            h.Get(headerProjectID),
		ClientCatalogVersion: h.Get(headerCatalogVer),
	}
}

// ── gRPC server interceptor ──────────────────────────────────────────────────

// UnaryServerInterceptor is a grpc.UnaryServerInterceptor that reads the
// canonical UDB headers off the inbound gRPC metadata, builds a Metadata, and
// stashes it on the handler's context. Downstream handlers read it with
// FromContext / MetadataFromContext.
//
// Usage:
//
//	grpc.NewServer(grpc.UnaryInterceptor(udbclient.UnaryServerInterceptor))
func UnaryServerInterceptor(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	meta := metadataFromIncoming(ctx)
	return handler(WithMetadata(ctx, meta), req)
}

// StreamServerInterceptor is the streaming counterpart of UnaryServerInterceptor:
// it stashes the per-request Metadata on the stream's context so streaming
// handlers see the same identity.
func StreamServerInterceptor(
	srv any,
	ss grpc.ServerStream,
	_ *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	meta := metadataFromIncoming(ss.Context())
	return handler(srv, &metaServerStream{ServerStream: ss, ctx: WithMetadata(ss.Context(), meta)})
}

// metaServerStream overrides Context so the wrapped handler sees the
// Metadata-carrying context.
type metaServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *metaServerStream) Context() context.Context { return s.ctx }

func metadataFromIncoming(ctx context.Context) Metadata {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return Metadata{}
	}
	first := func(key string) string {
		if vals := md.Get(key); len(vals) > 0 {
			return vals[0]
		}
		return ""
	}
	correlation := first(headerCorrelationID)
	if correlation == "" {
		correlation = first(headerRequestID)
	}
	return Metadata{
		TenantID:             first(headerTenantID),
		UserID:               first(headerUserID),
		Purpose:              first(headerPurpose),
		CorrelationID:        correlation,
		Scopes:               splitScopes(first(headerScopes)),
		ServiceIdentity:      first(headerServiceID),
		ProjectID:            first(headerProjectID),
		ClientCatalogVersion: first(headerCatalogVer),
	}
}

// splitScopes parses a comma-joined scope header (the inverse of joinScopes)
// into a slice, dropping empties. Returns nil for an empty input so the zero
// Metadata stays scope-free.
func splitScopes(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if seg := s[start:i]; seg != "" {
				out = append(out, seg)
			}
			start = i + 1
		}
	}
	return out
}
