package udbclient

import (
	"context"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const ProtocolVersion = "1.0.0"

type Metadata struct {
	TenantID             string
	UserID               string
	Purpose              string
	CorrelationID        string
	Scopes               []string
	ServiceIdentity      string
	ProjectID            string
	ClientCatalogVersion string
}

// Only AsUser installs this marker. It means a caller explicitly selected a
// different bearer, whose identity the broker must verify. It carries no
// trusted user claims and never changes the client's tenant/project binding.
type asUserContextKey struct{}

type asUserContext struct {
	inheritedAudit Metadata
}

type Client struct {
	Broker servicesv1.DataBrokerClient
	Meta   Metadata
}

func New(conn grpc.ClientConnInterface, meta Metadata) *Client {
	return &Client{
		Broker: servicesv1.NewDataBrokerClient(conn),
		Meta:   meta,
	}
}

func (c *Client) Context(ctx context.Context) context.Context {
	// UDB-GO-006: merge REQUEST-SCOPED audit metadata (correlation id, bounded
	// purpose, client catalog version) from the context set via WithMetadata,
	// so two concurrent operations carry their own audit values without
	// mutating the shared Client.Meta. Identity — tenant, user, project,
	// scopes, service identity — stays authoritative from the connected client,
	// except AsUser deliberately omits stale service identity hints. The broker
	// verifies that explicit user bearer. Each header is emitted exactly once
	// (this is the single point every Entity and direct op flows through).
	m := MergeRequestScopedAudit(ctx, c.Meta)
	pairs := []string{
		"x-tenant-id", m.TenantID,
		"x-user-id", m.UserID,
		"x-purpose", m.Purpose,
		"x-correlation-id", m.CorrelationID,
		"x-service-identity", m.ServiceIdentity,
		"x-udb-project-id", m.ProjectID,
		"x-udb-client-catalog-version", m.ClientCatalogVersion,
	}
	if len(m.Scopes) > 0 {
		pairs = append(pairs, "x-scopes", joinScopes(m.Scopes))
	}
	return outgoingOnce(ctx, MetadataFromContext(ctx), pairs)
}

// outgoingOnce sets each header once, so a context already built by Context or DataContext does
// not send a second copy, where the first would win and hide a value set with WithMetadata. A
// value the caller attached itself stays, unless this request set that header with WithMetadata.
func outgoingOnce(ctx context.Context, request Metadata, pairs []string) context.Context {
	existing, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return metadata.AppendToOutgoingContext(ctx, pairs...)
	}
	scoped := map[string]bool{
		"x-purpose":                    request.Purpose != "",
		"x-correlation-id":             request.CorrelationID != "",
		"x-udb-client-catalog-version": request.ClientCatalogVersion != "",
	}
	md := existing.Copy()
	for i := 0; i+1 < len(pairs); i += 2 {
		key, value := pairs[i], pairs[i+1]
		if !scoped[key] {
			for _, prior := range existing.Get(key) {
				if prior != "" {
					value = prior
					break
				}
			}
		}
		md.Set(key, value)
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// MergeRequestScopedAudit resolves the three REQUEST-SCOPED audit values
// (purpose, correlation id, client catalog version) by preferring what the
// caller attached to this context with WithMetadata over the connection-level
// value, and returns the Metadata to emit as headers.
//
// Identity is never adopted from request metadata. AsUser explicitly selects a
// different bearer and omits stale service user/service/scopes hints, leaving
// the broker to verify the bearer. Tenant and project stay bound to the client.
//
// Every header-building path (the DataBroker client, the auth client, and the
// generated dial interceptor that carries the native services) funnels through
// this one function. Keeping it single-sourced is what stops a facade from
// silently reverting to connection-level-only correlation, which is how
// per-request audit traceability was previously lost on native calls.
func MergeRequestScopedAudit(ctx context.Context, client Metadata) Metadata {
	req := MetadataFromContext(ctx)
	merged := client
	if delegated, ok := ctx.Value(asUserContextKey{}).(asUserContext); ok {
		merged.UserID = ""
		merged.ServiceIdentity = ""
		merged.Scopes = nil
		merged.Purpose = firstNonEmptyValue(delegated.inheritedAudit.Purpose, merged.Purpose)
		merged.CorrelationID = firstNonEmptyValue(delegated.inheritedAudit.CorrelationID, merged.CorrelationID)
		merged.ClientCatalogVersion = firstNonEmptyValue(delegated.inheritedAudit.ClientCatalogVersion, merged.ClientCatalogVersion)
	}
	merged.Purpose = firstNonEmptyValue(req.Purpose, merged.Purpose)
	merged.CorrelationID = firstNonEmptyValue(req.CorrelationID, merged.CorrelationID)
	merged.ClientCatalogVersion = firstNonEmptyValue(req.ClientCatalogVersion, merged.ClientCatalogVersion)
	return merged
}

func firstNonEmptyValue(reqVal, clientVal string) string {
	if reqVal != "" {
		return reqVal
	}
	return clientVal
}

func (c *Client) Select(ctx context.Context, req *entityv1.SelectRequest) (*entityv1.RecordSet, error) {
	return c.Broker.Select(c.Context(ctx), req)
}

func (c *Client) Upsert(ctx context.Context, req *entityv1.UpsertRequest) (*entityv1.MutationResponse, error) {
	return c.Broker.Upsert(c.Context(ctx), req)
}

func (c *Client) Delete(ctx context.Context, req *entityv1.DeleteRequest) (*entityv1.MutationResponse, error) {
	return c.Broker.Delete(c.Context(ctx), req)
}

// Update issues the partial-update RPC (W7): SET named columns and/or apply
// atomic increments on the matched rows — no full-record resend.
func (c *Client) Update(ctx context.Context, req *entityv1.UpdateRequest) (*entityv1.MutationResponse, error) {
	return c.Broker.Update(c.Context(ctx), req)
}

func joinScopes(scopes []string) string {
	if len(scopes) == 0 {
		return ""
	}
	out := scopes[0]
	for _, scope := range scopes[1:] {
		out += "," + scope
	}
	return out
}
