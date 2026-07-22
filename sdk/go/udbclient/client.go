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
	// scopes, service identity — stays AUTHORITATIVE from the connected client
	// and is never overridable per request. Each header is emitted exactly once
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
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

// MergeRequestScopedAudit resolves the three REQUEST-SCOPED audit values
// (purpose, correlation id, client catalog version) by preferring what the
// caller attached to this context with WithMetadata over the connection-level
// value, and returns the Metadata to emit as headers.
//
// Identity — tenant, user, project, scopes, service identity — is deliberately
// NOT resolved here: it stays authoritative from the connected client and can
// never be overridden per request, so a caller cannot smuggle another
// principal's identity in through a context value.
//
// Every header-building path (the DataBroker client, the auth client, and the
// generated dial interceptor that carries the native services) funnels through
// this one function. Keeping it single-sourced is what stops a facade from
// silently reverting to connection-level-only correlation, which is how
// per-request audit traceability was previously lost on native calls.
func MergeRequestScopedAudit(ctx context.Context, client Metadata) Metadata {
	req := MetadataFromContext(ctx)
	merged := client
	merged.Purpose = firstNonEmptyValue(req.Purpose, client.Purpose)
	merged.CorrelationID = firstNonEmptyValue(req.CorrelationID, client.CorrelationID)
	merged.ClientCatalogVersion = firstNonEmptyValue(req.ClientCatalogVersion, client.ClientCatalogVersion)
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
