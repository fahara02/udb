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
	pairs := []string{
		"x-tenant-id", c.Meta.TenantID,
		"x-user-id", c.Meta.UserID,
		"x-purpose", c.Meta.Purpose,
		"x-correlation-id", c.Meta.CorrelationID,
		"x-service-identity", c.Meta.ServiceIdentity,
		"x-udb-project-id", c.Meta.ProjectID,
		"x-udb-client-catalog-version", c.Meta.ClientCatalogVersion,
	}
	if len(c.Meta.Scopes) > 0 {
		pairs = append(pairs, "x-scopes", joinScopes(c.Meta.Scopes))
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

func (c *Client) Select(ctx context.Context, req *entityv1.SelectRequest) (*entityv1.RecordSet, error) {
	return c.Broker.Select(c.Context(ctx), req)
}

func (c *Client) Upsert(ctx context.Context, req *entityv1.UpsertRequest) (*entityv1.MutationResponse, error) {
	return c.Broker.Upsert(c.Context(ctx), req)
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
