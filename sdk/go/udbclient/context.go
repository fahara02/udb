package udbclient

import (
	commonpb "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	entitypb "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
)

// RequestContextMeta is a unified, SDK-level request context (critic.md §14).
//
// UDB has TWO proto RequestContext shapes: the DataBroker's flat
// entity/v1.RequestContext{TenantId, ProjectId, ...} and the native services'
// nested core/common/v1.RequestContext{Tenant: TenantContext{...}, ...}. App
// code should not have to import both packages or remember which RPC wants which
// shape — build this once from Metadata and convert with ToEntity()/ToCommon().
type RequestContextMeta struct {
	TenantID        string
	ProjectID       string
	UserID          string
	CorrelationID   string
	RequestID       string
	Purpose         string
	ServiceIdentity string
	Scopes          []string
}

// ToRequestContextMeta projects the caller Metadata into the unified context.
func (m Metadata) ToRequestContextMeta() RequestContextMeta {
	return RequestContextMeta{
		TenantID:        m.TenantID,
		ProjectID:       m.ProjectID,
		UserID:          m.UserID,
		CorrelationID:   m.CorrelationID,
		Purpose:         m.Purpose,
		ServiceIdentity: m.ServiceIdentity,
		Scopes:          m.Scopes,
	}
}

// ToEntity builds the DataBroker (entity/v1) RequestContext.
func (r RequestContextMeta) ToEntity() *entitypb.RequestContext {
	return &entitypb.RequestContext{
		TenantId:        r.TenantID,
		ProjectId:       r.ProjectID,
		UserId:          r.UserID,
		CorrelationId:   r.CorrelationID,
		Purpose:         r.Purpose,
		ServiceIdentity: r.ServiceIdentity,
		Scopes:          r.Scopes,
	}
}

// ToCommon builds the native-service (core/common/v1) RequestContext, nesting the
// tenant/project under TenantContext as those RPCs expect.
func (r RequestContextMeta) ToCommon() *commonpb.RequestContext {
	return &commonpb.RequestContext{
		Tenant: &commonpb.TenantContext{
			TenantId:  r.TenantID,
			ProjectId: r.ProjectID,
		},
		RequestId:       r.RequestID,
		CorrelationId:   r.CorrelationID,
		UserId:          r.UserID,
		Purpose:         r.Purpose,
		ServiceIdentity: r.ServiceIdentity,
		Scopes:          r.Scopes,
	}
}
