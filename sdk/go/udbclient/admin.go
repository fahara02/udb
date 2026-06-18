package udbclient

import (
	"context"
	"fmt"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
)

// ── Migration approval-token in body (chapter 08.7) ──────────────────────────
//
// The broker now returns MigrationStatusResponse.approval_token in the response
// BODY (lane 06), so the SDK chains plan -> approve -> apply off the typed
// response field instead of scraping the x-udb-approval-token metadata trailer.
// AdminFacade wraps the DataBroker migration RPCs the data-plane Client already
// holds (Client.Broker) and adds no new connection.

// AdminFacade exposes the migration lifecycle helpers over the data-plane
// DataBroker client.
type AdminFacade struct {
	client *Client
}

// Admin returns the migration-lifecycle helper bound to this data-plane client.
func (c *Client) Admin() *AdminFacade { return &AdminFacade{client: c} }

// Admin returns the migration-lifecycle helper for the project.
func (u *Udb) Admin() *AdminFacade { return u.Data.Admin() }

// ApplyCurrent runs the full plan -> approve -> apply chain for a project,
// reading the approval token from approveResp.GetApprovalToken() (the typed
// response body) rather than a grpc.Trailer/metadata callback. It returns the
// final apply status.
func (a *AdminFacade) ApplyCurrent(ctx context.Context, projectID string) (*entityv1.MigrationStatusResponse, error) {
	plan, err := a.client.Broker.PlanMigration(a.client.Context(ctx), &entityv1.MigrationPlanRequest{
		ProjectId: projectID,
	})
	if err != nil {
		return nil, err
	}
	runID := plan.GetRunId()
	if runID == "" {
		return nil, fmt.Errorf("udb: PlanMigration returned no run id")
	}

	approve, err := a.client.Broker.ApproveMigrationPlan(a.client.Context(ctx), &entityv1.MigrationRunRequest{
		RunId:     runID,
		ProjectId: projectID,
	})
	if err != nil {
		return nil, err
	}
	token := approve.GetApprovalToken()

	return a.client.Broker.ApplyMigration(a.client.Context(ctx), &entityv1.MigrationApplyRequest{
		RunId:         runID,
		ProjectId:     projectID,
		ApprovalToken: token,
	})
}
