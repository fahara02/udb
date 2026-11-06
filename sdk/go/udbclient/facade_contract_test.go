package udbclient

import (
	"context"
	"testing"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/services/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func outgoingFor(ctx context.Context) metadata.MD {
	md, _ := metadata.FromOutgoingContext(ctx)
	return md
}

type facadeStorage struct {
	storagev1.StorageServiceClient
	got *storagev1.DeleteFileRequest
}

func (f *facadeStorage) DeleteFile(_ context.Context, in *storagev1.DeleteFileRequest, _ ...grpc.CallOption) (*storagev1.DeleteFileResponse, error) {
	f.got = in
	return &storagev1.DeleteFileResponse{}, nil
}

func TestDeleteFileModeSendsTheModeAndReason(t *testing.T) {
	raw := &facadeStorage{}
	facade := &StorageFacade{Raw: raw}
	if _, err := facade.DeleteFileMode(context.Background(), "f-1", storagev1.DeleteMode_DELETE_MODE_HARD, "gdpr"); err != nil {
		t.Fatal(err)
	}
	if raw.got.GetFileId() != "f-1" || raw.got.GetMode() != storagev1.DeleteMode_DELETE_MODE_HARD || raw.got.GetReason() != "gdpr" {
		t.Fatalf("sent %v", raw.got)
	}
}

type facadeBroker struct {
	servicesv1.DataBrokerClient
	applied *entityv1.MigrationApplyRequest
}

func (f *facadeBroker) PlanMigration(context.Context, *entityv1.MigrationPlanRequest, ...grpc.CallOption) (*entityv1.MigrationPlanResponse, error) {
	return &entityv1.MigrationPlanResponse{RunId: "run-1"}, nil
}

func (f *facadeBroker) ApproveMigrationPlan(context.Context, *entityv1.MigrationRunRequest, ...grpc.CallOption) (*entityv1.MigrationStatusResponse, error) {
	token := "token-1"
	return &entityv1.MigrationStatusResponse{ApprovalToken: &token}, nil
}

func (f *facadeBroker) ApplyMigration(_ context.Context, in *entityv1.MigrationApplyRequest, _ ...grpc.CallOption) (*entityv1.MigrationStatusResponse, error) {
	f.applied = in
	return &entityv1.MigrationStatusResponse{RunId: in.GetRunId(), State: "APPLIED"}, nil
}

func TestApplyCurrentRunsPlanApproveApplyWithTheToken(t *testing.T) {
	broker := &facadeBroker{}
	status, err := (&AdminFacade{client: &Client{Broker: broker}}).ApplyCurrent(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	if status.GetRunId() != "run-1" || broker.applied.GetApprovalToken() != "token-1" || broker.applied.GetProjectId() != "p" {
		t.Fatalf("applied %v", broker.applied)
	}
}

func TestWithCrossTenantReadNamesTheTenantAndReason(t *testing.T) {
	ctx := WithCrossTenantRead(context.Background(), "tenant-b", "ticket 7")
	md := outgoingFor(ctx)
	if md["x-udb-cross-tenant-target"][0] != "tenant-b" || md["x-udb-cross-tenant-reason"][0] != "ticket 7" {
		t.Fatalf("headers: %v", md)
	}
}
