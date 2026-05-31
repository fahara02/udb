package main

import (
	"context"
	"log"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := udbclient.New(conn, udbclient.Metadata{
		TenantID:             "tenant-1",
		Purpose:              "workflow-write",
		CorrelationID:        "go-upsert-example",
		Scopes:               []string{"udb:write"},
		ServiceIdentity:      "example.service",
		ProjectID:            "default",
		ClientCatalogVersion: "1.0.0",
	})

	_, err = client.Upsert(context.Background(), &entityv1.UpsertRequest{
		MessageType:    "example.pipeline.v1.PipelineRun",
		RecordJson:     []byte(`{"tenant_id":"tenant-1","run_id":"run-1","status":"queued"}`),
		ConflictFields: []string{"run_id"},
		ReturnRecord:   true,
	})
	if err != nil {
		log.Fatal(err)
	}
}
