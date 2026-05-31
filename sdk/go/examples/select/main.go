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
		Purpose:              "workflow-read",
		CorrelationID:        "go-select-example",
		Scopes:               []string{"udb:read"},
		ServiceIdentity:      "example.service",
		ProjectID:            "default",
		ClientCatalogVersion: "1.0.0",
	})

	resp, err := client.Select(context.Background(), &entityv1.SelectRequest{
		MessageType: "example.processing.v1.ProcessingJob",
		Limit:       10,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("rows=%d", len(resp.Rows))
}
