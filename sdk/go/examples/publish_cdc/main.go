package main

import (
	"context"
	"io"
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
		Purpose:              "cdc-consume",
		CorrelationID:        "go-cdc-example",
		Scopes:               []string{"udb:cdc"},
		ServiceIdentity:      "example.service",
		ProjectID:            "default",
		ClientCatalogVersion: "1.0.0",
	})

	stream, err := client.Broker.PublishCDC(client.Context(context.Background()), &entityv1.CDCSubscriptionRequest{
		TopicPattern: "example.*",
	})
	if err != nil {
		log.Fatal(err)
	}
	for {
		envelope, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("event=%s topic=%s", envelope.EventId, envelope.Topic)
	}
}
