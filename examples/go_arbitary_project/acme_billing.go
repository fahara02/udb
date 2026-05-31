package main

import (
	"context"
	"log"
	"os"

	acmebillingv1 "github.com/fahara02/udb/examples/go_arbitary_project/gen/go/acme/billing/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	targetDefault = "localhost:50051"
	projectID     = "default"
	messageType   = "acme.billing.v1.Product"
	collection    = "acme_products"
	bucket        = "acme-billing-documents"
)

func main() {
	target := os.Getenv("UDB_TARGET")
	if target == "" {
		target = targetDefault
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := udbclient.New(conn, udbclient.Metadata{
		TenantID:      "acme-org-1",
		UserID:        "sdk-go-example",
		Purpose:       "billing.example",
		CorrelationID: "go-acme-billing-example",
		Scopes: []string{
			"udb:read",
			"udb:write",
			"udb:admin",
			"udb:vector:read",
			"udb:vector:write",
			"udb:object:presign",
			"udb:stream",
		},
		ServiceIdentity:      "examples.go",
		ProjectID:            projectID,
		ClientCatalogVersion: udbclient.ProtocolVersion,
	})

	ctx := context.Background()
	if _, err := client.Upsert(ctx, acmeProductUpsert()); err != nil {
		log.Fatal(err)
	}
	rows, err := client.Select(ctx, acmeProductSelect())
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("relational selected rows=%d", len(rows.RecordsJson)+len(rows.Rows))

	if _, err := client.Broker.VectorUpsert(client.Context(ctx), acmeVectorUpsert()); err != nil {
		log.Fatal(err)
	}
	vectorRows, err := client.Broker.VectorSearch(client.Context(ctx), acmeVectorSearch())
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("vector search points=%d", len(vectorRows.Points))

	if _, err := putObject(ctx, client); err != nil {
		log.Fatal(err)
	}
	url, err := client.Broker.GeneratePresignedUrl(client.Context(ctx), &entityv1.UrlRequest{
		Bucket:      bucket,
		ObjectKey:   "invoices/sdk/go/smoke.txt",
		Method:      "GET",
		TtlSeconds:  300,
		ContentType: "text/plain",
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("object presigned url expires_at=%d", url.ExpiresAtUnix)
}

func acmeProductUpsert() *entityv1.UpsertRequest {
	product := &acmebillingv1.Product{
		ProductId:   "prod-sdk-go-001",
		Name:        "SDK smoke test product",
		Description: "Inserted by the Go UDB SDK example",
		PriceCents:  12900,
		Sku:         "SDK-GO-001",
	}
	record, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(product)
	if err != nil {
		panic(err)
	}
	return &entityv1.UpsertRequest{
		MessageType:    messageType,
		RecordJson:     record,
		ConflictFields: []string{"product_id"},
		ReturnRecord:   true,
		IdempotencyKey: "go-product-sdk-001",
	}
}

func acmeProductSelect() *entityv1.SelectRequest {
	return &entityv1.SelectRequest{
		MessageType: messageType,
		Limit:       10,
	}
}

func acmeVectorUpsert() *entityv1.VectorUpsertRequest {
	payload, err := structpb.NewStruct(map[string]any{
		"product_id": "prod-sdk-go-001",
		"name":       "Go SDK vector point",
	})
	if err != nil {
		panic(err)
	}
	return &entityv1.VectorUpsertRequest{
		Collection: collection,
		Points: []*entityv1.VectorPointMutation{
			{
				Id:      "11111111-1111-4111-8111-111111111111",
				Vector:  makeVector(768),
				Payload: payload,
			},
		},
		IdempotencyKey: "go-vector-sdk-001",
	}
}

func acmeVectorSearch() *entityv1.VectorSearchRequest {
	return &entityv1.VectorSearchRequest{
		Collection:  collection,
		Vector:      makeVector(768),
		Limit:       3,
		WithPayload: true,
	}
}

func putObject(ctx context.Context, client *udbclient.Client) (*entityv1.MutationResponse, error) {
	stream, err := client.Broker.PutObject(client.Context(ctx))
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&entityv1.Chunk{
		Bucket:         bucket,
		ObjectKey:      "invoices/sdk/go/smoke.txt",
		Data:           []byte("hello from the Go UDB SDK example\n"),
		FinalChunk:     true,
		ContentType:    "text/plain",
		IdempotencyKey: "go-object-sdk-001",
	}); err != nil {
		return nil, err
	}
	return stream.CloseAndRecv()
}

func makeVector(dim int) []float32 {
	vector := make([]float32, dim)
	for i := range vector {
		vector[i] = float32((i%17)+1) / 17.0
	}
	return vector
}
