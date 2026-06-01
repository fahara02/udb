package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

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

	runID := time.Now().UnixNano()
	productID := fmt.Sprintf("prod-sdk-go-crud-%d", runID)

	if _, err := client.Broker.Delete(client.Context(ctx), acmeProductDelete(productID, fmt.Sprintf("go-product-cleanup-%d", runID))); err != nil {
		log.Fatal(err)
	}
	if _, err := client.Upsert(ctx, acmeProductUpsert(productID, "SDK smoke test product", fmt.Sprintf("go-product-create-%d", runID))); err != nil {
		log.Fatal(err)
	}
	rows, err := client.Select(ctx, acmeProductSelect(productID))
	if err != nil {
		log.Fatal(err)
	}
	if rowCount(rows) != 1 {
		log.Fatalf("relational create/read expected 1 row, got %d", rowCount(rows))
	}
	log.Printf("relational create/read rows=%d", rowCount(rows))

	if _, err := client.Upsert(ctx, acmeProductUpsert(productID, "SDK smoke test product updated", fmt.Sprintf("go-product-update-%d", runID))); err != nil {
		log.Fatal(err)
	}
	rows, err = client.Select(ctx, acmeProductSelect(productID))
	if err != nil {
		log.Fatal(err)
	}
	if !recordsContain(rows, "SDK smoke test product updated") {
		log.Fatal("relational update was not visible in select response")
	}
	log.Printf("relational update verified rows=%d", rowCount(rows))

	if _, err := client.Broker.Delete(client.Context(ctx), acmeProductDelete(productID, fmt.Sprintf("go-product-delete-%d", runID))); err != nil {
		log.Fatal(err)
	}
	rows, err = client.Select(ctx, acmeProductSelect(productID))
	if err != nil {
		log.Fatal(err)
	}
	if rowCount(rows) != 0 {
		log.Fatalf("relational delete expected 0 rows, got %d", rowCount(rows))
	}
	log.Printf("relational delete verified rows=%d", rowCount(rows))

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
	objectBytes, err := getObject(ctx, client)
	if err != nil {
		log.Fatal(err)
	}
	if !strings.Contains(string(objectBytes), "hello from the Go UDB SDK example") {
		log.Fatal("object readback did not match uploaded content")
	}
	log.Printf("object readback bytes=%d", len(objectBytes))
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

func acmeProductUpsert(productID, name, idempotencyKey string) *entityv1.UpsertRequest {
	product := &acmebillingv1.Product{
		ProductId:   productID,
		Name:        name,
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
		IdempotencyKey: idempotencyKey,
	}
}

func acmeProductSelect(productID string) *entityv1.SelectRequest {
	filter, err := structpb.NewStruct(map[string]any{
		"product_id": productID,
	})
	if err != nil {
		panic(err)
	}
	return &entityv1.SelectRequest{
		MessageType: messageType,
		Filter:      filter,
		Limit:       10,
	}
}

func acmeProductDelete(productID, idempotencyKey string) *entityv1.DeleteRequest {
	filter, err := structpb.NewStruct(map[string]any{
		"product_id": productID,
	})
	if err != nil {
		panic(err)
	}
	return &entityv1.DeleteRequest{
		MessageType:    messageType,
		Filter:         filter,
		IdempotencyKey: idempotencyKey,
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

func getObject(ctx context.Context, client *udbclient.Client) ([]byte, error) {
	stream, err := client.Broker.GetObject(client.Context(ctx), &entityv1.ObjectRequest{
		Bucket:    bucket,
		ObjectKey: "invoices/sdk/go/smoke.txt",
	})
	if err != nil {
		return nil, err
	}
	var out []byte
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, chunk.GetData()...)
	}
}

func makeVector(dim int) []float32 {
	vector := make([]float32, dim)
	for i := range vector {
		vector[i] = float32((i%17)+1) / 17.0
	}
	return vector
}

func rowCount(rows *entityv1.RecordSet) int {
	if len(rows.GetRecordsJson()) > 0 {
		return len(rows.GetRecordsJson())
	}
	return len(rows.GetRows())
}

func recordsContain(rows *entityv1.RecordSet, needle string) bool {
	for _, record := range rows.GetRecordsJson() {
		if strings.Contains(string(record), needle) {
			return true
		}
	}
	for _, row := range rows.GetRows() {
		if strings.Contains(row.String(), needle) {
			return true
		}
	}
	return false
}
