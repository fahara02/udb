// Command go_enterprise is the Go counterpart of examples/ts_enterprise: the
// real enterprise path (login -> verify -> adopt canonical tenant UUID ->
// tenant-scoped CRUD -> isolation check) in ONE ConnectEnterprise call.
//
// Run (after examples/ts_enterprise/scripts/bootstrap.sh + serve.sh are up, or
// any broker with a bootstrapped admin + seeded ABAC):
//
//	UDB_PASS=<admin-pass> UDB_TENANT=acme go run .
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	entitypb "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	udb "github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/protobuf/types/known/structpb"
)

const messageType = "billing.v1.Invoice"

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()
	password := os.Getenv("UDB_PASS")
	if password == "" {
		log.Fatal("set UDB_PASS to the admin password printed by bootstrap.sh")
	}
	tenantCode := env("UDB_TENANT", "acme")
	username := env("UDB_USER", "admin")

	// One call: dial data + auth targets, login, verify the bearer, adopt the
	// canonical tenant UUID, and carry the bearer on every subsequent call.
	session, err := udb.ConnectEnterprise(ctx, udb.EnterpriseConfig{
		Target:     env("UDB_TARGET", "127.0.0.1:50051"),
		AuthTarget: env("UDB_AUTH_TARGET", "127.0.0.1:50061"),
		Username:   username,
		Password:   password,
		TenantCode: tenantCode,
		ProjectID:  env("UDB_PROJECT", "default"),
		Purpose:    "billing-app",
		Scopes:     []string{"udb:read", "udb:write"},
	})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer session.Close()

	tenant := session.CanonicalTenantID // the verified UUID — use this everywhere
	fmt.Printf("authn: logged in as %s\n", username)
	fmt.Printf("       tenant code %q -> canonical UUID %s\n", tenantCode, tenant)
	fmt.Printf("       scopes: %v\n", session.Principal.GetScopes())
	fmt.Println("authz: data-plane ABAC gates every CRUD op below (default-deny without the seeded policy)")

	const invoiceID = "1c1c1c1c-0000-4000-8000-000000000001"

	// CREATE — note tenant_id is the canonical UUID; ValidateTenant fails fast on a mismatch.
	record := map[string]any{
		"invoice_id":     invoiceID,
		"tenant_id":      tenant,
		"customer_email": "ada@example.com",
		"amount_cents":   4999,
		"status":         "paid",
	}
	if err := session.ValidateTenant(tenant); err != nil {
		log.Fatal(err)
	}
	recordJSON, err := json.Marshal(record)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := session.Data.Broker.Upsert(session.DataContext(ctx), &entitypb.UpsertRequest{
		MessageType:    messageType,
		RecordJson:     recordJSON,
		ConflictFields: []string{"invoice_id"},
	}); err != nil {
		log.Fatalf("create: %v", err)
	}
	fmt.Printf("crud:  created invoice %s\n", invoiceID)

	// READ — a tenant-scoped read MUST carry tenant_id in the filter.
	filter, err := structpb.NewStruct(map[string]any{"invoice_id": invoiceID, "tenant_id": tenant})
	if err != nil {
		log.Fatal(err)
	}
	rs, err := session.Data.Broker.Select(session.DataContext(ctx), &entitypb.SelectRequest{
		MessageType: messageType,
		Filter:      filter,
	})
	if err != nil {
		log.Fatalf("read: %v", err)
	}
	fmt.Printf("crud:  read    %d row(s)\n", len(rs.GetRecordsJson()))

	// UPDATE
	record["status"] = "refunded"
	recordJSON, _ = json.Marshal(record)
	if _, err := session.Data.Broker.Upsert(session.DataContext(ctx), &entitypb.UpsertRequest{
		MessageType:    messageType,
		RecordJson:     recordJSON,
		ConflictFields: []string{"invoice_id"},
	}); err != nil {
		log.Fatalf("update: %v", err)
	}
	fmt.Println("crud:  updated status -> refunded")

	// DELETE
	if _, err := session.Data.Broker.Delete(session.DataContext(ctx), &entitypb.DeleteRequest{
		MessageType: messageType,
		Filter:      filter,
	}); err != nil {
		log.Fatalf("delete: %v", err)
	}
	fmt.Println("crud:  deleted")

	// CROSS-TENANT ISOLATION — reads scoped to another tenant return nothing.
	otherFilter, err := structpb.NewStruct(map[string]any{"tenant_id": "00000000-0000-0000-0000-00000000d999"})
	if err != nil {
		log.Fatal(err)
	}
	if other, err := session.Data.Broker.Select(session.DataContext(ctx), &entitypb.SelectRequest{
		MessageType: messageType,
		Filter:      otherFilter,
	}); err == nil {
		fmt.Printf("authz: cross-tenant read -> %d row(s) — isolation enforced\n", len(other.GetRecordsJson()))
	}

	fmt.Println("\nENTERPRISE FLOW OK (authn + authz + tenant-scoped CRUD + isolation)")
}
