// Command native-go is a progressive, simplest→advanced tour of UDB's native
// control-plane services from Go: authentication (users, API keys), authorization
// (RBAC roles + policies + access checks), and the Stage-2 native database
// fast-path grant — all over the `udbclient` SDK wrappers.
//
// Prerequisites — a running UDB broker with native auth enabled. The repo's
// integration stack is the easy path:
//
//	docker compose -f docker-compose.integration.yml up -d --wait postgres kafka redis
//	docker compose -f docker-compose.integration.yml --profile broker up -d --wait udb
//
// Then, from this folder:
//
//	go run .
//
// The broker authorizes the admin RPCs (create user/role/policy); the
// integration `udb` service sets UDB_ABAC_DEFAULT_ALLOW=true so this example
// runs without first bootstrapping policies for the example's own identity.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	apikeyv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/services/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const brokerAddr = "localhost:50051"

func main() {
	conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", brokerAddr, err)
	}
	defer conn.Close()

	// The caller identity is attached to every request as gRPC metadata. Broad
	// scopes + a control-plane purpose so the admin RPCs authorize cleanly.
	auth := udbclient.NewAuthClient(conn, udbclient.Metadata{
		TenantID:             "acme",
		ProjectID:            "billing",
		Purpose:              "control-plane",
		ServiceIdentity:      "examples.native-go",
		CorrelationID:        "native-go-example",
		Scopes:               []string{"udb:*"},
		ClientCatalogVersion: "1.0.0",
	})
	apiKeys := apikeyv1.NewApiKeyServiceClient(conn)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	// ── Step 1 (simplest): register a native user ────────────────────────────
	userID, username := createUser(ctx, auth, suffix)
	fmt.Printf("1) registered user %s (%s)\n", userID, username)

	// ── Step 2: define authorization — RBAC role → assignment → allow policy ─
	roleID, roleCode := createRole(ctx, auth, userID, suffix)
	assignRole(ctx, auth, userID, roleID)
	putAllowPolicy(ctx, auth, roleCode)
	fmt.Printf("2) role %q assigned to user; allow policy on invoice/data.select added\n", roleCode)

	// ── Step 3: the everyday authorization call ──────────────────────────────
	fmt.Printf("3) check data.select on invoice → allowed=%v\n", checkAccess(ctx, auth, userID, "invoice", "data.select"))
	fmt.Printf("   check data.delete on invoice → allowed=%v (no policy grants it)\n", checkAccess(ctx, auth, userID, "invoice", "data.delete"))

	// ── Step 4: machine credentials — mint an API key, then authenticate it ──
	plainKey := createAPIKey(ctx, auth, apiKeys, userID)
	if resp, err := auth.AuthenticateAPIKey(ctx, plainKey); err != nil {
		log.Printf("4) authenticate api key: %v", err)
	} else {
		p := resp.GetPrincipal()
		fmt.Printf("4) api key authenticated → principal user_id=%s scopes=%v\n", p.GetUserId(), p.GetScopes())
	}
	// Print the minted key (dev only) so the PHP/TS/Python consumer examples can
	// authenticate it: export UDB_API_KEY=<this>.
	fmt.Printf("   minted dev API key → export UDB_API_KEY=%s\n", plainKey)

	// ── Step 5 (advanced): Stage-2 native DB fast-path grant ─────────────────
	// GetNativeAccess runs the same authz decision and, when allowed + configured,
	// returns a short-lived restricted DSN + the SET LOCAL session variables the
	// caller applies per transaction so broker RLS still holds on a direct conn.
	grant, err := auth.NativeAccess(ctx, &authzv1.ResourceRef{ResourceName: "invoice", MessageType: "invoice"}, "data.select", "control-plane")
	switch {
	case err != nil:
		fmt.Printf("5) native access denied or unavailable: %v\n", err)
	case grant == nil:
		fmt.Printf("5) access allowed, but no native grant minted (server native-access not configured)\n")
	default:
		fmt.Printf("5) native grant: role=%s session_vars=%d (open *sql.DB on grant.Dsn + udbclient.WithNativeTx)\n",
			grant.GetRole(), len(grant.GetSessionVariables()))
	}
}

func createUser(ctx context.Context, auth *udbclient.AuthClient, suffix string) (userID, username string) {
	resp, err := auth.Authn.CreateUser(auth.Context(ctx), &authnv1.CreateUserRequest{
		Username:  "alice_" + suffix,
		Email:     "alice_" + suffix + "@example.com",
		Password:  "CorrectHorse1!",
		TenantId:  auth.Meta.TenantID,
		FullName:  "Alice Example",
		ProjectId: auth.Meta.ProjectID,
	})
	if err != nil {
		log.Fatalf("create user: %v", err)
	}
	u := resp.GetUser()
	return u.GetUserId(), u.GetUsername()
}

func createRole(ctx context.Context, auth *udbclient.AuthClient, createdBy, suffix string) (roleID, roleCode string) {
	resp, err := auth.Authz.CreateRole(auth.Context(ctx), &authzv1.CreateRoleRequest{
		Name:      "Reader " + suffix,
		RoleCode:  "reader_" + suffix,
		CreatedBy: createdBy,
		Domain:    auth.Meta.TenantID,
		TenantId:  auth.Meta.TenantID,
		ProjectId: auth.Meta.ProjectID,
	})
	if err != nil {
		log.Fatalf("create role: %v", err)
	}
	r := resp.GetRole()
	return r.GetRoleId(), r.GetRoleCode()
}

func assignRole(ctx context.Context, auth *udbclient.AuthClient, userID, roleID string) {
	if _, err := auth.Authz.AssignRole(auth.Context(ctx), &authzv1.AssignRoleRequest{
		UserId:     userID,
		RoleId:     roleID,
		Domain:     auth.Meta.TenantID,
		AssignedBy: userID,
		TenantId:   auth.Meta.TenantID,
		ProjectId:  auth.Meta.ProjectID,
	}); err != nil {
		log.Fatalf("assign role: %v", err)
	}
}

func putAllowPolicy(ctx context.Context, auth *udbclient.AuthClient, roleCode string) {
	if _, err := auth.Authz.PutAuthzPolicy(auth.Context(ctx), &authzv1.PutAuthzPolicyRequest{
		Policy: &authzv1.AuthzPolicyRecord{
			Id:       "policy-" + roleCode,
			Enabled:  true,
			Effect:   "allow",
			Tenant:   auth.Meta.TenantID,
			Project:  auth.Meta.ProjectID,
			Role:     roleCode,
			Action:   "data.select",
			Resource: "invoice",
		},
	}); err != nil {
		log.Fatalf("put policy: %v", err)
	}
}

func checkAccess(ctx context.Context, auth *udbclient.AuthClient, userID, object, action string) bool {
	resp, err := auth.CheckAccess(ctx, &authzv1.CheckAccessRequest{
		UserId:    userID,
		Domain:    auth.Meta.TenantID,
		TenantId:  auth.Meta.TenantID,
		ProjectId: auth.Meta.ProjectID,
		Object:    object,
		Action:    action,
	})
	if err != nil {
		log.Fatalf("check access: %v", err)
	}
	return resp.GetAllowed()
}

func createAPIKey(ctx context.Context, auth *udbclient.AuthClient, apiKeys apikeyv1.ApiKeyServiceClient, ownerID string) string {
	resp, err := apiKeys.CreateApiKey(auth.Context(ctx), &apikeyv1.CreateApiKeyRequest{
		Name:    "native-go-example-key",
		OwnerId: ownerID,
		Scopes:  []string{"data:read"},
	})
	if err != nil {
		log.Fatalf("create api key: %v", err)
	}
	return resp.GetPlainKey()
}
