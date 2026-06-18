package udbclient

import (
	"context"
	"reflect"
	"testing"

	authzentv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/entity/v1"
	authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/services/v1"
	"google.golang.org/grpc"
)

// ── R2.3 public-API parity aliases (naming contract R2.0) ────────────────────
//
// These tests pin the canonical simple-client names added in R2.3 to the exact
// declared RPC: AllowRole = one CreatePolicyRule, BindRole = one AssignRole,
// DownloadFile = one GetDownloadUrl, LoginSession = no RPC. The mock transports
// record the ordered RPC sequence so the no-hidden-List/Get gate is enforced.

// TestAuthzFacadeAllowRoleOneRPC asserts u.Authz.AllowRole emits EXACTLY one
// CreatePolicyRule with effect=ALLOW and the role→subject / resource→object /
// metadata-seeded fields, no probe RPC.
func TestAuthzFacadeAllowRoleOneRPC(t *testing.T) {
	var seq []string
	fake := &fakeAuthz{rpcSeq: &seq}
	authc := newTestAuthClient(fake)
	f := &AuthzFacade{client: authc}

	if _, err := f.AllowRole(context.Background(), "editor", &authzv1.ResourceRef{MessageType: "udb.Invoice"}, "read"); err != nil {
		t.Fatalf("AllowRole: %v", err)
	}
	if !reflect.DeepEqual(seq, []string{"CreatePolicyRule"}) {
		t.Fatalf("AllowRole must emit EXACTLY one CreatePolicyRule (no hidden List/Get): %v", seq)
	}
	req := fake.lastPolicyRule
	if req.GetSubject() != "editor" {
		t.Fatalf("role -> subject mismatch: %q", req.GetSubject())
	}
	if req.GetObject() != "udb.Invoice" {
		t.Fatalf("resource -> object mismatch (message_type precedence): %q", req.GetObject())
	}
	if req.GetAction() != "read" {
		t.Fatalf("action mismatch: %q", req.GetAction())
	}
	if req.GetEffect() != authzentv1.PolicyEffect_POLICY_EFFECT_ALLOW {
		t.Fatalf("effect must default to ALLOW: %v", req.GetEffect())
	}
	if req.GetTenantId() != "tenant-1" || req.GetProjectId() != "proj-9" {
		t.Fatalf("tenant/project not seeded from metadata: %+v", req)
	}
	if req.GetDomain() != "tenant-1" {
		t.Fatalf("domain not seeded from tenant: %q", req.GetDomain())
	}
	if req.GetCreatedBy() != "user-1" {
		t.Fatalf("created_by not seeded from metadata user: %q", req.GetCreatedBy())
	}
}

// TestAuthzFacadeBindRoleOneRPC asserts u.Authz.BindRole emits EXACTLY one
// AssignRole with subject sent as user_id + principal_id and role as role_id.
func TestAuthzFacadeBindRoleOneRPC(t *testing.T) {
	var seq []string
	fake := &fakeAuthz{rpcSeq: &seq}
	authc := newTestAuthClient(fake)
	f := &AuthzFacade{client: authc}

	if _, err := f.BindRole(context.Background(), "user-7", "editor"); err != nil {
		t.Fatalf("BindRole: %v", err)
	}
	if !reflect.DeepEqual(seq, []string{"AssignRole"}) {
		t.Fatalf("BindRole must emit EXACTLY one AssignRole (no hidden List/Get): %v", seq)
	}
	req := fake.lastAssignRole
	if req.GetUserId() != "user-7" || req.GetPrincipalId() != "user-7" {
		t.Fatalf("subject must be sent as both user_id and principal_id: %+v", req)
	}
	if req.GetRoleId() != "editor" {
		t.Fatalf("role -> role_id mismatch: %q", req.GetRoleId())
	}
	if req.GetDomain() != "tenant-1" || req.GetTenantId() != "tenant-1" || req.GetProjectId() != "proj-9" {
		t.Fatalf("domain/tenant/project not seeded from metadata: %+v", req)
	}
	if req.GetAssignedBy() != "user-1" {
		t.Fatalf("assigned_by not seeded from metadata user: %q", req.GetAssignedBy())
	}
}

// TestAuthClientAllowBindServiceIdentityActor asserts the actor field falls back
// to ServiceIdentity when UserID is empty.
func TestAuthClientAllowBindServiceIdentityActor(t *testing.T) {
	fake := &fakeAuthz{}
	authc := &AuthClient{Authz: fake, Meta: Metadata{TenantID: "t", ServiceIdentity: "svc-x"}}
	if _, err := authc.AllowRole(context.Background(), "r", &authzv1.ResourceRef{Table: "t1"}, "read"); err != nil {
		t.Fatalf("AllowRole: %v", err)
	}
	if fake.lastPolicyRule.GetCreatedBy() != "svc-x" {
		t.Fatalf("created_by should fall back to service identity: %q", fake.lastPolicyRule.GetCreatedBy())
	}
	if fake.lastPolicyRule.GetObject() != "t1" {
		t.Fatalf("table precedence not used for object: %q", fake.lastPolicyRule.GetObject())
	}
	if _, err := authc.BindRole(context.Background(), "s", "r"); err != nil {
		t.Fatalf("BindRole: %v", err)
	}
	if fake.lastAssignRole.GetAssignedBy() != "svc-x" {
		t.Fatalf("assigned_by should fall back to service identity: %q", fake.lastAssignRole.GetAssignedBy())
	}
}

// fakeStorage records GetDownloadUrl so DownloadFile can be proven to emit
// exactly one GetDownloadUrl RPC.
type fakeStorage struct {
	storagev1.StorageServiceClient
	seq        *[]string
	lastDLReq  *storagev1.GetDownloadUrlRequest
	downloadOK string
}

func (f *fakeStorage) GetDownloadUrl(_ context.Context, in *storagev1.GetDownloadUrlRequest, _ ...grpc.CallOption) (*storagev1.GetDownloadUrlResponse, error) {
	*f.seq = append(*f.seq, "GetDownloadUrl")
	f.lastDLReq = in
	return &storagev1.GetDownloadUrlResponse{DownloadUrl: f.downloadOK}, nil
}

// TestStorageDownloadFileOneRPC asserts Storage.DownloadFile emits EXACTLY one
// GetDownloadUrl RPC (no GetFile probe) with the file id + expiry threaded and
// tenant seeded from metadata.
func TestStorageDownloadFileOneRPC(t *testing.T) {
	var seq []string
	fs := &fakeStorage{seq: &seq, downloadOK: "https://example/dl"}
	sf := &StorageFacade{Raw: fs, meta: Metadata{TenantID: "tenant-1"}}

	resp, err := sf.DownloadFile(context.Background(), "file-42", 15)
	if err != nil {
		t.Fatalf("DownloadFile: %v", err)
	}
	if !reflect.DeepEqual(seq, []string{"GetDownloadUrl"}) {
		t.Fatalf("DownloadFile must emit EXACTLY one GetDownloadUrl (no GetFile probe): %v", seq)
	}
	if fs.lastDLReq.GetFileId() != "file-42" || fs.lastDLReq.GetExpiresInMinutes() != 15 {
		t.Fatalf("file id / expiry not threaded: %+v", fs.lastDLReq)
	}
	if fs.lastDLReq.GetTenantId() != "tenant-1" {
		t.Fatalf("tenant not seeded from metadata: %q", fs.lastDLReq.GetTenantId())
	}
	if resp.GetDownloadUrl() != "https://example/dl" {
		t.Fatalf("response not returned: %q", resp.GetDownloadUrl())
	}
}

// TestAuthLoginSessionNoRPC asserts LoginSession constructs a TokenManager bound
// to the AuthClient and issues NO RPC by itself.
func TestAuthLoginSessionNoRPC(t *testing.T) {
	var seq []string
	fa := &fakeAuthn{seq: &seq}
	c := &AuthClient{Authn: fa, Meta: Metadata{TenantID: "t-1"}}

	tm := c.LoginSession(nil)
	if tm == nil {
		t.Fatal("LoginSession returned nil manager")
	}
	if tm.auth != c {
		t.Fatal("LoginSession did not bind the AuthClient")
	}
	if len(seq) != 0 {
		t.Fatalf("LoginSession must issue NO RPC by itself: %v", seq)
	}
	// A custom store is honored.
	store := &MemoryTokenStore{}
	if tm2 := c.LoginSession(store); tm2.store != store {
		t.Fatal("LoginSession did not honor the supplied store")
	}
}

// TestConnectAliasMatchesNewUdb asserts Connect is a behavior-identical alias of
// NewUdb (both reject an empty Target identically, no dial).
func TestConnectAliasMatchesNewUdb(t *testing.T) {
	_, errConnect := Connect(context.Background(), Config{})
	_, errNew := NewUdb(context.Background(), Config{})
	if errConnect == nil || errNew == nil {
		t.Fatal("both Connect and NewUdb must reject empty Target")
	}
	if errConnect.Error() != errNew.Error() {
		t.Fatalf("Connect and NewUdb must error identically: %q vs %q", errConnect, errNew)
	}
}
