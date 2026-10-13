package udbtest_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	commonv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"github.com/fahara02/udb/sdk/go/udbtest"
	"github.com/fahara02/udb/sdk/go/udbtest/conformance"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func fileDescriptors(message proto.Message) *descriptorpb.FileDescriptorSet {
	set := &descriptorpb.FileDescriptorSet{}
	seen := map[string]bool{}
	var add func(protoreflect.FileDescriptor)
	add = func(file protoreflect.FileDescriptor) {
		if seen[file.Path()] {
			return
		}
		seen[file.Path()] = true
		for i := 0; i < file.Imports().Len(); i++ {
			add(file.Imports().Get(i).FileDescriptor)
		}
		set.File = append(set.File, protodesc.ToFileDescriptorProto(file))
	}
	add(message.ProtoReflect().Descriptor().ParentFile())
	return set
}
func policyFile(t testing.TB, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policies.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func fakeFromPolicy(t testing.TB, set *descriptorpb.FileDescriptorSet, content string) *udbtest.Fake {
	t.Helper()
	fake, err := udbtest.NewFake(set, policyFile(t, content))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fake.Close() })
	return fake
}

func TestDescriptorFakeRunsTheEntireSharedContract(t *testing.T) {
	fake := fakeFromPolicy(t, fileDescriptors(&storagev1.File{}), `policies:
  - id: typed-table-test
    action: '*'
    resource: udb.core.storage.entity.v1.File
    purpose: test
    required_scopes: [udb:write, udb:read]
`)
	connect := func(t testing.TB, tenant string) *udbclient.Udb { return fake.Client(t, tenant) }
	conformance.RunTable(t, connect)
	conformance.RunTableCases(t, connect, "../../conformance/table-cases.json")
}

func TestDescriptorFakeScopesRowsToTheVerifiedProject(t *testing.T) {
	fake := fakeFromPolicy(t, fileDescriptors(&storagev1.File{}), `policies:
  - id: project-test
    action: '*'
    resource: udb.core.storage.entity.v1.File
    purpose: test
    required_scopes: [udb:write, udb:read]
`)
	tenant, projectA, projectB := conformance.NewID(), conformance.NewID(), conformance.NewID()
	for key, project := range map[string]string{"project-a": projectA, "project-b": projectB} {
		fake.AddIdentityAPIKey(key, udbtest.Identity{TenantID: tenant, ProjectID: project,
			ServiceIdentity: key, Scopes: []string{"udb:read", "udb:write"}})
	}
	owner := fake.ClientForKey(t, "project-a")
	foreign := fake.ClientForKey(t, "project-b")
	files := udbclient.TableOf[*storagev1.File](owner)
	otherFiles := udbclient.TableOf[*storagev1.File](foreign)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	row := &storagev1.File{FileId: conformance.NewID(), Filename: "project-original.pdf", ObjectKey: "project/" + conformance.NewID()}
	key := udbclient.RowKey{"file_id": row.GetFileId()}
	if err := files.Insert(ctx, row); err != nil {
		t.Fatal(err)
	}
	got, err := files.Get(ctx, key)
	if err != nil || got.GetProjectId() != projectA {
		t.Fatalf("verified project was not filled: %v, %v", got, err)
	}
	if _, err := otherFiles.Get(ctx, key); !errors.Is(err, udbclient.ErrNotFound) {
		t.Fatalf("another project read the row: %v", err)
	}
	// A nested caller predicate cannot bypass the separate verified predicate.
	filter := udbclient.Filter{"$or": []any{udbclient.Filter{"project_id": projectA}, udbclient.Filter{"file_id": row.GetFileId()}}}
	page, err := otherFiles.Select(ctx, filter, udbclient.SelectOptions{})
	if err != nil || len(page.Rows) != 0 {
		t.Fatalf("nested filter bypassed verified project: %v, %v", page.Rows, err)
	}
	if err := otherFiles.Patch(ctx, key, udbclient.Record{"filename": "must-not-change.pdf"}); !errors.Is(err, udbclient.ErrNotFound) {
		t.Fatalf("another project updated the row: %v", err)
	}
	if err := otherFiles.Delete(ctx, key); !errors.Is(err, udbclient.ErrNotFound) {
		t.Fatalf("another project deleted the row: %v", err)
	}
	// The actual conflict guard returns zero affected rows; Table surfaces that
	// as missing rather than reporting a successful write which never happened.
	if err := otherFiles.Upsert(ctx, row); !errors.Is(err, udbclient.ErrNotFound) {
		t.Fatalf("another project took over the global key: %v", err)
	}
	forged := proto.Clone(row).(*storagev1.File)
	forged.FileId, forged.ObjectKey, forged.ProjectId = conformance.NewID(), "project/"+conformance.NewID(), projectA
	if err := otherFiles.Insert(ctx, forged); udbclient.CodeOf(err) != udbclient.CodeInvalid {
		t.Fatalf("foreign project record was admitted: %v", err)
	}
	got, err = files.Get(ctx, key)
	if err != nil || got.GetFilename() != row.GetFilename() || got.GetProjectId() != projectA || got.GetDeletedAt() != nil {
		t.Fatalf("project refusals changed the target: %v, %v", got, err)
	}
}

func TestDescriptorFakeUsesNativePolicyRulesAndVerifiedIdentity(t *testing.T) {
	set := fileDescriptors(&storagev1.File{})
	tenant, other := conformance.NewID(), conformance.NewID()
	fake := fakeFromPolicy(t, set, `tenant: `+tenant+`
policies:
  - id: reader
    priority: 100
    resource: udb.core.storage.entity.v1.*
    action: Select
    purpose: test
    role: reader
    required_scopes: [udb:read]
  - id: explicit-deny
    priority: 999
    effect: deny
    subject: denied-reader
    action: Select
    required_scopes: [scope-that-nobody-has]
  - id: disabled-write
    enabled: false
    action: Upsert
  - id: missing-attributes
    action: Upsert
    conditions: {channel: trusted}
`)
	for key, identity := range map[string]udbtest.Identity{
		"reader":   {TenantID: tenant, ServiceIdentity: "reader", Roles: []string{"reader"}, Scopes: []string{"udb:read"}},
		"denied":   {TenantID: tenant, ServiceIdentity: "denied-reader", Roles: []string{"reader"}, Scopes: []string{"udb:read"}},
		"unscoped": {TenantID: tenant, ServiceIdentity: "unscoped", Roles: []string{"reader"}},
		"foreign":  {TenantID: other, ServiceIdentity: "foreign", Roles: []string{"reader"}, Scopes: []string{"udb:read"}},
	} {
		fake.AddIdentityAPIKey(key, identity)
	}
	reader := fake.ClientForKey(t, "reader")
	if _, err := udbclient.TableOf[*storagev1.File](reader).Select(context.Background(), nil, udbclient.SelectOptions{}); err != nil {
		t.Fatalf("allowed read: %v", err)
	}
	for _, key := range []string{"denied", "unscoped", "foreign"} {
		client := fake.ClientForKey(t, key)
		// Caller-asserted scopes/roles cannot repair a missing verified grant.
		ctx := udbclient.WithMetadata(context.Background(), udbclient.Metadata{Purpose: "test", Scopes: []string{"udb:*"}})
		_, err := udbclient.TableOf[*storagev1.File](client).Select(ctx, nil, udbclient.SelectOptions{})
		if info := udbclient.Inspect(err); info.Code != udbclient.CodePolicyDenied {
			t.Fatalf("%s policy result: %v (%+v)", key, err, info)
		}
	}
	_, err := reader.Data.Broker.Upsert(context.Background(), &entityv1.UpsertRequest{MessageType: udbclient.MessageType(&storagev1.File{}), RecordJson: []byte(`{"file_id":"must-not-be-written"}`)})
	if udbclient.CodeOf(err) != udbclient.CodePolicyDenied {
		t.Fatalf("disabled/conditional write bypass: %v", err)
	}
	filter, _ := structpb.NewStruct(map[string]any{"tenant_id": other})
	_, err = reader.Data.Broker.Select(context.Background(), &entityv1.SelectRequest{MessageType: udbclient.MessageType(&storagev1.File{}), Filter: filter})
	if udbclient.Inspect(err).Reason != "UDB_TENANT_MISMATCH" {
		t.Fatalf("foreign tenant filter: %v", err)
	}
	_, err = reader.Data.Broker.Select(context.Background(), &entityv1.SelectRequest{Context: &entityv1.RequestContext{TenantId: other}, MessageType: udbclient.MessageType(&storagev1.File{})})
	if udbclient.Inspect(err).Reason != "UDB_TENANT_MISMATCH" {
		t.Fatalf("foreign body tenant: %v", err)
	}
	_, err = reader.Data.Broker.ListDlqEvents(context.Background(), &entityv1.DlqListRequest{})
	if info := udbclient.Inspect(err); info.Code != udbclient.CodeScopeMissing || info.Missing["scope"] != "udb:admin" {
		t.Fatalf("DLQ operator scope: %v (%+v)", err, info)
	}
}

func TestDescriptorFakeDeniesWithNoPolicyAndRejectsMalformedDocuments(t *testing.T) {
	set := fileDescriptors(&storagev1.File{})
	fake, err := udbtest.NewFake(set, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fake.Close()
	if _, err := udbclient.TableOf[*storagev1.File](fake.Client(t, conformance.NewID())).Select(context.Background(), nil, udbclient.SelectOptions{}); udbclient.CodeOf(err) != udbclient.CodePolicyDenied {
		t.Fatalf("empty-policy data access: %v", err)
	}
	for _, content := range []string{"{}", "policies: [{effect: ignore}]", "policies: []\n---\npolicies: []", "policies: ["} {
		candidate, err := udbtest.NewFake(set, policyFile(t, content))
		if err == nil {
			_ = candidate.Close()
			t.Fatalf("invalid policy accepted: %q", content)
		}
	}
	jsonFake := fakeFromPolicy(t, set, `[{"action":"Select","resource":"*","purpose":"test"}]`)
	if _, err := udbclient.TableOf[*storagev1.File](jsonFake.Client(t, conformance.NewID())).Select(context.Background(), nil, udbclient.SelectOptions{}); err != nil {
		t.Fatalf("native JSON policy list: %v", err)
	}
}

func piiDescriptors() *descriptorpb.FileDescriptorSet {
	set := fileDescriptors(&storagev1.File{})
	messageOptions := &descriptorpb.MessageOptions{}
	proto.SetExtension(messageOptions, commonv1.E_Table, &commonv1.TableOptions{IsTable: true, TableName: "private_records"})
	field := func(name string, number int32, column *commonv1.ColumnOptions) *descriptorpb.FieldDescriptorProto {
		options := &descriptorpb.FieldOptions{}
		proto.SetExtension(options, commonv1.E_Column, column)
		return &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(number), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: options}
	}
	id := field("id", 1, &commonv1.ColumnOptions{SqlType: "TEXT", PrimaryKey: true, NotNull: true})
	tenant := field("tenant_id", 2, &commonv1.ColumnOptions{SqlType: "UUID", TenantColumn: true, NotNull: true})
	secret := field("secret", 3, &commonv1.ColumnOptions{SqlType: "TEXT"})
	proto.SetExtension(secret.Options, commonv1.E_Pii, true)
	credential := field("credential", 4, &commonv1.ColumnOptions{SqlType: "TEXT"})
	proto.SetExtension(credential.Options, commonv1.E_DbColumnSecurity, &commonv1.DbColumnSecurityOptions{OutputView: commonv1.OutputView_OUTPUT_VIEW_STORAGE_ONLY})
	set.File = append(set.File, &descriptorpb.FileDescriptorProto{Name: proto.String("udbtest/private.proto"), Package: proto.String("udbtest"), Syntax: proto.String("proto3"), Dependency: []string{"udb/core/common/v1/db.proto", "udb/core/common/v1/security.proto"}, MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("PrivateRecord"), Options: messageOptions, Field: []*descriptorpb.FieldDescriptorProto{id, tenant, secret, credential}}}})
	return set
}

func TestDescriptorFakeMasksWithoutMutatingStorageOrAcceptingPlaceholders(t *testing.T) {
	fake := fakeFromPolicy(t, piiDescriptors(), `[{"action":"*","resource":"udbtest.PrivateRecord","purpose":"test"}]`)
	tenant := conformance.NewID()
	fake.AddIdentityAPIKey("masked", udbtest.Identity{TenantID: tenant, ServiceIdentity: "masked", Scopes: []string{"udb:admin"}})
	fake.AddIdentityAPIKey("pii", udbtest.Identity{TenantID: tenant, ServiceIdentity: "pii", Scopes: []string{"udb:pii:read"}})
	masked, pii := fake.ClientForKey(t, "masked"), fake.ClientForKey(t, "pii")
	_, err := pii.Data.Broker.Upsert(context.Background(), &entityv1.UpsertRequest{MessageType: "udbtest.PrivateRecord", RecordJson: []byte(`{"id":"private-1","secret":"alice@example.invalid","credential":"private-credential"}`)})
	if err != nil {
		t.Fatal(err)
	}
	read := func(client *udbclient.Udb, fields ...string) map[string]any {
		t.Helper()
		rows, err := client.Data.Broker.Select(context.Background(), &entityv1.SelectRequest{MessageType: "udbtest.PrivateRecord", Fields: fields})
		if err != nil || len(rows.GetRecordsJson()) != 1 {
			t.Fatalf("private read: %v, %v", rows, err)
		}
		var row map[string]any
		if err := json.Unmarshal(rows.GetRecordsJson()[0], &row); err != nil {
			t.Fatal(err)
		}
		return row
	}
	if got := read(masked); got["secret"] != nil || got["credential"] != "***MASKED***" {
		t.Fatalf("implicit read did not omit PII and mask storage-only column: %v", got)
	}
	_, err = masked.Data.Broker.Select(context.Background(), &entityv1.SelectRequest{MessageType: "udbtest.PrivateRecord", Fields: []string{"secret"}})
	if info := udbclient.Inspect(err); info.Code != udbclient.CodeScopeMissing || info.Missing["scope"] != "udb:pii:read" {
		t.Fatalf("explicit PII read lacked scope refusal: %v (%+v)", err, info)
	}
	if got := read(pii, "id", "tenant_id", "secret", "credential"); got["secret"] != "alice@example.invalid" || got["credential"] != "private-credential" {
		t.Fatalf("masking changed canonical storage: %v", got)
	}
	changes, _ := structpb.NewStruct(map[string]any{"secret": "***MASKED***"})
	filter, _ := structpb.NewStruct(map[string]any{"id": "private-1"})
	_, err = masked.Data.Broker.Update(context.Background(), &entityv1.UpdateRequest{MessageType: "udbtest.PrivateRecord", Filter: filter, Changes: changes})
	if info := udbclient.Inspect(err); info.Code != udbclient.CodeRedacted || info.Column != "secret" {
		t.Fatalf("placeholder write: %v (%+v)", err, info)
	}
	if read(pii, "secret")["secret"] != "alice@example.invalid" {
		t.Fatal("placeholder refusal changed storage")
	}
}

func TestFakeRefusesUncommittedRollbackAndUnauthorizedTransactionWrites(t *testing.T) {
	fake := fakeFromPolicy(t, fileDescriptors(&storagev1.File{}), `policies:
  - id: allow-file-write
    action: Upsert
    resource: udb.core.storage.entity.v1.File
    purpose: test
  - id: read-only
    action: Select
    resource: udb.core.storage.entity.v1.File
    purpose: test
`)
	tenant := conformance.NewID()
	u := fake.Client(t, tenant)
	for _, terminal := range []string{"none", "rollback"} {
		stream, err := u.Data.Broker.BeginTx(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		row := &storagev1.File{FileId: conformance.NewID(), Filename: "must-not-commit.pdf", ObjectKey: "tx/" + conformance.NewID()}
		record, err := udbclient.EncodeRecord(row, udbclient.WriteInsert)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&entityv1.Mutation{Operation: "upsert", MessageType: udbclient.MessageType(row), RecordJson: raw}); err != nil {
			t.Fatal(err)
		}
		if terminal == "rollback" {
			if err := stream.Send(&entityv1.Mutation{Rollback: true}); err != nil {
				t.Fatal(err)
			}
		}
		_ = stream.CloseSend()
		_, err = stream.Recv()
		if terminal == "none" && err == nil {
			t.Fatal("uncommitted stream reported success")
		}
		if terminal == "rollback" && err != nil {
			t.Fatalf("explicit rollback: %v", err)
		}
		if len(fake.Rows(udbclient.MessageType(row), tenant)) != 0 {
			t.Fatalf("%s transaction wrote a row", terminal)
		}
	}
	fresh := &storagev1.File{FileId: conformance.NewID(), Filename: "prefix.pdf", ObjectKey: "tx/" + conformance.NewID()}
	err := u.Tx(context.Background(), func(tx *udbclient.TxScope) error {
		if err := tx.Upsert(fresh); err != nil {
			return err
		}
		return tx.Patch(&storagev1.File{}, udbclient.RowKey{"file_id": fresh.GetFileId()}, udbclient.Record{"filename": "policy-bypass.pdf"})
	})
	if udbclient.CodeOf(err) != udbclient.CodePolicyDenied || len(fake.Rows(udbclient.MessageType(fresh), tenant)) != 0 {
		t.Fatalf("per-mutation authorization: %v", err)
	}
}

func TestFakeFaultAndRateLimitPreserveTypedErrorsBeforeWrites(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	u := fake.Client(t, conformance.NewID())
	files := udbclient.TableOf[*storagev1.File](u)
	for _, fault := range []struct {
		code  codes.Code
		want  udbclient.ErrorCode
		delay time.Duration
	}{{codes.Unavailable, udbclient.CodeUnavailable, 0}, {codes.ResourceExhausted, udbclient.CodeRateLimited, 125 * time.Millisecond}, {codes.Aborted, udbclient.CodeConflict, 0}} {
		if err := fake.InjectFault(udbtest.Fault{Method: "Insert", Code: fault.code, RetryAfter: fault.delay}); err != nil {
			t.Fatal(err)
		}
		row := &storagev1.File{FileId: conformance.NewID(), Filename: "fault.pdf", ObjectKey: "fault/" + conformance.NewID()}
		err := files.Insert(context.Background(), row)
		if info := udbclient.Inspect(err); info.Code != fault.want || info.RetryAfter != fault.delay {
			t.Fatalf("fault: %v (%+v)", err, info)
		}
		if _, err := files.Get(context.Background(), udbclient.RowKey{"file_id": row.GetFileId()}); !errors.Is(err, udbclient.ErrNotFound) {
			t.Fatalf("fault applied a write: %v", err)
		}
		if err := files.Insert(context.Background(), row); err != nil {
			t.Fatalf("one-shot fault did not expire: %v", err)
		}
		if err := files.Delete(context.Background(), udbclient.RowKey{"file_id": row.GetFileId()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := fake.SetRateLimit(1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := files.Select(context.Background(), nil, udbclient.SelectOptions{}); err != nil {
		t.Fatal(err)
	}
	row := &storagev1.File{FileId: conformance.NewID(), Filename: "limited.pdf", ObjectKey: "rate/" + conformance.NewID()}
	if err := files.Insert(context.Background(), row); err != nil {
		t.Fatalf("Select consumed a different operation's budget: %v", err)
	}
	row.FileId, row.ObjectKey = conformance.NewID(), "rate/"+conformance.NewID()
	err := files.Insert(context.Background(), row)
	if info := udbclient.Inspect(err); info.Code != udbclient.CodeRateLimited || info.RetryAfter <= 0 {
		t.Fatalf("rate limit: %v (%+v)", err, info)
	}
}

func TestFakeInjectsCredentialRefreshFailureThroughTheRealSDK(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	if err := fake.SetTokenLifetime(time.Second); err != nil {
		t.Fatal(err)
	}
	u := fake.Client(t, conformance.NewID())
	if err := fake.InjectFault(udbtest.Fault{Method: "Authenticate", Code: codes.Unavailable, Times: 100}); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	for u.CredentialErr() == nil {
		select {
		case <-poll.C:
		case <-deadline.C:
			t.Fatal("refresh failure was not surfaced")
		}
	}
	if err := u.CredentialErr(); err == nil || udbclient.CodeOf(err) != udbclient.CodeUnavailable {
		t.Fatalf("credential failure: %v", err)
	}
	if errors.Is(u.CredentialErr(), context.Canceled) {
		t.Fatal("refresh failed only because test context ended")
	}
}
