package udbtest_test

import (
	"context"
	"testing"

	authzentityv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/entity/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"github.com/fahara02/udb/sdk/go/udbtest"
	"github.com/fahara02/udb/sdk/go/udbtest/conformance"
)

// The shared include_deleted case on its allowed branch: a caller holding
// udb:deleted:read (the default fake client does not) reads its deleted rows.
func TestFakeIncludeDeletedContractWithTheScope(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	conformance.RunTable(t, func(t testing.TB, tenant string) *udbclient.Udb {
		key := "deleted-reader-" + tenant
		fake.AddIdentityAPIKey(key, udbtest.Identity{TenantID: tenant, UserID: "auditor",
			Scopes: []string{"udb:read", "udb:write", "udb:admin", udbtest.DeletedReadScope}})
		return fake.ClientForKey(t, key)
	})
}

// An include_deleted read stays in the caller's tenant, is recorded once with
// its row count, and is refused by name on an entity without soft delete.
func TestFakeIncludeDeletedIsTenantScopedAuditedAndRefusedWithoutSoftDelete(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{}, &authzentityv1.AccessDecisionAudit{})
	ctx := context.Background()
	tenant, other := conformance.NewID(), conformance.NewID()
	fake.AddIdentityAPIKey("reader", udbtest.Identity{TenantID: tenant, UserID: "auditor",
		Scopes: []string{"udb:read", "udb:write", udbtest.DeletedReadScope}})
	reader := fake.ClientForKey(t, "reader")
	foreign := udbclient.TableOf[*storagev1.File](fake.Client(t, other))

	theirs := &storagev1.File{FileId: conformance.NewID(), Filename: "theirs.pdf", ObjectKey: "t/" + conformance.NewID(),
		Status: storagev1.FileStatus_FILE_STATUS_PENDING}
	if err := foreign.Insert(ctx, theirs); err != nil {
		t.Fatal(err)
	}
	if err := foreign.Delete(ctx, udbclient.RowKey{"file_id": theirs.GetFileId()}); err != nil {
		t.Fatal(err)
	}
	files := udbclient.TableOf[*storagev1.File](reader)
	mine := &storagev1.File{FileId: conformance.NewID(), Filename: "mine.pdf", ObjectKey: "m/" + conformance.NewID(),
		Status: storagev1.FileStatus_FILE_STATUS_PENDING}
	if err := files.Insert(ctx, mine); err != nil {
		t.Fatal(err)
	}
	if err := files.Delete(ctx, udbclient.RowKey{"file_id": mine.GetFileId()}); err != nil {
		t.Fatal(err)
	}
	page, err := files.Select(ctx, udbclient.Filter{}, udbclient.SelectOptions{IncludeDeleted: true, Fields: []string{"file_id"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Rows) != 1 || page.Rows[0].GetFileId() != mine.GetFileId() || page.Rows[0].GetDeletedAt() == nil {
		t.Fatalf("only the caller's deleted row, marked even with a field list: %v", page.Rows)
	}
	reads := fake.DeletedReads()
	if len(reads) != 1 || reads[0].Table == "" {
		t.Fatalf("audit records = %+v, want one naming the table", reads)
	}
	want := udbtest.DeletedRead{TenantID: tenant, Actor: "auditor", MessageType: udbclient.MessageType(mine), Table: reads[0].Table, Rows: 1}
	if reads[0] != want {
		t.Fatalf("audit record = %+v, want %+v", reads[0], want)
	}

	audits := udbclient.TableOf[*authzentityv1.AccessDecisionAudit](reader)
	_, err = audits.Select(ctx, udbclient.Filter{}, udbclient.SelectOptions{IncludeDeleted: true})
	if udbclient.CodeOf(err) != udbclient.CodeInvalid {
		t.Fatalf("include_deleted on an entity without soft delete = %v, want CodeInvalid", err)
	}
	if len(fake.DeletedReads()) != 1 {
		t.Fatal("a refused read is not recorded")
	}
}
