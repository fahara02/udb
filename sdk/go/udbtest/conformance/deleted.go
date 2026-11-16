package conformance

import (
	"context"
	"testing"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
)

// runDeletedReads is the include_deleted contract. A caller holding
// udb:deleted:read sees its soft-deleted rows with their deleted_at set; any
// other caller is refused with UDB_DELETED_READ_SCOPE_MISSING. Either way a
// plain read keeps hiding the deleted row. Which branch runs depends on the
// connected identity's scopes, so the fake, embedded and live runs all apply.
func runDeletedReads(t *testing.T, connect Connect, tenant func() string) {
	t.Run("include_deleted needs udb:deleted:read and marks deleted rows", func(t *testing.T) {
		files := udbclient.TableOf[*storagev1.File](connect(t, tenant()))
		ctx := context.Background()
		live, gone := file("", NewID(), "kept.pdf"), file("", NewID(), "gone.pdf")
		for _, row := range []*storagev1.File{live, gone} {
			if err := files.Insert(ctx, row); err != nil {
				t.Fatal(err)
			}
		}
		if err := files.Delete(ctx, udbclient.RowKey{"file_id": gone.GetFileId()}); err != nil {
			t.Fatal(err)
		}
		where := udbclient.Filter{"file_id": udbclient.Filter{"$in": []any{live.GetFileId(), gone.GetFileId()}}}
		plainReadHidesTheDeletedRow := func() {
			t.Helper()
			page, err := files.Select(ctx, where, udbclient.SelectOptions{})
			if err != nil || len(page.Rows) != 1 || page.Rows[0].GetFileId() != live.GetFileId() {
				t.Fatalf("a plain read must return only the live row: %v, %v", page.Rows, err)
			}
		}
		plainReadHidesTheDeletedRow()
		page, err := files.Select(ctx, where, udbclient.SelectOptions{IncludeDeleted: true, IncludeTotal: true})
		if err != nil {
			info := udbclient.Inspect(err)
			if info.Code != udbclient.CodeScopeMissing || info.Reason != "UDB_DELETED_READ_SCOPE_MISSING" ||
				info.Missing["scope"] != "udb:deleted:read" {
				t.Fatalf("include_deleted without udb:deleted:read = %+v (%v), want UDB_DELETED_READ_SCOPE_MISSING naming the scope", info, err)
			}
			plainReadHidesTheDeletedRow()
			return
		}
		if len(page.Rows) != 2 || page.Total != 2 {
			t.Fatalf("include_deleted read: rows=%d total=%d, want both rows", len(page.Rows), page.Total)
		}
		for _, row := range page.Rows {
			deleted := row.GetDeletedAt() != nil
			if deleted != (row.GetFileId() == gone.GetFileId()) {
				t.Fatalf("row %s: deleted_at set = %v; only the deleted row carries it", row.GetFileId(), deleted)
			}
		}
		plainReadHidesTheDeletedRow()
	})
}
