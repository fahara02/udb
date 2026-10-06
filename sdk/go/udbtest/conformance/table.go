// Package conformance is the behaviour contract a typed store must meet,
// written once and run against both the in-memory fake (udbtest) and a live
// broker. A fake that drifts from the broker fails the same test the broker
// passes, so a service's unit tests stay honest.
package conformance

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
)

// Connect returns a client for tenant (a fresh tenant UUID per call keeps runs
// isolated).
type Connect func(t testing.TB, tenant string) *udbclient.Udb

// newUUID is a random version-4 UUID: every tenant and row of a run is distinct,
// so runs against a shared live broker never collide.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func newTenant() string { return newUUID() }

func newID(int) string { return newUUID() }

// file builds a row without a tenant: the broker fills the caller's verified
// tenant in, which is what the suite relies on (and what lets it run as the
// one live test login).
func file(_, id, name string) *storagev1.File {
	return &storagev1.File{
		FileId:    id,
		Filename:  name,
		ObjectKey: "conformance/" + id,
		Status:    storagev1.FileStatus_FILE_STATUS_PENDING,
	}
}

// Option adjusts a run for the environment it targets.
type Option func(*options)

type options struct{ singleTenant bool }

// SingleTenant marks a target that can only connect as one tenant (a live
// broker with one test login): the cross-tenant checks are skipped there and
// run against the fake instead.
func SingleTenant() Option { return func(o *options) { o.singleTenant = true } }

// RunTable runs the typed-store contract.
func RunTable(t *testing.T, connect Connect, opts ...Option) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	t.Run("get missing is ErrNotFound", func(t *testing.T) {
		u := connect(t, newTenant())
		files := udbclient.TableOf[*storagev1.File](u)
		_, err := files.Get(context.Background(), udbclient.RowKey{"file_id": newID(1)})
		if !errors.Is(err, udbclient.ErrNotFound) {
			t.Fatalf("Get of a missing row = %v, want ErrNotFound", err)
		}
	})

	t.Run("upsert then get round-trips the typed row", func(t *testing.T) {
		tenant := newTenant()
		u := connect(t, tenant)
		files := udbclient.TableOf[*storagev1.File](u)
		id := newID(2)
		in := file(tenant, id, "a.pdf")
		if err := files.Upsert(context.Background(), in); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		got, err := files.Get(context.Background(), udbclient.RowKey{"file_id": id})
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.GetFilename() != "a.pdf" || got.GetStatus() != storagev1.FileStatus_FILE_STATUS_PENDING || got.GetTenantId() == "" {
			t.Fatalf("round trip = %+v", got)
		}
	})

	t.Run("tenants never see each other's rows", func(t *testing.T) {
		if o.singleTenant {
			t.Skip("the target connects as one tenant; the fake runs this check")
		}
		a, b := newTenant(), newTenant()
		ua, ub := connect(t, a), connect(t, b)
		id := newID(3)
		if err := udbclient.TableOf[*storagev1.File](ua).Upsert(context.Background(), file(a, id, "secret.pdf")); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		_, err := udbclient.TableOf[*storagev1.File](ub).Get(context.Background(), udbclient.RowKey{"file_id": id})
		if !errors.Is(err, udbclient.ErrNotFound) {
			t.Fatalf("another tenant read the row: %v", err)
		}
		_, err = udbclient.TableOf[*storagev1.File](ub).Select(context.Background(), udbclient.Filter{"tenant_id": a}, udbclient.SelectOptions{})
		if err == nil {
			t.Fatal("naming another tenant in a filter must be refused")
		}
	})

	t.Run("patch changes only the given columns and needs the row", func(t *testing.T) {
		tenant := newTenant()
		files := udbclient.TableOf[*storagev1.File](connect(t, tenant))
		id := newID(4)
		if err := files.Upsert(context.Background(), file(tenant, id, "draft.pdf")); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if err := files.Patch(context.Background(), udbclient.RowKey{"file_id": id}, udbclient.Record{"filename": "final.pdf"}); err != nil {
			t.Fatalf("patch: %v", err)
		}
		got, _ := files.Get(context.Background(), udbclient.RowKey{"file_id": id})
		if got.GetFilename() != "final.pdf" || got.GetObjectKey() != "conformance/"+id {
			t.Fatalf("patch result = %+v", got)
		}
		err := files.Patch(context.Background(), udbclient.RowKey{"file_id": newID(5)}, udbclient.Record{"filename": "x"})
		if !errors.Is(err, udbclient.ErrNotFound) {
			t.Fatalf("patching a missing row = %v, want ErrNotFound", err)
		}
	})

	t.Run("conditional update detects a lost race", func(t *testing.T) {
		tenant := newTenant()
		files := udbclient.TableOf[*storagev1.File](connect(t, tenant))
		id := newID(6)
		if err := files.Upsert(context.Background(), file(tenant, id, "v1.pdf")); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		next := file(tenant, id, "v2.pdf")
		if err := files.UpdateIf(context.Background(), next, udbclient.Record{"filename": "v1.pdf"}); err != nil {
			t.Fatalf("update with the right expectation: %v", err)
		}
		stale := file(tenant, id, "v3.pdf")
		err := files.UpdateIf(context.Background(), stale, udbclient.Record{"filename": "v1.pdf"})
		if !errors.Is(err, udbclient.ErrConflict) {
			t.Fatalf("a stale expectation = %v, want ErrConflict", err)
		}
		got, _ := files.Get(context.Background(), udbclient.RowKey{"file_id": id})
		if got.GetFilename() != "v2.pdf" {
			t.Fatalf("a lost update was applied: %q", got.GetFilename())
		}
	})

	t.Run("update with retry applies the change once", func(t *testing.T) {
		tenant := newTenant()
		files := udbclient.TableOf[*storagev1.File](connect(t, tenant))
		id := newID(7)
		if err := files.Upsert(context.Background(), file(tenant, id, "n.pdf")); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		updated, err := files.UpdateWithRetry(context.Background(), udbclient.RowKey{"file_id": id}, 3, func(cur *storagev1.File) (*storagev1.File, error) {
			cur.Filename = "renamed-" + cur.GetFilename()
			return cur, nil
		})
		if err != nil {
			t.Fatalf("update with retry: %v", err)
		}
		if updated.GetFilename() != "renamed-n.pdf" {
			t.Fatalf("updated = %q", updated.GetFilename())
		}
	})

	t.Run("select pages, says when capped, and counts", func(t *testing.T) {
		tenant := newTenant()
		files := udbclient.TableOf[*storagev1.File](connect(t, tenant))
		for i := 0; i < 5; i++ {
			if err := files.Upsert(context.Background(), file(tenant, newID(100+i), fmt.Sprintf("p%d.pdf", i))); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
		page, err := files.Select(context.Background(), udbclient.Filter{}, udbclient.SelectOptions{Limit: 2, IncludeTotal: true})
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		if len(page.Rows) != 2 || !page.HasMore || page.Total != 5 {
			t.Fatalf("page = rows %d hasMore %v total %d", len(page.Rows), page.HasMore, page.Total)
		}
		seen := 0
		if err := files.SelectAll(context.Background(), udbclient.Filter{}, func(*storagev1.File) error { seen++; return nil }); err != nil {
			t.Fatalf("select all: %v", err)
		}
		if seen != 5 {
			t.Fatalf("SelectAll saw %d rows, want 5", seen)
		}
		n, err := files.Count(context.Background(), udbclient.Filter{"filename": udbclient.Filter{"$in": []any{"p1.pdf", "p3.pdf"}}})
		if err != nil || n != 2 {
			t.Fatalf("Count = %d, %v; want 2", n, err)
		}
	})

	t.Run("a transaction commits every write or none", func(t *testing.T) {
		tenant := newTenant()
		u := connect(t, tenant)
		files := udbclient.TableOf[*storagev1.File](u)
		existing := newID(9)
		if err := files.Upsert(context.Background(), file(tenant, existing, "v1.pdf")); err != nil {
			t.Fatalf("seed: %v", err)
		}
		fresh := newID(10)
		err := u.Tx(context.Background(), func(tx *udbclient.TxScope) error {
			if err := tx.Upsert(file(tenant, fresh, "new.pdf")); err != nil {
				return err
			}
			return tx.UpdateIf(file(tenant, existing, "v2.pdf"), udbclient.Record{"filename": "stale.pdf"})
		})
		if !errors.Is(err, udbclient.ErrConflict) {
			t.Fatalf("a transaction with a lost CAS = %v, want ErrConflict", err)
		}
		if _, err := files.Get(context.Background(), udbclient.RowKey{"file_id": fresh}); !errors.Is(err, udbclient.ErrNotFound) {
			t.Fatalf("the failed transaction's other write was kept: %v", err)
		}

		err = u.Tx(context.Background(), func(tx *udbclient.TxScope) error {
			if err := tx.Upsert(file(tenant, fresh, "new.pdf")); err != nil {
				return err
			}
			return tx.UpdateIf(file(tenant, existing, "v2.pdf"), udbclient.Record{"filename": "v1.pdf"})
		})
		if err != nil {
			t.Fatalf("a transaction whose CAS holds: %v", err)
		}
		if got, err := files.Get(context.Background(), udbclient.RowKey{"file_id": existing}); err != nil || got.GetFilename() != "v2.pdf" {
			t.Fatalf("committed update = %v, %v", got, err)
		}
		if _, err := files.Get(context.Background(), udbclient.RowKey{"file_id": fresh}); err != nil {
			t.Fatalf("committed insert: %v", err)
		}
	})

	t.Run("delete needs the row", func(t *testing.T) {
		tenant := newTenant()
		files := udbclient.TableOf[*storagev1.File](connect(t, tenant))
		id := newID(8)
		if err := files.Upsert(context.Background(), file(tenant, id, "gone.pdf")); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if err := files.Delete(context.Background(), udbclient.RowKey{"file_id": id}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if err := files.Delete(context.Background(), udbclient.RowKey{"file_id": id}); !errors.Is(err, udbclient.ErrNotFound) {
			t.Fatalf("deleting it again = %v, want ErrNotFound", err)
		}
	})
}
