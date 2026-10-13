// Package conformance is the behaviour contract a typed store must meet,
// written once and run against the in-memory fake, embedded and live
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

// NewID is a random version-4 UUID: every tenant and row of a run is distinct,
// so runs against a shared live broker never collide.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func newTenant() string { return NewID() }

func newID(int) string { return NewID() }

// file builds a row without a tenant: the broker fills the caller's verified
// tenant in, which is what the suite relies on for each verified live login.
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

type options struct{ tenants [2]string }

// Tenants selects two actually provisioned canonical tenant UUIDs. Without
// this option the fake provisions fresh tenants. Cross-tenant cases never skip.
func Tenants(first, second string) Option {
	return func(o *options) { o.tenants = [2]string{first, second} }
}

// RunTable runs the typed-store contract.
func RunTable(t *testing.T, connect Connect, opts ...Option) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if (o.tenants[0] != "" || o.tenants[1] != "") && (o.tenants[0] == "" || o.tenants[1] == "" || o.tenants[0] == o.tenants[1]) {
		t.Fatal("conformance requires two distinct provisioned tenants")
	}
	newTenant := func() string {
		if o.tenants[0] != "" {
			return o.tenants[0]
		}
		return NewID()
	}
	runConstraints(t, connect, newTenant)
	runQueries(t, connect, newTenant)
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

	t.Run("insert refuses a duplicate without changing the stored row", func(t *testing.T) {
		tenant := newTenant()
		files := udbclient.TableOf[*storagev1.File](connect(t, tenant))
		id := NewID()
		if err := files.Insert(context.Background(), file(tenant, id, "original.pdf")); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if err := files.Insert(context.Background(), file(tenant, id, "replacement.pdf")); !udbclient.IsUnique(err) {
			t.Fatalf("duplicate insert = %v, want Unique", err)
		}
		got, err := files.Get(context.Background(), udbclient.RowKey{"file_id": id})
		if err != nil || got.GetFilename() != "original.pdf" {
			t.Fatalf("duplicate changed the stored row: %v, %v", got, err)
		}
	})

	t.Run("two concurrent inserts have exactly one winner", func(t *testing.T) {
		tenant := newTenant()
		files := udbclient.TableOf[*storagev1.File](connect(t, tenant))
		id := NewID()
		type result struct {
			name string
			err  error
		}
		results := make(chan result, 2)
		start := make(chan struct{})
		for _, name := range []string{"a.pdf", "b.pdf"} {
			go func(name string) {
				<-start
				results <- result{name, files.Insert(context.Background(), file(tenant, id, name))}
			}(name)
		}
		close(start)
		winner := ""
		duplicates := 0
		for range 2 {
			result := <-results
			if result.err == nil {
				if winner != "" {
					t.Fatal("both inserts succeeded")
				}
				winner = result.name
			} else if udbclient.IsUnique(result.err) {
				duplicates++
			} else {
				t.Fatalf("insert failed without Unique: %v", result.err)
			}
		}
		if winner == "" || duplicates != 1 {
			t.Fatalf("winner=%q, duplicates=%d", winner, duplicates)
		}
		got, err := files.Get(context.Background(), udbclient.RowKey{"file_id": id})
		if err != nil || got.GetFilename() != winner {
			t.Fatalf("winner was overwritten: %v, %v", got, err)
		}
	})

	t.Run("transaction insert duplicate rolls back every earlier write", func(t *testing.T) {
		tenant := newTenant()
		u := connect(t, tenant)
		files := udbclient.TableOf[*storagev1.File](u)
		existing, fresh := NewID(), NewID()
		if err := files.Insert(context.Background(), file(tenant, existing, "original.pdf")); err != nil {
			t.Fatal(err)
		}
		err := u.Tx(context.Background(), func(tx *udbclient.TxScope) error {
			if err := tx.Insert(file(tenant, fresh, "new.pdf")); err != nil {
				return err
			}
			return tx.Insert(file(tenant, existing, "replacement.pdf"))
		})
		if !udbclient.IsUnique(err) {
			t.Fatalf("transaction duplicate = %v, want Unique", err)
		}
		if _, err := files.Get(context.Background(), udbclient.RowKey{"file_id": fresh}); !errors.Is(err, udbclient.ErrNotFound) {
			t.Fatalf("transaction left an earlier insert behind: %v", err)
		}
		got, err := files.Get(context.Background(), udbclient.RowKey{"file_id": existing})
		if err != nil || got.GetFilename() != "original.pdf" {
			t.Fatalf("transaction replaced duplicate row: %v, %v", got, err)
		}
		if err := u.Tx(context.Background(), func(tx *udbclient.TxScope) error { return tx.Insert(file(tenant, fresh, "committed.pdf")) }); err != nil {
			t.Fatal(err)
		}
		got, err = files.Get(context.Background(), udbclient.RowKey{"file_id": fresh})
		if err != nil || got.GetFilename() != "committed.pdf" {
			t.Fatalf("transaction insert did not commit: %v, %v", got, err)
		}
	})

	t.Run("tenants never see each other's rows", func(t *testing.T) {
		a, b := newTenant(), newTenant()
		if o.tenants[1] != "" {
			b = o.tenants[1]
		}
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
		foreign := file(b, id, "must-not-overwrite.pdf")
		if err := udbclient.TableOf[*storagev1.File](ub).Upsert(context.Background(), foreign); err == nil {
			t.Fatal("another tenant upserted a stored global primary key")
		}
		got, err := udbclient.TableOf[*storagev1.File](ua).Get(context.Background(), udbclient.RowKey{"file_id": id})
		if err != nil || got.GetFilename() != "secret.pdf" {
			t.Fatalf("foreign write changed original: %v, %v", got, err)
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
		ids := make([]any, 0, 5)
		for i := 0; i < 5; i++ {
			id := newID(100 + i)
			ids = append(ids, id)
			if err := files.Upsert(context.Background(), file(tenant, id, fmt.Sprintf("p%d.pdf", i))); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
		// A live login shares its tenant with the other cases and previous runs.
		// Count and pagination must cover this case's five rows only.
		filter := udbclient.Filter{"file_id": udbclient.Filter{"$in": ids}}
		page, err := files.Select(context.Background(), filter, udbclient.SelectOptions{Limit: 2, IncludeTotal: true})
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		if len(page.Rows) != 2 || !page.HasMore || page.Total != 5 {
			t.Fatalf("page = rows %d hasMore %v total %d", len(page.Rows), page.HasMore, page.Total)
		}
		seen := 0
		if err := files.SelectAll(context.Background(), filter, func(*storagev1.File) error { seen++; return nil }); err != nil {
			t.Fatalf("select all: %v", err)
		}
		if seen != 5 {
			t.Fatalf("SelectAll saw %d rows, want 5", seen)
		}
		filter["filename"] = udbclient.Filter{"$in": []any{"p1.pdf", "p3.pdf"}}
		n, err := files.Count(context.Background(), filter)
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
