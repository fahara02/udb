package udbclient

// V23-1 compile contract for the typed repositories emitted by
// `udb sdk generate --project-proto --lang go` (see src/cli/sdk_gen.rs,
// render_go_entities_file). The generator writes `<Entity>Repo` methods that
// call into THIS package; if the SDK DTOs drift incompatibly, the generated
// consumer code stops compiling — but that only surfaced downstream, in the
// consumer's build, never here. V23-1: the emitter declared an int64 List count
// return while udbclient.Page.TotalCount is int32, so every generated repo
// failed `go build` at the consumer.
//
// This test (plus the never-called shape function below) mirrors the generated
// repo's expressions line-for-line so the same mismatch fails to COMPILE inside
// the SDK's own CI (`go vet ./...` + `go test ./udbclient`). It is a
// compile-time contract, not a runtime test — no broker calls execute.

import (
	"context"
	"testing"
)

func TestEntityRepoGeneratedContract(t *testing.T) {
	// ── List() count widening ─────────────────────────────────────────────
	// The generated List declares an int64 count and returns
	// `int64(page.TotalCount)`. That widening must stay legal whatever concrete
	// integer type Page.TotalCount has — this is the line that broke.
	var page Page
	var _ []map[string]any = page.Rows // FromUDBRow consumes each row
	var _ string = page.NextPageToken
	var totalCount int64 = int64(page.TotalCount)
	_ = totalCount

	// ── PageOptions the caller of a generated List constructs ──────────────
	_ = PageOptions{
		Fields:    nil,
		Sort:      []SortKey{{Field: "id", Descending: true}},
		Limit:     0,
		PageToken: "",
	}

	// Keep the call-shape contract compiled/type-checked without invoking it.
	_ = entityRepoGeneratedCallShapes
}

// entityRepoGeneratedCallShapes is never called; it exists only so the compiler
// and `go vet` type-check the EXACT udbclient calls the generated <Entity>Repo
// methods emit. Taking `c *Client` as a parameter (not a provably-nil local)
// keeps the nilness analyzer from flagging an impossible branch, while still
// failing to build if any of these signatures drift.
func entityRepoGeneratedCallShapes(c *Client, ctx context.Context) {
	e := c.Entity("acme.v1.Thing", Key("id")) // New<Entity>Repo binding
	repo := struct{ E *Entity }{E: e}         // <Entity>Repo{ E *udbclient.Entity }
	where := map[string]any{"id": "x"}
	// List: SelectPage → rows → (out, token, int64 count, err)
	p, _ := repo.E.SelectPage(ctx, where, PageOptions{})
	_ = int64(p.TotalCount)
	// Get: Select → first row
	_, _ = repo.E.Select(ctx, where)
	// UpdateGuarded / DeleteGuarded: CAS options pass-through
	_, _ = repo.E.Update(ctx, where, map[string]any{"a": 1}, WithUpdateExpected(where))
	_, _ = repo.E.Delete(ctx, where, WithDeleteExpected(where))
}
