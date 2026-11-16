package udbtest

import (
	"strings"
	"testing"
)

func predicateColumns(column string) (string, bool) {
	aliases := map[string]string{"retired_at": "retired_at", "deleted_at": "deleted_at", "kind": "kind", "label": "label"}
	name, ok := aliases[column]
	return name, ok
}

func TestUniquePredicateConjunctionOfNullTests(t *testing.T) {
	test, err := uniquePredicate(`retired_at IS NULL and "Deleted_At" is null`, predicateColumns)
	if err != nil {
		t.Fatal(err)
	}
	if !test(map[string]any{"retired_at": nil, "deleted_at": nil}) {
		t.Fatal("a live row must take part in the unique key")
	}
	if test(map[string]any{"retired_at": "2026-01-01T00:00:00Z", "deleted_at": nil}) {
		t.Fatal("a retired row must not take part in the unique key")
	}
	if test(map[string]any{"retired_at": nil, "deleted_at": "2026-01-01T00:00:00Z"}) {
		t.Fatal("a deleted row must not take part in the unique key")
	}
}

func TestUniquePredicateLiteralComparison(t *testing.T) {
	test, err := uniquePredicate(`kind = 'primary' AND deleted_at IS NULL`, predicateColumns)
	if err != nil {
		t.Fatal(err)
	}
	if !test(map[string]any{"kind": "primary"}) {
		t.Fatal("a live primary row must take part in the unique key")
	}
	if test(map[string]any{"kind": "secondary"}) || test(map[string]any{"kind": nil}) {
		t.Fatal("another kind, or a NULL kind, must not take part")
	}
	if test(map[string]any{"kind": "primary", "deleted_at": "2026-01-01T00:00:00Z"}) {
		t.Fatal("a deleted primary row must not take part")
	}
	notEqual, err := uniquePredicate(`label <> 'it''s and more'`, predicateColumns)
	if err != nil {
		t.Fatal(err)
	}
	if notEqual(map[string]any{"label": "it's and more"}) || !notEqual(map[string]any{"label": "x"}) || notEqual(map[string]any{"label": nil}) {
		t.Fatal("<> compares the unescaped literal, and NULL is never included")
	}
}

func TestUniquePredicateRefusesWhatItCannotRead(t *testing.T) {
	for _, where := range []string{
		`retired_at IS NULL OR deleted_at IS NULL`,
		`(deleted_at IS NULL)`,
		`lower(kind) = 'primary'`,
		`unknown_column IS NULL`,
		`kind > 'a'`,
	} {
		if _, err := uniquePredicate(where, predicateColumns); err == nil || !strings.Contains(err.Error(), "unsupported unique-index predicate") {
			t.Fatalf("%q: want an unsupported-predicate error, got %v", where, err)
		}
	}
}

func TestEmbeddedEnvironmentCarriesOnlyTheApprovalPlanItIsGiven(t *testing.T) {
	source := []string{"PATH=/bin", "UDB_PG_DSN=postgres://other", "udb_grpc_addr=x", "DATABASE_URL=postgres://x", "UDB_HOME=/h", "UDB_MIGRATION_APPROVAL_PLAN=/stale.json"}
	plain := strings.Join(embeddedEnvironment(source, "/proto", ""), "\n")
	if plain != "PATH=/bin\nUDB_HOME=/h\nUDB_PROTO_ROOT=/proto" {
		t.Fatalf("a caller's UDB_* settings, approval plan included, must not be inherited: %q", plain)
	}
	approved := embeddedEnvironment(source, "/proto", "/project/approval.json")
	if approved[len(approved)-1] != "UDB_MIGRATION_APPROVAL_PLAN=/project/approval.json" {
		t.Fatalf("the approval plan is not passed to the broker: %q", approved)
	}
	if got := strings.Join(approvalPlanArgs("/project/approval.json"), " "); got != "plan --emit-approval-plan /project/approval.json" {
		t.Fatalf("approval plan arguments: %q", got)
	}
	if !mergeStartOptions([]StartOptions{{}, {ApproveReviewRequired: true}}).ApproveReviewRequired || mergeStartOptions(nil).ApproveReviewRequired {
		t.Fatal("ApproveReviewRequired is opt-in")
	}
}
