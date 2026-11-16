package udbtest

import "testing"

// A NULL column matches no array operator, nor its negation (SQL three-valued
// logic), and NULL array elements equal nothing.
func TestArrayOperatorsOnNull(t *testing.T) {
	for _, op := range []string{"$contains", "$contained_by", "$overlaps", jsonContainsOp, jsonContainedByOp} {
		for _, cond := range []map[string]any{
			{op: []any{"p1"}},
			{"$not": map[string]any{op: []any{"p1"}}},
		} {
			ok, err := matches(map[string]any{"c": nil}, map[string]any{"c": cond})
			if err != nil || ok {
				t.Errorf("%v on NULL = %v, %v; want no match", cond, ok, err)
			}
		}
	}
	if arrayContains([]any{"a", nil}, []any{nil}) || arrayOverlaps([]any{nil}, []any{nil}) {
		t.Error("a NULL array element must equal nothing")
	}
	if !jsonContains([]any{nil, 1.0}, []any{nil}, true) {
		t.Error("jsonb null equals null")
	}
	if jsonContains([]any{1.0, []any{1.0, 3.0}}, []any{1.0, 3.0}, true) {
		t.Error("jsonb: 3 is not a top-level element of [1,[1,3]]")
	}
	if !jsonContains([]any{1.0, []any{1.0, 3.0}}, []any{[]any{3.0}}, true) {
		t.Error("jsonb: [[3]] is contained in [1,[1,3]]")
	}
}
