package udbclient

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
)

// goldenFixturePath locates the committed, machine-derived cross-language golden
// (lane 07, derived from the Rust serde shape). It lives at the repo root under
// docs/generated/, OUTSIDE this Go module tree, so it cannot be go:embed'd; the
// test reads it at run time via a relative path from sdk/go/udbclient/.
const goldenFixturePath = "../../../docs/generated/consistency-golden.json"

// consistencyGolden mirrors the two top-level entries in the fixture.
type consistencyGolden struct {
	ReadFence    json.RawMessage `json:"read_fence"`
	WriteReceipt json.RawMessage `json:"write_receipt"`
}

// TestReceiptFenceGoldenJSON pins WriteReceipt + ReadFence JSON marshalling to
// lane 07's committed fixture. It re-marshals both the SDK-produced shapes and
// the fixture's entries through a canonical (sorted-key) form and asserts byte
// equality — so any tag/omitempty/field drift from the cross-language source of
// truth fails the test.
func TestReceiptFenceGoldenJSON(t *testing.T) {
	raw, err := os.ReadFile(filepath.FromSlash(goldenFixturePath))
	if err != nil {
		t.Fatalf("read golden fixture %s: %v", goldenFixturePath, err)
	}
	var g consistencyGolden
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatalf("decode golden fixture: %v", err)
	}
	if len(g.WriteReceipt) == 0 || len(g.ReadFence) == 0 {
		t.Fatalf("golden fixture missing write_receipt/read_fence entries")
	}

	// The exact values in the committed fixture (lane 07). WriteReceipt emits all
	// five keys; ReadFence skips empty min_outbox_lsn/projection_task_ids and
	// always emits max_wait_ms. We mirror the fixture's values exactly.
	receipt := WriteReceipt{
		SourceLsn:         "0/1A2B3C4D",
		OutboxSeq:         42,
		ProjectionTaskIds: []string{"projection-task-a", "projection-task-b"},
		ManifestChecksum:  "sha256:0123456789abcdef",
		WrittenAtUnixMs:   1735689600000,
	}
	// ReadFence golden = ReadFenceFromReceipt(receipt, 2500), proving the
	// source_lsn -> min_outbox_lsn mapping is byte-identical to the fixture.
	fence := ReadFenceFromReceipt(receipt, 2500)

	assertCanonicalEqual(t, "write_receipt", g.WriteReceipt, mustMarshal(t, receipt))
	assertCanonicalEqual(t, "read_fence", g.ReadFence, mustMarshal(t, fence))
}

// TestWriteReceiptAllFieldsUnconditional asserts WriteReceipt never omits a field
// (matches the Rust struct with no skip_serializing_if).
func TestWriteReceiptAllFieldsUnconditional(t *testing.T) {
	b := mustMarshal(t, WriteReceipt{})
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode zero receipt: %v", err)
	}
	for _, k := range []string{"source_lsn", "outbox_seq", "projection_task_ids", "manifest_checksum", "written_at_unix_ms"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("zero WriteReceipt dropped required key %q: %s", k, b)
		}
	}
}

// TestReadFenceEmptySkips asserts the empty-field skip semantics: an empty fence
// with only a wait budget emits exactly {"max_wait_ms":N}.
func TestReadFenceEmptySkips(t *testing.T) {
	got := string(mustMarshal(t, ReadFence{MaxWaitMs: 5000}))
	if got != `{"max_wait_ms":5000}` {
		t.Fatalf("ReadFence empty-skip drift: got %s", got)
	}
}

// TestReadFenceFromReceiptMapping pins the cross-type source_lsn -> min_outbox_lsn
// mapping and the projection-task-id copy.
func TestReadFenceFromReceiptMapping(t *testing.T) {
	f := ReadFenceFromReceipt(WriteReceipt{SourceLsn: "0/1A2B3C", ProjectionTaskIds: []string{"a", "b"}}, 5000)
	if f.MinOutboxLsn != "0/1A2B3C" {
		t.Fatalf("source_lsn not mapped to min_outbox_lsn: %q", f.MinOutboxLsn)
	}
	if len(f.ProjectionTaskIds) != 2 {
		t.Fatalf("projection task ids not copied: %v", f.ProjectionTaskIds)
	}
	if f.MaxWaitMs != 5000 {
		t.Fatalf("max_wait_ms not set: %d", f.MaxWaitMs)
	}
}

// TestAfterWriteStampsFence asserts AfterWrite mutates only the supplied context
// and leaves it untouched on an empty receipt.
func TestAfterWriteStampsFence(t *testing.T) {
	rc := &entityv1.RequestContext{}
	AfterWrite(rc, WriteReceipt{SourceLsn: "0/9"}, 5000)
	if rc.ReadFenceJson != `{"min_outbox_lsn":"0/9","max_wait_ms":5000}` {
		t.Fatalf("AfterWrite fence drift: %s", rc.ReadFenceJson)
	}
	rc2 := &entityv1.RequestContext{}
	AfterWrite(rc2, WriteReceipt{}, 5000)
	if rc2.ReadFenceJson != "" {
		t.Fatalf("AfterWrite stamped a fence for an empty receipt: %s", rc2.ReadFenceJson)
	}
}

// TestReceiptFromMutation round-trips the body field.
func TestReceiptFromMutation(t *testing.T) {
	r, err := ReceiptFromMutation(&entityv1.MutationResponse{WriteReceiptJson: `{"source_lsn":"0/9","outbox_seq":1,"projection_task_ids":[],"manifest_checksum":"","written_at_unix_ms":0}`})
	if err != nil {
		t.Fatalf("ReceiptFromMutation: %v", err)
	}
	if r.SourceLsn != "0/9" {
		t.Fatalf("source_lsn not parsed: %q", r.SourceLsn)
	}
	empty, err := ReceiptFromMutation(&entityv1.MutationResponse{})
	if err != nil || !empty.IsEmpty() {
		t.Fatalf("empty body field should yield a zero receipt: %+v err=%v", empty, err)
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// assertCanonicalEqual compares two JSON documents structurally (key order does
// not matter, but presence/absence of keys and values do).
func assertCanonicalEqual(t *testing.T, label string, want, got []byte) {
	t.Helper()
	var wv, gv any
	if err := json.Unmarshal(want, &wv); err != nil {
		t.Fatalf("%s: decode golden: %v", label, err)
	}
	if err := json.Unmarshal(got, &gv); err != nil {
		t.Fatalf("%s: decode sdk: %v", label, err)
	}
	if !reflect.DeepEqual(wv, gv) {
		t.Fatalf("%s drift:\n golden: %s\n sdk:    %s", label, want, got)
	}
}
