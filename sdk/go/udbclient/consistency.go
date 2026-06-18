package udbclient

import (
	"encoding/json"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
)

// ── Typed write-receipt / read-fence helpers (chapter 08.1) ──────────────────
//
// The broker emits MutationResponse.write_receipt_json (and an x-udb-write-receipt
// header) per write, and RequestContext carries read_fence_json for read-your-writes.
// These types mirror the Rust serde shapes 1:1 so apps never hand-author
// Rust-shaped JSON. The load-bearing cross-type mapping is
// WriteReceipt.SourceLsn -> ReadFence.MinOutboxLsn (see ReadFenceFromReceipt).
//
// Both shapes are pinned byte-for-byte to the committed, machine-derived golden
// fixture docs/generated/consistency-golden.json (lane 07) by
// consistency_test.go — do NOT drift the JSON tags from that fixture.

// WriteReceipt mirrors the Rust `WriteReceipt` struct (src/runtime/consistency.rs).
// The Rust struct serializes ALL five fields unconditionally (no
// skip_serializing_if), so none of the JSON tags carry `omitempty`.
type WriteReceipt struct {
	SourceLsn         string   `json:"source_lsn"`
	OutboxSeq         uint64   `json:"outbox_seq"`
	ProjectionTaskIds []string `json:"projection_task_ids"`
	ManifestChecksum  string   `json:"manifest_checksum"`
	WrittenAtUnixMs   int64    `json:"written_at_unix_ms"`
}

// ParseWriteReceipt decodes write_receipt_json bytes into a WriteReceipt.
// Empty input yields a zero receipt and a nil error (no write produced a receipt).
func ParseWriteReceipt(b []byte) (WriteReceipt, error) {
	if len(b) == 0 {
		return WriteReceipt{}, nil
	}
	var r WriteReceipt
	if err := json.Unmarshal(b, &r); err != nil {
		return WriteReceipt{}, err
	}
	return r, nil
}

// IsEmpty reports whether the receipt carries no write information.
func (r WriteReceipt) IsEmpty() bool {
	return r.SourceLsn == "" && r.OutboxSeq == 0 &&
		len(r.ProjectionTaskIds) == 0 && r.ManifestChecksum == "" && r.WrittenAtUnixMs == 0
}

// ReadFence mirrors the Rust `ReadFence` struct (src/runtime/consistency.rs).
// In Rust, min_outbox_lsn and projection_task_ids carry skip_serializing_if
// (empty omitted); max_wait_ms is `#[serde(default)]` only (always emitted), so
// it has NO omitempty here.
type ReadFence struct {
	MinOutboxLsn      string   `json:"min_outbox_lsn,omitempty"`
	ProjectionTaskIds []string `json:"projection_task_ids,omitempty"`
	MaxWaitMs         uint64   `json:"max_wait_ms"`
}

// IsEmpty reports whether the fence carries no positional constraint (only a
// wait budget, or nothing at all).
func (f ReadFence) IsEmpty() bool {
	return f.MinOutboxLsn == "" && len(f.ProjectionTaskIds) == 0
}

// ReadFenceFromReceipt builds the fence a follow-up read attaches to wait for its
// own write to be visible. It maps the receipt's SourceLsn -> MinOutboxLsn (the
// load-bearing cross-type field mapping from Rust `ReadFence::from_receipt`) and
// copies the projection task ids; it does NOT carry over outbox_seq or
// manifest_checksum.
func ReadFenceFromReceipt(r WriteReceipt, maxWaitMs uint64) ReadFence {
	return ReadFence{
		MinOutboxLsn:      r.SourceLsn,
		ProjectionTaskIds: append([]string(nil), r.ProjectionTaskIds...),
		MaxWaitMs:         maxWaitMs,
	}
}

// ReceiptFromMutation captures the WriteReceipt from a MutationResponse body
// field (the primary capture path; the x-udb-write-receipt header is the
// forward/embedded fallback). An empty body field yields a zero receipt and a nil
// error.
func ReceiptFromMutation(m *entityv1.MutationResponse) (WriteReceipt, error) {
	if m == nil {
		return WriteReceipt{}, nil
	}
	return ParseWriteReceipt([]byte(m.GetWriteReceiptJson()))
}

// AfterWrite stamps a read fence derived from a write receipt onto a single,
// caller-supplied per-read RequestContext so the follow-up read observes its own
// write. It NEVER touches shared Udb/facade metadata — the fence rides only this
// one request (guardrail: no leaking a fence into unrelated reads). An empty
// receipt leaves rc untouched.
func AfterWrite(rc *entityv1.RequestContext, r WriteReceipt, maxWaitMs uint64) {
	if rc == nil || r.IsEmpty() {
		return
	}
	fence := ReadFenceFromReceipt(r, maxWaitMs)
	b, err := json.Marshal(fence)
	if err != nil {
		return
	}
	rc.ReadFenceJson = string(b)
}
