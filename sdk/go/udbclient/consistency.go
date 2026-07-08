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

// WasDuplicate reports whether the broker collapsed this mutation onto a prior
// write via durable idempotency (a replay of the same idempotency key) instead
// of applying a fresh write. It mirrors MutationResponse.was_duplicate and lets a
// caller of the raw Upsert/Delete path distinguish an idempotency replay from a
// fresh mutation (Entity.Upsert also surfaces this on UpsertResult.WasDuplicate).
// A nil response yields false.
func WasDuplicate(m *entityv1.MutationResponse) bool {
	return m.GetWasDuplicate()
}

// ── Consistency-mode selection (chapter 08.1) ────────────────────────────────
//
// ConsistencyMode is an ergonomic selector for the read/write consistency a
// single request wants, mirroring Python's Metadata consistency knob. Its string
// value is the PINNED wire token the broker parses (src/runtime/consistency.rs
// ConsistencyMode::parse) — from either the x-udb-consistency header or the
// typed RequestContext.consistency_mode enum (proto field 22) it maps 1:1 onto.
// Changing a token breaks every client, so these stay byte-for-byte pinned.
type ConsistencyMode string

const (
	// ConsistencyDefault leaves the mode unset (broker default = strong).
	ConsistencyDefault ConsistencyMode = ""
	// ConsistencyStrong reads the primary / linearizable copy.
	ConsistencyStrong ConsistencyMode = "strong"
	// ConsistencyReadYourWrites guarantees a session observes its own writes.
	ConsistencyReadYourWrites ConsistencyMode = "read_your_writes"
	// ConsistencyBoundedStaleness allows a replica read within a lag bound.
	ConsistencyBoundedStaleness ConsistencyMode = "bounded_staleness"
	// ConsistencyReplicaBounded prefers a replica within a bound.
	ConsistencyReplicaBounded ConsistencyMode = "replica_bounded"
	// ConsistencyEventual allows any replica (fastest, weakest).
	ConsistencyEventual ConsistencyMode = "eventual"
	// ConsistencyProjectionOk permits serving from an async projection.
	ConsistencyProjectionOk ConsistencyMode = "projection_ok"
	// ConsistencyCacheOk permits serving from a cache.
	ConsistencyCacheOk ConsistencyMode = "cache_ok"
)

// proto maps the wire token onto the entity/v1.ConsistencyMode enum the broker
// reads from RequestContext.consistency_mode. Unknown/empty maps to UNSPECIFIED.
func (m ConsistencyMode) proto() entityv1.ConsistencyMode {
	switch m {
	case ConsistencyStrong:
		return entityv1.ConsistencyMode_CONSISTENCY_MODE_STRONG
	case ConsistencyReadYourWrites:
		return entityv1.ConsistencyMode_CONSISTENCY_MODE_READ_YOUR_WRITES
	case ConsistencyBoundedStaleness:
		return entityv1.ConsistencyMode_CONSISTENCY_MODE_BOUNDED_STALENESS
	case ConsistencyReplicaBounded:
		return entityv1.ConsistencyMode_CONSISTENCY_MODE_REPLICA_BOUNDED
	case ConsistencyEventual:
		return entityv1.ConsistencyMode_CONSISTENCY_MODE_EVENTUAL
	case ConsistencyProjectionOk:
		return entityv1.ConsistencyMode_CONSISTENCY_MODE_PROJECTION_OK
	case ConsistencyCacheOk:
		return entityv1.ConsistencyMode_CONSISTENCY_MODE_CACHE_OK
	default:
		return entityv1.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED
	}
}

// Apply stamps this consistency mode onto a single, caller-supplied per-read
// RequestContext (the typed consistency_mode enum the broker honors). It NEVER
// touches shared Udb/facade metadata — the choice rides only this one request. An
// empty mode leaves rc untouched.
func (m ConsistencyMode) Apply(rc *entityv1.RequestContext) {
	if rc == nil || m == ConsistencyDefault {
		return
	}
	rc.ConsistencyMode = m.proto()
}

// Header returns the (key, value) metadata pair for the x-udb-consistency header
// form of this mode, for callers stamping consistency via request headers rather
// than the RequestContext body. An empty mode yields an empty value.
func (m ConsistencyMode) Header() (string, string) {
	return "x-udb-consistency", string(m)
}

// ── Facade-mounted metadata accessor (naming contract R2.0) ──────────────────

// MetadataAccessor is the metadata surface mounted on the project facade via
// Udb.Metadata(), matching the cross-language udb.metadata.* shape (TS
// udb.metadata.afterWrite, Python udb.metadata). It groups the per-request
// RequestContext stamping helpers so callers reach them off the project object
// instead of the package-level functions.
type MetadataAccessor struct{ u *Udb }

// Metadata returns the metadata accessor mounted on the project facade so
// udb.Metadata().AfterWrite(rc, receipt, maxWaitMs) mirrors TS
// udb.metadata.afterWrite(receipt).
func (u *Udb) Metadata() *MetadataAccessor { return &MetadataAccessor{u: u} }

// AfterWrite stamps a read fence derived from a write receipt onto a single
// per-read RequestContext so the follow-up read observes its own write. It
// delegates to the package-level AfterWrite and never touches shared facade
// metadata — the fence rides only this one request.
func (a *MetadataAccessor) AfterWrite(rc *entityv1.RequestContext, r WriteReceipt, maxWaitMs uint64) {
	AfterWrite(rc, r, maxWaitMs)
}

// Consistency stamps a consistency mode onto a single per-read RequestContext
// (delegates to ConsistencyMode.Apply).
func (a *MetadataAccessor) Consistency(rc *entityv1.RequestContext, mode ConsistencyMode) {
	mode.Apply(rc)
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
