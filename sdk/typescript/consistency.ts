// Typed WriteReceipt / ReadFence helpers for read-after-write consistency.
//
// The broker emits `MutationResponse.write_receipt_json` (field 7) and the
// `x-udb-write-receipt` header on a write, and honors `RequestContext.read_fence_json`
// (field 14) + the `x-udb-read-fence` header on a follow-up read. These helpers
// give callers typed shapes + the load-bearing `source_lsn → min_outbox_lsn`
// mapping so apps never hand-author the Rust-serde-shaped JSON.
//
// The serde field names are pinned by lane 07's machine-derived golden fixture
// (`docs/generated/consistency-golden.json`); these interfaces match it exactly.

import type { CallOptions } from "./generatedClient";

/** A write receipt the broker returns after a mutation. Mirrors the Rust serde
 *  shape `udb::runtime::consistency::WriteReceipt` (all 5 fields always emitted,
 *  no skip_serializing_if). */
export interface WriteReceipt {
  /** Source log sequence number of the committed write (maps to the read
   *  fence's `min_outbox_lsn`). */
  source_lsn: string;
  /** Monotonic outbox sequence for the write. */
  outbox_seq: number;
  /** Projection task ids the write enqueued (empty when none). */
  projection_task_ids: string[];
  /** Catalog/manifest checksum at write time. */
  manifest_checksum: string;
  /** Server commit time, epoch milliseconds. */
  written_at_unix_ms: number;
}

/** A read fence attached to a SPECIFIC follow-up read so it observes the prior
 *  write. Mirrors the Rust serde shape `udb::runtime::consistency::ReadFence`:
 *  `min_outbox_lsn` / `projection_task_ids` are skip-if-empty; `max_wait_ms` is
 *  always serialized. */
export interface ReadFence {
  /** Skip-if-empty: the minimum outbox LSN the read must observe. */
  min_outbox_lsn?: string;
  /** Skip-if-empty: projection task ids that must be applied before the read. */
  projection_task_ids?: string[];
  /** Always serialized: max time the broker waits for the fence, milliseconds. */
  max_wait_ms: number;
}

/** Build a {@link ReadFence} from a {@link WriteReceipt}, applying the
 *  load-bearing cross-type mapping `source_lsn → min_outbox_lsn` and copying the
 *  projection task ids. Empty fields are OMITTED (undefined) so a later
 *  `JSON.stringify` mirrors the Rust `skip_serializing_if`. */
export function readFenceFromReceipt(r: WriteReceipt, maxWaitMs: number): ReadFence {
  return {
    min_outbox_lsn: r.source_lsn ? r.source_lsn : undefined,
    projection_task_ids:
      r.projection_task_ids && r.projection_task_ids.length ? r.projection_task_ids : undefined,
    max_wait_ms: maxWaitMs,
  };
}

/** Parse a receipt JSON string into a typed {@link WriteReceipt}. Returns `null`
 *  on empty/invalid/all-default input so callers never crash on a no-op write
 *  receipt (the broker emits an all-defaults receipt for a write that committed
 *  nothing). */
export function parseWriteReceipt(json: string): WriteReceipt | null {
  if (!json) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object") return null;
  const obj = parsed as Record<string, unknown>;
  const receipt: WriteReceipt = {
    source_lsn: typeof obj.source_lsn === "string" ? obj.source_lsn : "",
    outbox_seq: typeof obj.outbox_seq === "number" ? obj.outbox_seq : Number(obj.outbox_seq ?? 0),
    projection_task_ids: Array.isArray(obj.projection_task_ids)
      ? (obj.projection_task_ids as string[])
      : [],
    manifest_checksum: typeof obj.manifest_checksum === "string" ? obj.manifest_checksum : "",
    written_at_unix_ms:
      typeof obj.written_at_unix_ms === "number"
        ? obj.written_at_unix_ms
        : Number(obj.written_at_unix_ms ?? 0),
  };
  // An empty (all-default) receipt is a valid no-op signal — still return it so
  // callers can detect it (source_lsn === "" and outbox_seq === 0).
  return receipt;
}

/** Extract a {@link WriteReceipt} from a mutation response body, reading
 *  `write_receipt_json` (MutationResponse field 7). Returns `null` when absent. */
export function receiptFromResponse(resp: any): WriteReceipt | null {
  if (!resp) return null;
  const json = resp.write_receipt_json;
  if (typeof json !== "string") return null;
  return parseWriteReceipt(json);
}

/** Returns a {@link CallOptions} that attaches `fence` to exactly the one
 *  follow-up read via the `x-udb-read-fence` header (the broker reads it at
 *  security.rs / service/mod.rs). The fence rides this single call only — it is
 *  NEVER stored on shared project metadata. */
export function withReadFence(fence: ReadFence): CallOptions {
  return { headers: { "x-udb-read-fence": JSON.stringify(fence) } };
}

/** For body-context callers: returns a partial `RequestContext` setting
 *  `read_fence_json` (context.proto field 14). Spread into the request `context`. */
export function readFenceContext(fence: ReadFence): { read_fence_json: string } {
  return { read_fence_json: JSON.stringify(fence) };
}

/** One-shot helper (naming contract alias): convert a {@link WriteReceipt} into a
 *  {@link ReadFence} and attach it to the next read via `x-udb-read-fence`.
 *  Composes {@link readFenceFromReceipt} + {@link withReadFence} — no round trip. */
export function withReadFenceFromReceipt(r: WriteReceipt, maxWaitMs: number): CallOptions {
  return withReadFence(readFenceFromReceipt(r, maxWaitMs));
}

/** Canonical receipt accessor (naming contract): attach the read fence derived
 *  from a write `receipt` to exactly the next read. Alias of
 *  {@link withReadFenceFromReceipt}; the spec name is `metadata.afterWrite(receipt)`
 *  (exposed as `UdbProject.metadata.afterWrite`). The fence rides only this one
 *  CallOptions — never shared project metadata. */
export function afterWrite(receipt: WriteReceipt, maxWaitMs = 5_000): CallOptions {
  return withReadFenceFromReceipt(receipt, maxWaitMs);
}

/** Grouped receipt/fence accessors. `UdbProject.metadata` is an instance of this
 *  shape so callers can write `udb.metadata.afterWrite(receipt)` per the naming
 *  contract. Standalone functions of the same name remain exported directly. */
export const consistencyMetadata = {
  /** Alias of {@link afterWrite} — attach a receipt-derived read fence to the next read. */
  afterWrite,
  /** Alias of {@link withReadFenceFromReceipt}. */
  withReadFenceFromReceipt,
  /** Alias of {@link withReadFence}. */
  withReadFence,
  /** Alias of {@link readFenceFromReceipt}. */
  readFenceFromReceipt,
  /** Alias of {@link receiptFromResponse}. */
  receiptFromResponse,
};
