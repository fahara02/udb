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

/** Read the durable-idempotency replay flag off a mutation response body,
 *  reading `was_duplicate` (MutationResponse field 3). Returns `true` when the
 *  broker collapsed this call as a REPLAY of a prior write carrying the same
 *  idempotency key (no new row was written), and `false` for a fresh durable
 *  write. Mirrors {@link receiptFromResponse}'s body-reading shape so a caller
 *  that retried (or re-sent) an upsert/delete can tell "my write landed just now"
 *  from "the broker already had this and returned the earlier result".
 *
 *  Accepts either the keepCase wire object (`was_duplicate`) or a camelCase
 *  message (`wasDuplicate`). */
export function wasDuplicate(resp: any): boolean {
  if (!resp || typeof resp !== "object") return false;
  const flag = resp.was_duplicate ?? resp.wasDuplicate;
  return flag === true;
}

// ── Consistency-mode selection (RequestContext.consistency / x-udb-consistency) ─
//
// TS callers had no ergonomic way to declare a read consistency mode (the request
// context only carried `read_fence_json`). These helpers mirror the Python
// selector: they map a friendly mode name to the pinned wire token the broker
// reads at `security.rs` (`x-udb-consistency` header, field 18 `consistency`
// string) and the typed `consistency_mode` enum (field 22). The tokens are PUBLIC
// CONTRACT with `udb::runtime::consistency::ConsistencyMode::as_str` — do not
// rename them.

/** The pinned consistency-mode wire tokens. */
export type ConsistencyModeToken =
  | "strong"
  | "read_your_writes"
  | "bounded_staleness"
  | "replica_bounded"
  | "eventual"
  | "projection_ok"
  | "cache_ok";

/** Friendly, discoverable consistency-mode constants (token-valued, so
 *  `ConsistencyMode.Strong === "strong"`). Pass any of these — or a friendly
 *  alias like `"read-your-writes"` / `"bounded"` — to {@link withConsistency} /
 *  {@link consistencyContext}. */
export const ConsistencyMode = {
  /** Read the primary, no replica fallback (linearizable). */
  Strong: "strong",
  /** Read your own writes — primary or a replica fenced past your write LSN. */
  ReadYourWrites: "read_your_writes",
  /** Any replica caught up within `max_replica_lag_ms` (bounded staleness). */
  BoundedStaleness: "bounded_staleness",
  /** A physical read replica within a lag budget, failing over to the primary. */
  ReplicaBounded: "replica_bounded",
  /** Any healthy replica/projection; no fence, cheapest. */
  Eventual: "eventual",
  /** Projection-backed targets (Mongo/Qdrant/ClickHouse) acceptable. */
  ProjectionOk: "projection_ok",
  /** Redis cache hits acceptable (cache verifies manifest checksum). */
  CacheOk: "cache_ok",
} as const;

// Friendly aliases → canonical token. Mirrors the broker's lenient `parse`
// (consistency.rs) plus the short spec names ("bounded" → bounded_staleness).
const CONSISTENCY_ALIASES: Record<string, ConsistencyModeToken> = {
  strong: "strong",
  linearizable: "strong",
  primary: "strong",
  read_your_writes: "read_your_writes",
  "read-your-writes": "read_your_writes",
  ryw: "read_your_writes",
  bounded: "bounded_staleness",
  bounded_staleness: "bounded_staleness",
  "bounded-staleness": "bounded_staleness",
  replica_bounded: "replica_bounded",
  "replica-bounded": "replica_bounded",
  eventual: "eventual",
  eventual_consistency: "eventual",
  projection_ok: "projection_ok",
  "projection-ok": "projection_ok",
  cache_ok: "cache_ok",
  "cache-ok": "cache_ok",
};

// Canonical token → proto enum NAME (context.proto field 22). Under the keepCase
// / `enums: String` loader the wire carries the enum NAME string, not the number.
const CONSISTENCY_MODE_ENUM_NAME: Record<ConsistencyModeToken, string> = {
  strong: "CONSISTENCY_MODE_STRONG",
  read_your_writes: "CONSISTENCY_MODE_READ_YOUR_WRITES",
  bounded_staleness: "CONSISTENCY_MODE_BOUNDED_STALENESS",
  replica_bounded: "CONSISTENCY_MODE_REPLICA_BOUNDED",
  eventual: "CONSISTENCY_MODE_EVENTUAL",
  projection_ok: "CONSISTENCY_MODE_PROJECTION_OK",
  cache_ok: "CONSISTENCY_MODE_CACHE_OK",
};

/** Resolve a mode name/alias to its canonical wire token. Throws on an unknown
 *  mode so a typo fails loudly rather than silently defaulting to strong. */
export function consistencyToken(mode: ConsistencyModeToken | string): ConsistencyModeToken {
  const token = CONSISTENCY_ALIASES[String(mode).trim().toLowerCase()];
  if (!token) {
    throw new Error(
      `udb: unknown consistency mode "${mode}" (expected one of ` +
        `strong | read_your_writes | bounded_staleness | replica_bounded | eventual | projection_ok | cache_ok)`,
    );
  }
  return token;
}

/** Returns a {@link CallOptions} that declares the read consistency `mode` on
 *  exactly this one call via the `x-udb-consistency` header (the header the
 *  broker reads at `security.rs`; it WINS over the body `consistency` field when
 *  both are present). Mirrors the Python `x-udb-consistency` selector. */
export function withConsistency(mode: ConsistencyModeToken | string): CallOptions {
  return { headers: { "x-udb-consistency": consistencyToken(mode) } };
}

/** For body-context callers: returns a partial `RequestContext` setting both the
 *  legacy `consistency` string (context.proto field 18) and the typed
 *  `consistency_mode` enum name (field 22). Spread into the request `context`.
 *  The `x-udb-consistency` header still wins server-side when both are supplied. */
export function consistencyContext(
  mode: ConsistencyModeToken | string,
): { consistency: ConsistencyModeToken; consistency_mode: string } {
  const token = consistencyToken(mode);
  return { consistency: token, consistency_mode: CONSISTENCY_MODE_ENUM_NAME[token] };
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
  /** Alias of {@link wasDuplicate} — durable-idempotency replay flag off a mutation. */
  wasDuplicate,
  /** Alias of {@link withConsistency} — attach a consistency mode to the next call. */
  withConsistency,
  /** Alias of {@link consistencyContext} — body-context consistency-mode fields. */
  consistencyContext,
  /** Alias of {@link consistencyToken} — resolve a mode name to its wire token. */
  consistencyToken,
  /** The friendly consistency-mode constants ({@link ConsistencyMode}). */
  ConsistencyMode,
};
