"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.consistencyMetadata = exports.ConsistencyMode = void 0;
exports.readFenceFromReceipt = readFenceFromReceipt;
exports.parseWriteReceipt = parseWriteReceipt;
exports.receiptFromResponse = receiptFromResponse;
exports.wasDuplicate = wasDuplicate;
exports.consistencyToken = consistencyToken;
exports.withConsistency = withConsistency;
exports.consistencyContext = consistencyContext;
exports.withReadFence = withReadFence;
exports.readFenceContext = readFenceContext;
exports.withReadFenceFromReceipt = withReadFenceFromReceipt;
exports.afterWrite = afterWrite;
/** Build a {@link ReadFence} from a {@link WriteReceipt}, applying the
 *  load-bearing cross-type mapping `source_lsn → min_outbox_lsn` and copying the
 *  projection task ids. Empty fields are OMITTED (undefined) so a later
 *  `JSON.stringify` mirrors the Rust `skip_serializing_if`. */
function readFenceFromReceipt(r, maxWaitMs) {
    return {
        min_outbox_lsn: r.source_lsn ? r.source_lsn : undefined,
        projection_task_ids: r.projection_task_ids && r.projection_task_ids.length ? r.projection_task_ids : undefined,
        max_wait_ms: maxWaitMs,
    };
}
/** Parse a receipt JSON string into a typed {@link WriteReceipt}. Returns `null`
 *  on empty/invalid/all-default input so callers never crash on a no-op write
 *  receipt (the broker emits an all-defaults receipt for a write that committed
 *  nothing). */
function parseWriteReceipt(json) {
    if (!json)
        return null;
    let parsed;
    try {
        parsed = JSON.parse(json);
    }
    catch {
        return null;
    }
    if (!parsed || typeof parsed !== "object")
        return null;
    const obj = parsed;
    const receipt = {
        source_lsn: typeof obj.source_lsn === "string" ? obj.source_lsn : "",
        outbox_seq: typeof obj.outbox_seq === "number" ? obj.outbox_seq : Number(obj.outbox_seq ?? 0),
        projection_task_ids: Array.isArray(obj.projection_task_ids)
            ? obj.projection_task_ids
            : [],
        manifest_checksum: typeof obj.manifest_checksum === "string" ? obj.manifest_checksum : "",
        written_at_unix_ms: typeof obj.written_at_unix_ms === "number"
            ? obj.written_at_unix_ms
            : Number(obj.written_at_unix_ms ?? 0),
    };
    // An empty (all-default) receipt is a valid no-op signal — still return it so
    // callers can detect it (source_lsn === "" and outbox_seq === 0).
    return receipt;
}
/** Extract a {@link WriteReceipt} from a mutation response body, reading
 *  `write_receipt_json` (MutationResponse field 7). Returns `null` when absent. */
function receiptFromResponse(resp) {
    if (!resp)
        return null;
    const json = resp.write_receipt_json;
    if (typeof json !== "string")
        return null;
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
function wasDuplicate(resp) {
    if (!resp || typeof resp !== "object")
        return false;
    const flag = resp.was_duplicate ?? resp.wasDuplicate;
    return flag === true;
}
/** Friendly, discoverable consistency-mode constants (token-valued, so
 *  `ConsistencyMode.Strong === "strong"`). Pass any of these — or a friendly
 *  alias like `"read-your-writes"` / `"bounded"` — to {@link withConsistency} /
 *  {@link consistencyContext}. */
exports.ConsistencyMode = {
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
};
// Friendly aliases → canonical token. Mirrors the broker's lenient `parse`
// (consistency.rs) plus the short spec names ("bounded" → bounded_staleness).
const CONSISTENCY_ALIASES = {
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
const CONSISTENCY_MODE_ENUM_NAME = {
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
function consistencyToken(mode) {
    const token = CONSISTENCY_ALIASES[String(mode).trim().toLowerCase()];
    if (!token) {
        throw new Error(`udb: unknown consistency mode "${mode}" (expected one of ` +
            `strong | read_your_writes | bounded_staleness | replica_bounded | eventual | projection_ok | cache_ok)`);
    }
    return token;
}
/** Returns a {@link CallOptions} that declares the read consistency `mode` on
 *  exactly this one call via the `x-udb-consistency` header (the header the
 *  broker reads at `security.rs`; it WINS over the body `consistency` field when
 *  both are present). Mirrors the Python `x-udb-consistency` selector. */
function withConsistency(mode) {
    return { headers: { "x-udb-consistency": consistencyToken(mode) } };
}
/** For body-context callers: returns a partial `RequestContext` setting both the
 *  legacy `consistency` string (context.proto field 18) and the typed
 *  `consistency_mode` enum name (field 22). Spread into the request `context`.
 *  The `x-udb-consistency` header still wins server-side when both are supplied. */
function consistencyContext(mode) {
    const token = consistencyToken(mode);
    return { consistency: token, consistency_mode: CONSISTENCY_MODE_ENUM_NAME[token] };
}
/** Returns a {@link CallOptions} that attaches `fence` to exactly the one
 *  follow-up read via the `x-udb-read-fence` header (the broker reads it at
 *  security.rs / service/mod.rs). The fence rides this single call only — it is
 *  NEVER stored on shared project metadata. */
function withReadFence(fence) {
    return { headers: { "x-udb-read-fence": JSON.stringify(fence) } };
}
/** For body-context callers: returns a partial `RequestContext` setting
 *  `read_fence_json` (context.proto field 14). Spread into the request `context`. */
function readFenceContext(fence) {
    return { read_fence_json: JSON.stringify(fence) };
}
/** One-shot helper (naming contract alias): convert a {@link WriteReceipt} into a
 *  {@link ReadFence} and attach it to the next read via `x-udb-read-fence`.
 *  Composes {@link readFenceFromReceipt} + {@link withReadFence} — no round trip. */
function withReadFenceFromReceipt(r, maxWaitMs) {
    return withReadFence(readFenceFromReceipt(r, maxWaitMs));
}
/** Canonical receipt accessor (naming contract): attach the read fence derived
 *  from a write `receipt` to exactly the next read. Alias of
 *  {@link withReadFenceFromReceipt}; the spec name is `metadata.afterWrite(receipt)`
 *  (exposed as `UdbProject.metadata.afterWrite`). The fence rides only this one
 *  CallOptions — never shared project metadata. */
function afterWrite(receipt, maxWaitMs = 5_000) {
    return withReadFenceFromReceipt(receipt, maxWaitMs);
}
/** Grouped receipt/fence accessors. `UdbProject.metadata` is an instance of this
 *  shape so callers can write `udb.metadata.afterWrite(receipt)` per the naming
 *  contract. Standalone functions of the same name remain exported directly. */
exports.consistencyMetadata = {
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
    ConsistencyMode: exports.ConsistencyMode,
};
