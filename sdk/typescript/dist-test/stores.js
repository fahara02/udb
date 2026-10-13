"use strict";
// Typed stores, transactions, durable consumers and error codes — the
// TypeScript port of the Go SDK's Table[T], Tx, Consume and ErrorCode (T1).
//
// Everything here goes through a `StorePlane` (anything with the generated
// `UdbCore.unary`/`serverStream`/`bidiStream` shape), so tests can swap in
// `InMemoryDataPlane` instead of a broker.
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.InMemoryDataPlane = exports.TxScope = exports.EVENT_ENVELOPE_VERSION = exports.Table = exports.isUnique = exports.isConflict = exports.isNotFound = exports.codeOf = exports.ConflictError = exports.NotFoundError = exports.ErrorKind = exports.ErrorCode = void 0;
exports.inspect = inspect;
exports.tx = tx;
exports.decodeEnvelope = decodeEnvelope;
exports.consume = consume;
const node_crypto_1 = require("node:crypto");
const grpc = __importStar(require("@grpc/grpc-js"));
const generatedClient_1 = require("./generatedClient");
const DATABROKER = "udb.services.v1.DataBroker";
// ── Error codes ─────────────────────────────────────────────────────────────
/** A closed set of outcomes to branch on, instead of matching messages. */
var ErrorCode;
(function (ErrorCode) {
    ErrorCode["Unknown"] = "unknown";
    ErrorCode["NotFound"] = "not_found";
    ErrorCode["Conflict"] = "conflict";
    ErrorCode["Unique"] = "unique";
    ErrorCode["NotNull"] = "not_null";
    ErrorCode["ForeignKey"] = "foreign_key";
    ErrorCode["Invalid"] = "invalid";
    ErrorCode["PolicyDenied"] = "policy_denied";
    ErrorCode["ScopeMissing"] = "scope_missing";
    ErrorCode["Unauthenticated"] = "unauthenticated";
    ErrorCode["RateLimited"] = "rate_limited";
    ErrorCode["Unavailable"] = "unavailable";
    ErrorCode["Redacted"] = "redacted";
    ErrorCode["Precondition"] = "precondition";
    ErrorCode["Internal"] = "internal";
})(ErrorCode || (exports.ErrorCode = ErrorCode = {}));
const REASON_CODES = {
    UDB_CAS_CONFLICT: ErrorCode.Conflict,
    UDB_CAS_ROW_MISSING: ErrorCode.Conflict,
    UDB_REVISION_CONFLICT: ErrorCode.Conflict,
    UDB_GRANT_OWNED_BY_OTHER: ErrorCode.Conflict,
    UDB_CAS_KEY_NOT_PK: ErrorCode.Precondition,
    UDB_IDEMPOTENCY_REUSE: ErrorCode.Precondition,
    UDB_NO_ROWS_AFFECTED: ErrorCode.NotFound,
    UDB_UNIQUE_VIOLATION: ErrorCode.Unique,
    UDB_NOT_NULL_VIOLATION: ErrorCode.NotNull,
    UDB_UPSERT_INCOMPLETE_ROW: ErrorCode.NotNull,
    UDB_FOREIGN_KEY_VIOLATION: ErrorCode.ForeignKey,
    UDB_TYPE_MISMATCH: ErrorCode.Invalid,
    UDB_UNSUPPORTED_FILTER_OPERATOR: ErrorCode.Invalid,
    UDB_NULL_COMPARISON: ErrorCode.Invalid,
    UDB_TENANT_MISMATCH: ErrorCode.PolicyDenied,
    UDB_TABLE_NOT_TENANT_SCOPED: ErrorCode.Invalid,
    UDB_SCOPE_MISSING: ErrorCode.ScopeMissing,
    UDB_POLICY_DENIED: ErrorCode.PolicyDenied,
    UDB_REDACTED_VALUE_WRITE: ErrorCode.Redacted,
    UDB_RATE_LIMITED: ErrorCode.RateLimited,
    UDB_DECODE_FAILED: ErrorCode.Internal,
    UDB_UNKNOWN_MESSAGE_TYPE: ErrorCode.Invalid,
    UDB_ENVELOPE_VERSION_UNSUPPORTED: ErrorCode.Precondition,
    UDB_POLICY_WRONG_SURFACE: ErrorCode.Precondition,
};
/** Wire values of `udb.entity.v1.ErrorKind` (proto/udb/entity/v1/error.proto). */
exports.ErrorKind = {
    UNSPECIFIED: 0,
    CAPABILITY: 1,
    POLICY: 2,
    QUOTA: 3,
    SCHEMA: 4,
    RETRYABLE: 5,
    INTERNAL: 6,
    VALIDATION: 7,
    CONFLICT: 8,
    NOT_FOUND: 9,
    UNIQUE: 10,
    NOT_NULL: 11,
    FOREIGN_KEY: 12,
    PERMISSION: 13,
    REDACTED: 14,
    RATE_LIMITED: 15,
};
const KIND_CODES = {
    [exports.ErrorKind.CONFLICT]: ErrorCode.Conflict,
    [exports.ErrorKind.NOT_FOUND]: ErrorCode.NotFound,
    [exports.ErrorKind.UNIQUE]: ErrorCode.Unique,
    [exports.ErrorKind.NOT_NULL]: ErrorCode.NotNull,
    [exports.ErrorKind.FOREIGN_KEY]: ErrorCode.ForeignKey,
    [exports.ErrorKind.PERMISSION]: ErrorCode.PolicyDenied,
    [exports.ErrorKind.REDACTED]: ErrorCode.Redacted,
    [exports.ErrorKind.RATE_LIMITED]: ErrorCode.RateLimited,
    [exports.ErrorKind.QUOTA]: ErrorCode.RateLimited,
    [exports.ErrorKind.RETRYABLE]: ErrorCode.Unavailable,
    [exports.ErrorKind.VALIDATION]: ErrorCode.Invalid,
    [exports.ErrorKind.INTERNAL]: ErrorCode.Internal,
};
const GRPC_CODES = {
    5: ErrorCode.NotFound,
    6: ErrorCode.Unique,
    9: ErrorCode.Precondition,
    7: ErrorCode.PolicyDenied,
    8: ErrorCode.RateLimited,
    14: ErrorCode.Unavailable,
    4: ErrorCode.Unavailable,
    3: ErrorCode.Invalid,
    16: ErrorCode.Unauthenticated,
    13: ErrorCode.Internal,
};
/** The row a keyed read or conditional write named does not exist. */
class NotFoundError extends Error {
    code = ErrorCode.NotFound;
}
exports.NotFoundError = NotFoundError;
/** A compare-and-swap precondition did not hold. */
class ConflictError extends Error {
    code = ErrorCode.Conflict;
}
exports.ConflictError = ConflictError;
/** Classify any error from a store, the generated client or a raw gRPC call. */
function inspect(err) {
    if (err instanceof NotFoundError)
        return { code: ErrorCode.NotFound };
    if (err instanceof ConflictError)
        return { code: ErrorCode.Conflict };
    const udb = err instanceof generatedClient_1.UdbError ? err : err?.udb instanceof generatedClient_1.UdbError ? err.udb : undefined;
    if (!udb)
        return { code: ErrorCode.Unknown };
    const detail = udb.detail ?? {};
    const reason = typeof detail.reason === "string" && detail.reason.startsWith("UDB_") ? detail.reason : undefined;
    return {
        code: (reason && REASON_CODES[reason]) || KIND_CODES[detail.kind] || GRPC_CODES[udb.code] || ErrorCode.Unknown,
        reason,
        constraint: detail.constraint,
        column: detail.column,
        fixHint: detail.fix_hint,
        missing: detail.missing,
        retryAfterMs: detail.retry_after_ms,
    };
}
const codeOf = (err) => inspect(err).code;
exports.codeOf = codeOf;
const isNotFound = (err) => (0, exports.codeOf)(err) === ErrorCode.NotFound;
exports.isNotFound = isNotFound;
const isConflict = (err) => (0, exports.codeOf)(err) === ErrorCode.Conflict;
exports.isConflict = isConflict;
const isUnique = (err, constraint) => {
    const info = inspect(err);
    return info.code === ErrorCode.Unique && (!constraint || info.constraint === constraint);
};
exports.isUnique = isUnique;
function jsonBytes(value) {
    return Buffer.from(JSON.stringify(value), "utf8");
}
function decodeRows(response) {
    const raw = response?.records_json;
    const items = Array.isArray(raw) ? raw : raw ? [raw] : [];
    const rows = [];
    for (const item of items) {
        const text = Buffer.isBuffer(item) ? item.toString("utf8") : typeof item === "string" ? item : "";
        if (!text)
            continue;
        const parsed = JSON.parse(text);
        if (Array.isArray(parsed))
            rows.push(...parsed);
        else
            rows.push(parsed);
    }
    return rows;
}
/** A typed store over one entity. Writes go through the broker's data plane, so
 *  policies, tenancy, projections and events apply as for any write. */
class Table {
    plane;
    messageType;
    opts;
    constructor(plane, messageType, opts) {
        this.plane = plane;
        this.messageType = messageType;
        this.opts = opts;
        if (!opts.key.length)
            throw new Error(`udb: Table(${messageType}) needs its key fields`);
    }
    /** The filter that addresses exactly one row. */
    keyFilter(key) {
        const filter = {};
        for (const field of this.opts.key) {
            if (!(field in key))
                throw new Error(`udb: ${this.messageType} key is missing ${field}`);
            filter[field] = key[field];
        }
        return filter;
    }
    async get(key) {
        const page = await this.select(this.keyFilter(key), { limit: 1 });
        if (!page.rows.length)
            throw new NotFoundError(`udb: ${this.messageType} ${JSON.stringify(key)} not found`);
        return page.rows[0];
    }
    async select(filter = {}, opts = {}) {
        const response = await this.plane.unary(DATABROKER, "Select", {
            message_type: this.messageType,
            filter,
            limit: opts.limit ?? 0,
            include_total: opts.includeTotal ?? false,
            context: opts.primaryRead ? { primary_read: true } : undefined,
        });
        return {
            rows: decodeRows(response),
            hasMore: Boolean(response?.has_more),
            total: opts.includeTotal ? Number(response?.exact_total ?? 0) : undefined,
        };
    }
    async count(filter = {}) {
        return (await this.select(filter, { limit: 1, includeTotal: true })).total ?? 0;
    }
    /** Insert or replace the whole row; NotFoundError when the broker's scope
     *  guard leaves a conflicting row untouched. */
    async upsert(row) {
        const response = await this.plane.unary(DATABROKER, "Upsert", {
            message_type: this.messageType,
            record_json: jsonBytes(row),
            conflict_fields: this.opts.key,
        });
        if (String(response?.affected_rows ?? "0") === "0") {
            throw new NotFoundError(`${this.messageType}: write target not found in the verified scope`);
        }
    }
    /** Set some columns of an existing row; NotFoundError when it does not exist. */
    async patch(key, changes) {
        await this.update(key, changes);
    }
    /** Set columns only while the row still holds `expected`; ConflictError when
     *  it changed, NotFoundError when it is gone. */
    async updateIf(key, expected, changes) {
        await this.update(key, changes, expected);
    }
    async delete(key) {
        try {
            await this.plane.unary(DATABROKER, "Delete", {
                message_type: this.messageType,
                filter: this.keyFilter(key),
                require_affected: 1,
            });
        }
        catch (err) {
            throw classify(err, this.messageType, key);
        }
    }
    async update(key, changes, expected) {
        try {
            await this.plane.unary(DATABROKER, "Update", {
                message_type: this.messageType,
                filter: this.keyFilter(key),
                changes,
                expected,
                require_affected: 1,
            });
        }
        catch (err) {
            throw classify(err, this.messageType, key);
        }
    }
}
exports.Table = Table;
function classify(err, messageType, key) {
    switch ((0, exports.codeOf)(err)) {
        case ErrorCode.NotFound:
            return new NotFoundError(`udb: ${messageType} ${JSON.stringify(key)} not found`);
        case ErrorCode.Conflict:
            return new ConflictError(`udb: ${messageType} ${JSON.stringify(key)} changed`);
        default:
            return err;
    }
}
exports.EVENT_ENVELOPE_VERSION = 2;
/** Writes collected for one atomic commit. */
class TxScope {
    correlationId;
    mutations = [];
    constructor(correlationId) {
        this.correlationId = correlationId;
    }
    upsert(messageType, row) {
        this.mutations.push({ operation: "upsert", message_type: messageType, record_json: jsonBytes(row) });
    }
    patch(messageType, key, changes) {
        this.mutations.push({ operation: "update", message_type: messageType, filter: key, changes });
    }
    updateIf(messageType, key, expected, changes) {
        this.mutations.push({ operation: "update", message_type: messageType, filter: key, changes, expected });
    }
    delete(messageType, key) {
        this.mutations.push({ operation: "delete", message_type: messageType, filter: key });
    }
    emit(event) {
        this.mutations.push({
            operation: "enqueue_outbox_event",
            message_type: event.messageType ?? event.eventType,
            collection: event.topic,
            object_key: event.documentId,
            payload: {
                event_id: (0, node_crypto_1.randomUUID)(),
                event_type: event.eventType,
                correlation_id: this.correlationId,
                document_id: event.documentId,
                envelope_version: exports.EVENT_ENVELOPE_VERSION,
                payload: event.payload,
            },
        });
    }
}
exports.TxScope = TxScope;
/** Run `body`, then commit its writes and events atomically over BeginTx. A
 *  failed mutation rejects with its own error (Conflict, Unique, ...). */
async function tx(plane, body) {
    if (!plane.bidiStream)
        throw new Error("udb: this transport does not support transactions");
    const scope = new TxScope((0, node_crypto_1.randomUUID)());
    await body(scope);
    const txId = (0, node_crypto_1.randomUUID)();
    const stream = plane.bidiStream(DATABROKER, "BeginTx");
    await new Promise((resolve, reject) => {
        let committed = false;
        let failure;
        stream.on("data", (status) => {
            const state = String(status?.state ?? "");
            if (state === "TX_STATE_COMMITTED" || state === "2")
                committed = true;
            if (state === "TX_STATE_ERROR" || state === "TX_STATE_ROLLED_BACK" || state === "4" || state === "3") {
                const message = `udb: transaction ${txId} rolled back: ${status?.message ?? ""}`;
                failure = status?.error_detail ? new generatedClient_1.UdbError("DataBroker/BeginTx", Object.assign(new Error(message), { code: Number(status.code) || grpc.status.UNKNOWN,
                    details: message, metadata: new grpc.Metadata() }), status.error_detail) : new Error(message);
            }
        });
        // The trailer carries the typed mutation error; an ERROR data frame must
        // not reject first and hide the Conflict/Unique classification.
        stream.on("error", (err) => reject(err?.udb ?? err));
        stream.on("end", () => {
            if (failure || !committed)
                reject(failure ?? new Error(`udb: transaction ${txId} ended without a commit acknowledgement`));
            else
                resolve();
        });
        for (const mutation of scope.mutations)
            stream.write({ ...mutation, tx_id: txId });
        stream.write({ tx_id: txId, commit: true });
        stream.end();
    });
}
class EnvelopeVersionError extends Error {
    reason = "UDB_ENVELOPE_VERSION_UNSUPPORTED";
}
/** Unwrap the outbox envelope (a journal row may nest it once more). */
function decodeEnvelope(payloadJson) {
    let envelope = JSON.parse(payloadJson || "{}");
    const inner = envelope?.payload;
    if (inner && typeof inner === "object" && "event_type" in inner && "payload" in inner)
        envelope = inner;
    const version = Number(envelope?.envelope_version ?? 0);
    if (version > exports.EVENT_ENVELOPE_VERSION) {
        throw new EnvelopeVersionError(`udb: envelope_version ${version} is newer than this SDK understands (${exports.EVENT_ENVELOPE_VERSION}); upgrade the SDK`);
    }
    return envelope;
}
/** Deliver every event on `topicPattern` to `handle` as the durable consumer
 *  `name`: the broker keeps the position (AckCdcEvents), reconnects resume
 *  there, and an event is acked only after `handle` resolves. Returns when
 *  `signal` aborts or a refusal reconnecting cannot fix arrives. */
async function consume(plane, name, topicPattern, handle, opts = {}) {
    if (!plane.serverStream)
        throw new Error("udb: this transport does not support streams");
    const seen = new Set();
    let delay = opts.backoffMs ?? 500;
    for (;;) {
        if (opts.signal?.aborted)
            return;
        const failure = await new Promise((resolve) => {
            const stream = plane.serverStream(DATABROKER, "PublishCDC", { topic_pattern: topicPattern, consumer_name: name });
            let chain = Promise.resolve();
            let closing = false;
            let stopped = false;
            let handlerFailure;
            const finish = (terminalError) => {
                if (closing)
                    return;
                closing = true;
                // Drain queued work before reconnecting. A failed head event stops the
                // whole queue so a later ack cannot advance the cursor past it.
                void chain.then(() => {
                    opts.signal?.removeEventListener("abort", abort);
                    resolve(handlerFailure ?? terminalError);
                });
            };
            const abort = () => {
                stream.cancel?.();
                finish(undefined);
            };
            opts.signal?.addEventListener("abort", abort, { once: true });
            stream.on("data", (envelope) => {
                if (closing)
                    return;
                chain = chain.then(async () => {
                    if (stopped || opts.signal?.aborted)
                        return;
                    const id = String(envelope?.event_id ?? "");
                    if (seen.has(id))
                        return;
                    const decoded = decodeEnvelope(String(envelope?.payload_json ?? ""));
                    await handle({
                        id,
                        topic: String(envelope?.topic ?? ""),
                        eventType: String(decoded.event_type ?? ""),
                        partitionKey: String(envelope?.partition_key ?? ""),
                        payload: decoded.payload ?? {},
                        envelope: decoded,
                    });
                    await plane.unary(DATABROKER, "AckCdcEvents", { consumer_name: name, topic_pattern: topicPattern, event_id: id });
                    seen.add(id);
                    if (seen.size > 4096)
                        seen.delete(seen.values().next().value);
                    delay = opts.backoffMs ?? 500;
                }).catch((err) => {
                    stopped = true;
                    handlerFailure = err;
                    finish(err);
                    stream.cancel?.();
                });
            });
            stream.on("error", (err) => {
                finish(err?.udb ?? err);
            });
            stream.on("end", () => finish(new Error("udb: the event stream ended")));
            if (opts.signal?.aborted)
                abort();
        });
        if (opts.signal?.aborted)
            return;
        const error = failure?.udb ?? failure;
        if (error instanceof EnvelopeVersionError || [3, 5, 7].includes(error?.code))
            throw error;
        opts.onError?.(error);
        if (opts.signal?.aborted)
            return;
        await new Promise((resolve) => {
            const done = () => {
                clearTimeout(timer);
                opts.signal?.removeEventListener("abort", done);
                resolve();
            };
            const timer = setTimeout(done, delay);
            opts.signal?.addEventListener("abort", done, { once: true });
            if (opts.signal?.aborted)
                done();
        });
        delay = Math.min(delay * 2, 30_000);
    }
}
// ── In-memory data plane (tests) ────────────────────────────────────────────
/** A broker stand-in for unit tests of code that uses `Table`: Select, Upsert,
 *  Update (with expected/require_affected) and Delete over plain rows, with the
 *  same NotFound/Conflict outcomes and reason codes the broker sends. */
class InMemoryDataPlane {
    keys;
    rows = new Map();
    constructor(keys) {
        this.keys = keys;
    }
    matches(row, filter = {}) {
        return Object.entries(filter ?? {}).every(([field, value]) => row[field] === value);
    }
    refuse(code, reason, message) {
        const err = new Error(message);
        err.code = code;
        throw new generatedClient_1.UdbError("InMemory", err, { reason, kind: code === 9 ? 8 : 9 });
    }
    async unary(_service, method, request) {
        const table = this.rows.get(request.message_type) ?? [];
        this.rows.set(request.message_type, table);
        switch (method) {
            case "Select": {
                const found = table.filter((row) => this.matches(row, request.filter));
                const limited = request.limit ? found.slice(0, request.limit) : found;
                return {
                    records_json: limited.map((row) => Buffer.from(JSON.stringify(row))),
                    has_more: limited.length < found.length,
                    exact_total: request.include_total ? found.length : 0,
                };
            }
            case "Upsert": {
                const row = JSON.parse(Buffer.from(request.record_json).toString("utf8"));
                const key = this.keys[request.message_type] ?? request.conflict_fields ?? [];
                const index = table.findIndex((existing) => key.every((field) => existing[field] === row[field]));
                if (index >= 0)
                    table[index] = row;
                else
                    table.push(row);
                return { affected_rows: 1 };
            }
            case "Update": {
                const hits = table.filter((row) => this.matches(row, request.filter));
                if (request.require_affected && hits.length !== request.require_affected) {
                    if (request.expected && hits.length === 0)
                        this.refuse(9, "UDB_CAS_ROW_MISSING", "row missing");
                    this.refuse(5, "UDB_NO_ROWS_AFFECTED", "no rows affected");
                }
                if (request.expected && hits.some((row) => !this.matches(row, request.expected))) {
                    this.refuse(9, "UDB_CAS_CONFLICT", "precondition failed");
                }
                for (const row of hits)
                    Object.assign(row, request.changes ?? {});
                return { affected_rows: hits.length };
            }
            case "Delete": {
                const keep = table.filter((row) => !this.matches(row, request.filter));
                const removed = table.length - keep.length;
                if (request.require_affected && removed !== request.require_affected) {
                    this.refuse(5, "UDB_NO_ROWS_AFFECTED", "no rows affected");
                }
                this.rows.set(request.message_type, keep);
                return { affected_rows: removed };
            }
            default:
                throw new Error(`udb: InMemoryDataPlane does not implement ${method}`);
        }
    }
}
exports.InMemoryDataPlane = InMemoryDataPlane;
