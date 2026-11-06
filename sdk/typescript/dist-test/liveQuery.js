"use strict";
// Live queries.
//
// `liveQuery` keeps a view of the rows matching a filter: it delivers the current
// rows, then every insert, update and delete as it happens, and reconnects on its
// own, resuming after the last change it delivered. Callers otherwise hand-roll the
// subscribe loop, the resume header, keepalive detection and reconnect backoff for
// every view.
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
exports.lqIsNotNull = exports.lqIsNull = exports.lqNotIn = exports.lqIn = exports.lqGe = exports.lqGt = exports.lqLe = exports.lqLt = exports.lqNe = exports.lqEq = exports.NoDeltaFeedError = void 0;
exports.liveQuery = liveQuery;
const grpc = __importStar(require("@grpc/grpc-js"));
const generatedClient_1 = require("./generatedClient");
const LIVE_QUERY_SERVICE = "udb.core.livequery.services.v1.LiveQueryService";
// Brokers before 0.5.29 read the resume point from this header only.
const LIVE_QUERY_RESUME_HEADER = "x-udb-livequery-resume";
const OP_UNSPECIFIED = "LIVE_QUERY_CHANGE_OP_UNSPECIFIED";
const MAX_RECONNECT_DELAY_MS = 30_000;
/** A broker without a CDC change feed: the snapshot was delivered but no change
 *  ever will be. It needs UDB_CDC_ENABLED (default on) and UDB_KAFKA_BROKERS on the
 *  broker; reconnecting cannot fix it. */
class NoDeltaFeedError extends Error {
    constructor(cause) {
        super("udb: live query: the broker has no CDC change feed (set UDB_KAFKA_BROKERS and leave UDB_CDC_ENABLED on)", { cause });
        this.name = "NoDeltaFeedError";
    }
}
exports.NoDeltaFeedError = NoDeltaFeedError;
function lqPredicate(field, op, value = "") {
    return { field, op: `LIVE_QUERY_COMPARISON_${op}`, value };
}
// Values are strings on the wire and compare numerically when both sides are numbers.
/** Rows whose field equals `value`. */
const lqEq = (field, value) => lqPredicate(field, "EQ", value);
exports.lqEq = lqEq;
/** Rows whose field differs from `value`. */
const lqNe = (field, value) => lqPredicate(field, "NE", value);
exports.lqNe = lqNe;
/** Rows whose field is below `value`. */
const lqLt = (field, value) => lqPredicate(field, "LT", value);
exports.lqLt = lqLt;
/** Rows whose field is at most `value`. */
const lqLe = (field, value) => lqPredicate(field, "LE", value);
exports.lqLe = lqLe;
/** Rows whose field is above `value`. */
const lqGt = (field, value) => lqPredicate(field, "GT", value);
exports.lqGt = lqGt;
/** Rows whose field is at least `value`. */
const lqGe = (field, value) => lqPredicate(field, "GE", value);
exports.lqGe = lqGe;
/** Rows whose field equals one of `values`. */
const lqIn = (field, ...values) => ({ ...lqPredicate(field, "IN"), values });
exports.lqIn = lqIn;
/** Rows whose field is set and equals none of `values`. */
const lqNotIn = (field, ...values) => ({
    ...lqPredicate(field, "NOT_IN"),
    values,
});
exports.lqNotIn = lqNotIn;
/** Rows whose field is null or absent. */
const lqIsNull = (field) => lqPredicate(field, "IS_NULL");
exports.lqIsNull = lqIsNull;
/** Rows whose field is set. */
const lqIsNotNull = (field) => lqPredicate(field, "IS_NOT_NULL");
exports.lqIsNotNull = lqIsNotNull;
/** A failure reconnecting cannot fix. */
class FatalLiveQueryError {
    err;
    constructor(err) {
        this.err = err;
    }
}
function asError(err) {
    return err instanceof Error ? err : new Error(String(err));
}
function udbErrorOf(err) {
    if (err instanceof generatedClient_1.UdbError)
        return err;
    const tagged = err?.udb;
    return tagged instanceof generatedClient_1.UdbError ? tagged : undefined;
}
/** Separates refusals reconnecting cannot fix from transient failures. */
function liveQueryStreamError(err) {
    const udb = udbErrorOf(err);
    const code = udb?.code ?? err?.code;
    switch (code) {
        case grpc.status.FAILED_PRECONDITION:
            if (udb?.detail?.capability_required === "cdc_change_feed") {
                return new FatalLiveQueryError(new NoDeltaFeedError(udb));
            }
            return new FatalLiveQueryError(udb ?? err);
        case grpc.status.INVALID_ARGUMENT:
        case grpc.status.PERMISSION_DENIED:
        case grpc.status.NOT_FOUND:
        case grpc.status.UNIMPLEMENTED:
            return new FatalLiveQueryError(udb ?? err);
    }
    return udb ?? err;
}
function decodeLiveRow(raw, decode) {
    let record;
    try {
        record = JSON.parse(raw);
    }
    catch (err) {
        throw new FatalLiveQueryError(new Error(`udb: live query row is not a JSON object: ${asError(err).message}`));
    }
    if (record === null)
        record = {};
    if (typeof record !== "object" || Array.isArray(record)) {
        throw new FatalLiveQueryError(new Error(`udb: live query row is not a JSON object: ${raw.slice(0, 80)}`));
    }
    if (!decode)
        return record;
    try {
        return decode(record);
    }
    catch (err) {
        throw new FatalLiveQueryError(err);
    }
}
function waitFor(ms, signal) {
    return new Promise((resolve, reject) => {
        const onAbort = () => {
            clearTimeout(timer);
            reject(signal.reason);
        };
        const timer = setTimeout(() => {
            signal?.removeEventListener("abort", onAbort);
            resolve();
        }, ms);
        signal?.addEventListener("abort", onAbort, { once: true });
    });
}
/** Watch the rows of `messageType` matching `opts` in the caller's tenant and
 *  project, calling `handle` for the snapshot and for every change. It runs until
 *  `opts.signal` aborts, `handle` throws, or the broker refuses the view, and always
 *  ends by rejecting with that reason. Keepalive frames are absorbed. Transient
 *  failures reconnect with backoff and resume after the last delivered change. A
 *  broker without a change feed rejects with `NoDeltaFeedError` after delivering the
 *  snapshot. */
async function liveQuery(udb, messageType, opts, handle) {
    const core = udb.nativeCore;
    const meta = core.opts.meta;
    const backoffMs = opts.backoffMs && opts.backoffMs > 0 ? opts.backoffMs : 500;
    const { signal } = opts;
    const request = {
        tenant_id: meta.tenantId,
        project_id: meta.projectId ?? "",
        message_type: messageType,
        filters: opts.where ?? [],
        snapshot_limit: opts.snapshotLimit ?? 0,
        any_of: (opts.anyOf ?? []).map((predicates) => ({ predicates })),
    };
    let lastEventId = opts.sinceEventId ?? "";
    let delay = backoffMs;
    let connects = 0;
    // One subscription. Throws when it ends; `progressed` says anything was delivered.
    let progressed = false;
    const subscribeOnce = async (resumed) => {
        const since = lastEventId;
        const stream = core.serverStream(LIVE_QUERY_SERVICE, "Subscribe", { ...request, since_event_id: since }, {
            signal,
            ...(since ? { headers: { [LIVE_QUERY_RESUME_HEADER]: since } } : {}),
        });
        try {
            for await (const frame of stream) {
                const kind = frame?.payload ?? (frame?.snapshot ? "snapshot" : frame?.change ? "change" : "");
                if (kind === "snapshot") {
                    const rows = (frame.snapshot?.rows_json ?? []).map((raw) => decodeLiveRow(raw, opts.decode));
                    await deliver(handle, { isSnapshot: true, snapshot: rows, resumed, op: OP_UNSPECIFIED, eventId: "" });
                    progressed = true;
                }
                else if (kind === "change") {
                    const change = frame.change ?? {};
                    const eventId = change.event_id ?? "";
                    const op = change.op ?? OP_UNSPECIFIED;
                    // Legacy keepalive from brokers before the explicit heartbeat frame.
                    if (!eventId && op === OP_UNSPECIFIED)
                        continue;
                    const row = decodeLiveRow(change.row_json ?? "", opts.decode);
                    await deliver(handle, { isSnapshot: false, resumed: false, op, row, eventId });
                    if (eventId)
                        lastEventId = eventId;
                    progressed = true;
                }
            }
        }
        catch (err) {
            if (err instanceof FatalLiveQueryError)
                throw err;
            throw liveQueryStreamError(err);
        }
        finally {
            stream.cancel();
        }
        throw new Error("the live query stream ended");
    };
    // eslint-disable-next-line no-constant-condition
    while (true) {
        if (signal?.aborted)
            throw signal.reason;
        let failure;
        progressed = false;
        try {
            await subscribeOnce(connects > 0);
        }
        catch (err) {
            failure = err;
        }
        connects += 1;
        if (signal?.aborted)
            throw signal.reason;
        if (failure instanceof FatalLiveQueryError)
            throw failure.err;
        if (progressed)
            delay = backoffMs;
        opts.onError?.(new Error(`udb: live query ${messageType} reconnecting: ${asError(failure).message}`, { cause: failure }));
        const retryAfterMs = Number(udbErrorOf(failure)?.detail?.retry_after_ms ?? 0) || 0;
        await waitFor(Math.max(delay, retryAfterMs), signal);
        delay = Math.min(delay * 2, MAX_RECONNECT_DELAY_MS);
    }
}
async function deliver(handle, event) {
    try {
        await handle(event);
    }
    catch (err) {
        throw new FatalLiveQueryError(err);
    }
}
