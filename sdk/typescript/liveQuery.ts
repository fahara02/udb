// Live queries.
//
// `liveQuery` keeps a view of the rows matching a filter: it delivers the current
// rows, then every insert, update and delete as it happens, and reconnects on its
// own, resuming after the last change it delivered. Callers otherwise hand-roll the
// subscribe loop, the resume header, keepalive detection and reconnect backoff for
// every view.

import * as grpc from "@grpc/grpc-js";

import { UdbCore, UdbError } from "./generatedClient";

const LIVE_QUERY_SERVICE = "udb.core.livequery.services.v1.LiveQueryService";
// Brokers before 0.5.29 read the resume point from this header only.
const LIVE_QUERY_RESUME_HEADER = "x-udb-livequery-resume";
const OP_UNSPECIFIED = "LIVE_QUERY_CHANGE_OP_UNSPECIFIED";
const MAX_RECONNECT_DELAY_MS = 30_000;

/** A broker without a CDC change feed: the snapshot was delivered but no change
 *  ever will be. It needs UDB_CDC_ENABLED (default on) and UDB_KAFKA_BROKERS on the
 *  broker; reconnecting cannot fix it. */
export class NoDeltaFeedError extends Error {
  constructor(cause?: unknown) {
    super(
      "udb: live query: the broker has no CDC change feed (set UDB_KAFKA_BROKERS and leave UDB_CDC_ENABLED on)",
      { cause },
    );
    this.name = "NoDeltaFeedError";
  }
}

export type LiveQueryChangeOp =
  | "LIVE_QUERY_CHANGE_OP_UNSPECIFIED"
  | "LIVE_QUERY_CHANGE_OP_INSERT"
  | "LIVE_QUERY_CHANGE_OP_UPDATE"
  | "LIVE_QUERY_CHANGE_OP_DELETE";

/** One delivery: either every matching row (on the first connect and after each
 *  reconnect) or one change. */
export interface LiveQueryEvent<T> {
  /** True for a snapshot delivery, false for a change. */
  isSnapshot: boolean;
  /** The rows matching now, on snapshot deliveries. */
  snapshot?: T[];
  /** True for a snapshot that follows a reconnect. Changes missed while disconnected
   *  are replayed after it, so a view that applies changes by key converges. */
  resumed: boolean;
  /** The change kind; `..._UNSPECIFIED` on snapshots. */
  op: LiveQueryChangeOp;
  /** The changed row, on change deliveries. */
  row?: T;
  /** Identifies the change; `liveQuery` resumes after the last one. */
  eventId: string;
}

export interface LiveQueryPredicate {
  field: string;
  op: string;
  value: string;
  values?: string[];
}

/** Describes the view. The empty value watches every row of the caller's tenant. */
export interface LiveQueryOptions<T> {
  /** Predicates AND-ed together (see lqEq, lqIn, lqIsNull, ...). */
  where?: LiveQueryPredicate[];
  /** Groups that are each an OR of predicates, AND-ed with `where` and each other:
   *  status IN (open, held) AND (owner = me OR assignee = me). */
  anyOf?: LiveQueryPredicate[][];
  /** Caps the snapshot rows (the broker clamps it). */
  snapshotLimit?: number;
  /** Resumes after a change delivered by an earlier process. */
  sinceEventId?: string;
  /** The first reconnect delay in ms, doubled up to 30s (default 500). */
  backoffMs?: number;
  /** Observes reconnects. */
  onError?: (err: Error) => void;
  /** Ends the view; `liveQuery` then rejects with the signal's reason. */
  signal?: AbortSignal;
  /** Turns a row's JSON object into `T`; a failure ends the view. Default: the
   *  object as sent. */
  decode?: (record: Record<string, unknown>) => T;
}

function lqPredicate(field: string, op: string, value = ""): LiveQueryPredicate {
  return { field, op: `LIVE_QUERY_COMPARISON_${op}`, value };
}

// Values are strings on the wire and compare numerically when both sides are numbers.
/** Rows whose field equals `value`. */
export const lqEq = (field: string, value: string) => lqPredicate(field, "EQ", value);
/** Rows whose field differs from `value`. */
export const lqNe = (field: string, value: string) => lqPredicate(field, "NE", value);
/** Rows whose field is below `value`. */
export const lqLt = (field: string, value: string) => lqPredicate(field, "LT", value);
/** Rows whose field is at most `value`. */
export const lqLe = (field: string, value: string) => lqPredicate(field, "LE", value);
/** Rows whose field is above `value`. */
export const lqGt = (field: string, value: string) => lqPredicate(field, "GT", value);
/** Rows whose field is at least `value`. */
export const lqGe = (field: string, value: string) => lqPredicate(field, "GE", value);
/** Rows whose field equals one of `values`. */
export const lqIn = (field: string, ...values: string[]): LiveQueryPredicate => ({ ...lqPredicate(field, "IN"), values });
/** Rows whose field is set and equals none of `values`. */
export const lqNotIn = (field: string, ...values: string[]): LiveQueryPredicate => ({
  ...lqPredicate(field, "NOT_IN"),
  values,
});
/** Rows whose field is null or absent. */
export const lqIsNull = (field: string) => lqPredicate(field, "IS_NULL");
/** Rows whose field is set. */
export const lqIsNotNull = (field: string) => lqPredicate(field, "IS_NOT_NULL");

/** A failure reconnecting cannot fix. */
class FatalLiveQueryError {
  constructor(readonly err: unknown) {}
}

function asError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

function udbErrorOf(err: unknown): UdbError | undefined {
  if (err instanceof UdbError) return err;
  const tagged = (err as { udb?: unknown } | null)?.udb;
  return tagged instanceof UdbError ? tagged : undefined;
}

/** Separates refusals reconnecting cannot fix from transient failures. */
function liveQueryStreamError(err: unknown): unknown {
  const udb = udbErrorOf(err);
  const code = udb?.code ?? (err as { code?: unknown } | null)?.code;
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

function decodeLiveRow<T>(raw: string, decode?: (record: Record<string, unknown>) => T): T {
  let record: unknown;
  try {
    record = JSON.parse(raw);
  } catch (err) {
    throw new FatalLiveQueryError(new Error(`udb: live query row is not a JSON object: ${asError(err).message}`));
  }
  if (record === null) record = {};
  if (typeof record !== "object" || Array.isArray(record)) {
    throw new FatalLiveQueryError(new Error(`udb: live query row is not a JSON object: ${raw.slice(0, 80)}`));
  }
  if (!decode) return record as T;
  try {
    return decode(record as Record<string, unknown>);
  } catch (err) {
    throw new FatalLiveQueryError(err);
  }
}

function waitFor(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal!.reason);
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
export async function liveQuery<T = Record<string, unknown>>(
  udb: { nativeCore: UdbCore },
  messageType: string,
  opts: LiveQueryOptions<T>,
  handle: (event: LiveQueryEvent<T>) => void | Promise<void>,
): Promise<never> {
  const core = udb.nativeCore;
  const meta = core.opts.meta;
  const backoffMs = opts.backoffMs && opts.backoffMs > 0 ? opts.backoffMs : 500;
  const { signal } = opts;
  const request: Record<string, unknown> = {
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
  const subscribeOnce = async (resumed: boolean): Promise<never> => {
    const since = lastEventId;
    const stream = core.serverStream(
      LIVE_QUERY_SERVICE,
      "Subscribe",
      { ...request, since_event_id: since },
      {
        signal,
        ...(since ? { headers: { [LIVE_QUERY_RESUME_HEADER]: since } } : {}),
      },
    );
    try {
      for await (const frame of stream as AsyncIterable<any>) {
        const kind = frame?.payload ?? (frame?.snapshot ? "snapshot" : frame?.change ? "change" : "");
        if (kind === "snapshot") {
          const rows = (frame.snapshot?.rows_json ?? []).map((raw: string) => decodeLiveRow<T>(raw, opts.decode));
          await deliver(handle, { isSnapshot: true, snapshot: rows, resumed, op: OP_UNSPECIFIED, eventId: "" });
          progressed = true;
        } else if (kind === "change") {
          const change = frame.change ?? {};
          const eventId: string = change.event_id ?? "";
          const op: LiveQueryChangeOp = change.op ?? OP_UNSPECIFIED;
          // Legacy keepalive from brokers before the explicit heartbeat frame.
          if (!eventId && op === OP_UNSPECIFIED) continue;
          const row = decodeLiveRow<T>(change.row_json ?? "", opts.decode);
          await deliver(handle, { isSnapshot: false, resumed: false, op, row, eventId });
          if (eventId) lastEventId = eventId;
          progressed = true;
        }
      }
    } catch (err) {
      if (err instanceof FatalLiveQueryError) throw err;
      throw liveQueryStreamError(err);
    } finally {
      stream.cancel();
    }
    throw new Error("the live query stream ended");
  };

  // eslint-disable-next-line no-constant-condition
  while (true) {
    if (signal?.aborted) throw signal.reason;
    let failure: unknown;
    progressed = false;
    try {
      await subscribeOnce(connects > 0);
    } catch (err) {
      failure = err;
    }
    connects += 1;
    if (signal?.aborted) throw signal.reason;
    if (failure instanceof FatalLiveQueryError) throw failure.err;
    if (progressed) delay = backoffMs;
    opts.onError?.(
      new Error(`udb: live query ${messageType} reconnecting: ${asError(failure).message}`, { cause: failure }),
    );
    const retryAfterMs = Number(udbErrorOf(failure)?.detail?.retry_after_ms ?? 0) || 0;
    await waitFor(Math.max(delay, retryAfterMs), signal);
    delay = Math.min(delay * 2, MAX_RECONNECT_DELAY_MS);
  }
}

async function deliver<T>(
  handle: (event: LiveQueryEvent<T>) => void | Promise<void>,
  event: LiveQueryEvent<T>,
): Promise<void> {
  try {
    await handle(event);
  } catch (err) {
    throw new FatalLiveQueryError(err);
  }
}
