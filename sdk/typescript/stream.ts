// Stream convenience helpers: "send one message, close, await first response"
// for client-streaming and bidi RPCs.
//
// Guardrail: reconnect is safe only BEFORE a first observed response. These
// helpers preserve cancellation/deadlines (via `call.signal`, already wired in
// the underlying core invokers) and NEVER auto-retry — to retry, open a fresh
// stream.

import type { CallOptions, UdbCore } from "./generatedClient";

/**
 * Client-streaming send-one helper: open the stream, write exactly one message,
 * end it, and resolve the single response. No retry.
 *
 * RPC sequence: exactly one `clientStream` open → one `write` → one `end`.
 */
export async function sendOneClientStream<TRes = any>(
  core: UdbCore,
  service: string,
  method: string,
  msg: any,
  call?: CallOptions,
): Promise<TRes> {
  const { stream, response } = core.clientStream<TRes>(service, method, call);
  stream.write(msg);
  stream.end();
  return response;
}

/**
 * Bidi send-one / await-first helper: open the duplex, write exactly one request,
 * and resolve on the FIRST `'data'` event. Rejects on `'error'`, or on `'end'`
 * arriving before any data. Subsequent responses are ignored by the returned
 * promise; the caller owns closing the stream. No auto-retry (matches the
 * "never auto-retried; open a fresh stream" contract on `bidiStream`).
 */
export function sendOneBidiAwaitFirst<TRes = any>(
  core: UdbCore,
  service: string,
  method: string,
  msg: any,
  call?: CallOptions,
): Promise<TRes> {
  const duplex = core.bidiStream(service, method, call);
  return new Promise<TRes>((resolve, reject) => {
    let settled = false;
    const settle = (fn: () => void) => {
      if (settled) return;
      settled = true;
      fn();
    };
    duplex.on("data", (resp: TRes) => settle(() => resolve(resp)));
    duplex.on("error", (err: Error) => settle(() => reject((err as any).udb ?? err)));
    duplex.on("end", () =>
      settle(() => reject(new Error("udb: stream ended before any response"))),
    );
    duplex.write(msg);
  });
}
