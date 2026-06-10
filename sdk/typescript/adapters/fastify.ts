// Fastify plugin / hook (M5.6).
//
// Two entry points, both peer-dependency-light (Fastify is never imported):
//
//   - `udbFastifyHook(options)` — an `onRequest` hook `(req, reply, done)` that
//     attaches a request-scoped UDB context to `req.udb` and closes the
//     per-request facade when the request lifecycle ends.
//   - `udbFastifyPlugin(options)` — a Fastify plugin `(fastify, opts, done)` that
//     registers the hook (and an `onResponse` cleanup) for you; register it with
//     `fastify.register(udbFastifyPlugin, { target, tenantId, ... })`.
//
// The signatures are structurally typed so they slot into the real Fastify types
// without this SDK depending on the `fastify` package or its type defs.

import {
  UdbAdapterOptions,
  UdbRequestContext,
  contextFromHeaders,
  contextProperty,
} from "./context";

/** Minimal structural shape of a Fastify request the adapter reads/writes. */
export interface FastifyLikeRequest {
  headers: Record<string, string | string[] | undefined>;
  [key: string]: any;
}

export type FastifyDone = (err?: Error) => void;

/** Fastify `onRequest`/`onResponse` hook signature (structural). */
export type UdbFastifyHook = (
  request: FastifyLikeRequest,
  reply: unknown,
  done: FastifyDone,
) => void;

/** Minimal structural shape of a Fastify instance the plugin uses. */
export interface FastifyLike {
  addHook(name: "onRequest" | "onResponse", hook: (...args: any[]) => void): unknown;
  [key: string]: any;
}

export type UdbFastifyPlugin = (
  fastify: FastifyLike,
  opts: unknown,
  done: FastifyDone,
) => void;

/**
 * Fastify `onRequest` hook that attaches a request-scoped UDB context to
 * `request.udb` (or `request[options.contextProperty]`). Pair it with
 * {@link udbFastifyCleanup} as an `onResponse` hook, or use {@link udbFastifyPlugin}
 * which wires both for you.
 */
export function udbFastifyHook(options: UdbAdapterOptions): UdbFastifyHook {
  const prop = contextProperty(options);
  return function onRequest(request, _reply, done) {
    try {
      const ctx: UdbRequestContext = contextFromHeaders(request.headers, options);
      (request as any)[prop] = ctx;
      done();
    } catch (err) {
      done(err as Error);
    }
  };
}

/** Fastify `onResponse` hook that closes the per-request facade attached by
 *  {@link udbFastifyHook}. */
export function udbFastifyCleanup(options: UdbAdapterOptions): UdbFastifyHook {
  const prop = contextProperty(options);
  return function onResponse(request, _reply, done) {
    const ctx = (request as any)[prop] as UdbRequestContext | undefined;
    if (ctx?.udb) {
      try {
        ctx.udb.close();
      } catch {
        /* best-effort */
      }
    }
    done();
  };
}

/**
 * Fastify plugin that registers the request hook + response cleanup. Register it
 * with the adapter options:
 *
 * ```ts
 * fastify.register(udbFastifyPlugin, { target: "localhost:50051", tenantId: "acme" });
 * fastify.get("/invoices", async (req) => {
 *   await req.udb.authz.require({ message_type: "acme.v1.Invoice" }, "read");
 *   return req.udb.data.select({ message_type: "acme.v1.Invoice", limit: 50 });
 * });
 * ```
 */
export function udbFastifyPlugin(options: UdbAdapterOptions): UdbFastifyPlugin {
  const onRequest = udbFastifyHook(options);
  const onResponse = udbFastifyCleanup(options);
  return function plugin(fastify, _opts, done) {
    fastify.addHook("onRequest", onRequest);
    fastify.addHook("onResponse", onResponse);
    done();
  };
}
