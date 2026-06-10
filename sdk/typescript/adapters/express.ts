// Express middleware (M5.6).
//
// `udbExpress(options)` returns a standard Express middleware `(req, res, next)`
// that extracts the per-request UDB identity from headers (falling back to the
// adapter config), builds a request-scoped `UdbProject`, and attaches it (plus
// `auth`/`authz`/`meta`) to the request under `req.udb` (configurable). The
// per-request facade's channels are closed when the response finishes so each
// request gets an isolated, identity-bound client.
//
// Express itself is a peer dependency: this module never imports it. The handler
// signature is structurally typed so `app.use(udbExpress(...))` type-checks
// against the real express types without this SDK depending on @types/express.

import {
  UdbAdapterOptions,
  UdbRequestContext,
  contextFromHeaders,
  contextProperty,
} from "./context";

/** Minimal structural shape of an Express request the adapter reads/writes. */
export interface ExpressLikeRequest {
  headers: Record<string, string | string[] | undefined>;
  [key: string]: any;
}

/** Minimal structural shape of an Express response the adapter hooks `finish` on. */
export interface ExpressLikeResponse {
  on(event: "finish" | "close", listener: () => void): unknown;
  [key: string]: any;
}

export type ExpressNext = (err?: any) => void;

export type UdbExpressMiddleware = (
  req: ExpressLikeRequest,
  res: ExpressLikeResponse,
  next: ExpressNext,
) => void;

/**
 * Express middleware that attaches a request-scoped UDB context to `req.udb`
 * (or `req[options.contextProperty]`). The per-request facade is closed when the
 * response finishes so channels are not leaked.
 *
 * ```ts
 * app.use(udbExpress({ target: "localhost:50051", tenantId: "acme", purpose: "web" }));
 * app.get("/invoices", async (req, res) => {
 *   await req.udb.authz.require({ message_type: "acme.v1.Invoice" }, "read");
 *   res.json(await req.udb.data.select({ message_type: "acme.v1.Invoice", limit: 50 }));
 * });
 * ```
 */
export function udbExpress(options: UdbAdapterOptions): UdbExpressMiddleware {
  const prop = contextProperty(options);
  return function udbMiddleware(req, res, next) {
    let ctx: UdbRequestContext;
    try {
      ctx = contextFromHeaders(req.headers, options);
    } catch (err) {
      next(err);
      return;
    }
    (req as any)[prop] = ctx;

    let closed = false;
    const cleanup = () => {
      if (closed) return;
      closed = true;
      try {
        ctx.udb.close();
      } catch {
        /* best-effort */
      }
    };
    res.on("finish", cleanup);
    res.on("close", cleanup);

    next();
  };
}
