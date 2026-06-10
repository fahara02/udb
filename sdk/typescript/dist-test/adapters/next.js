"use strict";
// Next.js helper (M5.6).
//
// Next has two request shapes:
//   - App Router / route handlers: a WHATWG `Request` whose `.headers` is a
//     `Headers` instance (with `.get()` / `.forEach()`).
//   - Pages Router API routes: a Node `NextApiRequest` whose `.headers` is a
//     plain header bag.
//
// `udbContext(reqOrHeaders, options)` accepts either and returns a request-scoped
// `UdbRequestContext`. Because Next has no single middleware lifecycle that owns a
// per-request facade, the caller is responsible for closing it — use
// `withUdb(options, handler)` to get automatic cleanup around an async handler,
// or call `ctx.udb.close()` yourself.
//
// Next is a peer dependency and is never imported here.
Object.defineProperty(exports, "__esModule", { value: true });
exports.toHeaderBag = toHeaderBag;
exports.udbContext = udbContext;
exports.withUdb = withUdb;
const context_1 = require("./context");
function isHeadersLike(value) {
    return value != null && typeof value.get === "function" && typeof value.forEach === "function";
}
/** Normalize any of the accepted shapes into a plain {@link HeaderBag}. */
function toHeaderBag(source) {
    if (source == null)
        return {};
    // A Next request object: unwrap its `.headers`.
    const headers = "headers" in source && source.headers != null
        ? source.headers
        : source;
    if (isHeadersLike(headers)) {
        const bag = {};
        headers.forEach((value, key) => {
            bag[key.toLowerCase()] = value;
        });
        return bag;
    }
    return headers;
}
/**
 * Build a request-scoped UDB context from a Next.js request (App Router `Request`
 * or Pages Router `NextApiRequest`) or a raw header bag / `Headers` object.
 *
 * ```ts
 * // app/api/invoices/route.ts
 * export async function GET(req: Request) {
 *   const ctx = udbContext(req, { target: "localhost:50051", tenantId: "acme" });
 *   try {
 *     await ctx.authz.require({ message_type: "acme.v1.Invoice" }, "read");
 *     return Response.json(await ctx.udb.data.select({ message_type: "acme.v1.Invoice" }));
 *   } finally {
 *     ctx.udb.close();
 *   }
 * }
 * ```
 */
function udbContext(reqOrHeaders, options) {
    return (0, context_1.contextFromHeaders)(toHeaderBag(reqOrHeaders), options);
}
/**
 * Run `handler` with a request-scoped UDB context and close the per-request
 * facade afterwards (success or error). Convenient for route handlers:
 *
 * ```ts
 * export const GET = (req: Request) =>
 *   withUdb({ target: "localhost:50051", tenantId: "acme" }, async (ctx) => {
 *     await ctx.authz.require({ message_type: "acme.v1.Invoice" }, "read");
 *     return Response.json(await ctx.udb.data.select({ message_type: "acme.v1.Invoice" }));
 *   })(req);
 * ```
 */
function withUdb(options, handler) {
    return async (reqOrHeaders) => {
        const ctx = udbContext(reqOrHeaders, options);
        try {
            return await handler(ctx);
        }
        finally {
            try {
                ctx.udb.close();
            }
            catch {
                /* best-effort */
            }
        }
    };
}
