"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.udbExpress = udbExpress;
const context_1 = require("./context");
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
function udbExpress(options) {
    const prop = (0, context_1.contextProperty)(options);
    return function udbMiddleware(req, res, next) {
        let ctx;
        try {
            ctx = (0, context_1.contextFromHeaders)(req.headers, options);
        }
        catch (err) {
            next(err);
            return;
        }
        req[prop] = ctx;
        let closed = false;
        const cleanup = () => {
            if (closed)
                return;
            closed = true;
            try {
                ctx.udb.close();
            }
            catch {
                /* best-effort */
            }
        };
        res.on("finish", cleanup);
        res.on("close", cleanup);
        next();
    };
}
