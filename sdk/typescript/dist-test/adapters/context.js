"use strict";
// Shared request-context extraction for the framework adapters (M5.6).
//
// All three adapters (Express / Fastify / Next.js) extract the same per-request
// UDB identity (tenant / user / purpose / scopes / correlation / request id /
// project) from inbound headers, falling back to a static app config, then build
// a per-request `UdbProject` (and its `auth` / `authz` sub-clients) bound to that
// identity. The adapters are intentionally thin and depend only on this SDK plus
// the host framework's own request/response types (which are peer-required so the
// SDK never hard-depends on express/fastify/next).
Object.defineProperty(exports, "__esModule", { value: true });
exports.UDB_HEADERS = void 0;
exports.metadataFromHeaders = metadataFromHeaders;
exports.contextFromHeaders = contextFromHeaders;
exports.contextProperty = contextProperty;
const project_1 = require("../project");
/** Canonical UDB request headers (lowercase; HTTP headers are case-insensitive). */
exports.UDB_HEADERS = {
    apiKey: "x-api-key",
    tenantId: "x-tenant-id",
    userId: "x-user-id",
    purpose: "x-purpose",
    scopes: "x-scopes",
    correlationId: "x-correlation-id",
    requestId: "x-request-id",
    projectId: "x-udb-project-id",
};
function firstHeader(bag, name) {
    if (!bag)
        return undefined;
    const raw = bag[name] ?? bag[name.toLowerCase()];
    if (raw == null)
        return undefined;
    return Array.isArray(raw) ? raw[0] : raw;
}
function parseScopes(value) {
    if (value == null)
        return undefined;
    const scopes = value
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
    return scopes.length ? scopes : undefined;
}
/** Build a per-request `UdbMetadata` by overlaying trusted inbound headers on the
 *  adapter's static config. A missing header leaves the configured default in
 *  place; correlation id is generated when neither header nor config supplies one. */
function metadataFromHeaders(headers, options) {
    const trust = options.trustHeaders !== false;
    const pick = (name) => trust ? firstHeader(headers, name) : undefined;
    const correlationId = pick(exports.UDB_HEADERS.correlationId) ??
        pick(exports.UDB_HEADERS.requestId) ??
        options.correlationId ??
        `udb-${Date.now().toString(36)}`;
    return {
        tenantId: pick(exports.UDB_HEADERS.tenantId) ?? options.tenantId,
        purpose: pick(exports.UDB_HEADERS.purpose) ?? options.purpose ?? "",
        correlationId,
        scopes: parseScopes(pick(exports.UDB_HEADERS.scopes)) ?? options.scopes,
        userId: pick(exports.UDB_HEADERS.userId) ?? options.userId,
        projectId: pick(exports.UDB_HEADERS.projectId) ?? options.projectId,
        serviceIdentity: options.serviceIdentity,
    };
}
/** Build the per-request `UdbProject` + context from inbound headers. Per-request
 *  credentials (a forwarded `x-api-key`) override the configured api key. */
function contextFromHeaders(headers, options) {
    const meta = metadataFromHeaders(headers, options);
    const trust = options.trustHeaders !== false;
    const forwardedApiKey = trust ? firstHeader(headers, exports.UDB_HEADERS.apiKey) : undefined;
    const config = {
        ...options,
        tenantId: meta.tenantId,
        purpose: meta.purpose,
        correlationId: meta.correlationId,
        scopes: meta.scopes,
        userId: meta.userId,
        projectId: meta.projectId,
        serviceIdentity: meta.serviceIdentity,
    };
    if (forwardedApiKey) {
        config.credentials = { ...options.credentials, apiKey: forwardedApiKey };
    }
    const udb = (0, project_1.createUdb)(config);
    return { udb, meta, auth: udb.auth, authz: udb.authz };
}
/** The request property the adapters attach the context under (default `"udb"`). */
function contextProperty(options) {
    return options.contextProperty ?? "udb";
}
