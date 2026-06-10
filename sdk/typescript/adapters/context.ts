// Shared request-context extraction for the framework adapters (M5.6).
//
// All three adapters (Express / Fastify / Next.js) extract the same per-request
// UDB identity (tenant / user / purpose / scopes / correlation / request id /
// project) from inbound headers, falling back to a static app config, then build
// a per-request `UdbProject` (and its `auth` / `authz` sub-clients) bound to that
// identity. The adapters are intentionally thin and depend only on this SDK plus
// the host framework's own request/response types (which are peer-required so the
// SDK never hard-depends on express/fastify/next).

import { UdbMetadata } from "../client";
import { UdbProject, UdbProjectConfig, createUdb } from "../project";

/** Canonical UDB request headers (lowercase; HTTP headers are case-insensitive). */
export const UDB_HEADERS = {
  apiKey: "x-api-key",
  tenantId: "x-tenant-id",
  userId: "x-user-id",
  purpose: "x-purpose",
  scopes: "x-scopes",
  correlationId: "x-correlation-id",
  requestId: "x-request-id",
  projectId: "x-udb-project-id",
} as const;

/** A header bag as exposed by Node/Express/Fastify (`req.headers`): each value is
 *  a string, an array of strings (repeated header), or undefined. */
export type HeaderBag = Record<string, string | string[] | undefined>;

/** Static per-app defaults applied when a request does not carry the header. The
 *  `target`/`tenantId` form the base `UdbProjectConfig`; per-request headers
 *  override the identity fields below. */
export interface UdbAdapterOptions extends UdbProjectConfig {
  /** Trust inbound identity headers (`x-tenant-id`/`x-user-id`/`x-scopes`/…).
   *  Default `true`. Set `false` at the edge to pin every request to the static
   *  config identity regardless of what the client sends. */
  trustHeaders?: boolean;
  /** The property name attached to the request object. Default `"udb"`. */
  contextProperty?: string;
}

/** The request-scoped context the adapters attach to the framework request. */
export interface UdbRequestContext {
  /** Per-request facade bound to this request's identity. */
  udb: UdbProject;
  /** The per-request metadata that the facade's sub-clients send. */
  meta: UdbMetadata;
  /** Convenience handle to the auth ergonomics (authenticate*, can/require/…). */
  auth: UdbProject["auth"];
  /** Convenience handle to the authz ergonomics (can/require/explain/…). */
  authz: UdbProject["authz"];
}

function firstHeader(bag: HeaderBag | undefined, name: string): string | undefined {
  if (!bag) return undefined;
  const raw = bag[name] ?? bag[name.toLowerCase()];
  if (raw == null) return undefined;
  return Array.isArray(raw) ? raw[0] : raw;
}

function parseScopes(value: string | undefined): string[] | undefined {
  if (value == null) return undefined;
  const scopes = value
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  return scopes.length ? scopes : undefined;
}

/** Build a per-request `UdbMetadata` by overlaying trusted inbound headers on the
 *  adapter's static config. A missing header leaves the configured default in
 *  place; correlation id is generated when neither header nor config supplies one. */
export function metadataFromHeaders(
  headers: HeaderBag | undefined,
  options: UdbAdapterOptions,
): UdbMetadata {
  const trust = options.trustHeaders !== false;
  const pick = (name: string): string | undefined =>
    trust ? firstHeader(headers, name) : undefined;

  const correlationId =
    pick(UDB_HEADERS.correlationId) ??
    pick(UDB_HEADERS.requestId) ??
    options.correlationId ??
    `udb-${Date.now().toString(36)}`;

  return {
    tenantId: pick(UDB_HEADERS.tenantId) ?? options.tenantId,
    purpose: pick(UDB_HEADERS.purpose) ?? options.purpose ?? "",
    correlationId,
    scopes: parseScopes(pick(UDB_HEADERS.scopes)) ?? options.scopes,
    userId: pick(UDB_HEADERS.userId) ?? options.userId,
    projectId: pick(UDB_HEADERS.projectId) ?? options.projectId,
    serviceIdentity: options.serviceIdentity,
  };
}

/** Build the per-request `UdbProject` + context from inbound headers. Per-request
 *  credentials (a forwarded `x-api-key`) override the configured api key. */
export function contextFromHeaders(
  headers: HeaderBag | undefined,
  options: UdbAdapterOptions,
): UdbRequestContext {
  const meta = metadataFromHeaders(headers, options);
  const trust = options.trustHeaders !== false;
  const forwardedApiKey = trust ? firstHeader(headers, UDB_HEADERS.apiKey) : undefined;

  const config: UdbProjectConfig = {
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

  const udb = createUdb(config);
  return { udb, meta, auth: udb.auth, authz: udb.authz };
}

/** The request property the adapters attach the context under (default `"udb"`). */
export function contextProperty(options: UdbAdapterOptions): string {
  return options.contextProperty ?? "udb";
}
