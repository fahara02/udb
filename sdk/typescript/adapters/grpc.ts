// gRPC server adapter. It lifts the canonical UDB request context (tenant, user,
// correlation, project, purpose, scopes) off an inbound call and makes it available
// to the handler, so downstream code and any UDB client built from it propagate the
// same identity without re-parsing headers. The header names are the ones the SDK
// sends, so a call that came from a UDB client round-trips its context faithfully.
//
//   const server = new grpc.Server({ interceptors: [udbServerInterceptor] });
//   // in a handler, unary or streaming:
//   const tenant = requestTenantId();
//
// Node has no request context object, so the value lives in AsyncLocalStorage and
// follows the handler across awaits.

import { AsyncLocalStorage } from "node:async_hooks";
import * as grpc from "@grpc/grpc-js";

import { UdbMetadata } from "../client";

const REQUEST_METADATA = new AsyncLocalStorage<UdbMetadata>();

function emptyMetadata(): UdbMetadata {
  return { tenantId: "", purpose: "", correlationId: "" };
}

function splitScopes(raw: string): string[] | undefined {
  const scopes = raw.split(",").filter(Boolean);
  return scopes.length ? scopes : undefined;
}

/** The canonical UDB identity on inbound gRPC metadata. The request id stands in
 *  for a missing correlation id. */
export function metadataFromGrpc(md: grpc.Metadata): UdbMetadata {
  const first = (key: string): string => {
    const values = md.get(key);
    return values.length ? String(values[0]) : "";
  };
  return {
    tenantId: first("x-tenant-id"),
    userId: first("x-user-id"),
    purpose: first("x-purpose"),
    correlationId: first("x-correlation-id") || first("x-request-id"),
    scopes: splitScopes(first("x-scopes")),
    serviceIdentity: first("x-service-identity"),
    projectId: first("x-udb-project-id"),
    clientCatalogVersion: first("x-udb-client-catalog-version"),
  };
}

/** Server interceptor for `new grpc.Server({ interceptors: [...] })`, covering unary
 *  and all streaming handlers. Handlers read the identity with `requestMetadata()`. */
export const udbServerInterceptor: grpc.ServerInterceptor = (_method, call) => {
  let meta = emptyMetadata();
  return new grpc.ServerInterceptingCall(call, {
    start(next) {
      const listener = new grpc.ServerListenerBuilder()
        .withOnReceiveMetadata((md, proceed) => {
          meta = metadataFromGrpc(md);
          REQUEST_METADATA.run(meta, () => proceed(md));
        })
        .withOnReceiveMessage((message, proceed) => {
          REQUEST_METADATA.run(meta, () => proceed(message));
        })
        .withOnReceiveHalfClose((proceed) => {
          REQUEST_METADATA.run(meta, () => proceed());
        })
        .build();
      next(listener);
    },
  });
};

/** Run `fn` with `meta` as the request identity. */
export function withRequestMetadata<R>(meta: UdbMetadata, fn: () => R): R {
  return REQUEST_METADATA.run(meta, fn);
}

/** The request identity, or undefined outside a request. */
export function requestMetadata(): UdbMetadata | undefined {
  return REQUEST_METADATA.getStore();
}

/** The request's correlation id, or "" when unset. */
export function requestCorrelationId(): string {
  return requestMetadata()?.correlationId ?? "";
}

/** The request's tenant id, or "" when unset. */
export function requestTenantId(): string {
  return requestMetadata()?.tenantId ?? "";
}

/** The request's user id, or "" when unset. */
export function requestUserId(): string {
  return requestMetadata()?.userId ?? "";
}
