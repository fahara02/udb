import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { randomUUID } from "node:crypto";

import "./wkt"; // registers the google.protobuf.Struct serializer (must precede any loadSync)
import { defaultProtoRoot } from "./protoRoot";

export const UDB_PROTOCOL_VERSION = "1.0.0";

export interface UdbMetadata {
  tenantId: string;
  purpose: string;
  correlationId: string;
  scopes?: string[];
  serviceIdentity?: string;
  userId?: string;
  projectId?: string;
  clientCatalogVersion?: string;
  bearerToken?: string;
  apiKey?: string;
}

export function metadata(meta: UdbMetadata): grpc.Metadata {
  const headers = new grpc.Metadata();
  // §13: native RPCs require a request context (x-request-id / x-correlation-id /
  // traceparent) and fail closed without one. Always send a request id, and fall
  // back to it for the correlation id when the caller set none.
  const requestId = randomUUID();
  const correlationId = meta.correlationId || requestId;
  headers.set("x-tenant-id", meta.tenantId);
  headers.set("x-user-id", meta.userId ?? "");
  headers.set("x-purpose", meta.purpose);
  headers.set("x-correlation-id", correlationId);
  headers.set("x-request-id", requestId);
  headers.set("x-scopes", (meta.scopes ?? []).join(","));
  headers.set("x-service-identity", meta.serviceIdentity ?? "example.service");
  headers.set("x-udb-project-id", meta.projectId ?? "default");
  headers.set("x-udb-client-catalog-version", meta.clientCatalogVersion ?? UDB_PROTOCOL_VERSION);
  if (meta.bearerToken) headers.set("authorization", `Bearer ${meta.bearerToken}`);
  if (meta.apiKey) headers.set("x-api-key", meta.apiKey);
  return headers;
}

// Default channel options for the long-lived UDB channel: keepalive keeps an idle
// HTTP/2 connection warm instead of dropping to IDLE and re-handshaking. Retries
// are handled by the generated wrapper where proto-derived operation_kind is known.
export const UDB_DEFAULT_CHANNEL_OPTIONS: grpc.ChannelOptions = {
  "grpc.keepalive_time_ms": 30_000,
  "grpc.keepalive_timeout_ms": 10_000,
  "grpc.keepalive_permit_without_calls": 1,
  "grpc.http2.max_pings_without_data": 0,
};

export function dataBrokerClient(target: string, protoRoot = defaultProtoRoot()): any {
  const protoPath = path.join(protoRoot, "udb/services/v1/data_broker.proto");
  const definition = protoLoader.loadSync(protoPath, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const loaded = grpc.loadPackageDefinition(definition) as any;
  return new loaded.udb.services.v1.DataBroker(
    target,
    grpc.credentials.createInsecure(),
    UDB_DEFAULT_CHANNEL_OPTIONS,
  );
}
