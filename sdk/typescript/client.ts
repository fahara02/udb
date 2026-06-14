import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";

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
  headers.set("x-tenant-id", meta.tenantId);
  headers.set("x-user-id", meta.userId ?? "");
  headers.set("x-purpose", meta.purpose);
  headers.set("x-correlation-id", meta.correlationId);
  headers.set("x-scopes", (meta.scopes ?? []).join(","));
  headers.set("x-service-identity", meta.serviceIdentity ?? "example.service");
  headers.set("x-udb-project-id", meta.projectId ?? "default");
  headers.set("x-udb-client-catalog-version", meta.clientCatalogVersion ?? UDB_PROTOCOL_VERSION);
  if (meta.bearerToken) headers.set("authorization", `Bearer ${meta.bearerToken}`);
  if (meta.apiKey) headers.set("x-api-key", meta.apiKey);
  return headers;
}

// Default channel options for the long-lived UDB channel: keepalive (keep an idle
// HTTP/2 connection warm instead of dropping to IDLE and re-handshaking) plus a
// native gRPC service-config retry on UNAVAILABLE (jittered exponential backoff,
// bounded by the call deadline). Construct the client once and reuse it.
export const UDB_DEFAULT_CHANNEL_OPTIONS: grpc.ChannelOptions = {
  "grpc.keepalive_time_ms": 30_000,
  "grpc.keepalive_timeout_ms": 10_000,
  "grpc.keepalive_permit_without_calls": 1,
  "grpc.http2.max_pings_without_data": 0,
  "grpc.enable_retries": 1,
  "grpc.service_config": JSON.stringify({
    methodConfig: [
      {
        name: [{}],
        retryPolicy: {
          maxAttempts: 4,
          initialBackoff: "0.1s",
          maxBackoff: "2s",
          backoffMultiplier: 2.0,
          retryableStatusCodes: ["UNAVAILABLE"],
        },
      },
    ],
  }),
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
