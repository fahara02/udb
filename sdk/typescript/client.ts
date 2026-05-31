import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";

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
  return headers;
}

export function dataBrokerClient(target: string, protoRoot = path.resolve(__dirname, "../../proto")): any {
  const protoPath = path.join(protoRoot, "udb/services/v1/data_broker.proto");
  const definition = protoLoader.loadSync(protoPath, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const loaded = grpc.loadPackageDefinition(definition) as any;
  return new loaded.udb.services.v1.DataBroker(target, grpc.credentials.createInsecure());
}
