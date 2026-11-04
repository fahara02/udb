// Public entry point for the UDB TypeScript SDK.
export * from "./client";
export * from "./auth";
export * from "./negotiation";
export * from "./generatedClient";
export * from "./project";
export * from "./stores";
// The udb annotations of the bundled protos, and the message <-> record codec.
export { ProtoSchemas, protoSchemas } from "./protoSchema";
export * from "./recordCodec";
export * from "./adapters";
export { defaultProtoRoot } from "./protoRoot";
// Importing wkt registers the google.protobuf.Struct serializer (plain JS object
// → Struct on send); `structToObject` is the inverse for reading Struct responses.
export { structToObject } from "./wkt";
// Typed snake_case request/response interfaces for the common-RPC subset (§8B).
export * from "./messages";
// Typed WriteReceipt / ReadFence read-after-write consistency helpers.
export * from "./consistency";
// Stream send-one / await-first helpers for client-streaming and bidi RPCs.
export * from "./stream";
// Bound entity / table helper over the DataBroker Upsert/Select/Delete RPCs.
export * from "./entity";
// End-to-end encrypted messaging (MLS) over the MessagingService; the WebAssembly
// core is loaded lazily, so importing the SDK never requires it.
export * from "./messaging";
// Tenant identity and the per-tenant client pool.
export * from "./tenantState";
export * from "./tenantPool";
// The environment contract, principal assertions and the broker health check.
export { configFromEnv, parseDurationMs, verifyPrincipal, UdbExpect } from "./connectEnv";
// Flags, locks, session validation and per-request user calls.
export * from "./facades";
// Self-updating views over a filter.
export * from "./liveQuery";
// Graph traversal and hybrid vector search builders.
export * from "./graphVector";
// Schema reflection from the proto annotations.
export * from "./schema";
