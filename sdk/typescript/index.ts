// Public entry point for the UDB TypeScript SDK.
export * from "./client";
export * from "./auth";
export * from "./negotiation";
export * from "./generatedClient";
export * from "./project";
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
