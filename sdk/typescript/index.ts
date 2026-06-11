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
