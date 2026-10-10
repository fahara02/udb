// Well-known-type (google.protobuf.Struct) serialization fix.
//
// protobufjs (and @grpc/proto-loader on top of it) ships a fromObject wrapper
// ONLY for google.protobuf.Any — there is none for Struct/Value/ListValue. As a
// result a plain JS object passed to a `google.protobuf.Struct` field (a Select
// `filter`, a Mongo `document`, a vector point `payload`, …) silently serializes
// to EMPTY values: protobufjs's Value oneof members are CAMELCASE
// (`stringValue`/`numberValue`/`boolValue`), but a naive caller (and the loader's
// keepCase:true) produces snake_case keys that protobufjs ignores — so the field
// is sent blank and, e.g., a filter matches nothing.
//
// Registering a Struct wrapper here makes proto-loader accept a PLAIN JS object
// for any Struct field and encode it correctly (recursively, including nested
// objects/arrays). Importing this module (for its side effect) MUST happen before
// the first `protoLoader.loadSync(...)`; every SDK module that loads protos does
// so via a top-of-file `import "./wkt"`.
import * as protobuf from "protobufjs";

// Only wire objects produced here bypass normalization. A user's literal
// `fields` key is ordinary JSON data, so its shape cannot identify a wire Struct.
const normalizedStructs = new WeakSet<object>();

function checkDepth(depth: number): void {
  const limit = (protobuf.util as unknown as { recursionLimit: number }).recursionLimit;
  if (depth > limit) throw new Error("maximum nesting depth exceeded");
}

/** Recursively convert a JS value to a google.protobuf.Value (camelCase oneof). */
function jsToValue(v: unknown, depth: number): Record<string, unknown> {
  checkDepth(depth);
  if (v === null || v === undefined) return { nullValue: 0 };
  switch (typeof v) {
    case "string":
      return { stringValue: v };
    case "number":
      return { numberValue: v };
    case "boolean":
      return { boolValue: v };
  }
  if (Array.isArray(v)) {
    checkDepth(depth + 1); // ListValue adds one message before each nested Value.
    return { listValue: { values: v.map((value) => jsToValue(value, depth + 2)) } };
  }
  if (typeof v === "object") return { structValue: jsToStruct(v as Record<string, unknown>, depth + 1) };
  return { nullValue: 0 };
}

/** Convert a plain JS object to the explicit google.protobuf.Struct wire shape. */
function jsToStruct(o: Record<string, unknown>, depth: number): { fields: Record<string, unknown> } {
  checkDepth(depth);
  const fields: Record<string, unknown> = Object.create(null);
  for (const [k, val] of Object.entries(o ?? {})) fields[k] = jsToValue(val, depth + 1);
  const wire = { fields };
  normalizedStructs.add(wire);
  return wire;
}

const wrappers = (protobuf as unknown as { wrappers: Record<string, any> }).wrappers;

// Idempotent: only install once even if several modules import this.
if (!wrappers[".google.protobuf.Struct"]?.__udb) {
  wrappers[".google.protobuf.Struct"] = {
    __udb: true,
    // protobufjs binds the original converter here, but that converter calls
    // wrapped Type.fromObject again for nested Structs. Preserve our normalized
    // objects and genuine message instances instead of wrapping `fields` again.
    fromObject(this: any, object: any, depth = 0) {
      const structMessage = object instanceof protobuf.Message
        && object.$type?.fullName === ".google.protobuf.Struct";
      if (object instanceof this.ctor || structMessage || normalizedStructs.has(object)) {
        return this.fromObject(object, depth);
      }
      return this.fromObject(jsToStruct(object, depth), depth);
    },
    toObject(this: any, message: any, options: any, depth?: number) {
      return this.toObject(message, options, depth);
    },
  };
}

/** Read a google.protobuf.Struct response (explicit wire shape) into plain JS. */
export function structToObject(struct: any): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(struct?.fields ?? {})) out[k] = valueToJs(v);
  return out;
}

function valueToJs(v: any): unknown {
  if (v == null) return undefined;
  switch (v.kind) {
    case "nullValue":
      return null;
    case "numberValue":
      return v.numberValue;
    case "stringValue":
      return v.stringValue;
    case "boolValue":
      return v.boolValue;
    case "structValue":
      return structToObject(v.structValue);
    case "listValue":
      return (v.listValue?.values ?? []).map(valueToJs);
  }
  if (v.stringValue !== undefined) return v.stringValue;
  if (v.numberValue !== undefined) return v.numberValue;
  if (v.boolValue !== undefined) return v.boolValue;
  if (v.structValue !== undefined) return structToObject(v.structValue);
  if (v.listValue !== undefined) return (v.listValue.values ?? []).map(valueToJs);
  if (v.nullValue !== undefined) return null;
  return undefined;
}
