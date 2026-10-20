"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.structToObject = structToObject;
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
const protobuf = __importStar(require("protobufjs"));
// Only wire objects produced here bypass normalization. A user's literal
// `fields` key is ordinary JSON data, so its shape cannot identify a wire Struct.
const normalizedStructs = new WeakSet();
function checkDepth(depth) {
    const limit = protobuf.util.recursionLimit;
    if (depth > limit)
        throw new Error("maximum nesting depth exceeded");
}
/** Recursively convert a JS value to a google.protobuf.Value (camelCase oneof). */
function jsToValue(v, depth) {
    checkDepth(depth);
    if (v === null || v === undefined)
        return { nullValue: 0 };
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
    if (typeof v === "object")
        return { structValue: jsToStruct(v, depth + 1) };
    return { nullValue: 0 };
}
/** Convert a plain JS object to the explicit google.protobuf.Struct wire shape. */
function jsToStruct(o, depth) {
    checkDepth(depth);
    const fields = Object.create(null);
    for (const [k, val] of Object.entries(o ?? {}))
        fields[k] = jsToValue(val, depth + 1);
    const wire = { fields };
    normalizedStructs.add(wire);
    return wire;
}
const wrappers = protobuf.wrappers;
// Idempotent: only install once even if several modules import this.
if (!wrappers[".google.protobuf.Struct"]?.__udb) {
    wrappers[".google.protobuf.Struct"] = {
        __udb: true,
        // protobufjs binds the original converter here, but that converter calls
        // wrapped Type.fromObject again for nested Structs. Preserve our normalized
        // objects and genuine message instances instead of wrapping `fields` again.
        fromObject(object, depth = 0) {
            const structMessage = object instanceof protobuf.Message
                && object.$type?.fullName === ".google.protobuf.Struct";
            if (object instanceof this.ctor || normalizedStructs.has(object)) {
                return this.fromObject(object, depth);
            }
            if (structMessage) {
                // A foreign constructor's Value prototypes expose inactive oneof
                // defaults. Passing them to this converter would install false/0/""
                // as real fields. Decode its active members before target conversion.
                return this.fromObject(jsToStruct(structWireToObject(object, depth), depth), depth);
            }
            return this.fromObject(jsToStruct(object, depth), depth);
        },
        toObject(message, options, depth) {
            return this.toObject(message, options, depth);
        },
    };
}
/** Read a google.protobuf.Struct response (explicit wire shape) into plain JS. */
function structToObject(struct) {
    return structWireToObject(struct, 0);
}
function structWireToObject(struct, depth) {
    checkDepth(depth);
    const out = {};
    for (const [k, v] of Object.entries(struct?.fields ?? {})) {
        Object.defineProperty(out, k, {
            value: valueToJs(v, depth + 1), enumerable: true, configurable: true, writable: true,
        });
    }
    return out;
}
function valueToJs(v, depth) {
    checkDepth(depth);
    if (v == null)
        return undefined;
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
            return structWireToObject(v.structValue, depth + 1);
        case "listValue":
            checkDepth(depth + 1);
            return (v.listValue?.values ?? []).map((value) => valueToJs(value, depth + 2));
    }
    // Plain wire responses need not carry the oneof discriminator. Inherited
    // protobuf defaults are never evidence that a member is actually present.
    const owns = (field) => Object.prototype.hasOwnProperty.call(v, field);
    if (owns("stringValue") && v.stringValue !== undefined)
        return v.stringValue;
    if (owns("numberValue") && v.numberValue !== undefined)
        return v.numberValue;
    if (owns("boolValue") && v.boolValue !== undefined)
        return v.boolValue;
    if (owns("structValue") && v.structValue != null)
        return structWireToObject(v.structValue, depth + 1);
    if (owns("listValue") && v.listValue != null) {
        checkDepth(depth + 1);
        return (v.listValue.values ?? []).map((value) => valueToJs(value, depth + 2));
    }
    if (owns("nullValue") && v.nullValue !== undefined)
        return null;
    return undefined;
}
