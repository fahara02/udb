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
/** Recursively convert a JS value to a google.protobuf.Value (camelCase oneof). */
function jsToValue(v) {
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
    if (Array.isArray(v))
        return { listValue: { values: v.map(jsToValue) } };
    if (typeof v === "object")
        return { structValue: jsToStruct(v) };
    return { nullValue: 0 };
}
/** Convert a plain JS object to the explicit google.protobuf.Struct wire shape. */
function jsToStruct(o) {
    const fields = {};
    for (const [k, val] of Object.entries(o ?? {}))
        fields[k] = jsToValue(val);
    return { fields };
}
const wrappers = protobuf.wrappers;
// Idempotent: only install once even if several modules import this.
if (!wrappers[".google.protobuf.Struct"]?.__udb) {
    wrappers[".google.protobuf.Struct"] = {
        __udb: true,
        // `this.fromObject` is the ORIGINAL (protobufjs binds it), so we normalize a
        // plain JS object into the explicit {fields:{k:{<value>}}} form and hand it
        // back to the real decoder — no recursion.
        fromObject(object) {
            return this.fromObject(jsToStruct(object));
        },
        toObject(message, options) {
            return this.toObject(message, options);
        },
    };
}
/** Read a google.protobuf.Struct response (explicit wire shape) into plain JS. */
function structToObject(struct) {
    const out = {};
    for (const [k, v] of Object.entries(struct?.fields ?? {}))
        out[k] = valueToJs(v);
    return out;
}
function valueToJs(v) {
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
            return structToObject(v.structValue);
        case "listValue":
            return (v.listValue?.values ?? []).map(valueToJs);
    }
    if (v.stringValue !== undefined)
        return v.stringValue;
    if (v.numberValue !== undefined)
        return v.numberValue;
    if (v.boolValue !== undefined)
        return v.boolValue;
    if (v.structValue !== undefined)
        return structToObject(v.structValue);
    if (v.listValue !== undefined)
        return (v.listValue.values ?? []).map(valueToJs);
    if (v.nullValue !== undefined)
        return null;
    return undefined;
}
