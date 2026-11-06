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
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.verifyPrincipal = exports.parseDurationMs = exports.configFromEnv = exports.structToObject = exports.defaultProtoRoot = exports.protoSchemas = exports.ProtoSchemas = void 0;
// Public entry point for the UDB TypeScript SDK.
__exportStar(require("./client"), exports);
__exportStar(require("./auth"), exports);
__exportStar(require("./negotiation"), exports);
__exportStar(require("./generatedClient"), exports);
__exportStar(require("./project"), exports);
__exportStar(require("./stores"), exports);
// The udb annotations of the bundled protos, and the message <-> record codec.
var protoSchema_1 = require("./protoSchema");
Object.defineProperty(exports, "ProtoSchemas", { enumerable: true, get: function () { return protoSchema_1.ProtoSchemas; } });
Object.defineProperty(exports, "protoSchemas", { enumerable: true, get: function () { return protoSchema_1.protoSchemas; } });
__exportStar(require("./recordCodec"), exports);
__exportStar(require("./adapters"), exports);
var protoRoot_1 = require("./protoRoot");
Object.defineProperty(exports, "defaultProtoRoot", { enumerable: true, get: function () { return protoRoot_1.defaultProtoRoot; } });
// Importing wkt registers the google.protobuf.Struct serializer (plain JS object
// → Struct on send); `structToObject` is the inverse for reading Struct responses.
var wkt_1 = require("./wkt");
Object.defineProperty(exports, "structToObject", { enumerable: true, get: function () { return wkt_1.structToObject; } });
// Typed snake_case request/response interfaces for the common-RPC subset (§8B).
__exportStar(require("./messages"), exports);
// Typed WriteReceipt / ReadFence read-after-write consistency helpers.
__exportStar(require("./consistency"), exports);
// Stream send-one / await-first helpers for client-streaming and bidi RPCs.
__exportStar(require("./stream"), exports);
// Bound entity / table helper over the DataBroker Upsert/Select/Delete RPCs.
__exportStar(require("./entity"), exports);
// End-to-end encrypted messaging (MLS) over the MessagingService; the WebAssembly
// core is loaded lazily, so importing the SDK never requires it.
__exportStar(require("./messaging"), exports);
// Tenant identity and the per-tenant client pool.
__exportStar(require("./tenantState"), exports);
__exportStar(require("./tenantPool"), exports);
// The environment contract, principal assertions and the broker health check.
var connectEnv_1 = require("./connectEnv");
Object.defineProperty(exports, "configFromEnv", { enumerable: true, get: function () { return connectEnv_1.configFromEnv; } });
Object.defineProperty(exports, "parseDurationMs", { enumerable: true, get: function () { return connectEnv_1.parseDurationMs; } });
Object.defineProperty(exports, "verifyPrincipal", { enumerable: true, get: function () { return connectEnv_1.verifyPrincipal; } });
// Flags, locks, session validation and per-request user calls.
__exportStar(require("./facades"), exports);
// Self-updating views over a filter.
__exportStar(require("./liveQuery"), exports);
// Graph traversal and hybrid vector search builders.
__exportStar(require("./graphVector"), exports);
// Schema reflection from the proto annotations.
__exportStar(require("./schema"), exports);
