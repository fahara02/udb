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
exports.structToObject = exports.defaultProtoRoot = void 0;
// Public entry point for the UDB TypeScript SDK.
__exportStar(require("./client"), exports);
__exportStar(require("./auth"), exports);
__exportStar(require("./negotiation"), exports);
__exportStar(require("./generatedClient"), exports);
__exportStar(require("./project"), exports);
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
