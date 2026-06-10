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
exports.defaultProtoRoot = void 0;
// Public entry point for the UDB TypeScript SDK.
__exportStar(require("./client"), exports);
__exportStar(require("./auth"), exports);
__exportStar(require("./negotiation"), exports);
__exportStar(require("./generatedClient"), exports);
__exportStar(require("./project"), exports);
__exportStar(require("./adapters"), exports);
var protoRoot_1 = require("./protoRoot");
Object.defineProperty(exports, "defaultProtoRoot", { enumerable: true, get: function () { return protoRoot_1.defaultProtoRoot; } });
