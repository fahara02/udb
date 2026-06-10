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
// Framework adapters (M5.6): thin, peer-dependency-light request-context
// integrations for Express, Fastify, and Next.js. Each extracts the per-request
// UDB identity from headers (falling back to the adapter config) and attaches a
// request-scoped `UdbProject` + auth/authz context.
__exportStar(require("./context"), exports);
__exportStar(require("./express"), exports);
__exportStar(require("./fastify"), exports);
__exportStar(require("./next"), exports);
