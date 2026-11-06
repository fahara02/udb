"use strict";
// Test support for code that uses the SDK: an in-memory broker, the real broker with
// embedded PostgreSQL, and the typed-store contract both must meet. Import it from
// `@udb_plus/sdk/udbtest`; the SDK's main entry never loads it.
Object.defineProperty(exports, "__esModule", { value: true });
exports.tableCasesPath = exports.runTableCases = exports.runTable = exports.startPair = exports.start = exports.parsePolicies = exports.loadPolicies = exports.createDescriptorFake = exports.createFake = exports.Fake = void 0;
var fake_1 = require("./fake");
Object.defineProperty(exports, "Fake", { enumerable: true, get: function () { return fake_1.Fake; } });
Object.defineProperty(exports, "createFake", { enumerable: true, get: function () { return fake_1.createFake; } });
Object.defineProperty(exports, "createDescriptorFake", { enumerable: true, get: function () { return fake_1.createDescriptorFake; } });
var policy_1 = require("./policy");
Object.defineProperty(exports, "loadPolicies", { enumerable: true, get: function () { return policy_1.loadPolicies; } });
Object.defineProperty(exports, "parsePolicies", { enumerable: true, get: function () { return policy_1.parsePolicies; } });
var embedded_1 = require("./embedded");
Object.defineProperty(exports, "start", { enumerable: true, get: function () { return embedded_1.start; } });
Object.defineProperty(exports, "startPair", { enumerable: true, get: function () { return embedded_1.startPair; } });
var conformance_1 = require("./conformance");
Object.defineProperty(exports, "runTable", { enumerable: true, get: function () { return conformance_1.runTable; } });
Object.defineProperty(exports, "runTableCases", { enumerable: true, get: function () { return conformance_1.runTableCases; } });
Object.defineProperty(exports, "tableCasesPath", { enumerable: true, get: function () { return conformance_1.tableCasesPath; } });
