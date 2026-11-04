// Test support for code that uses the SDK: an in-memory broker, the real broker with
// embedded PostgreSQL, and the typed-store contract both must meet. Import it from
// `@udb_plus/sdk/udbtest`; the SDK's main entry never loads it.

export { Fake, createFake, createDescriptorFake } from "./fake";
export { loadPolicies, parsePolicies } from "./policy";
export type { Fault, FakeEvent, Policy, TestHooks } from "./fake";
export type { Identity } from "./entityRules";
export { start, startPair } from "./embedded";
export { runTable, runTableCases, tableCasesPath } from "./conformance";
export type { Connect, RunTableOptions } from "./conformance";
