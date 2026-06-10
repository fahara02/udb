// Framework adapters (M5.6): thin, peer-dependency-light request-context
// integrations for Express, Fastify, and Next.js. Each extracts the per-request
// UDB identity from headers (falling back to the adapter config) and attaches a
// request-scoped `UdbProject` + auth/authz context.
export * from "./context";
export * from "./express";
export * from "./fastify";
export * from "./next";
