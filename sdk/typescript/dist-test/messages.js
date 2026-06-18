"use strict";
// Hand-written snake_case request/response interfaces for the common-RPC subset
// the facades use (§8B). These match the WIRE JSON the `@grpc/proto-loader`
// runtime sends with `keepCase: true` (plain snake_case objects) — NOT the
// camelCase `@bufbuild/protobuf` `gen/**` Message classes (which are excluded
// from the build and must not be imported at runtime).
//
// Every interface carries `[k: string]: unknown` so the dynamic escape hatch
// (passing extra/raw fields) is preserved; a MISSPELLED known field is still a
// tsc error when a caller annotates a request with one of these types.
//
// This is a DELIBERATE subset (Select/Upsert/Delete + the storage/auth/migration
// helpers the facades use). The full descriptor-driven per-message interface
// generator is deferred (D10).
Object.defineProperty(exports, "__esModule", { value: true });
