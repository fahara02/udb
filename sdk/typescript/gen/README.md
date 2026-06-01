# `sdk/typescript/gen/` — generated parity stubs (not used at runtime)

This directory holds the [buf](https://buf.build)-generated TypeScript message
stubs (`*_pb.ts`) for the UDB protos, emitted by `buf generate --include-imports`
and kept in lockstep with `proto/**` by the CI drift check.

**The TypeScript SDK does not import these stubs.** The `@udb_plus/sdk` package
uses a **dynamic proto-loader** design instead:

- the `.proto` files are bundled into the published package (`scripts/bundle-proto.mjs`),
- `protoRoot.ts` resolves that bundled proto tree at runtime, and
- `@grpc/proto-loader` + `@grpc/grpc-js` load the service/message definitions on
  the fly — no compiled message classes are required.

Consequently this `gen/` tree:

- is **excluded** from the package `files` allow-list (it is never published),
- is **excluded** from `tsconfig.build.json` `include` (it is never compiled), and
- depends on `@bufbuild/protobuf`, which is deliberately **not** a dependency of
  the SDK (nothing here is on the runtime path).

It is retained purely so the TypeScript surface stays drift-checked against the
canonical protos alongside every other language. If you are consuming the SDK,
import from the package entry points (`@udb_plus/sdk`, `@udb_plus/sdk/client`,
`@udb_plus/sdk/auth`) — not from this folder.
