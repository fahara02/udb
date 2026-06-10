# UDB TypeScript SDK templates

These templates are consumed by the Rust-driven generator
(`udb sdk generate --lang typescript`, implemented in `src/cli/sdk_gen.rs`). The
generator reads the embedded proto `FileDescriptorSet` (proto is the single
source of truth — no RPC is hand-listed here) and materializes each file below
at the mirror path under `sdk/typescript/`.

## What renders where

| Template (`sdk-templates/typescript/`) | Renders to (`sdk/typescript/`) | Mode |
| --- | --- | --- |
| `generatedClient.ts.tmpl` | `generatedClient.ts` | rendered (`.tmpl` stripped) |
| `bin/udb.js.tmpl` | `bin/udb.js` | rendered (`.tmpl` stripped) |
| `README.md` | `README.md` | (this file is template-author docs; **not** rendered — see note) |

> Note: every non-`.tmpl`, non-dotfile, non-`sdkgen.*` file is *copied verbatim*
> by the generator, so a `README.md` placed here would overwrite the hand-written
> package README. To avoid clobbering it, this file documents the templates only;
> it should be kept here as author notes. If you want the generator to emit a
> README into the package, rename it (e.g. `GENERATED.md.tmpl`). The package's
> own `sdk/typescript/README.md` is owned by the SDK maintainers.

## How it composes with the existing SDK (never clobbers)

The hand-written layer already in `sdk/typescript/` is left untouched:

- `client.ts` — `UdbMetadata`, `metadata()`, `UDB_PROTOCOL_VERSION`, `dataBrokerClient()`
- `auth.ts` — `UdbAuthClient`, `AuthzCache`, `withNativeTx()`
- `negotiation.ts` — `Negotiator`, encoding constants
- `protoRoot.ts` — `defaultProtoRoot()`

`generatedClient.ts` is a **new** file that *imports* `UdbMetadata`/`metadata`/
`UDB_PROTOCOL_VERSION` from `./client` and `defaultProtoRoot` from `./protoRoot`,
then layers retry/deadline/typed-errors on top. It uses the same dynamic
`@grpc/proto-loader` + `@grpc/grpc-js` loading mechanism as `client.ts` and
`auth.ts` (it does **not** depend on the buf-generated `gen/*_pb.ts` message
classes). The aggregate `UdbGeneratedClient` exposes one robust sub-client per
service (`DataBroker`, `AuthnService`, `AuthzService`, …); method names are the
RPC's snake_case form and forward to the PascalCase dynamic-stub method.

## Generated robustness layer

Each per-RPC method adds, over the raw stub:

- per-call deadline / timeout (`deadlineMs`, defaultable on the client);
- retry with exponential backoff + full jitter on transient gRPC codes
  (`UNAVAILABLE`, `RESOURCE_EXHAUSTED`, and `DEADLINE_EXCEEDED` only for
  read-only RPCs) for unary calls only — client-streaming / bidi are never
  auto-retried;
- TLS / mTLS credentials wiring (`secure`, `tls`);
- metadata: the `UdbMetadata` headers plus `authorization: Bearer …` / `x-api-key`
  / `x-request-id` / SDK + protocol-version tags;
- typed errors (`UdbError`) that unpack the `udb-error-detail-bin` trailer when
  the server sends one.

The four RPC shapes are emitted via `kind=` filters on the per-RPC block
(`unary`, `server_streaming`, `client_streaming`, `bidi`), so streaming methods
return the correct stream/promise types.

## CLI bundling (`udb` binary on install)

`bin/udb.js.tmpl` renders a Node launcher with `{{UDB_VERSION}}` baked in. Installing
`@udb_plus/sdk` exposes a version-matched `udb` command via the package's
`bin` entry (`package.json` → `"bin": { "udb": "./bin/udb.js" }`). At runtime the
launcher:

1. uses `$UDB_BIN` if set;
2. else uses a `udb` already on `PATH` whose `--version` matches;
3. else downloads the matching release asset from
   `github.com/fahara02/udb` (tag `v{{UDB_VERSION}}`, per-OS/arch) into a per-user
   cache dir (`$UDB_CACHE_DIR` or the OS cache dir), extracts, and execs it.

## Manifest wiring (edited in place, additively)

These committed files in `sdk/typescript/` were edited minimally (no version
bump) so the rendered files build and publish:

- `package.json` — added `"bin": { "udb": "./bin/udb.js" }`, a
  `"./generatedClient"` export, and `"bin"` to `files`.
- `index.ts` — `export * from "./generatedClient"`.
- `tsconfig.json` / `tsconfig.build.json` — added `generatedClient.ts` to `include`.

## Customizing

- To change retry defaults or header names, edit `generatedClient.ts.tmpl`
  (`DEFAULT_RETRY_POLICY`, `metadataFor`). Do **not** edit the rendered
  `sdk/typescript/generatedClient.ts` — it is overwritten on regeneration.
- To change download behavior, edit `bin/udb.js.tmpl`.
- Re-run `udb sdk generate --lang typescript` after proto changes; the RPC set is
  re-derived from the descriptor set automatically.
