# Rust SDK templates

Rendered by `udb sdk generate --lang rust --out sdk` into `sdk/rust/`.

This file and `README.md` are never emitted into the SDK tree — see `should_skip`
in `src/cli/sdk_gen.rs`.

## What is generated, and what is not

| Layer | Where | Generated? |
|---|---|---|
| protobuf / tonic stubs | `OUT_DIR`, via `build.rs` | at **build** time, from `proto/` |
| RPC registry | `sdk/rust/src/generated_rpcs.rs` | by `udb sdk generate`, from the descriptor |
| client, auth, metadata, errors | `sdk/rust/src/*.rs` | hand-written |

Three different mechanisms, deliberately:

- **Stubs** are built rather than committed so the crate cannot drift from the
  contract it ships beside, and so `cargo publish` produces something that builds
  standalone off vendored protos.
- **The RPC registry** is committed because it encodes descriptor facts —
  `operation_kind`, `read_only`, `replay_safe` — that a consumer needs at compile
  time and that must be reviewable in a diff when the contract changes.
- **Everything else** is hand-written because it is a design surface, not a
  projection of the descriptor.

## Why the registry exists

`RpcSpec::replay_safe`. Whether an RPC may be retried is declared by the proto
(`operation_kind`), not inferable from a method name — "update" is not reliably a
mutation and "get" is not reliably a read across every service. Guessing from
names is how a retry replays a charge.

## Adding a template

Any file under this directory is rendered (`.tmpl`) or copied (everything else)
to the matching path under `sdk/rust/`. Placeholders come from
`substitute_rpc`/`template_scalars` in `src/cli/sdk_gen.rs`; a `{{TOKEN}}` with no
substitution is a hard generation error, so a typo fails loudly rather than
shipping a literal `{{TOKEN}}`.

Per-RPC blocks repeat their body once per RPC:

```
// @@UDB_RPC_BEGIN
... {{RPC_PATH}} ...
// @@UDB_RPC_END
```

and accept a filter, e.g. `// @@UDB_RPC_BEGIN kind=unary`.

## After changing a template

```sh
cargo build --bin udb
target/debug/udb sdk generate --lang rust --out sdk
cd sdk/rust && cargo test
```

CI regenerates with `--lang all` and fails if the committed output differs — or,
since the gate now marks new files intent-to-add, if a template emits a file that
was never committed at all.
