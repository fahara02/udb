# Vendored: `postgres-replication`

This subdirectory is a vendored copy of the `postgres-replication`
crate from the [`sfackler/rust-postgres`](https://github.com/sfackler/rust-postgres)
workspace, at the `iambriccardo/rust-postgres` fork's commit
`31acf55c7e5c2244e5bb3a36e7afa2a01bf52c38` (which adds support for
the PostgreSQL logical-replication `MESSAGE` (`M`) tag — used by
`pg_logical_emit_message()`).

## Why vendor

`postgres-replication` is not (yet) published to crates.io. Until
upstream publishes it, UDB cannot be released as a crates.io
package while depending on the crate via a git reference (crates.io
rejects git-only deps). Vendoring the source into the UDB tree
makes the published `udb` crate self-contained.

## Source provenance

| Item | Value |
|---|---|
| Upstream | https://github.com/sfackler/rust-postgres |
| Fork | https://github.com/iambriccardo/rust-postgres |
| Vendored rev | `31acf55c7e5c2244e5bb3a36e7afa2a01bf52c38` |
| Crate version at rev | `0.6.7` |
| Original authors | Petros Angelatos, plus contributors via #2 (`support-copy-done`) |
| License | MIT OR Apache-2.0 (dual) |
| Vendored on | 2026-05-31 |

The fork's only divergence from upstream at this rev is a 49-line
addition in `protocol.rs` adding the `MessageBody` type. The
sibling crates (`tokio-postgres`, `postgres-protocol`,
`postgres-types`) are byte-identical to upstream at this rev, so
we depend on their crates.io versions directly.

## Layout

- `mod.rs` — formerly the crate's `lib.rs`. Renamed to fit Rust
  module conventions; the only change vs. upstream is the
  references to `crate::protocol` work as-is because protocol.rs
  sits alongside.
- `protocol.rs` — wire-protocol decoding (verbatim from the fork).
- `LICENSES/` — dual MIT + Apache-2.0 license text, retained for
  attribution as required by both licenses.

## Refresh procedure

When upstream publishes `postgres-replication` to crates.io OR
when the fork's `MESSAGE` patch is merged upstream:

1. Compare the published version's source to this vendored copy.
2. If functionally equivalent, replace this directory with a
   crates.io dependency and a `use postgres_replication::*` import
   in `src/runtime/cdc/mod.rs`.
3. Drop the per-deps (`memchr`, `byteorder`, `pin-project-lite`)
   if no other code in UDB uses them.

## Modifications by UDB

None. The source is unchanged from the fork's rev. The only
adaptation is the module path (`mod.rs` instead of `lib.rs`); no
edits to function bodies, types, or behaviour.
