# UDB Performance Baseline (D.2)

Committed baseline for the hot-path micro-benchmarks, so every later
optimization (D.3 conversion churn, D.6 SIMD codecs, …) can cite a number and
prove a delta rather than guess. **Profile before optimizing.**

## How to reproduce

```bash
python data/gen_bench_data.py --target-mb 512                        # dataset (seeded)
cargo bench --features bench-internals --bench hotpath_bench          # CPU hot paths
cargo run --release --features bench-internals --example dhat_hotpath # allocations
# End-to-end against the live docker stack (skips any backend that's down):
UDB_BENCH_LIVE=1 cargo bench --features bench-internals --bench live_backends_bench
python scripts/bench_charts.py                                       # vibrant SVG artifacts
```

Two complementary benches:

- **`hotpath_bench`** — CPU-bound per-request helpers in isolation (below).
- **`live_backends_bench`** — UDB's *real executor path* end-to-end against the
  live backends (parse dispatch JSON → execute on the DB → row/object
  conversion), per backend TYPE: relational (Postgres / MySQL / MSSQL) and
  object (S3/MinIO). Env-gated (`UDB_BENCH_LIVE=1`); each backend is skipped, not
  failed, if its DSN env is unset or unreachable. DSNs default to the repo's
  docker ports (override `UDB_BENCH_{PG,MYSQL,MSSQL}_DSN`, `UDB_BENCH_S3_*`).

### Result artifacts (graphs)

- Interactive Criterion plots (violin / PDF / regression): `target/criterion/report/index.html`.
- Vibrant portable SVG dashboards via `scripts/bench_charts.py`:
  `bench-artifacts/{latency,throughput}.svg` + `bench-artifacts/index.html`
  (dependency-free; render anywhere, including PRs/GitHub).

### Persisted history (before/after comparison)

`target/criterion/` is wiped by `cargo clean`, so durable records live in the
**tracked** `bench-history/` directory:

```bash
python scripts/bench_snapshot.py --label "what changed"
```

Writes `bench-history/bench-<UTC>.json` (median + throughput per benchmark, with
git commit / rustc / os) and a `bench-<UTC>.md` (+ `latest.md`) that diffs
against the previous snapshot — `Δ time` (negative = faster, ✅) and `Δ thrpt`
(positive = faster). Run it after each optimization pass (D.3/D.6) to prove the
delta against the committed baseline below.

The benches load a bounded, representative sample from `data/` (first 8000
records / 8000 SQL / 4000 object / 4000 REST lines) and report `Throughput` so
numbers compare across machines and across passes. Criterion HTML reports +
saved baselines live under `target/criterion/`.

## Environment (this run)

- Platform: Windows 11 (x86_64), `bench` profile (optimized + LTO per Cargo.toml).
- Toolchain: rustc 1.95.0 (2026-04-14).
- Dataset: 512.1 MiB, seed 20260602 (`data/MANIFEST.json`). All default features
  + `bench-internals`.

## Timings (Criterion `[p5  median  p95]`, throughput at median)

| Hot path | low | **median** | high | throughput | notes |
|---|---|---|---|---|---|
| `struct_to_json` (row READ) | 17.60 ms | **18.15 ms** | 18.77 ms | 230 MiB/s | clones every field key |
| `json_to_struct` (row WRITE) | 38.63 ms | **39.72 ms** | 40.95 ms | **105 MiB/s** | ⚠ slowest — builds BTreeMap + clones strings |
| `json_to_prost_value` | 33.17 ms | **34.04 ms** | 34.91 ms | 123 MiB/s | recursive owned rebuild |
| `parse_sql_dispatch` | 6.74 ms | **7.02 ms** | 7.32 ms | 158 MiB/s | full serde parse + `sql`/params clone |
| `parse_object_dispatch` | 6.92 ms | **7.11 ms** | 7.30 ms | 1.52 GiB/s | parses whole line, reads 3 fields |
| `parse_rest_dispatch` | 10.71 ms | **11.08 ms** | 11.44 ms | **91 MiB/s** | ⚠ clones the entire `body` JSON |
| `object_bytes_from_json` (base64 DECODE) | 7.03 ms | **7.32 ms** | 7.62 ms | 1.05 GiB/s | scalar base64 — D.6 candidate |
| `base64_cell` (base64 ENCODE) | 4.46 ms | **4.59 ms** | 4.73 ms | 1.68 GiB/s | scalar base64 — D.6 candidate |
| `merge_context` (per request) | 327 ns | **339 ns** | 352 ns | 2.95 Melem/s | ~10 `String` allocs via `first_non_empty` |

(median per batch of N lines; per-op = median / N.)

## Live-backend end-to-end (UDB_BENCH_LIVE=1, docker stack)

UDB's real executor path (parse dispatch JSON → execute on the live DB → row/
object conversion), median per op. These are the numbers that must improve when
the runtime is optimized — they reflect shipped code, not scaffolding.

| Backend (type) | read / get | write / put | notes |
|---|---|---|---|
| Postgres (relational) | 1.93 ms (point select) | 2.68 ms (update) | tx-wrapped, RLS-capable path |
| MySQL (relational) | 1.80 ms (point select) | 1.62 ms (update) | |
| S3 / MinIO (object) | 2.33 ms (get 16 KiB) | 14.16 ms (put 16 KiB) | put dominated by MinIO round-trip |
| MSSQL (relational) | — | — | skipped: tiberius pool cold-handshake exceeds executor timeout; relational type already covered by PG+MySQL (known follow-up) |

Vector/document/KV/wide-column types are not yet wired into the live bench (the
executors need exposure like the SQL/object handles); they're the next coverage
extension.

## Allocations (dhat, one pass over the sample)

- total blocks allocated: **1,776,129**
- total bytes allocated: **265,599,523** (~253 MiB)
- peak live bytes: **95,258,967** (~91 MiB)

That is ~133 allocations per record across the read+write+parse+decode pass —
dominated by the JSON↔Struct conversions (per-field `String` clones + per-row
`BTreeMap`) and `parse_rest_dispatch`'s full-body clone.

## Optimization targets (ranked, for D.3 / D.6)

1. **Write path (`json_to_struct` / `json_to_prost_value`, 105–123 MiB/s).**
   Biggest CPU + allocation sink. D.3: borrowed/streaming construction; avoid
   per-field `key.clone()`; reuse buffers; convert to owned only at the executor
   boundary.
2. **`parse_rest_dispatch` (91 MiB/s).** `v.get("body").cloned()` deep-clones the
   whole body. D.3: take the body by move (`Value::take`/`Map::remove`) instead of
   cloning.
3. **`struct_to_json` (230 MiB/s).** `key.clone()` per field. D.3: build the map
   without cloning keys where the caller owns the Struct.
4. **base64 decode/encode (1.05 / 1.68 GiB/s, scalar).** D.6: evaluate
   `base64-simd` behind `simd-codecs`; adopt only if it beats scalar on the
   observed payload-size mix (object blobs 64 B–4 KiB; row `payload_b64` 8–96 B).
5. **`merge_context` (339 ns, ~10 allocs).** Minor; `first_non_empty` allocates
   even when the metadata side wins. D.3: return `Cow`/borrow where possible.

`parse_object_dispatch` (1.52 GiB/s) and `parse_sql_dispatch` (158 MiB/s) are
already cheap relative to the conversions; deprioritized.

## D.6 SIMD-crate adoption decision (data-grounded)

`runtime/accel` exposes feature-gated SIMD impls (`simd-codecs`→`base64-simd`,
`simd-checksum`→`crc32fast`), proven byte-identical to scalar by the equivalence
tests, and a same-run scalar-vs-SIMD harness (`hotpath_bench::simd_vs_scalar`).

**Decision: ADOPTED — `simd-codecs` + `simd-checksum` are now in the default
feature set** (per the production-grade directive, which supersedes the earlier
conservative "SIMD-off until benchmarks" gate). The object codec path (typed
PutObject/GetObject base64 via `accel`) now uses `base64-simd`; chunk integrity
uses `crc32fast`. Both are safe, maintained crates with internal runtime
CPU-feature detection and a scalar fallback, and are proven byte-identical to the
scalar path by the equivalence tests (which now run by default). Slim builds opt
OUT via `--no-default-features` (the scalar path always remains and is the
correctness oracle). Security/audit hashes stay on `sha2` — CRC32 is
non-cryptographic, integrity-only. Quantify the per-payload gain with
`hotpath_bench::simd_vs_scalar`.

## Measured deltas — SIMD adoption + move-conversion (same-run, contention-immune)

These are the numbers that justify the production changes. They are **same-run,
back-to-back** comparisons (scalar vs SIMD, clone vs move, measured one after the
other in a single `cargo bench` invocation), so they are immune to cross-run
machine-load variance. (The cross-run criterion `change:` deltas from the run
that produced these were uniformly inflated ~2–3× across *every* bench, including
untouched ones like `merge_context` — the signature of concurrent build load on
the box, not a code regression. Absolute cross-run numbers from a contended run
are not cited.)

### SIMD codecs (`accel`, now default) — scalar → SIMD speedup

| Payload | base64 encode | base64 decode | crc32 |
|---|---|---|---|
| 64 B    | 1.36× | 1.50× | 15.3× |
| 1 KiB   | 2.28× | 2.00× | 143×  |
| 16 KiB  | 2.59× | 2.17× | 168×  |
| 256 KiB | 2.86× | 2.13× | 175×  |

- base64: scalar ~0.8 GiB/s → `base64-simd` **1.6–2.3 GiB/s** on object payloads.
- crc32: scalar bit-loop **~90 MiB/s** → `crc32fast` (hardware CRC/PCLMULQDQ)
  **13–16 GiB/s**. The scalar CRC was genuinely the worst codec path; SIMD makes
  chunk-integrity checksums effectively free.
- Verified byte-identical to scalar by the `accel` equivalence tests (5/5 pass on
  the default+SIMD build).

### Move vs clone conversion (`json_into_struct`) — the write-path worst case

`json_to_struct` (borrow + clone every field) **68.0 ms** →
`json_into_struct` (move keys/strings/arrays) **47.6 ms** = **1.43× faster**
(61.5 → 87.9 MiB/s). Now wired into the shipped hot paths that own and discard
their parsed JSON: `service::handlers_stores::structs_from_result` (every SQL /
document / graph query result) and the qdrant response/upsert paths. The
borrowing variant is retained only where the value is genuinely reused (the
`core::mod` dual proto+JSON row build; the qdrant filter that is also moved into
`search`).

### Row-build loop allocation reduction (`core::rows_to_record_set`)

The relational read hot path now preallocates: the result vectors (`proto_rows`,
`records_json`) to the known `rows.len()`, and each row's proto `fields` map to
the row's column count. A wide, multi-row result no longer reallocates+copies the
growing result Vec per row, nor rehashes/regrows the per-row field HashMap
cell-by-cell. Benefit scales with result width × row count (allocation-count
reduction, not a single-row latency change — the existing single-row live bench
won't surface it; it matters for large scans). The `json_value` is genuinely
reused to build both the proto Value and the JSON-bytes representation, so the
per-cell conversion stays a borrow until the wire protocol negotiates a single
representation (maintainer's `negotiation.*` work).

## D.7 handwritten-assembly decision

**Decision: no handwritten assembly.** The `asm-kernels` feature gate stays
off-by-default with no kernels written. Rationale (the gate's criteria are not
met): (1) the codec hot paths aren't even the bottleneck (above), so a kernel
would optimize the wrong thing; (2) where SIMD does help, maintained crates
(`base64-simd`/`crc32fast`) cover it — the doc requires trying those + `core::arch`
intrinsics before `asm!`, and they suffice; (3) the scalar fallback is always
present and is the correctness oracle. Assembly cannot break unsupported targets
because none is compiled in. Revisit only if a future profile shows a tiny kernel
where intrinsics are provably worse.
