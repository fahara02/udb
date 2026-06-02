# Benchmark dataset (D.2)

This directory holds the dataset for the hot-path performance benchmarks
(`benches/hotpath_bench.rs`) and the allocation profile
(`examples/dhat_hotpath.rs`).

The data itself is **generated, not committed** (it can reach ~1 GiB and is
fully reproducible). Only this README and the generator are tracked; everything
else here is git-ignored.

## Generate

```bash
python data/gen_bench_data.py --target-mb 512   # clamped to [16, 1000] MiB
```

Produces deterministic (seeded) NDJSON:

| File | Drives |
|---|---|
| `records.ndjson` | `struct_to_json` / `json_to_struct` / `json_to_prost_value` |
| `dispatch_sql.ndjson` | `parse_sql_dispatch` |
| `dispatch_object.ndjson` | `parse_object_dispatch`, `object_bytes_from_json` (base64 decode) |
| `dispatch_rest.ndjson` | `parse_rest_dispatch` |
| `blobs_b64.ndjson` | base64 codec benches |
| `MANIFEST.json` | record counts + byte sizes (total ≤ 1 GiB) |

## Run

```bash
cargo bench  --features bench-internals --bench hotpath_bench
cargo run --release --features bench-internals --example dhat_hotpath
```
