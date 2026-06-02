# Benchmark snapshot `20260602T180224Z`

- commit `8dd0cbf` · rustc 1.95.0 (59807616e 2026-04-14) · os `win32`
- label: SIMD-default adopted + json_into_struct wired (structs_from_result); note: cross-run absolutes contaminated by concurrent build load, same-run simd_vs_scalar deltas are the valid proof

## Δ vs `20260602T132626Z` — median time (negative Δ = faster), throughput (positive Δ = faster)

| benchmark | prev | now | Δ time | Δ thrpt |
|---|---|---|---|---|
| `base64/base64_cell (encode)` | 4.14 ms | 7.39 ms | +78.4% ⚠️ | -43.9% |
| `base64/object_bytes_from_json (decode)` | 4.38 ms | 8.15 ms | +85.9% ⚠️ | -46.2% |
| `dispatch_parse/parse_object_dispatch` | 5.04 ms | 14.95 ms | +196.6% ⚠️ | -66.3% |
| `dispatch_parse/parse_rest_dispatch` | 5.31 ms | 15.19 ms | +186.1% ⚠️ | -65.0% |
| `dispatch_parse/parse_sql_dispatch` | 4.50 ms | 11.80 ms | +162.1% ⚠️ | -61.8% |
| `live_mysql/read (point select)` | 1.80 ms | 1.80 ms | +0.0% |  |
| `live_mysql/write (update)` | 1.62 ms | 1.62 ms | +0.0% |  |
| `live_postgres/read (point select)` | 1.93 ms | 1.93 ms | +0.0% |  |
| `live_postgres/write (update)` | 2.68 ms | 2.68 ms | +0.0% |  |
| `live_s3/get_object (16 kib)` | 2.33 ms | 2.33 ms | +0.0% | +0.0% |
| `live_s3/put_object (16 kib)` | 14.16 ms | 14.16 ms | +0.0% | +0.0% |
| `merge_context/merge_context (proto + metadata)` | 299.46 ns | 953.34 ns | +218.4% ⚠️ | -68.6% |
| `simd_vs_scalar/b64_decode_scalar_1024` | — | 1.38 µs | _new_ | |
| `simd_vs_scalar/b64_decode_scalar_16384` | — | 20.70 µs | _new_ | |
| `simd_vs_scalar/b64_decode_scalar_262144` | — | 322.34 µs | _new_ | |
| `simd_vs_scalar/b64_decode_scalar_64` | — | 229.46 ns | _new_ | |
| `simd_vs_scalar/b64_decode_simd_1024` | — | 696.46 ns | _new_ | |
| `simd_vs_scalar/b64_decode_simd_16384` | — | 9.84 µs | _new_ | |
| `simd_vs_scalar/b64_decode_simd_262144` | — | 144.91 µs | _new_ | |
| `simd_vs_scalar/b64_decode_simd_64` | — | 154.48 ns | _new_ | |
| `simd_vs_scalar/b64_encode_scalar_1024` | — | 1.20 µs | _new_ | |
| `simd_vs_scalar/b64_encode_scalar_16384` | — | 17.92 µs | _new_ | |
| `simd_vs_scalar/b64_encode_scalar_262144` | — | 294.32 µs | _new_ | |
| `simd_vs_scalar/b64_encode_scalar_64` | — | 208.63 ns | _new_ | |
| `simd_vs_scalar/b64_encode_simd_1024` | — | 531.01 ns | _new_ | |
| `simd_vs_scalar/b64_encode_simd_16384` | — | 6.74 µs | _new_ | |
| `simd_vs_scalar/b64_encode_simd_262144` | — | 109.60 µs | _new_ | |
| `simd_vs_scalar/b64_encode_simd_64` | — | 144.64 ns | _new_ | |
| `simd_vs_scalar/crc32_scalar_1024` | — | 10.36 µs | _new_ | |
| `simd_vs_scalar/crc32_scalar_16384` | — | 167.12 µs | _new_ | |
| `simd_vs_scalar/crc32_scalar_262144` | — | 2.60 ms | _new_ | |
| `simd_vs_scalar/crc32_scalar_64` | — | 644.80 ns | _new_ | |
| `simd_vs_scalar/crc32_simd_1024` | — | 73.72 ns | _new_ | |
| `simd_vs_scalar/crc32_simd_16384` | — | 996.56 ns | _new_ | |
| `simd_vs_scalar/crc32_simd_262144` | — | 16.14 µs | _new_ | |
| `simd_vs_scalar/crc32_simd_64` | — | 40.12 ns | _new_ | |
| `struct_json/json_into_struct (write path, move)` | — | 46.00 ms | _new_ | |
| `struct_json/json_to_prost_value` | 39.58 ms | 65.00 ms | +64.2% ⚠️ | -39.1% |
| `struct_json/json_to_struct (write path)` | 38.65 ms | 38.65 ms | +0.0% | +0.0% |
| `struct_json/json_to_struct (write path, clone)` | — | 65.63 ms | _new_ | |
| `struct_json/struct_to_json (read path)` | 48.27 ms | 58.99 ms | +22.2% ⚠️ | -18.2% |

_Noise floor ≈ **22.2%** (median |Δ| across all 15 benches; largest unrelated swing: `merge_context/merge_context (proto + metadata)` +218.4%). Deltas within ±22.2% are run-to-run variance — re-run on a quiet machine / CI (D.8) for precise figures._
