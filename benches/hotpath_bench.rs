// benches/hotpath_bench.rs — D.2 hot-path micro-benchmarks.
//
// Profiles the per-request conversion/parse helpers UDB runs on every read,
// write, dispatch, and object operation, using the realistic dataset under
// `data/` (generate it first: `python data/gen_bench_data.py --target-mb 512`).
//
// Requires the `bench-internals` feature to reach the `pub(crate)` helpers:
//   cargo bench --features bench-internals --bench hotpath_bench
//
// Each group reports Throughput so results are comparable across machines and
// across optimization passes (D.3/D.6). HTML reports land in target/criterion/.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use serde_json::Value as JsonValue;
use udb::bench_internals as bi;

// ── Dataset loading (bounded, representative sample) ──────────────────────────

fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data")
}

/// Read up to `max` lines from a `data/<name>` NDJSON file. Panics with a clear
/// hint if the dataset has not been generated.
fn load_lines(name: &str, max: usize) -> Vec<String> {
    let path = data_dir().join(name);
    let file = File::open(&path).unwrap_or_else(|e| {
        panic!(
            "missing {} ({e}). Generate the dataset first:\n  \
             python data/gen_bench_data.py --target-mb 512",
            path.display()
        )
    });
    BufReader::new(file)
        .lines()
        .take(max)
        .map(|l| l.expect("read dataset line"))
        .collect()
}

fn parse_values(lines: &[String]) -> Vec<JsonValue> {
    lines
        .iter()
        .map(|l| serde_json::from_str(l).expect("dataset line is valid JSON"))
        .collect()
}

fn total_bytes(lines: &[String]) -> u64 {
    lines.iter().map(|l| l.len() as u64).sum()
}

// ── Struct <-> JSON (the row read/write conversion path) ──────────────────────

fn bench_struct_json(c: &mut Criterion) {
    let lines = load_lines("records.ndjson", 8000);
    let values = parse_values(&lines);
    let structs: Vec<bi::Struct> = values
        .iter()
        .map(|v| bi::json_to_struct(v).expect("record is a JSON object"))
        .collect();
    let bytes = total_bytes(&lines);

    let mut group = c.benchmark_group("struct_json");
    group.throughput(Throughput::Bytes(bytes));
    group.bench_function("struct_to_json (read path)", |b| {
        b.iter(|| {
            for s in &structs {
                black_box(bi::struct_to_json(black_box(s)));
            }
        });
    });
    group.bench_function("json_to_struct (write path, clone)", |b| {
        b.iter(|| {
            for v in &values {
                black_box(bi::json_to_struct(black_box(v)));
            }
        });
    });
    // D.3: same-run comparison of the consuming variant. iter_batched keeps the
    // per-iteration `clone()` (to produce owned inputs) OUT of the measured
    // routine, so this measures only the move-based conversion vs the clone-based
    // one above — a fair delta immune to cross-run machine-load variance.
    group.bench_function("json_into_struct (write path, move)", |b| {
        b.iter_batched(
            || values.clone(),
            |owned| {
                for v in owned {
                    black_box(bi::json_into_struct(black_box(v)));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.bench_function("json_to_prost_value", |b| {
        b.iter(|| {
            for v in &values {
                black_box(bi::json_to_prost_value(black_box(v)));
            }
        });
    });
    group.finish();
}

// ── RecordSet row build (the per-row cost of populating `rows`) ───────────────

// The live relational read path serialises each row into `records_json` and
// pushes an EMPTY `ProtoRow`, while the cache path already builds real proto
// fields — so the same query answers differently on a cache hit than on a miss.
// Making the live path populate `rows` re-adds a per-row map plus one
// `json_to_prost_value` per column, and that cost is currently unmeasured.
//
// It cannot be measured end to end from a CPU-only bench: the row-build function
// takes `Vec<PgRow>` (it needs a live database) and depends on a private metrics
// type, so driving it here would mean widening visibility purely for
// benchmarking — which is exactly what the bench-internals doctrine warns
// against. Instead this group measures the work the change ADDS, through the
// same shipped conversion the runtime calls, by contrasting the two shapes on
// identical records.
//
// Scope, stated plainly so the number is not over-read: decryption, PII masking
// and the `PgRow` -> JSON step are NOT included. Treat this as a lower bound on
// the per-row cost, not as coverage of the whole row-build path.
fn bench_record_set_rows(c: &mut Criterion) {
    let lines = load_lines("records.ndjson", 8000);
    let values = parse_values(&lines);
    let bytes = total_bytes(&lines);

    let mut group = c.benchmark_group("record_set_rows");
    group.throughput(Throughput::Bytes(bytes));
    // Current live path: records_json only, empty proto rows.
    group.bench_function("records_json only (empty rows)", |b| {
        b.iter(|| {
            for v in &values {
                black_box(serde_json::to_vec(black_box(v)).expect("record serialises"));
            }
        });
    });
    // Populated shape: the same serialise plus the per-row proto field map that
    // the cache path already builds.
    group.bench_function("records_json + populated rows", |b| {
        b.iter(|| {
            for v in &values {
                black_box(serde_json::to_vec(black_box(v)).expect("record serialises"));
                let fields: std::collections::HashMap<String, bi::ProstValue> = v
                    .as_object()
                    .expect("record is a JSON object")
                    .iter()
                    .filter_map(|(key, val)| {
                        bi::json_to_prost_value(val).map(|value| (key.clone(), value))
                    })
                    .collect();
                black_box(fields);
            }
        });
    });
    group.finish();
}

// ── Generic-dispatch request parsing ──────────────────────────────────────────

fn bench_dispatch_parse(c: &mut Criterion) {
    let sql = load_lines("dispatch_sql.ndjson", 8000);
    let sql_bytes = total_bytes(&sql);
    let mut group = c.benchmark_group("dispatch_parse");
    group.throughput(Throughput::Bytes(sql_bytes));
    group.bench_function("parse_sql_dispatch", |b| {
        b.iter(|| {
            for line in &sql {
                black_box(bi::parse_sql_dispatch(black_box(line))).ok();
            }
        });
    });

    #[cfg(any(feature = "gcs", feature = "azureblob"))]
    {
        let obj = load_lines("dispatch_object.ndjson", 4000);
        group.throughput(Throughput::Bytes(total_bytes(&obj)));
        group.bench_function("parse_object_dispatch", |b| {
            b.iter(|| {
                for line in &obj {
                    black_box(bi::parse_object_dispatch(
                        black_box(line),
                        &["bucket", "container"],
                        &["key", "object"],
                        "bucket",
                    ))
                    .ok();
                }
            });
        });
    }

    #[cfg(any(feature = "pinecone", feature = "weaviate", feature = "elasticsearch"))]
    {
        let rest = load_lines("dispatch_rest.ndjson", 4000);
        group.throughput(Throughput::Bytes(total_bytes(&rest)));
        group.bench_function("parse_rest_dispatch", |b| {
            b.iter(|| {
                for line in &rest {
                    black_box(bi::parse_rest_dispatch(black_box(line))).ok();
                }
            });
        });
    }
    group.finish();
}

// ── Object/blob base64 codecs (D.6 SIMD candidates) ───────────────────────────

fn bench_base64(c: &mut Criterion) {
    let obj_lines = load_lines("dispatch_object.ndjson", 4000);
    let obj_values = parse_values(&obj_lines);
    // Raw bytes recovered from the dataset (drives the encode bench).
    let raw: Vec<Vec<u8>> = obj_values
        .iter()
        .filter_map(|v| bi::object_bytes_from_json(v).ok())
        .collect();
    let raw_bytes: u64 = raw.iter().map(|b| b.len() as u64).sum();

    let mut group = c.benchmark_group("base64");
    group.throughput(Throughput::Bytes(raw_bytes));
    group.bench_function("object_bytes_from_json (decode)", |b| {
        b.iter(|| {
            for v in &obj_values {
                black_box(bi::object_bytes_from_json(black_box(v))).ok();
            }
        });
    });

    #[cfg(any(feature = "mysql", feature = "sqlite", feature = "mssql"))]
    group.bench_function("base64_cell (encode)", |b| {
        b.iter(|| {
            for bytes in &raw {
                black_box(bi::base64_cell(black_box(bytes)));
            }
        });
    });
    group.finish();
}

// ── Per-request RequestContext merge ──────────────────────────────────────────

fn bench_merge_context(c: &mut Criterion) {
    let proto = bi::ProtoRequestContext {
        user_id: "user-42".into(),
        correlation_id: "corr-abc".into(),
        purpose: "read".into(),
        target_backend: "postgres".into(),
        target_instance: "primary".into(),
        ..Default::default()
    };
    let metadata = bi::RequestContext {
        tenant_id: "tenant-7".into(),
        project_id: "proj-3".into(),
        ..Default::default()
    };

    let mut group = c.benchmark_group("merge_context");
    group.throughput(Throughput::Elements(1));
    group.bench_function("merge_context (proto + metadata)", |b| {
        b.iter(|| {
            black_box(bi::merge_context(
                black_box(Some(&proto)),
                black_box(metadata.clone()),
            ))
        });
    });
    group.finish();
}

// ── Descriptor-derived method-security scope map ────────────────────────────
//
// The former `authz_snapshot_rebuild` group benched `bi::AbacPolicy` /
// `rebuild_authz_snapshot_from_abac`. That ABAC shape was replaced wholesale by
// the v2 engine (`runtime::authz::AuthzPolicy` / `Effect` / `AuthzSnapshot`),
// and neither the type nor the function exists any more — so this bench file
// did not compile at all. The group is removed rather than guessed at: the v2
// decision path exposes no public entry point to bench, so re-adding authz
// coverage needs a deliberate `bench_internals` shim over the real per-request
// decision, not a reconstruction of a measurement whose subject is gone.

fn bench_scope_maps(c: &mut Criterion) {
    let paths = bi::method_security_scope_paths();
    assert!(
        !paths.is_empty(),
        "descriptor-derived method-security scope map has no scoped methods"
    );
    let mut scope_map = c.benchmark_group("method_security_scope_map");
    scope_map.throughput(Throughput::Elements(paths.len() as u64));
    scope_map.bench_function("rebuild_from_descriptor", |b| {
        b.iter(|| {
            black_box(bi::rebuild_method_security_scope_registry());
        });
    });
    scope_map.bench_function("lookup_declared_scopes", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for path in &paths {
                total += bi::method_security_scope_count(black_box(path));
            }
            black_box(total);
        });
    });
    scope_map.finish();
}

// D.6: scalar vs maintained-SIMD-crate, measured side-by-side in ONE run across
// the object-payload size regimes — the decision input for adopting `base64-simd`
// / `crc32fast`. Only present when both SIMD features are enabled; otherwise a
// no-op (the default build still has no SIMD deps).
fn bench_simd_vs_scalar(c: &mut Criterion) {
    #[cfg(all(feature = "simd-codecs", feature = "simd-checksum"))]
    {
        use udb::bench_internals::{checksum_bench, codec_bench};
        let mut group = c.benchmark_group("simd_vs_scalar");
        for &n in &[64usize, 1024, 16 * 1024, 256 * 1024] {
            let data: Vec<u8> = (0..n).map(|i| (i * 31 + 7) as u8).collect();
            let encoded = codec_bench::simd_encode(&data);
            group.throughput(Throughput::Bytes(n as u64));
            group.bench_function(format!("b64_encode/scalar/{n}"), |b| {
                b.iter(|| black_box(codec_bench::scalar_encode(black_box(&data))))
            });
            group.bench_function(format!("b64_encode/simd/{n}"), |b| {
                b.iter(|| black_box(codec_bench::simd_encode(black_box(&data))))
            });
            group.bench_function(format!("b64_decode/scalar/{n}"), |b| {
                b.iter(|| black_box(codec_bench::scalar_decode(black_box(&encoded))))
            });
            group.bench_function(format!("b64_decode/simd/{n}"), |b| {
                b.iter(|| black_box(codec_bench::simd_decode(black_box(&encoded))))
            });
            group.bench_function(format!("crc32/scalar/{n}"), |b| {
                b.iter(|| black_box(checksum_bench::scalar_crc32(black_box(&data))))
            });
            group.bench_function(format!("crc32/simd/{n}"), |b| {
                b.iter(|| black_box(checksum_bench::simd_crc32(black_box(&data))))
            });
        }
        group.finish();
    }
    #[cfg(not(all(feature = "simd-codecs", feature = "simd-checksum")))]
    let _ = c;
}

criterion_group!(
    benches,
    bench_struct_json,
    bench_record_set_rows,
    bench_dispatch_parse,
    bench_base64,
    bench_merge_context,
    bench_scope_maps,
    bench_simd_vs_scalar,
);
criterion_main!(benches);
