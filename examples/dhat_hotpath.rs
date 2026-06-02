//! D.2 heap-allocation profile of the UDB hot paths.
//!
//! Runs the same conversion/parse helpers as `benches/hotpath_bench.rs` over a
//! sample of the `data/` dataset under the dhat heap profiler, so we can see
//! exactly where (and how many) allocations happen — the input to D.3's
//! allocation-reduction work.
//!
//!   cargo run --release --features bench-internals --example dhat_hotpath
//!
//! Writes `dhat-heap.json` in the working directory; open it with the dhat
//! viewer: https://nnethercote.github.io/dh_view/dh_view.html
//!
//! Prints total bytes/blocks allocated so deltas are visible in the terminal
//! before/after an optimization without opening the viewer.

use std::fs::File;
use std::hint::black_box;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use serde_json::Value as JsonValue;
use udb::bench_internals as bi;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data")
}

fn load_lines(name: &str, max: usize) -> Vec<String> {
    let path = data_dir().join(name);
    let file = File::open(&path).unwrap_or_else(|e| {
        panic!(
            "missing {} ({e}). Run: python data/gen_bench_data.py --target-mb 512",
            path.display()
        )
    });
    BufReader::new(file)
        .lines()
        .take(max)
        .map(|l| l.expect("read line"))
        .collect()
}

fn main() {
    let profiler = dhat::Profiler::new_heap();

    // Same bounded sample the Criterion bench uses.
    let record_lines = load_lines("records.ndjson", 8000);
    let sql_lines = load_lines("dispatch_sql.ndjson", 8000);
    let obj_lines = load_lines("dispatch_object.ndjson", 4000);

    let records: Vec<JsonValue> = record_lines
        .iter()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let obj_values: Vec<JsonValue> = obj_lines
        .iter()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    // Read path: Struct -> JSON.
    let structs: Vec<_> = records
        .iter()
        .filter_map(|v| bi::json_to_struct(v))
        .collect();
    for s in &structs {
        black_box(bi::struct_to_json(black_box(s)));
    }
    // Write path: JSON -> Struct / prost Value.
    // Measure the clone-variant vs the consuming move-variant allocation count
    // directly (block deltas around each loop). Allocation counts are
    // deterministic — immune to the CPU-load variance that contaminates timing —
    // so this is the noise-proof evidence that `json_into_struct` (now wired into
    // the store query-result + qdrant paths) cuts the write-path's worst cost.
    let blocks = || dhat::HeapStats::get().total_blocks;

    let c0 = blocks();
    for v in &records {
        black_box(bi::json_to_struct(black_box(v)));
    }
    let clone_blocks = blocks() - c0;

    // Pre-clone owned inputs OUTSIDE the measured region so only the conversion's
    // allocations are counted (the move variant consumes its input).
    let owned: Vec<JsonValue> = records.clone();
    let m0 = blocks();
    for v in owned {
        black_box(bi::json_into_struct(black_box(v)));
    }
    let move_blocks = blocks() - m0;

    for v in &records {
        black_box(bi::json_to_prost_value(black_box(v)));
    }

    println!(
        "write-path conversion (records={}):\n  json_to_struct  (clone): {} blocks\n  json_into_struct (move): {} blocks  ({:.1}% fewer)",
        records.len(),
        clone_blocks,
        move_blocks,
        if clone_blocks > 0 {
            100.0 * (clone_blocks as f64 - move_blocks as f64) / clone_blocks as f64
        } else {
            0.0
        }
    );
    // Dispatch parse.
    for line in &sql_lines {
        black_box(bi::parse_sql_dispatch(black_box(line))).ok();
    }
    // base64 decode.
    for v in &obj_values {
        black_box(bi::object_bytes_from_json(black_box(v))).ok();
    }

    let stats = dhat::HeapStats::get();
    println!(
        "dhat heap profile (sample): records={}, sql={}, object={}",
        records.len(),
        sql_lines.len(),
        obj_values.len()
    );
    println!("  total blocks allocated: {}", stats.total_blocks);
    println!("  total bytes  allocated: {}", stats.total_bytes);
    println!("  peak bytes  (max live): {}", stats.max_bytes);
    println!("wrote dhat-heap.json — view at https://nnethercote.github.io/dh_view/dh_view.html");
    drop(profiler);
}
