// benches/core_bench.rs — UDB performance benchmarks using Criterion.
//
// Run with:
//   cargo bench
//   cargo bench -- --output-format bencher 2>&1 | grep "test bench"
//
// HTML reports are written to target/criterion/.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::path::PathBuf;
use udb::{
    CatalogManifest, MigrationPlanConfig, ParserConfig, ProtoSchema, build_drift_report,
    build_migration_plan, lint_catalog, lint_policies, parse_directory_report, schema_checksum,
};

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Returns the path to the `tests/fixtures` directory.
fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

/// Parse the test fixture proto directory and return the schemas.
fn fixture_schemas() -> Vec<ProtoSchema> {
    let dir = fixtures_dir();
    let config = ParserConfig::new("myapp");
    let report = parse_directory_report(&dir, &config).expect("fixture protos must parse");
    report.schemas
}

// ── Benchmarks ───────────────────────────────────────────────────────────────

fn bench_schema_checksum(c: &mut Criterion) {
    let schemas = fixture_schemas();
    c.bench_function("schema_checksum", |b| {
        b.iter(|| schema_checksum(black_box(&schemas)).unwrap());
    });
}

fn bench_manifest_from_schemas(c: &mut Criterion) {
    let schemas = fixture_schemas();
    c.bench_function("CatalogManifest::from_schemas", |b| {
        b.iter(|| CatalogManifest::from_schemas(black_box(&schemas)).unwrap());
    });
}

fn bench_lint_catalog(c: &mut Criterion) {
    let schemas = fixture_schemas();
    let manifest = CatalogManifest::from_schemas(&schemas).unwrap();
    c.bench_function("lint_catalog", |b| {
        b.iter(|| lint_catalog(black_box(&manifest)));
    });
}

fn bench_drift_report(c: &mut Criterion) {
    let schemas = fixture_schemas();
    let manifest = CatalogManifest::from_schemas(&schemas).unwrap();
    c.bench_function("build_drift_report (no prior)", |b| {
        b.iter(|| build_drift_report(black_box(None), black_box(&manifest)));
    });
    c.bench_function("build_drift_report (self-diff)", |b| {
        b.iter(|| build_drift_report(black_box(Some(&manifest)), black_box(&manifest)));
    });
}

fn bench_migration_plan(c: &mut Criterion) {
    let schemas = fixture_schemas();
    c.bench_function("build_migration_plan", |b| {
        b.iter(|| {
            build_migration_plan(
                black_box(None),
                black_box(&schemas),
                black_box(&MigrationPlanConfig::default()),
            )
            .unwrap()
        });
    });
}

fn bench_lint_policies(c: &mut Criterion) {
    // Use an empty policy set to benchmark the lint infrastructure itself.
    let policies = vec![];
    c.bench_function("lint_policies (empty)", |b| {
        b.iter(|| lint_policies(black_box(&policies)));
    });
}

// ── Registration ─────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_schema_checksum,
    bench_manifest_from_schemas,
    bench_lint_catalog,
    bench_drift_report,
    bench_migration_plan,
    bench_lint_policies,
);
criterion_main!(benches);
