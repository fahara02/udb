// benches/live_backends_bench.rs — D.2 END-TO-END perf against LIVE backends.
//
// Unlike hotpath_bench (CPU-only helpers), this drives UDB's real executors
// against the running docker stack: parse generic-dispatch JSON -> execute on
// the live backend -> row/object conversion. It measures the data path UDB
// actually runs in production, per backend TYPE (relational + object here;
// extend with vector/document/KV as their executors are exposed).
//
//   docker compose -f docker-compose.canonical.yml up -d   # + the base stack
//   UDB_BENCH_LIVE=1 cargo bench --features bench-internals --bench live_backends_bench
//
// Each backend is skipped (not failed) if its DSN env is unset OR the connection
// fails, so the bench runs against whatever subset is up. DSNs default to the
// repo's docker ports; override with the env vars below.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use udb::bench_internals as bi;

fn live_enabled() -> bool {
    std::env::var("UDB_BENCH_LIVE").is_ok()
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Per-dialect SQL for the shared relational bench (placeholders differ).
struct SqlOps {
    create: &'static str,
    drop: &'static str,
    seed: &'static str,  // INSERT — params: id, payload
    read: &'static str,  // SELECT — param:  id
    write: &'static str, // UPDATE — params: payload, id
}

const SEED_ROWS: usize = 500;

fn sql_json(sql: &str, params: &[serde_json::Value]) -> String {
    serde_json::json!({ "sql": sql, "params": params }).to_string()
}

/// Connect (skip on failure), create+seed the bench table (DDL via raw_exec,
/// which bypasses UDB's DML-only mutate guard), then measure point read and
/// update write latency through UDB's real executor path.
fn bench_sql(
    c: &mut Criterion,
    name: &str,
    handle: bi::SqlBench,
    ops: &SqlOps,
    payloads: &[String],
) {
    let rt = runtime();

    let setup = rt.block_on(async {
        handle.raw_exec(ops.drop).await.ok();
        handle.raw_exec(ops.create).await?;
        for i in 0..SEED_ROWS {
            let payload = &payloads[i % payloads.len()];
            handle
                .mutate(&sql_json(
                    ops.seed,
                    &[
                        serde_json::json!(format!("bench-{i}")),
                        serde_json::json!(payload),
                    ],
                ))
                .await?;
        }
        Ok::<_, String>(())
    });
    if let Err(e) = setup {
        eprintln!("[{name}] skipped (setup failed): {e}");
        return;
    }

    let read_json = sql_json(ops.read, &[serde_json::json!("bench-0")]);
    let write_payload = payloads[0].clone();

    let mut group = c.benchmark_group(name);
    group.bench_function("read (point select)", |b| {
        b.iter(|| {
            rt.block_on(async { black_box(handle.query(black_box(&read_json)).await.unwrap()) })
        });
    });
    group.bench_function("write (update)", |b| {
        b.iter(|| {
            rt.block_on(async {
                black_box(
                    handle
                        .mutate(&sql_json(
                            ops.write,
                            &[
                                serde_json::json!(write_payload),
                                serde_json::json!("bench-0"),
                            ],
                        ))
                        .await
                        .unwrap(),
                )
            })
        });
    });
    group.finish();

    rt.block_on(async { handle.raw_exec(ops.drop).await.ok() });
}

fn load_payloads(n: usize) -> Vec<String> {
    use std::io::BufRead;
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/records.ndjson");
    let file = std::fs::File::open(&path).unwrap_or_else(|e| {
        panic!(
            "missing {} ({e}); run python data/gen_bench_data.py",
            path.display()
        )
    });
    std::io::BufReader::new(file)
        .lines()
        .take(n)
        .map(|l| l.unwrap())
        .collect()
}

fn bench_postgres(c: &mut Criterion, payloads: &[String]) {
    #[cfg(feature = "postgres")]
    {
        let dsn = env_or("UDB_BENCH_PG_DSN", "postgres://udb:udb@127.0.0.1:55432/udb");
        let rt = runtime();
        match rt.block_on(bi::SqlBench::connect_postgres(&dsn)) {
            Ok(h) => bench_sql(
                c,
                "live_postgres",
                h,
                &SqlOps {
                    create: "CREATE TABLE IF NOT EXISTS udb_bench (id VARCHAR(64) PRIMARY KEY, payload TEXT)",
                    drop: "DROP TABLE IF EXISTS udb_bench",
                    seed: "INSERT INTO udb_bench(id,payload) VALUES($1,$2)",
                    read: "SELECT id,payload FROM udb_bench WHERE id=$1",
                    write: "UPDATE udb_bench SET payload=$1 WHERE id=$2",
                },
                payloads,
            ),
            Err(e) => eprintln!("[live_postgres] skipped (connect failed): {e}"),
        }
    }
    #[cfg(not(feature = "postgres"))]
    let _ = (c, payloads);
}

fn bench_mysql(c: &mut Criterion, payloads: &[String]) {
    #[cfg(feature = "mysql")]
    {
        let dsn = env_or("UDB_BENCH_MYSQL_DSN", "mysql://udb:udb@127.0.0.1:53306/udb");
        let rt = runtime();
        match rt.block_on(bi::SqlBench::connect_mysql(&dsn)) {
            Ok(h) => bench_sql(
                c,
                "live_mysql",
                h,
                &SqlOps {
                    create: "CREATE TABLE IF NOT EXISTS udb_bench (id VARCHAR(64) PRIMARY KEY, payload LONGTEXT)",
                    drop: "DROP TABLE IF EXISTS udb_bench",
                    seed: "INSERT INTO udb_bench(id,payload) VALUES(?,?)",
                    read: "SELECT id,payload FROM udb_bench WHERE id=?",
                    write: "UPDATE udb_bench SET payload=? WHERE id=?",
                },
                payloads,
            ),
            Err(e) => eprintln!("[live_mysql] skipped (connect failed): {e}"),
        }
    }
    #[cfg(not(feature = "mysql"))]
    let _ = (c, payloads);
}

fn bench_mssql(c: &mut Criterion, payloads: &[String]) {
    #[cfg(feature = "mssql")]
    {
        let ado = env_or(
            "UDB_BENCH_MSSQL_DSN",
            "Server=127.0.0.1,51433;User=sa;Password=Udb_Strong#2026;Database=master;TrustServerCertificate=true",
        );
        let handle = bi::SqlBench::connect_mssql(&ado);
        // Tiberius connects lazily; a probe mutate tells us whether it's up.
        let rt = runtime();
        if let Err(e) = rt.block_on(handle.query(&sql_json("SELECT 1", &[]))) {
            eprintln!("[live_mssql] skipped (connect failed): {e}");
            return;
        }
        bench_sql(
            c,
            "live_mssql",
            handle,
            &SqlOps {
                create: "IF OBJECT_ID(N'udb_bench', N'U') IS NULL CREATE TABLE udb_bench (id NVARCHAR(64) PRIMARY KEY, payload NVARCHAR(MAX))",
                drop: "DROP TABLE IF EXISTS udb_bench",
                seed: "INSERT INTO udb_bench(id,payload) VALUES(@P1,@P2)",
                read: "SELECT id,payload FROM udb_bench WHERE id=@P1",
                write: "UPDATE udb_bench SET payload=@P1 WHERE id=@P2",
            },
            payloads,
        );
    }
    #[cfg(not(feature = "mssql"))]
    let _ = (c, payloads);
}

fn bench_s3(c: &mut Criterion, _payloads: &[String]) {
    #[cfg(feature = "s3")]
    {
        let endpoint = env_or("UDB_BENCH_S3_ENDPOINT", "http://127.0.0.1:59000");
        let access = env_or("UDB_BENCH_S3_ACCESS_KEY", "minio");
        let secret = env_or("UDB_BENCH_S3_SECRET_KEY", "minio123");
        let region = env_or("UDB_BENCH_S3_REGION", "us-east-1");
        let handle = bi::S3Bench::connect(&endpoint, &access, &secret, &region);
        let rt = runtime();
        let bucket = "udb-bench";
        // Best-effort create; the bucket may already exist from a prior run (and
        // some S3-compatible stores don't surface that as the SDK's idempotent
        // error). Real access is validated by the seed put below.
        if let Err(e) = rt.block_on(handle.ensure_bucket(bucket)) {
            eprintln!("[live_s3] ensure_bucket: {e} (continuing — bucket may already exist)");
        }
        let get_key =
            serde_json::json!({ "bucket": bucket, "object_key": "bench/get.bin" }).to_string();
        let put_key =
            serde_json::json!({ "bucket": bucket, "object_key": "bench/put.bin" }).to_string();
        let body: Vec<u8> = (0..16 * 1024).map(|i| (i % 251) as u8).collect();
        if let Err(e) = rt.block_on(async { handle.put(&get_key, body.clone()).await }) {
            eprintln!("[live_s3] skipped (object access failed): {e}");
            return;
        }

        let mut group = c.benchmark_group("live_s3");
        group.throughput(criterion::Throughput::Bytes(body.len() as u64));
        group.bench_function("put_object (16 KiB)", |b| {
            b.iter(|| {
                rt.block_on(async { black_box(handle.put(&put_key, body.clone()).await.unwrap()) })
            });
        });
        group.bench_function("get_object (16 KiB)", |b| {
            b.iter(|| rt.block_on(async { black_box(handle.get(&get_key).await.unwrap()) }));
        });
        group.finish();

        rt.block_on(async {
            handle.delete(&get_key).await.ok();
            handle.delete(&put_key).await.ok();
        });
    }
    #[cfg(not(feature = "s3"))]
    let _ = (c, _payloads);
}

fn live_backends(c: &mut Criterion) {
    if !live_enabled() {
        eprintln!("live_backends_bench skipped: set UDB_BENCH_LIVE=1 (with the docker stack up)");
        return;
    }
    let payloads = load_payloads(SEED_ROWS);
    bench_postgres(c, &payloads);
    bench_mysql(c, &payloads);
    bench_mssql(c, &payloads);
    bench_s3(c, &payloads);
}

criterion_group!(benches, live_backends);
criterion_main!(benches);
