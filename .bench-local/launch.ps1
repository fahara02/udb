# Isolated local bench broker — fresh PG db (udb_bench_local), ports 50071/50081,
# bound 0.0.0.0 so the PHP Docker container can reach it via host.docker.internal.
$ErrorActionPreference = "Stop"
Set-Location "E:\Projects\udb"

$env:UDB_GRPC_ADDR        = "0.0.0.0:50071"
$env:UDB_GRPC_TARGET      = "127.0.0.1:50071"
$env:UDB_AUTH_GRPC_ADDR   = "0.0.0.0:50081"
$env:UDB_AUTH_GRPC_TARGET = "127.0.0.1:50081"
$env:UDB_METRICS_ADDR     = "127.0.0.1:19093"

$env:UDB_PG_DSN        = "postgres://udb:udb@127.0.0.1:55460/udb_bench_local"
$env:UDB_PG_MAX_CONNECTIONS = "60"
$env:UDB_PG_MIN_CONNECTIONS = "4"
$env:UDB_PG_ACQUIRE_TIMEOUT = "60"
$env:UDB_NOSQL_DSN     = "mongodb://127.0.0.1:57017/udb_bench_local?directConnection=true"
$env:UDB_MONGODB_DSN   = "mongodb://127.0.0.1:57017/udb_bench_local?directConnection=true"
$env:UDB_NOSQL_DATABASE = "udb_bench_local"
$env:UDB_MINIO_ENDPOINT = "http://127.0.0.1:59000"
$env:UDB_MINIO_ACCESS_KEY = "minio"
$env:UDB_MINIO_SECRET_KEY = "minio123"
$env:UDB_KAFKA_BROKERS = "127.0.0.1:59192"
$env:UDB_REDIS_DSN     = "redis://127.0.0.1:56379"
$env:UDB_QDRANT_URL    = "http://127.0.0.1:56333"
$env:UDB_MINIO_REGION  = "us-east-1"

$env:UDB_ABAC_DEFAULT_ALLOW         = "true"
# Degrade (don't crash) when a backend fails to connect/apply — neo4j/qdrant drift
# would otherwise EXIT the whole process (bug_report.md C3). This is the fix.
$env:UDB_ALLOW_DEGRADED_BACKENDS    = "true"
$env:UDB_CDC_ENABLED                = "true"
$env:UDB_PROJECTION_WORKER_ENABLED  = "false"
$env:UDB_RECONCILIATION_ENABLED     = "false"
$env:UDB_SAGA_RECOVERY_ENABLED      = "false"
# Reapers off: the seed joins a WebRTC peer via a unary call (no live signaling
# heartbeat), so the 60s reaper would disconnect it before measurement.
$env:UDB_WEBRTC_REAP_INTERVAL_SECS  = "0"
$env:UDB_STORAGE_REAP_INTERVAL_SECS = "0"
$env:UDB_STARTUP_FORCE_SYNC         = "true"
$env:UDB_STARTUP_DRY_RUN            = "false"
$env:UDB_AUDIT_SINK_URL             = "file:///E:/Projects/udb/.bench-local/audit.jsonl"
$env:UDB_ENCRYPTION_KEY             = "QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI="
$env:UDB_JWT_PRIVATE_KEY            = "src/runtime/testdata/jwt_rs256_private.pem"
$env:UDB_JWT_PUBLIC_KEY             = "src/runtime/testdata/jwt_rs256_public.pem"
$env:UDB_SESSION_ENABLED           = "true"
$env:UDB_SESSION_HASH_SECRET       = "ci-bench-session-hash-secret"
$env:UDB_PASSWORD_HASH_SECRET      = "ci-bench-password-hash-secret"

# Restart loop: the release binary occasionally exits after a heavy run; keep it
# available for iterative bench runs. Each (re)start re-runs idempotent setup.
while ($true) {
  "=== broker (re)start $(Get-Date -Format o) ===" | Out-File -FilePath "E:\Projects\udb\.bench-local\broker.log" -Append
  & "E:\Projects\udb\target\debug\udb.exe" serve proto "" 0.0.0.0:50071 *>&1 | Tee-Object -FilePath "E:\Projects\udb\.bench-local\broker.log" -Append
  Start-Sleep -Seconds 2
}
