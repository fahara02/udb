$ErrorActionPreference = "Stop"
Set-Location "E:\Projects\udb"
. (Join-Path $PSScriptRoot "bench-process.ps1")
$UdbBenchBin = Resolve-UdbBenchBin
# CRITICAL: clear any MSSQL/MySQL/Cassandra DSN inherited from .env / the parent shell.
# The broker tries to connect to them at startup; MSSQL/MySQL not running → "Could not
# fetch metadata: No connections in the pool" → the process EXITS (bug_report C4).
foreach ($v in 'UDB_MSSQL_DSN','UDB_MYSQL_DSN','UDB_CASSANDRA_DSN','UDB_NEO4J_DSN','UDB_NEO4J_URI','NEO4J_URI') { Remove-Item "Env:$v" -ErrorAction SilentlyContinue }
# Single-process bench broker (no restart loop) on MY ports/DB, mirroring mars's
# proven launch-verify env (ALLOW_DEGRADED_BACKENDS so neo4j/qdrant degrade, not crash).
$env:UDB_GRPC_ADDR        = "0.0.0.0:50071"
$env:UDB_GRPC_TARGET      = "127.0.0.1:50071"
$env:UDB_AUTH_GRPC_ADDR   = "0.0.0.0:50081"
$env:UDB_AUTH_GRPC_TARGET = "127.0.0.1:50081"
$env:UDB_WEBRTC_GRPC_ADDR = "127.0.0.1:50091"
$env:UDB_METRICS_ADDR     = "127.0.0.1:19093"

$env:UDB_PG_DSN        = "postgres://udb:udb@127.0.0.1:55460/udb_bench_local"
$env:UDB_PG_MAX_CONNECTIONS = "40"
$env:UDB_PG_MIN_CONNECTIONS = "4"
$env:UDB_PG_ACQUIRE_TIMEOUT = "60"
$env:UDB_NOSQL_DSN     = "mongodb://127.0.0.1:57017/udb_bench_local?directConnection=true"
$env:UDB_MONGODB_DSN   = "mongodb://127.0.0.1:57017/udb_bench_local?directConnection=true"
$env:UDB_NOSQL_DATABASE = "udb_bench_local"
$env:UDB_MINIO_ENDPOINT = "http://127.0.0.1:59000"
$env:UDB_MINIO_ACCESS_KEY = "minio"
$env:UDB_MINIO_SECRET_KEY = "minio123"
$env:UDB_MINIO_REGION  = "us-east-1"
$env:UDB_KAFKA_BROKERS = "127.0.0.1:59192"
$env:UDB_REDIS_DSN     = "redis://127.0.0.1:56379"
$env:UDB_QDRANT_URL    = "http://127.0.0.1:56333"
$env:UDB_COLUMN_DSN    = "http://udb:udb@127.0.0.1:58123/udb"
$env:UDB_CLICKHOUSE_DSN = "http://udb:udb@127.0.0.1:58123/udb"
# Neo4j graph executor (GraphQuery/GraphMutate). Runtime reads UDB_GRAPH_* (the
# UDB_NEO4J_DSN cleared above is a DIFFERENT name); creds match canonical compose.
$env:UDB_GRAPH_DSN      = "http://127.0.0.1:57474"
$env:UDB_GRAPH_HTTP_URL = "http://127.0.0.1:57474"
$env:UDB_GRAPH_USER     = "neo4j"
$env:UDB_GRAPH_PASSWORD = "Udb_Strong#2026"
$env:UDB_GRAPH_DATABASE = "neo4j"

$env:UDB_ABAC_DEFAULT_ALLOW         = "true"
$env:UDB_ALLOW_DEGRADED_BACKENDS    = "true"
# EnsureBaseline (DataBroker admin baseline seed) is guarded; the sweep exercises it.
$env:UDB_ENABLE_ADMIN_SEED          = "1"
# Notification retry perf uses a served, env-gated FAILED-log path; keep it off
# outside harness launches.
$env:UDB_NOTIFICATION_TEST_MODE     = "1"
$env:UDB_VAULT_DB_ROLES_JSON        = '[{"role_name":"readonly","parent_role":"udb","ttl_seconds_max":900}]'
$env:UDB_CDC_ENABLED                = "true"   # PublishCDC needs the CDC tailer; §N abort was the kill-by-name artifact (harness_correction.md)
$env:UDB_PROJECTION_WORKER_ENABLED  = "false"
$env:UDB_RECONCILIATION_ENABLED     = "false"
$env:UDB_SAGA_RECOVERY_ENABLED      = "false"
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
# WebAuthn dev soft-authenticator (binary built --features webauthn): TEST_MODE makes
# the broker mint+verify a real credential from the "__UDB_WEBAUTHN_TEST__" sentinel.
$env:UDB_WEBAUTHN_TEST_MODE        = "1"
$env:UDB_WEBAUTHN_RP_ID            = "localhost"
$env:UDB_WEBAUTHN_ORIGIN           = "http://localhost"
$env:UDB_WEBAUTHN_RP_NAME          = "UDB Perf"
# SAML dev self-asserted IdP (accepts the "__UDB_SAML_TEST__" sentinel saml_response).
$env:UDB_SAML_TEST_MODE            = "1"
# OTP dev-echo + no cooldown → SendOTP/ResendOTP/VerifyOTP measurable (no rate-limit).
$env:UDB_OTP_DEV_ECHO              = "1"
$env:UDB_OTP_COOLDOWN_SECONDS      = "0"
# Object backend config so StorageService.object_exists HEADs where PutObject writes.
$env:UDB_OBJECT_BACKEND            = "minio"
$env:UDB_OBJECT_BUCKET             = "udb-storage"
# PHP lane runs the client IN DOCKER, where the presigned PUT (SigV4-bound to the broker's
# minio host) can't be reached/rewritten, so the seed places storage object bytes via the
# broker's PutObject into udb-live-sdk. The StorageService resolves its object bucket from
# UDB_STORAGE_BUCKET / UDB_STORAGE_OBJECT_BACKEND (NOT UDB_OBJECT_BUCKET — that's the
# data-plane object module), so point it at the same bucket the bytes land in, else
# FinalizeUpload HEADs udb-storage and reports "object not present". Go/Python run natively
# and use the presigned path against the default udb-storage bucket.
$env:UDB_STORAGE_OBJECT_BACKEND    = "minio"
$env:UDB_STORAGE_BUCKET            = "udb-live-sdk"

. (Join-Path $PSScriptRoot "bench-admission-headroom.ps1")

# Capture the crash: full backtrace + DIRECT unbuffered file redirect (NOT Tee, which
# buffers and loses the panic line before a panic=abort). bug_report.md C1/§H.
$env:RUST_BACKTRACE = "full"
$env:RUST_LIB_BACKTRACE = "full"
# OS-LEVEL file redirection via Start-Process (NOT the PowerShell `&`/`|` pipeline,
# which buffers the broker's stdout and crashes it mid-run — confirmed: direct
# redirection serves on all 3 ports with no crash). The child inherits this env.
$p = Start-Process -FilePath $UdbBenchBin `
    -ArgumentList 'serve', 'proto', '""', '0.0.0.0:50071' `
    -RedirectStandardOutput "E:\Projects\udb\.bench-local\once-broker.log" `
    -RedirectStandardError "E:\Projects\udb\.bench-local\once-stderr.log" `
    -NoNewWindow -PassThru
$p.WaitForExit()
"=== broker EXITED $(Get-Date -Format o) exit=$($p.ExitCode) ===" | Out-File -FilePath "E:\Projects\udb\.bench-local\once-broker.log" -Append
