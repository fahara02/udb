$ErrorActionPreference = "Stop"
Set-Location "E:\Projects\udb"

# Bench broker for bug-fix verification — single process (no restart loop, so a
# C1-style mid-run exit is observable), fresh udb_bench_local, explicit env (no
# UDB_MYSQL_DSN -> avoids the canonical-store MySQL panic), full DDL sync.
$env:UDB_GRPC_ADDR        = "0.0.0.0:51071"
$env:UDB_GRPC_TARGET      = "127.0.0.1:51071"
$env:UDB_AUTH_GRPC_ADDR   = "0.0.0.0:51081"
$env:UDB_AUTH_GRPC_TARGET = "127.0.0.1:51081"
$env:UDB_WEBRTC_GRPC_ADDR = "127.0.0.1:51091"
$env:UDB_METRICS_ADDR     = "127.0.0.1:19094"

$env:UDB_PG_DSN        = "postgres://udb:udb@127.0.0.1:55460/udb_perf_grind"
$env:UDB_PG_MAX_CONNECTIONS = "40"
$env:UDB_PG_MIN_CONNECTIONS = "4"
$env:UDB_PG_ACQUIRE_TIMEOUT = "60"
$env:UDB_NOSQL_DSN     = "mongodb://127.0.0.1:57017/udb_perf_grind?directConnection=true"
$env:UDB_MONGODB_DSN   = "mongodb://127.0.0.1:57017/udb_perf_grind?directConnection=true"
$env:UDB_NOSQL_DATABASE = "udb_perf_grind"
$env:UDB_MINIO_ENDPOINT = "http://127.0.0.1:59000"
$env:UDB_MINIO_ACCESS_KEY = "minio"
$env:UDB_MINIO_SECRET_KEY = "minio123"
$env:UDB_MINIO_REGION  = "us-east-1"
$env:UDB_KAFKA_BROKERS = "127.0.0.1:59192"
$env:UDB_REDIS_DSN     = "redis://127.0.0.1:56379"
$env:UDB_QDRANT_URL    = "http://127.0.0.1:56333"
# Neo4j graph executor (GraphQuery/GraphMutate). The runtime executor reads the
# UDB_GRAPH_* names (HTTP tx API at :57474); password has no default, so without
# these the executor fails to register → "backend executor 'neo4j:default' is not
# registered". Creds match docker-compose.canonical.yml NEO4J_AUTH.
$env:UDB_GRAPH_DSN      = "http://127.0.0.1:57474"
$env:UDB_GRAPH_HTTP_URL = "http://127.0.0.1:57474"
$env:UDB_GRAPH_USER     = "neo4j"
$env:UDB_GRAPH_PASSWORD = "Udb_Strong#2026"
$env:UDB_GRAPH_DATABASE = "neo4j"

$env:UDB_ABAC_DEFAULT_ALLOW         = "true"
$env:UDB_ALLOW_DEGRADED_BACKENDS    = "true"
# EnsureBaseline (DataBroker admin baseline seed) is guarded behind this flag and
# fails closed when unset; the perf sweep exercises it, so enable it here.
$env:UDB_ENABLE_ADMIN_SEED          = "1"
# CDC ON: the earlier "abort under CDC burst" was a harness kill-by-name artifact, not a
# broker crash (harness_correction.md). PublishCDC needs the CDC tailer configured
# (UDB_KAFKA_BROKERS below); enable it so PublishCDC runs its real publish path.
$env:UDB_CDC_ENABLED                = "true"
$env:UDB_PROJECTION_WORKER_ENABLED  = "false"
$env:UDB_RECONCILIATION_ENABLED     = "false"
$env:UDB_SAGA_RECOVERY_ENABLED      = "false"
$env:UDB_STARTUP_FORCE_SYNC         = "true"
$env:UDB_STARTUP_DRY_RUN            = "false"
# Disable the stale-peer / storage-orphan reapers: the perf seed joins a WebRTC peer
# via a unary call (no live signaling stream to heartbeat), so the 60s reaper would
# DISCONNECT it before measurement → "peer is not an active member" on PublishTrack/
# MuteTrack/Signal/IssueCredentials. 0 = reaper off, peer stays CONNECTED for the run.
$env:UDB_WEBRTC_REAP_INTERVAL_SECS  = "0"
$env:UDB_STORAGE_REAP_INTERVAL_SECS = "0"
# WebAuthn: StartWebAuthnRegistration requires RP id + origin (authn/mod.rs).
$env:UDB_WEBAUTHN_RP_ID  = "localhost"
$env:UDB_WEBAUTHN_ORIGIN = "http://localhost"
# Dev test-mode authenticators (Q#9-13): the harness sends sentinels and the broker mints
# REAL credentials/assertions verified through the production path. Fail-closed off by default.
$env:UDB_WEBAUTHN_TEST_MODE = "1"
$env:UDB_SAML_TEST_MODE     = "1"
# Dev OTP echo: SendOTP/IssueMfaChallenge return the plaintext code in the response
# (dev_otp_code) so VerifyOTP/ResetPassword/VerifyMfaChallenge can complete (authn/mfa.rs:208).
$env:UDB_OTP_DEV_ECHO = "1"
# OTP cooldown off (default 60s, per-(user,type)) so measured SendOTP/ResendOTP and the
# seeded reset OTP don't race the cooldown (authn/mod.rs:188, mfa.rs:122 → 0 disables).
$env:UDB_OTP_COOLDOWN_SECONDS = "0"
# Storage object plane: FinalizeUpload HEADs the object via UDB_OBJECT_BACKEND/BUCKET.
# The seed PUTs bytes through the minio object store, so point the object backend at minio
# (default is s3) and the bucket at udb-storage so the HEAD finds the seeded object.
$env:UDB_OBJECT_BACKEND = "minio"
$env:UDB_OBJECT_BUCKET  = "udb-storage"
$env:UDB_AUDIT_SINK_URL             = "file:///E:/Projects/udb/.bench-local/audit.jsonl"
$env:UDB_ENCRYPTION_KEY             = "QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI="
$env:UDB_JWT_PRIVATE_KEY            = "src/runtime/testdata/jwt_rs256_private.pem"
$env:UDB_JWT_PUBLIC_KEY             = "src/runtime/testdata/jwt_rs256_public.pem"
$env:UDB_SESSION_ENABLED           = "true"
$env:UDB_SESSION_HASH_SECRET       = "ci-bench-session-hash-secret"
$env:UDB_PASSWORD_HASH_SECRET      = "ci-bench-password-hash-secret"

# IMPORTANT: do NOT pipe the broker through Tee-Object — under the startup log burst the
# broker aborts (exitcode=-1) when a stdout write to the stalled pipe fails (bug_report.md
# §M). Use Start-Process with direct FILE redirection (stdout → verify-broker.log, which
# carries the "UDB DataBroker is ready" marker the grind waits on).
"=== broker start $(Get-Date -Format o) ===" | Out-File -FilePath "E:\Projects\udb\.bench-local\verify-broker.err.log"
$broker = Start-Process -FilePath "E:\Projects\udb\target-verify\debug\udb.exe" `
    -ArgumentList 'serve proto "" 0.0.0.0:51071' `
    -WorkingDirectory "E:\Projects\udb" `
    -RedirectStandardOutput "E:\Projects\udb\.bench-local\verify-broker.log" `
    -RedirectStandardError  "E:\Projects\udb\.bench-local\verify-broker.err.log" `
    -PassThru -NoNewWindow
Set-Content -Path "E:\Projects\udb\.bench-local\verify-broker.pid" -Value $broker.Id
$broker.WaitForExit()
$hex = '0x{0:X8}' -f $broker.ExitCode
"=== broker EXITED $(Get-Date -Format o) exitcode=$($broker.ExitCode) ntstatus=$hex ===" | Out-File -FilePath "E:\Projects\udb\.bench-local\verify-broker.log" -Append
Remove-Item -Path "E:\Projects\udb\.bench-local\verify-broker.pid" -ErrorAction SilentlyContinue
