#!/usr/bin/env python3
"""Source guard for UDB proof-workflow posture.

The repository now carries several workflow_dispatch smoke/proof workflows that
operators run to close masterplan live-proof tails. Syntax lint is not enough:
Docker-backed proof workflows must retain diagnostics + teardown, and Pages must
remain single-owned by pages.yml.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

PROOF_WORKFLOWS = (
    "branch-protection-audit.yml",
    "clickhouse-canonical-smoke.yml",
    "error-detail-served-smoke.yml",
    "ffmpeg-transcode-smoke.yml",
    "ha-smokes.yml",
    "idempotency-served-smoke.yml",
    "metering-smoke.yml",
    "pg-merge-smoke.yml",
    "rest-gateway-smoke.yml",
    "retry-safe-served-smoke.yml",
    "runner-evidence-audit.yml",
    "secrets-posture-smoke.yml",
    "sfu-smoke.yml",
    "sidecar-smokes.yml",
    "webauthn-smoke.yml",
)

DOCKER_PROOF_WORKFLOWS = {
    "clickhouse-canonical-smoke.yml",
    "ha-smokes.yml",
    "metering-smoke.yml",
    "pg-merge-smoke.yml",
    "sfu-smoke.yml",
    "sidecar-smokes.yml",
}

ARTIFACT_PROOF_WORKFLOWS = DOCKER_PROOF_WORKFLOWS | {
    "ffmpeg-transcode-smoke.yml",
}

RESILIENCE_WORKFLOW_REQUIREMENTS = (
    ("schedule:", "weekly schedule"),
    ("cron:", "schedule cron"),
    ("ha-smokes:", "HA smoke job"),
    ("cdc-fault-smoke:", "CDC fault smoke job"),
    ("bash scripts/ha_multinode_smoke.sh", "lease failover smoke script"),
    ("bash scripts/ha_cdc_no_duplicate_smoke.sh", "CDC no-duplicate smoke script"),
    ("bash scripts/ha_xa_recovery_smoke.sh", "XA recovery smoke script"),
    ("bash scripts/cdc_fault_smoke.sh", "CDC fault smoke script"),
    ("UDB_HA_PROJECT", "HA lease project env"),
    ("UDB_HA_CDC_PROJECT", "HA CDC project env"),
    ("UDB_HA_XA_PROJECT", "HA XA project env"),
    ("UDB_CDC_FAULT_PROJECT", "CDC fault project env"),
    ("UDB_CDC_FAULT_KEEP_STACK", "CDC fault keep-stack env"),
    ("ha-smoke-logs", "HA diagnostics artifact"),
    ("cdc-fault-smoke-logs", "CDC fault diagnostics artifact"),
    ('docker compose -p "$UDB_HA_PROJECT"', "HA lease stack compose scope"),
    ('docker compose -p "$UDB_HA_CDC_PROJECT"', "HA CDC stack compose scope"),
    ('docker compose -p "$UDB_HA_XA_PROJECT"', "HA XA stack compose scope"),
    ('docker compose -p "$UDB_CDC_FAULT_PROJECT"', "CDC fault stack compose scope"),
)

XA_RECOVERY_SCRIPT_REQUIREMENTS = (
    ("KILL_SERVICE", "kill-service selector"),
    ("SURVIVOR_SERVICE", "survivor-service selector"),
    ('compose --profile broker-xa-ha up -d --build', "two-broker XA stack startup"),
    ("postgres redis kafka qdrant minio mysql", "shared dependency stack"),
    ('compose kill -s KILL "$KILL_SERVICE"', "broker hard-kill action"),
    ('assert_service_stopped "$KILL_SERVICE"', "killed broker remains stopped assertion"),
    (
        'assert_service_running_container "$SURVIVOR_SERVICE" "$SURVIVOR_CID"',
        "original survivor keeps running assertion",
    ),
    ("XA START '${XID}';", "MySQL XA start"),
    ("XA PREPARE '${XID}';", "MySQL XA prepare"),
    ("INSERT INTO udb_system.udb_xa_ledger", "UDB XA ledger seed"),
    ("'in_doubt'", "in-doubt ledger decision"),
    ("commit decided; phase 2 in flight", "commit-intent ledger reason"),
    ("SELECT decision FROM udb_system.udb_xa_ledger WHERE xid = '${XID}';", "ledger recovery query"),
    ('"committed"', "committed ledger expectation"),
    ('mysql_exec "$MYSQL_DB" "XA RECOVER;"', "prepared-XA absence check"),
    ("grep -Fq \"$XID\"", "prepared-XA exact xid grep"),
    ("DROP DATABASE IF EXISTS \\`udb_xa_mysql_${suffix}\\`", "MySQL cleanup"),
    ("DROP SCHEMA IF EXISTS udb_xa_pg_${suffix} CASCADE", "Postgres cleanup"),
    ('compose down -v --remove-orphans', "stack teardown"),
)

SIDECAR_WORKFLOW_REQUIREMENTS = (
    ("embedding-sidecar:", "embedding sidecar job"),
    ("notify-sidecar:", "notification sidecar job"),
    ("UDB_EMBEDDING_PROJECT", "embedding project env"),
    ("UDB_NOTIFY_PROJECT", "notification project env"),
    ("python scripts/embedding_sidecar_roundtrip_smoke.py --selftest", "embedding round-trip selftest"),
    ("python scripts/notify_sidecar_roundtrip_smoke.py --selftest", "notification round-trip selftest"),
    ("python scripts/embedding_sidecar_smoke.py --selftest", "embedding sidecar smoke selftest"),
    ("python scripts/embedding_retrieval_eval.py", "embedding retrieval golden-set evaluation"),
    ("python scripts/notify_sidecar_smoke.py --selftest", "notification sidecar smoke selftest"),
    ("--profile embedding", "embedding compose profile"),
    ("--profile notify", "notification compose profile"),
    ("up -d --wait embedding-sidecar", "embedding sidecar startup"),
    ("up -d --wait notify-sidecar", "notification sidecar startup"),
    ("python scripts/embedding_sidecar_smoke.py --url http://127.0.0.1:58090", "embedding sidecar smoke URL"),
    ("python scripts/notify_sidecar_smoke.py --url http://127.0.0.1:58080", "notification sidecar smoke URL"),
    ("embedding-sidecar-smoke-logs", "embedding diagnostics artifact"),
    ("notify-sidecar-smoke-logs", "notification diagnostics artifact"),
    ('docker compose -p "$UDB_EMBEDDING_PROJECT"', "embedding stack compose scope"),
    ('docker compose -p "$UDB_NOTIFY_PROJECT"', "notification stack compose scope"),
)

EMBEDDING_ROUNDTRIP_SCRIPT_REQUIREMENTS = (
    ("TOPIC_WORK = \"udb.embedding.work.v1\"", "embedding work topic"),
    (
        "REPORT_METHOD = \"udb.core.embedding.services.v1.EmbeddingService/ReportEmbedding\"",
        "ReportEmbedding callback method",
    ),
    ("FORBIDDEN_WORK_KEYS", "credential-key denylist"),
    ("check_no_credentials(value)", "credential-key validation call"),
    ("load_work_from_postgres", "durable work loader"),
    ("udb_system.outbox_events", "outbox default relation"),
    ("udb_system.udb_cdc_event_journal", "CDC journal default relation"),
    ("CALLBACK_PROTO = \"proto/udb/core/embedding/services/v1/embedding_service.proto\"", "ReportEmbedding checked-in proto"),
    ("DEFAULT_PROTO_IMPORT_PATHS = (\"proto\", \"third_party/googleapis\")", "ReportEmbedding proto import paths"),
    ("--use-reflection", "ReportEmbedding reflection opt-in"),
    ("cmd.extend([\"-proto\", str((ROOT / args.proto).resolve())])", "ReportEmbedding proto-mode grpcurl"),
    ("sidecar_embed(args.sidecar_url, work)", "sidecar embed call"),
    ("call_report_embedding(args, report, work[\"tenant_id\"], args.project_id)", "ReportEmbedding callback call"),
    ("x-udb-scopes: udb:embedding:report-embedding", "ReportEmbedding scope metadata"),
    ("authorization: Bearer REDACTED", "redacted bearer command output"),
    ("payload.get(\"upserted\") is not True", "ReportEmbedding upsert assertion"),
)

NOTIFY_ROUNDTRIP_SCRIPT_REQUIREMENTS = (
    (
        "REPORT_METHOD = \"udb.core.notification.services.v1.NotificationService/ReportDelivery\"",
        "ReportDelivery callback method",
    ),
    ("load_intent_from_postgres", "durable notification intent loader"),
    ("udb_notification.notification_logs", "notification log default relation"),
    ("udb_notification.notification_delivery_attempts", "delivery attempt default relation"),
    ("CALLBACK_PROTO = \"proto/udb/core/notification/services/v1/notification_service.proto\"", "ReportDelivery checked-in proto"),
    ("DEFAULT_PROTO_IMPORT_PATHS = (\"proto\", \"third_party/googleapis\")", "ReportDelivery proto import paths"),
    ("--use-reflection", "ReportDelivery reflection opt-in"),
    ("cmd.extend([\"-proto\", str((ROOT / args.proto).resolve())])", "ReportDelivery proto-mode grpcurl"),
    ("sidecar_send(args.sidecar_url, intent, args.provider_credential)", "sidecar send call"),
    ("call_report_delivery(args, report, intent[\"tenant_id\"], args.project_id)", "ReportDelivery callback call"),
    ("x-udb-scopes: udb:notification:report-delivery", "ReportDelivery scope metadata"),
    ("authorization: Bearer REDACTED", "redacted bearer command output"),
    ("\"attempt\" not in payload", "ReportDelivery attempt assertion"),
    ("provider_message_id", "provider message id propagation"),
)

SIDECAR_CONTAINER_SOURCE_REQUIREMENTS = {
    "sidecars/embedding/Dockerfile": (
        ("FROM python:3.12-alpine", "embedding Python base"),
        ("COPY embedding_sidecar.py /app/embedding_sidecar.py", "embedding app copy"),
        ("ENV UDB_EMBED_PROVIDER=deterministic", "embedding deterministic provider default"),
        ("ENV UDB_EMBED_DIMS=16", "embedding deterministic dimensions"),
        ("HEALTHCHECK", "embedding Docker healthcheck"),
        ('CMD ["python", "/app/embedding_sidecar.py"]', "embedding sidecar command"),
    ),
    "sidecars/embedding/embedding_sidecar.py": (
        ("FORBIDDEN_WORK_KEYS", "embedding credential denylist"),
        ("check_no_credentials(value)", "embedding recursive credential check"),
        ("def parse_work", "embedding work parser"),
        ("def embed_work", "embedding provider seam"),
        ("def resolve_vault_reference", "embedding Vault resolver"),
        ('work.provider not in {"openai", "openai-compatible", "azure-openai"}', "embedding unsupported-provider fail closed"),
        ('self.path == "/healthz"', "embedding health endpoint"),
        ('"/embed-batch", "/v1/embed-batch", "/rerank", "/v1/rerank", "/parse", "/v1/parse"', "embedding advanced endpoint allowlist"),
        ('"status": "embedded"', "embedding status response"),
        ('"report_embedding_request": report', "ReportEmbedding payload response"),
        ('"vector": embed_work(work)', "embedding vector construction"),
        ('"report_embedding_batch_request"', "ReportEmbeddingBatch payload response"),
        ('"report_embedding_failure_request"', "ReportEmbeddingFailure payload response"),
    ),
    "sidecars/notify/Dockerfile": (
        ("FROM python:3.12-alpine", "notification Python base"),
        ("COPY notify_sidecar.py /app/notify_sidecar.py", "notification app copy"),
        ("HEALTHCHECK", "notification Docker healthcheck"),
        ('CMD ["python", "/app/notify_sidecar.py"]', "notification sidecar command"),
    ),
    "sidecars/notify/notify_sidecar.py": (
        ("def credential_from_authorization", "notification bearer credential extraction"),
        ('Authorization: Bearer <provider credential> is required', "notification bearer requirement"),
        ("def dry_run", "notification dry-run provider"),
        ('provider == "smtp"', "SMTP provider route"),
        ('provider == "ses"', "SES provider route"),
        ('provider == "twilio"', "Twilio provider route"),
        ('provider == "fcm"', "FCM provider route"),
        ('self.path == "/healthz"', "notification health endpoint"),
        ('self.path not in {"/send", "/v1/send"}', "notification endpoint allowlist"),
        ('"provider_message_id": result.provider_message_id', "provider message id body"),
        ('"x-provider-message-id": result.provider_message_id', "provider message id header"),
    ),
}

INTEGRATION_COMPOSE_PROFILE_REQUIREMENTS = (
    ("udb-livekit:", "LiveKit broker service"),
    ("livekit:", "LiveKit dev service"),
    ("coturn:", "coturn TURN service"),
    ("profiles: [\"sfu\"]", "SFU compose profile"),
    ("UDB_LIVEKIT_URL: ws://livekit:7880", "broker LiveKit internal URL"),
    ("UDB_LIVEKIT_API_KEY: devkey", "broker LiveKit dev key"),
    ("UDB_LIVEKIT_API_SECRET: secret", "broker LiveKit dev secret"),
    ("UDB_LIVEKIT_ALLOW_INSECURE: \"1\"", "explicit local insecure LiveKit opt-in"),
    ("UDB_SESSION_ENABLED: \"true\"", "SFU local sessions enabled"),
    ("UDB_SESSION_HASH_SECRET: local-sfu-session-secret", "SFU local session secret"),
    ("UDB_PASSWORD_HASH_SECRET: local-sfu-password-secret", "SFU local password secret"),
    ("UDB_JWT_PRIVATE_KEY: src/runtime/testdata/jwt_rs256_private.pem", "SFU local JWT private key"),
    ("UDB_JWT_PUBLIC_KEY: src/runtime/testdata/jwt_rs256_public.pem", "SFU local JWT public key"),
    ("UDB_TURN_SECRET: local-turn-secret", "broker TURN shared secret"),
    ("\"50081:50051\"", "SFU broker public gRPC host port"),
    ("\"50082:50052\"", "SFU broker control gRPC host port"),
    ("\"57880:7880\"", "LiveKit HTTP host port"),
    ("\"53478:3478/udp\"", "coturn UDP host port"),
    ("--static-auth-secret=local-turn-secret", "coturn shared secret"),
    ("notify-sidecar:", "notification sidecar service"),
    ("profiles: [\"notify\"]", "notification compose profile"),
    ("context: ./sidecars/notify", "notification sidecar build context"),
    ("UDB_NOTIFY_PROVIDER: smtp", "notification sidecar provider"),
    ("UDB_NOTIFY_DRY_RUN: \"1\"", "notification sidecar dry-run mode"),
    ("\"58080:8080\"", "notification sidecar host port"),
    ("embedding-sidecar:", "embedding sidecar service"),
    ("profiles: [\"embedding\"]", "embedding compose profile"),
    ("context: ./sidecars/embedding", "embedding sidecar build context"),
    ("UDB_EMBED_PROVIDER: deterministic", "embedding sidecar deterministic provider"),
    ("UDB_EMBED_DIMS: \"16\"", "embedding sidecar deterministic dims"),
    ("\"58090:8080\"", "embedding sidecar host port"),
    ("healthz", "sidecar healthcheck path"),
)

TARGETED_PROOF_WORKFLOW_REQUIREMENTS = {
    "branch-protection-audit.yml": (
        ("branch-protection-lockstep:", "branch-protection audit job"),
        ("branch:", "protected branch workflow input"),
        ("node --check scripts/check-branch-protection-lockstep.mjs", "branch-protection audit syntax check"),
        ("node scripts/check-branch-protection-lockstep.mjs --selftest", "branch-protection audit selftest"),
        ("GH_TOKEN: ${{ secrets.BRANCH_PROTECTION_TOKEN || github.token }}", "branch-protection token fallback"),
        ("--repo \"${GITHUB_REPOSITORY}\"", "repository handoff"),
        ("--branch \"${BRANCH_NAME}\"", "branch handoff"),
    ),
    "clickhouse-canonical-smoke.yml": (
        ("clickhouse-canonical:", "ClickHouse canonical job"),
        ("cache-key: clickhouse-canonical-smoke", "ClickHouse Rust cache key"),
        ("docker-compose.canonical.yml up -d --wait clickhouse", "Keeper-enabled ClickHouse startup"),
        ("UDB_COLUMN_DSN", "ClickHouse column DSN"),
        ("UDB_CLICKHOUSE_DSN", "ClickHouse native DSN"),
        ("cargo test --locked --lib --features clickhouse", "ClickHouse feature test command"),
        (
            "clickhouse_canonical_store_satisfies_all_contracts_live",
            "ClickHouse canonical live contract",
        ),
        ("clickhouse-canonical-logs", "ClickHouse diagnostics artifact"),
        ("docker-compose.canonical.yml down -v --remove-orphans", "ClickHouse teardown"),
    ),
    "ffmpeg-transcode-smoke.yml": (
        ("ffmpeg-transcode-smoke:", "ffmpeg transcode job"),
        ("python scripts/check-vendored-ffmpeg.py --selftest", "vendored ffmpeg verifier selftest"),
        ("sudo apt-get install -y --no-install-recommends ffmpeg", "ffmpeg package install"),
        ('python scripts/ffmpeg_transcode_smoke.py --ffmpeg-bin "$(command -v ffmpeg)" --artifact-dir ffmpeg-transcode-smoke', "ffmpeg transcode smoke"),
        ("ffmpeg-transcode-smoke", "ffmpeg artifact name"),
    ),
    "error-detail-served-smoke.yml": (
        ("error-detail-served:", "ErrorDetail served proof job"),
        ("release_tag:", "served release-tag input"),
        ("release_asset:", "served release-asset input"),
        ("broker_artifact_run_id:", "current broker artifact input"),
        ("actions: read", "served binary artifact read permission"),
        ("postgres:", "served Postgres service"),
        ("mongodb:", "served MongoDB service"),
        ("uses: ./.github/actions/resolve-served-binary", "shared served binary resolver"),
        ("broker-artifact-run-id: ${{ inputs.broker_artifact_run_id }}", "current broker artifact handoff"),
        ("uses: ./.github/actions/start-backends", "served backend action reuse"),
        ('clickhouse: "false"', "served proof skips unsupported ClickHouse for slim artifacts"),
        ('neo4j: "false"', "served proof skips unsupported Neo4j for slim artifacts"),
        ("uses: ./.github/actions/broker-env", "served broker env reuse"),
        ('enable_column_backend: "false"', "served proof skips ClickHouse env export"),
        ('enable_graph_backend: "false"', "served proof skips Neo4j env export"),
        ("UDB_OTP_COOLDOWN_SECONDS=60", "ErrorDetail quota cooldown override"),
        ("uses: ./.github/actions/launch-broker", "served broker launch action reuse"),
        ("Bootstrap served-smoke user", "served smoke user bootstrap"),
        ("scripts/write_error_detail_served_smoke_inputs.py", "ErrorDetail proof input generator"),
        ("python -m pip install -e sdk/python", "Python SDK runtime install"),
        ("python scripts/error_detail_served_smoke.py --selftest", "ErrorDetail smoke selftest"),
        ("--require-all-proofs", "complete Chapter 14.7 proof gate"),
        ('done < smoke-input/header.txt', "generated metadata handoff"),
        ("--target \"${UDB_AUTH_GRPC_TARGET}\"", "auth listener target handoff"),
        ("--validation-method /udb.core.authn.services.v1.AuthnService/SendPhoneVerification", "Authn validation method"),
        ("--validation-request-module udb.core.authn.services.v1.core_pb2", "Authn validation request module"),
        ("--validation-request-message SendPhoneVerificationRequest", "Authn validation request message"),
        ("--validation-request-json smoke-input/validation.json", "validation JSON handoff"),
        ("--validation-field phone", "validation field handoff"),
        ("--quota-method /udb.core.authn.services.v1.AuthnService/SendOTP", "Authn quota method"),
        ("--quota-request-module udb.core.authn.services.v1.core_pb2", "Authn quota request module"),
        ("--quota-request-message SendOTPRequest", "Authn quota request message"),
        ("--quota-request-json smoke-input/quota.json", "quota JSON handoff"),
        ("--quota-retry-after-min-ms 1000", "quota retry-after handoff"),
        ("--quota-backend authn", "quota backend handoff"),
        ("--quota-operation otp_cooldown", "quota operation handoff"),
        ("error-detail-served-smoke-diagnostics", "ErrorDetail diagnostics artifact"),
    ),
    "idempotency-served-smoke.yml": (
        ("idempotency-served-replay:", "idempotency served replay job"),
        ("release_tag:", "served release-tag input"),
        ("release_asset:", "served release-asset input"),
        ("broker_artifact_run_id:", "current broker artifact input"),
        ("actions: read", "served binary artifact read permission"),
        ("postgres:", "served Postgres service"),
        ("mongodb:", "served MongoDB service"),
        ("uses: ./.github/actions/resolve-served-binary", "shared served binary resolver"),
        ("broker-artifact-run-id: ${{ inputs.broker_artifact_run_id }}", "current broker artifact handoff"),
        ("uses: ./.github/actions/start-backends", "served backend action reuse"),
        ('clickhouse: "false"', "served proof skips unsupported ClickHouse for slim artifacts"),
        ('neo4j: "false"', "served proof skips unsupported Neo4j for slim artifacts"),
        ("uses: ./.github/actions/broker-env", "served broker env reuse"),
        ('enable_column_backend: "false"', "served proof skips ClickHouse env export"),
        ('enable_graph_backend: "false"', "served proof skips Neo4j env export"),
        ("uses: ./.github/actions/launch-broker", "served broker launch action reuse"),
        ("Bootstrap served-smoke users", "served smoke user bootstrap"),
        ("scripts/write_databroker_served_smoke_inputs.py", "served proof input generator"),
        ("--tenant2-username", "tenant2 auth fixture generation"),
        ("UDB_TENANT2_PROJECT", "tenant2 project fixture generation"),
        ("--tenant2-project", "tenant2 project generator handoff"),
        ("python -m pip install -e sdk/python", "Python SDK runtime install"),
        ("python scripts/idempotency_served_replay_smoke.py --selftest", "idempotency served smoke selftest"),
        ("Run live idempotency replay proofs", "healthy dedup replay phase"),
        ("Run live idempotency fail-closed proof", "dedup-store-down proof phase"),
        ("ALTER TABLE IF EXISTS udb_system.udb_idempotency_keys RENAME TO", "dedup relation disablement"),
        ("Restore idempotency relation", "dedup relation restore"),
        ('done < smoke-input/header.txt', "generated baseline metadata handoff"),
        ('done < smoke-input/tenant2-header.txt', "generated tenant2 metadata handoff"),
        ("--upsert-json smoke-input/upsert.json", "Upsert replay handoff"),
        ("--tenant2-upsert-json smoke-input/tenant2-upsert.json", "tenant isolation handoff"),
        ("--batch-upsert-json smoke-input/batch-upsert.json", "BatchUpsert replay handoff"),
        ("--fail-closed-upsert-json smoke-input/fail-closed-upsert.json", "fail-closed handoff"),
        ("--fail-closed-select-json smoke-input/fail-closed-select.json", "fail-closed no-write Select handoff"),
        ("--keyless-upsert-json smoke-input/keyless-upsert.json", "keyless handoff"),
        ("idempotency-served-smoke-diagnostics", "idempotency diagnostics artifact"),
    ),
    "metering-smoke.yml": (
        ("metering-rollup-smoke:", "metering rollup job"),
        ("UDB_METERING_PROJECT", "metering project env"),
        ("cache-key: metering-rollup-smoke", "metering Rust cache key"),
        ("up -d --wait postgres", "metering Postgres startup"),
        ("UDB_INTEGRATION_PG_DSN", "metering integration DSN"),
        ("UDB_LIVE_NATIVE_PG_DSN", "metering native DSN"),
        (
            "cargo test --locked --lib live_postgres_metering_rollup_exports_closed_window_once -- --ignored --nocapture --test-threads=1",
            "metering rollup live oracle",
        ),
        ("metering-smoke-logs", "metering diagnostics artifact"),
        ('docker compose -p "$UDB_METERING_PROJECT"', "metering stack compose scope"),
        ("down -v --remove-orphans", "metering stack teardown"),
    ),
    "secrets-posture-smoke.yml": (
        ("ws-signalling-redaction:", "ws-signalling redaction job"),
        ("cache-key: secrets-posture-smoke", "secrets Rust cache key"),
        ("--features ws-signalling storage_only_fields_match_generated_redaction_coverage", "descriptor redaction coverage target"),
        ("--features ws-signalling ice_config_debug_redacts_turn_secret", "IceConfig redaction target"),
    ),
    "webauthn-smoke.yml": (
        ("webauthn-openssl-smoke:", "WebAuthn OpenSSL job"),
        ("cache-key: webauthn-openssl-smoke", "WebAuthn Rust cache key"),
        (
            "cargo test --locked --lib --features webauthn webauthn_policy_tests -- --nocapture",
            "WebAuthn policy/attestation test target",
        ),
    ),
    "pg-merge-smoke.yml": (
        ("pg-merge-smoke:", "Postgres planner/IR merge job"),
        ("UDB_PG_MERGE_PROJECT", "Postgres merge project env"),
        ("cache-key: pg-merge-smoke", "Postgres merge Rust cache key"),
        ("up -d --wait postgres", "Postgres merge startup"),
        ("UDB_IR_LIVE_GOLDEN_TESTS", "IR live golden env"),
        ("UDB_PG_DSN", "Postgres IR DSN"),
        ("DATABASE_URL", "Postgres database URL"),
        (
            "cargo test --locked --lib postgres_data_plane_planner_and_bridged_ir_match_live_rows -- --ignored --nocapture --test-threads=1",
            "Postgres planner/IR A-B live oracle",
        ),
        ("pg-merge-smoke-logs", "Postgres merge diagnostics artifact"),
        ('docker compose -p "$UDB_PG_MERGE_PROJECT"', "Postgres merge compose scope"),
        ("down -v --remove-orphans", "Postgres merge teardown"),
    ),
    "rest-gateway-smoke.yml": (
        ("rest-gateway-boundary:", "REST gateway boundary job"),
        ("base_url:", "REST gateway base URL input"),
        ("success_route:", "REST success route input"),
        ("error_route:", "REST error route input"),
        ("error_code:", "REST ApiError.code input"),
        ("header:", "REST optional header input"),
        ("timeout_seconds:", "REST timeout input"),
        ("python3 scripts/rest_route_gateway_smoke.py --selftest", "REST route smoke selftest"),
        ("python3 scripts/rest_route_gateway_smoke.py --check-openapi", "REST OpenAPI route check"),
        ("--base-url \"$BASE_URL\"", "REST live base URL handoff"),
        ("--require-route-family-proof", "REST live canonical/retired route-family proof gate"),
        ("--require-boundary-proof", "REST live success/error boundary proof gate"),
        ("--boundary-success \"$SUCCESS_ROUTE\"", "REST live success route handoff"),
        ("--boundary-error \"$ERROR_ROUTE\"", "REST live error route handoff"),
        ("--boundary-error-code \"$ERROR_CODE\"", "REST live error code handoff"),
        ("--timeout \"$TIMEOUT_SECONDS\"", "REST live timeout handoff"),
        ("--evidence-out rest-gateway-evidence/evidence.json", "REST evidence JSON handoff"),
        ("name: rest-gateway-evidence", "REST evidence artifact name"),
        ("path: rest-gateway-evidence/evidence.json", "REST evidence artifact path"),
        ('args+=(--header "$HEADER")', "REST optional header handoff"),
    ),
    "retry-safe-served-smoke.yml": (
        ("retry-safe-served:", "retry-safe served proof job"),
        ("release_tag:", "served release-tag input"),
        ("release_asset:", "served release-asset input"),
        ("broker_artifact_run_id:", "current broker artifact input"),
        ("actions: read", "served binary artifact read permission"),
        ("postgres:", "served Postgres service"),
        ("mongodb:", "served MongoDB service"),
        ("uses: ./.github/actions/resolve-served-binary", "shared served binary resolver"),
        ("broker-artifact-run-id: ${{ inputs.broker_artifact_run_id }}", "current broker artifact handoff"),
        ("uses: ./.github/actions/start-backends", "served backend action reuse"),
        ('clickhouse: "false"', "served proof skips unsupported ClickHouse for slim artifacts"),
        ('neo4j: "false"', "served proof skips unsupported Neo4j for slim artifacts"),
        ("uses: ./.github/actions/broker-env", "served broker env reuse"),
        ('enable_column_backend: "false"', "served proof skips ClickHouse env export"),
        ('enable_graph_backend: "false"', "served proof skips Neo4j env export"),
        ("uses: ./.github/actions/launch-broker", "served broker launch action reuse"),
        ("Bootstrap served-smoke users", "served smoke user bootstrap"),
        ("scripts/write_databroker_served_smoke_inputs.py", "served proof input generator"),
        ("UDB_TENANT2_PROJECT", "tenant2 project fixture generation"),
        ("--tenant2-project", "tenant2 project generator handoff"),
        ("python -m pip install -e sdk/python", "Python SDK runtime install"),
        ("python scripts/retry_safe_served_smoke.py --selftest", "retry-safe smoke selftest"),
        ("--require-all-proofs", "complete retry-safe Upsert/Delete proof gate"),
        ('done < smoke-input/header.txt', "generated metadata handoff"),
        ("--upsert-json smoke-input/retry-upsert.json", "Upsert replay handoff"),
        ("--delete-json smoke-input/retry-delete.json", "Delete replay handoff"),
        ("Retry-safe mutation metadata served proof", "served proof job name"),
        ("retry-safe-served-smoke-diagnostics", "retry-safe diagnostics artifact"),
    ),
    "runner-evidence-audit.yml": (
        ("runner-evidence:", "runner-evidence audit job"),
        ("branch:", "integration branch workflow input"),
        ("release_tag:", "release tag workflow input"),
        ("pr_run_id:", "PR run id workflow input"),
        ("integration_run_id:", "integration run id workflow input"),
        ("release_run_id:", "release run id workflow input"),
        ("release_dry_run_id:", "release dry-run run id workflow input"),
        ("benchmark_run_id:", "benchmark run id workflow input"),
        ("pages_run_id:", "Pages run id workflow input"),
        ("branch_protection_run_id:", "branch-protection run id workflow input"),
        ("lint_run_id:", "lint run id workflow input"),
        ("idempotency_served_run_id:", "idempotency served run id workflow input"),
        ("error_detail_served_run_id:", "ErrorDetail served run id workflow input"),
        ("retry_safe_served_run_id:", "retry-safe served run id workflow input"),
        ("rest_gateway_run_id:", "REST gateway run id workflow input"),
        ("max_evidence_age_days:", "runner evidence max-age workflow input"),
        ("actions: read", "Actions read permission"),
        ("node --check scripts/check-ci-runner-evidence.mjs", "runner-evidence syntax check"),
        ("node scripts/check-ci-runner-evidence.mjs --selftest", "runner-evidence selftest"),
        ("--all-evidence", "full central evidence audit mode handoff"),
        ("--pr-budget-minutes 8", "PR budget handoff"),
        ("--integration-budget-minutes 30", "integration budget handoff"),
        ("--release-budget-minutes 40", "release budget handoff"),
        ("--release-dry-run-budget-minutes 120", "release dry-run budget handoff"),
        ("--benchmark-budget-minutes 120", "benchmark budget handoff"),
        ("--pages-budget-minutes 20", "Pages budget handoff"),
        ("--branch-protection-budget-minutes 10", "branch-protection budget handoff"),
        ("--lint-budget-minutes 10", "lint budget handoff"),
        ("--idempotency-served-budget-minutes 15", "idempotency served budget handoff"),
        ("--error-detail-served-budget-minutes 15", "ErrorDetail served budget handoff"),
        ("--retry-safe-served-budget-minutes 15", "retry-safe served budget handoff"),
        ("--rest-gateway-budget-minutes 15", "REST gateway budget handoff"),
        ("--max-evidence-age-days \"${MAX_EVIDENCE_AGE_DAYS}\"", "runner evidence max-age handoff"),
        ("--pr-run-id \"$PR_RUN_ID\"", "PR run id handoff"),
        ("--integration-run-id \"$INTEGRATION_RUN_ID\"", "integration run id handoff"),
        ("--release-run-id \"$RELEASE_RUN_ID\"", "release run id handoff"),
        ("--release-dry-run-id \"$RELEASE_DRY_RUN_ID\"", "release dry-run run id handoff"),
        ("--benchmark-run-id \"$BENCHMARK_RUN_ID\"", "benchmark run id handoff"),
        ("--pages-run-id \"$PAGES_RUN_ID\"", "Pages run id handoff"),
        ("--branch-protection-run-id \"$BRANCH_PROTECTION_RUN_ID\"", "branch-protection run id handoff"),
        ("--lint-run-id \"$LINT_RUN_ID\"", "lint run id handoff"),
        ("--idempotency-run-id \"$IDEMPOTENCY_SERVED_RUN_ID\"", "idempotency served run id handoff"),
        ("--error-detail-run-id \"$ERROR_DETAIL_SERVED_RUN_ID\"", "ErrorDetail served run id handoff"),
        ("--retry-safe-run-id \"$RETRY_SAFE_SERVED_RUN_ID\"", "retry-safe served run id handoff"),
        ("--rest-gateway-run-id \"$REST_GATEWAY_RUN_ID\"", "REST gateway run id handoff"),
    ),
    "sfu-smoke.yml": (
        ("livekit-sfu-smoke:", "LiveKit SFU job"),
        ("UDB_SFU_PROJECT", "SFU project env"),
        ("UDB_SFU_OPERATOR_USERNAME", "SFU operator username env"),
        ("UDB_SFU_OPERATOR_PASSWORD", "SFU operator password env"),
        ("UDB_SFU_OPERATOR_TENANT", "SFU operator tenant env"),
        ("UDB_SFU_OPERATOR_PROJECT", "SFU operator project env"),
        ("cache-key: livekit-sfu-smoke", "SFU Rust cache key"),
        ("--features webrtc livekit_join_token_binds_tenant_room_and_peer", "LiveKit token binding canary"),
        ("--features webrtc plaintext_livekit_url_requires_explicit_local_opt_in", "LiveKit plaintext URL canary"),
        ("--features webrtc livekit_room_service_base_derives_http_endpoint", "LiveKit HTTP endpoint canary"),
        ("--features webrtc sfu_join_metadata_uses_public_header_contract", "SFU metadata canary"),
        ("--features webrtc signal_offer_uses_injected_sfu_bridge", "SFU bridge offer canary"),
        ('python -m pip install -e "sdk/python"', "Python SDK editable install"),
        ("python scripts/livekit_sfu_smoke.py --selftest", "LiveKit smoke harness selftest"),
        ("--profile sfu", "SFU compose profile"),
        ("postgres redis qdrant minio kafka livekit coturn udb-livekit", "SFU compose services"),
        ("Bootstrap LiveKit SFU operator", "SFU operator bootstrap step"),
        ("udb auth bootstrap user", "SFU operator bootstrap command"),
        ("--username \"$UDB_SFU_OPERATOR_USERNAME\"", "SFU operator bootstrap username"),
        ("--password \"$UDB_SFU_OPERATOR_PASSWORD\"", "SFU operator bootstrap password"),
        ("python scripts/livekit_sfu_smoke.py", "LiveKit served-path smoke script"),
        ("--broker 127.0.0.1:50082", "LiveKit native broker target"),
        ("--auth-broker 127.0.0.1:50081", "LiveKit public auth target"),
        ("--username \"$UDB_SFU_OPERATOR_USERNAME\"", "LiveKit smoke login username"),
        ("--password \"$UDB_SFU_OPERATOR_PASSWORD\"", "LiveKit smoke login password"),
        ("--livekit-http http://127.0.0.1:57880", "LiveKit HTTP target"),
        ("--livekit-url ws://livekit:7880", "LiveKit internal URL"),
        ("livekit-sfu-smoke-logs", "LiveKit diagnostics artifact"),
        ('docker compose -p "$UDB_SFU_PROJECT"', "SFU compose scope"),
        ("down -v --remove-orphans", "SFU teardown"),
    ),
}

REQUIRED_PROOF_WORKFLOW_INPUTS = {
    "error-detail-served-smoke.yml": (
        "release_tag",
        "release_asset",
    ),
    "idempotency-served-smoke.yml": (
        "release_tag",
        "release_asset",
    ),
    "retry-safe-served-smoke.yml": (
        "release_tag",
        "release_asset",
    ),
    "rest-gateway-smoke.yml": (
        "base_url",
        "success_route",
        "error_route",
        "error_code",
    ),
}

NO_DEFAULT_PROOF_WORKFLOW_INPUTS = {
    "error-detail-served-smoke.yml": (),
    "idempotency-served-smoke.yml": (),
    "retry-safe-served-smoke.yml": (),
    "rest-gateway-smoke.yml": (
        "base_url",
        "success_route",
        "error_route",
        "error_code",
    ),
}

RELEASE_LEAF_WORKFLOWS = (
    "release-binaries.yml",
    "release-crates.yml",
    "release-docker.yml",
    "release-typescript-sdk.yml",
    "release-python-sdk.yml",
    "release-csharp-sdk.yml",
    "release-packagist.yml",
)

RELEASE_PUBLISHER_WORKFLOWS = tuple(
    name for name in RELEASE_LEAF_WORKFLOWS if name != "release-binaries.yml"
)

RELEASE_ORCHESTRATOR_REQUIREMENTS = (
    ("push:", "release tag trigger"),
    ("tags:", "release tag trigger list"),
    ("'v*.*.*'", "semver tag pattern"),
    ("cancel-in-progress: false", "uncancellable release concurrency"),
    ("ci-green:", "release CI-green gate job"),
    ("version-guard:", "release version guard job"),
    ("needs: ci-green", "version guard waits for CI-green"),
    ("gh run list", "CI run lookup"),
    ("--workflow ci.yml", "CI workflow lookup"),
    ("--commit \"${GITHUB_SHA}\"", "CI exact commit lookup"),
    ("build-binaries:", "release binary producer job"),
    ("needs: version-guard", "binary producer waits for version guard"),
    ("uses: ./.github/workflows/release-binaries.yml", "binary producer reusable workflow"),
    ("publish-crates:", "crates publisher job"),
    ("uses: ./.github/workflows/release-crates.yml", "crates reusable workflow"),
    ("publish-docker:", "Docker publisher job"),
    ("uses: ./.github/workflows/release-docker.yml", "Docker reusable workflow"),
    ("publish-ts:", "TypeScript publisher job"),
    ("uses: ./.github/workflows/release-typescript-sdk.yml", "TypeScript reusable workflow"),
    ("publish-py:", "Python publisher job"),
    ("uses: ./.github/workflows/release-python-sdk.yml", "Python reusable workflow"),
    ("publish-csharp:", "C# publisher job"),
    ("uses: ./.github/workflows/release-csharp-sdk.yml", "C# reusable workflow"),
    ("publish-packagist:", "Packagist publisher job"),
    ("uses: ./.github/workflows/release-packagist.yml", "Packagist reusable workflow"),
)

CLEANUP_PACKAGES_REQUIREMENTS = (
    ('workflows: ["Release"]', "top-level release completion trigger"),
    ("types: [completed]", "release-completed event filter"),
    ("schedule:", "weekly cleanup schedule"),
    ('cron: "0 2 * * 0"', "weekly Sunday cleanup cron"),
    ("workflow_dispatch:", "manual cleanup trigger"),
    ("keep_sha_tags:", "manual sha-retention input"),
    ("dry_run:", "manual dry-run input"),
    ("packages: write", "package delete permission"),
    ("cleanup-docker:", "GHCR cleanup job"),
    ("github.event.workflow_run.conclusion == 'success'", "successful-release cleanup gate"),
    ("actions/delete-package-versions@v5", "GitHub package deletion action"),
    ("package-name: udb", "UDB package target"),
    ("package-type: container", "container package target"),
    ("min-versions-to-keep: 0", "untagged cleanup keeps no stale untagged versions"),
    ("delete-only-untagged-versions: 'true'", "untagged-only cleanup pass"),
    ("github.event.inputs.keep_sha_tags || '5'", "sha tag retention default"),
    ("ignore-versions:", "semver/latest protection regex"),
    ("latest|\\d+\\.\\d+|\\d+", "major/minor/latest protection"),
    ("Dry run", "dry-run package listing step"),
    ("/users/fahara02/packages/container/udb/versions?per_page=100", "GHCR package listing endpoint"),
)

PUBLISH_SKILL_REQUIREMENTS = (
    ("push:", "skill source push trigger"),
    ("branches: [main]", "main branch publish trigger"),
    ('- "udb-skill/**"', "skill source trigger path"),
    ('- ".github/workflows/publish-skill.yml"', "publish-skill workflow trigger path"),
    ("release:", "release-published skill trigger"),
    ("types: [published]", "release published event filter"),
    ("workflow_dispatch:", "manual skill publish trigger"),
    ("permissions:\n  contents: read", "read-only repository permission"),
    ("validate:", "skill validation job"),
    ("Validate manifests + structure", "manifest structure hard gate"),
    ("udb-skill/plugins/udb/skills/using-udb/SKILL.md", "using-udb skill manifest"),
    ("udb-skill/plugins/udb/skills/udb-coding/SKILL.md", "udb-coding skill manifest"),
    ("udb-skill/.claude-plugin/marketplace.json", "Claude marketplace manifest"),
    ("udb-skill/plugins/udb/.claude-plugin/plugin.json", "Claude plugin manifest"),
    ("Wrapper drift check", "skill wrapper drift check"),
    ("Validate with Claude CLI", "Claude CLI validation step"),
    ("continue-on-error: true", "advisory smoke tolerance"),
    ("needs: validate", "publish jobs wait for validation"),
    ("ANTHROPIC_API_KEY not set", "Claude smoke optional-secret skip"),
    ("OLLAMA_API_KEY not set", "Ollama optional-secret skip"),
    ("OPENAI_API_KEY not set", "OpenAI optional-secret skip"),
    ('create_and_publish("udb-assistant"', "Ollama using-udb model publication"),
    ('create_and_publish("udb-coding"', "Ollama coding model publication"),
    ("registry.ollama.ai/v2/${model}/manifests/latest", "Ollama public manifest verification"),
    ("upsert \"UDB Assistant\"    udb-skill/openai/instructions.md", "OpenAI using-udb assistant sync"),
    ("upsert \"UDB Coding Agent\" udb-skill/openai/instructions-udb-coding.md", "OpenAI coding assistant sync"),
)

SHADOW_LIVE_SDK_REQUIREMENTS = (
    ("workflow_dispatch:", "manual shadow trigger"),
    ("release_tag:", "manual release tag input"),
    ('default: "latest"', "manual latest release default"),
    ("release_asset:", "manual release asset input"),
    ('default: "udb-linux-amd64-full"', "manual full Linux asset default"),
    ("permissions:\n  contents: read", "read-only shadow permissions"),
    ("shadow:", "shadow job"),
    ("uses: ./.github/workflows/_live-sdk-suite.yml", "reusable live SDK suite call"),
    ("release-tag: ${{ inputs.release_tag }}", "manual release tag handoff"),
    ("release-asset: ${{ inputs.release_asset }}", "manual release asset handoff"),
    ("secrets: inherit", "shadow reusable secrets handoff"),
)

COMPOSITE_SELFTEST_REQUIREMENTS = (
    ("workflow_dispatch:", "manual composite selftest trigger"),
    ("test_launch:", "documented launch-broker input"),
    ("permissions:\n  contents: read", "read-only composite selftest permissions"),
    ("concurrency:", "composite selftest concurrency"),
    ("cancel-in-progress: true", "composite selftest cancellation"),
    ("broker-env:", "broker-env selftest job"),
    ("uses: ./.github/actions/broker-env", "broker-env composite use"),
    ("test -n \"$UDB_TLS_REQUIRED\"", "broker-env assertion"),
    ("setup-rust:", "setup-rust selftest job"),
    ("uses: ./.github/actions/setup-rust", "setup-rust composite use"),
    ('install-build-deps: "false"', "fast setup-rust deps setting"),
    ("version-guard:", "version-guard selftest job"),
    ("uses: ./.github/actions/version-guard", "version-guard composite use"),
    ("setup-sdk-toolchains:", "setup-sdk-toolchains selftest job"),
    ("uses: ./.github/actions/setup-sdk-toolchains", "setup-sdk-toolchains composite use"),
    ("node -v", "Node toolchain assertion"),
    ("python --version", "Python toolchain assertion"),
    ("go version", "Go toolchain assertion"),
    ("start-backends:", "start-backends selftest job"),
    ("uses: ./.github/actions/start-backends", "start-backends composite use"),
    ('minio: "true"', "MinIO selftest backend"),
    ('kafka: "true"', "Kafka selftest backend"),
    ("docker ps --filter name=udb-bench-minio", "MinIO running assertion"),
    ("docker ps --filter name=udb-bench-kafka", "Kafka running assertion"),
    ("docker rm -f udb-bench-minio udb-bench-kafka", "selftest backend cleanup"),
    ("launch-broker:", "launch-broker documentation job"),
    ("use _live-sdk-suite for an end-to-end launch-broker test", "launch-broker coverage pointer"),
)

COMPOSITE_ACTION_SOURCE_REQUIREMENTS = {
    ".github/actions/broker-env/action.yml": (
        ("echo \"UDB_PG_DSN=${PG_DSN}\"", "Postgres DSN env export"),
        ("echo \"UDB_GRPC_ADDR=${GRPC_ADDR}\"", "gRPC bind env export"),
        ("echo \"UDB_LIVE_REQUIRED_BACKENDS=${LIVE_REQUIRED_BACKENDS}\"", "required backend env export"),
        ("echo \"UDB_MINIO_ENDPOINT=http://localhost:9000\"", "MinIO endpoint env"),
        ("echo \"UDB_KAFKA_BROKERS=localhost:59192\"", "Kafka broker env"),
        ("echo \"UDB_QDRANT_URL=http://localhost:6333\"", "Qdrant URL env"),
        ("echo \"UDB_REDIS_DSN=redis://localhost:6379/0\"", "Redis DSN env"),
        ("enable_column_backend:", "ClickHouse env toggle input"),
        ("enable_graph_backend:", "Neo4j env toggle input"),
        ('if [ "${ENABLE_COLUMN_BACKEND}" = "true" ]; then', "ClickHouse env conditional"),
        ('if [ "${ENABLE_GRAPH_BACKEND}" = "true" ]; then', "Neo4j env conditional"),
        ("echo \"UDB_COLUMN_DSN=http://localhost:8123\"", "ClickHouse HTTP env"),
        ("echo \"UDB_GRAPH_DSN=http://localhost:7474\"", "Neo4j HTTP env"),
        ("echo \"UDB_ALLOW_DEGRADED_BACKENDS=true\"", "degraded optional backend posture"),
        ("echo \"UDB_TLS_REQUIRED=false\"", "dev TLS posture"),
        ("echo \"UDB_SERVICE_IDENTITY_REQUIRED=false\"", "dev service identity posture"),
        ("echo \"UDB_ENABLE_ADMIN_SEED=1\"", "admin seed gate env"),
        ("echo \"UDB_NOTIFICATION_TEST_MODE=1\"", "notification failed-log test-mode env"),
        ("echo \"UDB_ABAC_DEFAULT_ALLOW=true\"", "dev ABAC allow env"),
        ("echo \"UDB_GRPC_MAX_CONCURRENT=512\"", "gRPC benchmark concurrency headroom"),
        ("echo \"UDB_READ_MAX_CONCURRENT=300\"", "read admission headroom"),
        ("echo \"UDB_WRITE_MAX_CONCURRENT=100\"", "write admission headroom"),
        ("echo \"UDB_TX_MAX_CONCURRENT=50\"", "transaction admission headroom"),
        ("echo \"UDB_MIGRATION_MAX_CONCURRENT=16\"", "migration admission headroom"),
        ("echo \"UDB_CDC_MAX_CONCURRENT=32\"", "CDC admission headroom"),
        ("echo \"UDB_ADMIN_MAX_CONCURRENT=64\"", "admin admission headroom"),
        ("echo \"UDB_VECTOR_MAX_CONCURRENT=80\"", "vector admission headroom"),
        ("echo \"UDB_OBJECT_MAX_CONCURRENT=80\"", "object admission headroom"),
        ("echo \"UDB_GENERIC_MAX_CONCURRENT=50\"", "generic dispatch admission headroom"),
        ("echo \"UDB_READ_QUEUE_TIMEOUT_MS=5000\"", "read queue timeout headroom"),
        ("echo \"UDB_WRITE_QUEUE_TIMEOUT_MS=5000\"", "write queue timeout headroom"),
        ("echo \"UDB_TX_QUEUE_TIMEOUT_MS=5000\"", "transaction queue timeout headroom"),
        ("echo \"UDB_MIGRATION_QUEUE_TIMEOUT_MS=5000\"", "migration queue timeout headroom"),
        ("echo \"UDB_CDC_QUEUE_TIMEOUT_MS=5000\"", "CDC queue timeout headroom"),
        ("echo \"UDB_ADMIN_QUEUE_TIMEOUT_MS=5000\"", "admin queue timeout headroom"),
        ("echo \"UDB_VECTOR_QUEUE_TIMEOUT_MS=5000\"", "vector queue timeout headroom"),
        ("echo \"UDB_OBJECT_QUEUE_TIMEOUT_MS=5000\"", "object queue timeout headroom"),
        ("echo \"UDB_GENERIC_QUEUE_TIMEOUT_MS=5000\"", "generic queue timeout headroom"),
        ("echo \"UDB_PG_MAX_CONNECTIONS=80\"", "Postgres pool benchmark headroom"),
        ("echo \"UDB_TENANT_CONN_BUDGET_DEFAULT=80\"", "tenant connection budget headroom"),
        ("echo \"UDB_TENANT_CONN_QUEUE_TIMEOUT_MS=5000\"", "tenant connection queue timeout headroom"),
        ("echo \"UDB_FAIR_TOKENS_PER_SEC=5000\"", "global fair-admission headroom"),
        ("echo \"UDB_AUDIT_SINK_URL=file:///tmp/udb-bench-audit.jsonl\"", "audit sink env"),
        ("echo \"UDB_ENCRYPTION_KEY=QkJC", "test encryption key env"),
        ("echo \"UDB_WEBAUTHN_TEST_MODE=1\"", "WebAuthn dev test mode"),
        ("echo \"UDB_SAML_TEST_MODE=1\"", "SAML dev test mode"),
        ("echo \"UDB_OTP_DEV_ECHO=1\"", "OTP dev echo env"),
        ("echo \"UDB_WEBRTC_REAP_INTERVAL_SECS=0\"", "WebRTC reaper disabled for harness"),
    ),
    ".github/actions/launch-broker/action.yml": (
        ("\"${BIN}\" auth bootstrap user", "optional bootstrap command"),
        ("\"${BIN}\" serve proto \"\" \"${GRPC_ADDR}\"", "served broker launch command"),
        ("echo \"pid=${PID}\" >> \"$GITHUB_OUTPUT\"", "PID output"),
        ("echo \"UDB_BROKER_PID=${PID}\" >> \"$GITHUB_ENV\"", "broker PID env export"),
        ("HOST=\"${GRPC_ADDR%:*}\"", "gRPC host parse"),
        ("PORT=\"${GRPC_ADDR##*:}\"", "gRPC port parse"),
        ("AUTH_PORT=$((PORT + 10))", "auth listener port derivation"),
        ("echo > \"/dev/tcp/${HOST}/${PORT}\"", "public listener readiness probe"),
        ("echo > \"/dev/tcp/${HOST}/${AUTH_PORT}\"", "auth listener readiness probe"),
        ("kill -0 \"${PID}\"", "broker process liveness probe"),
        ("tail -n 200 \"${LOG}\"", "failure log tail"),
        ("Broker not ready within", "timeout failure message"),
        ("exit 1", "fail-closed readiness timeout"),
    ),
    ".github/actions/resolve-served-binary/action.yml": (
        ("broker-artifact-run-id:", "current CI artifact input"),
        ("release-tag:", "release tag input"),
        ("release-asset:", "release asset input"),
        ("gh run download \"${BROKER_ARTIFACT_RUN_ID}\"", "current CI artifact download"),
        ("--name udb-broker-debug", "single broker artifact name"),
        ("find smoke-output/bin-artifact -type f -name udb", "artifact binary discovery"),
        ("UDB_SERVED_RELEASE_TAG=ci-artifact-${BROKER_ARTIFACT_RUN_ID}", "artifact source marker"),
        ("gh release download \"${tag}\"", "release binary download"),
        ("UDB_SERVED_BINARY_SOURCE=release", "release source marker"),
        ("UDB_SERVED_BIN=", "served binary env export"),
    ),
    ".github/actions/start-backends/action.yml": (
        ("docker run -d --name udb-bench-minio", "MinIO container name"),
        ("minio/minio:RELEASE.2025-01-20T14-49-07Z", "MinIO image pin"),
        ("curl -fsS http://localhost:9000/minio/health/live", "MinIO health gate"),
        ("docker run -d --name udb-bench-kafka", "Kafka container name"),
        ("apache/kafka:3.9.0", "Kafka image pin"),
        ("kafka-broker-api-versions.sh", "Kafka readiness gate"),
        ("kafka-topics.sh --create --if-not-exists", "Kafka topic creation"),
        ("docker run -d --name udb-bench-qdrant", "Qdrant container name"),
        ("qdrant/qdrant:v1.18.2", "Qdrant image pin"),
        ("curl -fsS http://localhost:6333/readyz", "Qdrant readiness gate"),
        ("docker run -d --name udb-bench-redis", "Redis container name"),
        ("redis:7-alpine", "Redis image pin"),
        ("redis-cli ping", "Redis readiness gate"),
        ("docker run -d --name udb-bench-clickhouse", "ClickHouse container name"),
        ("clickhouse/clickhouse-server:24.8", "ClickHouse image pin"),
        ("SELECT 1", "ClickHouse query readiness gate"),
        ("docker run -d --name udb-bench-neo4j", "Neo4j container name"),
        ("neo4j:5", "Neo4j image pin"),
        ("http://localhost:7474/db/neo4j/tx/commit", "Neo4j query readiness gate"),
        ("curl -sSL https://dl.min.io/client/mc/release/linux-amd64/mc", "MinIO client download"),
        ("\"${mc_bin}\" mb --ignore-existing \"local/${MINIO_BUCKET}\"", "MinIO live SDK bucket"),
        ("\"${mc_bin}\" mb --ignore-existing \"local/${MINIO_STORAGE_BUCKET}\"", "MinIO storage bucket"),
    ),
    ".github/actions/setup-rust/action.yml": (
        ("dtolnay/rust-toolchain@stable", "Rust toolchain action"),
        ("Swatinem/rust-cache@v2", "Rust cache action"),
        ("CARGO_NET_RETRY=10", "Cargo retry env"),
        ("CARGO_HTTP_MULTIPLEXING=false", "Cargo multiplexing env"),
        ("CARGO_HTTP_TIMEOUT=120", "Cargo timeout env"),
        ("sudo apt-get install -y --no-install-recommends build-essential cmake clang perl nasm ninja-build pkg-config libssl-dev", "Linux native build deps"),
        ("brew install cmake nasm ninja", "macOS native build deps"),
        ("choco install strawberryperl nasm --no-progress -y", "Windows native build deps"),
        ("ilammy/msvc-dev-cmd@v1", "MSVC dev command setup"),
    ),
    ".github/actions/setup-sdk-toolchains/action.yml": (
        ("node-version:", "Node version input"),
        ("default: \"20\"", "Node 20 default"),
        ("python-version:", "Python version input"),
        ("default: \"3.12\"", "Python 3.12 default"),
        ("go-version:", "Go version input"),
        ("default: \"1.22\"", "Go 1.22 default"),
        ("dotnet-version:", ".NET version input"),
        ("default: \"8.0.x\"", ".NET 8 default"),
        ("java-version:", "Java version input"),
        ("default: \"17\"", "Java 17 default"),
        ("php-version:", "PHP version input"),
        ("default: \"8.2\"", "PHP 8.2 default"),
        ("actions/setup-node@v4", "Node setup action"),
        ("actions/setup-python@v5", "Python setup action"),
        ("actions/setup-go@v5", "Go setup action"),
        ("actions/setup-dotnet@v4", ".NET setup action"),
        ("actions/setup-java@v4", "Java setup action"),
        ("shivammathur/setup-php@v2", "PHP setup action"),
        ("extensions: grpc, mbstring, intl", "PHP extension set without protobuf"),
        ("tools: composer:v2", "Composer v2 tool"),
    ),
    ".github/actions/version-guard/action.yml": (
        ("node scripts/check-versions.mjs", "version manifest self-consistency command"),
        ("require('./versions.json').components['${COMPONENT}'].version", "component version lookup"),
        ("GUARD_REF: ${{ github.ref }}", "guard ref env"),
        ("GUARD_REF_NAME: ${{ github.ref_name }}", "guard ref-name env"),
        ("refs/tags/*", "tag-trigger branch"),
        ("GUARD_REF_NAME#v", "v-prefix tag normalization"),
        ("DISPATCH_VERSION", "manual dispatch version input"),
        ("does not match versions.json", "mismatch failure message"),
        ("exit 1", "fail-closed version mismatch"),
    ),
}

RELEASE_FFMPEG_REQUIREMENTS = (
    ("vendored-ffmpeg:", "vendored ffmpeg release gate job"),
    ("needs: version-guard", "vendored ffmpeg gate after version guard"),
    ("python scripts/check-vendored-ffmpeg.py --selftest", "vendored ffmpeg verifier selftest"),
    ("sudo apt-get install -y --no-install-recommends ffmpeg", "ffmpeg package install"),
    ('python scripts/ffmpeg_transcode_smoke.py --ffmpeg-bin "$(command -v ffmpeg)" --artifact-dir ffmpeg-transcode-smoke', "ffmpeg transcode smoke"),
    ("name: ffmpeg-transcode-smoke", "release ffmpeg diagnostics artifact"),
    ("needs: vendored-ffmpeg", "binary build waits for ffmpeg gate"),
    ("node scripts/gen-release-manifest.mjs --selftest", "release manifest generator selftest"),
    ("node scripts/gen-release-manifest.mjs dist > dist/manifest.json", "release manifest generation"),
)

FFMPEG_TRANSCODE_SMOKE_REQUIREMENTS = (
    ("MAX_FFMPEG_COMMAND_TIMEOUT_SECONDS = 300.0", "ffmpeg command timeout ceiling"),
    ("TIMEOUT_DECIMAL_PATTERN", "ffmpeg timeout decimal pattern"),
    ("def normalize_timeout_seconds(", "ffmpeg timeout normalizer"),
    ("--timeout must not include surrounding whitespace", "ffmpeg timeout whitespace rejection"),
    ("--timeout must be a positive decimal number of seconds", "ffmpeg timeout decimal rejection"),
    ("--timeout must be <= 300 seconds", "ffmpeg timeout ceiling rejection"),
    ("canonical ffmpeg timeout string was rejected", "ffmpeg canonical timeout selftest"),
    ("ffmpeg timeout regression was not caught", "ffmpeg timeout negative selftest"),
    ('parser.add_argument("--timeout", default="30"', "raw ffmpeg timeout CLI token"),
)

LIVEKIT_SFU_SMOKE_REQUIREMENTS = (
    ("MAX_LIVEKIT_URL_BYTES = 2048", "LiveKit URL length ceiling"),
    ("MAX_LIVEKIT_RESPONSE_BYTES = 1_048_576", "LiveKit response byte ceiling"),
    ("BROKER_TARGET_PATTERN", "broker target pattern"),
    ("def canonical_network_token(", "network token canonicalizer"),
    ("must not include surrounding whitespace", "network token whitespace rejection"),
    ("must not include whitespace or control characters", "network token control-char rejection"),
    ("def validate_base_url(", "LiveKit base URL validator"),
    ("urllib.parse.urlsplit(value)", "structured URL parsing"),
    ("parsed.username or parsed.password", "credentialed URL rejection"),
    ("must be a base URL without path, query, or fragment", "base URL path/query rejection"),
    ("def validate_broker_target(", "broker target validator"),
    ("--broker must be a host:port gRPC target", "broker host-port rejection"),
    ("--broker must include a valid numeric port", "broker numeric port rejection"),
    ("def decode_limited_json(", "bounded LiveKit JSON decoder"),
    ("source.read(MAX_LIVEKIT_RESPONSE_BYTES + 1)", "bounded LiveKit response read"),
    ("LiveKit response exceeded 1048576 bytes", "oversized LiveKit response rejection"),
    (
        'validate_base_url("--livekit-http", args.livekit_http, schemes={"http", "https"})',
        "live LiveKit HTTP URL validation",
    ),
    (
        'validate_base_url("--livekit-url", args.livekit_url, schemes={"ws", "wss"})',
        "live SFU URL validation",
    ),
    ("validate_broker_target(args.broker)", "live broker target validation"),
    ("padded LiveKit HTTP URL", "padded LiveKit HTTP URL selftest"),
    ("credentialed LiveKit HTTP URL", "credentialed LiveKit HTTP URL selftest"),
    ("path-bearing LiveKit HTTP URL", "path-bearing LiveKit HTTP URL selftest"),
    ("unsupported LiveKit URL scheme", "unsupported LiveKit URL selftest"),
    ("out-of-range broker port", "out-of-range broker port selftest"),
    ("nonnumeric broker port", "nonnumeric broker port selftest"),
)

RELEASE_BINARY_MATRIX_REQUIREMENTS = (
    ("workflow_call:", "reusable release binary producer"),
    ("workflow_dispatch:", "manual build-only dry run"),
    ("PORTABLE_FEATURES:", "portable feature set env"),
    ("FULL_FEATURES:", "full feature set env"),
    ("postgres,mysql,sqlite,qdrant,s3,mongodb-native", "portable backend feature surface"),
    ("oidc,webauthn", "auth/full feature surface"),
    ("permissions:\n  contents: read", "read-only top-level permissions"),
    ("concurrency:", "release binary concurrency"),
    ("cancel-in-progress: true", "dry-run concurrency cancellation"),
    ("version-guard:", "version guard job"),
    ("component: udb", "UDB version guard component"),
    ("build:", "binary build matrix job"),
    ("needs: vendored-ffmpeg", "binary build waits for ffmpeg gate"),
    ("fail-fast: true", "release binary matrix fail-fast"),
    ("- os: ubuntu-22.04", "Linux glibc-floor runner pin"),
    ("target: x86_64-unknown-linux-gnu", "Linux amd64 target"),
    ("asset: udb-linux-amd64", "portable Linux asset"),
    ("asset: udb-windows-amd64.exe", "Windows asset"),
    ("target: x86_64-pc-windows-msvc", "Windows amd64 target"),
    ("asset: udb-darwin-arm64", "macOS arm64 asset"),
    ("target: aarch64-apple-darwin", "macOS arm64 target"),
    ("asset: udb-darwin-amd64", "macOS Intel asset"),
    ("target: x86_64-apple-darwin", "macOS Intel target"),
    ("asset: udb-linux-amd64-full", "full Linux asset"),
    ("variant: portable", "portable variant matrix entries"),
    ("variant: full", "full variant matrix entry"),
    ("target_cpu: x86-64-v2", "x86-64-v2 shipped CPU floor"),
    ("target_cpu: apple-m1", "Apple Silicon CPU floor"),
    ("uses: ./.github/actions/setup-rust", "shared Rust setup composite"),
    ("cache-key: ${{ matrix.target }}", "per-target Rust cache key"),
    ("export RUSTFLAGS=\"-C target-cpu=${MATRIX_TARGET_CPU} ${RUSTFLAGS:-}\"", "target CPU RUSTFLAGS"),
    ("cargo build --profile dist --locked --target \"${MATRIX_TARGET}\"", "dist profile locked build"),
    ("--features \"${FULL_FEATURES}\"", "full feature build"),
    ("--no-default-features --features \"${PORTABLE_FEATURES}\"", "portable feature build"),
    ("Stage asset + checksum", "asset staging step"),
    ("cp \"target/${MATRIX_TARGET}/dist/udb${MATRIX_EXT}\" \"dist/${MATRIX_ASSET}\"", "raw binary staging"),
    ("sha256sum \"${MATRIX_ASSET}\" > \"${MATRIX_ASSET}.sha256\"", "Linux checksum sidecar"),
    ("shasum -a 256 \"${MATRIX_ASSET}\" > \"${MATRIX_ASSET}.sha256\"", "macOS checksum sidecar"),
    ("Upload workflow artifact", "same-run workflow artifact upload"),
    ("name: ${{ matrix.asset }}", "workflow artifact name"),
    ("dist/${{ matrix.asset }}.sha256", "workflow checksum artifact"),
    ("Guard tag still points at this commit", "binary tag freshness guard"),
    ("gh api \"repos/${GITHUB_REPOSITORY}/git/ref/tags/${GITHUB_REF_NAME}\"", "tag ref lookup"),
    ("ref_sha}\" != \"${GITHUB_SHA}\"", "tag SHA equality check"),
    ("refusing to publish stale binary asset", "stale binary release refusal"),
    ("Attach to GitHub Release", "raw binary release attachment"),
    ("softprops/action-gh-release@v2", "GitHub release attachment action"),
    ("fail_on_unmatched_files: true", "release attachment hard fail"),
    ("manifest:", "release manifest job"),
    ("needs: build", "manifest waits for all binary builds"),
    ("if: startsWith(github.ref, 'refs/tags/')", "tag-only manifest attach"),
    ("Download the published binaries + checksums", "manifest downloads attached binaries"),
    ("gh release download \"${GITHUB_REF_NAME}\" --dir dist --pattern 'udb-*'", "manifest release download"),
    ("manifest.json.sha256", "manifest checksum sidecar"),
    ("refusing to publish stale release manifest", "stale manifest release refusal"),
    ("Attach manifest to GitHub Release", "manifest release attachment"),
)

RELEASE_MANIFEST_GENERATOR_REQUIREMENTS = (
    ("const NAME_RE = /^udb-(linux|darwin|windows)-(amd64|arm64)", "canonical raw binary name parser"),
    ("missing .sha256 sidecar", "missing checksum-sidecar rejection"),
    ("invalid .sha256 sidecar", "invalid checksum-sidecar rejection"),
    ("sha256 mismatch", "stale checksum-sidecar rejection"),
    ("unrecognized release asset name", "stale asset-name rejection"),
    ("tier: tier || \"portable\"", "portable tier default"),
    ("sha256: readExpectedSha256(dir, name)", "asset checksum verification"),
    ("size: fs.statSync(path.join(dir, name)).size", "asset size metadata"),
    ("scheme: \"udb-<os>-<arch>[-<tier>][.exe]\"", "published asset scheme"),
    ("base_url: `https://github.com/fahara02/udb/releases/download/v${version}`", "release download base URL"),
    ("assets.length === 3", "selftest asset count assertion"),
    ("udb-linux-amd64-full", "full Linux asset selftest"),
    ("selftest failed to reject missing checksum sidecar", "missing checksum selftest"),
    ("selftest failed to reject stale checksum sidecar", "stale checksum selftest"),
    ("selftest failed to reject unrecognized release asset name", "bad asset name selftest"),
)

RELEASE_PUBLISHER_LEAF_REQUIREMENTS = {
    "release-crates.yml": (
        ("uses: ./.github/actions/setup-rust", "Rust publish toolchain setup"),
        ("uses: ./.github/actions/setup-sdk-toolchains", "Node setup for version guard"),
        ("component: udb", "crate version guard component"),
        ("Check crates.io version availability", "crates.io availability check"),
        ("https://crates.io/api/v1/crates/udb/${version}", "crates.io version endpoint"),
        ("id: crate_version", "crate availability step id"),
        ("if: steps.crate_version.outputs.exists != 'true'", "crate publish skip-if-existing"),
        ("cargo publish --dry-run", "crate publish dry run"),
        ("CARGO_REGISTRY_TOKEN", "crates.io token env"),
        ("cargo publish 2>&1 | tee /tmp/cargo-publish.log", "crate publish log capture"),
        ("status=${PIPESTATUS[0]}", "crate publish status capture"),
        ("already exists on crates.io", "crate already-published idempotence"),
    ),
    "release-typescript-sdk.yml": (
        ("component: sdk-typescript", "TypeScript version guard component"),
        ("node-registry-url: \"https://registry.npmjs.org\"", "npm registry setup"),
        ("npm install --no-audit --no-fund", "npm dependency install"),
        ("id: npm_version", "npm availability step id"),
        ("npm view \"@udb_plus/sdk@${version}\" version --silent", "npm version availability check"),
        ("if: steps.npm_version.outputs.exists != 'true'", "npm publish skip-if-existing"),
        ("npm run build", "npm package build"),
        ("npm publish --dry-run --ignore-scripts --access public", "npm publish dry run without hooks"),
        ("NODE_AUTH_TOKEN", "npm token env"),
        ("npm publish --ignore-scripts --access public", "npm publish without hooks"),
    ),
    "release-python-sdk.yml": (
        ("component: sdk-python", "Python version guard component"),
        ("uses: astral-sh/setup-uv@v6", "uv setup"),
        ("uv sync --extra dev", "Python dependency install"),
        ("uv run python -m build", "Python package build"),
        ("uv run twine check dist/*", "Python package metadata check"),
        ("TWINE_USERNAME: __token__", "PyPI token username"),
        ("TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}", "PyPI token secret"),
        ("uv run twine upload --skip-existing dist/*", "PyPI skip-existing publish"),
    ),
    "release-csharp-sdk.yml": (
        ("id-token: write", "NuGet trusted publishing token permission"),
        ("component: sdk-csharp", "C# version guard component"),
        ("dotnet restore sdk/csharp/Udb.Client.Tests/Udb.Client.Tests.csproj", "Udb.Client restore"),
        ("dotnet restore sdk/csharp/Udb.Cli/Udb.Cli.csproj", "Udb.Cli restore"),
        ("dotnet build sdk/csharp/Udb.Client.Tests/Udb.Client.Tests.csproj --configuration Release --no-restore", "Udb.Client release build"),
        ("dotnet build sdk/csharp/Udb.Cli/Udb.Cli.csproj --configuration Release --no-restore", "Udb.Cli release build"),
        ("id: nuget_client", "NuGet Udb.Client availability step"),
        ("id: nuget_cli", "NuGet Udb.Cli availability step"),
        ("https://api.nuget.org/v3-flatcontainer/udb.client/${version}/udb.client.nuspec", "NuGet Udb.Client availability endpoint"),
        ("https://api.nuget.org/v3-flatcontainer/udb.cli/${version}/udb.cli.nuspec", "NuGet Udb.Cli availability endpoint"),
        ("dotnet pack --configuration Release --no-build --output ./nupkg", "NuGet pack without rebuild"),
        ("uses: NuGet/login@v1", "NuGet trusted publishing login"),
        ("dotnet nuget push ./nupkg/*.nupkg", "NuGet push"),
        ("--skip-duplicate", "NuGet duplicate skip"),
    ),
    "release-packagist.yml": (
        ("validate-php-sdk:", "PHP validation job"),
        ("composer validate --strict --no-check-publish", "Composer strict validation"),
        ("composer install --no-interaction --no-progress --prefer-dist", "Composer install"),
        ("Verify generated stubs are committed", "PHP generated stub presence check"),
        ("gen/Udb/Services/V1/DataBrokerClient.php", "PHP DataBroker generated stub check"),
        ("component: sdk-php", "PHP version guard component"),
        ("push-satellite:", "satellite push job"),
        ("needs: validate-php-sdk", "satellite push waits for validation"),
        ("fetch-depth: 0", "full history checkout for subtree split"),
        ("webfactory/ssh-agent@v0.9.0", "satellite deploy key setup"),
        ("git subtree split --prefix=sdk/php -b sdk-php-split", "PHP subtree split"),
        ("git push --force \"$SATELLITE_REPO\" sdk-php-split:main", "satellite main update"),
        ("git tag -f \"${TAG_NAME}\" sdk-php-split", "satellite tag mirror"),
        ("git push --force \"$SATELLITE_REPO\" \"refs/tags/${TAG_NAME}\"", "satellite tag push"),
        ("notify-packagist:", "Packagist notify job"),
        ("needs: push-satellite", "Packagist notify waits for satellite push"),
        ("Packagist credentials not configured", "Packagist optional-secret skip"),
        ("exit 0", "Packagist missing-credential nonfatal exit"),
        ("https://packagist.org/api/update-package", "Packagist update endpoint"),
    ),
}

RELEASE_DOCKER_REQUIREMENTS = (
    ("Download release binary into build context", "release binary download step"),
    ("gh release download", "release asset download command"),
    ("--pattern 'udb-linux-amd64-full'", "full Linux release asset pattern"),
    ("--output udb", "release binary output name"),
    ("chmod +x udb", "release binary executable bit"),
    ("docker/build-push-action@v6", "Docker build-push action"),
    ("file: ./Dockerfile.release", "release Dockerfile path"),
    ("platforms: linux/amd64", "release Docker platform"),
)

DOCKERFILE_RELEASE_REQUIREMENTS = (
    ("FROM debian:bookworm-slim AS runtime", "release runtime base"),
    ("apt-get install -y --no-install-recommends ca-certificates curl", "minimal runtime deps"),
    ("GRPC_HEALTH_PROBE_VERSION=v0.4.37", "grpc health probe pin"),
    ("COPY udb /usr/local/bin/udb", "prebuilt release binary copy"),
    ("COPY proto ./proto", "proto runtime copy"),
    ("COPY third_party ./third_party", "third-party runtime copy"),
    ("COPY configs ./configs", "config runtime copy"),
    ("UDB_FFMPEG_BIN=/usr/bin/ffmpeg", "ffmpeg binary env"),
    ("USER udb:udb", "non-root runtime user"),
    ('ENTRYPOINT ["/usr/local/bin/udb"]', "udb entrypoint"),
)

CI_LAUNCHER_ASSET_REQUIREMENTS = (
    ("Launcher asset-name conformance", "launcher asset-name CI step"),
    ("node scripts/check-launcher-assets.mjs --selftest", "launcher asset guard selftest"),
)

CI_SDK_SERVICE_COVERAGE_REQUIREMENTS = (
    ("SDK service-coverage guard", "SDK service-coverage CI step"),
    ("python3 scripts/check-sdk-service-coverage.py --selftest", "SDK service-coverage selftest"),
)

CI_TOPOLOGY_REQUIREMENTS = (
    ("on:\n  push:\n    branches: [main]\n  pull_request:\n    branches: [main]", "main-only push/PR triggers"),
    ("concurrency:\n  group: ci-${{ github.workflow }}-${{ github.ref }}\n  cancel-in-progress: true", "CI concurrency cancellation"),
    ("permissions:\n  contents: read", "read-only CI permissions"),
    ("LIVE_BROKER_FEATURES:", "single live broker feature tier"),
)

CI_TOPOLOGY_DEPENDENCY_FREE_JOBS = (
    "quick-gate",
    "clippy-advisory",
    "rust",
    "slim-build",
    "supply-chain",
    "buf",
    "php-sdk",
    "go-sdk",
    "ts-sdk",
    "python-sdk",
    "csharp-sdk",
    "java-sdk",
    "sdk-conformance",
    "versions",
    "docs-links",
)

CI_TOPOLOGY_QUICK_GATE_JOBS = (
    "build-broker",
    "feature-check",
    "plugin-feature-matrix",
    "optimized",
    "aarch64-scalar",
    "native-integration",
)

CI_TOPOLOGY_BUILD_BROKER_CONSUMERS = (
    "smoke",
    "scaffold-compiles",
)

CI_TOPOLOGY_PUSH_ONLY_JOBS = (
    "auth-release-binary",
    "plugin-feature-matrix",
    "optimized",
    "aarch64-scalar",
    "native-integration",
)

CI_TOPOLOGY_PR_ONLY_JOBS = (
    "feature-check",
)

CI_SDK_CONFORMANCE_REQUIREMENTS = (
    ("node sdk-conformance/run.mjs metadata error-details typescript python go csharp java php", "SDK alias/operationId metadata + error-detail conformance targets"),
)

CI_ARCHITECTURE_REQUIREMENTS = (
    ("post-release benchmark", "benchmark-owned live SDK coverage"),
    ("sdk-conformance(mock)", "offline SDK conformance in PR graph"),
    ("scaffold-compiles", "scaffold compile gate in PR graph"),
    ("release-binary SDK live benchmark/perf suite", "release-binary SDK benchmark description"),
    ("offline SDK conformance/facade/scaffold gates", "offline SDK CI ownership"),
    ("path-scoped actionlint + workflow posture", "path-scoped workflow lint description"),
    ("not currently a\n  branch-protection required check", "actionlint not required while path-filtered"),
    ("path-scoped `lint-workflows.yml`/`actionlint`", "actionlint advisory classification"),
    ("Post-release chain:", "post-release event chain section"),
    ("Release success -> benchmark-sdks.yml -> _live-sdk-suite.yml", "Release-to-benchmark event chain"),
    ("Benchmark completion -> pages.yml", "benchmark-to-Pages event chain"),
    ("Release success / schedule / dispatch -> cleanup-packages.yml", "cleanup event chain"),
    ("benchmark-sdks.yml", "benchmark workflow ownership"),
)

CI_QUICK_GATE_SOURCE_GUARDS = (
    ("Vector canonical CAS posture guard", "scripts/check-vector-cas-posture.py", "vector CAS posture"),
    ("ORM template posture guard", "scripts/check-orm-template-posture.py", "ORM template posture"),
    ("Workflow service posture guard", "scripts/check-workflow-service-posture.py", "WorkflowService posture"),
    ("IR live-golden posture guard", "scripts/check-ir-live-golden-posture.py", "IR live-golden posture"),
    ("Scaffold posture guard", "scripts/check-scaffold-posture.py", "scaffold posture"),
    ("SDK helper parity guard", "scripts/check-sdk-helper-parity.py", "SDK helper parity"),
    ("Todo-board status guard", "scripts/check-todo-board-status.py", "todo-board status"),
    ("Gap-closure posture guard", "scripts/check-gap-closure-posture.py", "gap-closure posture"),
    ("Bench harness posture guard", "scripts/check-bench-harness-posture.py", "bench harness posture"),
    ("Docs/CI freshness posture guard", "scripts/check-docs-ci-freshness-posture.py", "docs/CI freshness posture"),
    ("Go SDK posture guard", "scripts/check-go-sdk-posture.py", "Go SDK posture"),
    ("TypeScript SDK posture guard", "scripts/check-ts-sdk-posture.py", "TypeScript SDK posture"),
    ("Python/PHP SDK posture guard", "scripts/check-python-php-sdk-posture.py", "Python/PHP SDK posture"),
    ("Java/C# SDK audit guard", "scripts/check-java-csharp-sdk-audit.py", "Java/C# SDK audit"),
    ("API/SDK alias posture guard", "scripts/check-api-sdk-alias-posture.py", "API/SDK alias posture"),
    ("OpenAPI operation-id posture guard", "scripts/check-openapi-operationid-posture.py", "OpenAPI operation-id posture"),
    ("Idempotency dedup posture guard", "scripts/check-idempotency-dedup-posture.py", "idempotency dedup posture"),
    ("Retry-safe mutation posture guard", "scripts/check-retry-safe-posture.py", "retry-safe mutation posture"),
    ("Error-detail posture guard", "scripts/check-error-detail-posture.py", "error-detail posture"),
    ("Beta versioning posture guard", "scripts/check-beta-versioning-posture.py", "beta versioning posture"),
)

CI_PUBLIC_DOC_GUARDS = (
    ("Doc service-count drift guard", "scripts/check-doc-service-counts.py", "doc service-count drift"),
    ("No internal tables guard", "scripts/check-no-internal-tables.py", "no internal tables"),
)

CI_DOCS_LINKS_REQUIREMENTS = (
    ("docs-links:", "docs-links CI job key"),
    ("name: Markdown local links + readiness artifacts", "docs-links CI job name"),
    ("uses: ./.github/actions/setup-sdk-toolchains", "docs-links SDK toolchain setup"),
    ('node: "true"', "docs-links Node toolchain enablement"),
    ("node --check scripts/check-markdown-links.mjs", "markdown link syntax check"),
    ("node scripts/check-markdown-links.mjs --selftest", "markdown link selftest command"),
    ("node scripts/check-markdown-links.mjs", "markdown local-link guard command"),
    ("node --check scripts/check-enterprise-readiness.mjs", "enterprise readiness syntax check"),
    ("node scripts/check-enterprise-readiness.mjs --selftest", "enterprise readiness selftest command"),
    ("node scripts/check-enterprise-readiness.mjs", "enterprise readiness artifact guard command"),
)

CI_RUST_GENERATED_CONTRACT_DOC_GATES = (
    (
        "Native contract manifest drift + lint (F13 hard gate)",
        "native contract manifest drift/lint",
        (
            "cargo run --locked -q --bin udb -- native manifest > docs/generated/udb-native-contract.json",
            "git diff --quiet -- docs/generated/udb-native-contract.json",
            "cargo run --locked -q --bin udb -- native lint",
        ),
    ),
    (
        "Native docs markdown drift",
        "native docs markdown drift",
        (
            "cargo run --locked -q --bin udb -- native docs > docs/generated/native-services.md",
            "git diff --quiet -- docs/generated/native-services.md",
        ),
    ),
    (
        "Codebase map freshness gate",
        "codebase map freshness",
        (
            "python3 scripts/generate-codebase-map.py --check",
        ),
    ),
    (
        "Native contract breaking-change gate (Phase 3)",
        "native contract breaking-change",
        (
            "cargo run --locked -q --bin udb -- native contract-diff",
            "--baseline docs/generated/contract-baseline.bin",
        ),
    ),
)

CI_BUF_GENERATED_ARTIFACT_REQUIREMENTS = (
    ("buf:", "buf generated-artifact job"),
    ("name: Proto (buf)", "buf job stable name"),
    ("fetch-depth: 0", "full checkout for buf breaking/drift context"),
    ("bufbuild/buf-setup-action@v1", "buf setup action"),
    ("version: 1.65.0", "pinned buf version"),
    ("buf build", "buf build command"),
    ("Verify committed stubs are current", "committed stub drift step"),
    ("buf generate --include-imports", "include-imports SDK/API generation"),
    ("retrying remote plugin generation", "remote plugin retry diagnostic"),
    ("node scripts/openapi-postprocess.mjs", "OpenAPI deterministic postprocess"),
    ("node --check scripts/check-openapi-api-rules.mjs", "OpenAPI API-rule syntax check"),
    ("node scripts/check-openapi-api-rules.mjs --selftest", "OpenAPI API-rule selftest"),
    ("node scripts/check-openapi-api-rules.mjs", "OpenAPI API-rule repo scan"),
    ("node scripts/sdk-codegen-postprocess.mjs", "SDK deterministic postprocess"),
    (
        "git diff --quiet -- sdk/php/gen sdk/go/gen sdk/typescript/gen sdk/python/gen sdk/java/gen sdk/csharp/gen api",
        "SDK/API generated-output drift diff",
    ),
    (
        "git diff -- sdk/php/gen sdk/go/gen sdk/typescript/gen sdk/python/gen sdk/java/gen sdk/csharp/gen api",
        "SDK/API generated-output diff diagnostic",
    ),
    ("Authn/Authz inventory drift (Phase 0A)", "authn/authz inventory drift step"),
    ("node scripts/generate-authn-authz-inventory.mjs", "authn/authz inventory generator"),
    (
        "git diff --quiet -- docs/generated/authn-authz-rpc-inventory.md docs/generated/authn-authz-sensitive-fields.md",
        "authn/authz generated inventory drift diff",
    ),
)

CI_SMOKE_LOAD_GATE_REQUIREMENTS = (
    ("build-broker:", "build-broker job"),
    ("needs: quick-gate", "build-broker waits for quick-gate"),
    ("cache-key: build-broker-live", "build-broker Rust cache key"),
    (
        'cargo build --locked --bin udb --no-default-features --features "${LIVE_BROKER_FEATURES}"',
        "single broker build command",
    ),
    ("cp target/debug/udb artifact/udb", "broker artifact staging"),
    ("name: udb-broker-debug", "broker artifact name"),
    ("if-no-files-found: error", "broker artifact required"),
    ("smoke:", "smoke job"),
    ("needs: build-broker", "smoke consumes build-broker artifact"),
    ("postgres:16-alpine", "smoke Postgres service"),
    ("UDB_STARTUP_DRY_RUN: \"true\"", "smoke startup dry-run env"),
    ("actions/download-artifact@v4", "smoke downloads broker artifact"),
    ("path: target/debug", "smoke artifact download path"),
    ("./.github/actions/launch-broker", "smoke launches shared broker"),
    ("grpc-addr: 127.0.0.1:50051", "smoke broker address"),
    ("Verify reflection surface", "reflection smoke step"),
    ("grep -q '^udb.services.v1.DataBroker$'", "DataBroker reflection assertion"),
    ("grep -q 'rpc GetHealthReport'", "GetHealthReport reflection assertion"),
    ("grep -q 'rpc LookupMessageSchema'", "LookupMessageSchema reflection assertion"),
    ("Run native load smoke + p99 regression gate", "native load smoke step"),
    ("bash scripts/native-load-test.sh | tee /tmp/native-load.txt", "native load smoke command"),
    ("load_status=${PIPESTATUS[0]}", "native load status capture"),
    ("python scripts/native_load_gate.py", "native load p99 gate"),
    ("--input /tmp/native-load.txt", "native load gate input"),
    ("--baseline scripts/native_load_smoke_baseline.json", "native load baseline"),
    ("--max-regression 15", "native load regression budget"),
    ("Upload load summary", "native load artifact upload step"),
    ("name: native-load-smoke", "native load artifact name"),
    ("path: /tmp/native-load.txt", "native load artifact path"),
    ("Stop broker", "broker cleanup step"),
)

NATIVE_LOAD_REQUIRED_CASES = (
    "storage register upload",
    "storage finalize upload",
    "storage list objects (ListFiles)",
    "asset list",
    "asset start pipeline",
    "asset complete step",
    "webrtc list rooms",
    "webrtc join room",
    "webrtc signal fan-out",
    "cdc stream admission",
    "cdc dlq throughput (rejected events)",
    "policy revision read",
    "policy distribution push fan-out (StreamResources)",
)

NATIVE_LOAD_CASE_RE = re.compile(r'^\s*run_case\s+"([^"]+)"', re.MULTILINE)

CI_NATIVE_INTEGRATION_REQUIREMENTS = (
    ("native-integration:", "native-integration job"),
    ("timeout-minutes: 75", "native-integration timeout"),
    ("Reclaim runner disk for live backend stack", "native-integration runner disk reclaim step"),
    ("docker system prune -af --volumes", "native-integration Docker storage prune"),
    ("cache-key: native-integration", "native-integration Rust cache key"),
    (
        "docker compose -f docker-compose.integration.yml up -d --wait postgres kafka redis memcached qdrant minio",
        "integration stack startup",
    ),
    (
        "docker compose -f docker-compose.canonical.yml up -d --wait mysql mssql mongodb cassandra neo4j clickhouse elasticsearch weaviate",
        "canonical stack startup",
    ),
    ("Start integration stack while compiling tests", "native integration overlap step"),
    ("Start canonical-store stack", "canonical stack deferred startup step"),
    ("integration_stack_pid=$!", "integration stack background pid capture"),
    ('wait "$integration_stack_pid"', "integration stack wait"),
    ("native/integration compile preflight failed", "compile preflight status check"),
    ("Initialize SQL Server database", "SQL Server database bootstrap step"),
    ("IF DB_ID(N'udb') IS NULL CREATE DATABASE [udb];", "SQL Server udb database bootstrap"),
    ("curl -fsS http://127.0.0.1:58080/v1/.well-known/ready", "Weaviate readiness gate"),
    ("rs.initiate", "MongoDB replica-set init"),
    ("udb.authn.user.registered.v1 udb.notification.sent.v1", "native event topic precreate list"),
    ("kafka-topics.sh --create --if-not-exists", "Kafka topic creation command"),
    ("cargo test --locked --no-run --lib --test integration_tests --test runtime_live_backends", "native/integration compile preflight"),
    ("mc mb --ignore-existing local/udb-storage", "MinIO bucket bootstrap"),
    ("Native service live tests", "native service live-test step"),
    ('UDB_LIVE_AUTH_TESTS: "1"', "native service live-test env gate"),
    ("cargo test --locked --lib -- --ignored --nocapture --test-threads=1", "native service ignored live command"),
    ("Canonical store live conformance", "canonical conformance step"),
    ("canonical_store::conformance_live_tests", "canonical conformance target"),
    ("UDB_MYSQL_DSN", "canonical MySQL DSN"),
    ("UDB_MSSQL_DSN", "canonical MSSQL DSN"),
    ("UDB_MONGODB_DSN", "canonical MongoDB DSN"),
    ("UDB_CASSANDRA_DSN", "canonical Cassandra DSN"),
    ("UDB_NEO4J_DSN", "canonical Neo4j DSN"),
    ("UDB_CLICKHOUSE_DSN", "canonical ClickHouse DSN"),
    ("UDB_ELASTIC_DSN", "canonical Elasticsearch DSN"),
    ("Integration harness (CDC, sagas, backends)", "integration harness step"),
    ('UDB_INTEGRATION_TESTS: "1"', "integration harness env gate"),
    ("cargo test --locked --test integration_tests --test runtime_live_backends -- --ignored --nocapture", "integration ignored live command"),
    ("Dump stack logs on failure", "failure diagnostics step"),
    ("docker compose -f docker-compose.integration.yml logs --no-color --tail=200", "integration logs"),
    ("docker compose -f docker-compose.canonical.yml logs --no-color --tail=200", "canonical logs"),
    ("Stop integration stacks", "always-run stack cleanup step"),
    ("docker compose -f docker-compose.integration.yml down -v --remove-orphans", "integration stack teardown"),
    ("docker compose -f docker-compose.canonical.yml down -v --remove-orphans", "canonical stack teardown"),
)

BENCHMARK_WORKFLOW_REQUIREMENTS = (
    ("workflow_call:", "benchmark reusable workflow trigger"),
    ("release-tag:", "release tag workflow input"),
    ("release-asset:", "release asset workflow input"),
    ("default: udb-linux-amd64-full", "default full Linux release asset"),
    ("Resolve release binary (perf)", "release binary resolution step"),
    ("gh release view", "release existence check"),
    ("gh release download", "release asset download command"),
    ('--pattern "${RELEASE_ASSET}"', "configured release asset download pattern"),
    ("bench-output/bin", "benchmark binary staging directory"),
    ('chmod +x "bench-output/bin/${RELEASE_ASSET}"', "release binary executable bit"),
    ("UDB_BENCH_RELEASE_TAG", "benchmark release tag metadata"),
    ("UDB_BENCH_RELEASE_ASSET", "benchmark release asset metadata"),
    ("UDB_BENCH_RELEASE_URL", "benchmark release URL metadata"),
    ("UDB_BENCH_BIN", "benchmark binary env path"),
    ("Resolve broker binary path", "benchmark broker binary path step"),
    ("Start backends", "benchmark backend startup step"),
    ("Write broker env", "benchmark broker env step"),
    ('echo "UDB_LIVE_PERF=1" >> "$GITHUB_ENV"', "benchmark perf opt-in"),
    ("Prepare per-SDK reset script", "per-SDK reset script step"),
    ("Collect benchmark JSON", "benchmark collection step"),
    ("python scripts/collect_sdk_bench_results.py", "benchmark collector command"),
    ("Upload benchmark report artifact", "benchmark artifact upload step"),
    ("name: sdk-benchmark-results", "benchmark artifact name"),
    ("docs/site/bench-results.json", "benchmark JSON artifact path"),
    ("bench-output/logs/**", "benchmark logs artifact path"),
    ("bench-output/status/**", "benchmark status artifact path"),
    ("Fail on benchmark failures", "benchmark final failure gate"),
    ("python scripts/collect_sdk_bench_results.py --gate docs/site/bench-results.json", "central benchmark failure gate command"),
    ("Stop broker and backends", "benchmark cleanup step"),
)

BENCHMARK_ORCHESTRATOR_REQUIREMENTS = (
    ("workflow_dispatch:", "manual benchmark trigger"),
    ("release_tag:", "manual release tag input"),
    ("release_asset:", "manual release asset input"),
    ('default: "udb-linux-amd64-full"', "manual default release asset"),
    ("workflow_run:", "post-release benchmark trigger"),
    ('workflows: ["Release"]', "top-level Release completion trigger"),
    ("types: [completed]", "release-completed event filter"),
    ("permissions:\n  contents: read", "read-only benchmark permissions"),
    ("Confirm release benchmark is gated", "release benchmark validation step"),
    ("github.event.workflow_run.conclusion == 'success'", "successful release gate"),
    ("startsWith(github.event.workflow_run.head_branch, 'v')", "release tag branch gate"),
    ("uses: ./.github/workflows/_live-sdk-suite.yml", "reusable live SDK suite call"),
    ("release-tag: ${{ github.event.workflow_run.head_branch || inputs.release_tag || 'latest' }}", "release tag handoff"),
    ("release-asset: ${{ inputs.release_asset || 'udb-linux-amd64-full' }}", "release asset handoff"),
    ("secrets: inherit", "benchmark reusable secrets handoff"),
)

BENCHMARK_ORCHESTRATOR_TRIGGER_PATHS = (
    (("proto/**", "proto/udb/core/**"), "proto/API source trigger path"),
    (("api/**",), "published API trigger path"),
    (("src/runtime/descriptor_manifest.rs",), "descriptor manifest trigger path"),
    (("src/runtime/sdk_manifest.rs",), "SDK manifest trigger path"),
    (("src/cli/sdk_gen.rs",), "SDK generator trigger path"),
    (("sdk-templates/**",), "SDK template trigger path"),
    (("scripts/openapi-postprocess.mjs",), "OpenAPI postprocess trigger path"),
    (("scripts/collect_sdk_bench_results.py",), "benchmark collector trigger path"),
    (("scripts/gen-bench-bodies-skeleton.mjs",), "benchmark body skeleton trigger path"),
    (("scripts/gen-bench-bodies-json.mjs",), "benchmark body parser trigger path"),
    (("docs/bench-bodies/**",), "benchmark body source trigger path"),
    (("docs/site/benchmarks.html",), "benchmark page trigger path"),
    (("docs/site/benchmarks.js",), "benchmark script trigger path"),
    (("docs/site/README.md",), "site benchmark README trigger path"),
)

PAGES_PLAYGROUND_REQUIREMENTS = (
    ("workflow_dispatch:", "manual Pages trigger"),
    ('workflows: ["Benchmark · SDKs"]', "benchmark-completion Pages trigger"),
    ("docs/site/**", "site source trigger path"),
    ("docs/assets/**", "site asset trigger path"),
    ("api/**", "published API trigger path"),
    ("scripts/playground_wasm_smoke.mjs", "playground smoke trigger path"),
    ("crates/udb-wasm/**", "wasm crate trigger path"),
    ("crates/udb-portable/**", "portable parser trigger path"),
    ("src/parser/**", "parser trigger path"),
    ("pages: write", "Pages write permission"),
    ("id-token: write", "Pages OIDC permission"),
    ("actions: read", "benchmark artifact read permission"),
    ("Pull latest benchmark results into the site", "benchmark result handoff step"),
    ("GH_TOKEN: ${{ github.token }}", "benchmark artifact download token"),
    ("TRIGGER_RUN_ID: ${{ github.event.workflow_run.id }}", "benchmark workflow_run id handoff"),
    ('gh run download "${TRIGGER_RUN_ID}"', "benchmark artifact download command"),
    ('--repo "${GITHUB_REPOSITORY}"', "benchmark artifact repository scope"),
    ("--name sdk-benchmark-results", "benchmark artifact name"),
    ("--dir bench-artifact", "benchmark artifact staging directory"),
    ("bench-artifact/docs/site/bench-results.json", "benchmark artifact JSON source path"),
    ("docs/site/bench-results.json", "site benchmark JSON destination"),
    ("got_fresh=0", "benchmark fallback state initialization"),
    ("got_fresh=1", "benchmark fresh artifact state"),
    ("keeping committed docs/site/bench-results.json", "no-stale-republish benchmark fallback"),
    ("Build UDB's parser to WebAssembly", "fresh wasm build step"),
    ("rustup target add wasm32-unknown-unknown", "wasm target install"),
    ("cargo build -p udb-wasm --release --target wasm32-unknown-unknown", "real udb-wasm build"),
    ("cp -v docs/assets/*.svg docs/site/assets/", "site asset sync command"),
    ("cp -v api/*.json docs/site/api/", "site API sync command"),
    ("target/wasm32-unknown-unknown/release/udb_wasm.wasm docs/site/udb.wasm", "fresh wasm artifact copy"),
    ("Verify playground parses current editor input", "playground current-input smoke step"),
    ("node scripts/playground_wasm_smoke.mjs docs/site/udb.wasm", "playground smoke command"),
    ("Verify site artifact contract", "site artifact contract step"),
    ("test -f docs/site/index.html", "published index artifact check"),
    ("test -f docs/site/playground.html", "published playground artifact check"),
    ("test -f docs/site/architecture.html", "published architecture artifact check"),
    ("test -f docs/site/data-plane.html", "published data-plane artifact check"),
    ("test -f docs/site/control-plane.html", "published control-plane artifact check"),
    ("test -f docs/site/security.html", "published security artifact check"),
    ("test -f docs/site/enterprise.html", "published enterprise artifact check"),
    ("test -f docs/site/sdks.html", "published SDKs artifact check"),
    ("test -f docs/site/styles.css", "published stylesheet artifact check"),
    ("test -f docs/site/app.js", "published app script artifact check"),
    ("test -f docs/site/playground.js", "published playground script artifact check"),
    ("test -f docs/site/udb.wasm", "published wasm artifact check"),
    ("test -f docs/site/assets/udb_logo.svg", "published logo artifact check"),
    ("test -f docs/site/benchmarks.html", "published benchmark page artifact check"),
    ("test -f docs/site/benchmarks.js", "published benchmark script artifact check"),
    ("test -f docs/site/bench-results.json", "published benchmark JSON artifact check"),
    ('bench = json.loads(Path("docs/site/bench-results.json").read_text())', "benchmark JSON validation parse"),
    ('required_extensions = [', "Swagger descriptor extension validation list"),
    ('"x-udb-sdk-alias"', "Swagger SDK alias extension validation"),
    ('"x-udb-operation-kind"', "Swagger operation-kind extension validation"),
    ('assert operations, "published Swagger JSON has no operations"', "Swagger operation inventory validation"),
    ('assert not re.match(r"^[A-Za-z0-9]+Service_[A-Za-z0-9]+$", operation_id)', "Swagger generated operationId rejection"),
    ('missing_extensions = [key for key in required_extensions if key not in operation]', "Swagger descriptor extension missing check"),
    ('"failed_rpc_count" in summary', "benchmark failed-RPC summary validation"),
    ('isinstance(bench.get("sdks"), list)', "benchmark SDK list validation"),
    ('isinstance(bench.get("history"), list)', "benchmark history validation"),
    ('full_rows = []', "benchmark full-RPC row collection"),
    ('rows = sdk.get("full_rpcs") or []', "benchmark full-RPC row source"),
    ('row.setdefault("wire_api", wire_api)', "benchmark legacy wire identity normalizer"),
    ('row.setdefault("api_alias", "")', "benchmark legacy alias normalizer"),
    ('row.setdefault("operation_id", "")', "benchmark legacy operationId normalizer"),
    ('"wire_api" not in row or "api_alias" not in row or "operation_id" not in row', "benchmark public identity row validation"),
    ('benchmark full_rpcs rows must include wire_api, api_alias, and operation_id', "benchmark public identity failure"),
    ('row.get("operation_id") or row.get("api_alias") or row.get("wire_api")', "benchmark public identity fallback check"),
    ('benchmark full_rpcs rows lack public API identity', "benchmark public identity non-empty check"),
    ("from html.parser import HTMLParser", "HTML local-ref parser import"),
    ("from urllib.parse import urlparse", "HTML local-ref URL parser import"),
    ('site.glob("*.html")', "HTML artifact crawl"),
    ('key in {"href", "src"}', "HTML local href/src scan"),
    ('missing.append(f"{html.name}: missing local ref: {ref}")', "HTML missing local-ref failure"),
    ('assert not missing, "\\n".join(missing)', "HTML local-ref hard failure"),
    ("test -f docs/site/api.html", "published API page artifact check"),
    ("test -f docs/site/api/udb-broker.swagger.json", "published Swagger artifact check"),
    ('swagger.get("swagger") == "2.0"', "Swagger 2.0 artifact validation"),
    ('swagger.get("paths")', "Swagger paths artifact validation"),
    ("actions/upload-pages-artifact@v3", "Pages artifact upload"),
    ("actions/deploy-pages@v4", "Pages deployment action"),
)

PAGES_PLAYGROUND_SCRIPT_REQUIREMENTS = (
    ('const mobileProto = invoiceProto.replaceAll("email", "mobile");', "current-input edit fixture"),
    ('col.field === "mobile" && col.column === "mobile"', "edited mobile column assertion"),
    ('col.field === "email" || col.column === "email"', "stale email rejection"),
    ("invoice.checksum !== mobile.checksum", "checksum change assertion"),
    ("WebAssembly.instantiate(wasmBytes, imports)", "same WASM instantiation path"),
    ('Array.isArray(broken.diagnostics) && broken.diagnostics.length > 0', "malformed proto diagnostics assertion"),
)

PAGES_PLAYGROUND_HTML_REQUIREMENTS = (
    ('./playground.js?v=20260701-current-editor', "current playground script cache key"),
)

PAGES_PLAYGROUND_JS_REQUIREMENTS = (
    ('var WASM_ASSET_VERSION = "20260701-current-editor";', "current wasm asset cache key"),
)

PAGES_SITE_README_REQUIREMENTS = (
    ("authoring surface is static", "static authoring surface wording"),
    ("publish-time", "publish-time contract wording"),
    ("contract work", "publish-time contract work wording"),
    ("rebuilds `udb.wasm`", "README fresh WASM build contract"),
    ("syncs shared assets and Swagger JSON", "README asset/API sync contract"),
    ("latest benchmark artifact", "README benchmark artifact contract"),
    ("complete site", "README artifact validation contract"),
    ("before deploy", "README deploy validation boundary"),
    ("`benchmarks.js`", "README benchmark script inventory"),
    ("`sdk-benchmark-results` artifact", "README benchmark artifact name"),
    ("falls back to the already-published dashboard JSON", "README benchmark fallback contract"),
    ("current-editor WASM smoke", "README playground smoke contract"),
    ("verifies every first-class page/script/data artifact", "README full artifact contract"),
    ("HTML `href`/`src`", "README local-ref crawl contract"),
    ("before upload", "README pre-upload contract"),
)

MARKDOWN_LINK_GUARD_REQUIREMENTS = (
    ('"private"', "private research directory exclusion"),
    ("function checkRepo(repoRoot)", "markdown link reusable checker"),
    ("function stripTarget(raw)", "markdown link target normalization"),
    ("function isExternal(target)", "external link skip helper"),
    ("function existsFrom(baseFile, rawTarget)", "local link existence helper"),
    ("function stripFencedCodeBlocks(markdown)", "fenced code block stripping helper"),
    ("stripFencedCodeBlocks(markdown)", "fenced code block strip usage"),
    ("function collectLinks(markdown)", "markdown link collector"),
    ("function runSelftest()", "markdown link selftest function"),
    ("mkdtempSync", "markdown link temp fixture"),
    ("private/research/broken.md", "markdown link private fixture"),
    ("docs/code.md", "markdown link fenced-code fixture"),
    ("missing local link was not caught", "markdown link missing-link negative"),
    ('process.argv.includes("--selftest")', "markdown link selftest CLI"),
    ("walk(repoRoot, markdownFiles)", "repo markdown walk"),
    ("process.exit(1)", "broken markdown link hard failure"),
)

ENTERPRISE_READINESS_GUARD_REQUIREMENTS = (
    ("function checkRepo(repo)", "enterprise readiness reusable checker"),
    ("requiredFiles", "enterprise readiness required file inventory"),
    ("requiredCiSnippets", "enterprise readiness CI snippet inventory"),
    ("requiredRunbookTerms", "enterprise readiness runbook term inventory"),
    ("requiredCodeEvidence", "enterprise readiness code evidence inventory"),
    ("function runSelftest()", "enterprise readiness selftest function"),
    ("mkdtempSync", "enterprise readiness temp fixture"),
    ("buildFixture(root)", "enterprise readiness good fixture"),
    ("missing runbook term was not caught", "enterprise readiness runbook negative"),
    ("missing code evidence was not caught", "enterprise readiness code-evidence negative"),
    ('process.argv.includes("--selftest")', "enterprise readiness selftest CLI"),
    ("checkRepo(process.cwd())", "enterprise readiness repo scan entrypoint"),
    ("process.exit(1)", "enterprise readiness hard failure"),
)

OPENAPI_API_RULE_GUARD_REQUIREMENTS = (
    ("function checkSwagger(swagger)", "OpenAPI reusable rule checker"),
    ("retiredBetaRoutes", "retired beta route inventory"),
    ("descriptorOwnedExtensions", "descriptor-owned extension inventory"),
    ("function validateRestMediaBoundary(errors, swagger)", "REST JSON media-boundary validator"),
    ("root.produces must include application/json", "REST JSON response content-type rejection"),
    ("grpcHttpStatusMap", "REST gRPC-to-HTTP error status inventory"),
    ("requiredApiErrorFields", "REST ApiError public field inventory"),
    ("function isForbiddenSuccessWrapper(schema)", "REST success-wrapper rejection helper"),
    ("function restBoundaryResponses", "REST boundary selftest fixture helper"),
    ("function normalizedOperationId(id)", "SDK-normalized operationId helper"),
    ("function isKebabLiteral(segment)", "kebab path literal helper"),
    ("function isLowerCamel(value)", "lowerCamel helper"),
    ("betaStabilityClaim", "beta stability wording denylist"),
    ("function scanBetaStabilityClaim(errors, where, value)", "beta stability wording scanner"),
    ("function runSelftest()", "OpenAPI API-rule selftest function"),
    ("retired route regression was not caught", "retired route negative fixture"),
    ("path/operation naming regressions were not caught", "path and operation negative fixture"),
    ("custom action case regression was not caught", "custom action negative fixture"),
    ("missing descriptor extension was not caught", "descriptor extension negative fixture"),
    ("SDK-normalized operationId collision was not caught", "operationId collision negative fixture"),
    ("query dispatch parameter was not caught", "query dispatch negative fixture"),
    ("beta stability wording was not caught", "beta stability wording negative fixture"),
    ("stale rpcStatus default response was not caught", "REST stale rpcStatus negative fixture"),
    ("missing NOT_FOUND->404 response was not caught", "REST NOT_FOUND-to-404 negative fixture"),
    ("success envelope response was not caught", "REST success-wrapper negative fixture"),
    ("REST content-type regression was not caught", "REST content-type negative fixture"),
    ('process.argv.includes(\'--selftest\')', "OpenAPI API-rule selftest CLI"),
)

HTTP_API_STYLE_GUARD_REQUIREMENTS = (
    ("function inventory(root = repoRoot)", "HTTP API route inventory extractor"),
    ("function protoHttpInventory(root = repoRoot)", "proto HTTP route inventory extractor"),
    ("function protoApiModel(root = repoRoot)", "proto API model extractor"),
    ("function resourceIdentityContractRows(root = repoRoot)", "resource identity contract inventory extractor"),
    ("function paginationContractRows(root = repoRoot)", "pagination contract inventory extractor"),
    ("function queryUpdateContractRows(root = repoRoot)", "query/update contract inventory extractor"),
    ("function sourceIndex(root)", "source proto index"),
    ("function routeFlags(route, allow)", "route-style rule evaluator"),
    ("scripts/http-api-style.allow.json", "explicit route-style allowlist path"),
    ("allowedLiteralSegments", "literal exception allowlist"),
    ("allowedDeepPaths", "deep-path exception allowlist"),
    ("allowedCommandEndpoints", "command endpoint exception allowlist"),
    ("snake_case_literal", "snake_case literal rule"),
    ("slash_verb", "slash verb rule"),
    ("slash_read_action", "slash read action rule"),
    ("pseudo_read_action", "pseudo-read action rule"),
    ("singular_collection", "singular collection rule"),
    ("deep_path_review", "deep path review rule"),
    ("route inventory mismatch: native-contract HTTP operations=", "generated/proto operation-count invariant"),
    ("function buildExceptionReport(root = repoRoot)", "API exception report builder"),
    ("function writeExceptionReport(root = repoRoot)", "API exception report writer"),
    ("docs/generated/http-api-style-exceptions.json", "machine-readable API exception report path"),
    ("docs/generated/http-api-style-exceptions.md", "Markdown API exception report path"),
    ("resource_identity_contract_exceptions_by_rule", "resource identity contract exception report section"),
    ("pagination_contract_exceptions_by_rule", "pagination contract exception report section"),
    ("query_update_contract_exceptions_by_rule", "query/update contract exception report section"),
    ("not_yet_reported_by_this_guard", "API report uncovered-rule disclosure"),
    ("report did not group pseudo-read exception", "API exception report selftest"),
    ("missing path identity field regression was not caught", "path identity negative fixture"),
    ("missing response identity regression was not caught", "response resource identity negative fixture"),
    ("undocumented user-chosen ID regression was not caught", "user-chosen ID negative fixture"),
    ("legacy offset pagination regression was not caught", "pagination legacy-offset negative fixture"),
    ("missing next_page_token regression was not caught", "pagination response-token negative fixture"),
    ("undocumented filter regression was not caught", "query filter allowlist negative fixture"),
    ("missing update_mask regression was not caught", "PATCH update_mask negative fixture"),
    ("api_keys snake_case regression was not caught", "snake_case negative fixture"),
    ("slash finalize regression was not caught", "slash verb negative fixture"),
    ("slash download-url regression was not caught", "slash read action negative fixture"),
    ("pseudo-read action regression was not caught", "pseudo-read action negative fixture"),
    ("SCIM allowlist failed", "SCIM allowlist selftest"),
    ("JWKS allowlist failed", "JWKS allowlist selftest"),
    ("args.has('--selftest')", "HTTP API style selftest CLI"),
    ("args.has('--advisory')", "HTTP API style advisory mode"),
    ("args.has('--source-only')", "HTTP API style source-only audit mode"),
    ("args.has('--write-report')", "HTTP API style report generation mode"),
    ("args.has('--resource-identity-contract')", "HTTP API resource identity contract mode"),
    ("args.has('--pagination-contract')", "HTTP API pagination contract mode"),
    ("args.has('--query-update-contract')", "HTTP API query/update contract mode"),
)

REST_ROUTE_GATEWAY_SMOKE_REQUIREMENTS = (
    ("EXPECTED_ROUTE_FAMILY_NAMES", "REST route migration family inventory"),
    ("EXPECTED_CANONICAL_ROUTE_COUNT = 46", "REST route canonical inventory count"),
    ("EXPECTED_RETIRED_ROUTE_COUNT = 44", "REST route retired inventory count"),
    ("ROUTE_REDIRECT_STATUSES = set(range(300, 400))", "REST route redirect status set"),
    ("ROUTE_SERVER_ERROR_STATUSES = set(range(500, 600))", "REST route server-error status set"),
    ("ROUTE_NEGOTIATION_FAILURE_STATUSES = {406, 415}", "REST route JSON negotiation failure status set"),
    ("class _NoRedirectHandler", "REST live no-redirect opener"),
    ("NO_REDIRECT_OPENER.open", "REST live no-redirect request path"),
    ("CANONICAL_GRPC_ERROR_CODES", "REST canonical gRPC error-code allowlist"),
    ("GRPC_HTTP_STATUS_FOR_CODE", "REST canonical gRPC-to-HTTP status map"),
    ("LIVE_BOUNDARY_HTTP_METHODS", "REST live boundary method allowlist"),
    ("def validate_route_inventory(", "REST route inventory validator"),
    ("duplicate REST route migration OpenAPI inventory entry", "REST route OpenAPI inventory uniqueness validator"),
    ("duplicate REST route migration live inventory entry", "REST route live inventory uniqueness validator"),
    ("def _path_contains_dot_segment(", "REST route path dot-segment helper"),
    ("def _contains_control_character(", "REST control-character helper"),
    ("REST route {path_field} must not contain dot-segments", "REST route inventory dot-segment validator"),
    ("REST route {path_field} must not include surrounding whitespace", "REST route inventory path surrounding-whitespace validator"),
    ("REST route {path_field} must start with /", "REST route inventory path-root validator"),
    ("REST route {path_field} must not contain control characters", "REST route inventory path control-character validator"),
    ("REST route {path_field} must not contain whitespace", "REST route inventory path embedded-whitespace validator"),
    ("REST route {path_field} must be a path without authority, query, or fragment", "REST route inventory path query/authority validator"),
    ("REST route method must not include surrounding whitespace", "REST route inventory method whitespace validator"),
    ("REST route method must not contain control characters", "REST route inventory method control-character validator"),
    ("REST route method must be uppercase", "REST route inventory method uppercase validator"),
    ("REST route method must be one of", "REST route inventory method allowlist validator"),
    ("REST route operation_id must not include surrounding whitespace", "REST route operationId token whitespace validator"),
    ("REST route sdk_alias must not include whitespace", "REST route SDK alias embedded-whitespace validator"),
    ("operation_id: str | None = None", "REST route expected operationId inventory field"),
    ("sdk_alias: str | None = None", "REST route expected SDK alias inventory field"),
    ("def check_live_gateway(", "live canonical/retired route-family checker"),
    ("def check_live_boundary(", "live REST success/error boundary checker"),
    ("def _check_route_family_error_shape(", "live route-family ApiError checker"),
    ("def _api_error_body(", "REST route-family selftest ApiError fixture"),
    ("MAX_REST_RESPONSE_BYTES = 1_048_576", "REST live response byte ceiling constant"),
    ("def _read_limited_response_body(", "REST live bounded response reader"),
    ("REST response body must be bytes", "REST live response body bytes validator"),
    ("REST response body must be <=", "REST live oversized response validator"),
    ("def _response_content_type(", "REST live Content-Type multiplicity validator"),
    ("REST response Content-Type header could not be read", "REST live Content-Type read validator"),
    ("REST response must include exactly one Content-Type header", "REST live duplicate Content-Type validator"),
    ("REST response Content-Type header must be a string", "REST live Content-Type value type validator"),
    ("Content-Type header must not include surrounding whitespace", "REST Content-Type surrounding-whitespace validator"),
    ("Content-Type header must not contain control characters", "REST Content-Type control-character validator"),
    ("object_pairs_hook=_reject_duplicate_json_keys", "REST live duplicate response JSON key parser"),
    ("parse_constant=_reject_non_finite_json_constant", "REST live non-standard JSON constant parser"),
    ("response JSON must not contain duplicate key", "REST live duplicate response JSON key validator"),
    ("response JSON must not contain non-standard constant", "REST live non-standard JSON constant validator"),
    ("Content-Type header must be a string", "REST direct decoder Content-Type type validator"),
    ("response body must be bytes", "REST direct decoder response body type validator"),
    ("def validate_boundary_inputs(", "REST boundary proof input validator"),
    ("def parse_validated_boundary_routes(", "REST boundary validated-route parser"),
    ("invalid REST boundary proof should not be reparsed after validation failure", "REST boundary validation short-circuit negative fixture"),
    ("def validate_base_url(", "REST live base URL validator"),
    ("REST_TIMEOUT_DECIMAL_PATTERN", "REST timeout decimal pattern"),
    ("def normalize_timeout_seconds(", "REST timeout normalizer"),
    ("def validate_timeout_seconds(", "REST live timeout validator"),
    ("REST live timeout must be a finite number of seconds", "REST finite timeout validator"),
    ("REST live timeout must not include surrounding whitespace", "REST timeout surrounding-whitespace validator"),
    ("REST live timeout must be a positive decimal number of seconds", "REST timeout decimal-token validator"),
    ("REST live timeout must be greater than 0 seconds", "REST positive timeout validator"),
    ("REST live timeout must be <= 120 seconds", "REST timeout ceiling validator"),
    ("def parse_headers(", "REST live header parser"),
    ("HTTP_HEADER_NAME_CHARS", "REST live header-name character allowlist"),
    ("MAX_LIVE_HEADER_COUNT = 32", "REST live header count ceiling constant"),
    ("MAX_LIVE_HEADER_VALUE_BYTES = 8_192", "REST live header value byte ceiling constant"),
    ("PROOF_MANAGED_REQUEST_HEADERS = {\"accept\", \"content-type\"}", "REST proof-managed request header set"),
    ("header name must be an HTTP token", "REST live header-name validator"),
    ("header name must not include surrounding whitespace", "REST live header-name surrounding-whitespace validator"),
    ("header value is empty", "REST live header value validator"),
    ("header value must not include surrounding whitespace", "REST live header value surrounding-whitespace validator"),
    ("header value must not contain control characters", "REST live header value control-character validator"),
    ("header value must be <=", "REST live header value byte ceiling validator"),
    ("live headers must be <=", "REST live header count ceiling validator"),
    ("duplicate live header", "REST duplicate live header validator"),
    ("managed by the REST proof harness", "REST proof-managed header override validator"),
    ("def _check_api_error_public_shape(", "REST ApiError public field shape validator"),
    ("API_ERROR_ALLOWED_FIELDS = frozenset(API_ERROR_FIELDS)", "REST ApiError allowed-field lock"),
    ("API_ERROR_FIELD_VIOLATION_FIELDS = frozenset((\"field\", \"description\"))", "REST ApiError fieldViolation allowed-field lock"),
    ("ApiError must not expose undocumented fields", "REST ApiError undocumented-field rejection"),
    ("ApiError.fieldViolations[{index}] must not expose undocumented fields", "REST ApiError fieldViolation undocumented-field rejection"),
    ("--require-route-family-proof", "live route-family proof CLI option"),
    ("--require-boundary-proof", "live REST boundary proof CLI option"),
    ("--boundary-success", "live success boundary CLI option"),
    ("--boundary-error", "live error boundary CLI option"),
    ("--boundary-error-code", "live error canonical code CLI option"),
    ("--evidence-out", "live REST evidence JSON CLI option"),
    ("def write_evidence(", "REST evidence writer"),
    ("EVIDENCE_SCHEMA_VERSION = 1", "REST evidence schema version"),
    ("canonical_routes_probed", "REST evidence canonical route count"),
    ("retired_routes_probed", "REST evidence retired route count"),
    ("success body must be the bare typed JSON body", "REST success envelope rejection"),
    ("success body must be a non-empty typed JSON object", "REST success empty-object rejection"),
    ("success body must not expose a top-level success flag", "REST success status-flag rejection"),
    ("success body must not be the ApiError shape", "REST success ApiError-shape rejection"),
    ("error body must expose ApiError fields", "REST ApiError body rejection"),
    ("ApiError.code", "REST ApiError canonical code consistency check"),
    ("ApiError.code must not include surrounding whitespace", "REST ApiError code surrounding-whitespace check"),
    ("ApiError.code must not contain control characters", "REST ApiError code control-character check"),
    ("ApiError.code must not include whitespace", "REST ApiError code embedded-whitespace check"),
    ("ApiError.code must be a canonical gRPC error code", "REST ApiError canonical code allowlist check"),
    ("maps to HTTP", "REST ApiError canonical code/status mapping check"),
    ("ApiError.httpStatusCode", "REST ApiError HTTP status consistency check"),
    ("ApiError.httpStatusCode must be an integer", "REST ApiError HTTP status integer shape check"),
    ("ApiError.message must be a non-empty string", "REST ApiError message shape check"),
    ("ApiError.message must not include surrounding whitespace", "REST ApiError message surrounding-whitespace check"),
    ("MAX_API_ERROR_STRING_BYTES = 8_192", "REST ApiError string byte ceiling constant"),
    ("ApiError.message must not contain control characters", "REST ApiError message control-character check"),
    ("ApiError.message must be <=", "REST ApiError message byte ceiling check"),
    ("ApiError.retryable must be a boolean", "REST ApiError retryable shape check"),
    ("ApiError.fieldViolations must be an array", "REST ApiError fieldViolations shape check"),
    ("ApiError.fieldViolations must be non-empty for INVALID_ARGUMENT", "REST ApiError validation fieldViolations requirement"),
    ("ApiError.fieldViolations must be empty unless ApiError.code is INVALID_ARGUMENT", "REST ApiError non-validation fieldViolations absence check"),
    ("ApiError.fieldViolations[{index}] must be an object", "REST ApiError fieldViolations entry object check"),
    ("ApiError.fieldViolations[{index}].field must be a non-empty string", "REST ApiError fieldViolations field check"),
    ("ApiError.fieldViolations[{index}].field must not include surrounding whitespace", "REST ApiError fieldViolations field surrounding-whitespace check"),
    ("ApiError.fieldViolations[{index}].field must not include whitespace", "REST ApiError fieldViolations field embedded-whitespace check"),
    (
        "ApiError.fieldViolations[{index}].field must not contain control characters",
        "REST ApiError fieldViolations field control-character check",
    ),
    (
        "ApiError.fieldViolations[{index}].field must be ",
        "REST ApiError fieldViolations field byte ceiling check",
    ),
    ("ApiError.fieldViolations[{index}].description must be a non-empty string", "REST ApiError fieldViolations description check"),
    ("ApiError.fieldViolations[{index}].description must not include surrounding whitespace", "REST ApiError fieldViolations description surrounding-whitespace check"),
    (
        "ApiError.fieldViolations[{index}].description must not contain control characters",
        "REST ApiError fieldViolations description control-character check",
    ),
    (
        "ApiError.fieldViolations[{index}].description must be ",
        "REST ApiError fieldViolations description byte ceiling check",
    ),
    ("Content-Type media type must be application/json", "REST JSON media-type check"),
    ("REST boundary proof requires both --boundary-success and --boundary-error", "paired boundary route validator"),
    ("REST boundary proof requires distinct success and error routes", "distinct boundary route validator"),
    ("REST boundary proof requires --boundary-error-code", "boundary error-code validator"),
    ("REST boundary proof --boundary-error-code must not include surrounding whitespace", "boundary error-code surrounding-whitespace validator"),
    ("REST boundary proof --boundary-error-code must not contain control characters", "boundary error-code control-character validator"),
    ("REST boundary proof --boundary-error-code must not include whitespace", "boundary error-code whitespace validator"),
    ("live route method must be one of", "REST boundary method validator"),
    ("live route method token must not include surrounding whitespace", "REST boundary method-token whitespace validator"),
    ("live route method token must be uppercase", "REST boundary method-token uppercase validator"),
    ("live route must not include surrounding whitespace", "REST boundary route surrounding-whitespace validator"),
    ("live route path token must not include surrounding whitespace", "REST boundary route path-token whitespace validator"),
    ("live route path must not contain control characters", "REST boundary path control-character validator"),
    ("live route path must not contain whitespace", "REST boundary path whitespace validator"),
    ("live route path must not contain dot-segments", "REST boundary path dot-segment validator"),
    ("live route path must be a path without authority, query, or fragment", "REST boundary path query/authority validator"),
    ("REST live base URL must use http or https", "REST base URL scheme validator"),
    ("REST live base URL authority is malformed", "REST base URL malformed authority validator"),
    ("REST live base URL must include a host", "REST base URL host validator"),
    ("REST live base URL must not include userinfo", "REST base URL userinfo validator"),
    ("REST live base URL must not include a path", "REST base URL path validator"),
    ("REST live base URL must not include query or fragment", "REST base URL query/fragment validator"),
    ("REST live base URL must not contain control characters", "REST base URL control-character validator"),
    ("REST live base URL must not contain whitespace", "REST base URL whitespace validator"),
    ("REST live base URL port must be an integer from 1 to 65535", "REST base URL port validator"),
    ("/v1/analytics/pipeline-metrics", "analytics pipeline metrics canonical route proof"),
    ("/v1/assets/steps/{stepId}:complete", "asset complete-step canonical route proof"),
    ("/v1/webrtc/tracks/{trackId}:mute", "WebRTC mute canonical route proof"),
    ("/v1/auth/passwords:reset", "auth password reset canonical route proof"),
    ("/v1/authz/governance/policy-explanations", "authz explanation canonical route proof"),
    ('operation_id="downloadFile"', "storage downloadFile operationId proof"),
    ('sdk_alias="download_file"', "storage download_file SDK alias proof"),
    ("canonical route operationId drift", "OpenAPI operationId drift failure"),
    ("canonical route x-udb-sdk-alias drift", "OpenAPI SDK alias drift failure"),
    ("canonical route returned route-missing status", "canonical route-missing failure"),
    ("canonical route returned redirect status", "canonical route redirect failure"),
    ("canonical route returned server-error status", "canonical route server-error failure"),
    ("canonical route rejected JSON negotiation", "canonical route JSON negotiation failure"),
    ("canonical route client error body must expose ApiError fields", "canonical route-family ApiError body failure"),
    ("ROUTE_NO_BODY_SUCCESS_STATUSES = {204, 205}", "REST route-family no-body status denylist"),
    ("canonical route returned no-body success status", "REST route-family no-body status failure"),
    ("retired route is still served", "retired route served failure"),
    ("REST route migration inventory must cover", "route inventory count failure"),
    ("route inventory spaced operation_id was not caught", "route inventory operationId whitespace negative fixture"),
    ("route inventory embedded-whitespace sdk_alias was not caught", "route inventory SDK alias whitespace negative fixture"),
    ("route inventory family drift was not caught", "route inventory family negative fixture"),
    ("route inventory count drift was not caught", "route inventory count negative fixture"),
    ("route inventory duplicate was not caught", "route inventory duplicate negative fixture"),
    ("route inventory dot-segment was not caught", "route inventory dot-segment negative fixture"),
    ("route inventory query/authority path was not caught", "route inventory query/authority negative fixture"),
    ("route inventory whitespace path was not caught", "route inventory whitespace-path negative fixture"),
    ("route inventory lowercase method was not caught", "route inventory lowercase-method negative fixture"),
    ("route inventory unsupported method was not caught", "route inventory unsupported-method negative fixture"),
    ("stale operationId was not caught", "OpenAPI operationId drift negative fixture"),
    ("stale SDK alias was not caught", "OpenAPI SDK alias drift negative fixture"),
    ("live canonical/retired route families", "live route-family success marker"),
    ("--require-route-family-proof requires --base-url", "route-family proof missing-base-url failure"),
    ("--require-boundary-proof requires --base-url", "boundary proof missing-base-url failure"),
    ("missing required REST boundary proof was not caught", "required boundary proof negative fixture"),
    ("complete required REST live proof was rejected", "complete live proof positive fixture"),
    ("missing live route was not caught", "live canonical route negative fixture"),
    ("redirect live route was not caught", "live canonical redirect negative fixture"),
    ("server-error live route was not caught", "live canonical server-error negative fixture"),
    ("negotiation-failure live route was not caught", "live canonical negotiation-failure negative fixture"),
    ("route-family non-JSON client error was not caught", "live route-family non-JSON error negative fixture"),
    ("route-family incomplete ApiError body was not caught", "live route-family incomplete ApiError negative fixture"),
    ("no-body success live route was not caught", "live canonical no-body success negative fixture"),
    ("served retired route was not caught", "live retired route negative fixture"),
    ("clean REST boundary fixture failed", "REST boundary positive selftest fixture"),
    ("partial REST boundary proof was not caught", "partial boundary negative fixture"),
    ("same REST boundary route proof was not caught", "same boundary route negative fixture"),
    ("missing REST boundary error-code proof was not caught", "missing boundary code negative fixture"),
    ("non-canonical REST boundary error-code proof was not caught", "non-canonical boundary expected-code negative fixture"),
    ("spaced REST boundary error-code proof was not caught", "boundary expected-code surrounding-whitespace negative fixture"),
    ("embedded-whitespace REST boundary error-code proof was not caught", "boundary expected-code embedded-whitespace negative fixture"),
    ("surrounding-whitespace REST boundary route was not caught", "boundary route surrounding-whitespace negative fixture"),
    ("extra-separator REST boundary route was not caught", "boundary route path-token whitespace negative fixture"),
    ("spaced-method REST boundary route was not caught", "boundary route method-token whitespace negative fixture"),
    ("lowercase-method REST boundary route was not caught", "boundary route method-token uppercase negative fixture"),
    ("control-character REST boundary path was not caught", "boundary path control-character negative fixture"),
    ("whitespace REST boundary path was not caught", "boundary path whitespace negative fixture"),
    ("query REST boundary path was not caught", "boundary path query negative fixture"),
    ("authority REST boundary path was not caught", "boundary path authority negative fixture"),
    ("dot-segment REST boundary path was not caught", "boundary path dot-segment negative fixture"),
    ("encoded dot-segment REST boundary path was not caught", "boundary path encoded-dot-segment negative fixture"),
    ("non-HTTP REST base URL was not caught", "REST base URL scheme negative fixture"),
    ("query REST base URL was not caught", "REST base URL query negative fixture"),
    ("whitespace REST base URL was not caught", "REST base URL whitespace negative fixture"),
    ("control-character REST base URL was not caught", "REST base URL control-character negative fixture"),
    ("hostless REST base URL was not caught", "REST base URL host negative fixture"),
    ("empty-host REST base URL was not caught", "REST base URL empty-host negative fixture"),
    ("malformed REST base URL authority was not caught", "REST base URL malformed authority negative fixture"),
    ("userinfo REST base URL was not caught", "REST base URL userinfo negative fixture"),
    ("path-prefixed REST base URL was not caught", "REST base URL path negative fixture"),
    ("non-integer REST base URL port was not caught", "REST base URL non-integer port negative fixture"),
    ("out-of-range REST base URL port was not caught", "REST base URL out-of-range port negative fixture"),
    ("canonical REST timeout string was rejected", "REST canonical timeout string positive fixture"),
    ("padded REST timeout was not caught", "REST padded timeout negative fixture"),
    ("non-decimal REST timeout was not caught", "REST non-decimal timeout negative fixture"),
    ("non-positive REST timeout was not caught", "REST non-positive timeout negative fixture"),
    ("infinite REST timeout was not caught", "REST infinite timeout negative fixture"),
    ("excessive REST timeout was not caught", "REST excessive timeout negative fixture"),
    ("malformed REST header name was not caught", "REST malformed header-name negative fixture"),
    ("spaced REST header name was not caught", "REST spaced header-name negative fixture"),
    ("spaced REST header value was not caught", "REST spaced header-value negative fixture"),
    ("empty REST header value was not caught", "REST empty header negative fixture"),
    ("control-character REST header value was not caught", "REST header control-character negative fixture"),
    ("oversized REST header value was not caught", "REST oversized header value negative fixture"),
    ("excessive REST header count was not caught", "REST excessive header count negative fixture"),
    ("duplicate REST header was not caught", "REST duplicate header negative fixture"),
    ("Accept override REST header was not caught", "REST Accept override negative fixture"),
    ("Content-Type override REST header was not caught", "REST Content-Type override negative fixture"),
    ("unsupported REST boundary method was not caught", "unsupported boundary method negative fixture"),
    ("success ApiError-shaped body was not caught", "REST success ApiError-shape negative fixture"),
    ("success body must be a bare typed JSON object", "REST success typed-object assertion"),
    ("success non-object JSON body was not caught", "REST success non-object negative fixture"),
    ("empty success object body was not caught", "REST success empty-object negative fixture"),
    ("misleading non-JSON success Content-Type was not caught", "REST misleading content-type negative fixture"),
    ("duplicate Content-Type response was not caught", "REST duplicate Content-Type negative fixture"),
    ("non-string Content-Type response was not caught", "REST non-string Content-Type negative fixture"),
    ("unreadable Content-Type response was not caught", "REST unreadable Content-Type negative fixture"),
    ("non-bytes response body was not caught", "REST non-bytes response body negative fixture"),
    ("direct decoder non-string Content-Type was not caught", "REST direct decoder non-string Content-Type negative fixture"),
    ("direct decoder padded Content-Type was not caught", "REST direct decoder padded Content-Type negative fixture"),
    ("direct decoder control-character Content-Type was not caught", "REST direct decoder control-character Content-Type negative fixture"),
    ("direct decoder non-bytes response body was not caught", "REST direct decoder non-bytes body negative fixture"),
    ("oversized success response body was not caught", "REST oversized success body negative fixture"),
    ("oversized error response body was not caught", "REST oversized error body negative fixture"),
    ("non-standard JSON constant success response body was not caught", "REST non-standard JSON constant negative fixture"),
    ("success flag body was not caught", "REST success flag negative fixture"),
    ("duplicate-key success response body was not caught", "REST duplicate-key success body negative fixture"),
    ("duplicate-key error response body was not caught", "REST duplicate-key error body negative fixture"),
    ("incomplete ApiError body was not caught", "REST ApiError negative fixture"),
    ("wrong ApiError code was not caught", "REST ApiError canonical code negative fixture"),
    ("wrong ApiError code/status mapping was not caught", "REST ApiError canonical code/status mapping negative fixture"),
    ("non-canonical ApiError code was not caught", "REST ApiError non-canonical code negative fixture"),
    ("spaced ApiError code was not caught", "REST ApiError code surrounding-whitespace negative fixture"),
    ("control-character ApiError code was not caught", "REST ApiError code control-character negative fixture"),
    ("non-integer ApiError httpStatusCode was not caught", "REST ApiError HTTP status integer negative fixture"),
    ("boolean ApiError httpStatusCode was not caught", "REST ApiError HTTP status boolean negative fixture"),
    ("wrong ApiError httpStatusCode was not caught", "REST ApiError HTTP status negative fixture"),
    ("malformed ApiError public fields were not caught", "REST ApiError public-shape negative fixture"),
    ("undocumented ApiError field was not caught", "REST ApiError undocumented-field negative fixture"),
    ("undocumented ApiError fieldViolations entry field was not caught", "REST ApiError fieldViolation undocumented-field negative fixture"),
    ("spaced ApiError message was not caught", "REST ApiError message surrounding-whitespace negative fixture"),
    ("control-character ApiError message was not caught", "REST ApiError message control-character negative fixture"),
    (
        "oversized ApiError fieldViolations description was not caught",
        "REST ApiError fieldViolations description oversized negative fixture",
    ),
    ("empty INVALID_ARGUMENT fieldViolations were not caught", "REST ApiError missing validation fieldViolations negative fixture"),
    ("non-validation ApiError fieldViolations were not caught", "REST ApiError non-validation fieldViolations negative fixture"),
    ("malformed ApiError fieldViolations entries were not caught", "REST ApiError fieldViolations entry negative fixture"),
    ("whitespace ApiError fieldViolations entries were not caught", "REST ApiError fieldViolations whitespace negative fixture"),
    ("control-character REST boundary error-code proof was not caught", "REST boundary error-code control-character negative fixture"),
    (
        "invalid ApiError fieldViolations field tokens were not caught",
        "REST ApiError fieldViolations field token negative fixture",
    ),
)

BETA_VERSIONING_POSTURE_REQUIREMENTS = (
    ("EXPECTED_MIGRATION_ROWS", "beta migration fixture row inventory"),
    ("EXPECTED_MIGRATION_HEADER", "beta migration fixture table header inventory"),
    ("MIGRATION_TOKEN_COLUMNS", "beta migration fixture token-column inventory"),
    ("old_sdk_tokens", "beta migration fixture old-SDK token inventory"),
    ("EXPECTED_SERVED_ROUTE_PROOF_TOKENS", "beta migration served route proof inventory"),
    ("EXPECTED_OWNER_TOKENS", "beta migration fixture test/guard owner inventory"),
    ("EXPECTED_OPERATION_ID_ROUTE_TOKENS", "beta migration served route operationId inventory"),
    ("def _split_markdown_row(", "beta migration fixture markdown row parser"),
    ("import ast", "beta migration served route AST parser import"),
    ("def _contains_control_character(", "beta migration fixture control-character helper"),
    ("def _check_migration_fixture_coverage(", "beta migration fixture row coverage checker"),
    ("def _check_served_route_proof_coverage(", "beta migration served route proof coverage checker"),
    ("def _served_route_inventory_text(", "beta migration served route executable inventory extractor"),
    ("def _served_route_inventory_strings(", "beta migration served route AST string extractor"),
    ("def _served_route_operation_id_strings(", "beta migration served route operationId AST extractor"),
    ("def _served_route_cases_ast(", "beta migration served route ROUTE_CASES AST extractor"),
    ("served route proof inventory block ROUTE_CASES is missing", "beta migration served route inventory-block failure"),
    ("def _check_benchmark_identity(", "benchmark identity checker"),
    ("migration fixture duplicate row for", "beta migration fixture duplicate-row failure"),
    ("migration fixture header must be", "beta migration fixture header-shape failure"),
    ("has 8 columns, expected 7", "beta migration fixture row-shape failure"),
    ("contains control characters", "beta migration fixture control-character failure"),
    ("missing old SDK/public method token", "beta migration fixture old-SDK token failure"),
    ("in Current HTTP route", "beta migration fixture current-route column failure"),
    ("in Old SDK/public method shape", "beta migration fixture old-SDK column failure"),
    ("in Current SDK alias / operationId", "beta migration fixture alias column failure"),
    ("served route proof for Storage download URL missing token", "beta migration served route proof failure"),
    ("missing test/guard owner token", "beta migration fixture owner-token failure"),
    ("missing operationId token", "beta migration served route operationId failure"),
    ("benchmark_tokens", "beta migration fixture benchmark-label token inventory"),
    ("uses generic benchmark label", "beta migration fixture generic benchmark-label failure"),
    ("return operation_id or api_alias or wire_api", "benchmark collector canonical API identity"),
    ("r.api || r.operation_id || r.api_alias || r.wire_api", "benchmark dashboard public identity fallback"),
    ("operation_id || api_alias || wire_api", "benchmark generated-doc/listing identity prose"),
    ("migration fixture row", "beta migration fixture row-token failure"),
    ("missing benchmark label token", "beta migration fixture benchmark-label failure"),
    ("expected migration fixture coverage failure", "beta migration fixture coverage negative fixture"),
    ("expected migration fixture duplicate-row failure", "beta migration fixture duplicate-row negative fixture"),
    ("expected migration fixture row-shape failure", "beta migration fixture row-shape negative fixture"),
    ("expected migration fixture control-character failure", "beta migration fixture control-character negative fixture"),
    ("expected migration fixture column-specific route failure", "beta migration fixture route-column negative fixture"),
    ("expected migration fixture column-specific alias failure", "beta migration fixture alias-column negative fixture"),
    ("expected migration fixture old-SDK column failure", "beta migration fixture old-SDK negative fixture"),
    ("expected migration fixture benchmark-label failure", "beta migration fixture benchmark-label negative fixture"),
    ("expected generic benchmark-label failure", "beta migration fixture generic benchmark-label negative fixture"),
    ("expected migration fixture owner-token failure", "beta migration fixture owner-token negative fixture"),
    ("expected served route proof coverage regression", "beta migration served route proof negative fixture"),
    ("expected served route inventory-only coverage regression", "beta migration served route inventory-only negative fixture"),
    ("expected served route AST inventory coverage regression", "beta migration served route AST inventory negative fixture"),
    ("expected served route operationId proof regression", "beta migration served route operationId negative fixture"),
    ("expected benchmark collector identity failure", "benchmark collector negative fixture"),
    ("expected benchmark dashboard identity failure", "benchmark dashboard negative fixture"),
)

CI_HTTP_API_STYLE_COMMANDS = (
    ("node --check scripts/check-http-api-style.mjs", "HTTP API route-style syntax check"),
    ("node scripts/check-http-api-style.mjs --selftest", "HTTP API route-style selftest"),
    ("node scripts/check-http-api-style.mjs --source-only", "HTTP API source route-style hard gate"),
    ("node scripts/check-http-api-style.mjs --write-report", "HTTP API exception report generation"),
    ("git diff --quiet -- docs/generated/http-api-style-exceptions.json docs/generated/http-api-style-exceptions.md", "HTTP API exception report freshness diff"),
    ("node scripts/check-http-api-style.mjs --advisory", "HTTP API route-style advisory scan"),
    ("node scripts/check-http-api-style.mjs --resource-identity-contract", "HTTP API resource identity contract hard gate"),
    ("node scripts/check-http-api-style.mjs --pagination-contract", "HTTP API pagination contract hard gate"),
    ("node scripts/check-http-api-style.mjs --query-update-contract", "HTTP API query/update contract hard gate"),
    ("python3 scripts/rest_route_gateway_smoke.py --selftest", "REST route gateway smoke selftest"),
    ("python3 scripts/rest_route_gateway_smoke.py --check-openapi", "REST route gateway smoke OpenAPI check"),
)

CI_INVENTORY_GUARD_REQUIREMENTS = (
    ("function workflowInventory(repo)", "CI inventory collector"),
    ("function checkRepo(repo = ROOT)", "CI inventory repo checker"),
    ("requiredWorkflows", "required workflow inventory"),
    ("requiredActions", "required shared action inventory"),
    ("requiredCiJobs", "required CI job inventory"),
    ("requiredPrCheckJobs", "required PR branch-protection job inventory"),
    ("dependencyFreePrJobs", "dependency-free PR job inventory"),
    ("dependency-free PR job must not declare needs", "dependency-free PR job repo guard"),
    ("cheap PR job serialization regression was not caught", "dependency-free PR job negative selftest"),
    ("function requiredCheckNamesFromArchitecture(text)", "branch-protection required-check parser"),
    ("stale required PR check name", "stale branch-protection check negative"),
    ("releaseFanoutJobs", "release fanout job inventory"),
    ("releaseLeafWorkflows", "release leaf inventory"),
    ("feature-matrix.yml must stay folded", "feature-matrix duplicate negative"),
    ("release leaf tag-trigger regression was not caught", "release tag duplicate negative"),
    ('process.argv.includes("--selftest")', "CI inventory selftest CLI"),
    ("checkRepo(process.cwd())", "CI inventory repo scan entrypoint"),
    ("process.exit(1)", "CI inventory hard failure"),
)

BRANCH_PROTECTION_LOCKSTEP_REQUIREMENTS = (
    ("function requiredCheckNamesFromArchitecture(text)", "required-check docs parser"),
    ("function normalizeRequiredStatusChecks(payload)", "GitHub required-status parser"),
    ("payload.contexts", "legacy branch-protection context parsing"),
    ("payload.checks", "GitHub checks branch-protection parsing"),
    ("function compareRequiredChecks(documented, actual)", "required-check diff"),
    ("missingInBranchProtection", "missing required-check failure"),
    ("staleInBranchProtection", "stale required-check failure"),
    ("function repoArg(args, name, fallback)", "branch-protection repository CLI validator"),
    ("must be an owner/repo repository name", "branch-protection repository canonical rejection"),
    ("function branchArg(args, name, fallback)", "branch-protection branch CLI validator"),
    ("must be a canonical branch name", "branch-protection branch canonical rejection"),
    ('const repo = repoArg(args, "--repo", process.env.GITHUB_REPOSITORY)', "branch-protection repository validator wiring"),
    ('const branch = branchArg(args, "--branch", process.env.GITHUB_REF_NAME || "main")', "branch-protection branch validator wiring"),
    ("process.env.GH_TOKEN || process.env.GITHUB_TOKEN", "GitHub token fallback"),
    ("protection/required_status_checks", "GitHub branch-protection endpoint"),
    ("missing required check regression was not caught", "missing-check negative selftest"),
    ("stale required check regression was not caught", "stale-check negative selftest"),
    ("padded repository input regression was not caught", "padded repository input negative selftest"),
    ("malformed repository input regression was not caught", "malformed repository input negative selftest"),
    ("canonical repository input was rejected", "canonical repository input positive selftest"),
    ("padded branch input regression was not caught", "padded branch input negative selftest"),
    ("non-canonical branch input regression was not caught", "non-canonical branch input negative selftest"),
    ("canonical branch input was rejected", "canonical branch input positive selftest"),
    ("process.exit(1)", "hard failure exit"),
)

CI_RUNNER_EVIDENCE_REQUIREMENTS = (
    ("const DEFAULT_BUDGETS", "budget defaults"),
    ("const MAX_BUDGETS = { ...DEFAULT_BUDGETS }", "budget override ceiling defaults"),
    ("const DEFAULT_MAX_EVIDENCE_AGE_DAYS = 14", "runner evidence max-age default"),
    ("const MAX_EVIDENCE_AGE_DAYS = DEFAULT_MAX_EVIDENCE_AGE_DAYS", "runner evidence max-age override ceiling"),
    ("const GITHUB_API_REQUEST_TIMEOUT_MS = 30 * 1000", "GitHub API request timeout ceiling"),
    ("const MAX_GITHUB_API_RESPONSE_BYTES = 4 * 1024 * 1024", "GitHub API response byte ceiling"),
    ("const MAX_FIXTURE_BYTES = 1 * 1024 * 1024", "runner evidence fixture byte ceiling"),
    ("const MAX_GITHUB_RUN_JOBS = 500", "GitHub jobs pagination total_count ceiling"),
    ("const MAX_GITHUB_JOBS_PAGE_SIZE = 100", "GitHub jobs pagination page-size ceiling"),
    ("const MAX_GITHUB_WORKFLOW_RUN_CANDIDATES = 100", "GitHub workflow-run discovery candidate ceiling"),
    ("const ALL_EVIDENCE_MODE = \"--all-evidence\"", "runner evidence all-evidence mode constant"),
    ("pr: 8", "PR budget default"),
    ("integration: 30", "integration budget default"),
    ("release: 40", "release budget default"),
    ("releaseDryRun: 120", "release dry-run budget default"),
    ("benchmark: 120", "benchmark budget default"),
    ("pages: 20", "Pages budget default"),
    ("lint: 10", "lint budget default"),
    ("branchProtection: 10", "branch-protection budget default"),
    ("idempotencyServed: 15", "idempotency served proof budget default"),
    ("errorDetailServed: 15", "ErrorDetail served proof budget default"),
    ("retrySafeServed: 15", "retry-safe served proof budget default"),
    ("restGateway: 15", "REST gateway proof budget default"),
    ("const LINT_EVIDENCE_EVENTS = [\"workflow_dispatch\", \"pull_request\", \"push\"]", "lint evidence event set"),
    ("const DEFAULT_INTEGRATION_BRANCH = \"main\"", "integration evidence branch default"),
    ("const RELEASE_TAG_PATTERN = /^v\\d+\\.\\d+\\.\\d+", "release evidence tag pattern"),
    ("const GIT_SHA_PATTERN = /^[0-9a-f]{40}$/;", "release evidence lowercase SHA pattern"),
    ("const RUN_ID_PATTERN = /^[1-9]\\d*$/", "runner evidence run-id pattern"),
    ("const POSITIVE_DECIMAL_PATTERN", "positive decimal numeric CLI pattern"),
    ("const ACTIONS_TIMESTAMP_PATTERN", "GitHub Actions timestamp pattern"),
    ("const GITHUB_ACTIONS_RUN_URL_PATTERN", "GitHub Actions run inspection URL pattern"),
    ("const PR_REQUIRED_JOBS", "PR required check inventory"),
    ("const PR_ADVISORY_JOBS", "PR advisory/no-check-lost job inventory"),
    ("const PR_EVIDENCE_JOBS = [...PR_REQUIRED_JOBS, ...PR_ADVISORY_JOBS]", "complete PR runner evidence inventory"),
    ("const PR_BUDGET_JOBS", "PR required-lane budget job inventory"),
    ("const INTEGRATION_REQUIRED_JOBS", "integration full CI required job inventory"),
    ("releaseDryRun: \"release-binaries.yml\"", "release dry-run workflow identity"),
    ("benchmark: \"benchmark-sdks.yml\"", "benchmark workflow identity"),
    ("pages: \"pages.yml\"", "Pages workflow identity"),
    ("branchProtection: \"branch-protection-audit.yml\"", "branch-protection workflow identity"),
    ("idempotencyServed: \"idempotency-served-smoke.yml\"", "idempotency served workflow identity"),
    ("errorDetailServed: \"error-detail-served-smoke.yml\"", "ErrorDetail served workflow identity"),
    ("retrySafeServed: \"retry-safe-served-smoke.yml\"", "retry-safe served workflow identity"),
    ("restGateway: \"rest-gateway-smoke.yml\"", "REST gateway workflow identity"),
    ("const REQUIRED_JOBS", "runner evidence required job inventory"),
    (
        "idempotencyServed: [\"DataBroker idempotency served replay proof\"]",
        "idempotency served proof job inventory",
    ),
    (
        "errorDetailServed: [\"ErrorDetail served transport proof\"]",
        "ErrorDetail served proof job inventory",
    ),
    (
        "retrySafeServed: [\"Retry-safe mutation metadata served proof\"]",
        "retry-safe served proof job inventory",
    ),
    (
        "restGateway: [\"REST boundary content/status proof\"]",
        "REST gateway proof job inventory",
    ),
    ("const SERVED_SMOKE_AUDITS", "served-smoke evidence audit registry"),
    (
        "function assertSuccessfulBudgetRun(run, label, budgetMinutes, { maxAgeDays, nowMs = Date.now() } = {})",
        "budget and evidence-age assertion",
    ),
    ("function boundedBudgetArg(args, name, fallback, max)", "bounded budget override helper"),
    ("must be a positive decimal number", "numeric CLI decimal rejection"),
    ("must be <= ${max} minutes", "inflated budget override rejection"),
    ("function boundedMaxEvidenceAgeArg(args, name, fallback, max)", "bounded max evidence-age override helper"),
    ("must be <= ${max} days", "inflated max evidence-age override rejection"),
    ("function repoArg(args, name, fallback)", "repository CLI validator"),
    ("must be an owner/repo repository name", "repository CLI owner/repo rejection"),
    ("const repo = repoArg(args, \"--repo\", process.env.GITHUB_REPOSITORY)", "repository validator live wiring"),
    ("function optionalReleaseTagArg(args, name)", "release-tag CLI validator"),
    ("must not include surrounding whitespace", "release-tag CLI whitespace rejection"),
    ("function branchArg(args, name, fallback)", "branch CLI validator"),
    ("must be a canonical branch name", "branch CLI canonical rejection"),
    ("const branch = branchArg(args, \"--branch\", DEFAULT_INTEGRATION_BRANCH)", "branch validator live wiring"),
    ("--idempotency-served-budget-minutes", "idempotency served proof budget CLI"),
    ("--idempotency-run-id", "idempotency served exact run-id CLI"),
    ("--error-detail-served-budget-minutes", "ErrorDetail served proof budget CLI"),
    ("--error-detail-run-id", "ErrorDetail served exact run-id CLI"),
    ("--retry-safe-served-budget-minutes", "retry-safe served proof budget CLI"),
    ("--retry-safe-run-id", "retry-safe served exact run-id CLI"),
    ("--rest-gateway-budget-minutes", "REST gateway proof budget CLI"),
    ("--rest-gateway-run-id", "REST gateway exact run-id CLI"),
    ("function requestedServedAuditKeys(", "served evidence multi-mode selector"),
    (
        "if (args.includes(ALL_EVIDENCE_MODE)) {\n    return Object.keys(SERVED_SMOKE_AUDITS);\n  }",
        "all-evidence selects every served proof lane",
    ),
    ("async function auditRequestedServedSmokes(", "served evidence multi-mode auditor"),
    ("function formatNestedFailure(", "nested aggregate failure formatter"),
    ("function servedEvidenceSummaryText(", "served evidence aggregate summary"),
    ("async function auditAllEvidence(", "all-evidence aggregate auditor"),
    ("served evidence passed:", "served evidence aggregate success output"),
    ("served evidence audit failed:", "served evidence aggregate failure output"),
    ("idempotencyServedRunId", "idempotency served aggregate run-id summary"),
    ("restGatewayRunId", "REST gateway aggregate run-id summary"),
    ("multi-served evidence aggregation regression was not caught", "multi-served evidence aggregation negative selftest"),
    ("multi-served evidence lookup did not audit every requested served workflow", "multi-served evidence lookup negative selftest"),
    ("multi-served missing evidence aggregation regression was not caught", "multi-served missing evidence aggregate selftest"),
    ("all-evidence base plus served failure aggregation regression was not caught", "all-evidence base/served aggregate failure selftest"),
    ("--all-evidence did not select every served proof lane", "all-evidence served-lane selector selftest"),
    ("CI runner evidence: runner evidence discovery failed:", "all-evidence base failure aggregate marker"),
    ("served evidence: served evidence audit failed:", "all-evidence served failure aggregate marker"),
    ("ALL_EVIDENCE_MODE", "central all-evidence mode branch"),
    ("function optionalRunIdArg(args, name)", "run-id CLI validator"),
    ("must be a positive integer run id", "run-id CLI positive integer rejection"),
    ("const prRunId = optionalRunIdArg(args, \"--pr-run-id\")", "run-id validator live wiring"),
    ("const CI_RUN_ID_ARGS", "CI run-id override inventory"),
    ("const CI_BUDGET_ARGS", "CI budget override inventory"),
    ("const SERVED_BUDGET_ARGS", "served budget override inventory"),
    ("const VALUE_ARGS = new Set", "runner evidence value-argument registry"),
    ("const FLAG_ARGS = new Set", "runner evidence flag-argument registry"),
    ("function assertKnownArgs(args)", "runner evidence unknown-argument guard"),
    ("unknown runner evidence argument", "unknown runner evidence argument failure"),
    ("unexpected runner evidence argument", "unexpected runner evidence positional failure"),
    ("function assertNoUnusedEvidenceOverrides(args, servedAuditKeys)", "unused evidence override guard"),
    ("otherwise the run id would not be audited", "unused run-id override failure"),
    ("otherwise the CI budget would not be audited", "unused CI budget override failure"),
    ("otherwise the CI evidence option would not be audited", "unused CI option override failure"),
    ("otherwise the served budget would not be audited", "unused served budget override failure"),
    ("unused CI run-id override regression was not caught", "unused CI run-id negative selftest"),
    ("unused served run-id override regression was not caught", "unused served run-id negative selftest"),
    ("unused served budget override regression was not caught", "unused served budget negative selftest"),
    ("unused CI budget override regression was not caught", "unused CI budget negative selftest"),
    ("unused release-tag override regression was not caught", "unused release-tag negative selftest"),
    ("unused fixture override regression was not caught", "unused fixture negative selftest"),
    ("unknown runner-evidence argument regression was not caught", "unknown runner evidence arg negative selftest"),
    ("unexpected positional runner-evidence argument regression was not caught", "unexpected positional arg negative selftest"),
    ("missing runner-evidence argument value regression was not caught", "missing arg value negative selftest"),
    ("function parseActionsTimestampMs(value, label)", "canonical GitHub Actions timestamp parser"),
    ("must be a GitHub Actions UTC timestamp", "non-canonical timestamp rejection"),
    ("const completedAt = parseActionsTimestampMs", "runner evidence completion timestamp parser"),
    ("max evidence age", "stale runner evidence failure"),
    ("const start = runStartMs(run, \"budget\")", "budget duration uses canonical run start helper"),
    ("const end = runCompletedMs(run, \"budget\")", "budget duration uses canonical completion helper"),
    ("function assertSuccessfulJobWindowBudgetRun", "PR required-lane budget assertion"),
    ("PR CI required gate", "PR required-lane budget label"),
    ("--max-evidence-age-days", "runner evidence max-age CLI"),
    ("maxAgeDays: DEFAULT_MAX_EVIDENCE_AGE_DAYS", "runner evidence selftest max-age option"),
    ("function assertJobSucceeded(job, label)", "required job success assertion"),
    ("function assertJobEvidenceName(job, label)", "runner evidence job-name shape assertion helper"),
    ("job name must be a string", "runner evidence job-name type rejection"),
    ("job name must be non-empty", "runner evidence job-name non-empty rejection"),
    ("must not include surrounding whitespace", "runner evidence job-name whitespace rejection"),
    ("job ${jobName} is not completed", "required job completion failure"),
    ("job ${jobName} did not succeed", "required job conclusion failure"),
    ("run has duplicate required job", "required job duplicate failure"),
    ("function assertRequiredJobInventory(label, requiredNames)", "required job inventory shape assertion"),
    ("required job inventory must be a non-empty array", "required job inventory non-empty assertion"),
    ("required job inventory duplicates", "required job inventory duplicate failure"),
    ("duplicate required job inventory regression was not caught", "required job inventory negative selftest"),
    ("function assertPrBrokerCompileReduction(jobs)", "broker compile reduction assertion"),
    ("function assertRequiredJobs(jobs, label, requiredNames)", "runner evidence required job assertion"),
    ("const matchedJobs = []", "runner evidence matched job collector"),
    ("const jobNames = jobs.map((job) => assertJobEvidenceName(job, label))", "runner evidence canonical job-name matching"),
    ("return matchedJobs", "runner evidence matched job return"),
    ("const lintEvidenceJobs = assertRequiredJobs", "runner evidence scoped job subset"),
    ("const prEvidenceJobs = [prBrokerJob, ...assertRequiredJobs", "PR evidence includes build-broker subset"),
    ("function assertJobsBelongToRun(jobs, label, run)", "runner evidence job/run binding assertion"),
    ("function assertJobEvidenceId(job, label)", "runner evidence job-id shape assertion helper"),
    ("function assertRunEvidenceRunId(run, label)", "runner evidence run-id shape assertion helper"),
    ("function assertRunEvidenceAttempt(run, label)", "runner evidence run-attempt shape assertion helper"),
    ("function assertRunInspectionUrl(run, label, expectedRepo = \"\")", "runner evidence run inspection URL assertion"),
    ("html_url must be a canonical GitHub Actions run URL", "runner evidence malformed inspection URL failure"),
    ("html_url run id ${actualRunId}, want ${expectedRunId}", "runner evidence inspection URL run-id binding"),
    ("html_url repo ${actualRepo}, want ${expectedRepo}", "runner evidence inspection URL repo binding"),
    ("function assertPositiveIntegerEvidenceToken(value, label)", "runner evidence positive-integer token assertion helper"),
    ("RUN_ID_PATTERN.test(token)", "runner evidence positive-integer pattern assertion"),
    ("has invalid value ${token || \"(missing)\"}; want positive integer", "runner evidence positive-integer rejection"),
    ("assertRunEvidenceRunId(run, label)", "runner evidence identity run-id assertion wiring"),
    ("assertRunEvidenceAttempt(run, label)", "runner evidence identity run-attempt assertion wiring"),
    ("assertRunInspectionUrl(run, label, repo)", "runner evidence identity inspection URL wiring"),
    ("const expectedAttempt = assertRunEvidenceAttempt(run, label)", "runner evidence mandatory run-attempt binding"),
    ("`${label} job ${assertJobEvidenceName(job, label)} id`", "runner evidence job id token label"),
    ("reuses job id ${jobId} already used by ${previousJobName}", "runner evidence duplicate job id failure"),
    ("`${label} job ${jobName} run_id`", "runner evidence job run_id token label"),
    ("belongs to run ${actualRunId}, want ${expectedRunId}", "runner evidence wrong job run_id failure"),
    ("`${label} job ${jobName} run_attempt`", "runner evidence job run_attempt token label"),
    ("belongs to run attempt ${actualAttempt}, want ${expectedAttempt}", "runner evidence wrong job run_attempt failure"),
    ("function jobTimestampMs(value, label)", "runner evidence job timestamp parser"),
    ("function assertJobsWithinRunWindow(jobs, label, run)", "runner evidence job timestamp window assertion"),
    ("is missing timestamp", "runner evidence missing job timestamp failure"),
    ("must not include surrounding whitespace", "runner evidence timestamp whitespace rejection"),
    ("completed before it started", "runner evidence impossible job timestamp failure"),
    ("completed after parent run", "runner evidence late job timestamp failure"),
    ("function assertRunEvidenceIdentity(run, label,", "workflow/event evidence identity assertion"),
    ("function assertLintEvidenceBranch(run)", "lint evidence branch assertion helper"),
    ("lint/actionlint run ${run.id} used branch ${run.head_branch}, want ${DEFAULT_INTEGRATION_BRANCH}", "wrong lint branch failure"),
    ("function assertReleaseTag(value, label)", "release tag shape assertion helper"),
    ("const tag = String(value || \"\")", "release tag canonical token extraction"),
    ("has invalid release tag", "malformed release tag failure"),
    ("function assertGitSha(value, label)", "release-chain SHA shape assertion helper"),
    ("const sha = String(value || \"\")", "release SHA canonical token extraction"),
    ("want 40 hex characters", "malformed git SHA failure"),
    ("assertGitSha(run?.head_sha, `${label} run ${run.id || \"(unknown)\"}`)", "runner evidence all-run SHA assertion wiring"),
    ("function assertDistinctRunEvidence(runs)", "distinct runner evidence assertion"),
    ("assertDistinctRunEvidence({", "distinct runner evidence call"),
    ("const id = assertRunEvidenceRunId(run, `${label} evidence`)", "distinct runner evidence canonical ID assertion"),
    ("evidence reuses run", "duplicate runner evidence failure"),
    ("function assertSharedRunInspectionRepo(runs)", "runner evidence shared inspection repo assertion"),
    ("uses repo ${repo}, want ${expectedRepo} from ${expectedLabel}", "runner evidence cross-repo fixture failure"),
    ("assertSharedRunInspectionRepo({", "runner evidence shared inspection repo wiring"),
    ("function assertReleaseChainTags({ release, benchmark, pages })", "release-chain tag assertion"),
    ("used branch ${actualBranch || \"(missing)\"}, want ${DEFAULT_INTEGRATION_BRANCH}", "release-chain workflow_run branch mismatch failure"),
    ("used release tag ${actualBranch || \"(missing)\"}, want ${releaseTag}", "release-chain non-workflow_run tag mismatch failure"),
    ("release chain has missing release head_sha", "release-chain missing release SHA failure"),
    ("used head_sha ${actualSha}, want ${releaseSha}", "release-chain SHA mismatch failure"),
    ("function assertReleaseDryRunCommit({ release, releaseDryRun })", "release dry-run commit binding assertion"),
    ("const releaseTag = assertReleaseTag(release?.head_branch, \"release\")", "release dry-run release-tag binding source"),
    ("const dryRunTag = assertReleaseTag(releaseDryRun?.head_branch", "release dry-run tag extraction"),
    ("used release tag ${dryRunTag}, want ${releaseTag}", "release dry-run tag mismatch failure"),
    ("release dry-run run ${releaseDryRun?.id || \"(unknown)\"} used head_sha ${dryRunSha}, want ${releaseSha}", "release dry-run SHA mismatch failure"),
    ("branch: expectedReleaseTag", "release dry-run live lookup tag filter"),
    ("release dry-run lookup did not request workflow_dispatch branch v0.3.7", "release dry-run tag-filtered lookup selftest"),
    ("const discoveryFailures = []", "runner evidence aggregate discovery failure list"),
    ("runner evidence discovery failed:", "runner evidence aggregate discovery failure"),
    ("aggregate live discovery regression was not caught", "runner evidence aggregate discovery selftest"),
    ("PR CI: no successful completed ci.yml run found", "runner evidence aggregate PR failure selftest"),
    ("integration CI: no successful completed ci.yml run found", "runner evidence aggregate integration failure selftest"),
    ("function assertBranchProtectionCommit({ integration, branchProtection })", "branch-protection commit binding assertion"),
    ("const integrationSha = assertGitSha(integration?.head_sha, \"integration CI\")", "branch-protection integration SHA source"),
    ("branch-protection run ${branchProtection?.id || \"(unknown)\"} used head_sha ${branchProtectionSha}, want ${integrationSha}", "branch-protection SHA mismatch failure"),
    ("findLatestSuccessfulRun(repo, token, WORKFLOWS.branchProtection, {\n          event: \"workflow_dispatch\",\n          branch,\n        }, fetcher)", "branch-protection live lookup branch filter"),
    ("assertRunEvidenceIdentity(branchProtectionRun, \"branch-protection\", {\n    workflow: WORKFLOWS.branchProtection,\n    event: \"workflow_dispatch\",\n    branch,\n    repo,\n  })", "branch-protection live identity branch binding"),
    ("branch-protection lookup did not request workflow_dispatch branch main", "branch-protection branch-filtered lookup selftest"),
    ("custom branch live evidence summary regression was not caught", "custom branch live branch-protection binding selftest"),
    ("async function auditIdempotencyServed(", "idempotency served evidence audit function"),
    ("async function auditServedSmoke(", "generic served-smoke evidence audit function"),
    ("--idempotency-served-smoke", "idempotency served evidence audit mode"),
    ("--error-detail-served-smoke", "ErrorDetail served evidence audit mode"),
    ("--retry-safe-served-smoke", "retry-safe served evidence audit mode"),
    ("--rest-gateway-smoke", "REST gateway evidence audit mode"),
    ("event: \"workflow_dispatch\",\n        branch,", "idempotency served live lookup event/branch filter"),
    ("idempotency served evidence passed:", "idempotency served evidence success output"),
    ("idempotency served lookup did not request workflow_dispatch branch main", "idempotency served lookup selftest"),
    ("idempotency served missing proof job regression was not caught", "idempotency served missing-job selftest"),
    ("const servedSmokeSelftests = [", "Chapter 14 served-smoke audit selftest table"),
    ("[\"errorDetailServed\", 92, \"ErrorDetail served transport proof\", \"--error-detail-served-smoke\"]", "ErrorDetail served selftest case"),
    ("[\"retrySafeServed\", 93, \"Retry-safe mutation metadata served proof\", \"--retry-safe-served-smoke\"]", "retry-safe served selftest case"),
    ("[\"restGateway\", 94, \"REST boundary content/status proof\", \"--rest-gateway-smoke\"]", "REST gateway selftest case"),
    ("`${audit.label} lookup did not request workflow_dispatch branch main`", "served-smoke generic lookup selftest"),
    ("function assertReleaseChainOrder({ release, benchmark, pages })", "release-chain ordering assertion"),
    ("started before release run", "early benchmark ordering failure"),
    ("started before benchmark run", "early Pages ordering failure"),
    ("function appendGitHubApiChunk(body, chunk, label)", "GitHub API response chunk limiter"),
    ("Buffer.byteLength(next, \"utf8\") > MAX_GITHUB_API_RESPONSE_BYTES", "GitHub API response byte-count check"),
    ("GitHub API response exceeded", "GitHub API oversized response rejection"),
    ("function assertGitHubApiSuccessStatus(", "GitHub API status-code validator"),
    ("must include an integer HTTP status code", "GitHub API status-code type rejection"),
    ("GitHub API ${statusCode}:", "GitHub API non-success status rejection"),
    ("function githubApiMissingWorkflowError(", "GitHub API missing workflow classifier"),
    ("import { execFileSync } from \"node:child_process\"", "GitHub API missing workflow git-state import"),
    ("function localWorkflowVisibilityHint(", "GitHub API missing workflow local visibility helper"),
    ("function gitWorkflowPathState(", "GitHub API missing workflow git-state helper"),
    ("function commandSucceeds(", "GitHub API missing workflow git command helper"),
    ("git\", [\"ls-files\", \"--error-unmatch\", \"--\", localWorkflowPath]", "GitHub API missing workflow tracked-file check"),
    ("git\", [\"diff\", \"--cached\", \"--quiet\", \"--\", localWorkflowPath]", "GitHub API missing workflow staged-change check"),
    ("git\", [\"diff\", \"--quiet\", \"--\", localWorkflowPath]", "GitHub API missing workflow unstaged-change check"),
    ("GitHub Actions workflow ${workflow} is not visible in ${repo}", "GitHub API missing workflow actionable failure"),
    ("local file ${localWorkflowPath} exists", "GitHub API missing workflow local-file hint"),
    ("has staged and unstaged changes", "GitHub API missing workflow dirty-file hint"),
    ("has staged changes", "GitHub API missing workflow staged-file hint"),
    ("has unstaged changes", "GitHub API missing workflow unstaged-file hint"),
    ("is tracked and clean locally", "GitHub API missing workflow clean-file hint"),
    ("commit/push it to the default branch", "GitHub API missing workflow remediation"),
    ("missing workflow GitHub API regression was not caught", "GitHub API missing workflow negative selftest"),
    ("function githubApiRateLimitError(", "GitHub API rate-limit classifier"),
    ("GitHub API rate limit exceeded for ${label}", "GitHub API rate-limit actionable failure"),
    ("set GH_TOKEN or GITHUB_TOKEN for authenticated evidence lookup", "GitHub API rate-limit token guidance"),
    ("GitHub API rate-limit regression was not caught", "GitHub API rate-limit negative selftest"),
    ("GitHub API secondary-rate-limit regression was not caught", "GitHub API secondary-rate-limit negative selftest"),
    ("assertGitHubApiSuccessStatus(response, body, url)", "GitHub API status-code validator wiring"),
    ("function assertGitHubApiJsonContentType(", "GitHub API JSON content-type validator"),
    ("must include a JSON Content-Type", "GitHub API missing content-type rejection"),
    ("Content-Type must not include surrounding whitespace", "GitHub API padded content-type rejection"),
    ("must be JSON, got", "GitHub API non-JSON content-type rejection"),
    ("assertGitHubApiJsonContentType(response, url)", "GitHub API content-type validator wiring"),
    ("function githubApiTimeoutError(label)", "GitHub API timeout error helper"),
    ("request.setTimeout(GITHUB_API_REQUEST_TIMEOUT_MS", "GitHub API request timeout wiring"),
    ("request.destroy(error)", "GitHub API timeout request destroy"),
    ("rejectDuplicateJsonObjectKeys(body, `GitHub API response ${url}`)", "GitHub API duplicate-key parser wiring"),
    ("function readFixtureJson(path)", "runner evidence fixture reader"),
    ("statSync(path)", "runner evidence fixture stat check"),
    ("fixture ${path} must be a regular file", "runner evidence fixture regular-file rejection"),
    ("fixture ${path} must be <= ${MAX_FIXTURE_BYTES} bytes", "runner evidence fixture oversized rejection"),
    ("function rejectDuplicateJsonObjectKeys(text, label)", "runner evidence duplicate fixture-key scanner"),
    ("const keys = new Set()", "runner evidence per-object key set"),
    ("has duplicate JSON object key", "runner evidence duplicate fixture-key rejection"),
    ("rejectDuplicateJsonObjectKeys(fixtureText, `fixture ${path}`)", "runner evidence duplicate fixture-key parser wiring"),
    ("JSON.parse(fixtureText)", "runner evidence fixture parser wiring"),
    ("function assertFixtureShape(fixture)", "runner evidence fixture shape assertion helper"),
    ("const runs = githubObject(fixture?.runs, \"fixture runs\")", "runner evidence fixture runs object assertion"),
    ("const jobs = githubObject(fixture?.jobs, \"fixture jobs\")", "runner evidence fixture jobs object assertion"),
    ("fixture jobs.${lane} must be an array", "runner evidence fixture jobs lane array assertion"),
    ("githubObject(job, `fixture jobs.${lane}[${index}]`)", "runner evidence fixture job entry assertion"),
    ("assertFixtureShape(fixture)", "runner evidence fixture shape assertion wiring"),
    ("const expectedReleaseTag = releaseTag || String(releaseRun?.head_branch || \"\")", "live release tag handoff"),
    ("is missing workflow path", "missing workflow path failure"),
    ("want .github/workflows/release.yml", "wrong release workflow negative assertion"),
    ("release run is missing required jobs: ${RELEASE_SELFTEST_JOB}", "missing release job negative assertion"),
    (
        "release dry-run run is missing required jobs: build (udb-linux-amd64-full)",
        "missing release dry-run job negative assertion",
    ),
    ("release dry-run run 9 used event push, want workflow_dispatch", "wrong release dry-run event assertion"),
    ("exactly one build-broker job", "single build-broker failure"),
    ("PR CI run is missing required artifact-path job: quick-gate", "missing PR quick-gate assertion"),
    ("PR CI run has duplicate artifact-path job smoke; found 2", "duplicate PR smoke assertion"),
    ("PR CI required gate run is missing required jobs: Proto (buf)", "missing PR required job negative assertion"),
    ("PR CI run is missing required jobs: Rust (ubuntu-latest)", "missing PR advisory job negative assertion"),
    ("missing PR advisory job regression was not caught", "missing PR advisory job negative selftest"),
    ("PHP SDK (pest)", "PR SDK static job evidence"),
    ("SDK conformance (all languages)", "PR SDK conformance job evidence"),
    ("Slim build (postgres-only)", "PR slim build job evidence"),
    ("Feature check (all-features)", "PR feature subset job evidence"),
    ("Supply chain policy", "PR supply-chain job evidence"),
    ("Markdown local links + readiness artifacts", "PR docs-links job evidence"),
    ("Native services + canonical stores (live)", "integration required display-name job evidence"),
    ("Rust (ubuntu-latest)", "integration Rust Linux job evidence"),
    ("Auth binary (linux-amd64)", "integration release-binary matrix evidence"),
    ("Plugin feature (runtime-logging)", "integration full plugin matrix evidence"),
    ("Proto (buf)", "integration proto job evidence"),
    ("SDK conformance (all languages)", "integration SDK conformance job evidence"),
    (
        "integration CI run is missing required jobs: Proto (buf)",
        "missing integration full CI job negative assertion",
    ),
    (
        "integration CI run is missing required jobs: Native services + canonical stores (live)",
        "missing integration display-name job negative assertion",
    ),
    ("publish-docker / publish ghcr.io/fahara02/udb", "release required job evidence"),
    ("Vendored ffmpeg guard", "release dry-run ffmpeg gate evidence"),
    ("build (udb-linux-amd64-full)", "release dry-run full asset job evidence"),
    ("Release binary + SDK live benchmarks", "post-release benchmark job evidence"),
    ("post-release benchmark run 14 used event workflow_dispatch, want workflow_run", "wrong benchmark event negative assertion"),
    ("post-release benchmark run is missing required jobs: Release binary + SDK live benchmarks", "missing benchmark job negative assertion"),
    ("post-benchmark Pages run 15 used branch release/v0.3.7, want main", "wrong Pages branch negative assertion"),
    ("post-benchmark Pages run is missing required jobs: deploy", "missing Pages deploy negative assertion"),
    ("Branch protection required checks match docs", "branch-protection required job evidence"),
    ("Scaffold examples compile (six SDKs)", "scaffold artifact consumer assertion"),
    ("function runJobsUrl(repo, runId, page)", "runner evidence jobs pagination URL helper"),
    ("per_page=${MAX_GITHUB_JOBS_PAGE_SIZE}", "GitHub jobs endpoint page-size request"),
    ("actions/runs/${runId}/jobs", "GitHub jobs endpoint"),
    ("function githubObject(value, label)", "GitHub API object assertion helper"),
    ("const run = githubObject(payload, `run ${runId} response`)", "GitHub exact run object assertion"),
    (
        "const actualRunId = assertPositiveIntegerEvidenceToken(run.id, `run ${runId} response id`)",
        "GitHub exact run id token extraction",
    ),
    ("response id ${actualRunId || \"(missing)\"}, want ${runId}", "GitHub exact run id mismatch rejection"),
    ("function githubArrayField(payload, field, label)", "GitHub API array field assertion"),
    ("must be a JSON object", "GitHub API object response assertion"),
    ("response must include ${field} array", "GitHub API array field rejection"),
    ("function githubTotalCount(payload, label)", "GitHub jobs total_count assertion"),
    ("response must include non-negative integer total_count", "GitHub jobs total_count rejection"),
    ("payload.total_count > MAX_GITHUB_RUN_JOBS", "GitHub jobs total_count ceiling assertion"),
    ("response total_count ${payload.total_count} exceeds ${MAX_GITHUB_RUN_JOBS}", "GitHub jobs total_count ceiling rejection"),
    ("pageJobs.forEach((job, index) => githubObject(job", "GitHub job entry object assertion"),
    ("pageJobs.length > MAX_GITHUB_JOBS_PAGE_SIZE", "GitHub jobs page-size assertion"),
    ("response returned ${pageJobs.length} jobs, max ${MAX_GITHUB_JOBS_PAGE_SIZE}", "GitHub jobs oversized page rejection"),
    ("pageTotalCount !== totalCount", "GitHub jobs stable total_count assertion"),
    ("pagination total_count changed", "GitHub jobs total_count drift rejection"),
    ("githubArrayField(payload, \"workflow_runs\"", "GitHub workflow_runs shape assertion"),
    ("githubObject(candidate, `${workflow} runs workflow_runs[${index}]`", "GitHub workflow run entry object assertion"),
    ("per_page: String(MAX_GITHUB_WORKFLOW_RUN_CANDIDATES)", "GitHub workflow-run candidate limit request"),
    ("runs.length > MAX_GITHUB_WORKFLOW_RUN_CANDIDATES", "GitHub workflow-run candidate ceiling assertion"),
    ("workflow_runs, max ${MAX_GITHUB_WORKFLOW_RUN_CANDIDATES}", "GitHub workflow-run candidate ceiling rejection"),
    ("candidate.status !== \"completed\"", "GitHub workflow-run completed-status discovery filter"),
    ("page=2", "GitHub jobs pagination second-page assertion"),
    ("jobs pagination returned", "GitHub jobs pagination truncation failure"),
    ("actions/workflows/${encodeURIComponent(workflow)}/runs", "GitHub workflow runs endpoint"),
    ("no successful completed", "missing runner evidence failure"),
    ("over-budget PR run regression was not caught", "over-budget negative selftest"),
    ("inflated budget override regression was not caught", "inflated budget override negative selftest"),
    ("tightened budget override was rejected", "tightened budget override positive selftest"),
    ("padded numeric budget override regression was not caught", "padded numeric budget override negative selftest"),
    ("non-decimal budget override regression was not caught", "non-decimal budget override negative selftest"),
    ("inflated max evidence-age override regression was not caught", "inflated max evidence-age override negative selftest"),
    ("tightened max evidence-age override was rejected", "tightened max evidence-age override positive selftest"),
    ("empty max evidence-age override regression was not caught", "empty max evidence-age override negative selftest"),
    ("padded release-tag override regression was not caught", "padded release-tag override negative selftest"),
    ("canonical release-tag override was rejected", "canonical release-tag positive selftest"),
    ("padded branch override regression was not caught", "padded branch override negative selftest"),
    ("whitespace branch override regression was not caught", "whitespace branch override negative selftest"),
    ("non-canonical branch override regression was not caught", "non-canonical branch override negative selftest"),
    ("canonical branch override was rejected", "canonical branch positive selftest"),
    ("padded repo override regression was not caught", "padded repo override negative selftest"),
    ("malformed repo override regression was not caught", "malformed repo override negative selftest"),
    ("canonical repo override was rejected", "canonical repo positive selftest"),
    ("padded run-id override regression was not caught", "padded run-id override negative selftest"),
    ("non-numeric run-id override regression was not caught", "non-numeric run-id override negative selftest"),
    ("zero run-id override regression was not caught", "zero run-id override negative selftest"),
    ("canonical run-id override was rejected", "canonical run-id positive selftest"),
    ("wrong lint branch regression was not caught", "wrong lint branch negative selftest"),
    ("missing fixture runs regression was not caught", "missing fixture runs negative selftest"),
    ("non-array fixture jobs regression was not caught", "non-array fixture jobs negative selftest"),
    ("malformed fixture job regression was not caught", "malformed fixture job negative selftest"),
    ("lint/actionlint run 19 used branch feature/lint-proof, want main", "wrong lint branch negative assertion"),
    ("stale runner evidence regression was not caught", "stale runner evidence negative selftest"),
    ("late completed_at budget regression was not caught", "late completed_at budget negative selftest"),
    ("PR CI required gate run 2 required lane took 9.00 min, budget 8 min", "required PR lane budget failure"),
    ("integration CI run 3 took 31.00 min, budget 30 min", "late completed_at budget failure"),
    ("duplicate build-broker regression was not caught", "duplicate build-broker negative selftest"),
    ("duplicate PR smoke regression was not caught", "duplicate PR smoke negative selftest"),
    ("missing PR quick-gate regression was not caught", "missing PR quick-gate negative selftest"),
    ("missing PR required job regression was not caught", "missing PR required job negative selftest"),
    ("wrong workflow evidence regression was not caught", "wrong workflow negative selftest"),
    ("malformed release tag regression was not caught", "malformed release tag negative selftest"),
    ("release run 4 has invalid release tag vnext; want vMAJOR.MINOR.PATCH", "malformed release tag negative assertion"),
    ("padded release tag regression was not caught", "padded release tag negative selftest"),
    ("release run 4 has invalid release tag  v0.3.7; want vMAJOR.MINOR.PATCH", "padded release tag negative assertion"),
    ("duplicate run evidence regression was not caught", "duplicate run evidence negative selftest"),
    ("padded distinct run id regression was not caught", "padded distinct run id negative selftest"),
    ("second evidence run id has invalid value  2; want positive integer", "padded distinct run id negative assertion"),
    ("wrong job run_id regression was not caught", "wrong job run_id negative selftest"),
    ("release job ${RELEASE_SELFTEST_JOB} belongs to run 999, want 4", "wrong job run_id negative assertion"),
    ("padded job run_id regression was not caught", "padded job run_id negative selftest"),
    ("missing job id regression was not caught", "missing job id negative selftest"),
    ("padded job id regression was not caught", "padded job id negative selftest"),
    ("duplicate job id regression was not caught", "duplicate job id negative selftest"),
    ("padded job name regression was not caught", "padded job name negative selftest"),
    ("non-string job name regression was not caught", "non-string job name negative selftest"),
    ("missing run_attempt regression was not caught", "missing run_attempt negative selftest"),
    ("missing PR head_sha regression was not caught", "missing PR SHA negative selftest"),
    ("PR CI run 2 has invalid head_sha (missing); want 40 hex characters", "missing PR SHA negative assertion"),
    ("wrong run html_url regression was not caught", "wrong run inspection URL negative selftest"),
    ("cross-repo run html_url regression was not caught", "cross-repo run inspection URL negative selftest"),
    ("wrong job run_attempt regression was not caught", "wrong job run_attempt negative selftest"),
    (
        "release job ${RELEASE_SELFTEST_JOB} belongs to run attempt 1, want 2",
        "wrong job run_attempt negative assertion",
    ),
    ("padded job run_attempt regression was not caught", "padded job run_attempt negative selftest"),
    ("non-required job timestamp scope regression", "non-required job timestamp scope selftest"),
    ("impossible job timestamp regression was not caught", "impossible job timestamp negative selftest"),
    (
        "release job ${RELEASE_SELFTEST_JOB} completed before it started",
        "impossible job timestamp negative assertion",
    ),
    ("padded run timestamp regression was not caught", "padded run timestamp negative selftest"),
    ("offset job timestamp regression was not caught", "offset job timestamp negative selftest"),
    ("wrong integration branch evidence regression was not caught", "wrong integration branch negative selftest"),
    ("used branch feature/not-main, want main", "wrong integration branch negative assertion"),
    (
        "missing integration display-name job regression was not caught",
        "missing integration display-name job negative selftest",
    ),
    (
        "missing integration full-CI job regression was not caught",
        "missing integration full CI job negative selftest",
    ),
    ("missing release job regression was not caught", "missing release job negative selftest"),
    ("wrong release dry-run event regression was not caught", "wrong release dry-run event negative selftest"),
    ("missing release dry-run job regression was not caught", "missing release dry-run job negative selftest"),
    ("wrong release dry-run head_sha regression was not caught", "wrong release dry-run SHA negative selftest"),
    ("wrong release dry-run tag regression was not caught", "wrong release dry-run tag negative selftest"),
    ("release dry-run run 8 used release tag v0.3.8, want v0.3.7", "wrong release dry-run tag negative assertion"),
    ("release dry-run run 8 used head_sha ${benchmarkSha}, want ${releaseSha}", "wrong release dry-run SHA negative assertion"),
    ("wrong benchmark event regression was not caught", "wrong benchmark event negative selftest"),
    ("missing benchmark job regression was not caught", "missing benchmark job negative selftest"),
    ("wrong Pages release branch regression was not caught", "wrong Pages branch negative selftest"),
    ("missing Pages deploy regression was not caught", "missing Pages deploy negative selftest"),
    ("wrong benchmark head_sha regression was not caught", "wrong benchmark SHA negative selftest"),
    ("post-release benchmark run 12 used head_sha ${benchmarkSha}, want ${releaseSha}", "wrong benchmark SHA negative assertion"),
    ("wrong Pages head_sha regression was not caught", "wrong Pages SHA negative selftest"),
    ("post-benchmark Pages run 13 used head_sha ${pagesSha}, want ${releaseSha}", "wrong Pages SHA negative assertion"),
    ("missing release head_sha regression was not caught", "missing release SHA negative selftest"),
    ("padded release head_sha regression was not caught", "padded release SHA negative selftest"),
    ("uppercase release head_sha regression was not caught", "uppercase release SHA negative selftest"),
    ("release run 4 has invalid head_sha  ${releaseSha}; want 40 hex characters", "padded release SHA negative assertion"),
    ("release run 4 has invalid head_sha ${releaseSha.toUpperCase()}; want 40 hex characters", "uppercase release SHA negative assertion"),
    ("wrong benchmark head_sha regression was not caught", "wrong benchmark SHA negative selftest"),
    ("post-release benchmark run 12 used head_sha ${benchmarkSha}, want ${releaseSha}", "wrong benchmark SHA negative assertion"),
    ("wrong Pages head_sha regression was not caught", "wrong Pages SHA negative selftest"),
    ("post-benchmark Pages run 13 used head_sha ${pagesSha}, want ${releaseSha}", "wrong Pages SHA negative assertion"),
    ("malformed benchmark head_sha regression was not caught", "malformed benchmark SHA negative selftest"),
    (
        "post-release benchmark run 12 has invalid head_sha not-a-sha; want 40 hex characters",
        "malformed benchmark SHA negative assertion",
    ),
    ("early benchmark ordering regression was not caught", "early benchmark ordering negative selftest"),
    ("post-release benchmark run 12 started before release run 4 completed", "early benchmark ordering negative assertion"),
    ("early Pages ordering regression was not caught", "early Pages ordering negative selftest"),
    ("post-benchmark Pages run 13 started before benchmark run 12 completed", "early Pages ordering negative assertion"),
    ("wrong branch-protection event regression was not caught", "wrong branch-protection event negative selftest"),
    ("wrong branch-protection branch regression was not caught", "wrong branch-protection branch negative selftest"),
    (
        "branch-protection run 18 used branch feature/branch-protection-proof, want main",
        "wrong branch-protection branch negative assertion",
    ),
    ("missing branch-protection job regression was not caught", "missing branch-protection job negative selftest"),
    ("wrong branch-protection head_sha regression was not caught", "wrong branch-protection SHA negative selftest"),
    ("branch-protection run 10 used head_sha ${benchmarkSha}, want ${integrationSha}", "wrong branch-protection SHA negative assertion"),
    ("function fixtureJob(name, conclusion = \"success\"", "runner evidence selftest job fixture"),
    ("skipped release job regression was not caught", "skipped release job negative selftest"),
    ("release job ${RELEASE_SELFTEST_JOB} did not succeed: skipped", "skipped release job negative assertion"),
    ("duplicate release job regression was not caught", "duplicate required job negative selftest"),
    (
        "release run has duplicate required job ${RELEASE_SELFTEST_JOB}; found 2",
        "duplicate required job negative assertion",
    ),
    ("paginated jobs regression was not caught", "jobs pagination negative selftest"),
    ("truncated jobs pagination regression was not caught", "jobs pagination truncation negative selftest"),
    ("overreported jobs pagination regression was not caught", "jobs pagination over-count negative selftest"),
    ("oversized jobs page regression was not caught", "jobs pagination oversized-page negative selftest"),
    ("missing jobs array regression was not caught", "missing jobs array negative selftest"),
    ("missing jobs total_count regression was not caught", "missing jobs total_count negative selftest"),
    ("oversized jobs total_count regression was not caught", "oversized jobs total_count negative selftest"),
    ("changed jobs total_count regression was not caught", "changed jobs total_count negative selftest"),
    ("incomplete workflow run discovery regression was not caught", "incomplete workflow-run discovery negative selftest"),
    ("malformed exact run response regression was not caught", "malformed exact run response negative selftest"),
    ("missing exact run id regression was not caught", "missing exact run id negative selftest"),
    (
        "run 131 response id has invalid value (missing); want positive integer",
        "missing exact run id token assertion",
    ),
    ("wrong exact run id regression was not caught", "wrong exact run id negative selftest"),
    ("padded exact run id regression was not caught", "padded exact run id negative selftest"),
    ("run 134 response id has invalid value  134; want positive integer", "padded exact run id token assertion"),
    ("malformed job entry regression was not caught", "malformed job entry negative selftest"),
    ("malformed workflow runs response regression was not caught", "malformed workflow_runs negative selftest"),
    ("malformed workflow run entry regression was not caught", "malformed workflow run entry negative selftest"),
    ("bounded workflow run discovery regression was not caught", "bounded workflow-run discovery negative selftest"),
    ("workflow run discovery candidate limit was not requested", "workflow-run discovery request limit selftest"),
    ("oversized workflow runs response regression was not caught", "oversized workflow_runs response negative selftest"),
    ("oversized GitHub API response regression was not caught", "GitHub API oversized response negative selftest"),
    ("missing GitHub API status-code regression was not caught", "GitHub API missing status-code negative selftest"),
    ("malformed GitHub API status-code regression was not caught", "GitHub API malformed status-code negative selftest"),
    ("non-success GitHub API status-code regression was not caught", "GitHub API non-success status-code negative selftest"),
    ("missing GitHub API content-type regression was not caught", "GitHub API missing content-type negative selftest"),
    ("padded GitHub API content-type regression was not caught", "GitHub API padded content-type negative selftest"),
    ("non-JSON GitHub API content-type regression was not caught", "GitHub API non-JSON content-type negative selftest"),
    ("duplicate-key GitHub API response regression was not caught", "GitHub API duplicate-key negative selftest"),
    ("GitHub API request timeout regression was not caught", "GitHub API request timeout negative selftest"),
    ("fixture directory regression was not caught", "fixture directory negative selftest"),
    ("oversized fixture regression was not caught", "oversized fixture negative selftest"),
    ("duplicate fixture key regression was not caught", "duplicate fixture key negative selftest"),
    ("malformed run id regression was not caught", "malformed run id negative selftest"),
    ("process.exit(1)", "hard failure exit"),
)

ERROR_DETAIL_SERVED_SMOKE_REQUIREMENTS = (
    ("ERROR_DETAIL_METADATA_KEY = \"udb-error-detail-bin\"", "canonical trailer key"),
    ("VALIDATION_STATUS = \"INVALID_ARGUMENT\"", "validation proof status lock"),
    ("QUOTA_STATUS = \"RESOURCE_EXHAUSTED\"", "quota proof status lock"),
    ("matches: list[object]", "ErrorDetail duplicate-trailer collector"),
    ("def _trailing_metadata_items(", "ErrorDetail trailing metadata reader"),
    ("def decode_error_detail(", "ErrorDetail trailer decoder"),
    ("def check_error_detail(", "ErrorDetail assertion helper"),
    ("def invoke_expect_error(", "live unary error invoker"),
    ("def validate_runtime_unary_call(", "ErrorDetail runtime unary-call validator"),
    ("runtime unary call must be callable", "ErrorDetail runtime unary-call failure"),
    ("runtime unary factory raised error", "ErrorDetail runtime unary-factory failure"),
    ("runtime unary call raised non-gRPC error", "ErrorDetail runtime non-gRPC unary failure"),
    ('validate_method_path(f"{label} runtime proof", method)', "ErrorDetail runtime method-path validator"),
    ("def _parse_headers(", "ErrorDetail live metadata header parser"),
    ("GRPC_METADATA_NAME_CHARS", "ErrorDetail metadata header-name character allowlist"),
    ("MAX_LIVE_METADATA_COUNT = 32", "ErrorDetail metadata header count ceiling constant"),
    ("MAX_LIVE_METADATA_VALUE_BYTES = 8_192", "ErrorDetail metadata header value ceiling constant"),
    ("MAX_STATUS_MESSAGE_BYTES = 8_192", "ErrorDetail status message byte ceiling constant"),
    (
        "MAX_FIELD_VIOLATION_DESCRIPTION_BYTES = 8_192",
        "ErrorDetail field violation description byte ceiling constant",
    ),
    ("gRPC metadata header name must not include surrounding whitespace", "ErrorDetail metadata header-name whitespace validator"),
    ("gRPC metadata header name must contain only lowercase letters", "ErrorDetail metadata header-name validator"),
    ("gRPC metadata header name must not start with grpc-", "ErrorDetail reserved metadata header validator"),
    ("gRPC binary metadata headers are not supported by --header", "ErrorDetail binary metadata header validator"),
    ("gRPC metadata header value must not include surrounding whitespace", "ErrorDetail metadata header value whitespace validator"),
    ("gRPC metadata header value must not contain control characters", "ErrorDetail metadata header value control-character validator"),
    ("gRPC metadata header value must be <=", "ErrorDetail metadata header value ceiling validator"),
    ("gRPC metadata headers must be <=", "ErrorDetail metadata header count ceiling validator"),
    ("duplicate gRPC metadata header", "ErrorDetail duplicate metadata header validator"),
    ("def _contains_control_character(", "ErrorDetail control-character helper"),
    ("def validate_grpc_target(", "ErrorDetail live gRPC target validator"),
    ("gRPC target must be a host:port authority, not a URL or path", "ErrorDetail URL-shaped target validator"),
    ("gRPC target must not include control characters", "ErrorDetail target control-character validator"),
    ("gRPC target port must be an integer from 1 to 65535", "ErrorDetail target port validator"),
    ("MAX_LIVE_TIMEOUT_SECONDS = 120.0", "ErrorDetail timeout ceiling constant"),
    ("TIMEOUT_DECIMAL_PATTERN", "ErrorDetail timeout decimal pattern"),
    ("def normalize_timeout_seconds(", "ErrorDetail timeout normalizer"),
    ("def validate_timeout_seconds(", "ErrorDetail live timeout validator"),
    ("timeout must be a finite number of seconds", "ErrorDetail finite timeout validator"),
    ("timeout must not include surrounding whitespace", "ErrorDetail timeout surrounding-whitespace validator"),
    ("timeout must be a positive decimal number of seconds", "ErrorDetail timeout decimal-token validator"),
    ("timeout must be greater than 0 seconds", "ErrorDetail positive timeout validator"),
    ("timeout must be <= 120 seconds", "ErrorDetail timeout ceiling validator"),
    ("load_request(", "generated request loader"),
    ("MAX_PROOF_INPUT_BYTES = 1_048_576", "ErrorDetail proof input byte ceiling constant"),
    ("def _read_proof_text(", "ErrorDetail request proof file reader"),
    ("proof file must exist and be a regular file", "ErrorDetail missing proof file validator"),
    ("proof file must be <=", "ErrorDetail oversized proof file validator"),
    ("def validate_request_module_name(", "ErrorDetail request module token validator"),
    ("request module must not include surrounding whitespace", "ErrorDetail request module whitespace validator"),
    ("request module must be a dotted Python module path", "ErrorDetail request module shape validator"),
    ("def validate_request_message_name(", "ErrorDetail request message token validator"),
    ("request message must not include surrounding whitespace", "ErrorDetail request message whitespace validator"),
    ("request message must be a Python identifier", "ErrorDetail request message shape validator"),
    ("could not be imported", "ErrorDetail request module import validator"),
    ("does not expose", "ErrorDetail request message-class validator"),
    ("object_pairs_hook=_reject_duplicate_json_keys", "ErrorDetail duplicate request JSON key parser"),
    ("parse_constant=_reject_non_finite_json_constant", "ErrorDetail non-finite request JSON parser"),
    ("from google.protobuf.message import DecodeError, Message", "ErrorDetail protobuf DecodeError import"),
    ("trailer metadata could not be read", "ErrorDetail trailer metadata read assertion"),
    ("trailer metadata iteration failed", "ErrorDetail trailer metadata iteration assertion"),
    ("trailer metadata must be iterable", "ErrorDetail trailer metadata iterable assertion"),
    ("trailer metadata item could not be read", "ErrorDetail trailer metadata item read assertion"),
    ("trailer metadata item must be a key/value pair", "ErrorDetail trailer metadata item shape assertion"),
    ("trailer metadata key must be a string", "ErrorDetail trailer metadata-key type assertion"),
    ("trailer metadata key must be lowercase", "ErrorDetail trailer metadata-key lowercase assertion"),
    ("trailer must be bytes", "ErrorDetail binary trailer type assertion"),
    ("def rpc_status_message(", "ErrorDetail status message helper"),
    ("gRPC status code could not be read", "ErrorDetail status code read assertion"),
    ("gRPC status code must be a grpc.StatusCode", "ErrorDetail status code type assertion"),
    ("gRPC status message could not be read", "ErrorDetail status message read assertion"),
    ("gRPC status message must be a string", "ErrorDetail status message type assertion"),
    ("gRPC status message must be non-empty", "ErrorDetail status message non-empty assertion"),
    ("gRPC status message must not include surrounding whitespace", "ErrorDetail status message whitespace assertion"),
    ("gRPC status message must not contain control characters", "ErrorDetail status message control-character assertion"),
    ("gRPC status message must be <=", "ErrorDetail status message byte ceiling assertion"),
    ("does not expose protobuf message class", "ErrorDetail request symbol protobuf-class validator"),
    ("is not a protobuf message", "ErrorDetail constructed request protobuf instance validator"),
    ("request JSON must be a valid JSON object", "ErrorDetail malformed request JSON validator"),
    ("request JSON must be a JSON object", "ErrorDetail request JSON object validator"),
    ("request JSON must not contain duplicate key", "ErrorDetail duplicate request JSON key validator"),
    ("request JSON must not contain non-standard constant", "ErrorDetail non-finite request JSON validator"),
    ("ErrorDetail", "ErrorDetail proto import"),
    ("ErrorFieldViolation", "field-violation proto import"),
    ("ErrorKind", "ErrorKind proto import"),
    ("ERROR_KIND_VALIDATION", "validation kind assertion"),
    ("ERROR_KIND_QUOTA", "quota kind assertion"),
    ("got unknown ErrorDetail.kind", "unknown ErrorDetail kind assertion"),
    ("def _assert_error_detail_token(", "decoded ErrorDetail token validator"),
    ("ErrorDetail.backend must be non-empty", "decoded quota backend non-empty assertion"),
    (
        "ErrorDetail.{field} must not include control characters",
        "decoded quota backend/operation control-character assertion",
    ),
    (
        "ErrorDetail.operation must not include surrounding whitespace",
        "decoded quota operation surrounding-whitespace assertion",
    ),
    ("got ErrorDetail.backend", "quota backend identity assertion"),
    ("got ErrorDetail.operation", "quota operation identity assertion"),
    ("field_violations", "field violation assertion"),
    ("field_violations[{index}].field must be non-empty", "field violation field-shape assertion"),
    ("field_violations[{index}].field must not include surrounding whitespace", "field violation field surrounding-whitespace assertion"),
    ("field_violations[{index}].field must not include whitespace", "field violation field embedded-whitespace assertion"),
    ("field_violations[{index}].field must not include control characters", "field violation field control-character assertion"),
    ("field violation {violation.field!r} must include a non-empty description", "field violation description assertion"),
    ("description must not include surrounding whitespace", "field violation description surrounding-whitespace assertion"),
    ("description must not contain control characters", "field violation description control-character assertion"),
    ("description must be <=", "field violation description byte ceiling assertion"),
    (
        "control-character field description regression was not caught",
        "field violation description control-character negative selftest",
    ),
    (
        "oversized field description regression was not caught",
        "field violation description oversized negative selftest",
    ),
    ("quota/backpressure detail must not include field_violations", "quota field-violation absence assertion"),
    ("validation detail must not include retry_after_ms", "validation retry-after absence assertion"),
    ("validation detail must not include backend/operation", "validation backend/operation absence assertion"),
    ("validation proof must include exactly", "validation exact-field assertion"),
    ("got retryable=True, want False", "validation retryable false assertion"),
    ("got retryable=False, want True", "quota retryable assertion"),
    ("got retry_after_ms=100, want >= 200", "quota retry-after floor assertion"),
    ("retry_after_ms", "retry-after assertion"),
    ("def validate_live_proof_inputs(", "live proof input validator"),
    ("def validate_live_check_expectations(", "runtime proof expectation validator"),
    ("def validate_required_expected_token(", "required expectation token validator"),
    ("def validate_runtime_request_message(", "ErrorDetail runtime request-message validator"),
    ("def validate_runtime_metadata(", "ErrorDetail runtime metadata validator"),
    ("def validate_runtime_timeout_seconds(", "ErrorDetail runtime timeout validator"),
    ("def validate_runtime_channel_method(", "ErrorDetail runtime channel-method validator"),
    ("runtime channel must expose callable unary_unary", "ErrorDetail runtime channel-method failure"),
    ("def validate_expected_token(", "ErrorDetail expected-token validator"),
    ("def validate_method_path(", "ErrorDetail live method path validator"),
    ("method must be a full gRPC method path like /package.Service/Method", "ErrorDetail method-path shape assertion"),
    ("method must use protobuf identifier tokens", "ErrorDetail method-path token assertion"),
    ("method must not include surrounding whitespace", "ErrorDetail method-path whitespace assertion"),
    ("method must not include whitespace", "ErrorDetail method-path embedded-whitespace assertion"),
    ("must not include whitespace", "ErrorDetail expectation whitespace assertion"),
    ("validation runtime proof must expect", "runtime validation status semantic lock"),
    ("validation runtime proof requires an expected field", "runtime validation field semantic lock"),
    ("quota runtime proof requires expected backend and operation", "runtime quota backend/operation semantic lock"),
    ("validation proof must expect INVALID_ARGUMENT", "validation proof status validator"),
    ("quota retry/backpressure proof must expect RESOURCE_EXHAUSTED", "quota proof status validator"),
    ("quota retry/backpressure proof requires --quota-retry-after-min-ms > 0", "positive retry-after proof validator"),
    ("quota retry/backpressure proof requires --quota-backend", "quota backend proof validator"),
    ("quota retry/backpressure proof requires --quota-operation", "quota operation proof validator"),
    ("expected exactly one udb-error-detail-bin trailer", "duplicate ErrorDetail trailer assertion"),
    ("invalid udb-error-detail-bin trailer", "malformed ErrorDetail trailer assertion"),
    ("string ErrorDetail trailer regression was not caught", "string trailer negative selftest"),
    ("initial-metadata ErrorDetail regression was not caught", "initial metadata negative selftest"),
    ("--validation-method", "validation method CLI input"),
    ("--validation-request-module", "validation request module CLI input"),
    ("--validation-request-message", "validation request message CLI input"),
    ("--validation-request-json", "validation request JSON CLI input"),
    ("--validation-field", "validation field CLI input"),
    ("--quota-method", "quota method CLI input"),
    ("--quota-request-module", "quota request module CLI input"),
    ("--quota-request-message", "quota request message CLI input"),
    ("--quota-request-json", "quota request JSON CLI input"),
    ("--quota-retry-after-min-ms", "quota retry-after CLI input"),
    ("--quota-backend", "quota backend CLI input"),
    ("--quota-operation", "quota operation CLI input"),
    ("REQUIRED_LIVE_PROOF_INPUTS", "complete live proof input set"),
    ("def missing_required_live_proofs(", "complete proof input checker"),
    ("--require-all-proofs", "complete proof CLI gate"),
    ("error detail served smoke selftest passed", "selftest success marker"),
    ("error detail served smoke passed", "live success marker"),
    ("missing field regression was not caught", "negative selftest"),
    ("non-grpc StatusCode regression was not caught", "non-grpc StatusCode negative selftest"),
    ("unreadable status code regression was not caught", "unreadable status code negative selftest"),
    ("empty status message regression was not caught", "empty status message negative selftest"),
    ("unreadable status message regression was not caught", "unreadable status message negative selftest"),
    ("non-string status message regression was not caught", "non-string status message negative selftest"),
    ("padded status message regression was not caught", "padded status message negative selftest"),
    ("control-character status message regression was not caught", "control-character status message negative selftest"),
    ("oversized status message regression was not caught", "oversized status message negative selftest"),
    ("extra validation field regression was not caught", "extra validation field negative selftest"),
    ("empty field description regression was not caught", "empty field-description negative selftest"),
    ("malformed extra field violation regression was not caught", "malformed field-violation negative selftest"),
    ("spaced field violation regression was not caught", "field-violation field surrounding-whitespace negative selftest"),
    ("embedded-space field violation regression was not caught", "field-violation field embedded-whitespace negative selftest"),
    ("control-character field violation regression was not caught", "field-violation field control-character negative selftest"),
    ("padded field description regression was not caught", "field-violation description surrounding-whitespace negative selftest"),
    ("validation retry-after regression was not caught", "validation retry-after negative selftest"),
    ("validation retryable regression was not caught", "validation retryable negative selftest"),
    ("validation backend/operation regression was not caught", "validation backend/operation negative selftest"),
    ("quota retryable regression was not caught", "quota retryable negative selftest"),
    ("quota retry-after floor regression was not caught", "quota retry-after floor negative selftest"),
    ("quota field-violations regression was not caught", "quota field-violations negative selftest"),
    ("quota backend/operation regression was not caught", "quota backend/operation negative selftest"),
    ("quota backend token regression was not caught", "quota backend token negative selftest"),
    ("quota operation token regression was not caught", "quota operation token negative selftest"),
    ("canonical timeout string was rejected", "canonical timeout string positive selftest"),
    ("padded timeout regression was not caught", "padded timeout negative selftest"),
    ("non-decimal timeout regression was not caught", "non-decimal timeout negative selftest"),
    ("duplicate ErrorDetail trailer regression was not caught", "duplicate trailer negative selftest"),
    ("unreadable ErrorDetail trailer metadata regression was not caught", "unreadable trailer metadata negative selftest"),
    ("failing ErrorDetail trailer metadata iterator regression was not caught", "failing trailer metadata iterator negative selftest"),
    ("non-iterable ErrorDetail trailer metadata regression was not caught", "non-iterable trailer metadata negative selftest"),
    ("failing ErrorDetail trailer metadata item regression was not caught", "failing trailer metadata item negative selftest"),
    ("malformed ErrorDetail trailer metadata item regression was not caught", "malformed trailer metadata item negative selftest"),
    ("non-string ErrorDetail trailer metadata key regression was not caught", "non-string trailer metadata-key negative selftest"),
    ("uppercase ErrorDetail trailer metadata key regression was not caught", "uppercase trailer metadata-key negative selftest"),
    ("malformed ErrorDetail trailer regression was not caught", "malformed trailer negative selftest"),
    ("unknown ErrorDetail kind regression was not caught", "unknown ErrorDetail kind negative selftest"),
    ("runtime method-path validation regression was not caught", "runtime method-path negative selftest"),
    ("runtime expected-token validation regression was not caught", "runtime expectation-token negative selftest"),
    ("runtime expected-kind validation regression was not caught", "runtime expectation-kind negative selftest"),
    ("runtime request-message validation regression was not caught", "runtime request-message negative selftest"),
    ("runtime metadata validation regression was not caught", "runtime metadata negative selftest"),
    ("runtime timeout validation regression was not caught", "runtime timeout negative selftest"),
    ("runtime channel-method validation regression was not caught", "runtime channel-method negative selftest"),
    ("runtime unary-call validation regression was not caught", "runtime unary-call negative selftest"),
    ("runtime unary-factory validation regression was not caught", "runtime unary-factory negative selftest"),
    (
        "runtime unary non-gRPC error validation regression was not caught",
        "runtime unary non-gRPC error negative selftest",
    ),
    ("runtime validation semantics regression was not caught", "runtime validation semantics negative selftest"),
    ("runtime validation field semantics regression was not caught", "runtime validation field semantics negative selftest"),
    ("runtime quota semantics regression was not caught", "runtime quota semantics negative selftest"),
    ("quota proof missing positive retry-after", "quota retry-after negative selftest"),
    ("validation proof malformed method path", "validation method-path negative selftest"),
    ("validation proof method path has embedded whitespace", "validation method-path embedded-whitespace negative selftest"),
    ("validation proof method path has malformed token", "validation method-path token negative selftest"),
    ("validation proof field has surrounding whitespace", "validation expected-field whitespace negative selftest"),
    ("validation proof field has control character", "validation expected-field control-character negative selftest"),
    ("quota proof method path has surrounding whitespace", "quota method-path whitespace negative selftest"),
    ("quota proof method path has embedded whitespace", "quota method-path embedded-whitespace negative selftest"),
    ("quota proof method path has malformed token", "quota method-path token negative selftest"),
    ("quota proof missing backend", "quota backend proof negative selftest"),
    ("quota proof missing operation", "quota operation proof negative selftest"),
    ("quota proof backend has embedded whitespace", "quota backend whitespace negative selftest"),
    ("quota proof backend has control character", "quota backend control-character negative selftest"),
    ("quota proof operation has surrounding whitespace", "quota operation whitespace negative selftest"),
    ("quota proof operation has control character", "quota operation control-character negative selftest"),
    ("whitespace-only required proof input regression was not caught", "required proof whitespace negative selftest"),
    ("whitespace-only focused proof readiness regression was not caught", "focused proof whitespace negative selftest"),
    ("array request JSON", "request JSON-array negative selftest"),
    ("missing request JSON file", "missing request JSON file negative selftest"),
    ("missing request module", "missing request module negative selftest"),
    ("spaced request module", "spaced request module negative selftest"),
    ("malformed request module", "malformed request module negative selftest"),
    ("missing request message", "missing request message negative selftest"),
    ("spaced request message", "spaced request message negative selftest"),
    ("malformed request message", "malformed request message negative selftest"),
    ("non-message request symbol", "non-message request symbol negative selftest"),
    ("oversized request JSON file", "oversized request JSON file negative selftest"),
    ("malformed request JSON", "malformed request JSON negative selftest"),
    ("duplicate-key request JSON", "duplicate request JSON key negative selftest"),
    ("non-finite request JSON", "non-finite request JSON negative selftest"),
    ("duplicate live gRPC header regression was not caught", "duplicate metadata header negative selftest"),
    ("uppercase gRPC header name regression was not caught", "uppercase metadata header-name negative selftest"),
    ("spaced gRPC header name regression was not caught", "spaced metadata header-name negative selftest"),
    ("spaced gRPC header value regression was not caught", "spaced metadata header value negative selftest"),
    ("malformed gRPC header name regression was not caught", "malformed metadata header-name negative selftest"),
    ("reserved gRPC header name regression was not caught", "reserved metadata header-name negative selftest"),
    ("binary gRPC header name regression was not caught", "binary metadata header-name negative selftest"),
    ("control-character gRPC header value regression was not caught", "control-character metadata value negative selftest"),
    ("oversized gRPC header value regression was not caught", "oversized metadata value negative selftest"),
    ("excessive gRPC header count regression was not caught", "excessive metadata count negative selftest"),
    ("URL-shaped gRPC target regression was not caught", "URL-shaped target negative selftest"),
    ("whitespace gRPC target regression was not caught", "whitespace target negative selftest"),
    ("control-character gRPC target regression was not caught", "control-character target negative selftest"),
    ("missing-port gRPC target regression was not caught", "missing-port target negative selftest"),
    ("non-positive timeout regression was not caught", "non-positive timeout negative selftest"),
    ("infinite timeout regression was not caught", "infinite timeout negative selftest"),
    ("excessive timeout regression was not caught", "excessive timeout negative selftest"),
    ("validation proof status weakened", "validation status negative selftest"),
    ("quota proof status weakened", "quota status negative selftest"),
)

IDEMPOTENCY_SERVED_SMOKE_REQUIREMENTS = (
    ("def check_replay(", "keyed Upsert replay checker"),
    ("def check_tenant_isolation(", "tenant isolation checker"),
    ("def check_batch_replay(", "BatchUpsert replay checker"),
    ("def check_fail_closed(", "fail-closed checker"),
    ("expected_code = validate_fail_closed_status(code)", "idempotency runtime fail-closed status lock"),
    ("REQUIRED_LIVE_PROOF_INPUTS", "complete live proof input set"),
    ("FAIL_CLOSED_STATUS = \"UNAVAILABLE\"", "fail-closed status lock"),
    ("def missing_required_live_proofs(", "complete proof input checker"),
    ("def validate_live_proof_inputs(", "live proof input semantics validator"),
    ("def validate_tenant_isolation_requests(", "tenant isolation shared request validator"),
    ("def validate_batch_upsert_payload_pair(", "BatchUpsert shared payload-field validator"),
    ("def validate_fail_closed_requests(", "fail-closed shared request validator"),
    ("def validate_fail_closed_freshness_scope(", "fail-closed/keyless shared scope validator"),
    ("def validate_fail_closed_freshness_payload(", "fail-closed/keyless shared payload validator"),
    ("def validate_fail_closed_status(", "idempotency fail-closed status token validator"),
    ("def validate_upsert_request_message(", "idempotency runtime request-message validator"),
    ("def validate_runtime_stub_method(", "idempotency runtime stub-method validator"),
    ("runtime stub must expose callable", "idempotency runtime stub-method failure"),
    ("def validate_runtime_mutation_response(", "idempotency runtime response-message validator"),
    ("runtime response must be a MutationResponse", "idempotency runtime response-message failure"),
    ("def call_runtime_mutation(", "idempotency runtime call wrapper"),
    ("allow_rpc_error: bool = False", "idempotency runtime call-error opt-in flag"),
    ("allow_rpc_error=True", "idempotency fail-closed runtime RpcError passthrough"),
    ("runtime call raised error", "idempotency runtime call-error failure"),
    ("runtime call raised unexpected gRPC error", "idempotency unexpected runtime RpcError failure"),
    ("def validate_message_type_token(", "idempotency message_type token validator"),
    ("def validate_upsert_payload(", "Upsert record_json validator"),
    ("--tenant2-header", "tenant isolation metadata CLI"),
    ("tenant2_metadata = _parse_headers(args.tenant2_header) if args.tenant2_header else metadata", "tenant isolation metadata fallback"),
    ("check_tenant_isolation(stub, upsert, tenant2, tenant2_metadata, timeout)", "tenant isolation metadata wiring"),
    ('validate_keyed_upsert("keyed Upsert replay proof", request)', "keyed Upsert runtime request validator"),
    ("keyed Upsert runtime empty-key regression was not caught", "keyed Upsert runtime empty-key negative selftest"),
    ("def _parse_headers(", "idempotency live metadata header parser"),
    ("GRPC_METADATA_NAME_CHARS", "idempotency metadata header-name character allowlist"),
    ("MAX_LIVE_METADATA_COUNT = 32", "idempotency metadata header count ceiling constant"),
    ("MAX_LIVE_METADATA_VALUE_BYTES = 8_192", "idempotency metadata header value ceiling constant"),
    ("gRPC metadata header name must not include surrounding whitespace", "idempotency metadata header-name whitespace validator"),
    ("gRPC metadata header name must contain only lowercase letters", "idempotency metadata header-name validator"),
    ("gRPC metadata header name must not start with grpc-", "idempotency reserved metadata header validator"),
    ("gRPC binary metadata headers are not supported by --header", "idempotency binary metadata header validator"),
    ("gRPC metadata header value must not include surrounding whitespace", "idempotency metadata header value whitespace validator"),
    ("gRPC metadata header value must not contain control characters", "idempotency metadata header value control-character validator"),
    ("gRPC metadata header value must be <=", "idempotency metadata header value ceiling validator"),
    ("gRPC metadata headers must be <=", "idempotency metadata header count ceiling validator"),
    ("duplicate gRPC metadata header", "idempotency duplicate metadata header validator"),
    ("def _contains_control_character(", "idempotency control-character helper"),
    ("def validate_grpc_target(", "idempotency live gRPC target validator"),
    ("gRPC target must be a host:port authority, not a URL or path", "idempotency URL-shaped target validator"),
    ("gRPC target must not include control characters", "idempotency target control-character validator"),
    ("gRPC target port must be an integer from 1 to 65535", "idempotency target port validator"),
    ("MAX_LIVE_TIMEOUT_SECONDS = 120.0", "idempotency timeout ceiling constant"),
    ("TIMEOUT_DECIMAL_PATTERN", "idempotency timeout decimal pattern"),
    ("def normalize_timeout_seconds(", "idempotency timeout normalizer"),
    ("def validate_timeout_seconds(", "idempotency live timeout validator"),
    ("def validate_runtime_metadata(", "idempotency runtime metadata validator"),
    ("def validate_runtime_timeout_seconds(", "idempotency runtime timeout validator"),
    ("def validate_runtime_transport_inputs(", "idempotency runtime transport validator"),
    ("timeout must be a finite number of seconds", "idempotency finite timeout validator"),
    ("timeout must not include surrounding whitespace", "idempotency timeout surrounding-whitespace validator"),
    ("timeout must be a positive decimal number of seconds", "idempotency timeout decimal-token validator"),
    ("timeout must be greater than 0 seconds", "idempotency positive timeout validator"),
    ("timeout must be <= 120 seconds", "idempotency timeout ceiling validator"),
    ("MAX_PROOF_INPUT_BYTES = 1_048_576", "idempotency proof input byte ceiling constant"),
    ("MAX_FAIL_CLOSED_ERROR_MESSAGE_BYTES = 8_192", "idempotency fail-closed error-message byte ceiling constant"),
    ("def _read_proof_text(", "idempotency proof file reader"),
    ("proof file must exist and be a regular file", "idempotency missing proof file validator"),
    ("proof file must be <=", "idempotency oversized proof file validator"),
    ("object_pairs_hook=_reject_duplicate_json_keys", "idempotency duplicate proof JSON key parser"),
    ("parse_constant=_reject_non_finite_json_constant", "idempotency non-finite proof JSON parser"),
    ("proof JSON must not contain non-standard constant", "idempotency non-finite proof JSON assertion"),
    ("def _assert_restored_summary(", "idempotency replay summary assertion helper"),
    ("def _read_rpc_status_code(", "idempotency fail-closed gRPC status-code reader"),
    ("gRPC status code must be readable", "idempotency fail-closed status-code readability assertion"),
    ("gRPC status code could not be read", "idempotency fail-closed status-code read assertion"),
    ("gRPC status code must be a grpc.StatusCode", "idempotency fail-closed status-code type assertion"),
    ("_read_rpc_status_code(\"fail-closed keyed Upsert\", error).name", "idempotency fail-closed status-code reader wiring"),
    ("def _assert_rpc_error_message(", "idempotency fail-closed error-message assertion helper"),
    ("gRPC error message must be readable", "idempotency fail-closed error-message readability assertion"),
    ("gRPC error message must be a string", "idempotency fail-closed error-message type assertion"),
    ("gRPC error message must be non-empty", "idempotency fail-closed error-message non-empty assertion"),
    ("gRPC error message must not include surrounding whitespace", "idempotency fail-closed error-message whitespace assertion"),
    ("gRPC error message must not contain control characters", "idempotency fail-closed error-message control-character assertion"),
    ("gRPC error message must be <=", "idempotency fail-closed error-message byte ceiling assertion"),
    ("gRPC error message must identify idempotency dedup", "idempotency fail-closed error-message identity assertion"),
    (
        "dedup-store-down fail-closed proof status must be non-empty",
        "idempotency fail-closed empty-status validator",
    ),
    ("def validate_proof_token(", "idempotency proof token validator"),
    ("idempotency_key must not include surrounding whitespace", "idempotency key surrounding-whitespace validator"),
    ("context.tenant_id must not include whitespace", "idempotency tenant embedded-whitespace validator"),
    ("context.project_id must not include surrounding whitespace", "idempotency project surrounding-whitespace validator"),
    ("message_type must be non-empty", "idempotency message_type proof validator"),
    ("message_type must not include surrounding whitespace", "idempotency message_type surrounding-whitespace validator"),
    ("message_type must not include whitespace", "idempotency message_type embedded-whitespace validator"),
    ("record_json must be non-empty", "Upsert record_json non-empty proof validator"),
    ("record_json must be a valid JSON object", "Upsert record_json valid JSON validator"),
    ("record_json must not contain duplicate JSON keys", "Upsert record_json duplicate-key validator"),
    ("record_json must not contain non-standard JSON constants", "Upsert record_json non-finite validator"),
    ("record_json must be a JSON object", "Upsert record_json object-shape validator"),
    ("record_json must be a non-empty JSON object", "Upsert record_json non-empty object validator"),
    (
        "must use only one of record_json, record_json_object, or record_json_text",
        "Upsert record_json encoding ambiguity validator",
    ),
    ("record_json_object must be a JSON object", "Upsert record_json_object object helper validator"),
    ("record_json_text must be a string", "Upsert record_json_text string helper validator"),
    ("keyed Upsert duplicate-key payload input", "idempotency duplicate-key record_json negative selftest"),
    ("proof JSON must not contain duplicate key", "idempotency duplicate proof JSON key validator"),
    ("tenant/project key isolation proof requires --upsert-json", "tenant isolation baseline validator"),
    ("tenant/project key isolation proof must use a different tenant_id", "tenant isolation distinct-tenant validator"),
    ("tenant/project key isolation proof must use a different project_id", "tenant isolation distinct-project validator"),
    ("def _validate_payload_scope(", "tenant isolation scope-field validator"),
    ("def _shared_non_scope_payload_fields(", "tenant isolation shared non-scope payload validator"),
    (
        "tenant/project key isolation proof must use scope-correct record_json, not an exact reused payload",
        "tenant isolation exact-payload rejection",
    ),
    (
        "tenant/project key isolation proof must share at least one non-scope record_json field/value",
        "tenant isolation unrelated-payload rejection",
    ),
    ("def validate_tenant_isolation_requests(", "tenant isolation shared request validator"),
    (
        "payload = validate_tenant_isolation_requests(baseline, request)",
        "tenant isolation runtime request validator",
    ),
    (
        "tenant/project key isolation proof must reuse the --upsert-json idempotency_key",
        "tenant isolation shared idempotency-key validator",
    ),
    (
        "tenant/project key isolation proof must reuse the --upsert-json message_type",
        "tenant isolation shared message-type validator",
    ),
    ("second tenant/project keyed Upsert replay", "tenant isolation replay assertion"),
    ("tenant/project replay regression was not caught", "tenant isolation replay negative selftest"),
    ("tenant/project runtime baseline regression was not caught", "tenant isolation runtime baseline negative selftest"),
    ("tenant/project runtime empty-key regression was not caught", "tenant isolation runtime empty-key negative selftest"),
    (
        "tenant/project record_json request binding regression was not caught",
        "tenant isolation record_json request binding negative selftest",
    ),
    ("fresh response affected_rows must be positive", "fresh affected_rows positive assertion"),
    ("fresh affected_rows regression was not caught", "fresh affected_rows negative selftest"),
    ("def _assert_fresh_request_summary(", "keyless fail-closed freshness summary helper"),
    ("def _assert_typed_write_receipt_lockstep(", "idempotency typed write receipt lockstep helper"),
    (
        "fresh response resource_uri must be present for request identity proof",
        "keyless fail-closed freshness resource_uri assertion",
    ),
    (
        "fresh response record_json must be present for request payload proof",
        "keyless fail-closed freshness record_json assertion",
    ),
    (
        "fresh response write_receipt_json must be present for write receipt proof",
        "keyless fail-closed freshness write receipt assertion",
    ),
    (
        "bare keyless fail-closed freshness regression was not caught",
        "bare keyless fail-closed freshness negative selftest",
    ),
    (
        "missing-receipt keyless fail-closed freshness regression was not caught",
        "missing-receipt keyless fail-closed freshness negative selftest",
    ),
    ("MUTATION_ID_PATTERN = re.compile(", "idempotency served replay mutation_id UUID pattern"),
    ("def _assert_mutation_id(", "idempotency served replay mutation_id helper"),
    (
        '_assert_mutation_id(label, first, "first response")',
        "idempotency served replay first mutation_id assertion",
    ),
    (
        '_assert_mutation_id(label, second, "duplicate response")',
        "idempotency served replay duplicate mutation_id assertion",
    ),
    ("first response mutation_id must be non-empty", "idempotency served replay mutation_id presence assertion"),
    (
        "first response mutation_id must be a lowercase UUID",
        "idempotency served replay mutation_id UUID-shape assertion",
    ),
    ("duplicate response mutation_id differs from first response", "duplicate mutation_id replay assertion"),
    ("mutation_id replay regression was not caught", "duplicate mutation_id negative selftest"),
    ("invalid mutation_id replay regression was not caught", "invalid mutation_id negative selftest"),
    ("duplicate response affected_rows differs from first response", "duplicate affected_rows replay assertion"),
    ("affected_rows replay regression was not caught", "duplicate affected_rows negative selftest"),
    ("first response must include at least one replay summary field", "idempotency replay summary presence assertion"),
    ("first response {field} must not be whitespace-only", "idempotency replay summary whitespace-only assertion"),
    ("first response {field} must not include surrounding whitespace", "idempotency replay summary surrounding-whitespace assertion"),
    ("first response resource_uri must start with udb://", "idempotency replay summary resource_uri scheme assertion"),
    (
        "first response resource_uri must include non-empty authority and path",
        "idempotency replay summary resource_uri shape assertion",
    ),
    (
        "first response resource_uri authority must equal request tenant_id",
        "idempotency replay summary tenant-authority assertion",
    ),
    (
        "first response resource_uri path must start with request message_type",
        "idempotency replay summary message-path assertion",
    ),
    (
        "first response resource_uri path must include request message_type and resource id",
        "idempotency replay summary resource-id assertion",
    ),
    (
        "first response resource_uri must be present for request identity proof",
        "idempotency replay summary required resource_uri assertion",
    ),
    (
        "first response resource_uri id must match an identity request field value",
        "idempotency replay summary request-value id assertion",
    ),
    (
        "resource_uri id proof identity field value must not include surrounding whitespace",
        "idempotency replay summary identity surrounding-whitespace assertion",
    ),
    (
        "resource_uri id proof identity field value must not include whitespace",
        "idempotency replay summary identity embedded-whitespace assertion",
    ),
    (
        "resource_uri id proof requires at least one scalar identity request field",
        "idempotency replay summary identity-field requirement",
    ),
    ("first response {field} must be a valid JSON object", "idempotency replay summary JSON-object assertion"),
    ("first response {field} must be a non-empty JSON object", "idempotency replay summary non-empty JSON-object assertion"),
    ("first response {field} must not contain duplicate JSON key", "idempotency replay summary duplicate-key assertion"),
    (
        "first response {field} must not contain non-standard JSON constants",
        "idempotency replay summary non-finite assertion",
    ),
    ("first response write_receipt_json missing fields", "idempotency write receipt required-fields assertion"),
    ("first response write_receipt_json unexpected fields", "idempotency write receipt unexpected-fields assertion"),
    ("write_receipt_json projection_task_ids must be an array", "idempotency write receipt projection task array assertion"),
    (
        "first response write_receipt_json projection_task_ids[{index}] must not contain control characters",
        "idempotency write receipt projection task control-character assertion",
    ),
    (
        "first response write_receipt_json source_lsn must not contain control characters",
        "idempotency write receipt source-lsn control-character assertion",
    ),
    ("write_receipt_json written_at_unix_ms must be a positive integer", "idempotency write receipt timestamp assertion"),
    (
        "typed write_receipt must be present when write_receipt_json is present",
        "idempotency typed write receipt presence assertion",
    ),
    (
        "typed write_receipt must match write_receipt_json",
        "idempotency typed write receipt lockstep assertion",
    ),
    ("duplicate response record_json was absent from first response", "idempotency replay summary no-added-field assertion"),
    ("duplicate response write_receipt_json differs from first response", "idempotency replay summary restoration assertion"),
    ("empty replay summary regression was not caught", "empty replay summary negative selftest"),
    (
        "missing resource_uri identity proof regression was not caught",
        "missing resource_uri identity proof negative selftest",
    ),
    ("keyed Upsert missing identity field regression was not caught", "missing identity-field negative selftest"),
    ("invalid resource_uri replay summary regression was not caught", "invalid resource_uri replay summary negative selftest"),
    (
        "wrong-tenant resource_uri replay summary regression was not caught",
        "wrong-tenant resource_uri replay summary negative selftest",
    ),
    (
        "wrong-message resource_uri replay summary regression was not caught",
        "wrong-message resource_uri replay summary negative selftest",
    ),
    (
        "short-path resource_uri replay summary regression was not caught",
        "short-path resource_uri replay summary negative selftest",
    ),
    (
        "wrong-id resource_uri replay summary regression was not caught",
        "wrong-id resource_uri replay summary negative selftest",
    ),
    (
        "non-identity scalar resource_uri replay summary regression was not caught",
        "non-identity scalar resource_uri replay summary negative selftest",
    ),
    (
        "padded identity resource_uri replay summary regression was not caught",
        "padded identity resource_uri replay summary negative selftest",
    ),
    (
        "embedded-space identity resource_uri replay summary regression was not caught",
        "embedded-space identity resource_uri replay summary negative selftest",
    ),
    ("added replay summary regression was not caught", "added replay summary negative selftest"),
    ("whitespace replay summary regression was not caught", "whitespace replay summary negative selftest"),
    (
        "malformed record_json replay summary regression was not caught",
        "malformed record_json replay summary negative selftest",
    ),
    (
        "malformed write_receipt_json replay summary regression was not caught",
        "malformed write_receipt_json replay summary negative selftest",
    ),
    (
        "missing-fields write_receipt_json replay summary regression was not caught",
        "missing-fields write_receipt_json replay summary negative selftest",
    ),
    (
        "unexpected-field write_receipt_json replay summary regression was not caught",
        "unexpected-field write_receipt_json replay summary negative selftest",
    ),
    (
        "invalid projection_task_ids write_receipt_json replay summary regression was not caught",
        "invalid projection_task_ids write_receipt_json replay summary negative selftest",
    ),
    (
        "duplicate-key record_json replay summary regression was not caught",
        "duplicate-key record_json replay summary negative selftest",
    ),
    (
        "non-finite record_json replay summary regression was not caught",
        "non-finite record_json replay summary negative selftest",
    ),
    (
        "duplicate-key write_receipt_json replay summary regression was not caught",
        "duplicate-key write_receipt_json replay summary negative selftest",
    ),
    (
        "missing typed write_receipt replay summary regression was not caught",
        "missing typed write receipt negative selftest",
    ),
    (
        "mismatched typed write_receipt replay summary regression was not caught",
        "mismatched typed write receipt negative selftest",
    ),
    ("dropped replay summary regression was not caught", "dropped replay summary negative selftest"),
    ("def validate_batch_upsert_requests(", "BatchUpsert shared request validator"),
    (
        "first_payload, _second_payload = validate_batch_upsert_requests(requests)",
        "BatchUpsert runtime request validator",
    ),
    ("BatchUpsert proof first two requests must share idempotency_key", "BatchUpsert duplicate-key validator"),
    ("BatchUpsert proof requires exactly two request objects", "BatchUpsert exact-two validator"),
    ('validate_upsert_payload("BatchUpsert proof first request", first)', "BatchUpsert first payload JSON-object validator"),
    ('validate_upsert_payload("BatchUpsert proof second request", second)', "BatchUpsert second payload JSON-object validator"),
    ("BatchUpsert proof second request must carry semantically different record_json", "BatchUpsert first-writer replay validator"),
    (
        "BatchUpsert proof first two requests must share at least one identity record_json field/value",
        "BatchUpsert shared identity payload-field validator",
    ),
    ("BatchUpsert duplicate item", "BatchUpsert duplicate summary assertion"),
    (
        "BatchUpsert duplicate flag regression was not caught",
        "BatchUpsert duplicate flag negative selftest",
    ),
    ("class _CountingRequestIterator", "BatchUpsert request-stream consumption counter"),
    ("BatchUpsert proof runtime consumed", "BatchUpsert request-stream consumption assertion"),
    (
        "BatchUpsert request-stream consumption regression was not caught",
        "BatchUpsert request-stream consumption negative selftest",
    ),
    (
        "BatchUpsert duplicate affected_rows regression was not caught",
        "BatchUpsert duplicate affected_rows negative selftest",
    ),
    (
        "BatchUpsert duplicate missing mutation_id regression was not caught",
        "BatchUpsert duplicate missing mutation_id negative selftest",
    ),
    (
        "BatchUpsert duplicate item: duplicate response mutation_id must be non-empty",
        "BatchUpsert duplicate missing mutation_id assertion",
    ),
    (
        "BatchUpsert duplicate mutation_id regression was not caught",
        "BatchUpsert duplicate mutation_id negative selftest",
    ),
    (
        "BatchUpsert duplicate item: first response mutation_id must be a lowercase UUID",
        "BatchUpsert duplicate invalid mutation_id assertion",
    ),
    (
        "BatchUpsert duplicate invalid mutation_id regression was not caught",
        "BatchUpsert duplicate invalid mutation_id negative selftest",
    ),
    (
        "BatchUpsert duplicate missing record_json regression was not caught",
        "BatchUpsert duplicate missing record_json negative selftest",
    ),
    (
        "BatchUpsert duplicate record_json regression was not caught",
        "BatchUpsert duplicate record_json negative selftest",
    ),
    (
        "BatchUpsert duplicate missing resource_uri regression was not caught",
        "BatchUpsert duplicate missing resource_uri negative selftest",
    ),
    (
        "BatchUpsert duplicate resource_uri regression was not caught",
        "BatchUpsert duplicate resource_uri negative selftest",
    ),
    (
        "BatchUpsert duplicate missing write_receipt_json regression was not caught",
        "BatchUpsert duplicate missing write_receipt_json negative selftest",
    ),
    (
        "BatchUpsert duplicate write_receipt_json regression was not caught",
        "BatchUpsert duplicate write_receipt_json negative selftest",
    ),
    (
        "BatchUpsert duplicate missing typed write_receipt regression was not caught",
        "BatchUpsert duplicate missing typed write_receipt negative selftest",
    ),
    (
        "BatchUpsert duplicate typed write_receipt regression was not caught",
        "BatchUpsert duplicate typed write_receipt negative selftest",
    ),
    ("BatchUpsert resource_uri scope regression was not caught", "BatchUpsert resource_uri scope negative selftest"),
    ("def _assert_summary_record_json_matches_request(", "BatchUpsert record_json request binding helper"),
    ("first response record_json must include request field/value", "idempotency record_json request binding assertion"),
    (
        "keyed Upsert record_json request binding regression was not caught",
        "keyed Upsert record_json request binding negative selftest",
    ),
    (
        "BatchUpsert record_json request binding regression was not caught",
        "BatchUpsert record_json request binding negative selftest",
    ),
    (
        "BatchUpsert duplicate-key record_json regression was not caught",
        "BatchUpsert duplicate-key record_json negative selftest",
    ),
    (
        "BatchUpsert runtime input validation regression was not caught",
        "BatchUpsert runtime input validation negative selftest",
    ),
    (
        "BatchUpsert runtime request-message validation regression was not caught",
        "BatchUpsert runtime request-message negative selftest",
    ),
    ("BatchUpsert runtime stub validation regression was not caught", "BatchUpsert runtime stub negative selftest"),
    (
        "BatchUpsert proof runtime call raised unexpected gRPC error",
        "BatchUpsert unexpected runtime RpcError failure",
    ),
    ("BatchUpsert returned more than 2 responses, want exactly 2", "BatchUpsert bounded response-count assertion"),
    (
        "BatchUpsert extra runtime response validation regression was not caught",
        "BatchUpsert extra runtime response negative selftest",
    ),
    ("fresh duplicate-flag regression was not caught", "fresh duplicate-flag negative selftest"),
    (
        "BatchUpsert fresh item duplicate-flag regression was not caught",
        "BatchUpsert fresh duplicate-flag negative selftest",
    ),
    (
        "BatchUpsert fresh item control-character projection-task-id receipt regression was not caught",
        "BatchUpsert projection-task-id control-character negative selftest",
    ),
    (
        "BatchUpsert fresh item control-character source-lsn receipt regression was not caught",
        "BatchUpsert source-lsn control-character negative selftest",
    ),
    (
        "tenant/project fresh duplicate-flag regression was not caught",
        "tenant/project fresh duplicate-flag negative selftest",
    ),
    (
        "tenant/project missing write_receipt_json regression was not caught",
        "tenant/project missing receipt negative selftest",
    ),
    (
        "missing write_receipt_json replay proof regression was not caught",
        "keyed Upsert missing receipt negative selftest",
    ),
    ("if keyless.idempotency_key:", "keyless fail-closed exact-empty idempotency_key validator"),
    ("keyless fail-closed freshness proof must not set idempotency_key", "keyless fail-closed validator"),
    (
        "keyless fail-closed freshness proof must share tenant_id, project_id, and message_type",
        "keyless fail-closed shared scope validator",
    ),
    (
        "keyless fail-closed freshness proof must reuse the keyed fail-closed record_json",
        "keyless fail-closed shared payload validator",
    ),
    ("dedup-store-down fail-closed proof must expect UNAVAILABLE", "fail-closed status validator"),
    ("keyless proof with unrelated scope input", "keyless shared-scope negative selftest"),
    ("keyless proof with different payload input", "keyless shared-payload negative selftest"),
    (
        "keyless fail-closed freshness duplicate-flag regression was not caught",
        "keyless fail-closed duplicate-flag negative selftest",
    ),
    ("tenant isolation same-tenant input", "tenant isolation same-tenant negative selftest"),
    ("tenant isolation same-project input", "tenant isolation same-project negative selftest"),
    ("tenant isolation stale-scope payload input", "tenant isolation stale-scope negative selftest"),
    ("tenant isolation unrelated-payload input", "tenant isolation unrelated-payload negative selftest"),
    ("keyed Upsert missing message_type input", "keyed Upsert message_type negative selftest"),
    ("keyless proof missing message_type input", "keyless message_type negative selftest"),
    ("keyed Upsert message_type surrounding whitespace input", "keyed Upsert message_type surrounding-whitespace negative selftest"),
    ("keyed Upsert message_type embedded whitespace input", "keyed Upsert message_type embedded-whitespace negative selftest"),
    ("keyed Upsert idempotency_key surrounding whitespace input", "keyed Upsert idempotency_key surrounding-whitespace negative selftest"),
    ("keyed Upsert tenant embedded whitespace input", "keyed Upsert tenant embedded-whitespace negative selftest"),
    ("keyless proof project surrounding whitespace input", "keyless project surrounding-whitespace negative selftest"),
    ("keyed Upsert empty payload input", "Upsert empty payload negative selftest"),
    ("keyed Upsert malformed JSON payload input", "Upsert malformed JSON payload negative selftest"),
    ("keyed Upsert array JSON payload input", "Upsert non-object JSON payload negative selftest"),
    ("keyed Upsert non-finite payload input", "idempotency non-finite record_json negative selftest"),
    ("keyed Upsert empty JSON object input", "Upsert empty JSON object negative selftest"),
    ("BatchUpsert extra item input", "BatchUpsert extra-item negative selftest"),
    ("BatchUpsert identical duplicate payload input", "BatchUpsert identical payload negative selftest"),
    ("BatchUpsert semantically identical duplicate payload input", "BatchUpsert semantically identical payload negative selftest"),
    ("BatchUpsert unrelated duplicate payload input", "BatchUpsert unrelated payload negative selftest"),
    ("BatchUpsert malformed JSON payload input", "BatchUpsert malformed JSON negative selftest"),
    ("BatchUpsert array JSON payload input", "BatchUpsert JSON-array negative selftest"),
    ("ambiguous record_json encoding input", "ambiguous record_json encoding negative selftest"),
    ("non-object record_json_object input", "non-object record_json_object negative selftest"),
    ("non-string record_json_text input", "non-string record_json_text negative selftest"),
    ("keyless proof with whitespace idempotency_key input", "whitespace keyless idempotency_key negative selftest"),
    ("missing Upsert proof file", "missing Upsert proof file negative selftest"),
    ("missing BatchUpsert proof file", "missing BatchUpsert proof file negative selftest"),
    ("oversized Upsert proof file", "oversized Upsert proof file negative selftest"),
    ("oversized BatchUpsert proof file", "oversized BatchUpsert proof file negative selftest"),
    ("duplicate-key Upsert proof JSON input", "duplicate Upsert proof JSON negative selftest"),
    ("non-finite Upsert proof JSON input", "non-finite Upsert proof JSON negative selftest"),
    ("duplicate-key BatchUpsert array proof JSON input", "duplicate BatchUpsert array proof JSON negative selftest"),
    ("duplicate-key BatchUpsert JSONL proof input", "duplicate BatchUpsert JSONL proof negative selftest"),
    ("duplicate live gRPC header regression was not caught", "duplicate metadata header negative selftest"),
    ("uppercase gRPC header name regression was not caught", "uppercase metadata header-name negative selftest"),
    ("spaced gRPC header name regression was not caught", "spaced metadata header-name negative selftest"),
    ("spaced gRPC header value regression was not caught", "spaced metadata header value negative selftest"),
    ("malformed gRPC header name regression was not caught", "malformed metadata header-name negative selftest"),
    ("reserved gRPC header name regression was not caught", "reserved metadata header-name negative selftest"),
    ("binary gRPC header name regression was not caught", "binary metadata header-name negative selftest"),
    ("control-character gRPC header value regression was not caught", "control-character metadata value negative selftest"),
    ("oversized gRPC header value regression was not caught", "oversized metadata value negative selftest"),
    ("excessive gRPC header count regression was not caught", "excessive metadata count negative selftest"),
    ("URL-shaped gRPC target regression was not caught", "URL-shaped target negative selftest"),
    ("whitespace gRPC target regression was not caught", "whitespace target negative selftest"),
    ("control-character gRPC target regression was not caught", "control-character target negative selftest"),
    ("missing-port gRPC target regression was not caught", "missing-port target negative selftest"),
    ("canonical timeout string was rejected", "canonical timeout string positive selftest"),
    ("idempotency runtime request-message validation regression was not caught", "idempotency runtime request-message negative selftest"),
    ("idempotency runtime metadata validation regression was not caught", "idempotency runtime metadata negative selftest"),
    ("idempotency runtime timeout validation regression was not caught", "idempotency runtime timeout negative selftest"),
    ("idempotency runtime Upsert stub validation regression was not caught", "idempotency runtime Upsert stub negative selftest"),
    (
        "idempotency runtime Upsert response-message validation regression was not caught",
        "idempotency runtime Upsert response-message negative selftest",
    ),
    (
        "idempotency runtime Upsert call-error validation regression was not caught",
        "idempotency runtime Upsert call-error negative selftest",
    ),
    (
        "idempotency runtime Upsert unexpected-RpcError validation regression was not caught",
        "idempotency runtime unexpected-RpcError negative selftest",
    ),
    (
        "BatchUpsert proof runtime response stream must be iterable",
        "BatchUpsert runtime response-stream assertion",
    ),
    (
        "BatchUpsert runtime response-stream validation regression was not caught",
        "BatchUpsert runtime response-stream negative selftest",
    ),
    (
        "BatchUpsert proof runtime response stream iterator could not be opened",
        "BatchUpsert runtime response-stream iterator assertion",
    ),
    (
        "BatchUpsert runtime response-stream iterator regression was not caught",
        "BatchUpsert runtime response-stream iterator negative selftest",
    ),
    (
        "BatchUpsert proof runtime response stream iterator raised unexpected gRPC error",
        "BatchUpsert runtime response-stream iterator unexpected-RpcError assertion",
    ),
    (
        "BatchUpsert runtime response-stream iterator unexpected-RpcError regression was not caught",
        "BatchUpsert runtime response-stream iterator unexpected-RpcError negative selftest",
    ),
    (
        "BatchUpsert proof runtime response stream raised unexpected gRPC error",
        "BatchUpsert runtime response-stream unexpected-RpcError assertion",
    ),
    (
        "BatchUpsert runtime response-stream unexpected-RpcError regression was not caught",
        "BatchUpsert runtime response-stream unexpected-RpcError negative selftest",
    ),
    (
        "BatchUpsert proof runtime response stream iteration raised error",
        "BatchUpsert runtime response-stream iteration assertion",
    ),
    (
        "BatchUpsert runtime response-stream iteration regression was not caught",
        "BatchUpsert runtime response-stream iteration negative selftest",
    ),
    (
        "BatchUpsert runtime response-message validation regression was not caught",
        "BatchUpsert runtime response-message negative selftest",
    ),
    ("BatchUpsert runtime call-error validation regression was not caught", "BatchUpsert runtime call-error negative selftest"),
    (
        "BatchUpsert runtime unexpected-RpcError validation regression was not caught",
        "BatchUpsert runtime unexpected-RpcError negative selftest",
    ),
    (
        '_assert_fresh_request_summary("BatchUpsert first item", responses[0], requests[0])',
        "BatchUpsert fresh item summary assertion",
    ),
    (
        "BatchUpsert fresh item summary regression was not caught",
        "BatchUpsert fresh item summary negative selftest",
    ),
    (
        "BatchUpsert fresh item affected_rows regression was not caught",
        "BatchUpsert fresh item affected_rows negative selftest",
    ),
    (
        "BatchUpsert fresh item receipt regression was not caught",
        "BatchUpsert fresh item receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item malformed receipt regression was not caught",
        "BatchUpsert fresh item malformed receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item duplicate-key receipt regression was not caught",
        "BatchUpsert fresh item duplicate-key receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item missing-fields receipt regression was not caught",
        "BatchUpsert fresh item missing-fields receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item unexpected-field receipt regression was not caught",
        "BatchUpsert fresh item unexpected-field receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item invalid projection-tasks receipt regression was not caught",
        "BatchUpsert fresh item invalid projection-tasks receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item invalid projection-task-id receipt regression was not caught",
        "BatchUpsert fresh item invalid projection-task-id receipt negative selftest",
    ),
    (
        "first response write_receipt_json projection_task_ids[{index}] must not include whitespace",
        "BatchUpsert fresh item projection-task-id embedded-whitespace assertion",
    ),
    (
        "BatchUpsert fresh item whitespace projection-task-id receipt regression was not caught",
        "BatchUpsert fresh item whitespace projection-task-id receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item invalid timestamp receipt regression was not caught",
        "BatchUpsert fresh item invalid timestamp receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item boolean timestamp receipt regression was not caught",
        "BatchUpsert fresh item boolean timestamp receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item invalid outbox-seq receipt regression was not caught",
        "BatchUpsert fresh item invalid outbox-seq receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item boolean outbox-seq receipt regression was not caught",
        "BatchUpsert fresh item boolean outbox-seq receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item invalid source-lsn receipt regression was not caught",
        "BatchUpsert fresh item invalid source-lsn receipt negative selftest",
    ),
    (
        "first response write_receipt_json source_lsn must be non-empty",
        "BatchUpsert fresh item source-lsn non-empty assertion",
    ),
    (
        "first response write_receipt_json source_lsn must not include whitespace",
        "BatchUpsert fresh item source-lsn embedded-whitespace assertion",
    ),
    (
        "BatchUpsert fresh item empty source-lsn receipt regression was not caught",
        "BatchUpsert fresh item empty source-lsn receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item padded source-lsn receipt regression was not caught",
        "BatchUpsert fresh item padded source-lsn receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item whitespace source-lsn receipt regression was not caught",
        "BatchUpsert fresh item whitespace source-lsn receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item empty manifest-checksum receipt regression was not caught",
        "BatchUpsert fresh item empty manifest-checksum receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item padded manifest-checksum receipt regression was not caught",
        "BatchUpsert fresh item padded manifest-checksum receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item bad-prefix manifest-checksum receipt regression was not caught",
        "BatchUpsert fresh item bad-prefix manifest-checksum receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item uppercase manifest-checksum receipt regression was not caught",
        "BatchUpsert fresh item uppercase manifest-checksum receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item typed receipt regression was not caught",
        "BatchUpsert fresh item typed receipt negative selftest",
    ),
    (
        "BatchUpsert fresh item mismatched typed receipt regression was not caught",
        "BatchUpsert fresh item mismatched typed receipt negative selftest",
    ),
    ("padded timeout regression was not caught", "padded timeout negative selftest"),
    ("non-decimal timeout regression was not caught", "non-decimal timeout negative selftest"),
    ("non-positive timeout regression was not caught", "non-positive timeout negative selftest"),
    ("infinite timeout regression was not caught", "infinite timeout negative selftest"),
    ("excessive timeout regression was not caught", "excessive timeout negative selftest"),
    ("fail-closed status weakened", "fail-closed status negative selftest"),
    ("empty fail-closed status regression was not caught", "empty fail-closed status negative selftest"),
    ("empty runtime fail-closed status regression was not caught", "empty runtime fail-closed status negative selftest"),
    ("fail-closed runtime keyed-input regression was not caught", "fail-closed runtime keyed-input negative selftest"),
    ("fail-closed runtime keyless-input regression was not caught", "fail-closed runtime keyless-input negative selftest"),
    ("missing fail-closed gRPC status-code reader regression was not caught", "missing fail-closed status-code reader negative selftest"),
    ("unreadable fail-closed gRPC status-code regression was not caught", "unreadable fail-closed status-code negative selftest"),
    ("non-StatusCode fail-closed gRPC status-code regression was not caught", "non-StatusCode fail-closed status-code negative selftest"),
    ("empty fail-closed error message regression was not caught", "empty fail-closed error-message negative selftest"),
    ("missing fail-closed error message reader regression was not caught", "missing fail-closed error-message reader negative selftest"),
    ("unreadable fail-closed error message regression was not caught", "unreadable fail-closed error-message negative selftest"),
    ("padded fail-closed error message regression was not caught", "padded fail-closed error-message negative selftest"),
    ("control-character fail-closed error message regression was not caught", "control-character fail-closed error-message negative selftest"),
    ("non-string fail-closed error message regression was not caught", "non-string fail-closed error-message negative selftest"),
    ("oversized fail-closed error message regression was not caught", "oversized fail-closed error-message negative selftest"),
    ("generic fail-closed error message regression was not caught", "generic fail-closed error-message negative selftest"),
    ("live proof input validation selftest", "input-validation negative selftest marker"),
    ("DataBrokerStub", "real DataBroker stub import"),
    ("UpsertRequest", "real UpsertRequest import"),
    ("MutationResponse", "real MutationResponse import"),
    ("record_json_object", "operator-friendly record_json object helper"),
    ("--upsert-json", "Upsert replay CLI input"),
    ("--tenant2-upsert-json", "tenant isolation CLI input"),
    ("--batch-upsert-json", "BatchUpsert replay CLI input"),
    ("--fail-closed-upsert-json", "fail-closed CLI input"),
    ("--keyless-upsert-json", "keyless CLI input"),
    ("--fail-closed-code", "fail-closed status CLI input"),
    ("--require-all-proofs", "complete proof CLI gate"),
    ("missing required idempotency live proof inputs", "complete proof missing-input error"),
    ("idempotency served replay smoke selftest passed", "selftest success marker"),
    ("first keyed Upsert", "fresh first write assertion"),
    ("replay did not return was_duplicate=true", "duplicate replay assertion"),
)

RETRY_SAFE_SERVED_SMOKE_REQUIREMENTS = (
    ("UPSERT_METHOD = \"/udb.services.v1.DataBroker/Upsert\"", "Upsert method constant"),
    ("DELETE_METHOD = \"/udb.services.v1.DataBroker/Delete\"", "Delete method constant"),
    ("DOCUMENT_UPSERT_METHOD = \"/udb.services.v1.DataBroker/DocumentUpsert\"", "non-replay-safe mutation sample"),
    ("RPC_REPLAY_SAFE", "generated replay-safe metadata import"),
    ("RetryPolicy", "generated retry policy import"),
    ("DeleteRequest", "Delete request import"),
    ("_is_replay_safe", "generated replay-safe lookup import"),
    ("_request_has_idempotency_key", "generated idempotency-key detector import"),
    ("def assert_retry_metadata_gate(", "retry metadata gate checker"),
    ("def validate_replay_request(", "live replay request validator"),
    ("def validate_message_type_token(", "retry-safe message_type token validator"),
    ("def validate_upsert_payload(", "Upsert JSON-object payload validator"),
    ("def validate_delete_filter(", "Delete filter shape validator"),
    ("object_pairs_hook=_reject_duplicate_json_keys", "retry-safe duplicate proof JSON key parser"),
    ("parse_constant=_reject_non_finite_json_constant", "retry-safe non-finite proof JSON parser"),
    ("proof JSON must not contain non-standard constant", "retry-safe non-finite proof JSON assertion"),
    ("def validate_shared_replay_scope(", "Upsert/Delete shared replay scope validator"),
    ("def validate_delete_filter_matches_upsert_payload(", "Upsert/Delete filter-payload coherence validator"),
    ("def _parse_headers(", "retry-safe live metadata header parser"),
    ("GRPC_METADATA_NAME_CHARS", "retry-safe metadata header-name character allowlist"),
    ("MAX_LIVE_METADATA_COUNT = 32", "retry-safe metadata header count ceiling constant"),
    ("MAX_LIVE_METADATA_VALUE_BYTES = 8_192", "retry-safe metadata header value ceiling constant"),
    ("gRPC metadata header name must not include surrounding whitespace", "retry-safe metadata header-name whitespace validator"),
    ("gRPC metadata header name must contain only lowercase letters", "retry-safe metadata header-name validator"),
    ("gRPC metadata header name must not start with grpc-", "retry-safe reserved metadata header validator"),
    ("gRPC binary metadata headers are not supported by --header", "retry-safe binary metadata header validator"),
    ("gRPC metadata header value must not include surrounding whitespace", "retry-safe metadata header value whitespace validator"),
    ("gRPC metadata header value must not contain control characters", "retry-safe metadata header value control-character validator"),
    ("gRPC metadata header value must be <=", "retry-safe metadata header value ceiling validator"),
    ("gRPC metadata headers must be <=", "retry-safe metadata header count ceiling validator"),
    ("duplicate gRPC metadata header", "retry-safe duplicate metadata header validator"),
    ("def _contains_control_character(", "retry-safe control-character helper"),
    ("def validate_grpc_target(", "retry-safe live gRPC target validator"),
    ("gRPC target must be a host:port authority, not a URL or path", "retry-safe URL-shaped target validator"),
    ("gRPC target must not include control characters", "retry-safe target control-character validator"),
    ("gRPC target port must be an integer from 1 to 65535", "retry-safe target port validator"),
    ("MAX_LIVE_TIMEOUT_SECONDS = 120.0", "retry-safe timeout ceiling constant"),
    ("TIMEOUT_DECIMAL_PATTERN", "retry-safe timeout decimal pattern"),
    ("def normalize_timeout_seconds(", "retry-safe timeout normalizer"),
    ("def validate_timeout_seconds(", "retry-safe live timeout validator"),
    ("def validate_runtime_metadata(", "retry-safe runtime metadata validator"),
    ("def validate_runtime_timeout_seconds(", "retry-safe runtime timeout validator"),
    ("def validate_runtime_transport_inputs(", "retry-safe runtime transport validator"),
    ("def validate_runtime_upsert_request(", "retry-safe runtime Upsert request validator"),
    ("def validate_runtime_delete_request(", "retry-safe runtime Delete request validator"),
    ("def validate_runtime_stub_method(", "retry-safe runtime stub-method validator"),
    ("runtime stub must expose callable", "retry-safe runtime stub-method failure"),
    ("def validate_runtime_mutation_response(", "retry-safe runtime response-message validator"),
    ("runtime response must be a MutationResponse", "retry-safe runtime response-message failure"),
    ("def call_runtime_mutation(", "retry-safe runtime call wrapper"),
    ("runtime call raised unexpected gRPC error", "retry-safe unexpected runtime RpcError failure"),
    ("runtime call raised error", "retry-safe runtime call-error failure"),
    ("timeout must not include surrounding whitespace", "retry-safe timeout whitespace validator"),
    ("timeout must be a positive decimal number of seconds", "retry-safe timeout decimal validator"),
    ("timeout must be a finite number of seconds", "retry-safe finite timeout validator"),
    ("timeout must be greater than 0 seconds", "retry-safe positive timeout validator"),
    ("timeout must be <= 120 seconds", "retry-safe timeout ceiling validator"),
    ("MAX_PROOF_INPUT_BYTES = 1_048_576", "retry-safe proof input byte ceiling constant"),
    ("def _read_proof_text(", "retry-safe proof file reader"),
    ("proof file must exist and be a regular file", "retry-safe missing proof file validator"),
    ("proof file must be <=", "retry-safe oversized proof file validator"),
    ("def _assert_restored_summary(", "Upsert/Delete replay summary assertion helper"),
    ("def check_served_replay(", "served Upsert replay checker"),
    ("def check_served_delete_replay(", "served Delete replay checker"),
    ("def validate_replay_idempotency_key(", "retry-safe idempotency_key token validator"),
    ("idempotency_key must not include surrounding whitespace", "retry-safe idempotency_key surrounding-whitespace validator"),
    ("idempotency_key must not include whitespace", "retry-safe idempotency_key embedded-whitespace validator"),
    ("def validate_replay_scope_token(", "retry-safe replay scope token validator"),
    ("context.tenant_id must not include surrounding whitespace", "retry-safe tenant surrounding-whitespace validator"),
    ("context.project_id must not include whitespace", "retry-safe project embedded-whitespace validator"),
    ('validate_message_type_token(f"{label} proof message_type"', "retry-safe message_type proof validator"),
    ("message_type must not include surrounding whitespace", "retry-safe message_type surrounding-whitespace validator"),
    ("message_type must not include whitespace", "retry-safe message_type embedded-whitespace validator"),
    ("Upsert proof requires non-empty record_json", "retry-safe Upsert record_json validator"),
    ("Upsert proof record_json must be a valid JSON object", "retry-safe Upsert valid JSON validator"),
    ("Upsert proof record_json must not contain duplicate JSON keys", "retry-safe Upsert duplicate-key record_json validator"),
    (
        "Upsert proof record_json must not contain non-standard JSON constants",
        "retry-safe Upsert non-finite record_json validator",
    ),
    ("Upsert proof record_json must be a JSON object", "retry-safe Upsert JSON object validator"),
    ("Upsert proof record_json must be a non-empty JSON object", "retry-safe Upsert non-empty JSON object validator"),
    ("must use only one of record_json, record_json_object, or record_json_text", "retry-safe Upsert record_json encoding ambiguity validator"),
    ("record_json_object must be a JSON object", "retry-safe Upsert record_json_object object helper validator"),
    ("record_json_text must be a string", "retry-safe Upsert record_json_text string helper validator"),
    ("ambiguous Upsert record_json encoding regression was not caught", "retry-safe ambiguous record_json negative selftest"),
    ("duplicate-key Upsert record_json regression was not caught", "retry-safe duplicate-key record_json negative selftest"),
    ("non-finite Upsert record_json regression was not caught", "retry-safe non-finite record_json negative selftest"),
    ("non-object record_json_object regression was not caught", "retry-safe non-object record_json_object negative selftest"),
    ("non-string record_json_text regression was not caught", "retry-safe non-string record_json_text negative selftest"),
    ("proof JSON must not contain duplicate key", "retry-safe duplicate proof JSON key validator"),
    ("non-finite Upsert proof JSON regression was not caught", "retry-safe non-finite Upsert JSON negative selftest"),
    ("duplicate-key Upsert proof JSON regression was not caught", "retry-safe duplicate Upsert JSON negative selftest"),
    ("duplicate-key Delete proof JSON regression was not caught", "retry-safe duplicate Delete JSON negative selftest"),
    ("Delete proof requires a non-empty filter", "Delete filter validator"),
    ("Delete proof filter field names must be non-empty", "Delete filter field-name validator"),
    ("Delete proof filter field names must not include surrounding whitespace", "Delete filter field-name surrounding-whitespace validator"),
    ("Delete proof filter field names must not include whitespace", "Delete filter field-name embedded-whitespace validator"),
    ("Delete proof filter values must not be null", "Delete filter null-value validator"),
    ("Upsert/Delete replay proofs must share", "Upsert/Delete shared scope validator"),
    (
        "must share at least one Delete filter field/value with Upsert record_json",
        "Upsert/Delete filter-payload coherence assertion",
    ),
    ("idempotency_key", "Upsert/Delete shared idempotency-key validator"),
    ("DataBroker.Delete must be generated as replay-safe", "Delete replay-safe metadata assertion"),
    ("replay-safe keyed mutation should retry UNAVAILABLE", "keyed retry assertion"),
    ("replay-safe mutation without idempotency key must not retry", "missing-key no-retry assertion"),
    ("non-replay-safe mutation must not retry even with idempotency key", "non-replay-safe no-retry assertion"),
    ("mutation DEADLINE_EXCEEDED must not be auto-retried", "mutation deadline no-retry assertion"),
    ("second replay-safe Upsert did not return was_duplicate=true", "served duplicate assertion"),
    ("second replay-safe Delete did not return was_duplicate=true", "served Delete duplicate assertion"),
    ("first replay-safe Upsert affected_rows must be positive", "served Upsert fresh affected_rows assertion"),
    ("first replay-safe Delete affected_rows must be positive", "served Delete fresh affected_rows assertion"),
    ("duplicate replay mutation_id differs from first response", "served Upsert mutation_id assertion"),
    ("duplicate Delete replay mutation_id differs from first response", "served Delete mutation_id assertion"),
    ("MUTATION_ID_PATTERN = re.compile", "served mutation_id canonical UUID pattern"),
    ("mutation_id must be non-empty", "served mutation_id presence assertion"),
    ("mutation_id must be a canonical lowercase UUID", "served mutation_id UUID-shape assertion"),
    ("duplicate replay affected_rows differs from first response", "served Upsert affected_rows assertion"),
    ("duplicate Delete replay affected_rows differs from first response", "served Delete affected_rows assertion"),
    ("first response must include at least one replay summary field", "served replay summary presence assertion"),
    (
        "first response record_json must include request field/value",
        "served Upsert request/response payload binding assertion",
    ),
    ("first response {field} must not be whitespace-only", "served replay summary whitespace-only assertion"),
    ("first response {field} must not include surrounding whitespace", "served replay summary surrounding-whitespace assertion"),
    ("first response resource_uri must start with udb://", "served replay summary resource_uri scheme assertion"),
    (
        "first response resource_uri must include non-empty authority and path",
        "served replay summary resource_uri shape assertion",
    ),
    (
        "first response resource_uri authority must equal request tenant_id",
        "served replay summary tenant-authority assertion",
    ),
    (
        "first response resource_uri path must start with request message_type",
        "served replay summary message-path assertion",
    ),
    (
        "first response resource_uri path must include request message_type and resource id",
        "served replay summary resource-id assertion",
    ),
    (
        "first response resource_uri id must match an identity request field value",
        "served replay summary request-value id assertion",
    ),
    (
        "resource_uri id proof requires at least one scalar identity request field",
        "served replay summary identity-field requirement",
    ),
    (
        "resource_uri id proof identity field value must not include surrounding whitespace",
        "served replay summary identity surrounding-whitespace assertion",
    ),
    (
        "resource_uri id proof identity field value must not include whitespace",
        "served replay summary identity embedded-whitespace assertion",
    ),
    ("first response {field} must be a valid JSON object", "served replay summary JSON-object assertion"),
    ("first response {field} must be a non-empty JSON object", "served replay summary non-empty JSON-object assertion"),
    ("first response {field} must not contain duplicate JSON key", "served replay summary duplicate-key assertion"),
    (
        "first response {field} must not contain non-standard JSON constants",
        "served replay summary non-finite assertion",
    ),
    (
        "first response checksum_sha256 must be sha256:<64 lowercase hex>",
        "served replay summary checksum shape assertion",
    ),
    ("def _assert_typed_write_receipt_lockstep(", "served typed write receipt lockstep helper"),
    ("MANIFEST_CHECKSUM_PATTERN = re.compile", "served write receipt checksum shape pattern"),
    ("first response write_receipt_json missing fields", "served write receipt required-fields assertion"),
    ("first response write_receipt_json unexpected fields", "served write receipt unexpected-fields assertion"),
    ("first response write_receipt_json source_lsn must be non-empty", "served write receipt source_lsn non-empty assertion"),
    (
        "first response write_receipt_json source_lsn must not include whitespace",
        "served write receipt source_lsn embedded-whitespace assertion",
    ),
    (
        "first response write_receipt_json source_lsn must not contain control characters",
        "served write receipt source_lsn control-character assertion",
    ),
    ("write_receipt_json projection_task_ids must be an array", "served write receipt projection task array assertion"),
    (
        "first response write_receipt_json projection_task_ids[{index}] must not include whitespace",
        "served write receipt projection task embedded-whitespace assertion",
    ),
    (
        "first response write_receipt_json projection_task_ids[{index}] must not contain control characters",
        "served write receipt projection task control-character assertion",
    ),
    (
        "first response write_receipt_json manifest_checksum must be sha256:<64 lowercase hex>",
        "served write receipt manifest checksum shape assertion",
    ),
    ("write_receipt_json written_at_unix_ms must be a positive integer", "served write receipt timestamp assertion"),
    (
        "typed write_receipt must be present when write_receipt_json is present",
        "served typed write receipt presence assertion",
    ),
    (
        "typed write_receipt must match write_receipt_json",
        "served typed write receipt lockstep assertion",
    ),
    ("duplicate replay write_receipt_json differs from first response", "served replay summary restoration assertion"),
    ("Upsert affected_rows replay regression was not caught", "served Upsert affected_rows negative selftest"),
    ("Delete affected_rows replay regression was not caught", "served Delete affected_rows negative selftest"),
    ("Upsert mutation_id replay regression was not caught", "served Upsert mutation_id negative selftest"),
    ("Delete mutation_id replay regression was not caught", "served Delete mutation_id negative selftest"),
    ("Upsert invalid mutation_id shape regression was not caught", "served Upsert mutation_id shape negative selftest"),
    ("Delete invalid mutation_id shape regression was not caught", "served Delete mutation_id shape negative selftest"),
    ("Upsert fresh affected_rows regression was not caught", "served Upsert fresh affected_rows negative selftest"),
    ("Delete fresh affected_rows regression was not caught", "served Delete fresh affected_rows negative selftest"),
    ("Upsert empty replay summary regression was not caught", "served Upsert empty summary negative selftest"),
    ("Delete empty replay summary regression was not caught", "served Delete empty summary negative selftest"),
    ("Upsert invalid resource_uri replay summary regression was not caught", "served Upsert invalid resource_uri negative selftest"),
    ("Delete pathless resource_uri replay summary regression was not caught", "served Delete pathless resource_uri negative selftest"),
    (
        "Upsert wrong-tenant resource_uri replay summary regression was not caught",
        "served Upsert wrong-tenant resource_uri negative selftest",
    ),
    (
        "Delete wrong-tenant resource_uri replay summary regression was not caught",
        "served Delete wrong-tenant resource_uri negative selftest",
    ),
    (
        "Upsert wrong-message resource_uri replay summary regression was not caught",
        "served Upsert wrong-message resource_uri negative selftest",
    ),
    (
        "Delete wrong-message resource_uri replay summary regression was not caught",
        "served Delete wrong-message resource_uri negative selftest",
    ),
    (
        "Upsert short-path resource_uri replay summary regression was not caught",
        "served Upsert short-path resource_uri negative selftest",
    ),
    (
        "Delete short-path resource_uri replay summary regression was not caught",
        "served Delete short-path resource_uri negative selftest",
    ),
    (
        "Upsert wrong-id resource_uri replay summary regression was not caught",
        "served Upsert wrong-id resource_uri negative selftest",
    ),
    (
        "Delete wrong-id resource_uri replay summary regression was not caught",
        "served Delete wrong-id resource_uri negative selftest",
    ),
    (
        "Upsert non-identity scalar resource_uri replay summary regression was not caught",
        "served Upsert non-identity scalar resource_uri negative selftest",
    ),
    (
        "Delete non-identity scalar resource_uri replay summary regression was not caught",
        "served Delete non-identity scalar resource_uri negative selftest",
    ),
    (
        "Upsert missing identity resource_uri replay summary regression was not caught",
        "served Upsert missing identity resource_uri negative selftest",
    ),
    (
        "Delete missing identity resource_uri replay summary regression was not caught",
        "served Delete missing identity resource_uri negative selftest",
    ),
    (
        "Upsert padded identity resource_uri replay summary regression was not caught",
        "served Upsert padded identity resource_uri negative selftest",
    ),
    (
        "Upsert embedded-space identity resource_uri replay summary regression was not caught",
        "served Upsert embedded-space identity resource_uri negative selftest",
    ),
    (
        "Delete padded identity resource_uri replay summary regression was not caught",
        "served Delete padded identity resource_uri negative selftest",
    ),
    (
        "Delete embedded-space identity resource_uri replay summary regression was not caught",
        "served Delete embedded-space identity resource_uri negative selftest",
    ),
    ("Upsert whitespace replay summary regression was not caught", "served Upsert whitespace summary negative selftest"),
    ("Delete padded replay summary regression was not caught", "served Delete padded summary negative selftest"),
    (
        "Upsert malformed record_json replay summary regression was not caught",
        "served Upsert malformed record_json summary negative selftest",
    ),
    (
        "Delete malformed write_receipt_json replay summary regression was not caught",
        "served Delete malformed write_receipt_json summary negative selftest",
    ),
    (
        "Delete missing-fields write_receipt_json replay summary regression was not caught",
        "served Delete missing-fields write_receipt_json summary negative selftest",
    ),
    (
        "Delete invalid timestamp write_receipt_json replay summary regression was not caught",
        "served Delete invalid timestamp write_receipt_json summary negative selftest",
    ),
    (
        "Upsert unexpected-field write_receipt_json replay summary regression was not caught",
        "served Upsert unexpected-field write_receipt_json summary negative selftest",
    ),
    (
        "Upsert empty source_lsn write_receipt_json replay summary regression was not caught",
        "served Upsert empty source_lsn write_receipt_json summary negative selftest",
    ),
    (
        "Upsert control-character source_lsn write_receipt_json replay summary regression was not caught",
        "served Upsert control-character source_lsn write_receipt_json summary negative selftest",
    ),
    (
        "Upsert whitespace projection_task_ids write_receipt_json replay summary regression was not caught",
        "served Upsert whitespace projection_task_ids write_receipt_json summary negative selftest",
    ),
    (
        "Upsert control-character projection_task_ids write_receipt_json replay summary regression was not caught",
        "served Upsert control-character projection_task_ids write_receipt_json summary negative selftest",
    ),
    (
        "Upsert invalid manifest_checksum write_receipt_json replay summary regression was not caught",
        "served Upsert invalid manifest_checksum write_receipt_json summary negative selftest",
    ),
    (
        "Upsert duplicate-key record_json replay summary regression was not caught",
        "served Upsert duplicate-key record_json summary negative selftest",
    ),
    (
        "Upsert non-finite record_json replay summary regression was not caught",
        "served Upsert non-finite record_json summary negative selftest",
    ),
    (
        "Upsert mismatched record_json replay summary regression was not caught",
        "served Upsert mismatched record_json summary negative selftest",
    ),
    (
        "Upsert invalid checksum_sha256 replay summary regression was not caught",
        "served Upsert invalid checksum_sha256 summary negative selftest",
    ),
    (
        "Upsert checksum replay regression was not caught",
        "served Upsert checksum replay negative selftest",
    ),
    (
        "Upsert missing typed write_receipt replay summary regression was not caught",
        "served Upsert missing typed write receipt negative selftest",
    ),
    (
        "Upsert mismatched typed write_receipt replay summary regression was not caught",
        "served Upsert mismatched typed write receipt negative selftest",
    ),
    (
        "Delete duplicate-key write_receipt_json replay summary regression was not caught",
        "served Delete duplicate-key write_receipt_json summary negative selftest",
    ),
    (
        "Delete missing typed write_receipt replay summary regression was not caught",
        "served Delete missing typed write receipt negative selftest",
    ),
    (
        "Delete mismatched typed write_receipt replay summary regression was not caught",
        "served Delete mismatched typed write receipt negative selftest",
    ),
    ("Upsert dropped replay summary regression was not caught", "served Upsert dropped summary negative selftest"),
    ("Delete dropped replay summary regression was not caught", "served Delete dropped summary negative selftest"),
    ("retry-safe runtime Upsert request-message validation regression was not caught", "retry-safe runtime Upsert request-message negative selftest"),
    ("retry-safe runtime Delete request-message validation regression was not caught", "retry-safe runtime Delete request-message negative selftest"),
    ("retry-safe runtime metadata validation regression was not caught", "retry-safe runtime metadata negative selftest"),
    ("retry-safe runtime timeout validation regression was not caught", "retry-safe runtime timeout negative selftest"),
    ("retry-safe runtime Upsert stub validation regression was not caught", "retry-safe runtime Upsert stub negative selftest"),
    ("retry-safe runtime Delete stub validation regression was not caught", "retry-safe runtime Delete stub negative selftest"),
    (
        "retry-safe runtime Upsert response-message validation regression was not caught",
        "retry-safe runtime Upsert response-message negative selftest",
    ),
    (
        "retry-safe runtime Delete response-message validation regression was not caught",
        "retry-safe runtime Delete response-message negative selftest",
    ),
    (
        "retry-safe runtime Upsert call-error validation regression was not caught",
        "retry-safe runtime Upsert call-error negative selftest",
    ),
    (
        "retry-safe runtime Upsert unexpected-RpcError validation regression was not caught",
        "retry-safe runtime Upsert unexpected-RpcError negative selftest",
    ),
    (
        "retry-safe runtime Delete call-error validation regression was not caught",
        "retry-safe runtime Delete call-error negative selftest",
    ),
    (
        "retry-safe runtime Delete unexpected-RpcError validation regression was not caught",
        "retry-safe runtime Delete unexpected-RpcError negative selftest",
    ),
    ("missing Upsert proof file regression was not caught", "retry-safe missing Upsert proof file negative selftest"),
    ("missing Delete proof file regression was not caught", "retry-safe missing Delete proof file negative selftest"),
    ("oversized Upsert proof file regression was not caught", "retry-safe oversized Upsert proof file negative selftest"),
    ("oversized Delete proof file regression was not caught", "retry-safe oversized Delete proof file negative selftest"),
    ("spaced Upsert idempotency_key regression was not caught", "retry-safe Upsert idempotency_key surrounding-whitespace negative selftest"),
    ("embedded-space Delete idempotency_key regression was not caught", "retry-safe Delete idempotency_key embedded-whitespace negative selftest"),
    ("missing Upsert message_type regression was not caught", "retry-safe Upsert message_type negative selftest"),
    ("missing Delete message_type regression was not caught", "retry-safe Delete message_type negative selftest"),
    ("spaced Upsert tenant_id regression was not caught", "retry-safe Upsert tenant surrounding-whitespace negative selftest"),
    ("embedded-space Delete project_id regression was not caught", "retry-safe Delete project embedded-whitespace negative selftest"),
    ("spaced Upsert message_type regression was not caught", "retry-safe Upsert message_type surrounding-whitespace negative selftest"),
    ("embedded-space Delete message_type regression was not caught", "retry-safe Delete message_type embedded-whitespace negative selftest"),
    ("missing Upsert record_json regression was not caught", "retry-safe Upsert empty payload negative selftest"),
    ("malformed Upsert record_json regression was not caught", "retry-safe Upsert malformed JSON negative selftest"),
    ("array Upsert record_json regression was not caught", "retry-safe Upsert JSON-array negative selftest"),
    ("empty-object Upsert record_json regression was not caught", "retry-safe Upsert empty JSON object negative selftest"),
    ("missing Delete filter regression was not caught", "Delete filter negative selftest"),
    ("empty Delete filter field regression was not caught", "Delete empty filter field negative selftest"),
    ("spaced Delete filter field regression was not caught", "Delete spaced filter field negative selftest"),
    ("embedded-space Delete filter field regression was not caught", "Delete embedded-space filter field negative selftest"),
    ("null Delete filter value regression was not caught", "Delete null filter value negative selftest"),
    ("mismatched Upsert/Delete replay scope regression was not caught", "shared replay scope negative selftest"),
    ("mismatched Upsert/Delete idempotency key regression was not caught", "shared replay key negative selftest"),
    ("mismatched Delete filter payload regression was not caught", "Delete filter-payload negative selftest"),
    ("duplicate live gRPC header regression was not caught", "duplicate metadata header negative selftest"),
    ("uppercase gRPC header name regression was not caught", "uppercase metadata header-name negative selftest"),
    ("spaced gRPC header name regression was not caught", "spaced metadata header-name negative selftest"),
    ("spaced gRPC header value regression was not caught", "spaced metadata header value negative selftest"),
    ("malformed gRPC header name regression was not caught", "malformed metadata header-name negative selftest"),
    ("reserved gRPC header name regression was not caught", "reserved metadata header-name negative selftest"),
    ("binary gRPC header name regression was not caught", "binary metadata header-name negative selftest"),
    ("control-character gRPC header value regression was not caught", "control-character metadata value negative selftest"),
    ("oversized gRPC header value regression was not caught", "oversized metadata value negative selftest"),
    ("excessive gRPC header count regression was not caught", "excessive metadata count negative selftest"),
    ("URL-shaped gRPC target regression was not caught", "URL-shaped target negative selftest"),
    ("whitespace gRPC target regression was not caught", "whitespace target negative selftest"),
    ("control-character gRPC target regression was not caught", "control-character target negative selftest"),
    ("missing-port gRPC target regression was not caught", "missing-port target negative selftest"),
    ("canonical timeout string was rejected", "canonical timeout string positive selftest"),
    ("padded timeout regression was not caught", "padded timeout negative selftest"),
    ("non-decimal timeout regression was not caught", "non-decimal timeout negative selftest"),
    ("non-positive timeout regression was not caught", "non-positive timeout negative selftest"),
    ("infinite timeout regression was not caught", "infinite timeout negative selftest"),
    ("excessive timeout regression was not caught", "excessive timeout negative selftest"),
    ("--upsert-json", "Upsert replay CLI input"),
    ("--delete-json", "Delete replay CLI input"),
    ("served keyed Upsert/Delete replay", "live Upsert/Delete success marker"),
    ("retry-safe served smoke selftest passed", "selftest success marker"),
    ("retry-safe served smoke passed", "live success marker"),
    ("missing idempotency key regression was not caught", "negative selftest"),
)

LINT_WORKFLOW_TRIGGER_PATHS = (
    ("scripts/extract-changelog-section.mjs", "release-notes extractor"),
    (".github/workflows/**", "workflow files"),
    (".github/actions/**", "composite action files"),
    ("docs/ci-architecture.md", "CI architecture contract"),
    ("docs/site/**", "Pages site source"),
    ("docs/assets/**", "Pages static assets"),
    ("api/**", "published OpenAPI specs"),
    ("udb-skill/**", "published skill sources"),
    ("docker-compose.integration.yml", "integration compose profiles"),
    ("docker-compose.canonical.yml", "canonical compose stack"),
    ("docker-compose.xa-ha.yml", "XA HA compose overlay"),
    ("docker/postgres-pg-partman/Dockerfile", "Postgres pg_partman image"),
    ("docker/mysql-init/01-grant-replication-client.sql", "MySQL live-test grants"),
    ("docker/clickhouse/config.d/keeper.xml", "ClickHouse Keeper config"),
    ("Dockerfile.release", "release Dockerfile"),
    ("third_party/ffmpeg/**", "vendored ffmpeg package"),
    ("versions.json", "version manifest"),
    ("VERSIONING.md", "versioning policy doc"),
    ("docs/api-rules.md", "API rules doc"),
    ("docs/api-sdk-beta-migration.md", "API/SDK beta migration fixture"),
    ("scripts/check-workflow-posture.py", "workflow posture guard"),
    ("scripts/ci-inventory.mjs", "CI inventory guard"),
    ("scripts/check-branch-protection-lockstep.mjs", "branch-protection lockstep audit"),
    ("scripts/check-ci-runner-evidence.mjs", "CI runner evidence audit"),
    ("scripts/check-versions.mjs", "version guard"),
    ("scripts/check-launcher-assets.mjs", "launcher asset guard"),
    ("scripts/ci_slim_dep_guard.sh", "CI slim dependency guard"),
    ("scripts/generate-codebase-map.py", "codebase map generator"),
    ("scripts/check-vendored-ffmpeg.py", "vendored ffmpeg guard"),
    ("scripts/gen-release-manifest.mjs", "release manifest generator"),
    ("scripts/gen-bench-bodies-skeleton.mjs", "benchmark body skeleton generator"),
    ("scripts/gen-bench-bodies-json.mjs", "benchmark body generator"),
    ("scripts/gen-sdk-benchmark-docs.mjs", "SDK benchmark docs generator"),
    ("sdk/SDK_LIVE_TEST_COVERAGE.md", "SDK live coverage generated doc"),
    ("sdk/SDK_PERF_LISTING.md", "SDK performance generated doc"),
    ("scripts/ffmpeg_transcode_smoke.py", "ffmpeg transcode smoke"),
    ("scripts/playground_wasm_smoke.mjs", "playground WASM smoke"),
    ("scripts/collect_sdk_bench_results.py", "benchmark collector"),
    ("scripts/check-openapi-api-rules.mjs", "OpenAPI API-rule guard"),
    ("scripts/check-http-api-style.mjs", "HTTP API route-style guard"),
    ("scripts/rest_route_gateway_smoke.py", "REST route gateway smoke guard"),
    ("scripts/http-api-style.allow.json", "HTTP API route-style allowlist"),
    ("scripts/openapi-postprocess.mjs", "OpenAPI postprocess helper"),
    ("scripts/sdk-codegen-postprocess.mjs", "SDK codegen postprocess helper"),
    ("scripts/generate-authn-authz-inventory.mjs", "authn/authz inventory generator"),
    ("scripts/native_load_gate.py", "native load gate"),
    ("scripts/native-load-test.sh", "native load scenario harness"),
    ("scripts/native_load_smoke_baseline.json", "native load smoke baseline"),
    ("scripts/check-sdk-service-coverage.py", "SDK service-coverage guard"),
    ("scripts/check-vector-cas-posture.py", "vector CAS posture guard"),
    ("scripts/check-orm-template-posture.py", "ORM template posture guard"),
    ("scripts/check-workflow-service-posture.py", "WorkflowService posture guard"),
    ("scripts/check-ir-live-golden-posture.py", "IR live-golden posture guard"),
    ("scripts/check-scaffold-posture.py", "scaffold posture guard"),
    ("scripts/check-sdk-helper-parity.py", "SDK helper parity guard"),
    ("scripts/check-todo-board-status.py", "todo-board status guard"),
    ("scripts/check-gap-closure-posture.py", "gap-closure posture guard"),
    ("scripts/check-bench-harness-posture.py", "bench harness posture guard"),
    ("scripts/check-docs-ci-freshness-posture.py", "docs/CI freshness posture guard"),
    ("scripts/check-go-sdk-posture.py", "Go SDK posture guard"),
    ("scripts/check-ts-sdk-posture.py", "TypeScript SDK posture guard"),
    ("scripts/check-python-php-sdk-posture.py", "Python/PHP SDK posture guard"),
    ("scripts/check-java-csharp-sdk-audit.py", "Java/C# SDK audit guard"),
    ("scripts/check-api-sdk-alias-posture.py", "API/SDK alias posture guard"),
    ("scripts/check-openapi-operationid-posture.py", "OpenAPI operation-id posture guard"),
    ("scripts/check-idempotency-dedup-posture.py", "idempotency dedup posture guard"),
    ("scripts/error_detail_served_smoke.py", "ErrorDetail served smoke"),
    ("scripts/write_error_detail_served_smoke_inputs.py", "ErrorDetail proof input generator"),
    ("scripts/idempotency_served_replay_smoke.py", "idempotency served replay smoke"),
    ("scripts/write_databroker_served_smoke_inputs.py", "DataBroker served-smoke proof input generator"),
    ("scripts/retry_safe_served_smoke.py", "retry-safe served smoke"),
    ("scripts/check-retry-safe-posture.py", "retry-safe mutation posture guard"),
    ("scripts/check-error-detail-posture.py", "error-detail posture guard"),
    ("scripts/check-beta-versioning-posture.py", "beta versioning posture guard"),
    ("scripts/check-scaffold-compiles.sh", "scaffold compile guard"),
    ("scripts/check-doc-service-counts.py", "doc service-count guard"),
    ("scripts/check-no-internal-tables.py", "public-doc internal table guard"),
    ("scripts/check-markdown-links.mjs", "markdown link guard"),
    ("scripts/check-enterprise-readiness.mjs", "enterprise readiness guard"),
    ("scripts/ha_multinode_smoke.sh", "HA lease smoke script"),
    ("scripts/ha_cdc_no_duplicate_smoke.sh", "HA CDC no-duplicate smoke script"),
    ("scripts/ha_xa_recovery_smoke.sh", "HA XA recovery smoke script"),
    ("scripts/cdc_fault_smoke.sh", "CDC fault smoke script"),
    ("scripts/embedding_sidecar_roundtrip_smoke.py", "embedding sidecar round-trip smoke"),
    ("scripts/embedding_sidecar_smoke.py", "embedding sidecar smoke"),
    ("scripts/embedding_retrieval_eval.py", "embedding retrieval golden-set evaluation"),
    ("scripts/notify_sidecar_roundtrip_smoke.py", "notification sidecar round-trip smoke"),
    ("scripts/notify_sidecar_smoke.py", "notification sidecar smoke"),
    ("scripts/livekit_sfu_smoke.py", "LiveKit SFU smoke"),
    ("sidecars/embedding/**", "embedding sidecar source"),
    ("sidecars/notify/**", "notification sidecar source"),
)

WORKFLOW_SCRIPT_REF_RE = re.compile(r"scripts/[A-Za-z0-9_.\-/]+")
WORKFLOW_SCRIPT_REF_ALLOWLIST = {
    "scripts/download-actionlint.bash",  # External URL in lint-workflows.yml.
    "scripts/udb.ps1",  # Release asset name, not an in-repo helper.
}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _require(text: str, needle: str, label: str, failures: list[str]) -> None:
    if needle not in text:
        failures.append(f"missing {label}: {needle}")


def _non_comment_text(text: str) -> str:
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def _workflow_job_block(text: str, job_name: str) -> str | None:
    match = re.search(rf"(?m)^  {re.escape(job_name)}:\n", text)
    if not match:
        return None
    next_match = re.search(r"(?m)^  [A-Za-z0-9_-]+:\n", text[match.end() :])
    end = match.end() + next_match.start() if next_match else len(text)
    return text[match.start() : end]


def _has_push_tag_trigger(text: str) -> bool:
    lines = text.splitlines()
    in_on = False
    in_push = False
    push_indent = 0
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 0:
            in_on = stripped == "on:"
            in_push = False
            continue
        if not in_on:
            continue
        if indent == 2:
            in_push = stripped == "push:"
            push_indent = indent if in_push else 0
            continue
        if in_push and indent > push_indent and stripped == "tags:":
            return True
    return False


def check_proof_workflows(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    workflow_dir = root / ".github" / "workflows"

    for name in PROOF_WORKFLOWS:
        path = workflow_dir / name
        if not path.is_file():
            failures.append(f"{name}: proof workflow file is missing")
            continue
        text = _read(path)
        scoped: list[str] = []
        _require(text, "workflow_dispatch:", "manual trigger", scoped)
        _require(text, "permissions:", "explicit permissions", scoped)
        _require(text, "contents: read", "read-only contents permission", scoped)
        _require(text, "concurrency:", "concurrency group", scoped)
        _require(text, "timeout-minutes:", "job timeout", scoped)
        if name in ARTIFACT_PROOF_WORKFLOWS:
            _require(text, "actions/upload-artifact@v4", "diagnostic artifact upload", scoped)
            _require(text, "if: always()", "always-run diagnostic/upload step", scoped)
            _require(text, "retention-days: 14", "bounded diagnostic retention", scoped)
        if name in DOCKER_PROOF_WORKFLOWS:
            _require(text, "docker compose", "docker compose invocation", scoped)
            _require(text, "down -v", "volume-removing teardown", scoped)
            _require(text, "--remove-orphans", "orphan cleanup", scoped)
        failures.extend(f"{name}: {failure}" for failure in scoped)

    return failures


def check_resilience_smoke_workflow(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    path = root / ".github" / "workflows" / "ha-smokes.yml"
    if not path.is_file():
        return ["ha-smokes.yml: resilience smoke workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in RESILIENCE_WORKFLOW_REQUIREMENTS:
        _require(text, needle, label, scoped)
    down_count = text.count("down -v --remove-orphans")
    if down_count < 4:
        scoped.append(
            "missing per-stack teardown: expected at least 4 down -v --remove-orphans "
            f"commands, found {down_count}"
        )
    return [f"ha-smokes.yml: {failure}" for failure in scoped]


def check_xa_recovery_smoke_script(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "ha_xa_recovery_smoke.sh"
    if not path.is_file():
        return ["ha_xa_recovery_smoke.sh: XA recovery smoke script is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in XA_RECOVERY_SCRIPT_REQUIREMENTS:
        _require(text, needle, label, scoped)
    if text.find('compose kill -s KILL "$KILL_SERVICE"') > text.find("XA PREPARE '${XID}';"):
        scoped.append("broker kill must happen before seeding the prepared XA transaction")
    if text.find("XA PREPARE '${XID}';") > text.find("INSERT INTO udb_system.udb_xa_ledger"):
        scoped.append("prepared MySQL XA transaction must be recorded before the UDB ledger seed")
    if text.find("INSERT INTO udb_system.udb_xa_ledger") > text.find("Waiting for surviving broker"):
        scoped.append("UDB ledger seed must happen before waiting for survivor recovery")
    return [f"ha_xa_recovery_smoke.sh: {failure}" for failure in scoped]


def check_sidecar_smoke_workflow(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    path = root / ".github" / "workflows" / "sidecar-smokes.yml"
    if not path.is_file():
        return ["sidecar-smokes.yml: native sidecar smoke workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in SIDECAR_WORKFLOW_REQUIREMENTS:
        _require(text, needle, label, scoped)
    down_count = text.count("down -v --remove-orphans")
    if down_count < 2:
        scoped.append(
            "missing per-sidecar teardown: expected at least 2 down -v --remove-orphans "
            f"commands, found {down_count}"
        )
    return [f"sidecar-smokes.yml: {failure}" for failure in scoped]


def check_sidecar_roundtrip_scripts(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    scripts = (
        (
            "scripts/embedding_sidecar_roundtrip_smoke.py",
            EMBEDDING_ROUNDTRIP_SCRIPT_REQUIREMENTS,
            "embedding sidecar round-trip",
        ),
        (
            "scripts/notify_sidecar_roundtrip_smoke.py",
            NOTIFY_ROUNDTRIP_SCRIPT_REQUIREMENTS,
            "notification sidecar round-trip",
        ),
    )
    for script_path, requirements, label in scripts:
        path = root / script_path
        if not path.is_file():
            failures.append(f"{script_path}: {label} script is missing")
            continue
        text = _read(path)
        scoped: list[str] = []
        for needle, requirement_label in requirements:
            _require(text, needle, requirement_label, scoped)
        dry_run_at = text.rfind("if args.dry_run:")
        callback_at = text.rfind("call_report_")
        if min(dry_run_at, callback_at) < 0:
            scoped.append("missing dry-run/callback ordering anchors")
        elif dry_run_at > callback_at:
            scoped.append("dry-run branch must be evaluated before broker callback")
        failures.extend(f"{script_path}: {failure}" for failure in scoped)
    return failures


def check_sidecar_container_sources(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    for rel_path, requirements in SIDECAR_CONTAINER_SOURCE_REQUIREMENTS.items():
        path = root / rel_path
        if not path.is_file():
            failures.append(f"{rel_path}: sidecar source file is missing")
            continue
        text = _read(path)
        scoped: list[str] = []
        for needle, label in requirements:
            _require(text, needle, label, scoped)
        failures.extend(f"{rel_path}: {failure}" for failure in scoped)
    return failures


def check_integration_compose_gate_d_profiles(root: Path = ROOT) -> list[str]:
    path = root / "docker-compose.integration.yml"
    if not path.is_file():
        return ["docker-compose.integration.yml: integration compose file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in INTEGRATION_COMPOSE_PROFILE_REQUIREMENTS:
        _require(text, needle, label, scoped)

    if text.count('profiles: ["sfu"]') < 3:
        scoped.append("sfu profile must cover udb-livekit, livekit, and coturn")
    if text.count('profiles: ["notify"]') < 1:
        scoped.append("notification profile must cover notify-sidecar")
    if text.count('profiles: ["embedding"]') < 1:
        scoped.append("embedding profile must cover embedding-sidecar")
    if text.count("healthz") < 2:
        scoped.append("embedding and notification sidecars must both keep healthchecks")
    return [f"docker-compose.integration.yml: {failure}" for failure in scoped]


def check_compose_support_inputs(root: Path = ROOT) -> list[str]:
    checks = (
        (
            "docker-compose.integration.yml",
            (
                ("docker/postgres-pg-partman/Dockerfile", "pg_partman Dockerfile path"),
                ("max_prepared_transactions=32", "Postgres prepared-xact setting"),
            ),
        ),
        (
            "docker-compose.canonical.yml",
            (
                ("./docker/mysql-init:/docker-entrypoint-initdb.d:ro", "MySQL init mount"),
                (
                    "./docker/clickhouse/config.d/keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro",
                    "ClickHouse Keeper config mount",
                ),
                ("system.zookeeper", "ClickHouse Keeper healthcheck"),
            ),
        ),
        (
            "docker-compose.xa-ha.yml",
            (
                ('profiles: ["broker-xa-ha"]', "XA HA broker profile"),
                ("UDB_MYSQL_DSN: mysql://udb:udb@mysql:3306/udb", "XA MySQL DSN"),
                ('UDB_XA_RECOVERY_INTERVAL_SECS: "2"', "fast XA recovery interval"),
                ("udb-xa-ha-a:", "XA HA broker A"),
                ("udb-xa-ha-b:", "XA HA broker B"),
            ),
        ),
        (
            "docker/postgres-pg-partman/Dockerfile",
            (
                ("FROM postgres:16-alpine AS pg-partman-builder", "Postgres builder base"),
                ("ARG PG_PARTMAN_VERSION=5.2.4", "pg_partman version pin"),
                ("make NO_BGW=1 install", "pg_partman install command"),
                ("COPY --from=pg-partman-builder", "pg_partman extension copy"),
            ),
        ),
        (
            "docker/mysql-init/01-grant-replication-client.sql",
            (
                ("GRANT REPLICATION CLIENT", "MySQL binlog-position grant"),
                ("GRANT CREATE, DROP ON *.* TO 'udb'@'%';", "MySQL live DB create/drop grant"),
                ("GRANT ALL PRIVILEGES ON `udb\\_conf\\_%`.* TO 'udb'@'%';", "MySQL conformance DB grant"),
                ("GRANT ALL PRIVILEGES ON `udb\\_ir\\_live\\_%`.* TO 'udb'@'%';", "MySQL IR live DB grant"),
                ("GRANT ALL PRIVILEGES ON `udb\\_ir\\_include\\_%`.* TO 'udb'@'%';", "MySQL IR include DB grant"),
                ("GRANT XA_RECOVER_ADMIN", "MySQL XA recover grant"),
            ),
        ),
        (
            "docker/clickhouse/config.d/keeper.xml",
            (
                ("<keeper_map_path_prefix>/udb/keeper_map_tables</keeper_map_path_prefix>", "KeeperMap path prefix"),
                ("<keeper_server>", "embedded Keeper server"),
                ("<tcp_port>9181</tcp_port>", "Keeper client port"),
                ("<port>9234</port>", "Keeper raft port"),
                ("<zookeeper>", "ClickHouse zookeeper client config"),
            ),
        ),
    )
    failures: list[str] = []
    for rel_path, requirements in checks:
        path = root / rel_path
        if not path.is_file():
            failures.append(f"{rel_path}: compose support input is missing")
            continue
        text = _read(path)
        scoped: list[str] = []
        for needle, label in requirements:
            _require(text, needle, label, scoped)
        failures.extend(f"{rel_path}: {failure}" for failure in scoped)
    return failures


def _workflow_dispatch_input_block(text: str, input_name: str) -> str | None:
    lines = text.splitlines()
    start = None
    header = f"      {input_name}:"
    for index, line in enumerate(lines):
        if line == header:
            start = index
            break
    if start is None:
        return None
    block = [lines[start]]
    for line in lines[start + 1 :]:
        if line.startswith("      ") and not line.startswith("        "):
            break
        block.append(line)
    return "\n".join(block)


def _require_workflow_dispatch_input_required(text: str, input_name: str, failures: list[str]) -> None:
    block = _workflow_dispatch_input_block(text, input_name)
    if block is None:
        failures.append(f"workflow_dispatch input {input_name!r} is missing")
        return
    if re.search(r"(?m)^        required: true\s*$", block) is None:
        failures.append(
            f"workflow_dispatch input {input_name!r} must be required because the workflow consumes it as proof evidence"
        )
    if re.search(r'(?im)^        description:\s*["\']?[^"\']*\boptional\b', block):
        failures.append(
            f"workflow_dispatch input {input_name!r} must not be described as optional because the workflow consumes it as proof evidence"
        )


def _require_workflow_dispatch_input_has_no_default(text: str, input_name: str, failures: list[str]) -> None:
    block = _workflow_dispatch_input_block(text, input_name)
    if block is None:
        failures.append(f"workflow_dispatch input {input_name!r} is missing")
        return
    if re.search(r"(?m)^        default:\s*", block):
        failures.append(
            f"workflow_dispatch input {input_name!r} must not define a default because proof evidence must be operator-supplied"
        )


def check_targeted_proof_workflows(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    workflow_dir = root / ".github" / "workflows"
    for name, requirements in TARGETED_PROOF_WORKFLOW_REQUIREMENTS.items():
        path = workflow_dir / name
        if not path.is_file():
            failures.append(f"{name}: targeted proof workflow file is missing")
            continue
        text = _read(path)
        scoped: list[str] = []
        for needle, label in requirements:
            _require(text, needle, label, scoped)
        for input_name in REQUIRED_PROOF_WORKFLOW_INPUTS.get(name, ()):
            _require_workflow_dispatch_input_required(text, input_name, scoped)
        for input_name in NO_DEFAULT_PROOF_WORKFLOW_INPUTS.get(name, ()):
            _require_workflow_dispatch_input_has_no_default(text, input_name, scoped)
        if name == "runner-evidence-audit.yml":
            for redundant_flag in (
                "--idempotency-served-smoke",
                "--error-detail-served-smoke",
                "--retry-safe-served-smoke",
                "--rest-gateway-smoke",
            ):
                if redundant_flag in text:
                    scoped.append(
                        f"runner-evidence all-evidence must imply served proof lanes; remove redundant {redundant_flag}"
                    )
        failures.extend(f"{name}: {failure}" for failure in scoped)
    return failures


def check_release_topology(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    workflow_dir = root / ".github" / "workflows"
    release_path = workflow_dir / "release.yml"
    if not release_path.is_file():
        return ["release.yml: release orchestrator workflow file is missing"]

    release_text = _read(release_path)
    scoped: list[str] = []
    for needle, label in RELEASE_ORCHESTRATOR_REQUIREMENTS:
        _require(release_text, needle, label, scoped)
    if not _has_push_tag_trigger(release_text):
        scoped.append("release orchestrator must be the tag-triggered release entrypoint")
    for job_name in (
        "publish-crates",
        "publish-docker",
        "publish-ts",
        "publish-py",
        "publish-csharp",
        "publish-packagist",
    ):
        match = re.search(
            rf"(?ms)^  {re.escape(job_name)}:\n(?P<block>.*?)(?=^  [A-Za-z0-9_-]+:|\Z)",
            release_text,
        )
        if match is None:
            continue
        block = match.group("block")
        if "needs: build-binaries" not in block:
            scoped.append(f"{job_name} must wait for build-binaries")
        if "secrets: inherit" not in block:
            scoped.append(f"{job_name} must inherit release secrets only through the orchestrator")
    failures.extend(f"release.yml: {failure}" for failure in scoped)

    for name in RELEASE_LEAF_WORKFLOWS:
        path = workflow_dir / name
        if not path.is_file():
            failures.append(f"{name}: release leaf workflow file is missing")
            continue
        text = _read(path)
        scoped = []
        _require(text, "workflow_call:", "reusable workflow_call trigger", scoped)
        if _has_push_tag_trigger(text):
            scoped.append("release leaf must not define its own push tag trigger")
        active_text = _non_comment_text(text)
        if name in RELEASE_PUBLISHER_WORKFLOWS and "workflow_dispatch:" in active_text:
            _require_workflow_dispatch_input_required(text, "version", scoped)
            _require_workflow_dispatch_input_has_no_default(text, "version", scoped)
            _require(
                active_text,
                "dispatch-version: ${{ github.event.inputs.version }}",
                "manual release version guard",
                scoped,
            )
        failures.extend(f"{name}: {failure}" for failure in scoped)
    return failures


def check_cleanup_packages_ownership(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    workflow_dir = root / ".github" / "workflows"
    cleanup_path = workflow_dir / "cleanup-packages.yml"
    if not cleanup_path.is_file():
        return ["cleanup-packages.yml: cleanup workflow file is missing"]

    cleanup_text = _read(cleanup_path)
    scoped: list[str] = []
    for needle, label in CLEANUP_PACKAGES_REQUIREMENTS:
        _require(cleanup_text, needle, label, scoped)
    failures.extend(f"cleanup-packages.yml: {failure}" for failure in scoped)

    for path in workflow_dir.glob("*.yml"):
        if path.name == "cleanup-packages.yml":
            continue
        text = _read(path)
        if "actions/delete-package-versions" in text:
            failures.append(f"{path.name}: package deletion must stay owned by cleanup-packages.yml")
        if "/packages/container/udb/versions" in text:
            failures.append(f"{path.name}: GHCR package listing must stay owned by cleanup-packages.yml")
    return failures


def check_publish_skill_workflow(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "publish-skill.yml"
    if not path.is_file():
        return ["publish-skill.yml: skill publish workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in PUBLISH_SKILL_REQUIREMENTS:
        _require(text, needle, label, scoped)
    for job_name in ("smoke", "ollama", "openai"):
        match = re.search(
            rf"(?ms)^  {re.escape(job_name)}:\n(?P<block>.*?)(?=^  [A-Za-z0-9_-]+:|\Z)",
            text,
        )
        if match is None:
            scoped.append(f"missing {job_name} skill publish job")
            continue
        if "needs: validate" not in match.group("block"):
            scoped.append(f"{job_name} skill publish job must wait for validate")
    if "contents: write" in _non_comment_text(text) or "packages: write" in _non_comment_text(text):
        scoped.append("skill publish workflow must keep read-only repository permissions")
    return [f"publish-skill.yml: {failure}" for failure in scoped]


def check_shadow_live_sdk_workflow(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "_shadow-live-sdk.yml"
    if not path.is_file():
        return ["_shadow-live-sdk.yml: shadow live SDK workflow file is missing"]

    text = _read(path)
    active_text = _non_comment_text(text)
    scoped: list[str] = []
    for needle, label in SHADOW_LIVE_SDK_REQUIREMENTS:
        _require(text, needle, label, scoped)
    if "push:" in active_text or "workflow_run:" in active_text or "schedule:" in active_text:
        scoped.append("shadow live SDK workflow must remain manual-only")
    if "actions/deploy-pages" in active_text or "pages: write" in active_text:
        scoped.append("shadow live SDK workflow must not deploy Pages")
    if "cargo build" in active_text:
        scoped.append("shadow live SDK workflow must not rebuild the broker")
    return [f"_shadow-live-sdk.yml: {failure}" for failure in scoped]


def check_composite_selftest_workflow(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "_selftest.yml"
    if not path.is_file():
        return ["_selftest.yml: composite selftest workflow file is missing"]

    text = _read(path)
    active_text = _non_comment_text(text)
    scoped: list[str] = []
    for needle, label in COMPOSITE_SELFTEST_REQUIREMENTS:
        _require(text, needle, label, scoped)
    if "push:" in active_text or "workflow_run:" in active_text or "schedule:" in active_text:
        scoped.append("composite selftest workflow must remain manual-only")
    if "contents: write" in active_text or "packages: write" in active_text or "pages: write" in active_text:
        scoped.append("composite selftest workflow must keep read-only permissions")
    if "cargo build" in active_text:
        scoped.append("composite selftest workflow must not build release-grade artifacts")
    return [f"_selftest.yml: {failure}" for failure in scoped]


def check_composite_action_contracts(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    for rel_path, requirements in COMPOSITE_ACTION_SOURCE_REQUIREMENTS.items():
        path = root / rel_path
        if not path.is_file():
            failures.append(f"{rel_path}: composite action file is missing")
            continue
        text = _read(path)
        scoped: list[str] = []
        for needle, label in requirements:
            _require(text, needle, label, scoped)
        failures.extend(f"{rel_path}: {failure}" for failure in scoped)
    return failures


def check_release_binaries_ffmpeg_gate(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    path = root / ".github" / "workflows" / "release-binaries.yml"
    if not path.is_file():
        return ["release-binaries.yml: release workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in RELEASE_FFMPEG_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"release-binaries.yml: {failure}" for failure in scoped]


def check_ffmpeg_transcode_smoke_contract(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "ffmpeg_transcode_smoke.py"
    if not path.is_file():
        return ["scripts/ffmpeg_transcode_smoke.py: ffmpeg transcode smoke is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in FFMPEG_TRANSCODE_SMOKE_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"scripts/ffmpeg_transcode_smoke.py: {failure}" for failure in scoped]


def check_livekit_sfu_smoke_contract(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "livekit_sfu_smoke.py"
    if not path.is_file():
        return ["scripts/livekit_sfu_smoke.py: LiveKit SFU smoke is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in LIVEKIT_SFU_SMOKE_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"scripts/livekit_sfu_smoke.py: {failure}" for failure in scoped]


def check_release_binary_matrix_contract(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "release-binaries.yml"
    if not path.is_file():
        return ["release-binaries.yml: release binary producer workflow is missing"]

    text = _read(path)
    active_text = _non_comment_text(text)
    scoped: list[str] = []
    for needle, label in RELEASE_BINARY_MATRIX_REQUIREMENTS:
        _require(text, needle, label, scoped)

    expected_assets = {
        "udb-linux-amd64",
        "udb-windows-amd64.exe",
        "udb-darwin-arm64",
        "udb-darwin-amd64",
        "udb-linux-amd64-full",
    }
    found_assets = set(re.findall(r"^\s*asset:\s+([A-Za-z0-9_.-]+)\s*$", text, flags=re.MULTILINE))
    if found_assets != expected_assets:
        scoped.append(
            "release binary matrix assets must be exactly "
            f"{', '.join(sorted(expected_assets))}; found {', '.join(sorted(found_assets)) or '<none>'}"
        )
    if _has_push_tag_trigger(text):
        scoped.append("release-binaries must stay reusable/manual-only; release.yml owns tag trigger")
    if "actions/deploy-pages" in active_text or "pages: write" in active_text:
        scoped.append("release-binaries must not own Pages deployment")
    if "actions/delete-package-versions" in active_text or "/packages/container/udb/versions" in active_text:
        scoped.append("release-binaries must not own package cleanup")
    return [f"release-binaries.yml/matrix: {failure}" for failure in scoped]


def check_release_manifest_generator_contract(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "gen-release-manifest.mjs"
    if not path.is_file():
        return ["scripts/gen-release-manifest.mjs: release manifest generator is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in RELEASE_MANIFEST_GENERATOR_REQUIREMENTS:
        _require(text, needle, label, scoped)

    manifest_at = text.find("return {\n    version,")
    assets_at = text.find("assets,", manifest_at)
    if min(manifest_at, assets_at) < 0:
        scoped.append("release manifest must return version/tag/scheme/base_url/assets metadata together")
    return [f"scripts/gen-release-manifest.mjs: {failure}" for failure in scoped]


def check_release_publisher_leaf_contracts(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    workflow_dir = root / ".github" / "workflows"
    forbidden_commands = (
        "cargo build",
        "cargo test",
        "cargo run",
        "buf generate",
        "udb sdk generate",
        "native manifest",
        "native docs",
        "native contract-diff",
        "actions/deploy-pages",
        "actions/delete-package-versions",
    )

    def invokes_forbidden_command(text: str, command: str) -> bool:
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith(f"run: {command}") or stripped.startswith(f"- run: {command}"):
                return True
            if stripped.startswith(command):
                return True
            if command.startswith("actions/") and command in stripped:
                return True
        return False

    for name, requirements in RELEASE_PUBLISHER_LEAF_REQUIREMENTS.items():
        path = workflow_dir / name
        if not path.is_file():
            failures.append(f"{name}: release publisher workflow file is missing")
            continue
        text = _read(path)
        active_text = _non_comment_text(text)
        scoped: list[str] = []
        for needle, label in requirements:
            _require(text, needle, label, scoped)
        for forbidden in forbidden_commands:
            if invokes_forbidden_command(active_text, forbidden):
                scoped.append(
                    "release publisher leaf must not re-run CI Rust/build/codegen/cleanup/Pages gates: "
                    f"{forbidden}"
                )
        failures.extend(f"{name}: {failure}" for failure in scoped)
    return failures


def check_release_docker_single_artifact(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "release-docker.yml"
    if not path.is_file():
        return ["release-docker.yml: release Docker workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in RELEASE_DOCKER_REQUIREMENTS:
        _require(text, needle, label, scoped)
    download_at = text.find("Download release binary into build context")
    build_at = text.find("Build and push")
    if min(download_at, build_at) < 0:
        scoped.append("missing release Docker download/build ordering anchors")
    elif download_at > build_at:
        scoped.append("release Docker image must download the published binary before build-push")
    if "cargo build" in text:
        scoped.append("release Docker workflow must not rebuild the broker with cargo")
    return [f"release-docker.yml: {failure}" for failure in scoped]


def check_release_dockerfile_contract(root: Path = ROOT) -> list[str]:
    path = root / "Dockerfile.release"
    if not path.is_file():
        return ["Dockerfile.release: release Dockerfile is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in DOCKERFILE_RELEASE_REQUIREMENTS:
        _require(text, needle, label, scoped)
    if "cargo build" in text or "FROM rust" in text:
        scoped.append("release Dockerfile must stay runtime-only and never compile the broker")
    return [f"Dockerfile.release: {failure}" for failure in scoped]


def check_ci_launcher_asset_gate(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    path = root / ".github" / "workflows" / "ci.yml"
    if not path.is_file():
        return ["ci.yml: CI workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in CI_LAUNCHER_ASSET_REQUIREMENTS:
        _require(text, needle, label, scoped)
    if not any(line.strip() == "node scripts/check-launcher-assets.mjs" for line in text.splitlines()):
        scoped.append("missing launcher asset repo scan: node scripts/check-launcher-assets.mjs")
    return [f"ci.yml: {failure}" for failure in scoped]


def check_ci_sdk_service_coverage_gate(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    path = root / ".github" / "workflows" / "ci.yml"
    if not path.is_file():
        return ["ci.yml: CI workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in CI_SDK_SERVICE_COVERAGE_REQUIREMENTS:
        _require(text, needle, label, scoped)
    if not any(line.strip() == "python3 scripts/check-sdk-service-coverage.py" for line in text.splitlines()):
        scoped.append("missing SDK service-coverage repo scan: python3 scripts/check-sdk-service-coverage.py")
    return [f"ci.yml: {failure}" for failure in scoped]


def check_ci_topology_contract(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "ci.yml"
    if not path.is_file():
        return ["ci.yml: CI workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in CI_TOPOLOGY_REQUIREMENTS:
        _require(text, needle, label, scoped)

    jobs_at = text.find("\njobs:")
    trigger_block = text[:jobs_at] if jobs_at >= 0 else text
    if re.search(r"(?m)^\s+paths(-ignore)?:", trigger_block):
        scoped.append("required CI workflow must not be path-filtered")

    for job_name in CI_TOPOLOGY_DEPENDENCY_FREE_JOBS:
        block = _workflow_job_block(text, job_name)
        if block is None:
            scoped.append(f"missing dependency-free CI job: {job_name}")
            continue
        if re.search(r"(?m)^    needs:", block):
            scoped.append(f"{job_name} must stay dependency-free for maximum CI parallelism")

    for job_name in CI_TOPOLOGY_QUICK_GATE_JOBS:
        block = _workflow_job_block(text, job_name)
        if block is None:
            scoped.append(f"missing quick-gated CI job: {job_name}")
            continue
        if "needs: quick-gate" not in block:
            scoped.append(f"{job_name} must wait on quick-gate before expensive work")

    for job_name in CI_TOPOLOGY_BUILD_BROKER_CONSUMERS:
        block = _workflow_job_block(text, job_name)
        if block is None:
            scoped.append(f"missing broker-artifact consumer job: {job_name}")
            continue
        if "needs: build-broker" not in block:
            scoped.append(f"{job_name} must consume the single build-broker artifact")
        if "name: udb-broker-debug" not in block:
            scoped.append(f"{job_name} must download artifact udb-broker-debug")

    for job_name in CI_TOPOLOGY_PUSH_ONLY_JOBS:
        block = _workflow_job_block(text, job_name)
        if block is None:
            scoped.append(f"missing push-only CI job: {job_name}")
            continue
        if "if: github.event_name == 'push'" not in block:
            scoped.append(f"{job_name} must be push-only")

    for job_name in CI_TOPOLOGY_PR_ONLY_JOBS:
        block = _workflow_job_block(text, job_name)
        if block is None:
            scoped.append(f"missing PR-only CI job: {job_name}")
            continue
        if "if: github.event_name == 'pull_request'" not in block:
            scoped.append(f"{job_name} must be PR-only")

    sdk_conformance = _workflow_job_block(text, "sdk-conformance") or ""
    for needle, label in CI_SDK_CONFORMANCE_REQUIREMENTS:
        _require(sdk_conformance, needle, label, scoped)

    return [f"ci.yml/topology: {failure}" for failure in scoped]


def check_ci_architecture_contract(root: Path = ROOT) -> list[str]:
    doc_path = root / "docs" / "ci-architecture.md"
    ci_path = root / ".github" / "workflows" / "ci.yml"
    bench_path = root / ".github" / "workflows" / "benchmark-sdks.yml"
    pages_path = root / ".github" / "workflows" / "pages.yml"
    cleanup_path = root / ".github" / "workflows" / "cleanup-packages.yml"
    lint_path = root / ".github" / "workflows" / "lint-workflows.yml"
    missing = [
        f"{path.relative_to(root)}: missing"
        for path in (doc_path, ci_path, bench_path, pages_path, cleanup_path, lint_path)
        if not path.is_file()
    ]
    if missing:
        return [f"ci-architecture: {failure}" for failure in missing]

    doc_text = _read(doc_path)
    ci_text = _non_comment_text(_read(ci_path))
    bench_text = _read(bench_path)
    pages_text = _read(pages_path)
    cleanup_text = _read(cleanup_path)
    lint_text = _read(lint_path)
    scoped: list[str] = []
    for needle, label in CI_ARCHITECTURE_REQUIREMENTS:
        _require(doc_text, needle, label, scoped)

    stale_doc_claims = (
        "live-suite[conformance]",
        "live-suite (conformance)",
        "_live-sdk-suite[conformance]",
        "Called by ci (conformance)",
        "ci.yml::sdk-live-conformance` | calls",
    )
    for claim in stale_doc_claims:
        if claim in doc_text:
            scoped.append(f"architecture doc must not claim CI live-SDK conformance ownership: {claim}")

    if "uses: ./.github/workflows/_live-sdk-suite.yml" in ci_text:
        scoped.append("ci.yml must not call _live-sdk-suite; post-release benchmark owns live SDK RPC coverage")
    if "uses: ./.github/workflows/_live-sdk-suite.yml" not in bench_text:
        scoped.append("benchmark-sdks.yml must call _live-sdk-suite for live SDK benchmark coverage")
    if "→ live-suite[perf] → pages → cleanup" in doc_text or "-> live-suite[perf] -> pages -> cleanup" in doc_text:
        scoped.append("architecture doc must model benchmark/Pages/cleanup as workflow_run side effects, not inline release jobs")
    if 'workflows: ["Release"]' not in bench_text or "github.event.workflow_run.conclusion == 'success'" not in bench_text:
        scoped.append("benchmark-sdks.yml must run after successful top-level Release")
    if 'workflows: ["Benchmark · SDKs"]' not in pages_text or "sdk-benchmark-results" not in pages_text:
        scoped.append("pages.yml must publish after benchmark completion and consume sdk-benchmark-results")
    if 'workflows: ["Release"]' not in cleanup_text or "github.event.workflow_run.conclusion == 'success'" not in cleanup_text:
        scoped.append("cleanup-packages.yml must stay successful-Release/schedule/dispatch owned")
    required_at = doc_text.find("Required (PR gate):")
    not_required_at = doc_text.find("NOT required", required_at)
    required_block = doc_text[required_at:not_required_at] if required_at >= 0 and not_required_at > required_at else ""
    if "paths:" in lint_text and "actionlint" in required_block:
        scoped.append("path-filtered actionlint must not be listed as a required PR check")
    return [f"ci-architecture: {failure}" for failure in scoped]


def check_ci_quick_gate_source_guards(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    path = root / ".github" / "workflows" / "ci.yml"
    if not path.is_file():
        return ["ci.yml: CI workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    lines = {line.strip() for line in text.splitlines()}
    for step_name, script, label in CI_QUICK_GATE_SOURCE_GUARDS:
        _require(text, step_name, f"{label} CI step", scoped)
        selftest = f"python3 {script} --selftest"
        repo_scan = f"python3 {script}"
        if selftest not in lines:
            scoped.append(f"missing {label} selftest: {selftest}")
        if repo_scan not in lines:
            scoped.append(f"missing {label} repo scan: {repo_scan}")
    return [f"ci.yml: {failure}" for failure in scoped]


def check_ci_public_docs_guards(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "ci.yml"
    if not path.is_file():
        return ["ci.yml: CI workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for step_name, script, label in CI_PUBLIC_DOC_GUARDS:
        step_at = text.find(step_name)
        if step_at < 0:
            scoped.append(f"missing {label} CI step: {step_name}")
            continue
        next_step_at = text.find("\n      - name:", step_at + len(step_name))
        step_block = text[step_at:next_step_at] if next_step_at > step_at else text[step_at:]
        if "if: runner.os == 'Linux'" not in step_block:
            scoped.append(f"missing Linux-only gate for {label} step")
        for command, command_label in (
            (f"python3 {script} --selftest", "selftest"),
            (f"python3 {script}", "repo scan"),
        ):
            if command not in {line.strip() for line in step_block.splitlines()}:
                scoped.append(f"missing {label} {command_label}: {command}")
    return [f"ci.yml/rust-public-docs: {failure}" for failure in scoped]


def check_ci_docs_links_gate(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "ci.yml"
    if not path.is_file():
        return ["ci.yml: CI workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    job_block = _workflow_job_block(text, "docs-links")
    if not job_block:
        return ["ci.yml/docs-links: docs-links job is missing"]
    for needle, label in CI_DOCS_LINKS_REQUIREMENTS:
        _require(job_block, needle, label, scoped)
    return [f"ci.yml/docs-links: {failure}" for failure in scoped]


def check_markdown_link_guard_contract(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "check-markdown-links.mjs"
    if not path.is_file():
        return ["scripts/check-markdown-links.mjs: markdown link guard is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in MARKDOWN_LINK_GUARD_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"scripts/check-markdown-links.mjs: {failure}" for failure in scoped]


def check_enterprise_readiness_guard_contract(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "check-enterprise-readiness.mjs"
    if not path.is_file():
        return ["scripts/check-enterprise-readiness.mjs: enterprise readiness guard is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in ENTERPRISE_READINESS_GUARD_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"scripts/check-enterprise-readiness.mjs: {failure}" for failure in scoped]


def check_openapi_api_rule_guard_contract(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "check-openapi-api-rules.mjs"
    if not path.is_file():
        return ["scripts/check-openapi-api-rules.mjs: OpenAPI API-rule guard is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in OPENAPI_API_RULE_GUARD_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"scripts/check-openapi-api-rules.mjs: {failure}" for failure in scoped]


def check_http_api_style_guard_contract(root: Path = ROOT) -> list[str]:
    script = root / "scripts" / "check-http-api-style.mjs"
    allow = root / "scripts" / "http-api-style.allow.json"
    if not script.is_file():
        return ["scripts/check-http-api-style.mjs: HTTP API route-style guard is missing"]
    if not allow.is_file():
        return ["scripts/http-api-style.allow.json: HTTP API route-style allowlist is missing"]

    text = _read(script)
    scoped: list[str] = []
    for needle, label in HTTP_API_STYLE_GUARD_REQUIREMENTS:
        _require(text, needle, label, scoped)

    allow_text = _read(allow)
    for needle, label in (
        ("well-known", "JWKS allowlist entry"),
        ("idp/scim", "SCIM allowlist entry"),
        ("Users|Groups", "SCIM Users/Groups exception"),
        ("webauthn/credentials", "deep WebAuthn credential reason"),
        ("control", "control-plane command endpoint reason"),
    ):
        _require(allow_text, needle, label, scoped)

    return [f"scripts/check-http-api-style.mjs: {failure}" for failure in scoped]


def check_rest_route_gateway_smoke_contract(root: Path = ROOT) -> list[str]:
    script = root / "scripts" / "rest_route_gateway_smoke.py"
    if not script.is_file():
        return ["scripts/rest_route_gateway_smoke.py: REST route gateway smoke guard is missing"]
    text = _read(script)
    scoped: list[str] = []
    for needle, label in REST_ROUTE_GATEWAY_SMOKE_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"scripts/rest_route_gateway_smoke.py: {failure}" for failure in scoped]


def check_beta_versioning_posture_contract(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "check-beta-versioning-posture.py"
    if not path.is_file():
        return ["scripts/check-beta-versioning-posture.py: beta versioning posture guard is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in BETA_VERSIONING_POSTURE_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"scripts/check-beta-versioning-posture.py: {failure}" for failure in scoped]


def check_ci_http_api_style_gate(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "ci.yml"
    if not path.is_file():
        return ["ci.yml: CI workflow file is missing"]

    text = _read(path)
    step_at = text.find("HTTP API route-style guard")
    if step_at < 0:
        return ["ci.yml: missing HTTP API route-style guard CI step"]
    next_step_at = text.find("\n      - name:", step_at + len("HTTP API route-style guard"))
    block = text[step_at:next_step_at] if next_step_at > step_at else text[step_at:]
    scoped: list[str] = []
    lines = {line.strip() for line in block.splitlines()}
    for command, label in CI_HTTP_API_STYLE_COMMANDS:
        if command not in lines:
            scoped.append(f"missing {label}: {command}")
    return [f"ci.yml: {failure}" for failure in scoped]


def check_ci_inventory_guard_contract(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "ci-inventory.mjs"
    if not path.is_file():
        return ["scripts/ci-inventory.mjs: CI inventory guard is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in CI_INVENTORY_GUARD_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"scripts/ci-inventory.mjs: {failure}" for failure in scoped]


def check_branch_protection_lockstep_guard(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "check-branch-protection-lockstep.mjs"
    if not path.is_file():
        return ["scripts/check-branch-protection-lockstep.mjs: branch-protection lockstep audit is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in BRANCH_PROTECTION_LOCKSTEP_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"scripts/check-branch-protection-lockstep.mjs: {failure}" for failure in scoped]


def check_ci_runner_evidence_guard(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "check-ci-runner-evidence.mjs"
    if not path.is_file():
        return ["scripts/check-ci-runner-evidence.mjs: CI runner evidence audit is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in CI_RUNNER_EVIDENCE_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"scripts/check-ci-runner-evidence.mjs: {failure}" for failure in scoped]


def check_error_detail_served_smoke_contract(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "error_detail_served_smoke.py"
    if not path.is_file():
        return ["scripts/error_detail_served_smoke.py: ErrorDetail served smoke is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in ERROR_DETAIL_SERVED_SMOKE_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"scripts/error_detail_served_smoke.py: {failure}" for failure in scoped]


def check_idempotency_served_smoke_contract(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "idempotency_served_replay_smoke.py"
    if not path.is_file():
        return ["scripts/idempotency_served_replay_smoke.py: idempotency served replay smoke is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in IDEMPOTENCY_SERVED_SMOKE_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"scripts/idempotency_served_replay_smoke.py: {failure}" for failure in scoped]


def check_retry_safe_served_smoke_contract(root: Path = ROOT) -> list[str]:
    path = root / "scripts" / "retry_safe_served_smoke.py"
    if not path.is_file():
        return ["scripts/retry_safe_served_smoke.py: retry-safe served smoke is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in RETRY_SAFE_SERVED_SMOKE_REQUIREMENTS:
        _require(text, needle, label, scoped)
    return [f"scripts/retry_safe_served_smoke.py: {failure}" for failure in scoped]


def check_ci_rust_generated_contract_doc_gates(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "ci.yml"
    if not path.is_file():
        return ["ci.yml: CI workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for step_name, label, needles in CI_RUST_GENERATED_CONTRACT_DOC_GATES:
        step_at = text.find(step_name)
        if step_at < 0:
            scoped.append(f"missing {label} CI step: {step_name}")
            continue
        next_step_at = text.find("\n      - name:", step_at + len(step_name))
        step_block = text[step_at:next_step_at] if next_step_at > step_at else text[step_at:]
        if "if: runner.os == 'Linux'" not in step_block:
            scoped.append(f"missing Linux-only gate for {label} step")
        for needle in needles:
            _require(step_block, needle, label, scoped)
    return [f"ci.yml/rust-generated-contract-docs: {failure}" for failure in scoped]


def check_ci_buf_generated_artifact_gate(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "ci.yml"
    if not path.is_file():
        return ["ci.yml: CI workflow file is missing"]

    text = _read(path)
    block = _workflow_job_block(text, "buf") or text
    scoped: list[str] = []
    for needle, label in CI_BUF_GENERATED_ARTIFACT_REQUIREMENTS:
        _require(block, needle, label, scoped)

    anchors = (
        "buf:",
        "bufbuild/buf-setup-action@v1",
        "buf build",
        "Verify committed stubs are current",
        "buf generate --include-imports",
        "node scripts/openapi-postprocess.mjs",
        "node scripts/sdk-codegen-postprocess.mjs",
        "git diff --quiet -- sdk/php/gen",
        "Authn/Authz inventory drift (Phase 0A)",
        "node scripts/generate-authn-authz-inventory.mjs",
        "git diff --quiet -- docs/generated/authn-authz-rpc-inventory.md",
    )
    positions = [block.find(anchor) for anchor in anchors]
    if any(pos < 0 for pos in positions):
        scoped.append("missing buf generated-artifact ordering anchors")
    elif positions != sorted(positions):
        scoped.append("buf job must build, regenerate SDK/API artifacts, postprocess, diff, then refresh authn/authz inventories")

    return [f"ci.yml/buf-generated-artifacts: {failure}" for failure in scoped]


def check_ci_smoke_load_gate(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "ci.yml"
    if not path.is_file():
        return ["ci.yml: CI workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in CI_SMOKE_LOAD_GATE_REQUIREMENTS:
        _require(text, needle, label, scoped)

    anchors = (
        "build-broker:",
        "Build udb (live tier)",
        "Upload broker binary",
        "smoke:",
        "Download broker binary",
        "Launch broker",
        "Verify reflection surface",
        "Run native load smoke + p99 regression gate",
        "Upload load summary",
        "Stop broker",
    )
    positions = [text.find(anchor) for anchor in anchors]
    if any(pos < 0 for pos in positions):
        scoped.append("missing smoke/load ordering anchors")
    elif positions != sorted(positions):
        scoped.append("CI smoke must build once, download the artifact, launch, reflect, load-test, upload, then clean up")

    upload_at = text.find("Upload load summary")
    stop_at = text.find("Stop broker")
    upload_block = text[upload_at:stop_at] if upload_at >= 0 and stop_at > upload_at else ""
    stop_block = text[stop_at:] if stop_at >= 0 else ""
    if "if: always()" not in upload_block:
        scoped.append("native load summary upload must run with if: always()")
    if "if: always()" not in stop_block:
        scoped.append("broker cleanup must run with if: always()")
    return [f"ci.yml/smoke-load: {failure}" for failure in scoped]


def check_native_load_case_contract(root: Path = ROOT) -> list[str]:
    script_path = root / "scripts" / "native-load-test.sh"
    baseline_path = root / "scripts" / "native_load_smoke_baseline.json"
    scoped: list[str] = []
    if not script_path.is_file():
        scoped.append("native-load-test.sh is missing")
    if not baseline_path.is_file():
        scoped.append("native_load_smoke_baseline.json is missing")
    if scoped:
        return [f"native-load: {failure}" for failure in scoped]

    script_text = _read(script_path)
    script_cases = NATIVE_LOAD_CASE_RE.findall(script_text)
    duplicate_script_cases = sorted({case for case in script_cases if script_cases.count(case) > 1})
    if duplicate_script_cases:
        scoped.append(f"duplicate script run_case names: {', '.join(duplicate_script_cases)}")

    try:
        baseline = json.loads(_read(baseline_path))
    except json.JSONDecodeError as exc:
        scoped.append(f"baseline JSON is invalid: {exc}")
        baseline = {}

    baseline_cases_obj = baseline.get("cases") if isinstance(baseline, dict) else None
    if not isinstance(baseline_cases_obj, dict):
        scoped.append("baseline must contain a cases object")
        baseline_cases: list[str] = []
    else:
        baseline_cases = list(baseline_cases_obj)
        for case, entry in baseline_cases_obj.items():
            if not isinstance(entry, dict) or "p99_ms" not in entry:
                scoped.append(f"baseline case {case} is missing p99_ms")

    required = set(NATIVE_LOAD_REQUIRED_CASES)
    script_set = set(script_cases)
    baseline_set = set(baseline_cases)
    for label, case_set in (("script", script_set), ("baseline", baseline_set)):
        missing = sorted(required - case_set)
        unexpected = sorted(case_set - required)
        if missing:
            scoped.append(f"native load {label} missing required case(s): {', '.join(missing)}")
        if unexpected:
            scoped.append(f"native load {label} has untracked case(s): {', '.join(unexpected)}")

    if script_set != baseline_set:
        missing_from_baseline = sorted(script_set - baseline_set)
        missing_from_script = sorted(baseline_set - script_set)
        if missing_from_baseline:
            scoped.append(f"native load baseline missing script case(s): {', '.join(missing_from_baseline)}")
        if missing_from_script:
            scoped.append(f"native load script missing baseline case(s): {', '.join(missing_from_script)}")

    if baseline.get("source") != "scripts/native-load-test.sh smoke profile":
        scoped.append("baseline source must name scripts/native-load-test.sh smoke profile")
    threshold = baseline.get("threshold") if isinstance(baseline, dict) else None
    if not isinstance(threshold, dict) or threshold.get("max_regression_percent") != 15:
        scoped.append("baseline threshold must keep max_regression_percent at 15")
    return [f"native-load: {failure}" for failure in scoped]


def check_ci_native_integration_gate(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "ci.yml"
    if not path.is_file():
        return ["ci.yml: CI workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in CI_NATIVE_INTEGRATION_REQUIREMENTS:
        _require(text, needle, label, scoped)

    anchors = (
        "Start integration stack while compiling tests",
        "Compile native + integration tests",
        "Start canonical-store stack",
        "Initialize SQL Server database",
        "IR compiler live golden tests",
        "Native service live tests",
        "Canonical store live conformance",
        "Integration harness (CDC, sagas, backends)",
        "Dump stack logs on failure",
        "Stop integration stacks",
    )
    positions = [text.find(anchor) for anchor in anchors]
    if any(pos < 0 for pos in positions):
        scoped.append("missing native-integration ordering anchors")
    elif positions != sorted(positions):
        scoped.append("native-integration must overlap the integration stack with compile, start the heavier canonical stack after compile, initialize live dependencies, run live suites, dump diagnostics, then clean up")

    cleanup_at = text.find("Stop integration stacks")
    cleanup_block = text[cleanup_at:] if cleanup_at >= 0 else ""
    if "if: always()" not in cleanup_block:
        scoped.append("native-integration stack cleanup must run with if: always()")
    return [f"ci.yml/native-integration: {failure}" for failure in scoped]


def check_benchmark_workflow_gate(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "_live-sdk-suite.yml"
    if not path.is_file():
        return ["_live-sdk-suite.yml: reusable live SDK benchmark workflow file is missing"]

    text = _read(path)
    scoped: list[str] = []
    for needle, label in BENCHMARK_WORKFLOW_REQUIREMENTS:
        _require(text, needle, label, scoped)

    collect_at = text.find("Collect benchmark JSON")
    upload_at = text.find("Upload benchmark report artifact")
    fail_at = text.find("Fail on benchmark failures")
    if min(collect_at, upload_at, fail_at) < 0:
        scoped.append("missing benchmark collect/upload/fail step ordering anchors")
    elif not (collect_at < upload_at < fail_at):
        scoped.append("benchmark artifact upload must run after collection and before the final failure gate")

    upload_block = text[upload_at:fail_at] if upload_at >= 0 and fail_at > upload_at else ""
    if "if: always()" not in upload_block:
        scoped.append("missing always-run benchmark artifact upload before failure gate")

    fail_block = text[fail_at:] if fail_at >= 0 else ""
    if "if: always()" not in fail_block:
        scoped.append("missing always-run final benchmark failure gate")
    if "cargo build" in text:
        scoped.append("benchmark suite must consume a release binary and not rebuild the broker")
    if "actions/deploy-pages" in text or "pages: write" in text:
        scoped.append("benchmark suite must not deploy Pages")
    return [f"_live-sdk-suite.yml: {failure}" for failure in scoped]


def check_benchmark_orchestrator_gate(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "benchmark-sdks.yml"
    if not path.is_file():
        return ["benchmark-sdks.yml: benchmark orchestrator workflow file is missing"]

    text = _read(path)
    active_text = _non_comment_text(text)
    scoped: list[str] = []
    for needle, label in BENCHMARK_ORCHESTRATOR_REQUIREMENTS:
        _require(text, needle, label, scoped)
    for alternatives, label in BENCHMARK_ORCHESTRATOR_TRIGGER_PATHS:
        if not any(path in text for path in alternatives):
            scoped.append(f"missing {label}")
    if (
        "actions/deploy-pages" in active_text
        or "pages: write" in active_text
        or "concurrency: pages" in active_text
    ):
        scoped.append("benchmark orchestrator must not own Pages deployment")
    if "cargo build" in active_text:
        scoped.append("benchmark orchestrator must not rebuild the broker")
    return [f"benchmark-sdks.yml: {failure}" for failure in scoped]


def check_pages_playground_wasm_gate(root: Path = ROOT) -> list[str]:
    workflow_path = root / ".github" / "workflows" / "pages.yml"
    if not workflow_path.is_file():
        return ["pages.yml: Pages workflow file is missing"]
    smoke_path = root / "scripts" / "playground_wasm_smoke.mjs"
    if not smoke_path.is_file():
        return ["scripts/playground_wasm_smoke.mjs: playground smoke script is missing"]
    playground_html_path = root / "docs" / "site" / "playground.html"
    if not playground_html_path.is_file():
        return ["docs/site/playground.html: playground page is missing"]
    playground_js_path = root / "docs" / "site" / "playground.js"
    if not playground_js_path.is_file():
        return ["docs/site/playground.js: playground script is missing"]
    readme_path = root / "docs" / "site" / "README.md"
    if not readme_path.is_file():
        return ["docs/site/README.md: Pages site README is missing"]

    workflow = _read(workflow_path)
    smoke = _read(smoke_path)
    playground_html = _read(playground_html_path)
    playground_js = _read(playground_js_path)
    readme = _read(readme_path)
    scoped: list[str] = []
    for needle, label in PAGES_PLAYGROUND_REQUIREMENTS:
        _require(workflow, needle, label, scoped)
    for needle, label in PAGES_PLAYGROUND_SCRIPT_REQUIREMENTS:
        _require(smoke, needle, label, scoped)
    for needle, label in PAGES_PLAYGROUND_HTML_REQUIREMENTS:
        _require(playground_html, needle, label, scoped)
    for needle, label in PAGES_PLAYGROUND_JS_REQUIREMENTS:
        _require(playground_js, needle, label, scoped)
    for needle, label in PAGES_SITE_README_REQUIREMENTS:
        _require(readme, needle, label, scoped)

    asset_at = workflow.find("Sync brand assets into the site")
    api_at = workflow.find("Publish Swagger API document")
    bench_at = workflow.find("Pull latest benchmark results into the site")
    build_at = workflow.find("Build UDB's parser to WebAssembly")
    smoke_at = workflow.find("Verify playground parses current editor input")
    contract_at = workflow.find("Verify site artifact contract")
    upload_at = workflow.find("actions/upload-pages-artifact@v3")
    deploy_at = workflow.find("actions/deploy-pages@v4")
    positions = [asset_at, api_at, bench_at, build_at, smoke_at, contract_at, upload_at, deploy_at]
    if any(pos < 0 for pos in positions):
        scoped.append("missing Pages asset/API/benchmark/build/smoke/contract/upload/deploy ordering anchors")
    elif positions != sorted(positions):
        scoped.append(
            "Pages must sync assets/API, pull benchmark JSON, build fresh WASM, "
            "run the current-input smoke, verify artifact contract, upload, then deploy"
        )
    return [f"pages.yml/playground_wasm_smoke: {failure}" for failure in scoped]


def check_pages_single_owner(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    workflow_dir = root / ".github" / "workflows"
    for path in sorted(workflow_dir.glob("*.yml")):
        text = _read(path)
        if "actions/deploy-pages@" in text and path.name != "pages.yml":
            failures.append(f"{path.name}: Pages deploy must stay single-owned by pages.yml")
        if "pages: write" in text and path.name != "pages.yml":
            failures.append(f"{path.name}: pages: write permission is only allowed in pages.yml")
    return failures


def check_lint_workflow_trigger_paths(root: Path = ROOT) -> list[str]:
    path = root / ".github" / "workflows" / "lint-workflows.yml"
    if not path.is_file():
        return ["lint-workflows.yml: workflow lint file is missing"]

    text = _read(path)
    scoped: list[str] = []
    push_at = text.find("  push:")
    pr_at = text.find("  pull_request:")
    dispatch_at = text.find("  workflow_dispatch:")
    if min(push_at, pr_at, dispatch_at) < 0 or not (push_at < pr_at < dispatch_at):
        scoped.append("missing ordered push, pull_request, and workflow_dispatch triggers")
        push_block = ""
        pr_block = ""
    else:
        push_block = text[push_at:pr_at]
        pr_block = text[pr_at:dispatch_at]

    for trigger_path, label in LINT_WORKFLOW_TRIGGER_PATHS:
        needle = f'- "{trigger_path}"'
        if needle not in push_block:
            scoped.append(f"missing {label} trigger path in push: {needle}")
        if needle not in pr_block:
            scoped.append(f"missing {label} trigger path in pull_request: {needle}")

    for command, label in (
        ("node --check scripts/ci-inventory.mjs", "CI inventory syntax check"),
        ("node scripts/ci-inventory.mjs --selftest", "CI inventory selftest"),
        ("node scripts/ci-inventory.mjs", "CI inventory repo scan"),
        ("node --check scripts/check-branch-protection-lockstep.mjs", "branch-protection audit syntax check"),
        ("node scripts/check-branch-protection-lockstep.mjs --selftest", "branch-protection audit selftest"),
        ("node --check scripts/check-ci-runner-evidence.mjs", "CI runner evidence syntax check"),
        ("node scripts/check-ci-runner-evidence.mjs --selftest", "CI runner evidence selftest"),
        ("python3 scripts/error_detail_served_smoke.py --selftest", "ErrorDetail served smoke selftest"),
        ("python3 scripts/idempotency_served_replay_smoke.py --selftest", "idempotency served replay smoke selftest"),
        ("python3 scripts/retry_safe_served_smoke.py --selftest", "retry-safe served smoke selftest"),
        ("python3 scripts/native_load_gate.py --selftest", "native load gate selftest"),
        ("python3 scripts/check-workflow-posture.py --selftest", "workflow posture selftest"),
        ("python3 scripts/check-workflow-posture.py", "workflow posture repo scan"),
    ):
        _require(text, command, label, scoped)
    return [f"lint-workflows.yml: {failure}" for failure in scoped]


def check_lint_workflow_covers_referenced_helpers(root: Path = ROOT) -> list[str]:
    covered = {path for path, _label in LINT_WORKFLOW_TRIGGER_PATHS}
    referenced: set[str] = set()
    for base in (root / ".github" / "workflows", root / ".github" / "actions"):
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for match in WORKFLOW_SCRIPT_REF_RE.finditer(text):
                referenced.add(match.group(0).rstrip("."))

    missing = sorted(referenced - covered - WORKFLOW_SCRIPT_REF_ALLOWLIST)
    if not missing:
        return []
    return [f"lint-workflows.yml: workflow helper ref lacks trigger coverage: {path}" for path in missing]


def run_selftest() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        wf = root / ".github" / "workflows"
        wf.mkdir(parents=True)
        good = """name: demo
on:
  workflow_dispatch:
permissions:
  contents: read
concurrency:
  group: demo-${{ github.ref }}
jobs:
  demo:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - run: docker compose up -d demo
      - if: always()
        uses: actions/upload-artifact@v4
        with:
          retention-days: 14
      - if: always()
        run: docker compose down -v --remove-orphans
"""
        ha_good = """name: Resilience smokes
on:
  workflow_dispatch:
  schedule:
    - cron: "17 3 * * 1"
permissions:
  contents: read
concurrency:
  group: resilience-smokes-${{ github.ref }}
jobs:
  ha-smokes:
    runs-on: ubuntu-latest
    timeout-minutes: 90
    env:
      UDB_HA_PROJECT: udb-ha
      UDB_HA_CDC_PROJECT: udb-ha-cdc
      UDB_HA_XA_PROJECT: udb-ha-xa
    steps:
      - run: docker compose version
      - run: bash scripts/ha_multinode_smoke.sh
      - run: bash scripts/ha_cdc_no_duplicate_smoke.sh
      - run: bash scripts/ha_xa_recovery_smoke.sh
      - if: always()
        uses: actions/upload-artifact@v4
        with:
          name: ha-smoke-logs
          path: ha-smoke-logs
          retention-days: 14
      - if: always()
        run: |
          docker compose -p "$UDB_HA_PROJECT" down -v --remove-orphans
          docker compose -p "$UDB_HA_CDC_PROJECT" down -v --remove-orphans
          docker compose -p "$UDB_HA_XA_PROJECT" down -v --remove-orphans
  cdc-fault-smoke:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    env:
      UDB_CDC_FAULT_PROJECT: udb-cdc-fault
      UDB_CDC_FAULT_KEEP_STACK: "1"
    steps:
      - run: docker compose version
      - run: bash scripts/cdc_fault_smoke.sh
      - if: always()
        uses: actions/upload-artifact@v4
        with:
          name: cdc-fault-smoke-logs
          path: cdc-fault-smoke-logs
          retention-days: 14
      - if: always()
        run: docker compose -p "$UDB_CDC_FAULT_PROJECT" down -v --remove-orphans
"""
        sidecar_good = """name: Native sidecar smokes
on:
  workflow_dispatch:
permissions:
  contents: read
concurrency:
  group: native-sidecar-smokes-${{ github.ref }}
jobs:
  embedding-sidecar:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    env:
      UDB_EMBEDDING_PROJECT: udb-embedding-sidecar-smoke
    steps:
      - run: docker compose version
      - run: python scripts/embedding_sidecar_roundtrip_smoke.py --selftest
      - run: python scripts/embedding_sidecar_smoke.py --selftest
      - run: python scripts/embedding_retrieval_eval.py
      - run: docker compose -p "$UDB_EMBEDDING_PROJECT" -f docker-compose.integration.yml --profile embedding up -d --wait embedding-sidecar
      - run: python scripts/embedding_sidecar_smoke.py --url http://127.0.0.1:58090
      - if: always()
        uses: actions/upload-artifact@v4
        with:
          name: embedding-sidecar-smoke-logs
          path: embedding-sidecar-smoke-logs
          retention-days: 14
      - if: always()
        run: docker compose -p "$UDB_EMBEDDING_PROJECT" -f docker-compose.integration.yml --profile embedding down -v --remove-orphans
  notify-sidecar:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    env:
      UDB_NOTIFY_PROJECT: udb-notify-sidecar-smoke
    steps:
      - run: docker compose version
      - run: python scripts/notify_sidecar_roundtrip_smoke.py --selftest
      - run: python scripts/notify_sidecar_smoke.py --selftest
      - run: docker compose -p "$UDB_NOTIFY_PROJECT" -f docker-compose.integration.yml --profile notify up -d --wait notify-sidecar
      - run: python scripts/notify_sidecar_smoke.py --url http://127.0.0.1:58080
      - if: always()
        uses: actions/upload-artifact@v4
        with:
          name: notify-sidecar-smoke-logs
          path: notify-sidecar-smoke-logs
          retention-days: 14
      - if: always()
        run: docker compose -p "$UDB_NOTIFY_PROJECT" -f docker-compose.integration.yml --profile notify down -v --remove-orphans
"""
        metering_good = """name: Metering rollup smoke
on:
  workflow_dispatch:
permissions:
  contents: read
concurrency:
  group: metering-rollup-smoke-${{ github.ref }}
jobs:
  metering-rollup-smoke:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    env:
      UDB_METERING_PROJECT: udb-metering-smoke
    steps:
      - uses: ./.github/actions/setup-rust
        with:
          cache-key: metering-rollup-smoke
      - run: docker compose -p "$UDB_METERING_PROJECT" -f docker-compose.integration.yml up -d --wait postgres
      - env:
          UDB_INTEGRATION_PG_DSN: postgres://udb:udb@127.0.0.1:55432/udb
          UDB_LIVE_NATIVE_PG_DSN: postgres://udb:udb@127.0.0.1:55432/udb
        run: cargo test --locked --lib live_postgres_metering_rollup_exports_closed_window_once -- --ignored --nocapture --test-threads=1
      - if: always()
        uses: actions/upload-artifact@v4
        with:
          name: metering-smoke-logs
          path: metering-smoke-logs
          retention-days: 14
      - if: always()
        run: docker compose -p "$UDB_METERING_PROJECT" -f docker-compose.integration.yml down -v --remove-orphans
"""
        secrets_good = """name: Secrets posture smoke
on:
  workflow_dispatch:
permissions:
  contents: read
concurrency:
  group: secrets-posture-smoke-${{ github.ref }}
jobs:
  ws-signalling-redaction:
    runs-on: ubuntu-latest
    timeout-minutes: 35
    steps:
      - uses: ./.github/actions/setup-rust
        with:
          cache-key: secrets-posture-smoke
      - run: cargo test --locked --lib --features ws-signalling storage_only_fields_match_generated_redaction_coverage -- --nocapture
      - run: cargo test --locked --lib --features ws-signalling ice_config_debug_redacts_turn_secret -- --nocapture
"""
        webauthn_good = """name: WebAuthn OpenSSL smoke
on:
  workflow_dispatch:
permissions:
  contents: read
concurrency:
  group: webauthn-openssl-smoke-${{ github.ref }}
jobs:
  webauthn-openssl-smoke:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    steps:
      - uses: ./.github/actions/setup-rust
        with:
          cache-key: webauthn-openssl-smoke
      - run: cargo test --locked --lib --features webauthn webauthn_policy_tests -- --nocapture
"""
        clickhouse_good = """name: ClickHouse canonical smoke
on:
  workflow_dispatch:
permissions:
  contents: read
concurrency:
  group: clickhouse-canonical-smoke-${{ github.ref }}
jobs:
  clickhouse-canonical:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    steps:
      - uses: ./.github/actions/setup-rust
        with:
          cache-key: clickhouse-canonical-smoke
      - run: docker compose -f docker-compose.canonical.yml up -d --wait clickhouse
      - env:
          UDB_COLUMN_DSN: http://127.0.0.1:58123/udb
          UDB_CLICKHOUSE_DSN: http://127.0.0.1:58123/udb
        run: cargo test --locked --lib --features clickhouse clickhouse_canonical_store_satisfies_all_contracts_live -- --nocapture
      - if: always()
        uses: actions/upload-artifact@v4
        with:
          name: clickhouse-canonical-logs
          path: clickhouse-canonical-logs
          retention-days: 14
      - if: always()
        run: docker compose -f docker-compose.canonical.yml down -v --remove-orphans
"""
        ffmpeg_good = """name: Vendored ffmpeg transcode smoke
on:
  workflow_dispatch:
permissions:
  contents: read
concurrency:
  group: ffmpeg-transcode-smoke-${{ github.ref }}
jobs:
  ffmpeg-transcode-smoke:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - run: python scripts/check-vendored-ffmpeg.py --selftest
      - run: sudo apt-get install -y --no-install-recommends ffmpeg
      - run: python scripts/ffmpeg_transcode_smoke.py --ffmpeg-bin "$(command -v ffmpeg)" --artifact-dir ffmpeg-transcode-smoke
      - if: always()
        uses: actions/upload-artifact@v4
        with:
          name: ffmpeg-transcode-smoke
          path: ffmpeg-transcode-smoke
          retention-days: 14
"""
        pg_merge_good = """name: Postgres planner/IR merge smoke
on:
  workflow_dispatch:
permissions:
  contents: read
concurrency:
  group: pg-merge-smoke-${{ github.ref }}
jobs:
  pg-merge-smoke:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    env:
      UDB_PG_MERGE_PROJECT: udb-pg-merge-smoke
    steps:
      - uses: ./.github/actions/setup-rust
        with:
          cache-key: pg-merge-smoke
      - run: docker compose -p "$UDB_PG_MERGE_PROJECT" -f docker-compose.integration.yml up -d --wait postgres
      - env:
          UDB_IR_LIVE_GOLDEN_TESTS: "1"
          UDB_PG_DSN: postgres://udb:udb@127.0.0.1:55432/udb
          DATABASE_URL: postgres://udb:udb@127.0.0.1:55432/udb
        run: cargo test --locked --lib postgres_data_plane_planner_and_bridged_ir_match_live_rows -- --ignored --nocapture --test-threads=1
      - if: always()
        uses: actions/upload-artifact@v4
        with:
          name: pg-merge-smoke-logs
          path: pg-merge-smoke-logs
          retention-days: 14
      - if: always()
        run: docker compose -p "$UDB_PG_MERGE_PROJECT" -f docker-compose.integration.yml down -v --remove-orphans
"""
        rest_gateway_good = """name: REST gateway boundary smoke
on:
  workflow_dispatch:
    inputs:
      base_url:
        required: true
      success_route:
        required: true
      error_route:
        required: true
      error_code:
        required: true
      header:
        required: false
      timeout_seconds:
        required: false
permissions:
  contents: read
concurrency:
  group: rest-gateway-smoke-${{ github.ref }}
jobs:
  rest-gateway-boundary:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - run: |
          python3 scripts/rest_route_gateway_smoke.py --selftest
          python3 scripts/rest_route_gateway_smoke.py --check-openapi
      - env:
          BASE_URL: ${{ inputs.base_url }}
          SUCCESS_ROUTE: ${{ inputs.success_route }}
          ERROR_ROUTE: ${{ inputs.error_route }}
          ERROR_CODE: ${{ inputs.error_code }}
          HEADER: ${{ inputs.header }}
          TIMEOUT_SECONDS: ${{ inputs.timeout_seconds }}
        run: |
          mkdir -p rest-gateway-evidence
          args=(
            --base-url "$BASE_URL"
            --require-route-family-proof
            --require-boundary-proof
            --boundary-success "$SUCCESS_ROUTE"
            --boundary-error "$ERROR_ROUTE"
            --boundary-error-code "$ERROR_CODE"
            --timeout "$TIMEOUT_SECONDS"
            --evidence-out rest-gateway-evidence/evidence.json
          )
          if [ -n "$HEADER" ]; then
            args+=(--header "$HEADER")
          fi
          python3 scripts/rest_route_gateway_smoke.py "${args[@]}"
      - uses: actions/upload-artifact@v4
        with:
          name: rest-gateway-evidence
          path: rest-gateway-evidence/evidence.json
          if-no-files-found: ignore
          retention-days: 14
"""
        sfu_good = """name: LiveKit SFU smoke
on:
  workflow_dispatch:
permissions:
  contents: read
concurrency:
  group: livekit-sfu-smoke-${{ github.ref }}
jobs:
  livekit-sfu-smoke:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    env:
      UDB_SFU_PROJECT: udb-sfu-smoke
      UDB_SFU_OPERATOR_USERNAME: sfu_smoke
      UDB_SFU_OPERATOR_PASSWORD: CorrectHorse1!
      UDB_SFU_OPERATOR_TENANT: sfu-smoke
      UDB_SFU_OPERATOR_PROJECT: default
    steps:
      - uses: ./.github/actions/setup-rust
        with:
          cache-key: livekit-sfu-smoke
      - run: |
          cargo test --locked --lib --features webrtc livekit_join_token_binds_tenant_room_and_peer -- --nocapture
          cargo test --locked --lib --features webrtc plaintext_livekit_url_requires_explicit_local_opt_in -- --nocapture
          cargo test --locked --lib --features webrtc livekit_room_service_base_derives_http_endpoint -- --nocapture
          cargo test --locked --lib --features webrtc sfu_join_metadata_uses_public_header_contract -- --nocapture
          cargo test --locked --lib --features webrtc signal_offer_uses_injected_sfu_bridge -- --nocapture
      - run: python -m pip install -e "sdk/python"
      - run: python scripts/livekit_sfu_smoke.py --selftest
      - run: docker compose -p "$UDB_SFU_PROJECT" -f docker-compose.integration.yml --profile sfu up -d --wait postgres redis qdrant minio kafka livekit coturn udb-livekit
      - name: Bootstrap LiveKit SFU operator
        run: |
          docker compose -p "$UDB_SFU_PROJECT" -f docker-compose.integration.yml --profile sfu exec -T udb-livekit udb auth bootstrap user --username "$UDB_SFU_OPERATOR_USERNAME" --email "sfu-smoke@example.com" --password "$UDB_SFU_OPERATOR_PASSWORD" --tenant "$UDB_SFU_OPERATOR_TENANT" --project "$UDB_SFU_OPERATOR_PROJECT"
      - run: python scripts/livekit_sfu_smoke.py --broker 127.0.0.1:50082 --auth-broker 127.0.0.1:50081 --username "$UDB_SFU_OPERATOR_USERNAME" --password "$UDB_SFU_OPERATOR_PASSWORD" --livekit-http http://127.0.0.1:57880 --livekit-url ws://livekit:7880 --api-key devkey --api-secret secret
      - if: always()
        uses: actions/upload-artifact@v4
        with:
          name: livekit-sfu-smoke-logs
          path: livekit-sfu-smoke-logs
          retention-days: 14
      - if: always()
        run: docker compose -p "$UDB_SFU_PROJECT" -f docker-compose.integration.yml --profile sfu down -v --remove-orphans
"""
        release_binaries_good = """name: Release · binaries
on:
  workflow_call:
  workflow_dispatch:
env:
  PORTABLE_FEATURES: >-
    postgres,mysql,sqlite,qdrant,s3,mongodb-native,neo4j,clickhouse,redis,elasticsearch,weaviate,pinecone,azureblob,gcs,otel,runtime-logging,http-client,oidc,webauthn
  FULL_FEATURES: >-
    oidc,webauthn
permissions:
  contents: read
concurrency:
  group: release-binaries-${{ github.ref }}
  cancel-in-progress: true
jobs:
  version-guard:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/version-guard
        with:
          component: udb
  vendored-ffmpeg:
    needs: version-guard
    runs-on: ubuntu-latest
    steps:
      - run: python scripts/check-vendored-ffmpeg.py --selftest
      - run: sudo apt-get install -y --no-install-recommends ffmpeg
      - run: python scripts/ffmpeg_transcode_smoke.py --ffmpeg-bin "$(command -v ffmpeg)" --artifact-dir ffmpeg-transcode-smoke
      - uses: actions/upload-artifact@v4
        with:
          name: ffmpeg-transcode-smoke
  build:
    needs: vendored-ffmpeg
    runs-on: ${{ matrix.os }}
    strategy:
      fail-fast: true
      matrix:
        include:
          - os: ubuntu-22.04
            target: x86_64-unknown-linux-gnu
            asset: udb-linux-amd64
            ext: ''
            target_cpu: x86-64-v2
            variant: portable
          - os: windows-latest
            target: x86_64-pc-windows-msvc
            asset: udb-windows-amd64.exe
            ext: '.exe'
            target_cpu: x86-64-v2
            variant: portable
          - os: macos-14
            target: aarch64-apple-darwin
            asset: udb-darwin-arm64
            ext: ''
            target_cpu: apple-m1
            variant: portable
          - os: macos-15-intel
            target: x86_64-apple-darwin
            asset: udb-darwin-amd64
            ext: ''
            target_cpu: x86-64-v2
            variant: portable
          - os: ubuntu-22.04
            target: x86_64-unknown-linux-gnu
            asset: udb-linux-amd64-full
            ext: ''
            target_cpu: x86-64-v2
            variant: full
    steps:
      - uses: ./.github/actions/setup-rust
        with:
          target: ${{ matrix.target }}
          cache-key: ${{ matrix.target }}
      - name: Build
        run: |
          export RUSTFLAGS="-C target-cpu=${MATRIX_TARGET_CPU} ${RUSTFLAGS:-}"
          cargo build --profile dist --locked --target "${MATRIX_TARGET}" --bin udb --features "${FULL_FEATURES}"
          cargo build --profile dist --locked --target "${MATRIX_TARGET}" --bin udb --no-default-features --features "${PORTABLE_FEATURES}"
      - name: Stage asset + checksum
        run: |
          cp "target/${MATRIX_TARGET}/dist/udb${MATRIX_EXT}" "dist/${MATRIX_ASSET}"
          sha256sum "${MATRIX_ASSET}" > "${MATRIX_ASSET}.sha256"
          shasum -a 256 "${MATRIX_ASSET}" > "${MATRIX_ASSET}.sha256"
      - name: Upload workflow artifact
        uses: actions/upload-artifact@v4
        with:
          name: ${{ matrix.asset }}
          path: |
            dist/${{ matrix.asset }}
            dist/${{ matrix.asset }}.sha256
      - name: Guard tag still points at this commit
        run: |
          gh api "repos/${GITHUB_REPOSITORY}/git/ref/tags/${GITHUB_REF_NAME}"
          if [ "${ref_sha}" != "${GITHUB_SHA}" ]; then
            echo "refusing to publish stale binary asset"
          fi
      - name: Attach to GitHub Release
        if: startsWith(github.ref, 'refs/tags/')
        uses: softprops/action-gh-release@v2
        with:
          files: dist/${{ matrix.asset }}
          fail_on_unmatched_files: true
  manifest:
    needs: build
    if: startsWith(github.ref, 'refs/tags/')
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - run: node scripts/gen-release-manifest.mjs --selftest
      - name: Guard tag still points at this commit
        run: echo "refusing to publish stale release manifest"
      - name: Download the published binaries + checksums
        run: gh release download "${GITHUB_REF_NAME}" --dir dist --pattern 'udb-*'
      - run: node scripts/gen-release-manifest.mjs dist > dist/manifest.json
      - run: sha256sum manifest.json > manifest.json.sha256
      - name: Attach manifest to GitHub Release
        uses: softprops/action-gh-release@v2
        with:
          files: |
            dist/manifest.json
            dist/manifest.json.sha256
          fail_on_unmatched_files: true
"""
        release_docker_good = """name: Release · Docker image
on:
  workflow_call:
jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - name: Download release binary into build context
        run: |
          gh release download "${tag}" --pattern 'udb-linux-amd64-full' --output udb
          chmod +x udb
      - name: Build and push
        uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile.release
          platforms: linux/amd64
"""
        release_manifest_generator_good = """const NAME_RE = /^udb-(linux|darwin|windows)-(amd64|arm64)(?:-([a-z0-9]+))?(\\.exe)?$/;
function readExpectedSha256(dir, name) {
  throw new Error(`missing .sha256 sidecar for ${name}`);
  throw new Error(`invalid .sha256 sidecar for ${name}: <empty>`);
  throw new Error(`sha256 mismatch for ${name}: sidecar=0 actual=1`);
}
function assetFromName(dir, name) {
  if (!name.match(NAME_RE)) {
    throw new Error(`unrecognized release asset name: ${name}`);
  }
  const tier = "";
  return {
    name,
    tier: tier || "portable",
    sha256: readExpectedSha256(dir, name),
    size: fs.statSync(path.join(dir, name)).size,
  };
}
export function generateManifest(dir, version) {
  const assets = [];
  return {
    version,
    tag: `v${version}`,
    scheme: "udb-<os>-<arch>[-<tier>][.exe]",
    base_url: `https://github.com/fahara02/udb/releases/download/v${version}`,
    assets,
  };
}
function runSelftest() {
  writeAsset(root, "udb-linux-amd64-full", "full-linux");
  assert(manifest.assets.length === 3, "selftest asset count mismatch");
  throw new Error("selftest failed to reject missing checksum sidecar");
  throw new Error("selftest failed to reject stale checksum sidecar");
  throw new Error("selftest failed to reject unrecognized release asset name");
}
"""
        release_topology_good = """name: Release
on:
  push:
    tags:
      - 'v*.*.*'
permissions:
  contents: read
  actions: read
concurrency:
  group: release-${{ github.ref }}
  cancel-in-progress: false
jobs:
  ci-green:
    runs-on: ubuntu-latest
    steps:
      - run: |
          gh run list --workflow ci.yml --commit "${GITHUB_SHA}"
  version-guard:
    needs: ci-green
    runs-on: ubuntu-latest
  build-binaries:
    needs: version-guard
    uses: ./.github/workflows/release-binaries.yml
    secrets: inherit
  publish-crates:
    needs: build-binaries
    uses: ./.github/workflows/release-crates.yml
    secrets: inherit
  publish-docker:
    needs: build-binaries
    uses: ./.github/workflows/release-docker.yml
    secrets: inherit
  publish-ts:
    needs: build-binaries
    uses: ./.github/workflows/release-typescript-sdk.yml
    secrets: inherit
  publish-py:
    needs: build-binaries
    uses: ./.github/workflows/release-python-sdk.yml
    secrets: inherit
  publish-csharp:
    needs: build-binaries
    uses: ./.github/workflows/release-csharp-sdk.yml
    secrets: inherit
  publish-packagist:
    needs: build-binaries
    uses: ./.github/workflows/release-packagist.yml
    secrets: inherit
"""
        release_leaf_good = """name: Release leaf
on:
  workflow_call:
jobs:
  publish:
    runs-on: ubuntu-latest
"""
        release_crates_publisher_good = """name: Release crates
on:
  workflow_call:
jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/setup-rust
      - uses: ./.github/actions/setup-sdk-toolchains
        with:
          node: 'true'
      - uses: ./.github/actions/version-guard
        with:
          component: udb
      - name: Check crates.io version availability
        id: crate_version
        run: curl -fsS "https://crates.io/api/v1/crates/udb/${version}"
      - name: cargo publish --dry-run
        if: steps.crate_version.outputs.exists != 'true'
        run: cargo publish --dry-run
      - name: cargo publish
        if: steps.crate_version.outputs.exists != 'true'
        env:
          CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}
        run: |
          cargo publish 2>&1 | tee /tmp/cargo-publish.log
          status=${PIPESTATUS[0]}
          grep -Eq 'already exists on crates.io' /tmp/cargo-publish.log
"""
        release_typescript_publisher_good = """name: Release TypeScript SDK
on:
  workflow_call:
jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/setup-sdk-toolchains
        with:
          node: 'true'
          node-registry-url: "https://registry.npmjs.org"
      - uses: ./.github/actions/version-guard
        with:
          component: sdk-typescript
      - run: npm install --no-audit --no-fund
      - name: Check npm version availability
        id: npm_version
        run: npm view "@udb_plus/sdk@${version}" version --silent
      - name: Build package
        run: npm run build
      - name: Publish dry run
        if: steps.npm_version.outputs.exists != 'true'
        run: npm publish --dry-run --ignore-scripts --access public
      - name: Publish to npm
        if: steps.npm_version.outputs.exists != 'true'
        env:
          NODE_AUTH_TOKEN: ${{ secrets.NPM_TOKEN }}
        run: npm publish --ignore-scripts --access public
"""
        release_python_publisher_good = """name: Release Python SDK
on:
  workflow_call:
jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/setup-sdk-toolchains
        with:
          python: 'true'
          node: 'true'
      - uses: astral-sh/setup-uv@v6
      - uses: ./.github/actions/version-guard
        with:
          component: sdk-python
      - run: uv sync --extra dev
      - run: uv run python -m build
      - run: uv run twine check dist/*
      - env:
          TWINE_USERNAME: __token__
          TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
        run: uv run twine upload --skip-existing dist/*
"""
        release_csharp_publisher_good = """name: Release CSharp SDK
on:
  workflow_call:
jobs:
  publish:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write
    steps:
      - uses: ./.github/actions/setup-sdk-toolchains
        with:
          dotnet: 'true'
          node: 'true'
      - uses: ./.github/actions/version-guard
        with:
          component: sdk-csharp
      - run: |
          dotnet restore sdk/csharp/Udb.Client.Tests/Udb.Client.Tests.csproj
          dotnet restore sdk/csharp/Udb.Cli/Udb.Cli.csproj
          dotnet build sdk/csharp/Udb.Client.Tests/Udb.Client.Tests.csproj --configuration Release --no-restore
          dotnet build sdk/csharp/Udb.Cli/Udb.Cli.csproj --configuration Release --no-restore
      - id: nuget_client
        run: curl -fsS "https://api.nuget.org/v3-flatcontainer/udb.client/${version}/udb.client.nuspec"
      - id: nuget_cli
        run: curl -fsS "https://api.nuget.org/v3-flatcontainer/udb.cli/${version}/udb.cli.nuspec"
      - run: dotnet pack --configuration Release --no-build --output ./nupkg
      - uses: NuGet/login@v1
      - run: |
          dotnet nuget push ./nupkg/*.nupkg \
            --api-key "${NUGET_API_KEY}" \
            --source https://api.nuget.org/v3/index.json \
            --skip-duplicate
"""
        release_packagist_publisher_good = """name: Release Packagist
on:
  workflow_call:
env:
  SATELLITE_REPO: git@github.com:fahara02/udb-laravel.git
jobs:
  validate-php-sdk:
    runs-on: ubuntu-latest
    steps:
      - run: composer validate --strict --no-check-publish
      - run: composer install --no-interaction --no-progress --prefer-dist
      - name: Verify generated stubs are committed
        run: test -f gen/Udb/Services/V1/DataBrokerClient.php
      - uses: ./.github/actions/version-guard
        with:
          component: sdk-php
  push-satellite:
    needs: validate-php-sdk
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: webfactory/ssh-agent@v0.9.0
      - run: git subtree split --prefix=sdk/php -b sdk-php-split
      - run: git push --force "$SATELLITE_REPO" sdk-php-split:main
      - run: |
          git tag -f "${TAG_NAME}" sdk-php-split
          git push --force "$SATELLITE_REPO" "refs/tags/${TAG_NAME}"
  notify-packagist:
    needs: push-satellite
    steps:
      - run: |
          echo "Packagist credentials not configured"
          exit 0
          curl -X POST "https://packagist.org/api/update-package"
"""
        cleanup_packages_good = """name: Cleanup · Stale packages
on:
  workflow_run:
    workflows: ["Release"]
    types: [completed]
  schedule:
    - cron: "0 2 * * 0"
  workflow_dispatch:
    inputs:
      keep_sha_tags:
        default: "5"
      dry_run:
        type: boolean
        default: false
permissions:
  packages: write
jobs:
  cleanup-docker:
    runs-on: ubuntu-latest
    if: >
      github.event_name == 'workflow_dispatch' ||
      github.event_name == 'schedule'         ||
      github.event.workflow_run.conclusion == 'success'
    steps:
      - name: Delete untagged GHCR versions (udb)
        uses: actions/delete-package-versions@v5
        with:
          package-name: udb
          package-type: container
          min-versions-to-keep: 0
          delete-only-untagged-versions: 'true'
      - name: Prune old sha-* tags (udb) -- keep newest ${{ github.event.inputs.keep_sha_tags || '5' }}
        uses: actions/delete-package-versions@v5
        with:
          package-name: udb
          package-type: container
          min-versions-to-keep: ${{ github.event.inputs.keep_sha_tags || '5' }}
          ignore-versions: '^(v?\\d+\\.\\d+(\\.\\d+)?(-[a-zA-Z0-9._]+)?|latest|\\d+\\.\\d+|\\d+)$'
      - name: Dry run -- list package versions (no deletion)
        run: gh api "/users/fahara02/packages/container/udb/versions?per_page=100"
"""
        release_dockerfile_good = """FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.37
COPY udb /usr/local/bin/udb
COPY proto ./proto
COPY third_party ./third_party
COPY configs ./configs
ENV UDB_FFMPEG_BIN=/usr/bin/ffmpeg
USER udb:udb
ENTRYPOINT ["/usr/local/bin/udb"]
"""
        ci_good = """name: CI
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
concurrency:
  group: ci-${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true
permissions:
  contents: read
env:
  LIVE_BROKER_FEATURES: postgres,mongodb-native,s3,kafka,qdrant,runtime-logging
jobs:
  quick-gate:
    runs-on: ubuntu-latest
    steps:
      - name: SDK service-coverage guard
        run: |
          python3 scripts/check-sdk-service-coverage.py --selftest
          python3 scripts/check-sdk-service-coverage.py
      - name: Vector canonical CAS posture guard
        run: |
          python3 scripts/check-vector-cas-posture.py --selftest
          python3 scripts/check-vector-cas-posture.py
      - name: ORM template posture guard
        run: |
          python3 scripts/check-orm-template-posture.py --selftest
          python3 scripts/check-orm-template-posture.py
      - name: Workflow service posture guard
        run: |
          python3 scripts/check-workflow-service-posture.py --selftest
          python3 scripts/check-workflow-service-posture.py
      - name: IR live-golden posture guard
        run: |
          python3 scripts/check-ir-live-golden-posture.py --selftest
          python3 scripts/check-ir-live-golden-posture.py
      - name: Scaffold posture guard
        run: |
          python3 scripts/check-scaffold-posture.py --selftest
          python3 scripts/check-scaffold-posture.py
      - name: SDK helper parity guard
        run: |
          python3 scripts/check-sdk-helper-parity.py --selftest
          python3 scripts/check-sdk-helper-parity.py
      - name: Todo-board status guard
        run: |
          python3 scripts/check-todo-board-status.py --selftest
          python3 scripts/check-todo-board-status.py
      - name: Gap-closure posture guard
        run: |
          python3 scripts/check-gap-closure-posture.py --selftest
          python3 scripts/check-gap-closure-posture.py
      - name: Bench harness posture guard
        run: |
          node --check scripts/gen-bench-bodies-skeleton.mjs
          node scripts/gen-bench-bodies-skeleton.mjs --selftest
          node scripts/gen-bench-bodies-skeleton.mjs --check
          python3 scripts/check-bench-harness-posture.py --selftest
          python3 scripts/check-bench-harness-posture.py
      - name: Docs/CI freshness posture guard
        run: |
          python3 scripts/check-docs-ci-freshness-posture.py --selftest
          python3 scripts/check-docs-ci-freshness-posture.py
      - name: Go SDK posture guard
        run: |
          python3 scripts/check-go-sdk-posture.py --selftest
          python3 scripts/check-go-sdk-posture.py
      - name: TypeScript SDK posture guard
        run: |
          python3 scripts/check-ts-sdk-posture.py --selftest
          python3 scripts/check-ts-sdk-posture.py
      - name: Python/PHP SDK posture guard
        run: |
          python3 scripts/check-python-php-sdk-posture.py --selftest
          python3 scripts/check-python-php-sdk-posture.py
      - name: Java/C# SDK audit guard
        run: |
          python3 scripts/check-java-csharp-sdk-audit.py --selftest
          python3 scripts/check-java-csharp-sdk-audit.py
      - name: API/SDK alias posture guard
        run: |
          python3 scripts/check-api-sdk-alias-posture.py --selftest
          python3 scripts/check-api-sdk-alias-posture.py
      - name: OpenAPI operation-id posture guard
        run: |
          python3 scripts/check-openapi-operationid-posture.py --selftest
          python3 scripts/check-openapi-operationid-posture.py
      - name: Idempotency dedup posture guard
        run: |
          python3 scripts/check-idempotency-dedup-posture.py --selftest
          python3 scripts/check-idempotency-dedup-posture.py
      - name: Retry-safe mutation posture guard
        run: |
          python3 scripts/check-retry-safe-posture.py --selftest
          python3 scripts/check-retry-safe-posture.py
      - name: Error-detail posture guard
        run: |
          python3 scripts/check-error-detail-posture.py --selftest
          python3 scripts/check-error-detail-posture.py
      - name: HTTP API route-style guard
        run: |
          node --check scripts/check-http-api-style.mjs
          node scripts/check-http-api-style.mjs --selftest
          node scripts/check-http-api-style.mjs --source-only
          node scripts/check-http-api-style.mjs --write-report
          git diff --quiet -- docs/generated/http-api-style-exceptions.json docs/generated/http-api-style-exceptions.md
          node scripts/check-http-api-style.mjs --advisory
          node scripts/check-http-api-style.mjs --resource-identity-contract
          node scripts/check-http-api-style.mjs --pagination-contract
          node scripts/check-http-api-style.mjs --query-update-contract
          python3 scripts/rest_route_gateway_smoke.py --selftest
          python3 scripts/rest_route_gateway_smoke.py --check-openapi
      - name: Beta versioning posture guard
        run: |
          python3 scripts/check-beta-versioning-posture.py --selftest
          python3 scripts/check-beta-versioning-posture.py
  clippy-advisory:
    runs-on: ubuntu-latest
    steps:
      - run: cargo clippy --locked --all-targets
  versions:
    runs-on: ubuntu-latest
    steps:
      - name: Launcher asset-name conformance
        run: |
          node scripts/check-launcher-assets.mjs --selftest
          node scripts/check-launcher-assets.mjs
  rust:
    runs-on: ubuntu-latest
    steps:
      - name: Native contract manifest drift + lint (F13 hard gate)
        if: runner.os == 'Linux'
        run: |
          cargo run --locked -q --bin udb -- native manifest > docs/generated/udb-native-contract.json
          if ! git diff --quiet -- docs/generated/udb-native-contract.json; then
            exit 1
          fi
          cargo run --locked -q --bin udb -- native lint
      - name: Native docs markdown drift
        if: runner.os == 'Linux'
        run: |
          cargo run --locked -q --bin udb -- native docs > docs/generated/native-services.md
          if ! git diff --quiet -- docs/generated/native-services.md; then
            exit 1
          fi
      - name: Doc service-count drift guard
        if: runner.os == 'Linux'
        run: |
          python3 scripts/check-doc-service-counts.py --selftest
          python3 scripts/check-doc-service-counts.py
      - name: No internal tables guard (masterplan §12)
        if: runner.os == 'Linux'
        run: |
          python3 scripts/check-no-internal-tables.py --selftest
          python3 scripts/check-no-internal-tables.py
      - name: Codebase map freshness gate
        if: runner.os == 'Linux'
        run: python3 scripts/generate-codebase-map.py --check
      - name: Native contract breaking-change gate (Phase 3)
        if: runner.os == 'Linux'
        run: |
          cargo run --locked -q --bin udb -- native contract-diff \
            --baseline docs/generated/contract-baseline.bin
  build-broker:
    needs: quick-gate
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/setup-rust
        with:
          cache-key: build-broker-live
      - name: Build udb (live tier)
        run: cargo build --locked --bin udb --no-default-features --features "${LIVE_BROKER_FEATURES}"
      - name: Stage binary for upload
        run: |
          mkdir -p artifact
          cp target/debug/udb artifact/udb
      - name: Upload broker binary
        uses: actions/upload-artifact@v4
        with:
          name: udb-broker-debug
          path: artifact/udb
          if-no-files-found: error
  smoke:
    needs: build-broker
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16-alpine
    env:
      UDB_STARTUP_DRY_RUN: "true"
    steps:
      - name: Download broker binary
        uses: actions/download-artifact@v4
        with:
          name: udb-broker-debug
          path: target/debug
      - name: Launch broker
        uses: ./.github/actions/launch-broker
        with:
          grpc-addr: 127.0.0.1:50051
      - name: Verify reflection surface
        run: |
          grep -q '^udb.services.v1.DataBroker$' /tmp/grpcurl-list.txt
          grep -q 'rpc GetHealthReport' /tmp/grpcurl-describe.txt
          grep -q 'rpc LookupMessageSchema' /tmp/grpcurl-describe.txt
      - name: Run native load smoke + p99 regression gate
        run: |
          bash scripts/native-load-test.sh | tee /tmp/native-load.txt
          load_status=${PIPESTATUS[0]}
          python scripts/native_load_gate.py \
            --input /tmp/native-load.txt \
            --baseline scripts/native_load_smoke_baseline.json \
            --max-regression 15
      - name: Upload load summary
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: native-load-smoke
          path: /tmp/native-load.txt
      - name: Stop broker
        if: always()
        run: kill "${UDB_BROKER_PID}" 2>/dev/null || true
  auth-release-binary:
    if: github.event_name == 'push'
    runs-on: ubuntu-latest
    steps:
      - run: cargo check --locked
  slim-build:
    runs-on: ubuntu-latest
    steps:
      - run: cargo build --locked --all-targets --no-default-features --features postgres
  feature-check:
    if: github.event_name == 'pull_request'
    needs: quick-gate
    runs-on: ubuntu-latest
    steps:
      - run: cargo test --locked --all-features --lib
  plugin-feature-matrix:
    if: github.event_name == 'push'
    needs: quick-gate
    runs-on: ubuntu-latest
    steps:
      - run: cargo build --locked --all-targets --no-default-features --features postgres,qdrant
  optimized:
    if: github.event_name == 'push'
    needs: quick-gate
    runs-on: ubuntu-latest
    steps:
      - run: cargo test --locked --lib --features simd-codecs,simd-json,simd-checksum runtime::accel
  aarch64-scalar:
    if: github.event_name == 'push'
    needs: quick-gate
    runs-on: ubuntu-latest
    steps:
      - run: cargo check --locked --target aarch64-unknown-linux-gnu --no-default-features --features postgres
  supply-chain:
    runs-on: ubuntu-latest
    steps:
      - run: cargo deny check bans licenses sources
  buf:
    name: Proto (buf)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: bufbuild/buf-setup-action@v1
        with:
          version: 1.65.0
      - run: buf build
      - name: Verify committed stubs are current
        run: |
          for attempt in 1 2 3; do
            if buf generate --include-imports; then
              break
            fi
            echo "::warning::buf generate failed, retrying remote plugin generation (attempt $attempt/3)"
          done
          node scripts/openapi-postprocess.mjs
          node --check scripts/check-openapi-api-rules.mjs
          node scripts/check-openapi-api-rules.mjs --selftest
          node scripts/check-openapi-api-rules.mjs
          node scripts/sdk-codegen-postprocess.mjs
          git diff --quiet -- sdk/php/gen sdk/go/gen sdk/typescript/gen sdk/python/gen sdk/java/gen sdk/csharp/gen api
          git diff -- sdk/php/gen sdk/go/gen sdk/typescript/gen sdk/python/gen sdk/java/gen sdk/csharp/gen api
      - name: Authn/Authz inventory drift (Phase 0A)
        run: |
          node scripts/generate-authn-authz-inventory.mjs
          git diff --quiet -- docs/generated/authn-authz-rpc-inventory.md docs/generated/authn-authz-sensitive-fields.md
  php-sdk:
    runs-on: ubuntu-latest
    steps:
      - run: composer analyse
  go-sdk:
    runs-on: ubuntu-latest
    steps:
      - run: go vet ./...
  ts-sdk:
    runs-on: ubuntu-latest
    steps:
      - run: npm run build
  python-sdk:
    runs-on: ubuntu-latest
    steps:
      - run: python -m pytest
  csharp-sdk:
    runs-on: ubuntu-latest
    steps:
      - run: dotnet build
  java-sdk:
    runs-on: ubuntu-latest
    steps:
      - run: mvn -B -ntp compile
  sdk-conformance:
    runs-on: ubuntu-latest
    steps:
      - run: node sdk-conformance/run.mjs metadata error-details typescript python go csharp java php
  scaffold-compiles:
    needs: build-broker
    runs-on: ubuntu-latest
    steps:
      - name: Download broker binary
        uses: actions/download-artifact@v4
        with:
          name: udb-broker-debug
      - run: UDB_BIN=target/debug/udb bash scripts/check-scaffold-compiles.sh
  docs-links:
    name: Markdown local links + readiness artifacts
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - uses: ./.github/actions/setup-sdk-toolchains
        with:
          node: "true"
      - run: node --check scripts/check-markdown-links.mjs
      - run: node scripts/check-markdown-links.mjs --selftest
      - run: node scripts/check-markdown-links.mjs
      - run: node --check scripts/check-enterprise-readiness.mjs
      - run: node scripts/check-enterprise-readiness.mjs --selftest
      - run: node scripts/check-enterprise-readiness.mjs
  native-integration:
    if: github.event_name == 'push'
    needs: quick-gate
    runs-on: ubuntu-latest
    timeout-minutes: 75
    steps:
      - name: Reclaim runner disk for live backend stack
        run: |
          df -h
          docker system prune -af --volumes || true
          df -h
      - uses: ./.github/actions/setup-rust
        with:
          cache-key: native-integration
      - name: Start integration stack while compiling tests
        run: |
          docker compose -f docker-compose.integration.yml up -d --wait postgres kafka redis memcached qdrant minio &
          integration_stack_pid=$!
          # Compile native + integration tests while Docker services become healthy.
          cargo test --locked --no-run --lib --test integration_tests --test runtime_live_backends
          wait "$integration_stack_pid"
          echo "::error::native/integration compile preflight failed"
      - name: Start canonical-store stack
        run: docker compose -f docker-compose.canonical.yml up -d --wait mysql mssql mongodb cassandra neo4j clickhouse elasticsearch weaviate
      - name: Initialize SQL Server database
        run: IF DB_ID(N'udb') IS NULL CREATE DATABASE [udb];
      - name: Wait for Weaviate readiness
        run: curl -fsS http://127.0.0.1:58080/v1/.well-known/ready
      - name: Initialize MongoDB replica set
        run: rs.initiate
      - name: Create native-event Kafka topics
        run: |
          for topic in udb.authn.user.registered.v1 udb.notification.sent.v1; do
            kafka-topics.sh --create --if-not-exists
          done
      - name: Create MinIO storage bucket
        run: mc mb --ignore-existing local/udb-storage
      - name: IR compiler live golden tests
        run: cargo test --locked --lib ir::compile::live_tests -- --ignored --nocapture --test-threads=1
      - name: Native service live tests
        env:
          UDB_LIVE_AUTH_TESTS: "1"
        run: cargo test --locked --lib -- --ignored --nocapture --test-threads=1
      - name: Canonical store live conformance
        env:
          UDB_MYSQL_DSN: x
          UDB_MSSQL_DSN: x
          UDB_MONGODB_DSN: x
          UDB_CASSANDRA_DSN: x
          UDB_NEO4J_DSN: x
          UDB_CLICKHOUSE_DSN: x
          UDB_ELASTIC_DSN: x
        run: cargo test --locked --lib canonical_store::conformance_live_tests -- --nocapture
      - name: Integration harness (CDC, sagas, backends)
        env:
          UDB_INTEGRATION_TESTS: "1"
        run: cargo test --locked --test integration_tests --test runtime_live_backends -- --ignored --nocapture
      - name: Dump stack logs on failure
        run: |
          docker compose -f docker-compose.integration.yml logs --no-color --tail=200
          docker compose -f docker-compose.canonical.yml logs --no-color --tail=200
      - name: Stop integration stacks
        if: always()
        run: |
          docker compose -f docker-compose.integration.yml down -v --remove-orphans
          docker compose -f docker-compose.canonical.yml down -v --remove-orphans
"""
        live_sdk_suite_good = """name: _live-sdk-suite
on:
  workflow_call:
    inputs:
      release-tag:
        default: latest
      release-asset:
        default: udb-linux-amd64-full
jobs:
  live-suite:
    runs-on: ubuntu-latest
    steps:
      - name: Resolve release binary (perf)
        run: |
          tag="${RELEASE_TAG}"
          gh release view "${tag}" --repo "${GITHUB_REPOSITORY}" >/dev/null
          gh release download "${tag}" --repo "${GITHUB_REPOSITORY}" --pattern "${RELEASE_ASSET}" --dir bench-output/bin
          chmod +x "bench-output/bin/${RELEASE_ASSET}"
          echo "UDB_BENCH_RELEASE_TAG=${tag}" >> "$GITHUB_ENV"
          echo "UDB_BENCH_RELEASE_ASSET=${RELEASE_ASSET}" >> "$GITHUB_ENV"
          echo "UDB_BENCH_RELEASE_URL=https://github.com/${GITHUB_REPOSITORY}/releases/tag/${tag}" >> "$GITHUB_ENV"
          echo "UDB_BENCH_BIN=${GITHUB_WORKSPACE}/bench-output/bin/${RELEASE_ASSET}" >> "$GITHUB_ENV"
      - name: Resolve broker binary path
        run: echo "path=${UDB_BENCH_BIN}" >> "$GITHUB_OUTPUT"
      - name: Start backends
        uses: ./.github/actions/start-backends
      - name: Write broker env
        uses: ./.github/actions/broker-env
      - name: Enable perf opt-in
        run: echo "UDB_LIVE_PERF=1" >> "$GITHUB_ENV"
      - name: Prepare per-SDK reset script
        run: mkdir -p bench-output/status bench-output/logs
      - name: Collect benchmark JSON
        if: always()
        run: |
          python scripts/collect_sdk_bench_results.py \\
            --out docs/site/bench-results.json \\
            --status-dir bench-output/status
      - name: Upload benchmark report artifact
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: sdk-benchmark-results
          path: |
            docs/site/bench-results.json
            bench-output/logs/**
            bench-output/status/**
      - name: Fail on benchmark failures
        if: always()
        run: |
          python scripts/collect_sdk_bench_results.py --gate docs/site/bench-results.json
      - name: Stop broker and backends
        if: always()
        run: docker rm -f udb-bench-minio || true
"""
        benchmark_orchestrator_good = """name: Benchmark · SDKs
on:
  push:
    branches: [main]
    paths:
      - "proto/**"
      - "api/**"
      - "src/runtime/descriptor_manifest.rs"
      - "src/runtime/sdk_manifest.rs"
      - "src/cli/sdk_gen.rs"
      - "sdk-templates/**"
      - "scripts/openapi-postprocess.mjs"
      - "scripts/collect_sdk_bench_results.py"
      - "scripts/gen-bench-bodies-skeleton.mjs"
      - "scripts/gen-bench-bodies-json.mjs"
      - "docs/bench-bodies/**"
      - "docs/site/benchmarks.html"
      - "docs/site/benchmarks.js"
      - "docs/site/README.md"
  workflow_dispatch:
    inputs:
      release_tag:
        default: "latest"
      release_asset:
        default: "udb-linux-amd64-full"
  workflow_run:
    workflows: ["Release"]
    types: [completed]
permissions:
  contents: read
jobs:
  validate:
    steps:
      - name: Confirm release benchmark is gated
        run: |
          grep -q "github.event.workflow_run.conclusion == 'success'" .github/workflows/benchmark-sdks.yml
          grep -q "startsWith(github.event.workflow_run.head_branch, 'v')" .github/workflows/benchmark-sdks.yml
  benchmark:
    if: >
      github.event_name == 'workflow_dispatch' ||
      (
        github.event_name == 'workflow_run' &&
        github.event.workflow_run.conclusion == 'success' &&
        startsWith(github.event.workflow_run.head_branch, 'v')
      )
    uses: ./.github/workflows/_live-sdk-suite.yml
    with:
      release-tag: ${{ github.event.workflow_run.head_branch || inputs.release_tag || 'latest' }}
      release-asset: ${{ inputs.release_asset || 'udb-linux-amd64-full' }}
    secrets: inherit
"""
        ci_architecture_good = """# UDB CI Architecture

PR gate runs sdk-conformance(mock) and scaffold-compiles.
Live all-SDK/all-RPC coverage is
owned by the post-release benchmark.

Reusable workflows:
- _live-sdk-suite.yml is the release-binary SDK live benchmark/perf suite.
  CI owns only the
  offline SDK conformance/facade/scaffold gates.

Self-test + lint:
- lint-workflows.yml is path-scoped actionlint + workflow posture and is
  not currently a
  branch-protection required check.

Required (PR gate): quick-gate, buf, versions, sdk-static (*),
sdk-conformance, smoke, scaffold-compiles.

NOT required: path-scoped `lint-workflows.yml`/`actionlint`.

Post-release chain:
Release success -> benchmark-sdks.yml -> _live-sdk-suite.yml
Benchmark completion -> pages.yml
Release success / schedule / dispatch -> cleanup-packages.yml

benchmark-sdks.yml calls the reusable suite after Release.
"""
        publish_skill_good = """name: Publish UDB skill
on:
  push:
    branches: [main]
    paths:
      - "udb-skill/**"
      - ".github/workflows/publish-skill.yml"
  release:
    types: [published]
  workflow_dispatch:
permissions:
  contents: read
jobs:
  validate:
    steps:
      - name: Validate manifests + structure
        run: |
          test -f udb-skill/plugins/udb/skills/using-udb/SKILL.md
          test -f udb-skill/plugins/udb/skills/udb-coding/SKILL.md
          test -f udb-skill/.claude-plugin/marketplace.json
          test -f udb-skill/plugins/udb/.claude-plugin/plugin.json
      - name: Wrapper drift check
        run: true
      - name: Validate with Claude CLI
        run: true
  smoke:
    needs: validate
    continue-on-error: true
    steps:
      - run: echo "ANTHROPIC_API_KEY not set"
  ollama:
    needs: validate
    steps:
      - run: |
          echo "OLLAMA_API_KEY not set"
          create_and_publish("udb-assistant", "udb-skill/ollama/Modelfile")
          create_and_publish("udb-coding", "udb-skill/ollama/Modelfile.udb-coding")
          registry.ollama.ai/v2/${model}/manifests/latest
  openai:
    needs: validate
    steps:
      - run: |
          echo "OPENAI_API_KEY not set"
          upsert "UDB Assistant"    udb-skill/openai/instructions.md
          upsert "UDB Coding Agent" udb-skill/openai/instructions-udb-coding.md
"""
        shadow_live_sdk_good = """name: Shadow live-sdk-suite
on:
  workflow_dispatch:
    inputs:
      release_tag:
        default: "latest"
      release_asset:
        default: "udb-linux-amd64-full"
permissions:
  contents: read
jobs:
  shadow:
    uses: ./.github/workflows/_live-sdk-suite.yml
    with:
      release-tag: ${{ inputs.release_tag }}
      release-asset: ${{ inputs.release_asset }}
    secrets: inherit
"""
        composite_selftest_good = """name: _selftest
on:
  workflow_dispatch:
    inputs:
      test_launch:
        default: false
permissions:
  contents: read
concurrency:
  group: selftest-${{ github.ref }}
  cancel-in-progress: true
jobs:
  broker-env:
    steps:
      - uses: ./.github/actions/broker-env
      - run: test -n "$UDB_TLS_REQUIRED"
  setup-rust:
    steps:
      - uses: ./.github/actions/setup-rust
        with:
          install-build-deps: "false"
      - run: rustc --version
  version-guard:
    steps:
      - uses: ./.github/actions/version-guard
  setup-sdk-toolchains:
    steps:
      - uses: ./.github/actions/setup-sdk-toolchains
      - run: |
          node -v
          python --version
          go version
  start-backends:
    steps:
      - uses: ./.github/actions/start-backends
        with:
          minio: "true"
          kafka: "true"
      - run: |
          docker ps --filter name=udb-bench-minio
          docker ps --filter name=udb-bench-kafka
      - run: docker rm -f udb-bench-minio udb-bench-kafka
  launch-broker:
    steps:
      - run: echo "use _live-sdk-suite for an end-to-end launch-broker test"
"""
        pages_good = """name: Deploy site (GitHub Pages)
on:
  push:
    branches: [main]
    paths:
      - "docs/site/**"
      - "docs/assets/**"
      - "api/**"
      - "crates/udb-portable/**"
      - "crates/udb-wasm/**"
      - "src/parser/**"
      - "scripts/playground_wasm_smoke.mjs"
  workflow_dispatch:
  workflow_run:
    workflows: ["Benchmark · SDKs"]
    types: [completed]
permissions:
  contents: read
  pages: write
  id-token: write
  actions: read
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Sync brand assets into the site
        run: |
          mkdir -p docs/site/assets
          cp -v docs/assets/*.svg docs/site/assets/
      - name: Publish Swagger API document
        run: |
          mkdir -p docs/site/api
          cp -v api/*.json docs/site/api/
      - name: Pull latest benchmark results into the site
        env:
          GH_TOKEN: ${{ github.token }}
          TRIGGER_RUN_ID: ${{ github.event.workflow_run.id }}
        run: |
          got_fresh=0
          gh run download "${TRIGGER_RUN_ID}" --repo "${GITHUB_REPOSITORY}" --name sdk-benchmark-results --dir bench-artifact
          cp -v bench-artifact/docs/site/bench-results.json docs/site/bench-results.json
          got_fresh=1
          if [ "$got_fresh" != 1 ]; then echo "keeping committed docs/site/bench-results.json"; fi
      - name: Build UDB's parser to WebAssembly
        run: |
          rustup target add wasm32-unknown-unknown
          cargo build -p udb-wasm --release --target wasm32-unknown-unknown
          cp -v target/wasm32-unknown-unknown/release/udb_wasm.wasm docs/site/udb.wasm
      - name: Verify playground parses current editor input
        run: node scripts/playground_wasm_smoke.mjs docs/site/udb.wasm
      - name: Verify site artifact contract
        run: |
          test -f docs/site/index.html
          test -f docs/site/playground.html
          test -f docs/site/architecture.html
          test -f docs/site/data-plane.html
          test -f docs/site/control-plane.html
          test -f docs/site/security.html
          test -f docs/site/enterprise.html
          test -f docs/site/sdks.html
          test -f docs/site/styles.css
          test -f docs/site/app.js
          test -f docs/site/playground.js
          test -f docs/site/udb.wasm
          test -f docs/site/assets/udb_logo.svg
          test -f docs/site/benchmarks.html
          test -f docs/site/benchmarks.js
          test -f docs/site/bench-results.json
          test -f docs/site/api.html
          test -f docs/site/api/udb-broker.swagger.json
          python3 - <<'PY'
          import json
          import re
          from pathlib import Path

          swagger = json.loads(Path("docs/site/api/udb-broker.swagger.json").read_text())
          assert swagger.get("swagger") == "2.0", "published Swagger JSON is not Swagger 2.0"
          assert swagger.get("paths"), "published Swagger JSON has no API paths"
          required_extensions = [
              "x-udb-sdk-alias",
              "x-udb-scope",
              "x-udb-retry-safe",
              "x-udb-idempotency",
              "x-udb-resource",
              "x-udb-operation-kind",
          ]
          operations = []
          for api_path, path_item in swagger.get("paths", {}).items():
              for verb, operation in (path_item or {}).items():
                  if verb not in {"get", "put", "post", "patch", "delete"}:
                      continue
                  if not isinstance(operation, dict):
                      continue
                  operations.append((verb, api_path, operation))
          assert operations, "published Swagger JSON has no operations"
          for verb, api_path, operation in operations:
              where = f"{verb.upper()} {api_path}"
              operation_id = operation.get("operationId", "")
              assert operation_id, f"{where} has no operationId"
              assert not re.match(r"^[A-Za-z0-9]+Service_[A-Za-z0-9]+$", operation_id), f"{where} has generated operationId {operation_id}"
              missing_extensions = [key for key in required_extensions if key not in operation]
              assert not missing_extensions, f"{where} missing descriptor extensions: {missing_extensions}"
          bench = json.loads(Path("docs/site/bench-results.json").read_text())
          summary = bench.get("summary")
          assert isinstance(summary, dict), "benchmark JSON has no summary object"
          assert "failed_rpc_count" in summary, "benchmark JSON has no failed_rpc_count"
          assert isinstance(bench.get("sdks"), list), "benchmark JSON sdks must be a list"
          assert isinstance(bench.get("history"), list), "benchmark JSON history must be a list"
          full_rows = []
          for sdk in bench.get("sdks", []):
              rows = sdk.get("full_rpcs") or []
              assert isinstance(rows, list), f"benchmark full_rpcs for {sdk.get('id')} must be a list"
              for row in rows:
                  service = str(row.get("service") or "")
                  rpc = str(row.get("wire_rpc") or row.get("rpc") or "")
                  wire_api = row.get("wire_api") or (f"{service}/{rpc}" if service and rpc else rpc)
                  row.setdefault("wire_api", wire_api)
                  row.setdefault("api_alias", "")
                  row.setdefault("operation_id", "")
                  row.setdefault("api", row.get("operation_id") or row.get("api_alias") or wire_api)
              full_rows.extend(rows)
          if full_rows:
              missing_identity = [
                  row for row in full_rows
                  if "wire_api" not in row or "api_alias" not in row or "operation_id" not in row
              ]
              assert not missing_identity, "benchmark full_rpcs rows must include wire_api, api_alias, and operation_id"
              assert any(row.get("operation_id") or row.get("api_alias") or row.get("wire_api") for row in full_rows), "benchmark full_rpcs rows lack public API identity"
          Path("docs/site/bench-results.json").write_text(json.dumps(bench, indent=2, sort_keys=True) + "\n")

          from html.parser import HTMLParser
          from urllib.parse import urlparse

          site = Path("docs/site").resolve()

          class LocalRefParser(HTMLParser):
              def __init__(self):
                  super().__init__()
                  self.refs = []

              def handle_starttag(self, tag, attrs):
                  for key, value in attrs:
                      if key in {"href", "src"} and value:
                          self.refs.append(value)

          missing = []
          for html in sorted(site.glob("*.html")):
              parser = LocalRefParser()
              parser.feed(html.read_text(encoding="utf-8"))
              for ref in parser.refs:
                  parsed = urlparse(ref)
                  if parsed.scheme or parsed.netloc or ref.startswith(("#", "mailto:", "tel:")):
                      continue
                  target = parsed.path
                  if not target or target.startswith("/"):
                      continue
                  path = (html.parent / target).resolve()
                  if site != path and site not in path.parents:
                      missing.append(f"{html.name}: escapes site root: {ref}")
                  elif not path.exists():
                      missing.append(f"{html.name}: missing local ref: {ref}")
          assert not missing, "\\n".join(missing)
          PY
      - uses: actions/upload-pages-artifact@v3
        with:
          path: docs/site
  deploy:
    needs: build
    steps:
      - uses: actions/deploy-pages@v4
"""
        pages_readme_good = """# UDB site (`docs/site`)

The authoring surface is static HTML/CSS plus vanilla JS. The GitHub Pages workflow performs the publish-time contract work: rebuilds `udb.wasm`, syncs shared assets and Swagger JSON, pulls the latest benchmark artifact, and validates the complete site artifact before deploy.

Shared: `styles.css`, `app.js`, `playground.js`, `benchmarks.js`, `udb.wasm`.

`bench-results.json` is uploaded as the `sdk-benchmark-results` artifact and pages.yml falls back to the already-published dashboard JSON for non-benchmark publishes.

The workflow runs the current-editor WASM smoke, verifies every first-class page/script/data artifact, and crawls local HTML `href`/`src` references before upload.
"""
        playground_html_good = """<!doctype html>
<html>
<body>
<script defer src="./playground.js?v=20260701-current-editor"></script>
</body>
</html>
"""
        playground_js_good = """(function () {
  "use strict";
  var WASM_ASSET_VERSION = "20260701-current-editor";
})();
"""
        markdown_links_guard_good = """const ignoredDirs = new Set([
  ".git",
  "target",
  "node_modules",
  "private",
]);
function stripTarget(raw) { return raw; }
function isExternal(target) { return true; }
function existsFrom(baseFile, rawTarget) { return true; }
function stripFencedCodeBlocks(markdown) { return markdown; }
function collectLinks(markdown) { const searchable = stripFencedCodeBlocks(markdown); return []; }
function checkRepo(repoRoot) {
  const markdownFiles = [];
  walk(repoRoot, markdownFiles);
  return { failures: [], checked: markdownFiles.length };
}
function runSelftest() {
  fs.mkdtempSync("links");
  writeFixture(root, "private/research/broken.md", "[missing](./copied-upstream.html)");
  writeFixture(root, "docs/code.md", "```powershell");
  throw new Error("missing local link was not caught");
}
if (process.argv.includes("--selftest")) runSelftest();
checkRepo(process.cwd());
process.exit(1);
"""
        enterprise_readiness_guard_good = """const requiredFiles = [];
const requiredCiSnippets = [];
const requiredRunbookTerms = [];
const requiredCodeEvidence = [];
function checkRepo(repo) { return []; }
function buildFixture(root) {}
function runSelftest() {
  const root = mkdtempSync("enterprise");
  buildFixture(root);
  throw new Error("missing runbook term was not caught");
  throw new Error("missing code evidence was not caught");
}
if (process.argv.includes("--selftest")) runSelftest();
checkRepo(process.cwd());
process.exit(1);
"""
        openapi_api_rule_guard_good = """const retiredBetaRoutes = new Set();
const descriptorOwnedExtensions = [];
const requiredApiErrorFields = [];
const grpcHttpStatusMap = {};
function validateRestMediaBoundary(errors, swagger) {
  throw new Error("root.produces must include application/json");
}
function isForbiddenSuccessWrapper(schema) { return false; }
function restBoundaryResponses() { return {}; }
function normalizedOperationId(id) { return id; }
function isKebabLiteral(segment) { return true; }
function isLowerCamel(value) { return true; }
const betaStabilityClaim = /stable compatibility/;
function scanBetaStabilityClaim(errors, where, value) {}
function checkSwagger(swagger) { return { errors: [], operationCount: 0 }; }
function runSelftest() {
  throw new Error("retired route regression was not caught");
  throw new Error("path/operation naming regressions were not caught");
  throw new Error("custom action case regression was not caught");
  throw new Error("missing descriptor extension was not caught");
  throw new Error("SDK-normalized operationId collision was not caught");
  throw new Error("query dispatch parameter was not caught");
  throw new Error("beta stability wording was not caught");
  throw new Error("stale rpcStatus default response was not caught");
  throw new Error("missing NOT_FOUND->404 response was not caught");
  throw new Error("success envelope response was not caught");
  throw new Error("REST content-type regression was not caught");
}
if (process.argv.includes('--selftest')) runSelftest();
"""
        http_api_style_guard_good = """const allowPath = "scripts/http-api-style.allow.json";
function sourceIndex(root) { return new Map(); }
function inventory(root = repoRoot) {
  throw new Error("route inventory mismatch: native-contract HTTP operations=");
}
function protoHttpInventory(root = repoRoot) { return []; }
function protoApiModel(root = repoRoot) { return { messages: new Map(), rpcs: [] }; }
function resourceIdentityContractRows(root = repoRoot) {
  throw new Error("missing path identity field regression was not caught");
  throw new Error("missing response identity regression was not caught");
  throw new Error("undocumented user-chosen ID regression was not caught");
}
function paginationContractRows(root = repoRoot) {
  throw new Error("legacy offset pagination regression was not caught");
  throw new Error("missing next_page_token regression was not caught");
}
function queryUpdateContractRows(root = repoRoot) {
  throw new Error("undocumented filter regression was not caught");
  throw new Error("missing update_mask regression was not caught");
}
function routeFlags(route, allow) {
  return ["snake_case_literal", "slash_verb", "slash_read_action", "pseudo_read_action", "singular_collection", "deep_path_review"];
}
function buildExceptionReport(root = repoRoot) {
  return {
    resource_identity_contract_exceptions_by_rule: {},
    pagination_contract_exceptions_by_rule: {},
    query_update_contract_exceptions_by_rule: {},
    not_yet_reported_by_this_guard: [],
  };
}
function writeExceptionReport(root = repoRoot) {
  return ["docs/generated/http-api-style-exceptions.json", "docs/generated/http-api-style-exceptions.md"];
}
const allowedLiteralSegments = [];
const allowedDeepPaths = [];
const allowedCommandEndpoints = [];
function runSelftest() {
  throw new Error("api_keys snake_case regression was not caught");
  throw new Error("slash finalize regression was not caught");
  throw new Error("slash download-url regression was not caught");
  throw new Error("pseudo-read action regression was not caught");
  throw new Error("report did not group pseudo-read exception");
  throw new Error("SCIM allowlist failed");
  throw new Error("JWKS allowlist failed");
}
if (args.has('--selftest')) runSelftest();
if (args.has('--advisory')) {}
if (args.has('--source-only')) {}
if (args.has('--write-report')) {}
if (args.has('--resource-identity-contract')) {}
if (args.has('--pagination-contract')) {}
if (args.has('--query-update-contract')) {}
"""
        http_api_style_allow_good = """{
  "allowedLiteralSegments": [
    {"pathPattern": "^/\\\\.well-known/jwks\\\\.json$", "reason": "JWKS"},
    {"pathPattern": "^/v1/idp/scim/[^/]+/(Users|Groups)(/.*)?$", "reason": "SCIM"}
  ],
  "allowedDeepPaths": [
    {"pathPattern": "^/v1/auth/users/\\\\{[^/]+\\\\}/webauthn/credentials/\\\\{[^/]+\\\\}$", "reason": "WebAuthn"}
  ],
  "allowedCommandEndpoints": [
    {"pathPattern": "^/v1/control(/.*)?$", "reason": "control"}
  ]
}
"""
        ci_inventory_guard_good = """const requiredWorkflows = [];
const requiredActions = [];
const requiredCiJobs = [];
const requiredPrCheckJobs = [];
const dependencyFreePrJobs = [];
const releaseFanoutJobs = [];
const releaseLeafWorkflows = [];
function requiredCheckNamesFromArchitecture(text) { return []; }
function workflowInventory(repo) { return {}; }
function checkRepo(repo = ROOT) { return { errors: [], inventory: workflowInventory(repo) }; }
function runSelftest() {
  throw new Error("dependency-free PR job must not declare needs: buf");
  throw new Error("cheap PR job serialization regression was not caught");
  throw new Error("feature-matrix.yml must stay folded");
  throw new Error("release leaf tag-trigger regression was not caught");
  throw new Error("stale required PR check name regression was not caught");
}
if (process.argv.includes("--selftest")) runSelftest();
checkRepo(process.cwd());
process.exit(1);
"""
        branch_protection_lockstep_good = """function requiredCheckNamesFromArchitecture(text) { return []; }
function normalizeRequiredStatusChecks(payload) {
  const names = new Set();
  for (const context of payload.contexts || []) names.add(context);
  for (const check of payload.checks || []) names.add(check.context);
  return [...names].sort();
}
function compareRequiredChecks(documented, actual) {
  return { missingInBranchProtection: documented, staleInBranchProtection: actual };
}
function repoArg(args, name, fallback) {
  throw new Error("must be an owner/repo repository name");
}
function branchArg(args, name, fallback) {
  throw new Error("must be a canonical branch name");
}
const repo = repoArg(args, "--repo", process.env.GITHUB_REPOSITORY);
const branch = branchArg(args, "--branch", process.env.GITHUB_REF_NAME || "main");
const token = process.env.GH_TOKEN || process.env.GITHUB_TOKEN;
const endpoint = "protection/required_status_checks";
function runSelftest() {
  throw new Error("missing required check regression was not caught");
  throw new Error("stale required check regression was not caught");
  throw new Error("padded repository input regression was not caught");
  throw new Error("malformed repository input regression was not caught");
  throw new Error("canonical repository input was rejected");
  throw new Error("padded branch input regression was not caught");
  throw new Error("non-canonical branch input regression was not caught");
  throw new Error("canonical branch input was rejected");
}
process.exit(1);
"""
        ci_runner_evidence_good = """const DEFAULT_BUDGETS = { pr: 8, integration: 30, release: 40, releaseDryRun: 120, benchmark: 120, pages: 20, lint: 10, branchProtection: 10, idempotencyServed: 15, errorDetailServed: 15, retrySafeServed: 15, restGateway: 15 };
const MAX_BUDGETS = { ...DEFAULT_BUDGETS };
const DEFAULT_MAX_EVIDENCE_AGE_DAYS = 14;
const MAX_EVIDENCE_AGE_DAYS = DEFAULT_MAX_EVIDENCE_AGE_DAYS;
const MAX_GITHUB_API_RESPONSE_BYTES = 4 * 1024 * 1024;
const GITHUB_API_REQUEST_TIMEOUT_MS = 30 * 1000;
const MAX_FIXTURE_BYTES = 1 * 1024 * 1024;
const MAX_GITHUB_RUN_JOBS = 500;
const MAX_GITHUB_JOBS_PAGE_SIZE = 100;
const MAX_GITHUB_WORKFLOW_RUN_CANDIDATES = 100;
const ALL_EVIDENCE_MODE = "--all-evidence";
import { execFileSync } from "node:child_process";
const LINT_EVIDENCE_EVENTS = ["workflow_dispatch", "pull_request", "push"];
const DEFAULT_INTEGRATION_BRANCH = "main";
const RELEASE_TAG_PATTERN = /^v\\d+\\.\\d+\\.\\d+/;
const GIT_SHA_PATTERN = /^[0-9a-f]{40}$/;
const RUN_ID_PATTERN = /^[1-9]\\d*$/;
const POSITIVE_DECIMAL_PATTERN = /^(?:[1-9]\\d*(?:\\.\\d+)?|0\\.\\d*[1-9]\\d*)$/;
const GITHUB_ACTIONS_RUN_URL_PATTERN = /^https:\\/\\/github\\.com\\/([A-Za-z0-9_.-]+\\/[A-Za-z0-9_.-]+)\\/actions\\/runs\\/([1-9]\\d*)$/;
const WORKFLOWS = { releaseDryRun: "release-binaries.yml", benchmark: "benchmark-sdks.yml", pages: "pages.yml", branchProtection: "branch-protection-audit.yml", idempotencyServed: "idempotency-served-smoke.yml", errorDetailServed: "error-detail-served-smoke.yml", retrySafeServed: "retry-safe-served-smoke.yml", restGateway: "rest-gateway-smoke.yml" };
const CI_RUN_ID_ARGS = ["--lint-run-id", "--pr-run-id", "--integration-run-id", "--release-run-id", "--release-dry-run-id", "--benchmark-run-id", "--pages-run-id", "--branch-protection-run-id"];
const CI_BUDGET_ARGS = ["--lint-budget-minutes", "--pr-budget-minutes", "--integration-budget-minutes", "--release-budget-minutes", "--release-dry-run-budget-minutes", "--benchmark-budget-minutes", "--pages-budget-minutes", "--branch-protection-budget-minutes"];
const SERVED_BUDGET_ARGS = { idempotencyServed: "--idempotency-served-budget-minutes", errorDetailServed: "--error-detail-served-budget-minutes", retrySafeServed: "--retry-safe-served-budget-minutes", restGateway: "--rest-gateway-budget-minutes" };
const VALUE_ARGS = new Set(["--repo", "--branch", "--release-tag", "--fixture", "--max-evidence-age-days"]);
const FLAG_ARGS = new Set(["--selftest", ALL_EVIDENCE_MODE, "--idempotency-served-smoke", "--error-detail-served-smoke", "--retry-safe-served-smoke", "--rest-gateway-smoke"]);
function assertKnownArgs(args) {
  throw new Error("unknown runner evidence argument");
  throw new Error("unexpected runner evidence argument");
}
function assertNoUnusedEvidenceOverrides(args, servedAuditKeys) {
  throw new Error("otherwise the run id would not be audited");
  throw new Error("otherwise the CI budget would not be audited");
  throw new Error("otherwise the CI evidence option would not be audited");
  throw new Error("otherwise the served budget would not be audited");
}
const PR_REQUIRED_JOBS = [
  "quick-gate",
  "Proto (buf)",
  "Version consistency",
  "PHP SDK (pest)",
  "Go SDK (vet + build)",
  "TypeScript SDK (typecheck + build)",
  "Python SDK (pytest)",
  "C# SDK (build)",
  "Java SDK (compile)",
  "SDK conformance (all languages)",
  "smoke",
  "Scaffold examples compile (six SDKs)",
];
const PR_ADVISORY_JOBS = [
  "Clippy advisory",
  "Rust (ubuntu-latest)",
  "Rust (windows-latest)",
  "Slim build (postgres-only)",
  "Feature check (all-features)",
  "Supply chain policy",
  "Markdown local links + readiness artifacts",
];
const PR_EVIDENCE_JOBS = [...PR_REQUIRED_JOBS, ...PR_ADVISORY_JOBS];
const PR_BUDGET_JOBS = [...new Set([...PR_REQUIRED_JOBS, "build-broker"])];
const INTEGRATION_REQUIRED_JOBS = [
  "quick-gate",
  "Rust (ubuntu-latest)",
  "Rust (windows-latest)",
  "build-broker",
  "smoke",
  "Auth binary (linux-amd64)",
  "Auth binary (windows-amd64)",
  "Auth binary (darwin-arm64)",
  "Auth binary (darwin-amd64)",
  "Slim build (postgres-only)",
  "Plugin feature (qdrant)",
  "Plugin feature (runtime-logging)",
  "Optimized (SIMD accel)",
  "AArch64 scalar",
  "Supply chain policy",
  "Proto (buf)",
  "PHP SDK (pest)",
  "Go SDK (vet + build)",
  "TypeScript SDK (typecheck + build)",
  "Python SDK (pytest)",
  "C# SDK (build)",
  "Java SDK (compile)",
  "SDK conformance (all languages)",
  "Scaffold examples compile (six SDKs)",
  "Version consistency",
  "Markdown local links + readiness artifacts",
  "Native services + canonical stores (live)",
];
const REQUIRED_JOBS = {
  lint: ["actionlint"],
  pr: PR_EVIDENCE_JOBS,
  integration: INTEGRATION_REQUIRED_JOBS,
  release: [
    "ci-green",
    "version-guard",
    "build-binaries / Version guard",
    "build-binaries / Vendored ffmpeg guard",
    "build-binaries / build (udb-linux-amd64)",
    "build-binaries / build (udb-windows-amd64.exe)",
    "build-binaries / build (udb-darwin-arm64)",
    "build-binaries / build (udb-darwin-amd64)",
    "build-binaries / build (udb-linux-amd64-full)",
    "build-binaries / Release manifest",
    "publish-crates / cargo publish",
    "publish-docker / publish ghcr.io/fahara02/udb",
    "publish-ts / Build and publish @udb_plus/sdk",
    "publish-py / Build and publish udb-client",
    "publish-csharp / Build and publish Udb.Client + Udb.Cli",
    "publish-packagist / composer validate",
    "publish-packagist / Split + push to udb-laravel satellite",
    "publish-packagist / Notify Packagist",
  ],
  releaseDryRun: ["Version guard", "Vendored ffmpeg guard", "build (udb-linux-amd64-full)"],
  benchmark: ["Release binary + SDK live benchmarks"],
  pages: ["build", "deploy"],
  branchProtection: ["Branch protection required checks match docs"],
  idempotencyServed: ["DataBroker idempotency served replay proof"],
  errorDetailServed: ["ErrorDetail served transport proof"],
  retrySafeServed: ["Retry-safe mutation metadata served proof"],
  restGateway: ["REST boundary content/status proof"],
};
const RELEASE_SELFTEST_JOB = "publish-docker / publish ghcr.io/fahara02/udb";
const SERVED_SMOKE_AUDITS = {};
function assertSuccessfulBudgetRun(run, label, budgetMinutes, { maxAgeDays, nowMs = Date.now() } = {}) {
  const completedAt = parseActionsTimestampMs(run.completed_at || run.updated_at, "run completion timestamp");
  throw new Error("max evidence age");
}
function assertSuccessfulJobWindowBudgetRun(run, jobs, label, budgetMinutes, evidenceOptions = {}) {
  throw new Error("PR CI required gate run 2 required lane took 9.00 min, budget 8 min");
}
function boundedBudgetArg(args, name, fallback, max) {
  throw new Error("must be a positive decimal number");
  throw new Error("must be <= ${max} minutes");
}
function boundedMaxEvidenceAgeArg(args, name, fallback, max) {
  throw new Error("must be <= ${max} days");
}
function repoArg(args, name, fallback) {
  throw new Error("must be an owner/repo repository name");
}
function optionalReleaseTagArg(args, name) {
  throw new Error("must not include surrounding whitespace");
}
function branchArg(args, name, fallback) {
  throw new Error("must be a canonical branch name");
}
function optionalRunIdArg(args, name) {
  throw new Error("must be a positive integer run id");
}
const idempotencyRunId = optionalRunIdArg(args, "--idempotency-run-id");
boundedBudgetArg(args, "--idempotency-served-budget-minutes", 15, 15);
const errorDetailRunId = optionalRunIdArg(args, "--error-detail-run-id");
boundedBudgetArg(args, "--error-detail-served-budget-minutes", 15, 15);
const retrySafeRunId = optionalRunIdArg(args, "--retry-safe-run-id");
boundedBudgetArg(args, "--retry-safe-served-budget-minutes", 15, 15);
const restGatewayRunId = optionalRunIdArg(args, "--rest-gateway-run-id");
boundedBudgetArg(args, "--rest-gateway-budget-minutes", 15, 15);
function parseActionsTimestampMs(value, label) {
  const ACTIONS_TIMESTAMP_PATTERN = /^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$/;
  throw new Error("must be a GitHub Actions UTC timestamp");
  throw new Error("must not include surrounding whitespace");
}
function assertJobSucceeded(job, label) {
  throw new Error("job ${jobName} is not completed");
  throw new Error("job ${jobName} did not succeed");
  throw new Error("release job ${RELEASE_SELFTEST_JOB} did not succeed: skipped");
}
function assertJobEvidenceName(job, label) {
  throw new Error("job name must be a string");
  throw new Error("job name must be non-empty");
  throw new Error("must not include surrounding whitespace");
}
function assertPrBrokerCompileReduction(jobs) {
  throw new Error("PR CI run must have exactly one build-broker job; found 2");
  throw new Error("PR CI run is missing required artifact-path job: quick-gate");
  throw new Error("PR CI run has duplicate artifact-path job smoke; found 2");
  throw new Error("Scaffold examples compile (six SDKs)");
}
function assertRequiredJobs(jobs, label, requiredNames) {
  assertRequiredJobInventory(label, requiredNames);
  const matchedJobs = [];
  const jobNames = jobs.map((job) => assertJobEvidenceName(job, label));
  throw new Error("PR CI required gate run is missing required jobs: Proto (buf)");
  throw new Error("PR CI run is missing required jobs: Rust (ubuntu-latest)");
  throw new Error("release run is missing required jobs: ${RELEASE_SELFTEST_JOB}");
  throw new Error("release dry-run run is missing required jobs: build (udb-linux-amd64-full)");
  throw new Error("branch-protection run is missing required jobs: Branch protection required checks match docs");
  throw new Error("integration CI run is missing required jobs: Native services + canonical stores (live)");
  throw new Error("integration CI run is missing required jobs: Proto (buf)");
  throw new Error("release run has duplicate required job ${RELEASE_SELFTEST_JOB}; found 2");
  return matchedJobs;
}
function assertRequiredJobInventory(label, requiredNames) {
  throw new Error("required job inventory must be a non-empty array");
  throw new Error("required job inventory names must be strings");
  throw new Error("required job inventory names must be non-empty");
  throw new Error("required job inventory duplicates");
}
throw new Error("duplicate required job inventory regression was not caught");
const lintEvidenceJobs = assertRequiredJobs([], "lint/actionlint", []);
const prBrokerJob = assertPrBrokerCompileReduction([]);
const prBudgetJobs = assertRequiredJobs([], "PR CI required gate", PR_BUDGET_JOBS);
const prEvidenceJobs = [prBrokerJob, ...assertRequiredJobs([], "PR CI", [])];
function assertJobsBelongToRun(jobs, label, run) {
  assertRunEvidenceRunId(run, label);
  const expectedAttempt = assertRunEvidenceAttempt(run, label);
  throw new Error("`${label} job ${assertJobEvidenceName(job, label)} id`");
  throw new Error("reuses job id ${jobId} already used by ${previousJobName}");
  throw new Error("`${label} job ${jobName} run_id`");
  throw new Error("belongs to run ${actualRunId}, want ${expectedRunId}");
  throw new Error("release job ${RELEASE_SELFTEST_JOB} belongs to run 999, want 4");
  throw new Error("`${label} job ${jobName} run_attempt`");
  throw new Error("belongs to run attempt ${actualAttempt}, want ${expectedAttempt}");
  throw new Error("release job ${RELEASE_SELFTEST_JOB} belongs to run attempt 1, want 2");
}
function assertJobEvidenceId(job, label) {
  throw new Error("`${label} job ${assertJobEvidenceName(job, label)} id`");
}
function assertRunEvidenceRunId(run, label) {
  assertPositiveIntegerEvidenceToken(run?.id, `${label} run id`);
}
function assertRunEvidenceAttempt(run, label) {
  assertPositiveIntegerEvidenceToken(run?.run_attempt, `${label} run_attempt`);
}
function assertRunInspectionUrl(run, label, expectedRepo = "") {
  throw new Error("html_url must be a canonical GitHub Actions run URL");
  throw new Error("html_url run id ${actualRunId}, want ${expectedRunId}");
  throw new Error("html_url repo ${actualRepo}, want ${expectedRepo}");
}
function assertPositiveIntegerEvidenceToken(value, label) {
  const runId = String(run?.id ?? "");
  const token = String(value ?? "");
  if (!RUN_ID_PATTERN.test(token)) {
    throw new Error("has invalid value ${token || \"(missing)\"}; want positive integer");
  }
}
function jobTimestampMs(value, label) {
  throw new Error("is missing timestamp");
}
function durationMinutes(run) {
  const start = runStartMs(run, "budget");
  const end = runCompletedMs(run, "budget");
}
function assertJobsWithinRunWindow(jobs, label, run) {
  throw new Error("release job ${RELEASE_SELFTEST_JOB} completed before it started");
  throw new Error("release job ${RELEASE_SELFTEST_JOB} started_at must be a GitHub Actions UTC timestamp");
  throw new Error("completed before it started");
  throw new Error("completed after parent run");
}
function assertRunEvidenceIdentity(run, label, options) {
  assertRunEvidenceRunId(run, label);
  assertRunEvidenceAttempt(run, label);
  assertRunInspectionUrl(run, label, repo);
  assertGitSha(run?.head_sha, `${label} run ${run.id || "(unknown)"}`);
  throw new Error("run is missing workflow path");
  throw new Error("want .github/workflows/release.yml");
  throw new Error("release dry-run run 9 used event push, want workflow_dispatch");
  throw new Error("post-release benchmark run 14 used event workflow_dispatch, want workflow_run");
  throw new Error("post-benchmark Pages run 15 used branch release/v0.3.7, want main");
  throw new Error("branch-protection run 11 used event push, want workflow_dispatch");
}
function assertLintEvidenceBranch(run) {
  throw new Error("lint/actionlint run ${run.id} used branch ${run.head_branch}, want ${DEFAULT_INTEGRATION_BRANCH}");
}
function assertReleaseTag(value, label) {
  const tag = String(value || "");
  throw new Error("has invalid release tag");
  throw new Error("release run 4 has invalid release tag vnext; want vMAJOR.MINOR.PATCH");
  throw new Error("release run 4 has invalid release tag  v0.3.7; want vMAJOR.MINOR.PATCH");
}
function assertGitSha(value, label) {
  const sha = String(value || "");
  throw new Error("want 40 hex characters");
  throw new Error("post-release benchmark run 12 has invalid head_sha not-a-sha; want 40 hex characters");
}
function assertDistinctRunEvidence(runs) {
  const id = assertRunEvidenceRunId(run, `${label} evidence`);
  throw new Error("integration CI evidence reuses run 2 already used by PR CI");
}
function assertSharedRunInspectionRepo(runs) {
  throw new Error("uses repo ${repo}, want ${expectedRepo} from ${expectedLabel}");
}
assertSharedRunInspectionRepo({});
function assertReleaseChainTags({ release, benchmark, pages }) {
  throw new Error("used branch ${actualBranch || \"(missing)\"}, want ${DEFAULT_INTEGRATION_BRANCH}");
  throw new Error("used release tag ${actualBranch || \"(missing)\"}, want ${releaseTag}");
  throw new Error("release chain has missing release head_sha");
  throw new Error("used head_sha ${actualSha}, want ${releaseSha}");
}
function assertReleaseDryRunCommit({ release, releaseDryRun }) {
  const releaseTag = assertReleaseTag(release?.head_branch, "release");
  const dryRunTag = assertReleaseTag(releaseDryRun?.head_branch, `release dry-run run ${releaseDryRun?.id || "(unknown)"}`);
  throw new Error("used release tag ${dryRunTag}, want ${releaseTag}");
  throw new Error("release dry-run run ${releaseDryRun?.id || \"(unknown)\"} used head_sha ${dryRunSha}, want ${releaseSha}");
}
const releaseDryRunLookup = { branch: expectedReleaseTag };
const discoveryFailures = [];
throw new Error("runner evidence discovery failed:");
throw new Error("aggregate live discovery regression was not caught");
throw new Error("PR CI: no successful completed ci.yml run found");
throw new Error("integration CI: no successful completed ci.yml run found");
function assertBranchProtectionCommit({ integration, branchProtection }) {
  const integrationSha = assertGitSha(integration?.head_sha, "integration CI");
  throw new Error("branch-protection run ${branchProtection?.id || \"(unknown)\"} used head_sha ${branchProtectionSha}, want ${integrationSha}");
}
findLatestSuccessfulRun(repo, token, WORKFLOWS.branchProtection, {
          event: "workflow_dispatch",
          branch,
        }, fetcher);
assertRunEvidenceIdentity(branchProtectionRun, "branch-protection", {
    workflow: WORKFLOWS.branchProtection,
    event: "workflow_dispatch",
    branch,
    repo,
  });
throw new Error("custom branch live evidence summary regression was not caught");
async function auditIdempotencyServed(args, budgets, evidenceOptions = {}, fetcher = fetchJson) {
  await findLatestSuccessfulRun(repo, token, WORKFLOWS.idempotencyServed, {
        event: "workflow_dispatch",
        branch,
      }, fetcher);
  assertRequiredJobs([], "idempotency served replay", REQUIRED_JOBS.idempotencyServed);
}
function requestedServedAuditKeys(args) {
  if (args.includes(ALL_EVIDENCE_MODE)) {
    return Object.keys(SERVED_SMOKE_AUDITS);
  }
  return [];
}
async function auditServedSmoke(args, budgets, auditKey, evidenceOptions = {}, fetcher = fetchJson) {
  await findLatestSuccessfulRun(repo, token, WORKFLOWS.errorDetailServed, {
        event: "workflow_dispatch",
        branch,
      }, fetcher);
}
async function auditRequestedServedSmokes(args, budgets, evidenceOptions = {}, fetcher = fetchJson) {
  return { idempotencyServedRunId: "91", restGatewayRunId: "95" };
}
function formatNestedFailure(label, error) {
  return `${label}: ${String(error?.message || error).replace(/\\n/g, "\\n    ")}`;
}
function servedEvidenceSummaryText(summary, servedAuditKeys) {
  return "REST gateway smoke=7.00m(run=95)";
}
async function auditAllEvidence(args, budgets, evidenceOptions = {}, fetcher = fetchJson) {
  throw new Error("runner evidence audit failed:");
}
throw new Error("idempotency served evidence passed:");
throw new Error("served evidence passed:");
throw new Error("served evidence audit failed:");
throw new Error("idempotency served lookup did not request workflow_dispatch branch main");
throw new Error("idempotency served missing proof job regression was not caught");
if (args.includes("--idempotency-served-smoke")) {}
if (args.includes("--error-detail-served-smoke")) {}
if (args.includes("--retry-safe-served-smoke")) {}
if (args.includes("--rest-gateway-smoke")) {}
const servedSmokeSelftests = [
  ["errorDetailServed", 92, "ErrorDetail served transport proof", "--error-detail-served-smoke"],
  ["retrySafeServed", 93, "Retry-safe mutation metadata served proof", "--retry-safe-served-smoke"],
  ["restGateway", 94, "REST boundary content/status proof", "--rest-gateway-smoke"],
];
throw new Error("`${audit.label} lookup did not request workflow_dispatch branch main`");
throw new Error("multi-served evidence aggregation regression was not caught");
throw new Error("multi-served evidence lookup did not audit every requested served workflow");
throw new Error("multi-served missing evidence aggregation regression was not caught");
throw new Error("all-evidence base plus served failure aggregation regression was not caught");
throw new Error("--all-evidence did not select every served proof lane");
throw new Error("CI runner evidence: runner evidence discovery failed:");
throw new Error("served evidence: served evidence audit failed:");
throw new Error("unused CI run-id override regression was not caught");
throw new Error("unused served run-id override regression was not caught");
throw new Error("unused served budget override regression was not caught");
throw new Error("unused CI budget override regression was not caught");
throw new Error("unused release-tag override regression was not caught");
throw new Error("unused fixture override regression was not caught");
throw new Error("unknown runner-evidence argument regression was not caught");
throw new Error("unexpected positional runner-evidence argument regression was not caught");
throw new Error("missing runner-evidence argument value regression was not caught");
if (args.includes(ALL_EVIDENCE_MODE)) {}
function assertReleaseChainOrder({ release, benchmark, pages }) {
  throw new Error("post-release benchmark run 12 started before release run 4 completed");
  throw new Error("post-benchmark Pages run 13 started before benchmark run 12 completed");
}
function appendGitHubApiChunk(body, chunk, label) {
  const next = body + chunk;
  if (Buffer.byteLength(next, "utf8") > MAX_GITHUB_API_RESPONSE_BYTES) {
    throw new Error("GitHub API response exceeded");
  }
}
function assertGitHubApiSuccessStatus(response, body, label) {
  throw new Error("must include an integer HTTP status code");
  throw new Error("GitHub API ${statusCode}:");
}
function githubApiMissingWorkflowError(response, label) {
  throw new Error("GitHub Actions workflow ${workflow} is not visible in ${repo}");
  localWorkflowVisibilityHint(localWorkflowPath);
  gitWorkflowPathState(localWorkflowPath);
  commandSucceeds("git", ["ls-files", "--error-unmatch", "--", localWorkflowPath]);
  commandSucceeds("git", ["diff", "--cached", "--quiet", "--", localWorkflowPath]);
  commandSucceeds("git", ["diff", "--quiet", "--", localWorkflowPath]);
  throw new Error("local file ${localWorkflowPath} exists");
  throw new Error("has staged and unstaged changes");
  throw new Error("has staged changes");
  throw new Error("has unstaged changes");
  throw new Error("is tracked and clean locally");
  throw new Error("commit/push it to the default branch");
  throw new Error("missing workflow GitHub API regression was not caught");
}
function localWorkflowVisibilityHint(localWorkflowPath) {}
function gitWorkflowPathState(localWorkflowPath) {}
function commandSucceeds(command, args) {}
function githubApiRateLimitError(response, body, label) {
  throw new Error("GitHub API rate limit exceeded for ${label}");
  throw new Error("set GH_TOKEN or GITHUB_TOKEN for authenticated evidence lookup");
  throw new Error("GitHub API rate-limit regression was not caught");
  throw new Error("GitHub API secondary-rate-limit regression was not caught");
}
assertGitHubApiSuccessStatus(response, body, url);
function assertGitHubApiJsonContentType(response, label) {
  throw new Error("must include a JSON Content-Type");
  throw new Error("Content-Type must not include surrounding whitespace");
  throw new Error("must be JSON, got");
}
assertGitHubApiJsonContentType(response, url);
function githubApiTimeoutError(label) {
  throw new Error("timed out after ${GITHUB_API_REQUEST_TIMEOUT_MS} ms");
}
request.setTimeout(GITHUB_API_REQUEST_TIMEOUT_MS, () => {
  const error = githubApiTimeoutError(url);
  request.destroy(error);
});
rejectDuplicateJsonObjectKeys(body, `GitHub API response ${url}`);
function readFixtureJson(path) {
  statSync(path);
  throw new Error("fixture ${path} must be a regular file");
  throw new Error("fixture ${path} must be <= ${MAX_FIXTURE_BYTES} bytes");
}
function rejectDuplicateJsonObjectKeys(text, label) {
  const keys = new Set();
  throw new Error("has duplicate JSON object key");
}
function assertFixtureShape(fixture) {
  const runs = githubObject(fixture?.runs, "fixture runs");
  const jobs = githubObject(fixture?.jobs, "fixture jobs");
  throw new Error("fixture jobs.${lane} must be an array");
  githubObject(job, `fixture jobs.${lane}[${index}]`);
}
function auditFixture(path) {
  const fixtureText = readFixtureJson(path);
  rejectDuplicateJsonObjectKeys(fixtureText, `fixture ${path}`);
  const fixture = JSON.parse(fixtureText);
  assertFixtureShape(fixture);
}
function runJobsUrl(repo, runId, page) {
  return `actions/runs/${runId}/jobs?per_page=${MAX_GITHUB_JOBS_PAGE_SIZE}&page=${page}`;
}
function githubObject(value, label) {
  throw new Error("must be a JSON object");
}
async function fetchRun(repo, token, runId) {
  const payload = {};
  const run = githubObject(payload, `run ${runId} response`);
  const actualRunId = assertPositiveIntegerEvidenceToken(run.id, `run ${runId} response id`);
  throw new Error("response id ${actualRunId || \"(missing)\"}, want ${runId}");
}
function githubArrayField(payload, field, label) {
  githubObject(payload, `${label} response`);
  throw new Error("response must include ${field} array");
}
function githubTotalCount(payload, label) {
  throw new Error("response must include non-negative integer total_count");
  if (payload.total_count > MAX_GITHUB_RUN_JOBS) {
    throw new Error("response total_count ${payload.total_count} exceeds ${MAX_GITHUB_RUN_JOBS}");
  }
}
async function fetchRunJobs(repo, token, runId) {
  const payload = {};
  const pageTotalCount = githubTotalCount(payload, "page");
  const totalCount = 1;
  if (pageTotalCount !== totalCount) throw new Error("pagination total_count changed");
  const pageJobs = githubArrayField(payload, "jobs", "page");
  pageJobs.forEach((job, index) => githubObject(job, `${pageLabel} jobs[${index}]`));
  if (pageJobs.length > MAX_GITHUB_JOBS_PAGE_SIZE) {
    throw new Error("response returned ${pageJobs.length} jobs, max ${MAX_GITHUB_JOBS_PAGE_SIZE}");
  }
  if (!runJobsUrl(repo, runId, 2).includes("page=2")) throw new Error("page=2");
  throw new Error("jobs pagination returned 1/101 jobs");
  return fetch(`actions/runs/${runId}/jobs`);
}
async function findLatestSuccessfulRun(repo, token, workflow) {
  const params = { per_page: String(MAX_GITHUB_WORKFLOW_RUN_CANDIDATES) };
  const payload = {};
  const runs = githubArrayField(payload, "workflow_runs", "ci.yml runs");
  githubObject(candidate, `${workflow} runs workflow_runs[${index}]`);
  if (runs.length > MAX_GITHUB_WORKFLOW_RUN_CANDIDATES) {
    throw new Error("workflow_runs, max ${MAX_GITHUB_WORKFLOW_RUN_CANDIDATES}");
  }
  if (candidate.status !== "completed") return false;
  return fetch(`actions/workflows/${encodeURIComponent(workflow)}/runs`);
}
const expectedReleaseTag = releaseTag || String(releaseRun?.head_branch || "");
function fixtureJob(name, conclusion = "success") {}
function runSelftest() {
  const selftestEvidenceOptions = { maxAgeDays: DEFAULT_MAX_EVIDENCE_AGE_DAYS };
  throw new Error("no successful completed ci.yml run found");
  throw new Error("over-budget PR run regression was not caught");
  throw new Error("inflated budget override regression was not caught");
  throw new Error("tightened budget override was rejected");
  throw new Error("padded numeric budget override regression was not caught");
  throw new Error("non-decimal budget override regression was not caught");
  throw new Error("inflated max evidence-age override regression was not caught");
  throw new Error("tightened max evidence-age override was rejected");
  throw new Error("empty max evidence-age override regression was not caught");
  throw new Error("padded release-tag override regression was not caught");
  throw new Error("canonical release-tag override was rejected");
  throw new Error("padded branch override regression was not caught");
  throw new Error("whitespace branch override regression was not caught");
  throw new Error("non-canonical branch override regression was not caught");
  throw new Error("canonical branch override was rejected");
  throw new Error("padded repo override regression was not caught");
  throw new Error("malformed repo override regression was not caught");
  throw new Error("canonical repo override was rejected");
  throw new Error("padded run-id override regression was not caught");
  throw new Error("non-numeric run-id override regression was not caught");
  throw new Error("zero run-id override regression was not caught");
  throw new Error("canonical run-id override was rejected");
  throw new Error("wrong lint branch regression was not caught");
  throw new Error("missing fixture runs regression was not caught");
  throw new Error("non-array fixture jobs regression was not caught");
  throw new Error("malformed fixture job regression was not caught");
  throw new Error("lint/actionlint run 19 used branch feature/lint-proof, want main");
  throw new Error("stale runner evidence regression was not caught");
  throw new Error("late completed_at budget regression was not caught");
  throw new Error("PR CI required gate run 2 required lane took 9.00 min, budget 8 min");
  throw new Error("integration CI run 3 took 31.00 min, budget 30 min");
  throw new Error("padded run timestamp regression was not caught");
  throw new Error("offset job timestamp regression was not caught");
  throw new Error("duplicate build-broker regression was not caught");
  throw new Error("duplicate PR smoke regression was not caught");
  throw new Error("missing PR quick-gate regression was not caught");
  throw new Error("missing PR required job regression was not caught");
  throw new Error("missing PR advisory job regression was not caught");
  throw new Error("wrong workflow evidence regression was not caught");
  throw new Error("release dry-run lookup did not request workflow_dispatch branch v0.3.7");
  throw new Error("branch-protection lookup did not request workflow_dispatch branch main");
  assertDistinctRunEvidence({});
  throw new Error("duplicate run evidence regression was not caught");
  throw new Error("padded distinct run id regression was not caught");
  throw new Error("second evidence run id has invalid value  2; want positive integer");
  throw new Error("wrong job run_id regression was not caught");
  throw new Error("wrong job run_attempt regression was not caught");
  throw new Error("wrong run html_url regression was not caught");
  throw new Error("cross-repo run html_url regression was not caught");
  throw new Error("padded job run_id regression was not caught");
  throw new Error("missing job id regression was not caught");
  throw new Error("padded job id regression was not caught");
  throw new Error("duplicate job id regression was not caught");
  throw new Error("padded job name regression was not caught");
  throw new Error("non-string job name regression was not caught");
  throw new Error("missing run_attempt regression was not caught");
  throw new Error("missing PR head_sha regression was not caught");
  throw new Error("PR CI run 2 has invalid head_sha (missing); want 40 hex characters");
  throw new Error("padded job run_attempt regression was not caught");
  throw new Error("padded release tag regression was not caught");
  throw new Error("non-required job timestamp scope regression");
  throw new Error("impossible job timestamp regression was not caught");
  throw new Error("wrong integration branch evidence regression was not caught");
  throw new Error("used branch feature/not-main, want main");
  throw new Error("missing integration display-name job regression was not caught");
  throw new Error("missing integration full-CI job regression was not caught");
  throw new Error("missing release job regression was not caught");
  throw new Error("wrong release dry-run event regression was not caught");
  throw new Error("missing release dry-run job regression was not caught");
  throw new Error("wrong release dry-run head_sha regression was not caught");
  throw new Error("wrong release dry-run tag regression was not caught");
  throw new Error("release dry-run run 8 used release tag v0.3.8, want v0.3.7");
  throw new Error("release dry-run run 8 used head_sha ${benchmarkSha}, want ${releaseSha}");
  throw new Error("wrong benchmark event regression was not caught");
  throw new Error("missing benchmark job regression was not caught");
  throw new Error("post-release benchmark run is missing required jobs: Release binary + SDK live benchmarks");
  throw new Error("wrong Pages release branch regression was not caught");
  throw new Error("missing Pages deploy regression was not caught");
  throw new Error("post-benchmark Pages run is missing required jobs: deploy");
  throw new Error("malformed release tag regression was not caught");
  throw new Error("wrong benchmark head_sha regression was not caught");
  throw new Error("post-release benchmark run 12 used head_sha ${benchmarkSha}, want ${releaseSha}");
  throw new Error("wrong Pages head_sha regression was not caught");
  throw new Error("post-benchmark Pages run 13 used head_sha ${pagesSha}, want ${releaseSha}");
  throw new Error("missing release head_sha regression was not caught");
  throw new Error("padded release head_sha regression was not caught");
  throw new Error("release run 4 has invalid head_sha  ${releaseSha}; want 40 hex characters");
  throw new Error("uppercase release head_sha regression was not caught");
  throw new Error("release run 4 has invalid head_sha ${releaseSha.toUpperCase()}; want 40 hex characters");
  throw new Error("wrong benchmark head_sha regression was not caught");
  throw new Error("post-release benchmark run 12 used head_sha ${benchmarkSha}, want ${releaseSha}");
  throw new Error("wrong Pages head_sha regression was not caught");
  throw new Error("post-benchmark Pages run 13 used head_sha ${pagesSha}, want ${releaseSha}");
  throw new Error("malformed benchmark head_sha regression was not caught");
  throw new Error("early benchmark ordering regression was not caught");
  throw new Error("early Pages ordering regression was not caught");
  throw new Error("wrong branch-protection event regression was not caught");
  throw new Error("wrong branch-protection branch regression was not caught");
  throw new Error("branch-protection run 18 used branch feature/branch-protection-proof, want main");
  throw new Error("missing branch-protection job regression was not caught");
  throw new Error("wrong branch-protection head_sha regression was not caught");
  throw new Error("branch-protection run 10 used head_sha ${benchmarkSha}, want ${integrationSha}");
  throw new Error("skipped release job regression was not caught");
  throw new Error("duplicate release job regression was not caught");
  throw new Error("paginated jobs regression was not caught");
  throw new Error("truncated jobs pagination regression was not caught");
  throw new Error("overreported jobs pagination regression was not caught");
  throw new Error("oversized jobs page regression was not caught");
  throw new Error("missing jobs array regression was not caught");
  throw new Error("missing jobs total_count regression was not caught");
  throw new Error("oversized jobs total_count regression was not caught");
  throw new Error("changed jobs total_count regression was not caught");
  throw new Error("incomplete workflow run discovery regression was not caught");
  throw new Error("malformed exact run response regression was not caught");
  throw new Error("missing exact run id regression was not caught");
  throw new Error("run 131 response id has invalid value (missing); want positive integer");
  throw new Error("wrong exact run id regression was not caught");
  throw new Error("padded exact run id regression was not caught");
  throw new Error("run 134 response id has invalid value  134; want positive integer");
  throw new Error("malformed job entry regression was not caught");
  throw new Error("malformed workflow runs response regression was not caught");
  throw new Error("malformed workflow run entry regression was not caught");
  throw new Error("bounded workflow run discovery regression was not caught");
  throw new Error("workflow run discovery candidate limit was not requested");
  throw new Error("oversized workflow runs response regression was not caught");
  throw new Error("oversized GitHub API response regression was not caught");
  throw new Error("missing GitHub API status-code regression was not caught");
  throw new Error("malformed GitHub API status-code regression was not caught");
  throw new Error("non-success GitHub API status-code regression was not caught");
  throw new Error("missing GitHub API content-type regression was not caught");
  throw new Error("padded GitHub API content-type regression was not caught");
  throw new Error("non-JSON GitHub API content-type regression was not caught");
  throw new Error("duplicate-key GitHub API response regression was not caught");
  throw new Error("GitHub API request timeout regression was not caught");
  throw new Error("fixture directory regression was not caught");
  throw new Error("oversized fixture regression was not caught");
  throw new Error("duplicate fixture key regression was not caught");
  throw new Error("malformed run id regression was not caught");
}
function main(args) {
  const maxEvidenceAgeDays = boundedMaxEvidenceAgeArg(args, "--max-evidence-age-days", DEFAULT_MAX_EVIDENCE_AGE_DAYS, MAX_EVIDENCE_AGE_DAYS);
  const repo = repoArg(args, "--repo", process.env.GITHUB_REPOSITORY);
  const branch = branchArg(args, "--branch", DEFAULT_INTEGRATION_BRANCH);
  const releaseTag = optionalReleaseTagArg(args, "--release-tag");
  const prRunId = optionalRunIdArg(args, "--pr-run-id");
}
process.exit(1);
"""
        playground_smoke_good = """const mobileProto = invoiceProto.replaceAll("email", "mobile");
await WebAssembly.instantiate(wasmBytes, imports);
assert(col.field === "mobile" && col.column === "mobile", "edited mobile");
assert(!(col.field === "email" || col.column === "email"), "stale email");
assert(invoice.checksum !== mobile.checksum, "checksum changed");
assert(Array.isArray(broken.diagnostics) && broken.diagnostics.length > 0, "diagnostics");
"""
        embedding_roundtrip_good = '''TOPIC_WORK = "udb.embedding.work.v1"
REPORT_METHOD = "udb.core.embedding.services.v1.EmbeddingService/ReportEmbedding"
CALLBACK_PROTO = "proto/udb/core/embedding/services/v1/embedding_service.proto"
DEFAULT_PROTO_IMPORT_PATHS = ("proto", "third_party/googleapis")
FORBIDDEN_WORK_KEYS = {"api_key"}
def normalize_work(value):
    check_no_credentials(value)
def load_work_from_postgres(args): pass
def grpcurl_command():
    cmd.extend(["-proto", str((ROOT / args.proto).resolve())])
    return ["x-udb-scopes: udb:embedding:report-embedding", "authorization: Bearer REDACTED"]
def call_report_embedding(args, report, tenant_id, project_id):
    if payload.get("upserted") is not True:
        raise RuntimeError()
def build_parser():
    parser.add_argument("--use-reflection")
    parser.add_argument("--outbox-relation", default="udb_system.outbox_events")
    parser.add_argument("--journal-relation", default="udb_system.udb_cdc_event_journal")
def main():
    report = sidecar_embed(args.sidecar_url, work)
    if args.dry_run:
        return 0
    call_report_embedding(args, report, work["tenant_id"], args.project_id)
'''
        notify_roundtrip_good = '''REPORT_METHOD = "udb.core.notification.services.v1.NotificationService/ReportDelivery"
CALLBACK_PROTO = "proto/udb/core/notification/services/v1/notification_service.proto"
DEFAULT_PROTO_IMPORT_PATHS = ("proto", "third_party/googleapis")
def load_intent_from_postgres(args): pass
def grpcurl_command():
    cmd.extend(["-proto", str((ROOT / args.proto).resolve())])
    return ["x-udb-scopes: udb:notification:report-delivery", "authorization: Bearer REDACTED"]
def call_report_delivery(args, report, tenant_id, project_id):
    if "attempt" not in payload:
        raise RuntimeError()
def build_report():
    provider_message_id = "id"
def build_parser():
    parser.add_argument("--use-reflection")
    parser.add_argument("--notification-log-relation", default="udb_notification.notification_logs")
    parser.add_argument("--delivery-attempt-relation", default="udb_notification.notification_delivery_attempts")
def main():
    outcome = sidecar_send(args.sidecar_url, intent, args.provider_credential)
    if args.dry_run:
        return 0
    call_report_delivery(args, report, intent["tenant_id"], args.project_id)
'''
        xa_script_good = """#!/usr/bin/env bash
KILL_SERVICE="${UDB_HA_XA_KILL_SERVICE:-udb-xa-ha-a}"
SURVIVOR_SERVICE="${UDB_HA_XA_SURVIVOR_SERVICE:-udb-xa-ha-b}"
cleanup() {
  DROP="DROP DATABASE IF EXISTS \\`udb_xa_mysql_${suffix}\\`"
  psql_exec "DROP SCHEMA IF EXISTS udb_xa_pg_${suffix} CASCADE"
  compose down -v --remove-orphans
}
compose --profile broker-xa-ha up -d --build postgres redis kafka qdrant minio mysql "$KILL_SERVICE" "$SURVIVOR_SERVICE"
compose kill -s KILL "$KILL_SERVICE"
assert_service_stopped "$KILL_SERVICE"
assert_service_running_container "$SURVIVOR_SERVICE" "$SURVIVOR_CID"
mysql_exec "$MYSQL_DB" "
  XA START '${XID}';
  XA PREPARE '${XID}';
"
psql_exec "
  INSERT INTO udb_system.udb_xa_ledger
  VALUES ('${XID}', 'in_doubt', 'commit decided; phase 2 in flight');
"
echo "Waiting for surviving broker"
wait_sql_equals "ledger" "SELECT decision FROM udb_system.udb_xa_ledger WHERE xid = '${XID}';" "committed"
if mysql_exec "$MYSQL_DB" "XA RECOVER;" | grep -Fq "$XID"; then
  exit 1
fi
"""
        integration_compose_good = """services:
  postgres:
    build:
      dockerfile: docker/postgres-pg-partman/Dockerfile
    command:
      - max_prepared_transactions=32
  udb-livekit:
    profiles: ["sfu"]
    environment:
      UDB_LIVEKIT_URL: ws://livekit:7880
      UDB_LIVEKIT_API_KEY: devkey
      UDB_LIVEKIT_API_SECRET: secret
      UDB_LIVEKIT_ALLOW_INSECURE: "1"
      UDB_SESSION_ENABLED: "true"
      UDB_SESSION_HASH_SECRET: local-sfu-session-secret
      UDB_PASSWORD_HASH_SECRET: local-sfu-password-secret
      UDB_JWT_PRIVATE_KEY: src/runtime/testdata/jwt_rs256_private.pem
      UDB_JWT_PUBLIC_KEY: src/runtime/testdata/jwt_rs256_public.pem
      UDB_TURN_SECRET: local-turn-secret
    ports:
      - "50081:50051"
      - "50082:50052"
  livekit:
    profiles: ["sfu"]
    image: livekit/livekit-server:v1.8.4
    ports:
      - "57880:7880"
  coturn:
    profiles: ["sfu"]
    image: coturn/coturn:4.6.2
    command:
      - --static-auth-secret=local-turn-secret
    ports:
      - "53478:3478/udp"
  notify-sidecar:
    profiles: ["notify"]
    build:
      context: ./sidecars/notify
    environment:
      UDB_NOTIFY_PROVIDER: smtp
      UDB_NOTIFY_DRY_RUN: "1"
    ports:
      - "58080:8080"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/healthz"]
  embedding-sidecar:
    profiles: ["embedding"]
    build:
      context: ./sidecars/embedding
    environment:
      UDB_EMBED_PROVIDER: deterministic
      UDB_EMBED_DIMS: "16"
    ports:
      - "58090:8080"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/healthz"]
"""
        canonical_compose_good = """services:
  mysql:
    volumes:
      - ./docker/mysql-init:/docker-entrypoint-initdb.d:ro
  clickhouse:
    volumes:
      - ./docker/clickhouse/config.d/keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro
    healthcheck:
      test: system.zookeeper
"""
        xa_ha_compose_good = """services:
  udb-xa-ha-a:
    profiles: ["broker-xa-ha"]
  udb-xa-ha-b:
    profiles: ["broker-xa-ha"]
x-udb-xa-ha-env:
  UDB_MYSQL_DSN: mysql://udb:udb@mysql:3306/udb
  UDB_XA_RECOVERY_INTERVAL_SECS: "2"
"""
        postgres_partman_dockerfile_good = """FROM postgres:16-alpine AS pg-partman-builder
ARG PG_PARTMAN_VERSION=5.2.4
RUN cd /tmp/pg_partman && make NO_BGW=1 && make NO_BGW=1 install
FROM postgres:16-alpine
COPY --from=pg-partman-builder /usr/local/share/postgresql/extension/pg_partman* /usr/local/share/postgresql/extension/
"""
        mysql_init_good = """GRANT REPLICATION CLIENT ON *.* TO 'udb'@'%';
GRANT CREATE, DROP ON *.* TO 'udb'@'%';
GRANT ALL PRIVILEGES ON `udb\\_conf\\_%`.* TO 'udb'@'%';
GRANT ALL PRIVILEGES ON `udb\\_ir\\_live\\_%`.* TO 'udb'@'%';
GRANT ALL PRIVILEGES ON `udb\\_ir\\_include\\_%`.* TO 'udb'@'%';
GRANT XA_RECOVER_ADMIN ON *.* TO 'udb'@'%';
"""
        clickhouse_keeper_good = """<clickhouse>
  <keeper_map_path_prefix>/udb/keeper_map_tables</keeper_map_path_prefix>
  <keeper_server>
    <tcp_port>9181</tcp_port>
    <raft_configuration>
      <server><port>9234</port></server>
    </raft_configuration>
  </keeper_server>
  <zookeeper><node><port>9181</port></node></zookeeper>
</clickhouse>
"""
        embedding_dockerfile_good = """FROM python:3.12-alpine
COPY embedding_sidecar.py /app/embedding_sidecar.py
ENV UDB_EMBED_PROVIDER=deterministic
ENV UDB_EMBED_DIMS=16
HEALTHCHECK CMD true
CMD ["python", "/app/embedding_sidecar.py"]
"""
        embedding_sidecar_source_good = '''FORBIDDEN_WORK_KEYS = {"api_key"}
def check_no_credentials(value): pass
def parse_work(value):
    check_no_credentials(value)
def resolve_vault_reference(reference): return {}
def embed_work(work):
    if work.provider not in {"openai", "openai-compatible", "azure-openai"}:
        raise RuntimeError()
    return []
class Handler:
    def post(self, work):
        if self.path == "/healthz":
            return
        endpoints = {"/embed-batch", "/v1/embed-batch", "/rerank", "/v1/rerank", "/parse", "/v1/parse"}
        if self.path not in endpoints:
            return
        report = {"vector": embed_work(work)}
        return {"status": "embedded", "report_embedding_request": report, "report_embedding_batch_request": {}, "report_embedding_failure_request": {}}
'''
        notify_dockerfile_good = """FROM python:3.12-alpine
COPY notify_sidecar.py /app/notify_sidecar.py
HEALTHCHECK CMD true
CMD ["python", "/app/notify_sidecar.py"]
"""
        notify_sidecar_source_good = '''def credential_from_authorization(headers):
    raise RuntimeError("Authorization: Bearer <provider credential> is required")
def dry_run(provider, request): pass
def deliver(provider, credential, request):
    if provider == "smtp": pass
    if provider == "ses": pass
    if provider == "twilio": pass
    if provider == "fcm": pass
class Handler:
    def post(self):
        if self.path == "/healthz":
            return
        if self.path not in {"/send", "/v1/send"}:
            return
        result = type("R", (), {"provider_message_id": "id"})()
        return {"provider_message_id": result.provider_message_id}, {"x-provider-message-id": result.provider_message_id}
'''
        native_load_script_good = "\n".join(f'run_case "{case}"' for case in NATIVE_LOAD_REQUIRED_CASES)
        native_load_baseline_good = json.dumps(
            {
                "version": 1,
                "source": "scripts/native-load-test.sh smoke profile",
                "load_profile": {"concurrency": 2, "total": 20},
                "threshold": {"max_regression_percent": 15},
                "cases": {case: {"p99_ms": 1000.0} for case in NATIVE_LOAD_REQUIRED_CASES},
            },
            indent=2,
        )
        lint_paths = "\n".join(f'      - "{path}"' for path, _label in LINT_WORKFLOW_TRIGGER_PATHS)
        branch_protection_workflow_good = """name: Branch protection lockstep audit
on:
  workflow_dispatch:
    inputs:
      branch:
        default: main
permissions:
  contents: read
concurrency:
  group: branch-protection-audit-${{ github.ref }}
jobs:
  branch-protection-lockstep:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - run: node --check scripts/check-branch-protection-lockstep.mjs
      - run: node scripts/check-branch-protection-lockstep.mjs --selftest
      - env:
          GH_TOKEN: ${{ secrets.BRANCH_PROTECTION_TOKEN || github.token }}
          BRANCH_NAME: ${{ inputs.branch }}
        run: node scripts/check-branch-protection-lockstep.mjs --repo "${GITHUB_REPOSITORY}" --branch "${BRANCH_NAME}"
"""
        error_detail_served_workflow_good = """name: ErrorDetail served smoke
on:
  workflow_dispatch:
    inputs:
      release_tag:
        required: true
        default: latest
      release_asset:
        required: true
        default: udb-linux-amd64-full
      broker_artifact_run_id:
        required: false
        default: ""
permissions:
  contents: read
  actions: read
concurrency:
  group: error-detail-served-smoke-${{ github.ref }}
jobs:
  error-detail-served:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    services:
      postgres:
        image: postgres:16-alpine
      mongodb:
        image: mongo:7
    steps:
      - uses: ./.github/actions/resolve-served-binary
        with:
          release-tag: ${{ inputs.release_tag }}
          release-asset: ${{ inputs.release_asset }}
          broker-artifact-run-id: ${{ inputs.broker_artifact_run_id }}
      - uses: ./.github/actions/start-backends
        with:
          clickhouse: "false"
          neo4j: "false"
      - uses: ./.github/actions/broker-env
        with:
          enable_column_backend: "false"
          enable_graph_backend: "false"
      - run: echo "UDB_OTP_COOLDOWN_SECONDS=60" >> "$GITHUB_ENV"
      - run: echo "Bootstrap served-smoke user"
      - uses: ./.github/actions/launch-broker
      - run: python -m pip install -e sdk/python
      - run: python scripts/write_error_detail_served_smoke_inputs.py --out-dir smoke-input
      - run: python scripts/error_detail_served_smoke.py --selftest
      - run: |
          args=(--target "${UDB_AUTH_GRPC_TARGET}" --require-all-proofs)
          while IFS= read -r line || [ -n "$line" ]; do
            [ -n "$line" ] && args+=(--header "$line")
          done < smoke-input/header.txt
          python scripts/error_detail_served_smoke.py "${args[@]}" --validation-method /udb.core.authn.services.v1.AuthnService/SendPhoneVerification --validation-request-module udb.core.authn.services.v1.core_pb2 --validation-request-message SendPhoneVerificationRequest --validation-request-json smoke-input/validation.json --validation-field phone --quota-method /udb.core.authn.services.v1.AuthnService/SendOTP --quota-request-module udb.core.authn.services.v1.core_pb2 --quota-request-message SendOTPRequest --quota-request-json smoke-input/quota.json --quota-retry-after-min-ms 1000 --quota-backend authn --quota-operation otp_cooldown
      - uses: actions/upload-artifact@v4
        with:
          name: error-detail-served-smoke-diagnostics
"""
        idempotency_served_workflow_good = """name: Idempotency served replay smoke
on:
  workflow_dispatch:
    inputs:
      release_tag:
        required: true
        default: latest
      release_asset:
        required: true
        default: udb-linux-amd64-full
      broker_artifact_run_id:
        required: false
        default: ""
permissions:
  contents: read
  actions: read
concurrency:
  group: idempotency-served-smoke-${{ github.ref }}
jobs:
  idempotency-served-replay:
    name: DataBroker idempotency served replay proof
    runs-on: ubuntu-latest
    timeout-minutes: 20
    services:
      postgres: {}
      mongodb: {}
    steps:
      - uses: ./.github/actions/resolve-served-binary
        with:
          release-tag: ${{ inputs.release_tag }}
          release-asset: ${{ inputs.release_asset }}
          broker-artifact-run-id: ${{ inputs.broker_artifact_run_id }}
      - uses: ./.github/actions/start-backends
        with:
          clickhouse: "false"
          neo4j: "false"
      - uses: ./.github/actions/broker-env
        with:
          enable_column_backend: "false"
          enable_graph_backend: "false"
      - run: Bootstrap served-smoke users
      - run: echo "UDB_TENANT2_PROJECT=default-tenant2"
      - uses: ./.github/actions/launch-broker
      - run: python scripts/write_databroker_served_smoke_inputs.py --tenant2-username x --tenant2-project "${UDB_TENANT2_PROJECT}"
      - run: python -m pip install -e sdk/python
      - run: python scripts/idempotency_served_replay_smoke.py --selftest
      - run: |
          echo "Run live idempotency replay proofs"
          done < smoke-input/header.txt
          done < smoke-input/tenant2-header.txt
          python scripts/idempotency_served_replay_smoke.py --fail-closed-code UNAVAILABLE --upsert-json smoke-input/upsert.json --tenant2-upsert-json smoke-input/tenant2-upsert.json --batch-upsert-json smoke-input/batch-upsert.json
      - run: ALTER TABLE IF EXISTS udb_system.udb_idempotency_keys RENAME TO udb_idempotency_keys_served_disabled
      - run: |
          echo "Run live idempotency fail-closed proof"
          python scripts/idempotency_served_replay_smoke.py --fail-closed-code UNAVAILABLE --fail-closed-upsert-json smoke-input/fail-closed-upsert.json --fail-closed-select-json smoke-input/fail-closed-select.json --keyless-upsert-json smoke-input/keyless-upsert.json
      - run: Restore idempotency relation
      - uses: actions/upload-artifact@v4
        with:
          name: idempotency-served-smoke-diagnostics
"""
        retry_safe_served_workflow_good = """name: Retry-safe served smoke
on:
  workflow_dispatch:
    inputs:
      release_tag:
        required: true
        default: latest
      release_asset:
        required: true
        default: udb-linux-amd64-full
      broker_artifact_run_id:
        required: false
        default: ""
permissions:
  contents: read
  actions: read
concurrency:
  group: retry-safe-served-smoke-${{ github.ref }}
jobs:
  retry-safe-served:
    name: Retry-safe mutation metadata served proof
    runs-on: ubuntu-latest
    timeout-minutes: 20
    services:
      postgres: {}
      mongodb: {}
    steps:
      - uses: ./.github/actions/resolve-served-binary
        with:
          release-tag: ${{ inputs.release_tag }}
          release-asset: ${{ inputs.release_asset }}
          broker-artifact-run-id: ${{ inputs.broker_artifact_run_id }}
      - uses: ./.github/actions/start-backends
        with:
          clickhouse: "false"
          neo4j: "false"
      - uses: ./.github/actions/broker-env
        with:
          enable_column_backend: "false"
          enable_graph_backend: "false"
      - run: Bootstrap served-smoke users
      - run: echo "UDB_TENANT2_PROJECT=default-tenant2"
      - uses: ./.github/actions/launch-broker
      - run: python scripts/write_databroker_served_smoke_inputs.py --tenant2-project "${UDB_TENANT2_PROJECT}"
      - run: python -m pip install -e sdk/python
      - run: python scripts/retry_safe_served_smoke.py --selftest
      - run: |
          done < smoke-input/header.txt
          python scripts/retry_safe_served_smoke.py --require-all-proofs --upsert-json smoke-input/retry-upsert.json --delete-json smoke-input/retry-delete.json
      - uses: actions/upload-artifact@v4
        with:
          name: retry-safe-served-smoke-diagnostics
"""
        runner_evidence_workflow_good = """name: CI runner evidence audit
on:
  workflow_dispatch:
    inputs:
      branch:
        default: main
      release_tag:
        default: ""
      pr_run_id:
        default: ""
      integration_run_id:
        default: ""
      release_run_id:
        default: ""
      release_dry_run_id:
        default: ""
      benchmark_run_id:
        default: ""
      pages_run_id:
        default: ""
      branch_protection_run_id:
        default: ""
      lint_run_id:
        default: ""
      idempotency_served_run_id:
        default: ""
      error_detail_served_run_id:
        default: ""
      retry_safe_served_run_id:
        default: ""
      rest_gateway_run_id:
        default: ""
      max_evidence_age_days:
        default: "14"
permissions:
  contents: read
  actions: read
concurrency:
  group: runner-evidence-audit-${{ github.ref }}
jobs:
  runner-evidence:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - run: node --check scripts/check-ci-runner-evidence.mjs
      - run: node scripts/check-ci-runner-evidence.mjs --selftest
      - env:
          PR_RUN_ID: ${{ inputs.pr_run_id }}
          INTEGRATION_RUN_ID: ${{ inputs.integration_run_id }}
          RELEASE_RUN_ID: ${{ inputs.release_run_id }}
          RELEASE_DRY_RUN_ID: ${{ inputs.release_dry_run_id }}
          BENCHMARK_RUN_ID: ${{ inputs.benchmark_run_id }}
          PAGES_RUN_ID: ${{ inputs.pages_run_id }}
          BRANCH_PROTECTION_RUN_ID: ${{ inputs.branch_protection_run_id }}
          LINT_RUN_ID: ${{ inputs.lint_run_id }}
          IDEMPOTENCY_SERVED_RUN_ID: ${{ inputs.idempotency_served_run_id }}
          ERROR_DETAIL_SERVED_RUN_ID: ${{ inputs.error_detail_served_run_id }}
          RETRY_SAFE_SERVED_RUN_ID: ${{ inputs.retry_safe_served_run_id }}
          REST_GATEWAY_RUN_ID: ${{ inputs.rest_gateway_run_id }}
          MAX_EVIDENCE_AGE_DAYS: ${{ inputs.max_evidence_age_days }}
        run: |
          node scripts/check-ci-runner-evidence.mjs --all-evidence --pr-budget-minutes 8 --integration-budget-minutes 30 --release-budget-minutes 40 --release-dry-run-budget-minutes 120 --benchmark-budget-minutes 120 --pages-budget-minutes 20 --branch-protection-budget-minutes 10 --lint-budget-minutes 10 --idempotency-served-budget-minutes 15 --error-detail-served-budget-minutes 15 --retry-safe-served-budget-minutes 15 --rest-gateway-budget-minutes 15 --max-evidence-age-days "${MAX_EVIDENCE_AGE_DAYS}" --pr-run-id "$PR_RUN_ID" --integration-run-id "$INTEGRATION_RUN_ID" --release-run-id "$RELEASE_RUN_ID" --release-dry-run-id "$RELEASE_DRY_RUN_ID" --benchmark-run-id "$BENCHMARK_RUN_ID" --pages-run-id "$PAGES_RUN_ID" --branch-protection-run-id "$BRANCH_PROTECTION_RUN_ID" --lint-run-id "$LINT_RUN_ID" --idempotency-run-id "$IDEMPOTENCY_SERVED_RUN_ID" --error-detail-run-id "$ERROR_DETAIL_SERVED_RUN_ID" --retry-safe-run-id "$RETRY_SAFE_SERVED_RUN_ID" --rest-gateway-run-id "$REST_GATEWAY_RUN_ID"
"""
        lint_good = f"""name: Lint Workflows
on:
  push:
    branches: [main]
    paths:
{lint_paths}
  pull_request:
    paths:
{lint_paths}
  workflow_dispatch:
jobs:
  actionlint:
    runs-on: ubuntu-latest
    steps:
      - run: |
          node --check scripts/ci-inventory.mjs
          node scripts/ci-inventory.mjs --selftest
          node scripts/ci-inventory.mjs
          node --check scripts/check-branch-protection-lockstep.mjs
          node scripts/check-branch-protection-lockstep.mjs --selftest
          node --check scripts/check-ci-runner-evidence.mjs
          node scripts/check-ci-runner-evidence.mjs --selftest
          python3 scripts/error_detail_served_smoke.py --selftest
          python3 scripts/idempotency_served_replay_smoke.py --selftest
          python3 scripts/retry_safe_served_smoke.py --selftest
          python3 scripts/native_load_gate.py --selftest
          python3 scripts/check-workflow-posture.py --selftest
          python3 scripts/check-workflow-posture.py
"""
        for name in PROOF_WORKFLOWS:
            (wf / name).write_text(good, encoding="utf-8")
        (wf / "branch-protection-audit.yml").write_text(branch_protection_workflow_good, encoding="utf-8")
        (wf / "error-detail-served-smoke.yml").write_text(error_detail_served_workflow_good, encoding="utf-8")
        (wf / "ha-smokes.yml").write_text(ha_good, encoding="utf-8")
        (wf / "idempotency-served-smoke.yml").write_text(idempotency_served_workflow_good, encoding="utf-8")
        (wf / "sidecar-smokes.yml").write_text(sidecar_good, encoding="utf-8")
        (wf / "clickhouse-canonical-smoke.yml").write_text(clickhouse_good, encoding="utf-8")
        (wf / "ffmpeg-transcode-smoke.yml").write_text(ffmpeg_good, encoding="utf-8")
        (wf / "metering-smoke.yml").write_text(metering_good, encoding="utf-8")
        (wf / "pg-merge-smoke.yml").write_text(pg_merge_good, encoding="utf-8")
        (wf / "rest-gateway-smoke.yml").write_text(rest_gateway_good, encoding="utf-8")
        (wf / "retry-safe-served-smoke.yml").write_text(retry_safe_served_workflow_good, encoding="utf-8")
        (wf / "runner-evidence-audit.yml").write_text(runner_evidence_workflow_good, encoding="utf-8")
        (wf / "secrets-posture-smoke.yml").write_text(secrets_good, encoding="utf-8")
        (wf / "sfu-smoke.yml").write_text(sfu_good, encoding="utf-8")
        (wf / "webauthn-smoke.yml").write_text(webauthn_good, encoding="utf-8")
        (wf / "release.yml").write_text(release_topology_good, encoding="utf-8")
        (wf / "release-binaries.yml").write_text(release_binaries_good, encoding="utf-8")
        (wf / "release-crates.yml").write_text(release_crates_publisher_good, encoding="utf-8")
        (wf / "release-docker.yml").write_text(release_docker_good, encoding="utf-8")
        (wf / "release-typescript-sdk.yml").write_text(release_typescript_publisher_good, encoding="utf-8")
        (wf / "release-python-sdk.yml").write_text(release_python_publisher_good, encoding="utf-8")
        (wf / "release-csharp-sdk.yml").write_text(release_csharp_publisher_good, encoding="utf-8")
        (wf / "release-packagist.yml").write_text(release_packagist_publisher_good, encoding="utf-8")
        (wf / "cleanup-packages.yml").write_text(cleanup_packages_good, encoding="utf-8")
        (wf / "ci.yml").write_text(ci_good, encoding="utf-8")
        (wf / "_live-sdk-suite.yml").write_text(live_sdk_suite_good, encoding="utf-8")
        (wf / "benchmark-sdks.yml").write_text(benchmark_orchestrator_good, encoding="utf-8")
        (wf / "publish-skill.yml").write_text(publish_skill_good, encoding="utf-8")
        (wf / "_shadow-live-sdk.yml").write_text(shadow_live_sdk_good, encoding="utf-8")
        (wf / "_selftest.yml").write_text(composite_selftest_good, encoding="utf-8")
        (wf / "pages.yml").write_text(pages_good, encoding="utf-8")
        (wf / "lint-workflows.yml").write_text(lint_good, encoding="utf-8")
        (root / "docs").mkdir(parents=True)
        (root / "docs" / "ci-architecture.md").write_text(ci_architecture_good, encoding="utf-8")
        (root / "docker-compose.integration.yml").write_text(integration_compose_good, encoding="utf-8")
        (root / "docker-compose.canonical.yml").write_text(canonical_compose_good, encoding="utf-8")
        (root / "docker-compose.xa-ha.yml").write_text(xa_ha_compose_good, encoding="utf-8")
        (root / "Dockerfile.release").write_text(release_dockerfile_good, encoding="utf-8")
        (root / "docker" / "postgres-pg-partman").mkdir(parents=True)
        (root / "docker" / "mysql-init").mkdir(parents=True)
        (root / "docker" / "clickhouse" / "config.d").mkdir(parents=True)
        (root / "docker" / "postgres-pg-partman" / "Dockerfile").write_text(
            postgres_partman_dockerfile_good,
            encoding="utf-8",
        )
        (root / "docker" / "mysql-init" / "01-grant-replication-client.sql").write_text(
            mysql_init_good,
            encoding="utf-8",
        )
        (root / "docker" / "clickhouse" / "config.d" / "keeper.xml").write_text(
            clickhouse_keeper_good,
            encoding="utf-8",
        )
        (root / "docs" / "site").mkdir(parents=True)
        (root / "docs" / "site" / "README.md").write_text(pages_readme_good, encoding="utf-8")
        (root / "docs" / "site" / "playground.html").write_text(playground_html_good, encoding="utf-8")
        (root / "docs" / "site" / "playground.js").write_text(playground_js_good, encoding="utf-8")
        (root / "sidecars" / "embedding").mkdir(parents=True)
        (root / "sidecars" / "notify").mkdir(parents=True)
        (root / "sidecars" / "embedding" / "Dockerfile").write_text(
            embedding_dockerfile_good,
            encoding="utf-8",
        )
        (root / "sidecars" / "embedding" / "embedding_sidecar.py").write_text(
            embedding_sidecar_source_good,
            encoding="utf-8",
        )
        (root / "sidecars" / "notify" / "Dockerfile").write_text(
            notify_dockerfile_good,
            encoding="utf-8",
        )
        (root / "sidecars" / "notify" / "notify_sidecar.py").write_text(
            notify_sidecar_source_good,
            encoding="utf-8",
        )
        scripts_dir = root / "scripts"
        scripts_dir.mkdir(exist_ok=True)
        (scripts_dir / "playground_wasm_smoke.mjs").write_text(playground_smoke_good, encoding="utf-8")
        (scripts_dir / "ha_xa_recovery_smoke.sh").write_text(xa_script_good, encoding="utf-8")
        (scripts_dir / "embedding_sidecar_roundtrip_smoke.py").write_text(embedding_roundtrip_good, encoding="utf-8")
        (scripts_dir / "notify_sidecar_roundtrip_smoke.py").write_text(notify_roundtrip_good, encoding="utf-8")
        (scripts_dir / "native-load-test.sh").write_text(native_load_script_good, encoding="utf-8")
        (scripts_dir / "native_load_smoke_baseline.json").write_text(native_load_baseline_good, encoding="utf-8")
        (scripts_dir / "check-markdown-links.mjs").write_text(markdown_links_guard_good, encoding="utf-8")
        (scripts_dir / "check-enterprise-readiness.mjs").write_text(enterprise_readiness_guard_good, encoding="utf-8")
        (scripts_dir / "check-openapi-api-rules.mjs").write_text(openapi_api_rule_guard_good, encoding="utf-8")
        (scripts_dir / "check-http-api-style.mjs").write_text(http_api_style_guard_good, encoding="utf-8")
        (scripts_dir / "http-api-style.allow.json").write_text(http_api_style_allow_good, encoding="utf-8")
        (scripts_dir / "ci-inventory.mjs").write_text(ci_inventory_guard_good, encoding="utf-8")
        (scripts_dir / "check-branch-protection-lockstep.mjs").write_text(
            branch_protection_lockstep_good,
            encoding="utf-8",
        )
        (scripts_dir / "check-ci-runner-evidence.mjs").write_text(
            ci_runner_evidence_good,
            encoding="utf-8",
        )
        (scripts_dir / "error_detail_served_smoke.py").write_text(
            "\n".join(needle for needle, _label in ERROR_DETAIL_SERVED_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )
        (scripts_dir / "idempotency_served_replay_smoke.py").write_text(
            "\n".join(needle for needle, _label in IDEMPOTENCY_SERVED_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )
        (scripts_dir / "retry_safe_served_smoke.py").write_text(
            "\n".join(needle for needle, _label in RETRY_SAFE_SERVED_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )
        (scripts_dir / "ffmpeg_transcode_smoke.py").write_text(
            "\n".join(needle for needle, _label in FFMPEG_TRANSCODE_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )
        (scripts_dir / "livekit_sfu_smoke.py").write_text(
            "\n".join(needle for needle, _label in LIVEKIT_SFU_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )
        (scripts_dir / "gen-release-manifest.mjs").write_text(
            release_manifest_generator_good,
            encoding="utf-8",
        )
        (scripts_dir / "rest_route_gateway_smoke.py").write_text(
            "\n".join(needle for needle, _label in REST_ROUTE_GATEWAY_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )
        (scripts_dir / "check-beta-versioning-posture.py").write_text(
            "\n".join(needle for needle, _label in BETA_VERSIONING_POSTURE_REQUIREMENTS),
            encoding="utf-8",
        )
        for rel_path, requirements in COMPOSITE_ACTION_SOURCE_REQUIREMENTS.items():
            action_path = root / rel_path
            action_path.parent.mkdir(parents=True, exist_ok=True)
            action_path.write_text("\n".join(needle for needle, _label in requirements), encoding="utf-8")
        assert not check_proof_workflows(root), "good proof workflows failed"
        assert not check_resilience_smoke_workflow(root), "good resilience workflow failed"
        assert not check_xa_recovery_smoke_script(root), "good XA recovery script failed"
        assert not check_sidecar_smoke_workflow(root), "good sidecar workflow failed"
        assert not check_sidecar_roundtrip_scripts(root), "good sidecar round-trip scripts failed"
        assert not check_sidecar_container_sources(root), "good sidecar container sources failed"
        assert not check_integration_compose_gate_d_profiles(root), "good integration compose Gate D profiles failed"
        assert not check_compose_support_inputs(root), "good compose support inputs failed"
        assert not check_targeted_proof_workflows(root), "good targeted proof workflows failed"
        assert not check_ffmpeg_transcode_smoke_contract(root), "good ffmpeg transcode smoke contract failed"
        assert not check_livekit_sfu_smoke_contract(root), "good LiveKit SFU smoke contract failed"
        assert not check_release_binaries_ffmpeg_gate(root), "good release ffmpeg gate failed"
        assert not check_release_binary_matrix_contract(root), "good release binary matrix failed"
        assert not check_release_manifest_generator_contract(root), "good release manifest generator failed"
        assert not check_release_publisher_leaf_contracts(root), "good release publisher leaves failed"
        assert not check_release_docker_single_artifact(root), "good release Docker workflow failed"
        assert not check_release_dockerfile_contract(root), "good release Dockerfile failed"
        assert not check_ci_launcher_asset_gate(root), "good CI launcher asset gate failed"
        assert not check_ci_sdk_service_coverage_gate(root), "good CI SDK service-coverage gate failed"
        assert not check_ci_topology_contract(root), "good CI topology contract failed"
        assert not check_ci_architecture_contract(root), "good CI architecture contract failed"
        assert not check_ci_quick_gate_source_guards(root), "good CI quick-gate source guards failed"
        assert not check_ci_public_docs_guards(root), "good CI public-doc guards failed"
        assert not check_ci_docs_links_gate(root), "good CI docs-links gate failed"
        assert not check_markdown_link_guard_contract(root), "good markdown link guard contract failed"
        assert not check_enterprise_readiness_guard_contract(root), "good enterprise readiness guard contract failed"
        assert not check_openapi_api_rule_guard_contract(root), "good OpenAPI API-rule guard contract failed"
        assert not check_http_api_style_guard_contract(root), "good HTTP API route-style guard contract failed"
        assert not check_rest_route_gateway_smoke_contract(root), "good REST route gateway smoke contract failed"
        assert not check_beta_versioning_posture_contract(root), "good beta versioning posture contract failed"
        assert not check_ci_http_api_style_gate(root), "good HTTP API route-style CI gate failed"
        assert not check_ci_inventory_guard_contract(root), "good CI inventory guard contract failed"
        assert not check_branch_protection_lockstep_guard(root), "good branch-protection audit contract failed"
        assert not check_ci_runner_evidence_guard(root), "good CI runner evidence contract failed"
        assert not check_error_detail_served_smoke_contract(root), "good ErrorDetail served smoke contract failed"
        assert not check_idempotency_served_smoke_contract(root), "good idempotency served smoke contract failed"
        assert not check_retry_safe_served_smoke_contract(root), "good retry-safe served smoke contract failed"
        assert not check_ci_rust_generated_contract_doc_gates(root), "good CI generated contract/doc gates failed"
        assert not check_ci_buf_generated_artifact_gate(root), "good CI buf generated-artifact gate failed"
        assert not check_ci_smoke_load_gate(root), "good CI smoke/load gate failed"
        assert not check_native_load_case_contract(root), "good native load case contract failed"
        assert not check_ci_native_integration_gate(root), "good CI native-integration gate failed"
        assert not check_benchmark_orchestrator_gate(root), "good benchmark orchestrator gate failed"
        assert not check_benchmark_workflow_gate(root), "good benchmark workflow gate failed"
        assert not check_pages_playground_wasm_gate(root), "good Pages playground WASM gate failed"
        assert not check_pages_single_owner(root), "good pages owner failed"
        assert not check_cleanup_packages_ownership(root), "good cleanup packages ownership failed"
        assert not check_publish_skill_workflow(root), "good publish-skill workflow failed"
        assert not check_shadow_live_sdk_workflow(root), "good shadow live SDK workflow failed"
        assert not check_composite_selftest_workflow(root), "good composite selftest workflow failed"
        assert not check_composite_action_contracts(root), "good composite action contracts failed"
        assert not check_lint_workflow_trigger_paths(root), "good workflow lint trigger paths failed"
        assert not check_lint_workflow_covers_referenced_helpers(root), "good workflow helper reference coverage failed"

        (wf / "error-detail-served-smoke.yml").write_text(
            error_detail_served_workflow_good.replace(
                "      release_tag:\n        required: true",
                "      release_tag:\n        required: false",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("release_tag" in failure and "must be required" in failure for failure in failures), failures
        (wf / "error-detail-served-smoke.yml").write_text(error_detail_served_workflow_good, encoding="utf-8")

        (wf / "error-detail-served-smoke.yml").write_text(
            error_detail_served_workflow_good.replace(
                "scripts/write_error_detail_served_smoke_inputs.py",
                "scripts/missing_error_detail_generator.py",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("ErrorDetail proof input generator" in failure for failure in failures), failures
        (wf / "error-detail-served-smoke.yml").write_text(error_detail_served_workflow_good, encoding="utf-8")

        (wf / "error-detail-served-smoke.yml").write_text(
            error_detail_served_workflow_good.replace("--validation-field phone ", ""),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("validation field handoff" in failure for failure in failures), failures
        (wf / "error-detail-served-smoke.yml").write_text(error_detail_served_workflow_good, encoding="utf-8")

        (wf / "error-detail-served-smoke.yml").write_text(
            error_detail_served_workflow_good.replace(
                '      - run: echo "UDB_OTP_COOLDOWN_SECONDS=60" >> "$GITHUB_ENV"',
                '      - run: echo "UDB_OTP_COOLDOWN_SECONDS=0" >> "$GITHUB_ENV"',
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("ErrorDetail quota cooldown override" in failure for failure in failures), failures
        (wf / "error-detail-served-smoke.yml").write_text(error_detail_served_workflow_good, encoding="utf-8")

        (wf / "idempotency-served-smoke.yml").write_text(
            idempotency_served_workflow_good.replace(
                "scripts/write_databroker_served_smoke_inputs.py",
                "scripts/missing_generator.py",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("served proof input generator" in failure for failure in failures), failures
        (wf / "idempotency-served-smoke.yml").write_text(idempotency_served_workflow_good, encoding="utf-8")

        (wf / "idempotency-served-smoke.yml").write_text(
            idempotency_served_workflow_good.replace(
                "      release_tag:\n        required: true",
                "      release_tag:\n        required: false",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("release_tag" in failure and "must be required" in failure for failure in failures), failures
        (wf / "idempotency-served-smoke.yml").write_text(idempotency_served_workflow_good, encoding="utf-8")

        (wf / "idempotency-served-smoke.yml").write_text(
            idempotency_served_workflow_good.replace(
                "Run live idempotency fail-closed proof",
                "Run live idempotency partial proof",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("dedup-store-down proof phase" in failure for failure in failures), failures
        (wf / "idempotency-served-smoke.yml").write_text(idempotency_served_workflow_good, encoding="utf-8")

        (wf / "idempotency-served-smoke.yml").write_text(
            idempotency_served_workflow_good.replace(
                "Restore idempotency relation",
                "Skip idempotency relation restore",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("dedup relation restore" in failure for failure in failures), failures
        (wf / "idempotency-served-smoke.yml").write_text(idempotency_served_workflow_good, encoding="utf-8")

        (wf / "idempotency-served-smoke.yml").write_text(
            idempotency_served_workflow_good.replace(
                "ALTER TABLE IF EXISTS udb_system.udb_idempotency_keys RENAME TO",
                "ALTER TABLE IF EXISTS udb_system.udb_idempotency_keys_ignored RENAME TO",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("dedup relation disablement" in failure for failure in failures), failures
        (wf / "idempotency-served-smoke.yml").write_text(idempotency_served_workflow_good, encoding="utf-8")

        (wf / "retry-safe-served-smoke.yml").write_text(
            retry_safe_served_workflow_good.replace(
                "scripts/write_databroker_served_smoke_inputs.py",
                "scripts/missing_generator.py",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("served proof input generator" in failure for failure in failures), failures
        (wf / "retry-safe-served-smoke.yml").write_text(retry_safe_served_workflow_good, encoding="utf-8")

        (wf / "retry-safe-served-smoke.yml").write_text(
            retry_safe_served_workflow_good.replace("--require-all-proofs ", ""),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("complete retry-safe Upsert/Delete proof gate" in failure for failure in failures), failures
        (wf / "retry-safe-served-smoke.yml").write_text(retry_safe_served_workflow_good, encoding="utf-8")

        (wf / "retry-safe-served-smoke.yml").write_text(
            retry_safe_served_workflow_good.replace(
                "      release_asset:\n        required: true",
                "      release_asset:\n        required: false",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("release_asset" in failure and "must be required" in failure for failure in failures), failures
        (wf / "retry-safe-served-smoke.yml").write_text(retry_safe_served_workflow_good, encoding="utf-8")

        (wf / "rest-gateway-smoke.yml").write_text(
            rest_gateway_good.replace(
                "      success_route:\n        required: true",
                '      success_route:\n        required: true\n        default: "GET /v1/placeholder"',
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("success_route" in failure and "must not define a default" in failure for failure in failures), failures
        (wf / "rest-gateway-smoke.yml").write_text(rest_gateway_good, encoding="utf-8")

        (wf / "runner-evidence-audit.yml").write_text(
            runner_evidence_workflow_good.replace(
                '--max-evidence-age-days "${MAX_EVIDENCE_AGE_DAYS}" ',
                "",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("runner evidence max-age handoff" in failure for failure in failures), failures
        (wf / "runner-evidence-audit.yml").write_text(runner_evidence_workflow_good, encoding="utf-8")

        (wf / "runner-evidence-audit.yml").write_text(
            runner_evidence_workflow_good.replace(
                "--all-evidence ",
                "",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("full central evidence audit mode handoff" in failure for failure in failures), failures
        (wf / "runner-evidence-audit.yml").write_text(runner_evidence_workflow_good, encoding="utf-8")

        (wf / "runner-evidence-audit.yml").write_text(
            runner_evidence_workflow_good.replace(
                "--rest-gateway-budget-minutes 15 ",
                "--rest-gateway-smoke --rest-gateway-budget-minutes 15 ",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("remove redundant --rest-gateway-smoke" in failure for failure in failures), failures
        (wf / "runner-evidence-audit.yml").write_text(runner_evidence_workflow_good, encoding="utf-8")

        (wf / "pg-merge-smoke.yml").write_text(
            good.replace("down -v --remove-orphans", "ps"),
            encoding="utf-8",
        )
        failures = check_proof_workflows(root)
        assert any("volume-removing teardown" in failure for failure in failures), failures

        (wf / "ha-smokes.yml").write_text(
            ha_good.replace("bash scripts/cdc_fault_smoke.sh", "bash scripts/other.sh"),
            encoding="utf-8",
        )
        failures = check_resilience_smoke_workflow(root)
        assert any("CDC fault smoke script" in failure for failure in failures), failures

        (scripts_dir / "ha_xa_recovery_smoke.sh").write_text(
            xa_script_good.replace("XA PREPARE '${XID}';", "XA END '${XID}';"),
            encoding="utf-8",
        )
        failures = check_xa_recovery_smoke_script(root)
        assert any("MySQL XA prepare" in failure for failure in failures), failures
        (scripts_dir / "ha_xa_recovery_smoke.sh").write_text(xa_script_good, encoding="utf-8")

        (wf / "sidecar-smokes.yml").write_text(
            sidecar_good.replace("python scripts/notify_sidecar_roundtrip_smoke.py --selftest", "python scripts/notify_sidecar_smoke.py"),
            encoding="utf-8",
        )
        failures = check_sidecar_smoke_workflow(root)
        assert any("notification round-trip selftest" in failure for failure in failures), failures

        (wf / "sidecar-smokes.yml").write_text(
            sidecar_good.replace("python scripts/embedding_sidecar_smoke.py --selftest", "python scripts/embedding_sidecar_smoke.py --help"),
            encoding="utf-8",
        )
        failures = check_sidecar_smoke_workflow(root)
        assert any("embedding sidecar smoke selftest" in failure for failure in failures), failures

        (scripts_dir / "embedding_sidecar_roundtrip_smoke.py").write_text(
            embedding_roundtrip_good.replace(
                "call_report_embedding(args, report, work[\"tenant_id\"], args.project_id)",
                "print(report)",
            ),
            encoding="utf-8",
        )
        failures = check_sidecar_roundtrip_scripts(root)
        assert any("ReportEmbedding callback call" in failure for failure in failures), failures
        (scripts_dir / "embedding_sidecar_roundtrip_smoke.py").write_text(embedding_roundtrip_good, encoding="utf-8")

        (scripts_dir / "embedding_sidecar_roundtrip_smoke.py").write_text(
            embedding_roundtrip_good.replace(
                'cmd.extend(["-proto", str((ROOT / args.proto).resolve())])',
                "pass",
            ),
            encoding="utf-8",
        )
        failures = check_sidecar_roundtrip_scripts(root)
        assert any("ReportEmbedding proto-mode grpcurl" in failure for failure in failures), failures
        (scripts_dir / "embedding_sidecar_roundtrip_smoke.py").write_text(embedding_roundtrip_good, encoding="utf-8")

        (scripts_dir / "notify_sidecar_roundtrip_smoke.py").write_text(
            notify_roundtrip_good.replace(
                "x-udb-scopes: udb:notification:report-delivery",
                "x-udb-scopes: udb:notification:read",
            ),
            encoding="utf-8",
        )
        failures = check_sidecar_roundtrip_scripts(root)
        assert any("ReportDelivery scope metadata" in failure for failure in failures), failures
        (scripts_dir / "notify_sidecar_roundtrip_smoke.py").write_text(notify_roundtrip_good, encoding="utf-8")

        (scripts_dir / "notify_sidecar_roundtrip_smoke.py").write_text(
            notify_roundtrip_good.replace(
                'cmd.extend(["-proto", str((ROOT / args.proto).resolve())])',
                "pass",
            ),
            encoding="utf-8",
        )
        failures = check_sidecar_roundtrip_scripts(root)
        assert any("ReportDelivery proto-mode grpcurl" in failure for failure in failures), failures
        (scripts_dir / "notify_sidecar_roundtrip_smoke.py").write_text(notify_roundtrip_good, encoding="utf-8")

        (root / "sidecars" / "embedding" / "embedding_sidecar.py").write_text(
            embedding_sidecar_source_good.replace("check_no_credentials(value)", "pass"),
            encoding="utf-8",
        )
        failures = check_sidecar_container_sources(root)
        assert any("embedding recursive credential check" in failure for failure in failures), failures
        (root / "sidecars" / "embedding" / "embedding_sidecar.py").write_text(
            embedding_sidecar_source_good,
            encoding="utf-8",
        )

        (root / "docker-compose.integration.yml").write_text(
            integration_compose_good.replace('profiles: ["sfu"]', 'profiles: ["broker"]', 1),
            encoding="utf-8",
        )
        failures = check_integration_compose_gate_d_profiles(root)
        assert any("sfu profile must cover" in failure for failure in failures), failures

        (root / "docker-compose.integration.yml").write_text(
            integration_compose_good.replace('"58080:8080"', '"58081:8080"'),
            encoding="utf-8",
        )
        failures = check_integration_compose_gate_d_profiles(root)
        assert any("notification sidecar host port" in failure for failure in failures), failures
        (root / "docker-compose.integration.yml").write_text(integration_compose_good, encoding="utf-8")

        (root / "docker" / "clickhouse" / "config.d" / "keeper.xml").write_text(
            clickhouse_keeper_good.replace(
                "<keeper_map_path_prefix>/udb/keeper_map_tables</keeper_map_path_prefix>",
                "<keeper_map_path_prefix>/tmp/not-udb</keeper_map_path_prefix>",
            ),
            encoding="utf-8",
        )
        failures = check_compose_support_inputs(root)
        assert any("KeeperMap path prefix" in failure for failure in failures), failures
        (root / "docker" / "clickhouse" / "config.d" / "keeper.xml").write_text(
            clickhouse_keeper_good,
            encoding="utf-8",
        )

        (wf / "sidecar-smokes.yml").write_text(sidecar_good, encoding="utf-8")
        (wf / "ffmpeg-transcode-smoke.yml").write_text(
            ffmpeg_good.replace(
                "python scripts/check-vendored-ffmpeg.py --selftest",
                "python scripts/check-vendored-ffmpeg.py --version",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("vendored ffmpeg verifier selftest" in failure for failure in failures), failures

        (wf / "ffmpeg-transcode-smoke.yml").write_text(ffmpeg_good, encoding="utf-8")
        (scripts_dir / "ffmpeg_transcode_smoke.py").write_text(
            "\n".join(needle for needle, _label in FFMPEG_TRANSCODE_SMOKE_REQUIREMENTS).replace(
                "MAX_FFMPEG_COMMAND_TIMEOUT_SECONDS = 300.0",
                "MAX_FFMPEG_COMMAND_TIMEOUT_SECONDS = 3000.0",
            ),
            encoding="utf-8",
        )
        failures = check_ffmpeg_transcode_smoke_contract(root)
        assert any("ffmpeg command timeout ceiling" in failure for failure in failures), failures
        (scripts_dir / "ffmpeg_transcode_smoke.py").write_text(
            "\n".join(needle for needle, _label in FFMPEG_TRANSCODE_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )

        (scripts_dir / "livekit_sfu_smoke.py").write_text(
            "\n".join(needle for needle, _label in LIVEKIT_SFU_SMOKE_REQUIREMENTS).replace(
                "def validate_base_url(",
                "def unchecked_base_url(",
            ),
            encoding="utf-8",
        )
        failures = check_livekit_sfu_smoke_contract(root)
        assert any("LiveKit base URL validator" in failure for failure in failures), failures
        (scripts_dir / "livekit_sfu_smoke.py").write_text(
            "\n".join(needle for needle, _label in LIVEKIT_SFU_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )

        (wf / "rest-gateway-smoke.yml").write_text(
            rest_gateway_good.replace("--boundary-error-code", "--boundary-status-code"),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("REST live error code handoff" in failure for failure in failures), failures

        (wf / "rest-gateway-smoke.yml").write_text(
            rest_gateway_good.replace("            --require-boundary-proof\n", ""),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("REST live success/error boundary proof gate" in failure for failure in failures), failures

        (wf / "rest-gateway-smoke.yml").write_text(rest_gateway_good, encoding="utf-8")
        (wf / "release-binaries.yml").write_text(
            release_binaries_good.replace("needs: vendored-ffmpeg", "needs: version-guard"),
            encoding="utf-8",
        )
        failures = check_release_binaries_ffmpeg_gate(root)
        assert any("binary build waits for ffmpeg gate" in failure for failure in failures), failures

        (wf / "release-binaries.yml").write_text(
            release_binaries_good.replace(
                "node scripts/gen-release-manifest.mjs --selftest",
                "node scripts/gen-release-manifest.mjs --help",
            ),
            encoding="utf-8",
        )
        failures = check_release_binaries_ffmpeg_gate(root)
        assert any("release manifest generator selftest" in failure for failure in failures), failures

        (wf / "release-binaries.yml").write_text(
            release_binaries_good.replace("            asset: udb-linux-amd64-full\n", "            asset: udb-linux-amd64-fat\n"),
            encoding="utf-8",
        )
        failures = check_release_binary_matrix_contract(root)
        assert any("full Linux asset" in failure for failure in failures), failures

        (wf / "release-binaries.yml").write_text(
            release_binaries_good.replace(
                "on:\n  workflow_call:\n  workflow_dispatch:\n",
                "on:\n  push:\n    tags:\n      - 'v*.*.*'\n  workflow_call:\n  workflow_dispatch:\n",
            ),
            encoding="utf-8",
        )
        failures = check_release_binary_matrix_contract(root)
        assert any("release.yml owns tag trigger" in failure for failure in failures), failures
        (wf / "release-binaries.yml").write_text(release_binaries_good, encoding="utf-8")

        (scripts_dir / "gen-release-manifest.mjs").write_text(
            release_manifest_generator_good.replace("sha256 mismatch", "checksum mismatch"),
            encoding="utf-8",
        )
        failures = check_release_manifest_generator_contract(root)
        assert any("stale checksum-sidecar rejection" in failure for failure in failures), failures
        (scripts_dir / "gen-release-manifest.mjs").write_text(
            release_manifest_generator_good,
            encoding="utf-8",
        )

        (wf / "release-crates.yml").write_text(
            release_crates_publisher_good + "\n      - run: cargo test --workspace\n",
            encoding="utf-8",
        )
        failures = check_release_publisher_leaf_contracts(root)
        assert any("must not re-run CI Rust/build/codegen" in failure for failure in failures), failures
        (wf / "release-crates.yml").write_text(release_crates_publisher_good, encoding="utf-8")

        (wf / "release-python-sdk.yml").write_text(
            release_python_publisher_good.replace("--skip-existing ", ""),
            encoding="utf-8",
        )
        failures = check_release_publisher_leaf_contracts(root)
        assert any("PyPI skip-existing publish" in failure for failure in failures), failures
        (wf / "release-python-sdk.yml").write_text(release_python_publisher_good, encoding="utf-8")

        (wf / "release-docker.yml").write_text(
            release_docker_good.replace("chmod +x udb", "cargo build --release --bin udb"),
            encoding="utf-8",
        )
        failures = check_release_docker_single_artifact(root)
        assert any("must not rebuild" in failure for failure in failures), failures
        (wf / "release-docker.yml").write_text(release_docker_good, encoding="utf-8")

        (root / "Dockerfile.release").write_text(
            release_dockerfile_good.replace("ENV UDB_FFMPEG_BIN=/usr/bin/ffmpeg\n", ""),
            encoding="utf-8",
        )
        failures = check_release_dockerfile_contract(root)
        assert any("ffmpeg binary env" in failure for failure in failures), failures
        (root / "Dockerfile.release").write_text(release_dockerfile_good, encoding="utf-8")

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "node scripts/check-launcher-assets.mjs --selftest",
                "node scripts/check-launcher-assets.mjs --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_launcher_asset_gate(root)
        assert any("launcher asset guard selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "          node scripts/check-launcher-assets.mjs\n",
                "          node scripts/check-launcher-assets.mjs --version\n",
            ),
            encoding="utf-8",
        )
        failures = check_ci_launcher_asset_gate(root)
        assert any("launcher asset repo scan" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-sdk-service-coverage.py --selftest",
                "python3 scripts/check-sdk-service-coverage.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_sdk_service_coverage_gate(root)
        assert any("SDK service-coverage selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "          python3 scripts/check-sdk-service-coverage.py\n",
                "          python3 scripts/check-sdk-service-coverage.py --version\n",
            ),
            encoding="utf-8",
        )
        failures = check_ci_sdk_service_coverage_gate(root)
        assert any("SDK service-coverage repo scan" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "  pull_request:\n    branches: [main]",
                "  pull_request:\n    branches: [main]\n    paths:\n      - \"src/**\"",
            ),
            encoding="utf-8",
        )
        failures = check_ci_topology_contract(root)
        assert any("must not be path-filtered" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace("  build-broker:\n    needs: quick-gate", "  build-broker:"),
            encoding="utf-8",
        )
        failures = check_ci_topology_contract(root)
        assert any("build-broker must wait on quick-gate" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace("  scaffold-compiles:\n    needs: build-broker", "  scaffold-compiles:"),
            encoding="utf-8",
        )
        failures = check_ci_topology_contract(root)
        assert any("scaffold-compiles must consume" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "node sdk-conformance/run.mjs metadata error-details typescript python go csharp java php",
                "node sdk-conformance/run.mjs typescript python go csharp java php",
            ),
            encoding="utf-8",
        )
        failures = check_ci_topology_contract(root)
        assert any("SDK alias/operationId metadata + error-detail conformance targets" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace("  native-integration:\n    if: github.event_name == 'push'", "  native-integration:"),
            encoding="utf-8",
        )
        failures = check_ci_topology_contract(root)
        assert any("native-integration must be push-only" in failure for failure in failures), failures

        (root / "docs" / "ci-architecture.md").write_text(
            ci_architecture_good.replace(
                "owned by the post-release benchmark",
                "owned by live-suite[conformance]",
            ),
            encoding="utf-8",
        )
        failures = check_ci_architecture_contract(root)
        assert any("benchmark-owned live SDK coverage" in failure for failure in failures), failures
        (root / "docs" / "ci-architecture.md").write_text(ci_architecture_good, encoding="utf-8")

        (wf / "ci.yml").write_text(
            ci_good + "\n  live-suite:\n    uses: ./.github/workflows/_live-sdk-suite.yml\n",
            encoding="utf-8",
        )
        failures = check_ci_architecture_contract(root)
        assert any("must not call _live-sdk-suite" in failure for failure in failures), failures
        (wf / "ci.yml").write_text(ci_good, encoding="utf-8")

        (root / "docs" / "ci-architecture.md").write_text(
            ci_architecture_good.replace(
                "sdk-conformance, smoke, scaffold-compiles.",
                "sdk-conformance, smoke, scaffold-compiles, actionlint.",
            ),
            encoding="utf-8",
        )
        failures = check_ci_architecture_contract(root)
        assert any("path-filtered actionlint" in failure for failure in failures), failures
        (root / "docs" / "ci-architecture.md").write_text(ci_architecture_good, encoding="utf-8")

        (root / "docs" / "ci-architecture.md").write_text(
            ci_architecture_good + "\nrelease -> live-suite[perf] -> pages -> cleanup\n",
            encoding="utf-8",
        )
        failures = check_ci_architecture_contract(root)
        assert any("workflow_run side effects" in failure for failure in failures), failures
        (root / "docs" / "ci-architecture.md").write_text(ci_architecture_good, encoding="utf-8")

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-vector-cas-posture.py --selftest",
                "python3 scripts/check-vector-cas-posture.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("vector CAS posture selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-orm-template-posture.py --selftest",
                "python3 scripts/check-orm-template-posture.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("ORM template posture selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "          python3 scripts/check-scaffold-posture.py\n",
                "          python3 scripts/check-scaffold-posture.py --version\n",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("scaffold posture repo scan" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-sdk-helper-parity.py --selftest",
                "python3 scripts/check-sdk-helper-parity.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("SDK helper parity selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-gap-closure-posture.py --selftest",
                "python3 scripts/check-gap-closure-posture.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("gap-closure posture selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-bench-harness-posture.py --selftest",
                "python3 scripts/check-bench-harness-posture.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("bench harness posture selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-docs-ci-freshness-posture.py --selftest",
                "python3 scripts/check-docs-ci-freshness-posture.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("docs/CI freshness posture selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-go-sdk-posture.py --selftest",
                "python3 scripts/check-go-sdk-posture.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("Go SDK posture selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-ts-sdk-posture.py --selftest",
                "python3 scripts/check-ts-sdk-posture.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("TypeScript SDK posture selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-python-php-sdk-posture.py --selftest",
                "python3 scripts/check-python-php-sdk-posture.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("Python/PHP SDK posture selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-java-csharp-sdk-audit.py --selftest",
                "python3 scripts/check-java-csharp-sdk-audit.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("Java/C# SDK audit selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-api-sdk-alias-posture.py --selftest",
                "python3 scripts/check-api-sdk-alias-posture.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("API/SDK alias posture selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-openapi-operationid-posture.py --selftest",
                "python3 scripts/check-openapi-operationid-posture.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("OpenAPI operation-id posture selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-idempotency-dedup-posture.py --selftest",
                "python3 scripts/check-idempotency-dedup-posture.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("idempotency dedup posture selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-retry-safe-posture.py --selftest",
                "python3 scripts/check-retry-safe-posture.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("retry-safe mutation posture selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "node scripts/check-http-api-style.mjs --selftest",
                "node scripts/check-http-api-style.mjs --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_http_api_style_gate(root)
        assert any("HTTP API route-style selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace("          node scripts/check-http-api-style.mjs --source-only\n", ""),
            encoding="utf-8",
        )
        failures = check_ci_http_api_style_gate(root)
        assert any("HTTP API source route-style hard gate" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "          node scripts/check-http-api-style.mjs --write-report\n"
                "          git diff --quiet -- docs/generated/http-api-style-exceptions.json docs/generated/http-api-style-exceptions.md\n",
                "",
            ),
            encoding="utf-8",
        )
        failures = check_ci_http_api_style_gate(root)
        assert any("HTTP API exception report freshness diff" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace("          node scripts/check-http-api-style.mjs --resource-identity-contract\n", ""),
            encoding="utf-8",
        )
        failures = check_ci_http_api_style_gate(root)
        assert any("HTTP API resource identity contract hard gate" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace("          node scripts/check-http-api-style.mjs --pagination-contract\n", ""),
            encoding="utf-8",
        )
        failures = check_ci_http_api_style_gate(root)
        assert any("HTTP API pagination contract hard gate" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace("          node scripts/check-http-api-style.mjs --query-update-contract\n", ""),
            encoding="utf-8",
        )
        failures = check_ci_http_api_style_gate(root)
        assert any("HTTP API query/update contract hard gate" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace("          python3 scripts/rest_route_gateway_smoke.py --selftest\n", ""),
            encoding="utf-8",
        )
        failures = check_ci_http_api_style_gate(root)
        assert any("REST route gateway smoke selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace("          python3 scripts/rest_route_gateway_smoke.py --check-openapi\n", ""),
            encoding="utf-8",
        )
        failures = check_ci_http_api_style_gate(root)
        assert any("REST route gateway smoke OpenAPI check" in failure for failure in failures), failures

        (scripts_dir / "check-http-api-style.mjs").write_text(
            http_api_style_guard_good.replace("api_keys snake_case regression was not caught", "api keys regression"),
            encoding="utf-8",
        )
        failures = check_http_api_style_guard_contract(root)
        assert any("snake_case negative fixture" in failure for failure in failures), failures
        (scripts_dir / "check-http-api-style.mjs").write_text(http_api_style_guard_good, encoding="utf-8")

        rest_route_gateway_good = "\n".join(
            needle for needle, _label in REST_ROUTE_GATEWAY_SMOKE_REQUIREMENTS
        )
        (scripts_dir / "rest_route_gateway_smoke.py").write_text(
            rest_route_gateway_good.replace("def check_live_gateway(", "def check_live_routes("),
            encoding="utf-8",
        )
        failures = check_rest_route_gateway_smoke_contract(root)
        assert any("live canonical/retired route-family checker" in failure for failure in failures), failures
        (scripts_dir / "rest_route_gateway_smoke.py").write_text(
            rest_route_gateway_good.replace("def check_live_boundary(", "def check_route_only("),
            encoding="utf-8",
        )
        failures = check_rest_route_gateway_smoke_contract(root)
        assert any("live REST success/error boundary checker" in failure for failure in failures), failures
        (scripts_dir / "rest_route_gateway_smoke.py").write_text(
            rest_route_gateway_good.replace("CANONICAL_GRPC_ERROR_CODES", "GRPC_ERROR_CODES"),
            encoding="utf-8",
        )
        failures = check_rest_route_gateway_smoke_contract(root)
        assert any("REST canonical gRPC error-code allowlist" in failure for failure in failures), failures
        (scripts_dir / "rest_route_gateway_smoke.py").write_text(
            rest_route_gateway_good.replace("LIVE_BOUNDARY_HTTP_METHODS", "LIVE_HTTP_METHODS"),
            encoding="utf-8",
        )
        failures = check_rest_route_gateway_smoke_contract(root)
        assert any("REST live boundary method allowlist" in failure for failure in failures), failures
        (scripts_dir / "rest_route_gateway_smoke.py").write_text(rest_route_gateway_good, encoding="utf-8")

        beta_versioning_posture_good = "\n".join(
            needle for needle, _label in BETA_VERSIONING_POSTURE_REQUIREMENTS
        )
        (scripts_dir / "check-beta-versioning-posture.py").write_text(
            beta_versioning_posture_good.replace(
                "return operation_id or api_alias or wire_api",
                "return wire_api or operation_id or api_alias",
            ),
            encoding="utf-8",
        )
        failures = check_beta_versioning_posture_contract(root)
        assert any("benchmark collector canonical API identity" in failure for failure in failures), failures
        (scripts_dir / "check-beta-versioning-posture.py").write_text(
            beta_versioning_posture_good,
            encoding="utf-8",
        )
        (wf / "ci.yml").write_text(ci_good, encoding="utf-8")

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-beta-versioning-posture.py --selftest",
                "python3 scripts/check-beta-versioning-posture.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_quick_gate_source_guards(root)
        assert any("beta versioning posture selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "python3 scripts/check-doc-service-counts.py --selftest",
                "python3 scripts/check-doc-service-counts.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_public_docs_guards(root)
        assert any("doc service-count drift selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "      - name: No internal tables guard (masterplan §12)\n        if: runner.os == 'Linux'",
                "      - name: No internal tables guard (masterplan §12)",
            ),
            encoding="utf-8",
        )
        failures = check_ci_public_docs_guards(root)
        assert any("Linux-only gate for no internal tables" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "node scripts/check-markdown-links.mjs",
                "node scripts/check-docs.mjs",
            ),
            encoding="utf-8",
        )
        failures = check_ci_docs_links_gate(root)
        assert any("markdown local-link guard command" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                '          node: "true"\n',
                "",
            ),
            encoding="utf-8",
        )
        failures = check_ci_docs_links_gate(root)
        assert any("docs-links Node toolchain enablement" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "node scripts/check-markdown-links.mjs --selftest",
                "node scripts/check-markdown-links.mjs --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_docs_links_gate(root)
        assert any("markdown link selftest command" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "node scripts/check-enterprise-readiness.mjs --selftest",
                "node scripts/check-enterprise-readiness.mjs --help",
            ),
            encoding="utf-8",
        )
        failures = check_ci_docs_links_gate(root)
        assert any("enterprise readiness selftest command" in failure for failure in failures), failures

        (scripts_dir / "check-markdown-links.mjs").write_text(
            markdown_links_guard_good.replace('  "private",\n', ""),
            encoding="utf-8",
        )
        failures = check_markdown_link_guard_contract(root)
        assert any("private research directory exclusion" in failure for failure in failures), failures
        (scripts_dir / "check-markdown-links.mjs").write_text(markdown_links_guard_good, encoding="utf-8")

        (scripts_dir / "check-markdown-links.mjs").write_text(
            markdown_links_guard_good.replace("stripFencedCodeBlocks", "keepFencedCodeBlocks"),
            encoding="utf-8",
        )
        failures = check_markdown_link_guard_contract(root)
        assert any("fenced code block stripping helper" in failure for failure in failures), failures
        (scripts_dir / "check-markdown-links.mjs").write_text(markdown_links_guard_good, encoding="utf-8")

        (scripts_dir / "check-markdown-links.mjs").write_text(
            markdown_links_guard_good.replace('if (process.argv.includes("--selftest")) runSelftest();\n', ""),
            encoding="utf-8",
        )
        failures = check_markdown_link_guard_contract(root)
        assert any("markdown link selftest CLI" in failure for failure in failures), failures
        (scripts_dir / "check-markdown-links.mjs").write_text(markdown_links_guard_good, encoding="utf-8")

        (scripts_dir / "check-enterprise-readiness.mjs").write_text(
            enterprise_readiness_guard_good.replace('if (process.argv.includes("--selftest")) runSelftest();\n', ""),
            encoding="utf-8",
        )
        failures = check_enterprise_readiness_guard_contract(root)
        assert any("enterprise readiness selftest CLI" in failure for failure in failures), failures
        (scripts_dir / "check-enterprise-readiness.mjs").write_text(enterprise_readiness_guard_good, encoding="utf-8")

        (scripts_dir / "check-openapi-api-rules.mjs").write_text(
            openapi_api_rule_guard_good.replace("if (process.argv.includes('--selftest')) runSelftest();\n", ""),
            encoding="utf-8",
        )
        failures = check_openapi_api_rule_guard_contract(root)
        assert any("OpenAPI API-rule selftest CLI" in failure for failure in failures), failures
        (scripts_dir / "check-openapi-api-rules.mjs").write_text(openapi_api_rule_guard_good, encoding="utf-8")

        (scripts_dir / "ci-inventory.mjs").write_text(
            ci_inventory_guard_good.replace('if (process.argv.includes("--selftest")) runSelftest();\n', ""),
            encoding="utf-8",
        )
        failures = check_ci_inventory_guard_contract(root)
        assert any("CI inventory selftest CLI" in failure for failure in failures), failures
        (scripts_dir / "ci-inventory.mjs").write_text(ci_inventory_guard_good, encoding="utf-8")

        (scripts_dir / "ci-inventory.mjs").write_text(
            ci_inventory_guard_good.replace("const dependencyFreePrJobs = [];\n", ""),
            encoding="utf-8",
        )
        failures = check_ci_inventory_guard_contract(root)
        assert any("dependency-free PR job inventory" in failure for failure in failures), failures
        (scripts_dir / "ci-inventory.mjs").write_text(ci_inventory_guard_good, encoding="utf-8")

        (scripts_dir / "check-branch-protection-lockstep.mjs").write_text(
            branch_protection_lockstep_good.replace("missingInBranchProtection", "missingInDocs"),
            encoding="utf-8",
        )
        failures = check_branch_protection_lockstep_guard(root)
        assert any("missing required-check failure" in failure for failure in failures), failures
        (scripts_dir / "check-branch-protection-lockstep.mjs").write_text(
            branch_protection_lockstep_good,
            encoding="utf-8",
        )
        (scripts_dir / "check-branch-protection-lockstep.mjs").write_text(
            branch_protection_lockstep_good.replace("function branchArg(args, name, fallback)", "function branchToken(args, name, fallback)"),
            encoding="utf-8",
        )
        failures = check_branch_protection_lockstep_guard(root)
        assert any("branch CLI validator" in failure for failure in failures), failures
        (scripts_dir / "check-branch-protection-lockstep.mjs").write_text(
            branch_protection_lockstep_good,
            encoding="utf-8",
        )

        (scripts_dir / "check-ci-runner-evidence.mjs").write_text(
            ci_runner_evidence_good.replace("exactly one build-broker job", "at least one build-broker job"),
            encoding="utf-8",
        )
        failures = check_ci_runner_evidence_guard(root)
        assert any("single build-broker failure" in failure for failure in failures), failures
        (scripts_dir / "check-ci-runner-evidence.mjs").write_text(
            ci_runner_evidence_good,
            encoding="utf-8",
        )
        (scripts_dir / "check-ci-runner-evidence.mjs").write_text(
            ci_runner_evidence_good.replace("DEFAULT_MAX_EVIDENCE_AGE_DAYS = 14", "DEFAULT_MAX_EVIDENCE_AGE_DAYS = 365"),
            encoding="utf-8",
        )
        failures = check_ci_runner_evidence_guard(root)
        assert any("runner evidence max-age default" in failure for failure in failures), failures
        (scripts_dir / "check-ci-runner-evidence.mjs").write_text(
            ci_runner_evidence_good,
            encoding="utf-8",
        )

        (scripts_dir / "error_detail_served_smoke.py").write_text(
            "\n".join(needle for needle, _label in ERROR_DETAIL_SERVED_SMOKE_REQUIREMENTS).replace(
                "retry_after_ms",
                "retry_delay",
            ),
            encoding="utf-8",
        )
        failures = check_error_detail_served_smoke_contract(root)
        assert any("retry-after assertion" in failure for failure in failures), failures
        (scripts_dir / "error_detail_served_smoke.py").write_text(
            "\n".join(needle for needle, _label in ERROR_DETAIL_SERVED_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )
        (scripts_dir / "error_detail_served_smoke.py").write_text(
            "\n".join(needle for needle, _label in ERROR_DETAIL_SERVED_SMOKE_REQUIREMENTS).replace(
                "expected exactly one udb-error-detail-bin trailer",
                "using first udb-error-detail-bin trailer",
            ),
            encoding="utf-8",
        )
        failures = check_error_detail_served_smoke_contract(root)
        assert any("duplicate ErrorDetail trailer assertion" in failure for failure in failures), failures
        (scripts_dir / "error_detail_served_smoke.py").write_text(
            "\n".join(needle for needle, _label in ERROR_DETAIL_SERVED_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )

        (scripts_dir / "idempotency_served_replay_smoke.py").write_text(
            "\n".join(needle for needle, _label in IDEMPOTENCY_SERVED_SMOKE_REQUIREMENTS).replace(
                "def check_batch_replay(",
                "def check_batch_only(",
            ),
            encoding="utf-8",
        )
        failures = check_idempotency_served_smoke_contract(root)
        assert any("BatchUpsert replay checker" in failure for failure in failures), failures
        (scripts_dir / "idempotency_served_replay_smoke.py").write_text(
            "\n".join(needle for needle, _label in IDEMPOTENCY_SERVED_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )
        (scripts_dir / "idempotency_served_replay_smoke.py").write_text(
            "\n".join(needle for needle, _label in IDEMPOTENCY_SERVED_SMOKE_REQUIREMENTS).replace(
                'validate_upsert_payload("BatchUpsert proof first request", first)',
                'validate_keyed_upsert("BatchUpsert proof first request", first)',
            ),
            encoding="utf-8",
        )
        failures = check_idempotency_served_smoke_contract(root)
        assert any("BatchUpsert first payload JSON-object validator" in failure for failure in failures), failures
        (scripts_dir / "idempotency_served_replay_smoke.py").write_text(
            "\n".join(needle for needle, _label in IDEMPOTENCY_SERVED_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )

        (scripts_dir / "retry_safe_served_smoke.py").write_text(
            "\n".join(needle for needle, _label in RETRY_SAFE_SERVED_SMOKE_REQUIREMENTS).replace(
                "replay-safe mutation without idempotency key must not retry",
                "replay-safe mutation without idempotency key may retry",
            ),
            encoding="utf-8",
        )
        failures = check_retry_safe_served_smoke_contract(root)
        assert any("missing-key no-retry assertion" in failure for failure in failures), failures
        (scripts_dir / "retry_safe_served_smoke.py").write_text(
            "\n".join(needle for needle, _label in RETRY_SAFE_SERVED_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )
        (scripts_dir / "retry_safe_served_smoke.py").write_text(
            "\n".join(needle for needle, _label in RETRY_SAFE_SERVED_SMOKE_REQUIREMENTS).replace(
                "Upsert proof record_json must be a JSON object",
                "Upsert proof record_json may be any JSON value",
            ),
            encoding="utf-8",
        )
        failures = check_retry_safe_served_smoke_contract(root)
        assert any("retry-safe Upsert JSON object validator" in failure for failure in failures), failures
        (scripts_dir / "retry_safe_served_smoke.py").write_text(
            "\n".join(needle for needle, _label in RETRY_SAFE_SERVED_SMOKE_REQUIREMENTS),
            encoding="utf-8",
        )

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "          cargo run --locked -q --bin udb -- native lint\n",
                "          cargo run --locked -q --bin udb -- native help\n",
            ),
            encoding="utf-8",
        )
        failures = check_ci_rust_generated_contract_doc_gates(root)
        assert any("native contract manifest drift/lint" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "run: python3 scripts/generate-codebase-map.py --check",
                "run: python3 scripts/generate-codebase-map.py",
            ),
            encoding="utf-8",
        )
        failures = check_ci_rust_generated_contract_doc_gates(root)
        assert any("codebase map freshness" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "      - name: Native contract breaking-change gate (Phase 3)\n        if: runner.os == 'Linux'",
                "      - name: Native contract breaking-change gate (Phase 3)",
            ),
            encoding="utf-8",
        )
        failures = check_ci_rust_generated_contract_doc_gates(root)
        assert any("Linux-only gate for native contract breaking-change" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace("buf generate --include-imports", "buf generate"),
            encoding="utf-8",
        )
        failures = check_ci_buf_generated_artifact_gate(root)
        assert any("include-imports SDK/API generation" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "          node scripts/check-openapi-api-rules.mjs --selftest\n",
                "",
            ),
            encoding="utf-8",
        )
        failures = check_ci_buf_generated_artifact_gate(root)
        assert any("OpenAPI API-rule selftest" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace("          node scripts/generate-authn-authz-inventory.mjs\n", ""),
            encoding="utf-8",
        )
        failures = check_ci_buf_generated_artifact_gate(root)
        assert any("authn/authz inventory generator" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "--input /tmp/native-load.txt",
                "--input /tmp/other-load.txt",
            ),
            encoding="utf-8",
        )
        failures = check_ci_smoke_load_gate(root)
        assert any("native load gate input" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace(
                "      - name: Upload load summary\n        if: always()",
                "      - name: Upload load summary",
            ),
            encoding="utf-8",
        )
        failures = check_ci_smoke_load_gate(root)
        assert any("load summary upload" in failure for failure in failures), failures

        (scripts_dir / "native-load-test.sh").write_text(
            native_load_script_good.replace('run_case "webrtc signal fan-out"', '# skipped "webrtc signal fan-out"'),
            encoding="utf-8",
        )
        failures = check_native_load_case_contract(root)
        assert any("script missing required case" in failure for failure in failures), failures
        (scripts_dir / "native-load-test.sh").write_text(native_load_script_good, encoding="utf-8")

        baseline_without_cdc = json.loads(native_load_baseline_good)
        del baseline_without_cdc["cases"]["cdc dlq throughput (rejected events)"]
        (scripts_dir / "native_load_smoke_baseline.json").write_text(
            json.dumps(baseline_without_cdc),
            encoding="utf-8",
        )
        failures = check_native_load_case_contract(root)
        assert any("baseline missing required case" in failure for failure in failures), failures
        (scripts_dir / "native_load_smoke_baseline.json").write_text(native_load_baseline_good, encoding="utf-8")

        (wf / "ci.yml").write_text(
            ci_good.replace("          UDB_ELASTIC_DSN: x\n", ""),
            encoding="utf-8",
        )
        failures = check_ci_native_integration_gate(root)
        assert any("canonical Elasticsearch DSN" in failure for failure in failures), failures

        (wf / "ci.yml").write_text(
            ci_good.replace("        if: always()\n        run: |\n          docker compose -f docker-compose.integration.yml down", "        run: |\n          docker compose -f docker-compose.integration.yml down"),
            encoding="utf-8",
        )
        failures = check_ci_native_integration_gate(root)
        assert any("cleanup must run with if: always" in failure for failure in failures), failures

        (wf / "benchmark-sdks.yml").write_text(
            benchmark_orchestrator_good.replace(
                "github.event.workflow_run.conclusion == 'success'",
                "github.event.workflow_run.conclusion != 'failure'",
            ),
            encoding="utf-8",
        )
        failures = check_benchmark_orchestrator_gate(root)
        assert any("successful release gate" in failure for failure in failures), failures
        (wf / "benchmark-sdks.yml").write_text(benchmark_orchestrator_good, encoding="utf-8")

        (wf / "benchmark-sdks.yml").write_text(
            benchmark_orchestrator_good.replace(
                '      - "scripts/collect_sdk_bench_results.py"\n',
                "",
            ),
            encoding="utf-8",
        )
        failures = check_benchmark_orchestrator_gate(root)
        assert any("benchmark collector trigger path" in failure for failure in failures), failures
        (wf / "benchmark-sdks.yml").write_text(benchmark_orchestrator_good, encoding="utf-8")

        (wf / "_live-sdk-suite.yml").write_text(
            live_sdk_suite_good.replace(
                'gh release download "${tag}" --repo "${GITHUB_REPOSITORY}" --pattern "${RELEASE_ASSET}" --dir bench-output/bin',
                "cargo build --release --bin udb",
            ),
            encoding="utf-8",
        )
        failures = check_benchmark_workflow_gate(root)
        assert any("release asset download command" in failure for failure in failures), failures
        assert any("must consume a release binary" in failure for failure in failures), failures

        (wf / "_live-sdk-suite.yml").write_text(live_sdk_suite_good, encoding="utf-8")
        (wf / "_live-sdk-suite.yml").write_text(
            live_sdk_suite_good.replace(
                "python scripts/collect_sdk_bench_results.py --gate docs/site/bench-results.json",
                "python scripts/collect_sdk_bench_results.py docs/site/bench-results.json",
            ),
            encoding="utf-8",
        )
        failures = check_benchmark_workflow_gate(root)
        assert any("central benchmark failure gate command" in failure for failure in failures), failures

        (wf / "_live-sdk-suite.yml").write_text(
            live_sdk_suite_good.replace(
                "Upload benchmark report artifact",
                "Archive benchmark report artifact",
            ),
            encoding="utf-8",
        )
        failures = check_benchmark_workflow_gate(root)
        assert any("benchmark artifact upload step" in failure for failure in failures), failures

        (wf / "pages.yml").write_text(
            pages_good.replace(
                "node scripts/playground_wasm_smoke.mjs docs/site/udb.wasm",
                "node scripts/other_smoke.mjs docs/site/udb.wasm",
            ),
            encoding="utf-8",
        )
        failures = check_pages_playground_wasm_gate(root)
        assert any("playground smoke command" in failure for failure in failures), failures

        (wf / "pages.yml").write_text(
            pages_good.replace(
                "          test -f docs/site/api/udb-broker.swagger.json\n",
                "",
            ),
            encoding="utf-8",
        )
        failures = check_pages_playground_wasm_gate(root)
        assert any("published Swagger artifact check" in failure for failure in failures), failures

        (wf / "pages.yml").write_text(
            pages_good.replace(
                "          test -f docs/site/benchmarks.js\n",
                "",
            ),
            encoding="utf-8",
        )
        failures = check_pages_playground_wasm_gate(root)
        assert any("published benchmark script artifact check" in failure for failure in failures), failures

        (wf / "pages.yml").write_text(
            pages_good.replace(
                "assert not missing",
                "assert missing is not None",
            ),
            encoding="utf-8",
        )
        failures = check_pages_playground_wasm_gate(root)
        assert any("HTML local-ref hard failure" in failure for failure in failures), failures

        (root / "docs" / "site" / "README.md").write_text(
            pages_readme_good.replace(
                "rebuilds `udb.wasm`",
                "ships the checked-in `udb.wasm`",
            ),
            encoding="utf-8",
        )
        failures = check_pages_playground_wasm_gate(root)
        assert any("README fresh WASM build contract" in failure for failure in failures), failures
        (root / "docs" / "site" / "README.md").write_text(pages_readme_good, encoding="utf-8")

        (wf / "pages.yml").write_text(
            pages_good.replace(
                "--name sdk-benchmark-results",
                "--name stale-benchmark-results",
            ),
            encoding="utf-8",
        )
        failures = check_pages_playground_wasm_gate(root)
        assert any("benchmark artifact name" in failure for failure in failures), failures

        benchmark_block = """      - name: Pull latest benchmark results into the site
        env:
          GH_TOKEN: ${{ github.token }}
          TRIGGER_RUN_ID: ${{ github.event.workflow_run.id }}
        run: |
          got_fresh=0
          gh run download "${TRIGGER_RUN_ID}" --repo "${GITHUB_REPOSITORY}" --name sdk-benchmark-results --dir bench-artifact
          cp -v bench-artifact/docs/site/bench-results.json docs/site/bench-results.json
          got_fresh=1
          if [ "$got_fresh" != 1 ]; then echo "keeping committed docs/site/bench-results.json"; fi
"""
        (wf / "pages.yml").write_text(
            pages_good.replace(benchmark_block, "").replace(
                "      - uses: actions/upload-pages-artifact@v3",
                benchmark_block + "      - uses: actions/upload-pages-artifact@v3",
            ),
            encoding="utf-8",
        )
        failures = check_pages_playground_wasm_gate(root)
        assert any("Pages must sync assets/API, pull benchmark JSON" in failure for failure in failures), failures

        (wf / "pages.yml").write_text(pages_good, encoding="utf-8")
        (wf / "pages.yml").write_text(
            pages_good.replace('              "x-udb-sdk-alias",\n', ""),
            encoding="utf-8",
        )
        failures = check_pages_playground_wasm_gate(root)
        assert any("Swagger SDK alias extension validation" in failure for failure in failures), failures

        (wf / "pages.yml").write_text(
            pages_good.replace(' or "api_alias" not in row', ""),
            encoding="utf-8",
        )
        failures = check_pages_playground_wasm_gate(root)
        assert any("benchmark public identity row validation" in failure for failure in failures), failures

        (wf / "pages.yml").write_text(pages_good, encoding="utf-8")
        (scripts_dir / "playground_wasm_smoke.mjs").write_text(
            playground_smoke_good.replace(
                'col.field === "mobile" && col.column === "mobile"',
                'col.field === "email" && col.column === "email"',
            ),
            encoding="utf-8",
        )
        failures = check_pages_playground_wasm_gate(root)
        assert any("edited mobile column assertion" in failure for failure in failures), failures

        (scripts_dir / "playground_wasm_smoke.mjs").write_text(playground_smoke_good, encoding="utf-8")
        (root / "docs" / "site" / "playground.html").write_text(
            playground_html_good.replace(
                "./playground.js?v=20260701-current-editor",
                "./playground.js?v=20260619-field-column",
            ),
            encoding="utf-8",
        )
        failures = check_pages_playground_wasm_gate(root)
        assert any("current playground script cache key" in failure for failure in failures), failures

        (root / "docs" / "site" / "playground.html").write_text(playground_html_good, encoding="utf-8")
        (root / "docs" / "site" / "playground.js").write_text(
            playground_js_good.replace(
                'var WASM_ASSET_VERSION = "20260701-current-editor";',
                'var WASM_ASSET_VERSION = "20260628-current-editor";',
            ),
            encoding="utf-8",
        )
        failures = check_pages_playground_wasm_gate(root)
        assert any("current wasm asset cache key" in failure for failure in failures), failures

        (root / "docs" / "site" / "playground.js").write_text(playground_js_good, encoding="utf-8")
        (wf / "_live-sdk-suite.yml").write_text(live_sdk_suite_good, encoding="utf-8")
        (wf / "ci.yml").write_text(ci_good, encoding="utf-8")
        (wf / "release-binaries.yml").write_text(release_binaries_good, encoding="utf-8")
        (wf / "sfu-smoke.yml").write_text(
            sfu_good.replace(
                "python scripts/livekit_sfu_smoke.py --selftest",
                "python scripts/livekit_sfu_smoke.py --help",
            ),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("LiveKit smoke harness selftest" in failure for failure in failures), failures

        (wf / "sfu-smoke.yml").write_text(sfu_good, encoding="utf-8")
        (wf / "webauthn-smoke.yml").write_text(
            webauthn_good.replace("--features webauthn", "--features oidc"),
            encoding="utf-8",
        )
        failures = check_targeted_proof_workflows(root)
        assert any("WebAuthn policy/attestation test target" in failure for failure in failures), failures

        (wf / "release-typescript-sdk.yml").write_text(
            release_leaf_good.replace(
                "on:\n  workflow_call:\n",
                "on:\n  push:\n    tags:\n      - 'v*.*.*'\n  workflow_call:\n",
            ),
            encoding="utf-8",
        )
        failures = check_release_topology(root)
        assert any("release leaf must not define its own push tag trigger" in failure for failure in failures), failures
        (wf / "release-typescript-sdk.yml").write_text(release_leaf_good, encoding="utf-8")

        (wf / "release-python-sdk.yml").write_text(
            release_leaf_good.replace(
                "on:\n  workflow_call:\n",
                "on:\n  workflow_call:\n  workflow_dispatch:\n",
            ),
            encoding="utf-8",
        )
        failures = check_release_topology(root)
        assert any("workflow_dispatch input 'version' is missing" in failure for failure in failures), failures
        (wf / "release-python-sdk.yml").write_text(release_leaf_good, encoding="utf-8")

        (wf / "release-csharp-sdk.yml").write_text(
            release_leaf_good.replace(
                "on:\n  workflow_call:\n",
                "on:\n  workflow_call:\n  workflow_dispatch:\n    inputs:\n      version:\n        description: \"Expected UDB release version\"\n        required: true\n",
            ),
            encoding="utf-8",
        )
        failures = check_release_topology(root)
        assert any("manual release version guard" in failure for failure in failures), failures
        (wf / "release-csharp-sdk.yml").write_text(release_leaf_good, encoding="utf-8")

        (wf / "release.yml").write_text(
            release_topology_good.replace("    needs: build-binaries\n", ""),
            encoding="utf-8",
        )
        failures = check_release_topology(root)
        assert any("publish-crates must wait for build-binaries" in failure for failure in failures), failures
        (wf / "release.yml").write_text(release_topology_good, encoding="utf-8")

        (wf / "cleanup-packages.yml").write_text(
            cleanup_packages_good.replace(
                "      github.event.workflow_run.conclusion == 'success'\n",
                "",
            ),
            encoding="utf-8",
        )
        failures = check_cleanup_packages_ownership(root)
        assert any("successful-release cleanup gate" in failure for failure in failures), failures
        (wf / "cleanup-packages.yml").write_text(cleanup_packages_good, encoding="utf-8")

        (wf / "release-docker.yml").write_text(
            release_docker_good + "\n      - uses: actions/delete-package-versions@v5\n",
            encoding="utf-8",
        )
        failures = check_cleanup_packages_ownership(root)
        assert any("package deletion must stay owned by cleanup-packages.yml" in failure for failure in failures), failures
        (wf / "release-docker.yml").write_text(release_docker_good, encoding="utf-8")

        (wf / "publish-skill.yml").write_text(
            publish_skill_good.replace("  ollama:\n    needs: validate\n", "  ollama:\n"),
            encoding="utf-8",
        )
        failures = check_publish_skill_workflow(root)
        assert any("ollama skill publish job must wait for validate" in failure for failure in failures), failures
        (wf / "publish-skill.yml").write_text(publish_skill_good, encoding="utf-8")

        (wf / "_shadow-live-sdk.yml").write_text(
            shadow_live_sdk_good.replace("  workflow_dispatch:", "  push:\n    branches: [main]\n  workflow_dispatch:"),
            encoding="utf-8",
        )
        failures = check_shadow_live_sdk_workflow(root)
        assert any("manual-only" in failure for failure in failures), failures
        (wf / "_shadow-live-sdk.yml").write_text(shadow_live_sdk_good, encoding="utf-8")

        (wf / "_selftest.yml").write_text(
            composite_selftest_good.replace('          kafka: "true"\n', ""),
            encoding="utf-8",
        )
        failures = check_composite_selftest_workflow(root)
        assert any("Kafka selftest backend" in failure for failure in failures), failures
        (wf / "_selftest.yml").write_text(composite_selftest_good, encoding="utf-8")

        launch_action = root / ".github" / "actions" / "launch-broker" / "action.yml"
        launch_action.write_text(
            launch_action.read_text(encoding="utf-8").replace("AUTH_PORT=$((PORT + 10))\n", ""),
            encoding="utf-8",
        )
        failures = check_composite_action_contracts(root)
        assert any("auth listener port derivation" in failure for failure in failures), failures
        launch_action.write_text(
            "\n".join(needle for needle, _label in COMPOSITE_ACTION_SOURCE_REQUIREMENTS[".github/actions/launch-broker/action.yml"]),
            encoding="utf-8",
        )

        (wf / "release.yml").write_text(
            "permissions:\n  pages: write\nsteps:\n  - uses: actions/deploy-pages@v4\n",
            encoding="utf-8",
        )
        failures = check_pages_single_owner(root)
        assert any("Pages deploy" in failure for failure in failures), failures

        (wf / "lint-workflows.yml").write_text(
            lint_good.replace('      - "scripts/playground_wasm_smoke.mjs"\n', ""),
            encoding="utf-8",
        )
        failures = check_lint_workflow_trigger_paths(root)
        assert any("playground WASM smoke trigger path" in failure for failure in failures), failures

        (wf / "lint-workflows.yml").write_text(
            lint_good.replace('      - "scripts/check-versions.mjs"\n', ""),
            encoding="utf-8",
        )
        failures = check_lint_workflow_trigger_paths(root)
        assert any("version guard trigger path" in failure for failure in failures), failures

        (wf / "lint-workflows.yml").write_text(
            lint_good.replace("          python3 scripts/native_load_gate.py --selftest\n", ""),
            encoding="utf-8",
        )
        failures = check_lint_workflow_trigger_paths(root)
        assert any("native load gate selftest" in failure for failure in failures), failures

        (wf / "new-helper.yml").write_text(
            "steps:\n  - run: python scripts/new_workflow_helper.py\n",
            encoding="utf-8",
        )
        failures = check_lint_workflow_covers_referenced_helpers(root)
        assert any("scripts/new_workflow_helper.py" in failure for failure in failures), failures
        (wf / "new-helper.yml").unlink()

        (wf / "lint-workflows.yml").write_text(
            # Drop the workflow-files glob from the pull_request block ONLY.
            # Anchoring on the full rendered paths list (instead of assuming
            # ".github/workflows/**" is the FIRST entry) keeps this case valid
            # when new paths are prepended to LINT_WORKFLOW_TRIGGER_PATHS.
            lint_good.replace(
                f"  pull_request:\n    paths:\n{lint_paths}",
                "  pull_request:\n    paths:\n"
                + lint_paths.replace(
                    '      - ".github/workflows/**"',
                    '      - "scripts/check-workflow-posture.py"',
                ),
            ),
            encoding="utf-8",
        )
        failures = check_lint_workflow_trigger_paths(root)
        assert any("workflow files trigger path in pull_request" in failure for failure in failures), failures

    print("workflow posture selftest passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run no-repo assertions")
    args = parser.parse_args(argv)
    if args.selftest:
        return run_selftest()

    failures = (
        check_proof_workflows()
        + check_resilience_smoke_workflow()
        + check_xa_recovery_smoke_script()
        + check_sidecar_smoke_workflow()
        + check_sidecar_roundtrip_scripts()
        + check_sidecar_container_sources()
        + check_integration_compose_gate_d_profiles()
        + check_compose_support_inputs()
        + check_targeted_proof_workflows()
        + check_ffmpeg_transcode_smoke_contract()
        + check_livekit_sfu_smoke_contract()
        + check_release_topology()
        + check_release_binaries_ffmpeg_gate()
        + check_release_binary_matrix_contract()
        + check_release_manifest_generator_contract()
        + check_release_publisher_leaf_contracts()
        + check_release_docker_single_artifact()
        + check_release_dockerfile_contract()
        + check_ci_launcher_asset_gate()
        + check_ci_sdk_service_coverage_gate()
        + check_ci_topology_contract()
        + check_ci_architecture_contract()
        + check_ci_quick_gate_source_guards()
        + check_ci_public_docs_guards()
        + check_ci_docs_links_gate()
        + check_markdown_link_guard_contract()
        + check_enterprise_readiness_guard_contract()
        + check_openapi_api_rule_guard_contract()
        + check_http_api_style_guard_contract()
        + check_rest_route_gateway_smoke_contract()
        + check_beta_versioning_posture_contract()
        + check_ci_http_api_style_gate()
        + check_ci_inventory_guard_contract()
        + check_branch_protection_lockstep_guard()
        + check_ci_runner_evidence_guard()
        + check_error_detail_served_smoke_contract()
        + check_idempotency_served_smoke_contract()
        + check_retry_safe_served_smoke_contract()
        + check_ci_rust_generated_contract_doc_gates()
        + check_ci_buf_generated_artifact_gate()
        + check_ci_smoke_load_gate()
        + check_native_load_case_contract()
        + check_ci_native_integration_gate()
        + check_benchmark_orchestrator_gate()
        + check_benchmark_workflow_gate()
        + check_pages_playground_wasm_gate()
        + check_pages_single_owner()
        + check_cleanup_packages_ownership()
        + check_publish_skill_workflow()
        + check_shadow_live_sdk_workflow()
        + check_composite_selftest_workflow()
        + check_composite_action_contracts()
        + check_lint_workflow_trigger_paths()
        + check_lint_workflow_covers_referenced_helpers()
    )
    if failures:
        for failure in failures:
            print(f"::error::{failure}", file=sys.stderr)
        return 1
    print("workflow posture guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
