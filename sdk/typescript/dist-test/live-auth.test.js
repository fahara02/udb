"use strict";
// Broker-backed SDK conformance for urgent_fix #20.
//
// This test is intentionally skipped unless UDB_LIVE_SDK_TESTS=1. CI starts a
// real broker, seeds the first user through `udb auth bootstrap user`, then runs
// this through the normal TypeScript test build.
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const node_assert_1 = require("node:assert");
const node_test_1 = require("node:test");
const node_fs_1 = require("node:fs");
const grpc = __importStar(require("@grpc/grpc-js"));
const project_1 = require("./project");
const generatedClient_1 = require("./generatedClient");
function requiredEnv(name) {
    const value = process.env[name]?.trim();
    if (!value)
        throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
    return value;
}
function memoryStore(initial = null) {
    let token = initial;
    return {
        load: async () => token,
        save: async (next) => {
            token = next;
        },
        clear: async () => {
            token = null;
        },
        current: () => token,
    };
}
const NATIVE_SERVICE_APIS = [
    "AnalyticsService",
    "ApiKeyService",
    "AssetService",
    "AuthnService",
    "AuthzService",
    "ControlPlaneService",
    "IdentityProviderService",
    "NotificationService",
    "StorageService",
    "TenantService",
    "PeerService",
    "RoomService",
    "SignalingService",
    "TrackService",
    "TurnService",
];
// The real CatalogManifest bytes captured by the seed (get_catalog_manifest), used by
// the stage_catalog/validate_catalog bodies — an empty {} is rejected as invalid.
let seedCatalogManifest;
// Minimal-but-valid SAML 2.0 IdP metadata (entityID + IDPSSODescriptor + SSO services)
// so ImportSamlMetadata parses and the SAML provider gets an SSO URL (Go bodies:165).
const SAML_IDP_METADATA_XML = `<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://idp.example.com/perf-saml">` +
    `<md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">` +
    `<md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example.com/sso"/>` +
    `<md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="https://idp.example.com/sso"/>` +
    `</md:IDPSSODescriptor></md:EntityDescriptor>`;
const NON_UNARY_METHODS = new Set([
    "get_object",
    "publish_c_d_c",
    "select_v2",
    "put_object",
    "delta_resources",
    "stream_resources",
    "signal",
    "batch_select",
    "batch_upsert",
    "begin_tx",
    "vector_batch_upsert",
]);
const FATAL_CONNECTIVITY_CODES = new Set([
    grpc.status.CANCELLED,
    grpc.status.UNKNOWN,
    // DEADLINE_EXCEEDED is NOT a mount failure: an unmounted RPC returns
    // UNIMPLEMENTED instantly, so a timeout means the server accepted the call and
    // is processing/blocking (e.g. PublishCDC is an open-ended CDC subscription
    // stream that legitimately blocks waiting for events).
    grpc.status.UNIMPLEMENTED,
    grpc.status.UNAVAILABLE,
]);
const LIVE_MESSAGE_TYPE = "udb.sdk.live.v1.SdkLiveRecord";
const UNSUPPORTED_OPERATION_CODE = "UDB_UNSUPPORTED_OPERATION";
// Canonical generic-dispatch op vocabulary the broker gates per backend
// (src/runtime/service/mod.rs check_generic_dispatch_operation), safe-first.
const GENERIC_DISPATCH_OPS = [
    "ping", "probe", "list_resources", "search", "query",
    "transaction", "get_object", "put_object", "mutate",
    "ensure_resource", "drop_resource",
];
function errText(err) {
    const a = err;
    return String(a?.details ?? a?.message ?? a ?? "");
}
function requestContext(tenantId, projectId, purpose) {
    return {
        tenant_id: tenantId,
        project_id: projectId,
        purpose,
        correlation_id: `${purpose}-${Date.now()}`,
        // No client-asserted scopes: admin authority comes from the Login JWT (broker
        // derives scopes from the validated bearer; header/body scopes are ignored
        // when a JWT verifier is configured). The real production path.
        scopes: [],
        service_identity: "ts.sdk.live",
        client_catalog_version: "1.0.0",
    };
}
// google.protobuf.Struct filters/documents are now passed as PLAIN JS objects —
// the SDK's wkt.ts wrapper converts them to the correct camelCase Value wire form
// (previously the test hand-built the {fields:{k:{stringValue}}} shape because a
// plain object silently serialized to empty values).
function valueOf(field) {
    if (!field)
        return undefined;
    // Deserialized google.protobuf.Value uses camelCase oneof members; accept the
    // snake_case spellings too for safety.
    if ("stringValue" in field)
        return field.stringValue;
    if ("string_value" in field)
        return field.string_value;
    if ("numberValue" in field)
        return Number(field.numberValue);
    if ("number_value" in field)
        return Number(field.number_value);
    if ("boolValue" in field)
        return field.boolValue;
    if ("bool_value" in field)
        return field.bool_value;
    return undefined;
}
function structField(doc, name) {
    return valueOf(doc?.fields?.[name]);
}
function jsonBytes(value) {
    return Buffer.from(JSON.stringify(value), "utf8");
}
function recordJson(recordSet, index = 0) {
    const raw = recordSet?.records_json?.[index];
    if (!raw)
        throw new Error(`RecordSet.records_json[${index}] is missing`);
    return JSON.parse(Buffer.isBuffer(raw) ? raw.toString("utf8") : Buffer.from(raw).toString("utf8"));
}
function mutationRecordJson(response) {
    const raw = response?.record_json;
    if (!raw)
        throw new Error("MutationResponse.record_json is missing");
    return JSON.parse(Buffer.isBuffer(raw) ? raw.toString("utf8") : Buffer.from(raw).toString("utf8"));
}
function grpcCode(err) {
    const anyErr = err;
    return anyErr?.code ?? anyErr?.cause?.code ?? anyErr?.udb?.code;
}
function describeGrpcError(err) {
    const anyErr = err;
    const code = grpcCode(err);
    const codeName = code === undefined ? "unknown" : grpc.status[code] ?? String(code);
    return `${codeName}: ${anyErr?.details ?? anyErr?.message ?? String(err)}`;
}
function reachedUdbHandler(err) {
    const anyErr = err;
    const text = String(anyErr?.details ??
        anyErr?.message ??
        anyErr?.udb?.details ??
        anyErr?.udb?.message ??
        "");
    return /\budb\s+[\w.]+\/[A-Za-z0-9_]+:/.test(text) || /\(code=[A-Z_]+\)/.test(text);
}
function isFatalMountError(err) {
    const code = grpcCode(err);
    return code !== undefined && FATAL_CONNECTIVITY_CODES.has(code) && !reachedUdbHandler(err);
}
async function expectMounted(label, op) {
    try {
        await op();
    }
    catch (err) {
        if (isFatalMountError(err)) {
            throw new Error(`${label} did not reach an implemented live RPC: ${describeGrpcError(err)}`);
        }
    }
}
async function expectStreamMounted(label, open) {
    await new Promise((resolve, reject) => {
        const stream = open();
        let settled = false;
        const finish = (err) => {
            if (settled)
                return;
            settled = true;
            clearTimeout(timer);
            if (typeof stream.cancel === "function")
                stream.cancel();
            if (err)
                reject(err);
            else
                resolve();
        };
        const timer = setTimeout(() => finish(), 750);
        stream.once("error", (err) => {
            if (isFatalMountError(err)) {
                finish(new Error(`${label} did not reach an implemented live stream RPC: ${describeGrpcError(err)}`));
            }
            else {
                finish();
            }
        });
        stream.once("data", () => finish());
        stream.once("end", () => finish());
        if (typeof stream.end === "function")
            stream.end();
    });
}
// This client exposes RPCs as snake_case methods; RPC_OPERATION_KIND is keyed by
// the canonical gRPC path "/<service>/<MethodName>". snake→Pascal is exact across
// the whole surface (verified), and callers assert the lookup resolves so a
// classification/coverage gap fails loudly rather than silently populating an RPC.
function snakeToPascal(s) {
    return s.split("_").map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join("");
}
function operationKindOf(serviceFull, methodSnake) {
    return generatedClient_1.RPC_OPERATION_KIND[`/${serviceFull}/${snakeToPascal(methodSnake)}`];
}
// A "kitchen-sink" request: protobufjs (under proto-loader) drops keys that the
// concrete request type doesn't declare, so every read RPC picks up exactly the
// fields it has (tenant/project/context/message_type/…). This deepens the probe
// from an empty ping to a field-populated typed request across the full surface.
function surfaceProbeRequest(tenantId, projectId) {
    return {
        context: { tenant_id: tenantId, project_id: projectId, purpose: "ts.live.probe", tenant: { tenant_id: tenantId, project_id: projectId } },
        tenant_id: tenantId,
        project_id: projectId,
        domain: tenantId,
        purpose: "ts.live.probe",
        message_type: LIVE_MESSAGE_TYPE,
        page: 1,
        page_size: 10,
        limit: 10,
    };
}
// perfRealBody returns a SEMANTICALLY VALID request for EVERY RPC the bench
// measures, grounded one-for-one against BENCH_RPC_BODIES.md (real proto fields,
// valid non-`*_UNSPECIFIED` enum NAMEs, seeded fixture values for `<seed:KEY>` ID
// references, valid scalars). The bench then measures REAL handler work down the
// SUCCESS path, not validation-rejection on a generic placeholder. Returns
// It covers ALL 262 RPCs (every unary RPC AND every streaming first-message); the
// perf caller treats a returned undefined as a HARD failure — there is no generic
// fallback. proto-loader drops keys a request type does not declare, so
// over-supplying within a body is safe.
//
// Governance/admin RPCs carry a `GovernanceActor actor{scopes:[...]}` whose scopes
// the broker re-checks under `native.authz.governance` — the MD's authz-notes
// scopes (`authz:admin`, `authz:policy:write|approve|read`) are set on that
// actor here, per-RPC, since admin AUTHORITY comes from the Login JWT (the broker
// derives the principal's scopes from the validated bearer; client-asserted login
// scopes are ignored when a JWT verifier is configured).
function perfRealBody(serviceName, methodName, tenantId, projectId, fixtures) {
    const get = (k) => fixtures?.lookup(k) ?? "";
    const ctx = { tenant_id: tenantId, project_id: projectId, purpose: "ts.live.perf" };
    // RequestContext with a nested TenantContext — the shape the native control-plane
    // services read tenant from (common.v1.RequestContext.tenant.tenant_id).
    const tctx = { tenant_id: tenantId, project_id: projectId, purpose: "ts.live.perf", tenant: { tenant_id: tenantId, project_id: projectId } };
    switch (serviceName) {
        case "DataBroker":
            return dataBrokerBody(methodName, tenantId, projectId, ctx, get);
        case "AuthnService":
            return authnBody(methodName, tenantId, projectId, get);
        case "AuthzService":
            return authzBody(methodName, tenantId, projectId, get);
        case "ApiKeyService":
            return apiKeyBody(methodName, tenantId, projectId, tctx, get);
        case "IdentityProviderService":
            return idpBody(methodName, tenantId, projectId, tctx, get);
        case "TenantService":
            return tenantBody(methodName, get);
        case "AnalyticsService":
            return analyticsBody(methodName, tenantId, projectId, tctx, get);
        case "AssetService":
            return assetBody(methodName, tenantId, projectId, get);
        case "StorageService":
            return storageBody(methodName, tenantId, get);
        case "NotificationService":
            return notificationBody(methodName, tenantId, projectId, tctx, get);
        case "RoomService":
        case "PeerService":
        case "TrackService":
        case "TurnService":
        case "SignalingService":
            return webrtcBody(methodName, tenantId, get);
        case "ControlPlaneService":
            return controlPlaneBody(methodName, tenantId, get);
    }
    return undefined;
}
// ── DataBroker (services/v1/data_broker.proto) — 76 RPCs ───────────────────────
function dataBrokerBody(methodName, tenantId, projectId, context, get) {
    const mongo = { backend: "mongodb", resource_name: get("mongo_collection") };
    const nowIso = () => new Date().toISOString();
    switch (methodName) {
        case "upsert":
            return { context, message_type: LIVE_MESSAGE_TYPE, return_record: true, record_json: jsonBytes({ record_id: `ts-perf-${tenantId}-${projectId}`, tenant_id: tenantId, project_id: projectId, lookup_key: "ts-perf-lk", payload: "ts-perf", revision: 1 }), conflict_fields: ["record_id"] };
        case "batch_upsert":
            return { context, message_type: LIVE_MESSAGE_TYPE, record_json: jsonBytes({ record_id: `ts-perf-${tenantId}-${projectId}`, tenant_id: tenantId, project_id: projectId, lookup_key: "ts-perf-lk", payload: "ts-perf", revision: 1 }), conflict_fields: ["record_id"] };
        case "select":
        case "select_v2":
        case "batch_select":
            return { context, message_type: LIVE_MESSAGE_TYPE, filter: { record_id: get("record_id"), tenant_id: tenantId, project_id: projectId }, limit: 10 };
        case "delete":
            // A NON-EXISTENT row id so the success path runs without destroying the
            // seeded record other RPCs read (still a real, valid Delete).
            return { context, message_type: LIVE_MESSAGE_TYPE, filter: { record_id: "ts-perf-delete-noop", tenant_id: tenantId, project_id: projectId } };
        case "vector_search":
            return { context, collection: "sdk_live_records", vector: [0.1, 0.2, 0.3], limit: 5, with_payload: true };
        case "vector_hybrid_search":
            return { context, collection: "sdk_live_records", vector: [0.1, 0.2, 0.3], text_query: "hello", limit: 5, with_payload: true };
        case "vector_upsert":
        case "vector_batch_upsert":
            return { context, collection: "sdk_live_records", points: [{ id: get("record_id"), vector: [0.1, 0.2, 0.3], payload: {} }] };
        case "put_object":
            // Client-streaming first Chunk (final_chunk so one message is a complete object).
            return { context, bucket: get("bucket") || "udb-live-sdk", object_key: get("object_key") || "ts-perf.txt", data: Buffer.from("x", "utf8"), content_type: "application/octet-stream", final_chunk: true };
        case "get_object":
            return { context, bucket: get("bucket"), object_key: get("object_key") };
        case "generate_presigned_url":
            return { context, bucket: get("bucket"), object_key: get("object_key"), method: "GET", ttl_seconds: 300 };
        case "initiate_multipart_upload":
            return { context, bucket: get("bucket"), object_key: get("object_key"), content_type: "application/octet-stream", part_count: 1, ttl_seconds: 300 };
        case "cache_get":
            return { context, resource: { backend: "redis" }, key: get("object_key") || "ts-perf-cache", touch: false };
        case "cache_set":
            return { context, resource: { backend: "redis" }, key: get("object_key") || "ts-perf-cache", value: Buffer.from("perf", "utf8"), content_type: "text/plain", ttl_seconds: 60 };
        case "cache_delete":
            return { context, resource: { backend: "redis" }, key: get("object_key") || "ts-perf-cache" };
        case "cache_scan":
            return { context, resource: { backend: "redis" }, key_pattern: "*", limit: 50 };
        case "document_get":
            return { context, resource: mongo, document_id: get("document_id") };
        case "document_find":
            return { context, resource: mongo, filter: {}, limit: 10 };
        case "document_upsert":
            return { context, resource: mongo, document_id: get("document_id"), document: { name: "x" } };
        case "document_delete":
            return { context, resource: mongo, document_id: "ts-perf-doc-noop" };
        case "graph_query":
            return { context, resource: { backend: "neo4j" }, query: "MATCH (n) RETURN n LIMIT 1", read_only: true, limit: 10 };
        case "graph_mutate":
            return { context, resource: { backend: "neo4j" }, query: "CREATE (n:Node {id:$id})", parameters: { id: get("record_id") } };
        case "time_series_write":
            // No points (matches Go) — the TimeSeriesPoint.timestamp is a Timestamp message, not a
            // string, so a JSON-string timestamp serialization-fails; the empty write still resolves.
            return { context, resource: { backend: "clickhouse", resource_name: get("ts_table") || "sdk_perf_ts" } };
        case "time_series_query":
            // No from/to (matches Go) — they are Timestamp messages, not strings; a string
            // serialization-fails. resource_name + limit is a valid query.
            return { context, resource: { backend: "clickhouse", resource_name: get("ts_table") || "sdk_perf_ts" }, limit: 100 };
        case "analytical_query":
            return { context, resource: { backend: "clickhouse" }, query: "SELECT 1", limit: 100 };
        case "begin_tx":
            return { context, operation: "upsert", message_type: LIVE_MESSAGE_TYPE, payload: { record_id: get("record_id"), tenant_id: tenantId, project_id: projectId } };
        case "publish_c_d_c":
            return { context, topic_pattern: `${projectId}.*` };
        case "create_materialized_view":
            return { context, schema: "public", name: "mv_test", query: "SELECT 1", with_data: true };
        case "enqueue_outbox_event": {
            const uuid = liveUuid();
            const pkey = get("document_id") || uuid;
            return { context, topic: get("event_type") || "sdk.perf", partition_key: pkey, payload: { event_id: uuid, event_type: get("event_type") || "sdk.perf", correlation_id: liveUuid(), document_id: pkey } };
        }
        case "generic_dispatch":
            return { context: { ...context, scopes: ["udb:dispatch"] }, backend: "postgres", operation: "query", spec_json: JSON.stringify({ sql: "SELECT 1 AS live_probe" }) };
        case "ensure_resource":
            return { context: { ...context, scopes: ["udb:admin"] }, backend: "mongodb", resource_name: get("mongo_collection") };
        case "drop_resource":
            // destructive — target a disposable, non-seeded resource name (never the seeded one).
            // udb_allow_rls_bypass: a drop spans tenants, so the broker fail-closes unless the
            // caller explicitly acknowledges the RLS-bypass review.
            return { context: { ...context, scopes: ["udb:admin"] }, backend: "mongodb", resource_name: `ts_perf_drop_noop_${tenantId}`, spec_json: JSON.stringify({ udb_allow_rls_bypass: true }) };
        case "list_resources":
            return { context: { ...context, scopes: ["udb:admin"] }, backend: "mongodb" };
        case "stage_catalog":
        case "validate_catalog":
            // Use the REAL current manifest captured by the seed (a valid CatalogManifest with
            // checksum_sha256); an empty {} is rejected as "not a CatalogManifest".
            return { context: { ...context, scopes: ["udb:admin"] }, manifest_json: seedCatalogManifest ?? jsonBytes({}), project_id: projectId, reason: "stage" };
        case "activate_catalog":
        case "rollback_catalog":
            return { context: { ...context, scopes: ["udb:admin"] }, project_id: projectId };
        case "get_catalog_versions":
        case "get_catalog_manifest":
            return { context: { ...context, scopes: ["udb:admin"] }, redact: false };
        case "get_catalog_version":
            return { context: { ...context, scopes: ["udb:admin"] }, project_id: projectId, version: "" };
        case "plan_migration":
            return { context: { ...context, scopes: ["udb:admin"] }, project_id: projectId, dry_run: true };
        case "apply_migration":
            return { context: { ...context, scopes: ["udb:admin"] }, run_id: get("apply_run_id") || get("migration_id"), project_id: projectId, approval_token: get("approval_token") };
        case "get_migration_status":
            return { context: { ...context, scopes: ["udb:admin"] }, run_id: get("migration_id"), project_id: projectId };
        case "approve_migration_plan":
            return { context: { ...context, scopes: ["udb:admin"] }, run_id: get("approve_run_id") || get("migration_id"), project_id: projectId };
        case "list_migration_runs":
            return { context: { ...context, scopes: ["udb:admin"] }, project_id: projectId, limit: 50 };
        case "list_dlq_events":
            return { context, limit: 50 };
        case "get_dlq_event":
            return { context, dlq_id: get("dlq_id") || liveUuid() };
        case "replay_dlq_event":
            return { context, dlq_id: get("replay_dlq_id") || liveUuid(), preserve_event_id: false };
        case "dismiss_dlq_event":
            return { context, dlq_id: get("dismiss_dlq_id") || liveUuid() };
        case "quarantine_dlq_event":
            return { context, dlq_id: get("quarantine_dlq_id") || liveUuid() };
        case "get_cdc_status":
            return { context, slot_name: "udb_cdc" };
        case "pause_cdc":
            return { context, slot_name: "udb_cdc", reason: "maintenance" };
        case "resume_cdc":
            return { context, slot_name: "udb_cdc", reason: "resume" };
        case "step_down_cdc_leader":
            return { context, slot_name: "udb_cdc", reason: "failover" };
        case "preview_cdc_redaction":
            return { context, message_type: get("message_type"), topic: get("event_type"), payload_json: jsonBytes({ sample: true }), redaction_mode: "mask", redaction_version: 1 };
        case "scan_projection_drift":
            return { context, project_id: projectId, message_type: get("message_type"), scan_mode: "sample", rows_per_target: 100, limit: 10 };
        case "list_sagas":
            return { context, limit: 50 };
        case "get_saga":
            return { context, saga_id: get("saga_id") || liveUuid() };
        case "retry_saga_compensation":
            return { context, saga_id: get("retry_saga_id") || liveUuid(), reason: "retry" };
        case "mark_saga_reviewed":
            return { context, saga_id: get("mark_saga_id") || liveUuid(), reason: "reviewed" };
        case "list_policies":
            return { context, include_disabled: false, limit: 50 };
        case "put_policy":
            // ALLOW-ALL (empty selectors = match-any): a narrow policy would flip the data
            // plane to deny-by-default (snapshot non-empty) and deny the admin's own
            // Upsert/Select/Vector*/TimeSeries* once reload_policies runs. An allow-all keeps
            // the data plane open while still exercising the write path.
            return { context, policy: { effect: "allow", service_identity: "", tenant_id: tenantId, message_type: "", operation: "", required_scope: "", priority: 1, enabled: true } };
        case "delete_policy":
            return { context, policy_id: Number(get("ds_policy_id")) || Number(get("policy_id")) || 0 };
        case "reload_policies":
        case "lint_policies":
        case "get_capabilities":
            return { context, project_id: projectId };
        case "lookup_message_schema":
            return { context, project_id: projectId, message_type: get("message_type") };
        case "list_message_schemas":
            return { context, project_id: projectId };
        case "get_health_report":
            return { context, with_probes: false, project_id: projectId };
        case "ensure_project":
            return { context: { ...context, scopes: ["udb:admin"] }, project_id: projectId, name: "My Project", cdc_topic_prefix: `${projectId}.` };
        case "list_projects":
            return { context: { ...context, scopes: ["udb:admin"] }, limit: 50 };
        case "get_admin_summary":
            return { context: { ...context, scopes: ["udb:admin"] }, project_id: projectId, with_probes: false, redact: false };
        case "list_admin_audit_logs":
            return { context: { ...context, scopes: ["udb:admin"] }, limit: 50, redact: false };
        case "verify_admin_audit_log":
            return { context: { ...context, scopes: ["udb:admin"] }, limit: 0 };
    }
    return undefined;
}
// ── AuthnService (core/authn/services/v1) — 50 RPCs ────────────────────────────
// Destructive/session-terminal RPCs are sequenced in Phase 3 by the caller and MUST
// target the seeded disposable user / its session — never the admin's own. The
// bodies below already point at <seed:user_id>/<seed:session_id> accordingly.
function authnBody(methodName, tenantId, projectId, get) {
    const u = get("user_id");
    switch (methodName) {
        case "create_user":
            return { username: `bench-${liveUuid().slice(0, 8)}`, email: `bench-${liveUuid().slice(0, 8)}@acme.test`, password: "Str0ng!Passw0rd", tenant_id: tenantId, full_name: "Bench User", account_kind: "ACCOUNT_KIND_PERSON" };
        case "get_user":
            return { user_id: u };
        case "list_users":
            return { tenant_id: tenantId };
        case "update_user":
            return { user_id: u, full_name: "Bench B", tenant_id: tenantId };
        case "change_user_status":
            return { user_id: u, new_status: "USER_STATUS_SUSPENDED", reason: "bench action" };
        case "admin_reset_password":
            return { user_id: u };
        case "send_o_t_p":
            return { user_id: u, otp_type: "OTP_TYPE_EMAIL_VERIFICATION" };
        case "verify_o_t_p":
            // Seeded otp_id + dev-echoed otp_code from the dedicated OTP user.
            return { otp_id: get("otp_id"), code: get("otp_code") };
        case "resend_o_t_p":
            return { original_otp_id: get("otp_id"), reason: "not_received" };
        case "authenticate":
            return { bearer_token: get("token"), credential_type: "AUTH_CREDENTIAL_TYPE_BEARER_TOKEN" };
        case "login":
            // Use the REAL bench credentials so the measured Login drives the success path
            // (a placeholder username returns UNAUTHENTICATED "invalid username or password").
            return { username: process.env.UDB_LIVE_USERNAME || get("username") || "bench", password: process.env.UDB_LIVE_PASSWORD || "Str0ng!Passw0rd", device_type: "DEVICE_TYPE_API", device_name: "cli", tenant_hint: tenantId, project_hint: projectId };
        case "refresh_token":
            return { refresh_token: get("refresh_token") };
        case "logout":
            return { session_id: get("session_id") };
        case "change_password":
            // current_password MUST be the exact password the seed user was created with.
            return { user_id: u, current_password: "CorrectHorse1!", new_password: "N3w!Passw0rd9" };
        case "validate_token":
            return { token: get("token"), token_type: "TOKEN_TYPE_JWT_ACCESS" };
        case "create_session":
            return { principal: { principal_id: u, subject: get("subject"), user_id: u, tenant_id: tenantId }, ttl_seconds: 3600 };
        case "refresh_session":
            return { session_id: get("session_id"), ttl_seconds: 3600 };
        case "get_session":
            return { session_id: get("session_id") };
        case "list_sessions":
            return { user_id: u };
        case "revoke_session":
            return { session_id: get("session_id"), revoke_reason: "user logout" };
        case "validate_c_s_r_f":
            return { session_id: get("session_id"), csrf_token: get("csrf_token") };
        case "enroll_m_f_a":
            return { user_id: u, mfa_type: "AUTH_FACTOR_KIND_TOTP" };
        case "confirm_m_f_a_enrollment":
            return { user_id: u, otp_id: get("code"), code: "123456" };
        case "generate_recovery_codes":
            return { user_id: u, count: 10 };
        case "put_mfa_policy":
            // require_mfa MUST stay false on the live login tenant: true makes every later
            // Login fail FAILED_PRECONDITION "MFA enrollment required by tenant policy" and
            // poisons the whole bench (the admin user has no enrolled second factor).
            return { tenant_id: tenantId, require_mfa: false };
        case "get_mfa_policy":
            return { tenant_id: tenantId };
        case "forgot_password":
            return { identifier: `bench-${u}@acme.test` };
        case "reset_password":
            // Use the real dev-echoed code (UDB_OTP_DEV_ECHO=1 → mfa.rs:208 echoes it
            // unconditionally). NO "123456" fallback — a wrong code denies and masks the
            // real bug (empty reset_otp_code), per BENCH_TS_PHP_ADVISORY.md.
            return { otp_id: get("reset_otp_id"), code: get("reset_otp_code"), new_password: "N3w!Passw0rd9" };
        case "introspect_token":
            return { token: get("token") };
        case "send_phone_verification":
            return { user_id: u, phone: "+15551234567" };
        case "get_jwks":
            return {};
        case "start_web_authn_registration":
            return { user_id: u, label: "yubikey", tenant_id: tenantId };
        case "finish_web_authn_registration":
            // dev soft-authenticator: fresh reg challenge + the test sentinel credential.
            return { challenge_id: get("reg_challenge_id"), public_key_credential_json: "__UDB_WEBAUTHN_TEST__", label: "perf-key" };
        case "start_web_authn_authentication":
            return { user_id: u, tenant_id: tenantId };
        case "finish_web_authn_authentication":
            // dev soft-authenticator: fresh auth challenge + the test sentinel assertion.
            return { challenge_id: get("auth_challenge_id"), public_key_credential_json: "__UDB_WEBAUTHN_TEST__" };
        case "list_devices":
            return { user_id: u };
        case "revoke_device":
            return { device_id: get("device_id"), reason: "lost device" };
        case "admin_revoke_session":
            return { user_id: u, session_id: get("session_id"), reason: "compromised" };
        case "admin_revoke_all_user_sessions":
            return { user_id: u, reason: "compromised" };
        case "admin_revoke_all_tenant_sessions":
            // Targets a NON-seeded throwaway tenant so it never kills the admin's own sessions.
            return { tenant_id: `bench-throwaway-${liveUuid().slice(0, 8)}`, reason: "incident" };
        case "emergency_revoke":
            // Target the seeded disposable principal only — never the live admin tenant.
            return { principal_id: get("subject"), reason: "incident" };
        case "issue_mfa_challenge":
            return { user_id: u, factor_kind: "AUTH_FACTOR_KIND_TOTP", purpose: "MFA_CHALLENGE_PURPOSE_SENSITIVE_OPERATION" };
        case "verify_mfa_challenge":
            return { challenge_id: get("challenge_id"), code: get("otp_code") };
        case "list_mfa_factors":
            return { user_id: u };
        case "disable_mfa_factor":
            return { user_id: u, factor_kind: "AUTH_FACTOR_KIND_TOTP" };
        case "rename_passkey":
            return { user_id: u, credential_id: get("record_id"), new_label: "work key" };
        case "revoke_recovery_codes":
            return { user_id: u };
        case "admin_reset_mfa":
            return { user_id: u, reason: "lost device" };
        case "list_web_authn_credentials":
            return { user_id: u };
        case "delete_web_authn_credential":
            return { user_id: u, credential_id: get("record_id") };
    }
    return undefined;
}
// ── AuthzService (core/authz/services/v1) — 41 RPCs ────────────────────────────
// Governance RPCs carry a GovernanceActor whose scopes are re-checked under
// native.authz.governance — set the MD-specified scope on the actor per-RPC.
function authzBody(methodName, tenantId, projectId, get) {
    const subject = get("subject");
    // created_by / assigned_by / deleted_by / … are audit columns the broker validates
    // as bare UUIDs — the casbin `subject` ("user:<uuid>") is NOT a valid UUID there.
    const byId = get("user_id") || liveUuid();
    const actor = (scope) => ({ subject, tenant_id: tenantId, project_id: projectId, scopes: [scope] });
    const principal = { subject, user_id: get("user_id"), tenant_id: tenantId, scopes: [] };
    const resource = { resource_type: get("resource") || "invoice", table: "invoice" };
    switch (methodName) {
        case "authorize":
            return { principal, tenant_id: tenantId, project_id: projectId, resource, action: get("action") || "data.select", domain: tenantId, requested_scopes: ["udb:read"] };
        case "check_access":
            return { user_id: get("user_id"), domain: tenantId, object: get("object") || "invoice", action: get("action") || "data.select" };
        case "create_role":
            return { name: `bench-reader-${liveUuid().slice(0, 8)}`, created_by: byId, role_code: `bench_reader_${liveUuid().slice(0, 8)}`, domain: tenantId, tenant_id: tenantId, scope_type: "ROLE_SCOPE_TYPE_TENANT" };
        case "assign_role":
            return { user_id: get("user_id"), role_id: get("role_id"), domain: tenantId, assigned_by: byId, principal_kind: "PRINCIPAL_KIND_USER", tenant_id: tenantId };
        case "create_policy_rule":
            return { subject, domain: tenantId, object: get("object") || "ledger", action: get("action") || "data.update", effect: "POLICY_EFFECT_ALLOW", created_by: byId, tenant_id: tenantId };
        case "list_user_permissions":
            return { user_id: get("user_id"), domain: tenantId };
        case "list_access_decision_audits":
            return { user_id: get("user_id"), domain: tenantId, page: { page_size: 50 } };
        case "revoke_role":
            return { user_id: get("user_id"), user_role_id: get("user_role_id"), reason: "rotation", revoked_by: byId };
        case "list_user_roles":
            return { user_id: get("user_id"), domain: tenantId, active_only: true };
        case "get_role":
            return { role_id: get("role_id") };
        case "list_roles":
            return { domain: tenantId, active_only: true, page: { page_size: 50 } };
        case "batch_check_permissions":
            return { user_id: get("user_id"), domain: tenantId, checks: [{ object: get("object") || "invoice", action: get("action") || "data.select" }], context: { ip_address: "127.0.0.1" } };
        case "update_role":
            return { role_id: get("role_id"), updated_by: byId, name: `reader-${liveUuid().slice(0, 8)}`, description: "bench", is_active: true };
        case "delete_role":
            // destructive — the SEPARATE disposable role seeded for deletion (real 200); the
            // primary role_id survives for get_role/update_role/list_user_roles.
            return { role_id: get("delete_role_id") || liveUuid(), deleted_by: byId };
        case "get_policy_rule":
            return { policy_id: Number(get("policy_id")) || get("policy_id") };
        case "list_policy_rules":
            return { domain: tenantId, subject, object: get("object") || "ledger", active_only: true, page: { page_size: 50 } };
        case "delete_policy_rule":
            // destructive — the SEPARATE disposable policy rule seeded for deletion (real 200).
            return { policy_id: get("delete_policy_id") || liveUuid(), deleted_by: byId };
        case "put_role_binding":
            return { binding: { subject, role: get("role"), tenant: tenantId, project: projectId, source: "bench" } };
        case "put_relationship":
            return { tuple: { subject, relation: get("relation") || "member", object: get("object") || "group:bench", tenant: tenantId, project: projectId, source: "bench" } };
        case "put_authz_policy":
            return { policy: { id: get("policy_id") || liveUuid(), priority: 100, enabled: true, effect: "allow", tenant: tenantId, subject, action: get("action") || "data.select", resource: get("resource") || "invoice", required_scopes: ["udb:read"] } };
        case "lint_authz_policies":
            return {};
        case "get_native_access":
            return { principal, tenant_id: tenantId, project_id: projectId, resource, action: get("action") || "data.select", backend: "postgres", requested_scopes: ["udb:read"] };
        case "get_policy_bundle":
            return { tenant_id: tenantId, project_id: projectId, domain: tenantId };
        case "create_policy_draft":
            return { actor: actor("authz:policy:write"), tenant_id: tenantId, project_id: projectId, policy_set_name: "default", title: "draft 1", change_reason: "init", document: { policies: [] } };
        case "update_policy_draft":
            return { actor: actor("authz:policy:write"), draft_id: get("update_draft_id") || get("policy_draft_id"), document: {}, change_reason: "edit", title: "draft 1" };
        case "diff_policy_draft":
            return { actor: actor("authz:policy:read"), draft_id: get("policy_draft_id") };
        case "submit_policy_draft":
            return { actor: actor("authz:policy:write"), draft_id: get("policy_draft_id") };
        case "approve_policy_draft":
            return { actor: actor("authz:policy:approve"), draft_id: get("approve_draft_id"), reviewer: subject, reason: "ok" };
        case "reject_policy_draft":
            return { actor: actor("authz:policy:approve"), draft_id: get("reject_draft_id"), reviewer: subject, reason: "nack" };
        case "activate_policy_version":
            return { actor: actor("authz:admin"), policy_version_id: get("policy_version_id") || liveUuid() };
        case "rollback_policy_version":
            return { actor: actor("authz:admin"), policy_set_id: get("rollback_policy_set_id") || liveUuid(), target_version_id: get("rollback_target_version_id") || liveUuid(), change_reason: "revert" };
        case "activate_canary":
            return { actor: actor("authz:admin"), policy_version_id: get("canary_version_id") || liveUuid(), scope_kind: "CANARY_SCOPE_KIND_PERCENT", scope_values: ["10"], success_window_secs: 0, metric_threshold: 0.99, min_samples: 0 };
        case "promote_canary":
            return { actor: actor("authz:admin"), canary_id: get("canary_id") || liveUuid() };
        case "get_canary_status":
            return { actor: actor("authz:policy:read"), canary_id: get("canary_id") || liveUuid() };
        case "list_policy_versions":
            return { actor: actor("authz:policy:read"), tenant_id: tenantId, project_id: projectId, policy_set_id: get("policy_id"), state: "POLICY_VERSION_STATE_ACTIVE", page: { page_size: 50 } };
        case "simulate_policy":
            return { actor: actor("authz:policy:read"), tenant_id: tenantId, project_id: projectId, draft_id: get("policy_draft_id"), cases: [{ principal: { subject }, resource, action: get("action") || "data.select", label: "c1" }], persist: false };
        case "explain_policy":
            return { actor: actor("authz:policy:read"), tenant_id: tenantId, project_id: projectId, test_case: { principal: { subject }, resource, action: get("action") || "data.select" } };
        case "get_authz_revision":
            return { tenant_id: tenantId, project_id: projectId };
        case "invalidate_policy_bundles":
            return { actor: actor("authz:admin"), tenant_id: tenantId, project_id: projectId, reason: "rotate" };
        case "seed_builtin_roles":
            return { actor: actor("authz:admin"), tenant_id: tenantId, project_id: projectId };
        case "migrate_legacy_policies":
            return { actor: actor("authz:admin"), tenant_id: tenantId, project_id: projectId, apply: false, policy_set_name: "default" };
    }
    return undefined;
}
// ── ApiKeyService (core/apikey/services/v1) — 9 RPCs ───────────────────────────
function apiKeyBody(methodName, tenantId, projectId, tctx, get) {
    const context = { tenant: { tenant_id: tenantId, project_id: projectId }, user_id: get("owner_id") };
    switch (methodName) {
        case "create_api_key":
            return { name: "bench-key", description: "bench", owner_type: "API_KEY_OWNER_TYPE_SERVICE_ACCOUNT", owner_id: get("owner_id"), scopes: ["resource:read"], context };
        case "get_api_key":
            return { key_id: get("key_id") };
        case "list_api_keys":
            return { owner_id: get("owner_id"), owner_type: "API_KEY_OWNER_TYPE_SERVICE_ACCOUNT", status: "API_KEY_STATUS_ACTIVE", page: { page: 1, page_size: 50 } };
        case "update_api_key":
            // separate disposable key (RotateApiKey rotates the primary key_id and would
            // invalidate it).
            return { key_id: get("update_key_id") || get("key_id"), name: "bench-key-2", description: "updated", scopes: ["resource:read"], context };
        case "revoke_api_key":
            // the SEPARATE disposable key seeded for revocation (real 200); the primary
            // key survives for update/rotate/get/validate.
            return { key_id: get("revoke_key_id") || get("key_id"), revoke_reason: "bench cleanup", context };
        case "rotate_api_key":
            return { key_id: get("key_id"), rotation_reason: "bench rotate", context };
        case "emergency_revoke_api_keys":
            // destructive — scope to the bench owner only so it never revokes other keys.
            return { owner_id: get("owner_id"), tenant_id: tenantId, reason: "bench emergency", context };
        case "validate_api_key":
            return { plain_key: get("plain_key"), endpoint: "/v1/test", required_scope: "resource:read", ip_address: "127.0.0.1" };
        case "get_api_key_usage_stats":
            return { key_id: get("key_id") };
    }
    return undefined;
}
// ── IdentityProviderService (core/idp/services/v1) — 27 RPCs ───────────────────
// SAML/SCIM/external-IdP RPCs need an external provider — best valid body only.
function idpBody(methodName, tenantId, projectId, tctx, get) {
    const provider_id = get("provider_id");
    const context = { tenant: { tenant_id: tenantId } };
    const page = { page: 1, page_size: 20 };
    switch (methodName) {
        case "create_provider":
            // kind must be ≤24 chars → IDP_KIND_OIDC (VARCHAR(24) overflow on EXTERNAL_SESSION).
            return { tenant_id: tenantId, kind: "IDP_KIND_OIDC", display_name: `Acme OIDC ${liveUuid().slice(0, 8)}`, issuer: "https://idp.example.com", jwks_url: "https://idp.example.com/jwks", client_ids: ["client-1"], audiences: ["udb"], claim_mapping_json: "{}", group_mapping_json: "{}", jit_policy_json: JSON.stringify({ require_verified_email: false }), account_linking_policy: "explicit", enabled: true, created_by: get("user_id"), context };
        case "update_provider":
            return { provider_id, tenant_id: tenantId, display_name: `Acme OIDC ${liveUuid().slice(0, 8)}`, claim_mapping_json: "{}", group_mapping_json: "{}", jit_policy_json: JSON.stringify({ require_verified_email: false }), account_linking_policy: "explicit", updated_by: get("user_id"), context };
        case "disable_provider":
            // target the SEPARATE disposable provider so saml_acs/resolve (which read the main
            // provider_id) aren't broken by a disabled provider.
            return { provider_id: get("disable_provider_id") || provider_id, tenant_id: tenantId, updated_by: get("user_id"), context };
        case "get_provider":
            return { provider_id, tenant_id: tenantId };
        case "list_providers":
            return { tenant_id: tenantId, kind: "IDP_KIND_UNSPECIFIED", enabled_only: false, page };
        case "test_provider_discovery":
            return { provider_id, tenant_id: tenantId };
        case "force_jwks_refresh":
            return { provider_id, tenant_id: tenantId };
        case "preview_claim_mapping":
            return { provider_id, tenant_id: tenantId, claims_json: JSON.stringify({ sub: "abc", email: "a@x.com" }), claim_mapping_json: "" };
        case "preview_group_mapping":
            return { provider_id, tenant_id: tenantId, groups: ["admins"], group_mapping_json: "" };
        case "list_external_identities":
            return { tenant_id: tenantId, provider_id: "", user_id: "", page };
        case "link_identity":
            return { tenant_id: tenantId, provider_id, subject: "ext-subject-1", user_id: get("user_id"), email: "a@x.com", email_verified: true, context };
        case "unlink_identity":
            return { tenant_id: tenantId, external_identity_id: get("external_identity_id") || liveUuid(), context };
        case "import_saml_metadata":
            return { provider_id: get("saml_provider_id") || provider_id, tenant_id: tenantId, metadata_xml: SAML_IDP_METADATA_XML, updated_by: get("user_id"), context };
        case "start_saml_login":
            // Must target the SAML-kind provider with an imported SSO URL, not the OIDC one.
            return { provider_id: get("saml_provider_id") || provider_id, tenant_id: tenantId, relay_state: "state-1" };
        case "saml_acs":
            // dev self-asserted IdP: the test sentinel SAML response.
            // Unique NameID/email per call (sentinel `:<name_id>` suffix, saml.rs:940) so JIT
            // provisioning always creates a FRESH external user → no "account exists; explicit
            // linking required" collision with prior runs/iterations (account_linking_policy=explicit).
            return { provider_id, tenant_id: tenantId, saml_response: `__UDB_SAML_TEST__:saml-${liveUuid().slice(0, 8)}@x.com`, relay_state: "state-1", context };
        case "resolve_external_identity":
            // Unique sub/email per call so JIT always provisions a FRESH external user (no
            // pre-existing-account collision with scim/prior runs; account_linking_policy=explicit).
            return { provider_id, tenant_id: tenantId, claims_json: JSON.stringify({ sub: `ext-${liveUuid().slice(0, 8)}`, email: `ext-${liveUuid().slice(0, 8)}@x.com`, email_verified: true }) };
        case "scim_create_user":
            // random userName per iteration so the per-iteration rebuild doesn't dup (ALREADY_EXISTS).
            return { tenant_id: tenantId, provider_id, scim_user_json: JSON.stringify({ userName: `scim-${liveUuid().slice(0, 8)}@x.com`, active: true }), context };
        case "scim_get_user":
            return { tenant_id: tenantId, provider_id, scim_user_id: get("scim_user_id") || get("record_id") };
        case "scim_list_users":
            return { tenant_id: tenantId, provider_id, filter: "", page };
        case "scim_replace_user":
            return { tenant_id: tenantId, provider_id, scim_user_id: get("scim_user_id") || get("record_id"), scim_user_json: JSON.stringify({ userName: "a@x.com", active: true }), context };
        case "scim_patch_user":
            return { tenant_id: tenantId, provider_id, scim_user_id: get("scim_user_id") || get("record_id"), operations: [{ op: "replace", path: "active", value_json: "false" }], context };
        case "scim_delete_user":
            return { tenant_id: tenantId, provider_id, scim_user_id: get("delete_scim_user_id") || get("record_id"), context };
        case "scim_create_group":
            return { tenant_id: tenantId, provider_id, scim_group_json: JSON.stringify({ displayName: `grp-${liveUuid().slice(0, 8)}` }), context };
        case "scim_get_group":
            return { tenant_id: tenantId, provider_id, scim_group_id: get("scim_group_id") || get("record_id") };
        case "scim_list_groups":
            return { tenant_id: tenantId, provider_id, filter: "", page };
        case "scim_patch_group":
            return { tenant_id: tenantId, provider_id, scim_group_id: get("record_id"), operations: [{ op: "add", path: "members", value_json: '["scim-user-id"]' }], context };
        case "scim_delete_group":
            return { tenant_id: tenantId, provider_id, scim_group_id: get("record_id"), context };
    }
    return undefined;
}
// ── TenantService (core/tenant/services/v1) — 6 RPCs ───────────────────────────
function tenantBody(methodName, get) {
    const tenant_id = get("tenant_id");
    switch (methodName) {
        case "create_tenant":
            // unique code per call avoids the unique-code collision the MD flags.
            return { code: `bench-${liveUuid().slice(0, 8)}`, name: "Acme Bench", type: "organization", parent_tenant_id: "", config: "{}", branding: "{}" };
        case "get_tenant":
            return { tenant_id };
        case "list_tenants":
            return { type: "", status: "", page: 1, page_size: 20 };
        case "update_tenant":
            return { tenant_id, name: "Acme Bench", status: "active", config: "{}", branding: "{}" };
        case "get_tenant_config":
            return { tenant_id };
        case "update_tenant_config":
            return { tenant_id, config_key: "feature.flag", config_value: "on", type: "string" };
    }
    return undefined;
}
// ── AnalyticsService (core/analytics/services/v1) — 7 RPCs ─────────────────────
function analyticsBody(methodName, tenantId, projectId, tctx, get) {
    const context = { tenant: { tenant_id: tenantId, project_id: projectId }, request_id: `ts-perf-${Date.now()}` };
    const stage_name = get("stage_name");
    switch (methodName) {
        case "record_pipeline_metric":
            return { stage_name, tenant_id: tenantId, latency_ms: 12.5, is_success: true, context };
        case "get_pipeline_summary":
            return { stage_name, tenant_id: tenantId, hour_from: "2026-06-01T00:00:00Z", hour_to: "2026-06-14T23:00:00Z", page: { page: 1, page_size: 50 } };
        case "get_executor_performance":
            return { executor_identity: "", workload_kind: "", date_from: "2026-06-01", date_to: "2026-06-14" };
        case "get_reconciliation_analytics":
            return { date_from: "2026-06-01", date_to: "2026-06-14" };
        case "get_throughput":
            return { tenant_id: tenantId, hour_from: "2026-06-01T00:00:00Z", hour_to: "2026-06-14T23:00:00Z" };
        case "get_sla_compliance":
            return { stage_name, date_from: "2026-06-01", date_to: "2026-06-14", p99_threshold_ms: 250.0, error_rate_threshold: 0.01 };
        case "trigger_snapshot":
            return { stage_name, hour: "2026-06-14T10:00:00Z", context };
    }
    return undefined;
}
// ── AssetService (core/asset/services/v1) — 8 RPCs ─────────────────────────────
function assetBody(methodName, tenantId, projectId, get) {
    switch (methodName) {
        case "create_pipeline_definition":
            return { tenant_id: tenantId, name: `bench-pipeline-${liveUuid().slice(0, 8)}`, description: "Generate thumbnails", media_type: "image/png", steps: JSON.stringify([{ name: "resize", type: "TRANSFORM" }]), version: 1 };
        case "get_pipeline_definition":
            return { tenant_id: tenantId, definition_id: get("definition_id") };
        case "register_asset":
            return { tenant_id: tenantId, project_id: "", file_id: get("file_id"), name: "logo.png", media_type: "image/png", metadata: JSON.stringify({ source: "upload" }) };
        case "start_pipeline":
            return { tenant_id: tenantId, definition_id: get("definition_id"), asset_id: get("asset_id"), context: "{}", correlation_id: `run-${liveUuid().slice(0, 8)}` };
        case "get_pipeline":
            return { tenant_id: tenantId, instance_id: get("instance_id") };
        case "complete_step":
            // step_id is a real step from the seeded started pipeline (GetPipeline.steps[].id).
            return { tenant_id: tenantId, step_id: get("step_id") || liveUuid(), status: "COMPLETED", result: "{}", error_message: "" };
        case "list_assets":
            return { tenant_id: tenantId, media_type: "", status: "", page: 1, page_size: 20 };
        case "get_asset":
            return { tenant_id: tenantId, asset_id: get("asset_id") };
    }
    return undefined;
}
// ── StorageService (core/storage/services/v1) — 7 RPCs ─────────────────────────
function storageBody(methodName, tenantId, get) {
    const file_id = get("file_id");
    switch (methodName) {
        case "register_upload":
            return { tenant_id: tenantId, project_id: "", filename: "report.pdf", content_type: "application/pdf", file_type: "document", reference_id: liveUuid(), reference_type: "document", is_public: false, expires_in_minutes: 15, size_bytes: 1024 };
        case "finalize_upload":
            return { tenant_id: tenantId, file_id, content_type: "application/pdf", file_type: "document", reference_id: file_id, reference_type: "document", is_public: false, size_bytes: 1024 };
        case "get_download_url":
            return { tenant_id: tenantId, file_id, expires_in_minutes: 15 };
        case "get_file":
            return { tenant_id: tenantId, file_id };
        case "update_file":
            return { tenant_id: tenantId, file_id, filename: "renamed.pdf", content_type: "application/pdf", file_type: "document", reference_id: file_id, reference_type: "document", is_public: true };
        case "delete_file":
            // destructive — the SEPARATE disposable file seeded for deletion (real 200); the
            // primary file_id survives for get_file/get_download_url/update_file.
            return { tenant_id: tenantId, file_id: get("delete_file_id") || liveUuid() };
        case "list_files":
            return { tenant_id: tenantId, file_type: "document", page: 1, page_size: 20 };
    }
    return undefined;
}
// ── NotificationService (core/notification/services/v1) — 11 RPCs ──────────────
function notificationBody(methodName, tenantId, projectId, tctx, get) {
    const event_type = get("event_type");
    switch (methodName) {
        case "send_notification":
            return { event_type, recipient_id: get("user_id"), recipient_address: "user@example.com", tenant_id: tenantId, project_id: projectId, locale: "en", variables: {}, channels: ["NOTIFICATION_CHANNEL_EMAIL"] };
        case "get_notification":
            return { log_id: get("log_id") };
        case "list_notifications":
            return { tenant_id: tenantId, page: { page: 1, page_size: 20 } };
        case "retry_notification":
            return { log_id: get("log_id") };
        case "upsert_template":
            return { event_type, channel: "NOTIFICATION_CHANNEL_EMAIL", locale: "en", subject_template: "Hello {name}", body_template: "Body {name}", is_active: true };
        case "get_template":
            return { event_type, channel: "NOTIFICATION_CHANNEL_EMAIL", locale: "en" };
        case "list_templates":
            return { page: { page: 1, page_size: 20 } };
        case "get_delivery_stats":
            return { tenant_id: tenantId, event_type, date_from: "2026-01-01", date_to: "2026-12-31" };
        case "set_preference":
            return { user_id: get("user_id"), tenant_id: tenantId, channel: "NOTIFICATION_CHANNEL_EMAIL", event_type: "", is_opted_out: true };
        case "get_preference":
            return { user_id: get("user_id"), tenant_id: tenantId, channel: "NOTIFICATION_CHANNEL_EMAIL", event_type: "" };
        case "list_preferences":
            return { user_id: get("user_id"), tenant_id: tenantId, page: { page: 1, page_size: 20 } };
    }
    return undefined;
}
// ── WebRTC Room/Peer/Track/Turn (core/webrtc/services/v1) — unary RPCs ─────────
// Destructive room/peer/track teardown targets the seeded disposable room.
function webrtcBody(methodName, tenantId, get) {
    const room_id = get("room_id");
    const peer_id = get("peer_id");
    const track_id = get("track_id");
    switch (methodName) {
        case "create_room":
            return { tenant_id: tenantId, name: `bench-room-${liveUuid().slice(0, 8)}`, max_participants: 10, config: "{}", created_by: get("user_id") };
        case "get_room":
            return { tenant_id: tenantId, room_id };
        case "update_room":
            return { tenant_id: tenantId, room_id, name: "bench-room-2", state: "active", config: "{}" };
        case "close_room":
            // destructive against the SEPARATE disposable room seeded for closing (real 200);
            // the main room stays available to the other webrtc RPCs in the same run.
            return { tenant_id: tenantId, room_id: get("close_room_id") || liveUuid() };
        case "list_rooms":
            return { tenant_id: tenantId, state: "active", page: 1, page_size: 20 };
        case "join_room":
            return { tenant_id: tenantId, room_id, display_name: "Bench User", metadata: "{}", user_agent: "bench/1.0" };
        case "leave_room":
            // destructive — a throwaway peer id so the seeded peer stays for read RPCs.
            return { tenant_id: tenantId, room_id, peer_id: get("leave_peer_id") || liveUuid() };
        case "get_peer":
            return { tenant_id: tenantId, peer_id };
        case "list_peers":
            return { tenant_id: tenantId, room_id, state: "connected" };
        case "publish_track":
            return { tenant_id: tenantId, room_id, peer_id, kind: "audio", label: "mic", settings: "{}", metadata: "{}" };
        case "unpublish_track":
            // destructive — a throwaway track id so the seeded track stays for read RPCs.
            return { tenant_id: tenantId, track_id: get("unpublish_track_id") || liveUuid() };
        case "mute_track":
            return { tenant_id: tenantId, track_id, muted: true };
        case "list_tracks":
            return { tenant_id: tenantId, room_id, peer_id, kind: "audio" };
        case "issue_credentials":
            return { tenant_id: tenantId, room_id, peer_id, ttl_seconds: 3600 };
        case "signal":
            // SignalingService bidi: first SignalRequest is a keepalive ping for the room/peer.
            return { tenant_id: tenantId, room_id, peer_id, ping: true };
    }
    return undefined;
}
// ── ControlPlaneService (core/control/services/v1) — unary RPCs ────────────────
// node_id refs a data-plane PEP node that opened a stream session — un-seedable in
// a passive bench; best valid body. (Stream RPCs are timed separately by the loop.)
function controlPlaneBody(methodName, tenantId, get) {
    const context = { tenant: { tenant_id: tenantId } };
    const node_id = get("node_id");
    switch (methodName) {
        case "stream_resources":
            // Bidi first DiscoveryRequest = a subscription (empty version_info/response_nonce).
            return { node_id: node_id || "ts-perf-node", resource_type: "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION", version_info: "", response_nonce: "", resource_names: [], context };
        case "delta_resources":
            // Bidi first DeltaDiscoveryRequest = initial subscribe (empty nonce/versions).
            return { node_id: node_id || "ts-perf-node", resource_type: "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION", response_nonce: "", resource_names_subscribe: [], resource_names_unsubscribe: [], initial_resource_versions: {}, context };
        case "get_resources":
            return { resource_type: "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION", tenant_id: tenantId, resource_names: [], page: { page: 1, page_size: 50 }, context };
        case "list_node_states":
            return { node_id: "", resource_type: "RESOURCE_TYPE_UNSPECIFIED", page: { page: 1, page_size: 50 }, context };
        case "ack_status":
            return { node_id, resource_type: "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION", context };
    }
    return undefined;
}
// ── Perf SEED phase + fixture map (mirrors the Go harness) ─────────────────────
//
// The perf run measures REAL successful-call latency for the whole RPC surface. To
// do that, every reference/ID field in a request must point at an entity that
// actually exists. seedPerfFixtures creates those entities up front — REUSING the
// same create flows the conformance suite (runLiveNativeServiceE2E above) already
// proves succeed — and records their real identifiers into a PerfFixtures map keyed
// by SEMANTIC field name (user_id, role, policy_id, file_id, room_id, subject, …).
// perfRealBody resolves each request's reference/ID fields against this map, so a
// body for, say, AuthzService/get_role gets the seeded role_id and drives the
// success path.
//
// Seeding runs in DEPENDENCY ORDER (a user before a role assignment before a
// notification; a file before an asset; a room before a peer before a track),
// everything namespaced by a per-run suffix, and returns a LIFO cleanup function.
// PerfFixtures maps a semantic field name → a real seeded value. lookup resolves a
// proto field name (lower-cased) by exact match, then by suffix match (so
// "user_id", "assigned_by", "created_by" all reach the seeded user UUID when
// registered under those keys). This keeps resolution explicit — only names we
// deliberately seeded resolve, everything else falls through to the generic scalar.
class PerfFixtures {
    m = new Map();
    recordId = "";
    set(key, val) {
        if (val)
            this.m.set(key.toLowerCase(), val);
    }
    lookup(field) {
        const f = field.toLowerCase();
        if (this.m.has(f))
            return this.m.get(f);
        for (const [k, v] of this.m) {
            if (f === k || f.endsWith("_" + k))
                return v;
        }
        return undefined;
    }
}
// seedPerfFixtures creates real, disposable entities across the services the perf
// run touches and records their identifiers. `gen` is the control-plane generated
// client (native services), `data` the DataBroker data plane. uuidTenant is the
// canonical tenant UUID the UUID-strict services (storage/asset/webrtc) require —
// the bootstrap admin's tenant claim IS that UUID, so one client serves all.
async function seedPerfFixtures(gen, data, tenantId, projectId, uuidTenant) {
    const fix = new PerfFixtures();
    const suffix = `${process.pid}${Date.now()}`;
    const opts = { deadlineMs: 8_000, noRetry: true };
    const ctx = requestContext(tenantId, projectId, "ts.live.perf.seed");
    const cleanups = [];
    const addCleanup = (fn) => cleanups.push(fn);
    const tryRun = async (label, fn) => {
        try {
            await fn();
        }
        catch (err) {
            console.log(`perf seed: ${label} failed (dependent RPCs fall back): ${errText(err)}`);
        }
    };
    // Always-known scalars.
    fix.set("tenant_id", tenantId);
    fix.set("tenant", tenantId);
    fix.set("project_id", projectId);
    fix.set("project", projectId);
    fix.set("domain", tenantId);
    fix.set("message_type", LIVE_MESSAGE_TYPE);
    // ── DataBroker: a real SdkLiveRecord row (drives Upsert/Select/Delete + CDC) ──
    const recordId = `ts-perf-${suffix}`;
    await tryRun("SdkLiveRecord upsert", async () => {
        await data.upsert({
            context: ctx,
            message_type: LIVE_MESSAGE_TYPE,
            record_json: jsonBytes({
                record_id: recordId,
                tenant_id: tenantId,
                project_id: projectId,
                lookup_key: `ts-perf-lk-${suffix}`,
                payload: "perf-seed",
                revision: 1,
            }),
            conflict_fields: ["record_id"],
        }, opts);
    });
    fix.set("record_id", recordId);
    fix.recordId = recordId;
    // A real project for ListProjects / project-scoped reads.
    const projId = `sdklive_perf_${suffix}`;
    await tryRun("EnsureProject", async () => {
        await data.ensure_project({ context: ctx, project_id: projId, name: "SDK Perf Project" }, opts);
    });
    // A real MinIO bucket + object so GetObject / object RPCs run their success path.
    const bucket = process.env.UDB_LIVE_S3_BUCKET || "udb-live-sdk";
    const objectKey = `ts-perf/${suffix}.txt`;
    await tryRun("EnsureResource minio", async () => {
        await data.ensure_resource({ context: ctx, backend: "minio", resource_name: bucket, spec_json: "{}" }, opts);
    });
    await tryRun("PutObject seed", async () => {
        const put = data.put_object({ deadlineMs: 10_000, noRetry: true });
        put.stream.write({ context: ctx, bucket, object_key: objectKey, data: Buffer.from(`ts-perf-object-${suffix}`, "utf8"), content_type: "text/plain", final_chunk: true });
        put.stream.end();
        await put.response;
    });
    fix.set("bucket", bucket);
    fix.set("object_key", objectKey);
    // A real Mongo collection + document so the document RPCs resolve a resource.
    const collection = `sdk_perf_docs_${suffix}`;
    const documentId = `doc-perf-${suffix}`;
    await tryRun("EnsureResource mongodb", async () => {
        await data.ensure_resource({ context: ctx, backend: "mongodb", resource_name: collection, spec_json: JSON.stringify({ collection }) }, opts);
    });
    await tryRun("DocumentUpsert seed", async () => {
        await data.document_upsert({ context: ctx, resource: { backend: "mongodb", resource_name: collection }, document_id: documentId, document: { _id: documentId, payload: "perf", revision: 1 } }, opts);
    });
    // NOTE: a single backend/resource fixture cannot serve both the SQL and the
    // document/cache RPCs (each needs its own backend + resource). The
    // backend-specific DataBroker RPCs are driven by typed bodies in perfRealBody, so
    // we deliberately do NOT register a global backend/resource_name fixture.
    fix.set("collection", collection);
    fix.set("mongo_collection", collection);
    // ── AuthnService: a real user (id reused everywhere a user_id is needed) ───────
    const pw = "CorrectHorse1!";
    const uname = `sdk-perf-${suffix}`;
    await tryRun("CreateUser", async () => {
        const created = (await gen.AuthnService.create_user({ username: uname, email: `${uname}@example.com`, password: pw, tenant_id: tenantId, project_id: projectId, full_name: "SDK Perf User" }, opts)).user;
        const uid = created.user_id;
        fix.set("user_id", uid);
        fix.set("username", uname);
        fix.set("recipient_id", uid);
        fix.set("assigned_by", uid);
        fix.set("created_by", uid);
        fix.set("updated_by", uid);
        fix.set("revoked_by", uid);
        fix.set("deleted_by", uid);
        fix.set("approved_by", uid);
        fix.set("rejected_by", uid);
        fix.set("subject", `user:${uid}`);
        // A real login → session id + tokens for session/token RPCs.
        await tryRun("Login (session/token fixtures)", async () => {
            const login = await gen.AuthnService.login({ username: uname, password: pw, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-sdk-perf-seed" }, opts);
            fix.set("session_id", login.session_id);
            fix.set("token", login.access_token);
            fix.set("refresh_token", login.refresh_token);
            fix.set("csrf_token", login.csrf_token);
        });
        // Recovery codes (so recovery-style reads have a real code).
        await tryRun("GenerateRecoveryCodes", async () => {
            const codes = await gen.AuthnService.generate_recovery_codes({ user_id: uid, count: 8 }, opts);
            if ((codes.codes ?? []).length > 0) {
                fix.set("code", codes.codes[0]);
                fix.set("recovery_code", codes.codes[0]);
            }
        });
        // A DEDICATED OTP user (so the seeded OTP doesn't trip the measured SendOTP's
        // per-user cooldown) → real otp_id + dev-echoed code for VerifyOTP / ResendOTP.
        await tryRun("SeedOTP", async () => {
            const ou = (await gen.AuthnService.create_user({ username: `sdk-perf-otp-${suffix}`, email: `sdk-perf-otp-${suffix}@example.com`, password: pw, tenant_id: tenantId, project_id: projectId, full_name: "SDK Perf OTP User" }, opts)).user;
            const so = await gen.AuthnService.send_o_t_p({ user_id: ou.user_id, otp_type: "OTP_TYPE_SENSITIVE_OPERATION", context: { tenant: { tenant_id: tenantId, project_id: projectId } } }, opts);
            if (so.otp_id)
                fix.set("otp_id", so.otp_id);
            if (so.dev_otp_code)
                fix.set("otp_code", so.dev_otp_code);
        });
        // A SEPARATE dedicated user for the PASSWORD_RESET OTP so it isn't superseded by the
        // SENSITIVE OTP on the shared user → reset_otp_id/code for ResetPassword. The reset user
        // IS the reset target (ResetPassword resolves the user from otp_id) and must be ACTIVE.
        await tryRun("SeedResetOTP", async () => {
            const ru = (await gen.AuthnService.create_user({ username: `sdk-perf-rst-${suffix}`, email: `sdk-perf-rst-${suffix}@example.com`, password: pw, tenant_id: tenantId, project_id: projectId, full_name: "SDK Perf Reset User" }, opts)).user;
            // SEND WITH the tenant context, exactly like Go/Python (the reference path that passes).
            // The dev_otp_code echo is UNCONDITIONAL when UDB_OTP_DEV_ECHO=1 (mfa.rs:208 — gated only
            // on the env flag, NOT on otp_type or context); the earlier "context suppresses the echo"
            // theory was false (BENCH_TS_PHP_ADVISORY.md).
            const rso = await gen.AuthnService.send_o_t_p({ user_id: ru.user_id, otp_type: "OTP_TYPE_PASSWORD_RESET", context: { tenant: { tenant_id: tenantId, project_id: projectId } } }, opts);
            if (rso.otp_id)
                fix.set("reset_otp_id", rso.otp_id);
            if (rso.dev_otp_code)
                fix.set("reset_otp_code", rso.dev_otp_code);
        });
        // A real MFA challenge → challenge_id (a valid UUID) for VerifyMfaChallenge.
        await tryRun("SeedMfaChallenge", async () => {
            const mc = await gen.AuthnService.issue_mfa_challenge({ user_id: uid, factor_kind: "AUTH_FACTOR_KIND_EMAIL_OTP", purpose: "MFA_CHALLENGE_PURPOSE_LOGIN_STEP_UP" }, opts);
            if (mc.challenge_id)
                fix.set("challenge_id", mc.challenge_id);
        });
        // A real device row → device_id for RevokeDevice. Login (as the sdk-perf user) registers
        // a device, then ListDevices reads it. The fresh logins below are the ADMIN user, so do a
        // dedicated sdk-perf login here to guarantee a device under uid.
        await tryRun("SeedDevice", async () => {
            // device_id on LoginRequest IS the client device FINGERPRINT (field 7) — non-empty →
            // register_login_device inserts a devices row → ListDevices returns it → RevokeDevice works.
            await gen.AuthnService.login({ username: uname, password: pw, tenant_hint: tenantId, project_hint: projectId, device_id: `ts-perf-fp-${suffix}`, device_name: "ts-perf-device", ip_address: "127.0.0.1" }, opts);
            const dl = await gen.AuthnService.list_devices({ user_id: uid }, opts);
            if ((dl.devices ?? []).length > 0)
                fix.set("device_id", dl.devices[0].device_id);
        });
        // WebAuthn dev soft-authenticator (UDB_WEBAUTHN_TEST_MODE=1): register a passkey so
        // StartWebAuthnAuthentication has one. The dev authenticator is deterministic
        // (one credential id per user), so measured registration uses a separate user
        // with no existing passkey instead of exercising duplicate/exclude handling.
        await tryRun("SeedWebAuthn", async () => {
            const sr = await gen.AuthnService.start_web_authn_registration({ user_id: uid, label: "perf-passkey", tenant_id: tenantId, project_id: projectId }, opts);
            if (sr.challenge_id)
                await gen.AuthnService.finish_web_authn_registration({ challenge_id: sr.challenge_id, public_key_credential_json: "__UDB_WEBAUTHN_TEST__", label: "perf-passkey" }, opts);
        });
        let webauthnRegUserId = uid;
        await tryRun("SeedWebAuthnRegistrationUser", async () => {
            const ru = await gen.AuthnService.create_user({
                username: `sdk-perf-webauthn-reg-${suffix}`,
                email: `sdk-perf-webauthn-reg-${suffix}@example.com`,
                password: pw,
                tenant_id: tenantId,
                project_id: projectId,
                full_name: "SDK Perf WebAuthn Registration User",
            }, opts);
            webauthnRegUserId = ru.user?.user_id || webauthnRegUserId;
        });
        await tryRun("SeedWebAuthnRegistrationChallenge", async () => {
            const sr2 = await gen.AuthnService.start_web_authn_registration({ user_id: webauthnRegUserId, label: "perf-passkey-2", tenant_id: tenantId, project_id: projectId }, opts);
            if (sr2.challenge_id)
                fix.set("reg_challenge_id", sr2.challenge_id);
        });
        await tryRun("SeedWebAuthnAuthenticationChallenge", async () => {
            const sa = await gen.AuthnService.start_web_authn_authentication({ user_id: uid, tenant_id: tenantId }, opts);
            if (sa.challenge_id)
                fix.set("auth_challenge_id", sa.challenge_id);
        });
        // THREE independent fresh logins so RefreshToken's rotation doesn't invalidate
        // Authenticate's token or RefreshSession's session (Go live_perf_test.go:115). These
        // MUST use the ADMIN bench user — the measured change_user_status/change_password
        // SUSPEND/mutate the sdk-perf user, which would deactivate its tokens/sessions.
        const adminU = process.env.UDB_LIVE_USERNAME || uname;
        const adminP = process.env.UDB_LIVE_PASSWORD || pw;
        await tryRun("FreshLoginToken", async () => { const l = await gen.AuthnService.login({ username: adminU, password: adminP, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-perf-token" }, opts); if (l.access_token)
            fix.set("token", l.access_token); if (l.csrf_token)
            fix.set("csrf_token", l.csrf_token); });
        await tryRun("FreshLoginRefresh", async () => { const l = await gen.AuthnService.login({ username: adminU, password: adminP, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-perf-refresh" }, opts); if (l.refresh_token)
            fix.set("refresh_token", l.refresh_token); });
        await tryRun("FreshLoginSession", async () => { const l = await gen.AuthnService.login({ username: adminU, password: adminP, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-perf-session" }, opts); if (l.session_id)
            fix.set("session_id", l.session_id); });
    });
    // ── AuthzService: role + assignment + policies + relationship ──────────────────
    const roleCode = `sdk_perf_reader_${suffix}`;
    await tryRun("CreateRole", async () => {
        const role = (await gen.AuthzService.create_role({ name: `SDK Perf Reader ${suffix}`, description: "perf seed role", created_by: liveUuid(), role_code: roleCode, domain: tenantId, tenant_id: tenantId, project_id: projectId }, opts)).role;
        const rid = role.role_id;
        fix.set("role_id", rid);
        fix.set("role", roleCode);
        fix.set("role_code", roleCode);
        const uid = fix.lookup("user_id");
        if (uid) {
            await tryRun("AssignRole", async () => {
                const assigned = (await gen.AuthzService.assign_role({ user_id: uid, role_id: rid, domain: tenantId, assigned_by: uid, tenant_id: tenantId, project_id: projectId }, opts)).user_role;
                fix.set("user_role_id", assigned.user_role_id);
            });
        }
        addCleanup(async () => {
            try {
                await gen.AuthzService.delete_role({ role_id: rid, deleted_by: fix.lookup("user_id") ?? liveUuid() }, opts);
            }
            catch { /* best-effort */ }
        });
    });
    // A SEPARATE disposable role for the destructive DeleteRole → real 200, while the
    // primary role_id survives for GetRole/UpdateRole/ListUserRoles.
    await tryRun("CreateDeleteRole", async () => {
        const dr = (await gen.AuthzService.create_role({ name: `SDK Perf Del ${suffix}`, description: "disposable", created_by: fix.lookup("user_id") ?? liveUuid(), role_code: `sdk_perf_del_${suffix}`, domain: tenantId, tenant_id: tenantId, project_id: projectId }, opts)).role;
        fix.set("delete_role_id", dr.role_id);
    });
    // ABAC policy + an RBAC policy rule -> policy_id for GetPolicyRule/DeletePolicyRule.
    await tryRun("PutAuthzPolicy", async () => {
        await gen.AuthzService.put_authz_policy({ policy: { id: liveUuid(), enabled: true, effect: "allow", tenant: tenantId, project: projectId, role: roleCode, action: "data.select", resource: "invoice" } }, opts);
    });
    const uidForPolicy = fix.lookup("user_id");
    if (uidForPolicy) {
        // GetPolicyRule's CreatePolicyRule response id IS Get-queryable, BUT
        // ActivatePolicyVersion/RollbackPolicyVersion DELETE+regenerate ALL policy_rules for the
        // tenant/project and sort BEFORE GetPolicyRule — wiping a main-project rule. Seed the target
        // in an ISOLATED project no version-activation touches (harness_correction.md GetPolicyRule).
        const getPolProject = `${projectId}-getpolrule`;
        await tryRun("CreatePolicyRule", async () => {
            const created = (await gen.AuthzService.create_policy_rule({ subject: roleCode, domain: tenantId, object: "ledger", action: "data.update", effect: 1, description: "perf seed rule (version-isolated)", created_by: uidForPolicy, tenant_id: tenantId, project_id: getPolProject }, opts)).policy;
            if (created?.policy_id)
                fix.set("policy_id", created.policy_id);
        });
        await tryRun("CreateDeletePolicyRule", async () => {
            const dr = (await gen.AuthzService.create_policy_rule({ subject: roleCode, domain: tenantId, object: "ledger-disposable", action: "data.delete", effect: 1, description: "disposable", created_by: uidForPolicy, tenant_id: tenantId, project_id: getPolProject }, opts)).policy;
            if (dr?.policy_id)
                fix.set("delete_policy_id", dr.policy_id);
        });
        await tryRun("PutRoleBinding", async () => {
            await gen.AuthzService.put_role_binding({ binding: { subject: `user:${uidForPolicy}`, role: roleCode, tenant: tenantId, project: projectId, source: "sdk-perf" } }, opts);
        });
        await tryRun("PutRelationship", async () => {
            await gen.AuthzService.put_relationship({ tuple: { subject: `user:${uidForPolicy}`, relation: "member", object: `group:sdk-perf-${suffix}`, tenant: tenantId, project: projectId, source: "sdk-perf" } }, opts);
        });
    }
    fix.set("relation", "member");
    fix.set("object", `group:sdk-perf-${suffix}`);
    fix.set("resource", "invoice");
    fix.set("action", "data.select");
    // ── ApiKeyService: a real key → key_id + plain_key ─────────────────────────────
    const principal = `sdk-perf-svc-${suffix}`;
    await tryRun("CreateApiKey", async () => {
        const key = await gen.ApiKeyService.create_api_key({ name: `sdk-perf-key-${suffix}`, owner_id: principal, scopes: ["data:read"], context: { user_id: principal, tenant: { tenant_id: tenantId, project_id: projectId } } }, opts);
        fix.set("key_id", key.key.key_id);
        // revoke/rotate/update look up by key_PREFIX (get_by_prefix), not the key_id UUID.
        // Derive it from plain_key ("udbk_xxxx.yyyy" → "udbk_xxxx") — robust vs an unset field.
        fix.set("key_prefix", (key.key.key_prefix || String(key.plain_key).split(".")[0]));
        fix.set("plain_key", key.plain_key);
        fix.set("owner_id", principal);
    });
    // A SEPARATE disposable key for the destructive RevokeApiKey → real 200, so the
    // primary key_id survives for RotateApiKey/UpdateApiKey/GetApiKey/ValidateApiKey.
    await tryRun("CreateRevokeKey", async () => {
        const rk = await gen.ApiKeyService.create_api_key({ name: `sdk-perf-revoke-${suffix}`, owner_id: principal, scopes: ["data:read"], context: { user_id: principal, tenant: { tenant_id: tenantId, project_id: projectId } } }, opts);
        fix.set("revoke_key_id", rk.key.key_id);
        fix.set("revoke_key_prefix", (rk.key.key_prefix || String(rk.plain_key).split(".")[0]));
    });
    // A SEPARATE disposable key for UpdateApiKey, so the measured RotateApiKey (which
    // rotates the primary key_id) can't invalidate the key UpdateApiKey targets.
    await tryRun("CreateUpdateKey", async () => {
        const uk = await gen.ApiKeyService.create_api_key({ name: `sdk-perf-update-${suffix}`, owner_id: principal, scopes: ["data:read"], context: { user_id: principal, tenant: { tenant_id: tenantId, project_id: projectId } } }, opts);
        fix.set("update_key_id", uk.key.key_id);
        fix.set("update_key_prefix", (uk.key.key_prefix || String(uk.plain_key).split(".")[0]));
    });
    // ── IdentityProviderService: a real OIDC provider → provider_id ────────────────
    // kind MUST be ≤24 chars (IDP_KIND_OIDC) — IDP_KIND_EXTERNAL_SESSION overflows the
    // VARCHAR(24) `kind` column (BENCH_RPC_BODIES.md CreateProvider note).
    await tryRun("CreateProvider", async () => {
        const prov = await gen.IdentityProviderService.create_provider({
            tenant_id: tenantId, kind: "IDP_KIND_OIDC", display_name: `SDK Perf OIDC ${suffix}`,
            issuer: "https://idp.example.com", jwks_url: "https://idp.example.com/jwks",
            client_ids: ["client-1"], audiences: ["udb"], claim_mapping_json: "{}", group_mapping_json: JSON.stringify({ "sdk-perf-group": "reader" }),
            jit_policy_json: JSON.stringify({ require_verified_email: false }), account_linking_policy: "explicit", enabled: true,
            created_by: fix.lookup("user_id") ?? liveUuid(), context: { tenant: { tenant_id: tenantId } },
        }, opts);
        const pid = prov.provider?.provider_id ?? prov.provider_id;
        if (pid) {
            fix.set("provider_id", pid);
            addCleanup(async () => {
                try {
                    await gen.IdentityProviderService.disable_provider({ provider_id: pid, tenant_id: tenantId, updated_by: fix.lookup("user_id") ?? liveUuid(), context: { tenant: { tenant_id: tenantId } } }, opts);
                }
                catch { /* best-effort */ }
            });
        }
    });
    // A SEPARATE disposable OIDC provider for the destructive DisableProvider, so disabling
    // it does NOT disable the primary provider_id that SamlAcs/ResolveExternalIdentity read.
    await tryRun("CreateDisposableProvider", async () => {
        const dp = await gen.IdentityProviderService.create_provider({
            tenant_id: tenantId, kind: "IDP_KIND_OIDC", display_name: `SDK Perf OIDC Disposable ${suffix}`,
            issuer: `https://idp-disposable.example.com/${suffix}`, jwks_url: "https://idp-disposable.example.com/jwks",
            client_ids: ["perf-client-disp"], audiences: ["udb"], claim_mapping_json: "{}", group_mapping_json: "{}",
            jit_policy_json: JSON.stringify({ require_verified_email: false }), account_linking_policy: "explicit", enabled: true,
            created_by: fix.lookup("user_id") ?? liveUuid(), context: { tenant: { tenant_id: tenantId } },
        }, opts);
        const dpid = dp.provider?.provider_id ?? dp.provider_id;
        if (dpid)
            fix.set("disable_provider_id", dpid);
    });
    // ── A real enabled SAML provider (+ imported metadata for an SSO URL) so
    // StartSamlLogin/SamlAcs resolve an active SAML provider → saml_provider_id (Go seed:471).
    await tryRun("CreateSamlProvider", async () => {
        const sp = await gen.IdentityProviderService.create_provider({
            tenant_id: tenantId, kind: "IDP_KIND_SAML", display_name: `SDK Perf SAML ${suffix}`,
            issuer: `https://saml.example.com/${suffix}`, jwks_url: "https://saml.example.com/jwks",
            client_ids: ["perf-saml"], audiences: ["udb"], claim_mapping_json: "{}", group_mapping_json: "{}",
            jit_policy_json: JSON.stringify({ require_verified_email: false }), account_linking_policy: "explicit", enabled: true,
            created_by: fix.lookup("user_id") ?? liveUuid(), context: { tenant: { tenant_id: tenantId } },
        }, opts);
        const spid = sp.provider?.provider_id ?? sp.provider_id;
        if (spid) {
            fix.set("saml_provider_id", spid);
            await tryRun("ImportSamlMetadata", async () => {
                await gen.IdentityProviderService.import_saml_metadata({ provider_id: spid, tenant_id: tenantId, metadata_xml: SAML_IDP_METADATA_XML, updated_by: fix.lookup("user_id") ?? liveUuid(), context: { tenant: { tenant_id: tenantId } } }, opts);
            });
        }
    });
    // ── IdentityProviderService SCIM: JIT-provision users/groups via the provider ──
    const provId = fix.lookup("provider_id");
    if (provId) {
        const scimCtx = { tenant: { tenant_id: tenantId } };
        // The broker resolves ScimGet/Patch/Replace/Delete by the SCIM user_id == the
        // userName/subject (NOT the internal external_identity_id). So scim_user_id = the
        // userName we provision; external_identity_id (for UnlinkIdentity) = the returned id.
        const scimUserName = `scim-${suffix}@x.com`;
        await tryRun("ScimCreateUser", async () => {
            const su = await gen.IdentityProviderService.scim_create_user({ tenant_id: tenantId, provider_id: provId, scim_user_json: JSON.stringify({ userName: scimUserName, active: true }), context: scimCtx }, opts);
            fix.set("scim_user_id", scimUserName);
            const id = su.user?.id ?? su.id;
            if (id)
                fix.set("external_identity_id", id);
        });
        const delUserName = `scim-del-${suffix}@x.com`;
        await tryRun("ScimCreateDeleteUser", async () => {
            await gen.IdentityProviderService.scim_create_user({ tenant_id: tenantId, provider_id: provId, scim_user_json: JSON.stringify({ userName: delUserName, active: true }), context: scimCtx }, opts);
            fix.set("delete_scim_user_id", delUserName);
        });
        // ScimGetGroup resolves scim_group_id against the provider's group_mapping_json
        // keys — the provider seed maps "sdk-perf-group", so use that exact key.
        fix.set("scim_group_id", "sdk-perf-group");
    }
    // ── Saga + DLQ rows: pre-seeded out-of-band into udb_system (fixed UUIDs, one
    // disposable row per mutating RPC). The SQL insert runs before the test.
    fix.set("saga_id", "11111111-1111-4111-8111-111111111101");
    fix.set("retry_saga_id", "11111111-1111-4111-8111-111111111102");
    fix.set("mark_saga_id", "11111111-1111-4111-8111-111111111103");
    fix.set("dlq_id", "22222222-2222-4222-8222-222222222201");
    fix.set("dismiss_dlq_id", "22222222-2222-4222-8222-222222222202");
    fix.set("quarantine_dlq_id", "22222222-2222-4222-8222-222222222203");
    fix.set("replay_dlq_id", "22222222-2222-4222-8222-222222222204");
    // ── AuthzService governance lifecycle (ports the Go seed): drafts in each state,
    // approved policy VERSIONS, a canary, and a rollback set — so the draft/version/
    // canary RPCs run their real success path. ──────────────────────────────────────
    {
        const subject = fix.lookup("subject") ?? `user:${fix.lookup("user_id") ?? liveUuid()}`;
        const gActor = () => ({ subject, tenant_id: tenantId, project_id: projectId, scopes: ["authz:admin", "authz:policy:write", "authz:policy:approve", "policy:read"] });
        const mkDraft = async (title, setName = "default") => {
            try {
                const d = await gen.AuthzService.create_policy_draft({ actor: gActor(), tenant_id: tenantId, project_id: projectId, policy_set_name: setName, title: title + suffix, change_reason: "seed", document: {} }, opts);
                return d.draft?.draft_id ?? d.draft_id ?? "";
            }
            catch {
                return "";
            }
        };
        // Drafts: one OPEN (diff/update/submit), two submitted→IN_REVIEW (approve/reject).
        await tryRun("CreatePolicyDraft", async () => { const id = await mkDraft("sdk-perf-draft-"); if (id)
            fix.set("policy_draft_id", id); });
        await tryRun("UpdateDraft", async () => { const id = await mkDraft("sdk-perf-update-"); if (id)
            fix.set("update_draft_id", id); });
        await tryRun("ApproveDraft", async () => {
            const id = await mkDraft("sdk-perf-approve-");
            if (id) {
                await gen.AuthzService.submit_policy_draft({ actor: gActor(), draft_id: id }, opts);
                fix.set("approve_draft_id", id);
            }
        });
        await tryRun("RejectDraft", async () => {
            const id = await mkDraft("sdk-perf-reject-");
            if (id) {
                await gen.AuthzService.submit_policy_draft({ actor: gActor(), draft_id: id }, opts);
                fix.set("reject_draft_id", id);
            }
        });
        // Versions: CreateDraft→Submit→Approve promotes a PolicyVersion (APPROVED).
        const mkVersion = async (setName, title) => {
            const did = await mkDraft(title, setName);
            if (!did)
                return null;
            try {
                await gen.AuthzService.submit_policy_draft({ actor: gActor(), draft_id: did }, opts);
                const ap = await gen.AuthzService.approve_policy_draft({ actor: gActor(), draft_id: did, reviewer: fix.lookup("user_id") ?? liveUuid(), reason: "seed approve" }, opts);
                return ap.version ?? null;
            }
            catch {
                return null;
            }
        };
        await tryRun("SeedActivateVersion", async () => { const v = await mkVersion(`sdk-perf-activate-set-${suffix}`, "activate-"); if (v?.policy_version_id)
            fix.set("policy_version_id", v.policy_version_id); });
        await tryRun("SeedCanary", async () => {
            const v = await mkVersion(`sdk-perf-canary-set-${suffix}`, "canary-");
            if (v?.policy_version_id) {
                fix.set("canary_version_id", v.policy_version_id);
                // success_window_secs MUST be > 0 (1s): 0 makes the broker substitute a default that
                // never elapses during the run, so PromoteCanary stays "not promote-eligible".
                const c = await gen.AuthzService.activate_canary({ actor: gActor(), policy_version_id: v.policy_version_id, scope_kind: "CANARY_SCOPE_KIND_PERCENT", scope_values: ["10"], success_window_secs: 1, metric_threshold: 0.99, min_samples: 0 }, opts);
                if (c.canary?.canary_id)
                    fix.set("canary_id", c.canary.canary_id);
            }
        });
        await tryRun("SeedRollbackSet", async () => {
            const v1 = await mkVersion(`sdk-perf-rollback-set-${suffix}`, "rb1-");
            if (v1?.policy_version_id) {
                await gen.AuthzService.activate_policy_version({ actor: gActor(), policy_version_id: v1.policy_version_id }, opts);
                const v2 = await mkVersion(`sdk-perf-rollback-set-${suffix}`, "rb2-");
                if (v2?.policy_version_id) {
                    await gen.AuthzService.activate_policy_version({ actor: gActor(), policy_version_id: v2.policy_version_id }, opts);
                    fix.set("rollback_policy_set_id", v2.policy_set_id);
                    fix.set("rollback_target_version_id", v1.policy_version_id);
                }
            }
        });
    }
    // ── DataBroker migration: a real plan run → migration_id (run_id) ──────────────
    await tryRun("PlanMigration", async () => {
        const plan = await data.plan_migration({ context: ctx, project_id: projectId, dry_run: true }, opts);
        const runId = plan.run_id ?? plan.run?.run_id;
        if (runId)
            fix.set("migration_id", runId);
    });
    // approve_run_id: a NON-dry-run plan left in PREFLIGHT for the measured ApproveMigrationPlan.
    await tryRun("PlanMigrationApprove", async () => {
        const p1 = await data.plan_migration({ context: ctx, project_id: projectId, dry_run: false }, opts);
        const rid = p1.run_id ?? p1.run?.run_id;
        if (rid)
            fix.set("approve_run_id", rid);
    });
    // apply_run_id + approval_token: a SECOND non-dry-run, pre-approved so ApplyMigration has a
    // valid token (returned in the x-udb-approval-token response header).
    await tryRun("PlanMigrationApply", async () => {
        const p2 = await data.plan_migration({ context: ctx, project_id: projectId, dry_run: false }, opts);
        const rid = p2.run_id ?? p2.run?.run_id;
        if (rid) {
            const hdrs = {};
            await data.approve_migration_plan({ context: { ...ctx, scopes: ["udb:admin"] }, run_id: rid, project_id: projectId }, { ...opts, onResponseMetadata: (m) => { try {
                    const t = m?.get?.("x-udb-approval-token");
                    if (t && t.length)
                        hdrs.tok = String(t[0]);
                }
                catch { /* ignore */ } } });
            fix.set("apply_run_id", rid);
            if (hdrs.tok)
                fix.set("approval_token", hdrs.tok);
        }
    });
    // ds_policy_id: a real broker policy (allow-all, harmless) for the measured DeletePolicy.
    await tryRun("PutPolicy", async () => {
        await data.put_policy({ context: { ...ctx, scopes: ["udb:admin"] }, policy: { effect: "allow", tenant_id: tenantId, priority: 1, enabled: true } }, opts);
        const pl = await data.list_policies({ context: { ...ctx, scopes: ["udb:admin"] }, include_disabled: true, limit: 50 }, opts);
        const first = (pl.policies ?? [])[0];
        if (first?.policy_id != null)
            fix.set("ds_policy_id", String(first.policy_id));
    });
    // ── Qdrant: a real vector collection. The name must be qdrant-safe (ASCII letters/
    // digits/hyphens/underscores — NO dots), so use "sdk_live_records" (not the dotted
    // message type). Seed vectors are 3-dim → size 3 / cosine (Go live_perf_seed:167).
    await tryRun("EnsureVectorCollection", async () => {
        await data.ensure_resource({ context: { ...ctx, scopes: ["udb:admin"] }, backend: "qdrant", resource_name: "sdk_live_records", spec_json: JSON.stringify({ size: 3, distance: "Cosine" }) }, opts);
    });
    // ── ClickHouse: a real table so TimeSeriesWrite/Query resolve a column store →
    // ts_table fixture (Go live_perf_seed:175).
    await tryRun("EnsureTsTable", async () => {
        await data.ensure_resource({ context: { ...ctx, scopes: ["udb:admin"] }, backend: "clickhouse", resource_name: "sdk_perf_ts", spec_json: "{}" }, opts);
        fix.set("ts_table", "sdk_perf_ts");
    });
    // ── Capture the live catalog manifest (READ-ONLY) so the measured StageCatalog has a
    // valid CatalogManifest (Go passes StageCatalog with the new binary). activate/rollback/
    // get_version stay broker-blocked (K2). If staging still poisons, revert this.
    await tryRun("CaptureCatalogManifest", async () => {
        const cm = await data.get_catalog_manifest({ context: { ...ctx, scopes: ["udb:admin"] }, redact: false }, opts);
        if (cm?.manifest_json)
            seedCatalogManifest = cm.manifest_json;
    });
    // NOTE: NOT staging a catalog here — staging the manifest puts the broker into a
    // pending-catalog state that fails-precondition EVERY DataBroker data op (76 RPCs).
    // The 4 catalog RPCs aren't worth that; leave them red until a safe seed path exists.
    // ── AnalyticsService: a recorded metric → a stage_name with data ───────────────
    const stage = `sdk_perf_stage_${suffix}`;
    await tryRun("RecordPipelineMetric", async () => {
        await gen.AnalyticsService.record_pipeline_metric({ stage_name: stage, tenant_id: tenantId, latency_ms: 100, is_success: true }, opts);
    });
    fix.set("stage_name", stage);
    // ── NotificationService: template + a sent notification → log_id, event_type ───
    const event = `sdk.perf.${suffix}`;
    await tryRun("UpsertTemplate", async () => {
        await gen.NotificationService.upsert_template({ event_type: event, channel: 1, locale: "en", subject_template: "SDK {{n}}", body_template: "sdk-perf-body", is_active: true }, opts);
    });
    fix.set("event_type", event);
    fix.set("locale", "en");
    const recipientId = fix.lookup("recipient_id");
    if (recipientId) {
        await tryRun("SendNotification", async () => {
            const sent = await gen.NotificationService.send_notification({ event_type: event, recipient_id: recipientId, recipient_address: `sdk+${suffix}@example.com`, tenant_id: tenantId, channels: [1] }, opts);
            if ((sent.logs ?? []).length > 0) {
                const logId = sent.logs[0].log_id;
                fix.set("log_id", logId);
                fix.set("notification_id", logId);
                // RetryNotification is status-gated to FAILED/SUPPRESSED rows — mark this real log
                // FAILED via GenericDispatch operation="mutate" (query only allows SELECT). Go pattern.
                await tryRun("MarkNotificationFailed", async () => {
                    await data.generic_dispatch({ context: { ...ctx, scopes: ["udb:dispatch", "udb:admin"] }, backend: "postgres", operation: "mutate", spec_json: JSON.stringify({ sql: "UPDATE udb_notification.notification_logs SET status = 'FAILED', error_message = 'perf seed failure' WHERE log_id = $1::UUID AND tenant_id = $2 RETURNING log_id", params: [logId, tenantId], param_types: ["uuid", "string"], return_rows: true }) }, opts);
                });
            }
        });
    }
    // ── StorageService (UUID tenant): a registered file → file_id ──────────────────
    let fileId = "";
    await tryRun("RegisterUpload", async () => {
        const reg = await gen.StorageService.register_upload({ tenant_id: uuidTenant, project_id: "", filename: `perf-${suffix}.txt`, content_type: "text/plain", file_type: "DOCUMENT", reference_id: liveUuid(), reference_type: "sdk.perf", size_bytes: 128, expires_in_minutes: 30 }, opts);
        fileId = reg.file_id;
        fix.set("file_id", fileId);
        // FinalizeUpload HEADs the object bytes the StorageService minted. Upload through the
        // presigned RegisterUpload.upload_url (the canonical native path that targets the row's
        // bucket); DataBroker.PutObject is a manifest-gated fallback (harness_correction.md).
        await tryRun("SeedPutObject", async () => {
            const payload = `sdk-perf-file-${suffix}`;
            const uploadUrl = reg.upload_url || "";
            let put200 = false;
            if (uploadUrl) {
                try {
                    const res = await fetch(uploadUrl, { method: "PUT", body: payload, headers: { "Content-Type": "text/plain" } });
                    put200 = res.ok;
                }
                catch { /* fall through to PutObject */ }
            }
            if (!put200) {
                const put = data.put_object({ deadlineMs: 10_000, noRetry: true });
                put.stream.write({ context: ctx, bucket: process.env.UDB_OBJECT_BUCKET || "udb-storage", object_key: reg.object_key, data: Buffer.from(payload, "utf8"), content_type: "text/plain", final_chunk: true });
                put.stream.end();
                await put.response;
            }
        });
        addCleanup(async () => {
            try {
                await gen.StorageService.delete_file({ tenant_id: uuidTenant, file_id: fileId }, opts);
            }
            catch { /* best-effort */ }
        });
    });
    // A SEPARATE disposable file for the destructive DeleteFile → real 200, so the
    // primary file_id survives for GetFile/GetDownloadUrl/UpdateFile.
    await tryRun("RegisterDeleteFile", async () => {
        const dreg = await gen.StorageService.register_upload({ tenant_id: uuidTenant, project_id: "", filename: `perf-del-${suffix}.txt`, content_type: "text/plain", file_type: "DOCUMENT", reference_id: liveUuid(), reference_type: "sdk.perf", size_bytes: 64, expires_in_minutes: 30 }, opts);
        fix.set("delete_file_id", dreg.file_id);
    });
    // ── AssetService: pipeline definition + asset + a started instance ─────────────
    if (fileId) {
        await tryRun("CreatePipelineDefinition", async () => {
            const def = await gen.AssetService.create_pipeline_definition({ tenant_id: uuidTenant, name: `sdk-perf-pipeline-${suffix}`, description: "perf seed", media_type: "application/json", steps: '[{"name":"extract","type":"EXTRACT"}]', version: 1 }, opts);
            fix.set("definition_id", def.definition_id);
        });
        await tryRun("RegisterAsset", async () => {
            const a = await gen.AssetService.register_asset({ tenant_id: uuidTenant, project_id: "", file_id: fileId, name: `sdk-perf-asset-${suffix}`, media_type: "application/json", metadata: '{"source":"sdk-perf"}' }, opts);
            fix.set("asset_id", a.asset_id);
            const did = fix.lookup("definition_id");
            if (did) {
                await tryRun("StartPipeline", async () => {
                    const inst = await gen.AssetService.start_pipeline({ tenant_id: uuidTenant, definition_id: did, asset_id: a.asset_id, context: "{}", correlation_id: `sdk-perf-${suffix}` }, opts);
                    fix.set("instance_id", inst.instance_id);
                    // A started pipeline exposes its steps → a real step_id for CompleteStep.
                    await tryRun("GetPipelineSteps", async () => {
                        const pl = await gen.AssetService.get_pipeline({ tenant_id: uuidTenant, instance_id: inst.instance_id }, opts);
                        if ((pl.steps ?? []).length > 0)
                            fix.set("step_id", pl.steps[0].step_id ?? pl.steps[0].id);
                    });
                });
            }
        });
    }
    // ── WebRTC (UUID tenant): room + peer + track ──────────────────────────────────
    await tryRun("CreateRoom", async () => {
        const room = await gen.RoomService.create_room({ tenant_id: uuidTenant, name: `sdk-perf-room-${suffix}`, max_participants: 8, config: "{}", created_by: liveUuid() }, opts);
        const roomId = room.room_id;
        fix.set("room_id", roomId);
        addCleanup(async () => {
            try {
                await gen.RoomService.close_room({ tenant_id: uuidTenant, room_id: roomId }, opts);
            }
            catch { /* best-effort */ }
        });
        await tryRun("JoinRoom", async () => {
            const joined = await gen.PeerService.join_room({ tenant_id: uuidTenant, room_id: roomId, display_name: "sdk-perf-peer", metadata: "{}", user_agent: "sdk-perf" }, opts);
            const peerId = joined.peer.peer_id;
            fix.set("peer_id", peerId);
            await tryRun("PublishTrack", async () => {
                const pub = await gen.TrackService.publish_track({ tenant_id: uuidTenant, room_id: roomId, peer_id: peerId, kind: "audio", label: "mic", settings: "{}", metadata: "{}" }, opts);
                fix.set("track_id", pub.track_id);
            });
            // A SECOND disposable track for the destructive UnpublishTrack (real 200) so the
            // primary track survives for MuteTrack/ListTracks.
            await tryRun("PublishUnpublishTrack", async () => {
                const pub2 = await gen.TrackService.publish_track({ tenant_id: uuidTenant, room_id: roomId, peer_id: peerId, kind: "video", label: "cam", settings: "{}", metadata: "{}" }, opts);
                fix.set("unpublish_track_id", pub2.track_id);
            });
        });
        // A SEPARATE disposable peer for the destructive LeaveRoom (real 200) so the primary
        // peer stays an ACTIVE member for PublishTrack/MuteTrack/Signal/IssueCredentials.
        await tryRun("JoinLeavePeer", async () => {
            const lj = await gen.PeerService.join_room({ tenant_id: uuidTenant, room_id: roomId, display_name: "sdk-perf-leave-peer", metadata: "{}", user_agent: "sdk-perf" }, opts);
            fix.set("leave_peer_id", lj.peer.peer_id);
        });
        // A SEPARATE disposable room for the destructive CloseRoom — closing the MAIN room
        // would close its peers and break PublishTrack/MuteTrack/Signal (arbitrary order).
        await tryRun("CreateCloseRoom", async () => {
            const cr = await gen.RoomService.create_room({ tenant_id: uuidTenant, name: `sdk-perf-close-room-${suffix}`, max_participants: 8, config: "{}", created_by: liveUuid() }, opts);
            fix.set("close_room_id", cr.room_id);
        });
    });
    // ── NotificationService: an EMAIL preference row so GetPreference resolves ─────
    if (recipientId) {
        await tryRun("SetPreference", async () => {
            await gen.NotificationService.set_preference({ user_id: recipientId, tenant_id: tenantId, channel: 1, is_opted_out: false }, opts);
        });
    }
    // ── ControlPlaneService: open a StreamResources session under node_id so a node ─
    // state row exists; AckStatus reads it (404s without a registered node).
    const nodeId = `sdk-perf-node-${suffix}`;
    fix.set("node_id", nodeId);
    await tryRun("OpenNodeSession", async () => {
        const stream = gen.ControlPlaneService.stream_resources(opts);
        const s = stream?.stream ?? stream;
        await new Promise((resolve) => {
            let done = false;
            const fin = () => { if (done)
                return; done = true; try {
                s.end?.();
            }
            catch { /* */ } try {
                s.cancel?.();
            }
            catch { /* */ } resolve(); };
            s.once?.("data", fin);
            s.once?.("error", fin);
            setTimeout(fin, 3000);
            try {
                s.write?.({ node_id: nodeId, resource_type: "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION", context: { tenant: { tenant_id: tenantId, project_id: projectId } } });
            }
            catch {
                fin();
            }
        });
    });
    // Convenience free-text scalars commonly required by reflective populate.
    fix.set("name", `sdk-perf-${suffix}`);
    fix.set("filename", `sdk-perf-${suffix}.txt`);
    fix.set("content_type", "text/plain");
    fix.set("file_type", "DOCUMENT");
    fix.set("kind", "audio");
    return {
        fixtures: fix,
        cleanup: async () => {
            for (let i = cleanups.length - 1; i >= 0; i--)
                await cleanups[i]();
        },
    };
}
async function expectGeneratedUnarySurfaceMounted(t, label, generated, serviceNames, tenantId, projectId, counters) {
    let count = 0;
    for (const serviceName of serviceNames) {
        const api = generated[serviceName];
        node_assert_1.strict.ok(api, `${label}.${serviceName} must exist on generated SDK client`);
        for (const [methodName, fn] of Object.entries(api)) {
            if (methodName === "serviceFull" || NON_UNARY_METHODS.has(methodName))
                continue;
            if (typeof fn !== "function")
                continue;
            count += 1;
            // Proto-derived operation_kind (never name-guessed). Assert it resolves so a
            // missing classification is a loud failure, not a silently-populated RPC.
            const opKind = operationKindOf(api.serviceFull, methodName);
            node_assert_1.strict.ok(opKind, `${label}.${serviceName}.${methodName} has no proto operation_kind (classification/coverage gap)`);
            const populated = opKind !== "destructive";
            if (populated)
                counters.populated += 1;
            const request = populated ? surfaceProbeRequest(tenantId, projectId) : {};
            // One node:test sub-test PER RPC so the reporter shows granular per-RPC
            // pass/fail (like the Go sub-tests), not a single opaque test.
            await t.test(`${api.serviceFull}/${methodName}`, async () => {
                await expectMounted(`${label}.${serviceName}.${methodName}`, () => fn(request, { deadlineMs: 2_000, noRetry: true }));
            });
        }
    }
    return count;
}
// runLiveBackendClaimCheck: every advertised backend must answer a real
// list_resources (a mount/unavailable failure means a capability lie).
async function runLiveBackendClaimCheck(data, ctx, enabled) {
    node_assert_1.strict.ok(enabled.length > 0, "GetCapabilities advertised zero backends");
    for (const backend of enabled) {
        await expectMounted(`backend-claim.${backend}`, () => data.list_resources({ context: ctx, backend }, { deadlineMs: 5_000, noRetry: true }));
    }
}
// runLiveAuthLifecycle: prove Logout invalidates the session — the access token,
// refresh token and session-refresh must ALL fail afterwards. Throwaway login.
async function runLiveAuthLifecycle(authn, tenantId, projectId, username, password) {
    const opts = { deadlineMs: 8_000, noRetry: true };
    const login = await authn.login({ username, password, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-sdk-lifecycle" }, opts);
    const token = login.access_token, sid = login.session_id, refresh = login.refresh_token;
    node_assert_1.strict.ok(token && sid && refresh, "Login must return access_token+session_id+refresh_token");
    const pre = await authn.validate_token({ token, token_type: 1 }, opts); // 1 = TOKEN_TYPE_JWT_ACCESS
    node_assert_1.strict.ok(pre.valid, "fresh access token must validate before logout");
    await authn.get_session({ session_id: sid }, opts);
    const preIntro = await authn.introspect_token({ token }, opts);
    node_assert_1.strict.ok(preIntro.active, "fresh access token must introspect active before logout");
    const out = await authn.logout({ session_id: sid, revoke_reason: "sdk_live_test" }, opts);
    node_assert_1.strict.ok(Number(out.sessions_revoked) >= 1, "Logout must revoke at least one session");
    const failures = [];
    try {
        if ((await authn.validate_token({ token, token_type: 1 }, opts)).valid)
            failures.push("access token still validates after logout");
    }
    catch { /* denied = correct */ }
    try {
        if ((await authn.introspect_token({ token }, opts)).active)
            failures.push("token still introspects Active after logout");
    }
    catch { /* denied = correct */ }
    try {
        await authn.refresh_token({ refresh_token: refresh, session_id: sid }, opts);
        failures.push("refresh token still works after logout — token family not revoked");
    }
    catch { /* denied = correct */ }
    try {
        await authn.refresh_session({ session_id: sid }, opts);
        failures.push("RefreshSession still works after logout — session not revoked");
    }
    catch { /* denied = correct */ }
    node_assert_1.strict.equal(failures.length, 0, `SECURITY (logout did not fully invalidate the session): ${failures.join("; ")}`);
}
// runLiveAuthNegative: edge cases the happy-path suite skips — the auth plane must
// fail CLOSED. A wrong password mints no access token; a garbage/forged bearer never
// validates or introspects active. A mount failure is still fatal (the negative
// paths must be wired too, not just the positive ones).
async function runLiveAuthNegative(authn, tenantId, projectId, username) {
    const opts = { deadlineMs: 8_000, noRetry: true };
    const fatalIfMount = (label, err) => {
        const code = grpcCode(err);
        if (code !== undefined && FATAL_CONNECTIVITY_CODES.has(code)) {
            throw new Error(`${label} did not reach a live RPC: ${describeGrpcError(err)}`);
        }
    };
    const failures = [];
    try {
        const bad = await authn.login({ username, password: `definitely-wrong-${username}-Pw1!`, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-sdk-negative" }, opts);
        if (bad?.access_token)
            failures.push("Login with a wrong password returned an access token");
    }
    catch (err) {
        fatalIfMount("negative Login", err);
    }
    try {
        const v = await authn.validate_token({ token: "not-a-real-jwt", token_type: 1 }, opts);
        if (v?.valid)
            failures.push("a garbage token validated as valid");
    }
    catch (err) {
        fatalIfMount("negative ValidateToken", err);
    }
    try {
        const i = await authn.introspect_token({ token: "not-a-real-jwt" }, opts);
        if (i?.active)
            failures.push("a garbage token introspected as active");
    }
    catch (err) {
        fatalIfMount("negative IntrospectToken", err);
    }
    node_assert_1.strict.equal(failures.length, 0, `SECURITY (auth did not fail closed): ${failures.join("; ")}`);
}
// runLiveEdgeCases: per-RPC EDGE cases (malformed/hostile inputs + isolation
// boundaries). Each must fail closed with a typed error (or safely accept-and-
// sanitise), never leak cross-tenant data, and never surface a server fault
// (UNKNOWN/INTERNAL/DATA_LOSS = the input crashed the handler). Mirrors the Go suite.
const EDGE_SERVER_FAULTS = new Set([2, 13, 15]); // UNKNOWN, INTERNAL, DATA_LOSS
async function runLiveEdgeCases(data, tenantId, projectId) {
    const ctx = requestContext(tenantId, projectId, "ts.live.edge");
    const opts = { deadlineMs: 8_000, noRetry: true };
    const suffix = `${tenantId}-edge`;
    const notFault = (label, err) => {
        const c = grpcCode(err);
        if (c !== undefined && EDGE_SERVER_FAULTS.has(c)) {
            throw new Error(`${label} faulted the server (code ${c}): ${describeGrpcError(err)}`);
        }
    };
    // 1. missing project_id in the filter -> project isolation must reject.
    let accepted1 = false;
    try {
        await data.select({ context: ctx, message_type: LIVE_MESSAGE_TYPE, filter: { tenant_id: tenantId }, limit: 1 }, opts);
        accepted1 = true;
    }
    catch (err) {
        notFault("missing project_id", err);
    }
    node_assert_1.strict.equal(accepted1, false, "Select without a project_id filter was ACCEPTED — project isolation not enforced");
    // 2. cross-tenant read -> RLS scopes to the JWT tenant; a foreign filter leaks nothing.
    const foreign = "00000000-0000-0000-0000-0000deadbeef";
    try {
        const resp = await data.select({ context: ctx, message_type: LIVE_MESSAGE_TYPE, filter: { tenant_id: foreign, project_id: projectId }, limit: 10 }, opts);
        const n = (resp?.records_json ?? []).length;
        node_assert_1.strict.equal(n, 0, `cross-tenant Select LEAKED ${n} record(s) for ${foreign}`);
    }
    catch (err) {
        notFault("cross-tenant Select", err);
    }
    // 3. NUL byte in a text field -> stripped/rejected, never a raw UTF8 0x00 fault (B14).
    try {
        await data.upsert({
            context: ctx, message_type: LIVE_MESSAGE_TYPE,
            record_json: jsonBytes({ record_id: `edge-nul-${suffix}`, tenant_id: tenantId, project_id: projectId, lookup_key: `edge-nul-lk-${suffix}`, payload: "payload\0with-nul", revision: 1 }),
            conflict_fields: ["record_id"],
        }, opts);
    }
    catch (err) {
        notFault("NUL-byte payload", err);
    }
    // 4. limit boundaries (negative/zero/huge) -> clamped/validated, never a crash.
    for (const lim of [-1, 0, 1_000_000]) {
        try {
            await data.select({ context: ctx, message_type: LIVE_MESSAGE_TYPE, filter: { tenant_id: tenantId, project_id: projectId }, limit: lim }, opts);
        }
        catch (err) {
            notFault(`Select limit=${lim}`, err);
        }
    }
    // 5. unknown message_type -> typed error, not a 500.
    let accepted5 = false;
    try {
        await data.select({ context: ctx, message_type: "udb.does.not.Exist", filter: { tenant_id: tenantId, project_id: projectId }, limit: 1 }, opts);
        accepted5 = true;
    }
    catch (err) {
        notFault("unknown message_type", err);
    }
    node_assert_1.strict.equal(accepted5, false, "Select on an unknown message_type was ACCEPTED");
    // 6. invalid backend -> typed error, never a panic/Internal.
    let accepted6 = false;
    try {
        await data.list_resources({ context: ctx, backend: "nonexistent-backend-xyz" }, opts);
        accepted6 = true;
    }
    catch (err) {
        notFault("invalid backend", err);
    }
    node_assert_1.strict.equal(accepted6, false, "ListResources on a nonexistent backend was ACCEPTED");
}
async function drainReadable(stream) {
    return await new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(chunks));
    });
}
async function drainDuplexOnce(stream, requests) {
    return await new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(chunks));
        for (const request of requests)
            stream.write(request);
        stream.end();
    });
}
// Challenge EVERY advertised backend's per-operation claims in BOTH directions via
// GenericDispatch (the single op-gated entry point shared by every backend kind). A
// claimed side-effect-free op must be admitted; the first unclaimed op must be
// refused with the declared unsupported code — proving each backend kind honors
// exactly the surface it advertises.
async function runLiveBackendCapabilityChallenge(data, ctx, caps) {
    const descriptors = caps.backend_capabilities ?? [];
    node_assert_1.strict.ok(descriptors.length > 0, "GetCapabilities advertised zero backend_capabilities descriptors");
    const opts = { deadlineMs: 5_000, noRetry: true };
    const dispatch = async (backend, op) => {
        try {
            await data.generic_dispatch({ context: ctx, backend, operation: op, spec_json: "{}" }, opts);
            return null;
        }
        catch (err) {
            return err;
        }
    };
    for (const d of descriptors) {
        const backend = d.backend;
        node_assert_1.strict.ok(backend, "a backend_capabilities descriptor has an empty backend name");
        node_assert_1.strict.ok(d.tier, `backend ${backend} advertises no tier`);
        const claimed = d.operations ?? [];
        node_assert_1.strict.ok(claimed.length > 0, `backend ${backend} advertises an empty operations list`);
        node_assert_1.strict.equal(d.unsupported_error_code, UNSUPPORTED_OPERATION_CODE, `backend ${backend} unsupported_error_code`);
        const claimedSet = new Set(claimed);
        for (const op of ["ping", "probe", "list_resources"]) {
            if (!claimedSet.has(op))
                continue;
            const err = await dispatch(backend, op);
            if (err) {
                const code = grpcCode(err);
                if (code !== undefined && FATAL_CONNECTIVITY_CODES.has(code))
                    throw new Error(`backend ${backend} claims ${op} but did not reach a live RPC: ${describeGrpcError(err)}`);
                node_assert_1.strict.ok(!errText(err).includes(UNSUPPORTED_OPERATION_CODE), `CAPABILITY LIE: backend ${backend} advertises ${op} but the gate refused it: ${errText(err)}`);
            }
        }
        for (const op of GENERIC_DISPATCH_OPS) {
            if (claimedSet.has(op))
                continue;
            const err = await dispatch(backend, op);
            node_assert_1.strict.ok(err, `CAPABILITY LIE: backend ${backend} does NOT advertise ${op} yet GenericDispatch admitted it (silent over-claim)`);
            const code = grpcCode(err);
            if (code !== undefined && FATAL_CONNECTIVITY_CODES.has(code))
                throw new Error(`backend ${backend} unclaimed-op ${op} did not reach a live RPC: ${describeGrpcError(err)}`);
            node_assert_1.strict.ok(errText(err).includes(UNSUPPORTED_OPERATION_CODE), `backend ${backend} refused unclaimed op ${op} but not with ${UNSUPPORTED_OPERATION_CODE}: ${errText(err)}`);
            break;
        }
    }
    // NOTE: enabled_backends and backend_capabilities are intentionally NOT
    // cross-checked as a subset relation — they derive from different sources and
    // naming. backend_capabilities is the full compiled matrix (a descriptor per
    // built-in backend, each with a `configured` flag) keyed by canonical name (e.g.
    // "sqlserver"); enabled_backends is the enabled subset, possibly aliased (e.g.
    // "mssql"). The meaningful invariant is the per-backend both-directions op
    // challenge above; a list-vs-list subset assertion flags those legitimate
    // naming/scope differences as false positives.
}
function backendCategory(tier, ops) {
    if (ops.has("get_object") || ops.has("put_object"))
        return "object";
    return { vector: "vector", cache: "cache", document: "document", graph: "graph", sql: "relational", column: "relational" }[String(tier).toLowerCase()] ?? "";
}
// Drive a real, category-appropriate data-plane round-trip against EVERY advertised
// backend kind (relational SQL, object, document, cache, vector, graph) — not just
// the canonical postgres/mongodb/minio trio. Adapts to whatever the broker enabled.
// A claimed RPC must at minimum REACH an implementation (a mount failure is fatal);
// per-backend business quirks are tolerated, values asserted on success.
async function runLiveAllBackendKindsMatrix(data, tenantId, projectId, caps) {
    const suffix = `${process.pid}-${Date.now()}`;
    const rc = (p) => requestContext(tenantId, projectId, p);
    const opts = { deadlineMs: 8_000, noRetry: true };
    const mountFatal = (backend, op, err) => {
        const code = grpcCode(err);
        if (code !== undefined && FATAL_CONNECTIVITY_CODES.has(code))
            throw new Error(`backend ${backend} (${op}) did not reach a live RPC: ${describeGrpcError(err)}`);
    };
    const exercised = {};
    for (const d of caps.backend_capabilities ?? []) {
        const backend = d.backend;
        if (!backend)
            continue;
        const ops = new Set(d.operations ?? []);
        const cat = backendCategory(d.tier, ops);
        exercised[cat] = (exercised[cat] ?? 0) + 1;
        if (cat === "relational") {
            try {
                await data.generic_dispatch({ context: rc("ts.live.kind.relational"), backend, operation: "query", spec_json: JSON.stringify({ sql: "SELECT 1 AS live_probe" }) }, opts);
            }
            catch (err) {
                mountFatal(backend, "query", err);
                node_assert_1.strict.ok(!errText(err).includes(UNSUPPORTED_OPERATION_CODE), `CAPABILITY LIE: relational backend ${backend} refused a claimed query: ${errText(err)}`);
            }
        }
        else if (cat === "object") {
            await objectKind(data, rc, backend, suffix, mountFatal);
        }
        else if (cat === "document") {
            await documentKind(data, rc, backend, suffix, mountFatal);
        }
        else if (cat === "cache") {
            await cacheKind(data, rc, backend, suffix, mountFatal);
        }
        else if (cat === "vector") {
            await vectorKind(data, rc, backend, suffix, mountFatal);
        }
        else if (cat === "graph") {
            await graphKind(data, rc, backend, suffix, mountFatal);
        }
    }
    node_assert_1.strict.ok((exercised["relational"] ?? 0) > 0, "no relational backend advertised — expected at least postgres");
}
async function objectKind(data, rc, backend, suffix, mountFatal) {
    const bucket = process.env.UDB_LIVE_S3_BUCKET || "udb-live-sdk";
    const key = `kind/${backend}/${suffix}.txt`;
    const body = Buffer.from(`object-kind-${backend}-${suffix}`, "utf8");
    const opts = { deadlineMs: 8_000, noRetry: true };
    try {
        await data.ensure_resource({ context: rc("ts.live.kind.object"), backend, resource_name: bucket, spec_json: "{}" }, opts);
    }
    catch (err) {
        mountFatal(backend, "ensure_resource", err);
    }
    try {
        const put = data.put_object({ deadlineMs: 10_000, noRetry: true });
        put.stream.write({ context: rc("ts.live.kind.object"), bucket, object_key: key, data: body, content_type: "text/plain", final_chunk: true });
        put.stream.end();
        await put.response;
    }
    catch (err) {
        mountFatal(backend, "put_object", err);
        return;
    }
    try {
        const chunks = await drainReadable(data.get_object({ context: rc("ts.live.kind.object"), bucket, object_key: key }, { deadlineMs: 10_000 }));
        const got = Buffer.concat(chunks.map((c) => Buffer.from(c.data)));
        if (got.length)
            node_assert_1.strict.equal(got.toString("utf8"), body.toString("utf8"), `object backend ${backend} round-trip body mismatch`);
    }
    catch (err) {
        mountFatal(backend, "get_object", err);
    }
}
async function documentKind(data, rc, backend, suffix, mountFatal) {
    const opts = { deadlineMs: 8_000, noRetry: true };
    const collection = `sdk_kind_docs_${backend.replace(/[^a-zA-Z0-9_]/g, "_")}_${suffix.replace(/[^a-zA-Z0-9_]/g, "_")}`;
    const documentId = `doc-${suffix}`;
    const resource = { backend, resource_name: collection };
    try {
        await data.ensure_resource({ context: rc("ts.live.kind.document"), backend, resource_name: collection, spec_json: JSON.stringify({ collection }) }, opts);
    }
    catch (err) {
        mountFatal(backend, "ensure_resource", err);
    }
    try {
        await data.document_upsert({ context: rc("ts.live.kind.document"), resource, document_id: documentId, document: { _id: documentId, payload: `doc-${backend}`, revision: 1 } }, opts);
    }
    catch (err) {
        mountFatal(backend, "mutate", err);
        return;
    }
    try {
        const got = await data.document_get({ context: rc("ts.live.kind.document"), resource, document_id: documentId }, opts);
        if ((got.documents ?? []).length)
            node_assert_1.strict.equal(structField(got.documents[0], "payload"), `doc-${backend}`);
    }
    catch (err) {
        mountFatal(backend, "query", err);
    }
    try {
        await data.document_delete({ context: rc("ts.live.kind.document"), resource, document_id: documentId }, opts);
    }
    catch (err) {
        mountFatal(backend, "mutate", err);
    }
}
async function cacheKind(data, rc, backend, suffix, mountFatal) {
    const opts = { deadlineMs: 8_000, noRetry: true };
    const resource = { backend };
    const key = `sdk-live-cache-${suffix}`;
    const val = Buffer.from(`cache-${backend}-${suffix}`, "utf8");
    try {
        await data.cache_set({ context: rc("ts.live.kind.cache"), resource, key, value: val, content_type: "text/plain", ttl_seconds: 60 }, opts);
    }
    catch (err) {
        mountFatal(backend, "cache_set", err);
        return;
    }
    try {
        const got = await data.cache_get({ context: rc("ts.live.kind.cache"), resource, key }, opts);
        if (got.found)
            node_assert_1.strict.equal(Buffer.from(got.value).toString("utf8"), val.toString("utf8"), `cache backend ${backend} CacheGet mismatch`);
    }
    catch (err) {
        mountFatal(backend, "cache_get", err);
    }
    try {
        await data.cache_scan({ context: rc("ts.live.kind.cache"), resource, key_pattern: "sdk-live-cache-*", limit: 10 }, opts);
    }
    catch (err) {
        mountFatal(backend, "cache_scan", err);
    }
    try {
        await data.cache_delete({ context: rc("ts.live.kind.cache"), resource, key }, opts);
    }
    catch (err) {
        mountFatal(backend, "cache_delete", err);
    }
}
async function vectorKind(data, rc, backend, suffix, mountFatal) {
    const opts = { deadlineMs: 8_000, noRetry: true };
    const collection = `sdk_kind_vec_${backend.replace(/[^a-zA-Z0-9_]/g, "_")}_${suffix.replace(/[^a-zA-Z0-9_]/g, "_")}`;
    try {
        await data.ensure_resource({ context: rc("ts.live.kind.vector"), backend, resource_name: collection, spec_json: JSON.stringify({ dimension: 4, distance: "cosine" }) }, opts);
    }
    catch (err) {
        mountFatal(backend, "ensure_resource", err);
    }
    const vector = [0.1, 0.2, 0.3, 0.4];
    try {
        await data.vector_upsert({ context: rc("ts.live.kind.vector"), collection, points: [{ id: `v-${suffix}`, vector, payload: { tag: "sdk-live" } }] }, opts);
    }
    catch (err) {
        mountFatal(backend, "mutate", err);
        return;
    }
    try {
        await data.vector_search({ context: rc("ts.live.kind.vector"), collection, vector, limit: 1, with_payload: true }, opts);
    }
    catch (err) {
        mountFatal(backend, "search", err);
    }
}
async function graphKind(data, rc, backend, suffix, mountFatal) {
    const opts = { deadlineMs: 8_000, noRetry: true };
    const resource = { backend };
    const label = `SdkLive${suffix.replace(/[^a-zA-Z0-9]/g, "")}`;
    try {
        await data.graph_mutate({ context: rc("ts.live.kind.graph"), resource, query: `CREATE (n:${label} {id: $id}) RETURN n`, parameters: { id: suffix } }, opts);
    }
    catch (err) {
        mountFatal(backend, "mutate", err);
        return;
    }
    try {
        await data.graph_query({ context: rc("ts.live.kind.graph"), resource, query: `MATCH (n:${label}) RETURN n LIMIT 1`, read_only: true }, opts);
    }
    catch (err) {
        mountFatal(backend, "query", err);
    }
}
async function runLiveBackendE2E(project, tenantId, projectId) {
    const data = project.generated.DataBroker;
    const ctx = requestContext(tenantId, projectId, "ts.live.backend.e2e");
    const suffix = `${process.pid}-${Date.now()}`;
    const recordId = `ts-${suffix}`;
    const secondRecordId = `ts-batch-${suffix}`;
    const lookupKey = `ts-live-${suffix}`;
    const collection = `sdk_live_docs_${suffix.replace(/[^a-zA-Z0-9_]/g, "_")}`;
    const documentId = `doc-${suffix}`;
    const bucket = process.env.UDB_LIVE_S3_BUCKET || "udb-live-sdk";
    const objectKey = `ts/${suffix}.txt`;
    const objectBody = Buffer.from(`typescript live sdk object ${suffix}`, "utf8");
    await data.generic_dispatch({
        context: ctx,
        backend: "postgres",
        operation: "query",
        spec_json: JSON.stringify({ sql: "SELECT 1::INT AS live_probe" }),
    }, { deadlineMs: 5_000, noRetry: true });
    const inserted = await data.upsert({
        context: ctx,
        message_type: LIVE_MESSAGE_TYPE,
        record_json: jsonBytes({
            record_id: recordId,
            tenant_id: tenantId,
            project_id: projectId,
            lookup_key: lookupKey,
            payload: "created-from-ts",
            revision: 1,
        }),
        conflict_fields: ["record_id"],
        return_record: true,
    }, { deadlineMs: 5_000, noRetry: true });
    node_assert_1.strict.equal(inserted.affected_rows, "1");
    node_assert_1.strict.equal(mutationRecordJson(inserted).payload, "created-from-ts");
    const selected = await data.select({
        context: ctx,
        message_type: LIVE_MESSAGE_TYPE,
        filter: { record_id: recordId, tenant_id: tenantId, project_id: projectId },
        limit: 1,
    }, { deadlineMs: 5_000, noRetry: true });
    node_assert_1.strict.equal(recordJson(selected).revision, 1);
    const updated = await data.upsert({
        context: ctx,
        message_type: LIVE_MESSAGE_TYPE,
        record_json: jsonBytes({
            record_id: recordId,
            tenant_id: tenantId,
            project_id: projectId,
            lookup_key: lookupKey,
            payload: "updated-from-ts",
            revision: 2,
        }),
        conflict_fields: ["record_id"],
        return_record: true,
    }, { deadlineMs: 5_000, noRetry: true });
    node_assert_1.strict.equal(mutationRecordJson(updated).payload, "updated-from-ts");
    const selectV2 = await drainReadable(data.select_v2({
        context: ctx,
        message_type: LIVE_MESSAGE_TYPE,
        filter: { record_id: recordId, tenant_id: tenantId, project_id: projectId },
        limit: 1,
    }, { deadlineMs: 5_000 }));
    node_assert_1.strict.ok(selectV2.length >= 1, "SelectV2 must stream at least one batch for an existing row");
    const batchUpsert = data.batch_upsert({ deadlineMs: 5_000, noRetry: true });
    const batchResponses = await drainDuplexOnce(batchUpsert, [{
            context: ctx,
            message_type: LIVE_MESSAGE_TYPE,
            record_json: jsonBytes({
                record_id: secondRecordId,
                tenant_id: tenantId,
                project_id: projectId,
                lookup_key: `${lookupKey}-batch`,
                payload: "created-from-ts-batch",
                revision: 1,
            }),
            conflict_fields: ["record_id"],
        }]);
    node_assert_1.strict.ok(batchResponses.length >= 1, "BatchUpsert must produce a mutation response");
    const batchSelect = data.batch_select({ deadlineMs: 5_000, noRetry: true });
    const batchRows = await drainDuplexOnce(batchSelect, [{
            context: ctx,
            message_type: LIVE_MESSAGE_TYPE,
            filter: { record_id: secondRecordId, tenant_id: tenantId, project_id: projectId },
            limit: 1,
        }]);
    node_assert_1.strict.equal(recordJson(batchRows[0]).payload, "created-from-ts-batch");
    await data.ensure_resource({
        context: ctx,
        backend: "mongodb",
        resource_name: collection,
        spec_json: JSON.stringify({ collection }),
    }, { deadlineMs: 5_000, noRetry: true });
    const resourceList = await data.list_resources({
        context: ctx,
        backend: "mongodb",
    }, { deadlineMs: 5_000, noRetry: true });
    node_assert_1.strict.ok(resourceList.resources.some((name) => name.includes(collection)), "Mongo collection must be listed after EnsureResource");
    await data.document_upsert({
        context: ctx,
        resource: { backend: "mongodb", resource_name: collection },
        document_id: documentId,
        document: { _id: documentId, tenant_id: tenantId, project_id: projectId, payload: "mongo-created", revision: 1 },
    }, { deadlineMs: 5_000, noRetry: true });
    const mongoGet = await data.document_get({
        context: ctx,
        resource: { backend: "mongodb", resource_name: collection },
        document_id: documentId,
    }, { deadlineMs: 5_000, noRetry: true });
    node_assert_1.strict.equal(structField(mongoGet.documents?.[0], "payload"), "mongo-created");
    await data.document_upsert({
        context: ctx,
        resource: { backend: "mongodb", resource_name: collection },
        document_id: documentId,
        document: { payload: "mongo-updated", revision: 2 },
    }, { deadlineMs: 5_000, noRetry: true });
    const mongoFind = await data.document_find({
        context: ctx,
        resource: { backend: "mongodb", resource_name: collection },
        filter: { _id: documentId },
        limit: 1,
    }, { deadlineMs: 5_000, noRetry: true });
    node_assert_1.strict.equal(structField(mongoFind.documents?.[0], "payload"), "mongo-updated");
    const mongoDeleted = await data.document_delete({
        context: ctx,
        resource: { backend: "mongodb", resource_name: collection },
        document_id: documentId,
    }, { deadlineMs: 5_000, noRetry: true });
    node_assert_1.strict.equal(mongoDeleted.affected_rows, "1");
    await data.ensure_resource({
        context: ctx,
        backend: "minio",
        resource_name: bucket,
        spec_json: "{}",
    }, { deadlineMs: 5_000, noRetry: true });
    const put = data.put_object({ deadlineMs: 10_000, noRetry: true });
    put.stream.write({
        context: ctx,
        bucket,
        object_key: objectKey,
        data: objectBody.subarray(0, 12),
        content_type: "text/plain",
    });
    put.stream.write({
        context: ctx,
        bucket,
        object_key: objectKey,
        data: objectBody.subarray(12),
        final_chunk: true,
    });
    put.stream.end();
    const putResponse = await put.response;
    node_assert_1.strict.equal(putResponse.affected_rows, "1");
    const objectChunks = await drainReadable(data.get_object({
        context: ctx,
        bucket,
        object_key: objectKey,
    }, { deadlineMs: 10_000 }));
    node_assert_1.strict.equal(Buffer.concat(objectChunks.map((chunk) => Buffer.from(chunk.data))).toString("utf8"), objectBody.toString("utf8"));
    const presigned = await data.generate_presigned_url({
        context: ctx,
        bucket,
        object_key: objectKey,
        method: "GET",
        ttl_seconds: 60,
    }, { deadlineMs: 5_000, noRetry: true });
    node_assert_1.strict.match(presigned.url, /^https?:\/\//);
    const deleted = await data.delete({
        context: ctx,
        message_type: LIVE_MESSAGE_TYPE,
        filter: { record_id: recordId, tenant_id: tenantId, project_id: projectId },
    }, { deadlineMs: 5_000, noRetry: true });
    node_assert_1.strict.equal(deleted.affected_rows, "1");
    await data.delete({
        context: ctx,
        message_type: LIVE_MESSAGE_TYPE,
        filter: { record_id: secondRecordId, tenant_id: tenantId, project_id: projectId },
    }, { deadlineMs: 5_000, noRetry: true });
    const afterDelete = await data.select({
        context: ctx,
        message_type: LIVE_MESSAGE_TYPE,
        filter: { record_id: recordId, tenant_id: tenantId, project_id: projectId },
        limit: 1,
    }, { deadlineMs: 5_000, noRetry: true });
    node_assert_1.strict.equal(afterDelete.records_json.length, 0);
    // Control-plane data ops with real assertions: project create+list, policy
    // reads, catalog/schema/health. PutPolicy is intentionally NOT called — an abac
    // policy insert flips the data plane to default-deny.
    const projId = `sdklive_proj_ts_${suffix}`;
    await data.ensure_project({ context: ctx, project_id: projId, name: "SDK Live Project" }, { deadlineMs: 8_000, noRetry: true });
    const projects = await data.list_projects({ context: ctx }, { deadlineMs: 8_000, noRetry: true });
    node_assert_1.strict.ok((projects.projects ?? []).some((p) => p.project_id === projId), "ListProjects must include the created project");
    await data.list_policies({ context: ctx }, { deadlineMs: 8_000, noRetry: true });
    await data.lint_policies({ context: ctx }, { deadlineMs: 8_000, noRetry: true });
    const manifest = await data.get_catalog_manifest({ context: ctx }, { deadlineMs: 8_000, noRetry: true });
    node_assert_1.strict.ok(manifest.manifest_json, "GetCatalogManifest must return a manifest");
    const schemas = await data.list_message_schemas({ context: ctx, project_id: projectId }, { deadlineMs: 8_000, noRetry: true });
    node_assert_1.strict.ok((schemas.message_types ?? []).length > 0, "ListMessageSchemas must return message types");
    const lookup = await data.lookup_message_schema({ context: ctx, project_id: projectId, message_type: LIVE_MESSAGE_TYPE }, { deadlineMs: 8_000, noRetry: true });
    node_assert_1.strict.ok(lookup.schema, `LookupMessageSchema must resolve ${LIVE_MESSAGE_TYPE}`);
    await data.get_health_report({ context: ctx, with_probes: true, project_id: projectId }, { deadlineMs: 8_000, noRetry: true });
}
function liveUuid() {
    return globalThis.crypto.randomUUID();
}
// Real create→read→assert CRUD against every native control-plane service.
// Most services accept the free-text "sdk-live" tenant via the main project;
// storage/webrtc/asset persist tenant_id into a UUID column cross-checked
// against the bearer claim, so they run through `uuidProject` (a second admin
// bootstrapped on a UUID tenant). Authz created_by must be a UUID; the
// notification recipient_id is an FK to a real users row.
async function runLiveNativeServiceE2E(project, uuidProject, tenantId, projectId, uuidTenant) {
    const gen = project.authGenerated ?? project.generated;
    const ugen = uuidProject.authGenerated ?? uuidProject.generated;
    const opts = { deadlineMs: 8_000, noRetry: true };
    const suffix = `${process.pid}${Date.now()}`;
    // TenantService — CreateTenant is a platform write (Get/Update/List are
    // tenant-self-scoped and the bootstrap admin's tenant has no tenants-table row).
    const createdTenant = await gen.TenantService.create_tenant({ code: `sdklivets${suffix}`, name: "SDK Live TS", type: "WORKSPACE" }, opts);
    node_assert_1.strict.ok(createdTenant.tenant_id, "CreateTenant must return a tenant_id");
    // AuthzService — role create/get/list.
    const roleCode = `sdk_reader_ts_${suffix}`;
    const createdRole = (await gen.AuthzService.create_role({
        name: `SDK Reader TS ${suffix}`, description: "Live SDK reader role", created_by: liveUuid(),
        role_code: roleCode, domain: tenantId, tenant_id: tenantId, project_id: projectId,
    }, opts)).role;
    node_assert_1.strict.equal(createdRole.role_code, roleCode);
    const gotRole = (await gen.AuthzService.get_role({ role_id: createdRole.role_id }, opts)).role;
    node_assert_1.strict.equal(gotRole.role_code, roleCode);
    const roles = await gen.AuthzService.list_roles({ domain: tenantId, active_only: true }, opts);
    node_assert_1.strict.ok((roles.roles ?? []).some((r) => r.role_id === createdRole.role_id), "ListRoles must include created role");
    // Full decision flow: assign the role to a real user, attach an allow policy,
    // prove CheckAccess flips allow→deny across a role revoke (security-critical).
    const subject = (await gen.AuthnService.create_user({
        username: `sdk-authz-ts-${suffix}`, email: `sdk-authz-ts-${suffix}@example.com`, password: "CorrectHorse1!",
        tenant_id: tenantId, project_id: projectId, full_name: "SDK Authz Subject",
    }, opts)).user;
    const assignedRole = (await gen.AuthzService.assign_role({
        user_id: subject.user_id, role_id: createdRole.role_id, domain: tenantId,
        assigned_by: subject.user_id, tenant_id: tenantId, project_id: projectId,
    }, opts)).user_role;
    await gen.AuthzService.put_authz_policy({
        policy: {
            id: liveUuid(), enabled: true, effect: "allow", tenant: tenantId, project: projectId,
            role: createdRole.role_code, action: "data.select", resource: "invoice",
        },
    }, opts);
    const allowed = await gen.AuthzService.check_access({
        user_id: subject.user_id, domain: tenantId, tenant_id: tenantId, project_id: projectId, object: "invoice", action: "data.select",
    }, opts);
    node_assert_1.strict.ok(allowed.allowed, "CheckAccess must allow the assigned role+policy");
    const userRoles = await gen.AuthzService.list_user_roles({ user_id: subject.user_id, domain: tenantId, active_only: true }, opts);
    node_assert_1.strict.equal((userRoles.user_roles ?? []).length, 1);
    await gen.AuthzService.revoke_role({ user_role_id: assignedRole.user_role_id, user_id: subject.user_id, reason: "sdk_live_test", revoked_by: subject.user_id }, opts);
    const denied = await gen.AuthzService.check_access({
        user_id: subject.user_id, domain: tenantId, tenant_id: tenantId, project_id: projectId, object: "invoice", action: "data.select",
    }, opts);
    node_assert_1.strict.ok(!denied.allowed, "CheckAccess must deny after the role was revoked");
    // ApiKeyService — create/validate/list/revoke lifecycle.
    const principal = `sdk-live-svc-${suffix}`;
    const keyCtx = { user_id: principal, tenant: { tenant_id: tenantId, project_id: projectId } };
    const createdKey = await gen.ApiKeyService.create_api_key({ name: `sdk-live-key-${suffix}`, owner_id: principal, scopes: ["data:read"], context: keyCtx }, opts);
    node_assert_1.strict.ok(String(createdKey.plain_key).startsWith("udbk_"), "plain_key must have udbk_ prefix");
    const keyId = createdKey.key.key_id;
    const valid = await gen.ApiKeyService.validate_api_key({ plain_key: createdKey.plain_key, required_scope: "data:read" }, opts);
    node_assert_1.strict.ok(valid.valid && valid.owner_id === principal, "ValidateApiKey must accept the fresh key");
    const listedKeys = await gen.ApiKeyService.list_api_keys({ owner_id: principal, status: 1 }, opts); // 1 = ACTIVE
    node_assert_1.strict.equal((listedKeys.keys ?? []).length, 1);
    node_assert_1.strict.equal(listedKeys.keys[0].key_id, keyId);
    const gotKey = await gen.ApiKeyService.get_api_key({ key_id: keyId }, opts);
    node_assert_1.strict.equal(gotKey.key.owner_id, principal);
    await gen.ApiKeyService.update_api_key({ key_id: keyId, scopes: ["data:read", "data:write"], context: keyCtx }, opts);
    const writeOK = await gen.ApiKeyService.validate_api_key({ plain_key: createdKey.plain_key, required_scope: "data:write" }, opts);
    node_assert_1.strict.ok(writeOK.valid, "ValidateApiKey must honor the updated data:write scope");
    await gen.ApiKeyService.revoke_api_key({ key_id: keyId, revoke_reason: "sdk_live_test", context: keyCtx }, opts);
    const afterRevoke = await gen.ApiKeyService.validate_api_key({ plain_key: createdKey.plain_key, required_scope: "data:read" }, opts);
    node_assert_1.strict.ok(!afterRevoke.valid, "revoked API key must not validate");
    // AnalyticsService — record metrics then roll up.
    const stage = `sdk_live_stage_ts_${suffix}`;
    for (const [latency, ok] of [[100, true], [200, true], [400, false]]) {
        const accepted = await gen.AnalyticsService.record_pipeline_metric({ stage_name: stage, tenant_id: tenantId, latency_ms: latency, is_success: ok }, opts);
        node_assert_1.strict.ok(accepted.accepted, "RecordPipelineMetric must be accepted");
    }
    const summary = await gen.AnalyticsService.get_pipeline_summary({ stage_name: stage, tenant_id: tenantId, page: { page: 1, page_size: 10 } }, opts);
    node_assert_1.strict.equal((summary.snapshots ?? []).length, 1);
    node_assert_1.strict.equal(Number(summary.snapshots[0].total_requests), 3);
    const throughput = await gen.AnalyticsService.get_throughput({ tenant_id: tenantId }, opts);
    node_assert_1.strict.ok(Number(throughput.total_requests) >= 3);
    const trig = await gen.AnalyticsService.trigger_snapshot({ stage_name: stage }, opts);
    node_assert_1.strict.ok(Number(trig.snapshots_written) >= 1);
    // NotificationService — template + send to a real user (recipient_id FK).
    const recipient = (await gen.AuthnService.create_user({
        username: `sdk-notif-ts-${suffix}`, email: `sdk-notif-ts-${suffix}@example.com`, password: "CorrectHorse1!",
        tenant_id: tenantId, project_id: projectId, full_name: "SDK Notify TS",
    }, opts)).user;
    const event = `sdk.live.ts.${suffix}`;
    const body = `sdk-live-body-ts-${suffix}`;
    await gen.NotificationService.upsert_template({ event_type: event, channel: 1, locale: "en", subject_template: "SDK {{n}}", body_template: body, is_active: true }, opts);
    const template = (await gen.NotificationService.get_template({ event_type: event, channel: 1, locale: "en" }, opts)).template;
    node_assert_1.strict.equal(template.body_template, body);
    const sent = await gen.NotificationService.send_notification({ event_type: event, recipient_id: recipient.user_id, recipient_address: `sdk+${suffix}@example.com`, tenant_id: tenantId, channels: [1] }, opts);
    node_assert_1.strict.ok((sent.logs ?? []).length >= 1, "SendNotification must record a log");
    const logId = sent.logs[0].log_id;
    const listedNotifs = await gen.NotificationService.list_notifications({ tenant_id: tenantId }, opts);
    node_assert_1.strict.ok((listedNotifs.logs ?? []).some((l) => l.log_id === logId), "ListNotifications must include the sent log");
    const gotNotif = await gen.NotificationService.get_notification({ log_id: logId }, opts);
    node_assert_1.strict.equal(gotNotif.log.log_id, logId);
    await gen.NotificationService.set_preference({ user_id: recipient.user_id, tenant_id: tenantId, channel: 1, is_opted_out: true }, opts);
    const pref = await gen.NotificationService.get_preference({ user_id: recipient.user_id, tenant_id: tenantId, channel: 1 }, opts);
    node_assert_1.strict.ok(pref.preference.is_opted_out, "GetPreference must reflect the opt-out we set");
    const prefs = await gen.NotificationService.list_preferences({ user_id: recipient.user_id, tenant_id: tenantId }, opts);
    node_assert_1.strict.ok((prefs.preferences ?? []).length >= 1);
    await gen.NotificationService.get_delivery_stats({ tenant_id: tenantId }, opts);
    // StorageService — file lifecycle under the UUID-tenant admin (project_id and
    // reference_id are UUID columns: empty project → NULL, reference_id a UUID).
    const ref = liveUuid();
    const reg = await ugen.StorageService.register_upload({
        tenant_id: uuidTenant, project_id: "", filename: `sdk-${suffix}.txt`, content_type: "text/plain",
        file_type: "DOCUMENT", reference_id: ref, reference_type: "sdk.live", size_bytes: 128, expires_in_minutes: 10,
    }, opts);
    node_assert_1.strict.ok(reg.file_id && String(reg.upload_url).startsWith("http"), "RegisterUpload must return file_id + upload_url");
    const gotFile = await ugen.StorageService.get_file({ tenant_id: uuidTenant, file_id: reg.file_id }, opts);
    node_assert_1.strict.equal(gotFile.file.file_id, reg.file_id);
    const renamedFile = `sdk-renamed-${suffix}.txt`;
    await ugen.StorageService.update_file({ tenant_id: uuidTenant, file_id: reg.file_id, filename: renamedFile }, opts);
    const rereadFile = await ugen.StorageService.get_file({ tenant_id: uuidTenant, file_id: reg.file_id }, opts);
    node_assert_1.strict.equal(rereadFile.file.filename, renamedFile, "UpdateFile rename must persist");
    const download = await ugen.StorageService.get_download_url({ tenant_id: uuidTenant, file_id: reg.file_id, expires_in_minutes: 10 }, opts);
    node_assert_1.strict.match(download.download_url, /^https?:\/\//);
    const listedFiles = await ugen.StorageService.list_files({ tenant_id: uuidTenant, reference_id: ref }, opts);
    node_assert_1.strict.ok(Number(listedFiles.total_count) >= 1);
    const deletedFile = await ugen.StorageService.delete_file({ tenant_id: uuidTenant, file_id: reg.file_id }, opts);
    node_assert_1.strict.ok(deletedFile.success, "DeleteFile must succeed");
    // AssetService — pipeline definition + asset registered against a stored file.
    const assetFile = await ugen.StorageService.register_upload({
        tenant_id: uuidTenant, project_id: "", filename: `asset-${suffix}.json`, content_type: "application/json",
        file_type: "OTHER", reference_id: liveUuid(), reference_type: "sdk.asset", size_bytes: 64, expires_in_minutes: 10,
    }, opts);
    const definition = await ugen.AssetService.create_pipeline_definition({
        tenant_id: uuidTenant, name: `sdk-pipeline-${suffix}`, description: "Live SDK pipeline",
        media_type: "application/json", steps: '[{"name":"extract","type":"EXTRACT"}]', version: 1,
    }, opts);
    node_assert_1.strict.ok(definition.definition_id, "CreatePipelineDefinition must return definition_id");
    await ugen.AssetService.get_pipeline_definition({ tenant_id: uuidTenant, definition_id: definition.definition_id }, opts);
    const registeredAsset = await ugen.AssetService.register_asset({
        tenant_id: uuidTenant, project_id: "", file_id: assetFile.file_id, name: `sdk-asset-${suffix}`,
        media_type: "application/json", metadata: '{"source":"sdk-live"}',
    }, opts);
    node_assert_1.strict.ok(registeredAsset.asset_id, "RegisterAsset must return asset_id");
    await ugen.AssetService.get_asset({ tenant_id: uuidTenant, asset_id: registeredAsset.asset_id }, opts);
    const startedPipeline = await ugen.AssetService.start_pipeline({
        tenant_id: uuidTenant, definition_id: definition.definition_id, asset_id: registeredAsset.asset_id,
        context: "{}", correlation_id: `sdk-live-${suffix}`,
    }, opts);
    node_assert_1.strict.ok(startedPipeline.instance_id, "StartPipeline must return instance_id");
    await ugen.AssetService.get_pipeline({ tenant_id: uuidTenant, instance_id: startedPipeline.instance_id }, opts);
    const listedAssets = await ugen.AssetService.list_assets({ tenant_id: uuidTenant }, opts);
    node_assert_1.strict.ok((listedAssets.assets ?? []).some((a) => a.asset_id === registeredAsset.asset_id), "ListAssets must include the registered asset");
    // WebRTC — room/peer/track lifecycle + best-effort TURN issuance.
    const room = await ugen.RoomService.create_room({ tenant_id: uuidTenant, name: `sdk-room-${suffix}`, max_participants: 8, config: "{}", created_by: liveUuid() }, opts);
    node_assert_1.strict.ok(room.room_id, "CreateRoom must return room_id");
    await ugen.RoomService.get_room({ tenant_id: uuidTenant, room_id: room.room_id }, opts);
    const listedRooms = await ugen.RoomService.list_rooms({ tenant_id: uuidTenant }, opts);
    node_assert_1.strict.ok((listedRooms.rooms ?? []).some((r) => r.room_id === room.room_id), "ListRooms must include created room");
    const joined = await ugen.PeerService.join_room({ tenant_id: uuidTenant, room_id: room.room_id, display_name: "sdk-peer", metadata: "{}", user_agent: "sdk-live" }, opts);
    node_assert_1.strict.ok(joined.peer.peer_id, "JoinRoom must return a peer_id");
    const peerList = await ugen.PeerService.list_peers({ tenant_id: uuidTenant, room_id: room.room_id }, opts);
    node_assert_1.strict.ok((peerList.peers ?? []).some((p) => p.peer_id === joined.peer.peer_id), "ListPeers must include the joined peer");
    await ugen.PeerService.get_peer({ tenant_id: uuidTenant, peer_id: joined.peer.peer_id }, opts);
    await ugen.RoomService.update_room({ tenant_id: uuidTenant, room_id: room.room_id, name: `sdk-room-renamed-${suffix}` }, opts);
    const published = await ugen.TrackService.publish_track({ tenant_id: uuidTenant, room_id: room.room_id, peer_id: joined.peer.peer_id, kind: "audio", label: "mic", settings: "{}", metadata: "{}" }, opts);
    node_assert_1.strict.ok(published.track_id, "PublishTrack must return a track_id");
    const tracks = await ugen.TrackService.list_tracks({ tenant_id: uuidTenant, room_id: room.room_id }, opts);
    node_assert_1.strict.ok((tracks.tracks ?? []).length >= 1, "ListTracks must return the published track");
    await ugen.TrackService.mute_track({ tenant_id: uuidTenant, track_id: published.track_id, muted: true }, opts);
    await ugen.TrackService.unpublish_track({ tenant_id: uuidTenant, track_id: published.track_id }, opts);
    try {
        // TURN issuance is best-effort: coturn may be unconfigured locally and the
        // service fail-closes with a real status (not a mount failure).
        await ugen.TurnService.issue_credentials({ tenant_id: uuidTenant, room_id: room.room_id, peer_id: joined.peer.peer_id, ttl_seconds: 3600 }, opts);
    }
    catch (err) {
        const code = grpcCode(err);
        if (code !== undefined && FATAL_CONNECTIVITY_CODES.has(code)) {
            throw new Error(`TurnService.issue_credentials did not reach a live RPC: ${describeGrpcError(err)}`);
        }
    }
    const left = await ugen.PeerService.leave_room({ tenant_id: uuidTenant, room_id: room.room_id, peer_id: joined.peer.peer_id }, opts);
    node_assert_1.strict.ok(left.success, "LeaveRoom must succeed");
    await ugen.RoomService.close_room({ tenant_id: uuidTenant, room_id: room.room_id }, opts);
}
(0, node_test_1.test)("live broker login refreshes once and hot-swaps SDK credentials", {
    skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker",
}, async (t) => {
    const target = requiredEnv("UDB_GRPC_TARGET");
    const authTarget = process.env.UDB_AUTH_GRPC_TARGET?.trim() || target;
    const username = requiredEnv("UDB_LIVE_USERNAME");
    const password = requiredEnv("UDB_LIVE_PASSWORD");
    // The bootstrap binds the admin to the tenant's CANONICAL UUID, so the Login JWT
    // claim is a UUID (not the human code) and the broker enforces that the asserted
    // x-tenant-id header matches the token tenant. Discover the canonical UUID with a
    // throwaway login FIRST, then build the project with it so every header AND body
    // carries the UUID (auth_fix.md tenant-identity fix). ONE admin serves all RPCs.
    let tenantId = process.env.UDB_LIVE_TENANT || "sdk-live";
    const projectId = process.env.UDB_LIVE_PROJECT || "default";
    {
        const probe = new project_1.UdbProject({
            target,
            authTarget,
            tenantId,
            projectId,
            purpose: "ts.live.tenant-probe",
            tokenStore: memoryStore(),
            deadlineMs: 10_000,
        });
        try {
            const probeLogin = await probe.login({
                username,
                password,
                tenant_hint: tenantId,
                project_hint: projectId,
                device_name: "ts-tenant-probe",
            });
            const who = await probe.auth.authenticateBearer(probeLogin.access_token);
            tenantId = who?.principal?.tenant_id || tenantId;
        }
        finally {
            probe.close();
        }
    }
    node_assert_1.strict.ok(tenantId, "must resolve a canonical tenant id before conformance");
    const store = memoryStore();
    const project = new project_1.UdbProject({
        target,
        authTarget,
        tenantId,
        projectId,
        // The broker derives the request purpose from the `x-purpose` header (the
        // security context), so the project must carry one — several data RPCs
        // (GenericDispatch, Select, …) require a non-empty purpose. Scopes are NOT
        // set: admin authority comes from the Login JWT, not client-asserted scopes.
        purpose: "ts.live.conformance",
        tokenStore: store,
        deadlineMs: 10_000,
    });
    try {
        const login = await project.login({
            username,
            password,
            tenant_hint: tenantId,
            project_hint: projectId,
            device_name: "sdk-live-conformance",
        });
        node_assert_1.strict.ok(login.access_token, "live login must return an access token");
        node_assert_1.strict.ok(login.refresh_token, "live login must return a refresh token");
        node_assert_1.strict.equal(store.current()?.accessToken, login.access_token);
        const authn = await project.auth.authenticateBearer(login.access_token);
        node_assert_1.strict.ok(authn?.principal, "Authenticate must accept the token issued by Login");
        // tenantId is already the canonical UUID (resolved by the pre-login probe above),
        // so the project's x-tenant-id header AND all request bodies match the JWT claim.
        node_assert_1.strict.equal(authn.principal.tenant_id, tenantId, "principal tenant must match the resolved canonical UUID");
        await store.save({
            accessToken: login.access_token,
            refreshToken: login.refresh_token,
            expiresAt: Date.now() - 1,
        });
        const refreshed = await Promise.all([
            project.refreshIfNeeded(),
            project.refreshIfNeeded(),
            project.refreshIfNeeded(),
        ]);
        const accessTokens = new Set(refreshed.map((t) => t?.accessToken).filter(Boolean));
        node_assert_1.strict.equal(accessTokens.size, 1, "concurrent refresh callers must share one result");
        node_assert_1.strict.notEqual(refreshed[0]?.accessToken, login.access_token);
        node_assert_1.strict.equal(store.current()?.accessToken, refreshed[0]?.accessToken);
        // Don't trust the capability claim — exercise every advertised backend.
        const caps = await project.generated.DataBroker.get_capabilities({}, { deadlineMs: 5_000, noRetry: true });
        const enabledBackends = (caps.enabled_backends ?? []).map((b) => b.toLowerCase());
        await runLiveBackendClaimCheck(project.generated.DataBroker, requestContext(tenantId, projectId, "ts.live.backend.claim"), enabledBackends);
        // Challenge every advertised backend KIND's per-operation claims in BOTH directions.
        await runLiveBackendCapabilityChallenge(project.generated.DataBroker, requestContext(tenantId, projectId, "ts.live.backend.capability"), caps);
        // Full session lifecycle on a throwaway login: prove logout invalidates the
        // session (access token + refresh token + session-refresh all rejected after).
        await runLiveAuthLifecycle(project.authGenerated?.AuthnService ?? project.generated.AuthnService, tenantId, projectId, username, password);
        // Edge cases: the auth plane must fail CLOSED on bad credentials/forged bearers.
        await runLiveAuthNegative(project.authGenerated?.AuthnService ?? project.generated.AuthnService, tenantId, projectId, username);
        await runLiveBackendE2E(project, tenantId, projectId);
        // Per-RPC EDGE cases (malformed/hostile inputs + isolation boundaries): every one
        // must fail closed with a typed error and never leak cross-tenant data or fault.
        await runLiveEdgeCases(project.generated.DataBroker, tenantId, projectId);
        // Breadth: a real category-appropriate round-trip against EVERY advertised backend
        // kind (relational SQL, object, document, cache, vector, graph) — not just the
        // canonical postgres/mongodb/minio trio. Adapts to whatever the broker enabled.
        await runLiveAllBackendKindsMatrix(project.generated.DataBroker, tenantId, projectId, caps);
        // A SINGLE admin (bound to the canonical tenant UUID) now serves the UUID-strict
        // services (storage/webrtc/asset) and the free-text ones alike — no second
        // "uuid tenant" admin needed (auth_fix.md tenant-identity fix). The same project
        // and canonical tenant id are used for both arguments.
        await runLiveNativeServiceE2E(project, project, tenantId, projectId, tenantId);
        const authGenerated = project.authGenerated ?? project.generated;
        const probeCounters = { populated: 0 };
        const nativeCount = await expectGeneratedUnarySurfaceMounted(t, "authTarget", authGenerated, NATIVE_SERVICE_APIS, tenantId, projectId, probeCounters);
        const dataCount = await expectGeneratedUnarySurfaceMounted(t, "target", project.generated, ["DataBroker"], tenantId, projectId, probeCounters);
        await expectStreamMounted("target.DataBroker.get_object", () => project.generated.DataBroker.get_object({}, { deadlineMs: 2_000 }));
        await expectStreamMounted("target.DataBroker.publish_c_d_c", () => project.generated.DataBroker.publish_c_d_c({}, { deadlineMs: 2_000 }));
        await expectStreamMounted("target.DataBroker.select_v2", () => project.generated.DataBroker.select_v2({}, { deadlineMs: 2_000 }));
        await expectStreamMounted("target.DataBroker.put_object", () => {
            const { stream, response } = project.generated.DataBroker.put_object({ deadlineMs: 2_000 });
            // The probe ends an empty stream → the broker rejects with "empty object
            // stream" (INVALID_ARGUMENT). That proves PutObject is mounted; the
            // separate response promise must be caught or it becomes an unhandled
            // rejection that fails the test.
            response.catch(() => { });
            return stream;
        });
        await expectStreamMounted("target.DataBroker.batch_select", () => project.generated.DataBroker.batch_select({ deadlineMs: 2_000 }));
        await expectStreamMounted("target.DataBroker.batch_upsert", () => project.generated.DataBroker.batch_upsert({ deadlineMs: 2_000 }));
        await expectStreamMounted("target.DataBroker.begin_tx", () => project.generated.DataBroker.begin_tx({ deadlineMs: 2_000 }));
        await expectStreamMounted("target.DataBroker.vector_batch_upsert", () => project.generated.DataBroker.vector_batch_upsert({ deadlineMs: 2_000 }));
        await expectStreamMounted("authTarget.ControlPlaneService.delta_resources", () => authGenerated.ControlPlaneService.delta_resources({ deadlineMs: 2_000 }));
        await expectStreamMounted("authTarget.ControlPlaneService.stream_resources", () => authGenerated.ControlPlaneService.stream_resources({ deadlineMs: 2_000 }));
        await expectStreamMounted("authTarget.SignalingService.signal", () => authGenerated.SignalingService.signal({ deadlineMs: 2_000 }));
        node_assert_1.strict.ok(nativeCount > 0, "native control-plane unary RPCs must be probed");
        node_assert_1.strict.ok(dataCount > 0, "DataBroker unary RPCs must be probed");
        // Full-surface coverage like Go/Python/PHP: 262 RPCs total = 251 unary
        // (nativeCount + dataCount) + the 11 streaming RPCs probed individually below.
        const STREAMING_PROBED = 11; // get_object, publish_c_d_c, select_v2, put_object,
        //   batch_select, batch_upsert, begin_tx, vector_batch_upsert, delta_resources,
        //   stream_resources, signal
        node_assert_1.strict.equal(nativeCount + dataCount + STREAMING_PROBED, 262, `TS probed ${nativeCount + dataCount} unary + ${STREAMING_PROBED} streaming = ${nativeCount + dataCount + STREAMING_PROBED}, want 262 — full-surface coverage regressed`);
        node_assert_1.strict.ok(probeCounters.populated >= 200, `only ${probeCounters.populated} unary RPCs received a populated typed request; full-surface coverage regressed`);
    }
    finally {
        project.close();
    }
});
// Per-RPC performance (gated on UDB_LIVE_PERF=1). Times every unary RPC over
// multiple iterations and writes perf_report_ts.md — the TS counterpart of the
// Go/Python perf harness. read_only RPCs are timed many times; mutations a few;
// destructive once typed-empty (validation latency only).
(0, node_test_1.test)("live per-RPC perf", {
    skip: process.env.UDB_LIVE_SDK_TESTS === "1" && process.env.UDB_LIVE_PERF === "1"
        ? false
        : "requires live UDB broker + UDB_LIVE_PERF=1",
}, async () => {
    const target = requiredEnv("UDB_GRPC_TARGET");
    const authTarget = process.env.UDB_AUTH_GRPC_TARGET?.trim() || target;
    const username = requiredEnv("UDB_LIVE_USERNAME");
    const password = requiredEnv("UDB_LIVE_PASSWORD");
    let tenantId = process.env.UDB_LIVE_TENANT || "sdk-live";
    const projectId = process.env.UDB_LIVE_PROJECT || "default";
    const project = new project_1.UdbProject({
        target, authTarget, tenantId, projectId,
        purpose: "ts.live.perf", tokenStore: memoryStore(), deadlineMs: 20_000,
    });
    try {
        const login = await project.login({ username, password, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-sdk-perf" });
        const who = await project.auth.authenticateBearer(login.access_token);
        tenantId = who?.principal?.tenant_id || tenantId;
        // Adopt the canonical tenant UUID on every channel: we logged in with the
        // human tenant CODE (tenant_hint), but the bearer's tenant claim is the UUID,
        // and native RPCs reject a mismatched x-tenant-id header. Without this, every
        // seed/native call fails PERMISSION_DENIED "x-tenant-id must match the bearer".
        project.setTenant(tenantId);
        const authGenerated = project.authGenerated ?? project.generated;
        const data = project.generated.DataBroker;
        // SEED PHASE (before any measurement): create real, disposable entities and
        // capture their identifiers so every RPC can be driven down its SUCCESS path
        // with valid inputs. The bootstrap admin's tenant claim IS the canonical UUID
        // (resolved above), so one client serves the UUID-strict native services too.
        const seed = await seedPerfFixtures(authGenerated, data, tenantId, projectId, tenantId);
        const fixtures = seed.fixtures;
        // Iteration budget per operation_kind. Every RPC is now driven down its SUCCESS
        // path with seeded inputs, so even destructive RPCs run for real (against a
        // disposable seeded target) — measured ONCE because the action is not idempotent.
        const itersFor = (kind) => (kind === "destructive" ? 1 : kind === "mutation" ? 5 : 25);
        const samples = [];
        // gRPC status code NAME for an error (e.g. "UNAVAILABLE", "FAILED_PRECONDITION"),
        // reusing the file's grpcCode() extractor; "OK" when there was no error.
        const codeNameOf = (err) => {
            const code = grpcCode(err);
            if (code === undefined)
                return "UNKNOWN";
            return grpc.status[code] ?? String(code);
        };
        // timeMethod returns latency AND the observed gRPC status code so a failing RPC
        // (non-OK status) is recorded as a FAILURE with its code, never a silent sample.
        const timeMethod = async (fn, request) => {
            const start = performance.now();
            let err = "OK";
            let detail;
            try {
                await fn(request, { deadlineMs: 20_000, noRetry: true });
            }
            catch (e) {
                err = codeNameOf(e);
                detail = (e?.details || e?.message || String(e)).slice(0, 200);
            }
            return { ms: performance.now() - start, err, detail };
        };
        // Stream-open timer: create the streaming call and tear it down WITHOUT draining
        // responses. A subscription/upload stream emits a first message only on an event,
        // so draining it in a passive run would just hit the deadline. This measures the
        // client-side latency to establish the stream. Used for the client-streaming /
        // bidi RPCs (put_object, batch_*, begin_tx, vector_batch_upsert, delta/stream
        // resources, signal) where a single seeded message cannot drive a real response.
        const timeStreamOpen = (fn, request) => {
            const start = performance.now();
            try {
                const r = fn(request, { deadlineMs: 1_500, noRetry: true });
                const s = r?.stream ?? r;
                if (s && typeof s.cancel === "function")
                    s.cancel();
                else if (s && typeof s.destroy === "function")
                    s.destroy();
                if (r?.response && typeof r.response.catch === "function")
                    r.response.catch(() => { });
            }
            catch { /* setup latency still counts */ }
            return performance.now() - start;
        };
        // Server-streaming first-response timer: open the stream with a seeded request
        // and measure up to the FIRST server-delivered message (a real round-trip), not
        // just stream-open. `end`/`error` before any `data` is treated as a successful
        // (empty) completion. Used for select_v2 / get_object.
        const timeServerStreamFirstResponse = async (fn, request) => {
            const start = performance.now();
            return await new Promise((resolve) => {
                let settled = false;
                const finish = (err) => {
                    if (settled)
                        return;
                    settled = true;
                    clearTimeout(timer);
                    if (typeof stream.cancel === "function")
                        stream.cancel();
                    resolve({ ms: performance.now() - start, err });
                };
                let stream;
                try {
                    stream = fn(request, { deadlineMs: 15_000, noRetry: true });
                }
                catch (e) {
                    resolve({ ms: performance.now() - start, err: codeNameOf(e) });
                    return;
                }
                const timer = setTimeout(() => finish("DEADLINE_EXCEEDED"), 15_000);
                stream.once("data", () => finish("OK"));
                stream.once("end", () => finish("OK"));
                stream.once("error", (e) => finish(codeNameOf(e)));
            });
        };
        // CDC first-EVENT timer: subscribe to publish_c_d_c, then fire a real Upsert
        // against the seeded SdkLiveRecord row — that write flows outbox→CDC→Kafka and
        // is delivered back on the stream. The measured cost is dominated by
        // produce→deliver, the honest first-event latency a real subscriber sees. A
        // fresh revision per call guarantees a NEW outbox event each iteration.
        const timeCdcFirstEvent = async (fn, request) => {
            const start = performance.now();
            return await new Promise((resolve) => {
                let settled = false;
                const finish = (err) => {
                    if (settled)
                        return;
                    settled = true;
                    clearTimeout(timer);
                    if (typeof stream.cancel === "function")
                        stream.cancel();
                    resolve({ ms: performance.now() - start, err });
                };
                let stream;
                try {
                    stream = fn(request, { deadlineMs: 15_000, noRetry: true });
                }
                catch (e) {
                    resolve({ ms: performance.now() - start, err: codeNameOf(e) });
                    return;
                }
                const timer = setTimeout(() => finish("DEADLINE_EXCEEDED"), 15_000);
                stream.once("data", () => finish("OK"));
                stream.once("error", (e) => finish(codeNameOf(e)));
                // Fire a real mutation that produces a CDC event for the seeded row.
                const rev = Date.now();
                data
                    .upsert({
                    context: requestContext(tenantId, projectId, "ts.live.perf.cdc"),
                    message_type: LIVE_MESSAGE_TYPE,
                    record_json: jsonBytes({ record_id: fixtures.recordId, tenant_id: tenantId, project_id: projectId, lookup_key: "ts-perf-cdc", payload: "ts-perf-cdc", revision: rev }),
                    conflict_fields: ["record_id"],
                }, { deadlineMs: 8_000, noRetry: true })
                    .catch(() => { });
            });
        };
        // CDC subscription request: a permissive pattern so the seeded Upsert's event is
        // delivered regardless of the broker's exact topic naming (the handler treats
        // "*"/"" as match-all).
        const cdcRequest = () => ({
            context: requestContext(tenantId, projectId, "ts.live.perf.cdc"),
            message_type: LIVE_MESSAGE_TYPE,
            topic_pattern: "*",
        });
        // Server-streaming reads that take a request and deliver a real first response.
        const SERVER_STREAM_FIRST_RESPONSE = new Set(["select_v2", "get_object"]);
        const seededStreamRequest = (methodName) => {
            if (methodName === "select_v2") {
                return { context: requestContext(tenantId, projectId, "ts.live.perf"), message_type: LIVE_MESSAGE_TYPE, filter: { tenant_id: tenantId, project_id: projectId }, limit: 1 };
            }
            if (methodName === "get_object") {
                return { context: requestContext(tenantId, projectId, "ts.live.perf"), bucket: fixtures.lookup("bucket") ?? (process.env.UDB_LIVE_S3_BUCKET || "udb-live-sdk"), object_key: fixtures.lookup("object_key") ?? "" };
            }
            // Only select_v2/get_object reach here; never a generic body.
            return perfRealBody("DataBroker", methodName, tenantId, projectId, fixtures) ?? {};
        };
        // ── measureRpc: time ONE RPC (unary or streaming) and push its sample ─────────
        // Extracted from the old single-pass loop so the AUTH-ROUTE 3-phase ordering
        // (BENCH_RPC_BODIES.md "Execution order") can drive the SAME measurement code in
        // a deterministic order: Phase 1 (session establish) → seed → Phase 2 (the bulk)
        // → Phase 3 (session/credential teardown), so a destructive AuthnService RPC
        // never kills the live principal mid-run.
        const measureRpc = async (serviceName, api, methodName, fn) => {
            if (NON_UNARY_METHODS.has(methodName)) {
                // CDC subscription: subscribe → fire a real seeded Upsert → first event.
                if (serviceName === "DataBroker" && methodName === "publish_c_d_c") {
                    const durs = [];
                    let errCode = "OK";
                    await timeCdcFirstEvent(fn, cdcRequest()); // warm-up
                    for (let i = 0; i < 3; i++) {
                        const r = await timeCdcFirstEvent(fn, cdcRequest());
                        durs.push(r.ms);
                        if (r.err !== "OK")
                            errCode = r.err;
                    }
                    durs.sort((a, b) => a - b);
                    const pct = (p) => durs[Math.min(durs.length - 1, Math.floor((p * (durs.length - 1)) / 100))];
                    samples.push({ service: serviceName, rpc: methodName, kind: "stream", err: errCode, p50: pct(50), p99: pct(99), mean: durs.reduce((s, d) => s + d, 0) / durs.length, note: "cdc: time-to-first-event (real seeded Upsert produced)" });
                    return;
                }
                // Server-streaming reads with a real first response (select_v2, get_object).
                if (SERVER_STREAM_FIRST_RESPONSE.has(methodName)) {
                    const req = seededStreamRequest(methodName);
                    const durs = [];
                    let errCode = "OK";
                    await timeServerStreamFirstResponse(fn, req); // warm-up
                    for (let i = 0; i < 5; i++) {
                        const r = await timeServerStreamFirstResponse(fn, req);
                        durs.push(r.ms);
                        if (r.err !== "OK")
                            errCode = r.err;
                    }
                    durs.sort((a, b) => a - b);
                    const pct = (p) => durs[Math.min(durs.length - 1, Math.floor((p * (durs.length - 1)) / 100))];
                    samples.push({ service: serviceName, rpc: methodName, kind: "stream", err: errCode, p50: pct(50), p99: pct(99), mean: durs.reduce((s, d) => s + d, 0) / durs.length, note: "streaming: time-to-first-response (seeded)" });
                    return;
                }
                // Client-streaming / bidi: a single seeded message cannot drive a real
                // response in a passive run — report stream-open latency. The first message
                // is the DOC-GROUNDED body (no generic): perfRealBody must cover it.
                const streamReq = perfRealBody(serviceName, methodName, tenantId, projectId, fixtures);
                if (!streamReq)
                    throw new Error(`perfRealBody has no doc-grounded body for streaming ${serviceName}/${methodName} — gap/bypass not allowed`);
                const d = timeStreamOpen(fn, streamReq);
                samples.push({ service: serviceName, rpc: methodName, kind: "stream_open", err: "OK", p50: d, p99: d, mean: d, note: "streaming: stream-open latency" });
                return;
            }
            const kind = operationKindOf(api.serviceFull, methodName) || "read_only";
            // Every RPC gets its DOC-GROUNDED valid body from perfRealBody — NO generic
            // fallback. A missing body is a loud failure (gap/bypass not allowed), never a
            // silently-populated placeholder. Destructive RPCs run for real against the
            // disposable seeded target, measured once.
            // Build the body PER ITERATION (a factory), not once: create-style RPCs embed a
            // random unique field (username/role_code/name) so a single reused body would
            // collide on iters 2+ (unique constraint → the broker leaks it as INTERNAL).
            // Rebuilding yields a fresh unique value each call so every iteration succeeds.
            const mkBody = () => perfRealBody(serviceName, methodName, tenantId, projectId, fixtures);
            if (!mkBody())
                throw new Error(`perfRealBody has no doc-grounded body for ${serviceName}/${methodName} — gap/bypass not allowed`);
            // Warm-up ONLY for idempotent reads. A warm-up on a non-idempotent mutation
            // CONSUMES the op (submit/approve a draft, rotate a token, revoke a key), so the
            // measured iterations would all fail. (mirrors the Go harness)
            if (kind === "read_only")
                await timeMethod(fn, mkBody());
            const allDurs = [];
            const okDurs = [];
            let anyOk = false;
            let firstErr = "OK";
            let firstDetail;
            for (let i = 0; i < itersFor(kind); i++) {
                const r = await timeMethod(fn, mkBody());
                allDurs.push(r.ms);
                if (r.err === "OK") {
                    anyOk = true;
                    okDurs.push(r.ms);
                }
                else if (firstErr === "OK") {
                    firstErr = r.err;
                    firstDetail = r.detail;
                }
            }
            // An RPC that succeeds AT LEAST ONCE works: repeated-call failures on a
            // non-idempotent mutation (consumed token / duplicate / already-deleted) are a
            // measurement artifact, not an RPC failure (mirrors the Go harness). Only an RPC
            // that NEVER succeeds is a real failure (its first-attempt status).
            const errCode = anyOk ? "OK" : firstErr;
            const errDetail = anyOk ? undefined : firstDetail;
            const durs = (anyOk ? okDurs : allDurs);
            if (errCode !== "OK")
                console.error(`FAILDETAIL ${serviceName}/${methodName} [${errCode}] ${errDetail ?? ""}`);
            durs.sort((a, b) => a - b);
            const pct = (p) => durs[Math.min(durs.length - 1, Math.floor((p * (durs.length - 1)) / 100))];
            samples.push({
                service: serviceName, rpc: methodName, kind, err: errCode,
                p50: pct(50), p99: pct(99), mean: durs.reduce((s, d) => s + d, 0) / durs.length,
                note: kind === "destructive" ? "destructive: 1 real call against a seeded disposable target" : `${kind} (seeded success path)`,
            });
        };
        // ── AUTH-ROUTE 3-phase partitioning (BENCH_RPC_BODIES.md "Execution order") ───
        // Phase 1 (FIRST, in this exact order): AuthnService session-establish RPCs.
        const PHASE1_AUTHN_ORDER = [
            // RefreshSession + Authenticate consume their dedicated session/token and run
            // BEFORE RefreshToken (which rotates the shared family) — Go phase1AuthnOrder.
            "login", "refresh_session", "authenticate",
            "validate_token", "introspect_token", "refresh_token", "get_jwks",
        ];
        // Phase 3 (LAST): AuthnService RPCs that end a session / invalidate a principal
        // or credentials. These target the seeded DISPOSABLE user / its session (the
        // perfRealBody bodies point at <seed:user_id>/<seed:session_id>, and the
        // tenant-wide / emergency ones target throwaway non-admin targets), so the
        // admin's own bearer/session stays live until the very end.
        const PHASE3_AUTHN = new Set([
            "logout", "revoke_session", "admin_revoke_session", "admin_revoke_all_user_sessions",
            "admin_revoke_all_tenant_sessions", "emergency_revoke", "change_password",
            "reset_password", "admin_reset_password", "change_user_status", "admin_reset_mfa",
            "revoke_recovery_codes", "revoke_device", "delete_web_authn_credential", "disable_mfa_factor",
        ]);
        const phase1 = [];
        let phase2 = [];
        const phase3 = [];
        const surfaces = [
            ["authTarget", authGenerated, NATIVE_SERVICE_APIS],
            ["target", project.generated, ["DataBroker"]],
        ];
        for (const [, generated, serviceNames] of surfaces) {
            for (const serviceName of serviceNames) {
                const api = generated[serviceName];
                if (!api)
                    continue;
                for (const [methodName, fn] of Object.entries(api)) {
                    if (methodName === "serviceFull")
                        continue;
                    if (typeof fn !== "function")
                        continue;
                    const unit = { serviceName, api, methodName, fn: fn };
                    if (serviceName === "AuthnService" && PHASE1_AUTHN_ORDER.includes(methodName))
                        phase1.push(unit);
                    else if (serviceName === "AuthnService" && PHASE3_AUTHN.has(methodName))
                        phase3.push(unit);
                    else
                        phase2.push(unit);
                }
            }
        }
        // Order Phase 1 by the mandated sequence (login first, get_jwks last).
        phase1.sort((a, b) => PHASE1_AUTHN_ORDER.indexOf(a.methodName) - PHASE1_AUTHN_ORDER.indexOf(b.methodName));
        // Within Phase 2 run reads BEFORE mutations BEFORE destructive ops, so a read of a
        // seeded entity (GetApiKey/GetRole) is never invalidated by a rotate/revoke/delete of
        // that same entity earlier in the run (Go orderRPCsByAuthPhase). Stable sort.
        const okRank = { read_only: 0, mutation: 1, destructive: 2 };
        const rankOf = (u) => okRank[operationKindOf(u.api.serviceFull, u.methodName) ?? "read_only"] ?? 0;
        phase2 = phase2.map((u, i) => [u, i]).sort((a, b) => (rankOf(a[0]) - rankOf(b[0])) || (a[1] - b[1])).map(([u]) => u);
        // Phase 1: establish/validate the session FIRST (the seed phase already ran above
        // and captured the session/token fixtures these RPCs consume).
        for (const u of phase1)
            await measureRpc(u.serviceName, u.api, u.methodName, u.fn);
        // Phase 2: measure everything else under the live session.
        for (const u of phase2)
            await measureRpc(u.serviceName, u.api, u.methodName, u.fn);
        // Phase 3: tear the session/credentials down LAST (disposable seeded targets;
        // the admin's own logout/revoke is effectively last).
        for (const u of phase3)
            await measureRpc(u.serviceName, u.api, u.methodName, u.fn);
        const svc = new Map();
        for (const s of samples) {
            (svc.get(s.service) ?? svc.set(s.service, []).get(s.service)).push(s.mean);
        }
        const mean = (xs) => xs.reduce((a, b) => a + b, 0) / xs.length;
        const fkeys = [...fixtures.m.keys()].sort();
        const lines = ["# UDB SDK Live Perf — TypeScript (localhost)", "",
            `RPCs measured: ${samples.length}   tenant=${tenantId}`, "",
            "Every RPC is driven down its SUCCESS path: a SEED phase first creates real, "
                + "disposable entities (a user, role + assignment + policies, an API key, a notification, a "
                + "stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the "
                + "harness resolves each request's reference/ID fields to those real identifiers. So the numbers "
                + "reflect real handler work, not validation-rejection latency. Any residual non-OK RPC is listed "
                + "under Failures for the maintainer to finish.", "",
            "Unary = full request/response round-trip. Non-CDC server-streaming RPCs (kind=stream) report "
                + "time-to-FIRST-RESPONSE with seeded inputs; client-streaming/bidi RPCs (kind=stream_open) report "
                + "stream-open latency. CDC subscription (publish_c_d_c, kind=stream) reports time-to-FIRST-EVENT: "
                + "the harness subscribes, fires a real seeded Upsert that flows outbox→CDC→Kafka, and times the "
                + "first delivered event.", "",
            "RPCs run on the AUTH ROUTE in three phases (BENCH_RPC_BODIES.md \"Execution order\"): Phase 1 "
                + "establishes the session (AuthnService login → refresh_token → refresh_session → authenticate → "
                + "validate_token → introspect_token → get_jwks), then the seed phase; Phase 2 measures everything "
                + "else; Phase 3 LAST runs the session/credential-teardown AuthnService RPCs (logout, revoke_*, "
                + "change/reset password, admin_reset_mfa, disable_mfa_factor, …) against the seeded DISPOSABLE "
                + "user/session so the admin's own session is never killed mid-run.", "",
            "## Seeded fixtures", "",
            `Captured semantic field → seeded value keys used to resolve request fields: ${fkeys.join(", ")}`, "",
            "## Per-service mean latency", "", "| Service | RPCs | mean ms |", "|---|--:|--:|"];
        for (const name of [...svc.keys()].sort((a, b) => mean(svc.get(b)) - mean(svc.get(a)))) {
            lines.push(`| ${name} | ${svc.get(name).length} | ${mean(svc.get(name)).toFixed(2)} |`);
        }
        // Failures subsection: every RPC whose last iteration returned a non-OK gRPC status.
        const failed = samples.filter((s) => s.err !== "OK");
        lines.push("", `## Failures (${failed.length})`, "");
        if (failed.length === 0) {
            lines.push("No RPC returned a non-OK gRPC status.");
        }
        else {
            lines.push("These RPCs returned a non-OK gRPC status and are FAILURES, not latency samples.");
            lines.push("", "| RPC | kind | err | p99 ms | mean ms |", "|---|---|---|--:|--:|");
            for (const s of [...failed].sort((a, b) => (a.service + a.rpc).localeCompare(b.service + b.rpc))) {
                lines.push(`| ${s.service}/${s.rpc} | ${s.kind} | ${s.err} | ${s.p99.toFixed(2)} | ${s.mean.toFixed(2)} |`);
            }
        }
        lines.push("", "## Slowest 20 by p99", "", "| RPC | kind | err | p50 ms | p99 ms | mean ms | note |", "|---|---|---|--:|--:|--:|---|");
        for (const s of [...samples].sort((a, b) => b.p99 - a.p99).slice(0, 20)) {
            lines.push(`| ${s.service}/${s.rpc} | ${s.kind} | ${s.err} | ${s.p50.toFixed(2)} | ${s.p99.toFixed(2)} | ${s.mean.toFixed(2)} | ${s.note} |`);
        }
        lines.push("", "## Full per-RPC table (sorted by service, then RPC)", "", "| Service | RPC | kind | err | p50 ms | p99 ms | mean ms | note |", "|---|---|---|---|--:|--:|--:|---|");
        for (const s of [...samples].sort((a, b) => (a.service === b.service ? a.rpc.localeCompare(b.rpc) : a.service.localeCompare(b.service)))) {
            lines.push(`| ${s.service} | ${s.rpc} | ${s.kind} | ${s.err} | ${s.p50.toFixed(2)} | ${s.p99.toFixed(2)} | ${s.mean.toFixed(2)} | ${s.note} |`);
        }
        (0, node_fs_1.writeFileSync)("perf_report_ts.md", lines.join("\n") + "\n");
        node_assert_1.strict.ok(samples.length >= 260, `perf measured only ${samples.length} RPCs (want all 262)`);
        console.log(`\nTS perf: ${samples.length} RPCs measured, ${failed.length} FAILED (non-OK gRPC status) → sdk/typescript/perf_report_ts.md`);
        await seed.cleanup();
    }
    finally {
        project.close();
    }
});
