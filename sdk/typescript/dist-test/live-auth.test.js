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
const path = __importStar(require("node:path"));
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
// The native-service API surface the live runner drives on the AUTH route is
// DERIVED from the generated RPC catalog (RPC_OPERATION_KIND), never hand-listed:
// so every service the generator emits — including the newer Vault / Metering /
// Scheduler / Search / Webhook / Workflow / Lock / LiveQuery / Config / Backup /
// Embedding / Cache services — is iterated, and the full-surface coverage gates
// (expectedRpcCount / expectedPerfCount = the whole RPC_OPERATION_KIND count) can
// pass. DataBroker is excluded because it is driven on the DATA route (`target`).
// The short name (last dotted segment of the service path) is exactly the property
// name the generated client exposes each service under (e.g. `LiveQueryService`).
function serviceShortNameOf(fullPath) {
    const slash = fullPath.lastIndexOf("/");
    const servicePath = fullPath.slice(1, slash);
    return servicePath.slice(servicePath.lastIndexOf(".") + 1);
}
const NATIVE_SERVICE_APIS = [
    ...new Set(Object.keys(generatedClient_1.RPC_OPERATION_KIND)
        .map(serviceShortNameOf)
        .filter((service) => service !== "DataBroker")),
].sort();
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
    "entity",
    "table",
    "get_object",
    "publish_cdc",
    "select_v_2",
    "put_object",
    "delta_resources",
    "stream_resources",
    "signal",
    "batch_select",
    "batch_upsert",
    "begin_tx",
    "vector_batch_upsert",
    "download_file",
    // LiveQueryService.Subscribe is server-streaming (counted in STREAMING_PROBED,
    // not the unary surface), so it must be excluded from the unary probe/measure.
    "subscribe",
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
    const acronymSafe = {
        publish_cdc: "PublishCDC",
        send_otp: "SendOTP",
        verify_otp: "VerifyOTP",
        resend_otp: "ResendOTP",
        validate_csrf: "ValidateCSRF",
        enroll_mfa: "EnrollMFA",
        confirm_mfaenrollment: "ConfirmMFAEnrollment",
        publishCdc: "PublishCDC",
        sendOtp: "SendOTP",
        verifyOtp: "VerifyOTP",
        resendOtp: "ResendOTP",
        validateCsrf: "ValidateCSRF",
        enrollMfa: "EnrollMFA",
        confirmMfaenrollment: "ConfirmMFAEnrollment",
    };
    if (acronymSafe[s])
        return acronymSafe[s];
    return s.split("_").map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join("");
}
function snakeToCamel(s) {
    const pascal = snakeToPascal(s);
    return pascal.charAt(0).toLowerCase() + pascal.slice(1);
}
function rpcPathOf(serviceFull, methodSnake) {
    const direct = `/${serviceFull}/${snakeToPascal(methodSnake)}`;
    if (generatedClient_1.RPC_OPERATION_KIND[direct])
        return direct;
    const servicePrefix = `/${serviceFull}/`;
    const methodCamel = snakeToCamel(methodSnake);
    for (const path of Object.keys(generatedClient_1.RPC_OPERATION_KIND)) {
        if (!path.startsWith(servicePrefix))
            continue;
        if (generatedClient_1.RPC_API_ALIAS[path] === methodSnake)
            return path;
        if (generatedClient_1.RPC_OPERATION_ID[path] === methodSnake || generatedClient_1.RPC_OPERATION_ID[path] === methodCamel)
            return path;
    }
    return direct;
}
function operationKindOf(serviceFull, methodSnake) {
    return generatedClient_1.RPC_OPERATION_KIND[rpcPathOf(serviceFull, methodSnake)];
}
function apiAliasOf(serviceFull, methodSnake) {
    return generatedClient_1.RPC_API_ALIAS[rpcPathOf(serviceFull, methodSnake)] || methodSnake;
}
function operationIdOf(serviceFull, methodSnake) {
    return generatedClient_1.RPC_OPERATION_ID[rpcPathOf(serviceFull, methodSnake)] || snakeToCamel(methodSnake);
}
// ── bench-body manifest consumer (chapter 11.1.3 / 11.1.5.2) ──────────────────
// docs/bench-bodies/*.md is the cross-SDK SINGLE SOURCE OF TRUTH for the per-RPC
// valid request body (Go/Python/PHP/TS each load it). Each `| done | RPC | … |`
// row's col2 is the RPC name (bare PascalCase like `Select`, or `Service.Method`
// like `PeerService.JoinRoom`) and col5 is the strict JSON body cell. The TS
// bench harness hydrates every measured request from this manifest; there is no
// retained typed `*Body` switch fallback.
function benchBodiesDir() {
    const candidates = [
        path.resolve(__dirname, "../../../docs/bench-bodies"), // dev: dist-test/ -> repo/docs
        path.resolve(__dirname, "../../docs/bench-bodies"),
        path.resolve(__dirname, "../docs/bench-bodies"),
    ];
    for (const c of candidates) {
        try {
            if ((0, node_fs_1.readdirSync)(c).some((f) => f.endsWith(".md")))
                return c;
        }
        catch {
            /* not this candidate */
        }
    }
    return candidates[0];
}
// benchBodiesJSONPath resolves the GENERATED machine-readable manifest
// (scripts/gen-bench-bodies-json.mjs → docs/generated/bench-bodies.json), the new
// consumer source of truth. The markdown corpus stays the human-editable source;
// the drift test below proves the JSON equals a fresh markdown parse.
function benchBodiesJSONPath() {
    const candidates = [
        path.resolve(__dirname, "../../../docs/generated/bench-bodies.json"),
        path.resolve(__dirname, "../../docs/generated/bench-bodies.json"),
        path.resolve(__dirname, "../docs/generated/bench-bodies.json"),
    ];
    for (const c of candidates) {
        try {
            (0, node_fs_1.readFileSync)(c);
            return c;
        }
        catch {
            /* not this candidate */
        }
    }
    return candidates[0];
}
function duplicateRpcNames() {
    const counts = new Map();
    for (const fullPath of Object.keys(generatedClient_1.RPC_OPERATION_KIND)) {
        const method = fullPath.slice(fullPath.lastIndexOf("/") + 1);
        counts.set(method, (counts.get(method) ?? 0) + 1);
    }
    return new Set([...counts].filter(([, count]) => count > 1).map(([name]) => name));
}
function rpcBenchKey(fullPath) {
    const slash = fullPath.lastIndexOf("/");
    const servicePath = fullPath.slice(1, slash);
    const service = servicePath.slice(servicePath.lastIndexOf(".") + 1);
    const method = fullPath.slice(slash + 1);
    return duplicateRpcNames().has(method) ? `${service}.${method}` : method;
}
function rpcServiceKey(fullPath) {
    const slash = fullPath.lastIndexOf("/");
    const servicePath = fullPath.slice(1, slash);
    const service = servicePath.slice(servicePath.lastIndexOf(".") + 1);
    const method = fullPath.slice(slash + 1);
    return `${service}.${method}`;
}
function manifestRpcKey(rpc) {
    return rpc;
}
function loadBenchBodyEntries() {
    return JSON.parse((0, node_fs_1.readFileSync)(benchBodiesJSONPath(), "utf8"));
}
// Map of RPC manifest key → body cell, read from the generated JSON manifest.
// Unique RPC names use the bare method name; duplicated names use Service.Method.
function loadBenchBodyRows() {
    const entries = loadBenchBodyEntries();
    const rows = new Map();
    for (const e of entries) {
        if (!e.rpc)
            continue;
        const key = manifestRpcKey(e.rpc);
        if (rows.has(key)) {
            throw new Error(`bench-body manifest has a duplicate RPC key "${key}"`);
        }
        rows.set(key, e.body ?? "");
    }
    const expected = Object.keys(generatedClient_1.RPC_OPERATION_KIND).length;
    if (rows.size !== expected) {
        throw new Error(`bench-body manifest has ${rows.size} RPC rows in docs/generated/bench-bodies.json, want current generated RPC count ${expected}`);
    }
    return rows;
}
// parseBenchBodyMarkdownRows re-parses the human-editable markdown corpus — the
// LEGACY parse, kept ONLY to power the drift test that proves the generated JSON
// still equals a fresh markdown parse.
function parseBenchBodyMarkdownRows() {
    const dir = benchBodiesDir();
    const rows = new Map();
    let total = 0;
    for (const file of (0, node_fs_1.readdirSync)(dir)) {
        if (!file.endsWith(".md") || file === "workflow-sequences.md")
            continue;
        const text = (0, node_fs_1.readFileSync)(path.join(dir, file), "utf8");
        for (const line of text.split(/\r?\n/)) {
            const cells = line.split("|").map((c) => c.trim());
            if (cells.length < 6 || !/^\[.?\]$/.test(cells[1]))
                continue;
            const rpc = cells[2];
            if (!rpc)
                continue;
            total += 1;
            rows.set(manifestRpcKey(rpc), cells[5] ?? "");
        }
    }
    const expected = Object.keys(generatedClient_1.RPC_OPERATION_KIND).length;
    if (total !== expected) {
        throw new Error(`bench-body markdown has ${total} RPC rows, want current generated RPC count ${expected}`);
    }
    return rows;
}
(0, node_test_1.test)("bench-bodies.json matches a fresh markdown parse (R6.1 drift gate)", () => {
    const fromJSON = loadBenchBodyRows();
    const fromMD = parseBenchBodyMarkdownRows();
    const expected = Object.keys(generatedClient_1.RPC_OPERATION_KIND).length;
    node_assert_1.strict.equal(fromJSON.size, expected, `JSON manifest has ${fromJSON.size} rows, want ${expected}`);
    node_assert_1.strict.equal(fromMD.size, expected, `markdown manifest has ${fromMD.size} rows, want ${expected}`);
    const diffs = [];
    for (const [name, body] of fromMD) {
        if (!fromJSON.has(name))
            diffs.push(`missing in JSON: ${name}`);
        else if (fromJSON.get(name) !== body)
            diffs.push(`body mismatch for ${name}`);
    }
    for (const name of fromJSON.keys()) {
        if (!fromMD.has(name))
            diffs.push(`stale in JSON (not in markdown): ${name}`);
    }
    node_assert_1.strict.equal(diffs.length, 0, `bench-bodies.json drifted from markdown (run \`node scripts/gen-bench-bodies-json.mjs\`):\n${diffs.sort().join("\n")}`);
});
(0, node_test_1.test)("bench-body manifest matches the generated RPC contract", () => {
    const rows = loadBenchBodyRows();
    const expected = Object.keys(generatedClient_1.RPC_OPERATION_KIND).length;
    node_assert_1.strict.equal(rows.size, expected, `manifest has ${rows.size} unique RPC rows, want ${expected}`);
    // Every RPC the perf harness drives MUST have a manifest row — the manifest is
    // the only request-body source.
    const missing = [];
    for (const fullPath of Object.keys(generatedClient_1.RPC_OPERATION_KIND)) {
        const method = rpcBenchKey(fullPath);
        if (!rows.has(method) && !rows.has(rpcServiceKey(fullPath)))
            missing.push(fullPath);
    }
    node_assert_1.strict.equal(missing.length, 0, `RPC(s) on the generated surface have no bench-body manifest row: ${missing.join(", ")}`);
    // The bijection also holds the other way: a manifest row with no generated RPC
    // is stale contract drift.
    const surfaceShort = new Set(Object.keys(generatedClient_1.RPC_OPERATION_KIND).flatMap((p) => [rpcBenchKey(p), rpcServiceKey(p)]));
    const orphan = [...rows.keys()].filter((m) => !surfaceShort.has(m));
    node_assert_1.strict.equal(orphan.length, 0, `bench-body manifest row(s) have no generated RPC (stale contract): ${orphan.join(", ")}`);
});
function manifestBodyFor(serviceName, methodName) {
    const method = snakeToPascal(methodName);
    // Prefer the generated service+alias metadata before ambiguous bare RPC names.
    // Example: CacheService/cache_get must not hydrate from DataBroker/CacheGet.
    for (const entry of loadBenchBodyEntries()) {
        if (entry.service !== serviceName)
            continue;
        if (entry.api_alias === methodName ||
            entry.operation_id === methodName ||
            entry.operation_id === snakeToCamel(methodName) ||
            entry.rpc === method ||
            entry.rpc === `${serviceName}.${method}` ||
            entry.wire_rpc === `${serviceName}/${method}`) {
            return entry.body ?? "";
        }
    }
    const rows = loadBenchBodyRows();
    const candidates = [`${serviceName}.${method}`, method];
    for (const key of candidates) {
        const body = rows.get(key);
        if (body !== undefined)
            return body;
    }
    return undefined;
}
function strictManifestJSONCell(body) {
    const trimmed = body.trim();
    const unwrapped = trimmed.startsWith("`") && trimmed.endsWith("`") ? trimmed.slice(1, -1).trim() : trimmed;
    if (!unwrapped.startsWith("{") || !unwrapped.endsWith("}"))
        return undefined;
    return unwrapped;
}
function resolveManifestSeeds(body, fixtures) {
    return body.replace(/<seed:([^>]+)>/g, (_match, rawKey) => {
        const key = String(rawKey).trim().toLowerCase();
        const value = fixtures?.lookup(key);
        if (!value)
            throw new Error(`missing bench manifest seed ${key}`);
        return value;
    });
}
function manifestJSONBody(serviceName, methodName, fixtures) {
    const cell = manifestBodyFor(serviceName, methodName);
    if (!cell)
        return undefined;
    const jsonCell = strictManifestJSONCell(cell);
    if (!jsonCell)
        return undefined;
    const resolved = resolveManifestSeeds(jsonCell, fixtures);
    if (!resolved)
        return undefined;
    return JSON.parse(resolved);
}
function uniquifyPerfBody(serviceName, methodName, body) {
    if (!body || typeof body !== "object")
        return body;
    const rpc = `${serviceName}.${snakeToPascal(methodName)}`;
    const suffix = `${process.pid}${Date.now()}${Math.floor(Math.random() * 1_000_000)}`;
    if (rpc === "AuthnService.CreateUser") {
        body.username = `perf-u-${suffix}`;
        body.email = `perf-u-${suffix}@acme.test`;
    }
    else if (rpc === "AssetService.CreatePipelineDefinition") {
        body.name = `thumbnail-pipeline-${suffix}`;
    }
    return body;
}
function fullSurfaceManifestFixtures() {
    const fixtures = new PerfFixtures();
    for (const [key, value] of Object.entries({
        tenant_id: "tenant-1", tenant: "tenant-1", project: "project-1", project_id: "project-1",
        tenant_code: "tenant-code-1", purge_tenant_id: "tenant-1",
        message_type: LIVE_MESSAGE_TYPE, record_id: "record-1", bucket: "bucket-1", object_key: "object-1",
        document_id: "document-1", mongo_collection: "collection_1", node_id: "node-1",
        user_id: "user-1", subject: "user:user-1", session_id: "session-1", token: "token-1",
        grant_binding_id: "grant-binding-1", grant_create_user_id: "grant-create-user-1",
        refresh_token: "refresh-1", csrf_token: "csrf-1", code: "123456", role_id: "role-1",
        admin_reset_mfa_user_id: "admin-reset-mfa-user-1",
        admin_reset_password_user_id: "admin-reset-password-user-1",
        change_password_user_id: "change-password-user-1",
        change_status_user_id: "change-status-user-1",
        disable_mfa_user_id: "disable-mfa-user-1",
        revoke_recovery_user_id: "revoke-recovery-user-1",
        revoke_device_user_id: "revoke-device-user-1",
        revoke_device_id: "revoke-device-1",
        role: "reader", role_code: "reader", user_role_id: "user-role-1", policy_id: "1",
        policy_draft_id: "draft-1", relation: "member", object: "group:bench", resource: "invoice",
        action: "data.select", key_id: "key-1", plain_key: "udbk_key", stage_name: "stage-1",
        event_type: "event-1", log_id: "log-1", file_id: "file-1", definition_id: "definition-1",
        asset_id: "asset-1", instance_id: "instance-1", room_id: "room-1", peer_id: "peer-1",
        track_id: "track-1", provider_id: "provider-1", migration_id: "migration-1", saga_id: "saga-1",
        apply_run_id: "apply-run-1", approval_token: "approval-token-1", approve_run_id: "approve-run-1",
        auth_challenge_id: "auth-challenge-1", backup_id: "backup-1", canary_id: "canary-1",
        cancel_workflow_id: "cancel-workflow-1", catalog_manifest_b64: "e30=", challenge_id: "challenge-1",
        close_room_id: "close-room-1", delete_endpoint_id: "delete-endpoint-1", delete_file_id: "delete-file-1",
        device_id: "device-1", dismiss_dlq_id: "dismiss-dlq-1", dlq_id: "dlq-1",
        ds_policy_id: "2", egress_id: "eg-tenant-1-00000000-0000-4000-8000-000000000001", endpoint_id: "endpoint-1", external_identity_id: "external-1",
        fencing_token: "1", finalize_file_id: "finalize-file-1", gov_exp: "1900000000", job_id: "job-1",
        // Separate disposable targets: AdminPurgeTenant must not point at the caller's
        // tenant, the grant transfer needs its own source account, and FinalizeUpload must
        // resend the reference_id established at register_upload.
        admin_purge_tenant_id: "tenant-admin-purge-1", grant_transfer_from_user_id: "user-grant-from-1",
        finalize_reference_id: "finalize-ref-1",
        embedding_job_id: "11111111-1111-4111-8111-000000000101",
        embedding_work_item_id: "11111111-1111-4111-8111-000000000102",
        embedding_document_id: "11111111-1111-4111-8111-000000000103",
        embedding_document_job_id: "11111111-1111-4111-8111-000000000104",
        embedding_delete_model_id: "embedding-delete-model-1",
        join_session_room_id: "join-room-1", leave_peer_id: "leave-peer-1", mark_saga_id: "mark-saga-1",
        otp_code: "123456", otp_id: "otp-1", owner_id: "owner-1", quarantine_dlq_id: "quarantine-dlq-1",
        refresh_session_id: "refresh-session-1", reg_challenge_id: "reg-challenge-1", replay_dlq_id: "replay-dlq-1",
        reset_otp_code: "654321", reset_otp_id: "reset-otp-1", resource_name: "resource-1",
        restore_tenant_id: "restore-tenant-1", retry_saga_id: "retry-saga-1", revoke_key_id: "revoke-key-1",
        saml_provider_id: "saml-provider-1", scim_group_id: "sdk-perf-group", scim_user_id: "scim-user-1",
        signal_peer_id: "signal-peer-1", step_id: "step-1", topic_pattern: "topic.*", ts_table: "sdk_timeseries",
        unpublish_track_id: "unpublish-track-1", update_key_id: "update-key-1", username: "perf-u",
        vault_ciphertext: "vault-ciphertext-1", vault_db_role: "readonly", vault_delete_secret_path: "secret/delete",
        vault_destroy_secret_path: "secret/destroy", vault_key_name: "transit-key", vault_secret_path: "secret/path",
        vault_signature: "vault-signature-1", vault_signing_key_name: "transit-signing-key", vault_hmac_key_name: "transit-hmac-key", reissue_file_id: "reissue-file-1", workflow_id: "workflow-1",
        approve_draft_id: "approve-draft-1", canary_version_id: "canary-version-1",
        policy_version_id: "policy-version-1", reject_draft_id: "reject-draft-1",
        rollback_policy_set_id: "rollback-policy-set-1", rollback_target_version_id: "rollback-target-version-1",
        rollback_resource_version: "rollback-resource-version-1",
        update_draft_id: "update-draft-1", release_fencing_token: "1", renew_fencing_token: "1",
        vault_create_key_name: "transit-create-key", vault_put_secret_path: "secret/put",
    })) {
        fixtures.set(key, value);
    }
    return fixtures;
}
(0, node_test_1.test)("manifest-only perf body covers every generated RPC", () => {
    const fixtures = fullSurfaceManifestFixtures();
    const missing = [];
    for (const fullPath of Object.keys(generatedClient_1.RPC_OPERATION_KIND)) {
        const slash = fullPath.lastIndexOf("/");
        const servicePath = fullPath.slice(1, slash);
        const serviceName = servicePath.slice(servicePath.lastIndexOf(".") + 1);
        const methodName = fullPath.slice(slash + 1);
        if (perfRealBody(serviceName, methodName, "tenant-1", "project-1", fixtures) === undefined) {
            missing.push(`${serviceName}/${methodName}`);
        }
    }
    node_assert_1.strict.equal(missing.length, 0, `manifest-only perf body gaps: ${missing.join(", ")}`);
});
(0, node_test_1.test)("manifest JSON body hydrates AnalyticsService rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("stage_name", "stage-1");
    const body = manifestJSONBody("AnalyticsService", "get_pipeline_summary", fixtures);
    node_assert_1.strict.deepEqual(body?.page, { page: 1, page_size: 50 });
    node_assert_1.strict.equal(body?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(body?.stage_name, "stage-1");
});
(0, node_test_1.test)("manifest JSON body hydrates TenantService rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("tenant_code", "tenant-code-1");
    fixtures.set("purge_tenant_id", "tenant-1");
    const created = manifestJSONBody("TenantService", "create_tenant", fixtures);
    const tenant = manifestJSONBody("TenantService", "get_tenant", fixtures);
    const config = manifestJSONBody("TenantService", "get_tenant_config", fixtures);
    const list = manifestJSONBody("TenantService", "list_tenants", fixtures);
    const purged = manifestJSONBody("TenantService", "purge_tenant", fixtures);
    const updated = manifestJSONBody("TenantService", "update_tenant", fixtures);
    const updatedConfig = manifestJSONBody("TenantService", "update_tenant_config", fixtures);
    node_assert_1.strict.equal(created?.code, "tenant-code-1");
    node_assert_1.strict.equal(created?.config, "{}");
    node_assert_1.strict.equal(created?.branding, "{}");
    node_assert_1.strict.equal(tenant?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(config?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(list?.page_size, 20);
    node_assert_1.strict.equal(purged?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(purged?.confirmation_token, "sdk-perf-confirm-purge");
    node_assert_1.strict.equal(updated?.status, "active");
    node_assert_1.strict.equal(updatedConfig?.config_key, "feature.flag");
    node_assert_1.strict.equal(updatedConfig?.config_value, "on");
});
(0, node_test_1.test)("manifest JSON body hydrates DataBroker scalar read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("message_type", "myapp.v1.Invoice");
    fixtures.set("dlq_id", "dlq-1");
    fixtures.set("saga_id", "saga-1");
    fixtures.set("migration_id", "migration-1");
    fixtures.set("object_key", "cache-key-1");
    fixtures.set("mongo_collection", "invoices");
    fixtures.set("document_id", "document-1");
    fixtures.set("record_id", "record-1");
    fixtures.set("bucket", "bucket-1");
    fixtures.set("ts_table", "metrics_1");
    fixtures.set("event_type", "invoice.updated");
    const capabilities = manifestJSONBody("DataBroker", "get_capabilities", fixtures);
    const catalog = manifestJSONBody("DataBroker", "get_catalog_manifest", fixtures);
    const health = manifestJSONBody("DataBroker", "get_health_report", fixtures);
    const schemas = manifestJSONBody("DataBroker", "lookup_message_schema", fixtures);
    const dlq = manifestJSONBody("DataBroker", "get_dlq_event", fixtures);
    const dlqs = manifestJSONBody("DataBroker", "list_dlq_events", fixtures);
    const saga = manifestJSONBody("DataBroker", "get_saga", fixtures);
    const sagas = manifestJSONBody("DataBroker", "list_sagas", fixtures);
    const policies = manifestJSONBody("DataBroker", "list_policies", fixtures);
    const lint = manifestJSONBody("DataBroker", "lint_policies", fixtures);
    const admin = manifestJSONBody("DataBroker", "get_admin_summary", fixtures);
    const catalogVersion = manifestJSONBody("DataBroker", "get_catalog_version", fixtures);
    const catalogVersions = manifestJSONBody("DataBroker", "get_catalog_versions", fixtures);
    const cdc = manifestJSONBody("DataBroker", "get_cdc_status", fixtures);
    const migration = manifestJSONBody("DataBroker", "get_migration_status", fixtures);
    const migrationRuns = manifestJSONBody("DataBroker", "list_migration_runs", fixtures);
    const projects = manifestJSONBody("DataBroker", "list_projects", fixtures);
    const resources = manifestJSONBody("DataBroker", "list_resources", fixtures);
    const audit = manifestJSONBody("DataBroker", "list_admin_audit_logs", fixtures);
    const verify = manifestJSONBody("DataBroker", "verify_admin_audit_log", fixtures);
    const vector = manifestJSONBody("DataBroker", "vector_search", fixtures);
    const hybrid = manifestJSONBody("DataBroker", "vector_hybrid_search", fixtures);
    const cacheGet = manifestJSONBody("DataBroker", "cache_get", fixtures);
    const cacheScan = manifestJSONBody("DataBroker", "cache_scan", fixtures);
    const documentGet = manifestJSONBody("DataBroker", "document_get", fixtures);
    const documentFind = manifestJSONBody("DataBroker", "document_find", fixtures);
    const graph = manifestJSONBody("DataBroker", "graph_query", fixtures);
    const analytical = manifestJSONBody("DataBroker", "analytical_query", fixtures);
    const select = manifestJSONBody("DataBroker", "select", fixtures);
    const selectV2 = manifestJSONBody("DataBroker", "select_v_2", fixtures);
    const object = manifestJSONBody("DataBroker", "get_object", fixtures);
    const timeSeries = manifestJSONBody("DataBroker", "time_series_query", fixtures);
    const preview = manifestJSONBody("DataBroker", "preview_cdc_redaction", fixtures);
    const drift = manifestJSONBody("DataBroker", "scan_projection_drift", fixtures);
    node_assert_1.strict.equal(capabilities?.context?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(capabilities?.project_id, "project-1");
    node_assert_1.strict.equal(catalog?.redact, false);
    node_assert_1.strict.equal(health?.with_probes, false);
    node_assert_1.strict.equal(schemas?.message_type, "myapp.v1.Invoice");
    node_assert_1.strict.equal(dlq?.dlq_id, "dlq-1");
    node_assert_1.strict.equal(dlqs?.limit, 50);
    node_assert_1.strict.equal(saga?.saga_id, "saga-1");
    node_assert_1.strict.equal(sagas?.limit, 50);
    node_assert_1.strict.equal(policies?.include_disabled, false);
    node_assert_1.strict.equal(lint?.project_id, "project-1");
    node_assert_1.strict.deepEqual(admin?.context?.scopes, ["udb:admin"]);
    node_assert_1.strict.equal(admin?.with_probes, false);
    node_assert_1.strict.equal(catalogVersion?.version, "");
    node_assert_1.strict.equal(catalogVersions?.redact, false);
    node_assert_1.strict.equal(cdc?.slot_name, "udb_cdc");
    node_assert_1.strict.equal(migration?.run_id, "migration-1");
    node_assert_1.strict.equal(migrationRuns?.limit, 50);
    node_assert_1.strict.equal(projects?.limit, 50);
    node_assert_1.strict.equal(resources?.backend, "mongodb");
    node_assert_1.strict.equal(audit?.redact, false);
    node_assert_1.strict.equal(verify?.limit, 0);
    node_assert_1.strict.equal(vector?.collection, "sdk_live_records");
    node_assert_1.strict.deepEqual(vector?.vector, [0.1, 0.2, 0.3]);
    node_assert_1.strict.equal(hybrid?.text_query, "hello");
    node_assert_1.strict.equal(cacheGet?.resource?.backend, "redis");
    node_assert_1.strict.equal(cacheGet?.key, "cache-key-1");
    node_assert_1.strict.equal(cacheScan?.limit, 50);
    node_assert_1.strict.equal(documentGet?.resource?.resource_name, "invoices");
    node_assert_1.strict.equal(documentGet?.document_id, "document-1");
    node_assert_1.strict.deepEqual(documentFind?.filter, {});
    node_assert_1.strict.equal(graph?.read_only, true);
    node_assert_1.strict.equal(analytical?.query, "SELECT 1");
    node_assert_1.strict.equal(select?.filter?.record_id, "record-1");
    node_assert_1.strict.equal(selectV2?.limit, 10);
    node_assert_1.strict.equal(object?.bucket, "bucket-1");
    node_assert_1.strict.equal(timeSeries?.resource?.resource_name, "metrics_1");
    node_assert_1.strict.equal(preview?.payload_json, "e30=");
    node_assert_1.strict.equal(drift?.rows_per_target, 100);
});
(0, node_test_1.test)("manifest JSON body hydrates DataBroker CDC control mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    const pause = manifestJSONBody("DataBroker", "pause_cdc", fixtures);
    const resume = manifestJSONBody("DataBroker", "resume_cdc", fixtures);
    const stepDown = manifestJSONBody("DataBroker", "step_down_cdc_leader", fixtures);
    node_assert_1.strict.equal(pause?.slot_name, "udb_cdc");
    node_assert_1.strict.equal(pause?.reason, "maintenance");
    node_assert_1.strict.equal(resume?.reason, "resume");
    node_assert_1.strict.equal(stepDown?.reason, "failover");
});
(0, node_test_1.test)("manifest JSON body hydrates DataBroker unary mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("record_id", "record-1");
    fixtures.set("bucket", "bucket-1");
    fixtures.set("object_key", "object-1");
    fixtures.set("mongo_collection", "invoices");
    fixtures.set("document_id", "document-1");
    const url = manifestJSONBody("DataBroker", "generate_presigned_url", fixtures);
    const multipart = manifestJSONBody("DataBroker", "initiate_multipart_upload", fixtures);
    const doc = manifestJSONBody("DataBroker", "document_upsert", fixtures);
    const graph = manifestJSONBody("DataBroker", "graph_mutate", fixtures);
    const vector = manifestJSONBody("DataBroker", "vector_upsert", fixtures);
    const view = manifestJSONBody("DataBroker", "create_materialized_view", fixtures);
    const plan = manifestJSONBody("DataBroker", "plan_migration", fixtures);
    node_assert_1.strict.equal(url?.method, "GET");
    node_assert_1.strict.equal(url?.ttl_seconds, 300);
    node_assert_1.strict.equal(multipart?.part_count, 1);
    node_assert_1.strict.equal(doc?.document?.name, "x");
    node_assert_1.strict.equal(graph?.parameters?.id, "record-1");
    node_assert_1.strict.equal(vector?.points?.[0]?.id, "record-1");
    node_assert_1.strict.equal(view?.with_data, true);
    node_assert_1.strict.deepEqual(plan?.context?.scopes, ["udb:admin"]);
    node_assert_1.strict.equal(plan?.dry_run, true);
});
(0, node_test_1.test)("manifest JSON body hydrates DataBroker scalar action rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("object_key", "cache-key-1");
    fixtures.set("replay_dlq_id", "replay-dlq-1");
    fixtures.set("dismiss_dlq_id", "dismiss-dlq-1");
    fixtures.set("quarantine_dlq_id", "quarantine-dlq-1");
    fixtures.set("retry_saga_id", "retry-saga-1");
    fixtures.set("mark_saga_id", "mark-saga-1");
    fixtures.set("ds_policy_id", "42");
    const cacheDelete = manifestJSONBody("DataBroker", "cache_delete", fixtures);
    const replay = manifestJSONBody("DataBroker", "replay_dlq_event", fixtures);
    const dismiss = manifestJSONBody("DataBroker", "dismiss_dlq_event", fixtures);
    const quarantine = manifestJSONBody("DataBroker", "quarantine_dlq_event", fixtures);
    const retry = manifestJSONBody("DataBroker", "retry_saga_compensation", fixtures);
    const reviewed = manifestJSONBody("DataBroker", "mark_saga_reviewed", fixtures);
    const deletePolicy = manifestJSONBody("DataBroker", "delete_policy", fixtures);
    const reload = manifestJSONBody("DataBroker", "reload_policies", fixtures);
    node_assert_1.strict.equal(cacheDelete?.key, "cache-key-1");
    node_assert_1.strict.equal(replay?.dlq_id, "replay-dlq-1");
    node_assert_1.strict.equal(replay?.preserve_event_id, false);
    node_assert_1.strict.equal(dismiss?.dlq_id, "dismiss-dlq-1");
    node_assert_1.strict.equal(quarantine?.dlq_id, "quarantine-dlq-1");
    node_assert_1.strict.equal(retry?.reason, "retry");
    node_assert_1.strict.equal(reviewed?.reason, "reviewed");
    node_assert_1.strict.equal(deletePolicy?.policy_id, "42");
    node_assert_1.strict.equal(reload?.project_id, "project-1");
});
(0, node_test_1.test)("manifest JSON body hydrates DataBroker mutation/admin rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("message_type", "myapp.v1.Invoice");
    fixtures.set("record_id", "record-1");
    fixtures.set("object_key", "cache-key-1");
    fixtures.set("mongo_collection", "invoices");
    fixtures.set("document_id", "document-1");
    fixtures.set("apply_run_id", "apply-run-1");
    fixtures.set("approve_run_id", "approve-run-1");
    fixtures.set("approval_token", "approval-token-1");
    const apply = manifestJSONBody("DataBroker", "apply_migration", fixtures);
    const approve = manifestJSONBody("DataBroker", "approve_migration_plan", fixtures);
    const batchSelect = manifestJSONBody("DataBroker", "batch_select", fixtures);
    const batchUpsert = manifestJSONBody("DataBroker", "batch_upsert", fixtures);
    const cacheSet = manifestJSONBody("DataBroker", "cache_set", fixtures);
    const deleteReq = manifestJSONBody("DataBroker", "delete", fixtures);
    const documentDelete = manifestJSONBody("DataBroker", "document_delete", fixtures);
    const baseline = manifestJSONBody("DataBroker", "ensure_baseline", fixtures);
    const project = manifestJSONBody("DataBroker", "ensure_project", fixtures);
    const ensureResource = manifestJSONBody("DataBroker", "ensure_resource", fixtures);
    const dropResource = manifestJSONBody("DataBroker", "drop_resource", fixtures);
    const generic = manifestJSONBody("DataBroker", "generic_dispatch", fixtures);
    const publish = manifestJSONBody("DataBroker", "publish_cdc", fixtures);
    const upsert = manifestJSONBody("DataBroker", "upsert", fixtures);
    const vectorBatch = manifestJSONBody("DataBroker", "vector_batch_upsert", fixtures);
    node_assert_1.strict.equal(apply?.approval_token, "approval-token-1");
    node_assert_1.strict.equal(approve?.run_id, "approve-run-1");
    node_assert_1.strict.equal(batchSelect?.limit, 10);
    node_assert_1.strict.equal(batchUpsert?.return_record, true);
    node_assert_1.strict.equal(cacheSet?.value, "cGVyZg==");
    node_assert_1.strict.equal(deleteReq?.message_type, "myapp.v1.Invoice");
    node_assert_1.strict.equal(deleteReq?.filter?.record_id, "record-1");
    node_assert_1.strict.equal(documentDelete?.document_id, "document-1");
    node_assert_1.strict.deepEqual(baseline?.context?.scopes, ["udb:admin"]);
    node_assert_1.strict.equal(project?.cdc_topic_prefix, "project-1.");
    node_assert_1.strict.equal(ensureResource?.resource_name, "invoices");
    node_assert_1.strict.equal(dropResource?.spec_json, "{\"udb_allow_rls_bypass\":true}");
    node_assert_1.strict.equal(generic?.operation, "ping");
    node_assert_1.strict.equal(publish?.topic_pattern, "*");
    node_assert_1.strict.equal(upsert?.return_record, true);
    node_assert_1.strict.equal(vectorBatch?.points?.[0]?.id, "record-1");
});
(0, node_test_1.test)("manifest JSON body hydrates all remaining DataBroker rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("message_type", "myapp.v1.Invoice");
    fixtures.set("document_id", "document-1");
    fixtures.set("bucket", "bucket-1");
    fixtures.set("object_key", "object-1");
    fixtures.set("event_type", "invoice.updated");
    fixtures.set("ts_table", "metrics_1");
    fixtures.set("catalog_manifest_b64", "e30=");
    const activate = manifestJSONBody("DataBroker", "activate_catalog", fixtures);
    const begin = manifestJSONBody("DataBroker", "begin_tx", fixtures);
    const enqueue = manifestJSONBody("DataBroker", "enqueue_outbox_event", fixtures);
    const putObject = manifestJSONBody("DataBroker", "put_object", fixtures);
    const putPolicy = manifestJSONBody("DataBroker", "put_policy", fixtures);
    const rollback = manifestJSONBody("DataBroker", "rollback_catalog", fixtures);
    const stage = manifestJSONBody("DataBroker", "stage_catalog", fixtures);
    const timeSeries = manifestJSONBody("DataBroker", "time_series_write", fixtures);
    const validate = manifestJSONBody("DataBroker", "validate_catalog", fixtures);
    node_assert_1.strict.equal(activate?.project_id, "project-1");
    node_assert_1.strict.deepEqual(activate?.context?.scopes, ["udb:admin"]);
    node_assert_1.strict.equal(begin?.operation, "upsert");
    node_assert_1.strict.equal(begin?.payload?.lookup_key, "manifest-tx-lk");
    node_assert_1.strict.equal(enqueue?.payload?.event_type, "invoice.updated");
    node_assert_1.strict.equal(putObject?.data, "cGVyZg==");
    node_assert_1.strict.equal(putObject?.final_chunk, true);
    node_assert_1.strict.equal(putPolicy?.policy?.effect, "allow");
    node_assert_1.strict.equal(putPolicy?.policy?.enabled, true);
    node_assert_1.strict.equal(rollback?.project_id, "project-1");
    node_assert_1.strict.equal(stage?.manifest_json, "e30=");
    node_assert_1.strict.equal(stage?.reason, "stage");
    node_assert_1.strict.deepEqual(timeSeries?.points?.[0]?.timestamp, { seconds: 1767225600, nanos: 0 });
    node_assert_1.strict.equal(timeSeries?.points?.[0]?.values?.cpu, 0.5);
    node_assert_1.strict.equal(validate?.manifest_json, "e30=");
    node_assert_1.strict.equal(validate?.reason, "validate");
});
(0, node_test_1.test)("manifest JSON body hydrates StorageService read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("file_id", "file-1");
    fixtures.set("user_id", "user-1");
    const getFile = manifestJSONBody("StorageService", "get_file", fixtures);
    const download = manifestJSONBody("StorageService", "download_file", fixtures);
    const list = manifestJSONBody("StorageService", "list_files", fixtures);
    node_assert_1.strict.equal(getFile?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(getFile?.file_id, "file-1");
    node_assert_1.strict.equal(download?.chunk_size_bytes, 65536);
    node_assert_1.strict.equal(list?.reference_id, "");
    node_assert_1.strict.equal(list?.page_size, 20);
});
(0, node_test_1.test)("manifest JSON body hydrates ApiKeyService rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("key_id", "key-1");
    fixtures.set("plain_key", "plain-1");
    fixtures.set("owner_id", "owner-1");
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("update_key_id", "update-key-1");
    fixtures.set("revoke_key_id", "revoke-key-1");
    const created = manifestJSONBody("ApiKeyService", "create_api_key", fixtures);
    const getKey = manifestJSONBody("ApiKeyService", "get_api_key", fixtures);
    const usage = manifestJSONBody("ApiKeyService", "get_api_key_usage_stats", fixtures);
    const list = manifestJSONBody("ApiKeyService", "list_api_keys", fixtures);
    const updated = manifestJSONBody("ApiKeyService", "update_api_key", fixtures);
    const revoked = manifestJSONBody("ApiKeyService", "revoke_api_key", fixtures);
    const rotated = manifestJSONBody("ApiKeyService", "rotate_api_key", fixtures);
    const emergency = manifestJSONBody("ApiKeyService", "emergency_revoke_api_keys", fixtures);
    const validate = manifestJSONBody("ApiKeyService", "validate_api_key", fixtures);
    node_assert_1.strict.equal(created?.owner_id, "owner-1");
    node_assert_1.strict.deepEqual(created?.scopes, ["resource:read"]);
    node_assert_1.strict.equal(created?.context?.tenant?.project_id, "project-1");
    node_assert_1.strict.equal(created?.context?.user_id, "owner-1");
    node_assert_1.strict.equal(getKey?.key_id, "key-1");
    node_assert_1.strict.equal(usage?.key_id, "key-1");
    node_assert_1.strict.equal(list?.owner_id, "owner-1");
    node_assert_1.strict.equal(list?.owner_type, "API_KEY_OWNER_TYPE_SERVICE_ACCOUNT");
    node_assert_1.strict.deepEqual(list?.page, { page: 1, page_size: 50 });
    node_assert_1.strict.equal(updated?.key_id, "update-key-1");
    node_assert_1.strict.equal(updated?.name, "bench-key-2");
    node_assert_1.strict.deepEqual(updated?.ip_allowlist, []);
    node_assert_1.strict.equal(revoked?.key_id, "revoke-key-1");
    node_assert_1.strict.equal(revoked?.revoke_reason, "bench cleanup");
    node_assert_1.strict.equal(rotated?.key_id, "key-1");
    node_assert_1.strict.equal(rotated?.rotation_reason, "bench rotate");
    node_assert_1.strict.equal(emergency?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(emergency?.project_id, "project-1");
    node_assert_1.strict.equal(emergency?.scope, "resource:read");
    node_assert_1.strict.equal(validate?.plain_key, "plain-1");
    node_assert_1.strict.equal(validate?.required_scope, "resource:read");
});
(0, node_test_1.test)("manifest JSON body hydrates AuthnService read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("user_id", "user-1");
    fixtures.set("admin_reset_mfa_user_id", "admin-reset-mfa-user-1");
    fixtures.set("admin_reset_password_user_id", "admin-reset-password-user-1");
    fixtures.set("change_password_user_id", "change-password-user-1");
    fixtures.set("change_status_user_id", "change-status-user-1");
    fixtures.set("disable_mfa_user_id", "disable-mfa-user-1");
    fixtures.set("revoke_recovery_user_id", "revoke-recovery-user-1");
    fixtures.set("revoke_device_id", "revoke-device-1");
    fixtures.set("session_id", "session-1");
    fixtures.set("token", "token-1");
    fixtures.set("csrf_token", "csrf-1");
    fixtures.set("otp_id", "otp-1");
    fixtures.set("otp_code", "654321");
    fixtures.set("challenge_id", "challenge-1");
    const getUser = manifestJSONBody("AuthnService", "get_user", fixtures);
    const listSessions = manifestJSONBody("AuthnService", "list_sessions", fixtures);
    const validate = manifestJSONBody("AuthnService", "validate_token", fixtures);
    const authenticate = manifestJSONBody("AuthnService", "authenticate", fixtures);
    const csrf = manifestJSONBody("AuthnService", "validate_csrf", fixtures);
    const otp = manifestJSONBody("AuthnService", "verify_otp", fixtures);
    const mfa = manifestJSONBody("AuthnService", "verify_mfa_challenge", fixtures);
    node_assert_1.strict.equal(getUser?.user_id, "user-1");
    node_assert_1.strict.equal(listSessions?.user_id, "user-1");
    node_assert_1.strict.equal(listSessions?.active_only, true);
    node_assert_1.strict.deepEqual(listSessions?.page, { page: 1, page_size: 20 });
    node_assert_1.strict.equal(validate?.token, "token-1");
    node_assert_1.strict.equal(validate?.token_type, "TOKEN_TYPE_JWT_ACCESS");
    node_assert_1.strict.equal(authenticate?.bearer_token, "token-1");
    node_assert_1.strict.equal(authenticate?.credential_type, "AUTH_CREDENTIAL_TYPE_BEARER_TOKEN");
    node_assert_1.strict.equal(csrf?.csrf_token, "csrf-1");
    node_assert_1.strict.equal(otp?.otp_id, "otp-1");
    node_assert_1.strict.equal(otp?.code, "654321");
    node_assert_1.strict.equal(mfa?.challenge_id, "challenge-1");
});
(0, node_test_1.test)("manifest JSON body hydrates AuthnService session and MFA setup rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("username", "bench-user");
    fixtures.set("user_id", "user-1");
    fixtures.set("subject", "subject-1");
    fixtures.set("refresh_token", "refresh-1");
    fixtures.set("refresh_session_id", "refresh-session-1");
    fixtures.set("otp_id", "otp-1");
    const login = manifestJSONBody("AuthnService", "login", fixtures);
    const refreshedToken = manifestJSONBody("AuthnService", "refresh_token", fixtures);
    const refreshedSession = manifestJSONBody("AuthnService", "refresh_session", fixtures);
    const session = manifestJSONBody("AuthnService", "create_session", fixtures);
    const created = manifestJSONBody("AuthnService", "create_user", fixtures);
    const updated = manifestJSONBody("AuthnService", "update_user", fixtures);
    const sentOtp = manifestJSONBody("AuthnService", "send_otp", fixtures);
    const resentOtp = manifestJSONBody("AuthnService", "resend_otp", fixtures);
    const enrolled = manifestJSONBody("AuthnService", "enroll_mfa", fixtures);
    const recovery = manifestJSONBody("AuthnService", "generate_recovery_codes", fixtures);
    const policy = manifestJSONBody("AuthnService", "put_mfa_policy", fixtures);
    const forgot = manifestJSONBody("AuthnService", "forgot_password", fixtures);
    const phone = manifestJSONBody("AuthnService", "send_phone_verification", fixtures);
    const challenge = manifestJSONBody("AuthnService", "issue_mfa_challenge", fixtures);
    node_assert_1.strict.equal(login?.username, "bench-user");
    node_assert_1.strict.equal(login?.password, "CorrectHorse1!");
    node_assert_1.strict.equal(login?.device_type, "DEVICE_TYPE_API");
    node_assert_1.strict.equal(login?.project_hint, "project-1");
    node_assert_1.strict.equal(refreshedToken?.refresh_token, "refresh-1");
    node_assert_1.strict.equal(refreshedSession?.session_id, "refresh-session-1");
    node_assert_1.strict.equal(refreshedSession?.ttl_seconds, 3600);
    node_assert_1.strict.equal(session?.principal?.subject, "subject-1");
    node_assert_1.strict.equal(session?.principal?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(session?.ttl_seconds, 3600);
    node_assert_1.strict.equal(created?.username, "perf-u");
    node_assert_1.strict.equal(created?.account_kind, "ACCOUNT_KIND_PERSON");
    node_assert_1.strict.equal(updated?.full_name, "Perf U2");
    node_assert_1.strict.equal(updated?.email, "perf-u2@acme.test");
    node_assert_1.strict.equal(sentOtp?.otp_type, "OTP_TYPE_EMAIL_VERIFICATION");
    node_assert_1.strict.equal(resentOtp?.original_otp_id, "otp-1");
    node_assert_1.strict.equal(enrolled?.mfa_type, "AUTH_FACTOR_KIND_TOTP");
    node_assert_1.strict.equal(recovery?.count, 10);
    node_assert_1.strict.equal(policy?.require_mfa, false);
    node_assert_1.strict.equal(forgot?.identifier, "perf-u@acme.test");
    node_assert_1.strict.equal(phone?.phone, "+15551234567");
    node_assert_1.strict.equal(challenge?.purpose, "MFA_CHALLENGE_PURPOSE_SENSITIVE_OPERATION");
});
(0, node_test_1.test)("manifest JSON body hydrates AuthnService terminal and WebAuthn rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("user_id", "user-1");
    fixtures.set("admin_reset_mfa_user_id", "admin-reset-mfa-user-1");
    fixtures.set("admin_reset_password_user_id", "admin-reset-password-user-1");
    fixtures.set("change_password_user_id", "change-password-user-1");
    fixtures.set("change_status_user_id", "change-status-user-1");
    fixtures.set("disable_mfa_user_id", "disable-mfa-user-1");
    fixtures.set("revoke_recovery_user_id", "revoke-recovery-user-1");
    fixtures.set("revoke_device_id", "revoke-device-1");
    fixtures.set("session_id", "session-1");
    fixtures.set("subject", "subject-1");
    fixtures.set("code", "code-1");
    fixtures.set("reset_otp_id", "reset-otp-1");
    fixtures.set("reset_otp_code", "135790");
    fixtures.set("device_id", "device-1");
    fixtures.set("record_id", "credential-1");
    fixtures.set("reg_challenge_id", "reg-challenge-1");
    fixtures.set("auth_challenge_id", "auth-challenge-1");
    const logout = manifestJSONBody("AuthnService", "logout", fixtures);
    const revoked = manifestJSONBody("AuthnService", "revoke_session", fixtures);
    const adminRevoked = manifestJSONBody("AuthnService", "admin_revoke_session", fixtures);
    const adminAllUsers = manifestJSONBody("AuthnService", "admin_revoke_all_user_sessions", fixtures);
    const adminAllTenant = manifestJSONBody("AuthnService", "admin_revoke_all_tenant_sessions", fixtures);
    const emergency = manifestJSONBody("AuthnService", "emergency_revoke", fixtures);
    const changedPassword = manifestJSONBody("AuthnService", "change_password", fixtures);
    const resetPassword = manifestJSONBody("AuthnService", "reset_password", fixtures);
    const changedStatus = manifestJSONBody("AuthnService", "change_user_status", fixtures);
    const adminResetPassword = manifestJSONBody("AuthnService", "admin_reset_password", fixtures);
    const confirmedMfa = manifestJSONBody("AuthnService", "confirm_mfaenrollment", fixtures);
    const disabledMfa = manifestJSONBody("AuthnService", "disable_mfa_factor", fixtures);
    const renamed = manifestJSONBody("AuthnService", "rename_passkey", fixtures);
    const revokedRecovery = manifestJSONBody("AuthnService", "revoke_recovery_codes", fixtures);
    const adminResetMfa = manifestJSONBody("AuthnService", "admin_reset_mfa", fixtures);
    const revokedDevice = manifestJSONBody("AuthnService", "revoke_device", fixtures);
    const deletedWebAuthn = manifestJSONBody("AuthnService", "delete_web_authn_credential", fixtures);
    const startedReg = manifestJSONBody("AuthnService", "start_web_authn_registration", fixtures);
    const finishedReg = manifestJSONBody("AuthnService", "finish_web_authn_registration", fixtures);
    const startedAuth = manifestJSONBody("AuthnService", "start_web_authn_authentication", fixtures);
    const finishedAuth = manifestJSONBody("AuthnService", "finish_web_authn_authentication", fixtures);
    node_assert_1.strict.equal(logout?.session_id, "session-1");
    node_assert_1.strict.equal(revoked?.revoke_reason, "perf");
    node_assert_1.strict.equal(adminRevoked?.reason, "perf");
    node_assert_1.strict.equal(adminAllUsers?.user_id, "user-1");
    node_assert_1.strict.equal(adminAllTenant?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(emergency?.principal_id, "subject-1");
    node_assert_1.strict.equal(changedPassword?.user_id, "change-password-user-1");
    node_assert_1.strict.equal(changedPassword?.current_password, "CorrectHorse1!");
    node_assert_1.strict.equal(changedPassword?.otp_id, undefined);
    node_assert_1.strict.equal(resetPassword?.code, "135790");
    node_assert_1.strict.equal(changedStatus?.user_id, "change-status-user-1");
    node_assert_1.strict.equal(changedStatus?.new_status, "USER_STATUS_SUSPENDED");
    node_assert_1.strict.equal(adminResetPassword?.user_id, "admin-reset-password-user-1");
    node_assert_1.strict.equal(confirmedMfa?.otp_id, "code-1");
    node_assert_1.strict.equal(disabledMfa?.user_id, "disable-mfa-user-1");
    node_assert_1.strict.equal(disabledMfa?.factor_kind, "AUTH_FACTOR_KIND_TOTP");
    node_assert_1.strict.equal(renamed?.new_label, "perf-key2");
    node_assert_1.strict.equal(revokedRecovery?.user_id, "revoke-recovery-user-1");
    node_assert_1.strict.equal(adminResetMfa?.user_id, "admin-reset-mfa-user-1");
    node_assert_1.strict.equal(adminResetMfa?.reason, "perf");
    node_assert_1.strict.equal(revokedDevice?.device_id, "revoke-device-1");
    node_assert_1.strict.equal(deletedWebAuthn?.credential_id, "credential-1");
    node_assert_1.strict.equal(startedReg?.label, "perf-key");
    node_assert_1.strict.equal(finishedReg?.challenge_id, "reg-challenge-1");
    node_assert_1.strict.equal(finishedReg?.public_key_credential_json, "__UDB_WEBAUTHN_TEST__");
    node_assert_1.strict.equal(startedAuth?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(finishedAuth?.challenge_id, "auth-challenge-1");
    node_assert_1.strict.equal(finishedAuth?.public_key_credential_json, "__UDB_WEBAUTHN_TEST__");
});
(0, node_test_1.test)("manifest JSON body hydrates IdentityProviderService read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("provider_id", "provider-1");
    const getProvider = manifestJSONBody("IdentityProviderService", "get_provider", fixtures);
    const listProviders = manifestJSONBody("IdentityProviderService", "list_providers", fixtures);
    const claims = manifestJSONBody("IdentityProviderService", "preview_claim_mapping", fixtures);
    const groups = manifestJSONBody("IdentityProviderService", "preview_group_mapping", fixtures);
    node_assert_1.strict.equal(getProvider?.provider_id, "provider-1");
    node_assert_1.strict.equal(getProvider?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(listProviders?.kind, "IDP_KIND_UNSPECIFIED");
    node_assert_1.strict.deepEqual(listProviders?.page, { page: 1, page_size: 20 });
    node_assert_1.strict.equal(claims?.claims_json, "{\"sub\":\"abc\",\"email\":\"a@x.com\"}");
    node_assert_1.strict.deepEqual(groups?.groups, ["admins"]);
});
(0, node_test_1.test)("manifest JSON body hydrates AssetService rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("asset_id", "asset-1");
    fixtures.set("definition_id", "definition-1");
    fixtures.set("file_id", "file-1");
    fixtures.set("instance_id", "instance-1");
    fixtures.set("project", "project-1");
    fixtures.set("step_id", "step-1");
    const complete = manifestJSONBody("AssetService", "complete_step", fixtures);
    const created = manifestJSONBody("AssetService", "create_pipeline_definition", fixtures);
    const asset = manifestJSONBody("AssetService", "get_asset", fixtures);
    const definition = manifestJSONBody("AssetService", "get_pipeline_definition", fixtures);
    const pipeline = manifestJSONBody("AssetService", "get_pipeline", fixtures);
    const list = manifestJSONBody("AssetService", "list_assets", fixtures);
    const registered = manifestJSONBody("AssetService", "register_asset", fixtures);
    const started = manifestJSONBody("AssetService", "start_pipeline", fixtures);
    node_assert_1.strict.equal(complete?.step_id, "step-1");
    node_assert_1.strict.equal(complete?.status, "COMPLETED");
    node_assert_1.strict.equal(complete?.result, "{}");
    node_assert_1.strict.equal(created?.steps, "[{\"name\":\"resize\",\"type\":\"TRANSFORM\"}]");
    node_assert_1.strict.equal(created?.version, 1);
    node_assert_1.strict.equal(asset?.asset_id, "asset-1");
    node_assert_1.strict.equal(definition?.definition_id, "definition-1");
    node_assert_1.strict.equal(pipeline?.instance_id, "instance-1");
    node_assert_1.strict.equal(list?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(list?.media_type, "image/png");
    node_assert_1.strict.equal(list?.page_size, 20);
    node_assert_1.strict.equal(registered?.project_id, "");
    node_assert_1.strict.equal(registered?.file_id, "file-1");
    node_assert_1.strict.equal(registered?.metadata, "{\"source\":\"upload\"}");
    node_assert_1.strict.equal(started?.definition_id, "definition-1");
    node_assert_1.strict.equal(started?.asset_id, "asset-1");
    node_assert_1.strict.equal(started?.correlation_id, "run-001");
});
(0, node_test_1.test)("manifest JSON body hydrates WebRTC read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("room_id", "room-1");
    fixtures.set("peer_id", "peer-1");
    fixtures.set("track_id", "track-1");
    const room = manifestJSONBody("RoomService", "get_room", fixtures);
    const rooms = manifestJSONBody("RoomService", "list_rooms", fixtures);
    const egress = manifestJSONBody("RoomService", "list_egress", fixtures);
    const peer = manifestJSONBody("PeerService", "get_peer", fixtures);
    const peers = manifestJSONBody("PeerService", "list_peers", fixtures);
    const tracks = manifestJSONBody("TrackService", "list_tracks", fixtures);
    node_assert_1.strict.equal(room?.room_id, "room-1");
    node_assert_1.strict.equal(rooms?.state, "active");
    node_assert_1.strict.equal(rooms?.page_size, 20);
    node_assert_1.strict.equal(egress?.room_id, "room-1");
    node_assert_1.strict.equal(peer?.peer_id, "peer-1");
    node_assert_1.strict.equal(peers?.state, "connected");
    node_assert_1.strict.equal(tracks?.kind, "audio");
    node_assert_1.strict.equal(tracks?.page_size, 20);
});
(0, node_test_1.test)("manifest JSON body hydrates RoomService mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("room_id", "room-1");
    fixtures.set("close_room_id", "close-room-1");
    fixtures.set("track_id", "track-1");
    fixtures.set("object_key", "object-1");
    fixtures.set("egress_id", "eg-tenant-1-00000000-0000-4000-8000-000000000001");
    fixtures.set("user_id", "user-1");
    const created = manifestJSONBody("RoomService", "create_room", fixtures);
    const updated = manifestJSONBody("RoomService", "update_room", fixtures);
    const closed = manifestJSONBody("RoomService", "close_room", fixtures);
    const composite = manifestJSONBody("RoomService", "start_room_composite", fixtures);
    const trackEgress = manifestJSONBody("RoomService", "start_track_egress", fixtures);
    const stopped = manifestJSONBody("RoomService", "stop_egress", fixtures);
    node_assert_1.strict.equal(created?.created_by, "user-1");
    node_assert_1.strict.equal(created?.max_participants, 10);
    node_assert_1.strict.equal(created?.config, "{}");
    node_assert_1.strict.equal(updated?.name, "bench-room-2");
    node_assert_1.strict.equal(updated?.state, "active");
    node_assert_1.strict.equal(closed?.room_id, "close-room-1");
    node_assert_1.strict.equal(composite?.destination, "object-1");
    node_assert_1.strict.equal(composite?.options, "{}");
    node_assert_1.strict.equal(trackEgress?.track_id, "track-1");
    node_assert_1.strict.equal(trackEgress?.format, "mp4");
    node_assert_1.strict.equal(stopped?.egress_id, "eg-tenant-1-00000000-0000-4000-8000-000000000001");
});
(0, node_test_1.test)("manifest JSON body hydrates PeerService mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("room_id", "room-1");
    fixtures.set("join_session_room_id", "join-session-room-1");
    fixtures.set("leave_peer_id", "leave-peer-1");
    const joined = manifestJSONBody("PeerService", "join_room", fixtures);
    const session = manifestJSONBody("PeerService", "join_session", fixtures);
    const left = manifestJSONBody("PeerService", "leave_room", fixtures);
    node_assert_1.strict.equal(joined?.display_name, "Bench User");
    node_assert_1.strict.equal(joined?.metadata, "{}");
    node_assert_1.strict.equal(joined?.user_agent, "bench/1.0");
    node_assert_1.strict.equal(session?.room_id, "join-session-room-1");
    node_assert_1.strict.equal(session?.ttl_seconds, 3600);
    node_assert_1.strict.equal(left?.peer_id, "leave-peer-1");
});
(0, node_test_1.test)("manifest JSON body hydrates TrackService mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("room_id", "room-1");
    fixtures.set("peer_id", "peer-1");
    fixtures.set("track_id", "track-1");
    fixtures.set("unpublish_track_id", "track-disposable-1");
    const published = manifestJSONBody("TrackService", "publish_track", fixtures);
    const muted = manifestJSONBody("TrackService", "mute_track", fixtures);
    const unpublished = manifestJSONBody("TrackService", "unpublish_track", fixtures);
    node_assert_1.strict.equal(published?.kind, "audio");
    node_assert_1.strict.equal(published?.label, "mic");
    node_assert_1.strict.equal(published?.settings, "{}");
    node_assert_1.strict.equal(published?.metadata, "{}");
    node_assert_1.strict.equal(muted?.track_id, "track-1");
    node_assert_1.strict.equal(muted?.muted, true);
    node_assert_1.strict.equal(unpublished?.track_id, "track-disposable-1");
});
(0, node_test_1.test)("manifest JSON body hydrates NotificationService read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("user_id", "user-1");
    fixtures.set("event_type", "event-1");
    fixtures.set("log_id", "log-1");
    const stats = manifestJSONBody("NotificationService", "get_delivery_stats", fixtures);
    const notification = manifestJSONBody("NotificationService", "get_notification", fixtures);
    const preference = manifestJSONBody("NotificationService", "get_preference", fixtures);
    const template = manifestJSONBody("NotificationService", "get_template", fixtures);
    const notifications = manifestJSONBody("NotificationService", "list_notifications", fixtures);
    const preferences = manifestJSONBody("NotificationService", "list_preferences", fixtures);
    const templates = manifestJSONBody("NotificationService", "list_templates", fixtures);
    node_assert_1.strict.equal(stats?.event_type, "event-1");
    node_assert_1.strict.equal(stats?.date_to, "2026-12-31");
    node_assert_1.strict.equal(notification?.log_id, "log-1");
    node_assert_1.strict.equal(preference?.channel, "NOTIFICATION_CHANNEL_EMAIL");
    node_assert_1.strict.equal(template?.locale, "en");
    node_assert_1.strict.deepEqual(notifications?.page, { page: 1, page_size: 20 });
    node_assert_1.strict.equal(preferences?.user_id, "user-1");
    node_assert_1.strict.deepEqual(templates?.page, { page: 1, page_size: 20 });
});
(0, node_test_1.test)("manifest JSON body hydrates NotificationService mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("user_id", "user-1");
    fixtures.set("event_type", "event-1");
    fixtures.set("log_id", "log-1");
    const sent = manifestJSONBody("NotificationService", "send_notification", fixtures);
    const reported = manifestJSONBody("NotificationService", "report_delivery", fixtures);
    const retried = manifestJSONBody("NotificationService", "retry_notification", fixtures);
    const preference = manifestJSONBody("NotificationService", "set_preference", fixtures);
    const template = manifestJSONBody("NotificationService", "upsert_template", fixtures);
    node_assert_1.strict.equal(sent?.project_id, "project-1");
    node_assert_1.strict.deepEqual(sent?.variables, { name: "SDK" });
    node_assert_1.strict.deepEqual(sent?.channels, ["NOTIFICATION_CHANNEL_EMAIL"]);
    node_assert_1.strict.equal(sent?.context?.purpose, "go.live.perf");
    node_assert_1.strict.equal(reported?.provider, "sdk-perf");
    node_assert_1.strict.equal(reported?.status, "NOTIFICATION_STATUS_DELIVERED");
    node_assert_1.strict.equal(reported?.context?.tenant?.project_id, "project-1");
    node_assert_1.strict.equal(retried?.log_id, "log-1");
    node_assert_1.strict.equal(retried?.context?.purpose, "go.live.perf");
    node_assert_1.strict.equal(preference?.is_opted_out, true);
    node_assert_1.strict.equal(preference?.event_type, "");
    node_assert_1.strict.equal(template?.subject_template, "Hello {name}");
    node_assert_1.strict.equal(template?.body_template, "Body {name}");
    node_assert_1.strict.equal(template?.is_active, true);
});
(0, node_test_1.test)("manifest JSON body hydrates CacheService read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("object_key", "cache-key-1");
    const get = manifestJSONBody("CacheService", "get", fixtures);
    const stats = manifestJSONBody("CacheService", "get_namespace_stats", fixtures);
    const scan = manifestJSONBody("CacheService", "scan", fixtures);
    node_assert_1.strict.equal(get?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(get?.key, "cache-key-1");
    node_assert_1.strict.equal(stats?.namespace, "sdk-perf-cache");
    node_assert_1.strict.equal(scan?.limit, 50);
    node_assert_1.strict.equal(scan?.page_token, "0");
});
(0, node_test_1.test)("manifest JSON body hydrates CacheService mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("object_key", "cache-key-1");
    const created = manifestJSONBody("CacheService", "create_namespace", fixtures);
    const set = manifestJSONBody("CacheService", "set", fixtures);
    const deleted = manifestJSONBody("CacheService", "delete", fixtures);
    const dropped = manifestJSONBody("CacheService", "delete_namespace", fixtures);
    node_assert_1.strict.equal(created?.namespace, "sdk-perf-cache");
    node_assert_1.strict.equal(created?.max_bytes, 1048576);
    node_assert_1.strict.equal(created?.default_ttl_seconds, 300);
    node_assert_1.strict.equal(set?.key, "cache-key-1");
    node_assert_1.strict.equal(set?.value, "cGVyZg==");
    node_assert_1.strict.equal(set?.ttl_seconds, 300);
    node_assert_1.strict.equal(deleted?.key, "cache-key-1");
    node_assert_1.strict.equal(dropped?.confirmation_token, "sdk-perf-cache");
});
(0, node_test_1.test)("manifest JSON body hydrates MeteringService read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    const check = manifestJSONBody("MeteringService", "check_quota", fixtures);
    const quota = manifestJSONBody("MeteringService", "get_quota", fixtures);
    const list = manifestJSONBody("MeteringService", "list_quotas", fixtures);
    const usage = manifestJSONBody("MeteringService", "query_usage", fixtures);
    node_assert_1.strict.equal(check?.metric, "sdk.perf.request");
    node_assert_1.strict.equal(quota?.project_id, "project-1");
    node_assert_1.strict.equal(list?.limit, 50);
    node_assert_1.strict.equal(list?.page_size, 50);
    node_assert_1.strict.equal(usage?.window_seconds, 86400);
});
(0, node_test_1.test)("manifest JSON body hydrates MeteringService mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("user_id", "user-1");
    const quota = manifestJSONBody("MeteringService", "put_quota", fixtures);
    const usage = manifestJSONBody("MeteringService", "record_usage", fixtures);
    node_assert_1.strict.equal(quota?.limit_value, 1000000);
    node_assert_1.strict.equal(quota?.enabled, true);
    node_assert_1.strict.equal(quota?.metadata_json, "{}");
    node_assert_1.strict.equal(usage?.principal_id, "user-1");
    node_assert_1.strict.equal(usage?.quantity, 1);
    node_assert_1.strict.equal(usage?.unit, "request");
});
(0, node_test_1.test)("manifest JSON body hydrates LockService rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("user_id", "user-1");
    fixtures.set("fencing_token", "77");
    fixtures.set("release_fencing_token", "77");
    fixtures.set("renew_fencing_token", "77");
    const acquired = manifestJSONBody("LockService", "acquire_lock", fixtures);
    const renewed = manifestJSONBody("LockService", "renew_lock", fixtures);
    const released = manifestJSONBody("LockService", "release_lock", fixtures);
    node_assert_1.strict.equal(acquired?.lease_ttl_seconds, 60);
    node_assert_1.strict.equal(acquired?.metadata_json, "{}");
    node_assert_1.strict.equal(renewed?.fencing_token, 77);
    node_assert_1.strict.equal(released?.owner_id, "user-1");
    node_assert_1.strict.equal(released?.fencing_token, 77);
});
(0, node_test_1.test)("manifest JSON body hydrates SchedulerService read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("job_id", "job-1");
    const job = manifestJSONBody("SchedulerService", "get_job", fixtures);
    const jobs = manifestJSONBody("SchedulerService", "list_jobs", fixtures);
    node_assert_1.strict.equal(job?.job_id, "job-1");
    node_assert_1.strict.equal(jobs?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(jobs?.page_size, 20);
});
(0, node_test_1.test)("manifest JSON body hydrates SchedulerService mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("job_id", "job-1");
    const created = manifestJSONBody("SchedulerService", "create_job", fixtures);
    const paused = manifestJSONBody("SchedulerService", "pause_job", fixtures);
    const resumed = manifestJSONBody("SchedulerService", "resume_job", fixtures);
    const deleted = manifestJSONBody("SchedulerService", "delete_job", fixtures);
    node_assert_1.strict.equal(created?.project_id, "");
    node_assert_1.strict.equal(created?.name, "sdk-perf-job");
    node_assert_1.strict.equal(created?.schedule_type, "CRON");
    node_assert_1.strict.equal(created?.cron_expression, "*/5 * * * *");
    node_assert_1.strict.equal(created?.payload, "{}");
    node_assert_1.strict.equal(created?.target_topic, "sdk.perf.scheduler");
    node_assert_1.strict.equal(created?.max_attempts, 3);
    node_assert_1.strict.equal(created?.backoff_seconds, 30);
    node_assert_1.strict.equal(paused?.job_id, "job-1");
    node_assert_1.strict.equal(resumed?.job_id, "job-1");
    node_assert_1.strict.equal(deleted?.job_id, "job-1");
});
(0, node_test_1.test)("manifest JSON body hydrates WebhookService read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("endpoint_id", "endpoint-1");
    const endpoint = manifestJSONBody("WebhookService", "get_endpoint", fixtures);
    const deliveries = manifestJSONBody("WebhookService", "list_deliveries", fixtures);
    const endpoints = manifestJSONBody("WebhookService", "list_endpoints", fixtures);
    node_assert_1.strict.equal(endpoint?.endpoint_id, "endpoint-1");
    node_assert_1.strict.equal(deliveries?.page_size, 20);
    node_assert_1.strict.equal(endpoints?.active_only, true);
});
(0, node_test_1.test)("manifest JSON body hydrates WebhookService mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("endpoint_id", "endpoint-1");
    fixtures.set("delete_endpoint_id", "endpoint-delete-1");
    fixtures.set("topic_pattern", "tenant-1.*");
    const created = manifestJSONBody("WebhookService", "create_endpoint", fixtures);
    const updated = manifestJSONBody("WebhookService", "update_endpoint", fixtures);
    const deleted = manifestJSONBody("WebhookService", "delete_endpoint", fixtures);
    node_assert_1.strict.equal(created?.url, "https://example.com/udb-webhook");
    node_assert_1.strict.equal(created?.topic_pattern, "tenant-1.*");
    node_assert_1.strict.equal(created?.metadata_json, "{}");
    node_assert_1.strict.equal(created?.max_attempts, 3);
    node_assert_1.strict.equal(updated?.endpoint_id, "endpoint-1");
    node_assert_1.strict.equal(updated?.description, "sdk perf webhook updated");
    node_assert_1.strict.equal(updated?.active, true);
    node_assert_1.strict.equal(deleted?.endpoint_id, "endpoint-delete-1");
});
(0, node_test_1.test)("manifest JSON body hydrates BackupService rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("backup_id", "backup-1");
    fixtures.set("restore_tenant_id", "restore-tenant-1");
    const backup = manifestJSONBody("BackupService", "get_backup", fixtures);
    const policy = manifestJSONBody("BackupService", "get_backup_policy", fixtures);
    const policies = manifestJSONBody("BackupService", "list_backup_policies", fixtures);
    const backups = manifestJSONBody("BackupService", "list_backups", fixtures);
    const putPolicy = manifestJSONBody("BackupService", "put_backup_policy", fixtures);
    const started = manifestJSONBody("BackupService", "start_tenant_backup", fixtures);
    const restored = manifestJSONBody("BackupService", "restore_tenant", fixtures);
    const deleted = manifestJSONBody("BackupService", "delete_backup_policy", fixtures);
    node_assert_1.strict.equal(backup?.backup_id, "backup-1");
    node_assert_1.strict.equal(policy?.policy_name, "sdk-perf-default");
    node_assert_1.strict.equal(policies?.page_size, 20);
    node_assert_1.strict.equal(backups?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(putPolicy?.schedule_cron, "0 3 * * *");
    node_assert_1.strict.equal(putPolicy?.retention_days, 7);
    node_assert_1.strict.equal(putPolicy?.max_retained_backups, 3);
    node_assert_1.strict.equal(putPolicy?.enabled, true);
    node_assert_1.strict.equal(started?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(restored?.target_tenant_id, "restore-tenant-1");
    node_assert_1.strict.equal(restored?.confirmation_token, "yes");
    node_assert_1.strict.equal(restored?.allow_cross_tenant, true);
    node_assert_1.strict.equal(deleted?.policy_name, "sdk-perf-default");
});
(0, node_test_1.test)("manifest JSON body hydrates ConfigService read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    const evaluated = manifestJSONBody("ConfigService", "evaluate_flags", fixtures);
    const flag = manifestJSONBody("ConfigService", "get_flag", fixtures);
    const flags = manifestJSONBody("ConfigService", "list_flags", fixtures);
    node_assert_1.strict.deepEqual(evaluated?.keys, ["sdk.perf.enabled"]);
    node_assert_1.strict.equal(evaluated?.context?.project_id, "project-1");
    node_assert_1.strict.equal(flag?.flag_key, "sdk.perf.enabled");
    node_assert_1.strict.equal(flags?.limit, 50);
});
(0, node_test_1.test)("manifest JSON body hydrates ConfigService mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    const put = manifestJSONBody("ConfigService", "put_flag", fixtures);
    const del = manifestJSONBody("ConfigService", "delete_flag", fixtures);
    node_assert_1.strict.equal(put?.value?.bool_value, true);
    node_assert_1.strict.equal(put?.rollout_percentage, 100);
    node_assert_1.strict.equal(put?.metadata_json, "{}");
    node_assert_1.strict.equal(del?.flag_key, "sdk.perf.enabled");
    node_assert_1.strict.equal(del?.project_id, "project-1");
});
(0, node_test_1.test)("manifest JSON body hydrates WorkflowService read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("workflow_id", "workflow-1");
    const workflow = manifestJSONBody("WorkflowService", "get_workflow", fixtures);
    const workflows = manifestJSONBody("WorkflowService", "list_workflows", fixtures);
    node_assert_1.strict.equal(workflow?.workflow_id, "workflow-1");
    node_assert_1.strict.equal(workflows?.status, "RUNNING");
    node_assert_1.strict.equal(workflows?.page_size, 20);
});
(0, node_test_1.test)("manifest JSON body hydrates WorkflowService mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("record_id", "record-1");
    fixtures.set("workflow_id", "workflow-1");
    fixtures.set("cancel_workflow_id", "workflow-cancel-1");
    const started = manifestJSONBody("WorkflowService", "start_workflow", fixtures);
    const cancelled = manifestJSONBody("WorkflowService", "cancel_workflow", fixtures);
    const signalled = manifestJSONBody("WorkflowService", "signal_workflow", fixtures);
    node_assert_1.strict.equal(started?.project_id, "");
    node_assert_1.strict.equal(started?.workflow_type, "sdk.perf.workflow");
    node_assert_1.strict.equal(started?.total_steps, 20);
    node_assert_1.strict.equal(started?.payload, "{}");
    node_assert_1.strict.equal(started?.compensations, "[]");
    node_assert_1.strict.equal(started?.correlation_id, "record-1");
    node_assert_1.strict.equal(cancelled?.workflow_id, "workflow-cancel-1");
    node_assert_1.strict.equal(cancelled?.reason, "sdk perf cancel");
    node_assert_1.strict.equal(signalled?.workflow_id, "workflow-1");
    node_assert_1.strict.equal(signalled?.signal_name, "continue");
    node_assert_1.strict.equal(signalled?.signal_payload, "{\"ok\":true}");
});
(0, node_test_1.test)("manifest JSON body hydrates SearchService read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    const indexes = manifestJSONBody("SearchService", "list_indexes", fixtures);
    const search = manifestJSONBody("SearchService", "search", fixtures);
    node_assert_1.strict.equal(indexes?.page_size, 50);
    node_assert_1.strict.deepEqual(search?.query_vector, [0.1, 0.2, 0.3]);
    node_assert_1.strict.equal(search?.mode, "SEARCH_MODE_HYBRID");
    node_assert_1.strict.equal(search?.top_k, 5);
});
(0, node_test_1.test)("manifest JSON body hydrates SearchService mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("message_type", "myapp.v1.Invoice");
    const created = manifestJSONBody("SearchService", "create_index", fixtures);
    const reindex = manifestJSONBody("SearchService", "reindex", fixtures);
    const deleted = manifestJSONBody("SearchService", "delete_index", fixtures);
    node_assert_1.strict.equal(created?.source_message_type, "myapp.v1.Invoice");
    node_assert_1.strict.equal(created?.backend, "qdrant");
    node_assert_1.strict.equal(created?.vector_dims, 3);
    node_assert_1.strict.equal(created?.metadata_json, "{}");
    node_assert_1.strict.equal(reindex?.index_name, "sdk_live_records");
    node_assert_1.strict.equal(deleted?.index_name, "sdk_live_records");
});
(0, node_test_1.test)("manifest JSON body hydrates EmbeddingService read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    const sources = manifestJSONBody("EmbeddingService", "list_sources", fixtures);
    const retrieve = manifestJSONBody("EmbeddingService", "retrieve", fixtures);
    node_assert_1.strict.equal(sources?.page_size, 50);
    node_assert_1.strict.deepEqual(retrieve?.query_vector, [0.1, 0.2, 0.3]);
    node_assert_1.strict.equal(retrieve?.source_name, "sdk_live_records");
    node_assert_1.strict.equal(retrieve?.top_k, 5);
});
(0, node_test_1.test)("manifest JSON body hydrates EmbeddingService mutation rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("message_type", "myapp.v1.Invoice");
    fixtures.set("record_id", "record-1");
    const registered = manifestJSONBody("EmbeddingService", "register_source", fixtures);
    const reported = manifestJSONBody("EmbeddingService", "report_embedding", fixtures);
    const backfill = manifestJSONBody("EmbeddingService", "backfill", fixtures);
    const deleted = manifestJSONBody("EmbeddingService", "delete_source", fixtures);
    node_assert_1.strict.equal(registered?.source_message_type, "myapp.v1.Invoice");
    node_assert_1.strict.deepEqual(registered?.text_fields, ["payload"]);
    node_assert_1.strict.equal(registered?.metadata_json, "{}");
    node_assert_1.strict.equal(reported?.row_pk, "record-1");
    node_assert_1.strict.deepEqual(reported?.vector, [0.1, 0.2, 0.3]);
    node_assert_1.strict.equal(reported?.dims, 3);
    node_assert_1.strict.equal(backfill?.source_name, "sdk_live_records");
    node_assert_1.strict.equal(deleted?.source_name, "sdk_live_records");
});
(0, node_test_1.test)("manifest JSON body hydrates LiveQueryService subscribe row with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("message_type", "myapp.v1.Invoice");
    fixtures.set("record_id", "record-1");
    const subscribe = manifestJSONBody("LiveQueryService", "subscribe", fixtures);
    node_assert_1.strict.equal(subscribe?.message_type, "udb.core.lock.entity.v1.Lock");
    node_assert_1.strict.equal(subscribe?.project_id, undefined);
    node_assert_1.strict.equal(subscribe?.snapshot_limit, 10);
    node_assert_1.strict.deepEqual(subscribe?.filters, [
        { field: "lock_name", op: "LIVE_QUERY_COMPARISON_EQ", value: "sdk-perf-renew-lock" },
    ]);
});
(0, node_test_1.test)("manifest JSON body hydrates WebRTC turn and signaling rows", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("room_id", "room-1");
    fixtures.set("peer_id", "peer-1");
    fixtures.set("signal_peer_id", "signal-peer-1");
    const turn = manifestJSONBody("TurnService", "issue_credentials", fixtures);
    const signal = manifestJSONBody("SignalingService", "signal", fixtures);
    node_assert_1.strict.equal(turn?.ttl_seconds, 3600);
    node_assert_1.strict.equal(turn?.peer_id, "peer-1");
    node_assert_1.strict.equal(signal?.peer_id, "signal-peer-1");
    node_assert_1.strict.equal(signal?.ping, true);
});
(0, node_test_1.test)("manifest JSON body hydrates VaultService rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("vault_key_name", "sdk-perf-key");
    fixtures.set("vault_signing_key_name", "sdk-perf-signing-key");
    fixtures.set("vault_hmac_key_name", "sdk-perf-hmac-key");
    fixtures.set("vault_ciphertext", "udb-vault:v1:seed");
    fixtures.set("vault_secret_path", "app/config");
    fixtures.set("vault_signature", "udb-vault-sig:v1:seed");
    fixtures.set("vault_delete_secret_path", "app/delete");
    fixtures.set("vault_destroy_secret_path", "app/destroy");
    fixtures.set("vault_db_role", "sdk-readonly");
    fixtures.set("vault_create_key_name", "sdk-perf-create-key");
    fixtures.set("vault_put_secret_path", "app/put");
    const created = manifestJSONBody("VaultService", "create_transit_key", fixtures);
    const decrypt = manifestJSONBody("VaultService", "decrypt", fixtures);
    const deleted = manifestJSONBody("VaultService", "delete_secret", fixtures);
    const destroyed = manifestJSONBody("VaultService", "destroy_secret", fixtures);
    const encrypted = manifestJSONBody("VaultService", "encrypt", fixtures);
    const dbCreds = manifestJSONBody("VaultService", "generate_database_credentials", fixtures);
    const secret = manifestJSONBody("VaultService", "get_secret", fixtures);
    const hmac = manifestJSONBody("VaultService", "hmac", fixtures);
    const secrets = manifestJSONBody("VaultService", "list_secrets", fixtures);
    const put = manifestJSONBody("VaultService", "put_secret", fixtures);
    const rotated = manifestJSONBody("VaultService", "rotate_transit_key", fixtures);
    const seal = manifestJSONBody("VaultService", "seal_status", fixtures);
    const signed = manifestJSONBody("VaultService", "sign", fixtures);
    const verify = manifestJSONBody("VaultService", "verify", fixtures);
    node_assert_1.strict.equal(created?.algorithm, "aes256-gcm-siv");
    node_assert_1.strict.equal(decrypt?.ciphertext, "udb-vault:v1:seed");
    node_assert_1.strict.equal(deleted?.secret_path, "app/delete");
    // The irreversible crypto-shred is authorized only when confirmation_token EQUALS
    // secret_path; a fixed "destroy" literal is rejected INVALID_ARGUMENT.
    node_assert_1.strict.equal(destroyed?.confirmation_token, destroyed?.secret_path);
    node_assert_1.strict.equal(encrypted?.plaintext, "perf");
    node_assert_1.strict.equal(dbCreds?.role_name, "sdk-readonly");
    node_assert_1.strict.equal(dbCreds?.ttl_seconds, 900);
    node_assert_1.strict.equal(secret?.secret_path, "app/config");
    node_assert_1.strict.equal(hmac?.input, "perf");
    node_assert_1.strict.equal(hmac?.key_name, "sdk-perf-hmac-key");
    node_assert_1.strict.equal(secrets?.page_size, 50);
    node_assert_1.strict.equal(put?.secret_value, "perf-secret");
    node_assert_1.strict.equal(put?.expected_version, 0);
    node_assert_1.strict.equal(rotated?.key_name, "sdk-perf-key");
    node_assert_1.strict.equal(seal?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(signed?.input, "perf");
    node_assert_1.strict.equal(verify?.signature, "udb-vault-sig:v1:seed");
});
(0, node_test_1.test)("manifest JSON body hydrates ControlPlaneService rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("node_id", "node-1");
    fixtures.set("resource_name", "backend-target-1");
    fixtures.set("rollback_resource_version", "version-1");
    const ack = manifestJSONBody("ControlPlaneService", "ack_status", fixtures);
    const delta = manifestJSONBody("ControlPlaneService", "delta_resources", fixtures);
    const resources = manifestJSONBody("ControlPlaneService", "get_resources", fixtures);
    const nodes = manifestJSONBody("ControlPlaneService", "list_node_states", fixtures);
    const rollback = manifestJSONBody("ControlPlaneService", "rollback_resources", fixtures);
    const stream = manifestJSONBody("ControlPlaneService", "stream_resources", fixtures);
    node_assert_1.strict.equal(ack?.node_id, "node-1");
    node_assert_1.strict.equal(ack?.resource_type, "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION");
    node_assert_1.strict.equal(ack?.context?.tenant?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(delta?.node_id, "node-1");
    node_assert_1.strict.deepEqual(delta?.resource_names_subscribe, ["backend-target-1"]);
    node_assert_1.strict.deepEqual(delta?.initial_resource_versions, {});
    node_assert_1.strict.equal(resources?.resource_type, "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION");
    node_assert_1.strict.equal(resources?.context?.tenant?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(resources?.page?.page_size, 50);
    node_assert_1.strict.equal(nodes?.resource_type, "RESOURCE_TYPE_UNSPECIFIED");
    node_assert_1.strict.equal(nodes?.page?.page, 1);
    node_assert_1.strict.equal(rollback?.target_version, "version-1");
    node_assert_1.strict.equal(rollback?.context?.tenant?.project_id, "project-1");
    node_assert_1.strict.equal(stream?.node_id, "node-1");
    node_assert_1.strict.deepEqual(stream?.resource_names, []);
    node_assert_1.strict.equal(stream?.version_info, "");
});
(0, node_test_1.test)("manifest JSON body hydrates AuthzService core read-only rows with seed refs", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("user_id", "user-1");
    fixtures.set("object", "ledger");
    fixtures.set("action", "data.select");
    fixtures.set("subject", "subject-1");
    fixtures.set("policy_id", "policy-1");
    fixtures.set("resource", "invoice");
    fixtures.set("role_id", "role-1");
    fixtures.set("policy_draft_id", "draft-1");
    fixtures.set("canary_id", "canary-1");
    fixtures.set("gov_exp", "1893456000");
    const authorize = manifestJSONBody("AuthzService", "authorize", fixtures);
    const batch = manifestJSONBody("AuthzService", "batch_check_permissions", fixtures);
    const check = manifestJSONBody("AuthzService", "check_access", fixtures);
    const bundle = manifestJSONBody("AuthzService", "get_policy_bundle", fixtures);
    const nativeAccess = manifestJSONBody("AuthzService", "get_native_access", fixtures);
    const role = manifestJSONBody("AuthzService", "get_role", fixtures);
    const audits = manifestJSONBody("AuthzService", "list_access_decision_audits", fixtures);
    const lint = manifestJSONBody("AuthzService", "lint_authz_policies", fixtures);
    const roles = manifestJSONBody("AuthzService", "list_roles", fixtures);
    const rules = manifestJSONBody("AuthzService", "list_policy_rules", fixtures);
    const diff = manifestJSONBody("AuthzService", "diff_policy_draft", fixtures);
    const explain = manifestJSONBody("AuthzService", "explain_policy", fixtures);
    const canary = manifestJSONBody("AuthzService", "get_canary_status", fixtures);
    const versions = manifestJSONBody("AuthzService", "list_policy_versions", fixtures);
    node_assert_1.strict.equal(authorize?.principal?.user_id, "user-1");
    node_assert_1.strict.equal(authorize?.resource?.table, "sdk_live_records");
    node_assert_1.strict.deepEqual(authorize?.requested_scopes, ["udb:read"]);
    node_assert_1.strict.equal(batch?.checks?.[0]?.action, "data.select");
    node_assert_1.strict.equal(check?.object, "ledger");
    node_assert_1.strict.equal(bundle?.project_id, "project-1");
    node_assert_1.strict.equal(nativeAccess?.backend, "postgres");
    node_assert_1.strict.equal(role?.role_id, "role-1");
    node_assert_1.strict.equal(audits?.page?.page_size, 50);
    node_assert_1.strict.deepEqual(lint, {});
    node_assert_1.strict.equal(roles?.page?.page_size, 50);
    node_assert_1.strict.equal(rules?.active_only, true);
    node_assert_1.strict.equal(diff?.actor?.break_glass_expires_at_unix, "1893456000");
    node_assert_1.strict.equal(explain?.test_case?.resource?.resource_type, "invoice");
    node_assert_1.strict.equal(canary?.canary_id, "canary-1");
    node_assert_1.strict.equal(versions?.state, "POLICY_VERSION_STATE_APPROVED");
});
(0, node_test_1.test)("manifest JSON body hydrates AuthzService create-policy-draft row", () => {
    const fixtures = new PerfFixtures();
    fixtures.set("tenant_id", "tenant-1");
    fixtures.set("project", "project-1");
    fixtures.set("subject", "subject-1");
    fixtures.set("gov_exp", "1893456000");
    const draft = manifestJSONBody("AuthzService", "create_policy_draft", fixtures);
    node_assert_1.strict.equal(draft?.tenant_id, "tenant-1");
    node_assert_1.strict.equal(draft?.project_id, "project-1");
    node_assert_1.strict.equal(draft?.policy_set_name, "default");
    node_assert_1.strict.equal(draft?.title, "draft 1");
    node_assert_1.strict.equal(draft?.change_reason, "init");
    node_assert_1.strict.equal(draft?.actor?.subject, "subject-1");
    node_assert_1.strict.deepEqual(draft?.actor?.scopes, ["authz:policy:write"]);
    node_assert_1.strict.equal(draft?.actor?.break_glass, true);
    node_assert_1.strict.deepEqual(draft?.document, {});
});
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
// perfRealBody returns the shared strict-JSON bench-manifest request for every
// RPC the bench measures. The manifest is now the single request-body source; a
// missing or unhydratable body is a hard gap/bypass failure, never a typed switch
// or generic placeholder fallback.
function perfRealBody(serviceName, methodName, _tenantId, _projectId, fixtures) {
    const body = manifestJSONBody(serviceName, methodName, fixtures);
    return uniquifyPerfBody(serviceName, methodName, body);
}
// ── Perf SEED phase + fixture map (mirrors the Go harness) ─────────────────────
//
// The perf run measures REAL successful-call latency for the whole RPC surface. To
// do that, every reference/ID field in a request must point at an entity that
// actually exists. seedPerfFixtures creates those entities up front — REUSING the
// same create flows the conformance suite (runLiveNativeServiceE2E above) already
// proves succeed — and records their real identifiers into a PerfFixtures map keyed
// by SEMANTIC field name (user_id, role, policy_id, file_id, room_id, subject, …).
// manifest-only perfRealBody resolves each request's reference/ID fields against this map, so a
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
    const fix = fullSurfaceManifestFixtures();
    const suffix = `${process.pid}${Date.now()}`;
    const opts = { deadlineMs: 8_000, noRetry: true };
    const ctx = requestContext(tenantId, projectId, "ts.live.perf.seed");
    const pw = "CorrectHorse1!";
    fix.set("egress_id", `eg-${tenantId}-${liveUuid()}`);
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
    fix.set("tenant_code", `sdk-perf-tenant-${suffix}`);
    fix.set("purge_tenant_id", tenantId);
    // AdminPurgeTenant is a PRIVILEGED cross-tenant purge; the tenant-status gate
    // (live since 0.4.32) suspends the PURGED tenant, so pointing it at the caller's
    // own tenant self-suspends the benchmark tenant mid-run and denies every later
    // RPC. Target a SEPARATE disposable tenant so only the terminal self-PurgeTenant
    // suspends the caller, at the very end. Fall back to a non-existent UUID
    // (isolated NotFound, never a cascade) if creation fails.
    fix.set("admin_purge_tenant_id", liveUuid());
    await tryRun("disposable admin-purge tenant", async () => {
        const dispTenant = await gen.TenantService.create_tenant({ code: `sdkperfadminpurge${suffix}`, name: "SDK Perf Admin-Purge Disposable", type: "organization", config: "{}", branding: "{}" }, opts);
        if (dispTenant.tenant_id)
            fix.set("admin_purge_tenant_id", dispTenant.tenant_id);
    });
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
    // backend-specific DataBroker RPCs are driven by explicit manifest bodies, so we
    // deliberately do NOT register a global backend/resource_name fixture.
    fix.set("collection", collection);
    fix.set("mongo_collection", collection);
    // ── AuthnService: a real user (id reused everywhere a user_id is needed) ───────
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
            const so = await gen.AuthnService.send_otp({ user_id: ou.user_id, otp_type: "OTP_TYPE_SENSITIVE_OPERATION", context: { tenant: { tenant_id: tenantId, project_id: projectId } } }, opts);
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
            const rso = await gen.AuthnService.send_otp({ user_id: ru.user_id, otp_type: "OTP_TYPE_PASSWORD_RESET", context: { tenant: { tenant_id: tenantId, project_id: projectId } } }, opts);
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
        const seedAuthnUser = async (key, label) => {
            const username = `sdk-perf-${label}-${suffix}`;
            const disposable = (await gen.AuthnService.create_user({
                username,
                email: `${username}@example.com`,
                password: pw,
                tenant_id: tenantId,
                project_id: projectId,
                full_name: `SDK Perf ${label} User`,
            }, opts)).user;
            const disposableId = disposable?.user_id;
            if (!disposableId)
                throw new Error(`CreateUser(${key}) did not return user_id`);
            fix.set(key, disposableId);
            await tryRun(`Activate ${key}`, async () => {
                await gen.AuthnService.change_user_status({
                    user_id: disposableId,
                    new_status: "USER_STATUS_ACTIVE",
                    reason: `perf seed activate ${label}`,
                    context: { tenant: { tenant_id: tenantId, project_id: projectId } },
                }, opts);
            });
            return disposableId;
        };
        await tryRun("Authn disposable users", async () => {
            await seedAuthnUser("admin_reset_mfa_user_id", "admin-reset-mfa");
            await seedAuthnUser("admin_reset_password_user_id", "admin-reset-password");
            await seedAuthnUser("change_password_user_id", "change-password");
            await seedAuthnUser("change_status_user_id", "change-status");
            await seedAuthnUser("disable_mfa_user_id", "disable-mfa");
            const recoveryUserId = await seedAuthnUser("revoke_recovery_user_id", "revoke-recovery");
            await tryRun("GenerateRecoveryCodes revoke_recovery_user_id", async () => {
                await gen.AuthnService.generate_recovery_codes({ user_id: recoveryUserId, count: 4 }, opts);
            });
        });
        await tryRun("RevokeDevice disposable user", async () => {
            const revokeUserId = await seedAuthnUser("revoke_device_user_id", "revoke-device");
            const revokeUsername = `sdk-perf-revoke-device-${suffix}`;
            await gen.AuthnService.login({
                username: revokeUsername,
                password: pw,
                tenant_hint: tenantId,
                project_hint: projectId,
                device_id: `ts-perf-revoke-fp-${suffix}`,
                device_name: "ts-perf-revoke-device",
                ip_address: "127.0.0.1",
            }, opts);
            const dl = await gen.AuthnService.list_devices({ user_id: revokeUserId }, opts);
            if ((dl.devices ?? []).length > 0)
                fix.set("revoke_device_id", dl.devices[0].device_id);
            else
                fix.set("revoke_device_id", `ts-perf-revoke-fp-${suffix}`);
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
        await tryRun("FreshLoginSession", async () => { const l = await gen.AuthnService.login({ username: adminU, password: adminP, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-perf-session" }, opts); if (l.session_id) {
            fix.set("session_id", l.session_id);
            fix.set("refresh_session_id", l.session_id);
        } });
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
    // Canonical-identity model: the key owner must be an EXISTING ACTIVE
    // SERVICE_ACCOUNT with an active typed grant, addressed by its UUID — a
    // bare service NAME is not a user_id and never was one.
    const svcName = `sdk-perf-svc-${suffix}`;
    let svcOwner = "";
    await tryRun("SeedServiceAccount", async () => {
        const svcUser = (await gen.AuthnService.create_user({
            username: svcName, email: `${svcName}@example.com`, password: pw,
            tenant_id: tenantId, project_id: projectId, full_name: "SDK Perf Service Account",
            account_kind: "ACCOUNT_KIND_SERVICE_ACCOUNT",
        }, opts)).user;
        svcOwner = svcUser.user_id;
        // CreateUser persists PENDING_VERIFICATION; the typed grant and
        // CreateApiKey both require an ACTIVE service account.
        await gen.AuthnService.change_user_status({
            user_id: svcOwner, new_status: "USER_STATUS_ACTIVE", reason: "perf seed activate",
            context: { tenant: { tenant_id: tenantId, project_id: projectId } },
        }, opts);
        await gen.AuthnService.create_service_account_grant({
            tenant_id: tenantId, user_id: svcOwner, service_identity: svcName,
            project_id: projectId, approved_scopes: ["data:read", "resource:read"], reason: "sdk perf seed",
        }, opts);
        // The measured RevokeCertificateBinding revokes THIS seeded binding.
        const binding = await gen.AuthnService.create_certificate_binding({
            tenant_id: tenantId, user_id: svcOwner, selector_kind: "SPIFFE_URI",
            selector_value: `spiffe://bench/seed-binding-${suffix}`, reason: "perf seed binding",
        }, opts);
        if (binding?.binding?.binding_id)
            fix.set("grant_binding_id", binding.binding.binding_id);
    });
    if (!svcOwner)
        svcOwner = svcName; // fall back; CreateApiKey fails typed, not INTERNAL
    // A SECOND ACTIVE service account WITHOUT a grant: the measured
    // CreateServiceAccountGrant makes its revision-1 grant here, and the
    // destructive-phase RotateServiceAccountIdentity rotates that same grant.
    const svcBName = `sdk-perf-svc-b-${suffix}`;
    await tryRun("SeedServiceAccountB", async () => {
        const svcB = (await gen.AuthnService.create_user({
            username: svcBName, email: `${svcBName}@example.com`, password: pw,
            tenant_id: tenantId, project_id: projectId, full_name: "SDK Perf Service Account B",
            account_kind: "ACCOUNT_KIND_SERVICE_ACCOUNT",
        }, opts)).user;
        await gen.AuthnService.change_user_status({
            user_id: svcB.user_id, new_status: "USER_STATUS_ACTIVE", reason: "perf seed activate",
            context: { tenant: { tenant_id: tenantId, project_id: projectId } },
        }, opts);
        fix.set("grant_create_user_id", svcB.user_id);
    });
    // A THIRD ACTIVE service account, also grantless, reserved for the measured
    // TransferServiceAccountGrant: the transfer moves svcOwner's ACTIVE grant onto a
    // grantless ACTIVE SERVICE ACCOUNT. Service-account-B cannot serve here — the
    // measured CreateServiceAccountGrant gives B a grant, and the handler refuses a
    // target that already holds one. Without its own fixture the key suffix-matches a
    // HUMAN user_id and the transfer is rejected "grants may only target service accounts".
    await tryRun("CreateServiceAccountC", async () => {
        const svcCName = `sdk-perf-svc-c-${suffix}`;
        const svcC = (await gen.AuthnService.create_user({
            username: svcCName, email: `${svcCName}@example.com`, password: pw,
            tenant_id: tenantId, project_id: projectId, full_name: "SDK Perf Service Account C",
            account_kind: "ACCOUNT_KIND_SERVICE_ACCOUNT",
        }, opts)).user;
        await gen.AuthnService.change_user_status({
            user_id: svcC.user_id, new_status: "USER_STATUS_ACTIVE", reason: "perf seed activate",
            context: { tenant: { tenant_id: tenantId, project_id: projectId } },
        }, opts);
        fix.set("grant_transfer_to_user_id", svcC.user_id);
    });
    // A FOURTH service account that OWNS a fresh grant, used only as the transfer's
    // SOURCE. svcOwner cannot serve: its grant backs the measured api-key RPCs and its
    // revision moves, so the transfer's `expected_revision: 1` CAS fails "source grant is
    // inactive, missing, or its revision changed". Nothing else touches this grant.
    await tryRun("CreateServiceAccountD", async () => {
        const svcDName = `sdk-perf-svc-d-${suffix}`;
        const svcD = (await gen.AuthnService.create_user({
            username: svcDName, email: `${svcDName}@example.com`, password: pw,
            tenant_id: tenantId, project_id: projectId, full_name: "SDK Perf Service Account D",
            account_kind: "ACCOUNT_KIND_SERVICE_ACCOUNT",
        }, opts)).user;
        await gen.AuthnService.change_user_status({
            user_id: svcD.user_id, new_status: "USER_STATUS_ACTIVE", reason: "perf seed activate",
            context: { tenant: { tenant_id: tenantId, project_id: projectId } },
        }, opts);
        await gen.AuthnService.create_service_account_grant({
            tenant_id: tenantId, user_id: svcD.user_id, service_identity: svcDName,
            project_id: projectId, approved_scopes: ["data:read"], reason: "sdk perf transfer source",
        }, opts);
        fix.set("grant_transfer_from_user_id", svcD.user_id);
    });
    await tryRun("CreateApiKey", async () => {
        const key = await gen.ApiKeyService.create_api_key({ name: `sdk-perf-key-${suffix}`, owner_id: svcOwner, scopes: ["data:read"], context: { user_id: svcOwner, tenant: { tenant_id: tenantId, project_id: projectId } } }, opts);
        fix.set("key_id", key.key.key_id);
        // revoke/rotate/update look up by key_PREFIX (get_by_prefix), not the key_id UUID.
        // Derive it from plain_key ("udbk_xxxx.yyyy" → "udbk_xxxx") — robust vs an unset field.
        fix.set("key_prefix", (key.key.key_prefix || String(key.plain_key).split(".")[0]));
        fix.set("plain_key", key.plain_key);
        fix.set("owner_id", svcOwner);
    });
    // A SEPARATE disposable key for the destructive RevokeApiKey → real 200, so the
    // primary key_id survives for RotateApiKey/UpdateApiKey/GetApiKey/ValidateApiKey.
    await tryRun("CreateRevokeKey", async () => {
        const rk = await gen.ApiKeyService.create_api_key({ name: `sdk-perf-revoke-${suffix}`, owner_id: svcOwner, scopes: ["data:read"], context: { user_id: svcOwner, tenant: { tenant_id: tenantId, project_id: projectId } } }, opts);
        fix.set("revoke_key_id", rk.key.key_id);
        fix.set("revoke_key_prefix", (rk.key.key_prefix || String(rk.plain_key).split(".")[0]));
    });
    // A SEPARATE disposable key for UpdateApiKey, so the measured RotateApiKey (which
    // rotates the primary key_id) can't invalidate the key UpdateApiKey targets.
    await tryRun("CreateUpdateKey", async () => {
        const uk = await gen.ApiKeyService.create_api_key({ name: `sdk-perf-update-${suffix}`, owner_id: svcOwner, scopes: ["data:read"], context: { user_id: svcOwner, tenant: { tenant_id: tenantId, project_id: projectId } } }, opts);
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
    // ── Saga + DLQ rows: create recovery state through the served, admin-gated
    // EnsureBaseline RPC instead of raw udb_system inserts. Each mutating RPC gets
    // its own disposable row because the op transitions status.
    for (const [sagaKey, dlqKey] of [
        ["saga_id", "dlq_id"],
        ["retry_saga_id", "dismiss_dlq_id"],
        ["mark_saga_id", "quarantine_dlq_id"],
        ["", "replay_dlq_id"],
    ]) {
        await tryRun(`EnsureBaseline:${dlqKey}`, async () => {
            const baseline = await data.ensure_baseline({ context: ctx }, opts);
            if (sagaKey && (baseline.saga_ids ?? []).length > 0)
                fix.set(sagaKey, baseline.saga_ids[0]);
            if ((baseline.dlq_ids ?? []).length > 0)
                fix.set(dlqKey, baseline.dlq_ids[0]);
        });
    }
    // ── AuthzService governance lifecycle (ports the Go seed): drafts in each state,
    // approved policy VERSIONS, a canary, and a rollback set — so the draft/version/
    // canary RPCs run their real success path. ──────────────────────────────────────
    {
        const subject = fix.lookup("subject") ?? `user:${fix.lookup("user_id") ?? liveUuid()}`;
        // Body actor.scopes are ignored by the live D1/D2 gate (it reads claim scopes,
        // and no role projects to authz:*), so the seed's own governance writes (incl.
        // the first CreatePolicyDraft that stores policy_draft_id) must use the
        // body-authoritative break-glass bypass — otherwise the drafts/versions/canary
        // are never created and the governance RPCs that read them fail "<id> is
        // required". break_glass_expires_at_unix is int64-as-number (epoch seconds).
        const gActor = () => ({ subject, tenant_id: tenantId, project_id: projectId, break_glass: true, break_glass_reason: "sdk perf seed", break_glass_expires_at_unix: Math.floor(Date.now() / 1000) + 900 });
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
        if (cm?.manifest_json) {
            seedCatalogManifest = cm.manifest_json;
            const bytes = Buffer.isBuffer(cm.manifest_json) ? cm.manifest_json : Buffer.from(cm.manifest_json);
            fix.set("catalog_manifest_b64", bytes.toString("base64"));
        }
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
        // No "{{n}}" placeholder: the seed SendNotification below passes no variables,
        // so a placeholder subject would fail to render → no log_id → Get/Retry/Send
        // notification RPCs fail. Plain "SDK perf" renders with zero variables.
        await gen.NotificationService.upsert_template({ event_type: event, channel: 1, locale: "en", subject_template: "SDK perf", body_template: "sdk-perf-body", is_active: true }, opts);
    });
    fix.set("event_type", event);
    fix.set("locale", "en");
    // Governance break-glass expiry: the D1/D2 governance gate reads scopes from the
    // VERIFIED claim, not request-body actor.scopes, and no role projects to authz:*
    // — so the governance RPCs are reached via the body-authoritative break-glass
    // bypass (≤900s, reason-bearing, audited). Set at seed time; the governance RPCs
    // measure shortly after. int64-as-number epoch seconds.
    fix.set("gov_exp", String(Math.floor(Date.now() / 1000) + 900));
    const recipientId = fix.lookup("recipient_id");
    if (recipientId) {
        await tryRun("SendNotification", async () => {
            const sent = await gen.NotificationService.send_notification({ event_type: event, recipient_id: recipientId, recipient_address: `sdk+${suffix}@example.com`, tenant_id: tenantId, resource_type: "__perf_force_failed__", channels: [1], variables: { n: "1" } }, opts);
            if ((sent.logs ?? []).length > 0) {
                const logId = sent.logs[0].log_id;
                fix.set("log_id", logId);
                fix.set("notification_id", logId);
                // UDB_NOTIFICATION_TEST_MODE + ResourceType sentinel makes this served
                // send produce a real FAILED row for RetryNotification.
            }
        });
    }
    // ── CacheService: namespace + key for read/mutation rows ─────────────────────
    await tryRun("CreateCacheNamespace", async () => {
        await gen.CacheService.create_cache_namespace({ tenant_id: tenantId, namespace: "sdk-perf-cache", max_bytes: 1048576, default_ttl_seconds: 300 }, opts);
    });
    await tryRun("SeedCacheKey", async () => {
        await gen.CacheService.cache_set({ tenant_id: tenantId, namespace: "sdk-perf-cache", key: objectKey, value: Buffer.from("perf", "utf8"), ttl_seconds: 300 }, opts);
    });
    // ── EmbeddingService: model registry, durable jobs, and searchable vector ─────
    const registerEmbeddingModel = async (modelId, collection, alias) => {
        await gen.EmbeddingService.register_model({
            tenant_id: tenantId, model_id: modelId, provider: "deterministic",
            model_name: "text-embedding-3-small", version: "1", dimensions: 3,
            matryoshka_dims: [3], distance_metric: "COSINE", normalize: true,
            output_dtype: "FLOAT32", max_input_tokens: 8192, tokenizer: "cl100k_base",
            task_type: "DOCUMENT", provider_endpoint_ref: "vault://embedding/sdk-live",
            vector_backend: "qdrant", vector_instance: "default", collection_alias: alias,
            active_collection: collection, chunking_strategy: "TOKEN_RECURSIVE",
            chunk_tokens: 256, chunk_overlap_tokens: 32,
            metadata_json: JSON.stringify({ suite: "sdk-live" }),
        }, opts);
    };
    await tryRun("RegisterEmbeddingModel", async () => {
        await registerEmbeddingModel("text-embedding-3-small", "sdk_live_records", "sdk_live_records_alias");
    });
    const embeddingDeleteModelId = `sdk-live-delete-model-${suffix}`;
    fix.set("embedding_delete_model_id", embeddingDeleteModelId);
    await tryRun("RegisterEmbeddingDeleteModel", async () => {
        await registerEmbeddingModel(embeddingDeleteModelId, `sdk_live_delete_records_${suffix}`, `sdk_live_delete_records_alias_${suffix}`);
    });
    await tryRun("RegisterEmbeddingSource", async () => {
        await gen.EmbeddingService.register_source({ tenant_id: tenantId, source_name: "sdk_live_records", source_message_type: LIVE_MESSAGE_TYPE, text_fields: ["payload"], target_collection: "sdk_live_records", model_id: "text-embedding-3-small", metadata_json: "{}" }, opts);
    });
    await tryRun("ReportEmbedding", async () => {
        await gen.EmbeddingService.report_embedding({ tenant_id: tenantId, source_name: "sdk_live_records", row_pk: recordId, vector: [0.1, 0.2, 0.3], model: "text-embedding-3-small", dims: 3 }, opts);
    });
    await tryRun("IngestEmbeddingWorkFixture", async () => {
        const document = await gen.EmbeddingService.ingest_document({
            tenant_id: tenantId, external_id: `sdk-live-work-${suffix}`,
            title: "SDK benchmark work fixture",
            raw_text: "Durable embedding work is seeded from real document text for the SDK benchmark.",
            content_type: "text/plain", doc_version: "1", model_id: "text-embedding-3-small",
            metadata_json: JSON.stringify({ suite: "sdk-live", fixture: "work" }),
        }, opts);
        if (document.job_id) {
            fix.set("embedding_job_id", document.job_id);
            const work = await gen.EmbeddingService.list_work_items({ tenant_id: tenantId, job_id: document.job_id, page_size: 50 }, opts);
            if ((work.work_items ?? []).length > 0)
                fix.set("embedding_work_item_id", work.work_items[0].work_item_id);
        }
    });
    await tryRun("IngestEmbeddingParserFixture", async () => {
        const document = await gen.EmbeddingService.ingest_document({
            tenant_id: tenantId, external_id: `sdk-live-parser-${suffix}`,
            title: "SDK benchmark parser fixture", storage_object_ref: `udb://sdk-live/embedding-${suffix}.txt`,
            content_type: "text/plain", doc_version: "1", model_id: "text-embedding-3-small",
            metadata_json: JSON.stringify({ suite: "sdk-live", fixture: "parser" }),
        }, opts);
        if (document.document_id)
            fix.set("embedding_document_id", document.document_id);
        if (document.job_id)
            fix.set("embedding_document_job_id", document.job_id);
    });
    // ── LockService: separate held locks for renew and release ────────────────────
    const lockOwner = fix.lookup("user_id") ?? liveUuid();
    // Lease long enough to outlive the whole measured run: the perf surface takes
    // well over a minute to reach the measured RenewLock/ReleaseLock, and a short
    // lease would expire first → "lock_not_held".
    await tryRun("AcquireReleaseLock", async () => {
        const held = await gen.LockService.acquire_lock({ tenant_id: tenantId, lock_name: "sdk-perf-release-lock", owner_id: lockOwner, lease_ttl_seconds: 3600, metadata_json: "{}" }, opts);
        if (held.fencing_token)
            fix.set("release_fencing_token", String(held.fencing_token));
    });
    await tryRun("AcquireRenewLock", async () => {
        const held = await gen.LockService.acquire_lock({ tenant_id: tenantId, lock_name: "sdk-perf-renew-lock", owner_id: lockOwner, lease_ttl_seconds: 3600, metadata_json: "{}" }, opts);
        if (held.fencing_token)
            fix.set("renew_fencing_token", String(held.fencing_token));
    });
    // ── SchedulerService: durable jobs for read/pause/resume/delete rows ──────────
    await tryRun("CreateSchedulerJob", async () => {
        const job = await gen.SchedulerService.create_job({ tenant_id: tenantId, project_id: "", name: `sdk-perf-job-${suffix}`, schedule_type: "CRON", cron_expression: "*/5 * * * *", payload: "{}", target_topic: "sdk.perf.scheduler", max_attempts: 3, backoff_seconds: 30 }, opts);
        if (job.job_id)
            fix.set("job_id", job.job_id);
    });
    await tryRun("CreateDeleteSchedulerJob", async () => {
        const job = await gen.SchedulerService.create_job({ tenant_id: tenantId, project_id: "", name: `sdk-perf-delete-job-${suffix}`, schedule_type: "CRON", cron_expression: "*/5 * * * *", payload: "{}", target_topic: "sdk.perf.scheduler.delete", max_attempts: 3, backoff_seconds: 30 }, opts);
        if (job.job_id)
            fix.set("delete_job_id", job.job_id);
    });
    // ── WebhookService: endpoints for read/update/delete rows ────────────────────
    await tryRun("CreateWebhookEndpoint", async () => {
        const endpoint = await gen.WebhookService.create_endpoint({ tenant_id: tenantId, url: "https://example.com/udb-webhook", topic_pattern: fix.lookup("topic_pattern") ?? "topic.*", description: "sdk perf webhook", max_attempts: 3, metadata_json: "{}" }, opts);
        if (endpoint.endpoint_id)
            fix.set("endpoint_id", endpoint.endpoint_id);
    });
    await tryRun("CreateDeleteWebhookEndpoint", async () => {
        const endpoint = await gen.WebhookService.create_endpoint({ tenant_id: tenantId, url: "https://example.com/udb-webhook-delete", topic_pattern: fix.lookup("topic_pattern") ?? "topic.*", description: "sdk perf webhook delete", max_attempts: 3, metadata_json: "{}" }, opts);
        if (endpoint.endpoint_id)
            fix.set("delete_endpoint_id", endpoint.endpoint_id);
    });
    // ── WorkflowService: running instances for read/signal/cancel rows ────────────
    await tryRun("StartWorkflow", async () => {
        const workflow = await gen.WorkflowService.start_workflow({ tenant_id: tenantId, project_id: "", workflow_type: "sdk.perf.workflow", total_steps: 20, payload: "{}", compensations: "[]", correlation_id: recordId }, opts);
        if (workflow.workflow_id)
            fix.set("workflow_id", workflow.workflow_id);
    });
    await tryRun("StartCancelWorkflow", async () => {
        const workflow = await gen.WorkflowService.start_workflow({ tenant_id: tenantId, project_id: "", workflow_type: "sdk.perf.workflow.cancel", total_steps: 20, payload: "{}", compensations: "[]", correlation_id: `${recordId}-cancel` }, opts);
        if (workflow.workflow_id)
            fix.set("cancel_workflow_id", workflow.workflow_id);
    });
    // ── VaultService: secret + transit key material for crypto/read rows ──────────
    await tryRun("SeedVaultSecret", async () => {
        await gen.VaultService.put_secret({ tenant_id: tenantId, secret_path: "secret/path", secret_value: "perf-secret", expected_version: 0, metadata_json: "{}" }, opts);
    });
    await tryRun("SeedVaultDeleteSecret", async () => {
        await gen.VaultService.put_secret({ tenant_id: tenantId, secret_path: "secret/delete", secret_value: "perf-secret-delete", expected_version: 0, metadata_json: "{}" }, opts);
    });
    await tryRun("SeedVaultDestroySecret", async () => {
        await gen.VaultService.put_secret({ tenant_id: tenantId, secret_path: "secret/destroy", secret_value: "perf-secret-destroy", expected_version: 0, metadata_json: "{}" }, opts);
    });
    await tryRun("SeedVaultTransit", async () => {
        await gen.VaultService.create_transit_key({ tenant_id: tenantId, key_name: "transit-key", algorithm: "aes256-gcm-siv" }, opts);
        const encrypted = await gen.VaultService.encrypt({ tenant_id: tenantId, key_name: "transit-key", plaintext: "perf" }, opts);
        if (encrypted.ciphertext)
            fix.set("vault_ciphertext", encrypted.ciphertext);
        // A dedicated ed25519 SIGNING key so GetTransitPublicKey exports a real public
        // key — the aes256-gcm-siv key above has no exportable public half. Sign/Verify
        // also require this ed25519 key (the symmetric aes256-gcm-siv key is rejected),
        // so it must exist before the Sign below that seeds vault_signature.
        await gen.VaultService.create_transit_key({ tenant_id: tenantId, key_name: "transit-signing-key", algorithm: "ed25519" }, opts);
        const signed = await gen.VaultService.sign({ tenant_id: tenantId, key_name: "transit-signing-key", input: "perf" }, opts);
        if (signed.signature)
            fix.set("vault_signature", signed.signature);
        // A dedicated hmac-sha256 key — the transit Hmac verb now requires a
        // purpose-built hmac-sha256 key and rejects the symmetric aes256-gcm-siv key.
        await gen.VaultService.create_transit_key({ tenant_id: tenantId, key_name: "transit-hmac-key", algorithm: "hmac-sha256" }, opts);
        fix.set("vault_hmac_key_name", "transit-hmac-key");
    });
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
    // A SEPARATE registered+uploaded but NOT finalized file for the measured
    // FinalizeUpload — finalizing the primary file_id again fails "already
    // finalized", so the measured Finalize needs its own un-finalized target. Upload
    // the bytes (so Finalize's object HEAD succeeds) but intentionally do NOT call
    // FinalizeUpload — the measured RPC finalizes it.
    await tryRun("RegisterFinalizeFile", async () => {
        // FinalizeUpload verifies the stored object's byte length against the size
        // DECLARED at RegisterUpload, so declare exactly what we upload — a fixed
        // literal fails "uploaded object size N does not match declared M".
        // The shared bench body declares size_bytes: 1024 and FinalizeUpload verifies the
        // stored object against THAT, so the seeded object must be exactly 1024 B.
        const fpayloadBase = `sdk-perf-finalize-${suffix}`;
        const fpayload = fpayloadBase + "x".repeat(1024 - fpayloadBase.length);
        const fpayloadLen = Buffer.byteLength(fpayload, "utf8");
        // FinalizeUpload refuses to CHANGE reference_id from the value established at
        // RegisterUpload, so the measured body must resend that exact value — seed it.
        const finRefId = liveUuid();
        fix.set("finalize_reference_id", finRefId);
        const freg = await gen.StorageService.register_upload({ tenant_id: uuidTenant, project_id: "", filename: `perf-fin-${suffix}.txt`, content_type: "text/plain", file_type: "DOCUMENT", reference_id: finRefId, reference_type: "sdk.perf", size_bytes: fpayloadLen, expires_in_minutes: 30 }, opts);
        const ffid = freg.file_id;
        fix.set("finalize_file_id", ffid);
        fix.set("file_size_bytes", String(fpayloadLen));
        addCleanup(async () => {
            try {
                await gen.StorageService.delete_file({ tenant_id: uuidTenant, file_id: ffid }, opts);
            }
            catch { /* best-effort */ }
        });
        const fUploadUrl = freg.upload_url || "";
        let fput200 = false;
        if (fUploadUrl) {
            try {
                const res = await fetch(fUploadUrl, { method: "PUT", body: fpayload, headers: { "Content-Type": "text/plain" } });
                fput200 = res.ok;
            }
            catch { /* fall through to PutObject */ }
        }
        if (!fput200) {
            const put = data.put_object({ deadlineMs: 10_000, noRetry: true });
            put.stream.write({ context: ctx, bucket: process.env.UDB_OBJECT_BUCKET || "udb-storage", object_key: freg.object_key, data: Buffer.from(fpayload, "utf8"), content_type: "text/plain", final_chunk: true });
            put.stream.end();
            await put.response;
        }
    });
    // A registered-but-PENDING upload (never uploaded, never finalized) for the
    // measured ReissueUploadUrl — it resumes a PENDING upload and rejects any
    // non-PENDING (finalized/ACTIVE) file, so it needs its own PENDING target.
    await tryRun("RegisterReissueFile", async () => {
        const rreg = await gen.StorageService.register_upload({ tenant_id: uuidTenant, project_id: "", filename: `perf-reissue-${suffix}.txt`, content_type: "text/plain", file_type: "DOCUMENT", reference_id: liveUuid(), reference_type: "sdk.perf", size_bytes: 64, expires_in_minutes: 30 }, opts);
        const rfid = rreg.file_id;
        fix.set("reissue_file_id", rfid);
        addCleanup(async () => {
            try {
                await gen.StorageService.delete_file({ tenant_id: uuidTenant, file_id: rfid }, opts);
            }
            catch { /* best-effort */ }
        });
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
        // A SEPARATE high-capacity room for the measured JoinSession. The main room_id
        // is filled to its cap of 8 by JoinRoom's mutation iters, so JoinSession
        // against it would hit "room ... at capacity". maxParticipants=64 leaves room
        // for the 5 JoinSession iters.
        await tryRun("CreateJoinSessionRoom", async () => {
            const jsr = await gen.RoomService.create_room({ tenant_id: uuidTenant, name: `sdk-perf-joinsession-room-${suffix}`, max_participants: 64, config: "{}", created_by: liveUuid() }, opts);
            const jsrId = jsr.room_id;
            fix.set("join_session_room_id", jsrId);
            addCleanup(async () => {
                try {
                    await gen.RoomService.close_room({ tenant_id: uuidTenant, room_id: jsrId }, opts);
                }
                catch { /* best-effort */ }
            });
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
            s.once?.("data", (msg) => {
                if (msg?.version_info)
                    fix.set("rollback_resource_version", msg.version_info);
                fin();
            });
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
    // ── BackupService: a policy row + a started backup id for read/restore RPCs ──
    fix.set("restore_tenant_id", liveUuid());
    await tryRun("PutBackupPolicy", async () => {
        await gen.BackupService.put_backup_policy({
            tenant_id: tenantId,
            policy_name: "sdk-perf-default",
            schedule_cron: "0 3 * * *",
            retention_days: 7,
            max_retained_backups: 3,
            enabled: true,
            metadata_json: "{}",
        }, opts);
    });
    await tryRun("StartTenantBackup", async () => {
        const backup = await gen.BackupService.start_tenant_backup({ tenant_id: tenantId }, { deadlineMs: 60_000, noRetry: true });
        fix.set("backup_id", backup.backup_id);
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
            if (methodName === "serviceFull")
                continue;
            if (typeof fn !== "function")
                continue;
            if (!methodName.includes("_") && Object.entries(api).some(([otherName, otherFn]) => otherName.includes("_") && otherFn === fn))
                continue;
            if (NON_UNARY_METHODS.has(methodName))
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
    const requiredBackends = new Set((process.env.UDB_LIVE_REQUIRED_BACKENDS || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean));
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
        if (requiredBackends.size > 0 && !requiredBackends.has(backend))
            continue;
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
    const requiredBackends = new Set((process.env.UDB_LIVE_REQUIRED_BACKENDS || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean));
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
        if (requiredBackends.size > 0 && !requiredBackends.has(backend))
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
    const selectV2 = await drainReadable(data.select_v_2({
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
    await gen.NotificationService.upsert_template({ event_type: event, channel: 1, locale: "en", subject_template: "SDK notify", body_template: body, is_active: true }, opts);
    const template = (await gen.NotificationService.get_template({ event_type: event, channel: 1, locale: "en" }, opts)).template;
    node_assert_1.strict.equal(template.body_template, body);
    const sent = await gen.NotificationService.send_notification({ event_type: event, recipient_id: recipient.user_id, recipient_address: `sdk+${suffix}@example.com`, tenant_id: tenantId, channels: [1], variables: { n: "1" } }, opts);
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
        await expectStreamMounted("target.DataBroker.publish_cdc", () => project.generated.DataBroker.publish_cdc({}, { deadlineMs: 2_000 }));
        await expectStreamMounted("target.DataBroker.select_v_2", () => project.generated.DataBroker.select_v_2({}, { deadlineMs: 2_000 }));
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
        await expectStreamMounted("authTarget.StorageService.download_file", () => authGenerated.StorageService.download_file({}, { deadlineMs: 2_000 }));
        await expectStreamMounted("authTarget.LiveQueryService.subscribe", () => authGenerated.LiveQueryService.subscribe({}, { deadlineMs: 2_000 }));
        node_assert_1.strict.ok(nativeCount > 0, "native control-plane unary RPCs must be probed");
        node_assert_1.strict.ok(dataCount > 0, "DataBroker unary RPCs must be probed");
        // Full-surface coverage like Go/Python/PHP: unary RPCs plus the streaming
        // RPCs probed individually below must equal the generated operation catalog.
        const STREAMING_PROBED = 13; // get_object, publish_cdc, select_v_2, put_object,
        //   batch_select, batch_upsert, begin_tx, vector_batch_upsert, delta_resources,
        //   stream_resources, signal, download_file, subscribe
        const expectedRpcCount = Object.keys(generatedClient_1.RPC_OPERATION_KIND).length;
        node_assert_1.strict.equal(nativeCount + dataCount + STREAMING_PROBED, expectedRpcCount, `TS probed ${nativeCount + dataCount} unary + ${STREAMING_PROBED} streaming = ${nativeCount + dataCount + STREAMING_PROBED}, want ${expectedRpcCount} — full-surface coverage regressed`);
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
        const isCapabilitySkip = (serviceName, methodName, err, detail) => {
            if (err !== "FAILED_PRECONDITION")
                return false;
            if (serviceName !== "RoomService")
                return false;
            if (!new Set(["list_egress", "start_room_composite", "start_track_egress", "stop_egress"]).has(methodName))
                return false;
            return /webrtc_egress_(enabled|backend)/.test(detail ?? "");
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
        // (empty) completion. Used for select_v_2 / get_object.
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
        // CDC first-EVENT timer: subscribe to publish_cdc, then fire a real Upsert
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
        const SERVER_STREAM_FIRST_RESPONSE = new Set(["select_v_2", "get_object", "download_file"]);
        const seededStreamRequest = (methodName) => {
            if (methodName === "select_v_2") {
                return { context: requestContext(tenantId, projectId, "ts.live.perf"), message_type: LIVE_MESSAGE_TYPE, filter: { tenant_id: tenantId, project_id: projectId }, limit: 1 };
            }
            if (methodName === "get_object") {
                return { context: requestContext(tenantId, projectId, "ts.live.perf"), bucket: fixtures.lookup("bucket") ?? (process.env.UDB_LIVE_S3_BUCKET || "udb-live-sdk"), object_key: fixtures.lookup("object_key") ?? "" };
            }
            if (methodName === "download_file") {
                // StorageService server-streaming download of the seeded, finalized file:
                // the first DownloadFileChunk carries object metadata + the first bytes.
                return { tenant_id: tenantId, file_id: fixtures.lookup("file_id") ?? "", chunk_size_bytes: 65536 };
            }
            // Only select_v_2/get_object/download_file reach here; never a generic body.
            return perfRealBody("StorageService", methodName, tenantId, projectId, fixtures) ?? {};
        };
        // ── measureRpc: time ONE RPC (unary or streaming) and push its sample ─────────
        // Extracted from the old single-pass loop so the AUTH-ROUTE 3-phase ordering
        // (BENCH_RPC_BODIES.md "Execution order") can drive the SAME measurement code in
        // a deterministic order: Phase 1 (session establish) → seed → Phase 2 (the bulk)
        // → Phase 3 (session/credential teardown), so a destructive AuthnService RPC
        // never kills the live principal mid-run.
        const measureRpc = async (serviceName, api, methodName, fn) => {
            // Facade accessors (DataBroker.table(name) / entity(messageType)) are builder
            // helpers on the generated client, NOT RPCs. Skip them before classifying the
            // callable as stream/unary so a helper never trips the strict body coverage gate.
            if (methodName === "entity" || methodName === "table")
                return;
            if (NON_UNARY_METHODS.has(methodName)) {
                // CDC subscription: subscribe → fire a real seeded Upsert → first event.
                if (serviceName === "DataBroker" && methodName === "publish_cdc") {
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
                    samples.push({ service: serviceName, rpc: snakeToPascal(methodName), apiAlias: apiAliasOf(api.serviceFull, methodName), operationId: operationIdOf(api.serviceFull, methodName), kind: "stream", err: errCode, p50: pct(50), p99: pct(99), mean: durs.reduce((s, d) => s + d, 0) / durs.length, note: "cdc: time-to-first-event (real seeded Upsert produced)" });
                    return;
                }
                // Server-streaming reads with a real first response (select_v_2, get_object).
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
                    samples.push({ service: serviceName, rpc: snakeToPascal(methodName), apiAlias: apiAliasOf(api.serviceFull, methodName), operationId: operationIdOf(api.serviceFull, methodName), kind: "stream", err: errCode, p50: pct(50), p99: pct(99), mean: durs.reduce((s, d) => s + d, 0) / durs.length, note: "streaming: time-to-first-response (seeded)" });
                    return;
                }
                // Client-streaming / bidi: a single seeded message cannot drive a real
                // response in a passive run — report stream-open latency. The first message
                // is the shared manifest body (no generic): perfRealBody must cover it.
                const streamReq = perfRealBody(serviceName, methodName, tenantId, projectId, fixtures);
                if (!streamReq)
                    throw new Error(`perfRealBody has no doc-grounded body for streaming ${serviceName}/${methodName} — gap/bypass not allowed`);
                const d = timeStreamOpen(fn, streamReq);
                samples.push({ service: serviceName, rpc: snakeToPascal(methodName), apiAlias: apiAliasOf(api.serviceFull, methodName), operationId: operationIdOf(api.serviceFull, methodName), kind: "stream_open", err: "OK", p50: d, p99: d, mean: d, note: "streaming: stream-open latency" });
                return;
            }
            const kind = operationKindOf(api.serviceFull, methodName) || "read_only";
            // Every RPC gets its shared manifest body from perfRealBody — NO generic
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
            const capabilitySkipped = !anyOk && isCapabilitySkip(serviceName, methodName, firstErr, firstDetail);
            const errCode = anyOk ? "OK" : capabilitySkipped ? "CAPABILITY_SKIPPED" : firstErr;
            const errDetail = anyOk ? undefined : firstDetail;
            const durs = (anyOk ? okDurs : allDurs);
            if (errCode !== "OK" && errCode !== "CAPABILITY_SKIPPED")
                console.error(`FAILDETAIL ${serviceName}/${methodName} [${errCode}] ${errDetail ?? ""}`);
            durs.sort((a, b) => a - b);
            const pct = (p) => durs[Math.min(durs.length - 1, Math.floor((p * (durs.length - 1)) / 100))];
            samples.push({
                service: serviceName, rpc: snakeToPascal(methodName),
                apiAlias: apiAliasOf(api.serviceFull, methodName),
                operationId: operationIdOf(api.serviceFull, methodName),
                kind, err: errCode,
                p50: pct(50), p99: pct(99), mean: durs.reduce((s, d) => s + d, 0) / durs.length,
                note: capabilitySkipped ? `capability skipped: ${errDetail ?? "server reported unavailable capability"}` : kind === "destructive" ? "destructive: 1 real call against a seeded disposable target" : `${kind} (seeded success path)`,
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
        // or credentials. Dedicated manifest seeds target disposable users for the
        // terminal user-mutating rows, so the admin's bearer/session stays live until
        // the final tenant purge login below.
        const PHASE3_AUTHN = new Set([
            "logout", "revoke_session", "admin_revoke_session", "admin_revoke_all_user_sessions",
            "admin_revoke_all_tenant_sessions", "emergency_revoke", "change_password",
            "reset_password", "admin_reset_password", "change_user_status", "admin_reset_mfa",
            "revoke_recovery_codes", "revoke_device", "delete_web_authn_credential", "disable_mfa_factor",
        ]);
        const phase1 = [];
        let phase2 = [];
        const phase3 = [];
        const terminalDestructive = [];
        const TERMINAL_DESTRUCTIVE = new Set(["TenantService/purge_tenant"]);
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
                    // The generated TypeScript client exposes both idiomatic camelCase and
                    // documented snake_case aliases. Measure the snake alias once when both
                    // names point to the same RPC function, otherwise the perf count doubles.
                    if (!methodName.includes("_") && Object.entries(api).some(([otherName, otherFn]) => otherName.includes("_") && otherFn === fn))
                        continue;
                    const unit = { serviceName, api, methodName, fn: fn };
                    const unitKey = `${serviceName}/${methodName}`;
                    if (TERMINAL_DESTRUCTIVE.has(unitKey))
                        terminalDestructive.push(unit);
                    else if (serviceName === "AuthnService" && PHASE1_AUTHN_ORDER.includes(methodName))
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
        // Phase 3: tear the session/credentials down LAST against disposable seeded
        // targets. Some rows intentionally deactivate/revoke the current tenant's
        // authn state, so no later measurement may depend on that principal.
        for (const u of phase3)
            await measureRpc(u.serviceName, u.api, u.methodName, u.fn);
        // Terminal destructive RPCs can invalidate broad tenant state. Keep them
        // after Authn teardown, using the same verified tenant-scoped credential as
        // the other SDK harnesses instead of performing another login after
        // tenant-wide session revocation has run.
        if (terminalDestructive.length > 0) {
            tenantId = fixtures.lookup("purge_tenant_id") || tenantId;
            project.setTenant(tenantId);
        }
        for (const u of terminalDestructive)
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
                + "stream-open latency. CDC subscription (publish_cdc, kind=stream) reports time-to-FIRST-EVENT: "
                + "the harness subscribes, fires a real seeded Upsert that flows outbox→CDC→Kafka, and times the "
                + "first delivered event.", "",
            "RPCs run on the AUTH ROUTE in three phases (BENCH_RPC_BODIES.md \"Execution order\"): Phase 1 "
                + "establishes the session (AuthnService login -> refresh_session -> authenticate -> "
                + "validate_token → introspect_token → get_jwks), then the seed phase; Phase 2 measures everything "
                + "else; Phase 3 LAST runs the session/credential-teardown AuthnService RPCs (logout, revoke_*, "
                + "change/reset password, admin_reset_mfa, disable_mfa_factor, …) against the seeded DISPOSABLE "
                + "user/session so the admin's own session is never killed mid-run. The final terminal destructive "
                + "tenant purge uses the verified tenant-scoped benchmark credential, matching the other SDK harnesses.", "",
            "## Seeded fixtures", "",
            `Captured semantic field → seeded value keys used to resolve request fields: ${fkeys.join(", ")}`, "",
            "## Per-service mean latency", "", "| Service | RPCs | mean ms |", "|---|--:|--:|"];
        for (const name of [...svc.keys()].sort((a, b) => mean(svc.get(b)) - mean(svc.get(a)))) {
            lines.push(`| ${name} | ${svc.get(name).length} | ${mean(svc.get(name)).toFixed(2)} |`);
        }
        // Failures subsection: every RPC whose last iteration returned a non-OK gRPC
        // status, excluding explicit server-declared optional capability skips.
        const failed = samples.filter((s) => s.err !== "OK" && s.err !== "CAPABILITY_SKIPPED");
        const skipped = samples.filter((s) => s.err === "CAPABILITY_SKIPPED");
        lines.push("", `## Failures (${failed.length})`, "");
        if (failed.length === 0) {
            lines.push("No RPC returned a non-OK gRPC status.");
        }
        else {
            lines.push("These RPCs returned a non-OK gRPC status and are FAILURES, not latency samples.");
            lines.push("", "| RPC | api_alias | operation_id | kind | err | p99 ms | mean ms |", "|---|---|---|---|---|--:|--:|");
            for (const s of [...failed].sort((a, b) => (a.service + a.rpc).localeCompare(b.service + b.rpc))) {
                lines.push(`| ${s.service}/${s.rpc} | ${s.apiAlias} | ${s.operationId} | ${s.kind} | ${s.err} | ${s.p99.toFixed(2)} | ${s.mean.toFixed(2)} |`);
            }
        }
        lines.push("", `## Capability Skips (${skipped.length})`, "");
        if (skipped.length === 0) {
            lines.push("No RPC was skipped for an unavailable optional capability.");
        }
        else {
            lines.push("| RPC | api_alias | operation_id | kind | reason |", "|---|---|---|---|---|");
            for (const s of [...skipped].sort((a, b) => (a.service + a.rpc).localeCompare(b.service + b.rpc))) {
                lines.push(`| ${s.service}/${s.rpc} | ${s.apiAlias} | ${s.operationId} | ${s.kind} | ${s.note} |`);
            }
        }
        lines.push("", "## Slowest 20 by p99", "", "| RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | note |", "|---|---|---|---|---|--:|--:|--:|---|");
        for (const s of [...samples].sort((a, b) => b.p99 - a.p99).slice(0, 20)) {
            lines.push(`| ${s.service}/${s.rpc} | ${s.apiAlias} | ${s.operationId} | ${s.kind} | ${s.err} | ${s.p50.toFixed(2)} | ${s.p99.toFixed(2)} | ${s.mean.toFixed(2)} | ${s.note} |`);
        }
        lines.push("", "## Full per-RPC table (sorted by service, then RPC)", "", "| Service | RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | note |", "|---|---|---|---|---|---|--:|--:|--:|---|");
        for (const s of [...samples].sort((a, b) => (a.service === b.service ? a.rpc.localeCompare(b.rpc) : a.service.localeCompare(b.service)))) {
            lines.push(`| ${s.service} | ${s.rpc} | ${s.apiAlias} | ${s.operationId} | ${s.kind} | ${s.err} | ${s.p50.toFixed(2)} | ${s.p99.toFixed(2)} | ${s.mean.toFixed(2)} | ${s.note} |`);
        }
        (0, node_fs_1.writeFileSync)("perf_report_ts.md", lines.join("\n") + "\n");
        const expectedPerfCount = Object.keys(generatedClient_1.RPC_OPERATION_KIND).length;
        node_assert_1.strict.ok(samples.length >= expectedPerfCount, `perf measured only ${samples.length} RPCs (want all ${expectedPerfCount})`);
        console.log(`\nTS perf: ${samples.length} RPCs measured, ${failed.length} FAILED (non-OK gRPC status) → sdk/typescript/perf_report_ts.md`);
        await seed.cleanup();
    }
    finally {
        project.close();
    }
});
// ── Scenario perf (gated on UDB_SCENARIO_PERF=1, SEPARATE from the full sweep) ──
//
// This is the SCENARIO bench: it times the user-facing WORKFLOW HELPERS the
// simple-client docs prescribe (uploadFile, downloadFile, bound entity
// upsert/select/delete, loginAndAdoptTenant, events subscribe-ready/publishAndWait,
// webrtc joinSession) as end-to-end facade calls — NOT the raw generated RPC surface
// (that stays in the "live per-RPC perf" sweep above → perf_report_ts.md). It is
// gated by its OWN flag (UDB_SCENARIO_PERF=1) and writes its OWN report
// (scenario_perf_ts.md) so it can run/report independently. Each row's `seq` is the
// documented helper RPC sequence (docs/bench-bodies/workflow-sequences.md).
(0, node_test_1.test)("live scenario perf", {
    skip: process.env.UDB_LIVE_SDK_TESTS === "1" && process.env.UDB_SCENARIO_PERF === "1"
        ? false
        : "requires live UDB broker + UDB_SCENARIO_PERF=1",
}, async () => {
    const target = requiredEnv("UDB_GRPC_TARGET");
    const authTarget = process.env.UDB_AUTH_GRPC_TARGET?.trim() || target;
    const username = requiredEnv("UDB_LIVE_USERNAME");
    const password = requiredEnv("UDB_LIVE_PASSWORD");
    const tenantHint = process.env.UDB_LIVE_TENANT || "sdk-live";
    const projectId = process.env.UDB_LIVE_PROJECT || "default";
    const project = new project_1.UdbProject({
        target, authTarget, tenantId: tenantHint, projectId,
        purpose: "ts.live.scenario.perf", tokenStore: memoryStore(), deadlineMs: 20_000,
    });
    const codeNameOf = (err) => {
        const code = grpcCode(err);
        if (code === undefined)
            return "UNKNOWN";
        return grpc.status[code] ?? String(code);
    };
    const pct = (sorted, p) => {
        if (sorted.length === 0)
            return 0;
        const i = Math.min(sorted.length - 1, Math.floor((p * (sorted.length - 1)) / 100));
        return sorted[i];
    };
    try {
        const suffix = Date.now().toString(36);
        const ENTITY = "udb.sdk.live.v1.SdkLiveRecord";
        const entity = () => project.entity(ENTITY, { key: ["record_id"] });
        // Track the adopted canonical tenant locally (the bootstrap login below resolves
        // it from the verified principal; entity()/the facades already carry it on the
        // wire, but the request bodies need the same value).
        let adoptedTenant = tenantHint;
        const tenant = () => adoptedTenant;
        const scenarioRecord = (i) => ({
            record_id: `ts-scn-${suffix}-${i}`, tenant_id: tenant(), project_id: projectId,
            lookup_key: "ts-scn-lk", payload: "ts-scenario", revision: 1,
        });
        const scenarios = [
            {
                name: "loginAndAdoptTenant", seq: "Login, AuthenticateBearer", iters: 5, warmup: false,
                fn: async () => { await project.loginAndAdoptTenant({ username, password, tenant_hint: tenantHint, project_hint: projectId, device_name: "ts-sdk-scenario" }); },
            },
        ];
        // Adopt the canonical tenant before the data/native scenarios run, capturing the
        // verified principal's tenant for the scenario request bodies.
        await project.loginAndAdoptTenant({ username, password, tenant_hint: tenantHint, project_hint: projectId, device_name: "ts-sdk-scenario-bootstrap" });
        try {
            const tok = await project.currentToken();
            if (tok?.accessToken) {
                const who = await project.auth.authenticateBearer(tok.accessToken);
                adoptedTenant = who?.principal?.tenant_id || adoptedTenant;
            }
        }
        catch { /* fall back to the hint tenant */ }
        scenarios.push({
            name: "entity.upsert", seq: "Upsert", iters: 10, warmup: true,
            fn: async () => { await entity().upsert(scenarioRecord(0), { returnRecord: false }); },
        }, {
            name: "entity.select", seq: "Select", iters: 25, warmup: true,
            fn: async () => { await entity().select({ where: { tenant_id: tenant(), project_id: projectId } }); },
        }, {
            name: "entity.delete", seq: "Delete", iters: 5, warmup: true,
            fn: async () => { await entity().delete({ record_id: "ts-scn-delete-noop", tenant_id: tenant(), project_id: projectId }); },
        }, {
            name: "uploadFile", seq: "RegisterUpload, PUT, FinalizeUpload", iters: 5, warmup: true,
            fn: async () => { await project.storage.uploadFile("ts-scenario.txt", Buffer.from("ts-scenario-upload"), { contentType: "text/plain", fileType: "DOCUMENT" }); },
        }, {
            name: "events.publishAndWait", seq: "EnqueueOutboxEvent, PublishCDC first-event", iters: 3, warmup: false,
            fn: async () => { await project.events.publishAndWait("sdk.scenario." + suffix, { event: "ts-scenario", n: suffix }, () => true, 20_000); },
        });
        // downloadFile / webrtc.joinSession need a pre-existing file / room — seed one
        // of each (cost NOT measured) so the timed scenario is the pure helper path.
        try {
            const up = await project.storage.uploadFile("ts-scenario-dl.txt", Buffer.from("ts-scenario-download"), { contentType: "text/plain", fileType: "DOCUMENT" });
            const fileId = up?.file?.file_id ?? up?.file_id ?? "";
            if (fileId) {
                scenarios.push({ name: "downloadFile", seq: "GetDownloadUrl", iters: 25, warmup: true, fn: async () => { await project.storage.downloadFile(fileId, { expires_in_minutes: 5 }); } });
            }
        }
        catch (err) {
            console.log(`scenario seed: download file upload failed, downloadFile scenario skipped: ${codeNameOf(err)}`);
        }
        try {
            const room = await project.webrtc.room.createRoom({ name: "ts-scenario-room-" + suffix, max_participants: 8, config: "{}" });
            const roomId = room?.room_id ?? room?.room?.room_id ?? "";
            if (roomId) {
                scenarios.push({
                    name: "webrtc.joinSession", seq: "JoinSession, Signal(open)", iters: 5, warmup: false,
                    fn: async () => { const s = await project.webrtc.joinSession(roomId, { displayName: "ts-scenario-peer", ttlSeconds: 60, heartbeatMs: 0 }); await s.leave(); },
                });
            }
        }
        catch (err) {
            console.log(`scenario seed: webrtc room create failed, joinSession scenario skipped: ${codeNameOf(err)}`);
        }
        const samples = [];
        for (const sc of scenarios) {
            if (sc.warmup) {
                try {
                    await sc.fn();
                }
                catch { /* warm-up errors ignored */ }
            }
            const okMs = [];
            const allMs = [];
            let firstErr = "OK", firstDetail = "";
            for (let i = 0; i < sc.iters; i++) {
                const start = performance.now();
                let err = "OK";
                try {
                    await sc.fn();
                }
                catch (e) {
                    err = codeNameOf(e);
                    if (i === 0)
                        firstDetail = (e?.details || e?.message || String(e)).slice(0, 200);
                }
                const ms = performance.now() - start;
                if (i === 0)
                    firstErr = err;
                if (err === "OK")
                    okMs.push(ms);
                allMs.push(ms);
            }
            const measured = (okMs.length > 0 ? okMs : allMs).sort((a, b) => a - b);
            const errCode = okMs.length > 0 ? "OK" : firstErr;
            if (errCode !== "OK")
                console.log(`[SCENARIO-FAIL] ${sc.name} => ${errCode}: ${firstDetail}`);
            const mean = measured.reduce((a, b) => a + b, 0) / measured.length;
            samples.push({ name: sc.name, seq: sc.seq, err: errCode, p50: pct(measured, 50), p99: pct(measured, 99), mean, min: measured[0], max: measured[measured.length - 1], iters: sc.iters });
        }
        const lines = [];
        lines.push("# UDB SDK Scenario Perf — TypeScript (localhost)", "");
        lines.push(`Scenarios measured: ${samples.length}   tenant=${tenant()}`, "");
        lines.push("This is the SCENARIO bench: it times the user-facing WORKFLOW HELPERS the " +
            "simple-client docs prescribe (uploadFile, downloadFile, bound entity " +
            "upsert/select/delete, loginAndAdoptTenant, events publishAndWait, webrtc " +
            "joinSession) — measured as end-to-end facade calls, NOT the raw generated RPC " +
            "surface (that stays in perf_report_ts.md). Each `seq` is the documented helper " +
            "RPC sequence (docs/bench-bodies/workflow-sequences.md).", "");
        lines.push("| Scenario | seq | err | p50 ms | p99 ms | mean ms | min ms | max ms | iters |", "|---|---|---|--:|--:|--:|--:|--:|--:|");
        for (const s of [...samples].sort((a, b) => a.name.localeCompare(b.name))) {
            lines.push(`| ${s.name} | ${s.seq} | ${s.err} | ${s.p50.toFixed(2)} | ${s.p99.toFixed(2)} | ${s.mean.toFixed(2)} | ${s.min.toFixed(2)} | ${s.max.toFixed(2)} | ${s.iters} |`);
        }
        const failed = samples.filter((s) => s.err !== "OK");
        lines.push("", failed.length === 0
            ? "Every scenario ran its success path (no non-OK gRPC status)."
            : `${failed.length} scenario(s) returned a non-OK gRPC status (see [SCENARIO-FAIL] log lines).`);
        (0, node_fs_1.writeFileSync)("scenario_perf_ts.md", lines.join("\n") + "\n");
        console.log(`\nTS scenario perf: ${samples.length} workflow helpers measured, ${failed.length} FAILED → sdk/typescript/scenario_perf_ts.md`);
    }
    finally {
        project.close();
    }
});
