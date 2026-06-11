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
const grpc = __importStar(require("@grpc/grpc-js"));
const project_1 = require("./project");
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
async function expectMounted(label, op) {
    try {
        await op();
    }
    catch (err) {
        const code = grpcCode(err);
        if (code !== undefined && FATAL_CONNECTIVITY_CODES.has(code)) {
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
            const code = grpcCode(err);
            if (code !== undefined && FATAL_CONNECTIVITY_CODES.has(code)) {
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
async function expectGeneratedUnarySurfaceMounted(label, generated, serviceNames) {
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
            await expectMounted(`${label}.${serviceName}.${methodName}`, () => fn({}, { deadlineMs: 2_000, noRetry: true }));
        }
    }
    return count;
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
}
(0, node_test_1.test)("live broker login refreshes once and hot-swaps SDK credentials", {
    skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker",
}, async () => {
    const target = requiredEnv("UDB_GRPC_TARGET");
    const authTarget = process.env.UDB_AUTH_GRPC_TARGET?.trim() || target;
    const username = requiredEnv("UDB_LIVE_USERNAME");
    const password = requiredEnv("UDB_LIVE_PASSWORD");
    const tenantId = process.env.UDB_LIVE_TENANT || "sdk-live";
    const projectId = process.env.UDB_LIVE_PROJECT || "default";
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
        await runLiveBackendE2E(project, tenantId, projectId);
        const authGenerated = project.authGenerated ?? project.generated;
        const nativeCount = await expectGeneratedUnarySurfaceMounted("authTarget", authGenerated, NATIVE_SERVICE_APIS);
        const dataCount = await expectGeneratedUnarySurfaceMounted("target", project.generated, ["DataBroker"]);
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
    }
    finally {
        project.close();
    }
});
