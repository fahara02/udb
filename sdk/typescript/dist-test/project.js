"use strict";
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
exports.Udb = exports.AuthzFacade = exports.UdbProject = exports.CredentialBindingError = exports.SessionExpiredError = exports.WebRtcFacade = exports.EventsFacade = exports.AdminFacade = exports.MigrationsFacade = exports.WebRtcTurnFacade = exports.WebRtcTrackFacade = exports.WebRtcPeerFacade = exports.WebRtcRoomFacade = exports.AssetFacade = exports.StorageFacade = exports.AnalyticsFacade = exports.NotificationFacade = exports.TenantFacade = exports.ApiKeyFacade = exports.MemoryTokenStore = void 0;
exports.createUdb = createUdb;
// UdbProject — a single, ergonomic entry point over the UDB data plane and the
// native control plane. It composes the existing hand-written and generated
// layers rather than replacing them:
//
//   - the generated robust client (`UdbGeneratedClient`) owns the shared data
//     channel set + retry/backoff + typed errors, and exposes every service's raw RPCs;
//   - the hand-written `UdbAuthClient` / `AuthzCache` provide auth ergonomics
//     (authenticate*, can/require/explain/batchCan, native-access, policy bundle);
//   - thin convenience wrappers forward through the generated client's `core`
//     (so they reuse the right channel + metadata) for the most common
//     notification / api-key / tenant / storage operations.
//
// Every sub-client shares one `UdbMetadata` (tenant/purpose/project/scopes/…) so
// the same caller identity is attached to every call. The raw generated client
// stays reachable via `.generated` (and `.core`) as an escape hatch.
//
// Native-service wrappers forward through the auth/control-plane `UdbCore` when
// `authTarget` differs from `target`; DataBroker escape hatches stay on the
// data-plane `UdbCore`.
//
// The bundled proto tree now ships the storage / asset / webrtc native services
// in addition to the original 7 (DataBroker, Authn, Authz, ApiKey, Tenant,
// Notification, Analytics), so:
//   - `.storage` is the StorageService file-lifecycle surface
//     (registerUpload / finalizeUpload / getDownloadUrl / get/update/delete/list
//     File). The pre-existing DataBroker object-byte IO (put/get/presign) stays
//     reachable as `.storage.putObject / getObject / presign`.
//   - `.asset` is the AssetService pipeline + asset surface.
//   - `.webrtc` namespaces the four unary WebRTC services
//     (`.webrtc.room / .peer / .track / .turn`) plus a thin `.webrtc.signal()`
//     bidi-stream helper over SignalingService.Signal.
const grpc = __importStar(require("@grpc/grpc-js"));
const http = __importStar(require("http"));
const https = __importStar(require("https"));
const auth_1 = require("./auth");
const client_1 = require("./client");
const connectEnv_1 = require("./connectEnv");
const consistency_1 = require("./consistency");
const entity_1 = require("./entity");
const stores_1 = require("./stores");
const facades_1 = require("./facades");
const tenantState_1 = require("./tenantState");
const generatedClient_1 = require("./generatedClient");
const protoRoot_1 = require("./protoRoot");
function metaFromConfig(config) {
    return {
        tenantId: config.tenantId,
        purpose: config.purpose ?? "",
        correlationId: config.correlationId ?? `udb-${Date.now().toString(36)}`,
        scopes: config.scopes,
        serviceIdentity: config.serviceIdentity,
        userId: config.userId,
        projectId: config.projectId,
        clientCatalogVersion: config.clientCatalogVersion ?? "",
        bearerToken: config.credentials?.bearerToken,
        apiKey: undefined,
    };
}
/** Plain HTTP(S) PUT of `body` to `urlStr` with a content-type header. Used by
 *  `StorageFacade.uploadFile` to send bytes to the broker-minted presigned URL.
 *  Resolves on a 2xx; rejects otherwise. No extra dependency (Node http/https). */
function httpPut(urlStr, contentType, body) {
    return new Promise((resolve, reject) => {
        let url;
        try {
            url = new URL(urlStr);
        }
        catch (e) {
            reject(new Error(`udb: storage.uploadFile: invalid upload_url`));
            return;
        }
        const transport = url.protocol === "http:" ? http : https;
        const req = transport.request(url, {
            method: "PUT",
            headers: { "content-type": contentType, "content-length": body.length },
        }, (res) => {
            const status = res.statusCode ?? 0;
            res.resume(); // drain
            if (status >= 200 && status < 300)
                resolve();
            else
                reject(new Error(`udb: storage.uploadFile: PUT failed (HTTP ${status})`));
        });
        req.on("error", (err) => reject(err));
        req.end(body);
    });
}
/** Default in-memory token store (single token; process-local). */
class MemoryTokenStore {
    token = null;
    load() {
        return this.token;
    }
    save(token) {
        this.token = token;
    }
    clear() {
        this.token = null;
    }
}
exports.MemoryTokenStore = MemoryTokenStore;
// ── Convenience sub-clients ─────────────────────────────────────────────────
/** Thin wrappers over the ApiKeyService generated RPCs. */
class ApiKeyFacade {
    core;
    service;
    constructor(core, service = "udb.core.apikey.services.v1.ApiKeyService") {
        this.core = core;
        this.service = service;
    }
    /** Create a key. The plain key is returned ONCE in `plain_key`. */
    create(request, call) {
        return this.core.unary(this.service, "CreateApiKey", request, call);
    }
    /** Revoke a key by id (with an optional `revoke_reason`). */
    revoke(keyId, revokeReason = "", call) {
        return this.core.unary(this.service, "RevokeApiKey", { key_id: keyId, revoke_reason: revokeReason }, call);
    }
    /** Update mutable key fields (name / description / scopes / rate limits / …). */
    update(request, call) {
        return this.core.unary(this.service, "UpdateApiKey", request, call);
    }
    get(keyId, call) {
        return this.core.unary(this.service, "GetApiKey", { key_id: keyId }, call);
    }
    list(request = {}, call) {
        return this.core.unary(this.service, "ListApiKeys", request, call);
    }
    rotate(keyIdOrRequest, requestOrCall = {}, call) {
        if (typeof keyIdOrRequest !== "string") {
            return this.core.unary(this.service, "RotateApiKey", keyIdOrRequest, requestOrCall);
        }
        const request = typeof requestOrCall === "string"
            ? { key_id: keyIdOrRequest, rotation_reason: requestOrCall }
            : { ...requestOrCall, key_id: keyIdOrRequest };
        return this.core.unary(this.service, "RotateApiKey", request, call);
    }
}
exports.ApiKeyFacade = ApiKeyFacade;
/** Thin wrappers over the TenantService generated RPCs. */
class TenantFacade {
    core;
    service;
    constructor(core, service = "udb.core.tenant.services.v1.TenantService") {
        this.core = core;
        this.service = service;
    }
    /** Onboard / create a tenant. (TenantService exposes no separate onboarding
     *  RPC; `CreateTenant` is the onboarding entry point.) */
    create(request, call) {
        return this.core.unary(this.service, "CreateTenant", request, call);
    }
    /** Alias for {@link create} for callers that think in onboarding terms. */
    onboard(request, call) {
        return this.create(request, call);
    }
    get(tenantId, call) {
        return this.core.unary(this.service, "GetTenant", { tenant_id: tenantId }, call);
    }
    list(request = {}, call) {
        return this.core.unary(this.service, "ListTenants", request, call);
    }
    update(request, call) {
        return this.core.unary(this.service, "UpdateTenant", request, call);
    }
}
exports.TenantFacade = TenantFacade;
/** Thin wrappers over the NotificationService generated RPCs. */
class NotificationFacade {
    core;
    service;
    constructor(core, service = "udb.core.notification.services.v1.NotificationService") {
        this.core = core;
        this.service = service;
    }
    /** Send (or enqueue) a notification. `request` is a `SendNotificationRequest`
     *  ({ event_type, recipient_id, recipient_address, variables, channels, … }). */
    send(request, call) {
        return this.core.unary(this.service, "SendNotification", request, call);
    }
    get(logId, call) {
        return this.core.unary(this.service, "GetNotification", { log_id: logId }, call);
    }
    list(request = {}, call) {
        return this.core.unary(this.service, "ListNotifications", request, call);
    }
    retry(logId, call) {
        return this.core.unary(this.service, "RetryNotification", { log_id: logId }, call);
    }
    /** Send a templated notification: the broker renders the template from
     *  `event_type` + `variables` (template render landed broker-side). Delegates to
     *  the raw `SendNotification` — exactly one RPC. Returns the SendNotification
     *  response (carries the log ids). */
    sendTemplate(eventType, recipientId, variables = {}, extra = {}, call) {
        return this.send({ event_type: eventType, recipient_id: recipientId, variables, ...extra }, call);
    }
    /** Retry a FAILED notification log by id. One `RetryNotification` RPC. */
    retryFailed(logId, call) {
        return this.retry(logId, call);
    }
    /**
     * Wait for a notification to reach a terminal delivery status by READING the
     * log via `GetNotification` — bounded by `deadlineMs`, status-driven, NO fixed
     * sleeps between reads beyond a short backoff. Terminal statuses:
     * DELIVERED / FAILED / SUPPRESSED. Returns the terminal log; throws on timeout.
     */
    async waitForDelivery(logId, deadlineMs = 30_000, call) {
        const terminal = new Set(["DELIVERED", "FAILED", "SUPPRESSED"]);
        const deadline = Date.now() + deadlineMs;
        let pollMs = 200;
        // eslint-disable-next-line no-constant-condition
        while (true) {
            const resp = await this.get(logId, call);
            const status = String(resp?.log?.status ?? resp?.status ?? "").toUpperCase();
            if (terminal.has(status))
                return resp;
            if (Date.now() >= deadline) {
                throw new Error(`udb: waitForDelivery(${logId}) timed out in status '${status}'`);
            }
            await new Promise((r) => setTimeout(r, Math.min(pollMs, deadline - Date.now())));
            pollMs = Math.min(pollMs * 2, 2_000);
        }
    }
}
exports.NotificationFacade = NotificationFacade;
/** Thin wrappers over the AnalyticsService generated RPCs. */
class AnalyticsFacade {
    core;
    service;
    constructor(core, service = "udb.core.analytics.services.v1.AnalyticsService") {
        this.core = core;
        this.service = service;
    }
    getThroughput(request = {}, call) {
        return this.core.unary(this.service, "GetThroughput", request, call);
    }
    getPipelineSummary(request = {}, call) {
        return this.core.unary(this.service, "GetPipelineSummary", request, call);
    }
    getSlaCompliance(request = {}, call) {
        return this.core.unary(this.service, "GetSlaCompliance", request, call);
    }
    recordPipelineMetric(request, call) {
        return this.core.unary(this.service, "RecordPipelineMetric", request, call);
    }
    triggerSnapshot(request = {}, call) {
        return this.core.unary(this.service, "TriggerSnapshot", request, call);
    }
}
exports.AnalyticsFacade = AnalyticsFacade;
/**
 * Storage facade. The PRIMARY surface is the native StorageService file
 * lifecycle (register/finalize an upload, mint a download URL, get/update/delete/
 * list file records). The pre-existing DataBroker object-byte IO (streamed
 * put/get + presigned URL) is retained as escape-hatch helpers so Wave-1 callers
 * keep working: `putObject` (client stream), `getObject` (server stream) and
 * `presign`.
 */
class StorageFacade {
    core;
    service;
    objectCore;
    objectService;
    constructor(core, 
    /** StorageService full-name (file lifecycle). */
    service = "udb.core.storage.services.v1.StorageService", objectCore = core, 
    /** DataBroker full-name (raw object-byte IO). */
    objectService = "udb.services.v1.DataBroker") {
        this.core = core;
        this.service = service;
        this.objectCore = objectCore;
        this.objectService = objectService;
    }
    /** The request with the connected tenant filled in when it names none. The broker
     *  requires tenant_id on these calls; an explicit one is never overridden. */
    withTenant(request) {
        const tenantId = this.core.opts?.meta?.tenantId;
        return request.tenant_id || !tenantId ? request : { ...request, tenant_id: tenantId };
    }
    // ── StorageService (primary, file lifecycle) ──────────────────────────────
    /** Begin an upload: reserve a file id + (optionally) a presigned target
     *  (`RegisterUploadRequest` → `RegisterUploadResponse`). */
    registerUpload(request, call) {
        return this.core.unary(this.service, "RegisterUpload", request, call);
    }
    /** Finalize a previously-registered upload, committing the file record
     *  (`FinalizeUploadRequest` → `FinalizeUploadResponse`). */
    finalizeUpload(request, call) {
        return this.core.unary(this.service, "FinalizeUpload", request, call);
    }
    /** Mint a time-limited download URL for a stored file
     *  (`GetDownloadUrlRequest` → `GetDownloadUrlResponse`). Pass either a full
     *  request or a `file_id` string. */
    getDownloadUrl(request, call) {
        const req = typeof request === "string" ? { file_id: request } : request;
        return this.core.unary(this.service, "GetDownloadUrl", this.withTenant(req), call);
    }
    /**
     * Canonical download accessor (naming contract): by DEFAULT mints a
     * time-limited download URL for `fileId` (presigned HTTP — no object bytes
     * traverse the broker). Emits EXACTLY one `GetDownloadUrl` RPC in this mode —
     * NO streaming fallback / GetFile probe. Extra `opts` fields are merged into
     * the request. Alias of {@link getDownloadUrl} with a `file_id`-first signature.
     *
     * Pass `{ stream: true }` to instead pull the raw bytes over the new
     * `StorageService.DownloadFile` server-streaming RPC (for clients without
     * presigned-HTTP access). In that mode it returns the reassembled
     * `Uint8Array` from {@link downloadFileBytes}; the `stream` key is stripped
     * from the request and the remaining `opts` are forwarded (e.g.
     * `chunk_size_bytes`). The presigned path is unchanged and remains the default.
     */
    downloadFile(fileId, opts = {}, call) {
        const { stream, ...rest } = opts;
        if (stream)
            return this.downloadFileBytes(fileId, rest, call);
        return this.getDownloadUrl({ file_id: fileId, ...rest }, call);
    }
    /**
     * Streaming download: pull the raw file bytes over
     * `StorageService.DownloadFile` (server-streaming `DownloadFileChunk`s) and
     * reassemble them into a single `Uint8Array`. Emits EXACTLY one `DownloadFile`
     * RPC — no presigned URL, no GetFile probe; the object bytes flow through the
     * broker. Extra `opts` fields are merged into the `DownloadFileRequest` (e.g.
     * `chunk_size_bytes`). Use this when presigned HTTP is unavailable; prefer the
     * presigned default ({@link downloadFile}) otherwise.
     */
    async downloadFileBytes(fileId, opts = {}, call) {
        const stream = this.core.serverStream(this.service, "DownloadFile", this.withTenant({ file_id: fileId, ...opts }), call);
        const parts = [];
        let total = 0;
        for await (const chunk of stream) {
            const data = chunk?.data;
            if (data && data.length) {
                parts.push(data);
                total += data.length;
            }
        }
        const out = new Uint8Array(total);
        let off = 0;
        for (const p of parts) {
            out.set(p, off);
            off += p.length;
        }
        return out;
    }
    /** Fetch a file record by id (or full `GetFileRequest`). */
    getFile(request, call) {
        const req = typeof request === "string" ? { file_id: request } : request;
        return this.core.unary(this.service, "GetFile", this.withTenant(req), call);
    }
    /** Update mutable file metadata (`UpdateFileRequest` → `UpdateFileResponse`). */
    updateFile(request, call) {
        return this.core.unary(this.service, "UpdateFile", request, call);
    }
    /** Delete a file record (and its object) by id (or full `DeleteFileRequest`). The
     *  connected tenant is filled in when the request names none. */
    deleteFile(request, call) {
        const req = typeof request === "string" ? { file_id: request } : request;
        return this.core.unary(this.service, "DeleteFile", this.withTenant(req), call);
    }
    /** Delete a file with an explicit mode: `DELETE_MODE_SOFT` keeps the metadata
     *  tombstoned and removes the bytes best-effort, `DELETE_MODE_HARD` records a durable
     *  object-GC intent. `reason` is recorded on a hard delete. */
    deleteFileMode(fileId, mode, reason = "", call) {
        return this.deleteFile({ file_id: fileId, mode, reason }, call);
    }
    /** List file records (`ListFilesRequest` → `ListFilesResponse`). */
    listFiles(request = {}, call) {
        return this.core.unary(this.service, "ListFiles", request, call);
    }
    /**
     * Composite upload helper: RegisterUpload → HTTP PUT bytes to the presigned
     * `upload_url` → FinalizeUpload. EXACTLY three honest network ops (one gRPC
     * register, one plain HTTP PUT, one gRPC finalize) — NO hidden GetFile /
     * ListFiles / GetDownloadUrl. The broker owns object placement; `upload_url`
     * is the canonical bytes target.
     *
     * Throws when the broker returns an empty `upload_url` (degraded presign — the
     * broker populates `RegisterUploadResponse.error` in that case).
     */
    async uploadFile(fileName, bytes, opts = {}) {
        const contentType = opts.contentType ?? "application/octet-stream";
        const registered = await this.registerUpload({
            file_name: fileName,
            content_type: contentType,
            ...(opts.fileType ? { file_type: opts.fileType } : {}),
            ...(opts.register ?? {}),
        }, opts.call);
        const uploadUrl = registered?.upload_url ?? "";
        const fileId = registered?.file_id ?? "";
        if (!uploadUrl) {
            const reason = registered?.error?.message || registered?.error?.code || "upload_url unavailable";
            throw new Error(`udb: storage.uploadFile: presign degraded (${reason})`);
        }
        // Plain HTTP PUT of the bytes (NOT gRPC) to the presigned target. Uses Node's
        // http/https so the typing is deterministic (global `fetch`'s BodyInit type is
        // lib-dependent) and the SDK pulls in no extra HTTP dependency.
        const body = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
        await (opts.putFn ?? httpPut)(uploadUrl, contentType, body);
        return this.finalizeUpload({
            file_id: fileId,
            size_bytes: body.length,
            ...(opts.checksum ? { checksum: opts.checksum } : {}),
            ...(opts.finalize ?? {}),
        }, opts.call);
    }
    // ── DataBroker object-byte IO (retained escape hatches) ───────────────────
    /** Open a client-streaming upload. Write `Chunk`s to `stream`, then await
     *  `response` for the `MutationResponse`. */
    putObject(call) {
        return this.objectCore.clientStream(this.objectService, "PutObject", call);
    }
    /** Open a server-streaming download; consume the `Chunk`s with `for await`. */
    getObject(request, call) {
        return this.objectCore.serverStream(this.objectService, "GetObject", request, call);
    }
    /** Mint a presigned URL on the DataBroker object surface
     *  (`UrlRequest` → `UrlResponse`). */
    presign(request, call) {
        return this.objectCore.unary(this.objectService, "GeneratePresignedUrl", request, call);
    }
    /** @deprecated Use {@link presign}. Retained for Wave-1 compatibility. */
    generatePresignedUrl(request, call) {
        return this.presign(request, call);
    }
}
exports.StorageFacade = StorageFacade;
/** Thin wrappers over the AssetService generated RPCs (pipeline definitions,
 *  asset registration, pipeline runs + step completion). */
class AssetFacade {
    core;
    service;
    constructor(core, service = "udb.core.asset.services.v1.AssetService") {
        this.core = core;
        this.service = service;
    }
    /** Create a reusable pipeline definition
     *  (`CreatePipelineDefinitionRequest` → `…Response`). */
    createPipelineDefinition(request, call) {
        return this.core.unary(this.service, "CreatePipelineDefinition", request, call);
    }
    /** Fetch a pipeline definition by id (or full request). */
    getPipelineDefinition(request, call) {
        const req = typeof request === "string" ? { definition_id: request } : request;
        return this.core.unary(this.service, "GetPipelineDefinition", req, call);
    }
    /** Register an asset (`RegisterAssetRequest` → `RegisterAssetResponse`). */
    registerAsset(request, call) {
        return this.core.unary(this.service, "RegisterAsset", request, call);
    }
    /** Start a pipeline run for an asset
     *  (`StartPipelineRequest` → `StartPipelineResponse`). */
    startPipeline(request, call) {
        return this.core.unary(this.service, "StartPipeline", request, call);
    }
    /** Fetch a running/finished pipeline instance by id (or full request). */
    getPipeline(request, call) {
        const req = typeof request === "string" ? { instance_id: request } : request;
        return this.core.unary(this.service, "GetPipeline", req, call);
    }
    /** Mark a pipeline step complete (`CompleteStepRequest` → `…Response`). */
    completeStep(request, call) {
        return this.core.unary(this.service, "CompleteStep", request, call);
    }
    /** List assets (`ListAssetsRequest` → `ListAssetsResponse`). */
    listAssets(request = {}, call) {
        return this.core.unary(this.service, "ListAssets", request, call);
    }
    /** Fetch an asset by id (or full `GetAssetRequest`). */
    getAsset(request, call) {
        const req = typeof request === "string" ? { asset_id: request } : request;
        return this.core.unary(this.service, "GetAsset", req, call);
    }
    /** Define a reusable pipeline with a typed step list. Wraps
     *  `CreatePipelineDefinition` (steps marshalled to JSON). One RPC. */
    definePipeline(name, steps, extra = {}, call) {
        return this.createPipelineDefinition({ name, steps: JSON.stringify(steps), ...extra }, call);
    }
    /** Register an asset bound to an existing storage file id. Wraps
     *  `RegisterAsset`. One RPC. */
    registerFromStorageFile(fileId, name, mediaType, extra = {}, call) {
        return this.registerAsset({ file_id: fileId, name, media_type: mediaType, ...extra }, call);
    }
    /**
     * Start a pipeline and wait for it to reach a terminal state. Issues exactly
     * ONE `StartPipeline` and reads the INLINE `steps` from its response (lane 04's
     * `StartPipelineResponse.steps`) — NO immediate `GetPipeline` proof read. Then
     * polls instance status via `GetPipeline` (reads only, bounded by `deadlineMs`,
     * no fixed sleeps) until terminal: COMPLETED / FAILED / CANCELLED.
     */
    async startAndWait(request, deadlineMs = 60_000, call) {
        const started = await this.startPipeline(request, call);
        const steps = started?.steps ?? [];
        const instanceId = started?.instance_id ?? started?.instance?.instance_id ?? "";
        const terminal = new Set(["COMPLETED", "FAILED", "CANCELLED"]);
        const deadline = Date.now() + deadlineMs;
        let pollMs = 200;
        let final = started;
        if (instanceId) {
            // eslint-disable-next-line no-constant-condition
            while (true) {
                final = await this.getPipeline(instanceId, call);
                const status = String(final?.instance?.status ?? final?.status ?? "").toUpperCase();
                if (terminal.has(status))
                    break;
                if (Date.now() >= deadline) {
                    throw new Error(`udb: startAndWait(${instanceId}) timed out in status '${status}'`);
                }
                await new Promise((r) => setTimeout(r, Math.min(pollMs, deadline - Date.now())));
                pollMs = Math.min(pollMs * 2, 2_000);
            }
        }
        return { started, steps, final };
    }
}
exports.AssetFacade = AssetFacade;
/** WebRTC room sub-client (RoomService). */
class WebRtcRoomFacade {
    core;
    service;
    constructor(core, service = "udb.core.webrtc.services.v1.RoomService") {
        this.core = core;
        this.service = service;
    }
    createRoom(request, call) {
        return this.core.unary(this.service, "CreateRoom", request, call);
    }
    getRoom(request, call) {
        const req = typeof request === "string" ? { room_id: request } : request;
        return this.core.unary(this.service, "GetRoom", req, call);
    }
    updateRoom(request, call) {
        return this.core.unary(this.service, "UpdateRoom", request, call);
    }
    closeRoom(request, call) {
        const req = typeof request === "string" ? { room_id: request } : request;
        return this.core.unary(this.service, "CloseRoom", req, call);
    }
    listRooms(request = {}, call) {
        return this.core.unary(this.service, "ListRooms", request, call);
    }
}
exports.WebRtcRoomFacade = WebRtcRoomFacade;
/** WebRTC peer sub-client (PeerService). */
class WebRtcPeerFacade {
    core;
    service;
    constructor(core, service = "udb.core.webrtc.services.v1.PeerService") {
        this.core = core;
        this.service = service;
    }
    joinRoom(request, call) {
        return this.core.unary(this.service, "JoinRoom", request, call);
    }
    /** Atomic join: returns `{ peer, existing_peers, ice_servers, expires_at }` in
     *  ONE RPC (replaces the JoinRoom + IssueCredentials two-round-trip path). */
    joinSession(request, call) {
        return this.core.unary(this.service, "JoinSession", request, call);
    }
    leaveRoom(request, call) {
        return this.core.unary(this.service, "LeaveRoom", request, call);
    }
    getPeer(request, call) {
        const req = typeof request === "string" ? { peer_id: request } : request;
        return this.core.unary(this.service, "GetPeer", req, call);
    }
    listPeers(request = {}, call) {
        return this.core.unary(this.service, "ListPeers", request, call);
    }
}
exports.WebRtcPeerFacade = WebRtcPeerFacade;
/** WebRTC track sub-client (TrackService). */
class WebRtcTrackFacade {
    core;
    service;
    constructor(core, service = "udb.core.webrtc.services.v1.TrackService") {
        this.core = core;
        this.service = service;
    }
    publishTrack(request, call) {
        return this.core.unary(this.service, "PublishTrack", request, call);
    }
    unpublishTrack(request, call) {
        return this.core.unary(this.service, "UnpublishTrack", request, call);
    }
    muteTrack(request, call) {
        return this.core.unary(this.service, "MuteTrack", request, call);
    }
    listTracks(request = {}, call) {
        return this.core.unary(this.service, "ListTracks", request, call);
    }
}
exports.WebRtcTrackFacade = WebRtcTrackFacade;
/** WebRTC TURN sub-client (TurnService). */
class WebRtcTurnFacade {
    core;
    service;
    constructor(core, service = "udb.core.webrtc.services.v1.TurnService") {
        this.core = core;
        this.service = service;
    }
    /** Issue ephemeral TURN credentials
     *  (`IssueCredentialsRequest` → `IssueCredentialsResponse`). */
    issueCredentials(request, call) {
        return this.core.unary(this.service, "IssueCredentials", request, call);
    }
}
exports.WebRtcTurnFacade = WebRtcTurnFacade;
/** Migration lifecycle helpers over the DataBroker migration RPCs. */
class MigrationsFacade {
    core;
    service;
    constructor(core, service = "udb.services.v1.DataBroker") {
        this.core = core;
        this.service = service;
    }
    /** Plan a migration (`PlanMigration`). */
    plan(request = {}, call) {
        return this.core.unary(this.service, "PlanMigration", request, call);
    }
    /** Approve a planned migration (`ApproveMigrationPlan`). The response body
     *  carries `approval_token` (06.1.1.1) — read it from the BODY, not a header. */
    approve(request, call) {
        return this.core.unary(this.service, "ApproveMigrationPlan", request, call);
    }
    /** Apply a migration (`ApplyMigration`). */
    apply(request, call) {
        return this.core.unary(this.service, "ApplyMigration", request, call);
    }
    /**
     * Plan → (optionally Approve) → Apply the current migration. Reads the approval
     * token from the approve RESPONSE BODY (`approveResp.approval_token`, 06.1.1.1)
     * — NOT from an `onResponseMetadata` header callback. Emits exactly Plan →
     * Approve → Apply (Approve skipped when `autoApprove` is false).
     *
     * NOTE: the broker keeps emitting the `x-udb-approval-token` header for one
     * compat release; this helper deliberately uses the body field and ignores the
     * header.
     */
    async applyCurrent(opts = {}) {
        const planResp = await this.plan(opts.plan ?? {}, opts.call);
        const planId = planResp?.plan_id ?? planResp?.run_id ?? "";
        let approvalToken = "";
        if (opts.autoApprove) {
            const approveResp = await this.approve({ plan_id: planId }, opts.call);
            approvalToken = approveResp?.approval_token ?? "";
        }
        return this.apply({ plan_id: planId, approval_token: approvalToken, dry_run: opts.dryRun ?? false }, opts.call);
    }
}
exports.MigrationsFacade = MigrationsFacade;
/** Admin facade: groups admin-only control surfaces (currently migrations). */
class AdminFacade {
    migrations;
    constructor(core) {
        this.migrations = new MigrationsFacade(core);
    }
}
exports.AdminFacade = AdminFacade;
/**
 * Events facade over the DataBroker CDC surface (`PublishCDC` server-stream +
 * `EnqueueOutboxEvent`). Provides a sleep-free ready/ack boundary:
 * `subscribe(topic).ready()` resolves on the first server signal, and
 * `publishAndWait(...)` enqueues then awaits the matching envelope on a live
 * subscription — never a fixed sleep. The subscription is tenant-scoped.
 */
class EventsFacade {
    core;
    meta;
    service;
    constructor(core, meta, service = "udb.services.v1.DataBroker") {
        this.core = core;
        this.meta = meta;
        this.service = service;
    }
    /** Open a tenant-scoped `PublishCDC` subscription for `topic`. */
    subscribe(topic, call) {
        const request = {
            context: { tenant_id: this.meta.tenantId, project_id: this.meta.projectId ?? "" },
            topic,
        };
        const stream = this.core.serverStream(this.service, "PublishCDC", request, call);
        const ready = () => new Promise((resolve, reject) => {
            let settled = false;
            const done = (fn) => {
                if (settled)
                    return;
                settled = true;
                fn();
            };
            stream.once("metadata", () => done(resolve));
            stream.once("data", () => done(resolve));
            stream.once("error", (err) => done(() => reject(err.udb ?? err)));
            stream.once("end", () => done(() => reject(new Error("udb: CDC stream ended before ready"))));
        });
        return { stream, ready, cancel: () => stream.cancel() };
    }
    /**
     * Enqueue an outbox event then await the matching `CDCEnvelope` on a live
     * subscription. Issues exactly ONE `EnqueueOutboxEvent`; resolves on the first
     * envelope where `match(envelope)` is true. No sleeps; bounded by `deadlineMs`.
     */
    async publishAndWait(topic, payload, match, deadlineMs = 30_000, call) {
        const sub = this.subscribe(topic, call);
        try {
            await sub.ready();
            await this.core.unary(this.service, "EnqueueOutboxEvent", {
                context: { tenant_id: this.meta.tenantId, project_id: this.meta.projectId ?? "" },
                topic,
                payload,
            }, call);
            return await new Promise((resolve, reject) => {
                const timer = setTimeout(() => {
                    sub.cancel();
                    reject(new Error(`udb: publishAndWait(${topic}) timed out`));
                }, deadlineMs);
                if (typeof timer.unref === "function")
                    timer.unref();
                sub.stream.on("data", (envelope) => {
                    // A progress marker is not an event; never offer it to `match`.
                    if (envelope?.progress_only)
                        return;
                    if (match(envelope)) {
                        clearTimeout(timer);
                        resolve(envelope);
                    }
                });
                sub.stream.on("error", (err) => {
                    clearTimeout(timer);
                    reject(err.udb ?? err);
                });
                sub.stream.on("end", () => {
                    clearTimeout(timer);
                    reject(new Error(`udb: CDC stream ended before a matching envelope for ${topic}`));
                });
            });
        }
        finally {
            sub.cancel();
        }
    }
}
exports.EventsFacade = EventsFacade;
/**
 * WebRTC facade: namespaces the four unary WebRTC services plus a bidi signalling
 * helper. The raw per-service sub-clients are reachable as `.room / .peer /
 * .track / .turn`.
 */
class WebRtcFacade {
    core;
    signalingService;
    room;
    peer;
    track;
    turn;
    constructor(core, signalingService = "udb.core.webrtc.services.v1.SignalingService") {
        this.core = core;
        this.signalingService = signalingService;
        this.room = new WebRtcRoomFacade(core);
        this.peer = new WebRtcPeerFacade(core);
        this.track = new WebRtcTrackFacade(core);
        this.turn = new WebRtcTurnFacade(core);
    }
    /**
     * Open the bidirectional signalling stream (SignalingService.Signal). Write
     * `SignalRequest`s to the returned duplex and read `SignalResponse`s from it
     * (`for await … of` / `.on('data', …)`). Like every stream in the SDK this is
     * never auto-retried; open a fresh stream to reconnect. Errors are tagged on
     * the stream as `(err as any).udb` (a `UdbError`).
     */
    signal(call) {
        return this.core.bidiStream(this.signalingService, "Signal", call);
    }
    /**
     * Join a WebRTC session atomically: ONE `PeerService.JoinSession` RPC (peer +
     * ICE + existing peers in one call — no JoinRoom + IssueCredentials fan-out),
     * then open the bidi `Signal` stream and drive a heartbeat. Returns a session
     * handle whose `leave()` closes the signaling stream and issues `LeaveRoom`.
     *
     * Guardrail: reconnect is denied after the first observed signaling response
     * (until the broker exposes a resume token), so this opens exactly one stream
     * and never auto-reopens. The heartbeat writes a `{ heartbeat }` SignalRequest
     * on an interval; it is cleared on `leave()`.
     */
    async joinSession(roomId, opts = {}) {
        const request = { room_id: roomId };
        if (opts.tenantId)
            request.tenant_id = opts.tenantId;
        if (opts.displayName)
            request.display_name = opts.displayName;
        if (opts.metadata)
            request.metadata = opts.metadata;
        if (opts.ttlSeconds != null)
            request.ttl_seconds = opts.ttlSeconds;
        const joined = await this.peer.joinSession(request, opts.call);
        const peerId = joined?.peer?.peer_id ?? joined?.peer?.id ?? "";
        const stream = this.signal(opts.call);
        const heartbeatMs = opts.heartbeatMs ?? 15_000;
        let heartbeat = null;
        if (heartbeatMs > 0) {
            heartbeat = setInterval(() => {
                try {
                    stream.write({ heartbeat: { peer_id: peerId, room_id: roomId } });
                }
                catch {
                    /* stream closed — leave() clears the timer */
                }
            }, heartbeatMs);
            if (typeof heartbeat.unref === "function")
                heartbeat.unref();
        }
        return {
            peer: joined?.peer,
            existingPeers: joined?.existing_peers ?? [],
            iceServers: joined?.ice_servers ?? [],
            expiresAt: joined?.expires_at,
            signal: stream,
            leave: async () => {
                if (heartbeat) {
                    clearInterval(heartbeat);
                    heartbeat = null;
                }
                try {
                    stream.end();
                }
                catch {
                    /* already closed */
                }
                await this.peer.leaveRoom({ room_id: roomId, peer_id: peerId });
            },
        };
    }
}
exports.WebRtcFacade = WebRtcFacade;
// ── The facade ──────────────────────────────────────────────────────────────
/**
 * Single entry point over the UDB data plane and native control plane. Build it
 * with {@link createUdb} (or `Udb.project(config)`):
 *
 * ```ts
 * const udb = createUdb({
 *   target: "localhost:50051",
 *   tenantId: "acme",
 *   purpose: "web",
 *   credentials: { bearerToken: process.env.UDB_TOKEN },
 * });
 *
 * await udb.authz.require({ message_type: "acme.v1.Invoice" }, "read");
 * const rs = await udb.data.select({ message_type: "acme.v1.Invoice", limit: 50 });
 * await udb.notification.send({ event_type: "welcome", recipient_id: "u1" });
 * udb.close();
 * ```
 */
// Background bearer-refresher tuning (parity with the Go enterprise session).
const BG_REFRESH_SKEW_MS = 60_000; // refresh this long before expiry
const BG_REFRESH_MIN_MS = 1_000; // floor between attempts (never busy-loop)
const BG_REFRESH_IDLE_MS = 5 * 60_000; // cadence when the token carries no expiry
const BG_REFRESH_RETRY_MS = 5_000; // cadence after a load error or while poisoned
const BG_REFRESH_MAX_BACKOFF_MS = 60_000; // cap of the exponential failure backoff
const BG_REFRESH_LIFETIME_FRACTION = 0.8; // renew at 80% of a known token lifetime
const RENEWAL_ATTEMPTS = 3; // attempts per renewal before it is reported as failed
const RENEWAL_BACKOFF_MS = 1_000; // first jittered wait between renewal attempts
const RENEWAL_TIMEOUT_MS = 30_000; // bound on one renewal attempt (a lost reply must not hang it)
// How soon after renewing a bearer the broker refused another refusal is taken as
// final rather than as a reason to renew again (renewing cannot help).
const REJECTED_RENEW_INTERVAL_MS = 5_000;
// AuthnService calls are never repeated after a refusal: it is about the
// credential in the request, not the bearer the call carried.
const AUTHN_SERVICE = "udb.core.authn.services.v1.AuthnService";
/** The session can no longer authorize calls: its bearer expired and no renewal
 *  succeeded, or a renewal verified as a different identity. Calls fail locally
 *  with this error instead of reaching the broker with no or a dead bearer. `cause`
 *  is the last renewal failure. Shaped like `UdbError` (`code`, `rpc`, `retryable`). */
class SessionExpiredError extends Error {
    code = grpc.status.UNAUTHENTICATED;
    rpc;
    constructor(cause, rpc = "") {
        const reason = cause instanceof Error ? cause.message : cause ? String(cause) : "no credential can renew it";
        super(`udb: session expired, the bearer could not be renewed: ${reason} (code=UNAUTHENTICATED)`, { cause });
        this.name = "SessionExpiredError";
        this.rpc = rpc;
    }
    get retryable() {
        return false;
    }
}
exports.SessionExpiredError = SessionExpiredError;
/** Exponential backoff with jitter: the n-th failure waits a random time in
 *  [ceiling/2, ceiling], where ceiling = base·2^(n-1) capped at `capMs`. */
function jitteredBackoffMs(failures, baseMs, capMs) {
    const ceiling = Math.min(baseMs * 2 ** Math.max(failures - 1, 0), capMs);
    return Math.round(ceiling / 2 + (Math.random() * ceiling) / 2);
}
const TRANSIENT_GRPC_CODES = new Set([
    grpc.status.DEADLINE_EXCEEDED,
    grpc.status.RESOURCE_EXHAUSTED,
    grpc.status.ABORTED,
    grpc.status.UNAVAILABLE,
]);
/** Whether a failed renewal attempt may succeed when repeated. A gRPC status
 *  decides when present; otherwise only a message that names a refused credential
 *  is final. */
function isTransientRenewalError(err) {
    if (err instanceof CredentialBindingError || err instanceof SessionExpiredError)
        return false;
    const marked = err?.transient;
    if (typeof marked === "boolean")
        return marked;
    const code = err?.code;
    if (typeof code === "number")
        return TRANSIENT_GRPC_CODES.has(code);
    const message = err instanceof Error ? err.message : String(err);
    return !/invalid|revoked|reuse|expired|unauthenticated|permission|denied/i.test(message);
}
/** Whether a failed sign-in may succeed when repeated: the broker was unreachable
 *  or restarting, overloaded, or the reply was lost. A refused credential is final. */
function isTransientLoginError(err) {
    const code = err?.code;
    return typeof code === "number" && TRANSIENT_GRPC_CODES.has(code);
}
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
/** Reject when `work` has not settled within `ms`; the timer never keeps the process alive. */
function withTimeout(work, ms) {
    let timer;
    const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => {
            const err = new Error(`udb: renewal attempt timed out after ${ms}ms`);
            err.code = grpc.status.DEADLINE_EXCEEDED;
            reject(err);
        }, ms);
        timer.unref?.();
    });
    return Promise.race([work, timeout]).finally(() => clearTimeout(timer));
}
/** API keys retained for bearer re-exchange, keyed by project. A WeakMap (not a
 *  field) so the secret never appears in `util.inspect`/JSON dumps of the project. */
const RETAINED_API_KEYS = new WeakMap();
/** Credentials retained for re-login, keyed by project, kept out of the object so the
 *  password never appears in `util.inspect`/JSON dumps of it. */
const RETAINED_LOGINS = new WeakMap();
/** The member-tenant token exchange of a `forTenant` client, keyed by project: one
 *  call returns an Authenticate-shaped reply (`access_token`, `expires_at_unix`,
 *  verified `principal`). The exchanged token has no refresh token; this renews it. */
const RETAINED_EXCHANGES = new WeakMap();
/** A canonical tenant id: a lowercase hyphenated UUID. */
const CANONICAL_TENANT_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
/** A renewal that verified as a different principal or scope set. Renewing again cannot
 *  fix it, so the session fails closed until a new explicit login. */
class CredentialBindingError extends Error {
    constructor(message) {
        super(message);
        this.name = "CredentialBindingError";
    }
}
exports.CredentialBindingError = CredentialBindingError;
/** Whether a call carries its own bearer (`asUser`) instead of the connection's. */
function delegatesBearer(call) {
    return Object.keys(call?.headers ?? {}).some((name) => name.toLowerCase() === "authorization");
}
// Scope ordering and duplicates carry no additional permissions.
function scopeSet(scopes) {
    return [...new Set(Array.isArray(scopes) ? scopes : [])].sort();
}
function credentialPin(principal) {
    return {
        principalId: principal?.principal_id ?? "",
        subject: principal?.subject ?? "",
        accountKind: principal?.account_kind ?? "",
        tenantId: principal?.tenant_id ?? "",
        projectId: principal?.project_id ?? "",
        userId: principal?.user_id ?? "",
        serviceIdentity: principal?.service_identity ?? "",
        scopes: scopeSet(principal?.scopes),
    };
}
/** Refuse a renewal that switched the connected principal or its scope set. */
function checkCredentialPin(pin, principal) {
    if (!principal)
        throw new CredentialBindingError("udb: renewal principal is missing");
    const next = credentialPin(principal);
    const fields = [
        ["principal_id", next.principalId, pin.principalId],
        ["subject", next.subject, pin.subject],
        ["account_kind", next.accountKind, pin.accountKind],
    ];
    for (const [name, actual, expected] of fields) {
        if (actual !== expected)
            throw new CredentialBindingError(`udb: renewal changed ${name}`);
    }
    const identity = [
        ["tenant", next.tenantId, pin.tenantId],
        ["project", next.projectId, pin.projectId],
        ["user", next.userId, pin.userId],
        ["service identity", next.serviceIdentity, pin.serviceIdentity],
        ["scopes", next.scopes.join(","), pin.scopes.join(",")],
    ];
    for (const [name, actual, expected] of identity) {
        if (actual !== expected)
            throw new CredentialBindingError(`udb: renewed principal changed ${name}`);
    }
}
/** How long before expiry a token should be renewed: the larger of 60s and 20% of
 *  its known lifetime. A token too short for the 60s floor (more than half its life)
 *  renews at 80% of its lifetime instead. Same rule as the Go SDK. */
function refreshSkewMs(tok) {
    if (tok && tok.issuedAt && tok.expiresAt > tok.issuedAt) {
        const lifetime = tok.expiresAt - tok.issuedAt;
        const fraction = Math.round(lifetime * (1 - BG_REFRESH_LIFETIME_FRACTION));
        const skew = Math.max(BG_REFRESH_SKEW_MS, fraction);
        return skew > lifetime / 2 ? fraction : skew;
    }
    return BG_REFRESH_SKEW_MS;
}
class UdbProject {
    config;
    /** The shared generated client (escape hatch — raw, typed, per-service RPCs). */
    generated;
    /** The shared robust core (channels, retry, metadata, typed errors). */
    core;
    /** Data plane: the DataBroker service surface, additively augmented with the
     *  bound `table(name)` / `entity(messageType)` accessors so the
     *  `simple_client_code.md` headline form `udb.data.table("invoice").select(...)`
     *  works. The raw generated RPCs (`select`/`upsert`/`delete`/…) stay reachable. */
    data;
    /** Receipt/fence accessors per the naming contract: `udb.metadata.afterWrite(receipt)`
     *  attaches a receipt-derived read fence to exactly the next read. */
    metadata = consistency_1.consistencyMetadata;
    /** Read-your-writes: the latest write's receipt, which the reads of this client's
     *  entity handles, typed tables and stores carry as a read fence for 30 seconds. */
    fence = new consistency_1.SessionFence();
    /** Authentication ergonomics (authenticate*, login/refresh helpers, …). */
    auth;
    /** Authorization ergonomics (can / require / explain / batchCan / nativeAccess),
     *  routed through a TTL `AuthzCache`. */
    authz;
    apikey;
    tenant;
    notification;
    analytics;
    /** Storage: native StorageService file lifecycle (+ retained DataBroker
     *  object-byte IO via `.storage.putObject / getObject / presign`). */
    storage;
    /** Asset management: AssetService pipelines + asset registration. */
    asset;
    /** WebRTC: `.webrtc.room / .peer / .track / .turn` + `.webrtc.signal()`. */
    webrtc;
    /** Events: CDC subscribe(topic).ready() + publishAndWait over DataBroker. */
    events;
    /** Admin: migration lifecycle helpers (`.admin.migrations.applyCurrent`). */
    admin;
    /** Feature flags for the connected tenant and project. */
    flags;
    /** Distributed locks on the control-plane channel. */
    lock;
    /** The tenant code to canonical UUID transition; verified by `connectEnterprise`. */
    tenantState;
    tokenStore;
    refreshInFlight = null;
    /** Consecutive background-refresh failures (drives exponential backoff). */
    refreshFailures = 0;
    /** Background bearer-refresher state (parity with the Go enterprise session): a
     *  timer proactively refreshes before expiry; on a hard failure (expired +
     *  unrefreshable) the bearer is cleared so calls fail CLOSED instead of sending a
     *  dead credential. `poisoned` reflects that state; `lastRefreshError` exposes it. */
    refreshTimer = null;
    poisoned = false;
    lastRefreshError = null;
    closed = false;
    /** The principal the broker verified for the current login or API key. */
    verifiedPrincipal = null;
    /** Epoch ms when the bearer exchanged from an API key expires; 0 when none. */
    exchangedBearerExpiresAt = 0;
    /** The identity this connection's renewals must keep; null when none is pinned. */
    credentialPin = null;
    /** Set once a renewal changed the principal: every later renewal is refused. */
    credentialTerminal = null;
    /** A failed renewal may have spent the refresh token, so the next one logs in. */
    credentialNeedsLogin = false;
    /** The broker refused the stored refresh token and no credential can replace it:
     *  it is never resubmitted. Cleared by a new login or API-key adoption. */
    refreshRejected = null;
    /** Epoch ms before which a call does not start another renewal after a failed one. */
    renewalRetryAt = 0;
    /** An access token the broker refused before its local expiry (a restarted broker
     *  that lost the session, a revoked session): a stored token carrying it is renewed
     *  as if it were stale, and while it cannot be renewed calls are refused locally. */
    rejectedBearer = "";
    /** The last bearer renewed because the broker refused it, and when. */
    lastRejected = "";
    lastRejectedAt = 0;
    /** The last error passed to `onRefreshError` (the same failure is reported once). */
    notifiedRefreshError = null;
    /** Wait between renewal attempts; a field so tests can shorten it. */
    renewalBackoffMs = RENEWAL_BACKOFF_MS;
    /** Bound on one renewal attempt. */
    renewalTimeoutMs = RENEWAL_TIMEOUT_MS;
    /** Lazily built channel for the gRPC health check; closed by `close()`. */
    healthClient = null;
    /** Dedicated generated client for the auth/control-plane target when
     * `authTarget` differs from `target`; otherwise null (native facades share
     * `this.generated`). */
    authGenerated = null;
    /** Dedicated generated client for the WebRTC target, when `webrtcTarget`
     *  differs from both `target` and `authTarget`; otherwise null. */
    webrtcGenerated = null;
    /** The single UdbMetadata instance shared by reference across every sub-client
     *  core (data, auth, native, webrtc). Mutating it (e.g. via setTenant) updates
     *  the x-tenant-id / x-* headers on ALL outbound channels at once. */
    sharedMeta;
    /**
     * Canonical construction entry point (naming contract): build a
     * {@link UdbProject} from a single shared config. Async-shaped so the
     * `simple_client_code.md` headline `await UdbProject.connect({...})` reads as a
     * connect; construction itself is synchronous (channels connect lazily), so no
     * extra round trip is performed. `createUdb()` / `Udb.project()` remain as
     * (synchronous) aliases.
     */
    static async connect(config) {
        const apiKey = config.credentials?.apiKey;
        if (apiKey && config.credentials?.bearerToken) {
            throw new Error("udb: credentials.apiKey and credentials.bearerToken are mutually exclusive");
        }
        const udb = new UdbProject(config);
        if (apiKey) {
            try {
                await udb.authenticateApiKeyAndAdopt(apiKey);
            }
            catch (err) {
                udb.close();
                throw err;
            }
        }
        return udb;
    }
    /**
     * Read the environment contract (see `configFromEnv`), connect, and check the
     * verified principal against `expect`. The client carries an exchanged,
     * self-refreshing bearer when an API key was configured. A principal that does
     * not match closes the connection and throws.
     */
    static async connectFromEnv(prefix = "", expect = {}) {
        const udb = await UdbProject.connect((0, connectEnv_1.configFromEnv)(prefix));
        try {
            udb.verify(expect);
        }
        catch (err) {
            udb.close();
            throw err;
        }
        return udb;
    }
    /**
     * One-call enterprise setup — the parity of the Go SDK's `ConnectEnterprise`.
     * Connect, log in with username/password, verify the freshly-minted bearer, and
     * adopt the canonical tenant/project from the VERIFIED principal, then start a
     * background refresher that keeps the bearer fresh and fails closed if it can no
     * longer be renewed.
     *
     * Pass the human tenant CODE as `config.tenantCode` or `config.tenantId` (e.g.
     * `"acme"`); it is used as the pre-login hint and replaced by the canonical tenant
     * UUID after login — so every later call sends the UUID that row-level security
     * compares, never the code. A principal without a canonical lowercase tenant UUID
     * is refused. A refresh the broker refuses leads to a fresh login with these
     * credentials; a renewal that verifies as a different principal or scope set fails
     * the session closed, with no retry. Returns a session ready for tenant-scoped work: check a record with
     * `validateTenant` before a tenant-scoped write, and call `close()` to stop the
     * refresher and channels.
     */
    static async connectEnterprise(config) {
        const { username, password, tenantCode, ...rest } = config;
        if (!rest.target || !username || !password) {
            throw new Error("udb: connectEnterprise requires target, username, and password");
        }
        const code = tenantCode ?? rest.tenantId ?? "";
        const udb = new UdbProject({ ...rest, tenantId: code || "default" });
        udb.tenantState.tenantCode = code;
        try {
            // A broker that is unreachable or restarting gets up to RENEWAL_ATTEMPTS
            // sign-in attempts with jittered backoff, the same policy as the
            // service-account key exchange. A refused credential is final.
            for (let attempt = 0;; attempt += 1) {
                if (attempt > 0) {
                    await sleep(jitteredBackoffMs(attempt, udb.renewalBackoffMs ?? RENEWAL_BACKOFF_MS, BG_REFRESH_MAX_BACKOFF_MS));
                }
                try {
                    await udb.loginAndAdoptTenant({
                        username,
                        password,
                        tenant_hint: code,
                        project_hint: rest.projectId ?? "",
                    });
                    break;
                }
                catch (err) {
                    if (attempt + 1 >= RENEWAL_ATTEMPTS || !isTransientLoginError(err))
                        throw err;
                }
            }
            const principal = udb.verifiedPrincipal;
            if (!principal)
                throw new Error("udb: connectEnterprise: no principal returned");
            udb.tenantState.adopt(principal.tenant_id ?? "");
            udb.sharedMeta.userId = principal.user_id ?? "";
            udb.sharedMeta.serviceIdentity = principal.service_identity ?? "";
            udb.sharedMeta.scopes = [...(principal.scopes ?? [])];
            udb.credentialPin = credentialPin(principal);
            udb.retainLogin({ username, password, tenant_hint: code, project_hint: rest.projectId ?? "" });
        }
        catch (err) {
            udb.close();
            throw err;
        }
        udb.startBackgroundRefresh();
        return udb;
    }
    constructor(config) {
        this.config = config;
        const meta = metaFromConfig(config);
        this.sharedMeta = meta;
        const authTarget = (config.authTarget ?? config.target).trim();
        this.generated = new generatedClient_1.UdbGeneratedClient({
            target: config.target,
            meta,
            secure: config.secure,
            tls: config.tls,
            bearerToken: config.credentials?.bearerToken,
            apiKey: undefined,
            deadlineMs: config.deadlineMs,
            retry: config.retry,
            protoRoot: config.protoRoot,
            channelOptions: config.channelOptions,
        });
        this.core = this.generated.core;
        if (authTarget !== config.target) {
            this.authGenerated = new generatedClient_1.UdbGeneratedClient({
                target: authTarget,
                meta,
                secure: config.secure,
                tls: config.tls,
                bearerToken: config.credentials?.bearerToken,
                apiKey: undefined,
                deadlineMs: config.deadlineMs,
                retry: config.retry,
                protoRoot: config.protoRoot,
                channelOptions: config.channelOptions,
            });
        }
        const nativeCore = this.authGenerated?.core ?? this.core;
        // Augment the raw DataBroker surface with the bound table/entity accessors
        // (additive — the generated RPCs are untouched) so the headline form
        // `udb.data.table("invoice").select({ where })` works. Bound to `this`.
        const dataPlane = this.generated.DataBroker;
        dataPlane.entity = (messageType, opts) => this.entity(messageType, opts);
        dataPlane.table = (name, opts) => this.table(name, opts);
        this.data = dataPlane;
        const protoRoot = config.protoRoot ?? (0, protoRoot_1.defaultProtoRoot)();
        const authClient = new auth_1.UdbAuthClient(authTarget, meta, protoRoot, config.policyBundleSecret, {
            secure: config.secure,
            tls: config.tls,
            channelOptions: config.channelOptions,
        });
        this.auth = authClient;
        this.authz = new AuthzFacade(authClient);
        this.apikey = new ApiKeyFacade(nativeCore);
        this.tenant = new TenantFacade(nativeCore);
        this.notification = new NotificationFacade(nativeCore);
        this.analytics = new AnalyticsFacade(nativeCore);
        this.storage = new StorageFacade(nativeCore, undefined, this.core);
        this.asset = new AssetFacade(nativeCore);
        // Route WebRTC through control-plane by default; only open a dedicated
        // channel when an explicit webrtcTarget differs from both existing targets.
        const webrtcTarget = config.webrtcTarget?.trim() || authTarget;
        let webrtcCore = webrtcTarget === config.target ? this.core : nativeCore;
        if (webrtcTarget !== config.target && webrtcTarget !== authTarget) {
            this.webrtcGenerated = new generatedClient_1.UdbGeneratedClient({
                target: webrtcTarget,
                meta,
                secure: config.secure,
                tls: config.tls,
                bearerToken: config.credentials?.bearerToken,
                apiKey: undefined,
                deadlineMs: config.deadlineMs,
                retry: config.retry,
                protoRoot: config.protoRoot,
                channelOptions: config.channelOptions,
            });
            webrtcCore = this.webrtcGenerated.core;
        }
        this.webrtc = new WebRtcFacade(webrtcCore);
        // Events ride the data-plane DataBroker channel, tenant-scoped via sharedMeta.
        this.events = new EventsFacade(this.core, this.sharedMeta);
        // Migration admin rides the data-plane DataBroker channel.
        this.admin = new AdminFacade(this.core);
        this.flags = new facades_1.FlagsFacade(nativeCore, this.sharedMeta);
        this.lock = new facades_1.LockFacade(nativeCore, this.sharedMeta);
        this.tenantState = new tenantState_1.TenantState(config.tenantId);
        this.tokenStore = config.tokenStore ?? new MemoryTokenStore();
        // Every call on the connection's own credential first renews a bearer past its
        // renewal point, and fails locally once the session can no longer be renewed.
        this.installCredentialGate(this.core);
        if (this.authGenerated)
            this.installCredentialGate(this.authGenerated.core);
        if (this.webrtcGenerated)
            this.installCredentialGate(this.webrtcGenerated.core);
        this.auth.setCallGate(() => this.ensureCallCredential(), (sent, err) => this.bearerRejected(sent, err));
    }
    /** The core native-service calls ride: the control-plane target when it differs
     *  from the data target. Identity and credentials match `core`. */
    get nativeCore() {
        return this.authGenerated?.core ?? this.core;
    }
    /** The verified canonical tenant UUID ("" until `connectEnterprise` adopted it).
     *  Use this, never the human code, in tenant-scoped records and filters. */
    get canonicalTenantId() {
        return this.tenantState.canonicalId;
    }
    /** The project of the verified principal ("" when there is none). */
    get canonicalProjectId() {
        return this.verifiedPrincipal?.project_id ?? "";
    }
    /** Fail fast, naming both values, if `recordTenantId` differs from the verified
     *  canonical tenant. Call it before a tenant-scoped write. Throws when the tenant
     *  was never verified. */
    validateTenant(recordTenantId) {
        this.tenantState.validateTenantId(recordTenantId);
    }
    /** A copy of the principal the broker verified for this connection's API key or
     *  login; null when there is none (a raw bearer, or before connecting). */
    principal() {
        return this.verifiedPrincipal === null || this.verifiedPrincipal === undefined
            ? null
            : structuredClone(this.verifiedPrincipal);
    }
    /** When the current bearer exchanged from an API key expires; null when the
     *  connection does not exchange one. A login session's expiry is on `currentToken()`. */
    bearerExpiresAt() {
        return this.exchangedBearerExpiresAt ? new Date(this.exchangedBearerExpiresAt) : null;
    }
    /** The current "Bearer <token>" credential ("" when none), for callers that build
     *  their own metadata. */
    bearer() {
        const token = this.sharedMeta.bearerToken;
        return token ? `Bearer ${token}` : "";
    }
    /** Why this connection's credential is not current: it was closed or its background
     *  refresh is failing. Null while it is current. */
    credentialError() {
        if (this.closed)
            return new Error("udb: credential owner is closed");
        return this.lastRefreshError ?? null;
    }
    /** Check the connected principal against `expect`, reporting every mismatch at once.
     *  It needs a verified principal: a connection made with a raw bearer has none and
     *  fails here if anything is expected. */
    verify(expect = {}) {
        (0, connectEnv_1.verifyPrincipal)(this.verifiedPrincipal, expect);
    }
    /** Resolve when the broker is serving and this connection's credential is current:
     *  the standard gRPC health check on the broker connection plus `credentialError`.
     *  Use it as a readiness probe; it needs no admin scope. The check is bounded by
     *  `deadlineMs`, else the client's deadline, else 5 seconds. */
    async ready(options = {}) {
        const failure = this.credentialError();
        if (failure)
            throw new Error(`udb: credential refresh is failing: ${failure.message}`, { cause: failure });
        if (!this.healthClient) {
            const { secure, tls, target, channelOptions } = this.config;
            const credentials = tls
                ? grpc.credentials.createSsl(tls.rootCerts, tls.privateKey, tls.certChain)
                : secure
                    ? grpc.credentials.createSsl()
                    : grpc.credentials.createInsecure();
            this.healthClient = new grpc.Client(target, credentials, channelOptions);
        }
        await (0, connectEnv_1.checkBrokerHealth)(this.healthClient, (0, client_1.metadata)(this.sharedMeta), {
            deadlineMs: options.deadlineMs ?? this.config.deadlineMs ?? 5_000,
            signal: options.signal,
        });
    }
    /** Call options that carry the end user's bearer instead of this service's own, for
     *  operations the broker must authorize as that user. Pass them to any generated or
     *  facade call. The token may carry the "Bearer " prefix. The broker verifies it; the
     *  connected tenant and project do not change, so the user must belong to them.
     *  Throws when this connection's own credential can no longer be refreshed. */
    asUser(bearer, call) {
        if (this.closed || this.poisoned) {
            const cause = this.credentialError();
            throw new Error(`udb: bearer refresh failed, session not authorized${cause ? `: ${cause.message}` : ""}`, { cause });
        }
        return (0, facades_1.asUserCall)(bearer, call);
    }
    /** Check a user's bearer token (with or without the "Bearer " prefix). An invalid or
     *  expired token throws `NotFoundError`. It runs with this connection's credential,
     *  so pass `asUser(token)` as `call` when the service lacks token-validation scope. */
    validateSession(token, call) {
        return (0, facades_1.validateSessionOn)(this.nativeCore, token, call);
    }
    /** The currently stored token, if any. */
    currentToken() {
        return this.tokenStore.load();
    }
    /**
     * Username/password login via AuthnService.Login. On success the JWT (or
     * session) is persisted in the token store. Returns the raw `LoginResponse`.
     * If `mfa_required` comes back true, the caller must re-call `login` with the
     * second factor; nothing is stored in that case.
     */
    async login(request) {
        const resp = await this.auth.login(request);
        if (!resp?.mfa_required && (resp?.access_token || resp?.session_token)) {
            const expiresIn = Number(resp?.access_token_expires_in ?? 0);
            await this.tokenStore.save({
                accessToken: resp.access_token || resp.session_token || "",
                refreshToken: resp.refresh_token || undefined,
                sessionId: resp.session_id || undefined,
                expiresAt: expiresIn > 0 ? Date.now() + expiresIn * 1000 : 0,
            });
            // A password session supersedes any API-key identity: stop re-exchanging it.
            RETAINED_API_KEYS.delete(this);
            this.exchangedBearerExpiresAt = 0;
            this.forgetCredentialPin();
            this.resetRenewalState();
            this.applyCredentials(resp.access_token || resp.session_token || "");
        }
        return resp;
    }
    /** Adopt the canonical tenant id on EVERY outbound channel. After a password
     *  login with a human tenant code (`tenant_hint: "acme"`), the broker mints a
     *  bearer whose tenant claim is the canonical tenant UUID; subsequent native
     *  RPCs reject a mismatched `x-tenant-id`. Call this with the resolved
     *  `principal.tenant_id` so the shared metadata sends the UUID, not the code. */
    setTenant(tenantId) {
        this.sharedMeta.tenantId = tenantId;
    }
    /** A typed entity handle bound to a DataBroker message type. Hides
     *  `record_json`/`conflict_fields`/Struct-filter/decode. The key (and optional
     *  tenant/project field names) come from `opts` or, when omitted, from the
     *  generated `ENTITY_REGISTRY` (lane 07's descriptor-driven catalog) — falling
     *  back to `["id"]`. No catalog round-trip at construction. */
    entity(messageType, opts) {
        const binding = generatedClient_1.ENTITY_REGISTRY[messageType];
        const resolved = {
            key: opts?.key ?? binding?.key ?? ["id"],
            tenantField: opts?.tenantField ?? binding?.tenantField,
            projectField: opts?.projectField ?? binding?.projectField,
        };
        const context = {
            tenant_id: this.sharedMeta.tenantId,
            project_id: this.sharedMeta.projectId ?? "",
        };
        return new entity_1.EntityHandle(this.core, messageType, resolved, context, this.fence);
    }
    /** The typed store for one entity message; its key comes from the annotations. */
    tableOf(messageType, opts = {}) {
        return new stores_1.Table(this.core, messageType, { fence: this.fence, ...opts });
    }
    /** Run `body` and commit everything it collected atomically: every write and
     *  every event commits together or none does. `body` throwing commits nothing. */
    tx(body, opts = {}) {
        return (0, stores_1.tx)(this.core, body, {
            correlationId: this.sharedMeta.correlationId,
            fence: this.fence,
            ...opts,
        });
    }
    /** Deliver events on `topicPattern` to `handle` as the durable consumer `name`,
     *  resuming where it left off. See {@link consume}. */
    consume(name, topicPattern, handle, opts = {}) {
        return (0, stores_1.consume)(this.core, name, topicPattern, handle, opts);
    }
    /** Thin table-shaped alias over {@link entity}. Resolves the message type from
     *  a registry binding whose `table` matches `name` (else uses `name` as the
     *  message type); caller-supplied `opts.key` wins, default `["id"]`. Forwards to
     *  the same DataBroker Upsert/Select/Delete RPCs. */
    table(name, opts) {
        const binding = Object.values(generatedClient_1.ENTITY_REGISTRY).find((b) => b.table === name);
        const messageType = binding?.messageType ?? name;
        return this.entity(messageType, {
            key: opts?.key ?? binding?.key ?? ["id"],
        });
    }
    /**
     * Exchange a native API key (AuthnService.Authenticate) for a short-lived
     * bearer, adopt the verified principal, and keep the bearer alive: the key is
     * retained privately (never logged) and the background refresher re-exchanges
     * it at ~80% of the bearer lifetime. The key is never sent as a header: if the
     * broker mints no access token (no JWT signing key configured) the exchange
     * fails, and if re-exchange keeps failing past bearer expiry calls fail closed
     * until a re-exchange succeeds.
     */
    async authenticateApiKeyAndAdopt(apiKey = this.config.credentials?.apiKey ?? "") {
        if (!apiKey)
            throw new Error("udb: api key is required");
        // An explicit adoption chooses the identity; re-exchanges must then keep it.
        this.forgetCredentialPin();
        this.resetRenewalState();
        const { verified } = await this.exchangeApiKey(apiKey);
        if (!this.closed) {
            if (this.refreshTimer)
                this.scheduleRefresh();
            else
                this.startBackgroundRefresh();
        }
        return verified;
    }
    /** One API-key → bearer exchange: adopt principal, persist + install the bearer.
     *  Shared by the initial connect and the background re-exchange. */
    async exchangeApiKey(apiKey) {
        // The key travels in the request; an expired bearer must not ride along.
        const verified = await this.renewalAuth().authenticateApiKey(apiKey);
        return this.adoptExchangedBearer(verified, "exchange API key", "udb: the broker returned no access token for the API key; configure a JWT signing key on the broker (API keys are exchanged for a bearer, never sent raw)", () => RETAINED_API_KEYS.set(this, apiKey));
    }
    /** One member-tenant token exchange of a `forTenant` client (see `forTenant`). */
    async exchangeTenantToken() {
        const exchange = RETAINED_EXCHANGES.get(this);
        if (!exchange)
            throw new Error("udb: this client has no tenant token exchange");
        const verified = await exchange();
        return this.adoptExchangedBearer(verified, "exchange the service token for tenant", "udb: the broker returned no member-tenant token", () => undefined);
    }
    /** Check, adopt, persist and install a bearer obtained by an exchange (an API key,
     *  or a member-tenant token). The principal is checked against the pin before
     *  anything is adopted, so a refused renewal changes no identity. */
    async adoptExchangedBearer(verified, label, noTokenMessage, retain) {
        if (this.credentialPin)
            this.checkRenewal(verified?.principal);
        else if (verified?.access_token && !verified?.principal) {
            throw new Error(`udb: ${label}: the broker returned no verified principal`);
        }
        retain();
        const token = verified?.access_token ?? "";
        const principal = verified?.principal ?? {};
        const tenantId = principal?.tenant_id ?? "";
        if (tenantId)
            this.setTenant(tenantId);
        const projectId = principal?.project_id ?? "";
        if (projectId)
            this.sharedMeta.projectId = projectId;
        const userId = principal?.user_id ?? principal?.principal_id ?? "";
        if (userId)
            this.sharedMeta.userId = userId;
        const serviceIdentity = principal?.service_identity ?? "";
        if (serviceIdentity)
            this.sharedMeta.serviceIdentity = serviceIdentity;
        if (Array.isArray(principal?.scopes))
            this.sharedMeta.scopes = [...principal.scopes];
        if (!token) {
            // No JWT signing key on the broker. An API key is only ever exchanged for a
            // bearer; it is never sent as a raw header, so there is nothing to fall back to.
            await this.tokenStore.clear();
            this.clearBearerCredentials();
            throw new Error(noTokenMessage);
        }
        const now = Date.now();
        const expiresAtUnix = Number(verified?.expires_at_unix ?? 0);
        const next = {
            accessToken: token,
            issuedAt: now,
            expiresAt: expiresAtUnix > 0 ? expiresAtUnix * 1000 : 0,
        };
        await this.tokenStore.save(next);
        this.applyCredentials(token);
        this.exchangedBearerExpiresAt = next.expiresAt;
        if (verified?.principal) {
            this.verifiedPrincipal = verified.principal;
            this.credentialPin ??= credentialPin(verified.principal);
        }
        return { verified, token: next };
    }
    /** Check a renewed principal against the pin and the identity now on the wire. A
     *  mismatch fails this connection closed at once, whatever the bearer's expiry. */
    checkRenewal(principal) {
        try {
            checkCredentialPin(this.credentialPin, principal);
            const tenantId = principal.tenant_id ?? "";
            if (tenantId && tenantId !== this.sharedMeta?.tenantId) {
                throw new CredentialBindingError("udb: renewed principal changed tenant");
            }
        }
        catch (err) {
            this.credentialTerminal = err;
            this.lastRefreshError = err;
            this.poisoned = true;
            this.clearBearerCredentials();
            throw err;
        }
    }
    forgetCredentialPin() {
        this.credentialPin = null;
        this.credentialTerminal = null;
        this.credentialNeedsLogin = false;
        RETAINED_LOGINS.delete(this);
    }
    /** The auth client renewal calls use: no expired or revoked bearer rides along,
     *  no call gate runs, and every call has a deadline. */
    renewalAuth() {
        const deadlineMs = this.config?.deadlineMs || RENEWAL_TIMEOUT_MS / 2;
        return this.auth.withoutCredentials?.({ deadlineMs }) ?? this.auth;
    }
    /** Verify a renewed bearer through the broker, check it against the pin, then store
     *  and install it. Nothing is stored or installed for a bearer that fails. */
    async publishVerifiedToken(next) {
        const verified = await this.renewalAuth().authenticateBearer(next.accessToken);
        this.checkRenewal(verified?.principal);
        const expiresAtUnix = Number(verified?.expires_at_unix ?? 0);
        const published = expiresAtUnix > 0 ? { ...next, expiresAt: expiresAtUnix * 1000 } : next;
        if (!published.accessToken || (published.expiresAt > 0 && Date.now() >= published.expiresAt)) {
            throw new Error("udb: renewal returned an empty or expired bearer");
        }
        await this.tokenStore.save(published);
        this.verifiedPrincipal = verified.principal;
        this.applyCredentials(published.accessToken);
        return published;
    }
    async refreshVerified(current) {
        const issuedAt = Date.now();
        const resp = await this.renewalAuth().refreshToken({
            refresh_token: current.refreshToken ?? "",
            session_id: current.sessionId ?? "",
        });
        const expiresIn = Number(resp?.access_token_expires_in ?? 0);
        return this.publishVerifiedToken({
            accessToken: resp?.access_token ?? "",
            refreshToken: resp?.refresh_token || current.refreshToken,
            sessionId: current.sessionId,
            issuedAt,
            expiresAt: expiresIn > 0 ? issuedAt + expiresIn * 1000 : 0,
        });
    }
    async reloginVerified(credentials) {
        const issuedAt = Date.now();
        const resp = await this.renewalAuth().login(credentials);
        const accessToken = resp?.access_token || resp?.session_token || "";
        if (resp?.mfa_required || !accessToken)
            throw new Error("udb: re-login returned no bearer");
        const expiresIn = Number(resp?.access_token_expires_in ?? 0);
        return this.publishVerifiedToken({
            accessToken,
            refreshToken: resp?.refresh_token || undefined,
            sessionId: resp?.session_id || undefined,
            issuedAt,
            expiresAt: expiresIn > 0 ? issuedAt + expiresIn * 1000 : 0,
        });
    }
    /** Refresh a session that holds no other credential (a plain `login`). */
    async refreshPlain(current) {
        const resp = await this.renewalAuth().refreshToken({
            refresh_token: current.refreshToken ?? "",
            session_id: current.sessionId ?? "",
        });
        const expiresIn = Number(resp?.access_token_expires_in ?? 0);
        const now = Date.now();
        const next = {
            accessToken: resp?.access_token || current.accessToken,
            // The broker rotates the refresh token on every refresh: the one
            // presented is single-use, so keeping it fails the NEXT refresh with
            // "invalid credential". The reply omits it only for a legacy
            // session-id refresh, where the stored credential stays valid.
            refreshToken: resp?.refresh_token || current.refreshToken,
            // session_public_id is the display id, not the legacy refresh
            // session id this field holds, so the stored one is kept.
            sessionId: current.sessionId,
            issuedAt: expiresIn > 0 ? now : current.issuedAt,
            expiresAt: expiresIn > 0 ? now + expiresIn * 1000 : 0,
        };
        await this.tokenStore.save(next);
        this.applyCredentials(next.accessToken);
        return next;
    }
    /** One renewal attempt. The refresh token is tried first unless an earlier
     *  failure may have spent it; when it fails, a session that holds a durable
     *  credential (API key, or the username/password it connected with)
     *  re-authenticates with it in the same attempt. A refresh token whose refresh
     *  failed is never submitted again by such a session: the broker may have
     *  consumed it before the reply was lost. A session with no other credential
     *  retries the refresh only after a transient failure; a refused refresh token
     *  is final for it. */
    async renewOnce(current) {
        const login = RETAINED_LOGINS.get(this);
        const apiKey = RETAINED_API_KEYS.get(this);
        const exchange = RETAINED_EXCHANGES.get(this);
        const durable = Boolean(login || apiKey || exchange);
        const hasRefresh = Boolean(current.refreshToken || current.sessionId);
        if (!durable && this.refreshRejected)
            throw this.refreshRejected;
        let refreshFailure = null;
        if (hasRefresh && !this.credentialNeedsLogin) {
            try {
                return login ? await this.refreshVerified(current) : await this.refreshPlain(current);
            }
            catch (err) {
                if (err instanceof CredentialBindingError)
                    throw err;
                if (!durable) {
                    if (!isTransientRenewalError(err)) {
                        this.refreshRejected = err instanceof Error ? err : new Error(String(err));
                    }
                    throw err;
                }
                refreshFailure = err;
                this.credentialNeedsLogin = true;
            }
        }
        else if (this.credentialNeedsLogin) {
            refreshFailure = new Error("udb: prior refresh outcome requires re-login");
        }
        if (!durable)
            return current; // Nothing can renew this token; callers see it expire.
        try {
            const fresh = apiKey
                ? (await this.exchangeApiKey(apiKey)).token
                : exchange
                    ? (await this.exchangeTenantToken()).token
                    : await this.reloginVerified(login);
            this.credentialNeedsLogin = false;
            return fresh;
        }
        catch (err) {
            if (err instanceof CredentialBindingError || refreshFailure === null)
                throw err;
            const first = refreshFailure instanceof Error ? refreshFailure.message : String(refreshFailure);
            const second = err instanceof Error ? err.message : String(err);
            const combined = new Error(`udb: renewal and re-login failed: ${first}; ${second}`, { cause: err });
            // Whether another attempt can help depends on the re-authentication, not the refresh.
            combined.transient = isTransientRenewalError(err);
            throw combined;
        }
    }
    /** Renew with up to RENEWAL_ATTEMPTS attempts, waiting a jittered, doubling
     *  interval between them. Only a transient failure is retried; a changed
     *  identity or a refused credential ends the renewal at once. */
    async renewWithRetry(skewMs) {
        let lastErr = new Error("udb: renewal did not run");
        for (let attempt = 0; attempt < RENEWAL_ATTEMPTS; attempt += 1) {
            if (attempt > 0) {
                await sleep(jitteredBackoffMs(attempt, this.renewalBackoffMs ?? RENEWAL_BACKOFF_MS, BG_REFRESH_MAX_BACKOFF_MS));
            }
            // Another caller may have finished a renewal since this one read the store.
            const current = await this.tokenStore.load();
            if (!current)
                return null;
            if (!this.isRejected(current) && (current.expiresAt === 0 || Date.now() + skewMs < current.expiresAt)) {
                return current;
            }
            try {
                return await withTimeout(this.renewOnce(current), this.renewalTimeoutMs ?? RENEWAL_TIMEOUT_MS);
            }
            catch (err) {
                lastErr = err;
                if (!isTransientRenewalError(err))
                    break;
            }
        }
        throw lastErr;
    }
    /** Run one renewal and record its outcome once for every caller sharing it:
     *  success clears the failure state; failure records the error, sets the backoff
     *  before the next call-driven attempt, fails closed when the bearer has expired,
     *  and reports the error to `onRefreshError`. */
    async trackRenewal(skewMs) {
        try {
            const tok = await this.renewWithRetry(skewMs);
            this.refreshFailures = 0;
            this.renewalRetryAt = 0;
            if (tok && !this.isRejected(tok) && (tok.expiresAt === 0 || Date.now() < tok.expiresAt)) {
                this.poisoned = false;
                this.lastRefreshError = null;
                this.notifiedRefreshError = null;
            }
            return tok;
        }
        catch (err) {
            const failure = err instanceof Error ? err : new Error(String(err));
            this.lastRefreshError = failure;
            this.refreshFailures = (this.refreshFailures ?? 0) + 1;
            this.renewalRetryAt = Date.now() + jitteredBackoffMs(this.refreshFailures, BG_REFRESH_MIN_MS, BG_REFRESH_MAX_BACKOFF_MS);
            const tok = await Promise.resolve(this.tokenStore.load()).catch(() => null);
            if (!tok || this.isRejected(tok) || (tok.expiresAt > 0 && Date.now() >= tok.expiresAt)) {
                // Fail closed: drop the dead (or refused) bearer so no call goes out with it.
                this.poisoned = true;
                this.clearBearerCredentials();
            }
            this.notifyRefreshError(failure);
            throw failure;
        }
    }
    notifyRefreshError(err) {
        if (err === this.notifiedRefreshError)
            return;
        this.notifiedRefreshError = err;
        try {
            this.config?.onRefreshError?.(err);
        }
        catch {
            /* a reporting callback must not change the session */
        }
    }
    /** Clear the renewal failure state when a new credential is adopted. */
    resetRenewalState() {
        this.refreshRejected = null;
        this.renewalRetryAt = 0;
        this.refreshFailures = 0;
        this.poisoned = false;
        this.lastRefreshError = null;
        this.notifiedRefreshError = null;
    }
    /** Before a call on the connection's own credential: renew a bearer that is past
     *  its renewal point (sharing the single in-flight renewal with every concurrent
     *  caller), and fail locally with `SessionExpiredError` when the bearer has
     *  expired and cannot be renewed, instead of sending no or a dead bearer. */
    async ensureCallCredential(call) {
        if (this.closed)
            return;
        if (this.credentialTerminal)
            throw new SessionExpiredError(this.credentialTerminal);
        if (delegatesBearer(call)) {
            // asUser: the caller's bearer is sent; the owner's refusal still applies.
            if (this.poisoned)
                throw new SessionExpiredError(this.lastRefreshError ?? undefined);
            return;
        }
        const tok = await this.tokenStore.load();
        if (!tok)
            return;
        // A bearer the broker refused is renewed whatever its expiry. While it cannot
        // be renewed the call is refused here; the background refresher retries with
        // backoff, so a refused credential never turns into a sign-in per call.
        const rejected = this.isRejected(tok);
        if (rejected && this.poisoned)
            throw new SessionExpiredError(this.lastRefreshError ?? undefined);
        if (!rejected && !tok.expiresAt)
            return;
        const skew = refreshSkewMs(tok);
        if (!rejected && Date.now() + skew < tok.expiresAt)
            return;
        if (!this.refreshInFlight && (this.renewalRetryAt ?? 0) > Date.now()) {
            // A renewal just failed; the background refresher owns the next attempt.
            if (!rejected && Date.now() < tok.expiresAt)
                return;
            throw new SessionExpiredError(this.lastRefreshError ?? undefined);
        }
        let renewed;
        try {
            renewed = await this.refreshIfNeeded(skew);
        }
        catch (err) {
            if (!rejected && !(err instanceof CredentialBindingError) && Date.now() < tok.expiresAt)
                return;
            throw new SessionExpiredError(err);
        }
        if (!renewed || this.isRejected(renewed) || (renewed.expiresAt > 0 && Date.now() >= renewed.expiresAt)) {
            throw new SessionExpiredError(this.lastRefreshError ?? undefined);
        }
    }
    /** Whether the broker refused `tok`'s access token before its local expiry. */
    isRejected(tok) {
        return Boolean(this.rejectedBearer) && tok?.accessToken === this.rejectedBearer;
    }
    /** The broker answered UNAUTHENTICATED to a call that carried `sent`. Returns true
     *  when `sent` is the bearer this connection owns (a `connectEnterprise` session,
     *  an exchanged API key) and it is now marked for renewal, so the call may be made
     *  once more with the renewed bearer. The broker refuses authentication before any
     *  handler runs, so the repeat cannot apply a write twice. A bearer the connection
     *  does not own (a configured bearer, an `asUser` bearer) is never renewed, and a
     *  bearer renewed moments ago that is refused again is not renewed again. */
    bearerRejected(sent, err) {
        if (!sent || err?.code !== grpc.status.UNAUTHENTICATED)
            return false;
        // A local refusal never reached the broker.
        if (err instanceof SessionExpiredError)
            return false;
        if (this.closed || this.credentialTerminal)
            return false;
        if (!RETAINED_LOGINS.has(this) && !RETAINED_API_KEYS.has(this) && !RETAINED_EXCHANGES.has(this))
            return false;
        // Already marked by a concurrent call; its renewal is shared.
        if (sent === this.lastRejected)
            return true;
        // Not the owner's bearer, or one an ordinary refresh already replaced.
        if (sent !== this.sharedMeta?.bearerToken)
            return false;
        if (this.lastRejectedAt && Date.now() - this.lastRejectedAt < REJECTED_RENEW_INTERVAL_MS)
            return false;
        this.lastRejected = sent;
        this.lastRejectedAt = Date.now();
        this.rejectedBearer = sent;
        // Wake the background refresher, so a renewal that fails is retried with backoff.
        if (this.refreshTimer)
            this.armTimer(0);
        return true;
    }
    /** The synchronous check streaming calls get: refuse a session already known dead. */
    assertCallCredential() {
        if (this.closed)
            return;
        if (this.credentialTerminal)
            throw new SessionExpiredError(this.credentialTerminal);
        if (this.poisoned)
            throw new SessionExpiredError(this.lastRefreshError ?? undefined);
    }
    /** Gate every call a core makes on the connection's credential. */
    installCredentialGate(core) {
        const unary = core.unary.bind(core);
        core.unary = (async (fullName, method, request, call) => {
            await this.ensureCallCredential(call);
            const sent = delegatesBearer(call) ? "" : (core.opts?.bearerToken ?? "");
            try {
                return await unary(fullName, method, request, call);
            }
            catch (err) {
                // The broker refused the owned bearer this call carried (a restarted broker,
                // a revoked session): renew it and make the call exactly once more.
                if (fullName === AUTHN_SERVICE || !this.bearerRejected(sent, err))
                    throw err;
                await this.ensureCallCredential(call);
                return unary(fullName, method, request, call);
            }
        });
        for (const name of ["serverStream", "clientStream", "bidiStream"]) {
            const original = core[name].bind(core);
            core[name] = (...args) => {
                this.assertCallCredential();
                return original(...args);
            };
        }
    }
    /**
     * A client that acts in the MEMBER tenant `tenantId` (and `projectId`; empty lets
     * the membership choose) with a token exchanged from this client's bearer through
     * `AuthnService.ExchangeServiceTokenForTenant`, for a service connected to its
     * home tenant that holds an approved membership in another. The exchanged token
     * is short-lived and has no refresh token, so the member client exchanges again
     * before it expires, and at once when the broker refuses it early; while that
     * re-exchange is failing its calls are refused locally (`SessionExpiredError`)
     * and the background refresher retries with backoff. A token for any other
     * tenant or project than the one asked for is refused, never adopted. The member
     * client opens its own channels to the same targets; close it before (or with)
     * this client, whose bearer every re-exchange uses. `tenantId` must be the
     * tenant's canonical lowercase UUID.
     */
    async forTenant(tenantId, projectId = "") {
        const tenant = (tenantId ?? "").trim();
        const project = (projectId ?? "").trim();
        if (!CANONICAL_TENANT_UUID.test(tenant)) {
            throw new Error(`udb: forTenant: tenant "${tenantId}" is not a canonical (lowercase hyphenated) UUID`);
        }
        if (this.closed)
            throw new Error("udb: forTenant: this client is closed");
        // The member identity comes from the exchanged token's verified principal.
        const { credentials: _credentials, tokenStore: _tokenStore, userId: _userId, serviceIdentity: _serviceIdentity, scopes: _scopes, ...shared } = this.config;
        const member = new UdbProject({ ...shared, tenantId: tenant, projectId: project || undefined });
        const label = `udb: exchange the service token for tenant ${tenant}`;
        RETAINED_EXCHANGES.set(member, async () => {
            const resp = await this.auth.exchangeServiceTokenForTenant({
                target_tenant_id: tenant,
                target_project_id: project,
                requested_scopes: [],
            });
            if (!resp?.access_token || !Number(resp?.expires_at_unix ?? 0)) {
                throw new Error(`${label}: the broker returned no token (does the service have an ACTIVE membership in that tenant?)`);
            }
            if ((resp.tenant_id ?? "") !== tenant) {
                throw new CredentialBindingError(`${label}: the broker returned a token for tenant "${resp.tenant_id ?? ""}"`);
            }
            if (project && (resp.project_id ?? "") !== project) {
                throw new CredentialBindingError(`${label}: the broker returned a token for project "${resp.project_id ?? ""}", want "${project}"`);
            }
            const verified = await member.renewalAuth().authenticateBearer(resp.access_token);
            const principal = verified?.principal;
            if (principal && (principal.tenant_id ?? "") !== tenant) {
                throw new CredentialBindingError(`${label}: the token authenticates in tenant "${principal.tenant_id ?? ""}"`);
            }
            return { access_token: resp.access_token, expires_at_unix: resp.expires_at_unix, principal };
        });
        try {
            await member.exchangeTenantToken();
            member.tenantState.adopt(member.sharedMeta.tenantId);
        }
        catch (err) {
            member.close();
            throw err;
        }
        member.startBackgroundRefresh();
        return member;
    }
    /** Keep the username/password a session connected with, for re-login. */
    retainLogin(credentials) {
        RETAINED_LOGINS.set(this, credentials);
    }
    /**
     * Login, then ALWAYS authenticate the freshly-minted bearer and adopt the
     * canonical tenant/project from the VERIFIED principal. Canonical D11 sequence
     * (always 2 RPCs): `Login → AuthenticateBearer(token)` then adopt
     * `{tenant_id, project_id}` from the bearer-verified principal.
     *
     * There is NO "skip authenticate when the login response already carried a
     * principal" shortcut — the adopted identity must come from the broker-verified
     * bearer, never an unverified login-response field. Credential install + tenant
     * adoption happen as one logical step so no interleaved RPC observes the old
     * tenant header. When `mfa_required` comes back, returns early (nothing
     * adopted, no second factor stored).
     */
    async loginAndAdoptTenant(request) {
        const resp = await this.login(request);
        if (resp?.mfa_required)
            return resp;
        const token = resp?.access_token || resp?.session_token || "";
        if (!token)
            return resp;
        // login() already installed credentials; authenticate-bearer verifies them.
        const verified = await this.auth.authenticateBearer(token);
        const principal = verified?.principal ?? {};
        this.verifiedPrincipal = verified?.principal ?? null;
        const tenantId = principal?.tenant_id ?? "";
        if (tenantId)
            this.setTenant(tenantId);
        const projectId = principal?.project_id ?? "";
        if (projectId)
            this.sharedMeta.projectId = projectId;
        return resp;
    }
    /** Push a refreshed bearer into every outbound channel, clearing raw API-key metadata. */
    applyCredentials(bearerToken) {
        if (this.sharedMeta) {
            this.sharedMeta.bearerToken = bearerToken;
            this.sharedMeta.apiKey = undefined;
        }
        this.core.setCredentials({ bearerToken, apiKey: undefined });
        this.authGenerated?.core.setCredentials({ bearerToken, apiKey: undefined });
        this.auth.setCredentials({ bearerToken, apiKey: undefined });
        this.webrtcGenerated?.core.setCredentials({ bearerToken, apiKey: undefined });
    }
    /** Remove the active bearer and raw API-key metadata from every outbound channel. */
    clearBearerCredentials() {
        if (this.sharedMeta) {
            this.sharedMeta.bearerToken = undefined;
            this.sharedMeta.apiKey = undefined;
        }
        this.core.setCredentials({ bearerToken: undefined, apiKey: undefined });
        this.authGenerated?.core.setCredentials({ bearerToken: undefined, apiKey: undefined });
        this.auth.setCredentials({ bearerToken: undefined, apiKey: undefined });
        this.webrtcGenerated?.core.setCredentials({ bearerToken: undefined, apiKey: undefined });
    }
    /**
     * Refresh the access token via AuthnService.RefreshToken when it is within
     * `skewMs` of expiry (default 60s). Concurrent callers share ONE in-flight
     * refresh (single-flight) so a burst of requests triggers a single RPC. The
     * refreshed token is written back to the store and returned. Returns the
     * existing token unchanged when no refresh is needed; returns null when there
     * is no stored token / refresh token.
     *
     * A bearer obtained from an API key carries no refresh token; it is renewed by
     * re-exchanging the retained API key (same single-flight). A session that holds
     * an API key or the username/password it connected with re-authenticates with
     * it when a refresh fails, and never resubmits that refresh token. A renewal
     * makes up to three attempts with jittered backoff before it fails; the failure
     * is reported to `onRefreshError`.
     */
    async refreshIfNeeded(skewMs = 60_000) {
        if (this.credentialTerminal)
            throw this.credentialTerminal;
        const current = await this.tokenStore.load();
        if (!current)
            return null;
        const fresh = !this.isRejected(current) && (current.expiresAt === 0 || Date.now() + skewMs < current.expiresAt);
        if (fresh)
            return current;
        if (!this.refreshInFlight) {
            this.refreshInFlight = (async () => {
                try {
                    return await this.trackRenewal(skewMs);
                }
                finally {
                    this.refreshInFlight = null;
                }
            })();
        }
        return this.refreshInFlight;
    }
    /**
     * Start the background bearer refresher: it wakes shortly before the access
     * token's expiry and refreshes it (sharing `refreshIfNeeded`'s single-flight),
     * so no call pays the RefreshToken round-trip. On a hard failure — an expired
     * token that can no longer be refreshed — it clears the active bearer so
     * subsequent calls fail CLOSED (Unauthenticated) instead of sending a dead
     * credential. Idempotent; stopped by `close()`. `connectEnterprise` calls it
     * automatically. Retrieve the last failure with `refreshError()`.
     */
    startBackgroundRefresh() {
        if (this.refreshTimer || this.closed)
            return;
        this.scheduleRefresh();
    }
    /** The most recent background bearer-refresh error, or null if the last refresh
     *  succeeded. Useful for health checks / logging. */
    refreshError() {
        return this.lastRefreshError;
    }
    scheduleRefresh() {
        if (this.closed)
            return;
        if (this.refreshTimer) {
            clearTimeout(this.refreshTimer);
            this.refreshTimer = null;
        }
        void Promise.resolve(this.tokenStore.load())
            .then((tok) => {
            let delay = BG_REFRESH_IDLE_MS;
            if (tok && tok.expiresAt > 0) {
                delay = tok.expiresAt - refreshSkewMs(tok) - Date.now();
                if (delay < BG_REFRESH_MIN_MS)
                    delay = BG_REFRESH_MIN_MS;
            }
            // A bearer the broker refused is renewed as soon as the backoff allows.
            const rejected = this.isRejected(tok);
            if (rejected)
                delay = BG_REFRESH_MIN_MS;
            // Jittered exponential backoff across consecutive failures (1s, 2s, 4s …
            // 60s), recorded by the failed renewal, so a failing refresh/re-exchange is
            // retried without hot-looping and many clients do not retry in lockstep.
            const failures = this.refreshFailures ?? 0;
            if (failures > 0) {
                const backoff = Math.max((this.renewalRetryAt ?? 0) - Date.now(), jitteredBackoffMs(failures, BG_REFRESH_MIN_MS, BG_REFRESH_MAX_BACKOFF_MS) / 2);
                if (delay < backoff)
                    delay = backoff;
            }
            // While poisoned, back off from the 1s floor so a permanently-dead token
            // doesn't hot-loop the refresh RPC.
            if (this.poisoned && !rejected && delay < BG_REFRESH_RETRY_MS)
                delay = BG_REFRESH_RETRY_MS;
            this.armTimer(delay);
        })
            .catch(() => this.armTimer(BG_REFRESH_RETRY_MS));
    }
    armTimer(delayMs) {
        if (this.closed)
            return;
        // Two overlapping scheduleRefresh() calls must not leave an orphan timer.
        if (this.refreshTimer)
            clearTimeout(this.refreshTimer);
        this.refreshTimer = setTimeout(() => {
            void this.backgroundRefreshTick();
        }, delayMs);
        // Never keep the process alive just for the refresh timer.
        this.refreshTimer.unref?.();
    }
    async backgroundRefreshTick() {
        try {
            const before = await this.tokenStore.load();
            // The renewal records its own outcome (failure count, poison, callback).
            await this.refreshIfNeeded(refreshSkewMs(before));
            const tok = await this.tokenStore.load();
            if (tok && !this.isRejected(tok) && (tok.expiresAt === 0 || Date.now() < tok.expiresAt) && !this.refreshRejected) {
                this.refreshFailures = 0;
                this.poisoned = false;
                this.lastRefreshError = null;
            }
        }
        catch (err) {
            if (err !== this.lastRefreshError) {
                // A failure outside the renewal itself (e.g. the token store).
                this.lastRefreshError = err instanceof Error ? err : new Error(String(err));
                this.refreshFailures = (this.refreshFailures ?? 0) + 1;
            }
        }
        finally {
            this.scheduleRefresh();
        }
    }
    /** Clear the stored token (local only; does not call Logout). */
    async logout() {
        await this.tokenStore.clear();
        RETAINED_API_KEYS.delete(this);
        RETAINED_EXCHANGES.delete(this);
        this.verifiedPrincipal = null;
        this.exchangedBearerExpiresAt = 0;
        this.forgetCredentialPin();
        this.resetRenewalState();
        this.clearBearerCredentials();
    }
    /** Close the shared channels (and the dedicated WebRTC channel, if any), and
     *  stop the background bearer refresher. */
    close() {
        this.closed = true;
        if (this.refreshTimer) {
            clearTimeout(this.refreshTimer);
            this.refreshTimer = null;
        }
        RETAINED_API_KEYS.delete(this);
        RETAINED_LOGINS.delete(this);
        RETAINED_EXCHANGES.delete(this);
        this.healthClient?.close();
        this.healthClient = null;
        this.generated.close();
        this.authGenerated?.close();
        this.webrtcGenerated?.close();
    }
}
exports.UdbProject = UdbProject;
/**
 * Authorization facade: routes can/require/explain through a TTL `AuthzCache`
 * (so repeated checks within the cache window don't re-hit the server), while
 * batchCan / nativeAccess / getPolicyBundle go straight to the auth client.
 */
class AuthzFacade {
    client;
    cache;
    constructor(client) {
        this.client = client;
        this.cache = new auth_1.AuthzCache(client);
    }
    /** `[allowed, decision]` for (resource, action, purpose), cache-routed. */
    can(resource, action, purpose = "") {
        return this.cache.can(resource, action, purpose);
    }
    /** Throws `UdbAuthzDenied` on deny; returns the allowing decision. */
    require(resource, action, purpose = "") {
        return this.cache.require(resource, action, purpose);
    }
    /** Full decision without throwing. */
    explain(resource, action, purpose = "") {
        return this.cache.explain(resource, action, purpose);
    }
    /** Batch `(object, action)` checks in one RPC (not cached). */
    batchCan(checks) {
        return this.client.batchCan(checks);
    }
    /**
     * Grant a role permission on a resource/action. Emits EXACTLY one
     * `AuthzService.CreatePolicyRule` RPC (effect = ALLOW) — NO hidden List/Get.
     * `role` becomes the policy `subject`, `resource` the `object`, `action` the
     * `action`. Tenant/project default to the bound metadata.
     */
    allowRole(role, grant, extra = {}) {
        return this.client.createPolicyRule({
            subject: role,
            object: grant.resource,
            action: grant.action,
            effect: grant.effect ?? "ALLOW",
            ...extra,
        });
    }
    /**
     * Bind a subject (user/principal) to a role. Emits EXACTLY one
     * `AuthzService.AssignRole` RPC — NO hidden List/Get. `subject` is sent as both
     * `user_id` and `principal_id`; `role` as `role_id`. Tenant/domain default to
     * the bound metadata.
     */
    bindRole(subject, role, extra = {}) {
        return this.client.assignRole({
            user_id: subject,
            principal_id: subject,
            role_id: role,
            ...extra,
        });
    }
    /** Authorize and, when allowed, return the native-access grant. */
    nativeAccess(resource, action, purpose = "") {
        return this.client.nativeAccess(resource, action, purpose);
    }
    /** Fetch the signed policy bundle for local authorization caches. */
    getPolicyBundle() {
        return this.client.getPolicyBundle();
    }
    /** Drop the local decision cache (e.g. after a policy change). */
    invalidate() {
        this.cache.invalidate();
    }
}
exports.AuthzFacade = AuthzFacade;
/** Build a {@link UdbProject} from a single shared config. */
function createUdb(config) {
    return new UdbProject(config);
}
/** Namespaced alias: `Udb.project(config)` mirrors `createUdb(config)`. */
exports.Udb = {
    project(config) {
        return new UdbProject(config);
    },
};
