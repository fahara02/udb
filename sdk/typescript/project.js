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
exports.Udb = exports.AuthzFacade = exports.UdbProject = exports.WebRtcFacade = exports.EventsFacade = exports.AdminFacade = exports.MigrationsFacade = exports.WebRtcTurnFacade = exports.WebRtcTrackFacade = exports.WebRtcPeerFacade = exports.WebRtcRoomFacade = exports.AssetFacade = exports.StorageFacade = exports.AnalyticsFacade = exports.NotificationFacade = exports.TenantFacade = exports.ApiKeyFacade = exports.MemoryTokenStore = void 0;
exports.createUdb = createUdb;
const http = __importStar(require("http"));
const https = __importStar(require("https"));
const auth_1 = require("./auth");
const client_1 = require("./client");
const consistency_1 = require("./consistency");
const entity_1 = require("./entity");
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
        clientCatalogVersion: client_1.UDB_PROTOCOL_VERSION,
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
        return this.core.unary(this.service, "GetDownloadUrl", req, call);
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
        const stream = this.core.serverStream(this.service, "DownloadFile", { file_id: fileId, ...opts }, call);
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
        return this.core.unary(this.service, "GetFile", req, call);
    }
    /** Update mutable file metadata (`UpdateFileRequest` → `UpdateFileResponse`). */
    updateFile(request, call) {
        return this.core.unary(this.service, "UpdateFile", request, call);
    }
    /** Delete a file record (and its object) by id (or full `DeleteFileRequest`). */
    deleteFile(request, call) {
        const req = typeof request === "string" ? { file_id: request } : request;
        return this.core.unary(this.service, "DeleteFile", req, call);
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
    tokenStore;
    refreshInFlight = null;
    /** Background bearer-refresher state (parity with the Go enterprise session): a
     *  timer proactively refreshes before expiry; on a hard failure (expired +
     *  unrefreshable) the bearer is cleared so calls fail CLOSED instead of sending a
     *  dead credential. `poisoned` reflects that state; `lastRefreshError` exposes it. */
    refreshTimer = null;
    poisoned = false;
    lastRefreshError = null;
    closed = false;
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
        const udb = new UdbProject(config);
        const apiKey = config.credentials?.apiKey;
        if (apiKey && !config.credentials?.bearerToken) {
            await udb.authenticateApiKeyAndAdopt(apiKey);
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
     * Pass the human tenant CODE as `config.tenantId` (e.g. `"acme"`); it is used as
     * the pre-login hint and replaced by the canonical tenant UUID after login — so
     * every later call sends the UUID that row-level security compares, never the
     * code. Returns a session ready for tenant-scoped work; call `close()` to stop
     * the refresher and channels.
     */
    static async connectEnterprise(config) {
        const { username, password, ...rest } = config;
        const udb = new UdbProject(rest);
        await udb.loginAndAdoptTenant({
            username,
            password,
            tenant_hint: rest.tenantId,
            project_hint: rest.projectId ?? "",
        });
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
        this.tokenStore = config.tokenStore ?? new MemoryTokenStore();
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
        return new entity_1.EntityHandle(this.core, messageType, resolved, context);
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
    async authenticateApiKeyAndAdopt(apiKey = this.config.credentials?.apiKey ?? "") {
        if (!apiKey)
            throw new Error("udb: api key is required");
        const verified = await this.auth.authenticateApiKey(apiKey);
        const token = verified?.access_token ?? "";
        if (!token)
            throw new Error("udb: authenticate api key returned no access token");
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
        this.applyCredentials(token);
        return verified;
    }
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
     */
    async refreshIfNeeded(skewMs = 60_000) {
        const current = await this.tokenStore.load();
        if (!current)
            return null;
        const fresh = current.expiresAt === 0 || Date.now() + skewMs < current.expiresAt;
        if (fresh)
            return current;
        if (!current.refreshToken && !current.sessionId)
            return current;
        if (!this.refreshInFlight) {
            this.refreshInFlight = (async () => {
                try {
                    const resp = await this.auth.refreshToken({
                        refresh_token: current.refreshToken ?? "",
                        session_id: current.sessionId ?? "",
                    });
                    const expiresIn = Number(resp?.access_token_expires_in ?? 0);
                    const next = {
                        accessToken: resp?.access_token || current.accessToken,
                        refreshToken: current.refreshToken,
                        sessionId: current.sessionId,
                        expiresAt: expiresIn > 0 ? Date.now() + expiresIn * 1000 : 0,
                    };
                    await this.tokenStore.save(next);
                    this.applyCredentials(next.accessToken);
                    return next;
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
                delay = tok.expiresAt - BG_REFRESH_SKEW_MS - Date.now();
                if (delay < BG_REFRESH_MIN_MS)
                    delay = BG_REFRESH_MIN_MS;
            }
            // While poisoned, back off from the 1s floor so a permanently-dead token
            // doesn't hot-loop the refresh RPC.
            if (this.poisoned && delay < BG_REFRESH_RETRY_MS)
                delay = BG_REFRESH_RETRY_MS;
            this.armTimer(delay);
        })
            .catch(() => this.armTimer(BG_REFRESH_RETRY_MS));
    }
    armTimer(delayMs) {
        if (this.closed)
            return;
        this.refreshTimer = setTimeout(() => {
            void this.backgroundRefreshTick();
        }, delayMs);
        // Never keep the process alive just for the refresh timer.
        this.refreshTimer.unref?.();
    }
    async backgroundRefreshTick() {
        try {
            await this.refreshIfNeeded(BG_REFRESH_SKEW_MS);
            const tok = await this.tokenStore.load();
            if (tok && (tok.expiresAt === 0 || Date.now() < tok.expiresAt)) {
                this.poisoned = false;
                this.lastRefreshError = null;
            }
        }
        catch (err) {
            this.lastRefreshError = err instanceof Error ? err : new Error(String(err));
            const tok = await Promise.resolve(this.tokenStore.load()).catch(() => null);
            const expired = !tok || (tok.expiresAt > 0 && Date.now() >= tok.expiresAt);
            if (expired) {
                // Fail closed: drop the dead bearer so no call goes out with a stale
                // credential — the broker rejects (Unauthenticated) until a fresh login.
                this.poisoned = true;
                this.clearBearerCredentials();
            }
        }
        finally {
            this.scheduleRefresh();
        }
    }
    /** Clear the stored token (local only; does not call Logout). */
    async logout() {
        await this.tokenStore.clear();
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
//# sourceMappingURL=project.js.map