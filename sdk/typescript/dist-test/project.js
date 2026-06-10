"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Udb = exports.AuthzFacade = exports.UdbProject = exports.WebRtcFacade = exports.WebRtcTurnFacade = exports.WebRtcTrackFacade = exports.WebRtcPeerFacade = exports.WebRtcRoomFacade = exports.AssetFacade = exports.StorageFacade = exports.AnalyticsFacade = exports.NotificationFacade = exports.TenantFacade = exports.ApiKeyFacade = exports.MemoryTokenStore = void 0;
exports.createUdb = createUdb;
const auth_1 = require("./auth");
const client_1 = require("./client");
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
        apiKey: config.credentials?.apiKey,
    };
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
class UdbProject {
    config;
    /** The shared generated client (escape hatch — raw, typed, per-service RPCs). */
    generated;
    /** The shared robust core (channels, retry, metadata, typed errors). */
    core;
    /** Data plane: the DataBroker service surface. */
    data;
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
    tokenStore;
    refreshInFlight = null;
    /** Dedicated generated client for the auth/control-plane target when
     * `authTarget` differs from `target`; otherwise null (native facades share
     * `this.generated`). */
    authGenerated = null;
    /** Dedicated generated client for the WebRTC target, when `webrtcTarget`
     *  differs from both `target` and `authTarget`; otherwise null. */
    webrtcGenerated = null;
    constructor(config) {
        this.config = config;
        const meta = metaFromConfig(config);
        const authTarget = (config.authTarget ?? config.target).trim();
        this.generated = new generatedClient_1.UdbGeneratedClient({
            target: config.target,
            meta,
            secure: config.secure,
            tls: config.tls,
            bearerToken: config.credentials?.bearerToken,
            apiKey: config.credentials?.apiKey,
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
                apiKey: config.credentials?.apiKey,
                deadlineMs: config.deadlineMs,
                retry: config.retry,
                protoRoot: config.protoRoot,
                channelOptions: config.channelOptions,
            });
        }
        const nativeCore = this.authGenerated?.core ?? this.core;
        this.data = this.generated.DataBroker;
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
                apiKey: config.credentials?.apiKey,
                deadlineMs: config.deadlineMs,
                retry: config.retry,
                protoRoot: config.protoRoot,
                channelOptions: config.channelOptions,
            });
            webrtcCore = this.webrtcGenerated.core;
        }
        this.webrtc = new WebRtcFacade(webrtcCore);
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
    /** Push a refreshed bearer (keeping the configured API key) into every
     *  outbound channel: the data core, the auth client, and — when separate —
     *  the dedicated WebRTC core. */
    applyCredentials(bearerToken) {
        const apiKey = this.config.credentials?.apiKey;
        this.core.setCredentials({ bearerToken, apiKey });
        this.authGenerated?.core.setCredentials({ bearerToken, apiKey });
        this.auth.setCredentials({ bearerToken, apiKey });
        this.webrtcGenerated?.core.setCredentials({ bearerToken, apiKey });
    }
    /** Remove the active bearer from every outbound channel while preserving any
     * configured API key. */
    clearBearerCredentials() {
        const apiKey = this.config.credentials?.apiKey;
        this.core.setCredentials({ bearerToken: undefined, apiKey });
        this.authGenerated?.core.setCredentials({ bearerToken: undefined, apiKey });
        this.auth.setCredentials({ bearerToken: undefined, apiKey });
        this.webrtcGenerated?.core.setCredentials({ bearerToken: undefined, apiKey });
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
    /** Clear the stored token (local only; does not call Logout). */
    async logout() {
        await this.tokenStore.clear();
        this.clearBearerCredentials();
    }
    /** Close the shared channels (and the dedicated WebRTC channel, if any). */
    close() {
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
