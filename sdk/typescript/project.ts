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
import * as grpc from "@grpc/grpc-js";

import { AuthzCache, PermissionCheck, UdbAuthClient } from "./auth";
import { UDB_PROTOCOL_VERSION, UdbMetadata } from "./client";
import {
  CallOptions,
  RetryPolicy,
  TlsOptions,
  UdbCore,
  UdbGeneratedClient,
} from "./generatedClient";
import { defaultProtoRoot } from "./protoRoot";

// ── Configuration ───────────────────────────────────────────────────────────

/** Optional bearer/api-key credentials shared by every sub-client. */
export interface UdbCredentials {
  /** Bearer token; sent as `authorization: Bearer <token>`. */
  bearerToken?: string;
  /** API key; sent as `x-api-key`. */
  apiKey?: string;
}

/** One shared config for the whole project facade. The only required field is
 *  `target` (the UDB gRPC endpoint) and `tenantId`. */
export interface UdbProjectConfig {
  /** `host:port` of the UDB data-plane / generated-services gRPC endpoint. */
  target: string;
  /** `host:port` of the native auth control-plane listener. Defaults to
   *  `target` when the auth service is co-located. */
  authTarget?: string;
  /** `host:port` of the WebRTC signalling/peer endpoint. When set, the
   *  `.webrtc.*` facade (room/peer/track/turn + signalling) is built on a
   *  dedicated channel to this target; otherwise it shares the auth/control-plane
   *  target. */
  webrtcTarget?: string;

  tenantId: string;
  projectId?: string;
  purpose?: string;
  scopes?: string[];
  userId?: string;
  serviceIdentity?: string;
  correlationId?: string;

  credentials?: UdbCredentials;
  /** Enable TLS. When `tls` is provided this is implied. */
  tls?: TlsOptions;
  secure?: boolean;
  /** Extra channel options forwarded to @grpc/grpc-js for generated and auth clients. */
  channelOptions?: grpc.ChannelOptions;
  retry?: Partial<RetryPolicy>;
  /** Default per-call deadline in milliseconds. */
  deadlineMs?: number;
  /** Override the proto root (directory containing `udb/**`). */
  protoRoot?: string;
  /** A pluggable token store for `login` / `refreshIfNeeded`. */
  tokenStore?: TokenStore;
  /** HMAC secret for verifying server-issued signed policy bundles. When set,
   *  `authz.getPolicyBundle()` verifies the bundle signature (lowercase-hex
   *  HMAC-SHA256) and throws `UdbPolicyBundleError` on mismatch. */
  policyBundleSecret?: string;
}

function metaFromConfig(config: UdbProjectConfig): UdbMetadata {
  return {
    tenantId: config.tenantId,
    purpose: config.purpose ?? "",
    correlationId: config.correlationId ?? `udb-${Date.now().toString(36)}`,
    scopes: config.scopes,
    serviceIdentity: config.serviceIdentity,
    userId: config.userId,
    projectId: config.projectId,
    clientCatalogVersion: UDB_PROTOCOL_VERSION,
    bearerToken: config.credentials?.bearerToken,
    apiKey: config.credentials?.apiKey,
  };
}

// ── Token / session lifecycle ───────────────────────────────────────────────

/** A stored token + its bookkeeping. `expiresAt` is epoch milliseconds. */
export interface StoredToken {
  accessToken: string;
  refreshToken?: string;
  sessionId?: string;
  /** Epoch milliseconds when the access token expires (0 = unknown). */
  expiresAt: number;
}

/** Pluggable persistence for the active token. The default is in-process only;
 *  apps can supply a Redis/cookie/file-backed implementation. */
export interface TokenStore {
  load(): Promise<StoredToken | null> | StoredToken | null;
  save(token: StoredToken): Promise<void> | void;
  clear(): Promise<void> | void;
}

/** Default in-memory token store (single token; process-local). */
export class MemoryTokenStore implements TokenStore {
  private token: StoredToken | null = null;
  load(): StoredToken | null {
    return this.token;
  }
  save(token: StoredToken): void {
    this.token = token;
  }
  clear(): void {
    this.token = null;
  }
}

// ── Convenience sub-clients ─────────────────────────────────────────────────

/** Thin wrappers over the ApiKeyService generated RPCs. */
export class ApiKeyFacade {
  constructor(
    private core: UdbCore,
    private readonly service = "udb.core.apikey.services.v1.ApiKeyService",
  ) {}

  /** Create a key. The plain key is returned ONCE in `plain_key`. */
  create(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "CreateApiKey", request, call);
  }

  /** Revoke a key by id (with an optional `revoke_reason`). */
  revoke(keyId: string, revokeReason = "", call?: CallOptions): Promise<any> {
    return this.core.unary(
      this.service,
      "RevokeApiKey",
      { key_id: keyId, revoke_reason: revokeReason },
      call,
    );
  }

  /** Update mutable key fields (name / description / scopes / rate limits / …). */
  update(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "UpdateApiKey", request, call);
  }

  get(keyId: string, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "GetApiKey", { key_id: keyId }, call);
  }

  list(request: any = {}, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "ListApiKeys", request, call);
  }

  /** Rotate a key atomically through ApiKeyService.RotateApiKey. */
  rotate(keyId: string, request?: { rotation_reason?: string; context?: any } | string, call?: CallOptions): Promise<any>;
  rotate(request: any, call?: CallOptions): Promise<any>;
  rotate(
    keyIdOrRequest: string | any,
    requestOrCall: ({ rotation_reason?: string; context?: any } | string | CallOptions) = {},
    call?: CallOptions,
  ): Promise<any> {
    if (typeof keyIdOrRequest !== "string") {
      return this.core.unary(this.service, "RotateApiKey", keyIdOrRequest, requestOrCall as CallOptions);
    }

    const request =
      typeof requestOrCall === "string"
        ? { key_id: keyIdOrRequest, rotation_reason: requestOrCall }
        : { ...(requestOrCall as Record<string, unknown>), key_id: keyIdOrRequest };
    return this.core.unary(this.service, "RotateApiKey", request, call);
  }
}

/** Thin wrappers over the TenantService generated RPCs. */
export class TenantFacade {
  constructor(
    private core: UdbCore,
    private readonly service = "udb.core.tenant.services.v1.TenantService",
  ) {}

  /** Onboard / create a tenant. (TenantService exposes no separate onboarding
   *  RPC; `CreateTenant` is the onboarding entry point.) */
  create(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "CreateTenant", request, call);
  }

  /** Alias for {@link create} for callers that think in onboarding terms. */
  onboard(request: any, call?: CallOptions): Promise<any> {
    return this.create(request, call);
  }

  get(tenantId: string, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "GetTenant", { tenant_id: tenantId }, call);
  }

  list(request: any = {}, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "ListTenants", request, call);
  }

  update(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "UpdateTenant", request, call);
  }
}

/** Thin wrappers over the NotificationService generated RPCs. */
export class NotificationFacade {
  constructor(
    private core: UdbCore,
    private readonly service = "udb.core.notification.services.v1.NotificationService",
  ) {}

  /** Send (or enqueue) a notification. `request` is a `SendNotificationRequest`
   *  ({ event_type, recipient_id, recipient_address, variables, channels, … }). */
  send(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "SendNotification", request, call);
  }

  get(logId: string, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "GetNotification", { log_id: logId }, call);
  }

  list(request: any = {}, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "ListNotifications", request, call);
  }

  retry(logId: string, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "RetryNotification", { log_id: logId }, call);
  }
}

/** Thin wrappers over the AnalyticsService generated RPCs. */
export class AnalyticsFacade {
  constructor(
    private core: UdbCore,
    private readonly service = "udb.core.analytics.services.v1.AnalyticsService",
  ) {}

  getThroughput(request: any = {}, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "GetThroughput", request, call);
  }
  getPipelineSummary(request: any = {}, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "GetPipelineSummary", request, call);
  }
  getSlaCompliance(request: any = {}, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "GetSlaCompliance", request, call);
  }
  recordPipelineMetric(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "RecordPipelineMetric", request, call);
  }
  triggerSnapshot(request: any = {}, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "TriggerSnapshot", request, call);
  }
}

/**
 * Storage facade. The PRIMARY surface is the native StorageService file
 * lifecycle (register/finalize an upload, mint a download URL, get/update/delete/
 * list file records). The pre-existing DataBroker object-byte IO (streamed
 * put/get + presigned URL) is retained as escape-hatch helpers so Wave-1 callers
 * keep working: `putObject` (client stream), `getObject` (server stream) and
 * `presign`.
 */
export class StorageFacade {
  constructor(
    private core: UdbCore,
    /** StorageService full-name (file lifecycle). */
    private readonly service = "udb.core.storage.services.v1.StorageService",
    private readonly objectCore: UdbCore = core,
    /** DataBroker full-name (raw object-byte IO). */
    private readonly objectService = "udb.services.v1.DataBroker",
  ) {}

  // ── StorageService (primary, file lifecycle) ──────────────────────────────

  /** Begin an upload: reserve a file id + (optionally) a presigned target
   *  (`RegisterUploadRequest` → `RegisterUploadResponse`). */
  registerUpload(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "RegisterUpload", request, call);
  }

  /** Finalize a previously-registered upload, committing the file record
   *  (`FinalizeUploadRequest` → `FinalizeUploadResponse`). */
  finalizeUpload(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "FinalizeUpload", request, call);
  }

  /** Mint a time-limited download URL for a stored file
   *  (`GetDownloadUrlRequest` → `GetDownloadUrlResponse`). Pass either a full
   *  request or a `file_id` string. */
  getDownloadUrl(request: any, call?: CallOptions): Promise<any> {
    const req = typeof request === "string" ? { file_id: request } : request;
    return this.core.unary(this.service, "GetDownloadUrl", req, call);
  }

  /** Fetch a file record by id (or full `GetFileRequest`). */
  getFile(request: any, call?: CallOptions): Promise<any> {
    const req = typeof request === "string" ? { file_id: request } : request;
    return this.core.unary(this.service, "GetFile", req, call);
  }

  /** Update mutable file metadata (`UpdateFileRequest` → `UpdateFileResponse`). */
  updateFile(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "UpdateFile", request, call);
  }

  /** Delete a file record (and its object) by id (or full `DeleteFileRequest`). */
  deleteFile(request: any, call?: CallOptions): Promise<any> {
    const req = typeof request === "string" ? { file_id: request } : request;
    return this.core.unary(this.service, "DeleteFile", req, call);
  }

  /** List file records (`ListFilesRequest` → `ListFilesResponse`). */
  listFiles(request: any = {}, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "ListFiles", request, call);
  }

  // ── DataBroker object-byte IO (retained escape hatches) ───────────────────

  /** Open a client-streaming upload. Write `Chunk`s to `stream`, then await
   *  `response` for the `MutationResponse`. */
  putObject(call?: CallOptions): { stream: grpc.ClientWritableStream<any>; response: Promise<any> } {
    return this.objectCore.clientStream(this.objectService, "PutObject", call);
  }

  /** Open a server-streaming download; consume the `Chunk`s with `for await`. */
  getObject(request: any, call?: CallOptions): grpc.ClientReadableStream<any> {
    return this.objectCore.serverStream(this.objectService, "GetObject", request, call);
  }

  /** Mint a presigned URL on the DataBroker object surface
   *  (`UrlRequest` → `UrlResponse`). */
  presign(request: any, call?: CallOptions): Promise<any> {
    return this.objectCore.unary(this.objectService, "GeneratePresignedUrl", request, call);
  }

  /** @deprecated Use {@link presign}. Retained for Wave-1 compatibility. */
  generatePresignedUrl(request: any, call?: CallOptions): Promise<any> {
    return this.presign(request, call);
  }
}

/** Thin wrappers over the AssetService generated RPCs (pipeline definitions,
 *  asset registration, pipeline runs + step completion). */
export class AssetFacade {
  constructor(
    private core: UdbCore,
    private readonly service = "udb.core.asset.services.v1.AssetService",
  ) {}

  /** Create a reusable pipeline definition
   *  (`CreatePipelineDefinitionRequest` → `…Response`). */
  createPipelineDefinition(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "CreatePipelineDefinition", request, call);
  }

  /** Fetch a pipeline definition by id (or full request). */
  getPipelineDefinition(request: any, call?: CallOptions): Promise<any> {
    const req = typeof request === "string" ? { definition_id: request } : request;
    return this.core.unary(this.service, "GetPipelineDefinition", req, call);
  }

  /** Register an asset (`RegisterAssetRequest` → `RegisterAssetResponse`). */
  registerAsset(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "RegisterAsset", request, call);
  }

  /** Start a pipeline run for an asset
   *  (`StartPipelineRequest` → `StartPipelineResponse`). */
  startPipeline(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "StartPipeline", request, call);
  }

  /** Fetch a running/finished pipeline instance by id (or full request). */
  getPipeline(request: any, call?: CallOptions): Promise<any> {
    const req = typeof request === "string" ? { instance_id: request } : request;
    return this.core.unary(this.service, "GetPipeline", req, call);
  }

  /** Mark a pipeline step complete (`CompleteStepRequest` → `…Response`). */
  completeStep(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "CompleteStep", request, call);
  }

  /** List assets (`ListAssetsRequest` → `ListAssetsResponse`). */
  listAssets(request: any = {}, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "ListAssets", request, call);
  }

  /** Fetch an asset by id (or full `GetAssetRequest`). */
  getAsset(request: any, call?: CallOptions): Promise<any> {
    const req = typeof request === "string" ? { asset_id: request } : request;
    return this.core.unary(this.service, "GetAsset", req, call);
  }
}

/** WebRTC room sub-client (RoomService). */
export class WebRtcRoomFacade {
  constructor(
    private core: UdbCore,
    private readonly service = "udb.core.webrtc.services.v1.RoomService",
  ) {}

  createRoom(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "CreateRoom", request, call);
  }
  getRoom(request: any, call?: CallOptions): Promise<any> {
    const req = typeof request === "string" ? { room_id: request } : request;
    return this.core.unary(this.service, "GetRoom", req, call);
  }
  updateRoom(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "UpdateRoom", request, call);
  }
  closeRoom(request: any, call?: CallOptions): Promise<any> {
    const req = typeof request === "string" ? { room_id: request } : request;
    return this.core.unary(this.service, "CloseRoom", req, call);
  }
  listRooms(request: any = {}, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "ListRooms", request, call);
  }
}

/** WebRTC peer sub-client (PeerService). */
export class WebRtcPeerFacade {
  constructor(
    private core: UdbCore,
    private readonly service = "udb.core.webrtc.services.v1.PeerService",
  ) {}

  joinRoom(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "JoinRoom", request, call);
  }
  leaveRoom(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "LeaveRoom", request, call);
  }
  getPeer(request: any, call?: CallOptions): Promise<any> {
    const req = typeof request === "string" ? { peer_id: request } : request;
    return this.core.unary(this.service, "GetPeer", req, call);
  }
  listPeers(request: any = {}, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "ListPeers", request, call);
  }
}

/** WebRTC track sub-client (TrackService). */
export class WebRtcTrackFacade {
  constructor(
    private core: UdbCore,
    private readonly service = "udb.core.webrtc.services.v1.TrackService",
  ) {}

  publishTrack(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "PublishTrack", request, call);
  }
  unpublishTrack(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "UnpublishTrack", request, call);
  }
  muteTrack(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "MuteTrack", request, call);
  }
  listTracks(request: any = {}, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "ListTracks", request, call);
  }
}

/** WebRTC TURN sub-client (TurnService). */
export class WebRtcTurnFacade {
  constructor(
    private core: UdbCore,
    private readonly service = "udb.core.webrtc.services.v1.TurnService",
  ) {}

  /** Issue ephemeral TURN credentials
   *  (`IssueCredentialsRequest` → `IssueCredentialsResponse`). */
  issueCredentials(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "IssueCredentials", request, call);
  }
}

/**
 * WebRTC facade: namespaces the four unary WebRTC services plus a bidi signalling
 * helper. The raw per-service sub-clients are reachable as `.room / .peer /
 * .track / .turn`.
 */
export class WebRtcFacade {
  readonly room: WebRtcRoomFacade;
  readonly peer: WebRtcPeerFacade;
  readonly track: WebRtcTrackFacade;
  readonly turn: WebRtcTurnFacade;

  constructor(
    private core: UdbCore,
    private readonly signalingService = "udb.core.webrtc.services.v1.SignalingService",
  ) {
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
  signal(call?: CallOptions): grpc.ClientDuplexStream<any, any> {
    return this.core.bidiStream(this.signalingService, "Signal", call);
  }
}

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
export class UdbProject {
  /** The shared generated client (escape hatch — raw, typed, per-service RPCs). */
  readonly generated: UdbGeneratedClient;
  /** The shared robust core (channels, retry, metadata, typed errors). */
  readonly core: UdbCore;

  /** Data plane: the DataBroker service surface. */
  readonly data: UdbGeneratedClient["DataBroker"];
  /** Authentication ergonomics (authenticate*, login/refresh helpers, …). */
  readonly auth: UdbAuthClient;
  /** Authorization ergonomics (can / require / explain / batchCan / nativeAccess),
   *  routed through a TTL `AuthzCache`. */
  readonly authz: AuthzFacade;
  readonly apikey: ApiKeyFacade;
  readonly tenant: TenantFacade;
  readonly notification: NotificationFacade;
  readonly analytics: AnalyticsFacade;
  /** Storage: native StorageService file lifecycle (+ retained DataBroker
   *  object-byte IO via `.storage.putObject / getObject / presign`). */
  readonly storage: StorageFacade;
  /** Asset management: AssetService pipelines + asset registration. */
  readonly asset: AssetFacade;
  /** WebRTC: `.webrtc.room / .peer / .track / .turn` + `.webrtc.signal()`. */
  readonly webrtc: WebRtcFacade;

  private readonly tokenStore: TokenStore;
  private refreshInFlight: Promise<StoredToken> | null = null;
  /** Dedicated generated client for the auth/control-plane target when
   * `authTarget` differs from `target`; otherwise null (native facades share
   * `this.generated`). */
  private readonly authGenerated: UdbGeneratedClient | null = null;
  /** Dedicated generated client for the WebRTC target, when `webrtcTarget`
   *  differs from both `target` and `authTarget`; otherwise null. */
  private readonly webrtcGenerated: UdbGeneratedClient | null = null;

  constructor(private readonly config: UdbProjectConfig) {
    const meta = metaFromConfig(config);
    const authTarget = (config.authTarget ?? config.target).trim();
    this.generated = new UdbGeneratedClient({
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
      this.authGenerated = new UdbGeneratedClient({
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
    const protoRoot = config.protoRoot ?? defaultProtoRoot();
    const authClient = new UdbAuthClient(
      authTarget,
      meta,
      protoRoot,
      config.policyBundleSecret,
      {
        secure: config.secure,
        tls: config.tls,
        channelOptions: config.channelOptions,
      },
    );
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
      this.webrtcGenerated = new UdbGeneratedClient({
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
  currentToken(): Promise<StoredToken | null> | StoredToken | null {
    return this.tokenStore.load();
  }

  /**
   * Username/password login via AuthnService.Login. On success the JWT (or
   * session) is persisted in the token store. Returns the raw `LoginResponse`.
   * If `mfa_required` comes back true, the caller must re-call `login` with the
   * second factor; nothing is stored in that case.
   */
  async login(request: {
    username: string;
    password: string;
    [key: string]: any;
  }): Promise<any> {
    const resp: any = await this.auth.login(request);
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
  private applyCredentials(bearerToken: string): void {
    const apiKey = this.config.credentials?.apiKey;
    this.core.setCredentials({ bearerToken, apiKey });
    this.authGenerated?.core.setCredentials({ bearerToken, apiKey });
    this.auth.setCredentials({ bearerToken, apiKey });
    this.webrtcGenerated?.core.setCredentials({ bearerToken, apiKey });
  }

  /** Remove the active bearer from every outbound channel while preserving any
   * configured API key. */
  private clearBearerCredentials(): void {
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
  async refreshIfNeeded(skewMs = 60_000): Promise<StoredToken | null> {
    const current = await this.tokenStore.load();
    if (!current) return null;
    const fresh = current.expiresAt === 0 || Date.now() + skewMs < current.expiresAt;
    if (fresh) return current;
    if (!current.refreshToken && !current.sessionId) return current;

    if (!this.refreshInFlight) {
      this.refreshInFlight = (async () => {
        try {
          const resp: any = await this.auth.refreshToken({
            refresh_token: current.refreshToken ?? "",
            session_id: current.sessionId ?? "",
          });
          const expiresIn = Number(resp?.access_token_expires_in ?? 0);
          const next: StoredToken = {
            accessToken: resp?.access_token || current.accessToken,
            refreshToken: current.refreshToken,
            sessionId: current.sessionId,
            expiresAt: expiresIn > 0 ? Date.now() + expiresIn * 1000 : 0,
          };
          await this.tokenStore.save(next);
          this.applyCredentials(next.accessToken);
          return next;
        } finally {
          this.refreshInFlight = null;
        }
      })();
    }
    return this.refreshInFlight;
  }

  /** Clear the stored token (local only; does not call Logout). */
  async logout(): Promise<void> {
    await this.tokenStore.clear();
    this.clearBearerCredentials();
  }

  /** Close the shared channels (and the dedicated WebRTC channel, if any). */
  close(): void {
    this.generated.close();
    this.authGenerated?.close();
    this.webrtcGenerated?.close();
  }
}

/**
 * Authorization facade: routes can/require/explain through a TTL `AuthzCache`
 * (so repeated checks within the cache window don't re-hit the server), while
 * batchCan / nativeAccess / getPolicyBundle go straight to the auth client.
 */
export class AuthzFacade {
  private readonly cache: AuthzCache;
  constructor(private readonly client: UdbAuthClient) {
    this.cache = new AuthzCache(client);
  }

  /** `[allowed, decision]` for (resource, action, purpose), cache-routed. */
  can(resource: any, action: string, purpose = ""): Promise<[boolean, any]> {
    return this.cache.can(resource, action, purpose);
  }

  /** Throws `UdbAuthzDenied` on deny; returns the allowing decision. */
  require(resource: any, action: string, purpose = ""): Promise<any> {
    return this.cache.require(resource, action, purpose);
  }

  /** Full decision without throwing. */
  explain(resource: any, action: string, purpose = ""): Promise<any> {
    return this.cache.explain(resource, action, purpose);
  }

  /** Batch `(object, action)` checks in one RPC (not cached). */
  batchCan(checks: PermissionCheck[]): ReturnType<UdbAuthClient["batchCan"]> {
    return this.client.batchCan(checks);
  }

  /** Authorize and, when allowed, return the native-access grant. */
  nativeAccess(resource: any, action: string, purpose = ""): Promise<any | null> {
    return this.client.nativeAccess(resource, action, purpose);
  }

  /** Fetch the signed policy bundle for local authorization caches. */
  getPolicyBundle(): Promise<any> {
    return this.client.getPolicyBundle();
  }

  /** Drop the local decision cache (e.g. after a policy change). */
  invalidate(): void {
    this.cache.invalidate();
  }
}

/** Build a {@link UdbProject} from a single shared config. */
export function createUdb(config: UdbProjectConfig): UdbProject {
  return new UdbProject(config);
}

/** Namespaced alias: `Udb.project(config)` mirrors `createUdb(config)`. */
export const Udb = {
  project(config: UdbProjectConfig): UdbProject {
    return new UdbProject(config);
  },
};
