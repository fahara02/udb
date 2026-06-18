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
import * as http from "http";
import * as https from "https";

import { AuthzCache, PermissionCheck, UdbAuthClient } from "./auth";
import { UDB_PROTOCOL_VERSION, UdbMetadata } from "./client";
import { consistencyMetadata } from "./consistency";
import { EntityHandle, EntityHandleOptions } from "./entity";
import {
  CallOptions,
  ENTITY_REGISTRY,
  RetryPolicy,
  TlsOptions,
  UdbCore,
  UdbGeneratedClient,
} from "./generatedClient";
import { defaultProtoRoot } from "./protoRoot";

// ── Data plane (raw DataBroker + bound table/entity accessors) ────────────────

/** The raw DataBroker generated surface additively carrying the bound
 *  `table(name)` / `entity(messageType)` accessors. The headline snippet's
 *  `udb.data.table("invoice").select({ where })` resolves here; every raw RPC
 *  (`select`/`upsert`/`delete`/…) stays reachable as before. */
export type DataPlane = UdbGeneratedClient["DataBroker"] & {
  /** Bound entity handle for a DataBroker message type (alias of `UdbProject.entity`). */
  entity<T = any>(messageType: string, opts?: Partial<EntityHandleOptions>): EntityHandle<T>;
  /** Table-shaped bound handle (alias of `UdbProject.table`). */
  table<T = any>(name: string, opts?: { key?: string[] }): EntityHandle<T>;
};

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

/** Plain HTTP(S) PUT of `body` to `urlStr` with a content-type header. Used by
 *  `StorageFacade.uploadFile` to send bytes to the broker-minted presigned URL.
 *  Resolves on a 2xx; rejects otherwise. No extra dependency (Node http/https). */
function httpPut(urlStr: string, contentType: string, body: Buffer): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let url: URL;
    try {
      url = new URL(urlStr);
    } catch (e) {
      reject(new Error(`udb: storage.uploadFile: invalid upload_url`));
      return;
    }
    const transport = url.protocol === "http:" ? http : https;
    const req = transport.request(
      url,
      {
        method: "PUT",
        headers: { "content-type": contentType, "content-length": body.length },
      },
      (res) => {
        const status = res.statusCode ?? 0;
        res.resume(); // drain
        if (status >= 200 && status < 300) resolve();
        else reject(new Error(`udb: storage.uploadFile: PUT failed (HTTP ${status})`));
      },
    );
    req.on("error", (err) => reject(err));
    req.end(body);
  });
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

  /** Send a templated notification: the broker renders the template from
   *  `event_type` + `variables` (template render landed broker-side). Delegates to
   *  the raw `SendNotification` — exactly one RPC. Returns the SendNotification
   *  response (carries the log ids). */
  sendTemplate(
    eventType: string,
    recipientId: string,
    variables: Record<string, unknown> = {},
    extra: Record<string, unknown> = {},
    call?: CallOptions,
  ): Promise<any> {
    return this.send(
      { event_type: eventType, recipient_id: recipientId, variables, ...extra },
      call,
    );
  }

  /** Retry a FAILED notification log by id. One `RetryNotification` RPC. */
  retryFailed(logId: string, call?: CallOptions): Promise<any> {
    return this.retry(logId, call);
  }

  /**
   * Wait for a notification to reach a terminal delivery status by READING the
   * log via `GetNotification` — bounded by `deadlineMs`, status-driven, NO fixed
   * sleeps between reads beyond a short backoff. Terminal statuses:
   * DELIVERED / FAILED / SUPPRESSED. Returns the terminal log; throws on timeout.
   */
  async waitForDelivery(
    logId: string,
    deadlineMs = 30_000,
    call?: CallOptions,
  ): Promise<any> {
    const terminal = new Set(["DELIVERED", "FAILED", "SUPPRESSED"]);
    const deadline = Date.now() + deadlineMs;
    let pollMs = 200;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const resp: any = await this.get(logId, call);
      const status = String(resp?.log?.status ?? resp?.status ?? "").toUpperCase();
      if (terminal.has(status)) return resp;
      if (Date.now() >= deadline) {
        throw new Error(`udb: waitForDelivery(${logId}) timed out in status '${status}'`);
      }
      await new Promise((r) => setTimeout(r, Math.min(pollMs, deadline - Date.now())));
      pollMs = Math.min(pollMs * 2, 2_000);
    }
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
  downloadFile(
    fileId: string,
    opts: Record<string, unknown> & { stream?: boolean } = {},
    call?: CallOptions,
  ): Promise<any> {
    const { stream, ...rest } = opts;
    if (stream) return this.downloadFileBytes(fileId, rest, call);
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
  async downloadFileBytes(
    fileId: string,
    opts: Record<string, unknown> = {},
    call?: CallOptions,
  ): Promise<Uint8Array> {
    const stream = this.core.serverStream(
      this.service,
      "DownloadFile",
      { file_id: fileId, ...opts },
      call,
    );
    const parts: Uint8Array[] = [];
    let total = 0;
    for await (const chunk of stream as AsyncIterable<any>) {
      const data: Uint8Array | undefined = chunk?.data;
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
  async uploadFile(
    fileName: string,
    bytes: Buffer | Uint8Array,
    opts: {
      contentType?: string;
      fileType?: string;
      checksum?: string;
      /** Extra fields merged into the RegisterUpload request (context, tags, …). */
      register?: Record<string, unknown>;
      /** Extra fields merged into the FinalizeUpload request. */
      finalize?: Record<string, unknown>;
      /** Override the HTTP PUT (e.g. for tests). Defaults to Node http/https. */
      putFn?: (url: string, contentType: string, body: Buffer) => Promise<void>;
      call?: CallOptions;
    } = {},
  ): Promise<any> {
    const contentType = opts.contentType ?? "application/octet-stream";
    const registered: any = await this.registerUpload(
      {
        file_name: fileName,
        content_type: contentType,
        ...(opts.fileType ? { file_type: opts.fileType } : {}),
        ...(opts.register ?? {}),
      },
      opts.call,
    );
    const uploadUrl: string = registered?.upload_url ?? "";
    const fileId: string = registered?.file_id ?? "";
    if (!uploadUrl) {
      const reason =
        registered?.error?.message || registered?.error?.code || "upload_url unavailable";
      throw new Error(`udb: storage.uploadFile: presign degraded (${reason})`);
    }
    // Plain HTTP PUT of the bytes (NOT gRPC) to the presigned target. Uses Node's
    // http/https so the typing is deterministic (global `fetch`'s BodyInit type is
    // lib-dependent) and the SDK pulls in no extra HTTP dependency.
    const body = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
    await (opts.putFn ?? httpPut)(uploadUrl, contentType, body);
    return this.finalizeUpload(
      {
        file_id: fileId,
        size_bytes: body.length,
        ...(opts.checksum ? { checksum: opts.checksum } : {}),
        ...(opts.finalize ?? {}),
      },
      opts.call,
    );
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

  /** Define a reusable pipeline with a typed step list. Wraps
   *  `CreatePipelineDefinition` (steps marshalled to JSON). One RPC. */
  definePipeline(
    name: string,
    steps: unknown[],
    extra: Record<string, unknown> = {},
    call?: CallOptions,
  ): Promise<any> {
    return this.createPipelineDefinition(
      { name, steps: JSON.stringify(steps), ...extra },
      call,
    );
  }

  /** Register an asset bound to an existing storage file id. Wraps
   *  `RegisterAsset`. One RPC. */
  registerFromStorageFile(
    fileId: string,
    name: string,
    mediaType: string,
    extra: Record<string, unknown> = {},
    call?: CallOptions,
  ): Promise<any> {
    return this.registerAsset(
      { file_id: fileId, name, media_type: mediaType, ...extra },
      call,
    );
  }

  /**
   * Start a pipeline and wait for it to reach a terminal state. Issues exactly
   * ONE `StartPipeline` and reads the INLINE `steps` from its response (lane 04's
   * `StartPipelineResponse.steps`) — NO immediate `GetPipeline` proof read. Then
   * polls instance status via `GetPipeline` (reads only, bounded by `deadlineMs`,
   * no fixed sleeps) until terminal: COMPLETED / FAILED / CANCELLED.
   */
  async startAndWait(
    request: any,
    deadlineMs = 60_000,
    call?: CallOptions,
  ): Promise<{ started: any; steps: any[]; final: any }> {
    const started: any = await this.startPipeline(request, call);
    const steps: any[] = started?.steps ?? [];
    const instanceId: string = started?.instance_id ?? started?.instance?.instance_id ?? "";
    const terminal = new Set(["COMPLETED", "FAILED", "CANCELLED"]);
    const deadline = Date.now() + deadlineMs;
    let pollMs = 200;
    let final: any = started;
    if (instanceId) {
      // eslint-disable-next-line no-constant-condition
      while (true) {
        final = await this.getPipeline(instanceId, call);
        const status = String(
          final?.instance?.status ?? final?.status ?? "",
        ).toUpperCase();
        if (terminal.has(status)) break;
        if (Date.now() >= deadline) {
          throw new Error(
            `udb: startAndWait(${instanceId}) timed out in status '${status}'`,
          );
        }
        await new Promise((r) => setTimeout(r, Math.min(pollMs, deadline - Date.now())));
        pollMs = Math.min(pollMs * 2, 2_000);
      }
    }
    return { started, steps, final };
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
  /** Atomic join: returns `{ peer, existing_peers, ice_servers, expires_at }` in
   *  ONE RPC (replaces the JoinRoom + IssueCredentials two-round-trip path). */
  joinSession(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "JoinSession", request, call);
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

/** Migration lifecycle helpers over the DataBroker migration RPCs. */
export class MigrationsFacade {
  constructor(
    private core: UdbCore,
    private readonly service = "udb.services.v1.DataBroker",
  ) {}

  /** Plan a migration (`PlanMigration`). */
  plan(request: any = {}, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "PlanMigration", request, call);
  }

  /** Approve a planned migration (`ApproveMigrationPlan`). The response body
   *  carries `approval_token` (06.1.1.1) — read it from the BODY, not a header. */
  approve(request: any, call?: CallOptions): Promise<any> {
    return this.core.unary(this.service, "ApproveMigrationPlan", request, call);
  }

  /** Apply a migration (`ApplyMigration`). */
  apply(request: any, call?: CallOptions): Promise<any> {
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
  async applyCurrent(
    opts: { dryRun?: boolean; autoApprove?: boolean; plan?: any; call?: CallOptions } = {},
  ): Promise<any> {
    const planResp: any = await this.plan(opts.plan ?? {}, opts.call);
    const planId: string = planResp?.plan_id ?? planResp?.run_id ?? "";
    let approvalToken = "";
    if (opts.autoApprove) {
      const approveResp: any = await this.approve({ plan_id: planId }, opts.call);
      approvalToken = approveResp?.approval_token ?? "";
    }
    return this.apply(
      { plan_id: planId, approval_token: approvalToken, dry_run: opts.dryRun ?? false },
      opts.call,
    );
  }
}

/** Admin facade: groups admin-only control surfaces (currently migrations). */
export class AdminFacade {
  readonly migrations: MigrationsFacade;
  constructor(core: UdbCore) {
    this.migrations = new MigrationsFacade(core);
  }
}

/** A live CDC subscription handle returned by {@link EventsFacade.subscribe}. */
export interface EventSubscription {
  /** The underlying server-streaming `PublishCDC` call. */
  stream: grpc.ClientReadableStream<any>;
  /** Resolves on the FIRST observed stream metadata or envelope — a server-driven
   *  readiness boundary with NO timer/sleep. Rejects if the stream errors/ends
   *  before any signal. */
  ready(): Promise<void>;
  /** Close the subscription. */
  cancel(): void;
}

/**
 * Events facade over the DataBroker CDC surface (`PublishCDC` server-stream +
 * `EnqueueOutboxEvent`). Provides a sleep-free ready/ack boundary:
 * `subscribe(topic).ready()` resolves on the first server signal, and
 * `publishAndWait(...)` enqueues then awaits the matching envelope on a live
 * subscription — never a fixed sleep. The subscription is tenant-scoped.
 */
export class EventsFacade {
  constructor(
    private core: UdbCore,
    private readonly meta: UdbMetadata,
    private readonly service = "udb.services.v1.DataBroker",
  ) {}

  /** Open a tenant-scoped `PublishCDC` subscription for `topic`. */
  subscribe(topic: string, call?: CallOptions): EventSubscription {
    const request = {
      context: { tenant_id: this.meta.tenantId, project_id: this.meta.projectId ?? "" },
      topic,
    };
    const stream = this.core.serverStream(this.service, "PublishCDC", request, call);
    const ready = (): Promise<void> =>
      new Promise<void>((resolve, reject) => {
        let settled = false;
        const done = (fn: () => void) => {
          if (settled) return;
          settled = true;
          fn();
        };
        stream.once("metadata", () => done(resolve));
        stream.once("data", () => done(resolve));
        stream.once("error", (err: Error) => done(() => reject((err as any).udb ?? err)));
        stream.once("end", () =>
          done(() => reject(new Error("udb: CDC stream ended before ready"))),
        );
      });
    return { stream, ready, cancel: () => stream.cancel() };
  }

  /**
   * Enqueue an outbox event then await the matching `CDCEnvelope` on a live
   * subscription. Issues exactly ONE `EnqueueOutboxEvent`; resolves on the first
   * envelope where `match(envelope)` is true. No sleeps; bounded by `deadlineMs`.
   */
  async publishAndWait(
    topic: string,
    payload: Record<string, unknown>,
    match: (envelope: any) => boolean,
    deadlineMs = 30_000,
    call?: CallOptions,
  ): Promise<any> {
    const sub = this.subscribe(topic, call);
    try {
      await sub.ready();
      await this.core.unary(
        this.service,
        "EnqueueOutboxEvent",
        {
          context: { tenant_id: this.meta.tenantId, project_id: this.meta.projectId ?? "" },
          topic,
          payload,
        },
        call,
      );
      return await new Promise<any>((resolve, reject) => {
        const timer = setTimeout(() => {
          sub.cancel();
          reject(new Error(`udb: publishAndWait(${topic}) timed out`));
        }, deadlineMs);
        if (typeof (timer as any).unref === "function") (timer as any).unref();
        sub.stream.on("data", (envelope: any) => {
          if (match(envelope)) {
            clearTimeout(timer);
            resolve(envelope);
          }
        });
        sub.stream.on("error", (err: Error) => {
          clearTimeout(timer);
          reject((err as any).udb ?? err);
        });
        sub.stream.on("end", () => {
          clearTimeout(timer);
          reject(new Error(`udb: CDC stream ended before a matching envelope for ${topic}`));
        });
      });
    } finally {
      sub.cancel();
    }
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
  async joinSession(
    roomId: string,
    opts: {
      displayName?: string;
      metadata?: string;
      ttlSeconds?: number;
      tenantId?: string;
      /** Heartbeat interval in ms (0 disables). Default 15s. */
      heartbeatMs?: number;
      call?: CallOptions;
    } = {},
  ): Promise<WebRtcSession> {
    const request: Record<string, unknown> = { room_id: roomId };
    if (opts.tenantId) request.tenant_id = opts.tenantId;
    if (opts.displayName) request.display_name = opts.displayName;
    if (opts.metadata) request.metadata = opts.metadata;
    if (opts.ttlSeconds != null) request.ttl_seconds = opts.ttlSeconds;
    const joined: any = await this.peer.joinSession(request, opts.call);
    const peerId: string = joined?.peer?.peer_id ?? joined?.peer?.id ?? "";

    const stream = this.signal(opts.call);
    const heartbeatMs = opts.heartbeatMs ?? 15_000;
    let heartbeat: ReturnType<typeof setInterval> | null = null;
    if (heartbeatMs > 0) {
      heartbeat = setInterval(() => {
        try {
          stream.write({ heartbeat: { peer_id: peerId, room_id: roomId } });
        } catch {
          /* stream closed — leave() clears the timer */
        }
      }, heartbeatMs);
      if (typeof heartbeat.unref === "function") heartbeat.unref();
    }

    return {
      peer: joined?.peer,
      existingPeers: joined?.existing_peers ?? [],
      iceServers: joined?.ice_servers ?? [],
      expiresAt: joined?.expires_at,
      signal: stream,
      leave: async (): Promise<void> => {
        if (heartbeat) {
          clearInterval(heartbeat);
          heartbeat = null;
        }
        try {
          stream.end();
        } catch {
          /* already closed */
        }
        await this.peer.leaveRoom({ room_id: roomId, peer_id: peerId });
      },
    };
  }
}

/** A live WebRTC session returned by {@link WebRtcFacade.joinSession}. */
export interface WebRtcSession {
  peer: any;
  existingPeers: any[];
  iceServers: any[];
  expiresAt: any;
  /** The open bidi signaling duplex (write SignalRequests, read SignalResponses). */
  signal: grpc.ClientDuplexStream<any, any>;
  /** Close the signaling stream + issue LeaveRoom. */
  leave(): Promise<void>;
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

  /** Data plane: the DataBroker service surface, additively augmented with the
   *  bound `table(name)` / `entity(messageType)` accessors so the
   *  `simple_client_code.md` headline form `udb.data.table("invoice").select(...)`
   *  works. The raw generated RPCs (`select`/`upsert`/`delete`/…) stay reachable. */
  readonly data: DataPlane;
  /** Receipt/fence accessors per the naming contract: `udb.metadata.afterWrite(receipt)`
   *  attaches a receipt-derived read fence to exactly the next read. */
  readonly metadata = consistencyMetadata;
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
  /** Events: CDC subscribe(topic).ready() + publishAndWait over DataBroker. */
  readonly events: EventsFacade;
  /** Admin: migration lifecycle helpers (`.admin.migrations.applyCurrent`). */
  readonly admin: AdminFacade;

  private readonly tokenStore: TokenStore;
  private refreshInFlight: Promise<StoredToken> | null = null;
  /** Dedicated generated client for the auth/control-plane target when
   * `authTarget` differs from `target`; otherwise null (native facades share
   * `this.generated`). */
  private readonly authGenerated: UdbGeneratedClient | null = null;
  /** Dedicated generated client for the WebRTC target, when `webrtcTarget`
   *  differs from both `target` and `authTarget`; otherwise null. */
  private readonly webrtcGenerated: UdbGeneratedClient | null = null;

  /** The single UdbMetadata instance shared by reference across every sub-client
   *  core (data, auth, native, webrtc). Mutating it (e.g. via setTenant) updates
   *  the x-tenant-id / x-* headers on ALL outbound channels at once. */
  private readonly sharedMeta: UdbMetadata;

  /**
   * Canonical construction entry point (naming contract): build a
   * {@link UdbProject} from a single shared config. Async-shaped so the
   * `simple_client_code.md` headline `await UdbProject.connect({...})` reads as a
   * connect; construction itself is synchronous (channels connect lazily), so no
   * extra round trip is performed. `createUdb()` / `Udb.project()` remain as
   * (synchronous) aliases.
   */
  static connect(config: UdbProjectConfig): Promise<UdbProject> {
    return Promise.resolve(new UdbProject(config));
  }

  constructor(private readonly config: UdbProjectConfig) {
    const meta = metaFromConfig(config);
    this.sharedMeta = meta;
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

    // Augment the raw DataBroker surface with the bound table/entity accessors
    // (additive — the generated RPCs are untouched) so the headline form
    // `udb.data.table("invoice").select({ where })` works. Bound to `this`.
    const dataPlane = this.generated.DataBroker as DataPlane;
    dataPlane.entity = <T = any>(messageType: string, opts?: Partial<EntityHandleOptions>) =>
      this.entity<T>(messageType, opts);
    dataPlane.table = <T = any>(name: string, opts?: { key?: string[] }) =>
      this.table<T>(name, opts);
    this.data = dataPlane;
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
    // Events ride the data-plane DataBroker channel, tenant-scoped via sharedMeta.
    this.events = new EventsFacade(this.core, this.sharedMeta);
    // Migration admin rides the data-plane DataBroker channel.
    this.admin = new AdminFacade(this.core);

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

  /** Adopt the canonical tenant id on EVERY outbound channel. After a password
   *  login with a human tenant code (`tenant_hint: "acme"`), the broker mints a
   *  bearer whose tenant claim is the canonical tenant UUID; subsequent native
   *  RPCs reject a mismatched `x-tenant-id`. Call this with the resolved
   *  `principal.tenant_id` so the shared metadata sends the UUID, not the code. */
  setTenant(tenantId: string): void {
    this.sharedMeta.tenantId = tenantId;
  }

  /** A typed entity handle bound to a DataBroker message type. Hides
   *  `record_json`/`conflict_fields`/Struct-filter/decode. The key (and optional
   *  tenant/project field names) come from `opts` or, when omitted, from the
   *  generated `ENTITY_REGISTRY` (lane 07's descriptor-driven catalog) — falling
   *  back to `["id"]`. No catalog round-trip at construction. */
  entity<T = any>(
    messageType: string,
    opts?: Partial<EntityHandleOptions>,
  ): EntityHandle<T> {
    const binding = ENTITY_REGISTRY[messageType];
    const resolved: EntityHandleOptions = {
      key: opts?.key ?? binding?.key ?? ["id"],
      tenantField: opts?.tenantField ?? binding?.tenantField,
      projectField: opts?.projectField ?? binding?.projectField,
    };
    const context = {
      tenant_id: this.sharedMeta.tenantId,
      project_id: this.sharedMeta.projectId ?? "",
    };
    return new EntityHandle<T>(this.core, messageType, resolved, context);
  }

  /** Thin table-shaped alias over {@link entity}. Resolves the message type from
   *  a registry binding whose `table` matches `name` (else uses `name` as the
   *  message type); caller-supplied `opts.key` wins, default `["id"]`. Forwards to
   *  the same DataBroker Upsert/Select/Delete RPCs. */
  table<T = any>(name: string, opts?: { key?: string[] }): EntityHandle<T> {
    const binding = Object.values(ENTITY_REGISTRY).find((b) => b.table === name);
    const messageType = binding?.messageType ?? name;
    return this.entity<T>(messageType, {
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
  async loginAndAdoptTenant(request: {
    username: string;
    password: string;
    [key: string]: any;
  }): Promise<any> {
    const resp: any = await this.login(request);
    if (resp?.mfa_required) return resp;
    const token: string = resp?.access_token || resp?.session_token || "";
    if (!token) return resp;
    // login() already installed credentials; authenticate-bearer verifies them.
    const verified: any = await this.auth.authenticateBearer(token);
    const principal = verified?.principal ?? {};
    const tenantId: string = principal?.tenant_id ?? "";
    if (tenantId) this.setTenant(tenantId);
    const projectId: string = principal?.project_id ?? "";
    if (projectId) this.sharedMeta.projectId = projectId;
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

  /**
   * Grant a role permission on a resource/action. Emits EXACTLY one
   * `AuthzService.CreatePolicyRule` RPC (effect = ALLOW) — NO hidden List/Get.
   * `role` becomes the policy `subject`, `resource` the `object`, `action` the
   * `action`. Tenant/project default to the bound metadata.
   */
  allowRole(
    role: string,
    grant: { resource: string; action: string; effect?: string },
    extra: Record<string, unknown> = {},
  ): Promise<any> {
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
  bindRole(subject: string, role: string, extra: Record<string, unknown> = {}): Promise<any> {
    return this.client.assignRole({
      user_id: subject,
      principal_id: subject,
      role_id: role,
      ...extra,
    });
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
