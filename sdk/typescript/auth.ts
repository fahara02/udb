// Hand-written auth ergonomics over the generated AuthnService / AuthzService,
// mirroring `client.ts`'s dynamic proto-loader + metadata convention (item 108).
//
// Loads the native auth service protos from the same proto root the broker
// client uses; `google/api` annotation imports resolve from the vendored
// `third_party/googleapis` include dir.
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import * as crypto from "crypto";
import path from "path";

import { UdbMetadata, metadata, UDB_DEFAULT_CHANNEL_OPTIONS } from "./client";
import type { TlsOptions } from "./generatedClient";
import { defaultProtoRoot } from "./protoRoot";

export interface UdbAuthClientOptions {
  secure?: boolean;
  tls?: TlsOptions;
  channelOptions?: grpc.ChannelOptions;
}

function buildCredentials(opts: UdbAuthClientOptions): grpc.ChannelCredentials {
  if (opts.tls) {
    return grpc.credentials.createSsl(
      opts.tls.rootCerts,
      opts.tls.privateKey,
      opts.tls.certChain,
    );
  }
  if (opts.secure) {
    return grpc.credentials.createSsl();
  }
  return grpc.credentials.createInsecure();
}

function loadAuth(target: string, protoRoot: string, opts: UdbAuthClientOptions = {}) {
  const includeDirs = [protoRoot, path.resolve(protoRoot, "../third_party/googleapis")];
  const loaderOptions = {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs,
  };
  const authnDef = protoLoader.loadSync(
    "udb/core/authn/services/v1/authn_service.proto",
    loaderOptions,
  );
  const authzDef = protoLoader.loadSync(
    "udb/core/authz/services/v1/authz_service.proto",
    loaderOptions,
  );
  const authn = grpc.loadPackageDefinition(authnDef) as any;
  const authz = grpc.loadPackageDefinition(authzDef) as any;
  const creds = buildCredentials(opts);
  const channelOptions = { ...UDB_DEFAULT_CHANNEL_OPTIONS, ...(opts.channelOptions ?? {}) };
  return {
    authn: new authn.udb.core.authn.services.v1.AuthnService(target, creds, channelOptions),
    authz: new authz.udb.core.authz.services.v1.AuthzService(target, creds, channelOptions),
  };
}

/** A single permission to check in a `batchCan` call. Mirrors the authz
 *  `PermissionCheck` message ({ object, action }). */
export interface PermissionCheck {
  /** The object/resource identifier the policy engine keys on (authz `object`). */
  object: string;
  /** The action being attempted (authz `action`). */
  action: string;
}

/** Thrown by `require()` when the bound principal is NOT allowed to perform the
 *  action. Carries the full server `Decision` so callers can inspect the
 *  `decision_id`, `deny_reason`, and `required_scopes`. */
export class UdbAuthzDenied extends Error {
  readonly resource: any;
  readonly action: string;
  readonly purpose: string;
  /** The server `Decision` message (decision_id / allowed / deny_reason / …). */
  readonly decision: any;

  constructor(resource: any, action: string, purpose: string, decision: any) {
    const reason = decision?.deny_reason || "not allowed";
    const id = decision?.decision_id ? ` [decision ${decision.decision_id}]` : "";
    super(`udb: authorization denied for action '${action}': ${reason}${id}`);
    this.name = "UdbAuthzDenied";
    this.resource = resource;
    this.action = action;
    this.purpose = purpose;
    this.decision = decision;
  }
}

/** A server-issued, time-boxed `SignedPolicyBundle` (authz `core.proto`). The
 *  `bundle` is the canonical-JSON snapshot payload (delivered as a `Buffer`
 *  over the wire); `signature` is a lowercase-hex HMAC-SHA256 over those bytes. */
export interface SignedPolicyBundle {
  /** The canonical-JSON snapshot payload the signature covers. */
  bundle: Buffer | Uint8Array | string;
  /** Lowercase-hex HMAC-SHA256 over `bundle`. (The proto comment says Base64;
   *  the server actually emits lowercase hex — this SDK matches the server.) */
  signature: string;
  /** Identifier of the signing key, for rotation/verification. */
  key_id?: string;
  algorithm?: string;
  expires_at_unix?: number | string;
}

/** Thrown by {@link verifyPolicyBundle} / {@link UdbAuthClient.getPolicyBundle}
 *  when a signed policy bundle fails HMAC verification. */
export class UdbPolicyBundleError extends Error {
  constructor(message: string) {
    super(`udb: policy bundle verification failed: ${message}`);
    this.name = "UdbPolicyBundleError";
  }
}

function bundleBytes(bundle: Buffer | Uint8Array | string): Buffer {
  if (typeof bundle === "string") return Buffer.from(bundle, "utf8");
  return Buffer.isBuffer(bundle) ? bundle : Buffer.from(bundle);
}

/** Throw a loud, typed error when a gated conformance-proof field is empty
 *  (broker not in conformance mode). */
function requireProof(value: unknown, field: string): string {
  const s = typeof value === "string" ? value : "";
  if (!s) {
    throw new Error(
      `udb: conformanceProof: empty ${field} — broker is not in conformance/dev-echo mode`,
    );
  }
  return s;
}

/** Decode an RFC-4648 base32 secret (TOTP `totp_secret`) to bytes. Tolerates
 *  lowercase + padding. */
function base32Decode(secret: string): Buffer {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
  const clean = secret.replace(/=+$/, "").toUpperCase().replace(/\s+/g, "");
  let bits = 0;
  let value = 0;
  const out: number[] = [];
  for (const ch of clean) {
    const idx = alphabet.indexOf(ch);
    if (idx === -1) continue;
    value = (value << 5) | idx;
    bits += 5;
    if (bits >= 8) {
      bits -= 8;
      out.push((value >>> bits) & 0xff);
    }
  }
  return Buffer.from(out);
}

/** Compute the current 6-digit RFC-6238 TOTP code (SHA-1, 30s step) from a
 *  base32 `totp_secret`. Stdlib `crypto` only — no new dependency. */
function totpNow(secret: string, stepSeconds = 30, digits = 6): string {
  const key = base32Decode(secret);
  const counter = Math.floor(Date.now() / 1000 / stepSeconds);
  const buf = Buffer.alloc(8);
  // 64-bit big-endian counter (high 32 bits are ~0 for current epochs).
  buf.writeUInt32BE(Math.floor(counter / 0x100000000), 0);
  buf.writeUInt32BE(counter >>> 0, 4);
  const hmac = crypto.createHmac("sha1", key).update(buf).digest();
  const offset = hmac[hmac.length - 1] & 0x0f;
  const code =
    ((hmac[offset] & 0x7f) << 24) |
    ((hmac[offset + 1] & 0xff) << 16) |
    ((hmac[offset + 2] & 0xff) << 8) |
    (hmac[offset + 3] & 0xff);
  return (code % Math.pow(10, digits)).toString().padStart(digits, "0");
}

/**
 * Recompute the HMAC-SHA256 (lowercase hex) of `signed.bundle` keyed by `secret`
 * and compare it constant-time against `signed.signature`. Returns `true` on a
 * match, `false` otherwise (including a missing/empty signature). Never throws on
 * a mismatch — callers that want a throw should use {@link UdbAuthClient.getPolicyBundle}
 * with a configured `policyBundleSecret`.
 *
 * The server emits the signature as lowercase hex (the proto comment that says
 * Base64 is wrong); this verifier matches the server.
 */
export function verifyPolicyBundle(signed: SignedPolicyBundle, secret: string): boolean {
  if (!signed || !secret) return false;
  const provided = signed.signature ?? "";
  const expected = crypto
    .createHmac("sha256", secret)
    .update(bundleBytes(signed.bundle))
    .digest("hex");
  // Constant-time compare; the lengths must match for timingSafeEqual.
  const a = Buffer.from(expected, "utf8");
  const b = Buffer.from(provided, "utf8");
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

/** Convenience wrapper over AuthnService + AuthzService. The same `UdbMetadata`
 *  used for broker calls is attached to every auth RPC. */
export class UdbAuthClient {
  private authn: any;
  private authz: any;
  private meta: UdbMetadata;
  private policyBundleSecret?: string;

  constructor(
    target: string,
    meta: UdbMetadata,
    protoRoot = defaultProtoRoot(),
    policyBundleSecret?: string,
    options: UdbAuthClientOptions = {},
  ) {
    const clients = loadAuth(target, protoRoot, options);
    this.authn = clients.authn;
    this.authz = clients.authz;
    this.meta = meta;
    this.policyBundleSecret = policyBundleSecret;
  }

  private call<T>(client: any, method: string, request: any): Promise<T> {
    return new Promise((resolve, reject) => {
      client[method](request, metadata(this.meta), (err: grpc.ServiceError | null, resp: T) => {
        if (err) reject(err);
        else resolve(resp);
      });
    });
  }

  setCredentials(credentials: { bearerToken?: string; apiKey?: string }): void {
    if ("bearerToken" in credentials) {
      this.meta.bearerToken = credentials.bearerToken;
    }
    if ("apiKey" in credentials) {
      this.meta.apiKey = credentials.apiKey;
    }
  }

  // ── Authentication ──────────────────────────────────────────────────────
  authenticate(request: any): Promise<any> {
    return this.call(this.authn, "Authenticate", request);
  }

  authenticateBearer(token: string): Promise<any> {
    return this.authenticate({
      bearer_token: token,
      tenant_hint: this.meta.tenantId,
      project_hint: this.meta.projectId ?? "",
      requested_scopes: this.meta.scopes ?? [],
    });
  }

  authenticateApiKey(apiKey: string): Promise<any> {
    return this.authenticate({
      api_key: apiKey,
      tenant_hint: this.meta.tenantId,
      project_hint: this.meta.projectId ?? "",
      requested_scopes: this.meta.scopes ?? [],
    });
  }

  authenticateSession(sessionId: string): Promise<any> {
    return this.authenticate({
      session_id: sessionId,
      tenant_hint: this.meta.tenantId,
      project_hint: this.meta.projectId ?? "",
    });
  }

  login(request: any): Promise<any> {
    return this.call(this.authn, "Login", request);
  }

  /** Canonical login accessor (naming contract): start an authenticated session
   *  via `AuthnService.Login`. Thin alias of {@link login} — EXACTLY one Login
   *  RPC, no hidden round trip. For automatic token-store persistence + refresh,
   *  use `UdbProject.login` / `UdbProject.loginAndAdoptTenant`. */
  loginSession(creds: any): Promise<any> {
    return this.login(creds);
  }

  refreshToken(request: any): Promise<any> {
    return this.call(this.authn, "RefreshToken", request);
  }

  /** Better Auth bridge: forward a Better Auth session/JWT (already verified by
   *  the app) to UDB as an external identity. UDB maps the verified claims to a
   *  principal; UDB policy — not Better Auth roles — still decides authorization. */
  authenticateBetterAuth(token: string, providerId = "better-auth"): Promise<any> {
    return this.authenticate({
      external_provider_id: providerId,
      external_token: token,
      tenant_hint: this.meta.tenantId,
      project_hint: this.meta.projectId ?? "",
    });
  }

  // ── Conformance proof (dev/test only) ─────────────────────────────────────
  /**
   * Surface the gated proof material a HEADLESS conformance harness needs — it
   * does NOT bypass verification, it only reads what the broker's prod-guarded
   * dev-echo gate already exposes:
   *   - "otp"            → `SendOTP(...).dev_otp_code`
   *   - "password_reset" → `ForgotPassword(...).dev_otp_code`
   *   - "phone"          → `SendPhoneVerification(...).dev_otp_code`
   *   - "totp"           → compute the current RFC-6238 code from
   *                        `EnrollMFA(...).totp_secret` (no broker echo for TOTP)
   *
   * Rejects when the proof field is empty (broker NOT in conformance mode) so
   * callers fail loud, not silent. `args` carries the per-kind request fields
   * (e.g. `{ user_id }` / `{ identifier }` / `{ user_id, phone }`).
   */
  async conformanceProof(
    kind: "otp" | "password_reset" | "phone" | "totp",
    args: Record<string, any> = {},
  ): Promise<string> {
    const ctx = { context: args.context, ...args };
    switch (kind) {
      case "otp": {
        const resp: any = await this.call(this.authn, "SendOTP", ctx);
        return requireProof(resp?.dev_otp_code, "SendOTP.dev_otp_code");
      }
      case "password_reset": {
        const resp: any = await this.call(this.authn, "ForgotPassword", ctx);
        return requireProof(resp?.dev_otp_code, "ForgotPassword.dev_otp_code");
      }
      case "phone": {
        const resp: any = await this.call(this.authn, "SendPhoneVerification", ctx);
        return requireProof(resp?.dev_otp_code, "SendPhoneVerification.dev_otp_code");
      }
      case "totp": {
        const resp: any = await this.call(this.authn, "EnrollMFA", ctx);
        const secret: string = resp?.totp_secret ?? "";
        if (!secret) {
          throw new Error("udb: conformanceProof(totp): EnrollMFA returned no totp_secret");
        }
        return totpNow(secret);
      }
    }
  }

  /** Explicit WebAuthn passkey flows. Each sequences Start → Finish over the
   *  generated ceremony RPCs, threading the single-use Start challenge into
   *  Finish — exactly two RPCs per flow, no hidden Get. */
  readonly passkeys = {
    /** Register a passkey: StartWebAuthnRegistration → FinishWebAuthnRegistration.
     *  `credentialJson` is the authenticator's credential (or the dev soft-auth
     *  `TEST_CREDENTIAL_SENTINEL` when driving the test authenticator). */
    register: async (
      args: { user_id: string; label?: string; tenant_id?: string; project_id?: string; context?: any },
      credentialJson: string,
    ): Promise<any> => {
      const start: any = await this.call(this.authn, "StartWebAuthnRegistration", {
        user_id: args.user_id,
        label: args.label ?? "",
        tenant_id: args.tenant_id ?? this.meta.tenantId,
        project_id: args.project_id ?? this.meta.projectId ?? "",
        context: args.context,
      });
      return this.call(this.authn, "FinishWebAuthnRegistration", {
        challenge_id: start?.challenge_id ?? "",
        public_key_credential_json: credentialJson,
        label: args.label ?? "",
        context: args.context,
      });
    },
    /** Authenticate with a passkey: StartWebAuthnAuthentication →
     *  FinishWebAuthnAuthentication. */
    authenticate: async (
      args: { user_id?: string; tenant_id?: string; project_id?: string; context?: any },
      credentialJson: string,
    ): Promise<any> => {
      const start: any = await this.call(this.authn, "StartWebAuthnAuthentication", {
        user_id: args.user_id ?? this.meta.userId ?? "",
        tenant_id: args.tenant_id ?? this.meta.tenantId,
        project_id: args.project_id ?? this.meta.projectId ?? "",
        context: args.context,
      });
      return this.call(this.authn, "FinishWebAuthnAuthentication", {
        challenge_id: start?.challenge_id ?? "",
        public_key_credential_json: credentialJson,
        context: args.context,
      });
    },
  };

  // ── Authorization ─────────────────────────────────────────────────────────
  async authorize(request: any): Promise<any> {
    const resp: any = await this.call(this.authz, "Authorize", request);
    return resp.decision;
  }

  /** Raw `AuthzService.CreatePolicyRule` (one RPC, no hidden List/Get). Tenant
   *  defaults to the bound metadata when omitted. */
  createPolicyRule(request: any): Promise<any> {
    return this.call(this.authz, "CreatePolicyRule", {
      tenant_id: this.meta.tenantId,
      project_id: this.meta.projectId ?? "",
      ...request,
    });
  }

  /** Raw `AuthzService.AssignRole` (one RPC, no hidden List/Get). Tenant/domain
   *  default to the bound metadata when omitted. */
  assignRole(request: any): Promise<any> {
    return this.call(this.authz, "AssignRole", {
      tenant_id: this.meta.tenantId,
      project_id: this.meta.projectId ?? "",
      domain: this.meta.tenantId,
      ...request,
    });
  }

  /** Returns `[allowed, decision]` for the bound principal acting on a resource. */
  async can(resource: any, action: string, purpose = ""): Promise<[boolean, any]> {
    const decision = await this.authorize({
      principal: {
        user_id: this.meta.userId ?? "",
        service_identity: this.meta.serviceIdentity ?? "",
        tenant_id: this.meta.tenantId,
        project_id: this.meta.projectId ?? "",
        scopes: this.meta.scopes ?? [],
      },
      tenant_id: this.meta.tenantId,
      project_id: this.meta.projectId ?? "",
      resource,
      action,
      purpose: purpose || this.meta.purpose,
      requested_scopes: this.meta.scopes ?? [],
    });
    return [Boolean(decision?.allowed), decision];
  }

  /** Like `can`, but throws {@link UdbAuthzDenied} (carrying the full decision)
   *  when the principal is not allowed. Returns the allowing `Decision`. */
  async require(resource: any, action: string, purpose = ""): Promise<any> {
    const p = purpose || this.meta.purpose;
    const [allowed, decision] = await this.can(resource, action, p);
    if (!allowed) throw new UdbAuthzDenied(resource, action, p, decision);
    return decision;
  }

  /** Returns the full `Decision` (decision_id, allowed, deny_reason, …) without
   *  throwing, for callers that want to inspect the verdict rather than branch
   *  on a boolean. */
  async explain(resource: any, action: string, purpose = ""): Promise<any> {
    const [, decision] = await this.can(resource, action, purpose || this.meta.purpose);
    return decision;
  }

  /** Check many (object, action) pairs in a single round-trip via the authz
   *  `BatchCheckPermissions` RPC. Returns the server's `results` map keyed by
   *  `"object:action"` → allowed, plus a `lookup(object, action)` helper. */
  async batchCan(
    checks: PermissionCheck[],
  ): Promise<{ results: Record<string, boolean>; lookup: (object: string, action: string) => boolean }> {
    const resp: any = await this.call(this.authz, "BatchCheckPermissions", {
      user_id: this.meta.userId ?? "",
      domain: this.meta.tenantId,
      checks: checks.map((c) => ({ object: c.object, action: c.action })),
      context: {},
    });
    const results: Record<string, boolean> = resp?.results ?? {};
    return {
      results,
      lookup: (object: string, action: string) => Boolean(results[`${object}:${action}`]),
    };
  }

  // ── Stage 2: native database fast-path access (item 138) ──────────────────
  /** Authorize and, when allowed, return the native-access grant (restricted
   *  role + scoped DSN + RLS session variables). Resolves to `null` when access
   *  is allowed but no grant was minted; rejects when the decision denied. */
  async nativeAccess(resource: any, action: string, purpose = ""): Promise<any | null> {
    const resp: any = await this.call(this.authz, "GetNativeAccess", {
      principal: {
        user_id: this.meta.userId ?? "",
        service_identity: this.meta.serviceIdentity ?? "",
        tenant_id: this.meta.tenantId,
        project_id: this.meta.projectId ?? "",
        scopes: this.meta.scopes ?? [],
      },
      tenant_id: this.meta.tenantId,
      project_id: this.meta.projectId ?? "",
      resource,
      action,
      purpose: purpose || this.meta.purpose,
      requested_scopes: this.meta.scopes ?? [],
    });
    if (resp?.decision && !resp.decision.allowed) {
      throw new Error(`udb: native access denied: ${resp.decision.deny_reason ?? ""}`);
    }
    return resp?.grant ?? null;
  }

  // ── Stage 2: signed policy bundle (item 140) ──────────────────────────────
  /** Fetch the server's `SignedPolicyBundle`. When a `policyBundleSecret` is
   *  configured the bundle's HMAC-SHA256 (lowercase hex) signature is verified
   *  before the bundle is returned, and {@link UdbPolicyBundleError} is thrown
   *  on mismatch. With no secret configured the bundle is returned unverified. */
  async getPolicyBundle(): Promise<SignedPolicyBundle | undefined> {
    const resp: any = await this.call(this.authz, "GetPolicyBundle", {
      tenant_id: this.meta.tenantId,
      project_id: this.meta.projectId ?? "",
    });
    const bundle: SignedPolicyBundle | undefined = resp?.bundle;
    if (this.policyBundleSecret && bundle) {
      if (!verifyPolicyBundle(bundle, this.policyBundleSecret)) {
        throw new UdbPolicyBundleError(
          `signature mismatch${bundle.key_id ? ` (key ${bundle.key_id})` : ""}`,
        );
      }
    }
    return bundle;
  }
}

/** Apply a native-access grant's `app.current_*` session variables and run a
 *  transaction. `client` is any node-postgres-style client exposing `query`;
 *  the SDK pulls in no driver of its own. Commits on success, rolls back on
 *  error so RLS context never leaks across requests. */
export async function withNativeTx<T>(
  client: { query: (sql: string, params?: any[]) => Promise<any> },
  grant: any,
  fn: () => Promise<T>,
): Promise<T> {
  await client.query("BEGIN");
  try {
    const vars: Record<string, string> = grant?.session_variables ?? {};
    for (const [key, value] of Object.entries(vars)) {
      await client.query("SELECT set_config($1, $2, true)", [key, value]);
    }
    const result = await fn();
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  }
}

/** Local authorization cache over `UdbAuthClient.can`, keyed by
 *  (principal, resource, action, purpose) and bounded by the server's
 *  `Decision.cache_ttl_seconds` (a zero TTL is never cached). */
export class AuthzCache {
  private cache = new Map<string, { expires: number; decision: any }>();
  constructor(
    private client: UdbAuthClient,
    private now: () => number = () => Date.now(),
  ) {}

  private static key(meta: UdbMetadata, resource: any, action: string, purpose: string): string {
    const subject = meta.userId || meta.serviceIdentity || "";
    const res = resource?.message_type || resource?.resource_name || resource?.table || "";
    return [meta.tenantId, meta.projectId ?? "", subject, res, action, purpose].join("");
  }

  async can(resource: any, action: string, purpose = ""): Promise<[boolean, any]> {
    const meta = (this.client as any).meta as UdbMetadata;
    const p = purpose || meta.purpose;
    const key = AuthzCache.key(meta, resource, action, p);
    const hit = this.cache.get(key);
    if (hit && this.now() < hit.expires) {
      return [Boolean(hit.decision?.allowed), hit.decision];
    }
    const [allowed, decision] = await this.client.can(resource, action, p);
    const ttl = Number(decision?.cache_ttl_seconds ?? 0);
    if (ttl > 0) {
      this.cache.set(key, { expires: this.now() + ttl * 1000, decision });
    }
    return [allowed, decision];
  }

  /** Cache-routed {@link UdbAuthClient.require}: throws {@link UdbAuthzDenied}
   *  on a denied (cached or fresh) decision; returns the allowing decision. */
  async require(resource: any, action: string, purpose = ""): Promise<any> {
    const meta = (this.client as any).meta as UdbMetadata;
    const p = purpose || meta.purpose;
    const [allowed, decision] = await this.can(resource, action, p);
    if (!allowed) throw new UdbAuthzDenied(resource, action, p, decision);
    return decision;
  }

  /** Cache-routed {@link UdbAuthClient.explain}: returns the full decision
   *  (cached or fresh) without throwing. */
  async explain(resource: any, action: string, purpose = ""): Promise<any> {
    const [, decision] = await this.can(resource, action, purpose);
    return decision;
  }

  invalidate(): void {
    this.cache.clear();
  }
}
