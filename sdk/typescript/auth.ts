// Hand-written auth ergonomics over the generated AuthnService / AuthzService,
// mirroring `client.ts`'s dynamic proto-loader + metadata convention (item 108).
//
// Loads the native auth service protos from the same proto root the broker
// client uses; `google/api` annotation imports resolve from the vendored
// `third_party/googleapis` include dir.
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";

import { UdbMetadata, metadata } from "./client";
import { defaultProtoRoot } from "./protoRoot";

function loadAuth(target: string, protoRoot: string) {
  const includeDirs = [protoRoot, path.resolve(protoRoot, "../third_party/googleapis")];
  const opts = { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true, includeDirs };
  const authnDef = protoLoader.loadSync("udb/core/authn/services/v1/authn_service.proto", opts);
  const authzDef = protoLoader.loadSync("udb/core/authz/services/v1/authz_service.proto", opts);
  const authn = grpc.loadPackageDefinition(authnDef) as any;
  const authz = grpc.loadPackageDefinition(authzDef) as any;
  const creds = grpc.credentials.createInsecure();
  return {
    authn: new authn.udb.core.authn.services.v1.AuthnService(target, creds),
    authz: new authz.udb.core.authz.services.v1.AuthzService(target, creds),
  };
}

/** Convenience wrapper over AuthnService + AuthzService. The same `UdbMetadata`
 *  used for broker calls is attached to every auth RPC. */
export class UdbAuthClient {
  private authn: any;
  private authz: any;
  private meta: UdbMetadata;

  constructor(target: string, meta: UdbMetadata, protoRoot = defaultProtoRoot()) {
    const clients = loadAuth(target, protoRoot);
    this.authn = clients.authn;
    this.authz = clients.authz;
    this.meta = meta;
  }

  private call<T>(client: any, method: string, request: any): Promise<T> {
    return new Promise((resolve, reject) => {
      client[method](request, metadata(this.meta), (err: grpc.ServiceError | null, resp: T) => {
        if (err) reject(err);
        else resolve(resp);
      });
    });
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

  // ── Authorization ─────────────────────────────────────────────────────────
  async authorize(request: any): Promise<any> {
    const resp: any = await this.call(this.authz, "Authorize", request);
    return resp.decision;
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
  async getPolicyBundle(): Promise<any> {
    const resp: any = await this.call(this.authz, "GetPolicyBundle", {
      tenant_id: this.meta.tenantId,
      project_id: this.meta.projectId ?? "",
    });
    return resp?.bundle;
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

  invalidate(): void {
    this.cache.clear();
  }
}
