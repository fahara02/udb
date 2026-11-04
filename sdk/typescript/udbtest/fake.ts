// An in-memory udb broker for unit tests.
//
// It serves the DataBroker data verbs (Select, Insert, Upsert, Update, Delete),
// atomic transactions, durable CDC consumers and operator DLQ APIs, plus the API-key
// exchange, over a real loopback gRPC listener, so the code under test uses an
// unmodified UdbProject. Its semantics follow the broker's: the caller's tenant is
// filled into filters and records (a different tenant is refused), primary keys are
// unique, conditional writes compare by value, require_affected is enforced, reads
// page with has_more and exact totals, and every refusal carries the broker's stable
// UDB_* reason in the typed error detail.
//
// The same contract (conformance.ts) runs against this fake and a real broker,
// which keeps the two from drifting.

import { createHash, randomBytes, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

import { UdbProject } from "../project";
import { defaultProtoRoot } from "../protoRoot";
import { ProtoSchemas, protoSchemas } from "../protoSchema";
import { parseExactJson, recordToJson } from "../recordCodec";
import { structToObject } from "../wkt";
import { EntityRules, Identity, columnRefusal, copyValue, uniqueRefusal } from "./entityRules";
import { Row, compare, equal, holds, matches } from "./filter";
import { Policy, loadPolicies } from "./policy";
import { Refusal, errorKind, refuse, statusOf } from "./refusal";

const codes = grpc.status;

/** A test context that can run cleanup; node:test's `t` is one. */
export interface TestHooks {
  after(fn: () => unknown): void;
}

/** A bounded number of RPC failures, injected before any mutation applies. `method`
 *  is a bare RPC name or full path; empty matches any RPC. */
export interface Fault {
  method?: string;
  tenantId?: string;
  code: grpc.status;
  reason?: string;
  /** How many calls fail (default 1). */
  times?: number;
  retryAfterMs?: number;
}

export type { Policy } from "./policy";

/** One outbox event a committed transaction emitted. */
export interface FakeEvent {
  topic: string;
  partitionKey: string;
  envelope: Row;
}

interface Token {
  identity: Identity;
  expires: number;
}

interface Ctx {
  identity: Identity;
  token: Token;
  metadata: grpc.Metadata;
}

interface Cursor {
  owner: string;
  index: number;
  id: string;
}

interface DlqEntry {
  identity: Identity;
  event: FakeEvent;
  record: Row;
  failures: Row;
  failureMetadata: Row;
}

function wildcard(pattern: string | undefined, value: string): boolean {
  const trimmed = (pattern ?? "").trim();
  return trimmed === "" || trimmed === "*" || trimmed === value;
}

function policyPattern(pattern: string | undefined, value: string): boolean {
  const trimmed = (pattern ?? "").trim();
  if (wildcard(trimmed, value)) return true;
  if (trimmed.endsWith(".*")) {
    const prefix = trimmed.slice(0, -2);
    return value === prefix || value.startsWith(`${prefix}.`);
  }
  return false;
}

function hasScope(identity: Identity, scope: string): boolean {
  if (scope.trim() === "") return true;
  return (identity.scopes ?? []).some((granted) => granted === scope || granted === "*" || granted === "udb:*");
}

function topicMatches(pattern: string, topic: string): boolean {
  if (pattern === "*" || pattern === topic) return true;
  return pattern.endsWith("*") && topic.startsWith(pattern.slice(0, -1));
}

function topicPattern(pattern: string | undefined): string {
  const trimmed = (pattern ?? "").trim();
  return trimmed === "" ? "*" : trimmed;
}

function consumerOwner(identity: Identity): string {
  if (identity.serviceIdentity) return `service:${identity.serviceIdentity}`;
  if (identity.userId) return `user:${identity.userId}`;
  throw refuse(codes.UNAUTHENTICATED, "", "named CDC consumer requires a verified user or service identity");
}

function consumerName(raw: string): string {
  const name = raw.trim();
  if (name.length < 1 || name.length > 120) throw refuse(codes.INVALID_ARGUMENT, "", "invalid consumer_name length");
  if (!/^[A-Za-z0-9_.:-]+$/.test(name)) throw refuse(codes.INVALID_ARGUMENT, "", "invalid consumer_name character");
  return name;
}

function cursorKey(identity: Identity, consumer: string, pattern: string): string {
  return [identity.tenantId, identity.projectId ?? "", consumer, pattern].join("\0");
}

function cursorOwnerError(): Refusal {
  return refuse(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "named CDC consumer belongs to another verified principal");
}

function eventInScope(event: FakeEvent, identity: Identity, pattern: string): boolean {
  const project = typeof event.envelope.project_id === "string" ? event.envelope.project_id : "";
  return event.envelope.tenant_id === identity.tenantId && project === (identity.projectId ?? "") && topicMatches(pattern, event.topic);
}

function sameDomain(a: Identity, b: Identity): boolean {
  return a.tenantId === b.tenantId && (a.projectId ?? "") === (b.projectId ?? "");
}

function missingScopeRefusal(scope: string): Refusal {
  return refuse(codes.PERMISSION_DENIED, "UDB_SCOPE_MISSING", `verified identity lacks ${scope}`, {
    reason: "UDB_SCOPE_MISSING",
    kind: "ERROR_KIND_PERMISSION",
    missing: { scope },
  });
}

function bindBody(identity: Identity, body: any): void {
  const tenant = body?.tenant_id;
  if (tenant && tenant !== identity.tenantId) {
    throw refuse(codes.PERMISSION_DENIED, "UDB_TENANT_MISMATCH", "body tenant differs from verified tenant");
  }
  const project = body?.project_id;
  if (project && project !== (identity.projectId ?? "")) {
    throw refuse(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "body project differs from verified project");
  }
}

function struct(value: unknown): Row {
  return value ? structToObject(value) : {};
}

function hasFields(value: any): boolean {
  return Object.keys(value?.fields ?? {}).length > 0;
}

function decodeObject(raw: unknown): Row {
  let value: unknown;
  try {
    value = parseExactJson(Buffer.from((raw ?? Buffer.alloc(0)) as Uint8Array).toString("utf8"));
  } catch {
    throw refuse(codes.INVALID_ARGUMENT, "", "record_json must be a JSON object");
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw refuse(codes.INVALID_ARGUMENT, "", "record_json must be a JSON object");
  }
  return value as Row;
}

const DEFINITIONS = new Map<string, any>();

function loadedServices(root: string): any {
  let loaded = DEFINITIONS.get(root);
  if (loaded) return loaded;
  const files: string[] = [];
  const walk = (dir: string) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.name.endsWith(".proto")) files.push(full);
    }
  };
  walk(path.join(root, "udb"));
  loaded = grpc.loadPackageDefinition(
    protoLoader.loadSync(files, {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
      includeDirs: [root, path.resolve(root, "../third_party/googleapis")],
    }),
  );
  DEFINITIONS.set(root, loaded);
  return loaded;
}

/** The in-memory broker. */
export class Fake {
  private readonly entities = new Map<string, EntityRules>();
  // message type → tenant → primary-key string → row.
  private rowsByType = new Map<string, Map<string, Map<string, Row>>>();
  private eventLog: FakeEvent[] = [];
  private readonly cursors = new Map<string, Cursor>();
  private readonly dlq = new Map<string, DlqEntry>();
  private readonly receipts = new Map<string, { hash: string; response: Row }>();
  private revisions = new Map<string, number>();
  private readonly identities = new Map<string, Identity>();
  private readonly tokens = new Map<string, Token>();
  private readonly faults: (Fault & { times: number })[] = [];
  private tokenLifetimeMs = 3_600_000;
  private policies: Policy[] = [];
  private defaultAllow = false;
  private readonly tuples: { subject: string; relation: string; object: string; tenant: string; project: string }[] = [];
  private rate = { limit: 0, windowMs: 0, perPrincipal: false, buckets: new Map<string, { start: number; count: number }>() };
  private server?: grpc.Server;
  private readonly clients: UdbProject[] = [];
  private emitted = 0;
  private address = "";

  /** Use {@link createFake} or {@link createDescriptorFake}. */
  constructor(
    private readonly schemas: ProtoSchemas,
    private readonly protoRoot: string,
  ) {}

  /** Add an entity message type. Its annotations define keys and the tenant column. */
  register(messageType: string): void {
    this.entities.set(messageType, new EntityRules(this.schemas.message(messageType), this.schemas));
  }

  /** The fake's gRPC address. */
  get target(): string {
    return this.address;
  }

  /** @internal */
  configure(policies: Policy[], defaultAllow: boolean): void {
    this.policies = policies;
    this.defaultAllow = defaultAllow;
  }

  /** Make `key` exchangeable for a bearer of `tenant`. */
  addApiKey(key: string, tenant: string): void {
    this.addIdentityApiKey(key, {
      tenantId: tenant,
      serviceIdentity: `udbtest-service-${tenant}`,
      scopes: ["udb:read", "udb:write", "udb:admin", "udb:cdc:read", "udb:cdc:subscribe"],
    });
  }

  /** Provision immutable verified identity attributes. */
  addIdentityApiKey(key: string, identity: Identity): void {
    this.identities.set(key, {
      ...identity,
      scopes: [...(identity.scopes ?? [])],
      roles: [...(identity.roles ?? [])],
      subject: identity.subject || identity.userId || identity.serviceIdentity,
    });
  }

  /** Connect a client for `tenant` with an exchanged API key, as a service would. It
   *  closes with the fake. */
  client(tenant: string): Promise<UdbProject> {
    const key = `udbtest-key-${tenant}`;
    this.addApiKey(key, tenant);
    return this.clientForKey(key);
  }

  /** Connect with a provisioned key without replacing its identity. */
  async clientForKey(key: string): Promise<UdbProject> {
    const identity = this.identities.get(key);
    if (!identity) throw new Error("udbtest: key is not provisioned");
    const client = await UdbProject.connect({
      target: this.address,
      tenantId: identity.tenantId,
      projectId: identity.projectId ?? "",
      purpose: "test",
      credentials: { apiKey: key },
      protoRoot: this.protoRoot,
    });
    this.clients.push(client);
    return client;
  }

  /** A copy of every stored row of `messageType` for `tenant`, for assertions. */
  rows(messageType: string, tenant: string): Row[] {
    const table = this.rowsByType.get(messageType)?.get(tenant);
    return [...(table?.values() ?? [])].map((row) => copyValue(row)).sort((a, b) => (recordToJson(a) < recordToJson(b) ? -1 : 1));
  }

  /** The outbox events committed so far, in commit order. */
  events(): FakeEvent[] {
    return this.eventLog.map((event) => ({ ...event, envelope: copyValue(event.envelope) }));
  }

  /** Append an event as if a transaction of `tenant` had committed it, for tests that
   *  only exercise a consumer. Returns its event id. */
  emit(tenant: string, topic: string, partitionKey: string, payload: Row): string {
    this.emitted += 1;
    const id = `0190f1b2-0000-4000-8000-${this.emitted.toString(16).padStart(12, "0")}`;
    this.eventLog.push({
      topic,
      partitionKey,
      envelope: {
        event_id: id,
        event_type: topic,
        tenant_id: tenant,
        document_id: partitionKey,
        correlation_id: id,
        envelope_version: 2,
        payload: copyValue(payload),
        project_id: "",
      },
    });
    return id;
  }

  /** Fail the next matching calls with `fault.code`. */
  injectFault(fault: Fault): void {
    if (fault.code === codes.OK || (fault.times ?? 0) < 0 || (fault.retryAfterMs ?? 0) < 0) {
      throw new Error("udbtest: fault needs a failure code and non-negative count/delay");
    }
    const method = fault.method?.includes("/") ? fault.method.slice(fault.method.lastIndexOf("/") + 1) : fault.method;
    this.faults.push({ ...fault, method, times: fault.times || 1 });
  }

  /** Tune subsequently issued bearers, including refreshes. Existing bearers keep
   *  their expiry, as they do in production. */
  setTokenLifetime(lifetimeMs: number): void {
    if (lifetimeMs < 1000) throw new Error("udbtest: token lifetime must be at least one second");
    this.tokenLifetimeMs = lifetimeMs;
  }

  /** The broker's default tenant/operation fixed-window bucket. Zero disables it. */
  setRateLimit(limit: number, windowMs: number): void {
    if (limit < 0 || (limit > 0 && windowMs <= 0)) throw new Error("udbtest: invalid rate limit");
    this.rate = { limit, windowMs, perPrincipal: false, buckets: new Map() };
  }

  /** The broker's optional per-principal bucket. */
  setRateLimitPerPrincipal(limit: number, windowMs: number): void {
    this.setRateLimit(limit, windowMs);
    this.rate.perPrincipal = true;
  }

  /** Provision the scoped tuple the broker's relationship store consults. */
  addRelationship(subject: string, relation: string, object: string, tenant: string, project: string): void {
    this.tuples.push({ subject, relation, object, tenant, project });
  }

  // ── Serving ───────────────────────────────────────────────────────────────

  /** @internal */
  async serve(): Promise<void> {
    const loaded = loadedServices(this.protoRoot);
    const server = new grpc.Server();
    const unary = (method: string, handler: (ctx: Ctx, request: any) => unknown) => (call: any, callback: any) => {
      try {
        const ctx = this.beforeRpc(method, call.metadata, call.request);
        callback(null, handler(ctx, call.request));
      } catch (err) {
        callback(statusOf(err));
      }
    };
    server.addService(loaded.udb.services.v1.DataBroker.service, {
      Select: unary("Select", (ctx, r) => this.select(ctx, r)),
      Upsert: unary("Upsert", (ctx, r) => this.keyedWrite(ctx, "upsert", r, () => this.write(ctx.identity, r, false))),
      Insert: unary("Insert", (ctx, r) => this.keyedWrite(ctx, "insert", r, () => this.write(ctx.identity, r, true))),
      Update: unary("Update", (ctx, r) => this.keyedWrite(ctx, "update", r, () => this.update(ctx.identity, r))),
      Delete: unary("Delete", (ctx, r) => this.keyedWrite(ctx, "delete", r, () => this.remove(ctx.identity, r))),
      AckCdcEvents: unary("AckCdcEvents", (ctx, r) => this.settle(ctx, r)),
      NackCdcEvents: unary("NackCdcEvents", (ctx, r) => this.nack(ctx, r)),
      ListDlqEvents: unary("ListDlqEvents", (ctx, r) => this.listDlq(ctx, r)),
      GetDlqEvent: unary("GetDlqEvent", (ctx, r) => ({ event: { ...this.dlqEntry(ctx, r.dlq_id).record } })),
      ReplayDlqEvent: unary("ReplayDlqEvent", (ctx, r) => this.replayDlq(ctx, r)),
      DismissDlqEvent: unary("DismissDlqEvent", (ctx, r) => this.setDlqStatus(ctx, r, "DISMISSED")),
      QuarantineDlqEvent: unary("QuarantineDlqEvent", (ctx, r) => this.setDlqStatus(ctx, r, "QUARANTINED")),
      BeginTx: (call: any) => this.beginTx(call),
      PublishCDC: (call: any) => this.publishCdc(call),
    });
    server.addService(loaded.udb.core.authn.services.v1.AuthnService.service, {
      Authenticate: (call: any, callback: any) => {
        try {
          this.admit("Authenticate", {} as Identity, call.request);
          callback(null, this.authenticate(call.request.api_key));
        } catch (err) {
          callback(statusOf(err));
        }
      },
    });
    const port = await new Promise<number>((resolve, reject) => {
      server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, bound) => (err ? reject(err) : resolve(bound)));
    });
    this.server = server;
    this.address = `127.0.0.1:${port}`;
  }

  /** Stop the listener and the clients it handed out. */
  close(): void {
    for (const client of this.clients.splice(0)) client.close();
    this.server?.forceShutdown();
    this.server = undefined;
  }

  private entity(messageType: string): EntityRules {
    const entity = this.entities.get(messageType);
    if (!entity) {
      throw refuse(codes.INVALID_ARGUMENT, "UDB_UNKNOWN_MESSAGE_TYPE", `unknown message_type ${messageType}: the fake serves no such entity`);
    }
    if (entity.schemaError) throw refuse(codes.FAILED_PRECONDITION, "", entity.schemaError);
    return entity;
  }

  private beforeRpc(method: string, metadata: grpc.Metadata, request: any): Ctx {
    const authorization = metadata.get("authorization");
    if (authorization.length !== 1 || !String(authorization[0]).startsWith("Bearer ")) {
      throw refuse(codes.UNAUTHENTICATED, "", "a verified bearer is required");
    }
    const token = this.tokens.get(String(authorization[0]).slice("Bearer ".length));
    if (!token || Date.now() >= token.expires) throw refuse(codes.UNAUTHENTICATED, "", "bearer is unknown or expired");
    const identity = token.identity;
    for (const key of ["x-tenant-id", "x-udb-tenant-id"]) {
      const supplied = metadata.get(key);
      if (supplied.length > 1 || (supplied.length === 1 && supplied[0] !== identity.tenantId)) {
        throw refuse(codes.PERMISSION_DENIED, "UDB_TENANT_MISMATCH", "tenant header differs from verified tenant");
      }
    }
    for (const key of ["x-project-id", "x-udb-project-id"]) {
      const supplied = metadata.get(key);
      if (supplied.length > 1 || (supplied.length === 1 && supplied[0] !== (identity.projectId ?? ""))) {
        throw refuse(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "project header differs from verified project");
      }
    }
    const ctx: Ctx = { identity, token, metadata };
    if (request) {
      bindBody(identity, request.context);
      if (["ListDlqEvents", "GetDlqEvent", "ReplayDlqEvent", "DismissDlqEvent", "QuarantineDlqEvent"].includes(method) && !hasScope(identity, "udb:admin")) {
        throw missingScopeRefusal("udb:admin");
      }
      if (typeof request.message_type === "string") {
        this.authorize(ctx, request.message_type, method === "Insert" ? "Upsert" : method);
      }
    }
    this.admit(method, identity, request);
    return ctx;
  }

  private authorize(ctx: Ctx, resource: string, action: string): void {
    const purposes = ctx.metadata.get("x-purpose");
    const purpose = purposes.length ? String(purposes[0]) : "";
    if (purpose.trim() === "") throw refuse(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "purpose is required");
    if (!this.allows(ctx.identity, resource, action, purpose)) {
      throw refuse(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "no matching allow or an explicit deny policy", {
        reason: "UDB_POLICY_DENIED",
        kind: "ERROR_KIND_POLICY",
        missing: { rule: `${action} ${resource}`, purpose },
      });
    }
  }

  private allows(identity: Identity, resource: string, action: string, purpose: string): boolean {
    let allowed = this.policies.length === 0 && this.defaultAllow;
    for (const policy of this.policies) {
      if (policy.enabled === false) continue;
      if (
        !wildcard(policy.tenant, identity.tenantId) ||
        !wildcard(policy.project, identity.projectId ?? "") ||
        !policyPattern(policy.action, action) ||
        !policyPattern(policy.resource, resource) ||
        !wildcard(policy.purpose, purpose)
      ) {
        continue;
      }
      const subject = (policy.subject ?? "").trim();
      if (subject && subject !== "*" && subject !== identity.subject && subject !== identity.userId && subject !== identity.serviceIdentity) continue;
      const role = (policy.role ?? "").trim();
      if (role && role !== "*" && !(identity.roles ?? []).includes(role)) continue;
      // The native data-plane query uses no caller-asserted attributes.
      if (Object.keys(policy.conditions ?? {}).length !== 0) continue;
      if (policy.relationship) {
        const related = this.tuples.some(
          (tuple) =>
            tuple.relation === policy.relationship &&
            tuple.object === resource &&
            (tuple.subject === identity.subject || tuple.subject === identity.userId || tuple.subject === identity.serviceIdentity) &&
            wildcard(tuple.tenant, identity.tenantId) &&
            wildcard(tuple.project, identity.projectId ?? ""),
        );
        if (!related) continue;
      }
      if (policy.effect === "deny") return false;
      if ((policy.required_scopes ?? []).every((scope) => hasScope(identity, scope))) allowed = true;
    }
    return allowed;
  }

  private admit(method: string, identity: Identity, request: any): void {
    const actual = method === "Authenticate" ? this.identities.get(request?.api_key) ?? identity : identity;
    for (const fault of this.faults) {
      if (fault.times === 0 || (fault.method && fault.method !== method) || (fault.tenantId && fault.tenantId !== actual.tenantId)) continue;
      fault.times -= 1;
      let reason = fault.reason ?? "";
      if (!reason) {
        if (fault.code === codes.UNAVAILABLE) reason = "UDB_BACKEND_UNAVAILABLE";
        else if (fault.code === codes.RESOURCE_EXHAUSTED) reason = "UDB_RATE_LIMITED";
        else if (fault.code === codes.ABORTED) reason = "UDB_CAS_CONFLICT";
      }
      throw refuse(fault.code, reason, `injected ${method} failure`, {
        reason,
        kind: errorKind(fault.code, reason),
        retryable: fault.code === codes.UNAVAILABLE || fault.code === codes.RESOURCE_EXHAUSTED,
        retry_after_ms: fault.retryAfterMs ?? 0,
      });
    }
    if (method === "Authenticate" || this.rate.limit === 0) return;
    let key = `${actual.tenantId}\0${method}`;
    if (this.rate.perPrincipal) key += `\0${actual.subject ?? ""}`;
    const now = Date.now();
    let bucket = this.rate.buckets.get(key);
    if (!bucket || now - bucket.start >= this.rate.windowMs) {
      bucket = { start: Math.floor(now / this.rate.windowMs) * this.rate.windowMs, count: 0 };
    }
    bucket.count += 1;
    this.rate.buckets.set(key, bucket);
    if (bucket.count <= this.rate.limit) return;
    throw refuse(codes.RESOURCE_EXHAUSTED, "UDB_RATE_LIMITED", "verified caller exceeded the configured rate limit", {
      reason: "UDB_RATE_LIMITED",
      kind: "ERROR_KIND_RATE_LIMITED",
      retryable: true,
      retry_after_ms: Math.max(1, this.rate.windowMs - (now - bucket.start)),
      operation: method,
      missing: { bucket: key, limit: String(this.rate.limit), window_ms: String(this.rate.windowMs), principal: actual.subject ?? "" },
    });
  }

  private authenticate(key: string): Row {
    const identity = this.identities.get(key);
    if (!identity || !key || !identity.tenantId) throw refuse(codes.UNAUTHENTICATED, "", "unknown API key");
    const token = randomBytes(32).toString("base64url");
    const expiresAtUnix = Math.floor((Date.now() + this.tokenLifetimeMs) / 1000);
    this.tokens.set(token, { identity, expires: expiresAtUnix * 1000 });
    return {
      access_token: token,
      expires_at_unix: expiresAtUnix,
      principal: {
        tenant_id: identity.tenantId,
        project_id: identity.projectId ?? "",
        principal_id: identity.subject ?? "",
        subject: identity.subject ?? "",
        user_id: identity.userId ?? "",
        service_identity: identity.serviceIdentity ?? "",
        scopes: identity.scopes ?? [],
        roles: identity.roles ?? [],
        expires_at_unix: expiresAtUnix,
      },
    };
  }

  // ── Data verbs ────────────────────────────────────────────────────────────

  private table(messageType: string, tenant: string): Map<string, Row> {
    let byTenant = this.rowsByType.get(messageType);
    if (!byTenant) {
      byTenant = new Map();
      this.rowsByType.set(messageType, byTenant);
    }
    let table = byTenant.get(tenant);
    if (!table) {
      table = new Map();
      byTenant.set(tenant, table);
    }
    return table;
  }

  /** The implicit column list and export-control gate: even a PII reader must name a
   *  PII column explicitly. */
  private readColumns(entity: EntityRules, ctx: Ctx, fields: readonly string[]): Set<string> {
    const selected = new Set<string>();
    let canRead = hasScope(ctx.identity, "udb:pii:read");
    const purpose = ctx.metadata.get("x-purpose");
    if (purpose.length === 1 && ["export", "verification", "audit"].includes(String(purpose[0]))) canRead = true;
    const guarded = (name: string) => {
      const field = entity.columns.get(name)!;
      return field.pii || field.encrypted || Boolean(field.column?.encrypted);
    };
    if (fields.length === 0) {
      for (const [name, field] of entity.columns) {
        if (field.pii || field.encrypted) continue;
        if (guarded(name) && !canRead) throw missingScopeRefusal("udb:pii:read");
        selected.add(name);
      }
      return selected;
    }
    for (const requested of fields) {
      const name = entity.aliases.get(requested.trim().toLowerCase());
      if (!name) throw refuse(codes.INVALID_ARGUMENT, "", `unknown selected column ${requested}`);
      if (guarded(name) && !canRead) throw missingScopeRefusal("udb:pii:read");
      selected.add(name);
    }
    return selected;
  }

  private select(ctx: Ctx, req: any): Row {
    const entity = this.entity(req.message_type);
    const identity = ctx.identity;
    const filter = entity.queryFilter(struct(req.filter));
    entity.checkTenant(filter, identity.tenantId);
    const selected = this.readColumns(entity, ctx, req.fields ?? []);
    if (req.include_revision) for (const column of entity.primaryKey) selected.add(column);
    const found: Row[] = [];
    for (const row of this.rowsByType.get(entity.messageType)?.get(identity.tenantId)?.values() ?? []) {
      if (entity.live(row) && entity.visibleTo(row, identity) && matches(row, filter)) found.push(row);
    }
    const order: { field: string; descending: boolean }[] = [];
    for (const requested of req.sort ?? []) {
      const column = entity.aliases.get(String(requested.field).trim().toLowerCase());
      if (!column) throw refuse(codes.INVALID_ARGUMENT, "", `unknown sort column ${requested.field}`);
      order.push({ field: column, descending: Boolean(requested.descending) });
    }
    for (const column of entity.primaryKey) order.push({ field: column, descending: false });
    found.sort((a, b) => {
      for (const key of order) {
        const c = compare(a[key.field] ?? null, b[key.field] ?? null);
        if (c !== 0) return c < 0 !== key.descending ? -1 : 1;
      }
      return 0;
    });
    const limit = req.limit > 0 ? req.limit : 100;
    let offset = 0;
    if (req.page_token) {
      offset = Number(Buffer.from(req.page_token, "base64url").toString("utf8"));
      if (!Number.isInteger(offset) || offset < 0 || offset > found.length) {
        throw refuse(codes.INVALID_ARGUMENT, "", "invalid page_token offset");
      }
    }
    const end = Math.min(offset + limit, found.length);
    const records: Buffer[] = [];
    const revisions: string[] = [];
    const redacted = new Set<string>();
    const canRead = hasScope(identity, "udb:pii:read");
    for (const row of found.slice(Math.min(offset, found.length), end)) {
      const view: Row = {};
      for (const column of selected) {
        if (column in row) view[column] = entity.wireValue(column, copyValue(row[column]));
      }
      if (!canRead) {
        for (const [column, field] of entity.columns) {
          if (field.masked && column in view) {
            view[column] = "***MASKED***";
            redacted.add(column);
          }
        }
      }
      records.push(Buffer.from(recordToJson(view), "utf8"));
      if (req.include_revision) revisions.push(this.revision(identity, entity, entity.keyOf(row)));
    }
    const hasMore = records.length >= limit;
    return {
      records_json: records,
      rows: records.map(() => ({})),
      record_revisions: revisions,
      redacted_fields: [...redacted].sort(),
      has_more: hasMore,
      next_page_token: hasMore && end < found.length ? Buffer.from(String(end)).toString("base64url") : "",
      exact_total: req.include_total ? found.length : 0,
    };
  }

  private write(identity: Identity, req: any, insertOnly: boolean): Row {
    const tenant = identity.tenantId;
    if (insertOnly && ((req.conflict_fields ?? []).length > 0 || hasFields(req.expected))) {
      throw refuse(codes.INVALID_ARGUMENT, "", "Insert does not accept conflict_fields or expected");
    }
    const entity = this.entity(req.message_type);
    const record = entity.normalize(req.payload ? struct(req.payload) : decodeObject(req.record_json));
    entity.bindRecordScope(record, identity);
    entity.conflictFields(req.conflict_fields ?? []);
    entity.defaults(record);
    const key = entity.keyOf(record);
    const byTenant = this.rowsByType.get(entity.messageType);
    if (insertOnly) {
      for (const rows of byTenant?.values() ?? []) {
        if (rows.has(key)) throw uniqueRefusal(`${entity.tableName}_pkey`);
      }
    } else {
      for (const [owner, rows] of byTenant ?? []) {
        const existing = rows.get(key);
        if (existing && (owner !== tenant || !entity.sameRecordScope(existing, record))) {
          if (hasFields(req.expected)) {
            throw refuse(codes.FAILED_PRECONDITION, "UDB_CAS_ROW_MISSING", "compare-and-swap target does not exist in the verified scope");
          }
          // The native ON CONFLICT scope guard leaves a foreign row untouched.
          return { mutation_id: key, affected_rows: 0 };
        }
      }
    }
    const table = this.table(entity.messageType, tenant);
    const expected = entity.expectedColumns(struct(req.expected));
    if (Object.keys(expected).length > 0) {
      const current = table.get(key);
      if (!current) throw refuse(codes.FAILED_PRECONDITION, "UDB_CAS_ROW_MISSING", "compare-and-swap precondition failed: the target row does not exist");
      if (!holds(current, expected)) throw refuse(codes.FAILED_PRECONDITION, "UDB_CAS_CONFLICT", "compare-and-swap precondition failed: a field did not match the current row");
    }
    if (entity.softDelete && !insertOnly && !(entity.softDelete in record)) record[entity.softDelete] = null;
    this.validateRow(entity, tenant, key, record);
    const current = table.get(key);
    if (current && !insertOnly) {
      // Isolation columns are excluded from the native conflict UPDATE SET.
      for (const column of [entity.tenantColumn, entity.projectColumn]) {
        if (column && column in current) record[column] = copyValue(current[column]);
      }
    }
    table.set(key, record);
    return { mutation_id: key, affected_rows: 1, revision: this.bumpRevision(identity, entity, key) };
  }

  private validateRow(entity: EntityRules, tenant: string, key: string, row: Row): void {
    for (const [name, field] of entity.columns) {
      if ((field.column?.not_null || field.column?.primary_key) && (row[name] === null || row[name] === undefined)) {
        throw columnRefusal(name);
      }
    }
    for (const [otherTenant, rows] of this.rowsByType.get(entity.messageType) ?? []) {
      for (const [otherKey, stored] of rows) {
        if (otherTenant === tenant && otherKey === key) continue;
        if (otherKey === key) throw uniqueRefusal(`${entity.tableName}_pkey`);
        for (const unique of entity.unique) {
          if (!entity.inUnique(unique, row) || !entity.inUnique(unique, stored)) continue;
          // SQL UNIQUE treats NULLs as distinct.
          const collision = unique.columns.every(
            (name) => row[name] !== null && row[name] !== undefined && stored[name] !== null && stored[name] !== undefined && equal(row[name], stored[name]),
          );
          if (collision) throw uniqueRefusal(unique.name);
        }
      }
    }
  }

  private matchedKeys(entity: EntityRules, identity: Identity, table: Map<string, Row>, filter: Row): string[] {
    const keys: string[] = [];
    for (const [key, row] of table) {
      if (entity.live(row) && entity.visibleTo(row, identity) && matches(row, filter)) keys.push(key);
    }
    return keys;
  }

  private update(identity: Identity, req: any): Row {
    const tenant = identity.tenantId;
    const entity = this.entity(req.message_type);
    const filter = entity.queryFilter(struct(req.filter));
    if (Object.keys(filter).length === 0) throw refuse(codes.INVALID_ARGUMENT, "", "an update needs a filter");
    entity.checkTenant(filter, tenant);
    const rawChanges = struct(req.changes);
    const increments: { column: string; delta: number }[] = req.increments ?? [];
    entity.updateColumns(rawChanges, increments);
    const expected = entity.expectedColumns(struct(req.expected));
    if (Object.keys(expected).length > 0) entity.validateCasKey(filter);
    const changes = entity.normalize(rawChanges);
    const table = this.table(entity.messageType, tenant);
    const keys = this.matchedKeys(entity, identity, table, filter);
    if (Object.keys(expected).length > 0) {
      if (keys.length === 0) throw refuse(codes.FAILED_PRECONDITION, "UDB_CAS_ROW_MISSING", "CAS row is missing");
      if (keys.length !== 1 || !holds(table.get(keys[0])!, expected)) {
        throw refuse(codes.FAILED_PRECONDITION, "UDB_CAS_CONFLICT", "compare-and-swap precondition failed");
      }
    }
    this.checkRevision(identity, entity, filter, req.expected_revision);
    if (req.require_affected > 0 && keys.length !== req.require_affected) {
      throw refuse(codes.NOT_FOUND, "UDB_NO_ROWS_AFFECTED", "write matched a different number of rows than require_affected");
    }
    // Build the entire statement result before publishing it. A late unique or NOT
    // NULL violation rolls back every earlier matched row, like PostgreSQL.
    const candidate = new Map(table);
    for (const key of keys) candidate.delete(key);
    const changed: string[] = [];
    for (const key of keys) {
      const row = copyValue(table.get(key)!);
      for (const [name, value] of Object.entries(changes)) row[name] = copyValue(value);
      for (const increment of increments) {
        const name = entity.aliases.get(increment.column.trim().toLowerCase());
        if (!name) throw refuse(codes.INVALID_ARGUMENT, "", "unknown increment column");
        row[name] = entity.incrementValue(name, row[name] ?? null, increment.delta);
      }
      const newKey = entity.keyOf(row);
      if (candidate.has(newKey)) throw uniqueRefusal(`${entity.tableName}_pkey`);
      candidate.set(newKey, row);
      changed.push(newKey);
    }
    const byTenant = this.rowsByType.get(entity.messageType)!;
    byTenant.set(tenant, candidate);
    try {
      for (const key of changed) this.validateRow(entity, tenant, key, candidate.get(key)!);
    } catch (err) {
      byTenant.set(tenant, table);
      throw err;
    }
    const response: Row = { affected_rows: keys.length };
    if (keys.length > 0) {
      try {
        response.revision = this.bumpRevision(identity, entity, this.revisionFilterKey(identity, entity, filter));
      } catch {
        // A filter that does not pin one row has no single revision to report.
      }
    }
    return response;
  }

  private remove(identity: Identity, req: any): Row {
    const tenant = identity.tenantId;
    const entity = this.entity(req.message_type);
    const filter = entity.queryFilter(struct(req.filter));
    if (Object.keys(filter).length === 0) throw refuse(codes.INVALID_ARGUMENT, "", "a delete needs a filter");
    entity.checkTenant(filter, tenant);
    const expected = entity.expectedColumns(struct(req.expected));
    if (Object.keys(expected).length > 0) entity.validateCasKey(filter);
    const table = this.table(entity.messageType, tenant);
    const keys = this.matchedKeys(entity, identity, table, filter);
    if (Object.keys(expected).length > 0) {
      if (keys.length === 0) throw refuse(codes.FAILED_PRECONDITION, "UDB_CAS_ROW_MISSING", "CAS row is missing");
      if (keys.length !== 1 || !holds(table.get(keys[0])!, expected)) {
        throw refuse(codes.FAILED_PRECONDITION, "UDB_CAS_CONFLICT", "compare-and-swap precondition failed");
      }
    }
    this.checkRevision(identity, entity, filter, req.expected_revision);
    if (req.require_affected > 0 && keys.length !== req.require_affected) {
      throw refuse(codes.NOT_FOUND, "UDB_NO_ROWS_AFFECTED", `the write matched ${keys.length} row(s) but require_affected is ${req.require_affected}; nothing was changed`);
    }
    for (const key of keys) {
      if (!entity.softDelete) table.delete(key);
      else table.set(key, { ...copyValue(table.get(key)!), [entity.softDelete]: new Date().toISOString() });
    }
    return { affected_rows: keys.length };
  }

  // ── Revisions and idempotency ─────────────────────────────────────────────

  private revisionKey(identity: Identity, entity: EntityRules, key: string): string {
    return [identity.tenantId, identity.projectId ?? "", entity.messageType, key].join("\0");
  }

  /** Entries outlive a delete: re-inserting a key must never revive a revision. */
  private bumpRevision(identity: Identity, entity: EntityRules, key: string): string {
    const scope = this.revisionKey(identity, entity, key);
    const next = (this.revisions.get(scope) ?? 0) + 1;
    this.revisions.set(scope, next);
    return String(next);
  }

  private revision(identity: Identity, entity: EntityRules, key: string): string {
    const current = this.revisions.get(this.revisionKey(identity, entity, key));
    return current ? String(current) : "";
  }

  /** Opaque revisions address only primary keys. */
  private revisionFilterKey(identity: Identity, entity: EntityRules, filter: Row): string {
    const scoped = entity.scopedWriteFilter(filter, identity);
    const key: Row = {};
    for (const column of entity.primaryKey) {
      let value = scoped[column];
      let present = column in scoped;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        present = "$eq" in value && Object.keys(value).length === 1;
        value = value.$eq;
      }
      if (!present || value === null || value === undefined) {
        throw refuse(codes.FAILED_PRECONDITION, "UDB_CAS_KEY_NOT_PK", "conditional mutation requires an equality filter on every primary-key column", {
          reason: "UDB_CAS_KEY_NOT_PK",
          kind: "ERROR_KIND_VALIDATION",
          field_violations: [{ field: column, description: "primary-key column must be pinned by equality" }],
        });
      }
      key[column] = value;
    }
    return entity.keyOf(entity.normalize(key));
  }

  private checkRevision(identity: Identity, entity: EntityRules, filter: Row, expected: string | undefined): void {
    const trimmed = (expected ?? "").trim();
    if (!trimmed) return;
    const key = this.revisionFilterKey(identity, entity, filter);
    if (!/^-?\d+$/.test(trimmed)) {
      throw refuse(codes.INVALID_ARGUMENT, "", "expected_revision is not a valid revision token", {
        kind: "ERROR_KIND_VALIDATION",
        field_violations: [{ field: "expected_revision", description: "must be an opaque revision token previously returned by the broker" }],
      });
    }
    if (this.revisions.get(this.revisionKey(identity, entity, key)) !== Number(trimmed)) {
      throw refuse(codes.FAILED_PRECONDITION, "UDB_REVISION_CONFLICT", "revision precondition failed: the row's current revision differs from expected_revision, or the row is not revision-tracked", {
        reason: "UDB_REVISION_CONFLICT",
        kind: "ERROR_KIND_CONFLICT",
        field_violations: [{ field: "expected_revision", description: "the current revision differs from the expected revision" }],
      });
    }
  }

  /** Only successful writes keep a receipt; it becomes visible with the row change.
   *  Replay precedes CAS and row matching, so a lost-response retry does not fail
   *  because the first write changed the row. */
  private keyedWrite(ctx: Ctx, operation: string, req: any, write: () => Row): Row {
    const key: string = req.idempotency_key ?? "";
    if (!key) return write();
    if (/[\s\p{Cc}]/u.test(key)) {
      throw refuse(codes.INVALID_ARGUMENT, "", "idempotency_key must contain no whitespace or control characters", {
        kind: "ERROR_KIND_VALIDATION",
        field_violations: [{ field: "idempotency_key", description: "must contain no whitespace or control characters" }],
      });
    }
    const entity = this.entity(req.message_type);
    const identity = ctx.identity;
    const hash = createHash("sha256").update(recordToJson(this.mutationInputs(entity, identity, operation, req))).digest("hex");
    const scope = [identity.tenantId, identity.projectId ?? "", req.message_type, operation, key].join("\0");
    const receipt = this.receipts.get(scope);
    if (receipt) {
      if (receipt.hash !== hash) {
        throw refuse(codes.FAILED_PRECONDITION, "UDB_IDEMPOTENCY_REUSE", "idempotency_key was already used for a different request", {
          reason: "UDB_IDEMPOTENCY_REUSE",
          kind: "ERROR_KIND_CONFLICT",
          fix_hint: "Reuse a key only to retry identical inputs; use a new key for a different write.",
          field_violations: [{ field: "idempotency_key", description: "already used with different inputs" }],
        });
      }
      return { ...receipt.response, was_duplicate: true };
    }
    const response = write();
    this.receipts.set(scope, { hash, response: { ...response } });
    return response;
  }

  private mutationInputs(entity: EntityRules, identity: Identity, operation: string, req: any): Row {
    const inputs: Row = { expected: hasFields(req.expected) ? struct(req.expected) : null };
    if (operation === "upsert" || operation === "insert") {
      const record = entity.normalize(req.payload ? struct(req.payload) : decodeObject(req.record_json));
      entity.bindRecordScope(record, identity);
      inputs.record = record;
      inputs.conflict_fields = req.conflict_fields ?? [];
      return inputs;
    }
    const revision = (req.expected_revision ?? "").trim();
    if (revision) inputs.expected_revision = revision;
    if (req.require_affected) inputs.require_affected = req.require_affected;
    if (operation === "update") {
      inputs.changes = entity.normalize(struct(req.changes));
      inputs.increments = (req.increments ?? []).map((increment: any) => [increment.column, increment.delta]);
    }
    inputs.filter = entity.scopedWriteFilter(entity.queryFilter(struct(req.filter)), identity);
    return inputs;
  }

  // ── Transactions ──────────────────────────────────────────────────────────

  /** Apply a transaction all-or-nothing: any refusal rolls every earlier mutation
   *  back. Its outbox events are recorded only when it commits. */
  private beginTx(call: any): void {
    let ctx: Ctx;
    const fail = (err: unknown) => {
      if (!done) {
        done = true;
        call.emit("error", statusOf(err));
      }
    };
    let done = false;
    try {
      ctx = this.beforeRpc("BeginTx", call.metadata, null);
    } catch (err) {
      fail(err);
      return;
    }
    const mutations: any[] = [];
    let terminal: any;
    call.on("data", (m: any) => {
      if (done) return;
      try {
        if (terminal) throw refuse(codes.INVALID_ARGUMENT, "", "mutation after transaction terminal marker");
        bindBody(ctx.identity, m.context);
        if (m.commit && m.rollback) throw refuse(codes.INVALID_ARGUMENT, "", "commit and rollback cannot both be set");
        if (m.operation) {
          const operation = String(m.operation).toLowerCase();
          m.operation = operation;
          let action = "";
          let resource = m.message_type;
          if (operation === "upsert" || operation === "insert") action = "Upsert";
          else if (operation === "update") action = "Update";
          else if (operation === "delete") action = "Delete";
          else if (operation === "enqueue_outbox_event") {
            action = "EnqueueOutboxEvent";
            resource = m.collection || m.message_type;
          } else throw refuse(codes.INVALID_ARGUMENT, "", `unsupported transaction operation ${operation}`);
          if (String(m.idempotency_key ?? "").trim()) {
            throw refuse(codes.INVALID_ARGUMENT, "", "this transaction path cannot honour idempotency_key; nothing was written", {
              kind: "ERROR_KIND_VALIDATION",
              field_violations: [{ field: `mutations[${mutations.length}].idempotency_key`, description: "not supported on this operation inside BeginTx; leave it empty" }],
            });
          }
          this.authorize(ctx, resource, action);
          mutations.push(m);
        }
        if (m.commit || m.rollback) terminal = m;
      } catch (err) {
        fail(err);
      }
    });
    call.on("error", () => {
      done = true;
    });
    call.on("end", () => {
      if (done) return;
      try {
        if (!terminal) throw refuse(codes.FAILED_PRECONDITION, "", "transaction closed without commit or rollback");
        if (terminal.rollback) {
          call.write({ tx_id: terminal.tx_id, state: "TX_STATE_ROLLED_BACK" });
        } else {
          const snapshot = this.snapshot();
          const revisions = new Map(this.revisions);
          const events = this.eventLog.length;
          try {
            this.applyTx(ctx.identity, mutations);
          } catch (err) {
            this.rowsByType = snapshot;
            this.revisions = revisions;
            this.eventLog.length = events;
            throw err;
          }
          call.write({ tx_id: terminal.tx_id, state: "TX_STATE_COMMITTED", message: `${mutations.length} mutation(s) committed` });
        }
        done = true;
        call.end();
      } catch (err) {
        fail(err);
      }
    });
  }

  private applyTx(identity: Identity, mutations: any[]): void {
    for (const m of mutations) {
      switch (m.operation) {
        case "upsert":
        case "insert":
          this.write(identity, { message_type: m.message_type, record_json: m.record_json, payload: m.payload, expected: m.expected }, m.operation === "insert");
          break;
        case "update":
          this.update(identity, { message_type: m.message_type, filter: m.filter, changes: m.changes, expected: m.expected, increments: m.increments });
          break;
        case "delete":
          this.remove(identity, { message_type: m.message_type, filter: m.filter, expected: m.expected });
          break;
        case "enqueue_outbox_event": {
          const envelope = struct(m.payload);
          if (envelope.document_id !== m.object_key || !m.object_key) {
            throw refuse(codes.INVALID_ARGUMENT, "", "outbox partition_key must equal payload.document_id");
          }
          if (typeof envelope.tenant_id === "string" && envelope.tenant_id && envelope.tenant_id !== identity.tenantId) {
            throw refuse(codes.PERMISSION_DENIED, "UDB_TENANT_MISMATCH", "outbox tenant differs from verified tenant");
          }
          if (typeof envelope.project_id === "string" && envelope.project_id && envelope.project_id !== (identity.projectId ?? "")) {
            throw refuse(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "outbox project differs from verified project");
          }
          if (envelope.event_id == null || envelope.event_type == null || envelope.envelope_version !== 2) {
            throw refuse(codes.INVALID_ARGUMENT, "", "outbox needs a versioned event envelope");
          }
          envelope.tenant_id = identity.tenantId;
          envelope.project_id = identity.projectId ?? "";
          this.eventLog.push({ topic: m.collection || m.message_type, partitionKey: m.object_key, envelope: copyValue(envelope) });
          break;
        }
        default:
          throw refuse(codes.INVALID_ARGUMENT, "", `unsupported transaction operation ${m.operation}`);
      }
    }
  }

  private snapshot(): Map<string, Map<string, Map<string, Row>>> {
    const out = new Map<string, Map<string, Map<string, Row>>>();
    for (const [messageType, byTenant] of this.rowsByType) {
      const copied = new Map<string, Map<string, Row>>();
      for (const [tenant, table] of byTenant) {
        copied.set(tenant, new Map([...table].map(([key, row]) => [key, copyValue(row)])));
      }
      out.set(messageType, copied);
    }
    return out;
  }

  // ── Durable consumers ─────────────────────────────────────────────────────

  /** Keeps a named consumer's verified owner, uses its stored position, and
   *  revalidates authorization and expiry throughout the stream. */
  private publishCdc(call: any): void {
    const fail = (err: unknown) => call.emit("error", statusOf(err));
    let ctx: Ctx;
    let pattern: string;
    let next = 0;
    try {
      ctx = this.beforeRpc("PublishCDC", call.metadata, null);
      const req = call.request;
      bindBody(ctx.identity, req.context);
      pattern = topicPattern(req.topic_pattern);
      this.authorize(ctx, pattern, "PublishCDC");
      if (String(req.consumer_name ?? "").trim()) {
        const name = consumerName(req.consumer_name);
        const owner = consumerOwner(ctx.identity);
        const key = cursorKey(ctx.identity, name, pattern);
        let cursor = this.cursors.get(key);
        if (cursor && cursor.owner !== owner) throw cursorOwnerError();
        if (!cursor) {
          cursor = { owner, index: -1, id: "" };
          this.cursors.set(key, cursor);
        }
        if (!req.since_event_id) next = cursor.index + 1;
      }
      if (req.since_event_id) {
        const index = this.eventLog.findIndex((event) => event.envelope.event_id === req.since_event_id && eventInScope(event, ctx.identity, pattern));
        if (index < 0) throw refuse(codes.NOT_FOUND, "", "CDC resume event is unknown or outside the verified scope");
        next = index + 1;
      }
    } catch (err) {
      fail(err);
      return;
    }
    let stopped = false;
    const pump = () => {
      if (stopped) return;
      try {
        if (Date.now() >= ctx.token.expires) throw refuse(codes.UNAUTHENTICATED, "", "bearer expired during CDC stream");
        this.authorize(ctx, pattern, "PublishCDC");
        const pending = this.eventLog.slice(next);
        next = this.eventLog.length;
        for (const event of pending) {
          if (!eventInScope(event, ctx.identity, pattern)) continue;
          call.write({
            event_id: String(event.envelope.event_id ?? ""),
            topic: event.topic,
            partition_key: event.partitionKey,
            payload_json: recordToJson(event.envelope),
          });
        }
      } catch (err) {
        stop();
        fail(err);
      }
    };
    const timer = setInterval(pump, 20);
    const stop = () => {
      stopped = true;
      clearInterval(timer);
    };
    call.on("cancelled", stop);
    call.on("close", stop);
    call.on("error", stop);
    pump();
  }

  private nack(ctx: Ctx, req: any): Row {
    if (req.attempts < 1 || req.attempts > 1000 || Buffer.byteLength(req.failure_reason ?? "") > 2048) {
      throw refuse(codes.INVALID_ARGUMENT, "", "invalid handler attempts or diagnostic length");
    }
    return this.settle(ctx, req, req);
  }

  private settle(ctx: Ctx, req: any, failure?: any): Row {
    const name = consumerName(req.consumer_name ?? "");
    if (!/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(req.event_id ?? "")) {
      throw refuse(codes.INVALID_ARGUMENT, "", "event_id must be a UUID");
    }
    const pattern = topicPattern(req.topic_pattern);
    this.authorize(ctx, pattern, "PublishCDC");
    const identity = ctx.identity;
    const owner = consumerOwner(identity);
    const index = this.eventLog.findIndex((event) => event.envelope.event_id === req.event_id && eventInScope(event, identity, pattern));
    if (index < 0) throw refuse(codes.PERMISSION_DENIED, "UDB_POLICY_DENIED", "acknowledged CDC event is not retained in the verified scope");
    const key = cursorKey(identity, name, pattern);
    const cursor = this.cursors.get(key) ?? { owner, index: -1, id: "" };
    if (cursor.owner !== owner) throw cursorOwnerError();
    if (failure) {
      const event = this.eventLog[index];
      let entry = this.dlq.get(req.event_id);
      const now = Math.floor(Date.now() / 1000);
      if (!entry) {
        entry = {
          identity,
          event,
          failures: {},
          failureMetadata: {
            service: "udb-cdc-consumer",
            error_type: "CONSUMER_HANDLER_FAILED",
            error_message: failure.failure_reason,
            failed_at: new Date().toISOString(),
            retry_count: 0,
          },
          record: {
            dlq_id: randomUUID(),
            event_id: req.event_id,
            topic: event.topic,
            error_type: "CONSUMER_HANDLER_FAILED",
            error_message: failure.failure_reason,
            created_at_unix: now,
          },
        };
      }
      const failureKey = createHash("sha256").update([owner, name, pattern].map((part) => `${part.length}:${part}`).join("")).digest("hex");
      if (!(failureKey in entry.failures)) {
        entry.failures = {
          ...entry.failures,
          [failureKey]: { owner_identity: owner, consumer_name: name, topic_pattern: pattern, attempts: failure.attempts, failure_reason: failure.failure_reason, failed_at: new Date().toISOString() },
        };
        entry.record.payload_json = Buffer.from(recordToJson({ failed_event: event.envelope, failure_metadata: entry.failureMetadata, consumer_failures: entry.failures }));
        entry.record.status = "QUARANTINED";
        entry.record.updated_at_unix = now;
        this.dlq.set(req.event_id, entry);
      }
    }
    if (index > cursor.index) {
      cursor.index = index;
      cursor.id = req.event_id;
    }
    this.cursors.set(key, cursor);
    return { consumer_name: name, topic_pattern: pattern, event_id: req.event_id, acked_at_unix: Math.floor(Date.now() / 1000) };
  }

  private dlqEntry(ctx: Ctx, id: string): DlqEntry {
    for (const entry of this.dlq.values()) {
      if (entry.record.dlq_id === id && sameDomain(entry.identity, ctx.identity)) return entry;
    }
    throw refuse(codes.NOT_FOUND, "UDB_NO_ROWS_AFFECTED", "DLQ event not found in verified tenant/project");
  }

  private listDlq(ctx: Ctx, req: any): Row {
    const events = [...this.dlq.values()]
      .filter(
        (entry) =>
          sameDomain(entry.identity, ctx.identity) &&
          (!req.topic || req.topic === entry.record.topic) &&
          (!req.status_filter || req.status_filter === entry.record.status),
      )
      .map((entry) => ({ ...entry.record }))
      .sort((a, b) => (a.dlq_id < b.dlq_id ? -1 : 1));
    let offset = 0;
    if (req.page_token) {
      offset = Number(req.page_token);
      if (!Number.isInteger(offset) || offset < 0 || offset > events.length) throw refuse(codes.INVALID_ARGUMENT, "", "invalid DLQ page token");
    }
    const limit = req.limit > 0 ? req.limit : 100;
    const end = Math.min(events.length, offset + limit);
    return { events: events.slice(offset, end), total_count: events.length, next_page_token: end < events.length ? String(end) : "" };
  }

  private replayDlq(ctx: Ctx, req: any): Row {
    const entry = this.dlqEntry(ctx, req.dlq_id);
    if (!["OPEN", "RETRYING", "QUARANTINED"].includes(entry.record.status)) {
      throw refuse(codes.NOT_FOUND, "UDB_NO_ROWS_AFFECTED", "DLQ event is not replayable");
    }
    const event: FakeEvent = { topic: entry.event.topic, partitionKey: entry.event.partitionKey, envelope: copyValue(entry.event.envelope) };
    entry.record.status = "REPLAYED";
    entry.record.updated_at_unix = Math.floor(Date.now() / 1000);
    // The canonical outbox's event id is unique: preserving it adds no duplicate
    // retained event to the journal.
    if (!req.preserve_event_id) {
      event.envelope.event_id = randomUUID();
      this.eventLog.push(event);
    }
    return { mutation_id: String(event.envelope.event_id), affected_rows: 1 };
  }

  private setDlqStatus(ctx: Ctx, req: any, status: string): Row {
    const entry = this.dlqEntry(ctx, req.dlq_id);
    entry.record.status = status;
    entry.record.updated_at_unix = Math.floor(Date.now() / 1000);
    return { mutation_id: req.dlq_id, affected_rows: 1 };
  }
}

/** Start a fake serving the given entity messages, with an allow-all test policy. It
 *  stops when `t` ends. Their annotations define keys and the tenant column. */
export async function createFake(t: TestHooks, messageTypes: readonly string[], opts: { protoRoot?: string } = {}): Promise<Fake> {
  const root = path.resolve(opts.protoRoot ?? defaultProtoRoot());
  const fake = new Fake(protoSchemas(opts.protoRoot), root);
  fake.configure([], true);
  for (const messageType of messageTypes) fake.register(messageType);
  await fake.serve();
  t.after(() => fake.close());
  return fake;
}

/** Start a fake serving every table declared by a compiled descriptor set (the bytes
 *  of `protoc --include_imports --descriptor_set_out`, which must carry every import)
 *  or, without one, by a proto tree, under the given policies. With no policies it
 *  denies data requests by default, as a broker does. `policyFile` is the JSON or YAML
 *  file `udb policy apply` reads. Call `close()` when finished. */
export async function createDescriptorFake(
  opts: { descriptorSet?: Uint8Array; protoRoot?: string; policyFile?: string; policies?: Policy[] } = {},
): Promise<Fake> {
  if (opts.descriptorSet && opts.protoRoot) throw new Error("udbtest: pass descriptorSet or protoRoot, not both");
  const schemas = opts.descriptorSet ? ProtoSchemas.fromDescriptorSet(opts.descriptorSet) : protoSchemas(opts.protoRoot);
  const root = path.resolve((opts.descriptorSet ? undefined : opts.protoRoot) ?? defaultProtoRoot());
  const fake = new Fake(schemas, root);
  fake.configure(opts.policies ?? (opts.policyFile ? loadPolicies(opts.policyFile) : []), false);
  const tables = schemas.tables();
  if (tables.length === 0) throw new Error("udbtest: descriptor set declares no tables");
  for (const table of tables) fake.register(table.fullName);
  await fake.serve();
  return fake;
}
