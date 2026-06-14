// Broker-backed SDK conformance for urgent_fix #20.
//
// This test is intentionally skipped unless UDB_LIVE_SDK_TESTS=1. CI starts a
// real broker, seeds the first user through `udb auth bootstrap user`, then runs
// this through the normal TypeScript test build.

import { strict as assert } from "node:assert";
import { test } from "node:test";
import { writeFileSync } from "node:fs";
import * as grpc from "@grpc/grpc-js";

import { StoredToken, UdbProject } from "./project";
import { RPC_OPERATION_KIND } from "./generatedClient";

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
  return value;
}

function memoryStore(initial: StoredToken | null = null) {
  let token = initial;
  return {
    load: async () => token,
    save: async (next: StoredToken) => {
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
] as const;

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

const UNSUPPORTED_OPERATION_CODE = "UDB_UNSUPPORTED_OPERATION";
// Canonical generic-dispatch op vocabulary the broker gates per backend
// (src/runtime/service/mod.rs check_generic_dispatch_operation), safe-first.
const GENERIC_DISPATCH_OPS = [
  "ping", "probe", "list_resources", "search", "query",
  "transaction", "get_object", "put_object", "mutate",
  "ensure_resource", "drop_resource",
];

function errText(err: unknown): string {
  const a = err as any;
  return String(a?.details ?? a?.message ?? a ?? "");
}

function requestContext(tenantId: string, projectId: string, purpose: string) {
  return {
    tenant_id: tenantId,
    project_id: projectId,
    purpose,
    correlation_id: `${purpose}-${Date.now()}`,
    // No client-asserted scopes: admin authority comes from the Login JWT (broker
    // derives scopes from the validated bearer; header/body scopes are ignored
    // when a JWT verifier is configured). The real production path.
    scopes: [] as string[],
    service_identity: "ts.sdk.live",
    client_catalog_version: "1.0.0",
  };
}

// google.protobuf.Struct filters/documents are now passed as PLAIN JS objects —
// the SDK's wkt.ts wrapper converts them to the correct camelCase Value wire form
// (previously the test hand-built the {fields:{k:{stringValue}}} shape because a
// plain object silently serialized to empty values).

function valueOf(field: any): unknown {
  if (!field) return undefined;
  // Deserialized google.protobuf.Value uses camelCase oneof members; accept the
  // snake_case spellings too for safety.
  if ("stringValue" in field) return field.stringValue;
  if ("string_value" in field) return field.string_value;
  if ("numberValue" in field) return Number(field.numberValue);
  if ("number_value" in field) return Number(field.number_value);
  if ("boolValue" in field) return field.boolValue;
  if ("bool_value" in field) return field.bool_value;
  return undefined;
}

function structField(doc: any, name: string): unknown {
  return valueOf(doc?.fields?.[name]);
}

function jsonBytes(value: Record<string, unknown>): Buffer {
  return Buffer.from(JSON.stringify(value), "utf8");
}

function recordJson(recordSet: any, index = 0): any {
  const raw = recordSet?.records_json?.[index];
  if (!raw) throw new Error(`RecordSet.records_json[${index}] is missing`);
  return JSON.parse(Buffer.isBuffer(raw) ? raw.toString("utf8") : Buffer.from(raw).toString("utf8"));
}

function mutationRecordJson(response: any): any {
  const raw = response?.record_json;
  if (!raw) throw new Error("MutationResponse.record_json is missing");
  return JSON.parse(Buffer.isBuffer(raw) ? raw.toString("utf8") : Buffer.from(raw).toString("utf8"));
}

function grpcCode(err: unknown): number | undefined {
  const anyErr = err as any;
  return anyErr?.code ?? anyErr?.cause?.code ?? anyErr?.udb?.code;
}

function describeGrpcError(err: unknown): string {
  const anyErr = err as any;
  const code = grpcCode(err);
  const codeName = code === undefined ? "unknown" : (grpc.status as any)[code] ?? String(code);
  return `${codeName}: ${anyErr?.details ?? anyErr?.message ?? String(err)}`;
}

function reachedUdbHandler(err: unknown): boolean {
  const anyErr = err as any;
  const text = String(
    anyErr?.details ??
    anyErr?.message ??
    anyErr?.udb?.details ??
    anyErr?.udb?.message ??
    "",
  );
  return /\budb\s+[\w.]+\/[A-Za-z0-9_]+:/.test(text) || /\(code=[A-Z_]+\)/.test(text);
}

function isFatalMountError(err: unknown): boolean {
  const code = grpcCode(err);
  return code !== undefined && FATAL_CONNECTIVITY_CODES.has(code) && !reachedUdbHandler(err);
}

async function expectMounted(label: string, op: () => Promise<unknown>): Promise<void> {
  try {
    await op();
  } catch (err) {
    if (isFatalMountError(err)) {
      throw new Error(`${label} did not reach an implemented live RPC: ${describeGrpcError(err)}`);
    }
  }
}

async function expectStreamMounted(
  label: string,
  open: () => grpc.ClientReadableStream<any> | grpc.ClientWritableStream<any> | grpc.ClientDuplexStream<any, any>,
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const stream = open();
    let settled = false;
    const finish = (err?: unknown) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (typeof (stream as any).cancel === "function") (stream as any).cancel();
      if (err) reject(err);
      else resolve();
    };
    const timer = setTimeout(() => finish(), 750);
    stream.once("error", (err: grpc.ServiceError) => {
      if (isFatalMountError(err)) {
        finish(new Error(`${label} did not reach an implemented live stream RPC: ${describeGrpcError(err)}`));
      } else {
        finish();
      }
    });
    stream.once("data", () => finish());
    stream.once("end", () => finish());
    if (typeof (stream as any).end === "function") (stream as any).end();
  });
}

// This client exposes RPCs as snake_case methods; RPC_OPERATION_KIND is keyed by
// the canonical gRPC path "/<service>/<MethodName>". snake→Pascal is exact across
// the whole surface (verified), and callers assert the lookup resolves so a
// classification/coverage gap fails loudly rather than silently populating an RPC.
function snakeToPascal(s: string): string {
  return s.split("_").map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join("");
}
function operationKindOf(serviceFull: string, methodSnake: string): string | undefined {
  return RPC_OPERATION_KIND[`/${serviceFull}/${snakeToPascal(methodSnake)}`];
}
// A "kitchen-sink" request: protobufjs (under proto-loader) drops keys that the
// concrete request type doesn't declare, so every read RPC picks up exactly the
// fields it has (tenant/project/context/message_type/…). This deepens the probe
// from an empty ping to a field-populated typed request across the full surface.
function surfaceProbeRequest(tenantId: string, projectId: string) {
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

async function expectGeneratedUnarySurfaceMounted(
  t: any,
  label: string,
  generated: any,
  serviceNames: readonly string[],
  tenantId: string,
  projectId: string,
  counters: { populated: number },
): Promise<number> {
  let count = 0;
  for (const serviceName of serviceNames) {
    const api = generated[serviceName];
    assert.ok(api, `${label}.${serviceName} must exist on generated SDK client`);
    for (const [methodName, fn] of Object.entries(api)) {
      if (methodName === "serviceFull" || NON_UNARY_METHODS.has(methodName)) continue;
      if (typeof fn !== "function") continue;
      count += 1;
      // Proto-derived operation_kind (never name-guessed). Assert it resolves so a
      // missing classification is a loud failure, not a silently-populated RPC.
      const opKind = operationKindOf((api as any).serviceFull, methodName);
      assert.ok(
        opKind,
        `${label}.${serviceName}.${methodName} has no proto operation_kind (classification/coverage gap)`,
      );
      const populated = opKind !== "destructive";
      if (populated) counters.populated += 1;
      const request = populated ? surfaceProbeRequest(tenantId, projectId) : {};
      // One node:test sub-test PER RPC so the reporter shows granular per-RPC
      // pass/fail (like the Go sub-tests), not a single opaque test.
      await t.test(`${(api as any).serviceFull}/${methodName}`, async () => {
        await expectMounted(`${label}.${serviceName}.${methodName}`, () =>
          (fn as any)(request, { deadlineMs: 2_000, noRetry: true }),
        );
      });
    }
  }
  return count;
}

// runLiveBackendClaimCheck: every advertised backend must answer a real
// list_resources (a mount/unavailable failure means a capability lie).
async function runLiveBackendClaimCheck(data: any, ctx: any, enabled: string[]): Promise<void> {
  assert.ok(enabled.length > 0, "GetCapabilities advertised zero backends");
  for (const backend of enabled) {
    await expectMounted(`backend-claim.${backend}`, () =>
      data.list_resources({ context: ctx, backend }, { deadlineMs: 5_000, noRetry: true }),
    );
  }
}

// runLiveAuthLifecycle: prove Logout invalidates the session — the access token,
// refresh token and session-refresh must ALL fail afterwards. Throwaway login.
async function runLiveAuthLifecycle(authn: any, tenantId: string, projectId: string, username: string, password: string): Promise<void> {
  const opts = { deadlineMs: 8_000, noRetry: true };
  const login = await authn.login({ username, password, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-sdk-lifecycle" }, opts);
  const token = login.access_token, sid = login.session_id, refresh = login.refresh_token;
  assert.ok(token && sid && refresh, "Login must return access_token+session_id+refresh_token");
  const pre = await authn.validate_token({ token, token_type: 1 }, opts); // 1 = TOKEN_TYPE_JWT_ACCESS
  assert.ok(pre.valid, "fresh access token must validate before logout");
  await authn.get_session({ session_id: sid }, opts);
  const preIntro = await authn.introspect_token({ token }, opts);
  assert.ok(preIntro.active, "fresh access token must introspect active before logout");
  const out = await authn.logout({ session_id: sid, revoke_reason: "sdk_live_test" }, opts);
  assert.ok(Number(out.sessions_revoked) >= 1, "Logout must revoke at least one session");

  const failures: string[] = [];
  try { if ((await authn.validate_token({ token, token_type: 1 }, opts)).valid) failures.push("access token still validates after logout"); } catch { /* denied = correct */ }
  try { if ((await authn.introspect_token({ token }, opts)).active) failures.push("token still introspects Active after logout"); } catch { /* denied = correct */ }
  try { await authn.refresh_token({ refresh_token: refresh, session_id: sid }, opts); failures.push("refresh token still works after logout — token family not revoked"); } catch { /* denied = correct */ }
  try { await authn.refresh_session({ session_id: sid }, opts); failures.push("RefreshSession still works after logout — session not revoked"); } catch { /* denied = correct */ }
  assert.equal(failures.length, 0, `SECURITY (logout did not fully invalidate the session): ${failures.join("; ")}`);
}

// runLiveAuthNegative: edge cases the happy-path suite skips — the auth plane must
// fail CLOSED. A wrong password mints no access token; a garbage/forged bearer never
// validates or introspects active. A mount failure is still fatal (the negative
// paths must be wired too, not just the positive ones).
async function runLiveAuthNegative(authn: any, tenantId: string, projectId: string, username: string): Promise<void> {
  const opts = { deadlineMs: 8_000, noRetry: true };
  const fatalIfMount = (label: string, err: unknown) => {
    const code = grpcCode(err);
    if (code !== undefined && FATAL_CONNECTIVITY_CODES.has(code)) {
      throw new Error(`${label} did not reach a live RPC: ${describeGrpcError(err)}`);
    }
  };
  const failures: string[] = [];
  try {
    const bad = await authn.login({ username, password: `definitely-wrong-${username}-Pw1!`, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-sdk-negative" }, opts);
    if (bad?.access_token) failures.push("Login with a wrong password returned an access token");
  } catch (err) { fatalIfMount("negative Login", err); }
  try {
    const v = await authn.validate_token({ token: "not-a-real-jwt", token_type: 1 }, opts);
    if (v?.valid) failures.push("a garbage token validated as valid");
  } catch (err) { fatalIfMount("negative ValidateToken", err); }
  try {
    const i = await authn.introspect_token({ token: "not-a-real-jwt" }, opts);
    if (i?.active) failures.push("a garbage token introspected as active");
  } catch (err) { fatalIfMount("negative IntrospectToken", err); }
  assert.equal(failures.length, 0, `SECURITY (auth did not fail closed): ${failures.join("; ")}`);
}

// runLiveEdgeCases: per-RPC EDGE cases (malformed/hostile inputs + isolation
// boundaries). Each must fail closed with a typed error (or safely accept-and-
// sanitise), never leak cross-tenant data, and never surface a server fault
// (UNKNOWN/INTERNAL/DATA_LOSS = the input crashed the handler). Mirrors the Go suite.
const EDGE_SERVER_FAULTS = new Set([2, 13, 15]); // UNKNOWN, INTERNAL, DATA_LOSS
async function runLiveEdgeCases(data: any, tenantId: string, projectId: string): Promise<void> {
  const ctx = requestContext(tenantId, projectId, "ts.live.edge");
  const opts = { deadlineMs: 8_000, noRetry: true };
  const suffix = `${tenantId}-edge`;
  const notFault = (label: string, err: unknown) => {
    const c = grpcCode(err);
    if (c !== undefined && EDGE_SERVER_FAULTS.has(c)) {
      throw new Error(`${label} faulted the server (code ${c}): ${describeGrpcError(err)}`);
    }
  };

  // 1. missing project_id in the filter -> project isolation must reject.
  let accepted1 = false;
  try { await data.select({ context: ctx, message_type: LIVE_MESSAGE_TYPE, filter: { tenant_id: tenantId }, limit: 1 }, opts); accepted1 = true; }
  catch (err) { notFault("missing project_id", err); }
  assert.equal(accepted1, false, "Select without a project_id filter was ACCEPTED — project isolation not enforced");

  // 2. cross-tenant read -> RLS scopes to the JWT tenant; a foreign filter leaks nothing.
  const foreign = "00000000-0000-0000-0000-0000deadbeef";
  try {
    const resp = await data.select({ context: ctx, message_type: LIVE_MESSAGE_TYPE, filter: { tenant_id: foreign, project_id: projectId }, limit: 10 }, opts);
    const n = (resp?.records_json ?? []).length;
    assert.equal(n, 0, `cross-tenant Select LEAKED ${n} record(s) for ${foreign}`);
  } catch (err) { notFault("cross-tenant Select", err); }

  // 3. NUL byte in a text field -> stripped/rejected, never a raw UTF8 0x00 fault (B14).
  try {
    await data.upsert({
      context: ctx, message_type: LIVE_MESSAGE_TYPE,
      record_json: jsonBytes({ record_id: `edge-nul-${suffix}`, tenant_id: tenantId, project_id: projectId, lookup_key: `edge-nul-lk-${suffix}`, payload: "payload with-nul", revision: 1 }),
      conflict_fields: ["record_id"],
    }, opts);
  } catch (err) { notFault("NUL-byte payload", err); }

  // 4. limit boundaries (negative/zero/huge) -> clamped/validated, never a crash.
  for (const lim of [-1, 0, 1_000_000]) {
    try { await data.select({ context: ctx, message_type: LIVE_MESSAGE_TYPE, filter: { tenant_id: tenantId, project_id: projectId }, limit: lim }, opts); }
    catch (err) { notFault(`Select limit=${lim}`, err); }
  }

  // 5. unknown message_type -> typed error, not a 500.
  let accepted5 = false;
  try { await data.select({ context: ctx, message_type: "udb.does.not.Exist", filter: { tenant_id: tenantId, project_id: projectId }, limit: 1 }, opts); accepted5 = true; }
  catch (err) { notFault("unknown message_type", err); }
  assert.equal(accepted5, false, "Select on an unknown message_type was ACCEPTED");

  // 6. invalid backend -> typed error, never a panic/Internal.
  let accepted6 = false;
  try { await data.list_resources({ context: ctx, backend: "nonexistent-backend-xyz" }, opts); accepted6 = true; }
  catch (err) { notFault("invalid backend", err); }
  assert.equal(accepted6, false, "ListResources on a nonexistent backend was ACCEPTED");
}

async function drainReadable(stream: grpc.ClientReadableStream<any>): Promise<any[]> {
  return await new Promise<any[]>((resolve, reject) => {
    const chunks: any[] = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(chunks));
  });
}

async function drainDuplexOnce(stream: grpc.ClientDuplexStream<any, any>, requests: any[]): Promise<any[]> {
  return await new Promise<any[]>((resolve, reject) => {
    const chunks: any[] = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(chunks));
    for (const request of requests) stream.write(request);
    stream.end();
  });
}

// Challenge EVERY advertised backend's per-operation claims in BOTH directions via
// GenericDispatch (the single op-gated entry point shared by every backend kind). A
// claimed side-effect-free op must be admitted; the first unclaimed op must be
// refused with the declared unsupported code — proving each backend kind honors
// exactly the surface it advertises.
async function runLiveBackendCapabilityChallenge(data: any, ctx: any, caps: any): Promise<void> {
  const descriptors: any[] = caps.backend_capabilities ?? [];
  assert.ok(descriptors.length > 0, "GetCapabilities advertised zero backend_capabilities descriptors");
  const opts = { deadlineMs: 5_000, noRetry: true };
  const dispatch = async (backend: string, op: string): Promise<unknown> => {
    try {
      await data.generic_dispatch({ context: ctx, backend, operation: op, spec_json: "{}" }, opts);
      return null;
    } catch (err) {
      return err;
    }
  };
  for (const d of descriptors) {
    const backend = d.backend;
    assert.ok(backend, "a backend_capabilities descriptor has an empty backend name");
    assert.ok(d.tier, `backend ${backend} advertises no tier`);
    const claimed: string[] = d.operations ?? [];
    assert.ok(claimed.length > 0, `backend ${backend} advertises an empty operations list`);
    assert.equal(d.unsupported_error_code, UNSUPPORTED_OPERATION_CODE, `backend ${backend} unsupported_error_code`);
    const claimedSet = new Set(claimed);
    for (const op of ["ping", "probe", "list_resources"]) {
      if (!claimedSet.has(op)) continue;
      const err = await dispatch(backend, op);
      if (err) {
        const code = grpcCode(err);
        if (code !== undefined && FATAL_CONNECTIVITY_CODES.has(code)) throw new Error(`backend ${backend} claims ${op} but did not reach a live RPC: ${describeGrpcError(err)}`);
        assert.ok(!errText(err).includes(UNSUPPORTED_OPERATION_CODE), `CAPABILITY LIE: backend ${backend} advertises ${op} but the gate refused it: ${errText(err)}`);
      }
    }
    for (const op of GENERIC_DISPATCH_OPS) {
      if (claimedSet.has(op)) continue;
      const err = await dispatch(backend, op);
      assert.ok(err, `CAPABILITY LIE: backend ${backend} does NOT advertise ${op} yet GenericDispatch admitted it (silent over-claim)`);
      const code = grpcCode(err);
      if (code !== undefined && FATAL_CONNECTIVITY_CODES.has(code)) throw new Error(`backend ${backend} unclaimed-op ${op} did not reach a live RPC: ${describeGrpcError(err)}`);
      assert.ok(errText(err).includes(UNSUPPORTED_OPERATION_CODE), `backend ${backend} refused unclaimed op ${op} but not with ${UNSUPPORTED_OPERATION_CODE}: ${errText(err)}`);
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

function backendCategory(tier: string, ops: Set<string>): string {
  if (ops.has("get_object") || ops.has("put_object")) return "object";
  return ({ vector: "vector", cache: "cache", document: "document", graph: "graph", sql: "relational", column: "relational" } as Record<string, string>)[String(tier).toLowerCase()] ?? "";
}

type MountFatal = (backend: string, op: string, err: unknown) => void;

// Drive a real, category-appropriate data-plane round-trip against EVERY advertised
// backend kind (relational SQL, object, document, cache, vector, graph) — not just
// the canonical postgres/mongodb/minio trio. Adapts to whatever the broker enabled.
// A claimed RPC must at minimum REACH an implementation (a mount failure is fatal);
// per-backend business quirks are tolerated, values asserted on success.
async function runLiveAllBackendKindsMatrix(data: any, tenantId: string, projectId: string, caps: any): Promise<void> {
  const suffix = `${process.pid}-${Date.now()}`;
  const rc = (p: string) => requestContext(tenantId, projectId, p);
  const opts = { deadlineMs: 8_000, noRetry: true };
  const mountFatal: MountFatal = (backend, op, err) => {
    const code = grpcCode(err);
    if (code !== undefined && FATAL_CONNECTIVITY_CODES.has(code)) throw new Error(`backend ${backend} (${op}) did not reach a live RPC: ${describeGrpcError(err)}`);
  };
  const exercised: Record<string, number> = {};
  for (const d of caps.backend_capabilities ?? []) {
    const backend = d.backend;
    if (!backend) continue;
    const ops = new Set<string>(d.operations ?? []);
    const cat = backendCategory(d.tier, ops);
    exercised[cat] = (exercised[cat] ?? 0) + 1;
    if (cat === "relational") {
      try {
        await data.generic_dispatch({ context: rc("ts.live.kind.relational"), backend, operation: "query", spec_json: JSON.stringify({ sql: "SELECT 1 AS live_probe" }) }, opts);
      } catch (err) {
        mountFatal(backend, "query", err);
        assert.ok(!errText(err).includes(UNSUPPORTED_OPERATION_CODE), `CAPABILITY LIE: relational backend ${backend} refused a claimed query: ${errText(err)}`);
      }
    } else if (cat === "object") {
      await objectKind(data, rc, backend, suffix, mountFatal);
    } else if (cat === "document") {
      await documentKind(data, rc, backend, suffix, mountFatal);
    } else if (cat === "cache") {
      await cacheKind(data, rc, backend, suffix, mountFatal);
    } else if (cat === "vector") {
      await vectorKind(data, rc, backend, suffix, mountFatal);
    } else if (cat === "graph") {
      await graphKind(data, rc, backend, suffix, mountFatal);
    }
  }
  assert.ok((exercised["relational"] ?? 0) > 0, "no relational backend advertised — expected at least postgres");
}

async function objectKind(data: any, rc: (p: string) => any, backend: string, suffix: string, mountFatal: MountFatal): Promise<void> {
  const bucket = process.env.UDB_LIVE_S3_BUCKET || "udb-live-sdk";
  const key = `kind/${backend}/${suffix}.txt`;
  const body = Buffer.from(`object-kind-${backend}-${suffix}`, "utf8");
  const opts = { deadlineMs: 8_000, noRetry: true };
  try { await data.ensure_resource({ context: rc("ts.live.kind.object"), backend, resource_name: bucket, spec_json: "{}" }, opts); } catch (err) { mountFatal(backend, "ensure_resource", err); }
  try {
    const put = data.put_object({ deadlineMs: 10_000, noRetry: true });
    put.stream.write({ context: rc("ts.live.kind.object"), bucket, object_key: key, data: body, content_type: "text/plain", final_chunk: true });
    put.stream.end();
    await put.response;
  } catch (err) { mountFatal(backend, "put_object", err); return; }
  try {
    const chunks = await drainReadable(data.get_object({ context: rc("ts.live.kind.object"), bucket, object_key: key }, { deadlineMs: 10_000 }));
    const got = Buffer.concat(chunks.map((c) => Buffer.from(c.data)));
    if (got.length) assert.equal(got.toString("utf8"), body.toString("utf8"), `object backend ${backend} round-trip body mismatch`);
  } catch (err) { mountFatal(backend, "get_object", err); }
}

async function documentKind(data: any, rc: (p: string) => any, backend: string, suffix: string, mountFatal: MountFatal): Promise<void> {
  const opts = { deadlineMs: 8_000, noRetry: true };
  const collection = `sdk_kind_docs_${backend.replace(/[^a-zA-Z0-9_]/g, "_")}_${suffix.replace(/[^a-zA-Z0-9_]/g, "_")}`;
  const documentId = `doc-${suffix}`;
  const resource = { backend, resource_name: collection };
  try { await data.ensure_resource({ context: rc("ts.live.kind.document"), backend, resource_name: collection, spec_json: JSON.stringify({ collection }) }, opts); } catch (err) { mountFatal(backend, "ensure_resource", err); }
  try { await data.document_upsert({ context: rc("ts.live.kind.document"), resource, document_id: documentId, document: { _id: documentId, payload: `doc-${backend}`, revision: 1 } }, opts); } catch (err) { mountFatal(backend, "mutate", err); return; }
  try {
    const got = await data.document_get({ context: rc("ts.live.kind.document"), resource, document_id: documentId }, opts);
    if ((got.documents ?? []).length) assert.equal(structField(got.documents[0], "payload"), `doc-${backend}`);
  } catch (err) { mountFatal(backend, "query", err); }
  try { await data.document_delete({ context: rc("ts.live.kind.document"), resource, document_id: documentId }, opts); } catch (err) { mountFatal(backend, "mutate", err); }
}

async function cacheKind(data: any, rc: (p: string) => any, backend: string, suffix: string, mountFatal: MountFatal): Promise<void> {
  const opts = { deadlineMs: 8_000, noRetry: true };
  const resource = { backend };
  const key = `sdk-live-cache-${suffix}`;
  const val = Buffer.from(`cache-${backend}-${suffix}`, "utf8");
  try { await data.cache_set({ context: rc("ts.live.kind.cache"), resource, key, value: val, content_type: "text/plain", ttl_seconds: 60 }, opts); } catch (err) { mountFatal(backend, "cache_set", err); return; }
  try {
    const got = await data.cache_get({ context: rc("ts.live.kind.cache"), resource, key }, opts);
    if (got.found) assert.equal(Buffer.from(got.value).toString("utf8"), val.toString("utf8"), `cache backend ${backend} CacheGet mismatch`);
  } catch (err) { mountFatal(backend, "cache_get", err); }
  try { await data.cache_scan({ context: rc("ts.live.kind.cache"), resource, key_pattern: "sdk-live-cache-*", limit: 10 }, opts); } catch (err) { mountFatal(backend, "cache_scan", err); }
  try { await data.cache_delete({ context: rc("ts.live.kind.cache"), resource, key }, opts); } catch (err) { mountFatal(backend, "cache_delete", err); }
}

async function vectorKind(data: any, rc: (p: string) => any, backend: string, suffix: string, mountFatal: MountFatal): Promise<void> {
  const opts = { deadlineMs: 8_000, noRetry: true };
  const collection = `sdk_kind_vec_${backend.replace(/[^a-zA-Z0-9_]/g, "_")}_${suffix.replace(/[^a-zA-Z0-9_]/g, "_")}`;
  try { await data.ensure_resource({ context: rc("ts.live.kind.vector"), backend, resource_name: collection, spec_json: JSON.stringify({ dimension: 4, distance: "cosine" }) }, opts); } catch (err) { mountFatal(backend, "ensure_resource", err); }
  const vector = [0.1, 0.2, 0.3, 0.4];
  try { await data.vector_upsert({ context: rc("ts.live.kind.vector"), collection, points: [{ id: `v-${suffix}`, vector, payload: { tag: "sdk-live" } }] }, opts); } catch (err) { mountFatal(backend, "mutate", err); return; }
  try { await data.vector_search({ context: rc("ts.live.kind.vector"), collection, vector, limit: 1, with_payload: true }, opts); } catch (err) { mountFatal(backend, "search", err); }
}

async function graphKind(data: any, rc: (p: string) => any, backend: string, suffix: string, mountFatal: MountFatal): Promise<void> {
  const opts = { deadlineMs: 8_000, noRetry: true };
  const resource = { backend };
  const label = `SdkLive${suffix.replace(/[^a-zA-Z0-9]/g, "")}`;
  try { await data.graph_mutate({ context: rc("ts.live.kind.graph"), resource, query: `CREATE (n:${label} {id: $id}) RETURN n`, parameters: { id: suffix } }, opts); } catch (err) { mountFatal(backend, "mutate", err); return; }
  try { await data.graph_query({ context: rc("ts.live.kind.graph"), resource, query: `MATCH (n:${label}) RETURN n LIMIT 1`, read_only: true }, opts); } catch (err) { mountFatal(backend, "query", err); }
}

async function runLiveBackendE2E(project: UdbProject, tenantId: string, projectId: string): Promise<void> {
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
  assert.equal(inserted.affected_rows, "1");
  assert.equal(mutationRecordJson(inserted).payload, "created-from-ts");

  const selected = await data.select({
    context: ctx,
    message_type: LIVE_MESSAGE_TYPE,
    filter: { record_id: recordId, tenant_id: tenantId, project_id: projectId },
    limit: 1,
  }, { deadlineMs: 5_000, noRetry: true });
  assert.equal(recordJson(selected).revision, 1);

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
  assert.equal(mutationRecordJson(updated).payload, "updated-from-ts");

  const selectV2 = await drainReadable(data.select_v2({
    context: ctx,
    message_type: LIVE_MESSAGE_TYPE,
    filter: { record_id: recordId, tenant_id: tenantId, project_id: projectId },
    limit: 1,
  }, { deadlineMs: 5_000 }));
  assert.ok(selectV2.length >= 1, "SelectV2 must stream at least one batch for an existing row");

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
  assert.ok(batchResponses.length >= 1, "BatchUpsert must produce a mutation response");

  const batchSelect = data.batch_select({ deadlineMs: 5_000, noRetry: true });
  const batchRows = await drainDuplexOnce(batchSelect, [{
    context: ctx,
    message_type: LIVE_MESSAGE_TYPE,
    filter: { record_id: secondRecordId, tenant_id: tenantId, project_id: projectId },
    limit: 1,
  }]);
  assert.equal(recordJson(batchRows[0]).payload, "created-from-ts-batch");

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
  assert.ok(resourceList.resources.some((name: string) => name.includes(collection)), "Mongo collection must be listed after EnsureResource");

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
  assert.equal(structField(mongoGet.documents?.[0], "payload"), "mongo-created");

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
  assert.equal(structField(mongoFind.documents?.[0], "payload"), "mongo-updated");
  const mongoDeleted = await data.document_delete({
    context: ctx,
    resource: { backend: "mongodb", resource_name: collection },
    document_id: documentId,
  }, { deadlineMs: 5_000, noRetry: true });
  assert.equal(mongoDeleted.affected_rows, "1");

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
  assert.equal(putResponse.affected_rows, "1");

  const objectChunks = await drainReadable(data.get_object({
    context: ctx,
    bucket,
    object_key: objectKey,
  }, { deadlineMs: 10_000 }));
  assert.equal(Buffer.concat(objectChunks.map((chunk) => Buffer.from(chunk.data))).toString("utf8"), objectBody.toString("utf8"));
  const presigned = await data.generate_presigned_url({
    context: ctx,
    bucket,
    object_key: objectKey,
    method: "GET",
    ttl_seconds: 60,
  }, { deadlineMs: 5_000, noRetry: true });
  assert.match(presigned.url, /^https?:\/\//);

  const deleted = await data.delete({
    context: ctx,
    message_type: LIVE_MESSAGE_TYPE,
    filter: { record_id: recordId, tenant_id: tenantId, project_id: projectId },
  }, { deadlineMs: 5_000, noRetry: true });
  assert.equal(deleted.affected_rows, "1");
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
  assert.equal(afterDelete.records_json.length, 0);

  // Control-plane data ops with real assertions: project create+list, policy
  // reads, catalog/schema/health. PutPolicy is intentionally NOT called — an abac
  // policy insert flips the data plane to default-deny.
  const projId = `sdklive_proj_ts_${suffix}`;
  await data.ensure_project({ context: ctx, project_id: projId, name: "SDK Live Project" }, { deadlineMs: 8_000, noRetry: true });
  const projects = await data.list_projects({ context: ctx }, { deadlineMs: 8_000, noRetry: true });
  assert.ok((projects.projects ?? []).some((p: any) => p.project_id === projId), "ListProjects must include the created project");
  await data.list_policies({ context: ctx }, { deadlineMs: 8_000, noRetry: true });
  await data.lint_policies({ context: ctx }, { deadlineMs: 8_000, noRetry: true });
  const manifest = await data.get_catalog_manifest({ context: ctx }, { deadlineMs: 8_000, noRetry: true });
  assert.ok(manifest.manifest_json, "GetCatalogManifest must return a manifest");
  const schemas = await data.list_message_schemas({ context: ctx, project_id: projectId }, { deadlineMs: 8_000, noRetry: true });
  assert.ok((schemas.message_types ?? []).length > 0, "ListMessageSchemas must return message types");
  const lookup = await data.lookup_message_schema({ context: ctx, project_id: projectId, message_type: LIVE_MESSAGE_TYPE }, { deadlineMs: 8_000, noRetry: true });
  assert.ok(lookup.schema, `LookupMessageSchema must resolve ${LIVE_MESSAGE_TYPE}`);
  await data.get_health_report({ context: ctx, with_probes: true, project_id: projectId }, { deadlineMs: 8_000, noRetry: true });
}

function liveUuid(): string {
  return (globalThis as any).crypto.randomUUID();
}

// Real create→read→assert CRUD against every native control-plane service.
// Most services accept the free-text "sdk-live" tenant via the main project;
// storage/webrtc/asset persist tenant_id into a UUID column cross-checked
// against the bearer claim, so they run through `uuidProject` (a second admin
// bootstrapped on a UUID tenant). Authz created_by must be a UUID; the
// notification recipient_id is an FK to a real users row.
async function runLiveNativeServiceE2E(
  project: UdbProject,
  uuidProject: UdbProject,
  tenantId: string,
  projectId: string,
  uuidTenant: string,
): Promise<void> {
  const gen = (project as any).authGenerated ?? project.generated;
  const ugen = (uuidProject as any).authGenerated ?? uuidProject.generated;
  const opts = { deadlineMs: 8_000, noRetry: true };
  const suffix = `${process.pid}${Date.now()}`;

  // TenantService — CreateTenant is a platform write (Get/Update/List are
  // tenant-self-scoped and the bootstrap admin's tenant has no tenants-table row).
  const createdTenant = await gen.TenantService.create_tenant(
    { code: `sdklivets${suffix}`, name: "SDK Live TS", type: "WORKSPACE" }, opts);
  assert.ok(createdTenant.tenant_id, "CreateTenant must return a tenant_id");

  // AuthzService — role create/get/list.
  const roleCode = `sdk_reader_ts_${suffix}`;
  const createdRole = (await gen.AuthzService.create_role({
    name: `SDK Reader TS ${suffix}`, description: "Live SDK reader role", created_by: liveUuid(),
    role_code: roleCode, domain: tenantId, tenant_id: tenantId, project_id: projectId,
  }, opts)).role;
  assert.equal(createdRole.role_code, roleCode);
  const gotRole = (await gen.AuthzService.get_role({ role_id: createdRole.role_id }, opts)).role;
  assert.equal(gotRole.role_code, roleCode);
  const roles = await gen.AuthzService.list_roles({ domain: tenantId, active_only: true }, opts);
  assert.ok((roles.roles ?? []).some((r: any) => r.role_id === createdRole.role_id), "ListRoles must include created role");

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
  assert.ok(allowed.allowed, "CheckAccess must allow the assigned role+policy");
  const userRoles = await gen.AuthzService.list_user_roles({ user_id: subject.user_id, domain: tenantId, active_only: true }, opts);
  assert.equal((userRoles.user_roles ?? []).length, 1);
  await gen.AuthzService.revoke_role({ user_role_id: assignedRole.user_role_id, user_id: subject.user_id, reason: "sdk_live_test", revoked_by: subject.user_id }, opts);
  const denied = await gen.AuthzService.check_access({
    user_id: subject.user_id, domain: tenantId, tenant_id: tenantId, project_id: projectId, object: "invoice", action: "data.select",
  }, opts);
  assert.ok(!denied.allowed, "CheckAccess must deny after the role was revoked");

  // ApiKeyService — create/validate/list/revoke lifecycle.
  const principal = `sdk-live-svc-${suffix}`;
  const keyCtx = { user_id: principal, tenant: { tenant_id: tenantId, project_id: projectId } };
  const createdKey = await gen.ApiKeyService.create_api_key(
    { name: `sdk-live-key-${suffix}`, owner_id: principal, scopes: ["data:read"], context: keyCtx }, opts);
  assert.ok(String(createdKey.plain_key).startsWith("udbk_"), "plain_key must have udbk_ prefix");
  const keyId = createdKey.key.key_id;
  const valid = await gen.ApiKeyService.validate_api_key({ plain_key: createdKey.plain_key, required_scope: "data:read" }, opts);
  assert.ok(valid.valid && valid.owner_id === principal, "ValidateApiKey must accept the fresh key");
  const listedKeys = await gen.ApiKeyService.list_api_keys({ owner_id: principal, status: 1 }, opts); // 1 = ACTIVE
  assert.equal((listedKeys.keys ?? []).length, 1);
  assert.equal(listedKeys.keys[0].key_id, keyId);
  const gotKey = await gen.ApiKeyService.get_api_key({ key_id: keyId }, opts);
  assert.equal(gotKey.key.owner_id, principal);
  await gen.ApiKeyService.update_api_key({ key_id: keyId, scopes: ["data:read", "data:write"], context: keyCtx }, opts);
  const writeOK = await gen.ApiKeyService.validate_api_key({ plain_key: createdKey.plain_key, required_scope: "data:write" }, opts);
  assert.ok(writeOK.valid, "ValidateApiKey must honor the updated data:write scope");
  await gen.ApiKeyService.revoke_api_key({ key_id: keyId, revoke_reason: "sdk_live_test", context: keyCtx }, opts);
  const afterRevoke = await gen.ApiKeyService.validate_api_key({ plain_key: createdKey.plain_key, required_scope: "data:read" }, opts);
  assert.ok(!afterRevoke.valid, "revoked API key must not validate");

  // AnalyticsService — record metrics then roll up.
  const stage = `sdk_live_stage_ts_${suffix}`;
  for (const [latency, ok] of [[100, true], [200, true], [400, false]] as [number, boolean][]) {
    const accepted = await gen.AnalyticsService.record_pipeline_metric(
      { stage_name: stage, tenant_id: tenantId, latency_ms: latency, is_success: ok }, opts);
    assert.ok(accepted.accepted, "RecordPipelineMetric must be accepted");
  }
  const summary = await gen.AnalyticsService.get_pipeline_summary(
    { stage_name: stage, tenant_id: tenantId, page: { page: 1, page_size: 10 } }, opts);
  assert.equal((summary.snapshots ?? []).length, 1);
  assert.equal(Number(summary.snapshots[0].total_requests), 3);
  const throughput = await gen.AnalyticsService.get_throughput({ tenant_id: tenantId }, opts);
  assert.ok(Number(throughput.total_requests) >= 3);
  const trig = await gen.AnalyticsService.trigger_snapshot({ stage_name: stage }, opts);
  assert.ok(Number(trig.snapshots_written) >= 1);

  // NotificationService — template + send to a real user (recipient_id FK).
  const recipient = (await gen.AuthnService.create_user({
    username: `sdk-notif-ts-${suffix}`, email: `sdk-notif-ts-${suffix}@example.com`, password: "CorrectHorse1!",
    tenant_id: tenantId, project_id: projectId, full_name: "SDK Notify TS",
  }, opts)).user;
  const event = `sdk.live.ts.${suffix}`;
  const body = `sdk-live-body-ts-${suffix}`;
  await gen.NotificationService.upsert_template(
    { event_type: event, channel: 1, locale: "en", subject_template: "SDK {{n}}", body_template: body, is_active: true }, opts);
  const template = (await gen.NotificationService.get_template({ event_type: event, channel: 1, locale: "en" }, opts)).template;
  assert.equal(template.body_template, body);
  const sent = await gen.NotificationService.send_notification(
    { event_type: event, recipient_id: recipient.user_id, recipient_address: `sdk+${suffix}@example.com`, tenant_id: tenantId, channels: [1] }, opts);
  assert.ok((sent.logs ?? []).length >= 1, "SendNotification must record a log");
  const logId = sent.logs[0].log_id;
  const listedNotifs = await gen.NotificationService.list_notifications({ tenant_id: tenantId }, opts);
  assert.ok((listedNotifs.logs ?? []).some((l: any) => l.log_id === logId), "ListNotifications must include the sent log");
  const gotNotif = await gen.NotificationService.get_notification({ log_id: logId }, opts);
  assert.equal(gotNotif.log.log_id, logId);
  await gen.NotificationService.set_preference({ user_id: recipient.user_id, tenant_id: tenantId, channel: 1, is_opted_out: true }, opts);
  const pref = await gen.NotificationService.get_preference({ user_id: recipient.user_id, tenant_id: tenantId, channel: 1 }, opts);
  assert.ok(pref.preference.is_opted_out, "GetPreference must reflect the opt-out we set");
  const prefs = await gen.NotificationService.list_preferences({ user_id: recipient.user_id, tenant_id: tenantId }, opts);
  assert.ok((prefs.preferences ?? []).length >= 1);
  await gen.NotificationService.get_delivery_stats({ tenant_id: tenantId }, opts);

  // StorageService — file lifecycle under the UUID-tenant admin (project_id and
  // reference_id are UUID columns: empty project → NULL, reference_id a UUID).
  const ref = liveUuid();
  const reg = await ugen.StorageService.register_upload({
    tenant_id: uuidTenant, project_id: "", filename: `sdk-${suffix}.txt`, content_type: "text/plain",
    file_type: "DOCUMENT", reference_id: ref, reference_type: "sdk.live", size_bytes: 128, expires_in_minutes: 10,
  }, opts);
  assert.ok(reg.file_id && String(reg.upload_url).startsWith("http"), "RegisterUpload must return file_id + upload_url");
  const gotFile = await ugen.StorageService.get_file({ tenant_id: uuidTenant, file_id: reg.file_id }, opts);
  assert.equal(gotFile.file.file_id, reg.file_id);
  const renamedFile = `sdk-renamed-${suffix}.txt`;
  await ugen.StorageService.update_file({ tenant_id: uuidTenant, file_id: reg.file_id, filename: renamedFile }, opts);
  const rereadFile = await ugen.StorageService.get_file({ tenant_id: uuidTenant, file_id: reg.file_id }, opts);
  assert.equal(rereadFile.file.filename, renamedFile, "UpdateFile rename must persist");
  const download = await ugen.StorageService.get_download_url({ tenant_id: uuidTenant, file_id: reg.file_id, expires_in_minutes: 10 }, opts);
  assert.match(download.download_url, /^https?:\/\//);
  const listedFiles = await ugen.StorageService.list_files({ tenant_id: uuidTenant, reference_id: ref }, opts);
  assert.ok(Number(listedFiles.total_count) >= 1);
  const deletedFile = await ugen.StorageService.delete_file({ tenant_id: uuidTenant, file_id: reg.file_id }, opts);
  assert.ok(deletedFile.success, "DeleteFile must succeed");

  // AssetService — pipeline definition + asset registered against a stored file.
  const assetFile = await ugen.StorageService.register_upload({
    tenant_id: uuidTenant, project_id: "", filename: `asset-${suffix}.json`, content_type: "application/json",
    file_type: "OTHER", reference_id: liveUuid(), reference_type: "sdk.asset", size_bytes: 64, expires_in_minutes: 10,
  }, opts);
  const definition = await ugen.AssetService.create_pipeline_definition({
    tenant_id: uuidTenant, name: `sdk-pipeline-${suffix}`, description: "Live SDK pipeline",
    media_type: "application/json", steps: '[{"name":"extract","type":"EXTRACT"}]', version: 1,
  }, opts);
  assert.ok(definition.definition_id, "CreatePipelineDefinition must return definition_id");
  await ugen.AssetService.get_pipeline_definition({ tenant_id: uuidTenant, definition_id: definition.definition_id }, opts);
  const registeredAsset = await ugen.AssetService.register_asset({
    tenant_id: uuidTenant, project_id: "", file_id: assetFile.file_id, name: `sdk-asset-${suffix}`,
    media_type: "application/json", metadata: '{"source":"sdk-live"}',
  }, opts);
  assert.ok(registeredAsset.asset_id, "RegisterAsset must return asset_id");
  await ugen.AssetService.get_asset({ tenant_id: uuidTenant, asset_id: registeredAsset.asset_id }, opts);
  const startedPipeline = await ugen.AssetService.start_pipeline({
    tenant_id: uuidTenant, definition_id: definition.definition_id, asset_id: registeredAsset.asset_id,
    context: "{}", correlation_id: `sdk-live-${suffix}`,
  }, opts);
  assert.ok(startedPipeline.instance_id, "StartPipeline must return instance_id");
  await ugen.AssetService.get_pipeline({ tenant_id: uuidTenant, instance_id: startedPipeline.instance_id }, opts);
  const listedAssets = await ugen.AssetService.list_assets({ tenant_id: uuidTenant }, opts);
  assert.ok((listedAssets.assets ?? []).some((a: any) => a.asset_id === registeredAsset.asset_id), "ListAssets must include the registered asset");

  // WebRTC — room/peer/track lifecycle + best-effort TURN issuance.
  const room = await ugen.RoomService.create_room(
    { tenant_id: uuidTenant, name: `sdk-room-${suffix}`, max_participants: 8, config: "{}", created_by: liveUuid() }, opts);
  assert.ok(room.room_id, "CreateRoom must return room_id");
  await ugen.RoomService.get_room({ tenant_id: uuidTenant, room_id: room.room_id }, opts);
  const listedRooms = await ugen.RoomService.list_rooms({ tenant_id: uuidTenant }, opts);
  assert.ok((listedRooms.rooms ?? []).some((r: any) => r.room_id === room.room_id), "ListRooms must include created room");
  const joined = await ugen.PeerService.join_room(
    { tenant_id: uuidTenant, room_id: room.room_id, display_name: "sdk-peer", metadata: "{}", user_agent: "sdk-live" }, opts);
  assert.ok(joined.peer.peer_id, "JoinRoom must return a peer_id");
  const peerList = await ugen.PeerService.list_peers({ tenant_id: uuidTenant, room_id: room.room_id }, opts);
  assert.ok((peerList.peers ?? []).some((p: any) => p.peer_id === joined.peer.peer_id), "ListPeers must include the joined peer");
  await ugen.PeerService.get_peer({ tenant_id: uuidTenant, peer_id: joined.peer.peer_id }, opts);
  await ugen.RoomService.update_room({ tenant_id: uuidTenant, room_id: room.room_id, name: `sdk-room-renamed-${suffix}` }, opts);
  const published = await ugen.TrackService.publish_track(
    { tenant_id: uuidTenant, room_id: room.room_id, peer_id: joined.peer.peer_id, kind: "audio", label: "mic", settings: "{}", metadata: "{}" }, opts);
  assert.ok(published.track_id, "PublishTrack must return a track_id");
  const tracks = await ugen.TrackService.list_tracks({ tenant_id: uuidTenant, room_id: room.room_id }, opts);
  assert.ok((tracks.tracks ?? []).length >= 1, "ListTracks must return the published track");
  await ugen.TrackService.mute_track({ tenant_id: uuidTenant, track_id: published.track_id, muted: true }, opts);
  await ugen.TrackService.unpublish_track({ tenant_id: uuidTenant, track_id: published.track_id }, opts);
  try {
    // TURN issuance is best-effort: coturn may be unconfigured locally and the
    // service fail-closes with a real status (not a mount failure).
    await ugen.TurnService.issue_credentials(
      { tenant_id: uuidTenant, room_id: room.room_id, peer_id: joined.peer.peer_id, ttl_seconds: 3600 }, opts);
  } catch (err) {
    const code = grpcCode(err);
    if (code !== undefined && FATAL_CONNECTIVITY_CODES.has(code)) {
      throw new Error(`TurnService.issue_credentials did not reach a live RPC: ${describeGrpcError(err)}`);
    }
  }
  const left = await ugen.PeerService.leave_room({ tenant_id: uuidTenant, room_id: room.room_id, peer_id: joined.peer.peer_id }, opts);
  assert.ok(left.success, "LeaveRoom must succeed");
  await ugen.RoomService.close_room({ tenant_id: uuidTenant, room_id: room.room_id }, opts);
}

test("live broker login refreshes once and hot-swaps SDK credentials", {
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
    const probe = new UdbProject({
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
    } finally {
      probe.close();
    }
  }
  assert.ok(tenantId, "must resolve a canonical tenant id before conformance");
  const store = memoryStore();

  const project = new UdbProject({
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
    assert.ok(login.access_token, "live login must return an access token");
    assert.ok(login.refresh_token, "live login must return a refresh token");
    assert.equal(store.current()?.accessToken, login.access_token);

    const authn = await project.auth.authenticateBearer(login.access_token);
    assert.ok(authn?.principal, "Authenticate must accept the token issued by Login");
    // tenantId is already the canonical UUID (resolved by the pre-login probe above),
    // so the project's x-tenant-id header AND all request bodies match the JWT claim.
    assert.equal(authn.principal.tenant_id, tenantId, "principal tenant must match the resolved canonical UUID");

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
    assert.equal(accessTokens.size, 1, "concurrent refresh callers must share one result");
    assert.notEqual(refreshed[0]?.accessToken, login.access_token);
    assert.equal(store.current()?.accessToken, refreshed[0]?.accessToken);

    // Don't trust the capability claim — exercise every advertised backend.
    const caps = await project.generated.DataBroker.get_capabilities({}, { deadlineMs: 5_000, noRetry: true });
    const enabledBackends = (caps.enabled_backends ?? []).map((b: string) => b.toLowerCase());
    await runLiveBackendClaimCheck(project.generated.DataBroker, requestContext(tenantId, projectId, "ts.live.backend.claim"), enabledBackends);

    // Challenge every advertised backend KIND's per-operation claims in BOTH directions.
    await runLiveBackendCapabilityChallenge(project.generated.DataBroker, requestContext(tenantId, projectId, "ts.live.backend.capability"), caps);

    // Full session lifecycle on a throwaway login: prove logout invalidates the
    // session (access token + refresh token + session-refresh all rejected after).
    await runLiveAuthLifecycle((project as any).authGenerated?.AuthnService ?? project.generated.AuthnService, tenantId, projectId, username, password);

    // Edge cases: the auth plane must fail CLOSED on bad credentials/forged bearers.
    await runLiveAuthNegative((project as any).authGenerated?.AuthnService ?? project.generated.AuthnService, tenantId, projectId, username);

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

    const authGenerated = (project as any).authGenerated ?? project.generated;
    const probeCounters = { populated: 0 };
    const nativeCount = await expectGeneratedUnarySurfaceMounted(
      t,
      "authTarget",
      authGenerated,
      NATIVE_SERVICE_APIS,
      tenantId,
      projectId,
      probeCounters,
    );
    const dataCount = await expectGeneratedUnarySurfaceMounted(
      t,
      "target",
      project.generated,
      ["DataBroker"],
      tenantId,
      projectId,
      probeCounters,
    );

    await expectStreamMounted("target.DataBroker.get_object", () =>
      project.generated.DataBroker.get_object({}, { deadlineMs: 2_000 }),
    );
    await expectStreamMounted("target.DataBroker.publish_c_d_c", () =>
      project.generated.DataBroker.publish_c_d_c({}, { deadlineMs: 2_000 }),
    );
    await expectStreamMounted("target.DataBroker.select_v2", () =>
      project.generated.DataBroker.select_v2({}, { deadlineMs: 2_000 }),
    );
    await expectStreamMounted("target.DataBroker.put_object", () => {
      const { stream, response } = project.generated.DataBroker.put_object({ deadlineMs: 2_000 });
      // The probe ends an empty stream → the broker rejects with "empty object
      // stream" (INVALID_ARGUMENT). That proves PutObject is mounted; the
      // separate response promise must be caught or it becomes an unhandled
      // rejection that fails the test.
      response.catch(() => {});
      return stream;
    });
    await expectStreamMounted("target.DataBroker.batch_select", () =>
      project.generated.DataBroker.batch_select({ deadlineMs: 2_000 }),
    );
    await expectStreamMounted("target.DataBroker.batch_upsert", () =>
      project.generated.DataBroker.batch_upsert({ deadlineMs: 2_000 }),
    );
    await expectStreamMounted("target.DataBroker.begin_tx", () =>
      project.generated.DataBroker.begin_tx({ deadlineMs: 2_000 }),
    );
    await expectStreamMounted("target.DataBroker.vector_batch_upsert", () =>
      project.generated.DataBroker.vector_batch_upsert({ deadlineMs: 2_000 }),
    );
    await expectStreamMounted("authTarget.ControlPlaneService.delta_resources", () =>
      authGenerated.ControlPlaneService.delta_resources({ deadlineMs: 2_000 }),
    );
    await expectStreamMounted("authTarget.ControlPlaneService.stream_resources", () =>
      authGenerated.ControlPlaneService.stream_resources({ deadlineMs: 2_000 }),
    );
    await expectStreamMounted("authTarget.SignalingService.signal", () =>
      authGenerated.SignalingService.signal({ deadlineMs: 2_000 }),
    );

    assert.ok(nativeCount > 0, "native control-plane unary RPCs must be probed");
    assert.ok(dataCount > 0, "DataBroker unary RPCs must be probed");
    // Full-surface coverage like Go/Python/PHP: 262 RPCs total = 251 unary
    // (nativeCount + dataCount) + the 11 streaming RPCs probed individually below.
    const STREAMING_PROBED = 11; // get_object, publish_c_d_c, select_v2, put_object,
    //   batch_select, batch_upsert, begin_tx, vector_batch_upsert, delta_resources,
    //   stream_resources, signal
    assert.equal(
      nativeCount + dataCount + STREAMING_PROBED,
      262,
      `TS probed ${nativeCount + dataCount} unary + ${STREAMING_PROBED} streaming = ${nativeCount + dataCount + STREAMING_PROBED}, want 262 — full-surface coverage regressed`,
    );
    assert.ok(
      probeCounters.populated >= 200,
      `only ${probeCounters.populated} unary RPCs received a populated typed request; full-surface coverage regressed`,
    );
  } finally {
    project.close();
  }
});

// Per-RPC performance (gated on UDB_LIVE_PERF=1). Times every unary RPC over
// multiple iterations and writes perf_report_ts.md — the TS counterpart of the
// Go/Python perf harness. read_only RPCs are timed many times; mutations a few;
// destructive once typed-empty (validation latency only).
test("live per-RPC perf", {
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

  const project = new UdbProject({
    target, authTarget, tenantId, projectId,
    purpose: "ts.live.perf", tokenStore: memoryStore(), deadlineMs: 20_000,
  });
  try {
    const login = await project.login({ username, password, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-sdk-perf" });
    const who = await project.auth.authenticateBearer(login.access_token);
    tenantId = who?.principal?.tenant_id || tenantId;

    const authGenerated = (project as any).authGenerated ?? project.generated;
    const itersFor = (kind: string) => (kind === "destructive" ? 1 : kind === "mutation" ? 5 : 25);
    type Sample = { service: string; rpc: string; kind: string; p50: number; p99: number; mean: number };
    const samples: Sample[] = [];

    const timeMethod = async (fn: any, request: any): Promise<number> => {
      const start = performance.now();
      try { await fn(request, { deadlineMs: 20_000, noRetry: true }); } catch { /* latency still counts */ }
      return performance.now() - start;
    };

    // Stream-open timer: create the streaming call and tear it down WITHOUT draining
    // responses. A subscription/upload stream emits a first message only on an event,
    // so draining it in a passive run would just hit the deadline. This measures the
    // client-side latency to establish the stream.
    const timeStreamOpen = (fn: any, request: any): number => {
      const start = performance.now();
      try {
        const r = (fn as any)(request, { deadlineMs: 1_500, noRetry: true });
        const s = r?.stream ?? r;
        if (s && typeof s.cancel === "function") s.cancel();
        else if (s && typeof s.destroy === "function") s.destroy();
        if (r?.response && typeof r.response.catch === "function") r.response.catch(() => {});
      } catch { /* setup latency still counts */ }
      return performance.now() - start;
    };

    const surfaces: Array<[string, any, readonly string[]]> = [
      ["authTarget", authGenerated, NATIVE_SERVICE_APIS],
      ["target", project.generated, ["DataBroker"]],
    ];
    for (const [, generated, serviceNames] of surfaces) {
      for (const serviceName of serviceNames) {
        const api = generated[serviceName];
        if (!api) continue;
        for (const [methodName, fn] of Object.entries(api)) {
          if (methodName === "serviceFull") continue;
          if (typeof fn !== "function") continue;
          if (NON_UNARY_METHODS.has(methodName)) {
            // Measured, not dropped — streaming reports stream-open latency.
            const d = timeStreamOpen(fn, surfaceProbeRequest(tenantId, projectId));
            samples.push({ service: serviceName, rpc: methodName, kind: "stream_open", p50: d, p99: d, mean: d });
            continue;
          }
          const kind = operationKindOf((api as any).serviceFull, methodName) || "read_only";
          const request = kind !== "destructive" ? surfaceProbeRequest(tenantId, projectId) : {};
          await timeMethod(fn, request); // warm-up
          const durs: number[] = [];
          for (let i = 0; i < itersFor(kind); i++) durs.push(await timeMethod(fn, request));
          durs.sort((a, b) => a - b);
          const pct = (p: number) => durs[Math.min(durs.length - 1, Math.floor((p * (durs.length - 1)) / 100))];
          samples.push({
            service: serviceName, rpc: methodName, kind,
            p50: pct(50), p99: pct(99), mean: durs.reduce((s, d) => s + d, 0) / durs.length,
          });
        }
      }
    }

    const svc = new Map<string, number[]>();
    for (const s of samples) { (svc.get(s.service) ?? svc.set(s.service, []).get(s.service)!).push(s.mean); }
    const mean = (xs: number[]) => xs.reduce((a, b) => a + b, 0) / xs.length;
    const lines = ["# UDB SDK Live Perf — TypeScript (localhost)", "",
      `RPCs measured: ${samples.length}`, "",
      "Unary = full request/response round-trip. Streaming rows (kind=stream_open) report "
      + "stream-open latency (establish the stream, no response drain), NOT first-message latency.", "",
      "## Per-service mean latency", "", "| Service | RPCs | mean ms |", "|---|--:|--:|"];
    for (const name of [...svc.keys()].sort((a, b) => mean(svc.get(b)!) - mean(svc.get(a)!))) {
      lines.push(`| ${name} | ${svc.get(name)!.length} | ${mean(svc.get(name)!).toFixed(2)} |`);
    }
    lines.push("", "## Slowest 20 by p99", "", "| RPC | kind | p50 ms | p99 ms | mean ms |", "|---|---|--:|--:|--:|");
    for (const s of [...samples].sort((a, b) => b.p99 - a.p99).slice(0, 20)) {
      lines.push(`| ${s.service}/${s.rpc} | ${s.kind} | ${s.p50.toFixed(2)} | ${s.p99.toFixed(2)} | ${s.mean.toFixed(2)} |`);
    }
    writeFileSync("perf_report_ts.md", lines.join("\n") + "\n");
    assert.ok(samples.length >= 260, `perf measured only ${samples.length} RPCs (want all 262)`);
    console.log(`\nTS perf: ${samples.length} RPCs measured → sdk/typescript/perf_report_ts.md`);
  } finally {
    project.close();
  }
});
