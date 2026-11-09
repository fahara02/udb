// Unit tests for the surface shared with the Go SDK: enterprise session, tenant pool,
// environment bootstrap, per-request user, live query, graph and vector builders,
// flags, schema reflection, credential introspection and gRPC server interceptors.
//
// Anything that crosses the wire runs over a real grpc-js server on loopback, so the
// requests are really encoded and the assertions read what a broker would receive.

import { strict as assert } from "node:assert";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { after, before, test } from "node:test";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

import "./wkt";
import {
  metadataFromGrpc,
  requestCorrelationId,
  requestMetadata,
  requestTenantId,
  requestUserId,
  udbServerInterceptor,
  withRequestMetadata,
} from "./adapters/grpc";
import { metadata as buildMetadata } from "./client";
import { configFromEnv, parseDurationMs, verifyPrincipal } from "./connectEnv";
import { asUserCall } from "./facades";
import { UdbError } from "./generatedClient";
import { graphFrom, vectorsHybrid } from "./graphVector";
import {
  LiveQueryEvent,
  NoDeltaFeedError,
  liveQuery,
  lqEq,
  lqGe,
  lqGt,
  lqIn,
  lqIsNotNull,
  lqIsNull,
  lqLe,
  lqLt,
  lqNe,
  lqNotIn,
} from "./liveQuery";
import { CredentialBindingError, StorageFacade, UdbProject } from "./project";
import { defaultProtoRoot } from "./protoRoot";
import {
  applyColumnDefaults,
  eventType,
  exportEligible,
  graphLabel,
  maxLen,
  ownerColumn,
  ownerColumns,
  partitionKeyField,
  primaryKey,
  primaryKeys,
  projectColumn,
  retentionClass,
  softDelete,
  tenantColumn,
  topic,
  uniqueKeys,
} from "./schema";
import { NotFoundError } from "./stores";
import { TenantPoolClient, TenantSessionPool, newApiKeyTenantPool } from "./tenantPool";
import { TenantState } from "./tenantState";

const TENANT = "11111111-1111-4111-8111-111111111111";
const OTHER_TENANT = "99999999-9999-4999-8999-999999999999";
const PROJECT = "22222222-2222-4222-8222-222222222222";
const SERVICE_BEARER = "service-bearer";
const USER_BEARER = "user-bearer";

// ── Fixture broker ──────────────────────────────────────────────────────────

const root = defaultProtoRoot();
const loaded: any = grpc.loadPackageDefinition(
  protoLoader.loadSync(
    [
      "udb/core/authn/services/v1/authn_service.proto",
      "udb/core/livequery/services/v1/livequery_service.proto",
      "udb/core/config/services/v1/config_service.proto",
      "udb/core/lock/services/v1/lock_service.proto",
      "udb/services/v1/data_broker.proto",
    ],
    {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
      includeDirs: [root, path.resolve(root, "../third_party/googleapis")],
    },
  ),
);

const HEALTH_SERVICE: grpc.ServiceDefinition = {
  Check: {
    path: "/grpc.health.v1.Health/Check",
    requestStream: false,
    responseStream: false,
    requestSerialize: (value: Buffer) => value,
    requestDeserialize: (value: Buffer) => value,
    responseSerialize: (value: Buffer) => value,
    responseDeserialize: (value: Buffer) => value,
  },
};

interface Seen {
  method: string;
  request: any;
  headers: grpc.Metadata;
}

interface Fixture {
  target: string;
  seen: Seen[];
  health: number[];
  /** Replaces the stream of each Subscribe call, in order. */
  subscribe: Array<(call: grpc.ServerWritableStream<any, any>) => void>;
  principal: any;
  authenticate: (request: any) => any;
  validate: (request: any, headers: grpc.Metadata) => any;
  stop: () => void;
}

function principalFixture(overrides: Record<string, unknown> = {}) {
  return {
    tenant_id: TENANT,
    project_id: PROJECT,
    user_id: "",
    principal_id: "svc-1",
    service_identity: "spiffe://fixture/service",
    scopes: ["udb:read", "svc:scope"],
    ...overrides,
  };
}

let fixture: Fixture;

async function startFixture(): Promise<Fixture> {
  const seen: Seen[] = [];
  const state: Fixture = {
    target: "",
    seen,
    health: [],
    subscribe: [],
    principal: principalFixture(),
    authenticate: (request) => ({
      access_token: request.api_key ? SERVICE_BEARER : "",
      expires_at_unix: String(Math.floor(Date.now() / 1000) + 600),
      principal: state.principal,
    }),
    validate: () => ({
      valid: true,
      user_id: "user-1",
      tenant_id: TENANT,
      project_id: PROJECT,
      session_public_id: "sesspub_1",
      roles: ["admin"],
      scopes: ["udb:read"],
      expires_at: { seconds: "1800000000", nanos: 500_000_000 },
      principal: { user_id: "user-1" },
    }),
    stop: () => undefined,
  };
  const record = (method: string, call: { request?: any; metadata: grpc.Metadata }) =>
    seen.push({ method, request: call.request, headers: call.metadata });
  const unary =
    (method: string, reply: (request: any, headers: grpc.Metadata) => any) =>
    (call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) => {
      record(method, call);
      try {
        callback(null, reply(call.request, call.metadata));
      } catch (err) {
        callback(err as grpc.ServiceError);
      }
    };
  const server = new grpc.Server();
  server.addService(loaded.udb.core.authn.services.v1.AuthnService.service, {
    Authenticate: unary("Authenticate", (request) => state.authenticate(request)),
    ValidateToken: unary("ValidateToken", (request, headers) => state.validate(request, headers)),
    Login: unary("Login", () => ({
      access_token: "login-bearer",
      refresh_token: "refresh-1",
      session_id: "session-1",
      access_token_expires_in: "600",
    })),
  });
  server.addService(loaded.udb.core.livequery.services.v1.LiveQueryService.service, {
    Subscribe: (call: grpc.ServerWritableStream<any, any>) => {
      record("Subscribe", call);
      const next = state.subscribe.shift();
      if (next) next(call);
      else call.end();
    },
  });
  server.addService(loaded.udb.core.config.services.v1.ConfigService.service, {
    EvaluateFlags: unary("EvaluateFlags", (request) => {
      const values: Record<string, unknown> = {};
      if (request.keys.includes("on")) values.on = { bool_value: true };
      if (request.keys.includes("off")) values.off = { bool_value: false };
      if (request.keys.includes("text")) values.text = { string_value: "true" };
      return { values };
    }),
  });
  server.addService(loaded.udb.core.lock.services.v1.LockService.service, {
    AcquireLock: unary("AcquireLock", () => ({})),
    ListLocks: unary("ListLocks", () => ({})),
  });
  server.addService(loaded.udb.services.v1.DataBroker.service, {
    GraphQuery: unary("GraphQuery", () => ({
      records: [{ id: "n2", hops: 2 }],
    })),
    VectorHybridSearch: unary("VectorHybridSearch", () => ({
      points: [{ id: "p1", score: 0.5, payload: { title: "rotate" } }],
    })),
  });
  server.addService(HEALTH_SERVICE, {
    Check: (call: grpc.ServerUnaryCall<Buffer, Buffer>, callback: grpc.sendUnaryData<Buffer>) => {
      record("Check", call);
      const status = state.health.length > 1 ? state.health.shift()! : state.health[0] ?? 1;
      callback(null, Buffer.from([0x08, status]));
    },
  });
  const port = await new Promise<number>((resolve, reject) =>
    server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, bound) =>
      err ? reject(err) : resolve(bound),
    ),
  );
  state.target = `127.0.0.1:${port}`;
  state.stop = () => server.forceShutdown();
  return state;
}

function reset(): void {
  fixture.seen.length = 0;
  fixture.health.length = 0;
  fixture.subscribe.length = 0;
  fixture.principal = principalFixture();
}

function header(headers: grpc.Metadata, name: string): string | undefined {
  const values = headers.get(name);
  return values.length ? String(values[0]) : undefined;
}

function last(method: string): Seen {
  const hit = [...fixture.seen].reverse().find((entry) => entry.method === method);
  assert.ok(hit, `no ${method} call reached the fixture`);
  return hit;
}

function project(overrides: Record<string, unknown> = {}): UdbProject {
  return new UdbProject({
    target: fixture.target,
    tenantId: TENANT,
    projectId: PROJECT,
    purpose: "parity-test",
    deadlineMs: 5_000,
    ...overrides,
  });
}

before(async () => {
  fixture = await startFixture();
});

after(() => {
  fixture.stop();
});

// ── 1. Enterprise session and tenant state ──────────────────────────────────

test("TenantState adopts only a canonical lowercase UUID and refuses without mutating", () => {
  const state = new TenantState("acme");
  assert.equal(state.isVerified, false);
  assert.throws(() => state.getVerified(), /tenant not verified/);
  assert.throws(() => state.validateTenantId(TENANT), /tenant not verified/);
  for (const bad of ["", "   ", "acme", "ABCDEF00-1111-4111-8111-111111111111", `${TENANT}x`]) {
    assert.throws(() => state.adopt(bad), /canonical|empty/, `adopting ${JSON.stringify(bad)}`);
    assert.equal(state.canonicalId, "", "a refused adopt must not change the state");
    assert.equal(state.isVerified, false);
  }
  state.adopt(` ${TENANT} `);
  assert.equal(state.getVerified(), TENANT);
  assert.equal(state.tenantCode, "acme");
  state.validateTenantId(TENANT);
  state.validateTenantId("");
  assert.throws(
    () => state.validateTenantId(OTHER_TENANT),
    (err: Error) => err.message.includes(OTHER_TENANT) && err.message.includes(TENANT),
    "a mismatch must name both values",
  );
});

test("connectEnterprise verifies the tenant, adopts the principal and validates records", async () => {
  reset();
  fixture.principal = principalFixture({ user_id: "user-1" });
  const session = await UdbProject.connectEnterprise({
    target: fixture.target,
    username: "admin",
    password: "secret",
    tenantCode: "acme",
    purpose: "parity-test",
    deadlineMs: 5_000,
  });
  try {
    assert.equal(last("Login").request.tenant_hint, "acme");
    assert.equal(session.canonicalTenantId, TENANT);
    assert.equal(session.canonicalProjectId, PROJECT);
    assert.equal(session.tenantState.tenantCode, "acme");
    assert.equal(session.bearer(), "Bearer login-bearer");
    assert.equal(session.principal().user_id, "user-1");
    for (let i = 0; i < 3; i += 1) session.validateTenant(TENANT);
    session.validateTenant("");
    assert.throws(() => session.validateTenant(OTHER_TENANT), /tenant_id mismatch/);
    assert.throws(() => session.validateTenant("acme"), /tenant_id mismatch/);
    await session.flags.enabled("on");
    const sent = last("EvaluateFlags");
    assert.equal(header(sent.headers, "x-tenant-id"), TENANT, "calls must carry the canonical UUID, not the code");
    assert.equal(header(sent.headers, "x-user-id"), "user-1");
    assert.equal(header(sent.headers, "x-scopes"), "udb:read,svc:scope");
    assert.equal(sent.request.tenant_id, TENANT);
  } finally {
    session.close();
  }
});

test("connectEnterprise refuses missing input and a principal without a canonical tenant", async () => {
  reset();
  await assert.rejects(
    UdbProject.connectEnterprise({ target: fixture.target, username: "", password: "x" }),
    /requires target, username, and password/,
  );
  await assert.rejects(
    UdbProject.connectEnterprise({ target: "", username: "u", password: "x" }),
    /requires target, username, and password/,
  );
  assert.equal(fixture.seen.length, 0, "input errors must not reach the broker");
  fixture.principal = principalFixture({ tenant_id: "acme" });
  await assert.rejects(
    UdbProject.connectEnterprise({ target: fixture.target, username: "u", password: "x", tenantId: "acme" }),
    /not a canonical/,
  );
  fixture.principal = principalFixture();
  fixture.authenticate = (original => () => {
    fixture.authenticate = original;
    return { access_token: "x" };
  })(fixture.authenticate);
  await assert.rejects(
    UdbProject.connectEnterprise({ target: fixture.target, username: "u", password: "x" }),
    /no principal returned/,
  );
});

test("a session that was never adopted cannot validate a tenant", () => {
  const plain = project();
  try {
    assert.equal(plain.canonicalTenantId, "");
    assert.throws(() => plain.validateTenant(TENANT), /tenant not verified/);
  } finally {
    plain.close();
  }
});

// ── 2. Tenant pool ──────────────────────────────────────────────────────────

class PoolClient implements TenantPoolClient {
  closes = 0;
  failing: Error | null = null;
  constructor(readonly tenant: string) {}
  credentialError(): Error | null {
    return this.failing;
  }
  close(): void {
    this.closes += 1;
  }
}

function gate() {
  let open = () => {};
  const opened = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { open, opened };
}

test("pool shares one connect, reuses the client and lists tenants sorted", async () => {
  let connects = 0;
  const hold = gate();
  const pool = new TenantSessionPool<PoolClient>(async (tenant) => {
    connects += 1;
    await hold.opened;
    return new PoolClient(tenant);
  });
  const concurrent = [pool.get("b"), pool.get("b"), pool.get("b")];
  hold.open();
  const clients = await Promise.all(concurrent);
  assert.equal(connects, 1, "concurrent first calls must share one connect");
  assert.ok(clients.every((client) => client === clients[0]));
  assert.equal(await pool.get("b"), clients[0], "a later call reuses the client");
  await pool.get("a");
  assert.deepEqual(pool.tenants(), ["a", "b"]);
  await pool.close();
});

test("pool replaces a client whose credential refresh is failing and closes it", async () => {
  const pool = new TenantSessionPool<PoolClient>(async (tenant) => new PoolClient(tenant));
  const first = await pool.get("t");
  first.failing = new Error("refresh failing");
  const second = await pool.get("t");
  assert.notEqual(second, first);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(first.closes, 1, "the stale client must be closed");
  await pool.close();
});

test("pool does not cache a failed or nil connect", async () => {
  let attempts = 0;
  const pool = new TenantSessionPool<PoolClient>(async (tenant) => {
    attempts += 1;
    if (attempts === 1) throw new Error("dial failed");
    if (attempts === 2) return undefined as unknown as PoolClient;
    return new PoolClient(tenant);
  });
  await assert.rejects(pool.get("t"), /dial failed/);
  await assert.rejects(pool.get("t"), /returned a nil client/);
  assert.ok(await pool.get("t"));
  await pool.close();
});

test("evict fences a pending connect and closes its late client", async () => {
  const hold = gate();
  const late = new PoolClient("t");
  let signal: AbortSignal | undefined;
  let connects = 0;
  const pool = new TenantSessionPool<PoolClient>(async (tenant, abort) => {
    connects += 1;
    if (connects === 1) {
      signal = abort;
      await hold.opened;
      return late;
    }
    return new PoolClient(tenant);
  });
  const waiting = pool.get("t");
  await new Promise((resolve) => setImmediate(resolve));
  await pool.evict("t");
  await assert.rejects(waiting, /was evicted/);
  assert.equal(signal?.aborted, true, "evict must cancel the pending connect");
  const replacement = await pool.get("t");
  assert.notEqual(replacement, late);
  hold.open();
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(late.closes, 1, "a late result of a retired attempt must be closed");
  assert.equal(await pool.get("t"), replacement, "a late result must not replace the new generation");
  await pool.evict("t");
  assert.equal(replacement.closes, 1, "evict closes the live client");
  assert.deepEqual(pool.tenants(), []);
  await pool.close();
});

test("pool close refuses waiters and later gets, closes clients, reports the first failure", async () => {
  const hold = gate();
  const pool = new TenantSessionPool<PoolClient>(async (tenant) => {
    if (tenant === "slow") await hold.opened;
    const client = new PoolClient(tenant);
    if (tenant === "bad") client.close = () => {
      throw new Error("close failed");
    };
    return client;
  });
  const good = await pool.get("good");
  await pool.get("bad");
  const waiting = pool.get("slow");
  await assert.rejects(pool.close(), /close failed/);
  await assert.rejects(waiting, /pool is closed/);
  await assert.rejects(pool.get("good"), /pool is closed/);
  assert.equal(good.closes, 1);
  hold.open();
});

test("first caller's abort cancels the connect; a follower's abort only ends its wait", async () => {
  const signals: AbortSignal[] = [];
  const holds = [gate(), gate()];
  let connects = 0;
  const pool = new TenantSessionPool<PoolClient>(async (tenant, signal) => {
    signals.push(signal);
    await holds[connects++].opened;
    return new PoolClient(tenant);
  });
  const owner = new AbortController();
  const first = pool.get("t", { signal: owner.signal });
  owner.abort(new Error("owner gave up"));
  await assert.rejects(first, /owner gave up/);
  assert.equal(signals[0].aborted, true);

  const followerAbort = new AbortController();
  const second = pool.get("t");
  const follower = pool.get("t", { signal: followerAbort.signal });
  followerAbort.abort(new Error("follower gave up"));
  await assert.rejects(follower, /follower gave up/);
  assert.equal(signals[1].aborted, false, "a follower must not cancel the shared connect");
  holds[1].open();
  assert.ok(await second);
  const already = new AbortController();
  already.abort(new Error("already gone"));
  await assert.rejects(pool.get("t", { signal: already.signal }), /already gone/);
  await pool.close();
});

test("API key pool connects per tenant, owns its key map and refuses unknown or mismatched tenants", async () => {
  reset();
  const keys: Record<string, string> = { [TENANT]: "key-1", [OTHER_TENANT]: "key-2" };
  const pool = newApiKeyTenantPool({ target: fixture.target, purpose: "parity-test", deadlineMs: 5_000 }, keys);
  delete keys[TENANT];
  try {
    const client = await pool.get(TENANT);
    assert.equal(last("Authenticate").request.api_key, "key-1", "the pool must keep its own copy of the keys");
    assert.equal(await pool.get(TENANT), client);
    assert.equal(fixture.seen.filter((call) => call.method === "Authenticate").length, 1);
    await assert.rejects(pool.get("unknown"), /no API key for tenant "unknown"/);
    await assert.rejects(pool.get(OTHER_TENANT), /tenant is "11111111/, "a key for another tenant must be refused");
    assert.deepEqual(pool.tenants(), [TENANT]);
  } finally {
    await pool.close();
  }
});

// ── 3. Environment bootstrap and health ─────────────────────────────────────

test("configFromEnv reports every problem at once", () => {
  assert.throws(
    () => configFromEnv("APP_", { APP_TARGET: "nohost", APP_AUTH_TARGET: "nope", APP_DEADLINE: "soon" }),
    (err: Error) => {
      for (const part of [
        "APP_TENANT_ID is not set",
        "APP_PURPOSE is not set",
        "APP_API_KEY (or APP_BEARER) is not set",
        'APP_TARGET must be host:port, got "nohost"',
        'APP_AUTH_TARGET must be host:port, got "nope"',
        'APP_DEADLINE must be a positive duration such as 10s, got "soon"',
      ]) {
        assert.ok(err.message.includes(part), `missing "${part}" in: ${err.message}`);
      }
      return true;
    },
  );
  assert.throws(() => configFromEnv("", {}), /UDB_TARGET is not set/, "the default prefix is UDB_");
});

test("configFromEnv maps the contract", () => {
  const config = configFromEnv("", {
    UDB_TARGET: " broker:50051 ",
    UDB_AUTH_TARGET: "broker:50061",
    UDB_WEBRTC_TARGET: "broker:50071",
    UDB_TENANT_ID: TENANT,
    UDB_PROJECT: PROJECT,
    UDB_PURPOSE: "billing",
    UDB_API_KEY: "key",
    UDB_SCOPES: "a,b c\ta",
    UDB_SERVICE_IDENTITY: "svc",
    UDB_DEADLINE: "1m30s",
    UDB_TLS: "TRUE",
    UDB_TLS_SERVER_NAME: "broker.internal",
  });
  assert.equal(config.target, "broker:50051");
  assert.equal(config.authTarget, "broker:50061");
  assert.equal(config.webrtcTarget, "broker:50071");
  assert.equal(config.projectId, PROJECT, "UDB_PROJECT is accepted too");
  assert.deepEqual(config.scopes, ["a", "b", "c"], "scopes split on comma or space and dedupe");
  assert.equal(config.serviceIdentity, "svc");
  assert.equal(config.deadlineMs, 90_000);
  assert.equal(config.secure, true);
  assert.equal(config.channelOptions?.["grpc.ssl_target_name_override"], "broker.internal");
  assert.deepEqual(config.credentials, { apiKey: "key" });
  const plain = configFromEnv("", { UDB_TARGET: "b:1", UDB_TENANT_ID: "t", UDB_PURPOSE: "p", UDB_BEARER: "tok", UDB_TLS: "no" });
  assert.equal(plain.secure, undefined);
  assert.deepEqual(plain.credentials, { bearerToken: "tok" });
});

test("configFromEnv loads the TLS files and refuses bad ones", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "udb-tls-"));
  try {
    // Structurally PEM; the loader checks the blocks, grpc-js parses them on dial.
    const pemBlock = (label: string) => `-----BEGIN ${label}-----\nAAAA\n-----END ${label}-----\n`;
    const cert = path.join(dir, "client.pem");
    const key = path.join(dir, "client.key");
    const junk = path.join(dir, "junk.pem");
    const missing = path.join(dir, "absent.pem");
    fs.writeFileSync(cert, pemBlock("CERTIFICATE"));
    fs.writeFileSync(key, pemBlock("PRIVATE KEY"));
    fs.writeFileSync(junk, "not a certificate");
    const base = { UDB_TARGET: "b:1", UDB_TENANT_ID: "t", UDB_PURPOSE: "p", UDB_BEARER: "tok" };

    const full = configFromEnv("", { ...base, UDB_TLS_CA_FILE: cert, UDB_TLS_CERT_FILE: cert, UDB_TLS_KEY_FILE: key });
    assert.equal(full.secure, true, "a TLS file variable turns TLS on");
    assert.equal(full.tls?.rootCerts?.toString(), pemBlock("CERTIFICATE"));
    assert.equal(full.tls?.certChain?.toString(), pemBlock("CERTIFICATE"));
    assert.equal(full.tls?.privateKey?.toString(), pemBlock("PRIVATE KEY"));

    const caOnly = configFromEnv("", { ...base, UDB_TLS_CA_FILE: cert });
    assert.equal(caOnly.secure, true);
    assert.ok(caOnly.tls?.rootCerts);
    assert.equal(caOnly.tls?.certChain, undefined);
    assert.equal(configFromEnv("", base).tls, undefined);

    const refuses = (env: Record<string, string>, ...names: string[]) => {
      assert.throws(
        () => configFromEnv("", { ...base, ...env }),
        (err: Error) => names.every((name) => err.message.includes(name)),
        `expected an error naming ${names.join(", ")}`,
      );
    };
    refuses({ UDB_TLS_CA_FILE: missing }, "UDB_TLS_CA_FILE", missing);
    refuses({ UDB_TLS_CA_FILE: junk }, "UDB_TLS_CA_FILE", junk);
    refuses({ UDB_TLS_CERT_FILE: cert }, "UDB_TLS_CERT_FILE", "UDB_TLS_KEY_FILE");
    refuses({ UDB_TLS_KEY_FILE: key }, "UDB_TLS_CERT_FILE", "UDB_TLS_KEY_FILE");
    refuses({ UDB_TLS_CERT_FILE: cert, UDB_TLS_KEY_FILE: missing }, "UDB_TLS_KEY_FILE", missing);
    refuses({ UDB_TLS_CERT_FILE: cert, UDB_TLS_KEY_FILE: cert }, "UDB_TLS_KEY_FILE", cert);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("parseDurationMs follows Go durations", () => {
  assert.equal(parseDurationMs("10s"), 10_000);
  assert.equal(parseDurationMs("250ms"), 250);
  assert.equal(parseDurationMs("1h1m"), 3_660_000);
  assert.equal(parseDurationMs("1.5s"), 1_500);
  assert.equal(parseDurationMs("-1s"), -1_000);
  assert.equal(parseDurationMs("0"), 0);
  for (const bad of ["", "10", "s", "10 s", "1d", "soon"]) {
    assert.equal(parseDurationMs(bad), undefined, `parsing ${JSON.stringify(bad)}`);
  }
});

test("verifyPrincipal reports every mismatch and needs a principal only when expecting", () => {
  verifyPrincipal(null, {});
  verifyPrincipal(null);
  assert.throws(() => verifyPrincipal(null, { tenantId: TENANT }), /no verified principal/);
  const principal = principalFixture();
  verifyPrincipal(principal, {
    tenantId: TENANT,
    projectId: PROJECT,
    serviceIdentity: "spiffe://fixture/service",
    requiredScopes: ["UDB:READ"],
  });
  assert.throws(
    () =>
      verifyPrincipal(principal, {
        tenantId: OTHER_TENANT,
        projectId: "p",
        serviceIdentity: "s",
        requiredScopes: ["udb:read", "udb:write", "udb:admin"],
      }),
    (err: Error) => {
      for (const part of [`tenant is "${TENANT}", expected "${OTHER_TENANT}"`, `project is "${PROJECT}"`, "service identity is", "lacks scopes udb:write, udb:admin"]) {
        assert.ok(err.message.includes(part), `missing "${part}" in: ${err.message}`);
      }
      return true;
    },
  );
});

test("connectFromEnv connects and verifies; a mismatch is refused", async () => {
  reset();
  const prefix = "UDB_PARITY_UNIT_";
  const env = {
    TARGET: fixture.target,
    TENANT_ID: TENANT,
    PROJECT_ID: PROJECT,
    PURPOSE: "parity",
    API_KEY: "key-1",
    DEADLINE: "5s",
  };
  for (const [name, value] of Object.entries(env)) process.env[prefix + name] = value;
  try {
    for (let i = 0; i < 2; i += 1) {
      const udb = await UdbProject.connectFromEnv(prefix, { tenantId: TENANT, requiredScopes: ["udb:read"] });
      assert.equal(udb.bearer(), `Bearer ${SERVICE_BEARER}`);
      udb.close();
    }
    await assert.rejects(UdbProject.connectFromEnv(prefix, { requiredScopes: ["udb:admin"] }), /lacks scopes udb:admin/);
    await assert.rejects(UdbProject.connectFromEnv("UDB_PARITY_ABSENT_"), /environment is incomplete/);
    process.env[`${prefix}BEARER`] = "tok";
    await assert.rejects(UdbProject.connectFromEnv(prefix), /mutually exclusive/);
  } finally {
    for (const name of [...Object.keys(env), "BEARER"]) delete process.env[prefix + name];
  }
});

test("ready reports the broker's status and the connection's credential", async () => {
  reset();
  const udb = project();
  try {
    for (let i = 0; i < 3; i += 1) await udb.ready();
    assert.equal(fixture.seen.filter((call) => call.method === "Check").length, 3);
    fixture.health.push(2);
    await assert.rejects(udb.ready(), /udb: broker is NOT_SERVING/);
    fixture.health.length = 0;
    fixture.health.push(3);
    await assert.rejects(udb.ready(), /udb: broker is SERVICE_UNKNOWN/);
    fixture.health.length = 0;
    (udb as any).lastRefreshError = new Error("refresh refused");
    await assert.rejects(udb.ready(), /credential refresh is failing: refresh refused/);
    (udb as any).lastRefreshError = null;
    await udb.ready();
  } finally {
    udb.close();
  }
  await assert.rejects(udb.ready(), /credential owner is closed/);
  const dead = new UdbProject({ target: "127.0.0.1:1", tenantId: TENANT, purpose: "p" });
  try {
    await assert.rejects(dead.ready({ deadlineMs: 2_000 }), /broker health check failed/);
  } finally {
    dead.close();
  }
});

// ── 4. Per-request user ─────────────────────────────────────────────────────

test("asUser sends the user's bearer alone, keeps tenant and audit, blanks service hints", async () => {
  reset();
  const udb = await UdbProject.connect({
    target: fixture.target,
    tenantId: TENANT,
    projectId: PROJECT,
    purpose: "service-purpose",
    deadlineMs: 5_000,
    credentials: { apiKey: "key-1" },
  });
  try {
    await udb.flags.enabled("on");
    const service = last("EvaluateFlags").headers;
    assert.equal(header(service, "authorization"), `Bearer ${SERVICE_BEARER}`);
    assert.equal(header(service, "x-service-identity"), "spiffe://fixture/service");
    assert.equal(header(service, "x-scopes"), "udb:read,svc:scope");

    for (const token of [USER_BEARER, `Bearer ${USER_BEARER}`, `  Bearer ${USER_BEARER}  `]) {
      await udb.flags.enabled(
        "on",
        {},
        udb.asUser(token, {
          meta: { purpose: "delegated-purpose", correlationId: "delegated-correlation", apiKey: "raw" },
          headers: { "x-api-key": "raw", "X-UDB-API-KEY": "raw", "x-custom": "kept" },
        }),
      );
      const delegated = last("EvaluateFlags").headers;
      assert.deepEqual(delegated.get("authorization"), [`Bearer ${USER_BEARER}`], "exactly one credential");
      assert.equal(header(delegated, "x-tenant-id"), TENANT, "the connected tenant does not change");
      assert.equal(header(delegated, "x-udb-project-id"), PROJECT, "the connected project does not change");
      assert.equal(header(delegated, "x-purpose"), "delegated-purpose");
      assert.equal(header(delegated, "x-correlation-id"), "delegated-correlation");
      assert.equal(header(delegated, "x-custom"), "kept");
      assert.equal(header(delegated, "x-api-key"), undefined, "a raw key must not ride along");
      assert.equal(header(delegated, "x-udb-api-key"), undefined);
      for (const hint of ["x-user-id", "x-service-identity", "x-scopes"]) {
        assert.equal(header(delegated, hint), "", `${hint} must not keep the service's value`);
      }
    }
    await udb.flags.enabled("on");
    assert.equal(
      header(last("EvaluateFlags").headers, "authorization"),
      `Bearer ${SERVICE_BEARER}`,
      "delegation applies only to the call it was passed to",
    );
    (udb as any).poisoned = true;
    (udb as any).lastRefreshError = new Error("revoked");
    assert.throws(() => udb.asUser(USER_BEARER), /session not authorized: revoked/);
  } finally {
    udb.close();
  }
});

test("asUserCall does not change the options it was given", () => {
  const call = { meta: { apiKey: "raw" }, headers: { "x-api-key": "raw" } };
  const out = asUserCall(USER_BEARER, call);
  assert.deepEqual(call, { meta: { apiKey: "raw" }, headers: { "x-api-key": "raw" } });
  assert.equal(out.headers?.authorization, `Bearer ${USER_BEARER}`);
  assert.deepEqual(asUserCall(USER_BEARER).meta, {});
});

test("validateSession maps the reply and refuses an invalid token", async () => {
  reset();
  const udb = project();
  try {
    const session = await udb.validateSession(`Bearer ${USER_BEARER}`);
    const sent = last("ValidateToken").request;
    assert.equal(sent.token, USER_BEARER, "the prefix is stripped");
    assert.equal(sent.token_type, "TOKEN_TYPE_JWT_ACCESS");
    assert.equal(session.publicId, "sesspub_1");
    assert.equal(session.userId, "user-1");
    assert.equal(session.tenantId, TENANT);
    assert.deepEqual(session.roles, ["admin"]);
    assert.deepEqual(session.scopes, ["udb:read"]);
    assert.equal(session.expiresAt?.getTime(), 1_800_000_000_500);
    assert.equal(session.principal.user_id, "user-1");

    const original = fixture.validate;
    fixture.validate = () => ({ valid: false });
    await assert.rejects(udb.validateSession("bad"), NotFoundError, "an invalid token is NotFoundError");
    fixture.validate = () => {
      throw Object.assign(new Error("nope"), { code: grpc.status.UNAUTHENTICATED, details: "nope" });
    };
    await assert.rejects(udb.validateSession("bad"), (err: any) => err.code === grpc.status.UNAUTHENTICATED);
    fixture.validate = (_request, headers) => {
      assert.equal(header(headers, "authorization"), `Bearer ${USER_BEARER}`);
      return original(_request, headers);
    };
    await udb.validateSession(USER_BEARER, udb.asUser(USER_BEARER));
    fixture.validate = original;
  } finally {
    udb.close();
  }
});

// ── 5. Live query ───────────────────────────────────────────────────────────

const LOCK = "udb.core.lock.entity.v1.Lock";

function collectUntil(events: LiveQueryEvent<any>[], count: number, abort: AbortController) {
  return (event: LiveQueryEvent<any>) => {
    events.push(event);
    if (events.length >= count) abort.abort(new Error("done"));
  };
}

function errorDetailTrailer(capability: string, retryAfterMs = 0): grpc.Metadata {
  const capabilityBytes = Buffer.from(capability);
  const parts: Buffer[] = [];
  if (capability) parts.push(Buffer.from([0x1a, capabilityBytes.length]), capabilityBytes);
  if (retryAfterMs) {
    const varint: number[] = [];
    let rest = retryAfterMs;
    while (rest > 0x7f) {
      varint.push((rest & 0x7f) | 0x80);
      rest >>>= 7;
    }
    varint.push(rest);
    parts.push(Buffer.from([0x28, ...varint]));
  }
  const md = new grpc.Metadata();
  md.set("udb-error-detail-bin", Buffer.concat(parts));
  return md;
}

test("predicate helpers build the wire predicates", () => {
  assert.deepEqual(lqEq("a", "1"), { field: "a", op: "LIVE_QUERY_COMPARISON_EQ", value: "1" });
  assert.equal(lqNe("a", "1").op, "LIVE_QUERY_COMPARISON_NE");
  assert.equal(lqLt("a", "1").op, "LIVE_QUERY_COMPARISON_LT");
  assert.equal(lqLe("a", "1").op, "LIVE_QUERY_COMPARISON_LE");
  assert.equal(lqGt("a", "1").op, "LIVE_QUERY_COMPARISON_GT");
  assert.equal(lqGe("a", "1").op, "LIVE_QUERY_COMPARISON_GE");
  assert.deepEqual(lqIn("a", "1", "2"), { field: "a", op: "LIVE_QUERY_COMPARISON_IN", value: "", values: ["1", "2"] });
  assert.equal(lqNotIn("a", "1").op, "LIVE_QUERY_COMPARISON_NOT_IN");
  assert.deepEqual(lqIsNull("a"), { field: "a", op: "LIVE_QUERY_COMPARISON_IS_NULL", value: "" });
  assert.equal(lqIsNotNull("a").op, "LIVE_QUERY_COMPARISON_IS_NOT_NULL");
});

test("liveQuery delivers snapshot and changes and absorbs keepalives", async () => {
  reset();
  fixture.subscribe.push((call) => {
    call.write({ snapshot: { rows_json: ['{"id":"1"}', '{"id":"2"}'], row_count: "2" } });
    call.write({ heartbeat: {} });
    call.write({ change: { op: "LIVE_QUERY_CHANGE_OP_UNSPECIFIED", event_id: "", row_json: "" } });
    call.write({ change: { op: "LIVE_QUERY_CHANGE_OP_INSERT", row_json: '{"id":"3"}', event_id: "e3" } });
    call.write({ change: { op: "LIVE_QUERY_CHANGE_OP_DELETE", row_json: '{"id":"1"}', event_id: "e4" } });
  });
  const udb = project();
  const abort = new AbortController();
  const events: LiveQueryEvent<any>[] = [];
  try {
    await assert.rejects(
      liveQuery(
        udb,
        LOCK,
        { where: [lqEq("status", "HELD")], anyOf: [[lqEq("owner", "me"), lqEq("assignee", "me")]], snapshotLimit: 7, signal: abort.signal },
        collectUntil(events, 3, abort),
      ),
      /done/,
    );
    const sent = last("Subscribe");
    assert.equal(sent.request.tenant_id, TENANT);
    assert.equal(sent.request.project_id, PROJECT);
    assert.equal(sent.request.message_type, LOCK);
    assert.equal(sent.request.snapshot_limit, 7);
    assert.equal(sent.request.filters[0].op, "LIVE_QUERY_COMPARISON_EQ");
    assert.equal(sent.request.any_of[0].predicates.length, 2);
    assert.equal(sent.request.since_event_id, "");
    assert.equal(header(sent.headers, "x-udb-livequery-resume"), undefined);
    assert.equal(events[0].isSnapshot, true);
    assert.equal(events[0].resumed, false);
    assert.deepEqual(events[0].snapshot, [{ id: "1" }, { id: "2" }]);
    assert.equal(events[1].isSnapshot, false);
    assert.equal(events[1].op, "LIVE_QUERY_CHANGE_OP_INSERT");
    assert.deepEqual(events[1].row, { id: "3" });
    assert.equal(events[1].eventId, "e3");
    assert.equal(events[2].op, "LIVE_QUERY_CHANGE_OP_DELETE");
    assert.equal(events.length, 3, "keepalives must not reach the handler");
  } finally {
    udb.close();
  }
});

test("liveQuery reconnects after a transient failure and resumes after the last change", async () => {
  reset();
  fixture.subscribe.push((call) => {
    call.write({ snapshot: { rows_json: [] } });
    call.write({ change: { op: "LIVE_QUERY_CHANGE_OP_UPDATE", row_json: "{}", event_id: "e7" } });
    setImmediate(() => call.emit("error", { code: grpc.status.UNAVAILABLE, details: "blip" }));
  });
  fixture.subscribe.push((call) => {
    call.write({ snapshot: { rows_json: ['{"id":"r"}'] } });
  });
  const udb = project();
  const abort = new AbortController();
  const events: LiveQueryEvent<any>[] = [];
  const reconnects: Error[] = [];
  try {
    await assert.rejects(
      liveQuery(udb, LOCK, { backoffMs: 5, signal: abort.signal, onError: (err) => reconnects.push(err) }, collectUntil(events, 3, abort)),
      /done/,
    );
    assert.equal(reconnects.length, 1);
    assert.match(reconnects[0].message, new RegExp(`live query ${LOCK} reconnecting`));
    const resume = last("Subscribe");
    assert.equal(resume.request.since_event_id, "e7");
    assert.equal(header(resume.headers, "x-udb-livequery-resume"), "e7", "both resume carriers name one event");
    assert.equal(events[2].isSnapshot, true);
    assert.equal(events[2].resumed, true, "the snapshot after a reconnect is marked");
  } finally {
    udb.close();
  }
});

test("liveQuery honors the broker's retry-after and an explicit resume point", async () => {
  reset();
  fixture.subscribe.push((call) => {
    call.emit("error", { code: grpc.status.RESOURCE_EXHAUSTED, details: "busy", metadata: errorDetailTrailer("", 150) });
  });
  fixture.subscribe.push((call) => {
    call.write({ snapshot: { rows_json: [] } });
  });
  const udb = project();
  const abort = new AbortController();
  const events: LiveQueryEvent<any>[] = [];
  const started = Date.now();
  try {
    await assert.rejects(
      liveQuery(udb, LOCK, { backoffMs: 1, sinceEventId: "earlier", signal: abort.signal }, collectUntil(events, 1, abort)),
      /done/,
    );
    assert.ok(Date.now() - started >= 140, "the reconnect must wait the broker's retry-after");
    for (const call of fixture.seen.filter((entry) => entry.method === "Subscribe")) {
      assert.equal(call.request.since_event_id, "earlier");
    }
  } finally {
    udb.close();
  }
});

test("liveQuery stops on refusals reconnecting cannot fix", async () => {
  const udb = project();
  try {
    reset();
    fixture.subscribe.push((call) => {
      call.write({ snapshot: { rows_json: ['{"id":"1"}'] } });
      setImmediate(() =>
        call.emit("error", {
          code: grpc.status.FAILED_PRECONDITION,
          details: "no feed",
          metadata: errorDetailTrailer("cdc_change_feed"),
        }),
      );
    });
    const delivered: LiveQueryEvent<any>[] = [];
    await assert.rejects(
      liveQuery(udb, LOCK, {}, (event) => {
        delivered.push(event);
      }),
      NoDeltaFeedError,
    );
    assert.equal(delivered.length, 1, "the snapshot is delivered first");
    for (const code of [
      grpc.status.FAILED_PRECONDITION,
      grpc.status.INVALID_ARGUMENT,
      grpc.status.PERMISSION_DENIED,
      grpc.status.NOT_FOUND,
      grpc.status.UNIMPLEMENTED,
    ]) {
      reset();
      fixture.subscribe.push((call) => call.emit("error", { code, details: "refused" }));
      await assert.rejects(
        liveQuery(udb, LOCK, {}, () => undefined),
        (err: any) => err instanceof UdbError && err.code === code && !(err instanceof NoDeltaFeedError),
      );
      assert.equal(fixture.seen.filter((call) => call.method === "Subscribe").length, 1, `code ${code} must not reconnect`);
    }
  } finally {
    udb.close();
  }
});

test("liveQuery ends on a handler failure or an undecodable row", async () => {
  const udb = project();
  try {
    reset();
    fixture.subscribe.push((call) => call.write({ snapshot: { rows_json: [] } }));
    await assert.rejects(
      liveQuery(udb, LOCK, {}, () => {
        throw new Error("handler refused");
      }),
      /handler refused/,
    );
    for (const raw of ["not json", "[1]", "5"]) {
      fixture.subscribe.push((call) => call.write({ snapshot: { rows_json: [raw] } }));
      await assert.rejects(
        liveQuery(udb, LOCK, {}, () => undefined),
        /row is not a JSON object/,
      );
    }
    fixture.subscribe.push((call) => call.write({ snapshot: { rows_json: ['{"id":"1"}'] } }));
    await assert.rejects(
      liveQuery(udb, LOCK, { decode: () => { throw new Error("decode refused"); } }, () => undefined),
      /decode refused/,
    );
    const abort = new AbortController();
    abort.abort(new Error("never started"));
    await assert.rejects(liveQuery(udb, LOCK, { signal: abort.signal }, () => undefined), /never started/);
  } finally {
    udb.close();
  }
});

test("liveQuery decodes rows with the supplied decoder", async () => {
  reset();
  fixture.subscribe.push((call) => call.write({ snapshot: { rows_json: ['{"id":"1"}'] } }));
  const udb = project();
  const abort = new AbortController();
  const events: LiveQueryEvent<{ upper: string }>[] = [];
  try {
    await assert.rejects(
      liveQuery<{ upper: string }>(
        udb,
        LOCK,
        { signal: abort.signal, decode: (record) => ({ upper: String(record.id) + "!" }) },
        collectUntil(events as LiveQueryEvent<any>[], 1, abort),
      ),
      /done/,
    );
    assert.deepEqual(events[0].snapshot, [{ upper: "1!" }]);
  } finally {
    udb.close();
  }
});

// ── 6. Graph and vector builders ────────────────────────────────────────────

test("graphFrom builds the typed traversal and returns plain rows", async () => {
  reset();
  const udb = project();
  try {
    const builder = graphFrom(udb, LOCK, "n1")
      .out("RELATED", "KNOWS")
      .depth(1, 2)
      .labels("Note")
      .where("archived", "false")
      .whereEdge("kind", "peer")
      .limit(5)
      .withEdges();
    for (let i = 0; i < 2; i += 1) {
      const rows = await builder.run();
      assert.deepEqual(rows, [{ id: "n2", hops: 2 }]);
    }
    const sent = last("GraphQuery").request;
    assert.equal(sent.resource.backend, "neo4j");
    assert.equal(sent.resource.message_type, LOCK);
    assert.equal(sent.read_only, true);
    assert.equal(sent.limit, 5);
    assert.equal(sent.traversal.start_label, "locks", "the label falls back to the table name");
    assert.equal(sent.traversal.start_id, "n1");
    assert.equal(sent.traversal.direction, "GRAPH_TRAVERSAL_DIRECTION_OUTGOING");
    assert.deepEqual(sent.traversal.relationship_types, ["RELATED", "KNOWS"]);
    assert.equal(sent.traversal.min_depth, 1);
    assert.equal(sent.traversal.max_depth, 2);
    assert.deepEqual(sent.traversal.node_labels, ["Note"]);
    assert.deepEqual({ ...sent.traversal.node_property_equals }, { archived: "false" });
    assert.deepEqual({ ...sent.traversal.relationship_property_equals }, { kind: "peer" });
    assert.equal(sent.traversal.return_relationships, true);
    await graphFrom(udb, LOCK, "n1", { label: "Custom" }).in("A").run();
    const incoming = last("GraphQuery").request.traversal;
    assert.equal(incoming.start_label, "Custom");
    assert.equal(incoming.direction, "GRAPH_TRAVERSAL_DIRECTION_INCOMING");
    await graphFrom(udb, LOCK, "n1").both().run();
    assert.equal(last("GraphQuery").request.traversal.direction, "GRAPH_TRAVERSAL_DIRECTION_BOTH");
    const before = fixture.seen.length;
    await assert.rejects(graphFrom(udb, LOCK, "").run(), /needs a start node id/);
    assert.equal(fixture.seen.length, before, "a traversal without a start must not reach the broker");
    assert.throws(() => graphFrom(udb, "no.such.Message", "n1"), /no\.such\.Message/);
  } finally {
    udb.close();
  }
});

test("vectorsHybrid defaults, builds the request and decodes hits", async () => {
  reset();
  const udb = project();
  try {
    const hits = await vectorsHybrid(udb, "notes")
      .text("rotate keys")
      .near([1, 2])
      .weights(0.75, 0.25)
      .filter({ owner: "me" })
      .limit(3)
      .run();
    assert.deepEqual(hits, [{ id: "p1", score: 0.5, row: { title: "rotate" }, payload: { title: "rotate" } }]);
    const sent = last("VectorHybridSearch").request;
    assert.equal(sent.collection, "notes");
    assert.equal(sent.text_query, "rotate keys");
    assert.deepEqual(sent.vector, [1, 2]);
    assert.deepEqual(sent.fusion_weights, [0.75, 0.25]);
    assert.equal(sent.limit, 3);
    assert.equal(sent.with_payload, true);
    assert.equal(sent.filter.fields.owner.stringValue ?? sent.filter.fields.owner.string_value, "me");
    assert.equal((vectorsHybrid(udb, "notes").request() as any).limit, 10, "the default limit is 10");
    assert.equal((vectorsHybrid(udb, "notes").request() as any).with_payload, true);

    const decoded = await vectorsHybrid<{ t: string }>(udb, "notes", {
      decode: (payload) => ({ t: String(payload.title) }),
    })
      .text("x")
      .run();
    assert.deepEqual(decoded[0].row, { t: "rotate" });
    await assert.rejects(
      vectorsHybrid(udb, "notes", {
        decode: () => {
          throw new Error("bad row");
        },
      })
        .text("x")
        .run(),
      /vector hit p1: bad row/,
    );
    const before = fixture.seen.length;
    await assert.rejects(vectorsHybrid(udb, "notes").run(), /needs near\(vector\) and\/or text\(query\)/);
    assert.equal(fixture.seen.length, before, "an empty search must not reach the broker");
  } finally {
    udb.close();
  }
});

// ── 7. Flags and locks ──────────────────────────────────────────────────────

test("flags evaluate for the connected scope; absent and non-boolean flags are off", async () => {
  reset();
  const udb = project();
  try {
    for (let i = 0; i < 2; i += 1) {
      assert.equal(await udb.flags.enabled("on", { user_id: "u1" }), true);
    }
    const sent = last("EvaluateFlags").request;
    assert.equal(sent.tenant_id, TENANT);
    assert.deepEqual(sent.keys, ["on"]);
    assert.equal(sent.context.project_id, PROJECT);
    assert.deepEqual({ ...sent.context.attributes }, { user_id: "u1" });
    assert.equal(await udb.flags.enabled("off"), false);
    assert.equal(await udb.flags.enabled("missing"), false, "a flag that does not exist is off");
    assert.equal(await udb.flags.enabled("text"), false, "a string flag is not on");
    const values = await udb.flags.evaluate(["on", "missing"]);
    assert.deepEqual(Object.keys(values), ["on"], "unknown keys are absent");
    assert.equal(values.on.bool_value, true);
  } finally {
    udb.close();
  }
});

test("lock defaults the tenant and never overrides an explicit one", async () => {
  reset();
  const udb = project();
  try {
    const request = { lock_name: "l", owner_id: "o", lease_ttl_seconds: 30 };
    await udb.lock.acquire(request);
    assert.equal(last("AcquireLock").request.tenant_id, TENANT);
    assert.equal("tenant_id" in request, false, "the caller's request is not changed");
    await udb.lock.acquire({ ...request, tenant_id: OTHER_TENANT });
    assert.equal(last("AcquireLock").request.tenant_id, OTHER_TENANT);
    await udb.lock.list();
    assert.equal(last("ListLocks").request.tenant_id, TENANT);
  } finally {
    udb.close();
  }
});

// ── 8. Schema reflection ────────────────────────────────────────────────────

const APIKEY = "udb.core.apikey.entity.v1.ApiKey";
const APIKEY_CREATED = "udb.core.apikey.events.v1.ApiKeyCreatedEvent";

test("schema reads the annotations of the shipped protos", () => {
  for (let i = 0; i < 2; i += 1) {
    assert.equal(topic(APIKEY_CREATED), "udb.apikey.created.v1");
    assert.equal(eventType(APIKEY_CREATED), "udb.core.apikey.events.v1.ApiKeyCreatedEvent");
    assert.equal(partitionKeyField(APIKEY_CREATED), "tenant_id");
  }
  assert.equal(topic(LOCK), "", "a message with no contract has no topic");
  assert.equal(eventType(LOCK), "");
  assert.deepEqual(primaryKeys(LOCK), ["lock_id"]);
  assert.equal(primaryKey(LOCK), "lock_id");
  assert.deepEqual(uniqueKeys(LOCK), [["tenant_id", "lock_name"]]);
  assert.equal(tenantColumn(LOCK), "tenant_id");
  assert.equal(projectColumn(LOCK), "");
  assert.equal(maxLen(LOCK, "lock_name"), 255);
  assert.equal(maxLen(LOCK, "lock_id"), 0, "a UUID column has no VARCHAR bound");
  assert.equal(maxLen(LOCK, "no_such_field"), 0);
  assert.equal(softDelete(APIKEY), true);
  assert.equal(softDelete(LOCK), false);
  assert.equal(retentionClass(LOCK), "lock.operational");
  assert.equal(exportEligible(LOCK), false);
  assert.deepEqual(ownerColumns(LOCK), []);
  assert.equal(ownerColumn(LOCK), "");
  assert.equal(graphLabel(LOCK), "locks");
  assert.throws(() => topic("no.such.Message"), /no\.such\.Message/);
  assert.throws(() => uniqueKeys("no.such.Message"), /no\.such\.Message/);
});

/** A proto tree of one message type per test, importing the SDK's annotations. */
function schemaFixture(tableOptions: string, fields: string): { protoRoot: string; messageType: string } {
  const protoRoot = fs.mkdtempSync(path.join(os.tmpdir(), "udb-schema-"));
  const pkg = `fixture${path.basename(protoRoot).replace(/[^a-zA-Z0-9]/g, "")}.v1`;
  fs.writeFileSync(
    path.join(protoRoot, "fixture.proto"),
    `syntax = "proto3";
package ${pkg};
import "udb/core/common/v1/db.proto";
import "udb/core/common/v1/security.proto";
message Row {
  option (udb.core.common.v1.table) = { table_name: "rows" is_table: true ${tableOptions} };
  ${fields}
}
`,
  );
  return { protoRoot, messageType: `${pkg}.Row` };
}

const COLUMN = (extra: string) => `[(udb.core.common.v1.column) = { ${extra} }]`;
const ROW_ID = `string row_id = 1 ${COLUMN('sql_type: "UUID" primary_key: true')};`;

test("primaryKey is empty for a composite or absent key", () => {
  const composite = schemaFixture(
    "",
    `string a = 1 ${COLUMN('sql_type: "UUID" primary_key: true')};
    string b = 2 ${COLUMN('sql_type: "UUID" primary_key: true')};`,
  );
  assert.deepEqual(primaryKeys(composite.messageType, composite), ["a", "b"]);
  assert.equal(primaryKey(composite.messageType, composite), "", "a composite key is not one key");
  const none = schemaFixture("", `string a = 1 ${COLUMN('sql_type: "UUID"')};`);
  assert.deepEqual(primaryKeys(none.messageType, none), []);
  assert.equal(primaryKey(none.messageType, none), "");
});

test("uniqueKeys follows the declared constraints", () => {
  const plain = schemaFixture(
    `
    indexes: { index_name: "z_composite" unique: true columns: "owner_id" columns: "slug" }
    indexes: { index_name: "a_partial" unique: true columns: "email" where_clause: "deleted_at IS NULL" }
    indexes: { index_name: "b_expression" unique: true columns: "lower(email)" }
    indexes: { index_name: "c_not_unique" columns: "slug" }
    indexes: { index_name: "d_duplicate" unique: true columns: "slug,owner_id" }
    indexes: { index_name: "e_primary" unique: true columns: "row_id" }`,
    `${ROW_ID}
    string owner_id = 2 ${COLUMN('sql_type: "UUID"')};
    string slug = 3 ${COLUMN('sql_type: "VARCHAR(64)"')};
    string email = 4 ${COLUMN('sql_type: "TEXT" unique: true')};
    string code = 5 ${COLUMN('sql_type: "TEXT" index: { index_name: "f_code" unique: true columns: "owner_id" }')};`,
  );
  assert.deepEqual(uniqueKeys(plain.messageType, plain), [["code", "owner_id"], ["email"], ["owner_id", "slug"]]);
});

test("uniqueKeys adds the partition column and matches aliases by the parser's naming", () => {
  const partitioned = schemaFixture(
    `partition_strategy: PARTITION_STRATEGY_RANGE_MONTH partition_column: "created_on"
     indexes: { index_name: "a" unique: true columns: "SlugName" }`,
    `${ROW_ID}
    string slug_name = 2 ${COLUMN('sql_type: "TEXT"')};
    string created_on = 3 ${COLUMN('sql_type: "DATE"')};`,
  );
  assert.deepEqual(uniqueKeys(partitioned.messageType, partitioned), [["slug_name", "created_on"]]);
});

test("uniqueKeys refuses unknown, ambiguous and repeated columns", () => {
  const unknown = schemaFixture(`indexes: { index_name: "a" unique: true columns: "missing" }`, ROW_ID);
  assert.throws(() => uniqueKeys(unknown.messageType, unknown), /column "missing" resolves to 0 fields/);
  const ambiguous = schemaFixture(
    `indexes: { index_name: "a" unique: true columns: "slug" }`,
    `${ROW_ID}
    string slug = 2 ${COLUMN('sql_type: "TEXT"')};
    string other = 3 ${COLUMN('column_name: "slug" sql_type: "TEXT"')};`,
  );
  assert.throws(() => uniqueKeys(ambiguous.messageType, ambiguous), /column "slug" resolves to 2 fields/);
  const repeated = schemaFixture(`indexes: { index_name: "a" unique: true columns: "slug" columns: "slug" }`, `${ROW_ID}
    string slug = 2 ${COLUMN('sql_type: "TEXT"')};`);
  assert.throws(() => uniqueKeys(repeated.messageType, repeated), /repeats field "slug"/);
});

test("applyColumnDefaults sets bool and integer defaults and refuses a bad one", () => {
  const good = schemaFixture(
    "",
    `${ROW_ID}
    bool active = 2 ${COLUMN('sql_type: "BOOLEAN" default_value: "true"')};
    bool hidden = 3 ${COLUMN('sql_type: "BOOLEAN" default_value: "F"')};
    int32 tries = 4 ${COLUMN('sql_type: "INTEGER" default_value: " 3 "')};
    int64 big = 5 ${COLUMN('sql_type: "BIGINT" default_value: "9007199254740993"')};
    sint64 small = 6 ${COLUMN('sql_type: "BIGINT" default_value: "-5"')};
    string label = 7 ${COLUMN('sql_type: "TEXT" default_value: "x"')};
    repeated int32 many = 8 ${COLUMN('sql_type: "INTEGER[]" default_value: "1"')};
    int32 plain = 9 ${COLUMN('sql_type: "INTEGER"')};`,
  );
  const row: Record<string, unknown> = { active: false, plain: 9 };
  assert.equal(applyColumnDefaults(good.messageType, row, good), row);
  assert.deepEqual(row, {
    active: true,
    hidden: false,
    tries: 3,
    big: "9007199254740993",
    small: -5,
    plain: 9,
  });
  for (const [kind, value] of [
    ["bool", "yes"],
    ["int32", "abc"],
    ["int32", "2147483648"],
    ["int64", "9223372036854775808"],
    ["sfixed32", "1.5"],
  ]) {
    const bad = schemaFixture("", `${ROW_ID}
    ${kind} broken = 2 ${COLUMN(`sql_type: "X" default_value: "${value}"`)};`);
    assert.throws(() => applyColumnDefaults(bad.messageType, {}, bad), /broken default/, `${kind} ${value}`);
  }
});

test("graphLabel prefers the graph data store's names", () => {
  const store = (body: string) => {
    const protoRoot = fs.mkdtempSync(path.join(os.tmpdir(), "udb-schema-"));
    const pkg = `graph${path.basename(protoRoot).replace(/[^a-zA-Z0-9]/g, "")}.v1`;
    fs.writeFileSync(
      path.join(protoRoot, "fixture.proto"),
      `syntax = "proto3";
package ${pkg};
import "udb/core/common/v1/db.proto";
message Row {
  option (udb.core.common.v1.table) = { table_name: "rows" };
  option (udb.core.common.v1.data_store) = { ${body} };
  string id = 1;
}
`,
    );
    return graphLabel(`${pkg}.Row`, { protoRoot });
  };
  assert.equal(store('backend: "neo4j" options: { key: "udb.neo4j_label" value: "Primary" } options: { key: "node_label" value: "Second" }'), "Primary");
  assert.equal(store('store_kind: "graph" options: { key: "node_label" value: "Second" }'), "Second");
  assert.equal(store('backend: "neo4j" resource_name: "Named"'), "Named");
  assert.equal(store('backend: "qdrant" resource_name: "Named"'), "rows", "another store's name does not apply");
});

// ── 9. Credential introspection ─────────────────────────────────────────────

test("principal and bearerExpiresAt report an exchanged key and nothing else", async () => {
  reset();
  const raw = project({ credentials: { bearerToken: "raw" } });
  try {
    assert.equal(raw.principal(), null, "a raw bearer has no verified principal");
    assert.equal(raw.bearerExpiresAt(), null);
    assert.equal(raw.bearer(), "Bearer raw");
    assert.equal(raw.credentialError(), null);
    assert.throws(() => raw.verify({ tenantId: TENANT }), /no verified principal/);
    raw.verify();
  } finally {
    raw.close();
  }
  assert.match(raw.credentialError()?.message ?? "", /closed/);

  const udb = await UdbProject.connect({
    target: fixture.target,
    tenantId: TENANT,
    purpose: "p",
    deadlineMs: 5_000,
    credentials: { apiKey: "key-1" },
  });
  try {
    const principal = udb.principal();
    assert.equal(principal.tenant_id, TENANT);
    principal.tenant_id = "changed";
    principal.scopes.push("changed");
    assert.equal(udb.principal().tenant_id, TENANT, "the accessor returns a copy");
    assert.equal(udb.principal().scopes.includes("changed"), false);
    const expires = udb.bearerExpiresAt();
    assert.ok(expires && expires.getTime() > Date.now(), "the exchanged bearer's expiry is known");
    udb.verify({ tenantId: TENANT, projectId: PROJECT, requiredScopes: ["udb:read"] });
    await udb.logout();
    assert.equal(udb.principal(), null);
    assert.equal(udb.bearerExpiresAt(), null);
  } finally {
    udb.close();
  }
});

// ── 10. Delete mode ─────────────────────────────────────────────────────────

test("deleteFile reaches the wire with an explicit mode through its request form", async () => {
  const calls: any[] = [];
  const storage = new StorageFacade({
    unary: (_service: string, method: string, request: any) => {
      calls.push({ method, request });
      return Promise.resolve({});
    },
  } as any);
  await storage.deleteFile({ tenant_id: TENANT, file_id: "f1", mode: "DELETE_MODE_HARD", reason: "gdpr" });
  assert.deepEqual(calls[0], {
    method: "DeleteFile",
    request: { tenant_id: TENANT, file_id: "f1", mode: "DELETE_MODE_HARD", reason: "gdpr" },
  });
});

test("string file ids carry the connected tenant; an explicit tenant is never replaced", async () => {
  const calls: any[] = [];
  const core = {
    opts: { meta: { tenantId: TENANT } },
    unary: (_service: string, method: string, request: any) => {
      calls.push({ method, request });
      return Promise.resolve({});
    },
  } as any;
  const storage = new StorageFacade(core);
  await storage.deleteFile("f1");
  assert.deepEqual(calls.at(-1), { method: "DeleteFile", request: { file_id: "f1", tenant_id: TENANT } });
  await storage.deleteFile({ file_id: "f1", mode: "DELETE_MODE_SOFT" });
  assert.equal(calls.at(-1).request.tenant_id, TENANT);
  await storage.deleteFile({ tenant_id: OTHER_TENANT, file_id: "f1" });
  assert.equal(calls.at(-1).request.tenant_id, OTHER_TENANT, "an explicit tenant stays");
  await storage.deleteFileMode("f1", "DELETE_MODE_HARD", "gdpr");
  assert.deepEqual(calls.at(-1).request, { file_id: "f1", mode: "DELETE_MODE_HARD", reason: "gdpr", tenant_id: TENANT });
  await storage.getFile("f1");
  assert.equal(calls.at(-1).request.tenant_id, TENANT);
  await storage.getDownloadUrl("f1");
  assert.equal(calls.at(-1).request.tenant_id, TENANT);
  const request = { file_id: "f1" };
  await storage.deleteFile(request);
  assert.equal("tenant_id" in request, false, "the caller's request is not changed");
});

// ── Credential renewal ──────────────────────────────────────────────────────

const refused = () => {
  throw Object.assign(new Error("invalid credential"), { code: grpc.status.UNAUTHENTICATED });
};

/** A real connectEnterprise session whose renewal RPCs follow `script`. */
async function renewalSession(script: {
  refresh?: (request: any) => any;
  login?: (request: any) => any;
  authenticate?: (token: string) => any;
}) {
  reset();
  fixture.principal = principalFixture({ principal_id: "p1", subject: "s1", account_kind: "ACCOUNT_KIND_PERSON", user_id: "u1" });
  const session: any = await UdbProject.connectEnterprise({
    target: fixture.target,
    username: "admin",
    password: "secret",
    tenantCode: "acme",
    purpose: "parity-test",
    deadlineMs: 5_000,
  });
  const auth = session.auth;
  const calls: string[] = [];
  const principal = session.principal();
  const view = {
    refreshToken: async (request: any) => {
      calls.push(`refresh:${request.refresh_token}`);
      return (script.refresh ?? (() => ({ access_token: "refreshed", refresh_token: "r2", access_token_expires_in: "600" })))(request);
    },
    login: async (request: any) => {
      calls.push(`login:${request.username}:${request.tenant_hint}`);
      return (script.login ?? (() => ({ access_token: "relogged", refresh_token: "r3", session_id: "s3", access_token_expires_in: "600" })))(request);
    },
    authenticateBearer: async (token: string) => {
      calls.push(`verify:${token}`);
      return (script.authenticate ?? (() => ({ principal })))(token);
    },
  };
  auth.withoutCredentials = () => view;
  const expire = async () => {
    const token = await session.currentToken();
    await session.tokenStore.save({ ...token, expiresAt: Date.now() - 1_000 });
  };
  await expire();
  return { session, calls, principal, expire };
}

test("a refused refresh logs in again and later renewals use the new token", async () => {
  const { session, calls, expire } = await renewalSession({ refresh: refused });
  try {
    const renewed = await session.refreshIfNeeded();
    assert.equal(renewed.accessToken, "relogged");
    assert.deepEqual(calls, ["refresh:refresh-1", "login:admin:acme", "verify:relogged"]);
    assert.equal(session.bearer(), "Bearer relogged");
    assert.equal(renewed.refreshToken, "r3", "the new login's refresh token is kept");
    assert.equal(session.credentialError(), null);
    await expire();
    calls.length = 0;
    await session.refreshIfNeeded();
    assert.deepEqual(calls.slice(0, 1), ["refresh:r3"], "the next renewal refreshes with the new login's token");
  } finally {
    session.close();
  }
});

test("a healthy refresh is verified and does not log in", async () => {
  const { session, calls, expire } = await renewalSession({});
  try {
    for (let i = 0; i < 3; i += 1) {
      const renewed = await session.refreshIfNeeded();
      assert.equal(renewed.accessToken, "refreshed");
      await expire();
    }
    assert.equal(calls.filter((call: string) => call.startsWith("login")).length, 0);
    assert.equal(calls.filter((call: string) => call.startsWith("verify")).length, 3, "each bearer is verified");
  } finally {
    session.close();
  }
});

test("concurrent renewals share one refresh and one login", async () => {
  const { session, calls } = await renewalSession({ refresh: refused });
  try {
    const results = await Promise.all([session.refreshIfNeeded(), session.refreshIfNeeded(), session.refreshIfNeeded()]);
    assert.ok(results.every((token: any) => token.accessToken === "relogged"));
    assert.equal(calls.filter((call: string) => call.startsWith("login")).length, 1);
  } finally {
    session.close();
  }
});

test("a failed re-login keeps the spent token from being resubmitted", async () => {
  let loginWorks = false;
  const { session, calls } = await renewalSession({
    refresh: refused,
    login: () => {
      if (!loginWorks) throw Object.assign(new Error("password changed"), { code: grpc.status.UNAUTHENTICATED });
      return { access_token: "relogged", refresh_token: "r3", access_token_expires_in: "600" };
    },
  });
  try {
    await assert.rejects(session.refreshIfNeeded(), /renewal and re-login failed: invalid credential; password changed/);
    assert.notEqual(session.bearer(), "Bearer relogged");
    calls.length = 0;
    loginWorks = true;
    assert.equal((await session.refreshIfNeeded()).accessToken, "relogged");
    assert.deepEqual(calls, ["login:admin:acme", "verify:relogged"], "no second submission of the spent refresh token");
  } finally {
    session.close();
  }
});

test("a re-login needing MFA or returning no bearer is refused", async () => {
  for (const reply of [{ mfa_required: true }, {}]) {
    const { session } = await renewalSession({ refresh: refused, login: () => reply });
    try {
      await assert.rejects(session.refreshIfNeeded(), /re-login returned no bearer/);
    } finally {
      session.close();
    }
  }
});

for (const [name, change, message] of [
  ["principal_id", { principal_id: "other" }, /renewal changed principal_id/],
  ["subject", { subject: "other" }, /renewal changed subject/],
  ["account_kind", { account_kind: "ACCOUNT_KIND_SERVICE_ACCOUNT" }, /renewal changed account_kind/],
  ["tenant", { tenant_id: OTHER_TENANT }, /renewed principal changed tenant/],
  ["project", { project_id: "other" }, /renewed principal changed project/],
  ["user", { user_id: "other" }, /renewed principal changed user/],
  ["service identity", { service_identity: "other" }, /renewed principal changed service identity/],
  ["scopes", { scopes: ["udb:admin"] }, /renewed principal changed scopes/],
] as Array<[string, Record<string, unknown>, RegExp]>) {
  test(`a refresh whose principal changed ${name} fails closed with no re-login`, async () => {
    let principalNow: any;
    const { session, calls, principal } = await renewalSession({ authenticate: () => ({ principal: principalNow }) });
    principalNow = { ...principal, ...change };
    try {
      await assert.rejects(session.refreshIfNeeded(), (err: Error) => err instanceof CredentialBindingError && message.test(err.message));
      assert.equal(calls.filter((call: string) => call.startsWith("login")).length, 0, "a binding failure is not retried");
      assert.equal(session.bearer(), "", "the bearer is dropped");
      assert.equal((await session.currentToken()).accessToken, "login-bearer", "the rejected bearer is not stored");
      assert.match(session.credentialError()?.message ?? "", message);
      assert.throws(() => session.asUser(USER_BEARER), /session not authorized/);
      calls.length = 0;
      principalNow = principal;
      await assert.rejects(session.refreshIfNeeded(), CredentialBindingError, "the refusal is permanent");
      assert.deepEqual(calls, [], "nothing more reaches the broker");
    } finally {
      session.close();
    }
  });
}

test("scope order and duplicates are not a change; a re-login is pinned too", async () => {
  let principalNow: any;
  const { session, principal } = await renewalSession({ authenticate: () => ({ principal: principalNow }) });
  principalNow = { ...principal, scopes: [...principal.scopes].reverse().concat(principal.scopes[0]) };
  try {
    assert.equal((await session.refreshIfNeeded()).accessToken, "refreshed");
  } finally {
    session.close();
  }
  const second = await renewalSession({
    refresh: refused,
    authenticate: () => ({ principal: { ...second.principal, principal_id: "attacker" } }),
  });
  try {
    await assert.rejects(second.session.refreshIfNeeded(), CredentialBindingError);
    assert.equal(second.session.bearer(), "");
  } finally {
    second.session.close();
  }
});

test("an explicit login replaces the pin and stops re-login with the old credentials", async () => {
  const { session, calls } = await renewalSession({ refresh: refused });
  try {
    await session.login({ username: "someone", password: "else" });
    await session.tokenStore.save({ accessToken: "x", refreshToken: "r", expiresAt: Date.now() - 1_000 });
    calls.length = 0;
    await session.refreshIfNeeded().catch(() => undefined);
    // The plain refresh rides the credential-free renewal view too; what must be
    // gone is any re-login or verification with the replaced credentials.
    assert.deepEqual(calls, ["refresh:r"], "the enterprise renewal path must be gone");
  } finally {
    session.close();
  }
});

test("an API key re-exchange that changes the principal fails closed and adopts nothing", async () => {
  reset();
  const udb: any = await UdbProject.connect({
    target: fixture.target,
    tenantId: TENANT,
    projectId: PROJECT,
    purpose: "p",
    deadlineMs: 5_000,
    credentials: { apiKey: "key-1" },
  });
  const expire = async () => {
    const token = await udb.currentToken();
    await udb.tokenStore.save({ ...token, expiresAt: Date.now() - 1_000 });
  };
  try {
    for (let i = 0; i < 3; i += 1) {
      await expire();
      assert.equal((await udb.refreshIfNeeded()).accessToken, SERVICE_BEARER, "the same principal may renew");
    }
    assert.equal(udb.credentialError(), null);
    fixture.principal = principalFixture({ scopes: ["udb:read", "svc:scope", "udb:admin"] });
    await expire();
    await assert.rejects(udb.refreshIfNeeded(), (err: Error) => err instanceof CredentialBindingError && /changed scopes/.test(err.message));
    assert.equal(udb.bearer(), "");
    assert.deepEqual(udb.principal().scopes, ["udb:read", "svc:scope"], "the refused principal is not adopted");
    assert.deepEqual(udb.sharedMeta.scopes, ["udb:read", "svc:scope"]);
    await assert.rejects(udb.ready(), /credential refresh is failing/);
    await assert.rejects(udb.refreshIfNeeded(), CredentialBindingError);
    fixture.principal = principalFixture({ scopes: ["udb:admin"] });
    await udb.authenticateApiKeyAndAdopt("key-1");
    assert.equal(udb.bearer(), `Bearer ${SERVICE_BEARER}`, "an explicit adoption chooses the identity again");
  } finally {
    udb.close();
  }
});

test("a first API key exchange without a principal is refused", async () => {
  const original = fixture.authenticate;
  fixture.authenticate = () => ({ access_token: "x", expires_at_unix: "9999999999" });
  try {
    await assert.rejects(
      UdbProject.connect({ target: fixture.target, tenantId: TENANT, purpose: "p", credentials: { apiKey: "k" } }),
      /returned no verified principal/,
    );
  } finally {
    fixture.authenticate = original;
  }
});

// ── 11. gRPC server interceptors ────────────────────────────────────────────

interface Reply {
  tenant?: string;
  user?: string;
  correlation?: string;
  meta?: unknown;
}

const passthrough = {
  requestSerialize: (value: Reply) => Buffer.from(JSON.stringify(value)),
  requestDeserialize: (value: Buffer) => JSON.parse(value.toString() || "{}"),
  responseSerialize: (value: Reply) => Buffer.from(JSON.stringify(value)),
  responseDeserialize: (value: Buffer) => JSON.parse(value.toString() || "{}"),
};

const ECHO_SERVICE: grpc.ServiceDefinition = {
  Unary: { path: "/echo/Unary", requestStream: false, responseStream: false, ...passthrough },
  Server: { path: "/echo/Server", requestStream: false, responseStream: true, ...passthrough },
  Client: { path: "/echo/Client", requestStream: true, responseStream: false, ...passthrough },
  Bidi: { path: "/echo/Bidi", requestStream: true, responseStream: true, ...passthrough },
};

function identity(): Reply {
  return {
    tenant: requestTenantId(),
    user: requestUserId(),
    correlation: requestCorrelationId(),
    meta: requestMetadata(),
  };
}

async function echoServer(options: grpc.ServerOptions) {
  const server = new grpc.Server(options);
  server.addService(ECHO_SERVICE, {
    // An await before reading proves the identity follows the handler across it.
    Unary: async (_call: any, callback: any) => {
      await new Promise((resolve) => setTimeout(resolve, Math.random() * 10));
      callback(null, identity());
    },
    Server: async (call: any) => {
      call.write(identity());
      await new Promise((resolve) => setImmediate(resolve));
      call.write(identity());
      call.end();
    },
    Client: (call: any, callback: any) => {
      call.on("data", () => undefined);
      call.on("end", () => callback(null, identity()));
    },
    Bidi: (call: any) => {
      call.on("data", () => call.write(identity()));
      call.on("end", () => call.end());
    },
  });
  const port = await new Promise<number>((resolve, reject) =>
    server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, bound) =>
      err ? reject(err) : resolve(bound),
    ),
  );
  const client = new (grpc.makeGenericClientConstructor(ECHO_SERVICE, "Echo"))(
    `127.0.0.1:${port}`,
    grpc.credentials.createInsecure(),
  ) as any;
  return { server, client };
}

function sdkHeaders(tenant: string): grpc.Metadata {
  return buildMetadata({
    tenantId: tenant,
    userId: `user-of-${tenant}`,
    purpose: "audit",
    correlationId: `corr-${tenant}`,
    scopes: ["a", "b"],
    serviceIdentity: "svc",
    projectId: "proj",
    clientCatalogVersion: "1.2.3",
  });
}

test("the server interceptor gives unary and streaming handlers the request's identity", async () => {
  const { server, client } = await echoServer({ interceptors: [udbServerInterceptor] });
  try {
    const unary = (tenant: string) =>
      new Promise<Reply>((resolve, reject) =>
        client.Unary({}, sdkHeaders(tenant), (err: Error | null, reply: Reply) => (err ? reject(err) : resolve(reply))),
      );
    // Concurrent requests must not see each other's identity.
    const replies = await Promise.all(["t1", "t2", "t3", "t4"].map(unary));
    replies.forEach((reply, i) => {
      const tenant = `t${i + 1}`;
      assert.equal(reply.tenant, tenant);
      assert.equal(reply.user, `user-of-${tenant}`);
      assert.equal(reply.correlation, `corr-${tenant}`);
    });
    assert.deepEqual(replies[0].meta, {
      tenantId: "t1",
      userId: "user-of-t1",
      purpose: "audit",
      correlationId: "corr-t1",
      scopes: ["a", "b"],
      serviceIdentity: "svc",
      projectId: "proj",
      clientCatalogVersion: "1.2.3",
    });

    const serverStream = client.Server({}, sdkHeaders("s1"));
    const frames: Reply[] = [];
    for await (const frame of serverStream) frames.push(frame);
    assert.deepEqual(frames.map((frame) => frame.tenant), ["s1", "s1"]);

    const clientStreamReply = await new Promise<Reply>((resolve, reject) => {
      const call = client.Client(sdkHeaders("c1"), (err: Error | null, reply: Reply) => (err ? reject(err) : resolve(reply)));
      call.write({});
      call.end();
    });
    assert.equal(clientStreamReply.tenant, "c1");

    const bidiFrames: Reply[] = [];
    const bidi = client.Bidi(sdkHeaders("b1"));
    bidi.write({});
    bidi.end();
    for await (const frame of bidi) bidiFrames.push(frame);
    assert.equal(bidiFrames[0].tenant, "b1");
  } finally {
    client.close();
    server.forceShutdown();
  }
});

test("without the interceptor or outside a request there is no identity", async () => {
  assert.equal(requestMetadata(), undefined);
  assert.equal(requestTenantId(), "");
  assert.equal(requestUserId(), "");
  assert.equal(requestCorrelationId(), "");
  assert.equal(withRequestMetadata({ tenantId: "t", purpose: "", correlationId: "c" }, requestTenantId), "t");
  const { server, client } = await echoServer({});
  try {
    const reply = await new Promise<Reply>((resolve, reject) =>
      client.Unary({}, sdkHeaders("t1"), (err: Error | null, value: Reply) => (err ? reject(err) : resolve(value))),
    );
    assert.equal(reply.tenant, "");
  } finally {
    client.close();
    server.forceShutdown();
  }
});

test("metadataFromGrpc falls back to the request id and drops empty scopes", () => {
  const md = new grpc.Metadata();
  md.set("x-request-id", "req-1");
  md.set("x-scopes", ",a,,b,");
  const meta = metadataFromGrpc(md);
  assert.equal(meta.correlationId, "req-1");
  assert.deepEqual(meta.scopes, ["a", "b"]);
  assert.equal(meta.tenantId, "");
  assert.equal(metadataFromGrpc(new grpc.Metadata()).scopes, undefined);
  md.set("x-correlation-id", "corr-1");
  assert.equal(metadataFromGrpc(md).correlationId, "corr-1");
});
