"use strict";
// Unit tests for the surface shared with the Go SDK: enterprise session, tenant pool,
// environment bootstrap, per-request user, live query, graph and vector builders,
// flags, schema reflection, credential introspection and gRPC server interceptors.
//
// Anything that crosses the wire runs over a real grpc-js server on loopback, so the
// requests are really encoded and the assertions read what a broker would receive.
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
const node_assert_1 = require("node:assert");
const fs = __importStar(require("node:fs"));
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const node_test_1 = require("node:test");
const grpc = __importStar(require("@grpc/grpc-js"));
const protoLoader = __importStar(require("@grpc/proto-loader"));
require("./wkt");
const grpc_1 = require("./adapters/grpc");
const client_1 = require("./client");
const connectEnv_1 = require("./connectEnv");
const facades_1 = require("./facades");
const generatedClient_1 = require("./generatedClient");
const graphVector_1 = require("./graphVector");
const liveQuery_1 = require("./liveQuery");
const project_1 = require("./project");
const protoRoot_1 = require("./protoRoot");
const schema_1 = require("./schema");
const stores_1 = require("./stores");
const tenantPool_1 = require("./tenantPool");
const tenantState_1 = require("./tenantState");
const TENANT = "11111111-1111-4111-8111-111111111111";
const OTHER_TENANT = "99999999-9999-4999-8999-999999999999";
const PROJECT = "22222222-2222-4222-8222-222222222222";
const SERVICE_BEARER = "service-bearer";
const USER_BEARER = "user-bearer";
// ── Fixture broker ──────────────────────────────────────────────────────────
const root = (0, protoRoot_1.defaultProtoRoot)();
const loaded = grpc.loadPackageDefinition(protoLoader.loadSync([
    "udb/core/authn/services/v1/authn_service.proto",
    "udb/core/livequery/services/v1/livequery_service.proto",
    "udb/core/config/services/v1/config_service.proto",
    "udb/core/lock/services/v1/lock_service.proto",
    "udb/services/v1/data_broker.proto",
], {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs: [root, path.resolve(root, "../third_party/googleapis")],
}));
const HEALTH_SERVICE = {
    Check: {
        path: "/grpc.health.v1.Health/Check",
        requestStream: false,
        responseStream: false,
        requestSerialize: (value) => value,
        requestDeserialize: (value) => value,
        responseSerialize: (value) => value,
        responseDeserialize: (value) => value,
    },
};
function principalFixture(overrides = {}) {
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
let fixture;
async function startFixture() {
    const seen = [];
    const state = {
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
    const record = (method, call) => seen.push({ method, request: call.request, headers: call.metadata });
    const unary = (method, reply) => (call, callback) => {
        record(method, call);
        try {
            callback(null, reply(call.request, call.metadata));
        }
        catch (err) {
            callback(err);
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
        Subscribe: (call) => {
            record("Subscribe", call);
            const next = state.subscribe.shift();
            if (next)
                next(call);
            else
                call.end();
        },
    });
    server.addService(loaded.udb.core.config.services.v1.ConfigService.service, {
        EvaluateFlags: unary("EvaluateFlags", (request) => {
            const values = {};
            if (request.keys.includes("on"))
                values.on = { bool_value: true };
            if (request.keys.includes("off"))
                values.off = { bool_value: false };
            if (request.keys.includes("text"))
                values.text = { string_value: "true" };
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
        Check: (call, callback) => {
            record("Check", call);
            const status = state.health.length > 1 ? state.health.shift() : state.health[0] ?? 1;
            callback(null, Buffer.from([0x08, status]));
        },
    });
    const port = await new Promise((resolve, reject) => server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, bound) => err ? reject(err) : resolve(bound)));
    state.target = `127.0.0.1:${port}`;
    state.stop = () => server.forceShutdown();
    return state;
}
function reset() {
    fixture.seen.length = 0;
    fixture.health.length = 0;
    fixture.subscribe.length = 0;
    fixture.principal = principalFixture();
}
function header(headers, name) {
    const values = headers.get(name);
    return values.length ? String(values[0]) : undefined;
}
function last(method) {
    const hit = [...fixture.seen].reverse().find((entry) => entry.method === method);
    node_assert_1.strict.ok(hit, `no ${method} call reached the fixture`);
    return hit;
}
function project(overrides = {}) {
    return new project_1.UdbProject({
        target: fixture.target,
        tenantId: TENANT,
        projectId: PROJECT,
        purpose: "parity-test",
        deadlineMs: 5_000,
        ...overrides,
    });
}
(0, node_test_1.before)(async () => {
    fixture = await startFixture();
});
(0, node_test_1.after)(() => {
    fixture.stop();
});
// ── 1. Enterprise session and tenant state ──────────────────────────────────
(0, node_test_1.test)("TenantState adopts only a canonical lowercase UUID and refuses without mutating", () => {
    const state = new tenantState_1.TenantState("acme");
    node_assert_1.strict.equal(state.isVerified, false);
    node_assert_1.strict.throws(() => state.getVerified(), /tenant not verified/);
    node_assert_1.strict.throws(() => state.validateTenantId(TENANT), /tenant not verified/);
    for (const bad of ["", "   ", "acme", "ABCDEF00-1111-4111-8111-111111111111", `${TENANT}x`]) {
        node_assert_1.strict.throws(() => state.adopt(bad), /canonical|empty/, `adopting ${JSON.stringify(bad)}`);
        node_assert_1.strict.equal(state.canonicalId, "", "a refused adopt must not change the state");
        node_assert_1.strict.equal(state.isVerified, false);
    }
    state.adopt(` ${TENANT} `);
    node_assert_1.strict.equal(state.getVerified(), TENANT);
    node_assert_1.strict.equal(state.tenantCode, "acme");
    state.validateTenantId(TENANT);
    state.validateTenantId("");
    node_assert_1.strict.throws(() => state.validateTenantId(OTHER_TENANT), (err) => err.message.includes(OTHER_TENANT) && err.message.includes(TENANT), "a mismatch must name both values");
});
(0, node_test_1.test)("connectEnterprise verifies the tenant, adopts the principal and validates records", async () => {
    reset();
    fixture.principal = principalFixture({ user_id: "user-1" });
    const session = await project_1.UdbProject.connectEnterprise({
        target: fixture.target,
        username: "admin",
        password: "secret",
        tenantCode: "acme",
        purpose: "parity-test",
        deadlineMs: 5_000,
    });
    try {
        node_assert_1.strict.equal(last("Login").request.tenant_hint, "acme");
        node_assert_1.strict.equal(session.canonicalTenantId, TENANT);
        node_assert_1.strict.equal(session.canonicalProjectId, PROJECT);
        node_assert_1.strict.equal(session.tenantState.tenantCode, "acme");
        node_assert_1.strict.equal(session.bearer(), "Bearer login-bearer");
        node_assert_1.strict.equal(session.principal().user_id, "user-1");
        for (let i = 0; i < 3; i += 1)
            session.validateTenant(TENANT);
        session.validateTenant("");
        node_assert_1.strict.throws(() => session.validateTenant(OTHER_TENANT), /tenant_id mismatch/);
        node_assert_1.strict.throws(() => session.validateTenant("acme"), /tenant_id mismatch/);
        await session.flags.enabled("on");
        const sent = last("EvaluateFlags");
        node_assert_1.strict.equal(header(sent.headers, "x-tenant-id"), TENANT, "calls must carry the canonical UUID, not the code");
        node_assert_1.strict.equal(header(sent.headers, "x-user-id"), "user-1");
        node_assert_1.strict.equal(header(sent.headers, "x-scopes"), "udb:read,svc:scope");
        node_assert_1.strict.equal(sent.request.tenant_id, TENANT);
    }
    finally {
        session.close();
    }
});
(0, node_test_1.test)("connectEnterprise refuses missing input and a principal without a canonical tenant", async () => {
    reset();
    await node_assert_1.strict.rejects(project_1.UdbProject.connectEnterprise({ target: fixture.target, username: "", password: "x" }), /requires target, username, and password/);
    await node_assert_1.strict.rejects(project_1.UdbProject.connectEnterprise({ target: "", username: "u", password: "x" }), /requires target, username, and password/);
    node_assert_1.strict.equal(fixture.seen.length, 0, "input errors must not reach the broker");
    fixture.principal = principalFixture({ tenant_id: "acme" });
    await node_assert_1.strict.rejects(project_1.UdbProject.connectEnterprise({ target: fixture.target, username: "u", password: "x", tenantId: "acme" }), /not a canonical/);
    fixture.principal = principalFixture();
    fixture.authenticate = (original => () => {
        fixture.authenticate = original;
        return { access_token: "x" };
    })(fixture.authenticate);
    await node_assert_1.strict.rejects(project_1.UdbProject.connectEnterprise({ target: fixture.target, username: "u", password: "x" }), /no principal returned/);
});
(0, node_test_1.test)("a session that was never adopted cannot validate a tenant", () => {
    const plain = project();
    try {
        node_assert_1.strict.equal(plain.canonicalTenantId, "");
        node_assert_1.strict.throws(() => plain.validateTenant(TENANT), /tenant not verified/);
    }
    finally {
        plain.close();
    }
});
// ── 2. Tenant pool ──────────────────────────────────────────────────────────
class PoolClient {
    tenant;
    closes = 0;
    failing = null;
    constructor(tenant) {
        this.tenant = tenant;
    }
    credentialError() {
        return this.failing;
    }
    close() {
        this.closes += 1;
    }
}
function gate() {
    let open = () => { };
    const opened = new Promise((resolve) => {
        open = resolve;
    });
    return { open, opened };
}
(0, node_test_1.test)("pool shares one connect, reuses the client and lists tenants sorted", async () => {
    let connects = 0;
    const hold = gate();
    const pool = new tenantPool_1.TenantSessionPool(async (tenant) => {
        connects += 1;
        await hold.opened;
        return new PoolClient(tenant);
    });
    const concurrent = [pool.get("b"), pool.get("b"), pool.get("b")];
    hold.open();
    const clients = await Promise.all(concurrent);
    node_assert_1.strict.equal(connects, 1, "concurrent first calls must share one connect");
    node_assert_1.strict.ok(clients.every((client) => client === clients[0]));
    node_assert_1.strict.equal(await pool.get("b"), clients[0], "a later call reuses the client");
    await pool.get("a");
    node_assert_1.strict.deepEqual(pool.tenants(), ["a", "b"]);
    await pool.close();
});
(0, node_test_1.test)("pool replaces a client whose credential refresh is failing and closes it", async () => {
    const pool = new tenantPool_1.TenantSessionPool(async (tenant) => new PoolClient(tenant));
    const first = await pool.get("t");
    first.failing = new Error("refresh failing");
    const second = await pool.get("t");
    node_assert_1.strict.notEqual(second, first);
    await new Promise((resolve) => setImmediate(resolve));
    node_assert_1.strict.equal(first.closes, 1, "the stale client must be closed");
    await pool.close();
});
(0, node_test_1.test)("pool does not cache a failed or nil connect", async () => {
    let attempts = 0;
    const pool = new tenantPool_1.TenantSessionPool(async (tenant) => {
        attempts += 1;
        if (attempts === 1)
            throw new Error("dial failed");
        if (attempts === 2)
            return undefined;
        return new PoolClient(tenant);
    });
    await node_assert_1.strict.rejects(pool.get("t"), /dial failed/);
    await node_assert_1.strict.rejects(pool.get("t"), /returned a nil client/);
    node_assert_1.strict.ok(await pool.get("t"));
    await pool.close();
});
(0, node_test_1.test)("evict fences a pending connect and closes its late client", async () => {
    const hold = gate();
    const late = new PoolClient("t");
    let signal;
    let connects = 0;
    const pool = new tenantPool_1.TenantSessionPool(async (tenant, abort) => {
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
    await node_assert_1.strict.rejects(waiting, /was evicted/);
    node_assert_1.strict.equal(signal?.aborted, true, "evict must cancel the pending connect");
    const replacement = await pool.get("t");
    node_assert_1.strict.notEqual(replacement, late);
    hold.open();
    await new Promise((resolve) => setImmediate(resolve));
    await new Promise((resolve) => setImmediate(resolve));
    node_assert_1.strict.equal(late.closes, 1, "a late result of a retired attempt must be closed");
    node_assert_1.strict.equal(await pool.get("t"), replacement, "a late result must not replace the new generation");
    await pool.evict("t");
    node_assert_1.strict.equal(replacement.closes, 1, "evict closes the live client");
    node_assert_1.strict.deepEqual(pool.tenants(), []);
    await pool.close();
});
(0, node_test_1.test)("pool close refuses waiters and later gets, closes clients, reports the first failure", async () => {
    const hold = gate();
    const pool = new tenantPool_1.TenantSessionPool(async (tenant) => {
        if (tenant === "slow")
            await hold.opened;
        const client = new PoolClient(tenant);
        if (tenant === "bad")
            client.close = () => {
                throw new Error("close failed");
            };
        return client;
    });
    const good = await pool.get("good");
    await pool.get("bad");
    const waiting = pool.get("slow");
    await node_assert_1.strict.rejects(pool.close(), /close failed/);
    await node_assert_1.strict.rejects(waiting, /pool is closed/);
    await node_assert_1.strict.rejects(pool.get("good"), /pool is closed/);
    node_assert_1.strict.equal(good.closes, 1);
    hold.open();
});
(0, node_test_1.test)("first caller's abort cancels the connect; a follower's abort only ends its wait", async () => {
    const signals = [];
    const holds = [gate(), gate()];
    let connects = 0;
    const pool = new tenantPool_1.TenantSessionPool(async (tenant, signal) => {
        signals.push(signal);
        await holds[connects++].opened;
        return new PoolClient(tenant);
    });
    const owner = new AbortController();
    const first = pool.get("t", { signal: owner.signal });
    owner.abort(new Error("owner gave up"));
    await node_assert_1.strict.rejects(first, /owner gave up/);
    node_assert_1.strict.equal(signals[0].aborted, true);
    const followerAbort = new AbortController();
    const second = pool.get("t");
    const follower = pool.get("t", { signal: followerAbort.signal });
    followerAbort.abort(new Error("follower gave up"));
    await node_assert_1.strict.rejects(follower, /follower gave up/);
    node_assert_1.strict.equal(signals[1].aborted, false, "a follower must not cancel the shared connect");
    holds[1].open();
    node_assert_1.strict.ok(await second);
    const already = new AbortController();
    already.abort(new Error("already gone"));
    await node_assert_1.strict.rejects(pool.get("t", { signal: already.signal }), /already gone/);
    await pool.close();
});
(0, node_test_1.test)("API key pool connects per tenant, owns its key map and refuses unknown or mismatched tenants", async () => {
    reset();
    const keys = { [TENANT]: "key-1", [OTHER_TENANT]: "key-2" };
    const pool = (0, tenantPool_1.newApiKeyTenantPool)({ target: fixture.target, purpose: "parity-test", deadlineMs: 5_000 }, keys);
    delete keys[TENANT];
    try {
        const client = await pool.get(TENANT);
        node_assert_1.strict.equal(last("Authenticate").request.api_key, "key-1", "the pool must keep its own copy of the keys");
        node_assert_1.strict.equal(await pool.get(TENANT), client);
        node_assert_1.strict.equal(fixture.seen.filter((call) => call.method === "Authenticate").length, 1);
        await node_assert_1.strict.rejects(pool.get("unknown"), /no API key for tenant "unknown"/);
        await node_assert_1.strict.rejects(pool.get(OTHER_TENANT), /tenant is "11111111/, "a key for another tenant must be refused");
        node_assert_1.strict.deepEqual(pool.tenants(), [TENANT]);
    }
    finally {
        await pool.close();
    }
});
// ── 3. Environment bootstrap and health ─────────────────────────────────────
(0, node_test_1.test)("configFromEnv reports every problem at once", () => {
    node_assert_1.strict.throws(() => (0, connectEnv_1.configFromEnv)("APP_", { APP_TARGET: "nohost", APP_AUTH_TARGET: "nope", APP_DEADLINE: "soon" }), (err) => {
        for (const part of [
            "APP_TENANT_ID is not set",
            "APP_PURPOSE is not set",
            "APP_API_KEY (or APP_BEARER) is not set",
            'APP_TARGET must be host:port, got "nohost"',
            'APP_AUTH_TARGET must be host:port, got "nope"',
            'APP_DEADLINE must be a positive duration such as 10s, got "soon"',
        ]) {
            node_assert_1.strict.ok(err.message.includes(part), `missing "${part}" in: ${err.message}`);
        }
        return true;
    });
    node_assert_1.strict.throws(() => (0, connectEnv_1.configFromEnv)("", {}), /UDB_TARGET is not set/, "the default prefix is UDB_");
});
(0, node_test_1.test)("configFromEnv maps the contract", () => {
    const config = (0, connectEnv_1.configFromEnv)("", {
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
    node_assert_1.strict.equal(config.target, "broker:50051");
    node_assert_1.strict.equal(config.authTarget, "broker:50061");
    node_assert_1.strict.equal(config.webrtcTarget, "broker:50071");
    node_assert_1.strict.equal(config.projectId, PROJECT, "UDB_PROJECT is accepted too");
    node_assert_1.strict.deepEqual(config.scopes, ["a", "b", "c"], "scopes split on comma or space and dedupe");
    node_assert_1.strict.equal(config.serviceIdentity, "svc");
    node_assert_1.strict.equal(config.deadlineMs, 90_000);
    node_assert_1.strict.equal(config.secure, true);
    node_assert_1.strict.equal(config.channelOptions?.["grpc.ssl_target_name_override"], "broker.internal");
    node_assert_1.strict.deepEqual(config.credentials, { apiKey: "key" });
    const plain = (0, connectEnv_1.configFromEnv)("", { UDB_TARGET: "b:1", UDB_TENANT_ID: "t", UDB_PURPOSE: "p", UDB_BEARER: "tok", UDB_TLS: "no" });
    node_assert_1.strict.equal(plain.secure, undefined);
    node_assert_1.strict.deepEqual(plain.credentials, { bearerToken: "tok" });
});
(0, node_test_1.test)("parseDurationMs follows Go durations", () => {
    node_assert_1.strict.equal((0, connectEnv_1.parseDurationMs)("10s"), 10_000);
    node_assert_1.strict.equal((0, connectEnv_1.parseDurationMs)("250ms"), 250);
    node_assert_1.strict.equal((0, connectEnv_1.parseDurationMs)("1h1m"), 3_660_000);
    node_assert_1.strict.equal((0, connectEnv_1.parseDurationMs)("1.5s"), 1_500);
    node_assert_1.strict.equal((0, connectEnv_1.parseDurationMs)("-1s"), -1_000);
    node_assert_1.strict.equal((0, connectEnv_1.parseDurationMs)("0"), 0);
    for (const bad of ["", "10", "s", "10 s", "1d", "soon"]) {
        node_assert_1.strict.equal((0, connectEnv_1.parseDurationMs)(bad), undefined, `parsing ${JSON.stringify(bad)}`);
    }
});
(0, node_test_1.test)("verifyPrincipal reports every mismatch and needs a principal only when expecting", () => {
    (0, connectEnv_1.verifyPrincipal)(null, {});
    (0, connectEnv_1.verifyPrincipal)(null);
    node_assert_1.strict.throws(() => (0, connectEnv_1.verifyPrincipal)(null, { tenantId: TENANT }), /no verified principal/);
    const principal = principalFixture();
    (0, connectEnv_1.verifyPrincipal)(principal, {
        tenantId: TENANT,
        projectId: PROJECT,
        serviceIdentity: "spiffe://fixture/service",
        requiredScopes: ["UDB:READ"],
    });
    node_assert_1.strict.throws(() => (0, connectEnv_1.verifyPrincipal)(principal, {
        tenantId: OTHER_TENANT,
        projectId: "p",
        serviceIdentity: "s",
        requiredScopes: ["udb:read", "udb:write", "udb:admin"],
    }), (err) => {
        for (const part of [`tenant is "${TENANT}", expected "${OTHER_TENANT}"`, `project is "${PROJECT}"`, "service identity is", "lacks scopes udb:write, udb:admin"]) {
            node_assert_1.strict.ok(err.message.includes(part), `missing "${part}" in: ${err.message}`);
        }
        return true;
    });
});
(0, node_test_1.test)("connectFromEnv connects and verifies; a mismatch is refused", async () => {
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
    for (const [name, value] of Object.entries(env))
        process.env[prefix + name] = value;
    try {
        for (let i = 0; i < 2; i += 1) {
            const udb = await project_1.UdbProject.connectFromEnv(prefix, { tenantId: TENANT, requiredScopes: ["udb:read"] });
            node_assert_1.strict.equal(udb.bearer(), `Bearer ${SERVICE_BEARER}`);
            udb.close();
        }
        await node_assert_1.strict.rejects(project_1.UdbProject.connectFromEnv(prefix, { requiredScopes: ["udb:admin"] }), /lacks scopes udb:admin/);
        await node_assert_1.strict.rejects(project_1.UdbProject.connectFromEnv("UDB_PARITY_ABSENT_"), /environment is incomplete/);
        process.env[`${prefix}BEARER`] = "tok";
        await node_assert_1.strict.rejects(project_1.UdbProject.connectFromEnv(prefix), /mutually exclusive/);
    }
    finally {
        for (const name of [...Object.keys(env), "BEARER"])
            delete process.env[prefix + name];
    }
});
(0, node_test_1.test)("ready reports the broker's status and the connection's credential", async () => {
    reset();
    const udb = project();
    try {
        for (let i = 0; i < 3; i += 1)
            await udb.ready();
        node_assert_1.strict.equal(fixture.seen.filter((call) => call.method === "Check").length, 3);
        fixture.health.push(2);
        await node_assert_1.strict.rejects(udb.ready(), /udb: broker is NOT_SERVING/);
        fixture.health.length = 0;
        fixture.health.push(3);
        await node_assert_1.strict.rejects(udb.ready(), /udb: broker is SERVICE_UNKNOWN/);
        fixture.health.length = 0;
        udb.lastRefreshError = new Error("refresh refused");
        await node_assert_1.strict.rejects(udb.ready(), /credential refresh is failing: refresh refused/);
        udb.lastRefreshError = null;
        await udb.ready();
    }
    finally {
        udb.close();
    }
    await node_assert_1.strict.rejects(udb.ready(), /credential owner is closed/);
    const dead = new project_1.UdbProject({ target: "127.0.0.1:1", tenantId: TENANT, purpose: "p" });
    try {
        await node_assert_1.strict.rejects(dead.ready({ deadlineMs: 2_000 }), /broker health check failed/);
    }
    finally {
        dead.close();
    }
});
// ── 4. Per-request user ─────────────────────────────────────────────────────
(0, node_test_1.test)("asUser sends the user's bearer alone, keeps tenant and audit, blanks service hints", async () => {
    reset();
    const udb = await project_1.UdbProject.connect({
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
        node_assert_1.strict.equal(header(service, "authorization"), `Bearer ${SERVICE_BEARER}`);
        node_assert_1.strict.equal(header(service, "x-service-identity"), "spiffe://fixture/service");
        node_assert_1.strict.equal(header(service, "x-scopes"), "udb:read,svc:scope");
        for (const token of [USER_BEARER, `Bearer ${USER_BEARER}`, `  Bearer ${USER_BEARER}  `]) {
            await udb.flags.enabled("on", {}, udb.asUser(token, {
                meta: { purpose: "delegated-purpose", correlationId: "delegated-correlation", apiKey: "raw" },
                headers: { "x-api-key": "raw", "X-UDB-API-KEY": "raw", "x-custom": "kept" },
            }));
            const delegated = last("EvaluateFlags").headers;
            node_assert_1.strict.deepEqual(delegated.get("authorization"), [`Bearer ${USER_BEARER}`], "exactly one credential");
            node_assert_1.strict.equal(header(delegated, "x-tenant-id"), TENANT, "the connected tenant does not change");
            node_assert_1.strict.equal(header(delegated, "x-udb-project-id"), PROJECT, "the connected project does not change");
            node_assert_1.strict.equal(header(delegated, "x-purpose"), "delegated-purpose");
            node_assert_1.strict.equal(header(delegated, "x-correlation-id"), "delegated-correlation");
            node_assert_1.strict.equal(header(delegated, "x-custom"), "kept");
            node_assert_1.strict.equal(header(delegated, "x-api-key"), undefined, "a raw key must not ride along");
            node_assert_1.strict.equal(header(delegated, "x-udb-api-key"), undefined);
            for (const hint of ["x-user-id", "x-service-identity", "x-scopes"]) {
                node_assert_1.strict.equal(header(delegated, hint), "", `${hint} must not keep the service's value`);
            }
        }
        await udb.flags.enabled("on");
        node_assert_1.strict.equal(header(last("EvaluateFlags").headers, "authorization"), `Bearer ${SERVICE_BEARER}`, "delegation applies only to the call it was passed to");
        udb.poisoned = true;
        udb.lastRefreshError = new Error("revoked");
        node_assert_1.strict.throws(() => udb.asUser(USER_BEARER), /session not authorized: revoked/);
    }
    finally {
        udb.close();
    }
});
(0, node_test_1.test)("asUserCall does not change the options it was given", () => {
    const call = { meta: { apiKey: "raw" }, headers: { "x-api-key": "raw" } };
    const out = (0, facades_1.asUserCall)(USER_BEARER, call);
    node_assert_1.strict.deepEqual(call, { meta: { apiKey: "raw" }, headers: { "x-api-key": "raw" } });
    node_assert_1.strict.equal(out.headers?.authorization, `Bearer ${USER_BEARER}`);
    node_assert_1.strict.deepEqual((0, facades_1.asUserCall)(USER_BEARER).meta, {});
});
(0, node_test_1.test)("validateSession maps the reply and refuses an invalid token", async () => {
    reset();
    const udb = project();
    try {
        const session = await udb.validateSession(`Bearer ${USER_BEARER}`);
        const sent = last("ValidateToken").request;
        node_assert_1.strict.equal(sent.token, USER_BEARER, "the prefix is stripped");
        node_assert_1.strict.equal(sent.token_type, "TOKEN_TYPE_JWT_ACCESS");
        node_assert_1.strict.equal(session.publicId, "sesspub_1");
        node_assert_1.strict.equal(session.userId, "user-1");
        node_assert_1.strict.equal(session.tenantId, TENANT);
        node_assert_1.strict.deepEqual(session.roles, ["admin"]);
        node_assert_1.strict.deepEqual(session.scopes, ["udb:read"]);
        node_assert_1.strict.equal(session.expiresAt?.getTime(), 1_800_000_000_500);
        node_assert_1.strict.equal(session.principal.user_id, "user-1");
        const original = fixture.validate;
        fixture.validate = () => ({ valid: false });
        await node_assert_1.strict.rejects(udb.validateSession("bad"), stores_1.NotFoundError, "an invalid token is NotFoundError");
        fixture.validate = () => {
            throw Object.assign(new Error("nope"), { code: grpc.status.UNAUTHENTICATED, details: "nope" });
        };
        await node_assert_1.strict.rejects(udb.validateSession("bad"), (err) => err.code === grpc.status.UNAUTHENTICATED);
        fixture.validate = (_request, headers) => {
            node_assert_1.strict.equal(header(headers, "authorization"), `Bearer ${USER_BEARER}`);
            return original(_request, headers);
        };
        await udb.validateSession(USER_BEARER, udb.asUser(USER_BEARER));
        fixture.validate = original;
    }
    finally {
        udb.close();
    }
});
// ── 5. Live query ───────────────────────────────────────────────────────────
const LOCK = "udb.core.lock.entity.v1.Lock";
function collectUntil(events, count, abort) {
    return (event) => {
        events.push(event);
        if (events.length >= count)
            abort.abort(new Error("done"));
    };
}
function errorDetailTrailer(capability, retryAfterMs = 0) {
    const capabilityBytes = Buffer.from(capability);
    const parts = [];
    if (capability)
        parts.push(Buffer.from([0x1a, capabilityBytes.length]), capabilityBytes);
    if (retryAfterMs) {
        const varint = [];
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
(0, node_test_1.test)("predicate helpers build the wire predicates", () => {
    node_assert_1.strict.deepEqual((0, liveQuery_1.lqEq)("a", "1"), { field: "a", op: "LIVE_QUERY_COMPARISON_EQ", value: "1" });
    node_assert_1.strict.equal((0, liveQuery_1.lqNe)("a", "1").op, "LIVE_QUERY_COMPARISON_NE");
    node_assert_1.strict.equal((0, liveQuery_1.lqLt)("a", "1").op, "LIVE_QUERY_COMPARISON_LT");
    node_assert_1.strict.equal((0, liveQuery_1.lqLe)("a", "1").op, "LIVE_QUERY_COMPARISON_LE");
    node_assert_1.strict.equal((0, liveQuery_1.lqGt)("a", "1").op, "LIVE_QUERY_COMPARISON_GT");
    node_assert_1.strict.equal((0, liveQuery_1.lqGe)("a", "1").op, "LIVE_QUERY_COMPARISON_GE");
    node_assert_1.strict.deepEqual((0, liveQuery_1.lqIn)("a", "1", "2"), { field: "a", op: "LIVE_QUERY_COMPARISON_IN", value: "", values: ["1", "2"] });
    node_assert_1.strict.equal((0, liveQuery_1.lqNotIn)("a", "1").op, "LIVE_QUERY_COMPARISON_NOT_IN");
    node_assert_1.strict.deepEqual((0, liveQuery_1.lqIsNull)("a"), { field: "a", op: "LIVE_QUERY_COMPARISON_IS_NULL", value: "" });
    node_assert_1.strict.equal((0, liveQuery_1.lqIsNotNull)("a").op, "LIVE_QUERY_COMPARISON_IS_NOT_NULL");
});
(0, node_test_1.test)("liveQuery delivers snapshot and changes and absorbs keepalives", async () => {
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
    const events = [];
    try {
        await node_assert_1.strict.rejects((0, liveQuery_1.liveQuery)(udb, LOCK, { where: [(0, liveQuery_1.lqEq)("status", "HELD")], anyOf: [[(0, liveQuery_1.lqEq)("owner", "me"), (0, liveQuery_1.lqEq)("assignee", "me")]], snapshotLimit: 7, signal: abort.signal }, collectUntil(events, 3, abort)), /done/);
        const sent = last("Subscribe");
        node_assert_1.strict.equal(sent.request.tenant_id, TENANT);
        node_assert_1.strict.equal(sent.request.project_id, PROJECT);
        node_assert_1.strict.equal(sent.request.message_type, LOCK);
        node_assert_1.strict.equal(sent.request.snapshot_limit, 7);
        node_assert_1.strict.equal(sent.request.filters[0].op, "LIVE_QUERY_COMPARISON_EQ");
        node_assert_1.strict.equal(sent.request.any_of[0].predicates.length, 2);
        node_assert_1.strict.equal(sent.request.since_event_id, "");
        node_assert_1.strict.equal(header(sent.headers, "x-udb-livequery-resume"), undefined);
        node_assert_1.strict.equal(events[0].isSnapshot, true);
        node_assert_1.strict.equal(events[0].resumed, false);
        node_assert_1.strict.deepEqual(events[0].snapshot, [{ id: "1" }, { id: "2" }]);
        node_assert_1.strict.equal(events[1].isSnapshot, false);
        node_assert_1.strict.equal(events[1].op, "LIVE_QUERY_CHANGE_OP_INSERT");
        node_assert_1.strict.deepEqual(events[1].row, { id: "3" });
        node_assert_1.strict.equal(events[1].eventId, "e3");
        node_assert_1.strict.equal(events[2].op, "LIVE_QUERY_CHANGE_OP_DELETE");
        node_assert_1.strict.equal(events.length, 3, "keepalives must not reach the handler");
    }
    finally {
        udb.close();
    }
});
(0, node_test_1.test)("liveQuery reconnects after a transient failure and resumes after the last change", async () => {
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
    const events = [];
    const reconnects = [];
    try {
        await node_assert_1.strict.rejects((0, liveQuery_1.liveQuery)(udb, LOCK, { backoffMs: 5, signal: abort.signal, onError: (err) => reconnects.push(err) }, collectUntil(events, 3, abort)), /done/);
        node_assert_1.strict.equal(reconnects.length, 1);
        node_assert_1.strict.match(reconnects[0].message, new RegExp(`live query ${LOCK} reconnecting`));
        const resume = last("Subscribe");
        node_assert_1.strict.equal(resume.request.since_event_id, "e7");
        node_assert_1.strict.equal(header(resume.headers, "x-udb-livequery-resume"), "e7", "both resume carriers name one event");
        node_assert_1.strict.equal(events[2].isSnapshot, true);
        node_assert_1.strict.equal(events[2].resumed, true, "the snapshot after a reconnect is marked");
    }
    finally {
        udb.close();
    }
});
(0, node_test_1.test)("liveQuery honors the broker's retry-after and an explicit resume point", async () => {
    reset();
    fixture.subscribe.push((call) => {
        call.emit("error", { code: grpc.status.RESOURCE_EXHAUSTED, details: "busy", metadata: errorDetailTrailer("", 150) });
    });
    fixture.subscribe.push((call) => {
        call.write({ snapshot: { rows_json: [] } });
    });
    const udb = project();
    const abort = new AbortController();
    const events = [];
    const started = Date.now();
    try {
        await node_assert_1.strict.rejects((0, liveQuery_1.liveQuery)(udb, LOCK, { backoffMs: 1, sinceEventId: "earlier", signal: abort.signal }, collectUntil(events, 1, abort)), /done/);
        node_assert_1.strict.ok(Date.now() - started >= 140, "the reconnect must wait the broker's retry-after");
        for (const call of fixture.seen.filter((entry) => entry.method === "Subscribe")) {
            node_assert_1.strict.equal(call.request.since_event_id, "earlier");
        }
    }
    finally {
        udb.close();
    }
});
(0, node_test_1.test)("liveQuery stops on refusals reconnecting cannot fix", async () => {
    const udb = project();
    try {
        reset();
        fixture.subscribe.push((call) => {
            call.write({ snapshot: { rows_json: ['{"id":"1"}'] } });
            setImmediate(() => call.emit("error", {
                code: grpc.status.FAILED_PRECONDITION,
                details: "no feed",
                metadata: errorDetailTrailer("cdc_change_feed"),
            }));
        });
        const delivered = [];
        await node_assert_1.strict.rejects((0, liveQuery_1.liveQuery)(udb, LOCK, {}, (event) => {
            delivered.push(event);
        }), liveQuery_1.NoDeltaFeedError);
        node_assert_1.strict.equal(delivered.length, 1, "the snapshot is delivered first");
        for (const code of [
            grpc.status.FAILED_PRECONDITION,
            grpc.status.INVALID_ARGUMENT,
            grpc.status.PERMISSION_DENIED,
            grpc.status.NOT_FOUND,
            grpc.status.UNIMPLEMENTED,
        ]) {
            reset();
            fixture.subscribe.push((call) => call.emit("error", { code, details: "refused" }));
            await node_assert_1.strict.rejects((0, liveQuery_1.liveQuery)(udb, LOCK, {}, () => undefined), (err) => err instanceof generatedClient_1.UdbError && err.code === code && !(err instanceof liveQuery_1.NoDeltaFeedError));
            node_assert_1.strict.equal(fixture.seen.filter((call) => call.method === "Subscribe").length, 1, `code ${code} must not reconnect`);
        }
    }
    finally {
        udb.close();
    }
});
(0, node_test_1.test)("liveQuery ends on a handler failure or an undecodable row", async () => {
    const udb = project();
    try {
        reset();
        fixture.subscribe.push((call) => call.write({ snapshot: { rows_json: [] } }));
        await node_assert_1.strict.rejects((0, liveQuery_1.liveQuery)(udb, LOCK, {}, () => {
            throw new Error("handler refused");
        }), /handler refused/);
        for (const raw of ["not json", "[1]", "5"]) {
            fixture.subscribe.push((call) => call.write({ snapshot: { rows_json: [raw] } }));
            await node_assert_1.strict.rejects((0, liveQuery_1.liveQuery)(udb, LOCK, {}, () => undefined), /row is not a JSON object/);
        }
        fixture.subscribe.push((call) => call.write({ snapshot: { rows_json: ['{"id":"1"}'] } }));
        await node_assert_1.strict.rejects((0, liveQuery_1.liveQuery)(udb, LOCK, { decode: () => { throw new Error("decode refused"); } }, () => undefined), /decode refused/);
        const abort = new AbortController();
        abort.abort(new Error("never started"));
        await node_assert_1.strict.rejects((0, liveQuery_1.liveQuery)(udb, LOCK, { signal: abort.signal }, () => undefined), /never started/);
    }
    finally {
        udb.close();
    }
});
(0, node_test_1.test)("liveQuery decodes rows with the supplied decoder", async () => {
    reset();
    fixture.subscribe.push((call) => call.write({ snapshot: { rows_json: ['{"id":"1"}'] } }));
    const udb = project();
    const abort = new AbortController();
    const events = [];
    try {
        await node_assert_1.strict.rejects((0, liveQuery_1.liveQuery)(udb, LOCK, { signal: abort.signal, decode: (record) => ({ upper: String(record.id) + "!" }) }, collectUntil(events, 1, abort)), /done/);
        node_assert_1.strict.deepEqual(events[0].snapshot, [{ upper: "1!" }]);
    }
    finally {
        udb.close();
    }
});
// ── 6. Graph and vector builders ────────────────────────────────────────────
(0, node_test_1.test)("graphFrom builds the typed traversal and returns plain rows", async () => {
    reset();
    const udb = project();
    try {
        const builder = (0, graphVector_1.graphFrom)(udb, LOCK, "n1")
            .out("RELATED", "KNOWS")
            .depth(1, 2)
            .labels("Note")
            .where("archived", "false")
            .whereEdge("kind", "peer")
            .limit(5)
            .withEdges();
        for (let i = 0; i < 2; i += 1) {
            const rows = await builder.run();
            node_assert_1.strict.deepEqual(rows, [{ id: "n2", hops: 2 }]);
        }
        const sent = last("GraphQuery").request;
        node_assert_1.strict.equal(sent.resource.backend, "neo4j");
        node_assert_1.strict.equal(sent.resource.message_type, LOCK);
        node_assert_1.strict.equal(sent.read_only, true);
        node_assert_1.strict.equal(sent.limit, 5);
        node_assert_1.strict.equal(sent.traversal.start_label, "locks", "the label falls back to the table name");
        node_assert_1.strict.equal(sent.traversal.start_id, "n1");
        node_assert_1.strict.equal(sent.traversal.direction, "GRAPH_TRAVERSAL_DIRECTION_OUTGOING");
        node_assert_1.strict.deepEqual(sent.traversal.relationship_types, ["RELATED", "KNOWS"]);
        node_assert_1.strict.equal(sent.traversal.min_depth, 1);
        node_assert_1.strict.equal(sent.traversal.max_depth, 2);
        node_assert_1.strict.deepEqual(sent.traversal.node_labels, ["Note"]);
        node_assert_1.strict.deepEqual({ ...sent.traversal.node_property_equals }, { archived: "false" });
        node_assert_1.strict.deepEqual({ ...sent.traversal.relationship_property_equals }, { kind: "peer" });
        node_assert_1.strict.equal(sent.traversal.return_relationships, true);
        await (0, graphVector_1.graphFrom)(udb, LOCK, "n1", { label: "Custom" }).in("A").run();
        const incoming = last("GraphQuery").request.traversal;
        node_assert_1.strict.equal(incoming.start_label, "Custom");
        node_assert_1.strict.equal(incoming.direction, "GRAPH_TRAVERSAL_DIRECTION_INCOMING");
        await (0, graphVector_1.graphFrom)(udb, LOCK, "n1").both().run();
        node_assert_1.strict.equal(last("GraphQuery").request.traversal.direction, "GRAPH_TRAVERSAL_DIRECTION_BOTH");
        const before = fixture.seen.length;
        await node_assert_1.strict.rejects((0, graphVector_1.graphFrom)(udb, LOCK, "").run(), /needs a start node id/);
        node_assert_1.strict.equal(fixture.seen.length, before, "a traversal without a start must not reach the broker");
        node_assert_1.strict.throws(() => (0, graphVector_1.graphFrom)(udb, "no.such.Message", "n1"), /no\.such\.Message/);
    }
    finally {
        udb.close();
    }
});
(0, node_test_1.test)("vectorsHybrid defaults, builds the request and decodes hits", async () => {
    reset();
    const udb = project();
    try {
        const hits = await (0, graphVector_1.vectorsHybrid)(udb, "notes")
            .text("rotate keys")
            .near([1, 2])
            .weights(0.75, 0.25)
            .filter({ owner: "me" })
            .limit(3)
            .run();
        node_assert_1.strict.deepEqual(hits, [{ id: "p1", score: 0.5, row: { title: "rotate" }, payload: { title: "rotate" } }]);
        const sent = last("VectorHybridSearch").request;
        node_assert_1.strict.equal(sent.collection, "notes");
        node_assert_1.strict.equal(sent.text_query, "rotate keys");
        node_assert_1.strict.deepEqual(sent.vector, [1, 2]);
        node_assert_1.strict.deepEqual(sent.fusion_weights, [0.75, 0.25]);
        node_assert_1.strict.equal(sent.limit, 3);
        node_assert_1.strict.equal(sent.with_payload, true);
        node_assert_1.strict.equal(sent.filter.fields.owner.stringValue ?? sent.filter.fields.owner.string_value, "me");
        node_assert_1.strict.equal((0, graphVector_1.vectorsHybrid)(udb, "notes").request().limit, 10, "the default limit is 10");
        node_assert_1.strict.equal((0, graphVector_1.vectorsHybrid)(udb, "notes").request().with_payload, true);
        const decoded = await (0, graphVector_1.vectorsHybrid)(udb, "notes", {
            decode: (payload) => ({ t: String(payload.title) }),
        })
            .text("x")
            .run();
        node_assert_1.strict.deepEqual(decoded[0].row, { t: "rotate" });
        await node_assert_1.strict.rejects((0, graphVector_1.vectorsHybrid)(udb, "notes", {
            decode: () => {
                throw new Error("bad row");
            },
        })
            .text("x")
            .run(), /vector hit p1: bad row/);
        const before = fixture.seen.length;
        await node_assert_1.strict.rejects((0, graphVector_1.vectorsHybrid)(udb, "notes").run(), /needs near\(vector\) and\/or text\(query\)/);
        node_assert_1.strict.equal(fixture.seen.length, before, "an empty search must not reach the broker");
    }
    finally {
        udb.close();
    }
});
// ── 7. Flags and locks ──────────────────────────────────────────────────────
(0, node_test_1.test)("flags evaluate for the connected scope; absent and non-boolean flags are off", async () => {
    reset();
    const udb = project();
    try {
        for (let i = 0; i < 2; i += 1) {
            node_assert_1.strict.equal(await udb.flags.enabled("on", { user_id: "u1" }), true);
        }
        const sent = last("EvaluateFlags").request;
        node_assert_1.strict.equal(sent.tenant_id, TENANT);
        node_assert_1.strict.deepEqual(sent.keys, ["on"]);
        node_assert_1.strict.equal(sent.context.project_id, PROJECT);
        node_assert_1.strict.deepEqual({ ...sent.context.attributes }, { user_id: "u1" });
        node_assert_1.strict.equal(await udb.flags.enabled("off"), false);
        node_assert_1.strict.equal(await udb.flags.enabled("missing"), false, "a flag that does not exist is off");
        node_assert_1.strict.equal(await udb.flags.enabled("text"), false, "a string flag is not on");
        const values = await udb.flags.evaluate(["on", "missing"]);
        node_assert_1.strict.deepEqual(Object.keys(values), ["on"], "unknown keys are absent");
        node_assert_1.strict.equal(values.on.bool_value, true);
    }
    finally {
        udb.close();
    }
});
(0, node_test_1.test)("lock defaults the tenant and never overrides an explicit one", async () => {
    reset();
    const udb = project();
    try {
        const request = { lock_name: "l", owner_id: "o", lease_ttl_seconds: 30 };
        await udb.lock.acquire(request);
        node_assert_1.strict.equal(last("AcquireLock").request.tenant_id, TENANT);
        node_assert_1.strict.equal("tenant_id" in request, false, "the caller's request is not changed");
        await udb.lock.acquire({ ...request, tenant_id: OTHER_TENANT });
        node_assert_1.strict.equal(last("AcquireLock").request.tenant_id, OTHER_TENANT);
        await udb.lock.list();
        node_assert_1.strict.equal(last("ListLocks").request.tenant_id, TENANT);
    }
    finally {
        udb.close();
    }
});
// ── 8. Schema reflection ────────────────────────────────────────────────────
const APIKEY = "udb.core.apikey.entity.v1.ApiKey";
const APIKEY_CREATED = "udb.core.apikey.events.v1.ApiKeyCreatedEvent";
(0, node_test_1.test)("schema reads the annotations of the shipped protos", () => {
    for (let i = 0; i < 2; i += 1) {
        node_assert_1.strict.equal((0, schema_1.topic)(APIKEY_CREATED), "udb.apikey.created.v1");
        node_assert_1.strict.equal((0, schema_1.eventType)(APIKEY_CREATED), "udb.core.apikey.events.v1.ApiKeyCreatedEvent");
        node_assert_1.strict.equal((0, schema_1.partitionKeyField)(APIKEY_CREATED), "tenant_id");
    }
    node_assert_1.strict.equal((0, schema_1.topic)(LOCK), "", "a message with no contract has no topic");
    node_assert_1.strict.equal((0, schema_1.eventType)(LOCK), "");
    node_assert_1.strict.deepEqual((0, schema_1.primaryKeys)(LOCK), ["lock_id"]);
    node_assert_1.strict.equal((0, schema_1.primaryKey)(LOCK), "lock_id");
    node_assert_1.strict.deepEqual((0, schema_1.uniqueKeys)(LOCK), [["tenant_id", "lock_name"]]);
    node_assert_1.strict.equal((0, schema_1.tenantColumn)(LOCK), "tenant_id");
    node_assert_1.strict.equal((0, schema_1.projectColumn)(LOCK), "");
    node_assert_1.strict.equal((0, schema_1.maxLen)(LOCK, "lock_name"), 255);
    node_assert_1.strict.equal((0, schema_1.maxLen)(LOCK, "lock_id"), 0, "a UUID column has no VARCHAR bound");
    node_assert_1.strict.equal((0, schema_1.maxLen)(LOCK, "no_such_field"), 0);
    node_assert_1.strict.equal((0, schema_1.softDelete)(APIKEY), true);
    node_assert_1.strict.equal((0, schema_1.softDelete)(LOCK), false);
    node_assert_1.strict.equal((0, schema_1.retentionClass)(LOCK), "lock.operational");
    node_assert_1.strict.equal((0, schema_1.exportEligible)(LOCK), false);
    node_assert_1.strict.deepEqual((0, schema_1.ownerColumns)(LOCK), []);
    node_assert_1.strict.equal((0, schema_1.ownerColumn)(LOCK), "");
    node_assert_1.strict.equal((0, schema_1.graphLabel)(LOCK), "locks");
    node_assert_1.strict.throws(() => (0, schema_1.topic)("no.such.Message"), /no\.such\.Message/);
    node_assert_1.strict.throws(() => (0, schema_1.uniqueKeys)("no.such.Message"), /no\.such\.Message/);
});
/** A proto tree of one message type per test, importing the SDK's annotations. */
function schemaFixture(tableOptions, fields) {
    const protoRoot = fs.mkdtempSync(path.join(os.tmpdir(), "udb-schema-"));
    const pkg = `fixture${path.basename(protoRoot).replace(/[^a-zA-Z0-9]/g, "")}.v1`;
    fs.writeFileSync(path.join(protoRoot, "fixture.proto"), `syntax = "proto3";
package ${pkg};
import "udb/core/common/v1/db.proto";
import "udb/core/common/v1/security.proto";
message Row {
  option (udb.core.common.v1.table) = { table_name: "rows" is_table: true ${tableOptions} };
  ${fields}
}
`);
    return { protoRoot, messageType: `${pkg}.Row` };
}
const COLUMN = (extra) => `[(udb.core.common.v1.column) = { ${extra} }]`;
const ROW_ID = `string row_id = 1 ${COLUMN('sql_type: "UUID" primary_key: true')};`;
(0, node_test_1.test)("primaryKey is empty for a composite or absent key", () => {
    const composite = schemaFixture("", `string a = 1 ${COLUMN('sql_type: "UUID" primary_key: true')};
    string b = 2 ${COLUMN('sql_type: "UUID" primary_key: true')};`);
    node_assert_1.strict.deepEqual((0, schema_1.primaryKeys)(composite.messageType, composite), ["a", "b"]);
    node_assert_1.strict.equal((0, schema_1.primaryKey)(composite.messageType, composite), "", "a composite key is not one key");
    const none = schemaFixture("", `string a = 1 ${COLUMN('sql_type: "UUID"')};`);
    node_assert_1.strict.deepEqual((0, schema_1.primaryKeys)(none.messageType, none), []);
    node_assert_1.strict.equal((0, schema_1.primaryKey)(none.messageType, none), "");
});
(0, node_test_1.test)("uniqueKeys follows the declared constraints", () => {
    const plain = schemaFixture(`
    indexes: { index_name: "z_composite" unique: true columns: "owner_id" columns: "slug" }
    indexes: { index_name: "a_partial" unique: true columns: "email" where_clause: "deleted_at IS NULL" }
    indexes: { index_name: "b_expression" unique: true columns: "lower(email)" }
    indexes: { index_name: "c_not_unique" columns: "slug" }
    indexes: { index_name: "d_duplicate" unique: true columns: "slug,owner_id" }
    indexes: { index_name: "e_primary" unique: true columns: "row_id" }`, `${ROW_ID}
    string owner_id = 2 ${COLUMN('sql_type: "UUID"')};
    string slug = 3 ${COLUMN('sql_type: "VARCHAR(64)"')};
    string email = 4 ${COLUMN('sql_type: "TEXT" unique: true')};
    string code = 5 ${COLUMN('sql_type: "TEXT" index: { index_name: "f_code" unique: true columns: "owner_id" }')};`);
    node_assert_1.strict.deepEqual((0, schema_1.uniqueKeys)(plain.messageType, plain), [["code", "owner_id"], ["email"], ["owner_id", "slug"]]);
});
(0, node_test_1.test)("uniqueKeys adds the partition column and matches aliases by the parser's naming", () => {
    const partitioned = schemaFixture(`partition_strategy: PARTITION_STRATEGY_RANGE_MONTH partition_column: "created_on"
     indexes: { index_name: "a" unique: true columns: "SlugName" }`, `${ROW_ID}
    string slug_name = 2 ${COLUMN('sql_type: "TEXT"')};
    string created_on = 3 ${COLUMN('sql_type: "DATE"')};`);
    node_assert_1.strict.deepEqual((0, schema_1.uniqueKeys)(partitioned.messageType, partitioned), [["slug_name", "created_on"]]);
});
(0, node_test_1.test)("uniqueKeys refuses unknown, ambiguous and repeated columns", () => {
    const unknown = schemaFixture(`indexes: { index_name: "a" unique: true columns: "missing" }`, ROW_ID);
    node_assert_1.strict.throws(() => (0, schema_1.uniqueKeys)(unknown.messageType, unknown), /column "missing" resolves to 0 fields/);
    const ambiguous = schemaFixture(`indexes: { index_name: "a" unique: true columns: "slug" }`, `${ROW_ID}
    string slug = 2 ${COLUMN('sql_type: "TEXT"')};
    string other = 3 ${COLUMN('column_name: "slug" sql_type: "TEXT"')};`);
    node_assert_1.strict.throws(() => (0, schema_1.uniqueKeys)(ambiguous.messageType, ambiguous), /column "slug" resolves to 2 fields/);
    const repeated = schemaFixture(`indexes: { index_name: "a" unique: true columns: "slug" columns: "slug" }`, `${ROW_ID}
    string slug = 2 ${COLUMN('sql_type: "TEXT"')};`);
    node_assert_1.strict.throws(() => (0, schema_1.uniqueKeys)(repeated.messageType, repeated), /repeats field "slug"/);
});
(0, node_test_1.test)("applyColumnDefaults sets bool and integer defaults and refuses a bad one", () => {
    const good = schemaFixture("", `${ROW_ID}
    bool active = 2 ${COLUMN('sql_type: "BOOLEAN" default_value: "true"')};
    bool hidden = 3 ${COLUMN('sql_type: "BOOLEAN" default_value: "F"')};
    int32 tries = 4 ${COLUMN('sql_type: "INTEGER" default_value: " 3 "')};
    int64 big = 5 ${COLUMN('sql_type: "BIGINT" default_value: "9007199254740993"')};
    sint64 small = 6 ${COLUMN('sql_type: "BIGINT" default_value: "-5"')};
    string label = 7 ${COLUMN('sql_type: "TEXT" default_value: "x"')};
    repeated int32 many = 8 ${COLUMN('sql_type: "INTEGER[]" default_value: "1"')};
    int32 plain = 9 ${COLUMN('sql_type: "INTEGER"')};`);
    const row = { active: false, plain: 9 };
    node_assert_1.strict.equal((0, schema_1.applyColumnDefaults)(good.messageType, row, good), row);
    node_assert_1.strict.deepEqual(row, {
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
        node_assert_1.strict.throws(() => (0, schema_1.applyColumnDefaults)(bad.messageType, {}, bad), /broken default/, `${kind} ${value}`);
    }
});
(0, node_test_1.test)("graphLabel prefers the graph data store's names", () => {
    const store = (body) => {
        const protoRoot = fs.mkdtempSync(path.join(os.tmpdir(), "udb-schema-"));
        const pkg = `graph${path.basename(protoRoot).replace(/[^a-zA-Z0-9]/g, "")}.v1`;
        fs.writeFileSync(path.join(protoRoot, "fixture.proto"), `syntax = "proto3";
package ${pkg};
import "udb/core/common/v1/db.proto";
message Row {
  option (udb.core.common.v1.table) = { table_name: "rows" };
  option (udb.core.common.v1.data_store) = { ${body} };
  string id = 1;
}
`);
        return (0, schema_1.graphLabel)(`${pkg}.Row`, { protoRoot });
    };
    node_assert_1.strict.equal(store('backend: "neo4j" options: { key: "udb.neo4j_label" value: "Primary" } options: { key: "node_label" value: "Second" }'), "Primary");
    node_assert_1.strict.equal(store('store_kind: "graph" options: { key: "node_label" value: "Second" }'), "Second");
    node_assert_1.strict.equal(store('backend: "neo4j" resource_name: "Named"'), "Named");
    node_assert_1.strict.equal(store('backend: "qdrant" resource_name: "Named"'), "rows", "another store's name does not apply");
});
// ── 9. Credential introspection ─────────────────────────────────────────────
(0, node_test_1.test)("principal and bearerExpiresAt report an exchanged key and nothing else", async () => {
    reset();
    const raw = project({ credentials: { bearerToken: "raw" } });
    try {
        node_assert_1.strict.equal(raw.principal(), null, "a raw bearer has no verified principal");
        node_assert_1.strict.equal(raw.bearerExpiresAt(), null);
        node_assert_1.strict.equal(raw.bearer(), "Bearer raw");
        node_assert_1.strict.equal(raw.credentialError(), null);
        node_assert_1.strict.throws(() => raw.verify({ tenantId: TENANT }), /no verified principal/);
        raw.verify();
    }
    finally {
        raw.close();
    }
    node_assert_1.strict.match(raw.credentialError()?.message ?? "", /closed/);
    const udb = await project_1.UdbProject.connect({
        target: fixture.target,
        tenantId: TENANT,
        purpose: "p",
        deadlineMs: 5_000,
        credentials: { apiKey: "key-1" },
    });
    try {
        const principal = udb.principal();
        node_assert_1.strict.equal(principal.tenant_id, TENANT);
        principal.tenant_id = "changed";
        principal.scopes.push("changed");
        node_assert_1.strict.equal(udb.principal().tenant_id, TENANT, "the accessor returns a copy");
        node_assert_1.strict.equal(udb.principal().scopes.includes("changed"), false);
        const expires = udb.bearerExpiresAt();
        node_assert_1.strict.ok(expires && expires.getTime() > Date.now(), "the exchanged bearer's expiry is known");
        udb.verify({ tenantId: TENANT, projectId: PROJECT, requiredScopes: ["udb:read"] });
        await udb.logout();
        node_assert_1.strict.equal(udb.principal(), null);
        node_assert_1.strict.equal(udb.bearerExpiresAt(), null);
    }
    finally {
        udb.close();
    }
});
// ── 10. Delete mode ─────────────────────────────────────────────────────────
(0, node_test_1.test)("deleteFile reaches the wire with an explicit mode through its request form", async () => {
    const calls = [];
    const storage = new project_1.StorageFacade({
        unary: (_service, method, request) => {
            calls.push({ method, request });
            return Promise.resolve({});
        },
    });
    await storage.deleteFile({ tenant_id: TENANT, file_id: "f1", mode: "DELETE_MODE_HARD", reason: "gdpr" });
    node_assert_1.strict.deepEqual(calls[0], {
        method: "DeleteFile",
        request: { tenant_id: TENANT, file_id: "f1", mode: "DELETE_MODE_HARD", reason: "gdpr" },
    });
});
(0, node_test_1.test)("string file ids carry the connected tenant; an explicit tenant is never replaced", async () => {
    const calls = [];
    const core = {
        opts: { meta: { tenantId: TENANT } },
        unary: (_service, method, request) => {
            calls.push({ method, request });
            return Promise.resolve({});
        },
    };
    const storage = new project_1.StorageFacade(core);
    await storage.deleteFile("f1");
    node_assert_1.strict.deepEqual(calls.at(-1), { method: "DeleteFile", request: { file_id: "f1", tenant_id: TENANT } });
    await storage.deleteFile({ file_id: "f1", mode: "DELETE_MODE_SOFT" });
    node_assert_1.strict.equal(calls.at(-1).request.tenant_id, TENANT);
    await storage.deleteFile({ tenant_id: OTHER_TENANT, file_id: "f1" });
    node_assert_1.strict.equal(calls.at(-1).request.tenant_id, OTHER_TENANT, "an explicit tenant stays");
    await storage.deleteFileMode("f1", "DELETE_MODE_HARD", "gdpr");
    node_assert_1.strict.deepEqual(calls.at(-1).request, { file_id: "f1", mode: "DELETE_MODE_HARD", reason: "gdpr", tenant_id: TENANT });
    await storage.getFile("f1");
    node_assert_1.strict.equal(calls.at(-1).request.tenant_id, TENANT);
    await storage.getDownloadUrl("f1");
    node_assert_1.strict.equal(calls.at(-1).request.tenant_id, TENANT);
    const request = { file_id: "f1" };
    await storage.deleteFile(request);
    node_assert_1.strict.equal("tenant_id" in request, false, "the caller's request is not changed");
});
// ── Credential renewal ──────────────────────────────────────────────────────
const refused = () => {
    throw Object.assign(new Error("invalid credential"), { code: grpc.status.UNAUTHENTICATED });
};
/** A real connectEnterprise session whose renewal RPCs follow `script`. */
async function renewalSession(script) {
    reset();
    fixture.principal = principalFixture({ principal_id: "p1", subject: "s1", account_kind: "ACCOUNT_KIND_PERSON", user_id: "u1" });
    const session = await project_1.UdbProject.connectEnterprise({
        target: fixture.target,
        username: "admin",
        password: "secret",
        tenantCode: "acme",
        purpose: "parity-test",
        deadlineMs: 5_000,
    });
    const auth = session.auth;
    const calls = [];
    const principal = session.principal();
    const view = {
        refreshToken: async (request) => {
            calls.push(`refresh:${request.refresh_token}`);
            return (script.refresh ?? (() => ({ access_token: "refreshed", refresh_token: "r2", access_token_expires_in: "600" })))(request);
        },
        login: async (request) => {
            calls.push(`login:${request.username}:${request.tenant_hint}`);
            return (script.login ?? (() => ({ access_token: "relogged", refresh_token: "r3", session_id: "s3", access_token_expires_in: "600" })))(request);
        },
        authenticateBearer: async (token) => {
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
(0, node_test_1.test)("a refused refresh logs in again and later renewals use the new token", async () => {
    const { session, calls, expire } = await renewalSession({ refresh: refused });
    try {
        const renewed = await session.refreshIfNeeded();
        node_assert_1.strict.equal(renewed.accessToken, "relogged");
        node_assert_1.strict.deepEqual(calls, ["refresh:refresh-1", "login:admin:acme", "verify:relogged"]);
        node_assert_1.strict.equal(session.bearer(), "Bearer relogged");
        node_assert_1.strict.equal(renewed.refreshToken, "r3", "the new login's refresh token is kept");
        node_assert_1.strict.equal(session.credentialError(), null);
        await expire();
        calls.length = 0;
        await session.refreshIfNeeded();
        node_assert_1.strict.deepEqual(calls.slice(0, 1), ["refresh:r3"], "the next renewal refreshes with the new login's token");
    }
    finally {
        session.close();
    }
});
(0, node_test_1.test)("a healthy refresh is verified and does not log in", async () => {
    const { session, calls, expire } = await renewalSession({});
    try {
        for (let i = 0; i < 3; i += 1) {
            const renewed = await session.refreshIfNeeded();
            node_assert_1.strict.equal(renewed.accessToken, "refreshed");
            await expire();
        }
        node_assert_1.strict.equal(calls.filter((call) => call.startsWith("login")).length, 0);
        node_assert_1.strict.equal(calls.filter((call) => call.startsWith("verify")).length, 3, "each bearer is verified");
    }
    finally {
        session.close();
    }
});
(0, node_test_1.test)("concurrent renewals share one refresh and one login", async () => {
    const { session, calls } = await renewalSession({ refresh: refused });
    try {
        const results = await Promise.all([session.refreshIfNeeded(), session.refreshIfNeeded(), session.refreshIfNeeded()]);
        node_assert_1.strict.ok(results.every((token) => token.accessToken === "relogged"));
        node_assert_1.strict.equal(calls.filter((call) => call.startsWith("login")).length, 1);
    }
    finally {
        session.close();
    }
});
(0, node_test_1.test)("a failed re-login keeps the spent token from being resubmitted", async () => {
    let loginWorks = false;
    const { session, calls } = await renewalSession({
        refresh: refused,
        login: () => {
            if (!loginWorks)
                throw Object.assign(new Error("password changed"), { code: grpc.status.UNAUTHENTICATED });
            return { access_token: "relogged", refresh_token: "r3", access_token_expires_in: "600" };
        },
    });
    try {
        await node_assert_1.strict.rejects(session.refreshIfNeeded(), /renewal and re-login failed: invalid credential; password changed/);
        node_assert_1.strict.notEqual(session.bearer(), "Bearer relogged");
        calls.length = 0;
        loginWorks = true;
        node_assert_1.strict.equal((await session.refreshIfNeeded()).accessToken, "relogged");
        node_assert_1.strict.deepEqual(calls, ["login:admin:acme", "verify:relogged"], "no second submission of the spent refresh token");
    }
    finally {
        session.close();
    }
});
(0, node_test_1.test)("a re-login needing MFA or returning no bearer is refused", async () => {
    for (const reply of [{ mfa_required: true }, {}]) {
        const { session } = await renewalSession({ refresh: refused, login: () => reply });
        try {
            await node_assert_1.strict.rejects(session.refreshIfNeeded(), /re-login returned no bearer/);
        }
        finally {
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
]) {
    (0, node_test_1.test)(`a refresh whose principal changed ${name} fails closed with no re-login`, async () => {
        let principalNow;
        const { session, calls, principal } = await renewalSession({ authenticate: () => ({ principal: principalNow }) });
        principalNow = { ...principal, ...change };
        try {
            await node_assert_1.strict.rejects(session.refreshIfNeeded(), (err) => err instanceof project_1.CredentialBindingError && message.test(err.message));
            node_assert_1.strict.equal(calls.filter((call) => call.startsWith("login")).length, 0, "a binding failure is not retried");
            node_assert_1.strict.equal(session.bearer(), "", "the bearer is dropped");
            node_assert_1.strict.equal((await session.currentToken()).accessToken, "login-bearer", "the rejected bearer is not stored");
            node_assert_1.strict.match(session.credentialError()?.message ?? "", message);
            node_assert_1.strict.throws(() => session.asUser(USER_BEARER), /session not authorized/);
            calls.length = 0;
            principalNow = principal;
            await node_assert_1.strict.rejects(session.refreshIfNeeded(), project_1.CredentialBindingError, "the refusal is permanent");
            node_assert_1.strict.deepEqual(calls, [], "nothing more reaches the broker");
        }
        finally {
            session.close();
        }
    });
}
(0, node_test_1.test)("scope order and duplicates are not a change; a re-login is pinned too", async () => {
    let principalNow;
    const { session, principal } = await renewalSession({ authenticate: () => ({ principal: principalNow }) });
    principalNow = { ...principal, scopes: [...principal.scopes].reverse().concat(principal.scopes[0]) };
    try {
        node_assert_1.strict.equal((await session.refreshIfNeeded()).accessToken, "refreshed");
    }
    finally {
        session.close();
    }
    const second = await renewalSession({
        refresh: refused,
        authenticate: () => ({ principal: { ...second.principal, principal_id: "attacker" } }),
    });
    try {
        await node_assert_1.strict.rejects(second.session.refreshIfNeeded(), project_1.CredentialBindingError);
        node_assert_1.strict.equal(second.session.bearer(), "");
    }
    finally {
        second.session.close();
    }
});
(0, node_test_1.test)("an explicit login replaces the pin and stops re-login with the old credentials", async () => {
    const { session, calls } = await renewalSession({ refresh: refused });
    try {
        await session.login({ username: "someone", password: "else" });
        await session.tokenStore.save({ accessToken: "x", refreshToken: "r", expiresAt: Date.now() - 1_000 });
        calls.length = 0;
        await session.refreshIfNeeded().catch(() => undefined);
        node_assert_1.strict.equal(calls.length, 0, "the enterprise renewal path must be gone");
    }
    finally {
        session.close();
    }
});
(0, node_test_1.test)("an API key re-exchange that changes the principal fails closed and adopts nothing", async () => {
    reset();
    const udb = await project_1.UdbProject.connect({
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
            node_assert_1.strict.equal((await udb.refreshIfNeeded()).accessToken, SERVICE_BEARER, "the same principal may renew");
        }
        node_assert_1.strict.equal(udb.credentialError(), null);
        fixture.principal = principalFixture({ scopes: ["udb:read", "svc:scope", "udb:admin"] });
        await expire();
        await node_assert_1.strict.rejects(udb.refreshIfNeeded(), (err) => err instanceof project_1.CredentialBindingError && /changed scopes/.test(err.message));
        node_assert_1.strict.equal(udb.bearer(), "");
        node_assert_1.strict.deepEqual(udb.principal().scopes, ["udb:read", "svc:scope"], "the refused principal is not adopted");
        node_assert_1.strict.deepEqual(udb.sharedMeta.scopes, ["udb:read", "svc:scope"]);
        await node_assert_1.strict.rejects(udb.ready(), /credential refresh is failing/);
        await node_assert_1.strict.rejects(udb.refreshIfNeeded(), project_1.CredentialBindingError);
        fixture.principal = principalFixture({ scopes: ["udb:admin"] });
        await udb.authenticateApiKeyAndAdopt("key-1");
        node_assert_1.strict.equal(udb.bearer(), `Bearer ${SERVICE_BEARER}`, "an explicit adoption chooses the identity again");
    }
    finally {
        udb.close();
    }
});
(0, node_test_1.test)("a first API key exchange without a principal is refused", async () => {
    const original = fixture.authenticate;
    fixture.authenticate = () => ({ access_token: "x", expires_at_unix: "9999999999" });
    try {
        await node_assert_1.strict.rejects(project_1.UdbProject.connect({ target: fixture.target, tenantId: TENANT, purpose: "p", credentials: { apiKey: "k" } }), /returned no verified principal/);
    }
    finally {
        fixture.authenticate = original;
    }
});
const passthrough = {
    requestSerialize: (value) => Buffer.from(JSON.stringify(value)),
    requestDeserialize: (value) => JSON.parse(value.toString() || "{}"),
    responseSerialize: (value) => Buffer.from(JSON.stringify(value)),
    responseDeserialize: (value) => JSON.parse(value.toString() || "{}"),
};
const ECHO_SERVICE = {
    Unary: { path: "/echo/Unary", requestStream: false, responseStream: false, ...passthrough },
    Server: { path: "/echo/Server", requestStream: false, responseStream: true, ...passthrough },
    Client: { path: "/echo/Client", requestStream: true, responseStream: false, ...passthrough },
    Bidi: { path: "/echo/Bidi", requestStream: true, responseStream: true, ...passthrough },
};
function identity() {
    return {
        tenant: (0, grpc_1.requestTenantId)(),
        user: (0, grpc_1.requestUserId)(),
        correlation: (0, grpc_1.requestCorrelationId)(),
        meta: (0, grpc_1.requestMetadata)(),
    };
}
async function echoServer(options) {
    const server = new grpc.Server(options);
    server.addService(ECHO_SERVICE, {
        // An await before reading proves the identity follows the handler across it.
        Unary: async (_call, callback) => {
            await new Promise((resolve) => setTimeout(resolve, Math.random() * 10));
            callback(null, identity());
        },
        Server: async (call) => {
            call.write(identity());
            await new Promise((resolve) => setImmediate(resolve));
            call.write(identity());
            call.end();
        },
        Client: (call, callback) => {
            call.on("data", () => undefined);
            call.on("end", () => callback(null, identity()));
        },
        Bidi: (call) => {
            call.on("data", () => call.write(identity()));
            call.on("end", () => call.end());
        },
    });
    const port = await new Promise((resolve, reject) => server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, bound) => err ? reject(err) : resolve(bound)));
    const client = new (grpc.makeGenericClientConstructor(ECHO_SERVICE, "Echo"))(`127.0.0.1:${port}`, grpc.credentials.createInsecure());
    return { server, client };
}
function sdkHeaders(tenant) {
    return (0, client_1.metadata)({
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
(0, node_test_1.test)("the server interceptor gives unary and streaming handlers the request's identity", async () => {
    const { server, client } = await echoServer({ interceptors: [grpc_1.udbServerInterceptor] });
    try {
        const unary = (tenant) => new Promise((resolve, reject) => client.Unary({}, sdkHeaders(tenant), (err, reply) => (err ? reject(err) : resolve(reply))));
        // Concurrent requests must not see each other's identity.
        const replies = await Promise.all(["t1", "t2", "t3", "t4"].map(unary));
        replies.forEach((reply, i) => {
            const tenant = `t${i + 1}`;
            node_assert_1.strict.equal(reply.tenant, tenant);
            node_assert_1.strict.equal(reply.user, `user-of-${tenant}`);
            node_assert_1.strict.equal(reply.correlation, `corr-${tenant}`);
        });
        node_assert_1.strict.deepEqual(replies[0].meta, {
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
        const frames = [];
        for await (const frame of serverStream)
            frames.push(frame);
        node_assert_1.strict.deepEqual(frames.map((frame) => frame.tenant), ["s1", "s1"]);
        const clientStreamReply = await new Promise((resolve, reject) => {
            const call = client.Client(sdkHeaders("c1"), (err, reply) => (err ? reject(err) : resolve(reply)));
            call.write({});
            call.end();
        });
        node_assert_1.strict.equal(clientStreamReply.tenant, "c1");
        const bidiFrames = [];
        const bidi = client.Bidi(sdkHeaders("b1"));
        bidi.write({});
        bidi.end();
        for await (const frame of bidi)
            bidiFrames.push(frame);
        node_assert_1.strict.equal(bidiFrames[0].tenant, "b1");
    }
    finally {
        client.close();
        server.forceShutdown();
    }
});
(0, node_test_1.test)("without the interceptor or outside a request there is no identity", async () => {
    node_assert_1.strict.equal((0, grpc_1.requestMetadata)(), undefined);
    node_assert_1.strict.equal((0, grpc_1.requestTenantId)(), "");
    node_assert_1.strict.equal((0, grpc_1.requestUserId)(), "");
    node_assert_1.strict.equal((0, grpc_1.requestCorrelationId)(), "");
    node_assert_1.strict.equal((0, grpc_1.withRequestMetadata)({ tenantId: "t", purpose: "", correlationId: "c" }, grpc_1.requestTenantId), "t");
    const { server, client } = await echoServer({});
    try {
        const reply = await new Promise((resolve, reject) => client.Unary({}, sdkHeaders("t1"), (err, value) => (err ? reject(err) : resolve(value))));
        node_assert_1.strict.equal(reply.tenant, "");
    }
    finally {
        client.close();
        server.forceShutdown();
    }
});
(0, node_test_1.test)("metadataFromGrpc falls back to the request id and drops empty scopes", () => {
    const md = new grpc.Metadata();
    md.set("x-request-id", "req-1");
    md.set("x-scopes", ",a,,b,");
    const meta = (0, grpc_1.metadataFromGrpc)(md);
    node_assert_1.strict.equal(meta.correlationId, "req-1");
    node_assert_1.strict.deepEqual(meta.scopes, ["a", "b"]);
    node_assert_1.strict.equal(meta.tenantId, "");
    node_assert_1.strict.equal((0, grpc_1.metadataFromGrpc)(new grpc.Metadata()).scopes, undefined);
    md.set("x-correlation-id", "corr-1");
    node_assert_1.strict.equal((0, grpc_1.metadataFromGrpc)(md).correlationId, "corr-1");
});
