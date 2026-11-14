// UdbProject.forTenant (parity with the Go SDK's tenant_exchange_test.go). A
// service connected to its home tenant gets a client in a member tenant whose
// short-lived exchanged token (no refresh token) is exchanged again before it
// expires, and at once when the broker refuses it; while that re-exchange is
// refused calls fail locally. Run with Node's built-in runner over compiled JS:
// npx tsc -p tsconfig.test.json && node --test dist-test

import { strict as assert } from "node:assert";
import path from "node:path";
import { test } from "node:test";

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

import { UdbProject } from "./project";
import { defaultProtoRoot } from "./protoRoot";

const HOME_TENANT = "11111111-1111-4111-8111-111111111111";
const MEMBER_TENANT = "22222222-2222-4222-8222-222222222222";
const MEMBER_PROJECT = "33333333-3333-4333-8333-333333333333";
const SERVICE_USER = "44444444-4444-4444-8444-444444444444";
const CACHE = "udb.core.cache.services.v1.CacheService";

const root = defaultProtoRoot();
const loaded: any = grpc.loadPackageDefinition(
  protoLoader.loadSync(
    ["udb/core/authn/services/v1/authn_service.proto", "udb/core/cache/services/v1/cache_service.proto"],
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

function principal(tenant: string, project: string) {
  return { principal_id: "svc", subject: "svc", tenant_id: tenant, project_id: project, user_id: SERVICE_USER, scopes: [] };
}

function failure(code: grpc.status, message: string): grpc.ServiceError {
  return Object.assign(new Error(message), { code, details: message, metadata: new grpc.Metadata() });
}

function unixIn(ms: number): string {
  return String(Math.floor((Date.now() + ms) / 1000));
}

/** Issues a home bearer for an API key, mints member-tenant tokens for that home
 *  bearer, and serves an Increment that accepts only the newest member token. */
class ExchangeBroker {
  target = "";
  private server: grpc.Server | null = null;
  ttlMs = 3_600_000;
  homeBearer = "";
  memberToken = "";
  exchanges = 0;
  exchangeAttempts = 0;
  exchangeTenantHeaders: string[][] = [];
  incrementTenantHeaders: string[][] = [];
  refused = 0;
  refuseExchange = false;
  wrongTenant = "";

  async start(): Promise<void> {
    const server = new grpc.Server();
    const unary =
      (reply: (request: any, headers: grpc.Metadata) => any) =>
      (call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) => {
        try {
          callback(null, reply(call.request, call.metadata));
        } catch (err) {
          callback(err as grpc.ServiceError);
        }
      };
    server.addService(loaded.udb.core.authn.services.v1.AuthnService.service, {
      Authenticate: unary((request) => {
        if (request.api_key === "svc-key") {
          this.homeBearer = "home-key";
          return { access_token: this.homeBearer, expires_at_unix: unixIn(3_600_000), principal: principal(HOME_TENANT, "") };
        }
        if (String(request.bearer_token ?? "").startsWith("member-")) {
          return { expires_at_unix: unixIn(this.ttlMs), principal: principal(MEMBER_TENANT, MEMBER_PROJECT) };
        }
        throw failure(grpc.status.UNAUTHENTICATED, "unknown credential");
      }),
      ExchangeServiceTokenForTenant: unary((request, headers) => {
        this.exchangeAttempts += 1;
        this.exchangeTenantHeaders.push(headers.get("x-tenant-id").map(String));
        if (headers.get("authorization")[0] !== `Bearer ${this.homeBearer}`) {
          throw failure(grpc.status.UNAUTHENTICATED, "the exchange requires the home bearer");
        }
        if (this.refuseExchange) throw failure(grpc.status.PERMISSION_DENIED, "no active membership");
        this.exchanges += 1;
        this.memberToken = `member-${this.exchanges}`;
        return {
          access_token: this.memberToken,
          expires_at_unix: unixIn(this.ttlMs),
          tenant_id: this.wrongTenant || request.target_tenant_id,
          project_id: MEMBER_PROJECT,
          membership_id: "membership-1",
        };
      }),
    });
    server.addService(loaded.udb.core.cache.services.v1.CacheService.service, {
      Increment: unary((_request, headers) => {
        this.incrementTenantHeaders.push(headers.get("x-tenant-id").map(String));
        if (headers.get("authorization")[0] !== `Bearer ${this.memberToken}`) {
          this.refused += 1;
          throw failure(grpc.status.UNAUTHENTICATED, "invalid credential");
        }
        return { value: "1" };
      }),
    });
    const port = await new Promise<number>((resolve, reject) =>
      server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, bound) =>
        err ? reject(err) : resolve(bound),
      ),
    );
    this.target = `127.0.0.1:${port}`;
    this.server = server;
  }

  stop(): void {
    this.server?.forceShutdown();
    this.server = null;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function increment(udb: UdbProject): Promise<any> {
  return udb.core.unary(CACHE, "Increment", { namespace: "limits", key: "gateway", delta: 1 });
}

async function connectHome(broker: ExchangeBroker): Promise<UdbProject> {
  return UdbProject.connect({
    target: broker.target,
    tenantId: HOME_TENANT,
    purpose: "ts.test.for-tenant",
    deadlineMs: 2_000,
    credentials: { apiKey: "svc-key" },
  });
}

test("forTenant exchanges the home bearer and keeps the member token fresh", async () => {
  const broker = new ExchangeBroker();
  broker.ttlMs = 3_000;
  await broker.start();
  const home = await connectHome(broker);
  let member: UdbProject | null = null;
  try {
    member = await home.forTenant(MEMBER_TENANT, MEMBER_PROJECT);
    assert.equal(member.canonicalTenantId, MEMBER_TENANT);
    assert.equal(member.canonicalProjectId, MEMBER_PROJECT);
    assert.equal(member.principal()?.user_id, SERVICE_USER);
    assert.equal(member.bearer(), "Bearer member-1");
    assert.ok(member.bearerExpiresAt(), "the member client reports its token expiry");
    assert.equal(home.bearer(), "Bearer home-key", "forTenant must not change the home credential");

    for (let i = 0; i < 3; i += 1) await increment(member);
    assert.equal(broker.exchanges, 1, "three calls on a fresh token exchanged again");
    assert.deepEqual(broker.incrementTenantHeaders[0], [MEMBER_TENANT]);
    assert.deepEqual(broker.exchangeTenantHeaders[0], [HOME_TENANT]);

    // A 3s token renews at 80% of its life: a re-exchange before it expires.
    const deadline = Date.now() + 6_000;
    while (broker.exchanges < 2) {
      if (Date.now() > deadline) assert.fail("the member token was not exchanged again before it expired");
      await sleep(50);
    }
    await increment(member);
    assert.equal(member.bearer(), `Bearer member-${broker.exchanges}`);
    assert.equal(member.credentialError(), null);
    assert.equal(broker.refused, 0, "a member call carried a stale token");
  } finally {
    member?.close();
    home.close();
    broker.stop();
  }
});

test("forTenant: a refused member token fails closed while the re-exchange backs off", async () => {
  const broker = new ExchangeBroker();
  await broker.start();
  const home = await connectHome(broker);
  let member: UdbProject | null = null;
  try {
    member = await home.forTenant(MEMBER_TENANT);
    broker.memberToken = "revoked";
    broker.refuseExchange = true;
    await assert.rejects(increment(member), (e: any) => e.code === grpc.status.UNAUTHENTICATED);
    for (let i = 0; i < 20; i += 1) {
      await assert.rejects(increment(member), (e: any) => e.code === grpc.status.UNAUTHENTICATED);
    }
    assert.ok(member.credentialError(), "the refused member client reports its credential failure");
    assert.equal(broker.refused, 1, "the refused member token was resent");
    assert.ok(broker.exchangeAttempts <= 4, `${broker.exchangeAttempts} exchange attempts: one per call`);

    broker.refuseExchange = false;
    const deadline = Date.now() + 10_000;
    while (member.credentialError()) {
      if (Date.now() > deadline) assert.fail(`no recovery: ${member.credentialError()?.message}`);
      await sleep(50);
    }
    await increment(member);
  } finally {
    member?.close();
    home.close();
    broker.stop();
  }
});

test("forTenant refuses a token for another tenant and a non-canonical tenant id", async () => {
  const broker = new ExchangeBroker();
  broker.wrongTenant = "55555555-5555-4555-8555-555555555555";
  await broker.start();
  const home = await connectHome(broker);
  try {
    await assert.rejects(home.forTenant(MEMBER_TENANT), /returned a token for tenant/);
    for (const bad of ["", "acme", "ABCDEF01-2345-4678-89AB-CDEF01234567"]) {
      await assert.rejects(home.forTenant(bad), /canonical/);
    }
    assert.equal(broker.exchangeAttempts, 1, "a non-canonical tenant reached the broker");
  } finally {
    home.close();
    broker.stop();
  }
});
