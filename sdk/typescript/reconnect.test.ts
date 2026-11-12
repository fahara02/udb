// Recovery after a broker restart (parity with the Go SDK's reconnect_test.go).
// An in-process broker is stopped and started again on the same port; every
// start is a new generation that has lost the previous one's sessions, so the
// bearers and refresh tokens it issued are refused. The same client must keep
// working without being recreated, a credential the restarted broker refuses
// must fail closed without a sign-in per call, and a bearer the client does not
// own must never be renewed or resent. Run with Node's built-in runner over
// compiled JS: npx tsc -p tsconfig.test.json && node --test dist-test

import { strict as assert } from "node:assert";
import path from "node:path";
import { test } from "node:test";

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

import { UdbProject } from "./project";
import { defaultProtoRoot } from "./protoRoot";

const TENANT = "11111111-1111-4111-8111-111111111111";
const PROJECT = "22222222-2222-4222-8222-222222222222";
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

function principal() {
  return {
    tenant_id: TENANT,
    project_id: PROJECT,
    user_id: "user-1",
    principal_id: "principal-1",
    subject: "gateway",
    service_identity: "",
    scopes: ["udb:cache:increment"],
  };
}

function refusal(message: string): grpc.ServiceError {
  return Object.assign(new Error(message), {
    code: grpc.status.UNAUTHENTICATED,
    details: message,
    metadata: new grpc.Metadata(),
  });
}

/** An in-process broker that can be stopped and started again on the same port. */
class RestartBroker {
  target = "";
  private port = 0;
  private server: grpc.Server | null = null;
  gen = 0;
  logins = 0;
  exchanges = 0;
  refreshes = 0;
  increments = 0;
  refusedBearers: string[] = [];
  /** Makes Login, RefreshToken and the key exchange refuse. */
  refuseSignIn = false;

  private issued(kind: string, n: number): string {
    return `g${this.gen}-${kind}-${n}`;
  }

  private current(token: string): boolean {
    return token.startsWith(`g${this.gen}-`);
  }

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
      Login: unary(() => {
        this.logins += 1;
        if (this.refuseSignIn) throw refusal("invalid credential");
        return {
          access_token: this.issued("access", this.logins),
          refresh_token: this.issued("refresh", this.logins),
          session_id: this.issued("session", this.logins),
          access_token_expires_in: 3600,
        };
      }),
      Authenticate: unary((request) => {
        const expires = String(Math.floor(Date.now() / 1000) + 3600);
        if (request.api_key) {
          this.exchanges += 1;
          if (this.refuseSignIn) throw refusal("invalid API key");
          return { access_token: this.issued("key", this.exchanges), expires_at_unix: expires, principal: principal() };
        }
        return { expires_at_unix: expires, principal: principal() };
      }),
      RefreshToken: unary((request) => {
        this.refreshes += 1;
        if (this.refuseSignIn || !this.current(request.refresh_token ?? "")) {
          throw refusal("refresh token is not known");
        }
        return {
          access_token: this.issued("refreshed", this.refreshes),
          refresh_token: this.issued("refresh-next", this.refreshes),
          access_token_expires_in: 3600,
        };
      }),
    });
    server.addService(loaded.udb.core.cache.services.v1.CacheService.service, {
      // The request-limit counter of the incident: a mutation that is neither
      // read-only nor replay-safe. Authentication runs before it applies.
      Increment: unary((_request, headers) => {
        const bearer = String(headers.get("authorization")[0] ?? "").replace(/^Bearer /, "");
        if (!this.current(bearer)) {
          this.refusedBearers.push(bearer);
          throw refusal("invalid credential");
        }
        this.increments += 1;
        return { value: String(this.increments) };
      }),
    });
    const address = `127.0.0.1:${this.port}`;
    const deadline = Date.now() + 5_000;
    for (;;) {
      try {
        this.port = await new Promise<number>((resolve, reject) =>
          server.bindAsync(address, grpc.ServerCredentials.createInsecure(), (err, bound) =>
            err ? reject(err) : resolve(bound),
          ),
        );
        break;
      } catch (err) {
        if (Date.now() > deadline) throw err;
        await sleep(50);
      }
    }
    this.target = `127.0.0.1:${this.port}`;
    this.server = server;
  }

  /** Take the broker down: every connection is closed and nothing listens. */
  stop(): void {
    this.server?.forceShutdown();
    this.server = null;
  }

  /** Bring the broker back on the same port as a new generation. */
  async restart(): Promise<void> {
    this.gen += 1;
    await this.start();
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function newBroker(): Promise<RestartBroker> {
  const broker = new RestartBroker();
  await broker.start();
  return broker;
}

function increment(udb: UdbProject, call?: any): Promise<any> {
  return udb.core.unary(CACHE, "Increment", { namespace: "limits", key: "gateway", delta: 1 }, call);
}

/** Call Increment until the restarted broker answers. While the channel
 *  reconnects the call may fail UNAVAILABLE (a mutation is never repeated on its
 *  own); any other outcome is returned at once. */
async function incrementAfterRestart(udb: UdbProject, withinMs: number, call?: () => any): Promise<unknown> {
  const deadline = Date.now() + withinMs;
  for (;;) {
    try {
      await increment(udb, call?.());
      return null;
    } catch (err) {
      if ((err as { code?: unknown }).code !== grpc.status.UNAVAILABLE) return err;
      if (Date.now() > deadline) throw new Error(`broker did not become reachable again: ${String(err)}`);
      await sleep(50);
    }
  }
}

function connectEnterprise(broker: RestartBroker): Promise<UdbProject> {
  return UdbProject.connectEnterprise({
    target: broker.target,
    username: "gateway",
    password: "secret",
    tenantCode: "acme",
    purpose: "ts.test.restart",
    deadlineMs: 2_000,
  });
}

test("broker restart: the same enterprise session recovers without being recreated", async () => {
  const broker = await newBroker();
  const udb = await connectEnterprise(broker);
  try {
    await increment(udb);
    const before = udb.bearer();

    broker.stop();
    await assert.rejects(increment(udb), (err: any) => err.code === grpc.status.UNAVAILABLE);
    // Let the channel fail a few dials, growing its reconnect backoff.
    await sleep(1_500);
    await broker.restart();

    assert.equal(await incrementAfterRestart(udb, 10_000), null);
    assert.equal(broker.logins, 2, "the session signs in again after its refresh token was refused");
    assert.equal(broker.increments, 2, "the refused increment is neither applied nor repeated twice");
    assert.deepEqual(broker.refusedBearers.map((b) => `Bearer ${b}`), [before]);
    const after = udb.bearer();
    assert.notEqual(after, before);
    assert.ok(after.startsWith("Bearer g1-"), after);
    assert.equal(udb.credentialError(), null);
  } finally {
    udb.close();
    broker.stop();
  }
});

test("broker restart: a service account re-exchanges its key", async () => {
  const broker = await newBroker();
  const udb = await UdbProject.connect({
    target: broker.target,
    tenantId: TENANT,
    purpose: "ts.test.restart",
    deadlineMs: 2_000,
    credentials: { apiKey: "svc-key" },
  });
  try {
    await increment(udb);
    broker.stop();
    await sleep(500);
    await broker.restart();

    assert.equal(await incrementAfterRestart(udb, 10_000), null);
    assert.equal(broker.exchanges, 2);
    assert.equal(broker.increments, 2);
    assert.equal(broker.refusedBearers.length, 1);
    assert.equal(udb.credentialError(), null);
  } finally {
    udb.close();
    broker.stop();
  }
});

test("connectEnterprise waits for a restarting broker", async () => {
  const broker = await newBroker();
  broker.stop();
  const back = sleep(200).then(() => broker.restart());
  try {
    const udb = await connectEnterprise(broker);
    udb.close();
  } finally {
    await back;
    broker.stop();
  }
});

// Negative: a broker that, after its restart, no longer accepts the session's
// credential at all gets one sign-in attempt and the call fails UNAUTHENTICATED;
// the refused call is never applied and never resent with the refused bearer.
test("broker restart: a refused credential fails closed without a sign-in per call", async () => {
  const broker = await newBroker();
  const udb = await connectEnterprise(broker);
  try {
    broker.stop();
    broker.refuseSignIn = true;
    await broker.restart();

    const err: any = await incrementAfterRestart(udb, 10_000);
    assert.equal(err?.code, grpc.status.UNAUTHENTICATED, String(err));
    // Later calls are refused locally while the session is unauthorized.
    for (let i = 0; i < 20; i += 1) {
      await assert.rejects(increment(udb), (e: any) => e.code === grpc.status.UNAUTHENTICATED);
    }
    assert.ok(udb.refreshError(), "the refused session reports its credential failure");
    assert.equal(broker.increments, 0);
    assert.equal(broker.refusedBearers.length, 1, "the refused bearer was resent");
    const after = broker.logins - 1;
    assert.ok(after >= 1 && after <= 2, `${after} sign-in attempts after the refusal, want 1 or 2`);
  } finally {
    udb.close();
    broker.stop();
  }
});

test("broker restart: a refused key fails closed without an exchange per call", async () => {
  const broker = await newBroker();
  const udb = await UdbProject.connect({
    target: broker.target,
    tenantId: TENANT,
    purpose: "ts.test.restart",
    deadlineMs: 2_000,
    credentials: { apiKey: "svc-key" },
  });
  try {
    broker.stop();
    broker.refuseSignIn = true;
    await broker.restart();

    const err: any = await incrementAfterRestart(udb, 10_000);
    assert.equal(err?.code, grpc.status.UNAUTHENTICATED, String(err));
    for (let i = 0; i < 20; i += 1) {
      await assert.rejects(increment(udb), (e: any) => e.code === grpc.status.UNAUTHENTICATED);
    }
    assert.ok(udb.credentialError(), "the refused client reports its credential failure");
    assert.equal(broker.increments, 0);
    assert.equal(broker.refusedBearers.length, 1, "the refused bearer was resent");
    const after = broker.exchanges - 1;
    assert.ok(after >= 1 && after <= 2, `${after} key exchanges after the refusal, want 1 or 2`);
  } finally {
    udb.close();
    broker.stop();
  }
});

// Negative: a bearer the client does not own (a configured bearer, a delegated
// asUser bearer) is never renewed or resent after a refusal.
test("broker restart: a refused bearer the client does not own is not repeated", async () => {
  const broker = await newBroker();
  const staticClient = new UdbProject({
    target: broker.target,
    tenantId: TENANT,
    purpose: "ts.test.restart",
    credentials: { bearerToken: "g0-static" },
  });
  const udb = await connectEnterprise(broker);
  try {
    broker.stop();
    await broker.restart();

    const staticErr: any = await incrementAfterRestart(staticClient, 10_000);
    assert.equal(staticErr?.code, grpc.status.UNAUTHENTICATED, String(staticErr));
    const delegatedErr: any = await incrementAfterRestart(udb, 10_000, () => udb.asUser("g0-person"));
    assert.equal(delegatedErr?.code, grpc.status.UNAUTHENTICATED, String(delegatedErr));
    assert.equal(broker.logins, 1);
    assert.equal(broker.increments, 0);
    assert.equal(broker.refusedBearers.length, 2);
  } finally {
    staticClient.close();
    udb.close();
    broker.stop();
  }
});
