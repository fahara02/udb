// Login/refresh conformance unit tests (urgent_fix #20). No live server: the
// auth client and cores are capturing fakes, so we assert the SDK's refresh
// behaviour directly — (1) concurrent refreshIfNeeded() share ONE underlying
// RefreshToken RPC (single-flight), and (2) the refreshed bearer is hot-swapped
// into every outbound channel (data core, auth client, and the dedicated WebRTC
// core when present). Run with Node's built-in runner over compiled JS:
//   npx tsc -p tsconfig.test.json && node --test dist-test

import { strict as assert } from "node:assert";
import { test } from "node:test";

import { SessionExpiredError, UdbProject } from "./project";

interface StoredToken {
  accessToken: string;
  refreshToken?: string;
  sessionId?: string;
  expiresAt: number;
  issuedAt?: number;
}

// Minimal in-memory token store matching the SDK's TokenStore contract.
function memoryStore(initial: StoredToken | null) {
  let token = initial;
  return {
    load: async () => token,
    save: async (t: StoredToken) => {
      token = t;
    },
    clear: async () => {
      token = null;
    },
    current: () => token,
  };
}

// Records each setCredentials call so we can assert the hot-swap reached it.
function credSpy() {
  const calls: Array<{ bearerToken?: string; apiKey?: string }> = [];
  return {
    calls,
    setCredentials: (c: { bearerToken?: string; apiKey?: string }) => {
      calls.push(c);
    },
  };
}

// Build a UdbProject WITHOUT its real constructor (no channel/proto load), wiring
// only the fields refreshIfNeeded touches — exactly as facade.test.ts does.
function bareProject(opts: {
  store: ReturnType<typeof memoryStore>;
  auth: any;
  core: any;
  webrtcCore?: any;
}) {
  const project: any = Object.create(UdbProject.prototype);
  project.tokenStore = opts.store;
  project.refreshInFlight = null;
  project.auth = opts.auth;
  project.core = opts.core;
  project.webrtcGenerated = opts.webrtcCore ? { core: opts.webrtcCore } : null;
  project.config = { credentials: { apiKey: "key-1" } };
  project.renewalBackoffMs = 1; // keep the bounded renewal retries fast in tests
  return project as UdbProject;
}

test("refreshIfNeeded coalesces concurrent callers into ONE RefreshToken RPC", async () => {
  let refreshCalls = 0;
  const auth: any = {
    ...credSpy(),
    refreshToken: async (_req: any) => {
      refreshCalls += 1;
      // Force a real async boundary so concurrent callers overlap.
      await new Promise((r) => setTimeout(r, 5));
      return { access_token: "token-2", access_token_expires_in: 3600 };
    },
  };
  const core = credSpy();
  const store = memoryStore({
    accessToken: "token-1",
    refreshToken: "refresh-1",
    expiresAt: Date.now() - 1000, // already expired → must refresh
  });
  const project = bareProject({ store, auth, core });

  // 5 concurrent refreshers.
  const results = await Promise.all(
    Array.from({ length: 5 }, () => project.refreshIfNeeded()),
  );

  assert.equal(refreshCalls, 1, "single-flight: exactly one RefreshToken RPC");
  for (const r of results) {
    assert.equal((r as any)?.accessToken, "token-2", "all callers get the refreshed token");
  }
  assert.equal(store.current()?.accessToken, "token-2", "refreshed token persisted");
});

test("refreshIfNeeded hot-swaps the new bearer into core, auth, and webrtc core", async () => {
  const auth: any = {
    ...credSpy(),
    refreshToken: async () => ({ access_token: "token-2", access_token_expires_in: 3600 }),
  };
  const core = credSpy();
  const webrtcCore = credSpy();
  const store = memoryStore({
    accessToken: "token-1",
    refreshToken: "refresh-1",
    expiresAt: Date.now() - 1000,
  });
  const project = bareProject({ store, auth, core, webrtcCore });

  await project.refreshIfNeeded();

  for (const [label, spy] of [
    ["data core", core],
    ["auth client", auth],
    ["webrtc core", webrtcCore],
  ] as const) {
    const last = spy.calls.at(-1);
    assert.ok(last, `${label} setCredentials was not called`);
    assert.equal(last!.bearerToken, "token-2", `${label} did not receive the refreshed bearer`);
    assert.equal(last!.apiKey, undefined, `${label} retained raw API-key metadata`);
  }
});

test("refreshIfNeeded keeps the rotated refresh token across repeated refreshes", async () => {
  // The broker rotates the refresh token on every RefreshToken call and each
  // token works once; presenting a spent one is "invalid credential".
  let live = "refresh-1";
  let minted = 1;
  const presented: string[] = [];
  const auth: any = {
    ...credSpy(),
    refreshToken: async (req: any) => {
      presented.push(req.refresh_token);
      if (req.refresh_token !== live) throw new Error("16 UNAUTHENTICATED: invalid credential");
      minted += 1;
      live = `refresh-${minted}`;
      return {
        access_token: `token-${minted}`,
        access_token_expires_in: 900,
        refresh_token: live,
        refresh_token_expires_in: 86400,
        session_public_id: "public-session",
      };
    },
  };
  const store = memoryStore({
    accessToken: "token-1",
    refreshToken: "refresh-1",
    sessionId: "legacy-session",
    expiresAt: Date.now() - 1000,
  });
  const project = bareProject({ store, auth, core: credSpy() });

  for (let i = 0; i < 4; i += 1) {
    // Expire the stored access token so each call performs a real refresh.
    await store.save({ ...store.current()!, expiresAt: Date.now() - 1000 });
    await project.refreshIfNeeded();
  }

  assert.deepEqual(presented, ["refresh-1", "refresh-2", "refresh-3", "refresh-4"]);
  assert.equal(store.current()!.refreshToken, "refresh-5");
  assert.equal(store.current()!.accessToken, "token-5");
  assert.equal(store.current()!.sessionId, "legacy-session", "the legacy refresh session id is kept");
});

test("a legacy session-id refresh without a rotated token keeps the stored refresh token", async () => {
  const auth: any = {
    ...credSpy(),
    refreshToken: async () => ({ access_token: "token-2", access_token_expires_in: 900 }),
  };
  const store = memoryStore({
    accessToken: "token-1",
    refreshToken: "refresh-1",
    sessionId: "legacy-session",
    expiresAt: Date.now() - 1000,
  });
  const project = bareProject({ store, auth, core: credSpy() });

  await project.refreshIfNeeded();

  assert.equal(store.current()!.refreshToken, "refresh-1");
  assert.equal(store.current()!.accessToken, "token-2");
});

test("logout clears active bearer credentials and raw API-key metadata", async () => {
  const auth: any = credSpy();
  const core = credSpy();
  const webrtcCore = credSpy();
  const store = memoryStore({
    accessToken: "token-1",
    refreshToken: "refresh-1",
    expiresAt: Date.now() + 3600_000,
  });
  const project = bareProject({ store, auth, core, webrtcCore });

  await project.logout();

  assert.equal(store.current(), null, "stored token was not cleared");
  for (const [label, spy] of [
    ["data core", core],
    ["auth client", auth],
    ["webrtc core", webrtcCore],
  ] as const) {
    const last = spy.calls.at(-1);
    assert.ok(last, `${label} setCredentials was not called`);
    assert.equal(last!.bearerToken, undefined, `${label} retained the bearer token`);
    assert.equal(last!.apiKey, undefined, `${label} retained raw API-key metadata`);
  }
});

test("refreshIfNeeded is a no-op while the token is still fresh", async () => {
  let refreshCalls = 0;
  const auth: any = {
    ...credSpy(),
    refreshToken: async () => {
      refreshCalls += 1;
      return { access_token: "token-2", access_token_expires_in: 3600 };
    },
  };
  const core = credSpy();
  const store = memoryStore({
    accessToken: "token-1",
    refreshToken: "refresh-1",
    expiresAt: Date.now() + 3600_000, // far from expiry
  });
  const project = bareProject({ store, auth, core });

  await project.refreshIfNeeded();
  assert.equal(refreshCalls, 0, "no refresh while fresh");
  assert.equal(core.calls.length, 0, "no credential swap while fresh");
});

// Background refresher fail-closed parity with the Go enterprise session: an
// EXPIRED token whose refresh fails poisons the session and clears the bearer on
// every channel, so the next call can't go out with a dead credential.
test("background refresh fails CLOSED: expired token + failing refresh clears the bearer", async () => {
  const auth: any = {
    ...credSpy(),
    refreshToken: async () => {
      throw new Error("refresh token revoked");
    },
  };
  const core = credSpy();
  const webrtcCore = credSpy();
  const store = memoryStore({
    accessToken: "token-1",
    refreshToken: "refresh-1",
    expiresAt: Date.now() - 1000, // expired
  });
  const project = bareProject({ store, auth, core, webrtcCore }) as any;
  project.closed = true; // keep scheduleRefresh from arming a real timer in the test

  await project.backgroundRefreshTick();

  assert.ok(project.poisoned, "expired token + failed refresh must poison the session");
  assert.ok(project.refreshError(), "refreshError() must expose the failure");
  for (const [label, spy] of [
    ["data core", core],
    ["auth client", auth],
    ["webrtc core", webrtcCore],
  ] as const) {
    const last = spy.calls.at(-1);
    assert.ok(last, `${label} setCredentials was not called`);
    assert.equal(last!.bearerToken, undefined, `${label} still holds a bearer (not failed closed)`);
    assert.equal(last!.apiKey, undefined, `${label} retained raw API-key metadata`);
  }
});

// A transient refresh blip while the token is STILL valid must NOT fail closed.
test("background refresh does NOT fail closed on a blip while the token is still valid", async () => {
  const auth: any = {
    ...credSpy(),
    refreshToken: async () => {
      throw new Error("network blip");
    },
  };
  const core = credSpy();
  const store = memoryStore({
    accessToken: "token-1",
    refreshToken: "refresh-1",
    expiresAt: Date.now() + 30_000, // valid, but within skew → refresh is attempted (and fails)
  });
  const project = bareProject({ store, auth, core }) as any;
  project.poisoned = false; // real constructor inits this; Object.create skips field initializers
  project.closed = true;

  await project.backgroundRefreshTick();

  assert.equal(project.poisoned, false, "must not poison while the token is still valid");
  const cleared = core.calls.some((c: any) => c.bearerToken === undefined);
  assert.equal(cleared, false, "must not clear a still-valid bearer");
});

// A successful background refresh recovers a previously-poisoned session.
test("background refresh recovers: a successful refresh clears poison", async () => {
  const auth: any = {
    ...credSpy(),
    refreshToken: async () => ({ access_token: "token-2", access_token_expires_in: 3600 }),
  };
  const core = credSpy();
  const store = memoryStore({
    accessToken: "token-1",
    refreshToken: "refresh-1",
    expiresAt: Date.now() - 1000, // expired → refresh runs
  });
  const project = bareProject({ store, auth, core }) as any;
  project.poisoned = true;
  project.lastRefreshError = new Error("prev");
  project.closed = true;

  await project.backgroundRefreshTick();

  assert.equal(project.poisoned, false, "poison cleared after a successful refresh");
  assert.equal(project.refreshError(), null, "refreshError() cleared after success");
  assert.equal(store.current()?.accessToken, "token-2", "refreshed token persisted");
});

// ── API-key bearer lifecycle ────────────────────────────────────────────────
// An API key is exchanged (AuthnService.Authenticate) for a short-lived bearer
// that carries NO refresh token. The SDK must retain the key and re-exchange it
// before expiry, never send the key as a raw x-api-key header (no bearer means
// the call fails closed), and stop the refresher on close().

function apiKeyAuth(responses: Array<() => any>) {
  const keys: string[] = [];
  let i = 0;
  const auth: any = {
    ...credSpy(),
    keys,
    authenticateApiKey: async (key: string) => {
      keys.push(key);
      const next = responses[Math.min(i, responses.length - 1)];
      i += 1;
      return next();
    },
    refreshToken: async () => {
      throw new Error("RefreshToken must not be called for an API-key bearer");
    },
  };
  return auth;
}

function apiKeyProject(auth: any, core: any, store = memoryStore(null)) {
  const project = bareProject({ store, auth, core }) as any;
  project.sharedMeta = { tenantId: "t" };
  project.poisoned = false;
  project.refreshFailures = 0;
  project.closed = true; // tests drive ticks by hand; no real timers
  return { project, store };
}

const minted = (token: string, ttlSec: number) => () => ({
  access_token: token,
  expires_at_unix: String(Math.floor(Date.now() / 1000) + ttlSec),
  principal: { tenant_id: "tenant-uuid" },
});

test("API-key bearer is re-exchanged with the retained key at ~80% of its lifetime", async () => {
  const auth = apiKeyAuth([minted("bearer-1", 900), minted("bearer-2", 900)]);
  const core = credSpy();
  const { project, store } = apiKeyProject(auth, core);

  await project.authenticateApiKeyAndAdopt("svc-key");
  assert.equal(store.current()?.accessToken, "bearer-1");
  assert.equal(core.calls.at(-1)?.bearerToken, "bearer-1");

  // 170s left of a 900s lifetime: past the 80% mark (180s before expiry) but
  // outside the plain 60s skew — the refresher must already renew.
  const now = Date.now();
  store.current()!.issuedAt = now - 730_000;
  store.current()!.expiresAt = now + 170_000;
  await project.backgroundRefreshTick();

  assert.deepEqual(auth.keys, ["svc-key", "svc-key"], "re-exchanged with the retained key");
  assert.equal(store.current()?.accessToken, "bearer-2");
  const last = core.calls.at(-1)!;
  assert.equal(last.bearerToken, "bearer-2", "new bearer hot-swapped in");
  assert.equal(last.apiKey, undefined, "raw key not sent while a bearer is held");
});

test("empty access token is refused: the API key is never sent raw", async () => {
  const auth = apiKeyAuth([() => ({ access_token: "", principal: { tenant_id: "tenant-uuid" } })]);
  const core = credSpy();
  const { project } = apiKeyProject(auth, core);
  await assert.rejects(
    project.authenticateApiKeyAndAdopt("svc-key"),
    (err: Error) => /no access token/.test(err.message) && !err.message.includes("svc-key"),
  );
  assert.ok(
    core.calls.every((call: any) => call.apiKey === undefined),
    "the raw key must never be installed as a header",
  );
});

test("failing re-exchange keeps the bearer until expiry, then fails closed, then recovers", async () => {
  let fail = true;
  const auth = apiKeyAuth([
    minted("bearer-1", 900),
    () => {
      if (fail) throw new Error("authn unavailable");
      return minted("bearer-2", 900)();
    },
  ]);
  const core = credSpy();
  const { project, store } = apiKeyProject(auth, core);
  await project.authenticateApiKeyAndAdopt("svc-key");

  store.current()!.issuedAt = Date.now() - 890_000;
  store.current()!.expiresAt = Date.now() + 10_000; // inside the final skew window
  await project.backgroundRefreshTick();
  assert.equal(project.refreshFailures, 1, "failure counted for backoff");
  assert.equal(project.poisoned, false, "a still-live bearer is kept");
  assert.equal(core.calls.at(-1)?.bearerToken, "bearer-1");

  store.current()!.expiresAt = Date.now() - 1_000; // now expired
  await project.backgroundRefreshTick();
  assert.equal(project.refreshFailures, 2);
  assert.equal(project.poisoned, true, "an expired API-key bearer fails closed");
  assert.equal(core.calls.at(-1)?.bearerToken, undefined, "dead bearer dropped");
  assert.ok(
    core.calls.every((call: any) => call.apiKey === undefined),
    "the raw key must never be installed as a header",
  );

  fail = false;
  await project.backgroundRefreshTick();
  assert.equal(project.refreshFailures, 0);
  assert.equal(project.poisoned, false);
  assert.equal(core.calls.at(-1)?.bearerToken, "bearer-2", "bearer swapped back in");
});

test("a re-exchange blip while the bearer is still well within life keeps the bearer", async () => {
  const auth = apiKeyAuth([
    minted("bearer-1", 900),
    () => {
      throw new Error("blip");
    },
  ]);
  const core = credSpy();
  const { project, store } = apiKeyProject(auth, core);
  await project.authenticateApiKeyAndAdopt("svc-key");
  store.current()!.issuedAt = Date.now() - 800_000;
  store.current()!.expiresAt = Date.now() + 150_000; // due for renewal, not near expiry
  await project.backgroundRefreshTick();
  assert.equal(core.calls.at(-1)?.bearerToken, "bearer-1", "still-valid bearer kept");
  assert.equal(core.calls.at(-1)?.apiKey, undefined);
});

test("close() cancels the API-key re-exchange timer", async () => {
  const auth = apiKeyAuth([minted("bearer-1", 900)]);
  const core = credSpy();
  const { project } = apiKeyProject(auth, core);
  project.closed = false;
  project.generated = { close() {} };
  await project.authenticateApiKeyAndAdopt("svc-key");
  // scheduleRefresh arms the timer after an async token-store load.
  await new Promise((r) => setImmediate(r));
  assert.ok(project.refreshTimer, "refresher armed after the API-key exchange");
  project.close();
  assert.equal(project.refreshTimer, null, "timer cancelled on close");
});

// ── Session renewal recovery ────────────────────────────────────────────────
// A renewal that fails is retried (bounded, jittered); a session holding a
// durable credential re-authenticates with it and never resubmits a refresh
// token that may have been consumed; a session that cannot renew fails its calls
// locally with SessionExpiredError; a call past the renewal point awaits the one
// shared renewal instead of sending a stale or empty bearer.

/** A data-plane core whose calls go through the project's credential gate and
 *  record the bearer each call actually carried. */
function gatedCore(project: any) {
  const calls: Array<{ bearerToken?: string; apiKey?: string }> = [];
  const sent: Array<string | undefined> = [];
  let bearer: string | undefined;
  const core: any = {
    calls,
    setCredentials(c: { bearerToken?: string; apiKey?: string }) {
      calls.push(c);
      if ("bearerToken" in c) bearer = c.bearerToken;
    },
    async unary() {
      sent.push(bearer);
      return { ok: true };
    },
    serverStream() {
      return {};
    },
    clientStream() {
      return {};
    },
    bidiStream() {
      return {};
    },
  };
  project.core = core;
  project.installCredentialGate(core);
  return { core, sent };
}

const grpcError = (code: number, message: string) => Object.assign(new Error(message), { code });

function expireStored(store: ReturnType<typeof memoryStore>) {
  const now = Date.now();
  store.current()!.issuedAt = now - 900_000;
  store.current()!.expiresAt = now - 1_000;
}

test("API-key session: a lost exchange reply is retried with the key and the next call succeeds (x3)", async () => {
  let mintedCount = 0;
  let loseNextReply = false;
  const keys: string[] = [];
  const auth: any = {
    ...credSpy(),
    authenticateApiKey: async (key: string) => {
      keys.push(key);
      if (loseNextReply) {
        loseNextReply = false;
        throw grpcError(14, "14 UNAVAILABLE: connection reset before the reply");
      }
      mintedCount += 1;
      return minted(`bearer-${mintedCount}`, 900)();
    },
    refreshToken: async () => {
      throw new Error("RefreshToken must not be called for an API-key bearer");
    },
  };
  const { project, store } = apiKeyProject(auth, credSpy());
  const reported: Error[] = [];
  project.config.onRefreshError = (err: Error) => reported.push(err);
  const { sent } = gatedCore(project);
  await project.authenticateApiKeyAndAdopt("svc-key");
  project.closed = false; // opened after adoption so no real refresh timer is armed

  for (let cycle = 1; cycle <= 3; cycle += 1) {
    expireStored(store);
    loseNextReply = true;
    await project.core.unary("udb.services.v1.DataBroker", "Select", {});
    assert.equal(sent.at(-1), `bearer-${cycle + 1}`, `cycle ${cycle}: the call carried the re-exchanged bearer`);
    assert.equal(project.poisoned, false);
    assert.equal(project.refreshError(), null);
  }
  assert.equal(keys.length, 7, "connect + (lost + retried) exchange per cycle");
  assert.ok(keys.every((k) => k === "svc-key"));
  assert.equal(reported.length, 0, "a renewal that recovered within its attempts reports nothing");
});

test("password session: a refresh whose reply was lost re-logs in, never resubmitting the token (x3)", async () => {
  const PRINCIPAL = {
    principal_id: "p1",
    subject: "svc",
    tenant_id: "tenant-uuid",
    user_id: "u1",
    scopes: [] as string[],
  };
  const presented: string[] = [];
  let logins = 0;
  const auth: any = {
    ...credSpy(),
    // The broker consumes (rotates) the token, but the reply never arrives.
    refreshToken: async (req: any) => {
      presented.push(req.refresh_token);
      throw grpcError(4, "4 DEADLINE_EXCEEDED: reply lost");
    },
    login: async (req: any) => {
      assert.equal(req.username, "svc");
      assert.equal(req.password, "pw");
      logins += 1;
      return { access_token: `access-${logins}`, refresh_token: `refresh-${logins}`, access_token_expires_in: 900 };
    },
    authenticateBearer: async () => ({ principal: PRINCIPAL }),
  };
  const store = memoryStore({
    accessToken: "access-0",
    refreshToken: "refresh-0",
    issuedAt: Date.now() - 900_000,
    expiresAt: Date.now() - 1_000,
  });
  const project = bareProject({ store, auth, core: credSpy() }) as any;
  project.sharedMeta = { tenantId: "tenant-uuid" };
  project.credentialPin = {
    principalId: "p1",
    subject: "svc",
    accountKind: "",
    tenantId: "tenant-uuid",
    projectId: "",
    userId: "u1",
    serviceIdentity: "",
    scopes: [],
  };
  project.retainLogin({ username: "svc", password: "pw" });
  const { sent } = gatedCore(project);

  for (let cycle = 1; cycle <= 3; cycle += 1) {
    expireStored(store);
    await project.core.unary("udb.services.v1.DataBroker", "Select", {});
    assert.equal(sent.at(-1), `access-${cycle}`, `cycle ${cycle}: the call carried the re-login bearer`);
  }
  assert.deepEqual(presented, ["refresh-0", "refresh-1", "refresh-2"], "each refresh token presented exactly once");
  assert.equal(logins, 3);
  assert.equal(project.refreshError(), null);
});

test("refresh-only session: a refused refresh token fails calls locally with SessionExpiredError", async () => {
  const presented: string[] = [];
  const auth: any = {
    ...credSpy(),
    refreshToken: async (req: any) => {
      presented.push(req.refresh_token);
      if (presented.length === 1) throw grpcError(4, "4 DEADLINE_EXCEEDED: reply lost");
      throw grpcError(16, "16 UNAUTHENTICATED: invalid refresh token");
    },
  };
  const store = memoryStore({
    accessToken: "token-1",
    refreshToken: "refresh-1",
    issuedAt: Date.now() - 900_000,
    expiresAt: Date.now() - 1_000,
  });
  const project = bareProject({ store, auth, core: credSpy() }) as any;
  const reported: Error[] = [];
  project.config.onRefreshError = (err: Error) => reported.push(err);
  const { core, sent } = gatedCore(project);

  const isExpired = (err: any) =>
    err instanceof SessionExpiredError &&
    err.code === 16 &&
    /invalid refresh token/.test((err.cause as Error)?.message ?? "");
  await assert.rejects(core.unary("udb.services.v1.DataBroker", "Select", {}), isExpired);
  assert.equal(sent.length, 0, "no call left the client");
  assert.equal(reported.length, 1, "onRefreshError fired");
  assert.match(reported[0].message, /invalid refresh token/);
  assert.equal(core.calls.at(-1)?.bearerToken, undefined, "the dead bearer was dropped");
  assert.throws(() => core.serverStream("svc", "Watch", {}), (err: any) => err instanceof SessionExpiredError);

  // Past the backoff the refused token is still never resubmitted.
  project.renewalRetryAt = 0;
  await assert.rejects(core.unary("udb.services.v1.DataBroker", "Select", {}), isExpired);
  assert.deepEqual(presented, ["refresh-1", "refresh-1"], "one transient retry, then the refusal is final");
  assert.equal(reported.length, 1, "the same failure is reported once");
  assert.equal(sent.length, 0);
});

test("concurrent calls at expiry share exactly one renewal", async () => {
  let exchanges = 0;
  const auth: any = {
    ...credSpy(),
    authenticateApiKey: async () => {
      exchanges += 1;
      await new Promise((r) => setTimeout(r, 10));
      return minted(`bearer-${exchanges}`, 900)();
    },
  };
  const { project, store } = apiKeyProject(auth, credSpy());
  const { sent } = gatedCore(project);
  await project.authenticateApiKeyAndAdopt("svc-key");
  project.closed = false; // opened after adoption so no real refresh timer is armed
  expireStored(store);

  await Promise.all(
    Array.from({ length: 8 }, () => project.core.unary("udb.services.v1.DataBroker", "Select", {})),
  );
  assert.equal(exchanges, 2, "connect + ONE renewal for eight concurrent callers");
  assert.deepEqual(sent, Array(8).fill("bearer-2"), "every caller carried the renewed bearer");
});
