// Login/refresh conformance unit tests (urgent_fix #20). No live server: the
// auth client and cores are capturing fakes, so we assert the SDK's refresh
// behaviour directly — (1) concurrent refreshIfNeeded() share ONE underlying
// RefreshToken RPC (single-flight), and (2) the refreshed bearer is hot-swapped
// into every outbound channel (data core, auth client, and the dedicated WebRTC
// core when present). Run with Node's built-in runner over compiled JS:
//   npx tsc -p tsconfig.test.json && node --test dist-test

import { strict as assert } from "node:assert";
import { test } from "node:test";

import { UdbProject } from "./project";

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
// before expiry, fall back to the raw x-api-key header when no bearer can be
// obtained, and stop the refresher on close().

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

test("empty access token falls back to raw x-api-key instead of throwing", async () => {
  const auth = apiKeyAuth([() => ({ access_token: "", principal: { tenant_id: "tenant-uuid" } })]);
  const core = credSpy();
  const { project } = apiKeyProject(auth, core);
  const warn = console.warn;
  const warnings: string[] = [];
  console.warn = (m: string) => void warnings.push(m);
  try {
    await project.authenticateApiKeyAndAdopt("svc-key");
  } finally {
    console.warn = warn;
  }
  const last = core.calls.at(-1)!;
  assert.equal(last.apiKey, "svc-key", "raw key installed");
  assert.equal(last.bearerToken, undefined);
  assert.equal(warnings.length, 1, "warned once");
  assert.ok(!warnings[0].includes("svc-key"), "warning must not leak the key");
});

test("failing re-exchange near expiry falls back to raw key, then recovers to a bearer", async () => {
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
  const warn = console.warn;
  console.warn = () => {};
  try {
    await project.backgroundRefreshTick();
  } finally {
    console.warn = warn;
  }
  assert.equal(project.refreshFailures, 1, "failure counted for backoff");
  assert.equal(project.poisoned, false, "API-key session must not fail closed");
  assert.equal(core.calls.at(-1)?.apiKey, "svc-key", "raw key fallback installed");
  assert.equal(core.calls.at(-1)?.bearerToken, undefined);

  fail = false;
  await project.backgroundRefreshTick();
  assert.equal(project.refreshFailures, 0);
  assert.equal(core.calls.at(-1)?.bearerToken, "bearer-2", "bearer swapped back in");
  assert.equal(core.calls.at(-1)?.apiKey, undefined, "raw key dropped again");
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
