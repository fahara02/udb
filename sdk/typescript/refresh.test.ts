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
    assert.equal(last!.apiKey, "key-1", `${label} dropped the configured API key`);
  }
});

test("logout clears active bearer credentials while preserving configured API key", async () => {
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
    assert.equal(last!.apiKey, "key-1", `${label} dropped the configured API key`);
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
    assert.equal(last!.apiKey, "key-1", `${label} dropped the configured API key`);
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
