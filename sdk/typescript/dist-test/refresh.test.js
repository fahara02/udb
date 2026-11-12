"use strict";
// Login/refresh conformance unit tests (urgent_fix #20). No live server: the
// auth client and cores are capturing fakes, so we assert the SDK's refresh
// behaviour directly — (1) concurrent refreshIfNeeded() share ONE underlying
// RefreshToken RPC (single-flight), and (2) the refreshed bearer is hot-swapped
// into every outbound channel (data core, auth client, and the dedicated WebRTC
// core when present). Run with Node's built-in runner over compiled JS:
//   npx tsc -p tsconfig.test.json && node --test dist-test
Object.defineProperty(exports, "__esModule", { value: true });
const node_assert_1 = require("node:assert");
const node_test_1 = require("node:test");
const project_1 = require("./project");
// Minimal in-memory token store matching the SDK's TokenStore contract.
function memoryStore(initial) {
    let token = initial;
    return {
        load: async () => token,
        save: async (t) => {
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
    const calls = [];
    return {
        calls,
        setCredentials: (c) => {
            calls.push(c);
        },
    };
}
// Build a UdbProject WITHOUT its real constructor (no channel/proto load), wiring
// only the fields refreshIfNeeded touches — exactly as facade.test.ts does.
function bareProject(opts) {
    const project = Object.create(project_1.UdbProject.prototype);
    project.tokenStore = opts.store;
    project.refreshInFlight = null;
    project.auth = opts.auth;
    project.core = opts.core;
    project.webrtcGenerated = opts.webrtcCore ? { core: opts.webrtcCore } : null;
    project.config = { credentials: { apiKey: "key-1" } };
    project.renewalBackoffMs = 1; // keep the bounded renewal retries fast in tests
    return project;
}
(0, node_test_1.test)("refreshIfNeeded coalesces concurrent callers into ONE RefreshToken RPC", async () => {
    let refreshCalls = 0;
    const auth = {
        ...credSpy(),
        refreshToken: async (_req) => {
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
    const results = await Promise.all(Array.from({ length: 5 }, () => project.refreshIfNeeded()));
    node_assert_1.strict.equal(refreshCalls, 1, "single-flight: exactly one RefreshToken RPC");
    for (const r of results) {
        node_assert_1.strict.equal(r?.accessToken, "token-2", "all callers get the refreshed token");
    }
    node_assert_1.strict.equal(store.current()?.accessToken, "token-2", "refreshed token persisted");
});
(0, node_test_1.test)("refreshIfNeeded hot-swaps the new bearer into core, auth, and webrtc core", async () => {
    const auth = {
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
    ]) {
        const last = spy.calls.at(-1);
        node_assert_1.strict.ok(last, `${label} setCredentials was not called`);
        node_assert_1.strict.equal(last.bearerToken, "token-2", `${label} did not receive the refreshed bearer`);
        node_assert_1.strict.equal(last.apiKey, undefined, `${label} retained raw API-key metadata`);
    }
});
(0, node_test_1.test)("refreshIfNeeded keeps the rotated refresh token across repeated refreshes", async () => {
    // The broker rotates the refresh token on every RefreshToken call and each
    // token works once; presenting a spent one is "invalid credential".
    let live = "refresh-1";
    let minted = 1;
    const presented = [];
    const auth = {
        ...credSpy(),
        refreshToken: async (req) => {
            presented.push(req.refresh_token);
            if (req.refresh_token !== live)
                throw new Error("16 UNAUTHENTICATED: invalid credential");
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
        await store.save({ ...store.current(), expiresAt: Date.now() - 1000 });
        await project.refreshIfNeeded();
    }
    node_assert_1.strict.deepEqual(presented, ["refresh-1", "refresh-2", "refresh-3", "refresh-4"]);
    node_assert_1.strict.equal(store.current().refreshToken, "refresh-5");
    node_assert_1.strict.equal(store.current().accessToken, "token-5");
    node_assert_1.strict.equal(store.current().sessionId, "legacy-session", "the legacy refresh session id is kept");
});
(0, node_test_1.test)("a legacy session-id refresh without a rotated token keeps the stored refresh token", async () => {
    const auth = {
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
    node_assert_1.strict.equal(store.current().refreshToken, "refresh-1");
    node_assert_1.strict.equal(store.current().accessToken, "token-2");
});
(0, node_test_1.test)("logout clears active bearer credentials and raw API-key metadata", async () => {
    const auth = credSpy();
    const core = credSpy();
    const webrtcCore = credSpy();
    const store = memoryStore({
        accessToken: "token-1",
        refreshToken: "refresh-1",
        expiresAt: Date.now() + 3600_000,
    });
    const project = bareProject({ store, auth, core, webrtcCore });
    await project.logout();
    node_assert_1.strict.equal(store.current(), null, "stored token was not cleared");
    for (const [label, spy] of [
        ["data core", core],
        ["auth client", auth],
        ["webrtc core", webrtcCore],
    ]) {
        const last = spy.calls.at(-1);
        node_assert_1.strict.ok(last, `${label} setCredentials was not called`);
        node_assert_1.strict.equal(last.bearerToken, undefined, `${label} retained the bearer token`);
        node_assert_1.strict.equal(last.apiKey, undefined, `${label} retained raw API-key metadata`);
    }
});
(0, node_test_1.test)("refreshIfNeeded is a no-op while the token is still fresh", async () => {
    let refreshCalls = 0;
    const auth = {
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
    node_assert_1.strict.equal(refreshCalls, 0, "no refresh while fresh");
    node_assert_1.strict.equal(core.calls.length, 0, "no credential swap while fresh");
});
// Background refresher fail-closed parity with the Go enterprise session: an
// EXPIRED token whose refresh fails poisons the session and clears the bearer on
// every channel, so the next call can't go out with a dead credential.
(0, node_test_1.test)("background refresh fails CLOSED: expired token + failing refresh clears the bearer", async () => {
    const auth = {
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
    const project = bareProject({ store, auth, core, webrtcCore });
    project.closed = true; // keep scheduleRefresh from arming a real timer in the test
    await project.backgroundRefreshTick();
    node_assert_1.strict.ok(project.poisoned, "expired token + failed refresh must poison the session");
    node_assert_1.strict.ok(project.refreshError(), "refreshError() must expose the failure");
    for (const [label, spy] of [
        ["data core", core],
        ["auth client", auth],
        ["webrtc core", webrtcCore],
    ]) {
        const last = spy.calls.at(-1);
        node_assert_1.strict.ok(last, `${label} setCredentials was not called`);
        node_assert_1.strict.equal(last.bearerToken, undefined, `${label} still holds a bearer (not failed closed)`);
        node_assert_1.strict.equal(last.apiKey, undefined, `${label} retained raw API-key metadata`);
    }
});
// A transient refresh blip while the token is STILL valid must NOT fail closed.
(0, node_test_1.test)("background refresh does NOT fail closed on a blip while the token is still valid", async () => {
    const auth = {
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
    const project = bareProject({ store, auth, core });
    project.poisoned = false; // real constructor inits this; Object.create skips field initializers
    project.closed = true;
    await project.backgroundRefreshTick();
    node_assert_1.strict.equal(project.poisoned, false, "must not poison while the token is still valid");
    const cleared = core.calls.some((c) => c.bearerToken === undefined);
    node_assert_1.strict.equal(cleared, false, "must not clear a still-valid bearer");
});
// A successful background refresh recovers a previously-poisoned session.
(0, node_test_1.test)("background refresh recovers: a successful refresh clears poison", async () => {
    const auth = {
        ...credSpy(),
        refreshToken: async () => ({ access_token: "token-2", access_token_expires_in: 3600 }),
    };
    const core = credSpy();
    const store = memoryStore({
        accessToken: "token-1",
        refreshToken: "refresh-1",
        expiresAt: Date.now() - 1000, // expired → refresh runs
    });
    const project = bareProject({ store, auth, core });
    project.poisoned = true;
    project.lastRefreshError = new Error("prev");
    project.closed = true;
    await project.backgroundRefreshTick();
    node_assert_1.strict.equal(project.poisoned, false, "poison cleared after a successful refresh");
    node_assert_1.strict.equal(project.refreshError(), null, "refreshError() cleared after success");
    node_assert_1.strict.equal(store.current()?.accessToken, "token-2", "refreshed token persisted");
});
// ── API-key bearer lifecycle ────────────────────────────────────────────────
// An API key is exchanged (AuthnService.Authenticate) for a short-lived bearer
// that carries NO refresh token. The SDK must retain the key and re-exchange it
// before expiry, never send the key as a raw x-api-key header (no bearer means
// the call fails closed), and stop the refresher on close().
function apiKeyAuth(responses) {
    const keys = [];
    let i = 0;
    const auth = {
        ...credSpy(),
        keys,
        authenticateApiKey: async (key) => {
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
function apiKeyProject(auth, core, store = memoryStore(null)) {
    const project = bareProject({ store, auth, core });
    project.sharedMeta = { tenantId: "t" };
    project.poisoned = false;
    project.refreshFailures = 0;
    project.closed = true; // tests drive ticks by hand; no real timers
    return { project, store };
}
const minted = (token, ttlSec) => () => ({
    access_token: token,
    expires_at_unix: String(Math.floor(Date.now() / 1000) + ttlSec),
    principal: { tenant_id: "tenant-uuid" },
});
(0, node_test_1.test)("API-key bearer is re-exchanged with the retained key at ~80% of its lifetime", async () => {
    const auth = apiKeyAuth([minted("bearer-1", 900), minted("bearer-2", 900)]);
    const core = credSpy();
    const { project, store } = apiKeyProject(auth, core);
    await project.authenticateApiKeyAndAdopt("svc-key");
    node_assert_1.strict.equal(store.current()?.accessToken, "bearer-1");
    node_assert_1.strict.equal(core.calls.at(-1)?.bearerToken, "bearer-1");
    // 170s left of a 900s lifetime: past the 80% mark (180s before expiry) but
    // outside the plain 60s skew — the refresher must already renew.
    const now = Date.now();
    store.current().issuedAt = now - 730_000;
    store.current().expiresAt = now + 170_000;
    await project.backgroundRefreshTick();
    node_assert_1.strict.deepEqual(auth.keys, ["svc-key", "svc-key"], "re-exchanged with the retained key");
    node_assert_1.strict.equal(store.current()?.accessToken, "bearer-2");
    const last = core.calls.at(-1);
    node_assert_1.strict.equal(last.bearerToken, "bearer-2", "new bearer hot-swapped in");
    node_assert_1.strict.equal(last.apiKey, undefined, "raw key not sent while a bearer is held");
});
(0, node_test_1.test)("empty access token is refused: the API key is never sent raw", async () => {
    const auth = apiKeyAuth([() => ({ access_token: "", principal: { tenant_id: "tenant-uuid" } })]);
    const core = credSpy();
    const { project } = apiKeyProject(auth, core);
    await node_assert_1.strict.rejects(project.authenticateApiKeyAndAdopt("svc-key"), (err) => /no access token/.test(err.message) && !err.message.includes("svc-key"));
    node_assert_1.strict.ok(core.calls.every((call) => call.apiKey === undefined), "the raw key must never be installed as a header");
});
(0, node_test_1.test)("failing re-exchange keeps the bearer until expiry, then fails closed, then recovers", async () => {
    let fail = true;
    const auth = apiKeyAuth([
        minted("bearer-1", 900),
        () => {
            if (fail)
                throw new Error("authn unavailable");
            return minted("bearer-2", 900)();
        },
    ]);
    const core = credSpy();
    const { project, store } = apiKeyProject(auth, core);
    await project.authenticateApiKeyAndAdopt("svc-key");
    store.current().issuedAt = Date.now() - 890_000;
    store.current().expiresAt = Date.now() + 10_000; // inside the final skew window
    await project.backgroundRefreshTick();
    node_assert_1.strict.equal(project.refreshFailures, 1, "failure counted for backoff");
    node_assert_1.strict.equal(project.poisoned, false, "a still-live bearer is kept");
    node_assert_1.strict.equal(core.calls.at(-1)?.bearerToken, "bearer-1");
    store.current().expiresAt = Date.now() - 1_000; // now expired
    await project.backgroundRefreshTick();
    node_assert_1.strict.equal(project.refreshFailures, 2);
    node_assert_1.strict.equal(project.poisoned, true, "an expired API-key bearer fails closed");
    node_assert_1.strict.equal(core.calls.at(-1)?.bearerToken, undefined, "dead bearer dropped");
    node_assert_1.strict.ok(core.calls.every((call) => call.apiKey === undefined), "the raw key must never be installed as a header");
    fail = false;
    await project.backgroundRefreshTick();
    node_assert_1.strict.equal(project.refreshFailures, 0);
    node_assert_1.strict.equal(project.poisoned, false);
    node_assert_1.strict.equal(core.calls.at(-1)?.bearerToken, "bearer-2", "bearer swapped back in");
});
(0, node_test_1.test)("a re-exchange blip while the bearer is still well within life keeps the bearer", async () => {
    const auth = apiKeyAuth([
        minted("bearer-1", 900),
        () => {
            throw new Error("blip");
        },
    ]);
    const core = credSpy();
    const { project, store } = apiKeyProject(auth, core);
    await project.authenticateApiKeyAndAdopt("svc-key");
    store.current().issuedAt = Date.now() - 800_000;
    store.current().expiresAt = Date.now() + 150_000; // due for renewal, not near expiry
    await project.backgroundRefreshTick();
    node_assert_1.strict.equal(core.calls.at(-1)?.bearerToken, "bearer-1", "still-valid bearer kept");
    node_assert_1.strict.equal(core.calls.at(-1)?.apiKey, undefined);
});
(0, node_test_1.test)("close() cancels the API-key re-exchange timer", async () => {
    const auth = apiKeyAuth([minted("bearer-1", 900)]);
    const core = credSpy();
    const { project } = apiKeyProject(auth, core);
    project.closed = false;
    project.generated = { close() { } };
    await project.authenticateApiKeyAndAdopt("svc-key");
    // scheduleRefresh arms the timer after an async token-store load.
    await new Promise((r) => setImmediate(r));
    node_assert_1.strict.ok(project.refreshTimer, "refresher armed after the API-key exchange");
    project.close();
    node_assert_1.strict.equal(project.refreshTimer, null, "timer cancelled on close");
});
// ── Session renewal recovery ────────────────────────────────────────────────
// A renewal that fails is retried (bounded, jittered); a session holding a
// durable credential re-authenticates with it and never resubmits a refresh
// token that may have been consumed; a session that cannot renew fails its calls
// locally with SessionExpiredError; a call past the renewal point awaits the one
// shared renewal instead of sending a stale or empty bearer.
/** A data-plane core whose calls go through the project's credential gate and
 *  record the bearer each call actually carried. */
function gatedCore(project) {
    const calls = [];
    const sent = [];
    let bearer;
    const core = {
        calls,
        setCredentials(c) {
            calls.push(c);
            if ("bearerToken" in c)
                bearer = c.bearerToken;
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
const grpcError = (code, message) => Object.assign(new Error(message), { code });
function expireStored(store) {
    const now = Date.now();
    store.current().issuedAt = now - 900_000;
    store.current().expiresAt = now - 1_000;
}
(0, node_test_1.test)("API-key session: a lost exchange reply is retried with the key and the next call succeeds (x3)", async () => {
    let mintedCount = 0;
    let loseNextReply = false;
    const keys = [];
    const auth = {
        ...credSpy(),
        authenticateApiKey: async (key) => {
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
    const reported = [];
    project.config.onRefreshError = (err) => reported.push(err);
    const { sent } = gatedCore(project);
    await project.authenticateApiKeyAndAdopt("svc-key");
    project.closed = false; // opened after adoption so no real refresh timer is armed
    for (let cycle = 1; cycle <= 3; cycle += 1) {
        expireStored(store);
        loseNextReply = true;
        await project.core.unary("udb.services.v1.DataBroker", "Select", {});
        node_assert_1.strict.equal(sent.at(-1), `bearer-${cycle + 1}`, `cycle ${cycle}: the call carried the re-exchanged bearer`);
        node_assert_1.strict.equal(project.poisoned, false);
        node_assert_1.strict.equal(project.refreshError(), null);
    }
    node_assert_1.strict.equal(keys.length, 7, "connect + (lost + retried) exchange per cycle");
    node_assert_1.strict.ok(keys.every((k) => k === "svc-key"));
    node_assert_1.strict.equal(reported.length, 0, "a renewal that recovered within its attempts reports nothing");
});
(0, node_test_1.test)("password session: a refresh whose reply was lost re-logs in, never resubmitting the token (x3)", async () => {
    const PRINCIPAL = {
        principal_id: "p1",
        subject: "svc",
        tenant_id: "tenant-uuid",
        user_id: "u1",
        scopes: [],
    };
    const presented = [];
    let logins = 0;
    const auth = {
        ...credSpy(),
        // The broker consumes (rotates) the token, but the reply never arrives.
        refreshToken: async (req) => {
            presented.push(req.refresh_token);
            throw grpcError(4, "4 DEADLINE_EXCEEDED: reply lost");
        },
        login: async (req) => {
            node_assert_1.strict.equal(req.username, "svc");
            node_assert_1.strict.equal(req.password, "pw");
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
    const project = bareProject({ store, auth, core: credSpy() });
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
        node_assert_1.strict.equal(sent.at(-1), `access-${cycle}`, `cycle ${cycle}: the call carried the re-login bearer`);
    }
    node_assert_1.strict.deepEqual(presented, ["refresh-0", "refresh-1", "refresh-2"], "each refresh token presented exactly once");
    node_assert_1.strict.equal(logins, 3);
    node_assert_1.strict.equal(project.refreshError(), null);
});
(0, node_test_1.test)("refresh-only session: a refused refresh token fails calls locally with SessionExpiredError", async () => {
    const presented = [];
    const auth = {
        ...credSpy(),
        refreshToken: async (req) => {
            presented.push(req.refresh_token);
            if (presented.length === 1)
                throw grpcError(4, "4 DEADLINE_EXCEEDED: reply lost");
            throw grpcError(16, "16 UNAUTHENTICATED: invalid refresh token");
        },
    };
    const store = memoryStore({
        accessToken: "token-1",
        refreshToken: "refresh-1",
        issuedAt: Date.now() - 900_000,
        expiresAt: Date.now() - 1_000,
    });
    const project = bareProject({ store, auth, core: credSpy() });
    const reported = [];
    project.config.onRefreshError = (err) => reported.push(err);
    const { core, sent } = gatedCore(project);
    const isExpired = (err) => err instanceof project_1.SessionExpiredError &&
        err.code === 16 &&
        /invalid refresh token/.test(err.cause?.message ?? "");
    await node_assert_1.strict.rejects(core.unary("udb.services.v1.DataBroker", "Select", {}), isExpired);
    node_assert_1.strict.equal(sent.length, 0, "no call left the client");
    node_assert_1.strict.equal(reported.length, 1, "onRefreshError fired");
    node_assert_1.strict.match(reported[0].message, /invalid refresh token/);
    node_assert_1.strict.equal(core.calls.at(-1)?.bearerToken, undefined, "the dead bearer was dropped");
    node_assert_1.strict.throws(() => core.serverStream("svc", "Watch", {}), (err) => err instanceof project_1.SessionExpiredError);
    // Past the backoff the refused token is still never resubmitted.
    project.renewalRetryAt = 0;
    await node_assert_1.strict.rejects(core.unary("udb.services.v1.DataBroker", "Select", {}), isExpired);
    node_assert_1.strict.deepEqual(presented, ["refresh-1", "refresh-1"], "one transient retry, then the refusal is final");
    node_assert_1.strict.equal(reported.length, 1, "the same failure is reported once");
    node_assert_1.strict.equal(sent.length, 0);
});
(0, node_test_1.test)("concurrent calls at expiry share exactly one renewal", async () => {
    let exchanges = 0;
    const auth = {
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
    await Promise.all(Array.from({ length: 8 }, () => project.core.unary("udb.services.v1.DataBroker", "Select", {})));
    node_assert_1.strict.equal(exchanges, 2, "connect + ONE renewal for eight concurrent callers");
    node_assert_1.strict.deepEqual(sent, Array(8).fill("bearer-2"), "every caller carried the renewed bearer");
});
