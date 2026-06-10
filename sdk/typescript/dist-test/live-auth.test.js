"use strict";
// Broker-backed SDK conformance for urgent_fix #20.
//
// This test is intentionally skipped unless UDB_LIVE_SDK_TESTS=1. CI starts a
// real broker, seeds the first user through `udb auth bootstrap user`, then runs
// this through the normal TypeScript test build.
Object.defineProperty(exports, "__esModule", { value: true });
const node_assert_1 = require("node:assert");
const node_test_1 = require("node:test");
const project_1 = require("./project");
function requiredEnv(name) {
    const value = process.env[name]?.trim();
    if (!value)
        throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
    return value;
}
function memoryStore(initial = null) {
    let token = initial;
    return {
        load: async () => token,
        save: async (next) => {
            token = next;
        },
        clear: async () => {
            token = null;
        },
        current: () => token,
    };
}
(0, node_test_1.test)("live broker login refreshes once and hot-swaps SDK credentials", {
    skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker",
}, async () => {
    const target = requiredEnv("UDB_GRPC_TARGET");
    const username = requiredEnv("UDB_LIVE_USERNAME");
    const password = requiredEnv("UDB_LIVE_PASSWORD");
    const tenantId = process.env.UDB_LIVE_TENANT || "sdk-live";
    const projectId = process.env.UDB_LIVE_PROJECT || "default";
    const store = memoryStore();
    const project = new project_1.UdbProject({
        target,
        tenantId,
        projectId,
        scopes: ["udb:admin"],
        tokenStore: store,
        deadlineMs: 10_000,
    });
    const login = await project.login({
        username,
        password,
        tenant_hint: tenantId,
        project_hint: projectId,
        device_name: "sdk-live-conformance",
    });
    node_assert_1.strict.ok(login.access_token, "live login must return an access token");
    node_assert_1.strict.ok(login.refresh_token, "live login must return a refresh token");
    node_assert_1.strict.equal(store.current()?.accessToken, login.access_token);
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
    node_assert_1.strict.equal(accessTokens.size, 1, "concurrent refresh callers must share one result");
    node_assert_1.strict.notEqual(refreshed[0]?.accessToken, login.access_token);
    node_assert_1.strict.equal(store.current()?.accessToken, refreshed[0]?.accessToken);
});
