"use strict";
// Live refresh-token rotation conformance. Skipped unless UDB_LIVE_SDK_TESTS=1;
// CI's "SDK refresh-token rotation (live)" job runs it against the broker it
// just built, on every push to main.
//
// The broker rotates the refresh token on every AuthnService.RefreshToken call
// and each one works once. An SDK that kept the token it logged in with
// refreshed once and then failed with UNAUTHENTICATED "invalid credential" —
// a production outage no single-refresh test could catch. This drives the
// SDK's own refresh path (UdbProject.refreshIfNeeded) several times in a row.
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
const node_test_1 = require("node:test");
const grpc = __importStar(require("@grpc/grpc-js"));
const project_1 = require("./project");
const REFRESHES = 3;
function requiredEnv(name) {
    const value = process.env[name]?.trim();
    if (!value)
        throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
    return value;
}
function memoryStore() {
    let token = null;
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
(0, node_test_1.test)("live refresh token rotation survives repeated refreshes", {
    skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker",
}, async () => {
    const target = requiredEnv("UDB_GRPC_TARGET");
    const authTarget = process.env.UDB_AUTH_GRPC_TARGET?.trim() || target;
    const tenantId = process.env.UDB_LIVE_TENANT || "sdk-live";
    const projectId = process.env.UDB_LIVE_PROJECT || "default";
    const store = memoryStore();
    const project = new project_1.UdbProject({
        target,
        authTarget,
        tenantId,
        projectId,
        purpose: "ts.live.refresh-rotation",
        tokenStore: store,
        deadlineMs: 10_000,
    });
    try {
        await project.loginAndAdoptTenant({
            username: requiredEnv("UDB_LIVE_USERNAME"),
            password: requiredEnv("UDB_LIVE_PASSWORD"),
            tenant_hint: tenantId,
            project_hint: projectId,
            device_name: "ts-sdk-refresh-rotation",
        });
        const first = store.current()?.refreshToken;
        node_assert_1.strict.ok(first, "login must store a refresh token");
        const authn = (project.authGenerated ?? project.generated).AuthnService;
        const opts = { deadlineMs: 8_000, noRetry: true };
        let previous = first;
        for (let i = 1; i <= REFRESHES; i += 1) {
            // Expire the stored access token so refreshIfNeeded performs a real refresh.
            await store.save({ ...store.current(), expiresAt: Date.now() - 1_000 });
            const next = await project.refreshIfNeeded();
            node_assert_1.strict.ok(next?.accessToken, `refresh ${i} stored no access token`);
            node_assert_1.strict.ok(next.refreshToken, `refresh ${i} stored no refresh token`);
            node_assert_1.strict.notEqual(next.refreshToken, previous, `refresh ${i}: the refresh token did not change — the broker stopped rotating it or the SDK kept the spent one`);
            node_assert_1.strict.equal(store.current()?.refreshToken, next.refreshToken, `refresh ${i}: rotated token not persisted`);
            // An authenticated call carrying the bearer the refresh installed.
            const validated = await authn.validate_token({ token: next.accessToken, token_type: "TOKEN_TYPE_JWT_ACCESS" }, opts);
            node_assert_1.strict.equal(validated.valid, true, `refresh ${i}: the refreshed access token does not validate`);
            previous = next.refreshToken;
        }
        // The first refresh token was spent three rotations ago: the broker must
        // refuse it. (Replaying the immediately-previous token instead would trip
        // reuse detection and revoke every session of this shared live user.)
        await node_assert_1.strict.rejects(project.auth.refreshToken({ refresh_token: first }), (err) => {
            node_assert_1.strict.equal(err?.code, grpc.status.UNAUTHENTICATED, `spent refresh token returned ${err?.code}: ${err?.message}`);
            return true;
        }, "a spent refresh token was accepted");
    }
    finally {
        project.close();
    }
});
