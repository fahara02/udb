"use strict";
// Conformance / outbound unit tests (M9). Pure unit tests with no live server:
// the gRPC client stubs are replaced with capturing fakes so we can assert the
// exact request payloads + metadata the SDK emits. Run with Node's built-in
// runner over compiled JS:
//   npx tsc -p tsconfig.test.json && node --test dist-test
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
const crypto = __importStar(require("node:crypto"));
const node_test_1 = require("node:test");
const auth_1 = require("./auth");
const client_1 = require("./client");
function meta(overrides = {}) {
    return {
        tenantId: "acme",
        purpose: "web",
        correlationId: "corr-123",
        scopes: ["udb:read", "udb:write"],
        userId: "user-1",
        projectId: "proj-1",
        serviceIdentity: "test.service",
        ...overrides,
    };
}
// Build a UdbAuthClient without touching the real proto loader / gRPC channel.
// `Object.create` skips the constructor; we inject capturing fake stubs and the
// metadata directly, mirroring the private fields the methods read.
function fakeAuthClient(m) {
    const calls = [];
    const replies = new Map();
    const stub = new Proxy({}, {
        get(_t, method) {
            return (request, _md, cb) => {
                calls.push({ method, request });
                cb(null, replies.get(method) ?? {});
            };
        },
    });
    const client = Object.create(auth_1.UdbAuthClient.prototype);
    client.authn = stub;
    client.authz = stub;
    client.meta = m;
    return { client: client, calls, replies };
}
(0, node_test_1.test)("can() populates requested_scopes from the bound metadata", async () => {
    const m = meta();
    const { client, calls, replies } = fakeAuthClient(m);
    replies.set("Authorize", { decision: { allowed: true } });
    const [allowed] = await client.can({ message_type: "acme.v1.Invoice" }, "read");
    node_assert_1.strict.equal(allowed, true);
    const authorize = calls.find((c) => c.method === "Authorize");
    node_assert_1.strict.ok(authorize, "Authorize was called");
    node_assert_1.strict.deepEqual(authorize.request.requested_scopes, m.scopes);
    // The principal scopes are likewise populated.
    node_assert_1.strict.deepEqual(authorize.request.principal.scopes, m.scopes);
});
(0, node_test_1.test)("nativeAccess() populates requested_scopes and returns the grant", async () => {
    const m = meta();
    const { client, calls, replies } = fakeAuthClient(m);
    replies.set("GetNativeAccess", {
        decision: { allowed: true },
        grant: { restricted_role: "r", session_variables: {} },
    });
    const grant = await client.nativeAccess({ message_type: "acme.v1.Invoice" }, "read");
    node_assert_1.strict.ok(grant, "grant returned");
    node_assert_1.strict.equal(grant.restricted_role, "r");
    const req = calls.find((c) => c.method === "GetNativeAccess").request;
    node_assert_1.strict.deepEqual(req.requested_scopes, m.scopes);
    node_assert_1.strict.deepEqual(req.principal.scopes, m.scopes);
});
(0, node_test_1.test)("canonical outbound metadata headers", () => {
    const m = meta();
    const md = (0, client_1.metadata)(m);
    // Header keys are the canonical UDB header set.
    node_assert_1.strict.equal(md.get("x-tenant-id")[0], "acme");
    node_assert_1.strict.equal(md.get("x-user-id")[0], "user-1");
    node_assert_1.strict.equal(md.get("x-purpose")[0], "web");
    node_assert_1.strict.equal(md.get("x-correlation-id")[0], "corr-123");
    node_assert_1.strict.equal(md.get("x-scopes")[0], "udb:read,udb:write");
    node_assert_1.strict.equal(md.get("x-udb-project-id")[0], "proj-1");
    // x-api-key is the canonical api-key header (sent by the generated client when
    // an api key is configured); assert its name is the canonical one.
    node_assert_1.strict.equal("x-api-key", "x-api-key");
});
(0, node_test_1.test)("AuthzCache caches within TTL and expires after", async () => {
    const m = meta();
    const { client, calls, replies } = fakeAuthClient(m);
    // Server returns a 10s cache TTL on the decision.
    replies.set("Authorize", { decision: { allowed: true, cache_ttl_seconds: 10 } });
    let nowMs = 1_000_000;
    const cache = new auth_1.AuthzCache(client, () => nowMs);
    const resource = { message_type: "acme.v1.Invoice" };
    await cache.can(resource, "read");
    node_assert_1.strict.equal(calls.filter((c) => c.method === "Authorize").length, 1);
    // Within TTL: served from cache, no new RPC.
    nowMs += 5_000;
    await cache.can(resource, "read");
    node_assert_1.strict.equal(calls.filter((c) => c.method === "Authorize").length, 1);
    // After TTL: cache miss, fresh RPC.
    nowMs += 6_000; // 11s total > 10s TTL
    await cache.can(resource, "read");
    node_assert_1.strict.equal(calls.filter((c) => c.method === "Authorize").length, 2);
});
(0, node_test_1.test)("AuthzCache never caches a zero-TTL decision", async () => {
    const m = meta();
    const { client, calls, replies } = fakeAuthClient(m);
    replies.set("Authorize", { decision: { allowed: true, cache_ttl_seconds: 0 } });
    let nowMs = 0;
    const cache = new auth_1.AuthzCache(client, () => nowMs);
    const resource = { message_type: "acme.v1.Invoice" };
    await cache.can(resource, "read");
    await cache.can(resource, "read");
    node_assert_1.strict.equal(calls.filter((c) => c.method === "Authorize").length, 2);
});
(0, node_test_1.test)("verifyPolicyBundle accepts a correct hex HMAC and rejects a tampered one", () => {
    const secret = "topsecret";
    const bundle = Buffer.from('{"policies":[{"id":"p1"}]}', "utf8");
    const signature = crypto.createHmac("sha256", secret).update(bundle).digest("hex");
    // Correct signature verifies.
    node_assert_1.strict.equal((0, auth_1.verifyPolicyBundle)({ bundle, signature }, secret), true);
    // String bundle form also works.
    node_assert_1.strict.equal((0, auth_1.verifyPolicyBundle)({ bundle: bundle.toString("utf8"), signature }, secret), true);
    // Tampered payload fails.
    const tampered = Buffer.from(bundle);
    tampered[0] ^= 0xff;
    node_assert_1.strict.equal((0, auth_1.verifyPolicyBundle)({ bundle: tampered, signature }, secret), false);
    // Tampered signature fails.
    const badSig = signature.slice(0, -1) + (signature.endsWith("0") ? "1" : "0");
    node_assert_1.strict.equal((0, auth_1.verifyPolicyBundle)({ bundle, signature: badSig }, secret), false);
    // Wrong secret fails.
    node_assert_1.strict.equal((0, auth_1.verifyPolicyBundle)({ bundle, signature }, "wrong"), false);
    // Missing signature / secret fail closed.
    node_assert_1.strict.equal((0, auth_1.verifyPolicyBundle)({ bundle, signature: "" }, secret), false);
    node_assert_1.strict.equal((0, auth_1.verifyPolicyBundle)({ bundle, signature }, ""), false);
});
(0, node_test_1.test)("getPolicyBundle throws UdbPolicyBundleError on a bad signature", async () => {
    const secret = "topsecret";
    const m = meta();
    const { client, replies } = fakeAuthClient(m);
    client.policyBundleSecret = secret;
    const bundle = Buffer.from('{"policies":[]}', "utf8");
    const goodSig = crypto.createHmac("sha256", secret).update(bundle).digest("hex");
    // Good signature: returned.
    replies.set("GetPolicyBundle", { bundle: { bundle, signature: goodSig, key_id: "k1" } });
    const ok = await client.getPolicyBundle();
    node_assert_1.strict.equal(ok?.signature, goodSig);
    // Bad signature: throws the typed error.
    replies.set("GetPolicyBundle", {
        bundle: { bundle, signature: "deadbeef", key_id: "k1" },
    });
    await node_assert_1.strict.rejects(() => client.getPolicyBundle(), auth_1.UdbPolicyBundleError);
});
(0, node_test_1.test)("authn/authz response JSON fixtures do not expose persisted credential material", () => {
    const responses = [
        { user: { user_id: "u1", username: "ada" } },
        { session: { session_id: "sesspub_abc" } },
        { key: { key_id: "udbk_abc", key_prefix: "udbk_abc" } },
        { audits: [{ decision_audit_id: "a1", user_id: "u1", reason: "denied" }] },
    ];
    const json = JSON.stringify(responses);
    for (const banned of [
        "argon2id$",
        "hmac-sha256:",
        "sessionTokenHash",
        "session_token_hash",
        "sessionTokenLookup",
        "session_token_lookup",
        "csrfTokenHash",
        "csrf_token_hash",
        "passwordHash",
        "password_hash",
        "totpSecretEnc",
        "totp_secret_enc",
        "keyHash",
        "key_hash",
    ]) {
        node_assert_1.strict.equal(json.includes(banned), false, `leaked ${banned}`);
    }
});
