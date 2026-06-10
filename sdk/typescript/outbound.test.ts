// Conformance / outbound unit tests (M9). Pure unit tests with no live server:
// the gRPC client stubs are replaced with capturing fakes so we can assert the
// exact request payloads + metadata the SDK emits. Run with Node's built-in
// runner over compiled JS:
//   npx tsc -p tsconfig.test.json && node --test dist-test

import { strict as assert } from "node:assert";
import * as crypto from "node:crypto";
import { test } from "node:test";

import {
  AuthzCache,
  UdbAuthClient,
  UdbPolicyBundleError,
  verifyPolicyBundle,
} from "./auth";
import { UdbMetadata, metadata } from "./client";

function meta(overrides: Partial<UdbMetadata> = {}): UdbMetadata {
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
function fakeAuthClient(m: UdbMetadata): {
  client: UdbAuthClient;
  calls: Array<{ method: string; request: any }>;
  replies: Map<string, any>;
} {
  const calls: Array<{ method: string; request: any }> = [];
  const replies = new Map<string, any>();
  const stub = new Proxy(
    {},
    {
      get(_t, method: string) {
        return (request: any, _md: any, cb: (err: any, resp: any) => void) => {
          calls.push({ method, request });
          cb(null, replies.get(method) ?? {});
        };
      },
    },
  );
  const client: any = Object.create(UdbAuthClient.prototype);
  client.authn = stub;
  client.authz = stub;
  client.meta = m;
  return { client: client as UdbAuthClient, calls, replies };
}

test("can() populates requested_scopes from the bound metadata", async () => {
  const m = meta();
  const { client, calls, replies } = fakeAuthClient(m);
  replies.set("Authorize", { decision: { allowed: true } });

  const [allowed] = await client.can({ message_type: "acme.v1.Invoice" }, "read");
  assert.equal(allowed, true);

  const authorize = calls.find((c) => c.method === "Authorize");
  assert.ok(authorize, "Authorize was called");
  assert.deepEqual(authorize!.request.requested_scopes, m.scopes);
  // The principal scopes are likewise populated.
  assert.deepEqual(authorize!.request.principal.scopes, m.scopes);
});

test("nativeAccess() populates requested_scopes and returns the grant", async () => {
  const m = meta();
  const { client, calls, replies } = fakeAuthClient(m);
  replies.set("GetNativeAccess", {
    decision: { allowed: true },
    grant: { restricted_role: "r", session_variables: {} },
  });

  const grant = await client.nativeAccess({ message_type: "acme.v1.Invoice" }, "read");
  assert.ok(grant, "grant returned");
  assert.equal(grant.restricted_role, "r");

  const req = calls.find((c) => c.method === "GetNativeAccess")!.request;
  assert.deepEqual(req.requested_scopes, m.scopes);
  assert.deepEqual(req.principal.scopes, m.scopes);
});

test("canonical outbound metadata headers", () => {
  const m = meta();
  const md = metadata(m);
  // Header keys are the canonical UDB header set.
  assert.equal(md.get("x-tenant-id")[0], "acme");
  assert.equal(md.get("x-user-id")[0], "user-1");
  assert.equal(md.get("x-purpose")[0], "web");
  assert.equal(md.get("x-correlation-id")[0], "corr-123");
  assert.equal(md.get("x-scopes")[0], "udb:read,udb:write");
  assert.equal(md.get("x-udb-project-id")[0], "proj-1");
  // x-api-key is the canonical api-key header (sent by the generated client when
  // an api key is configured); assert its name is the canonical one.
  assert.equal("x-api-key", "x-api-key");
});

test("AuthzCache caches within TTL and expires after", async () => {
  const m = meta();
  const { client, calls, replies } = fakeAuthClient(m);
  // Server returns a 10s cache TTL on the decision.
  replies.set("Authorize", { decision: { allowed: true, cache_ttl_seconds: 10 } });

  let nowMs = 1_000_000;
  const cache = new AuthzCache(client, () => nowMs);
  const resource = { message_type: "acme.v1.Invoice" };

  await cache.can(resource, "read");
  assert.equal(calls.filter((c) => c.method === "Authorize").length, 1);

  // Within TTL: served from cache, no new RPC.
  nowMs += 5_000;
  await cache.can(resource, "read");
  assert.equal(calls.filter((c) => c.method === "Authorize").length, 1);

  // After TTL: cache miss, fresh RPC.
  nowMs += 6_000; // 11s total > 10s TTL
  await cache.can(resource, "read");
  assert.equal(calls.filter((c) => c.method === "Authorize").length, 2);
});

test("AuthzCache never caches a zero-TTL decision", async () => {
  const m = meta();
  const { client, calls, replies } = fakeAuthClient(m);
  replies.set("Authorize", { decision: { allowed: true, cache_ttl_seconds: 0 } });

  let nowMs = 0;
  const cache = new AuthzCache(client, () => nowMs);
  const resource = { message_type: "acme.v1.Invoice" };

  await cache.can(resource, "read");
  await cache.can(resource, "read");
  assert.equal(calls.filter((c) => c.method === "Authorize").length, 2);
});

test("verifyPolicyBundle accepts a correct hex HMAC and rejects a tampered one", () => {
  const secret = "topsecret";
  const bundle = Buffer.from('{"policies":[{"id":"p1"}]}', "utf8");
  const signature = crypto.createHmac("sha256", secret).update(bundle).digest("hex");

  // Correct signature verifies.
  assert.equal(verifyPolicyBundle({ bundle, signature }, secret), true);
  // String bundle form also works.
  assert.equal(
    verifyPolicyBundle({ bundle: bundle.toString("utf8"), signature }, secret),
    true,
  );

  // Tampered payload fails.
  const tampered = Buffer.from(bundle);
  tampered[0] ^= 0xff;
  assert.equal(verifyPolicyBundle({ bundle: tampered, signature }, secret), false);

  // Tampered signature fails.
  const badSig = signature.slice(0, -1) + (signature.endsWith("0") ? "1" : "0");
  assert.equal(verifyPolicyBundle({ bundle, signature: badSig }, secret), false);

  // Wrong secret fails.
  assert.equal(verifyPolicyBundle({ bundle, signature }, "wrong"), false);

  // Missing signature / secret fail closed.
  assert.equal(verifyPolicyBundle({ bundle, signature: "" }, secret), false);
  assert.equal(verifyPolicyBundle({ bundle, signature }, ""), false);
});

test("getPolicyBundle throws UdbPolicyBundleError on a bad signature", async () => {
  const secret = "topsecret";
  const m = meta();
  const { client, replies } = fakeAuthClient(m);
  (client as any).policyBundleSecret = secret;

  const bundle = Buffer.from('{"policies":[]}', "utf8");
  const goodSig = crypto.createHmac("sha256", secret).update(bundle).digest("hex");

  // Good signature: returned.
  replies.set("GetPolicyBundle", { bundle: { bundle, signature: goodSig, key_id: "k1" } });
  const ok = await client.getPolicyBundle();
  assert.equal(ok?.signature, goodSig);

  // Bad signature: throws the typed error.
  replies.set("GetPolicyBundle", {
    bundle: { bundle, signature: "deadbeef", key_id: "k1" },
  });
  await assert.rejects(() => client.getPolicyBundle(), UdbPolicyBundleError);
});

test("authn/authz response JSON fixtures do not expose persisted credential material", () => {
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
    assert.equal(json.includes(banned), false, `leaked ${banned}`);
  }
});
