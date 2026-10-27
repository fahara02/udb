// Live refresh-token rotation conformance. Skipped unless UDB_LIVE_SDK_TESTS=1;
// CI's "SDK refresh-token rotation (live)" job runs it against the broker it
// just built, on every push to main.
//
// The broker rotates the refresh token on every AuthnService.RefreshToken call
// and each one works once. An SDK that kept the token it logged in with
// refreshed once and then failed with UNAUTHENTICATED "invalid credential" —
// a production outage no single-refresh test could catch. This drives the
// SDK's own refresh path (UdbProject.refreshIfNeeded) several times in a row.

import { strict as assert } from "node:assert";
import { test } from "node:test";
import * as grpc from "@grpc/grpc-js";

import { StoredToken, UdbProject } from "./project";

const REFRESHES = 3;

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
  return value;
}

function memoryStore() {
  let token: StoredToken | null = null;
  return {
    load: async () => token,
    save: async (next: StoredToken) => {
      token = next;
    },
    clear: async () => {
      token = null;
    },
    current: () => token,
  };
}

test("live refresh token rotation survives repeated refreshes", {
  skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker",
}, async () => {
  const target = requiredEnv("UDB_GRPC_TARGET");
  const authTarget = process.env.UDB_AUTH_GRPC_TARGET?.trim() || target;
  const tenantId = process.env.UDB_LIVE_TENANT || "sdk-live";
  const projectId = process.env.UDB_LIVE_PROJECT || "default";
  const store = memoryStore();
  const project = new UdbProject({
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
    assert.ok(first, "login must store a refresh token");

    const authn = ((project as any).authGenerated ?? project.generated).AuthnService;
    const opts = { deadlineMs: 8_000, noRetry: true };
    let previous = first;
    for (let i = 1; i <= REFRESHES; i += 1) {
      // Expire the stored access token so refreshIfNeeded performs a real refresh.
      await store.save({ ...store.current()!, expiresAt: Date.now() - 1_000 });
      const next = await project.refreshIfNeeded();
      assert.ok(next?.accessToken, `refresh ${i} stored no access token`);
      assert.ok(next.refreshToken, `refresh ${i} stored no refresh token`);
      assert.notEqual(
        next.refreshToken,
        previous,
        `refresh ${i}: the refresh token did not change — the broker stopped rotating it or the SDK kept the spent one`,
      );
      assert.equal(store.current()?.refreshToken, next.refreshToken, `refresh ${i}: rotated token not persisted`);

      // An authenticated call carrying the bearer the refresh installed.
      const validated = await authn.validate_token(
        { token: next.accessToken, token_type: "TOKEN_TYPE_JWT_ACCESS" },
        opts,
      );
      assert.equal(validated.valid, true, `refresh ${i}: the refreshed access token does not validate`);
      previous = next.refreshToken;
    }

    // The first refresh token was spent three rotations ago: the broker must
    // refuse it. (Replaying the immediately-previous token instead would trip
    // reuse detection and revoke every session of this shared live user.)
    await assert.rejects(
      project.auth.refreshToken({ refresh_token: first }),
      (err: any) => {
        assert.equal(err?.code, grpc.status.UNAUTHENTICATED, `spent refresh token returned ${err?.code}: ${err?.message}`);
        return true;
      },
      "a spent refresh token was accepted",
    );
  } finally {
    project.close();
  }
});
