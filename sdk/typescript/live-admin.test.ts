// Live coverage of the admin-portal flows consumer consoles build on (T3):
// inviting a user who sets their own password, listing pending invites, and
// the feature-flag editor (put / read / delete). Skipped unless
// UDB_LIVE_SDK_TESTS=1, like the other broker-backed suites.

import { strict as assert } from "node:assert";
import { test } from "node:test";

import { StoredToken, UdbProject } from "./project";
import { runStoreCases } from "./test-support/store-conformance";

test("live Table meets the shared conformance cases", {
  skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker",
}, async (t) => {
  const target = requiredEnv("UDB_GRPC_TARGET");
  const projectId = process.env.UDB_LIVE_PROJECT || "default";
  const tenantId = process.env.UDB_LIVE_TENANT || "sdk-live";
  const project = new UdbProject({ target, authTarget: process.env.UDB_AUTH_GRPC_TARGET || target,
    tenantId, projectId, purpose: "ts.live.store-conformance", tokenStore: memoryStore(), deadlineMs: 10_000 });
  try {
    await project.loginAndAdoptTenant({ username: requiredEnv("UDB_LIVE_USERNAME"), password: requiredEnv("UDB_LIVE_PASSWORD"),
      tenant_hint: tenantId, project_hint: projectId, device_name: "ts-store-conformance" });
    await runStoreCases(t, () => project.core);
  } finally { project.close(); }
});

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
  };
}

test("admin portal flows: invite, pending list, feature flags", {
  skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker",
}, async () => {
  const target = requiredEnv("UDB_GRPC_TARGET");
  const authTarget = process.env.UDB_AUTH_GRPC_TARGET?.trim() || target;
  const username = requiredEnv("UDB_LIVE_USERNAME");
  const password = requiredEnv("UDB_LIVE_PASSWORD");
  const projectId = process.env.UDB_LIVE_PROJECT || "default";
  let tenantId = process.env.UDB_LIVE_TENANT || "sdk-live";

  // Resolve the canonical tenant UUID the admin's token carries.
  {
    const probe = new UdbProject({ target, authTarget, tenantId, projectId, purpose: "ts.live.admin-probe", tokenStore: memoryStore(), deadlineMs: 10_000 });
    try {
      const login = await probe.login({ username, password, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-admin-probe" });
      const who = await probe.auth.authenticateBearer(login.access_token);
      tenantId = who?.principal?.tenant_id || tenantId;
    } finally {
      probe.close();
    }
  }

  const project = new UdbProject({ target, authTarget, tenantId, projectId, purpose: "ts.live.admin", tokenStore: memoryStore(), deadlineMs: 10_000 });
  try {
    await project.login({ username, password, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-admin-portal" });
    const gen = (project as any).authGenerated ?? project.generated;
    const opts = { deadlineMs: 8_000, noRetry: true };
    const suffix = `${process.pid}${Date.now()}`;

    // Invite: no password now; the user sets one with the emailed reset code.
    const invited = await gen.AuthnService.create_user(
      {
        username: `invitee-${suffix}`,
        email: `invitee-${suffix}@example.com`,
        password: "",
        password_setup_required: true,
        tenant_id: tenantId,
        project_id: projectId,
        full_name: "Invited User",
      },
      opts,
    );
    assert.ok(invited.otp_id, "an invite issues a password-setup code");
    assert.equal(invited.user?.status, "USER_STATUS_PASSWORD_SETUP_REQUIRED");

    // The invited account cannot log in before setting a password.
    await assert.rejects(
      project.auth.login({ username: `invitee-${suffix}`, password: "Guess-Password-123!", tenant_hint: tenantId, project_hint: projectId, device_name: "invitee" }),
    );

    // Pending invites are listable by status.
    const pending = await gen.AuthnService.list_users(
      { tenant_id: tenantId, status: "USER_STATUS_PASSWORD_SETUP_REQUIRED", page: { page_size: 200 } },
      opts,
    );
    assert.ok(
      (pending.users ?? []).some((user: any) => user.user_id === invited.user.user_id),
      "the invite appears in the pending list",
    );

    // Feature-flag editor: put, read back, delete.
    const flagKey = `ts_live_flag_${suffix}`;
    const scope = { tenant_id: tenantId, project_id: projectId, environment: "live", flag_key: flagKey };
    await gen.ConfigService.put_flag({ ...scope, value: { bool_value: true }, enabled: true, rollout_percentage: 100 }, opts);
    const read = await gen.ConfigService.get_flag(scope, opts);
    assert.ok(read.found, "the flag reads back");
    assert.equal(read.flag?.enabled, true);
    await gen.ConfigService.delete_flag(scope, opts);
    const gone = await gen.ConfigService.get_flag(scope, opts);
    assert.equal(Boolean(gone.found), false, "the deleted flag is gone");
  } finally {
    project.close();
  }
});
