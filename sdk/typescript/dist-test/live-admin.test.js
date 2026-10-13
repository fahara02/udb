"use strict";
// Live coverage of the admin-portal flows consumer consoles build on (T3):
// inviting a user who sets their own password, listing pending invites, and
// the feature-flag editor (put / read / delete). Skipped unless
// UDB_LIVE_SDK_TESTS=1, like the other broker-backed suites.
Object.defineProperty(exports, "__esModule", { value: true });
const node_assert_1 = require("node:assert");
const node_test_1 = require("node:test");
const project_1 = require("./project");
const store_conformance_1 = require("./test-support/store-conformance");
(0, node_test_1.test)("live Table meets the shared conformance cases", {
    skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker",
}, async (t) => {
    const target = requiredEnv("UDB_GRPC_TARGET");
    const projectId = process.env.UDB_LIVE_PROJECT || "default";
    const tenantId = process.env.UDB_LIVE_TENANT || "sdk-live";
    const project = new project_1.UdbProject({ target, authTarget: process.env.UDB_AUTH_GRPC_TARGET || target,
        tenantId, projectId, purpose: "ts.live.store-conformance", tokenStore: memoryStore(), deadlineMs: 10_000 });
    try {
        await project.loginAndAdoptTenant({ username: requiredEnv("UDB_LIVE_USERNAME"), password: requiredEnv("UDB_LIVE_PASSWORD"),
            tenant_hint: tenantId, project_hint: projectId, device_name: "ts-store-conformance" });
        await (0, store_conformance_1.runStoreCases)(t, () => project.core);
    }
    finally {
        project.close();
    }
});
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
    };
}
(0, node_test_1.test)("admin portal flows: invite, pending list, feature flags", {
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
        const probe = new project_1.UdbProject({ target, authTarget, tenantId, projectId, purpose: "ts.live.admin-probe", tokenStore: memoryStore(), deadlineMs: 10_000 });
        try {
            const login = await probe.login({ username, password, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-admin-probe" });
            const who = await probe.auth.authenticateBearer(login.access_token);
            tenantId = who?.principal?.tenant_id || tenantId;
        }
        finally {
            probe.close();
        }
    }
    const project = new project_1.UdbProject({ target, authTarget, tenantId, projectId, purpose: "ts.live.admin", tokenStore: memoryStore(), deadlineMs: 10_000 });
    try {
        await project.login({ username, password, tenant_hint: tenantId, project_hint: projectId, device_name: "ts-admin-portal" });
        const gen = project.authGenerated ?? project.generated;
        const opts = { deadlineMs: 8_000, noRetry: true };
        const suffix = `${process.pid}${Date.now()}`;
        // Invite: no password now; the user sets one with the emailed reset code.
        const invited = await gen.AuthnService.create_user({
            username: `invitee-${suffix}`,
            email: `invitee-${suffix}@example.com`,
            password: "",
            password_setup_required: true,
            tenant_id: tenantId,
            project_id: projectId,
            full_name: "Invited User",
        }, opts);
        node_assert_1.strict.ok(invited.otp_id, "an invite issues a password-setup code");
        node_assert_1.strict.equal(invited.user?.status, "USER_STATUS_PASSWORD_SETUP_REQUIRED");
        // The invited account cannot log in before setting a password.
        await node_assert_1.strict.rejects(project.auth.login({ username: `invitee-${suffix}`, password: "Guess-Password-123!", tenant_hint: tenantId, project_hint: projectId, device_name: "invitee" }));
        // Pending invites are listable by status.
        const pending = await gen.AuthnService.list_users({ tenant_id: tenantId, status: "USER_STATUS_PASSWORD_SETUP_REQUIRED", page: { page_size: 200 } }, opts);
        node_assert_1.strict.ok((pending.users ?? []).some((user) => user.user_id === invited.user.user_id), "the invite appears in the pending list");
        // Feature-flag editor: put, read back, delete.
        const flagKey = `ts_live_flag_${suffix}`;
        const scope = { tenant_id: tenantId, project_id: projectId, environment: "live", flag_key: flagKey };
        await gen.ConfigService.put_flag({ ...scope, value: { bool_value: true }, enabled: true, rollout_percentage: 100 }, opts);
        const read = await gen.ConfigService.get_flag(scope, opts);
        node_assert_1.strict.ok(read.found, "the flag reads back");
        node_assert_1.strict.equal(read.flag?.enabled, true);
        await gen.ConfigService.delete_flag(scope, opts);
        const gone = await gen.ConfigService.get_flag(scope, opts);
        node_assert_1.strict.equal(Boolean(gone.found), false, "the deleted flag is gone");
    }
    finally {
        project.close();
    }
});
