"use strict";
// Live service-account delegation conformance (UDB 0.6.4). Skipped unless
// UDB_LIVE_SDK_TESTS=1; CI's "SDK service accounts (live)" job runs it against
// the broker it just built, after scripts/provision-service-account-live.py has
// created the identities it reads from the environment:
//
//   npm ci && npm run bundle-proto && npx tsc -p tsconfig.test.json \
//     && node --test dist-test/live-service-accounts.test.js
//
// What it proves, through the TypeScript SDK's generated clients:
//   1. Authenticate names the account: Principal.username is the live login name.
//   2. A service-account bearer carries the roles bound to the account, never a
//      withheld (admin-segment / platform / owner) role, and its grant's scopes.
//   3. A service account holding `udb:authz:check-access-for` gets CheckAccess
//      evaluated for an active person of its tenant (evaluated_subject is that
//      person); an unknown user id is refused with PERMISSION_DENIED.
//   4. ExchangeServiceTokenForTenant mints a short-lived target-tenant token
//      pinned to an approved membership, that token authenticates as the account
//      in the target tenant and CheckAccess with it evaluates the account itself;
//      a tenant with no membership is refused with PERMISSION_DENIED.
//   5. A phone-only CreateUser (no email) creates a PENDING_VERIFICATION account
//      whose PHONE_VERIFICATION OTP, verified, activates it.
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
const node_crypto_1 = require("node:crypto");
const node_test_1 = require("node:test");
const grpc = __importStar(require("@grpc/grpc-js"));
const client_1 = require("./client");
const generatedClient_1 = require("./generatedClient");
const CHECK_ACCESS_SCOPE = "udb:authz:check-access";
const CHECK_ACCESS_FOR_SCOPE = "udb:authz:check-access-for";
const NEVER_ON_SERVICE_BEARER = ["platform_admin", "organization_owner"];
const EXCHANGED_TOKEN_MAX_LIFETIME_SECONDS = 15 * 60;
const CLOCK_SKEW_SECONDS = 60;
const PHONE_USER_PASSWORD = "SdkLivePhone#2026";
const CALL = { deadlineMs: 10_000, noRetry: true };
function requiredEnv(name) {
    const value = process.env[name]?.trim();
    if (!value)
        throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
    return value;
}
function grpcCodeOf(err) {
    return err?.code ?? err?.cause?.code;
}
function codeName(code) {
    return code === undefined ? "none" : grpc.status[code] ?? String(code);
}
async function expectPermissionDenied(step, call) {
    await node_assert_1.strict.rejects(call(), (err) => {
        const code = grpcCodeOf(err);
        node_assert_1.strict.equal(code, grpc.status.PERMISSION_DENIED, `${step}: expected PERMISSION_DENIED, got ${codeName(code)}: ${err?.message ?? String(err)}`);
        return true;
    }, `${step}: the call succeeded; expected PERMISSION_DENIED`);
}
function lower(values) {
    return Array.isArray(values) ? values.map((value) => String(value).toLowerCase()) : [];
}
(0, node_test_1.test)("live service account delegation conformance", {
    skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker",
}, async (t) => {
    const target = requiredEnv("UDB_GRPC_TARGET");
    const authTarget = process.env.UDB_AUTH_GRPC_TARGET?.trim() || target;
    const humanUsername = requiredEnv("UDB_LIVE_USERNAME");
    const humanPassword = requiredEnv("UDB_LIVE_PASSWORD");
    const tenantHint = requiredEnv("UDB_LIVE_TENANT");
    const projectHint = requiredEnv("UDB_LIVE_PROJECT");
    const serviceUsername = requiredEnv("UDB_LIVE_SERVICE_USERNAME");
    const servicePassword = requiredEnv("UDB_LIVE_SERVICE_PASSWORD");
    const serviceUserId = requiredEnv("UDB_LIVE_SERVICE_USER_ID");
    const serviceRole = requiredEnv("UDB_LIVE_SERVICE_ROLE");
    const withheldRole = requiredEnv("UDB_LIVE_SERVICE_WITHHELD_ROLE");
    const delegatedUserId = requiredEnv("UDB_LIVE_DELEGATED_USER_ID");
    const targetTenantId = requiredEnv("UDB_LIVE_TARGET_TENANT_ID");
    const targetProjectId = process.env.UDB_LIVE_TARGET_PROJECT?.trim() || projectHint;
    const unapprovedTenantId = requiredEnv("UDB_LIVE_UNAPPROVED_TENANT_ID");
    const clients = [];
    // One generated client per identity: x-tenant-id must be the canonical tenant
    // UUID of the bearer it carries (Login/Authenticate are public and only need
    // the tenant hint). metadata() adds x-request-id / x-correlation-id per call.
    const authClient = (purpose, meta, bearerToken) => {
        const client = new generatedClient_1.UdbGeneratedClient({
            target: authTarget,
            meta: {
                tenantId: meta.tenantId,
                projectId: meta.projectId,
                userId: meta.userId,
                serviceIdentity: meta.serviceIdentity || undefined,
                purpose,
                correlationId: `${purpose}-${(0, node_crypto_1.randomUUID)()}`,
                clientCatalogVersion: client_1.UDB_PROTOCOL_VERSION,
            },
            bearerToken,
            deadlineMs: 10_000,
        });
        clients.push(client);
        return client;
    };
    const publicClient = authClient("ts.live.service-accounts.public", { tenantId: tenantHint, projectId: projectHint });
    const login = async (step, username, password) => {
        const resp = await publicClient.AuthnService.login({
            username,
            password,
            tenant_hint: tenantHint,
            project_hint: projectHint,
            device_name: "ts-sdk-service-accounts",
        }, CALL);
        node_assert_1.strict.ok(resp?.access_token, `${step}: Login(${username}) returned no access token (mfa_required=${resp?.mfa_required})`);
        return resp.access_token;
    };
    const authenticate = async (client, step, bearer) => {
        const resp = await client.AuthnService.authenticate({ bearer_token: bearer }, CALL);
        node_assert_1.strict.ok(resp?.principal, `${step}: Authenticate returned no principal`);
        return resp.principal;
    };
    try {
        // ── 1. Human administrator ─────────────────────────────────────────────
        let humanBearer = "";
        let human = {};
        let homeTenantId = "";
        await t.test("1 human login names the account", async () => {
            humanBearer = await login("step 1", humanUsername, humanPassword);
            human = await authenticate(publicClient, "step 1", humanBearer);
            node_assert_1.strict.equal(String(human.username ?? "").toLowerCase(), humanUsername.toLowerCase(), `step 1: principal.username = ${JSON.stringify(human.username)}, want ${humanUsername}`);
            homeTenantId = human.tenant_id ?? "";
            node_assert_1.strict.ok(homeTenantId, `step 1: principal.tenant_id is empty (principal ${JSON.stringify(human)})`);
        });
        // ── 2. Service-account bearer: identity, bound roles, scopes ───────────
        let serviceBearer = "";
        let service = {};
        await t.test("2 service account bearer carries bound roles and scopes", async () => {
            node_assert_1.strict.ok(homeTenantId, "step 2: step 1 did not resolve the home tenant");
            serviceBearer = await login("step 2", serviceUsername, servicePassword);
            service = await authenticate(publicClient, "step 2", serviceBearer);
            node_assert_1.strict.equal(String(service.username ?? "").toLowerCase(), serviceUsername.toLowerCase(), `step 2: principal.username = ${JSON.stringify(service.username)}, want ${serviceUsername}`);
            node_assert_1.strict.equal(service.user_id, serviceUserId, `step 2: principal.user_id = ${service.user_id}, want ${serviceUserId}`);
            node_assert_1.strict.equal(service.account_kind, "ACCOUNT_KIND_SERVICE_ACCOUNT", `step 2: principal.account_kind = ${service.account_kind}`);
            node_assert_1.strict.equal(service.tenant_id, homeTenantId, `step 2: principal.tenant_id = ${service.tenant_id}, want ${homeTenantId}`);
            const roles = lower(service.roles);
            node_assert_1.strict.ok(roles.includes(serviceRole.toLowerCase()), `step 2: service bearer roles ${JSON.stringify(service.roles)} lack the bound role ${serviceRole}`);
            for (const forbidden of [withheldRole, ...NEVER_ON_SERVICE_BEARER]) {
                node_assert_1.strict.ok(!roles.includes(forbidden.toLowerCase()), `step 2: service bearer roles ${JSON.stringify(service.roles)} carry the withheld role ${forbidden}`);
            }
            node_assert_1.strict.ok((service.scopes ?? []).includes(CHECK_ACCESS_FOR_SCOPE), `step 2: service bearer scopes ${JSON.stringify(service.scopes)} lack ${CHECK_ACCESS_FOR_SCOPE}`);
        });
        const serviceClient = () => authClient("ts.live.service-accounts.service", {
            tenantId: service.tenant_id,
            projectId: service.project_id || projectHint,
            userId: service.user_id,
            serviceIdentity: service.service_identity,
        }, serviceBearer);
        // ── 3. Delegated CheckAccess ────────────────────────────────────────────
        await t.test("3 service account checks access for a person of its tenant", async () => {
            node_assert_1.strict.ok(serviceBearer, "step 3: step 2 did not log the service account in");
            const svc = serviceClient();
            const delegated = await svc.AuthzService.check_access({
                user_id: delegatedUserId,
                object: "sdk-live-chart",
                action: "read",
            }, CALL);
            node_assert_1.strict.equal(delegated?.evaluated_subject, delegatedUserId, `step 3: delegated CheckAccess evaluated_subject = ${JSON.stringify(delegated?.evaluated_subject)}, want ${delegatedUserId}`);
            const unknownUserId = (0, node_crypto_1.randomUUID)();
            await expectPermissionDenied(`step 3: delegated CheckAccess for unknown user ${unknownUserId}`, () => svc.AuthzService.check_access({ user_id: unknownUserId, object: "sdk-live-chart", action: "read" }, CALL));
        });
        // ── 4. Per-tenant service token ─────────────────────────────────────────
        await t.test("4 service account exchanges its home bearer for a target-tenant token", async () => {
            node_assert_1.strict.ok(serviceBearer, "step 4: step 2 did not log the service account in");
            const svc = serviceClient();
            const before = Math.floor(Date.now() / 1000);
            const exchanged = await svc.AuthnService.exchange_service_token_for_tenant({
                target_tenant_id: targetTenantId,
                target_project_id: targetProjectId,
                requested_scopes: [CHECK_ACCESS_SCOPE],
            }, CALL);
            node_assert_1.strict.ok(exchanged?.access_token, "step 4: ExchangeServiceTokenForTenant returned no access_token");
            node_assert_1.strict.equal(exchanged.tenant_id, targetTenantId, `step 4: exchange tenant_id = ${exchanged.tenant_id}, want ${targetTenantId}`);
            node_assert_1.strict.equal(exchanged.project_id, targetProjectId, `step 4: exchange project_id = ${exchanged.project_id}, want ${targetProjectId}`);
            node_assert_1.strict.deepEqual(exchanged.scopes, [CHECK_ACCESS_SCOPE], `step 4: exchange scopes = ${JSON.stringify(exchanged.scopes)}`);
            node_assert_1.strict.ok(exchanged.membership_id, "step 4: exchange membership_id is empty");
            // int64 arrives as a decimal string under the SDK's `longs: String` loader.
            const expiresAt = Number(String(exchanged.expires_at_unix ?? "0"));
            const now = Math.floor(Date.now() / 1000);
            node_assert_1.strict.ok(Number.isFinite(expiresAt) && expiresAt > now, `step 4: exchange expires_at_unix = ${exchanged.expires_at_unix} is not in the future (now ${now})`);
            node_assert_1.strict.ok(expiresAt <= before + EXCHANGED_TOKEN_MAX_LIFETIME_SECONDS + CLOCK_SKEW_SECONDS, `step 4: exchange expires_at_unix = ${expiresAt} is more than 15 minutes away (issued ~${before})`);
            const exchangedToken = exchanged.access_token;
            const targetPublic = authClient("ts.live.service-accounts.target", { tenantId: targetTenantId, projectId: targetProjectId });
            const exchangedPrincipal = await authenticate(targetPublic, "step 4 (exchanged token)", exchangedToken);
            node_assert_1.strict.equal(exchangedPrincipal.tenant_id, targetTenantId, `step 4: exchanged principal.tenant_id = ${exchangedPrincipal.tenant_id}, want ${targetTenantId}`);
            node_assert_1.strict.equal(exchangedPrincipal.user_id, serviceUserId, `step 4: exchanged principal.user_id = ${exchangedPrincipal.user_id}, want ${serviceUserId}`);
            const targetSvc = authClient("ts.live.service-accounts.exchanged", {
                tenantId: targetTenantId,
                projectId: targetProjectId,
                userId: exchangedPrincipal.user_id,
                serviceIdentity: exchangedPrincipal.service_identity,
            }, exchangedToken);
            // A self-check: CheckAccess refuses an empty user_id (INVALID_ARGUMENT), so
            // the exchanged token names its own subject.
            const own = await targetSvc.AuthzService.check_access({
                user_id: serviceUserId,
                object: "sdk-live-chart",
                action: "read",
            }, CALL);
            node_assert_1.strict.equal(own?.evaluated_subject, serviceUserId, `step 4: CheckAccess with the exchanged token evaluated_subject = ${JSON.stringify(own?.evaluated_subject)}, want ${serviceUserId}`);
            await expectPermissionDenied(`step 4: exchange for unapproved tenant ${unapprovedTenantId}`, () => svc.AuthnService.exchange_service_token_for_tenant({
                target_tenant_id: unapprovedTenantId,
                target_project_id: targetProjectId,
                requested_scopes: [CHECK_ACCESS_SCOPE],
            }, CALL));
        });
        // ── 5. Phone-only account ───────────────────────────────────────────────
        await t.test("5 phone-only CreateUser verifies by SMS OTP", async () => {
            node_assert_1.strict.ok(humanBearer && homeTenantId, "step 5: step 1 did not log the human administrator in");
            const humanProject = human.project_id || projectHint;
            const admin = authClient("ts.live.service-accounts.human", {
                tenantId: homeTenantId,
                projectId: humanProject,
                userId: human.user_id,
                serviceIdentity: human.service_identity,
            }, humanBearer);
            const username = `sdk-live-phone-ts-${(0, node_crypto_1.randomBytes)(6).toString("hex")}`;
            const phone = `+1555${String((0, node_crypto_1.randomInt)(0, 10_000_000)).padStart(7, "0")}`;
            const created = await admin.AuthnService.create_user({
                username,
                phone,
                password: PHONE_USER_PASSWORD,
                tenant_id: homeTenantId,
                project_id: humanProject,
            }, CALL);
            const user = created?.user ?? {};
            node_assert_1.strict.ok(user.user_id, `step 5: CreateUser(${username}) returned no user_id`);
            node_assert_1.strict.equal(user.email ?? "", "", `step 5: phone-only user.email = ${JSON.stringify(user.email)}, want empty`);
            node_assert_1.strict.equal(user.phone, phone, `step 5: user.phone = ${JSON.stringify(user.phone)}, want ${phone}`);
            node_assert_1.strict.equal(user.status, "USER_STATUS_PENDING_VERIFICATION", `step 5: new user.status = ${user.status}`);
            node_assert_1.strict.ok(created.otp_id, "step 5: CreateUser returned no otp_id");
            const sent = await admin.AuthnService.send_otp({
                user_id: user.user_id,
                otp_type: "OTP_TYPE_PHONE_VERIFICATION",
            }, CALL);
            node_assert_1.strict.ok(sent?.otp_id, "step 5: SendOTP returned no otp_id");
            node_assert_1.strict.ok(sent.dev_otp_code, "step 5: SendOTP returned no dev_otp_code (is UDB_OTP_DEV_ECHO=1 on the broker?)");
            const verified = await admin.AuthnService.verify_otp({ otp_id: sent.otp_id, code: sent.dev_otp_code }, CALL);
            node_assert_1.strict.equal(verified?.verified, true, `step 5: VerifyOTP verified = ${verified?.verified}`);
            node_assert_1.strict.equal(verified.otp_type, "OTP_TYPE_PHONE_VERIFICATION", `step 5: VerifyOTP otp_type = ${verified.otp_type}`);
            const fetched = await admin.AuthnService.get_user({ user_id: user.user_id }, CALL);
            node_assert_1.strict.equal(fetched?.user?.status, "USER_STATUS_ACTIVE", `step 5: verified user.status = ${fetched?.user?.status}`);
            node_assert_1.strict.equal(fetched.user.phone, phone, `step 5: verified user.phone = ${JSON.stringify(fetched.user.phone)}, want ${phone}`);
        });
    }
    finally {
        for (const client of clients)
            client.close();
    }
});
