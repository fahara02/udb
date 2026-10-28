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

import { strict as assert } from "node:assert";
import { randomBytes, randomInt, randomUUID } from "node:crypto";
import { test } from "node:test";
import * as grpc from "@grpc/grpc-js";

import { UDB_PROTOCOL_VERSION } from "./client";
import { UdbGeneratedClient } from "./generatedClient";

const CHECK_ACCESS_SCOPE = "udb:authz:check-access";
const CHECK_ACCESS_FOR_SCOPE = "udb:authz:check-access-for";
const NEVER_ON_SERVICE_BEARER = ["platform_admin", "organization_owner"];
const EXCHANGED_TOKEN_MAX_LIFETIME_SECONDS = 15 * 60;
const CLOCK_SKEW_SECONDS = 60;
const PHONE_USER_PASSWORD = "SdkLivePhone#2026";
const CALL = { deadlineMs: 10_000, noRetry: true };

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
  return value;
}

function grpcCodeOf(err: any): number | undefined {
  return err?.code ?? err?.cause?.code;
}

function codeName(code: number | undefined): string {
  return code === undefined ? "none" : (grpc.status as any)[code] ?? String(code);
}

async function expectPermissionDenied(step: string, call: () => Promise<unknown>): Promise<void> {
  await assert.rejects(call(), (err: any) => {
    const code = grpcCodeOf(err);
    assert.equal(
      code,
      grpc.status.PERMISSION_DENIED,
      `${step}: expected PERMISSION_DENIED, got ${codeName(code)}: ${err?.message ?? String(err)}`,
    );
    return true;
  }, `${step}: the call succeeded; expected PERMISSION_DENIED`);
}

function lower(values: unknown): string[] {
  return Array.isArray(values) ? values.map((value) => String(value).toLowerCase()) : [];
}

test("live service account delegation conformance", {
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

  const clients: UdbGeneratedClient[] = [];
  // One generated client per identity: x-tenant-id must be the canonical tenant
  // UUID of the bearer it carries (Login/Authenticate are public and only need
  // the tenant hint). metadata() adds x-request-id / x-correlation-id per call.
  const authClient = (
    purpose: string,
    meta: { tenantId: string; projectId: string; userId?: string; serviceIdentity?: string },
    bearerToken?: string,
  ): UdbGeneratedClient => {
    const client = new UdbGeneratedClient({
      target: authTarget,
      meta: {
        tenantId: meta.tenantId,
        projectId: meta.projectId,
        userId: meta.userId,
        serviceIdentity: meta.serviceIdentity || undefined,
        purpose,
        correlationId: `${purpose}-${randomUUID()}`,
        clientCatalogVersion: UDB_PROTOCOL_VERSION,
      },
      bearerToken,
      deadlineMs: 10_000,
    });
    clients.push(client);
    return client;
  };

  const publicClient = authClient("ts.live.service-accounts.public", { tenantId: tenantHint, projectId: projectHint });
  const login = async (step: string, username: string, password: string) => {
    const resp: any = await publicClient.AuthnService.login({
      username,
      password,
      tenant_hint: tenantHint,
      project_hint: projectHint,
      device_name: "ts-sdk-service-accounts",
    }, CALL);
    assert.ok(resp?.access_token, `${step}: Login(${username}) returned no access token (mfa_required=${resp?.mfa_required})`);
    return resp.access_token as string;
  };
  const authenticate = async (client: UdbGeneratedClient, step: string, bearer: string) => {
    const resp: any = await client.AuthnService.authenticate({ bearer_token: bearer }, CALL);
    assert.ok(resp?.principal, `${step}: Authenticate returned no principal`);
    return resp.principal as any;
  };

  try {
    // ── 1. Human administrator ─────────────────────────────────────────────
    let humanBearer = "";
    let human: any = {};
    let homeTenantId = "";
    await t.test("1 human login names the account", async () => {
      humanBearer = await login("step 1", humanUsername, humanPassword);
      human = await authenticate(publicClient, "step 1", humanBearer);
      assert.equal(
        String(human.username ?? "").toLowerCase(),
        humanUsername.toLowerCase(),
        `step 1: principal.username = ${JSON.stringify(human.username)}, want ${humanUsername}`,
      );
      homeTenantId = human.tenant_id ?? "";
      assert.ok(homeTenantId, `step 1: principal.tenant_id is empty (principal ${JSON.stringify(human)})`);
    });

    // ── 2. Service-account bearer: identity, bound roles, scopes ───────────
    let serviceBearer = "";
    let service: any = {};
    await t.test("2 service account bearer carries bound roles and scopes", async () => {
      assert.ok(homeTenantId, "step 2: step 1 did not resolve the home tenant");
      serviceBearer = await login("step 2", serviceUsername, servicePassword);
      service = await authenticate(publicClient, "step 2", serviceBearer);
      assert.equal(
        String(service.username ?? "").toLowerCase(),
        serviceUsername.toLowerCase(),
        `step 2: principal.username = ${JSON.stringify(service.username)}, want ${serviceUsername}`,
      );
      assert.equal(service.user_id, serviceUserId, `step 2: principal.user_id = ${service.user_id}, want ${serviceUserId}`);
      assert.equal(
        service.account_kind,
        "ACCOUNT_KIND_SERVICE_ACCOUNT",
        `step 2: principal.account_kind = ${service.account_kind}`,
      );
      assert.equal(service.tenant_id, homeTenantId, `step 2: principal.tenant_id = ${service.tenant_id}, want ${homeTenantId}`);
      const roles = lower(service.roles);
      assert.ok(
        roles.includes(serviceRole.toLowerCase()),
        `step 2: service bearer roles ${JSON.stringify(service.roles)} lack the bound role ${serviceRole}`,
      );
      for (const forbidden of [withheldRole, ...NEVER_ON_SERVICE_BEARER]) {
        assert.ok(
          !roles.includes(forbidden.toLowerCase()),
          `step 2: service bearer roles ${JSON.stringify(service.roles)} carry the withheld role ${forbidden}`,
        );
      }
      assert.ok(
        (service.scopes ?? []).includes(CHECK_ACCESS_FOR_SCOPE),
        `step 2: service bearer scopes ${JSON.stringify(service.scopes)} lack ${CHECK_ACCESS_FOR_SCOPE}`,
      );
    });

    const serviceClient = () => authClient("ts.live.service-accounts.service", {
      tenantId: service.tenant_id,
      projectId: service.project_id || projectHint,
      userId: service.user_id,
      serviceIdentity: service.service_identity,
    }, serviceBearer);

    // ── 3. Delegated CheckAccess ────────────────────────────────────────────
    await t.test("3 service account checks access for a person of its tenant", async () => {
      assert.ok(serviceBearer, "step 3: step 2 did not log the service account in");
      const svc = serviceClient();
      const delegated: any = await svc.AuthzService.check_access({
        user_id: delegatedUserId,
        object: "sdk-live-chart",
        action: "read",
      }, CALL);
      assert.equal(
        delegated?.evaluated_subject,
        delegatedUserId,
        `step 3: delegated CheckAccess evaluated_subject = ${JSON.stringify(delegated?.evaluated_subject)}, want ${delegatedUserId}`,
      );
      const unknownUserId = randomUUID();
      await expectPermissionDenied(`step 3: delegated CheckAccess for unknown user ${unknownUserId}`, () =>
        svc.AuthzService.check_access({ user_id: unknownUserId, object: "sdk-live-chart", action: "read" }, CALL));
    });

    // ── 4. Per-tenant service token ─────────────────────────────────────────
    await t.test("4 service account exchanges its home bearer for a target-tenant token", async () => {
      assert.ok(serviceBearer, "step 4: step 2 did not log the service account in");
      const svc = serviceClient();
      const before = Math.floor(Date.now() / 1000);
      const exchanged: any = await svc.AuthnService.exchange_service_token_for_tenant({
        target_tenant_id: targetTenantId,
        target_project_id: targetProjectId,
        requested_scopes: [CHECK_ACCESS_SCOPE],
      }, CALL);
      assert.ok(exchanged?.access_token, "step 4: ExchangeServiceTokenForTenant returned no access_token");
      assert.equal(exchanged.tenant_id, targetTenantId, `step 4: exchange tenant_id = ${exchanged.tenant_id}, want ${targetTenantId}`);
      assert.equal(exchanged.project_id, targetProjectId, `step 4: exchange project_id = ${exchanged.project_id}, want ${targetProjectId}`);
      assert.deepEqual(exchanged.scopes, [CHECK_ACCESS_SCOPE], `step 4: exchange scopes = ${JSON.stringify(exchanged.scopes)}`);
      assert.ok(exchanged.membership_id, "step 4: exchange membership_id is empty");
      // int64 arrives as a decimal string under the SDK's `longs: String` loader.
      const expiresAt = Number(String(exchanged.expires_at_unix ?? "0"));
      const now = Math.floor(Date.now() / 1000);
      assert.ok(
        Number.isFinite(expiresAt) && expiresAt > now,
        `step 4: exchange expires_at_unix = ${exchanged.expires_at_unix} is not in the future (now ${now})`,
      );
      assert.ok(
        expiresAt <= before + EXCHANGED_TOKEN_MAX_LIFETIME_SECONDS + CLOCK_SKEW_SECONDS,
        `step 4: exchange expires_at_unix = ${expiresAt} is more than 15 minutes away (issued ~${before})`,
      );

      const exchangedToken: string = exchanged.access_token;
      const targetPublic = authClient("ts.live.service-accounts.target", { tenantId: targetTenantId, projectId: targetProjectId });
      const exchangedPrincipal = await authenticate(targetPublic, "step 4 (exchanged token)", exchangedToken);
      assert.equal(
        exchangedPrincipal.tenant_id,
        targetTenantId,
        `step 4: exchanged principal.tenant_id = ${exchangedPrincipal.tenant_id}, want ${targetTenantId}`,
      );
      assert.equal(
        exchangedPrincipal.user_id,
        serviceUserId,
        `step 4: exchanged principal.user_id = ${exchangedPrincipal.user_id}, want ${serviceUserId}`,
      );

      const targetSvc = authClient("ts.live.service-accounts.exchanged", {
        tenantId: targetTenantId,
        projectId: targetProjectId,
        userId: exchangedPrincipal.user_id,
        serviceIdentity: exchangedPrincipal.service_identity,
      }, exchangedToken);
      // A self-check: CheckAccess refuses an empty user_id (INVALID_ARGUMENT), so
      // the exchanged token names its own subject.
      const own: any = await targetSvc.AuthzService.check_access({
        user_id: serviceUserId,
        object: "sdk-live-chart",
        action: "read",
      }, CALL);
      assert.equal(
        own?.evaluated_subject,
        serviceUserId,
        `step 4: CheckAccess with the exchanged token evaluated_subject = ${JSON.stringify(own?.evaluated_subject)}, want ${serviceUserId}`,
      );

      await expectPermissionDenied(`step 4: exchange for unapproved tenant ${unapprovedTenantId}`, () =>
        svc.AuthnService.exchange_service_token_for_tenant({
          target_tenant_id: unapprovedTenantId,
          target_project_id: targetProjectId,
          requested_scopes: [CHECK_ACCESS_SCOPE],
        }, CALL));
    });

    // ── 5. Phone-only account ───────────────────────────────────────────────
    await t.test("5 phone-only CreateUser verifies by SMS OTP", async () => {
      assert.ok(humanBearer && homeTenantId, "step 5: step 1 did not log the human administrator in");
      const humanProject = human.project_id || projectHint;
      const admin = authClient("ts.live.service-accounts.human", {
        tenantId: homeTenantId,
        projectId: humanProject,
        userId: human.user_id,
        serviceIdentity: human.service_identity,
      }, humanBearer);
      const username = `sdk-live-phone-ts-${randomBytes(6).toString("hex")}`;
      const phone = `+1555${String(randomInt(0, 10_000_000)).padStart(7, "0")}`;
      const created: any = await admin.AuthnService.create_user({
        username,
        phone,
        password: PHONE_USER_PASSWORD,
        tenant_id: homeTenantId,
        project_id: humanProject,
      }, CALL);
      const user = created?.user ?? {};
      assert.ok(user.user_id, `step 5: CreateUser(${username}) returned no user_id`);
      assert.equal(user.email ?? "", "", `step 5: phone-only user.email = ${JSON.stringify(user.email)}, want empty`);
      assert.equal(user.phone, phone, `step 5: user.phone = ${JSON.stringify(user.phone)}, want ${phone}`);
      assert.equal(user.status, "USER_STATUS_PENDING_VERIFICATION", `step 5: new user.status = ${user.status}`);
      assert.ok(created.otp_id, "step 5: CreateUser returned no otp_id");

      const sent: any = await admin.AuthnService.send_otp({
        user_id: user.user_id,
        otp_type: "OTP_TYPE_PHONE_VERIFICATION",
      }, CALL);
      assert.ok(sent?.otp_id, "step 5: SendOTP returned no otp_id");
      assert.ok(sent.dev_otp_code, "step 5: SendOTP returned no dev_otp_code (is UDB_OTP_DEV_ECHO=1 on the broker?)");

      const verified: any = await admin.AuthnService.verify_otp({ otp_id: sent.otp_id, code: sent.dev_otp_code }, CALL);
      assert.equal(verified?.verified, true, `step 5: VerifyOTP verified = ${verified?.verified}`);
      assert.equal(
        verified.otp_type,
        "OTP_TYPE_PHONE_VERIFICATION",
        `step 5: VerifyOTP otp_type = ${verified.otp_type}`,
      );

      const fetched: any = await admin.AuthnService.get_user({ user_id: user.user_id }, CALL);
      assert.equal(fetched?.user?.status, "USER_STATUS_ACTIVE", `step 5: verified user.status = ${fetched?.user?.status}`);
      assert.equal(fetched.user.phone, phone, `step 5: verified user.phone = ${JSON.stringify(fetched.user.phone)}, want ${phone}`);
    });
  } finally {
    for (const client of clients) client.close();
  }
});
