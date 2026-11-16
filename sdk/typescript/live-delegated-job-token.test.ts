// Live delegated job tokens (AuthnService.ExchangeTokenForJob). Skipped unless
// UDB_LIVE_SDK_TESTS=1 and UDB_LIVE_SERVICE_ACCOUNT_TESTS=1; CI's "SDK service
// accounts (live)" job runs it after scripts/provision-service-account-live.py
// gave the service account's grant `udb:token:delegate`:
//
//   npm ci && npm run bundle-proto && npx tsc -p tsconfig.test.json \
//     && node --test dist-test/live-delegated-job-token.test.js
//
// What it proves, through UdbAuthClient.exchangeTokenForJob:
//   1. a service holding udb:token:delegate exchanges a user's access token for
//      a delegated job token, three times in a row: subject = the user, actor =
//      the service, the purpose and job id echoed, a dlg_ jti, the lifetime
//      bounded, never the delegate scope;
//   2. each delegated token authenticates as the USER and authorizes a call on
//      the native listener, repeatedly;
//   3. refusals: a delegated token is never exchanged again, a person cannot
//      mint one, and a token that is not a valid access token is refused.

import { strict as assert } from "node:assert";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import * as grpc from "@grpc/grpc-js";

import { UdbAuthClient } from "./auth";
import { UDB_PROTOCOL_VERSION } from "./client";
import { UdbGeneratedClient } from "./generatedClient";

const DELEGATE_SCOPE = "udb:token:delegate";
const CHECK_ACCESS_SCOPE = "udb:authz:check-access";
const DEFAULT_MAX_TTL_SECONDS = 24 * 60 * 60;
const CLOCK_SKEW_SECONDS = 60;
const CALL = { deadlineMs: 10_000, noRetry: true };

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
  return value;
}

function grpcCodeOf(err: any): number | undefined {
  return err?.code ?? err?.cause?.code;
}

async function expectPermissionDenied(step: string, call: () => Promise<unknown>): Promise<void> {
  await assert.rejects(call(), (err: any) => {
    const code = grpcCodeOf(err);
    assert.equal(code, grpc.status.PERMISSION_DENIED, `${step}: expected PERMISSION_DENIED, got ${code}: ${err?.message ?? String(err)}`);
    return true;
  }, `${step}: the call succeeded; expected PERMISSION_DENIED`);
}

test("live delegated job tokens", {
  skip:
    process.env.UDB_LIVE_SDK_TESTS === "1" && process.env.UDB_LIVE_SERVICE_ACCOUNT_TESTS === "1"
      ? false
      : "requires a live UDB broker and the CI service-account fixtures",
}, async (t) => {
  const target = requiredEnv("UDB_GRPC_TARGET");
  const authTarget = process.env.UDB_AUTH_GRPC_TARGET?.trim() || target;
  const tenantHint = requiredEnv("UDB_LIVE_TENANT");
  const projectHint = requiredEnv("UDB_LIVE_PROJECT");
  const humanUsername = requiredEnv("UDB_LIVE_USERNAME");
  const humanPassword = requiredEnv("UDB_LIVE_PASSWORD");
  const serviceUsername = requiredEnv("UDB_LIVE_SERVICE_USERNAME");
  const servicePassword = requiredEnv("UDB_LIVE_SERVICE_PASSWORD");

  const generated: UdbGeneratedClient[] = [];
  const authClients: UdbAuthClient[] = [];
  const generatedClient = (
    purpose: string,
    meta: { tenantId: string; projectId: string; userId?: string },
    bearerToken?: string,
  ): UdbGeneratedClient => {
    const client = new UdbGeneratedClient({
      target: authTarget,
      meta: {
        tenantId: meta.tenantId,
        projectId: meta.projectId,
        userId: meta.userId,
        purpose,
        correlationId: `${purpose}-${randomUUID()}`,
        clientCatalogVersion: UDB_PROTOCOL_VERSION,
      },
      bearerToken,
      deadlineMs: 10_000,
    });
    generated.push(client);
    return client;
  };
  const authClient = (
    purpose: string,
    meta: { tenantId: string; projectId: string; userId: string; serviceIdentity?: string },
    bearerToken: string,
  ): UdbAuthClient => {
    const client = new UdbAuthClient(authTarget, {
      tenantId: meta.tenantId,
      projectId: meta.projectId,
      userId: meta.userId,
      serviceIdentity: meta.serviceIdentity,
      purpose,
      correlationId: `${purpose}-${randomUUID()}`,
      clientCatalogVersion: UDB_PROTOCOL_VERSION,
      bearerToken,
    });
    authClients.push(client);
    return client;
  };

  const publicClient = generatedClient("ts.live.delegated.public", { tenantId: tenantHint, projectId: projectHint });
  const login = async (username: string, password: string): Promise<string> => {
    const resp: any = await publicClient.AuthnService.login({
      username,
      password,
      tenant_hint: tenantHint,
      project_hint: projectHint,
      device_name: "ts-sdk-delegated-job-token",
    }, CALL);
    assert.ok(resp?.access_token, `Login(${username}) returned no access token`);
    return resp.access_token as string;
  };
  const principalOf = async (bearer: string): Promise<any> => {
    const resp: any = await publicClient.AuthnService.authenticate({ bearer_token: bearer }, CALL);
    assert.ok(resp?.principal, "Authenticate returned no principal");
    return resp.principal;
  };

  try {
    const humanBearer = await login(humanUsername, humanPassword);
    const human = await principalOf(humanBearer);
    const homeTenantId = String(human.tenant_id ?? "");
    const humanProject = String(human.project_id || projectHint);
    assert.ok(homeTenantId, "the human principal has a canonical tenant_id");
    const serviceBearer = await login(serviceUsername, servicePassword);
    const service = await principalOf(serviceBearer);
    assert.ok(
      (service.scopes ?? []).includes(DELEGATE_SCOPE),
      `the service bearer must carry ${DELEGATE_SCOPE} (provisioned grant): ${JSON.stringify(service.scopes)}`,
    );
    const serviceAuth = authClient("ts.live.delegated.service", {
      tenantId: homeTenantId,
      projectId: String(service.project_id || projectHint),
      userId: String(service.user_id),
      serviceIdentity: String(service.service_identity),
    }, serviceBearer);

    const minted: { accessToken: string; jobId: string }[] = [];
    await t.test("1 a service mints delegated job tokens for a user, repeatedly", async () => {
      for (let i = 0; i < 3; i++) {
        const jobId = `ts-live-export-${randomUUID()}`;
        const before = Math.floor(Date.now() / 1000);
        const token = await serviceAuth.exchangeTokenForJob({ subjectToken: humanBearer, purpose: "export", jobId, ttlSeconds: 600 });
        assert.ok(token.accessToken, "a delegated token is returned");
        assert.ok(token.jti.startsWith("dlg_"), `jti ${token.jti}`);
        assert.equal(token.subject, human.user_id, "the token acts for the user");
        assert.equal(token.actor, service.service_identity, "the service is the actor");
        assert.equal(token.purpose, "export");
        assert.equal(token.jobId, jobId);
        assert.equal(token.tenantId, homeTenantId);
        assert.ok(token.expiresAtUnix > before && token.expiresAtUnix <= before + 600 + CLOCK_SKEW_SECONDS, `expires_at ${token.expiresAtUnix}`);
        assert.ok(token.scopes.includes(CHECK_ACCESS_SCOPE), `scopes ${JSON.stringify(token.scopes)}`);
        assert.ok(!token.scopes.includes(DELEGATE_SCOPE), "a delegated token never carries the delegate scope");
        minted.push({ accessToken: token.accessToken, jobId });
      }
      // A longer request is capped by the broker's maximum (24 hours by default).
      const before = Math.floor(Date.now() / 1000);
      const capped = await serviceAuth.exchangeTokenForJob({ subjectToken: humanBearer, purpose: "export", jobId: `ts-live-cap-${randomUUID()}`, ttlSeconds: 10_000_000 });
      assert.ok(capped.expiresAtUnix <= before + DEFAULT_MAX_TTL_SECONDS + CLOCK_SKEW_SECONDS, `capped expires_at ${capped.expiresAtUnix}`);
    });

    await t.test("2 each delegated token authenticates as the user and authorizes calls", async () => {
      assert.equal(minted.length, 3);
      for (const { accessToken } of minted) {
        for (let i = 0; i < 2; i++) {
          const who = await principalOf(accessToken);
          assert.equal(who.user_id, human.user_id, "the delegated token authenticates as the user");
          assert.equal(who.tenant_id, homeTenantId);
        }
        const asUser = generatedClient("ts.live.delegated.use", { tenantId: homeTenantId, projectId: humanProject, userId: String(human.user_id) }, accessToken);
        const decision: any = await asUser.AuthzService.checkAccess({ user_id: human.user_id, object: "sdk-live-chart", action: "read" }, CALL);
        assert.equal(decision?.evaluated_subject, human.user_id, "the call is evaluated for the user");
      }
    });

    await t.test("3 refusals", async () => {
      await expectPermissionDenied("re-exchange", () =>
        serviceAuth.exchangeTokenForJob({ subjectToken: minted[0].accessToken, purpose: "export", jobId: `ts-live-again-${randomUUID()}` }));
      const humanAuth = authClient("ts.live.delegated.human", {
        tenantId: homeTenantId,
        projectId: humanProject,
        userId: String(human.user_id),
      }, humanBearer);
      await expectPermissionDenied("a person minting", () =>
        humanAuth.exchangeTokenForJob({ subjectToken: humanBearer, purpose: "export", jobId: `ts-live-person-${randomUUID()}` }));
      await expectPermissionDenied("a token that is not an access token", () =>
        serviceAuth.exchangeTokenForJob({ subjectToken: "not.a.token", purpose: "export", jobId: `ts-live-garbage-${randomUUID()}` }));
    });
  } finally {
    for (const client of generated) client.close();
    for (const client of authClients) client.close();
  }
});
