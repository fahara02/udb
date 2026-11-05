// Live proof of the surface shared with the Go SDK, against a real broker. Skipped
// unless UDB_LIVE_SDK_TESTS=1. It runs on the Postgres-only broker posture: no Kafka,
// no CDC, no graph or vector store. It needs UDB_GRPC_TARGET, UDB_LIVE_USERNAME and
// UDB_LIVE_PASSWORD; UDB_AUTH_GRPC_TARGET, UDB_LIVE_TENANT and UDB_LIVE_PROJECT are
// optional. Everything it creates is its own and is removed at the end.
//
//   npm ci && npm run bundle-proto && npx tsc -p tsconfig.test.json \
//     && node --test dist-test/live-parity-surface.test.js
//
// What it proves, each repeated and with a case that must be refused:
//   1. connectEnterprise verifies the canonical tenant; validateTenant guards a write.
//   2. A tenant pool connects with a real API key, shares and replaces clients.
//   3. connectFromEnv and verify check the real principal; ready probes the real broker.
//   4. asUser makes a narrow service act with a person's authority, and only that call.
//   5. liveQuery delivers a real snapshot and reports the missing change feed.
//   6. Graph and vector builders reach the broker's own refusal without a store.
//   7. Flags evaluate what was stored; an absent flag is off.
//   8. Schema reflection agrees with the rows and bounds the broker really has.
//   9. principal and bearerExpiresAt report the exchanged key's real values.
//  10. A revoked session logs in again; a deactivated account cannot. A key re-exchanges
//      under its pinned principal; a revoked key cannot.
//  11. deleteFile with a bare id reaches the broker with the session's tenant.

import { strict as assert } from "node:assert";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import * as grpc from "@grpc/grpc-js";

import { UdbError } from "./generatedClient";
import { graphFrom, vectorsHybrid } from "./graphVector";
import { LiveQueryEvent, NoDeltaFeedError, liveQuery, lqEq } from "./liveQuery";
import { MemoryTokenStore, UdbProject } from "./project";
import { maxLen, primaryKey, retentionClass, tenantColumn, topic, uniqueKeys } from "./schema";
import { newApiKeyTenantPool } from "./tenantPool";

const REPEATS = 3;
const AUTHN = "udb.core.authn.services.v1.AuthnService";
const APIKEY = "udb.core.apikey.services.v1.ApiKeyService";
const CONFIG = "udb.core.config.services.v1.ConfigService";
const LOCK = "udb.core.lock.entity.v1.Lock";
const NO_GRAPH_STORE = "no graph store in the Postgres-only CI posture; the broker's refusal is proven instead";
const NO_VECTOR_STORE = "no vector store in the Postgres-only CI posture; the broker's refusal is proven instead";

/** `ready` reached the broker. A serving broker passes; this job's `file://` audit
 *  sink makes the broker report NOT_SERVING, which is that same probe's real answer. */
async function probeReady(udb: { ready(): Promise<void> }): Promise<void> {
  try {
    await udb.ready();
  } catch (err) {
    assert.match((err as Error).message, /udb: broker is NOT_SERVING/);
  }
}

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
  return value;
}

function codeOf(err: any): number | undefined {
  return err?.code ?? err?.cause?.code;
}

test("live parity surface", {
  skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker",
}, async (t) => {
  const target = requiredEnv("UDB_GRPC_TARGET");
  const authTarget = process.env.UDB_AUTH_GRPC_TARGET?.trim() || target;
  const username = requiredEnv("UDB_LIVE_USERNAME");
  const password = requiredEnv("UDB_LIVE_PASSWORD");
  const tenantCode = process.env.UDB_LIVE_TENANT?.trim() || "sdk-live";
  const projectHint = process.env.UDB_LIVE_PROJECT?.trim() || "default";
  const base = { target, authTarget, purpose: "ts.live.parity-surface", deadlineMs: 15_000 };

  const operator = await UdbProject.connectEnterprise({ ...base, username, password, tenantCode, projectId: projectHint });
  const cleanups: Array<[string, () => Promise<unknown>]> = [];
  const tenantId = operator.canonicalTenantId;
  const projectId = operator.canonicalProjectId || projectHint;
  const operatorId: string = operator.principal().user_id;
  const context = (user: string) => ({
    tenant: { tenant_id: tenantId, project_id: projectId },
    user_id: user,
    principal_id: user,
    purpose: base.purpose,
  });

  try {
    await t.test("1 connectEnterprise verifies the canonical tenant", async () => {
      assert.match(tenantId, /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
      assert.notEqual(tenantId, tenantCode, "the code must be replaced by the verified UUID");
      assert.equal(operator.tenantState.tenantCode, tenantCode);
      for (let i = 0; i < REPEATS; i += 1) {
        const session = await UdbProject.connectEnterprise({ ...base, username, password, tenantCode, projectId: projectHint });
        try {
          assert.equal(session.canonicalTenantId, tenantId, `connection ${i} verified another tenant`);
          session.validateTenant(tenantId);
          session.validateTenant("");
          assert.throws(() => session.validateTenant(tenantCode), /tenant_id mismatch/);
          assert.throws(() => session.validateTenant(randomUUID()), /tenant_id mismatch/);
        } finally {
          session.close();
        }
      }
      await assert.rejects(
        UdbProject.connectEnterprise({ ...base, username, password: `${password}-wrong`, tenantCode, projectId: projectHint }),
        (err: any) => codeOf(err) === grpc.status.UNAUTHENTICATED,
        "a wrong password must be refused by the broker",
      );
    });

    // A narrow service account with its own key, as a real consumer would have.
    const serviceName = `ts-parity-${randomUUID().replace(/-/g, "")}`;
    const created: any = await operator.nativeCore.unary(AUTHN, "CreateUser", {
      username: serviceName,
      email: `${serviceName}@example.invalid`,
      password: "CorrectHorse1!",
      tenant_id: tenantId,
      project_id: projectId,
      full_name: "TS parity surface service account",
      account_kind: "ACCOUNT_KIND_SERVICE_ACCOUNT",
    });
    const serviceId: string = created?.user?.user_id;
    assert.ok(serviceId, "the service account must be created");
    cleanups.push([
      "service account",
      () =>
        operator.nativeCore.unary(AUTHN, "ChangeUserStatus", {
          user_id: serviceId,
          new_status: "USER_STATUS_DEACTIVATED",
          reason: "ts live parity cleanup",
          context: context(operatorId),
        }),
    ]);
    await operator.nativeCore.unary(AUTHN, "ChangeUserStatus", {
      user_id: serviceId,
      new_status: "USER_STATUS_ACTIVE",
      reason: "ts live parity activation",
      context: context(operatorId),
    });
    await operator.nativeCore.unary(AUTHN, "CreateServiceAccountGrant", {
      tenant_id: tenantId,
      project_id: projectId,
      user_id: serviceId,
      service_identity: serviceName,
      approved_scopes: ["udb:read"],
      reason: "ts live parity narrow read fixture",
    });
    cleanups.push([
      "service grant",
      () =>
        operator.nativeCore.unary(AUTHN, "RevokeServiceAccountGrant", {
          tenant_id: tenantId,
          user_id: serviceId,
          reason: "ts live parity cleanup",
        }),
    ]);
    const keyReply: any = await operator.nativeCore.unary(APIKEY, "CreateApiKey", {
      name: serviceName,
      owner_id: serviceId,
      scopes: ["udb:read"],
      context: context(serviceId),
    });
    const plainKey: string = keyReply?.plain_key;
    const keyId: string = keyReply?.key?.key_id;
    assert.ok(plainKey && keyId, "the API key must be created");
    cleanups.push([
      "API key",
      () =>
        operator.nativeCore.unary(APIKEY, "RevokeApiKey", {
          key_id: keyId,
          revoke_reason: "ts live parity cleanup",
          context: context(serviceId),
        }),
    ]);

    await t.test("2 tenant pool connects, shares, replaces and refuses", async () => {
      const pool = newApiKeyTenantPool({ ...base, projectId }, { [tenantId]: plainKey, [randomUUID()]: plainKey });
      try {
        const first = await pool.get(tenantId);
        for (let i = 0; i < REPEATS; i += 1) {
          assert.equal(await pool.get(tenantId), first, "a live client is shared");
        }
        const [a, b] = await Promise.all([pool.get(tenantId), pool.get(tenantId)]);
        assert.ok(a === first && b === first);
        assert.deepEqual(pool.tenants(), [tenantId]);
        await probeReady(first);

        await pool.evict(tenantId);
        assert.deepEqual(pool.tenants(), []);
        const second = await pool.get(tenantId);
        assert.notEqual(second, first, "an evicted tenant reconnects");
        assert.equal(second.principal().tenant_id, tenantId);

        await assert.rejects(pool.get(randomUUID()), /no API key for tenant/);
      } finally {
        await pool.close();
      }
      await assert.rejects(pool.get(tenantId), /pool is closed/);

      const wrong = randomUUID();
      const mismatched = newApiKeyTenantPool({ ...base, projectId }, { [wrong]: plainKey });
      try {
        await assert.rejects(mismatched.get(wrong), "this tenant's key must not serve another tenant");
        assert.deepEqual(mismatched.tenants(), []);
      } finally {
        await mismatched.close();
      }
    });

    await t.test("3 connectFromEnv verifies the principal and ready probes the broker", async () => {
      const prefix = "UDB_PARITY_LIVE_";
      const env: Record<string, string> = {
        TARGET: target,
        AUTH_TARGET: authTarget,
        TENANT_ID: tenantId,
        PROJECT_ID: projectId,
        PURPOSE: base.purpose,
        API_KEY: plainKey,
        DEADLINE: "15s",
      };
      for (const [name, value] of Object.entries(env)) process.env[prefix + name] = value;
      try {
        const expect = { tenantId, projectId, serviceIdentity: serviceName, requiredScopes: ["udb:read"] };
        for (let i = 0; i < REPEATS; i += 1) {
          const udb = await UdbProject.connectFromEnv(prefix, expect);
          try {
            for (let probe = 0; probe < REPEATS; probe += 1) await probeReady(udb);
            assert.equal(udb.credentialError(), null);
          } finally {
            udb.close();
          }
          await assert.rejects(udb.ready(), /credential owner is closed/);
        }
        await assert.rejects(UdbProject.connectFromEnv(prefix, { tenantId: randomUUID() }), /tenant is/);
        await assert.rejects(UdbProject.connectFromEnv(prefix, { requiredScopes: ["udb:admin"] }), /lacks scopes udb:admin/);
        await assert.rejects(UdbProject.connectFromEnv(prefix, { serviceIdentity: "someone-else" }), /service identity is/);
        await assert.rejects(UdbProject.connectFromEnv("UDB_PARITY_LIVE_ABSENT_"), /environment is incomplete/);
        process.env[`${prefix}API_KEY`] = `${plainKey}-wrong`;
        await assert.rejects(UdbProject.connectFromEnv(prefix), "a wrong key must be refused by the broker");
      } finally {
        for (const name of Object.keys(env)) delete process.env[prefix + name];
      }
      const dead = new UdbProject({ target: "127.0.0.1:1", tenantId, purpose: base.purpose });
      try {
        await assert.rejects(dead.ready({ deadlineMs: 2_000 }), /broker health check failed/);
      } finally {
        dead.close();
      }
    });

    await t.test("4 asUser carries a person's authority for one call only", async () => {
      const service = await UdbProject.connect({
        ...base,
        tenantId,
        projectId,
        credentials: { apiKey: plainKey },
      });
      const login: any = await operator.auth.login({
        username,
        password,
        tenant_hint: tenantId,
        project_hint: projectId,
        device_name: "ts-live-parity-as-user",
      });
      assert.ok(login?.access_token, "the person must have a session");
      if (login.session_id) {
        cleanups.push([
          "person session",
          () =>
            operator.nativeCore.unary(AUTHN, "RevokeSession", {
              session_id: login.session_id,
              principal_id: operatorId,
              revoke_reason: "ts live parity cleanup",
              context: context(operatorId),
            }),
        ]);
      }
      try {
        const denied = () =>
          assert.rejects(
            service.validateSession(login.access_token),
            (err: any) => err instanceof UdbError && err.code === grpc.status.PERMISSION_DENIED,
            "the narrow service must lack token-validation authority of its own",
          );
        await denied();
        for (let i = 0; i < REPEATS; i += 1) {
          const session = await service.validateSession(login.access_token, service.asUser(login.access_token));
          assert.equal(session.userId, operatorId, `call ${i} validated another user`);
          assert.equal(session.tenantId, tenantId);
          assert.equal(session.projectId, projectId);
          assert.ok(session.expiresAt && session.expiresAt.getTime() > Date.now(), "the session's expiry is known");
          await service.validateSession(`Bearer ${login.access_token}`, service.asUser(`Bearer ${login.access_token}`));
          await denied();
        }
        const listed: any = await service.lock.list({}, service.asUser(login.access_token));
        assert.ok(listed, "a delegated native call must succeed");
        await assert.rejects(
          service.validateSession(login.access_token, service.asUser("not-a-token")),
          (err: any) => err instanceof UdbError && err.code === grpc.status.UNAUTHENTICATED,
          "a garbage user bearer must be refused",
        );
        await assert.rejects(operator.validateSession(`${login.access_token}x`), "a tampered token must not validate");
      } finally {
        service.close();
      }
    });

    await t.test("5 liveQuery delivers the snapshot and reports the missing feed", async () => {
      const lockName = `ts-parity-${randomUUID()}`;
      const acquired: any = await operator.lock.acquire({
        lock_name: lockName,
        owner_id: serviceName,
        lease_ttl_seconds: 120,
        metadata_json: "{}",
      });
      cleanups.push([
        "lock",
        () =>
          operator.lock.release({
            lock_name: lockName,
            owner_id: serviceName,
            fencing_token: acquired?.fencing_token ?? acquired?.lock?.fencing_token ?? "0",
          }),
      ]);
      const watch = async (where: ReturnType<typeof lqEq>[]) => {
        const events: LiveQueryEvent<any>[] = [];
        // A bound for a broker that stays open instead of refusing: the test then
        // fails on the assertion below rather than hanging.
        const signal = AbortSignal.timeout(30_000);
        await assert.rejects(
          liveQuery(operator, LOCK, { where, snapshotLimit: 10, signal }, (event) => {
            events.push(event);
          }),
          NoDeltaFeedError,
          "a broker without CDC must report it after the snapshot",
        );
        return events;
      };
      for (let i = 0; i < REPEATS; i += 1) {
        const events = await watch([lqEq("lock_name", lockName)]);
        assert.equal(events.length, 1, "exactly the snapshot is delivered");
        assert.equal(events[0].isSnapshot, true);
        assert.equal(events[0].resumed, false);
        assert.equal(events[0].snapshot?.length, 1);
        const row = events[0].snapshot![0];
        assert.equal(row.lock_name, lockName);
        // Schema reflection must describe the row the broker really returns.
        assert.equal(row[tenantColumn(LOCK)], tenantId, "the reflected tenant column holds the tenant");
        assert.ok(row[primaryKey(LOCK)], "the reflected primary key is present on the row");
      }
      const empty = await watch([lqEq("lock_name", `${lockName}-absent`)]);
      assert.deepEqual(empty[0].snapshot, [], "a filter matching nothing delivers an empty snapshot");

      const reconnects: Error[] = [];
      await assert.rejects(
        liveQuery(
          operator,
          "no.such.v1.Message",
          { backoffMs: 1, onError: (err) => reconnects.push(err), signal: AbortSignal.timeout(30_000) },
          () => assert.fail("nothing may be delivered for an unknown message type"),
        ),
        (err: any) => err instanceof UdbError && !(err instanceof NoDeltaFeedError),
        "an unknown message type must be refused",
      );
      assert.equal(reconnects.length, 0, "a refusal must not be retried");
    });

    await t.test("6 graph and vector builders reach the broker's refusal", async (sub) => {
      for (let i = 0; i < REPEATS; i += 1) {
        await assert.rejects(
          graphFrom(operator, LOCK, randomUUID()).out("RELATED").depth(1, 2).where("status", "HELD").limit(5).run(),
          (err: any) => err instanceof UdbError && err.code !== grpc.status.UNAUTHENTICATED,
          "a traversal with no graph store must be refused by the broker",
        );
        await assert.rejects(
          vectorsHybrid(operator, `ts-parity-absent-${randomUUID()}`).text("rotate keys").near([0.1, 0.2]).limit(3).run(),
          (err: any) => err instanceof UdbError && err.code !== grpc.status.UNAUTHENTICATED,
          "a search with no vector store must be refused by the broker",
        );
      }
      await assert.rejects(graphFrom(operator, LOCK, "").run(), /needs a start node id/);
      await assert.rejects(vectorsHybrid(operator, "notes").run(), /needs near\(vector\) and\/or text\(query\)/);
      await sub.test("graph traversal over real nodes", { skip: NO_GRAPH_STORE }, () => undefined);
      await sub.test("hybrid search over real points", { skip: NO_VECTOR_STORE }, () => undefined);
    });

    await t.test("7 flags evaluate what was stored", async () => {
      const key = `ts.parity.${randomUUID().replace(/-/g, "")}`;
      const put = (value: boolean) =>
        operator.nativeCore.unary(CONFIG, "PutFlag", {
          tenant_id: tenantId,
          project_id: projectId,
          flag_key: key,
          value: { bool_value: value },
          enabled: true,
          rollout_percentage: 100,
          metadata_json: "{}",
        });
      const remove = () =>
        operator.nativeCore.unary(CONFIG, "DeleteFlag", { tenant_id: tenantId, project_id: projectId, flag_key: key });
      cleanups.push(["flag", () => remove().catch(() => undefined)]);
      assert.equal(await operator.flags.enabled(key), false, "a flag that does not exist is off");
      await put(true);
      for (let i = 0; i < REPEATS; i += 1) {
        assert.equal(await operator.flags.enabled(key, { user_id: `user-${i}` }), true, "the stored value is read");
      }
      const missing = `${key}.absent`;
      const values = await operator.flags.evaluate([key, missing]);
      assert.equal(values[key]?.bool_value, true);
      assert.equal(missing in values, false, "unknown keys are absent");
      assert.equal(await operator.flags.enabled(missing), false);
      await put(false);
      assert.equal(await operator.flags.enabled(key), false, "a changed value is read");
      await remove();
      assert.equal(await operator.flags.enabled(key), false, "a deleted flag is off");
    });

    await t.test("8 schema reflection agrees with the broker", async () => {
      for (let i = 0; i < REPEATS; i += 1) {
        assert.equal(primaryKey(LOCK), "lock_id");
        assert.deepEqual(uniqueKeys(LOCK), [["tenant_id", "lock_name"]]);
        assert.equal(retentionClass(LOCK), "lock.operational");
        assert.equal(topic("udb.core.apikey.events.v1.ApiKeyCreatedEvent"), "udb.apikey.created.v1");
      }
      const bound = maxLen(LOCK, "lock_name");
      assert.equal(bound, 255);
      await assert.rejects(
        operator.lock.acquire({
          lock_name: "x".repeat(bound + 1),
          owner_id: serviceName,
          lease_ttl_seconds: 5,
          metadata_json: "{}",
        }),
        "a value past the reflected bound must be refused",
      );
      assert.throws(() => uniqueKeys("no.such.v1.Message"), /no\.such\.v1\.Message/);
    });

    await t.test("9 principal and bearerExpiresAt report the exchanged key", async () => {
      for (let i = 0; i < REPEATS; i += 1) {
        const service = await UdbProject.connect({ ...base, tenantId, projectId, credentials: { apiKey: plainKey } });
        try {
          const principal = service.principal();
          assert.equal(principal.tenant_id, tenantId);
          assert.equal(principal.project_id, projectId);
          assert.equal(principal.service_identity, serviceName);
          assert.equal(principal.principal_id, serviceId);
          assert.equal(principal.account_kind, "ACCOUNT_KIND_SERVICE_ACCOUNT");
          assert.deepEqual(principal.scopes, ["udb:read"]);
          const expires = service.bearerExpiresAt();
          assert.ok(expires && expires.getTime() > Date.now(), "the bearer must not have expired");
          service.verify({ tenantId, projectId, serviceIdentity: serviceName, requiredScopes: ["udb:read"] });
          assert.throws(() => service.verify({ requiredScopes: ["udb:write"] }), /lacks scopes udb:write/);
        } finally {
          service.close();
        }
      }
      const raw = new UdbProject({ ...base, tenantId, projectId, credentials: { bearerToken: "not-verified" } });
      try {
        assert.equal(raw.principal(), null, "a raw bearer has no verified principal");
        assert.equal(raw.bearerExpiresAt(), null);
        assert.throws(() => raw.verify({ tenantId }), /no verified principal/);
      } finally {
        raw.close();
      }
    });
    await t.test("10 renewal recovers by re-login and refuses a dead account", async () => {
      const name = `ts-parity-renewal-${randomUUID().replace(/-/g, "")}`;
      const accountPassword = "CorrectHorse1!";
      const made: any = await operator.nativeCore.unary(AUTHN, "CreateUser", {
        username: name,
        email: `${name}@example.invalid`,
        password: accountPassword,
        tenant_id: tenantId,
        project_id: projectId,
        full_name: "TS parity renewal account",
        account_kind: "ACCOUNT_KIND_SERVICE_ACCOUNT",
      });
      const userId: string = made?.user?.user_id;
      assert.ok(userId, "the renewal account must be created");
      const setStatus = (status: string) =>
        operator.nativeCore.unary(AUTHN, "ChangeUserStatus", {
          user_id: userId,
          new_status: status,
          reason: "ts live parity renewal",
          context: context(operatorId),
        });
      cleanups.push(["renewal account", () => setStatus("USER_STATUS_DEACTIVATED")]);
      await setStatus("USER_STATUS_ACTIVE");
      await operator.nativeCore.unary(AUTHN, "CreateServiceAccountGrant", {
        tenant_id: tenantId,
        project_id: projectId,
        user_id: userId,
        service_identity: name,
        approved_scopes: ["udb:read"],
        reason: "ts live parity renewal fixture",
      });
      cleanups.push([
        "renewal grant",
        () =>
          operator.nativeCore.unary(AUTHN, "RevokeServiceAccountGrant", {
            tenant_id: tenantId,
            user_id: userId,
            reason: "ts live parity cleanup",
          }),
      ]);
      const made2: any = await operator.nativeCore.unary(APIKEY, "CreateApiKey", {
        name,
        owner_id: userId,
        scopes: ["udb:read"],
        context: context(userId),
      });
      const renewalKey: string = made2?.plain_key;
      const renewalKeyId: string = made2?.key?.key_id;
      assert.ok(renewalKey && renewalKeyId, "the renewal key must be created");

      // A key re-exchanges under its pinned principal, repeatedly, then stops when revoked.
      const keyStore = new MemoryTokenStore();
      const keyed = await UdbProject.connect({
        ...base,
        tenantId,
        projectId,
        tokenStore: keyStore,
        credentials: { apiKey: renewalKey },
      });
      const expired = (store: MemoryTokenStore) => store.save({ ...store.load()!, expiresAt: Date.now() - 1_000 });
      try {
        for (let i = 0; i < REPEATS; i += 1) {
          expired(keyStore);
          const renewed = await keyed.refreshIfNeeded();
          assert.ok(renewed?.accessToken, `re-exchange ${i} must mint a bearer`);
          assert.ok(renewed.expiresAt > Date.now(), `re-exchange ${i} must not be expired`);
          await probeReady(keyed);
          assert.equal(keyed.credentialError(), null);
        }
        await operator.nativeCore.unary(APIKEY, "RevokeApiKey", {
          key_id: renewalKeyId,
          revoke_reason: "ts live parity renewal",
          context: context(userId),
        });
        expired(keyStore);
        await assert.rejects(keyed.refreshIfNeeded(), "a revoked key must not re-exchange");
      } finally {
        keyed.close();
      }

      const store = new MemoryTokenStore();
      const session = await UdbProject.connectEnterprise({
        ...base,
        username: name,
        password: accountPassword,
        tenantCode: tenantId,
        projectId,
        tokenStore: store,
      });
      try {
        const pinned = session.principal();
        let spent = store.load()!;
        for (let i = 0; i < REPEATS; i += 1) {
          assert.ok(spent.sessionId, `session ${i} must have an id to revoke`);
          await operator.nativeCore.unary(AUTHN, "RevokeSession", {
            session_id: spent.sessionId,
            principal_id: userId,
            revoke_reason: "ts live parity re-login",
            context: context(operatorId),
          });
          expired(store);
          const renewed = await session.refreshIfNeeded();
          assert.ok(renewed?.accessToken, `re-login ${i} must mint a bearer`);
          assert.notEqual(renewed.sessionId, spent.sessionId, `re-login ${i} must open a new session`);
          // A service-account login holds no refresh token, so only a held one can rotate.
          if (spent.refreshToken) {
            assert.notEqual(renewed.refreshToken, spent.refreshToken, `re-login ${i} must hold a new refresh token`);
          }
          assert.equal(session.bearer(), `Bearer ${renewed.accessToken}`);
          assert.equal(session.credentialError(), null);
          assert.deepEqual(session.principal(), pinned, "re-login must keep the pinned principal");
          spent = store.load()!;
        }
        // An ordinary refresh after the re-logins still works and is verified.
        expired(store);
        const refreshed = await session.refreshIfNeeded();
        assert.equal(refreshed?.sessionId, spent.sessionId, "a healthy refresh keeps the session");
        if (spent.refreshToken) assert.notEqual(refreshed?.refreshToken, spent.refreshToken);
        // The account is gone: both the refresh and the re-login must be refused.
        await setStatus("USER_STATUS_DEACTIVATED");
        expired(store);
        await assert.rejects(session.refreshIfNeeded(), /renewal and re-login failed/);
      } finally {
        session.close();
      }
    });

    await t.test("11 deleteFile fills the session's tenant", async (sub) => {
      let unmounted = false;
      for (let i = 0; i < REPEATS; i += 1) {
        await assert.rejects(operator.storage.deleteFile(randomUUID()), (err: any) => {
          assert.ok(err instanceof UdbError, `deleteFile failed before the broker: ${err?.message}`);
          const absent =
            err.code === grpc.status.UNIMPLEMENTED ||
            (err.code === grpc.status.FAILED_PRECONDITION && /native service 'storage' is disabled/.test(err.message));
          unmounted ||= absent;
          assert.ok(
            err.code === grpc.status.NOT_FOUND || absent,
            `an unknown file must be NOT_FOUND, got ${err.code}: ${err.message}`,
          );
          return true;
        });
      }
      await sub.test(
        "the broker accepted the tenant",
        { skip: unmounted ? "StorageService needs an object store, which the Postgres-only CI posture has none of" : false },
        async () => {
          await assert.rejects(operator.storage.deleteFileMode(randomUUID(), "DELETE_MODE_HARD", "ts parity"), (err: any) => err.code === grpc.status.NOT_FOUND);
          await assert.rejects(
            operator.storage.deleteFile({ tenant_id: randomUUID(), file_id: randomUUID() }),
            (err: any) => err instanceof UdbError && err.code !== grpc.status.NOT_FOUND,
            "another tenant named explicitly must be refused",
          );
        },
      );
    });
  } finally {
    for (const [name, cleanup] of cleanups.reverse()) {
      await cleanup().catch((err) => t.diagnostic(`cleanup of ${name} failed: ${err?.message ?? err}`));
    }
    operator.close();
  }
});
