// Live broker-restart recovery (parity with the Go SDK's TestLiveBrokerRestartRecovery).
// Skipped unless UDB_LIVE_SDK_TESTS=1 and UDB_LIVE_BROKER_RESTART_CMD names a
// command (run with sh -c) that restarts the broker on the same addresses and
// KEEPS its database (sessions, users and tenant ids survive); a reset that
// recreates the database is a different scenario.
//
// The same connectEnterprise session must reconnect by itself and keep working
// on the data plane (GetCapabilities) and the native control plane
// (ValidateSession). While the broker is down a call may fail UNAVAILABLE; any
// other failure, or no recovery within 90s, fails the test.

import { strict as assert } from "node:assert";
import { spawn } from "node:child_process";
import { test } from "node:test";
import * as grpc from "@grpc/grpc-js";

import { UdbProject } from "./project";

const RECOVERY_MS = 90_000;
const restartCommand = process.env.UDB_LIVE_BROKER_RESTART_CMD?.trim() ?? "";

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
  return value;
}

function runRestart(command: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn("sh", ["-c", command], { stdio: "inherit" });
    child.on("error", reject);
    child.on("exit", (code, signal) =>
      code === 0 ? resolve() : reject(new Error(`UDB_LIVE_BROKER_RESTART_CMD exited with ${code ?? signal}`)),
    );
  });
}

test("live broker restart: the same session recovers on the data and native planes", {
  skip:
    process.env.UDB_LIVE_SDK_TESTS === "1" && restartCommand
      ? false
      : "requires a live broker and UDB_LIVE_BROKER_RESTART_CMD",
  timeout: 4 * 60_000,
}, async () => {
  const target = requiredEnv("UDB_GRPC_TARGET");
  const session = await UdbProject.connectEnterprise({
    target,
    authTarget: requiredEnv("UDB_AUTH_GRPC_TARGET"),
    username: requiredEnv("UDB_LIVE_USERNAME"),
    password: requiredEnv("UDB_LIVE_PASSWORD"),
    tenantCode: requiredEnv("UDB_LIVE_TENANT"),
    projectId: requiredEnv("UDB_LIVE_PROJECT"),
    purpose: "ts.live.broker-restart",
    deadlineMs: 5_000,
  });
  try {
    const probes: Array<[string, () => Promise<unknown>]> = [
      ["data GetCapabilities", () => session.data.get_capabilities({})],
      ["native ValidateSession", () => session.validateSession(session.bearer())],
    ];
    for (const [name, call] of probes) {
      try {
        await call();
      } catch (err) {
        assert.fail(`before restart, ${name}: code=${(err as { code?: unknown }).code}`);
      }
    }

    await runRestart(restartCommand);

    for (const [name, call] of probes) {
      const deadline = Date.now() + RECOVERY_MS;
      for (;;) {
        try {
          await call();
          break;
        } catch (err) {
          const code = (err as { code?: unknown }).code;
          if (code !== grpc.status.UNAVAILABLE) {
            assert.fail(`after restart, ${name} on the same session: code=${String(code)} err=${String(err)}`);
          }
          if (Date.now() > deadline) assert.fail(`after restart, ${name} stayed UNAVAILABLE for 90s: ${String(err)}`);
          await new Promise((resolve) => setTimeout(resolve, 250));
        }
      }
    }
    assert.equal(session.credentialError(), null, "the session reports a credential failure after the restart");
  } finally {
    session.close();
  }
});
