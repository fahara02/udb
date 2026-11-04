// Runs the installed real broker with its embedded PostgreSQL, for tests that need
// what a fake cannot give: real SQL, real policies, real events.

import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import { UdbProject } from "../project";
import type { TestHooks } from "./fake";

const STARTUP_LIMIT_MS = 10 * 60_000;
// The broker reloads its authz snapshot on a timer; offline-written admins and
// policies are invisible until then, so wait for two reloads before the first login.
const AUTHZ_RELOAD_MS = 5_000;

function findOnPath(name: string): string | undefined {
  const extensions = process.platform === "win32" ? [".exe", ".cmd", ""] : [""];
  for (const dir of (process.env.PATH ?? "").split(path.delimiter)) {
    for (const extension of extensions) {
      const candidate = path.join(dir, name + extension);
      if (dir && fs.existsSync(candidate)) return candidate;
    }
  }
  return undefined;
}

async function reserve(reservations: net.Server[]): Promise<string> {
  const server = net.createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  reservations.push(server);
  return `127.0.0.1:${(server.address() as net.AddressInfo).port}`;
}

function run(binary: string, args: string[], options: { cwd: string; env: NodeJS.ProcessEnv; log: number; timeoutMs: number }): { ok: boolean; error: string } {
  const result = spawnSync(binary, args, { cwd: options.cwd, env: options.env, stdio: ["ignore", options.log, options.log], timeout: options.timeoutMs });
  return { ok: result.status === 0, error: result.error?.message ?? `exit ${result.status}` };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isListening(address: string): Promise<boolean> {
  const [host, port] = address.split(":");
  return new Promise((resolve) => {
    const socket = net.connect({ host, port: Number(port) });
    const done = (result: boolean) => {
      socket.destroy();
      resolve(result);
    };
    socket.setTimeout(1000, () => done(false));
    socket.once("connect", () => done(true));
    socket.once("error", () => done(false));
  });
}

async function startBroker(t: TestHooks, protoRootArg: string | undefined, policyFileArg: string | undefined, withPeer: boolean): Promise<[UdbProject, UdbProject | undefined]> {
  const binary = path.resolve(process.env.UDB_TEST_BINARY || findOnPath("udb") || "");
  if (!process.env.UDB_TEST_BINARY && !findOnPath("udb")) {
    throw new Error("udbtest.start: install an embedded-capable udb or set UDB_TEST_BINARY");
  }
  const project = fs.mkdtempSync(path.join(os.tmpdir(), "udbtest-"));
  let protoRoot = protoRootArg;
  if (!protoRoot) {
    protoRoot = path.join(project, "proto");
    fs.mkdirSync(protoRoot, { mode: 0o700 });
  }
  protoRoot = path.resolve(protoRoot);
  const policyFile = policyFileArg ? path.resolve(policyFileArg) : undefined;
  // Avoid inheriting another project's listeners, database DSNs or credentials.
  // Acquisition settings alone may be shared between isolated test brokers.
  const environment: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(process.env)) {
    const upper = key.toUpperCase();
    if (!upper.startsWith("UDB_") && upper !== "DATABASE_URL") environment[key] = value;
  }
  for (const key of ["UDB_HOME", "UDB_POSTGRES_BINARIES_URL", "UDB_EMBEDDED_PG_ARCHIVE"]) {
    if (process.env[key] !== undefined) environment[key] = process.env[key];
  }
  environment.UDB_PROTO_ROOT = protoRoot;
  // Hold reservations until every port is selected, so listeners in the same child
  // cannot receive the same ephemeral port.
  const reservations: net.Server[] = [];
  const pgAddress = await reserve(reservations);
  const pgPort = pgAddress.split(":")[1];
  const addresses: Record<string, string> = {};
  for (const key of ["UDB_GRPC_ADDR", "UDB_AUTH_GRPC_ADDR", "UDB_WEBRTC_GRPC_ADDR", "UDB_HTTP_ADDR", "UDB_METRICS_ADDR"]) {
    addresses[key] = await reserve(reservations);
    environment[key] = addresses[key];
  }
  const logPath = path.join(project, "broker.log");
  const log = fs.openSync(logPath, "wx", 0o600);
  await Promise.all(reservations.map((server) => new Promise((resolve) => server.close(resolve))));
  const child = spawn(binary, ["dev", "up", "--embedded", "--pg-port", pgPort], {
    cwd: project,
    env: environment,
    stdio: ["ignore", log, log],
  });
  let exited = false;
  let exitError = "";
  const exit = new Promise<void>((resolve) => {
    child.once("exit", (code, signal) => {
      exited = true;
      if (code !== 0 && code !== null) exitError = `exit ${code}`;
      else if (signal) exitError = `signal ${signal}`;
      resolve();
    });
    child.once("error", (err) => {
      exited = true;
      exitError = err.message;
      resolve();
    });
  });
  const clients: UdbProject[] = [];
  const stop = async () => {
    for (const client of clients) client.close();
    const stopped = run(binary, ["dev", "down", "--embedded"], { cwd: project, env: environment, log, timeoutMs: 45_000 });
    const finished = await Promise.race([exit.then(() => true), sleep(45_000).then(() => false)]);
    if (!finished) child.kill("SIGKILL");
    fs.closeSync(log);
    if (!stopped.ok) throw new Error(`udbtest.start: stop embedded broker: ${stopped.error} (private log: ${logPath})`);
    if (!finished) throw new Error("udbtest.start: embedded broker did not stop within 45 seconds");
    if (exitError) throw new Error(`udbtest.start: broker exit: ${exitError} (private log: ${logPath})`);
    for (const address of [pgAddress, addresses.UDB_GRPC_ADDR, addresses.UDB_AUTH_GRPC_ADDR]) {
      if (await isListening(address)) throw new Error(`udbtest.start: shutdown left ${address} listening`);
    }
  };
  t.after(stop);

  const marker = path.join(project, ".udb", "dev", "bootstrap.json");
  const deadline = Date.now() + STARTUP_LIMIT_MS;
  let credentials: { tenant_id: string; username: string; password: string; dsn: string } | undefined;
  for (;;) {
    try {
      const parsed = JSON.parse(fs.readFileSync(marker, "utf8"));
      if (parsed.password && parsed.tenant_id) {
        credentials = parsed;
        break;
      }
    } catch {
      // Not written yet.
    }
    if (exited) throw new Error(`udbtest.start: broker exited during startup: ${exitError} (private log: ${logPath})`);
    if (Date.now() > deadline) throw new Error(`udbtest.start: startup exceeded 10 minutes (private log: ${logPath})`);
    await sleep(200);
  }
  const creds = credentials!;
  const peerId = randomUUID();
  const peerUsername = `udbtest-peer-${peerId}`;
  const peerPassword = `UdbTestPeer#${randomUUID()}`;
  const peerTenant = randomUUID();
  if (withPeer) {
    // The peer is created offline against the dev cluster, so it needs the secrets
    // the dev broker hashes passwords and encrypts with.
    const devSecret = (file: string) => fs.readFileSync(path.join(project, ".udb", "dev", file), "utf8").trim();
    const provisioned = run(
      binary,
      ["auth", "bootstrap", "user", "--username", peerUsername, "--email", `${peerUsername}@udbtest.invalid`, "--password", peerPassword, "--tenant", peerTenant, "--project", "default"],
      {
        cwd: project,
        env: { ...environment, UDB_SESSION_HASH_SECRET: devSecret("session-secret"), UDB_ENCRYPTION_KEY: devSecret("encryption-key"), UDB_PG_DSN: creds.dsn },
        log,
        timeoutMs: 60_000,
      },
    );
    if (!provisioned.ok) throw new Error(`udbtest.startPair: provision peer tenant: ${provisioned.error} (private log: ${logPath})`);
  }
  if (policyFile) {
    // Every provisioned tenant gets the same policy: the shared contract writes as both.
    for (const tenant of withPeer ? [creds.tenant_id, peerTenant] : [creds.tenant_id]) {
      const applied = run(binary, ["policy", "apply", "-f", policyFile, "--tenant", tenant], { cwd: project, env: { ...environment, UDB_PG_DSN: creds.dsn }, log, timeoutMs: 60_000 });
      if (!applied.ok) throw new Error(`udbtest.start: apply policy file for tenant ${tenant}: ${applied.error} (private log: ${logPath})`);
    }
  }
  await sleep(2 * AUTHZ_RELOAD_MS + 1000);
  const connect = async (username: string, password: string, tenantId: string) => {
    const session = await UdbProject.connectEnterprise({
      target: addresses.UDB_GRPC_ADDR,
      authTarget: addresses.UDB_AUTH_GRPC_ADDR,
      username,
      password,
      tenantId,
      projectId: "default",
      purpose: "udbtest.embedded",
      deadlineMs: 30_000,
    });
    clients.push(session);
    return session;
  };
  // Bootstrap credentials and the DSN are never printed to test output.
  const session = await connect(creds.username, creds.password, creds.tenant_id);
  return [session, withPeer ? await connect(peerUsername, peerPassword, peerTenant) : undefined];
}

/** Run the installed real broker with its embedded PostgreSQL, log in as the
 *  bootstrapped administrator, and return its tenant-scoped client. Everything stops
 *  when `t` ends. `UDB_TEST_BINARY` selects a broker binary; otherwise `udb` on PATH.
 *  It never builds a broker or runs Docker.
 *
 *  `protoRoot` names the application's proto directory; omitted, an empty project
 *  serves only the broker's native models. `policyFile` (JSON or YAML) is applied
 *  through the real `udb policy apply` command. */
export async function start(t: TestHooks, protoRoot?: string, policyFile?: string): Promise<UdbProject> {
  return (await startBroker(t, protoRoot, policyFile, false))[0];
}

/** One isolated embedded broker with two independently provisioned tenant
 *  administrators, for the full shared isolation contract. */
export async function startPair(t: TestHooks, protoRoot?: string, policyFile?: string): Promise<[UdbProject, UdbProject]> {
  const [first, second] = await startBroker(t, protoRoot, policyFile, true);
  return [first, second!];
}
