// Rust <-> TypeScript end-to-end encrypted group chat through a real broker.
//
// A TypeScript device (MessagingClient over the WebAssembly MLS core) and a Rust
// device (the Rust SDK's `messaging_peer` example, one command per process, its
// state in a file) share one MLS group: the TypeScript device creates it and
// adds the Rust device by a claimed KeyPackage, the Rust device joins from the
// Welcome, both directions decrypt, and each side's key rotation is applied by
// the other.
//
// Gated on UDB_LIVE_SDK_TESTS=1. Needs UDB_AUTH_GRPC_TARGET, UDB_LIVE_USERNAME,
// UDB_LIVE_PASSWORD, UDB_LIVE_TENANT, UDB_RUST_MESSAGING_PEER (the built
// `messaging_peer` binary) and the WebAssembly module (`npm run build:mls-wasm`).

import { strict as assert } from "node:assert";
import { test } from "node:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { UdbGeneratedClient } from "./generatedClient";
import { loadMlsWasm, MessagingClient, MlsEvent } from "./messaging";

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
  return value;
}

function peer(binary: string, state: string, ...args: string[]): any[] {
  const out = execFileSync(binary, [state, ...args], { env: process.env, encoding: "utf8", timeout: 60_000 });
  return out
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

function texts(events: MlsEvent[]): Array<[string, string]> {
  return events
    .filter((e) => e.kind === "message")
    .map((e) => [e.deviceId, new TextDecoder().decode(e.plaintext)]);
}

test("Rust and TypeScript devices share an end-to-end encrypted group", async (t) => {
  if (process.env.UDB_LIVE_SDK_TESTS !== "1") {
    t.skip("the live messaging interop test needs UDB_LIVE_SDK_TESTS=1");
    return;
  }
  const rustPeer = requiredEnv("UDB_RUST_MESSAGING_PEER");
  const target = requiredEnv("UDB_AUTH_GRPC_TARGET");
  const tenantHint = requiredEnv("UDB_LIVE_TENANT");
  const projectId = process.env.UDB_LIVE_PROJECT?.trim() || "default";
  const wasm = loadMlsWasm();

  const meta = { tenantId: tenantHint, projectId, purpose: "ts.live.messaging", correlationId: "" };
  const anonymous = new UdbGeneratedClient({ target, meta });
  const login: any = await anonymous.AuthnService.login({
    username: requiredEnv("UDB_LIVE_USERNAME"),
    password: requiredEnv("UDB_LIVE_PASSWORD"),
    tenant_hint: tenantHint,
    project_hint: projectId,
    device_name: "ts-live-messaging",
  });
  const who: any = await anonymous.AuthnService.authenticate({
    bearer_token: login.access_token,
    tenant_hint: tenantHint,
    project_hint: projectId,
  });
  anonymous.core.close();
  const tenantId: string = who.principal.tenant_id;
  const subject: string = who.principal.subject;
  const generated = new UdbGeneratedClient({
    target,
    meta: { ...meta, tenantId },
    bearerToken: login.access_token,
  });

  const run = Date.now().toString(16);
  const group = `interop-${run}`;
  const tsDeviceId = `ts-${run}`;
  const dir = mkdtempSync(path.join(os.tmpdir(), "udb-mls-interop-"));
  const state = path.join(dir, "rust-peer.mls");
  const chat = new MessagingClient({
    api: generated.MessagingService,
    device: wasm.MlsDevice.create(tenantId, subject, tsDeviceId),
    wasm,
    keyPackageTarget: 3,
    keyPackageThreshold: 3,
  });
  try {
    await chat.registerDevice();
    await chat.ensureKeyPackages();
    const [rust] = peer(rustPeer, state, "init", `rs-${run}`);
    const rustDeviceId: string = rust.device_id;
    assert.ok(rust.published >= 1, "the Rust device published KeyPackages");

    // TypeScript creates the group and adds the Rust device.
    assert.equal(await chat.createGroup(group), 0);
    const added = await chat.addMembers(group, [], [rustDeviceId]);
    assert.deepEqual([added.epoch, added.addedDeviceIds], [1, [rustDeviceId]]);
    const joined = peer(rustPeer, state, "sync");
    assert.ok(
      joined.some((e) => e.kind === "joined" && e.group_id === group && e.members.includes(tsDeviceId)),
      JSON.stringify(joined),
    );

    // TypeScript -> Rust.
    await chat.send(group, "hello from typescript");
    assert.deepEqual(
      peer(rustPeer, state, "sync").filter((e) => e.kind === "message").map((e) => [e.device_id, e.text]),
      [[tsDeviceId, "hello from typescript"]],
    );

    // Rust -> TypeScript.
    peer(rustPeer, state, "send", group, "hello from rust");
    assert.deepEqual(texts(await chat.sync()), [[rustDeviceId, "hello from rust"]]);

    // Rust rotates its keys; TypeScript applies the Rust-built commit.
    const rotated = peer(rustPeer, state, "update", group)[0];
    assert.equal(rotated.epoch, 2);
    const tsEvents = await chat.sync();
    assert.ok(tsEvents.some((e) => e.kind === "commit" && e.epoch === 2 && e.deviceId === rustDeviceId), JSON.stringify(tsEvents));

    // TypeScript rotates; Rust applies the WebAssembly-built commit and reads on.
    assert.equal((await chat.updateKeys(group)).epoch, 3);
    await chat.send(group, "after both rotations");
    const rustEvents = peer(rustPeer, state, "sync");
    assert.ok(rustEvents.some((e) => e.kind === "commit" && e.epoch === 3 && e.device_id === tsDeviceId), JSON.stringify(rustEvents));
    assert.deepEqual(
      rustEvents.filter((e) => e.kind === "message").map((e) => [e.device_id, e.text]),
      [[tsDeviceId, "after both rotations"]],
    );

    // The follow stream: TypeScript waits, Rust sends.
    await chat.sync();
    const first = chat.nextEvent(15_000);
    peer(rustPeer, state, "send", group, "live from rust");
    let live: MlsEvent | undefined = await first;
    for (let i = 0; live && live.kind !== "message" && i < 5; i += 1) live = await chat.nextEvent(15_000);
    assert.ok(live, "a live event arrived");
    assert.deepEqual(texts([live]), [[rustDeviceId, "live from rust"]]);
  } finally {
    chat.close();
    try {
      peer(rustPeer, state, "revoke");
      await chat.revokeDevice(tsDeviceId, "interop test cleanup");
    } finally {
      generated.core.close();
      rmSync(dir, { recursive: true, force: true });
    }
  }
});
