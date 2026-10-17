// MessagingClient over the real WebAssembly MLS module, against an in-process
// stand-in for the broker's delivery rules (linear epochs with a typed
// stale-epoch refusal, per-device inboxes, COMMIT/WELCOME/MESSAGE fan-out). It
// proves the TypeScript client logic — key-package top-up, claim verification,
// commit confirm/re-base, inbox processing, persistence, the follow stream —
// without a broker; the served path is `live-messaging.test.ts`.
//
// Needs the module built (`npm run build:mls-wasm`). Without it the test is
// skipped, unless UDB_REQUIRE_MLS_WASM=1 (CI) makes that a failure.

import { strict as assert } from "node:assert";
import { test } from "node:test";
import { PassThrough } from "node:stream";
import * as grpc from "@grpc/grpc-js";

import { UdbError } from "./generatedClient";
import { KeyChangedError, loadMlsWasm, MessagingClient, MlsEvent, MlsWasmModule, staleEpoch } from "./messaging";

function wasmOrSkip(): MlsWasmModule | undefined {
  try {
    return loadMlsWasm();
  } catch (err) {
    if (process.env.UDB_REQUIRE_MLS_WASM === "1") throw err;
    return undefined;
  }
}

const TENANT = "tenant-ts-mls";

type Entry = {
  seq: string;
  kind: string;
  group_id: string;
  epoch: string;
  payload: Buffer;
  sender_device_id: string;
  client_message_id: string;
};

/** The broker's delivery rules, minus MLS validation (the served test has it). */
class FakeBroker {
  readonly devices = new Map<string, any>();
  readonly stock = new Map<string, { oneTime: Buffer[]; lastResort?: Buffer }>();
  readonly inboxes = new Map<string, Entry[]>();
  readonly followers = new Map<string, PassThrough>();
  readonly groups = new Map<string, { epoch: number; members: Set<string> }>();
  /** Devices whose KeyPackages a requester claimed and has not committed yet. */
  readonly claimed = new Map<string, Set<string>>();
  readonly acked = new Map<string, number>();
  readonly seen = new Set<string>();
  /** Backup envelopes by `<subject>/<backup id>`, with their versions. */
  readonly backups = new Map<string, { envelope: Buffer; version: number }>();
  private nextSeq = new Map<string, number>();

  constructor(private readonly wasm: MlsWasmModule) {}

  private append(device: string, entry: Omit<Entry, "seq">): void {
    const seq = (this.nextSeq.get(device) ?? 0) + 1;
    this.nextSeq.set(device, seq);
    const full: Entry = { ...entry, seq: String(seq) };
    const inbox = this.inboxes.get(device) ?? [];
    inbox.push(full);
    this.inboxes.set(device, inbox);
    this.followers.get(device)?.write({ entry: full });
  }

  private stale(rpc: string, current: number): UdbError {
    const cause = {
      code: grpc.status.FAILED_PRECONDITION,
      details: `epoch is stale; the group is at epoch ${current}`,
      message: "stale",
      metadata: new grpc.Metadata(),
    } as grpc.ServiceError;
    return new UdbError(rpc, cause, { missing: { current_epoch: String(current) } } as any);
  }

  private notFound(rpc: string): UdbError {
    const cause = {
      code: grpc.status.NOT_FOUND,
      details: "no messaging backup is stored",
      message: "not found",
      metadata: new grpc.Metadata(),
    } as grpc.ServiceError;
    return new UdbError(rpc, cause, {} as any);
  }

  api(): any {
    return {
      put_messaging_backup: async (req: any) => {
        const envelope = Buffer.from(req.ciphertext);
        // The broker refuses anything but a sealed envelope.
        if (envelope.subarray(0, 8).toString("latin1") !== "UDBMLSB1") throw new Error("not an envelope");
        const key = `${req.subject_id}/${req.backup_id || "default"}`;
        const prior = this.backups.get(key);
        const version = (prior?.version ?? 0) + 1;
        this.backups.set(key, { envelope, version });
        return {
          backup: { backup_id: req.backup_id || "default", version: String(version), size_bytes: String(envelope.length) },
          created: !prior,
        };
      },
      get_messaging_backup: async (req: any) => {
        const stored = this.backups.get(`${req.subject_id}/${req.backup_id || "default"}`);
        if (!stored) throw this.notFound("GetMessagingBackup");
        return { backup: { version: String(stored.version) }, ciphertext: stored.envelope };
      },
      delete_messaging_backup: async (req: any) => ({
        deleted: this.backups.delete(`${req.subject_id}/${req.backup_id || "default"}`),
      }),
      register_messaging_device: async (req: any) => {
        const device = {
          tenant_id: req.tenant_id,
          subject_id: req.subject_id,
          device_id: req.device_id,
          signature_public_key: req.signature_public_key,
          key_fingerprint: this.wasm.keyFingerprint(new Uint8Array(req.signature_public_key)),
          status: "ACTIVE",
        };
        this.devices.set(req.device_id, device);
        return { device, created: true };
      },
      list_messaging_devices: async (req: any) => ({
        devices: [...this.devices.values()].filter((d) => d.subject_id === req.subject_id),
      }),
      revoke_messaging_device: async () => ({ revoked: true, group_ids: [] }),
      get_key_package_stock: async (req: any) => {
        const stock = this.stock.get(req.device_id);
        return { one_time_remaining: String(stock?.oneTime.length ?? 0), has_last_resort: Boolean(stock?.lastResort) };
      },
      publish_key_packages: async (req: any) => {
        const stock = this.stock.get(req.device_id) ?? { oneTime: [] };
        stock.oneTime.push(...req.key_packages.map((kp: Uint8Array) => Buffer.from(kp)));
        if (req.last_resort_key_package?.length) stock.lastResort = Buffer.from(req.last_resort_key_package);
        this.stock.set(req.device_id, stock);
        return {
          accepted: req.key_packages.length,
          one_time_remaining: String(stock.oneTime.length),
          has_last_resort: Boolean(stock.lastResort),
        };
      },
      claim_key_packages: async (req: any) => {
        const key_packages: any[] = [];
        const missing_device_ids: string[] = [];
        const claimed = this.claimed.get(req.requester_device_id) ?? new Set<string>();
        for (const id of req.target_device_ids) {
          const stock = this.stock.get(id);
          const kp = stock?.oneTime.shift() ?? stock?.lastResort;
          if (!kp) {
            missing_device_ids.push(id);
            continue;
          }
          claimed.add(id);
          key_packages.push({ subject_id: this.devices.get(id).subject_id, device_id: id, key_package: kp });
        }
        this.claimed.set(req.requester_device_id, claimed);
        return { key_packages, missing_device_ids };
      },
      create_group: async (req: any) => {
        this.groups.set(req.group_id, { epoch: 0, members: new Set([req.creator_device_id]) });
        return { group_id: req.group_id, epoch: "0", created: true };
      },
      submit_commit: async (req: any) => {
        const group = this.groups.get(req.group_id)!;
        if (Number(req.epoch) !== group.epoch) throw this.stale("SubmitCommit", group.epoch);
        const before = new Set(group.members);
        const added = req.welcome.length ? [...(this.claimed.get(req.sender_device_id) ?? [])] : [];
        this.claimed.delete(req.sender_device_id);
        for (const id of added) group.members.add(id);
        for (const id of new Set([...before, ...group.members])) {
          const welcome = added.includes(id);
          this.append(id, {
            kind: welcome ? "WELCOME" : "COMMIT",
            group_id: req.group_id,
            epoch: String(welcome ? group.epoch + 1 : group.epoch),
            payload: Buffer.from(welcome ? req.welcome : req.commit),
            sender_device_id: req.sender_device_id,
            client_message_id: req.client_message_id,
          });
        }
        group.epoch += 1;
        return { epoch: String(group.epoch), member_device_ids: [...group.members], added_device_ids: added };
      },
      send_message: async (req: any) => {
        const key = `${req.group_id}/${req.client_message_id}`;
        if (this.seen.has(key)) return { duplicate: true };
        this.seen.add(key);
        const group = this.groups.get(req.group_id)!;
        for (const id of group.members) {
          this.append(id, {
            kind: "MESSAGE",
            group_id: req.group_id,
            epoch: String(group.epoch),
            payload: Buffer.from(req.ciphertext),
            sender_device_id: req.sender_device_id,
            client_message_id: req.client_message_id,
          });
        }
        return { epoch: String(group.epoch), fanout: group.members.size };
      },
      stream_inbox: (req: any) => {
        const after = Number(req.after_seq);
        const pending = (this.inboxes.get(req.device_id) ?? []).filter((e) => Number(e.seq) > after);
        const stream = new PassThrough({ objectMode: true });
        for (const entry of pending) stream.write({ entry });
        if (req.follow) {
          this.followers.set(req.device_id, stream);
          (stream as any).cancel = () => {
            this.followers.delete(req.device_id);
            stream.end();
          };
        } else {
          stream.end();
        }
        return stream;
      },
      ack_inbox: async (req: any) => {
        const acked = Math.max(this.acked.get(req.device_id) ?? 0, Number(req.up_to_seq));
        this.acked.set(req.device_id, acked);
        return { acked_seq: String(acked) };
      },
    };
  }
}

function texts(events: MlsEvent[]): Array<[string, string]> {
  return events
    .filter((e) => e.kind === "message")
    .map((e) => [e.deviceId, new TextDecoder().decode(e.plaintext)]);
}

test("TypeScript MessagingClient runs a group over the WebAssembly MLS core", async (t) => {
  const wasm = wasmOrSkip();
  if (!wasm) {
    t.skip("the MLS WebAssembly module is not built (npm run build:mls-wasm)");
    return;
  }
  const broker = new FakeBroker(wasm);
  const snapshots = new Map<string, Uint8Array>();
  const client = (subject: string, deviceId: string) =>
    new MessagingClient({
      api: broker.api(),
      device: wasm.MlsDevice.create(TENANT, subject, deviceId),
      wasm,
      persist: (snapshot) => {
        snapshots.set(deviceId, snapshot);
      },
      keyPackageTarget: 3,
      keyPackageThreshold: 3,
    });
  const a = client("alice", "a-1");
  const b = client("bob", "b-1");
  const c = client("carol", "c-1");
  for (const device of [a, b, c]) {
    const registered = await device.registerDevice();
    assert.equal(registered.status, "ACTIVE");
    assert.deepEqual(await device.ensureKeyPackages(), { oneTimeRemaining: 3, hasLastResort: true, published: 3 });
  }

  assert.equal(await a.createGroup("room"), 0);
  const added = await a.addMembers("room", ["bob"], ["c-1"]);
  assert.deepEqual([added.epoch, added.committed, added.addedDeviceIds], [1, true, ["b-1", "c-1"]]);
  for (const device of [b, c]) {
    const events = await device.sync();
    assert.equal(events.length, 1);
    assert.equal(events[0].kind, "joined");
    assert.deepEqual(events[0].members, ["a-1", "b-1", "c-1"]);
  }
  // The claim used one package; the top-up republishes exactly one.
  assert.deepEqual(await b.ensureKeyPackages(), { oneTimeRemaining: 3, hasLastResort: true, published: 1 });

  await a.send("room", "hello from a");
  await b.send("room", new TextEncoder().encode("hello from b"));
  assert.deepEqual(texts(await c.sync()), [["a-1", "hello from a"], ["b-1", "hello from b"]]);
  const aEvents = await a.sync();
  assert.deepEqual(texts(aEvents), [["b-1", "hello from b"]]);
  assert.ok(aEvents.some((e) => e.kind === "own_commit" && e.epoch === 1));

  // A lost race: b's rotation lands first; c's is refused stale and re-based.
  assert.deepEqual([(await b.updateKeys("room")).epoch], [2]);
  const cUpdate = await c.updateKeys("room");
  assert.deepEqual([cUpdate.epoch, cUpdate.rebases], [3, 1]);
  assert.ok((await c.sync()).some((e) => e.kind === "commit" && e.epoch === 2 && e.deviceId === "b-1"));
  for (const device of [a, b]) {
    await device.sync();
    assert.equal(device.device.epoch("room"), 3);
  }

  // The follow stream delivers live; b's persisted state restores and reads on.
  const waiting = b.nextEvent(5_000);
  await a.send("room", "live");
  const live = await waiting;
  assert.ok(live);
  assert.deepEqual(texts([live]), [["a-1", "live"]]);
  b.close();
  const restored = wasm.MlsDevice.restore(snapshots.get("b-1")!);
  assert.equal(restored.epoch("room"), 3);
  assert.equal(restored.inboxCursor, b.device.inboxCursor);

  // A stale-epoch error is recognised from the typed detail only.
  const err = new UdbError(
    "SubmitCommit",
    { code: grpc.status.FAILED_PRECONDITION, details: "x", message: "x", metadata: new grpc.Metadata() } as grpc.ServiceError,
    { missing: { current_epoch: "7" } } as any,
  );
  assert.equal(staleEpoch(err), 7);
  assert.equal(staleEpoch(new Error("FAILED_PRECONDITION current_epoch 7")), undefined);
});

test("an encrypted backup restores a device on a new machine", async (t) => {
  const wasm = wasmOrSkip();
  if (!wasm) {
    t.skip("the MLS WebAssembly module is not built (npm run build:mls-wasm)");
    return;
  }
  const broker = new FakeBroker(wasm);
  // Cheap Argon2id costs keep the test fast; the defaults are the recommended ones.
  const backupParams = { memoryKib: 256, iterations: 1, parallelism: 1 };
  const make = (subject: string, deviceId: string) =>
    new MessagingClient({
      api: broker.api(),
      device: wasm.MlsDevice.create(TENANT, subject, deviceId),
      wasm,
      keyPackageTarget: 2,
      keyPackageThreshold: 2,
      backupParams,
    });
  const a = make("alice", "a-1");
  const b = make("bob", "b-1");
  for (const device of [a, b]) {
    await device.registerDevice();
    await device.ensureKeyPackages();
  }
  await a.createGroup("room");
  await a.addMembers("room", [], ["b-1"]);
  await b.sync();
  await a.send("room", "before the backup");
  assert.deepEqual(texts(await b.sync()), [["a-1", "before the backup"]]);

  const passphrase = "bob's recovery passphrase";
  const stored = await b.backup(passphrase, "bob's chat history");
  assert.deepEqual([stored.backup_id, stored.version], ["default", "1"]);
  const held = broker.backups.get("bob/default")!.envelope;
  assert.equal(held.subarray(0, 8).toString("latin1"), "UDBMLSB1");
  for (const needle of ["bob's chat history", passphrase, `${TENANT}/bob/b-1`]) {
    assert.equal(held.includes(Buffer.from(needle)), false, `the broker copy must not contain ${needle}`);
  }

  // Bob's machine is lost; alice keeps talking.
  b.close();
  await a.send("room", "while bob was away");
  await assert.rejects(
    MessagingClient.restore({ api: broker.api(), wasm, tenantId: TENANT, subjectId: "bob", recoverySecret: "not the passphrase" }),
    /does not open/,
  );
  let persisted: Uint8Array | undefined;
  const { client: restored, history } = await MessagingClient.restore({
    api: broker.api(),
    wasm,
    tenantId: TENANT,
    subjectId: "bob",
    recoverySecret: passphrase,
    persist: (snapshot) => {
      persisted = snapshot;
    },
  });
  assert.equal(new TextDecoder().decode(history), "bob's chat history");
  assert.ok(persisted && persisted.length > 0, "the restored state is persisted before use");
  assert.equal(restored.device.deviceId, "b-1");
  assert.deepEqual(texts(await restored.sync()), [["a-1", "while bob was away"]]);

  // The restored device follows a rotation and keeps decrypting, both ways.
  await a.updateKeys("room");
  await a.send("room", "after the restore");
  assert.deepEqual(texts(await restored.sync()), [["a-1", "after the restore"]]);
  await restored.send("room", "bob is back");
  assert.deepEqual(texts(await a.sync()), [["b-1", "bob is back"]]);

  assert.equal(await restored.deleteBackup(), true);
  assert.equal(await restored.deleteBackup(), false);
  await assert.rejects(
    MessagingClient.restore({ api: broker.api(), wasm, tenantId: TENANT, subjectId: "bob", recoverySecret: passphrase }),
    (err: unknown) => err instanceof UdbError && err.code === grpc.status.NOT_FOUND,
  );
});

test("pinned keys flag a swapped directory key and safety numbers match", async (t) => {
  const wasm = wasmOrSkip();
  if (!wasm) {
    t.skip("the MLS WebAssembly module is not built (npm run build:mls-wasm)");
    return;
  }
  const broker = new FakeBroker(wasm);
  const make = (subject: string, deviceId: string) =>
    new MessagingClient({
      api: broker.api(),
      device: wasm.MlsDevice.create(TENANT, subject, deviceId),
      wasm,
      keyPackageTarget: 2,
      keyPackageThreshold: 2,
    });
  const a = make("alice", "a-1");
  const b = make("bob", "b-1");
  for (const device of [a, b]) {
    await device.registerDevice();
    await device.ensureKeyPackages();
  }

  // First sight pins silently; both sides compute the same safety number.
  await a.listDevices("bob");
  const pinned = a.pinnedDevices("bob");
  assert.deepEqual(pinned.map((p) => [p.deviceId, p.keyFingerprint, p.verified]), [
    ["b-1", b.device.registration().keyFingerprint, false],
  ]);
  assert.ok(pinned[0].firstSeenUnix > 0, "the JS host clock stamps pins");
  assert.deepEqual(await a.takeKeyEvents(), []);
  await a.createGroup("room");
  await a.addMembers("room", ["bob"]);
  await b.sync();
  const number = await a.safetyNumber("bob");
  assert.match(number, /^(\d{5} ){11}\d{5}$/);
  assert.equal(await b.safetyNumber("alice"), number);
  await a.verifyDevice("bob", "b-1", b.device.registration().keyFingerprint);
  await assert.rejects(a.verifyDevice("bob", "b-1", "0".repeat(64)), KeyChangedError);

  // A new device of a known subject is an event.
  const tablet = make("bob", "b-2");
  await tablet.registerDevice();
  await a.listDevices("bob");
  const events = await a.takeKeyEvents();
  assert.deepEqual(events.map((e) => [e.kind, e.subjectId, e.deviceId, e.source]), [
    ["new_device", "bob", "b-2", "directory"],
  ]);
  const withTablet = await a.safetyNumber("bob");
  assert.notEqual(withTablet, number);

  // A compromised directory swaps b-1's key: listing and adding fail closed.
  const impostor = make("bob", "b-1");
  await impostor.registerDevice();
  await impostor.ensureKeyPackages();
  const observed = impostor.device.registration().keyFingerprint;
  await assert.rejects(a.listDevices("bob"), (err: unknown) => {
    assert.ok(err instanceof KeyChangedError, String(err));
    assert.equal(err.deviceId, "b-1");
    assert.equal(err.pinnedFingerprint, b.device.registration().keyFingerprint);
    assert.equal(err.observedFingerprint, observed);
    return true;
  });
  await a.createGroup("room-2");
  await assert.rejects(a.addMembers("room-2", ["bob"]), KeyChangedError);
  assert.equal(a.device.members("room-2").length, 1);
  assert.equal(a.pinnedDevices("bob")[0].pendingFingerprint, observed);
  assert.equal(await a.safetyNumber("bob").catch((err) => err instanceof KeyChangedError), true);

  // Only the compared key is accepted; afterwards the numbers differ.
  await assert.rejects(a.acceptKeyChange("bob", "b-1", b.device.registration().keyFingerprint));
  await a.acceptKeyChange("bob", "b-1", observed);
  const after = await a.safetyNumber("bob");
  assert.notEqual(after, withTablet);
  assert.equal(a.pinnedDevices("bob").find((p) => p.deviceId === "b-1")?.verified, false);
});
