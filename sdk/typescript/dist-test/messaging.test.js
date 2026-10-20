"use strict";
// MessagingClient over the real WebAssembly MLS module, against an in-process
// stand-in for the broker's delivery rules (linear epochs with a typed
// stale-epoch refusal, per-device inboxes, COMMIT/WELCOME/MESSAGE fan-out). It
// proves the TypeScript client logic — key-package top-up, claim verification,
// commit confirm/re-base, inbox processing, persistence, the follow stream —
// without a broker; the served path is `live-messaging.test.ts`.
//
// Needs the module built (`npm run build:mls-wasm`). Without it the test is
// skipped, unless UDB_REQUIRE_MLS_WASM=1 (CI) makes that a failure.
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
const node_test_1 = require("node:test");
const node_stream_1 = require("node:stream");
const grpc = __importStar(require("@grpc/grpc-js"));
const generatedClient_1 = require("./generatedClient");
const messaging_1 = require("./messaging");
function wasmOrSkip() {
    try {
        return (0, messaging_1.loadMlsWasm)();
    }
    catch (err) {
        if (process.env.UDB_REQUIRE_MLS_WASM === "1")
            throw err;
        return undefined;
    }
}
const TENANT = "tenant-ts-mls";
/** The broker's delivery rules, minus MLS validation (the served test has it). */
class FakeBroker {
    wasm;
    devices = new Map();
    stock = new Map();
    inboxes = new Map();
    followers = new Map();
    groups = new Map();
    /** Devices whose KeyPackages a requester claimed and has not committed yet. */
    claimed = new Map();
    acked = new Map();
    seen = new Set();
    /** Backup envelopes by `<subject>/<backup id>`, with their versions. */
    backups = new Map();
    nextSeq = new Map();
    constructor(wasm) {
        this.wasm = wasm;
    }
    append(device, entry) {
        const seq = (this.nextSeq.get(device) ?? 0) + 1;
        this.nextSeq.set(device, seq);
        const full = { ...entry, seq: String(seq) };
        const inbox = this.inboxes.get(device) ?? [];
        inbox.push(full);
        this.inboxes.set(device, inbox);
        this.followers.get(device)?.write({ entry: full });
    }
    stale(rpc, current) {
        const cause = {
            code: grpc.status.FAILED_PRECONDITION,
            details: `epoch is stale; the group is at epoch ${current}`,
            message: "stale",
            metadata: new grpc.Metadata(),
        };
        return new generatedClient_1.UdbError(rpc, cause, { missing: { current_epoch: String(current) } });
    }
    notFound(rpc) {
        const cause = {
            code: grpc.status.NOT_FOUND,
            details: "no messaging backup is stored",
            message: "not found",
            metadata: new grpc.Metadata(),
        };
        return new generatedClient_1.UdbError(rpc, cause, {});
    }
    api() {
        return {
            put_messaging_backup: async (req) => {
                const envelope = Buffer.from(req.ciphertext);
                // The broker refuses anything but a sealed envelope.
                if (envelope.subarray(0, 8).toString("latin1") !== "UDBMLSB1")
                    throw new Error("not an envelope");
                const key = `${req.subject_id}/${req.backup_id || "default"}`;
                const prior = this.backups.get(key);
                const version = (prior?.version ?? 0) + 1;
                this.backups.set(key, { envelope, version });
                return {
                    backup: { backup_id: req.backup_id || "default", version: String(version), size_bytes: String(envelope.length) },
                    created: !prior,
                };
            },
            get_messaging_backup: async (req) => {
                const stored = this.backups.get(`${req.subject_id}/${req.backup_id || "default"}`);
                if (!stored)
                    throw this.notFound("GetMessagingBackup");
                return { backup: { version: String(stored.version) }, ciphertext: stored.envelope };
            },
            delete_messaging_backup: async (req) => ({
                deleted: this.backups.delete(`${req.subject_id}/${req.backup_id || "default"}`),
            }),
            register_messaging_device: async (req) => {
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
            list_messaging_devices: async (req) => ({
                devices: [...this.devices.values()].filter((d) => d.subject_id === req.subject_id),
            }),
            revoke_messaging_device: async () => ({ revoked: true, group_ids: [] }),
            get_key_package_stock: async (req) => {
                const stock = this.stock.get(req.device_id);
                return { one_time_remaining: String(stock?.oneTime.length ?? 0), has_last_resort: Boolean(stock?.lastResort) };
            },
            publish_key_packages: async (req) => {
                const stock = this.stock.get(req.device_id) ?? { oneTime: [] };
                stock.oneTime.push(...req.key_packages.map((kp) => Buffer.from(kp)));
                if (req.last_resort_key_package?.length)
                    stock.lastResort = Buffer.from(req.last_resort_key_package);
                this.stock.set(req.device_id, stock);
                return {
                    accepted: req.key_packages.length,
                    one_time_remaining: String(stock.oneTime.length),
                    has_last_resort: Boolean(stock.lastResort),
                };
            },
            claim_key_packages: async (req) => {
                const key_packages = [];
                const missing_device_ids = [];
                const claimed = this.claimed.get(req.requester_device_id) ?? new Set();
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
            create_group: async (req) => {
                this.groups.set(req.group_id, { epoch: 0, members: new Set([req.creator_device_id]) });
                return { group_id: req.group_id, epoch: "0", created: true };
            },
            submit_commit: async (req) => {
                const group = this.groups.get(req.group_id);
                if (Number(req.epoch) !== group.epoch)
                    throw this.stale("SubmitCommit", group.epoch);
                const before = new Set(group.members);
                const added = req.welcome.length ? [...(this.claimed.get(req.sender_device_id) ?? [])] : [];
                this.claimed.delete(req.sender_device_id);
                for (const id of added)
                    group.members.add(id);
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
            send_message: async (req) => {
                const key = `${req.group_id}/${req.client_message_id}`;
                if (this.seen.has(key))
                    return { duplicate: true };
                this.seen.add(key);
                const group = this.groups.get(req.group_id);
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
            stream_inbox: (req) => {
                const after = Number(req.after_seq);
                const pending = (this.inboxes.get(req.device_id) ?? []).filter((e) => Number(e.seq) > after);
                const stream = new node_stream_1.PassThrough({ objectMode: true });
                for (const entry of pending)
                    stream.write({ entry });
                if (req.follow) {
                    this.followers.set(req.device_id, stream);
                    stream.cancel = () => {
                        this.followers.delete(req.device_id);
                        stream.end();
                    };
                }
                else {
                    stream.end();
                }
                return stream;
            },
            ack_inbox: async (req) => {
                const acked = Math.max(this.acked.get(req.device_id) ?? 0, Number(req.up_to_seq));
                this.acked.set(req.device_id, acked);
                return { acked_seq: String(acked) };
            },
        };
    }
}
function texts(events) {
    return events
        .filter((e) => e.kind === "message")
        .map((e) => [e.deviceId, new TextDecoder().decode(e.plaintext)]);
}
(0, node_test_1.test)("TypeScript MessagingClient runs a group over the WebAssembly MLS core", async (t) => {
    const wasm = wasmOrSkip();
    if (!wasm) {
        t.skip("the MLS WebAssembly module is not built (npm run build:mls-wasm)");
        return;
    }
    const broker = new FakeBroker(wasm);
    const snapshots = new Map();
    const client = (subject, deviceId) => new messaging_1.MessagingClient({
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
        node_assert_1.strict.equal(registered.status, "ACTIVE");
        node_assert_1.strict.deepEqual(await device.ensureKeyPackages(), { oneTimeRemaining: 3, hasLastResort: true, published: 3 });
    }
    node_assert_1.strict.equal(await a.createGroup("room"), 0);
    const added = await a.addMembers("room", ["bob"], ["c-1"]);
    node_assert_1.strict.deepEqual([added.epoch, added.committed, added.addedDeviceIds], [1, true, ["b-1", "c-1"]]);
    for (const device of [b, c]) {
        const events = await device.sync();
        node_assert_1.strict.equal(events.length, 1);
        node_assert_1.strict.equal(events[0].kind, "joined");
        node_assert_1.strict.deepEqual(events[0].members, ["a-1", "b-1", "c-1"]);
    }
    // The claim used one package; the top-up republishes exactly one.
    node_assert_1.strict.deepEqual(await b.ensureKeyPackages(), { oneTimeRemaining: 3, hasLastResort: true, published: 1 });
    await a.send("room", "hello from a");
    await b.send("room", new TextEncoder().encode("hello from b"));
    node_assert_1.strict.deepEqual(texts(await c.sync()), [["a-1", "hello from a"], ["b-1", "hello from b"]]);
    const aEvents = await a.sync();
    node_assert_1.strict.deepEqual(texts(aEvents), [["b-1", "hello from b"]]);
    node_assert_1.strict.ok(aEvents.some((e) => e.kind === "own_commit" && e.epoch === 1));
    // A lost race: b's rotation lands first; c's is refused stale and re-based.
    node_assert_1.strict.deepEqual([(await b.updateKeys("room")).epoch], [2]);
    const cUpdate = await c.updateKeys("room");
    node_assert_1.strict.deepEqual([cUpdate.epoch, cUpdate.rebases], [3, 1]);
    node_assert_1.strict.ok((await c.sync()).some((e) => e.kind === "commit" && e.epoch === 2 && e.deviceId === "b-1"));
    for (const device of [a, b]) {
        await device.sync();
        node_assert_1.strict.equal(device.device.epoch("room"), 3);
    }
    // The follow stream delivers live; b's persisted state restores and reads on.
    const waiting = b.nextEvent(5_000);
    await a.send("room", "live");
    const live = await waiting;
    node_assert_1.strict.ok(live);
    node_assert_1.strict.deepEqual(texts([live]), [["a-1", "live"]]);
    b.close();
    const restored = wasm.MlsDevice.restore(snapshots.get("b-1"));
    node_assert_1.strict.equal(restored.epoch("room"), 3);
    node_assert_1.strict.equal(restored.inboxCursor, b.device.inboxCursor);
    // A stale-epoch error is recognised from the typed detail only.
    const err = new generatedClient_1.UdbError("SubmitCommit", { code: grpc.status.FAILED_PRECONDITION, details: "x", message: "x", metadata: new grpc.Metadata() }, { missing: { current_epoch: "7" } });
    node_assert_1.strict.equal((0, messaging_1.staleEpoch)(err), 7);
    node_assert_1.strict.equal((0, messaging_1.staleEpoch)(new Error("FAILED_PRECONDITION current_epoch 7")), undefined);
});
(0, node_test_1.test)("an encrypted backup restores a device on a new machine", async (t) => {
    const wasm = wasmOrSkip();
    if (!wasm) {
        t.skip("the MLS WebAssembly module is not built (npm run build:mls-wasm)");
        return;
    }
    const broker = new FakeBroker(wasm);
    // Cheap Argon2id costs keep the test fast; the defaults are the recommended ones.
    const backupParams = { memoryKib: 256, iterations: 1, parallelism: 1 };
    const make = (subject, deviceId) => new messaging_1.MessagingClient({
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
    node_assert_1.strict.deepEqual(texts(await b.sync()), [["a-1", "before the backup"]]);
    const passphrase = "bob's recovery passphrase";
    const stored = await b.backup(passphrase, "bob's chat history");
    node_assert_1.strict.deepEqual([stored.backup_id, stored.version], ["default", "1"]);
    const held = broker.backups.get("bob/default").envelope;
    node_assert_1.strict.equal(held.subarray(0, 8).toString("latin1"), "UDBMLSB1");
    for (const needle of ["bob's chat history", passphrase, `${TENANT}/bob/b-1`]) {
        node_assert_1.strict.equal(held.includes(Buffer.from(needle)), false, `the broker copy must not contain ${needle}`);
    }
    // Bob's machine is lost; alice keeps talking.
    b.close();
    await a.send("room", "while bob was away");
    await node_assert_1.strict.rejects(messaging_1.MessagingClient.restore({ api: broker.api(), wasm, tenantId: TENANT, subjectId: "bob", recoverySecret: "not the passphrase" }), /does not open/);
    let persisted;
    const { client: restored, history } = await messaging_1.MessagingClient.restore({
        api: broker.api(),
        wasm,
        tenantId: TENANT,
        subjectId: "bob",
        recoverySecret: passphrase,
        persist: (snapshot) => {
            persisted = snapshot;
        },
    });
    node_assert_1.strict.equal(new TextDecoder().decode(history), "bob's chat history");
    node_assert_1.strict.ok(persisted && persisted.length > 0, "the restored state is persisted before use");
    node_assert_1.strict.equal(restored.device.deviceId, "b-1");
    node_assert_1.strict.deepEqual(texts(await restored.sync()), [["a-1", "while bob was away"]]);
    // The restored device follows a rotation and keeps decrypting, both ways.
    await a.updateKeys("room");
    await a.send("room", "after the restore");
    node_assert_1.strict.deepEqual(texts(await restored.sync()), [["a-1", "after the restore"]]);
    await restored.send("room", "bob is back");
    node_assert_1.strict.deepEqual(texts(await a.sync()), [["b-1", "bob is back"]]);
    node_assert_1.strict.equal(await restored.deleteBackup(), true);
    node_assert_1.strict.equal(await restored.deleteBackup(), false);
    await node_assert_1.strict.rejects(messaging_1.MessagingClient.restore({ api: broker.api(), wasm, tenantId: TENANT, subjectId: "bob", recoverySecret: passphrase }), (err) => err instanceof generatedClient_1.UdbError && err.code === grpc.status.NOT_FOUND);
});
(0, node_test_1.test)("pinned keys flag a swapped directory key and safety numbers match", async (t) => {
    const wasm = wasmOrSkip();
    if (!wasm) {
        t.skip("the MLS WebAssembly module is not built (npm run build:mls-wasm)");
        return;
    }
    const broker = new FakeBroker(wasm);
    const make = (subject, deviceId) => new messaging_1.MessagingClient({
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
    node_assert_1.strict.deepEqual(pinned.map((p) => [p.deviceId, p.keyFingerprint, p.verified]), [
        ["b-1", b.device.registration().keyFingerprint, false],
    ]);
    node_assert_1.strict.ok(pinned[0].firstSeenUnix > 0, "the JS host clock stamps pins");
    node_assert_1.strict.deepEqual(await a.takeKeyEvents(), []);
    await a.createGroup("room");
    await a.addMembers("room", ["bob"]);
    await b.sync();
    const number = await a.safetyNumber("bob");
    node_assert_1.strict.match(number, /^(\d{5} ){11}\d{5}$/);
    node_assert_1.strict.equal(await b.safetyNumber("alice"), number);
    await a.verifyDevice("bob", "b-1", b.device.registration().keyFingerprint);
    await node_assert_1.strict.rejects(a.verifyDevice("bob", "b-1", "0".repeat(64)), messaging_1.KeyChangedError);
    // A new device of a known subject is an event.
    const tablet = make("bob", "b-2");
    await tablet.registerDevice();
    await a.listDevices("bob");
    const events = await a.takeKeyEvents();
    node_assert_1.strict.deepEqual(events.map((e) => [e.kind, e.subjectId, e.deviceId, e.source]), [
        ["new_device", "bob", "b-2", "directory"],
    ]);
    const withTablet = await a.safetyNumber("bob");
    node_assert_1.strict.notEqual(withTablet, number);
    // A compromised directory swaps b-1's key: listing and adding fail closed.
    const impostor = make("bob", "b-1");
    await impostor.registerDevice();
    await impostor.ensureKeyPackages();
    const observed = impostor.device.registration().keyFingerprint;
    await node_assert_1.strict.rejects(a.listDevices("bob"), (err) => {
        node_assert_1.strict.ok(err instanceof messaging_1.KeyChangedError, String(err));
        node_assert_1.strict.equal(err.deviceId, "b-1");
        node_assert_1.strict.equal(err.pinnedFingerprint, b.device.registration().keyFingerprint);
        node_assert_1.strict.equal(err.observedFingerprint, observed);
        return true;
    });
    await a.createGroup("room-2");
    await node_assert_1.strict.rejects(a.addMembers("room-2", ["bob"]), messaging_1.KeyChangedError);
    node_assert_1.strict.equal(a.device.members("room-2").length, 1);
    node_assert_1.strict.equal(a.pinnedDevices("bob")[0].pendingFingerprint, observed);
    node_assert_1.strict.equal(await a.safetyNumber("bob").catch((err) => err instanceof messaging_1.KeyChangedError), true);
    // Only the compared key is accepted; afterwards the numbers differ.
    await node_assert_1.strict.rejects(a.acceptKeyChange("bob", "b-1", b.device.registration().keyFingerprint));
    await a.acceptKeyChange("bob", "b-1", observed);
    const after = await a.safetyNumber("bob");
    node_assert_1.strict.notEqual(after, withTablet);
    node_assert_1.strict.equal(a.pinnedDevices("bob").find((p) => p.deviceId === "b-1")?.verified, false);
});
