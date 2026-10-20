"use strict";
// Rust <-> TypeScript end-to-end encrypted group chat through a real broker.
//
// A TypeScript device (MessagingClient over the WebAssembly MLS core) and a Rust
// device (the Rust SDK's `messaging_peer` example, one command per process, its
// state in a file) share one MLS group: the TypeScript device creates it and
// adds the Rust device by a claimed KeyPackage, the Rust device joins from the
// Welcome, both directions decrypt, and each side's key rotation is applied by
// the other. Each side then restores the other's encrypted backup on a new
// machine and reads on.
//
// Gated on UDB_LIVE_SDK_TESTS=1. Needs UDB_AUTH_GRPC_TARGET, UDB_LIVE_USERNAME,
// UDB_LIVE_PASSWORD, UDB_LIVE_TENANT, UDB_RUST_MESSAGING_PEER (the built
// `messaging_peer` binary) and the WebAssembly module (`npm run build:mls-wasm`).
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
const node_child_process_1 = require("node:child_process");
const node_fs_1 = require("node:fs");
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const generatedClient_1 = require("./generatedClient");
const messaging_1 = require("./messaging");
function requiredEnv(name) {
    const value = process.env[name]?.trim();
    if (!value)
        throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
    return value;
}
function peer(binary, state, ...args) {
    const out = (0, node_child_process_1.execFileSync)(binary, [state, ...args], { env: process.env, encoding: "utf8", timeout: 60_000 });
    return out
        .split("\n")
        .map((line) => line.trim())
        .filter(Boolean)
        .map((line) => JSON.parse(line));
}
function texts(events) {
    return events
        .filter((e) => e.kind === "message")
        .map((e) => [e.deviceId, new TextDecoder().decode(e.plaintext)]);
}
(0, node_test_1.test)("Rust and TypeScript devices share an end-to-end encrypted group", async (t) => {
    if (process.env.UDB_LIVE_SDK_TESTS !== "1") {
        t.skip("the live messaging interop test needs UDB_LIVE_SDK_TESTS=1");
        return;
    }
    const rustPeer = requiredEnv("UDB_RUST_MESSAGING_PEER");
    const target = requiredEnv("UDB_AUTH_GRPC_TARGET");
    const tenantHint = requiredEnv("UDB_LIVE_TENANT");
    const projectId = process.env.UDB_LIVE_PROJECT?.trim() || "default";
    const wasm = (0, messaging_1.loadMlsWasm)();
    const meta = { tenantId: tenantHint, projectId, purpose: "ts.live.messaging", correlationId: "" };
    const anonymous = new generatedClient_1.UdbGeneratedClient({ target, meta });
    const login = await anonymous.AuthnService.login({
        username: requiredEnv("UDB_LIVE_USERNAME"),
        password: requiredEnv("UDB_LIVE_PASSWORD"),
        tenant_hint: tenantHint,
        project_hint: projectId,
        device_name: "ts-live-messaging",
    });
    const who = await anonymous.AuthnService.authenticate({
        bearer_token: login.access_token,
        tenant_hint: tenantHint,
        project_hint: projectId,
    });
    anonymous.core.close();
    const tenantId = who.principal.tenant_id;
    const subject = who.principal.subject;
    const generated = new generatedClient_1.UdbGeneratedClient({
        target,
        meta: { ...meta, tenantId },
        bearerToken: login.access_token,
    });
    const run = Date.now().toString(16);
    const group = `interop-${run}`;
    const tsDeviceId = `ts-${run}`;
    const dir = (0, node_fs_1.mkdtempSync)(path.join(os.tmpdir(), "udb-mls-interop-"));
    const state = path.join(dir, "rust-peer.mls");
    const chat = new messaging_1.MessagingClient({
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
        const rustDeviceId = rust.device_id;
        node_assert_1.strict.ok(rust.published >= 1, "the Rust device published KeyPackages");
        // TypeScript creates the group and adds the Rust device.
        node_assert_1.strict.equal(await chat.createGroup(group), 0);
        const added = await chat.addMembers(group, [], [rustDeviceId]);
        node_assert_1.strict.deepEqual([added.epoch, added.addedDeviceIds], [1, [rustDeviceId]]);
        const joined = peer(rustPeer, state, "sync");
        node_assert_1.strict.ok(joined.some((e) => e.kind === "joined" && e.group_id === group && e.members.includes(tsDeviceId)), JSON.stringify(joined));
        // TypeScript -> Rust.
        await chat.send(group, "hello from typescript");
        node_assert_1.strict.deepEqual(peer(rustPeer, state, "sync").filter((e) => e.kind === "message").map((e) => [e.device_id, e.text]), [[tsDeviceId, "hello from typescript"]]);
        // Rust -> TypeScript.
        peer(rustPeer, state, "send", group, "hello from rust");
        node_assert_1.strict.deepEqual(texts(await chat.sync()), [[rustDeviceId, "hello from rust"]]);
        // Rust rotates its keys; TypeScript applies the Rust-built commit.
        const rotated = peer(rustPeer, state, "update", group)[0];
        node_assert_1.strict.equal(rotated.epoch, 2);
        const tsEvents = await chat.sync();
        node_assert_1.strict.ok(tsEvents.some((e) => e.kind === "commit" && e.epoch === 2 && e.deviceId === rustDeviceId), JSON.stringify(tsEvents));
        // TypeScript rotates; Rust applies the WebAssembly-built commit and reads on.
        node_assert_1.strict.equal((await chat.updateKeys(group)).epoch, 3);
        await chat.send(group, "after both rotations");
        const rustEvents = peer(rustPeer, state, "sync");
        node_assert_1.strict.ok(rustEvents.some((e) => e.kind === "commit" && e.epoch === 3 && e.device_id === tsDeviceId), JSON.stringify(rustEvents));
        node_assert_1.strict.deepEqual(rustEvents.filter((e) => e.kind === "message").map((e) => [e.device_id, e.text]), [[tsDeviceId, "after both rotations"]]);
        // The follow stream: TypeScript waits, Rust sends.
        await chat.sync();
        const first = chat.nextEvent(15_000);
        peer(rustPeer, state, "send", group, "live from rust");
        let live = await first;
        for (let i = 0; live && live.kind !== "message" && i < 5; i += 1)
            live = await chat.nextEvent(15_000);
        node_assert_1.strict.ok(live, "a live event arrived");
        node_assert_1.strict.deepEqual(texts([live]), [[rustDeviceId, "live from rust"]]);
        // Encrypted backup across the two implementations. The Rust device seals
        // its state; a fresh TypeScript (WebAssembly) client restores it on a "new
        // machine" and reads what arrived while the Rust device was away.
        chat.close();
        peer(rustPeer, state, "sync");
        const rustBackupId = `interop-rs-${run}`;
        const rustSecret = "interop rust recovery passphrase";
        const [sealed] = peer(rustPeer, state, "backup", rustBackupId, rustSecret);
        node_assert_1.strict.equal(sealed.version, 1);
        await chat.send(group, "while rust was away");
        const { client: rustOnTs, history } = await messaging_1.MessagingClient.restore({
            api: generated.MessagingService,
            wasm,
            tenantId,
            backupId: rustBackupId,
            recoverySecret: rustSecret,
        });
        node_assert_1.strict.equal(new TextDecoder().decode(history), "rust peer history");
        node_assert_1.strict.equal(rustOnTs.device.deviceId, rustDeviceId);
        node_assert_1.strict.deepEqual(texts(await rustOnTs.sync()), [[tsDeviceId, "while rust was away"]]);
        // The other way: TypeScript seals; the Rust peer restores it into a new
        // state file and decrypts what the restored Rust device sent meanwhile.
        await chat.sync();
        const tsBackupId = `interop-ts-${run}`;
        const tsSecret = "interop typescript recovery passphrase";
        await chat.backup(tsSecret, "typescript history", tsBackupId);
        await rustOnTs.send(group, "from the restored rust device");
        const [tsOnRust] = peer(rustPeer, path.join(dir, "ts-restored.mls"), "restore", tsBackupId, tsSecret);
        node_assert_1.strict.equal(tsOnRust.device_id, tsDeviceId);
        node_assert_1.strict.equal(tsOnRust.history, "typescript history");
        node_assert_1.strict.deepEqual(tsOnRust.events.filter((e) => e.kind === "message").map((e) => [e.device_id, e.text]), [[rustDeviceId, "from the restored rust device"]]);
        node_assert_1.strict.equal(await rustOnTs.deleteBackup(rustBackupId), true);
        node_assert_1.strict.equal(await chat.deleteBackup(tsBackupId), true);
    }
    finally {
        chat.close();
        try {
            peer(rustPeer, state, "revoke");
            await chat.revokeDevice(tsDeviceId, "interop test cleanup");
        }
        finally {
            generated.core.close();
            (0, node_fs_1.rmSync)(dir, { recursive: true, force: true });
        }
    }
});
