"use strict";
// End-to-end encrypted messaging over UDB's MessagingService.
//
// The broker is the MLS (RFC 9420) Delivery Service and key directory; it never
// holds a group secret and never sees plaintext. Every cryptographic operation
// runs in `udb-mls` — the same Rust crate the Rust SDK uses — compiled to
// WebAssembly (`crates/udb-mls-wasm`, built into `mls-wasm/` by
// `npm run build:mls-wasm`). This module drives the RPCs.
//
//   const mls = loadMlsWasm();
//   const device = mls.MlsDevice.create(tenantId, subjectId, "alice-laptop");
//   const chat = new MessagingClient({
//     api: new UdbGeneratedClient({ target: nativeTarget, meta, bearerToken }).MessagingService,
//     device,
//     persist: (snapshot) => fs.writeFileSync("alice-laptop.mls", snapshot, { mode: 0o600 }),
//   });
//   await chat.registerDevice();
//   await chat.ensureKeyPackages();
//   await chat.createGroup("room-1");
//   await chat.addMembers("room-1", ["bob"]);
//   await chat.send("room-1", "hello bob");
//   for (const event of await chat.sync()) if (event.kind === "message") ...
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
exports.MessagingClient = exports.KeyChangedError = exports.MessagingError = void 0;
exports.loadMlsWasm = loadMlsWasm;
exports.staleEpoch = staleEpoch;
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const grpc = __importStar(require("@grpc/grpc-js"));
const generatedClient_1 = require("./generatedClient");
const WASM_ENTRY = "udb_mls_wasm.js";
/** Load the WebAssembly MLS module: `modulePath`, else `$UDB_MLS_WASM`, else the
 *  `mls-wasm/` directory shipped next to this package's compiled output. */
function loadMlsWasm(modulePath) {
    const candidates = [
        modulePath,
        process.env.UDB_MLS_WASM,
        path.resolve(__dirname, "mls-wasm", WASM_ENTRY),
        path.resolve(__dirname, "..", "mls-wasm", WASM_ENTRY),
    ].filter((candidate) => Boolean(candidate && candidate.trim()));
    for (const candidate of candidates) {
        const file = fs.existsSync(candidate) && fs.statSync(candidate).isDirectory()
            ? path.join(candidate, WASM_ENTRY)
            : candidate;
        if (fs.existsSync(file)) {
            // eslint-disable-next-line @typescript-eslint/no-require-imports
            return require(file);
        }
    }
    throw new Error("udb: the MLS WebAssembly module is not built; run `npm run build:mls-wasm` " +
        `(looked in: ${candidates.join(", ")})`);
}
// ── the client ─────────────────────────────────────────────────────────────
/** How often a commit is rebuilt after losing an epoch race (or a send
 *  re-sealed after the group moved on) before giving up. */
const MAX_REBASES = 5;
/** Re-sends of a mutation with the SAME idempotency key after a transport failure. */
const MAX_TRANSPORT_RETRIES = 3;
const TRANSIENT = new Set([
    grpc.status.UNAVAILABLE,
    grpc.status.DEADLINE_EXCEEDED,
    grpc.status.RESOURCE_EXHAUSTED,
    grpc.status.ABORTED,
]);
/** The group epoch a stale-epoch refusal reports (FAILED_PRECONDITION with
 *  `ErrorDetail.missing.current_epoch`), if `err` is one. */
function staleEpoch(err) {
    if (!(err instanceof generatedClient_1.UdbError) || err.code !== grpc.status.FAILED_PRECONDITION)
        return undefined;
    const current = err.detail?.missing?.current_epoch;
    if (current === undefined || current === "")
        return undefined;
    const epoch = Number(current);
    return Number.isFinite(epoch) ? epoch : undefined;
}
function isTransient(err) {
    return err instanceof generatedClient_1.UdbError && TRANSIENT.has(err.code);
}
class MessagingError extends Error {
    constructor(message) {
        super(`udb messaging: ${message}`);
        this.name = "MessagingError";
    }
}
exports.MessagingError = MessagingError;
/** A pinned device presented a different signature key (from the directory or
 *  a claimed KeyPackage). The pin is unchanged; compare `observedFingerprint`
 *  with the device's owner out of band, then `acceptKeyChange` it or stop. */
class KeyChangedError extends MessagingError {
    subjectId;
    deviceId;
    pinnedFingerprint;
    observedFingerprint;
    constructor(subjectId, deviceId, pinnedFingerprint, observedFingerprint) {
        super(`the key of device '${deviceId}' of '${subjectId}' changed: pinned ${pinnedFingerprint}, observed ${observedFingerprint}`);
        this.subjectId = subjectId;
        this.deviceId = deviceId;
        this.pinnedFingerprint = pinnedFingerprint;
        this.observedFingerprint = observedFingerprint;
        this.name = "KeyChangedError";
    }
}
exports.KeyChangedError = KeyChangedError;
const KEY_CHANGED_PREFIX = "key_changed:";
/** The WebAssembly module reports a changed key as `key_changed:{json}`; turn
 *  that into a `KeyChangedError`, anything else is returned unchanged. */
function keyChangedOr(err) {
    const message = err?.message;
    if (typeof message !== "string" || !message.startsWith(KEY_CHANGED_PREFIX))
        return err;
    try {
        const detail = JSON.parse(message.slice(KEY_CHANGED_PREFIX.length));
        return new KeyChangedError(String(detail.subjectId ?? ""), String(detail.deviceId ?? ""), String(detail.pinnedFingerprint ?? ""), String(detail.observedFingerprint ?? ""));
    }
    catch {
        return err;
    }
}
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
function utf8(value) {
    return typeof value === "string" ? new TextEncoder().encode(value) : value;
}
function bytes(value) {
    if (!value)
        return Buffer.alloc(0);
    return Buffer.isBuffer(value) ? value : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
}
function streamError(err) {
    return err?.udb ?? err;
}
/** One device's messaging session over the MessagingService. Every inbox entry
 *  is processed — and the state persisted — before it is acknowledged. */
class MessagingClient {
    api;
    device;
    wasm;
    persistHook;
    target;
    threshold;
    call;
    backupParams;
    backlog = [];
    acked = 0;
    ids = 0;
    follow;
    followQueue = [];
    followError;
    followWaiters = [];
    followChain = Promise.resolve();
    constructor(options) {
        this.api = options.api;
        this.device = options.device;
        this.wasm = options.wasm;
        this.persistHook = options.persist;
        this.target = Math.max(1, options.keyPackageTarget ?? 20);
        this.threshold = Math.min(options.keyPackageThreshold ?? 5, this.target);
        this.call = options.call;
        this.backupParams = options.backupParams;
    }
    get tenantId() {
        return this.device.tenantId;
    }
    get deviceId() {
        return this.device.deviceId;
    }
    freshId(prefix) {
        this.ids += 1;
        const random = Math.floor(Math.random() * 0xffffffff).toString(16);
        return `${prefix}-${Date.now().toString(16)}-${this.ids.toString(16)}-${random}`;
    }
    async persist() {
        if (this.persistHook)
            await this.persistHook(this.device.snapshot());
    }
    /** A mutation re-sent with the same idempotency key on transport failures. */
    async withTransportRetry(send) {
        for (let attempt = 0;; attempt += 1) {
            try {
                return await send();
            }
            catch (err) {
                if (!isTransient(err) || attempt >= MAX_TRANSPORT_RETRIES)
                    throw err;
                await sleep(100 * (attempt + 1));
            }
        }
    }
    // ── device directory ─────────────────────────────────────────────────────
    /** Register this device's signature key and credential (idempotent). */
    async registerDevice() {
        const registration = this.device.registration();
        const response = await this.api.register_messaging_device({
            tenant_id: registration.tenantId,
            subject_id: registration.subjectId,
            device_id: registration.deviceId,
            signature_public_key: bytes(registration.signaturePublicKey),
            credential: bytes(registration.credential),
        }, this.call);
        const device = response.device;
        if (!device)
            throw new MessagingError("registration returned no device");
        if (device.key_fingerprint !== registration.keyFingerprint) {
            throw new MessagingError(`the directory holds key ${device.key_fingerprint} for this device, not ${registration.keyFingerprint}`);
        }
        return device;
    }
    /** A principal's devices with key fingerprints ("" = this device's
     *  principal). Every key is checked against this device's pins: the first key
     *  seen for a device is pinned, an unseen device of a known subject (or of
     *  this device's own) is queued as a `new_device` key event (`takeKeyEvents`),
     *  and a key that differs from its pin throws `KeyChangedError`. */
    async listDevices(subjectId = "", includeRevoked = false) {
        const devices = [];
        let pageToken = "";
        do {
            const response = await this.api.list_messaging_devices({ tenant_id: this.tenantId, subject_id: subjectId, include_revoked: includeRevoked, page_token: pageToken }, this.call);
            devices.push(...(response.devices ?? []));
            pageToken = String(response.next_page_token ?? "");
        } while (pageToken);
        try {
            this.device.observeDeviceKeys(devices.map((device) => ({
                subjectId: String(device.subject_id ?? ""),
                deviceId: String(device.device_id ?? ""),
                signaturePublicKey: new Uint8Array(device.signature_public_key ?? []),
            })), "directory");
        }
        catch (err) {
            // Every key was recorded (a changed one as pending) before the throw.
            await this.persist();
            throw keyChangedOr(err);
        }
        await this.persist();
        return devices;
    }
    // ── key pinning ──────────────────────────────────────────────────────────
    /** Key events (new devices; changed keys seen in a Welcome or another
     *  member's commit, which the group already accepted) since the last call. */
    async takeKeyEvents() {
        const events = this.device.takeKeyEvents();
        if (events.length)
            await this.persist();
        return events;
    }
    /** The safety number to compare with `peerSubjectId` out of band, after
     *  refreshing both subjects' devices (a changed pinned key throws
     *  `KeyChangedError`). */
    async safetyNumber(peerSubjectId) {
        const own = this.device.subjectId;
        await this.listDevices(own, false);
        if (peerSubjectId !== own)
            await this.listDevices(peerSubjectId, false);
        try {
            return this.device.safetyNumber(peerSubjectId);
        }
        catch (err) {
            throw keyChangedOr(err);
        }
    }
    /** Mark a pinned device verified after its owner read `fingerprint` out of
     *  band; a different fingerprint throws `KeyChangedError`. */
    async verifyDevice(subjectId, deviceId, fingerprint) {
        try {
            this.device.verifyDevice(subjectId, deviceId, fingerprint);
        }
        catch (err) {
            throw keyChangedOr(err);
        }
        await this.persist();
    }
    /** Re-pin a device to the key a `KeyChangedError` reported, after comparing
     *  its `observedFingerprint` with the device's owner. Only that key is accepted. */
    async acceptKeyChange(subjectId, deviceId, fingerprint) {
        this.device.acceptKeyChange(subjectId, deviceId, fingerprint);
        await this.persist();
    }
    /** A subject's pinned devices, sorted by device id. */
    pinnedDevices(subjectId) {
        return this.device.pinnedDevices(subjectId);
    }
    async revokeDevice(deviceId, reason = "") {
        return this.api.revoke_messaging_device({ tenant_id: this.tenantId, device_id: deviceId, reason }, this.call);
    }
    // ── key packages ─────────────────────────────────────────────────────────
    /** Top the one-time KeyPackage stock up to the target when it is below the
     *  threshold, and publish a last-resort package if the broker has none. */
    async ensureKeyPackages() {
        const stock = await this.api.get_key_package_stock({ tenant_id: this.tenantId, device_id: this.deviceId }, this.call);
        const remaining = Math.max(0, Number(stock.one_time_remaining ?? 0));
        const topUp = remaining < this.threshold ? this.target - remaining : 0;
        if (topUp === 0 && stock.has_last_resort) {
            return { oneTimeRemaining: remaining, hasLastResort: true, published: 0 };
        }
        const keyPackages = this.device.keyPackages(topUp).map(bytes);
        const lastResort = stock.has_last_resort ? Buffer.alloc(0) : bytes(this.device.lastResortKeyPackage());
        await this.persist();
        const published = await this.api.publish_key_packages({
            tenant_id: this.tenantId,
            device_id: this.deviceId,
            key_packages: keyPackages,
            last_resort_key_package: lastResort,
        }, this.call);
        return {
            oneTimeRemaining: Number(published.one_time_remaining ?? 0),
            hasLastResort: Boolean(published.has_last_resort),
            published: Number(published.accepted ?? 0),
        };
    }
    // ── encrypted backup ─────────────────────────────────────────────────────
    /** Seal this device's state and the application's `history` under a key
     *  derived from `recoverySecret` and store the envelope as the principal's
     *  backup `backupId` ("" = "default"), replacing an earlier one. The broker
     *  stores only the envelope. Back up after processing the inbox: a restore
     *  resumes from the cursor saved here. */
    async backup(recoverySecret, history = "", backupId = "") {
        const secret = utf8(recoverySecret);
        const plain = utf8(history);
        const envelope = this.backupParams
            ? this.device.exportBackupWith(secret, plain, this.backupParams.memoryKib, this.backupParams.iterations, this.backupParams.parallelism)
            : this.device.exportBackup(secret, plain);
        const response = await this.api.put_messaging_backup({
            tenant_id: this.tenantId,
            subject_id: this.device.subjectId,
            backup_id: backupId,
            ciphertext: bytes(envelope),
            expected_version: 0,
        }, this.call);
        if (!response.backup)
            throw new MessagingError("the backup write returned no backup");
        return response.backup;
    }
    /** Erase the principal's backup `backupId` ("" = "default"); false when none was stored. */
    async deleteBackup(backupId = "") {
        const response = await this.api.delete_messaging_backup({ tenant_id: this.tenantId, subject_id: this.device.subjectId, backup_id: backupId }, this.call);
        return Boolean(response.deleted);
    }
    /** Restore a device on a new machine: fetch the backup, open it with the
     *  recovery secret, and load the backed-up device (its identity, keys,
     *  groups and inbox cursor). The snapshot is handed to `persist` before this
     *  returns. Also returns the history the backup carried. */
    static async restore(options) {
        const stored = await options.api.get_messaging_backup({ tenant_id: options.tenantId, subject_id: options.subjectId ?? "", backup_id: options.backupId ?? "" }, options.call);
        const wasm = options.wasm ?? loadMlsWasm();
        const opened = wasm.openBackup(utf8(options.recoverySecret), new Uint8Array(stored.ciphertext ?? []));
        const device = wasm.MlsDevice.restore(opened.snapshot);
        if (device.tenantId !== options.tenantId) {
            throw new MessagingError(`the backup is of tenant ${device.tenantId}, not ${options.tenantId}`);
        }
        const client = new MessagingClient({ ...options, device, wasm });
        await client.persist();
        return { client, history: opened.history };
    }
    // ── groups ───────────────────────────────────────────────────────────────
    /** Create a group with this device as its only member. Returns its epoch. */
    async createGroup(groupId) {
        const creation = this.device.createGroup(groupId);
        await this.persist();
        try {
            const response = await this.api.create_group({
                tenant_id: this.tenantId,
                group_id: groupId,
                creator_device_id: this.deviceId,
                group_info: bytes(creation.groupInfo),
                ratchet_tree: bytes(creation.ratchetTree),
            }, this.call);
            return Number(response.epoch ?? 0);
        }
        catch (err) {
            this.device.forgetGroup(groupId);
            await this.persist();
            throw err;
        }
    }
    /** Add every active device of `subjectIds` plus `deviceIds`, claiming one
     *  KeyPackage per device; members (and this device) are skipped. */
    async addMembers(groupId, subjectIds, deviceIds = []) {
        const members = new Set(this.device.members(groupId));
        const targets = new Set(deviceIds);
        for (const subject of subjectIds) {
            for (const device of await this.listDevices(subject, false)) {
                if (device.status === "ACTIVE")
                    targets.add(device.device_id);
            }
        }
        targets.delete(this.deviceId);
        for (const member of members)
            targets.delete(member);
        if (targets.size === 0) {
            return this.noop(groupId, 0);
        }
        const claimed = await this.api.claim_key_packages({
            tenant_id: this.tenantId,
            requester_device_id: this.deviceId,
            target_device_ids: [...targets].sort(),
            target_subject_ids: [],
        }, this.call);
        const wasm = this.inspector();
        const keyPackages = [];
        for (const claim of claimed.key_packages ?? []) {
            const info = wasm(claim.key_package);
            if (info.tenantId !== this.tenantId || info.subjectId !== claim.subject_id || info.deviceId !== claim.device_id) {
                throw new MessagingError(`the KeyPackage claimed for device '${claim.device_id}' names '${info.tenantId}/${info.subjectId}/${info.deviceId}'`);
            }
            keyPackages.push(new Uint8Array(claim.key_package));
        }
        const outcome = await this.submit(groupId, () => this.device.commitAdd(groupId, keyPackages));
        outcome.missingDeviceIds = [...(claimed.missing_device_ids ?? [])];
        return outcome;
    }
    /** Remove member devices (for example after a `removal_hint` event). */
    async removeDevices(groupId, deviceIds) {
        return this.submit(groupId, () => this.device.commitRemove(groupId, deviceIds));
    }
    /** Rotate this device's leaf keys in the group (post-compromise security). */
    async updateKeys(groupId) {
        return this.submit(groupId, () => this.device.commitUpdate(groupId));
    }
    inspectorFn;
    /** The KeyPackage inspector of the module the device came from. */
    inspector() {
        if (!this.inspectorFn) {
            const module = this.wasm ?? loadMlsWasm();
            this.inspectorFn = (value) => module.inspectKeyPackage(new Uint8Array(value));
        }
        return this.inspectorFn;
    }
    noop(groupId, rebases) {
        return {
            epoch: this.device.epoch(groupId),
            addedDeviceIds: [],
            removedDeviceIds: [],
            missingDeviceIds: [],
            rebases,
            committed: false,
        };
    }
    /** Build, submit and resolve a commit; re-base it when another member's
     *  commit won the epoch. */
    async submit(groupId, build) {
        let rebases = 0;
        let next = build;
        for (;;) {
            let built;
            try {
                built = next();
            }
            catch (err) {
                // A claimed KeyPackage with a changed key: nothing was built, the
                // observed key is pending.
                await this.persist();
                throw keyChangedOr(err);
            }
            const pending = built;
            if (!pending) {
                await this.persist();
                return this.noop(groupId, rebases);
            }
            await this.persist();
            const clientMessageId = this.freshId("commit");
            try {
                await this.withTransportRetry(() => this.api.submit_commit({
                    tenant_id: this.tenantId,
                    group_id: groupId,
                    sender_device_id: this.deviceId,
                    epoch: String(pending.epoch),
                    commit: bytes(pending.commit),
                    welcome: bytes(pending.welcome),
                    group_info: bytes(pending.groupInfo),
                    client_message_id: clientMessageId,
                }, { ...(this.call ?? {}), noRetry: true }));
            }
            catch (err) {
                const current = staleEpoch(err);
                if (current !== undefined && rebases < MAX_REBASES) {
                    rebases += 1;
                    const kept = this.device.discardCommit(groupId);
                    await this.persist();
                    await this.catchUp(groupId, current);
                    next = kept ? () => this.device.recommit(groupId) : build;
                    continue;
                }
                if (!isTransient(err)) {
                    // A definite refusal: the commit never lands.
                    this.device.discardCommit(groupId);
                    await this.persist();
                }
                throw err;
            }
            // A duplicate response means an earlier attempt landed; its inbox echo
            // may already have merged the commit.
            const epoch = this.device.hasPendingCommit(groupId)
                ? this.device.confirmCommit(groupId)
                : this.device.epoch(groupId);
            await this.persist();
            return {
                epoch,
                addedDeviceIds: [...pending.addedDeviceIds],
                removedDeviceIds: [...pending.removedDeviceIds],
                missingDeviceIds: [],
                rebases,
                committed: true,
            };
        }
    }
    // ── messages ─────────────────────────────────────────────────────────────
    /** Encrypt and send an application message. If the group moved to a new
     *  epoch first, the inbox is processed and the message re-sealed. */
    async send(groupId, plaintext) {
        const data = typeof plaintext === "string" ? new TextEncoder().encode(plaintext) : plaintext;
        const clientMessageId = this.freshId("msg");
        for (let rebases = 0;; rebases += 1) {
            const sealed = this.device.encrypt(groupId, data);
            await this.persist();
            try {
                return await this.withTransportRetry(() => this.api.send_message({
                    tenant_id: this.tenantId,
                    group_id: groupId,
                    sender_device_id: this.deviceId,
                    ciphertext: bytes(sealed.ciphertext),
                    client_message_id: clientMessageId,
                }, { ...(this.call ?? {}), noRetry: true }));
            }
            catch (err) {
                const current = staleEpoch(err);
                if (current === undefined || rebases >= MAX_REBASES)
                    throw err;
                await this.catchUp(groupId, current);
            }
        }
    }
    // ── inbox ────────────────────────────────────────────────────────────────
    openInbox(follow) {
        return this.api.stream_inbox({
            tenant_id: this.tenantId,
            device_id: this.deviceId,
            after_seq: String(this.device.inboxCursor),
            follow,
            max_entries: 0,
        }, this.call);
    }
    /** Process one delivered entry; the event unless it needs no action. */
    handle(response) {
        if (response?.error && response.error.message) {
            throw new MessagingError(`inbox stream reported an error: ${response.error.message}`);
        }
        const entry = response?.entry;
        if (!entry)
            return undefined;
        const event = this.device.process(Number(entry.seq ?? 0), String(entry.kind ?? ""), String(entry.group_id ?? ""), Number(entry.epoch ?? 0), new Uint8Array(entry.payload ?? []), String(entry.sender_device_id ?? ""), String(entry.client_message_id ?? ""));
        return event.kind === "skipped" ? undefined : event;
    }
    /** Acknowledge every processed entry to the broker. */
    async ack() {
        const cursor = this.device.inboxCursor;
        if (cursor <= this.acked)
            return;
        const response = await this.api.ack_inbox({ tenant_id: this.tenantId, device_id: this.deviceId, up_to_seq: String(cursor) }, this.call);
        this.acked = Math.max(Number(response.acked_seq ?? 0), cursor);
    }
    async drainIntoBacklog() {
        const stream = this.openInbox(false);
        try {
            for await (const response of stream) {
                const event = this.handle(response);
                if (event)
                    this.backlog.push(event);
            }
        }
        catch (err) {
            throw streamError(err);
        }
        await this.persist();
        await this.ack();
    }
    /** Process the inbox until the group reaches `epoch` (a winning commit). */
    async catchUp(groupId, epoch) {
        for (let attempt = 0; attempt < 3; attempt += 1) {
            await this.drainIntoBacklog();
            if (this.device.epoch(groupId) >= epoch)
                return;
            await sleep(100);
        }
        throw new MessagingError(`group '${groupId}' is at epoch ${this.device.epoch(groupId)} after processing the inbox; the broker reports ${epoch}`);
    }
    /** Process everything currently in the inbox and return the events (plus any
     *  processed while re-basing). Does not wait for new entries. */
    async sync() {
        await this.drainIntoBacklog();
        const events = this.backlog;
        this.backlog = [];
        return events;
    }
    wake() {
        const waiters = this.followWaiters;
        this.followWaiters = [];
        for (const waiter of waiters)
            waiter();
    }
    ensureFollow() {
        if (this.follow)
            return;
        const stream = this.openInbox(true);
        this.follow = stream;
        stream.on("data", (response) => {
            // Processing is synchronous and in order; persistence and the ack are
            // chained so an entry is acknowledged only after its state was saved.
            try {
                const event = this.handle(response);
                this.followChain = this.followChain
                    .then(() => this.persist())
                    .then(() => this.ack())
                    .then(() => {
                    if (event)
                        this.followQueue.push(event);
                    this.wake();
                })
                    .catch((err) => {
                    this.followError = err;
                    this.wake();
                });
            }
            catch (err) {
                this.followError = err;
                this.wake();
            }
        });
        stream.on("error", (err) => {
            if (err?.code !== grpc.status.CANCELLED)
                this.followError = streamError(err);
            this.follow = undefined;
            this.wake();
        });
        stream.on("end", () => {
            this.follow = undefined;
            this.wake();
        });
    }
    /** The next inbox event from a follow stream, waiting up to `timeoutMs`.
     *  `undefined` on timeout. */
    async nextEvent(timeoutMs = 30_000) {
        const deadline = Date.now() + timeoutMs;
        for (;;) {
            const queued = this.backlog.shift() ?? this.followQueue.shift();
            if (queued)
                return queued;
            if (this.followError !== undefined) {
                const err = this.followError;
                this.followError = undefined;
                throw err;
            }
            this.ensureFollow();
            const remaining = deadline - Date.now();
            if (remaining <= 0)
                return undefined;
            await new Promise((resolve) => {
                const timer = setTimeout(resolve, remaining);
                this.followWaiters.push(() => {
                    clearTimeout(timer);
                    resolve();
                });
            });
        }
    }
    /** Close the follow stream, if one is open. */
    close() {
        this.follow?.cancel();
        this.follow = undefined;
    }
}
exports.MessagingClient = MessagingClient;
