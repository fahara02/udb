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

import * as fs from "fs";
import * as path from "path";
import * as grpc from "@grpc/grpc-js";

import type { CallOptions, MessagingServiceApi } from "./generatedClient";
import { UdbError } from "./generatedClient";

// ── the WebAssembly module ─────────────────────────────────────────────────

/** A commit to submit with SubmitCommit; confirm or discard it after. */
export interface MlsCommit {
  readonly groupId: string;
  /** The epoch the commit was built on. */
  readonly epoch: number;
  readonly commit: Uint8Array;
  /** Empty when the commit adds nobody. */
  readonly welcome: Uint8Array;
  readonly addedDeviceIds: string[];
  readonly removedDeviceIds: string[];
}

export type MlsEventKind =
  | "joined"
  | "commit"
  | "own_commit"
  | "removed"
  | "message"
  | "removal_hint"
  | "skipped"
  | "undecryptable";

/** The outcome of processing one inbox entry. Fields that do not apply to a
 *  kind are empty. `deviceId` is the committer, the device that added this one,
 *  the message sender, or the revoked device (removal hint). */
export interface MlsEvent {
  readonly kind: MlsEventKind;
  readonly seq: number;
  readonly groupId: string;
  readonly epoch: number;
  readonly deviceId: string;
  readonly senderSubjectId: string;
  readonly clientMessageId: string;
  readonly plaintext: Uint8Array;
  readonly members: string[];
  readonly added: string[];
  readonly removed: string[];
  readonly merged: boolean;
  readonly reason: string;
}

/** One device's MLS state inside the WebAssembly module. */
export interface MlsDevice {
  readonly tenantId: string;
  readonly subjectId: string;
  readonly deviceId: string;
  readonly inboxCursor: number;
  /** The complete device state (SECRET: private keys). */
  snapshot(): Uint8Array;
  /** Seal the state and `history` as an encrypted backup envelope under a key
   *  derived from `recoverySecret` (Argon2id, HKDF-SHA256, XChaCha20-Poly1305). */
  exportBackup(recoverySecret: Uint8Array, history: Uint8Array): Uint8Array;
  /** `exportBackup` with explicit Argon2id costs (memory in KiB). */
  exportBackupWith(
    recoverySecret: Uint8Array,
    history: Uint8Array,
    memoryKib: number,
    iterations: number,
    parallelism: number,
  ): Uint8Array;
  registration(): {
    tenantId: string;
    subjectId: string;
    deviceId: string;
    signaturePublicKey: Uint8Array;
    credential: Uint8Array;
    keyFingerprint: string;
  };
  keyPackages(count: number): Uint8Array[];
  lastResortKeyPackage(): Uint8Array;
  createGroup(groupId: string): { epoch: number; groupInfo: Uint8Array; ratchetTree: Uint8Array };
  forgetGroup(groupId: string): void;
  groupIds(): string[];
  epoch(groupId: string): number;
  members(groupId: string): string[];
  hasPendingCommit(groupId: string): boolean;
  commitAdd(groupId: string, keyPackages: Uint8Array[]): MlsCommit | undefined;
  commitRemove(groupId: string, deviceIds: string[]): MlsCommit | undefined;
  commitUpdate(groupId: string): MlsCommit | undefined;
  confirmCommit(groupId: string): number;
  discardCommit(groupId: string): boolean;
  recommit(groupId: string): MlsCommit | undefined;
  encrypt(groupId: string, plaintext: Uint8Array): { epoch: number; ciphertext: Uint8Array };
  process(
    seq: number,
    kind: string,
    groupId: string,
    epoch: number,
    payload: Uint8Array,
    senderDeviceId: string,
    clientMessageId: string,
  ): MlsEvent;
}

/** The `udb-mls-wasm` module (wasm-bindgen, Node target). */
export interface MlsWasmModule {
  MlsDevice: {
    create(tenantId: string, subjectId: string, deviceId: string): MlsDevice;
    restore(snapshot: Uint8Array): MlsDevice;
  };
  inspectKeyPackage(bytes: Uint8Array): {
    tenantId: string;
    subjectId: string;
    deviceId: string;
    lastResort: boolean;
  };
  keyFingerprint(signaturePublicKey: Uint8Array): string;
  /** Open a backup envelope; a wrong secret or a modified envelope throws. */
  openBackup(recoverySecret: Uint8Array, envelope: Uint8Array): { snapshot: Uint8Array; history: Uint8Array };
}

const WASM_ENTRY = "udb_mls_wasm.js";

/** Load the WebAssembly MLS module: `modulePath`, else `$UDB_MLS_WASM`, else the
 *  `mls-wasm/` directory shipped next to this package's compiled output. */
export function loadMlsWasm(modulePath?: string): MlsWasmModule {
  const candidates = [
    modulePath,
    process.env.UDB_MLS_WASM,
    path.resolve(__dirname, "mls-wasm", WASM_ENTRY),
    path.resolve(__dirname, "..", "mls-wasm", WASM_ENTRY),
  ].filter((candidate): candidate is string => Boolean(candidate && candidate.trim()));
  for (const candidate of candidates) {
    const file = fs.existsSync(candidate) && fs.statSync(candidate).isDirectory()
      ? path.join(candidate, WASM_ENTRY)
      : candidate;
    if (fs.existsSync(file)) {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      return require(file) as MlsWasmModule;
    }
  }
  throw new Error(
    "udb: the MLS WebAssembly module is not built; run `npm run build:mls-wasm` " +
      `(looked in: ${candidates.join(", ")})`,
  );
}

// ── the client ─────────────────────────────────────────────────────────────

/** How often a commit is rebuilt after losing an epoch race (or a send
 *  re-sealed after the group moved on) before giving up. */
const MAX_REBASES = 5;
/** Re-sends of a mutation with the SAME idempotency key after a transport failure. */
const MAX_TRANSPORT_RETRIES = 3;

const TRANSIENT = new Set<number>([
  grpc.status.UNAVAILABLE,
  grpc.status.DEADLINE_EXCEEDED,
  grpc.status.RESOURCE_EXHAUSTED,
  grpc.status.ABORTED,
]);

/** The group epoch a stale-epoch refusal reports (FAILED_PRECONDITION with
 *  `ErrorDetail.missing.current_epoch`), if `err` is one. */
export function staleEpoch(err: unknown): number | undefined {
  if (!(err instanceof UdbError) || err.code !== grpc.status.FAILED_PRECONDITION) return undefined;
  const current = err.detail?.missing?.current_epoch;
  if (current === undefined || current === "") return undefined;
  const epoch = Number(current);
  return Number.isFinite(epoch) ? epoch : undefined;
}

function isTransient(err: unknown): boolean {
  return err instanceof UdbError && TRANSIENT.has(err.code);
}

export class MessagingError extends Error {
  constructor(message: string) {
    super(`udb messaging: ${message}`);
    this.name = "MessagingError";
  }
}

export interface CommitOutcome {
  /** The group's epoch after the commit (unchanged when nothing was to do). */
  epoch: number;
  addedDeviceIds: string[];
  removedDeviceIds: string[];
  /** Devices that had no KeyPackage to claim (they were not added). */
  missingDeviceIds: string[];
  /** How many times the commit was rebuilt because a concurrent commit won. */
  rebases: number;
  /** False when there was nothing to commit. */
  committed: boolean;
}

export interface KeyPackageStock {
  oneTimeRemaining: number;
  hasLastResort: boolean;
  published: number;
}

/** Argon2id costs of the backup key (memory in KiB). */
export interface BackupParams {
  memoryKib: number;
  iterations: number;
  parallelism: number;
}

/** What `MessagingClient.restore` needs: the client options minus the device,
 *  plus where the backup is and the secret that opens it. */
export interface RestoreOptions extends Omit<MessagingClientOptions, "device"> {
  /** The tenant the restored device must belong to. */
  tenantId: string;
  recoverySecret: Uint8Array | string;
  /** The backup's owner ("" = the caller). */
  subjectId?: string;
  /** "" = "default". */
  backupId?: string;
}

export interface MessagingClientOptions {
  /** The generated MessagingService API (on the native listener's client). */
  api: MessagingServiceApi;
  device: MlsDevice;
  /** The WebAssembly module the device came from (default: `loadMlsWasm()`). */
  wasm?: MlsWasmModule;
  /** Called with the device snapshot after every state change, BEFORE the
   *  change is acknowledged to the broker or a ciphertext leaves. Persist it
   *  where the device keeps secrets; it holds private keys. */
  persist?: (snapshot: Uint8Array) => void | Promise<void>;
  /** Keep this many one-time KeyPackages published (default 20)… */
  keyPackageTarget?: number;
  /** …topping up when the stock falls below this (default 5). */
  keyPackageThreshold?: number;
  /** Per-call options forwarded to every RPC. */
  call?: CallOptions;
  /** Argon2id costs for `backup` (default: the recommended 19 MiB / 2 / 1). */
  backupParams?: BackupParams;
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function utf8(value: Uint8Array | string): Uint8Array {
  return typeof value === "string" ? new TextEncoder().encode(value) : value;
}

function bytes(value: Uint8Array | Buffer | undefined | null): Buffer {
  if (!value) return Buffer.alloc(0);
  return Buffer.isBuffer(value) ? value : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
}

function streamError(err: unknown): unknown {
  return (err as { udb?: unknown })?.udb ?? err;
}

/** One device's messaging session over the MessagingService. Every inbox entry
 *  is processed — and the state persisted — before it is acknowledged. */
export class MessagingClient {
  readonly api: MessagingServiceApi;
  readonly device: MlsDevice;
  private readonly wasm?: MlsWasmModule;
  private readonly persistHook?: (snapshot: Uint8Array) => void | Promise<void>;
  private readonly target: number;
  private readonly threshold: number;
  private readonly call?: CallOptions;
  private readonly backupParams?: BackupParams;
  private backlog: MlsEvent[] = [];
  private acked = 0;
  private ids = 0;
  private follow?: grpc.ClientReadableStream<any>;
  private followQueue: MlsEvent[] = [];
  private followError?: unknown;
  private followWaiters: Array<() => void> = [];
  private followChain: Promise<void> = Promise.resolve();

  constructor(options: MessagingClientOptions) {
    this.api = options.api;
    this.device = options.device;
    this.wasm = options.wasm;
    this.persistHook = options.persist;
    this.target = Math.max(1, options.keyPackageTarget ?? 20);
    this.threshold = Math.min(options.keyPackageThreshold ?? 5, this.target);
    this.call = options.call;
    this.backupParams = options.backupParams;
  }

  private get tenantId(): string {
    return this.device.tenantId;
  }

  private get deviceId(): string {
    return this.device.deviceId;
  }

  private freshId(prefix: string): string {
    this.ids += 1;
    const random = Math.floor(Math.random() * 0xffffffff).toString(16);
    return `${prefix}-${Date.now().toString(16)}-${this.ids.toString(16)}-${random}`;
  }

  private async persist(): Promise<void> {
    if (this.persistHook) await this.persistHook(this.device.snapshot());
  }

  /** A mutation re-sent with the same idempotency key on transport failures. */
  private async withTransportRetry<T>(send: () => Promise<T>): Promise<T> {
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await send();
      } catch (err) {
        if (!isTransient(err) || attempt >= MAX_TRANSPORT_RETRIES) throw err;
        await sleep(100 * (attempt + 1));
      }
    }
  }

  // ── device directory ─────────────────────────────────────────────────────

  /** Register this device's signature key and credential (idempotent). */
  async registerDevice(): Promise<any> {
    const registration = this.device.registration();
    const response = await this.api.register_messaging_device(
      {
        tenant_id: registration.tenantId,
        subject_id: registration.subjectId,
        device_id: registration.deviceId,
        signature_public_key: bytes(registration.signaturePublicKey),
        credential: bytes(registration.credential),
      },
      this.call,
    );
    const device = (response as any).device;
    if (!device) throw new MessagingError("registration returned no device");
    if (device.key_fingerprint !== registration.keyFingerprint) {
      throw new MessagingError(
        `the directory holds key ${device.key_fingerprint} for this device, not ${registration.keyFingerprint}`,
      );
    }
    return device;
  }

  /** A principal's devices with key fingerprints ("" = this device's principal). */
  async listDevices(subjectId = "", includeRevoked = false): Promise<any[]> {
    const response: any = await this.api.list_messaging_devices(
      { tenant_id: this.tenantId, subject_id: subjectId, include_revoked: includeRevoked },
      this.call,
    );
    return response.devices ?? [];
  }

  async revokeDevice(deviceId: string, reason = ""): Promise<any> {
    return this.api.revoke_messaging_device(
      { tenant_id: this.tenantId, device_id: deviceId, reason },
      this.call,
    );
  }

  // ── key packages ─────────────────────────────────────────────────────────

  /** Top the one-time KeyPackage stock up to the target when it is below the
   *  threshold, and publish a last-resort package if the broker has none. */
  async ensureKeyPackages(): Promise<KeyPackageStock> {
    const stock: any = await this.api.get_key_package_stock(
      { tenant_id: this.tenantId, device_id: this.deviceId },
      this.call,
    );
    const remaining = Math.max(0, Number(stock.one_time_remaining ?? 0));
    const topUp = remaining < this.threshold ? this.target - remaining : 0;
    if (topUp === 0 && stock.has_last_resort) {
      return { oneTimeRemaining: remaining, hasLastResort: true, published: 0 };
    }
    const keyPackages = this.device.keyPackages(topUp).map(bytes);
    const lastResort = stock.has_last_resort ? Buffer.alloc(0) : bytes(this.device.lastResortKeyPackage());
    await this.persist();
    const published: any = await this.api.publish_key_packages(
      {
        tenant_id: this.tenantId,
        device_id: this.deviceId,
        key_packages: keyPackages,
        last_resort_key_package: lastResort,
      },
      this.call,
    );
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
  async backup(recoverySecret: Uint8Array | string, history: Uint8Array | string = "", backupId = ""): Promise<any> {
    const secret = utf8(recoverySecret);
    const plain = utf8(history);
    const envelope = this.backupParams
      ? this.device.exportBackupWith(
          secret,
          plain,
          this.backupParams.memoryKib,
          this.backupParams.iterations,
          this.backupParams.parallelism,
        )
      : this.device.exportBackup(secret, plain);
    const response: any = await this.api.put_messaging_backup(
      {
        tenant_id: this.tenantId,
        subject_id: this.device.subjectId,
        backup_id: backupId,
        ciphertext: bytes(envelope),
        expected_version: 0,
      },
      this.call,
    );
    if (!response.backup) throw new MessagingError("the backup write returned no backup");
    return response.backup;
  }

  /** Erase the principal's backup `backupId` ("" = "default"); false when none was stored. */
  async deleteBackup(backupId = ""): Promise<boolean> {
    const response: any = await this.api.delete_messaging_backup(
      { tenant_id: this.tenantId, subject_id: this.device.subjectId, backup_id: backupId },
      this.call,
    );
    return Boolean(response.deleted);
  }

  /** Restore a device on a new machine: fetch the backup, open it with the
   *  recovery secret, and load the backed-up device (its identity, keys,
   *  groups and inbox cursor). The snapshot is handed to `persist` before this
   *  returns. Also returns the history the backup carried. */
  static async restore(options: RestoreOptions): Promise<{ client: MessagingClient; history: Uint8Array }> {
    const stored: any = await options.api.get_messaging_backup(
      { tenant_id: options.tenantId, subject_id: options.subjectId ?? "", backup_id: options.backupId ?? "" },
      options.call,
    );
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
  async createGroup(groupId: string): Promise<number> {
    const creation = this.device.createGroup(groupId);
    await this.persist();
    try {
      const response: any = await this.api.create_group(
        {
          tenant_id: this.tenantId,
          group_id: groupId,
          creator_device_id: this.deviceId,
          group_info: bytes(creation.groupInfo),
          ratchet_tree: bytes(creation.ratchetTree),
        },
        this.call,
      );
      return Number(response.epoch ?? 0);
    } catch (err) {
      this.device.forgetGroup(groupId);
      await this.persist();
      throw err;
    }
  }

  /** Add every active device of `subjectIds` plus `deviceIds`, claiming one
   *  KeyPackage per device; members (and this device) are skipped. */
  async addMembers(groupId: string, subjectIds: string[], deviceIds: string[] = []): Promise<CommitOutcome> {
    const members = new Set(this.device.members(groupId));
    const targets = new Set(deviceIds);
    for (const subject of subjectIds) {
      for (const device of await this.listDevices(subject, false)) {
        if (device.status === "ACTIVE") targets.add(device.device_id);
      }
    }
    targets.delete(this.deviceId);
    for (const member of members) targets.delete(member);
    if (targets.size === 0) {
      return this.noop(groupId, 0);
    }
    const claimed: any = await this.api.claim_key_packages(
      {
        tenant_id: this.tenantId,
        requester_device_id: this.deviceId,
        target_device_ids: [...targets].sort(),
        target_subject_ids: [],
      },
      this.call,
    );
    const wasm = this.inspector();
    const keyPackages: Uint8Array[] = [];
    for (const claim of claimed.key_packages ?? []) {
      const info = wasm(claim.key_package);
      if (info.tenantId !== this.tenantId || info.subjectId !== claim.subject_id || info.deviceId !== claim.device_id) {
        throw new MessagingError(
          `the KeyPackage claimed for device '${claim.device_id}' names '${info.tenantId}/${info.subjectId}/${info.deviceId}'`,
        );
      }
      keyPackages.push(new Uint8Array(claim.key_package));
    }
    const outcome = await this.submit(groupId, () => this.device.commitAdd(groupId, keyPackages));
    outcome.missingDeviceIds = [...(claimed.missing_device_ids ?? [])];
    return outcome;
  }

  /** Remove member devices (for example after a `removal_hint` event). */
  async removeDevices(groupId: string, deviceIds: string[]): Promise<CommitOutcome> {
    return this.submit(groupId, () => this.device.commitRemove(groupId, deviceIds));
  }

  /** Rotate this device's leaf keys in the group (post-compromise security). */
  async updateKeys(groupId: string): Promise<CommitOutcome> {
    return this.submit(groupId, () => this.device.commitUpdate(groupId));
  }

  private inspectorFn?: (bytes: Uint8Array) => ReturnType<MlsWasmModule["inspectKeyPackage"]>;

  /** The KeyPackage inspector of the module the device came from. */
  private inspector(): (bytes: Uint8Array) => ReturnType<MlsWasmModule["inspectKeyPackage"]> {
    if (!this.inspectorFn) {
      const module = this.wasm ?? loadMlsWasm();
      this.inspectorFn = (value) => module.inspectKeyPackage(new Uint8Array(value));
    }
    return this.inspectorFn;
  }

  private noop(groupId: string, rebases: number): CommitOutcome {
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
  private async submit(groupId: string, build: () => MlsCommit | undefined): Promise<CommitOutcome> {
    let rebases = 0;
    let next: () => MlsCommit | undefined = build;
    for (;;) {
      const pending = next();
      if (!pending) {
        await this.persist();
        return this.noop(groupId, rebases);
      }
      await this.persist();
      const clientMessageId = this.freshId("commit");
      try {
        await this.withTransportRetry(() =>
          this.api.submit_commit(
            {
              tenant_id: this.tenantId,
              group_id: groupId,
              sender_device_id: this.deviceId,
              epoch: String(pending.epoch),
              commit: bytes(pending.commit),
              welcome: bytes(pending.welcome),
              group_info: Buffer.alloc(0),
              client_message_id: clientMessageId,
            },
            { ...(this.call ?? {}), noRetry: true },
          ),
        );
      } catch (err) {
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
  async send(groupId: string, plaintext: Uint8Array | string): Promise<any> {
    const data = typeof plaintext === "string" ? new TextEncoder().encode(plaintext) : plaintext;
    const clientMessageId = this.freshId("msg");
    for (let rebases = 0; ; rebases += 1) {
      const sealed = this.device.encrypt(groupId, data);
      await this.persist();
      try {
        return await this.withTransportRetry(() =>
          this.api.send_message(
            {
              tenant_id: this.tenantId,
              group_id: groupId,
              sender_device_id: this.deviceId,
              ciphertext: bytes(sealed.ciphertext),
              client_message_id: clientMessageId,
            },
            { ...(this.call ?? {}), noRetry: true },
          ),
        );
      } catch (err) {
        const current = staleEpoch(err);
        if (current === undefined || rebases >= MAX_REBASES) throw err;
        await this.catchUp(groupId, current);
      }
    }
  }

  // ── inbox ────────────────────────────────────────────────────────────────

  private openInbox(follow: boolean): grpc.ClientReadableStream<any> {
    return this.api.stream_inbox(
      {
        tenant_id: this.tenantId,
        device_id: this.deviceId,
        after_seq: String(this.device.inboxCursor),
        follow,
        max_entries: 0,
      },
      this.call,
    );
  }

  /** Process one delivered entry; the event unless it needs no action. */
  private handle(response: any): MlsEvent | undefined {
    if (response?.error && response.error.message) {
      throw new MessagingError(`inbox stream reported an error: ${response.error.message}`);
    }
    const entry = response?.entry;
    if (!entry) return undefined;
    const event = this.device.process(
      Number(entry.seq ?? 0),
      String(entry.kind ?? ""),
      String(entry.group_id ?? ""),
      Number(entry.epoch ?? 0),
      new Uint8Array(entry.payload ?? []),
      String(entry.sender_device_id ?? ""),
      String(entry.client_message_id ?? ""),
    );
    return event.kind === "skipped" ? undefined : event;
  }

  /** Acknowledge every processed entry to the broker. */
  async ack(): Promise<void> {
    const cursor = this.device.inboxCursor;
    if (cursor <= this.acked) return;
    const response: any = await this.api.ack_inbox(
      { tenant_id: this.tenantId, device_id: this.deviceId, up_to_seq: String(cursor) },
      this.call,
    );
    this.acked = Math.max(Number(response.acked_seq ?? 0), cursor);
  }

  private async drainIntoBacklog(): Promise<void> {
    const stream = this.openInbox(false);
    try {
      for await (const response of stream) {
        const event = this.handle(response);
        if (event) this.backlog.push(event);
      }
    } catch (err) {
      throw streamError(err);
    }
    await this.persist();
    await this.ack();
  }

  /** Process the inbox until the group reaches `epoch` (a winning commit). */
  private async catchUp(groupId: string, epoch: number): Promise<void> {
    for (let attempt = 0; attempt < 3; attempt += 1) {
      await this.drainIntoBacklog();
      if (this.device.epoch(groupId) >= epoch) return;
      await sleep(100);
    }
    throw new MessagingError(
      `group '${groupId}' is at epoch ${this.device.epoch(groupId)} after processing the inbox; the broker reports ${epoch}`,
    );
  }

  /** Process everything currently in the inbox and return the events (plus any
   *  processed while re-basing). Does not wait for new entries. */
  async sync(): Promise<MlsEvent[]> {
    await this.drainIntoBacklog();
    const events = this.backlog;
    this.backlog = [];
    return events;
  }

  private wake(): void {
    const waiters = this.followWaiters;
    this.followWaiters = [];
    for (const waiter of waiters) waiter();
  }

  private ensureFollow(): void {
    if (this.follow) return;
    const stream = this.openInbox(true);
    this.follow = stream;
    stream.on("data", (response: any) => {
      // Processing is synchronous and in order; persistence and the ack are
      // chained so an entry is acknowledged only after its state was saved.
      try {
        const event = this.handle(response);
        this.followChain = this.followChain
          .then(() => this.persist())
          .then(() => this.ack())
          .then(() => {
            if (event) this.followQueue.push(event);
            this.wake();
          })
          .catch((err) => {
            this.followError = err;
            this.wake();
          });
      } catch (err) {
        this.followError = err;
        this.wake();
      }
    });
    stream.on("error", (err: unknown) => {
      if ((err as grpc.ServiceError)?.code !== grpc.status.CANCELLED) this.followError = streamError(err);
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
  async nextEvent(timeoutMs = 30_000): Promise<MlsEvent | undefined> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const queued = this.backlog.shift() ?? this.followQueue.shift();
      if (queued) return queued;
      if (this.followError !== undefined) {
        const err = this.followError;
        this.followError = undefined;
        throw err;
      }
      this.ensureFollow();
      const remaining = deadline - Date.now();
      if (remaining <= 0) return undefined;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, remaining);
        this.followWaiters.push(() => {
          clearTimeout(timer);
          resolve();
        });
      });
    }
  }

  /** Close the follow stream, if one is open. */
  close(): void {
    this.follow?.cancel();
    this.follow = undefined;
  }
}
