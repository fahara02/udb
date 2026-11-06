// MLS in the browser: the device's keys stay on the device, and the application carries the
// ciphertext however it likes. This file is safe to bundle for a browser: it imports only
// types, and takes the WebAssembly module from the caller, so a bundler resolves it.
//
//   import init, * as bindings from "@udb_plus/sdk/mls-wasm-web/udb_mls_wasm.js";
//   import { indexedDbKeyValue, loadMlsWeb, openWebDevice } from "@udb_plus/sdk/mls-web";
//
//   const wasm = await loadMlsWeb(init, bindings);
//   const device = await openWebDevice(wasm, indexedDbKeyValue("mls"), tenant, subject, deviceId);

import type { MlsDevice, MlsWasmModule } from "./messaging";

/** Where a device's sealed state lives. Values are what structured clone can store. */
export interface MlsKeyValue {
  get(key: string): Promise<unknown>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<void>;
}

const SEAL_KEY = "udb.mls.seal-key";
const STATE_PREFIX = "udb.mls.device:";

/** Run wasm-bindgen's initializer and return the module. `input` is the `.wasm` URL or bytes;
 *  left out, the initializer finds the file beside its own script. */
export async function loadMlsWeb(
  init: (input?: { module_or_path: unknown }) => Promise<unknown>,
  bindings: unknown,
  input?: unknown,
): Promise<MlsWasmModule> {
  await init(input === undefined ? undefined : { module_or_path: input });
  return bindings as MlsWasmModule;
}

/** The sealing key: AES-GCM, not extractable, so script on the page can use it and cannot read
 *  it out. A stored `CryptoKey` keeps that property. */
async function sealKey(store: MlsKeyValue): Promise<CryptoKey> {
  const existing = (await store.get(SEAL_KEY)) as CryptoKey | undefined;
  if (existing) return existing;
  const key = await crypto.subtle.generateKey({ name: "AES-GCM", length: 256 }, false, ["encrypt", "decrypt"]);
  await store.put(SEAL_KEY, key);
  return key;
}

function stateKey(tenantId: string, subjectId: string, deviceId: string): string {
  return `${STATE_PREFIX}${tenantId}/${subjectId}/${deviceId}`;
}

/** Save a device's state, sealed. Call it after every call that changes the device. */
export async function saveWebDevice(store: MlsKeyValue, device: MlsDevice): Promise<void> {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const sealed = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, await sealKey(store), device.snapshot() as BufferSource);
  await store.put(stateKey(device.tenantId, device.subjectId, device.deviceId), { iv, sealed });
}

/** The saved device, or a new one. State that fails to open throws: starting a new device over
 *  it would lose the old keys without saying so. */
export async function openWebDevice(
  wasm: MlsWasmModule,
  store: MlsKeyValue,
  tenantId: string,
  subjectId: string,
  deviceId: string,
): Promise<MlsDevice> {
  const saved = (await store.get(stateKey(tenantId, subjectId, deviceId))) as
    | { iv: Uint8Array; sealed: ArrayBuffer }
    | undefined;
  if (saved) {
    const plain = await crypto.subtle.decrypt({ name: "AES-GCM", iv: saved.iv as BufferSource }, await sealKey(store), saved.sealed);
    return wasm.MlsDevice.restore(new Uint8Array(plain));
  }
  const device = wasm.MlsDevice.create(tenantId, subjectId, deviceId);
  await saveWebDevice(store, device);
  return device;
}

/** Remove a device's saved state from this browser. */
export async function forgetWebDevice(
  store: MlsKeyValue,
  tenantId: string,
  subjectId: string,
  deviceId: string,
): Promise<void> {
  await store.delete(stateKey(tenantId, subjectId, deviceId));
}

function asPromise<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

/** An {@link MlsKeyValue} over one IndexedDB database. */
export function indexedDbKeyValue(databaseName: string): MlsKeyValue {
  const objectStore = "kv";
  const database = new Promise<IDBDatabase>((resolve, reject) => {
    const open = indexedDB.open(databaseName, 1);
    open.onupgradeneeded = () => {
      open.result.createObjectStore(objectStore);
    };
    open.onsuccess = () => resolve(open.result);
    open.onerror = () => reject(open.error);
  });
  const run = async <T>(mode: IDBTransactionMode, work: (kv: IDBObjectStore) => IDBRequest<T>): Promise<T> =>
    asPromise(work((await database).transaction(objectStore, mode).objectStore(objectStore)));
  return {
    get: (key) => run("readonly", (kv) => kv.get(key)),
    put: async (key, value) => {
      await run("readwrite", (kv) => kv.put(value, key));
    },
    delete: async (key) => {
      await run("readwrite", (kv) => kv.delete(key));
    },
  };
}
