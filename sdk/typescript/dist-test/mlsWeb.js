"use strict";
// MLS in the browser: the device's keys stay on the device, and the application carries the
// ciphertext however it likes. This file is safe to bundle for a browser: it imports only
// types, and takes the WebAssembly module from the caller, so a bundler resolves it.
//
//   import init, * as bindings from "@udb_plus/sdk/mls-wasm-web/udb_mls_wasm.js";
//   import { indexedDbKeyValue, loadMlsWeb, openWebDevice } from "@udb_plus/sdk/mls-web";
//
//   const wasm = await loadMlsWeb(init, bindings);
//   const device = await openWebDevice(wasm, indexedDbKeyValue("mls"), tenant, subject, deviceId);
Object.defineProperty(exports, "__esModule", { value: true });
exports.loadMlsWeb = loadMlsWeb;
exports.saveWebDevice = saveWebDevice;
exports.openWebDevice = openWebDevice;
exports.forgetWebDevice = forgetWebDevice;
exports.indexedDbKeyValue = indexedDbKeyValue;
const SEAL_KEY = "udb.mls.seal-key";
const STATE_PREFIX = "udb.mls.device:";
/** Run wasm-bindgen's initializer and return the module. `input` is the `.wasm` URL or bytes;
 *  left out, the initializer finds the file beside its own script. */
async function loadMlsWeb(init, bindings, input) {
    await init(input === undefined ? undefined : { module_or_path: input });
    return bindings;
}
/** The sealing key: AES-GCM, not extractable, so script on the page can use it and cannot read
 *  it out. A stored `CryptoKey` keeps that property. */
async function sealKey(store) {
    const existing = (await store.get(SEAL_KEY));
    if (existing)
        return existing;
    const key = await crypto.subtle.generateKey({ name: "AES-GCM", length: 256 }, false, ["encrypt", "decrypt"]);
    await store.put(SEAL_KEY, key);
    return key;
}
function stateKey(tenantId, subjectId, deviceId) {
    return `${STATE_PREFIX}${tenantId}/${subjectId}/${deviceId}`;
}
/** Save a device's state, sealed. Call it after every call that changes the device. */
async function saveWebDevice(store, device) {
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const sealed = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, await sealKey(store), device.snapshot());
    await store.put(stateKey(device.tenantId, device.subjectId, device.deviceId), { iv, sealed });
}
/** The saved device, or a new one. State that fails to open throws: starting a new device over
 *  it would lose the old keys without saying so. */
async function openWebDevice(wasm, store, tenantId, subjectId, deviceId) {
    const saved = (await store.get(stateKey(tenantId, subjectId, deviceId)));
    if (saved) {
        const plain = await crypto.subtle.decrypt({ name: "AES-GCM", iv: saved.iv }, await sealKey(store), saved.sealed);
        return wasm.MlsDevice.restore(new Uint8Array(plain));
    }
    const device = wasm.MlsDevice.create(tenantId, subjectId, deviceId);
    await saveWebDevice(store, device);
    return device;
}
/** Remove a device's saved state from this browser. */
async function forgetWebDevice(store, tenantId, subjectId, deviceId) {
    await store.delete(stateKey(tenantId, subjectId, deviceId));
}
function asPromise(request) {
    return new Promise((resolve, reject) => {
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
    });
}
/** An {@link MlsKeyValue} over one IndexedDB database. */
function indexedDbKeyValue(databaseName) {
    const objectStore = "kv";
    const database = new Promise((resolve, reject) => {
        const open = indexedDB.open(databaseName, 1);
        open.onupgradeneeded = () => {
            open.result.createObjectStore(objectStore);
        };
        open.onsuccess = () => resolve(open.result);
        open.onerror = () => reject(open.error);
    });
    const run = async (mode, work) => asPromise(work((await database).transaction(objectStore, mode).objectStore(objectStore)));
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
