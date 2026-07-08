#!/usr/bin/env node
// Verify the Pages playground calls the current UDB WASM parser, not a canned
// result. This intentionally mirrors docs/site/playground.js' C-ABI calls.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const enc = new TextEncoder();
const dec = new TextDecoder();
let ex;

const imports = {
  __wbindgen_placeholder__: {
    __wbindgen_describe() {},
    __wbg___wbindgen_throw_1506f2235d1bdba0(ptr, len) {
      let msg = "";
      try {
        msg = dec.decode(new Uint8Array(ex.memory.buffer, ptr, len));
      } catch {
        msg = "";
      }
      throw new Error(msg || "UDB WASM threw an exception");
    },
  },
  __wbindgen_externref_xform__: {
    __wbindgen_externref_table_set_null() {},
    __wbindgen_externref_table_grow() {
      return 0;
    },
  },
};

function nsFromProto(proto) {
  const match = /(?:^|\n)\s*package\s+([A-Za-z0-9_.]+)\s*;/.exec(proto);
  return match ? match[1] : "";
}

function writeStr(value) {
  const bytes = enc.encode(value);
  const ptr = ex.udb_alloc(bytes.length);
  new Uint8Array(ex.memory.buffer, ptr, bytes.length).set(bytes);
  return [ptr, bytes.length];
}

function parse(proto) {
  const src = writeStr(proto);
  const ns = writeStr(nsFromProto(proto));
  const packed = ex.udb_parse(src[0], src[1], ns[0], ns[1]);
  const ptr = Number(packed >> 32n);
  const len = Number(packed & 0xffffffffn);
  const json = dec.decode(new Uint8Array(ex.memory.buffer, ptr, len));
  ex.udb_free(src[0], src[1]);
  ex.udb_free(ns[0], ns[1]);
  ex.udb_free(ptr, len);
  return JSON.parse(json);
}

function firstColumns(result) {
  return (result.schemas?.[0]?.columns || []).map((col) => ({
    field: col.field_name || "",
    column: col.column_name || "",
  }));
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

const wasmPath = resolve(process.argv[2] || "docs/site/udb.wasm");
const wasmBytes = readFileSync(wasmPath);
const { instance } = await WebAssembly.instantiate(wasmBytes, imports);
ex = instance.exports;

for (const name of ["memory", "udb_alloc", "udb_free", "udb_parse"]) {
  assert(ex[name], `missing WASM export ${name}`);
}

const invoiceProto = `syntax = "proto3";
package myapp.v1;

message Invoice {
  option (myapp.v1.table) = {
    table_name: "invoices" schema_name: "billing"
    is_table: true enable_rls: true
  };

  string invoice_id = 1 [(myapp.v1.column) = { column_name: "invoice_id" sql_type: "UUID" primary_key: true not_null: true }];
  string tenant_id = 2 [(myapp.v1.column) = { column_name: "tenant_id" tenant_column: true not_null: true }];
  string email = 3 [(myapp.v1.column) = { column_name: "email" sql_type: "TEXT" pii_kind: PII_KIND_EMAIL encrypt: true }];
}`;

const mobileProto = invoiceProto.replaceAll("email", "mobile");
const invoice = parse(invoiceProto);
const mobile = parse(mobileProto);
const broken = parse('syntax = "proto3";\npackage myapp.v1;\nmessage Broken {\n  string id = 1 [\n}');

assert(invoice.ok === true, `invoice proto failed: ${invoice.error || "unknown"}`);
assert(mobile.ok === true, `mobile proto failed: ${mobile.error || "unknown"}`);
assert(
  Array.isArray(broken.diagnostics) && broken.diagnostics.length > 0,
  "malformed proto did not produce parser diagnostics",
);

const invoiceColumns = firstColumns(invoice);
const mobileColumns = firstColumns(mobile);
assert(
  invoiceColumns.some((col) => col.field === "email" && col.column === "email"),
  `invoice result did not include email/email: ${JSON.stringify(invoiceColumns)}`,
);
assert(
  mobileColumns.some((col) => col.field === "mobile" && col.column === "mobile"),
  `edited result did not include mobile/mobile: ${JSON.stringify(mobileColumns)}`,
);
assert(
  !mobileColumns.some((col) => col.field === "email" || col.column === "email"),
  `edited result still contained email: ${JSON.stringify(mobileColumns)}`,
);
assert(invoice.checksum !== mobile.checksum, "manifest checksum did not change after proto edit");

console.log(
  JSON.stringify(
    {
      ok: true,
      wasm: wasmPath,
      invoice_checksum: invoice.checksum,
      mobile_checksum: mobile.checksum,
      columns_changed: { before: invoiceColumns, after: mobileColumns },
    },
    null,
    2,
  ),
);
