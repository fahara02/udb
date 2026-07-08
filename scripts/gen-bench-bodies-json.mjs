#!/usr/bin/env node
// gen-bench-bodies-json.mjs — deterministic generator for the machine-readable
// bench manifest (revised_todo.md R6.1).
//
// The human-editable SOURCE OF TRUTH stays the markdown corpus
// docs/bench-bodies/*.md (column contract in data_broker.md:10 —
// col1=done, col2=RPC, col3=op_kind, col4=request msg, col5=valid body,
// col6=notes). This script parses every `| [ ... ]` data row across that corpus
// and emits docs/generated/bench-bodies.json: a sorted array of entries
//
//   { file, rpc, service, wire_rpc, api_alias, operation_id, op_kind, request_msg, body, notes }
//
// `file` is the source markdown basename (e.g. "authn.md") — the Python consumer
// keys on (file, rpc) to disambiguate per-service; Go/TS/PHP ignore it.
//
// preserving the `<seed:KEY>` references VERBATIM. The four SDK bench readers
// (Go loadBenchBodyRows, TS loadBenchBodyRows, Python bench_body_rows, PHP
// phpBenchBodyRows) consume this JSON instead of re-parsing markdown; a drift
// test asserts the JSON equals a fresh markdown parse so the two cannot diverge.
//
// Determinism: rows are emitted sorted by (file, rpc) so a clean tree always
// produces byte-identical output. NO cargo, NO network — pure file parse.

import { readdirSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, "..");
const benchDir = join(repoRoot, "docs", "bench-bodies");
const outFile = join(repoRoot, "docs", "generated", "bench-bodies.json");

function currentAllRPCCount() {
  const generatedGo = join(repoRoot, "sdk", "go", "udbclient", "generated_client.go");
  try {
    const text = readFileSync(generatedGo, "utf8");
    const block = text.match(/var AllRPCs = \[\]RPCInfo\{([\s\S]*?)\n\}/);
    if (!block) return null;
    return (block[1].match(/\{Service:\s*"/g) || []).length;
  } catch {
    return null;
  }
}

function currentAllRPCMetadata() {
  const generatedGo = join(repoRoot, "sdk", "go", "udbclient", "generated_client.go");
  const text = readFileSync(generatedGo, "utf8");
  const rows = [];
  const re = /\{Service:\s*"([^"]+)",\s*ServicePkg:\s*"([^"]+)",\s*FullMethod:\s*"([^"]+)",\s*Name:\s*"([^"]+)",\s*APIAlias:\s*"([^"]*)",\s*OperationID:\s*"([^"]*)"/g;
  for (const m of text.matchAll(re)) {
    rows.push({
      service: m[1],
      service_pkg: m[2],
      full_method: m[3],
      name: m[4],
      api_alias: m[5],
      operation_id: m[6],
    });
  }
  if (!rows.length) {
    throw new Error("could not parse AllRPCs metadata from sdk/go/udbclient/generated_client.go");
  }
  return rows;
}

function metadataIndex(rows) {
  const counts = new Map();
  for (const row of rows) counts.set(row.name, (counts.get(row.name) || 0) + 1);
  const byKey = new Map();
  for (const row of rows) {
    byKey.set(`${row.service}.${row.name}`, row);
    byKey.set(`${row.service}/${row.name}`, row);
    if (counts.get(row.name) === 1) byKey.set(row.name, row);
  }
  return byKey;
}

// parseBenchBodies walks docs/bench-bodies/*.md and returns the parsed rows.
// Excludes workflow-sequences.md (a different `| helper | sequence |` table, not
// an RPC-body manifest — matching every SDK reader). A data row is split on "|"
// into: [leadingEmpty, done, rpc, op_kind, request_msg, body, ...notes].
// The body cell can never contain a literal "|" (verified), so notes is the
// remaining cells re-joined to be safe.
export function parseBenchBodies(dir = benchDir) {
  const out = [];
  for (const file of readdirSync(dir).sort()) {
    if (!file.endsWith(".md") || file === "workflow-sequences.md") continue;
    const text = readFileSync(join(dir, file), "utf8");
    for (const rawLine of text.split(/\r?\n/)) {
      const line = rawLine.replace(/\r$/, "");
      // Data rows start with "| [" (the col1 done checkbox). Header
      // ("| done |") and separator ("| --- |") rows never do.
      if (!line.startsWith("| [")) continue;
      const cells = line.split("|").map((c) => c.trim());
      // cells[0] is the empty prefix before the first "|".
      if (cells.length < 7) {
        throw new Error(`malformed manifest row in ${file}: ${line}`);
      }
      const rpc = cells[2];
      if (!rpc) throw new Error(`manifest row with empty RPC in ${file}: ${line}`);
      const op_kind = cells[3];
      const request_msg = cells[4];
      const body = cells[5];
      // Re-join any trailing cells so a stray "|" in notes is preserved.
      const notes = cells.slice(6, cells.length - 1).join("|").trim();
      out.push({ file, rpc, op_kind, request_msg, body, notes });
    }
  }
  // Deterministic order: by file, then by RPC.
  out.sort((a, b) => (a.file < b.file ? -1 : a.file > b.file ? 1 : a.rpc < b.rpc ? -1 : a.rpc > b.rpc ? 1 : 0));
  return out;
}

// buildManifest shapes the public entry contract.
export function buildManifest(dir = benchDir) {
  const rows = parseBenchBodies(dir);
  const byRPC = metadataIndex(currentAllRPCMetadata());
  return rows.map(({ file, rpc, op_kind, request_msg, body, notes }) => {
    const meta = byRPC.get(rpc);
    if (!meta) {
      throw new Error(`bench-body row ${file}:${rpc} has no generated RPC metadata`);
    }
    return {
      file,
      rpc,
      service: meta.service,
      wire_rpc: `${meta.service}/${meta.name}`,
      api_alias: meta.api_alias,
      operation_id: meta.operation_id,
      op_kind,
      request_msg,
      body,
      notes,
    };
  });
}

function main() {
  const manifest = buildManifest();
  const expectedRows = currentAllRPCCount();
  if (expectedRows != null && manifest.length !== expectedRows) {
    console.error(`bench-body manifest has ${manifest.length} rows, want current AllRPCs count ${expectedRows}`);
    process.exit(1);
  }
  mkdirSync(dirname(outFile), { recursive: true });
  writeFileSync(outFile, JSON.stringify(manifest, null, 2) + "\n", "utf8");
  console.log(`wrote ${manifest.length} entries to ${outFile}`);
}

// Run when invoked directly (not when imported by a drift test).
if (resolve(process.argv[1] ?? "") === resolve(fileURLToPath(import.meta.url))) {
  main();
}
