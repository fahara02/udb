#!/usr/bin/env node
// Regenerate/check docs/bench-bodies/*.md RPC skeleton rows from the generated
// native contract while preserving hand-curated body and notes cells.

import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, "..");
const benchDir = join(repoRoot, "docs", "bench-bodies");
const nativeContractPath = join(repoRoot, "docs", "generated", "udb-native-contract.json");
const generatedJsonPath = join(repoRoot, "docs", "generated", "bench-bodies.json");
const tableHeader = "| done | RPC | op_kind | request msg | valid body | seed refs / notes |";
const tableSeparator = "| --- | --- | --- | --- | --- | --- |";

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function shortType(fqn) {
  return String(fqn || "").replace(/^\./, "").split(".").pop() || "";
}

function shortService(fqn) {
  return String(fqn || "").split(".").pop() || "";
}

function opKind(value) {
  const normalized = String(value || "").toLowerCase();
  if (normalized === "read_only") return "READ_ONLY";
  if (normalized === "mutation") return "MUTATION";
  if (normalized === "destructive") return "DESTRUCTIVE";
  throw new Error(`unknown operation_kind ${value}`);
}

function requestMsg(rpc) {
  const input = shortType(rpc.input);
  return rpc.kind === "client_streaming" || rpc.kind === "bidi" ? `stream ${input}` : input;
}

function markdownFiles(dir = benchDir) {
  return readdirSync(dir)
    .filter((file) => file.endsWith(".md") && file !== "workflow-sequences.md")
    .sort();
}

function parseRows(text, file) {
  const rows = new Map();
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.replace(/\r$/, "");
    if (!line.startsWith("| [")) continue;
    const cells = line.split("|").map((cell) => cell.trim());
    if (cells.length < 7) throw new Error(`malformed bench-body row in ${file}: ${line}`);
    const rpc = cells[2];
    if (!rpc) throw new Error(`empty RPC cell in ${file}: ${line}`);
    rows.set(rpc, {
      done: cells[1] || "[ ]",
      rpc,
      op_kind: cells[3],
      request_msg: cells[4],
      body: cells[5] || "TODO",
      notes: cells.slice(6, cells.length - 1).join("|").trim() || "TODO: fill valid body.",
    });
  }
  return rows;
}

function parseMarkdownCorpus(dir = benchDir) {
  const byFile = new Map();
  for (const file of markdownFiles(dir)) {
    byFile.set(file, parseRows(readFileSync(join(dir, file), "utf8"), file));
  }
  return byFile;
}

function generatedRowIndex(path = generatedJsonPath) {
  const rows = readJson(path);
  const byWire = new Map();
  for (const row of rows) {
    if (!row.wire_rpc || !row.file || !row.rpc) {
      throw new Error(`generated bench row missing wire_rpc/file/rpc: ${JSON.stringify(row)}`);
    }
    byWire.set(row.wire_rpc, { file: row.file, rpc: row.rpc });
  }
  return byWire;
}

function fallbackFileForService(service) {
  const svc = service.native_service?.service_id;
  if (svc) return `${svc}.md`;
  return `${basename(service.file || "", ".proto").replace(/_service$/, "")}.md`;
}

export function descriptorRows(contractPath = nativeContractPath, generatedPath = generatedJsonPath) {
  const contract = readJson(contractPath);
  const existing = generatedRowIndex(generatedPath);
  const out = new Map();
  for (const service of contract.services || []) {
    const serviceName = shortService(service.service);
    for (const rpc of service.rpcs || []) {
      const wireRpc = `${serviceName}/${rpc.method}`;
      const existingKey = existing.get(wireRpc);
      const file = existingKey?.file || fallbackFileForService(service);
      const rpcKey = existingKey?.rpc || rpc.method;
      if (!out.has(file)) out.set(file, []);
      out.get(file).push({
        file,
        rpc: rpcKey,
        op_kind: opKind(rpc.operation_kind),
        request_msg: requestMsg(rpc),
        wire_rpc: wireRpc,
      });
    }
  }
  return out;
}

function renderRow(row, previous) {
  const body = previous?.body || "TODO";
  const notes = previous?.notes || "TODO: fill valid body.";
  const done = previous?.done || "[ ]";
  return `| ${done} | ${row.rpc} | ${row.op_kind} | ${row.request_msg} | ${body} | ${notes} |`;
}

function renderFile(text, file, rows, previousRows) {
  const lines = text.split(/\r?\n/);
  const headerIndex = lines.findIndex((line) => line.trim() === tableHeader);
  if (headerIndex < 0) throw new Error(`${file}: missing bench-body table header`);
  const separatorIndex = lines.findIndex((line, index) => index > headerIndex && /^\|\s*-+/.test(line.trim()));
  if (separatorIndex < 0) throw new Error(`${file}: missing bench-body table separator`);
  let endIndex = separatorIndex + 1;
  while (endIndex < lines.length && lines[endIndex].startsWith("| [")) {
    endIndex += 1;
  }
  const renderedRows = rows.map((row) => renderRow(row, previousRows.get(row.rpc)));
  return [
    ...lines.slice(0, headerIndex),
    tableHeader,
    tableSeparator,
    ...renderedRows,
    ...lines.slice(endIndex),
  ].join("\n");
}

export function renderCorpus(dir = benchDir, contractPath = nativeContractPath, generatedPath = generatedJsonPath) {
  const previous = parseMarkdownCorpus(dir);
  const desired = descriptorRows(contractPath, generatedPath);
  const rendered = new Map();
  for (const [file, rows] of desired) {
    const path = join(dir, file);
    const text = readFileSync(path, "utf8");
    rendered.set(file, renderFile(text, file, rows, previous.get(file) || new Map()));
  }
  return rendered;
}

function checkCorpus() {
  const rendered = renderCorpus();
  const failures = [];
  for (const [file, desired] of rendered) {
    const path = join(benchDir, file);
    const current = readFileSync(path, "utf8");
    if (current !== desired) failures.push(file);
  }
  if (failures.length) {
    console.error(`bench-body markdown skeleton drift: ${failures.join(", ")}`);
    console.error("run: node scripts/gen-bench-bodies-skeleton.mjs --write");
    return 1;
  }
  console.log(`bench-body skeleton check passed (${rendered.size} files)`);
  return 0;
}

function writeCorpus() {
  const rendered = renderCorpus();
  for (const [file, desired] of rendered) {
    writeFileSync(join(benchDir, file), desired, "utf8");
  }
  console.log(`wrote bench-body skeleton rows for ${rendered.size} files`);
  return 0;
}

function writeFixture(root) {
  const docs = join(root, "docs", "bench-bodies");
  const generated = join(root, "docs", "generated");
  writeFileSync(join(docs, "alpha.md"), [
    "## Alpha",
    "",
    tableHeader,
    "|------|-----|---------|-------------|------------|-------------------|",
    "| [ ] | Old | RO | OldRequest | `{}` | old row |",
    "",
  ].join("\n"), "utf8");
  writeFileSync(join(generated, "bench-bodies.json"), JSON.stringify([
    { file: "alpha.md", rpc: "Old", wire_rpc: "AlphaService/Old" },
  ]), "utf8");
  writeFileSync(join(generated, "udb-native-contract.json"), JSON.stringify({
    services: [{
      service: "pkg.AlphaService",
      file: "pkg/alpha_service.proto",
      native_service: { service_id: "alpha" },
      rpcs: [
        { method: "Old", operation_kind: "read_only", kind: "unary", input: ".pkg.OldRequest" },
        { method: "Stream", operation_kind: "mutation", kind: "bidi", input: ".pkg.StreamRequest" },
      ],
    }],
  }), "utf8");
}

function runSelftest() {
  const root = mkdtempSync(join(tmpdir(), "udb-bench-skeleton-"));
  try {
    const docs = join(root, "docs", "bench-bodies");
    const generated = join(root, "docs", "generated");
    mkdirSync(docs, { recursive: true });
    mkdirSync(generated, { recursive: true });
    writeFixture(root);
    const rendered = renderCorpus(docs, join(generated, "udb-native-contract.json"), join(generated, "bench-bodies.json"));
    const text = rendered.get("alpha.md") || "";
    if (!text.includes("| [ ] | Old | READ_ONLY | OldRequest | `{}` | old row |")) {
      throw new Error(`did not canonicalize op_kind while preserving body/notes:\n${text}`);
    }
    if (!text.includes("| [ ] | Stream | MUTATION | stream StreamRequest | TODO | TODO: fill valid body. |")) {
      throw new Error(`did not add descriptor-derived missing RPC skeleton:\n${text}`);
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
  console.log("bench-body skeleton selftest passed");
  return 0;
}

async function main() {
  if (process.argv.includes("--selftest")) return runSelftest();
  if (process.argv.includes("--write")) return writeCorpus();
  if (process.argv.includes("--check")) return checkCorpus();
  console.error("usage: node scripts/gen-bench-bodies-skeleton.mjs --check|--write|--selftest");
  return 2;
}

if (resolve(process.argv[1] ?? "") === resolve(fileURLToPath(import.meta.url))) {
  process.exit(await main());
}
