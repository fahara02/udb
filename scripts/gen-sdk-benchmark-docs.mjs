#!/usr/bin/env node
// Generate SDK benchmark coverage docs from the committed machine-readable
// benchmark artifacts. This keeps surface counts and benchmark status out of
// hand-maintained prose.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, "..");

const benchManifestPath = join(repoRoot, "docs", "generated", "bench-bodies.json");
const benchResultsPath = join(repoRoot, "docs", "site", "bench-results.json");
const generatedGoPath = join(repoRoot, "sdk", "go", "udbclient", "generated_client.go");
const liveCoveragePath = join(repoRoot, "sdk", "SDK_LIVE_TEST_COVERAGE.md");
const perfListingPath = join(repoRoot, "sdk", "SDK_PERF_LISTING.md");

const LIVE_HARNESSES = new Map([
  ["go", "live full-surface + scenario benchmark harness"],
  ["typescript", "live full-surface + scenario benchmark harness"],
  ["python", "live full-surface + scenario benchmark harness"],
  ["php", "live full-surface + scenario benchmark harness"],
  ["java", "static SDK conformance only; no live per-RPC benchmark harness yet"],
  ["csharp", "static SDK conformance only; no live per-RPC benchmark harness yet"],
]);

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function esc(value) {
  return String(value ?? "").replaceAll("|", "\\|");
}

function code(value) {
  return `\`${esc(value)}\``;
}

function parseGeneratedGoRPCs() {
  const text = readFileSync(generatedGoPath, "utf8");
  const rows = [];
  const re = /\{Service:\s*"([^"]+)",\s*ServicePkg:\s*"([^"]+)",\s*FullMethod:\s*"([^"]+)",\s*Name:\s*"([^"]+)",\s*APIAlias:\s*"([^"]*)",\s*OperationID:\s*"([^"]*)",[\s\S]*?OperationKind:\s*"([^"]+)"/g;
  for (const match of text.matchAll(re)) {
    rows.push({
      service: match[1],
      path: match[3],
      rpc: match[4],
      wire_rpc: `${match[1]}/${match[4]}`,
      api_alias: match[5],
      operation_id: match[6],
      operation_kind: match[7],
    });
  }
  if (!rows.length) {
    throw new Error(`could not parse generated RPC metadata from ${generatedGoPath}`);
  }
  return rows;
}

function serviceSummary(rows) {
  const byService = new Map();
  for (const row of rows) {
    const current = byService.get(row.service) ?? {
      service: row.service,
      total: 0,
      read_only: 0,
      mutation: 0,
      destructive: 0,
      other: 0,
    };
    current.total += 1;
    const kind = normalizeKind(row.operation_kind || row.op_kind);
    if (kind === "read_only") {
      current.read_only += 1;
    } else if (kind === "mutation") {
      current.mutation += 1;
    } else if (kind === "destructive") {
      current.destructive += 1;
    } else {
      current.other += 1;
    }
    byService.set(row.service, current);
  }
  return [...byService.values()].sort((a, b) => a.service.localeCompare(b.service));
}

function normalizeKind(kind) {
  const raw = String(kind || "").toLowerCase();
  if (raw === "ro" || raw === "read-only" || raw === "read only") return "read_only";
  if (raw === "mut") return "mutation";
  if (raw === "dest") return "destructive";
  return raw;
}

function validateSurface(manifest, generated) {
  const failures = [];
  const manifestByWire = new Map(manifest.map((row) => [row.wire_rpc, row]));
  const generatedByWire = new Map(generated.map((row) => [row.wire_rpc, row]));
  if (manifest.length !== generated.length) {
    failures.push(`surface mismatch: bench manifest has ${manifest.length} rows, generated SDK has ${generated.length}`);
  }
  for (const row of generated) {
    const got = manifestByWire.get(row.wire_rpc);
    if (!got) {
      failures.push(`surface mismatch: missing manifest row for ${row.wire_rpc}`);
      continue;
    }
    if (got.api_alias !== row.api_alias || got.operation_id !== row.operation_id) {
      failures.push(
        `identity mismatch for ${row.wire_rpc}: ${got.api_alias}/${got.operation_id} != ${row.api_alias}/${row.operation_id}`,
      );
    }
    if (normalizeKind(got.op_kind) !== normalizeKind(row.operation_kind)) {
      failures.push(`operation kind mismatch for ${row.wire_rpc}: ${got.op_kind} != ${row.operation_kind}`);
    }
  }
  for (const row of manifest) {
    if (!generatedByWire.has(row.wire_rpc)) {
      failures.push(`surface mismatch: extra manifest row for ${row.wire_rpc}`);
    }
  }
  if (failures.length) {
    throw new Error(failures.join("\n"));
  }
}

function renderHarnessRows() {
  return [...LIVE_HARNESSES.entries()]
    .map(([sdk, status]) => `| ${sdk} | ${status} |`)
    .join("\n");
}

function renderServiceRows(summary) {
  return summary
    .map((row) => `| ${row.service} | ${row.total} | ${row.read_only} | ${row.mutation} | ${row.destructive} | ${row.other} |`)
    .join("\n");
}

function renderManifestRows(manifest) {
  return manifest
    .slice()
    .sort((a, b) => a.service.localeCompare(b.service) || a.rpc.localeCompare(b.rpc))
    .map((row) =>
      `| ${row.service} | ${code(row.wire_rpc)} | ${code(row.api_alias)} | ${code(row.operation_id)} | ${row.op_kind} | ${row.file} |`,
    )
    .join("\n");
}

function renderCoverageDoc(manifest, generated) {
  const services = serviceSummary(generated);
  return `# UDB SDK Live-Test Coverage

Generated by \`node scripts/gen-sdk-benchmark-docs.mjs\`.

Inputs:
- \`docs/generated/bench-bodies.json\`
- \`sdk/go/udbclient/generated_client.go\`
- \`docs/site/bench-results.json\`

Current generated RPC surface: ${generated.length} RPCs across ${services.length} services.

The benchmark body manifest is checked against generated SDK metadata before this
file is written. A missing row, extra row, alias drift, operationId drift, or
operation-kind drift fails generation.

## SDK Coverage Split

UDB ships six generated SDKs. Go, TypeScript, Python, and PHP own live broker
benchmark harnesses. Java and C# are still covered by static SDK conformance, but
do not yet publish per-RPC live benchmark results.

| SDK | Coverage owner |
|---|---|
${renderHarnessRows()}

## Service Surface

| Service | RPCs | Read-only | Mutation | Destructive | Other |
|---|---:|---:|---:|---:|---:|
${renderServiceRows(services)}

## Per-RPC Benchmark Manifest

Every row below is generated from the manifest consumed by the SDK live
benchmark harnesses.

| Service | Wire RPC | Public alias | Operation ID | Kind | Body source |
|---|---|---|---|---|---|
${renderManifestRows(manifest)}
`;
}

function renderBenchSdkRows(results) {
  const rows = Array.isArray(results.sdks) ? results.sdks : [];
  if (!rows.length) {
    return "| _none committed_ | - | - | - | - |\n";
  }
  return rows
    .map((sdk) => {
      const measured = sdk.measured_rpc_count ?? sdk.measured ?? sdk.full_rpc_count ?? "-";
      const failed = sdk.failed_rpc_count ?? sdk.failed ?? "-";
      const status = sdk.status ?? "-";
      const note = sdk.note ?? sdk.harness_error ?? "";
      return `| ${sdk.name ?? sdk.sdk ?? "-"} | ${status} | ${measured} | ${failed} | ${esc(note)} |`;
    })
    .join("\n");
}

function renderPerfDoc(manifest, generated, results) {
  const services = serviceSummary(generated);
  const summary = results.summary ?? {};
  const hasPublishedRun = Array.isArray(results.sdks) && results.sdks.length > 0;
  const releaseTag = results.release?.tag || "";
  const generatedAt = results.generated_at || "";
  return `# UDB SDK Performance Listing

Generated by \`node scripts/gen-sdk-benchmark-docs.mjs\`.

Inputs:
- \`docs/generated/bench-bodies.json\`
- \`docs/site/bench-results.json\`

Current generated RPC surface: ${generated.length} RPCs across ${services.length} services.

Published benchmark artifact: ${hasPublishedRun ? "present" : "not committed yet"}.
Release tag: ${releaseTag || "-"}.
Generated at: ${generatedAt || "-"}.

## Published Result Summary

| Metric | Value |
|---|---:|
| SDKs OK | ${summary.ok ?? 0} |
| SDKs failed | ${summary.failed ?? 0} |
| SDKs skipped | ${summary.skipped ?? 0} |
| Measured RPC rows | ${summary.measured_rpc_count ?? 0} |
| Failed RPC rows | ${summary.failed_rpc_count ?? 0} |

## SDK Result Status

| SDK | Status | Measured RPCs | Failed RPCs | Note |
|---|---|---:|---:|---|
${renderBenchSdkRows(results)}

## Benchmark Harness Ownership

| SDK | Coverage owner |
|---|---|
${renderHarnessRows()}

## Manifest Surface By Service

| Service | RPCs | Read-only | Mutation | Destructive | Other |
|---|---:|---:|---:|---:|---:|
${renderServiceRows(services)}

## Canonical APIs

The dashboard groups by \`operation_id || api_alias || wire_api\` and keeps the
raw wire RPC visible for debugging. The table below is the generated public
identity surface available to benchmark reports.

| Wire RPC | Public alias | Operation ID | Kind |
|---|---|---|---|
${manifest
  .slice()
  .sort((a, b) => a.service.localeCompare(b.service) || a.rpc.localeCompare(b.rpc))
  .map((row) => `| ${code(row.wire_rpc)} | ${code(row.api_alias)} | ${code(row.operation_id)} | ${row.op_kind} |`)
  .join("\n")}
`;
}

function buildDocs() {
  const manifest = readJson(benchManifestPath);
  const results = readJson(benchResultsPath);
  const generated = parseGeneratedGoRPCs();
  validateSurface(manifest, generated);
  return {
    [liveCoveragePath]: renderCoverageDoc(manifest, generated),
    [perfListingPath]: renderPerfDoc(manifest, generated, results),
  };
}

function main() {
  const check = process.argv.includes("--check");
  const docs = buildDocs();
  const failures = [];
  for (const [path, next] of Object.entries(docs)) {
    if (check) {
      let current = "";
      try {
        current = readFileSync(path, "utf8");
      } catch {
        failures.push(`${path} is missing; run node scripts/gen-sdk-benchmark-docs.mjs`);
        continue;
      }
      if (current !== next) {
        failures.push(`${path} is stale; run node scripts/gen-sdk-benchmark-docs.mjs`);
      }
    } else {
      writeFileSync(path, next, "utf8");
      console.log(`wrote ${path}`);
    }
  }
  if (failures.length) {
    for (const failure of failures) console.error(failure);
    process.exit(1);
  }
  if (check) console.log("SDK benchmark docs are fresh");
}

main();
