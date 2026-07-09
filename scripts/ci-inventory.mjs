#!/usr/bin/env node
// Source inventory/parity guard for Chapter 15 workflow consolidation.
//
// This does not claim runner-level success or timing wins. It proves the source
// baseline that the consolidation depends on: required CI jobs still exist,
// release is orchestrated once, Pages and cleanup have single owners, and shared
// workflow primitives are referenced where overlap used to live.

import { existsSync, mkdtempSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");

const requiredWorkflows = [
  "ci.yml",
  "release.yml",
  "release-binaries.yml",
  "release-crates.yml",
  "release-docker.yml",
  "release-typescript-sdk.yml",
  "release-python-sdk.yml",
  "release-csharp-sdk.yml",
  "release-packagist.yml",
  "benchmark-sdks.yml",
  "pages.yml",
  "cleanup-packages.yml",
  "publish-skill.yml",
  "lint-workflows.yml",
  "_live-sdk-suite.yml",
  "_shadow-live-sdk.yml",
  "_selftest.yml",
];

const requiredActions = [
  "broker-env",
  "launch-broker",
  "setup-rust",
  "setup-sdk-toolchains",
  "start-backends",
  "version-guard",
];

const requiredCiJobs = [
  "quick-gate",
  "clippy-advisory",
  "rust",
  "build-broker",
  "smoke",
  "auth-release-binary",
  "slim-build",
  "feature-check",
  "plugin-feature-matrix",
  "optimized",
  "aarch64-scalar",
  "supply-chain",
  "buf",
  "php-sdk",
  "go-sdk",
  "ts-sdk",
  "python-sdk",
  "csharp-sdk",
  "java-sdk",
  "sdk-conformance",
  "scaffold-compiles",
  "versions",
  "docs-links",
  "native-integration",
];

const requiredPrCheckJobs = [
  "quick-gate",
  "buf",
  "versions",
  "php-sdk",
  "go-sdk",
  "ts-sdk",
  "python-sdk",
  "csharp-sdk",
  "java-sdk",
  "sdk-conformance",
  "smoke",
  "scaffold-compiles",
];

const prJobTimeoutCeilings = {
  "quick-gate": 15,
  buf: 15,
  versions: 5,
  "php-sdk": 15,
  "go-sdk": 10,
  "ts-sdk": 10,
  "python-sdk": 10,
  "csharp-sdk": 10,
  "java-sdk": 15,
  "sdk-conformance": 35,
  smoke: 30,
  "scaffold-compiles": 35,
  "build-broker": 45,
};

const dependencyFreePrJobs = [
  "clippy-advisory",
  "buf",
  "versions",
  "php-sdk",
  "go-sdk",
  "ts-sdk",
  "python-sdk",
  "csharp-sdk",
  "java-sdk",
  "sdk-conformance",
  "docs-links",
  "supply-chain",
];

const pipelineBudgetClaims = [
  "PR gate",
  "≤ ~8 min",
  "Integration",
  "≤ ~30 min",
  "Release",
  "≤ ~40 min",
  "Budget Measurement Ledger",
  "Required PR check jobs all declare timeout-minutes",
  "Required PR timeout ceilings are enforced by scripts/ci-inventory.mjs",
  "Critical PR artifact path: quick-gate -> build-broker -> {smoke, scaffold-compiles}",
  "PR budget evidence measures the branch-protection-required lane",
  "Cheap PR checks stay dependency-free and start at t=0",
  "Timeout ceilings are source guardrails, not runner wall-clock evidence",
  "Runner wall-clock evidence is still required before marking 15.A.5 done",
  "PR broker compile count: 1 debug build in build-broker",
  "Release graph: ci-green -> version-guard -> build-binaries -> parallel publishers",
  "Post-release benchmark runs only after top-level Release success on a v* tag",
];

const releaseFanoutJobs = [
  "ci-green",
  "version-guard",
  "build-binaries",
  "publish-crates",
  "publish-docker",
  "publish-ts",
  "publish-py",
  "publish-csharp",
  "publish-packagist",
];

const releaseLeafWorkflows = [
  "release-binaries.yml",
  "release-crates.yml",
  "release-docker.yml",
  "release-typescript-sdk.yml",
  "release-python-sdk.yml",
  "release-csharp-sdk.yml",
  "release-packagist.yml",
];

const releaseJobWorkflow = {
  "build-binaries": "release-binaries.yml",
  "publish-crates": "release-crates.yml",
  "publish-docker": "release-docker.yml",
  "publish-ts": "release-typescript-sdk.yml",
  "publish-py": "release-python-sdk.yml",
  "publish-csharp": "release-csharp-sdk.yml",
  "publish-packagist": "release-packagist.yml",
};

const releaseGraphEdges = [
  ["version-guard", "ci-green"],
  ["build-binaries", "version-guard"],
  ["publish-crates", "build-binaries"],
  ["publish-docker", "build-binaries"],
  ["publish-ts", "build-binaries"],
  ["publish-py", "build-binaries"],
  ["publish-csharp", "build-binaries"],
  ["publish-packagist", "build-binaries"],
];

const allowedLiveSuiteCallers = [
  "_shadow-live-sdk.yml",
  "benchmark-sdks.yml",
];

function read(repo, rel) {
  return readFileSync(join(repo, rel), "utf8");
}

function exists(repo, rel) {
  return existsSync(join(repo, rel));
}

function workflowFiles(repo) {
  const dir = join(repo, ".github", "workflows");
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter((name) => name.endsWith(".yml") || name.endsWith(".yaml"))
    .sort();
}

function extractJobs(text) {
  const jobs = [];
  let inJobs = false;
  for (const line of text.split(/\r?\n/)) {
    if (/^jobs:\s*$/.test(line)) {
      inJobs = true;
      continue;
    }
    if (inJobs && /^[A-Za-z_][A-Za-z0-9_-]*:\s*$/.test(line)) {
      break;
    }
    const match = inJobs ? line.match(/^  ([A-Za-z0-9_-]+):\s*(?:#.*)?$/) : null;
    if (match) jobs.push(match[1]);
  }
  return jobs;
}

function workflowJobBlock(text, job) {
  const match = text.match(new RegExp(`^  ${job}:\\n([\\s\\S]*?)(?=^  [A-Za-z0-9_-]+:\\n|(?![\\s\\S]))`, "m"));
  return match ? match[1] : "";
}

function workflowJobName(text, job) {
  const block = workflowJobBlock(text, job);
  const match = block.match(/^\s+name:\s*(.+?)\s*$/m);
  return match ? match[1] : job;
}

function workflowJobNeeds(text, job) {
  const block = workflowJobBlock(text, job);
  const line = block.match(/^\s+needs:\s*(.+?)\s*$/m);
  if (!line) return [];
  return [...line[1].matchAll(/[A-Za-z0-9_-]+/g)].map((match) => match[0]);
}

function workflowJobUses(text, job) {
  const block = workflowJobBlock(text, job);
  const match = block.match(/^\s+uses:\s*(.+?)\s*$/m);
  return match ? match[1].trim() : "";
}

function workflowJobTimeoutMinutes(text, job) {
  const block = workflowJobBlock(text, job);
  const match = block.match(/^\s+timeout-minutes:\s*(\d+)\s*$/m);
  return match ? Number(match[1]) : null;
}

function requiredCheckNamesFromArchitecture(text) {
  const marker = "Required reported check names (branch protection):";
  const start = text.indexOf(marker);
  if (start < 0) return [];
  const tail = text.slice(start);
  const end = tail.search(/\n\s*\n/);
  const block = end >= 0 ? tail.slice(0, end) : tail;
  return [...block.matchAll(/`([^`]+)`/g)].map((match) => match[1]);
}

function pullRequestBlock(text) {
  const lines = text.split(/\r?\n/);
  const start = lines.findIndex((line) => /^  pull_request:\s*(?:#.*)?$/.test(line));
  if (start < 0) return "";
  const block = [lines[start]];
  for (let index = start + 1; index < lines.length; index += 1) {
    const line = lines[index];
    if (/^\S/.test(line) || /^  [A-Za-z_][A-Za-z0-9_-]*:\s*(?:#.*)?$/.test(line)) break;
    block.push(line);
  }
  return block.join("\n");
}

function occurrenceFiles(repo, needle, files = workflowFiles(repo)) {
  return files.filter((name) => read(repo, join(".github", "workflows", name)).includes(needle));
}

function workflowUseOwners(repo, target) {
  return workflowInventory(repo).workflows
    .filter((workflow) => workflow.uses.includes(target))
    .map((workflow) => workflow.name);
}

function workflowLineOwners(repo, pattern, files = workflowFiles(repo)) {
  const owners = [];
  for (const name of files) {
    const text = read(repo, join(".github", "workflows", name));
    const found = text
      .split(/\r?\n/)
      .some((line) => !line.trimStart().startsWith("#") && pattern.test(line));
    if (found) owners.push(name);
  }
  return owners;
}

function workflowInventory(repo) {
  const files = workflowFiles(repo);
  const workflows = files.map((name) => {
    const rel = join(".github", "workflows", name);
    const text = read(repo, rel);
    return {
      name,
      jobs: extractJobs(text),
      uses: [...text.matchAll(/uses:\s*([^\s#]+)/g)].map((match) => match[1]),
      cargoBuilds: (text.match(/cargo build --locked/g) || []).length,
      cargoTests: (text.match(/cargo test --locked/g) || []).length,
    };
  });

  return {
    workflows,
    workflowCount: workflows.length,
    actionCount: existsSync(join(repo, ".github", "actions"))
      ? readdirSync(join(repo, ".github", "actions"), { withFileTypes: true }).filter((entry) => entry.isDirectory()).length
      : 0,
    ciJobs: workflows.find((workflow) => workflow.name === "ci.yml")?.jobs || [],
    releaseJobs: workflows.find((workflow) => workflow.name === "release.yml")?.jobs || [],
  };
}

function addMissing(errors, present, expected, label) {
  for (const item of expected) {
    if (!present.includes(item)) errors.push(`${label} missing ${item}`);
  }
}

function checkRepo(repo = ROOT) {
  const errors = [];
  const files = workflowFiles(repo);
  const inventory = workflowInventory(repo);

  addMissing(errors, files, requiredWorkflows, "workflow");
  if (files.includes("feature-matrix.yml")) {
    errors.push("feature-matrix.yml must stay folded into ci.yml, not run as a duplicate workflow");
  }

  for (const action of requiredActions) {
    if (!exists(repo, join(".github", "actions", action, "action.yml"))) {
      errors.push(`shared action missing .github/actions/${action}/action.yml`);
    }
  }

  addMissing(errors, inventory.ciJobs, requiredCiJobs, "ci.yml job");
  addMissing(errors, inventory.releaseJobs, releaseFanoutJobs, "release.yml job");

  const releaseText = exists(repo, ".github/workflows/release.yml") ? read(repo, ".github/workflows/release.yml") : "";
  if (!/^\s{4}tags:\s*$/m.test(releaseText) || !releaseText.includes("'v*.*.*'")) {
    errors.push("release.yml must be the v*.*.* tag entrypoint");
  }
  for (const workflow of releaseLeafWorkflows) {
    const text = exists(repo, join(".github", "workflows", workflow)) ? read(repo, join(".github", "workflows", workflow)) : "";
    if (!text.includes("workflow_call:")) errors.push(`${workflow} must be workflow_call reusable`);
    if (/^\s{4}tags:\s*$/m.test(text)) errors.push(`${workflow} must not have its own tag trigger`);
    if (!text.includes("./.github/actions/version-guard")) errors.push(`${workflow} must use the shared version-guard`);
  }
  if (!releaseText.includes("./.github/workflows/release-binaries.yml")) {
    errors.push("release.yml must call release-binaries.yml as the sole binary producer");
  }
  for (const [job, needed] of releaseGraphEdges) {
    if (!workflowJobNeeds(releaseText, job).includes(needed)) {
      errors.push(`release.yml must preserve release graph edge ${job} needs ${needed}`);
    }
  }
  for (const [job, workflow] of Object.entries(releaseJobWorkflow)) {
    const expectedUse = `./.github/workflows/${workflow}`;
    const actualUse = workflowJobUses(releaseText, job);
    if (actualUse !== expectedUse) {
      errors.push(`release.yml job ${job} must use ${expectedUse}; found ${actualUse || "none"}`);
    }
    if (!workflowJobBlock(releaseText, job).includes("secrets: inherit")) {
      errors.push(`release.yml job ${job} must pass secrets: inherit to its reusable workflow`);
    }
  }

  const deployOwners = occurrenceFiles(repo, "actions/deploy-pages@");
  if (deployOwners.length !== 1 || deployOwners[0] !== "pages.yml") {
    errors.push(`Pages deploy must be single-owned by pages.yml; found ${deployOwners.join(", ") || "none"}`);
  }
  const cleanupOwners = occurrenceFiles(repo, "actions/delete-package-versions@");
  if (cleanupOwners.some((name) => name !== "cleanup-packages.yml") || cleanupOwners.length === 0) {
    errors.push(`GHCR cleanup must be single-owned by cleanup-packages.yml; found ${cleanupOwners.join(", ") || "none"}`);
  }

  const benchmarkText = exists(repo, ".github/workflows/benchmark-sdks.yml") ? read(repo, ".github/workflows/benchmark-sdks.yml") : "";
  if (!benchmarkText.includes('workflows: ["Release"]') || !benchmarkText.includes("./.github/workflows/_live-sdk-suite.yml")) {
    errors.push("benchmark-sdks.yml must be Release-triggered and call _live-sdk-suite.yml");
  }
  if (
    !benchmarkText.includes("github.event.workflow_run.conclusion == 'success'") ||
    !benchmarkText.includes("startsWith(github.event.workflow_run.head_branch, 'v')")
  ) {
    errors.push("benchmark-sdks.yml must run post-release benchmarks only after a successful top-level Release v* tag run");
  }
  const liveSuiteOwners = workflowUseOwners(repo, "./.github/workflows/_live-sdk-suite.yml");
  const unexpectedLiveSuiteOwners = liveSuiteOwners.filter((owner) => !allowedLiveSuiteCallers.includes(owner));
  const missingLiveSuiteOwners = allowedLiveSuiteCallers.filter((owner) => !liveSuiteOwners.includes(owner));
  if (unexpectedLiveSuiteOwners.length || missingLiveSuiteOwners.length) {
    errors.push(
      `_live-sdk-suite.yml callers must be exactly ${allowedLiveSuiteCallers.join(", ")}; found ${liveSuiteOwners.join(", ") || "none"}`,
    );
  }
  const pagesText = exists(repo, ".github/workflows/pages.yml") ? read(repo, ".github/workflows/pages.yml") : "";
  if (!pagesText.includes('workflows: ["Benchmark · SDKs"]')) {
    errors.push("pages.yml must consume the Benchmark · SDKs workflow_run handoff");
  }
  const pagesConcurrencyOwners = workflowLineOwners(repo, /^\s+group:\s*pages\s*$/);
  if (pagesConcurrencyOwners.length !== 1 || pagesConcurrencyOwners[0] !== "pages.yml") {
    errors.push(`Pages concurrency group must be single-owned by pages.yml; found ${pagesConcurrencyOwners.join(", ") || "none"}`);
  }
  const pagesPermissionOwners = workflowLineOwners(repo, /^\s+pages:\s*write\s*$/);
  if (pagesPermissionOwners.length !== 1 || pagesPermissionOwners[0] !== "pages.yml") {
    errors.push(`Pages write permission must be single-owned by pages.yml; found ${pagesPermissionOwners.join(", ") || "none"}`);
  }

  const liveSuiteText = exists(repo, ".github/workflows/_live-sdk-suite.yml") ? read(repo, ".github/workflows/_live-sdk-suite.yml") : "";
  for (const primitive of ["start-backends", "broker-env", "setup-sdk-toolchains"]) {
    if (!liveSuiteText.includes(`./.github/actions/${primitive}`)) {
      errors.push(`_live-sdk-suite.yml must use ${primitive}`);
    }
  }
  const ciText = exists(repo, ".github/workflows/ci.yml") ? read(repo, ".github/workflows/ci.yml") : "";
  if (!ciText.includes("./.github/actions/launch-broker")) {
    errors.push("ci.yml smoke path must use launch-broker");
  }
  const requiredGraphEdges = [
    ["build-broker", "quick-gate"],
    ["smoke", "build-broker"],
    ["scaffold-compiles", "build-broker"],
  ];
  for (const [job, needed] of requiredGraphEdges) {
    if (!workflowJobNeeds(ciText, job).includes(needed)) {
      errors.push(`ci.yml must preserve budget graph edge ${job} needs ${needed}`);
    }
  }
  for (const job of dependencyFreePrJobs) {
    const needs = workflowJobNeeds(ciText, job);
    if (needs.length) {
      errors.push(`ci.yml dependency-free PR job must not declare needs: ${job} has ${needs.join(", ")}`);
    }
  }
  const ciPrBlock = pullRequestBlock(ciText);
  if (!ciPrBlock || ciPrBlock.includes("paths:")) {
    errors.push("ci.yml pull_request trigger must exist and must not be paths-filtered because required checks would stall");
  }
  const prBuilds = inventory.ciJobs
    .map((job) => ({ job, block: workflowJobBlock(ciText, job) }))
    .filter(({ block }) => !block.includes("if: github.event_name == 'push'"))
    .reduce((count, { block }) => count + (block.match(/cargo build --locked --bin udb/g) || []).length, 0);
  if (prBuilds !== 1) {
    errors.push(`PR broker compile count must be exactly 1 debug build; found ${prBuilds}`);
  }

  const architectureText = exists(repo, "docs/ci-architecture.md") ? read(repo, "docs/ci-architecture.md") : "";
  for (const claim of pipelineBudgetClaims) {
    if (!architectureText.includes(claim)) errors.push(`docs/ci-architecture.md missing budget/measurement claim: ${claim}`);
  }
  const expectedRequiredCheckNames = [];
  for (const job of requiredPrCheckJobs) {
    if (!inventory.ciJobs.includes(job)) {
      errors.push(`required PR check job missing from ci.yml: ${job}`);
      continue;
    }
    const block = workflowJobBlock(ciText, job);
    if (block.includes("if: github.event_name == 'push'")) {
      errors.push(`required PR check job must not be push-only: ${job}`);
    }
    const timeout = workflowJobTimeoutMinutes(ciText, job);
    if (timeout === null) {
      errors.push(`required PR check job must declare timeout-minutes: ${job}`);
    } else if (timeout > prJobTimeoutCeilings[job]) {
      errors.push(
        `required PR check job timeout exceeds ceiling: ${job} has ${timeout}, max ${prJobTimeoutCeilings[job]}`,
      );
    }
    const reported = workflowJobName(ciText, job);
    expectedRequiredCheckNames.push(reported);
    if (!architectureText.includes(`\`${reported}\``)) {
      errors.push(`docs/ci-architecture.md must record reported required check name \`${reported}\``);
    }
  }
  const buildBrokerTimeout = workflowJobTimeoutMinutes(ciText, "build-broker");
  if (buildBrokerTimeout === null) {
    errors.push("PR broker artifact producer must declare timeout-minutes: build-broker");
  } else if (buildBrokerTimeout > prJobTimeoutCeilings["build-broker"]) {
    errors.push(
      `PR broker artifact producer timeout exceeds ceiling: build-broker has ${buildBrokerTimeout}, max ${prJobTimeoutCeilings["build-broker"]}`,
    );
  }
  const documentedRequiredCheckNames = requiredCheckNamesFromArchitecture(architectureText);
  if (documentedRequiredCheckNames.length === 0) {
    errors.push("docs/ci-architecture.md must record exact branch-protection required check names");
  }
  const duplicateDocumented = documentedRequiredCheckNames.filter(
    (name, index) => documentedRequiredCheckNames.indexOf(name) !== index,
  );
  for (const name of [...new Set(duplicateDocumented)]) {
    errors.push(`docs/ci-architecture.md duplicates required PR check name \`${name}\``);
  }
  const missingRequired = expectedRequiredCheckNames.filter((name) => !documentedRequiredCheckNames.includes(name));
  const staleRequired = documentedRequiredCheckNames.filter((name) => !expectedRequiredCheckNames.includes(name));
  for (const name of missingRequired) {
    errors.push(`docs/ci-architecture.md missing required PR check name \`${name}\``);
  }
  for (const name of staleRequired) {
    errors.push(`docs/ci-architecture.md lists stale required PR check name \`${name}\``);
  }

  return { errors, inventory };
}

function runSelftest() {
  const root = mkdtempSync(join(tmpdir(), "udb-ci-inventory-"));
  try {
    const workflows = join(root, ".github", "workflows");
    const actions = join(root, ".github", "actions");
    const docs = join(root, "docs");
    mkdirSync(workflows, { recursive: true });
    mkdirSync(actions, { recursive: true });
    mkdirSync(docs, { recursive: true });
    for (const action of requiredActions) {
      mkdirSync(join(actions, action), { recursive: true });
      writeFileSync(join(actions, action, "action.yml"), "name: fixture\nruns:\n  using: composite\n  steps: []\n");
    }
    const workflow = (name, body) => writeFileSync(join(workflows, name), body);
    const ciGood = `on:\n  push:\n    branches: [main]\n  pull_request:\n    branches: [main]\njobs:\n${requiredCiJobs
      .map((job) => `  ${job}:\n    name: ${job === "buf" ? "Proto (buf)" : job === "versions" ? "Version consistency" : job === "php-sdk" ? "PHP SDK (pest)" : job === "go-sdk" ? "Go SDK (vet + build)" : job === "ts-sdk" ? "TypeScript SDK (typecheck + build)" : job === "python-sdk" ? "Python SDK (pytest)" : job === "csharp-sdk" ? "C# SDK (build)" : job === "java-sdk" ? "Java SDK (compile)" : job === "sdk-conformance" ? "SDK conformance (all languages)" : job === "scaffold-compiles" ? "Scaffold examples compile (six SDKs)" : job}\n${job === "build-broker" ? "    needs: quick-gate\n" : job === "smoke" || job === "scaffold-compiles" ? "    needs: build-broker\n" : ""}    runs-on: ubuntu-latest\n    timeout-minutes: 5\n    steps:\n      - run: ${job === "build-broker" ? "cargo build --locked --bin udb" : `echo ${job}`}`)
      .join("\n")}\n      - uses: ./.github/actions/launch-broker\n`;
    workflow("ci.yml", ciGood);
    const architectureGood = `# CI Architecture

| Pipeline | Event | Purpose | Budget | Gates merge? |
| --- | --- | --- | --- | --- |
| PR gate | pull_request | fast | ≤ ~8 min | YES |
| Integration | push: main | full | ≤ ~30 min | no |
| Release | tag | publish | ≤ ~40 min | release-blocking |

Required reported check names (branch protection): ${requiredPrCheckJobs
        .map((job) => `\`${job === "buf" ? "Proto (buf)" : job === "versions" ? "Version consistency" : job === "php-sdk" ? "PHP SDK (pest)" : job === "go-sdk" ? "Go SDK (vet + build)" : job === "ts-sdk" ? "TypeScript SDK (typecheck + build)" : job === "python-sdk" ? "Python SDK (pytest)" : job === "csharp-sdk" ? "C# SDK (build)" : job === "java-sdk" ? "Java SDK (compile)" : job === "sdk-conformance" ? "SDK conformance (all languages)" : job === "scaffold-compiles" ? "Scaffold examples compile (six SDKs)" : job}\``)
        .join(", ")}.

## Budget Measurement Ledger

Required PR check jobs all declare timeout-minutes.
Required PR timeout ceilings are enforced by scripts/ci-inventory.mjs.
Critical PR artifact path: quick-gate -> build-broker -> {smoke, scaffold-compiles}.
Cheap PR checks stay dependency-free and start at t=0.
Timeout ceilings are source guardrails, not runner wall-clock evidence.
PR budget evidence measures the branch-protection-required lane.
PR broker compile count: 1 debug build in build-broker.
Release graph: ci-green -> version-guard -> build-binaries -> parallel publishers.
Post-release benchmark runs only after top-level Release success on a v* tag.
Runner wall-clock evidence is still required before marking 15.A.5 done.
`;
    writeFileSync(join(docs, "ci-architecture.md"), architectureGood);
    workflow(
      "release.yml",
      `on:\n  push:\n    tags:\n      - 'v*.*.*'\njobs:\n${releaseFanoutJobs
        .map((job) => {
          const uses = releaseJobWorkflow[job] || "noop.yml";
          const needs =
            job === "version-guard"
              ? "    needs: ci-green\n"
              : job === "build-binaries"
                ? "    needs: version-guard\n"
                : job.startsWith("publish-")
                  ? "    needs: build-binaries\n"
                  : "";
          const secrets = releaseJobWorkflow[job] ? "    secrets: inherit\n" : "";
          return `  ${job}:\n${needs}    uses: ./.github/workflows/${uses}\n${secrets}`.trimEnd();
        })
        .join("\n")}\n`,
    );
    for (const leaf of releaseLeafWorkflows) {
      workflow(leaf, "on:\n  workflow_call:\njobs:\n  publish:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: ./.github/actions/version-guard\n");
    }
    workflow(
      "benchmark-sdks.yml",
      "on:\n  workflow_run:\n    workflows: [\"Release\"]\njobs:\n  benchmark:\n    if: github.event.workflow_run.conclusion == 'success' && startsWith(github.event.workflow_run.head_branch, 'v')\n    uses: ./.github/workflows/_live-sdk-suite.yml\n",
    );
    workflow("_shadow-live-sdk.yml", "on:\n  workflow_dispatch:\njobs:\n  shadow:\n    uses: ./.github/workflows/_live-sdk-suite.yml\n");
    workflow("pages.yml", 'on:\n  workflow_run:\n    workflows: ["Benchmark · SDKs"]\npermissions:\n  pages: write\nconcurrency:\n  group: pages\n  cancel-in-progress: false\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/deploy-pages@v4\n');
    workflow("cleanup-packages.yml", "on:\n  workflow_run:\n    workflows: [Release]\njobs:\n  cleanup:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/delete-package-versions@v5\n");
    workflow("_live-sdk-suite.yml", "on:\n  workflow_call:\njobs:\n  live:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: ./.github/actions/start-backends\n      - uses: ./.github/actions/broker-env\n      - uses: ./.github/actions/setup-sdk-toolchains\n");
    workflow("_selftest.yml", "on:\n  workflow_dispatch:\njobs:\n  selftest:\n    runs-on: ubuntu-latest\n");
    workflow("publish-skill.yml", "on:\n  workflow_dispatch:\njobs:\n  validate:\n    runs-on: ubuntu-latest\n");
    workflow("lint-workflows.yml", "on:\n  pull_request:\njobs:\n  lint:\n    runs-on: ubuntu-latest\n");

    const good = checkRepo(root);
    if (good.errors.length) throw new Error(`good inventory fixture failed: ${good.errors.join("; ")}`);

    workflow("ci.yml", ciGood.replace("  smoke:\n    name: smoke\n    needs: build-broker\n", "  smoke:\n    name: smoke\n"));
    const missingBudgetEdge = checkRepo(root);
    if (!missingBudgetEdge.errors.some((error) => error.includes("budget graph edge smoke needs build-broker"))) {
      throw new Error("PR budget graph regression was not caught");
    }
    workflow("ci.yml", ciGood.replace("  smoke:\n    name: smoke\n    needs: build-broker\n    runs-on: ubuntu-latest\n    timeout-minutes: 5\n", "  smoke:\n    name: smoke\n    needs: build-broker\n    runs-on: ubuntu-latest\n"));
    const missingTimeout = checkRepo(root);
    if (!missingTimeout.errors.some((error) => error.includes("required PR check job must declare timeout-minutes: smoke"))) {
      throw new Error("required PR timeout regression was not caught");
    }
    workflow("ci.yml", ciGood);

    workflow("ci.yml", ciGood.replace("  smoke:\n    name: smoke\n    needs: build-broker\n    runs-on: ubuntu-latest\n    timeout-minutes: 5\n", "  smoke:\n    name: smoke\n    needs: build-broker\n    runs-on: ubuntu-latest\n    timeout-minutes: 120\n"));
    const excessiveTimeout = checkRepo(root);
    if (!excessiveTimeout.errors.some((error) => error.includes("required PR check job timeout exceeds ceiling: smoke"))) {
      throw new Error("required PR timeout ceiling regression was not caught");
    }
    workflow("ci.yml", ciGood);

    workflow("ci.yml", ciGood.replace("  buf:\n    name: Proto (buf)\n", "  buf:\n    name: Proto (buf)\n    needs: quick-gate\n"));
    const serializedCheapJob = checkRepo(root);
    if (!serializedCheapJob.errors.some((error) => error.includes("dependency-free PR job must not declare needs: buf"))) {
      throw new Error("cheap PR job serialization regression was not caught");
    }
    workflow("ci.yml", ciGood);

    workflow("ci.yml", `${ciGood}  sdk-live:\n    uses: ./.github/workflows/_live-sdk-suite.yml\n`);
    const duplicateLiveSuiteOwner = checkRepo(root);
    if (!duplicateLiveSuiteOwner.errors.some((error) => error.includes("_live-sdk-suite.yml callers must be exactly"))) {
      throw new Error("duplicate live SDK suite owner regression was not caught");
    }
    workflow("ci.yml", ciGood);

    workflow("benchmark-sdks.yml", 'on:\n  workflow_run:\n    workflows: ["Release"]\npermissions:\n  contents: read\n  pages: write\nconcurrency:\n  group: pages\njobs:\n  benchmark:\n    uses: ./.github/workflows/_live-sdk-suite.yml\n');
    const duplicatePagesOwner = checkRepo(root);
    if (!duplicatePagesOwner.errors.some((error) => error.includes("Pages concurrency group must be single-owned"))) {
      throw new Error("duplicate Pages concurrency owner regression was not caught");
    }
    if (!duplicatePagesOwner.errors.some((error) => error.includes("Pages write permission must be single-owned"))) {
      throw new Error("duplicate Pages permission owner regression was not caught");
    }
    workflow(
      "benchmark-sdks.yml",
      "on:\n  workflow_run:\n    workflows: [\"Release\"]\njobs:\n  benchmark:\n    if: github.event.workflow_run.conclusion == 'success' && startsWith(github.event.workflow_run.head_branch, 'v')\n    uses: ./.github/workflows/_live-sdk-suite.yml\n",
    );

    workflow("feature-matrix.yml", "on:\n  push:\njobs:\n  duplicate:\n    runs-on: ubuntu-latest\n");
    const duplicateFeature = checkRepo(root);
    if (!duplicateFeature.errors.some((error) => error.includes("feature-matrix.yml must stay folded"))) {
      throw new Error("feature-matrix duplicate regression was not caught");
    }
    rmSync(join(workflows, "feature-matrix.yml"));

    workflow("release-docker.yml", "on:\n  push:\n    tags:\n      - 'v*.*.*'\n  workflow_call:\njobs:\n  publish:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: ./.github/actions/version-guard\n");
    const duplicateRelease = checkRepo(root);
    if (!duplicateRelease.errors.some((error) => error.includes("must not have its own tag trigger"))) {
      throw new Error("release leaf tag-trigger regression was not caught");
    }
    workflow("release-docker.yml", "on:\n  workflow_call:\njobs:\n  publish:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: ./.github/actions/version-guard\n");

    workflow(
      "release.yml",
      `on:\n  push:\n    tags:\n      - 'v*.*.*'\njobs:\n${releaseFanoutJobs
        .map((job) => {
          const uses = releaseJobWorkflow[job] || "noop.yml";
          const needs =
            job === "version-guard"
              ? "    needs: ci-green\n"
              : job === "build-binaries"
                ? "    needs: version-guard\n"
                : job.startsWith("publish-") && job !== "publish-docker"
                  ? "    needs: build-binaries\n"
                  : "";
          const secrets = releaseJobWorkflow[job] ? "    secrets: inherit\n" : "";
          return `  ${job}:\n${needs}    uses: ./.github/workflows/${uses}\n${secrets}`.trimEnd();
        })
        .join("\n")}\n`,
    );
    const missingReleaseEdge = checkRepo(root);
    if (!missingReleaseEdge.errors.some((error) => error.includes("release graph edge publish-docker needs build-binaries"))) {
      throw new Error("release fan-out dependency regression was not caught");
    }
    workflow(
      "release.yml",
      `on:\n  push:\n    tags:\n      - 'v*.*.*'\njobs:\n${releaseFanoutJobs
        .map((job) => {
          const uses = releaseJobWorkflow[job] || "noop.yml";
          const needs =
            job === "version-guard"
              ? "    needs: ci-green\n"
              : job === "build-binaries"
                ? "    needs: version-guard\n"
                : job.startsWith("publish-")
                  ? "    needs: build-binaries\n"
                  : "";
          const secrets = releaseJobWorkflow[job] ? "    secrets: inherit\n" : "";
          return `  ${job}:\n${needs}    uses: ./.github/workflows/${uses}\n${secrets}`.trimEnd();
        })
        .join("\n")}\n`,
    );

    writeFileSync(
      join(docs, "ci-architecture.md"),
      architectureGood.replace(
        "Required reported check names (branch protection):",
        "Required reported check names (branch protection): `stale-required-check`,",
      ),
    );
    const staleRequiredCheck = checkRepo(root);
    if (!staleRequiredCheck.errors.some((error) => error.includes("stale required PR check name"))) {
      throw new Error("stale required PR check name regression was not caught");
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
  console.log("CI inventory selftest passed");
}

if (process.argv.includes("--selftest")) {
  runSelftest();
  process.exit(0);
}

const { errors, inventory } = checkRepo(process.cwd());
if (process.argv.includes("--json")) {
  console.log(JSON.stringify({ ok: errors.length === 0, errors, inventory }, null, 2));
}
if (errors.length) {
  console.error(`CI inventory check failed with ${errors.length} issue(s):`);
  for (const error of errors) console.error(`- ${error}`);
  process.exit(1);
}
if (!process.argv.includes("--json")) {
  console.log(
    `CI inventory check passed: ${inventory.workflowCount} workflows, ${inventory.actionCount} shared actions, ${inventory.ciJobs.length} CI jobs.`,
  );
}
