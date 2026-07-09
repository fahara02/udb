#!/usr/bin/env node
// Verify GitHub Actions runner evidence for Chapter 15 closeout.
//
// Source guards prove the workflow graph. This audit proves real completed runs:
// actionlint/lint success, PR CI budget, integration CI budget, release budget,
// release-binary dry-run budget, and required job parity for each evidence lane.

import { existsSync, mkdirSync, mkdtempSync, rmSync, statSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import https from "node:https";
import { execFileSync } from "node:child_process";

const DEFAULT_BUDGETS = {
  pr: 8,
  integration: 30,
  release: 40,
  releaseDryRun: 120,
  benchmark: 120,
  pages: 20,
  lint: 10,
  branchProtection: 10,
  idempotencyServed: 15,
  errorDetailServed: 15,
  retrySafeServed: 15,
  restGateway: 15,
};
const MAX_BUDGETS = { ...DEFAULT_BUDGETS };
const DEFAULT_MAX_EVIDENCE_AGE_DAYS = 14;
const MAX_EVIDENCE_AGE_DAYS = DEFAULT_MAX_EVIDENCE_AGE_DAYS;
const GITHUB_API_REQUEST_TIMEOUT_MS = 30 * 1000;
const MAX_GITHUB_API_RESPONSE_BYTES = 4 * 1024 * 1024;
const MAX_FIXTURE_BYTES = 1 * 1024 * 1024;
const MAX_GITHUB_RUN_JOBS = 500;
const MAX_GITHUB_JOBS_PAGE_SIZE = 100;
const MAX_GITHUB_WORKFLOW_RUN_CANDIDATES = 100;
const ALL_EVIDENCE_MODE = "--all-evidence";

const WORKFLOWS = {
  pr: "ci.yml",
  integration: "ci.yml",
  release: "release.yml",
  releaseDryRun: "release-binaries.yml",
  benchmark: "benchmark-sdks.yml",
  pages: "pages.yml",
  lint: "lint-workflows.yml",
  branchProtection: "branch-protection-audit.yml",
  idempotencyServed: "idempotency-served-smoke.yml",
  errorDetailServed: "error-detail-served-smoke.yml",
  retrySafeServed: "retry-safe-served-smoke.yml",
  restGateway: "rest-gateway-smoke.yml",
};

const CI_EVIDENCE_LANES = [
  "lint",
  "pr",
  "integration",
  "release",
  "releaseDryRun",
  "benchmark",
  "pages",
  "branchProtection",
];
const LINT_EVIDENCE_EVENTS = ["workflow_dispatch", "pull_request", "push"];
const DEFAULT_INTEGRATION_BRANCH = "main";
const RELEASE_TAG_PATTERN = /^v\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/;
const GIT_SHA_PATTERN = /^[0-9a-f]{40}$/;
const RUN_ID_PATTERN = /^[1-9]\d*$/;
const POSITIVE_DECIMAL_PATTERN = /^(?:[1-9]\d*(?:\.\d+)?|0\.\d*[1-9]\d*)$/;
const ACTIONS_TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/;
const GITHUB_ACTIONS_RUN_URL_PATTERN = /^https:\/\/github\.com\/([A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+)\/actions\/runs\/([1-9]\d*)$/;

const PR_REQUIRED_JOBS = [
  "quick-gate",
  "Proto (buf)",
  "Version consistency",
  "PHP SDK (pest)",
  "Go SDK (vet + build)",
  "TypeScript SDK (typecheck + build)",
  "Python SDK (pytest)",
  "C# SDK (build)",
  "Java SDK (compile)",
  "SDK conformance (all languages)",
  "smoke",
  "Scaffold examples compile (six SDKs)",
];

const PR_ADVISORY_JOBS = [
  "Rust (ubuntu-latest)",
  "Rust (windows-latest)",
  "Slim build (postgres-only)",
  "Feature check (all-features)",
  "Supply chain policy",
  "Markdown local links + readiness artifacts",
];

const PR_EVIDENCE_JOBS = [...PR_REQUIRED_JOBS, ...PR_ADVISORY_JOBS];

const INTEGRATION_REQUIRED_JOBS = [
  "quick-gate",
  "Rust (ubuntu-latest)",
  "Rust (windows-latest)",
  "build-broker",
  "smoke",
  "Auth binary (linux-amd64)",
  "Auth binary (windows-amd64)",
  "Auth binary (darwin-arm64)",
  "Auth binary (darwin-amd64)",
  "Slim build (postgres-only)",
  "Plugin feature (qdrant)",
  "Plugin feature (s3)",
  "Plugin feature (mongodb)",
  "Plugin feature (mongodb-native)",
  "Plugin feature (neo4j)",
  "Plugin feature (clickhouse)",
  "Plugin feature (redis)",
  "Plugin feature (elasticsearch)",
  "Plugin feature (memcached)",
  "Plugin feature (mssql)",
  "Plugin feature (weaviate)",
  "Plugin feature (pinecone)",
  "Plugin feature (cassandra)",
  "Plugin feature (azureblob)",
  "Plugin feature (gcs)",
  "Plugin feature (kafka)",
  "Plugin feature (otel)",
  "Plugin feature (runtime-logging)",
  "Optimized (SIMD accel)",
  "AArch64 scalar",
  "Supply chain policy",
  "Proto (buf)",
  "PHP SDK (pest)",
  "Go SDK (vet + build)",
  "TypeScript SDK (typecheck + build)",
  "Python SDK (pytest)",
  "C# SDK (build)",
  "Java SDK (compile)",
  "SDK conformance (all languages)",
  "Scaffold examples compile (six SDKs)",
  "Version consistency",
  "Markdown local links + readiness artifacts",
  "Native services + canonical stores (live)",
];

const REQUIRED_JOBS = {
  lint: ["actionlint"],
  pr: PR_REQUIRED_JOBS,
  integration: INTEGRATION_REQUIRED_JOBS,
  release: [
    "ci-green",
    "version-guard",
    "build-binaries",
    "publish-crates",
    "publish-docker",
    "publish-ts",
    "publish-py",
    "publish-csharp",
    "publish-packagist",
  ],
  releaseDryRun: [
    "Version guard",
    "Vendored ffmpeg guard",
    "build (udb-linux-amd64)",
    "build (udb-windows-amd64.exe)",
    "build (udb-darwin-arm64)",
    "build (udb-darwin-amd64)",
    "build (udb-linux-amd64-full)",
  ],
  benchmark: ["Release binary + SDK live benchmarks / Live SDK benchmark"],
  pages: ["build", "deploy"],
  branchProtection: ["Branch protection required checks match docs"],
  idempotencyServed: ["DataBroker idempotency served replay proof"],
  errorDetailServed: ["ErrorDetail served transport proof"],
  retrySafeServed: ["Retry-safe mutation metadata served proof"],
  restGateway: ["REST boundary content/status proof"],
};

const SERVED_SMOKE_AUDITS = {
  idempotencyServed: {
    mode: "--idempotency-served-smoke",
    runIdArg: "--idempotency-run-id",
    label: "idempotency served replay",
  },
  errorDetailServed: {
    mode: "--error-detail-served-smoke",
    runIdArg: "--error-detail-run-id",
    label: "ErrorDetail served transport",
  },
  retrySafeServed: {
    mode: "--retry-safe-served-smoke",
    runIdArg: "--retry-safe-run-id",
    label: "retry-safe served replay",
  },
  restGateway: {
    mode: "--rest-gateway-smoke",
    runIdArg: "--rest-gateway-run-id",
    label: "REST gateway boundary",
  },
};

const CI_RUN_ID_ARGS = [
  "--lint-run-id",
  "--pr-run-id",
  "--integration-run-id",
  "--release-run-id",
  "--release-dry-run-id",
  "--benchmark-run-id",
  "--pages-run-id",
  "--branch-protection-run-id",
];

const CI_BUDGET_ARGS = [
  "--lint-budget-minutes",
  "--pr-budget-minutes",
  "--integration-budget-minutes",
  "--release-budget-minutes",
  "--release-dry-run-budget-minutes",
  "--benchmark-budget-minutes",
  "--pages-budget-minutes",
  "--branch-protection-budget-minutes",
];

const SERVED_BUDGET_ARGS = {
  idempotencyServed: "--idempotency-served-budget-minutes",
  errorDetailServed: "--error-detail-served-budget-minutes",
  retrySafeServed: "--retry-safe-served-budget-minutes",
  restGateway: "--rest-gateway-budget-minutes",
};

const VALUE_ARGS = new Set([
  "--repo",
  "--branch",
  "--release-tag",
  "--fixture",
  "--max-evidence-age-days",
  ...CI_RUN_ID_ARGS,
  ...CI_BUDGET_ARGS,
  ...Object.values(SERVED_BUDGET_ARGS),
  ...Object.values(SERVED_SMOKE_AUDITS).map((audit) => audit.runIdArg),
]);

const FLAG_ARGS = new Set([
  "--selftest",
  ALL_EVIDENCE_MODE,
  ...Object.values(SERVED_SMOKE_AUDITS).map((audit) => audit.mode),
]);

function requestedServedAuditKeys(args) {
  if (args.includes(ALL_EVIDENCE_MODE)) {
    return Object.keys(SERVED_SMOKE_AUDITS);
  }
  return Object.entries(SERVED_SMOKE_AUDITS)
    .filter(([_auditKey, audit]) => args.includes(audit.mode))
    .map(([auditKey]) => auditKey);
}

function assertKnownArgs(args) {
  for (let index = 0; index < args.length; index += 1) {
    const arg = String(args[index]);
    if (VALUE_ARGS.has(arg)) {
      if (index + 1 >= args.length || String(args[index + 1]).startsWith("--")) {
        throw new Error(`${arg} requires a value`);
      }
      index += 1;
      continue;
    }
    if (FLAG_ARGS.has(arg)) continue;
    if (arg.startsWith("--")) {
      throw new Error(`unknown runner evidence argument ${arg}`);
    }
    throw new Error(`unexpected runner evidence argument ${arg}`);
  }
}

function assertNoUnusedEvidenceOverrides(args, servedAuditKeys) {
  const allEvidence = args.includes(ALL_EVIDENCE_MODE);
  if (!allEvidence) {
    for (const runIdArg of CI_RUN_ID_ARGS) {
      if (args.includes(runIdArg)) {
        throw new Error(`${runIdArg} requires ${ALL_EVIDENCE_MODE}; otherwise the run id would not be audited`);
      }
    }
  }
  const servedOnly = servedAuditKeys.length > 0 && !allEvidence;
  if (servedOnly) {
    for (const budgetArg of CI_BUDGET_ARGS) {
      if (args.includes(budgetArg)) {
        throw new Error(`${budgetArg} requires ${ALL_EVIDENCE_MODE}; otherwise the CI budget would not be audited`);
      }
    }
    for (const option of ["--release-tag", "--fixture"]) {
      if (args.includes(option)) {
        throw new Error(`${option} requires ${ALL_EVIDENCE_MODE}; otherwise the CI evidence option would not be audited`);
      }
    }
  }
  const requested = new Set(servedAuditKeys);
  for (const [auditKey, audit] of Object.entries(SERVED_SMOKE_AUDITS)) {
    if (args.includes(audit.runIdArg) && !requested.has(auditKey)) {
      throw new Error(`${audit.runIdArg} requires ${audit.mode}; otherwise the run id would not be audited`);
    }
    const servedBudgetArg = SERVED_BUDGET_ARGS[auditKey];
    if (args.includes(servedBudgetArg) && !requested.has(auditKey)) {
      throw new Error(`${servedBudgetArg} requires ${audit.mode}; otherwise the served budget would not be audited`);
    }
  }
}

function argValue(args, name, fallback = undefined) {
  const index = args.indexOf(name);
  if (index < 0) return fallback;
  if (index + 1 >= args.length) throw new Error(`${name} requires a value`);
  return args[index + 1];
}

function numberArg(args, name, fallback) {
  const value = argValue(args, name, undefined);
  if (value === undefined) return fallback;
  const raw = String(value);
  const trimmed = raw.trim();
  if (raw !== trimmed) throw new Error(`${name} must not include surrounding whitespace`);
  if (!POSITIVE_DECIMAL_PATTERN.test(trimmed)) throw new Error(`${name} must be a positive decimal number`);
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed) || parsed <= 0) throw new Error(`${name} must be a positive number`);
  return parsed;
}

function boundedBudgetArg(args, name, fallback, max) {
  const value = numberArg(args, name, fallback);
  if (value > max) throw new Error(`${name} must be <= ${max} minutes`);
  return value;
}

function boundedMaxEvidenceAgeArg(args, name, fallback, max) {
  const value = numberArg(args, name, fallback);
  if (value > max) throw new Error(`${name} must be <= ${max} days`);
  return value;
}

function repoArg(args, name, fallback) {
  const value = argValue(args, name, fallback);
  if (value === undefined || value === "") throw new Error(`${name} or GITHUB_REPOSITORY is required`);
  const repo = String(value);
  const trimmed = repo.trim();
  if (repo !== trimmed) throw new Error(`${name} must not include surrounding whitespace`);
  if (/\s/.test(trimmed)) throw new Error(`${name} must not include whitespace`);
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(trimmed)) {
    throw new Error(`${name} must be an owner/repo repository name`);
  }
  return trimmed;
}

function optionalReleaseTagArg(args, name) {
  const value = argValue(args, name, "");
  if (value === "") return "";
  const tag = String(value);
  const trimmed = tag.trim();
  if (tag !== trimmed) throw new Error(`${name} must not include surrounding whitespace`);
  return assertReleaseTag(trimmed, name);
}

function branchArg(args, name, fallback) {
  const value = argValue(args, name, fallback);
  const branch = String(value);
  const trimmed = branch.trim();
  if (branch !== trimmed) throw new Error(`${name} must not include surrounding whitespace`);
  if (!trimmed) throw new Error(`${name} must be non-empty`);
  if (/\s/.test(trimmed)) throw new Error(`${name} must not include whitespace`);
  if (
    trimmed.includes("..") ||
    trimmed.includes("@{") ||
    trimmed.includes("\\") ||
    trimmed.includes("//") ||
    trimmed.startsWith("/") ||
    trimmed.endsWith("/")
  ) {
    throw new Error(`${name} must be a canonical branch name`);
  }
  return trimmed;
}

function optionalRunIdArg(args, name) {
  const value = argValue(args, name, "");
  if (value === "") return "";
  const runId = String(value);
  const trimmed = runId.trim();
  if (runId !== trimmed) throw new Error(`${name} must not include surrounding whitespace`);
  if (!RUN_ID_PATTERN.test(trimmed)) throw new Error(`${name} must be a positive integer run id`);
  return trimmed;
}

function parseActionsTimestampMs(value, label) {
  if (value === undefined || value === null || value === "") {
    throw new Error(`${label} is missing timestamp`);
  }
  const raw = String(value);
  const trimmed = raw.trim();
  if (raw !== trimmed) throw new Error(`${label} must not include surrounding whitespace`);
  if (!ACTIONS_TIMESTAMP_PATTERN.test(trimmed)) {
    throw new Error(`${label} must be a GitHub Actions UTC timestamp`);
  }
  const parsed = Date.parse(trimmed);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${label} has invalid timestamp ${value}`);
  }
  return parsed;
}

function durationMinutes(run) {
  const start = runStartMs(run, "budget");
  const end = runCompletedMs(run, "budget");
  if (end < start) {
    throw new Error(`run ${run.id || "(unknown)"} has invalid timestamps`);
  }
  return (end - start) / 60000;
}

function assertSuccessfulBudgetRun(run, label, budgetMinutes, { maxAgeDays, nowMs = Date.now() } = {}) {
  if (run.status !== "completed") throw new Error(`${label} run ${run.id} is not completed: ${run.status}`);
  if (run.conclusion !== "success") throw new Error(`${label} run ${run.id} did not succeed: ${run.conclusion}`);
  const minutes = durationMinutes(run);
  if (minutes > budgetMinutes) {
    throw new Error(`${label} run ${run.id} took ${minutes.toFixed(2)} min, budget ${budgetMinutes} min`);
  }
  if (maxAgeDays !== undefined) {
    const completedAt = parseActionsTimestampMs(run.completed_at || run.updated_at, `${label} run ${run.id} completion timestamp`);
    if (completedAt > nowMs + 5 * 60000) {
      throw new Error(`${label} run ${run.id} completion timestamp is in the future`);
    }
    const ageDays = (nowMs - completedAt) / 86400000;
    if (ageDays > maxAgeDays) {
      throw new Error(`${label} run ${run.id} is ${ageDays.toFixed(1)} days old, max evidence age ${maxAgeDays} days`);
    }
  }
  return minutes;
}

function assertJobSucceeded(job, label) {
  const jobName = assertJobEvidenceName(job, label);
  if (job.status !== "completed") {
    throw new Error(`${label} job ${jobName} is not completed: ${job.status}`);
  }
  if (job.conclusion !== "success") {
    throw new Error(`${label} job ${jobName} did not succeed: ${job.conclusion}`);
  }
}

function assertPrBrokerCompileReduction(jobs) {
  const buildBrokerJobs = jobs.filter((job) => job.name === "build-broker");
  if (buildBrokerJobs.length !== 1) {
    throw new Error(`PR CI run must have exactly one build-broker job; found ${buildBrokerJobs.length}`);
  }
  for (const required of ["quick-gate", "build-broker", "smoke", "Scaffold examples compile (six SDKs)"]) {
    const matches = jobs.filter((candidate) => candidate.name === required);
    if (matches.length === 0) {
      throw new Error(`PR CI run is missing required artifact-path job: ${required}`);
    }
    if (matches.length > 1) {
      throw new Error(`PR CI run has duplicate artifact-path job ${required}; found ${matches.length}`);
    }
    const [job] = matches;
    assertJobSucceeded(job, "PR CI");
  }
  return buildBrokerJobs[0];
}

function assertRequiredJobs(jobs, label, requiredNames) {
  assertRequiredJobInventory(label, requiredNames);
  const matchedJobs = [];
  const jobNames = jobs.map((job) => assertJobEvidenceName(job, label));
  for (const name of requiredNames) {
    const matches = jobs.filter((_candidate, index) => jobNames[index] === name);
    if (matches.length === 0) {
      throw new Error(`${label} run is missing required jobs: ${name}`);
    }
    if (matches.length > 1) {
      throw new Error(`${label} run has duplicate required job ${name}; found ${matches.length}`);
    }
    const [job] = matches;
    assertJobSucceeded(job, label);
    matchedJobs.push(job);
  }
  return matchedJobs;
}

function assertRequiredJobInventory(label, requiredNames) {
  if (!Array.isArray(requiredNames) || requiredNames.length === 0) {
    throw new Error(`${label} required job inventory must be a non-empty array`);
  }
  const seen = new Set();
  for (const name of requiredNames) {
    if (typeof name !== "string") {
      throw new Error(`${label} required job inventory names must be strings`);
    }
    const trimmed = name.trim();
    if (!trimmed) {
      throw new Error(`${label} required job inventory names must be non-empty`);
    }
    if (name !== trimmed) {
      throw new Error(`${label} required job inventory name ${JSON.stringify(name)} must not include surrounding whitespace`);
    }
    if (seen.has(name)) {
      throw new Error(`${label} required job inventory duplicates ${name}`);
    }
    seen.add(name);
  }
}

function assertJobsBelongToRun(jobs, label, run) {
  const expectedRunId = assertRunEvidenceRunId(run, label);
  const expectedAttempt = assertRunEvidenceAttempt(run, label);
  const seenJobIds = new Map();
  for (const job of jobs) {
    const jobName = assertJobEvidenceName(job, label);
    const jobId = assertJobEvidenceId(job, label);
    const previousJobName = seenJobIds.get(jobId);
    if (previousJobName) {
      throw new Error(`${label} job ${jobName} reuses job id ${jobId} already used by ${previousJobName}`);
    }
    seenJobIds.set(jobId, jobName);
    const actualRunId = assertPositiveIntegerEvidenceToken(
      job?.run_id,
      `${label} job ${jobName} run_id`,
    );
    if (actualRunId !== expectedRunId) {
      throw new Error(`${label} job ${jobName} belongs to run ${actualRunId}, want ${expectedRunId}`);
    }
    const actualAttempt = assertPositiveIntegerEvidenceToken(
      job?.run_attempt,
      `${label} job ${jobName} run_attempt`,
    );
    if (actualAttempt !== expectedAttempt) {
      throw new Error(
        `${label} job ${jobName} belongs to run attempt ${actualAttempt}, want ${expectedAttempt}`,
      );
    }
  }
}

function assertJobEvidenceName(job, label) {
  if (typeof job?.name !== "string") {
    throw new Error(`${label} job name must be a string`);
  }
  const name = job.name;
  const trimmed = name.trim();
  if (!trimmed) {
    throw new Error(`${label} job name must be non-empty`);
  }
  if (name !== trimmed) {
    throw new Error(`${label} job name ${JSON.stringify(name)} must not include surrounding whitespace`);
  }
  return name;
}

function assertJobEvidenceId(job, label) {
  return assertPositiveIntegerEvidenceToken(job?.id, `${label} job ${assertJobEvidenceName(job, label)} id`);
}

function assertRunEvidenceRunId(run, label) {
  return assertPositiveIntegerEvidenceToken(run?.id, `${label} run id`);
}

function assertRunEvidenceAttempt(run, label) {
  return assertPositiveIntegerEvidenceToken(run?.run_attempt, `${label} run_attempt`);
}

function assertRunInspectionUrl(run, label, expectedRepo = "") {
  const expectedRunId = assertRunEvidenceRunId(run, label);
  if (typeof run?.html_url !== "string") {
    throw new Error(`${label} run ${expectedRunId} html_url must be a string`);
  }
  const url = run.html_url;
  if (url !== url.trim()) {
    throw new Error(`${label} run ${expectedRunId} html_url must not include surrounding whitespace`);
  }
  const match = GITHUB_ACTIONS_RUN_URL_PATTERN.exec(url);
  if (!match) {
    throw new Error(`${label} run ${expectedRunId} html_url must be a canonical GitHub Actions run URL`);
  }
  const [, actualRepo, actualRunId] = match;
  if (actualRunId !== expectedRunId) {
    throw new Error(`${label} run ${expectedRunId} html_url run id ${actualRunId}, want ${expectedRunId}`);
  }
  if (expectedRepo && actualRepo !== expectedRepo) {
    throw new Error(`${label} run ${expectedRunId} html_url repo ${actualRepo}, want ${expectedRepo}`);
  }
  return actualRepo;
}

function assertPositiveIntegerEvidenceToken(value, label) {
  const token = String(value ?? "");
  if (!RUN_ID_PATTERN.test(token)) {
    throw new Error(`${label} has invalid value ${token || "(missing)"}; want positive integer`);
  }
  return token;
}

function jobTimestampMs(value, label) {
  return parseActionsTimestampMs(value, label);
}

function assertJobsWithinRunWindow(jobs, label, run) {
  const runStart = runStartMs(run, label);
  const runCompleted = runCompletedMs(run, label);
  for (const job of jobs) {
    const jobLabel = `${label} job ${assertJobEvidenceName(job, label)}`;
    const started = jobTimestampMs(job?.started_at, `${jobLabel} started_at`);
    const completed = jobTimestampMs(job?.completed_at, `${jobLabel} completed_at`);
    if (completed < started) {
      throw new Error(`${jobLabel} completed before it started`);
    }
    if (started < runStart) {
      throw new Error(`${jobLabel} started before parent run ${run?.id || "(unknown)"}`);
    }
    if (completed > runCompleted) {
      throw new Error(`${jobLabel} completed after parent run ${run?.id || "(unknown)"}`);
    }
  }
}

function workflowRunUsesDefaultBranch(workflow, run) {
  return [WORKFLOWS.benchmark, WORKFLOWS.pages].includes(workflow) && run?.event === "workflow_run";
}

function assertRunEvidenceIdentity(run, label, { workflow, event, events, branch, releaseTag, repo } = {}) {
  assertRunEvidenceRunId(run, label);
  assertRunEvidenceAttempt(run, label);
  assertRunInspectionUrl(run, label, repo);
  const expectedPath = `.github/workflows/${workflow}`;
  const actualPath = String(run.path || "");
  if (workflow && !actualPath) throw new Error(`${label} run ${run.id} is missing workflow path`);
  if (workflow && actualPath !== expectedPath) {
    throw new Error(`${label} run ${run.id} came from ${actualPath}, want ${expectedPath}`);
  }

  const allowedEvents = events || (event ? [event] : []);
  if (allowedEvents.length && !allowedEvents.includes(run.event)) {
    throw new Error(`${label} run ${run.id} used event ${run.event}, want ${allowedEvents.join("/")}`);
  }
  if (branch && run.head_branch !== branch) {
    throw new Error(`${label} run ${run.id} used branch ${run.head_branch}, want ${branch}`);
  }
  if (releaseTag) {
    assertReleaseTag(releaseTag, `${label} expected release tag`);
  }
  if (releaseTag && !workflowRunUsesDefaultBranch(workflow, run) && run.head_branch !== releaseTag) {
    throw new Error(`${label} run ${run.id} used release tag ${run.head_branch}, want ${releaseTag}`);
  }
  if (
    !releaseTag &&
    workflow === WORKFLOWS.release &&
    !RELEASE_TAG_PATTERN.test(String(run.head_branch || ""))
  ) {
    throw new Error(`${label} run ${run.id} has invalid release tag ${run.head_branch || "(missing)"}; want vMAJOR.MINOR.PATCH`);
  }
  assertGitSha(run?.head_sha, `${label} run ${run.id || "(unknown)"}`);
}

function assertLintEvidenceBranch(run) {
  if (run.event === "pull_request") return;
  if (run.head_branch !== DEFAULT_INTEGRATION_BRANCH) {
    throw new Error(`lint/actionlint run ${run.id} used branch ${run.head_branch}, want ${DEFAULT_INTEGRATION_BRANCH}`);
  }
}

function assertReleaseTag(value, label) {
  const tag = String(value || "");
  if (!RELEASE_TAG_PATTERN.test(tag)) {
    throw new Error(`${label} has invalid release tag ${tag || "(missing)"}; want vMAJOR.MINOR.PATCH`);
  }
  return tag;
}

function assertGitSha(value, label) {
  const sha = String(value || "");
  if (!GIT_SHA_PATTERN.test(sha)) {
    throw new Error(`${label} has invalid head_sha ${sha || "(missing)"}; want 40 hex characters`);
  }
  return sha;
}

function assertDistinctRunEvidence(runs) {
  const seen = new Map();
  for (const [label, run] of Object.entries(runs)) {
    const id = assertRunEvidenceRunId(run, `${label} evidence`);
    const previousLabel = seen.get(id);
    if (previousLabel) {
      throw new Error(`${label} evidence reuses run ${id} already used by ${previousLabel}`);
    }
    seen.set(id, label);
  }
}

function assertSharedRunInspectionRepo(runs) {
  let expectedRepo = "";
  let expectedLabel = "";
  for (const [label, run] of Object.entries(runs)) {
    const repo = assertRunInspectionUrl(run, `${label} evidence`);
    if (!expectedRepo) {
      expectedRepo = repo;
      expectedLabel = label;
      continue;
    }
    if (repo !== expectedRepo) {
      throw new Error(`${label} evidence uses repo ${repo}, want ${expectedRepo} from ${expectedLabel}`);
    }
  }
}

function assertReleaseChainTags({ release, benchmark, pages }) {
  const releaseTag = assertReleaseTag(release?.head_branch, "release chain");
  const releaseShaText = String(release?.head_sha || "");
  if (!releaseShaText) {
    throw new Error("release chain has missing release head_sha");
  }
  const releaseSha = assertGitSha(releaseShaText, "release chain");
  for (const [label, run] of Object.entries({ "post-release benchmark": benchmark, "post-benchmark Pages": pages })) {
    const actualBranch = String(run?.head_branch || "");
    if (run?.event === "workflow_run" && actualBranch !== DEFAULT_INTEGRATION_BRANCH) {
      throw new Error(`${label} run ${run?.id || "(unknown)"} used branch ${actualBranch || "(missing)"}, want ${DEFAULT_INTEGRATION_BRANCH}`);
    }
    if (run?.event !== "workflow_run" && actualBranch !== releaseTag) {
      throw new Error(`${label} run ${run?.id || "(unknown)"} used release tag ${actualBranch || "(missing)"}, want ${releaseTag}`);
    }
    const actualSha = String(run?.head_sha || "");
    if (!actualSha) {
      throw new Error(`${label} run ${run?.id || "(unknown)"} is missing head_sha`);
    }
    assertGitSha(actualSha, `${label} run ${run?.id || "(unknown)"}`);
    if (actualSha !== releaseSha) {
      throw new Error(`${label} run ${run?.id || "(unknown)"} used head_sha ${actualSha}, want ${releaseSha}`);
    }
  }
  return releaseTag;
}

function assertReleaseDryRunCommit({ release, releaseDryRun }) {
  const releaseSha = assertGitSha(release?.head_sha, "release");
  const releaseTag = assertReleaseTag(release?.head_branch, "release");
  const dryRunTag = assertReleaseTag(releaseDryRun?.head_branch, `release dry-run run ${releaseDryRun?.id || "(unknown)"}`);
  if (dryRunTag !== releaseTag) {
    throw new Error(`release dry-run run ${releaseDryRun?.id || "(unknown)"} used release tag ${dryRunTag}, want ${releaseTag}`);
  }
  const dryRunSha = assertGitSha(releaseDryRun?.head_sha, `release dry-run run ${releaseDryRun?.id || "(unknown)"}`);
  if (dryRunSha !== releaseSha) {
    throw new Error(`release dry-run run ${releaseDryRun?.id || "(unknown)"} used head_sha ${dryRunSha}, want ${releaseSha}`);
  }
}

function assertBranchProtectionCommit({ integration, branchProtection }) {
  const integrationSha = assertGitSha(integration?.head_sha, "integration CI");
  const branchProtectionSha = assertGitSha(
    branchProtection?.head_sha,
    `branch-protection run ${branchProtection?.id || "(unknown)"}`,
  );
  if (branchProtectionSha !== integrationSha) {
    throw new Error(
      `branch-protection run ${branchProtection?.id || "(unknown)"} used head_sha ${branchProtectionSha}, want ${integrationSha}`,
    );
  }
}

function runStartMs(run, label) {
  return parseActionsTimestampMs(run?.run_started_at || run?.created_at, `${label} run ${run?.id || "(unknown)"} start timestamp`);
}

function runCompletedMs(run, label) {
  return parseActionsTimestampMs(run?.completed_at || run?.updated_at, `${label} run ${run?.id || "(unknown)"} completion timestamp`);
}

function assertReleaseChainOrder({ release, benchmark, pages }) {
  const releaseCompleted = runCompletedMs(release, "release");
  const benchmarkStarted = runStartMs(benchmark, "post-release benchmark");
  if (benchmarkStarted < releaseCompleted) {
    throw new Error(
      `post-release benchmark run ${benchmark?.id || "(unknown)"} started before release run ${release?.id || "(unknown)"} completed`,
    );
  }
  const benchmarkCompleted = runCompletedMs(benchmark, "post-release benchmark");
  const pagesStarted = runStartMs(pages, "post-benchmark Pages");
  if (pagesStarted < benchmarkCompleted) {
    throw new Error(
      `post-benchmark Pages run ${pages?.id || "(unknown)"} started before benchmark run ${benchmark?.id || "(unknown)"} completed`,
    );
  }
}

function appendGitHubApiChunk(body, chunk, label) {
  const next = body + chunk;
  if (Buffer.byteLength(next, "utf8") > MAX_GITHUB_API_RESPONSE_BYTES) {
    throw new Error(`GitHub API response exceeded ${MAX_GITHUB_API_RESPONSE_BYTES} bytes for ${label}`);
  }
  return next;
}

function assertGitHubApiSuccessStatus(response, body, label) {
  const statusCode = response?.statusCode;
  if (!Number.isInteger(statusCode)) {
    throw new Error(`GitHub API response for ${label} must include an integer HTTP status code`);
  }
  if (statusCode < 200 || statusCode >= 300) {
    const rateLimitError = githubApiRateLimitError(response, body, label);
    if (rateLimitError) throw rateLimitError;
    const missingWorkflowError = githubApiMissingWorkflowError(response, label);
    if (missingWorkflowError) throw missingWorkflowError;
    throw new Error(`GitHub API ${statusCode}: ${String(body ?? "").slice(0, 500)}`);
  }
}

function githubApiMissingWorkflowError(response, label) {
  if (response?.statusCode !== 404) return null;
  const match = String(label || "").match(
    /^https:\/\/api\.github\.com\/repos\/([^/]+\/[^/]+)\/actions\/workflows\/([^/?]+)\/runs(?:\?|$)/,
  );
  if (!match) return null;
  const repo = match[1];
  const workflow = decodeURIComponent(match[2]);
  const localWorkflowPath = `.github/workflows/${workflow}`;
  const localHint = localWorkflowVisibilityHint(localWorkflowPath);
  return new Error(
    `GitHub Actions workflow ${workflow} is not visible in ${repo}; ${localHint} or provide an exact run id after it exists`,
  );
}

function localWorkflowVisibilityHint(localWorkflowPath) {
  if (!existsSync(join(process.cwd(), localWorkflowPath))) {
    return `local file ${localWorkflowPath} is missing, so add it to this checkout`;
  }
  const gitState = gitWorkflowPathState(localWorkflowPath);
  if (!gitState.available) {
    return `local file ${localWorkflowPath} exists, so commit/push it to the default branch`;
  }
  if (!gitState.tracked) {
    return `local file ${localWorkflowPath} exists but is not tracked, so git add it before commit/push to the default branch`;
  }
  if (gitState.staged && gitState.unstaged) {
    return `local file ${localWorkflowPath} has staged and unstaged changes, so reconcile, commit, and push it to the default branch`;
  }
  if (gitState.staged) {
    return `local file ${localWorkflowPath} has staged changes, so commit and push it to the default branch`;
  }
  if (gitState.unstaged) {
    return `local file ${localWorkflowPath} has unstaged changes, so stage, commit, and push it to the default branch`;
  }
  return `local file ${localWorkflowPath} is tracked and clean locally, so push the commit containing it to the default branch`;
}

function gitWorkflowPathState(localWorkflowPath) {
  if (!commandSucceeds("git", ["rev-parse", "--is-inside-work-tree"])) {
    return { available: false };
  }
  const tracked = commandSucceeds("git", ["ls-files", "--error-unmatch", "--", localWorkflowPath]);
  const staged = !commandSucceeds("git", ["diff", "--cached", "--quiet", "--", localWorkflowPath]);
  const unstaged = !commandSucceeds("git", ["diff", "--quiet", "--", localWorkflowPath]);
  return { available: true, tracked, staged, unstaged };
}

function commandSucceeds(command, args) {
  try {
    execFileSync(command, args, { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

function githubApiRateLimitError(response, body, label) {
  const statusCode = response?.statusCode;
  if (statusCode !== 403 && statusCode !== 429) return null;
  const headers = response?.headers || {};
  const remaining = headers["x-ratelimit-remaining"];
  const reset = headers["x-ratelimit-reset"];
  let message = "";
  try {
    const parsed = JSON.parse(String(body ?? ""));
    if (typeof parsed?.message === "string") message = parsed.message;
  } catch {
    message = String(body ?? "");
  }
  const looksRateLimited =
    String(remaining ?? "") === "0" ||
    /\brate limit\b/i.test(message) ||
    /\bsecondary rate limit\b/i.test(message);
  if (!looksRateLimited) return null;
  let resetHint = "";
  const resetEpochSeconds = Number(reset);
  if (Number.isInteger(resetEpochSeconds) && resetEpochSeconds > 0) {
    resetHint = `; reset ${new Date(resetEpochSeconds * 1000).toISOString()}`;
  }
  return new Error(
    `GitHub API rate limit exceeded for ${label}${resetHint}; set GH_TOKEN or GITHUB_TOKEN for authenticated evidence lookup or rerun after the reset window`,
  );
}

function assertGitHubApiJsonContentType(response, label) {
  const value = response?.headers?.["content-type"];
  if (typeof value !== "string") {
    throw new Error(`GitHub API response for ${label} must include a JSON Content-Type`);
  }
  if (value !== value.trim()) {
    throw new Error(`GitHub API response for ${label} Content-Type must not include surrounding whitespace`);
  }
  const contentType = value.split(";", 1)[0].trim().toLowerCase();
  if (contentType !== "application/json" && contentType !== "application/vnd.github+json") {
    throw new Error(`GitHub API response for ${label} must be JSON, got ${value}`);
  }
}

function githubApiTimeoutError(label) {
  return new Error(`GitHub API request timed out after ${GITHUB_API_REQUEST_TIMEOUT_MS} ms for ${label}`);
}

function readFixtureJson(path) {
  if (!path) throw new Error("--fixture requires a path");
  if (!existsSync(path)) throw new Error(`fixture ${path} does not exist`);
  const stat = statSync(path);
  if (!stat.isFile()) throw new Error(`fixture ${path} must be a regular file`);
  if (stat.size > MAX_FIXTURE_BYTES) {
    throw new Error(`fixture ${path} must be <= ${MAX_FIXTURE_BYTES} bytes`);
  }
  return readFileSync(path, "utf8");
}

function rejectDuplicateJsonObjectKeys(text, label) {
  let index = 0;

  const fail = (message) => {
    throw new Error(`${label} ${message} at byte ${index}`);
  };
  const skipWhitespace = () => {
    while (index < text.length && /[\t\n\r ]/.test(text[index])) index += 1;
  };
  const expect = (token) => {
    if (text[index] !== token) fail(`has invalid JSON; expected ${JSON.stringify(token)}`);
    index += 1;
  };
  const parseString = () => {
    expect("\"");
    let value = "";
    while (index < text.length) {
      const char = text[index];
      index += 1;
      if (char === "\"") return value;
      if (char === "\\") {
        if (index >= text.length) fail("has invalid JSON string escape");
        const escaped = text[index];
        index += 1;
        if (escaped === "u") {
          const hex = text.slice(index, index + 4);
          if (!/^[0-9a-fA-F]{4}$/.test(hex)) fail("has invalid JSON unicode escape");
          value += String.fromCharCode(Number.parseInt(hex, 16));
          index += 4;
        } else if (escaped === "\"") {
          value += "\"";
        } else if (escaped === "\\") {
          value += "\\";
        } else if (escaped === "/") {
          value += "/";
        } else if (escaped === "b") {
          value += "\b";
        } else if (escaped === "f") {
          value += "\f";
        } else if (escaped === "n") {
          value += "\n";
        } else if (escaped === "r") {
          value += "\r";
        } else if (escaped === "t") {
          value += "\t";
        } else {
          fail("has invalid JSON string escape");
        }
      } else {
        if (char < " ") fail("has invalid JSON string control character");
        value += char;
      }
    }
    fail("has unterminated JSON string");
  };
  const parseNumber = () => {
    const match = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/.exec(text.slice(index));
    if (!match || match.index !== 0) fail("has invalid JSON number");
    index += match[0].length;
  };
  const parseLiteral = (literal) => {
    if (!text.startsWith(literal, index)) fail(`has invalid JSON; expected ${literal}`);
    index += literal.length;
  };
  const parseArray = () => {
    expect("[");
    skipWhitespace();
    if (text[index] === "]") {
      index += 1;
      return;
    }
    while (index < text.length) {
      parseValue();
      skipWhitespace();
      if (text[index] === "]") {
        index += 1;
        return;
      }
      expect(",");
      skipWhitespace();
    }
    fail("has unterminated JSON array");
  };
  const parseObject = () => {
    expect("{");
    const keys = new Set();
    skipWhitespace();
    if (text[index] === "}") {
      index += 1;
      return;
    }
    while (index < text.length) {
      skipWhitespace();
      if (text[index] !== "\"") fail("has invalid JSON object key");
      const key = parseString();
      if (keys.has(key)) {
        throw new Error(`${label} has duplicate JSON object key ${JSON.stringify(key)}`);
      }
      keys.add(key);
      skipWhitespace();
      expect(":");
      skipWhitespace();
      parseValue();
      skipWhitespace();
      if (text[index] === "}") {
        index += 1;
        return;
      }
      expect(",");
      skipWhitespace();
    }
    fail("has unterminated JSON object");
  };
  function parseValue() {
    skipWhitespace();
    const token = text[index];
    if (token === "{") {
      parseObject();
    } else if (token === "[") {
      parseArray();
    } else if (token === "\"") {
      parseString();
    } else if (token === "-" || (token >= "0" && token <= "9")) {
      parseNumber();
    } else if (token === "t") {
      parseLiteral("true");
    } else if (token === "f") {
      parseLiteral("false");
    } else if (token === "n") {
      parseLiteral("null");
    } else {
      fail("has invalid JSON value");
    }
  }

  parseValue();
  skipWhitespace();
  if (index !== text.length) fail("has trailing JSON content");
}

function fetchJson(url, token) {
  return new Promise((resolvePromise, reject) => {
    let settled = false;
    const fail = (error) => {
      if (settled) return;
      settled = true;
      reject(error);
    };
    const headers = {
      Accept: "application/vnd.github+json",
      "User-Agent": "udb-ci-runner-evidence",
      "X-GitHub-Api-Version": "2022-11-28",
    };
    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }
    const request = https.request(
      url,
      { headers },
      (response) => {
        let body = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          try {
            body = appendGitHubApiChunk(body, chunk, url);
          } catch (error) {
            response.destroy(error);
          }
        });
        response.on("error", fail);
        response.on("end", () => {
          if (settled) return;
          settled = true;
          try {
            assertGitHubApiSuccessStatus(response, body, url);
            assertGitHubApiJsonContentType(response, url);
          } catch (error) {
            reject(error);
            return;
          }
          try {
            rejectDuplicateJsonObjectKeys(body, `GitHub API response ${url}`);
            resolvePromise(JSON.parse(body));
          } catch (error) {
            reject(new Error(`GitHub API returned invalid JSON: ${error.message}`));
          }
        });
      },
    );
    request.on("error", fail);
    request.setTimeout(GITHUB_API_REQUEST_TIMEOUT_MS, () => {
      const error = githubApiTimeoutError(url);
      fail(error);
      request.destroy(error);
    });
    request.end();
  });
}

async function fetchRun(repo, token, runId, fetcher = fetchJson) {
  const payload = await fetcher(`https://api.github.com/repos/${repo}/actions/runs/${runId}`, token);
  const run = githubObject(payload, `run ${runId} response`);
  const actualRunId = assertPositiveIntegerEvidenceToken(run.id, `run ${runId} response id`);
  if (actualRunId !== String(runId)) {
    throw new Error(`run ${runId} response id ${actualRunId || "(missing)"}, want ${runId}`);
  }
  return run;
}

function runJobsUrl(repo, runId, page) {
  return `https://api.github.com/repos/${repo}/actions/runs/${runId}/jobs?per_page=${MAX_GITHUB_JOBS_PAGE_SIZE}&page=${page}`;
}

function githubObject(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be a JSON object`);
  }
  return value;
}

function githubArrayField(payload, field, label) {
  githubObject(payload, `${label} response`);
  if (!Array.isArray(payload[field])) {
    throw new Error(`${label} response must include ${field} array`);
  }
  return payload[field];
}

function githubTotalCount(payload, label) {
  if (!Number.isInteger(payload?.total_count) || payload.total_count < 0) {
    throw new Error(`${label} response must include non-negative integer total_count`);
  }
  if (payload.total_count > MAX_GITHUB_RUN_JOBS) {
    throw new Error(`${label} response total_count ${payload.total_count} exceeds ${MAX_GITHUB_RUN_JOBS}`);
  }
  return payload.total_count;
}

async function fetchRunJobs(repo, token, runId, fetcher = fetchJson) {
  const jobs = [];
  let page = 1;
  let totalCount = Number.POSITIVE_INFINITY;
  while (jobs.length < totalCount) {
    const payload = await fetcher(runJobsUrl(repo, runId, page), token);
    const pageLabel = `run ${runId} jobs page ${page}`;
    const pageJobs = githubArrayField(payload, "jobs", pageLabel);
    pageJobs.forEach((job, index) => githubObject(job, `${pageLabel} jobs[${index}]`));
    if (pageJobs.length > MAX_GITHUB_JOBS_PAGE_SIZE) {
      throw new Error(`${pageLabel} response returned ${pageJobs.length} jobs, max ${MAX_GITHUB_JOBS_PAGE_SIZE}`);
    }
    const pageTotalCount = githubTotalCount(payload, pageLabel);
    if (!Number.isFinite(totalCount)) {
      totalCount = pageTotalCount;
    } else if (pageTotalCount !== totalCount) {
      throw new Error(`run ${runId} jobs pagination total_count changed from ${totalCount} to ${pageTotalCount}`);
    }
    jobs.push(...pageJobs);
    if (Number.isFinite(totalCount) && jobs.length > totalCount) {
      throw new Error(`run ${runId} jobs pagination returned ${jobs.length}/${totalCount} jobs`);
    }
    if (pageJobs.length === 0 || pageJobs.length < MAX_GITHUB_JOBS_PAGE_SIZE) break;
    page += 1;
  }
  if (Number.isFinite(totalCount) && jobs.length < totalCount) {
    throw new Error(`run ${runId} jobs pagination returned ${jobs.length}/${totalCount} jobs`);
  }
  return jobs;
}

async function findLatestSuccessfulRun(repo, token, workflow, { event, branch, releaseTag, headSha } = {}, fetcher = fetchJson) {
  const events = Array.isArray(event) ? event : event ? [event] : [];
  if (headSha) assertGitSha(headSha, `${workflow} lookup`);
  const params = new URLSearchParams({ status: "completed", per_page: String(MAX_GITHUB_WORKFLOW_RUN_CANDIDATES) });
  if (events.length === 1) params.set("event", events[0]);
  if (branch) params.set("branch", branch);
  const payload = await fetcher(
    `https://api.github.com/repos/${repo}/actions/workflows/${encodeURIComponent(workflow)}/runs?${params}`,
    token,
  );
  const runs = githubArrayField(payload, "workflow_runs", `${workflow} runs`).map((candidate, index) =>
    githubObject(candidate, `${workflow} runs workflow_runs[${index}]`),
  );
  if (runs.length > MAX_GITHUB_WORKFLOW_RUN_CANDIDATES) {
    throw new Error(
      `${workflow} runs response returned ${runs.length} workflow_runs, max ${MAX_GITHUB_WORKFLOW_RUN_CANDIDATES}`,
    );
  }
  const run = runs.find((candidate) => {
    if (candidate.status !== "completed") return false;
    if (candidate.conclusion !== "success") return false;
    if (events.length && !events.includes(candidate.event)) return false;
    if (headSha && candidate.head_sha !== headSha) return false;
    if (releaseTag && !headSha && candidate.head_branch !== releaseTag) return false;
    if (!releaseTag && workflow === WORKFLOWS.release && !String(candidate.head_branch || "").startsWith("v")) return false;
    return true;
  });
  if (!run) {
    throw new Error(
      `no successful completed ${workflow} run found for ${JSON.stringify({ event: events.length ? events : undefined, branch, releaseTag, headSha })}`,
    );
  }
  return run;
}

function assertFixtureShape(fixture) {
  const runs = githubObject(fixture?.runs, "fixture runs");
  const jobs = githubObject(fixture?.jobs, "fixture jobs");
  for (const lane of CI_EVIDENCE_LANES) {
    githubObject(runs[lane], `fixture runs.${lane}`);
    const laneJobs = jobs[lane];
    if (!Array.isArray(laneJobs)) {
      throw new Error(`fixture jobs.${lane} must be an array`);
    }
    laneJobs.forEach((job, index) => githubObject(job, `fixture jobs.${lane}[${index}]`));
  }
}

function auditFixture(path, budgets, evidenceOptions = {}) {
  const fixtureText = readFixtureJson(path);
  rejectDuplicateJsonObjectKeys(fixtureText, `fixture ${path}`);
  const fixture = JSON.parse(fixtureText);
  assertFixtureShape(fixture);
  assertRunEvidenceIdentity(fixture.runs.lint, "lint/actionlint", {
    workflow: WORKFLOWS.lint,
    events: LINT_EVIDENCE_EVENTS,
  });
  assertLintEvidenceBranch(fixture.runs.lint);
  assertRunEvidenceIdentity(fixture.runs.pr, "PR CI", {
    workflow: WORKFLOWS.pr,
    event: "pull_request",
  });
  assertRunEvidenceIdentity(fixture.runs.integration, "integration CI", {
    workflow: WORKFLOWS.integration,
    event: "push",
    branch: DEFAULT_INTEGRATION_BRANCH,
  });
  assertRunEvidenceIdentity(fixture.runs.release, "release", {
    workflow: WORKFLOWS.release,
    event: "push",
  });
  assertRunEvidenceIdentity(fixture.runs.releaseDryRun, "release dry-run", {
    workflow: WORKFLOWS.releaseDryRun,
    event: "workflow_dispatch",
  });
  assertRunEvidenceIdentity(fixture.runs.benchmark, "post-release benchmark", {
    workflow: WORKFLOWS.benchmark,
    event: "workflow_run",
  });
  assertRunEvidenceIdentity(fixture.runs.pages, "post-benchmark Pages", {
    workflow: WORKFLOWS.pages,
    event: "workflow_run",
  });
  assertRunEvidenceIdentity(fixture.runs.branchProtection, "branch-protection", {
    workflow: WORKFLOWS.branchProtection,
    event: "workflow_dispatch",
    branch: DEFAULT_INTEGRATION_BRANCH,
  });
  assertDistinctRunEvidence({
    "lint/actionlint": fixture.runs.lint,
    "PR CI": fixture.runs.pr,
    "integration CI": fixture.runs.integration,
    release: fixture.runs.release,
    "release dry-run": fixture.runs.releaseDryRun,
    "post-release benchmark": fixture.runs.benchmark,
    "post-benchmark Pages": fixture.runs.pages,
    "branch-protection": fixture.runs.branchProtection,
  });
  assertSharedRunInspectionRepo({
    "lint/actionlint": fixture.runs.lint,
    "PR CI": fixture.runs.pr,
    "integration CI": fixture.runs.integration,
    release: fixture.runs.release,
    "release dry-run": fixture.runs.releaseDryRun,
    "post-release benchmark": fixture.runs.benchmark,
    "post-benchmark Pages": fixture.runs.pages,
    "branch-protection": fixture.runs.branchProtection,
  });
  const auditedReleaseTag = assertReleaseChainTags({
    release: fixture.runs.release,
    benchmark: fixture.runs.benchmark,
    pages: fixture.runs.pages,
  });
  assertReleaseDryRunCommit({
    release: fixture.runs.release,
    releaseDryRun: fixture.runs.releaseDryRun,
  });
  assertBranchProtectionCommit({
    integration: fixture.runs.integration,
    branchProtection: fixture.runs.branchProtection,
  });
  assertReleaseChainOrder({
    release: fixture.runs.release,
    benchmark: fixture.runs.benchmark,
    pages: fixture.runs.pages,
  });
  const summary = {
    releaseTag: auditedReleaseTag,
    lint: assertSuccessfulBudgetRun(fixture.runs.lint, "lint/actionlint", budgets.lint, evidenceOptions),
    pr: assertSuccessfulBudgetRun(fixture.runs.pr, "PR CI", budgets.pr, evidenceOptions),
    integration: assertSuccessfulBudgetRun(fixture.runs.integration, "integration CI", budgets.integration, evidenceOptions),
    release: assertSuccessfulBudgetRun(fixture.runs.release, "release", budgets.release, evidenceOptions),
    releaseDryRun: assertSuccessfulBudgetRun(
      fixture.runs.releaseDryRun,
      "release dry-run",
      budgets.releaseDryRun,
      evidenceOptions,
    ),
    benchmark: assertSuccessfulBudgetRun(
      fixture.runs.benchmark,
      "post-release benchmark",
      budgets.benchmark,
      evidenceOptions,
    ),
    pages: assertSuccessfulBudgetRun(fixture.runs.pages, "post-benchmark Pages", budgets.pages, evidenceOptions),
    branchProtection: assertSuccessfulBudgetRun(
      fixture.runs.branchProtection,
      "branch-protection",
      budgets.branchProtection,
      evidenceOptions,
    ),
  };
  const lintEvidenceJobs = assertRequiredJobs(fixture.jobs.lint || [], "lint/actionlint", REQUIRED_JOBS.lint);
  const prBrokerJob = assertPrBrokerCompileReduction(fixture.jobs.pr || []);
  const prEvidenceJobs = [prBrokerJob, ...assertRequiredJobs(fixture.jobs.pr || [], "PR CI", PR_EVIDENCE_JOBS)];
  const integrationEvidenceJobs = assertRequiredJobs(
    fixture.jobs.integration || [],
    "integration CI",
    REQUIRED_JOBS.integration,
  );
  const releaseEvidenceJobs = assertRequiredJobs(fixture.jobs.release || [], "release", REQUIRED_JOBS.release);
  const releaseDryRunEvidenceJobs = assertRequiredJobs(
    fixture.jobs.releaseDryRun || [],
    "release dry-run",
    REQUIRED_JOBS.releaseDryRun,
  );
  const benchmarkEvidenceJobs = assertRequiredJobs(
    fixture.jobs.benchmark || [],
    "post-release benchmark",
    REQUIRED_JOBS.benchmark,
  );
  const pagesEvidenceJobs = assertRequiredJobs(fixture.jobs.pages || [], "post-benchmark Pages", REQUIRED_JOBS.pages);
  const branchProtectionEvidenceJobs = assertRequiredJobs(
    fixture.jobs.branchProtection || [],
    "branch-protection",
    REQUIRED_JOBS.branchProtection,
  );
  assertJobsBelongToRun(lintEvidenceJobs, "lint/actionlint", fixture.runs.lint);
  assertJobsBelongToRun(prEvidenceJobs, "PR CI", fixture.runs.pr);
  assertJobsBelongToRun(integrationEvidenceJobs, "integration CI", fixture.runs.integration);
  assertJobsBelongToRun(releaseEvidenceJobs, "release", fixture.runs.release);
  assertJobsBelongToRun(releaseDryRunEvidenceJobs, "release dry-run", fixture.runs.releaseDryRun);
  assertJobsBelongToRun(benchmarkEvidenceJobs, "post-release benchmark", fixture.runs.benchmark);
  assertJobsBelongToRun(pagesEvidenceJobs, "post-benchmark Pages", fixture.runs.pages);
  assertJobsBelongToRun(branchProtectionEvidenceJobs, "branch-protection", fixture.runs.branchProtection);
  assertJobsWithinRunWindow(lintEvidenceJobs, "lint/actionlint", fixture.runs.lint);
  assertJobsWithinRunWindow(prEvidenceJobs, "PR CI", fixture.runs.pr);
  assertJobsWithinRunWindow(integrationEvidenceJobs, "integration CI", fixture.runs.integration);
  assertJobsWithinRunWindow(releaseEvidenceJobs, "release", fixture.runs.release);
  assertJobsWithinRunWindow(releaseDryRunEvidenceJobs, "release dry-run", fixture.runs.releaseDryRun);
  assertJobsWithinRunWindow(benchmarkEvidenceJobs, "post-release benchmark", fixture.runs.benchmark);
  assertJobsWithinRunWindow(pagesEvidenceJobs, "post-benchmark Pages", fixture.runs.pages);
  assertJobsWithinRunWindow(branchProtectionEvidenceJobs, "branch-protection", fixture.runs.branchProtection);
  return summary;
}

async function auditLive(args, budgets, evidenceOptions = {}, fetcher = fetchJson) {
  const repo = repoArg(args, "--repo", process.env.GITHUB_REPOSITORY);
  const token = process.env.GH_TOKEN || process.env.GITHUB_TOKEN || "";
  const branch = branchArg(args, "--branch", DEFAULT_INTEGRATION_BRANCH);
  const releaseTag = optionalReleaseTagArg(args, "--release-tag");
  const lintRunId = optionalRunIdArg(args, "--lint-run-id");
  const prRunId = optionalRunIdArg(args, "--pr-run-id");
  const integrationRunId = optionalRunIdArg(args, "--integration-run-id");
  const releaseRunId = optionalRunIdArg(args, "--release-run-id");
  const releaseDryRunId = optionalRunIdArg(args, "--release-dry-run-id");
  const benchmarkRunId = optionalRunIdArg(args, "--benchmark-run-id");
  const pagesRunId = optionalRunIdArg(args, "--pages-run-id");
  const branchProtectionRunId = optionalRunIdArg(args, "--branch-protection-run-id");
  const discoveryFailures = [];
  const discoverRun = async (label, lookup) => {
    try {
      return await lookup();
    } catch (error) {
      discoveryFailures.push(`${label}: ${error.message}`);
      return null;
    }
  };
  const lintRun = await discoverRun("lint/actionlint", () =>
    lintRunId
      ? fetchRun(repo, token, lintRunId, fetcher)
      : findLatestSuccessfulRun(repo, token, WORKFLOWS.lint, { event: LINT_EVIDENCE_EVENTS }, fetcher),
  );
  const prRun = await discoverRun("PR CI", () =>
    prRunId
      ? fetchRun(repo, token, prRunId, fetcher)
      : findLatestSuccessfulRun(repo, token, WORKFLOWS.pr, { event: "pull_request" }, fetcher),
  );
  const integrationRun = await discoverRun("integration CI", () =>
    integrationRunId
      ? fetchRun(repo, token, integrationRunId, fetcher)
      : findLatestSuccessfulRun(repo, token, WORKFLOWS.integration, { event: "push", branch }, fetcher),
  );
  const releaseRun = await discoverRun("release", () =>
    releaseRunId
      ? fetchRun(repo, token, releaseRunId, fetcher)
      : findLatestSuccessfulRun(repo, token, WORKFLOWS.release, { event: "push", releaseTag }, fetcher),
  );
  const expectedReleaseTag = releaseTag || String(releaseRun?.head_branch || "");
  const expectedReleaseSha = releaseRun?.head_sha ? assertGitSha(releaseRun.head_sha, "release discovery") : "";
  const releaseDryRunRun = expectedReleaseTag
    ? await discoverRun("release dry-run", () =>
        releaseDryRunId
          ? fetchRun(repo, token, releaseDryRunId, fetcher)
          : findLatestSuccessfulRun(repo, token, WORKFLOWS.releaseDryRun, {
              event: "workflow_dispatch",
              branch: expectedReleaseTag,
            }, fetcher),
      )
    : (discoveryFailures.push("release dry-run: release tag is required because release discovery failed and --release-tag was not provided"), null);
  const benchmarkRun = expectedReleaseTag
    ? await discoverRun("post-release benchmark", () =>
        benchmarkRunId
          ? fetchRun(repo, token, benchmarkRunId, fetcher)
          : findLatestSuccessfulRun(repo, token, WORKFLOWS.benchmark, {
              event: "workflow_run",
              releaseTag: expectedReleaseTag,
              headSha: expectedReleaseSha,
            }, fetcher),
      )
    : (discoveryFailures.push("post-release benchmark: release tag is required because release discovery failed and --release-tag was not provided"), null);
  const pagesRun = expectedReleaseTag
    ? await discoverRun("post-benchmark Pages", () =>
        pagesRunId
          ? fetchRun(repo, token, pagesRunId, fetcher)
          : findLatestSuccessfulRun(repo, token, WORKFLOWS.pages, {
              event: "workflow_run",
              releaseTag: expectedReleaseTag,
              headSha: expectedReleaseSha,
            }, fetcher),
      )
    : (discoveryFailures.push("post-benchmark Pages: release tag is required because release discovery failed and --release-tag was not provided"), null);
  const branchProtectionRun = await discoverRun("branch-protection", () =>
    branchProtectionRunId
      ? fetchRun(repo, token, branchProtectionRunId, fetcher)
      : findLatestSuccessfulRun(repo, token, WORKFLOWS.branchProtection, {
          event: "workflow_dispatch",
          branch,
        }, fetcher),
  );
  if (discoveryFailures.length) {
    throw new Error(`runner evidence discovery failed:\n  - ${discoveryFailures.join("\n  - ")}`);
  }

  assertRunEvidenceIdentity(lintRun, "lint/actionlint", {
    workflow: WORKFLOWS.lint,
    events: LINT_EVIDENCE_EVENTS,
    repo,
  });
  assertLintEvidenceBranch(lintRun);
  assertRunEvidenceIdentity(prRun, "PR CI", {
    workflow: WORKFLOWS.pr,
    event: "pull_request",
    repo,
  });
  assertRunEvidenceIdentity(integrationRun, "integration CI", {
    workflow: WORKFLOWS.integration,
    event: "push",
    branch,
    repo,
  });
  assertRunEvidenceIdentity(releaseRun, "release", {
    workflow: WORKFLOWS.release,
    event: "push",
    releaseTag,
    repo,
  });
  assertRunEvidenceIdentity(releaseDryRunRun, "release dry-run", {
    workflow: WORKFLOWS.releaseDryRun,
    event: "workflow_dispatch",
    repo,
  });
  assertRunEvidenceIdentity(benchmarkRun, "post-release benchmark", {
    workflow: WORKFLOWS.benchmark,
    event: "workflow_run",
    releaseTag: expectedReleaseTag,
    repo,
  });
  assertRunEvidenceIdentity(pagesRun, "post-benchmark Pages", {
    workflow: WORKFLOWS.pages,
    event: "workflow_run",
    releaseTag: expectedReleaseTag,
    repo,
  });
  assertRunEvidenceIdentity(branchProtectionRun, "branch-protection", {
    workflow: WORKFLOWS.branchProtection,
    event: "workflow_dispatch",
    branch,
    repo,
  });
  assertDistinctRunEvidence({
    "lint/actionlint": lintRun,
    "PR CI": prRun,
    "integration CI": integrationRun,
    release: releaseRun,
    "release dry-run": releaseDryRunRun,
    "post-release benchmark": benchmarkRun,
    "post-benchmark Pages": pagesRun,
    "branch-protection": branchProtectionRun,
  });
  const auditedReleaseTag = assertReleaseChainTags({
    release: releaseRun,
    benchmark: benchmarkRun,
    pages: pagesRun,
  });
  assertReleaseDryRunCommit({
    release: releaseRun,
    releaseDryRun: releaseDryRunRun,
  });
  assertBranchProtectionCommit({
    integration: integrationRun,
    branchProtection: branchProtectionRun,
  });
  assertReleaseChainOrder({
    release: releaseRun,
    benchmark: benchmarkRun,
    pages: pagesRun,
  });
  const summary = {
    releaseTag: auditedReleaseTag,
    lint: assertSuccessfulBudgetRun(lintRun, "lint/actionlint", budgets.lint, evidenceOptions),
    pr: assertSuccessfulBudgetRun(prRun, "PR CI", budgets.pr, evidenceOptions),
    integration: assertSuccessfulBudgetRun(integrationRun, "integration CI", budgets.integration, evidenceOptions),
    release: assertSuccessfulBudgetRun(releaseRun, "release", budgets.release, evidenceOptions),
    releaseDryRun: assertSuccessfulBudgetRun(releaseDryRunRun, "release dry-run", budgets.releaseDryRun, evidenceOptions),
    benchmark: assertSuccessfulBudgetRun(benchmarkRun, "post-release benchmark", budgets.benchmark, evidenceOptions),
    pages: assertSuccessfulBudgetRun(pagesRun, "post-benchmark Pages", budgets.pages, evidenceOptions),
    branchProtection: assertSuccessfulBudgetRun(
      branchProtectionRun,
      "branch-protection",
      budgets.branchProtection,
      evidenceOptions,
    ),
  };
  const lintJobs = await fetchRunJobs(repo, token, lintRun.id, fetcher);
  const prJobs = await fetchRunJobs(repo, token, prRun.id, fetcher);
  const integrationJobs = await fetchRunJobs(repo, token, integrationRun.id, fetcher);
  const releaseJobs = await fetchRunJobs(repo, token, releaseRun.id, fetcher);
  const releaseDryRunJobs = await fetchRunJobs(repo, token, releaseDryRunRun.id, fetcher);
  const benchmarkJobs = await fetchRunJobs(repo, token, benchmarkRun.id, fetcher);
  const pagesJobs = await fetchRunJobs(repo, token, pagesRun.id, fetcher);
  const branchProtectionJobs = await fetchRunJobs(repo, token, branchProtectionRun.id, fetcher);
  const lintEvidenceJobs = assertRequiredJobs(lintJobs, "lint/actionlint", REQUIRED_JOBS.lint);
  const prBrokerJob = assertPrBrokerCompileReduction(prJobs);
  const prEvidenceJobs = [prBrokerJob, ...assertRequiredJobs(prJobs, "PR CI", PR_EVIDENCE_JOBS)];
  const integrationEvidenceJobs = assertRequiredJobs(integrationJobs, "integration CI", REQUIRED_JOBS.integration);
  const releaseEvidenceJobs = assertRequiredJobs(releaseJobs, "release", REQUIRED_JOBS.release);
  const releaseDryRunEvidenceJobs = assertRequiredJobs(releaseDryRunJobs, "release dry-run", REQUIRED_JOBS.releaseDryRun);
  const benchmarkEvidenceJobs = assertRequiredJobs(benchmarkJobs, "post-release benchmark", REQUIRED_JOBS.benchmark);
  const pagesEvidenceJobs = assertRequiredJobs(pagesJobs, "post-benchmark Pages", REQUIRED_JOBS.pages);
  const branchProtectionEvidenceJobs = assertRequiredJobs(
    branchProtectionJobs,
    "branch-protection",
    REQUIRED_JOBS.branchProtection,
  );
  assertJobsBelongToRun(lintEvidenceJobs, "lint/actionlint", lintRun);
  assertJobsBelongToRun(prEvidenceJobs, "PR CI", prRun);
  assertJobsBelongToRun(integrationEvidenceJobs, "integration CI", integrationRun);
  assertJobsBelongToRun(releaseEvidenceJobs, "release", releaseRun);
  assertJobsBelongToRun(releaseDryRunEvidenceJobs, "release dry-run", releaseDryRunRun);
  assertJobsBelongToRun(benchmarkEvidenceJobs, "post-release benchmark", benchmarkRun);
  assertJobsBelongToRun(pagesEvidenceJobs, "post-benchmark Pages", pagesRun);
  assertJobsBelongToRun(branchProtectionEvidenceJobs, "branch-protection", branchProtectionRun);
  assertJobsWithinRunWindow(lintEvidenceJobs, "lint/actionlint", lintRun);
  assertJobsWithinRunWindow(prEvidenceJobs, "PR CI", prRun);
  assertJobsWithinRunWindow(integrationEvidenceJobs, "integration CI", integrationRun);
  assertJobsWithinRunWindow(releaseEvidenceJobs, "release", releaseRun);
  assertJobsWithinRunWindow(releaseDryRunEvidenceJobs, "release dry-run", releaseDryRunRun);
  assertJobsWithinRunWindow(benchmarkEvidenceJobs, "post-release benchmark", benchmarkRun);
  assertJobsWithinRunWindow(pagesEvidenceJobs, "post-benchmark Pages", pagesRun);
  assertJobsWithinRunWindow(branchProtectionEvidenceJobs, "branch-protection", branchProtectionRun);
  return summary;
}

async function auditServedSmoke(args, budgets, auditKey, evidenceOptions = {}, fetcher = fetchJson) {
  const audit = SERVED_SMOKE_AUDITS[auditKey];
  if (!audit) throw new Error(`unknown served smoke audit ${auditKey}`);
  const repo = repoArg(args, "--repo", process.env.GITHUB_REPOSITORY);
  const token = process.env.GH_TOKEN || process.env.GITHUB_TOKEN || "";
  const branch = branchArg(args, "--branch", DEFAULT_INTEGRATION_BRANCH);
  const runId = optionalRunIdArg(args, audit.runIdArg);
  const workflow = WORKFLOWS[auditKey];
  const run = runId
    ? await fetchRun(repo, token, runId, fetcher)
    : await findLatestSuccessfulRun(repo, token, workflow, {
        event: "workflow_dispatch",
        branch,
      }, fetcher);
  assertRunEvidenceIdentity(run, audit.label, {
    workflow,
    event: "workflow_dispatch",
    branch,
    repo,
  });
  const minutes = assertSuccessfulBudgetRun(
    run,
    audit.label,
    budgets[auditKey],
    evidenceOptions,
  );
  const jobs = await fetchRunJobs(repo, token, run.id, fetcher);
  const evidenceJobs = assertRequiredJobs(
    jobs,
    audit.label,
    REQUIRED_JOBS[auditKey],
  );
  assertJobsBelongToRun(evidenceJobs, audit.label, run);
  assertJobsWithinRunWindow(evidenceJobs, audit.label, run);
  return { [auditKey]: minutes, runId: String(run.id), label: audit.label };
}

async function auditIdempotencyServed(args, budgets, evidenceOptions = {}, fetcher = fetchJson) {
  return auditServedSmoke(args, budgets, "idempotencyServed", evidenceOptions, fetcher);
}

async function auditRequestedServedSmokes(args, budgets, evidenceOptions = {}, fetcher = fetchJson) {
  const servedAuditKeys = requestedServedAuditKeys(args);
  const summary = {};
  const failures = [];
  for (const auditKey of servedAuditKeys) {
    const audit = SERVED_SMOKE_AUDITS[auditKey];
    try {
      const result = await auditServedSmoke(args, budgets, auditKey, evidenceOptions, fetcher);
      summary[auditKey] = result[auditKey];
      summary[`${auditKey}RunId`] = result.runId;
    } catch (error) {
      failures.push(formatNestedFailure(audit.label, error));
    }
  }
  if (failures.length) {
    throw new Error(`served evidence audit failed:\n  - ${failures.join("\n  - ")}`);
  }
  return summary;
}

function formatNestedFailure(label, error) {
  return `${label}: ${String(error?.message || error).replace(/\n/g, "\n    ")}`;
}

function servedEvidenceSummaryText(summary, servedAuditKeys) {
  return servedAuditKeys
    .map((auditKey) => {
      const audit = SERVED_SMOKE_AUDITS[auditKey];
      return `${audit.label}=${summary[auditKey].toFixed(2)}m(run=${summary[`${auditKey}RunId`]})`;
    })
    .join(", ");
}

async function auditAllEvidence(args, budgets, evidenceOptions = {}, fetcher = fetchJson) {
  const fixture = argValue(args, "--fixture");
  const servedAuditKeys = requestedServedAuditKeys(args);
  const failures = [];
  let summary;
  let servedSummary = {};
  try {
    summary = fixture ? auditFixture(fixture, budgets, evidenceOptions) : await auditLive(args, budgets, evidenceOptions, fetcher);
  } catch (error) {
    failures.push(formatNestedFailure("CI runner evidence", error));
  }
  if (servedAuditKeys.length > 0) {
    try {
      servedSummary = await auditRequestedServedSmokes(args, budgets, evidenceOptions, fetcher);
    } catch (error) {
      failures.push(formatNestedFailure("served evidence", error));
    }
  }
  if (failures.length) {
    throw new Error(`runner evidence audit failed:\n  - ${failures.join("\n  - ")}`);
  }
  return { summary, servedSummary, servedAuditKeys };
}

function fixtureRun(id, minutes, conclusion = "success", extra = {}) {
  return {
    id,
    html_url: `https://github.com/udb/selftest/actions/runs/${id}`,
    status: "completed",
    conclusion,
    created_at: "2026-07-01T00:00:00Z",
    run_started_at: "2026-07-01T00:00:00Z",
    updated_at: new Date(Date.parse("2026-07-01T00:00:00Z") + minutes * 60000).toISOString(),
    run_attempt: 1,
    head_sha: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    ...extra,
  };
}

let fixtureJobId = 1000;

function fixtureJob(name, conclusion = "success", extra = {}) {
  return {
    id: fixtureJobId++,
    name,
    status: "completed",
    conclusion,
    ...extra,
  };
}

async function runSelftest() {
  const root = mkdtempSync(join(tmpdir(), "udb-runner-evidence-"));
  const selftestEvidenceOptions = {
    maxAgeDays: DEFAULT_MAX_EVIDENCE_AGE_DAYS,
    nowMs: Date.parse("2026-07-02T00:00:00Z"),
  };
  const auditSelftestFixture = (path) => auditFixture(path, DEFAULT_BUDGETS, selftestEvidenceOptions);
  const releaseSha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  const benchmarkSha = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  const pagesSha = "cccccccccccccccccccccccccccccccccccccccc";
  const integrationSha = "dddddddddddddddddddddddddddddddddddddddd";
  try {
    const good = {
      runs: {
        lint: fixtureRun(1, 2, "success", {
          path: ".github/workflows/lint-workflows.yml",
          event: "push",
          head_branch: "main",
        }),
        pr: fixtureRun(2, 7.5, "success", {
          path: ".github/workflows/ci.yml",
          event: "pull_request",
          head_branch: "feature/ci-proof",
        }),
        integration: fixtureRun(3, 29, "success", {
          path: ".github/workflows/ci.yml",
          event: "push",
          head_branch: "main",
          head_sha: integrationSha,
        }),
        release: fixtureRun(4, 39, "success", {
          path: ".github/workflows/release.yml",
          event: "push",
          head_branch: "v0.3.7",
          head_sha: releaseSha,
        }),
        releaseDryRun: fixtureRun(8, 95, "success", {
          path: ".github/workflows/release-binaries.yml",
          event: "workflow_dispatch",
          head_branch: "v0.3.7",
          head_sha: releaseSha,
        }),
        benchmark: fixtureRun(12, 80, "success", {
          created_at: "2026-07-01T00:40:00Z",
          run_started_at: "2026-07-01T00:40:00Z",
          updated_at: "2026-07-01T02:00:00Z",
          path: ".github/workflows/benchmark-sdks.yml",
          event: "workflow_run",
          head_branch: "main",
          head_sha: releaseSha,
        }),
        pages: fixtureRun(13, 12, "success", {
          created_at: "2026-07-01T02:01:00Z",
          run_started_at: "2026-07-01T02:01:00Z",
          updated_at: "2026-07-01T02:13:00Z",
          path: ".github/workflows/pages.yml",
          event: "workflow_run",
          head_branch: "main",
          head_sha: releaseSha,
        }),
        branchProtection: fixtureRun(10, 3, "success", {
          path: ".github/workflows/branch-protection-audit.yml",
          event: "workflow_dispatch",
          head_branch: "main",
          head_sha: integrationSha,
        }),
      },
      jobs: {
        lint: [fixtureJob("actionlint")],
        pr: [...PR_EVIDENCE_JOBS.map((name) => fixtureJob(name)), fixtureJob("build-broker")],
        integration: INTEGRATION_REQUIRED_JOBS.map((name) => fixtureJob(name)),
        release: [
          fixtureJob("ci-green"),
          fixtureJob("version-guard"),
          fixtureJob("build-binaries"),
          fixtureJob("publish-crates"),
          fixtureJob("publish-docker"),
          fixtureJob("publish-ts"),
          fixtureJob("publish-py"),
          fixtureJob("publish-csharp"),
          fixtureJob("publish-packagist"),
        ],
        releaseDryRun: REQUIRED_JOBS.releaseDryRun.map((name) => fixtureJob(name)),
        benchmark: REQUIRED_JOBS.benchmark.map((name) => fixtureJob(name)),
        pages: REQUIRED_JOBS.pages.map((name) => fixtureJob(name)),
        branchProtection: REQUIRED_JOBS.branchProtection.map((name) => fixtureJob(name)),
      },
    };
    for (const [lane, jobs] of Object.entries(good.jobs)) {
      for (const job of jobs) {
        job.run_id = good.runs[lane].id;
        job.run_attempt = good.runs[lane].run_attempt;
        job.started_at = good.runs[lane].run_started_at || good.runs[lane].created_at;
        job.completed_at = good.runs[lane].completed_at || good.runs[lane].updated_at;
      }
    }
    const goodPath = join(root, "good.json");
    writeFileSync(goodPath, JSON.stringify(good));
    auditSelftestFixture(goodPath);

    const customBranchEvidence = structuredClone(good);
    customBranchEvidence.runs.integration = {
      ...customBranchEvidence.runs.integration,
      head_branch: "release/v0.3.7",
    };
    customBranchEvidence.runs.branchProtection = {
      ...customBranchEvidence.runs.branchProtection,
      head_branch: "release/v0.3.7",
    };
    const runsById = new Map(Object.values(customBranchEvidence.runs).map((run) => [String(run.id), run]));
    const jobsByRunId = new Map(
      Object.entries(customBranchEvidence.jobs).map(([lane, jobs]) => [
        String(customBranchEvidence.runs[lane].id),
        jobs,
      ]),
    );
    const customBranchSummary = await auditLive(
      [
        "--repo",
        "udb/selftest",
        "--branch",
        "release/v0.3.7",
        "--lint-run-id",
        String(customBranchEvidence.runs.lint.id),
        "--pr-run-id",
        String(customBranchEvidence.runs.pr.id),
        "--integration-run-id",
        String(customBranchEvidence.runs.integration.id),
        "--release-run-id",
        String(customBranchEvidence.runs.release.id),
        "--release-dry-run-id",
        String(customBranchEvidence.runs.releaseDryRun.id),
        "--benchmark-run-id",
        String(customBranchEvidence.runs.benchmark.id),
        "--pages-run-id",
        String(customBranchEvidence.runs.pages.id),
        "--branch-protection-run-id",
        String(customBranchEvidence.runs.branchProtection.id),
      ],
      DEFAULT_BUDGETS,
      selftestEvidenceOptions,
      async (url) => {
        const runMatch = url.match(/\/actions\/runs\/(\d+)(?:\/jobs)?/);
        if (!runMatch) throw new Error(`unexpected custom branch evidence URL ${url}`);
        const runId = runMatch[1];
        if (url.includes("/jobs")) {
          return { total_count: jobsByRunId.get(runId)?.length || 0, jobs: jobsByRunId.get(runId) || [] };
        }
        const run = runsById.get(runId);
        if (!run) throw new Error(`unexpected custom branch evidence run ${runId}`);
        return run;
      },
    );
    if (customBranchSummary.integration !== 29 || customBranchSummary.branchProtection !== 3) {
      throw new Error("custom branch live evidence summary regression was not caught");
    }

    const fixtureDir = join(root, "fixture-dir");
    mkdirSync(fixtureDir);
    try {
      auditSelftestFixture(fixtureDir);
      throw new Error("fixture directory regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("must be a regular file")) throw error;
    }

    const oversizedFixturePath = join(root, "oversized-fixture.json");
    writeFileSync(oversizedFixturePath, " ".repeat(MAX_FIXTURE_BYTES + 1));
    try {
      auditSelftestFixture(oversizedFixturePath);
      throw new Error("oversized fixture regression was not caught");
    } catch (error) {
      if (!String(error.message).includes(`must be <= ${MAX_FIXTURE_BYTES} bytes`)) throw error;
    }

    const duplicateFixtureKeyPath = join(root, "duplicate-fixture-key.json");
    writeFileSync(duplicateFixtureKeyPath, "{\"runs\":{},\"r\\u0075ns\":{}}");
    try {
      auditSelftestFixture(duplicateFixtureKeyPath);
      throw new Error("duplicate fixture key regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("has duplicate JSON object key \"runs\"")) throw error;
    }

    const missingRunsFixturePath = join(root, "missing-runs-fixture.json");
    writeFileSync(missingRunsFixturePath, JSON.stringify({ jobs: good.jobs }));
    try {
      auditSelftestFixture(missingRunsFixturePath);
      throw new Error("missing fixture runs regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("fixture runs must be a JSON object")) throw error;
    }

    const nonArrayJobsFixture = structuredClone(good);
    nonArrayJobsFixture.jobs.release = {};
    const nonArrayJobsFixturePath = join(root, "non-array-jobs-fixture.json");
    writeFileSync(nonArrayJobsFixturePath, JSON.stringify(nonArrayJobsFixture));
    try {
      auditSelftestFixture(nonArrayJobsFixturePath);
      throw new Error("non-array fixture jobs regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("fixture jobs.release must be an array")) throw error;
    }

    const malformedJobFixture = structuredClone(good);
    malformedJobFixture.jobs.release = malformedJobFixture.jobs.release.map((job) =>
      job.name === "publish-docker" ? "publish-docker" : job,
    );
    const malformedJobFixturePath = join(root, "malformed-job-fixture.json");
    writeFileSync(malformedJobFixturePath, JSON.stringify(malformedJobFixture));
    try {
      auditSelftestFixture(malformedJobFixturePath);
      throw new Error("malformed fixture job regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("fixture jobs.release[4] must be a JSON object")) throw error;
    }

    const malformedRunId = structuredClone(good);
    malformedRunId.runs.pr = {
      ...malformedRunId.runs.pr,
      id: " 2",
    };
    const malformedRunIdPath = join(root, "malformed-run-id.json");
    writeFileSync(malformedRunIdPath, JSON.stringify(malformedRunId));
    try {
      auditSelftestFixture(malformedRunIdPath);
      throw new Error("malformed run id regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("PR CI run id has invalid value  2; want positive integer")) throw error;
    }

    const missingPrSha = structuredClone(good);
    delete missingPrSha.runs.pr.head_sha;
    const missingPrShaPath = join(root, "missing-pr-sha.json");
    writeFileSync(missingPrShaPath, JSON.stringify(missingPrSha));
    try {
      auditSelftestFixture(missingPrShaPath);
      throw new Error("missing PR head_sha regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("PR CI run 2 has invalid head_sha (missing); want 40 hex characters")) {
        throw error;
      }
    }

    const wrongRunHtmlUrl = structuredClone(good);
    wrongRunHtmlUrl.runs.pr = {
      ...wrongRunHtmlUrl.runs.pr,
      html_url: "https://github.com/udb/selftest/actions/runs/99",
    };
    const wrongRunHtmlUrlPath = join(root, "wrong-run-html-url.json");
    writeFileSync(wrongRunHtmlUrlPath, JSON.stringify(wrongRunHtmlUrl));
    try {
      auditSelftestFixture(wrongRunHtmlUrlPath);
      throw new Error("wrong run html_url regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("PR CI run 2 html_url run id 99, want 2")) throw error;
    }

    const crossRepoRunHtmlUrl = structuredClone(good);
    crossRepoRunHtmlUrl.runs.pr = {
      ...crossRepoRunHtmlUrl.runs.pr,
      html_url: "https://github.com/other/repo/actions/runs/2",
    };
    const crossRepoRunHtmlUrlPath = join(root, "cross-repo-run-html-url.json");
    writeFileSync(crossRepoRunHtmlUrlPath, JSON.stringify(crossRepoRunHtmlUrl));
    try {
      auditSelftestFixture(crossRepoRunHtmlUrlPath);
      throw new Error("cross-repo run html_url regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("PR CI evidence uses repo other/repo, want udb/selftest from lint/actionlint")) {
        throw error;
      }
    }

    const extraNonRequiredJob = structuredClone(good);
    extraNonRequiredJob.jobs.release.push(fixtureJob("release-note"));
    const extraNonRequiredJobPath = join(root, "extra-non-required-job.json");
    writeFileSync(extraNonRequiredJobPath, JSON.stringify(extraNonRequiredJob));
    try {
      auditSelftestFixture(extraNonRequiredJobPath);
    } catch (error) {
      throw new Error(`non-required job timestamp scope regression: ${error.message}`);
    }

    const wrongLintBranch = structuredClone(good);
    wrongLintBranch.runs.lint = fixtureRun(19, 2, "success", {
      path: ".github/workflows/lint-workflows.yml",
      event: "push",
      head_branch: "feature/lint-proof",
    });
    const wrongLintBranchPath = join(root, "wrong-lint-branch.json");
    writeFileSync(wrongLintBranchPath, JSON.stringify(wrongLintBranch));
    try {
      auditSelftestFixture(wrongLintBranchPath);
      throw new Error("wrong lint branch regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("lint/actionlint run 19 used branch feature/lint-proof, want main")) {
        throw error;
      }
    }

    const slow = structuredClone(good);
    slow.runs.pr = fixtureRun(5, 9, "success", {
      path: ".github/workflows/ci.yml",
      event: "pull_request",
      head_branch: "feature/ci-proof",
    });
    const slowPath = join(root, "slow.json");
    writeFileSync(slowPath, JSON.stringify(slow));
    try {
      auditSelftestFixture(slowPath);
      throw new Error("over-budget PR run regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("budget 8 min")) throw error;
    }

    try {
      boundedBudgetArg(["--pr-budget-minutes", "999"], "--pr-budget-minutes", DEFAULT_BUDGETS.pr, MAX_BUDGETS.pr);
      throw new Error("inflated budget override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--pr-budget-minutes must be <= 8 minutes")) throw error;
    }

    if (boundedBudgetArg(["--pr-budget-minutes", "7.5"], "--pr-budget-minutes", DEFAULT_BUDGETS.pr, MAX_BUDGETS.pr) !== 7.5) {
      throw new Error("tightened budget override was rejected");
    }

    try {
      boundedBudgetArg(["--pr-budget-minutes", " 7.5 "], "--pr-budget-minutes", DEFAULT_BUDGETS.pr, MAX_BUDGETS.pr);
      throw new Error("padded numeric budget override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--pr-budget-minutes must not include surrounding whitespace")) throw error;
    }

    try {
      boundedBudgetArg(["--pr-budget-minutes", "0x8"], "--pr-budget-minutes", DEFAULT_BUDGETS.pr, MAX_BUDGETS.pr);
      throw new Error("non-decimal budget override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--pr-budget-minutes must be a positive decimal number")) throw error;
    }

    try {
      boundedMaxEvidenceAgeArg(
        ["--max-evidence-age-days", "365"],
        "--max-evidence-age-days",
        DEFAULT_MAX_EVIDENCE_AGE_DAYS,
        MAX_EVIDENCE_AGE_DAYS,
      );
      throw new Error("inflated max evidence-age override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--max-evidence-age-days must be <= 14 days")) throw error;
    }

    if (
      boundedMaxEvidenceAgeArg(
        ["--max-evidence-age-days", "7"],
        "--max-evidence-age-days",
        DEFAULT_MAX_EVIDENCE_AGE_DAYS,
        MAX_EVIDENCE_AGE_DAYS,
      ) !== 7
    ) {
      throw new Error("tightened max evidence-age override was rejected");
    }

    try {
      boundedMaxEvidenceAgeArg(
        ["--max-evidence-age-days", ""],
        "--max-evidence-age-days",
        DEFAULT_MAX_EVIDENCE_AGE_DAYS,
        MAX_EVIDENCE_AGE_DAYS,
      );
      throw new Error("empty max evidence-age override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--max-evidence-age-days must be a positive decimal number")) throw error;
    }

    try {
      optionalReleaseTagArg(["--release-tag", " v0.3.7 "], "--release-tag");
      throw new Error("padded release-tag override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--release-tag must not include surrounding whitespace")) throw error;
    }

    if (optionalReleaseTagArg(["--release-tag", "v0.3.7"], "--release-tag") !== "v0.3.7") {
      throw new Error("canonical release-tag override was rejected");
    }

    try {
      optionalRunIdArg(["--pr-run-id", " 123 "], "--pr-run-id");
      throw new Error("padded run-id override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--pr-run-id must not include surrounding whitespace")) throw error;
    }

    try {
      optionalRunIdArg(["--pr-run-id", "abc"], "--pr-run-id");
      throw new Error("non-numeric run-id override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--pr-run-id must be a positive integer run id")) throw error;
    }

    try {
      optionalRunIdArg(["--pr-run-id", "0"], "--pr-run-id");
      throw new Error("zero run-id override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--pr-run-id must be a positive integer run id")) throw error;
    }

    if (optionalRunIdArg(["--pr-run-id", "123"], "--pr-run-id") !== "123") {
      throw new Error("canonical run-id override was rejected");
    }

    try {
      assertKnownArgs(["--rest-gatway-smoke"]);
      throw new Error("unknown runner-evidence argument regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("unknown runner evidence argument --rest-gatway-smoke")) throw error;
    }

    try {
      assertKnownArgs(["evidence.json"]);
      throw new Error("unexpected positional runner-evidence argument regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("unexpected runner evidence argument evidence.json")) throw error;
    }

    try {
      assertKnownArgs(["--repo"]);
      throw new Error("missing runner-evidence argument value regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--repo requires a value")) throw error;
    }

    assertKnownArgs([
      "--repo",
      "owner/repo",
      "--all-evidence",
      "--error-detail-served-smoke",
      "--error-detail-run-id",
      "92",
    ]);

    const allEvidenceServedKeys = requestedServedAuditKeys(["--all-evidence"]);
    const expectedAllEvidenceServedKeys = Object.keys(SERVED_SMOKE_AUDITS);
    if (JSON.stringify(allEvidenceServedKeys) !== JSON.stringify(expectedAllEvidenceServedKeys)) {
      throw new Error("--all-evidence did not select every served proof lane");
    }

    try {
      assertNoUnusedEvidenceOverrides(["--pr-run-id", "123"], []);
      throw new Error("unused CI run-id override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--pr-run-id requires --all-evidence")) throw error;
    }

    try {
      assertNoUnusedEvidenceOverrides(["--error-detail-run-id", "92"], []);
      throw new Error("unused served run-id override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--error-detail-run-id requires --error-detail-served-smoke")) throw error;
    }

    try {
      assertNoUnusedEvidenceOverrides(["--retry-safe-served-budget-minutes", "15"], []);
      throw new Error("unused served budget override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--retry-safe-served-budget-minutes requires --retry-safe-served-smoke")) throw error;
    }

    try {
      assertNoUnusedEvidenceOverrides(
        ["--error-detail-served-smoke", "--pr-budget-minutes", "7.5"],
        ["errorDetailServed"],
      );
      throw new Error("unused CI budget override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--pr-budget-minutes requires --all-evidence")) throw error;
    }

    try {
      assertNoUnusedEvidenceOverrides(
        ["--error-detail-served-smoke", "--release-tag", "v0.3.7"],
        ["errorDetailServed"],
      );
      throw new Error("unused release-tag override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--release-tag requires --all-evidence")) throw error;
    }

    try {
      assertNoUnusedEvidenceOverrides(
        ["--error-detail-served-smoke", "--fixture", "ci-evidence.json"],
        ["errorDetailServed"],
      );
      throw new Error("unused fixture override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--fixture requires --all-evidence")) throw error;
    }

    assertNoUnusedEvidenceOverrides(["--all-evidence", "--pr-run-id", "123"], []);
    assertNoUnusedEvidenceOverrides(
      ["--all-evidence", "--error-detail-run-id", "92"],
      requestedServedAuditKeys(["--all-evidence"]),
    );
    assertNoUnusedEvidenceOverrides(
      ["--error-detail-served-smoke", "--error-detail-run-id", "92"],
      ["errorDetailServed"],
    );

    try {
      branchArg(["--branch", " main "], "--branch", DEFAULT_INTEGRATION_BRANCH);
      throw new Error("padded branch override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--branch must not include surrounding whitespace")) throw error;
    }

    try {
      branchArg(["--branch", "feature ci-proof"], "--branch", DEFAULT_INTEGRATION_BRANCH);
      throw new Error("whitespace branch override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--branch must not include whitespace")) throw error;
    }

    try {
      branchArg(["--branch", "feature/../main"], "--branch", DEFAULT_INTEGRATION_BRANCH);
      throw new Error("non-canonical branch override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--branch must be a canonical branch name")) throw error;
    }

    if (branchArg(["--branch", "release/v0.3.7"], "--branch", DEFAULT_INTEGRATION_BRANCH) !== "release/v0.3.7") {
      throw new Error("canonical branch override was rejected");
    }

    try {
      repoArg(["--repo", " fahara02/udb "], "--repo", undefined);
      throw new Error("padded repo override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--repo must not include surrounding whitespace")) throw error;
    }

    try {
      repoArg(["--repo", "fahara02"], "--repo", undefined);
      throw new Error("malformed repo override regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--repo must be an owner/repo repository name")) throw error;
    }

    if (repoArg(["--repo", "fahara02/udb"], "--repo", undefined) !== "fahara02/udb") {
      throw new Error("canonical repo override was rejected");
    }

    const stale = structuredClone(good);
    stale.runs.pr = {
      ...stale.runs.pr,
      created_at: "2026-06-01T00:00:00.000Z",
      run_started_at: "2026-06-01T00:00:00.000Z",
      updated_at: "2026-06-01T00:08:00.000Z",
      completed_at: "2026-06-01T00:08:00.000Z",
    };
    const stalePath = join(root, "stale.json");
    writeFileSync(stalePath, JSON.stringify(stale));
    try {
      auditSelftestFixture(stalePath);
      throw new Error("stale runner evidence regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("max evidence age 14 days")) throw error;
    }

    const lateCompletedAt = structuredClone(good);
    lateCompletedAt.runs.pr = {
      ...lateCompletedAt.runs.pr,
      updated_at: "2026-07-01T00:07:30.000Z",
      completed_at: "2026-07-01T00:09:00.000Z",
    };
    const lateCompletedAtPath = join(root, "late-completed-at.json");
    writeFileSync(lateCompletedAtPath, JSON.stringify(lateCompletedAt));
    try {
      auditSelftestFixture(lateCompletedAtPath);
      throw new Error("late completed_at budget regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("PR CI run 2 took 9.00 min, budget 8 min")) throw error;
    }

    const paddedRunTimestamp = structuredClone(good);
    paddedRunTimestamp.runs.pr = {
      ...paddedRunTimestamp.runs.pr,
      run_started_at: " 2026-07-01T00:00:00Z ",
    };
    const paddedRunTimestampPath = join(root, "padded-run-timestamp.json");
    writeFileSync(paddedRunTimestampPath, JSON.stringify(paddedRunTimestamp));
    try {
      auditSelftestFixture(paddedRunTimestampPath);
      throw new Error("padded run timestamp regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("run 2 start timestamp must not include surrounding whitespace")) throw error;
    }

    const offsetJobTimestamp = structuredClone(good);
    offsetJobTimestamp.jobs.release = offsetJobTimestamp.jobs.release.map((job) =>
      job.name === "publish-docker" ? { ...job, started_at: "2026-07-01T00:00:00+00:00" } : job,
    );
    const offsetJobTimestampPath = join(root, "offset-job-timestamp.json");
    writeFileSync(offsetJobTimestampPath, JSON.stringify(offsetJobTimestamp));
    try {
      auditSelftestFixture(offsetJobTimestampPath);
      throw new Error("offset job timestamp regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release job publish-docker started_at must be a GitHub Actions UTC timestamp")) {
        throw error;
      }
    }

    const duplicate = structuredClone(good);
    duplicate.jobs.pr.push(fixtureJob("build-broker"));
    const duplicatePath = join(root, "duplicate.json");
    writeFileSync(duplicatePath, JSON.stringify(duplicate));
    try {
      auditSelftestFixture(duplicatePath);
      throw new Error("duplicate build-broker regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("exactly one build-broker")) throw error;
    }

    const duplicatePrSmoke = structuredClone(good);
    duplicatePrSmoke.jobs.pr.push(fixtureJob("smoke"));
    const duplicatePrSmokePath = join(root, "duplicate-pr-smoke.json");
    writeFileSync(duplicatePrSmokePath, JSON.stringify(duplicatePrSmoke));
    try {
      auditSelftestFixture(duplicatePrSmokePath);
      throw new Error("duplicate PR smoke regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("PR CI run has duplicate artifact-path job smoke; found 2")) {
        throw error;
      }
    }

    const missingPrQuickGate = structuredClone(good);
    missingPrQuickGate.jobs.pr = missingPrQuickGate.jobs.pr.filter((job) => job.name !== "quick-gate");
    const missingPrQuickGatePath = join(root, "missing-pr-quick-gate.json");
    writeFileSync(missingPrQuickGatePath, JSON.stringify(missingPrQuickGate));
    try {
      auditSelftestFixture(missingPrQuickGatePath);
      throw new Error("missing PR quick-gate regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("PR CI run is missing required artifact-path job: quick-gate")) {
        throw error;
      }
    }

    const missingPrRequiredJob = structuredClone(good);
    missingPrRequiredJob.jobs.pr = missingPrRequiredJob.jobs.pr.filter((job) => job.name !== "Proto (buf)");
    const missingPrRequiredJobPath = join(root, "missing-pr-required-job.json");
    writeFileSync(missingPrRequiredJobPath, JSON.stringify(missingPrRequiredJob));
    try {
      auditSelftestFixture(missingPrRequiredJobPath);
      throw new Error("missing PR required job regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("PR CI run is missing required jobs: Proto (buf)")) {
        throw error;
      }
    }

    const missingPrAdvisoryJob = structuredClone(good);
    missingPrAdvisoryJob.jobs.pr = missingPrAdvisoryJob.jobs.pr.filter(
      (job) => job.name !== "Rust (ubuntu-latest)",
    );
    const missingPrAdvisoryJobPath = join(root, "missing-pr-advisory-job.json");
    writeFileSync(missingPrAdvisoryJobPath, JSON.stringify(missingPrAdvisoryJob));
    try {
      auditSelftestFixture(missingPrAdvisoryJobPath);
      throw new Error("missing PR advisory job regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("PR CI run is missing required jobs: Rust (ubuntu-latest)")) {
        throw error;
      }
    }

    const wrongWorkflow = structuredClone(good);
    wrongWorkflow.runs.release = fixtureRun(6, 5, "success", {
      path: ".github/workflows/ci.yml",
      event: "push",
      head_branch: "v0.3.7",
      head_sha: releaseSha,
    });
    const wrongWorkflowPath = join(root, "wrong-workflow.json");
    writeFileSync(wrongWorkflowPath, JSON.stringify(wrongWorkflow));
    try {
      auditSelftestFixture(wrongWorkflowPath);
      throw new Error("wrong workflow evidence regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("want .github/workflows/release.yml")) throw error;
    }

    const malformedReleaseTag = structuredClone(good);
    malformedReleaseTag.runs.release = {
      ...malformedReleaseTag.runs.release,
      head_branch: "vnext",
    };
    const malformedReleaseTagPath = join(root, "malformed-release-tag.json");
    writeFileSync(malformedReleaseTagPath, JSON.stringify(malformedReleaseTag));
    try {
      auditSelftestFixture(malformedReleaseTagPath);
      throw new Error("malformed release tag regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release run 4 has invalid release tag vnext; want vMAJOR.MINOR.PATCH")) {
        throw error;
      }
    }

    const paddedReleaseTag = structuredClone(good);
    paddedReleaseTag.runs.release = {
      ...paddedReleaseTag.runs.release,
      head_branch: " v0.3.7",
    };
    const paddedReleaseTagPath = join(root, "padded-release-tag.json");
    writeFileSync(paddedReleaseTagPath, JSON.stringify(paddedReleaseTag));
    try {
      auditSelftestFixture(paddedReleaseTagPath);
      throw new Error("padded release tag regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release run 4 has invalid release tag  v0.3.7; want vMAJOR.MINOR.PATCH")) {
        throw error;
      }
    }

    const duplicateRunEvidence = structuredClone(good);
    duplicateRunEvidence.runs.integration = {
      ...duplicateRunEvidence.runs.integration,
      id: duplicateRunEvidence.runs.pr.id,
      html_url: duplicateRunEvidence.runs.pr.html_url,
      path: ".github/workflows/ci.yml",
      event: "push",
      head_branch: "main",
    };
    const duplicateRunEvidencePath = join(root, "duplicate-run-evidence.json");
    writeFileSync(duplicateRunEvidencePath, JSON.stringify(duplicateRunEvidence));
    try {
      auditSelftestFixture(duplicateRunEvidencePath);
      throw new Error("duplicate run evidence regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("integration CI evidence reuses run 2 already used by PR CI")) {
        throw error;
      }
    }

    try {
      assertDistinctRunEvidence({
        first: { id: "1" },
        second: { id: " 2" },
      });
      throw new Error("padded distinct run id regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("second evidence run id has invalid value  2; want positive integer")) {
        throw error;
      }
    }

    const wrongJobRun = structuredClone(good);
    wrongJobRun.jobs.release = wrongJobRun.jobs.release.map((job) =>
      job.name === "publish-docker" ? { ...job, run_id: 999 } : job,
    );
    const wrongJobRunPath = join(root, "wrong-job-run.json");
    writeFileSync(wrongJobRunPath, JSON.stringify(wrongJobRun));
    try {
      auditSelftestFixture(wrongJobRunPath);
      throw new Error("wrong job run_id regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release job publish-docker belongs to run 999, want 4")) {
        throw error;
      }
    }

    const paddedJobRunId = structuredClone(good);
    paddedJobRunId.jobs.release = paddedJobRunId.jobs.release.map((job) =>
      job.name === "publish-docker" ? { ...job, run_id: " 4" } : job,
    );
    const paddedJobRunIdPath = join(root, "padded-job-run-id.json");
    writeFileSync(paddedJobRunIdPath, JSON.stringify(paddedJobRunId));
    try {
      auditSelftestFixture(paddedJobRunIdPath);
      throw new Error("padded job run_id regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release job publish-docker run_id has invalid value  4; want positive integer")) {
        throw error;
      }
    }

    const missingJobId = structuredClone(good);
    missingJobId.jobs.release = missingJobId.jobs.release.map((job) => {
      if (job.name !== "publish-docker") return job;
      const { id: _id, ...withoutId } = job;
      return withoutId;
    });
    const missingJobIdPath = join(root, "missing-job-id.json");
    writeFileSync(missingJobIdPath, JSON.stringify(missingJobId));
    try {
      auditSelftestFixture(missingJobIdPath);
      throw new Error("missing job id regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release job publish-docker id has invalid value (missing); want positive integer")) {
        throw error;
      }
    }

    const paddedJobId = structuredClone(good);
    paddedJobId.jobs.release = paddedJobId.jobs.release.map((job) =>
      job.name === "publish-docker" ? { ...job, id: " 42" } : job,
    );
    const paddedJobIdPath = join(root, "padded-job-id.json");
    writeFileSync(paddedJobIdPath, JSON.stringify(paddedJobId));
    try {
      auditSelftestFixture(paddedJobIdPath);
      throw new Error("padded job id regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release job publish-docker id has invalid value  42; want positive integer")) {
        throw error;
      }
    }

    const duplicateJobId = structuredClone(good);
    const reusedJobId = duplicateJobId.jobs.release.find((job) => job.name === "publish-docker").id;
    duplicateJobId.jobs.release = duplicateJobId.jobs.release.map((job) =>
      job.name === "ci-green" ? { ...job, id: reusedJobId } : job,
    );
    const duplicateJobIdPath = join(root, "duplicate-job-id.json");
    writeFileSync(duplicateJobIdPath, JSON.stringify(duplicateJobId));
    try {
      auditSelftestFixture(duplicateJobIdPath);
      throw new Error("duplicate job id regression was not caught");
    } catch (error) {
      if (!String(error.message).includes(`release job publish-docker reuses job id ${reusedJobId} already used by ci-green`)) {
        throw error;
      }
    }

    const missingRunAttempt = structuredClone(good);
    delete missingRunAttempt.runs.release.run_attempt;
    const missingRunAttemptPath = join(root, "missing-run-attempt.json");
    writeFileSync(missingRunAttemptPath, JSON.stringify(missingRunAttempt));
    try {
      auditSelftestFixture(missingRunAttemptPath);
      throw new Error("missing run_attempt regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release run_attempt has invalid value (missing); want positive integer")) {
        throw error;
      }
    }

    const wrongJobAttempt = structuredClone(good);
    wrongJobAttempt.runs.release = { ...wrongJobAttempt.runs.release, run_attempt: 2 };
    wrongJobAttempt.jobs.release = wrongJobAttempt.jobs.release.map((job) =>
      job.name === "publish-docker" ? { ...job, run_attempt: 1 } : { ...job, run_attempt: 2 },
    );
    const wrongJobAttemptPath = join(root, "wrong-job-attempt.json");
    writeFileSync(wrongJobAttemptPath, JSON.stringify(wrongJobAttempt));
    try {
      auditSelftestFixture(wrongJobAttemptPath);
      throw new Error("wrong job run_attempt regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release job publish-docker belongs to run attempt 1, want 2")) {
        throw error;
      }
    }

    const paddedJobAttempt = structuredClone(good);
    paddedJobAttempt.runs.release = { ...paddedJobAttempt.runs.release, run_attempt: 2 };
    paddedJobAttempt.jobs.release = paddedJobAttempt.jobs.release.map((job) =>
      job.name === "publish-docker" ? { ...job, run_attempt: " 2" } : { ...job, run_attempt: 2 },
    );
    const paddedJobAttemptPath = join(root, "padded-job-attempt.json");
    writeFileSync(paddedJobAttemptPath, JSON.stringify(paddedJobAttempt));
    try {
      auditSelftestFixture(paddedJobAttemptPath);
      throw new Error("padded job run_attempt regression was not caught");
    } catch (error) {
      if (
        !String(error.message).includes(
          "release job publish-docker run_attempt has invalid value  2; want positive integer",
        )
      ) {
        throw error;
      }
    }

    const impossibleJobWindow = structuredClone(good);
    impossibleJobWindow.jobs.release = impossibleJobWindow.jobs.release.map((job) =>
      job.name === "publish-docker"
        ? { ...job, started_at: "2026-07-01T00:25:00.000Z", completed_at: "2026-07-01T00:20:00.000Z" }
        : job,
    );
    const impossibleJobWindowPath = join(root, "impossible-job-window.json");
    writeFileSync(impossibleJobWindowPath, JSON.stringify(impossibleJobWindow));
    try {
      auditSelftestFixture(impossibleJobWindowPath);
      throw new Error("impossible job timestamp regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release job publish-docker completed before it started")) {
        throw error;
      }
    }

    const wrongIntegrationBranch = structuredClone(good);
    wrongIntegrationBranch.runs.integration = fixtureRun(7, 5, "success", {
      path: ".github/workflows/ci.yml",
      event: "push",
      head_branch: "feature/not-main",
    });
    const wrongIntegrationBranchPath = join(root, "wrong-integration-branch.json");
    writeFileSync(wrongIntegrationBranchPath, JSON.stringify(wrongIntegrationBranch));
    try {
      auditSelftestFixture(wrongIntegrationBranchPath);
      throw new Error("wrong integration branch evidence regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("used branch feature/not-main, want main")) throw error;
    }

    const missingIntegrationDisplayJob = structuredClone(good);
    missingIntegrationDisplayJob.jobs.integration = missingIntegrationDisplayJob.jobs.integration.filter(
      (job) => job.name !== "Native services + canonical stores (live)",
    );
    missingIntegrationDisplayJob.jobs.integration.push(fixtureJob("native-integration"));
    const missingIntegrationDisplayJobPath = join(root, "missing-integration-display-job.json");
    writeFileSync(missingIntegrationDisplayJobPath, JSON.stringify(missingIntegrationDisplayJob));
    try {
      auditSelftestFixture(missingIntegrationDisplayJobPath);
      throw new Error("missing integration display-name job regression was not caught");
    } catch (error) {
      if (
        !String(error.message).includes(
          "integration CI run is missing required jobs: Native services + canonical stores (live)",
        )
      ) {
        throw error;
      }
    }

    const missingIntegrationFullCiJob = structuredClone(good);
    missingIntegrationFullCiJob.jobs.integration = missingIntegrationFullCiJob.jobs.integration.filter(
      (job) => job.name !== "Proto (buf)",
    );
    const missingIntegrationFullCiJobPath = join(root, "missing-integration-full-ci-job.json");
    writeFileSync(missingIntegrationFullCiJobPath, JSON.stringify(missingIntegrationFullCiJob));
    try {
      auditSelftestFixture(missingIntegrationFullCiJobPath);
      throw new Error("missing integration full-CI job regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("integration CI run is missing required jobs: Proto (buf)")) {
        throw error;
      }
    }

    const missingReleaseJob = structuredClone(good);
    missingReleaseJob.jobs.release = missingReleaseJob.jobs.release.filter((job) => job.name !== "publish-docker");
    const missingReleaseJobPath = join(root, "missing-release-job.json");
    writeFileSync(missingReleaseJobPath, JSON.stringify(missingReleaseJob));
    try {
      auditSelftestFixture(missingReleaseJobPath);
      throw new Error("missing release job regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release run is missing required jobs: publish-docker")) throw error;
    }

    try {
      assertRequiredJobs([fixtureJob("publish-docker")], "release", ["publish-docker", "publish-docker"]);
      throw new Error("duplicate required job inventory regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release required job inventory duplicates publish-docker")) {
        throw error;
      }
    }

    const paddedReleaseJobName = structuredClone(good);
    paddedReleaseJobName.jobs.release = paddedReleaseJobName.jobs.release.map((job) =>
      job.name === "publish-docker" ? { ...job, name: " publish-docker " } : job,
    );
    const paddedReleaseJobNamePath = join(root, "padded-release-job-name.json");
    writeFileSync(paddedReleaseJobNamePath, JSON.stringify(paddedReleaseJobName));
    try {
      auditSelftestFixture(paddedReleaseJobNamePath);
      throw new Error("padded job name regression was not caught");
    } catch (error) {
      if (!String(error.message).includes('release job name " publish-docker " must not include surrounding whitespace')) {
        throw error;
      }
    }

    const nonStringReleaseJobName = structuredClone(good);
    nonStringReleaseJobName.jobs.release = nonStringReleaseJobName.jobs.release.map((job) =>
      job.name === "publish-docker" ? { ...job, name: 42 } : job,
    );
    const nonStringReleaseJobNamePath = join(root, "non-string-release-job-name.json");
    writeFileSync(nonStringReleaseJobNamePath, JSON.stringify(nonStringReleaseJobName));
    try {
      auditSelftestFixture(nonStringReleaseJobNamePath);
      throw new Error("non-string job name regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release job name must be a string")) {
        throw error;
      }
    }

    const skippedReleaseJob = structuredClone(good);
    skippedReleaseJob.jobs.release = skippedReleaseJob.jobs.release.map((job) =>
      job.name === "publish-docker" ? { ...job, conclusion: "skipped" } : job,
    );
    const skippedReleaseJobPath = join(root, "skipped-release-job.json");
    writeFileSync(skippedReleaseJobPath, JSON.stringify(skippedReleaseJob));
    try {
      auditSelftestFixture(skippedReleaseJobPath);
      throw new Error("skipped release job regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release job publish-docker did not succeed: skipped")) throw error;
    }

    const duplicateReleaseJob = structuredClone(good);
    duplicateReleaseJob.jobs.release.push(fixtureJob("publish-docker"));
    const duplicateReleaseJobPath = join(root, "duplicate-release-job.json");
    writeFileSync(duplicateReleaseJobPath, JSON.stringify(duplicateReleaseJob));
    try {
      auditSelftestFixture(duplicateReleaseJobPath);
      throw new Error("duplicate release job regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release run has duplicate required job publish-docker; found 2")) throw error;
    }

    const wrongReleaseDryRunEvent = structuredClone(good);
    wrongReleaseDryRunEvent.runs.releaseDryRun = fixtureRun(9, 5, "success", {
      path: ".github/workflows/release-binaries.yml",
      event: "push",
      head_branch: "main",
    });
    const wrongReleaseDryRunEventPath = join(root, "wrong-release-dry-run-event.json");
    writeFileSync(wrongReleaseDryRunEventPath, JSON.stringify(wrongReleaseDryRunEvent));
    try {
      auditSelftestFixture(wrongReleaseDryRunEventPath);
      throw new Error("wrong release dry-run event regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release dry-run run 9 used event push, want workflow_dispatch")) {
        throw error;
      }
    }

    const missingReleaseDryRunJob = structuredClone(good);
    missingReleaseDryRunJob.jobs.releaseDryRun = missingReleaseDryRunJob.jobs.releaseDryRun.filter(
      (job) => job.name !== "build (udb-linux-amd64-full)",
    );
    const missingReleaseDryRunJobPath = join(root, "missing-release-dry-run-job.json");
    writeFileSync(missingReleaseDryRunJobPath, JSON.stringify(missingReleaseDryRunJob));
    try {
      auditSelftestFixture(missingReleaseDryRunJobPath);
      throw new Error("missing release dry-run job regression was not caught");
    } catch (error) {
      if (
        !String(error.message).includes(
          "release dry-run run is missing required jobs: build (udb-linux-amd64-full)",
        )
      ) {
        throw error;
      }
    }

    const wrongReleaseDryRunSha = structuredClone(good);
    wrongReleaseDryRunSha.runs.releaseDryRun = {
      ...wrongReleaseDryRunSha.runs.releaseDryRun,
      head_sha: benchmarkSha,
    };
    const wrongReleaseDryRunShaPath = join(root, "wrong-release-dry-run-sha.json");
    writeFileSync(wrongReleaseDryRunShaPath, JSON.stringify(wrongReleaseDryRunSha));
    try {
      auditSelftestFixture(wrongReleaseDryRunShaPath);
      throw new Error("wrong release dry-run head_sha regression was not caught");
    } catch (error) {
      if (!String(error.message).includes(`release dry-run run 8 used head_sha ${benchmarkSha}, want ${releaseSha}`)) {
        throw error;
      }
    }

    const wrongReleaseDryRunTag = structuredClone(good);
    wrongReleaseDryRunTag.runs.releaseDryRun = {
      ...wrongReleaseDryRunTag.runs.releaseDryRun,
      head_branch: "v0.3.8",
    };
    const wrongReleaseDryRunTagPath = join(root, "wrong-release-dry-run-tag.json");
    writeFileSync(wrongReleaseDryRunTagPath, JSON.stringify(wrongReleaseDryRunTag));
    try {
      auditSelftestFixture(wrongReleaseDryRunTagPath);
      throw new Error("wrong release dry-run tag regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release dry-run run 8 used release tag v0.3.8, want v0.3.7")) {
        throw error;
      }
    }

    const wrongBenchmarkEvent = structuredClone(good);
    wrongBenchmarkEvent.runs.benchmark = fixtureRun(14, 5, "success", {
      path: ".github/workflows/benchmark-sdks.yml",
      event: "workflow_dispatch",
      head_branch: "v0.3.7",
    });
    const wrongBenchmarkEventPath = join(root, "wrong-benchmark-event.json");
    writeFileSync(wrongBenchmarkEventPath, JSON.stringify(wrongBenchmarkEvent));
    try {
      auditSelftestFixture(wrongBenchmarkEventPath);
      throw new Error("wrong benchmark event regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("post-release benchmark run 14 used event workflow_dispatch, want workflow_run")) {
        throw error;
      }
    }

    const missingBenchmarkJob = structuredClone(good);
    missingBenchmarkJob.jobs.benchmark = [];
    const missingBenchmarkJobPath = join(root, "missing-benchmark-job.json");
    writeFileSync(missingBenchmarkJobPath, JSON.stringify(missingBenchmarkJob));
    try {
      auditSelftestFixture(missingBenchmarkJobPath);
      throw new Error("missing benchmark job regression was not caught");
    } catch (error) {
      if (
        !String(error.message).includes(
          "post-release benchmark run is missing required jobs: Release binary + SDK live benchmarks / Live SDK benchmark",
        )
      ) {
        throw error;
      }
    }

    const wrongPagesBranch = structuredClone(good);
    wrongPagesBranch.runs.pages = fixtureRun(15, 5, "success", {
      path: ".github/workflows/pages.yml",
      event: "workflow_run",
      head_branch: "release/v0.3.7",
      head_sha: releaseSha,
    });
    const wrongPagesBranchPath = join(root, "wrong-pages-branch.json");
    writeFileSync(wrongPagesBranchPath, JSON.stringify(wrongPagesBranch));
    try {
      auditSelftestFixture(wrongPagesBranchPath);
      throw new Error("wrong Pages release branch regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("post-benchmark Pages run 15 used branch release/v0.3.7, want main")) {
        throw error;
      }
    }

    const missingPagesDeploy = structuredClone(good);
    missingPagesDeploy.jobs.pages = missingPagesDeploy.jobs.pages.filter((job) => job.name !== "deploy");
    const missingPagesDeployPath = join(root, "missing-pages-deploy.json");
    writeFileSync(missingPagesDeployPath, JSON.stringify(missingPagesDeploy));
    try {
      auditSelftestFixture(missingPagesDeployPath);
      throw new Error("missing Pages deploy regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("post-benchmark Pages run is missing required jobs: deploy")) {
        throw error;
      }
    }

    const missingReleaseSha = structuredClone(good);
    delete missingReleaseSha.runs.release.head_sha;
    const missingReleaseShaPath = join(root, "missing-release-sha.json");
    writeFileSync(missingReleaseShaPath, JSON.stringify(missingReleaseSha));
    try {
      auditSelftestFixture(missingReleaseShaPath);
      throw new Error("missing release head_sha regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("release run 4 has invalid head_sha (missing); want 40 hex characters")) {
        throw error;
      }
    }

    const paddedReleaseSha = structuredClone(good);
    paddedReleaseSha.runs.release = {
      ...paddedReleaseSha.runs.release,
      head_sha: ` ${releaseSha}`,
    };
    const paddedReleaseShaPath = join(root, "padded-release-sha.json");
    writeFileSync(paddedReleaseShaPath, JSON.stringify(paddedReleaseSha));
    try {
      auditSelftestFixture(paddedReleaseShaPath);
      throw new Error("padded release head_sha regression was not caught");
    } catch (error) {
      if (
        !String(error.message).includes(
          `release run 4 has invalid head_sha  ${releaseSha}; want 40 hex characters`,
        )
      ) {
        throw error;
      }
    }

    const uppercaseReleaseSha = structuredClone(good);
    uppercaseReleaseSha.runs.release = {
      ...uppercaseReleaseSha.runs.release,
      head_sha: releaseSha.toUpperCase(),
    };
    const uppercaseReleaseShaPath = join(root, "uppercase-release-sha.json");
    writeFileSync(uppercaseReleaseShaPath, JSON.stringify(uppercaseReleaseSha));
    try {
      auditSelftestFixture(uppercaseReleaseShaPath);
      throw new Error("uppercase release head_sha regression was not caught");
    } catch (error) {
      if (
        !String(error.message).includes(
          `release run 4 has invalid head_sha ${releaseSha.toUpperCase()}; want 40 hex characters`,
        )
      ) {
        throw error;
      }
    }

    const wrongBenchmarkSha = structuredClone(good);
    wrongBenchmarkSha.runs.benchmark = {
      ...wrongBenchmarkSha.runs.benchmark,
      head_sha: benchmarkSha,
    };
    const wrongBenchmarkShaPath = join(root, "wrong-benchmark-sha.json");
    writeFileSync(wrongBenchmarkShaPath, JSON.stringify(wrongBenchmarkSha));
    try {
      auditSelftestFixture(wrongBenchmarkShaPath);
      throw new Error("wrong benchmark head_sha regression was not caught");
    } catch (error) {
      if (!String(error.message).includes(`post-release benchmark run 12 used head_sha ${benchmarkSha}, want ${releaseSha}`)) {
        throw error;
      }
    }

    const wrongPagesSha = structuredClone(good);
    wrongPagesSha.runs.pages = {
      ...wrongPagesSha.runs.pages,
      head_sha: pagesSha,
    };
    const wrongPagesShaPath = join(root, "wrong-pages-sha.json");
    writeFileSync(wrongPagesShaPath, JSON.stringify(wrongPagesSha));
    try {
      auditSelftestFixture(wrongPagesShaPath);
      throw new Error("wrong Pages head_sha regression was not caught");
    } catch (error) {
      if (!String(error.message).includes(`post-benchmark Pages run 13 used head_sha ${pagesSha}, want ${releaseSha}`)) {
        throw error;
      }
    }

    const malformedBenchmarkSha = structuredClone(good);
    malformedBenchmarkSha.runs.benchmark = {
      ...malformedBenchmarkSha.runs.benchmark,
      head_sha: "not-a-sha",
    };
    const malformedBenchmarkShaPath = join(root, "malformed-benchmark-sha.json");
    writeFileSync(malformedBenchmarkShaPath, JSON.stringify(malformedBenchmarkSha));
    try {
      auditSelftestFixture(malformedBenchmarkShaPath);
      throw new Error("malformed benchmark head_sha regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("post-release benchmark run 12 has invalid head_sha not-a-sha; want 40 hex characters")) {
        throw error;
      }
    }

    const earlyBenchmark = structuredClone(good);
    earlyBenchmark.runs.benchmark = {
      ...earlyBenchmark.runs.benchmark,
      created_at: "2026-07-01T00:10:00Z",
      run_started_at: "2026-07-01T00:10:00Z",
      updated_at: "2026-07-01T00:20:00Z",
    };
    const earlyBenchmarkPath = join(root, "early-benchmark.json");
    writeFileSync(earlyBenchmarkPath, JSON.stringify(earlyBenchmark));
    try {
      auditSelftestFixture(earlyBenchmarkPath);
      throw new Error("early benchmark ordering regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("post-release benchmark run 12 started before release run 4 completed")) {
        throw error;
      }
    }

    const earlyPages = structuredClone(good);
    earlyPages.runs.pages = {
      ...earlyPages.runs.pages,
      created_at: "2026-07-01T01:30:00Z",
      run_started_at: "2026-07-01T01:30:00Z",
      updated_at: "2026-07-01T01:40:00Z",
    };
    const earlyPagesPath = join(root, "early-pages.json");
    writeFileSync(earlyPagesPath, JSON.stringify(earlyPages));
    try {
      auditSelftestFixture(earlyPagesPath);
      throw new Error("early Pages ordering regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("post-benchmark Pages run 13 started before benchmark run 12 completed")) {
        throw error;
      }
    }

    const wrongBranchProtectionEvent = structuredClone(good);
    wrongBranchProtectionEvent.runs.branchProtection = fixtureRun(11, 3, "success", {
      path: ".github/workflows/branch-protection-audit.yml",
      event: "push",
      head_branch: "main",
    });
    const wrongBranchProtectionEventPath = join(root, "wrong-branch-protection-event.json");
    writeFileSync(wrongBranchProtectionEventPath, JSON.stringify(wrongBranchProtectionEvent));
    try {
      auditSelftestFixture(wrongBranchProtectionEventPath);
      throw new Error("wrong branch-protection event regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("branch-protection run 11 used event push, want workflow_dispatch")) {
        throw error;
      }
    }

    const wrongBranchProtectionBranch = structuredClone(good);
    wrongBranchProtectionBranch.runs.branchProtection = fixtureRun(18, 3, "success", {
      path: ".github/workflows/branch-protection-audit.yml",
      event: "workflow_dispatch",
      head_branch: "feature/branch-protection-proof",
    });
    const wrongBranchProtectionBranchPath = join(root, "wrong-branch-protection-branch.json");
    writeFileSync(wrongBranchProtectionBranchPath, JSON.stringify(wrongBranchProtectionBranch));
    try {
      auditSelftestFixture(wrongBranchProtectionBranchPath);
      throw new Error("wrong branch-protection branch regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("branch-protection run 18 used branch feature/branch-protection-proof, want main")) {
        throw error;
      }
    }

    const missingBranchProtectionJob = structuredClone(good);
    missingBranchProtectionJob.jobs.branchProtection = [];
    const missingBranchProtectionJobPath = join(root, "missing-branch-protection-job.json");
    writeFileSync(missingBranchProtectionJobPath, JSON.stringify(missingBranchProtectionJob));
    try {
      auditSelftestFixture(missingBranchProtectionJobPath);
      throw new Error("missing branch-protection job regression was not caught");
    } catch (error) {
      if (
        !String(error.message).includes(
          "branch-protection run is missing required jobs: Branch protection required checks match docs",
        )
      ) {
        throw error;
      }
    }

    const wrongBranchProtectionSha = structuredClone(good);
    wrongBranchProtectionSha.runs.branchProtection = {
      ...wrongBranchProtectionSha.runs.branchProtection,
      head_sha: benchmarkSha,
    };
    const wrongBranchProtectionShaPath = join(root, "wrong-branch-protection-sha.json");
    writeFileSync(wrongBranchProtectionShaPath, JSON.stringify(wrongBranchProtectionSha));
    try {
      auditSelftestFixture(wrongBranchProtectionShaPath);
      throw new Error("wrong branch-protection head_sha regression was not caught");
    } catch (error) {
      if (!String(error.message).includes(`branch-protection run 10 used head_sha ${benchmarkSha}, want ${integrationSha}`)) {
        throw error;
      }
    }

    const fetchedUrls = [];
    const pages = [
      {
        total_count: 101,
        jobs: Array.from({ length: 100 }, (_, index) => fixtureJob(`page-one-${index}`)),
      },
      {
        total_count: 101,
        jobs: [fixtureJob("page-two-final")],
      },
    ];
    const pagedJobs = await fetchRunJobs("owner/repo", "token", 123, async (url) => {
      fetchedUrls.push(url);
      return pages.shift();
    });
    if (pagedJobs.length !== 101 || !fetchedUrls.some((url) => url.includes("page=2"))) {
      throw new Error("paginated jobs regression was not caught");
    }

    try {
      await fetchRunJobs("owner/repo", "token", 124, async () => ({
        total_count: 101,
        jobs: [fixtureJob("only-one")],
      }));
      throw new Error("truncated jobs pagination regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("jobs pagination returned 1/101 jobs")) throw error;
    }

    try {
      await fetchRunJobs("owner/repo", "token", 129, async () => ({
        total_count: 1,
        jobs: [fixtureJob("first"), fixtureJob("second")],
      }));
      throw new Error("overreported jobs pagination regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("run 129 jobs pagination returned 2/1 jobs")) throw error;
    }

    try {
      await fetchRunJobs("owner/repo", "token", 131, async () => ({
        total_count: MAX_GITHUB_JOBS_PAGE_SIZE + 1,
        jobs: Array.from({ length: MAX_GITHUB_JOBS_PAGE_SIZE + 1 }, (_, index) =>
          fixtureJob(`oversized-page-${index}`),
        ),
      }));
      throw new Error("oversized jobs page regression was not caught");
    } catch (error) {
      if (
        !String(error.message).includes(
          `run 131 jobs page 1 response returned ${MAX_GITHUB_JOBS_PAGE_SIZE + 1} jobs, max ${MAX_GITHUB_JOBS_PAGE_SIZE}`,
        )
      ) {
        throw error;
      }
    }

    try {
      await fetchRunJobs("owner/repo", "token", 125, async () => ({
        total_count: 1,
      }));
      throw new Error("missing jobs array regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("run 125 jobs page 1 response must include jobs array")) throw error;
    }

    try {
      await fetchRunJobs("owner/repo", "token", 126, async () => ({
        jobs: [],
      }));
      throw new Error("missing jobs total_count regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("run 126 jobs page 1 response must include non-negative integer total_count")) {
        throw error;
      }
    }

    try {
      await fetchRunJobs("owner/repo", "token", 130, async () => ({
        total_count: MAX_GITHUB_RUN_JOBS + 1,
        jobs: [],
      }));
      throw new Error("oversized jobs total_count regression was not caught");
    } catch (error) {
      if (
        !String(error.message).includes(
          `run 130 jobs page 1 response total_count ${MAX_GITHUB_RUN_JOBS + 1} exceeds ${MAX_GITHUB_RUN_JOBS}`,
        )
      ) {
        throw error;
      }
    }

    const changedTotalCountPages = [
      {
        total_count: 101,
        jobs: Array.from({ length: 100 }, (_, index) => fixtureJob(`changed-total-page-one-${index}`)),
      },
      {
        total_count: 102,
        jobs: [fixtureJob("changed-total-page-two")],
      },
    ];
    try {
      await fetchRunJobs("owner/repo", "token", 127, async () => changedTotalCountPages.shift());
      throw new Error("changed jobs total_count regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("run 127 jobs pagination total_count changed from 101 to 102")) throw error;
    }

    try {
      await fetchRun("owner/repo", "token", 128, async () => []);
      throw new Error("malformed exact run response regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("run 128 response must be a JSON object")) throw error;
    }

    try {
      await fetchRun("owner/repo", "token", 131, async () => ({}));
      throw new Error("missing exact run id regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("run 131 response id has invalid value (missing); want positive integer")) {
        throw error;
      }
    }

    try {
      await fetchRun("owner/repo", "token", 132, async () => ({ id: 133 }));
      throw new Error("wrong exact run id regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("run 132 response id 133, want 132")) throw error;
    }

    try {
      await fetchRun("owner/repo", "token", 134, async () => ({ id: " 134" }));
      throw new Error("padded exact run id regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("run 134 response id has invalid value  134; want positive integer")) {
        throw error;
      }
    }

    try {
      await fetchRunJobs("owner/repo", "token", 129, async () => ({
        total_count: 1,
        jobs: [null],
      }));
      throw new Error("malformed job entry regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("run 129 jobs page 1 jobs[0] must be a JSON object")) throw error;
    }

    try {
      await findLatestSuccessfulRun("owner/repo", "token", WORKFLOWS.pr, {}, async () => ({
        workflow_runs: {},
      }));
      throw new Error("malformed workflow runs response regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("ci.yml runs response must include workflow_runs array")) throw error;
    }

    try {
      await findLatestSuccessfulRun("owner/repo", "token", WORKFLOWS.pr, {}, async () => ({
        workflow_runs: [null],
      }));
      throw new Error("malformed workflow run entry regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("ci.yml runs workflow_runs[0] must be a JSON object")) throw error;
    }

    const completedDiscoveredRun = await findLatestSuccessfulRun("owner/repo", "token", WORKFLOWS.pr, {}, async () => ({
      workflow_runs: [
        {
          id: 200,
          status: "in_progress",
          conclusion: "success",
          event: "pull_request",
        },
        {
          id: 201,
          status: "completed",
          conclusion: "success",
          event: "pull_request",
        },
      ],
    }));
    if (completedDiscoveredRun.id !== 201) {
      throw new Error("incomplete workflow run discovery regression was not caught");
    }

    const workflowRunUrls = [];
    try {
      await findLatestSuccessfulRun("owner/repo", "token", WORKFLOWS.pr, {}, async (url) => {
        workflowRunUrls.push(url);
        return { workflow_runs: [] };
      });
      throw new Error("bounded workflow run discovery regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("no successful completed ci.yml run found")) throw error;
      if (!workflowRunUrls.some((url) => url.includes(`per_page=${MAX_GITHUB_WORKFLOW_RUN_CANDIDATES}`))) {
        throw new Error("workflow run discovery candidate limit was not requested");
      }
    }

    const releaseDryRunLookupUrls = [];
    try {
      await findLatestSuccessfulRun(
        "owner/repo",
        "token",
        WORKFLOWS.releaseDryRun,
        { event: "workflow_dispatch", branch: "v0.3.7" },
        async (url) => {
          releaseDryRunLookupUrls.push(url);
          return { workflow_runs: [] };
        },
      );
      throw new Error("release dry-run tag-filtered lookup regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("no successful completed release-binaries.yml run found")) throw error;
      if (
        !releaseDryRunLookupUrls.some(
          (url) => url.includes("event=workflow_dispatch") && url.includes("branch=v0.3.7"),
        )
      ) {
        throw new Error("release dry-run lookup did not request workflow_dispatch branch v0.3.7");
      }
    }

    const branchProtectionLookupUrls = [];
    try {
      await findLatestSuccessfulRun(
        "owner/repo",
        "token",
        WORKFLOWS.branchProtection,
        { event: "workflow_dispatch", branch: "main" },
        async (url) => {
          branchProtectionLookupUrls.push(url);
          return { workflow_runs: [] };
        },
      );
      throw new Error("branch-protection branch-filtered lookup regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("no successful completed branch-protection-audit.yml run found")) throw error;
      if (
        !branchProtectionLookupUrls.some(
          (url) => url.includes("event=workflow_dispatch") && url.includes("branch=main"),
        )
      ) {
        throw new Error("branch-protection lookup did not request workflow_dispatch branch main");
      }
    }

    const idempotencyRun = fixtureRun(91, 5, "success", {
      html_url: "https://github.com/owner/repo/actions/runs/91",
      path: ".github/workflows/idempotency-served-smoke.yml",
      event: "workflow_dispatch",
      head_branch: "main",
    });
    const idempotencyJob = fixtureJob("DataBroker idempotency served replay proof", "success", {
      run_id: 91,
      run_attempt: 1,
      started_at: "2026-07-01T00:01:00Z",
      completed_at: "2026-07-01T00:04:00Z",
    });
    const idempotencyLookupUrls = [];
    const idempotencyFetcher = async (url) => {
      if (url.includes("/actions/workflows/idempotency-served-smoke.yml/runs")) {
        idempotencyLookupUrls.push(url);
        return { workflow_runs: [idempotencyRun] };
      }
      if (url.includes("/actions/runs/91/jobs")) {
        return { total_count: 1, jobs: [idempotencyJob] };
      }
      throw new Error(`unexpected idempotency served evidence URL ${url}`);
    };
    const idempotencySummary = await auditIdempotencyServed(
      ["--repo", "owner/repo"],
      DEFAULT_BUDGETS,
      selftestEvidenceOptions,
      idempotencyFetcher,
    );
    if (idempotencySummary.runId !== "91" || idempotencySummary.idempotencyServed !== 5) {
      throw new Error("idempotency served evidence summary regression was not caught");
    }
    if (
      !idempotencyLookupUrls.some(
        (url) => url.includes("event=workflow_dispatch") && url.includes("branch=main"),
      )
    ) {
      throw new Error("idempotency served lookup did not request workflow_dispatch branch main");
    }

    try {
      await auditIdempotencyServed(
        ["--repo", "owner/repo"],
        DEFAULT_BUDGETS,
        selftestEvidenceOptions,
        async (url) => {
          if (url.includes("/actions/workflows/idempotency-served-smoke.yml/runs")) {
            return { workflow_runs: [idempotencyRun] };
          }
          if (url.includes("/actions/runs/91/jobs")) {
            return { total_count: 1, jobs: [fixtureJob("wrong proof job", "success", {
              run_id: 91,
              run_attempt: 1,
              started_at: "2026-07-01T00:01:00Z",
              completed_at: "2026-07-01T00:04:00Z",
            })] };
          }
          throw new Error(`unexpected idempotency served missing-job URL ${url}`);
        },
      );
      throw new Error("idempotency served missing proof job regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("idempotency served replay run is missing required jobs: DataBroker idempotency served replay proof")) {
        throw error;
      }
    }

    const servedSmokeSelftests = [
      ["errorDetailServed", 92, "ErrorDetail served transport proof", "--error-detail-served-smoke"],
      ["retrySafeServed", 93, "Retry-safe mutation metadata served proof", "--retry-safe-served-smoke"],
      ["restGateway", 94, "REST boundary content/status proof", "--rest-gateway-smoke"],
    ];
    for (const [auditKey, runId, jobName, mode] of servedSmokeSelftests) {
      const audit = SERVED_SMOKE_AUDITS[auditKey];
      const run = fixtureRun(runId, 6, "success", {
        html_url: `https://github.com/owner/repo/actions/runs/${runId}`,
        path: `.github/workflows/${WORKFLOWS[auditKey]}`,
        event: "workflow_dispatch",
        head_branch: "main",
      });
      const job = fixtureJob(jobName, "success", {
        run_id: runId,
        run_attempt: 1,
        started_at: "2026-07-01T00:01:00Z",
        completed_at: "2026-07-01T00:05:00Z",
      });
      const lookupUrls = [];
      const summary = await auditServedSmoke(
        ["--repo", "owner/repo", mode],
        DEFAULT_BUDGETS,
        auditKey,
        selftestEvidenceOptions,
        async (url) => {
          if (url.includes(`/actions/workflows/${WORKFLOWS[auditKey]}/runs`)) {
            lookupUrls.push(url);
            return { workflow_runs: [run] };
          }
          if (url.includes(`/actions/runs/${runId}/jobs`)) {
            return { total_count: 1, jobs: [job] };
          }
          throw new Error(`unexpected ${audit.label} evidence URL ${url}`);
        },
      );
      if (summary.runId !== String(runId) || summary[auditKey] !== 6) {
        throw new Error(`${audit.label} evidence summary regression was not caught`);
      }
      if (
        !lookupUrls.some(
          (url) => url.includes("event=workflow_dispatch") && url.includes("branch=main"),
        )
      ) {
        throw new Error(`${audit.label} lookup did not request workflow_dispatch branch main`);
      }
    }

    const multiServedRun = fixtureRun(95, 7, "success", {
      html_url: "https://github.com/owner/repo/actions/runs/95",
      path: ".github/workflows/rest-gateway-smoke.yml",
      event: "workflow_dispatch",
      head_branch: "main",
    });
    const multiServedJob = fixtureJob("REST boundary content/status proof", "success", {
      run_id: 95,
      run_attempt: 1,
      started_at: "2026-07-01T00:01:00Z",
      completed_at: "2026-07-01T00:06:00Z",
    });
    const multiServedUrls = [];
    const multiServedSummary = await auditRequestedServedSmokes(
      ["--repo", "owner/repo", "--idempotency-served-smoke", "--rest-gateway-smoke"],
      DEFAULT_BUDGETS,
      selftestEvidenceOptions,
      async (url) => {
        multiServedUrls.push(url);
        if (url.includes("/actions/workflows/idempotency-served-smoke.yml/runs")) {
          return { workflow_runs: [idempotencyRun] };
        }
        if (url.includes("/actions/runs/91/jobs")) {
          return { total_count: 1, jobs: [idempotencyJob] };
        }
        if (url.includes("/actions/workflows/rest-gateway-smoke.yml/runs")) {
          return { workflow_runs: [multiServedRun] };
        }
        if (url.includes("/actions/runs/95/jobs")) {
          return { total_count: 1, jobs: [multiServedJob] };
        }
        throw new Error(`unexpected multi-served evidence URL ${url}`);
      },
    );
    if (
      multiServedSummary.idempotencyServedRunId !== "91"
      || multiServedSummary.restGatewayRunId !== "95"
      || multiServedSummary.idempotencyServed !== 5
      || multiServedSummary.restGateway !== 7
    ) {
      throw new Error("multi-served evidence aggregation regression was not caught");
    }
    if (
      !multiServedUrls.some((url) => url.includes("/idempotency-served-smoke.yml/runs"))
      || !multiServedUrls.some((url) => url.includes("/rest-gateway-smoke.yml/runs"))
    ) {
      throw new Error("multi-served evidence lookup did not audit every requested served workflow");
    }

    try {
      await auditRequestedServedSmokes(
        ["--repo", "owner/repo", "--idempotency-served-smoke", "--rest-gateway-smoke"],
        DEFAULT_BUDGETS,
        selftestEvidenceOptions,
        async (url) => {
          if (
            url.includes("/actions/workflows/idempotency-served-smoke.yml/runs")
            || url.includes("/actions/workflows/rest-gateway-smoke.yml/runs")
          ) {
            return { workflow_runs: [] };
          }
          throw new Error(`unexpected multi-served missing evidence URL ${url}`);
        },
      );
      throw new Error("multi-served missing evidence aggregation regression was not caught");
    } catch (error) {
      const message = String(error.message);
      if (
        !message.includes("served evidence audit failed:") ||
        !message.includes("idempotency served replay: no successful completed idempotency-served-smoke.yml run found") ||
        !message.includes("REST gateway boundary: no successful completed rest-gateway-smoke.yml run found")
      ) {
        throw error;
      }
    }

    try {
      await auditLive(
        ["--repo", "owner/repo"],
        DEFAULT_BUDGETS,
        {},
        async (url) => {
          if (url.includes("/actions/workflows/ci.yml/runs") && url.includes("event=pull_request")) {
            return { workflow_runs: [] };
          }
          if (url.includes("/actions/workflows/ci.yml/runs") && url.includes("event=push")) {
            return { workflow_runs: [] };
          }
          const workflow = decodeURIComponent(url.match(/\/actions\/workflows\/([^/]+)\/runs/)?.[1] || "workflow.yml");
          return {
            workflow_runs: [
              {
                id: workflow.length,
                status: "completed",
                conclusion: "success",
                event: url.includes("workflow_dispatch")
                  ? "workflow_dispatch"
                  : url.includes("workflow_run")
                    ? "workflow_run"
                    : "push",
                path: `.github/workflows/${workflow}`,
                head_branch: workflow === WORKFLOWS.release ? "v0.3.7" : "main",
              },
            ],
          };
        },
      );
      throw new Error("aggregate live discovery regression was not caught");
    } catch (error) {
      const message = String(error.message);
      if (
        !message.includes("runner evidence discovery failed:") ||
        !message.includes("PR CI: no successful completed ci.yml run found") ||
        !message.includes("integration CI: no successful completed ci.yml run found")
      ) {
        throw error;
      }
    }

    try {
      await auditAllEvidence(
        ["--repo", "owner/repo", "--all-evidence", "--idempotency-served-smoke", "--rest-gateway-smoke"],
        DEFAULT_BUDGETS,
        selftestEvidenceOptions,
        async (url) => {
          if (url.includes("/actions/workflows/ci.yml/runs") && url.includes("event=pull_request")) {
            return { workflow_runs: [] };
          }
          if (
            url.includes("/actions/workflows/idempotency-served-smoke.yml/runs")
            || url.includes("/actions/workflows/rest-gateway-smoke.yml/runs")
          ) {
            return { workflow_runs: [] };
          }
          const workflow = decodeURIComponent(url.match(/\/actions\/workflows\/([^/]+)\/runs/)?.[1] || "workflow.yml");
          return {
            workflow_runs: [
              {
                id: 200 + workflow.length,
                status: "completed",
                conclusion: "success",
                event: url.includes("workflow_dispatch")
                  ? "workflow_dispatch"
                  : url.includes("workflow_run")
                    ? "workflow_run"
                    : "push",
                path: `.github/workflows/${workflow}`,
                head_branch: workflow === WORKFLOWS.release ? "v0.3.7" : "main",
                head_sha: "0123456789abcdef0123456789abcdef01234567",
                html_url: `https://github.com/owner/repo/actions/runs/${200 + workflow.length}`,
                run_attempt: 1,
                created_at: "2026-07-01T00:00:00Z",
                run_started_at: "2026-07-01T00:00:00Z",
                updated_at: "2026-07-01T00:05:00Z",
                completed_at: "2026-07-01T00:05:00Z",
              },
            ],
          };
        },
      );
      throw new Error("all-evidence base plus served failure aggregation regression was not caught");
    } catch (error) {
      const message = String(error.message);
      if (
        !message.includes("runner evidence audit failed:") ||
        !message.includes("CI runner evidence: runner evidence discovery failed:") ||
        !message.includes("PR CI: no successful completed ci.yml run found") ||
        !message.includes("served evidence: served evidence audit failed:") ||
        !message.includes("idempotency served replay: no successful completed idempotency-served-smoke.yml run found") ||
        !message.includes("REST gateway boundary: no successful completed rest-gateway-smoke.yml run found")
      ) {
        throw error;
      }
    }

    try {
      await findLatestSuccessfulRun("owner/repo", "token", WORKFLOWS.pr, {}, async () => ({
        workflow_runs: Array.from({ length: MAX_GITHUB_WORKFLOW_RUN_CANDIDATES + 1 }, (_, index) => ({
          id: index + 1,
        })),
      }));
      throw new Error("oversized workflow runs response regression was not caught");
    } catch (error) {
      if (
        !String(error.message).includes(
          `ci.yml runs response returned ${MAX_GITHUB_WORKFLOW_RUN_CANDIDATES + 1} workflow_runs, max ${MAX_GITHUB_WORKFLOW_RUN_CANDIDATES}`,
        )
      ) {
        throw error;
      }
    }

    try {
      appendGitHubApiChunk("x".repeat(MAX_GITHUB_API_RESPONSE_BYTES), "x", "selftest");
      throw new Error("oversized GitHub API response regression was not caught");
    } catch (error) {
      if (!String(error.message).includes(`GitHub API response exceeded ${MAX_GITHUB_API_RESPONSE_BYTES} bytes`)) {
        throw error;
      }
    }

    assertGitHubApiSuccessStatus({ statusCode: 200 }, "", "selftest");
    try {
      assertGitHubApiSuccessStatus({}, "", "selftest");
      throw new Error("missing GitHub API status-code regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("must include an integer HTTP status code")) {
        throw error;
      }
    }
    try {
      assertGitHubApiSuccessStatus({ statusCode: "200" }, "", "selftest");
      throw new Error("malformed GitHub API status-code regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("must include an integer HTTP status code")) {
        throw error;
      }
    }
    try {
      assertGitHubApiSuccessStatus({ statusCode: 500 }, "server boom", "selftest");
      throw new Error("non-success GitHub API status-code regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("GitHub API 500: server boom")) {
        throw error;
      }
    }
    try {
      assertGitHubApiSuccessStatus(
        { statusCode: 404 },
        '{"message":"Not Found"}',
        "https://api.github.com/repos/owner/repo/actions/workflows/rest-gateway-smoke.yml/runs?status=completed",
      );
      throw new Error("missing workflow GitHub API regression was not caught");
    } catch (error) {
      if (
        !String(error.message).includes("GitHub Actions workflow rest-gateway-smoke.yml is not visible in owner/repo") ||
        !String(error.message).includes("local file .github/workflows/rest-gateway-smoke.yml") ||
        !String(error.message).includes("default branch")
      ) {
        throw error;
      }
    }
    try {
      assertGitHubApiSuccessStatus(
        {
          statusCode: 403,
          headers: {
            "x-ratelimit-remaining": "0",
            "x-ratelimit-reset": "1893456000",
          },
        },
        '{"message":"API rate limit exceeded"}',
        "selftest",
      );
      throw new Error("GitHub API rate-limit regression was not caught");
    } catch (error) {
      if (
        !String(error.message).includes("GitHub API rate limit exceeded for selftest") ||
        !String(error.message).includes("set GH_TOKEN or GITHUB_TOKEN") ||
        !String(error.message).includes("2030-01-01T00:00:00.000Z")
      ) {
        throw error;
      }
    }
    try {
      assertGitHubApiSuccessStatus(
        {
          statusCode: 429,
          headers: {},
        },
        "secondary rate limit",
        "selftest",
      );
      throw new Error("GitHub API secondary-rate-limit regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("GitHub API rate limit exceeded for selftest")) {
        throw error;
      }
    }

    assertGitHubApiJsonContentType({ headers: { "content-type": "application/json; charset=utf-8" } }, "selftest");
    assertGitHubApiJsonContentType({ headers: { "content-type": "application/vnd.github+json" } }, "selftest");
    const requestHeaderSets = [];
    const originalRequest = https.request;
    https.request = (_url, options, _callback) => {
      requestHeaderSets.push(options.headers);
      return {
        on() {
          return this;
        },
        setTimeout() {
          return this;
        },
        end() {
          return this;
        },
      };
    };
    try {
      fetchJson("https://api.github.com/repos/owner/repo/actions/runs/1", "");
      fetchJson("https://api.github.com/repos/owner/repo/actions/runs/2", "token");
    } finally {
      https.request = originalRequest;
    }
    if (Object.prototype.hasOwnProperty.call(requestHeaderSets[0] || {}, "Authorization")) {
      throw new Error("unauthenticated public GitHub request regression was not caught");
    }
    if ((requestHeaderSets[1] || {}).Authorization !== "Bearer token") {
      throw new Error("authenticated GitHub request regression was not caught");
    }
    try {
      assertGitHubApiJsonContentType({ headers: {} }, "selftest");
      throw new Error("missing GitHub API content-type regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("must include a JSON Content-Type")) {
        throw error;
      }
    }
    try {
      assertGitHubApiJsonContentType({ headers: { "content-type": " application/json " } }, "selftest");
      throw new Error("padded GitHub API content-type regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("Content-Type must not include surrounding whitespace")) {
        throw error;
      }
    }
    try {
      assertGitHubApiJsonContentType({ headers: { "content-type": "text/html" } }, "selftest");
      throw new Error("non-JSON GitHub API content-type regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("must be JSON, got text/html")) {
        throw error;
      }
    }

    try {
      rejectDuplicateJsonObjectKeys("{\"workflow_runs\":[],\"workflow\\u005fruns\":[]}", "GitHub API response selftest");
      throw new Error("duplicate-key GitHub API response regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("GitHub API response selftest has duplicate JSON object key \"workflow_runs\"")) {
        throw error;
      }
    }

    const timeoutError = githubApiTimeoutError("selftest");
    if (!String(timeoutError.message).includes(`timed out after ${GITHUB_API_REQUEST_TIMEOUT_MS} ms for selftest`)) {
      throw new Error("GitHub API request timeout regression was not caught");
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
  console.log("CI runner evidence selftest passed");
}

async function main() {
  const args = process.argv.slice(2);
  assertKnownArgs(args);
  const budgets = {
    pr: boundedBudgetArg(args, "--pr-budget-minutes", DEFAULT_BUDGETS.pr, MAX_BUDGETS.pr),
    integration: boundedBudgetArg(args, "--integration-budget-minutes", DEFAULT_BUDGETS.integration, MAX_BUDGETS.integration),
    release: boundedBudgetArg(args, "--release-budget-minutes", DEFAULT_BUDGETS.release, MAX_BUDGETS.release),
    releaseDryRun: boundedBudgetArg(
      args,
      "--release-dry-run-budget-minutes",
      DEFAULT_BUDGETS.releaseDryRun,
      MAX_BUDGETS.releaseDryRun,
    ),
    benchmark: boundedBudgetArg(args, "--benchmark-budget-minutes", DEFAULT_BUDGETS.benchmark, MAX_BUDGETS.benchmark),
    pages: boundedBudgetArg(args, "--pages-budget-minutes", DEFAULT_BUDGETS.pages, MAX_BUDGETS.pages),
    lint: boundedBudgetArg(args, "--lint-budget-minutes", DEFAULT_BUDGETS.lint, MAX_BUDGETS.lint),
    branchProtection: boundedBudgetArg(
      args,
      "--branch-protection-budget-minutes",
      DEFAULT_BUDGETS.branchProtection,
      MAX_BUDGETS.branchProtection,
    ),
    idempotencyServed: boundedBudgetArg(
      args,
      "--idempotency-served-budget-minutes",
      DEFAULT_BUDGETS.idempotencyServed,
      MAX_BUDGETS.idempotencyServed,
    ),
    errorDetailServed: boundedBudgetArg(
      args,
      "--error-detail-served-budget-minutes",
      DEFAULT_BUDGETS.errorDetailServed,
      MAX_BUDGETS.errorDetailServed,
    ),
    retrySafeServed: boundedBudgetArg(
      args,
      "--retry-safe-served-budget-minutes",
      DEFAULT_BUDGETS.retrySafeServed,
      MAX_BUDGETS.retrySafeServed,
    ),
    restGateway: boundedBudgetArg(
      args,
      "--rest-gateway-budget-minutes",
      DEFAULT_BUDGETS.restGateway,
      MAX_BUDGETS.restGateway,
    ),
  };
  const maxEvidenceAgeDays = boundedMaxEvidenceAgeArg(
    args,
    "--max-evidence-age-days",
    DEFAULT_MAX_EVIDENCE_AGE_DAYS,
    MAX_EVIDENCE_AGE_DAYS,
  );
  if (args.includes("--selftest")) {
    await runSelftest();
    return;
  }
  const servedAuditKeys = requestedServedAuditKeys(args);
  assertNoUnusedEvidenceOverrides(args, servedAuditKeys);
  const evidenceOptions = { maxAgeDays: maxEvidenceAgeDays };
  if (servedAuditKeys.length > 0 && !args.includes(ALL_EVIDENCE_MODE)) {
    const summary = await auditRequestedServedSmokes(args, budgets, evidenceOptions);
    if (servedAuditKeys.length === 1 && servedAuditKeys[0] === "idempotencyServed") {
      console.log(
        `idempotency served evidence passed: run=${summary.idempotencyServedRunId}, duration=${summary.idempotencyServed.toFixed(2)}m`,
      );
    } else {
      console.log(`served evidence passed: ${servedEvidenceSummaryText(summary, servedAuditKeys)}`);
    }
    return;
  }
  const { summary, servedSummary } = await auditAllEvidence(args, budgets, evidenceOptions);
  const servedText = servedAuditKeys.length > 0
    ? ` served=${servedEvidenceSummaryText(servedSummary, servedAuditKeys)}`
    : "";
  console.log(
    `CI runner evidence passed: lint=${summary.lint.toFixed(2)}m, PR=${summary.pr.toFixed(2)}m, integration=${summary.integration.toFixed(2)}m, release=${summary.release.toFixed(2)}m, releaseDryRun=${summary.releaseDryRun.toFixed(2)}m`,
    `benchmark=${summary.benchmark.toFixed(2)}m, pages=${summary.pages.toFixed(2)}m, branchProtection=${summary.branchProtection.toFixed(2)}m, releaseTag=${summary.releaseTag}${servedText}`,
  );
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
