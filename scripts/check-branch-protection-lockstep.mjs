#!/usr/bin/env node
// Compare docs/ci-architecture.md required PR checks with GitHub branch protection.
//
// ci-inventory.mjs proves the source contract. This audit proves the repository
// setting when run with a token that can read branch protection.

import { mkdtempSync, rmSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import https from "node:https";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function argValue(args, name, fallback = undefined) {
  const index = args.indexOf(name);
  if (index < 0) return fallback;
  if (index + 1 >= args.length) throw new Error(`${name} requires a value`);
  return args[index + 1];
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

function requiredCheckNamesFromArchitecture(text) {
  const marker = "Required reported check names (branch protection):";
  const start = text.indexOf(marker);
  if (start < 0) return [];
  const tail = text.slice(start);
  const end = tail.search(/\n\s*\n/);
  const block = end >= 0 ? tail.slice(0, end) : tail;
  return [...block.matchAll(/`([^`]+)`/g)].map((match) => match[1]);
}

function normalizeRequiredStatusChecks(payload) {
  const names = new Set();
  for (const context of payload.contexts || []) {
    if (typeof context === "string" && context.trim()) names.add(context.trim());
  }
  for (const check of payload.checks || []) {
    const context = typeof check?.context === "string" ? check.context.trim() : "";
    if (context) names.add(context);
  }
  return [...names].sort();
}

function compareRequiredChecks(documented, actual) {
  const documentedSet = new Set(documented);
  const actualSet = new Set(actual);
  const duplicateDocumented = documented.filter((name, index) => documented.indexOf(name) !== index);
  return {
    duplicateDocumented: [...new Set(duplicateDocumented)].sort(),
    missingInBranchProtection: documented.filter((name) => !actualSet.has(name)).sort(),
    staleInBranchProtection: actual.filter((name) => !documentedSet.has(name)).sort(),
  };
}

function fetchJson(url, token) {
  return new Promise((resolvePromise, reject) => {
    const request = https.request(
      url,
      {
        headers: {
          Accept: "application/vnd.github+json",
          Authorization: `Bearer ${token}`,
          "User-Agent": "udb-branch-protection-lockstep",
          "X-GitHub-Api-Version": "2022-11-28",
        },
      },
      (response) => {
        let body = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          body += chunk;
        });
        response.on("end", () => {
          if (response.statusCode < 200 || response.statusCode >= 300) {
            reject(new Error(`GitHub API ${response.statusCode}: ${body.slice(0, 500)}`));
            return;
          }
          try {
            resolvePromise(JSON.parse(body));
          } catch (error) {
            reject(new Error(`GitHub API returned invalid JSON: ${error.message}`));
          }
        });
      },
    );
    request.on("error", reject);
    request.end();
  });
}

function assertLockstep(documented, actual, label = "branch protection") {
  if (!documented.length) throw new Error("docs/ci-architecture.md has no documented required check names");
  if (!actual.length) throw new Error(`${label} has no required status checks`);
  const diff = compareRequiredChecks(documented, actual);
  const errors = [];
  if (diff.duplicateDocumented.length) {
    errors.push(`duplicate documented check(s): ${diff.duplicateDocumented.join(", ")}`);
  }
  if (diff.missingInBranchProtection.length) {
    errors.push(`missing from ${label}: ${diff.missingInBranchProtection.join(", ")}`);
  }
  if (diff.staleInBranchProtection.length) {
    errors.push(`stale in ${label}: ${diff.staleInBranchProtection.join(", ")}`);
  }
  if (errors.length) throw new Error(errors.join("\n"));
}

function runSelftest() {
  const root = mkdtempSync(join(tmpdir(), "udb-branch-protection-"));
  try {
    const docs = `# CI Architecture

Required reported check names (branch protection): \`quick-gate\`,
\`Proto (buf)\`, \`Version consistency\`.
`;
    const docPath = join(root, "ci-architecture.md");
    writeFileSync(docPath, docs);
    const documented = requiredCheckNamesFromArchitecture(readFileSync(docPath, "utf8"));
    const actual = normalizeRequiredStatusChecks({
      contexts: ["quick-gate"],
      checks: [{ context: "Proto (buf)" }, { context: "Version consistency" }],
    });
    assertLockstep(documented, actual, "fixture branch protection");

    try {
      assertLockstep(documented, actual.filter((name) => name !== "Version consistency"), "fixture branch protection");
      throw new Error("missing required check regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("missing from fixture branch protection")) throw error;
    }

    try {
      assertLockstep(documented, [...actual, "old-check"], "fixture branch protection");
      throw new Error("stale required check regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("stale in fixture branch protection")) throw error;
    }

    try {
      repoArg(["--repo", " fahara02/udb "], "--repo", undefined);
      throw new Error("padded repository input regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--repo must not include surrounding whitespace")) throw error;
    }
    try {
      repoArg(["--repo", "fahara02"], "--repo", undefined);
      throw new Error("malformed repository input regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--repo must be an owner/repo repository name")) throw error;
    }
    if (repoArg(["--repo", "fahara02/udb"], "--repo", undefined) !== "fahara02/udb") {
      throw new Error("canonical repository input was rejected");
    }

    try {
      branchArg(["--branch", " main "], "--branch", "main");
      throw new Error("padded branch input regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--branch must not include surrounding whitespace")) throw error;
    }
    try {
      branchArg(["--branch", "feature/../main"], "--branch", "main");
      throw new Error("non-canonical branch input regression was not caught");
    } catch (error) {
      if (!String(error.message).includes("--branch must be a canonical branch name")) throw error;
    }
    if (branchArg(["--branch", "release/v0.3.7"], "--branch", "main") !== "release/v0.3.7") {
      throw new Error("canonical branch input was rejected");
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
  console.log("branch protection lockstep selftest passed");
}

async function main() {
  const args = process.argv.slice(2);
  if (args.includes("--selftest")) {
    runSelftest();
    return;
  }

  const docsPath = resolve(argValue(args, "--docs", join(ROOT, "docs", "ci-architecture.md")));
  const documented = requiredCheckNamesFromArchitecture(readFileSync(docsPath, "utf8"));
  const fixture = argValue(args, "--fixture");
  let payload;
  let label;
  if (fixture) {
    payload = JSON.parse(readFileSync(resolve(fixture), "utf8"));
    label = fixture;
  } else {
    const repo = repoArg(args, "--repo", process.env.GITHUB_REPOSITORY);
    const branch = branchArg(args, "--branch", process.env.GITHUB_REF_NAME || "main");
    const token = process.env.GH_TOKEN || process.env.GITHUB_TOKEN;
    if (!token) throw new Error("GH_TOKEN or GITHUB_TOKEN is required");
    payload = await fetchJson(
      `https://api.github.com/repos/${repo}/branches/${encodeURIComponent(branch)}/protection/required_status_checks`,
      token,
    );
    label = `${repo}@${branch} branch protection`;
  }

  const actual = normalizeRequiredStatusChecks(payload);
  assertLockstep(documented, actual, label);
  console.log(`branch protection lockstep passed: ${actual.length} required checks match docs/ci-architecture.md`);
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
