#!/usr/bin/env node

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const ignoredDirs = new Set([
  ".git",
  "target",
  "node_modules",
  "vendor",
  ".venv",
  "venv",
  "dist",
  "build",
  "private",
  // Build/dependency caches are not UDB-authored docs — their vendored READMEs
  // carry their own relative links that resolve only inside the upstream repo.
  ".cache",
  ".cargo",
  ".gocache",
]);

function walk(dir, out) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      if (!ignoredDirs.has(entry.name)) {
        walk(path.join(dir, entry.name), out);
      }
      continue;
    }
    if (entry.isFile() && entry.name.toLowerCase().endsWith(".md")) {
      out.push(path.join(dir, entry.name));
    }
  }
}

function stripTarget(raw) {
  let target = raw.trim();
  if (!target || target.startsWith("#")) return "";
  if ((target.startsWith("<") && target.endsWith(">")) || (target.startsWith("'") && target.endsWith("'")) || (target.startsWith('"') && target.endsWith('"'))) {
    target = target.slice(1, -1).trim();
  }
  const hash = target.indexOf("#");
  if (hash >= 0) target = target.slice(0, hash);
  const query = target.indexOf("?");
  if (query >= 0) target = target.slice(0, query);
  return target.trim();
}

function isExternal(target) {
  return /^[a-z][a-z0-9+.-]*:/i.test(target) || target.startsWith("//");
}

function decodeTarget(target) {
  try {
    return decodeURIComponent(target);
  } catch {
    return target;
  }
}

function existsFrom(baseFile, rawTarget) {
  const target = stripTarget(rawTarget);
  if (!target || isExternal(target)) return true;
  const decoded = decodeTarget(target).replaceAll("/", path.sep);
  const resolved = path.resolve(path.dirname(baseFile), decoded);
  return fs.existsSync(resolved);
}

function stripFencedCodeBlocks(markdown) {
  return markdown.replace(/^```[\s\S]*?^```/gm, "");
}

function collectLinks(markdown) {
  const links = [];
  const searchable = stripFencedCodeBlocks(markdown);

  // Inline links/images: [text](target), ![alt](target).
  const inline = /!?\[[^\]\n]*\]\(([^)\n]+)\)/g;
  for (const match of searchable.matchAll(inline)) {
    links.push(match[1]);
  }

  // Reference definitions: [id]: target
  const reference = /^\s*\[[^\]\n]+\]:\s*(\S+)/gm;
  for (const match of searchable.matchAll(reference)) {
    links.push(match[1]);
  }

  return links;
}

function checkRepo(repoRoot) {
  const markdownFiles = [];
  walk(repoRoot, markdownFiles);

  const failures = [];
  for (const file of markdownFiles) {
    const markdown = fs.readFileSync(file, "utf8");
    for (const link of collectLinks(markdown)) {
      if (!existsFrom(file, link)) {
        failures.push({
          file: path.relative(repoRoot, file).replaceAll(path.sep, "/"),
          link,
        });
      }
    }
  }

  return { failures, checked: markdownFiles.length };
}

function writeFixture(root, rel, text) {
  const target = path.join(root, rel);
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(target, text, "utf8");
}

function runSelftest() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "udb-markdown-links-"));
  try {
    writeFixture(root, "docs/target.md", "# Target\n");
    writeFixture(root, "docs/good.md", "[target](./target.md)\n[external](https://example.com)\n[anchor](#local)\n");
    writeFixture(root, "private/research/broken.md", "[missing](./copied-upstream.html)\n");
    writeFixture(root, "docs/code.md", "```powershell\n[Environment]::SetEnvironmentVariable(\"CMAKE\",\n  \"C:/cmake.exe\",\n  \"User\")\n```\n");

    let result = checkRepo(root);
    if (result.failures.length) {
      throw new Error(`good fixture failed:\n${JSON.stringify(result.failures, null, 2)}`);
    }

    writeFixture(root, "docs/broken.md", "[missing](./missing.md)\n");
    result = checkRepo(root);
    if (!result.failures.some((failure) => failure.file === "docs/broken.md" && failure.link === "./missing.md")) {
      throw new Error(`missing local link was not caught:\n${JSON.stringify(result.failures, null, 2)}`);
    }
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
  console.log("markdown link selftest passed");
}

if (process.argv.includes("--selftest")) {
  runSelftest();
  process.exit(0);
}

const { failures, checked } = checkRepo(process.cwd());
if (failures.length > 0) {
  console.error("Broken local markdown links:");
  for (const failure of failures) {
    console.error(`- ${failure.file}: ${failure.link}`);
  }
  process.exit(1);
}

console.log(`Checked ${checked} markdown files; local links are valid.`);
