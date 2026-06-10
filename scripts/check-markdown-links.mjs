#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

const repoRoot = process.cwd();
const ignoredDirs = new Set([
  ".git",
  "target",
  "node_modules",
  "vendor",
  ".venv",
  "venv",
  "dist",
  "build",
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

function collectLinks(markdown) {
  const links = [];

  // Inline links/images: [text](target), ![alt](target).
  const inline = /!?\[[^\]\n]*\]\(([^)\n]+)\)/g;
  for (const match of markdown.matchAll(inline)) {
    links.push(match[1]);
  }

  // Reference definitions: [id]: target
  const reference = /^\s*\[[^\]\n]+\]:\s*(\S+)/gm;
  for (const match of markdown.matchAll(reference)) {
    links.push(match[1]);
  }

  return links;
}

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

if (failures.length > 0) {
  console.error("Broken local markdown links:");
  for (const failure of failures) {
    console.error(`- ${failure.file}: ${failure.link}`);
  }
  process.exit(1);
}

console.log(`Checked ${markdownFiles.length} markdown files; local links are valid.`);
