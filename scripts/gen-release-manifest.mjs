#!/usr/bin/env node
// Generate manifest.json for a UDB binary release: the single contract that
// lists every published asset with its os/arch/tier + sha256 + size, parsed
// from the canonical name `udb-<os>-<arch>[-<tier>][.exe]`. Launchers and
// downstream release jobs resolve assets from this instead of reconstructing
// names.
//
// Usage:
//   node scripts/gen-release-manifest.mjs <dist-dir> [version] > manifest.json
//   node scripts/gen-release-manifest.mjs --selftest

import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const NAME_RE = /^udb-(linux|darwin|windows)-(amd64|arm64)(?:-([a-z0-9]+))?(\.exe)?$/;

function defaultVersion() {
  return JSON.parse(fs.readFileSync("versions.json", "utf8")).components.udb.version;
}

function sha256File(file) {
  return crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
}

function readExpectedSha256(dir, name) {
  const shaPath = path.join(dir, `${name}.sha256`);
  if (!fs.existsSync(shaPath)) {
    throw new Error(`missing .sha256 sidecar for ${name}`);
  }
  const expected = fs.readFileSync(shaPath, "utf8").trim().split(/\s+/)[0] || "";
  if (!/^[a-fA-F0-9]{64}$/.test(expected)) {
    throw new Error(`invalid .sha256 sidecar for ${name}: ${expected || "<empty>"}`);
  }
  const actual = sha256File(path.join(dir, name));
  if (expected.toLowerCase() !== actual) {
    throw new Error(`sha256 mismatch for ${name}: sidecar=${expected} actual=${actual}`);
  }
  return actual;
}

function assetFromName(dir, name) {
  const match = name.match(NAME_RE);
  if (!match) {
    throw new Error(`unrecognized release asset name: ${name}`);
  }
  const [, osName, arch, tier, exe] = match;
  return {
    name,
    os: osName,
    arch,
    tier: tier || "portable",
    ext: exe || "",
    sha256: readExpectedSha256(dir, name),
    size: fs.statSync(path.join(dir, name)).size,
  };
}

export function generateManifest(dir, version) {
  const assets = [];
  for (const name of fs.readdirSync(dir).sort()) {
    if (!name.startsWith("udb-") || name.endsWith(".sha256") || name === "manifest.json") {
      continue;
    }
    assets.push(assetFromName(dir, name));
  }

  if (assets.length === 0) {
    throw new Error(`no udb-* assets found in ${dir}`);
  }

  return {
    version,
    tag: `v${version}`,
    scheme: "udb-<os>-<arch>[-<tier>][.exe]",
    base_url: `https://github.com/fahara02/udb/releases/download/v${version}`,
    assets,
  };
}

function writeAsset(dir, name, body) {
  const file = path.join(dir, name);
  fs.writeFileSync(file, body);
  fs.writeFileSync(`${file}.sha256`, `${sha256File(file)}  ${name}\n`);
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function runSelftest() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "udb-release-manifest-"));
  try {
    writeAsset(root, "udb-linux-amd64", "portable-linux");
    writeAsset(root, "udb-linux-amd64-full", "full-linux");
    writeAsset(root, "udb-windows-amd64.exe", "portable-windows");
    fs.writeFileSync(path.join(root, "notes.txt"), "ignored\n");

    const manifest = generateManifest(root, "9.8.7");
    assert(manifest.version === "9.8.7", "selftest version mismatch");
    assert(manifest.tag === "v9.8.7", "selftest tag mismatch");
    assert(manifest.assets.length === 3, `selftest asset count mismatch: ${manifest.assets.length}`);
    const byName = new Map(manifest.assets.map((asset) => [asset.name, asset]));
    assert(byName.get("udb-linux-amd64").tier === "portable", "portable tier not inferred");
    assert(byName.get("udb-linux-amd64-full").tier === "full", "full tier not parsed");
    assert(byName.get("udb-windows-amd64.exe").ext === ".exe", "windows extension not parsed");

    fs.unlinkSync(path.join(root, "udb-linux-amd64.sha256"));
    try {
      generateManifest(root, "9.8.7");
      throw new Error("selftest failed to reject missing checksum sidecar");
    } catch (err) {
      if (!String(err.message).includes("missing .sha256 sidecar")) {
        throw err;
      }
    }
    writeAsset(root, "udb-linux-amd64", "portable-linux");

    fs.writeFileSync(path.join(root, "udb-linux-amd64.sha256"), `${"0".repeat(64)}  udb-linux-amd64\n`);
    try {
      generateManifest(root, "9.8.7");
      throw new Error("selftest failed to reject stale checksum sidecar");
    } catch (err) {
      if (!String(err.message).includes("sha256 mismatch")) {
        throw err;
      }
    }
    writeAsset(root, "udb-linux-amd64", "portable-linux");

    fs.writeFileSync(path.join(root, "udb-linux-x64"), "bad-name");
    fs.writeFileSync(path.join(root, "udb-linux-x64.sha256"), `${sha256File(path.join(root, "udb-linux-x64"))}  udb-linux-x64\n`);
    try {
      generateManifest(root, "9.8.7");
      throw new Error("selftest failed to reject unrecognized release asset name");
    } catch (err) {
      if (!String(err.message).includes("unrecognized release asset name")) {
        throw err;
      }
    }
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

function main(argv) {
  if (argv.includes("--selftest")) {
    runSelftest();
    process.stdout.write("release manifest generator selftest passed\n");
    return;
  }
  const dir = argv[0] || "dist";
  const version = argv[1] || defaultVersion();
  const manifest = generateManifest(dir, version);
  process.stdout.write(`${JSON.stringify(manifest, null, 2)}\n`);
}

try {
  main(process.argv.slice(2));
} catch (err) {
  console.error(`gen-release-manifest: ${err.message}`);
  process.exit(1);
}
