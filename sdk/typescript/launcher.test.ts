// Recursion guard for the `udb` CLI launcher (bin/udb.js).
//
// Inside an npm script PATH starts with node_modules/.bin, so `which udb` finds
// the launcher's own shim. Probing a launcher with `--version` used to start the
// launcher again, which probed PATH again: unbounded recursion. These tests run
// the real launcher with launcher copies and shims on PATH and count every
// process the fake binary and the shims see.
import { strict as assert } from "node:assert";
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { test } from "node:test";

const LAUNCHER = path.resolve(__dirname, "..", "bin", "udb.js");
const POSIX = process.platform !== "win32";

function launcherVersion(): string {
  const m = /const UDB_VERSION = "([^"]+)"/.exec(fs.readFileSync(LAUNCHER, "utf8"));
  assert.ok(m, "launcher declares UDB_VERSION");
  return m[1];
}

function lines(file: string): string[] {
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, "utf8").split(/\r?\n/).filter((l) => l.length > 0);
}

interface Rig {
  root: string;
  record: string;
  cache: string;
  env: NodeJS.ProcessEnv;
}

function rig(): Rig {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "udb-launcher-test-"));
  const record = path.join(root, "record.log");
  const cache = path.join(root, "cache");
  fs.mkdirSync(cache);
  const env: NodeJS.ProcessEnv = { ...process.env };
  delete env.UDB_BIN;
  delete env.UDB_LAUNCHER_ACTIVE;
  // Windows spells it `Path`; drop every spelling so the PATH set below is the only one.
  for (const k of Object.keys(env)) if (k.toUpperCase() === "PATH") delete env[k];
  env.UDB_CACHE_DIR = cache;
  env.UDB_NO_DOWNLOAD = "1";
  return { root, record, cache, env };
}

function writeExec(file: string, body: string): void {
  fs.writeFileSync(file, body);
  fs.chmodSync(file, 0o755);
}

// A copy of the launcher that records each time it runs (it still names the
// guard variable, as every launcher copy does).
function launcherCopy(dir: string, record: string): string {
  const src = fs.readFileSync(LAUNCHER, "utf8").split("\n");
  src.splice(1, 0, `require("fs").appendFileSync(${JSON.stringify(record)}, "launcher-copy\\n");`);
  const copy = path.join(dir, "udb");
  writeExec(copy, src.join("\n"));
  return copy;
}

function runLauncher(r: Rig, pathDirs: string[], args: string[]) {
  // Only the test dirs plus what `which`/`where` and `node` need: a real `udb`
  // elsewhere on the developer's PATH must not leak into the result.
  const base = POSIX
    ? [path.dirname(process.execPath), "/usr/bin", "/bin"]
    : [path.dirname(process.execPath), path.join(process.env.SystemRoot ?? "C:\\Windows", "System32")];
  const env = { ...r.env, PATH: [...pathDirs, ...base].join(path.delimiter) };
  const started = Date.now();
  const res = spawnSync(process.execPath, [LAUNCHER, ...args], {
    env,
    encoding: "utf8",
    timeout: 30_000,
  });
  return { res, elapsedMs: Date.now() - started };
}

test("launcher skips its own copy, symlink and npm shim on PATH and runs the cached binary", { skip: !POSIX }, () => {
  const r = rig();
  try {
    const version = launcherVersion();
    // The real binary, served from the cache dir: records every invocation.
    writeExec(
      path.join(r.cache, "udb"),
      `#!/bin/sh\necho "fake guard=$UDB_LAUNCHER_ACTIVE $*" >> '${r.record}'\n` +
        `if [ "$1" = "--version" ]; then echo "udb ${version}"; exit 0; fi\nexit 3\n`,
    );
    const copyDir = path.join(r.root, "copy");
    const linkDir = path.join(r.root, "link");
    const shimDir = path.join(r.root, "proj", "node_modules", ".bin");
    for (const d of [copyDir, linkDir, shimDir]) fs.mkdirSync(d, { recursive: true });
    launcherCopy(copyDir, r.record);
    fs.symlinkSync(LAUNCHER, path.join(linkDir, "udb"));
    writeExec(path.join(shimDir, "udb"), `#!/bin/sh\necho "npm-shim" >> '${r.record}'\nexec node '${LAUNCHER}' "$@"\n`);

    const { res, elapsedMs } = runLauncher(r, [shimDir, linkDir, copyDir], ["status", "--x"]);
    assert.equal(res.error, undefined, String(res.error));
    assert.ok(elapsedMs < 10_000, `launcher took ${elapsedMs} ms`);
    assert.equal(res.status, 3, `exit code propagated; stderr: ${res.stderr}`);
    // Exactly one probe of the cached binary and one exec: no launcher ran again.
    assert.deepEqual(lines(r.record), ["fake guard=1 --version", "fake guard=1 status --x"]);
  } finally {
    fs.rmSync(r.root, { recursive: true, force: true });
  }
});

test("a launcher shim it cannot recognise is probed at most a bounded number of times", { skip: !POSIX }, () => {
  const r = rig();
  try {
    const version = launcherVersion();
    writeExec(
      path.join(r.cache, "udb"),
      `#!/bin/sh\necho "fake $*" >> '${r.record}'\n` +
        `if [ "$1" = "--version" ]; then echo "udb ${version}"; exit 0; fi\nexit 0\n`,
    );
    // A wrapper that does not name the guard variable and is not in node_modules/.bin.
    const wrapDir = path.join(r.root, "wrap");
    fs.mkdirSync(wrapDir);
    writeExec(path.join(wrapDir, "udb"), `#!/bin/sh\necho "wrapper" >> '${r.record}'\nexec node '${LAUNCHER}' "$@"\n`);

    const { res, elapsedMs } = runLauncher(r, [wrapDir], ["ping"]);
    assert.equal(res.error, undefined, String(res.error));
    assert.ok(elapsedMs < 10_000, `launcher took ${elapsedMs} ms`);
    assert.equal(res.status, 0, res.stderr);
    // Probe: wrapper -> nested launcher (skips PATH, checks the cache, runs the
    // cached binary with --version). Exec: the same once more with the real args.
    assert.deepEqual(lines(r.record), [
      "wrapper",
      "fake --version",
      "fake --version",
      "wrapper",
      "fake --version",
      "fake ping",
    ]);
  } finally {
    fs.rmSync(r.root, { recursive: true, force: true });
  }
});

test("a nested launcher never probes PATH", () => {
  const r = rig();
  try {
    const dir = path.join(r.root, "copy");
    fs.mkdirSync(dir);
    launcherCopy(dir, r.record);
    if (!POSIX) {
      fs.writeFileSync(path.join(dir, "udb.cmd"), `@echo launcher-cmd>>"${r.record}"\r\n@node "${LAUNCHER}" %*\r\n`);
    }
    r.env.UDB_LAUNCHER_ACTIVE = "1";
    const { res, elapsedMs } = runLauncher(r, [dir], ["--help"]);
    assert.equal(res.error, undefined, String(res.error));
    assert.ok(elapsedMs < 10_000, `launcher took ${elapsedMs} ms`);
    // No cached binary and downloads disabled: the launcher gives up cleanly.
    assert.equal(res.status, 127, res.stderr);
    assert.deepEqual(lines(r.record), []);
  } finally {
    fs.rmSync(r.root, { recursive: true, force: true });
  }
});

test("top-level launcher never runs a launcher copy or Windows shim found on PATH", () => {
  const r = rig();
  try {
    const dir = path.join(r.root, "copy");
    fs.mkdirSync(dir);
    launcherCopy(dir, r.record);
    if (!POSIX) {
      fs.writeFileSync(path.join(dir, "udb.cmd"), `@echo launcher-cmd>>"${r.record}"\r\n@node "${LAUNCHER}" %*\r\n`);
    }
    const { res, elapsedMs } = runLauncher(r, [dir], ["--help"]);
    assert.equal(res.error, undefined, String(res.error));
    assert.ok(elapsedMs < 10_000, `launcher took ${elapsedMs} ms`);
    assert.equal(res.status, 127, res.stderr);
    assert.deepEqual(lines(r.record), []);
  } finally {
    fs.rmSync(r.root, { recursive: true, force: true });
  }
});
