// Bundle the proto assets the runtime proto-loader needs into the package so the
// published npm tarball is self-contained. The handwritten clients load
// `.proto` files at runtime (see protoRoot.ts); in the repo those live at the
// root (`proto/`, `third_party/googleapis/`), which is outside the package, so
// the build copies them next to the compiled output.
//
// Node 18+ only (uses fs.cpSync). No extra dependency.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url)); // sdk/typescript/scripts
const pkg = path.resolve(here, ".."); // sdk/typescript
const repo = path.resolve(pkg, "..", ".."); // repo root

const copies = [
  [path.join(repo, "proto"), path.join(pkg, "proto")],
  [
    path.join(repo, "third_party", "googleapis"),
    path.join(pkg, "third_party", "googleapis"),
  ],
];

for (const [src, dst] of copies) {
  if (!fs.existsSync(src)) {
    console.error(`bundle-proto: source not found: ${src}`);
    process.exit(1);
  }
  fs.rmSync(dst, { recursive: true, force: true });
  fs.cpSync(src, dst, { recursive: true });
  console.log(`bundle-proto: ${path.relative(repo, src)} -> ${path.relative(repo, dst)}`);
}
