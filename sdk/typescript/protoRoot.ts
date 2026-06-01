import fs from "fs";
import path from "path";

/**
 * Resolve the directory that contains the `udb/**` proto tree, working both:
 *  - in the published npm package, where the build bundles `proto/` next to the
 *    compiled output (`dist/` → `../proto`); and
 *  - in this repo during development, where the protos live at the repo root
 *    (`sdk/typescript` → `../../proto`).
 *
 * The companion `third_party/googleapis` include dir is always a sibling of the
 * resolved root's parent (`<root>/../third_party/googleapis`), which holds for
 * both the bundled package layout and the repo layout.
 *
 * Callers may still pass an explicit `protoRoot` to override this.
 */
export function defaultProtoRoot(): string {
  const probe = "udb/services/v1/data_broker.proto";
  const candidates = [
    path.resolve(__dirname, "../proto"), // shipped package: dist/ -> <pkg>/proto
    path.resolve(__dirname, "../../proto"), // dev: sdk/typescript -> repo/proto
    path.resolve(__dirname, "proto"), // assets co-located with the module
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(path.join(candidate, probe))) {
      return candidate;
    }
  }
  return candidates[candidates.length - 1];
}
