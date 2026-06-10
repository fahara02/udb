"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.defaultProtoRoot = defaultProtoRoot;
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
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
function defaultProtoRoot() {
    const probe = "udb/services/v1/data_broker.proto";
    const candidates = [
        path_1.default.resolve(__dirname, "../proto"), // shipped package: dist/ -> <pkg>/proto
        path_1.default.resolve(__dirname, "../../proto"), // dev: sdk/typescript -> repo/proto
        path_1.default.resolve(__dirname, "proto"), // assets co-located with the module
    ];
    for (const candidate of candidates) {
        if (fs_1.default.existsSync(path_1.default.join(candidate, probe))) {
            return candidate;
        }
    }
    return candidates[candidates.length - 1];
}
