# Change note: propagate the v0.5.10 release identity everywhere

All components in `versions.json` now target 0.5.10. The repository-owned
version propagator updated the Rust crate and lock, Python/TypeScript/C#/Java
package manifests, generated SDK version constants, CLI launchers, release
workflow examples, READMEs, native examples, publishing instructions, skill
baselines, documentation headers, and GitHub Pages version labels.

Historical changelog entries, bug reports, benchmark evidence, and immutable
v0.5.9 release references remain historical and were not rewritten. Wire
protocol remains 1.0.0; the independently governed native contract advances to
7.1.0 for the Authn operation-kind correction.

No local compilation, test, code generation, or formatting was performed.
Required proof is CI version consistency plus the full Rust/SDK/docs/workflow
matrix, followed by the v0.5.10 tag guard and release publication chain.
