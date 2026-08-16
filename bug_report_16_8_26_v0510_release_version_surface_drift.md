# Bug report: successor release metadata could remain on v0.5.9

## Observed

The v0.5.10 product fixes changed runtime, native-contract, SDK benchmark, and
release-proof behavior, but the repository still declared v0.5.9 across the
crate, SDK packages, generated clients, examples, publishing instructions,
documentation headers, skill baselines, workflow examples, and GitHub Pages.

## Impact

Publishing without one authoritative propagation pass could produce a successor
binary whose package metadata, install examples, generated SDK constants, or
deployed documentation still advertised the already-published v0.5.9 release.
That would also make the tag/version release guard fail or, worse, leave an
ungated stale surface outside the primary manifests.

## Required correction

- Advance every component in `versions.json` to 0.5.10.
- Use the repository's canonical version propagator to update every governed
  package, generated version constant, example, publishing guide, skill, and
  site surface.
- Keep protocol version 1.0.0 and native contract version 7.1.0 independent.
- Require CI version consistency, docs/readiness, SDK builds, and the release
  tag guard before publication.

## Evidence

The canonical propagator identified and rewrote 116 governed files from 0.5.9
to 0.5.10. No local build, test, code generation, or formatter was run; CI is
the verification authority.
