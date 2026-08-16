# Bug report: CI could detect generated reference drift but not return its repair

## Observed

The Linux Rust job checks the canonical codebase map before the native contract
gate. A source edit that makes the map stale stops that job, while the existing
failure repair artifact regenerated only the native manifest, native docs, and
descriptor baseline. The failing run therefore had no CI-generated patch for
the codebase map or the bundled `udb-coding` reference that mirrors it.

## Impact

The repository deliberately forbids treating generated documentation as hand
authored. Under CI-only release verification, a correct source change could not
be completed from the repair artifact even though CI had the required Python
runtime and the already-built broker.

## Required correction

- Regenerate the canonical codebase map in the existing Linux failure-repair
  step.
- Synchronize the bundled skill reference from that canonical map.
- Include both files in the binary-safe repair patch alongside the native
  contract artifacts.
- Keep the freshness gates unchanged: generated drift must still fail the run
  that produced the diagnostic patch.

## Evidence

No local generator, build, or test is used. The PR CI Linux job must emit the
repair artifact when either reference is stale; the repaired commit must then
pass the same freshness gates in a new run.
