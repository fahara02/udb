# Change note: native body projects are claim-bound

Date: 2026-08-15
Release: 0.5.9

## Changed

- Added `validated_native_service_context`, which validates the request tenant
  and project against metadata and the verified claim before returning the
  runtime context.
- Config flag Put/Get/List/Delete/Evaluate, Metering quota Put/Get/List/Check,
  and LiveQuery Subscribe now use that atomic boundary.
- Genuinely tenant-only native handlers retain the tenant-only helper; this is a
  targeted project-isolation fix rather than a global behavior change.
- Added claim-mismatch/matching-scope unit coverage and a source posture guard
  covering all ten affected operations.

## Verification

- No local Cargo/build/test command was run, per operator direction.
- GitHub CI must compile all targets and run the unit/posture suites before
  merge. The native live suite remains the served integration gate.
