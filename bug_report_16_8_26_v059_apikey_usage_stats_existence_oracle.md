# Bug report: API-key usage stats exposed key existence

Date: 2026-08-16
Release: 0.5.9
Severity: release-blocking authorization contract drift

## Observed

The project-authority correction added an API-key row lookup before
`GetApiKeyUsageStats` applied its scope guard. The lookup converted an unknown
key identifier into `NotFound`, although the endpoint's established contract
reports an empty/zero usage result when no usage or key row exists.

## Impact

Besides breaking the live endpoint contract, different status codes for known
and unknown identifiers created a key-existence oracle. There is no tenant or
project scope to authorize for an identifier with no key row; keys that do
exist must still pass the centralized exact-scope/platform-authority guard.

## Correction

Return the default zero-valued usage response when the key-scope lookup has no
row. Continue through the existing tenant/project scope check and usage query
for every key that does exist.

## Verification

This correction arrived on `main` as `1cca1209` and is merged into the complete
v0.5.9 fix branch with this tracked report. No local Cargo/build/test/rustfmt
command was run. The combined head must pass GitHub CI, including the normal
library suite and the API-key live contract coverage, before release.
