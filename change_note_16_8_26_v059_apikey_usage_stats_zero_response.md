# Change note: v0.5.9 API-key usage zero response

`GetApiKeyUsageStats` once again returns an empty/zero response for an unknown
key identifier. This preserves the endpoint's non-enumerating behavior while
retaining exact tenant/project authorization for keys that exist.

The change is integrated from `main` commit `1cca1209` into the complete v0.5.9
release candidate. No local Cargo, build, test, code-generation, or rustfmt
command was run; GitHub CI is the required combined-head proof.
