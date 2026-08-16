# UDB v0.5.11 post-release benchmark closure

## Scope

The v0.5.10 post-release benchmark produced the complete canonical surface—381
RPCs for each of Go, Python, TypeScript, and PHP—but correctly refused publication
because 39 rows remained fatal. This change wave closes every one of those rows:

- four Asset read-after-write failures caused by dropping the effective project
  during RegisterAsset persistence;
- twelve Authz deletion/revocation failures caused by compiling tenant-scoped
  neutral IR under an empty request context;
- one Backup cross-tenant restore failure caused by reusing a BIGSERIAL primary
  key; and
- twenty-two TypeScript/PHP governance failures caused by unaudited seed actors
  and missing dependency provenance.

The sixteen WebRTC egress rows remain explicit `CAPABILITY_SKIPPED` outcomes when
that optional capability is disabled; they are neither hidden nor counted as
successful measurements.

The canonical Asset, Scheduler, Storage, and Workflow benchmark bodies now send
the live opaque project explicitly instead of relying on the obsolete claim that
their project columns require UUIDs. Asset metadata fallback remains covered by
the served live regression. Generated benchmark JSON is refreshed from those
canonical Markdown sources by CI.

## Release discipline

Product code changes require the immutable successor release `v0.5.11`; the
published `v0.5.10` tag is not moved. `versions.json` remains the single version
authority and is propagated across crate, SDK, documentation, examples, and site
metadata before CI.

All commits and tags for this wave must use sole author and committer
`fahara02 <idea3d.faruk@gmail.com>` with no co-author trailers.

## Verification boundary

No local Cargo build, test, rustfmt, SDK generation, or protocol generation is
permitted for this wave. Static diff review is local; GitHub CI owns compilation,
formatting, generated-artifact repair, focused live regressions, the complete
matrix, and the post-release Release → Benchmark → Pages evidence chain.

The first PR run, `31943894065`, identified only Backup rustfmt drift at its
quick gate and emitted `ci-rustfmt-repair-1` (`9262760140`). That CI-produced
patch was applied without running a local formatter; the successor exact-head
CI run remains the release authority.

The same run's SDK-conformance job exposed four stale source assertions left
behind when the canonical Asset, Scheduler, and Workflow bodies began carrying
the live project. The Go Asset assertion and the three TypeScript assertions now
expect the canonical `project-1` fixture, matching the generated manifest rather
than preserving the obsolete empty-project expectation.

Successor run `31944181018` passed rustfmt and version consistency, then the
skill-wrapper drift gate found that the plugin reference, OpenAI instructions,
and Ollama model still embedded `0.5.10`. The repository's canonical
`udb-skill/shared/using-udb.md` was propagated through `sync_skills.py`, updating
only those three maintained text mirrors to `0.5.11`.

Run `31944321004` then confirmed that the using-udb mirrors were clean and
identified the parallel udb-coding mirror family. The canonical
`udb-skill/shared/udb-coding.md` was propagated through
`sync_udb_coding.py`, updating its plugin, OpenAI, and Ollama references from
`0.5.10` to `0.5.11` without altering coding doctrine.
