# UDB blocked & decision-gated items — explained simply

This is the plain-English companion to the private masterplan/todo board. The
board has **6 items that cannot move until you make a call** — 5 marked `BLOCKED` and 1
marked `DECISION-GATED`. None of them are stuck on hard code problems; they're stuck
on a *decision* or a *machine/host* that only you can provide. This doc explains each
one the way I'd explain it to a teammate: what it is, why it's stuck, the choices, my
suggestion, and what flips it to "go". The recommendation is mine — **the decision is
yours.**

Quick map:

| # | Item | Type | The one question you answer |
|---|------|------|------------------------------|
| 1 | 0.4 — Build env (CMAKE/VS2026) | DECISION-GATED | "Is my machine still on VS 2026, and shall I pin CMAKE?" |
| 2 | 2.4 — Legacy Postgres planner | BLOCKED | "Delete the old query builder, or keep it and prove it matches?" |
| 3 | 3.1 — ClickHouse locking | BLOCKED | "Make ClickHouse cluster-safe, or label it read-only?" |
| 4 | 3.2 — Vector store locking | BLOCKED | "Same question for Qdrant/Pinecone/Weaviate/Elasticsearch." |
| 5 | 4.1 — WebAuthn hardware-key policy | BLOCKED | "Which crypto library do we use to verify security keys?" |
| 6 | 5.3 — Video transcoding (ffmpeg) | BLOCKED | "When do we get a build machine with ffmpeg?" |

---

## 1. Item 0.4 — Fix the Windows build environment (CMAKE for VS 2026)

**Type:** Decision-gated (really just a 2-minute machine setup) · **Lives in:** your shell + `TESTING.md`

**In plain words:** One of UDB's dependencies (`rdkafka`, the Kafka client) compiles
C code at build time using a tool called CMake. Your machine has an older CMake (3.29)
on its PATH that doesn't understand Visual Studio 2026, so builds randomly fail with:
`Could not create named generator Visual Studio 18 2026`.

**Why it's stuck:** It's not a code problem — it's a setting on *your* computer. The fix
is to point the `CMAKE` environment variable at the newer CMake that ships *inside*
Visual Studio. I can't set machine-wide environment variables for you reliably.

**The decision / action:** Confirm your machine is still on VS 2026 (or tell me the new
version), then set `CMAKE` once, user-wide, to the bundled cmake path. We've been using
this exact path in our build commands all session:
`C:\Program Files\Microsoft Visual Studio\18\Community\...\CMake\bin\cmake.exe`.

**My suggestion:** Do it — it permanently stops the random rebuild failures. I'll add a
short "Windows build" note to `TESTING.md` so it's documented for the next person.

**What unblocks it:** You set the env var (or confirm the VS version so I write the exact
line). Zero code changes.

---

## 2. Item 2.4 — Retire or pin the "legacy" Postgres query builder

**Type:** Blocked on an architecture decision · **Lives in:** `src/planning/broker/mod.rs::build_select_query_plan` (line 400) and `::build_upsert_plan` (line 574)

**In plain words:** UDB has **two** ways to turn a request into Postgres SQL:
- the **old planner** (`build_select_query_plan` / `build_upsert_plan`) — hand-written,
  fast, and what the Postgres path actually uses today (this is the code we fixed the
  upsert bug in);
- the **new IR compiler** (`ir::compile::postgres`) — the unified, audited one that all
  18 backends share.

Having two code paths that generate SQL is a long-term risk: they can drift apart, and a
security fix in one might not be in the other.

**Why it's stuck:** Both choices are reasonable and you haven't picked one. It's marked
`[blocked:maintainer decision]` in the old plan.

**Your two options:**
- **Option A — Delete the old planner.** Route Postgres through the IR compiler like every
  other backend, then remove the two functions. *Pro:* one source of truth, no drift ever.
  *Con:* it's a real surgery — every caller must be rewired, and we must be sure the IR
  compiler is at least as fast and correct first. (Also: this touches the exact code we
  just bug-fixed, so we'd re-verify carefully.)
- **Option B — Keep the old planner as the "fast path"** but add a test that proves it
  produces *the same SQL* as the IR compiler for every case. *Pro:* lower risk, keeps the
  fast path. *Con:* you now maintain the equivalence test forever, and if that test only
  runs on in-memory SQLite (not real Postgres) it's a false sense of safety.

**My suggestion:** **Option B for now, Option A later.** Keep the fast path, but bolt on a
live-Postgres equivalence test (planner SQL ≡ compiler SQL) so they *cannot* silently
drift. Revisit full retirement (A) once the IR-mediation work in Phase 2 is done and proven
— deleting it before then would be premature.

**What unblocks it:** You write "A" or "B" in the plan; the implementer follows that path.

---

## 3. Item 3.1 — ClickHouse: make it cluster-safe, or mark it read-only

**Type:** Blocked on an architecture decision · **Lives in:** `src/runtime/canonical_store/clickhouse.rs::try_acquire_advisory_lease` (line 351)

**In plain words:** When several UDB servers run together, they use a "lease" (a lock) so
only one of them does a given background job at a time. ClickHouse's lease is **faked** —
it does a read-then-write that *looks* atomic but isn't truly safe across multiple servers.
ClickHouse's own code comment admits it: *"a hardened path needs Keeper"* (Keeper is
ClickHouse's coordination service, like ZooKeeper).

**Why it's stuck:** Right now this is mostly harmless because ClickHouse isn't used as a
multi-server "source of truth." But the code still *claims* it could be. You need to decide
whether to make the claim true or drop it.

**Your two options:**
- **Option A — Make it real.** Add a ClickHouse-Keeper-backed lock and prove it with the
  multi-process test rig (Phase 1.1). *Pro:* ClickHouse becomes a first-class HA store.
  *Con:* real work, and adds Keeper as a dependency.
- **Option B — Pin it as "projection-only."** Officially say ClickHouse is for read
  copies/analytics, not for holding authoritative state, and stop advertising the unsafe
  lease. *Pro:* honest, cheap, matches how it's actually used. *Con:* closes the door on
  ClickHouse-as-primary (which nobody is asking for).

**My suggestion:** **Option B — pin it projection-only.** This matches reality and removes
a "capability lie." If a customer ever needs ClickHouse as a primary HA store, do A then.

**What unblocks it:** You pick A or B. (B is mostly removing an over-claim, not adding code.)

---

## 4. Item 3.2 — Vector stores: same question (Qdrant, Pinecone, Weaviate, Elasticsearch)

**Type:** Blocked on an architecture decision · **Lives in:** `src/runtime/canonical_store/vector_system.rs::try_acquire_advisory_lease` (line 748); registered at `src/runtime/core/setup_data.rs` (Elasticsearch line 2818, Weaviate line 3094, Pinecone line 3148)

**In plain words:** This is the *same problem as #3* but for the vector databases. Their
lease is only safe **inside a single process** — it uses an in-memory lock
(`tokio::sync::Mutex`, line 93). Across multiple servers it provides no real protection.
Yet three of them (Elasticsearch, Weaviate, Pinecone) are still registered as
"full-canonical" stores, which *implies* they're cluster-safe. They aren't.

**Why it's stuck:** Decision-gated, exactly like ClickHouse. The risk is concrete: a
multi-server production deployment using Weaviate as authoritative state could silently
lose the "only one winner" guarantee.

**Your two options:**
- **Option A — Prove multi-process safety** for these stores using the Phase 1.1 test rig.
- **Option B — Pin them projection-only:** remove the three "full-canonical" registrations
  (but keep the code compiling for the conformance test suite — *don't delete the
  implementations*).

**My suggestion:** **Option B — pin them projection-only.** Vector stores are search/embedding
indexes; treating them as authoritative HMA state was always a stretch. This is the safe,
honest default, and it's a small, surgical change (remove 3 registration calls, keep the
impls).

**What unblocks it:** You pick A or B for the vector group (can be the same call as #3).

> Note: items **3 and 4 are the same kind of decision** and pair naturally with Phase 3.5
> (the "deployment-tier startup guard" that refuses to start a store below its declared
> safety tier). Decide 3.1 + 3.2 together.

---

## 5. Item 4.1 — WebAuthn: enforce hardware-key (passkey) policies

**Type:** Blocked on a crypto-library decision · **Lives in:** `src/runtime/service/auth_service/authn/mod.rs::webauthn` (lines 842–898); enforcement would go in `finish_webauthn_registration_impl` (lines 1303–1400)

**In plain words:** UDB can already register and use passkeys / security keys. What it
**can't** yet do is enforce a security policy like *"only allow verified hardware keys from
approved manufacturers"* or *"require a resident key + user verification."* The settings are
even read from env vars today (`UDB_WEBAUTHN_ATTESTATION`, etc.) — and, correctly, UDB
**refuses to start** in production if you ask for a policy it can't enforce (good — no fake
safety). But the actual enforcement isn't built.

**Why it's stuck:** Enforcing "this is a genuine YubiKey" means validating an X.509
certificate chain (attestation). That needs a crypto library UDB doesn't currently link.
There are two ways to get it, and they're a trade-off only you should pick.

**Your two options:**
- **Option A — Add `openssl`** (vendored) to release builds. *Pro:* batteries-included,
  well-trodden path for cert-chain validation. *Con:* pulls OpenSSL into the build (bigger
  binary, C dependency, more supply-chain surface).
- **Option B — Use `ring` / `rustls-webpki`** (pure-Rust). *Pro:* stays in the Rust crypto
  stack we already use, no OpenSSL. *Con:* more code to write ourselves for attestation
  chain-building; less off-the-shelf.

**My suggestion:** **Option B (`rustls-webpki`)** to keep the pure-Rust, no-OpenSSL posture
the rest of UDB already has — *unless* you foresee needing exotic attestation formats soon,
in which case A is the pragmatic shortcut. Either way: once chosen, the rule is **the policy
must actually reject a non-conforming key, with a test that proves it** (no "setting that's
read but never enforced").

**What unblocks it:** You pick the crypto library. Then it's: add a per-tenant policy field
(proto), enforce it at register/assert, add the deny-test.

---

## 6. Item 5.3 — Video transcoding (ffmpeg) via a sidecar

**Type:** Blocked on host/CI infrastructure · **Lives in:** `src/runtime/service/asset_service/mod.rs::run_byte_step` (line 456); `TRANSCODE` currently fails honestly at lines 674–686

**In plain words:** UDB can store and thumbnail files, but it can't transcode video yet
(e.g. turn an upload into streamable MP4). Today the `TRANSCODE` step **fails on purpose**
with "not yet implemented" — it does *not* pretend to work, which is the correct behavior.
The plan is to run the heavy video work in a **separate sidecar container** (so the UDB
broker never has to link the giant `libav`/ffmpeg libraries or carry GPU/codec baggage).

**Why it's stuck:** This isn't a decision — it's a **missing machine**. To build and test
transcoding we need a build/CI environment that has ffmpeg available in a container. The
broker side is designed (emit a work-event with presigned URLs → sidecar does the ffmpeg
→ reports back), but it can't be finished and tested without that host.

**Your two options:**
- **Wait** until a CI runner / dev host with ffmpeg-container support is available, then
  build it.
- **Deprioritize** — if no one needs video transcoding soon, leave it failing-honestly
  (which it already does) and skip it for now.

**My suggestion:** **Deprioritize until there's a real need.** It's the only item here that
needs hardware we don't have, and it's correctly fail-closed today (no fake success). When a
customer needs it, provision the ffmpeg CI host and build the two lanes (broker work-event
step + `sidecars/transcoder/`). Important guardrail when we do: the sidecar gets **presigned
URLs only — never broker credentials or raw file paths.**

**What unblocks it:** A build/CI host with ffmpeg-in-a-container. Until then, no action.

---

## DECISIONS RECORDED — 2026-06-25 (these OVERRIDE my earlier suggestions above)

The maintainer has ruled. These are final; the per-item "my suggestion" lines above are
superseded where they differ.

1. **0.4 CMAKE** — ✅ **DONE.** User `CMAKE` env pinned to the VS18 cmake
   (`...\Visual Studio\18\Community\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe`,
   cmake 4.1.1). Documented for `TESTING.md`.
2. **2.4 PG planner** — **"legacy" REJECTED.** The two paths were born in the same commit and
   serve different RPCs (data-plane planner vs cross-backend IR compiler). Decision = **MERGE,
   not retire**: make the IR compiler the single SQL emitter, move the planner's data-plane
   value-adds (cache, scope/PII/audit) into the wrapper, prove `A-SQL ≡ B-SQL` live first.
   Full function-level evidence: **`UDB_TWO_PG_PATHS_ANALYSIS.md`**. Pairs with Phase 2.1.
3. **3.1 ClickHouse** — **FULL CANONICAL (non-negotiable).** Implement a real ClickHouse-Keeper
   distributed lock; do NOT pin projection-only. Keep `register_full_canonical_store`.
4. **3.2 Vector stores** — **FULL CANONICAL (non-negotiable).** Implement real multi-process CAS
   for Qdrant/Pinecone/Weaviate/ES; do NOT pin projection-only. Keep all registrations.
5. **4.1 WebAuthn** — **OpenSSL** (vendored). `rustls-webpki` is path-validation only — it does
   NOT parse/verify attestation statements (packed/tpm/android-key/fido-u2f), so it is **not** a
   1:1 replacement. Use OpenSSL, then enforce per-tenant attestation/RK/UV policy at
   register/assert with a deny-test.
6. **5.3 ffmpeg** — **VENDOR ffmpeg and ALWAYS support transcoding** (not deferred, not
   sidecar-only). Transcoding is first-class. Keep presigned-URL / no-broker-creds discipline for
   any out-of-process worker.

Items 3, 4, 5, 6 are welded to project memory so they are never re-litigated.
