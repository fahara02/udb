# Publishing the Laravel SDK to Packagist

The Laravel SDK lives at `sdk/php/` inside this monorepo, but Packagist
requires `composer.json` at the **root** of whatever git URL it
indexes. There's no Packagist-side option to point at a subdirectory
of a GitHub repo. The standard solution — used by Symfony, Laravel,
Doctrine, and every other Composer-shipping monorepo — is a
**satellite repo**: a read-only mirror of `sdk/php/*` whose root
contains `composer.json`, kept in sync via `git subtree split`.

## Topology

```
github.com/fahara02/udb              ← source of truth (this monorepo)
    sdk/php/                         ← Laravel SDK source
    sdk/php/composer.json
    sdk/php/src/...
    sdk/php/gen/...
              │
              │  `git subtree split --prefix=sdk/php` on every tag
              ▼
github.com/fahara02/udb-laravel      ← satellite, read-only
    composer.json                    ← was sdk/php/composer.json
    src/...
    gen/...
              │
              │  webhook (Packagist auto-discovery)
              ▼
packagist.org/packages/fahara02/udb-laravel
              │
              │  `composer require fahara02/udb-laravel`
              ▼
consumer Laravel apps
```

## One-time setup

Run these once on your machine. They create the satellite repo and
seed it with the current `sdk/php/` contents.

### 1. Create the empty satellite on GitHub

Go to https://github.com/new and create:

| Field | Value |
|---|---|
| Owner | `fahara02` |
| Repository name | `udb-laravel` |
| Visibility | Public |
| Initialize with README | **No** (leave empty — we're pushing content immediately) |

Do NOT add a `.gitignore`, license, or README — git subtree push
needs a truly empty repo.

### 2. Split + push the SDK subtree

From the root of this repo (`E:\Projects\udb`):

```bash
# Make a "split" branch that contains the sdk/php subtree as if it
# were the repo root. `git subtree split` rewrites history so every
# commit that touched sdk/php/ becomes a commit in the new branch
# rooted at the SDK files.
git subtree split --prefix=sdk/php -b sdk-php-split

# Push that branch to the satellite repo's main branch.
git push https://github.com/fahara02/udb-laravel.git sdk-php-split:main

# Clean up the local split branch (the workflow recreates it each
# release).
git branch -D sdk-php-split
```

After this push, `https://github.com/fahara02/udb-laravel` contains
`composer.json` at its root — Packagist can now find it.

### 3. Submit on Packagist

Go to https://packagist.org/packages/submit and paste:

```
https://github.com/fahara02/udb-laravel
```

Packagist reads `composer.json#name` → registers
`fahara02/udb-laravel`. The "warning: no composer.json found" goes
away because it now sits at the root.

### 4. Set up the Packagist webhook (auto-update on push)

Packagist shows you a webhook URL after the package is registered
(format: `https://packagist.org/api/github`). Add it to the
**satellite repo's** webhooks:

| Field | Value |
|---|---|
| Payload URL | `https://packagist.org/api/github?username=<your-packagist-name>` |
| Content type | `application/json` |
| Secret | your Packagist API token (from https://packagist.org/profile/) |
| SSL verification | enabled |
| Trigger events | "Just the push event" |

Now every push to `udb-laravel:main` updates the Packagist listing
within seconds (no Packagist poll wait).

## Automated release flow

The `.github/workflows/release-packagist.yml` workflow runs on every
`v*.*.*` tag push to **this** monorepo. It:

1. Validates `sdk/php/composer.json` is well-formed.
2. Runs `git subtree split --prefix=sdk/php`.
3. Force-pushes that split to `fahara02/udb-laravel:main`.
4. Tags the satellite repo with the same `v*.*.*` tag so Packagist
   indexes the new version.

The workflow needs a **deploy key** with write access to the
satellite repo:

### Generate + register the deploy key

On your machine:

```bash
ssh-keygen -t ed25519 -f udb_laravel_deploy_key -N "" -C "udb-monorepo -> udb-laravel"
cat udb_laravel_deploy_key.pub
```

- Paste the **public** key (`.pub`) into
  https://github.com/fahara02/udb-laravel/settings/keys/new with
  **Allow write access** ticked. Title: `udb-monorepo-sync`.
- Paste the **private** key (no `.pub`) into
  https://github.com/fahara02/udb/settings/secrets/actions/new as
  `UDB_LARAVEL_DEPLOY_KEY` (full file contents, including the
  `-----BEGIN OPENSSH PRIVATE KEY-----` lines).
- Delete both local files after pasting:
  ```bash
  shred -u udb_laravel_deploy_key udb_laravel_deploy_key.pub  # Linux
  # or on Windows PowerShell:
  Remove-Item udb_laravel_deploy_key, udb_laravel_deploy_key.pub
  ```

The deploy key is scoped to ONE repo, which is the right blast
radius for this purpose — it can't touch any other fahara02 repo
even if it leaks.

## Versioning

Every monorepo release tag (`v0.1.0`, `v1.0.0`, …) corresponds to
the same tag on the satellite. The SDK's wire-protocol version
lives in `sdk/UDB_PROTOCOL_VERSION` — bump that when the broker's
gRPC contract has a breaking change.

## Why not `composer.json` at the monorepo root?

The monorepo isn't itself a PHP package. The root contains
`Cargo.toml` (Rust crate), `buf.yaml` (proto module), `sdk/go/go.mod`
(Go module), and `sdk/php/composer.json` (PHP package). Adding a
top-level `composer.json` would either:

- Lie about what the package contains (no PHP code at the root), or
- Force the whole repo into Composer's PSR-4 autoload scheme (would
  clash with the Rust + Go layouts).

The satellite-repo pattern keeps each ecosystem's package manifest
at its natural root while preserving the monorepo as the single
source of truth.
