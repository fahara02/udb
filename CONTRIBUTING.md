# Contributing to UDB

## One branch: `main`

UDB keeps exactly one long-lived branch, `main`, in every clone and on GitHub.
Releases are commits on `main` plus tags (`vX.Y.Z` and `sdk/go/vX.Y.Z`, see
[VERSIONING.md](VERSIONING.md)). There are no release, feature or backup
branches.

Work that does not land on `main` is work that gets lost: branches and
worktrees that outlive a task drift until nobody can tell what they hold.

### Experiments

When a change needs isolation, use **one short-lived branch** and keep it
mergeable the whole time:

1. Cut it from the current `origin/main`.
2. Rebase it onto `origin/main` before every push, so it always merges cleanly.
3. Merge it into `main` and delete it, locally and on GitHub, as soon as the
   experiment ends, the same day if possible.
4. If the experiment is abandoned, keep it as a bundle instead of a branch:
   `git bundle create private/archive/<name>.bundle <branch>`, then delete the
   branch. `private/` is ignored by git.

Worktrees belong under `private/worktrees/` inside the repository, never next
to it, and are removed when their branch merges.

### Before you stop

A clean stopping point satisfies all four:

```sh
git branch                    # only main
git worktree list             # only the repository itself
git status --porcelain        # empty
git ls-remote --heads origin  # only refs/heads/main
```

`scripts/check-single-branch.sh --local` runs all four checks. CI runs the
remote half on every push and daily (`.github/workflows/single-branch.yml`):
it fails when a branch other than `main` is older than a day
(`UDB_BRANCH_MAX_AGE_HOURS`) or no longer merges cleanly into `main`.

Never park changes with `git stash`; commit them to `main` (a work-in-progress
commit is fine) or bundle them.

### Deleting a branch

Prove it holds nothing `main` lacks before deleting it: the branch is an
ancestor of `main`, or `git cherry origin/main <branch>` shows no `+` commits,
or each remaining commit's diff already reverse-applies to `main`. When in
doubt, bundle it first.

## Checks before pushing

CI is the authority, but the cheap gates run locally in seconds:
`node scripts/check-versions.mjs`, the `scripts/check-*-posture.py` scripts,
`python scripts/generate-codebase-map.py --check`, the three
`udb-skill/sync_*.py --check` generators, and `actionlint` for any change under
`.github/`. A version bump edits `versions.json` and then runs
`node scripts/check-versions.mjs --fix`; never hand-edit the manifests.
