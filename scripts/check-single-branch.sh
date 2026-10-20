#!/usr/bin/env bash
# Enforce the one-branch rule in CONTRIBUTING.md.
#
#   scripts/check-single-branch.sh          remote check (CI): every branch on
#                                           origin other than main is younger
#                                           than the age limit and merges into
#                                           main without conflicts
#   scripts/check-single-branch.sh --local  the remote check plus a clean
#                                           stopping point in this checkout:
#                                           only main, no extra worktree, no
#                                           uncommitted change
#
# UDB_BRANCH_MAX_AGE_HOURS (default 24) sets how long an experiment branch may
# live. Needs git >= 2.38 for `git merge-tree --write-tree`.
set -euo pipefail

max_age_hours="${UDB_BRANCH_MAX_AGE_HOURS:-24}"
local_check=false
[ "${1:-}" = "--local" ] && local_check=true

failures=()
fail() { failures+=("$1"); }

git fetch --quiet --prune origin '+refs/heads/*:refs/remotes/origin/*'
now=$(date +%s)

while read -r ref; do
  branch="${ref#refs/remotes/origin/}"
  [ "$branch" = "main" ] || [ "$branch" = "HEAD" ] && continue
  committed=$(git log -1 --format=%ct "$ref")
  age_hours=$(( (now - committed) / 3600 ))
  if [ "$age_hours" -ge "$max_age_hours" ]; then
    fail "origin/$branch is ${age_hours}h old (limit ${max_age_hours}h): merge it into main, or bundle it to private/archive and delete it"
  fi
  if ! git merge-tree --write-tree origin/main "$ref" > /dev/null 2>&1; then
    fail "origin/$branch no longer merges cleanly into main: rebase it onto origin/main"
  fi
done < <(git for-each-ref --format='%(refname)' refs/remotes/origin/)

if $local_check; then
  others=$(git for-each-ref --format='%(refname:short)' refs/heads/ | grep -vx main || true)
  [ -z "$others" ] || fail "local branches besides main: $(echo "$others" | tr '\n' ' ')"
  worktrees=$(git worktree list --porcelain | grep -c '^worktree ')
  [ "$worktrees" -le 1 ] || fail "$((worktrees - 1)) extra worktree(s): git worktree list"
  [ -z "$(git status --porcelain)" ] || fail "uncommitted changes: commit them to main or bundle them"
fi

if [ "${#failures[@]}" -gt 0 ]; then
  for message in "${failures[@]}"; do
    echo "::error::$message"
  done
  exit 1
fi
echo "single-branch rule holds: main only$($local_check && echo ', clean checkout')"
