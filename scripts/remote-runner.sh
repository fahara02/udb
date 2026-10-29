#!/usr/bin/env bash
# Use a borrowed Linux machine as a UDB build/test runner over SSH, and leave
# no trace when done.
#
#   scripts/remote-runner.sh <ssh-target> provision   # toolchains + clone (idempotent)
#   scripts/remote-runner.sh <ssh-target> sync        # ship this checkout's HEAD (unpushed commits too)
#   scripts/remote-runner.sh <ssh-target> run '<cmd>' # run in the remote checkout, niced
#   scripts/remote-runner.sh <ssh-target> status      # footprint, load, UDB processes
#   scripts/remote-runner.sh <ssh-target> cleanup     # stop UDB work, delete everything this script made
#
# <ssh-target> is anything `ssh` accepts (user@host or a ~/.ssh/config alias).
#
# Isolation, so the machine's own workload is unaffected:
# - every file lives under $UDB_REMOTE_ROOT (default /srv/udb): the Rust and Go
#   toolchains (RUSTUP_HOME/CARGO_HOME/GOROOT), the checkout, the cargo target
#   directory and caches. Nothing is written to the login user's dotfiles.
# - work runs under `nice -n 19 ionice -c3` with $UDB_REMOTE_JOBS cargo jobs
#   (default 2), so the host's services keep priority for CPU and disk.
# - containers a test starts must be named `udb-*`; cleanup removes exactly those.
# - provision installs build packages with apt (build-essential, cmake, …). Those
#   are shared system packages and cleanup leaves them, because another user of
#   the machine may depend on them; everything else is removed.
set -euo pipefail

target="${1:-}"
command="${2:-}"
root="${UDB_REMOTE_ROOT:-/srv/udb}"
jobs="${UDB_REMOTE_JOBS:-2}"
repo_url="${UDB_REMOTE_REPO:-https://github.com/fahara02/udb.git}"
go_version="${UDB_REMOTE_GO:-1.22.12}"

if [ -z "$target" ] || [ -z "$command" ]; then
  sed -n '2,24p' "$0"
  exit 2
fi
case "$root" in
  /srv/*|/opt/udb*|/var/tmp/udb*|/tmp/udb*) ;;
  *) echo "refusing UDB_REMOTE_ROOT=$root: use a dedicated path such as /srv/udb" >&2; exit 2 ;;
esac

remote() {
  ssh -o BatchMode=yes -o ServerAliveInterval=30 "$target" "$@"
}

env_script() {
  cat <<EOF
export RUSTUP_HOME=$root/.rustup CARGO_HOME=$root/.cargo GOROOT=$root/go GOPATH=$root/gopath
export GOCACHE=$root/cache/go-build GOMODCACHE=$root/gopath/pkg/mod PIP_CACHE_DIR=$root/cache/pip
export JAVA_HOME=$root/jdk DOTNET_ROOT=$root/dotnet DOTNET_CLI_HOME=$root/cache/dotnet NUGET_PACKAGES=$root/cache/nuget
export MAVEN_OPTS=-Dmaven.repo.local=$root/cache/m2 DOTNET_CLI_TELEMETRY_OPTOUT=1 npm_config_cache=$root/cache/npm
export PATH=$root/.cargo/bin:$root/go/bin:$root/jdk/bin:$root/maven/bin:$root/dotnet:$root/node/bin:$root/venv/bin:$root/bin:\$PATH
export CARGO_BUILD_JOBS=$jobs CARGO_TARGET_DIR=$root/target CARGO_INCREMENTAL=0
EOF
}

case "$command" in
  provision)
    remote "mkdir -p '$root' && cat > '$root/env.sh'" < <(env_script)
    remote "bash -s" <<EOF
set -euo pipefail
. '$root/env.sh'
export DEBIAN_FRONTEND=noninteractive
if ! dpkg -s build-essential cmake pkg-config libssl-dev libsasl2-dev clang python3-venv >/dev/null 2>&1; then
  nice -n 19 apt-get update -q >'$root/apt.log' 2>&1
  nice -n 19 apt-get install -y -q build-essential cmake pkg-config libssl-dev libsasl2-dev clang git curl python3-venv >>'$root/apt.log' 2>&1
fi
[ -x '$root/.cargo/bin/rustup' ] || curl --proto =https --tlsv1.2 -sSf https://sh.rustup.rs \
  | sh -s -- -y --no-modify-path --profile minimal --default-toolchain stable -c clippy,rustfmt >'$root/rustup.log' 2>&1
if [ ! -x '$root/go/bin/go' ]; then
  curl -fsSL "https://go.dev/dl/go${go_version}.linux-amd64.tar.gz" | tar -C '$root' -xz
fi
if [ ! -x '$root/jdk/bin/java' ]; then
  mkdir -p '$root/jdk'
  curl -fsSL 'https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse' | tar -C '$root/jdk' -xz --strip-components=1
fi
if [ ! -x '$root/maven/bin/mvn' ]; then
  mkdir -p '$root/maven'
  curl -fsSL 'https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz' | tar -C '$root/maven' -xz --strip-components=1
fi
if [ ! -x '$root/dotnet/dotnet' ]; then
  curl -fsSL https://dot.net/v1/dotnet-install.sh | bash -s -- --channel 8.0 --install-dir '$root/dotnet' >'$root/dotnet-install.log' 2>&1
fi
if [ ! -x '$root/node/bin/node' ]; then
  mkdir -p '$root/node'
  curl -fsSL 'https://nodejs.org/dist/v20.18.1/node-v20.18.1-linux-x64.tar.xz' | tar -C '$root/node' -xJ --strip-components=1
fi
# buf regenerates the committed gRPC stubs; the version matches CI's buf-setup pin.
if [ ! -x '$root/bin/buf' ] || [ "\$('$root/bin/buf' --version)" != 1.65.0 ]; then
  mkdir -p '$root/bin'
  curl -fsSL 'https://github.com/bufbuild/buf/releases/download/v1.65.0/buf-Linux-x86_64' -o '$root/bin/buf'
  chmod +x '$root/bin/buf'
fi
[ -x '$root/venv/bin/python' ] || python3 -m venv '$root/venv'
[ -d '$root/repo/.git' ] || git clone -q '$repo_url' '$root/repo'
# The Python SDK's tests (pytest + its dev extras) run from the repo checkout.
'$root/venv/bin/pip' install -q -e '$root/repo/sdk/python[dev]' >'$root/pip.log' 2>&1
echo "provisioned: \$(rustc --version); \$(go version); \$(python --version); java \$(java -version 2>&1 | head -1); \$(mvn -v | head -1); dotnet \$(dotnet --version); buf \$(buf --version)"
EOF
    ;;
  sync)
    # A bundle carries commits GitHub does not have yet, and is much faster than
    # waiting for a push to propagate.
    bundle="$(mktemp -t udb-sync-XXXXXX.bundle)"
    trap 'rm -f "$bundle"' EXIT
    head="$(git rev-parse HEAD)"
    have="$(remote "git -C '$root/repo' rev-parse HEAD" 2>/dev/null || true)"
    if [ "$have" = "$head" ]; then
      echo "remote checkout already at ${head:0:8}"
      exit 0
    fi
    # Ship only the commits the remote lacks when it shares history with us.
    if [ -n "$have" ] && git cat-file -e "${have}^{commit}" 2>/dev/null; then
      git bundle create "$bundle" HEAD "^$have" --quiet
    else
      git bundle create "$bundle" HEAD --quiet
    fi
    remote "cat > '$root/sync.bundle'" < "$bundle"
    remote "cd '$root/repo' && git fetch -q '$root/sync.bundle' HEAD && git checkout -q -f --detach '$head' && git clean -qfdx -e target && rm -f '$root/sync.bundle' && git log --oneline -1"
    ;;
  run)
    shift 2
    [ "$#" -ge 1 ] || { echo "run needs a command" >&2; exit 2; }
    remote "cd '$root/repo' && . '$root/env.sh' && nice -n 19 ionice -c3 bash -c $(printf '%q' "$*")"
    ;;
  status)
    remote "du -sh '$root' 2>/dev/null; uptime; free -g | sed -n 2p; pgrep -af 'cargo|rustc|/udb |go test' | head -20 || true; docker ps --filter name=udb- --format '{{.Names}} {{.Status}}' 2>/dev/null || true"
    ;;
  cleanup)
    remote "bash -s" <<EOF
set -uo pipefail
pkill -f '$root/' 2>/dev/null || true
if command -v docker >/dev/null 2>&1; then
  ids=\$(docker ps -aq --filter name=udb-)
  [ -z "\$ids" ] || docker rm -f \$ids >/dev/null
  vols=\$(docker volume ls -q --filter name=udb-)
  [ -z "\$vols" ] || docker volume rm \$vols >/dev/null
fi
rm -rf '$root'
echo "cleaned: \$( [ -e '$root' ] && echo '$root still present' || echo '$root removed' ); udb containers: \$(docker ps -aq --filter name=udb- 2>/dev/null | wc -l)"
EOF
    ;;
  *)
    echo "unknown command: $command" >&2
    exit 2
    ;;
esac
