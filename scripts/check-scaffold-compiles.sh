#!/usr/bin/env bash
# Compile-test the example clients emitted by `udb scaffold`.
#
# The gate generates a fresh scaffold and validates every supported SDK example
# (Go, TypeScript, Python, C#, Java, PHP) against the in-repo SDK surface. CI
# passes UDB_BIN from the build-once broker artifact; local runs may fall back to
# `cargo run -- scaffold`.
#
# Usage:  scripts/check-scaffold-compiles.sh
# Env:    UDB_BIN  path to a prebuilt udb binary
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "missing generated scaffold file: $path" >&2
    exit 1
  fi
}

echo "==> generating scaffold into $WORK"
if [[ -n "${UDB_BIN:-}" ]]; then
  UDB_INIT_DIR="$WORK" "$UDB_BIN" scaffold
else
  ( cd "$REPO" && UDB_INIT_DIR="$WORK" cargo run --quiet -- scaffold )
fi

for rel in \
  examples/go/client.go \
  examples/python/client.py \
  examples/typescript/client.ts \
  examples/csharp/Client.cs \
  examples/java/Client.java \
  examples/php/client.php
do
  require_file "$WORK/$rel"
done

# ── Go: build the emitted example against the in-repo SDK module ──────────────
echo "==> compiling Go scaffold example"
GO_DIR="$WORK/gocheck"
mkdir -p "$GO_DIR"
cp "$WORK/examples/go/client.go" "$GO_DIR/main.go"
cat > "$GO_DIR/go.mod" <<EOF
module scaffoldcheck

go 1.22

require (
	github.com/fahara02/udb/sdk/go v0.0.0
	google.golang.org/grpc v1.64.0
)

replace github.com/fahara02/udb/sdk/go => $REPO/sdk/go
EOF
( cd "$GO_DIR" && go mod tidy && go build ./... )
echo "    Go scaffold example built OK"

# ── Go ENTITY ADAPTERS: run the real generator over a proto tree that HAS a
# message-valued JSON column, and require it to succeed.
#
# This closes the gap that let four releases ship a broken Go entity emitter.
# The scaffold example above is the SDK CLIENT; nothing compiled the generator's
# ENTITY output, and `entity_repo_contract_test.go` only MIRRORS the emitted code
# by hand - a hand-written mirror cannot catch an emitter bug, which is exactly
# how 0.5.18 shipped a helper block carrying literal `{{` that gofmt rejected.
#
# The repo proto tree contains `udb.core.common.v1.AuditInfo` in a JSONB column
# (asset.proto), so this exercises the message-in-JSON arm. `udb sdk generate`
# gofmt-checks its own output and exits non-zero when the Go does not parse, so
# running it here is load-bearing on its own.
echo "==> generating Go entity adapters from the repo proto tree"
ENT_DIR="$WORK/entcheck"
if [[ -n "${UDB_BIN:-}" ]]; then
  "$UDB_BIN" sdk generate --project-proto "$REPO/proto" --lang go --out "$ENT_DIR"
else
  ( cd "$REPO" && cargo run --quiet -- sdk generate --project-proto "$REPO/proto" --lang go --out "$ENT_DIR" )
fi
require_file "$ENT_DIR/go/udb_entities_gen.go"
if grep -q "{{" "$ENT_DIR/go/udb_entities_gen.go"; then
  echo "generated Go contains a doubled brace - push_str does not process format! escapes" >&2
  exit 1
fi
echo "    Go entity adapters generated and gofmt-clean OK"

# Compile the real generator's output against consumer messages produced by
# protoc, then exercise their adapters across a JSON wire round trip.
echo "==> compiling and round-tripping Go consumer entity shapes"
SHAPE_DIR="$WORK/consumer"
# The generated client templates compose the existing SDK package. Keep them
# outside the consumer module so ./... compiles only its messages and adapters.
SHAPE_GENERATED_DIR="$WORK/consumer-generated"
mkdir -p "$SHAPE_DIR/adapters" "$WORK/protoc-bin"
# Match the SDK's protobuf runtime and keep the declared Go 1.22 floor honest.
# v1.36.11 requires Go 1.23 and would silently upgrade an auto toolchain.
export GOTOOLCHAIN=local
GOBIN="$WORK/protoc-bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.1
# Debian packages the standard protobuf imports separately from protoc.
# Allow a custom installation while making missing compiler inputs explicit.
PROTOC_INCLUDE="${PROTOC_INCLUDE:-/usr/include}"
require_file "$PROTOC_INCLUDE/google/protobuf/descriptor.proto"
require_file "$PROTOC_INCLUDE/google/protobuf/timestamp.proto"
protoc -I "$PROTOC_INCLUDE" -I "$REPO/proto" -I "$REPO/tests/fixtures/consumer_protos" \
  --plugin="protoc-gen-go=$WORK/protoc-bin/protoc-gen-go" \
  --go_out="$SHAPE_DIR" --go_opt=module=example.com/consumer \
  '--go_opt=Mudb/core/common/v1/db.proto=github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1;commonv1' \
  "$REPO/tests/fixtures/consumer_protos/foreign.proto" \
  "$REPO/tests/fixtures/consumer_protos/shape.proto"
if [[ -n "${UDB_BIN:-}" ]]; then
  GENERATOR=("$UDB_BIN")
else
  GENERATOR=(cargo run --quiet --manifest-path "$REPO/Cargo.toml" --)
fi
"${GENERATOR[@]}" sdk generate --project-proto "$REPO/tests/fixtures/consumer_protos" \
  --lang go --out "$SHAPE_GENERATED_DIR"
require_file "$SHAPE_GENERATED_DIR/go/udb_entities_gen.go"
cp "$SHAPE_GENERATED_DIR/go/udb_entities_gen.go" "$SHAPE_DIR/adapters/"
cp "$REPO/tests/fixtures/consumer_protos_test.go" "$SHAPE_DIR/adapters/"
cat > "$SHAPE_DIR/go.mod" <<EOF
module example.com/consumer

go 1.22

require github.com/fahara02/udb/sdk/go v0.0.0
replace github.com/fahara02/udb/sdk/go => $REPO/sdk/go
EOF
( cd "$SHAPE_DIR" && go mod tidy && go build ./... && go vet ./... && go test ./... -count=1 )
if "${GENERATOR[@]}" sdk generate --project-proto "$REPO/tests/fixtures/consumer_protos_invalid" \
  --lang go --out "$SHAPE_DIR/invalid" > "$SHAPE_DIR/refusal.log" 2>&1; then
  echo "generator accepted a message stored outside JSON" >&2
  exit 1
fi
grep -q 'field.*payload.*message.*Payload.*TEXT column' "$SHAPE_DIR/refusal.log"
echo "    Go consumer adapters compiled, round-tripped and refused unsupported storage"

# Exercise the actual project-proto/gofmt producer and prove --check preserves
# its output. Rust CLI integration tests cover generic templates and copied files.
echo "==> checking Go entity generator drift without output writes"
python - "$REPO" "$WORK" "${UDB_BIN:-}" <<'PY'
import os
from pathlib import Path
import subprocess
import sys

repo = Path(sys.argv[1]).resolve()
work = Path(sys.argv[2]).resolve() / "generate-check"
work.mkdir()
output = work / "requested output"
if sys.argv[3]:
    generator = [str(Path(sys.argv[3]).resolve())]
else:
    generator = ["cargo", "run", "--quiet", "--manifest-path", str(repo / "Cargo.toml"), "--"]

def arguments(out=output, package="checkentities", check=False):
    args = generator + [
        "sdk", "generate", "--project-proto", str(repo / "proto"),
        "--lang", "go", "--templates", str(repo / "sdk-templates"),
        "--go-package", package, "--out", str(out),
    ]
    if check:
        args.append("--check")
    return args

def run(args, expected):
    result = subprocess.run(args, cwd=work, capture_output=True, text=True, timeout=120)
    assert result.returncode == expected, (
        f"expected exit {expected}, got {result.returncode}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    return result

def snapshot():
    entries = {}
    for path in [work, *sorted(work.rglob("*"))]:
        metadata = path.stat()
        entries[str(path.relative_to(work))] = (
            path.is_dir(), metadata.st_mtime_ns,
            None if path.is_dir() else path.read_bytes(),
        )
    return entries

def check(expected, out=output, package="checkentities"):
    # A same-byte rewrite must still fail this check, even on coarse filesystems.
    for path in work.rglob("*"):
        if path.is_file():
            os.utime(path, ns=(1_234_567_890_000_000_000,) * 2)
    before = snapshot()
    result = run(arguments(out, package, check=True), expected)
    assert snapshot() == before, "sdk generate --check changed output paths, bytes or mtimes"
    return result

run(arguments(), 0)
entity_file = output / "go" / "udb_entities_gen.go"
assert "\npackage checkentities\n" in entity_file.read_text()
check(0)
check(1, package="differententities")
with entity_file.open("ab") as stream:
    stream.write(b"\n// Deliberately stale output for the CI regression.\n")
assert "udb_entities_gen.go" in check(1).stderr
run(arguments(), 0)
check(0)
entity_file.unlink()
check(1)
assert not entity_file.exists(), "check recreated the missing entity file"
run(arguments(), 0)
check(0)
missing = work / "missing output" / "never created"
check(1, out=missing)
assert not missing.parent.exists(), "check created the missing output directory"
assert not (work / "sdk").exists(), "generator ignored the requested output directory"
print("    Actual Go entity output checks fresh, package drift, stale, missing and repair paths")
PY

# ── TypeScript: type-check the emitted example ────────────────────────────────
echo "==> type-checking TypeScript scaffold example"
TS_DIR="$WORK/tscheck"
mkdir -p "$TS_DIR/examples/typescript"
cp "$WORK/examples/typescript/client.ts" "$TS_DIR/examples/typescript/client.ts"
ln -s "$REPO/proto" "$TS_DIR/proto"
( cd "$TS_DIR"
  npm init -y >/dev/null 2>&1
  npm install --no-audit --no-fund --silent \
    typescript @types/node @grpc/grpc-js @grpc/proto-loader >/dev/null 2>&1
  npx --yes tsc --noEmit --esModuleInterop --skipLibCheck --moduleResolution node16 \
    --target ES2020 --module Node16 examples/typescript/client.ts )
echo "    TypeScript scaffold example type-checked OK"

# ── Python: syntax-compile and import the generated UDB stubs it references ───
echo "==> compiling Python scaffold example"
PY_DIR="$WORK/pycheck"
mkdir -p "$PY_DIR/gen"
cp "$WORK/examples/python/client.py" "$PY_DIR/client.py"
ln -s "$REPO/sdk/python/gen" "$PY_DIR/gen/python"
( cd "$PY_DIR"
  python -m pip install --quiet --disable-pip-version-check "grpcio>=1.80" "protobuf>=6.31.1,<7"
  python -m py_compile client.py
  python - <<'PY'
import sys
sys.path.insert(0, "gen/python")
from udb.entity.v1 import types_pb2
from udb.services.v1 import data_broker_pb2_grpc
assert types_pb2.HealthReportRequest
assert data_broker_pb2_grpc.DataBrokerStub
PY
)
echo "    Python scaffold example compiled OK"

# ── C#: build the emitted top-level program against the local SDK project ─────
echo "==> compiling C# scaffold example"
CS_DIR="$WORK/cscheck"
mkdir -p "$CS_DIR"
cp "$WORK/examples/csharp/Client.cs" "$CS_DIR/Program.cs"
cat > "$CS_DIR/ScaffoldCheck.csproj" <<EOF
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="$REPO/sdk/csharp/Udb.Client/Udb.Client.csproj" />
  </ItemGroup>
</Project>
EOF
( cd "$CS_DIR" && dotnet build -c Release --nologo )
echo "    C# scaffold example built OK"

# ── Java: compile the emitted class with the local Java SDK source+gen roots ──
echo "==> compiling Java scaffold example"
JAVA_DIR="$WORK/javacheck"
mkdir -p "$JAVA_DIR/src/main/java"
cp "$WORK/examples/java/Client.java" "$JAVA_DIR/src/main/java/Client.java"
cat > "$JAVA_DIR/pom.xml" <<EOF
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>dev.udb.scaffold</groupId>
  <artifactId>scaffoldcheck</artifactId>
  <version>0.0.0</version>
  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <grpc.version>1.81.0</grpc.version>
    <protobuf.version>4.35.0</protobuf.version>
  </properties>
  <dependencies>
    <dependency><groupId>io.grpc</groupId><artifactId>grpc-api</artifactId><version>\${grpc.version}</version></dependency>
    <dependency><groupId>io.grpc</groupId><artifactId>grpc-stub</artifactId><version>\${grpc.version}</version></dependency>
    <dependency><groupId>io.grpc</groupId><artifactId>grpc-protobuf</artifactId><version>\${grpc.version}</version></dependency>
    <dependency><groupId>io.grpc</groupId><artifactId>grpc-netty-shaded</artifactId><version>\${grpc.version}</version></dependency>
    <dependency><groupId>com.google.protobuf</groupId><artifactId>protobuf-java</artifactId><version>\${protobuf.version}</version></dependency>
    <dependency><groupId>javax.annotation</groupId><artifactId>javax.annotation-api</artifactId><version>1.3.2</version></dependency>
    <dependency><groupId>jakarta.servlet</groupId><artifactId>jakarta.servlet-api</artifactId><version>6.1.0</version><scope>provided</scope><optional>true</optional></dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.codehaus.mojo</groupId>
        <artifactId>build-helper-maven-plugin</artifactId>
        <version>3.6.0</version>
        <executions>
          <execution>
            <id>add-udb-sdk-sources</id>
            <phase>generate-sources</phase>
            <goals><goal>add-source</goal></goals>
            <configuration>
              <sources>
                <source>$REPO/sdk/java/src/main/java</source>
                <source>$REPO/sdk/java/gen</source>
              </sources>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
EOF
# Recheck missing releases so a transient registry miss is not retained.
(
  cd "$JAVA_DIR"
  if [[ "${CI:-}" == "true" ]]; then
    # Resolve plugin downloads through the shared bounded CI setup retry. The
    # compile itself runs once after dependencies have been fetched.
    bash "$REPO/scripts/ci-retry.sh" -- mvn -U -B -ntp dependency:resolve-plugins
  fi
  mvn -U -B -ntp compile
)
echo "    Java scaffold example built OK"

# ── PHP: resolve the local package, lint the example, prove referenced classes ─
echo "==> compiling PHP scaffold example"
PHP_DIR="$WORK/phpcheck"
mkdir -p "$PHP_DIR"
cp "$WORK/examples/php/client.php" "$PHP_DIR/client.php"
cat > "$PHP_DIR/composer.json" <<EOF
{
  "repositories": [
    { "type": "path", "url": "$REPO/sdk/php", "options": { "symlink": true } }
  ],
  "require": {
    "fahara02/udb-laravel": "*"
  },
  "minimum-stability": "dev",
  "prefer-stable": true
}
EOF
( cd "$PHP_DIR"
  composer install --no-interaction --no-progress --prefer-dist
  php -l client.php
  php -r 'require "vendor/autoload.php"; foreach (["Udb\\Services\\V1\\DataBrokerClient", "Udb\\Entity\\V1\\HealthReportRequest", "Udb\\Entity\\V1\\RequestContext"] as $c) { if (!class_exists($c)) { fwrite(STDERR, "missing class $c\n"); exit(1); } }' )
echo "    PHP scaffold example compiled OK"

echo "OK: emitted Go, TypeScript, Python, C#, Java, and PHP scaffolds compile."
