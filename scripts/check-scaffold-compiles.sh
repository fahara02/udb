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
( cd "$JAVA_DIR" && mvn -B -ntp compile )
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
  php -r 'require "vendor/autoload.php"; foreach (["Udb\\\\Services\\\\V1\\\\DataBrokerClient", "Udb\\\\Entity\\\\V1\\\\HealthReportRequest", "Udb\\\\Entity\\\\V1\\\\RequestContext"] as $c) { if (!class_exists($c)) { fwrite(STDERR, "missing class $c\n"); exit(1); } }' )
echo "    PHP scaffold example compiled OK"

echo "OK: emitted Go, TypeScript, Python, C#, Java, and PHP scaffolds compile."
