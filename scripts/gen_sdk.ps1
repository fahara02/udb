param(
    [switch]$CheckOnly,
    [string]$ProtoRoot = "",
    [string]$OutRoot = ""
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$UdbRoot = Resolve-Path (Join-Path $ScriptDir "..")
$RepoRoot = Resolve-Path (Join-Path $UdbRoot "..")

if ([string]::IsNullOrWhiteSpace($ProtoRoot)) {
    $ProtoRoot = Join-Path $RepoRoot "proto"
}
if ([string]::IsNullOrWhiteSpace($OutRoot)) {
    $OutRoot = Join-Path $UdbRoot "sdk"
}

$ProtoRoot = (Resolve-Path $ProtoRoot).Path
$OutRoot = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutRoot)

$ProtoFiles = @(
    "udb/entity/v1/types.proto",
    "udb/events/v1/udb_events.proto",
    "udb/services/v1/data_broker.proto"
)

function Require-Command($Name, $Hint) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Missing required command '$Name'. $Hint"
    }
}

function Require-PythonGrpcTools {
    & python -c "import grpc_tools.protoc" 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "Missing Python grpcio-tools. Install with: python -m pip install grpcio grpcio-tools"
    }
}

Require-Command "protoc" "Install Protocol Buffers compiler and add it to PATH."
Require-Command "protoc-gen-go" "Install with: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"
Require-Command "protoc-gen-go-grpc" "Install with: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"
Require-Command "python" "Install Python 3 and add it to PATH."
Require-PythonGrpcTools

$GrpcCSharpPlugin = (Get-Command "grpc_csharp_plugin" -ErrorAction SilentlyContinue)
$CSharpEnabled = $null -ne $GrpcCSharpPlugin
if (-not $CSharpEnabled) {
    Write-Warning "grpc_csharp_plugin not found — C# SDK generation will be skipped. Install Grpc.Tools to enable it."
}
$GrpcJavaPlugin = (Get-Command "protoc-gen-grpc-java" -ErrorAction SilentlyContinue)
$JavaEnabled = $null -ne $GrpcJavaPlugin
if (-not $JavaEnabled) {
    Write-Warning "protoc-gen-grpc-java not found — Java SDK generation will be skipped."
}

foreach ($Proto in $ProtoFiles) {
    $FullPath = Join-Path $ProtoRoot ($Proto -replace "/", [IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path $FullPath)) {
        throw "Missing proto file: $FullPath"
    }
}

if ($CheckOnly) {
    Write-Host "SDK generation prerequisites are available."
    if (-not $CSharpEnabled) { Write-Host "  [warn] C# SDK skipped — grpc_csharp_plugin not found." }
    if (-not $JavaEnabled) { Write-Host "  [warn] Java SDK skipped — protoc-gen-grpc-java not found." }
    Write-Host "Proto root: $ProtoRoot"
    Write-Host "Output root: $OutRoot"
    exit 0
}

$GoOut = Join-Path $OutRoot "go\gen\go"
$PythonOut = Join-Path $OutRoot "python"
$CSharpOut = Join-Path $OutRoot "csharp\Generated"
$JavaOut = Join-Path $OutRoot "java\gen\java"
New-Item -ItemType Directory -Force -Path $GoOut, $PythonOut | Out-Null

Push-Location $ProtoRoot
try {
    & protoc -I $ProtoRoot `
        --go_out=$GoOut --go_opt=paths=source_relative `
        --go-grpc_out=$GoOut --go-grpc_opt=paths=source_relative `
        $ProtoFiles
    if ($LASTEXITCODE -ne 0) { throw "Go SDK generation failed." }

    & python -m grpc_tools.protoc -I $ProtoRoot `
        --python_out=$PythonOut `
        --grpc_python_out=$PythonOut `
        --pyi_out=$PythonOut `
        $ProtoFiles
    if ($LASTEXITCODE -ne 0) { throw "Python SDK generation failed." }

    if ($CSharpEnabled) {
        New-Item -ItemType Directory -Force -Path $CSharpOut | Out-Null
        & protoc -I $ProtoRoot `
            --csharp_out=$CSharpOut `
            --grpc_out=$CSharpOut `
            "--plugin=protoc-gen-grpc=$($GrpcCSharpPlugin.Source)" `
            $ProtoFiles
        if ($LASTEXITCODE -ne 0) { throw "C# SDK generation failed." }
    } else {
        Write-Warning "C# SDK skipped — grpc_csharp_plugin not available."
    }

    if ($JavaEnabled) {
        New-Item -ItemType Directory -Force -Path $JavaOut | Out-Null
        & protoc -I $ProtoRoot `
            --java_out=$JavaOut `
            --grpc-java_out=$JavaOut `
            $ProtoFiles
        if ($LASTEXITCODE -ne 0) { throw "Java SDK generation failed." }
    } else {
        Write-Warning "Java SDK skipped — protoc-gen-grpc-java not available."
    }
}
finally {
    Pop-Location
}

Write-Host "Generated UDB SDKs under $OutRoot"
