[CmdletBinding()]
param(
    [ValidateSet("auto", "docker", "release")]
    [string] $Runner = $(if ($env:UDB_RUNNER) { $env:UDB_RUNNER } else { "auto" }),

    [string] $Version = $(if ($env:UDB_VERSION) { $env:UDB_VERSION } else { "latest" }),
    [string] $Repo = $(if ($env:UDB_GITHUB_REPO) { $env:UDB_GITHUB_REPO } else { "fahara02/udb" }),

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $UdbArgs
)

$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

function Invoke-UdbDocker {
    docker compose --project-directory $ProjectRoot run --rm udb-cli @UdbArgs
}

function Get-ReleaseCli {
    $binDir = Join-Path $ProjectRoot ".udb/bin"
    New-Item -ItemType Directory -Force $binDir | Out-Null
    $exe = if ($IsWindows -or $env:OS -eq "Windows_NT") { "udb-proto-parser.exe" } else { "udb-proto-parser" }
    $target = Join-Path $binDir $exe
    if (Test-Path $target) {
        return $target
    }

    if ($env:UDB_CLI_URL) {
        if ((Test-Path $env:UDB_CLI_URL) -and -not (Get-Item $env:UDB_CLI_URL).PSIsContainer) {
            Copy-Item -LiteralPath $env:UDB_CLI_URL -Destination $target -Force
            return $target
        }
        Invoke-WebRequest -Uri $env:UDB_CLI_URL -OutFile $target
        if (-not ($IsWindows -or $env:OS -eq "Windows_NT")) {
            chmod +x $target
        }
        return $target
    }

    $releaseUrl = if ($Version -eq "latest") {
        "https://api.github.com/repos/$Repo/releases/latest"
    } else {
        "https://api.github.com/repos/$Repo/releases/tags/$Version"
    }
    $release = Invoke-RestMethod -Headers @{ "User-Agent" = "udb-example" } -Uri $releaseUrl
    $os = if ($IsWindows -or $env:OS -eq "Windows_NT") { "windows" } elseif ($IsMacOS) { "darwin|macos" } else { "linux" }
    $arch = if ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -match "Arm64") { "arm64|aarch64" } else { "amd64|x86_64" }
    $asset = $release.assets |
        Where-Object { $_.name -match "udb-proto-parser" -and $_.name -match $os -and $_.name -match $arch -and $_.name -notmatch "\.(zip|tar\.gz)$" } |
        Select-Object -First 1
    if (-not $asset) {
        throw "No matching UDB CLI release asset found. Set UDB_CLI_URL to the exact udb-proto-parser binary URL."
    }
    Invoke-WebRequest -Uri $asset.browser_download_url -OutFile $target
    if (-not ($IsWindows -or $env:OS -eq "Windows_NT")) {
        chmod +x $target
    }
    return $target
}

if ($Runner -eq "docker") {
    Invoke-UdbDocker
    exit $LASTEXITCODE
}

if ($Runner -eq "auto") {
    if ($env:UDB_CLI -and (Test-Path $env:UDB_CLI)) {
        & $env:UDB_CLI @UdbArgs
        exit $LASTEXITCODE
    }
    $installed = Get-Command udb-proto-parser -ErrorAction SilentlyContinue
    if ($installed) {
        & $installed.Source @UdbArgs
        exit $LASTEXITCODE
    }
    $docker = Get-Command docker -ErrorAction SilentlyContinue
    if ($docker) {
        Invoke-UdbDocker
        exit $LASTEXITCODE
    }
}

$cli = Get-ReleaseCli
& $cli @UdbArgs
exit $LASTEXITCODE
