#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Bootstrap Windows native dependencies for UDB WebAuthn builds.

.DESCRIPTION
    WebAuthn is implemented with webauthn-rs, which builds vendored OpenSSL on
    Windows/MSVC. OpenSSL needs a native Windows Perl plus MSVC build tools; Git
    Bash/MSYS Perl is not compatible with the MSVC build.

    This script installs the Windows prerequisites used by CI/release builds:
      - Strawberry Perl
      - NASM
      - CMake
      - Ninja
      - Visual Studio 2022 Build Tools with the C++ toolchain

    It also puts Strawberry Perl ahead of Git/MSYS Perl for this shell and for
    the user PATH.

.PARAMETER CheckOnly
    Probe dependencies and print the current state without installing anything.

.PARAMETER InstallChocolatey
    Install Chocolatey if it is missing. Requires an elevated PowerShell.

.PARAMETER SkipVsBuildTools
    Do not install Visual Studio Build Tools. Use this only if MSVC is already
    installed or you are running from a Developer PowerShell.

.PARAMETER FetchCargo
    Resolve and fetch Cargo dependencies after native prerequisites are checked.
    Resolves the normal default feature graph plus WebAuthn/OIDC, matching
    Cargo's `--features oidc,webauthn` behavior.

.PARAMETER CleanNativeBuildCache
    Remove stale native CMake build directories under the selected Cargo target
    directory. Use this when CMake reports that a previous generator, such as
    "Visual Studio 18 2026", does not match the current generator.

.PARAMETER RepairChocolateyLocks
    If Chocolatey reports a stale NuGet lock under C:\ProgramData\chocolatey\lib,
    remove that lock and retry the package install. The script refuses to do
    this while choco, NuGet, or msiexec processes are running.

.PARAMETER CargoTargetDir
    Optional CARGO_TARGET_DIR used when FetchCargo is enabled.

.EXAMPLE
    .\scripts\bootstrap-webauthn.ps1

.EXAMPLE
    .\scripts\bootstrap-webauthn.ps1 -CheckOnly

.EXAMPLE
    .\scripts\bootstrap-webauthn.ps1 -InstallChocolatey -FetchCargo
#>
param(
    [switch]$CheckOnly,
    [switch]$InstallChocolatey,
    [switch]$SkipVsBuildTools,
    [switch]$FetchCargo,
    [switch]$CleanNativeBuildCache,
    [switch]$RepairChocolateyLocks,
    [string]$CargoTargetDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
$RequiredFeatures = "oidc,webauthn"
$ReleaseBinaryFeatures = "postgres,mysql,sqlite,qdrant,s3,mongodb,neo4j,clickhouse,redis,elasticsearch,weaviate,pinecone,azureblob,gcs,otel,runtime-logging,http-client,oidc,webauthn"

function Test-IsWindows {
    return [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [System.Runtime.InteropServices.OSPlatform]::Windows
    )
}

function Test-IsAdmin {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Get-CommandPath {
    param([Parameter(Mandatory = $true)][string]$Name)
    $cmd = Get-Command $Name -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    return $null
}

function Add-PathForCurrentProcess {
    param([Parameter(Mandatory = $true)][string]$PathToAdd)
    if (-not (Test-Path $PathToAdd)) { return }

    $parts = [System.Collections.Generic.List[string]]::new()
    $parts.Add($PathToAdd)
    foreach ($part in ($env:Path -split ';')) {
        if ([string]::IsNullOrWhiteSpace($part)) { continue }
        if ($part.TrimEnd('\') -ieq $PathToAdd.TrimEnd('\')) { continue }
        $parts.Add($part)
    }
    $env:Path = ($parts -join ';')
}

function Add-ExistingPathForCurrentProcess {
    param([Parameter(Mandatory = $true)][string]$PathToAdd)
    if (Test-Path $PathToAdd) {
        Add-PathForCurrentProcess $PathToAdd
    }
}

function Add-UserPathIfMissing {
    param([Parameter(Mandatory = $true)][string]$PathToAdd)
    if (-not (Test-Path $PathToAdd)) { return }

    $current = [Environment]::GetEnvironmentVariable("Path", "User")
    if ([string]::IsNullOrWhiteSpace($current)) {
        [Environment]::SetEnvironmentVariable("Path", $PathToAdd, "User")
        return
    }

    $parts = $current -split ';' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    $exists = $false
    foreach ($part in $parts) {
        if ($part.TrimEnd('\') -ieq $PathToAdd.TrimEnd('\')) {
            $exists = $true
            break
        }
    }
    if (-not $exists) {
        [Environment]::SetEnvironmentVariable("Path", "$PathToAdd;$current", "User")
    }
}

function Add-KnownToolPaths {
    Add-ExistingPathForCurrentProcess "$env:ProgramData\chocolatey\bin"
    Add-ExistingPathForCurrentProcess "C:\Program Files\NASM"
    Add-ExistingPathForCurrentProcess "C:\Program Files\CMake\bin"
    Add-ExistingPathForCurrentProcess "C:\Program Files (x86)\CMake\bin"
    Add-StrawberryPerlToPath | Out-Null
}

function Ensure-Chocolatey {
    $choco = Get-CommandPath "choco"
    if ($choco) { return $choco }

    if ($CheckOnly) {
        Write-Warning "Chocolatey is not installed."
        return $null
    }

    if (-not $InstallChocolatey) {
        throw "Chocolatey is not installed. Re-run with -InstallChocolatey from an elevated PowerShell, or install packages manually."
    }

    if (-not (Test-IsAdmin)) {
        throw "Installing Chocolatey requires an elevated PowerShell."
    }

    Set-ExecutionPolicy Bypass -Scope Process -Force
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    Invoke-Expression ((New-Object Net.WebClient).DownloadString("https://community.chocolatey.org/install.ps1"))
    Add-PathForCurrentProcess "$env:ProgramData\chocolatey\bin"

    $choco = Get-CommandPath "choco"
    if (-not $choco) {
        throw "Chocolatey install completed but choco.exe is not on PATH."
    }
    return $choco
}

function Install-ChocoPackage {
    param(
        [Parameter(Mandatory = $true)][string]$Package,
        [string[]]$ExtraArgs = @()
    )

    if ($CheckOnly) { return }

    $args = @("install", $Package, "-y", "--no-progress")
    if ($ExtraArgs.Count -gt 0) {
        $args += $ExtraArgs
    }
    $output = & choco @args 2>&1
    $exitCode = $LASTEXITCODE
    $output | ForEach-Object { Write-Host $_ }

    if ($exitCode -ne 0) {
        $lockPath = Get-ChocolateyLockPathFromOutput -Output $output
        if ($lockPath) {
            if (-not $RepairChocolateyLocks) {
                throw "Chocolatey failed installing package '$Package' because a lock exists at '$lockPath'. If no other install is running, re-run with -RepairChocolateyLocks."
            }

            Clear-ChocolateyLock -LockPath $lockPath
            Write-Host "Retrying Chocolatey package '$Package' after clearing stale lock..."
            $retryOutput = & choco @args 2>&1
            $retryExitCode = $LASTEXITCODE
            $retryOutput | ForEach-Object { Write-Host $_ }
            if ($retryExitCode -eq 0) {
                return
            }
        }

        throw "Chocolatey failed installing package '$Package'."
    }
}

function Get-ChocolateyLockPathFromOutput {
    param([Parameter(Mandatory = $true)][object[]]$Output)

    $text = ($Output | ForEach-Object { [string]$_ }) -join "`n"
    $match = [regex]::Match(
        $text,
        "Unable to obtain lock file access on '([^']+)'",
        [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
    )
    if ($match.Success) {
        return $match.Groups[1].Value
    }
    return $null
}

function Clear-ChocolateyLock {
    param([Parameter(Mandatory = $true)][string]$LockPath)

    if (-not (Test-IsAdmin)) {
        throw "Repairing Chocolatey locks requires an elevated PowerShell."
    }

    $active = Get-Process -Name choco, chocolatey, nuget, msiexec -ErrorAction SilentlyContinue |
        Where-Object { $_.Id -ne $PID }
    if ($active) {
        $names = ($active | ForEach-Object { "$($_.ProcessName)#$($_.Id)" }) -join ", "
        throw "Refusing to remove Chocolatey lock while installer processes are running: $names"
    }

    $chocoLibRoot = "C:\ProgramData\chocolatey\lib"
    $fullLockPath = [System.IO.Path]::GetFullPath($LockPath)
    $fullRoot = [System.IO.Path]::GetFullPath($chocoLibRoot).TrimEnd('\') + "\"
    if (-not $fullLockPath.StartsWith($fullRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove lock outside Chocolatey lib root: $fullLockPath"
    }

    if (-not (Test-Path -LiteralPath $fullLockPath)) {
        Write-Warning "Chocolatey lock path no longer exists: $fullLockPath"
        return
    }

    Write-Warning "Removing stale Chocolatey lock: $fullLockPath"
    Remove-Item -LiteralPath $fullLockPath -Force -Recurse
}

function Ensure-ToolPackage {
    param(
        [Parameter(Mandatory = $true)][string]$CommandName,
        [Parameter(Mandatory = $true)][string]$PackageName,
        [string[]]$ExtraArgs = @()
    )

    Add-KnownToolPaths

    $path = Get-CommandPath $CommandName
    if ($path) {
        Write-Host "[ok] ${CommandName}: $path"
        return
    }

    if ($CheckOnly) {
        Write-Warning "Missing $CommandName. Chocolatey package: $PackageName"
        return
    }

    Write-Host "Installing $PackageName for $CommandName..."
    Install-ChocoPackage -Package $PackageName -ExtraArgs $ExtraArgs
}

function Get-StrawberryPerlBin {
    $candidates = @(
        "C:\Strawberry\perl\bin",
        "C:\tools\strawberry\perl\bin",
        "C:\tools\StrawberryPerl\perl\bin"
    )
    foreach ($candidate in $candidates) {
        if (Test-Path (Join-Path $candidate "perl.exe")) {
            return $candidate
        }
    }
    return $null
}

function Add-StrawberryPerlToPath {
    $bin = Get-StrawberryPerlBin
    if (-not $bin) { return $false }

    Add-PathForCurrentProcess $bin
    Add-UserPathIfMissing $bin
    return $true
}

function Get-PerlConfigValue {
    param(
        [Parameter(Mandatory = $true)][string]$PerlPath,
        [Parameter(Mandatory = $true)][string]$Key
    )

    $script = "print `$Config{$Key}"
    $value = (& $PerlPath -MConfig -e $script) 2>$null
    if ($LASTEXITCODE -ne 0) { return $null }
    return $value
}

function Test-PerlIsNativeWindows {
    $perl = Get-CommandPath "perl"
    if (-not $perl) { return $false }

    $osname = Get-PerlConfigValue -PerlPath $perl -Key "osname"
    return $osname -eq "MSWin32"
}

function Ensure-NativeWindowsPerl {
    Add-StrawberryPerlToPath | Out-Null

    if (Test-PerlIsNativeWindows) {
        Write-Host "[ok] perl: $(Get-CommandPath "perl")"
        return
    }

    if ($CheckOnly) {
        $current = Get-CommandPath "perl"
        if ($current) {
            Write-Warning "perl is present but is not native Windows Perl for OpenSSL/MSVC builds: $current"
        } else {
            Write-Warning "Missing native Windows Perl. Chocolatey package: strawberryperl"
        }
        return
    }

    Write-Host "Installing strawberryperl for native Windows Perl..."
    Install-ChocoPackage -Package "strawberryperl"
    Add-StrawberryPerlToPath | Out-Null

    if (-not (Test-PerlIsNativeWindows)) {
        $current = Get-CommandPath "perl"
        $strawberry = Get-StrawberryPerlBin
        throw "Strawberry Perl is installed but perl on PATH is still not native Windows Perl. Current perl: $current. Strawberry bin: $strawberry. Start a new elevated PowerShell or put Strawberry Perl first in PATH."
    }

    Write-Host "[ok] perl: $(Get-CommandPath "perl")"
}

function Test-MsvcAvailable {
    return [bool](Get-CommandPath "cl") -and [bool](Get-CommandPath "nmake")
}

function Find-VsDevCmd {
    $candidates = @(
        "${env:ProgramFiles(x86)}\Microsoft Visual Studio\2022\BuildTools\Common7\Tools\VsDevCmd.bat",
        "${env:ProgramFiles}\Microsoft Visual Studio\2022\BuildTools\Common7\Tools\VsDevCmd.bat",
        "${env:ProgramFiles}\Microsoft Visual Studio\2022\Community\Common7\Tools\VsDevCmd.bat",
        "${env:ProgramFiles}\Microsoft Visual Studio\2022\Professional\Common7\Tools\VsDevCmd.bat",
        "${env:ProgramFiles}\Microsoft Visual Studio\2022\Enterprise\Common7\Tools\VsDevCmd.bat"
    )
    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) { return $candidate }
    }
    return $null
}

function Import-VsDevCmd {
    $vsDevCmd = Find-VsDevCmd
    if (-not $vsDevCmd) { return $false }

    $envDump = & cmd /s /c "`"$vsDevCmd`" -arch=amd64 -host_arch=amd64 >nul && set"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to initialize MSVC environment via $vsDevCmd"
    }

    foreach ($line in $envDump) {
        $idx = $line.IndexOf('=')
        if ($idx -le 0) { continue }
        $name = $line.Substring(0, $idx)
        $value = $line.Substring($idx + 1)
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
    return $true
}

function Ensure-MsvcBuildTools {
    if (Test-MsvcAvailable) {
        Write-Host "[ok] MSVC build tools: cl and nmake are on PATH"
        return
    }

    if (Import-VsDevCmd -and (Test-MsvcAvailable)) {
        Write-Host "[ok] MSVC build tools initialized for this shell"
        return
    }

    if ($SkipVsBuildTools) {
        Write-Warning "MSVC build tools are not on PATH and installation was skipped."
        return
    }

    if ($CheckOnly) {
        Write-Warning "MSVC build tools are missing. Install Visual Studio 2022 Build Tools with the C++ workload."
        return
    }

    Write-Host "Installing Visual Studio 2022 Build Tools C++ workload..."
    Install-ChocoPackage `
        -Package "visualstudio2022buildtools" `
        -ExtraArgs @("--package-parameters", "--add Microsoft.VisualStudio.Workload.VCTools --includeRecommended --passive --norestart")

    if (Import-VsDevCmd -and (Test-MsvcAvailable)) {
        Write-Host "[ok] MSVC build tools initialized for this shell"
        return
    }

    throw "Visual Studio Build Tools installed, but cl/nmake are still unavailable. Open a new PowerShell or Developer PowerShell and re-run -CheckOnly."
}

function Print-Probe {
    Write-Host ""
    Write-Host "Dependency probe:"
    foreach ($cmd in @("perl", "nasm", "cmake", "ninja", "cargo", "cl", "nmake")) {
        $path = Get-CommandPath $cmd
        if ($path) {
            Write-Host "  [ok] $cmd -> $path"
        } else {
            Write-Host "  [missing] $cmd"
        }
    }

    if (Get-CommandPath "perl") {
        $perlPath = Get-CommandPath "perl"
        $osname = Get-PerlConfigValue -PerlPath $perlPath -Key "osname"
        $archname = Get-PerlConfigValue -PerlPath $perlPath -Key "archname"
        Write-Host "  perl osname: $osname"
        Write-Host "  perl archname: $archname"
        if (Test-PerlIsNativeWindows) {
            Write-Host "  [ok] perl reports native Windows platform"
        } else {
            $strawberry = Get-StrawberryPerlBin
            Write-Warning "perl is not a native Windows Perl for OpenSSL/MSVC builds. Current: $perlPath. Strawberry bin: $strawberry. Strawberry Perl must be first in PATH."
        }
    }
}

function Resolve-CargoFeatureSet {
    param(
        [Parameter(Mandatory = $true)][string]$Features,
        [switch]$NoDefaultFeatures
    )

    $args = @("metadata", "--locked", "--format-version", "1", "--features", $Features)
    if ($NoDefaultFeatures) {
        $args += "--no-default-features"
    }

    & cargo @args | Out-Null
    if ($LASTEXITCODE -ne 0) {
        $mode = if ($NoDefaultFeatures) { "no-default-features " } else { "" }
        throw "cargo metadata failed resolving ${mode}features '$Features'."
    }
}

function Get-CargoTargetDir {
    if (-not [string]::IsNullOrWhiteSpace($CargoTargetDir)) {
        return $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($CargoTargetDir)
    }
    return Join-Path $RepoRoot "target"
}

function Clear-NativeBuildCache {
    $targetDir = Get-CargoTargetDir
    $resolvedTarget = [System.IO.Path]::GetFullPath($targetDir)
    $repoFull = [System.IO.Path]::GetFullPath($RepoRoot)

    if (-not $resolvedTarget.StartsWith($repoFull, [System.StringComparison]::OrdinalIgnoreCase) -and
        -not $resolvedTarget.StartsWith("E:\tmp\", [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to clean native build cache outside repo target or E:\tmp: $resolvedTarget"
    }

    if (-not (Test-Path -LiteralPath $resolvedTarget)) {
        Write-Host "Native build cache clean skipped; target dir does not exist: $resolvedTarget"
        return
    }

    $cacheFiles = Get-ChildItem -LiteralPath $resolvedTarget -Recurse -Filter "CMakeCache.txt" -File -ErrorAction SilentlyContinue |
        Where-Object {
            $_.FullName -match "\\target\\(debug|release)\\build\\(rdkafka-sys|openssl-sys|curl-sys|aws-lc-sys)-" -or
            $_.FullName -match "\\build\\(rdkafka-sys|openssl-sys|curl-sys|aws-lc-sys)-"
        }

    if (-not $cacheFiles) {
        Write-Host "No stale native CMake build directories found under $resolvedTarget"
        return
    }

    function Remove-NativeCachePath {
        param([Parameter(Mandatory = $true)][string]$Path)

        if (-not (Test-Path -LiteralPath $Path)) { return }

        try {
            Get-ChildItem -LiteralPath $Path -Recurse -Force -ErrorAction SilentlyContinue |
                ForEach-Object {
                    if ($_.Attributes -band [System.IO.FileAttributes]::ReadOnly) {
                        $_.Attributes = $_.Attributes -band (-bnot [System.IO.FileAttributes]::ReadOnly)
                    }
                }
            $item = Get-Item -LiteralPath $Path -Force
            if ($item.Attributes -band [System.IO.FileAttributes]::ReadOnly) {
                $item.Attributes = $item.Attributes -band (-bnot [System.IO.FileAttributes]::ReadOnly)
            }
            Remove-Item -LiteralPath $Path -Recurse -Force
            return
        } catch [System.UnauthorizedAccessException] {
            if (-not (Test-IsAdmin)) {
                throw "Access denied removing '$Path'. Re-run PowerShell as Administrator, or use a fresh target dir: `$env:CARGO_TARGET_DIR='E:\tmp\udb-target-full-default'."
            }

            $user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
            & takeown.exe /F $Path /R /D Y | Out-Null
            & icacls.exe $Path /grant "${user}:(OI)(CI)F" /T /C | Out-Null
            Remove-Item -LiteralPath $Path -Recurse -Force
            return
        }
    }

    foreach ($cacheFile in $cacheFiles) {
        $cacheFull = [System.IO.Path]::GetFullPath($cacheFile.FullName)
        $buildDir = [System.IO.Path]::GetFullPath($cacheFile.Directory.FullName)
        if (-not $cacheFull.StartsWith($resolvedTarget, [System.StringComparison]::OrdinalIgnoreCase) -or
            -not $buildDir.StartsWith($resolvedTarget, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw "Refusing to remove path outside target dir: $cacheFull"
        }

        Write-Warning "Removing native CMake cache file: $cacheFull"
        Remove-NativeCachePath -Path $cacheFull

        $cmakeFiles = Join-Path $buildDir "CMakeFiles"
        if (Test-Path -LiteralPath $cmakeFiles) {
            Write-Warning "Removing native CMakeFiles directory: $cmakeFiles"
            Remove-NativeCachePath -Path $cmakeFiles
        }
    }
}

if (-not (Test-IsWindows)) {
    throw "bootstrap-webauthn.ps1 is for Windows. Linux/macOS CI installs perl/nasm/cmake/ninja in workflow steps."
}

if (-not $CheckOnly -and -not (Test-IsAdmin)) {
    Write-Warning "Package installation may require an elevated PowerShell. If Chocolatey fails, re-run as Administrator."
}

Add-KnownToolPaths
Ensure-Chocolatey | Out-Null

Ensure-NativeWindowsPerl
Ensure-ToolPackage -CommandName "nasm" -PackageName "nasm"
Ensure-ToolPackage -CommandName "cmake" -PackageName "cmake"
Ensure-ToolPackage -CommandName "ninja" -PackageName "ninja"

Add-StrawberryPerlToPath | Out-Null
Add-PathForCurrentProcess "C:\Program Files\NASM"
Add-UserPathIfMissing "C:\Program Files\NASM"

Ensure-MsvcBuildTools
Add-StrawberryPerlToPath | Out-Null

Print-Probe

$ready = (Test-PerlIsNativeWindows) `
    -and [bool](Get-CommandPath "nasm") `
    -and [bool](Get-CommandPath "cmake") `
    -and [bool](Get-CommandPath "ninja") `
    -and (Test-MsvcAvailable)

if (-not $ready) {
    $message = "WebAuthn build prerequisites are not ready. Install/fix the missing tools above and re-run this script."
    if ($CheckOnly) {
        Write-Warning $message
        exit 1
    }
    throw $message
}

if ($CleanNativeBuildCache) {
    Clear-NativeBuildCache
}

if ($FetchCargo) {
    if (-not (Get-CommandPath "cargo")) {
        throw "cargo is not on PATH. Install Rust with rustup before using -FetchCargo."
    }

    Push-Location $RepoRoot
    try {
        if (-not [string]::IsNullOrWhiteSpace($CargoTargetDir)) {
            $env:CARGO_TARGET_DIR = $CargoTargetDir
        }

        Write-Host ""
        Write-Host "Resolving Cargo dependencies for default features plus WebAuthn/OIDC..."
        Resolve-CargoFeatureSet -Features $RequiredFeatures

        Write-Host "Resolving Cargo dependencies for shipped binary feature set..."
        Resolve-CargoFeatureSet -Features $ReleaseBinaryFeatures -NoDefaultFeatures

        Write-Host "Fetching Cargo package sources..."
        cargo fetch --locked
        if ($LASTEXITCODE -ne 0) {
            throw "cargo fetch failed."
        }
    } finally {
        Pop-Location
    }
}

Write-Host ""
Write-Host "WebAuthn build prerequisites are ready."
Write-Host "Recommended verification:"
Write-Host "  cargo check --features oidc,webauthn"
Write-Host "Shipped binary feature verification:"
Write-Host "  cargo check --no-default-features --features `"$ReleaseBinaryFeatures`" --bin udb"
