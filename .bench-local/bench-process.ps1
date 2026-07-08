# Shared local bench process cleanup helpers.

function Stop-UdbOnBenchPorts {
  param(
    [int[]]$Ports,
    [string]$Root = ""
  )

  if ([string]::IsNullOrWhiteSpace($Root)) {
    $Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
  }

  $owners = @()
  foreach ($port in $Ports) {
    $owners += Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
      Select-Object -ExpandProperty OwningProcess -Unique
  }

  foreach ($owner in ($owners | Sort-Object -Unique)) {
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId=$owner" -ErrorAction SilentlyContinue
    if ($null -eq $proc) { continue }
    if (($proc.Name -ieq "udb.exe") -and ($proc.ExecutablePath -like "$Root\*")) {
      Stop-Process -Id $owner -Force -ErrorAction SilentlyContinue
    }
  }
}

function Resolve-UdbBenchBin {
  param(
    [string]$Root = ""
  )

  if ([string]::IsNullOrWhiteSpace($Root)) {
    $Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
  }

  $candidate = if ($env:UDB_BENCH_BIN) {
    $env:UDB_BENCH_BIN
  } else {
    Join-Path $Root "target\debug\udb.exe"
  }
  $resolved = Resolve-Path -LiteralPath $candidate -ErrorAction SilentlyContinue
  if (-not $resolved) {
    throw "UDB bench binary not found: $candidate"
  }
  return $resolved.Path
}
