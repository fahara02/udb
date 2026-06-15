$ErrorActionPreference = "Stop"
Set-Location "E:\Projects\udb"
$seen = New-Object System.Collections.Generic.HashSet[string]
foreach ($raw in Get-Content -LiteralPath "E:\Projects\udb\.env.local") {
    $line = $raw.Trim()
    if ($line.Length -eq 0 -or $line.StartsWith("#")) { continue }
    $eq = $line.IndexOf("="); if ($eq -lt 1) { continue }
    $key = $line.Substring(0, $eq).Trim()
    if (-not $seen.Add($key)) { continue }
    $val = $line.Substring($eq + 1).Trim()
    if (($val.StartsWith('"') -and $val.EndsWith('"')) -or ($val.StartsWith("'") -and $val.EndsWith("'"))) { $val = $val.Substring(1, $val.Length - 2) }
    [Environment]::SetEnvironmentVariable($key, $val, "Process")
}
$env:UDB_GRPC_ADDR = "0.0.0.0:50051"
$env:UDB_AUTH_GRPC_ADDR = "0.0.0.0:50061"
& "E:\Projects\udb\.bench-bin\udb-full.exe" serve proto "" 0.0.0.0:50051 *>&1 | Tee-Object -FilePath "E:\Projects\udb\.bench-local\live-broker.log"
