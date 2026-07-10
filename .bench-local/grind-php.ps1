$ErrorActionPreference = "Stop"
# Clean PHP perf grind on the 50071/50081 lane (DB udb_bench_local) - SEPARATE from mars's
# 51071/udb_perf_grind (DO NOT TOUCH). Rebuild the udb-php-live image (Dockerfile COPYs source),
# stop ONLY the 50071-lane broker by port+path, fresh-bootstrap, launch, run the
# PHP perf bench in Docker -> host.docker.internal:50071/50081, print [PERF-FAIL] set.
$ROOT = "E:\Projects\udb"; $PGC = "udb-bench-pg"; $DB = "udb_bench_local"

. (Join-Path $PSScriptRoot "bench-process.ps1")

Write-Host "== rebuild udb-php-live image (harness changed) ==" -ForegroundColor Cyan
& docker build -q -f (Join-Path $ROOT "sdk\php\Dockerfile.live-test") -t udb-php-live (Join-Path $ROOT "sdk\php") | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Host "PHP IMAGE BUILD FAILED"; exit 3 }

Write-Host "== stop ONLY the 50071-lane broker (mars's 51071 untouched) ==" -ForegroundColor Cyan
Stop-UdbOnBenchPorts -Ports @(50071,50081,50091)
Start-Sleep -Milliseconds 600

Write-Host "== fresh bootstrap (udb_bench_local) ==" -ForegroundColor Cyan
$bs = & pwsh -NoProfile -File "$ROOT\.bench-local\fresh-bootstrap.ps1" 2>&1
$tid = ([regex]::Match((($bs | Out-String)), 'TENANT=([0-9a-fA-F\-]{36})')).Groups[1].Value
if (-not $tid) { Write-Host "BOOTSTRAP FAILED:"; $bs | Select-Object -Last 12; exit 1 }
Write-Host "tenant_id=$tid" -ForegroundColor Green

Write-Host "== launch broker (50071) ==" -ForegroundColor Cyan
$job = Start-Job -ScriptBlock { param($r) & pwsh -NoProfile -File "$r\.bench-local\launch-once.ps1" } -ArgumentList $ROOT
$log = "$ROOT\.bench-local\launch-once-broker.log"
$ready = $false
for ($i=0; $i -lt 50; $i++) {
  Start-Sleep -Seconds 3
  if ((Test-NetConnection 127.0.0.1 -Port 50071 -WarningAction SilentlyContinue).TcpTestSucceeded -and `
      (Test-NetConnection 127.0.0.1 -Port 50081 -WarningAction SilentlyContinue).TcpTestSucceeded) { $ready = $true; break }
}
if (-not $ready) { Write-Host "BROKER TIMEOUT"; exit 2 }
Start-Sleep -Seconds 4
Write-Host "broker ready" -ForegroundColor Green

Write-Host "== run PHP perf bench (docker -> host.docker.internal:50071/50081) ==" -ForegroundColor Cyan
$container = "udb-php-live-grind-" + ([guid]::NewGuid().ToString("N"))
$reportHost = Join-Path $ROOT "sdk\php\perf_report_php.md"
try {
  & docker create --name $container --add-host=host.docker.internal:host-gateway `
    -v "${ROOT}\sdk\php\tests:/sdk/tests:ro" `
    -v "${ROOT}\sdk\php\src:/sdk/src:ro" `
    -v "${ROOT}\sdk\php\gen:/sdk/gen:ro" `
    -v "${ROOT}\docs:/docs:ro" `
    -e UDB_LIVE_SDK_TESTS=1 -e UDB_LIVE_PERF=1 `
    -e UDB_GRPC_TARGET=host.docker.internal:50071 -e UDB_AUTH_GRPC_TARGET=host.docker.internal:50081 `
    -e UDB_LIVE_USERNAME=sdk-live-admin -e "UDB_LIVE_PASSWORD=SdkLive#2026Pass" `
    -e UDB_LIVE_TENANT=sdk-live -e UDB_LIVE_PROJECT=default `
    -e UDB_LIVE_REQUIRED_BACKENDS=postgres,mongodb,minio -e UDB_LIVE_S3_BUCKET=udb-live-sdk `
    udb-php-live php vendor/bin/pest tests/Live/GeneratedRpcSurfaceTest.php --filter "measures per-RPC latency" | Out-Null
  if ($LASTEXITCODE -ne 0) { Write-Host "PHP CONTAINER CREATE FAILED"; exit 4 }

  & docker start -a $container 2>&1 |
    Tee-Object -FilePath "$ROOT\.bench-local\php-grind.log" | Out-Null
  $testExit = [int](& docker inspect $container --format '{{.State.ExitCode}}')
  & docker cp "${container}:/sdk/perf_report_php.md" $reportHost 2>$null
  $copyExit = $LASTEXITCODE
  if ($copyExit -ne 0) {
    Write-Host "PHP PERF REPORT COPY FAILED: /sdk/perf_report_php.md was not produced" -ForegroundColor Red
  }
  if ($testExit -ne 0) { Write-Host "PHP PERF BENCH FAILED: exit=$testExit"; exit $testExit }
  if ($copyExit -ne 0) { exit 5 }
} finally {
  & docker rm -f $container 2>$null | Out-Null
}

$fails = Select-String -Path "$ROOT\.bench-local\php-grind.log" -Pattern 'FAILDETAIL'
Write-Host ("== PHP FAILURES: {0} ==" -f $fails.Count) -ForegroundColor Yellow
$fails | ForEach-Object { ($_.Line -replace '.*FAILDETAIL','FAILDETAIL') }
Stop-UdbOnBenchPorts -Ports @(50071,50081,50091)
if ($fails.Count -gt 0) { Write-Host "PHP PERF BENCH FAILED: perf failure rows=$($fails.Count)"; exit 6 }
