$ErrorActionPreference = "Stop"
# Python parity grind: identical broker/DB/bootstrap/saga-dlq setup to grind-once.ps1,
# but runs the PYTHON perf bench (test_live_perf) instead of the Go one, so the Python
# SDK is measured against the SAME broker + fresh DB and its [PERF-FAIL] set can be
# compared 1:1 with Go.
$ROOT = "E:\Projects\udb"
$PGC  = "udb-bench-pg"; $DB = "udb_perf_grind"

. (Join-Path $PSScriptRoot "bench-process.ps1")
$UdbBenchBin = Resolve-UdbBenchBin -Root $ROOT

Write-Host "== stop broker on grind ports ==" -ForegroundColor Cyan
Stop-UdbOnBenchPorts -Ports @(51071,51081,51091,19094)
Start-Sleep -Milliseconds 600

Write-Host "== fresh DB ($DB) ==" -ForegroundColor Cyan
docker exec $PGC psql -U udb -d postgres -c "DROP DATABASE IF EXISTS $DB WITH (FORCE);" | Out-Null
docker exec $PGC psql -U udb -d postgres -c "CREATE DATABASE $DB;" | Out-Null
docker exec udb-mongodb-1 mongosh --quiet --eval "db.getSiblingDB('$DB').dropDatabase()" | Out-Null

Write-Host "== bootstrap admin (capture tenant UUID) ==" -ForegroundColor Cyan
$env:UDB_PG_DSN="postgres://udb:udb@127.0.0.1:55460/$DB"
$env:UDB_PASSWORD_HASH_SECRET="ci-bench-password-hash-secret"; $env:UDB_SESSION_HASH_SECRET="ci-bench-session-hash-secret"
$env:UDB_SESSION_ENABLED="true"; $env:UDB_JWT_PRIVATE_KEY="src/runtime/testdata/jwt_rs256_private.pem"; $env:UDB_JWT_PUBLIC_KEY="src/runtime/testdata/jwt_rs256_public.pem"
$env:UDB_ENCRYPTION_KEY="QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI="
Set-Location $ROOT
$bs = & $UdbBenchBin auth bootstrap user --username sdk-live-admin --password "SdkLive#2026Pass" --tenant sdk-live --project default 2>&1
$m = [regex]::Match((($bs | Out-String)), '"tenant_id":\s*"([0-9a-fA-F\-]{36})"')
$tid = if ($m.Success) { $m.Groups[1].Value } else { "" }
if (-not $tid) {
  $tid = (docker exec $PGC psql -U udb -d $DB -tAc "SELECT tenant_id::text FROM udb_tenant.tenants WHERE code='sdk-live' OR name='sdk-live' LIMIT 1;" 2>$null | Out-String).Trim()
}
if (-not $tid) { Write-Host "BOOTSTRAP FAILED (no tenant_id):"; $bs | Select-Object -Last 15; exit 1 }
Write-Host "tenant_id=$tid" -ForegroundColor Green

Write-Host "== launch broker ==" -ForegroundColor Cyan
$brokerJob = Start-Process pwsh -PassThru -WindowStyle Hidden -ArgumentList @("-NoProfile","-File","$ROOT\.bench-local\launch-verify.ps1")
$log = "$ROOT\.bench-local\verify-broker.log"
$ready=$false
for ($i=0;$i -lt 50;$i++){ Start-Sleep -Seconds 3
  if ((Test-Path $log) -and (Select-String -Path $log -Pattern 'UDB DataBroker is ready' -Quiet) -and (Test-NetConnection 127.0.0.1 -Port 51091 -WarningAction SilentlyContinue).TcpTestSucceeded){$ready=$true;break}
  if ((Test-Path $log) -and (Select-String -Path $log -Pattern 'broker EXITED' -Quiet)){Write-Host "BROKER EXITED"; Get-Content $log -Tail 6; exit 2}
}
if (-not $ready){Write-Host "BROKER TIMEOUT"; Get-Content $log -Tail 6; exit 2}
Write-Host "broker ready" -ForegroundColor Green

Write-Host "== run Python test_live_perf ==" -ForegroundColor Cyan
Set-Location "$ROOT\sdk\python"
$env:UDB_LIVE_SDK_TESTS="1"; $env:UDB_LIVE_PERF="1"
$env:UDB_GRPC_TARGET="127.0.0.1:51071"; $env:UDB_AUTH_GRPC_TARGET="127.0.0.1:51081"
$env:UDB_LIVE_USERNAME="sdk-live-admin"; $env:UDB_LIVE_PASSWORD="SdkLive#2026Pass"
$env:UDB_LIVE_TENANT="sdk-live"; $env:UDB_LIVE_PROJECT="default"
$env:UDB_LIVE_REQUIRED_BACKENDS="postgres,mongodb,minio"; $env:UDB_LIVE_S3_BUCKET="udb-live-sdk"
& uv run pytest tests/test_live_conformance.py -k "test_live_perf" -v -s 2>&1 | Tee-Object -FilePath "$ROOT\.bench-local\grind-python-run.log" | Out-Null
$testExit = $LASTEXITCODE

$fails = Select-String -Path "$ROOT\.bench-local\grind-python-run.log" -Pattern '\[PERF-FAIL\]'
Write-Host ("== PYTHON FAILURES: {0} ==" -f $fails.Count) -ForegroundColor Yellow
$fails | ForEach-Object { ($_.Line -replace '.*\[PERF-FAIL\] ','') }
if ($testExit -ne 0) { Write-Host "PYTHON PERF BENCH FAILED: exit=$testExit"; exit $testExit }
if ($fails.Count -gt 0) { Write-Host "PYTHON PERF BENCH FAILED: perf failure rows=$($fails.Count)"; exit 6 }
