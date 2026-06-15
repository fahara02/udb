$ErrorActionPreference = "Stop"
# Python parity grind: identical broker/DB/bootstrap/saga-dlq setup to grind-once.ps1,
# but runs the PYTHON perf bench (test_live_perf) instead of the Go one, so the Python
# SDK is measured against the SAME broker + fresh DB and its [PERF-FAIL] set can be
# compared 1:1 with Go.
$ROOT = "E:\Projects\udb"
$PGC  = "udb-bench-pg"; $DB = "udb_perf_grind"

function Stop-UdbOnBenchPorts {
  param([int[]]$Ports)
  $owners = @()
  foreach ($port in $Ports) {
    $owners += Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
      Select-Object -ExpandProperty OwningProcess -Unique
  }
  foreach ($owner in ($owners | Sort-Object -Unique)) {
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId=$owner" -ErrorAction SilentlyContinue
    if ($null -eq $proc) { continue }
    if (($proc.Name -ieq "udb.exe") -and ($proc.ExecutablePath -like "$ROOT\*")) {
      Stop-Process -Id $owner -Force -ErrorAction SilentlyContinue
    }
  }
}

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
$bs = & "$ROOT\target-verify\debug\udb.exe" auth bootstrap user --username sdk-live-admin --password "SdkLive#2026Pass" --tenant sdk-live --project default 2>&1
$m = [regex]::Match((($bs | Out-String)), '"tenant_id":\s*"([0-9a-fA-F\-]{36})"')
$tid = if ($m.Success) { $m.Groups[1].Value } else { "" }
if (-not $tid) {
  $tid = (docker exec $PGC psql -U udb -d $DB -tAc "SELECT tenant_id::text FROM udb_tenant.tenants WHERE code='sdk-live' OR name='sdk-live' LIMIT 1;" 2>$null | Out-String).Trim()
}
if (-not $tid) { Write-Host "BOOTSTRAP FAILED (no tenant_id):"; $bs | Select-Object -Last 15; exit 1 }
Write-Host "tenant_id=$tid" -ForegroundColor Green

$sql = @"
INSERT INTO udb_system.udb_sagas (saga_id,tx_id,tenant_id,correlation_id,status,backend_instance,operation,current_step,retry_count,recovery_attempts,compensation_status,steps,compensations,last_error,created_at,updated_at) VALUES
 ('11111111-1111-4111-8111-111111111101','t1','$tid','c1','manual_review','postgres:primary','mutate',0,0,0,'manual_review','[]'::jsonb,'[]'::jsonb,'seed',NOW(),NOW()),
 ('11111111-1111-4111-8111-111111111102','t2','$tid','c2','manual_review','postgres:primary','mutate',0,0,0,'manual_review','[]'::jsonb,'[]'::jsonb,'seed',NOW(),NOW()),
 ('11111111-1111-4111-8111-111111111103','t3','$tid','c3','manual_review','postgres:primary','mutate',0,0,0,'manual_review','[]'::jsonb,'[]'::jsonb,'seed',NOW(),NOW())
ON CONFLICT (saga_id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id,status='manual_review',compensation_status='manual_review',updated_at=NOW();
INSERT INTO udb_system.udb_cdc_dlq_events (dlq_id,event_id,topic,tenant_id,project_id,payload,error_type,error_message,retry_count,status,created_at,updated_at) VALUES
 ('22222222-2222-4222-8222-222222222201',gen_random_uuid(),'udb.sdk.live.records.cdc','$tid','default','{}'::jsonb,'seed','err',0,'OPEN',NOW(),NOW()),
 ('22222222-2222-4222-8222-222222222202',gen_random_uuid(),'udb.sdk.live.records.cdc','$tid','default','{}'::jsonb,'seed','err',0,'OPEN',NOW(),NOW()),
 ('22222222-2222-4222-8222-222222222203',gen_random_uuid(),'udb.sdk.live.records.cdc','$tid','default','{}'::jsonb,'seed','err',0,'OPEN',NOW(),NOW()),
 ('22222222-2222-4222-8222-222222222204',gen_random_uuid(),'udb.sdk.live.records.cdc','$tid','default','{}'::jsonb,'seed','err',0,'OPEN',NOW(),NOW())
ON CONFLICT (dlq_id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id,status='OPEN',updated_at=NOW();
"@

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

Write-Host "== seed saga/dlq ==" -ForegroundColor Cyan
$sql | docker exec -i $PGC psql -U udb -d $DB | Out-Null

Write-Host "== run Python test_live_perf ==" -ForegroundColor Cyan
Set-Location "$ROOT\sdk\python"
$env:UDB_LIVE_SDK_TESTS="1"; $env:UDB_LIVE_PERF="1"
$env:UDB_GRPC_TARGET="127.0.0.1:51071"; $env:UDB_AUTH_GRPC_TARGET="127.0.0.1:51081"
$env:UDB_LIVE_USERNAME="sdk-live-admin"; $env:UDB_LIVE_PASSWORD="SdkLive#2026Pass"
$env:UDB_LIVE_TENANT="sdk-live"; $env:UDB_LIVE_PROJECT="default"
$env:UDB_LIVE_REQUIRED_BACKENDS="postgres,mongodb,minio"; $env:UDB_LIVE_S3_BUCKET="udb-live-sdk"
& uv run pytest tests/test_live_conformance.py -k "test_live_perf" -v -s 2>&1 | Tee-Object -FilePath "$ROOT\.bench-local\grind-python-run.log" | Out-Null

$fails = Select-String -Path "$ROOT\.bench-local\grind-python-run.log" -Pattern '\[PERF-FAIL\]'
Write-Host ("== PYTHON FAILURES: {0} ==" -f $fails.Count) -ForegroundColor Yellow
$fails | ForEach-Object { ($_.Line -replace '.*\[PERF-FAIL\] ','') }
