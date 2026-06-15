$ErrorActionPreference = "Stop"
# Nuke udb_bench_local (PG + Mongo), bootstrap the sdk-live-admin user, print TENANT=<uuid>.
$ROOT = "E:\Projects\udb"; $PGC = "udb-bench-pg"; $DB = "udb_bench_local"
docker exec $PGC psql -U udb -d postgres -c "DROP DATABASE IF EXISTS $DB WITH (FORCE);" | Out-Null
docker exec $PGC psql -U udb -d postgres -c "CREATE DATABASE $DB;" | Out-Null
try { docker exec udb-mongodb-1 mongosh --quiet --eval "db.getSiblingDB('$DB').dropDatabase()" | Out-Null } catch {}
$env:UDB_PG_DSN = "postgres://udb:udb@127.0.0.1:55460/$DB"
$env:UDB_PASSWORD_HASH_SECRET = "ci-bench-password-hash-secret"
$env:UDB_SESSION_HASH_SECRET  = "ci-bench-session-hash-secret"
$env:UDB_SESSION_ENABLED = "true"
$env:UDB_JWT_PRIVATE_KEY = "src/runtime/testdata/jwt_rs256_private.pem"
$env:UDB_JWT_PUBLIC_KEY  = "src/runtime/testdata/jwt_rs256_public.pem"
$env:UDB_ENCRYPTION_KEY  = "QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI="
Set-Location $ROOT
$bs = & "$ROOT\target-verify\debug\udb.exe" auth bootstrap user --username sdk-live-admin --password "SdkLive#2026Pass" --tenant sdk-live --project default 2>&1
$m = [regex]::Match((($bs | Out-String)), '"tenant_id":\s*"([0-9a-fA-F\-]{36})"')
$tid = if ($m.Success) { $m.Groups[1].Value } else { (docker exec $PGC psql -U udb -d $DB -tAc "SELECT tenant_id::text FROM udb_tenant.tenants WHERE code='sdk-live' OR name='sdk-live' LIMIT 1;" | Out-String).Trim() }
if (-not $tid) { Write-Host "BOOTSTRAP_FAILED"; $bs | Select-Object -Last 12; exit 1 }
Write-Host "TENANT=$tid"
