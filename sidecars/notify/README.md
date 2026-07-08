# UDB Notification Sidecar

Provider adapter for master-plan 9.13. The broker keeps durable queueing,
tenant checks, vault decrypt, SSRF validation, and delivery-attempt recording.
This sidecar only converts the broker's generic HTTP request into a provider
call.

Broker request contract:

```http
POST /send
Authorization: Bearer <decrypted provider credential>
Content-Type: application/json
```

```json
{"to":"recipient","subject":"subject","body":"message"}
```

The sidecar returns `200` and `x-provider-message-id` on a successful provider
handoff. It supports `UDB_NOTIFY_PROVIDER=smtp|ses|twilio|fcm`. Set
`UDB_NOTIFY_DRY_RUN=1` only for local smoke checks.

Local smoke:

```bash
python scripts/notify_sidecar_smoke.py
docker compose -f docker-compose.integration.yml --profile notify up --build -d notify-sidecar
python scripts/notify_sidecar_smoke.py --url http://127.0.0.1:58080
```

The smoke is sidecar-scoped by design. The broker delivery worker reuses the
WebhookService SSRF guard, so Docker-internal/private HTTP endpoints are not
valid broker delivery targets. Full broker-worker proof still requires deploying
the sidecar behind an allowed HTTPS endpoint and configuring
`UDB_NOTIFICATION_DELIVERY_PROVIDERS_JSON`.

Gate-D reconciliation harness:

```bash
python scripts/notify_sidecar_roundtrip_smoke.py --selftest
python scripts/notify_sidecar_roundtrip_smoke.py \
  --pg-dsn "$UDB_INTEGRATION_PG_DSN" \
  --sidecar-url https://notify-sidecar.example \
  --broker 127.0.0.1:50061 \
  --bearer-token "$UDB_BEARER_TOKEN"
```

The live command loads one queued PENDING notification intent, calls the sidecar
with the broker-format POST body, then reports the provider result to internal
`NotificationService.ReportDelivery` through `grpcurl` using the checked-in
proto/import paths by default. Use `--use-reflection` only against a listener
that actually exposes reflection. This is the runnable proof command; the item
remains partial until it is observed green against an SSRF-allowed HTTPS sidecar
endpoint and real broker state.

Example broker provider config before vault wrapping:

```json
[
  {
    "channel": "EMAIL",
    "provider": "SMTP",
    "endpoint_url": "https://notify-sidecar.example/send",
    "wrapped_credential": "udb-aead:..."
  }
]
```

Credential JSON shapes:

```json
{"host":"smtp.example","port":587,"username":"user","password":"secret","from":"noreply@example.com"}
{"region":"us-east-1","access_key_id":"AKIA...","secret_access_key":"secret","from":"noreply@example.com"}
{"account_sid":"AC...","auth_token":"secret","from":"+15551234567"}
{"project_id":"my-firebase-project","access_token":"ya29..."}
```

Do not run this sidecar with plaintext HTTP across an untrusted network; the
broker sends the decrypted provider credential as a bearer token for one
delivery call.
