# coturn for UDB WebRTC

A TURN/STUN relay for the native WebRTC service's NAT traversal. The broker
issues short-lived TURN credentials; coturn validates them against a **shared
secret** — no per-user accounts.

## Credential model (RFC 5766 REST / coturn `use-auth-secret`)

The broker's `TurnService.IssueCredentials` returns:

- `username  = "<expiry-unix-seconds>:<peer-or-tenant>"`
- `credential = base64(HMAC-SHA1(UDB_TURN_SECRET, username))`
- `ice_servers` (from `UDB_TURN_URLS`, defaulting to a public STUN server)

coturn, started with `--use-auth-secret --static-auth-secret=$UDB_TURN_SECRET`,
recomputes the same HMAC and accepts the credential until `expiry`. **The broker
and coturn must share the exact same `UDB_TURN_SECRET`.**

## Run

```powershell
$env:UDB_TURN_SECRET = "<a-strong-shared-secret>"      # 32+ random chars
docker compose -f docker/coturn/docker-compose.yml up -d
```

Then start the broker with the **same** secret and point clients at this relay:

```powershell
$env:UDB_TURN_SECRET = "<same-secret>"
$env:UDB_TURN_URLS    = "turn:127.0.0.1:3478,stun:127.0.0.1:3478"
# ...serve the broker...
```

Now `IssueCredentials` returns ICE servers your WebRTC clients can use directly.

## Verify

Use the WebTRC samples' Trickle-ICE page or `turnutils_uclient`:

```bash
# username/credential come from a TurnService.IssueCredentials response
turnutils_uclient -u "<username>" -w "<credential>" -y 127.0.0.1
```

## Production notes

- Use **host networking** (or a public IP via `--external-ip`) so coturn
  advertises a reachable relay address.
- Terminate TLS on `5349` with a real cert (`--cert` / `--pkey`).
- Widen `--min-port/--max-port` to a real relay range and open it in the firewall.
- Rotate `UDB_TURN_SECRET` (the broker reads it at startup; coturn at restart).
