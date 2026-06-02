# UDB Go SDK — generation templates

These templates are consumed by the UDB Rust generator (`udb sdk generate`,
`src/cli/sdk_gen.rs`). Each file here is materialized at the mirror path under
`sdk/go/`. A `.tmpl` suffix means "render placeholders, then strip the suffix";
any other file is copied verbatim. `sdkgen.yaml`/`sdkgen.toml` and dotfiles are
skipped.

## What renders where

| Template (under `sdk-templates/go/`)        | Rendered output (under `sdk/go/`)            |
| ------------------------------------------- | -------------------------------------------- |
| `udbclient/generated_client.go.tmpl`        | `udbclient/generated_client.go`              |
| `cmd/udb/main.go.tmpl`                       | `cmd/udb/main.go`                            |
| `README.md`                                 | `README.md` *(copied verbatim — see note)*   |

> Note: the generator copies non-`.tmpl` files verbatim. If you do NOT want the
> hand-written `sdk/go/README.md` overwritten, rename this file (e.g.
> `GENERATED.md`) or drop it before shipping. It is documentation-only and is
> not imported by any Go code.

The generator substitutes scalars (`{{UDB_VERSION}}`, `{{PROTOCOL_VERSION}}`,
`{{LANG}}`, `{{RPC_COUNT}}`, `{{SERVICE_COUNT}}`, `{{GENERATED_NOTE}}`) and
expands per-RPC blocks (`// @@UDB_RPC_BEGIN` … `// @@UDB_RPC_END`) and per-service
blocks (`// @@UDB_SERVICE_BEGIN` … `// @@UDB_SERVICE_END`). The RPC/service set is
read from the embedded proto FileDescriptorSet, so nothing is hand-listed.

## Composition — does NOT clobber existing upgrades

The hand-written `udbclient` package already ships:

- `client.go` — typed `Client` over the `DataBroker` stub + `Metadata` + the 8
  UDB headers + `joinScopes`.
- `auth.go`, `auth_cache.go`, `auth_native.go` — `AuthClient` over Authn/Authz.
- `negotiation.go` — protocol-version / encoding negotiation + `ProtocolVersion`.

`generated_client.go` is a NEW file in the SAME package (`package udbclient`). It
adds the robustness layer and **reuses** the existing symbols (`Metadata`,
`joinScopes`, `ProtocolVersion`) rather than redefining them. None of the
hand-written files are templated or overwritten.

`generated_client.go` provides:

- `GeneratedClient` — wraps an existing `grpc.ClientConnInterface`, applies the 8
  UDB headers plus optional `authorization` / `x-api-key` / `x-request-id`,
  per-call deadline, retry with exponential backoff + jitter on transient codes
  (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`), and typed error
  mapping (`*Error`, decoding the `udb-error-detail-bin` trailer).
- `DialOptions()` — unary + stream interceptors so the typed hand-written
  wrappers (`Client`, `AuthClient`) transparently gain retry/metadata/error
  mapping when you build them on a connection dialed with these options.
- `InvokeUnary` / `NewServerStream` / `NewClientStream` — low-level, message-type
  agnostic escape hatches keyed on each RPC's full method path, for RPCs without
  a typed helper. Streaming RPCs are never retried.
- `AllRPCs`, `ServiceRPCCounts`, `LookupRPC` — generated metadata for every RPC
  across every service.

### Recommended usage

```go
gc := udbclient.NewGenerated(nil, udbclient.Options{
    Meta:        meta,
    CallTimeout: 5 * time.Second,
    Retry:       udbclient.DefaultRetryConfig(),
})

// Dial once with the interceptors; the typed wrappers inherit robustness.
conn, _ := grpc.NewClient(target, append(
    []grpc.DialOption{grpc.WithTransportCredentials(creds)},
    gc.DialOptions()...,
)...)

client := udbclient.New(conn, meta)              // typed DataBroker wrapper
auth := udbclient.NewAuthClient(conn, meta)      // typed Authn/Authz wrapper
```

`NewGenerated` accepts a connection for the `InvokeUnary`/stream escape hatches;
pass `nil` if you only use `DialOptions()` to configure the typed wrappers.

The `init()` in `generated_client.go` asserts `GeneratedProtocolVersion ==
ProtocolVersion`, catching drift between the generated wire version and the
hand-written `negotiation.go` constant.

## CLI bundling — `go install` gives you the matched `udb`

The Go module is **tag-driven**: there is no manifest file to add a `bin` entry
to (unlike npm/PyPI). The idiomatic equivalent is an installable `main` package:

```bash
go install github.com/fahara02/udb/sdk/go/cmd/udb@v{{UDB_VERSION}}
```

That installs a `udb` binary on `$GOBIN`. `cmd/udb/main.go` is a thin launcher
that resolves the **version-matched** real CLI (`v{{UDB_VERSION}}` is baked in at
generation time):

1. `$UDB_BIN` — if it points at an executable, exec it.
2. A previously cached download under `os.UserCacheDir()/udb/bin/v{{UDB_VERSION}}/`.
3. A `udb` already on `$PATH` whose `--version` matches (excluding the launcher).
4. Otherwise download the per-OS/arch release asset from
   `github.com/fahara02/udb` (tag `v{{UDB_VERSION}}`,
   `udb-v{{UDB_VERSION}}-<goos>-<goarch>.{tar.gz|zip}`) into the cache and exec it.

Set `UDB_NO_DOWNLOAD=1` to forbid the network fallback. All args, stdio, and the
exit code are forwarded. Because the version is pinned by the install tag, the
CLI and SDK stay locked together.

> No edit to `go.mod` is needed or made: an installable command is discovered by
> its package path, not a manifest entry. This matches the task's "minimal
> additive manifest wiring" requirement — for Go that wiring is the `cmd/udb`
> package path itself.

## Customizing

- Change retry policy: edit `DefaultRetryConfig()` or pass a custom `RetryConfig`
  in `Options.Retry`.
- Add headers: extend `outgoingContext` in the template.
- Add typed wrappers per RPC: add Go methods to the per-RPC block in
  `generated_client.go.tmpl` (use `kind=` filters to emit unary vs streaming
  shapes). Today the per-RPC block emits the `AllRPCs` descriptor table, which is
  sufficient because the typed surface is provided by the buf stubs + the
  hand-written `Client`/`AuthClient`.
