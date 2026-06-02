# UDB TypeScript SDK

Package: `@udb_plus/sdk`

Current release: `0.3.0`

Runtime: Node 18+

The TypeScript SDK is a Node gRPC wrapper for the UDB DataBroker plus native
Authn/Authz control-plane helpers. It loads bundled `.proto` files at runtime
through `@grpc/proto-loader`, so application code should import the package
entry points instead of `gen/` files.

## Install

```bash
npm i @udb_plus/sdk@0.3.0
```

`@grpc/grpc-js` and `@grpc/proto-loader` are package dependencies.

## Usage

```ts
import { dataBrokerClient, metadata, UdbMetadata } from "@udb_plus/sdk/client";
import { UdbAuthClient } from "@udb_plus/sdk/auth";

const meta: UdbMetadata = {
  tenantId: "acme",
  userId: "user-1",
  purpose: "web.request",
  scopes: ["udb:read", "udb:write"],
  serviceIdentity: "billing.api",
  projectId: "default",
};

const broker = dataBrokerClient("localhost:50051");
broker.Select(
  { message_type: "acme.billing.v1.Invoice", limit: 50 },
  metadata(meta),
  (err: unknown, rs: any) => {
    if (err) throw err;
    console.log(rs?.records);
  },
);

const auth = new UdbAuthClient("localhost:50051", meta);
const [allowed, decision] = await auth.can(
  { message_type: "acme.billing.v1.Invoice" },
  "read",
);
```

## Generated Files

The committed `gen/` directory is a drift-parity artifact kept in sync with the
repo protos. It is not the package runtime surface. Published consumers should
use:

- `@udb_plus/sdk`
- `@udb_plus/sdk/client`
- `@udb_plus/sdk/auth`

Regenerate from the repo root after proto changes:

```bash
./scripts/gen_sdk.sh
```

```powershell
.\scripts\gen_sdk.ps1
```
