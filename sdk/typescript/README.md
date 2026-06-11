# UDB TypeScript SDK

<!-- UDB_BRAND_HEADER_START -->
<p align="center">
  <img src="../../docs/assets/udb_logo.svg" alt="UDB logo" width="96">
</p>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.3.3 | protocol v1.0.0</sub>
</p>
<!-- UDB_BRAND_HEADER_END -->

`@udb_plus/sdk` is the Node.js client for UDB. Use it when a TypeScript service
needs to read or write through the broker, call UDB auth/authz, or run the
version-matched `udb` CLI from a project that installed the package.

## Install

```bash
npm i @udb_plus/sdk@0.3.3
```

Runtime: Node 18+

Main entry points:

- `@udb_plus/sdk/client` for the DataBroker client and metadata helper
- `@udb_plus/sdk/auth` for auth/authz convenience methods
- `@udb_plus/sdk` for the full public surface

## Export UDB Protos For Your App

If your app owns `.proto` schemas and wants to use UDB annotations, export the
shared UDB protos into your project:

```bash
npx udb proto export --fmt
```

Then your app protos can import:

```proto
import "udb/core/common/v1/db.proto";
```

`proto export` is safe to re-run. It refreshes `proto/udb/**`, vendors the
`google/api/**` protos needed for offline generation, and can merge `buf.yaml`
without replacing your own settings.

Run `npx udb proto fmt` any time after export or edits to keep long UDB field
annotations on one line for easier review.

## Connect And Query

```ts
import { dataBrokerClient, metadata, UdbMetadata } from "@udb_plus/sdk/client";
import { UdbAuthClient } from "@udb_plus/sdk/auth";

const meta: UdbMetadata = {
  tenantId: "acme",
  userId: "user-1",
  purpose: "web.request",
  scopes: ["udb:read", "udb:write"],
  serviceIdentity: "billing.api",
  projectId: "billing",
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

## Notes For Users

The package bundles the UDB wire protos and loads them at runtime through
`@grpc/proto-loader`. Application code should import the package entry points
above, not files under `gen/`.

The package also exposes a `udb` bin. With a local install, use `npx udb ...`;
with a global install, use `udb ...`.
