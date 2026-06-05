# TypeScript Gen Directory

This folder is not the public TypeScript SDK.

Use the package entry points instead:

```ts
import { dataBrokerClient } from "@udb_plus/sdk/client";
import { UdbAuthClient } from "@udb_plus/sdk/auth";
```

The published package loads bundled UDB protos at runtime. Application code
should not import files from this directory.

See [../README.md](../README.md) for the actual TypeScript SDK guide.
