# UDB SDKs

UDB SDKs are small language clients for a running UDB broker. They are meant for
application developers, not only for people working inside this repository.

Use an SDK when your service needs to:

- call the UDB `DataBroker` API for reads, writes, objects, vectors, cache, and
  admin/catalog operations;
- call the native auth/authz services;
- attach the required tenant, user, purpose, scope, project, and catalog-version
  metadata on every request;
- use the version-matched `udb` CLI from inside your own project.

Current wire protocol: [`1.0.0`](UDB_PROTOCOL_VERSION)

Current SDK release: `0.3.1`

## Install

| Language | Package | Install |
|---|---|---|
| Go | `github.com/fahara02/udb/sdk/go` | `go get github.com/fahara02/udb/sdk/go@v0.3.1` |
| Python | `udb-client` | `pip install udb-client==0.3.1` |
| TypeScript / Node | `@udb_plus/sdk` | `npm i @udb_plus/sdk@0.3.1` |
| PHP / Laravel | `fahara02/udb-laravel` | `composer require fahara02/udb-laravel:^0.3.1` |
| C# | `Udb.Client` | `dotnet add package Udb.Client --version 0.3.1` |
| Java | `dev.udb:udb-java-client` | build with `mvn -f sdk/java/pom.xml test` until Maven Central publishing lands |

## The Shared Flow

1. Install the SDK for your language.
2. Use the bundled `udb` CLI launcher, or install the matching CLI binary.
3. Export UDB's annotation and broker protos into your app:

```bash
udb proto export
```

4. In your own `.proto` files, import the annotations:

```proto
import "udb/core/common/v1/db.proto";
```

5. Start UDB with your proto root:

```bash
udb serve proto "" 0.0.0.0:50051
```

6. Use the SDK to call `Select`, `Upsert`, authz checks, object APIs, vector
   search, and the rest of the broker surface.

`udb proto export` is safe to run again. It refreshes `proto/udb/**`, vendors the
small `google/api/**` imports needed for offline `protoc`, and can merge the
required `buf.yaml` entries without replacing your own project config.

## Metadata

Every UDB request carries context. The SDKs keep this boring on purpose: create
one metadata object near your request boundary and reuse it.

Required or commonly used fields:

- tenant id
- user id, when there is an end user
- purpose, such as `web.request` or `billing.worker`
- correlation id
- scopes
- service identity
- project id
- client catalog/protocol version

## Language Guides

- [Go](go/README.md)
- [Python](python/README.md)
- [TypeScript / Node](typescript/README.md)
- [PHP / Laravel](php/README.md)
- [C#](csharp/README.md)
- [Java](java/README.md)

You do not need to run `protoc`, `buf`, or SDK generation just to use a client.
Install the package for your language and start with that language guide.
