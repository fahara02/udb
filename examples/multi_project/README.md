# UDB Multi-Project Template

This template demonstrates the intended universal deployment shape: one broker
serving two unrelated projects with separate proto roots, catalog versions,
backend routing labels, and traffic fixtures.

Projects:

- `acme-billing`: commerce-style billing data with Postgres as canonical store
  plus Redis/Qdrant/S3 projection targets.
- `zen-clinic`: healthcare appointment data with Postgres as canonical store
  plus ClickHouse/Neo4j projection targets.

The checked-in files are intentionally small so they can be copied into a new
project and extended. A full local run uses the playground:

```bash
cd udb
./scripts/playground.sh up
PROFILE=multi-project-smoke ./scripts/load_test.sh
```

The fixture requests in `traffic/` can also be sent through `grpcurl` against a
running broker after the catalogs are staged.
