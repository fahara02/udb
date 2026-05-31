# UDB Kubernetes Deployment Contracts

This directory contains the declarative API surface for running UDB on
Kubernetes. The CRDs are intentionally controller-neutral: they define the
desired state and status conditions that a future operator, Helm chart, or
GitOps controller can use.

Apply the CRDs:

```bash
kubectl apply -f crds/udb.io_crds.yaml
```

The contracts cover:

- `UdbBroker`: broker deployment, image, config, reload strategy, health gates.
- `UdbProjectCatalog`: project catalog source and activation status.
- `UdbBackendInstance`: named backend instance, role, credential reference, and
  connection budget.
- `UdbMigrationRun`: migration plan/apply lifecycle.
- `UdbCdcStream`: CDC stream/topic policy and offsets.
- `UdbProjectionWorker`: projection worker/reconciliation settings.

Every resource exposes `status.conditions[]` with `Ready`, `Progressing`, and
`Degraded`-style condition support so operators can mirror UDB health/admin API
state into Kubernetes-native status.
