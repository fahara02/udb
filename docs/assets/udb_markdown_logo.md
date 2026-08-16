# UDB Markdown And Docs Layout

This is the single canonical text layout for Markdown surfaces: README headers,
generated docs, release notes, package registries, and terminal screenshots.
Keep it plain text, emoji-free, and under 78 columns.

## Full Header

```text
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│    ██    ██  ██████   ██████                                               │
│    ██    ██  ██   ██  ██   ██                                              │
│    ██    ██  ██   ██  ██████                                               │
│    ██    ██  ██   ██  ██   ██                                              │
│     ██████   ██████   ██████                                               │
│                                                                            │
│    UNIVERSAL DATA BROKER                                                   │
│    gRPC data plane | native control plane | tenant/project scope guard     │
│                                                                            │
│    crate v0.5.16 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```

## Compact Header

```text
UDB :: Universal Data Broker
gRPC data plane | native control plane | tenant/project scope guard
```

## Badge Line

```text
UDB | crate v0.5.16 | protocol v1.0.0 | policy-aware data routing
```

## README Placement

Use the SVG mark above the text layout when the surface allows images:

```html
<p align="center">
  <img src="docs/assets/udb_logo.svg" alt="UDB logo" width="160">
</p>

<h1 align="center">Universal Data Broker</h1>
<p align="center">
  gRPC data plane | native control plane | tenant/project scope guard
</p>
```
