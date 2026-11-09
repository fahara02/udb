# Toy backend plugin

The smallest crate that plugs a backend into UDB. It lives outside the broker's
source tree, implements the `udb::Backend` trait, states its contract, and passes
the same conformance check the built-in backends pass.

Use it as the starting point for your own backend crate.

## Step by step

1. **Have Rust 1.88 or newer.** `rustc --version` should print 1.88 or later.

2. **Run the conformance test** from this folder:

   ```bash
   cd examples/toy_backend_plugin
   cargo test
   ```

   The first build compiles UDB itself with only the `postgres` feature, so it
   takes a while. Expected result:

   ```text
   test tests::toy_plugin_satisfies_udb_contract ... ok
   ```

3. **Read `src/lib.rs`.** It has three parts:
   - `kind()` names the backend family the plugin serves.
   - `contract()` starts from `BackendPluginContract::default_for(kind)` and
     replaces the surfaces your crate owns. The toy replaces only the executor.
   - The test calls `conformance_report()` and asserts it passed. A contract with
     a required surface left empty fails here, before anything runs.

4. **Make it yours.**
   - Rename the crate in `Cargo.toml` and the type in `src/lib.rs`.
   - Point the `udb` dependency at a released version instead of `../..`.
   - Fill in each surface your backend provides: config schema, connection
     factory, IR compiler, executor, resource applier, health probe, metrics
     labels and capability matrix.
   - Keep the conformance test. It is how you know the broker will accept the
     plugin.

## Notes

- The crate is `publish = false`. It is an example, not a package.
