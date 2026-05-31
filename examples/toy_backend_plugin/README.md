# Toy Backend Plugin

This tiny crate demonstrates the stable UDB backend plugin contract from U10.
It lives outside `src/runtime/core`, implements `udb::Backend`, advertises a
contract, and can run the same conformance check as built-in plugins.

Real backend crates should replace the toy executor name with their own config
schema, connection factory, IR compiler, executor, resource applier, health
probe, metrics labels, and capability matrix.
