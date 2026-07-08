package dev.udb.client;

import java.util.Objects;

/** Shared, thread-safe pointer to the currently adopted project metadata. */
final class UdbMetadataRef {
  private volatile UdbMetadata current;

  UdbMetadataRef(UdbMetadata initial) {
    this.current = Objects.requireNonNull(initial, "initial");
  }

  UdbMetadata current() {
    return current;
  }

  void set(UdbMetadata metadata) {
    this.current = Objects.requireNonNull(metadata, "metadata");
  }
}
