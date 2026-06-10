package dev.udb.client;

/**
 * Top-level entry point for the UDB Java SDK. {@code Udb.project(config)} opens a
 * shared-identity {@link UdbProject} facade over the data- and control-plane
 * services.
 */
public final class Udb {
  private Udb() {}

  /** Open a project facade from a {@link UdbProjectConfig}. */
  public static UdbProject project(UdbProjectConfig config) {
    return UdbProject.open(config);
  }
}
