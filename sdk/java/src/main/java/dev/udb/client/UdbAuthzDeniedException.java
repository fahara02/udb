package dev.udb.client;

import com.udb.core.authz.services.v1.Decision;
import com.udb.core.authz.services.v1.ResourceRef;

/**
 * Thrown by {@link UdbAuthClient#require} (and {@link AuthzCache#require}) when an
 * authorization decision denies access. Carries the full {@link Decision} so the
 * caller can inspect the deny reason and matched policies.
 */
public final class UdbAuthzDeniedException extends RuntimeException {
  private final transient Decision decision;

  public UdbAuthzDeniedException(ResourceRef resource, String action, Decision decision) {
    super("udb: authorization denied for " + resourceLabel(resource) + ":" + action
        + (decision.getDenyReason().isEmpty() ? "" : " — " + decision.getDenyReason()));
    this.decision = decision;
  }

  /** The denying decision (deny reason, matched policies, effect). */
  public Decision decision() {
    return decision;
  }

  private static String resourceLabel(ResourceRef resource) {
    if (resource == null) {
      return "<resource>";
    }
    if (!resource.getMessageType().isEmpty()) {
      return resource.getMessageType();
    }
    if (!resource.getResourceName().isEmpty()) {
      return resource.getResourceName();
    }
    if (!resource.getTable().isEmpty()) {
      return resource.getTable();
    }
    return "<resource>";
  }
}
