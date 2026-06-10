package dev.udb.client;

import com.udb.core.authz.services.v1.SignedPolicyBundle;

/**
 * Thrown by {@link UdbAuthClient#verifyPolicyBundle(SignedPolicyBundle, String)}
 * (and by {@link UdbAuthClient#getPolicyBundle()} when a bundle secret is set)
 * when the HMAC-SHA256 signature recomputed over the {@code bundle} bytes does
 * not match the server-supplied {@code signature}. Carries the offending bundle
 * so the caller can inspect its key id / policy version.
 */
public final class UdbPolicyBundleSignatureException extends RuntimeException {
  private final transient SignedPolicyBundle bundle;

  public UdbPolicyBundleSignatureException(String message, SignedPolicyBundle bundle) {
    super(message);
    this.bundle = bundle;
  }

  /** The bundle whose signature failed verification. */
  public SignedPolicyBundle bundle() {
    return bundle;
  }
}
