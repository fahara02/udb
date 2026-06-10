package dev.udb.client;

import com.udb.core.authz.services.v1.Decision;
import com.udb.core.authz.services.v1.ResourceRef;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A concurrency-safe, in-process TTL cache over {@link UdbAuthClient}, mirroring
 * the Go/Python/TS SDKs' {@code AuthzCache}. It keys cached decisions by the
 * (tenant, project, subject, resource, action, purpose) tuple and reuses an allow
 * or deny decision until it expires.
 *
 * <p>The TTL is <b>server-controlled</b>: it comes from {@link
 * Decision#getCacheTtlSeconds()}, so UDB stays in charge of how long a decision
 * may be reused. A zero server TTL is never cached, unless a non-zero {@code
 * defaultTtlSeconds} fallback is configured on this cache. This cuts the
 * per-request round-trip for hot checks without weakening freshness control.
 */
public final class AuthzCache {
  private static final char SEP = '\u001f';

  private final UdbAuthClient client;
  private final long defaultTtlSeconds;
  private final ConcurrentHashMap<String, Entry> cache = new ConcurrentHashMap<>();

  /** Cache that only caches decisions carrying a server-provided TTL. */
  public AuthzCache(UdbAuthClient client) {
    this(client, 0L);
  }

  /**
   * Cache with a fallback TTL (seconds) applied when the server returns a zero
   * {@code cache_ttl_seconds}. Pass 0 to never cache zero-TTL decisions.
   */
  public AuthzCache(UdbAuthClient client, long defaultTtlSeconds) {
    this.client = client;
    this.defaultTtlSeconds = Math.max(0L, defaultTtlSeconds);
  }

  /**
   * Return the {@link Decision} for the bound principal acting on a resource,
   * served from cache when a fresh entry exists, otherwise fetched and cached for
   * the server-provided TTL.
   */
  public Decision can(ResourceRef resource, String action, String purpose) {
    String resolvedPurpose = purpose == null || purpose.isEmpty() ? client.metadata().purpose() : purpose;
    String key = key(resource, action, resolvedPurpose);

    Entry entry = cache.get(key);
    long now = System.nanoTime();
    if (entry != null && now < entry.expiresNanos) {
      return entry.decision;
    }

    Decision decision = client.can(resource, action, resolvedPurpose);
    long ttl = decision.getCacheTtlSeconds();
    if (ttl == 0L) {
      ttl = defaultTtlSeconds;
    }
    if (ttl > 0L) {
      cache.put(key, new Entry(decision, now + ttl * 1_000_000_000L));
    }
    return decision;
  }

  /** Cache-aware boolean check. */
  public boolean allowed(ResourceRef resource, String action, String purpose) {
    return can(resource, action, purpose).getAllowed();
  }

  /**
   * Cache-aware authorize that throws {@link UdbAuthzDeniedException} on deny and
   * returns the allow decision otherwise.
   */
  public Decision require(ResourceRef resource, String action, String purpose) {
    Decision decision = can(resource, action, purpose);
    if (!decision.getAllowed()) {
      throw new UdbAuthzDeniedException(resource, action, decision);
    }
    return decision;
  }

  /** Drop all cached decisions (e.g. after a known policy change). */
  public void invalidate() {
    cache.clear();
  }

  private String key(ResourceRef resource, String action, String purpose) {
    UdbMetadata meta = client.metadata();
    String subject = meta.userId() == null || meta.userId().isEmpty()
        ? meta.serviceIdentity()
        : meta.userId();
    String res = "";
    if (resource != null) {
      res = resource.getMessageType();
      if (res.isEmpty()) {
        res = resource.getResourceName();
      }
      if (res.isEmpty()) {
        res = resource.getTable();
      }
    }
    return new StringBuilder()
        .append(meta.tenantId()).append(SEP)
        .append(meta.projectId()).append(SEP)
        .append(subject).append(SEP)
        .append(res).append(SEP)
        .append(action).append(SEP)
        .append(purpose)
        .toString();
  }

  private static final class Entry {
    final Decision decision;
    final long expiresNanos;

    Entry(Decision decision, long expiresNanos) {
      this.decision = decision;
      this.expiresNanos = expiresNanos;
    }
  }
}
