using System.Collections.Concurrent;
using AuthzV1 = udb.core.Authz.Services.V1;

namespace Udb.Client;

/// <summary>
/// Thread-safe TTL cache of authorization decisions, parity with the
/// Go/Python/TypeScript SDK authz caches. The cache key folds the principal,
/// resource, action, purpose and requested scopes so that decisions are never
/// shared across identities or scope sets.
///
/// TTL is server-controlled: when a <see cref="AuthzV1.Decision"/> carries a
/// non-zero <c>CacheTtlSeconds</c> that value is honored. A client-side
/// fallback TTL is opt-in via the constructor and is only used when the server
/// TTL is zero.
/// </summary>
public sealed class AuthzCache
{
    private readonly ConcurrentDictionary<string, Entry> _entries = new();
    private readonly TimeSpan _defaultTtl;

    private readonly record struct Entry(AuthzV1.Decision Decision, DateTimeOffset ExpiresAt);

    /// <param name="defaultTtl">
    /// Fallback TTL applied when the server returns zero
    /// <c>cache_ttl_seconds</c>. <see cref="TimeSpan.Zero"/> leaves zero-TTL
    /// decisions uncached.
    /// </param>
    public AuthzCache(TimeSpan? defaultTtl = null)
    {
        _defaultTtl = defaultTtl ?? TimeSpan.Zero;
    }

    /// <summary>Whether a client-side fallback TTL is active.</summary>
    public bool Enabled => _defaultTtl > TimeSpan.Zero;

    /// <summary>Builds the cache key. Public so the facade can share one key scheme.</summary>
    public static string Key(string principal, string resource, string action, string purpose, IEnumerable<string> scopes)
    {
        var scopeKey = string.Join(",", scopes is null ? Array.Empty<string>() : scopes.OrderBy(s => s, StringComparer.Ordinal));
        return $"{principal}\u001f{resource}\u001f{action}\u001f{purpose}\u001f{scopeKey}";
    }

    /// <summary>Returns a non-expired decision for <paramref name="key"/>, or <c>null</c>.</summary>
    public AuthzV1.Decision? TryGet(string key)
    {
        if (_entries.TryGetValue(key, out var entry))
        {
            if (entry.ExpiresAt > DateTimeOffset.UtcNow)
            {
                return entry.Decision;
            }
            // Expired — best-effort eviction.
            _entries.TryRemove(key, out _);
        }
        return null;
    }

    /// <summary>
    /// Stores <paramref name="decision"/> under <paramref name="key"/>. The TTL
    /// is the server's positive <c>cache_ttl_seconds</c>, else the explicit
    /// fallback default. Nothing is stored when the effective TTL is zero.
    /// </summary>
    public void Set(string key, AuthzV1.Decision decision)
    {
        var ttl = decision.CacheTtlSeconds > 0
            ? TimeSpan.FromSeconds(decision.CacheTtlSeconds)
            : _defaultTtl;
        if (ttl <= TimeSpan.Zero)
        {
            return;
        }
        _entries[key] = new Entry(decision, DateTimeOffset.UtcNow.Add(ttl));
    }

    /// <summary>Removes all cached decisions.</summary>
    public void Clear() => _entries.Clear();

    /// <summary>Current number of cached entries (including any not yet evicted but expired).</summary>
    public int Count => _entries.Count;
}
