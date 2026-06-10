using AuthzV1 = udb.core.Authz.Services.V1;

namespace Udb.Client;

/// <summary>
/// Thrown by <see cref="UdbAuthClient.RequireAsync"/> (and the facade's authz
/// helpers) when an access check is denied. Carries the full
/// <see cref="AuthzV1.Decision"/> so callers can inspect the deny reason,
/// matched policy ids, and required scopes. Parity with the Go/Python/TS
/// <c>UdbAuthzDenied</c> error types.
/// </summary>
public sealed class UdbAuthzDeniedException : Exception
{
    public UdbAuthzDeniedException(string resource, string action, AuthzV1.Decision decision)
        : base(BuildMessage(resource, action, decision))
    {
        Resource = resource;
        Action = action;
        Decision = decision;
    }

    /// <summary>The resource reference (rendered) that was denied.</summary>
    public string Resource { get; }

    /// <summary>The action that was denied.</summary>
    public string Action { get; }

    /// <summary>The full server decision (deny reason, matched policy ids, required scopes).</summary>
    public AuthzV1.Decision Decision { get; }

    /// <summary>The server-supplied deny reason, if any.</summary>
    public string DenyReason => Decision.DenyReason;

    private static string BuildMessage(string resource, string action, AuthzV1.Decision decision)
    {
        var reason = string.IsNullOrEmpty(decision.DenyReason) ? "denied" : decision.DenyReason;
        return $"udb: access denied for action '{action}' on '{resource}': {reason}";
    }
}
