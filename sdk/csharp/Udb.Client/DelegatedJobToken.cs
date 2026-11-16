namespace Udb.Client;

/// <summary>
/// A delegated job token (<c>AuthnService.ExchangeTokenForJob</c>): it acts for
/// <see cref="Subject"/> (the user) as <see cref="Actor"/> (the service's identity).
/// </summary>
/// <param name="AccessToken">The token the job sends as its bearer.</param>
/// <param name="Jti">The token's id; revoking it stops the token.</param>
/// <param name="ExpiresAtUnix">When the token expires, in Unix seconds.</param>
/// <param name="Subject">The user the token acts for.</param>
/// <param name="Actor">The acting service's identity (the RFC 8693 <c>act</c> claim).</param>
/// <param name="Purpose">Why the job acts for the user, as signed into the token.</param>
/// <param name="JobId">The job the token is for.</param>
/// <param name="Scopes">The scopes both the user's token and the service's grant allow.</param>
/// <param name="TenantId">The tenant the token is for.</param>
/// <param name="ProjectId">The project the token is for.</param>
public sealed record DelegatedJobToken(
    string AccessToken,
    string Jti,
    long ExpiresAtUnix,
    string Subject,
    string Actor,
    string Purpose,
    string JobId,
    IReadOnlyList<string> Scopes,
    string TenantId,
    string ProjectId);
