<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel;

/**
 * A delegated job token (`AuthnService.ExchangeTokenForJob`): it acts for
 * `subject` (the user) as `actor` (the service's identity). See
 * {@see UdbAuthClient::exchangeTokenForJob()}.
 */
final class DelegatedJobToken
{
    /**
     * @param string       $accessToken   the token the job sends as its bearer
     * @param string       $jti           the token's id; revoking it stops the token
     * @param int          $expiresAtUnix when the token expires, in Unix seconds
     * @param string       $subject       the user the token acts for
     * @param string       $actor         the acting service's identity (the RFC 8693 `act` claim)
     * @param string       $purpose       why the job acts for the user, as signed into the token
     * @param string       $jobId         the job the token is for
     * @param list<string> $scopes        the scopes both the user's token and the service's grant allow
     */
    public function __construct(
        public readonly string $accessToken,
        public readonly string $jti,
        public readonly int $expiresAtUnix,
        public readonly string $subject,
        public readonly string $actor,
        public readonly string $purpose,
        public readonly string $jobId,
        public readonly array $scopes,
        public readonly string $tenantId,
        public readonly string $projectId,
    ) {
    }

    /** Never print the token itself. */
    public function __debugInfo(): array
    {
        return [
            'accessToken' => '<redacted>',
            'jti' => $this->jti,
            'expiresAtUnix' => $this->expiresAtUnix,
            'subject' => $this->subject,
            'actor' => $this->actor,
            'purpose' => $this->purpose,
            'jobId' => $this->jobId,
            'scopes' => $this->scopes,
            'tenantId' => $this->tenantId,
            'projectId' => $this->projectId,
        ];
    }
}
