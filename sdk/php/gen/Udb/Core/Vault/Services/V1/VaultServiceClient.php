<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Vault\Services\V1;

/**
 * VaultService (master-plan 9.1, flagship) — secrets management built into the
 * broker. Three engines, one crypto stack (the broker AES-256-GCM-SIV envelope,
 * reused from `runtime::encryption`):
 *   * KV       — versioned, envelope-encrypted secrets at hierarchical paths
 *                with compare-and-swap, soft delete, and crypto-shred destroy.
 *   * Transit  — encrypt/decrypt/sign/verify/hmac by key NAME; key material is
 *                never exported; versioned keys with ACTIVE/VERIFYING rotation.
 *   * Seal     — every handler fails closed (failed_precondition) when the
 *                master key is unavailable; SealStatus reports the seal state.
 * The sensitive reads (GetSecret, Decrypt) are audited via the outbox compliance
 * envelope. Dynamic database credentials are a declared follow-up.
 */
class VaultServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * ── KV engine ─────────────────────────────────────────────────────────────
     *
     * Write a new secret version. Compare-and-swap: `expected_version` must equal
     * the current latest version (0 for a brand-new path) or the write is rejected.
     * @param \Udb\Core\Vault\Services\V1\PutSecretRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\PutSecretResponse>
     */
    public function PutSecret(\Udb\Core\Vault\Services\V1\PutSecretRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/PutSecret',
        $argument,
        ['\Udb\Core\Vault\Services\V1\PutSecretResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Read the secret value (latest active version, or a specific version). This
     * is the sensitive vault read: it is AUDITED via the outbox compliance envelope.
     * @param \Udb\Core\Vault\Services\V1\GetSecretRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\GetSecretResponse>
     */
    public function GetSecret(\Udb\Core\Vault\Services\V1\GetSecretRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/GetSecret',
        $argument,
        ['\Udb\Core\Vault\Services\V1\GetSecretResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List secret paths under an optional prefix. Returns metadata only — NEVER
     * any secret value.
     * @param \Udb\Core\Vault\Services\V1\ListSecretsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\ListSecretsResponse>
     */
    public function ListSecrets(\Udb\Core\Vault\Services\V1\ListSecretsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/ListSecrets',
        $argument,
        ['\Udb\Core\Vault\Services\V1\ListSecretsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Soft-delete the latest version (recoverable bookkeeping state). The ciphertext
     * is retained; use DestroySecret to crypto-shred.
     * @param \Udb\Core\Vault\Services\V1\DeleteSecretRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\DeleteSecretResponse>
     */
    public function DeleteSecret(\Udb\Core\Vault\Services\V1\DeleteSecretRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/DeleteSecret',
        $argument,
        ['\Udb\Core\Vault\Services\V1\DeleteSecretResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Restore a soft-DELETED secret: flip its latest deleted version back to ACTIVE.
     * A soft delete keeps the ciphertext + wrapped key, so recovery is exact. A
     * crypto-shredded (DestroySecret) version can NEVER be restored.
     * @param \Udb\Core\Vault\Services\V1\UndeleteSecretRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\UndeleteSecretResponse>
     */
    public function UndeleteSecret(\Udb\Core\Vault\Services\V1\UndeleteSecretRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/UndeleteSecret',
        $argument,
        ['\Udb\Core\Vault\Services\V1\UndeleteSecretResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Crypto-shred every version of a secret: clears the wrapped DEK + ciphertext
     * so the value is irrecoverable. DESTRUCTIVE + irreversible — a confirmation
     * token is required and an empty token fails closed.
     * @param \Udb\Core\Vault\Services\V1\DestroySecretRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\DestroySecretResponse>
     */
    public function DestroySecret(\Udb\Core\Vault\Services\V1\DestroySecretRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/DestroySecret',
        $argument,
        ['\Udb\Core\Vault\Services\V1\DestroySecretResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Transit engine ────────────────────────────────────────────────────────
     *
     * Create a named transit key (version 1, ACTIVE). Key material is generated
     * server-side and never returned.
     * @param \Udb\Core\Vault\Services\V1\CreateTransitKeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\CreateTransitKeyResponse>
     */
    public function CreateTransitKey(\Udb\Core\Vault\Services\V1\CreateTransitKeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/CreateTransitKey',
        $argument,
        ['\Udb\Core\Vault\Services\V1\CreateTransitKeyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Rotate a named transit key: the current ACTIVE version is demoted to
     * VERIFYING (still decrypts/verifies during the overlap) and a fresh ACTIVE
     * version is generated. New encryptions/signatures use the new version.
     * @param \Udb\Core\Vault\Services\V1\RotateTransitKeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\RotateTransitKeyResponse>
     */
    public function RotateTransitKey(\Udb\Core\Vault\Services\V1\RotateTransitKeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/RotateTransitKey',
        $argument,
        ['\Udb\Core\Vault\Services\V1\RotateTransitKeyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Encrypt plaintext under the ACTIVE version of a named key. Returns a
     * versioned ciphertext envelope; the key material is never returned.
     * @param \Udb\Core\Vault\Services\V1\EncryptRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\EncryptResponse>
     */
    public function Encrypt(\Udb\Core\Vault\Services\V1\EncryptRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/Encrypt',
        $argument,
        ['\Udb\Core\Vault\Services\V1\EncryptResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Decrypt a transit ciphertext envelope. The version is read from the envelope
     * and ACTIVE or VERIFYING versions are accepted. This is a sensitive read and is
     * AUDITED via the outbox compliance envelope.
     * @param \Udb\Core\Vault\Services\V1\DecryptRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\DecryptResponse>
     */
    public function Decrypt(\Udb\Core\Vault\Services\V1\DecryptRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/Decrypt',
        $argument,
        ['\Udb\Core\Vault\Services\V1\DecryptResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Produce a detached MAC ("signature") over the input under the ACTIVE key
     * version. Implemented as HMAC-SHA256 from the version DEK (symmetric);
     * asymmetric signing is a follow-up. Key material is never returned.
     * @param \Udb\Core\Vault\Services\V1\SignRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\SignResponse>
     */
    public function Sign(\Udb\Core\Vault\Services\V1\SignRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/Sign',
        $argument,
        ['\Udb\Core\Vault\Services\V1\SignResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Verify a MAC/signature over the input. The version is read from the
     * signature and ACTIVE or VERIFYING versions are accepted; comparison is
     * constant-time.
     * @param \Udb\Core\Vault\Services\V1\VerifyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\VerifyResponse>
     */
    public function Verify(\Udb\Core\Vault\Services\V1\VerifyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/Verify',
        $argument,
        ['\Udb\Core\Vault\Services\V1\VerifyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Compute an HMAC-SHA256 over the input under the ACTIVE key version. Key
     * material is never returned.
     * @param \Udb\Core\Vault\Services\V1\HmacRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\HmacResponse>
     */
    public function Hmac(\Udb\Core\Vault\Services\V1\HmacRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/Hmac',
        $argument,
        ['\Udb\Core\Vault\Services\V1\HmacResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Seal engine ───────────────────────────────────────────────────────────
     *
     * Report whether the vault is sealed (master key unavailable). Always answers,
     * even when sealed, so operators can diagnose a sealed vault.
     * @param \Udb\Core\Vault\Services\V1\SealStatusRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\SealStatusResponse>
     */
    public function SealStatus(\Udb\Core\Vault\Services\V1\SealStatusRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/SealStatus',
        $argument,
        ['\Udb\Core\Vault\Services\V1\SealStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Dynamic database credentials ──────────────────────────────────────────
     *
     * Mint short-lived, per-request Postgres credentials with a durable lease.
     * The requested role_name is an operator-configured alias resolved from
     * UDB_VAULT_DB_ROLES_JSON; arbitrary request-supplied role grants fail closed.
     * WORKER_VAULT_LEASE_REAPER revokes and drops expired generated login roles.
     * @param \Udb\Core\Vault\Services\V1\GenerateDatabaseCredentialsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\GenerateDatabaseCredentialsResponse>
     */
    public function GenerateDatabaseCredentials(\Udb\Core\Vault\Services\V1\GenerateDatabaseCredentialsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/GenerateDatabaseCredentials',
        $argument,
        ['\Udb\Core\Vault\Services\V1\GenerateDatabaseCredentialsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Generate a fresh 256-bit data key, returned BOTH plaintext (for the caller to
     * encrypt data locally) AND wrapped under the named transit key (store this and
     * Decrypt/Rewrap it later). Envelope-encryption without exposing the transit
     * key. Reuses the transit seal path; AUDITED via the outbox compliance envelope.
     * @param \Udb\Core\Vault\Services\V1\GenerateDataKeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\GenerateDataKeyResponse>
     */
    public function GenerateDataKey(\Udb\Core\Vault\Services\V1\GenerateDataKeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/GenerateDataKey',
        $argument,
        ['\Udb\Core\Vault\Services\V1\GenerateDataKeyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Re-wrap a transit ciphertext under the key's CURRENT active version: decrypt
     * with the version embedded in the envelope, then re-seal with the active
     * version. The post-rotation migration primitive (no plaintext leaves the
     * broker). AUDITED via the outbox compliance envelope.
     * @param \Udb\Core\Vault\Services\V1\RewrapRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\RewrapResponse>
     */
    public function Rewrap(\Udb\Core\Vault\Services\V1\RewrapRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/Rewrap',
        $argument,
        ['\Udb\Core\Vault\Services\V1\RewrapResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Export the Ed25519 PUBLIC key(s) of a signing transit key so an external
     * party can verify broker-produced signatures without ever holding the private
     * key — the missing half that makes Sign/Verify genuinely asymmetric. Only
     * valid for keys created with the ed25519 algorithm; READ-ONLY (public keys are
     * not secret). Returns one entry per usable (ACTIVE/VERIFYING) version.
     * @param \Udb\Core\Vault\Services\V1\GetTransitPublicKeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\GetTransitPublicKeyResponse>
     */
    public function GetTransitPublicKey(\Udb\Core\Vault\Services\V1\GetTransitPublicKeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/GetTransitPublicKey',
        $argument,
        ['\Udb\Core\Vault\Services\V1\GetTransitPublicKeyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Encrypt MANY plaintexts under one transit key in a single call: the key is
     * unwrapped ONCE and each plaintext sealed with the active version, amortizing
     * the master-key unwrap over the batch. Order-preserving. AUDITED.
     * @param \Udb\Core\Vault\Services\V1\BatchEncryptRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\BatchEncryptResponse>
     */
    public function BatchEncrypt(\Udb\Core\Vault\Services\V1\BatchEncryptRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/BatchEncrypt',
        $argument,
        ['\Udb\Core\Vault\Services\V1\BatchEncryptResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Decrypt MANY transit ciphertexts under one key in a single call; each
     * ciphertext carries its own key version in the envelope. Order-preserving.
     * @param \Udb\Core\Vault\Services\V1\BatchDecryptRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Vault\Services\V1\BatchDecryptResponse>
     */
    public function BatchDecrypt(\Udb\Core\Vault\Services\V1\BatchDecryptRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.vault.services.v1.VaultService/BatchDecrypt',
        $argument,
        ['\Udb\Core\Vault\Services\V1\BatchDecryptResponse', 'decode'],
        $metadata, $options);
    }

}
