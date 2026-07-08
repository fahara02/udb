<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Backup\Services\V1;

/**
 * BackupService (master-plan 9.10) — tenant-level logical backup and restore.
 * A backup enumerates the tenant's owned tables via the SAME shared resolver the
 * purge ripple uses, streams each table's tenant rows as JSONL, encrypts them at
 * rest, and writes them plus a checksummed manifest to object storage. Tables
 * without a resolvable tenant column are REPORTED as excluded, never silently
 * skipped. A restore validates the cross-tenant movement scope, refuses to write
 * over a live (non-empty) target tenant, and rewrites the tenant column to the
 * target on insert.
 */
class BackupServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Start a logical backup of the calling tenant. Enumerates tenant-owned tables
     * via the shared resolver, encrypts each table's rows to object storage, and
     * journals the run. Tenant-less tables are reported as excluded.
     * @param \Udb\Core\Backup\Services\V1\StartTenantBackupRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Backup\Services\V1\StartTenantBackupResponse>
     */
    public function StartTenantBackup(\Udb\Core\Backup\Services\V1\StartTenantBackupRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.backup.services.v1.BackupService/StartTenantBackup',
        $argument,
        ['\Udb\Core\Backup\Services\V1\StartTenantBackupResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Restore a tenant's backup into a FRESH target tenant. DESTRUCTIVE: requires
     * an explicit confirmation token, the cross-tenant movement scope check, and a
     * target tenant that holds no rows (restoring over a live tenant is refused).
     * @param \Udb\Core\Backup\Services\V1\RestoreTenantRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Backup\Services\V1\RestoreTenantResponse>
     */
    public function RestoreTenant(\Udb\Core\Backup\Services\V1\RestoreTenantRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.backup.services.v1.BackupService/RestoreTenant',
        $argument,
        ['\Udb\Core\Backup\Services\V1\RestoreTenantResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List the calling tenant's backup/restore journal runs (most recent first).
     * @param \Udb\Core\Backup\Services\V1\ListBackupsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Backup\Services\V1\ListBackupsResponse>
     */
    public function ListBackups(\Udb\Core\Backup\Services\V1\ListBackupsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.backup.services.v1.BackupService/ListBackups',
        $argument,
        ['\Udb\Core\Backup\Services\V1\ListBackupsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Fetch one backup run plus its per-table manifest detail.
     * @param \Udb\Core\Backup\Services\V1\GetBackupRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Backup\Services\V1\GetBackupResponse>
     */
    public function GetBackup(\Udb\Core\Backup\Services\V1\GetBackupRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.backup.services.v1.BackupService/GetBackup',
        $argument,
        ['\Udb\Core\Backup\Services\V1\GetBackupResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Create or update the calling tenant's backup retention/schedule policy.
     * @param \Udb\Core\Backup\Services\V1\PutBackupPolicyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Backup\Services\V1\PutBackupPolicyResponse>
     */
    public function PutBackupPolicy(\Udb\Core\Backup\Services\V1\PutBackupPolicyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.backup.services.v1.BackupService/PutBackupPolicy',
        $argument,
        ['\Udb\Core\Backup\Services\V1\PutBackupPolicyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Fetch a tenant's backup retention/schedule policy by name.
     * @param \Udb\Core\Backup\Services\V1\GetBackupPolicyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Backup\Services\V1\GetBackupPolicyResponse>
     */
    public function GetBackupPolicy(\Udb\Core\Backup\Services\V1\GetBackupPolicyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.backup.services.v1.BackupService/GetBackupPolicy',
        $argument,
        ['\Udb\Core\Backup\Services\V1\GetBackupPolicyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List the calling tenant's backup retention policies.
     * @param \Udb\Core\Backup\Services\V1\ListBackupPoliciesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Backup\Services\V1\ListBackupPoliciesResponse>
     */
    public function ListBackupPolicies(\Udb\Core\Backup\Services\V1\ListBackupPoliciesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.backup.services.v1.BackupService/ListBackupPolicies',
        $argument,
        ['\Udb\Core\Backup\Services\V1\ListBackupPoliciesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Delete a tenant's backup retention policy by name.
     * @param \Udb\Core\Backup\Services\V1\DeleteBackupPolicyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Backup\Services\V1\DeleteBackupPolicyResponse>
     */
    public function DeleteBackupPolicy(\Udb\Core\Backup\Services\V1\DeleteBackupPolicyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.backup.services.v1.BackupService/DeleteBackupPolicy',
        $argument,
        ['\Udb\Core\Backup\Services\V1\DeleteBackupPolicyResponse', 'decode'],
        $metadata, $options);
    }

}
