<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Messaging\Services\V1;

/**
 * MessagingService: the delivery service and key directory for end-to-end
 * encrypted user-to-user messaging over MLS (RFC 9420, ciphersuite
 * MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519). Clients do all cryptography. The
 * broker stores opaque ciphertext plus the PUBLIC group state, validates every
 * handshake commit against that public state (linear epochs, an authoritative
 * roster), and fans messages out to per-device inboxes. It never holds a group
 * secret and never sees plaintext. Handshake metadata (who joined or left, group
 * sizes, timing) is visible to the broker by design.
 */
class MessagingServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Register one device of the calling principal: its device id, its Ed25519
     * signature public key, and its MLS BasicCredential naming
     * `<tenant>/<subject>/<device_id>`. Re-registering the same device with the same
     * key is idempotent; a different key for an existing device id is refused (a key
     * change is a new device).
     * @param \Udb\Core\Messaging\Services\V1\RegisterMessagingDeviceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\RegisterMessagingDeviceResponse>
     */
    public function RegisterMessagingDevice(\Udb\Core\Messaging\Services\V1\RegisterMessagingDeviceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/RegisterMessagingDevice',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\RegisterMessagingDeviceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List a principal's devices with their key fingerprints, so a peer can verify
     * keys out of band (safety numbers). Revoked devices are included only when
     * asked for.
     * @param \Udb\Core\Messaging\Services\V1\ListMessagingDevicesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\ListMessagingDevicesResponse>
     */
    public function ListMessagingDevices(\Udb\Core\Messaging\Services\V1\ListMessagingDevicesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/ListMessagingDevices',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\ListMessagingDevicesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Revoke one device: it stops receiving fan-out, its unclaimed KeyPackages are
     * deleted, and every group it belongs to gets a removal hint in the other member
     * devices' inboxes so a member commits the removal.
     * @param \Udb\Core\Messaging\Services\V1\RevokeMessagingDeviceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\RevokeMessagingDeviceResponse>
     */
    public function RevokeMessagingDevice(\Udb\Core\Messaging\Services\V1\RevokeMessagingDeviceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/RevokeMessagingDevice',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\RevokeMessagingDeviceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Upload one-time MLS KeyPackages (and optionally one last-resort package) for a
     * device. Every package is validated: its credential must name the device and
     * its signature key must be the device's registered key.
     * @param \Udb\Core\Messaging\Services\V1\PublishKeyPackagesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\PublishKeyPackagesResponse>
     */
    public function PublishKeyPackages(\Udb\Core\Messaging\Services\V1\PublishKeyPackagesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/PublishKeyPackages',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\PublishKeyPackagesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Atomically claim one KeyPackage per target device. A one-time package is
     * consumed exactly once, even under concurrent claims; when a device has none
     * left its last-resort package is returned without being consumed.
     * @param \Udb\Core\Messaging\Services\V1\ClaimKeyPackagesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\ClaimKeyPackagesResponse>
     */
    public function ClaimKeyPackages(\Udb\Core\Messaging\Services\V1\ClaimKeyPackagesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/ClaimKeyPackages',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\ClaimKeyPackagesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * How many unclaimed one-time KeyPackages a device has left, so the client can
     * top up below a threshold.
     * @param \Udb\Core\Messaging\Services\V1\GetKeyPackageStockRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\GetKeyPackageStockResponse>
     */
    public function GetKeyPackageStock(\Udb\Core\Messaging\Services\V1\GetKeyPackageStockRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/GetKeyPackageStock',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\GetKeyPackageStockResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Register a new MLS group from the creator's signed public GroupInfo and
     * ratchet tree. The broker keeps only public group state and never holds group
     * secrets.
     * @param \Udb\Core\Messaging\Services\V1\CreateGroupRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\CreateGroupResponse>
     */
    public function CreateGroup(\Udb\Core\Messaging\Services\V1\CreateGroupRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/CreateGroup',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\CreateGroupResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Submit an MLS PublicMessage commit (plus the Welcome for any added devices).
     * Accepted only when its epoch is the group's current epoch and it validates
     * against the public group state; concurrent commits linearize and the loser
     * receives FAILED_PRECONDITION carrying the current epoch so it can re-base.
     * An external commit (a new device joining from GetGroupInfo's GroupInfo, no
     * Welcome) is accepted only as the group's external_join_policy allows; the
     * joiner receives the COMMIT echo, not a Welcome. Groups whose policy is not
     * NONE require the new epoch's GroupInfo on every commit.
     * @param \Udb\Core\Messaging\Services\V1\SubmitCommitRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\SubmitCommitResponse>
     */
    public function SubmitCommit(\Udb\Core\Messaging\Services\V1\SubmitCommitRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/SubmitCommit',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\SubmitCommitResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Return the group's current epoch, its exported public ratchet tree, and the
     * latest signed GroupInfo a member uploaded, for an external join. An end
     * user sees only groups one of its devices is a member of, or groups open to
     * the tenant (EXTERNAL_JOIN_POLICY_TENANT); others are NOT_FOUND.
     * @param \Udb\Core\Messaging\Services\V1\GetGroupInfoRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\GetGroupInfoResponse>
     */
    public function GetGroupInfo(\Udb\Core\Messaging\Services\V1\GetGroupInfoRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/GetGroupInfo',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\GetGroupInfoResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Fan an MLS PrivateMessage ciphertext for the group's current epoch out to every
     * member device's inbox. The broker stores the opaque bytes and never decrypts
     * them.
     * @param \Udb\Core\Messaging\Services\V1\SendMessageRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\SendMessageResponse>
     */
    public function SendMessage(\Udb\Core\Messaging\Services\V1\SendMessageRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/SendMessage',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\SendMessageResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Stream a device's ordered inbox (commits, welcomes, messages, removal hints)
     * from a cursor. With follow=true the stream stays open and delivers new entries
     * as they are appended.
     * @param \Udb\Core\Messaging\Services\V1\StreamInboxRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function StreamInbox(\Udb\Core\Messaging\Services\V1\StreamInboxRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/udb.core.messaging.services.v1.MessagingService/StreamInbox',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\StreamInboxResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Advance a device's inbox cursor. Acknowledged entries become collectable by
     * the retention worker.
     * @param \Udb\Core\Messaging\Services\V1\AckInboxRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\AckInboxResponse>
     */
    public function AckInbox(\Udb\Core\Messaging\Services\V1\AckInboxRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/AckInbox',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\AckInboxResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Store (or replace) the calling principal's opt-in, client-encrypted history
     * backup. The client seals the export with a key derived from a recovery secret
     * the broker never sees; the broker keeps only the opaque envelope and refuses
     * a body that is not one. With expected_version > 0 the write is a
     * compare-and-swap against the stored version.
     * @param \Udb\Core\Messaging\Services\V1\PutMessagingBackupRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\PutMessagingBackupResponse>
     */
    public function PutMessagingBackup(\Udb\Core\Messaging\Services\V1\PutMessagingBackupRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/PutMessagingBackup',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\PutMessagingBackupResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Return a principal's stored backup envelope for restore on a new device. Only
     * the principal itself (or a service account acting for it) can read it.
     * @param \Udb\Core\Messaging\Services\V1\GetMessagingBackupRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\GetMessagingBackupResponse>
     */
    public function GetMessagingBackup(\Udb\Core\Messaging\Services\V1\GetMessagingBackupRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/GetMessagingBackup',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\GetMessagingBackupResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Erase a principal's stored backup. Deleting a backup that does not exist
     * succeeds with deleted=false.
     * @param \Udb\Core\Messaging\Services\V1\DeleteMessagingBackupRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Messaging\Services\V1\DeleteMessagingBackupResponse>
     */
    public function DeleteMessagingBackup(\Udb\Core\Messaging\Services\V1\DeleteMessagingBackupRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.messaging.services.v1.MessagingService/DeleteMessagingBackup',
        $argument,
        ['\Udb\Core\Messaging\Services\V1\DeleteMessagingBackupResponse', 'decode'],
        $metadata, $options);
    }

}
