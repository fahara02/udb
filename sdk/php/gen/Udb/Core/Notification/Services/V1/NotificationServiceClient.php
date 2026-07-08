<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Notification\Services\V1;

/**
 * ---------------------------------------------------------------------------
 * NotificationService — Template-based multi-channel notification delivery.
 *
 * HTTP prefix: /v1/notifications
 * ---------------------------------------------------------------------------
 *
 */
class NotificationServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Send a notification (or enqueue it for async delivery).
     * @param \Udb\Core\Notification\Services\V1\SendNotificationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Notification\Services\V1\SendNotificationResponse>
     */
    public function SendNotification(\Udb\Core\Notification\Services\V1\SendNotificationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.notification.services.v1.NotificationService/SendNotification',
        $argument,
        ['\Udb\Core\Notification\Services\V1\SendNotificationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get delivery status for a specific log entry.
     * @param \Udb\Core\Notification\Services\V1\GetNotificationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Notification\Services\V1\GetNotificationResponse>
     */
    public function GetNotification(\Udb\Core\Notification\Services\V1\GetNotificationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.notification.services.v1.NotificationService/GetNotification',
        $argument,
        ['\Udb\Core\Notification\Services\V1\GetNotificationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List notification logs with rich filters.
     * @param \Udb\Core\Notification\Services\V1\ListNotificationsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Notification\Services\V1\ListNotificationsResponse>
     */
    public function ListNotifications(\Udb\Core\Notification\Services\V1\ListNotificationsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.notification.services.v1.NotificationService/ListNotifications',
        $argument,
        ['\Udb\Core\Notification\Services\V1\ListNotificationsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Retry a failed notification.
     * @param \Udb\Core\Notification\Services\V1\RetryNotificationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Notification\Services\V1\RetryNotificationResponse>
     */
    public function RetryNotification(\Udb\Core\Notification\Services\V1\RetryNotificationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.notification.services.v1.NotificationService/RetryNotification',
        $argument,
        ['\Udb\Core\Notification\Services\V1\RetryNotificationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Delivery reporting (master-plan 9.13) ───────────────────────────────
     *
     * Report the terminal per-channel delivery outcome for a sent notification.
     * Internal seam: the leader-elected delivery worker — or a provider webhook
     * bridge — reports queued/sent/delivered/failed; the handler upserts the
     * NotificationDeliveryAttempt row and emits `udb.notification.delivery.<status>.v1`.
     * @param \Udb\Core\Notification\Services\V1\ReportDeliveryRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Notification\Services\V1\ReportDeliveryResponse>
     */
    public function ReportDelivery(\Udb\Core\Notification\Services\V1\ReportDeliveryRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.notification.services.v1.NotificationService/ReportDelivery',
        $argument,
        ['\Udb\Core\Notification\Services\V1\ReportDeliveryResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Template management ─────────────────────────────────────────────────
     *
     * Upsert a notification template.
     * @param \Udb\Core\Notification\Services\V1\UpsertTemplateRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Notification\Services\V1\UpsertTemplateResponse>
     */
    public function UpsertTemplate(\Udb\Core\Notification\Services\V1\UpsertTemplateRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.notification.services.v1.NotificationService/UpsertTemplate',
        $argument,
        ['\Udb\Core\Notification\Services\V1\UpsertTemplateResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get a template by event_type + channel + locale.
     * @param \Udb\Core\Notification\Services\V1\GetTemplateRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Notification\Services\V1\GetTemplateResponse>
     */
    public function GetTemplate(\Udb\Core\Notification\Services\V1\GetTemplateRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.notification.services.v1.NotificationService/GetTemplate',
        $argument,
        ['\Udb\Core\Notification\Services\V1\GetTemplateResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List all templates.
     * @param \Udb\Core\Notification\Services\V1\ListTemplatesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Notification\Services\V1\ListTemplatesResponse>
     */
    public function ListTemplates(\Udb\Core\Notification\Services\V1\ListTemplatesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.notification.services.v1.NotificationService/ListTemplates',
        $argument,
        ['\Udb\Core\Notification\Services\V1\ListTemplatesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get delivery statistics.
     * @param \Udb\Core\Notification\Services\V1\GetDeliveryStatsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Notification\Services\V1\GetDeliveryStatsResponse>
     */
    public function GetDeliveryStats(\Udb\Core\Notification\Services\V1\GetDeliveryStatsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.notification.services.v1.NotificationService/GetDeliveryStats',
        $argument,
        ['\Udb\Core\Notification\Services\V1\GetDeliveryStatsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Notification preferences ────────────────────────────────────────────
     *
     * Set (upsert) a per-user channel/event opt-out preference.
     * @param \Udb\Core\Notification\Services\V1\SetPreferenceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Notification\Services\V1\SetPreferenceResponse>
     */
    public function SetPreference(\Udb\Core\Notification\Services\V1\SetPreferenceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.notification.services.v1.NotificationService/SetPreference',
        $argument,
        ['\Udb\Core\Notification\Services\V1\SetPreferenceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get a single preference entry.
     * @param \Udb\Core\Notification\Services\V1\GetPreferenceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Notification\Services\V1\GetPreferenceResponse>
     */
    public function GetPreference(\Udb\Core\Notification\Services\V1\GetPreferenceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.notification.services.v1.NotificationService/GetPreference',
        $argument,
        ['\Udb\Core\Notification\Services\V1\GetPreferenceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List all preferences for a user.
     * @param \Udb\Core\Notification\Services\V1\ListPreferencesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Notification\Services\V1\ListPreferencesResponse>
     */
    public function ListPreferences(\Udb\Core\Notification\Services\V1\ListPreferencesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.notification.services.v1.NotificationService/ListPreferences',
        $argument,
        ['\Udb\Core\Notification\Services\V1\ListPreferencesResponse', 'decode'],
        $metadata, $options);
    }

}
