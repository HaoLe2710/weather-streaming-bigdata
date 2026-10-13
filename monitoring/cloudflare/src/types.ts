export type CollectorStatus = "COMPLETE" | "PARTIAL" | "FAILED";
export type QualityStatus = "PASS" | "FAIL" | "UNKNOWN";
export type AdmissionStatus = "ACCEPTED" | "QUARANTINED";
export type AlertStatus = "OPEN" | "RESOLVED";

export interface MonitoringSnapshotV1 {
  schemaVersion: 1;
  snapshotId: string;
  timestamp: string;
  source: { environment: string; agentVersion: string; hostname: string };
  cohort: {
    runId: string;
    cohortId: string;
    protocolId: string;
    status: string;
    startsAt: string | null;
    endsAt: string | null;
    targetHours: number | null;
    targetLocations: number | null;
    targetForecastSlots: number | null;
    lastUpdateAt: string | null;
    statusFileUpdatedAt: string | null;
    identityValidation: string;
    frozenContractValidation: string;
    modelSha256: string | null;
    featureListSha256: string | null;
    providerModel: string | null;
    forecastHorizonHours: number | null;
  };
  progress: Record<string, number | null>;
  quality: { status: QualityStatus; [key: string]: unknown };
  inference: {
    modelSha256: string | null;
    modelContractStatus: string;
    [key: string]: unknown;
  };
  infrastructure: {
    host: { disk: { usagePercent: number | null } | null } | null;
    watchdog: {
      latestReport: { status: string | null; infrastructureErrors: string[] } | null;
      [key: string]: unknown;
    };
    [key: string]: unknown;
  };
  pipeline: { status: string; [key: string]: unknown };
  collector: { status: CollectorStatus; [key: string]: unknown };
  [key: string]: unknown;
}

export interface Env {
  DB: D1Database;
  MONITOR_HMAC_SECRET_PRIMARY?: string;
  MONITOR_HMAC_SECRET_PREVIOUS?: string;
  MONITOR_READ_API_TOKEN?: string;
  MONITOR_HMAC_KEY_ID_PRIMARY?: string;
  MONITOR_HMAC_KEY_ID_PREVIOUS?: string;
  HMAC_TIMESTAMP_TOLERANCE_SECONDS?: string;
  ALLOWED_ENVIRONMENTS?: string;
  ALLOWED_SOURCE_HOSTNAMES?: string;
  MONITOR_DEFAULT_INTERVAL_SECONDS?: string;
  MONITOR_DEFAULT_HEARTBEAT_TIMEOUT_SECONDS?: string;
  MONITOR_DEFAULT_STARTUP_GRACE_SECONDS?: string;
  MONITOR_DEFAULT_DISK_THRESHOLD_PERCENT?: string;
  MONITOR_TARGET_SCAN_LIMIT?: string;
  MONITOR_RECOVERY_BATCH_SIZE?: string;
  MONITOR_RECOVERY_MAX_ATTEMPTS?: string;
  MONITOR_RECOVERY_BASE_DELAY_SECONDS?: string;
  MONITOR_RETENTION_SNAPSHOT_DAYS?: string;
  MONITOR_RETENTION_ALERT_DAYS?: string;
  MONITOR_RETENTION_BATCH_SIZE?: string;
  MONITOR_MAX_PARTIAL_STREAK?: string;
  TELEGRAM_ALERTS_ENABLED?: string;
  TELEGRAM_BOT_TOKEN?: string;
  TELEGRAM_CHAT_ID?: string;
  TELEGRAM_MAX_ATTEMPTS?: string;
}

export interface AppVariables {
  requestId: string;
  snapshot: MonitoringSnapshotV1;
  rawBody: Uint8Array;
}

export type AppEnv = { Bindings: Env; Variables: AppVariables };

export interface StoredSnapshot {
  snapshot_id: string;
  stream_id: string | null;
  environment: string;
  cohort_id: string;
  run_id: string;
  source_identity: string;
  collected_at: string;
  received_at: string;
  collector_status: CollectorStatus;
  pipeline_status: string;
  quality_status: QualityStatus;
  validation_status: AdmissionStatus;
  payload_sha256: string;
  payload_json: string;
}

export interface AlertRow {
  alert_id: string;
  stream_id: string;
  alert_type: string;
  severity: "WARNING" | "CRITICAL";
  status: AlertStatus;
  fingerprint: string;
  first_triggered_at: string;
  last_observed_at: string;
  resolved_at: string | null;
  details_json: string;
  notification_status: "DISABLED" | "PENDING" | "SENT" | "FAILED";
}
