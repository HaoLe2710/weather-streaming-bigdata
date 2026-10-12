import validateSchema from "./generated-monitoring-snapshot-v1";
import type { MonitoringSnapshotV1 } from "../types";

export interface SnapshotValidation {
  valid: boolean;
  errors: string[];
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const UTC_ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/;
const TIMESTAMP_KEY = /(?:At|Time|Timestamp)$/;

function findNonUtcTimestamps(value: unknown, path = "$"): string[] {
  if (Array.isArray(value)) {
    return value.flatMap((child, index) => findNonUtcTimestamps(child, `${path}[${index}]`));
  }
  if (!value || typeof value !== "object") return [];
  return Object.entries(value).flatMap(([key, child]) => {
    const childPath = `${path}.${key}`;
    if (TIMESTAMP_KEY.test(key) && child !== null && typeof child === "string") {
      return UTC_ISO.test(child) && Number.isFinite(Date.parse(child))
        ? []
        : [`${childPath}:expected_utc_iso8601`];
    }
    return findNonUtcTimestamps(child, childPath);
  });
}

export function validateSnapshot(input: unknown): SnapshotValidation {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return { valid: false, errors: ["$:expected_object"] };
  }
  const snapshot = input as Record<string, unknown>;
  const errors: string[] = [];
  if (typeof snapshot.snapshotId !== "string" || !UUID.test(snapshot.snapshotId)) {
    errors.push("$.snapshotId:expected_uuid");
  }
  if (typeof snapshot.timestamp !== "string" || !UTC_ISO.test(snapshot.timestamp) || !Number.isFinite(Date.parse(snapshot.timestamp))) {
    errors.push("$.timestamp:expected_utc_iso8601");
  } else if (Date.parse(snapshot.timestamp) > Date.now() + 60_000) {
    errors.push("$.timestamp:future_timestamp");
  }
  errors.push(...findNonUtcTimestamps(input));

  const schemaValid = (validateSchema as (data: unknown) => boolean)(input);
  if (!schemaValid) {
    for (const error of (validateSchema as unknown as { errors?: Array<{ instancePath?: string; keyword?: string }> }).errors || []) {
      errors.push(`${error.instancePath || "$"}:${error.keyword || "schema_invalid"}`);
      if (errors.length >= 24) break;
    }
  }
  return { valid: errors.length === 0, errors: errors.slice(0, 24) };
}

export function isMonitoringSnapshot(input: unknown): input is MonitoringSnapshotV1 {
  return validateSnapshot(input).valid;
}
