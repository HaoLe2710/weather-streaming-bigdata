import type { StoredSnapshot } from "../types";

export async function findSnapshot(db: D1Database, environment: string, snapshotId: string) {
  return db.prepare(`
    SELECT payload_sha256, validation_status
    FROM monitoring_snapshots WHERE environment = ? AND snapshot_id = ? LIMIT 1
  `).bind(environment, snapshotId).first<{ payload_sha256: string; validation_status: string }>();
}

export async function latestSnapshot(db: D1Database, streamId: string): Promise<StoredSnapshot | null> {
  return db.prepare(`
    SELECT s.* FROM monitoring_streams st
    JOIN monitoring_snapshots s
      ON s.environment = st.environment AND s.snapshot_id = st.latest_snapshot_id
    WHERE st.stream_id = ? AND s.validation_status = 'ACCEPTED'
    LIMIT 1
  `).bind(streamId).first<StoredSnapshot>();
}
