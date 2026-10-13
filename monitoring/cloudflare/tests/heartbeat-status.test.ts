import { describe, expect, it } from "vitest";
import type { MonitoringTargetRow, StreamStateRow } from "../src/repositories/streams";
import { isLiveHeartbeatCandidate, resolveHeartbeatState } from "../src/services/heartbeat-status";

const now = "2026-10-13T12:00:00.000Z";
const target = {
  monitoring_enabled: 1,
  activated_at: "2026-10-13T11:00:00.000Z",
  startup_grace_seconds: 900,
  heartbeat_timeout_seconds: 900,
  expected_interval_seconds: 300,
} as Pick<MonitoringTargetRow,
  "monitoring_enabled" | "activated_at" | "startup_grace_seconds" |
  "heartbeat_timeout_seconds" | "expected_interval_seconds">;

function liveState(heartbeatAt: string, receivedAt = heartbeatAt) {
  return {
    last_live_heartbeat_at: heartbeatAt,
    last_live_received_at: receivedAt,
    last_live_snapshot_id: "snapshot-id",
  } as Pick<StreamStateRow, "last_live_heartbeat_at" | "last_live_received_at" | "last_live_snapshot_id">;
}

describe("unified heartbeat status", () => {
  it.each([
    ["disabled", { ...target, monitoring_enabled: 0 }, null, "DISABLED"],
    ["enabled without activation", { ...target, activated_at: null }, null, "UNKNOWN"],
    ["inside startup grace", { ...target, activated_at: "2026-10-13T11:55:00.000Z" }, null, "PENDING"],
    ["never observed after grace", target, null, "OFFLINE"],
    ["fresh live evidence", target, liveState("2026-10-13T11:55:00.000Z", "2026-10-13T11:55:02.000Z"), "HEALTHY"],
    ["timeout boundary", target, liveState("2026-10-13T11:45:00.000Z", "2026-10-13T11:45:01.000Z"), "OFFLINE"],
    ["collection clock skew within tolerance", target, liveState("2026-10-13T12:00:30.000Z", "2026-10-13T12:00:00.000Z"), "HEALTHY"],
    ["collection too far in the future", target, liveState("2026-10-13T12:01:01.000Z", "2026-10-13T12:00:00.000Z"), "UNKNOWN"],
    ["receipt too far in the future", target, liveState("2026-10-13T11:59:00.000Z", "2026-10-13T12:01:01.000Z"), "UNKNOWN"],
    ["delivery lag exceeds two expected intervals", target, liveState("2026-10-13T11:48:00.000Z", "2026-10-13T11:59:00.000Z"), "UNKNOWN"],
    ["missing receipt evidence", target, { ...liveState("2026-10-13T11:59:00.000Z"), last_live_received_at: null }, "UNKNOWN"],
  ] as const)("classifies %s as %s", (_name, candidateTarget, state, expected) => {
    expect(resolveHeartbeatState(candidateTarget, state, now)).toBe(expected);
  });

  it("uses collection-to-receipt freshness for heartbeat admission", () => {
    expect(isLiveHeartbeatCandidate("2026-10-13T11:55:00.000Z", "2026-10-13T11:55:02.000Z", 300, 900)).toBe(true);
    expect(isLiveHeartbeatCandidate("2026-10-13T10:00:00.000Z", "2026-10-13T12:00:00.000Z", 300, 900)).toBe(false);
    expect(isLiveHeartbeatCandidate("2026-10-13T12:00:30.000Z", "2026-10-13T12:00:00.000Z", 300, 900)).toBe(true);
    expect(isLiveHeartbeatCandidate("2026-10-13T12:01:01.000Z", "2026-10-13T12:00:00.000Z", 300, 900)).toBe(false);
  });
});
