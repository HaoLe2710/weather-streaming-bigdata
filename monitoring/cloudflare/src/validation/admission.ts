import { csvAllowlist, PINNED_CONTRACT } from "../config/contract";
import type { Env, MonitoringSnapshotV1 } from "../types";

export type QuarantineReason =
  | "SOURCE_NOT_ALLOWLISTED"
  | "COHORT_CONTRACT_MISMATCH"
  | "MODEL_CONTRACT_MISMATCH";

export function admissionDecision(
  snapshot: MonitoringSnapshotV1,
  env: Env,
): { admitted: true } | { admitted: false; reason: QuarantineReason } {
  const environments = csvAllowlist(env.ALLOWED_ENVIRONMENTS);
  const hostnames = csvAllowlist(env.ALLOWED_SOURCE_HOSTNAMES);
  if (!environments.has(snapshot.source.environment) || !hostnames.has(snapshot.source.hostname)) {
    return { admitted: false, reason: "SOURCE_NOT_ALLOWLISTED" };
  }

  const cohort = snapshot.cohort;
  if (
    cohort.runId !== PINNED_CONTRACT.runId ||
    cohort.cohortId !== PINNED_CONTRACT.cohortId ||
    cohort.protocolId !== PINNED_CONTRACT.protocolId ||
    cohort.status === "UNKNOWN" ||
    cohort.startsAt !== PINNED_CONTRACT.startsAt ||
    cohort.endsAt !== PINNED_CONTRACT.endsAt ||
    cohort.targetHours !== PINNED_CONTRACT.targetHours ||
    cohort.targetLocations !== PINNED_CONTRACT.targetLocations ||
    cohort.targetForecastSlots !== PINNED_CONTRACT.targetForecastSlots ||
    cohort.identityValidation !== "PASS" ||
    cohort.frozenContractValidation !== "PASS" ||
    cohort.providerModel !== PINNED_CONTRACT.providerModel ||
    cohort.forecastHorizonHours !== PINNED_CONTRACT.forecastHorizonHours
  ) {
    return { admitted: false, reason: "COHORT_CONTRACT_MISMATCH" };
  }

  if (
    cohort.modelSha256 !== PINNED_CONTRACT.modelSha256 ||
    cohort.featureListSha256 !== PINNED_CONTRACT.featureListSha256 ||
    snapshot.inference.expectedModelSha256 !== PINNED_CONTRACT.modelSha256 ||
    (snapshot.inference.modelSha256 !== null && snapshot.inference.modelSha256 !== PINNED_CONTRACT.modelSha256) ||
    snapshot.inference.modelContractStatus === "FAIL" ||
    snapshot.inference.modelContractStatus === "UNKNOWN_OR_MISMATCH"
  ) {
    return { admitted: false, reason: "MODEL_CONTRACT_MISMATCH" };
  }

  return { admitted: true };
}
