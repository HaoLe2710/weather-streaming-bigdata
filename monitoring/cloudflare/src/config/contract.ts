export interface FrozenContract {
  runId: string;
  cohortId: string;
  protocolId: string;
  startsAt: string;
  endsAt: string;
  targetHours: number;
  targetLocations: number;
  targetForecastSlots: number;
  modelSha256: string;
  featureListSha256: string;
  providerModel: string;
  forecastHorizonHours: number;
}

export const PINNED_CONTRACT: FrozenContract = Object.freeze({
  runId: "20261010T111433Z-prospective-live-t2h-v1",
  cohortId: "prospective-t2h-20261010T120000Z",
  protocolId: "T2H_LIVE_PROSPECTIVE_168H_V1",
  startsAt: "2026-10-10T12:00:00Z",
  endsAt: "2026-10-17T11:00:00Z",
  targetHours: 168,
  targetLocations: 63,
  targetForecastSlots: 10584,
  modelSha256: "bd5ee153b2709ac661557bdd11f8322b80de1264c65a27d1d6c79fbcf63ee66a",
  featureListSha256: "20a5d2fb56d9b7231f4c43b39ad7a833298d76b1bfd0f127b2b251c57e5d7fd2",
  providerModel: "ecmwf_ifs",
  forecastHorizonHours: 2,
});

export function csvAllowlist(value: string | undefined): Set<string> {
  return new Set((value || "").split(",").map((item) => item.trim()).filter(Boolean));
}
