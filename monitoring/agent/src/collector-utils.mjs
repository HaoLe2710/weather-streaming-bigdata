export function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function nonNegativeInteger(value) {
  return Number.isSafeInteger(value) && value >= 0 ? value : null;
}

export function utcTimestamp(value) {
  if (typeof value !== "string" || !value.trim()) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
}

export function collectorResult(status, data, warnings = []) {
  return { status, data, warnings: [...new Set(warnings)] };
}

export function sourceWarning(prefix, result) {
  if (result.status === "OK") return null;
  return `${prefix}_${result.status}`;
}

export function boundedCodes(value, maximum = 20) {
  if (!Array.isArray(value)) return [];
  return value
    .filter((item) => typeof item === "string")
    .map((item) => item.slice(0, 100))
    .slice(0, maximum);
}
