import { performance } from "node:perf_hooks";
import { execFileText } from "../command.mjs";
import { readJsonlTail } from "../readers.mjs";
import { boundedCodes, collectorResult, isRecord, sourceWarning, utcTimestamp } from "../collector-utils.mjs";

function parseProperties(output) {
  const values = {};
  for (const line of String(output).split(/\r?\n/)) {
    const separator = line.indexOf("=");
    if (separator > 0) values[line.slice(0, separator)] = line.slice(separator + 1);
  }
  return values;
}

function effectiveAuditStatus(audit) {
  if (!isRecord(audit)) return null;
  return audit.status === "UNRESOLVED" ? audit.last_status ?? "UNKNOWN" : audit.status ?? "UNKNOWN";
}

function auditSummary(audit) {
  if (!isRecord(audit)) return null;
  return {
    status: typeof audit.status === "string" ? audit.status : "UNKNOWN",
    effectiveStatus: effectiveAuditStatus(audit),
    lastStatus: typeof audit.last_status === "string" ? audit.last_status : null,
    classification: typeof audit.classification === "string" ? audit.classification.slice(0, 100) : null,
    checkedAt: utcTimestamp(audit.checked_at ?? audit.last_checked_at),
  };
}

function observationTime(row) {
  return utcTimestamp(row?.checked_at ?? row?.created_at ?? row?.hourly_delta_audit?.checked_at ?? row?.hourly_delta_audit?.last_checked_at);
}

function recentAuditHistory(rows, now) {
  const cutoff = now.getTime() - 24 * 60 * 60 * 1000;
  const observations = [];
  for (const row of rows) {
    const audit = row?.hourly_delta_audit;
    if (!isRecord(audit)) continue;
    const checkedAt = observationTime(row);
    if (!checkedAt || Date.parse(checkedAt) < cutoff) continue;
    const effectiveStatus = effectiveAuditStatus(audit);
    if (effectiveStatus !== "PASS" && effectiveStatus !== "FAIL") continue;
    observations.push({ checkedAt, status: effectiveStatus, classification: typeof audit.classification === "string" ? audit.classification.slice(0, 100) : null });
  }
  const failures = observations.filter((entry) => entry.status === "FAIL").slice(-10);
  return {
    windowSeconds: 86400,
    passCount: observations.filter((entry) => entry.status === "PASS").length,
    failCount: observations.filter((entry) => entry.status === "FAIL").length,
    recentFailures: failures,
  };
}

async function systemdProperties(unit, exec) {
  const properties = [
    "ActiveState", "SubState", "Result", "ExecMainStatus", "ExecMainStartTimestamp", "ExecMainExitTimestamp",
    "LastTriggerUSec", "NextElapseUSecRealtime",
  ];
  const output = await exec("systemctl", ["show", unit, "--no-pager", ...properties.map((property) => `--property=${property}`)], {
    timeoutMs: 2500,
    maxBuffer: 64 * 1024,
  });
  return parseProperties(output);
}

function commandCode(error) {
  if (error?.code === "ENOENT") return "SYSTEMCTL_UNAVAILABLE";
  if (error?.code === "ETIMEDOUT") return "SYSTEMCTL_TIMEOUT";
  return "SYSTEMCTL_ACCESS_UNAVAILABLE";
}

export async function collectWatchdog(
  config,
  { exec = execFileText, readJsonl = readJsonlTail, platform = process.platform, now = new Date() } = {},
) {
  const started = performance.now();
  const warnings = [];
  const [monitorResult, hourlyResult] = await Promise.all([
    readJsonl(config.paths.monitorLog),
    readJsonl(config.paths.hourlyReports),
  ]);
  for (const [name, result] of [["WATCHDOG_MONITOR_LOG", monitorResult], ["WATCHDOG_HOURLY_REPORTS", hourlyResult]]) {
    const warning = sourceWarning(name, result);
    if (warning) warnings.push(warning);
    if (result.invalidLines) warnings.push(`${name}_CONTAIN_INVALID_LINES`);
    if (result.incompleteTail) warnings.push(`${name}_HAVE_INCOMPLETE_TAIL`);
    if (result.truncated) warnings.push(`${name}_TAIL_TRUNCATED`);
  }
  const monitorRows = Array.isArray(monitorResult.records) ? monitorResult.records : [];
  const hourlyRows = Array.isArray(hourlyResult.records) ? hourlyResult.records : [];
  const latestReport = monitorRows.at(-1) ?? null;
  const latestHourly = hourlyRows.at(-1) ?? null;
  let latestAuditRecord = null;
  for (let index = hourlyRows.length - 1; index >= 0; index -= 1) {
    if (isRecord(hourlyRows[index]?.hourly_delta_audit)) {
      latestAuditRecord = hourlyRows[index].hourly_delta_audit;
      break;
    }
  }
  if (!latestAuditRecord && isRecord(latestReport?.hourly_delta_audit)) latestAuditRecord = latestReport.hourly_delta_audit;

  let timer = null;
  let service = null;
  if (platform !== "linux") {
    warnings.push("SYSTEMD_UNAVAILABLE_NON_LINUX_HOST");
  } else {
    const values = await Promise.all([
      ["timer", "weather-t2h-watchdog.timer"],
      ["service", "weather-t2h-watchdog.service"],
    ].map(async ([kind, unit]) => {
      try {
        return [kind, await systemdProperties(unit, exec)];
      } catch (error) {
        warnings.push(commandCode(error));
        return [kind, null];
      }
    }));
    timer = values.find(([kind]) => kind === "timer")?.[1] ?? null;
    service = values.find(([kind]) => kind === "service")?.[1] ?? null;
  }

  const monitorFailures = monitorRows.filter((row) => row?.status === "FAIL").length;
  const monitorFailureHistory = monitorRows
    .filter((row) => row?.status === "FAIL")
    .slice(-10)
    .map((row) => ({
      checkedAt: observationTime(row),
      dataErrors: boundedCodes(row.current_data_errors ?? row.data_errors, 12),
      infrastructureErrors: boundedCodes(row.infrastructure_errors, 12),
    }));
  const latestWarnings = boundedCodes(latestReport?.warnings, 30);
  const statusAgeMs = latestReport ? Date.parse(observationTime(latestReport) || "") : NaN;
  const data = {
    timer: timer ? {
      active: timer.ActiveState === "active",
      activeState: timer.ActiveState ?? null,
      subState: timer.SubState ?? null,
      lastTriggerAt: utcTimestamp(timer.LastTriggerUSec),
      nextTriggerAt: utcTimestamp(timer.NextElapseUSecRealtime),
    } : { active: null, activeState: null, subState: null, lastTriggerAt: null, nextTriggerAt: null },
    service: service ? {
      activeState: service.ActiveState ?? null,
      subState: service.SubState ?? null,
      lastResult: service.Result ?? null,
      exitStatus: /^\d+$/.test(service.ExecMainStatus || "") ? Number(service.ExecMainStatus) : null,
      lastStartedAt: utcTimestamp(service.ExecMainStartTimestamp),
      lastExitedAt: utcTimestamp(service.ExecMainExitTimestamp),
    } : { activeState: null, subState: null, lastResult: null, exitStatus: null, lastStartedAt: null, lastExitedAt: null },
    latestReport: latestReport ? {
      status: latestReport.status ?? null,
      checkedAt: observationTime(latestReport),
      cohortStatus: latestReport.cohort_status ?? null,
      cohortRunId: latestReport.run_id ?? null,
      cohortId: latestReport.cohort_id ?? null,
      microbatchUpdateCount: latestReport.microbatch_update_count ?? null,
      lastUpdateAt: utcTimestamp(latestReport.last_update_at),
      forecastsPersisted: latestReport.prospective_forecast_count ?? null,
      validEvaluations: latestReport.valid_evaluation_count ?? null,
      restartCount: latestReport.restart_count ?? null,
      persistenceReceiptCount: latestReport.persistence_receipt_count ?? null,
      currentDataErrors: boundedCodes(latestReport.current_data_errors, 30),
      infrastructureErrors: boundedCodes(latestReport.infrastructure_errors, 30),
      warnings: latestWarnings,
      ageSeconds: Number.isFinite(statusAgeMs) ? Math.max(0, Math.floor((now.getTime() - statusAgeMs) / 1000)) : null,
    } : null,
    latestHourlyStatus: latestHourly?.status ?? null,
    latestAudit: auditSummary(latestAuditRecord),
    auditHistory: recentAuditHistory(hourlyRows, now),
    failureHistory: { failedMonitorReportCount: monitorFailures, recentFailures: monitorFailureHistory },
    persistenceReceiptCount: typeof latestHourly?.persistence_receipt_count === "number" ? latestHourly.persistence_receipt_count : null,
    persistenceReceiptAudit: isRecord(latestAuditRecord?.forecast_count_snapshot_audit)
      ? {
        status: latestAuditRecord.forecast_count_snapshot_audit.status ?? null,
        checkedAt: utcTimestamp(latestAuditRecord.forecast_count_snapshot_audit.checked_at),
        receiptCount: latestAuditRecord.forecast_count_snapshot_audit.receipt_count ?? null,
      }
      : null,
    sourceFiles: {
      monitorLog: { status: monitorResult.status, modifiedAt: monitorResult.modifiedAt ?? null },
      hourlyReports: { status: hourlyResult.status, modifiedAt: hourlyResult.modifiedAt ?? null },
    },
    collectorDurationMs: Math.round((performance.now() - started) * 100) / 100,
  };
  return collectorResult(warnings.length ? "PARTIAL" : "PASS", data, warnings);
}
