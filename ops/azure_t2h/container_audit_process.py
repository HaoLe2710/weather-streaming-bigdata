#!/usr/bin/env python3
"""Run one isolated Spark audit process group inside the inference container.

The watchdog launches this supervisor with ``docker compose exec -d``. The
supervisor persists its identity before starting Spark, holds a container-side
exclusive lock, and can terminate only the new process session it created.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
from typing import Any, Mapping


PROCESS_LOCK_NAME = "delta_audit_process.lock"
ACTIVE_PROCESS_NAME = "delta_audit_process_active.json"
PROCESS_EVENTS_NAME = "delta_audit_process_events.jsonl"
AUDIT_JOB_DIRECTORY = "delta_audit_processes"
TERMINAL_STATUSES = frozenset({
    "SUCCEEDED",
    "FAILED",
    "TIMED_OUT",
    "CANCELLED",
    "ORPHAN_TERMINATED",
    "ORPHAN_CLEANED",
})


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.partial")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    temporary.replace(path)


def _append_event(state_dir: Path, event: str, payload: Mapping[str, Any]) -> None:
    path = state_dir / PROCESS_EVENTS_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"event": event, "created_at": _now(), **dict(payload)}
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n")


def _job_path(state_dir: Path, audit_id: str) -> Path:
    return state_dir / AUDIT_JOB_DIRECTORY / f"{audit_id}.json"


def _proc_stat(pid: int) -> dict[str, Any] | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (OSError, ValueError):
        return None
    try:
        suffix = raw.rsplit(")", 1)[1].strip().split()
        return {
            "state": suffix[0],
            "process_group_id": int(suffix[2]),
            "session_id": int(suffix[3]),
            "start_time_ticks": int(suffix[19]),
        }
    except (IndexError, ValueError):
        return None


def _capture_identity(pid: int, command: list[str]) -> dict[str, Any] | None:
    stat = _proc_stat(pid)
    if stat is None:
        return None
    if stat["process_group_id"] != pid or stat["session_id"] != pid:
        return None
    return {
        "pid": pid,
        "process_group_id": pid,
        "session_id": pid,
        "start_time_ticks": stat["start_time_ticks"],
        "command": command,
        "command_sha256": hashlib.sha256(json.dumps(command, separators=(",", ":")).encode("utf-8")).hexdigest(),
        "captured_at": _now(),
    }


def _capture_owner_identity() -> dict[str, Any] | None:
    stat = _proc_stat(os.getpid())
    if stat is None:
        return None
    return {"pid": os.getpid(), "start_time_ticks": stat["start_time_ticks"]}


def _group_members(pgid: int) -> list[dict[str, Any]]:
    members: list[dict[str, Any]] = []
    try:
        entries = list(Path("/proc").iterdir())
    except OSError:
        return members
    for entry in entries:
        if not entry.name.isdecimal():
            continue
        pid = int(entry.name)
        stat = _proc_stat(pid)
        if stat and stat["process_group_id"] == pgid and stat["session_id"] == pgid:
            members.append({"pid": pid, **stat})
    return members


def _process_group_state(identity: Any) -> dict[str, Any]:
    if not isinstance(identity, Mapping):
        return {"status": "UNVERIFIED", "reason": "process identity is missing"}
    try:
        pid = int(identity["pid"])
        pgid = int(identity["process_group_id"])
        sid = int(identity["session_id"])
        start_time = int(identity["start_time_ticks"])
    except (KeyError, TypeError, ValueError):
        return {"status": "UNVERIFIED", "reason": "process identity fields are invalid"}
    if pid != pgid or pid != sid or pid <= 1:
        return {"status": "UNVERIFIED", "reason": "audit process was not isolated in its own session"}

    leader = _proc_stat(pid)
    members = _group_members(pgid)
    active_members = [member for member in members if member["state"] != "Z"]
    if leader is not None:
        if leader["start_time_ticks"] != start_time:
            return {"status": "UNVERIFIED", "reason": "audit PID was reused; refusing to signal a different process", "group_members": active_members}
        if leader["process_group_id"] != pgid or leader["session_id"] != sid:
            return {"status": "UNVERIFIED", "reason": "audit PID no longer belongs to its recorded isolated process session", "group_members": active_members}
        if leader["state"] != "Z":
            return {"status": "ACTIVE", "reason": "recorded audit process identity is still live", "group_members": active_members}
    if active_members:
        return {"status": "ACTIVE_DESCENDANTS", "reason": "the recorded isolated audit session still has live descendants", "group_members": active_members}
    return {"status": "TERMINATED", "reason": "the recorded audit process session has no live processes", "group_members": []}


def _owner_alive(owner: Any) -> bool | None:
    if not isinstance(owner, Mapping):
        return None
    try:
        pid = int(owner["pid"])
        started = int(owner["start_time_ticks"])
    except (KeyError, TypeError, ValueError):
        return None
    current = _proc_stat(pid)
    if current is None:
        return False
    return current["start_time_ticks"] == started and current["state"] != "Z"


def _wait_group_terminated(identity: Mapping[str, Any], timeout_seconds: float) -> dict[str, Any]:
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    last = _process_group_state(identity)
    while last["status"] in {"ACTIVE", "ACTIVE_DESCENDANTS"} and time.monotonic() < deadline:
        time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        last = _process_group_state(identity)
    return last


def _terminate_process_group(
    identity: Any,
    *,
    audit_id: str,
    termination_grace_seconds: float,
) -> dict[str, Any]:
    state = _process_group_state(identity)
    outcome: dict[str, Any] = {
        "target_audit_id": audit_id,
        "process_identity": dict(identity) if isinstance(identity, Mapping) else None,
        "initial_process_state": state,
        "signals_sent": [],
        "confirmed_terminated": state.get("status") == "TERMINATED",
    }
    if state.get("status") == "TERMINATED":
        outcome["classification"] = "ALREADY_TERMINATED"
        return outcome
    if state.get("status") not in {"ACTIVE", "ACTIVE_DESCENDANTS"}:
        outcome.update({"classification": "PROCESS_IDENTITY_UNVERIFIED", "reason": state.get("reason")})
        return outcome

    try:
        pgid = int(identity["process_group_id"])
        os.killpg(pgid, signal.SIGTERM)
        outcome["signals_sent"].append("SIGTERM")
    except ProcessLookupError:
        pass
    except OSError as exc:
        outcome.update({"classification": "SIGTERM_FAILED", "reason": f"{type(exc).__name__}: {exc}"})
        return outcome
    state = _wait_group_terminated(identity, termination_grace_seconds)
    if state.get("status") in {"ACTIVE", "ACTIVE_DESCENDANTS"}:
        try:
            os.killpg(pgid, signal.SIGKILL)
            outcome["signals_sent"].append("SIGKILL")
        except ProcessLookupError:
            pass
        except OSError as exc:
            outcome.update({"classification": "SIGKILL_FAILED", "reason": f"{type(exc).__name__}: {exc}", "final_process_state": state})
            return outcome
        state = _wait_group_terminated(identity, max(1.0, termination_grace_seconds))
    outcome["final_process_state"] = state
    outcome["confirmed_terminated"] = state.get("status") == "TERMINATED"
    outcome["classification"] = "PROCESS_GROUP_TERMINATED" if outcome["confirmed_terminated"] else "PROCESS_GROUP_TERMINATION_UNCONFIRMED"
    return outcome


def _persist_job(state_dir: Path, job: Mapping[str, Any], *, update_active: bool = True) -> None:
    record = dict(job)
    _atomic_json(_job_path(state_dir, str(record["audit_id"])), record)
    if update_active:
        _atomic_json(state_dir / ACTIVE_PROCESS_NAME, record)


def _running_job_state(job: Mapping[str, Any], *, check_owner: bool = True) -> dict[str, Any]:
    process_state = _process_group_state(job.get("process_identity"))
    owner_state = _owner_alive(job.get("supervisor_identity")) if check_owner else None
    if process_state.get("status") in {"ACTIVE", "ACTIVE_DESCENDANTS"}:
        if owner_state is False:
            return {"status": "ORPHAN_ACTIVE", "process_state": process_state, "supervisor_alive": False}
        if owner_state is None:
            return {"status": "UNVERIFIED", "process_state": process_state, "supervisor_alive": None}
        return {"status": "RUNNING", "process_state": process_state, "supervisor_alive": True}
    if process_state.get("status") == "TERMINATED":
        if owner_state is True:
            return {"status": "COMPLETION_PENDING", "process_state": process_state, "supervisor_alive": True}
        if owner_state is None:
            return {"status": "UNVERIFIED", "process_state": process_state, "supervisor_alive": None}
        return {"status": "TERMINATED", "process_state": process_state, "supervisor_alive": False}
    return {"status": "UNVERIFIED", "process_state": process_state, "supervisor_alive": owner_state}


def _validate_audit_id(audit_id: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", audit_id) or audit_id in {".", ".."}:
        raise ValueError("invalid audit id")


def _write_terminal(state_dir: Path, job: Mapping[str, Any], status: str, **updates: Any) -> dict[str, Any]:
    record = {**dict(job), **updates, "status": status, "finished_at": _now()}
    _persist_job(state_dir, record)
    return record


def run_audit_job(
    state_dir: Path,
    audit_id: str,
    command: list[str],
    *,
    timeout_seconds: float = 900.0,
    termination_grace_seconds: float = 20.0,
) -> int:
    if os.name != "posix" or not Path("/proc").is_dir():
        raise RuntimeError("isolated audit process control requires a Linux container")
    _validate_audit_id(audit_id)
    if not command or timeout_seconds <= 0 or termination_grace_seconds < 0:
        raise ValueError("invalid Spark audit command or timeout configuration")
    state_dir.mkdir(parents=True, exist_ok=True)
    job_dir = state_dir / AUDIT_JOB_DIRECTORY
    job_dir.mkdir(parents=True, exist_ok=True)
    lock_path = state_dir / PROCESS_LOCK_NAME
    lock_handle = lock_path.open("a+b")
    try:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            active = _read_json(state_dir / ACTIVE_PROCESS_NAME)
            blocked = {
                "audit_id": audit_id,
                "status": "BLOCKED_ACTIVE_AUDIT",
                "classification": "CONTAINER_AUDIT_LOCK_HELD",
                "reason": "another container-side Spark audit holds the exclusive process lock",
                "active_audit_id": active.get("audit_id") if isinstance(active, Mapping) else None,
                "active_process_identity": active.get("process_identity") if isinstance(active, Mapping) else None,
                "started_at": _now(),
                "finished_at": _now(),
            }
            _persist_job(state_dir, blocked, update_active=False)
            _append_event(state_dir, "AUDIT_BLOCKED_LOCK_HELD", blocked)
            print(json.dumps(blocked, sort_keys=True))
            return 75

        active_path = state_dir / ACTIVE_PROCESS_NAME
        prior = _read_json(active_path)
        if isinstance(prior, Mapping) and prior.get("status") in {"RUNNING", "STARTING", "UNRESOLVED"}:
            prior_state = _running_job_state(prior)
            if prior_state["status"] in {"RUNNING", "ORPHAN_ACTIVE", "COMPLETION_PENDING", "UNVERIFIED"}:
                blocked = {
                    "audit_id": audit_id,
                    "status": "BLOCKED_ACTIVE_AUDIT",
                    "classification": "PREVIOUS_AUDIT_PROCESS_ACTIVE_OR_UNVERIFIED",
                    "reason": "a prior audit session is active or its termination cannot be confirmed",
                    "active_audit_id": prior.get("audit_id"),
                    "active_process_state": prior_state,
                    "started_at": _now(),
                    "finished_at": _now(),
                }
                _persist_job(state_dir, blocked, update_active=False)
                _append_event(state_dir, "AUDIT_BLOCKED_PREVIOUS_PROCESS_ACTIVE", blocked)
                print(json.dumps(blocked, sort_keys=True))
                return 75
            _append_event(state_dir, "PREVIOUS_AUDIT_PROCESS_CONFIRMED_TERMINATED", {
                "audit_id": prior.get("audit_id"),
                "process_identity": prior.get("process_identity"),
                "process_state": prior_state,
            })

        command_hash = hashlib.sha256(json.dumps(command, separators=(",", ":")).encode("utf-8")).hexdigest()
        log_path = job_dir / f"{audit_id}.log"
        job: dict[str, Any] = {
            "audit_id": audit_id,
            "status": "STARTING",
            "classification": "SPARK_AUDIT_STARTING",
            "started_at": _now(),
            "supervisor_identity": _capture_owner_identity(),
            "command": command,
            "command_sha256": command_hash,
            "timeout_seconds": float(timeout_seconds),
            "termination_grace_seconds": float(termination_grace_seconds),
            "log_path": str(log_path),
            "process_identity": None,
            "cleanup": None,
        }
        _persist_job(state_dir, job)
        _append_event(state_dir, "AUDIT_PROCESS_STARTING", {key: job.get(key) for key in ("audit_id", "command_sha256", "timeout_seconds", "supervisor_identity")})

        started_monotonic = time.monotonic()
        with log_path.open("wb") as output:
            try:
                process = subprocess.Popen(
                    command,
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    close_fds=True,
                    start_new_session=True,
                )
            except OSError as exc:
                terminal = _write_terminal(
                    state_dir,
                    job,
                    "FAILED",
                    classification="AUDIT_PROCESS_START_FAILED",
                    reason=f"{type(exc).__name__}: {exc}",
                    returncode=None,
                )
                _append_event(state_dir, "AUDIT_PROCESS_START_FAILED", terminal)
                print(json.dumps(terminal, sort_keys=True))
                return 127
            identity_deadline = time.monotonic() + 2.0
            identity = _capture_identity(process.pid, command)
            while identity is None and process.poll() is None and time.monotonic() < identity_deadline:
                time.sleep(0.02)
                identity = _capture_identity(process.pid, command)
            if identity is None and process.poll() is None:
                terminal = _write_terminal(
                    state_dir,
                    job,
                    "UNRESOLVED",
                    classification="AUDIT_PROCESS_IDENTITY_UNVERIFIED",
                    reason="started Spark audit did not enter a verifiable isolated session",
                    process_identity={"pid": process.pid, "process_group_id": process.pid, "session_id": process.pid},
                    returncode=None,
                    cleanup={"confirmed_terminated": False, "classification": "IDENTITY_UNVERIFIED"},
                )
                _append_event(state_dir, "AUDIT_PROCESS_IDENTITY_UNVERIFIED", terminal)
                print(json.dumps(terminal, sort_keys=True))
                return 125
            if identity is not None:
                job.update({"status": "RUNNING", "classification": "SPARK_AUDIT_RUNNING", "process_identity": identity})
                _persist_job(state_dir, job)
                _append_event(state_dir, "AUDIT_PROCESS_STARTED", {
                    "audit_id": audit_id,
                    "process_identity": identity,
                    "command_sha256": command_hash,
                    "timeout_seconds": float(timeout_seconds),
                })
            else:
                returncode = process.wait()
                terminal_status = "SUCCEEDED" if returncode == 0 else "FAILED"
                terminal = _write_terminal(state_dir, job, terminal_status, classification="AUDIT_PROCESS_EXITED", returncode=returncode)
                _append_event(state_dir, "AUDIT_PROCESS_EXITED", terminal)
                print(json.dumps(terminal, sort_keys=True))
                return returncode

            timed_out = False
            while process.poll() is None:
                if time.monotonic() - started_monotonic >= timeout_seconds:
                    timed_out = True
                    break
                time.sleep(min(0.1, max(0.01, timeout_seconds - (time.monotonic() - started_monotonic))))

            returncode = process.poll()
            cleanup: dict[str, Any] | None = None
            if timed_out:
                timeout_event = {
                    "audit_id": audit_id,
                    "process_identity": identity,
                    "timeout_seconds": float(timeout_seconds),
                    "elapsed_seconds": time.monotonic() - started_monotonic,
                }
                _append_event(state_dir, "AUDIT_TIMEOUT", timeout_event)
                cleanup = _terminate_process_group(identity, audit_id=audit_id, termination_grace_seconds=termination_grace_seconds)
                if cleanup["confirmed_terminated"]:
                    returncode = process.wait(timeout=max(1.0, termination_grace_seconds + 1.0))
                    status = "TIMED_OUT"
                    classification = "AUDIT_PROCESS_TIMEOUT_CLEANED"
                else:
                    status = "UNRESOLVED"
                    classification = "AUDIT_PROCESS_TIMEOUT_CLEANUP_UNCONFIRMED"
            else:
                returncode = process.wait()
                group_status = _process_group_state(identity)
                if group_status.get("status") in {"ACTIVE", "ACTIVE_DESCENDANTS"}:
                    cleanup = _terminate_process_group(identity, audit_id=audit_id, termination_grace_seconds=termination_grace_seconds)
                    if not cleanup["confirmed_terminated"]:
                        status = "UNRESOLVED"
                        classification = "AUDIT_DESCENDANT_CLEANUP_UNCONFIRMED"
                    else:
                        status = "SUCCEEDED" if returncode == 0 else "FAILED"
                        classification = "AUDIT_PROCESS_EXITED_DESCENDANTS_CLEANED"
                else:
                    cleanup = {"confirmed_terminated": True, "classification": "PROCESS_GROUP_EXITED_NATURALLY", "target_audit_id": audit_id, "process_identity": identity}
                    status = "SUCCEEDED" if returncode == 0 else "FAILED"
                    classification = "AUDIT_PROCESS_EXITED"

            terminal = _write_terminal(
                state_dir,
                job,
                status,
                classification=classification,
                returncode=returncode,
                cleanup=cleanup,
                elapsed_seconds=time.monotonic() - started_monotonic,
            )
            _append_event(state_dir, "AUDIT_PROCESS_CLEANUP", {
                "audit_id": audit_id,
                "process_identity": identity,
                "confirmed_terminated": bool(cleanup and cleanup.get("confirmed_terminated")),
                "cleanup": cleanup,
            })
            _append_event(state_dir, "AUDIT_PROCESS_FINISHED", terminal)
            print(json.dumps(terminal, sort_keys=True))
            if status == "TIMED_OUT":
                return 124
            if status == "UNRESOLVED":
                return 125
            return int(returncode or 0)
    finally:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            lock_handle.close()


def inspect_active_process(state_dir: Path, *, orphan_grace_seconds: float = 0.5) -> dict[str, Any]:
    active_path = state_dir / ACTIVE_PROCESS_NAME
    active = _read_json(active_path)
    if not isinstance(active, Mapping) or active.get("status") not in {"RUNNING", "STARTING", "UNRESOLVED"}:
        return {"status": "IDLE", "active_audit_id": active.get("audit_id") if isinstance(active, Mapping) else None}
    observed = _running_job_state(active)
    if observed["status"] == "COMPLETION_PENDING":
        deadline = time.monotonic() + max(0.0, orphan_grace_seconds)
        while time.monotonic() < deadline:
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
            active = _read_json(active_path)
            if not isinstance(active, Mapping) or active.get("status") in TERMINAL_STATUSES:
                return {"status": "IDLE", "active_audit_id": active.get("audit_id") if isinstance(active, Mapping) else None}
            observed = _running_job_state(active)
            if observed["status"] != "COMPLETION_PENDING":
                break
    audit_id = str(active.get("audit_id") or "")
    if observed["status"] == "RUNNING":
        return {"status": "RUNNING", "active_audit_id": audit_id, "process_identity": active.get("process_identity"), "supervisor_alive": True}
    if observed["status"] == "ORPHAN_ACTIVE":
        cleanup = _terminate_process_group(
            active.get("process_identity"),
            audit_id=audit_id,
            termination_grace_seconds=orphan_grace_seconds,
        )
        if not cleanup["confirmed_terminated"]:
            _append_event(state_dir, "ORPHAN_AUDIT_CLEANUP_UNCONFIRMED", {"audit_id": audit_id, "cleanup": cleanup})
            return {"status": "UNVERIFIED", "active_audit_id": audit_id, "cleanup": cleanup}
        resolved = {
            **dict(active),
            "status": "ORPHAN_CLEANED",
            "classification": "ORPHANED_AUDIT_PROCESS_TERMINATED",
            "finished_at": _now(),
            "cleanup": cleanup,
        }
        _persist_job(state_dir, resolved)
        _append_event(state_dir, "ORPHAN_AUDIT_PROCESS_TERMINATED", {"audit_id": audit_id, "process_identity": active.get("process_identity"), "cleanup": cleanup})
        return {"status": "TERMINATED", "active_audit_id": audit_id, "cleanup": cleanup}
    if observed["status"] == "TERMINATED":
        resolved = {
            **dict(active),
            "status": "ORPHAN_TERMINATED",
            "classification": "ORPHANED_AUDIT_PROCESS_CONFIRMED_TERMINATED",
            "finished_at": _now(),
            "cleanup": {"confirmed_terminated": True, "classification": "PROCESS_GROUP_ALREADY_TERMINATED", "target_audit_id": audit_id, "process_identity": active.get("process_identity")},
        }
        _persist_job(state_dir, resolved)
        _append_event(state_dir, "ORPHAN_AUDIT_PROCESS_CONFIRMED_TERMINATED", {"audit_id": audit_id, "process_identity": active.get("process_identity")})
        return {"status": "TERMINATED", "active_audit_id": audit_id, "cleanup": resolved["cleanup"]}
    return {"status": "UNVERIFIED", "active_audit_id": audit_id, "process_state": observed}


def terminate_active_process(state_dir: Path, audit_id: str, *, termination_grace_seconds: float = 20.0) -> dict[str, Any]:
    _validate_audit_id(audit_id)
    active_path = state_dir / ACTIVE_PROCESS_NAME
    active = _read_json(active_path)
    if not isinstance(active, Mapping) or active.get("audit_id") != audit_id:
        return {"status": "UNVERIFIED", "classification": "ACTIVE_AUDIT_IDENTITY_MISMATCH", "requested_audit_id": audit_id}
    if active.get("status") in TERMINAL_STATUSES:
        return {
            "status": "ALREADY_TERMINAL",
            "audit_id": audit_id,
            "job_status": active.get("status"),
            "cleanup": active.get("cleanup"),
            "finished_at": active.get("finished_at"),
        }
    observed = _running_job_state(active)
    if observed["status"] == "UNVERIFIED":
        return {"status": "UNVERIFIED", "classification": "ACTIVE_AUDIT_PROCESS_IDENTITY_UNVERIFIED", "audit_id": audit_id, "process_state": observed}
    cleanup = _terminate_process_group(
        active.get("process_identity"),
        audit_id=audit_id,
        termination_grace_seconds=termination_grace_seconds,
    )
    _append_event(state_dir, "AUDIT_PROCESS_TERMINATION_REQUESTED", {"audit_id": audit_id, "process_identity": active.get("process_identity"), "cleanup": cleanup})
    if not cleanup.get("confirmed_terminated"):
        return {"status": "UNVERIFIED", "audit_id": audit_id, "cleanup": cleanup}
    if _owner_alive(active.get("supervisor_identity")) is True:
        # The run supervisor owns the terminal audit record. Leave its active
        # state untouched so it can reap Spark and persist its real exit status.
        return {
            "status": "TERMINATED",
            "audit_id": audit_id,
            "cleanup": cleanup,
            "supervisor_alive": True,
            "job_record_updated": False,
        }
    terminal_after_cleanup = _read_json(active_path)
    if (
        isinstance(terminal_after_cleanup, Mapping)
        and terminal_after_cleanup.get("audit_id") == audit_id
        and terminal_after_cleanup.get("status") in TERMINAL_STATUSES
    ):
        return {
            "status": "ALREADY_TERMINAL",
            "audit_id": audit_id,
            "job_status": terminal_after_cleanup.get("status"),
            "cleanup": terminal_after_cleanup.get("cleanup"),
            "finished_at": terminal_after_cleanup.get("finished_at"),
        }
    record = {**dict(active), "status": "CANCELLED", "classification": "AUDIT_PROCESS_TERMINATED_BY_WATCHDOG", "finished_at": _now(), "cleanup": cleanup}
    _persist_job(state_dir, record)
    _append_event(state_dir, "AUDIT_PROCESS_TERMINATED_BY_WATCHDOG", {"audit_id": audit_id, "process_identity": active.get("process_identity"), "cleanup": cleanup})
    return {"status": "TERMINATED", "audit_id": audit_id, "cleanup": cleanup}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run and control a process-isolated Spark audit inside the inference container")
    subparsers = parser.add_subparsers(dest="operation", required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--state-dir", type=Path, required=True)
    run.add_argument("--audit-id", required=True)
    run.add_argument("--timeout-seconds", type=float, default=900.0)
    run.add_argument("--termination-grace-seconds", type=float, default=20.0)
    run.add_argument("command", nargs=argparse.REMAINDER)
    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("--state-dir", type=Path, required=True)
    inspect.add_argument("--orphan-grace-seconds", type=float, default=0.5)
    terminate = subparsers.add_parser("terminate")
    terminate.add_argument("--state-dir", type=Path, required=True)
    terminate.add_argument("--audit-id", required=True)
    terminate.add_argument("--termination-grace-seconds", type=float, default=20.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.operation == "run":
        command = list(args.command)
        if command and command[0] == "--":
            command = command[1:]
        return run_audit_job(
            args.state_dir,
            args.audit_id,
            command,
            timeout_seconds=args.timeout_seconds,
            termination_grace_seconds=args.termination_grace_seconds,
        )
    if args.operation == "inspect":
        result = inspect_active_process(args.state_dir, orphan_grace_seconds=args.orphan_grace_seconds)
        print(json.dumps(result, sort_keys=True))
        return 2 if result.get("status") == "UNVERIFIED" else 0
    result = terminate_active_process(args.state_dir, args.audit_id, termination_grace_seconds=args.termination_grace_seconds)
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("status") == "TERMINATED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
