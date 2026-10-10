from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
AUDIT_PROCESS_SCRIPT = REPOSITORY_ROOT / "ops" / "azure_t2h" / "container_audit_process.py"


pytestmark = pytest.mark.skipif(os.name != "posix", reason="container audit process control uses Linux process groups and flock")


def _run_command(state_dir: Path, audit_id: str, *command: str, timeout: float = 30.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(AUDIT_PROCESS_SCRIPT),
            "run",
            "--state-dir",
            str(state_dir),
            "--audit-id",
            audit_id,
            "--timeout-seconds",
            str(timeout),
            "--termination-grace-seconds",
            "0.2",
            "--",
            *command,
        ],
        text=True,
        capture_output=True,
        check=False,
        timeout=8,
    )


def _job(state_dir: Path, audit_id: str) -> dict:
    return json.loads((state_dir / "delta_audit_processes" / f"{audit_id}.json").read_text(encoding="utf-8"))


def test_timeout_terminates_only_the_audit_process_group_and_records_identity(tmp_path):
    unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
    try:
        result = _run_command(tmp_path, "timeout-test", sys.executable, "-c", "import time; time.sleep(30)", timeout=0.1)
        assert result.returncode == 124

        job = _job(tmp_path, "timeout-test")
        assert job["status"] == "TIMED_OUT"
        assert job["cleanup"]["confirmed_terminated"] is True
        assert job["process_identity"]["pid"] == job["process_identity"]["process_group_id"]
        assert job["process_identity"]["session_id"] == job["process_identity"]["pid"]
        assert job["process_identity"]["start_time_ticks"]
        assert job["timeout_seconds"] == 0.1
        assert job["cleanup"]["target_audit_id"] == "timeout-test"
        assert os.kill(unrelated.pid, 0) is None

        events = [json.loads(line) for line in (tmp_path / "delta_audit_process_events.jsonl").read_text(encoding="utf-8").splitlines()]
        assert any(row.get("event") == "AUDIT_TIMEOUT" and row.get("audit_id") == "timeout-test" for row in events)
        assert any(row.get("event") == "AUDIT_PROCESS_CLEANUP" and row.get("confirmed_terminated") is True for row in events)
    finally:
        if unrelated.poll() is None:
            os.killpg(unrelated.pid, signal.SIGTERM)
            unrelated.wait(timeout=5)


def test_container_lock_blocks_overlapping_spark_audits(tmp_path):
    first = subprocess.Popen(
        [
            sys.executable,
            str(AUDIT_PROCESS_SCRIPT),
            "run",
            "--state-dir",
            str(tmp_path),
            "--audit-id",
            "first-audit",
            "--timeout-seconds",
            "30",
            "--termination-grace-seconds",
            "0.2",
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(30)",
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 5
        first_state = tmp_path / "delta_audit_processes" / "first-audit.json"
        while time.monotonic() < deadline and not first_state.exists():
            time.sleep(0.02)
        assert first_state.exists()
        assert _job(tmp_path, "first-audit")["status"] == "RUNNING"

        second = _run_command(tmp_path, "second-audit", sys.executable, "-c", "print('should not run')")
        assert second.returncode == 75
        assert _job(tmp_path, "second-audit")["status"] == "BLOCKED_ACTIVE_AUDIT"
        assert _job(tmp_path, "second-audit")["active_audit_id"] == "first-audit"
        assert _job(tmp_path, "first-audit")["status"] == "RUNNING"

        wrong_target = subprocess.run(
            [sys.executable, str(AUDIT_PROCESS_SCRIPT), "terminate", "--state-dir", str(tmp_path), "--audit-id", "second-audit"],
            text=True,
            capture_output=True,
            check=False,
            timeout=5,
        )
        assert wrong_target.returncode != 0
        assert _job(tmp_path, "first-audit")["status"] == "RUNNING"
    finally:
        if first.poll() is None:
            terminate = subprocess.run(
                [sys.executable, str(AUDIT_PROCESS_SCRIPT), "terminate", "--state-dir", str(tmp_path), "--audit-id", "first-audit"],
                text=True,
                capture_output=True,
                check=False,
                timeout=5,
            )
            first.wait(timeout=5)
            assert terminate.returncode == 0
            assert _job(tmp_path, "first-audit")["status"] == "FAILED"


def test_inspect_confirms_stopped_process_group_before_new_audit(tmp_path):
    result = _run_command(tmp_path, "completed-audit", sys.executable, "-c", "print('done')")
    assert result.returncode == 0
    assert _job(tmp_path, "completed-audit")["status"] == "SUCCEEDED"

    inspected = subprocess.run(
        [sys.executable, str(AUDIT_PROCESS_SCRIPT), "inspect", "--state-dir", str(tmp_path)],
        text=True,
        capture_output=True,
        check=False,
        timeout=5,
    )
    assert inspected.returncode == 0
    assert json.loads(inspected.stdout)["status"] == "IDLE"


def test_terminate_does_not_overwrite_a_terminal_audit_record(tmp_path):
    result = _run_command(tmp_path, "completed-audit", sys.executable, "-c", "print('done')")
    assert result.returncode == 0
    job_path = tmp_path / "delta_audit_processes" / "completed-audit.json"
    original_job = job_path.read_bytes()

    terminate = subprocess.run(
        [
            sys.executable,
            str(AUDIT_PROCESS_SCRIPT),
            "terminate",
            "--state-dir",
            str(tmp_path),
            "--audit-id",
            "completed-audit",
        ],
        text=True,
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert terminate.returncode == 0
    assert json.loads(terminate.stdout)["status"] == "ALREADY_TERMINAL"
    assert job_path.read_bytes() == original_job
