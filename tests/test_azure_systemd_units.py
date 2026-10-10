from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SYSTEMD_DIR = REPOSITORY_ROOT / "ops" / "azure_t2h" / "systemd"


def _render_unit_template(source: Path) -> str:
    return (
        source.read_text(encoding="utf-8")
        .replace("@REPO_ROOT@", "/opt/weather-streaming/weather-streaming-bigdata")
        .replace("@DEPLOY_USER@", "weather")
        .replace("@DEPLOY_GROUP@", "weather")
    )


def test_systemd_installer_enables_only_installable_resume_and_timer_units():
    installer = (REPOSITORY_ROOT / "ops" / "azure_t2h" / "install-systemd.sh").read_text(encoding="utf-8")
    resume = _render_unit_template(SYSTEMD_DIR / "weather-t2h-resume.service.in")
    watchdog_service = _render_unit_template(SYSTEMD_DIR / "weather-t2h-watchdog.service.in")
    watchdog_timer = (SYSTEMD_DIR / "weather-t2h-watchdog.timer").read_text(encoding="utf-8")

    assert "[Install]" in resume
    assert "WantedBy=multi-user.target" in resume
    assert "[Install]" not in watchdog_service
    assert "Unit=weather-t2h-watchdog.service" in watchdog_timer
    assert "WantedBy=timers.target" in watchdog_timer
    assert installer.index("systemctl daemon-reload") < installer.index("systemctl enable weather-t2h-resume.service")
    assert installer.index("systemctl enable weather-t2h-resume.service") < installer.index(
        "systemctl enable --now weather-t2h-watchdog.timer"
    )


@pytest.mark.skipif(
    sys.platform != "linux" or shutil.which("systemctl") is None,
    reason="offline systemd enable check requires Linux systemctl",
)
def test_systemd_enable_succeeds_for_resume_service_and_watchdog_timer(tmp_path):
    system_root = tmp_path / "system-root"
    unit_dir = system_root / "etc" / "systemd" / "system"
    unit_dir.mkdir(parents=True)

    (unit_dir / "weather-t2h-resume.service").write_text(
        _render_unit_template(SYSTEMD_DIR / "weather-t2h-resume.service.in"),
        encoding="utf-8",
    )
    (unit_dir / "weather-t2h-watchdog.service").write_text(
        _render_unit_template(SYSTEMD_DIR / "weather-t2h-watchdog.service.in"),
        encoding="utf-8",
    )
    (unit_dir / "weather-t2h-watchdog.timer").write_text(
        (SYSTEMD_DIR / "weather-t2h-watchdog.timer").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    systemctl = shutil.which("systemctl")
    assert systemctl is not None
    for unit in ("weather-t2h-resume.service", "weather-t2h-watchdog.timer"):
        completed = subprocess.run(
            [systemctl, "--root", str(system_root), "enable", unit],
            text=True,
            capture_output=True,
            check=False,
        )
        assert completed.returncode == 0, f"systemctl enable {unit} failed: {completed.stderr}"

    assert (unit_dir / "multi-user.target.wants" / "weather-t2h-resume.service").is_symlink()
    assert (unit_dir / "timers.target.wants" / "weather-t2h-watchdog.timer").is_symlink()
