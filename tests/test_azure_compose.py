from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest

from ops.azure_t2h import watchdog
from validation import prospective_t2h as prospective
from validation import prospective_t2h_runtime as runtime
from validation.azure_compose import (
    AZURE_COMPOSE_FILE,
    AZURE_COMPOSE_FILES,
    AzureComposeConfigurationError,
    azure_compose_environment,
    validate_azure_compose_configuration,
)


RUN_ID = "20261010T120000Z-prospective-live-t2h-v1"


def _create_compose_files(root: Path) -> None:
    for name in AZURE_COMPOSE_FILES:
        (root / name).write_text("services: {}\n", encoding="utf-8")


def test_azure_compose_validation_uses_exact_files_and_config_command(tmp_path):
    _create_compose_files(tmp_path)
    calls = []

    env = validate_azure_compose_configuration(
        tmp_path,
        {"WEATHER_AZURE_RUNTIME": "1"},
        runner=lambda command, **kwargs: calls.append((command, kwargs))
        or subprocess.CompletedProcess(command, 0, "", ""),
    )

    assert AZURE_COMPOSE_FILE == "docker-compose.yml:compose.azure.yaml:compose.azure.resources.yaml"
    assert env["COMPOSE_FILE"] == AZURE_COMPOSE_FILE
    assert calls[0][0] == ["docker", "compose", "config", "--quiet"]
    assert calls[0][1]["cwd"] == tmp_path.resolve()
    assert calls[0][1]["env"]["COMPOSE_FILE"] == AZURE_COMPOSE_FILE


def test_missing_azure_overlay_fails_closed_without_compose_fallback(tmp_path):
    (tmp_path / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")
    calls = []

    with pytest.raises(AzureComposeConfigurationError, match="compose.azure.yaml"):
        validate_azure_compose_configuration(
            tmp_path,
            {"WEATHER_AZURE_RUNTIME": "1"},
            runner=lambda *args, **kwargs: calls.append((args, kwargs)),
        )

    assert calls == []


def test_unexpected_compose_file_override_fails_closed(tmp_path):
    _create_compose_files(tmp_path)
    with pytest.raises(AzureComposeConfigurationError, match="must be exactly"):
        azure_compose_environment(
            tmp_path,
            {
                "WEATHER_AZURE_RUNTIME": "1",
                "COMPOSE_FILE": "docker-compose.yml",
            },
        )


def test_readiness_watchdog_and_resume_share_the_same_azure_compose_stack(
    tmp_path,
    monkeypatch,
):
    _create_compose_files(tmp_path)
    monkeypatch.setenv("WEATHER_AZURE_RUNTIME", "1")
    monkeypatch.setenv("COMPOSE_FILE", AZURE_COMPOSE_FILE)
    monkeypatch.setattr(prospective, "REPOSITORY_ROOT", tmp_path)
    runtime_config = runtime.cohort_runtime_configuration(RUN_ID)
    compose_env_seen = []

    monkeypatch.setattr(
        runtime,
        "validate_azure_compose_configuration",
        lambda root, env: dict(env),
    )
    monkeypatch.setattr(
        runtime,
        "_wait_for_healthy_infrastructure",
        lambda *args, **kwargs: (True, "healthy"),
    )
    monkeypatch.setattr(
        runtime,
        "_run",
        lambda root, env, arguments, **kwargs: compose_env_seen.append(dict(env))
        or subprocess.CompletedProcess(list(arguments), 0, "ok", ""),
    )

    run_state = tmp_path / RUN_ID
    run_state.mkdir()
    (run_state / "readiness_request.json").write_text(
        json.dumps({"runtime_configuration": runtime_config}),
        encoding="utf-8",
    )
    completed = prospective._run_compose(
        RUN_ID,
        state_root=tmp_path,
        bootstrap=False,
    )
    watchdog_env = watchdog._compose_environment(
        tmp_path,
        RUN_ID,
        runtime_config,
    )

    assert completed.returncode == 0
    assert compose_env_seen
    assert all(env["COMPOSE_FILE"] == AZURE_COMPOSE_FILE for env in compose_env_seen)
    assert watchdog_env["COMPOSE_FILE"] == AZURE_COMPOSE_FILE

    repo_root = Path(__file__).resolve().parents[1]
    resume_script = (repo_root / "ops" / "azure_t2h" / "resume-existing-cohort.sh").read_text(encoding="utf-8")
    shared_shell = (repo_root / "ops" / "azure_t2h" / "compose-env.sh").read_text(encoding="utf-8")
    assert "source ops/azure_t2h/compose-env.sh" in resume_script
    assert 'set_weather_azure_compose_environment "$REPO_ROOT"' in resume_script
    assert AZURE_COMPOSE_FILE in shared_shell


def test_azure_deploy_preflight_readiness_and_systemd_require_shared_stack():
    repo_root = Path(__file__).resolve().parents[1]
    deploy = (repo_root / "ops" / "azure_t2h" / "deploy-azure.sh").read_text(encoding="utf-8")
    bootstrap = (repo_root / "ops" / "azure_t2h" / "bootstrap-azure.sh").read_text(encoding="utf-8")
    installer = (repo_root / "ops" / "azure_t2h" / "install-systemd.sh").read_text(encoding="utf-8")
    resume_unit = (repo_root / "ops" / "azure_t2h" / "systemd" / "weather-t2h-resume.service.in").read_text(encoding="utf-8")
    watchdog_unit = (repo_root / "ops" / "azure_t2h" / "systemd" / "weather-t2h-watchdog.service.in").read_text(encoding="utf-8")
    cli = (repo_root / "validation" / "prospective_t2h.py").read_text(encoding="utf-8")
    watchdog_source = (repo_root / "ops" / "azure_t2h" / "watchdog.py").read_text(encoding="utf-8")
    compose_helper = (repo_root / "ops" / "azure_t2h" / "compose-env.sh").read_text(encoding="utf-8")
    compose_module = (repo_root / "validation" / "azure_compose.py").read_text(encoding="utf-8")

    assert 'source ops/azure_t2h/compose-env.sh' in deploy
    assert 'set_weather_azure_compose_environment "$REPO_ROOT"' in deploy
    assert "compose.azure.yaml compose.azure.resources.yaml" in bootstrap
    assert 'validation.azure_compose --repo-root "$repo_root"' in compose_helper
    assert '["docker", "compose", "config", "--quiet"]' in compose_module
    assert 'source ops/azure_t2h/compose-env.sh' in installer
    assert "/etc/weather-streaming-t2h-compose.env" in installer
    assert "/etc/weather-streaming-t2h-compose.env" in resume_unit
    assert "/etc/weather-streaming-t2h-compose.env" in watchdog_unit
    assert "validate_azure_compose_configuration" in cli
    assert "validate_azure_compose_configuration" in watchdog_source


def test_azure_readiness_refuses_missing_overlay_before_creating_run_state(tmp_path, monkeypatch):
    state_root = tmp_path / "state"
    monkeypatch.setattr(prospective, "REPOSITORY_ROOT", tmp_path)
    monkeypatch.setenv("WEATHER_AZURE_RUNTIME", "1")
    monkeypatch.delenv("COMPOSE_FILE", raising=False)

    result = prospective.main([
        "--state-root",
        str(state_root),
        "readiness",
        "--run-id",
        RUN_ID,
    ])

    assert result == 2
    assert not state_root.exists()


def test_azure_preflight_runs_compose_config_with_the_required_file_list(tmp_path, monkeypatch):
    _create_compose_files(tmp_path)
    monkeypatch.setattr(prospective, "REPOSITORY_ROOT", tmp_path)
    monkeypatch.setenv("WEATHER_AZURE_RUNTIME", "1")
    monkeypatch.setenv("COMPOSE_FILE", AZURE_COMPOSE_FILE)
    calls = []

    def fake_run(command, **kwargs):
        calls.append((list(command), kwargs))
        output = "1 passed" if "pytest" in command else ""
        return subprocess.CompletedProcess(command, 0, output, "")

    monkeypatch.setattr(prospective.subprocess, "run", fake_run)
    result = prospective._preflight(None, tmp_path / "state")

    assert result == 0
    compose_calls = [item for item in calls if item[0] == ["docker", "compose", "config", "--quiet"]]
    assert len(compose_calls) == 1
    assert compose_calls[0][1]["env"]["COMPOSE_FILE"] == AZURE_COMPOSE_FILE
    assert compose_calls[0][1]["cwd"] == tmp_path


def test_watchdog_entrypoint_fails_closed_when_azure_overlay_is_missing(tmp_path, monkeypatch, capsys):
    calls = []
    monkeypatch.delenv("COMPOSE_FILE", raising=False)
    monkeypatch.setenv("WEATHER_AZURE_RUNTIME", "1")
    monkeypatch.setattr(
        watchdog.subprocess,
        "run",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    result = watchdog.main(["--repo-root", str(tmp_path), "--state-root", str(tmp_path / "state")])

    assert result == 2
    assert not calls
    assert not (tmp_path / "state").exists()
    assert "AZURE_COMPOSE_CONFIGURATION_INVALID" in capsys.readouterr().out
