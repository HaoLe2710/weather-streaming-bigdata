from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
from typing import Callable, Mapping


AZURE_REPOSITORY_ROOT = "/opt/weather-streaming/weather-streaming-bigdata"
AZURE_COMPOSE_FILES = (
    "docker-compose.yml",
    "compose.azure.yaml",
    "compose.azure.resources.yaml",
)
AZURE_COMPOSE_FILE = ":".join(AZURE_COMPOSE_FILES)


class AzureComposeConfigurationError(RuntimeError):
    """The required Azure Compose stack is absent, overridden, or invalid."""


def is_azure_runtime(
    repository_root: str | Path,
    environ: Mapping[str, str] | None = None,
) -> bool:
    source = os.environ if environ is None else environ
    if source.get("WEATHER_AZURE_RUNTIME") == "1":
        return True
    root = str(Path(repository_root).resolve()).replace("\\", "/").rstrip("/")
    return root == AZURE_REPOSITORY_ROOT


def azure_compose_environment(
    repository_root: str | Path,
    environ: Mapping[str, str] | None = None,
    *,
    required: bool | None = None,
) -> dict[str, str]:
    """Build the one Azure Compose environment; do not accept a silent override."""
    root = Path(repository_root).resolve()
    result = dict(os.environ if environ is None else environ)
    use_azure = is_azure_runtime(root, result) if required is None else required
    if not use_azure:
        return result

    configured = result.get("COMPOSE_FILE")
    if configured and configured != AZURE_COMPOSE_FILE:
        raise AzureComposeConfigurationError(
            f"COMPOSE_FILE must be exactly '{AZURE_COMPOSE_FILE}', got '{configured}'."
        )
    missing = [name for name in AZURE_COMPOSE_FILES if not (root / name).is_file()]
    if missing:
        raise AzureComposeConfigurationError(
            "Required Azure Compose files are missing under "
            f"{root}: {', '.join(missing)}. Refusing to fall back to docker-compose.yml."
        )
    result["COMPOSE_FILE"] = AZURE_COMPOSE_FILE
    result["WEATHER_AZURE_RUNTIME"] = "1"
    return result


def validate_azure_compose_configuration(
    repository_root: str | Path,
    environ: Mapping[str, str] | None = None,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> dict[str, str]:
    root = Path(repository_root).resolve()
    env = azure_compose_environment(root, environ, required=True)
    try:
        completed = (runner or subprocess.run)(
            ["docker", "compose", "config", "--quiet"],
            cwd=root,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise AzureComposeConfigurationError(
            f"Could not validate the required Azure Compose stack: {type(exc).__name__}: {exc}"
        ) from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "docker compose config failed").strip()
        raise AzureComposeConfigurationError(
            f"Azure Compose validation failed (exit {completed.returncode}): {detail}"
        )
    return env


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fail-closed Azure Compose stack validation.")
    parser.add_argument("--repo-root", type=Path, default=Path(AZURE_REPOSITORY_ROOT))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        validate_azure_compose_configuration(args.repo_root, os.environ)
    except AzureComposeConfigurationError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(f"Azure Compose configuration PASS: {AZURE_COMPOSE_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
