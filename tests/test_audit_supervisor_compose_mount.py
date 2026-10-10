from __future__ import annotations

from pathlib import Path
import re

from ops.azure_t2h import watchdog


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_audit_supervisor_path_is_inside_the_base_inference_service_mount():
    compose_text = (REPOSITORY_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    lines = compose_text.splitlines()
    service_name = "streaming-inference-t2h-live"
    service_start = next(
        index for index, line in enumerate(lines)
        if line == f"  {service_name}:"
    )
    service_end = next(
        (
            index for index in range(service_start + 1, len(lines))
            if re.fullmatch(r"  [A-Za-z0-9_.-]+:\s*", lines[index])
        ),
        len(lines),
    )
    service_lines = lines[service_start:service_end]
    volumes_index = service_lines.index("    volumes:")
    volume_specs = []
    for line in service_lines[volumes_index + 1:]:
        if line.strip() and not line.startswith("      "):
            break
        if line.startswith("      - "):
            volume_specs.append(line.strip()[2:])

    mount_targets = []
    for spec in volume_specs:
        parts = spec.split(":")
        if len(parts) >= 2 and parts[1].startswith("/opt/project/"):
            mount_targets.append(parts[1])

    assert "./spark:/opt/project/spark:ro" in volume_specs
    assert not any(target == "/opt/project/ops" or target.startswith("/opt/project/ops/") for target in mount_targets)
    assert any(
        watchdog.CONTAINER_AUDIT_SUPERVISOR_PATH == target
        or watchdog.CONTAINER_AUDIT_SUPERVISOR_PATH.startswith(target.rstrip("/") + "/")
        for target in mount_targets
    ), "the supervisor must remain under a path mounted by the base inference Compose service"
