"""Setup phase activities: SSH liveness check and tool installation.

Both activities receive a :class:`MachineSpec` and a snapshot of the
relevant config block; secrets come from :func:`load_secrets` inside each
call so the worker doesn't have to carry them across the activity
boundary (Temporal would log them otherwise).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from bare_metal_tests.activities._decode import as_machine
from bare_metal_tests.activities._ssh import open_ssh
from bare_metal_tests.config import load_secrets
from bare_metal_tests.constants import (
    PHASE_APT_INSTALL,
    PHASE_SSH_CHECK,
    STATUS_FAILED,
    STATUS_PASSED,
)
from bare_metal_tests.models import MachineSpec, PhaseResult
from bare_metal_tests.ssh.client import SSHError
from bare_metal_tests.ssh.runner import long_run


def _now() -> datetime:
    return datetime.now(UTC)


@activity.defn(name="ssh_check")
async def ssh_check(machine: MachineSpec) -> PhaseResult:
    """Connect to ``machine`` and run a trivial sanity command."""
    machine = as_machine(machine)
    started = _now()
    secrets = load_secrets()
    try:
        async with open_ssh(machine, secrets) as ssh:
            result = await ssh.run("hostname && uname -r")
    except SSHError as exc:
        raise ApplicationError(
            f"SSH unreachable: {exc}",
            type="SSHUnreachable",
            non_retryable=True,
        ) from exc

    finished = _now()
    if not result.ok:
        return PhaseResult(
            phase=PHASE_SSH_CHECK,
            status=STATUS_FAILED,
            details={"stderr": result.stderr.strip(), "exit": result.exit_status},
            started_at=started,
            finished_at=finished,
        )
    lines = result.stdout.strip().splitlines()
    return PhaseResult(
        phase=PHASE_SSH_CHECK,
        status=STATUS_PASSED,
        details={
            "hostname": lines[0] if lines else "",
            "kernel": lines[1] if len(lines) > 1 else "",
        },
        started_at=started,
        finished_at=finished,
    )


@activity.defn(name="apt_install")
async def apt_install(
    machine: MachineSpec,
    packages: list[str],
    mesh_mtu: int = 9000,
    set_mesh_mtu: bool = True,
) -> PhaseResult:
    """Install apt packages and optionally set the mesh interface MTU.

    Setting the MTU here keeps the network test idempotent on subsequent
    runs. Disable with ``set_mesh_mtu=False`` when the switch can't
    carry jumbo frames and the NIC should keep its default.
    """
    machine = as_machine(machine)
    started = _now()
    secrets = load_secrets()
    pkg_args = " ".join(packages)
    base_cmd = (
        f"DEBIAN_FRONTEND=noninteractive apt-get update "
        f"&& DEBIAN_FRONTEND=noninteractive apt-get install -y {pkg_args}"
    )
    if set_mesh_mtu:
        base_cmd += f" && ip link set {machine.mesh_iface} mtu {mesh_mtu}"
    install_cmd = f"sh -c '{base_cmd}'"
    try:
        async with open_ssh(machine, secrets) as ssh:
            result = await long_run(
                ssh,
                install_cmd,
                label=PHASE_APT_INSTALL,
                timeout=540,
                heartbeat=activity.heartbeat,
                sudo=True,
            )
    except SSHError as exc:
        raise ApplicationError(
            f"SSH transport failed during apt install: {exc}",
            type="SSHTransport",
        ) from exc

    finished = _now()
    if not result.ok:
        return PhaseResult(
            phase=PHASE_APT_INSTALL,
            status=STATUS_FAILED,
            details={
                "exit": result.exit_status,
                "stderr_tail": result.stderr.strip().splitlines()[-20:],
            },
            started_at=started,
            finished_at=finished,
        )
    installed = [line for line in result.stdout.splitlines() if "Setting up" in line]
    return PhaseResult(
        phase=PHASE_APT_INSTALL,
        status=STATUS_PASSED,
        details={"packages": packages, "installed_count": len(installed)},
        started_at=started,
        finished_at=finished,
    )


ALL_SETUP_ACTIVITIES: list[Any] = [ssh_check, apt_install]
