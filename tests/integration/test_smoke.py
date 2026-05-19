"""End-to-end smoke against the real cluster.

Requires:
- Three reachable hosts as configured in ``environment/machines.json``
- Local PostgreSQL with the ``bmt`` schema migrated
- A worker reachable via ``TEMPORAL_HOST`` in ``environment/secrets.env``

Skip-controlled via :func:`tests.integration.conftest.pytestmark`.
"""

from __future__ import annotations

import asyncio

import pytest

from bare_metal_tests.config import load_secrets, ssh_password_env_key
from bare_metal_tests.constants import AUTH_PASSWORD, MACHINES_PATH
from bare_metal_tests.models import MachineInventory, MachineSpec
from bare_metal_tests.ssh.client import SSHClient, SSHError


def _password_for(spec: MachineSpec, secrets: dict[str, str]) -> str | None:
    if spec.auth != AUTH_PASSWORD:
        return None
    return secrets.get(ssh_password_env_key(spec.name))


@pytest.mark.asyncio
async def test_every_host_is_reachable() -> None:
    """Each listed host accepts SSH and returns a hostname."""
    secrets = load_secrets()
    inventory = MachineInventory.model_validate_json(MACHINES_PATH.read_text())

    async def probe(spec: MachineSpec) -> tuple[str, bool]:
        try:
            async with SSHClient(
                machine=spec,
                sudo_password=secrets["SUDO_PASSWORD"],
                ssh_password=_password_for(spec, secrets),
                key_path=secrets.get("SSH_KEY_PATH"),
            ) as ssh:
                r = await ssh.run("hostname")
        except SSHError:
            return spec.name, False
        return spec.name, r.ok and bool(r.stdout.strip())

    results = await asyncio.gather(*(probe(m) for m in inventory.machines))
    for name, ok in results:
        assert ok, f"host {name} is not reachable"


@pytest.mark.asyncio
async def test_every_host_has_tools_installed() -> None:
    """After a successful setup run, the standard test tools must exist."""
    secrets = load_secrets()
    inventory = MachineInventory.model_validate_json(MACHINES_PATH.read_text())
    needed = [
        "fio",
        "iperf3",
        "stress-ng",
        "sysbench",
        "smartctl",
        "ethtool",
        "numactl",
    ]

    async def probe(spec: MachineSpec) -> dict[str, bool]:
        async with SSHClient(
            machine=spec,
            sudo_password=secrets["SUDO_PASSWORD"],
            ssh_password=_password_for(spec, secrets),
            key_path=secrets.get("SSH_KEY_PATH"),
        ) as ssh:
            results: dict[str, bool] = {}
            for tool in needed:
                r = await ssh.run(f"command -v {tool}")
                results[tool] = r.ok
            return results

    per_host = await asyncio.gather(*(probe(m) for m in inventory.machines))
    for tools, machine in zip(per_host, inventory.machines, strict=True):
        missing = [t for t, ok in tools.items() if not ok]
        assert not missing, f"{machine.name} missing: {missing}"
