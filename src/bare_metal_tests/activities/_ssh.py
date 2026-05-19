"""Shared SSH helper for activities.

Activities are stateless and re-entrant. Each invocation creates a fresh
:class:`SSHClient`, runs its work and closes the connection. asyncssh is
cheap enough that pooling buys us nothing on a per-host basis and keeps
the activity boundary clean.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from bare_metal_tests.config import ssh_password_env_key
from bare_metal_tests.constants import AUTH_PASSWORD
from bare_metal_tests.models import MachineSpec
from bare_metal_tests.ssh.client import SSHClient


@asynccontextmanager
async def open_ssh(
    machine: MachineSpec | dict[str, Any],
    secrets: dict[str, str],
    *,
    connect_timeout: int = 15,
) -> AsyncIterator[SSHClient]:
    """Yield a connected :class:`SSHClient` for ``machine``.

    Accepts a plain dict as well because Temporal's default converter may
    deserialise into one when the activity signature isn't reflected.
    """
    if isinstance(machine, dict):
        machine = MachineSpec.model_validate(machine)
    ssh_password = (
        secrets.get(ssh_password_env_key(machine.name))
        if machine.auth == AUTH_PASSWORD
        else None
    )
    client = SSHClient(
        machine=machine,
        sudo_password=secrets["SUDO_PASSWORD"],
        ssh_password=ssh_password,
        key_path=secrets.get("SSH_KEY_PATH"),
        connect_timeout=connect_timeout,
    )
    await client.connect()
    try:
        yield client
    finally:
        await client.close()
