"""Async SSH client with a single sudo helper.

The three target hosts authenticate differently — one with a password, the
other two with a key — so the client takes a :class:`MachineSpec` plus a
pair of secrets and figures out the right ``asyncssh`` arguments.

The class is intentionally tiny: ``run`` for normal commands and
``run_sudo`` for privileged commands. Both return a :class:`CommandResult`
and raise :class:`SSHError` when something goes wrong at the SSH layer.
The caller decides whether a non-zero exit is fatal.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncssh

from bare_metal_tests.constants import AUTH_KEY, AUTH_PASSWORD
from bare_metal_tests.models import MachineSpec


class SSHError(RuntimeError):
    """Raised on connection or transport-level failures."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Outcome of running a single command on a remote host."""

    exit_status: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.exit_status == 0


def _connect_kwargs(
    machine: MachineSpec,
    ssh_password: str | None,
    key_path: str | None,
    connect_timeout: int,
) -> dict[str, Any]:
    """Translate a :class:`MachineSpec` into ``asyncssh.connect`` kwargs."""
    kwargs: dict[str, Any] = {
        "host": machine.host,
        "username": machine.user,
        "known_hosts": None,
        "connect_timeout": connect_timeout,
    }
    if machine.auth == AUTH_PASSWORD:
        if not ssh_password:
            raise SSHError(
                f"machine {machine.name} needs a password but none was supplied"
            )
        kwargs["password"] = ssh_password
        kwargs["preferred_auth"] = "password,keyboard-interactive"
    elif machine.auth == AUTH_KEY:
        resolved = Path(os.path.expanduser(key_path or "~/.ssh/id_rsa")).resolve()
        if not resolved.exists():
            raise SSHError(f"ssh key not found at {resolved}")
        kwargs["client_keys"] = [str(resolved)]
    else:
        raise SSHError(f"unknown auth type: {machine.auth}")
    return kwargs


class SSHClient:
    """Async context manager wrapping a single asyncssh connection."""

    def __init__(
        self,
        machine: MachineSpec,
        sudo_password: str,
        ssh_password: str | None = None,
        key_path: str | None = None,
        connect_timeout: int = 15,
    ) -> None:
        self._machine = machine
        self._sudo_password = sudo_password
        self._kwargs = _connect_kwargs(machine, ssh_password, key_path, connect_timeout)
        self._conn: asyncssh.SSHClientConnection | None = None

    async def __aenter__(self) -> SSHClient:
        await self.connect()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def connect(self) -> None:
        """Open the underlying asyncssh connection."""
        try:
            self._conn = await asyncssh.connect(**self._kwargs)
        except (OSError, asyncssh.Error) as exc:
            raise SSHError(f"connect failed for {self._machine.name}: {exc}") from exc

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            await self._conn.wait_closed()
            self._conn = None

    async def run(self, command: str, *, timeout: float | None = None) -> CommandResult:
        """Run ``command`` and return its result without judging the exit code."""
        if self._conn is None:
            raise SSHError("client is not connected; call connect() first")
        try:
            proc = await self._conn.run(command, check=False, timeout=timeout)
        except asyncssh.Error as exc:
            raise SSHError(f"transport error running {command!r}: {exc}") from exc
        return CommandResult(
            exit_status=proc.exit_status or 0,
            stdout=str(proc.stdout or ""),
            stderr=str(proc.stderr or ""),
        )

    async def run_sudo(
        self, command: str, *, timeout: float | None = None
    ) -> CommandResult:
        """Run ``command`` under sudo, piping the password into stdin.

        ``sudo -S -p ''`` reads the password from stdin and suppresses the
        prompt. We prefix the password followed by a newline; the rest of
        the stream is the command's own input (empty here).
        """
        if self._conn is None:
            raise SSHError("client is not connected; call connect() first")
        wrapped = f"sudo -S -p '' {command}"
        try:
            proc = await self._conn.run(
                wrapped,
                input=f"{self._sudo_password}\n",
                check=False,
                timeout=timeout,
            )
        except asyncssh.Error as exc:
            raise SSHError(f"transport error running sudo {command!r}: {exc}") from exc
        return CommandResult(
            exit_status=proc.exit_status or 0,
            stdout=str(proc.stdout or ""),
            stderr=str(proc.stderr or ""),
        )
