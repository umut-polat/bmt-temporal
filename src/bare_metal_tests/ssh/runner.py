"""Long-running command helper with Temporal heartbeat support.

Stress / fio / iperf3 commands can run for minutes; Temporal will kill an
activity that goes silent for longer than its ``heartbeat_timeout``. The
runner spawns an async heartbeat task in the background while the remote
command executes, then cleans it up.

Activities that don't run inside the Temporal worker (the CLI smoke
helpers, tests) pass ``heartbeat=None`` and pay no cost.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import Any

from bare_metal_tests.ssh.client import CommandResult, SSHClient

HeartbeatFn = Callable[[dict[str, Any]], None]


async def _pump(heartbeat: HeartbeatFn, interval: float, label: str) -> None:
    elapsed = 0
    while True:
        await asyncio.sleep(interval)
        elapsed += int(interval)
        heartbeat({"phase": label, "elapsed_seconds": elapsed})


async def long_run(
    client: SSHClient,
    command: str,
    *,
    label: str,
    timeout: float,
    heartbeat: HeartbeatFn | None = None,
    heartbeat_interval: float = 15.0,
    sudo: bool = False,
) -> CommandResult:
    """Run a long command, pumping heartbeats while it executes.

    Args:
        client: An already-connected :class:`SSHClient`.
        command: The shell command to execute remotely.
        label: Human-readable label included in each heartbeat payload.
        timeout: Hard wall-clock limit for the command, in seconds.
        heartbeat: Callable to invoke periodically; usually
            ``temporalio.activity.heartbeat``.
        heartbeat_interval: Seconds between heartbeats; defaults to 15s
            which is comfortable for the 60s heartbeat_timeout the
            workflows configure.
        sudo: Run the command through :meth:`SSHClient.run_sudo`.

    Returns:
        :class:`CommandResult` from the remote command.
    """
    runner: Callable[..., Awaitable[CommandResult]] = (
        client.run_sudo if sudo else client.run
    )
    if heartbeat is None:
        return await runner(command, timeout=timeout)


    pump = asyncio.create_task(_pump(heartbeat, heartbeat_interval, label))
    try:
        return await runner(command, timeout=timeout)
    finally:
        pump.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pump
