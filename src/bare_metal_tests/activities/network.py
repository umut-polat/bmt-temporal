"""Network mesh activities: MTU verification, jumbo-frame check, iperf3.

The mesh workflow drives this module: it schedules pairs round-robin so
each host acts as iperf3 server in one round and client in another,
without saturating a NIC by running multiple flows on the same direction
in parallel.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from bare_metal_tests.activities._decode import as_machine
from bare_metal_tests.activities._ssh import open_ssh
from bare_metal_tests.config import load_secrets
from bare_metal_tests.constants import (
    PHASE_IPERF3_PAIR,
    PHASE_VERIFY_JUMBO,
    PHASE_VERIFY_MTU,
    STATUS_FAILED,
    STATUS_PASSED,
)
from bare_metal_tests.models import MachineSpec, Metric, PhaseResult
from bare_metal_tests.ssh.client import SSHError
from bare_metal_tests.ssh.parsers import parse_iperf3_json
from bare_metal_tests.ssh.runner import long_run

_IPERF3_PORT = 5201


def _now() -> datetime:
    return datetime.now(UTC)


@activity.defn(name="verify_mtu")
async def verify_mtu(machine: MachineSpec, expected_mtu: int) -> PhaseResult:
    """Read the MTU of ``mesh_iface`` and compare against ``expected_mtu``."""
    machine = as_machine(machine)
    started = _now()
    try:
        async with open_ssh(machine, load_secrets()) as ssh:
            result = await ssh.run(f"ip -j link show {machine.mesh_iface}")
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on verify_mtu: {exc}", type="SSHTransport"
        ) from exc
    if not result.ok:
        return PhaseResult(
            phase=PHASE_VERIFY_MTU,
            status=STATUS_FAILED,
            details={"reason": result.stderr.strip()},
            started_at=started,
            finished_at=_now(),
        )
    data = json.loads(result.stdout)
    mtu = data[0].get("mtu", 0) if data else 0
    ok = mtu == expected_mtu
    return PhaseResult(
        phase=PHASE_VERIFY_MTU,
        status=STATUS_PASSED if ok else STATUS_FAILED,
        details={"iface": machine.mesh_iface, "mtu": mtu, "expected": expected_mtu},
        started_at=started,
        finished_at=_now(),
    )


@activity.defn(name="verify_jumbo_frames")
async def verify_jumbo_frames(
    machine: MachineSpec, peer_ip: str, payload_bytes: int = 8972
) -> PhaseResult:
    """Send a DF-set ping with a jumbo payload to ``peer_ip``."""
    machine = as_machine(machine)
    started = _now()
    cmd = f"ping -M do -c 3 -s {payload_bytes} {peer_ip}"
    try:
        async with open_ssh(machine, load_secrets()) as ssh:
            result = await ssh.run(cmd, timeout=15)
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on verify_jumbo_frames: {exc}", type="SSHTransport"
        ) from exc
    ok = result.ok
    return PhaseResult(
        phase=PHASE_VERIFY_JUMBO,
        status=STATUS_PASSED if ok else STATUS_FAILED,
        details={
            "peer_ip": peer_ip,
            "payload_bytes": payload_bytes,
            "exit": result.exit_status,
            "stdout_tail": result.stdout.strip().splitlines()[-5:],
        },
        started_at=started,
        finished_at=_now(),
    )


@activity.defn(name="iperf3_pair")
async def iperf3_pair(
    server: MachineSpec,
    client: MachineSpec,
    duration_seconds: int,
    parallel_streams: int,
) -> PhaseResult:
    """Measure throughput from ``client`` to ``server`` over their mesh NICs.

    Starts an ephemeral one-shot iperf3 server on ``server`` (``-1`` exits
    after the first client disconnects) and runs the client. Both sides
    are torn down on completion or error.
    """
    server = as_machine(server)
    client = as_machine(client)
    started = _now()
    secrets = load_secrets()
    server_cmd = f"iperf3 -s -1 -p {_IPERF3_PORT}"
    client_cmd = (
        f"iperf3 -c {server.host} -p {_IPERF3_PORT} -t {duration_seconds} "
        f"-P {parallel_streams} --json"
    )

    async def _server_side() -> None:
        async with open_ssh(server, secrets) as ssh:
            await long_run(
                ssh,
                server_cmd,
                label=f"{PHASE_IPERF3_PAIR}_server",
                timeout=duration_seconds + 30,
                heartbeat=activity.heartbeat,
            )

    server_task = asyncio.create_task(_server_side())
    await asyncio.sleep(1)  # give the server a moment to bind

    try:
        async with open_ssh(client, secrets) as ssh:
            result = await long_run(
                ssh,
                client_cmd,
                label=f"{PHASE_IPERF3_PAIR}_client",
                timeout=duration_seconds + 30,
                heartbeat=activity.heartbeat,
            )
    except SSHError as exc:
        server_task.cancel()
        raise ApplicationError(
            f"ssh failure on iperf3 client: {exc}", type="SSHTransport"
        ) from exc
    finally:
        try:
            await asyncio.wait_for(server_task, timeout=duration_seconds + 30)
        except TimeoutError:
            server_task.cancel()

    if not result.ok or not result.stdout.strip():
        return PhaseResult(
            phase=PHASE_IPERF3_PAIR,
            status=STATUS_FAILED,
            details={
                "server": server.name,
                "client": client.name,
                "stderr": result.stderr.strip().splitlines()[-10:],
            },
            started_at=started,
            finished_at=_now(),
        )
    parsed = parse_iperf3_json(result.stdout)
    metrics = [
        Metric(name="throughput_gbps", value=parsed["gbps"], unit="Gb/s"),
        Metric(name="retransmits", value=float(parsed["retransmits"]), unit="count"),
    ]
    return PhaseResult(
        phase=PHASE_IPERF3_PAIR,
        status=STATUS_PASSED,
        details={"server": server.name, "client": client.name, "parsed": parsed},
        metrics=metrics,
        started_at=started,
        finished_at=_now(),
    )


ALL_NETWORK_ACTIVITIES: list[Any] = [verify_mtu, verify_jumbo_frames, iperf3_pair]
