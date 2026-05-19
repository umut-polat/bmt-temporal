"""VXLAN encapsulation activities.

The encap workflow picks two hosts and runs:

  setup_vxlan(host_a, host_b, peer_ip_a, peer_ip_b, vni)
  iperf3_over_tunnel(...)   # internally runs offload-on then offload-off
  teardown_vxlan(host_a, host_b)

Geneve was dropped from scope; vxlan is enough to satisfy the form's
"CPU offload comparison" item (we toggle ``tx-udp_tnl-segmentation`` on
the underlying NIC and re-measure).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from bare_metal_tests.activities._decode import as_machine
from bare_metal_tests.activities._ssh import open_ssh
from bare_metal_tests.config import load_secrets
from bare_metal_tests.constants import (
    PHASE_VXLAN_IPERF,
    PHASE_VXLAN_SETUP,
    PHASE_VXLAN_TEARDOWN,
    STATUS_FAILED,
    STATUS_PASSED,
)
from bare_metal_tests.models import MachineSpec, Metric, PhaseResult
from bare_metal_tests.ssh.client import SSHClient, SSHError
from bare_metal_tests.ssh.parsers import parse_iperf3_json
from bare_metal_tests.ssh.runner import long_run

_VXLAN_NAME = "bmtvxlan0"
_VXLAN_PORT = 4789
_TUNNEL_NET = "10.99.0"


def _now() -> datetime:
    return datetime.now(UTC)


def _tunnel_ip(index: int) -> str:
    return f"{_TUNNEL_NET}.{index}"


async def _bring_up_vxlan(
    ssh: SSHClient,
    machine: MachineSpec,
    local_index: int,
    peer_host: str,
    vni: int,
) -> None:
    """Create and configure a VXLAN device on the SSH session."""
    cmds = [
        "modprobe vxlan",
        f"ip link del {_VXLAN_NAME} 2>/dev/null; true",
        (
            f"ip link add {_VXLAN_NAME} type vxlan id {vni} dev {machine.mesh_iface} "
            f"remote {peer_host} dstport {_VXLAN_PORT}"
        ),
        f"ip addr add {_tunnel_ip(local_index)}/24 dev {_VXLAN_NAME}",
        f"ip link set {_VXLAN_NAME} up",
    ]
    for c in cmds:
        r = await ssh.run_sudo(c)
        if not r.ok and "already exists" not in r.stderr:
            raise ApplicationError(
                f"vxlan setup step failed on {machine.name}: {c} → {r.stderr.strip()}",
                type="VxlanSetup",
                non_retryable=True,
            )


@activity.defn(name="setup_vxlan")
async def setup_vxlan(a: MachineSpec, b: MachineSpec, vni: int = 4242) -> PhaseResult:
    """Bring up a VXLAN tunnel between ``a`` and ``b``."""
    a = as_machine(a)
    b = as_machine(b)
    started = _now()
    secrets = load_secrets()
    try:
        async with open_ssh(a, secrets) as ssh_a:
            await _bring_up_vxlan(ssh_a, a, 1, b.host, vni)
        async with open_ssh(b, secrets) as ssh_b:
            await _bring_up_vxlan(ssh_b, b, 2, a.host, vni)
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on setup_vxlan: {exc}", type="SSHTransport"
        ) from exc
    return PhaseResult(
        phase=PHASE_VXLAN_SETUP,
        status=STATUS_PASSED,
        details={
            "host_a": a.name,
            "host_b": b.name,
            "vni": vni,
            "tunnel_ip_a": _tunnel_ip(1),
            "tunnel_ip_b": _tunnel_ip(2),
        },
        started_at=started,
        finished_at=_now(),
    )


async def _set_offload(ssh: SSHClient, iface: str, on: bool) -> None:
    """Toggle the NIC's UDP-tunnel TX segmentation offload, best-effort."""
    state = "on" if on else "off"
    r = await ssh.run_sudo(f"ethtool -K {iface} tx-udp_tnl-segmentation {state}")
    _ = r


async def _measure_tunnel(
    server: MachineSpec,
    client: MachineSpec,
    duration: int,
    offload_on: bool,
) -> dict[str, Any]:
    secrets = load_secrets()
    async with open_ssh(server, secrets) as srv:
        await _set_offload(srv, server.mesh_iface, offload_on)
    async with open_ssh(client, secrets) as cli:
        await _set_offload(cli, client.mesh_iface, offload_on)

    async def _server_side() -> None:
        async with open_ssh(server, secrets) as ssh:
            await long_run(
                ssh,
                f"iperf3 -s -1 -p 5202 --bind {_tunnel_ip(1)}",
                label="vxlan_iperf3_server",
                timeout=duration + 30,
                heartbeat=activity.heartbeat,
            )

    srv_task = asyncio.create_task(_server_side())
    await asyncio.sleep(1)
    try:
        async with open_ssh(client, secrets) as ssh:
            r = await long_run(
                ssh,
                f"iperf3 -c {_tunnel_ip(1)} -p 5202 -t {duration} --json",
                label="vxlan_iperf3_client",
                timeout=duration + 30,
                heartbeat=activity.heartbeat,
            )
    finally:
        try:
            await asyncio.wait_for(srv_task, timeout=duration + 30)
        except TimeoutError:
            srv_task.cancel()
    if not r.ok or not r.stdout.strip():
        return {"ok": False, "stderr": r.stderr.strip().splitlines()[-8:]}
    parsed = parse_iperf3_json(r.stdout)
    return {"ok": True, **parsed}


@activity.defn(name="iperf3_over_tunnel")
async def iperf3_over_tunnel(
    server: MachineSpec,
    client: MachineSpec,
    duration_seconds: int,
    compare_offload: bool,
) -> PhaseResult:
    """Measure VXLAN throughput; if ``compare_offload`` is true also re-measure with offload off."""
    server = as_machine(server)
    client = as_machine(client)
    started = _now()
    on = await _measure_tunnel(server, client, duration_seconds, True)
    off = (
        await _measure_tunnel(server, client, duration_seconds, False)
        if compare_offload
        else {"ok": True, "skipped": True}
    )
    ok = bool(on.get("ok")) and bool(off.get("ok"))
    on_gbps = float(on.get("gbps", 0.0))
    off_gbps = float(off.get("gbps", 0.0))
    delta_pct = ((on_gbps - off_gbps) / off_gbps * 100.0) if off_gbps else 0.0
    metrics = [
        Metric(name="offload_on_gbps", value=on_gbps, unit="Gb/s"),
        Metric(name="offload_off_gbps", value=off_gbps, unit="Gb/s"),
        Metric(name="offload_delta_pct", value=delta_pct, unit="%"),
    ]
    return PhaseResult(
        phase=PHASE_VXLAN_IPERF,
        status=STATUS_PASSED if ok else STATUS_FAILED,
        details={"on": on, "off": off, "delta_pct": delta_pct},
        metrics=metrics,
        started_at=started,
        finished_at=_now(),
    )


@activity.defn(name="teardown_vxlan")
async def teardown_vxlan(a: MachineSpec, b: MachineSpec) -> PhaseResult:
    """Remove the VXLAN device on both hosts. Best-effort."""
    a = as_machine(a)
    b = as_machine(b)
    started = _now()
    secrets = load_secrets()
    details: dict[str, object] = {}
    try:
        for machine in (a, b):
            async with open_ssh(machine, secrets) as ssh:
                r = await ssh.run_sudo(f"ip link del {_VXLAN_NAME}")
                details[machine.name] = {
                    "exit": r.exit_status,
                    "stderr": r.stderr.strip(),
                }
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on teardown_vxlan: {exc}", type="SSHTransport"
        ) from exc
    return PhaseResult(
        phase=PHASE_VXLAN_TEARDOWN,
        status=STATUS_PASSED,
        details=details,
        started_at=started,
        finished_at=_now(),
    )


ALL_ENCAP_ACTIVITIES: list[Any] = [setup_vxlan, iperf3_over_tunnel, teardown_vxlan]
