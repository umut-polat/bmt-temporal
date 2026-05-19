"""Hardware discovery activities.

Each activity probes one facet of the host: CPU, memory modules, disks
with SMART, NIC details, NUMA topology, LLDP neighbours, BIOS/firmware.
They are pure (no installation), fast, and safe to run in parallel from
the parent workflow.

A probe never raises on a bad command exit; it returns a ``failed``
:class:`PhaseResult` so the report layer can show which probe didn't work
without losing the rest of the picture.
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
    PHASE_HW_BIOS,
    PHASE_HW_CPU,
    PHASE_HW_DISKS,
    PHASE_HW_LLDP,
    PHASE_HW_MEMORY,
    PHASE_HW_NICS,
    PHASE_HW_NUMA,
    STATUS_FAILED,
    STATUS_PASSED,
)
from bare_metal_tests.models import MachineSpec, PhaseResult
from bare_metal_tests.ssh.client import SSHError
from bare_metal_tests.ssh.parsers import (
    parse_dmidecode_memory,
    parse_ethtool,
    parse_ip_link_json,
    parse_lldpctl_json,
    parse_lscpu,
    parse_smartctl_json,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _failed(phase: str, started: datetime, reason: str) -> PhaseResult:
    return PhaseResult(
        phase=phase,
        status=STATUS_FAILED,
        details={"reason": reason},
        started_at=started,
        finished_at=_now(),
    )


def _passed(phase: str, started: datetime, details: dict[str, object]) -> PhaseResult:
    return PhaseResult(
        phase=phase,
        status=STATUS_PASSED,
        details=details,
        started_at=started,
        finished_at=_now(),
    )


@activity.defn(name="probe_cpu")
async def probe_cpu(machine: MachineSpec) -> PhaseResult:
    """Collect CPU model, sockets, cores, threads via ``lscpu``."""
    machine = as_machine(machine)
    started = _now()
    try:
        async with open_ssh(machine, load_secrets()) as ssh:
            result = await ssh.run("lscpu")
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on probe_cpu: {exc}", type="SSHTransport"
        ) from exc
    if not result.ok:
        return _failed(PHASE_HW_CPU, started, result.stderr.strip() or "lscpu failed")
    return _passed(PHASE_HW_CPU, started, parse_lscpu(result.stdout))


@activity.defn(name="probe_memory_ecc")
async def probe_memory_ecc(machine: MachineSpec) -> PhaseResult:
    """Collect populated DIMM modules and ECC capability via ``dmidecode``."""
    machine = as_machine(machine)
    started = _now()
    try:
        async with open_ssh(machine, load_secrets()) as ssh:
            result = await ssh.run_sudo("dmidecode -t memory")
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on probe_memory_ecc: {exc}", type="SSHTransport"
        ) from exc
    if not result.ok:
        return _failed(
            PHASE_HW_MEMORY, started, result.stderr.strip() or "dmidecode failed"
        )
    modules = parse_dmidecode_memory(result.stdout)
    sizes = [m.get("Size", "") for m in modules]
    return _passed(
        PHASE_HW_MEMORY,
        started,
        {"modules": modules, "module_count": len(modules), "sizes": sizes},
    )


@activity.defn(name="probe_disks_smart")
async def probe_disks_smart(machine: MachineSpec) -> PhaseResult:
    """Read SMART data for every test disk on the machine."""
    machine = as_machine(machine)
    started = _now()
    disks: list[dict[str, object]] = []
    try:
        async with open_ssh(machine, load_secrets()) as ssh:
            for disk in machine.test_disks:
                result = await ssh.run_sudo(f"smartctl -a -j {disk}")
                if not result.ok and not result.stdout.strip():
                    disks.append({"device": disk, "error": result.stderr.strip()})
                    continue
                disks.append({"device": disk, **parse_smartctl_json(result.stdout)})
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on probe_disks_smart: {exc}", type="SSHTransport"
        ) from exc

    any_failed = any(d.get("passed") is False for d in disks if "passed" in d)
    status = STATUS_FAILED if any_failed else STATUS_PASSED
    return PhaseResult(
        phase=PHASE_HW_DISKS,
        status=status,
        details={"disks": disks},
        started_at=started,
        finished_at=_now(),
    )


@activity.defn(name="probe_nics")
async def probe_nics(machine: MachineSpec) -> PhaseResult:
    """Enumerate NICs, capture speed/driver/firmware via ``ip`` and ``ethtool``."""
    machine = as_machine(machine)
    started = _now()
    try:
        async with open_ssh(machine, load_secrets()) as ssh:
            link_result = await ssh.run("ip -j link show")
            if not link_result.ok:
                return _failed(PHASE_HW_NICS, started, link_result.stderr.strip())
            links = parse_ip_link_json(link_result.stdout)
            nics: list[dict[str, object]] = []
            for link in links:
                name = link.get("ifname", "")
                if not name or name == "lo":
                    continue
                eth_result = await ssh.run(f"ethtool {name}")
                drv_result = await ssh.run(f"ethtool -i {name}")
                ring_state = link.get("operstate", "")
                nics.append(
                    {
                        "name": name,
                        "mac": link.get("address", ""),
                        "operstate": ring_state,
                        "details": (
                            parse_ethtool(eth_result.stdout) if eth_result.ok else {}
                        ),
                        "driver": drv_result.stdout if drv_result.ok else "",
                    }
                )
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on probe_nics: {exc}", type="SSHTransport"
        ) from exc
    return _passed(PHASE_HW_NICS, started, {"interfaces": nics, "count": len(nics)})


@activity.defn(name="probe_numa")
async def probe_numa(machine: MachineSpec) -> PhaseResult:
    """Capture NUMA topology with ``numactl --hardware``."""
    machine = as_machine(machine)
    started = _now()
    try:
        async with open_ssh(machine, load_secrets()) as ssh:
            result = await ssh.run("numactl --hardware")
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on probe_numa: {exc}", type="SSHTransport"
        ) from exc
    if not result.ok:
        return _failed(
            PHASE_HW_NUMA, started, result.stderr.strip() or "numactl failed"
        )
    return _passed(PHASE_HW_NUMA, started, {"raw": result.stdout.strip()})


@activity.defn(name="probe_lldp")
async def probe_lldp(machine: MachineSpec) -> PhaseResult:
    """Read LLDP neighbour information. ``lldpd`` must be running."""
    machine = as_machine(machine)
    started = _now()
    try:
        async with open_ssh(machine, load_secrets()) as ssh:
            await ssh.run_sudo("systemctl start lldpd")
            result = await ssh.run("lldpctl -f json")
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on probe_lldp: {exc}", type="SSHTransport"
        ) from exc
    if not result.ok:
        return _failed(
            PHASE_HW_LLDP, started, result.stderr.strip() or "lldpctl failed"
        )
    return _passed(PHASE_HW_LLDP, started, parse_lldpctl_json(result.stdout))


@activity.defn(name="probe_bios_firmware")
async def probe_bios_firmware(machine: MachineSpec) -> PhaseResult:
    """Collect BIOS vendor/version/date and chassis info."""
    machine = as_machine(machine)
    started = _now()
    try:
        async with open_ssh(machine, load_secrets()) as ssh:
            bios = await ssh.run_sudo("dmidecode -t bios")
            system = await ssh.run_sudo("dmidecode -t system")
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on probe_bios_firmware: {exc}", type="SSHTransport"
        ) from exc
    if not bios.ok or not system.ok:
        return _failed(PHASE_HW_BIOS, started, "dmidecode -t bios/system failed")

    def _extract(text: str, key: str) -> str:
        for line in text.splitlines():
            if key in line and ":" in line:
                return line.split(":", 1)[1].strip()
        return ""

    return _passed(
        PHASE_HW_BIOS,
        started,
        {
            "bios_vendor": _extract(bios.stdout, "Vendor"),
            "bios_version": _extract(bios.stdout, "Version"),
            "bios_release_date": _extract(bios.stdout, "Release Date"),
            "system_manufacturer": _extract(system.stdout, "Manufacturer"),
            "system_product": _extract(system.stdout, "Product Name"),
            "system_serial": _extract(system.stdout, "Serial Number"),
        },
    )


ALL_HARDWARE_ACTIVITIES: list[Any] = [
    probe_cpu,
    probe_memory_ecc,
    probe_disks_smart,
    probe_nics,
    probe_numa,
    probe_lldp,
    probe_bios_firmware,
]
