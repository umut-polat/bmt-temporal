"""Stress activities: CPU, memory, fio (4 profiles), sysbench.

These run for minutes; every activity heartbeats through
:func:`bare_metal_tests.ssh.runner.long_run`. The fio activities double as
a disk-safety gate — they refuse to touch a device that isn't listed in
:attr:`MachineSpec.test_disks`, so the boot device (DELLBOSS) is
protected even if the config is wrong.
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
    PHASE_FIO_RAND_READ,
    PHASE_FIO_RAND_WRITE,
    PHASE_FIO_SEQ_READ,
    PHASE_FIO_SEQ_WRITE,
    PHASE_STRESS_CPU,
    PHASE_STRESS_MEMORY,
    PHASE_SYSBENCH_CPU,
    STATUS_FAILED,
    STATUS_PASSED,
)
from bare_metal_tests.models import MachineSpec, Metric, PhaseResult
from bare_metal_tests.ssh.client import SSHError
from bare_metal_tests.ssh.parsers import parse_fio_json, parse_sysbench_cpu
from bare_metal_tests.ssh.runner import long_run


def _now() -> datetime:
    return datetime.now(UTC)


def _result(
    phase: str,
    started: datetime,
    *,
    ok: bool,
    details: dict[str, object],
    metrics: list[Metric] | None = None,
) -> PhaseResult:
    return PhaseResult(
        phase=phase,
        status=STATUS_PASSED if ok else STATUS_FAILED,
        details=details,
        metrics=metrics or [],
        started_at=started,
        finished_at=_now(),
    )


@activity.defn(name="stress_cpu")
async def stress_cpu(
    machine: MachineSpec, duration_seconds: int, workers: int
) -> PhaseResult:
    """Run ``stress-ng --cpu`` on every logical CPU for the configured duration."""
    machine = as_machine(machine)
    started = _now()
    worker_arg = workers if workers > 0 else 0  # 0 = stress-ng default = nproc
    cmd = f"stress-ng --cpu {worker_arg} --metrics --timeout {duration_seconds}s"
    try:
        async with open_ssh(machine, load_secrets()) as ssh:
            result = await long_run(
                ssh,
                cmd,
                label=PHASE_STRESS_CPU,
                timeout=duration_seconds + 60,
                heartbeat=activity.heartbeat,
            )
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on stress_cpu: {exc}", type="SSHTransport"
        ) from exc
    ok = result.ok
    return _result(
        PHASE_STRESS_CPU,
        started,
        ok=ok,
        details={
            "duration_seconds": duration_seconds,
            "tail": result.stdout.strip().splitlines()[-15:],
        },
    )


@activity.defn(name="stress_memory")
async def stress_memory(
    machine: MachineSpec,
    percent_of_total: int,
    duration_seconds: int,
    verify: bool,
) -> PhaseResult:
    """Stress and verify ``percent_of_total`` of the machine's RAM."""
    machine = as_machine(machine)
    started = _now()
    secrets = load_secrets()
    try:
        async with open_ssh(machine, secrets) as ssh:
            mem_kb_result = await ssh.run("awk '/MemTotal:/ {print $2}' /proc/meminfo")
            if not mem_kb_result.ok or not mem_kb_result.stdout.strip():
                return _result(
                    PHASE_STRESS_MEMORY,
                    started,
                    ok=False,
                    details={"reason": "could not read /proc/meminfo"},
                )
            total_kb = int(mem_kb_result.stdout.strip())
            mem_bytes = (total_kb * 1024 * percent_of_total) // 100
            verify_flag = "--verify" if verify else ""
            cmd = (
                f"stress-ng --vm 1 --vm-bytes {mem_bytes} --vm-keep {verify_flag} "
                f"--timeout {duration_seconds}s --metrics"
            )
            result = await long_run(
                ssh,
                cmd,
                label=PHASE_STRESS_MEMORY,
                timeout=duration_seconds + 90,
                heartbeat=activity.heartbeat,
            )
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on stress_memory: {exc}", type="SSHTransport"
        ) from exc
    return _result(
        PHASE_STRESS_MEMORY,
        started,
        ok=result.ok,
        details={
            "duration_seconds": duration_seconds,
            "stressed_bytes": mem_bytes,
            "percent_of_total": percent_of_total,
            "tail": result.stdout.strip().splitlines()[-15:],
        },
    )


def _guard_disk(machine: MachineSpec, target: str) -> None:
    """Refuse to run a destructive fio profile against a non-test disk."""
    if target not in machine.test_disks:
        raise ApplicationError(
            f"disk {target!r} is not in test_disks for {machine.name}; refusing to run fio",
            type="DiskSafetyViolation",
            non_retryable=True,
        )


async def _run_fio_profile(
    phase: str,
    machine: MachineSpec,
    target: str,
    rw: str,
    bs: str,
    iodepth: int,
    size: str,
    runtime_seconds: int,
    ioengine: str,
    direct: int,
) -> PhaseResult:
    started = _now()
    _guard_disk(machine, target)
    cmd = (
        f"fio --name={phase} --filename={target} --rw={rw} --bs={bs} "
        f"--iodepth={iodepth} --size={size} --runtime={runtime_seconds} "
        f"--time_based --ioengine={ioengine} --direct={direct} "
        f"--group_reporting --output-format=json"
    )
    try:
        async with open_ssh(machine, load_secrets()) as ssh:
            result = await long_run(
                ssh,
                cmd,
                label=phase,
                timeout=runtime_seconds + 60,
                heartbeat=activity.heartbeat,
                sudo=True,
            )
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on {phase}: {exc}", type="SSHTransport"
        ) from exc
    if not result.ok or not result.stdout.strip():
        return _result(
            phase,
            started,
            ok=False,
            details={"stderr": result.stderr.strip().splitlines()[-15:]},
        )
    parsed = parse_fio_json(result.stdout)
    direction = "read" if "read" in rw else "write"
    metrics = [
        Metric(name="iops", value=parsed[f"{direction}_iops"], unit="ops/s"),
        Metric(name="bandwidth", value=parsed[f"{direction}_bw_mb_s"], unit="MB/s"),
        Metric(name="latency", value=parsed[f"{direction}_latency_us"], unit="us"),
    ]
    return _result(
        phase,
        started,
        ok=True,
        details={"device": target, "profile": rw, "parsed": parsed},
        metrics=metrics,
    )


@activity.defn(name="fio_seq_read")
async def fio_seq_read(
    machine: MachineSpec,
    target: str,
    bs: str,
    iodepth: int,
    size: str,
    runtime_seconds: int,
    ioengine: str,
    direct: int,
) -> PhaseResult:
    """Sequential read fio profile."""
    machine = as_machine(machine)
    return await _run_fio_profile(
        PHASE_FIO_SEQ_READ,
        machine,
        target,
        "read",
        bs,
        iodepth,
        size,
        runtime_seconds,
        ioengine,
        direct,
    )


@activity.defn(name="fio_seq_write")
async def fio_seq_write(
    machine: MachineSpec,
    target: str,
    bs: str,
    iodepth: int,
    size: str,
    runtime_seconds: int,
    ioengine: str,
    direct: int,
) -> PhaseResult:
    """Sequential write fio profile. Destructive — only on test_disks."""
    machine = as_machine(machine)
    return await _run_fio_profile(
        PHASE_FIO_SEQ_WRITE,
        machine,
        target,
        "write",
        bs,
        iodepth,
        size,
        runtime_seconds,
        ioengine,
        direct,
    )


@activity.defn(name="fio_rand_read")
async def fio_rand_read(
    machine: MachineSpec,
    target: str,
    bs: str,
    iodepth: int,
    size: str,
    runtime_seconds: int,
    ioengine: str,
    direct: int,
) -> PhaseResult:
    """Random read fio profile."""
    machine = as_machine(machine)
    return await _run_fio_profile(
        PHASE_FIO_RAND_READ,
        machine,
        target,
        "randread",
        bs,
        iodepth,
        size,
        runtime_seconds,
        ioengine,
        direct,
    )


@activity.defn(name="fio_rand_write")
async def fio_rand_write(
    machine: MachineSpec,
    target: str,
    bs: str,
    iodepth: int,
    size: str,
    runtime_seconds: int,
    ioengine: str,
    direct: int,
) -> PhaseResult:
    """Random write fio profile. Destructive — only on test_disks."""
    machine = as_machine(machine)
    return await _run_fio_profile(
        PHASE_FIO_RAND_WRITE,
        machine,
        target,
        "randwrite",
        bs,
        iodepth,
        size,
        runtime_seconds,
        ioengine,
        direct,
    )


@activity.defn(name="sysbench_cpu")
async def sysbench_cpu(
    machine: MachineSpec,
    max_prime: int,
    threads: int,
    duration_seconds: int,
) -> PhaseResult:
    """Run sysbench CPU benchmark; record events/sec and average latency."""
    machine = as_machine(machine)
    started = _now()
    thread_arg = threads if threads > 0 else 0  # 0 means "let sysbench decide"
    thread_flag = f"--threads={thread_arg}" if thread_arg else ""
    cmd = (
        f"sysbench cpu --cpu-max-prime={max_prime} "
        f"--time={duration_seconds} {thread_flag} run"
    )
    try:
        async with open_ssh(machine, load_secrets()) as ssh:
            result = await long_run(
                ssh,
                cmd,
                label=PHASE_SYSBENCH_CPU,
                timeout=duration_seconds + 60,
                heartbeat=activity.heartbeat,
            )
    except SSHError as exc:
        raise ApplicationError(
            f"ssh failure on sysbench_cpu: {exc}", type="SSHTransport"
        ) from exc
    if not result.ok:
        return _result(
            PHASE_SYSBENCH_CPU,
            started,
            ok=False,
            details={"stderr": result.stderr.strip().splitlines()[-15:]},
        )
    parsed = parse_sysbench_cpu(result.stdout)
    metrics = [
        Metric(
            name="events_per_second", value=parsed["events_per_second"], unit="events/s"
        ),
        Metric(name="avg_latency", value=parsed["avg_latency_ms"], unit="ms"),
    ]
    return _result(
        PHASE_SYSBENCH_CPU,
        started,
        ok=True,
        details={"max_prime": max_prime, "threads": thread_arg, "parsed": parsed},
        metrics=metrics,
    )


ALL_STRESS_ACTIVITIES: list[Any] = [
    stress_cpu,
    stress_memory,
    fio_seq_read,
    fio_seq_write,
    fio_rand_read,
    fio_rand_write,
    sysbench_cpu,
]
