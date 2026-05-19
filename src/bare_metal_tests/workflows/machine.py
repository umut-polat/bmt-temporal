"""MachineWorkflow: run all per-host phases for one machine.

Workflows are deterministic. Anything that touches the network, the disk,
or the clock outside of ``workflow.*`` lives in an activity. Each phase
group runs its activities in parallel via :func:`asyncio.gather`.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from bare_metal_tests.activities.persist import MachineSink
    from bare_metal_tests.constants import (
        PHASE_FIO_RAND_READ,
        PHASE_FIO_RAND_WRITE,
        PHASE_FIO_SEQ_READ,
        PHASE_FIO_SEQ_WRITE,
        STATUS_FAILED,
        STATUS_PASSED,
    )
    from bare_metal_tests.models import MachineResult, MachineSpec, PhaseResult


HARDWARE_PROBES = [
    "probe_cpu",
    "probe_memory_ecc",
    "probe_disks_smart",
    "probe_nics",
    "probe_numa",
    "probe_lldp",
    "probe_bios_firmware",
]

_FIO_NAME_TO_ACTIVITY = {
    "seq_read": "fio_seq_read",
    "seq_write": "fio_seq_write",
    "rand_read": "fio_rand_read",
    "rand_write": "fio_rand_write",
}

_FIO_NAME_TO_PHASE = {
    "seq_read": PHASE_FIO_SEQ_READ,
    "seq_write": PHASE_FIO_SEQ_WRITE,
    "rand_read": PHASE_FIO_RAND_READ,
    "rand_write": PHASE_FIO_RAND_WRITE,
}


@dataclass
class FioProfileSpec:
    """Single fio profile passed from the CLI to a workflow."""

    name: str
    rw: str
    bs: str
    iodepth: int


@dataclass
class StressSpec:
    """All stress-phase tunables in one bag."""

    cpu_duration_seconds: int
    cpu_workers: int
    memory_percent_of_total: int
    memory_duration_seconds: int
    memory_verify: bool
    fio_profiles: list[FioProfileSpec]
    fio_size: str
    fio_runtime_seconds: int
    fio_ioengine: str
    fio_direct: int
    sysbench_max_prime: int
    sysbench_threads: int
    sysbench_duration_seconds: int


@dataclass
class MachinePlan:
    """Inputs the workflow expects from its parent or the CLI."""

    machine: MachineSpec
    apt_packages: list[str]
    phases: list[str]
    timeouts_seconds: dict[str, int]
    retry_initial_seconds: int
    retry_backoff: float
    retry_max_attempts: int
    stress: StressSpec | None = None
    batch_id: str | None = None


def _retry(plan: MachinePlan) -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(seconds=plan.retry_initial_seconds),
        backoff_coefficient=plan.retry_backoff,
        maximum_attempts=plan.retry_max_attempts,
    )


@workflow.defn
class MachineWorkflow:
    """Run the configured phases against a single host, in order.

    Phase groups (setup → hardware → stress) execute sequentially, but
    every activity inside a group runs in parallel. A failure inside one
    group does not skip later groups — we want as much data as possible
    even when one host has a sick disk or a flaky NIC.
    """

    @workflow.run
    async def run(self, plan: MachinePlan) -> MachineResult:
        started = workflow.now()
        results: list[PhaseResult] = []
        retry = _retry(plan)
        setup_timeout = timedelta(seconds=plan.timeouts_seconds["setup"])
        hw_timeout = timedelta(seconds=plan.timeouts_seconds["hardware_probe"])

        if "setup" in plan.phases:
            ssh = await workflow.execute_activity(
                "ssh_check",
                plan.machine,
                result_type=PhaseResult,
                start_to_close_timeout=setup_timeout,
                retry_policy=retry,
            )
            results.append(ssh)
            if ssh.status == STATUS_PASSED:
                apt = await workflow.execute_activity(
                    "apt_install",
                    args=[plan.machine, plan.apt_packages],
                    result_type=PhaseResult,
                    start_to_close_timeout=setup_timeout,
                    heartbeat_timeout=timedelta(seconds=60),
                    retry_policy=retry,
                )
                results.append(apt)

        if "hardware" in plan.phases:
            probes = [
                workflow.execute_activity(
                    name,
                    plan.machine,
                    result_type=PhaseResult,
                    start_to_close_timeout=hw_timeout,
                    retry_policy=retry,
                )
                for name in HARDWARE_PROBES
            ]
            hw_results = await asyncio.gather(*probes)
            results.extend(hw_results)

        if "stress" in plan.phases and plan.stress is not None:
            results.extend(await self._run_stress(plan, retry))

        finished = workflow.now()
        overall = (
            STATUS_PASSED
            if all(r.status == STATUS_PASSED for r in results)
            else STATUS_FAILED
        )
        machine_result = MachineResult(
            machine=plan.machine,
            status=overall,
            started_at=started,
            finished_at=finished,
            phases=results,
        )

        if plan.batch_id is not None:
            await workflow.execute_activity(
                "persist_machine_results",
                MachineSink(batch_id=plan.batch_id, machine_result=machine_result),
                start_to_close_timeout=timedelta(
                    seconds=plan.timeouts_seconds["persist_results"]
                ),
                retry_policy=retry,
            )

        return machine_result

    async def _run_stress(
        self, plan: MachinePlan, retry: RetryPolicy
    ) -> list[PhaseResult]:
        stress = plan.stress
        assert stress is not None  # guarded by caller
        machine = plan.machine
        timeouts = plan.timeouts_seconds

        coros = [
            workflow.execute_activity(
                "stress_cpu",
                args=[machine, stress.cpu_duration_seconds, stress.cpu_workers],
                result_type=PhaseResult,
                start_to_close_timeout=timedelta(seconds=timeouts["stress_cpu"]),
                heartbeat_timeout=timedelta(seconds=60),
                retry_policy=retry,
            ),
            workflow.execute_activity(
                "stress_memory",
                args=[
                    machine,
                    stress.memory_percent_of_total,
                    stress.memory_duration_seconds,
                    stress.memory_verify,
                ],
                result_type=PhaseResult,
                start_to_close_timeout=timedelta(seconds=timeouts["stress_memory"]),
                heartbeat_timeout=timedelta(seconds=60),
                retry_policy=retry,
            ),
            workflow.execute_activity(
                "sysbench_cpu",
                args=[
                    machine,
                    stress.sysbench_max_prime,
                    stress.sysbench_threads,
                    stress.sysbench_duration_seconds,
                ],
                result_type=PhaseResult,
                start_to_close_timeout=timedelta(seconds=timeouts["sysbench_cpu"]),
                heartbeat_timeout=timedelta(seconds=60),
                retry_policy=retry,
            ),
        ]

        for profile in stress.fio_profiles:
            target = machine.test_disks[0]
            coros.append(
                workflow.execute_activity(
                    _FIO_NAME_TO_ACTIVITY[profile.name],
                    args=[
                        machine,
                        target,
                        profile.bs,
                        profile.iodepth,
                        stress.fio_size,
                        stress.fio_runtime_seconds,
                        stress.fio_ioengine,
                        stress.fio_direct,
                    ],
                    result_type=PhaseResult,
                    start_to_close_timeout=timedelta(
                        seconds=timeouts["stress_disk_fio"]
                    ),
                    heartbeat_timeout=timedelta(seconds=60),
                    retry_policy=retry,
                )
            )

        return list(await asyncio.gather(*coros))
