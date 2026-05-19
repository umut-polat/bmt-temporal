"""Workflow control-flow tests using Temporal's time-skipping environment.

We replace the real SSH-driven activities with deterministic fakes so the
test exercises the workflow logic (phase ordering, fail propagation,
asyncio.gather fan-out) without touching the network.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from bare_metal_tests.constants import (
    AUTH_KEY,
    PHASE_APT_INSTALL,
    PHASE_HW_BIOS,
    PHASE_HW_CPU,
    PHASE_HW_DISKS,
    PHASE_HW_LLDP,
    PHASE_HW_MEMORY,
    PHASE_HW_NICS,
    PHASE_HW_NUMA,
    PHASE_SSH_CHECK,
    STATUS_PASSED,
)
from bare_metal_tests.models import MachineSpec, PhaseResult
from bare_metal_tests.temporal_data import DATA_CONVERTER
from bare_metal_tests.workflows.machine import MachinePlan, MachineWorkflow


def _now() -> datetime:
    return datetime.now(UTC)


def _ok(phase: str) -> PhaseResult:
    t = _now()
    return PhaseResult(
        phase=phase, status=STATUS_PASSED, details={}, started_at=t, finished_at=t
    )


@activity.defn(name="ssh_check")
async def fake_ssh_check(machine: MachineSpec) -> PhaseResult:
    return _ok(PHASE_SSH_CHECK)


@activity.defn(name="apt_install")
async def fake_apt_install(
    machine: MachineSpec, packages: list[str], mesh_mtu: int = 9000
) -> PhaseResult:
    return _ok(PHASE_APT_INSTALL)


@activity.defn(name="probe_cpu")
async def fake_probe_cpu(machine: MachineSpec) -> PhaseResult:
    return _ok(PHASE_HW_CPU)


@activity.defn(name="probe_memory_ecc")
async def fake_probe_memory(machine: MachineSpec) -> PhaseResult:
    return _ok(PHASE_HW_MEMORY)


@activity.defn(name="probe_disks_smart")
async def fake_probe_disks(machine: MachineSpec) -> PhaseResult:
    return _ok(PHASE_HW_DISKS)


@activity.defn(name="probe_nics")
async def fake_probe_nics(machine: MachineSpec) -> PhaseResult:
    return _ok(PHASE_HW_NICS)


@activity.defn(name="probe_numa")
async def fake_probe_numa(machine: MachineSpec) -> PhaseResult:
    return _ok(PHASE_HW_NUMA)


@activity.defn(name="probe_lldp")
async def fake_probe_lldp(machine: MachineSpec) -> PhaseResult:
    return _ok(PHASE_HW_LLDP)


@activity.defn(name="probe_bios_firmware")
async def fake_probe_bios(machine: MachineSpec) -> PhaseResult:
    return _ok(PHASE_HW_BIOS)


_ALL_FAKES = [
    fake_ssh_check,
    fake_apt_install,
    fake_probe_cpu,
    fake_probe_memory,
    fake_probe_disks,
    fake_probe_nics,
    fake_probe_numa,
    fake_probe_lldp,
    fake_probe_bios,
]


def _plan(phases: list[str]) -> MachinePlan:
    return MachinePlan(
        machine=MachineSpec(
            name="bm-test",
            host="127.0.0.1",
            user="ubuntu",
            auth=AUTH_KEY,
            mesh_iface="eth0",
            test_disks=["/dev/sdb"],
            notes="",
        ),
        apt_packages=["fio"],
        phases=phases,
        timeouts_seconds={"setup": 60, "hardware_probe": 60, "persist_results": 60},
        retry_initial_seconds=1,
        retry_backoff=1.5,
        retry_max_attempts=1,
    )


async def _run_workflow(plan: MachinePlan) -> object:
    async with await WorkflowEnvironment.start_time_skipping(
        data_converter=DATA_CONVERTER,
    ) as env, Worker(
        env.client,
        task_queue="t",
        workflows=[MachineWorkflow],
        activities=_ALL_FAKES,
    ):
        return await env.client.execute_workflow(
            MachineWorkflow.run,
            plan,
            id=f"wf-{uuid.uuid4()}",
            task_queue="t",
        )


@pytest.mark.asyncio
async def test_setup_phase_runs_two_activities() -> None:
    result = await _run_workflow(_plan(["setup"]))
    phases = [p.phase for p in result.phases]
    assert phases == [PHASE_SSH_CHECK, PHASE_APT_INSTALL]
    assert result.status == STATUS_PASSED


@pytest.mark.asyncio
async def test_hardware_phase_fans_out_into_seven_activities() -> None:
    result = await _run_workflow(_plan(["hardware"]))
    phases = sorted(p.phase for p in result.phases)
    assert phases == sorted(
        [
            PHASE_HW_CPU,
            PHASE_HW_MEMORY,
            PHASE_HW_DISKS,
            PHASE_HW_NICS,
            PHASE_HW_NUMA,
            PHASE_HW_LLDP,
            PHASE_HW_BIOS,
        ]
    )
    assert result.status == STATUS_PASSED


@pytest.mark.asyncio
async def test_setup_then_hardware_runs_nine_phases() -> None:
    result = await _run_workflow(_plan(["setup", "hardware"]))
    assert len(result.phases) == 9
    assert result.status == STATUS_PASSED
