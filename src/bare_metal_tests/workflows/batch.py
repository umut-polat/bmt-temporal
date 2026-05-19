"""BatchWorkflow: orchestrate per-host, mesh and encap child workflows.

The CLI starts one BatchWorkflow per batch run when ``--use-batch`` is
passed. Children execute in parallel; each child's persistence runs
inside itself (see ``MachineWorkflow``, ``NetworkMeshWorkflow``,
``EncapsulationWorkflow``). The batch row in PostgreSQL is opened by
``persist_batch_init`` here and finalised by ``persist_batch_finalize``
once every child has returned.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from bare_metal_tests.activities.persist import BatchFinalize, BatchInit
    from bare_metal_tests.constants import STATUS_FAILED, STATUS_PASSED
    from bare_metal_tests.models import MachineResult
    from bare_metal_tests.workflows.encapsulation import (
        EncapPlan,
        EncapResult,
        EncapsulationWorkflow,
    )
    from bare_metal_tests.workflows.machine import MachinePlan, MachineWorkflow
    from bare_metal_tests.workflows.network_mesh import (
        MeshPlan,
        MeshResult,
        NetworkMeshWorkflow,
    )


@dataclass
class BatchPlan:
    """Top-level inputs for a batch run."""

    batch_id: str
    workflow_id: str
    machine_plans: list[MachinePlan]
    mesh_plan: MeshPlan | None
    encap_plan: EncapPlan | None
    config_snapshot: dict[str, object]
    timeouts_seconds: dict[str, int]
    retry_initial_seconds: int
    retry_backoff: float
    retry_max_attempts: int
    fail_on_smart_error: bool
    fail_on_memory_hw_error: bool


@dataclass
class BatchResult:
    """Final shape returned by the workflow."""

    status: str
    machine_results: list[MachineResult]
    mesh: MeshResult | None
    encap: EncapResult | None


def _retry(plan: BatchPlan) -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(seconds=plan.retry_initial_seconds),
        backoff_coefficient=plan.retry_backoff,
        maximum_attempts=plan.retry_max_attempts,
    )


def _should_fail(plan: BatchPlan, machine_results: list[MachineResult]) -> bool:
    """Implement the form's auto-fail rules: SMART and memory HW errors."""
    if any(m.status != STATUS_PASSED for m in machine_results):
        return True
    for m in machine_results:
        for phase in m.phases:
            if plan.fail_on_smart_error and phase.phase == "hw_disks_smart":
                for disk in phase.details.get("disks", []):
                    if isinstance(disk, dict) and disk.get("passed") is False:
                        return True
            if (
                plan.fail_on_memory_hw_error
                and phase.phase == "hw_memory_ecc"
                and phase.status != STATUS_PASSED
            ):
                return True
    return False


@workflow.defn
class BatchWorkflow:
    """Top-level workflow that fans out into per-host and cross-host children."""

    @workflow.run
    async def run(self, plan: BatchPlan) -> BatchResult:
        retry = _retry(plan)
        persist_timeout = timedelta(seconds=plan.timeouts_seconds["persist_results"])

        await workflow.execute_activity(
            "persist_batch_init",
            BatchInit(
                batch_id=plan.batch_id,
                workflow_id=plan.workflow_id,
                config_snapshot=plan.config_snapshot,
            ),
            start_to_close_timeout=persist_timeout,
            retry_policy=retry,
        )

        machine_tasks = [
            workflow.execute_child_workflow(
                MachineWorkflow.run,
                args=[mp],
                id=f"{plan.workflow_id}-machine-{mp.machine.name}",
                result_type=MachineResult,
            )
            for mp in plan.machine_plans
        ]
        mesh_task = (
            workflow.execute_child_workflow(
                NetworkMeshWorkflow.run,
                args=[plan.mesh_plan],
                id=f"{plan.workflow_id}-mesh",
                result_type=MeshResult,
            )
            if plan.mesh_plan is not None
            else None
        )
        encap_task = (
            workflow.execute_child_workflow(
                EncapsulationWorkflow.run,
                args=[plan.encap_plan],
                id=f"{plan.workflow_id}-encap",
                result_type=EncapResult,
            )
            if plan.encap_plan is not None
            else None
        )

        children = [*machine_tasks]
        if mesh_task is not None:
            children.append(mesh_task)
        if encap_task is not None:
            children.append(encap_task)

        gathered = await asyncio.gather(*children, return_exceptions=True)

        machine_results: list[MachineResult] = []
        mesh_result: MeshResult | None = None
        encap_result: EncapResult | None = None
        for i, child in enumerate(gathered):
            if isinstance(child, BaseException):
                continue
            if i < len(machine_tasks):
                machine_results.append(child)
            elif mesh_task is not None and i == len(machine_tasks):
                mesh_result = child
            else:
                encap_result = child

        status = STATUS_FAILED if _should_fail(plan, machine_results) else STATUS_PASSED
        if mesh_result is not None and mesh_result.status != STATUS_PASSED:
            status = STATUS_FAILED
        if encap_result is not None and encap_result.status != STATUS_PASSED:
            status = STATUS_FAILED

        await workflow.execute_activity(
            "persist_batch_finalize",
            BatchFinalize(batch_id=plan.batch_id, status=status),
            start_to_close_timeout=persist_timeout,
            retry_policy=retry,
        )

        return BatchResult(
            status=status,
            machine_results=machine_results,
            mesh=mesh_result,
            encap=encap_result,
        )
