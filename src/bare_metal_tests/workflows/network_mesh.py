"""NetworkMeshWorkflow: pair-wise throughput and jumbo-frame check.

For N machines the workflow runs N*(N-1)/2 unordered pairs split into
rounds with edge-disjoint scheduling so no machine is asked to serve and
receive at the same time. With 3 hosts that is 3 pairs in 3 rounds, each
round running a single iperf3 pair (each host participates in exactly
one role per round). For larger N a round-robin algorithm would parallelise.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from bare_metal_tests.activities.persist import BatchPhaseSink
    from bare_metal_tests.constants import STATUS_FAILED, STATUS_PASSED
    from bare_metal_tests.models import MachineSpec, PhaseResult


@dataclass
class MeshPlan:
    """Network mesh inputs."""

    machines: list[MachineSpec]
    duration_seconds: int
    parallel_streams: int
    mtu: int
    verify_jumbo: bool
    timeouts_seconds: dict[str, int]
    retry_initial_seconds: int
    retry_backoff: float
    retry_max_attempts: int
    batch_id: str | None = None


@dataclass
class MeshResult:
    """Outcome shape returned by the workflow."""

    status: str
    phases: list[PhaseResult]


def _retry(plan: MeshPlan) -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(seconds=plan.retry_initial_seconds),
        backoff_coefficient=plan.retry_backoff,
        maximum_attempts=plan.retry_max_attempts,
    )


def _pairs(machines: list[MachineSpec]) -> list[tuple[MachineSpec, MachineSpec]]:
    """All unordered pairs (server, client). For 3 machines: 3 pairs."""
    pairs: list[tuple[MachineSpec, MachineSpec]] = []
    for i, server in enumerate(machines):
        for client in machines[i + 1 :]:
            pairs.append((server, client))
    return pairs


@workflow.defn
class NetworkMeshWorkflow:
    """Run MTU/jumbo checks then full pair-wise iperf3 mesh."""

    @workflow.run
    async def run(self, plan: MeshPlan) -> MeshResult:
        retry = _retry(plan)
        phases: list[PhaseResult] = []
        mesh_timeout = timedelta(seconds=plan.timeouts_seconds["network_mesh"])

        mtu_tasks = [
            workflow.execute_activity(
                "verify_mtu",
                args=[m, plan.mtu],
                result_type=PhaseResult,
                start_to_close_timeout=mesh_timeout,
                retry_policy=retry,
            )
            for m in plan.machines
        ]
        phases.extend(await asyncio.gather(*mtu_tasks))

        if plan.verify_jumbo:
            jumbo_tasks = []
            for src, dst in _pairs(plan.machines):
                jumbo_tasks.append(
                    workflow.execute_activity(
                        "verify_jumbo_frames",
                        args=[src, dst.host],
                        result_type=PhaseResult,
                        start_to_close_timeout=mesh_timeout,
                        retry_policy=retry,
                    )
                )
            phases.extend(await asyncio.gather(*jumbo_tasks))

        for server, client in _pairs(plan.machines):
            pair_result = await workflow.execute_activity(
                "iperf3_pair",
                args=[server, client, plan.duration_seconds, plan.parallel_streams],
                result_type=PhaseResult,
                start_to_close_timeout=timedelta(seconds=plan.duration_seconds + 60),
                heartbeat_timeout=timedelta(seconds=60),
                retry_policy=retry,
            )
            phases.append(pair_result)

        status = (
            STATUS_PASSED
            if all(p.status == STATUS_PASSED for p in phases)
            else STATUS_FAILED
        )

        if plan.batch_id is not None:
            await workflow.execute_activity(
                "persist_batch_phases",
                BatchPhaseSink(
                    batch_id=plan.batch_id, workflow_label="mesh", phases=phases
                ),
                start_to_close_timeout=timedelta(
                    seconds=plan.timeouts_seconds["persist_results"]
                ),
                retry_policy=_retry(plan),
            )

        return MeshResult(status=status, phases=phases)
