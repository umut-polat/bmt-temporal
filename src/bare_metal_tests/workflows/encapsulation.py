"""EncapsulationWorkflow: VXLAN tunnel + throughput + teardown."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from bare_metal_tests.activities.persist import BatchPhaseSink
    from bare_metal_tests.constants import STATUS_FAILED, STATUS_PASSED
    from bare_metal_tests.models import MachineSpec, PhaseResult


@dataclass
class EncapPlan:
    """Inputs for a single VXLAN tunnel test between two hosts."""

    host_a: MachineSpec
    host_b: MachineSpec
    duration_seconds: int
    compare_offload: bool
    vni: int
    timeouts_seconds: dict[str, int]
    retry_initial_seconds: int
    retry_backoff: float
    retry_max_attempts: int
    batch_id: str | None = None


@dataclass
class EncapResult:
    """Outcome shape returned by the workflow."""

    status: str
    phases: list[PhaseResult]


def _retry(plan: EncapPlan) -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(seconds=plan.retry_initial_seconds),
        backoff_coefficient=plan.retry_backoff,
        maximum_attempts=plan.retry_max_attempts,
    )


@workflow.defn
class EncapsulationWorkflow:
    """Bring up a VXLAN tunnel, measure throughput on/off offload, tear down.

    The teardown activity always runs even if iperf fails, so a leftover
    VXLAN device cannot pollute later runs.
    """

    @workflow.run
    async def run(self, plan: EncapPlan) -> EncapResult:
        retry = _retry(plan)
        timeout = timedelta(seconds=plan.timeouts_seconds["encapsulation"])
        phases: list[PhaseResult] = []

        setup = await workflow.execute_activity(
            "setup_vxlan",
            args=[plan.host_a, plan.host_b, plan.vni],
            result_type=PhaseResult,
            start_to_close_timeout=timeout,
            retry_policy=retry,
        )
        phases.append(setup)

        try:
            if setup.status == STATUS_PASSED:
                iperf = await workflow.execute_activity(
                    "iperf3_over_tunnel",
                    args=[
                        plan.host_a,
                        plan.host_b,
                        plan.duration_seconds,
                        plan.compare_offload,
                    ],
                    result_type=PhaseResult,
                    start_to_close_timeout=timedelta(
                        seconds=plan.duration_seconds * 3 + 60
                    ),
                    heartbeat_timeout=timedelta(seconds=60),
                    retry_policy=retry,
                )
                phases.append(iperf)
        finally:
            teardown = await workflow.execute_activity(
                "teardown_vxlan",
                args=[plan.host_a, plan.host_b],
                result_type=PhaseResult,
                start_to_close_timeout=timeout,
                retry_policy=retry,
            )
            phases.append(teardown)

        status = (
            STATUS_PASSED
            if all(p.status == STATUS_PASSED for p in phases)
            else STATUS_FAILED
        )

        if plan.batch_id is not None:
            await workflow.execute_activity(
                "persist_batch_phases",
                BatchPhaseSink(
                    batch_id=plan.batch_id, workflow_label="encap", phases=phases
                ),
                start_to_close_timeout=timedelta(
                    seconds=plan.timeouts_seconds["persist_results"]
                ),
                retry_policy=retry,
            )

        return EncapResult(status=status, phases=phases)
