"""Command-line entrypoint for starting workflows against the worker.

Usage:
    bmt-run --machine bm-13 --phases setup
    bmt-run --override config/demo.toml --batch all --phases setup,hardware,stress,mesh,encap
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

import structlog
from temporalio.client import Client

from bare_metal_tests.config import BmtConfig, load_config, load_secrets, postgres_dsn
from bare_metal_tests.constants import MACHINES_PATH
from bare_metal_tests.db.pool import close_pool, create_pool
from bare_metal_tests.db.repository import finalize_batch_run, insert_batch_run
from bare_metal_tests.models import MachineInventory, MachineSpec
from bare_metal_tests.temporal_data import DATA_CONVERTER
from bare_metal_tests.workflows.batch import BatchPlan, BatchResult, BatchWorkflow
from bare_metal_tests.workflows.encapsulation import EncapPlan, EncapsulationWorkflow
from bare_metal_tests.workflows.machine import (
    FioProfileSpec,
    MachinePlan,
    MachineWorkflow,
    StressSpec,
)
from bare_metal_tests.workflows.network_mesh import MeshPlan, NetworkMeshWorkflow

log = structlog.get_logger()

DEFAULT_PHASES = ["setup", "hardware", "stress"]
PER_MACHINE_PHASES = {"setup", "hardware", "stress"}


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="bmt-run")
    parser.add_argument("--override", type=Path, default=None)
    parser.add_argument("--machine", action="append", default=[])
    parser.add_argument("--batch", choices=["all"], default=None)
    parser.add_argument("--phases", default=",".join(DEFAULT_PHASES))
    parser.add_argument("--persist", action="store_true")
    parser.add_argument(
        "--use-batch",
        action="store_true",
        help="Start a single BatchWorkflow that spawns child workflows. Implies --persist.",
    )
    return parser.parse_args(argv)


def _select(inventory: MachineInventory, names: list[str]) -> list[MachineSpec]:
    if not names:
        return list(inventory.machines)
    by_name = {m.name: m for m in inventory.machines}
    missing = [n for n in names if n not in by_name]
    if missing:
        raise SystemExit(f"unknown machine(s): {missing}")
    return [by_name[n] for n in names]


def _build_stress(config: BmtConfig) -> StressSpec:
    """Translate config into a workflow-friendly stress spec."""
    fio = config.stress.disk_fio
    return StressSpec(
        cpu_duration_seconds=config.stress.cpu.duration_seconds,
        cpu_workers=config.stress.cpu.workers,
        memory_percent_of_total=config.stress.memory.percent_of_total,
        memory_duration_seconds=config.stress.memory.duration_seconds,
        memory_verify=config.stress.memory.verify,
        fio_profiles=[
            FioProfileSpec(name=p.name, rw=p.rw, bs=p.bs, iodepth=p.iodepth)
            for p in fio.profiles
        ],
        fio_size=fio.size,
        fio_runtime_seconds=fio.runtime_seconds,
        fio_ioengine=fio.ioengine,
        fio_direct=fio.direct,
        sysbench_max_prime=config.stress.sysbench_cpu.max_prime,
        sysbench_threads=config.stress.sysbench_cpu.threads,
        sysbench_duration_seconds=config.stress.sysbench_cpu.duration_seconds,
    )


def _build_plan(
    machine: MachineSpec,
    config: BmtConfig,
    phases: list[str],
    batch_id: str | None,
) -> MachinePlan:
    """Assemble the workflow input for one host."""
    timeouts = config.temporal.timeouts_seconds.model_dump()
    return MachinePlan(
        machine=machine,
        apt_packages=config.setup.apt_packages,
        phases=phases,
        timeouts_seconds=timeouts,
        retry_initial_seconds=config.temporal.retry.initial_interval_seconds,
        retry_backoff=config.temporal.retry.backoff_coefficient,
        retry_max_attempts=config.temporal.retry.maximum_attempts,
        stress=_build_stress(config) if "stress" in phases else None,
        batch_id=batch_id,
    )


def _build_mesh_plan(
    machines: list[MachineSpec],
    config: BmtConfig,
    batch_id: str | None,
) -> MeshPlan:
    """Assemble the workflow input for a mesh run."""
    return MeshPlan(
        machines=machines,
        duration_seconds=config.network.mesh.duration_seconds,
        parallel_streams=config.network.mesh.parallel_streams,
        mtu=config.network.mesh.mtu,
        verify_jumbo=config.network.mesh.verify_jumbo_frames,
        timeouts_seconds=config.temporal.timeouts_seconds.model_dump(),
        retry_initial_seconds=config.temporal.retry.initial_interval_seconds,
        retry_backoff=config.temporal.retry.backoff_coefficient,
        retry_max_attempts=config.temporal.retry.maximum_attempts,
        batch_id=batch_id,
    )


def _build_encap_plan(
    machines: list[MachineSpec],
    config: BmtConfig,
    batch_id: str | None,
) -> EncapPlan:
    """Assemble the workflow input for an encap (VXLAN) run between first 2 hosts."""
    if len(machines) < 2:
        raise SystemExit("encap phase requires at least 2 machines")
    return EncapPlan(
        host_a=machines[0],
        host_b=machines[1],
        duration_seconds=config.network.encapsulation.duration_seconds,
        compare_offload=config.network.encapsulation.compare_offload,
        vni=4242,
        timeouts_seconds=config.temporal.timeouts_seconds.model_dump(),
        retry_initial_seconds=config.temporal.retry.initial_interval_seconds,
        retry_backoff=config.temporal.retry.backoff_coefficient,
        retry_max_attempts=config.temporal.retry.maximum_attempts,
        batch_id=batch_id,
    )


async def _open_batch(
    secrets: dict[str, str], phases: list[str], targets: list[MachineSpec]
) -> str:
    """Insert an open batch_run row and return the new batch id."""
    batch_id = str(uuid.uuid4())
    workflow_id = f"bmt-cli-{batch_id}"
    pool = await create_pool(postgres_dsn(secrets), min_size=1, max_size=4)
    async with pool.acquire() as conn:
        await insert_batch_run(
            conn,
            batch_id=uuid.UUID(batch_id),
            workflow_id=workflow_id,
            config_snapshot={"phases": phases, "targets": [m.name for m in targets]},
        )
    log.info("cli.persist.open", batch_id=batch_id, workflow_id=workflow_id)
    return batch_id


async def _close_batch(secrets: dict[str, str], batch_id: str, status: str) -> None:
    """Mark the batch row finished and shut the pool."""
    pool = await create_pool(postgres_dsn(secrets))
    async with pool.acquire() as conn:
        await finalize_batch_run(
            conn,
            uuid.UUID(batch_id),
            status,
            datetime.now(UTC),
        )
    await close_pool()
    log.info("cli.persist.close", batch_id=batch_id, status=status)


def _build_batch_plan(
    batch_id: str,
    workflow_id: str,
    config: BmtConfig,
    targets: list[MachineSpec],
    phases: list[str],
) -> BatchPlan:
    """Compose a BatchPlan that wires together the child workflows."""
    machine_phases = [p for p in phases if p in PER_MACHINE_PHASES]
    machine_plans = [_build_plan(m, config, machine_phases, batch_id) for m in targets]
    mesh_plan = (
        _build_mesh_plan(targets, config, batch_id) if "mesh" in phases else None
    )
    encap_plan = (
        _build_encap_plan(targets, config, batch_id) if "encap" in phases else None
    )
    return BatchPlan(
        batch_id=batch_id,
        workflow_id=workflow_id,
        machine_plans=machine_plans,
        mesh_plan=mesh_plan,
        encap_plan=encap_plan,
        config_snapshot={"phases": phases, "targets": [m.name for m in targets]},
        timeouts_seconds=config.temporal.timeouts_seconds.model_dump(),
        retry_initial_seconds=config.temporal.retry.initial_interval_seconds,
        retry_backoff=config.temporal.retry.backoff_coefficient,
        retry_max_attempts=config.temporal.retry.maximum_attempts,
        fail_on_smart_error=config.report.fail_on_smart_error,
        fail_on_memory_hw_error=config.report.fail_on_memory_hw_error,
    )


async def _run_batch_workflow(
    client: Client,
    task_queue: str,
    plan: BatchPlan,
) -> tuple[str, int]:
    """Start the BatchWorkflow and return (status, exit_code)."""
    handle = await client.start_workflow(
        BatchWorkflow.run,
        plan,
        id=plan.workflow_id,
        task_queue=task_queue,
    )
    log.info("cli.started", workflow="BatchWorkflow", workflow_id=plan.workflow_id)
    result: BatchResult = await handle.result()
    log.info(
        "cli.finished",
        workflow="BatchWorkflow",
        status=result.status,
        machines=len(result.machine_results),
        mesh=result.mesh.status if result.mesh else None,
        encap=result.encap.status if result.encap else None,
    )
    return result.status, 0 if result.status == "passed" else 1


async def _run(args: argparse.Namespace) -> int:
    config = load_config(override_path=args.override)
    secrets = load_secrets()
    inventory = MachineInventory.model_validate_json(MACHINES_PATH.read_text())
    targets = _select(inventory, [] if args.batch == "all" else args.machine)
    phases = [p.strip() for p in args.phases.split(",") if p.strip()]

    host = secrets.get("TEMPORAL_HOST") or config.temporal.host
    namespace = secrets.get("TEMPORAL_NAMESPACE") or config.temporal.namespace
    task_queue = secrets.get("TEMPORAL_TASK_QUEUE") or config.temporal.task_queue

    batch_id: str | None = None
    if args.use_batch:
        batch_id = str(uuid.uuid4())
        workflow_id = f"bmt-batch-{batch_id}"
        log.info(
            "cli.connect",
            host=host,
            namespace=namespace,
            task_queue=task_queue,
            targets=[m.name for m in targets],
            phases=phases,
            batch_id=batch_id,
            mode="batch",
        )
        client = await Client.connect(
            host, namespace=namespace, data_converter=DATA_CONVERTER
        )
        plan = _build_batch_plan(batch_id, workflow_id, config, targets, phases)
        _, code = await _run_batch_workflow(client, task_queue, plan)
        return code

    if args.persist:
        batch_id = await _open_batch(secrets, phases, targets)

    log.info(
        "cli.connect",
        host=host,
        namespace=namespace,
        task_queue=task_queue,
        targets=[m.name for m in targets],
        phases=phases,
        batch_id=batch_id,
    )
    client = await Client.connect(
        host, namespace=namespace, data_converter=DATA_CONVERTER
    )

    machine_phases = [p for p in phases if p in PER_MACHINE_PHASES]
    handles: list[tuple[str, object]] = []

    if machine_phases:
        for machine in targets:
            plan = _build_plan(machine, config, machine_phases, batch_id)
            wf_id = f"bmt-machine-{machine.name}-{uuid.uuid4()}"
            handle = await client.start_workflow(
                MachineWorkflow.run,
                plan,
                id=wf_id,
                task_queue=task_queue,
            )
            log.info(
                "cli.started",
                workflow="MachineWorkflow",
                machine=machine.name,
                workflow_id=wf_id,
            )
            handles.append((machine.name, handle))

    if "mesh" in phases:
        wf_id = f"bmt-mesh-{uuid.uuid4()}"
        handle = await client.start_workflow(
            NetworkMeshWorkflow.run,
            _build_mesh_plan(targets, config, batch_id),
            id=wf_id,
            task_queue=task_queue,
        )
        log.info("cli.started", workflow="NetworkMeshWorkflow", workflow_id=wf_id)
        handles.append(("mesh", handle))

    if "encap" in phases:
        wf_id = f"bmt-encap-{uuid.uuid4()}"
        handle = await client.start_workflow(
            EncapsulationWorkflow.run,
            _build_encap_plan(targets, config, batch_id),
            id=wf_id,
            task_queue=task_queue,
        )
        log.info("cli.started", workflow="EncapsulationWorkflow", workflow_id=wf_id)
        handles.append(("encap", handle))

    failures = 0
    for name, handle in handles:
        result = await handle.result()
        log.info(
            "cli.finished",
            workflow=name,
            status=result.status,
            phases=[(p.phase, p.status) for p in result.phases],
        )
        if result.status != "passed":
            failures += 1

    if batch_id is not None:
        await _close_batch(secrets, batch_id, "passed" if failures == 0 else "failed")

    return 0 if failures == 0 else 1


def main() -> None:
    """Console-script entrypoint."""
    structlog.configure(processors=[structlog.processors.KeyValueRenderer()])
    args = _parse_args(sys.argv[1:])
    sys.exit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
