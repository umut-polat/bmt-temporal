"""Persistence activities — write batch/machine/phase rows to PostgreSQL.

Workflows are the source of truth for state; PostgreSQL is the reporting
sink. Each persistence activity is idempotent (UNIQUE constraints on the
business keys ensure double-runs do not duplicate rows). The pool is
lazily initialised on the first call inside the worker process.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import asyncpg
from temporalio import activity

from bare_metal_tests.config import load_secrets, postgres_dsn
from bare_metal_tests.db import pool as db_pool
from bare_metal_tests.db import repository as repo
from bare_metal_tests.models import MachineResult, PhaseResult


@dataclass
class BatchInit:
    """First persistence step: open the batch row in the DB."""

    batch_id: str
    workflow_id: str
    config_snapshot: dict[str, object]


@dataclass
class MachineSink:
    """Per-machine results bundle to persist at the end of MachineWorkflow."""

    batch_id: str
    machine_result: MachineResult


@dataclass
class BatchPhaseSink:
    """Cross-machine phases (mesh / encap) attached to the batch row."""

    batch_id: str
    workflow_label: str
    phases: list[PhaseResult]


@dataclass
class BatchFinalize:
    """Final mark on the batch row."""

    batch_id: str
    status: str


async def _ensure_pool() -> asyncpg.Pool:
    """Return the shared asyncpg pool, lazily creating it on first use."""
    secrets = load_secrets()
    return await db_pool.create_pool(postgres_dsn(secrets))


def _now() -> datetime:
    return datetime.now(UTC)


@activity.defn(name="persist_batch_init")
async def persist_batch_init(payload: BatchInit) -> None:
    """Insert the batch_run row in ``running`` state."""
    pool = await _ensure_pool()
    async with pool.acquire() as conn:
        await repo.insert_batch_run(
            conn,
            batch_id=uuid.UUID(payload.batch_id),
            workflow_id=payload.workflow_id,
            config_snapshot=payload.config_snapshot,
        )


@activity.defn(name="persist_machine_results")
async def persist_machine_results(payload: MachineSink) -> None:
    """Insert one machine_run plus every phase and metric in a single txn."""
    pool = await _ensure_pool()
    result = payload.machine_result
    machine_id = result.machine_id
    async with pool.acquire() as conn, conn.transaction():
        await repo.insert_machine_run(
            conn,
            machine_id=machine_id,
            batch_id=uuid.UUID(payload.batch_id),
            name=result.machine.name,
            host=result.machine.host,
        )
        for phase in result.phases:
            await repo.insert_phase_result(
                conn,
                phase_id=phase.id,
                batch_id=uuid.UUID(payload.batch_id),
                machine_id=machine_id,
                phase=phase.phase,
                status=phase.status,
                details=phase.details,
                started_at=phase.started_at,
                finished_at=phase.finished_at,
            )
            if phase.metrics:
                await repo.insert_metrics(
                    conn,
                    phase_id=phase.id,
                    metrics=[(m.name, m.value, m.unit) for m in phase.metrics],
                )
        await repo.finalize_machine_run(
            conn,
            machine_id,
            result.status,
            result.finished_at,
        )


@activity.defn(name="persist_batch_phases")
async def persist_batch_phases(payload: BatchPhaseSink) -> None:
    """Insert phases not tied to a single machine (mesh / encap)."""
    pool = await _ensure_pool()
    async with pool.acquire() as conn, conn.transaction():
        for phase in payload.phases:
            await repo.insert_phase_result(
                conn,
                phase_id=phase.id,
                batch_id=uuid.UUID(payload.batch_id),
                machine_id=None,
                phase=f"{payload.workflow_label}.{phase.phase}",
                status=phase.status,
                details=phase.details,
                started_at=phase.started_at,
                finished_at=phase.finished_at,
            )
            if phase.metrics:
                await repo.insert_metrics(
                    conn,
                    phase_id=phase.id,
                    metrics=[(m.name, m.value, m.unit) for m in phase.metrics],
                )


@activity.defn(name="persist_batch_finalize")
async def persist_batch_finalize(payload: BatchFinalize) -> None:
    """Mark the batch row finished."""
    pool = await _ensure_pool()
    async with pool.acquire() as conn:
        await repo.finalize_batch_run(
            conn,
            uuid.UUID(payload.batch_id),
            payload.status,
            _now(),
        )


ALL_PERSIST_ACTIVITIES: list[Any] = [
    persist_batch_init,
    persist_machine_results,
    persist_batch_phases,
    persist_batch_finalize,
]
