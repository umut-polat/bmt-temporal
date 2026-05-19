"""Plain INSERT/UPDATE statements for the ``bmt`` schema.

These functions are intentionally narrow: each one does one SQL operation
and returns nothing useful. Callers compose them inside an activity, which
opens a transaction once and runs the lot.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

import asyncpg

from bare_metal_tests.db.pool import jsonb


async def insert_batch_run(
    conn: asyncpg.Connection,
    batch_id: uuid.UUID,
    workflow_id: str,
    config_snapshot: dict[str, Any],
) -> None:
    """Insert the top-level batch row in the ``running`` state."""
    await conn.execute(
        """
        INSERT INTO bmt.batch_run (id, workflow_id, status, config_json)
        VALUES ($1, $2, 'running', $3::jsonb)
        ON CONFLICT (workflow_id) DO NOTHING
        """,
        batch_id,
        workflow_id,
        jsonb(config_snapshot),
    )


async def insert_machine_run(
    conn: asyncpg.Connection,
    machine_id: uuid.UUID,
    batch_id: uuid.UUID,
    name: str,
    host: str,
) -> None:
    """Insert one ``machine_run`` row for a host inside a batch."""
    await conn.execute(
        """
        INSERT INTO bmt.machine_run (id, batch_id, name, host, status)
        VALUES ($1, $2, $3, $4::inet, 'running')
        ON CONFLICT (batch_id, name) DO NOTHING
        """,
        machine_id,
        batch_id,
        name,
        host,
    )


async def insert_phase_result(
    conn: asyncpg.Connection,
    *,
    phase_id: uuid.UUID,
    batch_id: uuid.UUID,
    machine_id: uuid.UUID | None,
    phase: str,
    status: str,
    details: dict[str, Any],
    started_at: datetime,
    finished_at: datetime,
) -> None:
    """Store the outcome of a single phase."""
    await conn.execute(
        """
        INSERT INTO bmt.phase_result
            (id, batch_id, machine_id, phase, status, details, started_at, finished_at)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
        """,
        phase_id,
        batch_id,
        machine_id,
        phase,
        status,
        jsonb(details),
        started_at,
        finished_at,
    )


async def insert_metrics(
    conn: asyncpg.Connection,
    phase_id: uuid.UUID,
    metrics: list[tuple[str, float, str]],
) -> None:
    """Bulk insert numeric metrics tied to a phase."""
    if not metrics:
        return
    rows = [(phase_id, name, value, unit) for name, value, unit in metrics]
    await conn.executemany(
        "INSERT INTO bmt.metric (phase_id, name, value, unit) VALUES ($1, $2, $3, $4)",
        rows,
    )


async def finalize_machine_run(
    conn: asyncpg.Connection,
    machine_id: uuid.UUID,
    status: str,
    finished_at: datetime,
) -> None:
    """Mark a machine_run row finished with the final status."""
    await conn.execute(
        "UPDATE bmt.machine_run SET status=$2, finished_at=$3 WHERE id=$1",
        machine_id,
        status,
        finished_at,
    )


async def finalize_batch_run(
    conn: asyncpg.Connection,
    batch_id: uuid.UUID,
    status: str,
    finished_at: datetime,
) -> None:
    """Mark a batch_run row finished with the final status."""
    await conn.execute(
        "UPDATE bmt.batch_run SET status=$2, finished_at=$3 WHERE id=$1",
        batch_id,
        status,
        finished_at,
    )
