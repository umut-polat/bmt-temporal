"""asyncpg connection pool factory.

A single pool lives in the worker process; activities acquire connections
from it. The pool is created lazily so the CLI helpers (which talk to the
DB without running a worker) can use it too.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg

_pool: asyncpg.Pool | None = None


async def _init_connection(conn: asyncpg.Connection) -> None:
    """Register a JSON codec so we can pass plain Python dicts to JSONB."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def create_pool(
    dsn: str,
    *,
    min_size: int = 2,
    max_size: int = 10,
) -> asyncpg.Pool:
    """Create (or return the cached) connection pool."""
    global _pool
    if _pool is not None:
        return _pool
    _pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=min_size,
        max_size=max_size,
        init=_init_connection,
    )
    return _pool


async def close_pool() -> None:
    """Close the cached pool if one exists."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def get_pool() -> asyncpg.Pool:
    """Return the existing pool. Raises if :func:`create_pool` was not called."""
    if _pool is None:
        raise RuntimeError("pool not initialised; call create_pool() first")
    return _pool


def jsonb(value: Any) -> str:
    """Serialise ``value`` for insertion into a JSONB column."""
    return json.dumps(value, default=str)
