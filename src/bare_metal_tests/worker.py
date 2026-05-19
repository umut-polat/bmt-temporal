"""Temporal worker entrypoint.

Run with ``bmt-worker`` (installed via ``pyproject.toml`` scripts) or
``python -m bare_metal_tests.worker``.
"""

from __future__ import annotations

import asyncio

import structlog
from temporalio.client import Client
from temporalio.worker import Worker

from bare_metal_tests.activities import ALL_ACTIVITIES
from bare_metal_tests.config import BmtConfig, load_config, load_secrets
from bare_metal_tests.temporal_data import DATA_CONVERTER
from bare_metal_tests.workflows import ALL_WORKFLOWS

log = structlog.get_logger()


def _temporal_address(
    config: BmtConfig, secrets: dict[str, str]
) -> tuple[str, str, str]:
    """Pick Temporal host/namespace/task_queue, letting secrets win."""
    return (
        secrets.get("TEMPORAL_HOST") or config.temporal.host,
        secrets.get("TEMPORAL_NAMESPACE") or config.temporal.namespace,
        secrets.get("TEMPORAL_TASK_QUEUE") or config.temporal.task_queue,
    )


async def run_worker() -> None:
    """Connect to the Temporal cluster and serve until cancelled."""
    config = load_config()
    secrets = load_secrets()
    host, namespace, task_queue = _temporal_address(config, secrets)

    log.info("worker.connect", host=host, namespace=namespace, task_queue=task_queue)
    client = await Client.connect(
        host, namespace=namespace, data_converter=DATA_CONVERTER
    )

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        max_concurrent_activities=config.worker.max_concurrent_activities,
        max_concurrent_workflow_tasks=config.worker.max_concurrent_workflow_tasks,
    )
    log.info(
        "worker.ready",
        workflows=[w.__name__ for w in ALL_WORKFLOWS],
        activities=len(ALL_ACTIVITIES),
    )
    await worker.run()


def main() -> None:
    """Console-script entrypoint."""
    structlog.configure(processors=[structlog.processors.KeyValueRenderer()])
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        log.info("worker.shutdown")


if __name__ == "__main__":
    main()
