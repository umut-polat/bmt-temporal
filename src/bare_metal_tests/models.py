"""Domain models shared between workflows, activities and the CLI.

All structures crossing the Temporal boundary must be JSON-serialisable, so
every model here is a Pydantic ``BaseModel`` with ``model_config`` set to
emit plain dicts on dump.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from bare_metal_tests.constants import (
    AUTH_KEY,
    AUTH_PASSWORD,
    STATUS_FAILED,
    STATUS_PASSED,
    STATUS_SKIPPED,
)


class MachineSpec(BaseModel):
    """A single bare metal host the test pipeline can target."""

    model_config = ConfigDict(extra="forbid")

    name: str
    host: str
    user: str
    auth: str = Field(pattern=f"^({AUTH_PASSWORD}|{AUTH_KEY})$")
    mesh_iface: str
    test_disks: list[str]
    notes: str = ""


class MachineInventory(BaseModel):
    """Top-level shape of ``environment/machines.json``."""

    model_config = ConfigDict(extra="forbid")

    machines: list[MachineSpec]


class PhaseResult(BaseModel):
    """Outcome of a single test phase on a single host (or batch-wide)."""

    model_config = ConfigDict(extra="forbid")

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    phase: str
    status: str = Field(pattern=f"^({STATUS_PASSED}|{STATUS_FAILED}|{STATUS_SKIPPED})$")
    details: dict[str, Any] = Field(default_factory=dict)
    metrics: list[Metric] = Field(default_factory=list)
    started_at: datetime
    finished_at: datetime


class Metric(BaseModel):
    """A single numeric observation attached to a phase result."""

    model_config = ConfigDict(extra="forbid")

    name: str
    value: float
    unit: str


class MachineResult(BaseModel):
    """Aggregated result for every phase executed against one host."""

    model_config = ConfigDict(extra="forbid")

    machine_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    machine: MachineSpec
    status: str
    started_at: datetime
    finished_at: datetime
    phases: list[PhaseResult]


PhaseResult.model_rebuild()
