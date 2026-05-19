"""Defensive deserialisation for activity arguments.

Temporal's default JSON converter delivers ``MachineSpec`` to activities
as a plain dict because the activity signatures use string annotations
(``from __future__ import annotations``) that ``inspect.signature`` can't
fully reflect on. The Pydantic converter helps for some paths but not
all. Rather than relying on it, every activity that consumes
``MachineSpec`` calls :func:`as_machine` on its argument first.
"""

from __future__ import annotations

from typing import Any

from bare_metal_tests.models import MachineSpec


def as_machine(value: MachineSpec | dict[str, Any] | Any) -> MachineSpec:
    """Return ``value`` as a :class:`MachineSpec`, validating if necessary."""
    if isinstance(value, MachineSpec):
        return value
    if isinstance(value, dict):
        return MachineSpec.model_validate(value)
    return MachineSpec.model_validate(value, from_attributes=True)
